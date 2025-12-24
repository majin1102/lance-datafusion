// SPDX-License-Identifier: Apache-2.0
// High level Session wrapper that integrates DataFusion's SessionContext
// with Lance namespaces and minimal DML support (DELETE / UPDATE / MERGE).

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::{CatalogProviderList, SchemaProvider};
use datafusion::dataframe::DataFrame;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlanBuilder;
use lance_namespace::LanceNamespace;
use lance_namespace_impls::DirectoryNamespaceBuilder;

use crate::dml::execute_lance_sql;
use crate::{LanceCatalogProvider, LanceCatalogProviderList, LanceSchemaProvider};

/// The backend used to construct a Lance namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamespaceBackend {
    /// Directory based namespace, typically pointing at a local or remote
    /// filesystem / object-store path such as `/tmp/data` or `s3://bucket/path`.
    Directory,
    /// REST based namespace backed by a Lance Namespace service.
    ///
    /// Note: REST support depends on the `rest` feature of `lance-namespace-impls`
    /// being enabled at compile time. If it is not enabled, constructing such a
    /// namespace will return `DataFusionError::NotImplemented`.
    Rest,
}

/// Configuration for mounting a Lance namespace into DataFusion.
///
/// A `NamespaceConfig` describes how to construct the underlying
/// [`LanceNamespace`] as well as which catalog / schema it should be
/// registered under.
#[derive(Debug, Clone)]
pub struct NamespaceConfig {
    /// The backend type (directory or REST).
    pub backend: NamespaceBackend,
    /// Root URI / path of the namespace.
    pub uri: String,
    /// Optional storage options for directory namespaces.
    pub storage_options: Option<HashMap<String, String>>,
    /// Target catalog name (defaults to `"lance"`).
    pub catalog: String,
    /// Target schema name within the catalog (defaults to `"default"`).
    pub schema: String,
}

impl NamespaceConfig {
    /// Create a directory based namespace config with the given root path.
    pub fn for_directory(root: impl Into<String>) -> Self {
        Self {
            backend: NamespaceBackend::Directory,
            uri: root.into(),
            storage_options: None,
            catalog: "lance".to_string(),
            schema: "default".to_string(),
        }
    }

    /// Set the catalog name.
    pub fn with_catalog(mut self, catalog: impl Into<String>) -> Self {
        self.catalog = catalog.into();
        self
    }

    /// Set the schema name.
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = schema.into();
        self
    }

    /// Set storage options for directory namespaces.
    pub fn with_storage_options(mut self, storage_options: HashMap<String, String>) -> Self {
        self.storage_options = Some(storage_options);
        self
    }
}

impl From<&str> for NamespaceConfig {
    /// Interpret a bare string as a directory namespace root with default
    /// catalog ("lance") and schema ("default").
    fn from(root: &str) -> Self {
        NamespaceConfig::for_directory(root.to_string())
    }
}

impl From<String> for NamespaceConfig {
    fn from(root: String) -> Self {
        NamespaceConfig::for_directory(root)
    }
}

/// High level session wrapper that:
///
/// * Maintains a DataFusion [`SessionContext`].
/// * Maintains a Lance specific [`LanceCatalogProviderList`].
/// * Provides a convenient `use()` API to mount namespaces.
/// * Routes `DELETE` / `UPDATE` / `MERGE` statements to Lance's DML
///   implementation while delegating all other SQL to `SessionContext::sql`.
#[derive(Clone)]
pub struct Session {
    /// Underlying DataFusion session context.
    pub ctx: SessionContext,
    /// Catalog list used for Lance namespaces.
    pub catalog_list: Arc<LanceCatalogProviderList>,
    /// Default catalog name used when `NamespaceConfig` does not override it.
    pub default_catalog_name: String,
}

impl Session {
    /// Create a new `Session` with an empty `LanceCatalogProviderList` and
    /// default catalog name `"lance"`.
    pub fn new() -> Self {
        let ctx = SessionContext::new();
        let catalog_list = Arc::new(LanceCatalogProviderList::new());

        // Replace the default catalog list in DataFusion with ours.
        let catalog_list_arc: Arc<dyn CatalogProviderList> = catalog_list.clone();
        ctx.register_catalog_list(catalog_list_arc);

        Self {
            ctx,
            catalog_list,
            default_catalog_name: "lance".to_string(),
        }
    }

    /// Return a reference to the underlying [`SessionContext`].
    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }

    /// Mount a Lance namespace into this session.
    ///
    /// The namespace will be exposed as `config.catalog.config.schema.*` in SQL.
    ///
    /// This method is idempotent with respect to catalog creation: invoking
    /// `use()` multiple times with the same catalog name will reuse the
    /// existing catalog and simply register additional schemas on it.
    pub async fn r#use(&self, ns: impl Into<NamespaceConfig>) -> DFResult<()> {
        let mut config = ns.into();

        if config.catalog.is_empty() {
            config.catalog = self.default_catalog_name.clone();
        }
        if config.schema.is_empty() {
            config.schema = "default".to_string();
        }

        // Build the LanceNamespace implementation from the config.
        let namespace = build_namespace(&config).await?;
        let schema_provider: Arc<dyn SchemaProvider> =
            Arc::new(LanceSchemaProvider::new(namespace));

        // Get or create the catalog in our catalog list.
        let catalog_name = config.catalog.clone();
        let catalog = if let Some(existing) = self.catalog_list.catalog(&catalog_name) {
            existing
        } else {
            let catalog = Arc::new(LanceCatalogProvider::new())
                as Arc<dyn datafusion::catalog::CatalogProvider>;
            self.catalog_list
                .register_catalog(catalog_name.clone(), catalog.clone());
            catalog
        };

        catalog
            .register_schema(&config.schema, schema_provider)
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to register Lance schema '{}.{}': {}",
                    config.catalog, config.schema, e
                ))
            })?;

        // Ensure our catalog list is still registered with the context. This is
        // cheap (it simply swaps a reference inside SessionState) and makes the
        // behavior explicit for callers that may have modified the context.
        let catalog_list_arc: Arc<dyn CatalogProviderList> = self.catalog_list.clone();
        self.ctx.register_catalog_list(catalog_list_arc);

        Ok(())
    }

    /// Execute SQL against this session.
    ///
    /// * If the SQL starts with `DELETE` / `UPDATE` / `MERGE` (case-insensitive),
    ///   it is routed to Lance's custom DML engine via [`execute_lance_sql`].
    ///   In this case an empty [`DataFrame`] is returned, mirroring
    ///   DataFusion's behavior for non-query statements.
    /// * Otherwise the call is delegated directly to
    ///   [`SessionContext::sql`].
    pub async fn sql(&self, sql: &str) -> DFResult<DataFrame> {
        if is_lance_dml(sql) {
            // Execute DML side effects first.
            execute_lance_sql(&self.ctx, sql).await?;
            // Then return an empty DataFrame as an acknowledgement.
            empty_dataframe(&self.ctx)
        } else {
            self.ctx.sql(sql).await
        }
    }
}

fn is_lance_dml(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    if trimmed.is_empty() {
        return false;
    }
    let upper = trimmed
        .chars()
        .take(6)
        .map(|c| c.to_ascii_uppercase())
        .collect::<String>();
    upper.starts_with("DELETE") || upper.starts_with("UPDATE") || upper.starts_with("MERGE")
}

/// Helper to construct an empty DataFrame, similar to DataFusion's internal
/// `return_empty_dataframe` helper on `SessionContext`.
fn empty_dataframe(ctx: &SessionContext) -> DFResult<DataFrame> {
    let plan = LogicalPlanBuilder::empty(false).build()?;
    Ok(DataFrame::new(ctx.state(), plan))
}

async fn build_namespace(config: &NamespaceConfig) -> DFResult<Arc<dyn LanceNamespace>> {
    match config.backend {
        NamespaceBackend::Directory => {
            let mut builder = DirectoryNamespaceBuilder::new(&config.uri);
            if let Some(opts) = &config.storage_options {
                builder = builder.storage_options(opts.clone());
            }

            let ns = builder.build().await.map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to build DirectoryNamespace for '{}': {}",
                    config.uri, e
                ))
            })?;

            Ok(Arc::new(ns) as Arc<dyn LanceNamespace>)
        }
        NamespaceBackend::Rest => {
            #[cfg(feature = "rest")]
            {
                use lance_namespace_impls::RestNamespaceBuilder;

                let builder = RestNamespaceBuilder::new(&config.uri);
                let ns = builder.build().await.map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to build RestNamespace for '{}': {}",
                        config.uri, e
                    ))
                })?;
                Ok(Arc::new(ns) as Arc<dyn LanceNamespace>)
            }

            #[cfg(not(feature = "rest"))]
            {
                let msg = format!(
                    "REST namespace backend requested for '{}' but 'rest' feature is not enabled",
                    config.uri
                );
                Err(DataFusionError::NotImplemented(msg))
            }
        }
    }
}
