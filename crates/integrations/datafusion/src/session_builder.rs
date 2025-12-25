// SPDX-License-Identifier: Apache-2.0

//! Builder for constructing DataFusion [`SessionContext`] instances
//! backed by Lance namespaces.
//!
//! This API is intentionally minimal and returns the native
//! [`SessionContext`] without adding an extra wrapper. It wires up
//! dynamic catalog and schema providers so that Lance namespaces can
//! be accessed via `catalog.schema.table` names.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::CatalogProvider;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion_catalog::memory::MemoryCatalogProvider;
use lance_namespace::LanceNamespace;

use crate::catalog::LanceCatalogProviderList;
use crate::schema::LanceSchemaProvider;

/// Builder for configuring a [`SessionContext`] with Lance namespaces.
#[derive(Clone, Debug, Default)]
pub struct SessionBuilder {
    /// Optional root namespace exposed via a dynamic
    /// [`LanceCatalogProviderList`].
    root: Option<Arc<dyn LanceNamespace>>,
    /// Explicit catalogs to register by name.
    catalogs: Vec<(String, Arc<dyn LanceNamespace>)>,
    /// Explicit schemas to register by name.
    schemas: Vec<(String, Arc<dyn LanceNamespace>)>,
    /// Optional DataFusion session configuration.
    config: Option<SessionConfig>,
}

impl SessionBuilder {
    /// Create a new builder with no namespaces or configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach a root [`LanceNamespace`] that is exposed as a dynamic
    /// catalog list via [`LanceCatalogProviderList`].
    pub fn with_root(mut self, ns: Arc<dyn LanceNamespace>) -> Self {
        self.root = Some(ns);
        self
    }

    /// Register an additional catalog backed by the given namespace.
    ///
    /// The catalog is identified by `name` and can later be combined
    /// with schemas via [`SessionBuilder::add_schema`] using the same
    /// namespace.
    pub fn add_catalog(mut self, name: &str, ns: Arc<dyn LanceNamespace>) -> Self {
        self.catalogs.push((name.to_string(), ns));
        self
    }

    /// Register an additional schema backed by the given namespace.
    ///
    /// Schemas are attached to catalogs whose underlying
    /// `namespace_id()` matches that of `ns`. If no matching catalog
    /// exists, the schema is attached to a fallback catalog named
    /// `"lance"`.
    pub fn add_schema(mut self, name: &str, ns: Arc<dyn LanceNamespace>) -> Self {
        self.schemas.push((name.to_string(), ns));
        self
    }

    /// Provide an explicit [`SessionConfig`] for the underlying
    /// [`SessionContext`].
    pub fn with_config(mut self, config: SessionConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Build a [`SessionContext`] with all configured namespaces.
    pub fn build(self) -> SessionContext {
        let ctx = if let Some(config) = self.config {
            SessionContext::new_with_config(config)
        } else {
            SessionContext::new()
        };

        if let Some(root) = self.root {
            let catalog_list = Arc::new(LanceCatalogProviderList::new(root));
            ctx.register_catalog_list(catalog_list);
        }

        // Materialize user-specified catalogs using in-memory
        // `MemoryCatalogProvider` instances. Each catalog merely holds
        // schema providers; the schema providers themselves are fully
        // dynamic and do not cache tables.
        let mut memory_catalogs: HashMap<String, Arc<MemoryCatalogProvider>> =
            HashMap::with_capacity(self.catalogs.len());

        #[derive(Clone)]
        struct CatalogEntry {
            name: String,
            namespace_id: String,
        }

        let mut catalog_entries: Vec<CatalogEntry> = Vec::with_capacity(self.catalogs.len());

        for (name, ns) in self.catalogs {
            let catalog = memory_catalogs
                .entry(name.clone())
                .or_insert_with(|| Arc::new(MemoryCatalogProvider::new()))
                .clone();

            ctx.register_catalog(&name, catalog.clone() as Arc<dyn CatalogProvider>);

            catalog_entries.push(CatalogEntry {
                name,
                namespace_id: ns.namespace_id(),
            });
        }

        // Attach schemas to any catalogs whose namespace_id matches the
        // schema's namespace. This allows a single underlying
        // namespace to be exposed under multiple catalog names if
        // desired.
        for (schema_name, ns) in self.schemas {
            let ns_id = ns.namespace_id();
            let mut attached = false;

            for entry in &catalog_entries {
                if entry.namespace_id == ns_id {
                    if let Some(catalog) = memory_catalogs.get(&entry.name) {
                        let provider = LanceSchemaProvider::new(Arc::clone(&ns), vec![]);
                        let _ = catalog.register_schema(&schema_name, Arc::new(provider));
                        attached = true;
                    }
                }
            }

            if !attached {
                // Fallback: attach this schema under a dedicated
                // `lance` catalog.
                let catalog_name = "lance".to_string();
                let catalog = memory_catalogs
                    .entry(catalog_name.clone())
                    .or_insert_with(|| {
                        let cat = Arc::new(MemoryCatalogProvider::new());
                        ctx.register_catalog(
                            &catalog_name,
                            cat.clone() as Arc<dyn CatalogProvider>,
                        );
                        cat
                    })
                    .clone();

                let provider = LanceSchemaProvider::new(ns, vec![]);
                let _ = catalog.register_schema(&schema_name, Arc::new(provider));
            }
        }

        ctx
    }
}
