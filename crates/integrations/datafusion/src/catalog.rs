// SPDX-License-Identifier: Apache-2.0

//! Lance `CatalogProvider` implementations.
//!
//! This module provides two flavors of catalog providers:
//!
//! - [`LanceCatalogProviderList`] / [`LanceCatalogProvider`]:
//!   fully dynamic providers backed directly by a [`LanceNamespace`] tree.
//!   Catalog and schema names are resolved on demand via `list_namespaces`,
//!   and no schemas are cached in memory.
//!
//! - [`LanceLegacyCatalogProvider`]:
//!   a simple in-memory catalog that stores user-registered
//!   [`SchemaProvider`]s. This is kept mainly for backwards compatibility
//!   and ad-hoc usage; new code should prefer the dynamic providers above.

use std::any::Any;
use std::sync::Arc;

use dashmap::DashMap;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider};
use datafusion_common::Result as DFResult;
use futures::executor as futures_executor;
use lance_namespace::models::ListNamespacesRequest;
use lance_namespace::LanceNamespace;

use crate::error::to_datafusion_error;
use crate::schema::{LanceLegacySchemaProvider, LanceSchemaProvider};

// ---------------------------------------------------------------------------
// Helper: bridge async namespace calls into synchronous catalog APIs
// ---------------------------------------------------------------------------

fn block_on_future<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.block_on(fut)
    } else {
        futures_executor::block_on(fut)
    }
}

// ---------------------------------------------------------------------------
// Dynamic CatalogProviderList backed by LanceNamespace
// ---------------------------------------------------------------------------

/// A dynamic [`CatalogProviderList`] that maps Lance namespaces to catalogs.
///
/// Top-level namespaces (e.g. `ListNamespacesRequest { id: Some(vec![]) }`)
/// are exposed as catalog names. Child namespaces under a given catalog are
/// exposed as schemas via [`LanceCatalogProvider`].
#[derive(Debug, Clone)]
pub struct LanceCatalogProviderList {
    /// Root Lance namespace used to resolve catalogs / schemas / tables.
    root: Arc<dyn LanceNamespace>,
}

impl LanceCatalogProviderList {
    /// Create a new dynamic catalog list backed by the given root namespace.
    pub fn new(root: Arc<dyn LanceNamespace>) -> Self {
        Self { root }
    }

    /// Return the underlying root namespace.
    pub fn root(&self) -> &Arc<dyn LanceNamespace> {
        &self.root
    }
}

impl CatalogProviderList for LanceCatalogProviderList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        _name: String,
        _catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        // This dynamic catalog list is backed directly by a LanceNamespace
        // hierarchy and does not support manual catalog registration.
        //
        // Returning `None` keeps the API surface compatible while signaling
        // that nothing was stored.
        None
    }

    fn catalog_names(&self) -> Vec<String> {
        let root = Arc::clone(&self.root);

        block_on_future(async move {
            let request = ListNamespacesRequest {
                id: Some(vec![]),
                page_token: None,
                limit: None,
            };

            match root
                .list_namespaces(request)
                .await
                .map_err(to_datafusion_error)
            {
                Ok(resp) => resp.namespaces,
                Err(_) => Vec::new(),
            }
        })
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        Some(Arc::new(LanceCatalogProvider::new(
            Arc::clone(&self.root),
            name.to_string(),
        )) as Arc<dyn CatalogProvider>)
    }
}

// ---------------------------------------------------------------------------
// Dynamic CatalogProvider backed by a Lance namespace branch
// ---------------------------------------------------------------------------

/// Dynamic [`CatalogProvider`] that views child namespaces under a single
/// top-level namespace as schemas.
#[derive(Debug, Clone)]
pub struct LanceCatalogProvider {
    root: Arc<dyn LanceNamespace>,
    catalog: String,
}

impl LanceCatalogProvider {
    pub fn new(root: Arc<dyn LanceNamespace>, catalog: String) -> Self {
        Self { root, catalog }
    }
}

impl CatalogProvider for LanceCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let root = Arc::clone(&self.root);
        let catalog_id = vec![self.catalog.clone()];

        block_on_future(async move {
            let request = ListNamespacesRequest {
                id: Some(catalog_id),
                page_token: None,
                limit: None,
            };

            match root
                .list_namespaces(request)
                .await
                .map_err(to_datafusion_error)
            {
                Ok(resp) => resp.namespaces,
                Err(_) => Vec::new(),
            }
        })
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let namespace_id = vec![self.catalog.clone(), name.to_string()];
        let provider = LanceSchemaProvider::new(Arc::clone(&self.root), namespace_id);
        Some(Arc::new(provider) as Arc<dyn SchemaProvider>)
    }

    // For dynamic providers, schema registration is not supported and we
    // fall back to the default `not_impl_err!` behavior from DataFusion.
}

// ---------------------------------------------------------------------------
// Legacy in-memory catalog (schema map + registration helpers)
// ---------------------------------------------------------------------------

/// Legacy in-memory catalog provider that stores schemas in a map.
///
/// This implementation keeps an in-memory map of schema providers and is kept
/// mainly for backwards compatibility and ad-hoc usage. New code should
/// prefer [`LanceCatalogProviderList`] / [`LanceCatalogProvider`] for fully
/// dynamic namespace-backed catalogs.
#[derive(Debug, Default)]
#[deprecated(
    note = "LanceLegacyCatalogProvider is kept for compatibility; prefer LanceCatalogProviderList backed by a LanceNamespace"
)]
pub struct LanceLegacyCatalogProvider {
    /// Mapping from schema name to DataFusion schema provider.
    schemas: DashMap<String, Arc<dyn SchemaProvider>>,
}

impl LanceLegacyCatalogProvider {
    /// Create an empty catalog provider.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a pre-built [`SchemaProvider`] under the given name.
    #[deprecated(
        note = "LanceLegacyCatalogProvider is kept for compatibility; prefer dynamic providers backed by LanceNamespace"
    )]
    pub fn register_schema_provider(
        &self,
        name: impl Into<String>,
        schema: Arc<dyn SchemaProvider>,
    ) -> DFResult<Option<Arc<dyn SchemaProvider>>> {
        Ok(self.schemas.insert(name.into(), schema))
    }

    /// Convenience helper to register a [`LanceNamespace`] as a schema.
    #[deprecated(
        note = "register_namespace is deprecated; prefer using LanceCatalogProviderList with a root LanceNamespace"
    )]
    pub fn register_namespace(
        &self,
        schema_name: impl Into<String>,
        namespace: Arc<dyn LanceNamespace>,
    ) -> DFResult<Option<Arc<dyn SchemaProvider>>> {
        let schema_name = schema_name.into();
        let provider =
            Arc::new(LanceLegacySchemaProvider::new(namespace)) as Arc<dyn SchemaProvider>;
        self.register_schema_provider(schema_name, provider)
    }
}

impl CatalogProvider for LanceLegacyCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas
            .get(name)
            .map(|entry| Arc::clone(entry.value()))
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> datafusion_common::Result<Option<Arc<dyn SchemaProvider>>> {
        Ok(self.schemas.insert(name.to_string(), schema))
    }
}
