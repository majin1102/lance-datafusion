// SPDX-License-Identifier: Apache-2.0

//! Lance `CatalogProvider` implementations.
//!
//! This module provides dynamic catalog providers backed by a
//! [`LanceNamespace`] tree.
//!
//! - [`LanceCatalogProviderList`] / [`LanceCatalogProvider`]:
//!   fully dynamic providers backed directly by a [`LanceNamespace`] tree.
//!   Catalog and schema names are resolved on demand via `list_namespaces`,
//!   and no schemas are cached in memory.

use std::any::Any;
use std::sync::Arc;

use dashmap::DashMap;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider};
use futures::executor as futures_executor;
use lance_namespace::models::ListNamespacesRequest;
use lance_namespace::LanceNamespace;

use crate::error::to_datafusion_error;
use crate::schema::LanceSchemaProvider;

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
    /// Manually registered catalogs (for example from `SessionBuilder::add_catalog`).
    manual_catalogs: DashMap<String, Arc<dyn CatalogProvider>>,
}

impl LanceCatalogProviderList {
    /// Create a new dynamic catalog list backed by the given root namespace.
    pub fn new(root: Arc<dyn LanceNamespace>) -> Self {
        Self {
            root,
            manual_catalogs: DashMap::new(),
        }
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
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.manual_catalogs.insert(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        // Start with manually registered catalogs.
        let mut names: Vec<String> = self
            .manual_catalogs
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

        let root = Arc::clone(&self.root);

        let dynamic = block_on_future(async move {
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
        });

        for name in dynamic {
            if !names.contains(&name) {
                names.push(name);
            }
        }

        names
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        if let Some(entry) = self.manual_catalogs.get(name) {
            Some(Arc::clone(entry.value()))
        } else {
            Some(Arc::new(LanceCatalogProvider::new(
                Arc::clone(&self.root),
                name.to_string(),
            )) as Arc<dyn CatalogProvider>)
        }
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
