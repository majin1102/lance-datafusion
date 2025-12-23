// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::sync::Arc;

use dashmap::DashMap;
use datafusion::catalog::{CatalogProvider, CatalogProviderList};

/// A simple in-memory list of [`CatalogProvider`] implementations for Lance.
///
/// This mirrors DataFusion's [`MemoryCatalogProviderList`] but is tailored to
/// Lance-specific catalog providers.
#[derive(Debug, Default)]
pub struct LanceCatalogProviderList {
    catalogs: DashMap<String, Arc<dyn CatalogProvider>>,
}

impl LanceCatalogProviderList {
    /// Create an empty provider list.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a catalog under the given name.
    pub fn register_lance_catalog(
        &self,
        name: impl Into<String>,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.register_catalog(name.into(), catalog)
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
        self.catalogs.insert(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        self.catalogs.iter().map(|c| c.key().clone()).collect()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs.get(name).map(|c| Arc::clone(c.value()))
    }
}
