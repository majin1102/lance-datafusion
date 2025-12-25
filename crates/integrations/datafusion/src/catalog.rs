// SPDX-License-Identifier: Apache-2.0

//! Lance CatalogProvider implementation mirroring the structure of
//! `iceberg-datafusion`'s `IcebergCatalogProvider`.

use std::any::Any;
use std::sync::Arc;

use dashmap::DashMap;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion_common::Result as DFResult;
use lance_namespace::LanceNamespace;

use crate::schema::LanceSchemaProvider;

/// Provides an interface to manage and access multiple schemas
/// within a Lance namespace backend.
///
/// Each schema typically corresponds to a single logical namespace
/// (for example a directory namespace or a REST namespace).
#[derive(Debug, Default)]
pub struct LanceCatalogProvider {
    /// Mapping from schema name to DataFusion schema provider.
    schemas: DashMap<String, Arc<dyn SchemaProvider>>,
}

impl LanceCatalogProvider {
    /// Create an empty catalog provider.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a pre-built [`SchemaProvider`] under the given name.
    pub fn register_schema_provider(
        &self,
        name: impl Into<String>,
        schema: Arc<dyn SchemaProvider>,
    ) -> DFResult<Option<Arc<dyn SchemaProvider>>> {
        Ok(self.schemas.insert(name.into(), schema))
    }

    /// Convenience helper to register a [`LanceNamespace`] as a schema.
    pub fn register_namespace(
        &self,
        schema_name: impl Into<String>,
        namespace: Arc<dyn LanceNamespace>,
    ) -> DFResult<Option<Arc<dyn SchemaProvider>>> {
        let schema_name = schema_name.into();
        let provider = Arc::new(LanceSchemaProvider::new(namespace)) as Arc<dyn SchemaProvider>;
        self.register_schema_provider(schema_name, provider)
    }
}

impl CatalogProvider for LanceCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.iter().map(|entry| entry.key().clone()).collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).map(|entry| Arc::clone(entry.value()))
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> datafusion_common::Result<Option<Arc<dyn SchemaProvider>>> {
        Ok(self.schemas.insert(name.to_string(), schema))
    }
}
