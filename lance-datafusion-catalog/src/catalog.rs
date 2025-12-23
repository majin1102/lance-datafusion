// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::sync::Arc;

use dashmap::DashMap;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion_common::Result as DFResult;
use lance_namespace::LanceNamespace;

use crate::schema::LanceSchemaProvider;

/// A [`CatalogProvider`] that exposes one or more Lance namespaces as
/// DataFusion schemas.
///
/// Typically each schema corresponds to a single [`LanceNamespace`], such as
/// a directory-based namespace or a remote namespace service.
#[derive(Debug, Default)]
pub struct LanceCatalogProvider {
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
        let name = name.into();
        self.register_schema(&name, schema)
    }

    /// Convenience helper to register a [`LanceNamespace`] as a schema.
    ///
    /// For directory namespaces this typically maps a root like `/tmp/data` to
    /// the `default` schema.
    pub fn register_namespace(
        &self,
        schema_name: impl Into<String>,
        namespace: Arc<dyn LanceNamespace>,
    ) -> DFResult<Option<Arc<dyn SchemaProvider>>> {
        let schema_name = schema_name.into();
        let schema = Arc::new(LanceSchemaProvider::new(namespace)) as Arc<dyn SchemaProvider>;
        self.register_schema(&schema_name, schema)
    }
}

impl CatalogProvider for LanceCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.iter().map(|s| s.key().clone()).collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).map(|s| Arc::clone(s.value()))
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> DFResult<Option<Arc<dyn SchemaProvider>>> {
        Ok(self.schemas.insert(name.to_string(), schema))
    }

    fn deregister_schema(
        &self,
        name: &str,
        _cascade: bool,
    ) -> DFResult<Option<Arc<dyn SchemaProvider>>> {
        Ok(self.schemas.remove(name).map(|(_, v)| v))
    }
}
