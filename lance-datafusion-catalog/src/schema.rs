// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion_common::DataFusionError;
use lance::datafusion::LanceTableProvider;
use lance::dataset::Dataset;
use lance_namespace::models::DescribeTableRequest;
use lance_namespace::LanceNamespace;

/// A [`SchemaProvider`] implementation backed by a Lance namespace.
///
/// Each schema corresponds to a single logical Lance namespace (for example a
/// directory on local disk, or a remote namespace service). Tables are
/// resolved on demand via `describe_table` and exposed to DataFusion as
/// [`LanceTableProvider`] instances.
#[derive(Debug)]
pub struct LanceSchemaProvider {
    namespace: Arc<dyn LanceNamespace>,
    /// Optional namespace identifier. For directory namespaces this is usually
    /// `Some(vec![])` (root namespace).
    namespace_id: Option<Vec<String>>,
}

impl LanceSchemaProvider {
    /// Create a provider for the root namespace (no namespace id).
    pub fn new(namespace: Arc<dyn LanceNamespace>) -> Self {
        Self {
            namespace,
            namespace_id: Some(vec![]),
        }
    }

    /// Create a provider with an explicit namespace identifier.
    pub fn with_namespace_id(
        namespace: Arc<dyn LanceNamespace>,
        namespace_id: Option<Vec<String>>,
    ) -> Self {
        Self {
            namespace,
            namespace_id,
        }
    }

    fn table_id(&self, table: &str) -> Vec<String> {
        match &self.namespace_id {
            Some(prefix) if !prefix.is_empty() => {
                let mut id = prefix.clone();
                id.push(table.to_string());
                id
            }
            _ => vec![table.to_string()],
        }
    }

    async fn describe_table_uri(&self, table: &str) -> Result<String, DataFusionError> {
        let request = DescribeTableRequest {
            id: Some(self.table_id(table)),
            version: None,
            with_table_uri: None,
        };

        let response = self.namespace.describe_table(request).await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to describe Lance table '{}': {}", table, e))
        })?;

        response.table_uri.or(response.location).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Lance namespace did not return a location for table '{}'",
                table
            ))
        })
    }
}

#[async_trait]
impl SchemaProvider for LanceSchemaProvider {
    fn owner_name(&self) -> Option<&str> {
        None
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        // NOTE: Listing tables requires async access to the namespace. The
        // synchronous SchemaProvider API does not support async, so for now we
        // return an empty list and rely on direct table lookups via `table`.
        //
        // Future versions can provide a cached snapshot or use AsyncSchemaProvider.
        let _ = &self.namespace;
        let _ = &self.namespace_id;

        Vec::new()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let uri = self.describe_table_uri(name).await?;

        let dataset = Dataset::open(&uri).await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to open Lance dataset at '{}': {}", uri, e))
        })?;

        let provider = LanceTableProvider::new(Arc::new(dataset), false, false);
        Ok(Some(Arc::new(provider)))
    }

    fn register_table(
        &self,
        _name: String,
        _table: Arc<dyn TableProvider>,
    ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>> {
        datafusion_common::exec_err!(
            "LanceSchemaProvider does not support registering tables via DataFusion API"
        )
    }

    fn deregister_table(
        &self,
        _name: &str,
    ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>> {
        datafusion_common::exec_err!(
            "LanceSchemaProvider does not support deregistering tables via DataFusion API"
        )
    }

    fn table_exist(&self, _name: &str) -> bool {
        // Without synchronous access to the namespace we cannot reliably
        // implement existence checks here. DataFusion only uses this for DDL,
        // so it is safe to return false for the initial minimal integration.
        false
    }
}
