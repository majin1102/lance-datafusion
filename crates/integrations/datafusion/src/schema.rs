// SPDX-License-Identifier: Apache-2.0

//! Lance SchemaProvider implementations backed by a [`LanceNamespace`].
//!
//! - [`LanceSchemaProvider`]: a fully dynamic provider that resolves
//!   tables on demand from a root [`LanceNamespace`] and a fixed namespace
//!   identifier (e.g. `[catalog, schema]`). It does **not** cache table
//!   names or providers; each call to `table_names`, `table_exist`, or
//!   `table` goes back to the namespace.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DFResult};
use futures::executor as futures_executor;
use lance::dataset::Dataset;
use lance_namespace::models::{DescribeTableRequest, ListTablesRequest};
use lance_namespace::LanceNamespace;

use crate::error::to_datafusion_error;
use crate::table::lance_table_provider::LanceTableProvider;

// ---------------------------------------------------------------------------
// Helper: bridge async namespace calls into synchronous schema APIs
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
// Dynamic LanceSchemaProvider (no table caching)
// ---------------------------------------------------------------------------

/// Dynamic [`SchemaProvider`] backed directly by a [`LanceNamespace`].
///
/// * `table_names()` and `table_exist()` call `list_tables` on the
///   underlying namespace on every invocation.
/// * `table(name)` calls `describe_table` + `Dataset::open` to build a
///   fresh [`LanceTableProvider`]. No table providers are cached.
#[derive(Debug, Clone)]
pub struct LanceSchemaProvider {
    root: Arc<dyn LanceNamespace>,
    /// Full namespace identifier, e.g. `[catalog, schema]`.
    namespace_id: Vec<String>,
}

impl LanceSchemaProvider {
    pub fn new(root: Arc<dyn LanceNamespace>, namespace_id: Vec<String>) -> Self {
        Self { root, namespace_id }
    }

    fn table_id(&self, table: &str) -> Vec<String> {
        // For now we treat all tables as belonging to the root namespace.
        // This keeps `DirectoryNamespace` (which only supports root-level
        // table IDs) working while still allowing nested namespaces for
        // implementations that support them.
        vec![table.to_string()]
    }
}

#[async_trait]
impl SchemaProvider for LanceSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let root = Arc::clone(&self.root);

        block_on_future(async move {
            let request = ListTablesRequest {
                // Use the root namespace by default so that implementations
                // like `DirectoryNamespace` (which only support root-level
                // table listing) continue to work.
                id: Some(vec![]),
                page_token: None,
                limit: None,
            };

            match root.list_tables(request).await.map_err(to_datafusion_error) {
                Ok(resp) => resp.tables,
                Err(_) => Vec::new(),
            }
        })
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_names().iter().any(|t| t == name)
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let table_id = self.table_id(name);

        let request = DescribeTableRequest {
            id: Some(table_id),
            version: None,
            with_table_uri: None,
        };

        let response = self
            .root
            .describe_table(request)
            .await
            .map_err(to_datafusion_error)?;

        let uri = match (response.table_uri, response.location) {
            (Some(u), _) | (None, Some(u)) => u,
            _ => {
                return Err(DataFusionError::Execution(format!(
                    "Lance namespace did not return a location for table '{}'",
                    name
                )))
            }
        };

        let dataset = Dataset::open(&uri).await.map_err(to_datafusion_error)?;
        Ok(Some(
            Arc::new(LanceTableProvider::new(Arc::new(dataset))) as Arc<dyn TableProvider>
        ))
    }
}
