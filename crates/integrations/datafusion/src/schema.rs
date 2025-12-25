// SPDX-License-Identifier: Apache-2.0

//! Lance SchemaProvider backed by a [`LanceNamespace`].

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DFResult};
use lance::dataset::Dataset;
use lance_namespace::models::{DescribeTableRequest, ListTablesRequest};
use lance_namespace::LanceNamespace;

use crate::error::to_datafusion_error;
use crate::table::lance_table_provider::LanceTableProvider;

/// Represents a DataFusion [`SchemaProvider`] for a single Lance namespace.
#[derive(Debug)]
pub struct LanceSchemaProvider {
    namespace: Arc<dyn LanceNamespace>,
    /// Optional namespace identifier prefix (e.g. ["db"]).
    namespace_id: Option<Vec<String>>,
    /// Eagerly populated map of table name -> provider.
    tables: HashMap<String, Arc<LanceTableProvider>>,
}

impl LanceSchemaProvider {
    /// Construct a provider for the root namespace and eagerly
    /// populate table providers using `list_tables`.
    pub async fn try_new(namespace: Arc<dyn LanceNamespace>) -> DFResult<Self> {
        // Root namespace is represented by an empty ID vector.
        Self::with_namespace_id(namespace, Some(vec![])).await
    }

    /// Construct a provider with an explicit namespace id.
    pub async fn with_namespace_id(
        namespace: Arc<dyn LanceNamespace>,
        namespace_id: Option<Vec<String>>,
    ) -> DFResult<Self> {
        let table_names = list_table_names(&*namespace, namespace_id.clone()).await?;

        let mut tables = HashMap::new();
        for name in table_names {
            if let Some(provider) =
                build_table_provider(namespace.clone(), namespace_id.clone(), &name).await?
            {
                tables.insert(name, Arc::new(provider));
            }
        }

        Ok(Self {
            namespace,
            namespace_id,
            tables,
        })
    }

    pub fn new(namespace: Arc<dyn LanceNamespace>) -> Self {
        // For simple use-cases we can start with an empty table map and
        // lazily populate it on first access. This keeps the API ergonomic
        // while still allowing the async constructor to be used when
        // eager population is desired.
        Self {
            namespace,
            namespace_id: Some(vec![]),
            tables: HashMap::new(),
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
}

async fn list_table_names(
    namespace: &dyn LanceNamespace,
    namespace_id: Option<Vec<String>>,
) -> DFResult<Vec<String>> {
    let request = ListTablesRequest {
        id: namespace_id,
        page_token: None,
        limit: None,
    };
    let response = namespace
        .list_tables(request)
        .await
        .map_err(to_datafusion_error)?;
    Ok(response.tables)
}

async fn build_table_provider(
    namespace: Arc<dyn LanceNamespace>,
    namespace_id: Option<Vec<String>>,
    name: &str,
) -> DFResult<Option<LanceTableProvider>> {
    let request = DescribeTableRequest {
        id: Some(match &namespace_id {
            Some(prefix) if !prefix.is_empty() => {
                let mut id = prefix.clone();
                id.push(name.to_string());
                id
            }
            _ => vec![name.to_string()],
        }),
        version: None,
        with_table_uri: None,
    };

    let response = namespace
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
    Ok(Some(LanceTableProvider::new(Arc::new(dataset))))
}

#[async_trait]
impl SchemaProvider for LanceSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        if let Some(provider) = self.tables.get(name) {
            return Ok(Some(provider.clone()));
        }

        // Fallback: resolve on demand and cache in memory.
        if let Some(provider) =
            build_table_provider(self.namespace.clone(), self.namespace_id.clone(), name).await?
        {
            Ok(Some(Arc::new(provider)))
        } else {
            Ok(None)
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}
