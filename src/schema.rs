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
use dashmap::DashMap;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;

use crate::error::to_datafusion_error;
use crate::namespace::Namespace;
use crate::table_provider::LanceTableProvider;

/// Dynamic [`SchemaProvider`] backed directly by a [`LanceNamespace`].
///
/// * `table_names()` and `table_exist()` call `list_tables` on the
///   underlying namespace on every invocation.
/// * `table(name)` calls `describe_table` + `Dataset::open` to build a
///   fresh [`LanceTableProvider`]. No table providers are cached.
#[derive(Debug, Clone)]
pub struct LanceSchemaProvider {
    namespace: Namespace,
    tables: DashMap<String, Arc<LanceTableProvider>>,
}

impl LanceSchemaProvider {
    pub async fn try_new(namespace: Namespace) -> Result<Self> {
        Ok(Self {
            namespace,
            tables: DashMap::new(),
        })
    }

    async fn load_and_cache_table(
        &self,
        table_name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let dataset = self
            .namespace
            .load_dataset(table_name)
            .await
            .map_err(to_datafusion_error)?;
        let table_provider = Arc::new(LanceTableProvider::new(dataset));
        self.tables
            .insert(table_name.to_string(), table_provider.clone());
        Ok(Some(table_provider as Arc<dyn TableProvider>))
    }
}

#[async_trait]
impl SchemaProvider for LanceSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    async fn table(&self, table_name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        if let Some(table_provider) = self.tables.get(table_name) {
            if table_provider.is_stale().await? {
                self.tables.remove(table_name);
                self.load_and_cache_table(table_name).await
            } else {
                Ok(Some(table_provider.clone()))
            }
        } else {
            self.load_and_cache_table(table_name).await
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}
