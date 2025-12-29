// SPDX-License-Identifier: Apache-2.0

//! Lance SchemaProvider implementations backed by a [`LanceNamespace`].
//!
//! - [`LanceSchemaProvider`]: a fully dynamic provider that resolves
//!   tables on demand from a root [`LanceNamespace`] and a fixed namespace
//!   identifier (e.g. `[catalog, schema]`). It does **not** cache table
//!   names or providers; each call to `table_names`, `table_exist`, or
//!   `table` goes back to the namespace.

use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use lance::datafusion::LanceTableProvider;
use lance_namespace::LanceNamespace;

use crate::error::to_datafusion_error;
use crate::namespace::Namespace;

/// Dynamic [`SchemaProvider`] backed directly by a [`LanceNamespace`].
///
/// * `table_names()` and `table_exist()` call `list_tables` on the
///   underlying namespace on every invocation.
/// * `table(name)` calls `describe_table` + `Dataset::open` to build a
///   fresh [`LanceTableProvider`]. No table providers are cached.
#[derive(Debug, Clone)]
pub struct LanceSchemaProvider {
    namespace: Namespace,
    tables: HashSet<String>,
}

impl LanceSchemaProvider {
    pub async fn from(namespace: Namespace) -> Result<Self> {
        let tables = namespace.tables().await?.into_iter().collect();
        Ok(Self { namespace, tables })
    }

    fn table_id(&self, table: &str) -> Vec<String> {
        let mut id = self.namespace.id();
        id.push(table.to_string());
        id
    }
}

#[async_trait]
impl SchemaProvider for LanceSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.iter().map(|s| s.to_string()).collect()
    }

    async fn table(&self, table_name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let dataset = self
            .namespace
            .load_dataset(table_name)
            .await
            .map_err(to_datafusion_error)?;
        Ok(Some(Arc::new(LanceTableProvider::new(
            Arc::new(dataset),
            true,
            true,
        ))))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains(name)
    }
}
