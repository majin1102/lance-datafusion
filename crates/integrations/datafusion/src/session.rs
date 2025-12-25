// SPDX-License-Identifier: Apache-2.0

//! High-level `Session` API built on top of DataFusion's [`SessionContext`].
//!
//! The main entry points are:
//!
//! - [`SessionBuilder`]: configure and build a [`Session`].
//!   - [`SessionBuilder::with_root_namespace`] installs a
//!     [`LanceCatalogProviderList`](crate::LanceCatalogProviderList)
//!     backed by a root [`LanceNamespace`].
//! - [`Session`]: thin wrapper around [`SessionContext`] that routes
//!   `DELETE` / `UPDATE` / `MERGE` / `INSERT` statements through
//!   [`execute_lance_sql`](crate::dml::execute_lance_sql)
//!   while delegating all other SQL to [`SessionContext::sql`].

use std::sync::Arc;

use datafusion::dataframe::DataFrame;
use datafusion::error::Result as DFResult;
use datafusion::execution::context::SessionContext;
use lance_namespace::LanceNamespace;

use crate::catalog::LanceCatalogProviderList;
use crate::dml::execute_lance_sql;

/// Builder for constructing a [`Session`].
#[derive(Clone)]
pub struct SessionBuilder {
    ctx: SessionContext,
}

impl SessionBuilder {
    /// Create a new builder with a fresh [`SessionContext`].
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
        }
    }

    /// Attach a root [`LanceNamespace`] to this session.
    ///
    /// This installs a [`LanceCatalogProviderList`] as the
    /// catalog list for the underlying [`SessionContext`], exposing
    /// the namespace hierarchy as `catalog.schema.table` names.
    pub fn with_root_namespace(mut self, root: Arc<dyn LanceNamespace>) -> Self {
        let catalog_list = Arc::new(LanceCatalogProviderList::new(root));
        self.ctx.register_catalog_list(catalog_list);
        self
    }

    /// Consume the builder and create a [`Session`].
    pub fn build(self) -> Session {
        Session { ctx: self.ctx }
    }
}

impl Default for SessionBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// High-level wrapper around [`SessionContext`].
#[derive(Clone)]
pub struct Session {
    ctx: SessionContext,
}

impl Session {
    /// Create a new `Session` with default configuration.
    pub fn new() -> Self {
        SessionBuilder::new().build()
    }

    /// Access the underlying [`SessionContext`].
    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }

    /// Execute SQL against this session.
    ///
    /// - `DELETE` / `UPDATE` / `MERGE` / `INSERT` statements are executed
    ///   via [`execute_lance_sql`], and an empty [`DataFrame`] is
    ///   returned to match DataFusion's DDL/DML behavior.
    /// - All other SQL is delegated directly to
    ///   [`SessionContext::sql`].
    pub async fn sql(&self, sql: &str) -> DFResult<DataFrame> {
        if is_lance_dml(sql) {
            execute_lance_sql(&self.ctx, sql).await?;
            // Return an empty DataFrame, mirroring how DataFusion
            // returns an empty relation for many DDL/DML commands.
            return self.ctx.read_empty();
        }

        self.ctx.sql(sql).await
    }
}

fn is_lance_dml(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    if trimmed.is_empty() {
        return false;
    }

    let first_token: String = trimmed
        .chars()
        .take_while(|c| !c.is_whitespace())
        .collect::<String>()
        .to_ascii_uppercase();

    matches!(
        first_token.as_str(),
        "DELETE" | "UPDATE" | "MERGE" | "INSERT"
    )
}
