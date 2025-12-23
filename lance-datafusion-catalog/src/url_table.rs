// SPDX-License-Identifier: Apache-2.0
// DataFusion URL-table support for Lance datasets.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::{dynamic_file::DynamicListTableFactory, TableProvider};
use datafusion::execution::context::SessionContext;
use datafusion_catalog::{DynamicFileCatalog, UrlTableFactory};
use datafusion_common::{DataFusionError, Result as DFResult};
use datafusion_session::SessionStore;
use lance::datafusion::LanceTableProvider;
use lance::dataset::builder::DatasetBuilder;

/// Parse a URL for Lance datasets.
///
/// Recognizes URLs of the form `<base>.lance` and `<base>.lance@branch`.
fn split_lance_url(url: &str) -> Option<(String, Option<String>)> {
    if let Some((base, branch)) = url.rsplit_once('@') {
        if base.ends_with(".lance") && !branch.is_empty() {
            return Some((base.to_string(), Some(branch.to_string())));
        }
    }

    if url.ends_with(".lance") {
        return Some((url.to_string(), None));
    }

    None
}

/// A [`UrlTableFactory`] that knows how to open Lance datasets from URLs.
#[derive(Debug, Default)]
struct LanceUrlTableFactory;

#[async_trait]
impl UrlTableFactory for LanceUrlTableFactory {
    async fn try_new(&self, url: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let Some((base, branch)) = split_lance_url(url) else {
            return Ok(None);
        };

        let mut builder = DatasetBuilder::from_uri(&base);
        if let Some(branch) = branch {
            builder = builder.with_branch(&branch, None);
        }

        let dataset = builder.load().await.map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to open Lance dataset from '{}': {}",
                url, e
            ))
        })?;

        let provider = LanceTableProvider::new(Arc::new(dataset), false, false);
        Ok(Some(Arc::new(provider) as Arc<dyn TableProvider>))
    }
}

/// A composite factory that first tries Lance URLs, then falls back to
/// DataFusion's default [`DynamicListTableFactory`].
#[derive(Debug)]
struct LanceOrDefaultUrlTableFactory {
    lance: LanceUrlTableFactory,
    default: DynamicListTableFactory,
}

impl LanceOrDefaultUrlTableFactory {
    fn new(default: DynamicListTableFactory) -> Self {
        Self {
            lance: LanceUrlTableFactory::default(),
            default,
        }
    }

    fn default_factory(&self) -> &DynamicListTableFactory {
        &self.default
    }
}

#[async_trait]
impl UrlTableFactory for LanceOrDefaultUrlTableFactory {
    async fn try_new(&self, url: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        if let Some(table) = self.lance.try_new(url).await? {
            return Ok(Some(table));
        }

        self.default.try_new(url).await
    }
}

/// Extension methods on [`SessionContext`] for enabling Lance URL-table support.
pub trait SessionContextExt {
    /// Enable querying Lance datasets via string-literal URLs.
    ///
    /// After calling this, queries like
    ///
    /// ```sql
    /// SELECT * FROM '/path/to/table.lance';
    /// SELECT * FROM '/path/to/table.lance@exp_new_features';
    /// ```
    ///
    /// will be resolved to Lance datasets using [`DatasetBuilder`].
    fn enable_lance_url_table(self) -> SessionContext;
}

impl SessionContextExt for SessionContext {
    fn enable_lance_url_table(self) -> SessionContext {
        // Capture the current catalog list so we can wrap it.
        let current_catalog_list = {
            let state = self.state_ref();
            let guard = state.read();
            Arc::clone(guard.catalog_list())
        };

        let default_factory = DynamicListTableFactory::new(SessionStore::new());
        let composite_factory = Arc::new(LanceOrDefaultUrlTableFactory::new(default_factory));
        let catalog_list = Arc::new(DynamicFileCatalog::new(
            current_catalog_list,
            Arc::clone(&composite_factory) as Arc<dyn UrlTableFactory>,
        ));

        let session_id = self.session_id();
        let ctx: SessionContext = self
            .into_state_builder()
            .with_session_id(session_id)
            .with_catalog_list(catalog_list)
            .build()
            .into();

        // Register the new session state with the inner DynamicListTableFactory so
        // it can access configuration, object stores, etc.
        composite_factory
            .default_factory()
            .session_store()
            .with_state(ctx.state_weak_ref());

        ctx
    }
}

#[cfg(test)]
mod tests {
    use super::split_lance_url;

    #[test]
    fn parse_lance_urls() {
        assert_eq!(
            split_lance_url("s3://bucket/test.lance"),
            Some(("s3://bucket/test.lance".to_string(), None))
        );
        assert_eq!(
            split_lance_url("s3://bucket/test.lance@exp_new_features"),
            Some((
                "s3://bucket/test.lance".to_string(),
                Some("exp_new_features".to_string()),
            ))
        );
        assert_eq!(
            split_lance_url("/tmp/data/test.lance"),
            Some(("/tmp/data/test.lance".to_string(), None))
        );
        assert_eq!(
            split_lance_url("/tmp/data/test.lance@branch-1"),
            Some((
                "/tmp/data/test.lance".to_string(),
                Some("branch-1".to_string()),
            ))
        );
        // Non-lance URLs should be ignored.
        assert_eq!(split_lance_url("/tmp/data/test.parquet"), None);
        assert_eq!(split_lance_url("/tmp/data/test.lance@"), None);
    }
}
