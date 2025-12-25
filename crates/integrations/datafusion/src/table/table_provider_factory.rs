// SPDX-License-Identifier: Apache-2.0

//! Minimal table provider factory helper.
//! 
//! This does **not** implement DataFusion's `TableProviderFactory` trait yet.
//! It is a lightweight utility for constructing `LanceStaticTableProvider`
//! instances from dataset URIs. The full trait integration can be added later
//! once we have a stable external table story.

use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DFResult};
use lance::dataset::Dataset;

use super::lance_static_table_provider::LanceStaticTableProvider;

/// Factory that can create static table providers from Lance dataset URIs.
#[derive(Debug, Default)]
pub struct LanceTableProviderFactory;

impl LanceTableProviderFactory {
    /// Create a static table provider for the given Lance dataset URI.
    pub async fn create_static_table(
        &self,
        uri: &str,
    ) -> DFResult<Arc<dyn TableProvider>> {
        let dataset = Dataset::open(uri)
            .await
            .map_err(DataFusionError::from)?;
        let provider = LanceStaticTableProvider::try_new_from_dataset(dataset).await?;
        Ok(Arc::new(provider))
    }
}
