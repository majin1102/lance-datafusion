// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use crate::error::to_datafusion_error;
use crate::physical_plan::scan::LanceTableScan;
use crate::physical_plan::write::LanceInsertExec;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::ExecutionPlan;
use lance::dataset::Dataset;

/// Lazy table provider for Lance datasets.
///
/// Unlike the upstream `LanceTableProvider` in the `lance` crate, this
/// provider does **not** require an already-open `Dataset` at
/// construction time. Instead it stores only the dataset URI and
/// logical schema, and re-opens the dataset on each `scan` /
/// `insert_into` call to pick up the latest committed version.
#[derive(Debug, Clone)]
pub struct LanceTableProvider {
    /// Dataset URI (fully qualified path)
    dataset: Arc<Dataset>,
    /// Logical Arrow schema used for planning.
    schema: SchemaRef,
    /// Current version number of the dataset.
    version_number: u64,
}

impl LanceTableProvider {
    /// Create a new lazy provider from a dataset URI and Arrow schema.
    ///
    /// The schema is treated as logical planning schema; the underlying
    /// dataset is **not** opened here.
    pub fn new(dataset: Dataset) -> Self {
        let schema = Arc::new(dataset.schema().into());
        let version_number = dataset.version().version;
        Self {
            dataset: Arc::new(dataset),
            schema,
            version_number,
        }
    }

    pub fn dataset(&self) -> &Dataset {
        &self.dataset
    }

    pub async fn is_stale(&self) -> Result<bool> {
        let latest_version_number = self
            .dataset
            .latest_version_id()
            .await
            .map_err(to_datafusion_error)?;
        Ok(latest_version_number != self.version_number)
    }
}

#[async_trait]
impl TableProvider for LanceTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let scan = LanceTableScan::try_new(
            self.dataset.clone(),
            self.schema.clone(),
            projection,
            filters,
            limit,
        )
        .await?;
        Ok(Arc::new(scan))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // Forward all filters; the scanner will determine which can be pushed down.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match insert_op {
            InsertOp::Append => {
                // Coalesce all input into a single partition so that the
                // writer observes a single, ordered stream of batches.
                let coalesced = Arc::new(CoalescePartitionsExec::new(input));
                Ok(Arc::new(LanceInsertExec::new(
                    coalesced,
                    self.dataset.uri().to_string(),
                )))
            }
            other => Err(DataFusionError::NotImplemented(format!(
                "LazyLanceTableProvider::insert_into only supports `INSERT INTO` (Append); got {other}",
            ))),
        }
    }
}
