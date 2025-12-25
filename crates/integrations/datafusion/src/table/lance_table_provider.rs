// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::Session as DFSession;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::ExecutionPlan as DFExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::PlanProperties;
use lance::dataset::Dataset;

use crate::error::to_datafusion_error;
use crate::physical_plan::scan::LanceTableScan;

/// Catalog-backed table provider with automatic metadata refresh.
///
/// This provider opens the underlying Lance dataset on every scan and
/// write operation, ensuring that the latest committed version is used.
#[derive(Debug, Clone)]
pub struct LanceTableProvider {
    /// Dataset URI (fully qualified path)
    uri: String,
    /// Cached Arrow schema, obtained at construction time.
    schema: ArrowSchemaRef,
}

impl LanceTableProvider {
    pub fn new(dataset: Arc<Dataset>) -> Self {
        let uri = dataset.uri().to_string();
        let lance_schema = dataset.schema();
        let arrow_schema: datafusion::arrow::datatypes::Schema = lance_schema.into();
        let schema: ArrowSchemaRef = Arc::new(arrow_schema);
        Self { uri, schema }
    }

    /// Get the underlying dataset URI.
    pub fn uri(&self) -> &str {
        &self.uri
    }

    #[allow(dead_code)]
    fn open_dataset(&self) -> DFResult<Dataset> {
        Ok(futures::executor::block_on(Dataset::open(&self.uri)).map_err(to_datafusion_error)?)
    }
}

#[async_trait::async_trait]
impl TableProvider for LanceTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn DFSession,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let dataset = Dataset::open(&self.uri)
            .await
            .map_err(to_datafusion_error)?;
        let scan = LanceTableScan::new(dataset, self.schema.clone(), projection, filters, limit);
        Ok(Arc::new(scan))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // Forward all filters; the scanner will determine which can be pushed down.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn insert_into(
        &self,
        _state: &dyn DFSession,
        _input: Arc<dyn ExecutionPlan>,
        _insert_op: datafusion::logical_expr::dml::InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // For now we route INSERT statements through the custom DML path
        // in `crate::dml::execute_lance_sql`, so `insert_into` is not used.
        // Implementing a full physical writer similar to Iceberg can be
        // added in a follow-up.
        Err(datafusion::error::DataFusionError::NotImplemented(
            "LanceTableProvider::insert_into is not wired; use Session::sql INSERT for Lance DML"
                .to_string(),
        ))
    }
}
