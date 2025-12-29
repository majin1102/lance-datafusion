// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::Session as DFSession;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::{ExecutionPlan, PlanProperties};
use lance::dataset::Dataset;

use crate::error::to_datafusion_error;
use crate::physical_plan::scan::LanceTableScan;

/// Static table provider for read-only snapshot access.
#[derive(Debug, Clone)]
pub struct LanceStaticTableProvider {
    dataset: Dataset,
    schema: ArrowSchemaRef,
}

impl LanceStaticTableProvider {
    pub async fn try_new_from_dataset(dataset: Dataset) -> DFResult<Self> {
        let lance_schema = dataset.schema();
        let arrow_schema: datafusion::arrow::datatypes::Schema = lance_schema.into();
        let schema: ArrowSchemaRef = Arc::new(arrow_schema);
        Ok(Self { dataset, schema })
    }
}

#[async_trait]
impl TableProvider for LanceStaticTableProvider {
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
        let scan = LanceTableScan::new(
            self.dataset.clone(),
            self.schema.clone(),
            projection,
            filters,
            limit,
        );
        Ok(Arc::new(scan))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}
