// SPDX-License-Identifier: Apache-2.0

//! Lightweight scan node for Lance datasets.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use datafusion::prelude::Expr;
use futures::{Stream, TryStreamExt};
use lance::dataset::Dataset;

use crate::error::to_datafusion_error;

/// Simple scan node that reads from a Lance dataset using its `Scanner` API.
#[derive(Debug)]
pub struct LanceTableScan {
    dataset: Arc<Dataset>,
    schema: ArrowSchemaRef,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    limit: Option<usize>,
    plan_properties: PlanProperties,
}

impl LanceTableScan {
    pub fn new(
        dataset: Arc<Dataset>,
        schema: ArrowSchemaRef,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Self {
        let output_schema = match projection {
            None => schema.clone(),
            Some(cols) => Arc::new(schema.project(cols).unwrap()),
        };
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            dataset,
            schema: output_schema,
            projection: projection.cloned(),
            filters: filters.to_vec(),
            limit,
            plan_properties,
        }
    }
}

impl ExecutionPlan for LanceTableScan {
    fn name(&self) -> &str {
        "LanceTableScan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let fut = get_batch_stream(
            self.dataset.clone(),
            self.schema.clone(),
            self.projection.clone(),
            self.filters.clone(),
            self.limit,
        );
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

impl DisplayAs for LanceTableScan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "LanceTableScan projection:[{}] limit:[{}]",
            self.projection
                .as_ref()
                .map(|v| v
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(","))
                .unwrap_or_default(),
            self.limit
                .map(|v| v.to_string())
                .unwrap_or_else(|| "none".to_string())
        )
    }
}

async fn get_batch_stream(
    dataset: Arc<Dataset>,
    schema: ArrowSchemaRef,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    limit: Option<usize>,
) -> DFResult<Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>> {
    let mut scanner = dataset.scan();

    if let Some(cols) = projection {
        let names: Vec<String> = cols
            .iter()
            .map(|i| schema.field(*i).name().clone())
            .collect();
        scanner
            .project(&names)
            .map_err(|e| to_datafusion_error(e.to_string()))?;
    }

    if !filters.is_empty() {
        // For now serialize filters back to strings and let Lance parse them.
        // A dedicated Expr->filter translator can be added later.
        let combined = filters
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(" AND ");
        scanner
            .filter(&combined)
            .map_err(|e| to_datafusion_error(e.to_string()))?;
    }

    if let Some(n) = limit {
        scanner
            .limit(Some(n as i64), None)
            .map_err(|e| to_datafusion_error(e.to_string()))?;
    }

    let reader = scanner
        .try_into_stream()
        .await
        .map_err(|e| to_datafusion_error(e.to_string()))?;

    let mapped = reader.map_err(|e| to_datafusion_error(e.to_string()));
    Ok(Box::pin(mapped))
}
