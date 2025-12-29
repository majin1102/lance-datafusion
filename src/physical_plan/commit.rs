// SPDX-License-Identifier: Apache-2.0

//! Placeholder commit execution plan for Lance.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};

/// Minimal commit exec mirroring the structure of `IcebergCommitExec`.
#[derive(Debug)]
pub struct LanceCommitExec {
    schema: ArrowSchemaRef,
    plan_properties: PlanProperties,
}

impl LanceCommitExec {
    pub fn new(schema: ArrowSchemaRef) -> Self {
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            schema,
            plan_properties,
        }
    }
}

impl ExecutionPlan for LanceCommitExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "LanceCommitExec"
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
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

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        Err(datafusion::error::DataFusionError::NotImplemented(
            "LanceCommitExec is a placeholder; use Session::sql INSERT/UPDATE/MERGE for Lance DML"
                .to_string(),
        ))
    }
}

impl DisplayAs for LanceCommitExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "LanceCommitExec")
    }
}
