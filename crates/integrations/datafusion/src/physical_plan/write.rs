// SPDX-License-Identifier: Apache-2.0

//! Placeholder write execution plan for Lance.
//!
//! For now we route INSERT statements through the custom DML path
//! in `crate::dml`, so this module only contains stubs that can be
//! extended in a follow-up to mirror `IcebergWriteExec`.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};

/// Minimal no-op write exec so the module compiles. Real write support
/// can be added later.
#[derive(Debug)]
pub struct LanceWriteExec {
    schema: ArrowSchemaRef,
    plan_properties: PlanProperties,
}

impl LanceWriteExec {
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

impl ExecutionPlan for LanceWriteExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "LanceWriteExec"
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
        // No output; DML is handled via custom path for now.
        Err(datafusion::error::DataFusionError::NotImplemented(
            "LanceWriteExec is a placeholder; use Session::sql INSERT for Lance DML".to_string(),
        ))
    }
}

impl DisplayAs for LanceWriteExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "LanceWriteExec")
    }
}
