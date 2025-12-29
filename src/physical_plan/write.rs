// SPDX-License-Identifier: Apache-2.0

//! Insert execution plan for Lance.
//!
//! This plan is used by `LazyLanceTableProvider::insert_into` to route
//! `INSERT INTO` statements through a pure DataFusion physical plan:
//!
//! upstream plan --> LanceInsertExec --> count(*) result
//!
//! The actual data write is delegated to Lance's low-level insert
//! builder (`lance::dataset::InsertBuilder`), which in turn uses the
//! fragment writer and commit machinery. We intentionally avoid
//! calling high-level DML helpers like `Dataset::append`.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;

use arrow_array::UInt64Array;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use futures::stream;
use futures::{Stream, TryStreamExt};

use lance::dataset::{InsertBuilder, WriteMode, WriteParams};

use crate::error::to_datafusion_error;

/// Physical insert node for Lance tables.
///
/// This node has a single child that produces the rows to be written.
/// During execution it:
///
/// 1. Collects all input batches from the child
/// 2. Uses `InsertBuilder` with `WriteMode::Append` to write them into
///    the target dataset URI
/// 3. Returns a single-row batch with a `count` column reporting the
///    number of rows inserted.
///
/// The output schema therefore always has the shape:
///
/// ```text
/// count: UInt64
/// ```
#[derive(Debug)]
pub struct LanceInsertExec {
    /// Upstream physical plan that produces rows to insert.
    input: Arc<dyn ExecutionPlan>,
    /// Target dataset URI.
    table_uri: String,
    /// Output schema: single `count: UInt64` column.
    schema: ArrowSchemaRef,
    plan_properties: PlanProperties,
}

impl LanceInsertExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, table_uri: String) -> Self {
        let df_schema = datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new(
                "count",
                datafusion::arrow::datatypes::DataType::UInt64,
                false,
            ),
        ]);
        let schema: ArrowSchemaRef = Arc::new(df_schema);
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            // All input is funneled through a single writer.
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            input,
            table_uri,
            schema,
            plan_properties,
        }
    }

    async fn insert_and_build_stream(
        input: Arc<dyn ExecutionPlan>,
        table_uri: String,
        schema: ArrowSchemaRef,
        context: Arc<TaskContext>,
    ) -> DFResult<Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>> {
        // Execute the child plan (single partition).
        let input_stream = input.execute(0, context)?;

        // Collect all input batches.
        let batches: Vec<RecordBatch> = input_stream.try_collect().await?;

        let inserted_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();

        if inserted_rows > 0 {
            // Configure an append into the target URI using Lance's insert builder.
            let params = WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            };
            let builder = InsertBuilder::new(table_uri.as_str()).with_params(&params);

            builder
                .execute(batches)
                .await
                .map_err(to_datafusion_error)?;
        }

        // Produce a single-row count batch, even if 0 rows were inserted.
        let count_array = UInt64Array::from(vec![inserted_rows]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(count_array) as _])
            .map_err(to_datafusion_error)?;

        let out_stream = stream::iter(vec![Ok(batch)]);
        Ok(Box::pin(out_stream))
    }
}

impl ExecutionPlan for LanceInsertExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "LanceInsertExec"
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "LanceInsertExec expects exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(LanceInsertExec::new(
            children[0].clone(),
            self.table_uri.clone(),
        )))
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            // Should not be requested given UnknownPartitioning(1), but
            // return an empty stream defensively.
            let empty = stream::empty::<DFResult<RecordBatch>>();
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema.clone(),
                empty,
            )));
        }

        // Defer the async insert work to a one-shot future and flatten it
        // into a record batch stream, mirroring the pattern used in
        // `LanceTableScan`.
        let fut = LanceInsertExec::insert_and_build_stream(
            self.input.clone(),
            self.table_uri.clone(),
            self.schema.clone(),
            context,
        );
        let stream = stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

impl DisplayAs for LanceInsertExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "LanceInsertExec uri:{}", self.table_uri)
    }
}
