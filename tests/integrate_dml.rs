// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::error::Result as DFResult;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::SessionContext;
use lance::dataset::{WriteMode, WriteParams};
use lance::Dataset;
use lance_datafusion::table::table_provider::LanceTableProvider;
use lance_datafusion::SessionBuilder;
use tempfile::TempDir;

/// Create a temporary Lance dataset with a simple (id, value) schema.
fn create_test_batches(num_rows: i32) -> (ArrowSchemaRef, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let ids = Int32Array::from_iter_values(0..num_rows);
    let values = Int32Array::from_iter_values(0..num_rows);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(values)])
        .expect("failed to build test batch");

    (schema, vec![batch])
}

async fn create_temp_table(num_rows: i32) -> (TempDir, String) {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let table_path = temp_dir.path().join("test_table.lance");
    let table_uri = table_path.to_string_lossy().to_string();

    let (schema, batches) = create_test_batches(num_rows);
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

    let write_params = WriteParams {
        mode: WriteMode::Create,
        ..Default::default()
    };

    Dataset::write(reader, table_uri.as_str(), Some(write_params))
        .await
        .expect("failed to write test dataset");

    (temp_dir, table_uri)
}

async fn register_test_table(ctx: &SessionContext, table_uri: &str) {
    let dataset = Dataset::open(table_uri)
        .await
        .expect("failed to open test dataset");
    let provider = Arc::new(LanceTableProvider::new(dataset));

    ctx.register_table("test_table", provider)
        .expect("failed to register table");
}

fn extract_count(batches: &[RecordBatch]) -> u64 {
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    let col = batch.column(0);
    if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
        arr.value(0)
    } else if let Some(arr) = col.as_any().downcast_ref::<arrow_array::Int64Array>() {
        arr.value(0) as u64
    } else {
        panic!("unexpected count column type: {:?}", col.data_type());
    }
}

#[tokio::test]
async fn select_with_filter_and_projection_pushdown() -> DFResult<()> {
    let (_temp_dir, table_uri) = create_temp_table(100).await;

    let ctx = SessionBuilder::new().build().await?;
    register_test_table(&ctx, &table_uri).await;

    let df = ctx
        .sql("SELECT value FROM test_table WHERE id >= 10 AND id < 20")
        .await?;
    let batches = df.clone().collect().await?;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 10);

    let combined = concat_batches(&batches[0].schema(), &batches)?;
    let values = combined
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("value column should be Int32Array");
    assert_eq!(values.len(), 10);
    assert_eq!(values.value(0), 10);
    assert_eq!(values.value(9), 19);

    let plan = df.create_physical_plan().await?;
    let plan_str = DisplayableExecutionPlan::new(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(
        plan_str.contains("LanceTableScan"),
        "expected physical plan to contain LanceTableScan, got:\n{plan_str}"
    );

    Ok(())
}

#[tokio::test]
async fn insert_into_appends_rows() -> DFResult<()> {
    let (_temp_dir, table_uri) = create_temp_table(10).await;

    let ctx = SessionBuilder::new().build().await?;
    register_test_table(&ctx, &table_uri).await;

    let initial_df = ctx.sql("SELECT COUNT(*) AS c FROM test_table").await?;
    let initial_batches = initial_df.collect().await?;
    let initial_count = extract_count(&initial_batches);

    let insert_df = ctx
        .sql("INSERT INTO test_table SELECT id, value FROM test_table WHERE id < 4")
        .await?;
    let insert_batches = insert_df.collect().await?;
    let inserted_count = extract_count(&insert_batches);
    assert_eq!(inserted_count, 4);

    let dataset = Dataset::open(&table_uri)
        .await
        .expect("failed to reopen dataset after insert");
    let mut scanner = dataset.scan();
    scanner
        .project::<&str>(&[])
        .expect("failed to project empty schema for count");
    scanner.with_row_id();
    let new_count = scanner
        .count_rows()
        .await
        .expect("failed to count rows after insert");

    assert_eq!(new_count, initial_count + inserted_count);

    Ok(())
}
