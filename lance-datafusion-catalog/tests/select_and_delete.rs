// SPDX-License-Identifier: Apache-2.0

use arrow::datatypes::Int32Type;
use arrow_array::{Int64Array, UInt64Array};
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use lance::dataset::Dataset;
use lance_datafusion_catalog::dml::execute_lance_sql;
use lance_datafusion_catalog::register::register_directory_namespace_as_schema;
use lance_datagen::{self as datagen, BatchCount, RowCount};
use tempfile::TempDir;

async fn count_rows(ctx: &SessionContext, sql: &str) -> Result<i64, DataFusionError> {
    let df = ctx.sql(sql).await?;
    let batches = df.collect().await?;

    if batches.is_empty() {
        return Err(DataFusionError::Execution(
            "Expected at least one record batch from COUNT(*) query".to_string(),
        ));
    }

    let batch = &batches[0];
    if batch.num_columns() != 1 || batch.num_rows() != 1 {
        return Err(DataFusionError::Execution(format!(
            "Unexpected shape for COUNT(*) result: {}x{}",
            batch.num_rows(),
            batch.num_columns(),
        )));
    }

    let column = batch.column(0);

    if let Some(arr) = column.as_any().downcast_ref::<UInt64Array>() {
        Ok(arr.value(0) as i64)
    } else if let Some(arr) = column.as_any().downcast_ref::<Int64Array>() {
        Ok(arr.value(0))
    } else {
        Err(DataFusionError::Execution(format!(
            "Unexpected COUNT(*) array type: {:?}",
            column.data_type()
        )))
    }
}

#[tokio::test]
async fn select_and_delete() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Create a temporary directory and a Lance dataset "foo" with column x = 0..20.
    let tmp_dir = TempDir::new()?;
    let root = tmp_dir.path().to_str().unwrap().to_string();
    let table_uri = format!("{}/foo.lance", root);

    let batch_size = RowCount::from(20_u64);
    let reader = datagen::gen_batch()
        .col("x", datagen::generator::array::step::<Int32Type>())
        .into_reader_rows(batch_size, BatchCount::from(1_u32));

    // Write the dataset to disk so DirectoryNamespace can discover it.
    Dataset::write(reader, table_uri.as_str(), None).await?;

    // 2. Register a directory namespace as my_catalog.default in DataFusion.
    let ctx = SessionContext::new();
    register_directory_namespace_as_schema(&ctx, "my_catalog", "default", &root).await?;

    // 3. Run an initial COUNT(*) query.
    let initial_count =
        count_rows(&ctx, "SELECT COUNT(*) AS cnt FROM my_catalog.default.foo").await?;
    assert_eq!(initial_count, 20);

    // 4. Execute a minimal DELETE via execute_lance_sql.
    execute_lance_sql(&ctx, "DELETE FROM my_catalog.default.foo WHERE x >= 10").await?;

    // 5. Verify the row count has decreased.
    let remaining_count =
        count_rows(&ctx, "SELECT COUNT(*) AS cnt FROM my_catalog.default.foo").await?;
    assert_eq!(remaining_count, 10);

    Ok(())
}
