// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow_array::Int32Array;
use arrow_array::{RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema};
use datafusion::error::DataFusionError;
use lance::dataset::Dataset;
use lance_datafusion_catalog::{NamespaceConfig, Session};
use tempfile::TempDir;

async fn count_rows(ctx: &Session, sql: &str) -> Result<i64, DataFusionError> {
    use arrow_array::{Int64Array, UInt64Array};

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
async fn update_increments_range() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Create a temporary directory and a Lance dataset "foo" with columns id, x = 0..20.
    let tmp_dir = TempDir::new()?;
    let root = tmp_dir.path().to_str().unwrap().to_string();
    let table_uri = format!("{}/foo.lance", root);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("x", DataType::Int32, false),
    ]));

    let ids: Vec<i32> = (0..20).collect();
    let xs: Vec<i32> = (0..20).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(ids.clone())),
            Arc::new(Int32Array::from(xs.clone())),
        ],
    )?;

    let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema.clone());
    Dataset::write(reader, table_uri.as_str(), None).await?;

    // 2. Register a directory namespace as my_catalog.default in DataFusion.
    let ctx = Session::new();
    let ns = NamespaceConfig::for_directory(&root)
        .with_catalog("my_catalog")
        .with_schema("default");
    ctx.r#use(ns).await?;

    // 3. Run an initial COUNT(*) query.
    let initial_count =
        count_rows(&ctx, "SELECT COUNT(*) AS cnt FROM my_catalog.default.foo").await?;
    assert_eq!(initial_count, 20);

    // 4. Execute an UPDATE: x = x + 1 where id BETWEEN 5 AND 9.
    ctx.sql(
        "UPDATE my_catalog.default.foo \
             SET x = x + 1 \
             WHERE id BETWEEN 5 AND 9",
    )
    .await?;

    // 5. Verify the values: ids in [5, 9] have x incremented by 1, others unchanged.
    let df = ctx.sql("SELECT id, x FROM my_catalog.default.foo").await?;
    let batches = df.collect().await?;

    let mut seen = 0;
    for batch in batches {
        let id_col = batch
            .column_by_name("id")
            .expect("id column should exist")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id column should be Int32");
        let x_col = batch
            .column_by_name("x")
            .expect("x column should exist")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("x column should be Int32");

        for i in 0..batch.num_rows() {
            let id = id_col.value(i);
            let x = x_col.value(i);
            if (5..=9).contains(&id) {
                assert_eq!(x, id + 1, "id {id} should have been incremented");
            } else {
                assert_eq!(x, id, "id {id} should remain unchanged");
            }
        }
        seen += batch.num_rows();
    }

    assert_eq!(seen, 20);

    Ok(())
}
