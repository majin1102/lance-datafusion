// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema};
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use lance::dataset::Dataset;
use lance_datafusion_catalog::dml::execute_lance_sql;
use lance_datafusion_catalog::register::register_directory_namespace_as_schema;
use tempfile::TempDir;

async fn count_rows(ctx: &SessionContext, sql: &str) -> Result<i64, DataFusionError> {
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
async fn merge_into_updates_and_inserts() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Create a temporary directory.
    let tmp_dir = TempDir::new()?;
    let root = tmp_dir.path().to_str().unwrap().to_string();

    // 2. Create target dataset "foo" with id = 0..10, x = id.
    let target_uri = format!("{}/foo.lance", root);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("x", DataType::Int32, false),
    ]));

    let ids: Vec<i32> = (0..10).collect();
    let xs: Vec<i32> = (0..10).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(ids.clone())),
            Arc::new(Int32Array::from(xs.clone())),
        ],
    )?;

    let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema.clone());
    Dataset::write(reader, target_uri.as_str(), None).await?;

    // 3. Create source dataset "bar" with id = 5..15, x = id * 10.
    let source_uri = format!("{}/bar.lance", root);
    let ids_src: Vec<i32> = (5..15).collect();
    let xs_src: Vec<i32> = ids_src.iter().map(|v| v * 10).collect();

    let batch_src = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(ids_src.clone())),
            Arc::new(Int32Array::from(xs_src.clone())),
        ],
    )?;

    let reader_src = RecordBatchIterator::new(vec![Ok(batch_src)].into_iter(), schema.clone());
    Dataset::write(reader_src, source_uri.as_str(), None).await?;

    // 4. Register directory namespace as my_catalog.default.
    let ctx = SessionContext::new();
    register_directory_namespace_as_schema(&ctx, "my_catalog", "default", &root).await?;

    // 5. Initial checks: target has 10 rows, source has 10 rows.
    let target_count = count_rows(&ctx, "SELECT COUNT(*) FROM my_catalog.default.foo").await?;
    assert_eq!(target_count, 10);
    let source_count = count_rows(&ctx, "SELECT COUNT(*) FROM my_catalog.default.bar").await?;
    assert_eq!(source_count, 10);

    // 6. Run MERGE INTO:
    //    - WHEN MATCHED: update foo.x = bar.x
    //    - WHEN NOT MATCHED: insert new rows from bar
    execute_lance_sql(
        &ctx,
        "MERGE INTO my_catalog.default.foo AS t \
         USING my_catalog.default.bar AS s \
         ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET x = s.x \
         WHEN NOT MATCHED THEN INSERT (id, x) VALUES (s.id, s.x)",
    )
    .await?;

    // 7. Validate row count: ids 0..14 should now exist (15 rows).
    let merged_count = count_rows(&ctx, "SELECT COUNT(*) FROM my_catalog.default.foo").await?;
    assert_eq!(merged_count, 15);

    // 8. Validate values:
    //    - For id in [0, 4], x should remain id.
    //    - For id in [5, 9], x should be bar.x = id * 10.
    //    - For id in [10, 14], rows should be newly inserted with x = id * 10.
    let df = ctx
        .sql("SELECT id, x FROM my_catalog.default.foo ORDER BY id")
        .await?;
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
            if (0..=4).contains(&id) {
                assert_eq!(x, id, "id {id} should remain unchanged");
            } else if (5..=9).contains(&id) {
                assert_eq!(x, id * 10, "id {id} should be updated from bar");
            } else if (10..=14).contains(&id) {
                assert_eq!(x, id * 10, "id {id} should be newly inserted from bar");
            } else {
                panic!("Unexpected id {id} in merged result");
            }
        }
        seen += batch.num_rows();
    }

    assert_eq!(seen, 15);

    Ok(())
}
