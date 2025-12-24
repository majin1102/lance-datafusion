// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema};
use datafusion::error::DataFusionError;
use lance::dataset::Dataset;
use lance_datafusion_catalog::{NamespaceConfig, Session};
use tempfile::TempDir;

async fn count_rows(session: &Session, sql: &str) -> Result<i64, DataFusionError> {
    use arrow_array::{Int64Array, UInt64Array};

    let df = session.sql(sql).await?;
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
async fn session_end_to_end_directory_namespace() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Create a temporary directory and two Lance datasets "foo" and "bar".
    let tmp_dir = TempDir::new()?;
    let root = tmp_dir.path().to_str().unwrap().to_string();

    // Target dataset foo: id = 0..5, x = id.
    let foo_uri = format!("{}/foo.lance", root);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("x", DataType::Int32, false),
    ]));

    let ids: Vec<i32> = (0..5).collect();
    let xs: Vec<i32> = (0..5).collect();

    let batch_foo = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(ids.clone())),
            Arc::new(Int32Array::from(xs.clone())),
        ],
    )?;

    let reader_foo = RecordBatchIterator::new(vec![Ok(batch_foo)].into_iter(), schema.clone());
    Dataset::write(reader_foo, foo_uri.as_str(), None).await?;

    // Source dataset bar: id = 3..7, x = id * 10.
    let bar_uri = format!("{}/bar.lance", root);
    let ids_bar: Vec<i32> = (3..7).collect();
    let xs_bar: Vec<i32> = ids_bar.iter().map(|v| v * 10).collect();

    let batch_bar = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(ids_bar.clone())),
            Arc::new(Int32Array::from(xs_bar.clone())),
        ],
    )?;

    let reader_bar = RecordBatchIterator::new(vec![Ok(batch_bar)].into_iter(), schema.clone());
    Dataset::write(reader_bar, bar_uri.as_str(), None).await?;

    // 2. Create a Session and mount the directory namespace using defaults:
    //    catalog = "lance", schema = "default".
    let session = Session::new();
    let ns = NamespaceConfig::for_directory(&root);
    session.r#use(ns).await?;

    // 3. SELECT COUNT(*) via Session::sql.
    let initial_count = count_rows(&session, "SELECT COUNT(*) FROM lance.default.foo").await?;
    assert_eq!(initial_count, 5);

    // 4. UPDATE via Session::sql (routed to Lance DML): increment x where id < 3.
    session
        .sql(
            "UPDATE lance.default.foo \
             SET x = x + 1 \
             WHERE id < 3",
        )
        .await?;

    // 5. MERGE foo with bar via Session::sql using id as key.
    session
        .sql(
            "MERGE INTO lance.default.foo AS t \
             USING lance.default.bar AS s \
             ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET x = s.x \
             WHEN NOT MATCHED THEN INSERT (id, x) VALUES (s.id, s.x)",
        )
        .await?;

    // 6. Validate results: ids 0..6 should now exist.
    let merged_count = count_rows(&session, "SELECT COUNT(*) FROM lance.default.foo").await?;
    assert_eq!(merged_count, 7);

    let df = session
        .sql("SELECT id, x FROM lance.default.foo ORDER BY id")
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

            if id < 3 {
                // Updated once then potentially overwritten by MERGE if id >= 3.
                // For id 0..2, bar does not contain these ids, so they should be x = id + 1.
                assert_eq!(x, id + 1, "id {id} should have been incremented by UPDATE");
            } else if (3..=4).contains(&id) {
                // Present in both foo and bar: MERGE should set x = bar.x = id * 10.
                assert_eq!(x, id * 10, "id {id} should be updated from bar via MERGE");
            } else if (5..=6).contains(&id) {
                // Only present in bar: MERGE should insert new rows.
                assert_eq!(x, id * 10, "id {id} should be inserted from bar via MERGE");
            } else {
                panic!("Unexpected id {id} in merged result");
            }
        }
        seen += batch.num_rows();
    }

    assert_eq!(seen, 7);

    Ok(())
}
