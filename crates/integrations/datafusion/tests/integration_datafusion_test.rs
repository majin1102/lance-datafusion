// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow_array::{Int32Array, Int64Array, RecordBatch, RecordBatchIterator, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use datafusion::error::DataFusionError;
use lance::dataset::Dataset;
use lance_integrations_datafusion::{Session, SessionBuilder};
use lance_namespace::LanceNamespace;
use lance_namespace_impls::DirectoryNamespaceBuilder;
use tempfile::TempDir;

async fn count_rows(session: &Session, sql: &str) -> Result<i64, DataFusionError> {
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
async fn integration_select_insert_update_merge() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Create a temporary directory and two Lance datasets "foo" and "bar".
    let tmp_dir = TempDir::new()?;
    let root = tmp_dir.path().to_str().unwrap().to_string();

    // Target dataset foo: id = 0..10, x = id.
    let foo_uri = format!("{}/foo.lance", root);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("x", DataType::Int32, false),
    ]));

    let ids: Vec<i32> = (0..10).collect();
    let xs: Vec<i32> = (0..10).collect();

    let batch_foo = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(ids.clone())),
            Arc::new(Int32Array::from(xs.clone())),
        ],
    )?;

    let reader_foo = RecordBatchIterator::new(vec![Ok(batch_foo)].into_iter(), schema.clone());
    Dataset::write(reader_foo, foo_uri.as_str(), None).await?;

    // Source dataset bar: id = 10..15, x = id * 10.
    let bar_uri = format!("{}/bar.lance", root);
    let ids_bar: Vec<i32> = (10..15).collect();
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

    // 2. Build a DirectoryNamespace and attach it via SessionBuilder.
    let namespace_impl = DirectoryNamespaceBuilder::new(&root).build().await?;
    let namespace: Arc<dyn LanceNamespace> = Arc::new(namespace_impl);

    let session = SessionBuilder::new().with_root_namespace(namespace).build();

    // 3. SELECT COUNT(*) from lance.default.foo.
    let initial_count = count_rows(&session, "SELECT COUNT(*) FROM lance.default.foo").await?;
    assert_eq!(initial_count, 10);

    // 4. INSERT INTO foo FROM bar using custom Lance DML routed via Session::sql.
    session
        .sql("INSERT INTO lance.default.foo SELECT id, x FROM lance.default.bar")
        .await?;

    let after_insert = count_rows(&session, "SELECT COUNT(*) FROM lance.default.foo").await?;
    println!("DEBUG after INSERT: rows = {}", after_insert);
    assert_eq!(after_insert, 15);

    // 5. UPDATE: increment x where id BETWEEN 5 AND 9.
    session
        .sql("UPDATE lance.default.foo SET x = x + 1 WHERE id BETWEEN 5 AND 9")
        .await?;

    // 6. MERGE: upsert from bar into foo on id.
    session
        .sql(
            "MERGE INTO lance.default.foo AS t \
             USING lance.default.bar AS s \
             ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET x = s.x \
             WHEN NOT MATCHED THEN INSERT (id, x) VALUES (s.id, s.x)",
        )
        .await?;

    let final_count = count_rows(&session, "SELECT COUNT(*) FROM lance.default.foo").await?;
    assert_eq!(final_count, 15);

    // Spot-check values for a few ids.
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

            if (0..5).contains(&id) {
                // ids 0..4: original rows, not present in bar; only UPDATE applied
                assert_eq!(x, id);
            } else if (5..=9).contains(&id) {
                // Rows updated by UPDATE, not touched by MERGE because `bar` only
                // contains ids 10..14.
                assert_eq!(x, id + 1);
            } else if (10..=14).contains(&id) {
                // Inserted from bar
                assert_eq!(x, id * 10);
            }
        }
        seen += batch.num_rows();
    }

    assert_eq!(seen, 15);

    Ok(())
}
