// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow_array::{Int32Array, Int64Array, RecordBatch, RecordBatchIterator, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionContext;
use lance::dataset::Dataset;
use lance_integrations_datafusion::dml::execute_lance_sql;
use lance_integrations_datafusion::SessionBuilder;
use lance_namespace::LanceNamespace;
use lance_namespace_impls::DirectoryNamespaceBuilder;
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

async fn create_datasets(root: &str) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("x", DataType::Int32, false),
    ]));

    // Target dataset foo: id = 0..10, x = id.
    let foo_uri = format!("{}/foo.lance", root);
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

    Ok(())
}

async fn create_test_context() -> Result<(TempDir, SessionContext), Box<dyn std::error::Error>> {
    let tmp_dir = TempDir::new()?;
    let root = tmp_dir.path().to_str().unwrap().to_string();

    create_datasets(&root).await?;

    let namespace_impl = DirectoryNamespaceBuilder::new(&root).build().await?;
    let namespace: Arc<dyn LanceNamespace> = Arc::new(namespace_impl);

    let ctx = SessionBuilder::new()
        .with_root(Arc::clone(&namespace))
        .add_catalog("my_catalog", Arc::clone(&namespace))
        .add_schema("my_schema", Arc::clone(&namespace))
        .build();

    Ok((tmp_dir, ctx))
}

#[tokio::test]
async fn test_insert_dml() -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp_dir, ctx) = create_test_context().await?;

    let initial = count_rows(&ctx, "SELECT COUNT(*) FROM my_catalog.my_schema.foo").await?;
    assert_eq!(initial, 10);

    execute_lance_sql(
        &ctx,
        "INSERT INTO my_catalog.my_schema.foo \
         SELECT id, x FROM my_catalog.my_schema.bar",
    )
    .await?;

    let after = count_rows(&ctx, "SELECT COUNT(*) FROM my_catalog.my_schema.foo").await?;
    assert_eq!(after, 15);

    Ok(())
}

#[tokio::test]
async fn test_update_dml() -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp_dir, ctx) = create_test_context().await?;

    execute_lance_sql(
        &ctx,
        "UPDATE my_catalog.my_schema.foo \
         SET x = x + 1 \
         WHERE id BETWEEN 5 AND 9",
    )
    .await?;

    let df = ctx
        .sql("SELECT id, x FROM my_catalog.my_schema.foo ORDER BY id")
        .await?;
    let batches = df.collect().await?;

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
                assert_eq!(x, id);
            } else if (5..=9).contains(&id) {
                assert_eq!(x, id + 1);
            } else {
                assert_eq!(x, id);
            }
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_delete_dml() -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp_dir, ctx) = create_test_context().await?;

    execute_lance_sql(&ctx, "DELETE FROM my_catalog.my_schema.foo WHERE id >= 5").await?;

    let remaining = count_rows(&ctx, "SELECT COUNT(*) FROM my_catalog.my_schema.foo").await?;
    assert_eq!(remaining, 5);

    let df = ctx
        .sql("SELECT id FROM my_catalog.my_schema.foo ORDER BY id")
        .await?;
    let batches = df.collect().await?;

    for batch in batches {
        let id_col = batch
            .column_by_name("id")
            .expect("id column should exist")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id column should be Int32");

        for i in 0..batch.num_rows() {
            let id = id_col.value(i);
            assert!(id < 5);
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_merge_into_dml() -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp_dir, ctx) = create_test_context().await?;

    let initial = count_rows(&ctx, "SELECT COUNT(*) FROM my_catalog.my_schema.foo").await?;
    assert_eq!(initial, 10);

    execute_lance_sql(
        &ctx,
        "INSERT INTO my_catalog.my_schema.foo \
         SELECT id, x FROM my_catalog.my_schema.bar",
    )
    .await?;

    let after_insert = count_rows(&ctx, "SELECT COUNT(*) FROM my_catalog.my_schema.foo").await?;
    assert_eq!(after_insert, 15);

    execute_lance_sql(
        &ctx,
        "UPDATE my_catalog.my_schema.foo \
         SET x = x + 1 \
         WHERE id BETWEEN 5 AND 9",
    )
    .await?;

    execute_lance_sql(
        &ctx,
        "MERGE INTO my_catalog.my_schema.foo AS t \
         USING my_catalog.my_schema.bar AS s \
         ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET x = s.x \
         WHEN NOT MATCHED THEN INSERT (id, x) VALUES (s.id, s.x)",
    )
    .await?;

    let final_count = count_rows(&ctx, "SELECT COUNT(*) FROM my_catalog.my_schema.foo").await?;
    assert_eq!(final_count, 15);

    let df = ctx
        .sql("SELECT id, x FROM my_catalog.my_schema.foo ORDER BY id")
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
                assert_eq!(x, id);
            } else if (5..=9).contains(&id) {
                assert_eq!(x, id + 1);
            } else if (10..=14).contains(&id) {
                assert_eq!(x, id * 10);
            }
        }
        seen += batch.num_rows();
    }

    assert_eq!(seen, 15);

    Ok(())
}
