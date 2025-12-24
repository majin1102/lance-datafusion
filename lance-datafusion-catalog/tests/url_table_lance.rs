// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use lance::dataset::{Dataset, UpdateBuilder};
use lance_datafusion_catalog::SessionContextExt;
use tempfile::TempDir;

fn as_i64_vec(array: &Int64Array) -> Vec<i64> {
    (0..array.len()).map(|i| array.value(i)).collect()
}

fn as_f64_vec(array: &Float64Array) -> Vec<f64> {
    (0..array.len()).map(|i| array.value(i)).collect()
}

fn as_str_vec(array: &StringArray) -> Vec<String> {
    (0..array.len())
        .map(|i| array.value(i).to_string())
        .collect()
}

#[tokio::test]
async fn lance_url_table_main_and_branch_queries() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Create a temporary Lance dataset with simple id/label/score columns.
    let tmp_dir = TempDir::new()?;
    let root = tmp_dir.path().to_str().unwrap();
    let table_uri = format!("{}/test.lance", root);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("score", DataType::Float64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["cat", "dog", "cat"])),
            Arc::new(Float64Array::from(vec![0.9, 0.8, 0.95])),
        ],
    )?;

    let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema.clone());
    Dataset::write(reader, table_uri.as_str(), None).await?;

    // 2. Create a branch `exp_new_features` from main and modify a row on the branch.
    let mut main_ds = Dataset::open(&table_uri).await?;
    let branch_ds = main_ds
        .create_branch("exp_new_features", 1u64, None)
        .await?;

    let update_result = UpdateBuilder::new(Arc::new(branch_ds))
        .update_where("id = 2")?
        .set("label", "'cat_exp'")?
        .set("score", "0.97")?
        .build()?
        .execute()
        .await?;
    // Keep updated branch alive until the end of the test.
    let _branch_updated = update_result.new_dataset;

    // 3. Build a SessionContext with Lance URL-table support enabled.
    let ctx = SessionContext::new().enable_lance_url_table();

    // 4. Query the main branch via URL.
    let sql_main = format!(
        "SELECT id, label, score FROM '{}' WHERE label = 'cat' ORDER BY id",
        table_uri
    );
    let batches_main = ctx.sql(&sql_main).await?.collect().await?;
    assert_eq!(batches_main.len(), 1);
    let batch_main = &batches_main[0];
    assert_eq!(batch_main.num_rows(), 2);

    let ids = batch_main
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let labels = batch_main
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let scores = batch_main
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(as_i64_vec(ids), vec![1, 3]);
    assert_eq!(as_str_vec(labels), vec!["cat", "cat"]);
    assert_eq!(as_f64_vec(scores), vec![0.9, 0.95]);

    // 5. Query the experimental branch via URL with @exp_new_features suffix.
    let sql_branch = format!(
        "SELECT id, score FROM '{}@exp_new_features' WHERE score > 0.95 ORDER BY id",
        table_uri
    );
    let batches_branch = ctx.sql(&sql_branch).await?.collect().await?;
    assert_eq!(batches_branch.len(), 1);
    let batch_branch = &batches_branch[0];
    assert_eq!(batch_branch.num_rows(), 1);

    let ids_b = batch_branch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let scores_b = batch_branch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(as_i64_vec(ids_b), vec![2]);
    assert_eq!(as_f64_vec(scores_b), vec![0.97]);

    // 6. JOIN main vs branch to find rows where the label differs.
    let sql_join = format!(
        "SELECT m.id, m.label AS main_label, e.label AS exp_label \
         FROM '{base}' AS m \
         JOIN '{base}@exp_new_features' AS e \
           ON m.id = e.id \
         WHERE m.label <> e.label \
         ORDER BY m.id",
        base = table_uri
    );
    let batches_join = ctx.sql(&sql_join).await?.collect().await?;
    assert_eq!(batches_join.len(), 1);
    let batch_join = &batches_join[0];
    assert_eq!(batch_join.num_rows(), 1);

    let ids_j = batch_join
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let main_labels_j = batch_join
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let exp_labels_j = batch_join
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(as_i64_vec(ids_j), vec![2]);
    assert_eq!(as_str_vec(main_labels_j), vec!["dog"]);
    assert_eq!(as_str_vec(exp_labels_j), vec!["cat_exp"]);

    // 7. UNION ALL main vs branch.
    let sql_union = format!(
        "SELECT id, label, score FROM '{base}' \
         UNION ALL \
         SELECT id, label, score FROM '{base}@exp_new_features' \
         ORDER BY id, label, score",
        base = table_uri
    );
    let batches_union = ctx.sql(&sql_union).await?.collect().await?;
    assert_eq!(batches_union.len(), 1);
    let batch_union = &batches_union[0];
    assert_eq!(batch_union.num_rows(), 6);

    Ok(())
}
