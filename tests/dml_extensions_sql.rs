// SPDX-License-Identifier: Apache-2.0

mod setup;
use crate::setup::{col, setup_temp_env};
use arrow_array::{Int32Array, Int64Array, StringArray};
use datafusion::error::{DataFusionError, Result as DFResult};
use futures::StreamExt;
use lance::Dataset;

#[tokio::test]
async fn delete_single_order_reports_count_and_removes_row() -> DFResult<()> {
    let env = setup_temp_env().await?;

    let df = env
        .session
        .execute("DELETE FROM retail.sales.orders WHERE order_id = 102")
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let batches = df
        .collect()
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    assert_eq!(batches.len(), 1);

    let batch = &batches[0];
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.num_rows(), 1);

    let count_col = col::<Int64Array>(batch, 0);
    assert_eq!(count_col.value(0), 1);

    let orders_path = env.root_dir.path().join("retail$sales$orders.lance");
    let orders_uri = orders_path.to_string_lossy();

    let dataset = Dataset::open(&orders_uri)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let mut scanner = dataset.scan();
    scanner
        .project(&["order_id", "customer_id", "amount"])
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    let mut stream = scanner
        .try_into_stream()
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let mut rows = Vec::<(i32, i32, i32)>::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let order_id_col = col::<Int32Array>(&batch, 0);
        let customer_id_col = col::<Int32Array>(&batch, 1);
        let amount_col = col::<Int32Array>(&batch, 2);

        for i in 0..batch.num_rows() {
            rows.push((
                order_id_col.value(i),
                customer_id_col.value(i),
                amount_col.value(i),
            ));
        }
    }

    assert_eq!(rows.len(), 2);
    assert!(rows.contains(&(101, 1, 100)));
    assert!(rows.contains(&(103, 3, 300)));

    Ok(())
}

#[tokio::test]
async fn update_customer_city_reports_count_and_updates_value() -> DFResult<()> {
    let env = setup_temp_env().await?;

    let df = env
        .session
        .execute("UPDATE retail.sales.customers SET city = 'SEA' WHERE customer_id = 2")
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let batches = df
        .collect()
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    assert_eq!(batches.len(), 1);

    let batch = &batches[0];
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.num_rows(), 1);

    let count_col = col::<Int64Array>(batch, 0);
    assert_eq!(count_col.value(0), 1);

    let customers_path = env.root_dir.path().join("retail$sales$customers.lance");
    let customers_uri = customers_path.to_string_lossy();

    let dataset = Dataset::open(&customers_uri)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let mut scanner = dataset.scan();
    scanner
        .project(&["customer_id", "name", "city"])
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    let mut stream = scanner
        .try_into_stream()
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let mut rows = Vec::<(i32, String, String)>::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let id_col = col::<Int32Array>(&batch, 0);
        let name_col = col::<StringArray>(&batch, 1);
        let city_col = col::<StringArray>(&batch, 2);

        for i in 0..batch.num_rows() {
            rows.push((
                id_col.value(i),
                name_col.value(i).to_string(),
                city_col.value(i).to_string(),
            ));
        }
    }

    assert_eq!(rows.len(), 3);
    assert!(rows.contains(&(1, "Alice".to_string(), "NY".to_string())));
    assert!(rows.contains(&(2, "Bob".to_string(), "SEA".to_string())));
    assert!(rows.contains(&(3, "Carol".to_string(), "LA".to_string())));

    Ok(())
}

#[tokio::test]
async fn merge_orders_reports_count_and_inserts_new_rows() -> DFResult<()> {
    let env = setup_temp_env().await?;

    let df = env.session.execute(
        "MERGE INTO retail.sales.orders AS t \
         USING wholesale.sales2.orders2 AS s \
         ON t.order_id = s.order_id \
         WHEN MATCHED THEN UPDATE SET amount = s.amount \
         WHEN NOT MATCHED THEN INSERT (order_id, customer_id, amount) VALUES (s.order_id, s.customer_id, s.amount)",
    )
    .await
    .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let batches = df
        .collect()
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    assert_eq!(batches.len(), 1);

    let batch = &batches[0];
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.num_rows(), 1);

    let count_col = col::<Int64Array>(batch, 0);
    assert_eq!(count_col.value(0), 2);

    let orders_path = env.root_dir.path().join("retail$sales$orders.lance");
    let orders_uri = orders_path.to_string_lossy();

    let dataset = Dataset::open(&orders_uri)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let mut scanner = dataset.scan();
    scanner
        .project(&["order_id", "customer_id", "amount"])
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    let mut stream = scanner
        .try_into_stream()
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let mut rows = Vec::<(i32, i32, i32)>::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let order_id_col = col::<Int32Array>(&batch, 0);
        let customer_id_col = col::<Int32Array>(&batch, 1);
        let amount_col = col::<Int32Array>(&batch, 2);

        for i in 0..batch.num_rows() {
            rows.push((
                order_id_col.value(i),
                customer_id_col.value(i),
                amount_col.value(i),
            ));
        }
    }

    assert_eq!(rows.len(), 5);
    assert!(rows.contains(&(101, 1, 100)));
    assert!(rows.contains(&(102, 2, 200)));
    assert!(rows.contains(&(103, 3, 300)));
    assert!(rows.contains(&(201, 1, 150)));
    assert!(rows.contains(&(202, 2, 250)));

    Ok(())
}
