// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

mod setup;
use crate::setup::{col, setup_temp_env};
use arrow_array::{Int32Array, Int64Array, StringArray};
use datafusion::error::{DataFusionError, Result};
use futures::StreamExt;
use lance::dataset::builder::DatasetBuilder;
use lance_namespace_impls::DirectoryNamespaceBuilder;

#[tokio::test]
async fn join_within_retail() -> Result<()> {
    let env = setup_temp_env().await?;

    let df = env
        .session
        .sql(
            "SELECT customers.name, orders.amount \
             FROM retail.sales.customers customers \
             JOIN retail.sales.orders orders \
               ON customers.customer_id = orders.customer_id \
             WHERE customers.customer_id = 2",
        )
        .await?;
    let batches = df.collect().await?;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    let name_col = col::<StringArray>(batch, 0);
    let amount_col = col::<Int32Array>(batch, 1);

    assert_eq!(name_col.value(0), "Bob");
    assert_eq!(amount_col.value(0), 200);

    Ok(())
}

#[tokio::test]
async fn join_across_root_catalogs() -> Result<()> {
    let env = setup_temp_env().await?;

    let df = env
        .session
        .sql(
            "SELECT c.name, o2.amount \
             FROM retail.sales.customers c \
             JOIN wholesale.sales2.orders2 o2 \
               ON c.customer_id = o2.customer_id \
             WHERE o2.order_id = 202",
        )
        .await?;
    let batches = df.collect().await?;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    let name_col = col::<StringArray>(batch, 0);
    let amount_col = col::<Int32Array>(batch, 1);

    assert_eq!(name_col.value(0), "Bob");
    assert_eq!(amount_col.value(0), 250);

    Ok(())
}

#[tokio::test]
async fn join_across_catalogs() -> Result<()> {
    let env = setup_temp_env().await?;

    let df = env
        .session
        .sql(
            "SELECT customers.name, dim.segment \
             FROM retail.sales.customers customers \
             JOIN crm.dim.customers_dim dim \
               ON customers.customer_id = dim.customer_id \
             WHERE customers.customer_id = 3",
        )
        .await?;
    let batches = df.collect().await?;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    let name_col = col::<StringArray>(batch, 0);
    let segment_col = col::<StringArray>(batch, 1);

    assert_eq!(name_col.value(0), "Carol");
    assert_eq!(segment_col.value(0), "Platinum");

    Ok(())
}

#[tokio::test]
async fn aggregation_city_totals() -> Result<()> {
    let env = setup_temp_env().await?;

    let df = env
        .session
        .sql(
            "SELECT city, SUM(amount) AS total \
             FROM retail.sales.orders o \
             JOIN retail.sales.customers c \
               ON c.customer_id = o.customer_id \
             GROUP BY city \
             ORDER BY city",
        )
        .await?;
    let batches = df.collect().await?;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 3);

    let city_col = col::<StringArray>(batch, 0);
    let total_col = col::<Int64Array>(batch, 1);

    assert_eq!(city_col.value(0), "LA");
    assert_eq!(total_col.value(0), 300);

    assert_eq!(city_col.value(1), "NY");
    assert_eq!(total_col.value(1), 100);

    assert_eq!(city_col.value(2), "SF");
    assert_eq!(total_col.value(2), 200);

    Ok(())
}

#[tokio::test]
async fn cte_view_customer_orders() -> Result<()> {
    let env = setup_temp_env().await?;

    let df = env
        .session
        .sql(
            "WITH customer_orders AS ( \
                 SELECT c.customer_id, c.name, o.order_id, o.amount \
                 FROM retail.sales.customers c \
                 JOIN retail.sales.orders o \
                   ON c.customer_id = o.customer_id \
             ) \
             SELECT order_id, name, amount FROM customer_orders WHERE customer_id = 1",
        )
        .await?;
    let batches = df.collect().await?;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    let order_id_col = col::<Int32Array>(batch, 0);
    let name_col = col::<StringArray>(batch, 1);
    let amount_col = col::<Int32Array>(batch, 2);

    assert_eq!(order_id_col.value(0), 101);
    assert_eq!(name_col.value(0), "Alice");
    assert_eq!(amount_col.value(0), 100);

    Ok(())
}

#[tokio::test]
async fn insert_into_select_high_value() -> Result<()> {
    let env = setup_temp_env().await?;

    let df = env
        .session
        .sql(
            "INSERT INTO retail.sales.high_value_orders \
             SELECT c.customer_id AS id, c.name, o.amount \
             FROM retail.sales.customers c \
             JOIN retail.sales.orders o \
               ON c.customer_id = o.customer_id \
             WHERE o.amount >= 200",
        )
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    df.collect()
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let dataset = DatasetBuilder::from_namespace(
        Arc::new(
            DirectoryNamespaceBuilder::new(env.root_dir.path().to_string_lossy())
                .manifest_enabled(true)
                .dir_listing_enabled(true)
                .build()
                .await?,
        ),
        vec![
            "retail".to_string(),
            "sales".to_string(),
            "high_value_orders".to_string(),
        ],
        false,
    )
    .await?
    .load()
    .await?;

    let mut scanner = dataset.scan();
    scanner
        .project(&["id", "name", "amount"])
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    let mut stream = scanner
        .try_into_stream()
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let mut rows = Vec::<(i32, String, i32)>::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let id_col = col::<Int32Array>(&batch, 0);
        let name_col = col::<StringArray>(&batch, 1);
        let amount_col = col::<Int32Array>(&batch, 2);

        for i in 0..batch.num_rows() {
            rows.push((
                id_col.value(i),
                name_col.value(i).to_string(),
                amount_col.value(i),
            ));
        }
    }

    assert_eq!(rows.len(), 2);
    assert!(rows.contains(&(2, "Bob".to_string(), 200)));
    assert!(rows.contains(&(3, "Carol".to_string(), 300)));

    Ok(())
}

#[tokio::test]
async fn url_literal_select_customers_by_id() -> Result<()> {
    let env = setup_temp_env().await?;

    let path = env.root_dir.path().join("retail$sales$customers.lance");

    let sql = format!(
        "SELECT name FROM '{}' WHERE customer_id = 2",
        path.to_string_lossy(),
    );

    let df = env.session.sql(&sql).await?;
    let batches = df.collect().await?;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    let name_col = col::<StringArray>(batch, 0);
    assert_eq!(name_col.value(0), "Bob");

    Ok(())
}

#[tokio::test]
async fn url_literal_select_orders_by_amount() -> Result<()> {
    let env = setup_temp_env().await?;

    let path = env.root_dir.path().join("retail$sales$orders.lance");

    let sql = format!(
        "SELECT order_id, amount FROM '{}' WHERE amount >= 200 ORDER BY order_id",
        path.to_string_lossy(),
    );

    let df = env.session.sql(&sql).await?;
    let batches = df.collect().await?;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);

    let order_id_col = col::<Int32Array>(batch, 0);
    let amount_col = col::<Int32Array>(batch, 1);

    assert_eq!(order_id_col.value(0), 102);
    assert_eq!(amount_col.value(0), 200);
    assert_eq!(order_id_col.value(1), 103);
    assert_eq!(amount_col.value(1), 300);

    Ok(())
}

#[tokio::test]
async fn url_literal_join_customers_with_catalog_orders() -> Result<()> {
    let env = setup_temp_env().await?;

    let customers_path = env.root_dir.path().join("retail$sales$customers.lance");

    let sql = format!(
        "SELECT c.name, o.order_id, o.amount \
         FROM '{}' c \
         JOIN retail.sales.orders o \
           ON c.customer_id = o.customer_id \
         WHERE o.amount >= 200 \
         ORDER BY o.order_id",
        customers_path.to_string_lossy(),
    );

    let df = env.session.sql(&sql).await?;
    let batches = df.collect().await?;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);

    let name_col = col::<StringArray>(batch, 0);
    let order_id_col = col::<Int32Array>(batch, 1);
    let amount_col = col::<Int32Array>(batch, 2);

    assert_eq!(name_col.value(0), "Bob");
    assert_eq!(order_id_col.value(0), 102);
    assert_eq!(amount_col.value(0), 200);

    assert_eq!(name_col.value(1), "Carol");
    assert_eq!(order_id_col.value(1), 103);
    assert_eq!(amount_col.value(1), 300);

    Ok(())
}

#[tokio::test]
async fn insert_into_from_url_literal_sources() -> Result<()> {
    let env = setup_temp_env().await?;

    let customers_path = env.root_dir.path().join("retail$sales$customers.lance");
    let orders_path = env.root_dir.path().join("retail$sales$orders.lance");

    let sql = format!(
        "INSERT INTO retail.sales.high_value_orders \
         SELECT c.customer_id AS id, c.name, o.amount \
         FROM '{}' c \
         JOIN '{}' o \
           ON c.customer_id = o.customer_id \
         WHERE o.amount >= 200",
        customers_path.to_string_lossy(),
        orders_path.to_string_lossy(),
    );

    let df = env
        .session
        .sql(&sql)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    df.collect()
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let dataset = DatasetBuilder::from_namespace(
        Arc::new(
            DirectoryNamespaceBuilder::new(env.root_dir.path().to_string_lossy())
                .manifest_enabled(true)
                .dir_listing_enabled(true)
                .build()
                .await?,
        ),
        vec![
            "retail".to_string(),
            "sales".to_string(),
            "high_value_orders".to_string(),
        ],
        false,
    )
    .await?
    .load()
    .await?;

    let mut scanner = dataset.scan();
    scanner
        .project(&["id", "name", "amount"])
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    let mut stream = scanner
        .try_into_stream()
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let mut rows = Vec::<(i32, String, i32)>::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let id_col = col::<Int32Array>(&batch, 0);
        let name_col = col::<StringArray>(&batch, 1);
        let amount_col = col::<Int32Array>(&batch, 2);

        for i in 0..batch.num_rows() {
            rows.push((
                id_col.value(i),
                name_col.value(i).to_string(),
                amount_col.value(i),
            ));
        }
    }

    assert_eq!(rows.len(), 2);
    assert!(rows.contains(&(2, "Bob".to_string(), 200)));
    assert!(rows.contains(&(3, "Carol".to_string(), 300)));

    Ok(())
}
