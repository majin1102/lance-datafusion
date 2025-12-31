// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;


use arrow_array::{Int32Array, Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result as DFResult};
use futures::StreamExt;
use lance::dataset::{WriteMode, WriteParams};
use lance::Dataset;
use lance_datafusion::dml::LanceSession;
use lance_datafusion::{Namespace, SessionBuilder};
use lance_namespace::models::CreateNamespaceRequest;
use lance_namespace::LanceNamespace;
use lance_namespace_impls::DirectoryNamespaceBuilder;
use tempfile::TempDir;

struct NamespaceTestContext {
    root_dir: TempDir,
    _extra_dir: TempDir,
    _root_ns: Arc<dyn LanceNamespace>,
    ctx: LanceSession,
}

fn col<T: 'static>(batch: &RecordBatch, idx: usize) -> &T {
    batch.column(idx).as_any().downcast_ref::<T>().unwrap()
}

fn customers_data() -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, false),
    ]));

    let customer_ids = Int32Array::from(vec![1, 2, 3]);
    let names = StringArray::from(vec!["Alice", "Bob", "Carol"]);
    let cities = StringArray::from(vec!["NY", "SF", "LA"]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(customer_ids), Arc::new(names), Arc::new(cities)],
    )
    .unwrap();

    (schema, batch)
}

fn orders_data() -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int32, false),
        Field::new("customer_id", DataType::Int32, false),
        Field::new("amount", DataType::Int32, false),
    ]));

    let order_ids = Int32Array::from(vec![101, 102, 103]);
    let customer_ids = Int32Array::from(vec![1, 2, 3]);
    let amounts = Int32Array::from(vec![100, 200, 300]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(order_ids), Arc::new(customer_ids), Arc::new(amounts)],
    )
    .unwrap();

    (schema, batch)
}

fn orders2_data() -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int32, false),
        Field::new("customer_id", DataType::Int32, false),
        Field::new("amount", DataType::Int32, false),
    ]));

    let order_ids = Int32Array::from(vec![201, 202]);
    let customer_ids = Int32Array::from(vec![1, 2]);
    let amounts = Int32Array::from(vec![150, 250]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(order_ids), Arc::new(customer_ids), Arc::new(amounts)],
    )
    .unwrap();

    (schema, batch)
}

fn customers_dim_data() -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int32, false),
        Field::new("segment", DataType::Utf8, false),
    ]));

    let customer_ids = Int32Array::from(vec![1, 2, 3]);
    let segments = StringArray::from(vec!["Silver", "Gold", "Platinum"]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(customer_ids), Arc::new(segments)],
    )
    .unwrap();

    (schema, batch)
}

fn empty_high_value_orders() -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("amount", DataType::Int32, false),
    ]));

    let ids = Int32Array::from(Vec::<i32>::new());
    let names = StringArray::from(Vec::<&str>::new());
    let amounts = Int32Array::from(Vec::<i32>::new());

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(ids), Arc::new(names), Arc::new(amounts)],
    )
    .unwrap();

    (schema, batch)
}

async fn write_table(
    dir: &TempDir,
    file_name: &str,
    schema: Arc<Schema>,
    batch: RecordBatch,
) -> DFResult<()> {
    let full_path = dir.path().join(file_name);
    if let Some(parent) = full_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let uri = full_path.to_str().unwrap().to_string();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let write_params = WriteParams {
        mode: WriteMode::Create,
        ..Default::default()
    };

    Dataset::write(reader, &uri, Some(write_params))
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    Ok(())
}

async fn setup_test_context() -> DFResult<NamespaceTestContext> {
    let root_dir = TempDir::new()?;
    let extra_dir = TempDir::new()?;

    let (customers_schema, customers_batch) = customers_data();
    write_table(
        &root_dir,
        "retail$sales$customers.lance",
        customers_schema,
        customers_batch,
    )
    .await?;

    let (orders_schema, orders_batch) = orders_data();
    write_table(
        &root_dir,
        "retail$sales$orders.lance",
        orders_schema,
        orders_batch,
    )
    .await?;

    let (orders2_schema, orders2_batch) = orders2_data();
    write_table(
        &root_dir,
        "wholesale$sales2$orders2.lance",
        orders2_schema,
        orders2_batch,
    )
    .await?;

    let (high_value_schema, high_value_batch) = empty_high_value_orders();
    write_table(
        &root_dir,
        "retail$sales$high_value_orders.lance",
        high_value_schema,
        high_value_batch,
    )
    .await?;

    let (dim_schema, dim_batch) = customers_dim_data();
    write_table(
        &extra_dir,
        "crm$dim$customers_dim.lance",
        dim_schema,
        dim_batch,
    )
    .await?;

    let root_path = root_dir.path().to_string_lossy().to_string();
    let root_dir_ns = DirectoryNamespaceBuilder::new(root_path)
        .manifest_enabled(true)
        .dir_listing_enabled(true)
        .build()
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let extra_path = extra_dir.path().to_string_lossy().to_string();
    let extra_dir_ns = DirectoryNamespaceBuilder::new(extra_path)
        .manifest_enabled(true)
        .dir_listing_enabled(true)
        .build()
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let mut create_retail = CreateNamespaceRequest::new();
    create_retail.id = Some(vec!["retail".to_string()]);
    root_dir_ns
        .create_namespace(create_retail)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let mut create_sales = CreateNamespaceRequest::new();
    create_sales.id = Some(vec!["retail".to_string(), "sales".to_string()]);
    root_dir_ns
        .create_namespace(create_sales)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let mut create_wholesale = CreateNamespaceRequest::new();
    create_wholesale.id = Some(vec!["wholesale".to_string()]);
    root_dir_ns
        .create_namespace(create_wholesale)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let mut create_sales2 = CreateNamespaceRequest::new();
    create_sales2.id = Some(vec!["wholesale".to_string(), "sales2".to_string()]);
    root_dir_ns
        .create_namespace(create_sales2)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let mut create_crm = CreateNamespaceRequest::new();
    create_crm.id = Some(vec!["crm".to_string()]);
    extra_dir_ns
        .create_namespace(create_crm)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let mut create_dim = CreateNamespaceRequest::new();
    create_dim.id = Some(vec!["crm".to_string(), "dim".to_string()]);
    extra_dir_ns
        .create_namespace(create_dim)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    root_dir_ns
        .migrate()
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    extra_dir_ns
        .migrate()
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let root_ns: Arc<dyn LanceNamespace> = Arc::new(root_dir_ns);
    let extra_ns: Arc<dyn LanceNamespace> = Arc::new(extra_dir_ns);

    let ctx = SessionBuilder::new()
        .with_root(Namespace::from_root(Arc::clone(&root_ns)))
        .add_catalog(
            "crm",
            Namespace::from_namespace(Arc::clone(&extra_ns), vec!["crm".to_string()]),
        )
        .build()
        .await?;

    Ok(NamespaceTestContext {
        root_dir,
        _extra_dir: extra_dir,
        _root_ns: root_ns,
        ctx,
    })
}

#[tokio::test]
async fn delete_single_order_reports_count_and_removes_row() -> DFResult<()> {
    let ns = setup_test_context().await?;

    let df = ns.ctx.execute(
        "DELETE FROM retail.sales.orders WHERE order_id = 102",
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
    assert_eq!(count_col.value(0), 1);

    let orders_path = ns
        .root_dir
        .path()
        .join("retail$sales$orders.lance");
    let orders_uri = orders_path.to_string_lossy().to_string();

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
    let ns = setup_test_context().await?;

    let df = ns.ctx.execute(
        "UPDATE retail.sales.customers SET city = 'SEA' WHERE customer_id = 2",
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
    assert_eq!(count_col.value(0), 1);

    let customers_path = ns
        .root_dir
        .path()
        .join("retail$sales$customers.lance");
    let customers_uri = customers_path.to_string_lossy().to_string();

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
    let ns = setup_test_context().await?;

    let df = ns.ctx.execute(
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

    let orders_path = ns
        .root_dir
        .path()
        .join("retail$sales$orders.lance");
    let orders_uri = orders_path.to_string_lossy().to_string();

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
