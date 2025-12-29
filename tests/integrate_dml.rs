// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow_array::{Int32Array, Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use lance::dataset::{WriteMode, WriteParams};
use lance::Dataset;
use lance_datafusion::{Namespace, SessionBuilder};
use lance_namespace::models::{CreateNamespaceRequest, DescribeTableRequest, RegisterTableRequest};
use lance_namespace::LanceNamespace;
use lance_namespace_impls::ConnectBuilder;
use tempfile::TempDir;

struct NamespaceTestContext {
    root_dir: TempDir,
    extra_dir: TempDir,
    root_ns: Arc<dyn LanceNamespace>,
    extra_ns: Arc<dyn LanceNamespace>,
    ctx: SessionContext,
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
    .expect("failed to build customers batch");

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
    .expect("failed to build orders batch");

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
    .expect("failed to build customers_dim batch");

    (schema, batch)
}

async fn write_table(
    dir: &TempDir,
    relative_path: &str,
    schema: Arc<Schema>,
    batch: RecordBatch,
) -> DFResult<()> {
    let full_path = dir.path().join(relative_path);
    if let Some(parent) = full_path.parent() {
        std::fs::create_dir_all(parent).expect("failed to create parent dirs for table");
    }

    let uri = full_path
        .to_str()
        .expect("failed to convert table path to string")
        .to_string();

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

async fn register_table(
    ns: &Arc<dyn LanceNamespace>,
    id: Vec<String>,
    location: &str,
) -> DFResult<()> {
    let mut req = RegisterTableRequest::new(location.to_string());
    req.id = Some(id);

    ns.register_table(req)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    Ok(())
}

async fn setup_test_context() -> DFResult<NamespaceTestContext> {
    let root_dir = TempDir::new().expect("failed to create root temp dir");
    let extra_dir = TempDir::new().expect("failed to create extra temp dir");

    let root_path = root_dir.path().to_string_lossy().to_string();
    let extra_path = extra_dir.path().to_string_lossy().to_string();

    let root_ns = ConnectBuilder::new("dir")
        .property("root", root_path)
        .connect()
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let extra_ns = ConnectBuilder::new("dir")
        .property("root", extra_path)
        .connect()
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    // retail.sales under root_ns
    let mut create_retail = CreateNamespaceRequest::new();
    create_retail.id = Some(vec!["retail".to_string()]);
    root_ns
        .create_namespace(create_retail)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let mut create_sales = CreateNamespaceRequest::new();
    create_sales.id = Some(vec!["retail".to_string(), "sales".to_string()]);
    root_ns
        .create_namespace(create_sales)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    // crm.dim under extra_ns
    let mut create_crm = CreateNamespaceRequest::new();
    create_crm.id = Some(vec!["crm".to_string()]);
    extra_ns
        .create_namespace(create_crm)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let mut create_dim = CreateNamespaceRequest::new();
    create_dim.id = Some(vec!["crm".to_string(), "dim".to_string()]);
    extra_ns
        .create_namespace(create_dim)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    // Write and register retail.sales.customers
    let (customers_schema, customers_batch) = customers_data();
    write_table(
        &root_dir,
        "retail/sales/customers.lance",
        customers_schema,
        customers_batch,
    )
    .await?;
    register_table(
        &root_ns,
        vec!["retail".to_string(), "sales".to_string(), "customers".to_string()],
        "retail/sales/customers.lance",
    )
    .await?;

    // Write and register retail.sales.orders
    let (orders_schema, orders_batch) = orders_data();
    write_table(
        &root_dir,
        "retail/sales/orders.lance",
        orders_schema,
        orders_batch,
    )
    .await?;
    register_table(
        &root_ns,
        vec!["retail".to_string(), "sales".to_string(), "orders".to_string()],
        "retail/sales/orders.lance",
    )
    .await?;

    // Write and register crm.dim.customers_dim
    let (dim_schema, dim_batch) = customers_dim_data();
    write_table(
        &extra_dir,
        "crm/dim/customers_dim.lance",
        dim_schema,
        dim_batch,
    )
    .await?;
    register_table(
        &extra_ns,
        vec!["crm".to_string(), "dim".to_string(), "customers_dim".to_string()],
        "crm/dim/customers_dim.lance",
    )
    .await?;

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
        extra_dir,
        root_ns,
        extra_ns,
        ctx,
    })
}

#[tokio::test]
async fn join_within_retail() -> DFResult<()> {
    let ns = setup_test_context().await?;

    let df = ns
        .ctx
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

    let name_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name column should be StringArray");
    let amount_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("amount column should be Int32Array");

    assert_eq!(name_col.value(0), "Bob");
    assert_eq!(amount_col.value(0), 200);

    Ok(())
}

#[tokio::test]
async fn join_across_catalogs() -> DFResult<()> {
    let ns = setup_test_context().await?;

    let df = ns
        .ctx
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

    let name_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name column should be StringArray");
    let segment_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("segment column should be StringArray");

    assert_eq!(name_col.value(0), "Carol");
    assert_eq!(segment_col.value(0), "Platinum");

    Ok(())
}

#[tokio::test]
async fn aggregation_city_totals() -> DFResult<()> {
    let ns = setup_test_context().await?;

    let df = ns
        .ctx
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

    let city_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("city column should be StringArray");
    let total_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("total column should be Int64Array");

    assert_eq!(city_col.value(0), "LA");
    assert_eq!(total_col.value(0), 300);

    assert_eq!(city_col.value(1), "NY");
    assert_eq!(total_col.value(1), 100);

    assert_eq!(city_col.value(2), "SF");
    assert_eq!(total_col.value(2), 200);

    Ok(())
}

#[tokio::test]
async fn cte_view_customer_orders() -> DFResult<()> {
    let ns = setup_test_context().await?;

    let df = ns
        .ctx
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

    let order_id_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("order_id column should be Int32Array");
    let name_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name column should be StringArray");
    let amount_col = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("amount column should be Int32Array");

    assert_eq!(order_id_col.value(0), 101);
    assert_eq!(name_col.value(0), "Alice");
    assert_eq!(amount_col.value(0), 100);

    Ok(())
}

#[tokio::test]
async fn insert_into_select_high_value() -> DFResult<()> {
    let ns = setup_test_context().await?;

    // Create empty high_value_orders table under retail.sales via Dataset.write and manifest registration
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("amount", DataType::Int32, false),
    ]));
    let empty_ids = Int32Array::from(Vec::<i32>::new());
    let empty_names = StringArray::from(Vec::<&str>::new());
    let empty_amounts = Int32Array::from(Vec::<i32>::new());
    let empty_batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(empty_ids), Arc::new(empty_names), Arc::new(empty_amounts)],
    )
    .expect("failed to build empty batch for high_value_orders");

    let relative_path = "retail/sales/high_value_orders.lance";
    write_table(&ns.root_dir, relative_path, schema, empty_batch).await?;

    register_table(
        &ns.root_ns,
        vec![
            "retail".to_string(),
            "sales".to_string(),
            "high_value_orders".to_string(),
        ],
        relative_path,
    )
    .await?;

    // Run INSERT INTO ... SELECT using DML helper
    lance_datafusion::dml::execute_sql(
        &ns.ctx,
        "INSERT INTO retail.sales.high_value_orders \
         SELECT c.customer_id AS id, c.name, o.amount \
         FROM retail.sales.customers c \
         JOIN retail.sales.orders o \
           ON c.customer_id = o.customer_id \
         WHERE o.amount >= 200",
    )
    .await
    .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    // Resolve table URI via namespace
    let mut describe_req = DescribeTableRequest::new();
    describe_req.id = Some(vec![
        "retail".to_string(),
        "sales".to_string(),
        "high_value_orders".to_string(),
    ]);
    let describe_resp = ns
        .root_ns
        .describe_table(describe_req)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    let table_uri = describe_resp
        .table_uri
        .or(describe_resp.location)
        .expect("high_value_orders table must have a URI");

    let dataset = Dataset::open(&table_uri)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

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
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id column should be Int32Array");
        let name_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column should be StringArray");
        let amount_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("amount column should be Int32Array");

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
