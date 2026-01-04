// SPDX-License-Identifier: Apache-2.0

use std::path::{Path, PathBuf};
use std::sync::Arc;


use arrow_array::{Int32Array, Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use futures::StreamExt;
use lance::dataset::{WriteMode, WriteParams};
use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance_datafusion::{Namespace, SessionBuilder};
use lance_namespace::models::{CreateNamespaceRequest, DescribeTableRequest};
use lance_namespace::LanceNamespace;
use lance_namespace_impls::DirectoryNamespaceBuilder;
use tempfile::TempDir;
use lance_datafusion::dml::LanceSession;


// 修改后：使用 PathBuf 存储指定目录
struct TestSession {
    _root_dir: PathBuf,  // 改为 PathBuf
    _extra_dir: PathBuf, // 改为 PathBuf
    root_ns: Arc<dyn LanceNamespace>,
    session: LanceSession,
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
        vec![
            Arc::new(order_ids),
            Arc::new(customer_ids),
            Arc::new(amounts),
        ],
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
        vec![
            Arc::new(order_ids),
            Arc::new(customer_ids),
            Arc::new(amounts),
        ],
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
    dir: &Path,  // 兼容 PathBuf
    file_name: &str,
    schema: Arc<Schema>,
    batch: RecordBatch,
) -> Result<()> {
    let full_path = dir.join(file_name);  // 原 dir.path().join() 简化为 dir.join()
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
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;  // 补全原代码中的错误处理

    Ok(())
}

async fn setup_context() -> Result<TestSession> {
    // 修改前：临时目录
    // let root_dir = TempDir::new()?;
    // let extra_dir = TempDir::new()?;

    // 修改后：指定目录（替换为你的实际路径，示例为 /tmp 下的临时目录）
    let root_dir = PathBuf::from("/tmp/lance_test_root");
    let extra_dir = PathBuf::from("/tmp/lance_test_extra");
    std::fs::create_dir_all(&root_dir)?;  // 确保目录存在
    std::fs::create_dir_all(&extra_dir)?;

    // 原有逻辑保持不变（仅路径获取方式简化）
    let (customers_schema, customers_batch) = customers_data();
    write_table(
        &root_dir,  // 直接传入 PathBuf 的引用（自动转换为 &Path）
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

    let root_path = root_dir.to_string_lossy().to_string();  // PathBuf 直接转换
    let root_dir_ns = DirectoryNamespaceBuilder::new(root_path)
        .manifest_enabled(true)
        .dir_listing_enabled(true)
        .build()
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let extra_path = extra_dir.to_string_lossy().to_string();
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

    let session = SessionBuilder::new()
        .with_root(Namespace::from_root(Arc::clone(&root_ns)))
        .add_catalog(
            "crm",
            Namespace::from_namespace(Arc::clone(&extra_ns), vec!["crm".to_string()]),
        )
        .build()
        .await?;

    Ok(TestSession {
        _root_dir: root_dir,  // 存储 PathBuf
        _extra_dir: extra_dir,
        root_ns,
        session,
    })
}

#[tokio::test]
async fn join_within_retail() -> Result<()> {
    let context = setup_context().await?;

    let df = context.session
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
    let context = setup_context().await?;

    let df = context.session
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
    let context = setup_context().await?;

    let df = context.session
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
    let context = setup_context().await?;

    let df = context.session
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
    let context = setup_context().await?;

    let df = context.session
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
    let context = setup_context().await?;

    let df = context.session.sql(
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

    let mut describe_req = DescribeTableRequest::new();
    describe_req.id = Some(vec![
        "retail".to_string(),
        "sales".to_string(),
        "high_value_orders".to_string(),
    ]);

    let dataset = DatasetBuilder::from_namespace(
        Arc::clone(&context.root_ns),
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
