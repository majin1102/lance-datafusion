// SPDX-License-Identifier: Apache-2.0

use std::path::Path;
use std::sync::Arc;

use arrow_array::{Int32Array, Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use lance::dataset::{WriteMode, WriteParams};
use lance::Dataset;
use lance_datafusion::dml::LanceSession;
use lance_datafusion::{Namespace, SessionBuilder};
use lance_namespace::models::CreateNamespaceRequest;
use lance_namespace::LanceNamespace;
use lance_namespace_impls::DirectoryNamespaceBuilder;
use tempfile::TempDir;

/// Shared test session wrapper used by integration tests.
///
/// It holds the root namespace and the LanceSession built on top of it.
pub struct TestSession {
    pub root_ns: Arc<dyn LanceNamespace>,
    pub session: LanceSession,
}

/// Build customers fixture data.
pub fn customers_data() -> (Arc<Schema>, RecordBatch) {
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

/// Build orders fixture data for the retail.sales.orders table.
pub fn orders_data() -> (Arc<Schema>, RecordBatch) {
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

/// Build orders2 fixture data for the wholesale.sales2.orders2 table.
pub fn orders2_data() -> (Arc<Schema>, RecordBatch) {
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

/// Build customers_dim fixture data for the crm.dim.customers_dim table.
pub fn customers_dim_data() -> (Arc<Schema>, RecordBatch) {
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

/// Build an empty high_value_orders table used as an INSERT target.
pub fn empty_high_value_orders() -> (Arc<Schema>, RecordBatch) {
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

/// Write a single Lance table file under the given directory.
pub async fn write_table_to_dir(
    dir: &Path,
    file_name: &str,
    schema: Arc<Schema>,
    batch: RecordBatch,
) -> Result<()> {
    let full_path = dir.join(file_name);
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

async fn populate_directories_internal(
    root_path: &Path,
    extra_path: &Path,
) -> Result<(Arc<dyn LanceNamespace>, Arc<dyn LanceNamespace>)> {
    // Write all fixture tables under root and extra directories.
    let (customers_schema, customers_batch) = customers_data();
    write_table_to_dir(
        root_path,
        "retail$sales$customers.lance",
        customers_schema,
        customers_batch,
    )
    .await?;

    let (orders_schema, orders_batch) = orders_data();
    write_table_to_dir(
        root_path,
        "retail$sales$orders.lance",
        orders_schema,
        orders_batch,
    )
    .await?;

    let (orders2_schema, orders2_batch) = orders2_data();
    write_table_to_dir(
        root_path,
        "wholesale$sales2$orders2.lance",
        orders2_schema,
        orders2_batch,
    )
    .await?;

    let (high_value_schema, high_value_batch) = empty_high_value_orders();
    write_table_to_dir(
        root_path,
        "retail$sales$high_value_orders.lance",
        high_value_schema,
        high_value_batch,
    )
    .await?;

    let (dim_schema, dim_batch) = customers_dim_data();
    write_table_to_dir(
        extra_path,
        "crm$dim$customers_dim.lance",
        dim_schema,
        dim_batch,
    )
    .await?;

    let root_dir_ns = DirectoryNamespaceBuilder::new(root_path.to_string_lossy().to_string())
        .manifest_enabled(true)
        .dir_listing_enabled(true)
        .build()
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let extra_dir_ns = DirectoryNamespaceBuilder::new(extra_path.to_string_lossy().to_string())
        .manifest_enabled(true)
        .dir_listing_enabled(true)
        .build()
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    // Root namespaces: retail/sales and wholesale/sales2.
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

    // Extra namespaces: crm/dim.
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

    Ok((Arc::new(root_dir_ns), Arc::new(extra_dir_ns)))
}

/// Populate on-disk directories with all fixture tables and namespaces.
///
/// This is suitable for tests that only need to run the CLI against
/// prepared directories without building a LanceSession directly.
pub async fn populate_directories(root_path: &Path, extra_path: &Path) -> Result<()> {
    let _ = populate_directories_internal(root_path, extra_path).await?;
    Ok(())
}

/// Build a LanceSession and root namespace against the given directories.
///
/// This is used by integration tests that work directly with LanceSession.
pub async fn build_session_with_root_and_catalog(
    root_path: &Path,
    extra_path: &Path,
) -> Result<TestSession> {
    let (root_ns, extra_ns) = populate_directories_internal(root_path, extra_path).await?;

    let session = SessionBuilder::new()
        .with_root(Namespace::from_root(Arc::clone(&root_ns)))
        .add_catalog(
            "crm",
            Namespace::from_namespace(Arc::clone(&extra_ns), vec!["crm".to_string()]),
        )
        .build()
        .await?;

    Ok(TestSession { root_ns, session })
}

/// Create temporary root/extra directories, populate them, and build a test session.
///
/// The TempDir values must be kept alive by the caller to ensure the
/// underlying data remains available for the duration of the test.
pub async fn make_temp_env() -> Result<(TempDir, TempDir, TestSession)> {
    let root_dir = TempDir::new()?;
    let extra_dir = TempDir::new()?;

    let session = build_session_with_root_and_catalog(root_dir.path(), extra_dir.path()).await?;

    Ok((root_dir, extra_dir, session))
}
