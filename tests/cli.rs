// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use anyhow::Result;
use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lance::dataset::{WriteMode, WriteParams};
use lance::Dataset;
use lance_namespace::models::CreateNamespaceRequest;
use lance_namespace::LanceNamespace;
use lance_namespace_impls::DirectoryNamespaceBuilder;
use tempfile::TempDir;

fn cli_bin() -> PathBuf {
    if let Some(path) = option_env!("CARGO_BIN_EXE_lance-datafusion") {
        return PathBuf::from(path);
    }
    if let Some(path) = option_env!("CARGO_BIN_EXE_lance_datafusion") {
        return PathBuf::from(path);
    }
    panic!("CLI binary not found via CARGO_BIN_EXE_* environment variables");
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

async fn write_table(
    dir: &TempDir,
    file_name: &str,
    schema: Arc<Schema>,
    batch: RecordBatch,
) -> Result<()> {
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

    Dataset::write(reader, &uri, Some(write_params)).await?;
    Ok(())
}

async fn prepare_lance_directories(root_dir: &TempDir, extra_dir: &TempDir) -> Result<()> {
    // Root namespace: retail.sales.customers
    let (customers_schema, customers_batch) = customers_data();
    write_table(
        root_dir,
        "retail$sales$customers.lance",
        customers_schema,
        customers_batch,
    )
    .await?;

    // Extra catalog: crm.dim.customers_dim
    let (dim_schema, dim_batch) = customers_dim_data();
    write_table(
        extra_dir,
        "crm$dim$customers_dim.lance",
        dim_schema,
        dim_batch,
    )
    .await?;

    // Build directory namespaces and create namespaces
    let root_path = root_dir.path().to_string_lossy().to_string();
    let root_dir_ns = DirectoryNamespaceBuilder::new(root_path)
        .manifest_enabled(true)
        .dir_listing_enabled(true)
        .build()
        .await?;

    let extra_path = extra_dir.path().to_string_lossy().to_string();
    let extra_dir_ns = DirectoryNamespaceBuilder::new(extra_path)
        .manifest_enabled(true)
        .dir_listing_enabled(true)
        .build()
        .await?;

    let mut create_retail = CreateNamespaceRequest::new();
    create_retail.id = Some(vec!["retail".to_string()]);
    root_dir_ns.create_namespace(create_retail).await?;

    let mut create_sales = CreateNamespaceRequest::new();
    create_sales.id = Some(vec!["retail".to_string(), "sales".to_string()]);
    root_dir_ns.create_namespace(create_sales).await?;

    let mut create_crm = CreateNamespaceRequest::new();
    create_crm.id = Some(vec!["crm".to_string()]);
    extra_dir_ns.create_namespace(create_crm).await?;

    let mut create_dim = CreateNamespaceRequest::new();
    create_dim.id = Some(vec!["crm".to_string(), "dim".to_string()]);
    extra_dir_ns.create_namespace(create_dim).await?;

    root_dir_ns.migrate().await?;
    extra_dir_ns.migrate().await?;

    Ok(())
}

#[tokio::test]
async fn yaml_root_and_catalog_select() -> Result<()> {
    let root_dir = TempDir::new()?;
    let extra_dir = TempDir::new()?;

    prepare_lance_directories(&root_dir, &extra_dir).await?;

    // Write YAML config
    let yaml = format!(
        concat!(
            "lance.root.type: directory\n",
            "lance.root.path: {}\n",
            "lance.catalog.crm.type: directory\n",
            "lance.catalog.crm.path: {}\n",
        ),
        root_dir.path().to_string_lossy(),
        extra_dir.path().to_string_lossy(),
    );

    let yaml_file = tempfile::NamedTempFile::new()?;
    std::fs::write(yaml_file.path(), yaml)?;

    let cli = cli_bin();

    let output = Command::new(cli)
        .arg("--config")
        .arg(yaml_file.path())
        .arg("--format")
        .arg("csv")
        .arg("--command")
        .arg("SELECT name FROM retail.sales.customers WHERE customer_id = 2;")
        .output()
        .expect("failed to run CLI");

    assert!(
        output.status.success(),
        "CLI failed: stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("name"));
    assert!(stdout.contains("Bob"));

    Ok(())
}

#[tokio::test]
async fn yaml_crm_catalog_select() -> Result<()> {
    let root_dir = TempDir::new()?;
    let extra_dir = TempDir::new()?;

    prepare_lance_directories(&root_dir, &extra_dir).await?;

    // Write YAML config
    let yaml = format!(
        concat!(
            "lance.root.type: directory\n",
            "lance.root.path: {}\n",
            "lance.catalog.crm.type: directory\n",
            "lance.catalog.crm.path: {}\n",
        ),
        root_dir.path().to_string_lossy(),
        extra_dir.path().to_string_lossy(),
    );

    let yaml_file = tempfile::NamedTempFile::new()?;
    std::fs::write(yaml_file.path(), yaml)?;

    let cli = cli_bin();

    let output = Command::new(cli)
        .arg("--config")
        .arg(yaml_file.path())
        .arg("--format")
        .arg("csv")
        .arg("--command")
        .arg("SELECT segment FROM crm.dim.customers_dim WHERE customer_id = 3;")
        .output()
        .expect("failed to run CLI");

    assert!(
        output.status.success(),
        "CLI failed: stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("segment"));
    assert!(stdout.contains("Platinum"));

    Ok(())
}

#[test]
fn command_mode_simple_select() -> Result<()> {
    let cli = cli_bin();

    let output = Command::new(cli)
        .arg("--format")
        .arg("csv")
        .arg("--command")
        .arg("SELECT 1 + 2 AS sum;")
        .output()?;

    assert!(
        output.status.success(),
        "CLI failed: stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("sum"));
    assert!(stdout.contains("3"));

    Ok(())
}

#[test]
fn no_lance_config_default_session() -> Result<()> {
    let cli = cli_bin();

    let output = Command::new(cli)
        .arg("--format")
        .arg("csv")
        .arg("--command")
        .arg("SELECT 42 AS answer;")
        .output()?;

    assert!(
        output.status.success(),
        "CLI failed: stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("answer"));
    assert!(stdout.contains("42"));

    Ok(())
}
