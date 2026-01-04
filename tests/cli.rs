// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;
use std::process::Command;

use anyhow::Result;
use tempfile::TempDir;

mod setup;

fn cli_bin() -> PathBuf {
    if let Some(path) = option_env!("CARGO_BIN_EXE_lance-datafusion") {
        return PathBuf::from(path);
    }
    if let Some(path) = option_env!("CARGO_BIN_EXE_lance_datafusion") {
        return PathBuf::from(path);
    }
    panic!("CLI binary not found via CARGO_BIN_EXE_* environment variables");
}

#[tokio::test]
async fn yaml_root_and_catalog_select() -> Result<()> {
    let root_dir = TempDir::new()?;
    let extra_dir = TempDir::new()?;

    setup::populate_directories(root_dir.path(), extra_dir.path()).await?;

    // Write YAML config
    let yaml = format!(
        concat!(
            "lance.namespace.root.type: directory\n",
            "lance.namespace.root.path: {}\n",
            "lance.namespace.catalog.crm.type: directory\n",
            "lance.namespace.catalog.crm.path: {}\n",
            "lance.namespace.catalog.crm.id: crm\n",
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
async fn yaml_namespace_crm_select() -> Result<()> {
    let root_dir = TempDir::new()?;
    let extra_dir = TempDir::new()?;

    setup::populate_directories(root_dir.path(), extra_dir.path()).await?;

    // Write YAML config
    let yaml = format!(
        concat!(
            "lance.namespace.root.type: directory\n",
            "lance.namespace.root.path: {}\n",
            "lance.namespace.catalog.crm.type: directory\n",
            "lance.namespace.catalog.crm.path: {}\n",
            "lance.namespace.catalog.crm.id: crm\n",
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
