// SPDX-License-Identifier: Apache-2.0

//! Command line entrypoint for running DataFusion with Lance namespace integration.
//!
//! This binary provides a lightweight CLI that is behaviorally close to
//! `datafusion-cli`, with additional support for injecting Lance namespaces
//! via `lance.*` configuration keys.

use std::collections::{HashMap, HashSet};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use arrow_array::{
    Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, RecordBatch, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::DataType;
use clap::{Parser, ValueEnum};
use datafusion::dataframe::DataFrame;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::context::{SessionConfig, SessionContext};
use lance_namespace::LanceNamespace;
use lance_namespace_impls::DirectoryNamespaceBuilder;
use serde_yaml::Value as YamlValue;

use lance_datafusion::dml::LanceSession;
use lance_datafusion::{Namespace, SessionBuilder};

/// Command line arguments
#[derive(Parser, Debug)]
#[command(
    name = "lance-datafusion-cli",
    about = "DataFusion CLI with Lance namespace integration",
    version
)]
struct CliArgs {
    /// Configuration key-value pairs, e.g. --conf lance.root.path=/data
    #[arg(long = "conf")]
    conf: Vec<String>,

    /// YAML configuration file path
    #[arg(long = "config")]
    config: Option<PathBuf>,

    /// Execute the given SQL command(s) then exit (can be used multiple times)
    #[arg(short = 'c', long = "command")]
    command: Vec<String>,

    /// Execute commands from file(s) then exit (can be used multiple times)
    #[arg(short = 'f', long = "file")]
    file: Vec<PathBuf>,

    /// Output format: table (default), csv, json
    #[arg(long = "format", value_enum, default_value_t = OutputFormat::Table)]
    format: OutputFormat,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum OutputFormat {
    Table,
    Csv,
    Json,
}

/// Execution item from the command line, preserving user-specified order.
#[derive(Debug)]
enum ExecItem {
    Command(String),
    File(PathBuf),
}

/// Execution engine: plain DataFusion or Lance-extended session.
enum Engine {
    Plain(SessionContext),
    Lance(LanceSession),
}

impl Engine {
    async fn sql(&self, sql: &str) -> DataFusionResult<DataFrame> {
        match self {
            Engine::Plain(ctx) => ctx.sql(sql).await,
            Engine::Lance(session) => session.sql(sql).await,
        }
    }
}

#[derive(Debug, Clone)]
struct DirectoryNamespaceConfig {
    path: String,
}

#[derive(Debug, Default, Clone)]
struct LanceNamespaceConfig {
    root: Option<DirectoryNamespaceConfig>,
    catalogs: HashMap<String, DirectoryNamespaceConfig>,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    // Capture raw args first so we can reconstruct execution order
    let raw_args: Vec<String> = std::env::args().collect();
    let exec_sequence = parse_exec_sequence(&raw_args);

    let args = CliArgs::parse();

    // Load configuration from YAML and --conf
    let file_kv = if let Some(ref path) = args.config {
        load_yaml_key_values(path)
            .with_context(|| format!("failed to load YAML config from {}", path.display()))?
    } else {
        HashMap::new()
    };

    let cli_kv = parse_conf_args(&args.conf)?;
    let merged_kv = merge_maps(file_kv, cli_kv);

    let (lance_kv, df_kv) = partition_lance_and_df_options(merged_kv);
    let lance_cfg = parse_lance_namespace_config(&lance_kv)?;
    let session_config = build_session_config(&df_kv)?;

    let engine = build_engine(lance_cfg, session_config).await?;

    if !exec_sequence.is_empty() {
        for item in exec_sequence {
            match item {
                ExecItem::Command(sql) => {
                    execute_sql_script(&engine, &sql, args.format).await?;
                }
                ExecItem::File(path) => {
                    let contents = tokio::fs::read_to_string(&path)
                        .await
                        .with_context(|| format!("failed to read SQL file {}", path.display()))?;
                    execute_sql_script(&engine, &contents, args.format).await?;
                }
            }
        }
        Ok(())
    } else {
        run_repl(&engine, args.format).await
    }
}

/// Parse key=value pairs from --conf flags.
fn parse_conf_args(args: &[String]) -> Result<HashMap<String, String>> {
    let mut kv = HashMap::new();
    for raw in args {
        let (key, value) = raw
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("invalid --conf '{raw}', expected key=value"))?;
        kv.insert(key.trim().to_string(), value.trim().to_string());
    }
    Ok(kv)
}

/// Load a simple mapping from a YAML file.
///
/// Expected structure is a flat mapping, e.g.:
///
/// ```yaml
/// lance.root.type: directory
/// lance.root.path: /path/to/data
/// lance.catalog.crm.type: directory
/// lance.catalog.crm.path: /path/to/crm
/// datafusion.execution.batch_size: 1024
/// ```
fn load_yaml_key_values(path: &Path) -> Result<HashMap<String, String>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read YAML file {}", path.display()))?;
    let value: YamlValue = serde_yaml::from_str(&text)
        .with_context(|| format!("failed to parse YAML from {}", path.display()))?;

    let mut kv = HashMap::new();

    match value {
        YamlValue::Mapping(map) => {
            for (k, v) in map {
                let key = k
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("YAML keys must be strings"))?;
                let val_str = yaml_scalar_to_string(&v).ok_or_else(|| {
                    anyhow::anyhow!(
                        "YAML value for key '{}' must be a scalar (string/number/bool)",
                        key
                    )
                })?;
                kv.insert(key.to_string(), val_str);
            }
        }
        other => {
            bail!("YAML top-level must be a mapping, got {other:?}");
        }
    }

    Ok(kv)
}

fn yaml_scalar_to_string(value: &YamlValue) -> Option<String> {
    match value {
        YamlValue::Null => Some(String::new()),
        YamlValue::Bool(b) => Some(b.to_string()),
        YamlValue::Number(n) => Some(n.to_string()),
        YamlValue::String(s) => Some(s.clone()),
        _ => None,
    }
}

fn merge_maps(mut base: HashMap<String, String>, override_map: HashMap<String, String>) -> HashMap<String, String> {
    for (k, v) in override_map {
        base.insert(k, v);
    }
    base
}

fn partition_lance_and_df_options(
    kv: HashMap<String, String>,
) -> (HashMap<String, String>, HashMap<String, String>) {
    let mut lance = HashMap::new();
    let mut df = HashMap::new();

    for (k, v) in kv {
        if k.starts_with("lance.") {
            lance.insert(k, v);
        } else {
            df.insert(k, v);
        }
    }

    (lance, df)
}

fn parse_lance_namespace_config(kv: &HashMap<String, String>) -> Result<Option<LanceNamespaceConfig>> {
    if kv.is_empty() {
        return Ok(None);
    }

    let mut root_type: Option<String> = None;
    let mut root_path: Option<String> = None;
    let mut catalog_types: HashMap<String, String> = HashMap::new();
    let mut catalog_paths: HashMap<String, String> = HashMap::new();

    for (key, value) in kv {
        if let Some(suffix) = key.strip_prefix("lance.root.") {
            match suffix {
                "type" => root_type = Some(value.clone()),
                "path" => root_path = Some(value.clone()),
                _ => {}
            }
        } else if let Some(rest) = key.strip_prefix("lance.catalog.") {
            if let Some((name, attr)) = rest.split_once('.') {
                match attr {
                    "type" => {
                        catalog_types.insert(name.to_string(), value.clone());
                    }
                    "path" => {
                        catalog_paths.insert(name.to_string(), value.clone());
                    }
                    _ => {}
                }
            }
        }
    }

    let mut cfg = LanceNamespaceConfig::default();

    // Root namespace
    if root_type.is_some() || root_path.is_some() {
        let t = root_type.unwrap_or_else(|| "directory".to_string());
        if !t.eq_ignore_ascii_case("directory") {
            bail!("lance.root.type='{t}' is not supported (only 'directory' is currently supported)");
        }
        let path = root_path.ok_or_else(|| anyhow::anyhow!(
            "lance.root.type is set but 'lance.root.path' is missing"
        ))?;
        cfg.root = Some(DirectoryNamespaceConfig { path });
    }

    // Named catalogs
    let catalog_names: HashSet<String> = catalog_types
        .keys()
        .cloned()
        .chain(catalog_paths.keys().cloned())
        .collect();

    for name in catalog_names {
        let t = catalog_types
            .get(&name)
            .cloned()
            .unwrap_or_else(|| "directory".to_string());
        if !t.eq_ignore_ascii_case("directory") {
            bail!(
                "lance.catalog.{name}.type='{t}' is not supported (only 'directory' is currently supported)"
            );
        }
        let path = catalog_paths.get(&name).ok_or_else(|| {
            anyhow::anyhow!("lance.catalog.{name}.type is set but 'lance.catalog.{name}.path' is missing")
        })?;
        cfg
            .catalogs
            .insert(name.clone(), DirectoryNamespaceConfig { path: path.clone() });
    }

    if cfg.root.is_none() && cfg.catalogs.is_empty() {
        Ok(None)
    } else {
        Ok(Some(cfg))
    }
}

fn build_session_config(df_options: &HashMap<String, String>) -> Result<SessionConfig> {
    if df_options.is_empty() {
        return Ok(SessionConfig::new());
    }

    SessionConfig::from_string_hash_map(df_options)
        .with_context(|| "failed to build DataFusion SessionConfig from key/value options".to_string())
}

async fn build_engine(
    lance_cfg: Option<LanceNamespaceConfig>,
    session_config: SessionConfig,
) -> Result<Engine> {
    if let Some(cfg) = lance_cfg {
        let mut builder = SessionBuilder::new().with_config(session_config);

        if let Some(root) = cfg.root {
            let ns = build_directory_namespace(&root.path).await?;
            builder = builder.with_root(ns);
        }

        for (catalog_name, ns_cfg) in cfg.catalogs {
            let ns = build_directory_namespace(&ns_cfg.path).await?;
            builder = builder.add_catalog(&catalog_name, ns);
        }

        let session = builder.build().await?;
        Ok(Engine::Lance(session))
    } else {
        let ctx = SessionContext::new_with_config(session_config);
        Ok(Engine::Plain(ctx))
    }
}

async fn build_directory_namespace(path: &str) -> Result<Namespace> {
    let ns_impl = DirectoryNamespaceBuilder::new(path.to_string())
        .manifest_enabled(true)
        .dir_listing_enabled(true)
        .build()
        .await
        .with_context(|| format!("failed to build directory namespace for '{path}'"))?;

    let ns: Arc<dyn LanceNamespace> = Arc::new(ns_impl);
    Ok(Namespace::from_root(ns))
}

/// Execute a potentially multi-statement script.
async fn execute_sql_script(engine: &Engine, script: &str, format: OutputFormat) -> Result<()> {
    let statements = split_statements(script);
    for stmt in statements {
        let trimmed = stmt.trim();
        if trimmed.is_empty() {
            continue;
        }
        let df = engine
            .sql(trimmed)
            .await
            .with_context(|| format!("failed to execute SQL: {trimmed}"))?;
        render_dataframe(&df, format).await?;
    }
    Ok(())
}

fn split_statements(sql: &str) -> Vec<String> {
    sql.split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

async fn run_repl(engine: &Engine, format: OutputFormat) -> Result<()> {
    println!("Lance DataFusion CLI");
    println!("Enter SQL terminated by ';'. Type 'quit' or 'exit' to leave.");

    let mut buffer = String::new();

    loop {
        print!("> ");
        io::stdout().flush()?;
        buffer.clear();
        if io::stdin().read_line(&mut buffer)? == 0 {
            break;
        }

        let trimmed = buffer.trim();
        if trimmed.eq_ignore_ascii_case("quit") || trimmed.eq_ignore_ascii_case("exit") {
            break;
        }

        let mut statement = String::new();
        statement.push_str(&buffer);

        while !statement.trim_end().ends_with(';') {
            print!("... ");
            io::stdout().flush()?;
            buffer.clear();
            if io::stdin().read_line(&mut buffer)? == 0 {
                break;
            }
            statement.push_str(&buffer);
        }

        execute_sql_script(engine, &statement, format).await?;
    }

    Ok(())
}

/// Parse `-c/--command` and `-f/--file` options in order from raw argv.
fn parse_exec_sequence(raw_args: &[String]) -> Vec<ExecItem> {
    let mut items = Vec::new();
    let mut i = 1; // skip program name

    while i < raw_args.len() {
        let arg = &raw_args[i];

        if arg == "-c" || arg == "--command" {
            if let Some(val) = raw_args.get(i + 1) {
                items.push(ExecItem::Command(val.clone()));
                i += 2;
                continue;
            } else {
                break;
            }
        } else if let Some(rest) = arg.strip_prefix("--command=") {
            items.push(ExecItem::Command(rest.to_string()));
            i += 1;
            continue;
        } else if arg.starts_with("-c") && arg.len() > 2 {
            items.push(ExecItem::Command(arg[2..].to_string()));
            i += 1;
            continue;
        } else if arg == "-f" || arg == "--file" {
            if let Some(val) = raw_args.get(i + 1) {
                items.push(ExecItem::File(PathBuf::from(val)));
                i += 2;
                continue;
            } else {
                break;
            }
        } else if let Some(rest) = arg.strip_prefix("--file=") {
            items.push(ExecItem::File(PathBuf::from(rest)));
            i += 1;
            continue;
        } else if arg.starts_with("-f") && arg.len() > 2 {
            items.push(ExecItem::File(PathBuf::from(&arg[2..])));
            i += 1;
            continue;
        }

        i += 1;
    }

    items
}

async fn render_dataframe(df: &DataFrame, format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Table => {
            df.clone().show().await?;
        }
        OutputFormat::Csv => {
            let batches = df.clone().collect().await?;
            print_batches_csv(&batches)?;
        }
        OutputFormat::Json => {
            let batches = df.clone().collect().await?;
            print_batches_json(&batches)?;
        }
    }
    Ok(())
}

fn print_batches_csv(batches: &[RecordBatch]) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }

    let schema = batches[0].schema();
    let headers: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    println!("{}", headers.join(","));

    for batch in batches {
        let columns = batch.columns();
        let row_count = batch.num_rows();
        for row in 0..row_count {
            let mut values = Vec::with_capacity(columns.len());
            for col in columns {
                values.push(array_value_to_string(col.as_ref(), row));
            }
            println!("{}", values.join(","));
        }
    }

    Ok(())
}

fn print_batches_json(batches: &[RecordBatch]) -> Result<()> {
    if batches.is_empty() {
        println!("[]");
        return Ok(());
    }

    let schema = batches[0].schema();
    let field_names: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();

    let mut rows = Vec::new();

    for batch in batches {
        let columns = batch.columns();
        let row_count = batch.num_rows();
        for row in 0..row_count {
            let mut obj = serde_json::Map::new();
            for (idx, col) in columns.iter().enumerate() {
                let v = array_value_to_json(col.as_ref(), row);
                obj.insert(field_names[idx].clone(), v);
            }
            rows.push(serde_json::Value::Object(obj));
        }
    }

    println!("{}", serde_json::to_string_pretty(&rows)?);
    Ok(())
}

fn array_value_to_string(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        return "".to_string();
    }

    match array.data_type() {
        DataType::Boolean => {
            let a = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("BooleanArray downcast");
            a.value(row).to_string()
        }
        DataType::Int8 => {
            let a = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("Int8Array downcast");
            a.value(row).to_string()
        }
        DataType::Int16 => {
            let a = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .expect("Int16Array downcast");
            a.value(row).to_string()
        }
        DataType::Int32 => {
            let a = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32Array downcast");
            a.value(row).to_string()
        }
        DataType::Int64 => {
            let a = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64Array downcast");
            a.value(row).to_string()
        }
        DataType::UInt8 => {
            let a = array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .expect("UInt8Array downcast");
            a.value(row).to_string()
        }
        DataType::UInt16 => {
            let a = array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .expect("UInt16Array downcast");
            a.value(row).to_string()
        }
        DataType::UInt32 => {
            let a = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .expect("UInt32Array downcast");
            a.value(row).to_string()
        }
        DataType::UInt64 => {
            let a = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("UInt64Array downcast");
            a.value(row).to_string()
        }
        DataType::Float32 => {
            let a = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("Float32Array downcast");
            a.value(row).to_string()
        }
        DataType::Float64 => {
            let a = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("Float64Array downcast");
            a.value(row).to_string()
        }
        DataType::Utf8 => {
            let a = array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("StringArray downcast");
            // Simple CSV escaping: wrap in double quotes and escape inner quotes
            let s = a.value(row).replace('"', "\"\"");
            format!("\"{}\"", s)
        }
        other => format!("<unsupported:{other:?}>")
    }
}

fn array_value_to_json(array: &dyn Array, row: usize) -> serde_json::Value {
    if array.is_null(row) {
        return serde_json::Value::Null;
    }

    match array.data_type() {
        DataType::Boolean => {
            let a = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("BooleanArray downcast");
            serde_json::Value::Bool(a.value(row))
        }
        DataType::Int8 => {
            let a = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("Int8Array downcast");
            serde_json::Value::Number(serde_json::Number::from(a.value(row) as i64))
        }
        DataType::Int16 => {
            let a = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .expect("Int16Array downcast");
            serde_json::Value::Number(serde_json::Number::from(a.value(row) as i64))
        }
        DataType::Int32 => {
            let a = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32Array downcast");
            serde_json::Value::Number(serde_json::Number::from(a.value(row) as i64))
        }
        DataType::Int64 => {
            let a = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64Array downcast");
            serde_json::Value::Number(serde_json::Number::from(a.value(row)))
        }
        DataType::UInt8 => {
            let a = array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .expect("UInt8Array downcast");
            serde_json::Value::Number(serde_json::Number::from(a.value(row) as u64))
        }
        DataType::UInt16 => {
            let a = array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .expect("UInt16Array downcast");
            serde_json::Value::Number(serde_json::Number::from(a.value(row) as u64))
        }
        DataType::UInt32 => {
            let a = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .expect("UInt32Array downcast");
            serde_json::Value::Number(serde_json::Number::from(a.value(row) as u64))
        }
        DataType::UInt64 => {
            let a = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("UInt64Array downcast");
            serde_json::Value::Number(serde_json::Number::from(a.value(row)))
        }
        DataType::Float32 => {
            let a = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("Float32Array downcast");
            serde_json::Value::Number(
                serde_json::Number::from_f64(a.value(row) as f64).unwrap_or_else(|| {
                    serde_json::Number::from_f64(0.0).expect("0.0 is valid f64")
                }),
            )
        }
        DataType::Float64 => {
            let a = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("Float64Array downcast");
            serde_json::Value::Number(
                serde_json::Number::from_f64(a.value(row)).unwrap_or_else(|| {
                    serde_json::Number::from_f64(0.0).expect("0.0 is valid f64")
                }),
            )
        }
        DataType::Utf8 => {
            let a = array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("StringArray downcast");
            serde_json::Value::String(a.value(row).to_string())
        }
        other => serde_json::Value::String(format!("<unsupported:{other:?}>") ),
    }
}
