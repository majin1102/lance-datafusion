// SPDX-License-Identifier: Apache-2.0

//! Command line entrypoint for running DataFusion with Lance namespace integration.
//!
//! This binary parses Lance-specific configuration (via `--conf` / `--config`)
//! and builds a `SessionContext` using the existing `SessionBuilder`, then
//! delegates interactive REPL and batch execution to `datafusion-cli`'s
//! `exec` module and printing logic.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::Parser;
use datafusion::execution::context::SessionConfig;
use datafusion_cli::{
    exec,
    print_format::PrintFormat,
    print_options::{MaxRows, PrintOptions},
};
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

    /// Output format (delegated to datafusion-cli's PrintFormat)
    #[arg(long = "format", value_enum, default_value_t = PrintFormat::Table)]
    format: PrintFormat,

    /// Reduce printing other than the results and work quietly
    #[arg(short, long)]
    quiet: bool,

    /// Enables console syntax highlighting
    #[arg(long)]
    color: bool,

    /// The max number of rows to display for 'Table' format
    /// [possible values: numbers(0/10/...), inf(no limit)]
    #[arg(long, default_value = "40")]
    maxrows: MaxRows,
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

    // Build LanceSession using the existing SessionBuilder, then obtain
    // the underlying SessionContext for datafusion-cli exec functions.
    let session = build_lance_session(lance_cfg, session_config).await?;

    let mut print_options = PrintOptions {
        format: args.format,
        quiet: args.quiet,
        maxrows: args.maxrows,
        color: args.color,
    };

    let ctx = session.context();

    if args.command.is_empty() && args.file.is_empty() {
        // Pure REPL mode: delegate to datafusion-cli
        exec::exec_from_repl(ctx, &mut print_options).await?;
    } else {
        if !args.file.is_empty() {
            let files: Vec<String> = args
                .file
                .iter()
                .map(|p| p.to_string_lossy().into_owned())
                .collect();
            exec::exec_from_files(ctx, files, &print_options).await?;
        }

        if !args.command.is_empty() {
            exec::exec_from_commands(ctx, args.command.clone(), &print_options).await?;
        }
    }

    Ok(())
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

fn merge_maps(
    mut base: HashMap<String, String>,
    override_map: HashMap<String, String>,
) -> HashMap<String, String> {
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

fn parse_lance_namespace_config(
    kv: &HashMap<String, String>,
) -> Result<Option<LanceNamespaceConfig>> {
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
            bail!(
                "lance.root.type='{t}' is not supported (only 'directory' is currently supported)"
            );
        }
        let path = root_path.ok_or_else(|| {
            anyhow::anyhow!("lance.root.type is set but 'lance.root.path' is missing")
        })?;
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
            anyhow::anyhow!(
                "lance.catalog.{name}.type is set but 'lance.catalog.{name}.path' is missing"
            )
        })?;
        cfg.catalogs
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
        let config = SessionConfig::new()
            .with_information_schema(true);
        return Ok(config);
    }

    SessionConfig::from_string_hash_map(df_options)
        .with_context(|| "failed to build DataFusion SessionConfig from key/value options".to_string())
}

async fn build_directory_ns_arc(path: &str) -> Result<Arc<dyn LanceNamespace>> {
    let ns_impl = DirectoryNamespaceBuilder::new(path.to_string())
        .manifest_enabled(true)
        .dir_listing_enabled(true)
        .build()
        .await
        .with_context(|| format!("failed to build directory namespace for '{path}'"))?;

    Ok(Arc::new(ns_impl))
}

async fn build_lance_session(
    lance_cfg: Option<LanceNamespaceConfig>,
    session_config: SessionConfig,
) -> Result<LanceSession> {
    let mut builder = SessionBuilder::new().with_config(session_config);

    if let Some(cfg) = lance_cfg {
        if let Some(root) = cfg.root {
            let root_ns = build_directory_ns_arc(&root.path).await?;
            builder = builder.with_root(Namespace::from_root(Arc::clone(&root_ns)));
        }

        for (catalog_name, ns_cfg) in cfg.catalogs {
            let catalog_ns = build_directory_ns_arc(&ns_cfg.path).await?;
            // Anchor the catalog at the child namespace so `catalog.schema.table`
            // resolves correctly (e.g. crm.dim.* under the `crm` catalog).
            let ns = Namespace::from_namespace(
                Arc::clone(&catalog_ns),
                vec![catalog_name.clone()],
            );
            builder = builder.add_catalog(&catalog_name, ns);
        }
    }

    let session = builder.build().await?;
    Ok(session)
}
