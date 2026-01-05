# lance-datafusion

Lance integration with Apache DataFusion.

This crate provides providers and session helpers to integrate [Lance](httpshttps://github.com/lance-format/lance) with [Apache DataFusion](https://arrow.apache.org/datafusion/). It allows you to expose Lance datasets as tables within a DataFusion `SessionContext` using a structured `catalog.schema.table` hierarchy.

Key features include:
- **Catalog Integration**: Exposes a `LanceNamespace` as a tree of catalogs, schemas, and tables that DataFusion can query.
- **Session Builder**: A `SessionBuilder` API to easily construct a DataFusion `SessionContext` configured with Lance catalog providers.
- **DML Extensions**: A `LanceSession` wrapper that routes DML statements (`DELETE`, `UPDATE`, `MERGE`, `INSERT INTO ... SELECT`) to Lance's write APIs, enabling SQL-based mutations on Lance datasets.

## SQL Compatibility

This crate integrates with DataFusion's SQL engine by providing table providers and a DML-aware session wrapper. SQL support is divided into two categories:

### DataFusion-Native Queries

All read-only queries are planned and executed by DataFusion's native engine. This crate enables this by registering `TableProvider` implementations that can scan Lance datasets. This includes the full breadth of DataFusion's query capabilities:

- `SELECT` statements, including projections, filters, and limits.
- `JOIN` operations (e.g., `INNER JOIN`, `LEFT JOIN`).
- Aggregations with `GROUP BY`.
- `ORDER BY`.
- Common Table Expressions (`WITH` clauses).
- Window functions.

### Lance DML Extensions

Write-oriented SQL statements are intercepted by the `LanceSession` and routed to this crate's DML engine, which in turn calls Lance's underlying write APIs. Support for these statements is a subset of the SQL standard and is continually evolving.

The following DML statements are handled by this crate's extensions:
- `DELETE FROM ... WHERE ...`
- `UPDATE ... SET ... WHERE ...`
- `MERGE INTO ...` (upsert-style)
- `INSERT INTO ... SELECT ...`

Unsupported DML syntax will result in a `NotImplemented` error from DataFusion.

### DDL and Management

This crate does **not** provide support for SQL DDL statements (`CREATE TABLE`, `DROP TABLE`, etc.).

## Quick Start

The recommended way to start is by using the `SessionBuilder` to construct a `LanceSession`. This example mounts a directory at `/path/to/data` as the root namespace.

```rust
use std::sync::Arc;
use datafusion::error::Result;
use lance_datafusion::{Namespace, SessionBuilder};
use lance_namespace_impls::DirectoryNamespaceBuilder;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Build a Lance Namespace, for example from a directory.
    // This will treat subdirectories as catalogs and schemas.
    let root_ns_impl = DirectoryNamespaceBuilder::new("/path/to/data".to_string())
        .manifest_enabled(true)
        .dir_listing_enabled(true)
        .build()
        .await?;
    let root_ns = Namespace::from_root(Arc::new(root_ns_impl));

    // 2. Create a LanceSession using the SessionBuilder.
    let session = SessionBuilder::new()
        .with_root(root_ns)
        .build()
        .await?;

    // 3. Run a DataFusion-native SELECT query.
    // Assumes a table exists at /path/to/data/retail/sales/orders.lance
    let df = session
        .sql("SELECT * FROM retail.sales.orders LIMIT 10")
        .await?;
    df.show().await?;

    // 4. Run a Lance DML extension query.
    // The LanceSession::sql() method automatically routes DML.
    let df_delete = session
        .sql("DELETE FROM retail.sales.orders WHERE amount < 100")
        .await?;

    // DML queries return a DataFrame with a "count" column for affected rows.
    df_delete.show().await?;

    Ok(())
}
```

## CLI Usage

This crate includes a command-line binary built from `src/main.rs`. The compiled executable is named `lance-datafusion` (derived from the Cargo package name), while the CLI help banner shows `lance-datafusion-cli` as the command name. You can run it directly or via `cargo run` during development.

### Development usage with `cargo run`

The examples below show how to pass the root namespace path via the `--conf` flag, using the key `lance.namespace.root.path`.

**SELECT query (DataFusion native):**
```bash
cargo run -- --conf lance.namespace.root.path=/path/to/data -c "SELECT name, amount FROM retail.sales.orders WHERE amount >= 200"
```

**DELETE query (Lance DML extension):**
```bash
cargo run -- --conf lance.namespace.root.path=/path/to/data -c "DELETE FROM retail.sales.orders WHERE amount < 100"
```

### Executing from a file (`-f`)

Use the `-f` flag to execute one or more SQL statements from a file.

```bash
cargo run -- --conf lance.namespace.root.path=/path/to/data -f /path/to/query.sql
```

### Interactive REPL

Run the CLI without `-c` / `-f` to enter an interactive shell. In this mode, `SELECT` queries are handled by DataFusion, while DML statements (`DELETE`, `UPDATE`, `MERGE`, `INSERT INTO ... SELECT`) are automatically routed to the Lance DML handler.

```bash
cargo run -- --conf lance.namespace.root.path=/path/to/data
```

### Running the compiled binary

After building, you can invoke the compiled executable directly:

```bash
cargo build

# Debug binary
./target/debug/lance-datafusion --conf lance.namespace.root.path=/path/to/data -c "SELECT name, amount FROM retail.sales.orders WHERE amount >= 200"

# Release binary
cargo build --release
./target/release/lance-datafusion --conf lance.namespace.root.path=/path/to/data -c "DELETE FROM retail.sales.orders WHERE amount < 100"
```

## CLI Usage

This crate also provides an executable CLI built on DataFusion's SessionContext. At startup it can optionally inject Lance namespace support (directory-backed DirectoryNamespace).

### 1. Build and Run

At the repository root:

```bash
cargo build
cargo run -- --help
```

After building, the `lance-datafusion` binary can be used as a DataFusion CLI.

### 2. Inject Lance namespaces via YAML configuration

The CLI supports `--config` to load a YAML configuration file and `--conf` to pass extra settings:

```bash
# Use the directory-based example in examples/namespace.yaml
cargo run -- \
  --config examples/namespace.yaml \
  --format table
```

The YAML is a flat key/value mapping; only keys prefixed with `lance.namespace.*` are parsed:

```yaml
lance.namespace.root.type: directory
lance.namespace.root.path: /path/to/lance_root
lance.namespace.root.id: ""               # optional, defaults to empty string

lance.namespace.catalog.crm.type: directory
lance.namespace.catalog.crm.path: /path/to/crm_namespace
lance.namespace.catalog.crm.id: crm
```

Meaning:

- `lance.namespace.root.*`: root directory namespace. Registered via `SessionBuilder::with_root()` as a dynamic `LanceCatalogProviderList`, exposing top-level namespaces under the root as catalogs (e.g., `retail.sales.*`).
- `lance.namespace.catalog.<name>.*`: additional directory-backed catalogs registered via `SessionBuilder::add_catalog()` (e.g., `crm.dim.*`).

When any `lance.namespace.*` settings are present, the CLI:

1. Builds the corresponding directory `LanceNamespace` via `lance-namespace-impls::DirectoryNamespaceBuilder`.
2. Uses `SessionBuilder` to create a `SessionContext` with `LanceCatalogProviderList` / `LanceCatalogProvider` registered.
3. Executes SQL via `LanceSession` so `DELETE / UPDATE / MERGE` go through Lance DML extensions, while other statements use DataFusion's native path.

If no `lance.namespace.*` settings are provided, the CLI constructs a default `SessionContext` and behaves like native DataFusion.

### 3. Command-line options

The CLI supports:

- `--config <path>`: load a YAML configuration file (optional).
- `--conf key=value`: additional settings; can be specified multiple times.
  - Keys prefixed with `lance.namespace.*` are used for namespace configuration and override the same keys from YAML.
  - Other keys (e.g., `datafusion.execution.batch_size`) are forwarded to DataFusion `ConfigOptions`.
- `-c, --command <SQL>`: execute one or more SQL statements and then exit (repeatable).
- `-f, --file <path>`: execute SQL statements from files and then exit (repeatable).
- `--format <table|csv|json>`: result output format; default `table`.

Examples:

```bash
# Use DataFusion defaults only
cargo run -- --format csv \
  --command "SELECT 1 + 2 AS sum;"

# Query on an existing Lance directory namespace
cargo run -- \
  --config /path/to/namespace.yaml \
  --conf lance.namespace.root.path=/data/lance_root \
  --format json \
  --command "SELECT * FROM retail.sales.customers LIMIT 10;"
```

Keyspace conventions:

- Only directory namespaces are supported: `type=directory`.
- If `type=service` or other types are configured, the CLI returns an error: "not supported (only 'directory')".

## Limitations and Future Work

```bash
./target/debug/lance-datafusion --config config.yaml -c "SELECT name, amount FROM retail.sales.orders WHERE amount >= 200"
```

A minimal `config.yaml` might look like:

```yaml
lance.namespace.root.type: directory
lance.namespace.root.path: /path/to/data
# Optional: configure additional catalogs
# lance.namespace.catalog.crm.type: directory
# lance.namespace.catalog.crm.path: /path/to/crm
# lance.namespace.catalog.crm.id: crm
```

CLI options whose keys start with `lance.` are interpreted as Lance namespace settings (such as the root path), while other keys (for example, `datafusion.execution.*`) are forwarded to DataFusion's `SessionConfig`. Values passed via `--conf` override values loaded from `--config`. 
