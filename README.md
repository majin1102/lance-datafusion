# Lance DataFusion Catalog Provider

This crate provides a `CatalogProvider` implementation for [Apache DataFusion](https://arrow.apache.org/datafusion/) that integrates with the Lance `LanceNamespace` trait. It allows you to expose Lance datasets as tables within a DataFusion `SessionContext` using a structured `catalog.schema.table` hierarchy.

## Features

- **Namespace as Schema**: Maps a `LanceNamespace` to a DataFusion `SchemaProvider`.
- **Directory Namespace Support**: Easily register a directory of Lance datasets (e.g., `s3://bucket/data/`, `/path/to/data/`) as a schema.
- **Dynamic Table Resolution**: Tables are resolved on-the-fly by calling `namespace.describe_table()` to get the dataset URI.
- **Session Wrapper**: Provides a high-level `Session` API that wraps DataFusion's `SessionContext`, manages a `LanceCatalogProviderList`, and exposes a `use()` method to mount namespaces.
- **Minimal DML Support**: Routes `DELETE`, `UPDATE`, and `MERGE INTO` statements through Lance's custom DML engine when using the `Session::sql` API.

## Usage

Here is a minimal example of how to register a directory of Lance datasets and query them using SQL.

### 1. Project Setup

Add `lance-datafusion-catalog` to your `Cargo.toml` dependencies:

```toml
[dependencies]
lance-datafusion-catalog = { git = "https://code.byted.org/emr-core/lance-datafusion.git", branch = "main" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

### 2. Registering a Namespace

The recommended way to mount namespaces is via the high-level `Session` API. It wraps a DataFusion `SessionContext` and manages a `LanceCatalogProviderList` for you.

Assume you have a directory `/path/to/my_data` containing a Lance dataset at `/path/to/my_data/foo.lance`.

```rust
use lance_datafusion_catalog::{NamespaceConfig, Session};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a new Lance session with an underlying DataFusion SessionContext.
    let session = Session::new();

    // Mount the directory /path/to/my_data as the schema "default"
    // under the default catalog "lance".
    let ns = NamespaceConfig::for_directory("/path/to/my_data");
    session.r#use(ns).await?;

    // Now you can query the 'foo' table as lance.default.foo.
    let df = session
        .sql("SELECT * FROM lance.default.foo LIMIT 10")
        .await?;
    df.show().await?;

    Ok(())
}
```

### 3. Executing DML (DELETE / UPDATE / MERGE INTO)

When using the `Session` API, DML statements are routed automatically through Lance's custom engine when the SQL text starts with `DELETE`, `UPDATE`, or `MERGE`.

```rust
use lance_datafusion_catalog::{NamespaceConfig, Session};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let session = Session::new();
    let ns = NamespaceConfig::for_directory("/path/to/my_data");
    session.r#use(ns).await?;

    // Initial count
    let initial_count = session
        .sql("SELECT COUNT(*) FROM lance.default.foo")
        .await?
        .collect()
        .await?;
    println!("Initial count: {:?}", initial_count);

    // Delete some rows
    session
        .sql("DELETE FROM lance.default.foo WHERE x >= 10")
        .await?;

    // Update some rows
    session
        .sql("UPDATE lance.default.foo SET x = x + 1 WHERE id BETWEEN 5 AND 9")
        .await?;

    // Merge from another table (simplified example)
    session
        .sql(
            "MERGE INTO lance.default.foo AS t \
             USING lance.default.bar AS s \
             ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET x = s.x \
             WHEN NOT MATCHED THEN INSERT (id, x) VALUES (s.id, s.x)",
        )
        .await?;

    // Verify new count
    let final_count = session
        .sql("SELECT COUNT(*) FROM lance.default.foo")
        .await?
        .collect()
        .await?;
    println!("Final count: {:?}", final_count);

    Ok(())
}
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

The YAML is a flat key/value mapping; only keys prefixed with `lance.*` are parsed:

```yaml
lance.root.type: directory
lance.root.path: /path/to/lance_root

lance.catalog.crm.type: directory
lance.catalog.crm.path: /path/to/crm_namespace
```

Meaning:

- `lance.root.*`: root directory namespace. Registered via `SessionBuilder::with_root()` as a dynamic `LanceCatalogProviderList`, exposing top-level namespaces under the root as catalogs (e.g., `retail.sales.*`).
- `lance.catalog.<name>.*`: additional directory-backed catalogs registered via `SessionBuilder::add_catalog()` (e.g., `crm.dim.*`).

When any `lance.*` settings are present, the CLI:

1. Builds the corresponding directory `LanceNamespace` via `lance-namespace-impls::DirectoryNamespaceBuilder`.
2. Uses `SessionBuilder` to create a `SessionContext` with `LanceCatalogProviderList` / `LanceCatalogProvider` registered.
3. Executes SQL via `LanceSession` so `DELETE / UPDATE / MERGE` go through Lance DML extensions, while other statements use DataFusion's native path.

If no `lance.*` settings are provided, the CLI constructs a default `SessionContext` and behaves like native DataFusion.

### 3. Command-line options

The CLI supports:

- `--config <path>`: load a YAML configuration file (optional).
- `--conf key=value`: additional settings; can be specified multiple times.
  - Keys prefixed with `lance.*` are used for namespace configuration and override the same keys from YAML.
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
  --conf lance.root.path=/data/lance_root \
  --format json \
  --command "SELECT * FROM retail.sales.customers LIMIT 10;"
```

Keyspace conventions:

- Only directory namespaces are supported: `type=directory`.
- If `type=service` or other types are configured, the CLI returns an error: "not supported (only 'directory')".

## Limitations and Future Work

This is an initial implementation with several areas for improvement:

- **DML Support**: Only a constrained subset of `DELETE`, `UPDATE`, and `MERGE INTO` statements are supported via the Lance SQL extensions. The implementation focuses on single-table DELETE, simple UPDATE without `FROM` / `RETURNING` / `ORDER BY` / `LIMIT`, and a minimal MERGE form (single equality join key, one WHEN MATCHED + one WHEN NOT MATCHED). INSERT statements continue to use DataFusion's native SQL path via `TableProvider::insert_into` and return a single `count` column. The long-term plan is to expand coverage and integrate more deeply with DataFusion's native DML handling.
- **Async Catalog API**: The current implementation uses DataFusion's synchronous `CatalogProvider` and `SchemaProvider` traits. For namespaces with high-latency (e.g., remote services), this can block the query planner. Future work will involve implementing the `AsyncCatalogProvider` traits for better performance.
- **View and UDF Placement**: There is currently no strategy for managing views or user-defined functions within the Lance catalog structure. This will be explored in future iterations.
- **Table Listing**: `SchemaProvider::table_names()` is not yet implemented due to the synchronous nature of the API. Table discovery currently relies on direct lookups via `table()`.
