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

## Limitations and Future Work

This is an initial implementation with several areas for improvement:

- **DML Support**: Only a constrained subset of `DELETE`, `UPDATE`, and `MERGE INTO` statements are supported via `execute_lance_sql`. The implementation focuses on single-table DELETE, simple UPDATE without `FROM` / `RETURNING` / `ORDER BY` / `LIMIT`, and a minimal MERGE form (single equality join key, one WHEN MATCHED + one WHEN NOT MATCHED). The long-term plan is to expand coverage and integrate more deeply with DataFusion's native DML handling.
- **Async Catalog API**: The current implementation uses DataFusion's synchronous `CatalogProvider` and `SchemaProvider` traits. For namespaces with high-latency (e.g., remote services), this can block the query planner. Future work will involve implementing the `AsyncCatalogProvider` traits for better performance.
- **View and UDF Placement**: There is currently no strategy for managing views or user-defined functions within the Lance catalog structure. This will be explored in future iterations.
- **Table Listing**: `SchemaProvider::table_names()` is not yet implemented due to the synchronous nature of the API. Table discovery currently relies on direct lookups via `table()`.
