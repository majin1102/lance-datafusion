# Lance DataFusion Catalog Provider

This crate provides a `CatalogProvider` implementation for [Apache DataFusion](https://arrow.apache.org/datafusion/) that integrates with the Lance `LanceNamespace` trait. It allows you to expose Lance datasets as tables within a DataFusion `SessionContext` using a structured `catalog.schema.table` hierarchy.

## Features

- **Namespace as Schema**: Maps a `LanceNamespace` to a DataFusion `SchemaProvider`.
- **Directory Namespace Support**: Easily register a directory of Lance datasets (e.g., `s3://bucket/data/`, `/path/to/data/`) as a schema.
- **Dynamic Table Resolution**: Tables are resolved on-the-fly by calling `namespace.describe_table()` to get the dataset URI.
- **Minimal DML Support**: Includes an entry point `execute_lance_sql` for basic `DELETE` operations on Lance tables.

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

First, create a `SessionContext`. Then, use the `register_directory_namespace_as_schema` helper to mount a directory as a schema within a named catalog.

Assume you have a directory `/path/to/my_data` containing a Lance dataset at `/path/to/my_data/foo.lance`.

```rust
use datafusion::prelude::SessionContext;
use lance_datafusion_catalog::register::register_directory_namespace_as_schema;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();

    // Register the directory /path/to/my_data as the schema "default"
    // under the catalog "my_lance_catalog".
    register_directory_namespace_as_schema(
        &ctx,
        "my_lance_catalog",
        "default",
        "/path/to/my_data"
    ).await?;

    // Now you can query the 'foo' table.
    let df = ctx.sql("SELECT * FROM my_lance_catalog.default.foo LIMIT 10").await?;
    df.show().await?;

    Ok(())
}
```

### 3. Executing DML (DELETE)

This crate provides a separate function `execute_lance_sql` for DML operations. This is an initial implementation and is not a replacement for DataFusion's full DML capabilities.

```rust
use datafusion::prelude::SessionContext;
use lance_datafusion_catalog::dml::execute_lance_sql;
use lance_datafusion_catalog::register::register_directory_namespace_as_schema;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    register_directory_namespace_as_schema(
        &ctx,
        "my_lance_catalog",
        "default",
        "/path/to/my_data"
    ).await?;

    // Initial count
    let initial_count = ctx.sql("SELECT COUNT(*) FROM my_lance_catalog.default.foo").await?.collect().await?;
    println!("Initial count: {:?}", initial_count);

    // Delete some rows
    execute_lance_sql(
        &ctx,
        "DELETE FROM my_lance_catalog.default.foo WHERE x >= 10"
    ).await?;

    // Verify new count
    let final_count = ctx.sql("SELECT COUNT(*) FROM my_lance_catalog.default.foo").await?.collect().await?;
    println!("Final count: {:?}", final_count);
    
    Ok(())
}
```

## Limitations and Future Work

This is an initial implementation with several areas for improvement:

- **DML Support**: Only simple `DELETE` statements are supported via `execute_lance_sql`. The plan is to expand this to `UPDATE` and `MERGE INTO`, and eventually integrate more deeply with DataFusion's native DML handling.
- **Async Catalog API**: The current implementation uses DataFusion's synchronous `CatalogProvider` and `SchemaProvider` traits. For namespaces with high-latency (e.g., remote services), this can block the query planner. Future work will involve implementing the `AsyncCatalogProvider` traits for better performance.
- **View and UDF Placement**: There is currently no strategy for managing views or user-defined functions within the Lance catalog structure. This will be explored in future iterations.
- **Table Listing**: `SchemaProvider::table_names()` is not yet implemented due to the synchronous nature of the API. Table discovery currently relies on direct lookups via `table()`.
