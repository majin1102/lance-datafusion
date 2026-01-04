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

## CLI 使用

本 crate 同时提供一个可执行的 CLI，基于 DataFusion `SessionContext`，在启动时可选注入 Lance 命名空间能力（目录型 `DirectoryNamespace`）。

### 1. 构建与运行

在仓库根目录：

```bash
cargo build
cargo run -- --help
```

编译后将生成 `lance-datafusion` 二进制，可直接作为 DataFusion CLI 使用。

### 2. 通过 YAML 配置注入 Lance 命名空间

CLI 支持通过 `--config` 加载 YAML 配置文件，并通过 `--conf` 传入额外配置：

```bash
# 使用 examples/namespace.yaml 中的目录型示例
cargo run -- \
  --config examples/namespace.yaml \
  --format table
```

YAML 配置为扁平的 key/value 形式，仅解析 `lance.*` 前缀：

```yaml
lance.root.type: directory
lance.root.path: /path/to/lance_root

lance.catalog.crm.type: directory
lance.catalog.crm.path: /path/to/crm_namespace
```

含义：

- `lance.root.*`：根目录型命名空间，通过 `SessionBuilder::with_root()` 注册动态 `LanceCatalogProviderList`，暴露根下的顶层 namespace 作为 catalog（如 `retail.sales.*`）。
- `lance.catalog.<name>.*`：额外目录型命名 catalog，通过 `SessionBuilder::add_catalog()` 注册为固定 catalog（如 `crm.dim.*`）。

当存在任意 `lance.*` 配置时，CLI 会：

1. 使用 `lance-namespace-impls::DirectoryNamespaceBuilder` 构建对应的目录型 `LanceNamespace`；
2. 利用 `SessionBuilder` 构建带有 `LanceCatalogProviderList` / `LanceCatalogProvider` 的 `SessionContext`；
3. 通过 `LanceSession` 执行 SQL，使 `DELETE / UPDATE / MERGE` 走 Lance DML 扩展，其余语句使用 DataFusion 原生执行路径。

当用户未提供任何 `lance.*` 配置时，CLI 直接构建默认的 `SessionContext`，行为与原生 DataFusion 保持一致。

### 3. 命令行参数

CLI 支持以下主要参数：

- `--config <path>`：加载 YAML 配置文件（可选）。
- `--conf key=value`：附加配置，可多次使用；
  - 以前缀 `lance.*` 的键用于命名空间配置，覆盖 YAML 中的同名键；
  - 其他键（例如 `datafusion.execution.batch_size`）传递给 DataFusion `ConfigOptions`。
- `-c, --command <SQL>`：执行一条或多条 SQL，然后退出（可多次使用）。
- `-f, --file <path>`：从文件读取 SQL 脚本并执行，然后退出（可多次使用）。
- `--format <table|csv|json>`：结果输出格式，默认 `table`。

示例：

```bash
# 只使用 DataFusion 默认行为
cargo run -- --format csv \
  --command "SELECT 1 + 2 AS sum;"

# 在已有的 Lance 目录命名空间上查询
cargo run -- \
  --config /path/to/namespace.yaml \
  --conf lance.root.path=/data/lance_root \
  --format json \
  --command "SELECT * FROM retail.sales.customers LIMIT 10;"
```

键空间约定：

- 仅支持目录型命名空间：`type=directory`；
- 如配置 `type=service` 等其他类型，CLI 会报错：`当前未支持（仅支持 directory）`。

## Limitations and Future Work

This is an initial implementation with several areas for improvement:

- **DML Support**: Only a constrained subset of `DELETE`, `UPDATE`, and `MERGE INTO` statements are supported via the Lance SQL extensions. The implementation focuses on single-table DELETE, simple UPDATE without `FROM` / `RETURNING` / `ORDER BY` / `LIMIT`, and a minimal MERGE form (single equality join key, one WHEN MATCHED + one WHEN NOT MATCHED). INSERT statements continue to use DataFusion's native SQL path via `TableProvider::insert_into` and return a single `count` column. The long-term plan is to expand coverage and integrate more deeply with DataFusion's native DML handling.
- **Async Catalog API**: The current implementation uses DataFusion's synchronous `CatalogProvider` and `SchemaProvider` traits. For namespaces with high-latency (e.g., remote services), this can block the query planner. Future work will involve implementing the `AsyncCatalogProvider` traits for better performance.
- **View and UDF Placement**: There is currently no strategy for managing views or user-defined functions within the Lance catalog structure. This will be explored in future iterations.
- **Table Listing**: `SchemaProvider::table_names()` is not yet implemented due to the synchronous nature of the API. Table discovery currently relies on direct lookups via `table()`.
