// SPDX-License-Identifier: Apache-2.0

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::context::SessionContext;
use datafusion_common::TableReference;
use datafusion_sql::planner::object_name_to_table_reference as df_object_name_to_table_reference;
use datafusion_sql::sqlparser::{
    ast::{FromTable, ObjectName, Statement, TableFactor},
    dialect::GenericDialect,
    parser::Parser,
};
use lance::datafusion::LanceTableProvider;
use lance::dataset::Dataset;

/// Execute a minimal subset of DML against Lance tables.
///
/// Currently this only supports statements of the form:
///
/// ```sql
/// DELETE FROM catalog.schema.table WHERE <predicate>
/// ```
///
/// The predicate is forwarded to [`Dataset::delete`] as a Lance filter expression.
/// Other DML statements will return `DataFusionError::NotImplemented`.
pub async fn execute_lance_sql(ctx: &SessionContext, sql: &str) -> DFResult<()> {
    let dialect = GenericDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql).map_err(|e| {
        DataFusionError::Execution(format!("Failed to parse SQL in execute_lance_sql: {e}"))
    })?;

    if statements.len() != 1 {
        return Err(DataFusionError::Execution(
            "execute_lance_sql only supports a single SQL statement".to_string(),
        ));
    }

    let statement = statements.remove(0);

    match statement {
        Statement::Delete(delete) => {
            // Only support the simplest form of DELETE for now.
            if !delete.tables.is_empty()
                || delete.using.is_some()
                || delete.returning.is_some()
                || !delete.order_by.is_empty()
                || delete.limit.is_some()
            {
                return Err(DataFusionError::NotImplemented(
                    "execute_lance_sql only supports `DELETE FROM <table> WHERE <expr>`"
                        .to_string(),
                ));
            }

            let table_name = simple_delete_target(delete.from)?;
            let enable_normalization = ctx.enable_ident_normalization();
            let table_ref = df_object_name_to_table_reference(table_name, enable_normalization)?;

            let predicate = match delete.selection {
                Some(expr) => expr.to_string(),
                None => "true".to_string(),
            };

            execute_lance_delete(ctx, table_ref, predicate).await
        }
        other => Err(DataFusionError::NotImplemented(format!(
            "execute_lance_sql only supports DELETE statements, got: {:?}",
            other
        ))),
    }
}

fn simple_delete_target(from: FromTable) -> DFResult<ObjectName> {
    let mut from_vec = match from {
        FromTable::WithFromKeyword(v) => v,
        FromTable::WithoutKeyword(v) => v,
    };

    if from_vec.len() != 1 {
        return Err(DataFusionError::NotImplemented(
            "execute_lance_sql only supports DELETE from a single table".to_string(),
        ));
    }

    let table_with_joins = from_vec.pop().unwrap();
    if !table_with_joins.joins.is_empty() {
        return Err(DataFusionError::NotImplemented(
            "execute_lance_sql does not support DELETE with joins".to_string(),
        ));
    }

    match table_with_joins.relation {
        TableFactor::Table { name, .. } => Ok(name),
        other => Err(DataFusionError::NotImplemented(format!(
            "execute_lance_sql only supports simple table references in DELETE, got: {:?}",
            other
        ))),
    }
}

async fn execute_lance_delete(
    ctx: &SessionContext,
    table_ref: TableReference,
    predicate: String,
) -> DFResult<()> {
    // Resolve the table provider for the given reference.
    let provider = ctx.table_provider(table_ref.clone()).await?;

    let lance_provider = provider
        .as_any()
        .downcast_ref::<LanceTableProvider>()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "execute_lance_sql only supports tables backed by LanceTableProvider".to_string(),
            )
        })?;

    let dataset = lance_provider.dataset();
    let uri = dataset.uri().to_string();

    // Re-open the dataset so we can call `delete` and then re-register a fresh
    // table provider pointing at the updated version.
    let mut dataset = Dataset::open(&uri).await.map_err(|e| {
        DataFusionError::Execution(format!(
            "Failed to open Lance dataset for DELETE at '{}': {}",
            uri, e
        ))
    })?;

    dataset.delete(&predicate).await.map_err(|e| {
        DataFusionError::Execution(format!("Lance delete failed on '{}': {}", uri, e))
    })?;

    // After the delete completes, future table lookups will open the latest
    // dataset version via `LanceSchemaProvider::table`, so we do not need to
    // update the registration in the SessionContext.

    Ok(())
}
