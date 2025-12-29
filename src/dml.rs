// SPDX-License-Identifier: Apache-2.0

use arrow_array::RecordBatchIterator;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::sqlparser::ast::Insert;
use datafusion_sql::planner::object_name_to_table_reference;
use datafusion_sql::sqlparser::ast::TableObject;
use datafusion_sql::sqlparser::parser::Parser;
use datafusion_sql::sqlparser::{
    ast::{
        Assignment, AssignmentTarget, Delete, Expr as SQLExpr, FromTable, ObjectName, Statement,
        TableFactor, TableWithJoins,
    },
    dialect::GenericDialect,
};
use datafusion_sql::TableReference;
use lance::dataset::{Dataset, MergeInsertBuilder, UpdateBuilder, WhenMatched, WhenNotMatched};
use std::sync::Arc;
use crate::table::table_provider::LanceTableProvider;

/// Execute a minimal subset of DML against Lance tables.
///
/// Currently supports:
///
/// - `DELETE`:
///   `DELETE FROM catalog.schema.table WHERE <predicate>`
/// - `UPDATE`:
///   `UPDATE catalog.schema.table SET col1 = expr1, col2 = expr2 WHERE <predicate>`
/// - `MERGE` (upsert-style):
///   `MERGE INTO target USING source ON <key> WHEN MATCHED THEN ... WHEN NOT MATCHED THEN ...`
///
/// For UPDATE / MERGE, expressions are forwarded to Lance's update / merge
/// engines and are subject to their own expression support. Unsupported forms
/// will return `DataFusionError::NotImplemented` or `DataFusionError::Execution`.
pub async fn execute_sql(ctx: &SessionContext, sql: &str) -> Result<()> {
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
        Statement::Insert(insert) => handle_insert(ctx, insert).await,
        Statement::Delete(delete) => handle_delete(ctx, delete).await,
        Statement::Update {
            table,
            assignments,
            selection,
            ..
        } => {
            // NOTE: Older versions of the SQL parser do not expose FROM / RETURNING /
            // LIMIT / ON CONFLICT fields in the UPDATE AST, so we conservatively
            // rely on a textual check to reject obviously unsupported forms.
            let upper = sql.to_ascii_uppercase();
            if upper.contains(" UPDATE ") && upper.contains(" FROM ") {
                return Err(DataFusionError::NotImplemented(
                    "execute_lance_sql UPDATE does not yet support FROM / joins".to_string(),
                ));
            }
            if upper.contains(" RETURNING ") {
                return Err(DataFusionError::NotImplemented(
                    "execute_lance_sql UPDATE does not yet support RETURNING".to_string(),
                ));
            }
            if upper.contains(" ON CONFLICT ")
                || upper.contains(" OR ROLLBACK ")
                || upper.contains(" OR ABORT ")
                || upper.contains(" OR REPLACE ")
                || upper.contains(" OR IGNORE ")
            {
                return Err(DataFusionError::NotImplemented(
                    "execute_lance_sql UPDATE does not yet support ON CONFLICT / OR clauses"
                        .to_string(),
                ));
            }
            if upper.contains(" LIMIT ") {
                return Err(DataFusionError::NotImplemented(
                    "execute_lance_sql UPDATE does not yet support LIMIT".to_string(),
                ));
            }

            handle_update(ctx, table, &assignments, selection).await
        }
        other => {
            // MERGE is not yet part of DataFusion's logical DML, so we dispatch
            // based on the raw SQL text.
            if is_merge_statement(sql) {
                execute_lance_merge(ctx, sql).await
            } else {
                Err(DataFusionError::NotImplemented(format!(
                    "execute_lance_sql only supports DELETE / UPDATE / MERGE statements, got: {:?}",
                    other
                )))
            }
        }
    }
}

fn is_merge_statement(sql: &str) -> bool {
    sql.trim_start().to_ascii_uppercase().starts_with("MERGE")
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

async fn handle_delete(ctx: &SessionContext, delete: Delete) -> Result<()> {
    // Only support the simplest form of DELETE for now.
    if !delete.tables.is_empty()
        || delete.using.is_some()
        || delete.returning.is_some()
        || !delete.order_by.is_empty()
        || delete.limit.is_some()
    {
        return Err(DataFusionError::NotImplemented(
            "execute_lance_sql only supports `DELETE FROM <table> WHERE <expr>`".to_string(),
        ));
    }

    let table_name = simple_delete_target(delete.from)?;
    let enable_normalization = ctx.enable_ident_normalization();
    let table_ref = object_name_to_table_reference(table_name, enable_normalization)?;

    let predicate = match delete.selection {
        Some(expr) => expr.to_string(),
        None => "true".to_string(),
    };

    execute_lance_delete(ctx, table_ref, predicate).await
}

fn simple_delete_target(from: FromTable) -> Result<ObjectName> {
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
) -> Result<()> {
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

    let mut dataset = lance_provider.dataset().clone();

    dataset.delete(&predicate).await.map_err(|e| {
        DataFusionError::Execution(format!("Lance delete failed on '{}': {}", dataset.uri(), e))
    })?;

    // After the delete completes, future table lookups will open the latest
    // dataset version via `LanceSchemaProvider::table`, so we do not need to
    // update the registration in the SessionContext.

    Ok(())
}

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

async fn handle_update(
    ctx: &SessionContext,
    table: TableWithJoins,
    assignments: &[Assignment],
    selection: Option<SQLExpr>,
) -> Result<()> {
    let enable_normalization = ctx.enable_ident_normalization();
    let table_name = simple_update_target(&table)?;
    let table_ref = object_name_to_table_reference(table_name, enable_normalization)?;

    let assignments = collect_update_assignments(assignments)?;

    execute_lance_update(ctx, table_ref, assignments, selection).await
}

fn simple_update_target(table: &TableWithJoins) -> Result<ObjectName> {
    if !table.joins.is_empty() {
        return Err(DataFusionError::NotImplemented(
            "execute_lance_sql UPDATE does not support joins".to_string(),
        ));
    }

    match &table.relation {
        TableFactor::Table { name, .. } => Ok(name.clone()),
        other => Err(DataFusionError::NotImplemented(format!(
            "execute_lance_sql UPDATE only supports simple table references, got: {:?}",
            other
        ))),
    }
}

fn collect_update_assignments(assignments: &[Assignment]) -> Result<Vec<(String, String)>> {
    let mut result = Vec::with_capacity(assignments.len());

    for assign in assignments {
        let cols = match &assign.target {
            AssignmentTarget::ColumnName(cols) => cols,
            _ => {
                return Err(DataFusionError::NotImplemented(
                    "execute_lance_sql UPDATE only supports simple column targets".to_string(),
                ))
            }
        };

        let ident = cols
            .0
            .iter()
            .last()
            .and_then(|part| part.as_ident())
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "Invalid assignment target in UPDATE (expected column identifier)".to_string(),
                )
            })?;

        let column_name = ident.value.clone();
        let expr_sql = assign.value.to_string();

        result.push((column_name, expr_sql));
    }

    Ok(result)
}

async fn execute_lance_update(
    ctx: &SessionContext,
    table_ref: TableReference,
    assignments: Vec<(String, String)>,
    selection: Option<SQLExpr>,
) -> Result<()> {
    let provider = ctx.table_provider(table_ref.clone()).await?;

    let lance_provider = provider
        .as_any()
        .downcast_ref::<LanceTableProvider>()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "execute_lance_sql only supports tables backed by LanceTableProvider".to_string(),
            )
        })?;


    let uri = lance_provider.dataset().uri();
    let dataset = lance_provider.dataset().clone();
    let mut builder = UpdateBuilder::new(Arc::new(dataset));

    if let Some(expr) = selection {
        let predicate = expr.to_string();
        builder = builder.update_where(&predicate).map_err(|e| {
            DataFusionError::Execution(format!(
                "Lance UPDATE failed to parse WHERE predicate '{}': {}",
                predicate, e
            ))
        })?;
    }

    for (col, value_sql) in assignments {
        builder = builder.set(&col, &value_sql).map_err(|e| {
            DataFusionError::Execution(format!(
                "Lance UPDATE failed to register SET {} = {}: {}",
                col, value_sql, e
            ))
        })?;
    }

    let job = builder.build().map_err(|e| {
        DataFusionError::Execution(format!("Lance UPDATE build failed on '{}': {}", uri, e))
    })?;

    job.execute().await.map_err(|e| {
        DataFusionError::Execution(format!("Lance UPDATE execution failed on '{}': {}", uri, e))
    })?;

    Ok(())
}

// ---------------------------------------------------------------------------
// MERGE (upsert-style via MergeInsertBuilder)
// ---------------------------------------------------------------------------

async fn execute_lance_merge(ctx: &SessionContext, sql: &str) -> Result<()> {
    let enable_normalization = ctx.enable_ident_normalization();
    let (target_ref, source_ref, on_key) = parse_merge_components(sql, enable_normalization)?;

    // Resolve target as Lance table
    let provider = ctx.table_provider(target_ref.clone()).await?;
    let lance_provider = provider
        .as_any()
        .downcast_ref::<LanceTableProvider>()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "execute_lance_sql MERGE only supports target tables backed by LanceTableProvider"
                    .to_string(),
            )
        })?;

    let uri = lance_provider.dataset().uri();
    let dataset = lance_provider.dataset().clone();
    // Build source stream by querying the source table through DataFusion
    let source_df = ctx.table(source_ref.clone()).await?;
    let source_stream = source_df.execute_stream().await.map_err(|e| {
        DataFusionError::Execution(format!("Failed to execute MERGE source query: {e}"))
    })?;

    // Configure MergeInsert as upsert: update all matching rows and insert non-matching rows
    let mut builder =
        MergeInsertBuilder::try_new(Arc::new(dataset), vec![on_key.clone()]).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to create MergeInsertBuilder for MERGE on '{}': {}",
                uri, e
            ))
        })?;

    builder
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll);

    let job = builder.try_build().map_err(|e| {
        DataFusionError::Execution(format!(
            "Failed to build MergeInsertJob for MERGE on '{}': {}",
            uri, e
        ))
    })?;

    // Execute the merge. Updated dataset is committed inside Lance; future opens see new version.
    let _ = job.execute(source_stream).await.map_err(|e| {
        DataFusionError::Execution(format!("Lance MERGE execution failed on '{}': {}", uri, e))
    })?;

    Ok(())
}

fn parse_merge_components(
    sql: &str,
    enable_ident_normalization: bool,
) -> Result<(TableReference, TableReference, String)> {
    // Flatten newlines to simplify parsing while keeping original casing.
    let sql = sql.replace('\n', " ").replace('\r', " ");
    let sql = sql.trim_end_matches(';').trim();
    let upper = sql.to_ascii_uppercase();

    let merge_into = "MERGE INTO";
    let using_kw = " USING ";
    let on_kw = " ON ";
    let when_matched_kw = " WHEN MATCHED";

    let into_pos = upper.find(merge_into).ok_or_else(|| {
        DataFusionError::Execution("MERGE statement must start with `MERGE INTO`".to_string())
    })?;
    let using_pos = upper[into_pos + merge_into.len()..]
        .find(using_kw)
        .map(|idx| into_pos + merge_into.len() + idx)
        .ok_or_else(|| {
            DataFusionError::Execution("MERGE statement must contain `USING`".to_string())
        })?;
    let on_pos = upper[using_pos + using_kw.len()..]
        .find(on_kw)
        .map(|idx| using_pos + using_kw.len() + idx)
        .ok_or_else(|| {
            DataFusionError::Execution("MERGE statement must contain `ON`".to_string())
        })?;
    let when_matched_pos = upper[on_pos + on_kw.len()..]
        .find(when_matched_kw)
        .map(|idx| on_pos + on_kw.len() + idx)
        .ok_or_else(|| {
            DataFusionError::Execution("MERGE statement must contain `WHEN MATCHED`".to_string())
        })?;

    let target_part = sql[into_pos + merge_into.len()..using_pos].trim();
    let source_part = sql[using_pos + using_kw.len()..on_pos].trim();
    let on_part = sql[on_pos + on_kw.len()..when_matched_pos].trim();

    if target_part.is_empty() || source_part.is_empty() || on_part.is_empty() {
        return Err(DataFusionError::Execution(
            "MERGE statement has empty target, source, or ON clause".to_string(),
        ));
    }

    let target_ref = parse_table_ref(target_part, enable_ident_normalization)?;
    let source_ref = parse_table_ref(source_part, enable_ident_normalization)?;
    let on_key = parse_merge_on_key(on_part)?;

    Ok((target_ref, source_ref, on_key))
}

fn parse_table_ref(part: &str, _enable_ident_normalization: bool) -> Result<TableReference> {
    // Take the first token before any alias / keywords.
    let token = part.split_whitespace().next().ok_or_else(|| {
        DataFusionError::Execution(format!("Failed to parse table reference from '{}'", part))
    })?;

    // Use DataFusion's simple parser for catalog.schema.table forms.
    Ok(TableReference::parse_str(token))
}

fn parse_merge_on_key(on_part: &str) -> Result<String> {
    let eq_pos = on_part.find('=').ok_or_else(|| {
        DataFusionError::NotImplemented(
            "MERGE ON clause must be a simple equality like `t.id = s.id`".to_string(),
        )
    })?;

    let (left, right_with_eq) = on_part.split_at(eq_pos);
    let right = &right_with_eq[1..]; // skip '='

    fn extract_col(side: &str) -> Option<String> {
        let mut s = side.trim();
        // Strip surrounding parentheses
        s = s.trim_matches(|c: char| c == '(' || c == ')');
        if s.is_empty() {
            return None;
        }
        // Take last component after dot, e.g. "t.id" -> "id"
        if let Some(dot) = s.rfind('.') {
            s = &s[dot + 1..];
        }
        let s = s.trim_matches(|c| c == '"' || c == '`');
        if s.is_empty() {
            None
        } else {
            Some(s.to_string())
        }
    }

    let left_col = extract_col(left).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Failed to extract column name from MERGE ON left side '{}'",
            left
        ))
    })?;
    let right_col = extract_col(right).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Failed to extract column name from MERGE ON right side '{}'",
            right
        ))
    })?;

    if !left_col.eq_ignore_ascii_case(&right_col) {
        return Err(DataFusionError::NotImplemented(
            "MERGE ON only supports equality on the same column name on both sides (e.g. t.id = s.id)".to_string(),
        ));
    }

    Ok(left_col)
}

async fn handle_insert(ctx: &SessionContext, insert: Insert) -> Result<()> {
    // Extract basic pieces from the INSERT statement.
    let Insert {
        table,
        source,
        columns,
        ..
    } = insert;

    // Only support INSERT INTO <table> SELECT ...
    if !columns.is_empty() {
        return Err(DataFusionError::NotImplemented(
            "execute_lance_sql INSERT does not yet support column lists".to_string(),
        ));
    }

    let table_name = match table {
        TableObject::TableName(name) => name,
        _ => {
            return Err(DataFusionError::NotImplemented(
                "execute_lance_sql INSERT only supports INSERT INTO <table>".to_string(),
            ))
        }
    };

    let Some(source) = source else {
        return Err(DataFusionError::NotImplemented(
            "execute_lance_sql INSERT requires a source query".to_string(),
        ));
    };

    let enable_normalization = ctx.enable_ident_normalization();
    let table_ref = object_name_to_table_reference(table_name, enable_normalization)?;

    // Run the source query through DataFusion to get the input batches.
    let source_sql = source.to_string();
    let df = ctx.sql(&source_sql).await.map_err(|e| {
        DataFusionError::Execution(format!(
            "Failed to plan INSERT source query '{}': {}",
            source_sql, e
        ))
    })?;
    let batches = df.collect().await.map_err(|e| {
        DataFusionError::Execution(format!(
            "Failed to execute INSERT source query '{}': {}",
            source_sql, e
        ))
    })?;

    if batches.is_empty() {
        return Ok(());
    }

    let schema = batches[0].schema();
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

    // Append into the target Lance dataset.
    let provider = ctx.table_provider(table_ref.clone()).await?;
    let lance_provider = provider
        .as_any()
        .downcast_ref::<LanceTableProvider>()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "execute_lance_sql INSERT only supports tables backed by LanceTableProvider"
                    .to_string(),
            )
        })?;

    let uri = lance_provider.dataset().uri();
    let mut dataset = lance_provider.dataset().clone();

    dataset.append(reader, None).await.map_err(|e| {
        DataFusionError::Execution(format!("Lance INSERT append failed on '{}': {}", uri, e))
    })?;

    Ok(())
}
