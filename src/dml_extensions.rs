// SPDX-License-Identifier: Apache-2.0

use crate::table_provider::LanceTableProvider;
use arrow_array::{ArrayRef, Int64Array, RecordBatchIterator};
use datafusion::dataframe::DataFrame;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::JoinType;
use datafusion_sql::planner::object_name_to_table_reference;
use datafusion_sql::sqlparser::ast::{
    Assignment, AssignmentTarget, Delete, Expr as SQLExpr, FromTable, Insert, ObjectName,
    Statement, TableFactor, TableObject, TableWithJoins,
};
use datafusion_sql::sqlparser::dialect::GenericDialect;
use datafusion_sql::sqlparser::parser::Parser;
use datafusion_sql::TableReference;
use lance::dataset::{Dataset, MergeInsertBuilder, UpdateBuilder, WhenMatched, WhenNotMatched};
use std::sync::Arc;

/// Extend DML capabilities of Datafusion against Lance tables.
pub struct LanceSession {
    ctx: SessionContext,
}

impl LanceSession {
    pub fn new(ctx: SessionContext) -> Self {
        Self { ctx }
    }

    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }

    pub async fn sql(&self, sql: &str) -> Result<DataFrame> {
        // Route DML statements through Lance's extended DML implementation so
        // that INSERT / DELETE / UPDATE / MERGE semantics (including row
        // counts and write paths) are consistent with `execute`.
        if is_merge_statement(sql) {
            return self.execute(sql).await;
        }

        if let Ok(Statement::Insert(_) | Statement::Delete(_) | Statement::Update { .. }) =
            parse_single_statement(sql)
        {
            return self.execute(sql).await;
        }

        // Fallback to DataFusion's native SQL engine for all other statements
        // (e.g. SELECT, SHOW, EXPLAIN).
        self.ctx.sql(sql).await
    }

    pub async fn execute(&self, sql: &str) -> Result<DataFrame> {
        let statement = parse_single_statement(sql)?;

        match statement {
            // INSERT INTO is implemented by running the source query via
            // DataFusion and appending the resulting batches into the target
            // Lance dataset. This mirrors DataFusion's DML semantics but avoids
            // relying on the `INSERT` logical plan, which currently writes
            // only a single row when targeting Lance tables.
            Statement::Insert(insert) => handle_insert(&self.ctx, insert).await,
            Statement::Delete(delete) => handle_delete(&self.ctx, delete).await,
            Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => {
                // NOTE: Older versions of the SQL parser do not expose FROM /
                // RETURNING / LIMIT / ON CONFLICT fields in the UPDATE AST, so we
                // conservatively rely on a textual check to reject obviously
                // unsupported forms.
                validate_update_statement(sql)?;
                handle_update(&self.ctx, table, &assignments, selection).await
            }
            _ => {
                // MERGE is not yet part of DataFusion's logical DML, so we dispatch
                // based on the raw SQL text.
                if is_merge_statement(sql) {
                    execute_lance_merge(&self.ctx, sql).await
                } else {
                    handle_sql(&self.ctx, sql).await
                }
            }
        }
    }
}

fn parse_single_statement(sql: &str) -> Result<Statement> {
    let dialect = GenericDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql).map_err(|e| {
        DataFusionError::Execution(format!("Lance SQL extensions failed to parse SQL: {e}"))
    })?;

    if statements.len() != 1 {
        return Err(DataFusionError::Execution(
            "Lance SQL extensions only supports a single SQL statement".to_string(),
        ));
    }

    Ok(statements.remove(0))
}

fn validate_update_statement(sql: &str) -> Result<()> {
    let upper = sql.to_ascii_uppercase();

    if upper.contains(" UPDATE ") && upper.contains(" FROM ") {
        return Err(DataFusionError::NotImplemented(
            "Lance SQL extensions UPDATE does not yet support FROM / joins".to_string(),
        ));
    }
    if upper.contains(" RETURNING ") {
        return Err(DataFusionError::NotImplemented(
            "Lance SQL extensions UPDATE does not yet support RETURNING".to_string(),
        ));
    }
    if upper.contains(" ON CONFLICT ")
        || upper.contains(" OR ROLLBACK ")
        || upper.contains(" OR ABORT ")
        || upper.contains(" OR REPLACE ")
        || upper.contains(" OR IGNORE ")
    {
        return Err(DataFusionError::NotImplemented(
            "Lance SQL extensions UPDATE does not yet support ON CONFLICT / OR clauses".to_string(),
        ));
    }
    if upper.contains(" LIMIT ") {
        return Err(DataFusionError::NotImplemented(
            "Lance SQL extensions UPDATE does not yet support LIMIT".to_string(),
        ));
    }

    Ok(())
}

fn is_merge_statement(sql: &str) -> bool {
    sql.trim_start().to_ascii_uppercase().starts_with("MERGE")
}

/// INSERT handling for [`execute`].
///
/// This path writes into an existing Lance table using the semantic
/// equivalent of `INSERT INTO <table> SELECT ...` and returns a single-row
/// [`DataFrame`] reporting the number of inserted rows.
async fn handle_insert(ctx: &SessionContext, insert: Insert) -> Result<DataFrame> {
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
            "Lance SQL extensions INSERT does not yet support column lists".to_string(),
        ));
    }

    let table_name = match table {
        TableObject::TableName(name) => name,
        _ => {
            return Err(DataFusionError::NotImplemented(
                "Lance SQL extensions INSERT only supports INSERT INTO <table>".to_string(),
            ))
        }
    };

    let Some(source) = source else {
        return Err(DataFusionError::NotImplemented(
            "Lance SQL extensions INSERT requires a source query".to_string(),
        ));
    };

    let enable_normalization = ctx.enable_ident_normalization();
    let table_ref = object_name_to_table_reference(table_name, enable_normalization)?;

    // Run the source query through DataFusion to get the input batches.
    let source_sql = source.to_string();
    let df = ctx.sql(&source_sql).await.map_err(|e| {
        DataFusionError::Execution(format!(
            "Lance SQL extensions failed to plan INSERT source query '{}': {}",
            source_sql, e
        ))
    })?;
    let batches = df.collect().await.map_err(|e| {
        DataFusionError::Execution(format!(
            "Lance SQL extensions failed to execute INSERT source query '{}': {}",
            source_sql, e
        ))
    })?;

    if batches.is_empty() {
        // Nothing to write, but still return a count DataFrame for consistency.
        return build_count_dataframe(0);
    }

    let affected_rows: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

    let schema = batches[0].schema();
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

    // Append into the target Lance dataset.
    let mut dataset = load_lance_dataset(ctx, &table_ref, "INSERT").await?;
    let uri = dataset.uri().to_string();

    dataset.append(reader, None).await.map_err(|e| {
        DataFusionError::Execution(format!("Lance INSERT append failed on '{}': {}", uri, e))
    })?;

    build_count_dataframe(affected_rows)
}

/// Delete rows from a Lance table identified by `table_ref` using a boolean
/// predicate expression understood by Lance.
async fn delete_from(
    ctx: &SessionContext,
    table_ref: TableReference,
    predicate: &str,
) -> Result<()> {
    let mut dataset = load_lance_dataset(ctx, &table_ref, "DELETE").await?;
    let uri = dataset.uri().to_string();

    dataset.delete(predicate).await.map_err(|e| {
        DataFusionError::Execution(format!("Lance DELETE failed on '{}': {}", uri, e))
    })?;

    // After the delete completes, future table lookups will open the latest
    // dataset version via `LanceSchemaProvider::table`, so we do not need to
    // update the registration in the SessionContext.
    Ok(())
}

/// Apply an update to a Lance table using `assignments` and an optional
/// boolean predicate `selection`.
///
/// Each assignment is a `(column, expr_sql)` pair where `expr_sql` is parsed
/// by Lance's update engine.
async fn update_table(
    ctx: &SessionContext,
    table_ref: TableReference,
    assignments: &[(String, String)],
    selection: Option<&str>,
) -> Result<()> {
    let dataset = load_lance_dataset(ctx, &table_ref, "UPDATE").await?;
    let uri = dataset.uri().to_string();
    let mut builder = UpdateBuilder::new(Arc::new(dataset));

    if let Some(predicate) = selection {
        builder = builder.update_where(predicate).map_err(|e| {
            DataFusionError::Execution(format!(
                "Lance UPDATE failed to parse WHERE predicate '{}': {}",
                predicate, e
            ))
        })?;
    }

    for (col, value_sql) in assignments {
        builder = builder.set(col, value_sql).map_err(|e| {
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

/// Perform an upsert-style merge between a target Lance table and a source
/// relation resolved via the same [`SessionContext`].
///
/// The merge uses a single equality key column and is configured as:
///
/// * `WHEN MATCHED THEN UPDATE SET *`
/// * `WHEN NOT MATCHED THEN INSERT *`
async fn merge_tables(
    ctx: &SessionContext,
    target: TableReference,
    source: TableReference,
    on_key: &str,
) -> Result<()> {
    let dataset = load_lance_dataset(ctx, &target, "MERGE target").await?;
    let uri = dataset.uri().to_string();

    // Build source stream by querying the source table through DataFusion.
    let source_df = ctx.table(source.clone()).await?;
    let source_stream = source_df.execute_stream().await.map_err(|e| {
        DataFusionError::Execution(format!("Failed to execute MERGE source query: {e}"))
    })?;

    // Configure MergeInsert as upsert: update all matching rows and insert
    // non-matching rows.
    let mut builder = MergeInsertBuilder::try_new(Arc::new(dataset), vec![on_key.to_string()])
        .map_err(|e| {
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

    job.execute(source_stream).await.map_err(|e| {
        DataFusionError::Execution(format!("Lance MERGE execution failed on '{}': {}", uri, e))
    })?;

    Ok(())
}

async fn handle_sql(ctx: &SessionContext, sql: &str) -> Result<DataFrame> {
    ctx.sql(sql).await
}

/// DELETE handling for [`execute_sql`].
///
/// This path executes the delete against the underlying Lance table and
/// returns a single-row [`DataFrame`] reporting the number of affected rows.
async fn handle_delete(ctx: &SessionContext, delete: Delete) -> Result<DataFrame> {
    // Only support the simplest form of DELETE for now.
    if !delete.tables.is_empty()
        || delete.using.is_some()
        || delete.returning.is_some()
        || !delete.order_by.is_empty()
        || delete.limit.is_some()
    {
        return Err(DataFusionError::NotImplemented(
            "Lance SQL extensions only supports `DELETE FROM <table> WHERE <expr>`".to_string(),
        ));
    }

    let table_name = simple_delete_target(delete.from)?;
    let enable_normalization = ctx.enable_ident_normalization();
    let table_ref = object_name_to_table_reference(table_name.clone(), enable_normalization)?;
    let table_sql = table_name.to_string();

    let predicate_sql = delete.selection.as_ref().map(|expr| expr.to_string());
    let predicate_for_count = predicate_sql.as_deref();

    let affected_rows = count_rows_for_table(ctx, &table_sql, predicate_for_count).await?;

    // Lance delete uses a textual predicate; default to a tautology when no
    // explicit selection is provided.
    let lance_predicate = predicate_sql.unwrap_or_else(|| "true".to_string());
    delete_from(ctx, table_ref, &lance_predicate).await?;

    build_count_dataframe(affected_rows)
}

fn simple_delete_target(from: FromTable) -> Result<ObjectName> {
    let mut from_vec = match from {
        FromTable::WithFromKeyword(v) => v,
        FromTable::WithoutKeyword(v) => v,
    };

    if from_vec.len() != 1 {
        return Err(DataFusionError::NotImplemented(
            "Lance SQL extensions only supports DELETE from a single table".to_string(),
        ));
    }

    let table_with_joins = from_vec.pop().unwrap();
    if !table_with_joins.joins.is_empty() {
        return Err(DataFusionError::NotImplemented(
            "Lance SQL extensions does not support DELETE with joins".to_string(),
        ));
    }

    match table_with_joins.relation {
        TableFactor::Table { name, .. } => Ok(name),
        other => Err(DataFusionError::NotImplemented(format!(
            "Lance SQL extensions only supports simple table references in DELETE, got: {:?}",
            other
        ))),
    }
}

/// UPDATE handling for [`execute_sql`].
///
/// This path executes the update via Lance and returns a single-row
/// [`DataFrame`] reporting the number of affected rows.
async fn handle_update(
    ctx: &SessionContext,
    table: TableWithJoins,
    assignments: &[Assignment],
    selection: Option<SQLExpr>,
) -> Result<DataFrame> {
    let enable_normalization = ctx.enable_ident_normalization();
    let table_name = simple_update_target(&table)?;
    let table_ref = object_name_to_table_reference(table_name.clone(), enable_normalization)?;
    let table_sql = table_name.to_string();

    let assignments = collect_update_assignments(assignments)?;
    let selection_sql = selection.map(|expr| expr.to_string());
    let selection_for_count = selection_sql.as_deref();

    let affected_rows = count_rows_for_table(ctx, &table_sql, selection_for_count).await?;

    update_table(ctx, table_ref, &assignments, selection_sql.as_deref()).await?;

    build_count_dataframe(affected_rows)
}

fn simple_update_target(table: &TableWithJoins) -> Result<ObjectName> {
    if !table.joins.is_empty() {
        return Err(DataFusionError::NotImplemented(
            "Lance SQL extensions UPDATE does not support joins".to_string(),
        ));
    }

    match &table.relation {
        TableFactor::Table { name, .. } => Ok(name.clone()),
        other => Err(DataFusionError::NotImplemented(format!(
            "Lance SQL extensions UPDATE only supports simple table references, got: {:?}",
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
                    "Lance SQL extensions only supports simple column targets".to_string(),
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

/// MERGE (upsert-style via [`MergeInsertBuilder`]) handling for [`execute_sql`].
///
/// This path performs an upsert between `target` and `source` tables and
/// returns a single-row [`DataFrame`] reporting the total number of affected
/// rows (matched updates plus new inserts).
async fn execute_lance_merge(ctx: &SessionContext, sql: &str) -> Result<DataFrame> {
    let enable_normalization = ctx.enable_ident_normalization();
    let (target_ref, source_ref, on_key) = parse_merge_components(sql, enable_normalization)?;

    let affected_rows = compute_merge_affected_rows(ctx, &target_ref, &source_ref, &on_key).await?;

    merge_tables(ctx, target_ref, source_ref, &on_key).await?;
    build_count_dataframe(affected_rows)
}

fn parse_merge_components(
    sql: &str,
    enable_ident_normalization: bool,
) -> Result<(TableReference, TableReference, String)> {
    // Flatten newlines to simplify parsing while keeping original casing.
    let sql = sql.replace(['\n', '\r'], " ");
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
        // Strip surrounding parentheses.
        s = s.trim_matches(|c: char| c == '(' || c == ')');
        if s.is_empty() {
            return None;
        }
        // Take last component after dot, e.g. "t.id" -> "id".
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
            "MERGE ON only supports equality on the same column name on both sides (e.g. t.id = s.id)"
                .to_string(),
        ));
    }

    Ok(left_col)
}

async fn compute_merge_affected_rows(
    ctx: &SessionContext,
    target_ref: &TableReference,
    source_ref: &TableReference,
    on_key: &str,
) -> Result<i64> {
    let target_df = ctx.table(target_ref.clone()).await?;
    let source_df = ctx.table(source_ref.clone()).await?;

    let matched_df = target_df.clone().join(
        source_df.clone(),
        JoinType::Inner,
        &[on_key],
        &[on_key],
        None,
    )?;
    let matched_count = matched_df.count().await? as i64;

    let not_matched_df =
        source_df.join(target_df, JoinType::LeftAnti, &[on_key], &[on_key], None)?;
    let not_matched_count = not_matched_df.count().await? as i64;

    Ok(matched_count + not_matched_count)
}

async fn load_lance_dataset(
    ctx: &SessionContext,
    table_ref: &TableReference,
    operation: &str,
) -> Result<Dataset> {
    let provider = ctx.table_provider(table_ref.clone()).await?;

    let lance_provider = provider
        .as_any()
        .downcast_ref::<LanceTableProvider>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Lance {operation} only supports tables backed by LanceTableProvider",
            ))
        })?;

    Ok(lance_provider.dataset().clone())
}

async fn count_rows_for_table(
    ctx: &SessionContext,
    table_sql: &str,
    predicate_sql: Option<&str>,
) -> Result<i64> {
    let query = if let Some(predicate) = predicate_sql {
        format!("SELECT COUNT(*) AS count FROM {table_sql} WHERE {predicate}")
    } else {
        format!("SELECT COUNT(*) AS count FROM {table_sql}")
    };

    let df = ctx.sql(&query).await?;
    let batches = df.collect().await?;

    if batches.is_empty() {
        return Err(DataFusionError::Execution(
            "Lance SQL extensions expected COUNT(*) query to return at least one row".to_string(),
        ));
    }

    let batch = &batches[0];
    if batch.num_columns() != 1 || batch.num_rows() != 1 {
        return Err(DataFusionError::Execution(
            "Lance SQL extensions expected COUNT(*) query to return a single scalar value"
                .to_string(),
        ));
    }

    let array = batch.column(0);
    let any = array.as_any();

    if let Some(values) = any.downcast_ref::<Int64Array>() {
        Ok(values.value(0))
    } else if let Some(values) = any.downcast_ref::<arrow_array::UInt64Array>() {
        Ok(values.value(0) as i64)
    } else {
        Err(DataFusionError::Execution(format!(
            "Lance SQL extensions expected COUNT(*) to return Int64/UInt64, got {:?}",
            array.data_type(),
        )))
    }
}

fn build_count_dataframe(affected_rows: i64) -> Result<DataFrame> {
    let values = Int64Array::from(vec![affected_rows]);
    let array: ArrayRef = Arc::new(values);

    DataFrame::from_columns(vec![("count", array)])
}
