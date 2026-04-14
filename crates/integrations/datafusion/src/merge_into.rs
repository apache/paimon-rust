// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! MERGE INTO execution for data evolution tables.
//!
//! This module provides the DataFusion-specific SQL parsing and JOIN execution layer.
//! The engine-agnostic merge logic (file metadata lookup, row grouping, reading originals,
//! applying updates, writing partial files, committing) lives in
//! [`paimon::table::DataEvolutionWriter`].

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, UInt64Array};
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{
    AssignmentTarget, Merge, MergeAction, MergeClauseKind, MergeInsertKind, TableFactor,
};

use paimon::table::{DataEvolutionWriter, Table};

use crate::error::to_datafusion_error;

/// Maximum number of retries when MERGE INTO conflicts with concurrent compaction.
const MERGE_INTO_MAX_RETRIES: u32 = 5;

/// Execute a MERGE INTO statement on a data evolution Paimon table.
///
/// The `ctx` must already have the target and source tables registered.
/// The `merge` AST node comes from sqlparser.
///
/// When a concurrent compaction rewrites files that this MERGE INTO references,
/// the commit will fail with a row ID conflict. In that case, the entire operation
/// (scan + write + commit) is retried from scratch against the new file layout.
pub(crate) async fn execute_merge_into(
    ctx: &SessionContext,
    merge: &Merge,
    table: Table,
) -> DFResult<DataFrame> {
    for retry in 0..MERGE_INTO_MAX_RETRIES {
        match execute_merge_into_once(ctx, merge, &table).await {
            Ok(df) => return Ok(df),
            Err(e) if is_row_id_conflict(&e) => {
                if retry + 1 >= MERGE_INTO_MAX_RETRIES {
                    return Err(DataFusionError::External(Box::new(std::io::Error::other(
                        format!(
                            "MERGE INTO failed after {} retries due to concurrent compaction: {}",
                            MERGE_INTO_MAX_RETRIES, e
                        ),
                    ))));
                }
                // Retry: re-execute the entire MERGE INTO against the new file layout
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!()
}

/// Check if a DataFusion error is caused by a row ID conflict during commit.
pub(crate) fn is_row_id_conflict(err: &DataFusionError) -> bool {
    match err {
        DataFusionError::External(e) => e.to_string().contains("Row ID conflict"),
        _ => false,
    }
}

/// Single attempt of MERGE INTO execution.
async fn execute_merge_into_once(
    ctx: &SessionContext,
    merge: &Merge,
    table: &Table,
) -> DFResult<DataFrame> {
    // 1. Parse all MERGE clauses
    let parsed = extract_merge_clauses(merge)?;

    // Validate preconditions early and create writer (before executing any SQL)
    let update_writer = if let Some(ref upd) = parsed.update {
        Some(DataEvolutionWriter::new(table, upd.columns.clone()).map_err(to_datafusion_error)?)
    } else {
        None
    };

    let (target_ref, target_alias) = extract_table_ref(&merge.table)?;
    let (source_ref, source_alias) = extract_source_ref(&merge.source)?;
    let on_condition = merge.on.to_string();
    let t_alias = target_alias.as_deref().unwrap_or(&target_ref);
    let s_alias = source_alias.as_deref().unwrap_or(&source_ref);

    // 2. Build a single LEFT JOIN: source LEFT JOIN target
    //    _ROW_ID IS NOT NULL → matched (UPDATE path)
    //    _ROW_ID IS NULL     → not matched (INSERT path)
    let mut select_parts = vec![format!("{t_alias}.\"_ROW_ID\"")];

    // Add update expressions (prefixed to avoid collisions)
    if let Some(ref upd) = parsed.update {
        for (col, expr) in upd.columns.iter().zip(upd.exprs.iter()) {
            select_parts.push(format!("{expr} AS \"__upd_{col}\""));
        }
    }

    // Add source columns for INSERT path (all source columns via s.*)
    // We also need insert expressions if they differ from source columns
    if !parsed.inserts.is_empty() {
        select_parts.push(format!("{s_alias}.*"));
    }

    let select_clause = select_parts.join(", ");
    // Safety: all interpolated values (select_clause, source_ref, s_alias, t_alias, on_condition)
    // originate from sqlparser AST's Display impl, so they are well-formed SQL fragments.
    let join_sql = format!(
        "SELECT {select_clause} FROM {source_ref} AS {s_alias} \
         LEFT JOIN {target_ref} AS {t_alias} ON {on_condition}"
    );

    let join_result = ctx.sql(&join_sql).await?.collect().await?;

    // 3. Split by _ROW_ID null/not-null
    let mut all_messages = Vec::new();
    let mut total_count: u64 = 0;

    // Separate matched and not-matched rows
    let (matched_batches, not_matched_batches) = split_by_row_id(&join_result)?;

    // 4. Handle matched rows (UPDATE)
    if let Some(mut writer) = update_writer {
        let upd = parsed.update.as_ref().unwrap();
        let matched_count: usize = matched_batches.iter().map(|b| b.num_rows()).sum();
        if matched_count > 0 {
            // Extract _ROW_ID + update columns (rename __upd_X → X)
            let update_batches = project_update_columns(&matched_batches, &upd.columns)?;
            for batch in update_batches {
                writer
                    .add_matched_batch(batch)
                    .map_err(to_datafusion_error)?;
            }
            let update_messages = writer.prepare_commit().await.map_err(to_datafusion_error)?;
            all_messages.extend(update_messages);
            total_count += matched_count as u64;
        }
    }

    // 5. Handle not-matched rows (INSERT)
    if !parsed.inserts.is_empty() {
        // Collect the exact set of injected column names to strip from JOIN result
        let mut injected_columns: Vec<String> = vec!["_ROW_ID".to_string()];
        if let Some(ref upd) = parsed.update {
            for col in &upd.columns {
                injected_columns.push(format!("__upd_{col}"));
            }
        }
        // Table schema field names for reordering INSERT columns
        let table_fields: Vec<String> = table
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        let insert_batches = build_insert_batches(
            ctx,
            &not_matched_batches,
            &parsed.inserts,
            s_alias,
            &injected_columns,
            &table_fields,
        )
        .await?;
        let insert_count: usize = insert_batches.iter().map(|b| b.num_rows()).sum();
        if insert_count > 0 {
            let mut table_write = table
                .new_write_builder()
                .new_write()
                .map_err(to_datafusion_error)?;
            for batch in &insert_batches {
                table_write
                    .write_arrow_batch(batch)
                    .await
                    .map_err(to_datafusion_error)?;
            }
            let insert_messages = table_write
                .prepare_commit()
                .await
                .map_err(to_datafusion_error)?;
            all_messages.extend(insert_messages);
            total_count += insert_count as u64;
        }
    }

    // 6. Commit all messages atomically
    if !all_messages.is_empty() {
        let commit = table.new_write_builder().new_commit();
        commit
            .commit(all_messages)
            .await
            .map_err(to_datafusion_error)?;
    }

    ok_result(ctx, total_count)
}

/// Split join result into matched (_ROW_ID not null) and not-matched (_ROW_ID null) batches.
fn split_by_row_id(batches: &[RecordBatch]) -> DFResult<(Vec<RecordBatch>, Vec<RecordBatch>)> {
    let mut matched = Vec::new();
    let mut not_matched = Vec::new();

    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let row_id_col = batch.column_by_name("_ROW_ID").ok_or_else(|| {
            DataFusionError::Internal("_ROW_ID column not found in join result".to_string())
        })?;

        let is_not_null = compute::is_not_null(row_id_col)?;
        let is_null = compute::is_null(row_id_col)?;

        let matched_batch = compute::filter_record_batch(batch, &is_not_null)?;
        if matched_batch.num_rows() > 0 {
            matched.push(matched_batch);
        }

        let not_matched_batch = compute::filter_record_batch(batch, &is_null)?;
        if not_matched_batch.num_rows() > 0 {
            not_matched.push(not_matched_batch);
        }
    }

    Ok((matched, not_matched))
}

/// Extract _ROW_ID + __upd_X columns from matched batches, renaming __upd_X → X.
pub(crate) fn project_update_columns(
    batches: &[RecordBatch],
    update_columns: &[String],
) -> DFResult<Vec<RecordBatch>> {
    let mut result = Vec::new();
    for batch in batches {
        let row_id_idx = batch
            .schema()
            .index_of("_ROW_ID")
            .map_err(|e| DataFusionError::Internal(format!("_ROW_ID not found: {e}")))?;

        let mut columns = vec![batch.column(row_id_idx).clone()];
        let mut fields = vec![batch.schema().field(row_id_idx).clone()];

        for col in update_columns {
            let prefixed = format!("__upd_{col}");
            let idx = batch.schema().index_of(&prefixed).map_err(|e| {
                DataFusionError::Internal(format!("Column {prefixed} not found: {e}"))
            })?;
            columns.push(batch.column(idx).clone());
            fields.push(Field::new(
                col,
                batch.schema().field(idx).data_type().clone(),
                true,
            ));
        }

        let schema = Arc::new(Schema::new(fields));
        let projected = RecordBatch::try_new(schema, columns)?;
        result.push(projected);
    }
    Ok(result)
}

/// Build insert batches from not-matched rows, applying INSERT clause projections and predicates.
async fn build_insert_batches(
    ctx: &SessionContext,
    not_matched_batches: &[RecordBatch],
    inserts: &[MergeInsertClause],
    s_alias: &str,
    injected_columns: &[String],
    table_fields: &[String],
) -> DFResult<Vec<RecordBatch>> {
    if not_matched_batches.is_empty() || not_matched_batches.iter().all(|b| b.num_rows() == 0) {
        return Ok(Vec::new());
    }

    // Strip injected columns (_ROW_ID, __upd_*) — keep only source columns
    let source_batches = strip_non_source_columns(not_matched_batches, injected_columns)?;

    // Register as temp table for SQL-based projection/filtering
    let first_schema = source_batches[0].schema();
    let mem_table = MemTable::try_new(first_schema, vec![source_batches])?;
    let tmp_name = format!("__merge_not_matched_{}", std::process::id());
    ctx.register_table(&tmp_name, Arc::new(mem_table))?;

    let result = build_insert_batches_inner(ctx, inserts, s_alias, &tmp_name, table_fields).await;

    // Always clean up temp table, even on error
    let _ = ctx.deregister_table(&tmp_name);

    result
}

/// Execute INSERT clause queries against the registered temp table.
async fn build_insert_batches_inner(
    ctx: &SessionContext,
    inserts: &[MergeInsertClause],
    s_alias: &str,
    tmp_name: &str,
    table_fields: &[String],
) -> DFResult<Vec<RecordBatch>> {
    let mut all_batches = Vec::new();
    let mut consumed_predicates: Vec<String> = Vec::new();

    for ins in inserts {
        let mut conditions = Vec::new();
        for prev in &consumed_predicates {
            conditions.push(format!("NOT ({prev})"));
        }
        if let Some(ref pred) = ins.predicate {
            conditions.push(pred.clone());
            consumed_predicates.push(pred.clone());
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conditions.join(" AND "))
        };

        let select_clause = insert_select_clause(ins, table_fields);
        let sql = format!("SELECT {select_clause} FROM {tmp_name} AS {s_alias}{where_clause}");

        let batches = ctx.sql(&sql).await?.collect().await?;
        all_batches.extend(batches);
    }

    Ok(all_batches)
}

/// Remove injected columns from batches, keeping only source columns.
fn strip_non_source_columns(
    batches: &[RecordBatch],
    injected_columns: &[String],
) -> DFResult<Vec<RecordBatch>> {
    let mut result = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        let mut indices = Vec::new();
        let mut fields = Vec::new();
        for (i, field) in schema.fields().iter().enumerate() {
            if injected_columns.contains(field.name()) {
                continue;
            }
            indices.push(i);
            fields.push(field.as_ref().clone());
        }
        let new_schema = Arc::new(Schema::new(fields));
        let columns: Vec<_> = indices.iter().map(|&i| batch.column(i).clone()).collect();
        let projected = RecordBatch::try_new(new_schema, columns)?;
        result.push(projected);
    }
    Ok(result)
}

/// Build the SELECT clause for an INSERT clause, ordered by table schema fields.
///
/// When the INSERT specifies explicit columns (`INSERT (col2, col1) VALUES (expr2, expr1)`),
/// the output must be reordered to match the table schema so that `write_arrow_batch`
/// (which reads columns by positional index) maps them correctly.
fn insert_select_clause(ins: &MergeInsertClause, table_fields: &[String]) -> String {
    if ins.columns.is_empty() && ins.value_exprs.is_empty() {
        "*".to_string()
    } else {
        // Build column_name -> expression mapping from the INSERT clause
        let col_expr_map: HashMap<String, &str> = ins
            .columns
            .iter()
            .zip(ins.value_exprs.iter())
            .map(|(col, expr)| (col.to_lowercase(), expr.as_str()))
            .collect();

        // Emit SELECT in table schema order
        table_fields
            .iter()
            .map(|field| {
                let key = field.to_lowercase();
                match col_expr_map.get(&key) {
                    Some(expr) => format!("{expr} AS \"{field}\""),
                    // Column not in INSERT list — fill with NULL
                    None => format!("NULL AS \"{field}\""),
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Parsed WHEN NOT MATCHED THEN INSERT clause.
struct MergeInsertClause {
    /// Column names from INSERT (col1, col2). Empty means INSERT *.
    columns: Vec<String>,
    /// SQL expressions from VALUES(...).
    value_exprs: Vec<String>,
    /// Optional AND predicate (SQL string).
    predicate: Option<String>,
}

/// Parsed WHEN MATCHED THEN UPDATE clause.
struct MergeUpdateClause {
    columns: Vec<String>,
    exprs: Vec<String>,
}

/// Parsed merge clauses.
struct ParsedMergeClauses {
    update: Option<MergeUpdateClause>,
    inserts: Vec<MergeInsertClause>,
}

/// Extract UPDATE and INSERT clauses from the MERGE AST.
fn extract_merge_clauses(merge: &Merge) -> DFResult<ParsedMergeClauses> {
    let mut update: Option<MergeUpdateClause> = None;
    let mut inserts: Vec<MergeInsertClause> = Vec::new();

    for clause in &merge.clauses {
        match clause.clause_kind {
            MergeClauseKind::Matched => {
                if update.is_some() {
                    return Err(DataFusionError::Plan(
                        "Multiple WHEN MATCHED clauses are not yet supported".to_string(),
                    ));
                }
                if clause.predicate.is_some() {
                    return Err(DataFusionError::Plan(
                        "WHEN MATCHED AND <predicate> is not yet supported".to_string(),
                    ));
                }
                match &clause.action {
                    MergeAction::Update(update_expr) => {
                        let mut columns = Vec::new();
                        let mut exprs = Vec::new();
                        for assignment in &update_expr.assignments {
                            let col_name = match &assignment.target {
                                AssignmentTarget::ColumnName(name) => name
                                    .0
                                    .last()
                                    .and_then(|p| p.as_ident())
                                    .map(|id| id.value.clone())
                                    .ok_or_else(|| {
                                        DataFusionError::Plan(format!(
                                            "Invalid column name in SET: {name}"
                                        ))
                                    })?,
                                AssignmentTarget::Tuple(_) => {
                                    return Err(DataFusionError::Plan(
                                        "Tuple assignment in MERGE INTO SET is not supported"
                                            .to_string(),
                                    ));
                                }
                            };
                            columns.push(col_name);
                            exprs.push(assignment.value.to_string());
                        }
                        update = Some(MergeUpdateClause { columns, exprs });
                    }
                    MergeAction::Delete { .. } => {
                        return Err(DataFusionError::Plan(
                            "WHEN MATCHED THEN DELETE is not supported for data evolution tables"
                                .to_string(),
                        ));
                    }
                    MergeAction::Insert(_) => {
                        return Err(DataFusionError::Plan(
                            "WHEN MATCHED THEN INSERT is not valid SQL".to_string(),
                        ));
                    }
                }
            }
            MergeClauseKind::NotMatched | MergeClauseKind::NotMatchedByTarget => {
                match &clause.action {
                    MergeAction::Insert(insert_expr) => {
                        let columns: Vec<String> =
                            insert_expr.columns.iter().map(|c| c.to_string()).collect();

                        let value_exprs = match &insert_expr.kind {
                            MergeInsertKind::Values(values) => {
                                if values.rows.is_empty() {
                                    return Err(DataFusionError::Plan(
                                        "INSERT VALUES must have at least one row".to_string(),
                                    ));
                                }
                                values.rows[0].iter().map(|e| e.to_string()).collect()
                            }
                            MergeInsertKind::Row => {
                                // INSERT ROW — BigQuery syntax, treat as INSERT *
                                Vec::new()
                            }
                        };

                        let predicate = clause.predicate.as_ref().map(|p| p.to_string());

                        inserts.push(MergeInsertClause {
                            columns,
                            value_exprs,
                            predicate,
                        });
                    }
                    _ => {
                        return Err(DataFusionError::Plan(
                            "WHEN NOT MATCHED only supports INSERT".to_string(),
                        ));
                    }
                }
            }
            MergeClauseKind::NotMatchedBySource => {
                return Err(DataFusionError::Plan(
                    "WHEN NOT MATCHED BY SOURCE is not yet supported for data evolution MERGE INTO"
                        .to_string(),
                ));
            }
        }
    }

    if update.is_none() && inserts.is_empty() {
        return Err(DataFusionError::Plan(
            "MERGE INTO requires at least one WHEN MATCHED or WHEN NOT MATCHED clause".to_string(),
        ));
    }

    Ok(ParsedMergeClauses { update, inserts })
}

/// Extract table name and optional alias from a TableFactor.
fn extract_table_ref(table: &TableFactor) -> DFResult<(String, Option<String>)> {
    match table {
        TableFactor::Table { name, alias, .. } => {
            let table_name = name.to_string();
            let alias_name = alias.as_ref().map(|a| a.name.value.clone());
            Ok((table_name, alias_name))
        }
        other => Err(DataFusionError::Plan(format!(
            "Unsupported table reference in MERGE INTO: {other}"
        ))),
    }
}

/// Extract source reference (table or subquery) from a TableFactor.
fn extract_source_ref(source: &TableFactor) -> DFResult<(String, Option<String>)> {
    match source {
        TableFactor::Table { name, alias, .. } => {
            let table_name = name.to_string();
            let alias_name = alias.as_ref().map(|a| a.name.value.clone());
            Ok((table_name, alias_name))
        }
        TableFactor::Derived {
            subquery, alias, ..
        } => {
            let subquery_sql = format!("({subquery})");
            let alias_name = alias.as_ref().map(|a| a.name.value.clone());
            if alias_name.is_none() {
                return Err(DataFusionError::Plan(
                    "Subquery source in MERGE INTO must have an alias".to_string(),
                ));
            }
            Ok((subquery_sql, alias_name))
        }
        other => Err(DataFusionError::Plan(format!(
            "Unsupported source in MERGE INTO: {other}"
        ))),
    }
}

/// Return a DataFrame with a single "count" column.
pub(crate) fn ok_result(ctx: &SessionContext, count: u64) -> DFResult<DataFrame> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "count",
        ArrowDataType::UInt64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt64Array::from(vec![count]))],
    )?;
    ctx.read_batch(batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;
    use paimon::catalog::Identifier;
    use paimon::io::FileIOBuilder;
    use paimon::spec::{DataType, IntType, Schema as PaimonSchema, TableSchema, VarCharType};

    use crate::PaimonTableProvider;

    async fn setup_data_evolution_table() -> (SessionContext, Table) {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/test_merge_into";
        file_io
            .mkdirs(&format!("{table_path}/snapshot/"))
            .await
            .unwrap();
        file_io
            .mkdirs(&format!("{table_path}/manifest/"))
            .await
            .unwrap();

        let schema = PaimonSchema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("name", DataType::VarChar(VarCharType::string_type()))
            .column("value", DataType::Int(IntType::new()))
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true")
            .build()
            .unwrap();
        let table_schema = TableSchema::new(0, &schema);
        let table = Table::new(
            file_io,
            Identifier::new("default", "target"),
            table_path.to_string(),
            table_schema,
            None,
        );

        let provider = PaimonTableProvider::try_new(table.clone()).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("target", Arc::new(provider)).unwrap();

        (ctx, table)
    }

    fn parse_merge(sql: &str) -> Merge {
        let dialect = GenericDialect {};
        let stmts = Parser::parse_sql(&dialect, sql).unwrap();
        match stmts.into_iter().next().unwrap() {
            datafusion::sql::sqlparser::ast::Statement::Merge(m) => m,
            _ => panic!("Expected MERGE statement"),
        }
    }

    #[tokio::test]
    async fn test_merge_into_updates_matched_rows() {
        let (ctx, table) = setup_data_evolution_table().await;

        // Insert initial data
        ctx.sql("INSERT INTO target (id, name, value) VALUES (1, 'alice', 10), (2, 'bob', 20), (3, 'charlie', 30)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Create source table with updates
        ctx.sql(
            "CREATE TABLE source (id INT, name VARCHAR) AS VALUES (1, 'ALICE'), (3, 'CHARLIE')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

        // Execute MERGE INTO
        let merge = parse_merge(
            "MERGE INTO target t USING source s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name",
        );
        execute_merge_into(&ctx, &merge, table).await.unwrap();

        let batches = ctx
            .sql("SELECT id, name, value FROM target ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let mut rows = Vec::new();
        for batch in &batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int32Array>()
                .unwrap();
            let names = batch
                .column(1)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .unwrap();
            let values = batch
                .column(2)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int32Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                rows.push((ids.value(i), names.value(i).to_string(), values.value(i)));
            }
        }

        assert_eq!(
            rows,
            vec![
                (1, "ALICE".to_string(), 10),
                (2, "bob".to_string(), 20),
                (3, "CHARLIE".to_string(), 30),
            ]
        );
    }

    #[tokio::test]
    async fn test_merge_into_no_matches() {
        let (ctx, table) = setup_data_evolution_table().await;

        ctx.sql("INSERT INTO target (id, name, value) VALUES (1, 'alice', 10)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        ctx.sql("CREATE TABLE source (id INT, name VARCHAR) AS VALUES (99, 'nobody')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let merge = parse_merge(
            "MERGE INTO target t USING source s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name",
        );
        let result = execute_merge_into(&ctx, &merge, table).await.unwrap();
        let batches = result.collect().await.unwrap();
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_merge_into_rejects_non_data_evolution_table() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/test_merge_reject";
        file_io
            .mkdirs(&format!("{table_path}/snapshot/"))
            .await
            .unwrap();
        file_io
            .mkdirs(&format!("{table_path}/manifest/"))
            .await
            .unwrap();

        let schema = PaimonSchema::builder()
            .column("id", DataType::Int(IntType::new()))
            .build()
            .unwrap();
        let table_schema = TableSchema::new(0, &schema);
        let table = Table::new(
            file_io,
            Identifier::new("default", "t"),
            table_path.to_string(),
            table_schema,
            None,
        );

        let ctx = SessionContext::new();
        let merge = parse_merge(
            "MERGE INTO t USING s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET id = s.id",
        );
        let result = execute_merge_into(&ctx, &merge, table).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("data-evolution.enabled"));
    }
}
