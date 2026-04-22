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

//! MERGE INTO execution for Paimon tables.
//!
//! Supports two execution paths:
//! - **Data evolution tables**: partial-column writes via [`paimon::table::DataEvolutionWriter`].
//! - **Append-only tables** (no PK, no deletion vectors): copy-on-write file rewriting
//!   via [`paimon::table::CopyOnWriteMergeWriter`].

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, RecordBatch, UInt32Array, UInt64Array};
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{
    AssignmentTarget, Merge, MergeAction, MergeClauseKind, MergeInsertKind, TableFactor,
};
use futures::TryStreamExt;

use paimon::spec::{datums_to_binary_row, extract_datum_from_arrow, CoreOptions};
use paimon::table::{CopyOnWriteMergeWriter, DataEvolutionWriter, DataSplitBuilder, Table};

use crate::error::to_datafusion_error;

/// Maximum number of retries when DML conflicts with concurrent compaction.
const DML_MAX_RETRIES: u32 = 5;

/// Quote a SQL identifier by wrapping in double-quotes and escaping embedded quotes.
pub(crate) fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

static COW_TABLE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn next_cow_table_name(prefix: &str) -> String {
    let id = COW_TABLE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{id}")
}

/// RAII guard that deregisters a MemTable from the SessionContext on drop.
/// Prevents leaks when the future is cancelled between register and deregister.
pub(crate) struct CowTableGuard {
    ctx: SessionContext,
    table_name: String,
}

impl CowTableGuard {
    pub(crate) fn new(ctx: &SessionContext, table_name: String) -> Self {
        Self {
            ctx: ctx.clone(),
            table_name,
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.table_name
    }
}

impl Drop for CowTableGuard {
    fn drop(&mut self) {
        let _ = self.ctx.deregister_table(&self.table_name);
    }
}

/// Retry a DML operation on conflict, using `is_retryable` to detect retryable errors.
pub(crate) async fn retry_on_conflict<F, Fut>(
    op_name: &str,
    is_retryable: fn(&DataFusionError) -> bool,
    mut action: F,
) -> DFResult<DataFrame>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = DFResult<DataFrame>>,
{
    let mut last_err = None;
    for _ in 0..DML_MAX_RETRIES {
        match action().await {
            Ok(df) => return Ok(df),
            Err(e) if is_retryable(&e) => {
                last_err = Some(e);
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    Err(DataFusionError::External(Box::new(std::io::Error::other(
        format!(
            "{op_name} failed after {DML_MAX_RETRIES} retries due to concurrent compaction: {}",
            last_err.unwrap()
        ),
    ))))
}

/// Execute a MERGE INTO statement on a Paimon table.
///
/// Dispatches to the appropriate execution path based on table type:
/// - Data evolution tables → partial-column writes via `DataEvolutionWriter`
/// - Append-only tables (no PK) → copy-on-write file rewriting via `CopyOnWriteMergeWriter`
pub(crate) async fn execute_merge_into(
    ctx: &SessionContext,
    merge: &Merge,
    table: Table,
) -> DFResult<DataFrame> {
    let schema = table.schema();
    let core_options = CoreOptions::new(schema.options());

    if core_options.data_evolution_enabled() {
        execute_data_evolution_merge(ctx, merge, table).await
    } else if schema.trimmed_primary_keys().is_empty() {
        execute_cow_merge(ctx, merge, table).await
    } else {
        Err(DataFusionError::Plan(
            "MERGE INTO on primary-key tables without data-evolution is not supported".to_string(),
        ))
    }
}

/// Check if a DataFusion error is caused by a row ID conflict during commit.
pub(crate) fn is_row_id_conflict(err: &DataFusionError) -> bool {
    match err {
        DataFusionError::External(e) => e.to_string().contains("Row ID conflict"),
        _ => false,
    }
}

/// Check if a DataFusion error is caused by a delete conflict during commit.
pub(crate) fn is_delete_conflict(err: &DataFusionError) -> bool {
    match err {
        DataFusionError::External(e) => e.to_string().contains("Delete conflict"),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Data evolution path (existing)
// ---------------------------------------------------------------------------

/// Execute MERGE INTO on a data evolution table with retry on row ID conflict.
async fn execute_data_evolution_merge(
    ctx: &SessionContext,
    merge: &Merge,
    table: Table,
) -> DFResult<DataFrame> {
    retry_on_conflict("MERGE INTO", is_row_id_conflict, || {
        execute_merge_into_once(ctx, merge, &table)
    })
    .await
}

// ---------------------------------------------------------------------------
// Copy-on-Write path (append-only tables, no PK)
// ---------------------------------------------------------------------------

/// Parsed CoW merge clauses — supports UPDATE, DELETE, and INSERT.
struct CowMergeClauses {
    /// Ordered list of WHEN MATCHED clauses (preserves SQL ordering for correct semantics).
    matched: Vec<CowMatchedClause>,
    inserts: Vec<MergeInsertClause>,
}

/// A single WHEN MATCHED clause with optional predicate.
struct CowMatchedClause {
    action: CowMatchedAction,
    predicate: Option<String>,
}

enum CowMatchedAction {
    Update(MergeUpdateClause),
    Delete,
}

/// Parse MERGE clauses for the CoW path (supports DELETE unlike the data-evolution parser).
fn extract_cow_merge_clauses(merge: &Merge) -> DFResult<CowMergeClauses> {
    let mut matched: Vec<CowMatchedClause> = Vec::new();
    let mut inserts: Vec<MergeInsertClause> = Vec::new();

    for clause in &merge.clauses {
        match clause.clause_kind {
            MergeClauseKind::Matched => {
                let predicate = clause.predicate.as_ref().map(|p| p.to_string());
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
                        matched.push(CowMatchedClause {
                            action: CowMatchedAction::Update(MergeUpdateClause { columns, exprs }),
                            predicate,
                        });
                    }
                    MergeAction::Delete { .. } => {
                        matched.push(CowMatchedClause {
                            action: CowMatchedAction::Delete,
                            predicate,
                        });
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
                            MergeInsertKind::Row => Vec::new(),
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
                    "WHEN NOT MATCHED BY SOURCE is not yet supported for CoW MERGE INTO"
                        .to_string(),
                ));
            }
        }
    }

    if matched.is_empty() && inserts.is_empty() {
        return Err(DataFusionError::Plan(
            "MERGE INTO requires at least one WHEN MATCHED or WHEN NOT MATCHED clause".to_string(),
        ));
    }

    Ok(CowMergeClauses { matched, inserts })
}

/// Execute MERGE INTO on an append-only table with retry on delete conflict.
async fn execute_cow_merge(
    ctx: &SessionContext,
    merge: &Merge,
    table: Table,
) -> DFResult<DataFrame> {
    retry_on_conflict("CoW MERGE INTO", is_delete_conflict, || {
        execute_cow_merge_once(ctx, merge, &table)
    })
    .await
}

/// Execute a single attempt of CoW MERGE INTO.
async fn execute_cow_merge_once(
    ctx: &SessionContext,
    merge: &Merge,
    table: &Table,
) -> DFResult<DataFrame> {
    let mut clauses = extract_cow_merge_clauses(merge)?;

    // Collect the union of all update columns across matched clauses (preserving order)
    let mut update_columns: Vec<String> = Vec::new();
    for mc in &clauses.matched {
        if let CowMatchedAction::Update(upd) = &mc.action {
            for col in &upd.columns {
                if !update_columns.contains(col) {
                    update_columns.push(col.clone());
                }
            }
        }
    }

    let (source_ref, source_alias) = extract_source_ref(&merge.source)?;
    let (target_ref, target_alias) = extract_table_ref(&merge.table)?;
    let on_condition = merge.on.to_string();
    let s_alias = source_alias.as_deref().unwrap_or(&source_ref);
    let t_alias = target_alias.as_deref().unwrap_or("__cow_t");

    // Build partition filter from source data to avoid scanning all partitions
    let partition_set = build_source_partition_set(ctx, table, &source_ref, s_alias).await?;

    let mut writer = CopyOnWriteMergeWriter::new(table, update_columns.clone(), partition_set)
        .await
        .map_err(to_datafusion_error)?;

    // Rewrite ON condition and all clause expressions: replace original table references with aliases
    let on_condition = rewrite_condition(&on_condition, &target_ref, t_alias, &source_ref, s_alias);
    for mc in &mut clauses.matched {
        if let Some(ref mut pred) = mc.predicate {
            *pred = rewrite_condition(pred, &target_ref, t_alias, &source_ref, s_alias);
        }
        if let CowMatchedAction::Update(ref mut upd) = mc.action {
            for expr in &mut upd.exprs {
                *expr = rewrite_condition(expr, &target_ref, t_alias, &source_ref, s_alias);
            }
        }
    }
    for ins in &mut clauses.inserts {
        for expr in &mut ins.value_exprs {
            *expr = rewrite_condition(expr, &target_ref, t_alias, &source_ref, s_alias);
        }
        if let Some(ref mut pred) = ins.predicate {
            *pred = rewrite_condition(pred, &target_ref, t_alias, &source_ref, s_alias);
        }
    }

    // Read each target file individually, attach __paimon_file_idx and __paimon_row_offset
    let (has_target_data, cow_target_guard) =
        register_cow_target_table(ctx, table, &writer).await?;

    let merge_ctx = CowMergeContext {
        source_ref: &source_ref,
        s_alias,
        t_alias,
        on_condition: &on_condition,
        has_target_data,
        cow_target_name: cow_target_guard.name(),
        update_columns: &update_columns,
    };

    let result = execute_cow_merge_inner(ctx, &clauses, &mut writer, table, &merge_ctx).await;

    drop(cow_target_guard);

    let (insert_messages, total_count) = result?;

    // CoW rewrite: prepare_commit consumes the writer
    let cow_messages = writer.prepare_commit().await.map_err(to_datafusion_error)?;

    let mut all_messages = cow_messages;
    all_messages.extend(insert_messages);

    if !all_messages.is_empty() {
        let commit = table.new_write_builder().new_commit();
        commit
            .commit(all_messages)
            .await
            .map_err(to_datafusion_error)?;
    }

    ok_result(ctx, total_count)
}

/// Context for CoW merge inner execution — groups join-related parameters.
struct CowMergeContext<'a> {
    source_ref: &'a str,
    s_alias: &'a str,
    t_alias: &'a str,
    on_condition: &'a str,
    has_target_data: bool,
    cow_target_name: &'a str,
    update_columns: &'a [String],
}

/// Inner function that populates the CoW writer with matched operations and handles INSERT.
/// Returns (insert_commit_messages, total_affected_count).
async fn execute_cow_merge_inner(
    ctx: &SessionContext,
    clauses: &CowMergeClauses,
    writer: &mut CopyOnWriteMergeWriter,
    table: &Table,
    merge_ctx: &CowMergeContext<'_>,
) -> DFResult<(Vec<paimon::table::CommitMessage>, u64)> {
    let source_ref = merge_ctx.source_ref;
    let s_alias = merge_ctx.s_alias;
    let t_alias = merge_ctx.t_alias;
    let on_condition = merge_ctx.on_condition;
    let has_target_data = merge_ctx.has_target_data;
    let cow_target_name = merge_ctx.cow_target_name;
    let update_columns = merge_ctx.update_columns;
    let mut insert_messages = Vec::new();
    let mut total_count: u64 = 0;

    if has_target_data && !clauses.matched.is_empty() {
        let mut update_value_batches: Vec<RecordBatch> = Vec::new();
        let mut update_batch_counter: usize = 0;
        // Track consumed predicates for correct multi-clause ordering:
        // each clause only applies to rows NOT matched by any previous clause.
        let mut consumed_predicates: Vec<String> = Vec::new();

        for mc in &clauses.matched {
            // Build WHERE clause: exclude rows consumed by previous clauses, then apply this predicate
            let mut conditions: Vec<String> = Vec::new();
            for prev in &consumed_predicates {
                conditions.push(format!("NOT ({prev})"));
            }
            if let Some(ref pred) = mc.predicate {
                conditions.push(pred.clone());
                consumed_predicates.push(pred.clone());
            } else {
                consumed_predicates.push("TRUE".to_string());
            }
            let where_clause = if conditions.is_empty() {
                String::new()
            } else {
                format!(" WHERE {}", conditions.join(" AND "))
            };

            match &mc.action {
                CowMatchedAction::Update(upd) => {
                    let mut select_parts = vec![
                        format!("{t_alias}.\"__paimon_file_idx\""),
                        format!("{t_alias}.\"__paimon_row_offset\""),
                    ];
                    let clause_col_map: HashMap<&str, &str> = upd
                        .columns
                        .iter()
                        .zip(upd.exprs.iter())
                        .map(|(c, e)| (c.as_str(), e.as_str()))
                        .collect();
                    for col in update_columns {
                        let quoted_alias = quote_identifier(&format!("__upd_{col}"));
                        if let Some(expr) = clause_col_map.get(&col.as_str()) {
                            select_parts.push(format!("{expr} AS {quoted_alias}"));
                        } else {
                            select_parts.push(format!(
                                "{t_alias}.{} AS {quoted_alias}",
                                quote_identifier(col)
                            ));
                        }
                    }
                    let select_clause = select_parts.join(", ");
                    let join_sql = format!(
                        "SELECT {select_clause} FROM {source_ref} AS {s_alias} \
                         INNER JOIN {cow_target_name} AS {t_alias} ON {on_condition}{where_clause}"
                    );

                    let join_result = ctx.sql(&join_sql).await?.collect().await?;

                    for batch in &join_result {
                        if batch.num_rows() == 0 {
                            continue;
                        }

                        let (file_idx_col, row_offset_col) = extract_tracking_columns(batch)?;

                        let mut upd_fields = Vec::new();
                        let mut upd_columns: Vec<Arc<dyn Array>> = Vec::new();
                        for col in update_columns {
                            let prefixed = format!("__upd_{col}");
                            let idx = batch.schema().index_of(&prefixed).map_err(|e| {
                                DataFusionError::Internal(format!(
                                    "Column {prefixed} not found: {e}"
                                ))
                            })?;
                            upd_fields.push(Field::new(
                                col,
                                batch.schema().field(idx).data_type().clone(),
                                true,
                            ));
                            upd_columns.push(batch.column(idx).clone());
                        }
                        let upd_schema = Arc::new(Schema::new(upd_fields));
                        let upd_batch = RecordBatch::try_new(upd_schema, upd_columns)?;

                        let current_batch_idx = update_batch_counter;
                        update_value_batches.push(upd_batch);
                        update_batch_counter += 1;

                        for row in 0..batch.num_rows() {
                            let file_idx = file_idx_col.value(row) as usize;
                            let row_offset = row_offset_col.value(row) as usize;
                            writer.add_matched_update(file_idx, row_offset, current_batch_idx, row);
                            total_count += 1;
                        }
                    }
                }
                CowMatchedAction::Delete => {
                    let select_clause = format!(
                        "{t_alias}.\"__paimon_file_idx\", {t_alias}.\"__paimon_row_offset\""
                    );
                    let join_sql = format!(
                        "SELECT {select_clause} FROM {source_ref} AS {s_alias} \
                         INNER JOIN {cow_target_name} AS {t_alias} ON {on_condition}{where_clause}"
                    );

                    let join_result = ctx.sql(&join_sql).await?.collect().await?;

                    for batch in &join_result {
                        if batch.num_rows() == 0 {
                            continue;
                        }

                        let (file_idx_col, row_offset_col) = extract_tracking_columns(batch)?;

                        for row in 0..batch.num_rows() {
                            let file_idx = file_idx_col.value(row) as usize;
                            let row_offset = row_offset_col.value(row) as usize;
                            writer.add_matched_delete(file_idx, row_offset);
                            total_count += 1;
                        }
                    }
                }
            }
        }

        if !update_value_batches.is_empty() {
            writer.set_update_batches(update_value_batches);
        }
    }

    // Handle NOT MATCHED → INSERT
    if !clauses.inserts.is_empty() {
        let table_fields: Vec<String> = table
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect();

        let insert_sql = if has_target_data {
            format!(
                "SELECT {s_alias}.* FROM {source_ref} AS {s_alias} \
                 LEFT JOIN {cow_target_name} AS {t_alias} ON {on_condition} \
                 WHERE {t_alias}.\"__paimon_file_idx\" IS NULL"
            )
        } else {
            format!("SELECT * FROM {source_ref} AS {s_alias}")
        };

        let not_matched_batches = ctx.sql(&insert_sql).await?.collect().await?;

        if !not_matched_batches.is_empty() {
            let insert_batches = build_insert_batches(
                ctx,
                &not_matched_batches,
                &clauses.inserts,
                s_alias,
                &[],
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
                let msgs = table_write
                    .prepare_commit()
                    .await
                    .map_err(to_datafusion_error)?;
                insert_messages.extend(msgs);
                total_count += insert_count as u64;
            }
        }
    }

    Ok((insert_messages, total_count))
}

// ---------------------------------------------------------------------------
// Data evolution path helpers
// ---------------------------------------------------------------------------

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
            select_parts.push(format!(
                "{expr} AS {}",
                quote_identifier(&format!("__upd_{col}"))
            ));
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
    let tmp_name = next_cow_table_name("__merge_not_matched");
    ctx.register_table(&tmp_name, Arc::new(mem_table))?;
    let _guard = CowTableGuard::new(ctx, tmp_name.clone());

    let result = build_insert_batches_inner(ctx, inserts, s_alias, &tmp_name, table_fields).await;

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
        } else {
            consumed_predicates.push("TRUE".to_string());
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
                    Some(expr) => format!("{expr} AS {}", quote_identifier(field)),
                    // Column not in INSERT list — fill with NULL
                    None => format!("NULL AS {}", quote_identifier(field)),
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

/// Extract __paimon_file_idx and __paimon_row_offset columns from a JOIN result batch.
pub(crate) fn extract_tracking_columns(
    batch: &RecordBatch,
) -> DFResult<(&Int32Array, &UInt32Array)> {
    let file_idx_col = batch
        .column_by_name("__paimon_file_idx")
        .ok_or_else(|| DataFusionError::Internal("__paimon_file_idx not found".to_string()))?
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| DataFusionError::Internal("__paimon_file_idx is not Int32".to_string()))?;

    let row_offset_col = batch
        .column_by_name("__paimon_row_offset")
        .ok_or_else(|| DataFusionError::Internal("__paimon_row_offset not found".to_string()))?
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| {
            DataFusionError::Internal("__paimon_row_offset is not UInt32".to_string())
        })?;

    Ok((file_idx_col, row_offset_col))
}

/// Read all files from a table via the CoW writer's file index, attach `__paimon_file_idx`
/// and `__paimon_row_offset` tracking columns, and register the result as a MemTable.
///
/// Returns `(has_data, guard)`. The guard deregisters the table on drop.
///
/// Note: all matching partition files are loaded into memory at once. For partitions
/// with many large files this may cause significant memory pressure. A future
/// optimization could stream or batch-process files instead of materializing everything.
pub(crate) async fn register_cow_target_table(
    ctx: &SessionContext,
    table: &Table,
    writer: &CopyOnWriteMergeWriter,
) -> DFResult<(bool, CowTableGuard)> {
    let file_index = writer.file_index();
    if file_index.is_empty() {
        let table_name = next_cow_table_name("__cow_target");
        return Ok((false, CowTableGuard::new(ctx, table_name)));
    }

    // Read all files in parallel
    let read_futures: Vec<_> = file_index
        .iter()
        .enumerate()
        .map(|(file_idx, file_info)| async move {
            let single_split = DataSplitBuilder::new()
                .with_snapshot(file_info.snapshot_id)
                .with_partition(
                    paimon::spec::BinaryRow::from_serialized_bytes(&file_info.partition)
                        .map_err(to_datafusion_error)?,
                )
                .with_bucket(file_info.bucket)
                .with_bucket_path(file_info.bucket_path.clone())
                .with_total_buckets(file_info.total_buckets)
                .with_data_files(vec![file_info.file_meta.clone()])
                .build()
                .map_err(to_datafusion_error)?;

            let read = table
                .new_read_builder()
                .new_read()
                .map_err(to_datafusion_error)?;
            let batches: Vec<RecordBatch> = read
                .to_arrow(&[single_split])
                .map_err(to_datafusion_error)?
                .try_collect()
                .await
                .map_err(to_datafusion_error)?;

            Ok::<_, DataFusionError>((file_idx, batches))
        })
        .collect();

    let file_results = futures::future::try_join_all(read_futures).await?;

    let mut all_batches: Vec<RecordBatch> = Vec::new();
    let mut schema: Option<Arc<Schema>> = None;

    for (file_idx, batches) in file_results {
        let mut row_offset = 0u32;
        for batch in batches {
            let num_rows = batch.num_rows();
            if num_rows == 0 {
                continue;
            }

            let file_idx_i32 = i32::try_from(file_idx).map_err(|_| {
                DataFusionError::Internal(format!("file_idx {file_idx} exceeds i32 range"))
            })?;
            let num_rows_u32 = u32::try_from(num_rows).map_err(|_| {
                DataFusionError::Internal(format!("batch num_rows {num_rows} exceeds u32 range"))
            })?;
            let file_idx_col = Arc::new(Int32Array::from(vec![file_idx_i32; num_rows]));
            let end_offset = row_offset.checked_add(num_rows_u32).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "row_offset overflow: {row_offset} + {num_rows_u32}"
                ))
            })?;
            let row_offset_col = Arc::new(UInt32Array::from(
                (row_offset..end_offset).collect::<Vec<_>>(),
            ));

            let mut fields: Vec<Field> = batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.as_ref().clone())
                .collect();
            fields.push(Field::new("__paimon_file_idx", ArrowDataType::Int32, false));
            fields.push(Field::new(
                "__paimon_row_offset",
                ArrowDataType::UInt32,
                false,
            ));
            let augmented_schema = Arc::new(Schema::new(fields));

            let mut columns: Vec<Arc<dyn Array>> = batch.columns().to_vec();
            columns.push(file_idx_col);
            columns.push(row_offset_col);

            let augmented = RecordBatch::try_new(augmented_schema.clone(), columns)
                .map_err(|e| DataFusionError::Internal(format!("Failed to augment batch: {e}")))?;

            if schema.is_none() {
                schema = Some(augmented.schema());
            }
            all_batches.push(augmented);
            row_offset = end_offset;
        }
    }

    let has_data = !all_batches.is_empty();
    let table_name = next_cow_table_name("__cow_target");

    if has_data {
        let s = schema.unwrap();
        let mem_table = MemTable::try_new(s, vec![all_batches])?;
        ctx.register_table(&table_name, Arc::new(mem_table))?;
    }

    Ok((has_data, CowTableGuard::new(ctx, table_name)))
}

/// Build a partition set from Arrow batches containing partition column values.
///
/// Converts each row's partition columns into serialized `BinaryRow` bytes.
/// Returns `None` for non-partitioned tables.
pub(crate) fn build_partition_set_from_batches(
    table: &Table,
    batches: &[RecordBatch],
) -> DFResult<Option<HashSet<Vec<u8>>>> {
    let partition_keys = table.schema().partition_keys();
    if partition_keys.is_empty() {
        return Ok(None);
    }

    let partition_fields = table.schema().partition_fields();
    let mut partition_set = HashSet::new();

    for batch in batches {
        for row in 0..batch.num_rows() {
            let datums: Vec<(Option<paimon::spec::Datum>, paimon::spec::DataType)> =
                partition_fields
                    .iter()
                    .enumerate()
                    .map(|(col_idx, field)| {
                        let datum =
                            extract_datum_from_arrow(batch, row, col_idx, field.data_type())
                                .map_err(to_datafusion_error)?;
                        Ok((datum, field.data_type().clone()))
                    })
                    .collect::<DFResult<_>>()?;
            let refs: Vec<(&Option<paimon::spec::Datum>, &paimon::spec::DataType)> =
                datums.iter().map(|(d, t)| (d, t)).collect();
            partition_set.insert(datums_to_binary_row(&refs));
        }
    }

    Ok(Some(partition_set))
}

/// Query a table for distinct partition values matching an optional WHERE clause.
///
/// Returns `None` for non-partitioned tables.
pub(crate) async fn build_partition_set_from_where(
    ctx: &SessionContext,
    table: &Table,
    table_ref: &str,
    where_clause: Option<&str>,
) -> DFResult<Option<HashSet<Vec<u8>>>> {
    let partition_keys = table.schema().partition_keys();
    if partition_keys.is_empty() {
        return Ok(None);
    }

    let cols = partition_keys
        .iter()
        .map(|k| quote_identifier(k))
        .collect::<Vec<_>>()
        .join(", ");
    let where_part = match where_clause {
        Some(w) => format!(" WHERE {w}"),
        None => String::new(),
    };
    let sql = format!("SELECT DISTINCT {cols} FROM {table_ref}{where_part}");
    let batches = ctx.sql(&sql).await?.collect().await?;

    build_partition_set_from_batches(table, &batches)
}

/// Query source table for distinct partition values and build a partition set.
///
/// Returns `None` for non-partitioned tables or when the source lacks matching
/// partition key columns (falls back to full-partition scan).
async fn build_source_partition_set(
    ctx: &SessionContext,
    table: &Table,
    source_ref: &str,
    s_alias: &str,
) -> DFResult<Option<HashSet<Vec<u8>>>> {
    let partition_keys = table.schema().partition_keys();
    if partition_keys.is_empty() {
        return Ok(None);
    }

    let cols = partition_keys
        .iter()
        .map(|k| format!("{s_alias}.{}", quote_identifier(k)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SELECT DISTINCT {cols} FROM {source_ref} AS {s_alias}");
    match ctx.sql(&sql).await {
        Ok(df) => {
            let batches = df.collect().await?;
            build_partition_set_from_batches(table, &batches)
        }
        Err(_) => Ok(None),
    }
}

/// Rewrite SQL expressions by replacing original table references with aliases.
///
/// For example, `paimon.test_db.target.a = source.a` becomes `t.a = s.a`
/// when target_ref="paimon.test_db.target", t_alias="t", source_ref="source", s_alias="s".
///
/// Uses word-boundary-aware replacement to avoid mangling identifiers that
/// contain the table name as a substring (e.g. `"my_source.x"` won't match `"source."`).
///
/// Limitation: this is best-effort text replacement, not AST-level rewriting.
/// It may produce incorrect results for quoted identifiers containing dots
/// (e.g. `"my.table".col`) or other unusual naming patterns.
///
/// TODO: migrate to AST-level rewriting for correctness with edge-case identifiers.
fn rewrite_condition(
    condition: &str,
    target_ref: &str,
    t_alias: &str,
    source_ref: &str,
    s_alias: &str,
) -> String {
    let mut result = condition.to_string();
    // Replace longer (more qualified) names first to avoid partial matches
    if target_ref.len() >= source_ref.len() {
        result = replace_table_ref(&result, target_ref, t_alias);
        result = replace_table_ref(&result, source_ref, s_alias);
    } else {
        result = replace_table_ref(&result, source_ref, s_alias);
        result = replace_table_ref(&result, target_ref, t_alias);
    }
    result
}

/// Replace `"ref."` with `"alias."` only when `ref` is not preceded by a word character.
fn replace_table_ref(input: &str, table_ref: &str, alias: &str) -> String {
    let needle = format!("{table_ref}.");
    let replacement = format!("{alias}.");
    let mut result = String::with_capacity(input.len());
    let mut remaining = input;

    while let Some(pos) = remaining.find(&needle) {
        let preceding_is_word = pos > 0 && {
            let prev = remaining.as_bytes()[pos - 1];
            prev.is_ascii_alphanumeric() || prev == b'_'
        };
        if preceding_is_word {
            result.push_str(&remaining[..pos + needle.len()]);
        } else {
            result.push_str(&remaining[..pos]);
            result.push_str(&replacement);
        }
        remaining = &remaining[pos + needle.len()..];
    }
    result.push_str(remaining);
    result
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
    use paimon::catalog::{Catalog, Identifier};
    use paimon::io::FileIOBuilder;
    use paimon::spec::{DataType, IntType, Schema as PaimonSchema, TableSchema};
    use paimon::{CatalogOptions, FileSystemCatalog, Options};
    use tempfile::TempDir;

    use crate::{
        DynamicOptions, PaimonCatalogProvider, PaimonRelationPlanner, PaimonSqlHandler,
        PaimonTableProvider,
    };

    async fn setup_handler() -> (TempDir, PaimonSqlHandler, Arc<FileSystemCatalog>) {
        let temp_dir = TempDir::new().unwrap();
        let warehouse = format!("file://{}", temp_dir.path().display());
        let mut options = Options::new();
        options.set(CatalogOptions::WAREHOUSE, warehouse);
        let catalog = Arc::new(FileSystemCatalog::new(options).unwrap());

        let dynamic_options: DynamicOptions = Default::default();
        let ctx = SessionContext::new();
        ctx.register_catalog(
            "paimon",
            Arc::new(PaimonCatalogProvider::new(
                catalog.clone(),
                dynamic_options.clone(),
            )),
        );
        ctx.register_relation_planner(Arc::new(PaimonRelationPlanner::new()))
            .unwrap();
        let handler = PaimonSqlHandler::new(ctx, catalog.clone(), "paimon", dynamic_options);
        handler.sql("CREATE SCHEMA paimon.test_db").await.unwrap();

        (temp_dir, handler, catalog)
    }

    async fn setup_data_evolution_table(name: &str) -> (TempDir, SessionContext, Table) {
        let (tmp, handler, catalog) = setup_handler().await;

        handler
            .sql(&format!(
                "CREATE TABLE paimon.test_db.{name} (id INT, name VARCHAR, value INT) WITH ('row-tracking.enabled' = 'true')"
            ))
            .await
            .unwrap();

        handler
            .sql(&format!(
                "INSERT INTO paimon.test_db.{name} (id, name, value) VALUES (1, 'alice', 10), (2, 'bob', 20), (3, 'charlie', 30)"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let table = catalog
            .get_table(&Identifier::new("test_db", name))
            .await
            .unwrap();
        let mut extra = std::collections::HashMap::new();
        extra.insert("data-evolution.enabled".to_string(), "true".to_string());
        extra.insert("row-tracking.enabled".to_string(), "true".to_string());
        let de_table = table.copy_with_options(extra);

        let ctx = handler.ctx().clone();
        let provider = PaimonTableProvider::try_new(de_table.clone()).unwrap();
        ctx.register_table("target", Arc::new(provider)).unwrap();

        (tmp, ctx, de_table)
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
        let (_tmp, ctx, table) = setup_data_evolution_table("t_merge").await;

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
        let (_tmp, ctx, table) = setup_data_evolution_table("t_merge2").await;

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
    async fn test_merge_into_rejects_pk_table_without_data_evolution() {
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
            .primary_key(["id"])
            .option("bucket", "1")
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
            .contains("primary-key tables without data-evolution"));
    }

    // -----------------------------------------------------------------------
    // CoW MERGE INTO tests (append-only tables)
    // -----------------------------------------------------------------------

    async fn setup_append_only_table(name: &str) -> (TempDir, SessionContext, Table) {
        let (tmp, handler, catalog) = setup_handler().await;

        handler
            .sql(&format!(
                "CREATE TABLE paimon.test_db.{name} (id INT, name VARCHAR, value INT)"
            ))
            .await
            .unwrap();

        handler
            .sql(&format!(
                "INSERT INTO paimon.test_db.{name} (id, name, value) VALUES (1, 'alice', 10), (2, 'bob', 20), (3, 'charlie', 30)"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let table = catalog
            .get_table(&Identifier::new("test_db", name))
            .await
            .unwrap();

        let ctx = handler.ctx().clone();
        let provider = PaimonTableProvider::try_new(table.clone()).unwrap();
        ctx.register_table("target", Arc::new(provider)).unwrap();

        (tmp, ctx, table)
    }

    fn collect_rows(batches: &[RecordBatch]) -> Vec<(i32, String, i32)> {
        let mut rows = Vec::new();
        for batch in batches {
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
        rows.sort_by_key(|r| r.0);
        rows
    }

    #[tokio::test]
    async fn test_cow_merge_update_matched_rows() {
        let (_tmp, ctx, table) = setup_append_only_table("t_cow_upd").await;

        ctx.sql(
            "CREATE TABLE source (id INT, name VARCHAR) AS VALUES (1, 'ALICE'), (3, 'CHARLIE')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

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

        let rows = collect_rows(&batches);
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
    async fn test_cow_merge_delete_matched_rows() {
        let (_tmp, ctx, table) = setup_append_only_table("t_cow_del").await;

        ctx.sql("CREATE TABLE source (id INT) AS VALUES (2)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let merge = parse_merge(
            "MERGE INTO target t USING source s ON t.id = s.id \
             WHEN MATCHED THEN DELETE",
        );
        execute_merge_into(&ctx, &merge, table).await.unwrap();

        let batches = ctx
            .sql("SELECT id, name, value FROM target ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let rows = collect_rows(&batches);
        assert_eq!(
            rows,
            vec![(1, "alice".to_string(), 10), (3, "charlie".to_string(), 30),]
        );
    }

    #[tokio::test]
    async fn test_cow_merge_insert_not_matched() {
        let (_tmp, ctx, table) = setup_append_only_table("t_cow_ins").await;

        ctx.sql("CREATE TABLE source (id INT, name VARCHAR, value INT) AS VALUES (4, 'dave', 40), (5, 'eve', 50)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let merge = parse_merge(
            "MERGE INTO target t USING source s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (s.id, s.name, s.value)",
        );
        execute_merge_into(&ctx, &merge, table).await.unwrap();

        let batches = ctx
            .sql("SELECT id, name, value FROM target ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let rows = collect_rows(&batches);
        assert_eq!(
            rows,
            vec![
                (1, "alice".to_string(), 10),
                (2, "bob".to_string(), 20),
                (3, "charlie".to_string(), 30),
                (4, "dave".to_string(), 40),
                (5, "eve".to_string(), 50),
            ]
        );
    }

    #[tokio::test]
    async fn test_cow_merge_update_and_insert() {
        let (_tmp, ctx, table) = setup_append_only_table("t_cow_upsert").await;

        ctx.sql("CREATE TABLE source (id INT, name VARCHAR, value INT) AS VALUES (2, 'BOB', 200), (4, 'dave', 40)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let merge = parse_merge(
            "MERGE INTO target t USING source s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name, value = s.value \
             WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (s.id, s.name, s.value)",
        );
        execute_merge_into(&ctx, &merge, table).await.unwrap();

        let batches = ctx
            .sql("SELECT id, name, value FROM target ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let rows = collect_rows(&batches);
        assert_eq!(
            rows,
            vec![
                (1, "alice".to_string(), 10),
                (2, "BOB".to_string(), 200),
                (3, "charlie".to_string(), 30),
                (4, "dave".to_string(), 40),
            ]
        );
    }

    #[tokio::test]
    async fn test_cow_merge_no_matches() {
        let (_tmp, ctx, table) = setup_append_only_table("t_cow_nomatch").await;

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
}
