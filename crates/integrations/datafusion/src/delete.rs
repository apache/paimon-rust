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

//! DELETE execution for Paimon tables.
//!
//! Supports primary-key tombstones, append copy-on-write, and Data Evolution deletion vectors.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use datafusion::arrow::array::{Int8Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::tree_node::Transformed;
use datafusion::datasource::{provider_as_source, source_as_provider};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{Delete, FromTable, TableFactor};
use futures::StreamExt;

use paimon::spec::{CoreOptions, MergeEngine, RowKind, VALUE_KIND_FIELD_NAME};
use paimon::table::{CopyOnWriteMergeWriter, Table};

use crate::error::to_datafusion_error;
use crate::merge_into::TempTableTracker;
use crate::merge_into::{
    build_partition_set_from_where, extract_tracking_columns, is_delete_conflict,
    is_row_id_conflict, ok_result, register_cow_target_table, retry_on_conflict,
};
use crate::physical_plan::sink::to_paimon_batch;
use crate::sql_context::SQLContext;
use crate::table::PaimonTableProvider;

/// Execute a DELETE statement on a Paimon table.
///
/// `table_ref` is the SQL table reference string (e.g. `"paimon.test_db.t"`),
/// already extracted by the caller for catalog resolution.
pub(crate) async fn execute_delete(
    ctx: &SQLContext,
    delete: &Delete,
    table: Table,
    table_ref: &str,
) -> DFResult<DataFrame> {
    let tables = match &delete.from {
        FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
    };
    if let Some(first) = tables.first() {
        if let TableFactor::Table { alias: Some(a), .. } = &first.relation {
            return Err(DataFusionError::Plan(format!(
                "Table alias '{}' in DELETE is not yet supported",
                a.name.value
            )));
        }
    }

    let schema = table.schema();
    let core_options = CoreOptions::new(schema.options());

    if core_options.data_evolution_enabled() {
        return execute_data_evolution_delete(ctx, delete, &table, table_ref).await;
    }
    if !schema.trimmed_primary_keys().is_empty() {
        return execute_primary_key_delete(ctx, delete, &table, table_ref).await;
    }

    execute_cow_delete(ctx, delete, &table, table_ref).await
}

/// Execute DELETE on a deduplicate primary-key table by writing matched rows as DELETE records.
async fn execute_primary_key_delete(
    ctx: &SQLContext,
    delete: &Delete,
    table: &Table,
    table_ref: &str,
) -> DFResult<DataFrame> {
    let options = table.schema().core_options();
    if options.merge_engine().map_err(to_datafusion_error)? != MergeEngine::Deduplicate {
        return Err(DataFusionError::NotImplemented(
            "DELETE on primary-key tables requires merge-engine=deduplicate".into(),
        ));
    }
    if options.ignore_delete() {
        return Err(DataFusionError::NotImplemented(
            "DELETE on primary-key tables requires ignore-delete=false".into(),
        ));
    }
    // A masked key or sequence value cannot safely identify a tombstone target.
    if options.query_auth_enabled() {
        return Err(DataFusionError::NotImplemented(
            "DELETE on query-authorized primary-key tables is not supported".into(),
        ));
    }
    if options.rowkind_field().is_some() {
        return Err(DataFusionError::NotImplemented(
            "DELETE on primary-key tables with rowkind.field is not supported".into(),
        ));
    }
    let from = match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };
    if from.len() != 1
        || !from[0].joins.is_empty()
        || !delete.tables.is_empty()
        || delete.using.is_some()
        || delete.returning.is_some()
        || delete.output.is_some()
        || !delete.order_by.is_empty()
        || delete.limit.is_some()
        || !matches!(&from[0].relation,
            TableFactor::Table {
                args: None, version: None, with_ordinality: false,
                json_path: None, sample: None, with_hints, partitions, index_hints, ..
            } if with_hints.is_empty() && partitions.is_empty() && index_hints.is_empty())
    {
        return Err(DataFusionError::NotImplemented(
            "Primary-key DELETE supports a single target with an optional WHERE clause".into(),
        ));
    }

    let resources =
        crate::memory::writer_resources(&ctx.ctx().task_ctx()).map_err(to_datafusion_error)?;
    let write_builder = table.new_write_builder().with_resources(resources);
    let commit = write_builder
        .try_new_commit()
        .map_err(to_datafusion_error)?;
    let Some(input) = plan_primary_key_delete_input(ctx, delete, table, table_ref).await? else {
        return ok_result(ctx.ctx(), 0);
    };
    let mut writer = write_builder.new_write().map_err(to_datafusion_error)?;

    let staged = async {
        let mut stream = input.execute_stream().await?;
        let mut matched_rows = 0;
        while let Some(batch) = stream.next().await {
            let batch = to_paimon_batch(batch?, false)?;
            matched_rows += batch.num_rows() as u64;
            writer
                .write_arrow_batch(&with_delete_row_kind(batch)?)
                .await
                .map_err(to_datafusion_error)?;
        }
        let messages = writer.prepare_commit().await.map_err(to_datafusion_error)?;
        Ok::<_, DataFusionError>((matched_rows, messages))
    }
    .await;
    // Close unprepared output even on scan failure; prepared files are caller-owned.
    writer.close().await;
    let (matched_rows, messages) = staged?;
    if !messages.is_empty() {
        // Publication may have succeeded even on error. Keep prepared files for recovery.
        commit.commit(messages).await.map_err(|error| {
            let context = format!(
                "Primary-key DELETE commit failed for table '{}' (commit_user={}, matched_rows={matched_rows}); prepared files retained",
                table.identifier(),
                write_builder.commit_user(),
            );
            log::warn!("{context}: {error}");
            to_datafusion_error(error).context(context)
        })?;
    }
    ok_result(ctx.ctx(), matched_rows)
}

/// Mark every row of `batch` as a DELETE record via the `_VALUE_KIND` column.
fn with_delete_row_kind(batch: RecordBatch) -> DFResult<RecordBatch> {
    let schema = batch.schema();
    let mut fields = schema.fields().to_vec();
    fields.push(Arc::new(Field::new(
        VALUE_KIND_FIELD_NAME,
        DataType::Int8,
        false,
    )));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(Int8Array::from(vec![
        RowKind::Delete.to_value();
        batch.num_rows()
    ])));
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
        columns,
    )?)
}

/// Plan the target scan for the DELETE predicate, pinned to the latest snapshot.
///
/// Returns `None` when the table has no snapshot yet.
async fn plan_primary_key_delete_input(
    ctx: &SQLContext,
    delete: &Delete,
    table: &Table,
    table_ref: &str,
) -> DFResult<Option<DataFrame>> {
    let snapshot = table
        .resolve_read_snapshot()
        .await
        .map_err(to_datafusion_error)?;
    let where_clause = delete
        .selection
        .as_ref()
        .map(|predicate| format!(" WHERE {predicate}"))
        .unwrap_or_default();
    let matched = ctx
        .ctx()
        .sql(&format!("SELECT * FROM {table_ref}{where_clause}"))
        .await?;
    let is_target = |provider: &PaimonTableProvider| {
        let source = provider.table();
        source.identifier() == table.identifier()
            && source.location() == table.location()
            && source.branch() == table.branch()
            && !source.is_time_traveled()
    };
    // Validate the direct FROM source: a view can synthesize keys or hide scans.
    let source = match matched.logical_plan() {
        LogicalPlan::Projection(projection) => projection.input.as_ref(),
        plan => plan,
    };
    let source = match source {
        LogicalPlan::Filter(filter) => filter.input.as_ref(),
        plan => plan,
    };
    let target_changed = || DataFusionError::Plan("DELETE target changed during planning".into());
    let LogicalPlan::TableScan(scan) = source else {
        return Err(target_changed());
    };
    let provider = source_as_provider(&scan.source)?;
    if !provider
        .downcast_ref::<PaimonTableProvider>()
        .is_some_and(is_target)
    {
        return Err(target_changed());
    }
    let Some(snapshot) = snapshot else {
        return Ok(None);
    };
    // Self-references in predicates must use the captured target snapshot too.
    let plan = matched
        .into_unoptimized_plan()
        .transform_up_with_subqueries(|plan| {
            let LogicalPlan::TableScan(mut scan) = plan else {
                return Ok(Transformed::no(plan));
            };
            let Ok(provider) = source_as_provider(&scan.source) else {
                return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
            };
            if let Some(provider) = provider.downcast_ref::<PaimonTableProvider>() {
                if is_target(provider) {
                    scan.source = provider_as_source(Arc::new(
                        provider.clone().with_pinned_snapshot(&snapshot),
                    ));
                    return Ok(Transformed::yes(LogicalPlan::TableScan(scan)));
                }
            }
            Ok(Transformed::no(LogicalPlan::TableScan(scan)))
        })?
        .data;
    Ok(Some(DataFrame::new(ctx.ctx().state(), plan)))
}

/// Execute DELETE on a data-evolution table with retry on row ID conflict.
async fn execute_data_evolution_delete(
    ctx: &SQLContext,
    delete: &Delete,
    table: &Table,
    table_ref: &str,
) -> DFResult<DataFrame> {
    retry_on_conflict("Data-evolution DELETE", is_row_id_conflict, || {
        execute_data_evolution_delete_once(ctx, delete, table, table_ref)
    })
    .await
}

/// Single attempt of data-evolution DELETE execution.
async fn execute_data_evolution_delete_once(
    ctx: &SQLContext,
    delete: &Delete,
    table: &Table,
    table_ref: &str,
) -> DFResult<DataFrame> {
    let wb = table.new_write_builder();
    let mut writer = wb.new_delete().map_err(to_datafusion_error)?;

    let where_clause = match &delete.selection {
        Some(expr) => format!(" WHERE {expr}"),
        None => String::new(),
    };
    let query_sql = format!("SELECT \"_ROW_ID\" FROM {table_ref}{where_clause}");
    let batches = ctx.ctx().sql(&query_sql).await?.collect().await?;

    let total_count: u64 = batches.iter().map(|batch| batch.num_rows() as u64).sum();
    if total_count == 0 {
        return ok_result(ctx.ctx(), 0);
    }

    for batch in batches {
        writer
            .add_matched_batch(batch)
            .map_err(to_datafusion_error)?;
    }

    let messages = writer.prepare_commit().await.map_err(to_datafusion_error)?;
    if !messages.is_empty() {
        wb.try_new_commit()
            .map_err(to_datafusion_error)?
            .commit(messages)
            .await
            .map_err(to_datafusion_error)?;
    }

    ok_result(ctx.ctx(), total_count)
}

/// Execute DELETE on an append-only table with retry on delete conflict.
async fn execute_cow_delete(
    ctx: &SQLContext,
    delete: &Delete,
    table: &Table,
    table_ref: &str,
) -> DFResult<DataFrame> {
    retry_on_conflict("CoW DELETE", is_delete_conflict, || {
        execute_cow_delete_once(ctx, delete, table, table_ref)
    })
    .await
}

/// Single attempt of CoW DELETE execution.
async fn execute_cow_delete_once(
    ctx: &SQLContext,
    delete: &Delete,
    table: &Table,
    table_ref: &str,
) -> DFResult<DataFrame> {
    let where_str = delete.selection.as_ref().map(|e| e.to_string());
    let partition_set =
        build_partition_set_from_where(ctx, table, table_ref, where_str.as_deref()).await?;

    let mut writer = CopyOnWriteMergeWriter::new(table, vec![], partition_set)
        .await
        .map_err(to_datafusion_error)?;

    let mut temp_tracker = TempTableTracker::new(ctx);
    let (has_data, cow_table_name) =
        register_cow_target_table(ctx, table, &writer, &mut temp_tracker).await?;
    if !has_data {
        return ok_result(ctx.ctx(), 0);
    }

    let cow_target_qualified = cow_table_name;
    let result =
        execute_cow_delete_inner(ctx.ctx(), &cow_target_qualified, delete, &mut writer).await;
    let total_count = result?;

    let messages = writer.prepare_commit().await.map_err(to_datafusion_error)?;
    if !messages.is_empty() {
        let commit = table
            .new_write_builder()
            .try_new_commit()
            .map_err(to_datafusion_error)?;
        commit.commit(messages).await.map_err(to_datafusion_error)?;
    }

    ok_result(ctx.ctx(), total_count)
}

async fn execute_cow_delete_inner(
    ctx: &SessionContext,
    cow_table_name: &str,
    delete: &Delete,
    writer: &mut CopyOnWriteMergeWriter,
) -> DFResult<u64> {
    let where_clause = match &delete.selection {
        Some(expr) => format!(" WHERE {expr}"),
        None => String::new(),
    };

    // Safety: where_clause comes from sqlparser AST to_string(), not raw user input.
    let query_sql = format!(
        "SELECT \"__paimon_file_idx\", \"__paimon_row_offset\" FROM {cow_table_name}{where_clause}"
    );
    let batches = ctx.sql(&query_sql).await?.collect().await?;

    let mut total_count: u64 = 0;
    for batch in &batches {
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

    Ok(total_count)
}
