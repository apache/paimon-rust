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
//! Supports copy-on-write file rewriting for append-only tables (no PK, no deletion vectors).

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{Delete, FromTable, TableFactor};

use paimon::spec::CoreOptions;
use paimon::table::{CopyOnWriteMergeWriter, Table};

use crate::error::to_datafusion_error;
use crate::merge_into::{
    build_partition_set_from_where, extract_tracking_columns, is_delete_conflict, ok_result,
    register_cow_target_table, retry_on_conflict,
};

/// Execute a DELETE statement on a Paimon table.
///
/// `table_ref` is the SQL table reference string (e.g. `"paimon.test_db.t"`),
/// already extracted by the caller for catalog resolution.
pub(crate) async fn execute_delete(
    ctx: &SessionContext,
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
        return Err(DataFusionError::Plan(
            "DELETE on data-evolution tables is not yet supported".to_string(),
        ));
    }
    if !schema.trimmed_primary_keys().is_empty() {
        return Err(DataFusionError::Plan(
            "DELETE on primary-key tables is not yet supported".to_string(),
        ));
    }

    execute_cow_delete(ctx, delete, &table, table_ref).await
}

/// Execute DELETE on an append-only table with retry on delete conflict.
async fn execute_cow_delete(
    ctx: &SessionContext,
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
    ctx: &SessionContext,
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

    let (has_data, cow_table_guard) = register_cow_target_table(ctx, table, &writer).await?;
    if !has_data {
        return ok_result(ctx, 0);
    }

    let result = execute_cow_delete_inner(ctx, cow_table_guard.name(), delete, &mut writer).await;
    drop(cow_table_guard);
    let total_count = result?;

    let messages = writer.prepare_commit().await.map_err(to_datafusion_error)?;
    if !messages.is_empty() {
        let commit = table.new_write_builder().new_commit();
        commit.commit(messages).await.map_err(to_datafusion_error)?;
    }

    ok_result(ctx, total_count)
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
