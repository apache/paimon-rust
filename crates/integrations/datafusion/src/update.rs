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

//! UPDATE execution for Paimon tables.
//!
//! Supports two execution paths:
//! - **Data evolution tables**: partial-column writes via [`paimon::table::DataEvolutionWriter`].
//! - **Append-only tables** (no PK, no deletion vectors): copy-on-write file rewriting
//!   via [`paimon::table::CopyOnWriteMergeWriter`].

use std::sync::Arc;

use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{AssignmentTarget, TableFactor, Update};

use paimon::spec::CoreOptions;
use paimon::table::{CopyOnWriteMergeWriter, DataEvolutionWriter, Table};

use crate::error::to_datafusion_error;
use crate::merge_into::{
    build_partition_set_from_where, extract_tracking_columns, is_delete_conflict,
    is_row_id_conflict, ok_result, project_update_columns, quote_identifier,
    register_cow_target_table, retry_on_conflict,
};

/// Execute an UPDATE statement on a Paimon table.
///
/// Dispatches to the appropriate execution path based on table type:
/// - Data evolution tables → partial-column writes via `DataEvolutionWriter`
/// - Append-only tables (no PK) → copy-on-write file rewriting via `CopyOnWriteMergeWriter`
pub(crate) async fn execute_update(
    ctx: &SessionContext,
    update: &Update,
    table: Table,
) -> DFResult<DataFrame> {
    if let TableFactor::Table { alias: Some(a), .. } = &update.table.relation {
        return Err(DataFusionError::Plan(format!(
            "Table alias '{}' in UPDATE is not yet supported",
            a.name.value
        )));
    }

    let schema = table.schema();
    let core_options = CoreOptions::new(schema.options());

    if core_options.data_evolution_enabled() {
        execute_data_evolution_update(ctx, update, table).await
    } else if schema.trimmed_primary_keys().is_empty() {
        execute_cow_update(ctx, update, &table).await
    } else {
        Err(DataFusionError::Plan(
            "UPDATE on primary-key tables without data-evolution is not supported".to_string(),
        ))
    }
}

// ---------------------------------------------------------------------------
// Data evolution path
// ---------------------------------------------------------------------------

/// Execute UPDATE on a data evolution table with retry on row ID conflict.
async fn execute_data_evolution_update(
    ctx: &SessionContext,
    update: &Update,
    table: Table,
) -> DFResult<DataFrame> {
    retry_on_conflict("UPDATE", is_row_id_conflict, || {
        execute_update_once(ctx, update, &table)
    })
    .await
}

/// Single attempt of UPDATE execution.
async fn execute_update_once(
    ctx: &SessionContext,
    update: &Update,
    table: &Table,
) -> DFResult<DataFrame> {
    // 1. Extract SET assignments
    let mut columns = Vec::new();
    let mut exprs = Vec::new();
    for assignment in &update.assignments {
        let col_name = match &assignment.target {
            AssignmentTarget::ColumnName(name) => name
                .0
                .last()
                .and_then(|p| p.as_ident())
                .map(|id| id.value.clone())
                .ok_or_else(|| {
                    DataFusionError::Plan(format!("Invalid column name in SET: {name}"))
                })?,
            AssignmentTarget::Tuple(_) => {
                return Err(DataFusionError::Plan(
                    "Tuple assignment in UPDATE SET is not supported".to_string(),
                ));
            }
        };
        columns.push(col_name);
        exprs.push(assignment.value.to_string());
    }

    // 2. Create DataEvolutionWriter (validates preconditions)
    let mut writer =
        DataEvolutionWriter::new(table, columns.clone()).map_err(to_datafusion_error)?;

    // 3. Query the target table directly with WHERE filter.
    let table_ref = update.table.to_string();

    let select_parts: Vec<String> =
        std::iter::once("\"_ROW_ID\"".to_string())
            .chain(columns.iter().zip(exprs.iter()).map(|(col, expr)| {
                format!("{expr} AS {}", quote_identifier(&format!("__upd_{col}")))
            }))
            .collect();

    let select_clause = select_parts.join(", ");
    let where_clause = match &update.selection {
        Some(expr) => format!(" WHERE {expr}"),
        None => String::new(),
    };

    let query_sql = format!("SELECT {select_clause} FROM {table_ref}{where_clause}");
    let batches = ctx.sql(&query_sql).await?.collect().await?;

    // 4. Project update columns (rename __upd_X → X)
    let total_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    if total_count == 0 {
        return ok_result(ctx, 0);
    }

    let update_batches = project_update_columns(&batches, &columns)?;
    for batch in update_batches {
        writer
            .add_matched_batch(batch)
            .map_err(to_datafusion_error)?;
    }

    // 5. Commit
    let messages = writer.prepare_commit().await.map_err(to_datafusion_error)?;
    if !messages.is_empty() {
        let commit = table.new_write_builder().new_commit();
        commit.commit(messages).await.map_err(to_datafusion_error)?;
    }

    ok_result(ctx, total_count)
}

// ---------------------------------------------------------------------------
// Copy-on-Write path (append-only tables, no PK)
// ---------------------------------------------------------------------------

/// Execute UPDATE on an append-only table with retry on delete conflict.
async fn execute_cow_update(
    ctx: &SessionContext,
    update: &Update,
    table: &Table,
) -> DFResult<DataFrame> {
    retry_on_conflict("CoW UPDATE", is_delete_conflict, || {
        execute_cow_update_once(ctx, update, table)
    })
    .await
}

/// Single attempt of CoW UPDATE execution.
async fn execute_cow_update_once(
    ctx: &SessionContext,
    update: &Update,
    table: &Table,
) -> DFResult<DataFrame> {
    let (columns, exprs) = extract_set_assignments(update)?;

    let table_ref = update.table.to_string();
    let where_str = update.selection.as_ref().map(|e| e.to_string());
    let partition_set =
        build_partition_set_from_where(ctx, table, &table_ref, where_str.as_deref()).await?;

    let mut writer = CopyOnWriteMergeWriter::new(table, columns.clone(), partition_set)
        .await
        .map_err(to_datafusion_error)?;

    let (has_data, cow_table_guard) = register_cow_target_table(ctx, table, &writer).await?;
    if !has_data {
        return ok_result(ctx, 0);
    }

    let result = execute_cow_update_inner(
        ctx,
        &columns,
        &exprs,
        cow_table_guard.name(),
        update,
        &mut writer,
    )
    .await;
    drop(cow_table_guard);
    let total_count = result?;

    let messages = writer.prepare_commit().await.map_err(to_datafusion_error)?;
    if !messages.is_empty() {
        let commit = table.new_write_builder().new_commit();
        commit.commit(messages).await.map_err(to_datafusion_error)?;
    }

    ok_result(ctx, total_count)
}

async fn execute_cow_update_inner(
    ctx: &SessionContext,
    columns: &[String],
    exprs: &[String],
    cow_table_name: &str,
    update: &Update,
    writer: &mut CopyOnWriteMergeWriter,
) -> DFResult<u64> {
    let select_parts: Vec<String> =
        std::iter::once("\"__paimon_file_idx\"".to_string())
            .chain(std::iter::once("\"__paimon_row_offset\"".to_string()))
            .chain(columns.iter().zip(exprs.iter()).map(|(col, expr)| {
                format!("{expr} AS {}", quote_identifier(&format!("__upd_{col}")))
            }))
            .collect();

    let select_clause = select_parts.join(", ");
    let where_clause = match &update.selection {
        Some(expr) => format!(" WHERE {expr}"),
        None => String::new(),
    };

    // Safety: where_clause comes from sqlparser AST to_string(), not raw user input.
    let query_sql = format!("SELECT {select_clause} FROM {cow_table_name}{where_clause}");
    let join_result = ctx.sql(&query_sql).await?.collect().await?;

    let mut update_value_batches: Vec<RecordBatch> = Vec::new();
    let mut batch_counter: usize = 0;
    let mut total_count: u64 = 0;

    for batch in &join_result {
        if batch.num_rows() == 0 {
            continue;
        }

        let (file_idx_col, row_offset_col) = extract_tracking_columns(batch)?;

        let mut upd_fields = Vec::new();
        let mut upd_columns: Vec<Arc<dyn Array>> = Vec::new();
        for col in columns {
            let prefixed = format!("__upd_{col}");
            let idx = batch.schema().index_of(&prefixed).map_err(|e| {
                DataFusionError::Internal(format!("Column {prefixed} not found: {e}"))
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

        let current_batch_idx = batch_counter;
        update_value_batches.push(upd_batch);
        batch_counter += 1;

        for row in 0..batch.num_rows() {
            let file_idx = file_idx_col.value(row) as usize;
            let row_offset = row_offset_col.value(row) as usize;
            writer.add_matched_update(file_idx, row_offset, current_batch_idx, row);
            total_count += 1;
        }
    }

    if !update_value_batches.is_empty() {
        writer.set_update_batches(update_value_batches);
    }

    Ok(total_count)
}

/// Extract column names and expressions from UPDATE SET assignments.
fn extract_set_assignments(update: &Update) -> DFResult<(Vec<String>, Vec<String>)> {
    let mut columns = Vec::new();
    let mut exprs = Vec::new();
    for assignment in &update.assignments {
        let col_name = match &assignment.target {
            AssignmentTarget::ColumnName(name) => name
                .0
                .last()
                .and_then(|p| p.as_ident())
                .map(|id| id.value.clone())
                .ok_or_else(|| {
                    DataFusionError::Plan(format!("Invalid column name in SET: {name}"))
                })?,
            AssignmentTarget::Tuple(_) => {
                return Err(DataFusionError::Plan(
                    "Tuple assignment in UPDATE SET is not supported".to_string(),
                ));
            }
        };
        columns.push(col_name);
        exprs.push(assignment.value.to_string());
    }
    Ok((columns, exprs))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, StringArray, UInt64Array};
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

        // Register the data-evolution table under a simple name so _ROW_ID is visible
        let ctx = handler.ctx().clone();
        let provider = PaimonTableProvider::try_new(de_table.clone()).unwrap();
        ctx.register_table("target", Arc::new(provider)).unwrap();

        (tmp, ctx, de_table)
    }

    fn parse_update(sql: &str) -> Update {
        let dialect = GenericDialect {};
        let stmts = Parser::parse_sql(&dialect, sql).unwrap();
        match stmts.into_iter().next().unwrap() {
            datafusion::sql::sqlparser::ast::Statement::Update(u) => u,
            _ => panic!("Expected UPDATE statement"),
        }
    }

    fn collect_rows(batches: &[datafusion::arrow::array::RecordBatch]) -> Vec<(i32, String, i32)> {
        let mut rows = Vec::new();
        for batch in batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let values = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                rows.push((ids.value(i), names.value(i).to_string(), values.value(i)));
            }
        }
        rows
    }

    #[tokio::test]
    async fn test_update_with_where() {
        let (_tmp, ctx, table) = setup_data_evolution_table("t_with_where").await;

        let update = parse_update("UPDATE target SET name = 'ALICE' WHERE id = 1");
        execute_update(&ctx, &update, table).await.unwrap();

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
                (3, "charlie".to_string(), 30),
            ]
        );
    }

    #[tokio::test]
    async fn test_update_without_where() {
        let (_tmp, ctx, table) = setup_data_evolution_table("t_without_where").await;

        let update = parse_update("UPDATE target SET value = 99");
        execute_update(&ctx, &update, table).await.unwrap();

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
                (1, "alice".to_string(), 99),
                (2, "bob".to_string(), 99),
                (3, "charlie".to_string(), 99),
            ]
        );
    }

    #[tokio::test]
    async fn test_update_multiple_columns() {
        let (_tmp, ctx, table) = setup_data_evolution_table("t_multi_col").await;

        let update = parse_update("UPDATE target SET name = 'updated', value = 0 WHERE id = 2");
        execute_update(&ctx, &update, table).await.unwrap();

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
                (2, "updated".to_string(), 0),
                (3, "charlie".to_string(), 30),
            ]
        );
    }

    #[tokio::test]
    async fn test_update_no_matching_rows() {
        let (_tmp, ctx, table) = setup_data_evolution_table("t_no_match").await;

        let update = parse_update("UPDATE target SET name = 'nobody' WHERE id = 99");
        let result = execute_update(&ctx, &update, table).await.unwrap();
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
    async fn test_update_row_id_stability() {
        let (_tmp, ctx, table) = setup_data_evolution_table("t_row_id").await;

        // Get row IDs before update
        let before = ctx
            .sql("SELECT id, \"_ROW_ID\" FROM target ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let update = parse_update("UPDATE target SET name = 'ALICE' WHERE id = 1");
        execute_update(&ctx, &update, table).await.unwrap();

        // Get row IDs after update
        let after = ctx
            .sql("SELECT id, \"_ROW_ID\" FROM target ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Row IDs should remain the same
        assert_eq!(before, after);
    }

    #[tokio::test]
    async fn test_update_rejects_pk_table_without_data_evolution() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/test_update_reject";
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
        let update = parse_update("UPDATE t SET id = 1");
        let result = execute_update(&ctx, &update, table).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("primary-key tables without data-evolution"));
    }

    // -----------------------------------------------------------------------
    // CoW UPDATE tests (append-only tables)
    // -----------------------------------------------------------------------

    async fn setup_append_only_table(name: &str) -> (TempDir, PaimonSqlHandler) {
        let (tmp, handler, _catalog) = setup_handler().await;

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

        (tmp, handler)
    }

    async fn query_rows(handler: &PaimonSqlHandler, table: &str) -> Vec<(i32, String, i32)> {
        let batches = handler
            .sql(&format!("SELECT id, name, value FROM {table} ORDER BY id"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        collect_rows(&batches)
    }

    #[tokio::test]
    async fn test_cow_update_with_where() {
        let (_tmp, handler) = setup_append_only_table("t_cow_where").await;

        handler
            .sql("UPDATE paimon.test_db.t_cow_where SET name = 'ALICE' WHERE id = 1")
            .await
            .unwrap();

        let rows = query_rows(&handler, "paimon.test_db.t_cow_where").await;
        assert_eq!(
            rows,
            vec![
                (1, "ALICE".to_string(), 10),
                (2, "bob".to_string(), 20),
                (3, "charlie".to_string(), 30),
            ]
        );
    }

    #[tokio::test]
    async fn test_cow_update_without_where() {
        let (_tmp, handler) = setup_append_only_table("t_cow_no_where").await;

        handler
            .sql("UPDATE paimon.test_db.t_cow_no_where SET value = 99")
            .await
            .unwrap();

        let rows = query_rows(&handler, "paimon.test_db.t_cow_no_where").await;
        assert_eq!(
            rows,
            vec![
                (1, "alice".to_string(), 99),
                (2, "bob".to_string(), 99),
                (3, "charlie".to_string(), 99),
            ]
        );
    }

    #[tokio::test]
    async fn test_cow_update_multiple_columns() {
        let (_tmp, handler) = setup_append_only_table("t_cow_multi").await;

        handler
            .sql("UPDATE paimon.test_db.t_cow_multi SET name = 'updated', value = 0 WHERE id = 2")
            .await
            .unwrap();

        let rows = query_rows(&handler, "paimon.test_db.t_cow_multi").await;
        assert_eq!(
            rows,
            vec![
                (1, "alice".to_string(), 10),
                (2, "updated".to_string(), 0),
                (3, "charlie".to_string(), 30),
            ]
        );
    }

    #[tokio::test]
    async fn test_cow_update_no_matching_rows() {
        let (_tmp, handler) = setup_append_only_table("t_cow_nomatch").await;

        let result = handler
            .sql("UPDATE paimon.test_db.t_cow_nomatch SET name = 'nobody' WHERE id = 99")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let count = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_cow_update_expression() {
        let (_tmp, handler) = setup_append_only_table("t_cow_expr").await;

        handler
            .sql("UPDATE paimon.test_db.t_cow_expr SET value = value + 100 WHERE id >= 2")
            .await
            .unwrap();

        let rows = query_rows(&handler, "paimon.test_db.t_cow_expr").await;
        assert_eq!(
            rows,
            vec![
                (1, "alice".to_string(), 10),
                (2, "bob".to_string(), 120),
                (3, "charlie".to_string(), 130),
            ]
        );
    }
}
