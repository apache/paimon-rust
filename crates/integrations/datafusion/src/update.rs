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

//! UPDATE execution for data evolution tables.
//!
//! This module provides the DataFusion-specific SQL parsing and execution layer for
//! `UPDATE ... SET ... WHERE ...` statements. The engine-agnostic update logic
//! (file metadata lookup, row grouping, reading originals, applying updates,
//! writing partial files, committing) lives in [`paimon::table::DataEvolutionWriter`].

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{AssignmentTarget, Update};

use paimon::table::{DataEvolutionWriter, Table};

use crate::error::to_datafusion_error;
use crate::merge_into::{is_row_id_conflict, ok_result, project_update_columns};

/// Maximum number of retries when UPDATE conflicts with concurrent compaction.
const UPDATE_MAX_RETRIES: u32 = 5;

/// Execute an UPDATE statement on a data evolution Paimon table.
///
/// When a concurrent compaction rewrites files that this UPDATE references,
/// the commit will fail with a row ID conflict. In that case, the entire operation
/// is retried from scratch against the new file layout.
pub(crate) async fn execute_update(
    ctx: &SessionContext,
    update: &Update,
    table: Table,
) -> DFResult<DataFrame> {
    let mut last_err = None;
    for _ in 0..UPDATE_MAX_RETRIES {
        match execute_update_once(ctx, update, &table).await {
            Ok(df) => return Ok(df),
            Err(e) if is_row_id_conflict(&e) => {
                last_err = Some(e);
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    Err(DataFusionError::External(Box::new(std::io::Error::other(
        format!(
            "UPDATE failed after {} retries due to concurrent compaction: {}",
            UPDATE_MAX_RETRIES,
            last_err.unwrap()
        ),
    ))))
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

    let select_parts: Vec<String> = std::iter::once("\"_ROW_ID\"".to_string())
        .chain(
            columns
                .iter()
                .zip(exprs.iter())
                .map(|(col, expr)| format!("{expr} AS \"__upd_{col}\"")),
        )
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, StringArray, UInt64Array};
    use datafusion::prelude::SessionContext;
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;
    use paimon::catalog::Identifier;
    use paimon::io::FileIOBuilder;
    use paimon::spec::{DataType, IntType, Schema as PaimonSchema, TableSchema, VarCharType};

    use crate::PaimonTableProvider;

    async fn setup_data_evolution_table(name: &str) -> (SessionContext, Table) {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = format!("memory:/test_update_{name}");
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
            table_path,
            table_schema,
            None,
        );

        let provider = PaimonTableProvider::try_new(table.clone()).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("target", Arc::new(provider)).unwrap();

        (ctx, table)
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
        let (ctx, table) = setup_data_evolution_table("with_where").await;

        ctx.sql("INSERT INTO target (id, name, value) VALUES (1, 'alice', 10), (2, 'bob', 20), (3, 'charlie', 30)")
            .await.unwrap().collect().await.unwrap();

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
        let (ctx, table) = setup_data_evolution_table("without_where").await;

        ctx.sql("INSERT INTO target (id, name, value) VALUES (1, 'alice', 10), (2, 'bob', 20)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

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
            vec![(1, "alice".to_string(), 99), (2, "bob".to_string(), 99),]
        );
    }

    #[tokio::test]
    async fn test_update_multiple_columns() {
        let (ctx, table) = setup_data_evolution_table("multi_col").await;

        ctx.sql("INSERT INTO target (id, name, value) VALUES (1, 'alice', 10), (2, 'bob', 20)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

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
            vec![(1, "alice".to_string(), 10), (2, "updated".to_string(), 0),]
        );
    }

    #[tokio::test]
    async fn test_update_no_matching_rows() {
        let (ctx, table) = setup_data_evolution_table("no_match").await;

        ctx.sql("INSERT INTO target (id, name, value) VALUES (1, 'alice', 10)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

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
        let (ctx, table) = setup_data_evolution_table("row_id").await;

        ctx.sql("INSERT INTO target (id, name, value) VALUES (1, 'alice', 10), (2, 'bob', 20)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

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
    async fn test_update_rejects_non_data_evolution_table() {
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
            .contains("data-evolution.enabled"));
    }
}
