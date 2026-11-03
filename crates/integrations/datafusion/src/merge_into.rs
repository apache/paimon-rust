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

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, RecordBatch, UInt32Array, UInt64Array};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::prelude::{DataFrame, SessionContext};
use futures::TryStreamExt;

use paimon::spec::{datums_to_binary_row, extract_datum_from_arrow, DataField};
use paimon::table::{CopyOnWriteMergeWriter, DataSplitBuilder, Table};

use crate::error::to_datafusion_error;
use crate::sql_context::SQLContext;

mod native;
pub(crate) use native::plan_merge;

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

/// Tracks registered temporary table names and auto-cleans them up on drop.
///
/// This RAII guard ensures temp tables are always deregistered, even if the
/// enclosing function panics or returns early with an error.
pub(crate) struct TempTableTracker<'a> {
    tables: Vec<String>,
    ctx: &'a SQLContext,
}

impl<'a> TempTableTracker<'a> {
    pub(crate) fn new(ctx: &'a SQLContext) -> Self {
        Self {
            tables: Vec::new(),
            ctx,
        }
    }

    pub(crate) fn register(&mut self, table_name: &str) {
        self.tables.push(table_name.to_string());
    }
}

impl Drop for TempTableTracker<'_> {
    fn drop(&mut self) {
        for table in &self.tables {
            let _ = self.ctx.deregister_temp_table(table);
        }
    }
}

/// Retry a DML operation on conflict, using `is_retryable` to detect retryable errors.
pub(crate) async fn retry_on_conflict<F, Fut, T>(
    op_name: &str,
    is_retryable: fn(&DataFusionError) -> bool,
    mut action: F,
) -> DFResult<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = DFResult<T>>,
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

/// Check if a DataFusion error is caused by a row ID conflict during commit.
pub(crate) fn is_row_id_conflict(err: &DataFusionError) -> bool {
    match err {
        DataFusionError::External(e) => {
            matches!(
                e.downcast_ref::<paimon::Error>(),
                Some(paimon::Error::RowIdExistenceConflict { .. })
            ) || e.to_string().contains("Row ID conflict")
        }
        _ => false,
    }
}

/// Check if a DataFusion error is caused by a delete conflict during commit.
pub(crate) fn is_delete_conflict(err: &DataFusionError) -> bool {
    match err {
        DataFusionError::External(e) => {
            matches!(
                e.downcast_ref::<paimon::Error>(),
                Some(paimon::Error::RowIdExistenceConflict { .. })
            ) || e.to_string().contains("Delete conflict")
        }
        _ => false,
    }
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

pub(crate) fn validate_update_columns(
    columns: &[String],
    table_fields: &[DataField],
) -> DFResult<()> {
    validate_target_columns(columns, table_fields, "UPDATE")
}

fn validate_target_columns(
    columns: &[String],
    table_fields: &[DataField],
    operation: &str,
) -> DFResult<()> {
    let target_columns: HashSet<&str> = table_fields.iter().map(|field| field.name()).collect();
    let mut seen = HashSet::new();

    for column in columns {
        if !seen.insert(column.as_str()) {
            return Err(DataFusionError::Plan(format!(
                "Duplicate column '{column}' in {operation}"
            )));
        }
        if !target_columns.contains(column.as_str()) {
            return Err(DataFusionError::Plan(format!(
                "Unknown column '{column}' in {operation}"
            )));
        }
    }

    Ok(())
}

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
/// Returns `(has_data, cow_table_name)`. The caller is responsible for deregistering
/// via `TempTableTracker`.
///
/// Note: all matching partition files are loaded into memory at once. For partitions
/// with many large files this may cause significant memory pressure. A future
/// optimization could stream or batch-process files instead of materializing everything.
pub(crate) async fn register_cow_target_table(
    ctx: &SQLContext,
    table: &Table,
    writer: &CopyOnWriteMergeWriter,
    temp_tracker: &mut TempTableTracker<'_>,
) -> DFResult<(bool, String)> {
    let all_batches = read_cow_target_batches(table, writer).await?;
    let has_data = !all_batches.is_empty();
    let table_name = next_cow_table_name("__cow_target");
    if let Some(batch) = all_batches.first() {
        let mem_table =
            datafusion::datasource::MemTable::try_new(batch.schema(), vec![all_batches])?;
        ctx.register_temp_table(&table_name, Arc::new(mem_table))?;
        temp_tracker.register(&table_name);
    }
    Ok((has_data, table_name))
}

/// Read a captured CoW file index, retaining physical file/row identity.
async fn read_cow_target_batches(
    table: &Table,
    writer: &CopyOnWriteMergeWriter,
) -> DFResult<Vec<RecordBatch>> {
    let file_index = writer.file_index();
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

            all_batches.push(augmented);
            row_offset = end_offset;
        }
    }

    Ok(all_batches)
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
    ctx: &SQLContext,
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
    let batches = ctx.ctx().sql(&sql).await?.collect().await?;

    build_partition_set_from_batches(table, &batches)
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
    use datafusion::sql::sqlparser::ast::Merge;
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;
    use paimon::catalog::{Catalog, Identifier};
    use paimon::{CatalogOptions, FileSystemCatalog, Options};
    use tempfile::TempDir;

    use crate::SQLContext;

    #[tokio::test]
    async fn test_retry_limits_and_non_conflict_errors() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        for (message, classify) in [
            (
                "Row ID conflict",
                is_row_id_conflict as fn(&DataFusionError) -> bool,
            ),
            (
                "Delete conflict",
                is_delete_conflict as fn(&DataFusionError) -> bool,
            ),
        ] {
            let attempts = AtomicUsize::new(0);
            let result: DFResult<u64> = retry_on_conflict("MERGE INTO", classify, || async {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err(DataFusionError::External(Box::new(std::io::Error::other(
                    message,
                ))))
            })
            .await;
            assert_eq!(attempts.load(Ordering::SeqCst), 5);
            let err = result.unwrap_err().to_string();
            assert!(err.contains("after 5 retries"), "{err}");
            assert!(err.contains(message), "{err}");

            let attempts = AtomicUsize::new(0);
            let result: DFResult<u64> = retry_on_conflict("MERGE INTO", classify, || async {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err(DataFusionError::Execution("unrelated failure".into()))
            })
            .await;
            assert_eq!(attempts.load(Ordering::SeqCst), 1);
            assert!(
                matches!(result, Err(DataFusionError::Execution(message)) if message == "unrelated failure")
            );
            let overlapping_update = to_datafusion_error(paimon::Error::DataInvalid {
                message: "For Data Evolution table, multiple MERGE INTO operations have encountered conflicts, updating the same file".into(),
                source: None,
            });
            assert!(!classify(&overlapping_update));
        }
    }

    async fn setup_sql_context() -> (TempDir, SQLContext, Arc<FileSystemCatalog>) {
        let temp_dir = TempDir::new().unwrap();
        let warehouse = format!("file://{}", temp_dir.path().display());
        let mut options = Options::new();
        options.set(CatalogOptions::WAREHOUSE, warehouse);
        let catalog = Arc::new(FileSystemCatalog::new(options).unwrap());

        let mut sql_context = SQLContext::new();
        sql_context
            .register_catalog("paimon", catalog.clone())
            .await
            .unwrap();
        sql_context
            .sql("CREATE SCHEMA paimon.test_db")
            .await
            .unwrap();

        (temp_dir, sql_context, catalog)
    }

    async fn setup_data_evolution_table(name: &str) -> (TempDir, SQLContext, Table) {
        let (tmp, sql_context, catalog) = setup_sql_context().await;

        sql_context
            .sql(&format!(
                "CREATE TABLE paimon.test_db.{name} (id INT, name VARCHAR, value INT) WITH ('data-evolution.enabled' = 'true', 'row-tracking.enabled' = 'true')"
            ))
            .await
            .unwrap();

        sql_context
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

        (tmp, sql_context, de_table)
    }

    // Exercise the native planner and provider while retaining these behavioral fixtures.
    async fn execute_merge_into(
        ctx: &SQLContext,
        merge: &Merge,
        _table: Table,
        _enable_ident_normalization: bool,
    ) -> DFResult<DataFrame> {
        let batches = ctx.sql(&merge.to_string()).await?.collect().await?;
        ctx.ctx().read_batches(batches)
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
        let (_tmp, sql_context, table) = setup_data_evolution_table("t_merge").await;

        // Create source table with updates
        sql_context
            .sql("CREATE TABLE paimon.test_db.source (id INT, name VARCHAR)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        sql_context
            .sql("INSERT INTO paimon.test_db.source VALUES (1, 'ALICE'), (3, 'CHARLIE')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Execute MERGE INTO
        let merge = parse_merge(
            "MERGE INTO paimon.test_db.t_merge t USING paimon.test_db.source s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET NAME = s.name",
        );
        execute_merge_into(&sql_context, &merge, table, true)
            .await
            .unwrap();

        let batches = sql_context
            .sql("SELECT id, name, value FROM paimon.test_db.t_merge ORDER BY id")
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
                .downcast_ref::<datafusion::arrow::array::StringViewArray>()
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
        let (_tmp, sql_context, table) = setup_data_evolution_table("t_merge2").await;

        sql_context
            .sql("CREATE TABLE paimon.test_db.source (id INT, name VARCHAR)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        sql_context
            .sql("INSERT INTO paimon.test_db.source VALUES (99, 'nobody')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let merge = parse_merge(
            "MERGE INTO paimon.test_db.t_merge2 t USING paimon.test_db.source s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name",
        );
        let result = execute_merge_into(&sql_context, &merge, table, true)
            .await
            .unwrap();
        let batches = result.collect().await.unwrap();
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 0);
    }

    // -----------------------------------------------------------------------
    // CoW MERGE INTO tests (append-only tables)
    // -----------------------------------------------------------------------

    async fn setup_append_only_table(name: &str) -> (TempDir, SQLContext, Table) {
        let (tmp, sql_context, catalog) = setup_sql_context().await;

        sql_context
            .sql(&format!(
                "CREATE TABLE paimon.test_db.{name} (id INT, name VARCHAR, value INT)"
            ))
            .await
            .unwrap();

        sql_context
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

        (tmp, sql_context, table)
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
                .downcast_ref::<datafusion::arrow::array::StringViewArray>()
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
        let (_tmp, sql_context, table) = setup_append_only_table("t_cow_upd").await;

        sql_context
            .sql("CREATE TABLE paimon.test_db.source (id INT, name VARCHAR)")
            .await
            .unwrap();
        sql_context
            .sql("INSERT INTO paimon.test_db.source (id, name) VALUES (1, 'ALICE'), (3, 'CHARLIE')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let merge = parse_merge(
            "MERGE INTO paimon.test_db.t_cow_upd t USING paimon.test_db.source s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET NAME = s.name",
        );
        execute_merge_into(&sql_context, &merge, table, true)
            .await
            .unwrap();

        let batches = sql_context
            .sql("SELECT id, name, value FROM paimon.test_db.t_cow_upd ORDER BY id")
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
        let (_tmp, sql_context, table) = setup_append_only_table("t_cow_del").await;

        sql_context
            .sql("CREATE TABLE paimon.test_db.source (id INT)")
            .await
            .unwrap();
        sql_context
            .sql("INSERT INTO paimon.test_db.source (id) VALUES (2)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let merge = parse_merge(
            "MERGE INTO paimon.test_db.t_cow_del t USING paimon.test_db.source s ON t.id = s.id \
             WHEN MATCHED THEN DELETE",
        );
        execute_merge_into(&sql_context, &merge, table, true)
            .await
            .unwrap();

        let batches = sql_context
            .sql("SELECT id, name, value FROM paimon.test_db.t_cow_del ORDER BY id")
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
        let (_tmp, sql_context, table) = setup_append_only_table("t_cow_ins").await;

        sql_context
            .sql("CREATE TABLE paimon.test_db.source (id INT, name VARCHAR, value INT)")
            .await
            .unwrap();
        sql_context
            .sql("INSERT INTO paimon.test_db.source VALUES (4, 'dave', 40), (5, 'eve', 50)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let merge = parse_merge(
            "MERGE INTO paimon.test_db.t_cow_ins t USING paimon.test_db.source s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (s.id, s.name, s.value)",
        );
        execute_merge_into(&sql_context, &merge, table, true)
            .await
            .unwrap();

        let batches = sql_context
            .sql("SELECT id, name, value FROM paimon.test_db.t_cow_ins ORDER BY id")
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
    async fn test_cow_merge_insert_not_matched_without_columns() {
        let (_tmp, sql_context, table) = setup_append_only_table("t_cow_ins_nocols").await;

        sql_context
            .sql("CREATE TABLE paimon.test_db.source (id INT, name VARCHAR, value INT)")
            .await
            .unwrap();
        sql_context
            .sql("INSERT INTO paimon.test_db.source VALUES (4, 'dave', 40), (5, 'eve', 50)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // No column list: the VALUES map to the table's columns by position.
        // Before the fix this dropped the source values and inserted all-NULL rows.
        let merge = parse_merge(
            "MERGE INTO paimon.test_db.t_cow_ins_nocols t USING paimon.test_db.source s \
             ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.value)",
        );
        execute_merge_into(&sql_context, &merge, table, true)
            .await
            .unwrap();

        let batches = sql_context
            .sql("SELECT id, name, value FROM paimon.test_db.t_cow_ins_nocols ORDER BY id")
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
    async fn test_cow_merge_insert_arity_validated_even_when_all_matched() {
        let (_tmp, sql_context, table) = setup_append_only_table("t_cow_ins_arity").await;

        sql_context
            .sql("CREATE TABLE paimon.test_db.source (id INT, name VARCHAR, value INT)")
            .await
            .unwrap();
        // id = 1 matches the target, so the NOT MATCHED INSERT branch is never
        // built — the arity check must still run upfront.
        sql_context
            .sql("INSERT INTO paimon.test_db.source VALUES (1, 'ALICE', 99)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // The column-less INSERT lists 2 values for a 3-column table. Even with no
        // row to insert, the invalid arity must be rejected before execution so the
        // matched UPDATE is not applied.
        let merge = parse_merge(
            "MERGE INTO paimon.test_db.t_cow_ins_arity t USING paimon.test_db.source s \
             ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET value = s.value \
             WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name)",
        );
        let err = execute_merge_into(&sql_context, &merge, table, true)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("column(s) but"), "got {err}");

        // The target is unchanged: the matched UPDATE did not apply.
        let batches = sql_context
            .sql("SELECT id, name, value FROM paimon.test_db.t_cow_ins_arity ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            collect_rows(&batches),
            vec![
                (1, "alice".to_string(), 10),
                (2, "bob".to_string(), 20),
                (3, "charlie".to_string(), 30),
            ]
        );
    }

    #[tokio::test]
    async fn test_cow_merge_update_and_insert() {
        let (_tmp, sql_context, table) = setup_append_only_table("t_cow_upsert").await;

        sql_context
            .sql("CREATE TABLE paimon.test_db.source (id INT, name VARCHAR, value INT)")
            .await
            .unwrap();
        sql_context
            .sql("INSERT INTO paimon.test_db.source VALUES (2, 'BOB', 200), (4, 'dave', 40)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let merge = parse_merge(
            "MERGE INTO paimon.test_db.t_cow_upsert t USING paimon.test_db.source s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name, value = s.value \
             WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (s.id, s.name, s.value)",
        );
        execute_merge_into(&sql_context, &merge, table, true)
            .await
            .unwrap();

        let batches = sql_context
            .sql("SELECT id, name, value FROM paimon.test_db.t_cow_upsert ORDER BY id")
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
    async fn test_cow_merge_matched_null_predicates_fall_through() {
        for delete_fallback in [false, true] {
            let (_tmp, sql_context, table) = setup_append_only_table("t_null_matched").await;
            for sql in [
                "CREATE TABLE paimon.test_db.source (id INT, flag INT)",
                "INSERT INTO paimon.test_db.source VALUES (1, NULL), (2, 1), (3, 0)",
            ] {
                sql_context.sql(sql).await.unwrap().collect().await.unwrap();
            }

            let fallback = if delete_fallback {
                "DELETE"
            } else {
                "UPDATE SET name = 'fallback'"
            };
            let merge = parse_merge(&format!(
                "MERGE INTO paimon.test_db.t_null_matched t USING paimon.test_db.source s \
                 ON t.id = s.id \
                 WHEN MATCHED AND s.flag = 1 THEN UPDATE SET name = 'first' \
                 WHEN MATCHED AND s.flag = 0 THEN UPDATE SET name = 'second' \
                 WHEN MATCHED THEN {fallback}"
            ));
            let result = execute_merge_into(&sql_context, &merge, table, true)
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            assert_eq!(
                result[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(0),
                3,
            );

            let batches = sql_context
                .sql("SELECT id, name, value FROM paimon.test_db.t_null_matched ORDER BY id")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let mut expected = vec![(2, "first".to_string(), 20), (3, "second".to_string(), 30)];
            if !delete_fallback {
                expected.insert(0, (1, "fallback".to_string(), 10));
            }
            assert_eq!(collect_rows(&batches), expected);
        }
    }

    #[tokio::test]
    async fn test_merge_insert_null_predicates_fall_through() {
        for data_evolution in [false, true] {
            let (_tmp, sql_context, table) = if data_evolution {
                setup_data_evolution_table("t_null_insert").await
            } else {
                setup_append_only_table("t_null_insert").await
            };
            for sql in [
                "CREATE TABLE paimon.test_db.source (id INT, flag INT)",
                "INSERT INTO paimon.test_db.source VALUES (4, NULL), (5, 1), (6, 0)",
            ] {
                sql_context.sql(sql).await.unwrap().collect().await.unwrap();
            }

            let merge = parse_merge(
                "MERGE INTO paimon.test_db.t_null_insert t USING paimon.test_db.source s \
                 ON t.id = s.id \
                 WHEN NOT MATCHED AND s.flag = 1 THEN INSERT (id, name, value) VALUES (s.id, 'first', 100) \
                 WHEN NOT MATCHED AND s.flag = 0 THEN INSERT (id, name, value) VALUES (s.id, 'second', 100) \
                 WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (s.id, 'fallback', 100)",
            );
            let result = execute_merge_into(&sql_context, &merge, table, true)
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            assert_eq!(
                result[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(0),
                3,
            );

            let batches = sql_context
                .sql("SELECT id, name, value FROM paimon.test_db.t_null_insert ORDER BY id")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            assert_eq!(
                collect_rows(&batches),
                vec![
                    (1, "alice".to_string(), 10),
                    (2, "bob".to_string(), 20),
                    (3, "charlie".to_string(), 30),
                    (4, "fallback".to_string(), 100),
                    (5, "first".to_string(), 100),
                    (6, "second".to_string(), 100),
                ],
            );
        }
    }

    #[tokio::test]
    async fn test_cow_merge_matched_or_predicates_preserve_clause_order() {
        for delete_second in [false, true] {
            let (_tmp, sql_context, table) = setup_append_only_table("t_or_matched").await;
            for sql in [
                "CREATE TABLE paimon.test_db.source (id INT, flag INT)",
                "INSERT INTO paimon.test_db.source VALUES (1, 1), (2, 0), (3, NULL)",
            ] {
                sql_context.sql(sql).await.unwrap().collect().await.unwrap();
            }

            let second_action = if delete_second {
                "DELETE"
            } else {
                "UPDATE SET name = 'second'"
            };
            let merge = parse_merge(&format!(
                "MERGE INTO paimon.test_db.t_or_matched t USING paimon.test_db.source s \
                 ON t.id = s.id \
                 WHEN MATCHED AND s.flag = 1 THEN UPDATE SET name = 'first' \
                 WHEN MATCHED AND s.flag = 0 OR s.flag = 1 THEN {second_action} \
                 WHEN MATCHED THEN UPDATE SET name = 'fallback'"
            ));
            let result = execute_merge_into(&sql_context, &merge, table, true)
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            assert_eq!(
                result[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(0),
                3,
            );

            let batches = sql_context
                .sql("SELECT id, name, value FROM paimon.test_db.t_or_matched ORDER BY id")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let mut expected = vec![
                (1, "first".to_string(), 10),
                (3, "fallback".to_string(), 30),
            ];
            if !delete_second {
                expected.insert(1, (2, "second".to_string(), 20));
            }
            assert_eq!(collect_rows(&batches), expected);
        }
    }

    #[tokio::test]
    async fn test_merge_insert_or_predicates_preserve_clause_order() {
        for data_evolution in [false, true] {
            let (_tmp, sql_context, table) = if data_evolution {
                setup_data_evolution_table("t_or_insert").await
            } else {
                setup_append_only_table("t_or_insert").await
            };
            for sql in [
                "CREATE TABLE paimon.test_db.source (id INT, flag INT)",
                "INSERT INTO paimon.test_db.source VALUES (4, 1), (5, 0), (6, NULL)",
            ] {
                sql_context.sql(sql).await.unwrap().collect().await.unwrap();
            }

            let merge = parse_merge(
                "MERGE INTO paimon.test_db.t_or_insert t USING paimon.test_db.source s \
                 ON t.id = s.id \
                 WHEN NOT MATCHED AND s.flag = 1 THEN INSERT (id, name, value) VALUES (s.id, 'first', 100) \
                 WHEN NOT MATCHED AND s.flag = 0 OR s.flag = 1 THEN INSERT (id, name, value) VALUES (s.id, 'second', 100) \
                 WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (s.id, 'fallback', 100)",
            );
            let result = execute_merge_into(&sql_context, &merge, table, true)
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            assert_eq!(
                result[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(0),
                3,
            );

            let batches = sql_context
                .sql("SELECT id, name, value FROM paimon.test_db.t_or_insert ORDER BY id")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            assert_eq!(
                collect_rows(&batches),
                vec![
                    (1, "alice".to_string(), 10),
                    (2, "bob".to_string(), 20),
                    (3, "charlie".to_string(), 30),
                    (4, "first".to_string(), 100),
                    (5, "second".to_string(), 100),
                    (6, "fallback".to_string(), 100),
                ],
            );
        }
    }

    #[tokio::test]
    async fn test_cow_merge_preserves_string_literals() {
        for (on_predicate, matched_predicate, insert_predicate) in [
            ("", "", ""),
            (" AND s.name = 'paimon.test_db.source.payload'", "", ""),
            ("", " AND s.name = 'paimon.test_db.source.payload'", ""),
            ("", "", " AND s.name = 'paimon.test_db.source.payload'"),
        ] {
            let (_tmp, sql_context, table) = setup_append_only_table("target").await;
            for sql in [
                "CREATE TABLE paimon.test_db.source (id INT, name VARCHAR)",
                "INSERT INTO paimon.test_db.source VALUES (1, 'paimon.test_db.source.payload'), (4, 'paimon.test_db.source.payload')",
            ] {
                sql_context.sql(sql).await.unwrap().collect().await.unwrap();
            }

            let merge = parse_merge(&format!(
                "MERGE INTO paimon.test_db.target t USING paimon.test_db.source s \
                 ON t.id = s.id{on_predicate} \
                 WHEN MATCHED{matched_predicate} THEN UPDATE SET name = 'paimon.test_db.target.payload' \
                 WHEN NOT MATCHED{insert_predicate} THEN INSERT (id, name, value) VALUES (s.id, 'paimon.test_db.source.payload', 40)"
            ));
            let result = execute_merge_into(&sql_context, &merge, table, true)
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            assert_eq!(
                result[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(0),
                2,
            );

            let batches = sql_context
                .sql("SELECT id, name, value FROM paimon.test_db.target ORDER BY id")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            assert_eq!(
                collect_rows(&batches),
                vec![
                    (1, "paimon.test_db.target.payload".to_string(), 10),
                    (2, "bob".to_string(), 20),
                    (3, "charlie".to_string(), 30),
                    (4, "paimon.test_db.source.payload".to_string(), 40),
                ],
                "ON: {on_predicate}, MATCHED: {matched_predicate}, INSERT: {insert_predicate}",
            );
        }
    }

    #[tokio::test]
    async fn test_cow_merge_no_matches() {
        let (_tmp, sql_context, table) = setup_append_only_table("t_cow_nomatch").await;

        sql_context
            .sql("CREATE TABLE paimon.test_db.source (id INT, name VARCHAR)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let merge = parse_merge(
            "MERGE INTO paimon.test_db.t_cow_nomatch t USING paimon.test_db.source s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name",
        );
        let result = execute_merge_into(&sql_context, &merge, table, true)
            .await
            .unwrap();
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
