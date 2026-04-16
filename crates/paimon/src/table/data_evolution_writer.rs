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

//! Row-ID-based update writer for data evolution tables.
//!
//! [`DataEvolutionWriter`] accepts rows to update (identified by `_ROW_ID`) along with
//! new column values, then handles file metadata lookup, row grouping,
//! reading original columns, applying updates, and writing partial-column files.
//!
//! The writer does NOT commit — it returns `Vec<CommitMessage>` that the caller
//! passes to [`TableCommit`](super::TableCommit).
//! This separation allows callers to compose multiple operations into a single commit,
//! similar to Iceberg's Transaction/Action pattern.

use crate::io::FileIO;
use crate::spec::{BinaryRow, CoreOptions, DataFileMeta, PartitionComputer};
use crate::table::commit_message::CommitMessage;
use crate::table::data_file_writer::DataFileWriter;
use crate::table::stats_filter::group_by_overlapping_row_id;
use crate::table::DataSplitBuilder;
use crate::table::Table;
use crate::Result;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch};
use arrow_select::concat::concat_batches;
use arrow_select::interleave::interleave;
use futures::TryStreamExt;
use std::collections::HashMap;

/// Engine-agnostic writer for partial-column updates via `_ROW_ID`.
///
/// Usage:
/// 1. Create via [`DataEvolutionWriter::new`] (validates preconditions).
/// 2. Feed matched rows via [`add_matched_batch`](Self::add_matched_batch).
///    Each batch must contain a `_ROW_ID` (Int64) column plus the update columns.
/// 3. Call [`prepare_commit`](Self::prepare_commit) to produce `CommitMessage`s.
/// 4. Commit via [`TableCommit`](super::TableCommit) (caller's responsibility).
///
/// The query engine (DataFusion, custom, etc.) is responsible for:
/// - Parsing the MERGE INTO SQL
/// - Executing the JOIN to find matched rows
/// - Computing new column values
/// - Passing the results as `RecordBatch`es with `_ROW_ID` + update columns
#[must_use = "writer must be used to call prepare_commit()"]
pub struct DataEvolutionWriter {
    table: Table,
    update_columns: Vec<String>,
    matched_batches: Vec<RecordBatch>,
}

fn schema_contains_blob_type(table: &Table) -> bool {
    table
        .schema()
        .fields()
        .iter()
        .any(|field| field.data_type().contains_blob_type())
}

impl DataEvolutionWriter {
    /// Create a new writer for the given table and update columns.
    ///
    /// Validates:
    /// - `data-evolution.enabled = true`
    /// - `row-tracking.enabled = true`
    /// - No primary keys
    /// - Update columns don't include partition keys
    pub fn new(table: &Table, update_columns: Vec<String>) -> Result<Self> {
        let schema = table.schema();
        let core_options = CoreOptions::new(schema.options());

        if schema_contains_blob_type(table) {
            return Err(crate::Error::Unsupported {
                message:
                    "MERGE INTO does not support BlobType yet; blob write path is out of scope"
                        .to_string(),
            });
        }

        if !core_options.data_evolution_enabled() {
            return Err(crate::Error::Unsupported {
                message:
                    "MERGE INTO is only supported for tables with 'data-evolution.enabled' = 'true'"
                        .to_string(),
            });
        }
        if !core_options.row_tracking_enabled() {
            return Err(crate::Error::Unsupported {
                message: "MERGE INTO requires 'row-tracking.enabled' = 'true'".to_string(),
            });
        }
        if !schema.trimmed_primary_keys().is_empty() {
            return Err(crate::Error::Unsupported {
                message: "MERGE INTO on data evolution tables does not support primary keys"
                    .to_string(),
            });
        }

        let partition_keys = schema.partition_keys();
        for col in &update_columns {
            if partition_keys.contains(col) {
                return Err(crate::Error::Unsupported {
                    message: format!("Cannot update partition column '{col}' in MERGE INTO"),
                });
            }
        }

        Ok(Self {
            table: table.clone(),
            update_columns,
            matched_batches: Vec::new(),
        })
    }

    /// Add a batch of matched rows.
    ///
    /// The batch must contain:
    /// - A `_ROW_ID` column (Int64) identifying which rows to update
    /// - One column for each entry in `update_columns` with the new values
    pub fn add_matched_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        // Validate _ROW_ID column exists
        if batch.column_by_name("_ROW_ID").is_none() {
            return Err(crate::Error::DataInvalid {
                message: "Matched batch must contain a '_ROW_ID' column".to_string(),
                source: None,
            });
        }

        self.matched_batches.push(batch);
        Ok(())
    }

    /// Scan file metadata, group matched rows by file, read originals,
    /// apply updates, and write partial-column files.
    ///
    /// Returns `CommitMessage`s for the caller to commit via [`TableCommit`](super::TableCommit).
    #[must_use = "commit messages must be passed to TableCommit"]
    pub async fn prepare_commit(self) -> Result<Vec<CommitMessage>> {
        let total_matched: usize = self.matched_batches.iter().map(|b| b.num_rows()).sum();
        if total_matched == 0 {
            return Ok(Vec::new());
        }

        // 1. Scan file metadata and build row_id -> file group index.
        //    In data-evolution tables, multiple files can share the same first_row_id
        //    (base file + partial-column files). We must group them so the reader
        //    can merge columns correctly.
        let scan = self.table.new_read_builder().new_scan();
        let plan = scan.plan().await?;

        let mut file_index: Vec<FileRowRange> = Vec::new();
        for split in plan.splits() {
            let partition_bytes = split.partition().to_serialized_bytes();
            let bucket = split.bucket();
            let bucket_path = split.bucket_path().to_string();
            let snapshot_id = split.snapshot_id();
            let total_buckets = split.total_buckets();

            let all_files: Vec<DataFileMeta> = split
                .data_files()
                .iter()
                .filter(|f| f.first_row_id.is_some())
                .cloned()
                .collect();

            let groups = group_by_overlapping_row_id(all_files);
            for group in groups {
                // Compute the overall row_id range for this group.
                // The base file has the widest range; partial-column files share it.
                let first_row_id = group.iter().filter_map(|f| f.first_row_id).min().unwrap();
                let last_row_id = group
                    .iter()
                    .filter_map(|f| f.row_id_range().map(|(_, end)| end))
                    .max()
                    .unwrap();
                // The actual row count is the max among the group (base file's count).
                let row_count = group.iter().map(|f| f.row_count).max().unwrap();

                file_index.push(FileRowRange {
                    first_row_id,
                    last_row_id,
                    row_count,
                    partition: partition_bytes.clone(),
                    bucket,
                    bucket_path: bucket_path.clone(),
                    snapshot_id,
                    total_buckets,
                    files: group,
                });
            }
        }
        file_index.sort_by_key(|f| f.first_row_id);

        if file_index.is_empty() {
            return Err(crate::Error::DataInvalid {
                message: "No files with row tracking found in target table".to_string(),
                source: None,
            });
        }

        // 2. Group matched rows by their owning file
        let mut file_matches: HashMap<usize, Vec<MatchedRow>> = HashMap::new();

        for (batch_idx, batch) in self.matched_batches.iter().enumerate() {
            let row_id_col = batch
                .column_by_name("_ROW_ID")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: "_ROW_ID column must be Int64".to_string(),
                    source: None,
                })?;

            for row_idx in 0..batch.num_rows() {
                let row_id = row_id_col.value(row_idx);
                let (file_pos, file_range) =
                    find_owning_file(&file_index, row_id).ok_or_else(|| {
                        crate::Error::DataInvalid {
                            message: format!("No file found for _ROW_ID {row_id}"),
                            source: None,
                        }
                    })?;

                let offset = (row_id - file_range.first_row_id) as usize;
                file_matches.entry(file_pos).or_default().push(MatchedRow {
                    offset,
                    batch_idx,
                    row_idx,
                });
            }
        }

        // 3. For each affected file: read original columns, apply updates, write partial files
        let mut writer = DataEvolutionPartialWriter::new(&self.table, self.update_columns.clone())?;

        for (&file_pos, matched_rows) in &file_matches {
            let file_range = &file_index[file_pos];
            let first_row_id = file_range.first_row_id;
            let row_count = file_range.row_count as usize;

            // Read original columns from the entire file group (base + partial-column files).
            let col_refs: Vec<&str> = self.update_columns.iter().map(|s| s.as_str()).collect();
            let mut rb = self.table.new_read_builder();
            rb.with_projection(&col_refs);
            let read = rb.new_read()?;

            let split = DataSplitBuilder::new()
                .with_snapshot(file_range.snapshot_id)
                .with_partition(BinaryRow::from_serialized_bytes(&file_range.partition)?)
                .with_bucket(file_range.bucket)
                .with_bucket_path(file_range.bucket_path.clone())
                .with_total_buckets(file_range.total_buckets)
                .with_data_files(file_range.files.clone())
                .build()?;

            let stream = read.to_arrow(&[split])?;
            let original_batches: Vec<RecordBatch> = stream.try_collect().await?;

            let original_batch = if original_batches.is_empty() {
                continue;
            } else if original_batches.len() == 1 {
                original_batches.into_iter().next().unwrap()
            } else {
                concat_batches(&original_batches[0].schema(), &original_batches).map_err(|e| {
                    crate::Error::DataInvalid {
                        message: format!("Failed to concat batches: {e}"),
                        source: None,
                    }
                })?
            };

            if original_batch.num_rows() != row_count {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Expected {} rows from file, got {}",
                        row_count,
                        original_batch.num_rows()
                    ),
                    source: None,
                });
            }

            // Apply updates using 2-array interleave: [original_col, updates_col].
            // Matched rows are gathered into a single contiguous update array first,
            // avoiding O(N) array clones for every row in the file.
            let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(self.update_columns.len());

            // Sort matched rows by offset for contiguous iteration
            let mut sorted_matches: Vec<(usize, usize, usize)> = matched_rows
                .iter()
                .map(|m| (m.offset, m.batch_idx, m.row_idx))
                .collect();
            sorted_matches.sort_by_key(|(offset, _, _)| *offset);

            for (col_idx, col_name) in self.update_columns.iter().enumerate() {
                let original_col = original_batch.column(col_idx);
                let original_dtype = original_col.data_type();

                let join_col_idx = self.matched_batches[0]
                    .schema()
                    .index_of(col_name)
                    .map_err(|e| crate::Error::DataInvalid {
                        message: format!("Column {col_name} not found in matched batch: {e}"),
                        source: None,
                    })?;

                // Gather update values into a single array (one entry per matched row, in offset order)
                let update_indices: Vec<(usize, usize)> = sorted_matches
                    .iter()
                    .map(|&(_, batch_idx, row_idx)| (batch_idx, row_idx))
                    .collect();

                // Collect unique batch arrays, cast if needed
                let mut batch_arrays: Vec<ArrayRef> = Vec::new();
                let mut batch_id_map: HashMap<usize, usize> = HashMap::new();
                let mut interleave_src: Vec<(usize, usize)> =
                    Vec::with_capacity(update_indices.len());

                for &(batch_idx, row_idx) in &update_indices {
                    let arr_idx = match batch_id_map.get(&batch_idx) {
                        Some(&idx) => idx,
                        None => {
                            let src_col = self.matched_batches[batch_idx].column(join_col_idx);
                            let casted = if src_col.data_type() != original_dtype {
                                arrow_cast::cast(src_col, original_dtype).map_err(|e| {
                                    crate::Error::DataInvalid {
                                        message: format!("Failed to cast column {col_name}: {e}"),
                                        source: None,
                                    }
                                })?
                            } else {
                                src_col.clone()
                            };
                            let idx = batch_arrays.len();
                            batch_arrays.push(casted);
                            batch_id_map.insert(batch_idx, idx);
                            idx
                        }
                    };
                    interleave_src.push((arr_idx, row_idx));
                }

                let update_col = if batch_arrays.len() == 1 && interleave_src.len() == 1 {
                    // Single update value — just slice
                    let (_, row_idx) = interleave_src[0];
                    batch_arrays[0].slice(row_idx, 1)
                } else {
                    let refs: Vec<&dyn Array> = batch_arrays.iter().map(|a| a.as_ref()).collect();
                    interleave(&refs, &interleave_src).map_err(|e| crate::Error::DataInvalid {
                        message: format!("Failed to gather update values for {col_name}: {e}"),
                        source: None,
                    })?
                };

                // Build final indices: 2 sources — [0] = original, [1] = update_col
                let mut indices: Vec<(usize, usize)> = Vec::with_capacity(row_count);
                let mut match_pos = 0;
                for row in 0..row_count {
                    if match_pos < sorted_matches.len() && sorted_matches[match_pos].0 == row {
                        indices.push((1, match_pos));
                        match_pos += 1;
                    } else {
                        indices.push((0, row));
                    }
                }

                let sources: [&dyn Array; 2] = [original_col.as_ref(), update_col.as_ref()];
                let new_col =
                    interleave(&sources, &indices).map_err(|e| crate::Error::DataInvalid {
                        message: format!("Failed to interleave column {col_name}: {e}"),
                        source: None,
                    })?;
                new_columns.push(new_col);
            }

            let updated_batch = RecordBatch::try_new(original_batch.schema(), new_columns)
                .map_err(|e| crate::Error::DataInvalid {
                    message: format!("Failed to create updated batch: {e}"),
                    source: None,
                })?;

            writer
                .write_partial_batch(
                    file_range.partition.clone(),
                    file_range.bucket,
                    first_row_id,
                    updated_batch,
                )
                .await?;
        }

        // 4. Collect commit messages (caller is responsible for committing)
        writer.prepare_commit().await
    }
}

/// Binary search for the file that owns a given row_id.
fn find_owning_file(file_index: &[FileRowRange], row_id: i64) -> Option<(usize, &FileRowRange)> {
    let pos = file_index.partition_point(|f| f.first_row_id <= row_id);
    if pos == 0 {
        return None;
    }
    let idx = pos - 1;
    let candidate = &file_index[idx];
    if row_id <= candidate.last_row_id {
        Some((idx, candidate))
    } else {
        None
    }
}

struct FileRowRange {
    first_row_id: i64,
    last_row_id: i64,
    row_count: i64,
    partition: Vec<u8>,
    bucket: i32,
    bucket_path: String,
    snapshot_id: i64,
    total_buckets: i32,
    /// All files in this row-id group (base file + partial-column files).
    files: Vec<DataFileMeta>,
}

struct MatchedRow {
    offset: usize,
    batch_idx: usize,
    row_idx: usize,
}

// ---------------------------------------------------------------------------
// DataEvolutionPartialWriter — writes partial-column parquet files for data evolution
// ---------------------------------------------------------------------------

/// Key: (partition_bytes, bucket, first_row_id)
type WriterKey = (Vec<u8>, i32, i64);

/// Writer for data evolution partial-column files.
///
/// Unlike [`TableWrite`](super::TableWrite) which writes full-row files for append-only tables,
/// `DataEvolutionPartialWriter` writes partial-column files used by MERGE INTO on data evolution tables.
/// Each output file contains only the updated columns and shares the same `first_row_id` range
/// as the original file, allowing the reader to merge columns at read time.
///
/// Produces parquet files containing only the specified `write_columns`, with
/// `file_source = APPEND (0)`, caller-supplied `first_row_id`, and `write_cols`.
pub(crate) struct DataEvolutionPartialWriter {
    file_io: FileIO,
    table_location: String,
    partition_computer: PartitionComputer,
    partition_keys: Vec<String>,
    schema_id: i64,
    target_file_size: i64,
    file_compression: String,
    file_compression_zstd_level: i32,
    write_buffer_size: i64,
    write_columns: Vec<String>,
    /// Writers keyed by (partition_bytes, bucket, first_row_id).
    writers: HashMap<WriterKey, DataFileWriter>,
}

impl DataEvolutionPartialWriter {
    /// Create a new writer for partial-column data evolution files.
    ///
    /// `write_columns` specifies which table columns this write covers (the SET targets).
    pub fn new(table: &Table, write_columns: Vec<String>) -> Result<Self> {
        let schema = table.schema();
        let core_options = CoreOptions::new(schema.options());

        if schema_contains_blob_type(table) {
            return Err(crate::Error::Unsupported {
                message: "DataEvolutionPartialWriter does not support BlobType yet".to_string(),
            });
        }

        if !core_options.data_evolution_enabled() {
            return Err(crate::Error::Unsupported {
                message: "DataEvolutionPartialWriter requires data-evolution.enabled = true"
                    .to_string(),
            });
        }

        let partition_keys: Vec<String> = schema.partition_keys().to_vec();
        let fields = schema.fields();
        let partition_computer = PartitionComputer::new(
            &partition_keys,
            fields,
            core_options.partition_default_name(),
            core_options.legacy_partition_name(),
        )?;

        Ok(Self {
            file_io: table.file_io().clone(),
            table_location: table.location().to_string(),
            partition_computer,
            partition_keys,
            schema_id: schema.id(),
            target_file_size: core_options.target_file_size(),
            file_compression: core_options.file_compression().to_string(),
            file_compression_zstd_level: core_options.file_compression_zstd_level(),
            write_buffer_size: core_options.write_parquet_buffer_size(),
            write_columns,
            writers: HashMap::new(),
        })
    }

    /// Write a partial-column batch for a specific partition, bucket, and row ID range.
    ///
    /// The `batch` must contain only the columns specified in `write_columns`.
    /// `first_row_id` must match the original file's `first_row_id` for the affected rows.
    pub async fn write_partial_batch(
        &mut self,
        partition_bytes: Vec<u8>,
        bucket: i32,
        first_row_id: i64,
        batch: RecordBatch,
    ) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let key = (partition_bytes.clone(), bucket, first_row_id);
        if !self.writers.contains_key(&key) {
            let partition_path = if self.partition_keys.is_empty() {
                String::new()
            } else {
                let row = BinaryRow::from_serialized_bytes(&partition_bytes)?;
                self.partition_computer.generate_partition_path(&row)?
            };

            let writer = DataFileWriter::new(
                self.file_io.clone(),
                self.table_location.clone(),
                partition_path,
                bucket,
                self.schema_id,
                self.target_file_size,
                self.file_compression.clone(),
                self.file_compression_zstd_level,
                self.write_buffer_size,
                Some(0), // file_source: APPEND
                Some(first_row_id),
                Some(self.write_columns.clone()),
            );
            self.writers.insert(key.clone(), writer);
        }

        let writer = self.writers.get_mut(&key).unwrap();
        writer.write(&batch).await
    }

    /// Close all writers and collect CommitMessages for use with TableCommit.
    pub async fn prepare_commit(&mut self) -> Result<Vec<CommitMessage>> {
        let writers: Vec<(WriterKey, DataFileWriter)> = self.writers.drain().collect();

        let futures: Vec<_> = writers
            .into_iter()
            .map(
                |((partition_bytes, bucket, _first_row_id), mut writer)| async move {
                    let files = writer.prepare_commit().await?;
                    Ok::<_, crate::Error>((partition_bytes, bucket, files))
                },
            )
            .collect();

        let results = futures::future::try_join_all(futures).await?;

        // Group files by (partition, bucket) since multiple first_row_ids may share the same partition/bucket
        let mut grouped: HashMap<(Vec<u8>, i32), Vec<crate::spec::DataFileMeta>> = HashMap::new();
        for (partition_bytes, bucket, files) in results {
            grouped
                .entry((partition_bytes, bucket))
                .or_default()
                .extend(files);
        }

        let mut messages = Vec::new();
        for ((partition_bytes, bucket), files) in grouped {
            if !files.is_empty() {
                messages.push(CommitMessage::new(partition_bytes, bucket, files));
            }
        }
        Ok(messages)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Identifier;
    use crate::io::FileIOBuilder;
    use crate::spec::{
        BlobType, DataField, DataType, IntType, RowType, Schema, TableSchema, VarCharType,
    };
    use arrow_array::StringArray;
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use std::sync::Arc;

    fn test_file_io() -> FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    fn make_test_file_meta(
        file_name: &str,
        row_count: i64,
        first_row_id: Option<i64>,
        max_seq: i64,
        write_cols: Option<Vec<String>>,
    ) -> DataFileMeta {
        use crate::spec::stats::BinaryTableStats;
        let empty_stats = BinaryTableStats::new(vec![], vec![], vec![]);
        DataFileMeta {
            file_name: file_name.to_string(),
            file_size: 0,
            row_count,
            min_key: vec![],
            max_key: vec![],
            key_stats: empty_stats.clone(),
            value_stats: empty_stats,
            min_sequence_number: 0,
            max_sequence_number: max_seq,
            schema_id: 0,
            level: 0,
            extra_files: vec![],
            creation_time: None,
            delete_row_count: None,
            embedded_index: None,
            file_source: Some(0),
            value_stats_cols: None,
            external_path: None,
            first_row_id,
            write_cols,
        }
    }

    fn test_data_evolution_schema() -> TableSchema {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("name", DataType::VarChar(VarCharType::string_type()))
            .column("value", DataType::Int(IntType::new()))
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true")
            .build()
            .unwrap();
        TableSchema::new(0, &schema)
    }

    fn test_blob_data_evolution_schema() -> TableSchema {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column(
                "payload",
                DataType::Row(RowType::new(vec![DataField::new(
                    1,
                    "blob".into(),
                    DataType::Blob(BlobType::new()),
                )])),
            )
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true")
            .build()
            .unwrap();
        TableSchema::new(0, &schema)
    }

    fn test_table(file_io: &FileIO, table_path: &str) -> Table {
        Table::new(
            file_io.clone(),
            Identifier::new("default", "test_de_table"),
            table_path.to_string(),
            test_data_evolution_schema(),
            None,
        )
    }

    fn test_blob_table(file_io: &FileIO, table_path: &str) -> Table {
        Table::new(
            file_io.clone(),
            Identifier::new("default", "test_de_blob_table"),
            table_path.to_string(),
            test_blob_data_evolution_schema(),
            None,
        )
    }

    async fn setup_dirs(file_io: &FileIO, table_path: &str) {
        file_io
            .mkdirs(&format!("{table_path}/snapshot/"))
            .await
            .unwrap();
        file_io
            .mkdirs(&format!("{table_path}/manifest/"))
            .await
            .unwrap();
    }

    fn make_partial_batch(names: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "name",
            ArrowDataType::Utf8,
            true,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(names))]).unwrap()
    }

    #[tokio::test]
    async fn test_write_partial_column_file() {
        let file_io = test_file_io();
        let table_path = "memory:/test_de_write";
        setup_dirs(&file_io, table_path).await;

        let table = test_table(&file_io, table_path);
        let mut writer = DataEvolutionPartialWriter::new(&table, vec!["name".to_string()]).unwrap();

        let batch = make_partial_batch(vec!["alice", "bob", "charlie"]);
        writer
            .write_partial_batch(vec![], 0, 0, batch)
            .await
            .unwrap();

        let messages = writer.prepare_commit().await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].new_files.len(), 1);

        let meta = &messages[0].new_files[0];
        assert_eq!(meta.row_count, 3);
        assert_eq!(meta.first_row_id, Some(0));
        assert_eq!(meta.write_cols, Some(vec!["name".to_string()]));
        assert_eq!(meta.file_source, Some(0));
    }

    #[tokio::test]
    async fn test_different_first_row_id_creates_separate_files() {
        let file_io = test_file_io();
        let table_path = "memory:/test_de_write_multi";
        setup_dirs(&file_io, table_path).await;

        let table = test_table(&file_io, table_path);
        let mut writer = DataEvolutionPartialWriter::new(&table, vec!["name".to_string()]).unwrap();

        // Two batches with different first_row_id should produce two files
        let batch1 = make_partial_batch(vec!["alice", "bob"]);
        writer
            .write_partial_batch(vec![], 0, 0, batch1)
            .await
            .unwrap();

        let batch2 = make_partial_batch(vec!["charlie"]);
        writer
            .write_partial_batch(vec![], 0, 100, batch2)
            .await
            .unwrap();

        let messages = writer.prepare_commit().await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].new_files.len(), 2);

        let mut files = messages[0].new_files.clone();
        files.sort_by_key(|f| f.first_row_id);
        assert_eq!(files[0].first_row_id, Some(0));
        assert_eq!(files[0].row_count, 2);
        assert_eq!(files[1].first_row_id, Some(100));
        assert_eq!(files[1].row_count, 1);
    }

    #[test]
    fn test_find_owning_file_with_grouped_ranges() {
        // Simulate a file group: base file (3 cols, 100 rows) + partial file (1 col, 100 rows)
        // sharing the same first_row_id range [0, 99].
        let base_file = make_test_file_meta("base-0.parquet", 100, Some(0), 1, None);
        let partial_file = make_test_file_meta(
            "partial-0.parquet",
            100,
            Some(0),
            2,
            Some(vec!["name".to_string()]),
        );

        let file_index = vec![
            FileRowRange {
                first_row_id: 0,
                last_row_id: 99,
                row_count: 100,
                partition: vec![],
                bucket: 0,
                bucket_path: String::new(),
                snapshot_id: 1,
                total_buckets: 1,
                files: vec![base_file, partial_file],
            },
            FileRowRange {
                first_row_id: 100,
                last_row_id: 149,
                row_count: 50,
                partition: vec![],
                bucket: 0,
                bucket_path: String::new(),
                snapshot_id: 1,
                total_buckets: 1,
                files: vec![make_test_file_meta(
                    "base-1.parquet",
                    50,
                    Some(100),
                    1,
                    None,
                )],
            },
        ];

        // row_id 0 -> first group (2 files)
        let (pos, range) = find_owning_file(&file_index, 0).unwrap();
        assert_eq!(pos, 0);
        assert_eq!(range.files.len(), 2);

        // row_id 50 -> still first group
        let (pos, range) = find_owning_file(&file_index, 50).unwrap();
        assert_eq!(pos, 0);
        assert_eq!(range.row_count, 100);

        // row_id 99 -> last row of first group
        let (pos, _) = find_owning_file(&file_index, 99).unwrap();
        assert_eq!(pos, 0);

        // row_id 100 -> second group (1 file)
        let (pos, range) = find_owning_file(&file_index, 100).unwrap();
        assert_eq!(pos, 1);
        assert_eq!(range.files.len(), 1);

        // row_id 200 -> not found
        assert!(find_owning_file(&file_index, 200).is_none());
    }

    #[test]
    fn test_file_group_construction_from_overlapping_files() {
        // Verify that group_by_overlapping_row_id correctly groups base + partial files,
        // and that we can build FileRowRange from the result.
        let base = make_test_file_meta("base.parquet", 100, Some(0), 1, None);
        let partial1 = make_test_file_meta(
            "partial1.parquet",
            100,
            Some(0),
            2,
            Some(vec!["name".to_string()]),
        );
        let partial2 = make_test_file_meta(
            "partial2.parquet",
            100,
            Some(0),
            3,
            Some(vec!["value".to_string()]),
        );
        let separate = make_test_file_meta("separate.parquet", 50, Some(200), 1, None);

        let groups = group_by_overlapping_row_id(vec![base, partial1, partial2, separate]);

        // Should produce 2 groups: [base, partial1, partial2] and [separate]
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].len(), 3);
        assert_eq!(groups[1].len(), 1);

        // Build FileRowRange from first group
        let group = &groups[0];
        let first_row_id = group.iter().filter_map(|f| f.first_row_id).min().unwrap();
        let last_row_id = group
            .iter()
            .filter_map(|f| f.row_id_range().map(|(_, end)| end))
            .max()
            .unwrap();
        let row_count = group.iter().map(|f| f.row_count).max().unwrap();

        assert_eq!(first_row_id, 0);
        assert_eq!(last_row_id, 99);
        assert_eq!(row_count, 100);
    }

    #[tokio::test]
    async fn test_rejects_non_data_evolution_table() {
        let file_io = test_file_io();
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .build()
            .unwrap();
        let table_schema = TableSchema::new(0, &schema);
        let table = Table::new(
            file_io,
            Identifier::new("default", "test"),
            "memory:/test".to_string(),
            table_schema,
            None,
        );

        let result = DataEvolutionPartialWriter::new(&table, vec!["id".to_string()]);
        assert!(result.is_err());
    }

    #[test]
    fn test_rejects_blob_data_evolution_writer() {
        let file_io = test_file_io();
        let table = test_blob_table(&file_io, "memory:/test_blob_de_writer");

        let err = DataEvolutionWriter::new(&table, vec!["id".to_string()])
            .err()
            .unwrap();
        assert!(
            matches!(err, crate::Error::Unsupported { message } if message.contains("BlobType"))
        );
    }

    #[test]
    fn test_rejects_blob_partial_writer() {
        let file_io = test_file_io();
        let table = test_blob_table(&file_io, "memory:/test_blob_partial_writer");

        let err = DataEvolutionPartialWriter::new(&table, vec!["id".to_string()])
            .err()
            .unwrap();
        assert!(
            matches!(err, crate::Error::Unsupported { message } if message.contains("BlobType"))
        );
    }
}
