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

//! Copy-on-Write merge writer for append-only tables.
//!
//! [`CopyOnWriteMergeWriter`] rewrites entire data files when rows are updated or deleted.
//! Unaffected files remain untouched. The writer produces [`CommitMessage`]s with
//! `deleted_files` (old files) and `new_files` (rewritten files) for atomic commit.

use crate::spec::{BinaryRow, CoreOptions, DataFileMeta, PartitionComputer};
use crate::table::commit_message::CommitMessage;
use crate::table::data_file_writer::DataFileWriter;
use crate::table::partition_filter::PartitionFilter;
use crate::table::Table;
use crate::table::{DataSplitBuilder, TableScan};
use crate::Result;
use arrow_array::RecordBatch;
use arrow_select::concat::concat_batches;
use futures::TryStreamExt;
use std::collections::{HashMap, HashSet};

/// Metadata for a single data file in the target table.
pub struct FileInfo {
    pub partition: Vec<u8>,
    pub bucket: i32,
    pub bucket_path: String,
    pub snapshot_id: i64,
    pub total_buckets: i32,
    pub file_meta: DataFileMeta,
}

/// A row-level operation to apply during copy-on-write rewrite.
pub(crate) enum RowOperation {
    /// Update a row: replace columns with values from the given batch position.
    Update {
        row_offset: usize,
        batch_idx: usize,
        batch_row: usize,
    },
    /// Delete a row: skip it in the rewritten file.
    Delete { row_offset: usize },
}

/// Copy-on-Write merge writer for append-only tables (no PK, no deletion vectors).
///
/// Usage:
/// 1. Create via [`new`](Self::new) (validates preconditions and builds file index).
/// 2. Record operations via [`add_matched_update`](Self::add_matched_update)
///    and [`add_matched_delete`](Self::add_matched_delete).
/// 3. Supply update batches via [`set_update_batches`](Self::set_update_batches).
/// 4. Call [`prepare_commit`](Self::prepare_commit) to rewrite affected files.
/// 5. Commit via [`TableCommit`](super::TableCommit).
#[must_use = "writer must be used to call prepare_commit()"]
pub struct CopyOnWriteMergeWriter {
    table: Table,
    update_columns: Vec<String>,
    file_index: Vec<FileInfo>,
    affected_files: HashMap<usize, Vec<RowOperation>>,
    update_batches: Vec<RecordBatch>,
}

impl CopyOnWriteMergeWriter {
    /// Create a new CoW writer for the given table.
    ///
    /// `update_columns` lists the columns that UPDATE SET targets. Empty for DELETE-only merges.
    ///
    /// Validates:
    /// - No primary keys (append-only table)
    /// - Not a data-evolution table
    /// - No blob types
    ///
    /// `partition_set` optionally limits the scan to only the given partitions (serialized bytes).
    pub async fn new(
        table: &Table,
        update_columns: Vec<String>,
        partition_set: Option<HashSet<Vec<u8>>>,
    ) -> Result<Self> {
        let schema = table.schema();
        let core_options = CoreOptions::new(schema.options());

        if !schema.trimmed_primary_keys().is_empty() {
            return Err(crate::Error::Unsupported {
                message: "Copy-on-write MERGE INTO is only supported for append-only tables (no primary keys)".to_string(),
            });
        }

        if core_options.data_evolution_enabled() {
            return Err(crate::Error::Unsupported {
                message: "Copy-on-write MERGE INTO should not be used for data-evolution tables; use DataEvolutionWriter instead".to_string(),
            });
        }

        let partition_keys = schema.partition_keys();
        let blob_descriptor_fields = core_options.blob_descriptor_fields();
        for col in &update_columns {
            if partition_keys.contains(col) {
                return Err(crate::Error::Unsupported {
                    message: format!("Cannot update partition column '{col}' in MERGE INTO"),
                });
            }
            if let Some(field) = schema.fields().iter().find(|f| f.name() == col) {
                if field.data_type().is_blob_type() && !blob_descriptor_fields.contains(col) {
                    return Err(crate::Error::Unsupported {
                        message: format!(
                            "Cannot update raw-data BLOB column '{col}' in MERGE INTO. \
                             Only BLOB columns listed in 'blob-descriptor-field' can be updated"
                        ),
                    });
                }
            }
        }

        let partition_filter = if let Some(set) = partition_set {
            if set.is_empty() {
                None
            } else {
                let partition_fields = schema.partition_fields();
                Some(PartitionFilter::from_partition_set(set, &partition_fields)?)
            }
        } else {
            None
        };
        let scan = TableScan::new(table, partition_filter, vec![], None, None, None);
        let plan = scan.plan().await?;

        let mut file_index = Vec::new();
        for split in plan.splits() {
            let partition_bytes = split.partition().to_serialized_bytes();
            let bucket = split.bucket();
            let bucket_path = split.bucket_path().to_string();
            let snapshot_id = split.snapshot_id();
            let total_buckets = split.total_buckets();

            for file_meta in split.data_files() {
                file_index.push(FileInfo {
                    partition: partition_bytes.clone(),
                    bucket,
                    bucket_path: bucket_path.clone(),
                    snapshot_id,
                    total_buckets,
                    file_meta: file_meta.clone(),
                });
            }
        }

        Ok(Self {
            table: table.clone(),
            update_columns,
            file_index,
            affected_files: HashMap::new(),
            update_batches: Vec::new(),
        })
    }

    /// Get the file index (for the DataFusion layer to read files and attach file_idx).
    pub fn file_index(&self) -> &[FileInfo] {
        &self.file_index
    }

    /// Record an UPDATE operation for a matched row.
    pub fn add_matched_update(
        &mut self,
        file_idx: usize,
        row_offset: usize,
        batch_idx: usize,
        batch_row: usize,
    ) {
        self.affected_files
            .entry(file_idx)
            .or_default()
            .push(RowOperation::Update {
                row_offset,
                batch_idx,
                batch_row,
            });
    }

    /// Record a DELETE operation for a matched row.
    pub fn add_matched_delete(&mut self, file_idx: usize, row_offset: usize) {
        self.affected_files
            .entry(file_idx)
            .or_default()
            .push(RowOperation::Delete { row_offset });
    }

    /// Supply the update batches containing new column values.
    /// Each batch must contain the columns listed in `update_columns`.
    pub fn set_update_batches(&mut self, batches: Vec<RecordBatch>) {
        self.update_batches = batches;
    }

    /// Rewrite affected files and produce CommitMessages.
    #[must_use = "commit messages must be passed to TableCommit"]
    pub async fn prepare_commit(self) -> Result<Vec<CommitMessage>> {
        if self.affected_files.is_empty() {
            return Ok(Vec::new());
        }

        let schema = self.table.schema();
        let core_options = CoreOptions::new(schema.options());
        let partition_keys: Vec<String> = schema.partition_keys().to_vec();
        let partition_computer = PartitionComputer::new(
            &partition_keys,
            schema.fields(),
            core_options.partition_default_name(),
            core_options.legacy_partition_name(),
        )?;

        let target_file_size = core_options.target_file_size();
        let file_compression = core_options.file_compression().to_string();
        let file_compression_zstd_level = core_options.file_compression_zstd_level();
        let write_buffer_size = core_options.write_parquet_buffer_size();
        let file_format = core_options.file_format().to_string();
        let schema_id = schema.id();

        let update_columns = &self.update_columns;
        let update_batches = &self.update_batches;
        let file_index = &self.file_index;
        let table = &self.table;
        let partition_keys = &partition_keys;
        let partition_computer = &partition_computer;
        let file_compression = file_compression.as_str();
        let file_format = file_format.as_str();

        // Process each affected file in parallel
        let rewrite_futures: Vec<_> = self
            .affected_files
            .iter()
            .map(|(&file_idx, operations)| async move {
                let file_info = &file_index[file_idx];

                // Read the entire file
                let single_split = DataSplitBuilder::new()
                    .with_snapshot(file_info.snapshot_id)
                    .with_partition(BinaryRow::from_serialized_bytes(&file_info.partition)?)
                    .with_bucket(file_info.bucket)
                    .with_bucket_path(file_info.bucket_path.clone())
                    .with_total_buckets(file_info.total_buckets)
                    .with_data_files(vec![file_info.file_meta.clone()])
                    .build()?;

                let read = table.new_read_builder().new_read()?;
                let original_batches: Vec<RecordBatch> =
                    read.to_arrow(&[single_split])?.try_collect().await?;

                if original_batches.is_empty() {
                    return Ok::<_, crate::Error>(None);
                }

                let original = if original_batches.len() == 1 {
                    original_batches.into_iter().next().unwrap()
                } else {
                    concat_batches(&original_batches[0].schema(), &original_batches).map_err(
                        |e| crate::Error::DataInvalid {
                            message: format!("Failed to concat batches: {e}"),
                            source: None,
                        },
                    )?
                };

                let rewritten =
                    apply_operations(&original, operations, update_columns, update_batches)?;

                // Write the rewritten batch (may be empty if all rows deleted)
                let partition_path = if partition_keys.is_empty() {
                    String::new()
                } else {
                    let row = BinaryRow::from_serialized_bytes(&file_info.partition)?;
                    partition_computer.generate_partition_path(&row)?
                };

                let deleted_file = file_info.file_meta.clone();

                let new_files = if rewritten.num_rows() > 0 {
                    let mut writer = DataFileWriter::new(
                        table.file_io().clone(),
                        table.location().to_string(),
                        partition_path,
                        file_info.bucket,
                        schema_id,
                        target_file_size,
                        file_compression.to_string(),
                        file_compression_zstd_level,
                        write_buffer_size,
                        file_format.to_string(),
                        Some(0),
                        None,
                        None,
                    );
                    writer.write(&rewritten).await?;
                    writer.prepare_commit().await?
                } else {
                    vec![]
                };

                Ok(Some((
                    file_info.partition.clone(),
                    file_info.bucket,
                    deleted_file,
                    new_files,
                )))
            })
            .collect();

        let results = futures::future::try_join_all(rewrite_futures).await?;

        // Group by (partition, bucket) for CommitMessage grouping
        #[allow(clippy::type_complexity)]
        let mut grouped: HashMap<(Vec<u8>, i32), (Vec<DataFileMeta>, Vec<DataFileMeta>)> =
            HashMap::new();

        for result in results.into_iter().flatten() {
            let (partition, bucket, deleted_file, new_files) = result;
            let entry = grouped
                .entry((partition, bucket))
                .or_insert_with(|| (Vec::new(), Vec::new()));
            entry.0.push(deleted_file);
            entry.1.extend(new_files);
        }

        let mut messages = Vec::new();
        for ((partition, bucket), (deleted_files, new_files)) in grouped {
            let mut msg = CommitMessage::new(partition, bucket, new_files);
            msg.deleted_files = deleted_files;
            messages.push(msg);
        }
        Ok(messages)
    }
}

/// Apply row operations (update/delete) to a batch, producing a new batch.
fn apply_operations(
    original: &RecordBatch,
    operations: &[RowOperation],
    update_columns: &[String],
    update_batches: &[RecordBatch],
) -> Result<RecordBatch> {
    let num_rows = original.num_rows();
    let schema = original.schema();

    let mut delete_set = vec![false; num_rows];
    let mut update_map: HashMap<usize, (usize, usize)> = HashMap::new();

    for op in operations {
        let offset = match op {
            RowOperation::Delete { row_offset } => *row_offset,
            RowOperation::Update { row_offset, .. } => *row_offset,
        };
        if offset >= num_rows {
            return Err(crate::Error::DataInvalid {
                message: format!("row_offset {offset} is out of bounds (file has {num_rows} rows)"),
                source: None,
            });
        }

        match op {
            RowOperation::Delete { row_offset } => {
                if update_map.contains_key(row_offset) {
                    return Err(crate::Error::DataInvalid {
                        message: format!(
                            "row_offset {row_offset} has both DELETE and UPDATE operations"
                        ),
                        source: None,
                    });
                }
                delete_set[*row_offset] = true;
            }
            RowOperation::Update {
                row_offset,
                batch_idx,
                batch_row,
            } => {
                if delete_set[*row_offset] {
                    return Err(crate::Error::DataInvalid {
                        message: format!(
                            "row_offset {row_offset} has both DELETE and UPDATE operations"
                        ),
                        source: None,
                    });
                }
                if update_map.contains_key(row_offset) {
                    return Err(crate::Error::DataInvalid {
                        message: format!(
                            "row_offset {row_offset} has duplicate UPDATE operations; \
                             this may indicate a many-to-many join in the MERGE source"
                        ),
                        source: None,
                    });
                }
                update_map.insert(*row_offset, (*batch_idx, *batch_row));
            }
        }
    }

    let update_col_indices: Vec<(usize, String)> = update_columns
        .iter()
        .filter_map(|col| schema.index_of(col).ok().map(|idx| (idx, col.clone())))
        .collect();

    let surviving_indices: Vec<usize> = (0..num_rows).filter(|i| !delete_set[*i]).collect();

    if surviving_indices.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    let mut columns: Vec<arrow_array::ArrayRef> = Vec::with_capacity(schema.fields().len());

    for col_idx in 0..schema.fields().len() {
        let original_col = original.column(col_idx);

        let is_update_col = update_col_indices.iter().find(|(idx, _)| *idx == col_idx);

        if let Some((_, col_name)) = is_update_col {
            let mut builder_indices: Vec<(usize, usize)> =
                Vec::with_capacity(surviving_indices.len());
            let mut source_arrays: Vec<arrow_array::ArrayRef> = vec![original_col.clone()];

            let upd_col_idx = update_batches
                .first()
                .and_then(|b| b.schema().index_of(col_name).ok());

            if let Some(upd_idx) = upd_col_idx {
                let mut batch_source_map: HashMap<usize, usize> = HashMap::new();
                for &row in &surviving_indices {
                    if let Some(&(batch_idx, batch_row)) = update_map.get(&row) {
                        let source_idx = match batch_source_map.get(&batch_idx) {
                            Some(&idx) => idx,
                            None => {
                                let src_col = update_batches[batch_idx].column(upd_idx);
                                let casted = if src_col.data_type() != original_col.data_type() {
                                    arrow_cast::cast(src_col, original_col.data_type()).map_err(
                                        |e| crate::Error::DataInvalid {
                                            message: format!(
                                                "Failed to cast column {col_name}: {e}"
                                            ),
                                            source: None,
                                        },
                                    )?
                                } else {
                                    src_col.clone()
                                };
                                let idx = source_arrays.len();
                                source_arrays.push(casted);
                                batch_source_map.insert(batch_idx, idx);
                                idx
                            }
                        };
                        builder_indices.push((source_idx, batch_row));
                    } else {
                        builder_indices.push((0, row));
                    }
                }
            } else {
                for &row in &surviving_indices {
                    builder_indices.push((0, row));
                }
            }

            let refs: Vec<&dyn arrow_array::Array> =
                source_arrays.iter().map(|a| a.as_ref()).collect();
            let new_col =
                arrow_select::interleave::interleave(&refs, &builder_indices).map_err(|e| {
                    crate::Error::DataInvalid {
                        message: format!("Failed to interleave column {col_name}: {e}"),
                        source: None,
                    }
                })?;
            columns.push(new_col);
        } else {
            let indices = arrow_array::UInt32Array::from(
                surviving_indices
                    .iter()
                    .map(|&i| i as u32)
                    .collect::<Vec<_>>(),
            );
            let taken =
                arrow_select::take::take(original_col.as_ref(), &indices, None).map_err(|e| {
                    crate::Error::DataInvalid {
                        message: format!("Failed to take rows: {e}"),
                        source: None,
                    }
                })?;
            columns.push(taken);
        }
    }

    RecordBatch::try_new(schema, columns).map_err(|e| crate::Error::DataInvalid {
        message: format!("Failed to create rewritten batch: {e}"),
        source: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Identifier;
    use crate::io::{FileIO, FileIOBuilder};
    use crate::spec::{DataType, IntType, Schema, TableSchema, VarCharType};
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use std::sync::Arc;

    fn test_file_io() -> FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    fn test_append_schema() -> TableSchema {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("name", DataType::VarChar(VarCharType::string_type()))
            .column("value", DataType::Int(IntType::new()))
            .build()
            .unwrap();
        TableSchema::new(0, &schema)
    }

    fn test_table(file_io: &FileIO, table_path: &str) -> Table {
        Table::new(
            file_io.clone(),
            Identifier::new("default", "test_cow"),
            table_path.to_string(),
            test_append_schema(),
            None,
        )
    }

    #[tokio::test]
    async fn test_rejects_pk_table() {
        let file_io = test_file_io();
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .primary_key(["id"])
            .option("bucket", "1")
            .build()
            .unwrap();
        let table = Table::new(
            file_io,
            Identifier::new("default", "test"),
            "memory:/test".to_string(),
            TableSchema::new(0, &schema),
            None,
        );
        let err = CopyOnWriteMergeWriter::new(&table, vec!["id".to_string()], None)
            .await
            .err()
            .unwrap();
        assert!(
            matches!(err, crate::Error::Unsupported { message } if message.contains("append-only"))
        );
    }

    #[tokio::test]
    async fn test_rejects_data_evolution_table() {
        let file_io = test_file_io();
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .option("data-evolution.enabled", "true")
            .build()
            .unwrap();
        let table = Table::new(
            file_io,
            Identifier::new("default", "test"),
            "memory:/test".to_string(),
            TableSchema::new(0, &schema),
            None,
        );
        let err = CopyOnWriteMergeWriter::new(&table, vec!["id".to_string()], None)
            .await
            .err()
            .unwrap();
        assert!(
            matches!(err, crate::Error::Unsupported { message } if message.contains("data-evolution"))
        );
    }

    #[tokio::test]
    async fn test_rejects_partition_column_update() {
        let file_io = test_file_io();
        let schema = Schema::builder()
            .column("pt", DataType::VarChar(VarCharType::string_type()))
            .column("id", DataType::Int(IntType::new()))
            .partition_keys(["pt"])
            .build()
            .unwrap();
        let table = Table::new(
            file_io,
            Identifier::new("default", "test"),
            "memory:/test".to_string(),
            TableSchema::new(0, &schema),
            None,
        );
        let err = CopyOnWriteMergeWriter::new(&table, vec!["pt".to_string()], None)
            .await
            .err()
            .unwrap();
        assert!(
            matches!(err, crate::Error::Unsupported { message } if message.contains("partition column"))
        );
    }

    #[test]
    fn test_apply_operations_delete() {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("name", ArrowDataType::Utf8, true),
            ArrowField::new("value", ArrowDataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        let ops = vec![
            RowOperation::Delete { row_offset: 1 }, // delete row "b"
        ];
        let result = apply_operations(&batch, &ops, &[], &[]).unwrap();
        assert_eq!(result.num_rows(), 2);

        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.values(), &[1, 3]);
    }

    #[test]
    fn test_apply_operations_update() {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("name", ArrowDataType::Utf8, true),
            ArrowField::new("value", ArrowDataType::Int32, false),
        ]));
        let original = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        // Update batch: row 0 has new name "UPDATED"
        let upd_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "name",
            ArrowDataType::Utf8,
            true,
        )]));
        let upd_batch = RecordBatch::try_new(
            upd_schema,
            vec![Arc::new(StringArray::from(vec!["UPDATED"]))],
        )
        .unwrap();

        let update_columns = vec!["name".to_string()];
        let update_batches = vec![upd_batch];

        let ops = vec![RowOperation::Update {
            row_offset: 1, // update row "b" -> "UPDATED"
            batch_idx: 0,
            batch_row: 0,
        }];
        let result = apply_operations(&original, &ops, &update_columns, &update_batches).unwrap();
        assert_eq!(result.num_rows(), 3);

        let names = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "a");
        assert_eq!(names.value(1), "UPDATED");
        assert_eq!(names.value(2), "c");

        // Non-update columns should be unchanged
        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.values(), &[1, 2, 3]);
    }

    #[test]
    fn test_apply_operations_delete_all() {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))])
                .unwrap();

        let ops = vec![
            RowOperation::Delete { row_offset: 0 },
            RowOperation::Delete { row_offset: 1 },
        ];
        let result = apply_operations(&batch, &ops, &[], &[]).unwrap();
        assert_eq!(result.num_rows(), 0);
    }

    #[test]
    fn test_apply_operations_rejects_duplicate_update() {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("name", ArrowDataType::Utf8, true),
        ]));
        let original = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        let upd_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "name",
            ArrowDataType::Utf8,
            true,
        )]));
        let upd_batch = RecordBatch::try_new(
            upd_schema,
            vec![Arc::new(StringArray::from(vec!["X", "Y"]))],
        )
        .unwrap();

        let ops = vec![
            RowOperation::Update {
                row_offset: 0,
                batch_idx: 0,
                batch_row: 0,
            },
            RowOperation::Update {
                row_offset: 0,
                batch_idx: 0,
                batch_row: 1,
            },
        ];
        let err = apply_operations(&original, &ops, &["name".to_string()], &[upd_batch])
            .err()
            .unwrap();
        assert!(
            matches!(err, crate::Error::DataInvalid { message, .. } if message.contains("duplicate UPDATE"))
        );
    }

    #[test]
    fn test_apply_operations_rejects_out_of_bounds_offset() {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap();

        let ops = vec![RowOperation::Delete { row_offset: 5 }];
        let err = apply_operations(&batch, &ops, &[], &[]).err().unwrap();
        assert!(
            matches!(err, crate::Error::DataInvalid { message, .. } if message.contains("out of bounds"))
        );
    }

    #[tokio::test]
    async fn test_empty_affected_files_returns_empty() {
        let file_io = test_file_io();
        let table = test_table(&file_io, "memory:/test_cow_empty");
        let writer = CopyOnWriteMergeWriter::new(&table, vec![], None)
            .await
            .unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        assert!(messages.is_empty());
    }
}
