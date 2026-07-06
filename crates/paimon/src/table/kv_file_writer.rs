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

//! Key-value file writer for primary-key tables.
//!
//! Buffers data in memory, sorts by primary key on flush, and prepends
//! `_SEQUENCE_NUMBER` and `_VALUE_KIND` columns.
//!
//! Uses thin-mode (`data-file.thin-mode`): the physical file schema is
//! `[_SEQUENCE_NUMBER, _VALUE_KIND, all_user_cols...]` — primary key columns
//! are NOT duplicated. The read path extracts keys from the value portion.
//!
//! Reference: [org.apache.paimon.io.KeyValueDataFileWriterImpl](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/io/KeyValueDataFileWriterImpl.java)

use crate::arrow::format::create_format_writer;
use crate::io::FileIO;
use crate::spec::stats::{compute_column_stats, BinaryTableStats};
use crate::spec::{
    extract_datum_from_arrow, AggregationConfig, BinaryRowBuilder, DataFileMeta, DataType,
    MergeEngine, PartialUpdateConfig, RowKind, EMPTY_SERIALIZED_ROW, SEQUENCE_NUMBER_FIELD_NAME,
    VALUE_KIND_FIELD_NAME,
};
use crate::table::prepared_files::PreparedFiles;
use crate::Result;
use arrow_array::{Array, Int64Array, Int8Array, RecordBatch, UInt32Array};
use arrow_ord::sort::{lexsort_to_indices, SortColumn, SortOptions};
use arrow_row::{RowConverter, SortField};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;

/// Internal writer for primary-key tables that buffers data in memory,
/// sorts by primary key on flush, and prepends `_SEQUENCE_NUMBER` and `_VALUE_KIND` columns.
pub(crate) struct KeyValueFileWriter {
    file_io: FileIO,
    config: KeyValueWriteConfig,
    /// Next sequence number to assign (bucket-local, always auto-incremented).
    next_sequence_number: i64,
    /// Buffered batches (user schema).
    buffer: Vec<RecordBatch>,
    /// Approximate buffered bytes.
    buffer_bytes: usize,
    /// Completed file metadata.
    written_files: Vec<DataFileMeta>,
    /// Completed changelog file metadata.
    written_changelog_files: Vec<DataFileMeta>,
}

/// Configuration for [`KeyValueFileWriter`], grouping file-location, schema,
/// and key/merge parameters.
pub(crate) struct KeyValueWriteConfig {
    pub table_name: String,
    pub table_options: HashMap<String, String>,
    pub table_location: String,
    pub partition_path: String,
    pub bucket: i32,
    pub schema_id: i64,
    pub file_compression: String,
    pub file_compression_zstd_level: i32,
    pub write_buffer_size: i64,
    pub file_format: String,
    pub input_changelog: bool,
    pub changelog_file_prefix: String,
    pub changelog_file_compression: String,
    pub changelog_file_format: String,
    /// Primary key column indices in the user schema.
    pub primary_key_indices: Vec<usize>,
    /// Paimon DataTypes for each primary key column (same order as primary_key_indices).
    pub primary_key_types: Vec<DataType>,
    /// Sequence field column indices in the user schema (empty if not configured).
    pub sequence_field_indices: Vec<usize>,
    /// Merge engine for deduplication.
    pub merge_engine: MergeEngine,
    pub deletion_vectors_enabled: bool,
}

struct IndexedFileWrite<'a> {
    file_prefix: &'a str,
    file_ordinal: usize,
    file_format: &'a str,
    file_compression: &'a str,
    min_sequence_number: i64,
    max_sequence_number: i64,
    delete_row_count: i64,
}

impl KeyValueFileWriter {
    pub(crate) fn new(
        file_io: FileIO,
        config: KeyValueWriteConfig,
        next_sequence_number: i64,
    ) -> Result<Self> {
        if config.merge_engine == MergeEngine::PartialUpdate {
            PartialUpdateConfig::new(&config.table_options)
                .validate_runtime_mode(true, &config.table_name)?;

            if config.deletion_vectors_enabled {
                return Err(crate::Error::Unsupported {
                    message: format!(
                        "Table '{}' uses merge-engine=partial-update with deletion-vectors.enabled=true, which is not supported yet",
                        config.table_name
                    ),
                });
            }
        }

        if config.merge_engine == MergeEngine::Aggregation {
            AggregationConfig::new(&config.table_options)
                .validate_runtime_mode(true, &config.table_name)?;

            if config.deletion_vectors_enabled {
                return Err(crate::Error::Unsupported {
                    message: format!(
                        "Table '{}' uses merge-engine=aggregation with deletion-vectors.enabled=true, which is not supported yet",
                        config.table_name
                    ),
                });
            }
        }

        Ok(Self {
            file_io,
            config,
            next_sequence_number,
            buffer: Vec::new(),
            buffer_bytes: 0,
            written_files: Vec::new(),
            written_changelog_files: Vec::new(),
        })
    }

    /// Buffer a RecordBatch. Flushes when buffer exceeds write_buffer_size.
    /// Sequence numbers are assigned per-bucket on flush, matching Java Paimon behavior.
    pub(crate) async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let batch_bytes: usize = batch
            .columns()
            .iter()
            .map(|c| c.get_buffer_memory_size())
            .sum();
        self.buffer.push(batch.clone());
        self.buffer_bytes += batch_bytes;

        if self.buffer_bytes as i64 >= self.config.write_buffer_size {
            self.flush().await?;
        }
        Ok(())
    }

    /// Number of rows per chunk when writing sorted data to parquet.
    const FLUSH_CHUNK_ROWS: usize = 4096;

    /// Sort buffered data by primary key + sequence fields + auto-seq, deduplicate
    /// by merge engine, prepend _SEQUENCE_NUMBER/_VALUE_KIND, and write to a parquet file.
    ///
    /// Uses chunked writing: after sorting and dedup, data is materialized and written
    /// in small chunks so that only `combined`(1x) + one chunk lives in memory at a time.
    pub(crate) async fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let batches = std::mem::take(&mut self.buffer);
        self.buffer_bytes = 0;

        // Concatenate all buffered batches, then immediately free the originals.
        let user_schema = batches[0].schema();
        let combined =
            arrow_select::concat::concat_batches(&user_schema, &batches).map_err(|e| {
                crate::Error::DataInvalid {
                    message: format!("Failed to concat batches: {e}"),
                    source: None,
                }
            })?;
        drop(batches);

        let num_rows = combined.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        // Assign auto-incremented sequence numbers BEFORE sorting (arrival order).
        let start_seq = self.next_sequence_number;
        let end_seq = start_seq + num_rows as i64 - 1;
        self.next_sequence_number = end_seq + 1;
        let seq_array: Arc<dyn arrow_array::Array> =
            Arc::new(Int64Array::from((start_seq..=end_seq).collect::<Vec<_>>()));

        // Sort by: primary key columns + sequence field columns + auto-increment seq.
        let mut sort_columns: Vec<SortColumn> = Vec::new();
        for &idx in &self.config.primary_key_indices {
            sort_columns.push(SortColumn {
                values: combined.column(idx).clone(),
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
            });
        }
        for &idx in &self.config.sequence_field_indices {
            sort_columns.push(SortColumn {
                values: combined.column(idx).clone(),
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
            });
        }
        sort_columns.push(SortColumn {
            values: seq_array.clone(),
            options: Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
        });
        let sorted_indices =
            lexsort_to_indices(&sort_columns, None).map_err(|e| crate::Error::DataInvalid {
                message: format!("Failed to sort by primary key: {e}"),
                source: None,
            })?;

        // After sorting by PK + seq fields + auto-seq (all ascending), merge
        // each key group down to one row, mirroring Java's
        // MergeTreeWriter#flushWriteBuffer (the write buffer runs the merge
        // function before any file is written, so a flushed file never holds
        // two rows of one key):
        //   Deduplicate   → keep last row per key group (highest seq)
        //   FirstRow      → keep first row per key group (lowest seq)
        //   PartialUpdate → per column, keep the latest non-null value
        //   Aggregation   → keep all rows for read-side field-wise merge
        let (data_batch, data_seq, data_indices) = match self.config.merge_engine {
            MergeEngine::PartialUpdate => {
                let (merged, merged_seq) =
                    self.merge_partial_update_rows(&combined, seq_array.as_ref(), &sorted_indices)?;
                let identity =
                    UInt32Array::from_iter_values(0..u32::try_from(merged.num_rows()).unwrap());
                (merged, merged_seq, identity)
            }
            MergeEngine::Deduplicate | MergeEngine::FirstRow | MergeEngine::Aggregation => {
                let selected = self.select_flush_indices(&combined, &sorted_indices)?;
                (
                    combined.clone(),
                    seq_array.clone(),
                    UInt32Array::from(selected),
                )
            }
        };

        let data_delete_row_count = Self::indexed_delete_row_count(&data_batch, &data_indices)?;
        let changelog_delete_row_count = if self.config.input_changelog {
            Some(Self::indexed_delete_row_count(&combined, &sorted_indices)?)
        } else {
            None
        };

        let data_file = self
            .write_indexed_file(
                &data_batch,
                data_seq.as_ref(),
                &data_indices,
                IndexedFileWrite {
                    file_prefix: "data-",
                    file_ordinal: self.written_files.len(),
                    file_format: &self.config.file_format,
                    file_compression: &self.config.file_compression,
                    min_sequence_number: start_seq,
                    max_sequence_number: end_seq,
                    delete_row_count: data_delete_row_count,
                },
            )
            .await?;
        self.written_files.push(data_file);

        if let Some(delete_row_count) = changelog_delete_row_count {
            let changelog_file = self
                .write_indexed_file(
                    &combined,
                    seq_array.as_ref(),
                    &sorted_indices,
                    IndexedFileWrite {
                        file_prefix: &self.config.changelog_file_prefix,
                        file_ordinal: self.written_changelog_files.len(),
                        file_format: &self.config.changelog_file_format,
                        file_compression: &self.config.changelog_file_compression,
                        min_sequence_number: start_seq,
                        max_sequence_number: end_seq,
                        delete_row_count,
                    },
                )
                .await?;
            self.written_changelog_files.push(changelog_file);
        }
        Ok(())
    }

    async fn write_indexed_file(
        &self,
        batch: &RecordBatch,
        seq_array: &dyn Array,
        indices: &UInt32Array,
        write: IndexedFileWrite<'_>,
    ) -> Result<DataFileMeta> {
        if indices.is_empty() {
            return Err(crate::Error::DataInvalid {
                message: "Cannot write an empty key-value data file".to_string(),
                source: None,
            });
        }

        let user_schema = batch.schema();
        let first_row = indices.value(0) as usize;
        let last_row = indices.value(indices.len() - 1) as usize;
        let min_key = self.extract_key_binary_row(batch, first_row)?;
        let max_key = self.extract_key_binary_row(batch, last_row)?;

        let physical_schema = build_physical_schema(&user_schema);
        let file_name = format!(
            "{}{}-{}.{}",
            write.file_prefix,
            uuid::Uuid::new_v4(),
            write.file_ordinal,
            write.file_format,
        );
        let bucket_dir = if self.config.partition_path.is_empty() {
            format!(
                "{}/bucket-{}",
                self.config.table_location, self.config.bucket
            )
        } else {
            format!(
                "{}/{}/bucket-{}",
                self.config.table_location, self.config.partition_path, self.config.bucket
            )
        };
        self.file_io.mkdirs(&format!("{bucket_dir}/")).await?;
        let file_path = format!("{bucket_dir}/{file_name}");
        let output = self.file_io.new_output(&file_path)?;
        let mut writer = create_format_writer(
            &output,
            physical_schema.clone(),
            write.file_compression,
            self.config.file_compression_zstd_level,
            None,
            None,
            None,
        )
        .await?;

        let vk_idx = batch
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == crate::spec::VALUE_KIND_FIELD_NAME);

        for chunk_start in (0..indices.len()).step_by(Self::FLUSH_CHUNK_ROWS) {
            let chunk_len = Self::FLUSH_CHUNK_ROWS.min(indices.len() - chunk_start);
            let chunk_indices = indices.slice(chunk_start, chunk_len);

            let mut physical_columns: Vec<Arc<dyn Array>> = Vec::new();
            physical_columns.push(
                arrow_select::take::take(seq_array, &chunk_indices, None).map_err(|e| {
                    crate::Error::DataInvalid {
                        message: format!("Failed to reorder sequence numbers: {e}"),
                        source: None,
                    }
                })?,
            );

            match vk_idx {
                Some(vk_idx) => {
                    physical_columns.push(
                        arrow_select::take::take(
                            batch.column(vk_idx).as_ref(),
                            &chunk_indices,
                            None,
                        )
                        .map_err(|e| crate::Error::DataInvalid {
                            message: format!("Failed to reorder value kind column: {e}"),
                            source: None,
                        })?,
                    );
                }
                None => {
                    physical_columns.push(Arc::new(Int8Array::from(vec![0i8; chunk_len])));
                }
            }

            for idx in 0..batch.num_columns() {
                if Some(idx) == vk_idx {
                    continue;
                }
                physical_columns.push(
                    arrow_select::take::take(batch.column(idx).as_ref(), &chunk_indices, None)
                        .map_err(|e| crate::Error::DataInvalid {
                            message: format!("Failed to reorder by sort indices: {e}"),
                            source: None,
                        })?,
                );
            }

            let chunk_batch = RecordBatch::try_new(physical_schema.clone(), physical_columns)
                .map_err(|e| crate::Error::DataInvalid {
                    message: format!("Failed to create physical batch: {e}"),
                    source: None,
                })?;
            writer.write(&chunk_batch).await?;
        }

        let file_size = writer.close().await? as i64;

        let key_columns: Vec<Arc<dyn Array>> = self
            .config
            .primary_key_indices
            .iter()
            .map(|&idx| {
                arrow_select::take::take(batch.column(idx).as_ref(), indices, None).map_err(|e| {
                    crate::Error::DataInvalid {
                        message: format!("Failed to take key column for stats: {e}"),
                        source: None,
                    }
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let key_batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(
                self.config
                    .primary_key_indices
                    .iter()
                    .map(|&idx| user_schema.field(idx).clone())
                    .collect::<Vec<_>>(),
            )),
            key_columns,
        )
        .map_err(|e| crate::Error::DataInvalid {
            message: format!("Failed to build key batch for stats: {e}"),
            source: None,
        })?;
        let stats_col_indices: Vec<usize> = (0..self.config.primary_key_indices.len()).collect();
        let key_stats = compute_column_stats(
            &key_batch,
            &stats_col_indices,
            &self.config.primary_key_types,
        )?;

        Ok(DataFileMeta {
            file_name,
            file_size,
            row_count: indices.len() as i64,
            min_key,
            max_key,
            key_stats,
            value_stats: BinaryTableStats::new(
                EMPTY_SERIALIZED_ROW.clone(),
                EMPTY_SERIALIZED_ROW.clone(),
                vec![],
            ),
            min_sequence_number: write.min_sequence_number,
            max_sequence_number: write.max_sequence_number,
            schema_id: self.config.schema_id,
            level: 0,
            extra_files: vec![],
            creation_time: Some(Utc::now()),
            delete_row_count: Some(write.delete_row_count),
            embedded_index: None,
            file_source: Some(0), // FileSource.APPEND
            value_stats_cols: Some(vec![]),
            external_path: None,
            first_row_id: None,
            write_cols: None,
        })
    }

    fn indexed_delete_row_count(batch: &RecordBatch, indices: &UInt32Array) -> Result<i64> {
        let Some(vk_idx) = batch
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == crate::spec::VALUE_KIND_FIELD_NAME)
        else {
            return Ok(0);
        };

        let column = batch.column(vk_idx);
        let Some(value_kinds) = column.as_any().downcast_ref::<Int8Array>() else {
            return Err(crate::Error::DataInvalid {
                message: "_VALUE_KIND column must be Int8".to_string(),
                source: None,
            });
        };

        let mut delete_count = 0;
        for idx in 0..indices.len() {
            let row = indices.value(idx) as usize;
            let value = if column.is_null(row) {
                0
            } else {
                value_kinds.value(row)
            };
            match RowKind::from_value(value)? {
                RowKind::UpdateBefore | RowKind::Delete => delete_count += 1,
                RowKind::Insert | RowKind::UpdateAfter => {}
            }
        }
        Ok(delete_count)
    }

    /// Select output row indices from sorted inputs according to merge engine.
    ///
    /// Input: `sorted_indices` ordered by PK + seq fields + auto-seq (all ascending).
    /// Output: row indices to write in sorted PK order.
    fn select_flush_indices(
        &self,
        batch: &RecordBatch,
        sorted_indices: &arrow_array::UInt32Array,
    ) -> Result<Vec<u32>> {
        match self.config.merge_engine {
            MergeEngine::Deduplicate | MergeEngine::FirstRow => {
                self.dedup_sorted_indices(batch, sorted_indices)
            }
            MergeEngine::PartialUpdate => {
                unreachable!("partial-update merges rows at flush via merge_partial_update_rows")
            }
            // Aggregation keeps every row on flush and performs the per-field
            // merge on the read side.
            MergeEngine::Aggregation => Ok((0..sorted_indices.len())
                .map(|idx| sorted_indices.value(idx))
                .collect()),
        }
    }

    /// Merge same-key rows at flush for the partial-update engine, mirroring
    /// Java `MergeTreeWriter#flushWriteBuffer` (the write buffer applies the
    /// merge function before any file is written) with the same semantics as
    /// the read-side `PartialUpdateMergeFunction`: rows are visited in
    /// ascending (sequence fields, auto-seq) order and every column keeps its
    /// latest non-null value; a column that is null in every row stays null.
    /// DELETE / UPDATE_BEFORE rows are rejected, matching the read side.
    ///
    /// Returns the merged batch (user schema, in primary-key order) and its
    /// `_SEQUENCE_NUMBER` column; each merged row keeps the highest sequence
    /// number of its key group, so cross-file merge ordering is preserved.
    fn merge_partial_update_rows(
        &self,
        batch: &RecordBatch,
        seq_array: &dyn Array,
        sorted_indices: &arrow_array::UInt32Array,
    ) -> Result<(RecordBatch, Arc<dyn Array>)> {
        // Reject retract rows up front, mirroring the read-side error.
        let vk_idx = batch
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == crate::spec::VALUE_KIND_FIELD_NAME);
        if let Some(vk_idx) = vk_idx {
            let kinds = batch
                .column(vk_idx)
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: "_VALUE_KIND column must be Int8".to_string(),
                    source: None,
                })?;
            for row in 0..kinds.len() {
                if !RowKind::from_value(kinds.value(row))?.is_add() {
                    return Err(crate::Error::Unsupported {
                        message: "merge-engine=partial-update basic mode does not support DELETE or UPDATE_BEFORE rows".to_string(),
                    });
                }
            }
        }

        let key_rows = self.convert_key_rows(batch)?;

        let n = sorted_indices.len();
        let num_cols = batch.num_columns();
        // Per output column: the source row chosen for each key group.
        let mut col_indices: Vec<Vec<u32>> = vec![Vec::new(); num_cols];
        // Per key group: the last (highest-sequence) source row, for `_SEQUENCE_NUMBER`.
        let mut last_indices: Vec<u32> = Vec::new();

        let mut group_start = 0;
        while group_start < n {
            let mut group_end = group_start + 1;
            let first = sorted_indices.value(group_start) as usize;
            while group_end < n
                && key_rows.row(sorted_indices.value(group_end) as usize) == key_rows.row(first)
            {
                group_end += 1;
            }

            let last = sorted_indices.value(group_end - 1);
            last_indices.push(last);
            for (col_idx, chosen_per_group) in col_indices.iter_mut().enumerate() {
                let column = batch.column(col_idx);
                // Latest non-null wins; an all-null group keeps the (null)
                // value of the last row.
                let mut chosen = last;
                for pos in (group_start..group_end).rev() {
                    let row = sorted_indices.value(pos);
                    if column.is_valid(row as usize) {
                        chosen = row;
                        break;
                    }
                }
                chosen_per_group.push(chosen);
            }

            group_start = group_end;
        }

        let merged_columns: Vec<Arc<dyn Array>> = col_indices
            .iter()
            .enumerate()
            .map(|(col_idx, indices)| {
                arrow_select::take::take(
                    batch.column(col_idx).as_ref(),
                    &UInt32Array::from(indices.clone()),
                    None,
                )
                .map_err(|e| crate::Error::DataInvalid {
                    message: format!("Failed to take merged partial-update column: {e}"),
                    source: None,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let merged = RecordBatch::try_new(batch.schema(), merged_columns).map_err(|e| {
            crate::Error::DataInvalid {
                message: format!("Failed to build merged partial-update batch: {e}"),
                source: None,
            }
        })?;
        let merged_seq =
            arrow_select::take::take(seq_array, &UInt32Array::from(last_indices), None).map_err(
                |e| crate::Error::DataInvalid {
                    message: format!("Failed to take merged sequence numbers: {e}"),
                    source: None,
                },
            )?;
        Ok((merged, merged_seq))
    }

    /// Convert the primary-key columns into arrow-row `Rows` so same-key rows
    /// can be compared cheaply.
    fn convert_key_rows(&self, batch: &RecordBatch) -> Result<arrow_row::Rows> {
        let sort_fields: Vec<SortField> = self
            .config
            .primary_key_indices
            .iter()
            .map(|&idx| SortField::new(batch.schema().field(idx).data_type().clone()))
            .collect();
        let converter =
            RowConverter::new(sort_fields).map_err(|e| crate::Error::UnexpectedError {
                message: format!("Failed to create RowConverter for key grouping: {e}"),
                source: Some(Box::new(e)),
            })?;
        let key_columns: Vec<Arc<dyn arrow_array::Array>> = self
            .config
            .primary_key_indices
            .iter()
            .map(|&idx| batch.column(idx).clone())
            .collect();
        converter
            .convert_columns(&key_columns)
            .map_err(|e| crate::Error::UnexpectedError {
                message: format!("Failed to convert key columns for key grouping: {e}"),
                source: Some(Box::new(e)),
            })
    }

    /// Deduplicate sorted indices by primary key for Deduplicate / FirstRow engines.
    ///
    /// Input: `sorted_indices` ordered by PK + seq fields + auto-seq (all ascending).
    /// Output: a Vec<u32> of original row indices to keep, in sorted PK order.
    fn dedup_sorted_indices(
        &self,
        batch: &RecordBatch,
        sorted_indices: &arrow_array::UInt32Array,
    ) -> Result<Vec<u32>> {
        let n = sorted_indices.len();
        if n == 0 {
            return Ok(vec![]);
        }

        let rows = self.convert_key_rows(batch)?;

        let mut result: Vec<u32> = Vec::with_capacity(n);
        // Track the start of the current key group and the candidate winner.
        let mut group_winner = sorted_indices.value(0);

        for i in 1..n {
            let cur = sorted_indices.value(i);
            if rows.row(group_winner as usize) == rows.row(cur as usize) {
                // Same key group — update winner based on merge engine.
                match self.config.merge_engine {
                    // Deduplicate: keep last (highest seq), which is the current row
                    // since we sorted ascending.
                    MergeEngine::Deduplicate => group_winner = cur,
                    // FirstRow: keep first (lowest seq), so don't update.
                    MergeEngine::FirstRow => {}
                    MergeEngine::PartialUpdate | MergeEngine::Aggregation => unreachable!(
                        "{:?} should use select_flush_indices and skip dedup",
                        self.config.merge_engine
                    ),
                }
            } else {
                // New key group — emit the winner of the previous group.
                result.push(group_winner);
                group_winner = cur;
            }
        }
        // Emit the last group's winner.
        result.push(group_winner);
        Ok(result)
    }

    /// Flush remaining buffer and return all written file metadata.
    pub(crate) async fn prepare_commit(&mut self) -> Result<PreparedFiles> {
        self.flush().await?;
        Ok(PreparedFiles {
            data_files: std::mem::take(&mut self.written_files),
            changelog_files: std::mem::take(&mut self.written_changelog_files),
        })
    }

    /// Extract primary key columns from a batch at a given row index into a serialized BinaryRow.
    fn extract_key_binary_row(&self, batch: &RecordBatch, row_idx: usize) -> Result<Vec<u8>> {
        let num_keys = self.config.primary_key_indices.len();
        let mut builder = BinaryRowBuilder::new(num_keys as i32);
        for (pos, (&col_idx, data_type)) in self
            .config
            .primary_key_indices
            .iter()
            .zip(self.config.primary_key_types.iter())
            .enumerate()
        {
            match extract_datum_from_arrow(batch, row_idx, col_idx, data_type)? {
                Some(datum) => builder.write_datum(pos, &datum, data_type),
                None => builder.set_null_at(pos),
            }
        }
        Ok(builder.build_serialized())
    }
}

/// Build the physical schema: [_SEQUENCE_NUMBER, _VALUE_KIND, user_cols (excluding _VALUE_KIND)...]
pub(crate) fn build_physical_schema(user_schema: &ArrowSchema) -> Arc<ArrowSchema> {
    let mut physical_fields: Vec<Arc<ArrowField>> = Vec::new();
    physical_fields.push(Arc::new(ArrowField::new(
        SEQUENCE_NUMBER_FIELD_NAME,
        ArrowDataType::Int64,
        false,
    )));
    physical_fields.push(Arc::new(ArrowField::new(
        VALUE_KIND_FIELD_NAME,
        ArrowDataType::Int8,
        false,
    )));
    for field in user_schema.fields().iter() {
        if field.name() != VALUE_KIND_FIELD_NAME {
            physical_fields.push(field.clone());
        }
    }
    Arc::new(ArrowSchema::new(physical_fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::IntType;
    use arrow_array::{Int32Array, UInt32Array};
    use std::collections::HashMap;

    fn test_write_config(merge_engine: MergeEngine) -> KeyValueWriteConfig {
        let mut table_options = HashMap::new();
        match merge_engine {
            MergeEngine::PartialUpdate => {
                table_options.insert("merge-engine".to_string(), "partial-update".to_string());
            }
            MergeEngine::Aggregation => {
                table_options.insert("merge-engine".to_string(), "aggregation".to_string());
            }
            MergeEngine::Deduplicate | MergeEngine::FirstRow => {}
        }

        KeyValueWriteConfig {
            table_name: "default.test_table".to_string(),
            table_options,
            table_location: "memory:/kv-test".to_string(),
            partition_path: String::new(),
            bucket: 0,
            schema_id: 0,
            file_compression: "none".to_string(),
            file_compression_zstd_level: 0,
            write_buffer_size: 1024,
            file_format: "parquet".to_string(),
            input_changelog: false,
            changelog_file_prefix: "changelog-".to_string(),
            changelog_file_compression: "none".to_string(),
            changelog_file_format: "parquet".to_string(),
            primary_key_indices: vec![0],
            primary_key_types: vec![DataType::Int(IntType::new())],
            sequence_field_indices: vec![1],
            merge_engine,
            deletion_vectors_enabled: false,
        }
    }

    fn first_row_writer() -> KeyValueFileWriter {
        KeyValueFileWriter::new(
            FileIOBuilder::new("memory").build().unwrap(),
            test_write_config(MergeEngine::FirstRow),
            0,
        )
        .unwrap()
    }

    #[test]
    fn test_dedup_sorted_indices_keeps_first_row_for_first_row_engine() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Arc::new(ArrowField::new("id", ArrowDataType::Int32, false)),
            Arc::new(ArrowField::new("seq", ArrowDataType::Int64, false)),
            Arc::new(ArrowField::new("value", ArrowDataType::Int32, false)),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 2, 2])) as Arc<dyn arrow_array::Array>,
                Arc::new(Int64Array::from(vec![10, 20, 5, 6])) as Arc<dyn arrow_array::Array>,
                Arc::new(Int32Array::from(vec![100, 200, 300, 400])) as Arc<dyn arrow_array::Array>,
            ],
        )
        .unwrap();
        let sorted_indices = UInt32Array::from(vec![0, 1, 2, 3]);

        let deduped = first_row_writer()
            .dedup_sorted_indices(&batch, &sorted_indices)
            .unwrap();

        assert_eq!(deduped, vec![0, 2]);
    }

    fn partial_update_writer() -> KeyValueFileWriter {
        KeyValueFileWriter::new(
            FileIOBuilder::new("memory").build().unwrap(),
            test_write_config(MergeEngine::PartialUpdate),
            0,
        )
        .unwrap()
    }

    /// Partial-update merges each key group down to one row at flush: every
    /// column keeps its latest non-null value (different columns may come
    /// from different source rows) and the merged row carries the group's
    /// highest sequence number.
    #[test]
    fn test_merge_partial_update_rows_latest_non_null_per_column() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Arc::new(ArrowField::new("id", ArrowDataType::Int32, false)),
            Arc::new(ArrowField::new("seq", ArrowDataType::Int64, false)),
            Arc::new(ArrowField::new("v1", ArrowDataType::Int32, true)),
            Arc::new(ArrowField::new("v2", ArrowDataType::Int32, true)),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 1, 2])) as Arc<dyn arrow_array::Array>,
                Arc::new(Int64Array::from(vec![10, 20, 30, 5])) as Arc<dyn arrow_array::Array>,
                Arc::new(Int32Array::from(vec![Some(100), None, None, Some(9)]))
                    as Arc<dyn arrow_array::Array>,
                Arc::new(Int32Array::from(vec![None, Some(200), None, None]))
                    as Arc<dyn arrow_array::Array>,
            ],
        )
        .unwrap();
        let sorted_indices = UInt32Array::from(vec![0, 1, 2, 3]);
        let seq_array = Int64Array::from(vec![1000, 1001, 1002, 1003]);

        let (merged, merged_seq) = partial_update_writer()
            .merge_partial_update_rows(&batch, &seq_array, &sorted_indices)
            .unwrap();

        assert_eq!(merged.num_rows(), 2);
        let ids = merged
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let seqs = merged
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let v1 = merged
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let v2 = merged
            .column(3)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        // Key 1: v1 from the first row (only non-null), v2 from the second,
        // user seq column from the third (latest non-null).
        assert_eq!((ids.value(0), seqs.value(0)), (1, 30));
        assert_eq!((v1.value(0), v2.value(0)), (100, 200));
        // v2 of key 2 is null in every row and stays null.
        assert_eq!((ids.value(1), v1.value(1)), (2, 9));
        assert!(v2.is_null(1));

        // The merged _SEQUENCE_NUMBER is the highest of each group.
        let merged_seq = merged_seq
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(merged_seq, vec![1002, 1003]);
    }

    /// Lock the flush-time merge to the read-side `PartialUpdateMergeFunction`.
    ///
    /// Java uses one `MergeFunction` for write flush, compaction, and reads,
    /// so engine semantics have a single source of truth. The Rust write side
    /// is a vectorized re-implementation (per-column take) of the read side's
    /// streaming merge; this test feeds the same key groups through both and
    /// asserts identical output, so the two implementations cannot drift.
    #[test]
    fn test_flush_merge_matches_read_side_partial_update_merge() {
        use crate::table::sort_merge::{
            BufferedBatch, MergeFunction, MergeResult, MergeRow, PartialUpdateMergeFunction,
        };
        use arrow_array::StringArray;

        // Arrival order; auto-seq = 1000 + row index. The `seq` column is the
        // user sequence field (test_write_config: sequence_field_indices=[1]).
        //
        // Key 1 ordering by (user seq, auto-seq): r2(10) < r0(20,@1000) < r3(20,@1003)
        //   v1: latest non-null = r3 (7); v2: latest non-null = r0 ("b").
        // Key 2 ordering: r1(5,@1001) < r4(5,@1004)
        //   v1: latest non-null = r1 (9); v2: null in every row.
        let schema = Arc::new(ArrowSchema::new(vec![
            Arc::new(ArrowField::new("id", ArrowDataType::Int32, false)),
            Arc::new(ArrowField::new("seq", ArrowDataType::Int64, false)),
            Arc::new(ArrowField::new("v1", ArrowDataType::Int32, true)),
            Arc::new(ArrowField::new("v2", ArrowDataType::Utf8, true)),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 1, 1, 2])) as Arc<dyn arrow_array::Array>,
                Arc::new(Int64Array::from(vec![20, 5, 10, 20, 5])) as Arc<dyn arrow_array::Array>,
                Arc::new(Int32Array::from(vec![
                    None,
                    Some(9),
                    Some(100),
                    Some(7),
                    None,
                ])) as Arc<dyn arrow_array::Array>,
                Arc::new(StringArray::from(vec![
                    Some("b"),
                    None,
                    Some("a"),
                    None,
                    None,
                ])) as Arc<dyn arrow_array::Array>,
            ],
        )
        .unwrap();
        let seq_values: Vec<i64> = (1000..1005).collect();
        let seq_array = Int64Array::from(seq_values.clone());

        // Write side: replicate the flush sort (PK + sequence field + auto-seq).
        let sort_columns = vec![
            SortColumn {
                values: batch.column(0).clone(),
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
            },
            SortColumn {
                values: batch.column(1).clone(),
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
            },
            SortColumn {
                values: Arc::new(seq_array.clone()),
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
            },
        ];
        let sorted_indices = lexsort_to_indices(&sort_columns, None).unwrap();
        let (merged, merged_seq) = partial_update_writer()
            .merge_partial_update_rows(&batch, &seq_array, &sorted_indices)
            .unwrap();
        assert_eq!(merged.num_rows(), 2, "two keys, one merged row each");
        assert_eq!(
            merged_seq
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec(),
            vec![1003, 1004],
            "merged rows carry each group's highest sequence number"
        );

        // Read side: feed the same key groups (in arrival order — the merge
        // function orders rows itself) through PartialUpdateMergeFunction.
        let table_options =
            HashMap::from([("merge-engine".to_string(), "partial-update".to_string())]);
        let merge_fn =
            PartialUpdateMergeFunction::new(&table_options, "default.test_table").unwrap();
        let buffer = [BufferedBatch::Source(batch.clone())];
        let identity: Vec<usize> = (0..batch.num_columns()).collect();
        let seq_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        for (group_idx, group_rows) in [vec![0usize, 2, 3], vec![1usize, 4]].iter().enumerate() {
            let rows: Vec<MergeRow> = group_rows
                .iter()
                .map(|&row_idx| MergeRow {
                    batch_idx: 0,
                    row_idx,
                    sequence_number: seq_values[row_idx],
                    value_kind: 0,
                    user_sequences: vec![Some(seq_col.value(row_idx) as i128)],
                })
                .collect();
            let result = merge_fn.merge(&rows, &buffer, &identity, &schema).unwrap();
            let MergeResult::MaterializedRow(read_row) = result else {
                panic!("partial-update merge must materialize a row");
            };
            assert_eq!(
                merged.slice(group_idx, 1),
                read_row,
                "flush merge and read-side merge must agree for group {group_idx}"
            );
        }
    }

    /// Retract rows are rejected at flush, matching the read-side
    /// PartialUpdateMergeFunction error.
    #[test]
    fn test_merge_partial_update_rows_rejects_retract() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Arc::new(ArrowField::new("id", ArrowDataType::Int32, false)),
            Arc::new(ArrowField::new("seq", ArrowDataType::Int64, false)),
            Arc::new(ArrowField::new(
                VALUE_KIND_FIELD_NAME,
                ArrowDataType::Int8,
                false,
            )),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1])) as Arc<dyn arrow_array::Array>,
                Arc::new(Int64Array::from(vec![10])) as Arc<dyn arrow_array::Array>,
                // RowKind::Delete
                Arc::new(Int8Array::from(vec![3])) as Arc<dyn arrow_array::Array>,
            ],
        )
        .unwrap();
        let sorted_indices = UInt32Array::from(vec![0]);
        let seq_array = Int64Array::from(vec![1000]);

        let err = partial_update_writer()
            .merge_partial_update_rows(&batch, &seq_array, &sorted_indices)
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::Unsupported { ref message }
                if message.contains("does not support DELETE or UPDATE_BEFORE")),
            "got {err:?}"
        );
    }

    #[test]
    fn test_indexed_delete_row_count_rejects_invalid_value_kind() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Arc::new(ArrowField::new("id", ArrowDataType::Int32, false)),
            Arc::new(ArrowField::new(
                VALUE_KIND_FIELD_NAME,
                ArrowDataType::Int8,
                false,
            )),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1])) as Arc<dyn arrow_array::Array>,
                Arc::new(Int8Array::from(vec![4])) as Arc<dyn arrow_array::Array>,
            ],
        )
        .unwrap();
        let indices = UInt32Array::from(vec![0]);

        let err = KeyValueFileWriter::indexed_delete_row_count(&batch, &indices).unwrap_err();

        assert!(
            matches!(err, crate::Error::DataInvalid { message, .. } if message.contains("Invalid RowKind value"))
        );
    }

    #[test]
    fn test_new_rejects_partial_update_with_deletion_vectors() {
        let mut config = test_write_config(MergeEngine::PartialUpdate);
        config.deletion_vectors_enabled = true;

        let err = KeyValueFileWriter::new(FileIOBuilder::new("memory").build().unwrap(), config, 0)
            .err()
            .unwrap();

        assert!(matches!(
            err,
            crate::Error::Unsupported { message }
            if message.contains("deletion-vectors.enabled=true")
        ));
    }

    #[test]
    fn test_new_rejects_unsupported_partial_update_options() {
        let mut config = test_write_config(MergeEngine::PartialUpdate);
        config.table_options = HashMap::from([
            ("merge-engine".to_string(), "partial-update".to_string()),
            (
                "fields.price.aggregate-function".to_string(),
                "last_non_null".to_string(),
            ),
        ]);

        let err = KeyValueFileWriter::new(FileIOBuilder::new("memory").build().unwrap(), config, 0)
            .err()
            .unwrap();

        assert!(matches!(
            err,
            crate::Error::Unsupported { message }
            if message.contains("fields.price.aggregate-function")
        ));
    }

    #[test]
    fn test_select_flush_indices_keeps_all_rows_for_aggregation_engine() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Arc::new(ArrowField::new("id", ArrowDataType::Int32, false)),
            Arc::new(ArrowField::new("seq", ArrowDataType::Int64, false)),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 1])) as Arc<dyn arrow_array::Array>,
                Arc::new(Int64Array::from(vec![10, 20])) as Arc<dyn arrow_array::Array>,
            ],
        )
        .unwrap();
        let sorted_indices = UInt32Array::from(vec![0, 1]);
        let writer = KeyValueFileWriter::new(
            FileIOBuilder::new("memory").build().unwrap(),
            test_write_config(MergeEngine::Aggregation),
            0,
        )
        .unwrap();

        let selected = writer
            .select_flush_indices(&batch, &sorted_indices)
            .unwrap();

        assert_eq!(selected, vec![0, 1]);
    }

    #[test]
    fn test_new_rejects_aggregation_with_deletion_vectors() {
        let mut config = test_write_config(MergeEngine::Aggregation);
        config.deletion_vectors_enabled = true;

        let err = KeyValueFileWriter::new(FileIOBuilder::new("memory").build().unwrap(), config, 0)
            .err()
            .unwrap();

        assert!(matches!(
            err,
            crate::Error::Unsupported { message }
            if message.contains("deletion-vectors.enabled=true")
        ));
    }

    #[test]
    fn test_new_rejects_unsupported_aggregation_options() {
        let mut config = test_write_config(MergeEngine::Aggregation);
        config.table_options.insert(
            "fields.price.ignore-retract".to_string(),
            "true".to_string(),
        );

        let err = KeyValueFileWriter::new(FileIOBuilder::new("memory").build().unwrap(), config, 0)
            .err()
            .unwrap();

        assert!(matches!(
            err,
            crate::Error::Unsupported { message }
            if message.contains("fields.price.ignore-retract")
        ));
    }
}
