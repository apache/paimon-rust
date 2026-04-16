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
    extract_datum_from_arrow, BinaryRowBuilder, DataFileMeta, DataType, MergeEngine,
    EMPTY_SERIALIZED_ROW, SEQUENCE_NUMBER_FIELD_NAME, VALUE_KIND_FIELD_NAME,
};
use crate::Result;
use arrow_array::{Int64Array, Int8Array, RecordBatch};
use arrow_ord::sort::{lexsort_to_indices, SortColumn, SortOptions};
use arrow_row::{RowConverter, SortField};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use chrono::Utc;
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
}

/// Configuration for [`KeyValueFileWriter`], grouping file-location, schema,
/// and key/merge parameters.
pub(crate) struct KeyValueWriteConfig {
    pub table_location: String,
    pub partition_path: String,
    pub bucket: i32,
    pub schema_id: i64,
    pub file_compression: String,
    pub file_compression_zstd_level: i32,
    pub write_buffer_size: i64,
    /// Primary key column indices in the user schema.
    pub primary_key_indices: Vec<usize>,
    /// Paimon DataTypes for each primary key column (same order as primary_key_indices).
    pub primary_key_types: Vec<DataType>,
    /// Sequence field column indices in the user schema (empty if not configured).
    pub sequence_field_indices: Vec<usize>,
    /// Merge engine for deduplication.
    pub merge_engine: MergeEngine,
}

impl KeyValueFileWriter {
    pub(crate) fn new(
        file_io: FileIO,
        config: KeyValueWriteConfig,
        next_sequence_number: i64,
    ) -> Self {
        Self {
            file_io,
            config,
            next_sequence_number,
            buffer: Vec::new(),
            buffer_bytes: 0,
            written_files: Vec::new(),
        }
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

        // Deduplicate: for consecutive rows with the same PK, pick the winner.
        // After sorting by PK + seq fields + auto-seq (all ascending):
        //   Deduplicate → keep last row per key group (highest seq)
        //   FirstRow    → keep first row per key group (lowest seq)
        let deduped_indices = self.dedup_sorted_indices(&combined, &sorted_indices)?;
        let deduped_num_rows = deduped_indices.len();

        // Extract min_key / max_key from deduped endpoints.
        let first_row = deduped_indices[0] as usize;
        let last_row = deduped_indices[deduped_num_rows - 1] as usize;
        let min_key = self.extract_key_binary_row(&combined, first_row)?;
        let max_key = self.extract_key_binary_row(&combined, last_row)?;

        // Build physical schema and open writer.
        let physical_schema = build_physical_schema(&user_schema);

        // Open parquet writer.
        let file_name = format!(
            "data-{}-{}.parquet",
            uuid::Uuid::new_v4(),
            self.written_files.len()
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
        let file_path = format!("{}/{}", bucket_dir, file_name);
        let output = self.file_io.new_output(&file_path)?;
        let mut writer = create_format_writer(
            &output,
            physical_schema.clone(),
            &self.config.file_compression,
            self.config.file_compression_zstd_level,
        )
        .await?;

        // Chunked write using deduped indices.
        let deduped_u32 = arrow_array::UInt32Array::from(deduped_indices);
        for chunk_start in (0..deduped_num_rows).step_by(Self::FLUSH_CHUNK_ROWS) {
            let chunk_len = Self::FLUSH_CHUNK_ROWS.min(deduped_num_rows - chunk_start);
            let chunk_indices = deduped_u32.slice(chunk_start, chunk_len);

            let mut physical_columns: Vec<Arc<dyn arrow_array::Array>> = Vec::new();
            // Sequence numbers for this chunk.
            physical_columns.push(
                arrow_select::take::take(seq_array.as_ref(), &chunk_indices, None).map_err(
                    |e| crate::Error::DataInvalid {
                        message: format!("Failed to reorder sequence numbers: {e}"),
                        source: None,
                    },
                )?,
            );
            // Value kind column — resolve from batch schema.
            let vk_idx = combined
                .schema()
                .fields()
                .iter()
                .position(|f| f.name() == crate::spec::VALUE_KIND_FIELD_NAME);
            match vk_idx {
                Some(vk_idx) => {
                    physical_columns.push(
                        arrow_select::take::take(
                            combined.column(vk_idx).as_ref(),
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
                    // All rows are INSERT (value_kind = 0).
                    physical_columns.push(Arc::new(Int8Array::from(vec![0i8; chunk_len])));
                }
            }
            // All user columns (skip _VALUE_KIND if present — already handled above).
            for idx in 0..combined.num_columns() {
                if Some(idx) == vk_idx {
                    continue;
                }
                physical_columns.push(
                    arrow_select::take::take(combined.column(idx).as_ref(), &chunk_indices, None)
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

        // Compute key_stats on deduped data (not the raw combined batch).
        let deduped_key_columns: Vec<Arc<dyn arrow_array::Array>> =
            self.config
                .primary_key_indices
                .iter()
                .map(|&idx| {
                    arrow_select::take::take(combined.column(idx).as_ref(), &deduped_u32, None)
                        .map_err(|e| crate::Error::DataInvalid {
                            message: format!("Failed to take key column for stats: {e}"),
                            source: None,
                        })
                })
                .collect::<Result<Vec<_>>>()?;
        let deduped_key_batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(
                self.config
                    .primary_key_indices
                    .iter()
                    .map(|&idx| user_schema.field(idx).clone())
                    .collect::<Vec<_>>(),
            )),
            deduped_key_columns,
        )
        .map_err(|e| crate::Error::DataInvalid {
            message: format!("Failed to build deduped key batch for stats: {e}"),
            source: None,
        })?;
        let stats_col_indices: Vec<usize> = (0..self.config.primary_key_indices.len()).collect();
        let key_stats = compute_column_stats(
            &deduped_key_batch,
            &stats_col_indices,
            &self.config.primary_key_types,
        )?;

        // Sequence numbers span the full assigned range.
        let meta = DataFileMeta {
            file_name,
            file_size,
            row_count: deduped_num_rows as i64,
            min_key,
            max_key,
            key_stats,
            value_stats: BinaryTableStats::new(
                EMPTY_SERIALIZED_ROW.clone(),
                EMPTY_SERIALIZED_ROW.clone(),
                vec![],
            ),
            min_sequence_number: start_seq,
            max_sequence_number: end_seq,
            schema_id: self.config.schema_id,
            level: 0,
            extra_files: vec![],
            creation_time: Some(Utc::now()),
            delete_row_count: Some(0),
            embedded_index: None,
            file_source: Some(0), // FileSource.APPEND
            value_stats_cols: Some(vec![]),
            external_path: None,
            first_row_id: None,
            write_cols: None,
        };
        self.written_files.push(meta);
        Ok(())
    }

    /// Deduplicate sorted indices by primary key using the configured merge engine.
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

        // Convert PK columns to arrow-row Rows for efficient comparison.
        let sort_fields: Vec<SortField> = self
            .config
            .primary_key_indices
            .iter()
            .map(|&idx| SortField::new(batch.schema().field(idx).data_type().clone()))
            .collect();
        let converter =
            RowConverter::new(sort_fields).map_err(|e| crate::Error::UnexpectedError {
                message: format!("Failed to create RowConverter for dedup: {e}"),
                source: Some(Box::new(e)),
            })?;
        let key_columns: Vec<Arc<dyn arrow_array::Array>> = self
            .config
            .primary_key_indices
            .iter()
            .map(|&idx| batch.column(idx).clone())
            .collect();
        let rows =
            converter
                .convert_columns(&key_columns)
                .map_err(|e| crate::Error::UnexpectedError {
                    message: format!("Failed to convert key columns for dedup: {e}"),
                    source: Some(Box::new(e)),
                })?;

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
    pub(crate) async fn prepare_commit(&mut self) -> Result<Vec<DataFileMeta>> {
        self.flush().await?;
        Ok(std::mem::take(&mut self.written_files))
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
