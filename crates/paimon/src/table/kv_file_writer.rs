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

use crate::arrow::arrow_fields_to_paimon;
use crate::arrow::format::{
    create_format_writer, parquet::ParquetFormatWriter, with_write_resources, FormatFileWriter,
};
use crate::io::FileIO;
use crate::resource::{MemoryReservation, ResourceContext};
use crate::spec::stats::{compute_column_stats, BinaryTableStats};
use crate::spec::{
    bucket_path_under, data_file_to_file_index_file_name, extract_datum_from_arrow,
    AggregationConfig, BigIntType, BinaryRowBuilder, CoreOptions, DataField, DataFileMeta,
    DataType, MergeEngine, PartialUpdateConfig, RowKind, TinyIntType, SEQUENCE_NUMBER_FIELD_ID,
    SEQUENCE_NUMBER_FIELD_NAME, VALUE_KIND_FIELD_ID, VALUE_KIND_FIELD_NAME,
};
use crate::table::data_file_index_writer::FileIndexOptions;
use crate::table::managed_blob_reference::ManagedBlobReferences;
use crate::table::managed_blob_writer::ManagedBlobWriteState;
use crate::table::prepared_files::PreparedFiles;
use crate::table::sort_merge::{
    AggregateMergeFunction, BufferedBatch, MergeFunction, MergeResult, MergeRow,
    PartialUpdateMergeFunction,
};
use crate::Result;
use arrow_array::{Array, BooleanArray, Int64Array, Int8Array, RecordBatch, UInt32Array};
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
    target_file_row_num: usize,
    ignore_delete: bool,
    /// Next sequence number to assign (bucket-local, always auto-incremented).
    next_sequence_number: i64,
    /// Buffered batches (user schema).
    buffer: Vec<RecordBatch>,
    /// Approximate buffered bytes.
    buffer_bytes: usize,
    resources: Option<ResourceContext>,
    buffer_reservation: Option<MemoryReservation>,
    /// Completed file metadata.
    written_files: Vec<DataFileMeta>,
    /// Completed changelog file metadata.
    written_changelog_files: Vec<DataFileMeta>,
    managed_blob_writer: ManagedBlobWriteState,
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
    pub data_file_prefix: String,
    pub input_changelog: bool,
    pub changelog_file_prefix: String,
    pub changelog_file_compression: String,
    pub changelog_file_format: String,
    /// Full primary keys, including partition columns which must not be aggregated.
    pub primary_keys: Vec<String>,
    /// Trimmed primary key column indices in the user schema.
    pub primary_key_indices: Vec<usize>,
    /// Paimon DataTypes for each primary key column (same order as primary_key_indices).
    pub primary_key_types: Vec<DataType>,
    /// Logical value fields, used for footer statistics and to retain Paimon
    /// types when building the physical file schema.
    pub value_fields: Vec<DataField>,
    /// Sequence field column indices in the user schema (empty if not configured).
    pub sequence_field_indices: Vec<usize>,
    /// Merge engine for deduplication.
    pub merge_engine: MergeEngine,
    pub deletion_vectors_enabled: bool,
    /// File indexes follow the sorted, merged data rows and never changelog rows.
    pub file_index_options: Option<Arc<FileIndexOptions>>,
}

struct IndexedFileWrite<'a> {
    is_changelog: bool,
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
        let core_options = CoreOptions::new(&config.table_options);
        let target_file_row_num = core_options
            .target_file_row_num()?
            .try_into()
            .unwrap_or(usize::MAX);
        let ignore_delete =
            config.merge_engine == MergeEngine::PartialUpdate && core_options.ignore_delete();
        if config.merge_engine == MergeEngine::PartialUpdate {
            let partial_update = PartialUpdateConfig::new(&config.table_options);
            partial_update.validate_write_mode(true, &config.table_name)?;
            partial_update
                .validated_aggregate_functions(&config.value_fields, &config.primary_keys)?;

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

        let managed_blob_writer = ManagedBlobWriteState::new(&file_io, &config)?;

        Ok(Self {
            file_io,
            config,
            target_file_row_num,
            ignore_delete,
            next_sequence_number,
            buffer: Vec::new(),
            buffer_bytes: 0,
            resources: None,
            buffer_reservation: None,
            written_files: Vec::new(),
            written_changelog_files: Vec::new(),
            managed_blob_writer,
        })
    }

    pub(crate) fn with_resources(mut self, resources: Option<ResourceContext>) -> Self {
        self.buffer_reservation = resources.as_ref().map(ResourceContext::reservation);
        self.resources = resources;
        self
    }

    /// Buffer a RecordBatch. Flushes when buffer exceeds write_buffer_size.
    /// Sequence numbers are assigned per-bucket on flush, matching Java Paimon behavior.
    pub(crate) async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        // Filter before byte accounting and buffering so ignored retracts do
        // not consume write-buffer memory or automatic sequence numbers.
        let batch = if self.ignore_delete {
            Self::filter_retract_rows(batch)?
        } else {
            batch.clone()
        };
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let batch = self.managed_blob_writer.externalize(batch).await?;
        let batch_bytes: usize = batch
            .columns()
            .iter()
            .map(|c| c.get_buffer_memory_size())
            .sum();
        if let Some(reservation) = &mut self.buffer_reservation {
            reservation.try_grow(batch_bytes)?;
        }
        self.buffer.push(batch);
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
        let _buffer_reservation = self.buffer_reservation.take();
        self.buffer_reservation = self.resources.as_ref().map(ResourceContext::reservation);

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
        let user_sequence_descending =
            !CoreOptions::new(&self.config.table_options).sequence_field_sort_order_is_ascending();
        for &idx in &self.config.sequence_field_indices {
            sort_columns.push(SortColumn {
                values: combined.column(idx).clone(),
                options: Some(SortOptions {
                    descending: user_sequence_descending,
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

        // After sorting by PK + configured user sequence + auto-seq, merge
        // each key group down to one row, mirroring Java's
        // MergeTreeWriter#flushWriteBuffer (the write buffer runs the merge
        // function before any file is written, so a flushed file never holds
        // two rows of one key):
        //   Deduplicate   → keep last row per key group (highest seq)
        //   FirstRow      → keep first row per key group (lowest seq)
        //   PartialUpdate → per column, keep the latest non-null value
        //   Aggregation   → apply per-field aggregators in sequence order
        let (data_batch, data_seq, data_indices) = match self.config.merge_engine {
            MergeEngine::PartialUpdate => {
                let (merged, merged_seq) =
                    self.merge_partial_update_rows(&combined, seq_array.as_ref(), &sorted_indices)?;
                let identity =
                    UInt32Array::from_iter_values(0..u32::try_from(merged.num_rows()).unwrap());
                (merged, merged_seq, identity)
            }
            MergeEngine::Aggregation => {
                let (merged, merged_seq) =
                    self.merge_aggregation_rows(&combined, seq_array.as_ref(), &sorted_indices)?;
                let identity =
                    UInt32Array::from_iter_values(0..u32::try_from(merged.num_rows()).unwrap());
                (merged, merged_seq, identity)
            }
            MergeEngine::Deduplicate | MergeEngine::FirstRow => {
                let selected = self.select_flush_indices(&combined, &sorted_indices)?;
                (
                    combined.clone(),
                    seq_array.clone(),
                    UInt32Array::from(selected),
                )
            }
        };

        // The sorted output is already materialized in FLUSH_CHUNK_ROWS batches.
        // Use the row target as an upper bound for each emitted batch and keep
        // every file's key bounds, sequence bounds, and index local to its rows.
        let data_sequences = data_seq.as_any().downcast_ref::<Int64Array>().unwrap();
        for offset in (0..data_indices.len()).step_by(self.target_file_row_num) {
            let len = self.target_file_row_num.min(data_indices.len() - offset);
            let file_indices = data_indices.slice(offset, len);
            let (min_sequence_number, max_sequence_number) = file_indices
                .values()
                .iter()
                .map(|&idx| data_sequences.value(idx as usize))
                .fold((i64::MAX, i64::MIN), |(min, max), seq| {
                    (min.min(seq), max.max(seq))
                });
            let file = self
                .write_indexed_file(
                    &data_batch,
                    data_seq.as_ref(),
                    &file_indices,
                    IndexedFileWrite {
                        is_changelog: false,
                        file_prefix: &self.config.data_file_prefix,
                        file_ordinal: self.written_files.len(),
                        file_format: &self.config.file_format,
                        file_compression: &self.config.file_compression,
                        min_sequence_number,
                        max_sequence_number,
                        delete_row_count: Self::indexed_delete_row_count(
                            &data_batch,
                            &file_indices,
                        )?,
                    },
                )
                .await?;
            self.written_files.push(file);
        }

        if self.config.input_changelog {
            let input_sequences = seq_array.as_any().downcast_ref::<Int64Array>().unwrap();
            for offset in (0..sorted_indices.len()).step_by(self.target_file_row_num) {
                let len = self.target_file_row_num.min(sorted_indices.len() - offset);
                let file_indices = sorted_indices.slice(offset, len);
                let (min_sequence_number, max_sequence_number) = file_indices
                    .values()
                    .iter()
                    .map(|&idx| input_sequences.value(idx as usize))
                    .fold((i64::MAX, i64::MIN), |(min, max), seq| {
                        (min.min(seq), max.max(seq))
                    });
                let file = self
                    .write_indexed_file(
                        &combined,
                        seq_array.as_ref(),
                        &file_indices,
                        IndexedFileWrite {
                            is_changelog: true,
                            file_prefix: &self.config.changelog_file_prefix,
                            file_ordinal: self.written_changelog_files.len(),
                            file_format: &self.config.changelog_file_format,
                            file_compression: &self.config.changelog_file_compression,
                            min_sequence_number,
                            max_sequence_number,
                            delete_row_count: Self::indexed_delete_row_count(
                                &combined,
                                &file_indices,
                            )?,
                        },
                    )
                    .await?;
                self.written_changelog_files.push(file);
            }
        }
        Ok(())
    }

    fn filter_retract_rows(batch: &RecordBatch) -> Result<RecordBatch> {
        let Some(vk_idx) = batch
            .schema()
            .fields()
            .iter()
            .position(|field| field.name() == VALUE_KIND_FIELD_NAME)
        else {
            return Ok(batch.clone());
        };
        let value_kinds = batch
            .column(vk_idx)
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| crate::Error::DataInvalid {
                message: "_VALUE_KIND column must be Int8".to_string(),
                source: None,
            })?;
        let keep = BooleanArray::from(
            (0..batch.num_rows())
                .map(|row| {
                    let value = if value_kinds.is_null(row) {
                        RowKind::Insert.to_value()
                    } else {
                        value_kinds.value(row)
                    };
                    RowKind::from_value(value).map(|kind| kind.is_add())
                })
                .collect::<Result<Vec<_>>>()?,
        );

        arrow_select::filter::filter_record_batch(batch, &keep).map_err(|e| {
            crate::Error::DataInvalid {
                message: format!("Failed to filter ignored retract rows: {e}"),
                source: None,
            }
        })
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
        let mut file_index = if write.is_changelog {
            None
        } else {
            self.config
                .file_index_options
                .as_ref()
                .map(|options| options.create_writer())
                .transpose()?
        };
        let physical_schema = build_physical_schema(&user_schema);
        let mut blob_references = ManagedBlobReferences::new(
            &self.config.value_fields,
            &CoreOptions::new(&self.config.table_options),
            &physical_schema,
            self.managed_blob_writer.enabled(),
            write.is_changelog,
        )?;

        let file_name = format!(
            "{}{}-{}.{}",
            write.file_prefix,
            uuid::Uuid::new_v4(),
            write.file_ordinal,
            write.file_format,
        );
        let bucket_dir = bucket_path_under(
            &self.config.table_location,
            &self.config.partition_path,
            self.config.bucket,
        );
        self.file_io.mkdirs(&format!("{bucket_dir}/")).await?;
        let file_path = format!("{bucket_dir}/{file_name}");
        let output = self.file_io.new_output(&file_path)?;
        // The physical KV file also contains sequence and row-kind columns. Give
        // Parquet only the logical value fields so metadata stats and their dense
        // column mapping follow Java's value schema (and its stats options).
        // Keep the existing unshredded KV layout for this writer.
        let writer: Box<dyn FormatFileWriter> = if write.file_format.eq_ignore_ascii_case("parquet")
        {
            let mut stats_options = self.config.table_options.clone();
            let core_options = CoreOptions::new(&self.config.table_options);
            let stats_mode = core_options.pk_file_metadata_stats_mode(0, write.is_changelog)?;
            stats_options.insert("metadata.stats-mode".to_string(), stats_mode.to_string());
            Box::new(
                ParquetFormatWriter::new(
                    &output,
                    physical_schema.clone(),
                    write.file_compression,
                    self.config.file_compression_zstd_level,
                    Some(&self.config.value_fields),
                    &stats_options,
                )
                .await?,
            )
        } else {
            let physical_fields =
                build_physical_fields(&physical_schema, &self.config.value_fields)?;
            create_format_writer(
                &output,
                physical_schema.clone(),
                write.file_compression,
                self.config.file_compression_zstd_level,
                None,
                Some(&physical_fields),
                Some(&self.config.table_options),
            )
            .await?
        };
        let mut writer = with_write_resources(writer, self.resources.as_ref());

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
            if let Err(error) = ManagedBlobReferences::collect(&mut blob_references, &chunk_batch) {
                let _ = writer.close().await;
                let _ = self.file_io.delete_file(&file_path).await;
                return Err(error);
            }
            if let Err(error) = writer.write(&chunk_batch).await {
                let _ = writer.close().await;
                let _ = self.file_io.delete_file(&file_path).await;
                return Err(error);
            }
            if let Some(index) = file_index.as_mut() {
                // The index positions must match the physical file after PK
                // sorting and flush-time merging. Its field positions refer
                // to the logical value schema, so omit the two KV metadata
                // columns from the same output chunk used by the file writer.
                let logical_indices = (2..chunk_batch.num_columns()).collect::<Vec<_>>();
                let index_result = chunk_batch
                    .project(&logical_indices)
                    .map_err(|error| crate::Error::DataInvalid {
                        message: format!("Failed to project KV index values: {error}"),
                        source: None,
                    })
                    .and_then(|logical_batch| index.write(&logical_batch));
                if let Err(error) = index_result {
                    let _ = writer.close().await;
                    let _ = self.file_io.delete_file(&file_path).await;
                    return Err(error);
                }
            }
        }

        let write_result = writer.close().await?;
        let file_size = write_result.file_size as i64;
        let (value_stats, value_stats_cols) = match write_result.value_stats {
            Some(stats) => (stats.stats, stats.columns),
            None => (BinaryTableStats::empty(), Some(Vec::new())),
        };

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

        let mut meta = DataFileMeta {
            file_name,
            file_size,
            row_count: indices.len() as i64,
            min_key,
            max_key,
            key_stats,
            value_stats,
            min_sequence_number: write.min_sequence_number,
            max_sequence_number: write.max_sequence_number,
            schema_id: self.config.schema_id,
            level: 0,
            extra_files: vec![],
            creation_time: Some(Utc::now()),
            delete_row_count: Some(write.delete_row_count),
            embedded_index: None,
            file_source: Some(0), // FileSource.APPEND
            value_stats_cols,
            external_path: None,
            first_row_id: None,
            write_cols: None,
            column_max_sequence_numbers: None,
        };
        if let Some(index) = file_index {
            let index_result = index.serialize();
            let bytes = match index_result {
                Ok(bytes) => bytes,
                Err(error) => {
                    let _ = self.file_io.delete_file(&file_path).await;
                    return Err(error);
                }
            };
            let threshold = self
                .config
                .file_index_options
                .as_ref()
                .expect("file index writer must have options")
                .in_manifest_threshold;
            if bytes.len() as u64 > threshold as u64 {
                let name = data_file_to_file_index_file_name(&meta.file_name);
                let index_path = format!("{bucket_dir}/{name}");
                let output = match self.file_io.new_output(&index_path) {
                    Ok(output) => output,
                    Err(error) => {
                        let _ = self.file_io.delete_file(&file_path).await;
                        return Err(error);
                    }
                };
                if let Err(error) = output.write(bytes).await {
                    let _ = self.file_io.delete_file(&index_path).await;
                    let _ = self.file_io.delete_file(&file_path).await;
                    return Err(error);
                }
                meta.extra_files.push(name);
            } else {
                meta.embedded_index = Some(bytes.to_vec());
            }
        }

        ManagedBlobReferences::finish(
            blob_references,
            &self.file_io,
            &file_path,
            &bucket_dir,
            &mut meta,
        )
        .await?;

        Ok(meta)
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
            MergeEngine::Aggregation => {
                unreachable!("aggregation merges rows at flush via merge_aggregation_rows")
            }
        }
    }

    /// Java's reducer wrapper keeps a one-row key group byte-for-byte. In
    /// particular, a lone retract must reach an older file with its original
    /// kind and payload instead of being aggregated against an empty state.
    fn singleton_merge_row(
        batch: &RecordBatch,
        row_idx: usize,
        output_indices: &[usize],
        output_schema: &Arc<ArrowSchema>,
    ) -> Result<RecordBatch> {
        let columns = output_indices
            .iter()
            .map(|&index| batch.column(index).slice(row_idx, 1))
            .collect();
        RecordBatch::try_new(output_schema.clone(), columns).map_err(|error| {
            crate::Error::DataInvalid {
                message: format!("Failed to keep singleton merge row: {error}"),
                source: Some(Box::new(error)),
            }
        })
    }

    fn merge_aggregation_rows(
        &self,
        batch: &RecordBatch,
        seq_array: &dyn Array,
        sorted_indices: &UInt32Array,
    ) -> Result<(RecordBatch, Arc<dyn Array>)> {
        let schema = batch.schema();
        let value_kind_idx = schema
            .fields()
            .iter()
            .position(|field| field.name() == VALUE_KIND_FIELD_NAME);
        let value_kinds = value_kind_idx
            .map(|idx| {
                batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .ok_or_else(|| crate::Error::DataInvalid {
                        message: "_VALUE_KIND column must be Int8".into(),
                        source: None,
                    })
            })
            .transpose()?;
        let output_indices: Vec<_> = (0..batch.num_columns())
            .filter(|idx| Some(*idx) != value_kind_idx)
            .collect();
        let output_fields: Vec<_> = output_indices
            .iter()
            .map(|&idx| schema.field(idx).clone())
            .collect();
        let output_schema = Arc::new(ArrowSchema::new(output_fields.clone()));
        let sequence_fields: Vec<_> = self
            .config
            .sequence_field_indices
            .iter()
            .map(|&idx| schema.field(idx).name().clone())
            .collect();
        let merge = AggregateMergeFunction::new(
            &self.config.table_options,
            &self.config.table_name,
            &arrow_fields_to_paimon(&output_fields)?,
            &self.config.primary_keys,
            &sequence_fields,
        )?;
        let rows: Vec<_> = sorted_indices
            .values()
            .iter()
            .map(|&idx| MergeRow {
                batch_idx: 0,
                row_idx: idx as usize,
                // Ordering is already established by the write buffer's Arrow sort.
                sequence_number: 0,
                user_sequence: None,
                value_kind: value_kinds
                    .filter(|kinds| kinds.is_valid(idx as usize))
                    .map_or(0, |kinds| kinds.value(idx as usize)),
            })
            .collect();
        let key_rows = self.convert_key_rows(batch)?;
        let buffers = [BufferedBatch::Source(batch.clone())];
        let mut merged = Vec::new();
        let mut merged_kinds = Vec::new();
        let mut last_indices = Vec::new();
        let mut start = 0;
        while start < rows.len() {
            let mut end = start + 1;
            while end < rows.len()
                && key_rows.row(rows[end].row_idx) == key_rows.row(rows[start].row_idx)
            {
                end += 1;
            }
            if end - start == 1 {
                merged.push(Self::singleton_merge_row(
                    batch,
                    rows[start].row_idx,
                    &output_indices,
                    &output_schema,
                )?);
                merged_kinds.push(rows[start].value_kind);
            } else {
                let group = rows[start..end].iter().collect::<Vec<_>>();
                let (row, delete) =
                    merge.merge_ordered(&group, &buffers, &output_indices, &output_schema)?;
                merged.push(row);
                merged_kinds.push(if delete {
                    RowKind::Delete as i8
                } else {
                    RowKind::Insert as i8
                });
            }
            // Java retains the last row in sequence-field order, which need not
            // have the largest arrival sequence number.
            last_indices.push(sorted_indices.value(end - 1));
            start = end;
        }
        let arrow_error = |e: arrow_schema::ArrowError| crate::Error::DataInvalid {
            message: format!("Failed to build merged aggregation batch: {e}"),
            source: Some(Box::new(e)),
        };
        let merged =
            arrow_select::concat::concat_batches(&output_schema, &merged).map_err(arrow_error)?;
        let merged = if let Some(idx) = value_kind_idx {
            let mut columns = merged.columns().to_vec();
            columns.insert(idx, Arc::new(Int8Array::from(merged_kinds)));
            RecordBatch::try_new(schema, columns).map_err(arrow_error)?
        } else {
            merged
        };
        let merged_seq =
            arrow_select::take::take(seq_array, &UInt32Array::from(last_indices), None)
                .map_err(arrow_error)?;
        Ok((merged, merged_seq))
    }

    /// Merge each key group with the same function used by scans and compaction.
    /// The buffer is already sorted by PK, user sequence, and auto sequence, so
    /// equal-sequence MergeRows retain that established order.
    fn merge_partial_update_rows(
        &self,
        batch: &RecordBatch,
        seq_array: &dyn Array,
        sorted_indices: &UInt32Array,
    ) -> Result<(RecordBatch, Arc<dyn Array>)> {
        let schema = batch.schema();
        let value_kind_idx = schema
            .fields()
            .iter()
            .position(|field| field.name() == VALUE_KIND_FIELD_NAME);
        let value_kinds = value_kind_idx
            .map(|idx| {
                batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .ok_or_else(|| crate::Error::DataInvalid {
                        message: "_VALUE_KIND column must be Int8".into(),
                        source: None,
                    })
            })
            .transpose()?;
        let output_indices: Vec<_> = (0..batch.num_columns())
            .filter(|idx| Some(*idx) != value_kind_idx)
            .collect();
        let output_fields: Vec<_> = output_indices
            .iter()
            .map(|&idx| schema.field(idx).clone())
            .collect();
        let output_schema = Arc::new(ArrowSchema::new(output_fields.clone()));
        let merge = PartialUpdateMergeFunction::new_with_schema(
            &self.config.table_options,
            &self.config.table_name,
            &self.config.value_fields,
            &arrow_fields_to_paimon(&output_fields)?,
            &self.config.primary_keys,
        )?;
        let rows: Vec<_> = sorted_indices
            .values()
            .iter()
            .enumerate()
            .map(|(sorted_rank, &idx)| MergeRow {
                batch_idx: 0,
                row_idx: idx as usize,
                // The write buffer has already sorted by user sequence and
                // arrival sequence. Preserve that order when the shared merge
                // function sorts MergeRows again.
                sequence_number: sorted_rank as i64,
                user_sequence: None,
                value_kind: value_kinds
                    .filter(|kinds| kinds.is_valid(idx as usize))
                    .map_or(0, |kinds| kinds.value(idx as usize)),
            })
            .collect();
        let key_rows = self.convert_key_rows(batch)?;
        let buffers = [BufferedBatch::Source(batch.clone())];
        let mut merged = Vec::new();
        let mut merged_kinds = Vec::new();
        let mut last_indices = Vec::new();
        let mut start = 0;
        while start < rows.len() {
            let mut end = start + 1;
            while end < rows.len()
                && key_rows.row(rows[end].row_idx) == key_rows.row(rows[start].row_idx)
            {
                end += 1;
            }
            if end - start == 1 {
                merged.push(Self::singleton_merge_row(
                    batch,
                    rows[start].row_idx,
                    &output_indices,
                    &output_schema,
                )?);
                merged_kinds.push(rows[start].value_kind);
                last_indices.push(sorted_indices.value(start));
                start = end;
                continue;
            }
            match merge.merge(&rows[start..end], &buffers, &output_indices, &output_schema)? {
                MergeResult::MaterializedRow(row) => {
                    merged.push(row);
                    merged_kinds.push(RowKind::Insert as i8);
                    last_indices.push(sorted_indices.value(end - 1));
                }
                MergeResult::MaterializedDeleteRow(row) => {
                    merged.push(row);
                    merged_kinds.push(RowKind::Delete as i8);
                    last_indices.push(sorted_indices.value(end - 1));
                }
                MergeResult::Omit => {}
                MergeResult::SourceRow { .. } => {
                    return Err(crate::Error::UnexpectedError {
                        message: "Partial-update merge returned an unmaterialized row".into(),
                        source: None,
                    });
                }
            }
            start = end;
        }
        let arrow_error = |e: arrow_schema::ArrowError| crate::Error::DataInvalid {
            message: format!("Failed to build merged partial-update batch: {e}"),
            source: Some(Box::new(e)),
        };
        let merged = if merged.is_empty() {
            RecordBatch::new_empty(output_schema.clone())
        } else {
            arrow_select::concat::concat_batches(&output_schema, &merged).map_err(arrow_error)?
        };
        let merged = if let Some(idx) = value_kind_idx {
            let mut columns = merged.columns().to_vec();
            columns.insert(idx, Arc::new(Int8Array::from(merged_kinds)));
            RecordBatch::try_new(schema, columns).map_err(arrow_error)?
        } else {
            merged
        };
        let merged_seq =
            arrow_select::take::take(seq_array, &UInt32Array::from(last_indices), None)
                .map_err(arrow_error)?;
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

    pub(crate) async fn abort(&mut self) {
        self.buffer.clear();
        self.buffer_bytes = 0;
        if let Some(reservation) = &mut self.buffer_reservation {
            let _ = reservation.try_resize(0);
        }
        let bucket_path = bucket_path_under(
            &self.config.table_location,
            &self.config.partition_path,
            self.config.bucket,
        );
        for file in self
            .written_files
            .drain(..)
            .chain(self.written_changelog_files.drain(..))
        {
            for path in file.collect_files(&bucket_path) {
                let _ = self.file_io.delete_file(&path).await;
            }
        }
        self.managed_blob_writer.abort().await;
    }

    /// Flush remaining buffer and return all written file metadata.
    pub(crate) async fn prepare_commit(&mut self) -> Result<PreparedFiles> {
        self.flush().await?;
        self.managed_blob_writer.prepare_commit().await?;
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

/// Describe the actual file columns while retaining logical Paimon types that
/// Arrow cannot distinguish (for example MULTISET and MAP).
fn build_physical_fields(
    physical_schema: &ArrowSchema,
    value_fields: &[DataField],
) -> Result<Vec<DataField>> {
    physical_schema
        .fields()
        .iter()
        .map(|arrow_field| match arrow_field.name().as_str() {
            SEQUENCE_NUMBER_FIELD_NAME => Ok(DataField::new(
                SEQUENCE_NUMBER_FIELD_ID,
                SEQUENCE_NUMBER_FIELD_NAME.into(),
                DataType::BigInt(BigIntType::new()),
            )),
            VALUE_KIND_FIELD_NAME => Ok(DataField::new(
                VALUE_KIND_FIELD_ID,
                VALUE_KIND_FIELD_NAME.into(),
                DataType::TinyInt(TinyIntType::new()),
            )),
            name => value_fields
                .iter()
                .find(|field| field.name() == name)
                .cloned()
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: format!(
                        "Physical file column '{name}' is missing from the table schema"
                    ),
                    source: None,
                }),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::IntType;
    use arrow_array::{Int32Array, RecordBatchReader, StringArray, UInt32Array};
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
            data_file_prefix: "data-".to_string(),
            input_changelog: false,
            changelog_file_prefix: "changelog-".to_string(),
            changelog_file_compression: "none".to_string(),
            changelog_file_format: "parquet".to_string(),
            primary_keys: vec!["id".into()],
            primary_key_indices: vec![0],
            primary_key_types: vec![DataType::Int(IntType::new())],
            value_fields: vec![
                DataField::new(0, "id".into(), DataType::Int(IntType::new())),
                DataField::new(
                    1,
                    "seq".into(),
                    DataType::BigInt(crate::spec::BigIntType::new()),
                ),
                DataField::new(2, "value".into(), DataType::Int(IntType::new())),
            ],
            sequence_field_indices: vec![1],
            merge_engine,
            deletion_vectors_enabled: false,
            file_index_options: None,
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

    #[tokio::test]
    async fn target_file_row_num_rolls_data_and_changelog_with_local_metadata() {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("seq", ArrowDataType::Int64, false),
            ArrowField::new("value", ArrowDataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![5, 1, 4, 2, 3])),
                Arc::new(Int64Array::from(vec![50, 10, 40, 20, 30])),
                Arc::new(Int32Array::from(vec![50, 10, 40, 20, 30])),
            ],
        )
        .unwrap();
        let mut config = test_write_config(MergeEngine::Deduplicate);
        config.input_changelog = true;
        config.write_buffer_size = i64::MAX;
        config
            .table_options
            .insert("target-file-row-num".into(), "2".into());
        let mut writer =
            KeyValueFileWriter::new(FileIOBuilder::new("memory").build().unwrap(), config, 0)
                .unwrap();
        writer.write(&batch).await.unwrap();
        let prepared = writer.prepare_commit().await.unwrap();

        for files in [&prepared.data_files, &prepared.changelog_files] {
            assert_eq!(
                files.iter().map(|file| file.row_count).collect::<Vec<_>>(),
                vec![2, 2, 1]
            );
            assert_eq!(
                files
                    .iter()
                    .map(|file| (file.min_sequence_number, file.max_sequence_number))
                    .collect::<Vec<_>>(),
                vec![(1, 3), (2, 4), (0, 0)]
            );
        }
    }

    #[tokio::test]
    async fn test_pk_value_stats_use_emitted_rows_and_logical_columns() {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("seq", ArrowDataType::Int64, false),
            ArrowField::new("value", ArrowDataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 1])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(Int32Array::from(vec![Some(100), None, Some(300)])),
            ],
        )
        .unwrap();
        let mut config = test_write_config(MergeEngine::Deduplicate);
        config.input_changelog = true;
        config.write_buffer_size = i64::MAX;
        config
            .table_options
            .insert("metadata.stats-dense-store".to_string(), "true".to_string());
        let mut writer =
            KeyValueFileWriter::new(FileIOBuilder::new("memory").build().unwrap(), config, 0)
                .unwrap();
        writer.write(&batch).await.unwrap();
        let prepared = writer.prepare_commit().await.unwrap();

        let data = &prepared.data_files[0];
        assert_eq!(data.row_count, 2);
        assert_eq!(data.value_stats_cols, None);
        assert_eq!(
            data.value_stats.null_counts(),
            &vec![Some(0), Some(0), Some(1)]
        );
        let min =
            crate::spec::BinaryRow::from_serialized_bytes(data.value_stats.min_values()).unwrap();
        let max =
            crate::spec::BinaryRow::from_serialized_bytes(data.value_stats.max_values()).unwrap();
        assert_eq!(min.arity(), 3);
        assert_eq!(min.get_int(0).unwrap(), 1);
        assert_eq!(max.get_int(0).unwrap(), 2);
        assert_eq!(min.get_long(1).unwrap(), 20);
        assert_eq!(max.get_long(1).unwrap(), 30);
        assert_eq!(min.get_int(2).unwrap(), 300);
        assert_eq!(max.get_int(2).unwrap(), 300);

        let changelog = &prepared.changelog_files[0];
        assert_eq!(changelog.row_count, 3);
        assert_eq!(
            changelog.value_stats.null_counts(),
            &vec![Some(0), Some(0), Some(1)]
        );
        let min = crate::spec::BinaryRow::from_serialized_bytes(changelog.value_stats.min_values())
            .unwrap();
        assert_eq!(min.get_int(2).unwrap(), 100);
    }

    #[tokio::test]
    async fn test_pk_value_stats_respect_dense_column_modes() {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("seq", ArrowDataType::Int64, false),
            ArrowField::new("value", ArrowDataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(Int32Array::from(vec![Some(100), None])),
            ],
        )
        .unwrap();
        let mut config = test_write_config(MergeEngine::Deduplicate);
        config.write_buffer_size = i64::MAX;
        config.table_options.extend([
            ("metadata.stats-mode".to_string(), "none".to_string()),
            ("metadata.stats-dense-store".to_string(), "true".to_string()),
            ("fields.id.stats-mode".to_string(), "full".to_string()),
            ("fields.value.stats-mode".to_string(), "counts".to_string()),
        ]);
        let mut writer =
            KeyValueFileWriter::new(FileIOBuilder::new("memory").build().unwrap(), config, 0)
                .unwrap();
        writer.write(&batch).await.unwrap();
        let prepared = writer.prepare_commit().await.unwrap();
        let file = &prepared.data_files[0];

        assert_eq!(
            file.value_stats_cols,
            Some(vec!["id".into(), "value".into()])
        );
        assert_eq!(file.value_stats.null_counts(), &vec![Some(0), Some(1)]);
        let min =
            crate::spec::BinaryRow::from_serialized_bytes(file.value_stats.min_values()).unwrap();
        assert_eq!(min.arity(), 2);
        assert_eq!(min.get_int(0).unwrap(), 1);
        assert!(min.is_null_at(1));
    }

    #[tokio::test]
    async fn test_pk_binary_value_stats_match_full_and_truncate_modes() {
        use crate::spec::{BinaryType, VarBinaryType};
        use arrow_array::BinaryArray;

        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("payload", ArrowDataType::Binary, false),
            ArrowField::new("raw", ArrowDataType::Binary, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(BinaryArray::from_iter_values([b"ab1", b"ac0"])),
                Arc::new(BinaryArray::from_iter_values([
                    b"\xfe\x01".as_slice(),
                    b"\xff\x01".as_slice(),
                ])),
            ],
        )
        .unwrap();
        let mut config = test_write_config(MergeEngine::Deduplicate);
        config.value_fields = vec![
            DataField::new(0, "id".into(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "payload".into(),
                DataType::Binary(BinaryType::new(4).unwrap()),
            ),
            DataField::new(
                2,
                "raw".into(),
                DataType::VarBinary(VarBinaryType::new(8).unwrap()),
            ),
        ];
        config.write_buffer_size = i64::MAX;
        config.table_options.extend([
            ("metadata.stats-mode".to_string(), "full".to_string()),
            (
                "fields.payload.stats-mode".to_string(),
                "truncate(2)".to_string(),
            ),
        ]);
        let mut writer =
            KeyValueFileWriter::new(FileIOBuilder::new("memory").build().unwrap(), config, 0)
                .unwrap();
        writer.write(&batch).await.unwrap();
        let prepared = writer.prepare_commit().await.unwrap();
        let stats = &prepared.data_files[0].value_stats;
        assert_eq!(stats.null_counts(), &vec![Some(0); 3]);
        let min = crate::spec::BinaryRow::from_serialized_bytes(stats.min_values()).unwrap();
        let max = crate::spec::BinaryRow::from_serialized_bytes(stats.max_values()).unwrap();
        assert_eq!(min.get_binary(1).unwrap(), b"ab");
        assert_eq!(max.get_binary(1).unwrap(), b"ad");
        assert_eq!(min.get_binary(2).unwrap(), b"\xfe\x01");
        assert_eq!(max.get_binary(2).unwrap(), b"\xff\x01");
    }

    #[tokio::test]
    async fn test_pk_value_stats_choose_level_and_changelog_modes() {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("seq", ArrowDataType::Int64, false),
            ArrowField::new("value", ArrowDataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(Int32Array::from(vec![100, 200])),
            ],
        )
        .unwrap();
        let mut config = test_write_config(MergeEngine::Deduplicate);
        config.input_changelog = true;
        config.write_buffer_size = i64::MAX;
        config.table_options.extend([
            ("metadata.stats-mode".to_string(), "none".to_string()),
            (
                "metadata.stats-mode.per.level".to_string(),
                "0:counts".to_string(),
            ),
            ("changelog-file.stats-mode".to_string(), "full".to_string()),
        ]);
        let mut writer =
            KeyValueFileWriter::new(FileIOBuilder::new("memory").build().unwrap(), config, 0)
                .unwrap();
        writer.write(&batch).await.unwrap();
        let prepared = writer.prepare_commit().await.unwrap();
        let data = &prepared.data_files[0].value_stats;
        assert_eq!(data.null_counts(), &vec![Some(0); 3]);
        let data_min = crate::spec::BinaryRow::from_serialized_bytes(data.min_values()).unwrap();
        assert!(data_min.is_null_at(0));
        assert!(data_min.is_null_at(2));

        let changelog = &prepared.changelog_files[0].value_stats;
        assert_eq!(changelog.null_counts(), &vec![Some(0); 3]);
        let changelog_min =
            crate::spec::BinaryRow::from_serialized_bytes(changelog.min_values()).unwrap();
        assert_eq!(changelog_min.get_int(0).unwrap(), 1);
        assert_eq!(changelog_min.get_int(2).unwrap(), 100);
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

    #[test]
    fn test_flush_merge_engines_preserve_delete_tombstones() {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("seq", ArrowDataType::Int64, false),
            ArrowField::new(VALUE_KIND_FIELD_NAME, ArrowDataType::Int8, false),
            ArrowField::new("value", ArrowDataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 2])),
                Arc::new(Int64Array::from(vec![10, 20, 5])),
                Arc::new(Int8Array::from(vec![0, 3, 0])),
                Arc::new(Int32Array::from(vec![Some(100), Some(200), Some(50)])),
            ],
        )
        .unwrap();
        let sorted = UInt32Array::from(vec![0, 1, 2]);
        let sequence = Int64Array::from(vec![0, 1, 2]);
        for engine in [MergeEngine::PartialUpdate, MergeEngine::Aggregation] {
            let mut config = test_write_config(engine);
            config.table_options.insert(
                match engine {
                    MergeEngine::PartialUpdate => "partial-update.remove-record-on-delete",
                    MergeEngine::Aggregation => "aggregation.remove-record-on-delete",
                    _ => unreachable!(),
                }
                .into(),
                "true".into(),
            );
            let writer =
                KeyValueFileWriter::new(FileIOBuilder::new("memory").build().unwrap(), config, 0)
                    .unwrap();
            let (merged, _) = match engine {
                MergeEngine::PartialUpdate => {
                    writer.merge_partial_update_rows(&batch, &sequence, &sorted)
                }
                MergeEngine::Aggregation => {
                    writer.merge_aggregation_rows(&batch, &sequence, &sorted)
                }
                _ => unreachable!(),
            }
            .unwrap();
            assert_eq!(merged.num_rows(), 2);
            let kinds = merged
                .column(2)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap();
            let values = merged
                .column(3)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(kinds.values().as_ref(), &[3, 0]);
            assert_eq!(values.values().as_ref(), &[200, 50]);
        }
    }

    #[test]
    fn test_flush_partial_update_sequence_group_delete_preserves_tombstone() {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("seq", ArrowDataType::Int64, false),
            ArrowField::new(VALUE_KIND_FIELD_NAME, ArrowDataType::Int8, false),
            ArrowField::new("value", ArrowDataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 1])),
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(Int8Array::from(vec![0, 3])),
                Arc::new(Int32Array::from(vec![Some(100), Some(100)])),
            ],
        )
        .unwrap();
        let mut config = test_write_config(MergeEngine::PartialUpdate);
        config
            .table_options
            .insert("fields.seq.sequence-group".into(), "value".into());
        config.table_options.insert(
            "partial-update.remove-record-on-sequence-group".into(),
            "seq".into(),
        );
        let writer =
            KeyValueFileWriter::new(FileIOBuilder::new("memory").build().unwrap(), config, 0)
                .unwrap();
        let (merged, _) = writer
            .merge_partial_update_rows(
                &batch,
                &Int64Array::from(vec![0, 1]),
                &UInt32Array::from(vec![0, 1]),
            )
            .unwrap();
        assert_eq!(merged.num_rows(), 1);
        assert_eq!(
            merged
                .column(2)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(0),
            3
        );
    }

    #[test]
    fn test_partial_update_sequence_group_retracts_aggregate_or_ignores_it_by_option() {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("seq", ArrowDataType::Int64, false),
            ArrowField::new(VALUE_KIND_FIELD_NAME, ArrowDataType::Int8, false),
            ArrowField::new("value", ArrowDataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 1])),
                Arc::new(Int64Array::from(vec![10, 11, 11])),
                Arc::new(Int8Array::from(vec![0, 0, 3])),
                Arc::new(Int32Array::from(vec![Some(100), Some(20), Some(20)])),
            ],
        )
        .unwrap();
        for (ignore_retract, expected) in [(false, 100), (true, 120)] {
            let mut config = test_write_config(MergeEngine::PartialUpdate);
            config
                .table_options
                .insert("fields.seq.sequence-group".into(), "value".into());
            config
                .table_options
                .insert("fields.value.aggregate-function".into(), "sum".into());
            if ignore_retract {
                config
                    .table_options
                    .insert("fields.value.ignore-retract".into(), "true".into());
            }
            let writer =
                KeyValueFileWriter::new(FileIOBuilder::new("memory").build().unwrap(), config, 0)
                    .unwrap();
            let (merged, _) = writer
                .merge_partial_update_rows(
                    &batch,
                    &Int64Array::from(vec![0, 1, 2]),
                    &UInt32Array::from(vec![0, 1, 2]),
                )
                .unwrap();
            assert_eq!(merged.num_rows(), 1);
            assert_eq!(
                merged
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .unwrap()
                    .value(0),
                0
            );
            assert_eq!(
                merged
                    .column(3)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(0),
                expected
            );
        }
    }

    #[tokio::test]
    async fn test_flush_partial_update_ignore_delete_skips_retract_only_batch() {
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
                Arc::new(Int32Array::from(vec![1, 2])) as Arc<dyn arrow_array::Array>,
                Arc::new(Int64Array::from(vec![10, 20])) as Arc<dyn arrow_array::Array>,
                Arc::new(Int8Array::from(vec![1, 3])) as Arc<dyn arrow_array::Array>,
            ],
        )
        .unwrap();
        let mut config = test_write_config(MergeEngine::PartialUpdate);
        config
            .table_options
            .insert("ignore-delete".to_string(), "true".to_string());
        let mut writer =
            KeyValueFileWriter::new(FileIOBuilder::new("memory").build().unwrap(), config, 7)
                .unwrap();

        writer.write(&batch).await.unwrap();
        assert!(writer.buffer.is_empty());
        assert_eq!(writer.buffer_bytes, 0);
        assert_eq!(writer.next_sequence_number, 7);

        let prepared = writer.prepare_commit().await.unwrap();

        assert!(prepared.data_files.is_empty());
        assert!(prepared.changelog_files.is_empty());
        assert_eq!(writer.next_sequence_number, 7);
    }

    #[tokio::test]
    async fn test_flush_partial_update_ignore_delete_filters_data_and_changelog() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Arc::new(ArrowField::new("id", ArrowDataType::Int32, false)),
            Arc::new(ArrowField::new("seq", ArrowDataType::Int64, false)),
            Arc::new(ArrowField::new(
                VALUE_KIND_FIELD_NAME,
                ArrowDataType::Int8,
                false,
            )),
            Arc::new(ArrowField::new("value", ArrowDataType::Int32, true)),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 2])) as Arc<dyn arrow_array::Array>,
                Arc::new(Int64Array::from(vec![10, 20, 30])) as Arc<dyn arrow_array::Array>,
                Arc::new(Int8Array::from(vec![0, 3, 1])) as Arc<dyn arrow_array::Array>,
                Arc::new(Int32Array::from(vec![Some(100), None, None]))
                    as Arc<dyn arrow_array::Array>,
            ],
        )
        .unwrap();
        let mut config = test_write_config(MergeEngine::PartialUpdate);
        config.input_changelog = true;
        config.table_options.insert(
            "partial-update.ignore-delete".to_string(),
            "true".to_string(),
        );
        let mut writer =
            KeyValueFileWriter::new(FileIOBuilder::new("memory").build().unwrap(), config, 7)
                .unwrap();

        writer.write(&batch).await.unwrap();
        let prepared = writer.prepare_commit().await.unwrap();

        assert_eq!(prepared.data_files.len(), 1);
        assert_eq!(prepared.changelog_files.len(), 1);
        for file in prepared
            .data_files
            .iter()
            .chain(prepared.changelog_files.iter())
        {
            assert_eq!(file.row_count, 1);
            assert_eq!(file.min_sequence_number, 7);
            assert_eq!(file.max_sequence_number, 7);
            assert_eq!(file.delete_row_count, Some(0));
        }
        assert_eq!(writer.next_sequence_number, 8);
    }

    #[tokio::test]
    async fn test_flush_partial_update_explicit_false_preserves_singleton_retract() {
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
                Arc::new(Int8Array::from(vec![3])) as Arc<dyn arrow_array::Array>,
            ],
        )
        .unwrap();
        let mut config = test_write_config(MergeEngine::PartialUpdate);
        config
            .table_options
            .insert("ignore-delete".to_string(), "false".to_string());
        let io = FileIOBuilder::new("memory").build().unwrap();
        let mut writer = KeyValueFileWriter::new(io.clone(), config, 0).unwrap();

        writer.write(&batch).await.unwrap();
        let prepared = writer.prepare_commit().await.unwrap();
        let stored = read_kv_file(&io, &prepared.data_files[0]).await;
        assert_eq!(
            stored
                .column_by_name(VALUE_KIND_FIELD_NAME)
                .unwrap()
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(0),
            RowKind::Delete as i8
        );
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

    #[test]
    fn test_flush_partial_update_sequence_group_and_aggregates() {
        let mut config = test_write_config(MergeEngine::PartialUpdate);
        config.table_options.extend([
            (
                "fields.version.sequence-group".into(),
                "price,total,tag".into(),
            ),
            ("fields.total.aggregate-function".into(), "sum".into()),
            ("fields.tag.aggregate-function".into(), "listagg".into()),
        ]);
        config.value_fields = vec![
            DataField::new(0, "id".into(), DataType::Int(IntType::new())),
            DataField::new(1, "arrival".into(), DataType::BigInt(BigIntType::new())),
            DataField::new(2, "version".into(), DataType::BigInt(BigIntType::new())),
            DataField::new(3, "price".into(), DataType::Int(IntType::new())),
            DataField::new(4, "total".into(), DataType::Int(IntType::new())),
            DataField::new(
                5,
                "tag".into(),
                DataType::VarChar(crate::spec::VarCharType::string_type()),
            ),
        ];
        let writer =
            KeyValueFileWriter::new(FileIOBuilder::new("memory").build().unwrap(), config, 0)
                .unwrap();
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("arrival", ArrowDataType::Int64, false),
            ArrowField::new("version", ArrowDataType::Int64, true),
            ArrowField::new("price", ArrowDataType::Int32, true),
            ArrowField::new("total", ArrowDataType::Int32, true),
            ArrowField::new("tag", ArrowDataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 1])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 5, 20])),
                Arc::new(Int32Array::from(vec![100, 50, 200])),
                Arc::new(Int32Array::from(vec![3, 7, 11])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        let (merged, merged_seq) = writer
            .merge_partial_update_rows(
                &batch,
                &Int64Array::from(vec![100, 101, 102]),
                &UInt32Array::from(vec![0, 1, 2]),
            )
            .unwrap();
        assert_eq!(merged.num_rows(), 1);
        assert_eq!(
            merged
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            20,
        );
        assert_eq!(
            merged
                .column(3)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            200,
        );
        assert_eq!(
            merged
                .column(4)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            21,
        );
        assert_eq!(
            merged
                .column(5)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "b,a,c",
        );
        assert_eq!(
            merged_seq
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            102,
        );
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
        let user_converter = RowConverter::new(vec![SortField::new(ArrowDataType::Int64)]).unwrap();
        let user_sequences = user_converter
            .convert_columns(&[Arc::new(seq_col.clone())])
            .unwrap();

        for (group_idx, group_rows) in [vec![0usize, 2, 3], vec![1usize, 4]].iter().enumerate() {
            let rows: Vec<MergeRow> = group_rows
                .iter()
                .map(|&row_idx| MergeRow {
                    batch_idx: 0,
                    row_idx,
                    sequence_number: seq_values[row_idx],
                    value_kind: 0,
                    user_sequence: Some(user_sequences.row(row_idx).owned()),
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

    /// Java's reducer preserves a singleton retract row and its sequence.
    #[test]
    fn test_merge_partial_update_rows_preserves_singleton_retract() {
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

        let (merged, sequence) = partial_update_writer()
            .merge_partial_update_rows(&batch, &seq_array, &sorted_indices)
            .unwrap();
        assert_eq!(merged, batch);
        assert_eq!(
            sequence
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1000
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
            ("fields.price.ignore-delete".to_string(), "true".to_string()),
        ]);

        let err = KeyValueFileWriter::new(FileIOBuilder::new("memory").build().unwrap(), config, 0)
            .err()
            .unwrap();

        assert!(matches!(
            err,
            crate::Error::Unsupported { message }
            if message.contains("fields.price.ignore-delete")
        ));
    }

    #[tokio::test]
    async fn test_flush_aggregation_merges_same_key_across_buffered_batches() {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("value", ArrowDataType::Int32, true),
        ]));
        let mut config = test_write_config(MergeEngine::Aggregation);
        config.sequence_field_indices.clear();
        config.write_buffer_size = i64::MAX;
        config
            .table_options
            .insert("fields.value.aggregate-function".into(), "sum".into());
        let io = FileIOBuilder::new("memory").build().unwrap();
        let mut writer = KeyValueFileWriter::new(io.clone(), config, 7).unwrap();
        for value in [10, 20] {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1])),
                    Arc::new(Int32Array::from(vec![value])),
                ],
            )
            .unwrap();
            writer.write(&batch).await.unwrap();
        }
        let prepared = writer.prepare_commit().await.unwrap();
        assert_eq!(prepared.data_files.len(), 1);
        let file = &prepared.data_files[0];
        assert_eq!(file.row_count, 1);
        assert_eq!((file.min_sequence_number, file.max_sequence_number), (8, 8));
        let physical = read_kv_file(&io, file).await;
        assert_eq!(physical.num_rows(), 1);
        assert_eq!(
            physical
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[30]
        );
    }

    #[tokio::test]
    async fn test_flush_aggregation_sequence_partition_keys_and_input_changelog() {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("p", ArrowDataType::Int32, false),
            ArrowField::new("seq", ArrowDataType::Int64, true),
            ArrowField::new(VALUE_KIND_FIELD_NAME, ArrowDataType::Int8, false),
            ArrowField::new("amount", ArrowDataType::Int32, true),
            ArrowField::new("label", ArrowDataType::Utf8, true),
            ArrowField::new("note", ArrowDataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![2, 1, 1, 1, 2])),
                Arc::new(Int32Array::from(vec![7; 5])),
                Arc::new(Int64Array::from(vec![
                    Some(30),
                    Some(20),
                    Some(10),
                    Some(20),
                    None,
                ])),
                Arc::new(Int8Array::from(vec![0, 2, 0, 2, 0])),
                Arc::new(Int32Array::from(vec![None, Some(10), Some(20), None, None])),
                Arc::new(StringArray::from(vec![
                    "two", "later", "early", "tie", "low",
                ])),
                Arc::new(StringArray::from(vec![
                    None,
                    Some("kept"),
                    None,
                    None,
                    None,
                ])),
            ],
        )
        .unwrap();
        let mut config = test_write_config(MergeEngine::Aggregation);
        // Only id participates in within-partition grouping, but p is also a
        // primary key and must not be summed by the default aggregator.
        config.primary_keys = vec!["id".into(), "p".into()];
        config.sequence_field_indices = vec![2];
        config.write_buffer_size = i64::MAX;
        config.input_changelog = true;
        for (key, value) in [
            ("fields.default-aggregate-function", "sum"),
            ("fields.label.aggregate-function", "listagg"),
            ("fields.note.aggregate-function", "last_non_null_value"),
        ] {
            config.table_options.insert(key.into(), value.into());
        }
        let io = FileIOBuilder::new("memory").build().unwrap();
        let mut writer = KeyValueFileWriter::new(io.clone(), config, 7).unwrap();
        writer.write(&batch.slice(0, 2)).await.unwrap();
        writer.write(&batch.slice(2, 3)).await.unwrap();
        let prepared = writer.prepare_commit().await.unwrap();
        let file = &prepared.data_files[0];
        assert_eq!(file.row_count, 2);
        assert_eq!(
            (file.min_sequence_number, file.max_sequence_number),
            (7, 10)
        );
        assert_eq!(file.delete_row_count, Some(0));
        let physical = read_kv_file(&io, file).await;
        let i32_values = |name| {
            physical
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
        };
        let i64_values = |name| {
            physical
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
        };
        assert_eq!(i32_values("id"), vec![Some(1), Some(2)]);
        assert_eq!(i32_values("p"), vec![Some(7), Some(7)]);
        assert_eq!(i32_values("amount"), vec![Some(30), None]);
        assert_eq!(i64_values("seq"), vec![Some(20), Some(30)]);
        assert_eq!(
            i64_values(SEQUENCE_NUMBER_FIELD_NAME),
            vec![Some(10), Some(7)]
        );
        let labels = physical
            .column_by_name("label")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            labels.iter().collect::<Vec<_>>(),
            vec![Some("early,later,tie"), Some("low,two")]
        );
        let notes = physical
            .column_by_name("note")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(notes.iter().collect::<Vec<_>>(), vec![Some("kept"), None]);
        assert_eq!(
            physical
                .column_by_name(VALUE_KIND_FIELD_NAME)
                .unwrap()
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .values(),
            &[0, 0]
        );
        let changelog = &prepared.changelog_files[0];
        assert_eq!(changelog.row_count, 5);
        let changelog = read_kv_file(&io, changelog).await;
        assert_eq!(
            changelog
                .column_by_name("amount")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(20), Some(10), None, None, None]
        );
        assert_eq!(
            changelog
                .column_by_name(VALUE_KIND_FIELD_NAME)
                .unwrap()
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .values(),
            &[0, 2, 2, 0, 0]
        );
        // Sequence allocation advances over all consumed input, not output rows.
        writer.write(&batch.slice(1, 1)).await.unwrap();
        let next = writer.prepare_commit().await.unwrap();
        assert_eq!(
            (
                next.data_files[0].min_sequence_number,
                next.data_files[0].max_sequence_number
            ),
            (12, 12)
        );
    }

    #[tokio::test]
    async fn test_flush_aggregation_preserves_non_nullable_singleton_retract() {
        for kind in [1, 3] {
            let schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new("id", ArrowDataType::Int32, false),
                ArrowField::new("seq", ArrowDataType::Int64, false),
                ArrowField::new(VALUE_KIND_FIELD_NAME, ArrowDataType::Int8, false),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int32Array::from(vec![1, 2])),
                    Arc::new(Int64Array::from(vec![10, 20])),
                    Arc::new(Int8Array::from(vec![0, kind])),
                ],
            )
            .unwrap();
            let io = FileIOBuilder::new("memory").build().unwrap();
            let mut writer =
                KeyValueFileWriter::new(io.clone(), test_write_config(MergeEngine::Aggregation), 0)
                    .unwrap();
            writer.write(&batch).await.unwrap();
            let prepared = writer.prepare_commit().await.unwrap();
            let stored = read_kv_file(&io, &prepared.data_files[0]).await;
            let kinds = stored
                .column_by_name(VALUE_KIND_FIELD_NAME)
                .unwrap()
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap();
            assert_eq!(kinds.values(), &[0, kind]);
            let seq = stored
                .column_by_name("seq")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(seq.values(), &[10, 20]);
        }
    }

    async fn read_kv_file(io: &FileIO, file: &DataFileMeta) -> RecordBatch {
        let path = format!("memory:/kv-test/bucket-0/{}", file.file_name);
        let data = io.new_input(&path).unwrap().read().await.unwrap();
        let reader =
            parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(data, 1024).unwrap();
        let schema = reader.schema();
        let batches = reader.map(|batch| batch.unwrap()).collect::<Vec<_>>();
        arrow_select::concat::concat_batches(&schema, &batches).unwrap()
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
        config
            .table_options
            .insert("fields.price.ignore-delete".to_string(), "true".to_string());

        let err = KeyValueFileWriter::new(FileIOBuilder::new("memory").build().unwrap(), config, 0)
            .err()
            .unwrap();

        assert!(matches!(
            err,
            crate::Error::Unsupported { message }
            if message.contains("fields.price.ignore-delete")
        ));
    }
}
