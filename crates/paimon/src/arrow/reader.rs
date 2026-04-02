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

use crate::deletion_vector::{DeletionVector, DeletionVectorFactory};
use crate::io::{FileIO, FileRead, FileStatus};
use crate::spec::{DataField, DataFileMeta};
use crate::table::ArrowRecordBatchStream;
use crate::{DataSplit, Error};
use arrow_array::{new_null_array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use arrow_select::concat::concat_batches;
use async_stream::try_stream;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{StreamExt, TryFutureExt};
use parquet::arrow::arrow_reader::{ArrowReaderOptions, RowSelection, RowSelector};
use parquet::arrow::async_reader::{AsyncFileReader, MetadataFetch};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use tokio::try_join;

/// Builder to create ArrowReader
pub struct ArrowReaderBuilder {
    batch_size: Option<usize>,
    file_io: FileIO,
}

impl ArrowReaderBuilder {
    /// Create a new ArrowReaderBuilder
    pub(crate) fn new(file_io: FileIO) -> Self {
        ArrowReaderBuilder {
            batch_size: None,
            file_io,
        }
    }

    /// Build the ArrowReader with the given read type (logical row type or projected subset).
    /// Used to clip Parquet schema to requested columns only.
    pub fn build(self, read_type: Vec<DataField>) -> ArrowReader {
        ArrowReader {
            batch_size: self.batch_size,
            file_io: self.file_io,
            read_type,
        }
    }
}

/// Reads data from Parquet files
#[derive(Clone)]
pub struct ArrowReader {
    batch_size: Option<usize>,
    file_io: FileIO,
    read_type: Vec<DataField>,
}

impl ArrowReader {
    /// Take a stream of DataSplits and read every data file in each split.
    /// Returns a stream of Arrow RecordBatches from all files.
    /// When a split has deletion files (see [DataSplit::data_deletion_files]), the corresponding
    /// deletion vectors are loaded and applied so that deleted rows are filtered out from the stream.
    /// Row positions are 0-based within each data file, matching Java's ApplyDeletionVectorReader.
    ///
    /// Matches [RawFileSplitRead.createReader](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/operation/RawFileSplitRead.java):
    /// one DV factory per DataSplit (created from that split's data files and deletion files).
    ///
    /// Parquet schema is clipped to this reader's read type (column names from [DataField]s).
    /// File-only columns are not read. See [ParquetReaderFactory.clipParquetSchema](https://github.com/apache/paimon/blob/master/paimon-format/paimon-format-common/src/main/java/org/apache/paimon/format/FormatReaderFactory.java).
    pub fn read(self, data_splits: &[DataSplit]) -> crate::Result<ArrowRecordBatchStream> {
        let file_io = self.file_io.clone();
        let batch_size = self.batch_size;
        // Owned list of splits so the stream does not hold references.
        let splits: Vec<DataSplit> = data_splits.to_vec();
        let read_type = self.read_type;
        let projected_column_names: Vec<String> = read_type
            .iter()
            .map(|field| field.name().to_string())
            .collect();
        Ok(try_stream! {
            for split in splits {
                // Create DV factory for this split only (like Java createReader(partition, bucket, files, deletionFiles)).
                let core_data_files = split.data_files();
                let dv_factory = if split
                    .data_deletion_files()
                    .is_some_and(|files| files.iter().any(Option::is_some))
                {
                    Some(
                        DeletionVectorFactory::new(
                            &file_io,
                            core_data_files,
                            split.data_deletion_files(),
                        )
                        .await?,
                    )
                } else {
                    None
                };

                for file_meta in core_data_files {
                    let path_to_read = split.data_file_path(file_meta);
                    if !path_to_read.to_ascii_lowercase().ends_with(".parquet") {
                        Err(Error::Unsupported {
                            message: format!(
                                "unsupported file format: only .parquet is supported, got: {path_to_read}"
                            ),
                        })?
                    }
                    let dv = dv_factory
                        .as_ref()
                        .and_then(|factory| factory.get_deletion_vector(&file_meta.file_name));

                    let parquet_file = file_io.new_input(&path_to_read)?;
                    let (parquet_metadata, parquet_reader) = try_join!(
                        parquet_file.metadata(),
                        parquet_file.reader()
                    )?;
                    let arrow_file_reader = ArrowFileReader::new(parquet_metadata, parquet_reader);

                    let mut batch_stream_builder =
                        ParquetRecordBatchStreamBuilder::new(arrow_file_reader)
                            .await?;
                    // ProjectionMask preserves parquet-schema order; read_type order is restored below.
                    let mask = {
                        let parquet_schema = batch_stream_builder.parquet_schema();
                        ProjectionMask::columns(
                            parquet_schema,
                            projected_column_names.iter().map(String::as_str),
                        )
                    };
                    batch_stream_builder = batch_stream_builder.with_projection(mask);

                    if let Some(dv) = dv {
                        if !dv.is_empty() {
                            let row_selection =
                                build_deletes_row_selection(batch_stream_builder.metadata().row_groups(), dv)?;
                            batch_stream_builder = batch_stream_builder.with_row_selection(row_selection);
                        }
                    }
                    if let Some(size) = batch_size {
                        batch_stream_builder = batch_stream_builder.with_batch_size(size);
                    }
                    let mut batch_stream = batch_stream_builder.build()?;

                    while let Some(batch) = batch_stream.next().await {
                        let batch = batch?;
                        // Reorder columns from parquet-schema order to read_type order.
                        // Every projected column must exist in the batch; a missing
                        // column indicates schema mismatch and must not be silenced.
                        let reorder_indices: Vec<usize> = projected_column_names
                            .iter()
                            .map(|name| {
                                batch.schema().index_of(name).map_err(|_| {
                                    Error::UnexpectedError {
                                        message: format!(
                                            "Projected column '{name}' not found in Parquet batch schema of file {path_to_read}"
                                        ),
                                        source: None,
                                    }
                                })
                            })
                            .collect::<crate::Result<Vec<_>>>()?;
                        yield batch.project(&reorder_indices).map_err(|e| {
                            Error::UnexpectedError {
                                message: "Failed to reorder projected columns".to_string(),
                                source: Some(Box::new(e)),
                            }
                        })?;
                    }
                }
            }
        }
            .boxed())
    }

    /// Read data files in data evolution mode, merging columns from files that share the same row ID range.
    ///
    /// Each DataSplit contains files grouped by `first_row_id`. Files within a split may contain
    /// different columns for the same logical rows. This method reads each file and merges them
    /// column-wise, respecting `max_sequence_number` for conflict resolution.
    ///
    /// `table_fields` is the full table schema fields, used to determine which columns each file
    /// provides when `write_cols` is not set.
    pub fn read_data_evolution(
        self,
        data_splits: &[DataSplit],
        table_fields: &[DataField],
    ) -> crate::Result<ArrowRecordBatchStream> {
        let file_io = self.file_io.clone();
        let batch_size = self.batch_size;
        let splits: Vec<DataSplit> = data_splits.to_vec();
        let read_type = self.read_type;
        let table_field_names: Vec<String> =
            table_fields.iter().map(|f| f.name().to_string()).collect();
        let projected_column_names: Vec<String> = read_type
            .iter()
            .map(|field| field.name().to_string())
            .collect();

        Ok(try_stream! {
            for split in &splits {
                if split.raw_convertible() || split.data_files().len() == 1 {
                    // Single file or raw convertible — read normally.
                    for file_meta in split.data_files() {
                        let batches = read_single_file(
                            &file_io, split, file_meta, &projected_column_names, batch_size, None,
                        ).await?;
                        for batch in batches {
                            yield batch;
                        }
                    }
                } else {
                    // Multiple files need column-wise merge.
                    let merged_batches = merge_files_by_columns(
                        &file_io,
                        split,
                        &projected_column_names,
                        &table_field_names,
                        batch_size,
                    ).await?;
                    for batch in merged_batches {
                        yield batch;
                    }
                }
            }
        }
        .boxed())
    }
}

/// Read a single parquet file from a split, returning all batches.
/// Optionally applies a deletion vector.
async fn read_single_file(
    file_io: &FileIO,
    split: &DataSplit,
    file_meta: &DataFileMeta,
    projected_column_names: &[String],
    batch_size: Option<usize>,
    dv: Option<&DeletionVector>,
) -> crate::Result<Vec<RecordBatch>> {
    let path_to_read = split.data_file_path(file_meta);
    if !path_to_read.to_ascii_lowercase().ends_with(".parquet") {
        return Err(Error::Unsupported {
            message: format!(
                "unsupported file format: only .parquet is supported, got: {path_to_read}"
            ),
        });
    }

    let parquet_file = file_io.new_input(&path_to_read)?;
    let (parquet_metadata, parquet_reader) =
        try_join!(parquet_file.metadata(), parquet_file.reader())?;
    let arrow_file_reader = ArrowFileReader::new(parquet_metadata, parquet_reader);

    let mut batch_stream_builder = ParquetRecordBatchStreamBuilder::new(arrow_file_reader).await?;

    // Only project columns that exist in this file.
    let parquet_schema = batch_stream_builder.parquet_schema().clone();
    let file_column_names: Vec<&str> = parquet_schema.columns().iter().map(|c| c.name()).collect();
    let available_columns: Vec<&str> = projected_column_names
        .iter()
        .filter(|name| file_column_names.contains(&name.as_str()))
        .map(String::as_str)
        .collect();

    if available_columns.is_empty() {
        return Ok(Vec::new());
    }

    let mask = ProjectionMask::columns(&parquet_schema, available_columns.iter().copied());
    batch_stream_builder = batch_stream_builder.with_projection(mask);

    if let Some(dv) = dv {
        if !dv.is_empty() {
            let row_selection =
                build_deletes_row_selection(batch_stream_builder.metadata().row_groups(), dv)?;
            batch_stream_builder = batch_stream_builder.with_row_selection(row_selection);
        }
    }
    if let Some(size) = batch_size {
        batch_stream_builder = batch_stream_builder.with_batch_size(size);
    }

    let mut batch_stream = batch_stream_builder.build()?;
    let mut batches = Vec::new();
    while let Some(batch) = batch_stream.next().await {
        batches.push(batch?);
    }
    Ok(batches)
}

/// Merge multiple files column-wise for data evolution.
///
/// All files in the split share the same `first_row_id` and `row_count`.
/// Each file contributes a subset of columns. When multiple files provide the same column,
/// the file with the higher `max_sequence_number` wins.
async fn merge_files_by_columns(
    file_io: &FileIO,
    split: &DataSplit,
    projected_column_names: &[String],
    table_field_names: &[String],
    batch_size: Option<usize>,
) -> crate::Result<Vec<RecordBatch>> {
    let data_files = split.data_files();
    if data_files.is_empty() {
        return Ok(Vec::new());
    }

    // Determine which columns each file provides and resolve conflicts by max_sequence_number.
    // column_name -> (file_index, max_sequence_number)
    let mut column_source: HashMap<String, (usize, i64)> = HashMap::new();

    for (file_idx, file_meta) in data_files.iter().enumerate() {
        let file_columns: Vec<String> = if let Some(ref wc) = file_meta.write_cols {
            wc.clone()
        } else {
            // File written before data evolution — contains all table columns at that schema version.
            table_field_names.to_vec()
        };

        for col in &file_columns {
            let entry = column_source
                .entry(col.clone())
                .or_insert((file_idx, i64::MIN));
            if file_meta.max_sequence_number > entry.1 {
                *entry = (file_idx, file_meta.max_sequence_number);
            }
        }
    }

    // For each file, determine which projected columns to read from it.
    // file_index -> Vec<column_name>
    let mut file_read_columns: HashMap<usize, Vec<String>> = HashMap::new();
    for col_name in projected_column_names {
        if let Some(&(file_idx, _)) = column_source.get(col_name) {
            file_read_columns
                .entry(file_idx)
                .or_default()
                .push(col_name.clone());
        }
    }

    // Read each file that contributes columns.
    let mut file_batches: HashMap<usize, Vec<RecordBatch>> = HashMap::new();
    for file_idx in file_read_columns.keys() {
        let file_meta = &data_files[*file_idx];
        let batches = read_single_file(
            file_io,
            split,
            file_meta,
            projected_column_names,
            batch_size,
            None,
        )
        .await?;
        file_batches.insert(*file_idx, batches);
    }

    // Concatenate all batches per file into a single RecordBatch.
    let mut file_concat: HashMap<usize, RecordBatch> = HashMap::new();
    for (file_idx, batches) in &file_batches {
        if batches.is_empty() {
            continue;
        }
        let schema = batches[0].schema();
        let concat = concat_batches(&schema, batches).map_err(|e| Error::UnexpectedError {
            message: format!("Failed to concatenate batches for file index {file_idx}: {e}"),
            source: Some(Box::new(e)),
        })?;
        file_concat.insert(*file_idx, concat);
    }

    // Determine the total row count from any file.
    let row_count = file_concat
        .values()
        .next()
        .map(|b| b.num_rows())
        .unwrap_or(0);

    if row_count == 0 {
        return Ok(Vec::new());
    }

    // Build the merged RecordBatch: for each projected column, pick from the winning file
    // or fill with nulls.
    let mut columns: Vec<Arc<dyn arrow_array::Array>> =
        Vec::with_capacity(projected_column_names.len());
    let mut schema_fields: Vec<ArrowField> = Vec::with_capacity(projected_column_names.len());

    for col_name in projected_column_names {
        if let Some(&(file_idx, _)) = column_source.get(col_name) {
            if let Some(concat_batch) = file_concat.get(&file_idx) {
                if let Ok(col_idx) = concat_batch.schema().index_of(col_name) {
                    columns.push(concat_batch.column(col_idx).clone());
                    schema_fields.push(concat_batch.schema().field(col_idx).clone());
                    continue;
                }
            }
        }
        // Column not found in any file — fill with nulls.
        let null_type =
            find_column_type_from_batches(&file_concat, col_name).unwrap_or(ArrowDataType::Utf8);
        let null_array = new_null_array(&null_type, row_count);
        schema_fields.push(ArrowField::new(col_name, null_type, true));
        columns.push(null_array);
    }

    let schema = Arc::new(ArrowSchema::new(schema_fields));
    let merged = RecordBatch::try_new(schema, columns).map_err(|e| Error::UnexpectedError {
        message: format!("Failed to build merged RecordBatch: {e}"),
        source: Some(Box::new(e)),
    })?;

    Ok(vec![merged])
}

/// Find the Arrow data type for a column name from any available concatenated batch.
fn find_column_type_from_batches(
    file_concat: &HashMap<usize, RecordBatch>,
    col_name: &str,
) -> Option<ArrowDataType> {
    for batch in file_concat.values() {
        if let Ok(idx) = batch.schema().index_of(col_name) {
            return Some(batch.schema().field(idx).data_type().clone());
        }
    }
    None
}

/// Builds a Parquet [RowSelection] from deletion vector.
/// Only rows not in the deletion vector are selected; deleted rows are skipped at read time.
/// todo: Uses [DeletionVectorIterator] with [advance_to](DeletionVectorIterator::advance_to) when skipping row groups similar to iceberg-rust
fn build_deletes_row_selection(
    row_group_metadata_list: &[RowGroupMetaData],
    deletion_vector: &DeletionVector,
) -> crate::Result<RowSelection> {
    let mut delete_iter = deletion_vector.iter();

    let mut results: Vec<RowSelector> = Vec::new();
    let mut current_row_group_base_idx: u64 = 0;
    let mut next_deleted_row_idx_opt = delete_iter.next();

    for row_group_metadata in row_group_metadata_list {
        let row_group_num_rows = row_group_metadata.num_rows() as u64;
        let next_row_group_base_idx = current_row_group_base_idx + row_group_num_rows;

        let mut next_deleted_row_idx = match next_deleted_row_idx_opt {
            Some(next_deleted_row_idx) => {
                if next_deleted_row_idx >= next_row_group_base_idx {
                    results.push(RowSelector::select(row_group_num_rows as usize));
                    current_row_group_base_idx += row_group_num_rows;
                    continue;
                }
                next_deleted_row_idx
            }
            None => {
                results.push(RowSelector::select(row_group_num_rows as usize));
                current_row_group_base_idx += row_group_num_rows;
                continue;
            }
        };

        let mut current_idx = current_row_group_base_idx;
        'chunks: while next_deleted_row_idx < next_row_group_base_idx {
            if current_idx < next_deleted_row_idx {
                let run_length = next_deleted_row_idx - current_idx;
                results.push(RowSelector::select(run_length as usize));
                current_idx += run_length;
            }
            let mut run_length = 0u64;
            while next_deleted_row_idx == current_idx
                && next_deleted_row_idx < next_row_group_base_idx
            {
                run_length += 1;
                current_idx += 1;
                next_deleted_row_idx_opt = delete_iter.next();
                next_deleted_row_idx = match next_deleted_row_idx_opt {
                    Some(v) => v,
                    None => {
                        results.push(RowSelector::skip(run_length as usize));
                        break 'chunks;
                    }
                };
            }
            if run_length > 0 {
                results.push(RowSelector::skip(run_length as usize));
            }
        }
        if current_idx < next_row_group_base_idx {
            results.push(RowSelector::select(
                (next_row_group_base_idx - current_idx) as usize,
            ));
        }
        current_row_group_base_idx += row_group_num_rows;
    }

    Ok(results.into())
}

/// ArrowFileReader is a wrapper around a FileRead that impls parquets AsyncFileReader.
///
/// # TODO
///
/// [ParquetObjectReader](https://docs.rs/parquet/latest/src/parquet/arrow/async_reader/store.rs.html#64)
/// contains the following hints to speed up metadata loading, similar to iceberg, we can consider adding them to this struct:
///
/// - `metadata_size_hint`: Provide a hint as to the size of the parquet file's footer.
/// - `preload_column_index`: Load the Column Index  as part of [`Self::get_metadata`].
/// - `preload_offset_index`: Load the Offset Index as part of [`Self::get_metadata`].
struct ArrowFileReader<R: FileRead> {
    meta: FileStatus,
    r: R,
}

impl<R: FileRead> ArrowFileReader<R> {
    /// Create a new ArrowFileReader
    fn new(meta: FileStatus, r: R) -> Self {
        Self { meta, r }
    }

    fn read_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        Box::pin(self.r.read(range.start..range.end).map_err(|err| {
            let err_msg = format!("{err}");
            parquet::errors::ParquetError::External(err_msg.into())
        }))
    }
}

impl<R: FileRead> MetadataFetch for ArrowFileReader<R> {
    fn fetch(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.read_bytes(range)
    }
}

impl<R: FileRead> AsyncFileReader for ArrowFileReader<R> {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.read_bytes(range)
    }

    fn get_metadata(
        &mut self,
        options: Option<&ArrowReaderOptions>,
    ) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        let metadata_opts = options.map(|o| o.metadata_options().clone());
        Box::pin(async move {
            let file_size = self.meta.size;
            let metadata = ParquetMetaDataReader::new()
                .with_metadata_options(metadata_opts)
                .load_and_finish(self, file_size)
                .await?;
            Ok(Arc::new(metadata))
        })
    }
}
