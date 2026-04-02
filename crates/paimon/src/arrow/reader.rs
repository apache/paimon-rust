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
use arrow_array::RecordBatch;
use arrow_schema::{Field as ArrowField, Schema as ArrowSchema};

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
        let splits: Vec<DataSplit> = data_splits.to_vec();
        let read_type = self.read_type;
        let projected_column_names: Vec<String> = read_type
            .iter()
            .map(|field| field.name().to_string())
            .collect();
        Ok(try_stream! {
            for split in splits {
                // Create DV factory for this split only (like Java createReader(partition, bucket, files, deletionFiles)).
                let dv_factory = if split
                    .data_deletion_files()
                    .is_some_and(|files| files.iter().any(Option::is_some))
                {
                    Some(
                        DeletionVectorFactory::new(
                            &file_io,
                            split.data_files(),
                            split.data_deletion_files(),
                        )
                        .await?,
                    )
                } else {
                    None
                };

                for file_meta in split.data_files().to_vec() {
                    let dv = dv_factory
                        .as_ref()
                        .and_then(|factory| factory.get_deletion_vector(&file_meta.file_name))
                        .cloned();

                    let mut stream = read_single_file_stream(
                        file_io.clone(),
                        split.clone(),
                        file_meta,
                        projected_column_names.clone(),
                        batch_size,
                        dv,
                    )?;
                    while let Some(batch) = stream.next().await {
                        yield batch?;
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
            for split in splits {
                if split.raw_convertible() || split.data_files().len() == 1 {
                    // Single file or raw convertible — stream lazily without loading all into memory.
                    for file_meta in split.data_files().to_vec() {
                        let mut stream = read_single_file_stream(
                            file_io.clone(), split.clone(), file_meta, projected_column_names.clone(), batch_size, None,
                        )?;
                        while let Some(batch) = stream.next().await {
                            yield batch?;
                        }
                    }
                } else {
                    // Multiple files need column-wise merge — also streamed lazily.
                    let mut merge_stream = merge_files_by_columns(
                        &file_io,
                        &split,
                        &projected_column_names,
                        &table_field_names,
                        batch_size,
                    )?;
                    while let Some(batch) = merge_stream.next().await {
                        yield batch?;
                    }
                }
            }
        }
        .boxed())
    }
}

/// Read a single parquet file from a split, returning a lazy stream of batches.
/// Optionally applies a deletion vector.
fn read_single_file_stream(
    file_io: FileIO,
    split: DataSplit,
    file_meta: DataFileMeta,
    projected_column_names: Vec<String>,
    batch_size: Option<usize>,
    dv: Option<Arc<DeletionVector>>,
) -> crate::Result<ArrowRecordBatchStream> {
    Ok(try_stream! {
        let path_to_read = split.data_file_path(&file_meta);
        if !path_to_read.to_ascii_lowercase().ends_with(".parquet") {
            Err(Error::Unsupported {
                message: format!(
                    "unsupported file format: only .parquet is supported, got: {path_to_read}"
                ),
            })?
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

        let mask = ProjectionMask::columns(&parquet_schema, available_columns.iter().copied());
        batch_stream_builder = batch_stream_builder.with_projection(mask);

        if let Some(ref dv) = dv {
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
            // Reorder columns from parquet-schema order to projected_column_names order,
            // consistent with the normal read() path.
            let reorder_indices: Vec<usize> = projected_column_names
                .iter()
                .filter_map(|name| batch.schema().index_of(name).ok())
                .collect();
            if reorder_indices.len() == batch.num_columns() {
                yield batch.project(&reorder_indices).map_err(|e| {
                    Error::UnexpectedError {
                        message: "Failed to reorder projected columns".to_string(),
                        source: Some(Box::new(e)),
                    }
                })?;
            } else {
                // Not all projected columns exist in this file (data evolution case),
                // return as-is; the caller (merge_files_by_columns) handles missing columns.
                yield batch;
            }
        }
    }
    .boxed())
}

/// Merge multiple files column-wise for data evolution, streaming with bounded memory.
///
/// Opens all file readers simultaneously and maintains a cursor (current batch + offset)
/// per file. Each poll slices up to `batch_size` rows from each file's current batch,
/// assembles columns from the winning files, and yields the merged batch. When a file's
/// current batch is exhausted, the next batch is read from its stream on demand.
fn merge_files_by_columns(
    file_io: &FileIO,
    split: &DataSplit,
    projected_column_names: &[String],
    table_field_names: &[String],
    batch_size: Option<usize>,
) -> crate::Result<ArrowRecordBatchStream> {
    let data_files = split.data_files();
    if data_files.is_empty() {
        return Ok(futures::stream::empty().boxed());
    }

    // Determine which columns each file provides and resolve conflicts by max_sequence_number.
    // column_name -> (file_index, max_sequence_number)
    let mut column_source: HashMap<String, (usize, i64)> = HashMap::new();

    for (file_idx, file_meta) in data_files.iter().enumerate() {
        let file_columns: Vec<String> = if let Some(ref wc) = file_meta.write_cols {
            wc.clone()
        } else {
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

    // For each projected column, record (file_index, column_name) for assembly.
    let column_plan: Vec<(Option<usize>, String)> = projected_column_names
        .iter()
        .map(|col_name| {
            let file_idx = column_source.get(col_name).map(|&(idx, _)| idx);
            (file_idx, col_name.clone())
        })
        .collect();

    // Collect which file indices we need to open streams for.
    let active_file_indices: Vec<usize> = file_read_columns.keys().copied().collect();

    // Build owned data for the stream closure.
    let file_io = file_io.clone();
    let split = split.clone();
    let data_files: Vec<DataFileMeta> = data_files.to_vec();
    let projected_column_names = projected_column_names.to_vec();
    let output_batch_size = batch_size.unwrap_or(1024);

    Ok(try_stream! {
        // Open a stream for each active file.
        let mut file_streams: HashMap<usize, ArrowRecordBatchStream> = HashMap::new();
        for &file_idx in &active_file_indices {
            let stream = read_single_file_stream(
                file_io.clone(),
                split.clone(),
                data_files[file_idx].clone(),
                projected_column_names.clone(),
                batch_size,
                None,
            )?;
            file_streams.insert(file_idx, stream);
        }

        // Per-file cursor: current batch + offset within it.
        let mut file_cursors: HashMap<usize, (RecordBatch, usize)> = HashMap::new();

        loop {
            // Ensure each active file has a current batch. If a file's cursor is exhausted
            // or not yet initialized, read the next batch from its stream.
            for &file_idx in &active_file_indices {
                let needs_next = match file_cursors.get(&file_idx) {
                    None => true,
                    Some((batch, offset)) => *offset >= batch.num_rows(),
                };
                if needs_next {
                    file_cursors.remove(&file_idx);
                    if let Some(stream) = file_streams.get_mut(&file_idx) {
                        if let Some(batch_result) = stream.next().await {
                            let batch = batch_result?;
                            if batch.num_rows() > 0 {
                                file_cursors.insert(file_idx, (batch, 0));
                            }
                        }
                    }
                }
            }

            // All active files must have a cursor to assemble a valid row.
            // If any file has no cursor (stream exhausted), we're done.
            if active_file_indices.iter().any(|idx| !file_cursors.contains_key(idx)) {
                break;
            }

            // Determine how many rows we can emit: min of remaining rows across all files.
            let remaining: usize = active_file_indices
                .iter()
                .map(|idx| {
                    let (batch, offset) = file_cursors.get(idx).unwrap();
                    batch.num_rows() - offset
                })
                .min()
                .unwrap_or(0);

            if remaining == 0 {
                break;
            }

            let rows_to_emit = remaining.min(output_batch_size);

            // Slice each file's current batch and assemble columns.
            let mut columns: Vec<Arc<dyn arrow_array::Array>> =
                Vec::with_capacity(column_plan.len());
            let mut schema_fields: Vec<ArrowField> = Vec::with_capacity(column_plan.len());

            for (file_idx_opt, col_name) in &column_plan {
                if let Some(file_idx) = file_idx_opt {
                    if let Some((batch, offset)) = file_cursors.get(file_idx) {
                        if let Ok(col_idx) = batch.schema().index_of(col_name) {
                            let col = batch.column(col_idx).slice(*offset, rows_to_emit);
                            columns.push(col);
                            schema_fields.push(batch.schema().field(col_idx).clone());
                        }
                    }
                }
            }

            // Advance all cursors.
            for &file_idx in &active_file_indices {
                if let Some((_, ref mut offset)) = file_cursors.get_mut(&file_idx) {
                    *offset += rows_to_emit;
                }
            }

            if !columns.is_empty() {
                let schema = Arc::new(ArrowSchema::new(schema_fields));
                let merged = RecordBatch::try_new(schema, columns).map_err(|e| Error::UnexpectedError {
                    message: format!("Failed to build merged RecordBatch: {e}"),
                    source: Some(Box::new(e)),
                })?;
                yield merged;
            }
        }
    }
    .boxed())
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
