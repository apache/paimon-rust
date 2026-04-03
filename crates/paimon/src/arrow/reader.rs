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

use crate::arrow::build_target_arrow_schema;
use crate::arrow::schema_evolution::{create_index_mapping, NULL_FIELD_INDEX};
use crate::deletion_vector::{DeletionVector, DeletionVectorFactory};
use crate::io::{FileIO, FileRead, FileStatus};
use crate::spec::{DataField, DataFileMeta};
use crate::table::schema_manager::SchemaManager;
use crate::table::ArrowRecordBatchStream;
use crate::{DataSplit, Error};
use arrow_array::RecordBatch;
use arrow_cast::cast;

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
    schema_manager: SchemaManager,
    table_schema_id: i64,
}

impl ArrowReaderBuilder {
    /// Create a new ArrowReaderBuilder
    pub(crate) fn new(
        file_io: FileIO,
        schema_manager: SchemaManager,
        table_schema_id: i64,
    ) -> Self {
        ArrowReaderBuilder {
            batch_size: None,
            file_io,
            schema_manager,
            table_schema_id,
        }
    }

    /// Build the ArrowReader with the given read type (logical row type or projected subset).
    /// Used to clip Parquet schema to requested columns only.
    pub fn build(self, read_type: Vec<DataField>) -> ArrowReader {
        ArrowReader {
            batch_size: self.batch_size,
            file_io: self.file_io,
            schema_manager: self.schema_manager,
            table_schema_id: self.table_schema_id,
            read_type,
        }
    }
}

/// Reads data from Parquet files
#[derive(Clone)]
pub struct ArrowReader {
    batch_size: Option<usize>,
    file_io: FileIO,
    schema_manager: SchemaManager,
    table_schema_id: i64,
    read_type: Vec<DataField>,
}

impl ArrowReader {
    /// Take a stream of DataSplits and read every data file in each split.
    /// Returns a stream of Arrow RecordBatches from all files.
    ///
    /// Uses SchemaManager to load the data file's schema (via `DataFileMeta.schema_id`)
    /// and computes field-ID-based index mapping for schema evolution (added columns,
    /// type promotion, column reordering).
    ///
    /// Matches [RawFileSplitRead.createReader](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/operation/RawFileSplitRead.java).
    pub fn read(self, data_splits: &[DataSplit]) -> crate::Result<ArrowRecordBatchStream> {
        let file_io = self.file_io.clone();
        let batch_size = self.batch_size;
        let splits: Vec<DataSplit> = data_splits.to_vec();
        let read_type = self.read_type;
        let schema_manager = self.schema_manager;
        let table_schema_id = self.table_schema_id;
        Ok(try_stream! {
            for split in splits {
                // Create DV factory for this split only.
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

                    // Load data file's schema if it differs from the table schema.
                    let data_fields: Option<Vec<DataField>> = if file_meta.schema_id != table_schema_id {
                        let data_schema = schema_manager.schema(file_meta.schema_id).await?;
                        Some(data_schema.fields().to_vec())
                    } else {
                        None
                    };

                    let mut stream = read_single_file_stream(
                        file_io.clone(),
                        split.clone(),
                        file_meta,
                        read_type.clone(),
                        data_fields,
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
        let table_fields: Vec<DataField> = table_fields.to_vec();
        let schema_manager = self.schema_manager;
        let table_schema_id = self.table_schema_id;

        Ok(try_stream! {
            for split in splits {
                if split.raw_convertible() || split.data_files().len() == 1 {
                    for file_meta in split.data_files().to_vec() {
                        let data_fields: Option<Vec<DataField>> = if file_meta.schema_id != table_schema_id {
                            let data_schema = schema_manager.schema(file_meta.schema_id).await?;
                            Some(data_schema.fields().to_vec())
                        } else {
                            None
                        };

                        let mut stream = read_single_file_stream(
                            file_io.clone(), split.clone(), file_meta, read_type.clone(),
                            data_fields, batch_size, None,
                        )?;
                        while let Some(batch) = stream.next().await {
                            yield batch?;
                        }
                    }
                } else {
                    // Multiple files need column-wise merge.
                    let mut merge_stream = merge_files_by_columns(
                        &file_io,
                        &split,
                        &read_type,
                        &table_fields,
                        schema_manager.clone(),
                        table_schema_id,
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
///
/// Handles schema evolution using field-ID-based index mapping:
/// - `data_fields`: if `Some`, the fields from the data file's schema (loaded via SchemaManager).
///   Used to compute index mapping between `read_type` and data fields by field ID.
/// - Columns missing from the file are filled with null arrays.
/// - Columns whose Arrow type differs from the target type are cast (type promotion).
///
/// Reference: [RawFileSplitRead.createFileReader](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/operation/RawFileSplitRead.java)
fn read_single_file_stream(
    file_io: FileIO,
    split: DataSplit,
    file_meta: DataFileMeta,
    read_type: Vec<DataField>,
    data_fields: Option<Vec<DataField>>,
    batch_size: Option<usize>,
    dv: Option<Arc<DeletionVector>>,
) -> crate::Result<ArrowRecordBatchStream> {
    let target_schema = build_target_arrow_schema(&read_type)?;

    // Compute index mapping and determine which columns to read from the parquet file.
    // If data_fields is provided, use field-ID-based mapping; otherwise use read_type names directly.
    let (parquet_read_fields, index_mapping) = if let Some(ref df) = data_fields {
        let mapping = create_index_mapping(&read_type, df);
        match mapping {
            Some(ref idx_map) => {
                // Only read data fields that are referenced by the index mapping.
                // Dedup by data field index to avoid duplicate parquet column projections.
                let mut seen = std::collections::HashSet::new();
                let fields_to_read: Vec<DataField> = idx_map
                    .iter()
                    .filter(|&&idx| idx != NULL_FIELD_INDEX && seen.insert(idx))
                    .map(|&idx| df[idx as usize].clone())
                    .collect();
                (fields_to_read, Some(idx_map.clone()))
            }
            None => {
                // Identity mapping — read data fields in order.
                (df.clone(), None)
            }
        }
    } else {
        // No schema evolution — read by read_type names.
        (read_type.clone(), None)
    };

    let parquet_column_names: Vec<String> = parquet_read_fields
        .iter()
        .map(|f| f.name().to_string())
        .collect();

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
        let available_columns: Vec<&str> = parquet_column_names
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
            let num_rows = batch.num_rows();
            let batch_schema = batch.schema();

            // Build output columns using index mapping (field-ID-based) or by name.
            let mut columns: Vec<Arc<dyn arrow_array::Array>> = Vec::with_capacity(target_schema.fields().len());
            for (i, target_field) in target_schema.fields().iter().enumerate() {
                let source_col = if let Some(ref idx_map) = index_mapping {
                    let data_idx = idx_map[i];
                    if data_idx == NULL_FIELD_INDEX {
                        None
                    } else {
                        // Find the column in the batch by the data field's name.
                        let data_field = &data_fields.as_ref().unwrap()[data_idx as usize];
                        batch_schema
                            .index_of(data_field.name())
                            .ok()
                            .map(|col_idx| batch.column(col_idx))
                    }
                } else if let Some(ref df) = data_fields {
                    // Identity mapping with data_fields present (e.g. renamed column).
                    // Use data field name (old name in parquet) at the same position.
                    batch_schema
                        .index_of(df[i].name())
                        .ok()
                        .map(|col_idx| batch.column(col_idx))
                } else {
                    // No schema evolution — look up by target field name.
                    batch_schema
                        .index_of(target_field.name())
                        .ok()
                        .map(|col_idx| batch.column(col_idx))
                };

                match source_col {
                    Some(col) => {
                        if col.data_type() == target_field.data_type() {
                            columns.push(col.clone());
                        } else {
                            // Type promotion: cast to target type.
                            let casted = cast(col, target_field.data_type()).map_err(|e| {
                                Error::UnexpectedError {
                                    message: format!(
                                        "Failed to cast column '{}' from {:?} to {:?}: {e}",
                                        target_field.name(),
                                        col.data_type(),
                                        target_field.data_type()
                                    ),
                                    source: Some(Box::new(e)),
                                }
                            })?;
                            columns.push(casted);
                        }
                    }
                    None => {
                        // Column missing from file: fill with nulls.
                        let null_array = arrow_array::new_null_array(target_field.data_type(), num_rows);
                        columns.push(null_array);
                    }
                }
            }

            let result = if columns.is_empty() {
                RecordBatch::try_new_with_options(
                    target_schema.clone(),
                    columns,
                    &arrow_array::RecordBatchOptions::new().with_row_count(Some(num_rows)),
                )
            } else {
                RecordBatch::try_new(target_schema.clone(), columns)
            }
            .map_err(|e| {
                Error::UnexpectedError {
                    message: format!("Failed to build schema-evolved RecordBatch: {e}"),
                    source: Some(Box::new(e)),
                }
            })?;
            yield result;
        }
    }
    .boxed())
}

/// Merge multiple files column-wise for data evolution, streaming with bounded memory.
///
/// Uses field IDs (not column names) to resolve which file provides which column,
/// ensuring correctness across schema evolution (column rename, add, drop).
///
/// Opens all file readers simultaneously and maintains a cursor (current batch + offset)
/// per file. Each poll slices up to `batch_size` rows from each file's current batch,
/// assembles columns from the winning files, and yields the merged batch. When a file's
/// current batch is exhausted, the next batch is read from its stream on demand.
fn merge_files_by_columns(
    file_io: &FileIO,
    split: &DataSplit,
    read_type: &[DataField],
    table_fields: &[DataField],
    schema_manager: SchemaManager,
    table_schema_id: i64,
    batch_size: Option<usize>,
) -> crate::Result<ArrowRecordBatchStream> {
    let data_files = split.data_files();
    if data_files.is_empty() {
        return Ok(futures::stream::empty().boxed());
    }

    // Build owned data for the stream closure.
    let file_io = file_io.clone();
    let split = split.clone();
    let data_files: Vec<DataFileMeta> = data_files.to_vec();
    let read_type = read_type.to_vec();
    let table_fields = table_fields.to_vec();
    let output_batch_size = batch_size.unwrap_or(1024);
    let target_schema = build_target_arrow_schema(&read_type)?;

    Ok(try_stream! {
        // Pre-load schemas and collect field IDs + data_fields per file.
        // file_idx -> (field_ids, Option<Vec<DataField>>)
        let mut file_info: HashMap<usize, (Vec<i32>, Option<Vec<DataField>>)> = HashMap::new();

        for (file_idx, file_meta) in data_files.iter().enumerate() {
            let (field_ids, data_fields) = if file_meta.schema_id != table_schema_id {
                let file_schema = schema_manager.schema(file_meta.schema_id).await?;
                let file_fields = file_schema.fields();

                let ids: Vec<i32> = if let Some(ref wc) = file_meta.write_cols {
                    // write_cols names are from the file's schema at write time.
                    wc.iter()
                        .filter_map(|name| file_fields.iter().find(|f| f.name() == name).map(|f| f.id()))
                        .collect()
                } else {
                    file_fields.iter().map(|f| f.id()).collect()
                };

                (ids, Some(file_fields.to_vec()))
            } else {
                let ids: Vec<i32> = if let Some(ref wc) = file_meta.write_cols {
                    // write_cols names are from the current table schema.
                    wc.iter()
                        .filter_map(|name| table_fields.iter().find(|f| f.name() == name).map(|f| f.id()))
                        .collect()
                } else {
                    table_fields.iter().map(|f| f.id()).collect()
                };

                (ids, None)
            };

            file_info.insert(file_idx, (field_ids, data_fields));
        }

        // Determine which file provides each field ID, resolving conflicts by max_sequence_number.
        // field_id -> (file_index, max_sequence_number)
        let mut field_id_source: HashMap<i32, (usize, i64)> = HashMap::new();
        for (file_idx, file_meta) in data_files.iter().enumerate() {
            let (ref field_ids, _) = file_info[&file_idx];
            for &fid in field_ids {
                let entry = field_id_source
                    .entry(fid)
                    .or_insert((file_idx, i64::MIN));
                if file_meta.max_sequence_number > entry.1 {
                    *entry = (file_idx, file_meta.max_sequence_number);
                }
            }
        }

        // For each projected field, determine which file provides it (by field ID).
        // file_index -> Vec<column_name>  (target column names)
        let mut file_read_columns: HashMap<usize, Vec<String>> = HashMap::new();
        for field in &read_type {
            if let Some(&(file_idx, _)) = field_id_source.get(&field.id()) {
                file_read_columns
                    .entry(file_idx)
                    .or_default()
                    .push(field.name().to_string());
            }
        }

        // For each projected field, record (file_index, target_column_name) for assembly.
        let column_plan: Vec<(Option<usize>, String)> = read_type
            .iter()
            .map(|field| {
                let file_idx = field_id_source.get(&field.id()).map(|&(idx, _)| idx);
                (file_idx, field.name().to_string())
            })
            .collect();

        // Collect which file indices we need to open streams for.
        let active_file_indices: Vec<usize> = file_read_columns.keys().copied().collect();

        // Edge case: if no file provides any projected column (e.g. SELECT on a newly added
        // column that no file contains yet), we still need to emit NULL-filled rows to
        // preserve the correct row count.
        if active_file_indices.is_empty() {
            // All files in a merge group cover the same rows; use the first file's row_count.
            let total_rows = data_files[0].row_count as usize;
            let mut emitted = 0;
            while emitted < total_rows {
                let rows_to_emit = (total_rows - emitted).min(output_batch_size);
                let columns: Vec<Arc<dyn arrow_array::Array>> = target_schema
                    .fields()
                    .iter()
                    .map(|f| arrow_array::new_null_array(f.data_type(), rows_to_emit))
                    .collect();
                let batch = if columns.is_empty() {
                    RecordBatch::try_new_with_options(
                        target_schema.clone(),
                        columns,
                        &arrow_array::RecordBatchOptions::new().with_row_count(Some(rows_to_emit)),
                    )
                } else {
                    RecordBatch::try_new(target_schema.clone(), columns)
                }
                .map_err(|e| Error::UnexpectedError {
                    message: format!("Failed to build NULL-filled RecordBatch: {e}"),
                    source: Some(Box::new(e)),
                })?;
                emitted += rows_to_emit;
                yield batch;
            }
        } else {

        // Open a stream for each active file.
        // Build per-file read_type: only the DataFields this file is responsible for.
        let mut file_streams: HashMap<usize, ArrowRecordBatchStream> = HashMap::new();
        for &file_idx in &active_file_indices {
            let file_cols = file_read_columns.get(&file_idx).cloned().unwrap_or_default();
            let file_read_type: Vec<DataField> = file_cols
                .iter()
                .filter_map(|col_name| read_type.iter().find(|f| f.name() == col_name).cloned())
                .collect();

            let (_, ref data_fields) = file_info[&file_idx];

            let stream = read_single_file_stream(
                file_io.clone(),
                split.clone(),
                data_files[file_idx].clone(),
                file_read_type,
                data_fields.clone(),
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
            // Use the target schema so that missing columns are null-filled.
            let mut columns: Vec<Arc<dyn arrow_array::Array>> =
                Vec::with_capacity(column_plan.len());

            for (i, (file_idx_opt, col_name)) in column_plan.iter().enumerate() {
                let target_field = &target_schema.fields()[i];
                let col = file_idx_opt
                    .and_then(|file_idx| file_cursors.get(&file_idx))
                    .and_then(|(batch, offset)| {
                        batch
                            .schema()
                            .index_of(col_name)
                            .ok()
                            .map(|col_idx| batch.column(col_idx).slice(*offset, rows_to_emit))
                    });

                columns.push(col.unwrap_or_else(|| {
                    arrow_array::new_null_array(target_field.data_type(), rows_to_emit)
                }));
            }

            // Advance all cursors.
            for &file_idx in &active_file_indices {
                if let Some((_, ref mut offset)) = file_cursors.get_mut(&file_idx) {
                    *offset += rows_to_emit;
                }
            }

            let merged = RecordBatch::try_new(target_schema.clone(), columns).map_err(|e| Error::UnexpectedError {
                message: format!("Failed to build merged RecordBatch: {e}"),
                source: Some(Box::new(e)),
            })?;
            yield merged;
        }
        } // end else (active_file_indices non-empty)
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
