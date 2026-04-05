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
use crate::arrow::filtering::{
    build_field_mapping, predicates_may_match_with_schema, StatsAccessor,
};
use crate::arrow::schema_evolution::{create_index_mapping, NULL_FIELD_INDEX};
use crate::deletion_vector::{DeletionVector, DeletionVectorFactory};
use crate::io::{FileIO, FileRead, FileStatus};
use crate::spec::{DataField, DataFileMeta, DataType, Datum, Predicate, PredicateOperator};
use crate::table::schema_manager::SchemaManager;
use crate::table::ArrowRecordBatchStream;
use crate::{DataSplit, Error};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, Scalar, StringArray,
};
use arrow_cast::cast;
use arrow_ord::cmp::{
    eq as arrow_eq, gt as arrow_gt, gt_eq as arrow_gt_eq, lt as arrow_lt, lt_eq as arrow_lt_eq,
    neq as arrow_neq,
};
use arrow_schema::ArrowError;

use async_stream::try_stream;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{StreamExt, TryFutureExt};
use parquet::arrow::arrow_reader::{
    ArrowPredicate, ArrowPredicateFn, ArrowReaderOptions, RowFilter, RowSelection, RowSelector,
};
use parquet::arrow::async_reader::{AsyncFileReader, MetadataFetch};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::statistics::Statistics as ParquetStatistics;
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
    predicates: Vec<Predicate>,
    table_fields: Vec<DataField>,
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
            predicates: Vec::new(),
            table_fields: Vec::new(),
        }
    }

    /// Set data predicates used for Parquet row-group pruning and partial
    /// decode-time filtering.
    pub(crate) fn with_predicates(mut self, predicates: Vec<Predicate>) -> Self {
        self.predicates = predicates;
        self
    }

    /// Set the full table schema fields used for filter-to-file field mapping.
    pub(crate) fn with_table_fields(mut self, table_fields: Vec<DataField>) -> Self {
        self.table_fields = table_fields;
        self
    }

    /// Build the ArrowReader with the given read type (logical row type or projected subset).
    /// Used to clip Parquet schema to requested columns only.
    pub fn build(self, read_type: Vec<DataField>) -> ArrowReader {
        ArrowReader {
            batch_size: self.batch_size,
            file_io: self.file_io,
            schema_manager: self.schema_manager,
            table_schema_id: self.table_schema_id,
            predicates: self.predicates,
            table_fields: self.table_fields,
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
    predicates: Vec<Predicate>,
    table_fields: Vec<DataField>,
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
        let predicates = self.predicates;
        let table_fields = self.table_fields;
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
                        SingleFileReadRequest {
                            split: split.clone(),
                            file_meta,
                            read_type: read_type.clone(),
                            table_fields: table_fields.clone(),
                            data_fields,
                            predicates: predicates.clone(),
                            batch_size,
                            dv,
                        },
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
                            file_io.clone(),
                            SingleFileReadRequest {
                                split: split.clone(),
                                file_meta,
                                read_type: read_type.clone(),
                                table_fields: table_fields.clone(),
                                data_fields,
                                predicates: Vec::new(),
                                batch_size,
                                dv: None,
                            },
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

struct SingleFileReadRequest {
    split: DataSplit,
    file_meta: DataFileMeta,
    read_type: Vec<DataField>,
    table_fields: Vec<DataField>,
    data_fields: Option<Vec<DataField>>,
    predicates: Vec<Predicate>,
    batch_size: Option<usize>,
    dv: Option<Arc<DeletionVector>>,
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
    request: SingleFileReadRequest,
) -> crate::Result<ArrowRecordBatchStream> {
    let SingleFileReadRequest {
        split,
        file_meta,
        read_type,
        table_fields,
        data_fields,
        predicates,
        batch_size,
        dv,
    } = request;

    let target_schema = build_target_arrow_schema(&read_type)?;
    let file_fields = data_fields.clone().unwrap_or_else(|| table_fields.clone());

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

        // Project columns by root-level index to correctly handle complex types
        // (ARRAY, MAP, STRUCT). `ProjectionMask::columns` matches leaf column names
        // which doesn't work for nested types; `ProjectionMask::roots` uses top-level
        // field indices instead.
        let parquet_schema = batch_stream_builder.parquet_schema().clone();
        let root_schema = parquet_schema.root_schema();
        let root_indices: Vec<usize> = parquet_column_names
            .iter()
            .filter_map(|name| {
                root_schema
                    .get_fields()
                    .iter()
                    .position(|f| f.name() == name)
            })
            .collect();

        let mask = ProjectionMask::roots(&parquet_schema, root_indices);
        batch_stream_builder = batch_stream_builder.with_projection(mask);
        let row_filter =
            build_parquet_row_filter(&parquet_schema, &predicates, &table_fields, &file_fields)?;
        if let Some(row_filter) = row_filter {
            batch_stream_builder = batch_stream_builder.with_row_filter(row_filter);
        }

        let predicate_row_selection = build_predicate_row_selection(
            batch_stream_builder.metadata().row_groups(),
            &predicates,
            &table_fields,
            &file_fields,
        )?;
        let mut row_selection = predicate_row_selection;

        if let Some(ref dv) = dv {
            if !dv.is_empty() {
                let delete_row_selection =
                    build_deletes_row_selection(batch_stream_builder.metadata().row_groups(), dv)?;
                row_selection = intersect_optional_row_selections(
                    row_selection,
                    Some(delete_row_selection),
                );
            }
        }
        if let Some(row_selection) = row_selection {
            batch_stream_builder = batch_stream_builder.with_row_selection(row_selection);
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
                SingleFileReadRequest {
                    split: split.clone(),
                    file_meta: data_files[file_idx].clone(),
                    read_type: file_read_type,
                    table_fields: table_fields.clone(),
                    data_fields: data_fields.clone(),
                    predicates: Vec::new(),
                    batch_size,
                    dv: None,
                },
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

fn intersect_optional_row_selections(
    left: Option<RowSelection>,
    right: Option<RowSelection>,
) -> Option<RowSelection> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.intersection(&right)),
        (Some(selection), None) | (None, Some(selection)) => Some(selection),
        (None, None) => None,
    }
}

fn sanitize_filter_mask(mask: BooleanArray) -> BooleanArray {
    if mask.null_count() == 0 {
        return mask;
    }

    boolean_mask_from_predicate(mask.len(), |row_index| {
        mask.is_valid(row_index) && mask.value(row_index)
    })
}

fn evaluate_exact_leaf_predicate(
    array: &ArrayRef,
    data_type: &DataType,
    op: PredicateOperator,
    literals: &[Datum],
) -> Result<BooleanArray, ArrowError> {
    match op {
        PredicateOperator::IsNull => Ok(boolean_mask_from_predicate(array.len(), |row_index| {
            array.is_null(row_index)
        })),
        PredicateOperator::IsNotNull => Ok(boolean_mask_from_predicate(array.len(), |row_index| {
            array.is_valid(row_index)
        })),
        PredicateOperator::In | PredicateOperator::NotIn => {
            evaluate_set_membership_predicate(array, data_type, op, literals)
        }
        PredicateOperator::Eq
        | PredicateOperator::NotEq
        | PredicateOperator::Lt
        | PredicateOperator::LtEq
        | PredicateOperator::Gt
        | PredicateOperator::GtEq => {
            let Some(literal) = literals.first() else {
                return Ok(BooleanArray::from(vec![true; array.len()]));
            };
            let Some(scalar) = literal_scalar_for_parquet_filter(literal, data_type)
                .map_err(|e| ArrowError::ComputeError(e.to_string()))?
            else {
                return Ok(BooleanArray::from(vec![true; array.len()]));
            };
            let result = evaluate_column_predicate(array, &scalar, op)?;
            Ok(sanitize_filter_mask(result))
        }
    }
}

fn evaluate_set_membership_predicate(
    array: &ArrayRef,
    data_type: &DataType,
    op: PredicateOperator,
    literals: &[Datum],
) -> Result<BooleanArray, ArrowError> {
    if literals.is_empty() {
        return Ok(match op {
            PredicateOperator::In => BooleanArray::from(vec![false; array.len()]),
            PredicateOperator::NotIn => {
                boolean_mask_from_predicate(array.len(), |row_index| array.is_valid(row_index))
            }
            PredicateOperator::IsNull
            | PredicateOperator::IsNotNull
            | PredicateOperator::Eq
            | PredicateOperator::NotEq
            | PredicateOperator::Lt
            | PredicateOperator::LtEq
            | PredicateOperator::Gt
            | PredicateOperator::GtEq => unreachable!(),
        });
    }

    let mut combined = match op {
        PredicateOperator::In => BooleanArray::from(vec![false; array.len()]),
        PredicateOperator::NotIn => {
            boolean_mask_from_predicate(array.len(), |row_index| array.is_valid(row_index))
        }
        PredicateOperator::IsNull
        | PredicateOperator::IsNotNull
        | PredicateOperator::Eq
        | PredicateOperator::NotEq
        | PredicateOperator::Lt
        | PredicateOperator::LtEq
        | PredicateOperator::Gt
        | PredicateOperator::GtEq => unreachable!(),
    };

    for literal in literals {
        let Some(scalar) = literal_scalar_for_parquet_filter(literal, data_type)
            .map_err(|e| ArrowError::ComputeError(e.to_string()))?
        else {
            return Ok(BooleanArray::from(vec![true; array.len()]));
        };
        let comparison_op = match op {
            PredicateOperator::In => PredicateOperator::Eq,
            PredicateOperator::NotIn => PredicateOperator::NotEq,
            PredicateOperator::IsNull
            | PredicateOperator::IsNotNull
            | PredicateOperator::Eq
            | PredicateOperator::NotEq
            | PredicateOperator::Lt
            | PredicateOperator::LtEq
            | PredicateOperator::Gt
            | PredicateOperator::GtEq => unreachable!(),
        };
        let mask = sanitize_filter_mask(evaluate_column_predicate(array, &scalar, comparison_op)?);
        combined = combine_filter_masks(&combined, &mask, matches!(op, PredicateOperator::In));
    }

    Ok(combined)
}

fn combine_filter_masks(left: &BooleanArray, right: &BooleanArray, use_or: bool) -> BooleanArray {
    debug_assert_eq!(left.len(), right.len());
    boolean_mask_from_predicate(left.len(), |row_index| {
        if use_or {
            left.value(row_index) || right.value(row_index)
        } else {
            left.value(row_index) && right.value(row_index)
        }
    })
}

fn boolean_mask_from_predicate(
    len: usize,
    mut predicate: impl FnMut(usize) -> bool,
) -> BooleanArray {
    BooleanArray::from((0..len).map(&mut predicate).collect::<Vec<_>>())
}

struct ParquetRowGroupStats<'a> {
    row_group: &'a RowGroupMetaData,
    column_indices: &'a [Option<usize>],
}

impl StatsAccessor for ParquetRowGroupStats<'_> {
    fn row_count(&self) -> i64 {
        self.row_group.num_rows()
    }

    fn null_count(&self, index: usize) -> Option<i64> {
        let _ = index;
        // parquet::Statistics::null_count_opt() may return Some(0) even when
        // the null-count statistic is absent, so treating it as authoritative
        // would make IS NULL / IS NOT NULL pruning unsafe. Fail open here.
        None
    }

    fn min_value(&self, index: usize, data_type: &DataType) -> Option<Datum> {
        let column_index = self.column_indices.get(index).copied().flatten()?;
        parquet_stats_to_datum(
            self.row_group.column(column_index).statistics()?,
            data_type,
            true,
        )
    }

    fn max_value(&self, index: usize, data_type: &DataType) -> Option<Datum> {
        let column_index = self.column_indices.get(index).copied().flatten()?;
        parquet_stats_to_datum(
            self.row_group.column(column_index).statistics()?,
            data_type,
            false,
        )
    }
}

fn build_predicate_row_selection(
    row_groups: &[RowGroupMetaData],
    predicates: &[Predicate],
    table_fields: &[DataField],
    file_fields: &[DataField],
) -> crate::Result<Option<RowSelection>> {
    if predicates.is_empty() || row_groups.is_empty() {
        return Ok(None);
    }

    let field_mapping = build_field_mapping(table_fields, file_fields);
    let column_indices = build_row_group_column_indices(row_groups[0].columns(), file_fields);
    let mut selectors = Vec::with_capacity(row_groups.len());
    let mut all_selected = true;

    for row_group in row_groups {
        let stats = ParquetRowGroupStats {
            row_group,
            column_indices: &column_indices,
        };
        let may_match =
            predicates_may_match_with_schema(predicates, &stats, &field_mapping, file_fields);
        if !may_match {
            all_selected = false;
        }
        selectors.push(if may_match {
            RowSelector::select(row_group.num_rows() as usize)
        } else {
            RowSelector::skip(row_group.num_rows() as usize)
        });
    }

    if all_selected {
        Ok(None)
    } else {
        Ok(Some(selectors.into()))
    }
}

fn build_parquet_row_filter(
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
    predicates: &[Predicate],
    table_fields: &[DataField],
    file_fields: &[DataField],
) -> crate::Result<Option<RowFilter>> {
    // Keep decode-time filtering intentionally narrow for the submit-ready
    // Parquet path. Unsupported predicate shapes or types remain residual
    // filters above the reader.
    if predicates.is_empty() {
        return Ok(None);
    }

    let field_mapping = build_field_mapping(table_fields, file_fields);
    let mut filters: Vec<Box<dyn ArrowPredicate>> = Vec::new();

    for predicate in predicates {
        if let Some(filter) =
            build_parquet_arrow_predicate(parquet_schema, predicate, &field_mapping, file_fields)?
        {
            filters.push(filter);
        }
    }

    if filters.is_empty() {
        Ok(None)
    } else {
        Ok(Some(RowFilter::new(filters)))
    }
}

fn build_parquet_arrow_predicate(
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
    predicate: &Predicate,
    field_mapping: &[Option<usize>],
    file_fields: &[DataField],
) -> crate::Result<Option<Box<dyn ArrowPredicate>>> {
    let Predicate::Leaf {
        index,
        data_type: _,
        op,
        literals,
        ..
    } = predicate
    else {
        return Ok(None);
    };
    if !predicate_supported_for_parquet_row_filter(*op) {
        return Ok(None);
    }

    let Some(file_index) = field_mapping.get(*index).copied().flatten() else {
        return Ok(None);
    };
    let Some(file_field) = file_fields.get(file_index) else {
        return Ok(None);
    };
    let Some(root_index) = parquet_root_index(parquet_schema, file_field.name()) else {
        return Ok(None);
    };
    if !parquet_row_filter_literals_supported(*op, literals, file_field.data_type())? {
        return Ok(None);
    }

    let projection = ProjectionMask::roots(parquet_schema, [root_index]);
    let op = *op;
    let data_type = file_field.data_type().clone();
    let literals = literals.to_vec();
    Ok(Some(Box::new(ArrowPredicateFn::new(
        projection,
        move |batch: RecordBatch| {
            let Some(column) = batch.columns().first() else {
                return Ok(BooleanArray::new_null(batch.num_rows()));
            };
            evaluate_exact_leaf_predicate(column, &data_type, op, &literals)
        },
    ))))
}

fn predicate_supported_for_parquet_row_filter(op: PredicateOperator) -> bool {
    matches!(
        op,
        PredicateOperator::IsNull
            | PredicateOperator::IsNotNull
            | PredicateOperator::Eq
            | PredicateOperator::NotEq
            | PredicateOperator::Lt
            | PredicateOperator::LtEq
            | PredicateOperator::Gt
            | PredicateOperator::GtEq
            | PredicateOperator::In
            | PredicateOperator::NotIn
    )
}

fn parquet_row_filter_literals_supported(
    op: PredicateOperator,
    literals: &[Datum],
    file_data_type: &DataType,
) -> crate::Result<bool> {
    match op {
        PredicateOperator::IsNull | PredicateOperator::IsNotNull => Ok(true),
        PredicateOperator::Eq
        | PredicateOperator::NotEq
        | PredicateOperator::Lt
        | PredicateOperator::LtEq
        | PredicateOperator::Gt
        | PredicateOperator::GtEq => {
            let Some(literal) = literals.first() else {
                return Ok(false);
            };
            Ok(literal_scalar_for_parquet_filter(literal, file_data_type)?.is_some())
        }
        PredicateOperator::In | PredicateOperator::NotIn => {
            for literal in literals {
                if literal_scalar_for_parquet_filter(literal, file_data_type)?.is_none() {
                    return Ok(false);
                }
            }
            Ok(true)
        }
    }
}

fn parquet_root_index(
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
    root_name: &str,
) -> Option<usize> {
    parquet_schema
        .root_schema()
        .get_fields()
        .iter()
        .position(|field| field.name() == root_name)
}

fn evaluate_column_predicate(
    column: &ArrayRef,
    scalar: &Scalar<ArrayRef>,
    op: PredicateOperator,
) -> Result<BooleanArray, ArrowError> {
    match op {
        PredicateOperator::Eq => arrow_eq(column, scalar),
        PredicateOperator::NotEq => arrow_neq(column, scalar),
        PredicateOperator::Lt => arrow_lt(column, scalar),
        PredicateOperator::LtEq => arrow_lt_eq(column, scalar),
        PredicateOperator::Gt => arrow_gt(column, scalar),
        PredicateOperator::GtEq => arrow_gt_eq(column, scalar),
        PredicateOperator::IsNull
        | PredicateOperator::IsNotNull
        | PredicateOperator::In
        | PredicateOperator::NotIn => Ok(BooleanArray::new_null(column.len())),
    }
}

fn literal_scalar_for_parquet_filter(
    literal: &Datum,
    file_data_type: &DataType,
) -> crate::Result<Option<Scalar<ArrayRef>>> {
    let array: ArrayRef = match file_data_type {
        DataType::Boolean(_) => match literal {
            Datum::Bool(value) => Arc::new(BooleanArray::new_scalar(*value).into_inner()),
            _ => return Ok(None),
        },
        DataType::TinyInt(_) => {
            match integer_literal(literal).and_then(|value| i8::try_from(value).ok()) {
                Some(value) => Arc::new(Int8Array::new_scalar(value).into_inner()),
                None => return Ok(None),
            }
        }
        DataType::SmallInt(_) => {
            match integer_literal(literal).and_then(|value| i16::try_from(value).ok()) {
                Some(value) => Arc::new(Int16Array::new_scalar(value).into_inner()),
                None => return Ok(None),
            }
        }
        DataType::Int(_) => {
            match integer_literal(literal).and_then(|value| i32::try_from(value).ok()) {
                Some(value) => Arc::new(Int32Array::new_scalar(value).into_inner()),
                None => return Ok(None),
            }
        }
        DataType::BigInt(_) => {
            match integer_literal(literal).and_then(|value| i64::try_from(value).ok()) {
                Some(value) => Arc::new(Int64Array::new_scalar(value).into_inner()),
                None => return Ok(None),
            }
        }
        DataType::Float(_) => match float32_literal(literal) {
            Some(value) => Arc::new(Float32Array::new_scalar(value).into_inner()),
            None => return Ok(None),
        },
        DataType::Double(_) => match float64_literal(literal) {
            Some(value) => Arc::new(Float64Array::new_scalar(value).into_inner()),
            None => return Ok(None),
        },
        DataType::Char(_) | DataType::VarChar(_) => match literal {
            Datum::String(value) => Arc::new(StringArray::new_scalar(value.as_str()).into_inner()),
            _ => return Ok(None),
        },
        DataType::Binary(_) | DataType::VarBinary(_) => match literal {
            Datum::Bytes(value) => Arc::new(BinaryArray::new_scalar(value.as_slice()).into_inner()),
            _ => return Ok(None),
        },
        DataType::Date(_) => match literal {
            Datum::Date(value) => Arc::new(Date32Array::new_scalar(*value).into_inner()),
            _ => return Ok(None),
        },
        DataType::Decimal(decimal) => match literal {
            Datum::Decimal {
                unscaled,
                precision,
                scale,
            } if *precision <= decimal.precision() && *scale == decimal.scale() => {
                let precision =
                    u8::try_from(decimal.precision()).map_err(|_| Error::Unsupported {
                        message: "Decimal precision exceeds Arrow decimal128 range".to_string(),
                    })?;
                let scale =
                    i8::try_from(decimal.scale() as i32).map_err(|_| Error::Unsupported {
                        message: "Decimal scale exceeds Arrow decimal128 range".to_string(),
                    })?;
                Arc::new(
                    Decimal128Array::new_scalar(*unscaled)
                        .into_inner()
                        .with_precision_and_scale(precision, scale)
                        .map_err(|e| Error::UnexpectedError {
                            message: format!(
                                "Failed to build decimal scalar for parquet row filter: {e}"
                            ),
                            source: Some(Box::new(e)),
                        })?,
                )
            }
            _ => return Ok(None),
        },
        DataType::Time(_)
        | DataType::Timestamp(_)
        | DataType::LocalZonedTimestamp(_)
        | DataType::Array(_)
        | DataType::Map(_)
        | DataType::Multiset(_)
        | DataType::Row(_) => return Ok(None),
    };

    Ok(Some(Scalar::new(array)))
}

fn integer_literal(literal: &Datum) -> Option<i128> {
    match literal {
        Datum::TinyInt(value) => Some(i128::from(*value)),
        Datum::SmallInt(value) => Some(i128::from(*value)),
        Datum::Int(value) => Some(i128::from(*value)),
        Datum::Long(value) => Some(i128::from(*value)),
        _ => None,
    }
}

fn float32_literal(literal: &Datum) -> Option<f32> {
    match literal {
        Datum::Float(value) => Some(*value),
        Datum::Double(value) => {
            let casted = *value as f32;
            ((casted as f64) == *value).then_some(casted)
        }
        _ => None,
    }
}

fn float64_literal(literal: &Datum) -> Option<f64> {
    match literal {
        Datum::Float(value) => Some(f64::from(*value)),
        Datum::Double(value) => Some(*value),
        _ => None,
    }
}

fn build_row_group_column_indices(
    columns: &[parquet::file::metadata::ColumnChunkMetaData],
    file_fields: &[DataField],
) -> Vec<Option<usize>> {
    let mut by_root_name: HashMap<&str, Option<usize>> = HashMap::new();
    for (column_index, column) in columns.iter().enumerate() {
        let Some(root_name) = column.column_path().parts().first() else {
            continue;
        };
        let entry = by_root_name
            .entry(root_name.as_str())
            .or_insert(Some(column_index));
        if entry.is_some() && *entry != Some(column_index) {
            *entry = None;
        }
    }

    file_fields
        .iter()
        .map(|field| by_root_name.get(field.name()).copied().flatten())
        .collect()
}

fn parquet_stats_to_datum(
    stats: &ParquetStatistics,
    data_type: &DataType,
    is_min: bool,
) -> Option<Datum> {
    let exact = if is_min {
        stats.min_is_exact()
    } else {
        stats.max_is_exact()
    };
    if !exact {
        return None;
    }

    match (stats, data_type) {
        (ParquetStatistics::Boolean(stats), DataType::Boolean(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(Datum::Bool)
        }
        (ParquetStatistics::Int32(stats), DataType::TinyInt(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .and_then(|value| i8::try_from(*value).ok())
                .map(Datum::TinyInt)
        }
        (ParquetStatistics::Int32(stats), DataType::SmallInt(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .and_then(|value| i16::try_from(*value).ok())
                .map(Datum::SmallInt)
        }
        (ParquetStatistics::Int32(stats), DataType::Int(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(Datum::Int)
        }
        (ParquetStatistics::Int32(stats), DataType::Date(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(Datum::Date)
        }
        (ParquetStatistics::Int32(stats), DataType::Time(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(Datum::Time)
        }
        (ParquetStatistics::Int64(stats), DataType::BigInt(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(Datum::Long)
        }
        (ParquetStatistics::Int64(stats), DataType::Timestamp(ts)) if ts.precision() <= 3 => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(|millis| Datum::Timestamp { millis, nanos: 0 })
        }
        (ParquetStatistics::Int64(stats), DataType::LocalZonedTimestamp(ts))
            if ts.precision() <= 3 =>
        {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(|millis| Datum::LocalZonedTimestamp { millis, nanos: 0 })
        }
        (ParquetStatistics::Float(stats), DataType::Float(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(Datum::Float)
        }
        (ParquetStatistics::Double(stats), DataType::Double(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(Datum::Double)
        }
        (ParquetStatistics::ByteArray(stats), DataType::Char(_))
        | (ParquetStatistics::ByteArray(stats), DataType::VarChar(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .and_then(|value| std::str::from_utf8(value.data()).ok())
                .map(|value| Datum::String(value.to_string()))
        }
        (ParquetStatistics::ByteArray(stats), DataType::Binary(_))
        | (ParquetStatistics::ByteArray(stats), DataType::VarBinary(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .map(|value| Datum::Bytes(value.data().to_vec()))
        }
        (ParquetStatistics::FixedLenByteArray(stats), DataType::Binary(_))
        | (ParquetStatistics::FixedLenByteArray(stats), DataType::VarBinary(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .map(|value| Datum::Bytes(value.data().to_vec()))
        }
        _ => None,
    }
}

fn exact_parquet_value<'a, T>(
    is_min: bool,
    min: Option<&'a T>,
    max: Option<&'a T>,
) -> Option<&'a T> {
    if is_min {
        min
    } else {
        max
    }
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

#[cfg(test)]
mod tests {
    use super::build_parquet_row_filter;
    use crate::spec::{DataField, DataType, Datum, IntType, PredicateBuilder};
    use parquet::schema::{parser::parse_message_type, types::SchemaDescriptor};
    use std::sync::Arc;

    fn test_fields() -> Vec<DataField> {
        vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "score".to_string(), DataType::Int(IntType::new())),
        ]
    }

    fn test_parquet_schema() -> SchemaDescriptor {
        SchemaDescriptor::new(Arc::new(
            parse_message_type(
                "
                message test_schema {
                  OPTIONAL INT32 id;
                  OPTIONAL INT32 score;
                }
                ",
            )
            .expect("test schema should parse"),
        ))
    }

    #[test]
    fn test_build_parquet_row_filter_supports_null_and_membership_predicates() {
        let fields = test_fields();
        let builder = PredicateBuilder::new(&fields);
        let predicates = vec![
            builder
                .is_null("id")
                .expect("is null predicate should build"),
            builder
                .is_in("score", vec![Datum::Int(7)])
                .expect("in predicate should build"),
            builder
                .is_not_in("score", vec![Datum::Int(9)])
                .expect("not in predicate should build"),
        ];

        let row_filter =
            build_parquet_row_filter(&test_parquet_schema(), &predicates, &fields, &fields)
                .expect("parquet row filter should build");

        assert!(row_filter.is_some());
    }
}
