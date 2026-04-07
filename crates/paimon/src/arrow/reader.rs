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
use crate::arrow::format::create_format_reader;
use crate::arrow::schema_evolution::{create_index_mapping, NULL_FIELD_INDEX};
use crate::deletion_vector::{DeletionVector, DeletionVectorFactory};
use crate::io::FileIO;
use crate::spec::{DataField, DataFileMeta, Predicate, ROW_ID_FIELD_NAME};
use crate::table::schema_manager::SchemaManager;
use crate::table::ArrowRecordBatchStream;
use crate::table::RowRange;
use crate::{DataSplit, Error};
use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_cast::cast;

use async_stream::try_stream;
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;

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
                            row_ranges: None,
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

        let row_id_index = read_type.iter().position(|f| f.name() == ROW_ID_FIELD_NAME);
        let file_read_type: Vec<DataField> = read_type
            .iter()
            .filter(|f| f.name() != ROW_ID_FIELD_NAME)
            .cloned()
            .collect();
        let output_schema = build_target_arrow_schema(&read_type)?;

        Ok(try_stream! {
            for split in splits {
                let row_ranges = split.row_ranges().map(|r| r.to_vec());

                if split.raw_convertible() || split.data_files().len() == 1 {
                    for file_meta in split.data_files().to_vec() {
                        let data_fields: Option<Vec<DataField>> = if file_meta.schema_id != table_schema_id {
                            let data_schema = schema_manager.schema(file_meta.schema_id).await?;
                            Some(data_schema.fields().to_vec())
                        } else {
                            None
                        };

                        let has_row_id = file_meta.first_row_id.is_some();
                        let effective_row_ranges = if has_row_id { row_ranges.clone() } else { None };

                        let selected_row_ids = if row_id_index.is_some() && has_row_id {
                            effective_row_ranges.as_ref().map(|ranges| {
                                expand_selected_row_ids(
                                    file_meta.first_row_id.unwrap(),
                                    file_meta.row_count,
                                    ranges,
                                )
                            })
                        } else {
                            None
                        };
                        let file_base_row_id = file_meta.first_row_id.unwrap_or(0);
                        let mut row_id_cursor = file_base_row_id;
                        let mut row_id_offset: usize = 0;

                        let mut stream = read_single_file_stream(
                            file_io.clone(),
                            SingleFileReadRequest {
                                split: split.clone(),
                                file_meta,
                                read_type: file_read_type.clone(),
                                table_fields: table_fields.clone(),
                                data_fields,
                                predicates: Vec::new(),
                                batch_size,
                                dv: None,
                                row_ranges: effective_row_ranges,
                            },
                        )?;
                        while let Some(batch) = stream.next().await {
                            let batch = batch?;
                            let num_rows = batch.num_rows();
                            if let Some(idx) = row_id_index {
                                if !has_row_id {
                                    yield append_null_row_id_column(batch, idx, &output_schema)?;
                                } else if let Some(ref ids) = selected_row_ids {
                                    yield attach_row_id(batch, idx, ids, &mut row_id_offset, &output_schema)?;
                                } else {
                                    let row_ids: Vec<i64> = (row_id_cursor..row_id_cursor + num_rows as i64).collect();
                                    row_id_cursor += num_rows as i64;
                                    let array: Arc<dyn arrow_array::Array> = Arc::new(Int64Array::from(row_ids));
                                    yield insert_column_at(batch, array, idx, &output_schema)?;
                                }
                            } else {
                                yield batch;
                            }
                        }
                    }
                } else {
                    let files = split.data_files();
                    assert!(
                        files.iter().all(|f| f.first_row_id.is_some()),
                        "All files in a field merge split should have first_row_id"
                    );
                    assert!(
                        files.iter().all(|f| f.row_count == files[0].row_count),
                        "All files in a field merge split should have the same row count"
                    );
                    assert!(
                        files.iter().all(|f| f.first_row_id == files[0].first_row_id),
                        "All files in a field merge split should have the same first row id"
                    );

                    let group_base_row_id = files
                        .iter()
                        .filter_map(|f| f.first_row_id)
                        .min();
                    let has_group_row_id = group_base_row_id.is_some();
                    let group_row_count = files.iter().map(|f| f.row_count).max().unwrap_or(0);
                    let effective_row_ranges = if has_group_row_id { row_ranges.clone() } else { None };

                    let selected_row_ids = if row_id_index.is_some() && has_group_row_id {
                        effective_row_ranges.as_ref().map(|ranges| {
                            expand_selected_row_ids(
                                group_base_row_id.unwrap(),
                                group_row_count,
                                ranges,
                            )
                        })
                    } else {
                        None
                    };
                    let mut row_id_cursor = group_base_row_id.unwrap_or(0);
                    let mut row_id_offset: usize = 0;

                    let mut merge_stream = merge_files_by_columns(
                        &file_io,
                        &split,
                        &file_read_type,
                        &table_fields,
                        schema_manager.clone(),
                        table_schema_id,
                        batch_size,
                        effective_row_ranges,
                    )?;
                    while let Some(batch) = merge_stream.next().await {
                        let batch = batch?;
                        let num_rows = batch.num_rows();
                        if let Some(idx) = row_id_index {
                            if !has_group_row_id {
                                yield append_null_row_id_column(batch, idx, &output_schema)?;
                            } else if let Some(ref ids) = selected_row_ids {
                                yield attach_row_id(batch, idx, ids, &mut row_id_offset, &output_schema)?;
                            } else {
                                let row_ids: Vec<i64> = (row_id_cursor..row_id_cursor + num_rows as i64).collect();
                                row_id_cursor += num_rows as i64;
                                let array: Arc<dyn arrow_array::Array> = Arc::new(Int64Array::from(row_ids));
                                yield insert_column_at(batch, array, idx, &output_schema)?;
                            }
                        } else {
                            yield batch;
                        }
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
    row_ranges: Option<Vec<RowRange>>,
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
        row_ranges,
    } = request;

    let target_schema = build_target_arrow_schema(&read_type)?;
    let file_fields = data_fields.clone().unwrap_or_else(|| table_fields.clone());

    // Compute index mapping and determine which columns to read from the file.
    // If data_fields is provided, use field-ID-based mapping; otherwise use read_type names directly.
    let (projected_read_fields, index_mapping) = if let Some(ref df) = data_fields {
        let mapping = create_index_mapping(&read_type, df);
        match mapping {
            Some(ref idx_map) => {
                // Only read data fields that are referenced by the index mapping.
                // Dedup by data field index to avoid duplicate column projections.
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

    // Remap predicates from table-level to file-level indices.
    let file_predicates = {
        let remapped = crate::arrow::filtering::remap_predicates_to_file(
            &predicates,
            &table_fields,
            &file_fields,
        );
        if remapped.is_empty() {
            None
        } else {
            Some(crate::arrow::format::FilePredicates {
                predicates: remapped,
                file_fields: file_fields.clone(),
            })
        }
    };

    Ok(try_stream! {
        let path_to_read = split.data_file_path(&file_meta);
        let format_reader = create_format_reader(&path_to_read)?;
        let input_file = file_io.new_input(&path_to_read)?;
        let file_reader = input_file.reader().await?;
        let local_ranges = row_ranges.as_ref().map(|ranges| {
            to_local_row_ranges(ranges, file_meta.first_row_id.unwrap_or(0), file_meta.row_count)
        });

        let row_selection = merge_row_selection(
            file_meta.row_count,
            dv.as_deref(),
            local_ranges.as_deref(),
        );

        let mut batch_stream = format_reader.read_batch_stream(
            Box::new(file_reader),
            file_meta.file_size as u64,
            &projected_read_fields,
            file_predicates.as_ref(),
            batch_size,
            row_selection,
        ).await?;

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
#[allow(clippy::too_many_arguments)]
fn merge_files_by_columns(
    file_io: &FileIO,
    split: &DataSplit,
    read_type: &[DataField],
    table_fields: &[DataField],
    schema_manager: SchemaManager,
    table_schema_id: i64,
    batch_size: Option<usize>,
    row_ranges: Option<Vec<RowRange>>,
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
            let first_row_id = data_files[0].first_row_id.unwrap_or(0);
            let file_row_count = data_files[0].row_count;
            let total_rows = match &row_ranges {
                Some(ranges) => expand_selected_row_ids(first_row_id, file_row_count, ranges).len(),
                None => file_row_count as usize,
            };
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
                    row_ranges: row_ranges.clone(),
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

/// Convert absolute RowRanges to file-local 0-based ranges.
fn to_local_row_ranges(
    row_ranges: &[RowRange],
    first_row_id: i64,
    row_count: i64,
) -> Vec<RowRange> {
    let file_end = first_row_id + row_count - 1;
    row_ranges
        .iter()
        .filter_map(|r| {
            if r.to() < first_row_id || r.from() > file_end {
                return None;
            }
            let local_from = (r.from() - first_row_id).max(0);
            let local_to = (r.to() - first_row_id).min(row_count - 1);
            Some(RowRange::new(local_from, local_to))
        })
        .collect()
}

/// Merge DV and row_ranges into a unified list of 0-based inclusive RowRanges.
/// Returns `None` if no filtering is needed (no DV and no ranges).
///
/// Complexity: O(D + R) where D = number of deleted rows, R = number of ranges.
fn merge_row_selection(
    row_count: i64,
    dv: Option<&DeletionVector>,
    row_ranges: Option<&[RowRange]>,
) -> Option<Vec<RowRange>> {
    let has_dv = dv.is_some_and(|d| !d.is_empty());
    let has_ranges = row_ranges.is_some();
    if !has_dv && !has_ranges {
        return None;
    }

    // Fast path: no DV, just return row_ranges as-is.
    if !has_dv {
        return row_ranges.map(|r| r.to_vec());
    }

    // Build non-deleted ranges from DV (sorted iterator).
    let dv_ranges = dv_to_non_deleted_ranges(dv.unwrap(), row_count);

    match row_ranges {
        Some(ranges) => Some(intersect_sorted_ranges(&dv_ranges, ranges)),
        None => Some(dv_ranges),
    }
}

/// Convert a DeletionVector into sorted non-deleted inclusive RowRanges.
/// The DV iterator yields sorted deleted positions.
fn dv_to_non_deleted_ranges(dv: &DeletionVector, row_count: i64) -> Vec<RowRange> {
    let mut result = Vec::new();
    let mut cursor: i64 = 0;
    for deleted in dv.iter() {
        let del = deleted as i64;
        if del >= row_count {
            break;
        }
        if del > cursor {
            result.push(RowRange::new(cursor, del - 1));
        }
        cursor = del + 1;
    }
    if cursor < row_count {
        result.push(RowRange::new(cursor, row_count - 1));
    }
    result
}

/// Intersect two sorted lists of inclusive RowRanges using a merge-style scan.
fn intersect_sorted_ranges(a: &[RowRange], b: &[RowRange]) -> Vec<RowRange> {
    let mut result = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        let from = a[i].from().max(b[j].from());
        let to = a[i].to().min(b[j].to());
        if from <= to {
            result.push(RowRange::new(from, to));
        }
        // Advance the range that ends first.
        if a[i].to() < b[j].to() {
            i += 1;
        } else {
            j += 1;
        }
    }
    result
}

/// Expand row_ranges into a flat sequence of selected row IDs for a file.
fn expand_selected_row_ids(first_row_id: i64, row_count: i64, row_ranges: &[RowRange]) -> Vec<i64> {
    if row_count == 0 {
        return Vec::new();
    }
    let file_end = first_row_id + row_count - 1;
    let mut ids = Vec::new();
    for r in row_ranges {
        let from = r.from().max(first_row_id);
        let to = r.to().min(file_end);
        for id in from..=to {
            ids.push(id);
        }
    }
    ids
}

fn attach_row_id(
    batch: RecordBatch,
    row_id_index: usize,
    selected_row_ids: &[i64],
    row_id_offset: &mut usize,
    output_schema: &Arc<arrow_schema::Schema>,
) -> crate::Result<RecordBatch> {
    let num_rows = batch.num_rows();
    let batch_ids = &selected_row_ids[*row_id_offset..*row_id_offset + num_rows];
    *row_id_offset += num_rows;
    let array: Arc<dyn arrow_array::Array> = Arc::new(Int64Array::from(batch_ids.to_vec()));
    insert_column_at(batch, array, row_id_index, output_schema)
}

fn insert_column_at(
    batch: RecordBatch,
    column: Arc<dyn arrow_array::Array>,
    insert_index: usize,
    output_schema: &Arc<arrow_schema::Schema>,
) -> crate::Result<RecordBatch> {
    let mut columns: Vec<Arc<dyn arrow_array::Array>> = Vec::with_capacity(batch.num_columns() + 1);
    for (i, col) in batch.columns().iter().enumerate() {
        if i == insert_index {
            columns.push(column.clone());
        }
        columns.push(col.clone());
    }
    if insert_index >= batch.num_columns() {
        columns.push(column);
    }
    RecordBatch::try_new(output_schema.clone(), columns).map_err(|e| Error::UnexpectedError {
        message: format!("Failed to insert column into RecordBatch: {e}"),
        source: Some(Box::new(e)),
    })
}

/// Append a null `_ROW_ID` column for files without `first_row_id`.
fn append_null_row_id_column(
    batch: RecordBatch,
    insert_index: usize,
    output_schema: &Arc<arrow_schema::Schema>,
) -> crate::Result<RecordBatch> {
    let array: Arc<dyn arrow_array::Array> = Arc::new(Int64Array::new_null(batch.num_rows()));
    insert_column_at(batch, array, insert_index, output_schema)
}
