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
use crate::spec::{DataField, DataFileMeta, Predicate};
use crate::table::schema_manager::SchemaManager;
use crate::table::ArrowRecordBatchStream;
use crate::table::RowRange;
use crate::{DataSplit, Error};
use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_cast::cast;

use async_stream::try_stream;
use futures::StreamExt;
use std::sync::Arc;

/// Reads data from Parquet files.
#[derive(Clone)]
pub(crate) struct DataFileReader {
    file_io: FileIO,
    schema_manager: SchemaManager,
    table_schema_id: i64,
    table_fields: Vec<DataField>,
    read_type: Vec<DataField>,
    predicates: Vec<Predicate>,
}

impl DataFileReader {
    pub(crate) fn new(
        file_io: FileIO,
        schema_manager: SchemaManager,
        table_schema_id: i64,
        table_fields: Vec<DataField>,
        read_type: Vec<DataField>,
        predicates: Vec<Predicate>,
    ) -> Self {
        Self {
            file_io,
            schema_manager,
            table_schema_id,
            table_fields,
            read_type,
            predicates,
        }
    }

    /// Take a stream of DataSplits and read every data file in each split.
    /// Returns a stream of Arrow RecordBatches from all files.
    ///
    /// Uses SchemaManager to load the data file's schema (via `DataFileMeta.schema_id`)
    /// and computes field-ID-based index mapping for schema evolution (added columns,
    /// type promotion, column reordering).
    ///
    /// Matches [RawFileSplitRead.createReader](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/operation/RawFileSplitRead.java).
    pub fn read(self, data_splits: &[DataSplit]) -> crate::Result<ArrowRecordBatchStream> {
        let splits: Vec<DataSplit> = data_splits.to_vec();
        let reader = self;
        Ok(try_stream! {
            for split in splits {
                // Create DV factory for this split only.
                let dv_factory = if split
                    .data_deletion_files()
                    .is_some_and(|files| files.iter().any(Option::is_some))
                {
                    Some(
                        DeletionVectorFactory::new(
                            &reader.file_io,
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
                    let data_fields: Option<Vec<DataField>> = if file_meta.schema_id != reader.table_schema_id {
                        let data_schema = reader.schema_manager.schema(file_meta.schema_id).await?;
                        Some(data_schema.fields().to_vec())
                    } else {
                        None
                    };

                    let mut stream = reader.read_single_file_stream(
                        &split,
                        file_meta,
                        data_fields,
                        dv,
                        None,
                    )?;
                    while let Some(batch) = stream.next().await {
                        yield batch?;
                    }
                }
            }
        }
        .boxed())
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
    pub(super) fn read_single_file_stream(
        &self,
        split: &DataSplit,
        file_meta: DataFileMeta,
        data_fields: Option<Vec<DataField>>,
        dv: Option<Arc<DeletionVector>>,
        row_ranges: Option<Vec<RowRange>>,
    ) -> crate::Result<ArrowRecordBatchStream> {
        let read_type = self.read_type.clone();
        let table_fields = self.table_fields.clone();
        let predicates = self.predicates.clone();
        let file_io = self.file_io.clone();
        let split = split.clone();

        let target_schema = build_target_arrow_schema(&read_type)?;
        let file_fields = data_fields.clone().unwrap_or_else(|| table_fields.clone());

        // Compute index mapping and determine which columns to read from the file.
        let (projected_read_fields, index_mapping) = if let Some(ref df) = data_fields {
            let mapping = create_index_mapping(&read_type, df);
            match mapping {
                Some(ref idx_map) => {
                    let mut seen = std::collections::HashSet::new();
                    let fields_to_read: Vec<DataField> = idx_map
                        .iter()
                        .filter(|&&idx| idx != NULL_FIELD_INDEX && seen.insert(idx))
                        .map(|&idx| df[idx as usize].clone())
                        .collect();
                    (fields_to_read, Some(idx_map.clone()))
                }
                None => (df.clone(), None),
            }
        } else {
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
                None,
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
                            let data_field = &data_fields.as_ref().unwrap()[data_idx as usize];
                            batch_schema
                                .index_of(data_field.name())
                                .ok()
                                .map(|col_idx| batch.column(col_idx))
                        }
                    } else if let Some(ref df) = data_fields {
                        batch_schema
                            .index_of(df[i].name())
                            .ok()
                            .map(|col_idx| batch.column(col_idx))
                    } else {
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

    if !has_dv {
        return row_ranges.map(|r| r.to_vec());
    }

    let dv_ranges = dv_to_non_deleted_ranges(dv.unwrap(), row_count);

    match row_ranges {
        Some(ranges) => Some(intersect_sorted_ranges(&dv_ranges, ranges)),
        None => Some(dv_ranges),
    }
}

/// Convert a DeletionVector into sorted non-deleted inclusive RowRanges.
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
        if a[i].to() < b[j].to() {
            i += 1;
        } else {
            j += 1;
        }
    }
    result
}

/// Expand row_ranges into a flat sequence of selected row IDs for a file.
/// Intended for per-batch _ROW_ID attachment — callers should not pass
/// whole-file ranges with millions of rows, as this allocates a Vec<i64>
/// proportional to the selected range size.
pub(super) fn expand_selected_row_ids(
    first_row_id: i64,
    row_count: i64,
    row_ranges: &[RowRange],
) -> Vec<i64> {
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

pub(super) fn attach_row_id(
    batch: RecordBatch,
    row_id_index: usize,
    selected_row_ids: &[i64],
    row_id_offset: &mut usize,
    output_schema: &Arc<arrow_schema::Schema>,
) -> crate::Result<RecordBatch> {
    let num_rows = batch.num_rows();
    let end = *row_id_offset + num_rows;
    if end > selected_row_ids.len() {
        return Err(Error::UnexpectedError {
            message: format!(
                "Row ID offset out of bounds: need {}..{} but selected_row_ids has {} entries",
                *row_id_offset,
                end,
                selected_row_ids.len()
            ),
            source: None,
        });
    }
    let batch_ids = &selected_row_ids[*row_id_offset..end];
    *row_id_offset = end;
    let array: Arc<dyn arrow_array::Array> = Arc::new(Int64Array::from(batch_ids.to_vec()));
    insert_column_at(batch, array, row_id_index, output_schema)
}

pub(super) fn insert_column_at(
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
pub(super) fn append_null_row_id_column(
    batch: RecordBatch,
    insert_index: usize,
    output_schema: &Arc<arrow_schema::Schema>,
) -> crate::Result<RecordBatch> {
    let array: Arc<dyn arrow_array::Array> = Arc::new(Int64Array::new_null(batch.num_rows()));
    insert_column_at(batch, array, insert_index, output_schema)
}
