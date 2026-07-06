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
use crate::spec::{
    is_variant_extraction_row_type, DataField, DataFileMeta, DataType, Predicate, ROW_ID_FIELD_NAME,
};
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
    blob_as_descriptor: bool,
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
            blob_as_descriptor: false,
        }
    }

    pub(crate) fn with_blob_as_descriptor(mut self, blob_as_descriptor: bool) -> Self {
        self.blob_as_descriptor = blob_as_descriptor;
        self
    }

    /// Reject projecting `_ROW_ID` alongside a data predicate. `_ROW_ID` is
    /// assigned positionally from post-filter batch row counts, so a residual
    /// filter that drops rows would desync it. (`_ROW_ID` predicates travel via
    /// `row_ranges`, not `predicates`, so they do not trip this.)
    fn reject_row_id_with_predicate(
        read_type: &[DataField],
        predicates: &[Predicate],
    ) -> crate::Result<()> {
        let projects_row_id = read_type
            .iter()
            .any(|field| field.name() == ROW_ID_FIELD_NAME);
        // Only predicates that can actually drop rows desync positional `_ROW_ID`.
        // A constant `AlwaysTrue` keeps every row in order and is harmless, so it
        // must not trip the guard.
        let has_row_filtering_predicate = predicates
            .iter()
            .any(|p| !matches!(p, Predicate::AlwaysTrue));
        if projects_row_id && has_row_filtering_predicate {
            return Err(crate::Error::Unsupported {
                message: "reading _ROW_ID together with a data predicate is not supported yet"
                    .to_string(),
            });
        }
        Ok(())
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
        // Guard at the true risk site: `_ROW_ID` is materialized positionally from
        // each batch's row count (see `row_id_column_for_batch`), assuming the
        // reader emits rows in original file order and count. The format readers
        // apply an exact residual filter that drops non-matching rows *before*
        // `_ROW_ID` is assigned here, which would desync the ids. So projecting
        // `_ROW_ID` together with a data predicate is unsupported — fail loudly
        // rather than return wrong ids. Placed here (not only in `read()`) because
        // `read_single_file_stream` is also called directly by the KV and
        // data-evolution readers; both strip/omit `_ROW_ID` from the read_type
        // they pass, so this guard does not affect them.
        Self::reject_row_id_with_predicate(&self.read_type, &self.predicates)?;

        let read_type = self.read_type.clone();
        let table_fields = self.table_fields.clone();
        let predicates = self.predicates.clone();
        let file_io = self.file_io.clone();
        let split = split.clone();
        let blob_as_descriptor = self.blob_as_descriptor;

        let target_schema = build_target_arrow_schema(&read_type)?;
        let file_fields = data_fields.clone().unwrap_or_else(|| table_fields.clone());
        let is_row_file = is_row_file(&file_meta);

        // Compute index mapping and determine which columns to read from the file.
        let (projected_read_fields, index_mapping) = if let Some(ref df) = data_fields {
            let mapping = create_index_mapping(&read_type, df);
            let fields_to_read = read_data_fields(df, &read_type)?;
            (fields_to_read, mapping)
        } else {
            (
                read_type
                    .iter()
                    .filter(|field| field.name() != ROW_ID_FIELD_NAME)
                    .cloned()
                    .collect(),
                None,
            )
        };
        let format_read_fields = if is_row_file {
            file_fields.clone()
        } else {
            projected_read_fields
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
            let format_reader =
                create_format_reader(&path_to_read, blob_as_descriptor, &format_read_fields)?;
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
            let selected_row_ids = match (file_meta.first_row_id, row_selection.as_ref()) {
                (Some(first_row_id), Some(ranges)) => {
                    Some(expand_local_selected_row_ids(first_row_id, ranges))
                }
                _ => None,
            };
            let mut row_id_cursor = file_meta.first_row_id.unwrap_or(0);
            let mut row_id_offset = 0usize;

            let mut batch_stream = format_reader.read_batch_stream(
                Box::new(file_reader),
                file_meta.file_size as u64,
                &format_read_fields,
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
                    if target_field.name() == ROW_ID_FIELD_NAME {
                        columns.push(row_id_column_for_batch(
                            file_meta.first_row_id,
                            num_rows,
                            &mut row_id_cursor,
                            selected_row_ids.as_deref(),
                            &mut row_id_offset,
                        )?);
                        continue;
                    }

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

fn read_data_fields(
    all_data_fields: &[DataField],
    expected_fields: &[DataField],
) -> crate::Result<Vec<DataField>> {
    let mut read_fields = Vec::new();
    for data_field in all_data_fields {
        if let Some(expected) = expected_fields
            .iter()
            .find(|field| field.id() == data_field.id())
        {
            if let Some(pruned_type) =
                prune_data_type(expected.data_type(), data_field.data_type())?
            {
                read_fields.push(data_field_with_type(data_field, pruned_type));
            }
        }
    }
    Ok(read_fields)
}

fn prune_data_type(read_type: &DataType, data_type: &DataType) -> crate::Result<Option<DataType>> {
    match read_type {
        DataType::Row(read_row) if is_variant_extraction_row_type(read_type) => {
            Ok(Some(DataType::Row(read_row.clone())))
        }
        DataType::Row(read_row) => {
            let DataType::Row(data_row) = data_type else {
                return Ok(Some(data_type.clone()));
            };
            let mut fields = Vec::new();
            for read_field in read_row.fields() {
                if let Some(data_field) = data_row
                    .fields()
                    .iter()
                    .find(|field| field.id() == read_field.id())
                {
                    if let Some(pruned_type) =
                        prune_data_type(read_field.data_type(), data_field.data_type())?
                    {
                        fields.push(data_field_with_type(data_field, pruned_type));
                    }
                }
            }
            if fields.is_empty() {
                Ok(None)
            } else {
                Ok(Some(DataType::Row(crate::spec::RowType::with_nullable(
                    read_type.is_nullable(),
                    fields,
                ))))
            }
        }
        _ => Ok(Some(data_type.clone())),
    }
}

fn data_field_with_type(field: &DataField, data_type: DataType) -> DataField {
    DataField::new(field.id(), field.name().to_string(), data_type)
        .with_description(field.description().map(ToString::to_string))
}

fn is_row_file(file_meta: &DataFileMeta) -> bool {
    file_meta.file_name.to_ascii_lowercase().ends_with(".row")
        || file_meta
            .external_path
            .as_deref()
            .is_some_and(|path| path.to_ascii_lowercase().ends_with(".row"))
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

fn expand_local_selected_row_ids(first_row_id: i64, local_ranges: &[RowRange]) -> Vec<i64> {
    let mut ids = Vec::new();
    for range in local_ranges {
        for local_id in range.from()..=range.to() {
            ids.push(first_row_id + local_id);
        }
    }
    ids
}

fn row_id_column_for_batch(
    first_row_id: Option<i64>,
    num_rows: usize,
    row_id_cursor: &mut i64,
    selected_row_ids: Option<&[i64]>,
    row_id_offset: &mut usize,
) -> crate::Result<Arc<dyn arrow_array::Array>> {
    let Some(_) = first_row_id else {
        return Ok(Arc::new(Int64Array::new_null(num_rows)));
    };

    if let Some(selected_row_ids) = selected_row_ids {
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
        return Ok(Arc::new(Int64Array::from(batch_ids.to_vec())));
    }

    let start = *row_id_cursor;
    let end = start + num_rows as i64;
    *row_id_cursor = end;
    Ok(Arc::new(Int64Array::from((start..end).collect::<Vec<_>>())))
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

#[cfg(test)]
mod row_tests {
    use super::*;
    use crate::arrow::build_target_arrow_schema;
    use crate::arrow::format::create_format_writer;
    use crate::io::FileIOBuilder;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{
        is_variant_extraction_row_type, variant_extraction_row, BigIntType, BinaryRow,
        DataFileMeta, DataType, Datum, IntType, Predicate, PredicateBuilder, RowType, VarCharType,
    };
    use crate::table::source::DataSplitBuilder;
    use crate::variant::variant_shredding_type;
    use arrow_array::{Int32Array, StringArray};
    use futures::TryStreamExt;

    fn field(id: i32, name: &str, data_type: DataType) -> DataField {
        DataField::new(id, name.to_string(), data_type)
    }

    fn data_file(file_name: &str, file_size: i64, row_count: i64, schema_id: i64) -> DataFileMeta {
        DataFileMeta {
            file_name: file_name.to_string(),
            file_size,
            row_count,
            min_key: Vec::new(),
            max_key: Vec::new(),
            key_stats: BinaryTableStats::empty(),
            value_stats: BinaryTableStats::empty(),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id,
            level: 0,
            extra_files: Vec::new(),
            creation_time: None,
            delete_row_count: None,
            embedded_index: None,
            file_source: None,
            value_stats_cols: None,
            external_path: None,
            first_row_id: None,
            write_cols: None,
        }
    }

    #[test]
    fn read_data_fields_preserves_variant_extraction_row_type() {
        let configured = DataType::Row(RowType::new(vec![field(
            0,
            "age",
            DataType::Int(IntType::new()),
        )]));
        let physical_type = variant_shredding_type(&configured).unwrap();
        let data_field = field(1, "v", physical_type);
        let extraction_type = DataType::Row(variant_extraction_row(
            true,
            vec![(
                DataType::Int(IntType::new()),
                "$.age".to_string(),
                true,
                "UTC".to_string(),
            )],
        ));
        let expected_field = field(1, "v", extraction_type.clone());

        let read_fields = read_data_fields(&[data_field], &[expected_field]).unwrap();

        assert_eq!(read_fields.len(), 1);
        assert!(is_variant_extraction_row_type(read_fields[0].data_type()));
        assert_eq!(read_fields[0].data_type(), &extraction_type);
    }

    #[test]
    fn read_data_fields_uses_physical_type_for_castable_field() {
        let data_field = field(1, "n", DataType::Int(IntType::new()));
        let expected_field = field(1, "n", DataType::BigInt(BigIntType::new()));

        let read_fields = read_data_fields(&[data_field], &[expected_field]).unwrap();

        assert_eq!(read_fields.len(), 1);
        assert_eq!(read_fields[0].data_type(), &DataType::Int(IntType::new()));
    }

    #[tokio::test]
    async fn row_projection_reads_full_file_schema_before_projecting() {
        let fields = vec![
            field(0, "id", DataType::Int(IntType::new())),
            field(1, "name", DataType::VarChar(VarCharType::string_type())),
            field(2, "score", DataType::Int(IntType::new())),
        ];
        let schema = build_target_arrow_schema(&fields).unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/row_projection";
        let bucket_path = format!("{table_path}/bucket-0");
        let file_name = "part-0.row";
        let file_path = format!("{bucket_path}/{file_name}");
        let output = file_io.new_output(&file_path).unwrap();
        let mut writer = create_format_writer(&output, schema, "zstd", 1, None, None, None)
            .await
            .unwrap();
        writer.write(&batch).await.unwrap();
        let file_size = writer.close().await.unwrap() as i64;

        let schema_id = 1;
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket_path)
            .with_total_buckets(1)
            .with_data_files(vec![data_file(file_name, file_size, 3, schema_id)])
            .build()
            .unwrap();

        let read_type = vec![fields[2].clone()];
        let reader = DataFileReader::new(
            file_io.clone(),
            SchemaManager::new(file_io, table_path.to_string()),
            schema_id,
            fields,
            read_type,
            Vec::new(),
        );

        let batches = reader
            .read(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "score");
        let scores = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(scores.values(), &[10, 20, 30]);
    }

    /// End-to-end guard: a non-partition predicate on a `.row` file must be
    /// applied exactly by the time batches leave `DataFileReader`. The Row
    /// format has no pushdown, so without a residual filter this would return
    /// every row; the residual filter makes it exact. Guards against a
    /// regression if the per-reader wiring is later refactored.
    #[tokio::test]
    async fn row_read_applies_exact_residual_filter_end_to_end() {
        let fields = vec![
            field(0, "id", DataType::Int(IntType::new())),
            field(1, "age", DataType::Int(IntType::new())),
        ];
        let schema = build_target_arrow_schema(&fields).unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )
        .unwrap();

        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/row_residual";
        let bucket_path = format!("{table_path}/bucket-0");
        let file_name = "part-0.row";
        let file_path = format!("{bucket_path}/{file_name}");
        let output = file_io.new_output(&file_path).unwrap();
        let mut writer = create_format_writer(&output, schema, "zstd", 1, None, None, None)
            .await
            .unwrap();
        writer.write(&batch).await.unwrap();
        let file_size = writer.close().await.unwrap() as i64;

        let schema_id = 1;
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket_path)
            .with_total_buckets(1)
            .with_data_files(vec![data_file(file_name, file_size, 5, schema_id)])
            .build()
            .unwrap();

        // age > 25 -> only [30, 40, 50] must survive.
        let predicate: Predicate = PredicateBuilder::new(&fields)
            .greater_than("age", Datum::Int(25))
            .unwrap();
        let read_type = vec![fields[1].clone()];
        let reader = DataFileReader::new(
            file_io.clone(),
            SchemaManager::new(file_io, table_path.to_string()),
            schema_id,
            fields,
            read_type,
            vec![predicate],
        );

        let batches = reader
            .read(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let ages: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(ages, vec![30, 40, 50]);
    }

    /// Guard: projecting `_ROW_ID` together with a data predicate must fail
    /// loudly rather than assign wrong row ids. `_ROW_ID` is materialized
    /// positionally from post-filter batch row counts, so the readers' residual
    /// filter dropping rows would desync it. See the guard in `read()`.
    #[tokio::test]
    async fn read_rejects_row_id_projection_with_data_predicate() {
        // Write a real .row file so read() reaches read_single_file_stream (where
        // the guard lives). Project _ROW_ID alongside a data predicate → Unsupported.
        let fields = vec![
            field(0, "id", DataType::Int(IntType::new())),
            field(1, "age", DataType::Int(IntType::new())),
        ];
        let schema = build_target_arrow_schema(&fields).unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/row_id_guard";
        let bucket_path = format!("{table_path}/bucket-0");
        let file_name = "part-0.row";
        let output = file_io
            .new_output(&format!("{bucket_path}/{file_name}"))
            .unwrap();
        let mut writer = create_format_writer(&output, schema, "zstd", 1, None, None, None)
            .await
            .unwrap();
        writer.write(&batch).await.unwrap();
        let file_size = writer.close().await.unwrap() as i64;

        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket_path)
            .with_total_buckets(1)
            .with_data_files(vec![data_file(file_name, file_size, 3, 1)])
            .build()
            .unwrap();

        let row_id = DataField::new(
            crate::spec::ROW_ID_FIELD_ID,
            ROW_ID_FIELD_NAME.to_string(),
            DataType::BigInt(crate::spec::BigIntType::new()),
        );
        // read_type projects _ROW_ID alongside a real column; predicate on age.
        let read_type = vec![fields[1].clone(), row_id];
        let predicate: Predicate = PredicateBuilder::new(&fields)
            .greater_than("age", Datum::Int(25))
            .unwrap();

        let reader = DataFileReader::new(
            file_io.clone(),
            SchemaManager::new(file_io, table_path.to_string()),
            1,
            fields,
            read_type,
            vec![predicate],
        );

        // The guard is inside read_single_file_stream, reached while consuming the
        // stream, so the error surfaces on collect.
        let result = reader.read(&[split]).unwrap().try_collect::<Vec<_>>().await;
        let err = match result {
            Ok(_) => panic!("must reject _ROW_ID + predicate"),
            Err(err) => err,
        };
        assert!(
            matches!(&err, crate::Error::Unsupported { message } if message.contains("_ROW_ID")),
            "expected Unsupported mentioning _ROW_ID, got: {err:?}"
        );
    }

    #[test]
    fn reject_row_id_guard_allows_constant_always_true_predicate() {
        // A constant AlwaysTrue keeps every row in order, so it cannot desync
        // positional _ROW_ID and must NOT trip the guard.
        let row_id = DataField::new(
            crate::spec::ROW_ID_FIELD_ID,
            ROW_ID_FIELD_NAME.to_string(),
            DataType::BigInt(crate::spec::BigIntType::new()),
        );
        let read_type = vec![row_id];
        // AlwaysTrue alone -> allowed.
        assert!(
            DataFileReader::reject_row_id_with_predicate(&read_type, &[Predicate::AlwaysTrue])
                .is_ok(),
            "AlwaysTrue must not trip the _ROW_ID guard"
        );
        // A real filtering predicate -> rejected.
        let filtering = PredicateBuilder::new(&[field(0, "age", DataType::Int(IntType::new()))])
            .greater_than("age", Datum::Int(1))
            .unwrap();
        assert!(
            DataFileReader::reject_row_id_with_predicate(&read_type, &[filtering]).is_err(),
            "a row-filtering predicate must trip the _ROW_ID guard"
        );
    }
}

#[cfg(all(test, feature = "mosaic"))]
mod tests {
    use super::*;
    use crate::arrow::build_target_arrow_schema;
    use crate::io::FileIOBuilder;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{
        ArrayType, DataFileMeta, DataType, Datum, IntType, Predicate, PredicateBuilder, VarCharType,
    };
    use crate::table::source::{DataSplitBuilder, DeletionFile};
    use arrow_array::{Int32Array, StringArray};
    use bytes::Bytes;
    use futures::TryStreamExt;
    use paimon_mosaic_core::spec::COMPRESSION_NONE;
    use paimon_mosaic_core::writer::{MosaicWriter, OutputFile, WriterOptions};
    use roaring::RoaringBitmap;
    use std::io;

    struct MemOutputFile {
        data: Vec<u8>,
    }

    impl MemOutputFile {
        fn new() -> Self {
            Self { data: Vec::new() }
        }
    }

    impl OutputFile for MemOutputFile {
        fn write(&mut self, data: &[u8]) -> io::Result<()> {
            self.data.extend_from_slice(data);
            Ok(())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn pos(&self) -> u64 {
            self.data.len() as u64
        }
    }

    fn data_field(id: i32, name: &str, data_type: DataType) -> DataField {
        DataField::new(id, name.to_string(), data_type)
    }

    fn data_file(file_name: &str, file_size: i64, row_count: i64, schema_id: i64) -> DataFileMeta {
        DataFileMeta {
            file_name: file_name.to_string(),
            file_size,
            row_count,
            min_key: Vec::new(),
            max_key: Vec::new(),
            key_stats: BinaryTableStats::empty(),
            value_stats: BinaryTableStats::empty(),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id,
            level: 0,
            extra_files: Vec::new(),
            creation_time: None,
            delete_row_count: None,
            embedded_index: None,
            file_source: None,
            value_stats_cols: None,
            external_path: None,
            first_row_id: None,
            write_cols: None,
        }
    }

    fn write_mosaic(batch: &RecordBatch) -> Bytes {
        let out = MemOutputFile::new();
        let mut writer = MosaicWriter::new(
            out,
            batch.schema().as_ref(),
            WriterOptions {
                compression: COMPRESSION_NONE,
                num_buckets: 2,
                row_group_max_size: u64::MAX,
                ..Default::default()
            },
        )
        .unwrap();
        writer.write_batch(batch).unwrap();
        writer.close().unwrap();
        Bytes::from(writer.output().data.to_vec())
    }

    #[tokio::test]
    async fn test_mosaic_physical_missing_column_is_null_filled() {
        let physical_fields = vec![
            data_field(0, "id", DataType::Int(IntType::with_nullable(false))),
            data_field(
                1,
                "name",
                DataType::VarChar(VarCharType::with_nullable(true, 20).unwrap()),
            ),
        ];
        let read_fields = vec![
            physical_fields[0].clone(),
            data_field(
                2,
                "items",
                DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
            ),
            physical_fields[1].clone(),
        ];

        let physical_arrow_schema = build_target_arrow_schema(&physical_fields).unwrap();
        let batch = RecordBatch::try_new(
            physical_arrow_schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        let data = write_mosaic(&batch);

        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/mosaic_schema_evolution";
        let bucket_path = format!("{table_path}/bucket-0");
        let file_name = "part-0.mosaic";
        let file_path = format!("{bucket_path}/{file_name}");
        file_io
            .new_output(&file_path)
            .unwrap()
            .write(data.clone())
            .await
            .unwrap();

        let table_schema_id = 1;
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(crate::spec::BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket_path)
            .with_total_buckets(1)
            .with_data_files(vec![data_file(
                file_name,
                data.len() as i64,
                3,
                table_schema_id,
            )])
            .build()
            .unwrap();
        let schema_manager = SchemaManager::new(file_io.clone(), table_path.to_string());
        let reader = DataFileReader::new(
            file_io,
            schema_manager,
            table_schema_id,
            read_fields.clone(),
            read_fields.clone(),
            Vec::new(),
        );
        let stream = reader.read(&[split]).unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();

        assert_eq!(batches.len(), 1);
        let result = &batches[0];
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 3);
        assert_eq!(result.schema().field(0).name(), "id");
        assert_eq!(result.schema().field(1).name(), "items");
        assert_eq!(result.schema().field(2).name(), "name");
        assert_eq!(result.column(1).null_count(), 3);

        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.values(), &[1, 2, 3]);
        let names = result
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "a");
        assert_eq!(names.value(2), "c");
    }

    fn pk_fields() -> Vec<DataField> {
        vec![
            data_field(0, "id", DataType::Int(IntType::with_nullable(false))),
            data_field(
                1,
                "name",
                DataType::VarChar(VarCharType::with_nullable(true, 20).unwrap()),
            ),
        ]
    }

    fn pk_batch(ids: Vec<i32>, names: Vec<&str>) -> RecordBatch {
        RecordBatch::try_new(
            build_target_arrow_schema(&pk_fields()).unwrap(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    fn write_multi_row_group_mosaic(batches: &[RecordBatch], stats_columns: Vec<String>) -> Bytes {
        let out = MemOutputFile::new();
        let mut writer = MosaicWriter::new(
            out,
            batches[0].schema().as_ref(),
            WriterOptions {
                compression: COMPRESSION_NONE,
                num_buckets: 2,
                // One row group per written batch, so each batch carries its own stats.
                row_group_max_size: 1,
                stats_columns,
                ..Default::default()
            },
        )
        .unwrap();
        for batch in batches {
            writer.write_batch(batch).unwrap();
        }
        writer.close().unwrap();
        Bytes::from(writer.output().data.to_vec())
    }

    fn write_parquet(batch: &RecordBatch) -> Bytes {
        let mut buf = Vec::new();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
        Bytes::from(buf)
    }

    /// Writes a Paimon deletion-vector blob and returns the `DeletionFile` pointing at it.
    /// Layout matches [`DeletionVector::read_from_bytes`]:
    /// `i32 bitmapLength (magic + bitmap) | i32 magic | bitmap bytes | i32 crc`.
    async fn write_deletion_file(
        file_io: &crate::io::FileIO,
        path: &str,
        deleted_rows: &[u32],
    ) -> DeletionFile {
        // BitmapDeletionVector.MAGIC_NUMBER, see crate::deletion_vector.
        const MAGIC_NUMBER: i32 = 1581511376;
        let mut bitmap = RoaringBitmap::new();
        for row in deleted_rows {
            bitmap.insert(*row);
        }
        let mut bitmap_bytes = Vec::new();
        bitmap.serialize_into(&mut bitmap_bytes).unwrap();

        let bitmap_length = 4 + bitmap_bytes.len() as i32;
        let mut blob = Vec::new();
        blob.extend_from_slice(&bitmap_length.to_be_bytes());
        blob.extend_from_slice(&MAGIC_NUMBER.to_be_bytes());
        blob.extend_from_slice(&bitmap_bytes);
        blob.extend_from_slice(&0i32.to_be_bytes()); // crc, skipped on read
        file_io
            .new_output(path)
            .unwrap()
            .write(Bytes::from(blob))
            .await
            .unwrap();

        DeletionFile::new(
            path.to_string(),
            0,
            bitmap_length as i64,
            Some(deleted_rows.len() as i64),
        )
    }

    fn collect_ids(batches: &[RecordBatch]) -> Vec<i32> {
        batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    /// Deletion vectors are applied format-agnostically by `DataFileReader`; verify a
    /// Mosaic file honors deleted rows end to end.
    #[tokio::test]
    async fn test_mosaic_with_deletion_vector() {
        let fields = pk_fields();
        let data = write_mosaic(&pk_batch(vec![1, 2, 3], vec!["a", "b", "c"]));

        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/mosaic_dv";
        let bucket_path = format!("{table_path}/bucket-0");
        let file_name = "part-0.mosaic";
        file_io
            .new_output(&format!("{bucket_path}/{file_name}"))
            .unwrap()
            .write(data.clone())
            .await
            .unwrap();
        // Delete row index 1 (id = 2).
        let dv = write_deletion_file(&file_io, &format!("{table_path}/index/dv-0"), &[1]).await;

        let table_schema_id = 1;
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(crate::spec::BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket_path)
            .with_total_buckets(1)
            .with_data_files(vec![data_file(
                file_name,
                data.len() as i64,
                3,
                table_schema_id,
            )])
            .with_data_deletion_files(vec![Some(dv)])
            .build()
            .unwrap();
        let schema_manager = SchemaManager::new(file_io.clone(), table_path.to_string());
        let reader = DataFileReader::new(
            file_io,
            schema_manager,
            table_schema_id,
            fields.clone(),
            fields.clone(),
            Vec::new(),
        );
        let batches = reader
            .read(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(collect_ids(&batches), vec![1, 3]);
    }

    /// A Mosaic file and a Parquet file in the same split must both be read and concatenated.
    #[tokio::test]
    async fn test_mosaic_mixed_format_read() {
        let fields = pk_fields();
        let mosaic_data = write_mosaic(&pk_batch(vec![1, 2], vec!["a", "b"]));
        let parquet_data = write_parquet(&pk_batch(vec![3, 4], vec!["c", "d"]));

        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/mosaic_mixed";
        let bucket_path = format!("{table_path}/bucket-0");
        for (name, data) in [
            ("part-0.mosaic", &mosaic_data),
            ("part-1.parquet", &parquet_data),
        ] {
            file_io
                .new_output(&format!("{bucket_path}/{name}"))
                .unwrap()
                .write(data.clone())
                .await
                .unwrap();
        }

        let table_schema_id = 1;
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(crate::spec::BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket_path)
            .with_total_buckets(1)
            .with_data_files(vec![
                data_file(
                    "part-0.mosaic",
                    mosaic_data.len() as i64,
                    2,
                    table_schema_id,
                ),
                data_file(
                    "part-1.parquet",
                    parquet_data.len() as i64,
                    2,
                    table_schema_id,
                ),
            ])
            .build()
            .unwrap();
        let schema_manager = SchemaManager::new(file_io.clone(), table_path.to_string());
        let reader = DataFileReader::new(
            file_io,
            schema_manager,
            table_schema_id,
            fields.clone(),
            fields.clone(),
            Vec::new(),
        );
        let batches = reader
            .read(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let mut ids = collect_ids(&batches);
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2, 3, 4]);
    }

    /// Row-group predicate pruning, deletion vectors and projection must compose correctly:
    /// the predicate keeps one row group, the DV deletes one of its rows, projection keeps `id`.
    #[tokio::test]
    async fn test_mosaic_predicate_dv_projection_combination() {
        let fields = pk_fields();
        let data = write_multi_row_group_mosaic(
            &[
                pk_batch(vec![1, 2], vec!["a", "b"]),
                pk_batch(vec![10, 11], vec!["c", "d"]),
                pk_batch(vec![20, 21], vec!["e", "f"]),
            ],
            vec!["id".to_string()],
        );

        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/mosaic_combo";
        let bucket_path = format!("{table_path}/bucket-0");
        let file_name = "part-0.mosaic";
        file_io
            .new_output(&format!("{bucket_path}/{file_name}"))
            .unwrap()
            .write(data.clone())
            .await
            .unwrap();
        // Delete global row index 2 (id = 10, first row of the second row group).
        let dv = write_deletion_file(&file_io, &format!("{table_path}/index/dv-0"), &[2]).await;

        let table_schema_id = 1;
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(crate::spec::BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket_path)
            .with_total_buckets(1)
            .with_data_files(vec![data_file(
                file_name,
                data.len() as i64,
                6,
                table_schema_id,
            )])
            .with_data_deletion_files(vec![Some(dv)])
            .build()
            .unwrap();

        let predicate: Predicate = PredicateBuilder::new(&fields)
            .equal("id", Datum::Int(10))
            .unwrap();
        let read_type = vec![fields[0].clone()];
        let schema_manager = SchemaManager::new(file_io.clone(), table_path.to_string());
        let reader = DataFileReader::new(
            file_io,
            schema_manager,
            table_schema_id,
            fields.clone(),
            read_type,
            vec![predicate],
        );
        let batches = reader
            .read(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(batches.iter().map(|b| b.num_columns()).max(), Some(1));
        assert_eq!(collect_ids(&batches), vec![11]);
    }
}

/// Parquet-only end-to-end tests for the inline VECTOR (`FixedSizeList`) read path.
///
/// This module is deliberately NOT gated behind the `mosaic` feature: the vector
/// read capability is core parquet support, so these tests must run under a plain
/// `cargo test -p paimon`.
#[cfg(test)]
mod vector_parquet_tests {
    use super::*;
    use crate::arrow::format::FormatFileWriter;
    use crate::arrow::format::ParquetFormatWriter;
    use crate::io::FileIOBuilder;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{DataFileMeta, DataType, FloatType, VectorType};
    use crate::table::source::DataSplitBuilder;
    use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
    use arrow_array::{FixedSizeListArray, Float32Array, RecordBatch};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField};
    use futures::TryStreamExt;

    fn data_file(file_name: &str, file_size: i64, row_count: i64, schema_id: i64) -> DataFileMeta {
        DataFileMeta {
            file_name: file_name.to_string(),
            file_size,
            row_count,
            min_key: Vec::new(),
            max_key: Vec::new(),
            key_stats: BinaryTableStats::empty(),
            value_stats: BinaryTableStats::empty(),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id,
            level: 0,
            extra_files: Vec::new(),
            creation_time: None,
            delete_row_count: None,
            embedded_index: None,
            file_source: None,
            value_stats_cols: None,
            external_path: None,
            first_row_id: None,
            write_cols: None,
        }
    }

    /// TRUE end-to-end: write a parquet data file containing a `FixedSizeList<Float32, 2>`
    /// column, then read it back through `DataFileReader` using a Paimon `read_type` whose
    /// field is `DataType::Vector`. This exercises `build_target_arrow_schema`, the parquet
    /// format dispatch (by `.parquet` extension), and the read path's pass-through/cast
    /// logic — not just a raw Arrow/parquet round-trip.
    #[tokio::test]
    async fn test_datafilereader_inline_vector_column_e2e() {
        // Paimon read schema: a single nullable VECTOR<FLOAT> column of length 2.
        let vector_type = VectorType::try_new(true, 2, DataType::Float(FloatType::new())).unwrap();
        let read_fields = vec![DataField::new(
            0,
            "embedding".to_string(),
            DataType::Vector(vector_type),
        )];

        // Build the physical Arrow data via the Paimon -> Arrow conversion under test,
        // so the parquet file matches what the read path expects to materialize.
        let arrow_schema = build_target_arrow_schema(&read_fields).unwrap();

        // Build a FixedSizeList<Float32, 2> column:
        //   row 0 = [1.0, 2.0]   (non-null)
        //   row 1 = null         (null vector row)
        //   row 2 = [3.0, 4.0]   (non-null)
        let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), 2).with_field(Arc::new(
            ArrowField::new("element", ArrowDataType::Float32, true),
        ));
        builder.values().append_value(1.0);
        builder.values().append_value(2.0);
        builder.append(true);
        builder.values().append_value(0.0);
        builder.values().append_value(0.0);
        builder.append(false); // null vector row
        builder.values().append_value(3.0);
        builder.values().append_value(4.0);
        builder.append(true);
        let vec_array = builder.finish();
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(vec_array)]).unwrap();

        // Write the data file as parquet into the split's bucket path.
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/vector_inline_e2e";
        let bucket_path = format!("{table_path}/bucket-0");
        let file_name = "part-0.parquet";
        let file_path = format!("{bucket_path}/{file_name}");
        let output = file_io.new_output(&file_path).unwrap();
        let mut writer: Box<dyn FormatFileWriter> = Box::new(
            ParquetFormatWriter::new(&output, arrow_schema.clone(), "zstd", 1)
                .await
                .unwrap(),
        );
        writer.write(&batch).await.unwrap();
        let file_size = writer.close().await.unwrap();

        // Build a split whose data file's schema_id matches the table schema_id, so the
        // read path uses `read_type` directly (no SchemaManager lookup needed).
        let table_schema_id = 1;
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(crate::spec::BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket_path)
            .with_total_buckets(1)
            .with_data_files(vec![data_file(
                file_name,
                file_size as i64,
                3,
                table_schema_id,
            )])
            .build()
            .unwrap();

        let schema_manager = SchemaManager::new(file_io.clone(), table_path.to_string());
        let reader = DataFileReader::new(
            file_io,
            schema_manager,
            table_schema_id,
            read_fields.clone(),
            read_fields.clone(),
            Vec::new(),
        );
        let batches = reader
            .read(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
        let result = &batches[0];
        assert_eq!(result.num_columns(), 1);
        assert_eq!(result.schema().field(0).name(), "embedding");

        // The materialized column must be a FixedSizeListArray with the right length,
        // child Float32 values, and null bitmap (one non-null and one null row).
        let fsl = result
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("column should materialize as FixedSizeListArray");
        assert_eq!(fsl.value_length(), 2);
        assert!(fsl.is_valid(0));
        assert!(fsl.is_null(1)); // null vector row preserved through the read path
        assert!(fsl.is_valid(2));

        let row0 = fsl.value(0);
        let floats0 = row0
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("child should be Float32Array");
        assert_eq!(floats0.values(), &[1.0, 2.0]);

        let row2 = fsl.value(2);
        let floats2 = row2
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("child should be Float32Array");
        assert_eq!(floats2.values(), &[3.0, 4.0]);
    }
}
