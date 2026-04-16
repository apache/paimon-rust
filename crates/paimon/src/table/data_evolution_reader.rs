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

use super::data_file_reader::{
    append_null_row_id_column, attach_row_id, expand_selected_row_ids, insert_column_at,
    DataFileReader,
};
use crate::arrow::build_target_arrow_schema;
use crate::io::FileIO;
use crate::spec::{DataField, DataFileMeta, ROW_ID_FIELD_NAME};
use crate::table::schema_manager::SchemaManager;
use crate::table::ArrowRecordBatchStream;
use crate::table::RowRange;
use crate::{DataSplit, Error};
use arrow_array::{Array, Int64Array, RecordBatch};

use async_stream::try_stream;
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;

/// Whether the files in a split can be read independently (no column-wise merge needed).
fn is_raw_convertible(files: &[DataFileMeta]) -> bool {
    if files.len() <= 1 {
        return true;
    }
    // If all files have first_row_id and their row_id ranges don't overlap, they're independent.
    if files.iter().any(|f| f.first_row_id.is_none()) {
        return false;
    }
    let mut ranges: Vec<(i64, i64)> = files
        .iter()
        .map(|f| {
            let start = f.first_row_id.unwrap();
            (start, start + f.row_count)
        })
        .collect();
    ranges.sort_by_key(|r| r.0);
    for w in ranges.windows(2) {
        if w[0].1 > w[1].0 {
            return false;
        }
    }
    true
}

/// Reads data files in data evolution mode, merging columns from files
/// that share the same row ID range.
pub(crate) struct DataEvolutionReader {
    file_io: FileIO,
    schema_manager: SchemaManager,
    table_schema_id: i64,
    table_fields: Vec<DataField>,
    /// read_type with _ROW_ID filtered out — used for file reads.
    file_read_type: Vec<DataField>,
    /// Position of _ROW_ID in the original read_type, if requested.
    row_id_index: Option<usize>,
    /// Arrow schema for the full output (including _ROW_ID if requested).
    output_schema: Arc<arrow_schema::Schema>,
}

impl DataEvolutionReader {
    pub(crate) fn new(
        file_io: FileIO,
        schema_manager: SchemaManager,
        table_schema_id: i64,
        table_fields: Vec<DataField>,
        read_type: Vec<DataField>,
    ) -> crate::Result<Self> {
        let row_id_index = read_type.iter().position(|f| f.name() == ROW_ID_FIELD_NAME);
        let file_read_type: Vec<DataField> = read_type
            .iter()
            .filter(|f| f.name() != ROW_ID_FIELD_NAME)
            .cloned()
            .collect();
        let output_schema = build_target_arrow_schema(&read_type)?;

        Ok(Self {
            file_io,
            schema_manager,
            table_schema_id,
            table_fields,
            file_read_type,
            row_id_index,
            output_schema,
        })
    }

    /// Read data files in data evolution mode.
    ///
    /// Each DataSplit contains files grouped by `first_row_id`. Files within a split may contain
    /// different columns for the same logical rows. This method reads each file and merges them
    /// column-wise, respecting `max_sequence_number` for conflict resolution.
    pub fn read(self, data_splits: &[DataSplit]) -> crate::Result<ArrowRecordBatchStream> {
        let splits: Vec<DataSplit> = data_splits.to_vec();

        Ok(try_stream! {
            let file_reader = DataFileReader::new(
                self.file_io.clone(),
                self.schema_manager.clone(),
                self.table_schema_id,
                self.table_fields.clone(),
                self.file_read_type.clone(),
                Vec::new(),
            );

            for split in splits {
                let row_ranges = split.row_ranges().map(|r| r.to_vec());

                if is_raw_convertible(split.data_files()) {
                    for file_meta in split.data_files().to_vec() {
                        let data_fields: Option<Vec<DataField>> = if file_meta.schema_id != self.table_schema_id {
                            let data_schema = self.schema_manager.schema(file_meta.schema_id).await?;
                            Some(data_schema.fields().to_vec())
                        } else {
                            None
                        };

                        let has_row_id = file_meta.first_row_id.is_some();
                        let effective_row_ranges = if has_row_id { row_ranges.clone() } else { None };

                        let selected_row_ids = if self.row_id_index.is_some() && has_row_id {
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

                        let mut stream = file_reader.read_single_file_stream(
                            &split,
                            file_meta,
                            data_fields,
                            None,
                            effective_row_ranges,
                        )?;
                        while let Some(batch) = stream.next().await {
                            let batch = batch?;
                            let num_rows = batch.num_rows();
                            if let Some(idx) = self.row_id_index {
                                if !has_row_id {
                                    yield append_null_row_id_column(batch, idx, &self.output_schema)?;
                                } else if let Some(ref ids) = selected_row_ids {
                                    yield attach_row_id(batch, idx, ids, &mut row_id_offset, &self.output_schema)?;
                                } else {
                                    let row_ids: Vec<i64> = (row_id_cursor..row_id_cursor + num_rows as i64).collect();
                                    row_id_cursor += num_rows as i64;
                                    let array: Arc<dyn arrow_array::Array> = Arc::new(Int64Array::from(row_ids));
                                    yield insert_column_at(batch, array, idx, &self.output_schema)?;
                                }
                            } else {
                                yield batch;
                            }
                        }
                    }
                } else {
                    let files = split.data_files();
                    if !files.iter().all(|f| f.first_row_id.is_some()) {
                        Err(Error::UnexpectedError {
                            message: "All files in a field merge split should have first_row_id".to_string(),
                            source: None,
                        })?;
                    }
                    if !files.iter().all(|f| f.row_count == files[0].row_count) {
                        Err(Error::UnexpectedError {
                            message: "All files in a field merge split should have the same row count".to_string(),
                            source: None,
                        })?;
                    }
                    if !files.iter().all(|f| f.first_row_id == files[0].first_row_id) {
                        Err(Error::UnexpectedError {
                            message: "All files in a field merge split should have the same first row id".to_string(),
                            source: None,
                        })?;
                    }

                    let group_base_row_id = files[0].first_row_id;
                    let has_group_row_id = group_base_row_id.is_some();
                    let group_row_count = files[0].row_count;
                    let effective_row_ranges = if has_group_row_id { row_ranges.clone() } else { None };

                    let selected_row_ids = if self.row_id_index.is_some() && has_group_row_id {
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

                    let mut merge_stream = self.merge_files_by_columns(
                        &split,
                        effective_row_ranges,
                    )?;
                    while let Some(batch) = merge_stream.next().await {
                        let batch = batch?;
                        let num_rows = batch.num_rows();
                        if let Some(idx) = self.row_id_index {
                            if !has_group_row_id {
                                yield append_null_row_id_column(batch, idx, &self.output_schema)?;
                            } else if let Some(ref ids) = selected_row_ids {
                                yield attach_row_id(batch, idx, ids, &mut row_id_offset, &self.output_schema)?;
                            } else {
                                let row_ids: Vec<i64> = (row_id_cursor..row_id_cursor + num_rows as i64).collect();
                                row_id_cursor += num_rows as i64;
                                let array: Arc<dyn arrow_array::Array> = Arc::new(Int64Array::from(row_ids));
                                yield insert_column_at(batch, array, idx, &self.output_schema)?;
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

    /// Merge multiple files column-wise for data evolution, streaming with bounded memory.
    ///
    /// Uses field IDs (not column names) to resolve which file provides which column,
    /// ensuring correctness across schema evolution (column rename, add, drop).
    fn merge_files_by_columns(
        &self,
        split: &DataSplit,
        row_ranges: Option<Vec<RowRange>>,
    ) -> crate::Result<ArrowRecordBatchStream> {
        let data_files = split.data_files();
        if data_files.is_empty() {
            return Ok(futures::stream::empty().boxed());
        }

        let file_io = self.file_io.clone();
        let schema_manager = self.schema_manager.clone();
        let table_schema_id = self.table_schema_id;
        let split = split.clone();
        let data_files: Vec<DataFileMeta> = data_files.to_vec();
        let read_type = self.file_read_type.clone();
        let table_fields = self.table_fields.clone();
        // Batch size for column-merge output. Matches the default Parquet reader batch size.
        const MERGE_BATCH_SIZE: usize = 1024;
        let output_batch_size: usize = MERGE_BATCH_SIZE;
        let target_schema = build_target_arrow_schema(&read_type)?;

        Ok(try_stream! {
            // Pre-load schemas and collect field IDs + data_fields per file.
            let mut file_info: HashMap<usize, (Vec<i32>, Option<Vec<DataField>>)> = HashMap::new();

            for (file_idx, file_meta) in data_files.iter().enumerate() {
                let (field_ids, data_fields) = if file_meta.schema_id != table_schema_id {
                    let file_schema = schema_manager.schema(file_meta.schema_id).await?;
                    let file_fields = file_schema.fields();

                    let ids: Vec<i32> = if let Some(ref wc) = file_meta.write_cols {
                        wc.iter()
                            .filter_map(|name| file_fields.iter().find(|f| f.name() == name).map(|f| f.id()))
                            .collect()
                    } else {
                        file_fields.iter().map(|f| f.id()).collect()
                    };

                    (ids, Some(file_fields.to_vec()))
                } else {
                    let ids: Vec<i32> = if let Some(ref wc) = file_meta.write_cols {
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
            let mut file_read_columns: HashMap<usize, Vec<String>> = HashMap::new();
            for field in &read_type {
                if let Some(&(file_idx, _)) = field_id_source.get(&field.id()) {
                    file_read_columns
                        .entry(file_idx)
                        .or_default()
                        .push(field.name().to_string());
                }
            }

            let column_plan: Vec<(Option<usize>, String)> = read_type
                .iter()
                .map(|field| {
                    let file_idx = field_id_source.get(&field.id()).map(|&(idx, _)| idx);
                    (file_idx, field.name().to_string())
                })
                .collect();

            let active_file_indices: Vec<usize> = file_read_columns.keys().copied().collect();

            // Edge case: no file provides any projected column.
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
                // Open a stream for each active file via DataFileReader.
                let mut file_streams: HashMap<usize, ArrowRecordBatchStream> = HashMap::new();
                for &file_idx in &active_file_indices {
                    let file_cols = file_read_columns.get(&file_idx).cloned().unwrap_or_default();
                    let file_read_type: Vec<DataField> = file_cols
                        .iter()
                        .filter_map(|col_name| read_type.iter().find(|f| f.name() == col_name).cloned())
                        .collect();

                    let (_, ref data_fields) = file_info[&file_idx];

                    let file_reader = DataFileReader::new(
                        file_io.clone(),
                        schema_manager.clone(),
                        table_schema_id,
                        table_fields.clone(),
                        file_read_type,
                        Vec::new(),
                    );
                    let stream = file_reader.read_single_file_stream(
                        &split,
                        data_files[file_idx].clone(),
                        data_fields.clone(),
                        None,
                        row_ranges.clone(),
                    )?;
                    file_streams.insert(file_idx, stream);
                }

                // Per-file cursor: current batch + offset within it.
                let mut file_cursors: HashMap<usize, (RecordBatch, usize)> = HashMap::new();

                loop {
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

                    // All files in a merge group have the same row count (validated above),
                    // so any file stream exhausting means all streams are done.
                    if active_file_indices.iter().any(|idx| !file_cursors.contains_key(idx)) {
                        break;
                    }

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
            }
        }
        .boxed())
    }
}
