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
use crate::deletion_vector::{DeletionVector, DeletionVectorFactory};
use crate::io::FileIO;
use crate::spec::{DataField, DataFileMeta, DataType, ROW_ID_FIELD_NAME};
use crate::table::blob_file_writer::is_blob_file_name;
use crate::table::schema_manager::SchemaManager;
use crate::table::ArrowRecordBatchStream;
use crate::table::RowRange;
use crate::{DataSplit, Error};
use arrow_array::{Array, Int64Array, RecordBatch};
use async_stream::try_stream;
use futures::StreamExt;
use roaring::RoaringBitmap;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Whether a file name denotes a dedicated vector-store file (`*.vector.<format>`).
/// Mirrors upstream `VectorType.isVectorStoreFile`: the name contains `.vector.`.
fn is_vector_store_file_name(file_name: &str) -> bool {
    file_name.to_ascii_lowercase().contains(".vector.")
}

/// Whether the files in a split can be read independently (no column-wise merge needed).
fn is_raw_convertible(files: &[DataFileMeta]) -> bool {
    // A split containing a dedicated vector file must go through the column-merge
    // path so vector columns are routed to their VectorBunch source. Check this
    // BEFORE the single-file early-return.
    if files
        .iter()
        .any(|file| is_vector_store_file_name(&file.file_name))
    {
        return false;
    }
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
/// that share the same logical row range.
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
    blob_as_descriptor: bool,
    blob_descriptor_fields: HashSet<String>,
}

impl DataEvolutionReader {
    pub(crate) fn new(
        file_io: FileIO,
        schema_manager: SchemaManager,
        table_schema_id: i64,
        table_fields: Vec<DataField>,
        read_type: Vec<DataField>,
        blob_as_descriptor: bool,
        blob_descriptor_fields: HashSet<String>,
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
            blob_as_descriptor,
            blob_descriptor_fields,
        })
    }

    /// Read data files in data evolution mode.
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
                        let deletion_vector = read_file_deletion_vector(
                            &self.file_io,
                            &split,
                            &file_meta,
                        )
                        .await?;
                        let data_fields: Option<Vec<DataField>> =
                            if file_meta.schema_id != self.table_schema_id {
                                let data_schema =
                                    self.schema_manager.schema(file_meta.schema_id).await?;
                                Some(data_schema.fields().to_vec())
                            } else {
                                None
                            };

                        let has_row_id = file_meta.first_row_id.is_some();
                        let effective_row_ranges = if has_row_id { row_ranges.clone() } else { None };

                        let selected_row_ids = if self.row_id_index.is_some() && has_row_id {
                            selected_absolute_row_ranges_for_file(
                                file_meta.first_row_id.unwrap(),
                                file_meta.row_count,
                                effective_row_ranges.as_deref(),
                                deletion_vector.as_deref(),
                            )?
                            .map(|ranges| {
                                expand_selected_row_ids(
                                    file_meta.first_row_id.unwrap(),
                                    file_meta.row_count,
                                    &ranges,
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
                            deletion_vector,
                            effective_row_ranges,
                        )?;
                        while let Some(batch) = stream.next().await {
                            let batch = batch?;
                            let batch = if !self.blob_as_descriptor && !self.blob_descriptor_fields.is_empty() {
                                resolve_descriptor_columns(batch, &self.blob_descriptor_fields, &self.file_io).await?
                            } else {
                                batch
                            };
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
                    let prepared_group = PreparedMergeGroup::new(split.data_files())?;
                    let anchor_deletion_vector = read_anchor_deletion_vector(
                        &self.file_io,
                        &split,
                        &prepared_group.files,
                    )
                    .await?;
                    let effective_row_ranges = row_ranges.clone();
                    let selected_ranges = selected_absolute_row_ranges_for_file(
                        prepared_group.first_row_id,
                        prepared_group.logical_row_count,
                        effective_row_ranges.as_deref(),
                        anchor_deletion_vector
                            .as_ref()
                            .map(|ctx| ctx.deletion_vector.as_ref()),
                    )?;
                    let expected_output_rows = match selected_ranges.as_ref() {
                        Some(ranges) => ranges.iter().map(|r| r.count() as usize).sum(),
                        None => prepared_group.logical_row_count as usize,
                    };

                    let selected_row_ids = if self.row_id_index.is_some() {
                        selected_ranges.as_ref().map(|ranges| {
                            expand_selected_row_ids(
                                prepared_group.first_row_id,
                                prepared_group.logical_row_count,
                                ranges,
                            )
                        })
                    } else {
                        None
                    };
                    let mut row_id_cursor = prepared_group.first_row_id;
                    let mut row_id_offset: usize = 0;

                    let mut merge_stream = self.merge_files_by_columns(
                        &split,
                        &prepared_group,
                        effective_row_ranges,
                        expected_output_rows,
                        anchor_deletion_vector,
                    )?;
                    while let Some(batch) = merge_stream.next().await {
                        let batch = batch?;
                        let num_rows = batch.num_rows();
                        if let Some(idx) = self.row_id_index {
                            if let Some(ref ids) = selected_row_ids {
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

    /// Merge multiple logical sources column-wise for data evolution.
    ///
    /// Normal partial-column files remain one source per file. Rolling `.blob`
    /// files are first grouped into a logical BlobBunch source per field, then
    /// source streams are merged by projected field position.
    fn merge_files_by_columns(
        &self,
        split: &DataSplit,
        prepared_group: &PreparedMergeGroup,
        row_ranges: Option<Vec<RowRange>>,
        expected_output_rows: usize,
        anchor_deletion_vector: Option<DeletionVectorContext>,
    ) -> crate::Result<ArrowRecordBatchStream> {
        if prepared_group.files.is_empty() {
            return Ok(futures::stream::empty().boxed());
        }

        let file_io = self.file_io.clone();
        let schema_manager = self.schema_manager.clone();
        let table_schema_id = self.table_schema_id;
        let split = split.clone();
        let prepared_group = prepared_group.clone();
        let read_type = self.file_read_type.clone();
        let table_fields = self.table_fields.clone();
        let blob_descriptor_fields = self.blob_descriptor_fields.clone();
        let blob_as_descriptor = self.blob_as_descriptor;
        let anchor_deletion_vector = anchor_deletion_vector.clone();
        // Batch size for column-merge output. Matches the default Parquet reader batch size.
        const MERGE_BATCH_SIZE: usize = 1024;
        let target_schema = build_target_arrow_schema(&read_type)?;

        Ok(try_stream! {
            let file_infos = load_file_infos(
                &schema_manager,
                table_schema_id,
                &table_fields,
                &prepared_group.files,
            )
            .await?;
            let source_plan = build_source_plan(&prepared_group, &file_infos, &read_type, &blob_descriptor_fields)?;

            let active_source_indices: Vec<usize> = source_plan
                .sources
                .iter()
                .enumerate()
                .filter_map(|(idx, source)| (!source.read_fields().is_empty()).then_some(idx))
                .collect();

            // Edge case: no file provides any projected column.
            if active_source_indices.is_empty() {
                let mut emitted = 0usize;
                while emitted < expected_output_rows {
                    let rows_to_emit = (expected_output_rows - emitted).min(MERGE_BATCH_SIZE);
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
                return;
            }

            let mut source_streams: Vec<Option<ArrowRecordBatchStream>> = source_plan
                .sources
                .iter()
                .map(|source| {
                    if source.read_fields().is_empty() {
                        Ok(None)
                    } else {
                        open_source_stream(
                            &split,
                            source,
                            row_ranges.clone(),
                            file_io.clone(),
                            schema_manager.clone(),
                            table_schema_id,
                            table_fields.clone(),
                            blob_as_descriptor,
                            anchor_deletion_vector.as_ref(),
                        )
                        .map(Some)
                    }
                })
                .collect::<crate::Result<_>>()?;
            let mut source_cursors: Vec<Option<(RecordBatch, usize)>> = source_plan
                .sources
                .iter()
                .map(|_| None)
                .collect();
            let mut emitted_rows = 0usize;

            loop {
                for &source_idx in &active_source_indices {
                    let needs_next = match source_cursors[source_idx].as_ref() {
                        None => true,
                        Some((batch, offset)) => *offset >= batch.num_rows(),
                    };
                    if needs_next {
                        source_cursors[source_idx] = None;
                        if let Some(stream) = source_streams[source_idx].as_mut() {
                            while let Some(batch_result) = stream.next().await {
                                let batch = batch_result?;
                                if batch.num_rows() == 0 {
                                    continue;
                                }
                                source_cursors[source_idx] = Some((batch, 0));
                                break;
                            }
                        }
                    }
                }

                let finished_sources = active_source_indices
                    .iter()
                    .filter(|&&idx| source_cursors[idx].is_none())
                    .count();
                if finished_sources > 0 {
                    if finished_sources == active_source_indices.len() {
                        if emitted_rows != expected_output_rows {
                            Err(Error::DataInvalid {
                                message: format!(
                                    "Merged data evolution sources produced {emitted_rows} rows but expected {expected_output_rows}"
                                ),
                                source: None,
                            })?;
                        }
                        break;
                    }

                    Err(Error::DataInvalid {
                        message: "Data evolution sources exhausted at different row counts".to_string(),
                        source: None,
                    })?;
                }

                let remaining = active_source_indices
                    .iter()
                    .map(|&idx| {
                        let (batch, offset) = source_cursors[idx].as_ref().unwrap();
                        batch.num_rows() - offset
                    })
                    .min()
                    .unwrap_or(0);
                if remaining == 0 {
                    Err(Error::UnexpectedError {
                        message: "Data evolution source cursor reached an empty batch".to_string(),
                        source: None,
                    })?;
                }

                let rows_to_emit = remaining.min(MERGE_BATCH_SIZE);
                let mut columns: Vec<Arc<dyn arrow_array::Array>> =
                    Vec::with_capacity(source_plan.column_plan.len());

                for (idx, provider) in source_plan.column_plan.iter().enumerate() {
                    let target_field = &target_schema.fields()[idx];
                    let array = provider
                        .and_then(|(source_idx, field_offset)| {
                            source_cursors[source_idx].as_ref().map(|(batch, offset)| {
                                batch.column(field_offset).slice(*offset, rows_to_emit)
                            })
                        })
                        .unwrap_or_else(|| {
                            arrow_array::new_null_array(target_field.data_type(), rows_to_emit)
                        });
                    columns.push(array);
                }

                for &source_idx in &active_source_indices {
                    if let Some((_, offset)) = source_cursors[source_idx].as_mut() {
                        *offset += rows_to_emit;
                    }
                }

                emitted_rows += rows_to_emit;
                let merged =
                    RecordBatch::try_new(target_schema.clone(), columns).map_err(|e| {
                        Error::UnexpectedError {
                            message: format!("Failed to build merged RecordBatch: {e}"),
                            source: Some(Box::new(e)),
                        }
                    })?;
                let merged = if !blob_as_descriptor && !blob_descriptor_fields.is_empty() {
                    resolve_descriptor_columns(merged, &blob_descriptor_fields, &file_io).await?
                } else {
                    merged
                };
                yield merged;
            }
        }
        .boxed())
    }
}

async fn resolve_descriptor_columns(
    batch: RecordBatch,
    blob_descriptor_fields: &HashSet<String>,
    file_io: &FileIO,
) -> crate::Result<RecordBatch> {
    let schema = batch.schema();
    let mut columns: Vec<Arc<dyn arrow_array::Array>> = Vec::with_capacity(batch.num_columns());
    let mut changed = false;

    for (idx, field) in schema.fields().iter().enumerate() {
        if blob_descriptor_fields.contains(field.name()) {
            if let Some(bin_col) = batch
                .column(idx)
                .as_any()
                .downcast_ref::<arrow_array::BinaryArray>()
            {
                let resolved =
                    super::blob_file_writer::resolve_blob_column(bin_col, file_io).await?;
                columns.push(Arc::new(resolved));
                changed = true;
                continue;
            }
        }
        columns.push(batch.column(idx).clone());
    }

    if !changed {
        return Ok(batch);
    }

    RecordBatch::try_new(schema, columns).map_err(|e| Error::UnexpectedError {
        message: format!("Failed to rebuild RecordBatch after resolving blob descriptors: {e}"),
        source: Some(Box::new(e)),
    })
}

#[allow(clippy::too_many_arguments)]
fn open_source_stream(
    split: &DataSplit,
    source: &FieldSource,
    row_ranges: Option<Vec<RowRange>>,
    file_io: FileIO,
    schema_manager: SchemaManager,
    table_schema_id: i64,
    table_fields: Vec<DataField>,
    blob_as_descriptor: bool,
    anchor_deletion_vector: Option<&DeletionVectorContext>,
) -> crate::Result<ArrowRecordBatchStream> {
    let file_reader = DataFileReader::new(
        file_io,
        schema_manager,
        table_schema_id,
        table_fields,
        source.read_fields().to_vec(),
        Vec::new(),
    )
    .with_blob_as_descriptor(blob_as_descriptor);

    match source {
        FieldSource::DataFile {
            file, data_fields, ..
        } => {
            let deletion_vector = shifted_deletion_vector_for_file(file, anchor_deletion_vector)?;
            file_reader.read_single_file_stream(
                split,
                file.as_ref().clone(),
                data_fields.clone(),
                deletion_vector,
                row_ranges,
            )
        }
        FieldSource::BlobBunch {
            bunch, data_fields, ..
        } => read_bunch_files_stream(
            file_reader,
            split,
            bunch.files.clone(),
            data_fields.clone(),
            row_ranges,
            anchor_deletion_vector.cloned(),
        ),
        FieldSource::VectorBunch {
            bunch, data_fields, ..
        } => read_bunch_files_stream(
            file_reader,
            split,
            bunch.files.clone(),
            data_fields.clone(),
            row_ranges,
            anchor_deletion_vector.cloned(),
        ),
    }
}

fn read_bunch_files_stream(
    file_reader: DataFileReader,
    split: &DataSplit,
    files: Vec<DataFileMeta>,
    data_fields: Option<Vec<DataField>>,
    row_ranges: Option<Vec<RowRange>>,
    anchor_deletion_vector: Option<DeletionVectorContext>,
) -> crate::Result<ArrowRecordBatchStream> {
    let split = split.clone();
    Ok(try_stream! {
        for file in files {
            let deletion_vector =
                shifted_deletion_vector_for_file(&file, anchor_deletion_vector.as_ref())?;
            let mut stream = file_reader.read_single_file_stream(
                &split,
                file,
                data_fields.clone(),
                deletion_vector,
                row_ranges.clone(),
            )?;
            while let Some(batch) = stream.next().await {
                yield batch?;
            }
        }
    }
    .boxed())
}

#[derive(Debug, Clone)]
struct DeletionVectorContext {
    first_row_id: i64,
    deletion_vector: Arc<DeletionVector>,
}

async fn read_file_deletion_vector(
    file_io: &FileIO,
    split: &DataSplit,
    file: &DataFileMeta,
) -> crate::Result<Option<Arc<DeletionVector>>> {
    let Some(deletion_file) = split.deletion_file_for_data_file(file) else {
        return Ok(None);
    };
    Ok(Some(Arc::new(
        DeletionVectorFactory::read(file_io, deletion_file).await?,
    )))
}

async fn read_anchor_deletion_vector(
    file_io: &FileIO,
    split: &DataSplit,
    files: &[DataFileMeta],
) -> crate::Result<Option<DeletionVectorContext>> {
    let anchor = crate::table::source::data_evolution_anchor_file(files)?;
    let Some(deletion_file) = split.deletion_file_for_data_file(anchor) else {
        return Ok(None);
    };
    let first_row_id = anchor.first_row_id.ok_or_else(|| Error::DataInvalid {
        message: format!(
            "Data-evolution anchor file '{}' is missing first_row_id",
            anchor.file_name
        ),
        source: None,
    })?;
    Ok(Some(DeletionVectorContext {
        first_row_id,
        deletion_vector: Arc::new(DeletionVectorFactory::read(file_io, deletion_file).await?),
    }))
}

fn shifted_deletion_vector_for_file(
    file: &DataFileMeta,
    context: Option<&DeletionVectorContext>,
) -> crate::Result<Option<Arc<DeletionVector>>> {
    let Some(context) = context else {
        return Ok(None);
    };
    let Some(file_first_row_id) = file.first_row_id else {
        return Ok(None);
    };

    if file_first_row_id == context.first_row_id {
        return Ok(Some(context.deletion_vector.clone()));
    }

    let file_end = file_first_row_id + file.row_count - 1;
    let mut bitmap = RoaringBitmap::new();
    for deleted in context.deletion_vector.iter() {
        let row_id = context.first_row_id + deleted as i64;
        if row_id < file_first_row_id || row_id > file_end {
            continue;
        }
        let local = u32::try_from(row_id - file_first_row_id).map_err(|_| Error::DataInvalid {
            message: format!(
                "Deleted row id {row_id} cannot be represented as a local deletion-vector position for file '{}'",
                file.file_name
            ),
            source: None,
        })?;
        bitmap.insert(local);
    }

    if bitmap.is_empty() {
        Ok(None)
    } else {
        Ok(Some(Arc::new(DeletionVector::from_bitmap(bitmap))))
    }
}

fn selected_absolute_row_ranges_for_file(
    first_row_id: i64,
    row_count: i64,
    row_ranges: Option<&[RowRange]>,
    deletion_vector: Option<&DeletionVector>,
) -> crate::Result<Option<Vec<RowRange>>> {
    let has_ranges = row_ranges.is_some();
    let has_deletion_vector = deletion_vector.is_some_and(|dv| !dv.is_empty());
    if !has_ranges && !has_deletion_vector {
        return Ok(None);
    }
    if row_count == 0 {
        return Ok(Some(Vec::new()));
    }

    let mut local_ranges = if let Some(dv) = deletion_vector {
        non_deleted_local_ranges(row_count, dv)
    } else {
        vec![RowRange::new(0, row_count - 1)]
    };

    if let Some(ranges) = row_ranges {
        let selected = ranges
            .iter()
            .filter_map(|range| {
                range
                    .intersect_inclusive(first_row_id, first_row_id + row_count - 1)
                    .map(|range| {
                        RowRange::new(range.from() - first_row_id, range.to() - first_row_id)
                    })
            })
            .collect::<Vec<_>>();
        local_ranges = intersect_local_ranges(&local_ranges, &selected);
    }

    let absolute = local_ranges
        .into_iter()
        .map(|range| RowRange::new(first_row_id + range.from(), first_row_id + range.to()))
        .collect::<Vec<_>>();
    Ok(Some(absolute))
}

fn non_deleted_local_ranges(row_count: i64, deletion_vector: &DeletionVector) -> Vec<RowRange> {
    let mut ranges = Vec::new();
    let mut cursor = 0i64;
    for deleted in deletion_vector.iter() {
        let deleted = deleted as i64;
        if deleted >= row_count {
            break;
        }
        if deleted > cursor {
            ranges.push(RowRange::new(cursor, deleted - 1));
        }
        cursor = deleted + 1;
    }
    if cursor < row_count {
        ranges.push(RowRange::new(cursor, row_count - 1));
    }
    ranges
}

fn intersect_local_ranges(left: &[RowRange], right: &[RowRange]) -> Vec<RowRange> {
    let mut result = Vec::new();
    let (mut i, mut j) = (0usize, 0usize);
    while i < left.len() && j < right.len() {
        let from = left[i].from().max(right[j].from());
        let to = left[i].to().min(right[j].to());
        if from <= to {
            result.push(RowRange::new(from, to));
        }
        if left[i].to() < right[j].to() {
            i += 1;
        } else {
            j += 1;
        }
    }
    result
}

#[derive(Debug, Clone)]
struct PreparedMergeGroup {
    files: Vec<DataFileMeta>,
    logical_row_count: i64,
    first_row_id: i64,
}

impl PreparedMergeGroup {
    fn new(files: &[DataFileMeta]) -> crate::Result<Self> {
        let files = normalize_merge_group(files.to_vec())?;
        if files.is_empty() {
            return Ok(Self {
                files,
                logical_row_count: 0,
                first_row_id: 0,
            });
        }

        let data_files: Vec<&DataFileMeta> = files
            .iter()
            .filter(|file| {
                !is_blob_file_name(&file.file_name) && !is_vector_store_file_name(&file.file_name)
            })
            .collect();
        if data_files.is_empty() {
            return Err(Error::DataInvalid {
                message: "Field merge split with .blob/.vector. files requires at least one normal data file".to_string(),
                source: None,
            });
        }

        let first_data_file = data_files[0];
        let first_row_id = first_data_file
            .first_row_id
            .ok_or_else(|| Error::DataInvalid {
                message: "All files in a field merge split should have first_row_id".to_string(),
                source: None,
            })?;
        let logical_row_count = first_data_file.row_count;

        for file in data_files.iter().skip(1) {
            if file.first_row_id != Some(first_row_id) || file.row_count != logical_row_count {
                return Err(Error::DataInvalid {
                    message: "All non-blob files in a field merge split should have the same row id range".to_string(),
                    source: None,
                });
            }
        }

        Ok(Self {
            files,
            logical_row_count,
            first_row_id,
        })
    }
}

#[derive(Debug, Clone)]
struct ResolvedFileInfo {
    field_ids: Vec<i32>,
    data_fields: Option<Vec<DataField>>,
    normalized_write_cols: Option<Vec<String>>,
}

async fn load_file_infos(
    schema_manager: &SchemaManager,
    table_schema_id: i64,
    table_fields: &[DataField],
    files: &[DataFileMeta],
) -> crate::Result<Vec<ResolvedFileInfo>> {
    let mut infos = Vec::with_capacity(files.len());

    for file in files {
        let (field_ids, data_fields, effective_fields_owned);
        if file.schema_id == table_schema_id {
            field_ids = resolve_field_ids(file, table_fields)?;
            data_fields = None;
            effective_fields_owned = None;
        } else {
            let data_schema = schema_manager.schema(file.schema_id).await?;
            let fields = data_schema.fields().to_vec();
            field_ids = resolve_field_ids(file, &fields)?;
            data_fields = Some(fields.clone());
            effective_fields_owned = Some(fields);
        }

        let normalized_write_cols = if is_vector_store_file_name(&file.file_name) {
            let effective_fields: &[DataField] = match effective_fields_owned.as_deref() {
                Some(fields) => fields,
                None => table_fields,
            };
            Some(normalize_vector_write_cols(file, effective_fields)?)
        } else {
            None
        };

        infos.push(ResolvedFileInfo {
            field_ids,
            data_fields,
            normalized_write_cols,
        });
    }

    Ok(infos)
}

fn resolve_field_ids(file: &DataFileMeta, fields: &[DataField]) -> crate::Result<Vec<i32>> {
    match &file.write_cols {
        Some(write_cols) => write_cols
            .iter()
            .map(|name| {
                fields
                    .iter()
                    .find(|field| field.name() == name)
                    .map(|field| field.id())
                    .ok_or_else(|| Error::DataInvalid {
                        message: format!(
                            "Failed to resolve write column '{}' in file '{}'",
                            name, file.file_name
                        ),
                        source: None,
                    })
            })
            .collect(),
        None => Ok(fields.iter().map(|field| field.id()).collect()),
    }
}

/// Lowercased final filename extension, used as a vector bunch's format identifier.
/// `"data.vector.parquet" -> "parquet"`, `"emb-1.vector.vortex" -> "vortex"`.
fn vector_format_suffix(file_name: &str) -> String {
    file_name
        .rsplit('.')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase()
}

/// Normalize a vector file's write columns into a stable key component: the write
/// column names sorted by their field position in the file's effective row type.
/// A `.vector.` file with no `write_cols` is ambiguous and rejected. An unknown
/// column name is rejected. Raw orderings that differ but normalize equal compare equal.
fn normalize_vector_write_cols(
    file: &DataFileMeta,
    fields: &[DataField],
) -> crate::Result<Vec<String>> {
    let write_cols = file.write_cols.as_ref().ok_or_else(|| Error::DataInvalid {
        message: format!("Vector file '{}' must declare write_cols", file.file_name),
        source: None,
    })?;

    let mut indexed: Vec<(usize, String)> = write_cols
        .iter()
        .map(|name| {
            fields
                .iter()
                .position(|field| field.name() == name)
                .map(|pos| (pos, name.clone()))
                .ok_or_else(|| Error::DataInvalid {
                    message: format!(
                        "Failed to resolve vector write column '{}' in file '{}'",
                        name, file.file_name
                    ),
                    source: None,
                })
        })
        .collect::<crate::Result<_>>()?;

    indexed.sort_by_key(|(pos, _)| *pos);
    Ok(indexed.into_iter().map(|(_, name)| name).collect())
}

#[derive(Debug, Clone)]
struct SourcePlan {
    sources: Vec<FieldSource>,
    column_plan: Vec<Option<(usize, usize)>>,
}

fn build_source_plan(
    prepared_group: &PreparedMergeGroup,
    file_infos: &[ResolvedFileInfo],
    read_type: &[DataField],
    blob_descriptor_fields: &HashSet<String>,
) -> crate::Result<SourcePlan> {
    let mut sources = Vec::new();
    let mut normal_providers: HashMap<i32, usize> = HashMap::new(); // field_id -> source_idx
    let mut vector_field_providers: HashMap<i32, usize> = HashMap::new(); // field_id -> source_idx
    let mut vector_bunch_indices: HashMap<(i64, String, Vec<String>), usize> = HashMap::new();
    let mut blob_source_indices: HashMap<i32, usize> = HashMap::new();
    let mut expected_blob_row_count: Option<i64> = None;

    for (file_idx, file) in prepared_group.files.iter().enumerate() {
        let info = &file_infos[file_idx];
        if is_blob_file_name(&file.file_name) {
            let field_id = resolve_blob_field_id(file, info)?;
            let expected_row_count = expected_blob_row_count.ok_or_else(|| Error::DataInvalid {
                message: format!(
                    "Blob file '{}' must be ordered after a non-blob data file",
                    file.file_name
                ),
                source: None,
            })?;

            let source_idx = if let Some(&existing_idx) = blob_source_indices.get(&field_id) {
                existing_idx
            } else {
                let source_idx = sources.len();
                sources.push(FieldSource::BlobBunch {
                    bunch: BlobBunch::new(expected_row_count),
                    data_fields: info.data_fields.clone(),
                    read_fields: Vec::new(),
                });
                blob_source_indices.insert(field_id, source_idx);
                source_idx
            };

            sources[source_idx]
                .blob_bunch_mut()
                .unwrap()
                .add(file.clone())?;
        } else if is_vector_store_file_name(&file.file_name) {
            // A vector file is a column provider only; unlike a normal data file it does
            // NOT update `expected_blob_row_count` (it must not anchor a following blob's
            // row count). Segments sharing the same (schema_id, format, normalized
            // write cols) key aggregate into one bunch.
            let normalized =
                info.normalized_write_cols
                    .clone()
                    .ok_or_else(|| Error::DataInvalid {
                        message: format!(
                            "Vector file '{}' is missing normalized write columns",
                            file.file_name
                        ),
                        source: None,
                    })?;
            let format_suffix = vector_format_suffix(&file.file_name);
            let key = (file.schema_id, format_suffix.clone(), normalized.clone());

            let source_idx = if let Some(&existing_idx) = vector_bunch_indices.get(&key) {
                existing_idx
            } else {
                let source_idx = sources.len();
                sources.push(FieldSource::VectorBunch {
                    bunch: VectorBunch::new(
                        prepared_group.logical_row_count,
                        file.schema_id,
                        format_suffix,
                        normalized.clone(),
                    ),
                    data_fields: info.data_fields.clone(),
                    read_fields: Vec::new(),
                });
                vector_bunch_indices.insert(key, source_idx);
                source_idx
            };

            sources[source_idx]
                .vector_bunch_mut()
                .unwrap()
                .add(file.clone(), &normalized)?;

            for &field_id in &info.field_ids {
                match vector_field_providers.get(&field_id) {
                    // Same bunch aggregating another segment: fine.
                    Some(&existing_idx) if existing_idx == source_idx => {}
                    // Different bunch key advertising the same field id: ambiguous.
                    Some(_) => {
                        return Err(Error::DataInvalid {
                            message: format!(
                                "Vector field id {field_id} is provided by more than one vector bunch"
                            ),
                            source: None,
                        });
                    }
                    None => {
                        vector_field_providers.insert(field_id, source_idx);
                    }
                }
            }
        } else {
            expected_blob_row_count = Some(file.row_count);
            let source_idx = sources.len();
            sources.push(FieldSource::DataFile {
                file: Box::new(file.clone()),
                data_fields: info.data_fields.clone(),
                read_fields: Vec::new(),
            });
            for &field_id in &info.field_ids {
                // first normal file that carries the id wins (preserve existing semantics)
                normal_providers.entry(field_id).or_insert(source_idx);
            }
        }
    }

    let mut column_plan = Vec::with_capacity(read_type.len());
    for field in read_type {
        let source_idx = if matches!(field.data_type(), DataType::Blob(_))
            && !blob_descriptor_fields.contains(field.name())
        {
            blob_source_indices.get(&field.id()).copied()
        } else if matches!(field.data_type(), DataType::Vector(_)) {
            // Prefer the dedicated .vector. bunch; fall back to a normal data file
            // (PR 2 inline-vector compatibility path).
            vector_field_providers
                .get(&field.id())
                .copied()
                .or_else(|| normal_providers.get(&field.id()).copied())
        } else {
            // Non-vector fields never read from a .vector. file.
            normal_providers.get(&field.id()).copied()
        };

        if let Some(source_idx) = source_idx {
            let field_offset = sources[source_idx].add_read_field(field.clone());
            column_plan.push(Some((source_idx, field_offset)));
        } else {
            column_plan.push(None);
        }
    }

    for source in &sources {
        if let FieldSource::BlobBunch {
            bunch, read_fields, ..
        } = source
        {
            if !read_fields.is_empty() && bunch.row_count() != prepared_group.logical_row_count {
                return Err(Error::DataInvalid {
                    message: format!(
                        "Blob bunch row count {} does not match logical row count {}",
                        bunch.row_count(),
                        prepared_group.logical_row_count
                    ),
                    source: None,
                });
            }
        }
    }

    for source in &sources {
        if let FieldSource::VectorBunch {
            bunch, read_fields, ..
        } = source
        {
            if !read_fields.is_empty() && bunch.row_count() != prepared_group.logical_row_count {
                return Err(Error::DataInvalid {
                    message: format!(
                        "Vector bunch row count {} does not match logical row count {}",
                        bunch.row_count(),
                        prepared_group.logical_row_count
                    ),
                    source: None,
                });
            }
        }
    }

    Ok(SourcePlan {
        sources,
        column_plan,
    })
}

fn resolve_blob_field_id(file: &DataFileMeta, info: &ResolvedFileInfo) -> crate::Result<i32> {
    if info.field_ids.len() != 1 {
        return Err(Error::DataInvalid {
            message: format!(
                "Blob file '{}' should resolve to exactly one write column, got {}",
                file.file_name,
                info.field_ids.len()
            ),
            source: None,
        });
    }

    Ok(info.field_ids[0])
}

#[derive(Debug, Clone)]
enum FieldSource {
    DataFile {
        file: Box<DataFileMeta>,
        data_fields: Option<Vec<DataField>>,
        read_fields: Vec<DataField>,
    },
    VectorBunch {
        bunch: VectorBunch,
        data_fields: Option<Vec<DataField>>,
        read_fields: Vec<DataField>,
    },
    BlobBunch {
        bunch: BlobBunch,
        data_fields: Option<Vec<DataField>>,
        read_fields: Vec<DataField>,
    },
}

impl FieldSource {
    fn read_fields(&self) -> &[DataField] {
        match self {
            FieldSource::DataFile { read_fields, .. }
            | FieldSource::VectorBunch { read_fields, .. }
            | FieldSource::BlobBunch { read_fields, .. } => read_fields,
        }
    }

    fn add_read_field(&mut self, field: DataField) -> usize {
        let read_fields = match self {
            FieldSource::DataFile { read_fields, .. }
            | FieldSource::VectorBunch { read_fields, .. }
            | FieldSource::BlobBunch { read_fields, .. } => read_fields,
        };
        if let Some(offset) = read_fields
            .iter()
            .position(|existing| existing.id() == field.id())
        {
            return offset;
        }

        read_fields.push(field);
        read_fields.len() - 1
    }

    fn blob_bunch_mut(&mut self) -> Option<&mut BlobBunch> {
        match self {
            FieldSource::BlobBunch { bunch, .. } => Some(bunch),
            FieldSource::DataFile { .. } | FieldSource::VectorBunch { .. } => None,
        }
    }

    fn vector_bunch_mut(&mut self) -> Option<&mut VectorBunch> {
        match self {
            FieldSource::VectorBunch { bunch, .. } => Some(bunch),
            FieldSource::DataFile { .. } | FieldSource::BlobBunch { .. } => None,
        }
    }
}

#[derive(Debug, Clone)]
struct BlobBunch {
    files: Vec<DataFileMeta>,
    expected_row_count: i64,
    latest_first_row_id: i64,
    expected_next_first_row_id: i64,
    latest_max_sequence_number: i64,
    row_count: i64,
}

impl BlobBunch {
    fn new(expected_row_count: i64) -> Self {
        Self {
            files: Vec::new(),
            expected_row_count,
            latest_first_row_id: -1,
            expected_next_first_row_id: -1,
            latest_max_sequence_number: -1,
            row_count: 0,
        }
    }

    fn add(&mut self, file: DataFileMeta) -> crate::Result<()> {
        if !is_blob_file_name(&file.file_name) {
            return Err(Error::DataInvalid {
                message: "Only blob file can be added to a blob bunch.".to_string(),
                source: None,
            });
        }

        let first_row_id = file.first_row_id.ok_or_else(|| Error::DataInvalid {
            message: format!("Blob file '{}' is missing first_row_id", file.file_name),
            source: None,
        })?;

        if first_row_id == self.latest_first_row_id {
            if file.max_sequence_number >= self.latest_max_sequence_number {
                return Err(Error::DataInvalid {
                    message:
                        "Blob file with same first row id should have decreasing sequence number."
                            .to_string(),
                    source: None,
                });
            }
            return Ok(());
        }

        if !self.files.is_empty() {
            if first_row_id < self.expected_next_first_row_id {
                if file.max_sequence_number >= self.latest_max_sequence_number {
                    return Err(Error::DataInvalid {
                        message:
                            "Blob file with overlapping row id should have decreasing sequence number."
                                .to_string(),
                        source: None,
                    });
                }
                return Ok(());
            } else if first_row_id > self.expected_next_first_row_id {
                return Err(Error::DataInvalid {
                    message: format!(
                        "Blob file first row id should be continuous, expect {} but got {}",
                        self.expected_next_first_row_id, first_row_id
                    ),
                    source: None,
                });
            }

            if !self.files.is_empty() {
                let first_file = &self.files[0];
                if file.schema_id != first_file.schema_id {
                    return Err(Error::DataInvalid {
                        message: "All files in a blob bunch should have the same schema id."
                            .to_string(),
                        source: None,
                    });
                }
                if file.write_cols != first_file.write_cols {
                    return Err(Error::DataInvalid {
                        message: "All files in a blob bunch should have the same write columns."
                            .to_string(),
                        source: None,
                    });
                }
            }
        }

        self.row_count += file.row_count;
        if self.row_count > self.expected_row_count {
            return Err(Error::DataInvalid {
                message: format!(
                    "Blob files row count {} exceed the expected {}",
                    self.row_count, self.expected_row_count
                ),
                source: None,
            });
        }
        self.latest_max_sequence_number = file.max_sequence_number;
        self.latest_first_row_id = first_row_id;
        self.expected_next_first_row_id = first_row_id + file.row_count;
        self.files.push(file);
        Ok(())
    }

    fn row_count(&self) -> i64 {
        self.row_count
    }
}

/// Aggregates rolled `.vector.<format>` segments belonging to one logical vector
/// source, mirroring upstream `VectorFileBunch` non-pushdown semantics. Unlike
/// `BlobBunch`, the expected row count is taken directly from the prepared group's
/// logical row count (vectors sit before blobs and never anchor a blob's row count).
///
/// `normalize_merge_group` is responsible for ordering segments; `add` assumes sorted
/// input and enforces continuity/dedup.
#[derive(Debug, Clone)]
struct VectorBunch {
    files: Vec<DataFileMeta>,
    schema_id: i64,
    format_suffix: String,
    normalized_write_cols: Vec<String>,
    expected_row_count: i64,
    latest_first_row_id: i64,
    expected_next_first_row_id: i64,
    latest_max_sequence_number: i64,
    row_count: i64,
}

impl VectorBunch {
    fn new(
        expected_row_count: i64,
        schema_id: i64,
        format_suffix: String,
        normalized_write_cols: Vec<String>,
    ) -> Self {
        Self {
            files: Vec::new(),
            schema_id,
            format_suffix,
            normalized_write_cols,
            expected_row_count,
            latest_first_row_id: -1,
            expected_next_first_row_id: -1,
            latest_max_sequence_number: -1,
            row_count: 0,
        }
    }

    fn add(&mut self, file: DataFileMeta, normalized_write_cols: &[String]) -> crate::Result<()> {
        if !is_vector_store_file_name(&file.file_name) {
            return Err(Error::DataInvalid {
                message: "Only vector file can be added to a vector bunch.".to_string(),
                source: None,
            });
        }

        let first_row_id = file.first_row_id.ok_or_else(|| Error::DataInvalid {
            message: format!("Vector file '{}' is missing first_row_id", file.file_name),
            source: None,
        })?;

        if first_row_id == self.latest_first_row_id {
            if file.max_sequence_number >= self.latest_max_sequence_number {
                return Err(Error::DataInvalid {
                    message:
                        "Vector file with same first row id should have decreasing sequence number."
                            .to_string(),
                    source: None,
                });
            }
            return Ok(());
        }

        if !self.files.is_empty() {
            if first_row_id < self.expected_next_first_row_id {
                if file.max_sequence_number >= self.latest_max_sequence_number {
                    return Err(Error::DataInvalid {
                        message:
                            "Vector file with overlapping row id should have decreasing sequence number."
                                .to_string(),
                        source: None,
                    });
                }
                return Ok(());
            } else if first_row_id > self.expected_next_first_row_id {
                return Err(Error::DataInvalid {
                    message: format!(
                        "Vector file first row id should be continuous, expect {} but got {}",
                        self.expected_next_first_row_id, first_row_id
                    ),
                    source: None,
                });
            }
        }

        // Defensive key-identity check against the bunch's key (not raw write_cols).
        if file.schema_id != self.schema_id {
            return Err(Error::DataInvalid {
                message: "All files in a vector bunch should have the same schema id.".to_string(),
                source: None,
            });
        }
        if vector_format_suffix(&file.file_name) != self.format_suffix {
            return Err(Error::DataInvalid {
                message: "All files in a vector bunch should have the same format.".to_string(),
                source: None,
            });
        }
        if normalized_write_cols != self.normalized_write_cols.as_slice() {
            return Err(Error::DataInvalid {
                message:
                    "All files in a vector bunch should have the same normalized write columns."
                        .to_string(),
                source: None,
            });
        }

        self.row_count += file.row_count;
        if self.row_count > self.expected_row_count {
            return Err(Error::DataInvalid {
                message: format!(
                    "Vector files row count {} exceed the expected {}",
                    self.row_count, self.expected_row_count
                ),
                source: None,
            });
        }
        self.latest_max_sequence_number = file.max_sequence_number;
        self.latest_first_row_id = first_row_id;
        self.expected_next_first_row_id = first_row_id + file.row_count;
        self.files.push(file);
        Ok(())
    }

    fn row_count(&self) -> i64 {
        self.row_count
    }
}

fn normalize_merge_group(files: Vec<DataFileMeta>) -> crate::Result<Vec<DataFileMeta>> {
    let mut normal_files = Vec::new();
    let mut vector_files = Vec::new();
    let mut blob_files = Vec::new();

    for file in files {
        if is_blob_file_name(&file.file_name) {
            blob_files.push(file);
        } else if is_vector_store_file_name(&file.file_name) {
            vector_files.push(file);
        } else {
            normal_files.push(file);
        }
    }

    normal_files.sort_by_key(|f| std::cmp::Reverse(f.max_sequence_number));

    // Vector files: sort by first_row_id asc, then max_sequence_number desc (like blobs).
    // They are NOT validated against the normal-file row range — rolled segments are
    // slices with their own ranges. They DO require first_row_id.
    if vector_files.iter().any(|file| file.first_row_id.is_none()) {
        return Err(Error::DataInvalid {
            message: "All vector files in a field merge split should have first_row_id".to_string(),
            source: None,
        });
    }
    vector_files.sort_by(|left, right| {
        let l = left.first_row_id.unwrap_or(i64::MIN);
        let r = right.first_row_id.unwrap_or(i64::MIN);
        l.cmp(&r)
            .then_with(|| right.max_sequence_number.cmp(&left.max_sequence_number))
    });

    // Normal files share the anchor's row range. Validate normal files ONLY (vectors removed).
    let mut range_ref: Option<(i64, i64)> = None;
    for file in normal_files.iter() {
        let first_row_id = file.first_row_id.ok_or_else(|| Error::DataInvalid {
            message: "All data files in a field merge split should have first_row_id".to_string(),
            source: None,
        })?;
        match range_ref {
            None => range_ref = Some((first_row_id, file.row_count)),
            Some((ref_first, ref_count)) => {
                if first_row_id != ref_first || file.row_count != ref_count {
                    return Err(Error::DataInvalid {
                        message: "All data files in a field merge split should have the same row id range.".to_string(),
                        source: None,
                    });
                }
            }
        }
    }

    blob_files.sort_by(|left, right| {
        let l = left.first_row_id.unwrap_or(i64::MIN);
        let r = right.first_row_id.unwrap_or(i64::MIN);
        l.cmp(&r)
            .then_with(|| right.max_sequence_number.cmp(&left.max_sequence_number))
    });
    if blob_files.iter().any(|file| file.first_row_id.is_none()) {
        return Err(Error::DataInvalid {
            message: "All blob files in a field merge split should have first_row_id".to_string(),
            source: None,
        });
    }

    let mut out = normal_files;
    out.extend(vector_files);
    out.extend(blob_files);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Identifier;
    use crate::io::FileIOBuilder;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{BinaryRow, BlobType, FloatType, IntType, Schema, TableSchema, VectorType};
    use crate::table::{DataSplitBuilder, Table, TableRead};
    use arrow_array::{
        Array, BinaryArray, FixedSizeListArray, Float32Array, Int32Array, RecordBatch,
    };
    use futures::TryStreamExt;
    use std::fs;
    use std::path::{Path, PathBuf};
    use tempfile::tempdir;

    mod blob_test_utils {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../blob_test_utils.rs"
        ));
    }

    #[allow(dead_code)]
    mod test_utils {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/../test_utils.rs"));
    }

    use blob_test_utils::write_blob_file;
    use test_utils::{local_file_path, write_int_parquet_file};

    #[test]
    fn test_build_source_plan_aggregates_same_key_vector_segments() {
        // Two contiguous vector segments, same key -> ONE VectorBunch, files in sorted order.
        let files = vec![
            data_file("d1.parquet", 0, 20, 1, Some(vec!["id"])),
            data_file("v1.vector.parquet", 0, 10, 1, Some(vec!["emb"])),
            data_file("v2.vector.parquet", 10, 10, 1, Some(vec!["emb"])),
        ];
        let prepared_group = PreparedMergeGroup {
            files: files.clone(),
            logical_row_count: 20,
            first_row_id: 0,
        };
        let file_infos = vec![
            ResolvedFileInfo {
                field_ids: vec![1],
                data_fields: None,
                normalized_write_cols: None,
            },
            ResolvedFileInfo {
                field_ids: vec![2],
                data_fields: None,
                normalized_write_cols: Some(vec!["emb".to_string()]),
            },
            ResolvedFileInfo {
                field_ids: vec![2],
                data_fields: None,
                normalized_write_cols: Some(vec!["emb".to_string()]),
            },
        ];
        let read_type = vec![
            DataField::new(1, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(2, "emb".to_string(), vector_float_type(2)),
        ];
        let plan =
            build_source_plan(&prepared_group, &file_infos, &read_type, &HashSet::new()).unwrap();

        // sources: [DataFile(d1), VectorBunch(v1,v2)]
        assert_eq!(plan.sources.len(), 2);
        assert_eq!(plan.column_plan, vec![Some((0, 0)), Some((1, 0))]);
        match &plan.sources[1] {
            FieldSource::VectorBunch { bunch, .. } => {
                let names: Vec<&str> = bunch.files.iter().map(|f| f.file_name.as_str()).collect();
                assert_eq!(names, vec!["v1.vector.parquet", "v2.vector.parquet"]);
            }
            _ => panic!("expected vector bunch source"),
        }
    }

    #[test]
    fn test_build_source_plan_aggregates_differently_ordered_write_cols() {
        // Two segments with multiple vector cols whose RAW write_cols differ in order but
        // normalize to the same key -> one bunch (#5b). field 2 = "a", field 3 = "b".
        let files = vec![
            data_file("d1.parquet", 0, 20, 1, Some(vec!["id"])),
            data_file("v1.vector.parquet", 0, 10, 1, Some(vec!["a", "b"])),
            data_file("v2.vector.parquet", 10, 10, 1, Some(vec!["b", "a"])),
        ];
        let prepared_group = PreparedMergeGroup {
            files: files.clone(),
            logical_row_count: 20,
            first_row_id: 0,
        };
        // Both segments normalize to ["a","b"] (field-position order).
        let file_infos = vec![
            ResolvedFileInfo {
                field_ids: vec![1],
                data_fields: None,
                normalized_write_cols: None,
            },
            ResolvedFileInfo {
                field_ids: vec![2, 3],
                data_fields: None,
                normalized_write_cols: Some(vec!["a".to_string(), "b".to_string()]),
            },
            ResolvedFileInfo {
                field_ids: vec![2, 3],
                data_fields: None,
                normalized_write_cols: Some(vec!["a".to_string(), "b".to_string()]),
            },
        ];
        let read_type = vec![
            DataField::new(1, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(2, "a".to_string(), vector_float_type(2)),
            DataField::new(3, "b".to_string(), vector_float_type(2)),
        ];
        let plan =
            build_source_plan(&prepared_group, &file_infos, &read_type, &HashSet::new()).unwrap();
        // One vector bunch holding both segments; both vector columns map to it.
        assert_eq!(plan.sources.len(), 2);
        match &plan.sources[1] {
            FieldSource::VectorBunch { bunch, .. } => assert_eq!(bunch.files.len(), 2),
            _ => panic!("expected vector bunch source"),
        }
        assert_eq!(plan.column_plan[1].map(|(s, _)| s), Some(1));
        assert_eq!(plan.column_plan[2].map(|(s, _)| s), Some(1));
    }

    #[test]
    fn test_build_source_plan_rejects_field_id_across_two_bunch_keys() {
        // Same field id 2 advertised by two DIFFERENT bunch keys (different write col sets) -> error (#6).
        let files = vec![
            data_file("d1.parquet", 0, 10, 1, Some(vec!["id"])),
            data_file("v1.vector.parquet", 0, 10, 1, Some(vec!["emb"])),
            data_file("v2.vector.parquet", 0, 10, 2, Some(vec!["emb", "other"])),
        ];
        let prepared_group = PreparedMergeGroup {
            files: files.clone(),
            logical_row_count: 10,
            first_row_id: 0,
        };
        let file_infos = vec![
            ResolvedFileInfo {
                field_ids: vec![1],
                data_fields: None,
                normalized_write_cols: None,
            },
            ResolvedFileInfo {
                field_ids: vec![2],
                data_fields: None,
                normalized_write_cols: Some(vec!["emb".to_string()]),
            },
            ResolvedFileInfo {
                field_ids: vec![2, 3],
                data_fields: None,
                normalized_write_cols: Some(vec!["emb".to_string(), "other".to_string()]),
            },
        ];
        let read_type = vec![
            DataField::new(1, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(2, "emb".to_string(), vector_float_type(2)),
            DataField::new(3, "other".to_string(), vector_float_type(2)),
        ];
        let err = build_source_plan(&prepared_group, &file_infos, &read_type, &HashSet::new());
        assert!(matches!(err, Err(Error::DataInvalid { .. })));
    }

    #[test]
    fn test_normalize_merge_group_orders_blob_files_after_data_files() {
        let files = vec![
            data_file("file1.parquet", 1, 10, 1, None),
            data_file("file2.blob", 1, 1, 1, Some(vec!["payload"])),
            data_file("file3.blob", 1, 1, 3, Some(vec!["payload"])),
            data_file("file4.blob", 2, 9, 1, Some(vec!["payload"])),
            data_file("file7.parquet", 1, 10, 3, None),
        ];

        let normalized = normalize_merge_group(files).unwrap();
        let file_names: Vec<&str> = normalized
            .iter()
            .map(|file| file.file_name.as_str())
            .collect();
        assert_eq!(
            file_names,
            vec![
                "file7.parquet",
                "file1.parquet",
                "file3.blob",
                "file2.blob",
                "file4.blob",
            ]
        );
    }

    #[test]
    fn test_normalize_merge_group_orders_vector_files_between_data_and_blob() {
        // Discriminating fixture: the vector file has a HIGHER max_sequence_number than
        // the normal file and is listed first. Old two-group code sorted it among the
        // "data files" by Reverse(seq), yielding [v1, d1, ...]; the three-way split must
        // force normal -> vector -> blob regardless of sequence, yielding [d1, v1, b1].
        let files = vec![
            data_file("v1.vector.parquet", 0, 10, 5, Some(vec!["emb"])),
            data_file("b1.blob", 0, 1, 1, Some(vec!["payload"])),
            data_file("d1.parquet", 0, 10, 1, Some(vec!["id"])),
        ];
        let normalized = normalize_merge_group(files).unwrap();
        let names: Vec<&str> = normalized.iter().map(|f| f.file_name.as_str()).collect();
        // normal first, then vector, then blob
        assert_eq!(names, vec!["d1.parquet", "v1.vector.parquet", "b1.blob"]);
    }

    #[test]
    fn test_normalize_merge_group_accepts_rolled_vectors_with_differing_ranges() {
        // Rolled vector segments are slices with differing row ranges; they must NOT be
        // rejected against the normal anchor's full range (inverts the old reject test).
        let files = vec![
            data_file("d1.parquet", 0, 20, 1, Some(vec!["id"])),
            data_file("v1.vector.parquet", 0, 10, 1, Some(vec!["emb"])),
            data_file("v2.vector.parquet", 10, 10, 1, Some(vec!["emb"])),
        ];
        let normalized = normalize_merge_group(files).unwrap();
        let names: Vec<&str> = normalized.iter().map(|f| f.file_name.as_str()).collect();
        assert_eq!(
            names,
            vec!["d1.parquet", "v1.vector.parquet", "v2.vector.parquet"]
        );
    }

    #[test]
    fn test_normalize_merge_group_sorts_multi_segment_vectors() {
        // Vectors out of order: must sort by first_row_id asc, then max_seq desc,
        // and land after normal, before blob.
        let files = vec![
            data_file("b1.blob", 0, 1, 1, Some(vec!["payload"])),
            data_file("v-mid.vector.parquet", 10, 10, 1, Some(vec!["emb"])),
            data_file("d1.parquet", 0, 30, 1, Some(vec!["id"])),
            data_file("v-late-low.vector.parquet", 20, 10, 1, Some(vec!["emb"])),
            data_file("v-late-high.vector.parquet", 20, 10, 5, Some(vec!["emb"])),
            data_file("v-early.vector.parquet", 0, 10, 1, Some(vec!["emb"])),
        ];
        let normalized = normalize_merge_group(files).unwrap();
        let names: Vec<&str> = normalized.iter().map(|f| f.file_name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "d1.parquet",
                "v-early.vector.parquet",
                "v-mid.vector.parquet",
                "v-late-high.vector.parquet", // same first_row_id 20, higher seq first
                "v-late-low.vector.parquet",
                "b1.blob",
            ]
        );
    }

    #[test]
    fn test_normalize_merge_group_requires_first_row_id_on_vector_files() {
        let mut vector_no_rid = data_file("v1.vector.parquet", 0, 10, 1, Some(vec!["emb"]));
        vector_no_rid.first_row_id = None;
        let files = vec![
            data_file("d1.parquet", 0, 10, 1, Some(vec!["id"])),
            vector_no_rid,
        ];
        let err = normalize_merge_group(files);
        assert!(matches!(err, Err(Error::DataInvalid { .. })));
    }

    #[test]
    fn test_blob_bunch_ignores_same_first_row_id_with_lower_sequence() {
        let mut bunch = BlobBunch::new(1000);
        bunch
            .add(data_file(
                "blob-high.blob",
                0,
                100,
                3,
                Some(vec!["payload"]),
            ))
            .unwrap();
        bunch
            .add(data_file("blob-low.blob", 0, 100, 2, Some(vec!["payload"])))
            .unwrap();

        assert_eq!(bunch.row_count(), 100);
        assert_eq!(bunch.files.len(), 1);
        assert_eq!(bunch.files[0].file_name, "blob-high.blob");
    }

    #[test]
    fn test_is_vector_store_file_name() {
        assert!(is_vector_store_file_name("data-1.vector.parquet"));
        assert!(is_vector_store_file_name("data-1.vector.vortex"));
        assert!(is_vector_store_file_name("PART.VECTOR.PARQUET")); // case-insensitive
        assert!(!is_vector_store_file_name("data-1.parquet"));
        assert!(!is_vector_store_file_name("data-1.blob"));
        assert!(!is_vector_store_file_name("x.vectorstuff")); // not the ".vector." segment
    }

    #[test]
    fn test_is_raw_convertible_false_for_single_vector_file() {
        // A lone vector file must NOT be raw-convertible (would bypass merge routing).
        let files = vec![data_file("v1.vector.parquet", 0, 10, 1, Some(vec!["emb"]))];
        assert!(!is_raw_convertible(&files));
    }

    #[test]
    fn test_prepared_merge_group_rejects_vector_only_split() {
        // No normal anchor file -> DataInvalid.
        let files = vec![data_file("v1.vector.parquet", 0, 10, 1, Some(vec!["emb"]))];
        let err = PreparedMergeGroup::new(&files);
        assert!(matches!(err, Err(Error::DataInvalid { .. })));
    }

    #[test]
    fn test_blob_bunch_rejects_same_first_row_id_with_higher_sequence() {
        let mut bunch = BlobBunch::new(1000);
        bunch
            .add(data_file("blob-low.blob", 0, 100, 2, Some(vec!["payload"])))
            .unwrap();

        let err = bunch
            .add(data_file(
                "blob-high.blob",
                0,
                100,
                3,
                Some(vec!["payload"]),
            ))
            .unwrap_err();

        assert!(
            matches!(err, Error::DataInvalid { message, .. } if message.contains("same first row id"))
        );
    }

    #[test]
    fn test_blob_bunch_rejects_overlapping_higher_sequence_file() {
        let mut bunch = BlobBunch::new(1000);
        bunch
            .add(data_file("blob1.blob", 0, 100, 1, Some(vec!["payload"])))
            .unwrap();

        let err = bunch
            .add(data_file("blob2.blob", 50, 150, 2, Some(vec!["payload"])))
            .unwrap_err();

        assert!(
            matches!(err, Error::DataInvalid { message, .. } if message.contains("overlapping row id"))
        );
    }

    #[test]
    fn test_blob_bunch_rejects_non_continuous_first_row_id() {
        let mut bunch = BlobBunch::new(1000);
        bunch
            .add(data_file("blob1.blob", 0, 100, 3, Some(vec!["payload"])))
            .unwrap();

        let err = bunch
            .add(data_file("blob2.blob", 150, 100, 2, Some(vec!["payload"])))
            .unwrap_err();

        assert!(
            matches!(err, Error::DataInvalid { message, .. } if message.contains("continuous"))
        );
    }

    #[test]
    fn test_blob_bunch_rejects_mixed_write_columns() {
        let mut bunch = BlobBunch::new(200);
        bunch
            .add(data_file("blob1.blob", 0, 100, 3, Some(vec!["payload"])))
            .unwrap();

        let err = bunch
            .add(data_file("blob2.blob", 100, 100, 2, Some(vec!["payload2"])))
            .unwrap_err();

        assert!(
            matches!(err, Error::DataInvalid { message, .. } if message.contains("same write columns"))
        );
    }

    #[test]
    fn test_blob_bunch_rejects_mixed_schema_ids() {
        let mut bunch = BlobBunch::new(200);
        bunch
            .add(data_file("blob1.blob", 0, 100, 3, Some(vec!["payload"])))
            .unwrap();

        let mut mixed_schema = data_file("blob2.blob", 100, 100, 2, Some(vec!["payload"]));
        mixed_schema.schema_id = 1;
        let err = bunch.add(mixed_schema).unwrap_err();

        assert!(
            matches!(err, Error::DataInvalid { message, .. } if message.contains("same schema id"))
        );
    }

    #[test]
    fn test_blob_bunch_rejects_row_count_exceeding_expected() {
        let mut bunch = BlobBunch::new(100);
        bunch
            .add(data_file("blob1.blob", 0, 60, 3, Some(vec!["payload"])))
            .unwrap();

        let err = bunch
            .add(data_file("blob2.blob", 60, 50, 2, Some(vec!["payload"])))
            .unwrap_err();

        assert!(
            matches!(err, Error::DataInvalid { message, .. } if message.contains("exceed the expected"))
        );
    }

    #[test]
    fn test_vector_bunch_aggregates_contiguous_segments() {
        let mut bunch = VectorBunch::new(30, 0, "parquet".to_string(), vec!["emb".to_string()]);
        bunch
            .add(
                data_file("v1.vector.parquet", 0, 10, 1, Some(vec!["emb"])),
                &["emb".to_string()],
            )
            .unwrap();
        bunch
            .add(
                data_file("v2.vector.parquet", 10, 10, 1, Some(vec!["emb"])),
                &["emb".to_string()],
            )
            .unwrap();
        bunch
            .add(
                data_file("v3.vector.parquet", 20, 10, 1, Some(vec!["emb"])),
                &["emb".to_string()],
            )
            .unwrap();
        assert_eq!(bunch.row_count(), 30);
        let names: Vec<&str> = bunch.files.iter().map(|f| f.file_name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "v1.vector.parquet",
                "v2.vector.parquet",
                "v3.vector.parquet"
            ]
        );
    }

    #[test]
    fn test_vector_bunch_rejects_gap() {
        let mut bunch = VectorBunch::new(30, 0, "parquet".to_string(), vec!["emb".to_string()]);
        bunch
            .add(
                data_file("v1.vector.parquet", 0, 10, 1, Some(vec!["emb"])),
                &["emb".to_string()],
            )
            .unwrap();
        // first_row_id 15 > expected_next 10 -> gap
        let err = bunch
            .add(
                data_file("v2.vector.parquet", 15, 10, 1, Some(vec!["emb"])),
                &["emb".to_string()],
            )
            .unwrap_err();
        assert!(
            matches!(err, Error::DataInvalid { message, .. } if message.contains("continuous"))
        );
    }

    #[test]
    fn test_vector_bunch_ignores_same_first_row_id_lower_seq() {
        let mut bunch = VectorBunch::new(30, 0, "parquet".to_string(), vec!["emb".to_string()]);
        bunch
            .add(
                data_file("v-high.vector.parquet", 0, 10, 3, Some(vec!["emb"])),
                &["emb".to_string()],
            )
            .unwrap();
        // same first_row_id, strictly lower seq -> ignored (dedup), no row_count contribution
        bunch
            .add(
                data_file("v-low.vector.parquet", 0, 10, 2, Some(vec!["emb"])),
                &["emb".to_string()],
            )
            .unwrap();
        assert_eq!(bunch.row_count(), 10);
        assert_eq!(bunch.files.len(), 1);
        assert_eq!(bunch.files[0].file_name, "v-high.vector.parquet");
    }

    #[test]
    fn test_vector_bunch_rejects_same_first_row_id_higher_seq() {
        let mut bunch = VectorBunch::new(30, 0, "parquet".to_string(), vec!["emb".to_string()]);
        bunch
            .add(
                data_file("v-low.vector.parquet", 0, 10, 2, Some(vec!["emb"])),
                &["emb".to_string()],
            )
            .unwrap();
        let err = bunch
            .add(
                data_file("v-high.vector.parquet", 0, 10, 3, Some(vec!["emb"])),
                &["emb".to_string()],
            )
            .unwrap_err();
        assert!(matches!(err, Error::DataInvalid { .. }));
    }

    #[test]
    fn test_vector_bunch_ignores_overlapping_lower_seq() {
        let mut bunch = VectorBunch::new(30, 0, "parquet".to_string(), vec!["emb".to_string()]);
        bunch
            .add(
                data_file("v1.vector.parquet", 0, 10, 3, Some(vec!["emb"])),
                &["emb".to_string()],
            )
            .unwrap();
        // first_row_id 5 < expected_next 10 -> overlap; lower seq -> ignored
        bunch
            .add(
                data_file("v2.vector.parquet", 5, 10, 2, Some(vec!["emb"])),
                &["emb".to_string()],
            )
            .unwrap();
        assert_eq!(bunch.row_count(), 10);
        assert_eq!(bunch.files.len(), 1);
    }

    #[test]
    fn test_vector_bunch_rejects_row_count_overflow() {
        let mut bunch = VectorBunch::new(15, 0, "parquet".to_string(), vec!["emb".to_string()]);
        bunch
            .add(
                data_file("v1.vector.parquet", 0, 10, 1, Some(vec!["emb"])),
                &["emb".to_string()],
            )
            .unwrap();
        let err = bunch
            .add(
                data_file("v2.vector.parquet", 10, 10, 1, Some(vec!["emb"])),
                &["emb".to_string()],
            )
            .unwrap_err();
        assert!(matches!(err, Error::DataInvalid { message, .. } if message.contains("exceed")));
    }

    #[test]
    fn test_vector_bunch_rejects_key_identity_mismatch() {
        // schema_id mismatch
        let mut bunch = VectorBunch::new(30, 0, "parquet".to_string(), vec!["emb".to_string()]);
        bunch
            .add(
                data_file("v1.vector.parquet", 0, 10, 1, Some(vec!["emb"])),
                &["emb".to_string()],
            )
            .unwrap();
        let mut wrong_schema = data_file("v2.vector.parquet", 10, 10, 1, Some(vec!["emb"]));
        wrong_schema.schema_id = 99;
        let err = bunch.add(wrong_schema, &["emb".to_string()]).unwrap_err();
        assert!(matches!(err, Error::DataInvalid { .. }));

        // format_suffix mismatch
        let mut bunch2 = VectorBunch::new(30, 0, "parquet".to_string(), vec!["emb".to_string()]);
        bunch2
            .add(
                data_file("v1.vector.parquet", 0, 10, 1, Some(vec!["emb"])),
                &["emb".to_string()],
            )
            .unwrap();
        let err2 = bunch2
            .add(
                data_file("v2.vector.vortex", 10, 10, 1, Some(vec!["emb"])),
                &["emb".to_string()],
            )
            .unwrap_err();
        assert!(matches!(err2, Error::DataInvalid { .. }));

        // normalized_write_cols mismatch
        let mut bunch3 = VectorBunch::new(30, 0, "parquet".to_string(), vec!["emb".to_string()]);
        bunch3
            .add(
                data_file("v1.vector.parquet", 0, 10, 1, Some(vec!["emb"])),
                &["emb".to_string()],
            )
            .unwrap();
        let err3 = bunch3
            .add(
                data_file("v2.vector.parquet", 10, 10, 1, Some(vec!["other"])),
                &["other".to_string()],
            )
            .unwrap_err();
        assert!(matches!(err3, Error::DataInvalid { .. }));
    }

    #[test]
    fn test_vector_bunch_rejects_non_vector_file() {
        let mut bunch = VectorBunch::new(30, 0, "parquet".to_string(), vec!["emb".to_string()]);
        let err = bunch
            .add(
                data_file("v1.parquet", 0, 10, 1, Some(vec!["emb"])),
                &["emb".to_string()],
            )
            .unwrap_err();
        assert!(matches!(err, Error::DataInvalid { .. }));
    }

    #[test]
    fn test_vector_format_suffix() {
        assert_eq!(vector_format_suffix("data.vector.parquet"), "parquet");
        assert_eq!(vector_format_suffix("emb-1.vector.vortex"), "vortex");
        assert_eq!(vector_format_suffix("X.VECTOR.PARQUET"), "parquet");
    }

    #[test]
    fn test_build_source_plan_picks_latest_blob_segments() {
        let files = vec![
            data_file("others.parquet", 0, 1000, 1, None),
            data_file("blob1.blob", 0, 1000, 1, Some(vec!["payload"])),
            data_file("blob2.blob", 0, 500, 2, Some(vec!["payload"])),
            data_file("blob3.blob", 500, 250, 2, Some(vec!["payload"])),
            data_file("blob4.blob", 750, 250, 2, Some(vec!["payload"])),
            data_file("blob5.blob", 0, 100, 3, Some(vec!["payload"])),
            data_file("blob6.blob", 100, 400, 3, Some(vec!["payload"])),
            data_file("blob7.blob", 750, 100, 3, Some(vec!["payload"])),
            data_file("blob8.blob", 850, 150, 3, Some(vec!["payload"])),
            data_file("blob9.blob", 100, 650, 4, Some(vec!["payload"])),
        ];
        let prepared_group = PreparedMergeGroup::new(&files).unwrap();
        let file_infos: Vec<ResolvedFileInfo> = prepared_group
            .files
            .iter()
            .map(|file| {
                if is_blob_file_name(&file.file_name) {
                    resolved_info(vec![2])
                } else {
                    resolved_info(vec![1])
                }
            })
            .collect();

        let read_type = vec![
            DataField::new(1, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(2, "payload".to_string(), DataType::Blob(BlobType::new())),
        ];
        let source_plan =
            build_source_plan(&prepared_group, &file_infos, &read_type, &HashSet::new()).unwrap();

        assert_eq!(source_plan.sources.len(), 2);
        assert_eq!(source_plan.column_plan, vec![Some((0, 0)), Some((1, 0))]);

        match &source_plan.sources[1] {
            FieldSource::BlobBunch { bunch, .. } => {
                let file_names: Vec<&str> = bunch
                    .files
                    .iter()
                    .map(|file| file.file_name.as_str())
                    .collect();
                assert_eq!(
                    file_names,
                    vec!["blob5.blob", "blob9.blob", "blob7.blob", "blob8.blob"]
                );
            }
            FieldSource::DataFile { .. } | FieldSource::VectorBunch { .. } => {
                panic!("expected blob bunch source")
            }
        }
    }

    #[test]
    fn test_build_source_plan_prefers_latest_normal_file_provider() {
        let files = vec![
            data_file("base-v1.parquet", 0, 4, 1, None),
            data_file("base-v2.parquet", 0, 4, 2, None),
            data_file("payload.blob", 0, 4, 2, Some(vec!["payload"])),
        ];
        let prepared_group = PreparedMergeGroup::new(&files).unwrap();
        let file_infos = vec![
            resolved_info(vec![1]),
            resolved_info(vec![1]),
            resolved_info(vec![2]),
        ];
        let read_type = vec![
            DataField::new(1, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(2, "payload".to_string(), DataType::Blob(BlobType::new())),
        ];

        let source_plan =
            build_source_plan(&prepared_group, &file_infos, &read_type, &HashSet::new()).unwrap();

        assert_eq!(source_plan.column_plan, vec![Some((0, 0)), Some((2, 0))]);
    }

    #[test]
    fn test_build_source_plan_groups_multiple_blob_columns() {
        let files = vec![
            data_file("others.parquet", 0, 1000, 1, None),
            data_file("blob5.blob", 0, 100, 3, Some(vec!["payload"])),
            data_file("blob9.blob", 100, 650, 4, Some(vec!["payload"])),
            data_file("blob7.blob", 750, 100, 3, Some(vec!["payload"])),
            data_file("blob8.blob", 850, 150, 3, Some(vec!["payload"])),
            data_file("blob15.blob", 0, 100, 3, Some(vec!["payload2"])),
            data_file("blob19.blob", 100, 650, 4, Some(vec!["payload2"])),
            data_file("blob17.blob", 750, 100, 3, Some(vec!["payload2"])),
            data_file("blob18.blob", 850, 150, 3, Some(vec!["payload2"])),
        ];
        let prepared_group = PreparedMergeGroup::new(&files).unwrap();
        let file_infos: Vec<ResolvedFileInfo> = prepared_group
            .files
            .iter()
            .map(
                |file| match file.write_cols.as_ref().and_then(|cols| cols.first()) {
                    Some(name) if name == "payload" => resolved_info(vec![2]),
                    Some(name) if name == "payload2" => resolved_info(vec![3]),
                    _ => resolved_info(vec![1]),
                },
            )
            .collect();

        let read_type = vec![
            DataField::new(1, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(2, "payload".to_string(), DataType::Blob(BlobType::new())),
            DataField::new(3, "payload2".to_string(), DataType::Blob(BlobType::new())),
        ];
        let source_plan =
            build_source_plan(&prepared_group, &file_infos, &read_type, &HashSet::new()).unwrap();

        assert_eq!(source_plan.sources.len(), 3);
        assert_eq!(
            source_plan.column_plan,
            vec![Some((0, 0)), Some((1, 0)), Some((2, 0))]
        );

        match &source_plan.sources[1] {
            FieldSource::BlobBunch { bunch, .. } => {
                let file_names: Vec<&str> = bunch
                    .files
                    .iter()
                    .map(|file| file.file_name.as_str())
                    .collect();
                assert_eq!(
                    file_names,
                    vec!["blob5.blob", "blob9.blob", "blob7.blob", "blob8.blob"]
                );
            }
            FieldSource::DataFile { .. } | FieldSource::VectorBunch { .. } => {
                panic!("expected blob bunch source")
            }
        }

        match &source_plan.sources[2] {
            FieldSource::BlobBunch { bunch, .. } => {
                let file_names: Vec<&str> = bunch
                    .files
                    .iter()
                    .map(|file| file.file_name.as_str())
                    .collect();
                assert_eq!(
                    file_names,
                    vec!["blob15.blob", "blob19.blob", "blob17.blob", "blob18.blob"]
                );
            }
            FieldSource::DataFile { .. } | FieldSource::VectorBunch { .. } => {
                panic!("expected blob bunch source")
            }
        }
    }

    #[tokio::test]
    async fn test_table_read_merges_parquet_and_java_rolling_blob_files() {
        let tempdir = tempdir().unwrap();
        let table_path = local_file_path(tempdir.path());
        let bucket_dir = tempdir.path().join("bucket-0");
        fs::create_dir_all(&bucket_dir).unwrap();

        let parquet_path = bucket_dir.join("data.parquet");
        write_int_parquet_file(&parquet_path, vec![("id", vec![1, 2, 3, 4])], None);

        let blob_part1_path = bucket_dir.join("blob-part-1.blob");
        let blob_part2_path = bucket_dir.join("blob-part-2.blob");
        copy_blob_fixture("blob-part-1.blob", &blob_part1_path);
        copy_blob_fixture("blob-part-2.blob", &blob_part2_path);

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("payload", DataType::Blob(BlobType::new()))
                .option("data-evolution.enabled", "true")
                .build()
                .unwrap(),
        );
        let table = Table::new(
            file_io,
            Identifier::new("default", "blob_t"),
            table_path,
            table_schema,
            None,
        );

        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_path(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![
                data_file_meta_with_path(
                    "data.parquet",
                    0,
                    4,
                    1,
                    parquet_path.metadata().unwrap().len() as i64,
                    Some(vec!["id"]),
                ),
                data_file_meta_with_path(
                    "blob-part-1.blob",
                    0,
                    2,
                    1,
                    blob_part1_path.metadata().unwrap().len() as i64,
                    Some(vec!["payload"]),
                ),
                data_file_meta_with_path(
                    "blob-part-2.blob",
                    2,
                    2,
                    1,
                    blob_part2_path.metadata().unwrap().len() as i64,
                    Some(vec!["payload"]),
                ),
            ])
            .build()
            .unwrap();

        let read = TableRead::new(&table, table.schema().fields().to_vec(), Vec::new());
        let batches = read
            .to_arrow(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(collect_int_values(&batches, "id"), vec![1, 2, 3, 4]);
        assert_eq!(
            collect_binary_values(&batches, "payload"),
            vec![
                Some(b"hello".to_vec()),
                None,
                Some(b"world".to_vec()),
                Some(Vec::new()),
            ]
        );
    }

    #[tokio::test]
    async fn test_table_read_merges_multiple_blob_columns_with_row_ranges() {
        let tempdir = tempdir().unwrap();
        let table_path = local_file_path(tempdir.path());
        let bucket_dir = tempdir.path().join("bucket-0");
        fs::create_dir_all(&bucket_dir).unwrap();

        let parquet_path = bucket_dir.join("data.parquet");
        write_int_parquet_file(&parquet_path, vec![("id", vec![1, 2, 3, 4])], None);

        let payload_a_1 = bucket_dir.join("payload-a-1.blob");
        let payload_a_2 = bucket_dir.join("payload-a-2.blob");
        let payload_b_1 = bucket_dir.join("payload-b-1.blob");
        let payload_b_2 = bucket_dir.join("payload-b-2.blob");
        write_blob_file(&payload_a_1, &[Some(&b"a1"[..]), Some(&b"a2"[..])]);
        write_blob_file(&payload_a_2, &[Some(&b"a3"[..]), Some(&b"a4"[..])]);
        write_blob_file(&payload_b_1, &[Some(&b"b1"[..]), Some(&b"b2"[..])]);
        write_blob_file(&payload_b_2, &[Some(&b"b3"[..]), Some(&b"b4"[..])]);

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("payload", DataType::Blob(BlobType::new()))
                .column("payload2", DataType::Blob(BlobType::new()))
                .option("data-evolution.enabled", "true")
                .build()
                .unwrap(),
        );
        let table = Table::new(
            file_io,
            Identifier::new("default", "blob_multi_t"),
            table_path,
            table_schema,
            None,
        );

        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_path(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![
                data_file_meta_with_path(
                    "data.parquet",
                    0,
                    4,
                    1,
                    parquet_path.metadata().unwrap().len() as i64,
                    Some(vec!["id"]),
                ),
                data_file_meta_with_path(
                    "payload-a-1.blob",
                    0,
                    2,
                    1,
                    payload_a_1.metadata().unwrap().len() as i64,
                    Some(vec!["payload"]),
                ),
                data_file_meta_with_path(
                    "payload-a-2.blob",
                    2,
                    2,
                    1,
                    payload_a_2.metadata().unwrap().len() as i64,
                    Some(vec!["payload"]),
                ),
                data_file_meta_with_path(
                    "payload-b-1.blob",
                    0,
                    2,
                    1,
                    payload_b_1.metadata().unwrap().len() as i64,
                    Some(vec!["payload2"]),
                ),
                data_file_meta_with_path(
                    "payload-b-2.blob",
                    2,
                    2,
                    1,
                    payload_b_2.metadata().unwrap().len() as i64,
                    Some(vec!["payload2"]),
                ),
            ])
            .with_row_ranges(vec![RowRange::new(1, 2)])
            .build()
            .unwrap();

        let read = TableRead::new(&table, table.schema().fields().to_vec(), Vec::new());
        let batches = read
            .to_arrow(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(collect_int_values(&batches, "id"), vec![2, 3]);
        assert_eq!(
            collect_binary_values(&batches, "payload"),
            vec![Some(b"a2".to_vec()), Some(b"a3".to_vec())]
        );
        assert_eq!(
            collect_binary_values(&batches, "payload2"),
            vec![Some(b"b2".to_vec()), Some(b"b3".to_vec())]
        );
    }

    fn write_fixed_size_list_parquet(
        path: &std::path::Path,
        col: &str,
        dim: i32,
        rows: &[Option<Vec<f32>>],
    ) {
        use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
        use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
        use parquet::arrow::ArrowWriter;
        use std::fs::File;

        let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), dim).with_field(
            Arc::new(ArrowField::new("element", ArrowDataType::Float32, true)),
        );
        for row in rows {
            match row {
                Some(vals) => {
                    assert_eq!(vals.len() as i32, dim);
                    for v in vals {
                        builder.values().append_value(*v);
                    }
                    builder.append(true);
                }
                None => {
                    for _ in 0..dim {
                        builder.values().append_value(0.0);
                    }
                    builder.append(false);
                }
            }
        }
        let array = builder.finish();
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            col,
            ArrowDataType::FixedSizeList(
                Arc::new(ArrowField::new("element", ArrowDataType::Float32, true)),
                dim,
            ),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    /// Build a `VECTOR<FLOAT, dim>` column type whose element is nullable, matching the
    /// arrow `FixedSizeList(element: Float32 nullable)` produced by the writer helper.
    fn vector_float_type(dim: u32) -> DataType {
        DataType::Vector(VectorType::try_new(true, dim, DataType::Float(FloatType::new())).unwrap())
    }

    #[test]
    fn test_normalize_vector_write_cols_sorts_by_field_position() {
        let fields = vec![
            DataField::new(1, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(2, "a".to_string(), vector_float_type(2)),
            DataField::new(3, "b".to_string(), vector_float_type(2)),
        ];
        // raw write_cols listed b, a -> normalized must be a, b (field-position order)
        let file = data_file("v.vector.parquet", 0, 10, 1, Some(vec!["b", "a"]));
        let normalized = normalize_vector_write_cols(&file, &fields).unwrap();
        assert_eq!(normalized, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn test_normalize_vector_write_cols_rejects_missing_write_cols() {
        let fields = vec![DataField::new(2, "a".to_string(), vector_float_type(2))];
        let file = data_file("v.vector.parquet", 0, 10, 1, None);
        let err = normalize_vector_write_cols(&file, &fields).unwrap_err();
        assert!(matches!(err, Error::DataInvalid { .. }));
    }

    #[test]
    fn test_normalize_vector_write_cols_rejects_unknown_column() {
        let fields = vec![DataField::new(2, "a".to_string(), vector_float_type(2))];
        let file = data_file("v.vector.parquet", 0, 10, 1, Some(vec!["ghost"]));
        let err = normalize_vector_write_cols(&file, &fields).unwrap_err();
        assert!(matches!(err, Error::DataInvalid { .. }));
    }

    /// Locate the embedding column, downcast to `FixedSizeListArray`, and assert the
    /// per-row validity bitmap and child `Float32` values across all batches.
    fn assert_fixed_size_list(
        batches: &[RecordBatch],
        column_name: &str,
        expected_dim: i32,
        expected: &[Option<Vec<f32>>],
    ) {
        let mut row = 0usize;
        for batch in batches {
            let idx = batch.schema().index_of(column_name).unwrap();
            let list = batch
                .column(idx)
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .unwrap();
            assert_eq!(list.value_length(), expected_dim);
            for i in 0..list.len() {
                let want = &expected[row];
                match want {
                    Some(vals) => {
                        assert!(list.is_valid(i), "row {row} expected non-null");
                        let child = list.value(i);
                        let floats = child.as_any().downcast_ref::<Float32Array>().unwrap();
                        let got: Vec<f32> = (0..floats.len()).map(|j| floats.value(j)).collect();
                        assert_eq!(&got, vals, "row {row} value mismatch");
                    }
                    None => {
                        assert!(list.is_null(i), "row {row} expected null");
                    }
                }
                row += 1;
            }
        }
        assert_eq!(row, expected.len(), "row count mismatch");
    }

    /// (1) Provider priority: the normal data file ALSO advertises the embedding write_col,
    /// but the dedicated `.vector.parquet` file must win.
    #[tokio::test]
    async fn test_read_dedicated_vector_parquet_file_with_provider_priority() {
        let tempdir = tempdir().unwrap();
        let table_path = local_file_path(tempdir.path());
        let bucket_dir = tempdir.path().join("bucket-0");
        fs::create_dir_all(&bucket_dir).unwrap();

        // Normal data file carries id AND a (wrong) inline embedding to prove priority.
        let normal_path = bucket_dir.join("data.parquet");
        write_int_parquet_file(&normal_path, vec![("id", vec![1, 2, 3])], None);

        // Dedicated vector file: row1=[1,2], row2=null, row3=[3,4].
        let vector_path = bucket_dir.join("data.vector.parquet");
        write_fixed_size_list_parquet(
            &vector_path,
            "embedding",
            2,
            &[Some(vec![1.0, 2.0]), None, Some(vec![3.0, 4.0])],
        );

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("embedding", vector_float_type(2))
                .option("data-evolution.enabled", "true")
                .build()
                .unwrap(),
        );
        let table = Table::new(
            file_io,
            Identifier::new("default", "vec_priority_t"),
            table_path,
            table_schema,
            None,
        );

        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_path(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![
                // Normal file advertises BOTH id and embedding write_cols.
                data_file_meta_with_path(
                    "data.parquet",
                    0,
                    3,
                    1,
                    normal_path.metadata().unwrap().len() as i64,
                    Some(vec!["id", "embedding"]),
                ),
                data_file_meta_with_path(
                    "data.vector.parquet",
                    0,
                    3,
                    1,
                    vector_path.metadata().unwrap().len() as i64,
                    Some(vec!["embedding"]),
                ),
            ])
            .build()
            .unwrap();

        let read = TableRead::new(&table, table.schema().fields().to_vec(), Vec::new());
        let batches = read
            .to_arrow(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(collect_int_values(&batches, "id"), vec![1, 2, 3]);
        // Value MUST come from the .vector. file (vector-provider priority).
        assert_fixed_size_list(
            &batches,
            "embedding",
            2,
            &[Some(vec![1.0, 2.0]), None, Some(vec![3.0, 4.0])],
        );
    }

    /// (2) Same shape but the dedicated vector file is `.vector.vortex`.
    #[cfg(feature = "vortex")]
    #[tokio::test]
    async fn test_read_dedicated_vector_vortex_file() {
        use crate::arrow::format::create_format_writer;
        use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
        use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};

        let tempdir = tempdir().unwrap();
        let table_path = local_file_path(tempdir.path());
        let bucket_dir = tempdir.path().join("bucket-0");
        fs::create_dir_all(&bucket_dir).unwrap();

        let normal_path = bucket_dir.join("data.parquet");
        write_int_parquet_file(&normal_path, vec![("id", vec![1, 2, 3])], None);

        // Write data.vector.vortex via the format writer (dispatches on the .vortex suffix).
        let vector_path = bucket_dir.join("data.vector.vortex");
        let file_io = FileIOBuilder::new("file").build().unwrap();
        {
            let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), 2).with_field(
                Arc::new(ArrowField::new("element", ArrowDataType::Float32, true)),
            );
            for row in [Some([1.0_f32, 2.0]), None, Some([3.0, 4.0])] {
                match row {
                    Some(vals) => {
                        for v in vals {
                            builder.values().append_value(v);
                        }
                        builder.append(true);
                    }
                    None => {
                        builder.values().append_value(0.0);
                        builder.values().append_value(0.0);
                        builder.append(false);
                    }
                }
            }
            let array = builder.finish();
            let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
                "embedding",
                ArrowDataType::FixedSizeList(
                    Arc::new(ArrowField::new("element", ArrowDataType::Float32, true)),
                    2,
                ),
                true,
            )]));
            let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(array)]).unwrap();
            let output = file_io.new_output(&local_file_path(&vector_path)).unwrap();
            let mut writer =
                create_format_writer(&output, arrow_schema, "zstd", 1, None, None, None)
                    .await
                    .unwrap();
            writer.write(&batch).await.unwrap();
            writer.close().await.unwrap();
        }

        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("embedding", vector_float_type(2))
                .option("data-evolution.enabled", "true")
                .build()
                .unwrap(),
        );
        let table = Table::new(
            file_io,
            Identifier::new("default", "vec_vortex_t"),
            table_path,
            table_schema,
            None,
        );

        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_path(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![
                data_file_meta_with_path(
                    "data.parquet",
                    0,
                    3,
                    1,
                    normal_path.metadata().unwrap().len() as i64,
                    Some(vec!["id"]),
                ),
                data_file_meta_with_path(
                    "data.vector.vortex",
                    0,
                    3,
                    1,
                    vector_path.metadata().unwrap().len() as i64,
                    Some(vec!["embedding"]),
                ),
            ])
            .build()
            .unwrap();

        let read = TableRead::new(&table, table.schema().fields().to_vec(), Vec::new());
        let batches = read
            .to_arrow(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(collect_int_values(&batches, "id"), vec![1, 2, 3]);
        assert_fixed_size_list(
            &batches,
            "embedding",
            2,
            &[Some(vec![1.0, 2.0]), None, Some(vec![3.0, 4.0])],
        );
    }

    /// (3) Multiple vector columns living in ONE `.vector.parquet` file; both must
    /// route to the same VectorBunch source and materialize.
    #[tokio::test]
    async fn test_read_dedicated_vector_file_multiple_columns() {
        use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
        use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
        use parquet::arrow::ArrowWriter;
        use std::fs::File;

        let tempdir = tempdir().unwrap();
        let table_path = local_file_path(tempdir.path());
        let bucket_dir = tempdir.path().join("bucket-0");
        fs::create_dir_all(&bucket_dir).unwrap();

        let normal_path = bucket_dir.join("data.parquet");
        write_int_parquet_file(&normal_path, vec![("id", vec![1, 2, 3])], None);

        // One vector file with two FixedSizeList columns: emb1 (dim 2), emb2 (dim 3).
        let vector_path = bucket_dir.join("data.vector.parquet");
        {
            let elem = || Arc::new(ArrowField::new("element", ArrowDataType::Float32, true));
            let mut b1 = FixedSizeListBuilder::new(Float32Builder::new(), 2).with_field(elem());
            let mut b2 = FixedSizeListBuilder::new(Float32Builder::new(), 3).with_field(elem());
            // emb1: [1,2], null, [5,6]
            for row in [Some(vec![1.0_f32, 2.0]), None, Some(vec![5.0, 6.0])] {
                match row {
                    Some(v) => {
                        for x in v {
                            b1.values().append_value(x);
                        }
                        b1.append(true);
                    }
                    None => {
                        b1.values().append_value(0.0);
                        b1.values().append_value(0.0);
                        b1.append(false);
                    }
                }
            }
            // emb2: [7,8,9], [1,1,1], null
            for row in [
                Some(vec![7.0_f32, 8.0, 9.0]),
                Some(vec![1.0, 1.0, 1.0]),
                None,
            ] {
                match row {
                    Some(v) => {
                        for x in v {
                            b2.values().append_value(x);
                        }
                        b2.append(true);
                    }
                    None => {
                        for _ in 0..3 {
                            b2.values().append_value(0.0);
                        }
                        b2.append(false);
                    }
                }
            }
            let schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new("emb1", ArrowDataType::FixedSizeList(elem(), 2), true),
                ArrowField::new("emb2", ArrowDataType::FixedSizeList(elem(), 3), true),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(b1.finish()), Arc::new(b2.finish())],
            )
            .unwrap();
            let file = File::create(&vector_path).unwrap();
            let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("emb1", vector_float_type(2))
                .column("emb2", vector_float_type(3))
                .option("data-evolution.enabled", "true")
                .build()
                .unwrap(),
        );
        let table = Table::new(
            file_io,
            Identifier::new("default", "vec_multi_t"),
            table_path,
            table_schema,
            None,
        );

        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_path(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![
                data_file_meta_with_path(
                    "data.parquet",
                    0,
                    3,
                    1,
                    normal_path.metadata().unwrap().len() as i64,
                    Some(vec!["id"]),
                ),
                data_file_meta_with_path(
                    "data.vector.parquet",
                    0,
                    3,
                    1,
                    vector_path.metadata().unwrap().len() as i64,
                    Some(vec!["emb1", "emb2"]),
                ),
            ])
            .build()
            .unwrap();

        let read = TableRead::new(&table, table.schema().fields().to_vec(), Vec::new());
        let batches = read
            .to_arrow(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(collect_int_values(&batches, "id"), vec![1, 2, 3]);
        assert_fixed_size_list(
            &batches,
            "emb1",
            2,
            &[Some(vec![1.0, 2.0]), None, Some(vec![5.0, 6.0])],
        );
        assert_fixed_size_list(
            &batches,
            "emb2",
            3,
            &[Some(vec![7.0, 8.0, 9.0]), Some(vec![1.0, 1.0, 1.0]), None],
        );
    }

    /// (4) Inline fallback: embedding lives in the normal parquet, NO `.vector.` file
    /// present. Routing must fall back to the normal provider (PR 2 compatibility).
    #[tokio::test]
    async fn test_inline_vector_fallback_still_reads() {
        let tempdir = tempdir().unwrap();
        let table_path = local_file_path(tempdir.path());
        let bucket_dir = tempdir.path().join("bucket-0");
        fs::create_dir_all(&bucket_dir).unwrap();

        // Single normal parquet holding id + an inline FixedSizeList embedding.
        let normal_path = bucket_dir.join("data.parquet");
        {
            use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
            use arrow_schema::{
                DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
            };
            use parquet::arrow::ArrowWriter;
            use std::fs::File;

            let mut emb = FixedSizeListBuilder::new(Float32Builder::new(), 2).with_field(Arc::new(
                ArrowField::new("element", ArrowDataType::Float32, true),
            ));
            for row in [Some([1.0_f32, 2.0]), None, Some([3.0, 4.0])] {
                match row {
                    Some(vals) => {
                        for v in vals {
                            emb.values().append_value(v);
                        }
                        emb.append(true);
                    }
                    None => {
                        emb.values().append_value(0.0);
                        emb.values().append_value(0.0);
                        emb.append(false);
                    }
                }
            }
            let schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new("id", ArrowDataType::Int32, false),
                ArrowField::new(
                    "embedding",
                    ArrowDataType::FixedSizeList(
                        Arc::new(ArrowField::new("element", ArrowDataType::Float32, true)),
                        2,
                    ),
                    true,
                ),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(emb.finish()),
                ],
            )
            .unwrap();
            let file = File::create(&normal_path).unwrap();
            let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("embedding", vector_float_type(2))
                .option("data-evolution.enabled", "true")
                .build()
                .unwrap(),
        );
        let table = Table::new(
            file_io,
            Identifier::new("default", "vec_inline_t"),
            table_path,
            table_schema,
            None,
        );

        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_path(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![data_file_meta_with_path(
                "data.parquet",
                0,
                3,
                1,
                normal_path.metadata().unwrap().len() as i64,
                Some(vec!["id", "embedding"]),
            )])
            .build()
            .unwrap();

        let read = TableRead::new(&table, table.schema().fields().to_vec(), Vec::new());
        let batches = read
            .to_arrow(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(collect_int_values(&batches, "id"), vec![1, 2, 3]);
        assert_fixed_size_list(
            &batches,
            "embedding",
            2,
            &[Some(vec![1.0, 2.0]), None, Some(vec![3.0, 4.0])],
        );
    }

    /// (5) A `.vector.` file is present, but a non-vector field (`id`) must still be
    /// read from the normal file, never mis-selected from the vector file.
    #[tokio::test]
    async fn test_non_vector_field_ignores_vector_file() {
        let tempdir = tempdir().unwrap();
        let table_path = local_file_path(tempdir.path());
        let bucket_dir = tempdir.path().join("bucket-0");
        fs::create_dir_all(&bucket_dir).unwrap();

        let normal_path = bucket_dir.join("data.parquet");
        write_int_parquet_file(&normal_path, vec![("id", vec![10, 20, 30])], None);

        let vector_path = bucket_dir.join("data.vector.parquet");
        write_fixed_size_list_parquet(
            &vector_path,
            "embedding",
            2,
            &[Some(vec![1.0, 2.0]), None, Some(vec![3.0, 4.0])],
        );

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("embedding", vector_float_type(2))
                .option("data-evolution.enabled", "true")
                .build()
                .unwrap(),
        );
        let table = Table::new(
            file_io,
            Identifier::new("default", "vec_nonvec_t"),
            table_path,
            table_schema,
            None,
        );

        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_path(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![
                data_file_meta_with_path(
                    "data.parquet",
                    0,
                    3,
                    1,
                    normal_path.metadata().unwrap().len() as i64,
                    Some(vec!["id"]),
                ),
                data_file_meta_with_path(
                    "data.vector.parquet",
                    0,
                    3,
                    1,
                    vector_path.metadata().unwrap().len() as i64,
                    Some(vec!["embedding"]),
                ),
            ])
            .build()
            .unwrap();

        // Project only the non-vector `id` field.
        let id_field = table
            .schema()
            .fields()
            .iter()
            .find(|f| f.name() == "id")
            .unwrap()
            .clone();
        let read = TableRead::new(&table, vec![id_field], Vec::new());
        let batches = read
            .to_arrow(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(collect_int_values(&batches, "id"), vec![10, 20, 30]);
    }

    /// (8) normal data.parquet (id) + 3 rolled .vector.parquet segments (embedding,
    /// contiguous row ranges) reassemble into one column with values in correct order.
    #[tokio::test]
    async fn test_read_rolled_vector_segments_reassemble() {
        let tempdir = tempdir().unwrap();
        let table_path = local_file_path(tempdir.path());
        let bucket_dir = tempdir.path().join("bucket-0");
        fs::create_dir_all(&bucket_dir).unwrap();

        // Normal data file: id 1..=6 (6 rows total).
        let normal_path = bucket_dir.join("data.parquet");
        write_int_parquet_file(&normal_path, vec![("id", vec![1, 2, 3, 4, 5, 6])], None);

        // Three rolled vector segments, 2 rows each, contiguous first_row_ids 0,2,4.
        let seg1 = bucket_dir.join("emb-1.vector.parquet");
        write_fixed_size_list_parquet(
            &seg1,
            "embedding",
            2,
            &[Some(vec![1.0, 1.0]), Some(vec![2.0, 2.0])],
        );
        let seg2 = bucket_dir.join("emb-2.vector.parquet");
        write_fixed_size_list_parquet(&seg2, "embedding", 2, &[Some(vec![3.0, 3.0]), None]);
        let seg3 = bucket_dir.join("emb-3.vector.parquet");
        write_fixed_size_list_parquet(
            &seg3,
            "embedding",
            2,
            &[Some(vec![5.0, 5.0]), Some(vec![6.0, 6.0])],
        );

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("embedding", vector_float_type(2))
                .option("data-evolution.enabled", "true")
                .build()
                .unwrap(),
        );
        let table = Table::new(
            file_io,
            Identifier::new("default", "vec_rolled_t"),
            table_path,
            table_schema,
            None,
        );

        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_path(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![
                data_file_meta_with_path(
                    "data.parquet",
                    0,
                    6,
                    1,
                    normal_path.metadata().unwrap().len() as i64,
                    Some(vec!["id"]),
                ),
                data_file_meta_with_path(
                    "emb-1.vector.parquet",
                    0,
                    2,
                    1,
                    seg1.metadata().unwrap().len() as i64,
                    Some(vec!["embedding"]),
                ),
                data_file_meta_with_path(
                    "emb-2.vector.parquet",
                    2,
                    2,
                    1,
                    seg2.metadata().unwrap().len() as i64,
                    Some(vec!["embedding"]),
                ),
                data_file_meta_with_path(
                    "emb-3.vector.parquet",
                    4,
                    2,
                    1,
                    seg3.metadata().unwrap().len() as i64,
                    Some(vec!["embedding"]),
                ),
            ])
            .build()
            .unwrap();

        let read = TableRead::new(&table, table.schema().fields().to_vec(), Vec::new());
        let batches = read
            .to_arrow(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(collect_int_values(&batches, "id"), vec![1, 2, 3, 4, 5, 6]);
        assert_fixed_size_list(
            &batches,
            "embedding",
            2,
            &[
                Some(vec![1.0, 1.0]),
                Some(vec![2.0, 2.0]),
                Some(vec![3.0, 3.0]),
                None,
                Some(vec![5.0, 5.0]),
                Some(vec![6.0, 6.0]),
            ],
        );
    }

    /// (9) row_ranges selecting rows ACROSS a segment boundary -> correct subset,
    /// locking in the to_local_row_ranges clip-per-segment behavior.
    ///
    /// `RowRange::new` is inclusive on both ends (see source::RowRange::count), so the
    /// absolute window [1, 3] selects rows at index 1,2,3 (ids 2,3,4). Row 1 lives in
    /// segment emb-1 [0,2) and rows 2,3 live in emb-2 [2,4), so the window straddles the
    /// emb-1/emb-2 boundary and must be clipped per segment via `to_local_row_ranges`.
    #[tokio::test]
    async fn test_read_rolled_vector_segments_with_cross_boundary_row_ranges() {
        let tempdir = tempdir().unwrap();
        let table_path = local_file_path(tempdir.path());
        let bucket_dir = tempdir.path().join("bucket-0");
        fs::create_dir_all(&bucket_dir).unwrap();

        // Normal data file: id 1..=6 (6 rows total).
        let normal_path = bucket_dir.join("data.parquet");
        write_int_parquet_file(&normal_path, vec![("id", vec![1, 2, 3, 4, 5, 6])], None);

        // Three rolled vector segments, 2 rows each, contiguous first_row_ids 0,2,4.
        let seg1 = bucket_dir.join("emb-1.vector.parquet");
        write_fixed_size_list_parquet(
            &seg1,
            "embedding",
            2,
            &[Some(vec![1.0, 1.0]), Some(vec![2.0, 2.0])],
        );
        let seg2 = bucket_dir.join("emb-2.vector.parquet");
        write_fixed_size_list_parquet(&seg2, "embedding", 2, &[Some(vec![3.0, 3.0]), None]);
        let seg3 = bucket_dir.join("emb-3.vector.parquet");
        write_fixed_size_list_parquet(
            &seg3,
            "embedding",
            2,
            &[Some(vec![5.0, 5.0]), Some(vec![6.0, 6.0])],
        );

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("embedding", vector_float_type(2))
                .option("data-evolution.enabled", "true")
                .build()
                .unwrap(),
        );
        let table = Table::new(
            file_io,
            Identifier::new("default", "vec_rolled_rr_t"),
            table_path,
            table_schema,
            None,
        );

        // Select absolute rows [1, 3] -> rows at index 1,2,3 (ids 2,3,4;
        // embeddings [2,2],[3,3],null). This window straddles the emb-1/emb-2 boundary.
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_path(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![
                data_file_meta_with_path(
                    "data.parquet",
                    0,
                    6,
                    1,
                    normal_path.metadata().unwrap().len() as i64,
                    Some(vec!["id"]),
                ),
                data_file_meta_with_path(
                    "emb-1.vector.parquet",
                    0,
                    2,
                    1,
                    seg1.metadata().unwrap().len() as i64,
                    Some(vec!["embedding"]),
                ),
                data_file_meta_with_path(
                    "emb-2.vector.parquet",
                    2,
                    2,
                    1,
                    seg2.metadata().unwrap().len() as i64,
                    Some(vec!["embedding"]),
                ),
                data_file_meta_with_path(
                    "emb-3.vector.parquet",
                    4,
                    2,
                    1,
                    seg3.metadata().unwrap().len() as i64,
                    Some(vec!["embedding"]),
                ),
            ])
            .with_row_ranges(vec![RowRange::new(1, 3)])
            .build()
            .unwrap();

        let read = TableRead::new(&table, table.schema().fields().to_vec(), Vec::new());
        let batches = read
            .to_arrow(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(collect_int_values(&batches, "id"), vec![2, 3, 4]);
        assert_fixed_size_list(
            &batches,
            "embedding",
            2,
            &[Some(vec![2.0, 2.0]), Some(vec![3.0, 3.0]), None],
        );
    }

    /// (6) Row-range mismatch: normal file row_count=3 but `.vector.parquet` row_count=2
    /// must surface as DataInvalid.
    #[tokio::test]
    async fn test_read_vector_file_row_range_mismatch_errors() {
        let tempdir = tempdir().unwrap();
        let table_path = local_file_path(tempdir.path());
        let bucket_dir = tempdir.path().join("bucket-0");
        fs::create_dir_all(&bucket_dir).unwrap();

        let normal_path = bucket_dir.join("data.parquet");
        write_int_parquet_file(&normal_path, vec![("id", vec![1, 2, 3])], None);

        let vector_path = bucket_dir.join("data.vector.parquet");
        write_fixed_size_list_parquet(
            &vector_path,
            "embedding",
            2,
            &[Some(vec![1.0, 2.0]), Some(vec![3.0, 4.0])],
        );

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("embedding", vector_float_type(2))
                .option("data-evolution.enabled", "true")
                .build()
                .unwrap(),
        );
        let table = Table::new(
            file_io,
            Identifier::new("default", "vec_mismatch_t"),
            table_path,
            table_schema,
            None,
        );

        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_path(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![
                data_file_meta_with_path(
                    "data.parquet",
                    0,
                    3,
                    1,
                    normal_path.metadata().unwrap().len() as i64,
                    Some(vec!["id"]),
                ),
                data_file_meta_with_path(
                    "data.vector.parquet",
                    0,
                    2, // row_count mismatch vs the normal file's 3
                    1,
                    vector_path.metadata().unwrap().len() as i64,
                    Some(vec!["embedding"]),
                ),
            ])
            .build()
            .unwrap();

        let read = TableRead::new(&table, table.schema().fields().to_vec(), Vec::new());
        let result = read.to_arrow(&[split]);
        let collected = match result {
            Ok(stream) => stream.try_collect::<Vec<_>>().await,
            Err(e) => Err(e),
        };
        assert!(
            matches!(collected, Err(Error::DataInvalid { .. })),
            "expected DataInvalid, got {collected:?}"
        );
    }

    fn resolved_info(field_ids: Vec<i32>) -> ResolvedFileInfo {
        ResolvedFileInfo {
            field_ids,
            data_fields: None,
            normalized_write_cols: None,
        }
    }

    fn data_file(
        file_name: &str,
        first_row_id: i64,
        row_count: i64,
        max_sequence_number: i64,
        write_cols: Option<Vec<&str>>,
    ) -> DataFileMeta {
        DataFileMeta {
            file_name: file_name.to_string(),
            file_size: 0,
            row_count,
            min_key: Vec::new(),
            max_key: Vec::new(),
            key_stats: BinaryTableStats::new(Vec::new(), Vec::new(), Vec::new()),
            value_stats: BinaryTableStats::new(Vec::new(), Vec::new(), Vec::new()),
            min_sequence_number: 0,
            max_sequence_number,
            schema_id: 0,
            level: 0,
            extra_files: Vec::new(),
            creation_time: None,
            delete_row_count: None,
            embedded_index: None,
            file_source: None,
            value_stats_cols: None,
            external_path: None,
            first_row_id: Some(first_row_id),
            write_cols: write_cols.map(|cols| cols.into_iter().map(str::to_string).collect()),
        }
    }

    fn data_file_meta_with_path(
        file_name: &str,
        first_row_id: i64,
        row_count: i64,
        max_sequence_number: i64,
        file_size: i64,
        write_cols: Option<Vec<&str>>,
    ) -> DataFileMeta {
        let mut file = data_file(
            file_name,
            first_row_id,
            row_count,
            max_sequence_number,
            write_cols,
        );
        file.file_size = file_size;
        file
    }

    fn copy_blob_fixture(name: &str, destination: &Path) {
        let source = blob_fixture_path(name);
        fs::copy(&source, destination).unwrap_or_else(|e| {
            panic!("Failed to copy blob fixture {source:?} -> {destination:?}: {e}")
        });
    }

    fn blob_fixture_path(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("testdata/blob/{name}"))
    }

    fn collect_int_values(batches: &[RecordBatch], column_name: &str) -> Vec<i32> {
        batches
            .iter()
            .flat_map(|batch| {
                let idx = batch.schema().index_of(column_name).unwrap();
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                (0..array.len())
                    .map(|row| array.value(row))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn collect_binary_values(batches: &[RecordBatch], column_name: &str) -> Vec<Option<Vec<u8>>> {
        batches
            .iter()
            .flat_map(|batch| {
                let idx = batch.schema().index_of(column_name).unwrap();
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap();
                (0..array.len())
                    .map(|row| (!array.is_null(row)).then(|| array.value(row).to_vec()))
                    .collect::<Vec<_>>()
            })
            .collect()
    }
}
