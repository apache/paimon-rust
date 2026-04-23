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
use crate::spec::{DataField, DataFileMeta, DataType, ROW_ID_FIELD_NAME};
use crate::table::blob_file_writer::is_blob_file_name;
use crate::table::schema_manager::SchemaManager;
use crate::table::ArrowRecordBatchStream;
use crate::table::RowRange;
use crate::{DataSplit, Error};
use arrow_array::{Array, Int64Array, RecordBatch};
use async_stream::try_stream;
use futures::StreamExt;
use std::collections::{HashMap, HashSet};
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
                    let effective_row_ranges = row_ranges.clone();
                    let expected_output_rows = count_selected_rows(
                        prepared_group.first_row_id,
                        prepared_group.logical_row_count,
                        effective_row_ranges.as_deref(),
                    )?;

                    let selected_row_ids = if self.row_id_index.is_some() {
                        effective_row_ranges.as_ref().map(|ranges| {
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
        } => file_reader.read_single_file_stream(
            split,
            file.as_ref().clone(),
            data_fields.clone(),
            None,
            row_ranges,
        ),
        FieldSource::BlobBunch {
            bunch, data_fields, ..
        } => {
            let split = split.clone();
            let files = bunch.files.clone();
            let data_fields = data_fields.clone();
            Ok(try_stream! {
                for file in files {
                    let mut stream = file_reader.read_single_file_stream(
                        &split,
                        file,
                        data_fields.clone(),
                        None,
                        row_ranges.clone(),
                    )?;
                    while let Some(batch) = stream.next().await {
                        yield batch?;
                    }
                }
            }
            .boxed())
        }
    }
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
            .filter(|file| !is_blob_file_name(&file.file_name))
            .collect();
        if data_files.is_empty() {
            return Err(Error::DataInvalid {
                message: "Field merge split containing .blob files requires at least one non-blob data file".to_string(),
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
}

async fn load_file_infos(
    schema_manager: &SchemaManager,
    table_schema_id: i64,
    table_fields: &[DataField],
    files: &[DataFileMeta],
) -> crate::Result<Vec<ResolvedFileInfo>> {
    let mut infos = Vec::with_capacity(files.len());

    for file in files {
        if file.schema_id == table_schema_id {
            infos.push(ResolvedFileInfo {
                field_ids: resolve_field_ids(file, table_fields)?,
                data_fields: None,
            });
        } else {
            let data_schema = schema_manager.schema(file.schema_id).await?;
            let data_fields = data_schema.fields().to_vec();
            infos.push(ResolvedFileInfo {
                field_ids: resolve_field_ids(file, &data_fields)?,
                data_fields: Some(data_fields),
            });
        }
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
    let mut normal_source_indices: HashMap<usize, usize> = HashMap::new();
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
        } else {
            expected_blob_row_count = Some(file.row_count);
            let source_idx = sources.len();
            sources.push(FieldSource::DataFile {
                file: Box::new(file.clone()),
                data_fields: info.data_fields.clone(),
                read_fields: Vec::new(),
            });
            normal_source_indices.insert(file_idx, source_idx);
        }
    }

    let mut column_plan = Vec::with_capacity(read_type.len());
    for field in read_type {
        let source_idx = if matches!(field.data_type(), DataType::Blob(_))
            && !blob_descriptor_fields.contains(field.name())
        {
            blob_source_indices.get(&field.id()).copied()
        } else {
            select_normal_provider(
                &prepared_group.files,
                file_infos,
                &normal_source_indices,
                field.id(),
            )
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

    Ok(SourcePlan {
        sources,
        column_plan,
    })
}

fn select_normal_provider(
    files: &[DataFileMeta],
    file_infos: &[ResolvedFileInfo],
    normal_source_indices: &HashMap<usize, usize>,
    field_id: i32,
) -> Option<usize> {
    files.iter().enumerate().find_map(|(file_idx, file)| {
        if is_blob_file_name(&file.file_name) {
            return None;
        }

        file_infos[file_idx]
            .field_ids
            .contains(&field_id)
            .then(|| normal_source_indices.get(&file_idx).copied())
            .flatten()
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
            | FieldSource::BlobBunch { read_fields, .. } => read_fields,
        }
    }

    fn add_read_field(&mut self, field: DataField) -> usize {
        let read_fields = match self {
            FieldSource::DataFile { read_fields, .. }
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
            FieldSource::DataFile { .. } => None,
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

fn normalize_merge_group(files: Vec<DataFileMeta>) -> crate::Result<Vec<DataFileMeta>> {
    let mut data_files = Vec::new();
    let mut blob_files = Vec::new();

    for file in files {
        if is_blob_file_name(&file.file_name) {
            blob_files.push(file);
        } else {
            data_files.push(file);
        }
    }

    data_files.sort_by_key(|f| std::cmp::Reverse(f.max_sequence_number));
    if let Some(first) = data_files.first() {
        let first_row_id = first.first_row_id.ok_or_else(|| Error::DataInvalid {
            message: "All data files in a field merge split should have first_row_id".to_string(),
            source: None,
        })?;
        let first_row_count = first.row_count;
        for file in data_files.iter().skip(1) {
            if file.first_row_id != Some(first_row_id) || file.row_count != first_row_count {
                return Err(Error::DataInvalid {
                    message:
                        "All data files in a field merge split should have the same row id range."
                            .to_string(),
                    source: None,
                });
            }
        }
    }

    blob_files.sort_by(|left, right| {
        let left_first_row_id = left.first_row_id.unwrap_or(i64::MIN);
        let right_first_row_id = right.first_row_id.unwrap_or(i64::MIN);
        left_first_row_id
            .cmp(&right_first_row_id)
            .then_with(|| right.max_sequence_number.cmp(&left.max_sequence_number))
    });

    if blob_files.iter().any(|file| file.first_row_id.is_none()) {
        return Err(Error::DataInvalid {
            message: "All blob files in a field merge split should have first_row_id".to_string(),
            source: None,
        });
    }

    data_files.extend(blob_files);
    Ok(data_files)
}

fn count_selected_rows(
    first_row_id: i64,
    row_count: i64,
    row_ranges: Option<&[RowRange]>,
) -> crate::Result<usize> {
    match row_ranges {
        Some(ranges) => Ok(expand_selected_row_ids(first_row_id, row_count, ranges).len()),
        None => usize::try_from(row_count).map_err(|e| Error::DataInvalid {
            message: format!("Invalid logical row count {row_count}"),
            source: Some(Box::new(e)),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Identifier;
    use crate::io::FileIOBuilder;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{BinaryRow, BlobType, IntType, Schema, TableSchema};
    use crate::table::{DataSplitBuilder, Table, TableRead};
    use arrow_array::{Array, BinaryArray, Int32Array, RecordBatch};
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
            FieldSource::DataFile { .. } => panic!("expected blob bunch source"),
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
            FieldSource::DataFile { .. } => panic!("expected blob bunch source"),
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
            FieldSource::DataFile { .. } => panic!("expected blob bunch source"),
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

    fn resolved_info(field_ids: Vec<i32>) -> ResolvedFileInfo {
        ResolvedFileInfo {
            field_ids,
            data_fields: None,
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
