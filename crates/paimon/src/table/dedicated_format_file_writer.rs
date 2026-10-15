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

use crate::io::FileIO;
use crate::resource::ResourceContext;
use crate::spec::{CoreOptions, DataField, DataFileMeta, DataType};
use crate::table::data_file_writer::DataFileWriter;
use crate::Result;
use arrow_array::RecordBatch;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub(crate) fn is_blob_or_video_file_name(file_name: &str) -> bool {
    let lower = file_name.to_ascii_lowercase();
    lower.ends_with(".blob") || lower.ends_with(".video")
}

struct BlobFieldWriter {
    writer: DataFileWriter,
    field_name: String,
    column_index: usize,
}

struct VectorFieldWriter {
    writer: DataFileWriter,
    field_names: Vec<String>,
    column_indices: Vec<usize>,
    schema: Arc<arrow_schema::Schema>,
}

/// Writes append-only data with columns split into dedicated file formats.
///
/// Remaining columns go to the table's normal append `DataFileWriter`.
/// Each non-descriptor blob column gets its own `DataFileWriter` with
/// `file_format = "blob"` and `write_cols = Some(vec![field_name])`.
/// When `vector.file.format` is configured, all VECTOR columns are written
/// together to a dedicated `*.vector.<format>` file.
///
/// If a blob value is already a serialized `BlobDescriptor`, the actual data is
/// resolved from the referenced URI and written to the `.blob` file.
pub(crate) struct AppendDedicatedFormatFileWriter {
    normal_writer: Option<DataFileWriter>,
    blob_writers: Vec<BlobFieldWriter>,
    vector_writer: Option<VectorFieldWriter>,
    normal_column_indices: Vec<usize>,
    normal_schema: Arc<arrow_schema::Schema>,
    // Completed groups awaiting prepare_commit; each format owns its abort policy.
    written_files: Vec<DataFileMeta>,
    target_file_row_num: i64,
    current_group_row_count: i64,
}

impl AppendDedicatedFormatFileWriter {
    #[cfg(test)]
    pub(super) fn inject_blob_close_failure(&mut self) {
        self.blob_writers[0].writer.inject_close_failure();
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        file_io: FileIO,
        table_location: String,
        partition_path: String,
        bucket: i32,
        schema_id: i64,
        target_file_size: i64,
        target_file_row_num: i64,
        blob_target_file_size: i64,
        file_compression: String,
        file_compression_zstd_level: i32,
        write_buffer_size: i64,
        file_format: String,
        vector_target_file_size: i64,
        vector_file_format: Option<&str>,
        input_schema: &arrow_schema::Schema,
        write_fields: &[DataField],
        table_fields: &[DataField],
        format_options: &HashMap<String, String>,
        blob_inline_fields: &HashSet<String>,
    ) -> Result<Self> {
        let paths = Arc::new(super::data_file_path_factory::DataFilePathFactory::new(
            &table_location,
            &partition_path,
            bucket,
            format_options,
        )?);
        let mut normal_column_indices = Vec::new();
        let mut normal_arrow_fields = Vec::new();
        let mut normal_table_fields = Vec::new();
        let mut blob_writers = Vec::new();
        let mut vector_column_indices = Vec::new();
        let mut vector_arrow_fields = Vec::new();
        let mut vector_table_fields = Vec::new();
        let mut vector_field_names = Vec::new();

        for (idx, field) in write_fields.iter().enumerate() {
            let is_blob = field.data_type().is_blob_file_field();
            let is_inline = blob_inline_fields.contains(field.name());
            let is_dedicated_vector =
                vector_file_format.is_some() && matches!(field.data_type(), DataType::Vector(_));

            if is_dedicated_vector {
                vector_column_indices.push(idx);
                vector_arrow_fields.push(input_schema.field(idx).clone());
                vector_table_fields.push(field.clone());
                vector_field_names.push(field.name().to_string());
            } else if is_blob && !is_inline {
                blob_writers.push(BlobFieldWriter {
                    writer: DataFileWriter::new(
                        file_io.clone(),
                        table_location.clone(),
                        partition_path.clone(),
                        bucket,
                        schema_id,
                        blob_target_file_size,
                        String::new(),
                        0,
                        write_buffer_size,
                        "blob".to_string(),
                        vec![field.clone()],
                        format_options.clone(),
                        Some(0),
                        None,
                        Some(vec![field.name().to_string()]),
                    )?
                    .with_path_factory(paths.clone())
                    .with_target_file_row_num(target_file_row_num),
                    field_name: field.name().to_string(),
                    column_index: idx,
                });
            } else {
                normal_column_indices.push(idx);
                normal_arrow_fields.push(input_schema.field(idx).clone());
                normal_table_fields.push(field.clone());
            }
        }

        let normal_schema = Arc::new(arrow_schema::Schema::new(normal_arrow_fields));
        let normal_field_names: Vec<String> = normal_table_fields
            .iter()
            .map(|field| field.name().to_string())
            .collect();
        let vector_writer = if let Some(vector_file_format) = vector_file_format {
            if vector_table_fields.is_empty() {
                None
            } else {
                let vector_schema = Arc::new(arrow_schema::Schema::new(vector_arrow_fields));
                Some(VectorFieldWriter {
                    writer: DataFileWriter::new(
                        file_io.clone(),
                        table_location.clone(),
                        partition_path.clone(),
                        bucket,
                        schema_id,
                        vector_target_file_size,
                        file_compression.clone(),
                        file_compression_zstd_level,
                        write_buffer_size,
                        format!("vector.{}", vector_file_format.trim().to_ascii_lowercase()),
                        vector_table_fields,
                        format_options.clone(),
                        Some(0),
                        None,
                        Some(vector_field_names.clone()),
                    )?
                    .with_path_factory(paths.clone())
                    .with_target_file_row_num(target_file_row_num),
                    field_names: vector_field_names,
                    column_indices: vector_column_indices,
                    schema: vector_schema,
                })
            }
        } else {
            None
        };

        let core_options = CoreOptions::new(format_options);
        let normal_write_cols = (!super::data_evolution_fields::can_omit_normal_write_cols(
            table_fields,
            &normal_field_names,
            &core_options,
        ))
        .then_some(normal_field_names);
        let normal_index =
            super::data_file_index_writer::FileIndexOptions::parse(format_options, table_fields)?
                .and_then(|options| options.project_to_fields(&normal_table_fields))
                .map(Arc::new);
        let normal_writer = if normal_table_fields.is_empty() {
            // Java has no normal writer when the write type is entirely
            // dedicated columns. Do not create an empty Parquet file.
            None
        } else {
            let normal_writer = DataFileWriter::new(
                file_io.clone(),
                table_location,
                partition_path,
                bucket,
                schema_id,
                target_file_size,
                file_compression,
                file_compression_zstd_level,
                write_buffer_size,
                file_format,
                normal_table_fields,
                format_options.clone(),
                Some(0),
                None,
                normal_write_cols,
            )?
            .with_path_factory(paths.clone())
            .with_file_index(normal_index)
            .with_target_file_row_num(target_file_row_num);
            // A view-only schema also uses this adapter to translate references,
            // but Java treats its physical writer as an ordinary inline writer.
            Some(if !blob_writers.is_empty() || vector_writer.is_some() {
                normal_writer.without_row_sidecar()
            } else {
                normal_writer
            })
        };

        Ok(Self {
            written_files: Vec::new(),
            normal_writer,
            blob_writers,
            vector_writer,
            normal_column_indices,
            normal_schema,
            target_file_row_num,
            current_group_row_count: 0,
        })
    }

    pub(crate) fn with_resources(mut self, resources: Option<ResourceContext>) -> Self {
        if let Some(normal) = &mut self.normal_writer {
            normal.set_resources(resources.clone());
        }
        for blob in &mut self.blob_writers {
            blob.writer.set_resources(resources.clone());
        }
        if let Some(vector) = &mut self.vector_writer {
            vector.writer.set_resources(resources);
        }
        self
    }

    pub(crate) fn with_blob_writer_options(
        mut self,
        consumer: Option<Arc<dyn crate::spec::BlobConsumer>>,
        uri_reader_factory: Option<Arc<dyn crate::io::UriReaderFactory>>,
    ) -> Result<Self> {
        if consumer.is_some() || uri_reader_factory.is_some() {
            for blob in &mut self.blob_writers {
                blob.writer
                    .set_blob_writer_options(consumer.clone(), uri_reader_factory.clone())?;
            }
        }
        Ok(self)
    }

    pub(crate) async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        // Write normal columns
        if let Some(normal) = &mut self.normal_writer {
            let normal_columns: Vec<Arc<dyn arrow_array::Array>> = self
                .normal_column_indices
                .iter()
                .map(|&idx| batch.column(idx).clone())
                .collect();
            let normal_batch = RecordBatch::try_new(self.normal_schema.clone(), normal_columns)
                .map_err(|e| crate::Error::DataInvalid {
                    message: format!("Failed to project normal columns: {e}"),
                    source: None,
                })?;
            normal.write(&normal_batch).await?;
        }

        // Write each blob column directly — BlobFormatWriter resolves descriptors inline
        for blob_writer in &mut self.blob_writers {
            let col = batch.column(blob_writer.column_index).clone();
            let schema = Arc::new(arrow_schema::Schema::new(vec![batch
                .schema()
                .field(blob_writer.column_index)
                .clone()]));
            let blob_batch =
                RecordBatch::try_new(schema, vec![col]).map_err(|e| crate::Error::DataInvalid {
                    message: format!(
                        "Failed to project blob column '{}': {e}",
                        blob_writer.field_name
                    ),
                    source: None,
                })?;
            // Serialized descriptor size says nothing about the payload size.
            // Java checks the physical Blob bytes after every row.
            for row in 0..blob_batch.num_rows() {
                blob_writer.writer.write(&blob_batch.slice(row, 1)).await?;
            }
        }

        if let Some(vector_writer) = &mut self.vector_writer {
            let vector_columns: Vec<Arc<dyn arrow_array::Array>> = vector_writer
                .column_indices
                .iter()
                .map(|&idx| batch.column(idx).clone())
                .collect();
            let vector_batch =
                match RecordBatch::try_new(vector_writer.schema.clone(), vector_columns) {
                    Ok(batch) => batch,
                    Err(e) => {
                        return Err(crate::Error::DataInvalid {
                            message: format!(
                                "Failed to project vector columns {:?}: {e}",
                                vector_writer.field_names
                            ),
                            source: None,
                        });
                    }
                };
            vector_writer.writer.write(&vector_batch).await?;
        }

        self.current_group_row_count = self
            .current_group_row_count
            .saturating_add(batch.num_rows() as i64);
        if self.current_group_row_count >= self.target_file_row_num
            || self
                .normal_writer
                .as_ref()
                .is_some_and(|writer| !writer.has_open_file())
        {
            self.close_group().await?;
        }
        Ok(())
    }

    pub(crate) async fn abort(&mut self) {
        self.delete_completed_files().await;
        self.current_group_row_count = 0;
        if let Some(normal) = &mut self.normal_writer {
            normal.abort().await;
        }
        for writer in &mut self.blob_writers {
            writer.writer.abort().await;
        }
        if let Some(writer) = &mut self.vector_writer {
            writer.writer.abort().await;
        }
    }

    /// Each physical writer owns its abort policy. A normal writer must not
    /// delete Blob files whose descriptors may have escaped through a consumer.
    async fn delete_completed_files(&mut self) {
        for file in std::mem::take(&mut self.written_files) {
            let blob = self.blob_writers.iter_mut().find(|blob| {
                file.write_cols.as_deref() == Some(std::slice::from_ref(&blob.field_name))
            });
            let writer = blob
                .map(|blob| &mut blob.writer)
                .or(self.normal_writer.as_mut())
                .or_else(|| self.vector_writer.as_mut().map(|vector| &mut vector.writer));
            if let Some(writer) = writer {
                writer.delete_files(std::slice::from_ref(&file)).await;
            }
        }
    }

    pub(crate) async fn prepare_commit(&mut self) -> Result<Vec<DataFileMeta>> {
        self.close_group().await?;
        Ok(std::mem::take(&mut self.written_files))
    }

    async fn close_group(&mut self) -> Result<()> {
        let writers = self
            .normal_writer
            .iter_mut()
            .chain(
                self.blob_writers
                    .iter_mut()
                    .map(|writer| &mut writer.writer),
            )
            .chain(
                self.vector_writer
                    .iter_mut()
                    .map(|writer| &mut writer.writer),
            );
        match DataFileWriter::prepare_group(writers).await {
            Ok(files) => self.written_files.extend(files.into_iter().flatten()),
            Err(error) => {
                self.abort().await;
                return Err(error);
            }
        }
        self.current_group_row_count = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{FileIOBuilder, FileRead};
    use crate::spec::{BlobType, IntType};
    use arrow_array::{ArrayRef, Int32Array, LargeBinaryArray};

    #[tokio::test]
    async fn failed_blob_close_respects_each_physical_writers_abort_policy() {
        for (external, rolled, normal, has_consumer) in [
            (false, false, true),
            (true, false, true),
            (false, true, true),
            (true, true, true),
            (false, false, false),
            (true, false, false),
            (false, true, false),
            (true, true, false),
        ]
        .into_iter()
        .flat_map(|(external, rolled, normal)| {
            [false, true].map(|consumer| (external, rolled, normal, consumer))
        }) {
            let io = FileIOBuilder::new("memory").build().unwrap();
            let batch = RecordBatch::try_from_iter([
                ("id", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
                (
                    "a",
                    Arc::new(LargeBinaryArray::from(vec![Some(b"a".as_slice())])) as ArrayRef,
                ),
                (
                    "b",
                    Arc::new(LargeBinaryArray::from(vec![Some(b"b".as_slice())])) as ArrayRef,
                ),
            ])
            .unwrap();
            let fields = vec![
                DataField::new(0, "id".into(), DataType::Int(IntType::new())),
                DataField::new(1, "a".into(), DataType::Blob(BlobType::new())),
                DataField::new(2, "b".into(), DataType::Blob(BlobType::new())),
            ];
            let write_fields = if normal {
                fields.clone()
            } else {
                fields[1..].to_vec()
            };
            let batch = if normal {
                batch
            } else {
                batch.project(&[1, 2]).unwrap()
            };
            let mut options = if external {
                HashMap::from([
                    (
                        "data-file.external-paths".into(),
                        "memory:/first,memory:/second".into(),
                    ),
                    (
                        "data-file.external-paths.strategy".into(),
                        "entropy-inject".into(),
                    ),
                ])
            } else {
                HashMap::new()
            };
            options.extend([
                ("file-index.bloom-filter.columns".into(), "id".into()),
                ("file-index.bloom-filter.id.items".into(), "10".into()),
                ("file-index.in-manifest-threshold".into(), "0 B".into()),
            ]);
            let mut writer = AppendDedicatedFormatFileWriter::new(
                io.clone(),
                "memory:/table".into(),
                "".into(),
                0,
                0,
                i64::MAX,
                if rolled { 1 } else { i64::MAX },
                i64::MAX,
                "none".into(),
                0,
                i64::MAX,
                "parquet".into(),
                i64::MAX,
                None,
                batch.schema().as_ref(),
                &write_fields,
                &fields,
                &options,
                &HashSet::new(),
            )
            .unwrap();
            let descriptors = Arc::new(std::sync::Mutex::new(Vec::new()));
            if has_consumer {
                let received = descriptors.clone();
                writer = writer
                    .with_blob_writer_options(
                        Some(Arc::new(
                            move |_: &str, descriptor: Option<&crate::spec::BlobDescriptor>| {
                                received.lock().unwrap().push(descriptor.unwrap().clone());
                                Ok(false)
                            },
                        )),
                        None,
                    )
                    .unwrap();
            }
            writer.write(&batch).await.unwrap();
            writer.blob_writers[1].writer.inject_close_failure();
            let error = if rolled {
                assert!(!writer.written_files.is_empty());
                // A later group failure must also remove earlier successful
                // groups, which have not been handed to a committer yet.
                writer.write(&batch).await.unwrap_err()
            } else {
                writer.prepare_commit().await.unwrap_err()
            };
            assert!(error.to_string().contains("injected close failure"));
            let remaining = io.list_status_recursive("memory:/").await.unwrap();
            if has_consumer {
                assert!(!remaining.is_empty());
                assert!(remaining
                    .iter()
                    .all(|status| status.path.ends_with(".blob")));
                let descriptors = descriptors.lock().unwrap().clone();
                for descriptor in descriptors {
                    let reader = io
                        .new_input(descriptor.uri())
                        .unwrap()
                        .reader()
                        .await
                        .unwrap();
                    let start = descriptor.offset() as u64;
                    assert_eq!(
                        reader
                            .read(start..start + descriptor.length() as u64)
                            .await
                            .unwrap()
                            .len(),
                        1
                    );
                }
            } else {
                assert!(remaining.is_empty());
            }
        }
    }
}
