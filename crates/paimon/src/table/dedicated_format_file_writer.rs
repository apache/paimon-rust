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
use crate::spec::{BlobDescriptor, DataField, DataFileMeta, DataType};
use crate::table::data_file_writer::DataFileWriter;
use crate::Result;
use arrow_array::builder::BinaryBuilder;
use arrow_array::{Array, RecordBatch};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub(crate) fn is_blob_file_name(file_name: &str) -> bool {
    file_name.to_ascii_lowercase().ends_with(".blob")
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
    normal_writer: DataFileWriter,
    blob_writers: Vec<BlobFieldWriter>,
    vector_writer: Option<VectorFieldWriter>,
    normal_column_indices: Vec<usize>,
    normal_schema: Arc<arrow_schema::Schema>,
}

impl AppendDedicatedFormatFileWriter {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        file_io: FileIO,
        table_location: String,
        partition_path: String,
        bucket: i32,
        schema_id: i64,
        target_file_size: i64,
        blob_target_file_size: i64,
        file_compression: String,
        file_compression_zstd_level: i32,
        write_buffer_size: i64,
        file_format: String,
        vector_target_file_size: i64,
        vector_file_format: Option<&str>,
        input_schema: &arrow_schema::Schema,
        table_fields: &[DataField],
        format_options: &HashMap<String, String>,
        blob_descriptor_fields: &HashSet<String>,
    ) -> Self {
        let mut normal_column_indices = Vec::new();
        let mut normal_arrow_fields = Vec::new();
        let mut normal_table_fields = Vec::new();
        let mut blob_writers = Vec::new();
        let mut vector_column_indices = Vec::new();
        let mut vector_arrow_fields = Vec::new();
        let mut vector_table_fields = Vec::new();
        let mut vector_field_names = Vec::new();

        for (idx, field) in table_fields.iter().enumerate() {
            let is_blob = field.data_type().is_blob_type();
            let is_descriptor = blob_descriptor_fields.contains(field.name());
            let is_dedicated_vector =
                vector_file_format.is_some() && matches!(field.data_type(), DataType::Vector(_));

            if is_dedicated_vector {
                vector_column_indices.push(idx);
                vector_arrow_fields.push(input_schema.field(idx).clone());
                vector_table_fields.push(field.clone());
                vector_field_names.push(field.name().to_string());
            } else if is_blob && !is_descriptor {
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
                        HashMap::new(),
                        Some(0),
                        None,
                        Some(vec![field.name().to_string()]),
                    ),
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
                    ),
                    field_names: vector_field_names,
                    column_indices: vector_column_indices,
                    schema: vector_schema,
                })
            }
        } else {
            None
        };

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
            None,
        );

        Self {
            normal_writer,
            blob_writers,
            vector_writer,
            normal_column_indices,
            normal_schema,
        }
    }

    pub(crate) async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        // Write normal columns
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
        self.normal_writer.write(&normal_batch).await?;

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
            blob_writer.writer.write(&blob_batch).await?;
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

        Ok(())
    }

    pub(crate) async fn prepare_commit(&mut self) -> Result<Vec<DataFileMeta>> {
        let mut results = self.normal_writer.prepare_commit().await?;

        for blob_writer in &mut self.blob_writers {
            let blob_metas = blob_writer.writer.prepare_commit().await?;
            results.extend(blob_metas);
        }

        if let Some(vector_writer) = &mut self.vector_writer {
            let vector_metas = vector_writer.writer.prepare_commit().await?;
            results.extend(vector_metas);
        }

        Ok(results)
    }
}

/// For each row in a blob column, if the value is a serialized `BlobDescriptor`,
/// resolve it by reading the actual data from the referenced URI+offset+length.
/// Raw data values are passed through unchanged.
pub(crate) async fn resolve_blob_column(
    col: &arrow_array::BinaryArray,
    file_io: &FileIO,
) -> Result<arrow_array::BinaryArray> {
    use crate::io::FileRead;
    use std::collections::HashMap;

    let mut needs_resolve = false;
    for i in 0..col.len() {
        if !col.is_null(i) && BlobDescriptor::is_blob_descriptor(col.value(i)) {
            needs_resolve = true;
            break;
        }
    }

    if !needs_resolve {
        return Ok(col.clone());
    }

    let mut readers: HashMap<String, Box<dyn FileRead>> = HashMap::new();
    let mut builder = BinaryBuilder::with_capacity(col.len(), 0);
    for i in 0..col.len() {
        if col.is_null(i) {
            builder.append_null();
        } else {
            let value = col.value(i);
            if BlobDescriptor::is_blob_descriptor(value) {
                let desc = BlobDescriptor::deserialize(value)?;
                let uri = desc.uri().to_string();
                if !readers.contains_key(&uri) {
                    let input = file_io.new_input(&uri)?;
                    let reader = input.reader().await?;
                    readers.insert(uri.clone(), Box::new(reader));
                }
                let reader = readers.get(&uri).unwrap();
                let start = desc.offset() as u64;
                let end = start + desc.length() as u64;
                let data = reader.read(start..end).await?;
                builder.append_value(&data);
            } else {
                builder.append_value(value);
            }
        }
    }
    Ok(builder.finish())
}
