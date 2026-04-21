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
use crate::spec::{BlobDescriptor, DataFileMeta};
use crate::table::data_file_writer::DataFileWriter;
use crate::Result;
use arrow_array::builder::BinaryBuilder;
use arrow_array::{Array, RecordBatch};
use std::collections::HashSet;
use std::sync::Arc;

pub(crate) fn is_blob_file_name(file_name: &str) -> bool {
    file_name.to_ascii_lowercase().ends_with(".blob")
}

struct BlobFieldWriter {
    writer: DataFileWriter,
    field_name: String,
    column_index: usize,
}

/// Writes append-only data with blob columns split into separate `.blob` files.
///
/// Normal (non-blob) columns go to a parquet `DataFileWriter`.
/// Each blob column (not in `blob_descriptor_fields`) gets its own `DataFileWriter`
/// with `file_format = "blob"` and `write_cols = Some(vec![field_name])`.
///
/// If a blob value is already a serialized `BlobDescriptor`, the actual data is
/// resolved from the referenced URI and written to the `.blob` file.
pub(crate) struct AppendBlobFileWriter {
    normal_writer: DataFileWriter,
    blob_writers: Vec<BlobFieldWriter>,
    normal_column_indices: Vec<usize>,
    normal_schema: Arc<arrow_schema::Schema>,
}

impl AppendBlobFileWriter {
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
        input_schema: &arrow_schema::Schema,
        table_fields: &[crate::spec::DataField],
        blob_descriptor_fields: &HashSet<String>,
    ) -> Self {
        let mut normal_column_indices = Vec::new();
        let mut normal_arrow_fields = Vec::new();
        let mut blob_writers = Vec::new();

        for (idx, field) in table_fields.iter().enumerate() {
            let is_blob = field.data_type().is_blob_type();
            let is_descriptor = blob_descriptor_fields.contains(field.name());

            if is_blob && !is_descriptor {
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
            }
        }

        let normal_schema = Arc::new(arrow_schema::Schema::new(normal_arrow_fields));

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
            Some(0),
            None,
            None,
        );

        Self {
            normal_writer,
            blob_writers,
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

        Ok(())
    }

    pub(crate) async fn prepare_commit(&mut self) -> Result<Vec<DataFileMeta>> {
        let mut results = self.normal_writer.prepare_commit().await?;

        for blob_writer in &mut self.blob_writers {
            let blob_metas = blob_writer.writer.prepare_commit().await?;
            results.extend(blob_metas);
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
