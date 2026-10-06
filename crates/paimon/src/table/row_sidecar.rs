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

//! Aligned ROW files for normal Data Evolution data files, matching Java's
//! RowSidecarAuxiliaryWriter and DataEvolutionSplitRead.readTarget.

use super::blob_resolver::{resolve_descriptor_column, BlobReadLimiter};
use crate::arrow::format::{create_format_writer_factory, with_write_resources, FormatFileWriter};
use crate::io::FileIO;
use crate::resource::ResourceContext;
use crate::spec::{CoreOptions, DataField, DataFileMeta};
use crate::table::RowRange;
use crate::Result;
use arrow_array::{Array, LargeBinaryArray, RecordBatch};
use arrow_schema::{Field, Schema, SchemaRef};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub(super) struct RowSidecarWriter {
    pub file_name: String,
    pub writer: Box<dyn FormatFileWriter>,
    io: FileIO,
    descriptor_fields: HashSet<String>,
    view_fields: HashSet<String>,
}

impl RowSidecarWriter {
    pub async fn new(
        io: &FileIO,
        path: &str,
        file_name: &str,
        schema: SchemaRef,
        fields: &[DataField],
        options: &HashMap<String, String>,
        resources: Option<&ResourceContext>,
    ) -> Result<Self> {
        // ROW stores logical values. Parquet's MAP / VARIANT storage layouts
        // must not change the auxiliary file's schema or encoding.
        let options = CoreOptions::new(options);
        let factory = create_format_writer_factory(
            "row",
            schema,
            options.file_compression_zstd_level(),
            None,
            Some(fields),
            None,
            None,
        )?;
        let writer = factory.create_writer(&io.new_output(path)?, "zstd").await?;
        Ok(Self {
            file_name: format!("{file_name}.row"),
            writer: with_write_resources(writer, resources),
            io: io.clone(),
            descriptor_fields: options.blob_descriptor_fields(),
            view_fields: options.blob_view_fields(),
        })
    }

    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let mut descriptors = Vec::new();
        for (index, field) in batch.schema().fields().iter().enumerate() {
            if self.descriptor_fields.contains(field.name()) {
                descriptors.push(index);
            }
            if self.view_fields.contains(field.name()) {
                let values = blob_column(batch, index)?;
                // Java BlobView.toData() rejects an unresolved view. The Arrow
                // API supplies serialized references, without a resolved Blob.
                if values.null_count() != values.len() {
                    return Err(crate::Error::DataInvalid {
                        message:
                            "ROW sidecars require resolved BLOB views; BlobView is not resolved"
                                .into(),
                        source: None,
                    });
                }
            }
        }
        if descriptors.is_empty() {
            return self.writer.write(batch).await;
        }
        let limiter = BlobReadLimiter::new();
        // Like Java Blob.toData(), materialize one row at a time. A batch of
        // small references must not retain all referenced payloads in memory.
        for row in 0..batch.num_rows() {
            let indices = arrow_array::UInt64Array::from(vec![row as u64]);
            let data = arrow_select::take::take_record_batch(batch, &indices).map_err(|error| {
                crate::Error::DataInvalid {
                    message: format!("Failed to select ROW sidecar BLOB row: {error}"),
                    source: Some(Box::new(error)),
                }
            })?;
            let mut columns = data.columns().to_vec();
            for &index in &descriptors {
                // ROW stores the actual payload; Parquet keeps the reference.
                columns[index] = Arc::new(
                    resolve_descriptor_column(
                        blob_column(&data, index)?,
                        &self.io,
                        limiter.clone(),
                    )
                    .await?,
                );
            }
            let data = RecordBatch::try_new(data.schema(), columns).map_err(|error| {
                crate::Error::DataInvalid {
                    message: format!("Failed to build ROW sidecar BLOB row: {error}"),
                    source: Some(Box::new(error)),
                }
            })?;
            self.writer.write(&data).await?;
        }
        Ok(())
    }
}

fn blob_column(batch: &RecordBatch, index: usize) -> Result<&LargeBinaryArray> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .ok_or_else(|| crate::Error::DataInvalid {
            message: format!(
                "ROW sidecar BLOB field '{}' requires LargeBinary",
                batch.schema().field(index).name()
            ),
            source: None,
        })
}

// Arrow represents both BlobData and BlobRef as LargeBinary. Preserve the
// distinction only inside the Data Evolution pipeline; project_output removes
// this marker before exposing batches. Raw bytes can themselves look exactly
// like a descriptor, so magic-number detection cannot supply the distinction.
const BLOB_DATA: &str = "paimon.row-sidecar.blob-data";

pub(super) fn is_blob_data(field: &Field) -> bool {
    field.metadata().contains_key(BLOB_DATA)
}

pub(super) fn mark_blob_data(schema: &SchemaRef, names: &HashSet<String>) -> SchemaRef {
    if names.is_empty() {
        return schema.clone();
    }
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| {
            if !names.contains(field.name()) {
                return field.clone();
            }
            let mut metadata = field.metadata().clone();
            metadata.insert(BLOB_DATA.into(), "true".into());
            Arc::new(field.as_ref().clone().with_metadata(metadata))
        })
        .collect();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

pub(super) fn propagate_blob_data(schema: &SchemaRef, batch: &RecordBatch) -> SchemaRef {
    let names = batch
        .schema()
        .fields()
        .iter()
        .filter(|field| is_blob_data(field))
        .map(|field| field.name().clone())
        .collect();
    mark_blob_data(schema, &names)
}

/// Exactly one ROW extra file is required, as in Java. Do not guess between
/// multiple sidecars or enable one merely because deletion vectors prune rows.
fn selected_sidecar<'a>(
    file: &'a DataFileMeta,
    local_ranges: Option<&[RowRange]>,
    options: &CoreOptions<'_>,
) -> Result<Option<&'a str>> {
    if !options.data_evolution_enabled() {
        return Ok(None);
    }
    let mut names = file
        .extra_files
        .iter()
        .filter(|name| name.ends_with(".row"));
    let Some(name) = names.next() else {
        return Ok(None);
    };
    if names.next().is_some() {
        return Ok(None);
    }
    let max_rows = options.data_evolution_row_sidecar_max_selected_rows()?;
    let max_ratio = options.data_evolution_row_sidecar_max_selection_ratio()?;
    let Some(ranges) = local_ranges else {
        return Ok(None);
    };
    if file.row_count <= 0
        || file.file_name.ends_with(".blob")
        || file.file_name.contains(".vector.")
    {
        return Ok(None);
    }
    // The caller clips, sorts and merges the requested ranges into local
    // positions before intersecting them with indexes and deletion vectors.
    let selected: i64 = ranges.iter().map(RowRange::count).sum();
    Ok((selected > 0
        && selected < file.row_count
        && selected <= max_rows
        && selected as f64 / file.row_count as f64 <= max_ratio)
        .then_some(name.as_str()))
}

pub(super) fn supports_read_type(read_type: &[DataField], blob_as_descriptor: bool) -> bool {
    // BlobData in ROW has no URI. A caller requesting descriptors must keep
    // the primary file, rather than receive payload bytes in place of refs.
    !blob_as_descriptor
        || !read_type
            .iter()
            .any(|field| matches!(field.data_type(), crate::spec::DataType::Blob(_)))
}

pub(super) async fn read_target(
    io: &FileIO,
    file: &DataFileMeta,
    bucket_path: &str,
    local_ranges: Option<&[RowRange]>,
    options: &CoreOptions<'_>,
) -> Result<(String, u64)> {
    if let Some(name) = selected_sidecar(file, local_ranges, options)? {
        let path = file.aligned_file_path(bucket_path, name);
        match io.get_status(&path).await {
            Ok(status) => return Ok((path, status.size)),
            Err(error) => {
                if !options.scan_ignore_lost_file() {
                    return Err(error);
                }
            }
        }
    }
    Ok((file.data_file_path(bucket_path), file.file_size as u64))
}
