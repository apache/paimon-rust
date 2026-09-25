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

//! Externalize primary-key BLOB values before they enter the merge buffer.
//!
//! Java's `PrimaryKeyBlobExternalizer` writes scalar, array-element, and map-
//! value BLOBs into per-field `.managed.blob` packs. The buffered row carries
//! descriptors, so a merge or compaction never copies large payloads into
//! Parquet. A separate `.blobref` sidecar records the packs retained by each
//! physical data file.

use crate::arrow::format::blob::BlobFormatWriter;
use crate::arrow::format::FormatFileWriter;
use crate::io::FileIO;
use crate::spec::{bucket_path_under, BlobDescriptor, CoreOptions, DataField, DataType, RowKind};
use crate::Result;
use arrow_array::builder::LargeBinaryBuilder;
use arrow_array::{
    Array, ArrayRef, Int8Array, LargeBinaryArray, ListArray, MapArray, RecordBatch, StructArray,
};
use arrow_buffer::NullBuffer;
use arrow_schema::DataType as ArrowDataType;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ManagedBlobKind {
    Scalar,
    Array,
    Map,
}

pub(crate) fn managed_blob_kind(data_type: &DataType) -> Option<ManagedBlobKind> {
    match data_type {
        DataType::Blob(_) => Some(ManagedBlobKind::Scalar),
        DataType::Array(array) if matches!(array.element_type(), DataType::Blob(_)) => {
            Some(ManagedBlobKind::Array)
        }
        DataType::Map(map) if matches!(map.value_type(), DataType::Blob(_)) => {
            Some(ManagedBlobKind::Map)
        }
        _ => None,
    }
}

pub(crate) fn managed_blob_fields(
    fields: &[DataField],
    options: &CoreOptions<'_>,
) -> Vec<(usize, ManagedBlobKind)> {
    let inline = options.blob_inline_fields();
    fields
        .iter()
        .enumerate()
        .filter(|(_, field)| !inline.contains(field.name()))
        .filter_map(|(index, field)| managed_blob_kind(field.data_type()).map(|kind| (index, kind)))
        .collect()
}

struct ManagedBlobField {
    index: usize,
    kind: ManagedBlobKind,
    current: Option<ManagedBlobPack>,
}

struct ManagedBlobPack {
    path: String,
    writer: Box<BlobFormatWriter>,
}

pub(crate) struct ManagedBlobWriter {
    file_io: FileIO,
    bucket_dir: String,
    file_prefix: String,
    target_file_size: u64,
    fields: Vec<ManagedBlobField>,
    uncommitted_paths: Vec<String>,
}

impl ManagedBlobWriter {
    pub(crate) fn new(
        file_io: FileIO,
        table_location: &str,
        partition_path: &str,
        bucket: i32,
        file_prefix: &str,
        target_file_size: i64,
        fields: Vec<(usize, ManagedBlobKind)>,
    ) -> Result<Option<Self>> {
        if fields.is_empty() {
            return Ok(None);
        }
        let target_file_size = u64::try_from(target_file_size)
            .ok()
            .filter(|size| *size > 0)
            .ok_or_else(|| crate::Error::DataInvalid {
                message: "Managed BLOB target file size must be positive".to_string(),
                source: None,
            })?;
        Ok(Some(Self {
            file_io,
            bucket_dir: bucket_path_under(table_location, partition_path, bucket),
            file_prefix: file_prefix.to_string(),
            target_file_size,
            fields: fields
                .into_iter()
                .map(|(index, kind)| ManagedBlobField {
                    index,
                    kind,
                    current: None,
                })
                .collect(),
            uncommitted_paths: Vec::new(),
        }))
    }

    pub(crate) async fn externalize(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        let kind_index = batch
            .schema()
            .fields()
            .iter()
            .position(|field| field.name() == crate::spec::VALUE_KIND_FIELD_NAME);
        let kinds = kind_index
            .map(|index| {
                batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .ok_or_else(|| crate::Error::DataInvalid {
                        message: "_VALUE_KIND column must be Int8".to_string(),
                        source: None,
                    })
            })
            .transpose()?;
        let mut retract = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let kind = kinds
                .filter(|column| column.is_valid(row))
                .map_or(RowKind::Insert.to_value(), |column| column.value(row));
            retract.push(RowKind::from_value(kind)?.is_retract());
        }

        let mut columns = batch.columns().to_vec();
        for field_index in 0..self.fields.len() {
            let column_index = self.fields[field_index].index;
            let column = match self.fields[field_index].kind {
                ManagedBlobKind::Scalar => {
                    let values = downcast_blob_column(columns[column_index].as_ref())?;
                    Arc::new(
                        self.externalize_values(field_index, values, &retract)
                            .await?,
                    ) as ArrayRef
                }
                ManagedBlobKind::Array => {
                    let array = columns[column_index]
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .ok_or_else(|| invalid_blob_column("ARRAY<BLOB> requires ListArray"))?;
                    let values = downcast_blob_column(array.values().as_ref())?;
                    let child_retract = child_retract_mask(array.value_offsets(), array, &retract)?;
                    let values = Arc::new(
                        self.externalize_values(field_index, values, &child_retract)
                            .await?,
                    );
                    let ArrowDataType::List(element) = array.data_type() else {
                        unreachable!()
                    };
                    Arc::new(
                        ListArray::try_new(
                            element.clone(),
                            array.offsets().clone(),
                            values,
                            combined_nulls(array, &retract),
                        )
                        .map_err(|error| invalid_blob_column(&error.to_string()))?,
                    )
                }
                ManagedBlobKind::Map => {
                    let map = columns[column_index]
                        .as_any()
                        .downcast_ref::<MapArray>()
                        .ok_or_else(|| invalid_blob_column("MAP<X, BLOB> requires MapArray"))?;
                    let values = downcast_blob_column(map.entries().column(1).as_ref())?;
                    let child_retract = child_retract_mask(map.value_offsets(), map, &retract)?;
                    let values = Arc::new(
                        self.externalize_values(field_index, values, &child_retract)
                            .await?,
                    );
                    let ArrowDataType::Map(entries_field, ordered) = map.data_type() else {
                        unreachable!()
                    };
                    let ArrowDataType::Struct(entry_fields) = entries_field.data_type() else {
                        unreachable!()
                    };
                    let entries = StructArray::try_new(
                        entry_fields.clone(),
                        vec![map.entries().column(0).clone(), values],
                        None,
                    )
                    .map_err(|error| invalid_blob_column(&error.to_string()))?;
                    Arc::new(
                        MapArray::try_new(
                            entries_field.clone(),
                            map.offsets().clone(),
                            entries,
                            combined_nulls(map, &retract),
                            *ordered,
                        )
                        .map_err(|error| invalid_blob_column(&error.to_string()))?,
                    )
                }
            };
            columns[column_index] = column;
        }
        RecordBatch::try_new(batch.schema(), columns)
            .map_err(|error| invalid_blob_column(&error.to_string()))
    }

    async fn externalize_values(
        &mut self,
        field_index: usize,
        values: &LargeBinaryArray,
        retract: &[bool],
    ) -> Result<LargeBinaryArray> {
        if values.len() != retract.len() {
            return Err(invalid_blob_column(
                "Managed BLOB retract mask length mismatch",
            ));
        }
        let mut builder = LargeBinaryBuilder::new();
        for (index, is_retract) in retract.iter().enumerate() {
            if *is_retract || values.is_null(index) {
                builder.append_null();
                continue;
            }
            let descriptor = self.write_value(field_index, values.value(index)).await?;
            builder.append_value(descriptor.serialize());
        }
        Ok(builder.finish())
    }

    async fn write_value(&mut self, field_index: usize, value: &[u8]) -> Result<BlobDescriptor> {
        if self.fields[field_index].current.is_none() {
            self.file_io
                .mkdirs(&format!("{}/", self.bucket_dir))
                .await?;
            let path = format!(
                "{}/{}{}.managed.blob",
                self.bucket_dir,
                self.file_prefix,
                uuid::Uuid::new_v4()
            );
            let output = self.file_io.new_output(&path)?;
            let writer =
                Box::new(BlobFormatWriter::new(&output, Some(self.file_io.clone())).await?);
            self.uncommitted_paths.push(path.clone());
            self.fields[field_index].current = Some(ManagedBlobPack { path, writer });
        }
        let pack = self.fields[field_index].current.as_mut().unwrap();
        let (offset, length) = pack.writer.write_managed_value(value).await?;
        let descriptor = BlobDescriptor::new(pack.path.clone(), offset, length);
        if pack.writer.num_bytes() as u64 >= self.target_file_size {
            self.close_pack(field_index).await?;
        }
        Ok(descriptor)
    }

    async fn close_pack(&mut self, field_index: usize) -> Result<()> {
        if let Some(pack) = self.fields[field_index].current.take() {
            pack.writer.close().await?;
        }
        Ok(())
    }

    pub(crate) async fn prepare_commit(&mut self) -> Result<()> {
        for index in 0..self.fields.len() {
            self.close_pack(index).await?;
        }
        self.uncommitted_paths.clear();
        Ok(())
    }

    pub(crate) async fn abort(&mut self) {
        for field in &mut self.fields {
            field.current.take();
        }
        for path in self.uncommitted_paths.drain(..) {
            let _ = self.file_io.delete_file(&path).await;
        }
    }
}

fn downcast_blob_column(array: &dyn Array) -> Result<&LargeBinaryArray> {
    array
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .ok_or_else(|| invalid_blob_column("BLOB values require LargeBinaryArray"))
}

fn invalid_blob_column(message: &str) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.to_string(),
        source: None,
    }
}

fn child_retract_mask(offsets: &[i32], array: &dyn Array, retract: &[bool]) -> Result<Vec<bool>> {
    let length = usize::try_from(*offsets.last().unwrap_or(&0))
        .map_err(|_| invalid_blob_column("Negative BLOB child offset"))?;
    let mut mask = vec![false; length];
    for (row, is_retract) in retract.iter().enumerate() {
        if *is_retract || array.is_null(row) {
            let start = usize::try_from(offsets[row])
                .map_err(|_| invalid_blob_column("Negative BLOB child offset"))?;
            let end = usize::try_from(offsets[row + 1])
                .map_err(|_| invalid_blob_column("Negative BLOB child offset"))?;
            mask[start..end].fill(true);
        }
    }
    Ok(mask)
}

fn combined_nulls(array: &dyn Array, retract: &[bool]) -> Option<NullBuffer> {
    let valid = (0..array.len())
        .map(|index| array.is_valid(index) && !retract[index])
        .collect::<Vec<_>>();
    valid
        .iter()
        .any(|valid| !valid)
        .then(|| NullBuffer::from(valid))
}
