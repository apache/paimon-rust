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

//! Java-compatible `.blobref` sidecars for managed primary-key BLOB packs.

use super::managed_blob_writer::ManagedBlobKind;
use crate::io::FileIO;
use crate::spec::{BlobDescriptor, RowKind, VALUE_KIND_FIELD_NAME};
use crate::Result;
use arrow_array::{Array, Int8Array, LargeBinaryArray, ListArray, MapArray, RecordBatch};
use bytes::Bytes;
use std::collections::BTreeSet;

const MAGIC: i32 = 0x50424C52;
const VERSION: u8 = 1;
pub(crate) const REFERENCE_FILE_SUFFIX: &str = ".blobref";
const MANAGED_BLOB_SUFFIX: &str = ".managed.blob";

/// One reference is a storage root and a file name, matching Java
/// `ManagedBlobReferenceFile.Reference`.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct ManagedBlobReference {
    pub storage_root: String,
    pub file_name: String,
}

#[derive(Default)]
pub(crate) struct ManagedBlobReferenceCollector {
    references: BTreeSet<ManagedBlobReference>,
}

impl ManagedBlobReferenceCollector {
    pub(crate) fn collect_batch(
        &mut self,
        batch: &RecordBatch,
        fields: &[(usize, ManagedBlobKind)],
    ) -> Result<()> {
        let kind_index = batch
            .schema()
            .fields()
            .iter()
            .position(|field| field.name() == VALUE_KIND_FIELD_NAME);
        let kinds = kind_index
            .map(|index| {
                batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .ok_or_else(|| invalid("_VALUE_KIND column must be Int8"))
            })
            .transpose()?;
        for row in 0..batch.num_rows() {
            let kind = kinds
                .filter(|array| array.is_valid(row))
                .map_or(RowKind::Insert.to_value(), |array| array.value(row));
            if RowKind::from_value(kind)?.is_retract() {
                continue;
            }
            for &(index, field_kind) in fields {
                let column = batch.column(index);
                if column.is_null(row) {
                    continue;
                }
                match field_kind {
                    ManagedBlobKind::Scalar => {
                        let values = binary_column(column.as_ref())?;
                        self.collect_value(values.value(row))?;
                    }
                    ManagedBlobKind::Array => {
                        let array = column
                            .as_any()
                            .downcast_ref::<ListArray>()
                            .ok_or_else(|| invalid("Managed ARRAY<BLOB> requires ListArray"))?;
                        let values = binary_column(array.values().as_ref())?;
                        for value in array.value_offsets()[row]..array.value_offsets()[row + 1] {
                            let value = value as usize;
                            if values.is_valid(value) {
                                self.collect_value(values.value(value))?;
                            }
                        }
                    }
                    ManagedBlobKind::Map => {
                        let map = column
                            .as_any()
                            .downcast_ref::<MapArray>()
                            .ok_or_else(|| invalid("Managed MAP<X, BLOB> requires MapArray"))?;
                        let values = binary_column(map.entries().column(1).as_ref())?;
                        for value in map.value_offsets()[row]..map.value_offsets()[row + 1] {
                            let value = value as usize;
                            if values.is_valid(value) {
                                self.collect_value(values.value(value))?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn collect_value(&mut self, value: &[u8]) -> Result<()> {
        if !BlobDescriptor::is_blob_descriptor(value) {
            return Ok(());
        }
        let descriptor = BlobDescriptor::deserialize(value)?;
        let uri = descriptor.uri();
        if !uri.ends_with(MANAGED_BLOB_SUFFIX) {
            return Ok(());
        }
        let slash = uri
            .rfind('/')
            .ok_or_else(|| invalid("Managed BLOB descriptor URI has no parent directory"))?;
        let file_name = &uri[slash + 1..];
        if file_name.is_empty() || file_name == "." || file_name == ".." {
            return Err(invalid("Managed BLOB descriptor has an invalid file name"));
        }
        self.references.insert(ManagedBlobReference {
            storage_root: uri[..slash].to_string(),
            file_name: file_name.to_string(),
        });
        Ok(())
    }

    pub(crate) async fn write(&self, file_io: &FileIO, data_path: &str) -> Result<String> {
        let name = data_path
            .rsplit('/')
            .next()
            .ok_or_else(|| invalid("Managed BLOB data file has no file name"))?;
        let sidecar_name = format!("{name}{REFERENCE_FILE_SUFFIX}");
        let path = format!("{data_path}{REFERENCE_FILE_SUFFIX}");
        let bytes = self.serialize()?;
        let output = file_io.new_output(&path)?;
        if let Err(error) = output.write(Bytes::from(bytes)).await {
            let _ = file_io.delete_file(&path).await;
            return Err(error);
        }
        Ok(sidecar_name)
    }

    pub(crate) fn serialize(&self) -> Result<Vec<u8>> {
        let count = i32::try_from(self.references.len())
            .map_err(|_| invalid("Managed BLOB reference count exceeds i32"))?;
        let mut payload = Vec::new();
        payload.push(VERSION);
        payload.extend_from_slice(&count.to_be_bytes());
        for reference in &self.references {
            write_java_utf(&reference.storage_root, &mut payload)?;
            write_java_utf(&reference.file_name, &mut payload)?;
        }
        let checksum = crc32fast::hash(&payload);
        let mut bytes = Vec::with_capacity(4 + payload.len() + 4);
        bytes.extend_from_slice(&MAGIC.to_be_bytes());
        bytes.extend_from_slice(&payload);
        bytes.extend_from_slice(&checksum.to_be_bytes());
        Ok(bytes)
    }

    #[cfg(test)]
    pub(crate) fn references_for_test(
        &mut self,
        references: impl IntoIterator<Item = ManagedBlobReference>,
    ) {
        self.references.extend(references);
    }
}

fn binary_column(array: &dyn Array) -> Result<&LargeBinaryArray> {
    array
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .ok_or_else(|| invalid("Managed BLOB descriptor column requires LargeBinaryArray"))
}

fn invalid(message: &str) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.to_string(),
        source: None,
    }
}

/// `DataOutputStream.writeUTF` uses modified UTF-8 over UTF-16 code units.
/// A plain Rust UTF-8 string differs for NUL and supplementary characters.
fn write_java_utf(value: &str, output: &mut Vec<u8>) -> Result<()> {
    let mut encoded = Vec::new();
    for unit in value.encode_utf16() {
        if (1..=0x7f).contains(&unit) {
            encoded.push(unit as u8);
        } else if unit <= 0x7ff {
            encoded.push(0xc0 | (unit >> 6) as u8);
            encoded.push(0x80 | (unit & 0x3f) as u8);
        } else {
            encoded.push(0xe0 | (unit >> 12) as u8);
            encoded.push(0x80 | ((unit >> 6) & 0x3f) as u8);
            encoded.push(0x80 | (unit & 0x3f) as u8);
        }
    }
    let length = u16::try_from(encoded.len())
        .map_err(|_| invalid("Managed BLOB reference path exceeds Java UTF length"))?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(&encoded);
    Ok(())
}
