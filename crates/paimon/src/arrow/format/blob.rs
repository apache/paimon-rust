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

use super::metadata_cache::FileMetadataCache;
use super::{FilePredicates, FormatFileReader, FormatFileWriter, FormatWriteResult};
use crate::arrow::build_target_arrow_schema;
use crate::io::{BlobIndexCacheContext, FileRead, FileWrite};
use crate::spec::{BlobDescriptor, DataField, DataType};
use crate::table::{ArrowRecordBatchStream, RowRange};
use crate::Error;
use arrow_array::builder::{LargeBinaryBuilder, ListBuilder};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Int16Array,
    Int32Array, Int64Array, Int8Array, LargeBinaryArray, MapArray, RecordBatch, RecordBatchOptions,
    StringArray, StructArray, Time32MillisecondArray,
};
use arrow_buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::DataType as ArrowDataType;
use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use std::ops::Range;
use std::sync::Arc;

pub(crate) struct BlobFormatReader {
    descriptor_mode: bool,
    file_path: String,
    blob_parallelism: usize,
}

impl BlobFormatReader {
    pub(crate) fn new(file_path: String, descriptor_mode: bool) -> Self {
        Self {
            descriptor_mode,
            file_path,
            blob_parallelism: DEFAULT_BLOB_READ_PARALLELISM,
        }
    }

    pub(crate) fn with_blob_parallelism(mut self, blob_parallelism: usize) -> Self {
        debug_assert!(blob_parallelism > 0);
        self.blob_parallelism = blob_parallelism;
        self
    }
}

pub(crate) struct IndexedBlobReader {
    reader: Box<dyn FileRead>,
    index: Arc<BlobFileIndex>,
    descriptor_mode: bool,
    file_path: String,
    blob_parallelism: usize,
}

impl IndexedBlobReader {
    #[cfg(test)]
    pub(crate) async fn open(
        reader: Box<dyn FileRead>,
        file_size: u64,
        file_path: String,
        descriptor_mode: bool,
    ) -> crate::Result<Self> {
        Self::open_with_parallelism(
            reader,
            file_size,
            file_path,
            descriptor_mode,
            DEFAULT_BLOB_READ_PARALLELISM,
        )
        .await
    }

    pub(crate) async fn open_with_parallelism(
        reader: Box<dyn FileRead>,
        file_size: u64,
        file_path: String,
        descriptor_mode: bool,
        blob_parallelism: usize,
    ) -> crate::Result<Self> {
        debug_assert!(blob_parallelism > 0);
        let index = BlobFileIndex::load_cached(reader.as_ref(), file_size).await?;
        Ok(Self {
            reader,
            index,
            descriptor_mode,
            file_path,
            blob_parallelism,
        })
    }

    pub(crate) fn num_rows(&self) -> usize {
        self.index.num_rows()
    }

    pub(crate) async fn read_positions(
        &self,
        positions: &[usize],
    ) -> crate::Result<Vec<BlobReadValue>> {
        if self.descriptor_mode {
            build_descriptor_values(&self.index, positions, &self.file_path)
        } else {
            let planned_reads = plan_blob_reads(&self.index, positions)?;
            fetch_blob_values(self.reader.as_ref(), planned_reads, self.blob_parallelism).await
        }
    }

    pub(crate) async fn read_array_positions(
        &self,
        positions: &[usize],
    ) -> crate::Result<Vec<BlobReadValue>> {
        let planned_reads = plan_blob_array_reads(&self.index, positions)?;
        fetch_blob_array_values(
            self.reader.as_ref(),
            planned_reads,
            &self.file_path,
            self.descriptor_mode,
            self.blob_parallelism,
        )
        .await
    }

    pub(crate) async fn read_map_positions(
        &self,
        positions: &[usize],
        key_type: &DataType,
    ) -> crate::Result<Vec<BlobReadValue>> {
        let planned_reads = plan_blob_array_reads(&self.index, positions)?;
        fetch_blob_map_values(
            self.reader.as_ref(),
            planned_reads,
            &self.file_path,
            self.descriptor_mode,
            key_type,
            self.blob_parallelism,
        )
        .await
    }
}

#[derive(Debug)]
pub(crate) enum BlobReadValue {
    Value(Bytes),
    Array(Vec<Option<Bytes>>),
    Map(Vec<(Bytes, Option<Bytes>)>),
    Null,
    Placeholder,
}

const BLOB_FOOTER_SIZE: u64 = 5;
const BLOB_FORMAT_VERSION: u8 = 1;
const BLOB_INDEX_CACHE_CONTAINER_OVERHEAD: usize = std::mem::size_of::<String>()
    + std::mem::size_of::<Arc<BlobFileIndex>>()
    + 4 * std::mem::size_of::<usize>();
const BLOB_MAGIC_NUMBER: i32 = 1481511375;
const BLOB_MAGIC_NUMBER_BYTES: [u8; 4] = BLOB_MAGIC_NUMBER.to_le_bytes();
const BLOB_INLINE_HEADER_SIZE: u64 = 4;
const BLOB_TRAILER_SIZE: u64 = 12;
const BLOB_ENTRY_OVERHEAD: u64 = BLOB_INLINE_HEADER_SIZE + BLOB_TRAILER_SIZE;
const DEFAULT_BATCH_SIZE: usize = 128;
pub(crate) const DEFAULT_BLOB_READ_PARALLELISM: usize = 8;
const BLOB_RANGE_MERGE_GAP: u64 = 1024 * 1024;
const BLOB_RANGE_MERGE_MAX_SPAN: u64 = 8 * 1024 * 1024;
// Never fetch more than twice the unique selected entry bytes in a merged span.
const BLOB_RANGE_MERGE_MAX_AMPLIFICATION: u64 = 2;
const BLOB_ARRAY_MAGIC_NUMBER: i32 = 1094861634;
const BLOB_ARRAY_VERSION: u8 = 1;
const BLOB_ARRAY_HEADER_SIZE: u64 = 9;
const BLOB_ARRAY_INDEX_LENGTH_SIZE: u64 = 4;
const BLOB_ARRAY_MIN_PAYLOAD_SIZE: u64 = BLOB_ARRAY_HEADER_SIZE + BLOB_ARRAY_INDEX_LENGTH_SIZE;
const BLOB_ARRAY_NULL_ELEMENT_LENGTH: i64 = -1;
const BLOB_MAP_MAGIC_NUMBER: i32 = 0x4D424342;
const BLOB_MAP_VERSION: u8 = 1;
const BLOB_MAP_HEADER_SIZE: u64 = 9;
const BLOB_MAP_INDEX_LENGTHS_SIZE: u64 = 8;
const BLOB_MAP_MIN_PAYLOAD_SIZE: u64 = BLOB_MAP_HEADER_SIZE + BLOB_MAP_INDEX_LENGTHS_SIZE;
const BLOB_MAP_NULL_LENGTH: i64 = -1;

#[derive(Debug, Clone)]
pub(crate) enum BlobFieldKind {
    Scalar,
    Array,
    Map(DataType),
}

#[async_trait]
impl FormatFileReader for BlobFormatReader {
    async fn read_batch_stream(
        &self,
        reader: Box<dyn FileRead>,
        file_size: u64,
        read_fields: &[DataField],
        predicates: Option<&FilePredicates>,
        batch_size: Option<usize>,
        row_selection: Option<Vec<RowRange>>,
    ) -> crate::Result<ArrowRecordBatchStream> {
        // This reader evaluates no predicate at all, so nothing would enforce a
        // `_ROW_ID` one.
        if let Some(fp) = predicates {
            crate::table::row_id_predicate::reject_row_id_filter(&fp.predicates, "blob files")?;
        }
        let field_kind = validate_read_fields(read_fields)?;

        let target_schema = build_target_arrow_schema(read_fields)?;
        let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
        let blob_reader = IndexedBlobReader::open_with_parallelism(
            reader,
            file_size,
            self.file_path.clone(),
            self.descriptor_mode,
            self.blob_parallelism,
        )
        .await?;
        let mut selection = RowSelectionCursor::new(blob_reader.num_rows(), row_selection)?;

        Ok(try_stream! {
            while let Some(positions) = selection.next_batch(batch_size) {
                let batch = match &field_kind {
                    Some(BlobFieldKind::Scalar) => {
                        let values = blob_reader.read_positions(&positions).await?;
                        build_blob_batch(&target_schema, values)?
                    }
                    Some(BlobFieldKind::Array) => {
                        let values = blob_reader.read_array_positions(&positions).await?;
                        build_blob_array_batch(&target_schema, values)?
                    }
                    Some(BlobFieldKind::Map(key_type)) => {
                        let values = blob_reader.read_map_positions(&positions, key_type).await?;
                        build_blob_map_batch(&target_schema, values, key_type)?
                    }
                    None => RecordBatch::try_new_with_options(
                        target_schema.clone(),
                        Vec::new(),
                        &RecordBatchOptions::new().with_row_count(Some(positions.len())),
                    )
                    .map_err(|e| Error::UnexpectedError {
                        message: format!("Failed to build empty blob RecordBatch: {e}"),
                        source: Some(Box::new(e)),
                    })?,
                };
                yield batch;
            }
        }
        .boxed())
    }
}

fn validate_read_fields(read_fields: &[DataField]) -> crate::Result<Option<BlobFieldKind>> {
    if read_fields.len() > 1 {
        return Err(Error::DataInvalid {
            message: format!(
                ".blob format only supports reading at most one projected column, got {}",
                read_fields.len()
            ),
            source: None,
        });
    }

    read_fields
        .first()
        .map(|field| match field.data_type() {
            DataType::Blob(_) => Ok(BlobFieldKind::Scalar),
            DataType::Array(array) if matches!(array.element_type(), DataType::Blob(_)) => {
                Ok(BlobFieldKind::Array)
            }
            DataType::Map(map) if matches!(map.value_type(), DataType::Blob(_)) => {
                Ok(BlobFieldKind::Map(map.key_type().clone()))
            }
            other => Err(Error::DataInvalid {
                message: format!(
                    ".blob format requires a Blob, Array<Blob>, or Map<X, Blob> field, got {:?} for column '{}'",
                    other,
                    field.name()
                ),
                source: None,
            }),
        })
        .transpose()
}

fn build_descriptor_values(
    blob_index: &BlobFileIndex,
    positions: &[usize],
    file_path: &str,
) -> crate::Result<Vec<BlobReadValue>> {
    positions
        .iter()
        .map(|&position| {
            let entry = blob_index
                .entry(position)
                .ok_or_else(|| Error::DataInvalid {
                    message: format!(
                        "Blob row selection referenced out-of-range position {position} for {} rows",
                        blob_index.num_rows()
                    ),
                    source: None,
                })?;

            Ok(match entry {
                BlobEntry::Value(range) => {
                    let descriptor = BlobDescriptor::new(
                        file_path.to_string(),
                        range.start as i64,
                        (range.end - range.start) as i64,
                    );
                    BlobReadValue::Value(Bytes::from(descriptor.serialize()))
                }
                BlobEntry::Null => BlobReadValue::Null,
                BlobEntry::Placeholder => BlobReadValue::Placeholder,
            })
        })
        .collect()
}

pub(crate) fn build_blob_batch(
    target_schema: &Arc<arrow_schema::Schema>,
    values: Vec<BlobReadValue>,
) -> crate::Result<RecordBatch> {
    let mut builder = LargeBinaryBuilder::new();
    for value in values {
        match value {
            BlobReadValue::Value(bytes) => builder.append_value(bytes.as_ref()),
            BlobReadValue::Null | BlobReadValue::Placeholder => builder.append_null(),
            BlobReadValue::Array(_) | BlobReadValue::Map(_) => {
                return Err(Error::UnexpectedError {
                    message: "Scalar BLOB reader produced an ARRAY<BLOB> value".to_string(),
                    source: None,
                });
            }
        }
    }

    let columns: Vec<ArrayRef> = vec![Arc::new(builder.finish())];
    RecordBatch::try_new(target_schema.clone(), columns).map_err(|e| Error::UnexpectedError {
        message: format!("Failed to build blob RecordBatch: {e}"),
        source: Some(Box::new(e)),
    })
}

pub(crate) fn build_blob_array_batch(
    target_schema: &Arc<arrow_schema::Schema>,
    values: Vec<BlobReadValue>,
) -> crate::Result<RecordBatch> {
    let element_field = match target_schema.field(0).data_type() {
        arrow_schema::DataType::List(element_field) => element_field.clone(),
        other => {
            return Err(Error::UnexpectedError {
                message: format!(
                    "Expected Array<Blob> to map to Arrow List<LargeBinary>, got {other:?}"
                ),
                source: None,
            });
        }
    };
    let mut builder = ListBuilder::new(LargeBinaryBuilder::new()).with_field(element_field);
    for value in values {
        match value {
            BlobReadValue::Array(elements) => {
                for element in elements {
                    match element {
                        Some(bytes) => builder.values().append_value(bytes.as_ref()),
                        None => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            BlobReadValue::Null | BlobReadValue::Placeholder => builder.append(false),
            BlobReadValue::Value(_) | BlobReadValue::Map(_) => {
                return Err(Error::UnexpectedError {
                    message: "ARRAY<BLOB> reader produced a scalar BLOB value".to_string(),
                    source: None,
                });
            }
        }
    }

    let columns: Vec<ArrayRef> = vec![Arc::new(builder.finish())];
    RecordBatch::try_new(target_schema.clone(), columns).map_err(|e| Error::UnexpectedError {
        message: format!("Failed to build ARRAY<BLOB> RecordBatch: {e}"),
        source: Some(Box::new(e)),
    })
}

pub(crate) fn build_blob_map_batch(
    target_schema: &Arc<arrow_schema::Schema>,
    values: Vec<BlobReadValue>,
    key_type: &DataType,
) -> crate::Result<RecordBatch> {
    let ArrowDataType::Map(entries_field, ordered) = target_schema.field(0).data_type() else {
        return Err(Error::UnexpectedError {
            message: "Expected MAP<X, BLOB> to map to Arrow Map".to_string(),
            source: None,
        });
    };
    let ArrowDataType::Struct(entry_fields) = entries_field.data_type() else {
        return Err(Error::UnexpectedError {
            message: "Expected MAP<X, BLOB> entries to be an Arrow Struct".to_string(),
            source: None,
        });
    };

    let mut keys = Vec::new();
    let mut blobs = Vec::new();
    let mut offsets = vec![0i32];
    let mut validity = Vec::with_capacity(values.len());
    for value in values {
        match value {
            BlobReadValue::Map(entries) => {
                validity.push(true);
                let next = offsets
                    .last()
                    .copied()
                    .unwrap()
                    .checked_add(
                        i32::try_from(entries.len()).map_err(|e| Error::DataInvalid {
                            message: "MAP<X, BLOB> entry count exceeds Arrow i32 offsets"
                                .to_string(),
                            source: Some(Box::new(e)),
                        })?,
                    )
                    .ok_or_else(|| Error::DataInvalid {
                        message: "MAP<X, BLOB> batch exceeds Arrow i32 offsets".to_string(),
                        source: None,
                    })?;
                for (key, blob) in entries {
                    keys.push(key);
                    blobs.push(blob);
                }
                offsets.push(next);
            }
            BlobReadValue::Null | BlobReadValue::Placeholder => {
                validity.push(false);
                offsets.push(*offsets.last().unwrap());
            }
            BlobReadValue::Value(_) | BlobReadValue::Array(_) => {
                return Err(Error::UnexpectedError {
                    message: "MAP<X, BLOB> reader produced a non-map value".to_string(),
                    source: None,
                });
            }
        }
    }

    let key_array = decode_blob_map_keys(&keys, key_type)?;
    let value_array = Arc::new(LargeBinaryArray::from_iter(
        blobs.iter().map(|value| value.as_deref()),
    )) as ArrayRef;
    let entries = StructArray::try_new(entry_fields.clone(), vec![key_array, value_array], None)
        .map_err(|e| Error::UnexpectedError {
            message: format!("Failed to build MAP<X, BLOB> entries: {e}"),
            source: Some(Box::new(e)),
        })?;
    let map = MapArray::try_new(
        entries_field.clone(),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        entries,
        Some(NullBuffer::new(BooleanBuffer::from(validity))),
        *ordered,
    )
    .map_err(|e| Error::UnexpectedError {
        message: format!("Failed to build MAP<X, BLOB> array: {e}"),
        source: Some(Box::new(e)),
    })?;
    RecordBatch::try_new(target_schema.clone(), vec![Arc::new(map)]).map_err(|e| {
        Error::UnexpectedError {
            message: format!("Failed to build MAP<X, BLOB> RecordBatch: {e}"),
            source: Some(Box::new(e)),
        }
    })
}

fn decode_blob_map_keys(keys: &[Bytes], key_type: &DataType) -> crate::Result<ArrayRef> {
    macro_rules! fixed_keys {
        ($array:ty, $type:ty, $size:expr) => {{
            let values = keys
                .iter()
                .map(|key| {
                    let bytes: [u8; $size] =
                        key.as_ref().try_into().map_err(|_| Error::DataInvalid {
                            message: format!(
                                "Invalid MAP<X, BLOB> fixed-width key length: {}",
                                key.len()
                            ),
                            source: None,
                        })?;
                    Ok(<$type>::from_le_bytes(bytes))
                })
                .collect::<crate::Result<Vec<_>>>()?;
            Ok(Arc::new(<$array>::from(values)) as ArrayRef)
        }};
    }

    for key in keys {
        validate_blob_map_key_length(key_type, key.len() as u64)?;
    }
    if blob_map_key_uses_binary_offsets(key_type) {
        keys.iter().try_fold(0u64, |total, key| {
            checked_arrow_binary_data_length(total, key.len() as u64, "MAP<X, BLOB> batch key data")
        })?;
    }

    match key_type {
        DataType::TinyInt(_) => fixed_keys!(Int8Array, i8, 1),
        DataType::SmallInt(_) => fixed_keys!(Int16Array, i16, 2),
        DataType::Int(_) => fixed_keys!(Int32Array, i32, 4),
        DataType::BigInt(_) => fixed_keys!(Int64Array, i64, 8),
        DataType::Date(_) => fixed_keys!(Date32Array, i32, 4),
        DataType::Time(_) => fixed_keys!(Time32MillisecondArray, i32, 4),
        DataType::Boolean(_) => {
            let values = keys
                .iter()
                .map(|key| match key.as_ref() {
                    [0] => Ok(false),
                    [1] => Ok(true),
                    _ => Err(Error::DataInvalid {
                        message: "Invalid MAP<X, BLOB> boolean key".to_string(),
                        source: None,
                    }),
                })
                .collect::<crate::Result<Vec<_>>>()?;
            Ok(Arc::new(BooleanArray::from(values)))
        }
        DataType::Char(_) | DataType::VarChar(_) => {
            let values = keys
                .iter()
                .map(|key| {
                    std::str::from_utf8(key).map_err(|e| Error::DataInvalid {
                        message: "Invalid MAP<X, BLOB> string key".to_string(),
                        source: Some(Box::new(e)),
                    })
                })
                .collect::<crate::Result<Vec<_>>>()?;
            Ok(Arc::new(StringArray::from(values)))
        }
        DataType::Binary(_) | DataType::VarBinary(_) => Ok(Arc::new(
            BinaryArray::from_iter_values(keys.iter().map(|key| key.as_ref())),
        )),
        DataType::Decimal(decimal) => {
            let values = keys
                .iter()
                .map(|key| decode_blob_map_decimal(key, decimal.precision()))
                .collect::<crate::Result<Vec<_>>>()?;
            let array = Decimal128Array::from(values)
                .with_precision_and_scale(decimal.precision() as u8, decimal.scale() as i8)
                .map_err(|e| Error::DataInvalid {
                    message: format!("Invalid MAP<X, BLOB> decimal key: {e}"),
                    source: Some(Box::new(e)),
                })?;
            Ok(Arc::new(array))
        }
        other => Err(Error::Unsupported {
            message: format!("Unsupported key type for MAP<X, BLOB>: {other:?}"),
        }),
    }
}

fn blob_map_key_uses_binary_offsets(key_type: &DataType) -> bool {
    matches!(
        key_type,
        DataType::Char(_) | DataType::VarChar(_) | DataType::Binary(_) | DataType::VarBinary(_)
    )
}

fn validate_blob_map_key_length(key_type: &DataType, length: u64) -> crate::Result<()> {
    let fixed_length = match key_type {
        DataType::TinyInt(_) | DataType::Boolean(_) => Some(1),
        DataType::SmallInt(_) => Some(2),
        DataType::Int(_) | DataType::Date(_) | DataType::Time(_) => Some(4),
        DataType::BigInt(_) => Some(8),
        DataType::Decimal(decimal) if decimal.precision() <= 18 => Some(8),
        DataType::Decimal(_) => {
            if !(1..=16).contains(&length) {
                return Err(Error::DataInvalid {
                    message: "Invalid MAP<X, BLOB> decimal key".to_string(),
                    source: None,
                });
            }
            return Ok(());
        }
        DataType::Char(_) | DataType::VarChar(_) | DataType::Binary(_) | DataType::VarBinary(_) => {
            return Ok(())
        }
        other => {
            return Err(Error::Unsupported {
                message: format!("Unsupported key type for MAP<X, BLOB>: {other:?}"),
            });
        }
    };
    if fixed_length != Some(length) {
        return Err(Error::DataInvalid {
            message: format!("Invalid MAP<X, BLOB> fixed-width key length: {length}"),
            source: None,
        });
    }
    Ok(())
}

fn checked_arrow_binary_data_length(
    current: u64,
    additional: u64,
    context: &str,
) -> crate::Result<u64> {
    let total = current
        .checked_add(additional)
        .filter(|total| *total <= i32::MAX as u64)
        .ok_or_else(|| Error::DataInvalid {
            message: format!("{context} is too large for Arrow Binary"),
            source: None,
        })?;
    Ok(total)
}

fn decode_blob_map_decimal(bytes: &[u8], precision: u32) -> crate::Result<i128> {
    let value = if precision <= 18 {
        let bytes: [u8; 8] = bytes.try_into().map_err(|_| Error::DataInvalid {
            message: format!(
                "Invalid MAP<X, BLOB> fixed-width key length: {}",
                bytes.len()
            ),
            source: None,
        })?;
        i64::from_le_bytes(bytes) as i128
    } else {
        if bytes.is_empty() || bytes.len() > 16 {
            return Err(Error::DataInvalid {
                message: "Invalid MAP<X, BLOB> decimal key".to_string(),
                source: None,
            });
        }
        let fill = if bytes[0] & 0x80 == 0 { 0 } else { 0xff };
        let mut extended = [fill; 16];
        extended[16 - bytes.len()..].copy_from_slice(bytes);
        i128::from_be_bytes(extended)
    };
    let digits = value.unsigned_abs().to_string().len() as u32;
    if digits > precision {
        return Err(Error::DataInvalid {
            message: "MAP<X, BLOB> decimal key exceeds declared precision".to_string(),
            source: None,
        });
    }
    Ok(value)
}

fn plan_blob_reads(
    blob_index: &BlobFileIndex,
    positions: &[usize],
) -> crate::Result<Vec<PlannedBlobRead>> {
    positions
        .iter()
        .map(|&position| {
            let entry = blob_index
                .entry(position)
                .ok_or_else(|| Error::DataInvalid {
                    message: format!(
                        "Blob row selection referenced out-of-range position {position} for {} rows",
                        blob_index.num_rows()
                    ),
                    source: None,
                })?;

            Ok(match entry {
                BlobEntry::Value(range) => PlannedBlobRead::Entry(blob_entry_range(range)),
                BlobEntry::Null => PlannedBlobRead::Null,
                BlobEntry::Placeholder => PlannedBlobRead::Placeholder,
            })
        })
        .collect()
}

async fn fetch_blob_values(
    reader: &dyn FileRead,
    planned_reads: Vec<PlannedBlobRead>,
    blob_parallelism: usize,
) -> crate::Result<Vec<BlobReadValue>> {
    let mut values = Vec::with_capacity(planned_reads.len());
    let mut entry_reads = Vec::new();
    for (result_index, planned_read) in planned_reads.into_iter().enumerate() {
        match planned_read {
            PlannedBlobRead::Null => values.push(Some(BlobReadValue::Null)),
            PlannedBlobRead::Placeholder => values.push(Some(BlobReadValue::Placeholder)),
            PlannedBlobRead::Entry(range) => {
                values.push(None);
                entry_reads.push(BlobEntryRead {
                    result_index,
                    range,
                });
            }
        }
    }

    for resolved in read_merged_blob_entries(reader, entry_reads, blob_parallelism).await? {
        let value = decode_blob_entry(resolved.entry, resolved.range)?;
        values[resolved.result_index] = Some(BlobReadValue::Value(value));
    }
    collect_blob_read_values(values)
}

#[derive(Debug)]
struct BlobEntryRead {
    result_index: usize,
    range: Range<u64>,
}

#[derive(Debug)]
struct MergedBlobEntryRead {
    range: Range<u64>,
    entries: Vec<BlobEntryRead>,
    selected_bytes: u64,
}

struct ResolvedBlobEntryRead {
    result_index: usize,
    range: Range<u64>,
    entry: Bytes,
}

async fn read_merged_blob_entries(
    reader: &dyn FileRead,
    entry_reads: Vec<BlobEntryRead>,
    blob_parallelism: usize,
) -> crate::Result<Vec<ResolvedBlobEntryRead>> {
    let merged_reads = merge_blob_entry_reads(entry_reads);
    let resolved: Vec<Vec<ResolvedBlobEntryRead>> =
        futures::stream::iter(merged_reads.into_iter().map(|merged_read| async move {
            let expected_length = merged_read.range.end - merged_read.range.start;
            let copy_entries = expected_length > merged_read.selected_bytes;
            let data = reader.read(merged_read.range.clone()).await?;
            if data.len() as u64 != expected_length {
                return Err(Error::DataInvalid {
                    message: format!(
                        "Short read for merged Blob range {:?}: expected {expected_length} bytes, got {}",
                        merged_read.range,
                        data.len()
                    ),
                    source: None,
                });
            }

            merged_read
                .entries
                .into_iter()
                .map(|entry_read| {
                    let start = usize::try_from(entry_read.range.start - merged_read.range.start)
                        .map_err(|e| Error::DataInvalid {
                            message: "Blob entry offset exceeds usize".to_string(),
                            source: Some(Box::new(e)),
                        })?;
                    let end = usize::try_from(entry_read.range.end - merged_read.range.start)
                        .map_err(|e| Error::DataInvalid {
                            message: "Blob entry offset exceeds usize".to_string(),
                            source: Some(Box::new(e)),
                        })?;
                    // Bytes::slice keeps the complete merged response alive. Copy entries from a
                    // gapped span so unselected bytes can be released as soon as this read resolves.
                    let entry = if copy_entries {
                        Bytes::copy_from_slice(&data[start..end])
                    } else {
                        data.slice(start..end)
                    };
                    Ok(ResolvedBlobEntryRead {
                        result_index: entry_read.result_index,
                        range: entry_read.range,
                        entry,
                    })
                })
                .collect::<crate::Result<Vec<_>>>()
        }))
        .buffer_unordered(blob_parallelism)
        .try_collect()
        .await?;
    Ok(resolved.into_iter().flatten().collect())
}

fn collect_blob_read_values(
    values: Vec<Option<BlobReadValue>>,
) -> crate::Result<Vec<BlobReadValue>> {
    values
        .into_iter()
        .map(|value| {
            value.ok_or_else(|| Error::UnexpectedError {
                message: "Blob read did not produce a value".to_string(),
                source: None,
            })
        })
        .collect()
}

fn merge_blob_entry_reads(mut reads: Vec<BlobEntryRead>) -> Vec<MergedBlobEntryRead> {
    if reads.is_empty() {
        return Vec::new();
    }

    reads.sort_unstable_by_key(|read| (read.range.start, read.range.end, read.result_index));
    let mut reads = reads.into_iter();
    let first = reads.next().unwrap();
    let mut current = MergedBlobEntryRead {
        range: first.range.clone(),
        selected_bytes: first.range.end - first.range.start,
        entries: vec![first],
    };
    let mut merged = Vec::new();
    for read in reads {
        let merged_end = current.range.end.max(read.range.end);
        let added_selected_bytes = read
            .range
            .end
            .saturating_sub(read.range.start.max(current.range.end));
        let selected_bytes = current.selected_bytes.saturating_add(added_selected_bytes);
        let merged_span = merged_end - current.range.start;
        let close_enough = read
            .range
            .start
            .checked_sub(current.range.end)
            .map(|gap| gap <= BLOB_RANGE_MERGE_GAP)
            .unwrap_or(true);
        let amplification_bounded =
            merged_span <= selected_bytes.saturating_mul(BLOB_RANGE_MERGE_MAX_AMPLIFICATION);
        if close_enough && merged_span <= BLOB_RANGE_MERGE_MAX_SPAN && amplification_bounded {
            current.range.end = merged_end;
            current.selected_bytes = selected_bytes;
            current.entries.push(read);
        } else {
            merged.push(current);
            current = MergedBlobEntryRead {
                range: read.range.clone(),
                selected_bytes: read.range.end - read.range.start,
                entries: vec![read],
            };
        }
    }
    merged.push(current);
    merged
}

fn blob_entry_range(payload_range: &Range<u64>) -> Range<u64> {
    payload_range.start - BLOB_INLINE_HEADER_SIZE..payload_range.end + BLOB_TRAILER_SIZE
}

async fn read_blob_entry(reader: &dyn FileRead, entry_range: Range<u64>) -> crate::Result<Bytes> {
    let entry = reader.read(entry_range.clone()).await?;
    decode_blob_entry(entry, entry_range)
}

fn decode_blob_entry(entry: Bytes, entry_range: Range<u64>) -> crate::Result<Bytes> {
    let expected_entry_length = entry_range.end - entry_range.start;
    if entry.len() as u64 != expected_entry_length {
        return Err(Error::DataInvalid {
            message: format!(
                "Short read for Blob entry range {entry_range:?}: expected {expected_entry_length} bytes, got {}",
                entry.len()
            ),
            source: None,
        });
    }

    let actual_magic = i32::from_le_bytes(
        entry[..BLOB_INLINE_HEADER_SIZE as usize]
            .try_into()
            .unwrap(),
    );
    if actual_magic != BLOB_MAGIC_NUMBER {
        return Err(Error::DataInvalid {
            message: format!(
                "Invalid Blob entry magic at offset {}: expected {BLOB_MAGIC_NUMBER}, got {actual_magic}",
                entry_range.start
            ),
            source: None,
        });
    }

    let length_offset = entry.len() - BLOB_TRAILER_SIZE as usize;
    let crc_offset = entry.len() - std::mem::size_of::<u32>();
    let embedded_length = i64::from_le_bytes(entry[length_offset..crc_offset].try_into().unwrap());
    if u64::try_from(embedded_length).ok() != Some(expected_entry_length) {
        return Err(Error::DataInvalid {
            message: format!(
                "Blob entry length mismatch at offset {}: index declares {expected_entry_length}, entry stores {embedded_length}",
                entry_range.start
            ),
            source: None,
        });
    }

    let expected_crc = u32::from_le_bytes(entry[crc_offset..].try_into().unwrap());
    let actual_crc = crc32fast::hash(&entry[..crc_offset]);
    if actual_crc != expected_crc {
        return Err(Error::DataInvalid {
            message: format!(
                "Blob entry CRC32 mismatch at offset {}: expected {expected_crc:#010x}, got {actual_crc:#010x}",
                entry_range.start
            ),
            source: None,
        });
    }

    Ok(entry.slice(BLOB_INLINE_HEADER_SIZE as usize..length_offset))
}

struct InMemoryBlobEntryReader {
    entry_range: Range<u64>,
    entry: Bytes,
}

#[async_trait]
impl FileRead for InMemoryBlobEntryReader {
    async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
        if range.start > range.end
            || range.start < self.entry_range.start
            || range.end > self.entry_range.end
        {
            return Err(Error::DataInvalid {
                message: format!(
                    "Blob entry memory read {range:?} is outside {:?}",
                    self.entry_range
                ),
                source: None,
            });
        }
        let start = usize::try_from(range.start - self.entry_range.start).map_err(|e| {
            Error::DataInvalid {
                message: "Blob entry memory offset exceeds usize".to_string(),
                source: Some(Box::new(e)),
            }
        })?;
        let end = usize::try_from(range.end - self.entry_range.start).map_err(|e| {
            Error::DataInvalid {
                message: "Blob entry memory offset exceeds usize".to_string(),
                source: Some(Box::new(e)),
            }
        })?;
        Ok(self.entry.slice(start..end))
    }
}

fn plan_blob_array_reads(
    blob_index: &BlobFileIndex,
    positions: &[usize],
) -> crate::Result<Vec<PlannedBlobArrayRead>> {
    positions
        .iter()
        .map(|&position| {
            let entry = blob_index
                .entry(position)
                .ok_or_else(|| Error::DataInvalid {
                    message: format!(
                        "Blob row selection referenced out-of-range position {position} for {} rows",
                        blob_index.num_rows()
                    ),
                    source: None,
                })?;

            Ok(match entry {
                BlobEntry::Value(range) => PlannedBlobArrayRead::Read(range.clone()),
                BlobEntry::Null => PlannedBlobArrayRead::Null,
                BlobEntry::Placeholder => PlannedBlobArrayRead::Placeholder,
            })
        })
        .collect()
}

async fn fetch_blob_array_values(
    reader: &dyn FileRead,
    planned_reads: Vec<PlannedBlobArrayRead>,
    file_path: &str,
    descriptor_mode: bool,
    blob_parallelism: usize,
) -> crate::Result<Vec<BlobReadValue>> {
    if !descriptor_mode {
        return fetch_inline_nested_blob_values(
            reader,
            planned_reads,
            blob_parallelism,
            InlineNestedBlobKind::Array,
        )
        .await;
    }

    futures::stream::iter(planned_reads.into_iter().map(|planned_read| async move {
        match planned_read {
            PlannedBlobArrayRead::Null => Ok(BlobReadValue::Null),
            PlannedBlobArrayRead::Placeholder => Ok(BlobReadValue::Placeholder),
            PlannedBlobArrayRead::Read(payload_range) => {
                let metadata = read_blob_array_metadata(reader, payload_range).await?;
                build_blob_array_descriptors(metadata, file_path)
            }
        }
    }))
    .buffered(blob_parallelism)
    .try_collect()
    .await
}

async fn read_blob_array_metadata(
    reader: &dyn FileRead,
    payload_range: Range<u64>,
) -> crate::Result<BlobArrayMetadata> {
    let layout = read_blob_array_layout(reader, payload_range).await?;
    let index_bytes = if layout.element_index_range.is_empty() {
        Bytes::new()
    } else {
        read_blob_array_range(reader, layout.element_index_range.clone(), "element index").await?
    };

    decode_blob_array_metadata(layout, index_bytes.as_ref())
}

async fn read_blob_array_layout(
    reader: &dyn FileRead,
    payload_range: Range<u64>,
) -> crate::Result<BlobArrayLayout> {
    let payload_length = validate_blob_array_payload_range(&payload_range)?;

    let header_end = payload_range.start + BLOB_ARRAY_HEADER_SIZE;
    let header = read_blob_array_range(reader, payload_range.start..header_end, "header").await?;

    let index_length_position = payload_range.end - BLOB_ARRAY_INDEX_LENGTH_SIZE;
    let index_length_bytes = read_blob_array_range(
        reader,
        index_length_position..payload_range.end,
        "index length",
    )
    .await?;
    parse_blob_array_layout(
        payload_range,
        payload_length,
        header.as_ref(),
        index_length_bytes.as_ref(),
    )
}

fn validate_blob_array_payload_range(payload_range: &Range<u64>) -> crate::Result<u64> {
    let payload_length = payload_range
        .end
        .checked_sub(payload_range.start)
        .ok_or_else(|| Error::DataInvalid {
            message: format!("Invalid ARRAY<BLOB> payload range: {payload_range:?}"),
            source: None,
        })?;
    if payload_length < BLOB_ARRAY_MIN_PAYLOAD_SIZE {
        return Err(Error::DataInvalid {
            message: format!(
                "ARRAY<BLOB> payload is too small: expected at least {BLOB_ARRAY_MIN_PAYLOAD_SIZE} bytes, got {payload_length}"
            ),
            source: None,
        });
    }
    Ok(payload_length)
}

fn parse_blob_array_layout(
    payload_range: Range<u64>,
    payload_length: u64,
    header: &[u8],
    index_length_bytes: &[u8],
) -> crate::Result<BlobArrayLayout> {
    let magic = i32::from_le_bytes(header[..4].try_into().unwrap());
    if magic != BLOB_ARRAY_MAGIC_NUMBER {
        return Err(Error::DataInvalid {
            message: format!(
                "Invalid ARRAY<BLOB> payload magic number: expected {BLOB_ARRAY_MAGIC_NUMBER}, got {magic}"
            ),
            source: None,
        });
    }
    if header[4] != BLOB_ARRAY_VERSION {
        return Err(Error::Unsupported {
            message: format!(
                "Unsupported ARRAY<BLOB> payload version: expected {BLOB_ARRAY_VERSION}, got {}",
                header[4]
            ),
        });
    }
    let element_count = i32::from_le_bytes(header[5..9].try_into().unwrap());
    if element_count < 0 {
        return Err(Error::DataInvalid {
            message: format!("Invalid ARRAY<BLOB> element count: {element_count}"),
            source: None,
        });
    }

    let index_length = i32::from_le_bytes(index_length_bytes[..4].try_into().unwrap());
    let maximum_index_length = payload_length - BLOB_ARRAY_MIN_PAYLOAD_SIZE;
    if index_length < 0 || index_length as u64 > maximum_index_length {
        return Err(Error::DataInvalid {
            message: format!("Invalid ARRAY<BLOB> element index length: {index_length}"),
            source: None,
        });
    }
    let index_length = index_length as u64;
    if element_count as u64 > index_length {
        return Err(Error::DataInvalid {
            message: "ARRAY<BLOB> element count exceeds element index length".to_string(),
            source: None,
        });
    }

    let index_length_position = payload_range.end - BLOB_ARRAY_INDEX_LENGTH_SIZE;
    let index_start = index_length_position - index_length;
    Ok(BlobArrayLayout {
        element_count: element_count as usize,
        element_data_range: payload_range.start + BLOB_ARRAY_HEADER_SIZE..index_start,
        element_index_range: index_start..index_length_position,
    })
}

fn decode_blob_array_metadata(
    layout: BlobArrayLayout,
    index_bytes: &[u8],
) -> crate::Result<BlobArrayMetadata> {
    let encoded_lengths = decode_delta_varints(index_bytes).map_err(|e| Error::DataInvalid {
        message: format!("Invalid ARRAY<BLOB> element index: {e}"),
        source: Some(Box::new(e)),
    })?;
    if encoded_lengths.len() != layout.element_count {
        return Err(Error::DataInvalid {
            message: format!(
                "ARRAY<BLOB> element count {} does not match index value count {}",
                layout.element_count,
                encoded_lengths.len()
            ),
            source: None,
        });
    }

    let mut remaining_data_length = layout.element_data_range.end - layout.element_data_range.start;
    let mut element_lengths = Vec::with_capacity(encoded_lengths.len());
    for encoded_length in encoded_lengths {
        if encoded_length == BLOB_ARRAY_NULL_ELEMENT_LENGTH {
            element_lengths.push(None);
            continue;
        }
        let element_length = u64::try_from(encoded_length).map_err(|e| Error::DataInvalid {
            message: format!("Invalid ARRAY<BLOB> element length: {encoded_length}"),
            source: Some(Box::new(e)),
        })?;
        if element_length > remaining_data_length {
            return Err(Error::DataInvalid {
                message: "ARRAY<BLOB> element lengths exceed the payload data length".to_string(),
                source: None,
            });
        }
        remaining_data_length -= element_length;
        element_lengths.push(Some(element_length));
    }
    if remaining_data_length != 0 {
        return Err(Error::DataInvalid {
            message: "ARRAY<BLOB> element lengths do not match the payload data length".to_string(),
            source: None,
        });
    }

    Ok(BlobArrayMetadata {
        element_data_range: layout.element_data_range,
        element_lengths,
    })
}

async fn read_blob_array_range(
    reader: &dyn FileRead,
    range: Range<u64>,
    part: &str,
) -> crate::Result<Bytes> {
    let expected_length = range.end - range.start;
    let bytes = reader
        .read(range.clone())
        .await
        .map_err(|e| Error::UnexpectedError {
            message: format!("Failed to read ARRAY<BLOB> {part} range {range:?}: {e}"),
            source: Some(Box::new(e)),
        })?;
    if bytes.len() as u64 != expected_length {
        return Err(Error::DataInvalid {
            message: format!(
                "Short read for ARRAY<BLOB> {part} range {range:?}: expected {expected_length} bytes, got {}",
                bytes.len()
            ),
            source: None,
        });
    }
    Ok(bytes)
}

async fn read_inline_blob_array_entry(
    reader: &dyn FileRead,
    payload_range: Range<u64>,
) -> crate::Result<BlobReadValue> {
    let _preflight_layout = read_blob_array_layout(reader, payload_range.clone()).await?;

    let payload = read_blob_entry(reader, blob_entry_range(&payload_range)).await?;
    let payload_length = validate_blob_array_payload_range(&payload_range)?;
    if payload.len() as u64 != payload_length {
        return Err(Error::DataInvalid {
            message: format!(
                "ARRAY<BLOB> payload length mismatch: expected {payload_length} bytes, got {}",
                payload.len()
            ),
            source: None,
        });
    }

    let index_length_position = payload.len() - BLOB_ARRAY_INDEX_LENGTH_SIZE as usize;
    let layout = parse_blob_array_layout(
        payload_range.clone(),
        payload_length,
        &payload[..BLOB_ARRAY_HEADER_SIZE as usize],
        &payload[index_length_position..],
    )?;
    let index_start = (layout.element_index_range.start - payload_range.start) as usize;
    let index_end = (layout.element_index_range.end - payload_range.start) as usize;
    let metadata = decode_blob_array_metadata(layout, &payload[index_start..index_end])?;

    let data_start = (metadata.element_data_range.start - payload_range.start) as usize;
    let data_end = (metadata.element_data_range.end - payload_range.start) as usize;
    let data = payload.slice(data_start..data_end);

    let mut offset = 0usize;
    let mut elements = Vec::with_capacity(metadata.element_lengths.len());
    for element_length in metadata.element_lengths {
        match element_length {
            None => elements.push(None),
            Some(element_length) => {
                let element_length = element_length as usize;
                let end = offset + element_length;
                elements.push(Some(data.slice(offset..end)));
                offset = end;
            }
        }
    }
    Ok(BlobReadValue::Array(elements))
}

fn build_blob_array_descriptors(
    metadata: BlobArrayMetadata,
    file_path: &str,
) -> crate::Result<BlobReadValue> {
    let mut element_offset = metadata.element_data_range.start;
    let mut elements = Vec::with_capacity(metadata.element_lengths.len());
    for element_length in metadata.element_lengths {
        match element_length {
            None => elements.push(None),
            Some(element_length) => {
                let descriptor_offset =
                    i64::try_from(element_offset).map_err(|e| Error::DataInvalid {
                        message: format!(
                            "ARRAY<BLOB> descriptor offset exceeds i64: {element_offset}"
                        ),
                        source: Some(Box::new(e)),
                    })?;
                let descriptor_length =
                    i64::try_from(element_length).map_err(|e| Error::DataInvalid {
                        message: format!(
                            "ARRAY<BLOB> descriptor length exceeds i64: {element_length}"
                        ),
                        source: Some(Box::new(e)),
                    })?;
                let descriptor = BlobDescriptor::new(
                    file_path.to_string(),
                    descriptor_offset,
                    descriptor_length,
                );
                elements.push(Some(Bytes::from(descriptor.serialize())));
                element_offset += element_length;
            }
        }
    }
    Ok(BlobReadValue::Array(elements))
}

async fn fetch_blob_map_values(
    reader: &dyn FileRead,
    planned_reads: Vec<PlannedBlobArrayRead>,
    file_path: &str,
    descriptor_mode: bool,
    key_type: &DataType,
    blob_parallelism: usize,
) -> crate::Result<Vec<BlobReadValue>> {
    if !descriptor_mode {
        return fetch_inline_nested_blob_values(
            reader,
            planned_reads,
            blob_parallelism,
            InlineNestedBlobKind::Map {
                file_path,
                key_type,
            },
        )
        .await;
    }

    futures::stream::iter(planned_reads.into_iter().map(|planned_read| async move {
        match planned_read {
            PlannedBlobArrayRead::Null => Ok(BlobReadValue::Null),
            PlannedBlobArrayRead::Placeholder => Ok(BlobReadValue::Placeholder),
            PlannedBlobArrayRead::Read(payload_range) => {
                read_blob_map_entry(reader, payload_range, file_path, descriptor_mode, key_type)
                    .await
            }
        }
    }))
    .buffered(blob_parallelism)
    .try_collect()
    .await
}

#[derive(Clone, Copy)]
enum InlineNestedBlobKind<'a> {
    Array,
    Map {
        file_path: &'a str,
        key_type: &'a DataType,
    },
}

async fn fetch_inline_nested_blob_values(
    reader: &dyn FileRead,
    planned_reads: Vec<PlannedBlobArrayRead>,
    blob_parallelism: usize,
    kind: InlineNestedBlobKind<'_>,
) -> crate::Result<Vec<BlobReadValue>> {
    let mut values = Vec::with_capacity(planned_reads.len());
    let mut entry_reads = Vec::new();
    for (result_index, planned_read) in planned_reads.into_iter().enumerate() {
        match planned_read {
            PlannedBlobArrayRead::Null => values.push(Some(BlobReadValue::Null)),
            PlannedBlobArrayRead::Placeholder => values.push(Some(BlobReadValue::Placeholder)),
            PlannedBlobArrayRead::Read(payload_range) => {
                values.push(None);
                entry_reads.push(BlobEntryRead {
                    result_index,
                    range: blob_entry_range(&payload_range),
                });
            }
        }
    }

    for resolved in read_merged_blob_entries(reader, entry_reads, blob_parallelism).await? {
        let result_index = resolved.result_index;
        let payload_range =
            resolved.range.start + BLOB_INLINE_HEADER_SIZE..resolved.range.end - BLOB_TRAILER_SIZE;
        let memory_reader = InMemoryBlobEntryReader {
            entry_range: resolved.range,
            entry: resolved.entry,
        };
        let value = match kind {
            InlineNestedBlobKind::Array => {
                read_inline_blob_array_entry(&memory_reader, payload_range).await?
            }
            InlineNestedBlobKind::Map {
                file_path,
                key_type,
            } => {
                read_blob_map_entry(&memory_reader, payload_range, file_path, false, key_type)
                    .await?
            }
        };
        values[result_index] = Some(value);
    }
    collect_blob_read_values(values)
}

async fn read_blob_map_entry(
    reader: &dyn FileRead,
    payload_range: Range<u64>,
    file_path: &str,
    descriptor_mode: bool,
    key_type: &DataType,
) -> crate::Result<BlobReadValue> {
    let payload_length = payload_range
        .end
        .checked_sub(payload_range.start)
        .ok_or_else(|| Error::DataInvalid {
            message: format!("Invalid MAP<X, BLOB> payload range: {payload_range:?}"),
            source: None,
        })?;
    if payload_length < BLOB_MAP_MIN_PAYLOAD_SIZE {
        return Err(Error::DataInvalid {
            message: format!(
                "MAP<X, BLOB> payload is too small: expected at least {BLOB_MAP_MIN_PAYLOAD_SIZE} bytes, got {payload_length}"
            ),
            source: None,
        });
    }

    let index_lengths_start = payload_range.end - BLOB_MAP_INDEX_LENGTHS_SIZE;
    let header = read_blob_map_range(
        reader,
        payload_range.start..payload_range.start + BLOB_MAP_HEADER_SIZE,
        "header",
    )
    .await?;
    let magic = i32::from_le_bytes(header[..4].try_into().unwrap());
    if magic != BLOB_MAP_MAGIC_NUMBER {
        return Err(Error::DataInvalid {
            message: format!(
                "Invalid MAP<X, BLOB> payload magic number: expected {BLOB_MAP_MAGIC_NUMBER}, got {magic}"
            ),
            source: None,
        });
    }
    if header[4] != BLOB_MAP_VERSION {
        return Err(Error::Unsupported {
            message: format!(
                "Unsupported MAP<X, BLOB> payload version: expected {BLOB_MAP_VERSION}, got {}",
                header[4]
            ),
        });
    }
    let entry_count = i32::from_le_bytes(header[5..9].try_into().unwrap());
    if entry_count < 0 {
        return Err(Error::DataInvalid {
            message: format!("Invalid MAP<X, BLOB> entry count: {entry_count}"),
            source: None,
        });
    }
    let entry_count = entry_count as usize;

    let index_lengths = read_blob_map_range(
        reader,
        index_lengths_start..payload_range.end,
        "index lengths",
    )
    .await?;
    let key_index_length = i32::from_le_bytes(index_lengths[..4].try_into().unwrap());
    let value_index_length = i32::from_le_bytes(index_lengths[4..8].try_into().unwrap());
    let max_indexes = payload_length - BLOB_MAP_MIN_PAYLOAD_SIZE;
    if key_index_length < 0 || key_index_length as u64 > max_indexes {
        return Err(Error::DataInvalid {
            message: format!("Invalid MAP<X, BLOB> key index length: {key_index_length}"),
            source: None,
        });
    }
    if value_index_length < 0 || value_index_length as u64 > max_indexes {
        return Err(Error::DataInvalid {
            message: format!("Invalid MAP<X, BLOB> value index length: {value_index_length}"),
            source: None,
        });
    }
    let key_index_length = key_index_length as u64;
    let value_index_length = value_index_length as u64;
    if key_index_length + value_index_length > max_indexes
        || entry_count as u64 > key_index_length
        || entry_count as u64 > value_index_length
    {
        return Err(Error::DataInvalid {
            message: "MAP<X, BLOB> indexes do not match the payload".to_string(),
            source: None,
        });
    }

    let value_index_start = index_lengths_start - value_index_length;
    let key_index_start = value_index_start - key_index_length;
    let indexes = read_blob_map_range(
        reader,
        key_index_start..index_lengths_start,
        "key/value indexes",
    )
    .await?;
    let key_index_length = key_index_length as usize;
    let key_index = &indexes[..key_index_length];
    let value_index = &indexes[key_index_length..];
    let key_lengths = decode_delta_varints(key_index).map_err(|e| Error::DataInvalid {
        message: format!("Invalid MAP<X, BLOB> key index: {e}"),
        source: Some(Box::new(e)),
    })?;
    let value_lengths = decode_delta_varints(value_index).map_err(|e| Error::DataInvalid {
        message: format!("Invalid MAP<X, BLOB> value index: {e}"),
        source: Some(Box::new(e)),
    })?;
    if key_lengths.len() != entry_count || value_lengths.len() != entry_count {
        return Err(Error::DataInvalid {
            message: "MAP<X, BLOB> entry count does not match index lengths".to_string(),
            source: None,
        });
    }

    let data_start = payload_range.start + BLOB_MAP_HEADER_SIZE;
    let data_length = key_index_start - data_start;
    let mut key_data_length = 0u64;
    for &length in &key_lengths {
        if length == BLOB_MAP_NULL_LENGTH {
            return Err(Error::DataInvalid {
                message: "MAP<X, BLOB> null keys cannot be represented by Arrow".to_string(),
                source: None,
            });
        }
        let length = u64::try_from(length).map_err(|e| Error::DataInvalid {
            message: format!("Invalid MAP<X, BLOB> key length: {length}"),
            source: Some(Box::new(e)),
        })?;
        validate_blob_map_key_length(key_type, length)?;
        key_data_length = key_data_length
            .checked_add(length)
            .filter(|total| *total <= data_length)
            .ok_or_else(|| Error::DataInvalid {
                message: "MAP<X, BLOB> key lengths exceed the payload data length".to_string(),
                source: None,
            })?;
    }
    let value_data_length = data_length - key_data_length;
    let mut total_value_length = 0u64;
    for &length in &value_lengths {
        if length == BLOB_MAP_NULL_LENGTH {
            continue;
        }
        let length = u64::try_from(length).map_err(|e| Error::DataInvalid {
            message: format!("Invalid MAP<X, BLOB> value length: {length}"),
            source: Some(Box::new(e)),
        })?;
        total_value_length = total_value_length
            .checked_add(length)
            .filter(|total| *total <= value_data_length)
            .ok_or_else(|| Error::DataInvalid {
                message: "MAP<X, BLOB> value lengths exceed the payload data length".to_string(),
                source: None,
            })?;
    }
    if total_value_length != value_data_length {
        return Err(Error::DataInvalid {
            message: "MAP<X, BLOB> key/value lengths do not match the payload data length"
                .to_string(),
            source: None,
        });
    }
    if blob_map_key_uses_binary_offsets(key_type) {
        checked_arrow_binary_data_length(0, key_data_length, "MAP<X, BLOB> key data")?;
    }

    let key_data =
        read_blob_map_range(reader, data_start..data_start + key_data_length, "key data").await?;
    let mut keys = Vec::with_capacity(entry_count);
    let mut cursor = 0usize;
    let mut unique = std::collections::HashSet::with_capacity(entry_count);
    for length in key_lengths {
        let length = length as usize;
        let end = cursor + length;
        let key = key_data.slice(cursor..end);
        if !unique.insert(key.clone()) {
            return Err(Error::DataInvalid {
                message: "Invalid MAP<X, BLOB> payload: duplicate key".to_string(),
                source: None,
            });
        }
        keys.push(key);
        cursor = end;
    }

    let mut value_offset = data_start + key_data_length;
    let mut reads = Vec::with_capacity(entry_count);
    for length in value_lengths {
        if length == BLOB_MAP_NULL_LENGTH {
            reads.push(None);
        } else {
            let length = length as u64;
            reads.push(Some(value_offset..value_offset + length));
            value_offset += length;
        }
    }
    let values = if descriptor_mode {
        reads
            .into_iter()
            .map(|range| {
                range
                    .map(|range| {
                        let offset =
                            i64::try_from(range.start).map_err(|e| Error::DataInvalid {
                                message: "MAP<X, BLOB> descriptor offset exceeds i64".to_string(),
                                source: Some(Box::new(e)),
                            })?;
                        let length = i64::try_from(range.end - range.start).map_err(|e| {
                            Error::DataInvalid {
                                message: "MAP<X, BLOB> descriptor length exceeds i64".to_string(),
                                source: Some(Box::new(e)),
                            }
                        })?;
                        Ok(Bytes::from(
                            BlobDescriptor::new(file_path.to_string(), offset, length).serialize(),
                        ))
                    })
                    .transpose()
            })
            .collect::<crate::Result<Vec<_>>>()?
    } else {
        let payload = read_blob_entry(reader, blob_entry_range(&payload_range)).await?;
        reads
            .into_iter()
            .map(|range| {
                range.map(|range| {
                    payload.slice(
                        (range.start - payload_range.start) as usize
                            ..(range.end - payload_range.start) as usize,
                    )
                })
            })
            .collect()
    };
    Ok(BlobReadValue::Map(keys.into_iter().zip(values).collect()))
}

async fn read_blob_map_range(
    reader: &dyn FileRead,
    range: Range<u64>,
    part: &str,
) -> crate::Result<Bytes> {
    let expected = range.end - range.start;
    let bytes = reader.read(range.clone()).await?;
    if bytes.len() as u64 != expected {
        return Err(Error::DataInvalid {
            message: format!(
                "Short read for MAP<X, BLOB> {part} range {range:?}: expected {expected} bytes, got {}",
                bytes.len()
            ),
            source: None,
        });
    }
    Ok(bytes)
}

#[derive(Debug, Clone)]
enum PlannedBlobRead {
    Null,
    Placeholder,
    Entry(Range<u64>),
}

#[derive(Debug, Clone)]
enum PlannedBlobArrayRead {
    Null,
    Placeholder,
    Read(Range<u64>),
}

#[derive(Debug)]
struct BlobArrayMetadata {
    element_data_range: Range<u64>,
    element_lengths: Vec<Option<u64>>,
}

#[derive(Debug)]
struct BlobArrayLayout {
    element_count: usize,
    element_data_range: Range<u64>,
    element_index_range: Range<u64>,
}

#[derive(Debug)]
struct BlobFileIndex {
    entries: Vec<BlobEntry>,
}

async fn read_blob_range(reader: &dyn FileRead, range: Range<u64>) -> crate::Result<Bytes> {
    let expected = range.end - range.start;
    let bytes = reader.read(range.clone()).await?;
    if bytes.len() as u64 != expected {
        return Err(Error::DataInvalid {
            message: format!(
                "Short BLOB index read for {range:?}: expected {expected} bytes, got {}",
                bytes.len()
            ),
            source: None,
        });
    }
    Ok(bytes)
}

type BlobIndexLoadResult = Result<Arc<BlobFileIndex>, Arc<Error>>;

struct BlobIndexCache {
    entries: FileMetadataCache<String, BlobIndexLoadResult>,
}

impl BlobIndexCache {
    fn new(max_bytes: usize) -> Self {
        Self {
            entries: FileMetadataCache::new(max_bytes, usize::MAX),
        }
    }
}

impl BlobFileIndex {
    async fn load_cached(reader: &dyn FileRead, file_size: u64) -> crate::Result<Arc<Self>> {
        let Some(context) = reader
            .blob_index_cache()
            .and_then(|cache| cache.downcast_ref::<BlobIndexCacheContext>())
        else {
            return Ok(Arc::new(Self::load(reader, file_size).await?));
        };
        let cache = context.get_or_init(BlobIndexCache::new);
        let cache_key = reader.cache_key().map(ToOwned::to_owned);
        let key_heap_bytes = cache_key.as_ref().map_or(0, String::capacity);
        let load = cache
            .entries
            .get_or_try_insert_with_admission(
                cache_key,
                key_heap_bytes,
                || async {
                    Ok::<_, std::convert::Infallible>(Arc::new(
                        Self::load(reader, file_size)
                            .await
                            .map(Arc::new)
                            .map_err(Arc::new),
                    ))
                },
                |load| {
                    load.as_ref()
                        .ok()
                        .map(|index| index.estimated_cache_bytes())
                },
            )
            .await
            .unwrap();
        match load.as_ref() {
            Ok(index) => Ok(Arc::clone(index)),
            Err(error) => Err(clone_blob_index_error(error)),
        }
    }

    fn estimated_cache_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(
                self.entries
                    .capacity()
                    .saturating_mul(std::mem::size_of::<BlobEntry>()),
            )
            // Account for the LRU node, hash slot and Arc allocation which are
            // not represented by the decoded index itself.
            .saturating_add(BLOB_INDEX_CACHE_CONTAINER_OVERHEAD)
    }

    async fn load(reader: &dyn FileRead, file_size: u64) -> crate::Result<Self> {
        if file_size < BLOB_FOOTER_SIZE {
            return Err(Error::DataInvalid {
                message: format!(
                    "Blob file is too small: expected at least {BLOB_FOOTER_SIZE} bytes, got {file_size}"
                ),
                source: None,
            });
        }

        const TAIL_PREFETCH_SIZE: u64 = 4096;
        let tail_start = file_size - file_size.min(TAIL_PREFETCH_SIZE);
        let tail = read_blob_range(reader, tail_start..file_size).await?;
        let footer_start = tail.len() - BLOB_FOOTER_SIZE as usize;
        let footer_bytes = &tail[footer_start..];
        let index_length = i32::from_le_bytes(footer_bytes[..4].try_into().unwrap());
        if index_length < 0 {
            return Err(Error::DataInvalid {
                message: format!("Blob footer contains a negative index length: {index_length}"),
                source: None,
            });
        }
        if footer_bytes[4] != BLOB_FORMAT_VERSION {
            return Err(Error::Unsupported {
                message: format!(
                    "unsupported .blob footer version: expected {BLOB_FORMAT_VERSION}, got {}",
                    footer_bytes[4]
                ),
            });
        }

        let index_length = index_length as u64;
        if index_length > file_size - BLOB_FOOTER_SIZE {
            return Err(Error::DataInvalid {
                message: format!(
                    "Blob footer index length {index_length} exceeds file payload size {}",
                    file_size - BLOB_FOOTER_SIZE
                ),
                source: None,
            });
        }

        let index_start = file_size - BLOB_FOOTER_SIZE - index_length;
        let data_region_end = index_start;
        let index_bytes = if index_start >= tail_start {
            tail.slice((index_start - tail_start) as usize..footer_start)
        } else {
            // Reread the small tail overlap rather than copy the entire index.
            read_blob_range(reader, index_start..file_size - BLOB_FOOTER_SIZE).await?
        };

        let lengths = decode_delta_varints(index_bytes.as_ref())?;
        let entries = BlobEntry::build_all(&lengths, data_region_end)?;
        Ok(Self { entries })
    }

    fn num_rows(&self) -> usize {
        self.entries.len()
    }

    fn entry(&self, position: usize) -> Option<&BlobEntry> {
        self.entries.get(position)
    }
}

#[derive(Debug)]
struct SharedBlobIndexError(Arc<Error>);

impl std::fmt::Display for SharedBlobIndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.0.as_ref(), f)
    }
}

impl std::error::Error for SharedBlobIndexError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

fn clone_blob_index_error(error: &Arc<Error>) -> Error {
    let source = || {
        Some(Box::new(SharedBlobIndexError(Arc::clone(error)))
            as Box<dyn std::error::Error + Send + Sync>)
    };
    match error.as_ref() {
        Error::DataInvalid { message, .. } => Error::DataInvalid {
            message: message.clone(),
            source: source(),
        },
        Error::Unsupported { message } => Error::Unsupported {
            message: message.clone(),
        },
        Error::UnexpectedError { message, .. } => Error::UnexpectedError {
            message: message.clone(),
            source: source(),
        },
        _ => Error::UnexpectedError {
            message: error.to_string(),
            source: source(),
        },
    }
}

#[derive(Debug, Clone)]
enum BlobEntry {
    Value(Range<u64>),
    Null,
    Placeholder,
}

impl BlobEntry {
    fn build_all(lengths: &[i64], data_region_end: u64) -> crate::Result<Vec<Self>> {
        let mut entries = Vec::with_capacity(lengths.len());
        let mut next_offset = 0_u64;

        for &entry_length in lengths {
            match entry_length {
                -1 => {
                    entries.push(Self::Null);
                    continue;
                }
                -2 => {
                    entries.push(Self::Placeholder);
                    continue;
                }
                _ => {}
            }

            let entry_length = u64::try_from(entry_length).map_err(|e| Error::DataInvalid {
                message: format!(
                    "Blob entry length must be positive, -1, or -2, got {entry_length}"
                ),
                source: Some(Box::new(e)),
            })?;

            if entry_length < BLOB_ENTRY_OVERHEAD {
                return Err(Error::DataInvalid {
                    message: format!(
                        "Blob entry length {entry_length} is smaller than minimum overhead {BLOB_ENTRY_OVERHEAD}"
                    ),
                    source: None,
                });
            }

            let entry_end =
                next_offset
                    .checked_add(entry_length)
                    .ok_or_else(|| Error::DataInvalid {
                        message: format!("Blob entry length overflow at offset {next_offset}"),
                        source: None,
                    })?;
            if entry_end > data_region_end {
                return Err(Error::DataInvalid {
                    message: format!(
                        "Blob entry range [{next_offset}, {entry_end}) exceeds data region end {data_region_end}"
                    ),
                    source: None,
                });
            }

            let data_offset = next_offset + BLOB_INLINE_HEADER_SIZE;
            let data_length = entry_length - BLOB_ENTRY_OVERHEAD;
            entries.push(Self::Value(data_offset..data_offset + data_length));
            next_offset = entry_end;
        }

        Ok(entries)
    }
}

#[derive(Debug, Clone)]
struct RowSelectionCursor {
    state: RowSelectionState,
}

#[derive(Debug, Clone)]
enum RowSelectionState {
    All {
        next: usize,
        total_rows: usize,
    },
    Ranges {
        total_rows: usize,
        ranges: Vec<RowRange>,
        range_idx: usize,
        next_in_range: i64,
    },
}

impl RowSelectionCursor {
    fn new(total_rows: usize, row_selection: Option<Vec<RowRange>>) -> crate::Result<Self> {
        let state = match row_selection {
            None => RowSelectionState::All {
                next: 0,
                total_rows,
            },
            Some(ranges) => {
                for range in &ranges {
                    if range.from() < 0 {
                        return Err(Error::DataInvalid {
                            message: format!(
                                "Blob row selection must be non-negative, got [{}..={}]",
                                range.from(),
                                range.to()
                            ),
                            source: None,
                        });
                    }
                    let to = usize::try_from(range.to()).map_err(|e| Error::DataInvalid {
                        message: format!(
                            "Blob row selection upper bound {} is out of range",
                            range.to()
                        ),
                        source: Some(Box::new(e)),
                    })?;
                    if to >= total_rows && total_rows != 0 {
                        return Err(Error::DataInvalid {
                            message: format!(
                                "Blob row selection [{}..={}] exceeds available rows {}",
                                range.from(),
                                range.to(),
                                total_rows
                            ),
                            source: None,
                        });
                    }
                }

                let next_in_range = ranges.first().map_or(0, RowRange::from);
                RowSelectionState::Ranges {
                    total_rows,
                    ranges,
                    range_idx: 0,
                    next_in_range,
                }
            }
        };

        Ok(Self { state })
    }

    fn next_batch(&mut self, batch_size: usize) -> Option<Vec<usize>> {
        if batch_size == 0 {
            return None;
        }

        match &mut self.state {
            RowSelectionState::All { next, total_rows } => {
                if *next >= *total_rows {
                    return None;
                }

                let end = (*next + batch_size).min(*total_rows);
                let batch: Vec<usize> = (*next..end).collect();
                *next = end;
                Some(batch)
            }
            RowSelectionState::Ranges {
                total_rows,
                ranges,
                range_idx,
                next_in_range,
            } => {
                if *range_idx >= ranges.len() || *total_rows == 0 {
                    return None;
                }

                let mut batch = Vec::with_capacity(batch_size);
                while batch.len() < batch_size && *range_idx < ranges.len() {
                    let range = &ranges[*range_idx];
                    if *next_in_range > range.to() {
                        *range_idx += 1;
                        if *range_idx < ranges.len() {
                            *next_in_range = ranges[*range_idx].from();
                        }
                        continue;
                    }

                    batch.push(*next_in_range as usize);
                    *next_in_range += 1;
                }

                if batch.is_empty() {
                    None
                } else {
                    Some(batch)
                }
            }
        }
    }
}

fn decode_delta_varints(bytes: &[u8]) -> crate::Result<Vec<i64>> {
    let mut values = Vec::new();
    let mut cursor = 0usize;
    let mut previous = 0_i64;

    while cursor < bytes.len() {
        let (delta, consumed) = decode_varint(&bytes[cursor..])?;
        cursor += consumed;

        let value = if values.is_empty() {
            delta
        } else {
            previous
                .checked_add(delta)
                .ok_or_else(|| Error::DataInvalid {
                    message: format!(
                        "Blob delta-varint index overflow after previous value {previous}"
                    ),
                    source: None,
                })?
        };
        values.push(value);
        previous = value;
    }

    Ok(values)
}

fn decode_varint(bytes: &[u8]) -> crate::Result<(i64, usize)> {
    let mut value = 0_u64;
    let mut shift = 0_u32;

    for (idx, byte) in bytes.iter().copied().enumerate() {
        value |= u64::from(byte & 0x7f) << shift;
        if (byte & 0x80) == 0 {
            let decoded = ((value >> 1) as i64) ^ (-((value & 1) as i64));
            return Ok((decoded, idx + 1));
        }

        shift += 7;
        if shift > 63 {
            return Err(Error::DataInvalid {
                message: "Blob delta-varint index overflow".to_string(),
                source: None,
            });
        }
    }

    Err(Error::DataInvalid {
        message: "Unexpected end of blob delta-varint index".to_string(),
        source: None,
    })
}

// --- Blob Format Writer ---

pub(crate) struct BlobFormatWriter {
    writer: Box<dyn FileWrite>,
    file_io: Option<crate::io::FileIO>,
    bytes_written: u64,
    lengths: Vec<i64>,
}

impl BlobFormatWriter {
    pub(crate) async fn new(
        output: &crate::io::OutputFile,
        file_io: Option<crate::io::FileIO>,
    ) -> crate::Result<Self> {
        let writer = output.writer().await?;
        Ok(Self {
            writer,
            file_io,
            bytes_written: 0,
            lengths: Vec::new(),
        })
    }

    /// Append one managed BLOB and return the payload range in this pack.
    /// The range excludes the four-byte entry magic and twelve-byte trailer,
    /// matching Java `BlobFormatWriter`'s descriptor callback.
    pub(crate) async fn write_managed_value(&mut self, value: &[u8]) -> crate::Result<(i64, i64)> {
        let start = self.bytes_written;
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "blob",
            ArrowDataType::LargeBinary,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(LargeBinaryArray::from(vec![Some(value)]))],
        )
        .map_err(|error| Error::DataInvalid {
            message: format!("Failed to build managed BLOB input: {error}"),
            source: Some(Box::new(error)),
        })?;
        self.write(&batch).await?;
        let entry_len =
            self.bytes_written
                .checked_sub(start)
                .ok_or_else(|| Error::DataInvalid {
                    message: "Managed BLOB writer position moved backwards".to_string(),
                    source: None,
                })?;
        let payload_len =
            entry_len
                .checked_sub(BLOB_ENTRY_OVERHEAD)
                .ok_or_else(|| Error::DataInvalid {
                    message: "Managed BLOB entry is shorter than its framing".to_string(),
                    source: None,
                })?;
        let offset =
            start
                .checked_add(BLOB_INLINE_HEADER_SIZE)
                .ok_or_else(|| Error::DataInvalid {
                    message: "Managed BLOB payload offset overflows u64".to_string(),
                    source: None,
                })?;
        Ok((
            i64::try_from(offset).map_err(|error| Error::DataInvalid {
                message: "Managed BLOB payload offset exceeds i64".to_string(),
                source: Some(Box::new(error)),
            })?,
            i64::try_from(payload_len).map_err(|error| Error::DataInvalid {
                message: "Managed BLOB payload length exceeds i64".to_string(),
                source: Some(Box::new(error)),
            })?,
        ))
    }
}

const BLOB_WRITE_BUFFER_SIZE: u64 = 8 * 1024 * 1024; // 8 MB

fn checked_blob_entry_length(payload_len: u64) -> crate::Result<i64> {
    let entry_length = payload_len
        .checked_add(BLOB_ENTRY_OVERHEAD)
        .ok_or_else(|| Error::DataInvalid {
            message: format!(
                "Blob entry length overflows u64: payload_length={payload_len}, overhead={BLOB_ENTRY_OVERHEAD}"
            ),
            source: None,
        })?;
    i64::try_from(entry_length).map_err(|e| Error::DataInvalid {
        message: format!(
            "Blob entry length exceeds i64: payload_length={payload_len}, entry_length={entry_length}"
        ),
        source: Some(Box::new(e)),
    })
}

#[async_trait]
impl FormatFileWriter for BlobFormatWriter {
    async fn write(&mut self, batch: &RecordBatch) -> crate::Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::LargeBinaryArray>()
            .ok_or_else(|| Error::DataInvalid {
                message: "BlobFormatWriter expects a single LargeBinary column".to_string(),
                source: None,
            })?;

        for row_idx in 0..col.len() {
            if col.is_null(row_idx) {
                self.lengths.push(-1);
                continue;
            }

            let value = col.value(row_idx);

            if BlobDescriptor::is_blob_descriptor(value) {
                let desc = BlobDescriptor::deserialize(value)?;
                let range = desc.range_spec()?;

                let file_io = self.file_io.as_ref().ok_or_else(|| Error::DataInvalid {
                    message:
                        "BlobFormatWriter received a BlobDescriptor but has no FileIO to resolve it"
                            .to_string(),
                    source: None,
                })?;
                let input = crate::io::uri_reader::UriInput::new(file_io, desc.uri())?;
                let offset = range.offset();
                let payload_len = match range.length() {
                    Some(length) => length,
                    None => input
                        .size()
                        .await
                        .map_err(|e| Error::UnexpectedError {
                            message: format!(
                                "Failed to read metadata for BlobDescriptor '{}': {e}",
                                crate::io::uri_reader::sanitize_blob_uri(desc.uri())
                            ),
                            source: Some(Box::new(e)),
                        })?
                        .saturating_sub(offset),
                };
                let end = offset
                    .checked_add(payload_len)
                    .ok_or_else(|| Error::DataInvalid {
                        message: format!(
                            "BlobDescriptor range overflows u64: offset={offset}, length={payload_len}"
                        ),
                        source: None,
                    })?;
                let entry_length = checked_blob_entry_length(payload_len)?;
                let entry_length_u64 = entry_length as u64;
                let bytes_written = self
                    .bytes_written
                    .checked_add(entry_length_u64)
                    .ok_or_else(|| Error::DataInvalid {
                        message: format!(
                            "Blob file size overflows u64: current_size={}, entry_length={entry_length_u64}",
                            self.bytes_written
                        ),
                        source: None,
                    })?;
                let reader = if payload_len == 0 {
                    None
                } else {
                    Some(input.reader_for_range(offset..end).await?)
                };

                let mut hasher = crc32fast::Hasher::new();

                hasher.update(&BLOB_MAGIC_NUMBER_BYTES);
                self.writer
                    .write(Bytes::copy_from_slice(&BLOB_MAGIC_NUMBER_BYTES))
                    .await?;

                // Stream payload in chunks to avoid loading entire blob into memory
                if let Some(reader) = reader.as_ref() {
                    let mut pos = offset;
                    while pos < end {
                        let chunk_end = pos.saturating_add(BLOB_WRITE_BUFFER_SIZE).min(end);
                        let chunk = reader.read(pos..chunk_end).await.map_err(|e| {
                            Error::UnexpectedError {
                                message: format!(
                                    "Failed to read BlobDescriptor '{}' range {pos}..{chunk_end}: {e}",
                                    crate::io::uri_reader::sanitize_blob_uri(desc.uri())
                                ),
                                source: Some(Box::new(e)),
                            }
                        })?;
                        let actual_len = chunk.len() as u64;
                        let expected_len = chunk_end - pos;
                        if actual_len != expected_len {
                            return Err(Error::DataInvalid {
                                message: format!(
                                    "Failed to read BlobDescriptor '{}': short read for range {pos}..{chunk_end}, expected={expected_len} bytes, actual={actual_len} bytes",
                                    crate::io::uri_reader::sanitize_blob_uri(desc.uri())
                                ),
                                source: None,
                            });
                        }
                        hasher.update(&chunk);
                        self.writer.write(chunk).await?;
                        pos = chunk_end;
                    }
                }

                let entry_length_bytes = entry_length.to_le_bytes();
                hasher.update(&entry_length_bytes);
                self.writer
                    .write(Bytes::copy_from_slice(&entry_length_bytes))
                    .await?;

                self.writer
                    .write(Bytes::copy_from_slice(&hasher.finalize().to_le_bytes()))
                    .await?;

                self.lengths.push(entry_length);
                self.bytes_written = bytes_written;
            } else {
                let entry_length = (value.len() + BLOB_ENTRY_OVERHEAD as usize) as i64;
                self.lengths.push(entry_length);

                let mut buf = Vec::with_capacity(entry_length as usize);
                let mut hasher = crc32fast::Hasher::new();

                hasher.update(&BLOB_MAGIC_NUMBER_BYTES);
                buf.extend_from_slice(&BLOB_MAGIC_NUMBER_BYTES);

                hasher.update(value);
                buf.extend_from_slice(value);

                let entry_length_bytes = entry_length.to_le_bytes();
                hasher.update(&entry_length_bytes);
                buf.extend_from_slice(&entry_length_bytes);

                buf.extend_from_slice(&hasher.finalize().to_le_bytes());

                self.writer.write(Bytes::from(buf)).await?;
                self.bytes_written += entry_length as u64;
            }
        }

        Ok(())
    }

    fn num_bytes(&self) -> usize {
        self.bytes_written as usize
    }

    fn in_progress_size(&self) -> usize {
        0
    }

    async fn flush(&mut self) -> crate::Result<()> {
        Ok(())
    }

    async fn close(mut self: Box<Self>) -> crate::Result<FormatWriteResult> {
        let index_bytes = encode_delta_varints_write(&self.lengths);
        let index_length = index_bytes.len() as i32;

        self.writer.write(Bytes::from(index_bytes)).await?;
        self.writer
            .write(Bytes::copy_from_slice(&index_length.to_le_bytes()))
            .await?;
        self.writer
            .write(Bytes::from_static(&[BLOB_FORMAT_VERSION]))
            .await?;

        let total = self.bytes_written + index_length as u64 + BLOB_FOOTER_SIZE;
        self.writer.close().await?;
        Ok(FormatWriteResult::new(total))
    }
}

fn encode_delta_varints_write(values: &[i64]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }
    let mut encoded = Vec::new();
    let mut previous = 0_i64;
    for (idx, &value) in values.iter().enumerate() {
        let delta = if idx == 0 { value } else { value - previous };
        previous = value;
        encode_varint(delta, &mut encoded);
    }
    encoded
}

fn encode_varint(value: i64, out: &mut Vec<u8>) {
    let mut remaining = ((value << 1) ^ (value >> 63)) as u64;
    while (remaining & !0x7f) != 0 {
        out.push(((remaining & 0x7f) as u8) | 0x80);
        remaining >>= 7;
    }
    out.push(remaining as u8);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::test_util::BytesFileRead;
    use crate::common::CatalogOptions;
    #[cfg(feature = "storage-oss")]
    use crate::io::FileIOCacheContext;
    #[cfg(feature = "storage-oss")]
    use crate::io::FileIOProvider;
    use crate::io::{BlobIndexCacheContext, FileIO, FileIOBuilder};
    use crate::spec::{ArrayType, BlobType, MapType, VarCharType};
    use arrow_array::Array;
    #[cfg(feature = "storage-oss")]
    use axum::{
        body::Body,
        extract::State,
        http::{
            header::{CONTENT_LENGTH, CONTENT_RANGE, RANGE},
            HeaderMap, Response, StatusCode,
        },
        routing::get,
        Router,
    };
    use bytes::Bytes;
    use futures::TryStreamExt;
    #[cfg(feature = "storage-oss")]
    use opendal::{Configurator, HttpTransporter, OperationContext, Operator};
    #[cfg(feature = "storage-oss")]
    use opendal_http_transport_reqwest::ReqwestTransport;
    #[cfg(feature = "storage-oss")]
    use opendal_service_oss::OssConfig;
    #[cfg(feature = "storage-oss")]
    use std::collections::HashMap;
    use std::mem::size_of;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;

    #[allow(dead_code)]
    mod blob_test_utils {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/blob_test_utils.rs"));
    }

    struct ForkFailingBlobRead {
        tail: Option<Bytes>,
    }

    #[async_trait::async_trait]
    impl FileRead for ForkFailingBlobRead {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            if range == (4..4100) {
                if let Some(tail) = &self.tail {
                    return Ok(tail.clone());
                }
            }
            Err(Error::ProcessForkUnsupported {
                message: crate::error::JINDO_FORK_ERROR.to_string(),
            })
        }
    }

    #[tokio::test]
    async fn blob_index_reads_preserve_fork_safety_error() {
        let error = BlobFileIndex::load(&ForkFailingBlobRead { tail: None }, 4100)
            .await
            .unwrap_err();
        assert!(matches!(error, Error::ProcessForkUnsupported { .. }));

        let mut tail = vec![0; 4096];
        tail[4091..4095].copy_from_slice(&4092_i32.to_le_bytes());
        tail[4095] = BLOB_FORMAT_VERSION;
        let error = BlobFileIndex::load(
            &ForkFailingBlobRead {
                tail: Some(Bytes::from(tail)),
            },
            4100,
        )
        .await
        .unwrap_err();
        assert!(matches!(error, Error::ProcessForkUnsupported { .. }));
    }

    #[tokio::test]
    async fn test_index_tail_prefetch_boundaries() {
        for rows in [0, 1, 4090, 4091, 4092, 9000] {
            let mut data = vec![0; 8192];
            let index = encode_delta_varints_write(&vec![-1; rows]);
            assert_eq!(index.len(), rows);
            data.extend_from_slice(&index);
            data.extend_from_slice(&(index.len() as i32).to_le_bytes());
            data.push(BLOB_FORMAT_VERSION);
            let size = data.len() as u64;
            let reader = TrackingFileRead::new(Bytes::from(data));
            let result = BlobFileIndex::load(&reader, size).await.unwrap();
            assert_eq!(result.num_rows(), rows);
            assert!(result
                .entries
                .iter()
                .all(|entry| matches!(entry, BlobEntry::Null)));
            let mut expected = Vec::with_capacity(2);
            expected.push(size - 4096..size);
            if rows + 5 > 4096 {
                expected.push(8192..size - BLOB_FOOTER_SIZE);
            }
            assert_eq!(reader.ranges(), expected);
        }
    }

    #[tokio::test]
    async fn test_large_index_varint_crosses_tail_boundary() {
        let lengths = vec![128; 4091];
        let index = encode_delta_varints_write(&lengths);
        assert_eq!(index.len(), 4092);
        // The tail starts between the two bytes encoding the first length.
        assert_ne!(index[0] & 0x80, 0);
        assert_eq!(index[1] & 0x80, 0);
        let index_start = 128 * lengths.len() as u64;
        let expected = BlobEntry::build_all(&lengths, index_start).unwrap();
        let mut data = vec![0; index_start as usize];
        data.extend_from_slice(&index);
        data.extend_from_slice(&(index.len() as i32).to_le_bytes());
        data.push(BLOB_FORMAT_VERSION);
        let size = data.len() as u64;
        assert_eq!(size - 4096, index_start + 1);
        let reader = TrackingFileRead::new(Bytes::from(data));
        let actual = BlobFileIndex::load(&reader, size).await.unwrap();
        assert_eq!(format!("{:?}", actual.entries), format!("{expected:?}"));
        assert_eq!(
            reader.ranges(),
            vec![size - 4096..size, index_start..size - BLOB_FOOTER_SIZE]
        );
    }

    #[tokio::test]
    async fn test_index_tail_prefetch_matches_legacy_fixtures() {
        for name in [
            "blob-basic.blob",
            "blob-array.blob",
            "blob-placeholder.blob",
        ] {
            let data = load_blob_fixture(name);
            let size = data.len() as u64;
            let index_length =
                i32::from_le_bytes(data[data.len() - 5..data.len() - 1].try_into().unwrap())
                    as usize;
            let index_start = data.len() - 5 - index_length;
            let legacy = BlobEntry::build_all(
                &decode_delta_varints(&data[index_start..data.len() - 5]).unwrap(),
                index_start as u64,
            )
            .unwrap();
            let reader = TrackingFileRead::new(Bytes::from(data));
            let actual = BlobFileIndex::load(&reader, size).await.unwrap();
            assert_eq!(format!("{:?}", actual.entries), format!("{legacy:?}"));
            let positions = (0..actual.num_rows()).rev().collect::<Vec<_>>();
            let old = BlobFileIndex { entries: legacy };
            assert_eq!(
                format!(
                    "{:?}",
                    build_descriptor_values(&actual, &positions, "file:///test.blob").unwrap()
                ),
                format!(
                    "{:?}",
                    build_descriptor_values(&old, &positions, "file:///test.blob").unwrap()
                ),
            );
            assert_eq!(reader.ranges(), vec![size.saturating_sub(4096)..size]);
        }
    }

    #[tokio::test]
    async fn test_index_tail_prefetch_invalid_input() {
        for size in 0..5 {
            let reader = TrackingFileRead::new(Bytes::from(vec![0; size]));
            assert!(BlobFileIndex::load(&reader, size as u64).await.is_err());
            assert!(reader.ranges().is_empty());
        }
        for (index, length, version) in [
            (vec![], -1_i32, 1_u8),
            (vec![], 1, 1),
            (vec![], 0, 2),
            (vec![0x80], 1, 1),
        ] {
            let mut data = index;
            data.extend_from_slice(&length.to_le_bytes());
            data.push(version);
            let size = data.len() as u64;
            let reader = TrackingFileRead::new(Bytes::from(data));
            assert!(BlobFileIndex::load(&reader, size).await.is_err());
            assert_eq!(reader.ranges(), vec![0..size]);
        }
        let reader = TrackingFileRead::new(Bytes::from_static(&[0, 0, 0, 0, 1]));
        assert_eq!(BlobFileIndex::load(&reader, 5).await.unwrap().num_rows(), 0);
    }

    #[tokio::test]
    async fn test_index_tail_prefetch_short_reads_and_errors() {
        for bytes in [Bytes::new(), Bytes::from_static(&[0; 4])] {
            let reader = SparseFileRead::new(vec![(0..5, bytes)]);
            assert!(matches!(
                BlobFileIndex::load(&reader, 5).await,
                Err(Error::DataInvalid { .. })
            ));
        }
        let reader = SparseFileRead::new(vec![]);
        assert!(BlobFileIndex::load(&reader, 5).await.is_err());
        let mut tail = vec![0; 4091];
        tail.extend_from_slice(&5000_i32.to_le_bytes());
        tail.push(1);
        for index in [None, Some(Bytes::new()), Some(Bytes::from(vec![0; 4999]))] {
            let mut responses = vec![(909..5005, Bytes::from(tail.clone()))];
            if let Some(index) = index {
                responses.push((0..5000, index));
            }
            let reader = SparseFileRead::new(responses);
            assert!(BlobFileIndex::load(&reader, 5005).await.is_err());
            assert_eq!(reader.ranges(), vec![909..5005, 0..5000]);
        }
    }

    #[tokio::test]
    async fn test_index_tail_prefetch_sixteen_cold_files() {
        let mut requests = 0;
        let mut bytes = 0;
        let mut legacy_bytes = 0;
        let mut legacy_requests = 0;
        for rows in (1526..).take(16) {
            let index = encode_delta_varints_write(&vec![-1; rows]);
            let mut data = vec![0; 8192];
            data.extend_from_slice(&index);
            data.extend_from_slice(&(index.len() as i32).to_le_bytes());
            data.push(1);
            let size = data.len() as u64;
            let reader = TrackingFileRead::new(Bytes::from(data.clone()));
            let legacy_reader = TrackingFileRead::new(Bytes::from(data));
            let footer = legacy_reader.read(size - 5..size).await.unwrap();
            let length = i32::from_le_bytes(footer[..4].try_into().unwrap()) as u64;
            let old_index = legacy_reader
                .read(size - 5 - length..size - 5)
                .await
                .unwrap();
            let old_entries = BlobEntry::build_all(
                &decode_delta_varints(&old_index).unwrap(),
                size - 5 - length,
            )
            .unwrap();
            let loaded = BlobFileIndex::load(&reader, size).await.unwrap();
            assert_eq!(loaded.num_rows(), rows);
            assert_eq!(format!("{:?}", loaded.entries), format!("{old_entries:?}"));
            requests += reader.ranges().len();
            bytes += reader
                .ranges()
                .iter()
                .map(|range| range.end - range.start)
                .sum::<u64>();
            legacy_requests += legacy_reader.ranges().len();
            legacy_bytes += legacy_reader
                .ranges()
                .iter()
                .map(|r| r.end - r.start)
                .sum::<u64>();
        }
        assert_eq!(legacy_requests, 32);
        assert_eq!((requests, bytes, legacy_bytes), (16, 65536, 24616));
    }

    #[tokio::test]
    async fn test_blob_reader_reads_inline_bytes_and_selection() {
        let read_fields = vec![DataField::new(
            0,
            "payload".to_string(),
            DataType::Blob(BlobType::new()),
        )];
        let reader = BlobFormatReader::new(String::new(), false);
        let file_bytes = load_blob_fixture("blob-basic.blob");

        let stream = reader
            .read_batch_stream(
                Box::new(BytesFileRead(Bytes::from(file_bytes.clone()))),
                file_bytes.len() as u64,
                &read_fields,
                None,
                Some(2),
                None,
            )
            .await
            .unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();

        assert_eq!(batches.len(), 2);
        assert_eq!(
            collect_binary_values(&batches[0]),
            vec![Some(b"hello".to_vec()), None]
        );
        assert_eq!(
            collect_binary_values(&batches[1]),
            vec![Some(b"world".to_vec()), Some(Vec::new())]
        );

        let selected = BlobFormatReader::new(String::new(), false)
            .read_batch_stream(
                Box::new(BytesFileRead(Bytes::from(file_bytes.clone()))),
                file_bytes.len() as u64,
                &read_fields,
                None,
                Some(8),
                Some(vec![RowRange::new(2, 3)]),
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(selected.len(), 1);
        assert_eq!(
            collect_binary_values(&selected[0]),
            vec![Some(b"world".to_vec()), Some(Vec::new())]
        );
    }

    #[tokio::test]
    async fn test_blob_reader_reuses_cached_index() {
        let file_path = "file:///blob-index-cache-test/data.blob";
        let file_bytes = load_blob_fixture("blob-basic.blob");
        let cache = blob_index_cache("64 MiB");
        let first = TrackingFileRead::new(Bytes::from(file_bytes.clone()))
            .with_blob_index_cache(file_path, cache.clone());
        let second = TrackingFileRead::new(Bytes::from(file_bytes.clone()))
            .with_blob_index_cache(file_path, cache);

        let first_reader = IndexedBlobReader::open(
            Box::new(first.clone()),
            file_bytes.len() as u64,
            file_path.to_string(),
            true,
        )
        .await
        .unwrap();
        let second_reader = IndexedBlobReader::open(
            Box::new(second.clone()),
            file_bytes.len() as u64,
            file_path.to_string(),
            true,
        )
        .await
        .unwrap();

        assert_eq!(first_reader.num_rows(), second_reader.num_rows());
        assert_eq!(first.ranges().len(), 1);
        assert!(second.ranges().is_empty());
    }

    #[tokio::test]
    async fn test_blob_index_cache_can_be_disabled() {
        let file_path = "file:///blob-index-cache-disabled/data.blob";
        let file_bytes = load_blob_fixture("blob-basic.blob");
        let cache = blob_index_cache("0");
        let first = TrackingFileRead::new(Bytes::from(file_bytes.clone()))
            .with_blob_index_cache(file_path, cache.clone());
        let second = TrackingFileRead::new(Bytes::from(file_bytes.clone()))
            .with_blob_index_cache(file_path, cache);

        for reader in [first.clone(), second.clone()] {
            IndexedBlobReader::open(
                Box::new(reader),
                file_bytes.len() as u64,
                file_path.to_string(),
                true,
            )
            .await
            .unwrap();
        }

        assert_eq!(first.ranges().len(), 1);
        assert_eq!(second.ranges().len(), 1);
    }

    #[tokio::test]
    async fn test_blob_index_cache_coalesces_concurrent_loads() {
        let file_path = "file:///blob-index-cache-concurrent/data.blob";
        let file_bytes = load_blob_fixture("blob-basic.blob");
        let cache = blob_index_cache("64 MiB");
        let tracking = TrackingFileRead::new(Bytes::from(file_bytes.clone()))
            .with_blob_index_cache(file_path, cache);

        let open = |reader: TrackingFileRead| {
            IndexedBlobReader::open(
                Box::new(reader),
                file_bytes.len() as u64,
                file_path.to_string(),
                true,
            )
        };
        let (first, second) = tokio::join!(open(tracking.clone()), open(tracking.clone()));

        assert_eq!(first.unwrap().num_rows(), second.unwrap().num_rows());
        assert_eq!(tracking.ranges().len(), 1);
    }

    #[tokio::test]
    async fn test_blob_index_cache_coalesces_failure_then_retries() {
        let file_path = "file:///blob-index-cache-failure/data.blob";
        let file_bytes = load_blob_fixture("blob-basic.blob");
        let cache = blob_index_cache("64 MiB");
        let failing = TrackingFileRead::new(Bytes::from(file_bytes.clone()))
            .with_blob_index_cache(file_path, cache.clone())
            .with_failure();

        let open = |reader: TrackingFileRead| {
            IndexedBlobReader::open(
                Box::new(reader),
                file_bytes.len() as u64,
                file_path.to_string(),
                true,
            )
        };
        let failures = futures::future::join_all((0..8).map(|_| open(failing.clone()))).await;

        assert!(failures.iter().all(Result::is_err));
        for failure in failures {
            let Error::UnexpectedError {
                source: Some(source),
                ..
            } = failure.err().unwrap()
            else {
                panic!("cached failure lost its source");
            };
            let shared = source.downcast_ref::<SharedBlobIndexError>().unwrap();
            assert!(std::error::Error::source(shared)
                .unwrap()
                .downcast_ref::<Error>()
                .is_some());
            assert!(matches!(
                shared.0.as_ref(),
                Error::UnexpectedError {
                    source: Some(_),
                    ..
                }
            ));
        }
        assert_eq!(failing.ranges().len(), 1);

        let retry = TrackingFileRead::new(Bytes::from(file_bytes.clone()))
            .with_blob_index_cache(file_path, cache);
        assert_eq!(open(retry.clone()).await.unwrap().num_rows(), 4);
        assert_eq!(retry.ranges().len(), 1);
    }

    #[test]
    fn test_blob_index_cache_preserves_direct_error_cause() {
        let original = Arc::new(Error::IoUnexpected {
            message: "injected storage failure".to_string(),
            source: Box::new(opendal::Error::new(
                opendal::ErrorKind::Unsupported,
                "injected native failure",
            )),
        });
        let Error::UnexpectedError {
            source: Some(source),
            ..
        } = clone_blob_index_error(&original)
        else {
            panic!("cached failure lost its cause");
        };
        let cause = std::error::Error::source(source.as_ref())
            .unwrap()
            .downcast_ref::<Error>()
            .unwrap();
        assert!(std::ptr::eq(cause, original.as_ref()));
    }

    #[tokio::test]
    async fn test_blob_index_cache_evicts_by_decoded_bytes() {
        let first_key = "memory:/blob-index-cache-bytes/first.blob";
        let second_key = "memory:/blob-index-cache-bytes/other.blob";
        assert_eq!(first_key.len(), second_key.len());
        let file_bytes = blob_test_utils::build_blob_file_bytes(&[None, None]);
        let max_bytes = blob_index_cache_entry_weight(first_key, 2);
        let cache = blob_index_cache(&max_bytes.to_string());
        let first = TrackingFileRead::new(Bytes::from(file_bytes.clone()))
            .with_blob_index_cache(first_key, cache.clone());
        let second = TrackingFileRead::new(Bytes::from(file_bytes.clone()))
            .with_blob_index_cache(second_key, cache.clone());
        let first_again = TrackingFileRead::new(Bytes::from(file_bytes.clone()))
            .with_blob_index_cache(first_key, cache);

        for (reader, key) in [
            (first.clone(), first_key),
            (second.clone(), second_key),
            (first_again.clone(), first_key),
        ] {
            IndexedBlobReader::open(
                Box::new(reader),
                file_bytes.len() as u64,
                key.to_string(),
                true,
            )
            .await
            .unwrap();
        }

        assert_eq!(first.ranges().len(), 1);
        assert_eq!(second.ranges().len(), 1);
        assert_eq!(first_again.ranges().len(), 1);
    }

    #[tokio::test]
    async fn test_oversized_blob_index_does_not_evict_cached_index() {
        let small_key = "memory:/blob-index-cache-size/small.blob";
        let large_key = "memory:/blob-index-cache-size/large.blob";
        assert_eq!(small_key.len(), large_key.len());
        let small_bytes = blob_test_utils::build_blob_file_bytes(&[None]);
        let large_values = vec![None; 100];
        let large_bytes = blob_test_utils::build_blob_file_bytes(&large_values);
        let max_bytes = blob_index_cache_entry_weight(small_key, 1);
        let cache = blob_index_cache(&max_bytes.to_string());
        let small = TrackingFileRead::new(Bytes::from(small_bytes.clone()))
            .with_blob_index_cache(small_key, cache.clone());
        let large = TrackingFileRead::new(Bytes::from(large_bytes.clone()))
            .with_blob_index_cache(large_key, cache.clone());
        let small_again = TrackingFileRead::new(Bytes::from(small_bytes.clone()))
            .with_blob_index_cache(small_key, cache);

        IndexedBlobReader::open(
            Box::new(small.clone()),
            small_bytes.len() as u64,
            small_key.to_string(),
            true,
        )
        .await
        .unwrap();
        IndexedBlobReader::open(
            Box::new(large.clone()),
            large_bytes.len() as u64,
            large_key.to_string(),
            true,
        )
        .await
        .unwrap();
        IndexedBlobReader::open(
            Box::new(small_again.clone()),
            small_bytes.len() as u64,
            small_key.to_string(),
            true,
        )
        .await
        .unwrap();

        assert_eq!(small.ranges().len(), 1);
        assert_eq!(large.ranges().len(), 1);
        assert!(small_again.ranges().is_empty());
    }

    #[tokio::test]
    async fn test_blob_index_cache_isolated_by_file_io() {
        let path = "memory:///blob-index-cache-namespace/data.blob";
        let value = b"value";
        let first_bytes = blob_test_utils::build_blob_file_bytes(&[None, Some(value.as_slice())]);
        let second_bytes = blob_test_utils::build_blob_file_bytes(&[Some(value.as_slice()), None]);
        assert_eq!(first_bytes.len(), second_bytes.len());

        let first_io = FileIOBuilder::new("memory").build().unwrap();
        let second_io = FileIOBuilder::new("memory").build().unwrap();
        first_io
            .new_output(path)
            .unwrap()
            .write(Bytes::from(first_bytes))
            .await
            .unwrap();
        second_io
            .new_output(path)
            .unwrap()
            .write(Bytes::from(second_bytes))
            .await
            .unwrap();

        assert!(!Arc::ptr_eq(
            &first_io.blob_index_cache(),
            &second_io.blob_index_cache()
        ));

        assert_eq!(
            read_scalar_blob_file(&first_io, path).await,
            vec![None, Some(value.to_vec())]
        );
        assert_eq!(
            read_scalar_blob_file(&second_io, path).await,
            vec![Some(value.to_vec()), None]
        );
    }

    #[tokio::test]
    #[cfg(feature = "storage-oss")]
    async fn test_blob_index_cache_isolated_by_storage_endpoint() {
        let value = b"value";
        let first_bytes = Bytes::from(blob_test_utils::build_blob_file_bytes(&[
            None,
            Some(value.as_slice()),
        ]));
        let second_bytes = Bytes::from(blob_test_utils::build_blob_file_bytes(&[
            Some(value.as_slice()),
            None,
        ]));
        assert_eq!(first_bytes.len(), second_bytes.len());
        let first_endpoint = serve_blob_file(first_bytes.clone()).await;
        let second_endpoint = serve_blob_file(second_bytes.clone()).await;
        let cache_context = FileIOCacheContext::from_props(&HashMap::new()).unwrap();

        let first_io = blob_oss_file_io(&first_endpoint, cache_context.clone());
        let second_io = blob_oss_file_io(&second_endpoint, cache_context);
        let path = "/data.blob";

        let first = IndexedBlobReader::open(
            Box::new(first_io.new_input(path).unwrap().reader().await.unwrap()),
            first_bytes.len() as u64,
            path.to_string(),
            false,
        )
        .await
        .unwrap();
        let second = IndexedBlobReader::open(
            Box::new(second_io.new_input(path).unwrap().reader().await.unwrap()),
            second_bytes.len() as u64,
            path.to_string(),
            false,
        )
        .await
        .unwrap();

        assert_eq!(second.num_rows(), 2);
        assert!(matches!(
            first.read_positions(&[0]).await.unwrap().as_slice(),
            [BlobReadValue::Null]
        ));
        assert!(matches!(
            second.read_positions(&[0]).await.unwrap().as_slice(),
            [BlobReadValue::Value(bytes)] if bytes.as_ref() == value
        ));
    }

    #[tokio::test]
    #[cfg(feature = "storage-azdls")]
    async fn test_blob_index_cache_isolated_by_azure_account() {
        let first_path = "abfss://container@account-a.dfs.core.windows.net/data.blob";
        let second_path = "abfss://container@account-b.dfs.core.windows.net/data.blob";
        let file_io = FileIO::from_path(first_path)
            .unwrap()
            .with_prop("azure.account-key", "account-key")
            .build()
            .unwrap();
        let first_key = file_io
            .new_input(first_path)
            .unwrap()
            .reader()
            .await
            .unwrap()
            .cache_key()
            .unwrap()
            .to_string();
        let second_key = file_io
            .new_input(second_path)
            .unwrap()
            .reader()
            .await
            .unwrap()
            .cache_key()
            .unwrap()
            .to_string();
        assert_ne!(first_key, second_key);

        let value = b"value";
        let first_bytes = blob_test_utils::build_blob_file_bytes(&[None, Some(value)]);
        let second_bytes = blob_test_utils::build_blob_file_bytes(&[Some(value), None]);
        let cache = blob_index_cache("64 MiB");
        let first = TrackingFileRead::new(Bytes::from(first_bytes.clone()))
            .with_blob_index_cache(first_key, cache.clone());
        let second = TrackingFileRead::new(Bytes::from(second_bytes.clone()))
            .with_blob_index_cache(second_key, cache);
        let first = IndexedBlobReader::open(
            Box::new(first),
            first_bytes.len() as u64,
            first_path.to_string(),
            false,
        )
        .await
        .unwrap();
        let second = IndexedBlobReader::open(
            Box::new(second),
            second_bytes.len() as u64,
            second_path.to_string(),
            false,
        )
        .await
        .unwrap();
        assert!(matches!(
            first.read_positions(&[0]).await.unwrap().as_slice(),
            [BlobReadValue::Null]
        ));
        assert!(matches!(
            second.read_positions(&[0]).await.unwrap().as_slice(),
            [BlobReadValue::Value(bytes)] if bytes.as_ref() == value
        ));
    }

    #[tokio::test]
    #[cfg(feature = "storage-hdfs")]
    async fn test_blob_index_cache_isolated_by_hdfs_name_node() {
        let path = "hdfs://logical-cluster/table/data.blob";
        let first_io = FileIO::from_path(path)
            .unwrap()
            .with_prop("hdfs.name-node", "hdfs://cluster-a:8020")
            .build()
            .unwrap();
        let second_io = FileIO::from_path(path)
            .unwrap()
            .with_prop("hdfs.name-node", "hdfs://cluster-b:8020")
            .with_cache_context(first_io.cache_context())
            .build()
            .unwrap();
        let first_key = first_io
            .new_input(path)
            .unwrap()
            .reader()
            .await
            .unwrap()
            .cache_key()
            .unwrap()
            .to_string();
        let second_key = second_io
            .new_input(path)
            .unwrap()
            .reader()
            .await
            .unwrap()
            .cache_key()
            .unwrap()
            .to_string();
        assert_ne!(first_key, second_key);

        let value = b"value";
        let first_bytes = blob_test_utils::build_blob_file_bytes(&[None, Some(value)]);
        let second_bytes = blob_test_utils::build_blob_file_bytes(&[Some(value), None]);
        assert_eq!(first_bytes.len(), second_bytes.len());
        let cache = first_io.blob_index_cache();
        let first = TrackingFileRead::new(Bytes::from(first_bytes.clone()))
            .with_blob_index_cache(first_key, cache.clone());
        let second = TrackingFileRead::new(Bytes::from(second_bytes.clone()))
            .with_blob_index_cache(second_key, cache);
        let first = IndexedBlobReader::open(
            Box::new(first),
            first_bytes.len() as u64,
            path.to_string(),
            false,
        )
        .await
        .unwrap();
        let second = IndexedBlobReader::open(
            Box::new(second),
            second_bytes.len() as u64,
            path.to_string(),
            false,
        )
        .await
        .unwrap();
        assert!(matches!(
            first.read_positions(&[0]).await.unwrap().as_slice(),
            [BlobReadValue::Null]
        ));
        assert!(
            matches!(second.read_positions(&[0]).await.unwrap().as_slice(), [BlobReadValue::Value(bytes)] if bytes.as_ref() == value)
        );
    }

    #[derive(Debug)]
    #[cfg(feature = "storage-oss")]
    struct FixedBlobProvider(Operator);

    #[async_trait]
    #[cfg(feature = "storage-oss")]
    impl FileIOProvider for FixedBlobProvider {
        async fn create(&self, _path: &str) -> crate::Result<(Operator, String)> {
            Ok((self.0.clone(), "data.blob".to_string()))
        }
    }

    #[derive(Debug)]
    #[cfg(feature = "storage-oss")]
    struct RoutedBlobProvider {
        first: Operator,
        second: Operator,
    }

    #[async_trait]
    #[cfg(feature = "storage-oss")]
    impl FileIOProvider for RoutedBlobProvider {
        async fn create(&self, path: &str) -> crate::Result<(Operator, String)> {
            let op = if path.starts_with("oss://first/") {
                &self.first
            } else {
                &self.second
            };
            Ok((op.clone(), "data.blob".to_string()))
        }
    }

    #[tokio::test]
    #[cfg(feature = "storage-oss")]
    async fn test_blob_index_cache_bypassed_for_opaque_provider_routes() {
        let value = b"value";
        let first_bytes = Bytes::from(blob_test_utils::build_blob_file_bytes(&[None, Some(value)]));
        let second_bytes =
            Bytes::from(blob_test_utils::build_blob_file_bytes(&[Some(value), None]));
        assert_eq!(first_bytes.len(), second_bytes.len());
        let first_endpoint = serve_blob_file(first_bytes.clone()).await;
        let second_endpoint = serve_blob_file(second_bytes.clone()).await;
        let io = FileIOBuilder::new("fs")
            .with_provider(Arc::new(RoutedBlobProvider {
                first: blob_oss_operator(&first_endpoint),
                second: blob_oss_operator(&second_endpoint),
            }))
            .build()
            .unwrap();
        let first_path = "oss://first/data.blob";
        let second_path = "oss://second/data.blob";
        let first_reader = io.new_input(first_path).unwrap().reader().await.unwrap();
        let second_reader = io.new_input(second_path).unwrap().reader().await.unwrap();
        assert_eq!(first_reader.cache_key(), None);
        assert_eq!(second_reader.cache_key(), None);
        let first = IndexedBlobReader::open(
            Box::new(first_reader),
            first_bytes.len() as u64,
            first_path.to_string(),
            false,
        )
        .await
        .unwrap();
        let second = IndexedBlobReader::open(
            Box::new(second_reader),
            second_bytes.len() as u64,
            second_path.to_string(),
            false,
        )
        .await
        .unwrap();
        assert!(matches!(
            first.read_positions(&[0]).await.unwrap().as_slice(),
            [BlobReadValue::Null]
        ));
        assert!(
            matches!(second.read_positions(&[0]).await.unwrap().as_slice(), [BlobReadValue::Value(bytes)] if bytes.as_ref() == value)
        );
    }

    #[tokio::test]
    #[cfg(feature = "storage-oss")]
    async fn test_blob_index_cache_isolated_after_provider_replacement() {
        let value = b"value";
        let first_bytes = Bytes::from(blob_test_utils::build_blob_file_bytes(&[
            None,
            Some(value.as_slice()),
        ]));
        let second_bytes = Bytes::from(blob_test_utils::build_blob_file_bytes(&[
            Some(value.as_slice()),
            None,
        ]));
        let first_endpoint = serve_blob_file(first_bytes.clone()).await;
        let second_endpoint = serve_blob_file(second_bytes.clone()).await;
        let cache_context = FileIOCacheContext::from_props(&HashMap::new()).unwrap();
        let original = FileIOBuilder::new("fs")
            .with_cache_context(cache_context)
            .build()
            .unwrap()
            .with_provider(Arc::new(FixedBlobProvider(blob_oss_operator(
                &first_endpoint,
            ))));
        let replacement =
            original
                .clone()
                .with_provider(Arc::new(FixedBlobProvider(blob_oss_operator(
                    &second_endpoint,
                ))));
        let path = "oss://bucket/data.blob";
        let first = IndexedBlobReader::open(
            Box::new(original.new_input(path).unwrap().reader().await.unwrap()),
            first_bytes.len() as u64,
            path.to_string(),
            false,
        )
        .await
        .unwrap();
        let second = IndexedBlobReader::open(
            Box::new(replacement.new_input(path).unwrap().reader().await.unwrap()),
            second_bytes.len() as u64,
            path.to_string(),
            false,
        )
        .await
        .unwrap();
        assert!(matches!(
            first.read_positions(&[0]).await.unwrap().as_slice(),
            [BlobReadValue::Null]
        ));
        assert!(matches!(
            second.read_positions(&[0]).await.unwrap().as_slice(),
            [BlobReadValue::Value(bytes)] if bytes.as_ref() == value
        ));
    }

    #[tokio::test]
    async fn test_blob_array_reader_reads_java_fixture() {
        let read_fields = vec![DataField::new(
            0,
            "payloads".to_string(),
            DataType::Array(ArrayType::new(DataType::Blob(BlobType::new()))),
        )];
        let file_bytes = load_blob_fixture("blob-array.blob");

        let batches = BlobFormatReader::new(String::new(), false)
            .read_batch_stream(
                Box::new(BytesFileRead(Bytes::from(file_bytes.clone()))),
                file_bytes.len() as u64,
                &read_fields,
                None,
                Some(2),
                None,
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(batches.len(), 2);
        assert_eq!(
            collect_blob_array_values(&batches[0]),
            vec![
                Some(vec![Some(b"hello".to_vec()), None, Some(b"world".to_vec())]),
                None,
            ]
        );
        assert_eq!(
            collect_blob_array_values(&batches[1]),
            vec![None, Some(Vec::new())]
        );

        let selected = BlobFormatReader::new(String::new(), false)
            .read_batch_stream(
                Box::new(BytesFileRead(Bytes::from(file_bytes.clone()))),
                file_bytes.len() as u64,
                &read_fields,
                None,
                Some(1),
                Some(vec![RowRange::new(0, 0), RowRange::new(3, 3)]),
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(selected.len(), 2);
        assert_eq!(
            collect_blob_array_values(&selected[0]),
            vec![Some(vec![
                Some(b"hello".to_vec()),
                None,
                Some(b"world".to_vec()),
            ])]
        );
        assert_eq!(
            collect_blob_array_values(&selected[1]),
            vec![Some(Vec::new())]
        );
    }

    #[tokio::test]
    async fn test_blob_map_reader_returns_inline_values_and_descriptors() {
        let file_path = "file:///tmp/map-values-and-descriptors.blob";
        let payload = build_blob_map_payload(&[
            ("video", Some(b"alpha")),
            ("thumbnail", None),
            ("empty", Some(b"")),
        ]);
        let file_bytes = blob_test_utils::build_blob_file_bytes(&[Some(payload.as_slice()), None]);
        let fields = blob_map_read_fields();

        let inline = BlobFormatReader::new(file_path.to_string(), false)
            .read_batch_stream(
                Box::new(BytesFileRead(Bytes::from(file_bytes.clone()))),
                file_bytes.len() as u64,
                &fields,
                None,
                None,
                None,
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            collect_blob_map_values(&inline[0]),
            vec![
                Some(vec![
                    ("video".to_string(), Some(b"alpha".to_vec())),
                    ("thumbnail".to_string(), None),
                    ("empty".to_string(), Some(Vec::new())),
                ]),
                None,
            ]
        );

        let descriptors = BlobFormatReader::new(file_path.to_string(), true)
            .read_batch_stream(
                Box::new(BytesFileRead(Bytes::from(file_bytes.clone()))),
                file_bytes.len() as u64,
                &fields,
                None,
                None,
                None,
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let rows = collect_blob_map_values(&descriptors[0]);
        let entries = rows[0].as_ref().unwrap();
        let video = BlobDescriptor::deserialize(entries[0].1.as_ref().unwrap()).unwrap();
        assert_eq!(video.uri(), file_path);
        assert_eq!(video.length(), 5);
        assert!(entries[1].1.is_none());
        let empty = BlobDescriptor::deserialize(entries[2].1.as_ref().unwrap()).unwrap();
        assert_eq!(empty.length(), 0);
    }

    #[tokio::test]
    async fn test_inline_blob_map_reader_coalesces_adjacent_entries() {
        let first = build_blob_map_payload(&[("first", Some(b"alpha"))]);
        let second = build_blob_map_payload(&[("second", Some(b"beta"))]);
        let file_bytes = blob_test_utils::build_blob_file_bytes(&[
            Some(first.as_slice()),
            Some(second.as_slice()),
        ]);
        let reader = TrackingFileRead::new(Bytes::from(file_bytes.clone()));

        let batches = BlobFormatReader::new(String::new(), false)
            .read_batch_stream(
                Box::new(reader.clone()),
                file_bytes.len() as u64,
                &blob_map_read_fields(),
                None,
                None,
                None,
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(
            collect_blob_map_values(&batches[0]),
            vec![
                Some(vec![("first".to_string(), Some(b"alpha".to_vec()))]),
                Some(vec![("second".to_string(), Some(b"beta".to_vec()))]),
            ]
        );
        let payload_end = BLOB_ENTRY_OVERHEAD * 2 + first.len() as u64 + second.len() as u64;
        let payload_reads = reader
            .ranges()
            .into_iter()
            .skip(1) // The bounded index prefetch may include payload bytes.
            .filter(|range| range.start < payload_end && range.end > 0)
            .collect::<Vec<_>>();
        assert_eq!(payload_reads, vec![0..payload_end]);
    }

    #[tokio::test]
    async fn test_blob_map_descriptor_read_skips_values() {
        let file_path = "file:///tmp/map-descriptor-skip-values.blob";
        let payload =
            build_blob_map_payload(&[("first", Some(b"alpha")), ("second", Some(b"beta"))]);
        let file_bytes = blob_test_utils::build_blob_file_bytes(&[Some(payload.as_slice())]);
        let reader = TrackingFileRead::new(Bytes::from(file_bytes.clone()));
        let batches = BlobFormatReader::new(file_path.to_string(), true)
            .read_batch_stream(
                Box::new(reader.clone()),
                file_bytes.len() as u64,
                &blob_map_read_fields(),
                None,
                None,
                None,
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        for (_, descriptor) in collect_blob_map_values(&batches[0])[0].as_ref().unwrap() {
            let descriptor = BlobDescriptor::deserialize(descriptor.as_ref().unwrap()).unwrap();
            let value_range =
                descriptor.offset() as u64..(descriptor.offset() + descriptor.length()) as u64;
            assert!(reader
                .ranges()
                .iter()
                .skip(1) // Exclude the bounded index prefetch.
                .all(|range| range.end <= value_range.start || range.start >= value_range.end));
        }
    }

    #[tokio::test]
    async fn test_blob_map_descriptor_merges_adjacent_indexes() {
        let payload =
            build_blob_map_payload(&[("first", Some(b"alpha")), ("second", Some(b"beta"))]);
        let reader = TrackingFileRead::new(Bytes::from(payload.clone()));
        let key_type = DataType::VarChar(VarCharType::new(VarCharType::MAX_LENGTH).unwrap());

        read_blob_map_entry(&reader, 0..payload.len() as u64, "", true, &key_type)
            .await
            .unwrap();

        assert_eq!(reader.ranges().len(), 4);
        assert_eq!(reader.max_in_flight(), 1);
    }

    #[tokio::test]
    async fn test_blob_map_descriptor_honors_configured_parallelism() {
        let payloads = ["first", "second", "third"]
            .into_iter()
            .map(|key| build_blob_map_payload(&[(key, Some(b"value"))]))
            .collect::<Vec<_>>();
        let rows = payloads
            .iter()
            .map(|payload| Some(payload.as_slice()))
            .collect::<Vec<_>>();
        let file_bytes = blob_test_utils::build_blob_file_bytes(&rows);

        for parallelism in [1, 2] {
            let reader = TrackingFileRead::new(Bytes::from(file_bytes.clone()));
            let batches = BlobFormatReader::new(String::new(), true)
                .with_blob_parallelism(parallelism)
                .read_batch_stream(
                    Box::new(reader.clone()),
                    file_bytes.len() as u64,
                    &blob_map_read_fields(),
                    None,
                    Some(rows.len()),
                    None,
                )
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();

            assert_eq!(collect_blob_map_values(&batches[0]).len(), rows.len());
            assert_eq!(reader.max_in_flight(), parallelism);
        }
    }

    #[tokio::test]
    async fn test_inline_blob_map_reader_rejects_crc_mismatch() {
        let payload = build_blob_map_payload(&[("key", Some(b"value"))]);
        let mut file_bytes = blob_test_utils::build_blob_file_bytes(&[Some(payload.as_slice())]);
        let value_offset =
            (BLOB_INLINE_HEADER_SIZE + BLOB_MAP_HEADER_SIZE + "key".len() as u64) as usize;
        file_bytes[value_offset] ^= 0xff;

        let stream = BlobFormatReader::new(String::new(), false)
            .read_batch_stream(
                Box::new(BytesFileRead(Bytes::from(file_bytes.clone()))),
                file_bytes.len() as u64,
                &blob_map_read_fields(),
                None,
                None,
                None,
            )
            .await
            .unwrap();
        let error = stream.try_collect::<Vec<_>>().await.unwrap_err();
        assert_data_invalid(error, "CRC32 mismatch");
    }

    #[tokio::test]
    async fn test_inline_blob_map_reader_accepts_large_binary_value_data() {
        let value_length = i32::MAX as u64 + 1;
        let (reader, payload_range) = sparse_blob_map_entry(&[0], &[value_length as i64]);
        let key_type = DataType::VarChar(VarCharType::new(VarCharType::MAX_LENGTH).unwrap());

        let _error = read_blob_map_entry(&reader, payload_range.clone(), "", false, &key_type)
            .await
            .unwrap_err();

        assert!(
            reader.ranges().contains(&blob_entry_range(&payload_range)),
            "LargeBinary MAP values must not be rejected by the i32 Arrow Binary limit"
        );
    }

    #[tokio::test]
    async fn test_blob_map_reader_rejects_oversized_key_before_data_read() {
        let key_length = i32::MAX as u64 + 1;
        let (reader, payload_range) = sparse_blob_map_entry(&[key_length as i64], &[0]);
        let key_type = DataType::VarChar(VarCharType::new(VarCharType::MAX_LENGTH).unwrap());

        let error = read_blob_map_entry(&reader, payload_range.clone(), "", false, &key_type)
            .await
            .unwrap_err();

        assert_eq!(reader.ranges().len(), 3);
        assert!(!reader.ranges().contains(&blob_entry_range(&payload_range)));
        assert_data_invalid(error, "too large");
    }

    #[tokio::test]
    async fn test_blob_map_reader_rejects_invalid_fixed_key_before_data_read() {
        let key_length = i32::MAX as u64 + 1;
        let (reader, payload_range) = sparse_blob_map_entry(&[key_length as i64], &[0]);
        let key_type = DataType::Int(crate::spec::IntType::new());

        let error = read_blob_map_entry(&reader, payload_range.clone(), "", false, &key_type)
            .await
            .unwrap_err();

        assert_eq!(reader.ranges().len(), 3);
        assert!(!reader.ranges().contains(&blob_entry_range(&payload_range)));
        assert_data_invalid(error, "fixed-width key length");
    }

    #[tokio::test]
    async fn test_blob_map_reader_rejects_null_key_for_arrow() {
        let (reader, payload_range) = sparse_blob_map_entry(&[-1], &[0]);
        let key_type = DataType::VarChar(VarCharType::new(VarCharType::MAX_LENGTH).unwrap());

        let error = read_blob_map_entry(&reader, payload_range, "", false, &key_type)
            .await
            .unwrap_err();

        assert_data_invalid(error, "null keys cannot be represented by Arrow");
    }

    #[test]
    fn test_blob_map_batch_rejects_oversized_binary_key_data() {
        let error = checked_arrow_binary_data_length(
            i32::MAX as u64,
            1,
            "MAP<BINARY, BLOB> batch key data",
        )
        .unwrap_err();

        assert_data_invalid(error, "too large");
    }

    #[tokio::test]
    async fn test_inline_blob_array_reader_rejects_payload_crc_mismatch() {
        let payload = build_blob_array_payload(b"helloworld", &[5, -1, 5]);
        let mut file_bytes = blob_test_utils::build_blob_file_bytes(&[Some(payload.as_slice())]);
        let first_element_offset = (BLOB_INLINE_HEADER_SIZE + BLOB_ARRAY_HEADER_SIZE) as usize;
        file_bytes[first_element_offset] ^= 0xff;

        let stream = BlobFormatReader::new(String::new(), false)
            .read_batch_stream(
                Box::new(BytesFileRead(Bytes::from(file_bytes.clone()))),
                file_bytes.len() as u64,
                &blob_array_read_fields(),
                None,
                None,
                None,
            )
            .await
            .unwrap();
        let error = stream.try_collect::<Vec<_>>().await.unwrap_err();

        assert_data_invalid(error, "CRC32 mismatch");
    }

    #[tokio::test]
    async fn test_inline_blob_array_reader_accepts_large_binary_element_data() {
        let element_data_length = i32::MAX as u64 + 1;
        let element_index = encode_delta_varints_write(&[element_data_length as i64]);
        let payload_length =
            BLOB_ARRAY_MIN_PAYLOAD_SIZE + element_data_length + element_index.len() as u64;
        let payload_range = BLOB_INLINE_HEADER_SIZE..BLOB_INLINE_HEADER_SIZE + payload_length;
        let mut header = Vec::with_capacity(BLOB_ARRAY_HEADER_SIZE as usize);
        header.extend_from_slice(&BLOB_ARRAY_MAGIC_NUMBER.to_le_bytes());
        header.push(BLOB_ARRAY_VERSION);
        header.extend_from_slice(&1i32.to_le_bytes());
        let reader = SparseFileRead::new(vec![
            (
                payload_range.start..payload_range.start + BLOB_ARRAY_HEADER_SIZE,
                Bytes::from(header),
            ),
            (
                payload_range.end - BLOB_ARRAY_INDEX_LENGTH_SIZE..payload_range.end,
                Bytes::copy_from_slice(&(element_index.len() as i32).to_le_bytes()),
            ),
        ]);

        let _error = read_inline_blob_array_entry(&reader, payload_range.clone())
            .await
            .unwrap_err();

        assert!(
            reader.ranges().contains(&blob_entry_range(&payload_range)),
            "LargeBinary ARRAY elements must not be rejected by the i32 Arrow Binary limit"
        );
    }

    #[tokio::test]
    async fn test_blob_array_reader_builds_exact_descriptors_without_payload_reads() {
        let file_path = "file:///tmp/blob-array.blob";
        let file_bytes = load_blob_fixture("blob-array.blob");
        let reader = TrackingFileRead::new(Bytes::from(file_bytes.clone()));

        let batches = BlobFormatReader::new(file_path.to_string(), true)
            .read_batch_stream(
                Box::new(reader.clone()),
                file_bytes.len() as u64,
                &blob_array_read_fields(),
                None,
                Some(8),
                None,
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let rows = collect_blob_array_values(&batches[0]);
        let first = rows[0].as_ref().unwrap();
        assert!(first[1].is_none());
        let hello = BlobDescriptor::deserialize(first[0].as_ref().unwrap()).unwrap();
        let world = BlobDescriptor::deserialize(first[2].as_ref().unwrap()).unwrap();
        assert_eq!(
            (hello.uri(), hello.offset(), hello.length()),
            (file_path, 13, 5)
        );
        assert_eq!(
            (world.uri(), world.offset(), world.length()),
            (file_path, 18, 5)
        );
        assert_eq!(rows[1], None);
        assert_eq!(rows[2], None);
        assert_eq!(rows[3], Some(Vec::new()));

        assert!(
            !reader
                .ranges()
                .iter()
                .skip(1) // Exclude the bounded index prefetch.
                .any(|range| range.start < 23 && range.end > 13),
            "descriptor mode must not read element payload bytes beyond the index prefetch"
        );
    }

    #[tokio::test]
    async fn test_blob_array_reader_reads_each_row_element_data_once() {
        let file_bytes = load_blob_fixture("blob-array.blob");
        let reader = TrackingFileRead::new(Bytes::from(file_bytes.clone()));

        BlobFormatReader::new(String::new(), false)
            .read_batch_stream(
                Box::new(reader.clone()),
                file_bytes.len() as u64,
                &blob_array_read_fields(),
                None,
                Some(8),
                None,
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let element_data_range = 13..23;
        let expected_entry_end =
            BLOB_ENTRY_OVERHEAD + build_blob_array_payload(b"helloworld", &[5, -1, 5]).len() as u64;
        let overlapping_reads = reader
            .ranges()
            .into_iter()
            .skip(1) // Exclude the bounded index prefetch.
            .filter(|range| {
                range.start < element_data_range.end && element_data_range.start < range.end
            })
            .collect::<Vec<_>>();
        assert_eq!(overlapping_reads.len(), 1);
        assert_eq!(overlapping_reads[0].start, 0);
        assert!(overlapping_reads[0].end >= expected_entry_end);
    }

    #[tokio::test]
    async fn test_blob_array_reader_preserves_order_after_coalescing() {
        let payloads = (0_u8..12)
            .map(|value| build_blob_array_payload(&[value], &[1]))
            .collect::<Vec<_>>();
        let rows = payloads
            .iter()
            .map(|payload| Some(payload.as_slice()))
            .collect::<Vec<_>>();
        let file_bytes = blob_test_utils::build_blob_file_bytes(&rows);
        let reader = TrackingFileRead::new(Bytes::from(file_bytes.clone()));

        let batches = BlobFormatReader::new(String::new(), false)
            .read_batch_stream(
                Box::new(reader.clone()),
                file_bytes.len() as u64,
                &blob_array_read_fields(),
                None,
                Some(12),
                None,
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(
            collect_blob_array_values(&batches[0]),
            (0_u8..12)
                .map(|value| Some(vec![Some(vec![value])]))
                .collect::<Vec<_>>()
        );
        assert_eq!(reader.max_in_flight(), 1);
    }

    #[tokio::test]
    async fn test_blob_reader_honors_configured_parallelism() {
        let entry_length = BLOB_ENTRY_OVERHEAD + 1;
        let stride = entry_length + BLOB_RANGE_MERGE_GAP + 1;
        let mut bytes = vec![0; (3 * stride) as usize];
        let mut planned_reads = Vec::new();
        for value in 0_u8..3 {
            let entry = blob_test_utils::build_blob_file_bytes(&[Some(&[value])]);
            let offset = u64::from(value) * stride;
            bytes[offset as usize..(offset + entry_length) as usize]
                .copy_from_slice(&entry[..entry_length as usize]);
            planned_reads.push(PlannedBlobRead::Entry(offset..offset + entry_length));
        }
        let reader = TrackingFileRead::new(Bytes::from(bytes));

        let values = fetch_blob_values(&reader, planned_reads, 2).await.unwrap();
        let actual = values
            .into_iter()
            .map(|value| match value {
                BlobReadValue::Value(value) => value[0],
                other => panic!("Expected scalar BLOB value, got {other:?}"),
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![0, 1, 2]);
        assert_eq!(reader.max_in_flight(), 2);
    }

    #[tokio::test]
    async fn test_blob_reader_treats_java_placeholders_as_null() {
        let read_fields = vec![DataField::new(
            0,
            "payload".to_string(),
            DataType::Blob(BlobType::new()),
        )];
        let file_bytes = load_blob_fixture("blob-placeholder.blob");

        let batches = BlobFormatReader::new(String::new(), false)
            .read_batch_stream(
                Box::new(BytesFileRead(Bytes::from(file_bytes.clone()))),
                file_bytes.len() as u64,
                &read_fields,
                None,
                Some(2),
                None,
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(batches.len(), 2);
        assert_eq!(collect_binary_values(&batches[0]), vec![None, None]);
        assert_eq!(
            collect_binary_values(&batches[1]),
            vec![Some(b"latest-3".to_vec()), None]
        );
    }

    #[tokio::test]
    async fn test_blob_reader_coalesces_adjacent_payload_reads() {
        let read_fields = vec![DataField::new(
            0,
            "payload".to_string(),
            DataType::Blob(BlobType::new()),
        )];
        let file_bytes = load_blob_fixture("blob-basic.blob");
        let reader = TrackingFileRead::new(Bytes::from(file_bytes.clone()));

        let batches = BlobFormatReader::new(String::new(), false)
            .read_batch_stream(
                Box::new(reader.clone()),
                file_bytes.len() as u64,
                &read_fields,
                None,
                Some(8),
                None,
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(
            collect_binary_values(&batches[0]),
            vec![
                Some(b"hello".to_vec()),
                None,
                Some(b"world".to_vec()),
                Some(Vec::new()),
            ]
        );
        let payload_end = basic_blob_rows()
            .into_iter()
            .flatten()
            .map(|value| BLOB_ENTRY_OVERHEAD + value.len() as u64)
            .sum::<u64>();
        let payload_reads = reader
            .ranges()
            .into_iter()
            .skip(1) // The bounded index prefetch may include payload bytes.
            .filter(|range| range.start < payload_end && range.end > 0)
            .collect::<Vec<_>>();
        assert_eq!(payload_reads, vec![0..payload_end]);
    }

    #[tokio::test]
    async fn test_blob_reader_limits_sparse_read_amplification() {
        let selected = vec![b's'; 4 * 1024];
        let skipped = vec![b'g'; 64 * 1024];
        let rows = vec![
            Some(selected.as_slice()),
            Some(skipped.as_slice()),
            Some(selected.as_slice()),
            Some(skipped.as_slice()),
            Some(selected.as_slice()),
        ];
        let file_bytes = blob_test_utils::build_blob_file_bytes(&rows);
        let selected_entry_length = BLOB_ENTRY_OVERHEAD + selected.len() as u64;
        let skipped_entry_length = BLOB_ENTRY_OVERHEAD + skipped.len() as u64;
        let selected_ranges = vec![
            0..selected_entry_length,
            selected_entry_length + skipped_entry_length
                ..2 * selected_entry_length + skipped_entry_length,
            2 * (selected_entry_length + skipped_entry_length)
                ..3 * selected_entry_length + 2 * skipped_entry_length,
        ];
        let reader = TrackingFileRead::new(Bytes::from(file_bytes));
        let planned_reads = selected_ranges
            .iter()
            .cloned()
            .map(PlannedBlobRead::Entry)
            .collect();

        let values = fetch_blob_values(&reader, planned_reads, 1).await.unwrap();

        assert_eq!(values.len(), 3);
        assert!(values.into_iter().all(|value| matches!(
            value,
            BlobReadValue::Value(bytes) if bytes.as_ref() == selected.as_slice()
        )));
        let ranges = reader.ranges();
        assert_eq!(ranges, selected_ranges);
        let read_bytes = ranges
            .iter()
            .map(|range| range.end - range.start)
            .sum::<u64>();
        assert!(read_bytes <= 2 * selected_entry_length * 3);
    }

    #[test]
    fn test_blob_range_merge_rejects_reported_sparse_layout() {
        let entry_length = BLOB_ENTRY_OVERHEAD + 4 * 1024;
        let stride = entry_length + BLOB_RANGE_MERGE_GAP;
        let reads = (0..128)
            .map(|result_index| {
                let start = result_index as u64 * stride;
                BlobEntryRead {
                    result_index,
                    range: start..start + entry_length,
                }
            })
            .collect();

        let merged = merge_blob_entry_reads(reads);

        assert_eq!(merged.len(), 128);
        assert_eq!(
            merged
                .iter()
                .map(|read| read.range.end - read.range.start)
                .sum::<u64>(),
            128 * entry_length
        );
    }

    #[tokio::test]
    async fn test_blob_reader_copies_selected_entries_from_gapped_span() {
        let first = vec![b'a'; 4 * 1024];
        let skipped = vec![b'g'; 1024];
        let second = vec![b'b'; 4 * 1024];
        let rows = vec![
            Some(first.as_slice()),
            Some(skipped.as_slice()),
            Some(second.as_slice()),
        ];
        let file_bytes = blob_test_utils::build_blob_file_bytes(&rows);
        let first_end = BLOB_ENTRY_OVERHEAD + first.len() as u64;
        let second_start = first_end + BLOB_ENTRY_OVERHEAD + skipped.len() as u64;
        let second_end = second_start + BLOB_ENTRY_OVERHEAD + second.len() as u64;
        let reader = TrackingFileRead::new(Bytes::from(file_bytes));

        let values = fetch_blob_values(
            &reader,
            vec![
                PlannedBlobRead::Entry(0..first_end),
                PlannedBlobRead::Entry(second_start..second_end),
            ],
            1,
        )
        .await
        .unwrap();

        assert_eq!(reader.ranges(), vec![0..second_end]);
        for value in values {
            let BlobReadValue::Value(value) = value else {
                panic!("Expected scalar BLOB value");
            };
            assert!(
                value.is_unique(),
                "selected values must not retain the merged span buffer"
            );
        }
    }

    #[test]
    fn test_blob_reader_test_helper_matches_java_fixture() {
        let generated = blob_test_utils::build_blob_file_bytes(&basic_blob_rows());

        assert_eq!(generated, load_blob_fixture("blob-basic.blob"));
    }

    #[test]
    fn test_blob_reader_test_helper_matches_java_placeholder_fixture() {
        use blob_test_utils::BlobFixtureValue::{Null, Placeholder, Value};

        let generated = blob_test_utils::build_blob_file_bytes_with_values(&[
            Placeholder,
            Null,
            Value(b"latest-3"),
            Placeholder,
        ]);

        assert_eq!(generated, load_blob_fixture("blob-placeholder.blob"));
    }

    #[test]
    fn test_blob_array_fixture_matches_java_writer_layout() {
        use blob_test_utils::BlobFixtureValue::{Null, Placeholder, Value};

        let first = build_blob_array_payload(b"helloworld", &[5, -1, 5]);
        let empty = build_blob_array_payload(b"", &[]);
        let generated = blob_test_utils::build_blob_file_bytes_with_values(&[
            Value(first.as_slice()),
            Null,
            Placeholder,
            Value(empty.as_slice()),
        ]);

        assert_eq!(generated, load_blob_fixture("blob-array.blob"));
    }

    #[tokio::test]
    async fn test_blob_reader_supports_empty_projection() {
        let reader = BlobFormatReader::new(String::new(), false);
        let file_bytes = load_blob_fixture("blob-basic.blob");

        let batches = reader
            .read_batch_stream(
                Box::new(BytesFileRead(Bytes::from(file_bytes.clone()))),
                file_bytes.len() as u64,
                &[],
                None,
                Some(2),
                None,
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(batches.len(), 2);
        assert!(batches[0].columns().is_empty());
        assert_eq!(batches[0].num_rows(), 2);
        assert!(batches[1].columns().is_empty());
        assert_eq!(batches[1].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_blob_reader_rejects_out_of_range_selection() {
        let reader = BlobFormatReader::new(String::new(), false);
        let file_bytes = load_blob_fixture("blob-basic.blob");
        let read_fields = vec![DataField::new(
            0,
            "payload".to_string(),
            DataType::Blob(BlobType::new()),
        )];

        let result = reader
            .read_batch_stream(
                Box::new(BytesFileRead(Bytes::from(file_bytes.clone()))),
                file_bytes.len() as u64,
                &read_fields,
                None,
                None,
                Some(vec![RowRange::new(0, 4)]),
            )
            .await;

        assert!(
            matches!(result, Err(Error::DataInvalid { message, .. }) if message.contains("exceeds available rows"))
        );
    }

    #[tokio::test]
    async fn test_blob_reader_rejects_wrong_field_family() {
        let reader = BlobFormatReader::new(String::new(), false);
        let file_bytes = load_blob_fixture("blob-basic.blob");
        let read_fields = vec![DataField::new(
            0,
            "payload".to_string(),
            DataType::Int(crate::spec::IntType::new()),
        )];

        let result = reader
            .read_batch_stream(
                Box::new(BytesFileRead(Bytes::from(file_bytes.clone()))),
                file_bytes.len() as u64,
                &read_fields,
                None,
                None,
                None,
            )
            .await;

        assert!(
            matches!(result, Err(Error::DataInvalid { message, .. }) if message.contains("Blob, Array<Blob>, or Map<X, Blob> field"))
        );
    }

    #[tokio::test]
    async fn test_blob_array_reader_rejects_nested_array() {
        let file_bytes = load_blob_fixture("blob-array.blob");
        let read_fields = vec![DataField::new(
            0,
            "payloads".to_string(),
            DataType::Array(ArrayType::new(DataType::Array(ArrayType::new(
                DataType::Blob(BlobType::new()),
            )))),
        )];

        let result = BlobFormatReader::new(String::new(), false)
            .read_batch_stream(
                Box::new(BytesFileRead(Bytes::from(file_bytes.clone()))),
                file_bytes.len() as u64,
                &read_fields,
                None,
                None,
                None,
            )
            .await;

        assert!(
            matches!(result, Err(Error::DataInvalid { message, .. }) if message.contains("Blob, Array<Blob>, or Map<X, Blob>"))
        );
    }

    #[tokio::test]
    async fn test_blob_array_reader_rejects_invalid_header() {
        let mut invalid_magic = build_blob_array_payload(b"a", &[1]);
        invalid_magic[..4].copy_from_slice(&0_i32.to_le_bytes());
        assert_data_invalid(
            read_blob_array_payload_error(invalid_magic).await,
            "magic number",
        );

        let mut unsupported_version = build_blob_array_payload(b"a", &[1]);
        unsupported_version[4] = 2;
        assert!(
            matches!(read_blob_array_payload_error(unsupported_version).await, Error::Unsupported { message } if message.contains("payload version"))
        );

        let mut negative_count = build_blob_array_payload(b"a", &[1]);
        negative_count[5..9].copy_from_slice(&(-1_i32).to_le_bytes());
        assert_data_invalid(
            read_blob_array_payload_error(negative_count).await,
            "element count",
        );

        assert_data_invalid(
            read_blob_array_payload_error(vec![0; BLOB_ARRAY_MIN_PAYLOAD_SIZE as usize - 1]).await,
            "too small",
        );
    }

    #[tokio::test]
    async fn test_blob_array_reader_rejects_invalid_index() {
        let mut negative_length = build_blob_array_payload(b"a", &[1]);
        set_blob_array_index_length(&mut negative_length, -1);
        assert_data_invalid(
            read_blob_array_payload_error(negative_length).await,
            "index length",
        );

        let mut oversized_length = build_blob_array_payload(b"a", &[1]);
        set_blob_array_index_length(&mut oversized_length, 3);
        assert_data_invalid(
            read_blob_array_payload_error(oversized_length).await,
            "index length",
        );

        let mut count_exceeds_index = build_blob_array_payload(b"a", &[1]);
        count_exceeds_index[5..9].copy_from_slice(&2_i32.to_le_bytes());
        assert_data_invalid(
            read_blob_array_payload_error(count_exceeds_index).await,
            "count exceeds",
        );

        let mut truncated_varint = build_blob_array_payload(b"a", &[1]);
        let index_position = truncated_varint.len() - BLOB_ARRAY_INDEX_LENGTH_SIZE as usize - 1;
        truncated_varint[index_position] = 0x80;
        assert_data_invalid(
            read_blob_array_payload_error(truncated_varint).await,
            "element index",
        );

        let mut count_mismatch = build_blob_array_payload(b"ab", &[1, 1]);
        count_mismatch[5..9].copy_from_slice(&1_i32.to_le_bytes());
        assert_data_invalid(
            read_blob_array_payload_error(count_mismatch).await,
            "does not match index value count",
        );
    }

    #[tokio::test]
    async fn test_blob_array_reader_rejects_invalid_element_bounds() {
        assert_data_invalid(
            read_blob_array_payload_error(build_blob_array_payload(b"", &[-2])).await,
            "element length",
        );
        assert_data_invalid(
            read_blob_array_payload_error(build_blob_array_payload(b"a", &[2])).await,
            "exceed the payload data length",
        );
        assert_data_invalid(
            read_blob_array_payload_error(build_blob_array_payload(b"ab", &[1])).await,
            "do not match the payload data length",
        );
    }

    #[tokio::test]
    async fn test_blob_array_reader_preserves_empty_and_null_elements() {
        let payload = build_blob_array_payload(b"", &[0, -1]);
        let file_bytes = blob_test_utils::build_blob_file_bytes(&[Some(payload.as_slice())]);

        let batches = BlobFormatReader::new(String::new(), false)
            .read_batch_stream(
                Box::new(BytesFileRead(Bytes::from(file_bytes.clone()))),
                file_bytes.len() as u64,
                &blob_array_read_fields(),
                None,
                None,
                None,
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(
            collect_blob_array_values(&batches[0]),
            vec![Some(vec![Some(Vec::new()), None])]
        );
    }

    #[tokio::test]
    async fn test_blob_reader_rejects_unsupported_version() {
        let mut file_bytes = blob_test_utils::build_blob_file_bytes(&basic_blob_rows());
        let last = file_bytes.len() - 1;
        file_bytes[last] = 2;

        let result = BlobFormatReader::new(String::new(), false)
            .read_batch_stream(
                Box::new(BytesFileRead(Bytes::from(file_bytes.clone()))),
                file_bytes.len() as u64,
                &[DataField::new(
                    0,
                    "payload".to_string(),
                    DataType::Blob(BlobType::new()),
                )],
                None,
                None,
                None,
            )
            .await;

        assert!(
            matches!(result, Err(Error::Unsupported { message }) if message.contains("footer version"))
        );
    }

    #[tokio::test]
    async fn test_blob_reader_rejects_truncated_entry() {
        let mut file_bytes = blob_test_utils::build_blob_file_bytes(&basic_blob_rows());
        let footer_start = file_bytes.len() - BLOB_FOOTER_SIZE as usize;
        let index_length = i32::from_le_bytes(
            file_bytes[footer_start..footer_start + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let index_start = footer_start - index_length;
        let lengths = decode_delta_varints(&file_bytes[index_start..footer_start]).unwrap();
        let mut replacement_lengths = lengths.clone();
        replacement_lengths[0] = 15;
        let replacement = blob_test_utils::encode_delta_varints(&replacement_lengths);
        file_bytes.splice(index_start..footer_start, replacement.iter().copied());
        let footer_start = file_bytes.len() - BLOB_FOOTER_SIZE as usize;
        file_bytes[footer_start..footer_start + 4]
            .copy_from_slice(&(replacement.len() as i32).to_le_bytes());

        let result = BlobFormatReader::new(String::new(), false)
            .read_batch_stream(
                Box::new(BytesFileRead(Bytes::from(file_bytes.clone()))),
                file_bytes.len() as u64,
                &[DataField::new(
                    0,
                    "payload".to_string(),
                    DataType::Blob(BlobType::new()),
                )],
                None,
                None,
                None,
            )
            .await;

        assert!(!lengths.is_empty());
        assert!(
            matches!(result, Err(Error::DataInvalid { message, .. }) if message.contains("minimum overhead"))
        );
    }

    #[tokio::test]
    async fn test_blob_reader_rejects_invalid_entry_magic() {
        let payload = b"hello";
        let mut file_bytes = blob_test_utils::build_blob_file_bytes(&[Some(payload.as_slice())]);
        file_bytes[..BLOB_INLINE_HEADER_SIZE as usize].copy_from_slice(&0_i32.to_le_bytes());
        rewrite_first_blob_entry_crc(&mut file_bytes, payload.len());

        let error = read_scalar_blob_values(file_bytes).await.unwrap_err();
        assert_data_invalid(error, "magic");
    }

    #[tokio::test]
    async fn test_blob_reader_rejects_mismatched_entry_length() {
        let payload = b"hello";
        let mut file_bytes = blob_test_utils::build_blob_file_bytes(&[Some(payload.as_slice())]);
        let length_offset = BLOB_INLINE_HEADER_SIZE as usize + payload.len();
        let mismatched_length = payload.len() as i64 + BLOB_ENTRY_OVERHEAD as i64 + 1;
        file_bytes[length_offset..length_offset + size_of::<i64>()]
            .copy_from_slice(&mismatched_length.to_le_bytes());
        rewrite_first_blob_entry_crc(&mut file_bytes, payload.len());

        let error = read_scalar_blob_values(file_bytes).await.unwrap_err();
        assert_data_invalid(error, "length mismatch");
    }

    #[tokio::test]
    async fn test_blob_reader_rejects_payload_crc_mismatch() {
        let payload = b"hello";
        let mut file_bytes = blob_test_utils::build_blob_file_bytes(&[Some(payload.as_slice())]);
        file_bytes[BLOB_INLINE_HEADER_SIZE as usize] ^= 0xff;

        let error = read_scalar_blob_values(file_bytes).await.unwrap_err();
        assert_data_invalid(error, "CRC32 mismatch");
    }

    #[tokio::test]
    async fn test_blob_reader_rejects_empty_entry_crc_mismatch() {
        let payload = b"";
        let mut file_bytes = blob_test_utils::build_blob_file_bytes(&[Some(payload.as_slice())]);
        let crc_offset = BLOB_INLINE_HEADER_SIZE as usize + size_of::<i64>();
        file_bytes[crc_offset] ^= 0xff;

        let error = read_scalar_blob_values(file_bytes).await.unwrap_err();
        assert_data_invalid(error, "CRC32 mismatch");
    }

    #[test]
    fn test_varint_encode_decode_roundtrip() {
        let values = vec![21, -1, 0, i64::MAX, i64::MIN + 1, 127, -128, 300, -300];
        for &v in &values {
            let mut buf = Vec::new();
            encode_varint(v, &mut buf);
            let (decoded, consumed) = decode_varint(&buf).unwrap();
            assert_eq!(decoded, v, "roundtrip failed for {v}");
            assert_eq!(consumed, buf.len());
        }
    }

    #[test]
    fn test_delta_varints_encode_decode_roundtrip() {
        let values = vec![21, -1, 0, 100, -50, 1000];
        let encoded = encode_delta_varints_write(&values);
        let decoded = decode_delta_varints(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_checked_blob_entry_length() {
        assert_eq!(
            checked_blob_entry_length(0).unwrap(),
            BLOB_ENTRY_OVERHEAD as i64
        );

        let max_payload = i64::MAX as u64 - BLOB_ENTRY_OVERHEAD;
        assert_eq!(checked_blob_entry_length(max_payload).unwrap(), i64::MAX);
        assert!(checked_blob_entry_length(max_payload + 1).is_err());
        assert!(checked_blob_entry_length(u64::MAX).is_err());
    }

    fn basic_blob_rows() -> [Option<&'static [u8]>; 4] {
        [
            Some(&b"hello"[..]),
            None,
            Some(&b"world"[..]),
            Some(&b""[..]),
        ]
    }

    fn blob_array_read_fields() -> Vec<DataField> {
        vec![DataField::new(
            0,
            "payloads".to_string(),
            DataType::Array(ArrayType::new(DataType::Blob(BlobType::new()))),
        )]
    }

    fn blob_map_read_fields() -> Vec<DataField> {
        vec![DataField::new(
            0,
            "payloads".to_string(),
            DataType::Map(MapType::new(
                DataType::VarChar(VarCharType::new(VarCharType::MAX_LENGTH).unwrap()),
                DataType::Blob(BlobType::new()),
            )),
        )]
    }

    fn build_blob_array_payload(element_data: &[u8], element_lengths: &[i64]) -> Vec<u8> {
        let index = encode_delta_varints_write(element_lengths);
        let mut payload = Vec::with_capacity(
            BLOB_ARRAY_MIN_PAYLOAD_SIZE as usize + element_data.len() + index.len(),
        );
        payload.extend_from_slice(&BLOB_ARRAY_MAGIC_NUMBER.to_le_bytes());
        payload.push(BLOB_ARRAY_VERSION);
        payload.extend_from_slice(&(element_lengths.len() as i32).to_le_bytes());
        payload.extend_from_slice(element_data);
        payload.extend_from_slice(&index);
        payload.extend_from_slice(&(index.len() as i32).to_le_bytes());
        payload
    }

    fn build_blob_map_payload(entries: &[(&str, Option<&[u8]>)]) -> Vec<u8> {
        let key_lengths = entries
            .iter()
            .map(|(key, _)| key.len() as i64)
            .collect::<Vec<_>>();
        let value_lengths = entries
            .iter()
            .map(|(_, value)| value.map_or(-1, |value| value.len() as i64))
            .collect::<Vec<_>>();
        let key_index = encode_delta_varints_write(&key_lengths);
        let value_index = encode_delta_varints_write(&value_lengths);
        let mut payload = Vec::new();
        payload.extend_from_slice(&BLOB_MAP_MAGIC_NUMBER.to_le_bytes());
        payload.push(BLOB_MAP_VERSION);
        payload.extend_from_slice(&(entries.len() as i32).to_le_bytes());
        for (key, _) in entries {
            payload.extend_from_slice(key.as_bytes());
        }
        for (_, value) in entries {
            if let Some(value) = value {
                payload.extend_from_slice(value);
            }
        }
        payload.extend_from_slice(&key_index);
        payload.extend_from_slice(&value_index);
        payload.extend_from_slice(&(key_index.len() as i32).to_le_bytes());
        payload.extend_from_slice(&(value_index.len() as i32).to_le_bytes());
        payload
    }

    fn set_blob_array_index_length(payload: &mut [u8], index_length: i32) {
        let index_length_position = payload.len() - BLOB_ARRAY_INDEX_LENGTH_SIZE as usize;
        payload[index_length_position..].copy_from_slice(&index_length.to_le_bytes());
    }

    async fn read_blob_array_payload_error(payload: Vec<u8>) -> Error {
        let file_bytes = blob_test_utils::build_blob_file_bytes(&[Some(payload.as_slice())]);
        BlobFormatReader::new(String::new(), false)
            .read_batch_stream(
                Box::new(BytesFileRead(Bytes::from(file_bytes.clone()))),
                file_bytes.len() as u64,
                &blob_array_read_fields(),
                None,
                None,
                None,
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err()
    }

    async fn read_scalar_blob_values(file_bytes: Vec<u8>) -> crate::Result<Vec<Option<Vec<u8>>>> {
        let batches = BlobFormatReader::new(String::new(), false)
            .read_batch_stream(
                Box::new(BytesFileRead(Bytes::from(file_bytes.clone()))),
                file_bytes.len() as u64,
                &[DataField::new(
                    0,
                    "payload".to_string(),
                    DataType::Blob(BlobType::new()),
                )],
                None,
                None,
                None,
            )
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        Ok(batches.iter().flat_map(collect_binary_values).collect())
    }

    async fn read_scalar_blob_file(file_io: &FileIO, path: &str) -> Vec<Option<Vec<u8>>> {
        let input = file_io.new_input(path).unwrap();
        let file_size = input.metadata().await.unwrap().size;
        let batches = BlobFormatReader::new(path.to_string(), false)
            .read_batch_stream(
                Box::new(input.reader().await.unwrap()),
                file_size,
                &[DataField::new(
                    0,
                    "payload".to_string(),
                    DataType::Blob(BlobType::new()),
                )],
                None,
                None,
                None,
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        batches.iter().flat_map(collect_binary_values).collect()
    }

    fn rewrite_first_blob_entry_crc(file_bytes: &mut [u8], payload_length: usize) {
        let crc_offset = BLOB_INLINE_HEADER_SIZE as usize + payload_length + size_of::<i64>();
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&file_bytes[..crc_offset]);
        file_bytes[crc_offset..crc_offset + size_of::<u32>()]
            .copy_from_slice(&hasher.finalize().to_le_bytes());
    }

    fn assert_data_invalid(error: Error, expected_message: &str) {
        assert!(
            matches!(error, Error::DataInvalid { message, .. } if message.contains(expected_message)),
            "expected DataInvalid containing '{expected_message}'"
        );
    }

    fn collect_binary_values(batch: &RecordBatch) -> Vec<Option<Vec<u8>>> {
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::LargeBinaryArray>()
            .unwrap();
        (0..array.len())
            .map(|idx| (!array.is_null(idx)).then(|| array.value(idx).to_vec()))
            .collect()
    }

    fn collect_blob_array_values(batch: &RecordBatch) -> Vec<Option<Vec<Option<Vec<u8>>>>> {
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .unwrap();
        (0..array.len())
            .map(|row_idx| {
                if array.is_null(row_idx) {
                    return None;
                }

                let values = array.value(row_idx);
                let values = values
                    .as_any()
                    .downcast_ref::<arrow_array::LargeBinaryArray>()
                    .unwrap();
                Some(
                    (0..values.len())
                        .map(|idx| (!values.is_null(idx)).then(|| values.value(idx).to_vec()))
                        .collect(),
                )
            })
            .collect()
    }

    type BlobMapRows = Vec<Option<Vec<(String, Option<Vec<u8>>)>>>;

    fn collect_blob_map_values(batch: &RecordBatch) -> BlobMapRows {
        let array = batch.column(0).as_any().downcast_ref::<MapArray>().unwrap();
        let keys = array.keys().as_any().downcast_ref::<StringArray>().unwrap();
        let values = array
            .values()
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        (0..array.len())
            .map(|row| {
                if array.is_null(row) {
                    return None;
                }
                let start = array.value_offsets()[row];
                let end = array.value_offsets()[row + 1];
                Some(
                    (start..end)
                        .map(|index| {
                            let index = index as usize;
                            (
                                keys.value(index).to_string(),
                                (!values.is_null(index)).then(|| values.value(index).to_vec()),
                            )
                        })
                        .collect(),
                )
            })
            .collect()
    }

    fn load_blob_fixture(name: &str) -> Vec<u8> {
        let path = format!("{}/testdata/blob/{name}", env!("CARGO_MANIFEST_DIR"));
        std::fs::read(&path).unwrap_or_else(|e| panic!("Failed to read {path}: {e}"))
    }

    fn blob_index_cache(max_size: &str) -> Arc<BlobIndexCacheContext> {
        BlobIndexCacheContext::from_props(&std::collections::HashMap::from([(
            CatalogOptions::BLOB_INDEX_CACHE_MAX_SIZE.to_string(),
            max_size.to_string(),
        )]))
        .unwrap()
    }

    fn blob_index_cache_entry_weight(key: &str, rows: usize) -> usize {
        let index = BlobFileIndex {
            entries: vec![BlobEntry::Null; rows],
        };
        FileMetadataCache::<String, BlobFileIndex>::entry_weight(
            key.len(),
            index.estimated_cache_bytes(),
        )
    }

    #[cfg(feature = "storage-oss")]
    async fn serve_blob_file(bytes: Bytes) -> String {
        async fn get_blob(State(bytes): State<Bytes>, headers: HeaderMap) -> Response<Body> {
            let range = headers
                .get(RANGE)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix("bytes="))
                .and_then(|value| value.split_once('-'))
                .and_then(|(start, end)| {
                    Some((start.parse::<usize>().ok()?, end.parse::<usize>().ok()?))
                });
            let Some((start, end)) = range else {
                return Response::builder()
                    .status(StatusCode::OK)
                    .header(CONTENT_LENGTH, bytes.len())
                    .body(Body::from(bytes))
                    .unwrap();
            };
            let body = bytes.slice(start..=end);
            Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header(CONTENT_LENGTH, body.len())
                .header(
                    CONTENT_RANGE,
                    format!("bytes {start}-{end}/{}", bytes.len()),
                )
                .body(Body::from(body))
                .unwrap()
        }

        let app = Router::new().fallback(get(get_blob)).with_state(bytes);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{address}")
    }

    #[cfg(feature = "storage-oss")]
    fn blob_oss_file_io(endpoint: &str, cache_context: FileIOCacheContext) -> FileIO {
        FileIOBuilder::new("fs")
            .with_prop("fs.oss.endpoint", endpoint)
            .with_fs_operator(blob_oss_operator(endpoint))
            .with_cache_context(cache_context)
            .build()
            .unwrap()
    }

    #[cfg(feature = "storage-oss")]
    fn blob_oss_operator(endpoint: &str) -> Operator {
        let mut config = OssConfig::default();
        config.endpoint = Some(endpoint.to_string());
        config.addressing_style = Some("path".to_string());
        config.skip_signature = true;
        Operator::new(config.into_builder().bucket("bucket"))
            .unwrap()
            .with_context(
                OperationContext::new()
                    .with_http_transport(HttpTransporter::new(ReqwestTransport::default())),
            )
    }

    #[derive(Clone)]
    struct TrackingFileRead {
        bytes: Bytes,
        cache_key: Option<String>,
        blob_index_cache: Option<Arc<BlobIndexCacheContext>>,
        fail: bool,
        in_flight: Arc<AtomicUsize>,
        max_in_flight: Arc<AtomicUsize>,
        ranges: Arc<Mutex<Vec<Range<u64>>>>,
    }

    impl TrackingFileRead {
        fn new(bytes: Bytes) -> Self {
            Self {
                bytes,
                cache_key: None,
                blob_index_cache: None,
                fail: false,
                in_flight: Arc::new(AtomicUsize::new(0)),
                max_in_flight: Arc::new(AtomicUsize::new(0)),
                ranges: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn with_blob_index_cache(
            mut self,
            cache_key: impl Into<String>,
            cache: Arc<BlobIndexCacheContext>,
        ) -> Self {
            self.cache_key = Some(cache_key.into());
            self.blob_index_cache = Some(cache);
            self
        }

        fn with_failure(mut self) -> Self {
            self.fail = true;
            self
        }

        fn max_in_flight(&self) -> usize {
            self.max_in_flight.load(Ordering::SeqCst)
        }

        fn ranges(&self) -> Vec<Range<u64>> {
            self.ranges.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl FileRead for TrackingFileRead {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            self.ranges.lock().unwrap().push(range.clone());
            let in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(in_flight, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(10)).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            if self.fail {
                return Err(Error::UnexpectedError {
                    message: "injected BLOB index read failure".to_string(),
                    source: Some(Box::new(std::io::Error::other("injected source"))),
                });
            }
            Ok(self.bytes.slice(range.start as usize..range.end as usize))
        }

        fn cache_key(&self) -> Option<&str> {
            self.cache_key.as_deref()
        }

        fn blob_index_cache(&self) -> Option<&(dyn std::any::Any + Send + Sync)> {
            self.blob_index_cache
                .as_deref()
                .map(|cache| cache as &(dyn std::any::Any + Send + Sync))
        }
    }

    struct SparseFileRead {
        responses: Vec<(Range<u64>, Bytes)>,
        ranges: Mutex<Vec<Range<u64>>>,
    }

    impl SparseFileRead {
        fn new(responses: Vec<(Range<u64>, Bytes)>) -> Self {
            Self {
                responses,
                ranges: Mutex::new(Vec::new()),
            }
        }

        fn ranges(&self) -> Vec<Range<u64>> {
            self.ranges.lock().unwrap().clone()
        }
    }

    fn sparse_blob_map_entry(
        key_lengths: &[i64],
        value_lengths: &[i64],
    ) -> (SparseFileRead, Range<u64>) {
        let key_index = encode_delta_varints_write(key_lengths);
        let value_index = encode_delta_varints_write(value_lengths);
        let data_length = key_lengths
            .iter()
            .chain(value_lengths)
            .filter(|length| **length >= 0)
            .map(|length| *length as u64)
            .sum::<u64>();
        let payload_length = BLOB_MAP_MIN_PAYLOAD_SIZE
            + data_length
            + key_index.len() as u64
            + value_index.len() as u64;
        let payload_range = BLOB_INLINE_HEADER_SIZE..BLOB_INLINE_HEADER_SIZE + payload_length;
        let mut header = Vec::with_capacity(BLOB_MAP_HEADER_SIZE as usize);
        header.extend_from_slice(&BLOB_MAP_MAGIC_NUMBER.to_le_bytes());
        header.push(BLOB_MAP_VERSION);
        header.extend_from_slice(&(key_lengths.len() as i32).to_le_bytes());
        let lengths_start = payload_range.end - BLOB_MAP_INDEX_LENGTHS_SIZE;
        let value_index_start = lengths_start - value_index.len() as u64;
        let key_index_start = value_index_start - key_index.len() as u64;
        let data_start = payload_range.start + BLOB_MAP_HEADER_SIZE;
        let mut index_lengths = Vec::with_capacity(BLOB_MAP_INDEX_LENGTHS_SIZE as usize);
        index_lengths.extend_from_slice(&(key_index.len() as i32).to_le_bytes());
        index_lengths.extend_from_slice(&(value_index.len() as i32).to_le_bytes());
        let mut indexes = key_index.clone();
        indexes.extend_from_slice(&value_index);
        let reader = SparseFileRead::new(vec![
            (
                payload_range.start..payload_range.start + BLOB_MAP_HEADER_SIZE,
                Bytes::from(header),
            ),
            (lengths_start..payload_range.end, Bytes::from(index_lengths)),
            (key_index_start..lengths_start, Bytes::from(indexes)),
            (data_start..data_start, Bytes::new()),
        ]);
        (reader, payload_range)
    }

    #[async_trait::async_trait]
    impl FileRead for SparseFileRead {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            self.ranges.lock().unwrap().push(range.clone());
            if let Some((_, bytes)) = self
                .responses
                .iter()
                .find(|(expected, _)| expected == &range)
            {
                return Ok(bytes.clone());
            }

            Err(Error::UnexpectedError {
                message: format!("Unexpected sparse Blob test read: {range:?}"),
                source: None,
            })
        }
    }
}
