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

use super::{FilePredicates, FormatFileReader};
use crate::arrow::build_target_arrow_schema;
use crate::io::FileRead;
use crate::spec::{DataField, DataType};
use crate::table::{ArrowRecordBatchStream, RowRange};
use crate::Error;
use arrow_array::builder::BinaryBuilder;
use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use std::ops::Range;
use std::sync::Arc;

pub(crate) struct BlobFormatReader;

const BLOB_FOOTER_SIZE: u64 = 5;
const BLOB_FORMAT_VERSION: u8 = 1;
const BLOB_INLINE_HEADER_SIZE: u64 = 4;
const BLOB_TRAILER_SIZE: u64 = 12;
const BLOB_ENTRY_OVERHEAD: u64 = BLOB_INLINE_HEADER_SIZE + BLOB_TRAILER_SIZE;
const DEFAULT_BATCH_SIZE: usize = 128;
const BLOB_READ_CONCURRENCY: usize = 8;

#[async_trait]
impl FormatFileReader for BlobFormatReader {
    async fn read_batch_stream(
        &self,
        reader: Box<dyn FileRead>,
        file_size: u64,
        read_fields: &[DataField],
        _predicates: Option<&FilePredicates>,
        batch_size: Option<usize>,
        row_selection: Option<Vec<RowRange>>,
    ) -> crate::Result<ArrowRecordBatchStream> {
        validate_read_fields(read_fields)?;

        let target_schema = build_target_arrow_schema(read_fields)?;
        let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
        let blob_index = BlobFileIndex::load(reader.as_ref(), file_size).await?;
        let mut selection = RowSelectionCursor::new(blob_index.num_rows(), row_selection)?;
        let project_values = !read_fields.is_empty();

        Ok(try_stream! {
            while let Some(positions) = selection.next_batch(batch_size) {
                let batch = read_blob_batch(
                    reader.as_ref(),
                    &blob_index,
                    &target_schema,
                    &positions,
                    project_values,
                ).await?;
                yield batch;
            }
        }
        .boxed())
    }
}

fn validate_read_fields(read_fields: &[DataField]) -> crate::Result<()> {
    if read_fields.len() > 1 {
        return Err(Error::DataInvalid {
            message: format!(
                ".blob format only supports reading at most one projected column, got {}",
                read_fields.len()
            ),
            source: None,
        });
    }

    if let Some(field) = read_fields.first() {
        match field.data_type() {
            DataType::Blob(_) => Ok(()),
            other => Err(Error::DataInvalid {
                message: format!(
                    ".blob format requires a Blob field, got {:?} for column '{}'",
                    other,
                    field.name()
                ),
                source: None,
            }),
        }?;
    }

    Ok(())
}

async fn read_blob_batch(
    reader: &dyn FileRead,
    blob_index: &BlobFileIndex,
    target_schema: &Arc<arrow_schema::Schema>,
    positions: &[usize],
    project_values: bool,
) -> crate::Result<RecordBatch> {
    if !project_values {
        return RecordBatch::try_new_with_options(
            target_schema.clone(),
            Vec::new(),
            &RecordBatchOptions::new().with_row_count(Some(positions.len())),
        )
        .map_err(|e| Error::UnexpectedError {
            message: format!("Failed to build empty blob RecordBatch: {e}"),
            source: Some(Box::new(e)),
        });
    }

    let planned_reads = plan_blob_reads(blob_index, positions)?;
    let values = fetch_blob_values(reader, planned_reads).await?;
    let mut builder = BinaryBuilder::new();
    for value in values {
        match value {
            BlobValue::Null => builder.append_null(),
            BlobValue::Inline(bytes) => builder.append_value(bytes.as_ref()),
        }
    }

    let columns: Vec<ArrayRef> = vec![Arc::new(builder.finish())];
    RecordBatch::try_new(target_schema.clone(), columns).map_err(|e| Error::UnexpectedError {
        message: format!("Failed to build blob RecordBatch: {e}"),
        source: Some(Box::new(e)),
    })
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

            Ok(match entry.inline_data_range() {
                Some(range) if range.start == range.end => PlannedBlobRead::Empty,
                Some(range) => PlannedBlobRead::Read(range),
                None => PlannedBlobRead::Null,
            })
        })
        .collect()
}

async fn fetch_blob_values(
    reader: &dyn FileRead,
    planned_reads: Vec<PlannedBlobRead>,
) -> crate::Result<Vec<BlobValue>> {
    futures::stream::iter(planned_reads.into_iter().map(|planned_read| async move {
        match planned_read {
            PlannedBlobRead::Null => Ok(BlobValue::Null),
            PlannedBlobRead::Empty => Ok(BlobValue::Inline(Bytes::new())),
            PlannedBlobRead::Read(range) => reader.read(range).await.map(BlobValue::Inline),
        }
    }))
    .buffered(BLOB_READ_CONCURRENCY)
    .try_collect()
    .await
}

#[derive(Debug, Clone)]
enum PlannedBlobRead {
    Null,
    Empty,
    Read(Range<u64>),
}

#[derive(Debug, Clone)]
enum BlobValue {
    Null,
    Inline(Bytes),
}

#[derive(Debug, Clone)]
struct BlobFileIndex {
    entries: Vec<BlobEntry>,
}

impl BlobFileIndex {
    async fn load(reader: &dyn FileRead, file_size: u64) -> crate::Result<Self> {
        if file_size < BLOB_FOOTER_SIZE {
            return Err(Error::DataInvalid {
                message: format!(
                    "Blob file is too small: expected at least {BLOB_FOOTER_SIZE} bytes, got {file_size}"
                ),
                source: None,
            });
        }

        let footer = reader
            .read(file_size - BLOB_FOOTER_SIZE..file_size)
            .await
            .map_err(|e| Error::UnexpectedError {
                message: format!("Failed to read blob footer: {e}"),
                source: Some(Box::new(e)),
            })?;

        let footer_bytes: [u8; BLOB_FOOTER_SIZE as usize] =
            footer.as_ref().try_into().map_err(|_| Error::DataInvalid {
                message: "Blob footer should be exactly 5 bytes".to_string(),
                source: None,
            })?;
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
        let index_bytes = reader
            .read(index_start..index_start + index_length)
            .await
            .map_err(|e| Error::UnexpectedError {
                message: format!("Failed to read blob index bytes: {e}"),
                source: Some(Box::new(e)),
            })?;

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

#[derive(Debug, Clone)]
struct BlobEntry {
    data_offset: Option<u64>,
    data_length: u64,
}

impl BlobEntry {
    fn build_all(lengths: &[i64], data_region_end: u64) -> crate::Result<Vec<Self>> {
        let mut entries = Vec::with_capacity(lengths.len());
        let mut next_offset = 0_u64;

        for &entry_length in lengths {
            if entry_length == -1 {
                entries.push(Self {
                    data_offset: None,
                    data_length: 0,
                });
                continue;
            }

            let entry_length = u64::try_from(entry_length).map_err(|e| Error::DataInvalid {
                message: format!("Blob entry length must be positive or -1, got {entry_length}"),
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

            entries.push(Self {
                data_offset: Some(next_offset + BLOB_INLINE_HEADER_SIZE),
                data_length: entry_length - BLOB_ENTRY_OVERHEAD,
            });
            next_offset = entry_end;
        }

        Ok(entries)
    }

    fn inline_data_range(&self) -> Option<Range<u64>> {
        self.data_offset
            .map(|offset| offset..offset + self.data_length)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::test_util::BytesFileRead;
    use crate::spec::BlobType;
    use arrow_array::Array;
    use bytes::Bytes;
    use futures::TryStreamExt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[allow(dead_code)]
    mod blob_test_utils {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../blob_test_utils.rs"
        ));
    }

    #[tokio::test]
    async fn test_blob_reader_reads_inline_bytes_and_selection() {
        let read_fields = vec![DataField::new(
            0,
            "payload".to_string(),
            DataType::Blob(BlobType::new()),
        )];
        let reader = BlobFormatReader;
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

        let selected = reader
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
    async fn test_blob_reader_reads_payloads_with_bounded_parallelism() {
        let read_fields = vec![DataField::new(
            0,
            "payload".to_string(),
            DataType::Blob(BlobType::new()),
        )];
        let file_bytes = load_blob_fixture("blob-basic.blob");
        let reader = TrackingFileRead::new(Bytes::from(file_bytes.clone()));

        let batches = BlobFormatReader
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
        assert!(reader.max_in_flight() > 1);
    }

    #[test]
    fn test_blob_reader_test_helper_matches_java_fixture() {
        let generated = blob_test_utils::build_blob_file_bytes(&basic_blob_rows());

        assert_eq!(generated, load_blob_fixture("blob-basic.blob"));
    }

    #[tokio::test]
    async fn test_blob_reader_supports_empty_projection() {
        let reader = BlobFormatReader;
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
        let reader = BlobFormatReader;
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
        let reader = BlobFormatReader;
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
            matches!(result, Err(Error::DataInvalid { message, .. }) if message.contains("Blob field"))
        );
    }

    #[tokio::test]
    async fn test_blob_reader_rejects_unsupported_version() {
        let mut file_bytes = blob_test_utils::build_blob_file_bytes(&basic_blob_rows());
        let last = file_bytes.len() - 1;
        file_bytes[last] = 2;

        let result = BlobFormatReader
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

        let result = BlobFormatReader
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

    fn basic_blob_rows() -> [Option<&'static [u8]>; 4] {
        [
            Some(&b"hello"[..]),
            None,
            Some(&b"world"[..]),
            Some(&b""[..]),
        ]
    }

    fn collect_binary_values(batch: &RecordBatch) -> Vec<Option<Vec<u8>>> {
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .unwrap();
        (0..array.len())
            .map(|idx| (!array.is_null(idx)).then(|| array.value(idx).to_vec()))
            .collect()
    }

    fn load_blob_fixture(name: &str) -> Vec<u8> {
        let path = format!("{}/testdata/blob/{name}", env!("CARGO_MANIFEST_DIR"));
        std::fs::read(&path).unwrap_or_else(|e| panic!("Failed to read {path}: {e}"))
    }

    #[derive(Clone)]
    struct TrackingFileRead {
        bytes: Bytes,
        in_flight: Arc<AtomicUsize>,
        max_in_flight: Arc<AtomicUsize>,
    }

    impl TrackingFileRead {
        fn new(bytes: Bytes) -> Self {
            Self {
                bytes,
                in_flight: Arc::new(AtomicUsize::new(0)),
                max_in_flight: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn max_in_flight(&self) -> usize {
            self.max_in_flight.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl FileRead for TrackingFileRead {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            let in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(in_flight, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(10)).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            Ok(self.bytes.slice(range.start as usize..range.end as usize))
        }
    }
}
