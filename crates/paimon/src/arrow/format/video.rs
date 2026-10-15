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

//! Packed video v1 metadata, matching Java VideoFileMeta and VideoFormatReader.
//! Reading returns frame descriptors; encoded videos and keyframe indexes stay lazy.

use super::blob::{build_blob_batch, BlobReadValue};
use super::delta_varint::decode_delta_varints;
use super::{FilePredicates, FormatFileReader};
use crate::arrow::build_target_arrow_schema;
use crate::io::FileRead;
use crate::spec::{DataField, DataType, VideoFrameDescriptor};
use crate::table::{ArrowRecordBatchStream, RowRange};
use crate::{Error, Result};
use arrow_array::{RecordBatch, RecordBatchOptions};
use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;

const FOOTER_SIZE: u64 = 25;
const MAGIC: i32 = 0x4F454449;
const VERSION: u8 = 1;
const MAX_KEYFRAME_INDEX_BYTES: i64 = 16 * 1024 * 1024;
const MAX_TOTAL_KEYFRAME_INDEX_BYTES: i64 = 64 * 1024 * 1024;

fn corrupt(message: impl Into<String>) -> Error {
    Error::DataInvalid {
        message: format!("Corrupt video file: {}", message.into()),
        source: None,
    }
}

struct VideoFileMeta {
    physical_lengths: Vec<i64>,
    physical_offsets: Vec<i64>,
    keyframe_lengths: Vec<i64>,
    keyframe_offsets: Vec<i64>,
    run_ends: Vec<usize>,
    run_references: Vec<i64>,
    run_first_frames: Vec<i64>,
}

impl VideoFileMeta {
    async fn load(reader: &dyn FileRead, file_size: u64) -> Result<Self> {
        if file_size < FOOTER_SIZE {
            return Err(corrupt("file is smaller than its footer"));
        }
        let footer_start = file_size - FOOTER_SIZE;
        let footer = reader.read(footer_start..file_size).await?;
        if footer.len() != FOOTER_SIZE as usize {
            return Err(corrupt("short footer read"));
        }
        if i32::from_le_bytes(footer[20..24].try_into().unwrap()) != MAGIC {
            return Err(corrupt("invalid footer magic"));
        }
        if footer[24] != VERSION {
            return Err(Error::Unsupported {
                message: format!("Unsupported video format version {}", footer[24]),
            });
        }
        let lengths: Vec<_> = footer[..20]
            .as_chunks::<4>()
            .0
            .iter()
            .map(|bytes| i32::from_le_bytes(*bytes))
            .collect();
        if lengths.iter().any(|length| *length < 0) {
            return Err(corrupt("negative index length"));
        }
        let total_length: u64 = lengths.iter().map(|length| *length as u64).sum();
        let index_start = footer_start
            .checked_sub(total_length)
            .ok_or_else(|| corrupt("indexes exceed file size"))?;
        let index = reader.read(index_start..footer_start).await?;
        if index.len() as u64 != total_length {
            return Err(corrupt("short index read"));
        }
        let mut cursor = 0;
        let mut indexes = Vec::with_capacity(5);
        for length in lengths {
            let end = cursor + length as usize;
            indexes.push(decode_delta_varints(&index[cursor..end])?);
            cursor = end;
        }
        let mut indexes = indexes.into_iter();
        let physical_lengths = indexes.next().unwrap();
        let keyframe_lengths = indexes.next().unwrap();
        let run_lengths = indexes.next().unwrap();
        let run_references = indexes.next().unwrap();
        let run_first_frames = indexes.next().unwrap();
        if physical_lengths.len() != keyframe_lengths.len() {
            return Err(corrupt("physical video and keyframe index counts differ"));
        }
        let mut keyframe_size = 0i64;
        for length in &keyframe_lengths {
            if !(0..=MAX_KEYFRAME_INDEX_BYTES).contains(length) {
                return Err(corrupt(
                    "invalid keyframe index length or index exceeds the 16 MiB limit",
                ));
            }
            keyframe_size += length;
            if keyframe_size > MAX_TOTAL_KEYFRAME_INDEX_BYTES {
                return Err(corrupt("keyframe indexes exceed the 64 MiB limit"));
            }
        }
        let index_start =
            i64::try_from(index_start).map_err(|_| corrupt("file offset exceeds i64"))?;
        let keyframe_start = index_start
            .checked_sub(keyframe_size)
            .filter(|offset| *offset >= 0)
            .ok_or_else(|| corrupt("keyframe indexes exceed file size"))?;
        let mut physical_offsets = Vec::with_capacity(physical_lengths.len());
        let mut offset = 0;
        for length in &physical_lengths {
            if *length <= 0 || *length > keyframe_start - offset {
                return Err(corrupt("invalid physical video length"));
            }
            physical_offsets.push(offset);
            offset += length;
        }
        if offset != keyframe_start {
            return Err(corrupt(
                "physical video lengths do not match payload region",
            ));
        }
        let mut keyframe_offsets = Vec::with_capacity(keyframe_lengths.len());
        for length in &keyframe_lengths {
            keyframe_offsets.push(offset);
            offset += length;
        }
        if run_lengths.len() != run_references.len() || run_lengths.len() != run_first_frames.len()
        {
            return Err(corrupt("run indexes have different counts"));
        }
        let mut rows = 0i64;
        let mut run_ends = Vec::with_capacity(run_lengths.len());
        for ((length, reference), frame) in run_lengths
            .iter()
            .zip(&run_references)
            .zip(&run_first_frames)
        {
            if *length <= 0 || *length > i64::from(i32::MAX) - rows {
                return Err(corrupt("invalid run length or row count exceeds i32"));
            }
            if *reference != -1 && *reference != -2 {
                if *reference < 0 || *reference as u64 >= physical_lengths.len() as u64 {
                    return Err(corrupt("run references an invalid physical video"));
                }
                if *frame < 0 || frame.checked_add(length - 1).is_none() {
                    return Err(corrupt("invalid first frame or frame index overflow"));
                }
            }
            rows += length;
            run_ends.push(rows as usize);
        }
        Ok(Self {
            physical_lengths,
            physical_offsets,
            keyframe_lengths,
            keyframe_offsets,
            run_ends,
            run_references,
            run_first_frames,
        })
    }

    fn num_rows(&self) -> usize {
        self.run_ends.last().copied().unwrap_or(0)
    }

    fn frame(&self, position: usize, path: &str) -> Result<BlobReadValue> {
        if position >= self.num_rows() {
            return Err(corrupt("selected row is outside row count"));
        }
        let run = self.run_ends.partition_point(|end| *end <= position);
        let reference = self.run_references[run];
        match reference {
            -1 => Ok(BlobReadValue::Null),
            -2 => Ok(BlobReadValue::Placeholder),
            _ => {
                let ordinal = reference as usize;
                let start = if run == 0 { 0 } else { self.run_ends[run - 1] };
                let length = self.keyframe_lengths[ordinal];
                let frame = VideoFrameDescriptor::new(
                    path.into(),
                    self.physical_offsets[ordinal],
                    self.physical_lengths[ordinal],
                    self.run_first_frames[run] + (position - start) as i64,
                    if length == 0 {
                        -1
                    } else {
                        self.keyframe_offsets[ordinal]
                    },
                    length,
                )?;
                Ok(BlobReadValue::Value(Bytes::from(frame.serialize())))
            }
        }
    }
}

pub(crate) struct IndexedVideoReader {
    metadata: VideoFileMeta,
    path: String,
}

impl IndexedVideoReader {
    pub(crate) async fn open(reader: &dyn FileRead, file_size: u64, path: String) -> Result<Self> {
        Ok(Self {
            metadata: VideoFileMeta::load(reader, file_size).await?,
            path,
        })
    }
    pub(crate) fn num_rows(&self) -> usize {
        self.metadata.num_rows()
    }
    pub(crate) fn read_positions(&self, positions: &[usize]) -> Result<Vec<BlobReadValue>> {
        positions
            .iter()
            .map(|position| self.metadata.frame(*position, &self.path))
            .collect()
    }
}

pub(crate) struct VideoFormatReader {
    path: String,
}

impl VideoFormatReader {
    pub(crate) fn new(path: String) -> Self {
        Self { path }
    }
}

#[async_trait]
impl FormatFileReader for VideoFormatReader {
    async fn read_batch_stream(
        &self,
        reader: Box<dyn FileRead>,
        file_size: u64,
        read_fields: &[DataField],
        predicates: Option<&FilePredicates>,
        batch_size: Option<usize>,
        row_selection: Option<Vec<RowRange>>,
    ) -> Result<ArrowRecordBatchStream> {
        if let Some(predicates) = predicates {
            crate::table::row_id_predicate::reject_row_id_filter(
                &predicates.predicates,
                "video files",
            )?;
        }
        if read_fields.len() > 1
            || read_fields
                .first()
                .is_some_and(|field| !matches!(field.data_type(), DataType::Blob(_)))
        {
            return Err(corrupt("video format only supports one scalar BLOB field"));
        }
        let schema = build_target_arrow_schema(read_fields)?;
        let indexed =
            IndexedVideoReader::open(reader.as_ref(), file_size, self.path.clone()).await?;
        let mut positions: Box<dyn Iterator<Item = usize> + Send> = match row_selection {
            None => Box::new(0..indexed.num_rows()),
            Some(ranges) => {
                for range in &ranges {
                    if range.from() < 0
                        || range.to() < range.from()
                        || range.to() as u64 >= indexed.num_rows() as u64
                    {
                        return Err(corrupt("selected row is outside row count"));
                    }
                }
                Box::new(
                    ranges
                        .into_iter()
                        .flat_map(|range| range.from() as usize..=range.to() as usize),
                )
            }
        };
        let batch_size = batch_size.unwrap_or(1024).max(1);
        Ok(try_stream! {
            loop {
                let selected: Vec<_> = positions.by_ref().take(batch_size).collect();
                if selected.is_empty() { break; }
                if schema.fields().is_empty() {
                    yield RecordBatch::try_new_with_options(schema.clone(), vec![],
                        &RecordBatchOptions::new().with_row_count(Some(selected.len())))
                        .map_err(|error| corrupt(error.to_string()))?;
                } else {
                    yield build_blob_batch(&schema, indexed.read_positions(&selected)?)?;
                }
            }
        }
        .boxed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::test_util::BytesFileRead;
    use crate::spec::{BlobType, IntType};
    use futures::TryStreamExt;

    fn java_fixture() -> Bytes {
        let text = include_str!("../../../testdata/video/video-v1.hex");
        hex::decode(
            text.lines()
                .find(|line| !line.is_empty() && !line.starts_with('#'))
                .unwrap(),
        )
        .unwrap()
        .into()
    }

    fn file(indexes: [&[i64]; 5], payload: &[u8]) -> Bytes {
        let mut bytes = payload.to_vec();
        let mut lengths = Vec::new();
        for values in indexes {
            let index = super::super::delta_varint::encode_delta_varints(values);
            lengths.push(index.len() as i32);
            bytes.extend(index);
        }
        for length in lengths {
            bytes.extend(length.to_le_bytes());
        }
        bytes.extend(MAGIC.to_le_bytes());
        bytes.push(VERSION);
        bytes.into()
    }

    #[tokio::test]
    async fn latest_java_fixture_preserves_frames_indexes_nulls_and_placeholders() {
        let bytes = java_fixture();
        let reader = IndexedVideoReader::open(
            &BytesFileRead(bytes.clone()),
            bytes.len() as u64,
            "file:/data.video".into(),
        )
        .await
        .unwrap();
        assert_eq!(reader.num_rows(), 7);
        let values = reader.read_positions(&[0, 1, 2, 3, 4, 5, 6]).unwrap();
        for (index, frame, offset, length, keyframe) in [
            (0, 2, 0, 3, true),
            (1, 3, 0, 3, true),
            (4, 7, 3, 4, false),
            (5, 8, 3, 4, false),
            (6, 10, 0, 3, true),
        ] {
            let BlobReadValue::Value(bytes) = &values[index] else {
                panic!("expected frame")
            };
            let descriptor = VideoFrameDescriptor::deserialize(bytes).unwrap();
            assert_eq!(descriptor.frame_index(), frame);
            assert_eq!(descriptor.payload_descriptor().offset(), offset);
            assert_eq!(descriptor.payload_descriptor().length(), length);
            assert_eq!(descriptor.keyframe_index_descriptor().is_some(), keyframe);
            if let Some(mapping) = descriptor.keyframe_index_descriptor() {
                assert_eq!(mapping.offset(), 7);
                assert_eq!(mapping.length(), 54);
            }
        }
        assert!(matches!(values[2], BlobReadValue::Null));
        assert!(matches!(values[3], BlobReadValue::Placeholder));
        assert!(reader.read_positions(&[7]).is_err());
    }

    #[tokio::test]
    async fn selection_batching_and_zero_column_reads_keep_logical_rows() {
        let bytes = java_fixture();
        let format = VideoFormatReader::new("file:/data.video".into());
        let field = DataField::new(0, "video".into(), DataType::Blob(BlobType::new()));
        for fields in [vec![field], vec![]] {
            let stream = format
                .read_batch_stream(
                    Box::new(BytesFileRead(bytes.clone())),
                    bytes.len() as u64,
                    &fields,
                    None,
                    Some(2),
                    Some(vec![RowRange::new(1, 3), RowRange::new(6, 6)]),
                )
                .await
                .unwrap();
            let batches: Vec<_> = stream.try_collect().await.unwrap();
            assert_eq!(
                batches
                    .iter()
                    .map(RecordBatch::num_rows)
                    .collect::<Vec<_>>(),
                vec![2, 2]
            );
            assert!(batches
                .iter()
                .all(|batch| batch.num_columns() == fields.len()));
        }
        for selection in [vec![RowRange::new(7, 7)], vec![RowRange::new(-1, 0)]] {
            assert!(format
                .read_batch_stream(
                    Box::new(BytesFileRead(bytes.clone())),
                    bytes.len() as u64,
                    &[],
                    None,
                    None,
                    Some(selection)
                )
                .await
                .is_err());
        }
        let empty = format
            .read_batch_stream(
                Box::new(BytesFileRead(bytes.clone())),
                bytes.len() as u64,
                &[],
                None,
                None,
                Some(vec![]),
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(empty.is_empty());
        assert!(format
            .read_batch_stream(
                Box::new(BytesFileRead(bytes.clone())),
                bytes.len() as u64,
                &[DataField::new(
                    0,
                    "id".into(),
                    DataType::Int(IntType::new())
                )],
                None,
                None,
                None
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn malformed_footer_and_run_indexes_fail_before_returning_rows() {
        let good = file([&[3], &[0], &[2], &[0], &[0]], b"abc");
        assert_eq!(
            VideoFileMeta::load(&BytesFileRead(good.clone()), good.len() as u64)
                .await
                .unwrap()
                .num_rows(),
            2
        );
        let mut bad = Vec::new();
        bad.push(Bytes::from_static(b"short"));
        let mut bytes = good.to_vec();
        *bytes.last_mut().unwrap() = 2;
        bad.push(bytes.into());
        let mut bytes = good.to_vec();
        let n = bytes.len();
        bytes[n - 5] ^= 1;
        bad.push(bytes.into());
        let mut bytes = good.to_vec();
        let n = bytes.len();
        bytes[n - 25..n - 21].copy_from_slice(&(-1i32).to_le_bytes());
        bad.push(bytes.into());
        for indexes in [
            [&[0][..], &[0], &[1], &[0], &[0]],
            [&[4][..], &[0], &[1], &[0], &[0]],
            [&[3][..], &[], &[1], &[0], &[0]],
            [&[3][..], &[-1], &[1], &[0], &[0]],
            [&[3][..], &[16 * 1024 * 1024 + 1], &[1], &[0], &[0]],
            [&[3][..], &[0], &[0], &[0], &[0]],
            [&[3][..], &[0], &[1], &[1], &[0]],
            [&[3][..], &[0], &[1], &[-3], &[0]],
            [&[3][..], &[0], &[1], &[0], &[-1]],
            [&[3][..], &[0], &[2], &[0], &[i64::MAX]],
            [&[3][..], &[0], &[i32::MAX as i64, 1], &[0, 0], &[0, 0]],
            [&[3][..], &[0], &[1], &[0], &[]],
        ] {
            bad.push(file(indexes, b"abc"));
        }
        for bytes in bad {
            assert!(
                VideoFileMeta::load(&BytesFileRead(bytes.clone()), bytes.len() as u64)
                    .await
                    .is_err()
            );
        }
        let nulls = file([&[], &[], &[3], &[-1], &[0]], b"");
        let meta = VideoFileMeta::load(&BytesFileRead(nulls.clone()), nulls.len() as u64)
            .await
            .unwrap();
        assert_eq!(meta.num_rows(), 3);
        assert!(matches!(
            meta.frame(2, "unused").unwrap(),
            BlobReadValue::Null
        ));
    }
}
