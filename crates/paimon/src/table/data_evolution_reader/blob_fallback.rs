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

use super::{
    blob_file_row_range, row_range_overlaps_any, selected_absolute_row_ranges_for_file, BlobBunch,
    DeletionVectorContext,
};
use crate::arrow::build_target_arrow_schema;
use crate::arrow::format::blob::{BlobReadValue, IndexedBlobReader};
use crate::io::FileIO;
use crate::spec::DataField;
use crate::table::{ArrowRecordBatchStream, RowRange};
use crate::{DataSplit, Error};
use arrow_array::builder::BinaryBuilder;
use arrow_array::RecordBatch;
use async_stream::try_stream;
use futures::StreamExt;
use std::collections::VecDeque;
use std::sync::Arc;

const BATCH_SIZE: usize = 1024;

struct LazyBlobFile {
    range: RowRange,
    file_name: String,
    path: String,
    file_size: i64,
    row_count: i64,
    reader: Option<IndexedBlobReader>,
}

impl LazyBlobFile {
    async fn read_positions(
        &mut self,
        positions: &[usize],
        file_io: &FileIO,
        blob_as_descriptor: bool,
    ) -> crate::Result<Vec<BlobReadValue>> {
        if self.reader.is_none() {
            let file_size = u64::try_from(self.file_size).map_err(|e| Error::DataInvalid {
                message: format!(
                    "Blob file '{}' has negative file size {}",
                    self.file_name, self.file_size
                ),
                source: Some(Box::new(e)),
            })?;
            let input = file_io.new_input(&self.path)?;
            let reader = input.reader().await?;
            let reader = IndexedBlobReader::open(
                Box::new(reader),
                file_size,
                self.path.clone(),
                blob_as_descriptor,
            )
            .await?;
            let indexed_rows =
                i64::try_from(reader.num_rows()).map_err(|e| Error::DataInvalid {
                    message: format!(
                        "Blob file '{}' index row count {} exceeds i64",
                        self.file_name,
                        reader.num_rows()
                    ),
                    source: Some(Box::new(e)),
                })?;
            if indexed_rows != self.row_count {
                return Err(Error::DataInvalid {
                    message: format!(
                        "Blob file '{}' index contains {indexed_rows} rows but metadata declares {}",
                        self.file_name, self.row_count
                    ),
                    source: None,
                });
            }
            self.reader = Some(reader);
        }

        self.reader
            .as_ref()
            .expect("blob reader is initialized above")
            .read_positions(positions)
            .await
    }

    fn release_reader(&mut self) {
        self.reader = None;
    }
}

pub(super) fn read(
    split: &DataSplit,
    bunch: BlobBunch,
    read_fields: Vec<DataField>,
    row_ranges: Option<Vec<RowRange>>,
    file_io: FileIO,
    blob_as_descriptor: bool,
    anchor_deletion_vector: Option<DeletionVectorContext>,
) -> crate::Result<ArrowRecordBatchStream> {
    if read_fields.len() != 1 || !read_fields[0].data_type().is_blob_type() {
        return Err(Error::DataInvalid {
            message: "Blob bunch should provide exactly one BLOB field".to_string(),
            source: None,
        });
    }

    let target_schema = build_target_arrow_schema(&read_fields)?;
    let split = split.clone();

    Ok(try_stream! {
        bunch.validate_logical_range()?;
        let expected_range = bunch.expected_range()?;
        let selected_ranges = selected_absolute_row_ranges_for_file(
            bunch.expected_first_row_id,
            bunch.expected_row_count,
            row_ranges.as_deref(),
            anchor_deletion_vector
                .as_ref()
                .map(|context| context.deletion_vector.as_ref()),
        )?
        .unwrap_or_else(|| vec![expected_range]);

        let mut sequence_groups = Vec::new();
        for files in bunch.sequence_groups() {
            let mut group = VecDeque::with_capacity(files.len());
            for file in files {
                let range = blob_file_row_range(&file)?;
                if !row_range_overlaps_any(&range, &selected_ranges) {
                    continue;
                }
                let path = split.data_file_path(&file);
                group.push_back(LazyBlobFile {
                    range,
                    file_name: file.file_name,
                    path,
                    file_size: file.file_size,
                    row_count: file.row_count,
                    reader: None,
                });
            }
            if !group.is_empty() {
                sequence_groups.push(group);
            }
        }

        let mut row_cursor = RowIdBatchCursor::new(selected_ranges);
        while let Some(row_ids) = row_cursor.next_batch(BATCH_SIZE) {
            yield resolve_batch(
                &mut sequence_groups,
                &row_ids,
                target_schema.clone(),
                &file_io,
                blob_as_descriptor,
            ).await?;
        }
    }
    .boxed())
}

async fn resolve_batch(
    sequence_groups: &mut [VecDeque<LazyBlobFile>],
    row_ids: &[i64],
    target_schema: Arc<arrow_schema::Schema>,
    file_io: &FileIO,
    blob_as_descriptor: bool,
) -> crate::Result<RecordBatch> {
    let mut resolved = (0..row_ids.len())
        .map(|_| BlobReadValue::Placeholder)
        .collect::<Vec<_>>();
    let mut unresolved_count = resolved.len();
    let batch_from = row_ids[0];
    let batch_to = *row_ids.last().expect("row id batch is non-empty");

    for group in sequence_groups.iter_mut() {
        while group
            .front()
            .is_some_and(|file| file.range.to() < batch_from)
        {
            group.pop_front();
        }
    }

    // Groups are newest first. A missing row or placeholder leaves the row unresolved;
    // an explicit NULL or value stops fallback.
    for group in sequence_groups.iter_mut() {
        for file in group.iter_mut() {
            if unresolved_count == 0 || file.range.from() > batch_to {
                break;
            }

            let mut output_positions = Vec::new();
            let mut file_positions = Vec::new();
            for (output_position, row_id) in row_ids.iter().copied().enumerate() {
                if !matches!(&resolved[output_position], BlobReadValue::Placeholder) {
                    continue;
                }
                if row_id < file.range.from() || row_id > file.range.to() {
                    continue;
                }
                output_positions.push(output_position);
                file_positions.push(usize::try_from(row_id - file.range.from()).map_err(|e| {
                    Error::DataInvalid {
                        message: format!(
                            "Blob row id {row_id} cannot be represented as a file position"
                        ),
                        source: Some(Box::new(e)),
                    }
                })?);
            }

            if !file_positions.is_empty() {
                let values = file
                    .read_positions(&file_positions, file_io, blob_as_descriptor)
                    .await?;
                for (output_position, value) in output_positions.into_iter().zip(values) {
                    if !matches!(&value, BlobReadValue::Placeholder) {
                        resolved[output_position] = value;
                        unresolved_count -= 1;
                    }
                }
            }

            if file.range.to() <= batch_to {
                file.release_reader();
            }
        }

        if unresolved_count == 0 {
            break;
        }
    }

    for group in sequence_groups.iter_mut() {
        while group
            .front()
            .is_some_and(|file| file.range.to() <= batch_to)
        {
            group.pop_front();
        }
    }

    let mut builder = BinaryBuilder::new();
    for value in resolved {
        match value {
            BlobReadValue::Value(bytes) => builder.append_value(bytes),
            BlobReadValue::Null | BlobReadValue::Placeholder => builder.append_null(),
        }
    }
    RecordBatch::try_new(target_schema, vec![Arc::new(builder.finish())]).map_err(|e| {
        Error::UnexpectedError {
            message: format!("Failed to build blob fallback RecordBatch: {e}"),
            source: Some(Box::new(e)),
        }
    })
}

struct RowIdBatchCursor {
    ranges: Vec<RowRange>,
    range_index: usize,
    next_row_id: Option<i64>,
}

impl RowIdBatchCursor {
    fn new(ranges: Vec<RowRange>) -> Self {
        let next_row_id = ranges.first().map(RowRange::from);
        Self {
            ranges,
            range_index: 0,
            next_row_id,
        }
    }

    fn next_batch(&mut self, batch_size: usize) -> Option<Vec<i64>> {
        let mut row_ids = Vec::with_capacity(batch_size);
        while row_ids.len() < batch_size {
            let Some(row_id) = self.next_row_id else {
                break;
            };
            row_ids.push(row_id);

            let range = &self.ranges[self.range_index];
            if row_id == range.to() {
                self.range_index += 1;
                self.next_row_id = self.ranges.get(self.range_index).map(RowRange::from);
            } else {
                self.next_row_id = Some(row_id + 1);
            }
        }
        (!row_ids.is_empty()).then_some(row_ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileRead;
    use crate::spec::{BlobType, DataType};
    use arrow_array::{Array, BinaryArray};
    use bytes::Bytes;
    use std::ops::Range;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[allow(dead_code)]
    mod blob_test_utils {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../blob_test_utils.rs"
        ));
    }

    use blob_test_utils::{build_blob_file_bytes_with_values, BlobFixtureValue};

    #[derive(Clone)]
    struct TrackingFileRead {
        bytes: Bytes,
        reads: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl FileRead for TrackingFileRead {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            Ok(self.bytes.slice(range.start as usize..range.end as usize))
        }
    }

    async fn tracking_blob_file(
        file_name: &str,
        first_row_id: i64,
        values: &[BlobFixtureValue<'_>],
    ) -> (LazyBlobFile, Arc<AtomicUsize>) {
        let bytes = Bytes::from(build_blob_file_bytes_with_values(values));
        let file_size = bytes.len() as u64;
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = IndexedBlobReader::open(
            Box::new(TrackingFileRead {
                bytes,
                reads: reads.clone(),
            }),
            file_size,
            file_name.to_string(),
            false,
        )
        .await
        .unwrap();
        reads.store(0, Ordering::SeqCst);

        let last_row_id = first_row_id + i64::try_from(values.len()).unwrap() - 1;
        (
            LazyBlobFile {
                range: RowRange::new(first_row_id, last_row_id),
                file_name: file_name.to_string(),
                path: file_name.to_string(),
                file_size: i64::try_from(file_size).unwrap(),
                row_count: i64::try_from(values.len()).unwrap(),
                reader: Some(reader),
            },
            reads,
        )
    }

    #[tokio::test]
    async fn test_resolve_batch_skips_payloads_for_resolved_rows() {
        use BlobFixtureValue::{Null, Placeholder, Value};

        let (latest, latest_reads) =
            tracking_blob_file("latest.blob", 0, &[Value(b"new-0"), Null, Placeholder]).await;
        let (older, older_reads) = tracking_blob_file(
            "older.blob",
            0,
            &[
                Value(b"old-0"),
                Value(b"old-1"),
                Value(b"old-2"),
                Value(b"old-3"),
            ],
        )
        .await;
        let (oldest, oldest_reads) = tracking_blob_file(
            "oldest.blob",
            0,
            &[
                Value(b"ancient-0"),
                Value(b"ancient-1"),
                Value(b"ancient-2"),
                Value(b"ancient-3"),
            ],
        )
        .await;
        let schema = build_target_arrow_schema(&[DataField::new(
            0,
            "payload".to_string(),
            DataType::Blob(BlobType::new()),
        )])
        .unwrap();

        let mut groups = vec![
            VecDeque::from([latest]),
            VecDeque::from([older]),
            VecDeque::from([oldest]),
        ];
        let file_io = crate::io::FileIOBuilder::new("file").build().unwrap();
        let batch = resolve_batch(&mut groups, &[0, 1, 2, 3], schema, &file_io, false)
            .await
            .unwrap();
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();

        assert_eq!(values.value(0), b"new-0");
        assert!(values.is_null(1));
        assert_eq!(values.value(2), b"old-2");
        assert_eq!(values.value(3), b"old-3");
        assert_eq!(latest_reads.load(Ordering::SeqCst), 1);
        assert_eq!(older_reads.load(Ordering::SeqCst), 2);
        assert_eq!(oldest_reads.load(Ordering::SeqCst), 0);
    }
}
