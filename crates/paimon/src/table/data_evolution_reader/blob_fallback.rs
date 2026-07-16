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
use std::sync::Arc;

const BATCH_SIZE: usize = 1024;

struct OpenBlobFile {
    range: RowRange,
    reader: IndexedBlobReader,
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
            let mut group = Vec::with_capacity(files.len());
            for file in files {
                let range = blob_file_row_range(&file)?;
                if !row_range_overlaps_any(&range, &selected_ranges) {
                    continue;
                }
                let path = split.data_file_path(&file);
                let input = file_io.new_input(&path)?;
                let reader = input.reader().await?;
                let file_size = u64::try_from(file.file_size).map_err(|e| Error::DataInvalid {
                    message: format!(
                        "Blob file '{}' has negative file size {}",
                        file.file_name, file.file_size
                    ),
                    source: Some(Box::new(e)),
                })?;
                let reader = IndexedBlobReader::open(
                    Box::new(reader),
                    file_size,
                    path,
                    blob_as_descriptor,
                )
                .await?;
                let indexed_rows = i64::try_from(reader.num_rows()).map_err(|e| {
                    Error::DataInvalid {
                        message: format!(
                            "Blob file '{}' index row count {} exceeds i64",
                            file.file_name,
                            reader.num_rows()
                        ),
                        source: Some(Box::new(e)),
                    }
                })?;
                if indexed_rows != file.row_count {
                    Err(Error::DataInvalid {
                        message: format!(
                            "Blob file '{}' index contains {indexed_rows} rows but metadata declares {}",
                            file.file_name, file.row_count
                        ),
                        source: None,
                    })?;
                }
                group.push(OpenBlobFile { range, reader });
            }
            sequence_groups.push(group);
        }

        let mut row_cursor = RowIdBatchCursor::new(selected_ranges);
        while let Some(row_ids) = row_cursor.next_batch(BATCH_SIZE) {
            yield resolve_batch(&sequence_groups, &row_ids, target_schema.clone()).await?;
        }
    }
    .boxed())
}

async fn resolve_batch(
    sequence_groups: &[Vec<OpenBlobFile>],
    row_ids: &[i64],
    target_schema: Arc<arrow_schema::Schema>,
) -> crate::Result<RecordBatch> {
    let mut resolved = (0..row_ids.len())
        .map(|_| BlobReadValue::Placeholder)
        .collect::<Vec<_>>();

    // Groups are newest first. A missing row or placeholder leaves the row unresolved;
    // an explicit NULL or value stops fallback. Older payloads are still read to keep
    // this compatibility path free of stale-read optimizations.
    for group in sequence_groups {
        for file in group {
            let mut output_positions = Vec::new();
            let mut file_positions = Vec::new();
            for (output_position, row_id) in row_ids.iter().copied().enumerate() {
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

            if file_positions.is_empty() {
                continue;
            }

            let values = file.reader.read_positions(&file_positions).await?;
            for (output_position, value) in output_positions.into_iter().zip(values) {
                if matches!(&resolved[output_position], BlobReadValue::Placeholder)
                    && !matches!(&value, BlobReadValue::Placeholder)
                {
                    resolved[output_position] = value;
                }
            }
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
