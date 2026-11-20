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

//! Range-wise Vector column providers across projections, formats and schemas.

use std::collections::HashMap;

use arrow_array::RecordBatch;
use async_stream::try_stream;
use futures::StreamExt;

use super::{
    is_vector_store_file_name, vector_file_row_range, vector_format_suffix, PreparedMergeGroup,
    ResolvedFileInfo,
};
use crate::spec::{DataField, DataFileMeta};
use crate::table::data_file_reader::DataFileReader;
use crate::table::row_id_predicate::intersect_sorted_ranges;
use crate::table::{ArrowRecordBatchStream, RowRange};
use crate::{DataSplit, Error};

#[cfg(test)]
#[path = "vector_versions_tests.rs"]
mod tests;

/// One lazy reader for all winning ranges of a file. A batch may span several
/// ranges; keep its unused rows while another version supplies the rows between.
struct VectorFileRead {
    file: DataFileMeta,
    ranges: Vec<RowRange>,
    stream: Option<ArrowRecordBatchStream>,
    cursor: Option<(RecordBatch, usize)>,
    remaining_rows: i64,
}

impl VectorFileRead {
    fn new(file: DataFileMeta, ranges: Vec<RowRange>) -> Self {
        let remaining_rows = ranges.iter().map(RowRange::count).sum();
        Self {
            file,
            ranges,
            stream: None,
            cursor: None,
            remaining_rows,
        }
    }

    fn invalid_row_count(&self) -> Error {
        Error::DataInvalid {
            message: format!(
                "Vector file '{}' did not produce its selected row count",
                self.file.file_name
            ),
            source: None,
        }
    }

    async fn next_batch(
        &mut self,
        reader: &DataFileReader,
        split: &DataSplit,
        range_rows: i64,
    ) -> crate::Result<RecordBatch> {
        if self.stream.is_none() {
            let data_fields = reader.derive_data_fields(&self.file).await?;
            self.stream = Some(reader.read_single_file_stream(
                split,
                self.file.clone(),
                data_fields,
                None,
                Some(std::mem::take(&mut self.ranges)),
            )?);
        }
        while self.cursor.is_none() {
            let Some(batch) = self.stream.as_mut().unwrap().next().await else {
                return Err(self.invalid_row_count());
            };
            let batch = batch?;
            if batch.num_rows() > 0 {
                self.cursor = Some((batch, 0));
            }
        }
        let (batch, offset) = self.cursor.as_mut().unwrap();
        let count = range_rows.min((batch.num_rows() - *offset) as i64) as usize;
        let output = batch.slice(*offset, count);
        *offset += count;
        if *offset == batch.num_rows() {
            self.cursor = None;
        }
        self.remaining_rows -= count as i64;
        if self.remaining_rows == 0 {
            if self.cursor.is_some() {
                return Err(self.invalid_row_count());
            }
            // Check EOF (including late errors), then release the reader and
            // its decoded pages as soon as this file's last range is consumed.
            while let Some(batch) = self.stream.as_mut().unwrap().next().await {
                if batch?.num_rows() > 0 {
                    return Err(self.invalid_row_count());
                }
            }
            self.stream = None;
        }
        Ok(output)
    }
}

/// Retain the bundled fast path for simple rolled files. A field can otherwise
/// have several schemas/projections, overlapping versions or missing ranges.
pub(super) fn required(group: &PreparedMergeGroup, infos: &[ResolvedFileInfo]) -> bool {
    let files = &group.files;
    let mut providers = HashMap::<i32, (usize, Vec<(i64, i64)>)>::new();
    for (index, (file, info)) in files.iter().zip(infos).enumerate() {
        if !is_vector_store_file_name(&file.file_name) {
            continue;
        }
        for id in &info.field_ids {
            let (first, ranges) = providers.entry(*id).or_insert_with(|| (index, Vec::new()));
            let previous = &files[*first];
            if previous.schema_id != file.schema_id
                || vector_format_suffix(&previous.file_name)
                    != vector_format_suffix(&file.file_name)
                || infos[*first].normalized_write_cols != info.normalized_write_cols
            {
                return true;
            }
            if let Some(range) = file.row_id_range() {
                ranges.push(range);
            }
        }
    }
    providers.values_mut().any(|(_, ranges)| {
        ranges.sort_unstable();
        ranges
            .first()
            .is_none_or(|range| range.0 != group.first_row_id)
            || ranges
                .last()
                .is_none_or(|range| range.1 != group.first_row_id + group.logical_row_count - 1)
            || ranges
                .windows(2)
                .any(|pair| pair[0].1.checked_add(1) != Some(pair[1].0))
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) fn read(
    reader: DataFileReader,
    split: &DataSplit,
    files: &[DataFileMeta],
    first: i64,
    rows: i64,
    selected: Option<Vec<RowRange>>,
    fields: &[DataField],
    batch_size: Option<usize>,
) -> crate::Result<ArrowRecordBatchStream> {
    let mut files = files.to_vec();
    files.sort_by_key(|file| {
        (
            std::cmp::Reverse(file.max_sequence_number),
            file.first_row_id,
        )
    });
    let selected = selected.unwrap_or_else(|| vec![RowRange::new(first, first + rows - 1)]);
    let mut covered = Vec::new();
    let mut reads = Vec::new();
    let mut file_reads = Vec::new();
    let mut previous_version: Option<(i64, i64)> = None;
    for file in files {
        let range = vector_file_row_range(&file)?;
        if range.from() < first || range.to() >= first + rows {
            return Err(Error::DataInvalid {
                message: "Vector version exceeds its normal row-ID range".into(),
                source: None,
            });
        }
        if previous_version.is_some_and(|(sequence, end)| {
            sequence == file.max_sequence_number && range.from() <= end
        }) {
            return Err(Error::DataInvalid {
                message: "Vector versions at the same sequence overlap for one field".into(),
                source: None,
            });
        }
        previous_version = Some((file.max_sequence_number, range.to()));
        let winning =
            crate::table::source::exclude_row_ranges(std::slice::from_ref(&range), &covered);
        let ranges = intersect_sorted_ranges(&winning, &selected);
        if !ranges.is_empty() {
            let index = file_reads.len();
            reads.extend(ranges.iter().map(|range| (range.clone(), Some(index))));
            file_reads.push(VectorFileRead::new(file, ranges));
        }
        covered.push(range);
        covered = crate::table::merge_row_ranges(covered);
    }
    let missing = crate::table::source::exclude_row_ranges(&selected, &covered);
    if !missing.is_empty() && fields.iter().any(|field| !field.data_type().is_nullable()) {
        return Err(Error::DataInvalid {
            message: "Vector versions do not cover the requested row-ID range".into(),
            source: None,
        });
    }
    // Normal merges can join ranges predating a nullable field. Reading their
    // missing values is safe; compaction separately requires full field coverage.
    reads.extend(missing.into_iter().map(|range| (range, None)));
    reads.sort_by_key(|(range, _)| range.from());
    // Interleaved readers can deadlock if a paused reader holds the shared
    // prefetch permits needed by the next file.
    let mut started = vec![false; file_reads.len()];
    let mut previous_source = None;
    let interleaved = reads
        .iter()
        .filter_map(|(_, source)| *source)
        .any(|source| {
            if previous_source == Some(source) {
                return false;
            }
            previous_source = Some(source);
            std::mem::replace(&mut started[source], true)
        });
    let reader = if interleaved {
        reader.without_parquet_prefetch()
    } else {
        reader
    };
    let schema = crate::arrow::build_read_arrow_schema(fields)?;
    let batch_size = batch_size.unwrap_or(1024).max(1);
    let split = split.clone();
    Ok(try_stream! {
        for (range, source) in reads {
            if let Some(index) = source {
                let mut left = range.count();
                while left > 0 {
                    let batch = file_reads[index].next_batch(&reader, &split, left).await?;
                    left -= batch.num_rows() as i64;
                    yield batch;
                }
            } else {
                let mut left = range.count();
                while left > 0 {
                    let count = usize::try_from(left).unwrap_or(usize::MAX).min(batch_size);
                    let columns = schema.fields().iter()
                        .map(|field| arrow_array::new_null_array(field.data_type(), count))
                        .collect();
                    yield RecordBatch::try_new(schema.clone(), columns).map_err(|error| {
                        Error::DataInvalid {
                            message: "Cannot build missing Vector rows".into(),
                            source: Some(Box::new(error)),
                        }
                    })?;
                    left -= count as i64;
                }
            }
        }
    }
    .boxed())
}
