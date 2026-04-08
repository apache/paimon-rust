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
use crate::io::FileRead;
use crate::spec::DataField;
use crate::table::{ArrowRecordBatchStream, RowRange};
use crate::Error;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{future::BoxFuture, StreamExt};
use orc_rust::projection::ProjectionMask;
use orc_rust::reader::AsyncChunkReader;
use orc_rust::ArrowReaderBuilder;

pub(crate) struct OrcFormatReader;

#[async_trait]
impl FormatFileReader for OrcFormatReader {
    async fn read_batch_stream(
        &self,
        reader: Box<dyn FileRead>,
        file_size: u64,
        read_fields: &[DataField],
        // TODO: support predicate pushdown for ORC (stripe pruning + row-level filtering)
        _predicates: Option<&FilePredicates>,
        batch_size: Option<usize>,
        row_selection: Option<Vec<RowRange>>,
    ) -> crate::Result<ArrowRecordBatchStream> {
        let orc_reader = OrcFileReader::new(file_size, reader);

        let builder = ArrowReaderBuilder::try_new_async(orc_reader)
            .await
            .map_err(|e| Error::UnexpectedError {
                message: format!("Failed to open ORC file: {e}"),
                source: Some(Box::new(e)),
            })?;

        let projected_names: Vec<&str> = read_fields.iter().map(|f| f.name()).collect();
        let projection =
            ProjectionMask::named_roots(builder.file_metadata().root_data_type(), &projected_names);

        let mut builder = builder.with_projection(projection);

        if let Some(size) = batch_size {
            builder = builder.with_batch_size(size);
        }

        if let Some(ref ranges) = row_selection {
            let total_rows: usize = builder
                .file_metadata()
                .stripe_metadatas()
                .iter()
                .map(|s| s.number_of_rows() as usize)
                .sum();
            let selection = build_range_row_selection(total_rows, ranges);
            builder = builder.with_row_selection(selection);
        }

        let stream = builder.build_async();
        Ok(stream
            .map(|r| {
                r.map_err(|e| Error::UnexpectedError {
                    message: format!("ORC read error: {e}"),
                    source: Some(Box::new(e)),
                })
            })
            .boxed())
    }
}

// ---------------------------------------------------------------------------
// Row ranges → orc_rust::RowSelection
// ---------------------------------------------------------------------------

fn build_range_row_selection(
    total_rows: usize,
    row_ranges: &[RowRange],
) -> orc_rust::row_selection::RowSelection {
    if total_rows == 0 {
        return orc_rust::row_selection::RowSelection::default();
    }

    let file_end = total_rows as i64 - 1;
    let mut local_ranges: Vec<(usize, usize)> = row_ranges
        .iter()
        .filter_map(|r| {
            if r.to() < 0 || r.from() > file_end {
                return None;
            }
            let local_start = r.from().max(0) as usize;
            let local_end = (r.to().min(file_end) + 1) as usize;
            Some((local_start, local_end))
        })
        .collect();
    local_ranges.sort_by_key(|&(s, _)| s);

    orc_rust::row_selection::RowSelection::from_consecutive_ranges(
        local_ranges.into_iter().map(|(s, e)| s..e),
        total_rows,
    )
}

// ---------------------------------------------------------------------------
// OrcFileReader — adapts paimon FileRead to orc-rust AsyncChunkReader
// ---------------------------------------------------------------------------

struct OrcFileReader {
    file_size: u64,
    r: Box<dyn FileRead>,
}

impl OrcFileReader {
    fn new(file_size: u64, r: Box<dyn FileRead>) -> Self {
        Self { file_size, r }
    }
}

impl AsyncChunkReader for OrcFileReader {
    fn len(&mut self) -> BoxFuture<'_, std::io::Result<u64>> {
        Box::pin(std::future::ready(Ok(self.file_size)))
    }

    fn get_bytes(
        &mut self,
        offset_from_start: u64,
        length: u64,
    ) -> BoxFuture<'_, std::io::Result<Bytes>> {
        Box::pin(async move {
            self.r
                .read(offset_from_start..offset_from_start + length)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use orc_rust::row_selection::RowSelector;

    #[test]
    fn test_build_range_row_selection_single_range() {
        let ranges = vec![RowRange::new(2, 4)];
        let sel = build_range_row_selection(6, &ranges);
        // rows 0,1 skip; 2,3,4 select; 5 skip
        let expected: orc_rust::row_selection::RowSelection = vec![
            RowSelector::skip(2),
            RowSelector::select(3),
            RowSelector::skip(1),
        ]
        .into();
        assert_eq!(sel, expected);
    }

    #[test]
    fn test_build_range_row_selection_with_offset() {
        let ranges = vec![RowRange::new(1, 3)];
        let sel = build_range_row_selection(5, &ranges);
        let expected: orc_rust::row_selection::RowSelection = vec![
            RowSelector::skip(1),
            RowSelector::select(3),
            RowSelector::skip(1),
        ]
        .into();
        assert_eq!(sel, expected);
    }

    #[test]
    fn test_build_range_row_selection_out_of_file() {
        let ranges = vec![RowRange::new(10, 20)];
        let sel = build_range_row_selection(5, &ranges);
        let expected: orc_rust::row_selection::RowSelection = vec![RowSelector::skip(5)].into();
        assert_eq!(sel, expected);
    }
}
