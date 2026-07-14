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

use crate::io::{FileIO, FileRead};
use crate::spec::BlobDescriptor;
use crate::Result;
use arrow_array::builder::BinaryBuilder;
use arrow_array::{Array, BinaryArray};
use bytes::Bytes;
use std::collections::HashMap;

const BLOB_RANGE_MERGE_GAP: u64 = 64 * 1024;
const BLOB_RANGE_MERGE_MAX_SPAN: u64 = 8 * 1024 * 1024;

/// For each row in a blob column, if the value is a serialized `BlobDescriptor`,
/// resolve it by reading the actual data from the referenced URI+offset+length.
/// Raw data values are passed through unchanged.
pub(crate) async fn resolve_blob_column(
    col: &BinaryArray,
    file_io: &FileIO,
) -> Result<BinaryArray> {
    let mut needs_resolve = false;
    for i in 0..col.len() {
        if !col.is_null(i) && BlobDescriptor::is_blob_descriptor(col.value(i)) {
            needs_resolve = true;
            break;
        }
    }

    if !needs_resolve {
        return Ok(col.clone());
    }

    let mut cells = Vec::with_capacity(col.len());
    let mut requests_by_uri: HashMap<String, Vec<BlobReadRequestSpec>> = HashMap::new();
    let mut value_capacity = 0usize;

    for row in 0..col.len() {
        if col.is_null(row) {
            cells.push(ResolvedBlobCell::Null);
            continue;
        }

        let value = col.value(row);
        if BlobDescriptor::is_blob_descriptor(value) {
            let desc = BlobDescriptor::deserialize(value)?;
            let range = desc.range_spec()?;
            requests_by_uri
                .entry(desc.uri().to_string())
                .or_default()
                .push(BlobReadRequestSpec {
                    row,
                    offset: range.offset(),
                    length: range.length(),
                });
            cells.push(ResolvedBlobCell::Null);
        } else {
            value_capacity = value_capacity.saturating_add(value.len());
            cells.push(ResolvedBlobCell::Value(Bytes::copy_from_slice(value)));
        }
    }

    for (uri, requests) in requests_by_uri {
        let input = file_io.new_input(&uri)?;
        let file_size = if requests.iter().any(|request| request.length.is_none()) {
            input
                .metadata()
                .await
                .map_err(|e| crate::Error::UnexpectedError {
                    message: format!("Failed to read metadata for BlobDescriptor URI '{uri}': {e}"),
                    source: Some(Box::new(e)),
                })?
                .size
        } else {
            0
        };
        let mut bounded_requests = Vec::with_capacity(requests.len());
        for request in requests {
            let length = request
                .length
                .unwrap_or_else(|| file_size.saturating_sub(request.offset));
            request
                .offset
                .checked_add(length)
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: format!(
                        "BlobDescriptor range overflows u64: offset={}, length={length}",
                        request.offset
                    ),
                    source: None,
                })?;
            value_capacity = value_capacity.saturating_add(length as usize);
            if length == 0 {
                cells[request.row] = ResolvedBlobCell::Value(Bytes::new());
                continue;
            }
            bounded_requests.push(BlobReadRequest {
                row: request.row,
                offset: request.offset,
                length,
            });
        }

        if bounded_requests.is_empty() {
            continue;
        }

        let reader = input.reader().await?;
        for merged in merge_blob_read_requests(bounded_requests) {
            let data = reader.read(merged.start..merged.end).await.map_err(|e| {
                crate::Error::UnexpectedError {
                    message: format!(
                        "Failed to read BlobDescriptor URI '{uri}' range {}..{}: {e}",
                        merged.start, merged.end
                    ),
                    source: Some(Box::new(e)),
                }
            })?;
            let expected_len = merged.end - merged.start;
            let actual_len = data.len() as u64;
            if actual_len != expected_len {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Failed to read BlobDescriptor URI '{uri}': short read for range {}..{}, expected={expected_len} bytes, actual={actual_len} bytes",
                        merged.start, merged.end
                    ),
                    source: None,
                });
            }
            for request in merged.requests {
                let start = usize::try_from(request.offset - merged.start).map_err(|e| {
                    crate::Error::DataInvalid {
                        message: format!(
                            "BlobDescriptor slice offset exceeds usize: offset={}, merged_start={}",
                            request.offset, merged.start
                        ),
                        source: Some(Box::new(e)),
                    }
                })?;
                let length =
                    usize::try_from(request.length).map_err(|e| crate::Error::DataInvalid {
                        message: format!(
                            "BlobDescriptor slice length exceeds usize: {}",
                            request.length
                        ),
                        source: Some(Box::new(e)),
                    })?;
                let end = start
                    .checked_add(length)
                    .filter(|end| *end <= data.len())
                    .ok_or_else(|| crate::Error::DataInvalid {
                        message: format!(
                            "BlobDescriptor slice exceeds read data: start={start}, length={length}, actual={}",
                            data.len()
                        ),
                        source: None,
                    })?;
                cells[request.row] = ResolvedBlobCell::Value(data.slice(start..end));
            }
        }
    }

    let mut builder = BinaryBuilder::with_capacity(col.len(), value_capacity);
    for cell in cells {
        match cell {
            ResolvedBlobCell::Null => builder.append_null(),
            ResolvedBlobCell::Value(value) => builder.append_value(value.as_ref()),
        }
    }
    Ok(builder.finish())
}

#[derive(Debug)]
enum ResolvedBlobCell {
    Null,
    Value(Bytes),
}

#[derive(Debug)]
struct BlobReadRequestSpec {
    row: usize,
    offset: u64,
    length: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BlobReadRequest {
    row: usize,
    offset: u64,
    length: u64,
}

#[derive(Debug, PartialEq, Eq)]
struct MergedBlobRead {
    start: u64,
    end: u64,
    requests: Vec<BlobReadRequest>,
}

fn merge_blob_read_requests(mut requests: Vec<BlobReadRequest>) -> Vec<MergedBlobRead> {
    if requests.is_empty() {
        return Vec::new();
    }

    requests.sort_by_key(|request| (request.offset, request.length, request.row));
    let mut merged = Vec::new();
    let mut current = MergedBlobRead {
        start: requests[0].offset,
        end: requests[0].offset + requests[0].length,
        requests: vec![requests[0].clone()],
    };

    for request in requests.into_iter().skip(1) {
        let request_end = request.offset + request.length;
        let close_enough = current
            .end
            .checked_add(BLOB_RANGE_MERGE_GAP)
            .is_some_and(|merge_limit| request.offset <= merge_limit);
        let merged_end = current.end.max(request_end);
        let merged_span = merged_end - current.start;
        if close_enough && merged_span <= BLOB_RANGE_MERGE_MAX_SPAN {
            current.end = merged_end;
            current.requests.push(request);
        } else {
            merged.push(current);
            current = MergedBlobRead {
                start: request.offset,
                end: request_end,
                requests: vec![request],
            };
        }
    }
    merged.push(current);
    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_blob_read_requests_merges_nearby_ranges() {
        let merged = merge_blob_read_requests(vec![
            BlobReadRequest {
                row: 2,
                offset: 120,
                length: 5,
            },
            BlobReadRequest {
                row: 0,
                offset: 0,
                length: 10,
            },
            BlobReadRequest {
                row: 1,
                offset: 10,
                length: 8,
            },
            BlobReadRequest {
                row: 3,
                offset: BLOB_RANGE_MERGE_MAX_SPAN + 1,
                length: 4,
            },
        ]);

        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].start, 0);
        assert_eq!(merged[0].end, 125);
        assert_eq!(
            merged[0]
                .requests
                .iter()
                .map(|request| request.row)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(merged[1].start, BLOB_RANGE_MERGE_MAX_SPAN + 1);
        assert_eq!(merged[1].end, BLOB_RANGE_MERGE_MAX_SPAN + 5);
    }
}
