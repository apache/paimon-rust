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

use crate::arrow::format::blob::DEFAULT_BLOB_READ_PARALLELISM;
use crate::io::{FileIO, FileRead};
use crate::spec::BlobDescriptor;
use crate::Result;
use arrow_array::builder::LargeBinaryBuilder;
use arrow_array::{Array, LargeBinaryArray};
use bytes::Bytes;
use futures::{stream, StreamExt, TryStreamExt};
use std::collections::HashMap;
use std::io::SeekFrom;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const BLOB_RANGE_MERGE_GAP: u64 = 64 * 1024;
const BLOB_RANGE_MERGE_MAX_SPAN: u64 = 8 * 1024 * 1024;
pub(crate) const BLOB_DESCRIPTOR_READ_CONCURRENCY: usize = DEFAULT_BLOB_READ_PARALLELISM;
const BLOB_DESCRIPTOR_READ_BYTE_UNIT: u64 = 1024 * 1024;
const BLOB_DESCRIPTOR_READ_MAX_IN_FLIGHT_BYTES: u64 = 64 * 1024 * 1024;

/// Reads serialized [`BlobDescriptor`] values without requiring a table.
#[derive(Clone, Debug)]
pub struct BlobReader {
    storage_options: HashMap<String, String>,
    file_io: Option<FileIO>,
    limiter: BlobReadLimiter,
}

impl Default for BlobReader {
    fn default() -> Self {
        Self::new(HashMap::new())
    }
}

impl BlobReader {
    pub fn new(storage_options: HashMap<String, String>) -> Self {
        Self {
            storage_options,
            file_io: None,
            limiter: BlobReadLimiter::new(),
        }
    }

    /// Create a reader that reuses an existing FileIO.
    pub fn from_file_io(file_io: FileIO) -> Self {
        Self {
            storage_options: HashMap::new(),
            file_io: Some(file_io),
            limiter: BlobReadLimiter::new(),
        }
    }

    /// Open one descriptor for incremental reads.
    pub fn open_blob(&self, bytes: &[u8]) -> Result<BlobStream> {
        let descriptor = BlobDescriptor::deserialize(bytes)
            .map_err(|error| blob_error_with_context(error, &[0], None))?;
        let range = descriptor
            .range_spec()
            .map_err(|error| blob_error_with_context(error, &[0], Some(descriptor.uri())))?;
        let file_io = match &self.file_io {
            Some(file_io) => file_io.clone(),
            None => FileIO::from_path(descriptor.uri())
                .and_then(|builder| builder.with_props(self.storage_options.iter()).build())
                .map_err(|error| blob_error_with_context(error, &[0], Some(descriptor.uri())))?,
        };

        Ok(BlobStream {
            file_io,
            uri: descriptor.uri().to_string(),
            offset: range.offset(),
            length: range.length(),
            position: 0,
            limiter: self.limiter.clone(),
        })
    }

    /// Read a descriptor batch in input order.
    pub async fn read_blobs(&self, descriptors: &[Vec<u8>]) -> Result<Vec<Vec<u8>>> {
        let mut by_uri = HashMap::<String, Vec<(usize, BlobDescriptor)>>::new();
        for (index, bytes) in descriptors.iter().enumerate() {
            let descriptor = BlobDescriptor::deserialize(bytes)
                .map_err(|error| blob_error_with_context(error, &[index], None))?;
            descriptor.range_spec().map_err(|error| {
                blob_error_with_context(error, &[index], Some(descriptor.uri()))
            })?;
            by_uri
                .entry(descriptor.uri().to_string())
                .or_default()
                .push((index, descriptor));
        }

        let limiter = self.limiter.clone();
        let blob_parallelism = limiter.parallelism();
        let groups: Vec<Vec<(usize, Vec<u8>)>> = stream::iter(by_uri)
            .map(|(uri, entries)| {
                let limiter = limiter.clone();
                async move {
                    let indices = entries.iter().map(|(index, _)| *index).collect::<Vec<_>>();
                    let file_io = match &self.file_io {
                        Some(file_io) => file_io.clone(),
                        None => FileIO::from_path(&uri)
                            .and_then(|builder| {
                                builder.with_props(self.storage_options.iter()).build()
                            })
                            .map_err(|error| {
                                blob_error_with_context(error, &indices, Some(&uri))
                            })?,
                    };
                    let mut builder = LargeBinaryBuilder::with_capacity(entries.len(), 0);
                    for (_, descriptor) in &entries {
                        builder.append_value(
                            BlobDescriptor::new(
                                descriptor.uri().to_string(),
                                descriptor.offset(),
                                descriptor.length(),
                            )
                            .serialize(),
                        );
                    }
                    let resolved = resolve_blob_column(&builder.finish(), &file_io, limiter)
                        .await
                        .map_err(|error| blob_error_with_context(error, &indices, Some(&uri)))?;
                    Ok::<_, crate::Error>(
                        entries
                            .into_iter()
                            .enumerate()
                            .map(|(position, (index, _))| {
                                (index, resolved.value(position).to_vec())
                            })
                            .collect(),
                    )
                }
            })
            .buffer_unordered(blob_parallelism)
            .try_collect()
            .await?;

        let mut values = groups.into_iter().flatten().collect::<Vec<_>>();
        values.sort_unstable_by_key(|(index, _)| *index);
        Ok(values.into_iter().map(|(_, value)| value).collect())
    }
}

/// Incremental reader for one serialized [`BlobDescriptor`].
#[derive(Debug)]
pub struct BlobStream {
    file_io: FileIO,
    uri: String,
    offset: u64,
    length: Option<u64>,
    position: u64,
    limiter: BlobReadLimiter,
}

impl BlobStream {
    /// Read at most `max_bytes`, returning an empty buffer at end of stream.
    pub async fn read(&mut self, max_bytes: usize) -> Result<Vec<u8>> {
        self.read_inner(max_bytes)
            .await
            .map_err(|error| blob_error_with_context(error, &[0], Some(&self.uri)))
    }

    /// Seek within the descriptor range.
    pub async fn seek(&mut self, from: SeekFrom) -> Result<u64> {
        self.seek_inner(from)
            .await
            .map_err(|error| blob_error_with_context(error, &[0], Some(&self.uri)))
    }

    async fn seek_inner(&mut self, from: SeekFrom) -> Result<u64> {
        let position = match from {
            SeekFrom::Start(position) => i128::from(position),
            SeekFrom::Current(offset) => i128::from(self.position) + i128::from(offset),
            SeekFrom::End(offset) => i128::from(self.length().await?) + i128::from(offset),
        };
        self.position = u64::try_from(position).map_err(|_| crate::Error::DataInvalid {
            message: "invalid BlobDescriptor stream seek".to_string(),
            source: None,
        })?;
        Ok(self.position)
    }

    async fn length(&mut self) -> Result<u64> {
        if let Some(length) = self.length {
            return Ok(length);
        }
        let input = self.file_io.new_input(&self.uri)?;
        let _permit = self.limiter.acquire_request(&self.uri, "metadata").await?;
        let length = input.metadata().await?.size.saturating_sub(self.offset);
        self.length = Some(length);
        Ok(length)
    }

    async fn read_inner(&mut self, max_bytes: usize) -> Result<Vec<u8>> {
        if max_bytes == 0 {
            return Ok(Vec::new());
        }

        let remaining = self.length().await?.saturating_sub(self.position);
        if remaining == 0 {
            return Ok(Vec::new());
        }

        let length = remaining.min(u64::try_from(max_bytes).unwrap_or(u64::MAX));
        let start =
            self.offset
                .checked_add(self.position)
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: "BlobDescriptor stream position overflows u64".to_string(),
                    source: None,
                })?;
        let end = start
            .checked_add(length)
            .ok_or_else(|| crate::Error::DataInvalid {
                message: "BlobDescriptor stream range overflows u64".to_string(),
                source: None,
            })?;
        let input = self.file_io.new_input(&self.uri)?;
        let reader = input.reader().await?;
        let _permits = self.limiter.acquire_read(length, &self.uri).await?;
        let bytes = reader.read(start..end).await?;
        if bytes.len() as u64 != length {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "short read for range {start}..{end}, expected={length} bytes, actual={} bytes",
                    bytes.len()
                ),
                source: None,
            });
        }
        self.position += length;
        Ok(bytes.to_vec())
    }
}

fn blob_error_with_context(
    error: crate::Error,
    indices: &[usize],
    uri: Option<&str>,
) -> crate::Error {
    let location = match uri {
        Some(uri) => format!(
            "input indices {indices:?}, URI '{}'",
            sanitize_blob_uri(uri)
        ),
        None => format!("input indices {indices:?}, URI unavailable"),
    };
    let sanitize = |message: String| match uri {
        Some(uri) => message.replace(uri, &sanitize_blob_uri(uri)),
        None => message,
    };
    match error {
        crate::Error::Unsupported { message } => crate::Error::Unsupported {
            message: format!("BlobDescriptor {location}: {}", sanitize(message)),
        },
        crate::Error::IoUnsupported { message } => crate::Error::IoUnsupported {
            message: format!("BlobDescriptor {location}: {}", sanitize(message)),
        },
        crate::Error::ConfigInvalid { .. } => crate::Error::ConfigInvalid {
            message: format!("BlobDescriptor {location}: invalid storage URI or options"),
        },
        crate::Error::DataInvalid { message, .. } => crate::Error::DataInvalid {
            message: format!("BlobDescriptor {location}: {}", sanitize(message)),
            source: None,
        },
        crate::Error::IoUnexpected { source, .. }
            if source.kind() == opendal::ErrorKind::NotFound =>
        {
            crate::Error::UnexpectedError {
                message: format!("BlobDescriptor {location}: object not found"),
                source: None,
            }
        }
        crate::Error::IoUnexpected { .. } => crate::Error::UnexpectedError {
            message: format!("BlobDescriptor {location}: storage I/O failed"),
            source: None,
        },
        crate::Error::UnexpectedError { message, .. } => crate::Error::UnexpectedError {
            message: format!("BlobDescriptor {location}: {}", sanitize(message)),
            source: None,
        },
        _ => crate::Error::UnexpectedError {
            message: format!("BlobDescriptor {location}: operation failed"),
            source: None,
        },
    }
}

fn sanitize_blob_uri(uri: &str) -> String {
    if let Ok(mut url) = url::Url::parse(uri) {
        let _ = url.set_username("");
        let _ = url.set_password(None);
        url.set_query(None);
        url.set_fragment(None);
        return url.to_string();
    }
    uri.split(['?', '#']).next().unwrap_or(uri).to_string()
}

/// Shared admission control for external descriptor metadata and range reads.
///
/// The byte semaphore budgets active range I/O only. A single range larger than
/// the budget consumes every byte permit and runs alone, but can still allocate
/// more than the configured budget because the complete value is required.
#[derive(Clone, Debug)]
pub(crate) struct BlobReadLimiter {
    requests: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    byte_unit: u64,
    max_byte_permits: u32,
    parallelism: usize,
}

impl BlobReadLimiter {
    pub(crate) fn new() -> Self {
        Self::with_limits(
            BLOB_DESCRIPTOR_READ_CONCURRENCY,
            BLOB_DESCRIPTOR_READ_MAX_IN_FLIGHT_BYTES,
            BLOB_DESCRIPTOR_READ_BYTE_UNIT,
        )
    }

    pub(crate) fn with_parallelism(parallelism: usize) -> Self {
        Self::with_limits(
            parallelism,
            BLOB_DESCRIPTOR_READ_MAX_IN_FLIGHT_BYTES,
            BLOB_DESCRIPTOR_READ_BYTE_UNIT,
        )
    }

    pub(crate) fn parallelism(&self) -> usize {
        self.parallelism
    }

    fn with_limits(request_limit: usize, byte_budget: u64, byte_unit: u64) -> Self {
        assert!(request_limit > 0);
        assert!(byte_budget > 0);
        assert!(byte_unit > 0);
        let max_byte_permits = byte_budget.div_ceil(byte_unit);
        assert!(max_byte_permits <= u32::MAX as u64);
        Self {
            requests: Arc::new(Semaphore::new(request_limit)),
            bytes: Arc::new(Semaphore::new(max_byte_permits as usize)),
            byte_unit,
            max_byte_permits: max_byte_permits as u32,
            parallelism: request_limit,
        }
    }

    async fn acquire_read(
        &self,
        length: u64,
        uri: &str,
    ) -> Result<(OwnedSemaphorePermit, OwnedSemaphorePermit)> {
        let request_permit = self.acquire_request(uri, "range read").await?;
        let byte_permits = length
            .div_ceil(self.byte_unit)
            .max(1)
            .min(self.max_byte_permits as u64) as u32;
        let byte_permit = self
            .bytes
            .clone()
            .acquire_many_owned(byte_permits)
            .await
            .map_err(|e| crate::Error::UnexpectedError {
                message: format!(
                    "Failed to acquire BlobDescriptor byte permits for URI '{uri}': {e}"
                ),
                source: Some(Box::new(e)),
            })?;
        Ok((request_permit, byte_permit))
    }

    async fn acquire_request(&self, uri: &str, operation: &str) -> Result<OwnedSemaphorePermit> {
        self.requests
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| crate::Error::UnexpectedError {
                message: format!(
                    "Failed to acquire BlobDescriptor {operation} permit for URI '{uri}': {e}"
                ),
                source: Some(Box::new(e)),
            })
    }
}

/// For each row in a blob column, if the value is a serialized `BlobDescriptor`,
/// resolve it by reading the actual data from the referenced URI+offset+length.
/// Raw data values are passed through unchanged.
pub(crate) async fn resolve_blob_column(
    col: &LargeBinaryArray,
    file_io: &FileIO,
    limiter: BlobReadLimiter,
) -> Result<LargeBinaryArray> {
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

    let mut read_groups = Vec::with_capacity(requests_by_uri.len());
    for (uri, requests) in requests_by_uri {
        let input = file_io.new_input(&uri)?;
        let file_size = if requests.iter().any(|request| request.length.is_none()) {
            let _metadata_permit = limiter.acquire_request(&uri, "metadata").await?;
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

        let reader: Arc<dyn FileRead> = Arc::new(input.reader().await?);
        read_groups.push(BlobReadGroup {
            uri,
            reader,
            reads: merge_blob_read_requests(bounded_requests),
        });
    }

    for ResolvedMergedBlobRead { merged, data } in read_blob_groups(read_groups, limiter).await? {
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

    let mut builder = LargeBinaryBuilder::with_capacity(col.len(), value_capacity);
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

struct ResolvedMergedBlobRead {
    merged: MergedBlobRead,
    data: Bytes,
}

struct BlobReadGroup {
    uri: String,
    reader: Arc<dyn FileRead>,
    reads: Vec<MergedBlobRead>,
}

async fn read_blob_groups(
    groups: Vec<BlobReadGroup>,
    limiter: BlobReadLimiter,
) -> Result<Vec<ResolvedMergedBlobRead>> {
    let blob_parallelism = limiter.parallelism();
    let grouped_results: Vec<Vec<ResolvedMergedBlobRead>> =
        stream::iter(groups)
            .map(|group| {
                let limiter = limiter.clone();
                async move {
                    read_merged_blob_ranges(&group.uri, group.reader, group.reads, limiter).await
                }
            })
            .buffer_unordered(blob_parallelism)
            .try_collect()
            .await?;
    Ok(grouped_results.into_iter().flatten().collect())
}

async fn read_merged_blob_ranges(
    uri: &str,
    reader: Arc<dyn FileRead>,
    reads: Vec<MergedBlobRead>,
    limiter: BlobReadLimiter,
) -> Result<Vec<ResolvedMergedBlobRead>> {
    let blob_parallelism = limiter.parallelism();
    stream::iter(reads)
        .map(|merged| {
            let uri = uri.to_string();
            let reader = reader.clone();
            let limiter = limiter.clone();
            async move {
                let _permits = limiter
                    .acquire_read(merged.end - merged.start, &uri)
                    .await?;
                let data = reader
                    .read(merged.start..merged.end)
                    .await
                    .map_err(|e| crate::Error::UnexpectedError {
                        message: format!(
                            "Failed to read BlobDescriptor URI '{uri}' range {}..{}: {e}",
                            merged.start, merged.end
                        ),
                        source: Some(Box::new(e)),
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
                Ok(ResolvedMergedBlobRead { merged, data })
            }
        })
        .buffer_unordered(blob_parallelism)
        .try_collect()
        .await
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

    #[derive(Clone)]
    struct TrackingFileRead {
        bytes: Bytes,
        in_flight: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        max_in_flight: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        ranges: std::sync::Arc<std::sync::Mutex<Vec<std::ops::Range<u64>>>>,
    }

    impl TrackingFileRead {
        fn new(bytes: Bytes) -> Self {
            Self {
                bytes,
                in_flight: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                max_in_flight: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                ranges: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }

        fn with_counters(
            bytes: Bytes,
            in_flight: std::sync::Arc<std::sync::atomic::AtomicUsize>,
            max_in_flight: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        ) -> Self {
            Self {
                bytes,
                in_flight,
                max_in_flight,
                ranges: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }

        fn max_in_flight(&self) -> usize {
            self.max_in_flight.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn ranges(&self) -> Vec<std::ops::Range<u64>> {
            self.ranges.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl FileRead for TrackingFileRead {
        async fn read(&self, range: std::ops::Range<u64>) -> crate::Result<Bytes> {
            self.ranges.lock().unwrap().push(range.clone());
            let in_flight = self
                .in_flight
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            self.max_in_flight
                .fetch_max(in_flight, std::sync::atomic::Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            self.in_flight
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            Ok(self.bytes.slice(range.start as usize..range.end as usize))
        }
    }

    struct ShortFileRead;

    #[async_trait::async_trait]
    impl FileRead for ShortFileRead {
        async fn read(&self, _range: std::ops::Range<u64>) -> crate::Result<Bytes> {
            Ok(Bytes::from_static(b"x"))
        }
    }

    #[tokio::test]
    async fn test_blob_range_reads_use_bounded_parallelism() {
        let reader = TrackingFileRead::new(Bytes::from_static(b"abcdefghijkl"));
        let reads = (0..12)
            .map(|row| MergedBlobRead {
                start: row,
                end: row + 1,
                requests: vec![BlobReadRequest {
                    row: row as usize,
                    offset: row,
                    length: 1,
                }],
            })
            .collect();

        let results = read_merged_blob_ranges(
            "memory:/blob.bin",
            std::sync::Arc::new(reader.clone()),
            reads,
            BlobReadLimiter::new(),
        )
        .await
        .unwrap();

        assert_eq!(results.len(), 12);
        assert!(reader.max_in_flight() > 1);
        assert!(reader.max_in_flight() <= BLOB_DESCRIPTOR_READ_CONCURRENCY);
    }

    #[tokio::test]
    async fn test_blob_range_reads_honor_configured_parallelism() {
        let reader = TrackingFileRead::new(Bytes::from_static(b"abcdefghijkl"));
        let reads = (0..12)
            .map(|row| MergedBlobRead {
                start: row,
                end: row + 1,
                requests: vec![BlobReadRequest {
                    row: row as usize,
                    offset: row,
                    length: 1,
                }],
            })
            .collect();

        let results = read_merged_blob_ranges(
            "memory:/blob.bin",
            std::sync::Arc::new(reader.clone()),
            reads,
            BlobReadLimiter::with_parallelism(2),
        )
        .await
        .unwrap();

        assert_eq!(results.len(), 12);
        assert_eq!(reader.max_in_flight(), 2);
    }

    #[tokio::test]
    async fn test_blob_range_reads_apply_byte_budget_and_preserve_rows() {
        let reader = TrackingFileRead::new(Bytes::from_static(b"abcdefgh"));
        let reads = vec![
            MergedBlobRead {
                start: 4,
                end: 8,
                requests: vec![BlobReadRequest {
                    row: 0,
                    offset: 4,
                    length: 4,
                }],
            },
            MergedBlobRead {
                start: 0,
                end: 4,
                requests: vec![BlobReadRequest {
                    row: 1,
                    offset: 0,
                    length: 4,
                }],
            },
        ];

        let results = read_merged_blob_ranges(
            "memory:/blob.bin",
            std::sync::Arc::new(reader.clone()),
            reads,
            BlobReadLimiter::with_limits(8, 4, 1),
        )
        .await
        .unwrap();

        let mut by_row = results
            .into_iter()
            .map(|result| (result.merged.requests[0].row, result.data))
            .collect::<Vec<_>>();
        by_row.sort_by_key(|(row, _)| *row);
        assert_eq!(by_row[0], (0, Bytes::from_static(b"efgh")));
        assert_eq!(by_row[1], (1, Bytes::from_static(b"abcd")));
        assert_eq!(reader.max_in_flight(), 1);
    }

    #[tokio::test]
    async fn test_blob_range_reads_overlap_across_uris() {
        let in_flight = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let max_in_flight = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let groups = b"ab"
            .iter()
            .copied()
            .enumerate()
            .map(|(row, value)| BlobReadGroup {
                uri: format!("memory:/blob-{row}.bin"),
                reader: std::sync::Arc::new(TrackingFileRead::with_counters(
                    Bytes::from(vec![value]),
                    in_flight.clone(),
                    max_in_flight.clone(),
                )),
                reads: vec![MergedBlobRead {
                    start: 0,
                    end: 1,
                    requests: vec![BlobReadRequest {
                        row,
                        offset: 0,
                        length: 1,
                    }],
                }],
            })
            .collect();

        let results = read_blob_groups(groups, BlobReadLimiter::new())
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(max_in_flight.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_blob_read_limiter_is_shared_by_metadata_and_ranges() {
        let limiter = BlobReadLimiter::with_limits(1, 4, 1);
        let metadata_permit = limiter
            .acquire_request("memory:/blob.bin", "metadata")
            .await
            .unwrap();

        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(10),
            limiter.acquire_read(1, "memory:/blob.bin")
        )
        .await
        .is_err());

        drop(metadata_permit);
        let _permits = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            limiter.acquire_read(1, "memory:/blob.bin"),
        )
        .await
        .unwrap()
        .unwrap();
    }

    #[tokio::test]
    async fn test_merged_descriptors_issue_one_underlying_read() {
        let reader = TrackingFileRead::new(Bytes::from_static(b"abcdefghijkl"));
        let reads = merge_blob_read_requests(vec![
            BlobReadRequest {
                row: 0,
                offset: 0,
                length: 4,
            },
            BlobReadRequest {
                row: 1,
                offset: 4,
                length: 4,
            },
            BlobReadRequest {
                row: 2,
                offset: 2,
                length: 6,
            },
        ]);

        let results = read_merged_blob_ranges(
            "memory:/blob.bin",
            Arc::new(reader.clone()),
            reads,
            BlobReadLimiter::new(),
        )
        .await
        .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(reader.ranges(), vec![0..8]);
    }

    #[tokio::test]
    async fn test_blob_range_read_rejects_short_data() {
        let error = read_merged_blob_ranges(
            "memory:/blob.bin",
            Arc::new(ShortFileRead),
            vec![MergedBlobRead {
                start: 4,
                end: 8,
                requests: vec![BlobReadRequest {
                    row: 0,
                    offset: 4,
                    length: 4,
                }],
            }],
            BlobReadLimiter::new(),
        )
        .await
        .err()
        .expect("short read must fail");

        assert!(error.to_string().contains("short read"));
    }

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

    fn java_v2_descriptor(uri: &str, offset: i64, length: i64) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.push(2);
        bytes.extend_from_slice(&0x424C4F4244455343_u64.to_le_bytes());
        bytes.extend_from_slice(&(uri.len() as i32).to_le_bytes());
        bytes.extend_from_slice(uri.as_bytes());
        bytes.extend_from_slice(&offset.to_le_bytes());
        bytes.extend_from_slice(&length.to_le_bytes());
        bytes
    }

    fn java_v1_descriptor(uri: &str, offset: i64, length: i64) -> Vec<u8> {
        let mut bytes = vec![1];
        bytes.extend_from_slice(&(uri.len() as i32).to_le_bytes());
        bytes.extend_from_slice(uri.as_bytes());
        bytes.extend_from_slice(&offset.to_le_bytes());
        bytes.extend_from_slice(&length.to_le_bytes());
        bytes
    }

    fn file_uri(path: &std::path::Path) -> String {
        url::Url::from_file_path(path).unwrap().to_string()
    }

    #[tokio::test]
    async fn test_standalone_blob_reader_reads_ranges_in_input_order() {
        let first = tempfile::NamedTempFile::new().unwrap();
        let second = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(first.path(), b"abcdefghij").unwrap();
        std::fs::write(second.path(), b"UVWXYZ").unwrap();
        let first_uri = file_uri(first.path());
        let second_uri = file_uri(second.path());
        let descriptors = vec![
            java_v2_descriptor(&second_uri, 1, 3),
            java_v2_descriptor(&first_uri, 3, -1),
            java_v2_descriptor(&first_uri, 5, 0),
            java_v2_descriptor(&first_uri, 2, 4),
            java_v2_descriptor(&first_uri, 2, 4),
        ];

        let values = BlobReader::default()
            .read_blobs(&descriptors)
            .await
            .unwrap();

        assert_eq!(
            values,
            vec![
                b"VWX".to_vec(),
                b"defghij".to_vec(),
                Vec::new(),
                b"cdef".to_vec(),
                b"cdef".to_vec(),
            ]
        );
    }

    #[tokio::test]
    async fn test_standalone_blob_reader_reads_java_v1_descriptor() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), b"abcdefghij").unwrap();
        let descriptor = java_v1_descriptor(&file_uri(file.path()), 2, 4);

        let values = BlobReader::default()
            .read_blobs(&[descriptor])
            .await
            .unwrap();

        assert_eq!(values, vec![b"cdef".to_vec()]);
    }

    #[tokio::test]
    async fn test_standalone_blob_reader_validates_input() {
        let reader = BlobReader::default();

        let error = reader.read_blobs(&[Vec::new()]).await.unwrap_err();
        assert!(error.to_string().contains("input indices [0]"));

        let error = reader
            .read_blobs(&[BlobDescriptor::new("file:///tmp/a".to_string(), -1, 1).serialize()])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("offset must be non-negative"));

        let secret_uri = "ftp://access-key:secret@example.com/a?token=sensitive";
        let error = reader
            .read_blobs(&[BlobDescriptor::new(secret_uri.to_string(), 0, 1).serialize()])
            .await
            .unwrap_err();
        let message = error.to_string();
        assert!(message.contains("ftp://example.com/a"));
        assert!(!message.contains("access-key"));
        assert!(!message.contains("sensitive"));
    }

    #[tokio::test]
    async fn test_standalone_blob_reader_empty_batch() {
        assert!(BlobReader::default()
            .read_blobs(&[])
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn test_blob_stream_reads_and_seeks() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), b"abcdefghij").unwrap();
        let uri = file_uri(file.path());
        let reader = BlobReader::default();

        let mut fixed = reader.open_blob(&java_v2_descriptor(&uri, 2, 5)).unwrap();
        assert_eq!(fixed.read(2).await.unwrap(), b"cd");
        assert_eq!(fixed.seek(SeekFrom::Start(1)).await.unwrap(), 1);
        assert_eq!(fixed.read(2).await.unwrap(), b"de");
        assert_eq!(fixed.seek(SeekFrom::Current(-1)).await.unwrap(), 2);
        assert_eq!(fixed.read(2).await.unwrap(), b"ef");
        assert_eq!(fixed.seek(SeekFrom::End(-2)).await.unwrap(), 3);
        assert_eq!(fixed.read(8).await.unwrap(), b"fg");
        assert!(fixed.read(1).await.unwrap().is_empty());
        assert!(fixed.seek(SeekFrom::Current(-6)).await.is_err());

        let mut to_end = reader.open_blob(&java_v1_descriptor(&uri, 4, -1)).unwrap();
        assert_eq!(to_end.seek(SeekFrom::End(-3)).await.unwrap(), 3);
        assert_eq!(to_end.read(3).await.unwrap(), b"hij");
        assert_eq!(to_end.seek(SeekFrom::Start(0)).await.unwrap(), 0);
        assert_eq!(to_end.read(8).await.unwrap(), b"efghij");

        let mut empty = reader.open_blob(&java_v2_descriptor(&uri, 3, 0)).unwrap();
        assert!(empty.read(1).await.unwrap().is_empty());

        let mut short = reader.open_blob(&java_v2_descriptor(&uri, 8, 4)).unwrap();
        assert!(short.read(4).await.is_err());
    }

    #[tokio::test]
    async fn test_blob_stream_is_lazy_and_validates_input() {
        let reader = BlobReader::default();
        let directory = tempfile::tempdir().unwrap();
        let missing = file_uri(&directory.path().join("missing"));
        let mut stream = reader
            .open_blob(&java_v2_descriptor(&missing, 0, -1))
            .unwrap();

        assert!(stream.read(0).await.unwrap().is_empty());
        let error = stream.read(1).await.unwrap_err().to_string();
        assert!(error.contains("input indices [0]"));
        assert!(error.contains("object not found") || error.contains("storage I/O failed"));

        assert!(reader.open_blob(&[]).is_err());
        assert!(reader
            .open_blob(&BlobDescriptor::new("file:///tmp/a".to_string(), -1, 1).serialize())
            .is_err());
    }
}
