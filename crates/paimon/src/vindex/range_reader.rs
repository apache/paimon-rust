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

use crate::io::FileRead;
#[cfg(test)]
use crate::spec::DEFAULT_GLOBAL_INDEX_VINDEX_READ_THREAD_NUM;
use crate::vindex::vector_search_timing_enabled;
use bytes::Bytes;
use futures::{stream, StreamExt};
use paimon_vindex_core::io::{ReadRequest, SeekRead, SeekReadCapabilities};
use std::io;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

const SCALAR_READ_MAX: usize = 64;
const SCALAR_READ_AHEAD: u64 = 64 * 1024;
const RANGE_COALESCE_GAP: u64 = 16 * 1024;

struct CachedRange {
    start: u64,
    data: Bytes,
}

impl CachedRange {
    fn end(&self) -> u64 {
        self.start + self.data.len() as u64
    }

    fn contains(&self, range: &Range<u64>) -> bool {
        self.start <= range.start && range.end <= self.end()
    }
}

struct RequestedRange {
    request_index: usize,
    range: Range<u64>,
}

struct MergedRange {
    range: Range<u64>,
    request_indices: Vec<usize>,
    requested_bytes: u64,
}

struct InFlightRead<'a>(&'a AtomicU64);

impl Drop for InFlightRead<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Default)]
pub(crate) struct RangeIoStats {
    logical_ranges: AtomicU64,
    requested_bytes: AtomicU64,
    file_read_calls: AtomicU64,
    returned_bytes: AtomicU64,
    read_ahead_hits: AtomicU64,
    io_wait_nanos: AtomicU64,
    range_permit_wait_nanos: AtomicU64,
    in_flight_reads: AtomicU64,
    peak_in_flight_reads: AtomicU64,
    read_many_merged_ranges: AtomicU64,
    read_many_chunks: AtomicU64,
    read_many_chunk_size_sum: AtomicU64,
    read_many_chunk_size_min: AtomicU64,
    read_many_chunk_size_max: AtomicU64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RangeIoStatsSnapshot {
    pub(crate) logical_ranges: u64,
    pub(crate) requested_bytes: u64,
    pub(crate) file_read_calls: u64,
    pub(crate) returned_bytes: u64,
    pub(crate) read_ahead_hits: u64,
    pub(crate) io_wait_nanos: u64,
    pub(crate) range_permit_wait_nanos: u64,
    pub(crate) peak_in_flight_reads: u64,
    pub(crate) read_many_merged_ranges: u64,
    pub(crate) read_many_chunks: u64,
    pub(crate) read_many_chunk_size_sum: u64,
    pub(crate) read_many_chunk_size_min: u64,
    pub(crate) read_many_chunk_size_max: u64,
}

impl RangeIoStats {
    pub(crate) fn snapshot(&self) -> RangeIoStatsSnapshot {
        RangeIoStatsSnapshot {
            logical_ranges: self.logical_ranges.load(Ordering::Relaxed),
            requested_bytes: self.requested_bytes.load(Ordering::Relaxed),
            file_read_calls: self.file_read_calls.load(Ordering::Relaxed),
            returned_bytes: self.returned_bytes.load(Ordering::Relaxed),
            read_ahead_hits: self.read_ahead_hits.load(Ordering::Relaxed),
            io_wait_nanos: self.io_wait_nanos.load(Ordering::Relaxed),
            range_permit_wait_nanos: self.range_permit_wait_nanos.load(Ordering::Relaxed),
            peak_in_flight_reads: self.peak_in_flight_reads.load(Ordering::Relaxed),
            read_many_merged_ranges: self.read_many_merged_ranges.load(Ordering::Relaxed),
            read_many_chunks: self.read_many_chunks.load(Ordering::Relaxed),
            read_many_chunk_size_sum: self.read_many_chunk_size_sum.load(Ordering::Relaxed),
            read_many_chunk_size_min: self.read_many_chunk_size_min.load(Ordering::Relaxed),
            read_many_chunk_size_max: self.read_many_chunk_size_max.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone)]
pub(crate) struct RangeReadLimiter {
    io_permits: Arc<tokio::sync::Semaphore>,
    response_permits: Arc<tokio::sync::Semaphore>,
    io_limit: usize,
    response_limit: usize,
}

impl RangeReadLimiter {
    pub(crate) fn new(io_limit: usize) -> Self {
        let response_limit = io_limit
            .saturating_mul(2)
            .min(tokio::sync::Semaphore::MAX_PERMITS);
        Self {
            io_permits: Arc::new(tokio::sync::Semaphore::new(io_limit)),
            response_permits: Arc::new(tokio::sync::Semaphore::new(response_limit)),
            io_limit,
            response_limit,
        }
    }
}

struct RangeResponse {
    data: Bytes,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

/// Bridges vindex-core's synchronous positional reads to Paimon's asynchronous
/// range reader. This type is consumed from a blocking search task; it captures
/// the surrounding Tokio runtime so remote storage reads still run asynchronously.
pub(crate) struct VindexFileReader {
    reader: Arc<dyn FileRead>,
    runtime: tokio::runtime::Handle,
    limiter: RangeReadLimiter,
    file_size: u64,
    path: String,
    scalar_cache: Option<CachedRange>,
    stats: Option<Arc<RangeIoStats>>,
}

impl VindexFileReader {
    #[cfg(test)]
    pub(crate) fn new(
        reader: Arc<dyn FileRead>,
        runtime: tokio::runtime::Handle,
        file_size: u64,
        path: String,
    ) -> Self {
        Self::new_with_limiter(
            reader,
            runtime,
            RangeReadLimiter::new(DEFAULT_GLOBAL_INDEX_VINDEX_READ_THREAD_NUM),
            file_size,
            path,
        )
    }

    pub(crate) fn new_with_limiter(
        reader: Arc<dyn FileRead>,
        runtime: tokio::runtime::Handle,
        limiter: RangeReadLimiter,
        file_size: u64,
        path: String,
    ) -> Self {
        Self {
            reader,
            runtime,
            limiter,
            file_size,
            path,
            scalar_cache: None,
            stats: vector_search_timing_enabled().then(|| Arc::new(RangeIoStats::default())),
        }
    }

    pub(crate) fn range_io_stats(&self) -> Option<Arc<RangeIoStats>> {
        self.stats.clone()
    }

    fn validate_range(&self, pos: u64, len: usize) -> io::Result<Range<u64>> {
        let end = pos.checked_add(len as u64).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("vindex read range overflows for '{}'", self.path),
            )
        })?;
        if end > self.file_size {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "vindex read range {pos}..{end} exceeds file '{}' size {}",
                    self.path, self.file_size
                ),
            ));
        }
        Ok(pos..end)
    }

    fn read_one(&mut self, pos: u64, buf: &mut [u8]) -> io::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        let range = self.validate_range(pos, buf.len())?;
        if buf.len() <= SCALAR_READ_MAX {
            if let Some(cache) = &self.scalar_cache {
                if cache.contains(&range) {
                    if let Some(stats) = &self.stats {
                        stats.read_ahead_hits.fetch_add(1, Ordering::Relaxed);
                    }
                    let start = (range.start - cache.start) as usize;
                    buf.copy_from_slice(&cache.data[start..start + buf.len()]);
                    return Ok(());
                }
            }

            let read_end = range
                .start
                .saturating_add(SCALAR_READ_AHEAD)
                .max(range.end)
                .min(self.file_size);
            let response = self.fetch_exact(range.start..read_end)?;
            buf.copy_from_slice(&response.data[..buf.len()]);
            self.scalar_cache = Some(CachedRange {
                start: range.start,
                data: response.data,
            });
            return Ok(());
        }

        let response = self.fetch_exact(range)?;
        buf.copy_from_slice(&response.data);
        Ok(())
    }

    fn fetch_exact(&self, range: Range<u64>) -> io::Result<RangeResponse> {
        let mut result = None;
        self.fetch_range_batch(std::slice::from_ref(&range), |_, response| {
            result = Some(response);
            Ok(())
        })?;
        Ok(result.expect("one requested range"))
    }

    fn fetch_range_batch(
        &self,
        ranges: &[Range<u64>],
        mut consume: impl FnMut(usize, RangeResponse) -> io::Result<()>,
    ) -> io::Result<()> {
        let reader = Arc::clone(&self.reader);
        let io_permits = Arc::clone(&self.limiter.io_permits);
        let response_permits = Arc::clone(&self.limiter.response_permits);
        let path = self.path.clone();
        let requested = ranges.to_vec();
        let stats = self.stats.clone();
        let io_limit = self.limiter.io_limit;
        let response_limit = self.limiter.response_limit;
        let (sender, mut receiver) = tokio::sync::mpsc::channel(io_limit);
        self.runtime.spawn(async move {
            let fetched = stream::iter(requested.into_iter().enumerate().map(|(index, range)| {
                let reader = Arc::clone(&reader);
                let io_permits = Arc::clone(&io_permits);
                let response_permits = Arc::clone(&response_permits);
                let sender = sender.clone();
                let path = path.clone();
                let stats = stats.clone();
                async move {
                    let response_permit = response_permits
                        .acquire_owned()
                        .await
                        .map_err(|_| {
                            io::Error::other("vindex range response limiter closed")
                        })?;
                    let permit_wait_start = stats.as_ref().map(|_| Instant::now());
                    let permit = io_permits.acquire_owned().await;
                    if let (Some(stats), Some(start)) = (&stats, permit_wait_start) {
                        stats
                            .range_permit_wait_nanos
                            .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    }
                    let io_permit = permit.map_err(|_| {
                        io::Error::other("vindex range read concurrency limiter closed")
                    })?;
                    let expected = (range.end - range.start) as usize;
                    let in_flight_read = stats.as_ref().map(|stats| {
                        stats.file_read_calls.fetch_add(1, Ordering::Relaxed);
                        let active = stats.in_flight_reads.fetch_add(1, Ordering::Relaxed) + 1;
                        stats
                            .peak_in_flight_reads
                            .fetch_max(active, Ordering::Relaxed);
                        InFlightRead(&stats.in_flight_reads)
                    });
                    let read_result = reader.read(range.clone()).await;
                    drop(in_flight_read);
                    drop(io_permit);
                    let data = read_result.map_err(|error| {
                        io::Error::other(format!(
                            "failed to read vindex file '{path}' range {}..{}: {error}",
                            range.start, range.end
                        ))
                    })?;
                    if data.len() != expected {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            format!(
                                "short read for vindex file '{path}' range {}..{}: expected {expected} bytes, got {}",
                                range.start,
                                range.end,
                                data.len()
                            ),
                        ));
                    }
                    if let Some(stats) = &stats {
                        stats
                            .returned_bytes
                            .fetch_add(data.len() as u64, Ordering::Relaxed);
                    }
                    sender
                        .send(Ok((index, data, response_permit)))
                        .await
                        .map_err(|_| io::Error::other("vindex range read receiver closed"))
                }
            }))
            .buffer_unordered(response_limit);
            futures::pin_mut!(fetched);
            while let Some(result) = fetched.next().await {
                if let Err(error) = result {
                    let _ = sender.send(Err(error)).await;
                    return;
                }
            }
        });

        let mut io_wait_nanos = 0u64;
        let result = (0..ranges.len()).try_for_each(|_| {
            let wait_start = self.stats.as_ref().map(|_| Instant::now());
            let fetched = receiver.blocking_recv();
            if let Some(start) = wait_start {
                io_wait_nanos = io_wait_nanos.saturating_add(start.elapsed().as_nanos() as u64);
            }
            let (index, data, response_permit) = fetched.ok_or_else(|| {
                io::Error::other(format!(
                    "vindex range read task for '{}' was cancelled",
                    self.path
                ))
            })??;
            consume(
                index,
                RangeResponse {
                    data,
                    _permit: response_permit,
                },
            )
        });
        if let Some(stats) = &self.stats {
            stats
                .io_wait_nanos
                .fetch_add(io_wait_nanos, Ordering::Relaxed);
        }
        result
    }

    fn read_many(&self, requests: &mut [ReadRequest<'_>]) -> io::Result<()> {
        let mut requested = Vec::with_capacity(requests.len());
        for (request_index, request) in requests.iter().enumerate() {
            if request.buf.is_empty() {
                continue;
            }
            requested.push(RequestedRange {
                request_index,
                range: self.validate_range(request.pos, request.buf.len())?,
            });
        }
        if requested.is_empty() {
            return Ok(());
        }

        requested.sort_by_key(|request| request.range.start);
        let mut merged = Vec::<MergedRange>::new();
        for request in requested {
            if let Some(last) = merged.last_mut() {
                let merged_end = last.range.end.max(request.range.end);
                let merged_bytes = merged_end - last.range.start;
                let requested_bytes = last
                    .requested_bytes
                    .saturating_add(request.range.end - request.range.start);
                let within_gap =
                    request.range.start <= last.range.end.saturating_add(RANGE_COALESCE_GAP);
                let bounded_amplification = merged_bytes <= requested_bytes.saturating_mul(2);
                if within_gap && bounded_amplification {
                    last.range.end = last.range.end.max(request.range.end);
                    last.request_indices.push(request.request_index);
                    last.requested_bytes = requested_bytes;
                    continue;
                }
            }
            let requested_bytes = request.range.end - request.range.start;
            merged.push(MergedRange {
                range: request.range,
                request_indices: vec![request.request_index],
                requested_bytes,
            });
        }

        if let Some(stats) = &self.stats {
            let chunk_size = merged.len() as u64;
            stats
                .read_many_merged_ranges
                .fetch_add(chunk_size, Ordering::Relaxed);
            stats.read_many_chunks.fetch_add(1, Ordering::Relaxed);
            stats
                .read_many_chunk_size_sum
                .fetch_add(chunk_size, Ordering::Relaxed);
            let _ = stats.read_many_chunk_size_min.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |current| {
                    Some(if current == 0 {
                        chunk_size
                    } else {
                        current.min(chunk_size)
                    })
                },
            );
            stats
                .read_many_chunk_size_max
                .fetch_max(chunk_size, Ordering::Relaxed);
        }
        let ranges: Vec<_> = merged.iter().map(|merged| merged.range.clone()).collect();
        self.fetch_range_batch(&ranges, |merged_index, response| {
            let merged_range = &merged[merged_index];
            for &request_index in &merged_range.request_indices {
                let request = &mut requests[request_index];
                let start = (request.pos - merged_range.range.start) as usize;
                request
                    .buf
                    .copy_from_slice(&response.data[start..start + request.buf.len()]);
            }
            Ok(())
        })
    }
}

impl SeekRead for VindexFileReader {
    fn pread(&mut self, requests: &mut [ReadRequest<'_>]) -> io::Result<()> {
        if let Some(stats) = &self.stats {
            let (logical_ranges, requested_bytes) = requests
                .iter()
                .filter(|request| !request.buf.is_empty())
                .fold((0u64, 0u64), |(ranges, bytes), request| {
                    (ranges + 1, bytes.saturating_add(request.buf.len() as u64))
                });
            stats
                .logical_ranges
                .fetch_add(logical_ranges, Ordering::Relaxed);
            stats
                .requested_bytes
                .fetch_add(requested_bytes, Ordering::Relaxed);
        }
        let non_empty = requests
            .iter()
            .filter(|request| !request.buf.is_empty())
            .count();
        if non_empty == 1 {
            let request = requests
                .iter_mut()
                .find(|request| !request.buf.is_empty())
                .expect("one non-empty request");
            self.read_one(request.pos, request.buf)
        } else {
            self.read_many(requests)
        }
    }

    fn try_clone_reader(&self) -> io::Result<Option<Self>> {
        Ok(Some(Self {
            reader: Arc::clone(&self.reader),
            runtime: self.runtime.clone(),
            limiter: self.limiter.clone(),
            file_size: self.file_size,
            path: self.path.clone(),
            scalar_cache: None,
            stats: self.stats.clone(),
        }))
    }

    fn read_capabilities(&self) -> SeekReadCapabilities {
        // `max_ranges_per_pread` is a planning hint, not an I/O concurrency limit.
        // This adapter accepts any number of ranges and limits concurrent I/O with permits.
        SeekReadCapabilities::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIO;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;

    async fn acquire_test_permits(semaphore: &tokio::sync::Semaphore, permits: u32, message: &str) {
        tokio::time::timeout(Duration::from_secs(5), semaphore.acquire_many(permits))
            .await
            .expect(message)
            .unwrap()
            .forget();
    }

    struct TrackingRead {
        data: Bytes,
        ranges: Mutex<Vec<Range<u64>>>,
        short_read: bool,
    }

    impl TrackingRead {
        fn new(data: Bytes) -> Arc<Self> {
            Arc::new(Self {
                data,
                ranges: Mutex::new(Vec::new()),
                short_read: false,
            })
        }

        fn short(data: Bytes) -> Arc<Self> {
            Arc::new(Self {
                data,
                ranges: Mutex::new(Vec::new()),
                short_read: true,
            })
        }

        fn ranges(&self) -> Vec<Range<u64>> {
            self.ranges.lock().unwrap().clone()
        }
    }

    struct RuntimeTrackingRead {
        data: Bytes,
        runtime_id: Mutex<Option<tokio::runtime::Id>>,
    }

    struct ConcurrencyTrackingRead {
        data: Bytes,
        active: AtomicUsize,
        max_active: AtomicUsize,
        started: tokio::sync::Semaphore,
        release: tokio::sync::Semaphore,
    }

    struct FailOnceRead {
        data: Bytes,
        calls: AtomicUsize,
    }

    struct DropTrackedPayload {
        data: Vec<u8>,
        dropped: Arc<tokio::sync::Semaphore>,
    }

    impl AsRef<[u8]> for DropTrackedPayload {
        fn as_ref(&self) -> &[u8] {
            &self.data
        }
    }

    impl Drop for DropTrackedPayload {
        fn drop(&mut self) {
            self.dropped.add_permits(1);
        }
    }

    struct StreamingTrackingRead {
        stride: u64,
        first_started: tokio::sync::Semaphore,
        release_first: tokio::sync::Semaphore,
        dropped: Arc<tokio::sync::Semaphore>,
    }

    struct BenchmarkRead {
        data: Bytes,
        calls: AtomicUsize,
        active: AtomicUsize,
        max_active: AtomicUsize,
        fast_delay: Duration,
        slow_every: usize,
        slow_delay: Duration,
    }

    #[async_trait]
    impl FileRead for RuntimeTrackingRead {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            *self.runtime_id.lock().unwrap() = Some(tokio::runtime::Handle::current().id());
            Ok(self.data.slice(range.start as usize..range.end as usize))
        }
    }

    #[async_trait]
    impl FileRead for ConcurrencyTrackingRead {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            self.started.add_permits(1);
            self.release.acquire().await.unwrap().forget();
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(self.data.slice(range.start as usize..range.end as usize))
        }
    }

    #[async_trait]
    impl FileRead for FailOnceRead {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(crate::Error::UnexpectedError {
                    message: "injected range read failure".to_string(),
                    source: None,
                });
            }
            Ok(self.data.slice(range.start as usize..range.end as usize))
        }
    }

    #[async_trait]
    impl FileRead for StreamingTrackingRead {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            if range.start == 0 {
                self.first_started.add_permits(1);
                acquire_test_permits(
                    &self.release_first,
                    1,
                    "test did not release the first range read",
                )
                .await;
            }
            let value = (range.start / self.stride + 1) as u8;
            Ok(Bytes::from_owner(DropTrackedPayload {
                data: vec![value; (range.end - range.start) as usize],
                dropped: Arc::clone(&self.dropped),
            }))
        }
    }

    #[async_trait]
    impl FileRead for TrackingRead {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            self.ranges.lock().unwrap().push(range.clone());
            let mut end = range.end as usize;
            if self.short_read && end > range.start as usize {
                end -= 1;
            }
            Ok(self.data.slice(range.start as usize..end))
        }
    }

    #[async_trait]
    impl FileRead for BenchmarkRead {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            let call = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
            let active = self.active.fetch_add(1, Ordering::Relaxed) + 1;
            self.max_active.fetch_max(active, Ordering::Relaxed);
            let delay = if self.slow_every != 0 && call.is_multiple_of(self.slow_every) {
                self.slow_delay
            } else {
                self.fast_delay
            };
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            self.active.fetch_sub(1, Ordering::Relaxed);
            Ok(self.data.slice(range.start as usize..range.end as usize))
        }
    }

    async fn run_range_read_benchmark(
        case: &str,
        range_count: usize,
        iterations: usize,
        fast_delay: Duration,
        slow_every: usize,
        slow_delay: Duration,
    ) {
        const RANGE_SIZE: usize = 4 * 1024;
        let concurrency = 64;
        let stride = RANGE_COALESCE_GAP as usize + RANGE_SIZE + 1;
        let source = Arc::new(BenchmarkRead {
            data: Bytes::from(vec![7u8; range_count * stride]),
            calls: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            fast_delay,
            slow_every,
            slow_delay,
        });
        let reader_source: Arc<dyn FileRead> = source.clone();
        let mut reader = VindexFileReader::new_with_limiter(
            reader_source,
            tokio::runtime::Handle::current(),
            RangeReadLimiter::new(concurrency),
            source.data.len() as u64,
            "benchmark-index".to_string(),
        );
        let task_source = source.clone();
        let elapsed = tokio::task::spawn_blocking(move || {
            let mut buffers = vec![[0u8; RANGE_SIZE]; range_count];
            let mut run_iteration = || {
                let mut requests = buffers
                    .iter_mut()
                    .enumerate()
                    .map(|(index, buffer)| {
                        ReadRequest::new((index * stride) as u64, buffer.as_mut_slice())
                    })
                    .collect::<Vec<_>>();
                reader.pread(&mut requests).unwrap();
                std::hint::black_box(&buffers);
            };

            run_iteration();
            task_source.calls.store(0, Ordering::Relaxed);
            task_source.max_active.store(0, Ordering::Relaxed);
            let start = Instant::now();
            for _ in 0..iterations {
                run_iteration();
            }
            start.elapsed()
        })
        .await
        .unwrap();

        let total_ranges = range_count * iterations;
        let total_bytes = total_ranges * RANGE_SIZE;
        let ranges_per_second = total_ranges as f64 / elapsed.as_secs_f64();
        let mib_per_second = total_bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
        let peak_in_flight = source.max_active.load(Ordering::Relaxed);
        assert_eq!(source.calls.load(Ordering::Relaxed), total_ranges);
        assert!(peak_in_flight <= concurrency);
        eprintln!(
            "vindex_range_read_benchmark case={case} profile={} concurrency={concurrency} range_bytes={RANGE_SIZE} ranges_per_iteration={range_count} iterations={iterations} elapsed_ms={:.3} ranges_per_second={ranges_per_second:.0} mib_per_second={mib_per_second:.2} peak_in_flight={peak_in_flight}",
            if cfg!(debug_assertions) {
                "debug"
            } else {
                "release"
            },
            elapsed.as_secs_f64() * 1000.0,
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "manual performance comparison; run with --release --ignored --nocapture"]
    async fn vindex_range_read_benchmark() {
        run_range_read_benchmark("hot_cache", 1024, 50, Duration::ZERO, 0, Duration::ZERO).await;
        run_range_read_benchmark(
            "oss_straggler",
            256,
            10,
            Duration::from_millis(1),
            64,
            Duration::from_millis(10),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scalar_reads_reuse_bounded_read_ahead() {
        let data = Bytes::from((0..200_000).map(|value| value as u8).collect::<Vec<_>>());
        let tracking = TrackingRead::new(data.clone());
        let source: Arc<dyn FileRead> = tracking.clone();
        let mut reader = VindexFileReader::new(
            source,
            tokio::runtime::Handle::current(),
            data.len() as u64,
            "index".to_string(),
        );

        tokio::task::spawn_blocking(move || {
            let mut first = [0u8; 8];
            reader
                .pread(&mut [ReadRequest::new(32, &mut first)])
                .unwrap();
            assert_eq!(&first, &data[32..40]);

            let mut second = [0u8; 4];
            reader
                .pread(&mut [ReadRequest::new(128, &mut second)])
                .unwrap();
            assert_eq!(&second, &data[128..132]);
        })
        .await
        .unwrap();

        assert_eq!(tracking.ranges(), vec![32..32 + SCALAR_READ_AHEAD]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn multi_range_reads_merge_only_nearby_requests() {
        let data = Bytes::from((0..100_000).map(|value| value as u8).collect::<Vec<_>>());
        let tracking = TrackingRead::new(data.clone());
        let source: Arc<dyn FileRead> = tracking.clone();
        let mut reader = VindexFileReader::new(
            source,
            tokio::runtime::Handle::current(),
            data.len() as u64,
            "index".to_string(),
        );

        tokio::task::spawn_blocking(move || {
            let mut first = [0u8; 4];
            let mut second = [0u8; 4];
            let mut third = [0u8; 4];
            reader
                .pread(&mut [
                    ReadRequest::new(0, &mut first),
                    ReadRequest::new(8, &mut second),
                    ReadRequest::new(20_000, &mut third),
                ])
                .unwrap();
            assert_eq!(&first, &data[0..4]);
            assert_eq!(&second, &data[8..12]);
            assert_eq!(&third, &data[20_000..20_004]);
        })
        .await
        .unwrap();

        assert_eq!(tracking.ranges(), vec![0..12, 20_000..20_004]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn multi_range_reads_reject_sparse_amplification() {
        let data = Bytes::from(vec![7u8; 100_000]);
        let tracking = TrackingRead::new(data.clone());
        let source: Arc<dyn FileRead> = tracking.clone();
        let mut reader = VindexFileReader::new(
            source,
            tokio::runtime::Handle::current(),
            data.len() as u64,
            "index".to_string(),
        );

        tokio::task::spawn_blocking(move || {
            let mut first = [0u8; 4];
            let mut second = [0u8; 4];
            reader
                .pread(&mut [
                    ReadRequest::new(0, &mut first),
                    ReadRequest::new(16_000, &mut second),
                ])
                .unwrap();
        })
        .await
        .unwrap();

        assert_eq!(tracking.ranges(), vec![0..4, 16_000..16_004]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn short_range_read_fails_loudly() {
        let data = Bytes::from(vec![1u8; 1024]);
        let tracking = TrackingRead::short(data.clone());
        let source: Arc<dyn FileRead> = tracking;
        let mut reader = VindexFileReader::new(
            source,
            tokio::runtime::Handle::current(),
            data.len() as u64,
            "index".to_string(),
        );

        let error = tokio::task::spawn_blocking(move || {
            let mut output = [0u8; 128];
            reader
                .pread(&mut [ReadRequest::new(256, &mut output)])
                .unwrap_err()
        })
        .await
        .unwrap();

        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        assert!(error.to_string().contains("short read"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worker_read_is_safe_on_multi_thread_runtime() {
        let data = Bytes::from(vec![3u8; 1024]);
        let source: Arc<dyn FileRead> = TrackingRead::new(data.clone());
        let mut reader = VindexFileReader::new(
            source,
            tokio::runtime::Handle::current(),
            data.len() as u64,
            "index".to_string(),
        );
        let output = tokio::task::spawn_blocking(move || {
            let mut output = [0u8; 16];
            reader
                .pread(&mut [ReadRequest::new(128, &mut output)])
                .unwrap();
            output
        })
        .await
        .unwrap();
        assert_eq!(&output, &data[128..144]);
    }

    #[tokio::test]
    async fn worker_read_is_safe_on_current_thread_runtime() {
        let data = Bytes::from(vec![5u8; 1024]);
        let source: Arc<dyn FileRead> = TrackingRead::new(data.clone());
        let mut reader = VindexFileReader::new(
            source,
            tokio::runtime::Handle::current(),
            data.len() as u64,
            "index".to_string(),
        );
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let worker = std::thread::spawn(move || {
            let mut output = [0u8; 16];
            let result = reader.pread(&mut [ReadRequest::new(256, &mut output)]);
            let _ = sender.send((result, output));
        });
        let (result, output) = receiver.await.unwrap();
        result.unwrap();
        worker.join().unwrap();
        assert_eq!(&output, &data[256..272]);
    }

    #[test]
    fn async_reads_run_on_the_calling_runtime() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let caller_runtime_id = runtime.handle().id();
        let data = Bytes::from(vec![6u8; 1024]);
        let tracking = Arc::new(RuntimeTrackingRead {
            data: data.clone(),
            runtime_id: Mutex::new(None),
        });
        let source: Arc<dyn FileRead> = tracking.clone();
        let mut reader = VindexFileReader::new(
            source,
            runtime.handle().clone(),
            data.len() as u64,
            "index".to_string(),
        );
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let worker = std::thread::spawn(move || {
            let mut output = [0u8; 16];
            let result = reader.pread(&mut [ReadRequest::new(128, &mut output)]);
            let _ = sender.send((result, output));
        });

        let (result, output) = runtime.block_on(receiver).unwrap();
        result.unwrap();
        worker.join().unwrap();
        assert_eq!(&output, &data[128..144]);
        assert_eq!(
            *tracking.runtime_id.lock().unwrap(),
            Some(caller_runtime_id)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reader_can_clone_for_parallel_search() {
        let data = Bytes::from(vec![7u8; 1024]);
        let source: Arc<dyn FileRead> = TrackingRead::new(data.clone());
        let reader = VindexFileReader::new(
            source,
            tokio::runtime::Handle::current(),
            data.len() as u64,
            "index".to_string(),
        );

        assert_eq!(reader.read_capabilities(), SeekReadCapabilities::default());
        let cloned = reader.try_clone_reader().unwrap().unwrap();
        assert_eq!(cloned.read_capabilities(), SeekReadCapabilities::default());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn range_io_stats_are_shared_across_clones() {
        let data = Bytes::from(vec![7u8; 1024]);
        let source: Arc<dyn FileRead> = TrackingRead::new(data.clone());
        let mut reader = VindexFileReader::new(
            source,
            tokio::runtime::Handle::current(),
            data.len() as u64,
            "index".to_string(),
        );
        let stats = Arc::new(RangeIoStats::default());
        reader.stats = Some(Arc::clone(&stats));
        let mut cloned = reader.try_clone_reader().unwrap().unwrap();

        tokio::task::spawn_blocking(move || {
            let mut first = [0u8; 128];
            reader
                .pread(&mut [ReadRequest::new(0, &mut first)])
                .unwrap();
            let mut second = [0u8; 128];
            cloned
                .pread(&mut [ReadRequest::new(128, &mut second)])
                .unwrap();
        })
        .await
        .unwrap();

        let stats = stats.snapshot();
        assert_eq!(
            (
                stats.logical_ranges,
                stats.requested_bytes,
                stats.file_read_calls,
                stats.returned_bytes,
                stats.read_ahead_hits,
                stats.peak_in_flight_reads,
                stats.read_many_merged_ranges,
                stats.read_many_chunks,
                stats.read_many_chunk_size_sum,
                stats.read_many_chunk_size_min,
                stats.read_many_chunk_size_max,
            ),
            (2, 256, 2, 256, 0, 1, 0, 0, 0, 0, 0)
        );
        assert!(stats.io_wait_nanos > 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn range_io_stats_count_coalesced_reads() {
        let data = Bytes::from(vec![7u8; 100_000]);
        let source: Arc<dyn FileRead> = TrackingRead::new(data.clone());
        let mut reader = VindexFileReader::new(
            source,
            tokio::runtime::Handle::current(),
            data.len() as u64,
            "index".to_string(),
        );
        let stats = Arc::new(RangeIoStats::default());
        reader.stats = Some(Arc::clone(&stats));

        tokio::task::spawn_blocking(move || {
            let mut first = [0u8; 4];
            let mut second = [0u8; 4];
            let mut third = [0u8; 4];
            reader
                .pread(&mut [
                    ReadRequest::new(0, &mut first),
                    ReadRequest::new(8, &mut second),
                    ReadRequest::new(20_000, &mut third),
                ])
                .unwrap();
            let mut fourth = [0u8; 4];
            let mut fifth = [0u8; 4];
            reader
                .pread(&mut [
                    ReadRequest::new(40, &mut fourth),
                    ReadRequest::new(48, &mut fifth),
                ])
                .unwrap();
        })
        .await
        .unwrap();

        let stats = stats.snapshot();
        assert_eq!(
            (
                stats.logical_ranges,
                stats.requested_bytes,
                stats.file_read_calls,
                stats.returned_bytes,
                stats.read_ahead_hits,
                stats.peak_in_flight_reads,
                stats.read_many_merged_ranges,
                stats.read_many_chunks,
                stats.read_many_chunk_size_sum,
                stats.read_many_chunk_size_min,
                stats.read_many_chunk_size_max,
            ),
            (5, 20, 3, 28, 0, 1, 3, 2, 3, 1, 2)
        );
        assert!(stats.io_wait_nanos > 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn clones_share_range_read_permits() {
        let data = Bytes::from(vec![8u8; 1024]);
        let tracking = Arc::new(ConcurrencyTrackingRead {
            data: data.clone(),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            started: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        });
        let limiter = RangeReadLimiter::new(1);
        let source: Arc<dyn FileRead> = tracking.clone();
        let mut first_reader = VindexFileReader::new_with_limiter(
            source,
            tokio::runtime::Handle::current(),
            limiter,
            data.len() as u64,
            "index".to_string(),
        );
        let stats = Arc::new(RangeIoStats::default());
        first_reader.stats = Some(Arc::clone(&stats));
        let mut second_reader = first_reader.try_clone_reader().unwrap().unwrap();
        assert!(Arc::ptr_eq(
            &first_reader.limiter.io_permits,
            &second_reader.limiter.io_permits
        ));
        assert!(Arc::ptr_eq(
            &first_reader.limiter.response_permits,
            &second_reader.limiter.response_permits
        ));

        let first = tokio::task::spawn_blocking(move || {
            let mut output = [0u8; 128];
            first_reader
                .pread(&mut [ReadRequest::new(0, &mut output)])
                .unwrap();
        });
        let second = tokio::task::spawn_blocking(move || {
            let mut output = [0u8; 128];
            second_reader
                .pread(&mut [ReadRequest::new(128, &mut output)])
                .unwrap();
        });
        tracking.started.acquire().await.unwrap().forget();
        assert_eq!(tracking.max_active.load(Ordering::SeqCst), 1);
        tracking.release.add_permits(1);
        tracking.started.acquire().await.unwrap().forget();
        assert_eq!(tracking.max_active.load(Ordering::SeqCst), 1);
        tracking.release.add_permits(1);
        first.await.unwrap();
        second.await.unwrap();

        assert_eq!(tracking.max_active.load(Ordering::SeqCst), 1);
        let stats = stats.snapshot();
        assert!(stats.range_permit_wait_nanos > 0);
        assert!(stats.io_wait_nanos >= stats.range_permit_wait_nanos);
    }

    #[test]
    fn response_limit_saturates_at_semaphore_max_permits() {
        let io_limit = tokio::sync::Semaphore::MAX_PERMITS / 2 + 1;
        let limiter = RangeReadLimiter::new(io_limit);

        assert_eq!(limiter.io_limit, io_limit);
        assert_eq!(limiter.response_limit, tokio::sync::Semaphore::MAX_PERMITS);
        assert_eq!(limiter.io_permits.available_permits(), io_limit);
        assert_eq!(
            limiter.response_permits.available_permits(),
            tokio::sync::Semaphore::MAX_PERMITS
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn configured_range_read_concurrency_can_exceed_32() {
        let configured_concurrency = 64;
        let range_count = configured_concurrency + 1;
        let stride = RANGE_COALESCE_GAP + 2;
        let data = Bytes::from(vec![8u8; range_count * stride as usize]);
        let tracking = Arc::new(ConcurrencyTrackingRead {
            data: data.clone(),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            started: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        });
        let source: Arc<dyn FileRead> = tracking.clone();
        let mut reader = VindexFileReader::new_with_limiter(
            source,
            tokio::runtime::Handle::current(),
            RangeReadLimiter::new(configured_concurrency),
            data.len() as u64,
            "index".to_string(),
        );
        let stats = Arc::new(RangeIoStats::default());
        reader.stats = Some(Arc::clone(&stats));

        let read = tokio::task::spawn_blocking(move || {
            let mut buffers = vec![[0u8; 1]; range_count];
            let mut requests = buffers
                .iter_mut()
                .enumerate()
                .map(|(index, buffer)| ReadRequest::new(index as u64 * stride, buffer))
                .collect::<Vec<_>>();
            reader.pread(&mut requests).unwrap();
        });

        tokio::time::timeout(
            Duration::from_secs(5),
            tracking.started.acquire_many(configured_concurrency as u32),
        )
        .await
        .expect("configured range reads did not start")
        .unwrap()
        .forget();
        assert_eq!(tracking.max_active.load(Ordering::SeqCst), 64);
        tracking.release.add_permits(configured_concurrency);
        tracking.started.acquire().await.unwrap().forget();
        tracking.release.add_permits(1);
        read.await.unwrap();

        assert_eq!(tracking.max_active.load(Ordering::SeqCst), 64);
        assert!(tracking.max_active.load(Ordering::SeqCst) > 32);
        let snapshot = stats.snapshot();
        assert_eq!(
            (
                snapshot.logical_ranges,
                snapshot.requested_bytes,
                snapshot.file_read_calls,
                snapshot.returned_bytes,
                snapshot.read_ahead_hits,
                snapshot.peak_in_flight_reads,
                snapshot.read_many_merged_ranges,
                snapshot.read_many_chunks,
                snapshot.read_many_chunk_size_sum,
                snapshot.read_many_chunk_size_min,
                snapshot.read_many_chunk_size_max,
            ),
            (65, 65, 65, 65, 0, 64, 65, 1, 65, 65, 65)
        );
        assert!(snapshot.io_wait_nanos > 0);
        assert!(snapshot.range_permit_wait_nanos > 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn range_reads_refill_before_slowest_batch_member_finishes() {
        let concurrency = 2;
        let range_count = 3;
        let stride = RANGE_COALESCE_GAP + 2;
        let data = Bytes::from(vec![8u8; range_count * stride as usize]);
        let tracking = Arc::new(ConcurrencyTrackingRead {
            data: data.clone(),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            started: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        });
        let source: Arc<dyn FileRead> = tracking.clone();
        let mut reader = VindexFileReader::new_with_limiter(
            source,
            tokio::runtime::Handle::current(),
            RangeReadLimiter::new(concurrency),
            data.len() as u64,
            "index".to_string(),
        );

        let read = tokio::task::spawn_blocking(move || {
            let mut buffers = vec![[0u8; 1]; range_count];
            let mut requests = buffers
                .iter_mut()
                .enumerate()
                .map(|(index, buffer)| ReadRequest::new(index as u64 * stride, buffer))
                .collect::<Vec<_>>();
            reader.pread(&mut requests).unwrap();
        });

        tracking
            .started
            .acquire_many(concurrency as u32)
            .await
            .unwrap()
            .forget();
        tracking.release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(1), tracking.started.acquire())
            .await
            .expect("next range did not refill while another range was still running")
            .unwrap()
            .forget();
        tracking.release.add_permits(concurrency);
        read.await.unwrap();

        assert_eq!(tracking.max_active.load(Ordering::SeqCst), concurrency);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn response_copy_and_buffers_are_bounded_across_clones() {
        let stride = RANGE_COALESCE_GAP + 2;
        let data = Bytes::from(vec![8u8; 3 * stride as usize]);
        let tracking = Arc::new(ConcurrencyTrackingRead {
            data: data.clone(),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            started: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        });
        let source: Arc<dyn FileRead> = tracking.clone();
        let first_reader = VindexFileReader::new_with_limiter(
            source,
            tokio::runtime::Handle::current(),
            RangeReadLimiter::new(1),
            data.len() as u64,
            "index".to_string(),
        );
        let second_reader = first_reader.try_clone_reader().unwrap().unwrap();
        let (copy_started_tx, copy_started_rx) = std::sync::mpsc::channel();
        let (release_copy_tx, release_copy_rx) = std::sync::mpsc::channel();

        let first = tokio::task::spawn_blocking(move || {
            first_reader
                .fetch_range_batch(&[0..1, stride..stride + 1], |index, _| {
                    if index == 0 {
                        copy_started_tx.send(()).unwrap();
                        release_copy_rx.recv().unwrap();
                    }
                    Ok(())
                })
                .unwrap();
        });

        acquire_test_permits(&tracking.started, 1, "first range read did not start").await;
        tracking.release.add_permits(1);
        copy_started_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("first response did not reach the copy stage");
        acquire_test_permits(&tracking.started, 1, "second range read did not start").await;
        let second = tokio::task::spawn_blocking(move || {
            let range = 2 * stride..2 * stride + 1;
            second_reader
                .fetch_range_batch(std::slice::from_ref(&range), |_, _| Ok(()))
                .unwrap();
        });
        tracking.release.add_permits(1);
        let third_started_early =
            tokio::time::timeout(Duration::from_secs(1), tracking.started.acquire())
                .await
                .map(|permit| permit.unwrap().forget())
                .is_ok();
        release_copy_tx.send(()).unwrap();
        if !third_started_early {
            acquire_test_permits(&tracking.started, 1, "third range read did not start").await;
        }
        tracking.release.add_permits(1);
        first.await.unwrap();
        second.await.unwrap();

        assert!(
            !third_started_early,
            "more than 2x the I/O concurrency was retained as responses"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_range_response_holds_permit_until_consumed() {
        let data = Bytes::from(vec![8u8; 384]);
        let limiter = RangeReadLimiter::new(1);
        let response_permits = Arc::clone(&limiter.response_permits);
        let source: Arc<dyn FileRead> = TrackingRead::new(data.clone());
        let first_reader = VindexFileReader::new_with_limiter(
            source,
            tokio::runtime::Handle::current(),
            limiter,
            data.len() as u64,
            "index".to_string(),
        );
        let second_reader = first_reader.try_clone_reader().unwrap().unwrap();
        let third_reader = first_reader.try_clone_reader().unwrap().unwrap();

        let first = tokio::task::spawn_blocking(move || first_reader.fetch_exact(0..128).unwrap())
            .await
            .unwrap();
        let second =
            tokio::task::spawn_blocking(move || second_reader.fetch_exact(128..256).unwrap())
                .await
                .unwrap();
        let mut third =
            tokio::task::spawn_blocking(move || third_reader.fetch_exact(256..384).unwrap());

        assert!(tokio::time::timeout(Duration::from_secs(1), &mut third)
            .await
            .is_err());
        drop(first);
        let third = tokio::time::timeout(Duration::from_secs(5), third)
            .await
            .expect("third single-range response did not start after a permit was released")
            .unwrap();
        drop(second);
        drop(third);
        assert_eq!(response_permits.available_permits(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn completed_range_buffers_are_released_before_slowest_read() {
        let concurrency = 2;
        let range_count = 3;
        let stride = RANGE_COALESCE_GAP + 2;
        let dropped = Arc::new(tokio::sync::Semaphore::new(0));
        let tracking = Arc::new(StreamingTrackingRead {
            stride,
            first_started: tokio::sync::Semaphore::new(0),
            release_first: tokio::sync::Semaphore::new(0),
            dropped: Arc::clone(&dropped),
        });
        let source: Arc<dyn FileRead> = tracking.clone();
        let mut reader = VindexFileReader::new_with_limiter(
            source,
            tokio::runtime::Handle::current(),
            RangeReadLimiter::new(concurrency),
            range_count as u64 * stride,
            "index".to_string(),
        );

        let read = tokio::task::spawn_blocking(move || {
            let mut buffers = vec![[0u8; 1]; range_count];
            let mut requests = buffers
                .iter_mut()
                .enumerate()
                .map(|(index, buffer)| ReadRequest::new(index as u64 * stride, buffer))
                .collect::<Vec<_>>();
            reader.pread(&mut requests).unwrap();
            buffers
        });

        acquire_test_permits(&tracking.first_started, 1, "first range read did not start").await;
        let released = tokio::time::timeout(Duration::from_secs(5), dropped.acquire_many(2)).await;
        tracking.release_first.add_permits(1);
        released
            .expect("completed range buffers were retained by the slowest read")
            .unwrap()
            .forget();

        let output = tokio::time::timeout(Duration::from_secs(5), read)
            .await
            .expect("range read task did not finish")
            .unwrap();
        assert_eq!(output, vec![[1], [2], [3]]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_range_read_releases_permit() {
        let data = Bytes::from(vec![9u8; 1024]);
        let source: Arc<dyn FileRead> = Arc::new(FailOnceRead {
            data: data.clone(),
            calls: AtomicUsize::new(0),
        });
        let limiter = RangeReadLimiter::new(1);
        let io_permits = Arc::clone(&limiter.io_permits);
        let response_permits = Arc::clone(&limiter.response_permits);
        let mut reader = VindexFileReader::new_with_limiter(
            source,
            tokio::runtime::Handle::current(),
            limiter,
            data.len() as u64,
            "index".to_string(),
        );

        let (mut reader, error) = tokio::task::spawn_blocking(move || {
            let mut output = [0u8; 128];
            let error = reader
                .pread(&mut [ReadRequest::new(0, &mut output)])
                .unwrap_err();
            (reader, error)
        })
        .await
        .unwrap();
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(io_permits.available_permits(), 1);
        assert_eq!(response_permits.available_permits(), 2);

        tokio::time::timeout(
            Duration::from_secs(5),
            tokio::task::spawn_blocking(move || {
                let mut output = [0u8; 128];
                reader
                    .pread(&mut [ReadRequest::new(0, &mut output)])
                    .unwrap();
                assert_eq!(output, [9u8; 128]);
            }),
        )
        .await
        .expect("range read blocked after an error")
        .unwrap();
    }

    #[test]
    fn local_fs_read_completes_with_one_host_blocking_thread() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("index.bin");
        let data = vec![9u8; 4096];
        std::fs::write(&path, &data).unwrap();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();

        runtime.block_on(async {
            let file_io = FileIO::from_path(path.to_string_lossy())
                .unwrap()
                .build()
                .unwrap();
            let input = file_io.new_input(path.to_string_lossy().as_ref()).unwrap();
            let source: Arc<dyn FileRead> = Arc::new(input.reader().await.unwrap());
            let mut reader = VindexFileReader::new(
                source,
                tokio::runtime::Handle::current(),
                data.len() as u64,
                path.to_string_lossy().into_owned(),
            );

            let (sender, receiver) = tokio::sync::oneshot::channel();
            let worker = std::thread::spawn(move || {
                let mut output = [0u8; 32];
                let result = reader.pread(&mut [ReadRequest::new(512, &mut output)]);
                let _ = sender.send((result, output));
            });
            let (result, output) = tokio::time::timeout(Duration::from_secs(5), receiver)
                .await
                .expect("vindex range read deadlocked on the host blocking pool")
                .unwrap();
            result.unwrap();
            worker.join().unwrap();
            assert_eq!(output, [9u8; 32]);
        });
    }

    #[test]
    fn local_fs_read_is_safe_on_current_thread_runtime() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("index.bin");
        let data = vec![11u8; 4096];
        std::fs::write(&path, &data).unwrap();
        let worker = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                let file_io = FileIO::from_path(path.to_string_lossy())
                    .unwrap()
                    .build()
                    .unwrap();
                let input = file_io.new_input(path.to_string_lossy().as_ref()).unwrap();
                let source: Arc<dyn FileRead> = Arc::new(input.reader().await.unwrap());
                let mut reader = VindexFileReader::new(
                    source,
                    tokio::runtime::Handle::current(),
                    data.len() as u64,
                    path.to_string_lossy().into_owned(),
                );
                let (sender, receiver) = tokio::sync::oneshot::channel();
                let search = std::thread::spawn(move || {
                    let mut output = [0u8; 32];
                    let result = reader.pread(&mut [ReadRequest::new(1024, &mut output)]);
                    let _ = sender.send((result, output));
                });
                let (result, output) = tokio::time::timeout(Duration::from_secs(5), receiver)
                    .await
                    .expect("vindex range read hung on a current-thread runtime")
                    .unwrap();
                result.unwrap();
                search.join().unwrap();
                assert_eq!(output, [11u8; 32]);
            });
        });
        worker.join().unwrap();
    }
}
