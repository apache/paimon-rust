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
use crate::vindex::vector_search_timing_enabled;
use bytes::Bytes;
use futures::future::try_join_all;
use paimon_vindex_core::io::{ReadRequest, SeekRead, SeekReadCapabilities};
use std::io;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Instant;

const SCALAR_READ_MAX: usize = 64;
const SCALAR_READ_AHEAD: u64 = 64 * 1024;
const RANGE_COALESCE_GAP: u64 = 16 * 1024;
const RANGE_READ_CONCURRENCY: usize = 32;

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

#[derive(Debug, Default)]
pub(crate) struct RangeIoStats {
    logical_ranges: AtomicU64,
    requested_bytes: AtomicU64,
    file_read_calls: AtomicU64,
    returned_bytes: AtomicU64,
    read_ahead_hits: AtomicU64,
    io_wait_nanos: AtomicU64,
    range_permit_wait_nanos: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RangeIoStatsSnapshot {
    pub(crate) logical_ranges: u64,
    pub(crate) requested_bytes: u64,
    pub(crate) file_read_calls: u64,
    pub(crate) returned_bytes: u64,
    pub(crate) read_ahead_hits: u64,
    pub(crate) io_wait_nanos: u64,
    pub(crate) range_permit_wait_nanos: u64,
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
        }
    }
}

/// Bridges vindex-core's synchronous positional reads to Paimon's asynchronous
/// range reader. This type is consumed from a blocking search task; it captures
/// the surrounding Tokio runtime so remote storage reads still run asynchronously.
pub(crate) struct VindexFileReader {
    reader: Arc<dyn FileRead>,
    runtime: tokio::runtime::Handle,
    permits: Arc<tokio::sync::Semaphore>,
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
        Self::new_with_permits(
            reader,
            runtime,
            Arc::new(tokio::sync::Semaphore::new(RANGE_READ_CONCURRENCY)),
            file_size,
            path,
        )
    }

    pub(crate) fn new_with_permits(
        reader: Arc<dyn FileRead>,
        runtime: tokio::runtime::Handle,
        permits: Arc<tokio::sync::Semaphore>,
        file_size: u64,
        path: String,
    ) -> Self {
        Self {
            reader,
            runtime,
            permits,
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
            let data = self.fetch_exact(range.start..read_end)?;
            buf.copy_from_slice(&data[..buf.len()]);
            self.scalar_cache = Some(CachedRange {
                start: range.start,
                data,
            });
            return Ok(());
        }

        let data = self.fetch_exact(range)?;
        buf.copy_from_slice(&data);
        Ok(())
    }

    fn fetch_exact(&self, range: Range<u64>) -> io::Result<Bytes> {
        let mut results = self.fetch_range_batch(std::slice::from_ref(&range))?;
        Ok(results.pop().expect("one requested range"))
    }

    fn fetch_range_batch(&self, ranges: &[Range<u64>]) -> io::Result<Vec<Bytes>> {
        debug_assert!(ranges.len() <= RANGE_READ_CONCURRENCY);
        let reader = Arc::clone(&self.reader);
        let permits = Arc::clone(&self.permits);
        let path = self.path.clone();
        let requested = ranges.to_vec();
        let stats = self.stats.clone();
        let (sender, receiver) = mpsc::sync_channel(1);
        let wait_start = self.stats.as_ref().map(|_| Instant::now());
        self.runtime.spawn(async move {
            let fetched = try_join_all(requested.iter().cloned().map(|range| {
                let reader = Arc::clone(&reader);
                let permits = Arc::clone(&permits);
                let path = path.clone();
                let stats = stats.clone();
                async move {
                    let permit_wait_start = stats.as_ref().map(|_| Instant::now());
                    let permit = permits.acquire_owned().await;
                    if let (Some(stats), Some(start)) = (&stats, permit_wait_start) {
                        stats
                            .range_permit_wait_nanos
                            .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    }
                    let _permit = permit.map_err(|_| {
                        io::Error::other("vindex range read concurrency limiter closed")
                    })?;
                    let expected = (range.end - range.start) as usize;
                    if let Some(stats) = &stats {
                        stats.file_read_calls.fetch_add(1, Ordering::Relaxed);
                    }
                    let data = reader.read(range.clone()).await.map_err(|error| {
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
                    Ok(data)
                }
            }))
            .await;
            let _ = sender.send(fetched);
        });
        let result = receiver.recv();
        if let (Some(stats), Some(start)) = (&self.stats, wait_start) {
            stats
                .io_wait_nanos
                .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
        result.map_err(|_| {
            io::Error::other(format!(
                "vindex range read task for '{}' was cancelled",
                self.path
            ))
        })?
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

        for batch in merged.chunks(RANGE_READ_CONCURRENCY) {
            let ranges: Vec<_> = batch.iter().map(|merged| merged.range.clone()).collect();
            let fetched = self.fetch_range_batch(&ranges)?;
            for (merged_range, data) in batch.iter().zip(fetched) {
                for &request_index in &merged_range.request_indices {
                    let request = &mut requests[request_index];
                    let start = (request.pos - merged_range.range.start) as usize;
                    request
                        .buf
                        .copy_from_slice(&data[start..start + request.buf.len()]);
                }
            }
        }
        Ok(())
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
            permits: Arc::clone(&self.permits),
            file_size: self.file_size,
            path: self.path.clone(),
            scalar_cache: None,
            stats: self.stats.clone(),
        }))
    }

    fn read_capabilities(&self) -> SeekReadCapabilities {
        // This adapter accepts any number of ranges and splits them internally.
        // The efficient window size depends on the underlying FileRead backend,
        // so leave both storage-specific hints unspecified.
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
            tokio::time::sleep(Duration::from_millis(25)).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(self.data.slice(range.start as usize..range.end as usize))
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
            ),
            (2, 256, 2, 256, 0)
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
        })
        .await
        .unwrap();

        let stats = stats.snapshot();
        assert_eq!(stats.logical_ranges, 3);
        assert_eq!(stats.requested_bytes, 12);
        assert!(stats.file_read_calls < stats.logical_ranges);
        assert!(stats.returned_bytes >= stats.requested_bytes);
        assert_eq!(stats.read_ahead_hits, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shared_permits_bound_reads_across_independent_readers() {
        let data = Bytes::from(vec![8u8; 1024]);
        let tracking = Arc::new(ConcurrencyTrackingRead {
            data: data.clone(),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
        });
        let permits = Arc::new(tokio::sync::Semaphore::new(0));
        let make_reader = |path: &str| {
            let source: Arc<dyn FileRead> = tracking.clone();
            VindexFileReader::new_with_permits(
                source,
                tokio::runtime::Handle::current(),
                Arc::clone(&permits),
                data.len() as u64,
                path.to_string(),
            )
        };
        let mut first_reader = make_reader("first.index");
        let mut second_reader = make_reader("second.index");
        let stats = Arc::new(RangeIoStats::default());
        first_reader.stats = Some(Arc::clone(&stats));
        second_reader.stats = Some(Arc::clone(&stats));
        assert!(Arc::ptr_eq(&first_reader.permits, &second_reader.permits));

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
        tokio::time::sleep(Duration::from_millis(10)).await;
        permits.add_permits(1);
        first.await.unwrap();
        second.await.unwrap();

        assert_eq!(tracking.max_active.load(Ordering::SeqCst), 1);
        let stats = stats.snapshot();
        assert!(stats.range_permit_wait_nanos > 0);
        assert!(stats.io_wait_nanos >= stats.range_permit_wait_nanos);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_many_caps_each_batch_at_32_ranges() {
        let range_count = RANGE_READ_CONCURRENCY + 1;
        let stride = RANGE_COALESCE_GAP + 2;
        let data = Bytes::from(vec![8u8; range_count * stride as usize]);
        let tracking = Arc::new(ConcurrencyTrackingRead {
            data: data.clone(),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
        });
        let source: Arc<dyn FileRead> = tracking.clone();
        let mut reader = VindexFileReader::new(
            source,
            tokio::runtime::Handle::current(),
            data.len() as u64,
            "index".to_string(),
        );

        tokio::task::spawn_blocking(move || {
            let mut buffers = vec![[0u8; 1]; range_count];
            let mut requests = buffers
                .iter_mut()
                .enumerate()
                .map(|(index, buffer)| ReadRequest::new(index as u64 * stride, buffer))
                .collect::<Vec<_>>();
            reader.pread(&mut requests).unwrap();
        })
        .await
        .unwrap();

        assert_eq!(
            tracking.max_active.load(Ordering::SeqCst),
            RANGE_READ_CONCURRENCY
        );
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
