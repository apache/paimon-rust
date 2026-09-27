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

use std::ops::Range;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use crate::io::FileRead;

use super::{CacheReadToken, LocalCache};

pub(crate) struct CachedFileReader {
    delegate: Arc<dyn FileRead>,
    path: String,
    // Some enables aligned blocks; None caches only explicit BLOB metadata ranges.
    block_file_size: Option<u64>,
    cache: Arc<LocalCache>,
    read_token: CacheReadToken,
}

impl CachedFileReader {
    #[cfg(test)]
    pub(crate) fn new(
        delegate: Arc<dyn FileRead>,
        path: impl Into<String>,
        file_size: u64,
        cache: Arc<LocalCache>,
    ) -> Self {
        let path = path.into();
        let read_token = cache.read_token(&path);
        let block_file_size = cache.cache_blocks(&path).then_some(file_size);
        Self::new_with_token(delegate, path, block_file_size, cache, read_token)
    }

    pub(in crate::io) fn new_with_token(
        delegate: Arc<dyn FileRead>,
        path: impl Into<String>,
        block_file_size: Option<u64>,
        cache: Arc<LocalCache>,
        read_token: CacheReadToken,
    ) -> Self {
        Self {
            delegate,
            path: path.into(),
            block_file_size,
            cache,
            read_token,
        }
    }

    async fn read_block(&self, block_index: u64, file_size: u64) -> crate::Result<Bytes> {
        let block_size = self.cache.block_size();
        let key = self.cache.block_key(&self.path, block_index);
        let start = block_index * block_size;
        let end = start.saturating_add(block_size).min(file_size);
        let expected_len = usize::try_from(end - start).map_err(|_| crate::Error::DataInvalid {
            message: format!("Cache block is too large for '{}'", self.path),
            source: None,
        })?;
        if let Some(payload) = self
            .cache
            .get_block(&key, expected_len, &self.read_token)
            .await
        {
            if payload.len() == expected_len {
                return Ok(payload);
            }
            self.cache.remove_block(&key).await;
        }

        let load_lock = self.cache.block_load_lock(&key).await;
        let load_guard = load_lock.lock().await;
        let result = if let Some(payload) = self
            .cache
            .get_block(&key, expected_len, &self.read_token)
            .await
        {
            if payload.len() == expected_len {
                Ok(payload)
            } else {
                self.cache.remove_block(&key).await;
                Err(crate::Error::DataInvalid {
                    message: format!(
                        "Cached block {} for '{}' has length {}, expected {}",
                        block_index,
                        self.path,
                        payload.len(),
                        expected_len
                    ),
                    source: None,
                })
            }
        } else {
            match self.delegate.read(start..end).await {
                Ok(payload) if payload.len() == expected_len => {
                    self.cache
                        .put_block(&key, payload.clone(), &self.read_token)
                        .await;
                    Ok(payload)
                }
                Ok(payload) => Err(crate::Error::DataInvalid {
                    message: format!(
                        "Source block {} for '{}' has length {}, expected {}",
                        block_index,
                        self.path,
                        payload.len(),
                        expected_len
                    ),
                    source: None,
                }),
                Err(error) => Err(error),
            }
        };
        drop(load_guard);
        self.cache.release_block_load_lock(&key, &load_lock).await;
        result
    }

    pub(crate) async fn read_full(&self) -> crate::Result<Bytes> {
        let file_size = self
            .block_file_size
            .expect("full reads require block caching");
        if file_size == 0 {
            return Ok(Bytes::new());
        }
        if let Some(payload) = self.read_cached_full(file_size).await {
            return Ok(payload);
        }

        let first_key = self.cache.block_key(&self.path, 0);
        let load_lock = self.cache.block_load_lock(&first_key).await;
        let load_guard = load_lock.lock().await;
        let result = if let Some(payload) = self.read_cached_full(file_size).await {
            Ok(payload)
        } else {
            match self.delegate.read(0..file_size).await {
                Ok(payload) if payload.len() as u64 == file_size => {
                    let chunk_size = usize::try_from(self.cache.block_size()).map_err(|_| {
                        crate::Error::DataInvalid {
                            message: format!("Cache block size is too large for '{}'", self.path),
                            source: None,
                        }
                    })?;
                    for (block_index, block) in payload.chunks(chunk_size).enumerate() {
                        let block_index =
                            u64::try_from(block_index).map_err(|_| crate::Error::DataInvalid {
                                message: format!("Too many cache blocks for '{}'", self.path),
                                source: None,
                            })?;
                        let key = self.cache.block_key(&self.path, block_index);
                        self.cache
                            .put_block(&key, Bytes::copy_from_slice(block), &self.read_token)
                            .await;
                    }
                    Ok(payload)
                }
                Ok(payload) => Err(crate::Error::DataInvalid {
                    message: format!(
                        "Source file '{}' has length {}, expected {}",
                        self.path,
                        payload.len(),
                        file_size
                    ),
                    source: None,
                }),
                Err(error) => Err(error),
            }
        };
        drop(load_guard);
        self.cache
            .release_block_load_lock(&first_key, &load_lock)
            .await;
        result
    }

    async fn read_cached_full(&self, file_size: u64) -> Option<Bytes> {
        let block_size = self.cache.block_size();
        let block_count = file_size.div_ceil(block_size);
        let output_len = usize::try_from(file_size).ok()?;
        let mut output = BytesMut::with_capacity(output_len);
        for block_index in 0..block_count {
            let key = self.cache.block_key(&self.path, block_index);
            let start = block_index * block_size;
            let expected_len = (file_size - start).min(block_size) as usize;
            let payload = self
                .cache
                .get_block(&key, expected_len, &self.read_token)
                .await?;
            if payload.len() != expected_len {
                self.cache.remove_block(&key).await;
                return None;
            }
            output.extend_from_slice(&payload);
        }
        Some(output.freeze())
    }
}

#[async_trait::async_trait]
impl FileRead for CachedFileReader {
    async fn read_blob_metadata(&self, range: Range<u64>) -> crate::Result<Bytes> {
        if self.block_file_size.is_some() {
            return self.read(range).await;
        }
        if range.start >= range.end {
            return self.delegate.read(range).await;
        }
        let key = self.cache.range_key(&self.path, &range);
        let expected_len =
            usize::try_from(range.end - range.start).map_err(|_| crate::Error::DataInvalid {
                message: "BLOB metadata range is too large".to_string(),
                source: None,
            })?;
        if !self.cache.can_cache_exact_range(&key, expected_len).await {
            return self.delegate.read(range).await;
        }
        if let Some(payload) = self
            .cache
            .get_block(&key, expected_len, &self.read_token)
            .await
        {
            if payload.len() == expected_len {
                return Ok(payload);
            }
        }
        let load_lock = self.cache.block_load_lock(&key).await;
        let guard = load_lock.lock().await;
        let result = async {
            if let Some(payload) = self
                .cache
                .get_block(&key, expected_len, &self.read_token)
                .await
            {
                if payload.len() == expected_len {
                    return Ok(payload);
                }
            }
            let payload = self.delegate.read(range).await?;
            if payload.len() == expected_len {
                let payload = Bytes::copy_from_slice(&payload);
                self.cache
                    .put_block(&key, payload.clone(), &self.read_token)
                    .await;
                return Ok(payload);
            }
            Ok(payload)
        }
        .await;
        drop(guard);
        self.cache.release_block_load_lock(&key, &load_lock).await;
        result
    }

    async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
        let Some(file_size) = self.block_file_size else {
            return self.delegate.read(range).await;
        };
        if range.start > range.end || range.end > file_size {
            return self.delegate.read(range).await;
        }
        let end = range.end;
        if range.start >= end {
            return Ok(Bytes::new());
        }

        let block_size = self.cache.block_size();
        let first_block = range.start / block_size;
        let last_block = (end - 1) / block_size;
        let output_len =
            usize::try_from(end - range.start).map_err(|_| crate::Error::DataInvalid {
                message: format!("File read range is too large for '{}'", self.path),
                source: None,
            })?;
        let mut output = BytesMut::with_capacity(output_len);

        for block_index in first_block..=last_block {
            let block = self.read_block(block_index, file_size).await?;
            let block_start = block_index * block_size;
            let copy_start = range.start.max(block_start) - block_start;
            let copy_end = end.min(block_start + block.len() as u64) - block_start;
            output.extend_from_slice(&block[copy_start as usize..copy_end as usize]);
        }

        Ok(output.freeze())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::super::{FileType, LocalCache, LocalCacheConfig};
    use super::*;
    use crate::common::{CatalogOptions, Options};
    use crate::io::cache::create_local_cache;

    #[tokio::test]
    async fn test_blob_metadata_exact_ranges_and_body_bypass() {
        for disk in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let config = || LocalCacheConfig {
                dir: disk.then(|| directory.path().to_path_buf()),
                namespace: "blob-test".into(),
                max_size: Some(64 * 1024),
                block_size: 1024,
                whitelist: std::collections::HashSet::from([FileType::BlobMeta]),
            };
            let cache = Arc::new(LocalCache::new(config()).unwrap());
            let delegate = Arc::new(CountingReader {
                data: Bytes::from_static(b"abcdefgh"),
                reads: AtomicUsize::new(0),
            });
            let reader = CachedFileReader::new(delegate.clone(), "data.blob", 8, cache.clone());
            assert_eq!(reader.read_blob_metadata(1..3).await.unwrap(), b"bc"[..]);
            assert_eq!(reader.read_blob_metadata(1..4).await.unwrap(), b"bcd"[..]);
            assert_eq!(reader.read_blob_metadata(1..3).await.unwrap(), b"bc"[..]);
            assert_eq!(delegate.reads.load(Ordering::SeqCst), 2);
            // Values never enter the metadata cache.
            reader.read(4..8).await.unwrap();
            reader.read(4..8).await.unwrap();
            assert_eq!(delegate.reads.load(Ordering::SeqCst), 4);
            drop(reader);
            drop(cache);
            if disk {
                let cache = Arc::new(LocalCache::new(config()).unwrap());
                let reader = CachedFileReader::new(delegate.clone(), "data.blob", 8, cache.clone());
                assert_eq!(reader.read_blob_metadata(1..3).await.unwrap(), b"bc"[..]);
                assert_eq!(delegate.reads.load(Ordering::SeqCst), 4);
                cache.invalidate_path("data.blob").await;
                let reader = CachedFileReader::new(delegate.clone(), "data.blob", 8, cache);
                reader.read_blob_metadata(1..3).await.unwrap();
                assert_eq!(delegate.reads.load(Ordering::SeqCst), 5);
            }
        }
    }

    #[tokio::test]
    async fn test_blob_metadata_cold_reads_are_coalesced() {
        let delegate = Arc::new(SlowCountingReader {
            data: Bytes::from_static(b"abcdefgh"),
            reads: AtomicUsize::new(0),
        });
        let cache = Arc::new(
            LocalCache::new(LocalCacheConfig {
                dir: None,
                namespace: "test".into(),
                max_size: Some(65536),
                block_size: 1024,
                whitelist: std::collections::HashSet::from([FileType::BlobMeta]),
            })
            .unwrap(),
        );
        let readers: Vec<_> = (0..16)
            .map(|_| CachedFileReader::new(delegate.clone(), "data.blob", 8, cache.clone()))
            .collect();
        let results =
            futures::future::join_all(readers.iter().map(|reader| reader.read_blob_metadata(1..3)))
                .await;
        assert!(results
            .into_iter()
            .all(|result| result.unwrap() == b"bc"[..]));
        assert_eq!(delegate.reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_uncacheable_first_read_recovers_reduced_disk_budget() {
        let directory = tempfile::tempdir().unwrap();
        let config = |max_size| LocalCacheConfig {
            dir: Some(directory.path().to_path_buf()),
            namespace: "restart".into(),
            max_size: Some(max_size),
            block_size: 1024,
            whitelist: std::collections::HashSet::from([FileType::BlobMeta]),
        };
        let cache = LocalCache::new(config(8192)).unwrap();
        let key = cache.range_key("data.blob", &(0..2));
        let token = cache.read_token("data.blob");
        cache
            .put_block(&key, Bytes::from_static(b"ab"), &token)
            .await;
        let cached_file = directory
            .path()
            .join(super::super::CACHE_DIRECTORY_NAME)
            .join(key.cache_relative_path());
        assert!(cached_file.exists());
        drop(cache);

        let cache = Arc::new(LocalCache::new(config(1)).unwrap());
        let delegate = Arc::new(CountingReader {
            data: Bytes::from_static(b"abcdefgh"),
            reads: AtomicUsize::new(0),
        });
        let reader = CachedFileReader::new(delegate.clone(), "data.blob", 8, cache);
        assert_eq!(reader.read_blob_metadata(2..4).await.unwrap(), b"cd"[..]);
        assert_eq!(delegate.reads.load(Ordering::SeqCst), 1);
        assert!(!cached_file.exists());
    }

    #[tokio::test]
    async fn test_uncacheable_blob_metadata_reads_are_not_serialized() {
        for disk in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let delegate = Arc::new(ConcurrentCountingReader {
                data: Bytes::from_static(b"abcdefgh"),
                reads: AtomicUsize::new(0),
                in_flight: AtomicUsize::new(0),
                max_in_flight: AtomicUsize::new(0),
            });
            let cache = Arc::new(
                LocalCache::new(LocalCacheConfig {
                    dir: disk.then(|| directory.path().to_path_buf()),
                    namespace: "test".into(),
                    max_size: Some(1),
                    block_size: 1024,
                    whitelist: std::collections::HashSet::from([FileType::BlobMeta]),
                })
                .unwrap(),
            );
            let readers: Vec<_> = (0..16)
                .map(|_| CachedFileReader::new(delegate.clone(), "data.blob", 8, cache.clone()))
                .collect();
            let results = futures::future::join_all(
                readers.iter().map(|reader| reader.read_blob_metadata(1..3)),
            )
            .await;

            assert!(results
                .into_iter()
                .all(|result| result.unwrap() == b"bc"[..]));
            assert_eq!(delegate.reads.load(Ordering::SeqCst), 16);
            assert!(delegate.max_in_flight.load(Ordering::SeqCst) > 1);
        }
    }

    #[allow(dead_code)]
    mod blob_utils {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/blob_test_utils.rs"));
    }

    #[tokio::test]
    async fn test_blob_descriptor_scans_cache_metadata_not_values() {
        use crate::arrow::format::{blob::BlobFormatReader, FormatFileReader};
        use crate::spec::{ArrayType, BlobType, DataField, DataType, MapType, VarCharType};
        use futures::TryStreamExt;
        use std::sync::Mutex;

        struct RecordingReader {
            data: Bytes,
            ranges: Mutex<Vec<Range<u64>>>,
        }
        #[async_trait::async_trait]
        impl FileRead for RecordingReader {
            async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
                self.ranges.lock().unwrap().push(range.clone());
                Ok(self.data.slice(range.start as usize..range.end as usize))
            }
        }
        let blob_type = DataType::Blob(BlobType::new());
        let mut map_payload = Vec::new();
        map_payload.extend_from_slice(&0x4D424342_i32.to_le_bytes());
        map_payload.push(1);
        map_payload.extend_from_slice(&1_i32.to_le_bytes());
        map_payload.extend_from_slice(b"keyhello");
        let key_index = blob_utils::encode_delta_varints(&[3]);
        let value_index = blob_utils::encode_delta_varints(&[5]);
        map_payload.extend_from_slice(&key_index);
        map_payload.extend_from_slice(&value_index);
        map_payload.extend_from_slice(&(key_index.len() as i32).to_le_bytes());
        map_payload.extend_from_slice(&(value_index.len() as i32).to_le_bytes());
        let cases = [
            (
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/testdata/blob/blob-basic.blob"
                ))
                .to_vec(),
                blob_type.clone(),
            ),
            (
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/testdata/blob/blob-array.blob"
                ))
                .to_vec(),
                DataType::Array(ArrayType::new(blob_type.clone())),
            ),
            (
                blob_utils::build_blob_file_bytes(&[Some(&map_payload)]),
                DataType::Map(MapType::new(
                    DataType::VarChar(VarCharType::new(100).unwrap()),
                    blob_type,
                )),
            ),
        ];
        for disk in [false, true] {
            for (data, data_type) in &cases {
                let directory = tempfile::tempdir().unwrap();
                let cache = Arc::new(
                    LocalCache::new(LocalCacheConfig {
                        dir: disk.then(|| directory.path().to_path_buf()),
                        namespace: "scan".into(),
                        max_size: Some(65536),
                        block_size: 1024,
                        whitelist: std::collections::HashSet::from([FileType::BlobMeta]),
                    })
                    .unwrap(),
                );
                let delegate = Arc::new(RecordingReader {
                    data: Bytes::copy_from_slice(data),
                    ranges: Mutex::new(Vec::new()),
                });
                let fields = [DataField::new(0, "value".into(), data_type.clone())];
                let scan = |descriptor| {
                    let reader = CachedFileReader::new(
                        delegate.clone(),
                        "data.blob",
                        data.len() as u64,
                        cache.clone(),
                    );
                    let fields = fields.clone();
                    async move {
                        BlobFormatReader::new("data.blob".into(), descriptor)
                            .read_batch_stream(
                                Box::new(reader),
                                data.len() as u64,
                                &fields,
                                None,
                                Some(1024),
                                None,
                            )
                            .await
                            .unwrap()
                            .try_collect::<Vec<_>>()
                            .await
                            .unwrap()
                    }
                };
                let first = scan(true).await;
                let reads = delegate.ranges.lock().unwrap().clone();
                assert!(!reads.is_empty());
                for needle in [b"hello", b"world"] {
                    for (offset, _) in data
                        .windows(needle.len())
                        .enumerate()
                        .filter(|(_, bytes)| *bytes == needle)
                    {
                        assert!(reads.iter().all(|range| range.end <= offset as u64
                            || range.start >= (offset + needle.len()) as u64));
                    }
                }
                assert_eq!(scan(true).await, first);
                assert_eq!(delegate.ranges.lock().unwrap().len(), reads.len());
                let materialized = scan(false).await;
                assert_eq!(
                    materialized
                        .iter()
                        .map(|batch| batch.num_rows())
                        .sum::<usize>(),
                    first.iter().map(|batch| batch.num_rows()).sum::<usize>()
                );
                assert!(delegate.ranges.lock().unwrap().len() > reads.len());
            }
        }
    }

    #[derive(Debug)]
    struct CountingReader {
        data: Bytes,
        reads: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl FileRead for CountingReader {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            Ok(self.data.slice(range.start as usize..range.end as usize))
        }
    }

    #[tokio::test]
    async fn test_cached_range_reader_reads_unaligned_blocks_and_reuses_them() {
        let delegate = Arc::new(CountingReader {
            data: Bytes::from_static(b"abcdefghijkl"),
            reads: AtomicUsize::new(0),
        });
        let cache = Arc::new(
            LocalCache::new(LocalCacheConfig {
                dir: None,
                namespace: "test".to_string(),
                max_size: None,
                block_size: 4,
                whitelist: std::collections::HashSet::from([FileType::Meta]),
            })
            .unwrap(),
        );
        let reader = CachedFileReader::new(
            delegate.clone(),
            "s3://bucket/table/snapshot/snapshot-1",
            12,
            cache,
        );

        assert_eq!(
            reader.read(2..10).await.unwrap(),
            Bytes::from_static(b"cdefghij")
        );
        assert_eq!(delegate.reads.load(Ordering::SeqCst), 3);
        assert_eq!(
            reader.read(2..10).await.unwrap(),
            Bytes::from_static(b"cdefghij")
        );
        assert_eq!(delegate.reads.load(Ordering::SeqCst), 3);
    }

    #[derive(Debug)]
    struct SlowCountingReader {
        data: Bytes,
        reads: AtomicUsize,
    }

    #[derive(Debug)]
    struct ConcurrentCountingReader {
        data: Bytes,
        reads: AtomicUsize,
        in_flight: AtomicUsize,
        max_in_flight: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl FileRead for ConcurrentCountingReader {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            let in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(in_flight, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            Ok(self.data.slice(range.start as usize..range.end as usize))
        }
    }

    #[async_trait::async_trait]
    impl FileRead for SlowCountingReader {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Ok(self.data.slice(range.start as usize..range.end as usize))
        }
    }

    #[tokio::test]
    async fn test_cached_range_single_flight_reads_cold_block_once() {
        let delegate = Arc::new(SlowCountingReader {
            data: Bytes::from_static(b"abcdefgh"),
            reads: AtomicUsize::new(0),
        });
        let cache = Arc::new(
            LocalCache::new(LocalCacheConfig {
                dir: None,
                namespace: "test".to_string(),
                max_size: None,
                block_size: 4,
                whitelist: std::collections::HashSet::from([FileType::Meta]),
            })
            .unwrap(),
        );
        let reader = CachedFileReader::new(
            delegate.clone(),
            "s3://bucket/table/snapshot/snapshot-1",
            8,
            cache,
        );

        let (first, second) = tokio::join!(reader.read(0..4), reader.read(0..4));

        assert_eq!(first.unwrap(), Bytes::from_static(b"abcd"));
        assert_eq!(second.unwrap(), Bytes::from_static(b"abcd"));
        assert_eq!(delegate.reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_cached_range_single_flight_is_shared_across_cache_instances() {
        let directory = tempfile::tempdir().unwrap();
        let delegate = Arc::new(SlowCountingReader {
            data: Bytes::from_static(b"abcdefgh"),
            reads: AtomicUsize::new(0),
        });
        let config = || LocalCacheConfig {
            dir: Some(directory.path().to_path_buf()),
            namespace: "test".to_string(),
            max_size: None,
            block_size: 4,
            whitelist: std::collections::HashSet::from([FileType::Meta]),
        };
        let first_reader = CachedFileReader::new(
            delegate.clone(),
            "s3://bucket/table/snapshot/snapshot-1",
            8,
            Arc::new(LocalCache::new(config()).unwrap()),
        );
        let second_reader = CachedFileReader::new(
            delegate.clone(),
            "s3://bucket/table/snapshot/snapshot-1",
            8,
            Arc::new(LocalCache::new(config()).unwrap()),
        );

        let (first, second) = tokio::join!(first_reader.read(0..4), second_reader.read(0..4));

        assert_eq!(first.unwrap(), Bytes::from_static(b"abcd"));
        assert_eq!(second.unwrap(), Bytes::from_static(b"abcd"));
        assert_eq!(delegate.reads.load(Ordering::SeqCst), 1);
    }

    #[derive(Debug)]
    struct BlockingReader {
        data: Bytes,
        started: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl FileRead for BlockingReader {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            self.started.notify_one();
            self.release.notified().await;
            Ok(self.data.slice(range.start as usize..range.end as usize))
        }
    }

    #[tokio::test]
    async fn test_in_flight_miss_does_not_republish_after_invalidation() {
        let path = "s3://bucket/table/snapshot/snapshot-1";
        let cache = Arc::new(
            LocalCache::new(LocalCacheConfig {
                dir: None,
                namespace: "test".to_string(),
                max_size: None,
                block_size: 4,
                whitelist: std::collections::HashSet::from([FileType::Meta]),
            })
            .unwrap(),
        );
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let old_reader = CachedFileReader::new(
            Arc::new(BlockingReader {
                data: Bytes::from_static(b"old!"),
                started: started.clone(),
                release: release.clone(),
            }),
            path,
            4,
            cache.clone(),
        );
        let old_load = tokio::spawn(async move { old_reader.read(0..4).await });

        started.notified().await;
        cache.invalidate_path(path).await;
        release.notify_one();
        assert_eq!(
            old_load.await.unwrap().unwrap(),
            Bytes::from_static(b"old!")
        );

        let current_delegate = Arc::new(CountingReader {
            data: Bytes::from_static(b"new!"),
            reads: AtomicUsize::new(0),
        });
        let current_reader =
            CachedFileReader::new(current_delegate.clone(), path, 4, cache.clone());
        assert_eq!(
            current_reader.read(0..4).await.unwrap(),
            Bytes::from_static(b"new!")
        );
        assert_eq!(current_delegate.reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_in_flight_miss_cannot_republish_across_shared_cache_instances() {
        let directory = tempfile::tempdir().unwrap();
        let path = "s3://bucket/table/snapshot/snapshot-1";
        let cache_a = Arc::new(
            LocalCache::new(LocalCacheConfig {
                dir: Some(directory.path().to_path_buf()),
                namespace: "test".to_string(),
                max_size: None,
                block_size: 4,
                whitelist: std::collections::HashSet::from([FileType::Meta]),
            })
            .unwrap(),
        );
        let cache_b = Arc::new(
            LocalCache::new(LocalCacheConfig {
                dir: Some(directory.path().to_path_buf()),
                namespace: "test".to_string(),
                max_size: None,
                block_size: 4,
                whitelist: std::collections::HashSet::from([FileType::Meta]),
            })
            .unwrap(),
        );
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let old_reader = CachedFileReader::new(
            Arc::new(BlockingReader {
                data: Bytes::from_static(b"old!"),
                started: started.clone(),
                release: release.clone(),
            }),
            path,
            4,
            cache_a,
        );
        let old_load = tokio::spawn(async move { old_reader.read(0..4).await });

        started.notified().await;
        cache_b.invalidate_path(path).await;
        release.notify_one();
        assert_eq!(
            old_load.await.unwrap().unwrap(),
            Bytes::from_static(b"old!")
        );

        let current_delegate = Arc::new(CountingReader {
            data: Bytes::from_static(b"new!"),
            reads: AtomicUsize::new(0),
        });
        let current_reader =
            CachedFileReader::new(current_delegate.clone(), path, 4, cache_b.clone());
        assert_eq!(
            current_reader.read(0..4).await.unwrap(),
            Bytes::from_static(b"new!")
        );
        assert_eq!(current_delegate.reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_prefix_invalidation_cannot_hit_old_block_while_waiting_on_sibling() {
        let directory = tempfile::tempdir().unwrap();
        let prefix = "s3://bucket/table/snapshot";
        let first_path = "s3://bucket/table/snapshot/snapshot-1";
        let observed_path = "s3://bucket/table/snapshot/snapshot-2";
        let blocked_path = "s3://bucket/table/snapshot/snapshot-3";
        let config = || LocalCacheConfig {
            dir: Some(directory.path().to_path_buf()),
            namespace: "test".to_string(),
            max_size: None,
            block_size: 4,
            whitelist: std::collections::HashSet::from([FileType::Meta]),
        };
        let cache_a = Arc::new(LocalCache::new(config()).unwrap());
        let cache_b = Arc::new(LocalCache::new(config()).unwrap());
        let old_reader = CachedFileReader::new(
            Arc::new(CountingReader {
                data: Bytes::from_static(b"old!"),
                reads: AtomicUsize::new(0),
            }),
            first_path,
            4,
            cache_a.clone(),
        );
        assert_eq!(
            old_reader.read(0..4).await.unwrap(),
            Bytes::from_static(b"old!")
        );
        let warm_size_token = cache_a.read_token(first_path);
        cache_a.put_file_size(first_path, 4, &warm_size_token).await;
        drop(warm_size_token);
        drop(old_reader);

        let observed_token = cache_a.read_token(observed_path);
        let blocked_token = cache_a.read_token(blocked_path);
        let blocked_guard = blocked_token.publish_guard().await;
        let invalidating_cache = cache_b;
        let invalidation =
            tokio::spawn(async move { invalidating_cache.invalidate_prefix(prefix).await });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while observed_token.is_current() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let current_delegate = Arc::new(CountingReader {
            data: Bytes::from_static(b"new!"),
            reads: AtomicUsize::new(0),
        });
        let current_reader =
            CachedFileReader::new(current_delegate.clone(), first_path, 4, cache_a.clone());
        let mut current_load = tokio::spawn(async move { current_reader.read(0..4).await });
        let size_cache = cache_a;
        let mut current_size = tokio::spawn(async move {
            let token = size_cache.read_token(first_path);
            size_cache.file_size(first_path, &token).await
        });

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut current_load)
                .await
                .is_err(),
            "cache read completed before prefix invalidation removed the old block"
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut current_size)
                .await
                .is_err(),
            "file-size lookup completed before prefix invalidation removed the old entry"
        );
        drop(blocked_guard);
        invalidation.await.unwrap();
        assert_eq!(
            current_load.await.unwrap().unwrap(),
            Bytes::from_static(b"new!")
        );
        assert_eq!(current_size.await.unwrap(), None);
        assert_eq!(current_delegate.reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_catalog_namespace_prevents_cross_catalog_cache_hits() {
        let directory = tempfile::tempdir().unwrap();
        let path = "s3://bucket/table/snapshot/snapshot-1";
        let mut first_options = Options::new();
        first_options.set(CatalogOptions::LOCAL_CACHE_ENABLED, "true");
        first_options.set(
            CatalogOptions::LOCAL_CACHE_DIR,
            directory.path().to_string_lossy(),
        );
        first_options.set(CatalogOptions::URI, "https://catalog-a.example");
        let first_cache = create_local_cache(&first_options).unwrap().unwrap();
        let first_reader = CachedFileReader::new(
            Arc::new(CountingReader {
                data: Bytes::from_static(b"from"),
                reads: AtomicUsize::new(0),
            }),
            path,
            4,
            first_cache,
        );
        assert_eq!(
            first_reader.read(0..4).await.unwrap(),
            Bytes::from_static(b"from")
        );

        let mut second_options = first_options;
        second_options.set(CatalogOptions::URI, "https://catalog-b.example");
        let second_cache = create_local_cache(&second_options).unwrap().unwrap();
        let second_delegate = Arc::new(CountingReader {
            data: Bytes::from_static(b"new!"),
            reads: AtomicUsize::new(0),
        });
        let second_reader = CachedFileReader::new(second_delegate.clone(), path, 4, second_cache);

        assert_eq!(
            second_reader.read(0..4).await.unwrap(),
            Bytes::from_static(b"new!")
        );
        assert_eq!(second_delegate.reads.load(Ordering::SeqCst), 1);
    }

    #[derive(Debug)]
    struct BoundedReader {
        data: Bytes,
    }

    #[async_trait::async_trait]
    impl FileRead for BoundedReader {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            if range.end > self.data.len() as u64 {
                return Err(crate::Error::DataInvalid {
                    message: "range exceeds file size".to_string(),
                    source: None,
                });
            }
            if range.start > range.end {
                return Ok(Bytes::new());
            }
            Ok(self.data.slice(range.start as usize..range.end as usize))
        }
    }

    #[tokio::test]
    async fn test_cached_range_reader_preserves_delegate_boundary_semantics() {
        let directory = tempfile::tempdir().unwrap();
        let delegate = Arc::new(BoundedReader {
            data: Bytes::from_static(b"abcde"),
        });
        let cache = Arc::new(
            LocalCache::new(LocalCacheConfig {
                dir: Some(directory.path().to_path_buf()),
                namespace: "test".to_string(),
                max_size: None,
                block_size: 4,
                whitelist: std::collections::HashSet::from([FileType::Meta]),
            })
            .unwrap(),
        );
        let reader = CachedFileReader::new(delegate, "snapshot-1", 5, cache);

        assert!(reader.read(3..8).await.is_err());
        let reversed_start = 4;
        let reversed_end = 3;
        assert_eq!(
            reader.read(reversed_start..reversed_end).await.unwrap(),
            Bytes::new()
        );
    }

    #[derive(Debug)]
    struct ShortReader;

    #[async_trait::async_trait]
    impl FileRead for ShortReader {
        async fn read(&self, _range: Range<u64>) -> crate::Result<Bytes> {
            Ok(Bytes::from_static(b"x"))
        }
    }

    #[tokio::test]
    async fn test_cached_range_reader_rejects_logically_short_block() {
        let directory = tempfile::tempdir().unwrap();
        let cache = Arc::new(
            LocalCache::new(LocalCacheConfig {
                dir: Some(directory.path().to_path_buf()),
                namespace: "test".to_string(),
                max_size: None,
                block_size: 4,
                whitelist: std::collections::HashSet::from([FileType::Meta]),
            })
            .unwrap(),
        );
        let reader = CachedFileReader::new(Arc::new(ShortReader), "snapshot-1", 8, cache);

        assert!(reader.read(2..4).await.is_err());
    }

    #[tokio::test]
    async fn test_cached_full_reader_loads_source_once_then_hits_blocks() {
        let delegate = Arc::new(CountingReader {
            data: Bytes::from_static(b"abcdefghijkl"),
            reads: AtomicUsize::new(0),
        });
        let cache = Arc::new(
            LocalCache::new(LocalCacheConfig {
                dir: None,
                namespace: "test".to_string(),
                max_size: None,
                block_size: 4,
                whitelist: std::collections::HashSet::from([FileType::Meta]),
            })
            .unwrap(),
        );
        let reader = CachedFileReader::new(delegate.clone(), "snapshot-1", 12, cache.clone());

        assert_eq!(
            reader.read_full().await.unwrap(),
            Bytes::from_static(b"abcdefghijkl")
        );
        assert_eq!(delegate.reads.load(Ordering::SeqCst), 1);
        assert_eq!(
            reader.read_full().await.unwrap(),
            Bytes::from_static(b"abcdefghijkl")
        );
        assert_eq!(delegate.reads.load(Ordering::SeqCst), 1);
    }
}
