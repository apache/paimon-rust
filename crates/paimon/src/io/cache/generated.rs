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

//! Disposable immutable files stored in the ordinary cache backend and budget.

use super::{CacheBackend, LocalCache};
use crate::io::{FileRead, FileWrite};
use crate::{Error, Result};
use bytes::{Bytes, BytesMut};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

impl LocalCache {
    pub(crate) fn generated_block_size(&self) -> u64 {
        self.block_size
    }

    pub(crate) async fn generated_get(&self, path: &str, index: u64, len: usize) -> Option<Bytes> {
        let token = self.read_token(path);
        self.get_block(&self.block_key(path, index), len, &token)
            .await
            .filter(|block| block.len() == len)
    }

    pub(crate) async fn generated_put(&self, path: &str, index: u64, data: Bytes) -> Result<()> {
        let token = self.read_token(path);
        let key = self.block_key(path, index);
        self.put_block(&key, data.clone(), &token).await;
        if self.get_block(&key, data.len(), &token).await.as_ref() != Some(&data) {
            return Err(Error::ResourceExhausted {
                message: "Local cache could not retain a Lookup File block; check local-cache.max-size and cache storage".into(),
            });
        }
        Ok(())
    }

    /// Coalesce process-local builds by their exact head key. Do not hold a
    /// shared download stripe while the build reads its source: a source block
    /// may use the same stripe and would wait for its own build to finish.
    pub(crate) async fn generated_load_lock(&self, path: &str) -> tokio::sync::OwnedMutexGuard<()> {
        let key = self.block_key(path, 0);
        self.block_load_lock(&key).await.lock_owned().await
    }

    pub(crate) async fn generated_publish(&self, path: &str, descriptor: Bytes) -> Result<()> {
        // Only publication holds the shared stripe. No source reads occur
        // here, and another publisher cannot overwrite before verification.
        let _guard = self.shared_load_lock(&self.block_key(path, 0)).await;
        self.generated_put(path, 0, descriptor).await
    }

    async fn generated_remove(&self, path: &str, blocks: u64) {
        let _guard = self
            .coordinator
            .begin_path_invalidation(&self.namespace, path)
            .await;
        match &self.backend {
            CacheBackend::Memory(cache) => cache.invalidate_path(&self.namespace, path),
            CacheBackend::Disk(cache) => {
                // Retry by exact key: path invalidation drops its in-memory
                // index before unlinking, so cancelling it could hide orphans.
                for index in 0..blocks {
                    cache.remove_block(&self.block_key(path, index)).await;
                }
            }
            CacheBackend::SharedDisk(cache) => {
                if let Err(error) = cache
                    .remove_generated(self.block_key(path, 0), blocks)
                    .await
                {
                    log::debug!("Failed to remove unpublished Lookup File: {error}");
                }
            }
            CacheBackend::External(cache) => cache.invalidate_path(path).await,
        }
    }
}

struct GeneratedFileState {
    cache: Arc<LocalCache>,
    path: String,
    write_gate: Arc<tokio::sync::Mutex<()>>,
    attempted_blocks: AtomicU64,
    #[cfg(test)]
    cleaned: Arc<tokio::sync::Notify>,
}

impl GeneratedFileState {
    async fn cleanup(&self) {
        // A cancelled caller must wait for any already-started backend put.
        let _guard = self.write_gate.lock().await;
        self.cache
            .generated_remove(&self.path, self.attempted_blocks.load(Ordering::SeqCst))
            .await;
        #[cfg(test)]
        self.cleaned.notify_one();
    }
}

/// Owns an unpublished generation until its head has been published.
pub(crate) struct GeneratedFile {
    state: Arc<GeneratedFileState>,
    runtime: Option<tokio::runtime::Handle>,
    settled: bool,
}

impl GeneratedFile {
    pub(crate) fn new(cache: Arc<LocalCache>, path: String) -> Self {
        Self {
            state: Arc::new(GeneratedFileState {
                cache,
                path,
                write_gate: Arc::new(tokio::sync::Mutex::new(())),
                attempted_blocks: AtomicU64::new(0),
                #[cfg(test)]
                cleaned: Arc::new(tokio::sync::Notify::new()),
            }),
            runtime: tokio::runtime::Handle::try_current().ok(),
            settled: false,
        }
    }

    pub(crate) fn writer(&self) -> GeneratedFileWrite {
        GeneratedFileWrite {
            state: self.state.clone(),
            buffer: BytesMut::new(),
            index: 0,
            length: 0,
        }
    }

    pub(crate) fn published(&mut self) {
        self.settled = true;
    }

    pub(crate) async fn cleanup(&mut self) {
        self.state.cleanup().await;
        self.settled = true;
    }
}

impl Drop for GeneratedFile {
    fn drop(&mut self) {
        if !self.settled {
            let state = self.state.clone();
            if let Some(runtime) = &self.runtime {
                runtime.spawn(async move { state.cleanup().await });
            }
        }
    }
}

#[cfg(test)]
pub(crate) fn colliding_cached_source(
    cache: Arc<LocalCache>,
    head: &str,
    data: Bytes,
) -> Arc<dyn FileRead> {
    struct Source(Bytes);
    #[async_trait::async_trait]
    impl FileRead for Source {
        async fn read(&self, range: Range<u64>) -> Result<Bytes> {
            Ok(self.0.slice(range.start as usize..range.end as usize))
        }
    }
    // Shared downloads use 256 lock stripes, not exact block locks. Find a
    // genuine source-block collision rather than relying on scheduling luck.
    let head_key = cache.block_key(head, 0).cache_relative_path();
    let path = (0..)
        .map(|index| format!("lookup-source-{index}.parquet"))
        .find(|path| {
            cache
                .block_key(path, 0)
                .cache_relative_path()
                .components()
                .next()
                == head_key.components().next()
        })
        .unwrap();
    assert!(cache.is_cacheable(&path));
    assert!(data.len() as u64 <= cache.generated_block_size());
    Arc::new(super::reader::CachedFileReader::new(
        Arc::new(Source(data.clone())),
        path,
        data.len() as u64,
        cache,
    ))
}

pub(crate) struct GeneratedFileRead {
    cache: Arc<LocalCache>,
    path: String,
    size: u64,
}

impl GeneratedFileRead {
    pub(crate) fn new(cache: Arc<LocalCache>, path: String, size: u64) -> Self {
        Self { cache, path, size }
    }
}

#[async_trait::async_trait]
impl FileRead for GeneratedFileRead {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        if range.start > range.end || range.end > self.size {
            return Err(invalid("Generated cache range exceeds its file"));
        }
        let len = usize::try_from(range.end - range.start)
            .map_err(|_| invalid("Generated cache range exceeds the address space"))?;
        let mut output = BytesMut::with_capacity(len);
        let mut offset = range.start;
        let size = self.cache.block_size;
        while offset < range.end {
            let index = offset / size;
            let start = index * size;
            let end = start.saturating_add(size).min(self.size);
            let len = usize::try_from(end - start)
                .map_err(|_| invalid("Generated cache block exceeds the address space"))?;
            let block = self
                .cache
                .generated_get(&self.path, index, len)
                .await
                .ok_or_else(|| invalid("Lookup File block was evicted or corrupt"))?;
            let take_end = range.end.min(end);
            output
                .extend_from_slice(&block[(offset - start) as usize..(take_end - start) as usize]);
            offset = take_end;
        }
        Ok(output.freeze())
    }
}

pub(crate) struct GeneratedFileWrite {
    state: Arc<GeneratedFileState>,
    buffer: BytesMut,
    index: u64,
    length: u64,
}

impl GeneratedFileWrite {
    async fn flush_block(&mut self) -> Result<()> {
        if !self.buffer.is_empty() {
            let index = self.index;
            self.index += 1;
            // Include the in-flight block even if cancellation happens before
            // the backend's publication/verification returns.
            self.state
                .attempted_blocks
                .store(self.index, Ordering::SeqCst);
            let data = self.buffer.split().freeze();
            let state = self.state.clone();
            // Cancelled callers may leave their current backend put running.
            // Reuse the cache's bounded fill slots instead of accumulating tasks.
            let slot = state
                .cache
                .fill_slots
                .clone()
                .acquire_owned()
                .await
                .map_err(|error| Error::UnexpectedError {
                    message: "Lookup File write capacity was closed".into(),
                    source: Some(Box::new(error)),
                })?;
            let gate = state.write_gate.clone().lock_owned().await;
            tokio::spawn(async move {
                let _slot = slot;
                let _gate = gate;
                state.cache.generated_put(&state.path, index, data).await
            })
            .await
            .map_err(|error| Error::UnexpectedError {
                message: "Lookup File cache write task failed".into(),
                source: Some(Box::new(error)),
            })??;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl FileWrite for GeneratedFileWrite {
    async fn write(&mut self, mut bytes: Bytes) -> Result<()> {
        self.length = self
            .length
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| invalid("Generated cache file length overflow"))?;
        let size = usize::try_from(self.state.cache.block_size)
            .map_err(|_| invalid("Generated cache block exceeds the address space"))?;
        while !bytes.is_empty() {
            let take = (size - self.buffer.len()).min(bytes.len());
            self.buffer.extend_from_slice(&bytes.split_to(take));
            if self.buffer.len() == size {
                self.flush_block().await?;
            }
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        self.flush_block().await?;
        for index in 0..self.index {
            let length = (self.length - index * self.state.cache.block_size)
                .min(self.state.cache.block_size) as usize;
            if self
                .state
                .cache
                .generated_get(&self.state.path, index, length)
                .await
                .is_none()
            {
                return Err(Error::ResourceExhausted {
                    message:
                        "Lookup File was evicted during construction; increase local-cache.max-size"
                            .into(),
                });
            }
        }
        Ok(())
    }
}

fn invalid(message: &str) -> Error {
    Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unpublished_blocks_are_reclaimed_without_invalidating_other_files_or_the_epoch() {
        for backend in ["memory", "disk", "shared"] {
            let directory = tempfile::tempdir().unwrap();
            let mut options = crate::Options::new();
            options.set("local-cache.enabled", "true");
            options.set("local-cache.block-size", "8kb");
            if backend != "memory" {
                options.set("local-cache.dir", directory.path().to_str().unwrap());
            }
            if backend == "shared" {
                options.set("local-cache.shared", "true");
                options.set("local-cache.max-size", "4mb");
            }
            // Memory and ordinary disk deliberately have no capacity limit.
            let local = super::super::create_local_cache(&options).unwrap().unwrap();
            let bytes = Bytes::from(vec![7; 8192]);
            let mut published = GeneratedFile::new(local.clone(), "published".into());
            let mut writer = published.writer();
            writer.write(bytes.clone()).await.unwrap();
            writer.close().await.unwrap();
            local
                .generated_publish("published/head", Bytes::from(vec![0; 24]))
                .await
                .unwrap();
            published.published();
            drop(published);
            let epoch = local.read_token("published").shared_generation;
            let usage = directory.path().join("paimon-shared-local-cache-v1/usage");
            let used = std::fs::read_to_string(&usage).ok();
            for attempt in 0..3 {
                let path = format!("unpublished-{attempt}");
                let mut file = GeneratedFile::new(local.clone(), path.clone());
                let cleaned = file.state.cleaned.clone();
                let mut writer = file.writer();
                writer
                    .write(Bytes::from(vec![9; 3 * 8192 + 1]))
                    .await
                    .unwrap();
                for index in 0..3 {
                    assert!(local.generated_get(&path, index, 8192).await.is_some());
                }
                drop(writer);
                if attempt == 1 {
                    // Cancellation drops the generation owner after full blocks
                    // have already reached its backend.
                    std::thread::spawn(move || drop(file)).join().unwrap();
                    tokio::time::timeout(std::time::Duration::from_secs(5), cleaned.notified())
                        .await
                        .unwrap();
                } else {
                    file.cleanup().await;
                }
                for index in 0..3 {
                    assert!(
                        local.generated_get(&path, index, 8192).await.is_none(),
                        "{backend}"
                    );
                    if backend == "disk" {
                        assert!(!directory
                            .path()
                            .join("paimon-local-cache-v2")
                            .join(local.block_key(&path, index).cache_relative_path())
                            .exists());
                    }
                }
                assert_eq!(
                    local.generated_get("published", 0, 8192).await,
                    Some(bytes.clone())
                );
                assert!(local.generated_get("published/head", 0, 24).await.is_some());
                assert_eq!(local.read_token("published").shared_generation, epoch);
                assert_eq!(std::fs::read_to_string(&usage).ok(), used);
                assert_eq!(local.fill_slots.available_permits(), 8);
            }
        }
    }
}
