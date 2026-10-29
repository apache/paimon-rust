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

use super::LocalCache;
use crate::io::{FileRead, FileWrite};
use crate::{Error, Result};
use bytes::{Bytes, BytesMut};
use std::ops::Range;
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
    cache: Arc<LocalCache>,
    path: String,
    buffer: BytesMut,
    index: u64,
    length: u64,
}

impl GeneratedFileWrite {
    pub(crate) fn new(cache: Arc<LocalCache>, path: String) -> Self {
        Self {
            cache,
            path,
            buffer: BytesMut::new(),
            index: 0,
            length: 0,
        }
    }

    async fn flush_block(&mut self) -> Result<()> {
        if !self.buffer.is_empty() {
            self.cache
                .generated_put(&self.path, self.index, self.buffer.split().freeze())
                .await?;
            self.index += 1;
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
        let size = usize::try_from(self.cache.block_size)
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
            let length =
                (self.length - index * self.cache.block_size).min(self.cache.block_size) as usize;
            if self
                .cache
                .generated_get(&self.path, index, length)
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
