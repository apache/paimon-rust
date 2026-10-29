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

//! Disposable generated files in the same backend and budget as cached reads.
//! Callers publish a descriptor only after closing every immutable file. A
//! missing block is an error, never EOF: the caller must rebuild the artifact.

use super::LocalCache;
use crate::io::{FileRead, FileWrite};
use crate::{Error, Result};
use bytes::{Bytes, BytesMut};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

impl LocalCache {
    // Generated artifacts deliberately bypass the remote-file whitelist. Their
    // paths include an artifact format and immutable build ID, and participate
    // in the ordinary cache's namespace, accounting, CRCs and regular LRU.
    pub(crate) async fn generated_get(
        &self,
        path: &str,
        index: u64,
        length: usize,
    ) -> Option<Bytes> {
        let token = self.read_token(path);
        let block = self
            .get_block(&self.block_key(path, index), length, &token)
            .await?;
        (block.len() == length).then_some(block)
    }

    pub(crate) async fn generated_put(&self, path: &str, index: u64, value: Bytes) -> Result<()> {
        let length = value.len();
        let token = self.read_token(path);
        let key = self.block_key(path, index);
        self.put_block(&key, value.clone(), &token).await;
        if self.get_block(&key, length, &token).await.as_ref() != Some(&value) {
            return Err(Error::ResourceExhausted {
                message: "Local cache could not retain a Lookup File block; check local-cache.max-size and cache storage".into(),
            });
        }
        Ok(())
    }

    pub(crate) fn generated_block_size(&self) -> u64 {
        self.block_size
    }
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
            return Err(Error::DataInvalid {
                message: "Generated cache file read exceeds its length".into(),
                source: None,
            });
        }
        let mut output =
            BytesMut::with_capacity(usize::try_from(range.end - range.start).map_err(|_| {
                Error::DataInvalid {
                    message: "Generated cache range is too large".into(),
                    source: None,
                }
            })?);
        let block_size = self.cache.block_size;
        let mut offset = range.start;
        while offset < range.end {
            let index = offset / block_size;
            let start = index * block_size;
            let end = start.saturating_add(block_size).min(self.size);
            let length = usize::try_from(end - start).map_err(|_| Error::DataInvalid {
                message: "Generated cache block is too large".into(),
                source: None,
            })?;
            let block = self.cache.generated_get(&self.path, index, length).await.ok_or_else(|| Error::DataInvalid {
                message: "Lookup cache block was evicted or corrupt; rebuild the Lookup File (check local-cache.max-size if this repeats)".into(), source: None,
            })?;
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
    position: Arc<AtomicU64>,
}

impl GeneratedFileWrite {
    pub(crate) fn new(cache: Arc<LocalCache>, path: String) -> (Self, Arc<AtomicU64>) {
        let position = Arc::new(AtomicU64::new(0));
        (
            Self {
                cache,
                path,
                buffer: BytesMut::new(),
                index: 0,
                position: position.clone(),
            },
            position,
        )
    }

    async fn flush(&mut self) -> Result<()> {
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
        let total = bytes.len() as u64;
        let block_size =
            usize::try_from(self.cache.block_size).map_err(|_| Error::ConfigInvalid {
                message: "local-cache.block-size exceeds the address space".into(),
            })?;
        while !bytes.is_empty() {
            let take = (block_size - self.buffer.len()).min(bytes.len());
            self.buffer.extend_from_slice(&bytes.split_to(take));
            if self.buffer.len() == block_size {
                self.flush().await?;
            }
        }
        self.position.fetch_add(total, Ordering::Relaxed);
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        self.flush().await
    }
}
