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

use super::format::{get_box, int, level_starts, long, page_bytes, verify, Metadata, HEADER_SIZE};
use super::{invalid, BoundingBox, RTreeFileMeta};
use crate::io::FileRead;
use bytes::Bytes;
use futures::stream::FuturesUnordered;
use futures::{stream, StreamExt, TryStreamExt};
use lru::LruCache;
use roaring::{RoaringBitmap, RoaringTreemap};
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};

/// An immutable index file and the storage reader authorized to access it.
pub struct RTreeFileSource {
    pub meta: RTreeFileMeta,
    pub reader: Arc<dyn FileRead>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct FileIdentity {
    name: String,
    header: [u8; HEADER_SIZE],
    size: u64,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct PageKey {
    file: Arc<FileIdentity>,
    page: u32,
}

struct CacheState {
    pages: LruCache<PageKey, Bytes>,
    bytes: usize,
}

/// Byte-bounded cache shared by readers in one storage/authorization context.
/// Names must identify immutable files uniquely within this cache's context.
/// The budget accounts for page payloads; map/key overhead is additional.
pub struct RTreePageCache {
    capacity: usize,
    state: Mutex<CacheState>,
}

impl RTreePageCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            state: Mutex::new(CacheState {
                pages: LruCache::unbounded(),
                bytes: 0,
            }),
        }
    }

    fn get(&self, key: &PageKey) -> Option<Bytes> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pages
            .get(key)
            .cloned()
    }

    fn put(&self, key: PageKey, bytes: Bytes) {
        if bytes.len() > self.capacity {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.pages.contains(&key) {
            return;
        }
        state.bytes += bytes.len();
        state.pages.put(key, bytes);
        while state.bytes > self.capacity {
            if let Some((_, bytes)) = state.pages.pop_lru() {
                state.bytes -= bytes.len();
            } else {
                break;
            }
        }
    }

    pub fn cached_page_bytes(&self) -> usize {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).bytes
    }
}

struct File {
    level_starts: Vec<u32>,
    metadata: Metadata,
    identity: Arc<FileIdentity>,
    reader: Arc<dyn FileRead>,
}

#[derive(Clone, Copy)]
enum Predicate {
    Bounds(BoundingBox),
    Nulls(bool),
}

impl Predicate {
    fn matches(self, meta: &Metadata) -> bool {
        match self {
            Self::Bounds(query) => meta.bounds.intersects(&query),
            Self::Nulls(true) => meta.nulls != 0,
            Self::Nulls(false) => meta.non_nulls != 0,
        }
    }
}

/// Demand-loaded, Java-compatible R-tree reader. File IO is asynchronous and
/// batches merge adjacent pages and read at most four disjoint ranges at once.
pub struct RTreeIndexReader {
    files: Vec<File>,
    total_rows: u64,
    cache: Arc<RTreePageCache>,
    batch_bytes: usize,
    file_concurrency: usize,
    permits: Arc<tokio::sync::Semaphore>,
}

impl RTreeIndexReader {
    /// Default maximum simultaneously visited files across queries on this reader.
    pub const DEFAULT_FILE_CONCURRENCY: usize = 4;

    /// Set the maximum number of file traversals in flight across queries on this reader.
    pub fn with_file_concurrency(mut self, limit: usize) -> io::Result<Self> {
        if !(1..=tokio::sync::Semaphore::MAX_PERMITS).contains(&limit) {
            return Err(invalid("R-tree read file concurrency must be positive"));
        }
        self.file_concurrency = limit;
        self.permits = Arc::new(tokio::sync::Semaphore::new(limit));
        Ok(self)
    }

    pub(crate) fn with_permits(mut self, permits: Arc<tokio::sync::Semaphore>) -> Self {
        self.permits = permits;
        self
    }

    /// Validate all container metadata before any optional range pruning or IO.
    /// Dropping this reader does not clear the caller-owned shared cache.
    pub fn new(
        files: Vec<RTreeFileSource>,
        total_rows: u64,
        cache: Arc<RTreePageCache>,
        batch_bytes: usize,
    ) -> io::Result<Self> {
        if !(4096..=16 * 1024 * 1024).contains(&batch_bytes) {
            return Err(invalid("R-tree read batch size must be in [4 KiB, 16 MiB]"));
        }
        if files.is_empty() && total_rows > 0 {
            return Err(invalid("R-tree index files are missing"));
        }
        let files = files
            .into_iter()
            .map(|source| {
                let metadata = Metadata::parse(&source.meta, total_rows)?;
                let identity = Arc::new(FileIdentity {
                    name: source.meta.name,
                    header: source.meta.header,
                    size: source.meta.file_size,
                });
                Ok(File {
                    level_starts: level_starts(metadata.items, metadata.page_size),
                    metadata,
                    identity,
                    reader: source.reader,
                })
            })
            .collect::<io::Result<_>>()?;
        Ok(Self {
            files,
            total_rows,
            cache,
            batch_bytes,
            file_concurrency: Self::DEFAULT_FILE_CONCURRENCY,
            permits: Arc::new(tokio::sync::Semaphore::new(Self::DEFAULT_FILE_CONCURRENCY)),
        })
    }

    pub async fn query(&self, query: BoundingBox) -> io::Result<RoaringTreemap> {
        self.search(Predicate::Bounds(query)).await
    }

    pub async fn query_nulls(&self, nulls: bool) -> io::Result<RoaringTreemap> {
        self.search(Predicate::Nulls(nulls)).await
    }

    async fn search(&self, predicate: Predicate) -> io::Result<RoaringTreemap> {
        let mut files = self.files.iter().filter(|f| predicate.matches(&f.metadata));
        let mut active = FuturesUnordered::new();
        for file in files.by_ref().take(self.file_concurrency) {
            active.push(self.search_file(file, predicate));
        }
        let mut result = RoaringTreemap::new();
        let mut failure = None;
        while let Some(part) = active.next().await {
            match part {
                Ok(rows) if failure.is_none() => {
                    result |= rows;
                    if let Some(file) = files.next() {
                        active.push(self.search_file(file, predicate));
                    }
                }
                Err(err) if failure.is_none() => failure = Some(err),
                _ => {}
            }
        }
        failure.map_or(Ok(result), Err)
    }

    async fn search_file(&self, file: &File, predicate: Predicate) -> io::Result<RoaringTreemap> {
        let _permit = self.permits.acquire().await.map_err(io::Error::other)?;
        let mut access = Access {
            file,
            verified: false,
        };
        let meta = &file.metadata;
        match predicate {
            Predicate::Bounds(query) => {
                let mut result = RoaringTreemap::new();
                let mut children = RoaringBitmap::new();
                let mut pending = vec![(
                    (meta.pages - 1) as u32,
                    (file.level_starts.len() - 1) as u32,
                )];
                let batch_pages = (self.batch_bytes / page_bytes(meta.page_size)).max(1);
                while !pending.is_empty() {
                    let mut nodes = pending.split_off(pending.len().saturating_sub(batch_pages));
                    nodes.reverse();
                    let pages = self.pages(&mut access, &nodes).await?;
                    for (page_id, expected_level) in nodes {
                        let bytes = &pages[&page_id];
                        let count = int(bytes, 0) as usize;
                        let level = int(bytes, 4);
                        if count == 0 || count > meta.page_size || level != expected_level {
                            return Err(invalid("Invalid R-tree page header"));
                        }
                        for index in 0..count {
                            let at = 8 + index * 40;
                            let bounds = get_box(&bytes[at..at + 32])?;
                            let id = long(bytes, at + 32);
                            if level == 0 {
                                self.validate_row(meta, id)?;
                            } else if id < file.level_starts[level as usize - 1] as u64
                                || id >= file.level_starts[level as usize] as u64
                                || !children.insert(id as u32)
                            {
                                return Err(invalid("Invalid R-tree child page"));
                            }
                            if bounds.intersects(&query) {
                                if level == 0 {
                                    result.insert(id);
                                } else {
                                    pending.push((id as u32, level - 1));
                                }
                            }
                        }
                    }
                }
                Ok(result)
            }
            Predicate::Nulls(nulls) => {
                let target_count = if nulls { meta.nulls } else { meta.non_nulls };
                access.verify_header().await?;
                let mut offset = meta.rows_offset();
                if !nulls {
                    offset += meta.nulls as u64 * 8 + 4;
                }
                let bytes = access.read(offset, target_count * 8 + 4).await?;
                verify(&bytes)?;
                let mut seen = RoaringTreemap::new();
                for index in 0..target_count {
                    let id = long(&bytes, index * 8);
                    self.validate_row(meta, id)?;
                    if !seen.insert(id) {
                        return Err(invalid("Duplicate R-tree source row ID"));
                    }
                }
                Ok(seen)
            }
        }
    }

    fn validate_row(&self, meta: &Metadata, id: u64) -> io::Result<()> {
        if id < meta.first || id > meta.last || id >= self.total_rows {
            return Err(invalid("Invalid R-tree row ID"));
        }
        Ok(())
    }

    async fn pages(
        &self,
        access: &mut Access<'_>,
        nodes: &[(u32, u32)],
    ) -> io::Result<HashMap<u32, Bytes>> {
        let meta = &access.file.metadata;
        let page_size = page_bytes(meta.page_size);
        let mut pages = HashMap::with_capacity(nodes.len());
        let mut missing = Vec::new();
        for &(page, _) in nodes {
            if page as usize >= meta.pages {
                return Err(invalid("Invalid R-tree page ID"));
            }
            let key = PageKey {
                file: Arc::clone(&access.file.identity),
                page,
            };
            if let Some(bytes) = self.cache.get(&key) {
                pages.insert(page, bytes);
            } else {
                missing.push(page);
            }
        }
        if missing.is_empty() {
            return Ok(pages);
        }
        missing.sort_unstable();
        missing.dedup();
        access.verify_header().await?;
        let mut ranges = Vec::new();
        let mut at = 0;
        while at < missing.len() {
            let mut end = at + 1;
            while end < missing.len() && missing[end] == missing[end - 1] + 1 {
                end += 1;
            }
            ranges.push((at, end));
            at = end;
        }
        // FileRead uses immutable positioned ranges, so concurrent queries never share a cursor.
        let missing_pages = &missing;
        let access = &*access;
        let mut fetched = stream::iter(ranges)
            .map(|(first, end)| async move {
                let offset = HEADER_SIZE as u64 + missing_pages[first] as u64 * page_size as u64;
                let bytes = access.read(offset, (end - first) * page_size).await?;
                Ok::<_, io::Error>((first, end, bytes))
            })
            .buffer_unordered(4)
            .try_collect::<Vec<_>>()
            .await?;
        fetched.sort_unstable_by_key(|(first, _, _)| *first);
        for (first, end, merged) in fetched {
            for (index, &page) in missing.iter().enumerate().take(end).skip(first) {
                let at = (index - first) * page_size;
                // Copy each page: a Bytes slice would pin the entire merged range while
                // charging only one page against the cache budget.
                let bytes = Bytes::copy_from_slice(&merged[at..at + page_size]);
                verify(&bytes)?;
                self.cache.put(
                    PageKey {
                        file: Arc::clone(&access.file.identity),
                        page,
                    },
                    bytes.clone(),
                );
                pages.insert(page, bytes);
            }
        }
        Ok(pages)
    }
}

struct Access<'a> {
    file: &'a File,
    verified: bool,
}

impl Access<'_> {
    async fn read(&self, offset: u64, length: usize) -> io::Result<Bytes> {
        let bytes = self
            .file
            .reader
            .read(offset..offset + length as u64)
            .await
            .map_err(io::Error::other)?;
        if bytes.len() != length {
            return Err(invalid("Truncated R-tree read"));
        }
        Ok(bytes)
    }

    async fn verify_header(&mut self) -> io::Result<()> {
        if !self.verified {
            let bytes = self.read(0, HEADER_SIZE).await?;
            if bytes[..] != self.file.identity.header {
                return Err(invalid(
                    "R-tree file header disagrees with container metadata",
                ));
            }
            self.verified = true;
        }
        Ok(())
    }
}
