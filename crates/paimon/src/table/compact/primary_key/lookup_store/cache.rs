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

//! Lookup artifacts stored in FileIO local-cache. SST keys are Arrow sort keys; values are
//! physical row positions. IPC payload blocks retain the entire KV state. No
//! table files or manifests depend on these disposable, versioned local files.

use crate::btree::{
    BTreeDataBlockCache, BTreeFileFooter, BTreeIndexMeta, BTreeIndexReader, BTreeIndexWriter,
    BlockCompressionType,
};
use crate::common::options::parse_memory_size;
use crate::io::cache::generated::{GeneratedFileRead, GeneratedFileWrite};
use crate::io::cache::LocalCache;
use crate::io::{FileRead, FileWrite};
use crate::table::ArrowRecordBatchStream;
use crate::{Error, Result};
use arrow_array::{RecordBatch, UInt64Array};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_row::{RowConverter, SortField};
use bytes::Bytes;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, SystemTime};

pub(super) fn invalid(message: impl ToString) -> Error {
    Error::DataInvalid {
        message: message.to_string(),
        source: None,
    }
}
fn exhausted(message: impl Into<String>) -> Error {
    Error::ResourceExhausted {
        message: message.into(),
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LookupCacheMetrics {
    pub file_hits: u64,
    pub file_builds: u64,
    pub file_evictions: u64,
}

#[derive(Serialize, Deserialize)]
struct PayloadBlock {
    first: u64,
    rows: u64,
    offset: u64,
    length: u64,
    crc: u32,
}
#[derive(Serialize, Deserialize)]
struct Manifest {
    version: u32,
    identity: String,
    generation: String,
    created_millis: u64,
    index_size: u64,
    data_size: u64,
    first_key: Option<Vec<u8>>,
    last_key: Option<Vec<u8>>,
    blocks: Vec<PayloadBlock>,
}
type Index = BTreeIndexReader<fn(&[u8], &[u8]) -> Result<std::cmp::Ordering>>;
pub(super) struct LookupFile {
    index: Index,
    data: GeneratedFileRead,
    manifest: Manifest,
    memory: usize,
    identity: String,
    payloads: Arc<Mutex<PayloadCache>>,
}
struct PayloadCache {
    entries: lru::LruCache<(String, u64), Arc<RecordBatch>>,
    bytes: usize,
    limit: usize,
}
impl PayloadCache {
    fn insert(&mut self, key: (String, u64), batch: Arc<RecordBatch>) -> Result<()> {
        let bytes = batch.get_array_memory_size();
        if bytes > self.limit {
            return Err(exhausted(
                "Decoded lookup block exceeds lookup.cache-max-memory-size",
            ));
        }
        while self.bytes.saturating_add(bytes) > self.limit {
            if let Some((_, old)) = self.entries.pop_lru() {
                self.bytes = self.bytes.saturating_sub(old.get_array_memory_size());
            } else {
                break;
            }
        }
        if let Some(old) = self.entries.put(key, batch) {
            self.bytes = self.bytes.saturating_sub(old.get_array_memory_size());
        }
        self.bytes += bytes;
        Ok(())
    }
}
impl LookupFile {
    pub(super) async fn lookup(&self, key: &[u8]) -> Result<Option<(RecordBatch, u64)>> {
        let rows = self.index.query_equal(key).await.map_err(invalid)?;
        if rows.is_empty() {
            return Ok(None);
        }
        if rows.len() != 1 {
            return Err(invalid("Lookup history contains duplicate primary keys"));
        }
        let position = rows.iter().next().unwrap();
        let block_index = self
            .manifest
            .blocks
            .partition_point(|block| block.first <= position);
        let block = block_index
            .checked_sub(1)
            .and_then(|index| self.manifest.blocks.get(index))
            .filter(|block| position < block.first + block.rows)
            .ok_or_else(|| invalid("Lookup position is outside payload blocks"))?;
        let payload_key = (self.identity.clone(), block.offset);
        let cached = self
            .payloads
            .lock()
            .unwrap()
            .entries
            .get(&payload_key)
            .cloned();
        let batch = if let Some(batch) = cached {
            batch
        } else {
            let bytes = self
                .data
                .read(block.offset..block.offset + block.length)
                .await?;
            if crc32fast::hash(&bytes) != block.crc {
                return Err(invalid("Lookup payload checksum mismatch"));
            }
            let mut reader = StreamReader::try_new(Cursor::new(bytes), None).map_err(invalid)?;
            let batch = Arc::new(
                reader
                    .next()
                    .ok_or_else(|| invalid("Empty lookup payload block"))?
                    .map_err(invalid)?,
            );
            if batch.num_rows() as u64 != block.rows {
                return Err(invalid("Lookup payload row count changed"));
            }
            self.payloads
                .lock()
                .unwrap()
                .insert(payload_key, batch.clone())?;
            batch
        };
        let indices = UInt64Array::from(vec![position - block.first]);
        let row =
            arrow_select::take::take_record_batch(batch.as_ref(), &indices).map_err(invalid)?;
        Ok(Some((row, position)))
    }
}

struct Entry {
    used: SystemTime,
    loaded: Arc<LookupFile>,
}
struct State {
    entries: HashMap<String, Entry>,
    locks: HashMap<String, Weak<tokio::sync::Mutex<()>>>,
    invalid: HashSet<String>,
}

pub(in crate::table) struct LookupCache {
    local: Arc<LocalCache>,
    max_memory: usize,
    retention: Duration,
    compression: BlockCompressionType,
    blocks: Arc<BTreeDataBlockCache>,
    payloads: Arc<Mutex<PayloadCache>>,
    state: Mutex<State>,
    hits: AtomicU64,
    builds: AtomicU64,
    evictions: AtomicU64,
}
type Registry = HashMap<(usize, usize, u64, u8), Weak<LookupCache>>;
static CACHES: OnceLock<Mutex<Registry>> = OnceLock::new();

fn artifact_path(identity: &str, generation: &str, name: &str) -> String {
    format!("__paimon_generated__/lookup-v2/{identity}/{generation}/{name}")
}
fn descriptor_path(identity: &str) -> String {
    format!("__paimon_generated__/lookup-v2/{identity}/head")
}
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
impl LookupCache {
    pub(in crate::table) fn open(
        local: Arc<LocalCache>,
        options: &HashMap<String, String>,
    ) -> Result<Arc<Self>> {
        if options.contains_key("lookup.cache-max-disk-size") {
            return Err(Error::ConfigInvalid {
                message:
                    "Lookup Files share local-cache.max-size; remove lookup.cache-max-disk-size"
                        .into(),
            });
        }
        let max_memory = options
            .get("lookup.cache-max-memory-size")
            .map(|value| {
                let value = parse_memory_size(value)
                    .map_err(|_| invalid("Invalid lookup.cache-max-memory-size"))?;
                usize::try_from(value)
                    .ok()
                    .filter(|value| *value > 0)
                    .ok_or_else(|| invalid("lookup.cache-max-memory-size must be positive"))
            })
            .transpose()?
            .unwrap_or(256 * 1024 * 1024);
        let retention = options
            .get("lookup.cache-file-retention")
            .map(|value| {
                crate::spec::parse_duration_millis(value)
                    .ok_or_else(|| invalid("Invalid lookup.cache-file-retention"))
            })
            .transpose()?
            .unwrap_or(3_600_000);
        let compression = match options
            .get("lookup.cache-spill-compression")
            .map(String::as_str)
            .unwrap_or("zstd")
        {
            "zstd" => BlockCompressionType::Zstd,
            "none" => BlockCompressionType::None,
            "lz4" => BlockCompressionType::Lz4,
            "lzo" => BlockCompressionType::Lzo,
            value => {
                return Err(invalid(format!(
                    "Unsupported lookup.cache-spill-compression: {value}"
                )))
            }
        };
        let key = (
            Arc::as_ptr(&local) as usize,
            max_memory,
            retention,
            compression as u8,
        );
        let mut caches = CACHES.get_or_init(Default::default).lock().unwrap();
        if let Some(cache) = caches.get(&key).and_then(Weak::upgrade) {
            return Ok(cache);
        }
        caches.retain(|_, cache| cache.strong_count() > 0);
        let cache = Arc::new(Self {
            local,
            max_memory,
            retention: Duration::from_millis(retention),
            compression,
            blocks: Arc::new(BTreeDataBlockCache::new(max_memory / 4)),
            payloads: Arc::new(Mutex::new(PayloadCache {
                entries: lru::LruCache::unbounded(),
                bytes: 0,
                limit: max_memory / 2,
            })),
            state: Mutex::new(State {
                entries: HashMap::new(),
                locks: HashMap::new(),
                invalid: HashSet::new(),
            }),
            hits: AtomicU64::new(0),
            builds: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        });
        caches.insert(key, Arc::downgrade(&cache));
        Ok(cache)
    }
    pub(super) fn memory_limit(&self) -> usize {
        self.max_memory
    }
    pub(in crate::table) fn metrics(&self) -> LookupCacheMetrics {
        LookupCacheMetrics {
            file_hits: self.hits.load(Ordering::Relaxed),
            file_builds: self.builds.load(Ordering::Relaxed),
            file_evictions: self.evictions.load(Ordering::Relaxed),
        }
    }
    pub(super) fn invalidate(&self, id: &str) {
        let mut state = self.state.lock().unwrap();
        state.entries.remove(id);
        state.invalid.insert(id.into());
        self.evictions.fetch_add(1, Ordering::Relaxed);
        // Do not invalidate FileIO paths: a shared-cache storage mutation fence
        // is unnecessary for immutable artifacts. The rebuild uses new block
        // paths and publishes its descriptor last. Old blocks age out by LRU.
    }
    async fn descriptor(&self, identity: &str) -> Result<Option<(Manifest, usize)>> {
        let Some(head) = self
            .local
            .generated_get(&descriptor_path(identity), 0, 24)
            .await
        else {
            return Ok(None);
        };
        let generation = uuid::Uuid::from_slice(&head[..16])
            .map_err(invalid)?
            .to_string();
        let length = u64::from_le_bytes(head[16..24].try_into().unwrap());
        if length > self.max_memory as u64 / 4 {
            return Err(exhausted(
                "Lookup descriptor exceeds lookup.cache-max-memory-size",
            ));
        }
        let reader = GeneratedFileRead::new(
            self.local.clone(),
            artifact_path(identity, &generation, "descriptor"),
            length,
        );
        let bytes = reader.read(0..length).await?;
        let manifest: Manifest = serde_json::from_slice(&bytes).map_err(invalid)?;
        if manifest.identity != identity
            || manifest.generation != generation
            || manifest.version != 2
        {
            return Err(invalid("Lookup cache identity/version mismatch"));
        }
        if Duration::from_millis(now_millis().saturating_sub(manifest.created_millis))
            >= self.retention
        {
            return Ok(None);
        }
        Ok(Some((manifest, bytes.len())))
    }
    pub(super) async fn get<F>(
        &self,
        identity: &str,
        input: F,
        keys: &[usize],
    ) -> Result<Arc<LookupFile>>
    where
        F: FnOnce() -> Result<ArrowRecordBatchStream> + Send,
    {
        let lock = {
            let mut state = self.state.lock().unwrap();
            if state.locks.len() > 1024 {
                state.locks.retain(|_, lock| lock.strong_count() > 0);
            }
            let lock = state
                .locks
                .get(identity)
                .and_then(Weak::upgrade)
                .unwrap_or_else(|| {
                    let lock = Arc::new(tokio::sync::Mutex::new(()));
                    state.locks.insert(identity.into(), Arc::downgrade(&lock));
                    lock
                });
            lock
        };
        let _guard = lock.lock().await;
        let force = {
            let mut state = self.state.lock().unwrap();
            if let Some(entry) = state.entries.get_mut(identity) {
                if entry.used.elapsed().unwrap_or_default() < self.retention
                    || Arc::strong_count(&entry.loaded) > 1
                {
                    entry.used = SystemTime::now();
                    self.hits.fetch_add(1, Ordering::Relaxed);
                    return Ok(entry.loaded.clone());
                }
                state.entries.remove(identity);
                state.invalid.insert(identity.into());
                self.evictions.fetch_add(1, Ordering::Relaxed);
            }
            state.invalid.remove(identity)
        };
        let recovered = if force {
            None
        } else {
            self.descriptor(identity).await?
        };
        let built = recovered.is_none();
        let (manifest, manifest_size) = match recovered {
            Some(value) => value,
            None => self.build(identity, input()?, keys).await?,
        };
        let mut end = 0u64;
        let mut first = 0u64;
        for block in &manifest.blocks {
            if block.offset != end
                || block.first != first
                || block.rows == 0
                || block.length > self.max_memory as u64 / 2
            {
                return Err(invalid("Invalid lookup payload block metadata"));
            }
            end = end
                .checked_add(block.length)
                .filter(|end| *end <= manifest.data_size)
                .ok_or_else(|| invalid("Lookup payload exceeds file length"))?;
            first = first
                .checked_add(block.rows)
                .ok_or_else(|| invalid("Lookup row count overflow"))?;
        }
        if end != manifest.data_size {
            return Err(invalid("Lookup payload coverage is incomplete"));
        }
        let reader = GeneratedFileRead::new(
            self.local.clone(),
            artifact_path(identity, &manifest.generation, "index"),
            manifest.index_size,
        );
        let footer = BTreeFileFooter::read_footer(
            &reader
                .read(
                    manifest
                        .index_size
                        .checked_sub(52)
                        .ok_or_else(|| invalid("Truncated lookup index"))?
                        ..manifest.index_size,
                )
                .await?,
        )
        .map_err(invalid)?;
        let memory = manifest_size
            .saturating_mul(3)
            .saturating_add(footer.index_block_handle.size as usize * 2)
            .saturating_add(
                footer
                    .bloom_filter_handle
                    .as_ref()
                    .map_or(0, |filter| filter.size as usize),
            )
            .saturating_add(4096);
        if memory > self.max_memory / 4 {
            return Err(exhausted(
                "Lookup file metadata exceeds lookup.cache-max-memory-size",
            ));
        }
        let meta =
            BTreeIndexMeta::new(manifest.first_key.clone(), manifest.last_key.clone(), false);
        let generation_key = format!("{identity}/{}", manifest.generation);
        let index = BTreeIndexReader::open_with_data_block_cache(
            Box::new(reader),
            manifest.index_size,
            &meta,
            (|a: &[u8], b: &[u8]| Ok(a.cmp(b))) as fn(&[u8], &[u8]) -> Result<std::cmp::Ordering>,
            self.blocks.clone(),
            Arc::from(generation_key.as_str()),
        )
        .await
        .map_err(invalid)?;
        let data = GeneratedFileRead::new(
            self.local.clone(),
            artifact_path(identity, &manifest.generation, "rows"),
            manifest.data_size,
        );
        let file = Arc::new(LookupFile {
            index,
            data,
            manifest,
            memory,
            identity: generation_key,
            payloads: self.payloads.clone(),
        });
        let mut state = self.state.lock().unwrap();
        let mut retained = state
            .entries
            .values()
            .map(|entry| entry.loaded.memory)
            .sum::<usize>();
        if retained.saturating_add(memory) > self.max_memory / 4 {
            state.entries.retain(|_, entry| {
                if Arc::strong_count(&entry.loaded) == 1 {
                    retained = retained.saturating_sub(entry.loaded.memory);
                    false
                } else {
                    true
                }
            });
        }
        if retained.saturating_add(memory) > self.max_memory / 4 {
            return Err(exhausted(
                "Lookup memory cache is full and readers are pinned",
            ));
        }
        state.entries.insert(
            identity.into(),
            Entry {
                used: SystemTime::now(),
                loaded: file.clone(),
            },
        );
        if !built {
            self.hits.fetch_add(1, Ordering::Relaxed);
        }
        Ok(file)
    }
    async fn build(
        &self,
        identity: &str,
        mut input: ArrowRecordBatchStream,
        keys: &[usize],
    ) -> Result<(Manifest, usize)> {
        if self.local.generated_block_size() > self.max_memory as u64 / 4 {
            return Err(exhausted(
                "local-cache.block-size exceeds the Lookup File working memory budget",
            ));
        }
        let generation = uuid::Uuid::new_v4();
        let generation_id = generation.to_string();
        let (index_writer, index_size) = GeneratedFileWrite::new(
            self.local.clone(),
            artifact_path(identity, &generation_id, "index"),
        );
        let mut index = BTreeIndexWriter::with_comparator_and_options(
            Box::new(index_writer),
            32 * 1024,
            self.compression,
            1,
            true,
            (|a: &[u8], b: &[u8]| Ok(a.cmp(b))) as fn(&[u8], &[u8]) -> Result<std::cmp::Ordering>,
        );
        let (mut data, _) = GeneratedFileWrite::new(
            self.local.clone(),
            artifact_path(identity, &generation_id, "rows"),
        );
        let mut blocks = Vec::new();
        let mut offset = 0u64;
        let mut position = 0u64;
        let mut converter = None;
        let mut previous = None::<Vec<u8>>;
        while let Some(batch) = input.try_next().await? {
            if batch.num_rows() == 0 {
                continue;
            }
            if converter.is_none() {
                converter = Some(
                    RowConverter::new(
                        keys.iter()
                            .map(|key| {
                                SortField::new(batch.schema().field(*key).data_type().clone())
                            })
                            .collect(),
                    )
                    .map_err(invalid)?,
                );
            }
            let columns = keys
                .iter()
                .map(|index| batch.column(*index).clone())
                .collect::<Vec<_>>();
            let rows = converter
                .as_mut()
                .unwrap()
                .convert_columns(&columns)
                .map_err(invalid)?;
            for row in 0..batch.num_rows() {
                let key = rows.row(row);
                if previous
                    .as_deref()
                    .is_some_and(|previous| previous > key.as_ref())
                {
                    return Err(invalid("Lookup source is not primary-key ordered"));
                }
                previous = Some(key.as_ref().to_vec());
                index
                    .write(
                        Some(key.as_ref()),
                        i64::try_from(position + row as u64).map_err(invalid)?,
                    )
                    .await
                    .map_err(invalid)?;
            }
            let mut bytes = Vec::new();
            {
                let mut writer =
                    StreamWriter::try_new(&mut bytes, batch.schema().as_ref()).map_err(invalid)?;
                writer.write(&batch).map_err(invalid)?;
                writer.finish().map_err(invalid)?;
            }
            if bytes.len() > self.max_memory / 2 {
                return Err(exhausted("Lookup payload block exceeds lookup.cache-max-memory-size; reduce read.batch-size"));
            }
            if index
                .retained_bytes()
                .saturating_add(blocks.capacity() * std::mem::size_of::<PayloadBlock>())
                > self.max_memory / 4
            {
                return Err(exhausted(
                    "Lookup index construction exceeds lookup.cache-max-memory-size",
                ));
            }
            let length = bytes.len() as u64;
            let crc = crc32fast::hash(&bytes);
            data.write(Bytes::from(bytes)).await?;
            blocks.push(PayloadBlock {
                first: position,
                rows: batch.num_rows() as u64,
                offset,
                length,
                crc,
            });
            offset += length;
            position += batch.num_rows() as u64;
        }
        data.close().await?;
        let result = index.finish().await.map_err(invalid)?;
        let index_size = index_size.load(Ordering::Relaxed);
        let manifest = Manifest {
            version: 2,
            generation: generation_id.clone(),
            created_millis: now_millis(),
            identity: identity.into(),
            index_size,
            data_size: offset,
            first_key: result.meta.first_key,
            last_key: result.meta.last_key,
            blocks,
        };
        let bytes = serde_json::to_vec(&manifest).map_err(invalid)?;
        let length = bytes.len();
        if length > self.max_memory / 4 {
            return Err(exhausted(
                "Lookup descriptor exceeds lookup.cache-max-memory-size",
            ));
        }
        let (mut descriptor, _) = GeneratedFileWrite::new(
            self.local.clone(),
            artifact_path(identity, &generation_id, "descriptor"),
        );
        descriptor.write(Bytes::from(bytes)).await?;
        descriptor.close().await?;
        // The fixed-size head is the only mutable pointer. A failed/cancelled
        // build never publishes it; partial blocks stay in the bounded LRU.
        let mut head = generation.as_bytes().to_vec();
        head.extend_from_slice(&(length as u64).to_le_bytes());
        self.local
            .generated_put(&descriptor_path(identity), 0, Bytes::from(head))
            .await?;
        self.builds.fetch_add(1, Ordering::Relaxed);
        Ok((manifest, length))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int32Array, Int64Array, Int8Array, StringArray};
    use futures::StreamExt;

    fn batch() -> RecordBatch {
        RecordBatch::try_from_iter([
            (
                "_SEQUENCE_NUMBER",
                Arc::new(Int64Array::from_iter_values(0..100)) as ArrayRef,
            ),
            (
                "_VALUE_KIND",
                Arc::new(Int8Array::from(vec![0; 100])) as ArrayRef,
            ),
            (
                "id",
                Arc::new(Int32Array::from_iter_values(0..100)) as ArrayRef,
            ),
            (
                "text",
                Arc::new(StringArray::from(vec!["x".repeat(256); 100])) as ArrayRef,
            ),
        ])
        .unwrap()
    }
    fn input(count: Arc<AtomicU64>) -> ArrowRecordBatchStream {
        futures::stream::once(async move {
            count.fetch_add(1, Ordering::Relaxed);
            Ok(batch())
        })
        .boxed()
    }
    fn key(value: i32) -> Vec<u8> {
        let converter =
            RowConverter::new(vec![SortField::new(arrow_schema::DataType::Int32)]).unwrap();
        converter
            .convert_columns(&[Arc::new(Int32Array::from(vec![value]))])
            .unwrap()
            .row(0)
            .as_ref()
            .to_vec()
    }

    fn local(directory: Option<&std::path::Path>, shared: bool, capacity: &str) -> Arc<LocalCache> {
        let mut options = crate::Options::new();
        options.set("local-cache.enabled", "true");
        options.set("local-cache.max-size", capacity);
        options.set("local-cache.block-size", "8kb");
        if let Some(directory) = directory {
            options.set("local-cache.dir", directory.to_str().unwrap());
        }
        if shared {
            options.set("local-cache.shared", "true");
        }
        crate::io::cache::create_local_cache(&options)
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn concurrent_misses_share_one_build_on_memory_disk_and_shared_backends() {
        for backend in ["memory", "disk", "shared"] {
            let directory = tempfile::tempdir().unwrap();
            let local = local(
                (backend != "memory").then_some(directory.path()),
                backend == "shared",
                "4mb",
            );
            let cache = LookupCache::open(local, &HashMap::new()).unwrap();
            let count = Arc::new(AtomicU64::new(0));
            let id = "a".repeat(64);
            let (left, right) = tokio::join!(
                cache.get(&id, || Ok(input(count.clone())), &[2]),
                cache.get(&id, || Ok(input(count.clone())), &[2])
            );
            let left = left.unwrap();
            let right = right.unwrap();
            assert!(Arc::ptr_eq(&left, &right));
            let (row, position) = left.lookup(&key(42)).await.unwrap().unwrap();
            assert_eq!(position, 42);
            assert_eq!(
                row.column(3)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                "x".repeat(256)
            );
            assert!(right.lookup(&key(999)).await.unwrap().is_none());
            let warm = cache
                .get(
                    &id,
                    || panic!("cache hits must not construct a source reader"),
                    &[2],
                )
                .await
                .unwrap();
            assert!(Arc::ptr_eq(&left, &warm));
            assert_eq!(count.load(Ordering::Relaxed), 1);
            assert_eq!(cache.metrics().file_builds, 1);
        }
    }

    #[tokio::test]
    async fn lookup_artifacts_compete_with_cached_reads_in_the_same_capacity() {
        struct Source;
        #[async_trait::async_trait]
        impl FileRead for Source {
            async fn read(&self, range: std::ops::Range<u64>) -> Result<Bytes> {
                Ok(Bytes::from(vec![1; (range.end - range.start) as usize]))
            }
        }
        for disk in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let local = local(disk.then_some(directory.path()), false, "128kb");
            let id = "b".repeat(64);
            let count = Arc::new(AtomicU64::new(0));
            let cache = LookupCache::open(local.clone(), &HashMap::new()).unwrap();
            drop(
                cache
                    .get(&id, || Ok(input(count.clone())), &[2])
                    .await
                    .unwrap(),
            );
            drop(cache);
            // More ordinary data blocks than the entire cache can retain must
            // evict generated blocks too; there is no separate lookup quota.
            let reader = crate::io::cache::CachedFileReader::new(
                Arc::new(Source),
                "s3://bucket/data.parquet",
                256 * 1024,
                local.clone(),
            );
            reader.read(0..256 * 1024).await.unwrap();
            assert!(local
                .generated_get(&descriptor_path(&id), 0, 24)
                .await
                .is_none());
            let reopened = LookupCache::open(local, &HashMap::new()).unwrap();
            assert!(reopened
                .get(&id, || Ok(input(count.clone())), &[2])
                .await
                .unwrap()
                .lookup(&key(42))
                .await
                .unwrap()
                .is_some());
            assert_eq!(count.load(Ordering::Relaxed), 2);
        }
    }

    #[tokio::test]
    async fn completed_artifacts_reopen_after_the_disk_cache_manager_is_recreated() {
        let directory = tempfile::tempdir().unwrap();
        let id = "c".repeat(64);
        let count = Arc::new(AtomicU64::new(0));
        {
            let cache =
                LookupCache::open(local(Some(directory.path()), false, "1mb"), &HashMap::new())
                    .unwrap();
            drop(
                cache
                    .get(&id, || Ok(input(count.clone())), &[2])
                    .await
                    .unwrap(),
            );
        }
        let reopened =
            LookupCache::open(local(Some(directory.path()), false, "1mb"), &HashMap::new())
                .unwrap();
        let file = reopened
            .get(
                &id,
                || panic!("complete disk artifacts must survive manager recreation"),
                &[2],
            )
            .await
            .unwrap();
        assert_eq!(file.lookup(&key(42)).await.unwrap().unwrap().1, 42);
        assert_eq!(reopened.metrics().file_builds, 0);
        assert_eq!(count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn cancellation_and_capacity_failure_never_publish_a_descriptor() {
        let id = "d".repeat(64);
        let tiny = LookupCache::open(local(None, false, "1b"), &HashMap::new()).unwrap();
        assert!(matches!(
            tiny.get(&id, || Ok(input(Arc::new(AtomicU64::new(0)))), &[2])
                .await
                .err()
                .unwrap(),
            Error::ResourceExhausted { .. }
        ));
        assert!(tiny
            .local
            .generated_get(&descriptor_path(&id), 0, 24)
            .await
            .is_none());
        let cache = LookupCache::open(local(None, false, "128kb"), &HashMap::new()).unwrap();
        let (started, wait) = tokio::sync::oneshot::channel();
        let stream = futures::stream::once(async { Ok(batch()) })
            .chain(futures::stream::once(async move {
                let _ = started.send(());
                futures::future::pending::<Result<RecordBatch>>().await
            }))
            .boxed();
        let cloned = cache.clone();
        let task_id = id.clone();
        let task = tokio::spawn(async move { cloned.get(&task_id, || Ok(stream), &[2]).await });
        wait.await.unwrap();
        task.abort();
        assert!(task.await.err().unwrap().is_cancelled());
        assert!(cache
            .local
            .generated_get(&descriptor_path(&id), 0, 24)
            .await
            .is_none());
        assert!(cache
            .get(&id, || Ok(input(Arc::new(AtomicU64::new(0)))), &[2])
            .await
            .unwrap()
            .lookup(&key(42))
            .await
            .unwrap()
            .is_some());
        assert_eq!(cache.metrics().file_builds, 1);
    }

    #[tokio::test]
    async fn decoded_memory_and_expired_pinned_reader_limits_are_enforced() {
        let local = local(None, false, "1mb");
        let id = "e".repeat(64);
        let tiny = LookupCache::open(
            local.clone(),
            &HashMap::from([("lookup.cache-max-memory-size".into(), "1b".into())]),
        )
        .unwrap();
        assert!(matches!(
            tiny.get(&id, || Ok(input(Arc::new(AtomicU64::new(0)))), &[2])
                .await
                .err()
                .unwrap(),
            Error::ResourceExhausted { .. }
        ));
        let cache = LookupCache::open(
            local,
            &HashMap::from([("lookup.cache-file-retention".into(), "0ms".into())]),
        )
        .unwrap();
        let count = Arc::new(AtomicU64::new(0));
        let pinned = cache
            .get(&id, || Ok(input(count.clone())), &[2])
            .await
            .unwrap();
        let reused = cache
            .get(&id, || panic!("expired pinned reader stays usable"), &[2])
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&pinned, &reused));
        drop(reused);
        drop(pinned);
        drop(
            cache
                .get(&id, || Ok(input(count.clone())), &[2])
                .await
                .unwrap(),
        );
        assert_eq!(count.load(Ordering::Relaxed), 2);
    }
}
