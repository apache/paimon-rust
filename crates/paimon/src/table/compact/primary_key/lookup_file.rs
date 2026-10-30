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

//! Disposable, versioned local SST files containing complete physical KV rows.

use crate::btree::{
    compress_block, decompress_block, read_block_from_bytes, BTreeFileFooter, BlockCompressionType,
    BlockHandle, BlockReader, SstFileWriter,
};
use crate::common::options::parse_memory_size;
use crate::io::cache::generated::{GeneratedFile, GeneratedFileRead};
use crate::io::cache::LocalCache;
use crate::io::FileRead;
use crate::table::ArrowRecordBatchStream;
use crate::{Error, Result};
use arrow_array::{RecordBatch, UInt64Array};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_row::{RowConverter, SortField};
use bytes::Bytes;
use futures::TryStreamExt;
use std::collections::HashMap;
use std::io::{Cursor, Write};
use std::sync::{Arc, Mutex};

const PREFIX: &str = "__paimon_generated__/lookup-sst-v2";
const HEAD_LEN: usize = 24;
const POINTER_LEN: usize = 45;

struct Payload {
    path: String,
    offset: u64,
    batch: Arc<RecordBatch>,
}

struct ParsedIndex {
    path: String,
    size: u64,
    footer_start: u64,
    block: BlockReader,
}

pub(in crate::table::compact) struct LookupFileCache {
    local: Arc<LocalCache>,
    limit: usize,
    compression: BlockCompressionType,
    // One executor cache owns this working budget. Serialize both builds and
    // warm index/IPC reads so different files cannot multiply that budget.
    working: tokio::sync::Mutex<()>,
    // One parsed index, charged to the same working budget as the IPC payload.
    index: Mutex<Option<Arc<ParsedIndex>>>,
    // One decoded IPC batch across all files, never one cache per file/key.
    payload: Mutex<Option<Payload>>,
    vector: Mutex<
        Option<(
            crate::DeletionFile,
            crate::deletion_vector::DeletionVector,
            Option<crate::resource::MemoryReservation>,
        )>,
    >,
}

impl LookupFileCache {
    pub(in crate::table::compact) fn new(
        local: Arc<LocalCache>,
        options: &HashMap<String, String>,
    ) -> Result<Self> {
        if options.contains_key("lookup.cache-max-disk-size") {
            return Err(invalid(
                "Lookup Files use local-cache.max-size, not lookup.cache-max-disk-size",
            ));
        }
        let memory = options
            .get("lookup.cache-max-memory-size")
            .map(|value| {
                parse_memory_size(value)
                    .map_err(|_| invalid("Invalid lookup.cache-max-memory-size"))
            })
            .transpose()?
            .unwrap_or(256 * 1024 * 1024);
        if memory == 0 {
            return Err(Error::ConfigInvalid {
                message: "lookup.cache-max-memory-size must be positive".into(),
            });
        }
        // Bound input, encoded/decoded payloads, SST construction and reads
        // separately, leaving room for copies and Arrow sort-key construction.
        let limit = usize::try_from(memory / 8).map_err(invalid)?;
        if limit < 64 * 1024 || local.generated_block_size() > limit as u64 {
            return Err(exhausted(
                "Lookup memory budget is too small for local-cache.block-size and SST metadata",
            ));
        }
        let compression = match options
            .get("lookup.cache-spill-compression")
            .map(String::as_str)
            .unwrap_or("zstd")
        {
            "none" => BlockCompressionType::None,
            "zstd" => BlockCompressionType::Zstd,
            "lz4" => BlockCompressionType::Lz4,
            "lzo" => BlockCompressionType::Lzo,
            _ => return Err(invalid("Invalid lookup.cache-spill-compression")),
        };
        Ok(Self {
            local,
            limit,
            compression,
            working: tokio::sync::Mutex::new(()),
            index: Mutex::new(None),
            payload: Mutex::new(None),
            vector: Mutex::new(None),
        })
    }

    pub(super) async fn lookup<F>(
        &self,
        identity: &str,
        key: &[u8],
        keys: &[usize],
        input: F,
    ) -> Result<Option<RecordBatch>>
    where
        F: Fn() -> Result<ArrowRecordBatchStream> + Send,
    {
        self.lookup_with_position(identity, key, keys, input)
            .await
            .map(|row| row.map(|(batch, _)| batch))
    }

    pub(super) async fn lookup_with_position<F>(
        &self,
        identity: &str,
        key: &[u8],
        keys: &[usize],
        input: F,
    ) -> Result<Option<(RecordBatch, u64)>>
    where
        F: Fn() -> Result<ArrowRecordBatchStream> + Send,
    {
        // Lock order is cache working set, then the shared per-file head.
        let _working = self.working.lock().await;
        let head = format!("{PREFIX}/{identity}/head");
        let _guard = self.local.generated_load_lock(&head).await;
        let mut published = self.local.generated_get(&head, 0, HEAD_LEN).await;
        for attempt in 0..2 {
            let descriptor = match published.take() {
                Some(bytes) => bytes,
                None => self.build(&head, input()?, keys).await?,
            };
            let generation = uuid::Uuid::from_slice(&descriptor[..16]).map_err(invalid)?;
            let path = format!("{PREFIX}/{identity}/{generation}");
            let size = u64::from_le_bytes(descriptor[16..].try_into().unwrap());
            match self.read(&path, size, key).await {
                Ok(row) => return Ok(row),
                Err(error) if attempt == 1 => return Err(error),
                Err(_) => {
                    *self.payload.lock().unwrap() = None;
                    *self.index.lock().unwrap() = None;
                    // A new immutable generation replaces the head only after
                    // close. Do not invalidate a shared backend's global epoch.
                }
            }
        }
        unreachable!()
    }

    /// One snapshot-dependent bitmap shares the executor's working budget.
    pub(super) async fn is_deleted(
        &self,
        io: &crate::io::FileIO,
        file: &crate::DeletionFile,
        position: u64,
        resources: Option<&crate::resource::ResourceContext>,
    ) -> Result<bool> {
        let _working = self.working.lock().await;
        {
            let cached = self.vector.lock().unwrap();
            if let Some((_, vector, _)) = cached.as_ref().filter(|(key, _, _)| key == file) {
                return Ok(vector.is_deleted(position));
            }
        }
        *self.vector.lock().unwrap() = None;
        if usize::try_from(file.length()).map_or(true, |size| size > self.limit) {
            return Err(exhausted(
                "Lookup deletion vector exceeds the memory budget",
            ));
        }
        let vector = crate::deletion_vector::DeletionVectorFactory::read(io, file).await?;
        let size = vector.retained_bytes();
        if size > self.limit {
            return Err(exhausted(
                "Lookup deletion vector exceeds the memory budget",
            ));
        }
        let reservation = resources
            .map(|resources| {
                let mut reservation = resources.reservation();
                reservation.try_grow(size)?;
                Ok::<_, Error>(reservation)
            })
            .transpose()?;
        let deleted = vector.is_deleted(position);
        *self.vector.lock().unwrap() = Some((file.clone(), vector, reservation));
        Ok(deleted)
    }

    async fn build(
        &self,
        head: &str,
        input: ArrowRecordBatchStream,
        keys: &[usize],
    ) -> Result<Bytes> {
        *self.index.lock().unwrap() = None;
        *self.payload.lock().unwrap() = None;
        let generation = uuid::Uuid::new_v4();
        let path = format!("{}/{generation}", head.strip_suffix("/head").unwrap());
        let mut file = GeneratedFile::new(self.local.clone(), path);
        let result = self.build_file(head, input, keys, &file, generation).await;
        if result.is_ok() {
            file.published();
        } else {
            file.cleanup().await;
        }
        result
    }

    async fn build_file(
        &self,
        head: &str,
        mut input: ArrowRecordBatchStream,
        keys: &[usize],
        file: &GeneratedFile,
        generation: uuid::Uuid,
    ) -> Result<Bytes> {
        let mut writer = SstFileWriter::with_compression_level(
            Box::new(file.writer()),
            32 * 1024,
            self.compression,
            1,
        );
        let mut previous = None::<Vec<u8>>;
        let mut converter = None;
        let mut position = 0u64;
        while let Some(batch) = input.try_next().await? {
            if batch.num_rows() == 0 {
                continue;
            }
            if batch.get_array_memory_size() > self.limit {
                return Err(exhausted("Lookup input batch exceeds lookup.cache-max-memory-size; reduce read.batch-size"));
            }
            if keys.is_empty() || keys.iter().any(|&index| index >= batch.num_columns()) {
                return Err(invalid("Invalid Lookup File primary-key projection"));
            }
            if converter.is_none() {
                converter = Some(
                    RowConverter::new(
                        keys.iter()
                            .map(|&key| {
                                SortField::new(batch.schema().field(key).data_type().clone())
                            })
                            .collect(),
                    )
                    .map_err(invalid)?,
                );
            }
            let rows = converter
                .as_ref()
                .unwrap()
                .convert_columns(
                    &keys
                        .iter()
                        .map(|&key| batch.column(key).clone())
                        .collect::<Vec<_>>(),
                )
                .map_err(invalid)?;
            if rows.size() > self.limit {
                return Err(exhausted("Lookup sort keys exceed the memory budget"));
            }
            let mut ipc = LimitedBuffer {
                bytes: Vec::new(),
                limit: self.limit,
                exceeded: false,
            };
            let encoded = (|| {
                let mut stream = StreamWriter::try_new(&mut ipc, batch.schema().as_ref())?;
                stream.write(&batch)?;
                stream.finish()
            })();
            if let Err(error) = encoded {
                if ipc.exceeded {
                    return Err(exhausted(error));
                }
                return Err(invalid(error));
            }
            let (bytes, compression) =
                compress_block(&ipc.bytes, self.compression, 1).map_err(invalid)?;
            if bytes.len() > self.limit {
                return Err(exhausted(
                    "Lookup encoded payload exceeds the memory budget",
                ));
            }
            let offset = writer.position();
            writer.write_raw(&bytes).await.map_err(exhausted)?;
            let pointer = Pointer {
                offset,
                length: bytes.len() as u64,
                raw_length: ipc.bytes.len() as u64,
                row: 0,
                position: 0,
                crc: crc32fast::hash(&bytes),
                compression,
            };
            for row in 0..batch.num_rows() {
                let key = rows.row(row);
                if previous.as_deref().is_some_and(|old| old >= key.as_ref()) {
                    return Err(invalid(
                        "Lookup history must have strictly increasing primary keys",
                    ));
                }
                previous = Some(key.as_ref().to_vec());
                if writer
                    .retained_bytes()
                    .saturating_add(key.as_ref().len().saturating_mul(3))
                    .saturating_add(POINTER_LEN)
                    > self.limit
                {
                    return Err(exhausted(
                        "Lookup SST construction exceeds lookup.cache-max-memory-size",
                    ));
                }
                writer
                    .put(
                        key.as_ref(),
                        &Pointer {
                            row: row as u64,
                            position: position + row as u64,
                            ..pointer
                        }
                        .encode(),
                    )
                    .await
                    .map_err(exhausted)?;
                if writer.retained_bytes() > self.limit {
                    return Err(exhausted(
                        "Lookup SST construction exceeds lookup.cache-max-memory-size",
                    ));
                }
            }
            position = position
                .checked_add(batch.num_rows() as u64)
                .ok_or_else(|| invalid("Lookup physical position overflow"))?;
        }
        writer.flush().await.map_err(exhausted)?;
        if writer.retained_bytes() > self.limit {
            return Err(exhausted(
                "Lookup SST construction exceeds lookup.cache-max-memory-size",
            ));
        }
        let index = writer.write_index_block().await.map_err(exhausted)?;
        writer
            .write_raw(&BTreeFileFooter::new(None, index, None).write_footer())
            .await
            .map_err(exhausted)?;
        let size = writer.position();
        writer.close().await.map_err(exhausted)?;
        let mut descriptor = generation.as_bytes().to_vec();
        descriptor.extend_from_slice(&size.to_le_bytes());
        let descriptor = Bytes::from(descriptor);
        self.local
            .generated_publish(head, descriptor.clone())
            .await?;
        Ok(descriptor)
    }

    async fn block(
        &self,
        reader: &GeneratedFileRead,
        size: u64,
        handle: BlockHandle,
    ) -> Result<BlockReader> {
        let length = u64::from(handle.size) + 5;
        if length > self.limit as u64 {
            return Err(exhausted("Lookup index block exceeds the memory budget"));
        }
        let end = handle
            .offset
            .checked_add(length)
            .filter(|&end| end <= size)
            .ok_or_else(|| invalid("Lookup block is outside its file"))?;
        let bytes = reader.read(handle.offset..end).await?;
        read_block_from_bytes(&bytes, handle.size).map_err(invalid)
    }

    async fn parsed_index(&self, path: &str, size: u64) -> Result<Arc<ParsedIndex>> {
        {
            let mut cached = self.index.lock().unwrap();
            if let Some(index) = cached
                .as_ref()
                .filter(|index| index.path == path && index.size == size)
            {
                return Ok(index.clone());
            }
            // Drop the previous file before allocating its replacement.
            *cached = None;
        }
        let footer_start = size
            .checked_sub(BTreeFileFooter::ENCODED_LENGTH as u64)
            .ok_or_else(|| invalid("Truncated Lookup File"))?;
        let reader = GeneratedFileRead::new(self.local.clone(), path.into(), size);
        let footer = BTreeFileFooter::read_footer(&reader.read(footer_start..size).await?)
            .map_err(invalid)?;
        let block = self
            .block(&reader, footer_start, footer.index_block_handle)
            .await?;
        if block.retained_bytes() > self.limit {
            return Err(exhausted("Lookup parsed index exceeds the memory budget"));
        }
        let index = Arc::new(ParsedIndex {
            path: path.into(),
            size,
            footer_start,
            block,
        });
        *self.index.lock().unwrap() = Some(index.clone());
        Ok(index)
    }

    async fn read(&self, path: &str, size: u64, key: &[u8]) -> Result<Option<(RecordBatch, u64)>> {
        let index = self.parsed_index(path, size).await?;
        let footer_start = index.footer_start;
        let reader = GeneratedFileRead::new(self.local.clone(), path.into(), size);
        let cmp = |a: &[u8], b: &[u8]| Ok(a.cmp(b));
        let (_, mut candidates) = index.block.seek_and_iter(key, &cmp).map_err(invalid)?;
        let Some((_, bytes)) = candidates.next() else {
            return Ok(None);
        };
        let block = self
            .block(
                &reader,
                footer_start,
                BlockHandle::decode(bytes).map_err(invalid)?,
            )
            .await?;
        let (found, mut entries) = block.seek_and_iter(key, &cmp).map_err(invalid)?;
        let Some((_, bytes)) = entries.next().filter(|_| found) else {
            return Ok(None);
        };
        let pointer = Pointer::decode(bytes)?;
        if pointer.length > self.limit as u64 || pointer.raw_length > self.limit as u64 {
            return Err(exhausted("Lookup IPC block exceeds the memory budget"));
        }
        let cached = self
            .payload
            .lock()
            .unwrap()
            .as_ref()
            .filter(|p| p.path == path && p.offset == pointer.offset)
            .map(|p| p.batch.clone());
        let batch = match cached {
            Some(batch) => batch,
            None => {
                let end = pointer
                    .offset
                    .checked_add(pointer.length)
                    .filter(|&end| end <= footer_start)
                    .ok_or_else(|| invalid("Lookup payload is outside its file"))?;
                let bytes = reader.read(pointer.offset..end).await?;
                if crc32fast::hash(&bytes) != pointer.crc {
                    return Err(invalid("Lookup IPC checksum mismatch"));
                }
                let decoded = decompress_block(&bytes, pointer.compression).map_err(invalid)?;
                if decoded.len() as u64 != pointer.raw_length {
                    return Err(invalid("Lookup IPC length mismatch"));
                }
                let mut stream =
                    StreamReader::try_new(Cursor::new(decoded), None).map_err(invalid)?;
                let batch = Arc::new(
                    stream
                        .next()
                        .ok_or_else(|| invalid("Empty Lookup IPC payload"))?
                        .map_err(invalid)?,
                );
                if stream.next().is_some() {
                    return Err(invalid("Lookup IPC payload contains multiple batches"));
                }
                if batch.get_array_memory_size() > self.limit {
                    return Err(exhausted("Decoded Lookup IPC exceeds the memory budget"));
                }
                *self.payload.lock().unwrap() = Some(Payload {
                    path: path.into(),
                    offset: pointer.offset,
                    batch: batch.clone(),
                });
                batch
            }
        };
        if pointer.row >= batch.num_rows() as u64 {
            return Err(invalid("Lookup row is outside its IPC batch"));
        }
        arrow_select::take::take_record_batch(&batch, &UInt64Array::from(vec![pointer.row]))
            .map(|batch| Some((batch, pointer.position)))
            .map_err(invalid)
    }
}

#[derive(Clone, Copy)]
struct Pointer {
    offset: u64,
    length: u64,
    raw_length: u64,
    row: u64,
    position: u64,
    crc: u32,
    compression: BlockCompressionType,
}
impl Pointer {
    fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(POINTER_LEN);
        for value in [
            self.offset,
            self.length,
            self.raw_length,
            self.row,
            self.position,
        ] {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        bytes.extend_from_slice(&self.crc.to_le_bytes());
        bytes.push(self.compression as u8);
        bytes
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != POINTER_LEN {
            return Err(invalid("Invalid Lookup IPC pointer"));
        }
        let number = |offset| u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        Ok(Self {
            offset: number(0),
            length: number(8),
            raw_length: number(16),
            row: number(24),
            position: number(32),
            crc: u32::from_le_bytes(bytes[40..44].try_into().unwrap()),
            compression: BlockCompressionType::from_persistent_id(bytes[44]).map_err(invalid)?,
        })
    }
}

struct LimitedBuffer {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}
impl Write for LimitedBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.bytes.len().saturating_add(bytes.len()) > self.limit {
            self.exceeded = true;
            return Err(std::io::Error::other(
                "Lookup IPC exceeds the memory budget",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn invalid(message: impl ToString) -> Error {
    Error::DataInvalid {
        message: message.to_string(),
        source: None,
    }
}
fn exhausted(message: impl ToString) -> Error {
    Error::ResourceExhausted {
        message: message.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{FileBlockCache, FileIOBuilder};
    use arrow_array::{ArrayRef, Int32Array, Int64Array, Int8Array, StringArray};
    use futures::{stream, StreamExt};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[derive(Debug, Default)]
    struct CountingBlocks {
        blocks: Arc<Mutex<HashMap<(String, u64), Bytes>>>,
        gets: AtomicUsize,
        invalidations: AtomicUsize,
        invalidated: tokio::sync::Notify,
        reject_head: AtomicBool,
        delay_put: AtomicBool,
        put_started: Arc<tokio::sync::Notify>,
        resume_put: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl FileBlockCache for CountingBlocks {
        async fn get(&self, path: &str, range: std::ops::Range<u64>) -> Option<Bytes> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            self.blocks
                .lock()
                .unwrap()
                .get(&(path.into(), range.start))
                .cloned()
                .filter(|bytes| bytes.len() as u64 == range.end - range.start)
        }
        async fn put(&self, path: &str, offset: u64, data: Bytes) {
            if path.ends_with("/head") && self.reject_head.load(Ordering::SeqCst) {
                return;
            }
            if self.delay_put.swap(false, Ordering::SeqCst) {
                let blocks = self.blocks.clone();
                let key = (path.to_owned(), offset);
                let started = self.put_started.clone();
                let resume = self.resume_put.clone();
                // Model a backend write that survives cancellation of its caller.
                tokio::spawn(async move {
                    started.notify_one();
                    resume.notified().await;
                    blocks.lock().unwrap().insert(key, data);
                })
                .await
                .unwrap();
            } else {
                self.blocks
                    .lock()
                    .unwrap()
                    .insert((path.into(), offset), data);
            }
        }
        async fn invalidate_path(&self, path: &str) {
            self.blocks
                .lock()
                .unwrap()
                .retain(|(stored, _), _| stored != path);
            self.invalidations.fetch_add(1, Ordering::SeqCst);
            self.invalidated.notify_one();
        }
        async fn invalidate_prefix(&self, prefix: &str) {
            self.blocks
                .lock()
                .unwrap()
                .retain(|(stored, _), _| !stored.starts_with(prefix));
        }
    }

    fn external() -> (Arc<LocalCache>, Arc<CountingBlocks>) {
        let blocks = Arc::new(CountingBlocks::default());
        let io = FileIOBuilder::new("memory")
            .build()
            .unwrap()
            .with_file_block_cache(blocks.clone(), 8192, "meta")
            .unwrap();
        (io.local_cache().unwrap(), blocks)
    }

    async fn wait_for_cleanup(blocks: &CountingBlocks, count: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while blocks.invalidations.load(Ordering::SeqCst) < count {
                blocks.invalidated.notified().await;
            }
        })
        .await
        .unwrap();
    }

    fn local(directory: Option<&std::path::Path>, shared: bool, capacity: &str) -> Arc<LocalCache> {
        local_with_whitelist(directory, shared, capacity, "meta")
    }

    fn local_with_whitelist(
        directory: Option<&std::path::Path>,
        shared: bool,
        capacity: &str,
        whitelist: &str,
    ) -> Arc<LocalCache> {
        let mut options = crate::Options::new();
        for (key, value) in [
            ("local-cache.enabled", "true"),
            ("local-cache.max-size", capacity),
            ("local-cache.block-size", "8kb"),
            ("local-cache.whitelist", whitelist),
        ] {
            options.set(key, value);
        }
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
    fn batch() -> RecordBatch {
        RecordBatch::try_from_iter([
            (
                "_SEQUENCE_NUMBER",
                Arc::new(Int64Array::from(vec![9, 10, 11])) as ArrayRef,
            ),
            (
                "_VALUE_KIND",
                Arc::new(Int8Array::from(vec![3, 2, 0])) as ArrayRef,
            ),
            ("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
            (
                "value",
                Arc::new(StringArray::from(vec![
                    Some("deleted payload"),
                    None,
                    Some("variable payload"),
                ])) as ArrayRef,
            ),
        ])
        .unwrap()
    }
    fn key(value: i32) -> Vec<u8> {
        RowConverter::new(vec![SortField::new(arrow_schema::DataType::Int32)])
            .unwrap()
            .convert_columns(&[Arc::new(Int32Array::from(vec![value]))])
            .unwrap()
            .row(0)
            .as_ref()
            .to_vec()
    }
    fn source(
        counter: Arc<AtomicUsize>,
    ) -> impl Fn() -> Result<ArrowRecordBatchStream> + Send + Sync {
        move || {
            counter.fetch_add(1, Ordering::Relaxed);
            Ok(stream::once(async {
                tokio::task::yield_now().await;
                Ok(batch())
            })
            .boxed())
        }
    }
    async fn descriptor(cache: &LookupFileCache, identity: &str) -> Option<Bytes> {
        cache
            .local
            .generated_get(&format!("{PREFIX}/{identity}/head"), 0, HEAD_LEN)
            .await
    }

    #[tokio::test]
    async fn parsed_index_is_reused_and_missing_data_still_rebuilds_its_generation() {
        let (local, blocks) = external();
        let cache = LookupFileCache::new(local, &HashMap::new()).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let input = source(calls.clone());
        cache.lookup("index", &key(1), &[2], &input).await.unwrap();
        let before = descriptor(&cache, "index").await.unwrap();
        for rebuilt in [false, true] {
            blocks.gets.store(0, Ordering::SeqCst);
            for value in [2, 3, 1] {
                assert_eq!(
                    cache
                        .lookup("index", &key(value), &[2], &input)
                        .await
                        .unwrap(),
                    Some(batch().slice((value - 1) as usize, 1))
                );
            }
            // Only the head and target data block are read for each key; the
            // footer/index and decoded IPC payload remain resident.
            assert_eq!(blocks.gets.load(Ordering::SeqCst), 6);
            if !rebuilt {
                blocks
                    .blocks
                    .lock()
                    .unwrap()
                    .retain(|(path, _), _| path.ends_with("/head"));
                assert_eq!(
                    cache.lookup("index", &key(1), &[2], &input).await.unwrap(),
                    Some(batch().slice(0, 1))
                );
                assert_ne!(descriptor(&cache, "index").await.unwrap(), before);
            }
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn cold_hot_and_concurrent_lookups_retain_complete_kvs_on_all_backends() {
        for backend in ["memory", "disk", "shared"] {
            let directory = tempfile::tempdir().unwrap();
            let local = local(
                (backend != "memory").then_some(directory.path()),
                backend == "shared",
                "4mb",
            );
            let cache = LookupFileCache::new(local.clone(), &HashMap::new()).unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let input = source(calls.clone());
            let results = futures::future::join_all((0..8).map(|_| async {
                cache
                    .lookup("full-kv", &key(2), &[2], &input)
                    .await
                    .unwrap()
            }))
            .await;
            assert!(results
                .iter()
                .all(|row| row.as_ref() == Some(&batch().slice(1, 1))));
            for value in [1, 3, 2] {
                assert_eq!(
                    cache
                        .lookup("full-kv", &key(value), &[2], &input)
                        .await
                        .unwrap(),
                    Some(batch().slice((value - 1) as usize, 1))
                );
            }
            assert!(cache
                .lookup("full-kv", &key(99), &[2], &input)
                .await
                .unwrap()
                .is_none());
            assert_eq!(calls.load(Ordering::Relaxed), 1, "{backend}");
            let active = Arc::new(AtomicUsize::new(0));
            let peak = Arc::new(AtomicUsize::new(0));
            let distinct_input = || {
                let active = active.clone();
                let peak = peak.clone();
                Ok(async_stream::try_stream! {
                    let concurrent = active.fetch_add(1, Ordering::Relaxed) + 1;
                    peak.fetch_max(concurrent, Ordering::Relaxed);
                    yield batch();
                    // Keep the source open after one decoded batch, while
                    // other identities have a chance to start building.
                    tokio::task::yield_now().await;
                    active.fetch_sub(1, Ordering::Relaxed);
                }
                .boxed())
            };
            let distinct = futures::future::join_all((0..8).map(|id| {
                let cache = &cache;
                let input = &distinct_input;
                async move {
                    cache
                        .lookup(&format!("distinct-{id}"), &key(2), &[2], input)
                        .await
                        .unwrap()
                }
            }))
            .await;
            assert!(distinct
                .iter()
                .all(|row| row.as_ref() == Some(&batch().slice(1, 1))));
            assert_eq!(peak.load(Ordering::Relaxed), 1, "{backend}");
            assert_eq!(active.load(Ordering::Relaxed), 0);
            // A fresh cache object has no decoded payload, but can reuse the
            // published local file without calling the source closure.
            drop(cache);
            let cache = LookupFileCache::new(local, &HashMap::new()).unwrap();
            assert_eq!(
                cache
                    .lookup("full-kv", &key(1), &[2], &input)
                    .await
                    .unwrap(),
                Some(batch().slice(0, 1))
            );
            assert_eq!(calls.load(Ordering::Relaxed), 1);
        }
    }

    #[tokio::test]
    async fn shared_head_build_does_not_hold_a_colliding_source_download_stripe() {
        let directory = tempfile::tempdir().unwrap();
        let local = local_with_whitelist(Some(directory.path()), true, "4mb", "data");
        let mut bytes = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut bytes, batch().schema().as_ref()).unwrap();
            writer.write(&batch()).unwrap();
            writer.finish().unwrap();
        }
        let length = bytes.len() as u64;
        let reader = crate::io::cache::generated::colliding_cached_source(
            local.clone(),
            &format!("{PREFIX}/source-collision/head"),
            Bytes::from(bytes),
        );
        let input = || {
            let reader = reader.clone();
            Ok(async_stream::try_stream! {
                let bytes = reader.read(0..length).await?;
                let mut stream = StreamReader::try_new(Cursor::new(bytes), None).map_err(invalid)?;
                yield stream.next().unwrap().map_err(invalid)?;
            }
            .boxed())
        };
        let cache = LookupFileCache::new(local, &HashMap::new()).unwrap();
        let lookup_key = key(2);
        let row = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            cache.lookup("source-collision", &lookup_key, &[2], input),
        )
        .await
        .expect("a cold Lookup File must not wait for its own source download stripe")
        .unwrap();
        assert_eq!(row, Some(batch().slice(1, 1)));
    }

    #[tokio::test]
    async fn interleaved_payloads_and_sst_blocks_support_point_lookups() {
        let options = HashMap::from([("lookup.cache-spill-compression".into(), "none".into())]);
        let cache = LookupFileCache::new(local(None, false, "4mb"), &options).unwrap();
        let calls = AtomicUsize::new(0);
        let input = || {
            calls.fetch_add(1, Ordering::Relaxed);
            let batches = (0..20)
                .map(|batch| {
                    let ids = (batch * 100..(batch + 1) * 100).collect::<Vec<i32>>();
                    let values = ids
                        .iter()
                        .map(|id| format!("payload-{id:08}"))
                        .collect::<Vec<_>>();
                    RecordBatch::try_from_iter([
                        ("id", Arc::new(Int32Array::from(ids)) as ArrayRef),
                        ("value", Arc::new(StringArray::from(values)) as ArrayRef),
                    ])
                    .map_err(invalid)
                })
                .collect::<Vec<_>>();
            Ok(stream::iter(batches).boxed())
        };
        for id in [0, 99, 100, 975, 1501, 1999] {
            let row = cache
                .lookup("blocks", &key(id), &[0], &input)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                row.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(0),
                id
            );
            assert_eq!(
                row.column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                format!("payload-{id:08}")
            );
        }
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn missing_and_corrupt_artifacts_are_rebuilt_instead_of_becoming_key_misses() {
        for corrupt in [false, true] {
            let local = local(None, false, "64kb");
            let cache = LookupFileCache::new(local.clone(), &HashMap::new()).unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let input = source(calls.clone());
            cache.lookup("repair", &key(1), &[2], &input).await.unwrap();
            if corrupt {
                let head = descriptor(&cache, "repair").await.unwrap();
                let generation = uuid::Uuid::from_slice(&head[..16]).unwrap();
                let path = format!("{PREFIX}/repair/{generation}");
                let size = u64::from_le_bytes(head[16..].try_into().unwrap());
                let mut bytes = local
                    .generated_get(&path, 0, size.min(8192) as usize)
                    .await
                    .unwrap()
                    .to_vec();
                bytes[0] ^= 0xff;
                local
                    .generated_put(&path, 0, Bytes::from(bytes))
                    .await
                    .unwrap();
                *cache.payload.lock().unwrap() = None;
            } else {
                for block in 0..16 {
                    local
                        .generated_put("cache-pressure", block, Bytes::from(vec![0; 8192]))
                        .await
                        .unwrap();
                }
                assert!(descriptor(&cache, "repair").await.is_none());
            }
            assert_eq!(
                cache.lookup("repair", &key(1), &[2], &input).await.unwrap(),
                Some(batch().slice(0, 1))
            );
            assert_eq!(calls.load(Ordering::Relaxed), 2);
        }
    }

    #[tokio::test]
    async fn source_failure_and_cancellation_never_publish_partial_files() {
        let (local, blocks) = external();
        let cache = LookupFileCache::new(
            local,
            &HashMap::from([("lookup.cache-spill-compression".into(), "none".into())]),
        )
        .unwrap();
        let input = source(Arc::new(AtomicUsize::new(0)));
        cache
            .lookup("survivor", &key(1), &[2], &input)
            .await
            .unwrap();
        let survivor = descriptor(&cache, "survivor").await.unwrap();
        let large = RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int32Array::from((0..4096).collect::<Vec<_>>())) as ArrayRef,
            ),
            (
                "value",
                Arc::new(StringArray::from(vec!["x".repeat(64); 4096])) as ArrayRef,
            ),
        ])
        .unwrap();
        let failed =
            || Ok(stream::iter(vec![Ok(large.clone()), Err(invalid("source failed"))]).boxed());
        let only_survivor = || {
            assert!(blocks
                .blocks
                .lock()
                .unwrap()
                .keys()
                .all(|(path, _)| path.starts_with(&format!("{PREFIX}/survivor/"))))
        };
        for _ in 0..2 {
            assert!(cache
                .lookup("failed", &key(1), &[0], &failed)
                .await
                .is_err());
            assert!(descriptor(&cache, "failed").await.is_none());
            only_survivor();
        }
        // Closing the SST is insufficient: a rejected head must reclaim it too.
        blocks.reject_head.store(true, Ordering::SeqCst);
        assert!(cache
            .lookup("head-failed", &key(1), &[2], &input)
            .await
            .is_err());
        blocks.reject_head.store(false, Ordering::SeqCst);
        only_survivor();
        let ready = Arc::new(tokio::sync::Notify::new());
        let stalled = || {
            let ready = ready.clone();
            let blocks = blocks.clone();
            Ok(stream::iter(vec![Ok(large.clone())])
                .chain(stream::once(async move {
                    assert!(
                        blocks
                            .blocks
                            .lock()
                            .unwrap()
                            .keys()
                            .filter(|(path, _)| path.starts_with(&format!("{PREFIX}/cancelled/")))
                            .count()
                            > 2
                    );
                    ready.notify_one();
                    futures::future::pending::<Result<RecordBatch>>().await
                }))
                .boxed())
        };
        let lookup_key = key(1);
        tokio::select! {
            result = cache.lookup("cancelled", &lookup_key, &[0], stalled) => panic!("unexpected completion: {result:?}"),
            _ = ready.notified() => {}
        }
        wait_for_cleanup(&blocks, 4).await;
        assert!(descriptor(&cache, "cancelled").await.is_none());
        only_survivor();
        blocks.delay_put.store(true, Ordering::SeqCst);
        tokio::select! {
            result = cache.lookup("late-put", &lookup_key, &[0], || Ok(stream::iter(vec![Ok(large.clone())]).boxed())) => panic!("unexpected completion: {result:?}"),
            _ = blocks.put_started.notified() => {}
        }
        blocks.resume_put.notify_one();
        wait_for_cleanup(&blocks, 5).await;
        only_survivor();
        assert_eq!(descriptor(&cache, "survivor").await.unwrap(), survivor);
        assert_eq!(
            cache
                .lookup("survivor", &key(2), &[2], &input)
                .await
                .unwrap(),
            Some(batch().slice(1, 1))
        );
    }

    #[tokio::test]
    async fn capacity_input_and_index_limits_are_enforced_without_publishing() {
        let cache = LookupFileCache::new(local(None, false, "1b"), &HashMap::new()).unwrap();
        assert!(matches!(
            cache
                .lookup(
                    "capacity",
                    &key(1),
                    &[2],
                    source(Arc::new(AtomicUsize::new(0)))
                )
                .await,
            Err(Error::ResourceExhausted { .. })
        ));
        assert!(descriptor(&cache, "capacity").await.is_none());
        let options = HashMap::from([
            ("lookup.cache-max-memory-size".into(), "1mb".into()),
            ("lookup.cache-spill-compression".into(), "none".into()),
        ]);
        let cache = LookupFileCache::new(local(None, false, "4mb"), &options).unwrap();
        let oversized = || {
            let large = "x".repeat(256 * 1024);
            Ok(stream::iter(vec![Ok(RecordBatch::try_from_iter([(
                "id",
                Arc::new(StringArray::from(vec![large])) as ArrayRef,
            )])
            .unwrap())])
            .boxed())
        };
        assert!(matches!(
            cache.lookup("input-limit", &[], &[0], oversized).await,
            Err(Error::ResourceExhausted { .. })
        ));
        assert!(descriptor(&cache, "input-limit").await.is_none());
        let wide_keys = || {
            let batches = (b'a'..=b'f')
                .map(|prefix| {
                    let key = format!("{}{}", prefix as char, "x".repeat(40_000));
                    RecordBatch::try_from_iter([(
                        "id",
                        Arc::new(StringArray::from(vec![key])) as ArrayRef,
                    )])
                    .map_err(invalid)
                })
                .collect::<Vec<_>>();
            Ok(stream::iter(batches).boxed())
        };
        let error = cache
            .lookup("index-limit", &[], &[0], wide_keys)
            .await
            .unwrap_err();
        assert!(matches!(&error, Error::ResourceExhausted { .. }), "{error}");
        assert!(error.to_string().contains("SST construction"), "{error}");
        assert!(descriptor(&cache, "index-limit").await.is_none());
    }
}
