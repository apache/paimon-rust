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

use super::format::{
    decode_bit_block, decode_int_block, decode_quad_block, read_blocks, read_directory,
    read_footer, read_index_meta, validate_container, BitBlock, BitBlockMeta, BitVectorMeta,
    BlockInfo, Directory, Footer, IntBlockMeta, IntVectorMeta, QuadBlock, QuadBlockMeta,
    QuadVectorMeta, SharedFileRead, BLOCK_BITS, BLOCK_WORDS, MAX_DATA_BLOCK_UNCOMPRESSED_LENGTH,
    QUAD_BLOCK_VALUES, VALUE_BLOCK_INTS,
};
use crate::io::FileRead;
use lru::LruCache;
use roaring::RoaringTreemap;
use std::io;
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;

const MIN_LOCATE_CACHE_BYTES: usize = BLOCK_WORDS * 8 + (BLOCK_WORDS.div_ceil(64) + 1) * 4 * 4;
const MAX_CONCURRENT_FILE_READS: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct FMReadOptions {
    pub(crate) cache_size: usize,
    pub(crate) demand_page_size: usize,
    pub(crate) locate_cost_ratio: f64,
}

impl FMReadOptions {
    pub(crate) fn validate(self) -> io::Result<Self> {
        if !(MAX_DATA_BLOCK_UNCOMPRESSED_LENGTH..=64 * 1024 * 1024).contains(&self.demand_page_size)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "FM demand page size must be in [64 KiB, 64 MiB]",
            ));
        }
        if !self.locate_cost_ratio.is_finite()
            || self.locate_cost_ratio <= 0.0
            || self.locate_cost_ratio > 1.0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "FM locate cost ratio must be in (0, 1]",
            ));
        }
        Ok(self)
    }
}

impl Default for FMReadOptions {
    fn default() -> Self {
        Self {
            cache_size: 64 * 1024 * 1024,
            demand_page_size: 512 * 1024,
            locate_cost_ratio: 0.001,
        }
    }
}

struct PartitionReader {
    footer: Footer,
    directory: Directory,
}

pub(crate) struct FMReadContext {
    cache: Mutex<DecodedCache>,
    file_reads: Semaphore,
}

impl FMReadContext {
    pub(crate) fn new(cache_size: usize) -> Self {
        Self {
            cache: Mutex::new(DecodedCache::new(cache_size)),
            file_reads: Semaphore::new(MAX_CONCURRENT_FILE_READS),
        }
    }
}

pub(crate) struct FMGlobalIndexReader {
    reader: SharedFileRead,
    file_size: u64,
    partitions: Vec<PartitionReader>,
    options: FMReadOptions,
    context: Arc<FMReadContext>,
    cache_namespace: Arc<str>,
}

impl FMGlobalIndexReader {
    #[cfg(test)]
    pub(crate) async fn open(
        reader: Box<dyn FileRead>,
        file_size: u64,
        manifest_meta: &[u8],
        options: FMReadOptions,
    ) -> io::Result<Self> {
        let context = Arc::new(FMReadContext::new(options.cache_size));
        Self::open_with_context(reader, file_size, manifest_meta, options, context, "").await
    }

    pub(crate) async fn open_with_context(
        reader: Box<dyn FileRead>,
        file_size: u64,
        manifest_meta: &[u8],
        options: FMReadOptions,
        context: Arc<FMReadContext>,
        cache_namespace: impl Into<Arc<str>>,
    ) -> io::Result<Self> {
        let options = options.validate()?;
        let index_meta = read_index_meta(manifest_meta)?;
        let reader: SharedFileRead = Arc::from(reader);
        let open_permit = context
            .file_reads
            .acquire()
            .await
            .map_err(|_| io::Error::other("FM file-read concurrency guard was closed"))?;
        validate_container(reader.as_ref(), file_size, &index_meta).await?;
        let mut partitions = Vec::with_capacity(index_meta.partitions.len());
        for partition in &index_meta.partitions {
            let footer = read_footer(reader.as_ref(), file_size, partition).await?;
            let directory = read_directory(reader.as_ref(), &footer).await?;
            partitions.push(PartitionReader { footer, directory });
        }
        drop(open_permit);
        Ok(Self {
            reader,
            file_size,
            partitions,
            options,
            context,
            cache_namespace: cache_namespace.into(),
        })
    }

    /// Return `None` when the configured locate budget asks the caller to scan source rows.
    pub(crate) async fn contains(&self, needle: &[u8]) -> io::Result<Option<RoaringTreemap>> {
        if needle.is_empty() {
            return self.null_rows(false).await.map(Some);
        }
        if !self.supports_locate() {
            return Ok(None);
        }
        let mut result = RoaringTreemap::new();
        for partition in &self.partitions {
            let interval = self.backward_search(&partition.directory, needle).await?;
            if interval.0 >= interval.1 {
                continue;
            }
            if !self.should_locate(&partition.directory, interval) {
                return Ok(None);
            }
            for bwt_row in interval.0..interval.1 {
                let text_position = self.locate(&partition.directory, bwt_row).await?;
                if text_position >= partition.directory.text_length - 1 {
                    return Err(invalid("FM locate returned an invalid text position"));
                }
                let row_ordinal = self
                    .rank_ones(&partition.directory.row_boundaries, text_position)
                    .await?;
                if row_ordinal >= partition.directory.row_count {
                    return Err(invalid("FM locate returned an invalid row ordinal"));
                }
                result.insert(partition.footer.first_row_id + row_ordinal as u64);
            }
        }
        Ok(Some(result))
    }

    pub(crate) async fn is_null(&self) -> io::Result<RoaringTreemap> {
        self.null_rows(true).await
    }

    pub(crate) async fn is_not_null(&self) -> io::Result<RoaringTreemap> {
        self.null_rows(false).await
    }

    async fn backward_search(
        &self,
        directory: &Directory,
        needle: &[u8],
    ) -> io::Result<(usize, usize)> {
        let mut lower = 0usize;
        let mut upper = directory.text_length;
        for byte in needle.iter().rev() {
            if lower >= upper {
                break;
            }
            let symbol = directory.byte_to_symbol[*byte as usize];
            if symbol < 0 {
                return Ok((0, 0));
            }
            let symbol = symbol as usize;
            let cumulative = directory.cumulative_counts[symbol];
            let (lower_rank, upper_rank) = self.rank_pair(directory, symbol, lower, upper).await?;
            lower = cumulative + lower_rank;
            upper = cumulative + upper_rank;
        }
        Ok((lower, upper))
    }

    fn should_locate(&self, directory: &Directory, interval: (usize, usize)) -> bool {
        if !self.supports_locate() {
            return false;
        }
        let locate_cost = (interval.1 - interval.0) as f64 * directory.sample_rate as f64;
        let text_bytes = directory.text_length - directory.row_count - 1;
        locate_cost < self.options.locate_cost_ratio * text_bytes as f64
    }

    fn supports_locate(&self) -> bool {
        self.options.cache_size >= MIN_LOCATE_CACHE_BYTES
    }

    async fn locate(&self, directory: &Directory, bwt_row: usize) -> io::Result<usize> {
        let mut current = bwt_row;
        let mut steps = 0usize;
        while !self.bit(&directory.sampled_rows, current).await? {
            current = self.lf(directory, current).await?;
            steps += 1;
            if steps >= directory.sample_rate {
                return Err(invalid("FM SA locate exceeded its value-sampling bound"));
            }
        }
        let sample_ordinal = self.rank_ones(&directory.sampled_rows, current).await?;
        let sample = self
            .sample_value(&directory.sample_values, sample_ordinal)
            .await?;
        if sample >= directory.text_length || sample % directory.sample_rate != 0 {
            return Err(invalid("Invalid FM sampled suffix value"));
        }
        Ok((sample + steps) % directory.text_length)
    }

    async fn lf(&self, directory: &Directory, position: usize) -> io::Result<usize> {
        let mut current = position;
        let mut symbol = 0usize;
        for level in 0..directory.level_count {
            let vector = &directory.wavelets[level];
            let meta = vector.block(current)?;
            let block = self.quad_block(vector, meta).await?;
            let local = current - meta.first_value;
            let digit = block.get(local)?;
            symbol = (symbol << 2) | digit;
            current = directory.digit_starts[level][digit]
                + meta.prefix_counts[digit]
                + block.rank(digit, local)?;
        }
        if symbol >= directory.alphabet_size
            || current < directory.wavelet_starts[symbol]
            || current
                >= directory.wavelet_starts[symbol] + directory.cumulative_counts[symbol + 1]
                    - directory.cumulative_counts[symbol]
        {
            return Err(invalid("FM LF mapping returned an invalid row"));
        }
        Ok(directory.cumulative_counts[symbol] + current - directory.wavelet_starts[symbol])
    }

    async fn rank_pair(
        &self,
        directory: &Directory,
        symbol: usize,
        mut lower: usize,
        mut upper: usize,
    ) -> io::Result<(usize, usize)> {
        if lower > upper || upper > directory.text_length {
            return Err(invalid("Invalid FM rank interval"));
        }
        for level in 0..directory.level_count {
            let shift = (directory.level_count - level - 1) * 2;
            let digit = (symbol >> shift) & 3;
            let start = directory.digit_starts[level][digit];
            let ranks = self
                .rank_digit_pair(&directory.wavelets[level], digit, lower, upper)
                .await?;
            lower = start + ranks.0;
            upper = start + ranks.1;
        }
        let symbol_start = directory.wavelet_starts[symbol];
        Ok((lower - symbol_start, upper - symbol_start))
    }

    async fn rank_digit_pair(
        &self,
        vector: &QuadVectorMeta,
        digit: usize,
        lower: usize,
        upper: usize,
    ) -> io::Result<(usize, usize)> {
        if lower == upper {
            let rank = self.rank_digit(vector, digit, lower).await?;
            return Ok((rank, rank));
        }
        let lower_meta = rank_block(vector, lower)?;
        let upper_meta = rank_block(vector, upper)?;
        if let (Some(lower_meta), Some(upper_meta)) = (lower_meta, upper_meta) {
            if lower_meta.first_value == upper_meta.first_value {
                let block = self.quad_block(vector, lower_meta).await?;
                return Ok((
                    lower_meta.prefix_counts[digit]
                        + block.rank(digit, lower - lower_meta.first_value)?,
                    upper_meta.prefix_counts[digit]
                        + block.rank(digit, upper - upper_meta.first_value)?,
                ));
            }
        }
        Ok((
            self.rank_digit(vector, digit, lower).await?,
            self.rank_digit(vector, digit, upper).await?,
        ))
    }

    async fn rank_digit(
        &self,
        vector: &QuadVectorMeta,
        digit: usize,
        end: usize,
    ) -> io::Result<usize> {
        if end == 0 {
            return Ok(0);
        }
        if end == vector.value_length {
            return Ok(vector.total_counts[digit]);
        }
        if end.is_multiple_of(QUAD_BLOCK_VALUES) {
            return Ok(vector.block(end)?.prefix_counts[digit]);
        }
        let meta = vector.block(end - 1)?;
        Ok(meta.prefix_counts[digit]
            + self
                .quad_block(vector, meta)
                .await?
                .rank(digit, end - meta.first_value)?)
    }

    async fn quad_block(
        &self,
        vector: &QuadVectorMeta,
        meta: &QuadBlockMeta,
    ) -> io::Result<Arc<QuadBlock>> {
        let key = CacheKey::new(&self.cache_namespace, meta.block, CacheKind::Quad);
        if let Some(CacheValue::Quad(block)) = self.context.cache.lock().unwrap().get(&key) {
            return Ok(block);
        }
        let block_index = meta.first_value / QUAD_BLOCK_VALUES;
        let page = demand_page(
            &vector.blocks,
            block_index,
            self.effective_page_size(),
            |value| value.block.uncompressed_length,
        )?;
        let infos = page.iter().map(|value| value.block).collect::<Vec<_>>();
        let bytes = self.read_blocks(&infos).await?;
        let mut requested = None;
        for (page_meta, bytes) in page.iter().zip(bytes) {
            let page_key = CacheKey::new(&self.cache_namespace, page_meta.block, CacheKind::Quad);
            let cached = { self.context.cache.lock().unwrap().get(&page_key) };
            let block = match cached {
                Some(CacheValue::Quad(block)) => block,
                _ => {
                    let block = Arc::new(decode_quad_block(&bytes, page_meta)?);
                    self.context.cache.lock().unwrap().put(
                        page_key,
                        CacheValue::Quad(Arc::clone(&block)),
                        block.retained_size(),
                    );
                    block
                }
            };
            if page_meta.first_value == meta.first_value {
                requested = Some(block);
            }
        }
        requested.ok_or_else(|| invalid("FM demand page missed its requested quad block"))
    }

    async fn bit(&self, vector: &BitVectorMeta, position: usize) -> io::Result<bool> {
        let meta = vector.block(position)?;
        self.bit_block(vector, meta)
            .await?
            .get(position - meta.first_bit)
    }

    async fn rank_ones(&self, vector: &BitVectorMeta, end: usize) -> io::Result<usize> {
        if end == 0 {
            return Ok(0);
        }
        if end == vector.bit_length {
            return Ok(vector.total_ones);
        }
        if end.is_multiple_of(BLOCK_BITS) {
            return Ok(vector.block(end)?.prefix_ones);
        }
        let meta = vector.block(end - 1)?;
        Ok(meta.prefix_ones
            + self
                .bit_block(vector, meta)
                .await?
                .rank_ones(end - meta.first_bit)?)
    }

    async fn bit_block(
        &self,
        vector: &BitVectorMeta,
        meta: &BitBlockMeta,
    ) -> io::Result<Arc<BitBlock>> {
        let key = CacheKey::new(&self.cache_namespace, meta.block, CacheKind::Bit);
        if let Some(CacheValue::Bit(block)) = self.context.cache.lock().unwrap().get(&key) {
            return Ok(block);
        }
        let block_index = meta.first_bit / BLOCK_BITS;
        let page = demand_page(
            &vector.blocks,
            block_index,
            self.effective_page_size(),
            |value| value.block.uncompressed_length,
        )?;
        let infos = page.iter().map(|value| value.block).collect::<Vec<_>>();
        let bytes = self.read_blocks(&infos).await?;
        let mut requested = None;
        for (page_meta, bytes) in page.iter().zip(bytes) {
            let page_key = CacheKey::new(&self.cache_namespace, page_meta.block, CacheKind::Bit);
            let cached = { self.context.cache.lock().unwrap().get(&page_key) };
            let block = match cached {
                Some(CacheValue::Bit(block)) => block,
                _ => {
                    let block = Arc::new(decode_bit_block(&bytes, page_meta)?);
                    self.context.cache.lock().unwrap().put(
                        page_key,
                        CacheValue::Bit(Arc::clone(&block)),
                        block.retained_size(),
                    );
                    block
                }
            };
            if page_meta.first_bit == meta.first_bit {
                requested = Some(block);
            }
        }
        requested.ok_or_else(|| invalid("FM demand page missed its requested bit block"))
    }

    async fn sample_value(&self, vector: &IntVectorMeta, position: usize) -> io::Result<usize> {
        let meta = vector.block(position)?;
        let key = CacheKey::new(&self.cache_namespace, meta.block, CacheKind::Int);
        let cached = { self.context.cache.lock().unwrap().get(&key) };
        let values = match cached {
            Some(CacheValue::Int(values)) => values,
            _ => self.load_int_block(vector, meta).await?,
        };
        values
            .get(position - meta.first_value)
            .copied()
            .ok_or_else(|| invalid("FM sample offset is outside its block"))
    }

    async fn load_int_block(
        &self,
        vector: &IntVectorMeta,
        meta: &IntBlockMeta,
    ) -> io::Result<Arc<Vec<usize>>> {
        let block_index = meta.first_value / VALUE_BLOCK_INTS;
        let page = demand_page(
            &vector.blocks,
            block_index,
            self.effective_page_size(),
            |value| value.block.uncompressed_length,
        )?;
        let infos = page.iter().map(|value| value.block).collect::<Vec<_>>();
        let bytes = self.read_blocks(&infos).await?;
        let mut requested = None;
        for (page_meta, bytes) in page.iter().zip(bytes) {
            let page_key = CacheKey::new(&self.cache_namespace, page_meta.block, CacheKind::Int);
            let cached = { self.context.cache.lock().unwrap().get(&page_key) };
            let values = match cached {
                Some(CacheValue::Int(values)) => values,
                _ => {
                    let values = Arc::new(decode_int_block(&bytes, page_meta)?);
                    self.context.cache.lock().unwrap().put(
                        page_key,
                        CacheValue::Int(Arc::clone(&values)),
                        values.len() * std::mem::size_of::<usize>(),
                    );
                    values
                }
            };
            if page_meta.first_value == meta.first_value {
                requested = Some(values);
            }
        }
        requested.ok_or_else(|| invalid("FM demand page missed its requested sample block"))
    }

    async fn null_rows(&self, select_nulls: bool) -> io::Result<RoaringTreemap> {
        let mut result = RoaringTreemap::new();
        for partition in &self.partitions {
            let vector = &partition.directory.null_rows;
            for meta in &vector.blocks {
                let block = self.bit_block(vector, meta).await?;
                for local in 0..meta.bit_count {
                    if block.get(local)? == select_nulls {
                        result.insert(
                            partition.footer.first_row_id + meta.first_bit as u64 + local as u64,
                        );
                    }
                }
            }
        }
        Ok(result)
    }

    fn effective_page_size(&self) -> usize {
        self.options.demand_page_size.min(self.options.cache_size)
    }

    async fn read_blocks(&self, blocks: &[BlockInfo]) -> io::Result<Vec<Vec<u8>>> {
        let _permit = self
            .context
            .file_reads
            .acquire()
            .await
            .map_err(|_| io::Error::other("FM file-read concurrency guard was closed"))?;
        read_blocks(self.reader.as_ref(), blocks, self.file_size).await
    }
}

fn rank_block(vector: &QuadVectorMeta, end: usize) -> io::Result<Option<&QuadBlockMeta>> {
    if end == 0 || end == vector.value_length || end.is_multiple_of(QUAD_BLOCK_VALUES) {
        Ok(None)
    } else {
        vector.block(end - 1).map(Some)
    }
}

fn demand_page<T>(
    blocks: &[T],
    requested: usize,
    target_bytes: usize,
    size: impl Fn(&T) -> usize,
) -> io::Result<&[T]> {
    if requested >= blocks.len() {
        return Err(invalid("FM requested block is outside its vector"));
    }
    let mut start = 0usize;
    while start < blocks.len() {
        let mut end = start;
        let mut bytes = 0usize;
        while end < blocks.len() {
            let block_bytes = size(&blocks[end]);
            if end > start && bytes.saturating_add(block_bytes) > target_bytes {
                break;
            }
            bytes = bytes.saturating_add(block_bytes);
            end += 1;
        }
        if requested < end {
            return Ok(&blocks[start..end]);
        }
        start = end;
    }
    Err(invalid("FM requested block is outside its demand pages"))
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum CacheKind {
    Quad,
    Bit,
    Int,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    namespace: Arc<str>,
    block: BlockInfo,
    kind: CacheKind,
}

impl CacheKey {
    fn new(namespace: &Arc<str>, block: BlockInfo, kind: CacheKind) -> Self {
        Self {
            namespace: Arc::clone(namespace),
            block,
            kind,
        }
    }
}

enum CacheValue {
    Quad(Arc<QuadBlock>),
    Bit(Arc<BitBlock>),
    Int(Arc<Vec<usize>>),
}

impl Clone for CacheValue {
    fn clone(&self) -> Self {
        match self {
            Self::Quad(value) => Self::Quad(Arc::clone(value)),
            Self::Bit(value) => Self::Bit(Arc::clone(value)),
            Self::Int(value) => Self::Int(Arc::clone(value)),
        }
    }
}

struct CacheEntry {
    value: CacheValue,
    retained_bytes: usize,
}

struct DecodedCache {
    budget: usize,
    retained_bytes: usize,
    entries: LruCache<CacheKey, CacheEntry>,
}

impl DecodedCache {
    fn new(budget: usize) -> Self {
        Self {
            budget,
            retained_bytes: 0,
            entries: LruCache::unbounded(),
        }
    }

    fn get(&mut self, key: &CacheKey) -> Option<CacheValue> {
        self.entries.get(key).map(|entry| entry.value.clone())
    }

    fn put(&mut self, key: CacheKey, value: CacheValue, retained_bytes: usize) {
        if retained_bytes > self.budget {
            return;
        }
        if let Some(previous) = self.entries.put(
            key,
            CacheEntry {
                value,
                retained_bytes,
            },
        ) {
            self.retained_bytes -= previous.retained_bytes;
        }
        self.retained_bytes += retained_bytes;
        while self.retained_bytes > self.budget {
            let Some((_, oldest)) = self.entries.pop_lru() else {
                break;
            };
            self.retained_bytes -= oldest.retained_bytes;
        }
    }
}
