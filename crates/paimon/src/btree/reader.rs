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

//! BTree index reader compatible with Java Paimon's BTreeIndexReader.
//!
//! Supports:
//! - Point lookup (equal)
//! - Range queries (less than, greater than, between, etc.)
//! - Null bitmap reading
//! - IN / NOT IN queries

use crate::btree::block::{BlockHandle, BlockReader};
use crate::btree::bloom_filter::BloomFilter;
use crate::btree::footer::{BTreeFileFooter, BloomFilterHandle, BTREE_FOOTER_ENCODED_LENGTH};
use crate::btree::meta::BTreeIndexMeta;
use crate::btree::posting_list;
use crate::btree::sst_file::{read_block_from_bytes, SstFileReader};
use crate::io::FileRead;
use crate::spec::murmur_hash::hash_bytes;
use roaring::RoaringTreemap;
use std::cmp::Ordering;
use std::io;
use tokio::sync::OnceCell;

struct LazyBloomFilter {
    handle: BloomFilterHandle,
    filter: OnceCell<BloomFilter>,
}

/// BTree index reader with on-demand async data block loading.
pub struct BTreeIndexReader<F: Fn(&[u8], &[u8]) -> Ordering> {
    reader: Box<dyn FileRead>,
    sst_reader: SstFileReader,
    null_bitmap: RoaringTreemap,
    min_key: Option<Vec<u8>>,
    max_key: Option<Vec<u8>>,
    key_comparator: F,
    bloom_filter: Option<LazyBloomFilter>,
    file_version: u32,
}

impl<F: Fn(&[u8], &[u8]) -> Ordering> BTreeIndexReader<F> {
    /// Open a BTree index reader from a FileRead and file metadata.
    /// Only reads footer, index block, and null bitmap on open.
    /// Data blocks are read on demand during queries.
    pub async fn open(
        reader: Box<dyn FileRead>,
        file_size: u64,
        meta: &BTreeIndexMeta,
        key_comparator: F,
    ) -> io::Result<Self> {
        if file_size < BTREE_FOOTER_ENCODED_LENGTH as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "File too small for BTree footer",
            ));
        }

        // 1. Read footer (last 52 bytes)
        let footer_start = file_size - BTREE_FOOTER_ENCODED_LENGTH as u64;
        let footer_bytes = reader
            .read(footer_start..file_size)
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
        let footer = BTreeFileFooter::read_footer(&footer_bytes)?;
        let bloom_filter = footer.bloom_filter_handle.map(|handle| LazyBloomFilter {
            handle,
            filter: OnceCell::new(),
        });

        // 2. Read index block
        let idx = &footer.index_block_handle;
        let idx_end = idx.offset + idx.full_block_size() as u64;
        let index_bytes = reader
            .read(idx.offset..idx_end)
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
        let index_block = read_block_from_bytes(&index_bytes, idx.size)?;
        let sst_reader = SstFileReader::from_index_block(index_block);

        // 3. Read null bitmap
        let null_bitmap = match &footer.null_bitmap_handle {
            Some(h) => read_null_bitmap(reader.as_ref(), h).await?,
            None => RoaringTreemap::new(),
        };

        Ok(Self {
            reader,
            sst_reader,
            null_bitmap,
            min_key: meta.first_key.clone(),
            max_key: meta.last_key.clone(),
            key_comparator,
            bloom_filter,
            file_version: footer.version,
        })
    }

    /// Get the null bitmap (row ids of null keys).
    pub fn null_bitmap(&self) -> &RoaringTreemap {
        &self.null_bitmap
    }

    /// Collect all non-null row ids into a bitmap.
    pub async fn all_non_null_rows(&self) -> io::Result<RoaringTreemap> {
        if self.min_key.is_none() {
            return Ok(RoaringTreemap::new());
        }
        self.range_query(
            self.min_key.as_deref().unwrap(),
            self.max_key.as_deref().unwrap(),
            true,
            true,
        )
        .await
    }

    /// Scan all non-null entries and collect row ids for keys matching `predicate`.
    pub async fn scan_entries(
        &self,
        predicate: impl Fn(&[u8]) -> bool + Send + Sync,
    ) -> io::Result<RoaringTreemap> {
        let Some(min_key) = self.min_key.as_deref() else {
            return Ok(RoaringTreemap::new());
        };

        let cmp = &self.key_comparator;
        let index_block = self.sst_reader.index_block();
        let (_, mut index_iter) = index_block.seek_and_iter(min_key, cmp);
        let mut result = RoaringTreemap::new();

        while let Some((_key, handle_bytes)) = index_iter.next() {
            let handle = BlockHandle::decode(handle_bytes)?;
            let block = self.read_data_block(&handle).await?;
            let mut offset = 0;
            while offset < block.data.len() {
                let (key, value, next_offset) = block.read_entry_at(offset);
                offset = next_offset;
                if predicate(key) {
                    posting_list::add_to(value, self.file_version, &mut result)?;
                }
            }
        }

        Ok(result)
    }

    /// Range query: returns a bitmap of all row ids whose keys fall in [from, to]
    /// with configurable inclusivity. Reads data blocks on demand.
    pub async fn range_query(
        &self,
        from: &[u8],
        to: &[u8],
        from_inclusive: bool,
        to_inclusive: bool,
    ) -> io::Result<RoaringTreemap> {
        let cmp = &self.key_comparator;
        let mut result = RoaringTreemap::new();

        match cmp(from, to) {
            Ordering::Greater => return Ok(result),
            Ordering::Equal if !from_inclusive || !to_inclusive => return Ok(result),
            _ => {}
        }

        // Seek in index block to find the first data block that may contain `from`
        let index_block = self.sst_reader.index_block();
        let (_, mut index_iter) = index_block.seek_and_iter(from, cmp);

        // First data block: seek within it
        let first_block = match index_iter.next() {
            Some((_key, handle_bytes)) => {
                let handle = BlockHandle::decode(handle_bytes)?;
                self.read_data_block(&handle).await?
            }
            None => return Ok(result),
        };

        let (_, seeked) = first_block.seek_and_iter(from, cmp);
        let mut offset = seeked.offset;

        // Iterate first block from seeked position
        if self.scan_block(
            &first_block,
            &mut offset,
            from,
            to,
            from_inclusive,
            to_inclusive,
            &mut result,
        )? {
            return Ok(result);
        }

        // Continue with subsequent data blocks
        while let Some((_key, handle_bytes)) = index_iter.next() {
            let handle = BlockHandle::decode(handle_bytes)?;
            let block = self.read_data_block(&handle).await?;
            let mut block_offset = 0;
            if self.scan_block(
                &block,
                &mut block_offset,
                from,
                to,
                from_inclusive,
                to_inclusive,
                &mut result,
            )? {
                return Ok(result);
            }
        }

        Ok(result)
    }

    /// Scan entries in a block, inserting matching row ids into result.
    /// Returns true if we've passed the upper bound (done).
    #[allow(clippy::too_many_arguments)]
    fn scan_block(
        &self,
        block: &BlockReader,
        offset: &mut usize,
        from: &[u8],
        to: &[u8],
        from_inclusive: bool,
        to_inclusive: bool,
        result: &mut RoaringTreemap,
    ) -> io::Result<bool> {
        let cmp = &self.key_comparator;
        while *offset < block.data.len() {
            let (key, value, next_offset) = block.read_entry_at(*offset);
            *offset = next_offset;

            if !from_inclusive && cmp(key, from) == Ordering::Equal {
                continue;
            }

            let diff = cmp(key, to);
            if diff == Ordering::Greater || (!to_inclusive && diff == Ordering::Equal) {
                return Ok(true);
            }

            posting_list::add_to(value, self.file_version, result)?;
        }
        Ok(false)
    }

    /// Read a data block from the file on demand.
    async fn read_data_block(&self, handle: &BlockHandle) -> io::Result<BlockReader> {
        let end = handle.offset + handle.full_block_size() as u64;
        let bytes = self
            .reader
            .read(handle.offset..end)
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
        read_block_from_bytes(&bytes, handle.size)
    }

    async fn bloom_might_contain(&self, key: &[u8]) -> io::Result<bool> {
        let Some(lazy) = &self.bloom_filter else {
            return Ok(true);
        };
        let filter = lazy
            .filter
            .get_or_try_init(|| async {
                if lazy.handle.size == 0 || lazy.handle.expected_entries == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "BTree Bloom filter handle must have positive size and expected entries",
                    ));
                }
                let end = lazy
                    .handle
                    .offset
                    .checked_add(u64::from(lazy.handle.size))
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "BTree Bloom filter range overflows",
                        )
                    })?;
                let bits = self
                    .reader
                    .read(lazy.handle.offset..end)
                    .await
                    .map_err(|error| io::Error::other(error.to_string()))?;
                if bits.len() != lazy.handle.size as usize {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!(
                            "BTree Bloom filter expected {} bytes, read {}",
                            lazy.handle.size,
                            bits.len()
                        ),
                    ));
                }
                BloomFilter::from_bytes(lazy.handle.expected_entries, bits)
            })
            .await?;
        Ok(filter.test_hash(hash_bytes(key)))
    }

    /// Equal query: returns row ids for the given key.
    pub async fn query_equal(&self, key: &[u8]) -> io::Result<RoaringTreemap> {
        let cmp = &self.key_comparator;
        if self
            .min_key
            .as_deref()
            .is_none_or(|min| cmp(key, min) == Ordering::Less)
            || self
                .max_key
                .as_deref()
                .is_none_or(|max| cmp(key, max) == Ordering::Greater)
        {
            return Ok(RoaringTreemap::new());
        }
        if !self.bloom_might_contain(key).await? {
            return Ok(RoaringTreemap::new());
        }

        let index_block = self.sst_reader.index_block();
        let (_, mut index_iter) = index_block.seek_and_iter(key, cmp);
        let Some((_last_key, handle_bytes)) = index_iter.next() else {
            return Ok(RoaringTreemap::new());
        };
        let handle = BlockHandle::decode(handle_bytes)?;
        let block = self.read_data_block(&handle).await?;
        let (found, mut entry_iter) = block.seek_and_iter(key, cmp);
        let mut result = RoaringTreemap::new();
        if let (true, Some((_entry_key, value))) = (found, entry_iter.next()) {
            posting_list::add_to(value, self.file_version, &mut result)?;
        }
        Ok(result)
    }

    /// Less than query.
    pub async fn query_less_than(&self, key: &[u8]) -> io::Result<RoaringTreemap> {
        match &self.min_key {
            Some(min) => self.range_query(min, key, true, false).await,
            None => Ok(RoaringTreemap::new()),
        }
    }

    /// Less or equal query.
    pub async fn query_less_or_equal(&self, key: &[u8]) -> io::Result<RoaringTreemap> {
        match &self.min_key {
            Some(min) => self.range_query(min, key, true, true).await,
            None => Ok(RoaringTreemap::new()),
        }
    }

    /// Greater than query.
    pub async fn query_greater_than(&self, key: &[u8]) -> io::Result<RoaringTreemap> {
        match &self.max_key {
            Some(max) => self.range_query(key, max, false, true).await,
            None => Ok(RoaringTreemap::new()),
        }
    }

    /// Greater or equal query.
    pub async fn query_greater_or_equal(&self, key: &[u8]) -> io::Result<RoaringTreemap> {
        match &self.max_key {
            Some(max) => self.range_query(key, max, true, true).await,
            None => Ok(RoaringTreemap::new()),
        }
    }

    pub async fn query_prefix(&self, prefix: &[u8]) -> io::Result<RoaringTreemap> {
        match Self::prefix_successor(prefix) {
            Some(upper) => self.range_query(prefix, &upper, true, false).await,
            None => self.query_greater_or_equal(prefix).await,
        }
    }

    fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
        let mut bound = prefix.to_vec();
        while let Some(&last) = bound.last() {
            if last != 0xFF {
                *bound.last_mut().unwrap() = last + 1;
                return Some(bound);
            }
            bound.pop();
        }
        None
    }

    /// Between query (inclusive on both ends).
    pub async fn query_between(&self, from: &[u8], to: &[u8]) -> io::Result<RoaringTreemap> {
        self.range_query(from, to, true, true).await
    }

    /// IN query: group sorted target keys by data block, then seek each key within its block.
    /// This reads every target block at most once and skips unrelated blocks and entries between
    /// sparse target keys.
    pub async fn query_in(&self, keys: &[&[u8]]) -> io::Result<RoaringTreemap> {
        if keys.is_empty() {
            return Ok(RoaringTreemap::new());
        }

        let cmp = &self.key_comparator;
        let (Some(min_key), Some(max_key)) = (self.min_key.as_deref(), self.max_key.as_deref())
        else {
            return Ok(RoaringTreemap::new());
        };

        // Sort, deduplicate, and discard keys outside this file's bounds before resolving blocks.
        let mut sorted_keys: Vec<&[u8]> = keys.to_vec();
        sorted_keys.sort_by(|a, b| cmp(a, b));
        sorted_keys.dedup_by(|a, b| cmp(a, b) == Ordering::Equal);
        sorted_keys.retain(|key| {
            cmp(key, min_key) != Ordering::Less && cmp(key, max_key) != Ordering::Greater
        });
        if sorted_keys.is_empty() {
            return Ok(RoaringTreemap::new());
        }

        let mut bloom_matches = Vec::with_capacity(sorted_keys.len());
        for key in sorted_keys {
            if self.bloom_might_contain(key).await? {
                bloom_matches.push(key);
            }
        }
        if bloom_matches.is_empty() {
            return Ok(RoaringTreemap::new());
        }

        // The index block is already resident in memory. Resolve every target key to its first
        // possible data block and coalesce adjacent targets that share the same block handle.
        let index_block = self.sst_reader.index_block();
        let mut target_blocks: Vec<(BlockHandle, Vec<&[u8]>)> = Vec::new();
        for key in bloom_matches {
            let (_, mut index_iter) = index_block.seek_and_iter(key, cmp);
            let Some((_last_key, handle_bytes)) = index_iter.next() else {
                break;
            };
            let handle = BlockHandle::decode(handle_bytes)?;

            match target_blocks.last_mut() {
                Some((current, block_keys))
                    if current.offset == handle.offset && current.size == handle.size =>
                {
                    block_keys.push(key);
                }
                _ => target_blocks.push((handle, vec![key])),
            }
        }

        let mut result = RoaringTreemap::new();
        for (handle, block_keys) in target_blocks {
            let block = self.read_data_block(&handle).await?;
            for key in block_keys {
                let (found, mut entry_iter) = block.seek_and_iter(key, cmp);
                if let (true, Some((_entry_key, value))) = (found, entry_iter.next()) {
                    posting_list::add_to(value, self.file_version, &mut result)?;
                }
            }
        }

        Ok(result)
    }

    /// Not equal query.
    pub async fn query_not_equal(&self, key: &[u8]) -> io::Result<RoaringTreemap> {
        let mut result = self.all_non_null_rows().await?;
        let equal = self.query_equal(key).await?;
        result -= equal;
        Ok(result)
    }
}

/// Read null bitmap from a FileRead at the given handle.
async fn read_null_bitmap(
    reader: &dyn FileRead,
    handle: &BlockHandle,
) -> io::Result<RoaringTreemap> {
    let offset = handle.offset;
    let size = handle.size as u64;
    // Read bitmap bytes + CRC (4 bytes)
    let bytes = reader
        .read(offset..offset + size + 4)
        .await
        .map_err(|e| io::Error::other(e.to_string()))?;
    let bitmap_bytes = &bytes[..size as usize];
    let crc_bytes = &bytes[size as usize..];

    verify_null_bitmap_crc(bitmap_bytes, crc_bytes)?;

    RoaringTreemap::deserialize_from(bitmap_bytes)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn verify_null_bitmap_crc(bitmap_bytes: &[u8], crc_bytes: &[u8]) -> io::Result<()> {
    let expected_crc = u32::from_le_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(bitmap_bytes);
    let actual_crc = hasher.finalize();
    if actual_crc != expected_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Null bitmap CRC mismatch: expected 0x{:08X}, got 0x{:08X}",
                expected_crc, actual_crc
            ),
        ));
    }
    Ok(())
}
