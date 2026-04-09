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
use crate::btree::footer::{BTreeFileFooter, BTREE_FOOTER_ENCODED_LENGTH};
use crate::btree::meta::BTreeIndexMeta;
use crate::btree::sst_file::{read_block_from_bytes, SstFileReader};
use crate::btree::var_len::{decode_var_int, decode_var_long};
use crate::io::FileRead;
use roaring::RoaringTreemap;
use std::cmp::Ordering;
use std::io::{self, Cursor};

/// BTree index reader with on-demand async data block loading.
pub struct BTreeIndexReader<F: Fn(&[u8], &[u8]) -> Ordering> {
    reader: Box<dyn FileRead>,
    sst_reader: SstFileReader,
    null_bitmap: RoaringTreemap,
    min_key: Option<Vec<u8>>,
    max_key: Option<Vec<u8>>,
    key_comparator: F,
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

            insert_row_ids_into(value, result)?;
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

    /// Equal query: returns row ids for the given key.
    pub async fn query_equal(&self, key: &[u8]) -> io::Result<RoaringTreemap> {
        self.range_query(key, key, true, true).await
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

    /// Between query (inclusive on both ends).
    pub async fn query_between(&self, from: &[u8], to: &[u8]) -> io::Result<RoaringTreemap> {
        self.range_query(from, to, true, true).await
    }

    /// In query: sort keys and do a single sequential scan (merge-join style).
    pub async fn query_in(&self, keys: &[&[u8]]) -> io::Result<RoaringTreemap> {
        if keys.is_empty() {
            return Ok(RoaringTreemap::new());
        }

        let cmp = &self.key_comparator;

        // Sort query keys
        let mut sorted_keys: Vec<&[u8]> = keys.to_vec();
        sorted_keys.sort_by(|a, b| cmp(a, b));
        sorted_keys.dedup_by(|a, b| cmp(a, b) == Ordering::Equal);

        let mut result = RoaringTreemap::new();
        let mut key_idx = 0;

        // Seek in index block to the first data block
        let index_block = self.sst_reader.index_block();
        let (_, mut index_iter) = index_block.seek_and_iter(sorted_keys[0], cmp);

        // First block: seek within
        let first_block = match index_iter.next() {
            Some((_key, handle_bytes)) => {
                let handle = BlockHandle::decode(handle_bytes)?;
                self.read_data_block(&handle).await?
            }
            None => return Ok(result),
        };

        let (_, seeked) = first_block.seek_and_iter(sorted_keys[0], cmp);
        let mut offset = seeked.offset;

        if self.scan_block_in(
            &first_block,
            &mut offset,
            &sorted_keys,
            &mut key_idx,
            &mut result,
        )? {
            return Ok(result);
        }

        while let Some((_key, handle_bytes)) = index_iter.next() {
            let handle = BlockHandle::decode(handle_bytes)?;
            let block = self.read_data_block(&handle).await?;
            let mut block_offset = 0;
            if self.scan_block_in(
                &block,
                &mut block_offset,
                &sorted_keys,
                &mut key_idx,
                &mut result,
            )? {
                return Ok(result);
            }
        }

        Ok(result)
    }

    /// Scan a block for IN query. Returns true when all keys are consumed.
    fn scan_block_in(
        &self,
        block: &BlockReader,
        offset: &mut usize,
        sorted_keys: &[&[u8]],
        key_idx: &mut usize,
        result: &mut RoaringTreemap,
    ) -> io::Result<bool> {
        let cmp = &self.key_comparator;
        while *offset < block.data.len() {
            let (entry_key, value, next_offset) = block.read_entry_at(*offset);
            *offset = next_offset;

            // Advance key_idx past keys smaller than current entry
            while *key_idx < sorted_keys.len()
                && cmp(sorted_keys[*key_idx], entry_key) == Ordering::Less
            {
                *key_idx += 1;
            }

            if *key_idx >= sorted_keys.len() {
                return Ok(true);
            }

            // Past the last query key
            if cmp(entry_key, sorted_keys[sorted_keys.len() - 1]) == Ordering::Greater {
                return Ok(true);
            }

            if cmp(entry_key, sorted_keys[*key_idx]) == Ordering::Equal {
                insert_row_ids_into(value, result)?;
            }
        }
        Ok(false)
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

/// Deserialize row ids from value bytes and insert directly into bitmap.
fn insert_row_ids_into(data: &[u8], bitmap: &mut RoaringTreemap) -> io::Result<()> {
    let mut cursor = Cursor::new(data);
    let count = decode_var_int(&mut cursor)?;
    if count < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Invalid row id count: {count}"),
        ));
    }
    for _ in 0..count {
        bitmap.insert(decode_var_long(&mut cursor)? as u64);
    }
    Ok(())
}
