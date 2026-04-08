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

//! BTree file footer, compatible with Java Paimon's BTreeFileFooter.
//!
//! Footer layout (52 bytes, little-endian):
//! ```text
//! | bloom_filter_offset (8) | bloom_filter_size (4) | bloom_filter_expected_entries (8) |
//! | index_block_offset  (8) | index_block_size  (4) |
//! | null_bitmap_offset  (8) | null_bitmap_size  (4) |
//! | version (4) | magic_number (4) |
//! ```

use crate::btree::block::BlockHandle;
use std::io;

pub const BTREE_MAGIC_NUMBER: u32 = 0x50425449;
pub const BTREE_CURRENT_VERSION: u32 = 1;
pub const BTREE_FOOTER_ENCODED_LENGTH: usize = 52;

/// Bloom filter handle: offset + size + expected_entries.
#[derive(Debug, Clone, Copy)]
pub struct BloomFilterHandle {
    pub offset: u64,
    pub size: u32,
    pub expected_entries: u64,
}

/// BTree file footer.
#[derive(Debug, Clone)]
pub struct BTreeFileFooter {
    pub version: u32,
    pub bloom_filter_handle: Option<BloomFilterHandle>,
    pub index_block_handle: BlockHandle,
    pub null_bitmap_handle: Option<BlockHandle>,
}

impl BTreeFileFooter {
    pub fn new(
        bloom_filter_handle: Option<BloomFilterHandle>,
        index_block_handle: BlockHandle,
        null_bitmap_handle: Option<BlockHandle>,
    ) -> Self {
        Self {
            version: BTREE_CURRENT_VERSION,
            bloom_filter_handle,
            index_block_handle,
            null_bitmap_handle,
        }
    }

    /// Serialize footer to exactly 52 bytes (little-endian, compatible with Java).
    pub fn write_footer(&self) -> [u8; BTREE_FOOTER_ENCODED_LENGTH] {
        let mut buf = [0u8; BTREE_FOOTER_ENCODED_LENGTH];
        let mut pos = 0;

        // bloom filter handle (8 + 4 + 8 = 20 bytes)
        match &self.bloom_filter_handle {
            Some(bf) => {
                buf[pos..pos + 8].copy_from_slice(&(bf.offset as i64).to_le_bytes());
                pos += 8;
                buf[pos..pos + 4].copy_from_slice(&(bf.size as i32).to_le_bytes());
                pos += 4;
                buf[pos..pos + 8].copy_from_slice(&(bf.expected_entries as i64).to_le_bytes());
                pos += 8;
            }
            None => {
                // Already zeroed
                pos += 20;
            }
        }

        // index block handle (8 + 4 = 12 bytes)
        buf[pos..pos + 8].copy_from_slice(&(self.index_block_handle.offset as i64).to_le_bytes());
        pos += 8;
        buf[pos..pos + 4].copy_from_slice(&(self.index_block_handle.size as i32).to_le_bytes());
        pos += 4;

        // null bitmap handle (8 + 4 = 12 bytes)
        match &self.null_bitmap_handle {
            Some(nb) => {
                buf[pos..pos + 8].copy_from_slice(&(nb.offset as i64).to_le_bytes());
                pos += 8;
                buf[pos..pos + 4].copy_from_slice(&(nb.size as i32).to_le_bytes());
                pos += 4;
            }
            None => {
                // Already zeroed
                pos += 12;
            }
        }

        // version (4 bytes) + magic number (4 bytes)
        buf[pos..pos + 4].copy_from_slice(&(self.version as i32).to_le_bytes());
        pos += 4;
        buf[pos..pos + 4].copy_from_slice(&(BTREE_MAGIC_NUMBER as i32).to_le_bytes());

        buf
    }

    /// Read footer from exactly 52 bytes.
    pub fn read_footer(data: &[u8]) -> io::Result<Self> {
        if data.len() < BTREE_FOOTER_ENCODED_LENGTH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Footer data too short: {} < {}",
                    data.len(),
                    BTREE_FOOTER_ENCODED_LENGTH
                ),
            ));
        }

        // Read version and magic number first (last 8 bytes)
        let version = i32::from_le_bytes([data[44], data[45], data[46], data[47]]) as u32;
        let magic = i32::from_le_bytes([data[48], data[49], data[50], data[51]]) as u32;
        if magic != BTREE_MAGIC_NUMBER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Bad magic number: expected 0x{:08X}, got 0x{:08X}",
                    BTREE_MAGIC_NUMBER, magic
                ),
            ));
        }

        let mut pos = 0;

        // bloom filter handle
        let bf_offset = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()) as u64;
        pos += 8;
        let bf_size = i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as u32;
        pos += 4;
        let bf_expected = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()) as u64;
        pos += 8;

        let bloom_filter_handle = if bf_offset == 0 && bf_size == 0 && bf_expected == 0 {
            None
        } else {
            Some(BloomFilterHandle {
                offset: bf_offset,
                size: bf_size,
                expected_entries: bf_expected,
            })
        };

        // index block handle
        let idx_offset = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()) as u64;
        pos += 8;
        let idx_size = i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as u32;
        pos += 4;
        let index_block_handle = BlockHandle::new(idx_offset, idx_size);

        // null bitmap handle
        let nb_offset = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()) as u64;
        pos += 8;
        let nb_size = i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as u32;

        let null_bitmap_handle = if nb_offset == 0 && nb_size == 0 {
            None
        } else {
            Some(BlockHandle::new(nb_offset, nb_size))
        };

        Ok(Self {
            version,
            bloom_filter_handle,
            index_block_handle,
            null_bitmap_handle,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_footer_roundtrip() {
        let footer = BTreeFileFooter::new(
            None,
            BlockHandle::new(1000, 200),
            Some(BlockHandle::new(500, 100)),
        );

        let encoded = footer.write_footer();
        assert_eq!(encoded.len(), BTREE_FOOTER_ENCODED_LENGTH);

        let decoded = BTreeFileFooter::read_footer(&encoded).unwrap();
        assert_eq!(decoded.version, BTREE_CURRENT_VERSION);
        assert!(decoded.bloom_filter_handle.is_none());
        assert_eq!(decoded.index_block_handle.offset, 1000);
        assert_eq!(decoded.index_block_handle.size, 200);
        assert!(decoded.null_bitmap_handle.is_some());
        let nb = decoded.null_bitmap_handle.unwrap();
        assert_eq!(nb.offset, 500);
        assert_eq!(nb.size, 100);
    }

    #[test]
    fn test_footer_with_bloom_filter() {
        let footer = BTreeFileFooter::new(
            Some(BloomFilterHandle {
                offset: 2000,
                size: 512,
                expected_entries: 10000,
            }),
            BlockHandle::new(3000, 300),
            None,
        );

        let encoded = footer.write_footer();
        let decoded = BTreeFileFooter::read_footer(&encoded).unwrap();

        let bf = decoded.bloom_filter_handle.unwrap();
        assert_eq!(bf.offset, 2000);
        assert_eq!(bf.size, 512);
        assert_eq!(bf.expected_entries, 10000);
        assert!(decoded.null_bitmap_handle.is_none());
    }

    #[test]
    fn test_footer_bad_magic() {
        let mut encoded = BTreeFileFooter::new(None, BlockHandle::new(0, 0), None).write_footer();
        // Corrupt magic number
        encoded[51] = 0xFF;
        assert!(BTreeFileFooter::read_footer(&encoded).is_err());
    }
}
