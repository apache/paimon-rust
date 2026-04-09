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

//! SST block infrastructure compatible with Java Paimon's sst package.
//!
//! Block layout:
//! ```text
//!     +---------------+
//!     | Block Trailer |  (5 bytes: 1 byte compression type + 4 bytes CRC32)
//!     +---------------+
//!     |  Block Data   |
//!     +---------------+--------------------------------+----+
//!     | key len | key bytes | value len | value bytes  |    |
//!     +------------------------------------------------+    |
//!     | key len | key bytes | value len | value bytes  |    +-> Key-Value pairs
//!     +------------------------------------------------+    |
//!     |                  ... ...                       |    |
//!     +------------------------------------------------+----+
//!     | entry pos | entry pos |     ...    | entry pos |    +-> optional, for unaligned block
//!     +------------------------------------------------+----+
//!     |   entry num  /  entry size   |   aligned type  |
//!     +------------------------------------------------+
//! ```

use crate::btree::var_len::{
    decode_var_int, decode_var_int_from_slice, encode_var_int, encode_var_int_to_slice,
};
use std::cmp::Ordering;
use std::io::{self, Cursor, Read, Write};

/// Block compression type, compatible with Java's BlockCompressionType.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BlockCompressionType {
    None = 0,
    Zstd = 1,
    Lz4 = 2,
    Lzo = 3,
}

impl BlockCompressionType {
    pub fn from_persistent_id(id: u8) -> io::Result<Self> {
        match id {
            0 => Ok(Self::None),
            1 => Ok(Self::Zstd),
            2 => Ok(Self::Lz4),
            3 => Ok(Self::Lzo),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unknown compression type: {id}"),
            )),
        }
    }
}

/// Block aligned type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BlockAlignedType {
    Aligned = 0,
    Unaligned = 1,
}

impl BlockAlignedType {
    pub fn from_byte(b: u8) -> io::Result<Self> {
        match b {
            0 => Ok(Self::Aligned),
            1 => Ok(Self::Unaligned),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unknown aligned type: {b}"),
            )),
        }
    }
}

/// Block trailer: compression type (1 byte) + CRC32 (4 bytes) = 5 bytes.
pub const BLOCK_TRAILER_LENGTH: usize = 5;

pub struct BlockTrailer {
    pub compression_type: BlockCompressionType,
    pub crc32c: u32,
}

impl BlockTrailer {
    #[allow(dead_code)]
    pub fn write_to(&self, out: &mut impl Write) -> io::Result<()> {
        out.write_all(&[self.compression_type as u8])?;
        out.write_all(&(self.crc32c as i32).to_le_bytes())?;
        Ok(())
    }

    /// Encode trailer to a fixed-size stack array.
    pub fn to_bytes(&self) -> [u8; BLOCK_TRAILER_LENGTH] {
        let crc_bytes = (self.crc32c as i32).to_le_bytes();
        [
            self.compression_type as u8,
            crc_bytes[0],
            crc_bytes[1],
            crc_bytes[2],
            crc_bytes[3],
        ]
    }

    pub fn read_from(data: &[u8]) -> io::Result<Self> {
        if data.len() < BLOCK_TRAILER_LENGTH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Block trailer too short",
            ));
        }
        let compression_type = BlockCompressionType::from_persistent_id(data[0])?;
        let crc32c = u32::from_le_bytes([data[1], data[2], data[3], data[4]]);
        Ok(Self {
            compression_type,
            crc32c,
        })
    }
}

/// Compute CRC32 of block data + compression type byte (compatible with Java).
pub fn compute_crc32(data: &[u8], compression_type: BlockCompressionType) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    hasher.update(&[compression_type as u8]);
    hasher.finalize()
}

/// BlockHandle: offset + size of a block in the file.
pub const BLOCK_HANDLE_MAX_ENCODED_LENGTH: usize = 14; // 9 (var long) + 5 (var int)

#[derive(Debug, Clone, Copy, Default)]
pub struct BlockHandle {
    pub offset: u64,
    pub size: u32,
}

impl BlockHandle {
    pub fn new(offset: u64, size: u32) -> Self {
        Self { offset, size }
    }

    pub fn full_block_size(&self) -> u32 {
        self.size + BLOCK_TRAILER_LENGTH as u32
    }

    pub fn write_to(&self, out: &mut impl Write) -> io::Result<()> {
        crate::btree::var_len::encode_var_long(out, self.offset as i64)?;
        encode_var_int(out, self.size as i32)?;
        Ok(())
    }

    pub fn read_from(input: &mut impl Read) -> io::Result<Self> {
        let offset = crate::btree::var_len::decode_var_long(input)? as u64;
        let size = decode_var_int(input)? as u32;
        Ok(Self { offset, size })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(BLOCK_HANDLE_MAX_ENCODED_LENGTH);
        self.write_to(&mut buf).unwrap();
        buf
    }

    /// Encode into a stack buffer, returning (buffer, length).
    pub fn encode_to_buf(&self) -> ([u8; BLOCK_HANDLE_MAX_ENCODED_LENGTH], usize) {
        let mut buf = [0u8; BLOCK_HANDLE_MAX_ENCODED_LENGTH];
        let mut cursor = io::Cursor::new(&mut buf[..]);
        self.write_to(&mut cursor).unwrap();
        let len = cursor.position() as usize;
        (buf, len)
    }

    pub fn decode(data: &[u8]) -> io::Result<Self> {
        Self::read_from(&mut Cursor::new(data))
    }
}

/// BlockWriter: builds a block of key-value pairs.
pub struct BlockWriter {
    data: Vec<u8>,
    positions: Vec<i32>,
    aligned_size: i32,
    aligned: bool,
}

impl BlockWriter {
    pub fn new(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            positions: Vec::with_capacity(32),
            aligned_size: 0,
            aligned: true,
        }
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        let start = self.data.len() as i32;

        // key_len (var int) + key + value_len (var int) + value
        let mut var_buf = [0u8; 5];
        let n = encode_var_int_to_slice(&mut var_buf, 0, key.len() as i32);
        self.data.extend_from_slice(&var_buf[..n]);
        self.data.extend_from_slice(key);

        let n = encode_var_int_to_slice(&mut var_buf, 0, value.len() as i32);
        self.data.extend_from_slice(&var_buf[..n]);
        self.data.extend_from_slice(value);

        let end = self.data.len() as i32;
        self.positions.push(start);

        if self.aligned {
            let current_size = end - start;
            if self.aligned_size == 0 {
                self.aligned_size = current_size;
            } else {
                self.aligned = self.aligned_size == current_size;
            }
        }
    }

    pub fn entry_count(&self) -> usize {
        self.positions.len()
    }

    /// Estimated memory usage of the block.
    pub fn memory(&self) -> usize {
        let mut mem = self.data.len() + 5;
        if !self.aligned {
            mem += self.positions.len() * 4;
        }
        mem
    }

    /// Finish building the block, returning the raw block bytes.
    pub fn finish(&mut self) -> Vec<u8> {
        if self.positions.is_empty() {
            self.aligned = false;
        }

        if self.aligned {
            self.data
                .extend_from_slice(&(self.aligned_size).to_le_bytes());
        } else {
            for &pos in &self.positions {
                self.data.extend_from_slice(&pos.to_le_bytes());
            }
            self.data
                .extend_from_slice(&(self.positions.len() as i32).to_le_bytes());
        }
        self.data.push(if self.aligned {
            BlockAlignedType::Aligned as u8
        } else {
            BlockAlignedType::Unaligned as u8
        });

        let result = std::mem::take(&mut self.data);
        self.reset();
        result
    }

    pub fn reset(&mut self) {
        self.data.clear();
        self.positions.clear();
        self.aligned_size = 0;
        self.aligned = true;
    }
}

/// BlockReader: reads a block and supports binary search.
pub struct BlockReader {
    /// The raw data portion (key-value pairs only, no index/footer).
    pub(crate) data: Vec<u8>,
    record_count: usize,
    /// For aligned blocks: record_size; for unaligned: index offsets.
    seek_info: SeekInfo,
}

enum SeekInfo {
    Aligned { record_size: usize },
    Unaligned { offsets: Vec<i32> },
}

impl BlockReader {
    /// Create a BlockReader from raw (decompressed) block bytes (borrows and copies).
    #[allow(dead_code)]
    pub fn create(block: &[u8]) -> io::Result<Self> {
        Self::create_from_vec(block.to_vec())
    }

    /// Create a BlockReader taking ownership of the decompressed block bytes (zero-copy).
    pub fn create_from_vec(mut block: Vec<u8>) -> io::Result<Self> {
        if block.is_empty() {
            return Ok(Self {
                data: Vec::new(),
                record_count: 0,
                seek_info: SeekInfo::Unaligned {
                    offsets: Vec::new(),
                },
            });
        }

        let aligned_type = BlockAlignedType::from_byte(block[block.len() - 1])?;
        let int_value = i32::from_le_bytes([
            block[block.len() - 5],
            block[block.len() - 4],
            block[block.len() - 3],
            block[block.len() - 2],
        ]);

        match aligned_type {
            BlockAlignedType::Aligned => {
                let record_size = int_value as usize;
                let data_len = block.len() - 5;
                let record_count = if record_size > 0 {
                    data_len / record_size
                } else {
                    0
                };
                block.truncate(data_len);
                Ok(Self {
                    data: block,
                    record_count,
                    seek_info: SeekInfo::Aligned { record_size },
                })
            }
            BlockAlignedType::Unaligned => {
                let num_entries = int_value as usize;
                let index_len = num_entries * 4;
                let data_end = block.len() - 5 - index_len;
                let index_start = data_end;

                let mut offsets = Vec::with_capacity(num_entries);
                for i in 0..num_entries {
                    let pos = index_start + i * 4;
                    let off = i32::from_le_bytes([
                        block[pos],
                        block[pos + 1],
                        block[pos + 2],
                        block[pos + 3],
                    ]);
                    offsets.push(off);
                }

                block.truncate(data_end);
                Ok(Self {
                    data: block,
                    record_count: num_entries,
                    seek_info: SeekInfo::Unaligned { offsets },
                })
            }
        }
    }

    #[allow(dead_code)]
    pub fn record_count(&self) -> usize {
        self.record_count
    }

    fn seek_to_position(&self, record_pos: usize) -> usize {
        match &self.seek_info {
            SeekInfo::Aligned { record_size } => record_pos * record_size,
            SeekInfo::Unaligned { offsets } => offsets[record_pos] as usize,
        }
    }

    /// Read only the key at the given byte offset, skipping value parsing.
    /// Returns (key_slice, next_entry_offset).
    fn read_key_at(&self, offset: usize) -> (&[u8], usize) {
        let (key_len, consumed) = decode_var_int_from_slice(&self.data, offset);
        let key_start = offset + consumed;
        let key = &self.data[key_start..key_start + key_len as usize];

        let val_offset = key_start + key_len as usize;
        let (val_len, consumed2) = decode_var_int_from_slice(&self.data, val_offset);
        let next_offset = val_offset + consumed2 + val_len as usize;

        (key, next_offset)
    }

    /// Read a key-value entry at the given byte offset, returning (key, value, next_offset).
    /// Returns slices into the block's data buffer (zero-copy).
    pub(crate) fn read_entry_at(&self, offset: usize) -> (&[u8], &[u8], usize) {
        let (key_len, consumed) = decode_var_int_from_slice(&self.data, offset);
        let key_start = offset + consumed;
        let key = &self.data[key_start..key_start + key_len as usize];

        let val_offset = key_start + key_len as usize;
        let (val_len, consumed2) = decode_var_int_from_slice(&self.data, val_offset);
        let val_start = val_offset + consumed2;
        let value = &self.data[val_start..val_start + val_len as usize];

        (key, value, val_start + val_len as usize)
    }

    /// Create a sequential iterator over all entries.
    #[cfg(test)]
    pub fn iter(&self) -> BlockIter<'_> {
        BlockIter {
            reader: self,
            offset: 0,
            index: 0,
        }
    }

    /// Binary search for the given target key. Returns an iterator positioned at the
    /// first entry whose key >= target_key.
    /// The comparator compares two key byte slices.
    pub fn seek_and_iter<F>(&self, target_key: &[u8], cmp: &F) -> (bool, BlockIter<'_>)
    where
        F: Fn(&[u8], &[u8]) -> Ordering,
    {
        let mut left: i32 = 0;
        let mut right: i32 = self.record_count as i32 - 1;
        let mut found = false;
        let mut best_index: Option<usize> = None;
        let mut best_offset: Option<usize> = None;

        while left <= right {
            let mid = left + (right - left) / 2;
            let byte_offset = self.seek_to_position(mid as usize);
            let (key, _next_offset) = self.read_key_at(byte_offset);

            match cmp(key, target_key) {
                Ordering::Equal => {
                    found = true;
                    best_index = Some(mid as usize);
                    best_offset = Some(byte_offset);
                    break;
                }
                Ordering::Greater => {
                    best_index = Some(mid as usize);
                    best_offset = Some(byte_offset);
                    right = mid - 1;
                }
                Ordering::Less => {
                    left = mid + 1;
                }
            }
        }

        match (best_index, best_offset) {
            (Some(idx), Some(off)) => (
                found,
                BlockIter {
                    reader: self,
                    offset: off,
                    index: idx,
                },
            ),
            _ => (
                false,
                BlockIter {
                    reader: self,
                    offset: self.data.len(),
                    index: self.record_count,
                },
            ),
        }
    }
}

/// Iterator over block entries.
pub struct BlockIter<'a> {
    reader: &'a BlockReader,
    pub(crate) offset: usize,
    index: usize,
}

impl<'a> BlockIter<'a> {
    pub fn has_next(&self) -> bool {
        self.index < self.reader.record_count && self.offset < self.reader.data.len()
    }

    /// Returns (key, value) as borrowed slices (zero-copy).
    pub fn next(&mut self) -> Option<(&'a [u8], &'a [u8])> {
        if !self.has_next() {
            return None;
        }
        let (key, value, next_offset) = self.reader.read_entry_at(self.offset);
        self.offset = next_offset;
        self.index += 1;
        Some((key, value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_roundtrip_aligned() {
        let mut writer = BlockWriter::new(1024);
        // All entries same size -> aligned
        writer.add(b"aa", b"11");
        writer.add(b"bb", b"22");
        writer.add(b"cc", b"33");

        let block = writer.finish();
        let reader = BlockReader::create(&block).unwrap();
        assert_eq!(reader.record_count(), 3);

        let entries: Vec<_> = {
            let mut iter = reader.iter();
            let mut v = Vec::new();
            while let Some(e) = iter.next() {
                v.push(e);
            }
            v
        };
        assert_eq!(entries[0], (&b"aa"[..], &b"11"[..]));
        assert_eq!(entries[1], (&b"bb"[..], &b"22"[..]));
        assert_eq!(entries[2], (&b"cc"[..], &b"33"[..]));
    }

    #[test]
    fn test_block_roundtrip_unaligned() {
        let mut writer = BlockWriter::new(1024);
        writer.add(b"a", b"1");
        writer.add(b"bb", b"22");
        writer.add(b"ccc", b"333");

        let block = writer.finish();
        let reader = BlockReader::create(&block).unwrap();
        assert_eq!(reader.record_count(), 3);

        let entries: Vec<_> = {
            let mut iter = reader.iter();
            let mut v = Vec::new();
            while let Some(e) = iter.next() {
                v.push(e);
            }
            v
        };
        assert_eq!(entries[0], (&b"a"[..], &b"1"[..]));
        assert_eq!(entries[1], (&b"bb"[..], &b"22"[..]));
        assert_eq!(entries[2], (&b"ccc"[..], &b"333"[..]));
    }

    #[test]
    fn test_block_seek() {
        let mut writer = BlockWriter::new(1024);
        writer.add(b"apple", b"1");
        writer.add(b"banana", b"2");
        writer.add(b"cherry", b"3");
        writer.add(b"date", b"4");

        let block = writer.finish();
        let reader = BlockReader::create(&block).unwrap();

        let cmp = |a: &[u8], b: &[u8]| a.cmp(b);

        // Exact match
        let (found, mut iter) = reader.seek_and_iter(b"banana", &cmp);
        assert!(found);
        let (k, v) = iter.next().unwrap();
        assert_eq!(k, b"banana");
        assert_eq!(v, b"2");

        // Seek to position >= "bz" -> should land on "cherry"
        let (found, mut iter) = reader.seek_and_iter(b"bz", &cmp);
        assert!(!found);
        let (k, _) = iter.next().unwrap();
        assert_eq!(k, b"cherry");

        // Seek past all entries
        let (found, iter) = reader.seek_and_iter(b"zzz", &cmp);
        assert!(!found);
        assert!(!iter.has_next());
    }

    #[test]
    fn test_block_handle_roundtrip() {
        let handle = BlockHandle::new(12345, 6789);
        let encoded = handle.encode();
        let decoded = BlockHandle::decode(&encoded).unwrap();
        assert_eq!(decoded.offset, 12345);
        assert_eq!(decoded.size, 6789);
    }

    #[test]
    fn test_block_trailer_roundtrip() {
        let trailer = BlockTrailer {
            compression_type: BlockCompressionType::None,
            crc32c: 0xDEADBEEF,
        };
        let mut buf = Vec::new();
        trailer.write_to(&mut buf).unwrap();
        assert_eq!(buf.len(), BLOCK_TRAILER_LENGTH);
        let decoded = BlockTrailer::read_from(&buf).unwrap();
        assert_eq!(decoded.compression_type, BlockCompressionType::None);
        assert_eq!(decoded.crc32c, 0xDEADBEEF);
    }

    #[test]
    fn test_crc32_compatible() {
        let data = b"hello world";
        let crc = compute_crc32(data, BlockCompressionType::None);
        // Just verify it's deterministic
        let crc2 = compute_crc32(data, BlockCompressionType::None);
        assert_eq!(crc, crc2);
    }
}
