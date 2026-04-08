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

//! SST file writer and reader compatible with Java Paimon's SstFileWriter/SstFileReader.

use crate::btree::block::{
    compute_crc32, BlockCompressionType, BlockHandle, BlockReader, BlockTrailer, BlockWriter,
    BLOCK_HANDLE_MAX_ENCODED_LENGTH, BLOCK_TRAILER_LENGTH,
};
use crate::btree::var_len::encode_var_int_to_slice;
use std::borrow::Cow;
use std::io::{self, Cursor};

/// SstFileWriter writes sorted key-value pairs into an SST file format.
///
/// The file consists of:
/// - Multiple data blocks (each containing sorted key-value pairs)
/// - An index block (mapping last-key-of-block -> block handle)
/// - Optional bloom filter
pub struct SstFileWriter {
    out: Vec<u8>,
    block_size: usize,
    data_block_writer: BlockWriter,
    index_block_writer: BlockWriter,
    compression_type: BlockCompressionType,
    last_key: Option<Vec<u8>>,
    record_count: u64,
}

impl SstFileWriter {
    pub fn new(block_size: usize, compression_type: BlockCompressionType) -> Self {
        Self {
            out: Vec::new(),
            block_size,
            data_block_writer: BlockWriter::new((block_size as f64 * 1.1) as usize),
            index_block_writer: BlockWriter::new(BLOCK_HANDLE_MAX_ENCODED_LENGTH * 1024),
            compression_type,
            last_key: None,
            record_count: 0,
        }
    }

    /// Current write position in the output buffer.
    pub fn position(&self) -> u64 {
        self.out.len() as u64
    }

    /// Put a key-value pair. Keys must be monotonically increasing.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.data_block_writer.add(key, value);

        // Only clone key if it changed
        match &self.last_key {
            Some(last) if last.as_slice() == key => {}
            _ => {
                self.last_key = Some(key.to_vec());
            }
        }

        if self.data_block_writer.memory() > self.block_size {
            self.flush()?;
        }

        self.record_count += 1;
        Ok(())
    }

    /// Flush the current data block to output.
    pub fn flush(&mut self) -> io::Result<()> {
        if self.data_block_writer.entry_count() == 0 {
            return Ok(());
        }

        let block_handle = self.write_block_data()?;
        let (handle_buf, handle_len) = block_handle.encode_to_buf();
        if let Some(ref last_key) = self.last_key {
            self.index_block_writer
                .add(last_key, &handle_buf[..handle_len]);
        }
        Ok(())
    }

    /// Write a data block: compress, compute CRC, write block + trailer.
    fn write_block_data(&mut self) -> io::Result<BlockHandle> {
        let block = self.data_block_writer.finish();

        let (final_data, block_compression_type) = self.maybe_compress(&block);

        let crc = compute_crc32(&final_data, block_compression_type);
        let trailer = BlockTrailer {
            compression_type: block_compression_type,
            crc32c: crc,
        };

        let block_handle = BlockHandle::new(self.out.len() as u64, final_data.len() as u32);

        self.out.extend_from_slice(&final_data);
        self.out.extend_from_slice(&trailer.to_bytes());

        Ok(block_handle)
    }

    fn maybe_compress<'a>(&self, block: &'a [u8]) -> (Cow<'a, [u8]>, BlockCompressionType) {
        match self.compression_type {
            BlockCompressionType::None => (Cow::Borrowed(block), BlockCompressionType::None),
            BlockCompressionType::Zstd => {
                // Prepend uncompressed length as var-int, then compressed data
                let mut compressed_buf =
                    vec![0u8; 5 + zstd::zstd_safe::compress_bound(block.len())];
                let var_len = encode_var_int_to_slice(&mut compressed_buf, 0, block.len() as i32);
                let compressed_size =
                    zstd::bulk::compress_to_buffer(block, &mut compressed_buf[var_len..], 3)
                        .unwrap_or(0);

                if compressed_size > 0
                    && (var_len + compressed_size) < block.len() - (block.len() / 8)
                {
                    compressed_buf.truncate(var_len + compressed_size);
                    (Cow::Owned(compressed_buf), BlockCompressionType::Zstd)
                } else {
                    (Cow::Borrowed(block), BlockCompressionType::None)
                }
            }
            _ => {
                // LZ4/LZO not implemented yet, fall back to no compression
                (Cow::Borrowed(block), BlockCompressionType::None)
            }
        }
    }

    /// Write the index block. Returns the index block handle.
    pub fn write_index_block(&mut self) -> io::Result<BlockHandle> {
        let block = self.index_block_writer.finish();
        let crc = compute_crc32(&block, BlockCompressionType::None);
        let trailer = BlockTrailer {
            compression_type: BlockCompressionType::None,
            crc32c: crc,
        };

        let block_handle = BlockHandle::new(self.out.len() as u64, block.len() as u32);

        self.out.extend_from_slice(&block);
        self.out.extend_from_slice(&trailer.to_bytes());

        Ok(block_handle)
    }

    /// Write raw bytes (e.g., footer).
    pub fn write_raw(&mut self, data: &[u8]) {
        self.out.extend_from_slice(data);
    }

    /// Consume the writer and return the complete file bytes.
    pub fn finish(self) -> Vec<u8> {
        self.out
    }
}

/// Read and decode a block from raw file data at the given handle position.
/// The data slice must contain the block data + trailer at the handle's offset.
pub fn read_block_at(data: &[u8], handle: &BlockHandle) -> io::Result<BlockReader> {
    let offset = handle.offset as usize;
    let size = handle.size as usize;

    // Read trailer
    let trailer_offset = offset + size;
    let trailer =
        BlockTrailer::read_from(&data[trailer_offset..trailer_offset + BLOCK_TRAILER_LENGTH])?;

    // Read block data
    let block_data = &data[offset..offset + size];

    // Verify CRC
    let crc = compute_crc32(block_data, trailer.compression_type);
    if crc != trailer.crc32c {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "CRC mismatch: expected 0x{:08X}, got 0x{:08X}",
                trailer.crc32c, crc
            ),
        ));
    }

    // Decompress if needed
    let decompressed = decompress_block(block_data, &trailer)?;
    BlockReader::create_from_vec(decompressed)
}

/// Read and decode a block from raw bytes where offset 0 is the start of the block.
/// The bytes must contain exactly: block_data (handle.size) + trailer (5 bytes).
pub fn read_block_from_bytes(bytes: &[u8], size: u32) -> io::Result<BlockReader> {
    let size = size as usize;
    let trailer = BlockTrailer::read_from(&bytes[size..size + BLOCK_TRAILER_LENGTH])?;
    let block_data = &bytes[..size];

    let crc = compute_crc32(block_data, trailer.compression_type);
    if crc != trailer.crc32c {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "CRC mismatch: expected 0x{:08X}, got 0x{:08X}",
                trailer.crc32c, crc
            ),
        ));
    }

    let decompressed = decompress_block(block_data, &trailer)?;
    BlockReader::create_from_vec(decompressed)
}

fn decompress_block(data: &[u8], trailer: &BlockTrailer) -> io::Result<Vec<u8>> {
    match trailer.compression_type {
        BlockCompressionType::None => Ok(data.to_vec()),
        BlockCompressionType::Zstd => {
            let mut cursor = Cursor::new(data);
            let uncompressed_size = crate::btree::var_len::decode_var_int(&mut cursor)? as usize;
            let compressed_start = cursor.position() as usize;
            let compressed_data = &data[compressed_start..];
            let mut decompressed = vec![0u8; uncompressed_size];
            let actual = zstd::bulk::decompress_to_buffer(compressed_data, &mut decompressed)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            if actual != uncompressed_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Decompressed size mismatch: expected {uncompressed_size}, got {actual}"
                    ),
                ));
            }
            Ok(decompressed)
        }
        _ => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!(
                "Compression type {:?} not supported",
                trailer.compression_type
            ),
        )),
    }
}

/// SstFileReader reads an SST file and supports point lookups and range iteration.
pub struct SstFileReader {
    data: Option<Vec<u8>>,
    index_block: BlockReader,
}

impl SstFileReader {
    /// Create a reader from file bytes and the index block handle.
    pub fn new(data: Vec<u8>, index_block_handle: BlockHandle) -> io::Result<Self> {
        let index_block = read_block_at(&data, &index_block_handle)?;
        Ok(Self {
            data: Some(data),
            index_block,
        })
    }

    /// Create a reader from a pre-loaded index block only (no full file data).
    /// Data blocks must be loaded externally via `read_block_at`.
    pub fn from_index_block(index_block: BlockReader) -> Self {
        Self {
            data: None,
            index_block,
        }
    }

    /// Get a reference to the index block.
    pub fn index_block(&self) -> &BlockReader {
        &self.index_block
    }

    /// Create an iterator for range queries.
    /// Only works when full file data is available (created via `new`).
    pub fn create_iterator(&self) -> SstFileIterator<'_> {
        SstFileIterator {
            reader: self,
            index_iter: self.index_block.iter(),
            seeked_data_block: None,
        }
    }

    fn get_next_data_block(
        &self,
        index_iter: &mut crate::btree::block::BlockIter<'_>,
    ) -> io::Result<Option<BlockReader>> {
        let data = self
            .data
            .as_ref()
            .expect("full file data required for iteration");
        match index_iter.next() {
            Some((_key, value)) => {
                let handle = BlockHandle::decode(value)?;
                let block = read_block_at(data, &handle)?;
                Ok(Some(block))
            }
            None => Ok(None),
        }
    }
}

/// Iterator over SST file data blocks for range queries.
pub struct SstFileIterator<'a> {
    reader: &'a SstFileReader,
    index_iter: crate::btree::block::BlockIter<'a>,
    seeked_data_block: Option<(BlockReader, usize)>, // (block, start_offset)
}

impl<'a> SstFileIterator<'a> {
    /// Seek to the first entry whose key >= target_key.
    #[cfg(test)]
    pub fn seek_to<F>(&mut self, key: &[u8], cmp: &F)
    where
        F: Fn(&[u8], &[u8]) -> std::cmp::Ordering,
    {
        // Seek in index block to find the data block that may contain the key.
        // Index block entries have the last key of each data block as key.
        let (_, mut index_positioned) = self.reader.index_block.seek_and_iter(key, cmp);

        if index_positioned.has_next() {
            let (_index_key, handle_bytes) = index_positioned.next().unwrap();
            let handle = BlockHandle::decode(handle_bytes).unwrap();
            let data = self
                .reader
                .data
                .as_ref()
                .expect("full file data required for seek");
            let data_block = read_block_at(data, &handle).unwrap();

            // Seek within the data block
            let (_, seeked_iter) = data_block.seek_and_iter(key, cmp);
            let offset = seeked_iter.offset;

            // The index block entry key is the last key of the corresponding data block.
            // If there is some index entry key >= targetKey, the related data block must
            // also contain some key >= target key.
            debug_assert!(
                seeked_iter.has_next(),
                "Data block must contain key >= target after index seek"
            );
            self.seeked_data_block = Some((data_block, offset));

            // Update index_iter to continue from after this block
            self.index_iter = index_positioned;
        } else {
            self.seeked_data_block = None;
            self.index_iter = index_positioned;
        }
    }

    /// Read the next batch (data block). Returns None when reaching file end.
    pub fn read_batch(&mut self) -> io::Result<Option<DataBlockBatch>> {
        if let Some((block, start_offset)) = self.seeked_data_block.take() {
            return Ok(Some(DataBlockBatch {
                reader: block,
                offset: start_offset,
                index: 0, // will be recalculated
            }));
        }

        match self.reader.get_next_data_block(&mut self.index_iter)? {
            Some(block) => Ok(Some(DataBlockBatch {
                reader: block,
                offset: 0,
                index: 0,
            })),
            None => Ok(None),
        }
    }
}

/// A batch of entries from a single data block.
pub struct DataBlockBatch {
    reader: BlockReader,
    offset: usize,
    index: usize,
}

impl DataBlockBatch {
    /// Returns (key, value) as borrowed slices (zero-copy).
    pub fn next(&mut self) -> Option<(&[u8], &[u8])> {
        if self.offset >= self.reader.data.len() {
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
    fn test_sst_file_roundtrip() {
        let mut writer = SstFileWriter::new(64, BlockCompressionType::None);

        let entries: Vec<(&[u8], &[u8])> = vec![
            (b"apple", b"1"),
            (b"banana", b"2"),
            (b"cherry", b"3"),
            (b"date", b"4"),
            (b"elderberry", b"5"),
            (b"fig", b"6"),
            (b"grape", b"7"),
        ];

        for (k, v) in &entries {
            writer.put(k, v).unwrap();
        }
        writer.flush().unwrap();
        let index_handle = writer.write_index_block().unwrap();
        let data = writer.finish();

        let reader = SstFileReader::new(data, index_handle).unwrap();
        let mut iter = reader.create_iterator();

        let mut result: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        while let Some(mut batch) = iter.read_batch().unwrap() {
            while let Some((k, v)) = batch.next() {
                result.push((k.to_vec(), v.to_vec()));
            }
        }

        assert_eq!(result.len(), entries.len());
        for (i, (k, v)) in result.iter().enumerate() {
            assert_eq!(k.as_slice(), entries[i].0);
            assert_eq!(v.as_slice(), entries[i].1);
        }
    }

    #[test]
    fn test_sst_file_seek() {
        let mut writer = SstFileWriter::new(32, BlockCompressionType::None);

        let entries: Vec<(&[u8], &[u8])> = vec![
            (b"aaa", b"1"),
            (b"bbb", b"2"),
            (b"ccc", b"3"),
            (b"ddd", b"4"),
            (b"eee", b"5"),
            (b"fff", b"6"),
        ];

        for (k, v) in &entries {
            writer.put(k, v).unwrap();
        }
        writer.flush().unwrap();
        let index_handle = writer.write_index_block().unwrap();
        let data = writer.finish();

        let reader = SstFileReader::new(data, index_handle).unwrap();
        let cmp = |a: &[u8], b: &[u8]| a.cmp(b);

        // Seek to "ccc"
        let mut iter = reader.create_iterator();
        iter.seek_to(b"ccc", &cmp);

        let mut result: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        while let Some(mut batch) = iter.read_batch().unwrap() {
            while let Some((k, v)) = batch.next() {
                result.push((k.to_vec(), v.to_vec()));
            }
        }

        // Should get ccc, ddd, eee, fff
        assert!(!result.is_empty());
        assert_eq!(result[0].0, b"ccc");
    }
}
