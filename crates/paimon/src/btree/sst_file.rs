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
use crate::io::FileWrite;
use bytes::Bytes;
use std::borrow::Cow;
use std::io::{self, Cursor};

/// SstFileWriter writes sorted key-value pairs into an SST file format
/// via streaming writes to a `FileWrite`.
///
/// The file consists of:
/// - Multiple data blocks (each containing sorted key-value pairs)
/// - An index block (mapping last-key-of-block -> block handle)
/// - Optional bloom filter
pub struct SstFileWriter {
    writer: Box<dyn FileWrite>,
    bytes_written: u64,
    block_size: usize,
    data_block_writer: BlockWriter,
    index_block_writer: BlockWriter,
    compression_type: BlockCompressionType,
    last_key: Option<Vec<u8>>,
    record_count: u64,
}

impl SstFileWriter {
    pub fn new(
        writer: Box<dyn FileWrite>,
        block_size: usize,
        compression_type: BlockCompressionType,
    ) -> Self {
        Self {
            writer,
            bytes_written: 0,
            block_size,
            data_block_writer: BlockWriter::new((block_size as f64 * 1.1) as usize),
            index_block_writer: BlockWriter::new(BLOCK_HANDLE_MAX_ENCODED_LENGTH * 1024),
            compression_type,
            last_key: None,
            record_count: 0,
        }
    }

    /// Current write position in the output.
    pub fn position(&self) -> u64 {
        self.bytes_written
    }

    /// Write bytes to the underlying writer and track position.
    async fn write_bytes(&mut self, data: &[u8]) -> io::Result<()> {
        self.writer
            .write(Bytes::copy_from_slice(data))
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
        self.bytes_written += data.len() as u64;
        Ok(())
    }

    /// Put a key-value pair. Keys must be monotonically increasing.
    pub async fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.data_block_writer.add(key, value);

        // Only clone key if it changed
        match &self.last_key {
            Some(last) if last.as_slice() == key => {}
            _ => {
                self.last_key = Some(key.to_vec());
            }
        }

        if self.data_block_writer.memory() > self.block_size {
            self.flush().await?;
        }

        self.record_count += 1;
        Ok(())
    }

    /// Flush the current data block to output.
    pub async fn flush(&mut self) -> io::Result<()> {
        if self.data_block_writer.entry_count() == 0 {
            return Ok(());
        }

        let block_handle = self.write_block_data().await?;
        let (handle_buf, handle_len) = block_handle.encode_to_buf();
        if let Some(ref last_key) = self.last_key {
            self.index_block_writer
                .add(last_key, &handle_buf[..handle_len]);
        }
        Ok(())
    }

    /// Write a data block: compress, compute CRC, write block + trailer.
    async fn write_block_data(&mut self) -> io::Result<BlockHandle> {
        let block = self.data_block_writer.finish();

        let (final_data, block_compression_type) = self.maybe_compress(&block);

        let crc = compute_crc32(&final_data, block_compression_type);
        let trailer = BlockTrailer {
            compression_type: block_compression_type,
            crc32c: crc,
        };

        let block_handle = BlockHandle::new(self.bytes_written, final_data.len() as u32);

        self.write_bytes(&final_data).await?;
        self.write_bytes(&trailer.to_bytes()).await?;

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
    pub async fn write_index_block(&mut self) -> io::Result<BlockHandle> {
        let block = self.index_block_writer.finish();
        let crc = compute_crc32(&block, BlockCompressionType::None);
        let trailer = BlockTrailer {
            compression_type: BlockCompressionType::None,
            crc32c: crc,
        };

        let block_handle = BlockHandle::new(self.bytes_written, block.len() as u32);

        self.write_bytes(&block).await?;
        self.write_bytes(&trailer.to_bytes()).await?;

        Ok(block_handle)
    }

    /// Write raw bytes (e.g., footer, null bitmap).
    pub async fn write_raw(&mut self, data: &[u8]) -> io::Result<()> {
        self.write_bytes(data).await
    }

    /// Close the underlying writer.
    pub async fn close(mut self) -> io::Result<()> {
        self.writer
            .close()
            .await
            .map_err(|e| io::Error::other(e.to_string()))
    }
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

/// SstFileReader reads an SST file index block for async on-demand data block loading.
pub struct SstFileReader {
    index_block: BlockReader,
}

impl SstFileReader {
    /// Create a reader from a pre-loaded index block.
    pub fn from_index_block(index_block: BlockReader) -> Self {
        Self { index_block }
    }

    /// Get a reference to the index block.
    pub fn index_block(&self) -> &BlockReader {
        &self.index_block
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::test_util::VecFileWrite;

    #[tokio::test]
    async fn test_sst_file_roundtrip() {
        let buf = VecFileWrite::new();
        let mut writer = SstFileWriter::new(Box::new(buf.clone()), 64, BlockCompressionType::None);

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
            writer.put(k, v).await.unwrap();
        }
        writer.flush().await.unwrap();
        let index_handle = writer.write_index_block().await.unwrap();
        writer.close().await.unwrap();

        let data = buf.into_bytes();
        let reader = SstFileReader::from_index_block(
            read_block_from_bytes(
                &data[index_handle.offset as usize
                    ..index_handle.offset as usize + index_handle.full_block_size() as usize],
                index_handle.size,
            )
            .unwrap(),
        );

        // Verify by reading all entries through index block
        let index_block = reader.index_block();
        let mut iter = index_block.iter();
        let mut block_count = 0;
        let mut result: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        while let Some((_key, handle_bytes)) = iter.next() {
            let handle = BlockHandle::decode(handle_bytes).unwrap();
            let block = read_block_from_bytes(
                &data[handle.offset as usize
                    ..handle.offset as usize + handle.full_block_size() as usize],
                handle.size,
            )
            .unwrap();
            let mut offset = 0;
            while offset < block.data.len() {
                let (k, v, next) = block.read_entry_at(offset);
                result.push((k.to_vec(), v.to_vec()));
                offset = next;
            }
            block_count += 1;
        }

        assert!(block_count > 0);
        assert_eq!(result.len(), entries.len());
        for (i, (k, v)) in result.iter().enumerate() {
            assert_eq!(k.as_slice(), entries[i].0);
            assert_eq!(v.as_slice(), entries[i].1);
        }
    }
}
