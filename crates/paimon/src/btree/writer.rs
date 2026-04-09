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

//! BTree index writer compatible with Java Paimon's BTreeIndexWriter.
//!
//! Keys must be written in monotonically increasing order.
//! Entries with the same key are combined into a compact list of row ids.
//! Null keys are stored in a separate roaring bitmap.

use crate::btree::block::{BlockCompressionType, BlockHandle};
use crate::btree::footer::BTreeFileFooter;
use crate::btree::meta::BTreeIndexMeta;
use crate::btree::sst_file::SstFileWriter;
use crate::btree::var_len::{encode_var_int, encode_var_long};
use crate::io::FileWrite;
use roaring::RoaringTreemap;
use std::cmp::Ordering;
use std::io;

/// BTree index writer. Writes sorted key -> row_id_list entries into a BTree index file.
///
/// Usage:
/// 1. Call `write(key, row_id)` for each entry (keys must be sorted).
/// 2. Call `finish()` to close the file and get the index meta.
pub struct BTreeIndexWriter<F: Fn(&[u8], &[u8]) -> Ordering> {
    sst_writer: SstFileWriter,
    current_row_ids: Vec<i64>,
    last_key: Option<Vec<u8>>,
    first_key: Option<Vec<u8>>,
    null_bitmap: Option<RoaringTreemap>,
    row_count: u64,
    key_comparator: F,
}

/// Result of finishing a BTree index write.
pub struct BTreeWriteResult {
    /// The serialized index meta (first_key, last_key, has_nulls).
    pub meta: BTreeIndexMeta,
    /// Total row count written.
    pub row_count: u64,
}

impl BTreeIndexWriter<fn(&[u8], &[u8]) -> Ordering> {
    pub fn new(
        writer: Box<dyn FileWrite>,
        block_size: usize,
        compression_type: BlockCompressionType,
    ) -> Self {
        Self {
            sst_writer: SstFileWriter::new(writer, block_size, compression_type),
            current_row_ids: Vec::new(),
            last_key: None,
            first_key: None,
            null_bitmap: None,
            row_count: 0,
            key_comparator: |a, b| a.cmp(b),
        }
    }
}

impl<F: Fn(&[u8], &[u8]) -> Ordering> BTreeIndexWriter<F> {
    /// Create a writer with a custom key comparator.
    pub fn with_comparator(
        writer: Box<dyn FileWrite>,
        block_size: usize,
        compression_type: BlockCompressionType,
        cmp: F,
    ) -> Self {
        Self {
            sst_writer: SstFileWriter::new(writer, block_size, compression_type),
            current_row_ids: Vec::new(),
            last_key: None,
            first_key: None,
            null_bitmap: None,
            row_count: 0,
            key_comparator: cmp,
        }
    }

    /// Write a key and its associated row id.
    /// If key is None, the row id is added to the null bitmap.
    /// Keys must be written in sorted order; entries with the same key are combined.
    pub async fn write(&mut self, key: Option<&[u8]>, row_id: i64) -> io::Result<()> {
        self.row_count += 1;

        match key {
            None => {
                self.null_bitmap
                    .get_or_insert_with(RoaringTreemap::new)
                    .insert(row_id as u64);
            }
            Some(k) => {
                if let Some(ref last) = self.last_key {
                    if (self.key_comparator)(k, last) != Ordering::Equal {
                        self.flush_row_ids().await?;
                    }
                }
                self.last_key = Some(k.to_vec());
                self.current_row_ids.push(row_id);

                if self.first_key.is_none() {
                    self.first_key = Some(k.to_vec());
                }
            }
        }
        Ok(())
    }

    /// Flush accumulated row ids for the current key.
    async fn flush_row_ids(&mut self) -> io::Result<()> {
        if self.current_row_ids.is_empty() {
            return Ok(());
        }

        // Serialize row id list: var_len_int(count) + var_len_long(id) * count
        let mut value_buf = Vec::with_capacity(self.current_row_ids.len() * 9 + 5);
        encode_var_int(&mut value_buf, self.current_row_ids.len() as i32)?;
        for &row_id in &self.current_row_ids {
            encode_var_long(&mut value_buf, row_id)?;
        }
        self.current_row_ids.clear();

        if let Some(ref key) = self.last_key {
            self.sst_writer.put(key, &value_buf).await?;
        }
        Ok(())
    }

    /// Write the null bitmap block. Returns the block handle if there are nulls.
    async fn write_null_bitmap(&mut self) -> io::Result<Option<BlockHandle>> {
        let bitmap = match &self.null_bitmap {
            Some(bm) => bm,
            None => return Ok(None),
        };

        let mut serialized = Vec::new();
        bitmap.serialize_into(&mut serialized)?;
        let length = serialized.len();

        // CRC32 of the serialized bitmap
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&serialized);
        let crc_value = hasher.finalize();

        let null_bitmap_handle = BlockHandle::new(self.sst_writer.position(), length as u32);

        // Write serialized bitmap + CRC (4 bytes, little-endian)
        let mut block_data = Vec::with_capacity(length + 4);
        block_data.extend_from_slice(&serialized);
        block_data.extend_from_slice(&(crc_value as i32).to_le_bytes());
        self.sst_writer.write_raw(&block_data).await?;

        Ok(Some(null_bitmap_handle))
    }

    /// Finish writing: flush remaining data, write footer, close the file.
    /// Returns the index meta and row count.
    pub async fn finish(mut self) -> io::Result<BTreeWriteResult> {
        // Flush remaining row ids
        self.flush_row_ids().await?;

        // Flush remaining data blocks in SST writer
        self.sst_writer.flush().await?;

        // Write null bitmap
        let null_bitmap_handle = self.write_null_bitmap().await?;

        // No bloom filter for now (same as Java: todo)
        let bloom_filter_handle = None;

        // Write index block
        let index_block_handle = self.sst_writer.write_index_block().await?;

        // Write footer
        let footer =
            BTreeFileFooter::new(bloom_filter_handle, index_block_handle, null_bitmap_handle);
        let footer_bytes = footer.write_footer();
        self.sst_writer.write_raw(&footer_bytes).await?;

        // Close the underlying writer
        self.sst_writer.close().await?;

        // Build meta
        let meta = BTreeIndexMeta::new(
            self.first_key.clone(),
            self.last_key.clone(),
            self.null_bitmap.is_some(),
        );

        Ok(BTreeWriteResult {
            meta,
            row_count: self.row_count,
        })
    }
}
