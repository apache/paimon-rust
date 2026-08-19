// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Writer for Java Paimon's `BitmapGlobalIndexFormat`.

use super::bitmap_global_index_format::{BlockInfo, MAGIC, VERSION};
use crate::btree::var_len::{encode_var_int, encode_var_long};
use crate::btree::{compress_block, compute_crc32, BTreeIndexMeta, BlockCompressionType};
use crate::io::FileWrite;
use bytes::Bytes;
use roaring::RoaringTreemap;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::io::{self, Write};

/// Result of finishing a Java-compatible bitmap global index write.
pub(crate) struct BitmapWriteResult {
    pub(crate) meta: BTreeIndexMeta,
    pub(crate) row_count: u64,
}

pub(crate) struct BitmapGlobalIndexWriter<F: Fn(&[u8], &[u8]) -> Ordering> {
    writer: Box<dyn FileWrite>,
    dictionary_block_size: usize,
    compression_type: BlockCompressionType,
    compression_level: i32,
    key_comparator: F,
    bitmaps: BTreeMap<Vec<u8>, RoaringTreemap>,
    null_rows: RoaringTreemap,
    non_null_rows: RoaringTreemap,
    first_key: Option<Vec<u8>>,
    last_key: Option<Vec<u8>>,
    row_count: u64,
}

impl<F: Fn(&[u8], &[u8]) -> Ordering> BitmapGlobalIndexWriter<F> {
    #[cfg(test)]
    pub(crate) fn new(
        writer: Box<dyn FileWrite>,
        dictionary_block_size: usize,
        compression_type: BlockCompressionType,
        key_comparator: F,
    ) -> Self {
        Self::with_compression_level(
            writer,
            dictionary_block_size,
            compression_type,
            1,
            key_comparator,
        )
    }

    pub(crate) fn with_compression_level(
        writer: Box<dyn FileWrite>,
        dictionary_block_size: usize,
        compression_type: BlockCompressionType,
        compression_level: i32,
        key_comparator: F,
    ) -> Self {
        Self {
            writer,
            dictionary_block_size,
            compression_type,
            compression_level,
            key_comparator,
            bitmaps: BTreeMap::new(),
            null_rows: RoaringTreemap::new(),
            non_null_rows: RoaringTreemap::new(),
            first_key: None,
            last_key: None,
            row_count: 0,
        }
    }

    pub(crate) fn write(&mut self, key: Option<&[u8]>, relative_row_id: i64) -> io::Result<()> {
        if relative_row_id < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Bitmap global index row id must be non-negative: {relative_row_id}"),
            ));
        }

        self.row_count += 1;
        match key {
            Some(key) => {
                let row_id = relative_row_id as u64;
                self.non_null_rows.insert(row_id);
                self.bitmaps.entry(key.to_vec()).or_default().insert(row_id);
                self.update_min_max(key);
            }
            None => {
                self.null_rows.insert(relative_row_id as u64);
            }
        }
        Ok(())
    }

    /// Add one normalized multivalue posting without changing source-row
    /// accounting or the scalar null/non-null bitmaps.
    pub(crate) fn write_posting(&mut self, key: &[u8], relative_row_id: i64) -> io::Result<()> {
        if relative_row_id < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Bitmap global index row id must be non-negative: {relative_row_id}"),
            ));
        }
        let row_id = relative_row_id as u64;
        self.bitmaps.entry(key.to_vec()).or_default().insert(row_id);
        self.update_min_max(key);
        Ok(())
    }

    pub(crate) async fn finish(self) -> io::Result<BitmapWriteResult> {
        let row_count = self.row_count;
        self.finish_with_row_count(row_count).await
    }

    /// Finish a zero-to-many-key index while reporting the number of source
    /// rows covered by the file rather than the number of emitted postings.
    pub(crate) async fn finish_with_source_row_count(
        self,
        source_row_count: u64,
    ) -> io::Result<BitmapWriteResult> {
        self.finish_with_row_count(source_row_count).await
    }

    async fn finish_with_row_count(mut self, row_count: u64) -> io::Result<BitmapWriteResult> {
        let mut bitmaps = std::mem::take(&mut self.bitmaps)
            .into_iter()
            .collect::<Vec<_>>();
        bitmaps.sort_by(|(left, _), (right, _)| (self.key_comparator)(left, right));

        let mut bytes = Vec::new();
        write_bitmap_index_bytes(
            &mut bytes,
            &self.null_rows,
            &self.non_null_rows,
            &bitmaps,
            self.dictionary_block_size,
            self.compression_type,
            self.compression_level,
        )?;
        self.writer
            .write(Bytes::from(bytes))
            .await
            .map_err(|error| io::Error::other(error.to_string()))?;
        self.writer
            .close()
            .await
            .map_err(|error| io::Error::other(error.to_string()))?;

        Ok(BitmapWriteResult {
            meta: BTreeIndexMeta::new(self.first_key, self.last_key, !self.null_rows.is_empty()),
            row_count,
        })
    }

    fn update_min_max(&mut self, key: &[u8]) {
        if self
            .first_key
            .as_ref()
            .is_none_or(|existing| (self.key_comparator)(key, existing).is_lt())
        {
            self.first_key = Some(key.to_vec());
        }
        if self
            .last_key
            .as_ref()
            .is_none_or(|existing| (self.key_comparator)(key, existing).is_gt())
        {
            self.last_key = Some(key.to_vec());
        }
    }
}

#[derive(Clone)]
struct DictionaryBlockMeta {
    first_key: Vec<u8>,
    block: BlockInfo,
}

struct DictionaryEntry {
    key: Vec<u8>,
    bitmap_block: BlockInfo,
}

fn write_bitmap_index_bytes(
    out: &mut Vec<u8>,
    null_rows: &RoaringTreemap,
    non_null_rows: &RoaringTreemap,
    bitmaps: &[(Vec<u8>, RoaringTreemap)],
    dictionary_block_size: usize,
    compression_type: BlockCompressionType,
    compression_level: i32,
) -> io::Result<()> {
    if dictionary_block_size == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Bitmap dictionary block size must be greater than 0",
        ));
    }

    let null_rows_block = write_bitmap_block(out, null_rows)?;
    let non_null_rows_block = write_bitmap_block(out, non_null_rows)?;
    let (dictionary_blocks, value_count) = write_dictionary_and_bitmap_blocks(
        out,
        bitmaps,
        dictionary_block_size,
        compression_type,
        compression_level,
    )?;
    let index_block =
        write_index_block(out, &dictionary_blocks, compression_type, compression_level)?;

    out.extend_from_slice(&u64_to_i64(null_rows_block.offset)?.to_be_bytes());
    out.extend_from_slice(&usize_to_i32(null_rows_block.length)?.to_be_bytes());
    out.extend_from_slice(&u64_to_i64(non_null_rows_block.offset)?.to_be_bytes());
    out.extend_from_slice(&usize_to_i32(non_null_rows_block.length)?.to_be_bytes());
    out.extend_from_slice(&u64_to_i64(index_block.offset)?.to_be_bytes());
    out.extend_from_slice(&usize_to_i32(index_block.length)?.to_be_bytes());
    out.extend_from_slice(&usize_to_i32(value_count)?.to_be_bytes());
    out.extend_from_slice(&VERSION.to_be_bytes());
    out.extend_from_slice(&MAGIC.to_be_bytes());
    Ok(())
}

fn write_bitmap_block(out: &mut Vec<u8>, bitmap: &RoaringTreemap) -> io::Result<BlockInfo> {
    let offset = out.len() as u64;
    bitmap.serialize_into(&mut *out)?;
    Ok(BlockInfo {
        offset,
        length: out.len() - offset as usize,
    })
}

fn write_dictionary_and_bitmap_blocks(
    out: &mut Vec<u8>,
    bitmaps: &[(Vec<u8>, RoaringTreemap)],
    dictionary_block_size: usize,
    compression_type: BlockCompressionType,
    compression_level: i32,
) -> io::Result<(Vec<DictionaryBlockMeta>, usize)> {
    let mut block_metas = Vec::new();
    let mut current = DictionaryBlockBuilder::default();
    let mut value_count = 0usize;

    for (key, bitmap) in bitmaps {
        let bitmap_block = write_bitmap_block(out, bitmap)?;
        let entry = DictionaryEntry {
            key: key.clone(),
            bitmap_block,
        };
        if current.has_entries() && current.estimated_size_after(&entry) > dictionary_block_size {
            block_metas.push(write_dictionary_block(
                out,
                &current.entries,
                compression_type,
                compression_level,
            )?);
            current = DictionaryBlockBuilder::default();
        }
        current.add(entry);
        value_count += 1;
    }

    if current.has_entries() {
        block_metas.push(write_dictionary_block(
            out,
            &current.entries,
            compression_type,
            compression_level,
        )?);
    }
    Ok((block_metas, value_count))
}

fn write_dictionary_block(
    out: &mut Vec<u8>,
    entries: &[DictionaryEntry],
    compression_type: BlockCompressionType,
    compression_level: i32,
) -> io::Result<DictionaryBlockMeta> {
    let mut bytes = Vec::new();
    encode_var_int(&mut bytes, usize_to_i32(entries.len())?)?;
    for entry in entries {
        encode_var_int(&mut bytes, usize_to_i32(entry.key.len())?)?;
        bytes.extend_from_slice(&entry.key);
        encode_var_long(&mut bytes, u64_to_i64(entry.bitmap_block.offset)?)?;
        encode_var_int(&mut bytes, usize_to_i32(entry.bitmap_block.length)?)?;
    }
    let block = write_compressible_block(out, &bytes, compression_type, compression_level)?;
    Ok(DictionaryBlockMeta {
        first_key: entries[0].key.clone(),
        block,
    })
}

fn write_index_block(
    out: &mut Vec<u8>,
    blocks: &[DictionaryBlockMeta],
    compression_type: BlockCompressionType,
    compression_level: i32,
) -> io::Result<BlockInfo> {
    let mut bytes = Vec::new();
    encode_var_int(&mut bytes, usize_to_i32(blocks.len())?)?;
    for block in blocks {
        encode_var_int(&mut bytes, usize_to_i32(block.first_key.len())?)?;
        bytes.extend_from_slice(&block.first_key);
        encode_var_long(&mut bytes, u64_to_i64(block.block.offset)?)?;
        encode_var_int(&mut bytes, usize_to_i32(block.block.length)?)?;
    }
    write_compressible_block(out, &bytes, compression_type, compression_level)
}

fn write_compressible_block(
    out: &mut Vec<u8>,
    bytes: &[u8],
    compression_type: BlockCompressionType,
    compression_level: i32,
) -> io::Result<BlockInfo> {
    let (block_bytes, actual_compression_type) =
        compress_block(bytes, compression_type, compression_level)?;
    let offset = out.len() as u64;
    out.write_all(&block_bytes)?;
    let crc = compute_crc32(&block_bytes, actual_compression_type);
    out.write_all(&[actual_compression_type as u8])?;
    out.write_all(&crc.to_le_bytes())?;
    Ok(BlockInfo {
        offset,
        length: block_bytes.len(),
    })
}

#[derive(Default)]
struct DictionaryBlockBuilder {
    entries: Vec<DictionaryEntry>,
    entries_size: usize,
}

impl DictionaryBlockBuilder {
    fn has_entries(&self) -> bool {
        !self.entries.is_empty()
    }

    fn estimated_size_after(&self, entry: &DictionaryEntry) -> usize {
        estimated_var_len_int_size(self.entries.len() + 1)
            + self.entries_size
            + entry.estimated_size()
    }

    fn add(&mut self, entry: DictionaryEntry) {
        self.entries_size += entry.estimated_size();
        self.entries.push(entry);
    }
}

impl DictionaryEntry {
    fn estimated_size(&self) -> usize {
        estimated_var_len_int_size(self.key.len())
            + self.key.len()
            + estimated_var_len_long_size(self.bitmap_block.offset)
            + estimated_var_len_int_size(self.bitmap_block.length)
    }
}

fn estimated_var_len_int_size(mut value: usize) -> usize {
    let mut size = 1;
    while (value & !0x7f) != 0 {
        value >>= 7;
        size += 1;
    }
    size
}

fn estimated_var_len_long_size(mut value: u64) -> usize {
    let mut size = 1;
    while (value & !0x7f) != 0 {
        value >>= 7;
        size += 1;
    }
    size
}

fn usize_to_i32(value: usize) -> io::Result<i32> {
    i32::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Bitmap global index value is too large: {value}"),
        )
    })
}

fn u64_to_i64(value: u64) -> io::Result<i64> {
    i64::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Bitmap global index offset is too large: {value}"),
        )
    })
}
