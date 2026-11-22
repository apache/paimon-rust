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
use super::index_build::{error, sort::SortedRows};
use super::sorted_global_index_options::SortedIndexWriteOptions;
use crate::btree::key_serde::key_comparison_io_error;
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

pub(crate) struct BitmapGlobalIndexWriter<F: Fn(&[u8], &[u8]) -> crate::Result<Ordering>> {
    writer: Box<dyn FileWrite>,
    builder: BitmapIndexBuilder<F>,
}

struct BitmapIndexBuilder<F: Fn(&[u8], &[u8]) -> crate::Result<Ordering>> {
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

impl<F: Fn(&[u8], &[u8]) -> crate::Result<Ordering>> BitmapGlobalIndexWriter<F> {
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
            builder: BitmapIndexBuilder::new(
                dictionary_block_size,
                compression_type,
                compression_level,
                key_comparator,
            ),
        }
    }

    pub(crate) fn write(&mut self, key: Option<&[u8]>, relative_row_id: i64) -> io::Result<()> {
        self.builder.write(key, relative_row_id)
    }

    pub(crate) fn write_posting(&mut self, key: &[u8], relative_row_id: i64) -> io::Result<()> {
        self.builder.write_posting(key, relative_row_id)
    }

    pub(crate) async fn finish(self) -> io::Result<BitmapWriteResult> {
        let row_count = self.builder.row_count;
        self.finish_with_source_row_count(row_count).await
    }

    /// Finish a zero-to-many-key index with its physical source row count.
    pub(crate) async fn finish_with_source_row_count(
        mut self,
        row_count: u64,
    ) -> io::Result<BitmapWriteResult> {
        let (bytes, result) = self.builder.encode_with_row_count(row_count)?;
        self.writer.write(bytes).await.map_err(io::Error::other)?;
        self.writer.close().await.map_err(io::Error::other)?;
        Ok(result)
    }
}

impl<F: Fn(&[u8], &[u8]) -> crate::Result<Ordering>> BitmapIndexBuilder<F> {
    fn new(
        dictionary_block_size: usize,
        compression_type: BlockCompressionType,
        compression_level: i32,
        key_comparator: F,
    ) -> Self {
        Self {
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
                self.update_min_max(key)?;
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
        self.update_min_max(key)?;
        Ok(())
    }

    fn encode_with_row_count(mut self, row_count: u64) -> io::Result<(Bytes, BitmapWriteResult)> {
        let mut bitmaps = std::mem::take(&mut self.bitmaps)
            .into_iter()
            .collect::<Vec<_>>();
        // The build side serializes every key from the column's current type, so a
        // comparison failure here is a real defect rather than schema evolution.
        let mut failure = None;
        bitmaps.sort_by(
            |(left, _), (right, _)| match (self.key_comparator)(left, right) {
                Ok(order) => order,
                Err(error) => {
                    failure.get_or_insert(error);
                    Ordering::Equal
                }
            },
        );
        if let Some(error) = failure {
            return Err(key_comparison_io_error(error));
        }

        let mut section = BitmapSectionWriter::new(
            self.dictionary_block_size,
            self.compression_type,
            self.compression_level,
        )?;
        let nulls = write_bitmap_block(&mut section.output, &self.null_rows)?;
        let non_nulls = write_bitmap_block(&mut section.output, &self.non_null_rows)?;
        for (key, bitmap) in bitmaps {
            section.write_posting(key, &bitmap)?;
        }
        section.finish(nulls, non_nulls)?;
        Ok((
            Bytes::from(section.output.bytes),
            BitmapWriteResult {
                meta: BTreeIndexMeta::new(
                    self.first_key,
                    self.last_key,
                    !self.null_rows.is_empty(),
                ),
                row_count,
            },
        ))
    }

    fn update_min_max(&mut self, key: &[u8]) -> io::Result<()> {
        let replaces_first = match &self.first_key {
            None => true,
            Some(existing) => (self.key_comparator)(key, existing)
                .map_err(key_comparison_io_error)?
                .is_lt(),
        };
        if replaces_first {
            self.first_key = Some(key.to_vec());
        }
        let replaces_last = match &self.last_key {
            None => true,
            Some(existing) => (self.key_comparator)(key, existing)
                .map_err(key_comparison_io_error)?
                .is_gt(),
        };
        if replaces_last {
            self.last_key = Some(key.to_vec());
        }
        Ok(())
    }
}

/// Write one complete section to a shared centroid file, using section-relative offsets.
/// The input is sorted by key, so only the current key's posting needs to be retained.
pub(super) async fn write_sorted_bitmap_section(
    writer: &mut dyn FileWrite,
    mut rows: SortedRows,
    options: &SortedIndexWriteOptions,
) -> crate::Result<(usize, BitmapWriteResult)> {
    const FLUSH_BYTES: usize = 1024 * 1024;
    let mut section = BitmapSectionWriter::new(
        options.block_size,
        options.compression_type,
        options.compression_level,
    )
    .map_err(error)?;
    let mut null_rows = RoaringTreemap::new();
    let mut non_null_rows = RoaringTreemap::new();
    let mut posting = RoaringTreemap::new();
    let mut first_key = None;
    let mut current_key = None;
    let mut row_count = 0;
    while let Some((key, id)) = rows.next().await? {
        let id = u64::try_from(id).map_err(error)?;
        row_count += 1;
        let Some(key) = key else {
            null_rows.insert(id);
            continue;
        };
        non_null_rows.insert(id);
        if current_key.as_ref() != Some(&key) {
            if let Some(previous) = current_key.replace(key) {
                section.write_posting(previous, &posting).map_err(error)?;
                posting.clear();
                if section.output.bytes.len() >= FLUSH_BYTES {
                    section.output.flush_to(writer).await?;
                }
            } else {
                first_key = current_key.clone();
            }
        }
        posting.insert(id);
    }
    if let Some(key) = current_key.as_ref() {
        section
            .write_posting(key.clone(), &posting)
            .map_err(error)?;
    }
    let nulls = write_bitmap_block(&mut section.output, &null_rows).map_err(error)?;
    let non_nulls = write_bitmap_block(&mut section.output, &non_null_rows).map_err(error)?;
    section.finish(nulls, non_nulls).map_err(error)?;
    let length = usize::try_from(section.output.position()).map_err(error)?;
    section.output.flush_to(writer).await?;
    Ok((
        length,
        BitmapWriteResult {
            meta: BTreeIndexMeta::new(first_key, current_key, !null_rows.is_empty()),
            row_count,
        },
    ))
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

struct BitmapSectionWriter {
    output: SectionOutput,
    current: DictionaryBlockBuilder,
    blocks: Vec<DictionaryBlockMeta>,
    value_count: usize,
    dictionary_block_size: usize,
    compression_type: BlockCompressionType,
    compression_level: i32,
}

impl BitmapSectionWriter {
    fn new(
        dictionary_block_size: usize,
        compression_type: BlockCompressionType,
        compression_level: i32,
    ) -> io::Result<Self> {
        if dictionary_block_size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Bitmap dictionary block size must be greater than 0",
            ));
        }
        Ok(Self {
            output: SectionOutput::default(),
            current: DictionaryBlockBuilder::default(),
            blocks: Vec::new(),
            value_count: 0,
            dictionary_block_size,
            compression_type,
            compression_level,
        })
    }

    fn write_posting(&mut self, key: Vec<u8>, bitmap: &RoaringTreemap) -> io::Result<()> {
        let bitmap_block = write_bitmap_block(&mut self.output, bitmap)?;
        let entry = DictionaryEntry { key, bitmap_block };
        if self.current.has_entries()
            && self.current.estimated_size_after(&entry) > self.dictionary_block_size
        {
            self.flush_dictionary()?;
        }
        self.current.add(entry);
        self.value_count += 1;
        Ok(())
    }

    fn flush_dictionary(&mut self) -> io::Result<()> {
        if self.current.has_entries() {
            self.blocks.push(write_dictionary_block(
                &mut self.output,
                &self.current.entries,
                self.compression_type,
                self.compression_level,
            )?);
            self.current = DictionaryBlockBuilder::default();
        }
        Ok(())
    }

    fn finish(&mut self, nulls: BlockInfo, non_nulls: BlockInfo) -> io::Result<()> {
        self.flush_dictionary()?;
        let index = write_index_block(
            &mut self.output,
            &self.blocks,
            self.compression_type,
            self.compression_level,
        )?;
        let bytes = &mut self.output.bytes;
        for block in [nulls, non_nulls, index] {
            bytes.extend_from_slice(&u64_to_i64(block.offset)?.to_be_bytes());
            bytes.extend_from_slice(&usize_to_i32(block.length)?.to_be_bytes());
        }
        bytes.extend_from_slice(&usize_to_i32(self.value_count)?.to_be_bytes());
        bytes.extend_from_slice(&VERSION.to_be_bytes());
        bytes.extend_from_slice(&MAGIC.to_be_bytes());
        Ok(())
    }
}

#[derive(Default)]
struct SectionOutput {
    bytes: Vec<u8>,
    flushed: u64,
}

impl SectionOutput {
    fn position(&self) -> u64 {
        self.flushed + self.bytes.len() as u64
    }

    async fn flush_to(&mut self, writer: &mut dyn FileWrite) -> crate::Result<()> {
        let position = self.position();
        writer
            .write(Bytes::from(std::mem::take(&mut self.bytes)))
            .await?;
        self.flushed = position;
        Ok(())
    }
}

fn write_bitmap_block(out: &mut SectionOutput, bitmap: &RoaringTreemap) -> io::Result<BlockInfo> {
    let offset = out.position();
    let start = out.bytes.len();
    bitmap.serialize_into(&mut out.bytes)?;
    Ok(BlockInfo {
        offset,
        length: out.bytes.len() - start,
    })
}

fn write_dictionary_block(
    out: &mut SectionOutput,
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
    out: &mut SectionOutput,
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
    out: &mut SectionOutput,
    bytes: &[u8],
    compression_type: BlockCompressionType,
    compression_level: i32,
) -> io::Result<BlockInfo> {
    let (block_bytes, actual_compression_type) =
        compress_block(bytes, compression_type, compression_level)?;
    let offset = out.position();
    out.bytes.write_all(&block_bytes)?;
    let crc = compute_crc32(&block_bytes, actual_compression_type);
    out.bytes.write_all(&[actual_compression_type as u8])?;
    out.bytes.write_all(&crc.to_le_bytes())?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::{DataType, Datum, PredicateOperator, VarCharType};
    use crate::table::bitmap_global_index_format::make_bitmap_key_comparator;
    use crate::table::bitmap_global_index_reader::BitmapGlobalIndexReader;
    use crate::table::index_build::sort::IndexSorter;
    use std::collections::HashMap;

    #[tokio::test]
    async fn streamed_bitmap_section_preserves_offsets_across_flushes() {
        let data_type = DataType::VarChar(VarCharType::string_type());
        let suffix = "x".repeat(16 * 1024);
        let key = |id| format!("{id:03}{suffix}");
        let mut sorter = IndexSorter::new(256 * 1024, make_bitmap_key_comparator(&data_type));
        for id in (0..96).rev() {
            sorter.push((Some(key(id).into_bytes()), id * 7)).unwrap();
        }
        sorter.push((None, 700)).unwrap();
        let options = SortedIndexWriteOptions::from_options(
            "bitmap",
            &HashMap::from([("bitmap-index.compression".into(), "none".into())]),
        )
        .unwrap();
        let io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/streamed-bitmap-offsets";
        let mut output = io.new_output(path).unwrap().writer().await.unwrap();
        let (length, written) =
            write_sorted_bitmap_section(output.as_mut(), sorter.finish().unwrap(), &options)
                .await
                .unwrap();
        output.close().await.unwrap();
        assert!(length > 1024 * 1024);
        assert_eq!(written.row_count, 97);
        let input = io.new_input(path).unwrap().reader().await.unwrap();
        let reader = BitmapGlobalIndexReader::open(Box::new(input), length as u64)
            .await
            .unwrap();
        for id in [0, 48, 95] {
            let matches = reader
                .query(PredicateOperator::Eq, &[Datum::String(key(id))], &data_type)
                .await
                .unwrap();
            assert_eq!(matches.iter().collect::<Vec<_>>(), [id as u64 * 7]);
        }
        let nulls = reader
            .query(PredicateOperator::IsNull, &[], &data_type)
            .await
            .unwrap();
        assert_eq!(nulls.iter().collect::<Vec<_>>(), [700]);
    }
}
