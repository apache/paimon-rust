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

//! Java-compatible V1 FM-index container format.

use crate::btree::{
    compress_block, compute_crc32, decompress_block_with_expected_size, BlockCompressionType,
};
use crate::io::FileRead;
use std::io;
use std::ops::Range;
use std::sync::Arc;

pub(crate) const TERMINATOR: u16 = 0;
pub(crate) const SEPARATOR: u16 = 1;
pub(crate) const FIRST_BYTE_SYMBOL: u16 = 2;
const MAX_ALPHABET_SIZE: usize = 258;

const PARTITION_MAGIC: u32 = 0x464d4950;
const CONTAINER_MAGIC: u32 = 0x464d4958;
const VERSION: u32 = 1;
const INDEX_META_MAGIC: u32 = 0x464d4d45;
const INDEX_META_VERSION: u32 = 1;
const INDEX_META_HEADER_LENGTH: usize = 28;
const INDEX_META_PARTITION_LENGTH: usize = 28;
const INDEX_META_CHECKSUM_LENGTH: usize = 4;
const FEATURE_FLAGS: u32 = 1 | (1 << 1) | (1 << 2);

pub(crate) const BLOCK_WORDS: usize = 4096;
pub(crate) const BLOCK_BITS: usize = BLOCK_WORDS * u64::BITS as usize;
const QUAD_VALUES_PER_WORD: usize = 32;
pub(crate) const QUAD_BLOCK_VALUES: usize = BLOCK_WORDS * QUAD_VALUES_PER_WORD;
pub(crate) const VALUE_BLOCK_INTS: usize = 8192;
pub(crate) const PARTITION_FOOTER_LENGTH: usize = 64;
const CONTAINER_FOOTER_LENGTH: usize = 64;
const FOOTER_CHECKSUM_OFFSET: usize = 60;
const MAX_DIRECTORY_UNCOMPRESSED_LENGTH: usize = 16 * 1024 * 1024;
pub(crate) const MAX_DATA_BLOCK_UNCOMPRESSED_LENGTH: usize = 64 * 1024;

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct BlockInfo {
    pub(crate) offset: u64,
    pub(crate) stored_length: usize,
    pub(crate) uncompressed_length: usize,
    pub(crate) compression: BlockCompressionType,
    pub(crate) checksum: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PartitionMeta {
    pub(crate) start_offset: u64,
    pub(crate) end_offset: u64,
    pub(crate) first_row_id: u64,
    pub(crate) row_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IndexMeta {
    pub(crate) first_row_id: u64,
    pub(crate) row_count: u64,
    pub(crate) partitions: Vec<PartitionMeta>,
}

#[derive(Debug, Clone)]
pub(crate) struct QuadBlockMeta {
    pub(crate) first_value: usize,
    pub(crate) value_count: usize,
    pub(crate) prefix_counts: [usize; 4],
    pub(crate) counts: [usize; 4],
    pub(crate) block: BlockInfo,
}

#[derive(Debug, Clone)]
pub(crate) struct QuadVectorMeta {
    pub(crate) value_length: usize,
    pub(crate) total_counts: [usize; 4],
    pub(crate) blocks: Vec<QuadBlockMeta>,
}

impl QuadVectorMeta {
    pub(crate) fn block(&self, position: usize) -> io::Result<&QuadBlockMeta> {
        if position >= self.value_length {
            return Err(invalid("FM quaternary position is outside the vector"));
        }
        self.blocks
            .get(position / QUAD_BLOCK_VALUES)
            .ok_or_else(|| invalid("FM quaternary block is missing"))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BitBlockMeta {
    pub(crate) first_bit: usize,
    pub(crate) bit_count: usize,
    pub(crate) prefix_ones: usize,
    pub(crate) ones_count: usize,
    pub(crate) block: BlockInfo,
}

#[derive(Debug, Clone)]
pub(crate) struct BitVectorMeta {
    pub(crate) bit_length: usize,
    pub(crate) total_ones: usize,
    pub(crate) blocks: Vec<BitBlockMeta>,
}

impl BitVectorMeta {
    pub(crate) fn block(&self, position: usize) -> io::Result<&BitBlockMeta> {
        if position >= self.bit_length {
            return Err(invalid("FM bit position is outside the vector"));
        }
        self.blocks
            .get(position / BLOCK_BITS)
            .ok_or_else(|| invalid("FM bit block is missing"))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct IntBlockMeta {
    pub(crate) first_value: usize,
    pub(crate) value_count: usize,
    pub(crate) block: BlockInfo,
}

#[derive(Debug, Clone)]
pub(crate) struct IntVectorMeta {
    pub(crate) value_count: usize,
    pub(crate) blocks: Vec<IntBlockMeta>,
}

impl IntVectorMeta {
    pub(crate) fn block(&self, position: usize) -> io::Result<&IntBlockMeta> {
        if position >= self.value_count {
            return Err(invalid("FM sample position is outside the vector"));
        }
        self.blocks
            .get(position / VALUE_BLOCK_INTS)
            .ok_or_else(|| invalid("FM sample block is missing"))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Directory {
    pub(crate) row_count: usize,
    pub(crate) text_length: usize,
    pub(crate) sample_rate: usize,
    pub(crate) level_count: usize,
    pub(crate) alphabet_size: usize,
    pub(crate) byte_to_symbol: [i32; 256],
    pub(crate) cumulative_counts: Vec<usize>,
    pub(crate) wavelet_starts: Vec<usize>,
    pub(crate) digit_starts: Vec<[usize; 4]>,
    pub(crate) wavelets: Vec<QuadVectorMeta>,
    pub(crate) sampled_rows: BitVectorMeta,
    pub(crate) sample_values: IntVectorMeta,
    pub(crate) null_rows: BitVectorMeta,
    pub(crate) row_boundaries: BitVectorMeta,
}

#[derive(Debug, Clone)]
pub(crate) struct Footer {
    pub(crate) directory: BlockInfo,
    pub(crate) first_row_id: u64,
    pub(crate) row_count: usize,
    pub(crate) text_length: usize,
    pub(crate) sample_rate: usize,
    pub(crate) partition_start_offset: u64,
    pub(crate) partition_end_offset: u64,
}

#[derive(Debug)]
pub(crate) struct QuadBlock {
    words: Vec<u64>,
    prefixes: Vec<usize>,
    value_count: usize,
}

impl QuadBlock {
    pub(crate) fn get(&self, value: usize) -> io::Result<usize> {
        if value >= self.value_count {
            return Err(invalid("Invalid FM quaternary offset"));
        }
        Ok(((self.words[value >> 5] >> ((value & 31) * 2)) & 3) as usize)
    }

    pub(crate) fn rank(&self, digit: usize, end: usize) -> io::Result<usize> {
        if digit >= 4 || end > self.value_count {
            return Err(invalid("Invalid FM quaternary rank range"));
        }
        let full_words = end >> 5;
        let group = full_words >> 6;
        let mut count = self.prefixes[group * 4 + digit];
        for word in &self.words[(group << 6)..full_words] {
            count += count_digit(*word, digit, QUAD_VALUES_PER_WORD);
        }
        let remaining = end & (QUAD_VALUES_PER_WORD - 1);
        if remaining > 0 {
            count += count_digit(self.words[full_words], digit, remaining);
        }
        Ok(count)
    }

    pub(crate) fn retained_size(&self) -> usize {
        self.words.len() * 8 + self.prefixes.len() * 4
    }
}

#[derive(Debug)]
pub(crate) struct BitBlock {
    words: Vec<u64>,
    prefixes: Vec<usize>,
    bit_count: usize,
}

impl BitBlock {
    pub(crate) fn get(&self, bit: usize) -> io::Result<bool> {
        if bit >= self.bit_count {
            return Err(invalid("Invalid FM bit offset"));
        }
        Ok(self.words[bit >> 6] & (1u64 << (bit & 63)) != 0)
    }

    pub(crate) fn rank_ones(&self, end: usize) -> io::Result<usize> {
        if end > self.bit_count {
            return Err(invalid("Invalid FM rank range"));
        }
        let full_words = end >> 6;
        let group = full_words >> 6;
        let mut ones = self.prefixes[group];
        for word in &self.words[(group << 6)..full_words] {
            ones += word.count_ones() as usize;
        }
        let remaining = end & 63;
        if remaining > 0 {
            ones += (self.words[full_words] & ((1u64 << remaining) - 1)).count_ones() as usize;
        }
        Ok(ones)
    }

    pub(crate) fn retained_size(&self) -> usize {
        self.words.len() * 8 + self.prefixes.len() * 4
    }
}

pub(crate) fn write_index_meta(meta: &IndexMeta) -> io::Result<Vec<u8>> {
    validate_index_meta(meta)?;
    let length = INDEX_META_HEADER_LENGTH
        .checked_add(
            meta.partitions
                .len()
                .checked_mul(INDEX_META_PARTITION_LENGTH)
                .ok_or_else(|| invalid_input("FM partition directory is too large"))?,
        )
        .and_then(|value| value.checked_add(INDEX_META_CHECKSUM_LENGTH))
        .ok_or_else(|| invalid_input("FM partition directory is too large"))?;
    if length > MAX_DIRECTORY_UNCOMPRESSED_LENGTH {
        return Err(invalid_input("FM partition directory exceeds 16 MiB"));
    }
    let mut out = Vec::with_capacity(length);
    put_u32(&mut out, INDEX_META_MAGIC);
    put_u32(&mut out, INDEX_META_VERSION);
    put_u64(&mut out, meta.first_row_id);
    put_u64(&mut out, meta.row_count);
    put_u32(
        &mut out,
        to_u32(meta.partitions.len(), "FM partition count")?,
    );
    for partition in &meta.partitions {
        put_u64(&mut out, partition.start_offset);
        put_u64(&mut out, partition.end_offset);
        put_u64(&mut out, partition.first_row_id);
        put_u32(
            &mut out,
            to_u32(partition.row_count, "FM partition row count")?,
        );
    }
    let checksum = compute_crc32(&out, BlockCompressionType::None);
    put_u32(&mut out, checksum);
    debug_assert_eq!(out.len(), length);
    read_index_meta(&out)?;
    Ok(out)
}

pub(crate) fn read_index_meta(bytes: &[u8]) -> io::Result<IndexMeta> {
    if bytes.len() < INDEX_META_HEADER_LENGTH + INDEX_META_CHECKSUM_LENGTH
        || bytes.len() > MAX_DIRECTORY_UNCOMPRESSED_LENGTH
    {
        return Err(invalid("Invalid FM manifest metadata length"));
    }
    let mut input = Decoder::new(bytes);
    if input.u32()? != INDEX_META_MAGIC {
        return Err(invalid("Invalid FM manifest metadata magic"));
    }
    if input.u32()? != INDEX_META_VERSION {
        return Err(invalid("Unsupported FM manifest metadata version"));
    }
    let first_row_id = input.u64()?;
    let row_count = input.u64()?;
    let partition_count = input.usize32("FM partition count")?;
    if partition_count == 0 {
        return Err(invalid("FM index must contain partitions"));
    }
    let expected_length = INDEX_META_HEADER_LENGTH
        .checked_add(
            partition_count
                .checked_mul(INDEX_META_PARTITION_LENGTH)
                .ok_or_else(|| invalid("FM manifest metadata length overflow"))?,
        )
        .and_then(|value| value.checked_add(INDEX_META_CHECKSUM_LENGTH))
        .ok_or_else(|| invalid("FM manifest metadata length overflow"))?;
    if expected_length != bytes.len() {
        return Err(invalid("Invalid FM manifest metadata length"));
    }
    let expected_checksum = u32::from_be_bytes(bytes[bytes.len() - 4..].try_into().unwrap());
    let actual_checksum = compute_crc32(&bytes[..bytes.len() - 4], BlockCompressionType::None);
    if expected_checksum != actual_checksum {
        return Err(invalid("FM manifest metadata checksum mismatch"));
    }

    let mut partitions = Vec::with_capacity(partition_count);
    for _ in 0..partition_count {
        partitions.push(PartitionMeta {
            start_offset: input.u64()?,
            end_offset: input.u64()?,
            first_row_id: input.u64()?,
            row_count: input.usize32("FM partition row count")?,
        });
    }
    let _checksum = input.u32()?;
    input.finish()?;
    let meta = IndexMeta {
        first_row_id,
        row_count,
        partitions,
    };
    validate_index_meta(&meta)?;
    Ok(meta)
}

fn validate_index_meta(meta: &IndexMeta) -> io::Result<()> {
    if meta.row_count == 0 || meta.partitions.is_empty() {
        return Err(invalid("Invalid FM index row range"));
    }
    let mut expected_offset = 0u64;
    let mut expected_row_id = meta.first_row_id;
    for partition in &meta.partitions {
        if partition.start_offset != expected_offset
            || partition.end_offset <= partition.start_offset
            || partition.end_offset - partition.start_offset < PARTITION_FOOTER_LENGTH as u64
            || partition.first_row_id != expected_row_id
            || partition.row_count == 0
        {
            return Err(invalid(
                "FM partitions or row ranges are not canonical and contiguous",
            ));
        }
        expected_offset = partition.end_offset;
        expected_row_id = expected_row_id
            .checked_add(partition.row_count as u64)
            .ok_or_else(|| invalid("FM row range overflow"))?;
    }
    if expected_row_id
        != meta
            .first_row_id
            .checked_add(meta.row_count)
            .ok_or_else(|| invalid("FM row range overflow"))?
    {
        return Err(invalid(
            "FM partition row counts do not match the file row count",
        ));
    }
    Ok(())
}

pub(crate) fn write_block(
    out: &mut Vec<u8>,
    base_offset: u64,
    uncompressed: &[u8],
    compression: BlockCompressionType,
    compression_level: i32,
) -> io::Result<BlockInfo> {
    let offset = base_offset
        .checked_add(out.len() as u64)
        .ok_or_else(|| invalid_input("FM output offset overflow"))?;
    let (stored, actual_compression) =
        compress_block(uncompressed, compression, compression_level)?;
    let checksum = compute_crc32(stored.as_ref(), actual_compression);
    let stored_length = stored.len();
    out.extend_from_slice(stored.as_ref());
    Ok(BlockInfo {
        offset,
        stored_length,
        uncompressed_length: uncompressed.len(),
        compression: actual_compression,
        checksum,
    })
}

pub(crate) fn write_quad_vector(
    out: &mut Vec<u8>,
    base_offset: u64,
    words: &[u64],
    value_length: usize,
    compression: BlockCompressionType,
    compression_level: i32,
) -> io::Result<QuadVectorMeta> {
    if words.len() != words_for_quads(value_length) {
        return Err(invalid_input("Invalid FM quaternary-vector word count"));
    }
    let mut blocks = Vec::new();
    let mut total_counts = [0usize; 4];
    for word_start in (0..words.len()).step_by(BLOCK_WORDS) {
        let word_count = BLOCK_WORDS.min(words.len() - word_start);
        let first_value = word_start * QUAD_VALUES_PER_WORD;
        let value_count = (word_count * QUAD_VALUES_PER_WORD).min(value_length - first_value);
        let mut counts = [0usize; 4];
        for i in 0..word_count {
            let valid = QUAD_VALUES_PER_WORD.min(value_count - i * QUAD_VALUES_PER_WORD);
            for (digit, count) in counts.iter_mut().enumerate() {
                *count += count_digit(words[word_start + i], digit, valid);
            }
        }
        let encoded = encode_quad_block(&words[word_start..word_start + word_count], value_count);
        let block = write_block(out, base_offset, &encoded, compression, compression_level)?;
        blocks.push(QuadBlockMeta {
            first_value,
            value_count,
            prefix_counts: total_counts,
            counts,
            block,
        });
        for digit in 0..4 {
            total_counts[digit] += counts[digit];
        }
    }
    Ok(QuadVectorMeta {
        value_length,
        total_counts,
        blocks,
    })
}

pub(crate) fn write_bit_vector(
    out: &mut Vec<u8>,
    base_offset: u64,
    words: &[u64],
    bit_length: usize,
    compression: BlockCompressionType,
    compression_level: i32,
) -> io::Result<BitVectorMeta> {
    if words.len() != words_for_bits(bit_length) {
        return Err(invalid_input("Invalid FM bit-vector word count"));
    }
    let mut blocks = Vec::new();
    let mut total_ones = 0usize;
    for word_start in (0..words.len()).step_by(BLOCK_WORDS) {
        let word_count = BLOCK_WORDS.min(words.len() - word_start);
        let first_bit = word_start * 64;
        let bit_count = (word_count * 64).min(bit_length - first_bit);
        let block_words = &words[word_start..word_start + word_count];
        let ones_count = block_words
            .iter()
            .map(|word| word.count_ones() as usize)
            .sum();
        let encoded = encode_bit_block(block_words);
        let block = write_block(out, base_offset, &encoded, compression, compression_level)?;
        blocks.push(BitBlockMeta {
            first_bit,
            bit_count,
            prefix_ones: total_ones,
            ones_count,
            block,
        });
        total_ones += ones_count;
    }
    Ok(BitVectorMeta {
        bit_length,
        total_ones,
        blocks,
    })
}

pub(crate) fn write_int_vector(
    out: &mut Vec<u8>,
    base_offset: u64,
    values: &[usize],
    compression: BlockCompressionType,
    compression_level: i32,
) -> io::Result<IntVectorMeta> {
    let mut blocks = Vec::new();
    for first_value in (0..values.len()).step_by(VALUE_BLOCK_INTS) {
        let value_count = VALUE_BLOCK_INTS.min(values.len() - first_value);
        let mut encoded = Vec::with_capacity(value_count * 4);
        for value in &values[first_value..first_value + value_count] {
            put_u32(&mut encoded, to_u32(*value, "FM sampled suffix value")?);
        }
        let block = write_block(out, base_offset, &encoded, compression, compression_level)?;
        blocks.push(IntBlockMeta {
            first_value,
            value_count,
            block,
        });
    }
    Ok(IntVectorMeta {
        value_count: values.len(),
        blocks,
    })
}

pub(crate) fn write_directory(
    out: &mut Vec<u8>,
    base_offset: u64,
    directory: &Directory,
    compression: BlockCompressionType,
    compression_level: i32,
) -> io::Result<BlockInfo> {
    let mut bytes = Vec::new();
    put_usize32(&mut bytes, directory.row_count, "FM row count")?;
    put_usize32(&mut bytes, directory.text_length, "FM text length")?;
    put_usize32(&mut bytes, directory.sample_rate, "FM sample rate")?;
    put_usize32(&mut bytes, directory.level_count, "FM level count")?;
    put_usize32(&mut bytes, directory.alphabet_size, "FM alphabet size")?;
    put_usize32(&mut bytes, BLOCK_WORDS, "FM block words")?;
    for symbol in directory.byte_to_symbol {
        put_i32(&mut bytes, symbol);
    }
    for value in &directory.cumulative_counts {
        put_usize32(&mut bytes, *value, "FM cumulative count")?;
    }
    for level in 0..directory.level_count {
        for start in directory.digit_starts[level] {
            put_usize32(&mut bytes, start, "FM wavelet digit start")?;
        }
        write_quad_vector_meta(&mut bytes, &directory.wavelets[level])?;
    }
    write_bit_vector_meta(&mut bytes, &directory.sampled_rows)?;
    write_int_vector_meta(&mut bytes, &directory.sample_values)?;
    write_bit_vector_meta(&mut bytes, &directory.null_rows)?;
    write_bit_vector_meta(&mut bytes, &directory.row_boundaries)?;
    if bytes.len() > MAX_DIRECTORY_UNCOMPRESSED_LENGTH {
        return Err(invalid_input("FM directory exceeds 16 MiB"));
    }
    write_block(out, base_offset, &bytes, compression, compression_level)
}

pub(crate) fn write_partition_footer(
    out: &mut Vec<u8>,
    directory: BlockInfo,
    first_row_id: u64,
    row_count: usize,
    text_length: usize,
    sample_rate: usize,
) -> io::Result<()> {
    let mut footer = vec![0u8; PARTITION_FOOTER_LENGTH];
    write_block_info_at(&mut footer, 0, directory)?;
    write_u64_at(&mut footer, 24, first_row_id);
    write_u32_at(&mut footer, 32, to_u32(row_count, "FM row count")?);
    write_u32_at(&mut footer, 36, to_u32(text_length, "FM text length")?);
    write_u32_at(&mut footer, 40, to_u32(sample_rate, "FM sample rate")?);
    write_u32_at(&mut footer, 44, FEATURE_FLAGS);
    write_u32_at(&mut footer, 52, VERSION);
    write_u32_at(&mut footer, 56, PARTITION_MAGIC);
    let checksum = compute_crc32(
        &footer[..FOOTER_CHECKSUM_OFFSET],
        BlockCompressionType::None,
    );
    write_u32_at(&mut footer, FOOTER_CHECKSUM_OFFSET, checksum);
    out.extend_from_slice(&footer);
    Ok(())
}

pub(crate) fn write_container_footer(
    out: &mut Vec<u8>,
    directory: BlockInfo,
    first_row_id: u64,
    row_count: u64,
    partition_count: usize,
) -> io::Result<()> {
    let mut footer = vec![0u8; CONTAINER_FOOTER_LENGTH];
    write_block_info_at(&mut footer, 0, directory)?;
    write_u64_at(&mut footer, 24, first_row_id);
    write_u64_at(&mut footer, 32, row_count);
    write_u32_at(
        &mut footer,
        40,
        to_u32(partition_count, "FM partition count")?,
    );
    write_u32_at(&mut footer, 44, FEATURE_FLAGS);
    write_u32_at(&mut footer, 52, VERSION);
    write_u32_at(&mut footer, 56, CONTAINER_MAGIC);
    let checksum = compute_crc32(
        &footer[..FOOTER_CHECKSUM_OFFSET],
        BlockCompressionType::None,
    );
    write_u32_at(&mut footer, FOOTER_CHECKSUM_OFFSET, checksum);
    out.extend_from_slice(&footer);
    Ok(())
}

pub(crate) async fn validate_container(
    reader: &dyn FileRead,
    file_size: u64,
    expected: &IndexMeta,
) -> io::Result<()> {
    if file_size < CONTAINER_FOOTER_LENGTH as u64 {
        return Err(invalid("Invalid FM container size"));
    }
    let footer = read_exact(
        reader,
        file_size - CONTAINER_FOOTER_LENGTH as u64..file_size,
    )
    .await?;
    validate_footer_common(&footer, CONTAINER_MAGIC, "container")?;
    let mut input = Decoder::new(&footer);
    let directory = input.block_info()?;
    let first_row_id = input.u64()?;
    let row_count = input.u64()?;
    let partition_count = input.usize32("FM partition count")?;
    let flags = input.u32()?;
    if flags != FEATURE_FLAGS || input.u32()? != 0 || input.u32()? != VERSION {
        return Err(invalid("Unsupported FM container physical layout"));
    }
    let _magic = input.u32()?;
    let _checksum = input.u32()?;
    input.finish()?;
    if first_row_id != expected.first_row_id
        || row_count != expected.row_count
        || partition_count != expected.partitions.len()
    {
        return Err(invalid(
            "FM manifest metadata does not match the container footer",
        ));
    }
    validate_block(
        directory,
        file_size - CONTAINER_FOOTER_LENGTH as u64,
        MAX_DIRECTORY_UNCOMPRESSED_LENGTH,
    )?;
    if directory.offset + directory.stored_length as u64
        != file_size - CONTAINER_FOOTER_LENGTH as u64
    {
        return Err(invalid(
            "FM container directory is not immediately before the footer",
        ));
    }
    let actual = read_index_meta(&read_block(reader, directory, file_size).await?)?;
    if &actual != expected {
        return Err(invalid(
            "FM manifest metadata does not match the container directory",
        ));
    }
    if actual
        .partitions
        .last()
        .is_none_or(|partition| partition.end_offset != directory.offset)
    {
        return Err(invalid(
            "FM partitions do not exactly cover the container payload",
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(crate) async fn read_container_index_meta(
    reader: &dyn FileRead,
    file_size: u64,
) -> io::Result<IndexMeta> {
    if file_size < CONTAINER_FOOTER_LENGTH as u64 {
        return Err(invalid("Invalid FM container size"));
    }
    let footer = read_exact(
        reader,
        file_size - CONTAINER_FOOTER_LENGTH as u64..file_size,
    )
    .await?;
    validate_footer_common(&footer, CONTAINER_MAGIC, "container")?;
    let mut input = Decoder::new(&footer);
    let directory = input.block_info()?;
    let first_row_id = input.u64()?;
    let row_count = input.u64()?;
    let partition_count = input.usize32("FM partition count")?;
    let flags = input.u32()?;
    if flags != FEATURE_FLAGS || input.u32()? != 0 || input.u32()? != VERSION {
        return Err(invalid("Unsupported FM container physical layout"));
    }
    let _magic = input.u32()?;
    let _checksum = input.u32()?;
    input.finish()?;
    validate_block(
        directory,
        file_size - CONTAINER_FOOTER_LENGTH as u64,
        MAX_DIRECTORY_UNCOMPRESSED_LENGTH,
    )?;
    if directory.offset + directory.stored_length as u64
        != file_size - CONTAINER_FOOTER_LENGTH as u64
    {
        return Err(invalid(
            "FM container directory is not immediately before the footer",
        ));
    }
    let meta = read_index_meta(&read_block(reader, directory, file_size).await?)?;
    if meta.first_row_id != first_row_id
        || meta.row_count != row_count
        || meta.partitions.len() != partition_count
    {
        return Err(invalid("FM container footer does not match its directory"));
    }
    validate_container(reader, file_size, &meta).await?;
    Ok(meta)
}

pub(crate) async fn read_footer(
    reader: &dyn FileRead,
    file_size: u64,
    partition: &PartitionMeta,
) -> io::Result<Footer> {
    if partition.end_offset > file_size
        || partition.end_offset <= partition.start_offset
        || partition.end_offset - partition.start_offset < PARTITION_FOOTER_LENGTH as u64
    {
        return Err(invalid("Invalid FM partition range"));
    }
    let footer_offset = partition.end_offset - PARTITION_FOOTER_LENGTH as u64;
    let bytes = read_exact(reader, footer_offset..partition.end_offset).await?;
    validate_footer_common(&bytes, PARTITION_MAGIC, "partition")?;
    let mut input = Decoder::new(&bytes);
    let directory = input.block_info()?;
    let first_row_id = input.u64()?;
    let row_count = input.usize32("FM row count")?;
    let text_length = input.usize32("FM text length")?;
    let sample_rate = input.usize32("FM sample rate")?;
    let flags = input.u32()?;
    if flags != FEATURE_FLAGS || input.u32()? != 0 || input.u32()? != VERSION {
        return Err(invalid("Unsupported FM partition physical layout"));
    }
    let _magic = input.u32()?;
    let _checksum = input.u32()?;
    input.finish()?;
    validate_sample_rate(sample_rate)?;
    if row_count == 0
        || text_length < row_count + 1
        || first_row_id != partition.first_row_id
        || row_count != partition.row_count
    {
        return Err(invalid("FM partition footer metadata is inconsistent"));
    }
    validate_block(directory, footer_offset, MAX_DIRECTORY_UNCOMPRESSED_LENGTH)?;
    if directory.offset < partition.start_offset
        || directory.offset + directory.stored_length as u64 != footer_offset
    {
        return Err(invalid(
            "FM partition directory is not immediately before its footer",
        ));
    }
    Ok(Footer {
        directory,
        first_row_id,
        row_count,
        text_length,
        sample_rate,
        partition_start_offset: partition.start_offset,
        partition_end_offset: partition.end_offset,
    })
}

pub(crate) async fn read_directory(
    reader: &dyn FileRead,
    footer: &Footer,
) -> io::Result<Directory> {
    let bytes = read_block(reader, footer.directory, footer.partition_end_offset).await?;
    let mut input = Decoder::new(&bytes);
    let row_count = input.usize32("FM row count")?;
    let text_length = input.usize32("FM text length")?;
    let sample_rate = input.usize32("FM sample rate")?;
    if row_count != footer.row_count
        || text_length != footer.text_length
        || sample_rate != footer.sample_rate
    {
        return Err(invalid("FM footer and directory metadata do not match"));
    }
    let level_count = input.usize32("FM level count")?;
    let alphabet_size = input.usize32("FM alphabet size")?;
    if !(FIRST_BYTE_SYMBOL as usize..=MAX_ALPHABET_SIZE).contains(&alphabet_size)
        || level_count != levels_for_alphabet(alphabet_size)?
        || input.usize32("FM block words")? != BLOCK_WORDS
    {
        return Err(invalid("Unsupported FM physical layout"));
    }
    let mut byte_to_symbol = [-1i32; 256];
    let mut next_dense = FIRST_BYTE_SYMBOL as i32;
    for symbol in &mut byte_to_symbol {
        *symbol = input.i32()?;
        if *symbol != -1 && *symbol != next_dense {
            return Err(invalid("Invalid FM dense byte alphabet"));
        }
        if *symbol >= 0 {
            next_dense += 1;
        }
    }
    if next_dense as usize != alphabet_size {
        return Err(invalid("FM dense byte alphabet does not match its size"));
    }
    let mut cumulative_counts = Vec::with_capacity(alphabet_size + 1);
    for _ in 0..=alphabet_size {
        cumulative_counts.push(input.usize32("FM cumulative count")?);
    }
    if cumulative_counts[0] != 0
        || cumulative_counts[alphabet_size] != text_length
        || cumulative_counts.windows(2).any(|pair| pair[0] > pair[1])
        || cumulative_counts[1] != 1
        || cumulative_counts[SEPARATOR as usize + 1] - cumulative_counts[SEPARATOR as usize]
            != row_count
    {
        return Err(invalid("Invalid FM cumulative counts"));
    }
    for symbol in byte_to_symbol.iter().copied().filter(|symbol| *symbol >= 0) {
        let symbol = symbol as usize;
        if cumulative_counts[symbol + 1] == cumulative_counts[symbol] {
            return Err(invalid("FM alphabet contains an unused byte symbol"));
        }
    }

    let mut expected_offset = footer.partition_start_offset;
    let mut digit_starts = Vec::with_capacity(level_count);
    let mut wavelets = Vec::with_capacity(level_count);
    for level in 0..level_count {
        let expected_counts =
            expected_digit_counts(&cumulative_counts, (level_count - level - 1) * 2);
        let mut starts = [0usize; 4];
        let mut start = 0usize;
        for digit in 0..4 {
            starts[digit] = input.usize32("FM wavelet digit start")?;
            if starts[digit] != start {
                return Err(invalid("Invalid FM wavelet digit start"));
            }
            start += expected_counts[digit];
        }
        if start != text_length {
            return Err(invalid("FM wavelet digits do not cover the text"));
        }
        digit_starts.push(starts);
        wavelets.push(input.quad_vector_meta(
            text_length,
            expected_counts,
            &mut expected_offset,
            footer.directory.offset,
        )?);
    }
    let sampled_rows =
        input.bit_vector_meta(text_length, &mut expected_offset, footer.directory.offset)?;
    let expected_samples = (text_length - 1) / sample_rate + 1;
    if sampled_rows.total_ones != expected_samples {
        return Err(invalid(
            "FM sampled-row cardinality does not match its rate",
        ));
    }
    let sample_values = input.int_vector_meta(
        expected_samples,
        &mut expected_offset,
        footer.directory.offset,
    )?;
    let null_rows =
        input.bit_vector_meta(row_count, &mut expected_offset, footer.directory.offset)?;
    let row_boundaries =
        input.bit_vector_meta(text_length, &mut expected_offset, footer.directory.offset)?;
    if row_boundaries.total_ones != row_count || expected_offset != footer.directory.offset {
        return Err(invalid(
            "FM payload blocks or row boundaries are inconsistent",
        ));
    }
    input.finish()?;
    let wavelet_starts = wavelet_starts(&cumulative_counts, level_count)?;
    Ok(Directory {
        row_count,
        text_length,
        sample_rate,
        level_count,
        alphabet_size,
        byte_to_symbol,
        cumulative_counts,
        wavelet_starts,
        digit_starts,
        wavelets,
        sampled_rows,
        sample_values,
        null_rows,
        row_boundaries,
    })
}

pub(crate) async fn read_block(
    reader: &dyn FileRead,
    block: BlockInfo,
    payload_end: u64,
) -> io::Result<Vec<u8>> {
    validate_block(block, payload_end, MAX_DIRECTORY_UNCOMPRESSED_LENGTH)?;
    let stored = read_exact(reader, block.range()?).await?;
    decode_stored_block(&stored, block)
}

pub(crate) async fn read_blocks(
    reader: &dyn FileRead,
    blocks: &[BlockInfo],
    payload_end: u64,
) -> io::Result<Vec<Vec<u8>>> {
    let first = blocks
        .first()
        .ok_or_else(|| invalid_input("FM demand page must contain blocks"))?;
    let mut next_offset = first.offset;
    let mut total = 0usize;
    for block in blocks {
        validate_block(*block, payload_end, MAX_DATA_BLOCK_UNCOMPRESSED_LENGTH)?;
        if block.offset != next_offset {
            return Err(invalid("FM demand-page blocks are not contiguous"));
        }
        total = total
            .checked_add(block.stored_length)
            .ok_or_else(|| invalid("FM demand page length overflow"))?;
        next_offset = next_offset
            .checked_add(block.stored_length as u64)
            .ok_or_else(|| invalid("FM demand page range overflow"))?;
    }
    let stored = read_exact(
        reader,
        first.offset
            ..first.offset + u64::try_from(total).map_err(|_| invalid("FM page too large"))?,
    )
    .await?;
    let mut result = Vec::with_capacity(blocks.len());
    let mut offset = 0usize;
    for block in blocks {
        let end = offset + block.stored_length;
        result.push(decode_stored_block(&stored[offset..end], *block)?);
        offset = end;
    }
    Ok(result)
}

pub(crate) fn decode_quad_block(bytes: &[u8], meta: &QuadBlockMeta) -> io::Result<QuadBlock> {
    let mut input = Decoder::new(bytes);
    let word_count = input.usize32("FM quad word count")?;
    let prefix_count = input.usize32("FM quad prefix count")?;
    let expected_words = words_for_quads(meta.value_count);
    let expected_prefixes = word_count.div_ceil(64) + 1;
    if word_count != expected_words || prefix_count != expected_prefixes {
        return Err(invalid("Invalid FM quaternary rank block header"));
    }
    let mut prefixes = Vec::with_capacity(prefix_count * 4);
    for _ in 0..prefix_count * 4 {
        prefixes.push(input.usize32("FM quad prefix")?);
    }
    let mut words = Vec::with_capacity(word_count);
    let mut counts = [0usize; 4];
    for i in 0..word_count {
        if i & 63 == 0 {
            let prefix = (i / 64) * 4;
            if prefixes[prefix..prefix + 4] != counts {
                return Err(invalid("Invalid FM quaternary rank prefixes"));
            }
        }
        let word = input.u64()?;
        words.push(word);
        let valid = QUAD_VALUES_PER_WORD.min(meta.value_count - i * QUAD_VALUES_PER_WORD);
        for (digit, count) in counts.iter_mut().enumerate() {
            *count += count_digit(word, digit, valid);
        }
    }
    if prefixes[(prefix_count - 1) * 4..prefix_count * 4] != counts || counts != meta.counts {
        return Err(invalid("FM quaternary rank cardinality mismatch"));
    }
    input.finish()?;
    let remaining = meta.value_count & (QUAD_VALUES_PER_WORD - 1);
    if remaining != 0
        && words.last().is_some_and(|word| {
            let mask = !((1u64 << (remaining * 2)) - 1);
            word & mask != 0
        })
    {
        return Err(invalid("FM quaternary block has non-zero padding"));
    }
    Ok(QuadBlock {
        words,
        prefixes,
        value_count: meta.value_count,
    })
}

pub(crate) fn decode_bit_block(bytes: &[u8], meta: &BitBlockMeta) -> io::Result<BitBlock> {
    let mut input = Decoder::new(bytes);
    let word_count = input.usize32("FM bit word count")?;
    let prefix_count = input.usize32("FM bit prefix count")?;
    if word_count != words_for_bits(meta.bit_count) || prefix_count != word_count.div_ceil(64) + 1 {
        return Err(invalid("Invalid FM rank block header"));
    }
    let mut prefixes = Vec::with_capacity(prefix_count);
    for _ in 0..prefix_count {
        prefixes.push(input.usize32("FM bit prefix")?);
    }
    let mut words = Vec::with_capacity(word_count);
    let mut ones = 0usize;
    for i in 0..word_count {
        if i & 63 == 0 && prefixes[i / 64] != ones {
            return Err(invalid("Invalid FM rank block prefix counts"));
        }
        let word = input.u64()?;
        words.push(word);
        ones += word.count_ones() as usize;
    }
    if prefixes[prefix_count - 1] != ones || ones != meta.ones_count {
        return Err(invalid("FM rank block cardinality mismatch"));
    }
    input.finish()?;
    let remaining = meta.bit_count & 63;
    if remaining != 0
        && words
            .last()
            .is_some_and(|word| word & !((1u64 << remaining) - 1) != 0)
    {
        return Err(invalid("FM rank block has non-zero padding"));
    }
    Ok(BitBlock {
        words,
        prefixes,
        bit_count: meta.bit_count,
    })
}

pub(crate) fn decode_int_block(bytes: &[u8], meta: &IntBlockMeta) -> io::Result<Vec<usize>> {
    if bytes.len() != meta.value_count * 4 {
        return Err(invalid("Invalid FM sample block length"));
    }
    let mut input = Decoder::new(bytes);
    let mut values = Vec::with_capacity(meta.value_count);
    for _ in 0..meta.value_count {
        values.push(input.usize32("FM sample value")?);
    }
    input.finish()?;
    Ok(values)
}

fn decode_stored_block(stored: &[u8], block: BlockInfo) -> io::Result<Vec<u8>> {
    if stored.len() != block.stored_length {
        return Err(invalid("FM block stored length mismatch"));
    }
    let checksum = compute_crc32(stored, block.compression);
    if checksum != block.checksum {
        return Err(invalid(format!(
            "FM block checksum mismatch: expected={}, actual={checksum}",
            block.checksum
        )));
    }
    if block.compression == BlockCompressionType::None {
        if block.stored_length != block.uncompressed_length {
            return Err(invalid("Invalid uncompressed FM block length"));
        }
        return Ok(stored.to_vec());
    }
    decompress_block_with_expected_size(stored, block.compression, block.uncompressed_length)
}

fn validate_footer_common(bytes: &[u8], magic: u32, scope: &str) -> io::Result<()> {
    if bytes.len() != 64 || u32::from_be_bytes(bytes[56..60].try_into().unwrap()) != magic {
        return Err(invalid(format!(
            "File is not an FM {scope} (bad footer magic)"
        )));
    }
    if u32::from_be_bytes(bytes[52..56].try_into().unwrap()) != VERSION {
        return Err(invalid(format!("Unsupported FM {scope} version")));
    }
    let expected = u32::from_be_bytes(bytes[60..64].try_into().unwrap());
    let actual = compute_crc32(&bytes[..60], BlockCompressionType::None);
    if expected != actual {
        return Err(invalid(format!("FM {scope} footer checksum mismatch")));
    }
    Ok(())
}

fn validate_block(block: BlockInfo, payload_end: u64, max_uncompressed: usize) -> io::Result<()> {
    let end = block
        .offset
        .checked_add(block.stored_length as u64)
        .ok_or_else(|| invalid("FM block range overflow"))?;
    if block.stored_length == 0
        || block.uncompressed_length == 0
        || block.stored_length > block.uncompressed_length
        || block.uncompressed_length > max_uncompressed
        || end > payload_end
    {
        return Err(invalid("Invalid FM block metadata"));
    }
    Ok(())
}

async fn read_exact(reader: &dyn FileRead, range: Range<u64>) -> io::Result<Vec<u8>> {
    let expected = usize::try_from(range.end.saturating_sub(range.start))
        .map_err(|_| invalid("FM read range is too large"))?;
    let bytes = reader
        .read(range)
        .await
        .map_err(|error| io::Error::other(error.to_string()))?;
    if bytes.len() != expected {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "FM file ended before the requested block was read",
        ));
    }
    Ok(bytes.to_vec())
}

fn encode_bit_block(words: &[u64]) -> Vec<u8> {
    let prefix_count = words.len().div_ceil(64) + 1;
    let mut bytes = Vec::with_capacity((2 + prefix_count) * 4 + words.len() * 8);
    put_u32(&mut bytes, words.len() as u32);
    put_u32(&mut bytes, prefix_count as u32);
    let mut ones = 0u32;
    for group in 0..prefix_count {
        put_u32(&mut bytes, ones);
        for word in &words[(group * 64).min(words.len())..((group + 1) * 64).min(words.len())] {
            ones += word.count_ones();
        }
    }
    for word in words {
        put_u64(&mut bytes, *word);
    }
    bytes
}

fn encode_quad_block(words: &[u64], value_count: usize) -> Vec<u8> {
    let prefix_count = words.len().div_ceil(64) + 1;
    let mut bytes = Vec::with_capacity((2 + prefix_count * 4) * 4 + words.len() * 8);
    put_u32(&mut bytes, words.len() as u32);
    put_u32(&mut bytes, prefix_count as u32);
    let mut counts = [0u32; 4];
    for group in 0..prefix_count {
        for count in counts {
            put_u32(&mut bytes, count);
        }
        for (i, word) in words
            .iter()
            .enumerate()
            .take(((group + 1) * 64).min(words.len()))
            .skip(group * 64)
        {
            let valid = QUAD_VALUES_PER_WORD.min(value_count - i * QUAD_VALUES_PER_WORD);
            for (digit, count) in counts.iter_mut().enumerate() {
                *count += count_digit(*word, digit, valid) as u32;
            }
        }
    }
    for word in words {
        put_u64(&mut bytes, *word);
    }
    bytes
}

fn write_quad_vector_meta(out: &mut Vec<u8>, vector: &QuadVectorMeta) -> io::Result<()> {
    put_usize32(out, vector.value_length, "FM quad vector length")?;
    for count in vector.total_counts {
        put_usize32(out, count, "FM quad total count")?;
    }
    put_usize32(out, vector.blocks.len(), "FM quad block count")?;
    for block in &vector.blocks {
        put_usize32(out, block.first_value, "FM quad first value")?;
        put_usize32(out, block.value_count, "FM quad value count")?;
        for count in block.prefix_counts {
            put_usize32(out, count, "FM quad prefix count")?;
        }
        for count in block.counts {
            put_usize32(out, count, "FM quad count")?;
        }
        write_block_info(out, block.block)?;
    }
    Ok(())
}

fn write_bit_vector_meta(out: &mut Vec<u8>, vector: &BitVectorMeta) -> io::Result<()> {
    put_usize32(out, vector.bit_length, "FM bit vector length")?;
    put_usize32(out, vector.total_ones, "FM bit total ones")?;
    put_usize32(out, vector.blocks.len(), "FM bit block count")?;
    for block in &vector.blocks {
        put_usize32(out, block.first_bit, "FM first bit")?;
        put_usize32(out, block.bit_count, "FM bit count")?;
        put_usize32(out, block.prefix_ones, "FM prefix ones")?;
        put_usize32(out, block.ones_count, "FM ones count")?;
        write_block_info(out, block.block)?;
    }
    Ok(())
}

fn write_int_vector_meta(out: &mut Vec<u8>, vector: &IntVectorMeta) -> io::Result<()> {
    put_usize32(out, vector.value_count, "FM sample count")?;
    put_usize32(out, vector.blocks.len(), "FM sample block count")?;
    for block in &vector.blocks {
        put_usize32(out, block.first_value, "FM sample first value")?;
        put_usize32(out, block.value_count, "FM sample value count")?;
        write_block_info(out, block.block)?;
    }
    Ok(())
}

fn write_block_info(out: &mut Vec<u8>, block: BlockInfo) -> io::Result<()> {
    put_u64(out, block.offset);
    put_usize32(out, block.stored_length, "FM stored block length")?;
    put_usize32(
        out,
        block.uncompressed_length,
        "FM uncompressed block length",
    )?;
    put_u32(out, block.compression as u32);
    put_u32(out, block.checksum);
    Ok(())
}

fn write_block_info_at(out: &mut [u8], offset: usize, block: BlockInfo) -> io::Result<()> {
    write_u64_at(out, offset, block.offset);
    write_u32_at(
        out,
        offset + 8,
        to_u32(block.stored_length, "FM stored block length")?,
    );
    write_u32_at(
        out,
        offset + 12,
        to_u32(block.uncompressed_length, "FM uncompressed block length")?,
    );
    write_u32_at(out, offset + 16, block.compression as u32);
    write_u32_at(out, offset + 20, block.checksum);
    Ok(())
}

impl BlockInfo {
    fn range(self) -> io::Result<Range<u64>> {
        Ok(self.offset
            ..self
                .offset
                .checked_add(self.stored_length as u64)
                .ok_or_else(|| invalid("FM block range overflow"))?)
    }
}

impl Decoder<'_> {
    fn block_info(&mut self) -> io::Result<BlockInfo> {
        let offset = self.u64()?;
        let stored_length = self.usize32("FM stored block length")?;
        let uncompressed_length = self.usize32("FM uncompressed block length")?;
        let compression_id = self.u32()?;
        let compression = u8::try_from(compression_id)
            .ok()
            .and_then(|id| BlockCompressionType::from_persistent_id(id).ok())
            .ok_or_else(|| invalid(format!("Unknown FM compression ID: {compression_id}")))?;
        let checksum = self.u32()?;
        Ok(BlockInfo {
            offset,
            stored_length,
            uncompressed_length,
            compression,
            checksum,
        })
    }

    fn quad_vector_meta(
        &mut self,
        expected_value_length: usize,
        expected_total_counts: [usize; 4],
        expected_offset: &mut u64,
        payload_end: u64,
    ) -> io::Result<QuadVectorMeta> {
        let value_length = self.usize32("FM quad vector length")?;
        let mut total_counts = [0usize; 4];
        for count in &mut total_counts {
            *count = self.usize32("FM quad total count")?;
        }
        let block_count = self.usize32("FM quad block count")?;
        if value_length != expected_value_length
            || total_counts != expected_total_counts
            || total_counts.iter().sum::<usize>() != value_length
            || block_count != value_length.div_ceil(QUAD_BLOCK_VALUES)
        {
            return Err(invalid("Invalid FM quaternary-vector metadata"));
        }
        let mut blocks = Vec::with_capacity(block_count);
        let mut first_value = 0usize;
        let mut prefix_counts = [0usize; 4];
        for _ in 0..block_count {
            let stored_first = self.usize32("FM quad first value")?;
            let value_count = self.usize32("FM quad value count")?;
            let mut stored_prefixes = [0usize; 4];
            for value in &mut stored_prefixes {
                *value = self.usize32("FM quad prefix count")?;
            }
            let mut counts = [0usize; 4];
            for value in &mut counts {
                *value = self.usize32("FM quad count")?;
            }
            let block = self.block_info()?;
            let expected_values = QUAD_BLOCK_VALUES.min(value_length - first_value);
            if stored_first != first_value
                || value_count != expected_values
                || stored_prefixes != prefix_counts
                || counts.iter().sum::<usize>() != value_count
                || block.uncompressed_length != encoded_quad_block_length(value_count)
            {
                return Err(invalid("Invalid FM quaternary block metadata"));
            }
            validate_canonical_block(block, expected_offset, payload_end)?;
            blocks.push(QuadBlockMeta {
                first_value,
                value_count,
                prefix_counts: stored_prefixes,
                counts,
                block,
            });
            for digit in 0..4 {
                prefix_counts[digit] += counts[digit];
            }
            first_value += value_count;
        }
        if first_value != value_length || prefix_counts != total_counts {
            return Err(invalid("FM quaternary blocks do not cover the vector"));
        }
        Ok(QuadVectorMeta {
            value_length,
            total_counts,
            blocks,
        })
    }

    fn bit_vector_meta(
        &mut self,
        expected_bit_length: usize,
        expected_offset: &mut u64,
        payload_end: u64,
    ) -> io::Result<BitVectorMeta> {
        let bit_length = self.usize32("FM bit vector length")?;
        let total_ones = self.usize32("FM bit total ones")?;
        let block_count = self.usize32("FM bit block count")?;
        if bit_length != expected_bit_length
            || total_ones > bit_length
            || block_count != bit_length.div_ceil(BLOCK_BITS)
        {
            return Err(invalid("Invalid FM bit-vector metadata"));
        }
        let mut blocks = Vec::with_capacity(block_count);
        let mut first_bit = 0usize;
        let mut prefix_ones = 0usize;
        for _ in 0..block_count {
            let stored_first = self.usize32("FM first bit")?;
            let bit_count = self.usize32("FM bit count")?;
            let stored_prefix = self.usize32("FM prefix ones")?;
            let ones_count = self.usize32("FM ones count")?;
            let block = self.block_info()?;
            let expected_bits = BLOCK_BITS.min(bit_length - first_bit);
            if stored_first != first_bit
                || bit_count != expected_bits
                || stored_prefix != prefix_ones
                || ones_count > bit_count
                || block.uncompressed_length != encoded_bit_block_length(bit_count)
            {
                return Err(invalid("Invalid FM bit block metadata"));
            }
            validate_canonical_block(block, expected_offset, payload_end)?;
            blocks.push(BitBlockMeta {
                first_bit,
                bit_count,
                prefix_ones,
                ones_count,
                block,
            });
            first_bit += bit_count;
            prefix_ones += ones_count;
        }
        if first_bit != bit_length || prefix_ones != total_ones {
            return Err(invalid("FM bit-vector blocks do not match their summary"));
        }
        Ok(BitVectorMeta {
            bit_length,
            total_ones,
            blocks,
        })
    }

    fn int_vector_meta(
        &mut self,
        expected_value_count: usize,
        expected_offset: &mut u64,
        payload_end: u64,
    ) -> io::Result<IntVectorMeta> {
        let value_count = self.usize32("FM sample count")?;
        let block_count = self.usize32("FM sample block count")?;
        if value_count != expected_value_count
            || block_count != value_count.div_ceil(VALUE_BLOCK_INTS)
        {
            return Err(invalid("Invalid FM sample vector metadata"));
        }
        let mut blocks = Vec::with_capacity(block_count);
        let mut first_value = 0usize;
        for _ in 0..block_count {
            let stored_first = self.usize32("FM sample first value")?;
            let count = self.usize32("FM sample value count")?;
            let block = self.block_info()?;
            let expected_count = VALUE_BLOCK_INTS.min(value_count - first_value);
            if stored_first != first_value
                || count != expected_count
                || block.uncompressed_length != count * 4
            {
                return Err(invalid("Invalid FM sample block metadata"));
            }
            validate_canonical_block(block, expected_offset, payload_end)?;
            blocks.push(IntBlockMeta {
                first_value,
                value_count: count,
                block,
            });
            first_value += count;
        }
        if first_value != value_count {
            return Err(invalid("FM sample blocks do not cover all samples"));
        }
        Ok(IntVectorMeta {
            value_count,
            blocks,
        })
    }
}

fn validate_canonical_block(
    block: BlockInfo,
    expected_offset: &mut u64,
    payload_end: u64,
) -> io::Result<()> {
    validate_block(block, payload_end, MAX_DATA_BLOCK_UNCOMPRESSED_LENGTH)?;
    if block.offset != *expected_offset {
        return Err(invalid(
            "FM payload blocks are aliased, reordered, or contain gaps",
        ));
    }
    *expected_offset = expected_offset
        .checked_add(block.stored_length as u64)
        .ok_or_else(|| invalid("FM payload offset overflow"))?;
    Ok(())
}

pub(crate) fn levels_for_alphabet(alphabet_size: usize) -> io::Result<usize> {
    if !(FIRST_BYTE_SYMBOL as usize..=MAX_ALPHABET_SIZE).contains(&alphabet_size) {
        return Err(invalid_input("Invalid FM alphabet size"));
    }
    let highest = alphabet_size - 1;
    let bits = usize::BITS as usize - highest.leading_zeros() as usize;
    Ok(1.max(bits.div_ceil(2)))
}

fn expected_digit_counts(cumulative: &[usize], shift: usize) -> [usize; 4] {
    let mut counts = [0usize; 4];
    for symbol in 0..cumulative.len() - 1 {
        counts[(symbol >> shift) & 3] += cumulative[symbol + 1] - cumulative[symbol];
    }
    counts
}

fn wavelet_starts(cumulative: &[usize], level_count: usize) -> io::Result<Vec<usize>> {
    let alphabet_size = cumulative.len() - 1;
    let mut starts = vec![0usize; alphabet_size];
    let mut offset = 0usize;
    for order in 0..1usize << (level_count * 2) {
        let mut value = order;
        let mut symbol = 0usize;
        for _ in 0..level_count {
            symbol = (symbol << 2) | (value & 3);
            value >>= 2;
        }
        if symbol < alphabet_size {
            starts[symbol] = offset;
            offset += cumulative[symbol + 1] - cumulative[symbol];
        }
    }
    if offset != cumulative[alphabet_size] {
        return Err(invalid("Invalid FM wavelet starts"));
    }
    Ok(starts)
}

pub(crate) fn words_for_bits(bit_length: usize) -> usize {
    bit_length.div_ceil(64)
}

pub(crate) fn words_for_quads(value_length: usize) -> usize {
    value_length.div_ceil(QUAD_VALUES_PER_WORD)
}

fn encoded_bit_block_length(bit_count: usize) -> usize {
    let words = words_for_bits(bit_count);
    let prefixes = words.div_ceil(64) + 1;
    (2 + prefixes) * 4 + words * 8
}

fn encoded_quad_block_length(value_count: usize) -> usize {
    let words = words_for_quads(value_count);
    let prefixes = words.div_ceil(64) + 1;
    (2 + prefixes * 4) * 4 + words * 8
}

fn count_digit(word: u64, digit: usize, valid_values: usize) -> usize {
    if valid_values == 0 {
        return 0;
    }
    let repeated = digit as u64 * 0x5555_5555_5555_5555;
    let different = word ^ repeated;
    let matches = !(different | (different >> 1)) & 0x5555_5555_5555_5555;
    (matches & low_quad_mask(valid_values)).count_ones() as usize
}

fn low_quad_mask(values: usize) -> u64 {
    if values >= QUAD_VALUES_PER_WORD {
        0x5555_5555_5555_5555
    } else {
        0x5555_5555_5555_5555 & ((1u64 << (values * 2)) - 1)
    }
}

pub(crate) fn validate_sample_rate(sample_rate: usize) -> io::Result<()> {
    if sample_rate == 0 || sample_rate > 1024 || !sample_rate.is_power_of_two() {
        return Err(invalid_input(
            "FM SA sample rate must be a power of two in [1, 1024]",
        ));
    }
    Ok(())
}

fn to_u32(value: usize, context: &str) -> io::Result<u32> {
    u32::try_from(value).map_err(|_| invalid_input(format!("{context} exceeds u32")))
}

fn put_usize32(out: &mut Vec<u8>, value: usize, context: &str) -> io::Result<()> {
    put_u32(out, to_u32(value, context)?);
    Ok(())
}

fn put_i32(out: &mut Vec<u8>, value: i32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_u32_at(out: &mut [u8], offset: usize, value: u32) {
    out[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}

fn write_u64_at(out: &mut [u8], offset: usize, value: u64) {
    out[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
}

struct Decoder<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take<const N: usize>(&mut self) -> io::Result<[u8; N]> {
        let end = self
            .position
            .checked_add(N)
            .ok_or_else(|| invalid("FM metadata position overflow"))?;
        let slice = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "Truncated FM metadata"))?;
        self.position = end;
        Ok(slice.try_into().unwrap())
    }

    fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_be_bytes(self.take()?))
    }

    fn i32(&mut self) -> io::Result<i32> {
        Ok(i32::from_be_bytes(self.take()?))
    }

    fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_be_bytes(self.take()?))
    }

    fn usize32(&mut self, context: &str) -> io::Result<usize> {
        usize::try_from(self.u32()?).map_err(|_| invalid(format!("{context} exceeds usize")))
    }

    fn finish(&self) -> io::Result<()> {
        if self.position != self.bytes.len() {
            return Err(invalid("FM metadata contains trailing bytes"));
        }
        Ok(())
    }
}

pub(crate) type SharedFileRead = Arc<dyn FileRead>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc_contract_matches_java() {
        assert_eq!(
            compute_crc32(b"123456789", BlockCompressionType::None),
            0x00c4_9e49
        );
    }
}
