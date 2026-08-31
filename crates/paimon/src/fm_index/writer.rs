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

use super::format::{
    levels_for_alphabet, validate_sample_rate, words_for_bits, words_for_quads, write_bit_vector,
    write_block, write_container_footer, write_directory, write_index_meta, write_int_vector,
    write_partition_footer, write_quad_vector, Directory, IndexMeta, PartitionMeta,
    FIRST_BYTE_SYMBOL, SEPARATOR, TERMINATOR,
};
use super::suffix_array;
use crate::btree::BlockCompressionType;
use crate::io::FileWrite;
use bytes::Bytes;
use std::io;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FMWriteOptions {
    pub(crate) partition_size: usize,
    pub(crate) partition_row_count: usize,
    pub(crate) sample_rate: usize,
    pub(crate) compression: BlockCompressionType,
    pub(crate) compression_level: i32,
}

impl FMWriteOptions {
    pub(crate) fn validate(self) -> io::Result<Self> {
        if self.partition_size < 2 || self.partition_size >= i32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "FM partition size must be in [2, i32::MAX)",
            ));
        }
        if self.partition_row_count == 0 || self.partition_row_count > i32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "FM partition row count must be in [1, i32::MAX]",
            ));
        }
        validate_sample_rate(self.sample_rate)?;
        Ok(self)
    }
}

impl Default for FMWriteOptions {
    fn default() -> Self {
        Self {
            partition_size: 16 * 1024 * 1024,
            partition_row_count: 100_000,
            sample_rate: 32,
            compression: BlockCompressionType::Lz4,
            compression_level: 1,
        }
    }
}

pub(crate) struct FMWriteResult {
    pub(crate) row_count: u64,
    pub(crate) index_meta: Vec<u8>,
}

pub(crate) struct FMGlobalIndexWriter {
    writer: Box<dyn FileWrite>,
    options: FMWriteOptions,
    file_offset: u64,
    output: Vec<u8>,
    partitions: Vec<PartitionMeta>,
    text: Vec<u16>,
    null_rows: Vec<bool>,
    first_row_id: u64,
    total_row_count: u64,
    partition_first_row_id: u64,
    last_row_id: u64,
    has_last_row_id: bool,
}

impl FMGlobalIndexWriter {
    pub(crate) fn new(writer: Box<dyn FileWrite>, options: FMWriteOptions) -> io::Result<Self> {
        Ok(Self {
            writer,
            options: options.validate()?,
            file_offset: 0,
            output: Vec::new(),
            partitions: Vec::new(),
            text: Vec::new(),
            null_rows: Vec::new(),
            first_row_id: 0,
            total_row_count: 0,
            partition_first_row_id: 0,
            last_row_id: 0,
            has_last_row_id: false,
        })
    }

    pub(crate) async fn write(
        &mut self,
        key: Option<&[u8]>,
        relative_row_id: u64,
    ) -> io::Result<()> {
        if self.has_last_row_id && relative_row_id != self.last_row_id + 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "FM row IDs must be consecutive: previous={}, current={relative_row_id}",
                    self.last_row_id
                ),
            ));
        }
        let encoded_length = key.map_or(1usize, |bytes| bytes.len().saturating_add(1));
        if encoded_length >= self.options.partition_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "A value exceeds fm-index.partition-size ({} encoded symbols)",
                    self.options.partition_size
                ),
            ));
        }
        if !self.null_rows.is_empty()
            && (self.null_rows.len() >= self.options.partition_row_count
                || self.text.len()
                    > self
                        .options
                        .partition_size
                        .saturating_sub(encoded_length)
                        .saturating_sub(1))
        {
            self.flush_partition().await?;
        }

        if !self.has_last_row_id {
            self.first_row_id = relative_row_id;
        }
        if self.null_rows.is_empty() {
            self.partition_first_row_id = relative_row_id;
        }
        self.null_rows.push(key.is_none());
        if let Some(bytes) = key {
            self.text.extend(
                bytes
                    .iter()
                    .map(|value| u16::from(*value) + FIRST_BYTE_SYMBOL),
            );
        }
        self.text.push(SEPARATOR);
        self.total_row_count = self
            .total_row_count
            .checked_add(1)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "FM row count overflow"))?;
        self.last_row_id = relative_row_id;
        self.has_last_row_id = true;
        Ok(())
    }

    pub(crate) async fn finish(mut self) -> io::Result<FMWriteResult> {
        self.flush_partition().await?;
        if self.partitions.is_empty() {
            self.writer
                .close()
                .await
                .map_err(|error| io::Error::other(error.to_string()))?;
            return Ok(FMWriteResult {
                row_count: 0,
                index_meta: Vec::new(),
            });
        }
        let meta = IndexMeta {
            first_row_id: self.first_row_id,
            row_count: self.total_row_count,
            partitions: self.partitions,
        };
        let index_meta = write_index_meta(&meta)?;
        let directory = write_block(
            &mut self.output,
            self.file_offset,
            &index_meta,
            self.options.compression,
            self.options.compression_level,
        )?;
        write_container_footer(
            &mut self.output,
            directory,
            self.first_row_id,
            self.total_row_count,
            meta.partitions.len(),
        )?;
        self.writer
            .write(Bytes::from(self.output))
            .await
            .map_err(|error| io::Error::other(error.to_string()))?;
        self.writer
            .close()
            .await
            .map_err(|error| io::Error::other(error.to_string()))?;
        Ok(FMWriteResult {
            row_count: self.total_row_count,
            index_meta,
        })
    }

    async fn flush_partition(&mut self) -> io::Result<()> {
        if self.null_rows.is_empty() {
            return Ok(());
        }
        debug_assert!(self.output.is_empty());
        let partition_start = self.file_offset;
        self.text.push(TERMINATOR);
        let symbols = std::mem::take(&mut self.text);
        let null_rows = std::mem::take(&mut self.null_rows);
        let options = self.options;
        let partition_first_row_id = self.partition_first_row_id;
        let (output, partition) = tokio::task::spawn_blocking(move || {
            encode_partition(
                symbols,
                null_rows,
                options,
                partition_start,
                partition_first_row_id,
            )
        })
        .await
        .map_err(|error| {
            io::Error::other(format!("FM partition encoding task failed: {error}"))
        })??;
        let partition_end = partition.end_offset;
        self.writer
            .write(Bytes::from(output))
            .await
            .map_err(|error| io::Error::other(error.to_string()))?;
        self.partitions.push(partition);
        self.file_offset = partition_end;
        Ok(())
    }
}

fn encode_partition(
    mut symbols: Vec<u16>,
    null_row_values: Vec<bool>,
    options: FMWriteOptions,
    partition_start: u64,
    partition_first_row_id: u64,
) -> io::Result<(Vec<u8>, PartitionMeta)> {
    let (alphabet_size, byte_to_symbol) = densify(&mut symbols);
    let suffix_array = suffix_array::build(&symbols, alphabet_size - 1)?;
    let mut output = Vec::new();

    let mut bwt = vec![0u16; symbols.len()];
    let mut sampled_words = vec![0u64; words_for_bits(symbols.len())];
    let mut sample_values = Vec::with_capacity((symbols.len() - 1) / options.sample_rate + 1);
    for (row, suffix) in suffix_array.into_iter().enumerate() {
        bwt[row] = symbols[if suffix == 0 {
            symbols.len() - 1
        } else {
            suffix - 1
        }];
        if suffix % options.sample_rate == 0 {
            sampled_words[row >> 6] |= 1u64 << (row & 63);
            sample_values.push(suffix);
        }
    }
    let expected_samples = (symbols.len() - 1) / options.sample_rate + 1;
    if sample_values.len() != expected_samples {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "FM sampled suffix count is inconsistent",
        ));
    }

    let cumulative_counts = cumulative_counts(&symbols, alphabet_size);
    let level_count = levels_for_alphabet(alphabet_size)?;
    let mut digit_starts = Vec::with_capacity(level_count);
    let mut wavelets = Vec::with_capacity(level_count);
    let mut current = bwt;
    let mut reordered = vec![0u16; current.len()];
    for level in 0..level_count {
        let shift = (level_count - level - 1) * 2;
        let mut quads = vec![0u64; words_for_quads(current.len())];
        let mut counts = [0usize; 4];
        for (i, symbol) in current.iter().enumerate() {
            let digit = (usize::from(*symbol) >> shift) & 3;
            counts[digit] += 1;
            quads[i >> 5] |= (digit as u64) << ((i & 31) * 2);
        }
        let mut starts = [0usize; 4];
        let mut next = 0usize;
        for digit in 0..4 {
            starts[digit] = next;
            next += counts[digit];
        }
        let mut positions = starts;
        for symbol in &current {
            let digit = (usize::from(*symbol) >> shift) & 3;
            reordered[positions[digit]] = *symbol;
            positions[digit] += 1;
        }
        wavelets.push(write_quad_vector(
            &mut output,
            partition_start,
            &quads,
            current.len(),
            options.compression,
            options.compression_level,
        )?);
        digit_starts.push(starts);
        std::mem::swap(&mut current, &mut reordered);
    }

    let sampled_rows = write_bit_vector(
        &mut output,
        partition_start,
        &sampled_words,
        symbols.len(),
        options.compression,
        options.compression_level,
    )?;
    let sample_values = write_int_vector(
        &mut output,
        partition_start,
        &sample_values,
        options.compression,
        options.compression_level,
    )?;
    let mut null_words = vec![0u64; words_for_bits(null_row_values.len())];
    for (row, is_null) in null_row_values.iter().enumerate() {
        if *is_null {
            null_words[row >> 6] |= 1u64 << (row & 63);
        }
    }
    let null_rows = write_bit_vector(
        &mut output,
        partition_start,
        &null_words,
        null_row_values.len(),
        options.compression,
        options.compression_level,
    )?;
    let mut boundary_words = vec![0u64; words_for_bits(symbols.len())];
    for (position, symbol) in symbols.iter().enumerate() {
        if *symbol == SEPARATOR {
            boundary_words[position >> 6] |= 1u64 << (position & 63);
        }
    }
    let row_boundaries = write_bit_vector(
        &mut output,
        partition_start,
        &boundary_words,
        symbols.len(),
        options.compression,
        options.compression_level,
    )?;
    let directory = Directory {
        row_count: null_row_values.len(),
        text_length: symbols.len(),
        sample_rate: options.sample_rate,
        level_count,
        alphabet_size,
        byte_to_symbol,
        cumulative_counts,
        wavelet_starts: Vec::new(),
        digit_starts,
        wavelets,
        sampled_rows,
        sample_values,
        null_rows,
        row_boundaries,
    };
    let directory_block = write_directory(
        &mut output,
        partition_start,
        &directory,
        options.compression,
        options.compression_level,
    )?;
    write_partition_footer(
        &mut output,
        directory_block,
        partition_first_row_id,
        directory.row_count,
        directory.text_length,
        directory.sample_rate,
    )?;
    let partition_end = partition_start
        .checked_add(output.len() as u64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "FM file size overflow"))?;
    Ok((
        output,
        PartitionMeta {
            start_offset: partition_start,
            end_offset: partition_end,
            first_row_id: partition_first_row_id,
            row_count: directory.row_count,
        },
    ))
}

fn densify(symbols: &mut [u16]) -> (usize, [i32; 256]) {
    let mut present = [false; 256];
    for symbol in symbols.iter().copied() {
        if symbol >= FIRST_BYTE_SYMBOL {
            present[usize::from(symbol - FIRST_BYTE_SYMBOL)] = true;
        }
    }
    let mut byte_to_symbol = [-1i32; 256];
    let mut alphabet_size = usize::from(FIRST_BYTE_SYMBOL);
    for (byte, is_present) in present.into_iter().enumerate() {
        if is_present {
            byte_to_symbol[byte] = alphabet_size as i32;
            alphabet_size += 1;
        }
    }
    for symbol in symbols {
        if *symbol >= FIRST_BYTE_SYMBOL {
            *symbol = byte_to_symbol[usize::from(*symbol - FIRST_BYTE_SYMBOL)] as u16;
        }
    }
    (alphabet_size, byte_to_symbol)
}

fn cumulative_counts(symbols: &[u16], alphabet_size: usize) -> Vec<usize> {
    let mut counts = vec![0usize; alphabet_size + 1];
    for symbol in symbols {
        counts[usize::from(*symbol) + 1] += 1;
    }
    for i in 1..counts.len() {
        counts[i] += counts[i - 1];
    }
    counts
}
