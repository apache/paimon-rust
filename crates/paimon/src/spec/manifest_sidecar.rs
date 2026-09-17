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

//! Binary sidecar indexes for pruning complete Avro manifest blocks.
//!
//! The format is compatible with Java Paimon's `ManifestSidecar` version 1.
//! Coverage for partitions, row IDs, and buckets is independent: unavailable
//! coverage in one dimension never disables pruning by the other dimensions.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::ops::Range;

use bytes::Bytes;
use indexmap::IndexMap;

use crate::io::{FileIO, FileRead};
use crate::spec::avro::ocf::parse_ocf_streaming;
use crate::spec::{ManifestEntry, ManifestFileMeta};
use crate::table::RowRange;
use crate::{Error, Result};

/// Suffix used by manifest sidecar files.
pub const MANIFEST_SIDECAR_SUFFIX: &str = ".avro.sidecar";

const MAGIC: &[u8; 4] = b"PMSC";
const FORMAT_VERSION: u64 = 1;
const MIN_HEADER_BYTES: usize = 29;
const CHECKSUM_BYTES: usize = 4;
const MIN_BLOCK_BYTES: usize = 6;
const MAX_INT: u64 = i32::MAX as u64;
const MAX_LONG: u64 = i64::MAX as u64;
const BLOCK_READ_BUFFER_BYTES: u64 = 4 * 1024 * 1024;

type RowBlockFilter<'a> = dyn Fn(i64, i64) -> bool + Sync + 'a;
type PartitionBlockFilter<'a> = dyn FnMut(&[u8]) -> bool + Send + 'a;
type BucketBlockFilter<'a> = dyn FnMut(i32, i32) -> bool + Send + 'a;

/// Original file offset/length and zero-based manifest entry ordinal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestSidecarBlock {
    offset: u64,
    length: u64,
    first_record: u64,
    record_count: u64,
}

impl ManifestSidecarBlock {
    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn length(&self) -> u64 {
        self.length
    }

    pub fn first_record(&self) -> u64 {
        self.first_record
    }

    pub fn record_count(&self) -> u64 {
        self.record_count
    }
}

/// Selected blocks in original file order. An empty block list excludes the manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestSidecarSelection {
    header: Bytes,
    blocks: Vec<ManifestSidecarBlock>,
}

impl ManifestSidecarSelection {
    pub fn header(&self) -> &[u8] {
        &self.header
    }

    pub fn blocks(&self) -> &[ManifestSidecarBlock] {
        &self.blocks
    }
}

#[derive(Debug)]
struct IndexedBlock {
    block: ManifestSidecarBlock,
    partitions: Vec<u8>,
    row_ids: Vec<u8>,
    buckets: Vec<u8>,
}

/// Incremental builder for a complete manifest block directory.
#[derive(Debug)]
pub struct ManifestSidecarBuilder {
    row_id_enabled: bool,
    bucket_enabled: bool,
    header: Vec<u8>,
    ranges: BTreeMap<u64, u64>,
    dictionary: IndexMap<Vec<u8>, usize>,
    partition_ids: BTreeSet<usize>,
    bucket_pairs: BTreeSet<(i32, i32)>,
    blocks: Vec<IndexedBlock>,
    next_offset: u64,
    next_record: u64,
    current: Option<ManifestSidecarBlock>,
    entries_in_block: u64,
    row_available: bool,
    partition_available: bool,
    bucket_available: bool,
}

impl ManifestSidecarBuilder {
    pub fn new(header: Vec<u8>, row_id_enabled: bool, bucket_enabled: bool) -> Self {
        let next_offset = header.len() as u64;
        Self {
            row_id_enabled,
            bucket_enabled,
            header,
            ranges: BTreeMap::new(),
            dictionary: IndexMap::new(),
            partition_ids: BTreeSet::new(),
            bucket_pairs: BTreeSet::new(),
            blocks: Vec::new(),
            next_offset,
            next_record: 0,
            current: None,
            entries_in_block: 0,
            row_available: false,
            partition_available: false,
            bucket_available: false,
        }
    }

    pub fn begin_block(&mut self, offset: u64, length: u64, records: u64) -> Result<()> {
        require(self.current.is_none() && offset == self.next_offset && length > 0 && records > 0)?;
        self.current = Some(ManifestSidecarBlock {
            offset,
            length,
            first_record: self.next_record,
            record_count: records,
        });
        self.entries_in_block = 0;
        self.row_available = self.row_id_enabled;
        self.partition_available = true;
        self.bucket_available = self.bucket_enabled;
        self.ranges.clear();
        self.partition_ids.clear();
        self.bucket_pairs.clear();
        Ok(())
    }

    pub fn add(
        &mut self,
        first_row_id: Option<i64>,
        row_count: i64,
        partition: Option<&[u8]>,
        bucket: Option<i32>,
        total_buckets: Option<i32>,
    ) -> Result<()> {
        require(self.current.is_some())?;
        self.entries_in_block = self
            .entries_in_block
            .checked_add(1)
            .ok_or_else(invalid_sidecar)?;
        self.add_partition(partition);
        self.add_bucket(bucket, total_buckets);
        self.add_row_range(first_row_id, row_count);
        Ok(())
    }

    fn add_partition(&mut self, partition: Option<&[u8]>) {
        if !self.partition_available {
            return;
        }
        let Some(partition) = partition else {
            self.partition_available = false;
            self.partition_ids.clear();
            return;
        };
        let id = if let Some(id) = self.dictionary.get_index_of(partition) {
            id
        } else {
            let id = self.dictionary.len();
            self.dictionary.insert(partition.to_vec(), id);
            id
        };
        self.partition_ids.insert(id);
    }

    fn add_bucket(&mut self, bucket: Option<i32>, total_buckets: Option<i32>) {
        if !self.bucket_available {
            return;
        }
        let (Some(bucket), Some(total_buckets)) = (bucket, total_buckets) else {
            self.bucket_available = false;
            self.bucket_pairs.clear();
            return;
        };
        if bucket < 0 || total_buckets <= bucket {
            self.bucket_available = false;
            self.bucket_pairs.clear();
            return;
        }
        self.bucket_pairs.insert((bucket, total_buckets));
    }

    fn add_row_range(&mut self, first_row_id: Option<i64>, row_count: i64) {
        if !self.row_available {
            return;
        }
        let Some(first_row_id) = first_row_id.filter(|first| *first >= 0) else {
            self.row_available = false;
            self.ranges.clear();
            return;
        };
        let Some(end) = row_count
            .checked_sub(1)
            .filter(|_| row_count > 0)
            .and_then(|delta| first_row_id.checked_add(delta))
        else {
            self.row_available = false;
            self.ranges.clear();
            return;
        };

        let mut start = first_row_id as u64;
        let mut end = end as u64;
        if let Some((&before_start, &before_end)) = self.ranges.range(..=start).next_back() {
            if before_end.saturating_add(1) >= start {
                start = before_start;
                end = end.max(before_end);
                self.ranges.remove(&before_start);
            }
        }
        loop {
            let next = self
                .ranges
                .range(start..)
                .next()
                .map(|(&next_start, &next_end)| (next_start, next_end));
            let Some((next_start, next_end)) = next else {
                break;
            };
            if next_start > end.saturating_add(1) {
                break;
            }
            end = end.max(next_end);
            self.ranges.remove(&next_start);
        }
        self.ranges.insert(start, end);
    }

    pub fn end_block(&mut self) -> Result<()> {
        let block = self.current.ok_or_else(invalid_sidecar)?;
        require(self.entries_in_block == block.record_count)?;
        let partitions = if self.partition_available {
            encode_deltas(
                self.partition_ids.iter().map(|id| *id as u64),
                self.partition_ids.len(),
                0,
                false,
            )?
        } else {
            Vec::new()
        };
        let row_ids = if self.row_available {
            self.encode_row_ranges()?
        } else {
            Vec::new()
        };
        let buckets = if self.bucket_available {
            self.encode_buckets()?
        } else {
            Vec::new()
        };
        self.blocks.push(IndexedBlock {
            block,
            partitions,
            row_ids,
            buckets,
        });
        self.next_offset = block
            .offset
            .checked_add(block.length)
            .ok_or_else(invalid_sidecar)?;
        self.next_record = block
            .first_record
            .checked_add(block.record_count)
            .ok_or_else(invalid_sidecar)?;
        self.ranges.clear();
        self.partition_ids.clear();
        self.bucket_pairs.clear();
        self.current = None;
        Ok(())
    }

    fn encode_row_ranges(&self) -> Result<Vec<u8>> {
        let (&minimum, _) = self.ranges.first_key_value().ok_or_else(invalid_sidecar)?;
        let maximum = *self
            .ranges
            .last_key_value()
            .map(|(_, end)| end)
            .ok_or_else(invalid_sidecar)?;
        let mut out = Vec::new();
        out.extend_from_slice(&(minimum as i64).to_be_bytes());
        out.extend_from_slice(&(maximum as i64).to_be_bytes());

        let range_count = self.ranges.len();
        let mut endpoints = Vec::with_capacity(range_count.saturating_sub(1) * 2);
        for (index, (&start, &end)) in self.ranges.iter().enumerate() {
            if index > 0 {
                endpoints.push(start);
            }
            if index + 1 < range_count {
                endpoints.push(end);
            }
        }
        out.extend(encode_deltas(
            endpoints,
            range_count.saturating_sub(1) * 2,
            minimum,
            false,
        )?);
        Ok(out)
    }

    fn encode_buckets(&self) -> Result<Vec<u8>> {
        let mut out = encode_deltas(
            self.bucket_pairs.iter().map(|(bucket, _)| *bucket as u64),
            self.bucket_pairs.len(),
            0,
            false,
        )?;
        out.extend(encode_deltas(
            self.bucket_pairs.iter().map(|(_, total)| *total as u64),
            self.bucket_pairs.len(),
            0,
            true,
        )?);
        Ok(out)
    }

    pub fn serialize(&self, file_size: u64, entry_count: u64) -> Result<Vec<u8>> {
        require(
            self.current.is_none()
                && self.next_offset == file_size
                && self.next_record == entry_count,
        )?;
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        encode_varint(FORMAT_VERSION, &mut out)?;
        encode_varint(self.header.len() as u64, &mut out)?;
        out.extend_from_slice(&self.header);
        encode_varint(self.dictionary.len() as u64, &mut out)?;
        for partition in self.dictionary.keys() {
            encode_varint(partition.len() as u64, &mut out)?;
            out.extend_from_slice(partition);
        }
        encode_varint(self.blocks.len() as u64, &mut out)?;
        for indexed in &self.blocks {
            encode_varint(indexed.block.offset, &mut out)?;
            encode_varint(indexed.block.length, &mut out)?;
            encode_varint(indexed.block.record_count, &mut out)?;
            write_payload(&mut out, &indexed.partitions)?;
            write_payload(&mut out, &indexed.row_ids)?;
            write_payload(&mut out, &indexed.buckets)?;
        }
        out.extend_from_slice(&crc32fast::hash(&out).to_be_bytes());
        Ok(out)
    }
}

/// Codec and I/O helpers for version-1 manifest sidecars.
pub struct ManifestSidecar;

impl ManifestSidecar {
    pub fn path(manifest_path: &str) -> String {
        format!("{manifest_path}{MANIFEST_SIDECAR_SUFFIX}")
    }

    /// Return the explicitly published sidecar name. Readers never probe a derived path.
    pub fn file_name(manifest: &ManifestFileMeta) -> Option<&str> {
        manifest
            .extra_files()?
            .iter()
            .find(|name| name.ends_with(MANIFEST_SIDECAR_SUFFIX))
            .map(String::as_str)
    }

    /// Build a sidecar from final OCF bytes and their corresponding manifest entries.
    pub fn build(
        avro_bytes: &[u8],
        entries: &[ManifestEntry],
        row_id_enabled: bool,
        bucket_enabled: bool,
    ) -> Result<Vec<u8>> {
        let (header, mut blocks) = parse_ocf_streaming(avro_bytes)?;
        let mut builder = ManifestSidecarBuilder::new(
            avro_bytes[..header.encoded_len].to_vec(),
            row_id_enabled,
            bucket_enabled,
        );
        let mut position = 0usize;
        while let Some(block) = blocks.next_block()? {
            let end = position
                .checked_add(block.object_count)
                .ok_or_else(invalid_sidecar)?;
            require(end <= entries.len())?;
            builder.begin_block(
                block.encoded_offset as u64,
                block.encoded_len as u64,
                block.object_count as u64,
            )?;
            for entry in &entries[position..end] {
                let partition = if entry.partition().is_empty() || entry.partition() == [0, 0, 0, 0]
                {
                    crate::spec::EMPTY_SERIALIZED_ROW.as_slice()
                } else {
                    entry.partition()
                };
                builder.add(
                    if row_id_enabled {
                        entry.file().first_row_id
                    } else {
                        None
                    },
                    if row_id_enabled {
                        entry.file().row_count
                    } else {
                        0
                    },
                    Some(partition),
                    bucket_enabled.then(|| entry.bucket()),
                    bucket_enabled.then(|| entry.total_buckets()),
                )?;
            }
            builder.end_block()?;
            position = end;
        }
        require(position == entries.len())?;
        builder.serialize(avro_bytes.len() as u64, entries.len() as u64)
    }

    /// Validate a sidecar and select blocks using row-ID ranges.
    pub fn select(
        data: &[u8],
        manifest: &ManifestFileMeta,
        row_ranges: Option<&[RowRange]>,
    ) -> Result<ManifestSidecarSelection> {
        let query = row_ranges.map(RowRangeQuery::new);
        let row_filter = |start, end| {
            query
                .as_ref()
                .is_some_and(|query| query.intersects(start, end))
        };
        select_with_filters(
            data,
            manifest,
            query
                .as_ref()
                .map(|_| &row_filter as &(dyn Fn(i64, i64) -> bool + Sync)),
            None,
            None,
            None,
        )
    }

    /// Read and validate the explicitly referenced sidecar. Any I/O or format error
    /// returns `None`, allowing the caller to fall back to a full manifest read.
    pub async fn read(
        file_io: &FileIO,
        manifest_path: &str,
        manifest: &ManifestFileMeta,
        row_ranges: Option<&[RowRange]>,
    ) -> Option<ManifestSidecarSelection> {
        let query = row_ranges.map(RowRangeQuery::new);
        let row_filter = |start, end| {
            query
                .as_ref()
                .is_some_and(|query| query.intersects(start, end))
        };
        Self::read_with_filters(
            file_io,
            manifest_path,
            manifest,
            query
                .as_ref()
                .map(|_| &row_filter as &(dyn Fn(i64, i64) -> bool + Sync)),
            None,
            None,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn read_with_filters(
        file_io: &FileIO,
        manifest_path: &str,
        manifest: &ManifestFileMeta,
        row_filter: Option<&RowBlockFilter<'_>>,
        partition_filter: Option<&mut PartitionBlockFilter<'_>>,
        partition_arity: Option<usize>,
        bucket_filter: Option<&mut BucketBlockFilter<'_>>,
    ) -> Option<ManifestSidecarSelection> {
        let name = Self::file_name(manifest)?;
        let sidecar_path = sibling_path(manifest_path, name);
        let result = async {
            let input = file_io.new_input(&sidecar_path)?;
            let data = input.read().await?;
            select_with_filters(
                &data,
                manifest,
                row_filter,
                partition_filter,
                partition_arity,
                bucket_filter,
            )
        }
        .await;
        match result {
            Ok(selection) => Some(selection),
            Err(error) => {
                log::debug!(
                    "Cannot use manifest sidecar for {}; reading full manifest: {}",
                    manifest_path,
                    error
                );
                None
            }
        }
    }

    /// Read an OCF stream composed of the original header and selected complete blocks.
    pub async fn read_selected_bytes(
        file_io: &FileIO,
        manifest_path: &str,
        selection: &ManifestSidecarSelection,
    ) -> Result<Bytes> {
        if selection.blocks.is_empty() {
            return Ok(selection.header.clone());
        }
        let reader = file_io.new_input(manifest_path)?.reader().await?;
        let total_block_bytes = selection.blocks.iter().try_fold(0usize, |total, block| {
            let length = usize::try_from(block.length).map_err(|_| invalid_sidecar())?;
            total.checked_add(length).ok_or_else(invalid_sidecar)
        })?;
        let mut out = Vec::with_capacity(
            selection
                .header
                .len()
                .checked_add(total_block_bytes)
                .ok_or_else(invalid_sidecar)?,
        );
        out.extend_from_slice(&selection.header);

        let mut block_index = 0usize;
        while block_index < selection.blocks.len() {
            let first = selection.blocks[block_index];
            let mut span_end = first
                .offset
                .checked_add(first.length)
                .ok_or_else(invalid_sidecar)?;
            block_index += 1;
            while block_index < selection.blocks.len() {
                let next = selection.blocks[block_index];
                let Some(next_end) = next.offset.checked_add(next.length) else {
                    return Err(invalid_sidecar());
                };
                if next.offset != span_end
                    || next_end.saturating_sub(first.offset) > BLOCK_READ_BUFFER_BYTES
                {
                    break;
                }
                span_end = next_end;
                block_index += 1;
            }

            let mut position = first.offset;
            while position < span_end {
                let end = span_end.min(position + BLOCK_READ_BUFFER_BYTES);
                let bytes = reader
                    .read(Range {
                        start: position,
                        end,
                    })
                    .await?;
                require(bytes.len() as u64 == end - position)?;
                out.extend_from_slice(&bytes);
                position = end;
            }
        }
        Ok(Bytes::from(out))
    }
}

struct RowRangeQuery {
    starts: Vec<i64>,
    ends: Vec<i64>,
}

impl RowRangeQuery {
    fn new(ranges: &[RowRange]) -> Self {
        let ranges = crate::table::merge_row_ranges(ranges.to_vec());
        Self {
            starts: ranges.iter().map(RowRange::from).collect(),
            ends: ranges.iter().map(RowRange::to).collect(),
        }
    }
}

impl RowRangeQuery {
    fn intersects(&self, start: i64, end: i64) -> bool {
        let candidate = self.ends.partition_point(|candidate| *candidate < start);
        candidate < self.starts.len() && self.starts[candidate] <= end
    }
}

#[allow(clippy::too_many_arguments)]
fn select_with_filters(
    data: &[u8],
    manifest: &ManifestFileMeta,
    row_filter: Option<&RowBlockFilter<'_>>,
    mut partition_filter: Option<&mut PartitionBlockFilter<'_>>,
    partition_arity: Option<usize>,
    mut bucket_filter: Option<&mut BucketBlockFilter<'_>>,
) -> Result<ManifestSidecarSelection> {
    require(data.len() >= MIN_HEADER_BYTES + CHECKSUM_BYTES)?;
    let payload_end = data.len() - CHECKSUM_BYTES;
    let expected_checksum = u32::from_be_bytes(data[payload_end..].try_into().unwrap());
    require(crc32fast::hash(&data[..payload_end]) == expected_checksum)?;

    let mut input = Cursor::new(&data[..payload_end]);
    require(input.take(4)? == MAGIC)?;
    require(input.varint(MAX_INT)? == FORMAT_VERSION)?;
    let entries = manifest
        .num_added_files()
        .checked_add(manifest.num_deleted_files())
        .filter(|entries| *entries >= 0)
        .ok_or_else(invalid_sidecar)? as u64;
    let manifest_size = u64::try_from(manifest.file_size()).map_err(|_| invalid_sidecar())?;

    let header_length = usize::try_from(input.varint(MAX_INT)?).map_err(|_| invalid_sidecar())?;
    require(
        header_length >= 21
            && header_length <= input.remaining().saturating_sub(2)
            && header_length as u64 <= manifest_size,
    )?;
    let header = input.take(header_length)?;
    require(header.starts_with(b"Obj\x01"))?;

    let partition_count = usize::try_from(input.varint(MAX_INT)?).map_err(|_| invalid_sidecar())?;
    require(partition_count <= input.remaining() / 13)?;
    let mut partition_matches = partition_filter
        .as_ref()
        .map(|_| Vec::with_capacity(partition_count));
    let mut unique = HashSet::with_capacity(partition_count);
    for _ in 0..partition_count {
        let length = usize::try_from(input.varint(MAX_INT)?).map_err(|_| invalid_sidecar())?;
        require(length >= 12 && length <= input.remaining())?;
        let partition = input.take(length)?;
        let arity = i32::from_be_bytes(partition[..4].try_into().unwrap());
        require(arity >= 0)?;
        let minimum_length = 4u64
            .checked_add(((arity as u64 + 71) / 64) * 8)
            .and_then(|size| size.checked_add(arity as u64 * 8))
            .ok_or_else(invalid_sidecar)?;
        require(minimum_length <= length as u64)?;
        require(partition_arity.is_none_or(|expected| expected == arity as usize))?;
        require(unique.insert(partition))?;
        if let (Some(matches), Some(filter)) =
            (partition_matches.as_mut(), partition_filter.as_mut())
        {
            matches.push((*filter)(partition));
        }
    }

    let block_count = usize::try_from(input.varint(MAX_INT)?).map_err(|_| invalid_sidecar())?;
    require(block_count <= input.remaining() / MIN_BLOCK_BYTES)?;
    let mut next_offset = header_length as u64;
    let mut first_record = 0u64;
    let mut selected = Vec::new();
    for _ in 0..block_count {
        require(input.remaining() >= MIN_BLOCK_BYTES)?;
        let offset = input.varint(MAX_LONG)?;
        let length = input.varint(MAX_LONG)?;
        let records = input.varint(MAX_LONG)?;
        require(
            offset == next_offset
                && length > 0
                && offset
                    .checked_add(length)
                    .is_some_and(|end| end <= manifest_size)
                && records > 0
                && first_record
                    .checked_add(records)
                    .is_some_and(|end| end <= entries),
        )?;
        let partition_payload = payload(&mut input)?;
        let row_payload = payload(&mut input)?;
        let bucket_payload = payload(&mut input)?;

        let partition_values = if let Some(payload) = partition_payload {
            let values =
                DeltaReader::new(payload, 0, partition_count.saturating_sub(1) as u64, false)?;
            require(
                values.count > 0
                    && values.count as u64 <= records
                    && values.count <= partition_count,
            )?;
            Some(values)
        } else {
            None
        };

        let row_values = if let Some(payload) = row_payload {
            let mut cursor = Cursor::new(payload);
            require(cursor.remaining() >= 17)?;
            let minimum = cursor.long()?;
            let maximum = cursor.long()?;
            require(minimum >= 0 && minimum <= maximum)?;
            let values = DeltaReader::from_cursor(cursor, minimum as u64, maximum as u64, false)?;
            require(values.count % 2 == 0 && values.count as u64 / 2 < records)?;
            require(values.count != 0 || values.cursor.remaining() == 0)?;
            Some((minimum, maximum, values))
        } else {
            None
        };

        if let Some(payload) = bucket_payload {
            let mut prefix = Cursor::new(payload);
            let pairs = prefix.varint(MAX_INT)?;
            require(
                pairs > 0
                    && pairs <= records
                    && pairs
                        .checked_mul(2)
                        .and_then(|value| value.checked_add(1))
                        .is_some_and(|minimum| minimum <= prefix.remaining() as u64),
            )?;
        }

        let block = ManifestSidecarBlock {
            offset,
            length,
            first_record,
            record_count: records,
        };
        next_offset = offset.checked_add(length).ok_or_else(invalid_sidecar)?;
        first_record = first_record
            .checked_add(records)
            .ok_or_else(invalid_sidecar)?;

        if let (Some(filter), Some((minimum, maximum, mut endpoints))) = (row_filter, row_values) {
            if !filter(minimum, maximum) {
                continue;
            }
            let range_count = endpoints.count / 2 + 1;
            let mut hit = range_count == 1;
            let mut start = minimum;
            for range in 0..range_count {
                if hit {
                    break;
                }
                let end = if range + 1 == range_count {
                    maximum
                } else {
                    endpoints.next()? as i64
                };
                require(end >= start)?;
                hit = filter(start, end);
                if !hit && range + 1 < range_count {
                    start = endpoints.next()? as i64;
                    require(start > end)?;
                }
                require(endpoints.remaining > 0 || endpoints.cursor.remaining() == 0)?;
            }
            if !hit {
                continue;
            }
        }

        if partition_filter.is_some() {
            if let Some(mut ids) = partition_values {
                let mut hit = false;
                let mut previous = None;
                while !hit && ids.remaining > 0 {
                    let id = ids.next()?;
                    require(previous.is_none_or(|previous| id > previous))?;
                    require(ids.remaining > 0 || ids.cursor.remaining() == 0)?;
                    previous = Some(id);
                    hit = partition_matches
                        .as_ref()
                        .and_then(|matches| matches.get(id as usize))
                        .copied()
                        .ok_or_else(invalid_sidecar)?;
                }
                if !hit {
                    continue;
                }
            }
        }

        if let (Some(filter), Some(payload)) = (bucket_filter.as_mut(), bucket_payload) {
            let mut directory = DeltaReader::new(payload, 0, MAX_INT, false)?;
            while directory.remaining > 0 {
                directory.next()?;
            }
            let totals_offset = directory.cursor.position();
            let mut buckets = DeltaReader::new(&payload[..totals_offset], 0, MAX_INT, false)?;
            let mut totals = DeltaReader::new(&payload[totals_offset..], 0, MAX_INT, true)?;
            require(totals.count == buckets.count)?;
            let mut hit = false;
            let mut previous: Option<(i32, i32)> = None;
            while !hit && buckets.remaining > 0 {
                let bucket = i32::try_from(buckets.next()?).map_err(|_| invalid_sidecar())?;
                let total = i32::try_from(totals.next()?).map_err(|_| invalid_sidecar())?;
                require(total > bucket)?;
                require(previous.is_none_or(|previous| (bucket, total) > previous))?;
                require(
                    buckets.remaining > 0
                        || (buckets.cursor.remaining() == 0 && totals.cursor.remaining() == 0),
                )?;
                previous = Some((bucket, total));
                hit = (*filter)(bucket, total);
            }
            if !hit {
                continue;
            }
        }
        selected.push(block);
    }
    require(input.remaining() == 0 && next_offset == manifest_size && first_record == entries)?;
    Ok(ManifestSidecarSelection {
        header: Bytes::copy_from_slice(header),
        blocks: selected,
    })
}

fn sibling_path(path: &str, name: &str) -> String {
    path.rsplit_once('/')
        .map(|(parent, _)| format!("{parent}/{name}"))
        .unwrap_or_else(|| name.to_string())
}

fn write_payload(out: &mut Vec<u8>, payload: &[u8]) -> Result<()> {
    if payload.is_empty() {
        out.push(0);
    } else {
        out.push(1);
        encode_varint(payload.len() as u64, out)?;
        out.extend_from_slice(payload);
    }
    Ok(())
}

fn payload<'a>(input: &mut Cursor<'a>) -> Result<Option<&'a [u8]>> {
    let encoding = input.byte()?;
    if encoding == 0 {
        return Ok(None);
    }
    let length = usize::try_from(input.varint(MAX_INT)?).map_err(|_| invalid_sidecar())?;
    let payload = input.take(length)?;
    Ok((encoding == 1).then_some(payload))
}

fn encode_deltas<I>(values: I, count: usize, mut base: u64, signed: bool) -> Result<Vec<u8>>
where
    I: IntoIterator<Item = u64>,
{
    require(count <= i32::MAX as usize)?;
    let mut out = Vec::new();
    encode_varint(count as u64, &mut out)?;
    let mut actual_count = 0usize;
    for value in values {
        require(value <= if signed { MAX_INT } else { MAX_LONG })?;
        let encoded = if signed {
            let delta = value as i64 - base as i64;
            ((delta << 1) ^ (delta >> 63)) as u64
        } else {
            value.checked_sub(base).ok_or_else(invalid_sidecar)?
        };
        encode_varint(encoded, &mut out)?;
        base = value;
        actual_count += 1;
    }
    require(actual_count == count)?;
    Ok(out)
}

fn encode_varint(mut value: u64, out: &mut Vec<u8>) -> Result<()> {
    require(value <= MAX_LONG)?;
    while value >= 0x80 {
        out.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
    Ok(())
}

#[derive(Clone, Copy)]
struct Cursor<'a> {
    data: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, position: 0 }
    }

    fn position(&self) -> usize {
        self.position
    }

    fn remaining(&self) -> usize {
        self.data.len() - self.position
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(count)
            .filter(|end| *end <= self.data.len())
            .ok_or_else(invalid_sidecar)?;
        let result = &self.data[self.position..end];
        self.position = end;
        Ok(result)
    }

    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn long(&mut self) -> Result<i64> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn varint(&mut self, maximum: u64) -> Result<u64> {
        let mut value = 0u64;
        for index in 0..9 {
            let byte = self.byte()?;
            value |= u64::from(byte & 0x7f) << (index * 7);
            if byte & 0x80 == 0 {
                require((index == 0 || byte != 0) && value <= maximum)?;
                return Ok(value);
            }
        }
        Err(invalid_sidecar())
    }
}

struct DeltaReader<'a> {
    cursor: Cursor<'a>,
    value: u64,
    maximum: u64,
    signed: bool,
    count: usize,
    remaining: usize,
}

impl<'a> DeltaReader<'a> {
    fn new(data: &'a [u8], base: u64, maximum: u64, signed: bool) -> Result<Self> {
        Self::from_cursor(Cursor::new(data), base, maximum, signed)
    }

    fn from_cursor(mut cursor: Cursor<'a>, base: u64, maximum: u64, signed: bool) -> Result<Self> {
        require(base <= maximum && (!signed || maximum <= MAX_INT))?;
        let count = usize::try_from(cursor.varint(MAX_INT)?).map_err(|_| invalid_sidecar())?;
        require(count <= cursor.remaining())?;
        Ok(Self {
            cursor,
            value: base,
            maximum,
            signed,
            count,
            remaining: count,
        })
    }

    fn next(&mut self) -> Result<u64> {
        require(self.remaining > 0)?;
        let encoded = self
            .cursor
            .varint(if self.signed { MAX_INT * 2 } else { MAX_LONG })?;
        let next = if self.signed {
            let delta = ((encoded >> 1) as i64) ^ -((encoded & 1) as i64);
            let value = i64::try_from(self.value).map_err(|_| invalid_sidecar())?;
            value
                .checked_add(delta)
                .filter(|value| *value >= 0 && *value as u64 <= self.maximum)
                .ok_or_else(invalid_sidecar)? as u64
        } else {
            self.value
                .checked_add(encoded)
                .filter(|value| *value <= self.maximum)
                .ok_or_else(invalid_sidecar)?
        };
        self.value = next;
        self.remaining -= 1;
        Ok(next)
    }
}

fn require(valid: bool) -> Result<()> {
    if valid {
        Ok(())
    } else {
        Err(invalid_sidecar())
    }
}

fn invalid_sidecar() -> Error {
    Error::DataInvalid {
        message: "Invalid, unsupported or mismatched manifest sidecar".to_string(),
        source: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::manifest_common::FileKind;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{BinaryRowBuilder, DataFileMeta, MANIFEST_ENTRY_SCHEMA};
    use apache_avro::{to_value, Schema};

    fn header() -> Vec<u8> {
        let mut header =
            b"Obj\x01\x04\x14avro.codec\x08null\x16avro.schema\x0c\"long\"\x00".to_vec();
        header.extend_from_slice(&[0; 16]);
        header
    }

    fn meta(size: u64, entries: u64) -> ManifestFileMeta {
        ManifestFileMeta::new(
            "manifest-golden".to_string(),
            size as i64,
            entries as i64,
            0,
            BinaryTableStats::empty(),
            0,
        )
    }

    fn test_sidecar() -> (Vec<u8>, ManifestFileMeta) {
        let header = header();
        let mut builder = ManifestSidecarBuilder::new(header.clone(), true, false);
        for (offset, length, ranges) in [
            (0, 100, vec![(0, 10), (5, 5), (20, 5)]),
            (
                100,
                200,
                vec![((1_i64 << 32) - 2, 5), (8_254_058_425_445, 1)],
            ),
            (300, 100, vec![(20, 5), (i64::MAX, 1)]),
        ] {
            builder
                .begin_block(header.len() as u64 + offset, length, ranges.len() as u64)
                .unwrap();
            for (first, count) in ranges {
                builder.add(Some(first), count, None, None, None).unwrap();
            }
            builder.end_block().unwrap();
        }
        let size = header.len() as u64 + 400;
        (builder.serialize(size, 7).unwrap(), meta(size, 7))
    }

    fn partition(p: i32, q: Option<&str>) -> Vec<u8> {
        let mut builder = BinaryRowBuilder::new(2);
        builder.write_int(0, p);
        if let Some(q) = q {
            builder.write_string_inline(1, q);
        } else {
            builder.set_null_at(1);
        }
        builder.build_serialized()
    }

    fn manifest_entry(first_row_id: i64, bucket: i32, total_buckets: i32) -> ManifestEntry {
        ManifestEntry::new(
            FileKind::Add,
            crate::spec::EMPTY_SERIALIZED_ROW.clone(),
            bucket,
            total_buckets,
            DataFileMeta {
                file_name: format!("data-{first_row_id}.parquet"),
                file_size: 100,
                row_count: 10,
                min_key: Vec::new(),
                max_key: Vec::new(),
                key_stats: BinaryTableStats::empty(),
                value_stats: BinaryTableStats::empty(),
                min_sequence_number: 0,
                max_sequence_number: 0,
                schema_id: 0,
                level: 0,
                extra_files: Vec::new(),
                creation_time: None,
                delete_row_count: None,
                embedded_index: None,
                file_source: None,
                value_stats_cols: None,
                external_path: None,
                first_row_id: Some(first_row_id),
                write_cols: None,
                column_max_sequence_numbers: None,
            },
            2,
        )
    }

    #[test]
    fn builder_matches_java_v1_crc_and_layout() {
        let header = header();
        let a = partition(7, Some("left"));
        let b = partition(9, None);
        let mut builder = ManifestSidecarBuilder::new(header.clone(), true, true);
        for (offset, length, entries) in [
            (
                0,
                100,
                vec![(0, 10, &a, 1, 4), (5, 5, &a, 1, 4), (20, 5, &b, 1, 8)],
            ),
            (
                100,
                200,
                vec![
                    ((1_i64 << 32) - 2, 5, &b, 2, 4),
                    (8_254_058_425_445, 1, &a, 2, 8),
                ],
            ),
            (300, 100, vec![(20, 5, &a, 0, 1), (i64::MAX, 1, &b, 3, 4)]),
        ] {
            builder
                .begin_block(header.len() as u64 + offset, length, entries.len() as u64)
                .unwrap();
            for (first, count, partition, bucket, total_buckets) in entries {
                builder
                    .add(
                        Some(first),
                        count,
                        Some(partition),
                        Some(bucket),
                        Some(total_buckets),
                    )
                    .unwrap();
            }
            builder.end_block().unwrap();
        }
        let sidecar = builder.serialize(header.len() as u64 + 400, 7).unwrap();
        assert_eq!(
            u32::from_be_bytes(sidecar[sidecar.len() - 4..].try_into().unwrap()),
            0xdf82_cd30
        );
        assert_eq!(
            ManifestSidecar::select(
                &sidecar,
                &meta(header.len() as u64 + 400, 7),
                Some(&[RowRange::new(20, 20)]),
            )
            .unwrap()
            .blocks()
            .iter()
            .map(ManifestSidecarBlock::first_record)
            .collect::<Vec<_>>(),
            vec![0, 5]
        );
    }

    #[test]
    fn build_select_and_decode_real_avro_blocks() {
        let entries = vec![
            manifest_entry(0, 0, 4),
            manifest_entry(100, 1, 4),
            manifest_entry(200, 2, 4),
        ];
        let schema = Schema::parse_str(MANIFEST_ENTRY_SCHEMA).unwrap();
        let mut writer = crate::spec::new_avro_writer(&schema, "zstd", 1).unwrap();
        for entry in &entries {
            let value = to_value(entry).unwrap().resolve(&schema).unwrap();
            writer.append(value).unwrap();
        }
        let avro = writer.into_inner().unwrap();
        let sidecar = ManifestSidecar::build(&avro, &entries, true, true).unwrap();
        let manifest = meta(avro.len() as u64, entries.len() as u64);
        let selection =
            ManifestSidecar::select(&sidecar, &manifest, Some(&[RowRange::new(105, 105)])).unwrap();
        assert_eq!(selection.blocks().len(), 1);

        let mut selected_avro = selection.header().to_vec();
        for block in selection.blocks() {
            selected_avro.extend_from_slice(
                &avro[block.offset() as usize..(block.offset() + block.length()) as usize],
            );
        }
        let decoded =
            crate::spec::avro::from_avro_bytes_fast::<ManifestEntry>(&selected_avro).unwrap();
        assert_eq!(decoded, vec![entries[1].clone()]);
    }

    #[test]
    fn row_id_selection_preserves_exact_gaps_and_ordinals() {
        let (sidecar, meta) = test_sidecar();
        for point in [
            0,
            9,
            20,
            24,
            (1_i64 << 32) - 2,
            1_i64 << 32,
            (1_i64 << 32) + 2,
            8_254_058_425_445,
            i64::MAX,
        ] {
            let ranges = [RowRange::new(point, point)];
            assert!(!ManifestSidecar::select(&sidecar, &meta, Some(&ranges))
                .unwrap()
                .blocks()
                .is_empty());
        }
        for point in [
            10,
            19,
            25,
            (1_i64 << 32) - 3,
            (1_i64 << 32) + 3,
            8_254_058_425_444,
            i64::MAX - 1,
        ] {
            let ranges = [RowRange::new(point, point)];
            assert!(ManifestSidecar::select(&sidecar, &meta, Some(&ranges))
                .unwrap()
                .blocks()
                .is_empty());
        }

        let selected =
            ManifestSidecar::select(&sidecar, &meta, Some(&[RowRange::new(20, 20)])).unwrap();
        assert_eq!(
            selected
                .blocks()
                .iter()
                .map(ManifestSidecarBlock::first_record)
                .collect::<Vec<_>>(),
            vec![0, 5]
        );
    }

    #[test]
    fn corrupt_and_mismatched_sidecars_are_rejected() {
        let (sidecar, manifest) = test_sidecar();
        for length in 0..sidecar.len() {
            assert!(ManifestSidecar::select(&sidecar[..length], &manifest, None).is_err());
        }
        let mut corrupt = sidecar.clone();
        corrupt[4] = 2;
        let end = corrupt.len() - 4;
        let crc = crc32fast::hash(&corrupt[..end]);
        corrupt[end..].copy_from_slice(&crc.to_be_bytes());
        assert!(ManifestSidecar::select(&corrupt, &manifest, None).is_err());

        let wrong_size = meta(manifest.file_size() as u64 + 1, 7);
        assert!(ManifestSidecar::select(&sidecar, &wrong_size, None).is_err());
    }

    #[test]
    fn unavailable_row_coverage_fails_open() {
        let header = header();
        for (first, count) in [(None, 1), (Some(-1), 1), (Some(10), 0)] {
            let mut builder = ManifestSidecarBuilder::new(header.clone(), true, false);
            builder.begin_block(header.len() as u64, 100, 1).unwrap();
            builder.add(first, count, None, None, None).unwrap();
            builder.end_block().unwrap();
            let bytes = builder.serialize(header.len() as u64 + 100, 1).unwrap();
            assert_eq!(
                ManifestSidecar::select(
                    &bytes,
                    &meta(header.len() as u64 + 100, 1),
                    Some(&[RowRange::new(1_000, 1_000)]),
                )
                .unwrap()
                .blocks()
                .len(),
                1
            );
        }
    }

    #[test]
    fn partition_and_bucket_dimensions_are_independent() {
        let header = header();
        let p0 = crate::spec::EMPTY_SERIALIZED_ROW.clone();
        let p1 = {
            let mut bytes = p0.clone();
            bytes.extend_from_slice(b"distinct");
            bytes
        };
        let mut builder = ManifestSidecarBuilder::new(header.clone(), false, true);
        builder.begin_block(header.len() as u64, 100, 2).unwrap();
        builder.add(None, 0, Some(&p0), Some(1), Some(4)).unwrap();
        builder.add(None, 0, Some(&p1), Some(2), Some(8)).unwrap();
        builder.end_block().unwrap();
        let bytes = builder.serialize(header.len() as u64 + 100, 2).unwrap();
        let mut partition = |bytes: &[u8]| bytes == p0;
        let mut bucket = |bucket: i32, total: i32| bucket == 1 && total == 4;
        let selected = select_with_filters(
            &bytes,
            &meta(header.len() as u64 + 100, 2),
            None,
            Some(&mut partition),
            None,
            Some(&mut bucket),
        )
        .unwrap();
        assert_eq!(selected.blocks().len(), 1);

        let mut bucket_miss = |bucket: i32, total: i32| bucket == 7 && total == 8;
        assert!(select_with_filters(
            &bytes,
            &meta(header.len() as u64 + 100, 2),
            None,
            None,
            None,
            Some(&mut bucket_miss),
        )
        .unwrap()
        .blocks()
        .is_empty());
    }

    #[tokio::test]
    async fn read_falls_back_on_corruption_and_selected_reads_skip_gaps() {
        let (sidecar, manifest) = test_sidecar();
        let manifest = manifest.with_extra_files(Some(vec![format!(
            "manifest-golden{MANIFEST_SIDECAR_SUFFIX}"
        )]));
        let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let manifest_path = "memory:/manifest/manifest-golden";
        let sidecar_path = ManifestSidecar::path(manifest_path);
        file_io
            .new_output(&sidecar_path)
            .unwrap()
            .write(Bytes::from(sidecar.clone()))
            .await
            .unwrap();

        let selection = ManifestSidecar::read(
            &file_io,
            manifest_path,
            &manifest,
            Some(&[RowRange::new(20, 20)]),
        )
        .await
        .unwrap();
        let mut avro = header();
        avro.extend((0..400).map(|value| value as u8));
        file_io
            .new_output(manifest_path)
            .unwrap()
            .write(Bytes::from(avro.clone()))
            .await
            .unwrap();
        let selected = ManifestSidecar::read_selected_bytes(&file_io, manifest_path, &selection)
            .await
            .unwrap();
        let mut expected = header();
        expected.extend_from_slice(&avro[header().len()..header().len() + 100]);
        expected.extend_from_slice(&avro[header().len() + 300..header().len() + 400]);
        assert_eq!(selected.as_ref(), expected);

        let mut corrupt = sidecar;
        corrupt[0] ^= 1;
        file_io
            .new_output(&sidecar_path)
            .unwrap()
            .write(Bytes::from(corrupt))
            .await
            .unwrap();
        assert!(
            ManifestSidecar::read(&file_io, manifest_path, &manifest, None)
                .await
                .is_none()
        );
    }
}
