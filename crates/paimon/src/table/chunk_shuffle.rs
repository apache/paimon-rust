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

//! Deterministic, fixed-row chunk planning for native Python reads.
//!
//! The shuffle intentionally matches `random.Random(seed).shuffle` in CPython.
//! PyPaimon exposed that ordering before native planning existed, so using a
//! different Rust RNG would silently assign different chunks to workers.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

use crate::deletion_vector::{DeletionVector, DeletionVectorFactory};
use crate::spec::{BinaryRow, DataField, DataFileMeta, Datum};
use crate::table::source::{data_evolution_anchor_file, is_data_evolution_normal_file};
use crate::table::stats_filter::group_by_overlapping_row_id;
use crate::table::{merge_row_ranges, DataSplit, DataSplitBuilder, DeletionFile, RowRange, Table};

/// Native chunk-shuffle configuration. The seed is stored as the unsigned
/// little-endian 32-bit words consumed by CPython's MT19937 initializer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChunkShuffle {
    seed_words: Vec<u32>,
    chunk_size: i64,
}

impl ChunkShuffle {
    /// Build from a Python integer's decimal spelling. Negative integers use
    /// their absolute value, matching `random.Random`.
    pub(crate) fn from_decimal_seed(seed: &str, chunk_size: u64) -> crate::Result<Self> {
        let chunk_size = i64::try_from(chunk_size).map_err(|_| crate::Error::DataInvalid {
            message: format!("chunk_shuffle chunk_size {chunk_size} exceeds i64::MAX"),
            source: None,
        })?;
        if chunk_size == 0 {
            return Err(crate::Error::DataInvalid {
                message: "chunk_shuffle chunk_size must be positive".to_string(),
                source: None,
            });
        }
        Ok(Self {
            seed_words: decimal_seed_words(seed)?,
            chunk_size,
        })
    }
}

#[derive(Debug, Clone)]
struct InputFile {
    file: DataFileMeta,
    deletion_file: Option<DeletionFile>,
}

#[derive(Debug)]
struct InputGroup {
    partition: BinaryRow,
    bucket: i32,
    bucket_path: String,
    total_buckets: i32,
    snapshot_id: i64,
    is_streaming: bool,
    files: Vec<InputFile>,
}

#[derive(Debug)]
struct AppendSegment {
    input: InputFile,
    ranges: Vec<RowRange>,
}

#[derive(Debug)]
struct EvolutionSegment {
    files: Vec<InputFile>,
    ranges: Vec<RowRange>,
}

/// Repack planned files into shuffled, fixed-live-row chunks. Normal scan
/// planning runs first, so partition/stats/projection pruning and deletion-file
/// resolution remain centralized in `TableScan`.
pub(crate) async fn chunk_shuffle_splits(
    table: &Table,
    splits: Vec<DataSplit>,
    config: &ChunkShuffle,
    shard: Option<(usize, usize)>,
) -> crate::Result<Vec<DataSplit>> {
    if !table.schema().primary_keys().is_empty() {
        return Err(crate::Error::Unsupported {
            message: "chunk_shuffle only supports append tables".to_string(),
        });
    }
    if splits.iter().any(|split| split.row_ranges().is_some()) {
        return Err(crate::Error::Unsupported {
            message: "chunk_shuffle cannot combine with row-range selection".to_string(),
        });
    }

    let partition_fields = partition_fields(table)?;
    let mut groups = flatten_groups(splits)?;
    groups.sort_by(|left, right| {
        compare_partitions(&left.partition, &right.partition, &partition_fields)
            .unwrap_or_else(|_| {
                left.partition
                    .to_serialized_bytes()
                    .cmp(&right.partition.to_serialized_bytes())
            })
            .then_with(|| left.bucket.cmp(&right.bucket))
    });

    let data_evolution = table.schema().core_options().data_evolution_enabled();
    let mut chunks = Vec::new();
    for mut group in groups {
        if data_evolution {
            group.files.sort_by(|left, right| {
                left.file
                    .first_row_id
                    .cmp(&right.file.first_row_id)
                    .then_with(|| {
                        is_data_evolution_normal_file(&right.file)
                            .cmp(&is_data_evolution_normal_file(&left.file))
                    })
                    .then_with(|| left.file.file_name.cmp(&right.file.file_name))
            });
            chunks.extend(evolution_chunks(table, group, config.chunk_size).await?);
        } else {
            group
                .files
                .sort_by(|left, right| left.file.file_name.cmp(&right.file.file_name));
            chunks.extend(append_chunks(table, group, config.chunk_size).await?);
        }
    }

    PythonRandom::new(&config.seed_words).shuffle(&mut chunks);
    if let Some((index, count)) = shard {
        let (start, end) = shard_range(chunks.len(), index, count);
        chunks = chunks.drain(start..end).collect();
    }
    Ok(chunks)
}

fn partition_fields(table: &Table) -> crate::Result<Vec<DataField>> {
    let fields = table.schema().fields();
    table
        .schema()
        .partition_keys()
        .iter()
        .map(|name| {
            fields
                .iter()
                .find(|field| field.name() == name)
                .cloned()
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: format!("partition field '{name}' does not exist"),
                    source: None,
                })
        })
        .collect()
}

fn compare_partitions(
    left: &BinaryRow,
    right: &BinaryRow,
    fields: &[DataField],
) -> crate::Result<Ordering> {
    for (index, field) in fields.iter().enumerate() {
        let left = left.get_datum(index, field.data_type())?;
        let right = right.get_datum(index, field.data_type())?;
        // Python's key is `(value is None, value)`: non-null sorts first.
        let ordering = match (left, right) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
            (Some(left), Some(right)) => compare_datums(&left, &right),
        };
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(Ordering::Equal)
}

fn compare_datums(left: &Datum, right: &Datum) -> Ordering {
    left.partial_cmp(right).unwrap_or_else(|| {
        // NaN has no total order. Python's stable sort leaves incomparable
        // values in input order; the binary representation is a deterministic
        // fallback when manifest concurrency changed that input order.
        left.to_string().cmp(&right.to_string())
    })
}

fn flatten_groups(splits: Vec<DataSplit>) -> crate::Result<Vec<InputGroup>> {
    let mut grouped: BTreeMap<(Vec<u8>, i32), InputGroup> = BTreeMap::new();
    for split in splits {
        let deletion_files = split.data_deletion_files();
        let key = (split.partition().to_serialized_bytes(), split.bucket());
        let group = grouped.entry(key).or_insert_with(|| InputGroup {
            partition: split.partition().clone(),
            bucket: split.bucket(),
            bucket_path: split.bucket_path().to_string(),
            total_buckets: split.total_buckets(),
            snapshot_id: split.snapshot_id(),
            is_streaming: split.is_streaming(),
            files: Vec::new(),
        });
        if group.bucket_path != split.bucket_path()
            || group.total_buckets != split.total_buckets()
            || group.snapshot_id != split.snapshot_id()
            || group.is_streaming != split.is_streaming()
        {
            return Err(crate::Error::DataInvalid {
                message: "inconsistent split metadata within a partition bucket".to_string(),
                source: None,
            });
        }
        for (index, file) in split.data_files().iter().cloned().enumerate() {
            group.files.push(InputFile {
                file,
                deletion_file: deletion_files
                    .and_then(|files| files.get(index))
                    .cloned()
                    .flatten(),
            });
        }
    }
    Ok(grouped.into_values().collect())
}

async fn append_chunks(
    table: &Table,
    mut group: InputGroup,
    chunk_size: i64,
) -> crate::Result<Vec<DataSplit>> {
    let mut chunks: Vec<Vec<AppendSegment>> = Vec::new();
    let mut current = Vec::new();
    let mut current_rows = 0;

    let inputs = std::mem::take(&mut group.files);
    for input in inputs {
        let mut slicer = match live_row_slicer(table, &input).await? {
            Some(slicer) => slicer,
            None => continue,
        };
        loop {
            if current_rows == chunk_size {
                chunks.push(std::mem::take(&mut current));
                current_rows = 0;
            }
            let Some(slice) = slicer.take(chunk_size - current_rows)? else {
                break;
            };
            current_rows += slice.live_rows;
            current.push(AppendSegment {
                input: input.clone(),
                ranges: slice.ranges,
            });
        }
    }
    if !current.is_empty() {
        chunks.push(current);
    }

    chunks
        .into_iter()
        .map(|segments| build_append_split(&group, segments))
        .collect()
}

fn build_append_split(
    group: &InputGroup,
    segments: Vec<AppendSegment>,
) -> crate::Result<DataSplit> {
    let mut files = Vec::with_capacity(segments.len());
    let mut deletion_files = Vec::with_capacity(segments.len());
    let mut ranges = Vec::new();
    let mut split_offset = 0;
    for segment in segments {
        ranges.extend(
            segment
                .ranges
                .into_iter()
                .map(|range| RowRange::new(split_offset + range.from(), split_offset + range.to())),
        );
        split_offset += segment.input.file.row_count;
        files.push(segment.input.file);
        deletion_files.push(segment.input.deletion_file);
    }

    let mut builder = base_builder(group, files, true).with_row_ranges(merge_row_ranges(ranges));
    if deletion_files.iter().any(Option::is_some) {
        builder = builder.with_data_deletion_files(deletion_files);
    }
    builder.build()
}

async fn evolution_chunks(
    table: &Table,
    group: InputGroup,
    chunk_size: i64,
) -> crate::Result<Vec<DataSplit>> {
    if group
        .files
        .iter()
        .any(|input| input.file.first_row_id.is_none())
    {
        return Err(crate::Error::DataInvalid {
            message: "chunk_shuffle for data evolution requires first_row_id on every file"
                .to_string(),
            source: None,
        });
    }
    let deletion_by_name: HashMap<_, _> = group
        .files
        .iter()
        .map(|input| (input.file.file_name.clone(), input.deletion_file.clone()))
        .collect();
    let files = group.files.iter().map(|input| input.file.clone()).collect();
    let aligned_groups = group_by_overlapping_row_id(files);
    let mut chunks: Vec<Vec<EvolutionSegment>> = Vec::new();
    let mut current = Vec::new();
    let mut current_rows = 0;

    for files in aligned_groups {
        let from = files
            .iter()
            .filter_map(|file| file.first_row_id)
            .min()
            .ok_or_else(|| crate::Error::DataInvalid {
                message: "data evolution chunk group has no first_row_id".to_string(),
                source: None,
            })?;
        let to = files
            .iter()
            .filter_map(DataFileMeta::row_id_range)
            .map(|(_, to)| to)
            .max()
            .ok_or_else(|| crate::Error::DataInvalid {
                message: "data evolution chunk group has no row range".to_string(),
                source: None,
            })?;
        let anchor = data_evolution_anchor_file(&files)?;
        let anchor_deletion = deletion_by_name.get(&anchor.file_name).cloned().flatten();
        let (first_row_id, physical_count) = if anchor_deletion.is_some() {
            let (anchor_from, anchor_to) = anchor.row_id_range().unwrap();
            if anchor_from > from || anchor_to < to {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "data evolution anchor range [{anchor_from}, {anchor_to}] does not contain group [{from}, {to}]"
                    ),
                    source: None,
                });
            }
            (anchor_from, anchor.row_count)
        } else {
            (from, to - from + 1)
        };
        let anchor_input = InputFile {
            file: anchor.clone(),
            deletion_file: anchor_deletion.clone(),
        };
        let slicer = if anchor_deletion.is_some() {
            live_row_slicer(table, &anchor_input).await?
        } else {
            LiveRowSlicer::new(physical_count, Vec::new())?
        };
        let mut slicer = match slicer {
            Some(slicer) => slicer,
            None => continue,
        };
        if slicer.physical_count != physical_count {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "data evolution anchor row count {} does not match group row count {physical_count}",
                    slicer.physical_count
                ),
                source: None,
            });
        }
        let inputs: Vec<_> = files
            .into_iter()
            .map(|file| InputFile {
                deletion_file: deletion_by_name.get(&file.file_name).cloned().flatten(),
                file,
            })
            .collect();
        loop {
            if current_rows == chunk_size {
                chunks.push(std::mem::take(&mut current));
                current_rows = 0;
            }
            let Some(slice) = slicer.take(chunk_size - current_rows)? else {
                break;
            };
            current_rows += slice.live_rows;
            current.push(EvolutionSegment {
                files: inputs.clone(),
                ranges: slice
                    .ranges
                    .into_iter()
                    .map(|range| {
                        RowRange::new(first_row_id + range.from(), first_row_id + range.to())
                    })
                    .collect(),
            });
        }
    }
    if !current.is_empty() {
        chunks.push(current);
    }

    chunks
        .into_iter()
        .map(|segments| build_evolution_split(&group, segments))
        .collect()
}

fn build_evolution_split(
    group: &InputGroup,
    segments: Vec<EvolutionSegment>,
) -> crate::Result<DataSplit> {
    let mut files = Vec::new();
    let mut deletion_files = Vec::new();
    let mut ranges = Vec::new();
    for segment in segments {
        ranges.extend(segment.ranges);
        for input in segment.files {
            files.push(input.file);
            deletion_files.push(input.deletion_file);
        }
    }
    let mut builder = base_builder(group, files, false).with_row_ranges(merge_row_ranges(ranges));
    if deletion_files.iter().any(Option::is_some) {
        builder = builder.with_data_deletion_files(deletion_files);
    }
    builder.build()
}

fn base_builder(group: &InputGroup, files: Vec<DataFileMeta>, raw: bool) -> DataSplitBuilder {
    DataSplitBuilder::new()
        .with_snapshot(group.snapshot_id)
        .with_partition(group.partition.clone())
        .with_bucket(group.bucket)
        .with_bucket_path(group.bucket_path.clone())
        .with_total_buckets(group.total_buckets)
        .with_data_files(files)
        .with_raw_convertible(raw)
        .with_streaming(group.is_streaming)
}

#[derive(Debug)]
struct PhysicalSlice {
    ranges: Vec<RowRange>,
    live_rows: i64,
}

#[derive(Debug)]
struct LiveRowSlicer {
    physical_count: i64,
    deleted: Vec<i64>,
    deleted_index: usize,
    position: i64,
}

impl LiveRowSlicer {
    fn new(physical_count: i64, deleted: Vec<i64>) -> crate::Result<Option<Self>> {
        if physical_count < 0 {
            return Err(crate::Error::DataInvalid {
                message: format!("negative physical row count {physical_count}"),
                source: None,
            });
        }
        if deleted
            .iter()
            .any(|position| *position < 0 || *position >= physical_count)
        {
            return Err(crate::Error::DataInvalid {
                message: "deletion vector position is outside the data file".to_string(),
                source: None,
            });
        }
        if deleted.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(crate::Error::DataInvalid {
                message: "deletion vector positions must be strictly increasing".to_string(),
                source: None,
            });
        }
        if deleted.len() as i64 == physical_count {
            return Ok(None);
        }
        Ok(Some(Self {
            physical_count,
            deleted,
            deleted_index: 0,
            position: 0,
        }))
    }

    fn take(&mut self, expected_live_rows: i64) -> crate::Result<Option<PhysicalSlice>> {
        if expected_live_rows <= 0 {
            return Err(crate::Error::DataInvalid {
                message: "chunk slice must request a positive number of live rows".to_string(),
                source: None,
            });
        }
        if self.position >= self.physical_count {
            return Ok(None);
        }
        let mut live_rows = 0;
        let mut ranges = Vec::new();
        while self.position < self.physical_count {
            let next_deleted = self.deleted.get(self.deleted_index).copied();
            if let Some(deleted) = next_deleted {
                let live_run = deleted - self.position;
                let take = (expected_live_rows - live_rows).min(live_run);
                if take > 0 {
                    ranges.push(RowRange::new(self.position, self.position + take - 1));
                    self.position += take;
                    live_rows += take;
                }
            } else {
                let take =
                    (expected_live_rows - live_rows).min(self.physical_count - self.position);
                if take > 0 {
                    ranges.push(RowRange::new(self.position, self.position + take - 1));
                    self.position += take;
                    live_rows += take;
                }
            }
            if live_rows == expected_live_rows {
                self.skip_deleted_at_cursor();
                return Ok(Some(PhysicalSlice { ranges, live_rows }));
            }
            self.skip_deleted_at_cursor();
        }
        if live_rows == 0 {
            Ok(None)
        } else {
            Ok(Some(PhysicalSlice { ranges, live_rows }))
        }
    }

    fn skip_deleted_at_cursor(&mut self) {
        while self.deleted.get(self.deleted_index) == Some(&self.position) {
            self.position += 1;
            self.deleted_index += 1;
        }
    }
}

async fn live_row_slicer(table: &Table, input: &InputFile) -> crate::Result<Option<LiveRowSlicer>> {
    let Some(deletion_file) = &input.deletion_file else {
        return LiveRowSlicer::new(input.file.row_count, Vec::new());
    };
    if deletion_file.cardinality() == Some(0) {
        return LiveRowSlicer::new(input.file.row_count, Vec::new());
    }
    if let Some(cardinality) = deletion_file.cardinality() {
        if cardinality < 0 || cardinality > input.file.row_count {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "deletion vector cardinality {cardinality} is outside [0, {}]",
                    input.file.row_count
                ),
                source: None,
            });
        }
    }
    let vector = DeletionVectorFactory::read(table.file_io(), deletion_file).await?;
    validate_cardinality(deletion_file, &vector)?;
    let deleted = vector.iter().map(|position| position as i64).collect();
    LiveRowSlicer::new(input.file.row_count, deleted)
}

fn validate_cardinality(file: &DeletionFile, vector: &DeletionVector) -> crate::Result<()> {
    if let Some(expected) = file.cardinality() {
        if expected as u64 != vector.cardinality() {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "deletion vector cardinality mismatch: metadata {expected}, bitmap {}",
                    vector.cardinality()
                ),
                source: None,
            });
        }
    }
    Ok(())
}

fn shard_range(total: usize, index: usize, count: usize) -> (usize, usize) {
    let base = total / count;
    let remainder = total % count;
    let start = index * base + index.min(remainder);
    (start, start + base + usize::from(index < remainder))
}

fn decimal_seed_words(seed: &str) -> crate::Result<Vec<u32>> {
    let digits = seed
        .strip_prefix('-')
        .or_else(|| seed.strip_prefix('+'))
        .unwrap_or(seed);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(crate::Error::DataInvalid {
            message: format!("invalid integer chunk_shuffle seed '{seed}'"),
            source: None,
        });
    }
    let mut words = vec![0u32];
    for digit in digits.bytes().map(|byte| u64::from(byte - b'0')) {
        let mut carry = digit;
        for word in &mut words {
            let value = u64::from(*word) * 10 + carry;
            *word = value as u32;
            carry = value >> 32;
        }
        if carry != 0 {
            words.push(carry as u32);
        }
    }
    while words.len() > 1 && words.last() == Some(&0) {
        words.pop();
    }
    Ok(words)
}

/// MT19937 plus CPython's integer-seed and `_randbelow_with_getrandbits`
/// conventions. Only up to 64 random bits are needed because slice lengths are
/// Rust `usize` values.
struct PythonRandom {
    state: [u32; 624],
    index: usize,
}

impl PythonRandom {
    fn new(seed_words: &[u32]) -> Self {
        let mut random = Self {
            state: [0; 624],
            index: 624,
        };
        random.init_genrand(19_650_218);
        let mut i = 1usize;
        let mut j = 0usize;
        for _ in 0..624usize.max(seed_words.len()) {
            random.state[i] = (random.state[i]
                ^ (random.state[i - 1] ^ (random.state[i - 1] >> 30)).wrapping_mul(1_664_525))
            .wrapping_add(seed_words[j])
            .wrapping_add(j as u32);
            i += 1;
            j += 1;
            if i >= 624 {
                random.state[0] = random.state[623];
                i = 1;
            }
            if j >= seed_words.len() {
                j = 0;
            }
        }
        for _ in 0..623 {
            random.state[i] = (random.state[i]
                ^ (random.state[i - 1] ^ (random.state[i - 1] >> 30)).wrapping_mul(1_566_083_941))
            .wrapping_sub(i as u32);
            i += 1;
            if i >= 624 {
                random.state[0] = random.state[623];
                i = 1;
            }
        }
        random.state[0] = 0x8000_0000;
        random
    }

    fn init_genrand(&mut self, seed: u32) {
        self.state[0] = seed;
        for i in 1..624 {
            self.state[i] = 1_812_433_253u32
                .wrapping_mul(self.state[i - 1] ^ (self.state[i - 1] >> 30))
                .wrapping_add(i as u32);
        }
    }

    fn gen_u32(&mut self) -> u32 {
        if self.index >= 624 {
            for i in 0..624 {
                let y = (self.state[i] & 0x8000_0000) | (self.state[(i + 1) % 624] & 0x7fff_ffff);
                self.state[i] = self.state[(i + 397) % 624]
                    ^ (y >> 1)
                    ^ if y & 1 == 0 { 0 } else { 0x9908_b0df };
            }
            self.index = 0;
        }
        let mut y = self.state[self.index];
        self.index += 1;
        y ^= y >> 11;
        y ^= (y << 7) & 0x9d2c_5680;
        y ^= (y << 15) & 0xefc6_0000;
        y ^ (y >> 18)
    }

    fn getrandbits(&mut self, bits: u32) -> u64 {
        if bits <= 32 {
            return u64::from(self.gen_u32() >> (32 - bits));
        }
        let low = u64::from(self.gen_u32());
        let high_bits = bits - 32;
        let high = u64::from(self.gen_u32() >> (32 - high_bits));
        low | (high << 32)
    }

    fn randbelow(&mut self, n: usize) -> usize {
        debug_assert!(n > 0);
        let bits = usize::BITS - n.leading_zeros();
        loop {
            let value = self.getrandbits(bits) as usize;
            if value < n {
                return value;
            }
        }
    }

    fn shuffle<T>(&mut self, values: &mut [T]) {
        for index in (1..values.len()).rev() {
            let other = self.randbelow(index + 1);
            values.swap(index, other);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Identifier;
    use crate::io::FileIOBuilder;
    use crate::spec::{stats::BinaryTableStats, DataType, IntType, Schema, TableSchema};

    fn test_table(data_evolution: bool) -> Table {
        let mut schema = Schema::builder().column("id", DataType::Int(IntType::new()));
        if data_evolution {
            schema = schema
                .option("data-evolution.enabled", "true")
                .option("row-tracking.enabled", "true");
        }
        Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("default", "chunk_test"),
            "memory:/chunk-test".to_string(),
            TableSchema::new(0, &schema.build().unwrap()),
            None,
        )
    }

    fn file(name: &str, row_count: i64, first_row_id: Option<i64>) -> DataFileMeta {
        DataFileMeta {
            file_name: name.to_string(),
            file_size: 100,
            row_count,
            min_key: Vec::new(),
            max_key: Vec::new(),
            key_stats: BinaryTableStats::new(Vec::new(), Vec::new(), Vec::new()),
            value_stats: BinaryTableStats::new(Vec::new(), Vec::new(), Vec::new()),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id: 0,
            level: 0,
            extra_files: Vec::new(),
            creation_time: None,
            delete_row_count: None,
            embedded_index: None,
            first_row_id,
            write_cols: None,
            external_path: None,
            file_source: None,
            value_stats_cols: None,
            column_max_sequence_numbers: None,
        }
    }

    fn split(files: Vec<DataFileMeta>, raw: bool) -> DataSplit {
        DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("memory:/chunk-test/bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(files)
            .with_raw_convertible(raw)
            .build()
            .unwrap()
    }

    #[test]
    fn python_shuffle_matches_cpython_for_signed_and_large_integer_seeds() {
        for (seed, expected) in [
            ("0", vec![7, 8, 1, 5, 3, 4, 2, 0, 9, 6]),
            ("42", vec![7, 3, 2, 8, 5, 6, 9, 4, 0, 1]),
            ("-11", vec![2, 6, 0, 1, 5, 4, 3, 9, 8, 7]),
            ("1180591620717411303424", vec![4, 9, 5, 2, 1, 8, 0, 7, 6, 3]),
        ] {
            let words = decimal_seed_words(seed).unwrap();
            let mut actual: Vec<_> = (0..10).collect();
            PythonRandom::new(&words).shuffle(&mut actual);
            assert_eq!(actual, expected, "seed={seed}");
        }
    }

    #[test]
    fn live_row_slicer_returns_only_visible_physical_ranges() {
        let mut slicer = LiveRowSlicer::new(12, vec![0, 3, 4, 8, 11])
            .unwrap()
            .unwrap();
        let first = slicer.take(3).unwrap().unwrap();
        assert_eq!(
            (first.ranges, first.live_rows),
            (vec![RowRange::new(1, 2), RowRange::new(5, 5)], 3)
        );
        let second = slicer.take(3).unwrap().unwrap();
        assert_eq!(
            (second.ranges, second.live_rows),
            (vec![RowRange::new(6, 7), RowRange::new(9, 9)], 3)
        );
        let last = slicer.take(3).unwrap().unwrap();
        assert_eq!(
            (last.ranges, last.live_rows),
            (vec![RowRange::new(10, 10)], 1)
        );
        assert!(slicer.take(1).unwrap().is_none());

        let mut alternating = LiveRowSlicer::new(8, vec![1, 3, 5, 7]).unwrap().unwrap();
        assert_eq!(
            alternating.take(3).unwrap().unwrap().ranges,
            vec![
                RowRange::new(0, 0),
                RowRange::new(2, 2),
                RowRange::new(4, 4)
            ]
        );
        assert_eq!(
            alternating.take(3).unwrap().unwrap().ranges,
            vec![RowRange::new(6, 6)]
        );
    }

    #[test]
    fn shard_ranges_are_balanced_disjoint_and_cover_all_chunks() {
        let ranges: Vec<_> = (0..5).map(|index| shard_range(12, index, 5)).collect();
        assert_eq!(ranges, vec![(0, 3), (3, 6), (6, 8), (8, 10), (10, 12)]);
    }

    #[tokio::test]
    async fn append_chunks_cover_every_file_position_once_and_shard_by_chunk() {
        let table = test_table(false);
        // File-name order, not the input split order, defines chunk positions.
        let input = split(
            vec![file("b.parquet", 4, None), file("a.parquet", 5, None)],
            true,
        );
        let config = ChunkShuffle::from_decimal_seed("42", 3).unwrap();
        let chunks = chunk_shuffle_splits(&table, vec![input.clone()], &config, None)
            .await
            .unwrap();
        assert_eq!(chunks.len(), 3);
        assert!(chunks
            .iter()
            .all(|chunk| chunk.row_count() == 3 && chunk.merged_row_count() == Some(3)));

        let mut covered = Vec::new();
        for chunk in &chunks {
            let mut split_offset = 0;
            for file in chunk.data_files() {
                for range in chunk.row_ranges().unwrap() {
                    let from = range.from().max(split_offset);
                    let to = range.to().min(split_offset + file.row_count - 1);
                    if from <= to {
                        covered.push((
                            file.file_name.clone(),
                            from - split_offset,
                            to - split_offset,
                        ));
                    }
                }
                split_offset += file.row_count;
            }
            let serialized = chunk.serialize_split_v1().unwrap();
            assert_eq!(
                DataSplit::deserialize_split_v1(&serialized)
                    .unwrap()
                    .row_ranges(),
                chunk.row_ranges()
            );
        }
        covered.sort();
        assert_eq!(
            covered,
            vec![
                ("a.parquet".to_string(), 0, 2),
                ("a.parquet".to_string(), 3, 4),
                ("b.parquet".to_string(), 0, 0),
                ("b.parquet".to_string(), 1, 3),
            ]
        );

        let left = chunk_shuffle_splits(&table, vec![input.clone()], &config, Some((0, 2)))
            .await
            .unwrap();
        let right = chunk_shuffle_splits(&table, vec![input], &config, Some((1, 2)))
            .await
            .unwrap();
        assert_eq!([left, right].concat(), chunks);
    }

    #[tokio::test]
    async fn data_evolution_chunks_keep_aligned_files_and_global_ranges() {
        let table = test_table(true);
        let input = split(
            vec![
                file("base.parquet", 5, Some(10)),
                file("payload.blob", 5, Some(10)),
                file("next.parquet", 4, Some(20)),
            ],
            false,
        );
        let chunks = chunk_shuffle_splits(
            &table,
            vec![input],
            &ChunkShuffle::from_decimal_seed("0", 3).unwrap(),
            None,
        )
        .await
        .unwrap();
        assert_eq!(chunks.len(), 3);
        assert!(chunks
            .iter()
            .all(|chunk| chunk.row_count() == 3 && chunk.merged_row_count() == Some(3)));

        let mut ranges: Vec<_> = chunks
            .iter()
            .flat_map(|chunk| chunk.row_ranges().unwrap())
            .map(|range| (range.from(), range.to()))
            .collect();
        ranges.sort();
        assert_eq!(ranges, vec![(10, 12), (13, 14), (20, 20), (21, 23)]);
        for chunk in chunks {
            for range in chunk.row_ranges().unwrap() {
                if range.from() < 20 {
                    assert!(chunk
                        .data_files()
                        .iter()
                        .any(|file| file.file_name == "payload.blob"));
                }
            }
        }
    }
}
