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

//! Planning primitives for commit-time manifest sorting.
//!
//! This mirrors the partition-sort foundation in Java Paimon's
//! `ManifestFileSorter`: manifests are organized into non-overlapping sorted
//! runs, runs are selected with the universal-compaction size strategy, and
//! selected files are split into independently rewritable overlap sections.

use crate::spec::{
    datum_cmp, extract_datum, BinaryRow, CoreOptions, DataField, Datum, FileKind, ManifestEntry,
    ManifestFileMeta,
};
use crate::Result;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

pub(crate) const MAX_SORTED_RUN_LEVEL: i32 = 4;

#[derive(Debug, Clone)]
pub(crate) struct ManifestSortConfig {
    pub(crate) target_size: i64,
    pub(crate) merge_min_count: usize,
    pub(crate) full_compaction_threshold_size: i64,
    pub(crate) max_rewrite_size: i64,
    pub(crate) max_size_amplification_percent: i32,
    pub(crate) sorted_run_size_ratio: i32,
    pub(crate) spill_buffer_size: usize,
    pub(crate) spill_max_disk_size: u64,
    pub(crate) max_file_handles: usize,
}

impl ManifestSortConfig {
    pub(crate) fn from_options(options: &CoreOptions<'_>) -> Self {
        Self {
            target_size: options.manifest_target_size().max(1),
            merge_min_count: options.manifest_merge_min_count(),
            full_compaction_threshold_size: options.manifest_full_compaction_threshold_size(),
            max_rewrite_size: options.manifest_sort_max_rewrite_size(),
            max_size_amplification_percent: options.compaction_max_size_amplification_percent(),
            sorted_run_size_ratio: options.compaction_size_ratio(),
            spill_buffer_size: usize::try_from(options.sort_spill_buffer_size())
                .unwrap_or(usize::MAX)
                .max(1),
            spill_max_disk_size: u64::try_from(options.write_buffer_spill_max_disk_size())
                .unwrap_or(u64::MAX),
            max_file_handles: options.local_sort_max_num_file_handles().max(2),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PartitionSortField {
    field_index: usize,
    data_type: crate::spec::DataType,
}

#[derive(Debug, Clone)]
pub(crate) enum ManifestSortKey {
    Partition {
        field: PartitionSortField,
    },
    Bucket {
        field: Option<PartitionSortField>,
        compare_manifest_buckets: bool,
    },
    RowId {
        partition_fields: Vec<PartitionSortField>,
    },
}

impl ManifestSortKey {
    pub(crate) fn create(
        partition_fields: &[DataField],
        configured_field: Option<&str>,
        data_evolution_enabled: bool,
        manifests: &[ManifestFileMeta],
        bucketed: bool,
    ) -> Result<Option<Self>> {
        if data_evolution_enabled
            && !manifests.is_empty()
            && manifests
                .iter()
                .all(|meta| meta.min_row_id().is_some() && meta.max_row_id().is_some())
        {
            let partition_fields = match configured_field {
                Some(name) => vec![resolve_partition_field(partition_fields, name)?],
                None => partition_fields
                    .iter()
                    .enumerate()
                    .map(|(field_index, field)| PartitionSortField {
                        field_index,
                        data_type: field.data_type().clone(),
                    })
                    .collect(),
            };
            return Ok(Some(Self::RowId { partition_fields }));
        }

        let field = match (configured_field, partition_fields.first()) {
            (Some(name), _) => Some(resolve_partition_field(partition_fields, name)?),
            (None, Some(field)) => Some(PartitionSortField {
                field_index: 0,
                data_type: field.data_type().clone(),
            }),
            (None, None) => None,
        };
        if bucketed {
            return Ok(Some(Self::Bucket {
                field,
                compare_manifest_buckets: manifests
                    .iter()
                    .all(|meta| meta.min_bucket().is_some() && meta.max_bucket().is_some()),
            }));
        }
        Ok(field.map(|field| Self::Partition { field }))
    }

    fn entry_key(&self, entry: &ManifestEntry) -> Result<ManifestEntrySortKey> {
        match self {
            Self::Partition { field } => Ok(ManifestEntrySortKey::Partition(
                read_partition_values(entry.partition(), std::slice::from_ref(field))?,
            )),
            Self::Bucket { field, .. } => Ok(ManifestEntrySortKey::Bucket {
                bucket: entry.bucket(),
                partition: read_partition_values(
                    entry.partition(),
                    field.as_ref().map(std::slice::from_ref).unwrap_or_default(),
                )?,
            }),
            Self::RowId { partition_fields } => {
                let first_row_id =
                    entry
                        .file()
                        .first_row_id
                        .ok_or_else(|| crate::Error::DataInvalid {
                            message: format!(
                                "Manifest entry '{}' has no first RowID",
                                entry.file().file_name
                            ),
                            source: None,
                        })?;
                let row_count = entry.file().row_count;
                if row_count <= 0 {
                    return Err(crate::Error::DataInvalid {
                        message: format!(
                            "Manifest entry '{}' has invalid row count {row_count}",
                            entry.file().file_name
                        ),
                        source: None,
                    });
                }
                let range_end = first_row_id.checked_add(row_count - 1).ok_or_else(|| {
                    crate::Error::DataInvalid {
                        message: format!(
                            "Manifest entry '{}' RowID range overflows i64",
                            entry.file().file_name
                        ),
                        source: None,
                    }
                })?;
                Ok(ManifestEntrySortKey::RowId {
                    partition: read_partition_values(entry.partition(), partition_fields)?,
                    first_row_id,
                    range_end,
                    max_sequence_number: entry.file().max_sequence_number,
                })
            }
        }
    }

    pub(crate) fn range(&self, meta: ManifestFileMeta) -> Result<ManifestRange> {
        let (min, max) = match self {
            Self::Partition { field } => (
                ManifestBound::Partition(read_partition_values(
                    meta.partition_stats().min_values(),
                    std::slice::from_ref(field),
                )?),
                ManifestBound::Partition(read_partition_values(
                    meta.partition_stats().max_values(),
                    std::slice::from_ref(field),
                )?),
            ),
            Self::Bucket {
                field,
                compare_manifest_buckets,
            } => {
                let fields = field.as_ref().map(std::slice::from_ref).unwrap_or_default();
                (
                    ManifestBound::Bucket {
                        bucket: compare_manifest_buckets.then(|| {
                            meta.min_bucket()
                                .expect("bucket stats were validated for the whole input")
                        }),
                        partition: read_partition_values(
                            meta.partition_stats().min_values(),
                            fields,
                        )?,
                    },
                    ManifestBound::Bucket {
                        bucket: compare_manifest_buckets.then(|| {
                            meta.max_bucket()
                                .expect("bucket stats were validated for the whole input")
                        }),
                        partition: read_partition_values(
                            meta.partition_stats().max_values(),
                            fields,
                        )?,
                    },
                )
            }
            Self::RowId { partition_fields } => (
                ManifestBound::RowId {
                    partition: read_partition_values(
                        meta.partition_stats().min_values(),
                        partition_fields,
                    )?,
                    row_id: meta.min_row_id().ok_or_else(|| crate::Error::DataInvalid {
                        message: format!("Manifest '{}' has no minimum RowID", meta.file_name()),
                        source: None,
                    })?,
                },
                ManifestBound::RowId {
                    partition: read_partition_values(
                        meta.partition_stats().max_values(),
                        partition_fields,
                    )?,
                    row_id: meta.max_row_id().ok_or_else(|| crate::Error::DataInvalid {
                        message: format!("Manifest '{}' has no maximum RowID", meta.file_name()),
                        source: None,
                    })?,
                },
            ),
        };
        Ok(ManifestRange { meta, min, max })
    }

    fn compare_bounds(&self, left: &ManifestBound, right: &ManifestBound) -> Ordering {
        match (self, left, right) {
            (
                Self::Partition { .. },
                ManifestBound::Partition(left),
                ManifestBound::Partition(right),
            ) => compare_partition_values(left, right),
            (
                Self::Bucket { .. },
                ManifestBound::Bucket {
                    bucket: left_bucket,
                    partition: left_partition,
                },
                ManifestBound::Bucket {
                    bucket: right_bucket,
                    partition: right_partition,
                },
            ) => left_bucket
                .cmp(right_bucket)
                .then_with(|| compare_partition_values(left_partition, right_partition)),
            (
                Self::RowId { .. },
                ManifestBound::RowId {
                    partition: left_partition,
                    row_id: left_row_id,
                },
                ManifestBound::RowId {
                    partition: right_partition,
                    row_id: right_row_id,
                },
            ) => compare_partition_values(left_partition, right_partition)
                .then_with(|| left_row_id.cmp(right_row_id)),
            _ => unreachable!("manifest bounds are built by one whole-pass sort key"),
        }
    }

    fn is_after_max(&self, min: &ManifestBound, max: &ManifestBound) -> bool {
        let ordering = self.compare_bounds(min, max);
        match self {
            // Partition ranges preserve Java's historical boundary-equality
            // behavior. RowID ranges are inclusive, so equality overlaps.
            Self::Partition { .. } => ordering != Ordering::Less,
            Self::Bucket {
                field,
                compare_manifest_buckets,
            } => match (min, max) {
                (
                    ManifestBound::Bucket {
                        bucket: min_bucket,
                        partition: min_partition,
                    },
                    ManifestBound::Bucket {
                        bucket: max_bucket,
                        partition: max_partition,
                    },
                ) => {
                    if *compare_manifest_buckets {
                        let bucket_order = min_bucket.cmp(max_bucket);
                        if bucket_order != Ordering::Equal {
                            return bucket_order == Ordering::Greater;
                        }
                    }
                    field.as_ref().is_some_and(|_| {
                        compare_partition_values(min_partition, max_partition) != Ordering::Less
                    }) || (field.is_none() && *compare_manifest_buckets)
                }
                _ => unreachable!("bucket sort builds bucket manifest bounds"),
            },
            Self::RowId { .. } => ordering == Ordering::Greater,
        }
    }
}

fn resolve_partition_field(
    partition_fields: &[DataField],
    name: &str,
) -> Result<PartitionSortField> {
    partition_fields
        .iter()
        .enumerate()
        .find(|(_, field)| field.name() == name)
        .map(|(field_index, field)| PartitionSortField {
            field_index,
            data_type: field.data_type().clone(),
        })
        .ok_or_else(|| crate::Error::ConfigInvalid {
            message: format!(
                "Cannot resolve manifest sort partition field '{name}' from {:?}",
                partition_fields
                    .iter()
                    .map(|field| field.name())
                    .collect::<Vec<_>>()
            ),
        })
}

fn read_partition_values(
    bytes: &[u8],
    fields: &[PartitionSortField],
) -> Result<Vec<Option<Datum>>> {
    if fields.is_empty() {
        return Ok(Vec::new());
    }
    let row = BinaryRow::from_serialized_bytes(bytes)?;
    fields
        .iter()
        .map(|field| {
            if field.field_index >= row.arity() as usize {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Manifest sort field index {} is outside partition row arity {}",
                        field.field_index,
                        row.arity()
                    ),
                    source: None,
                });
            }
            extract_datum(&row, field.field_index, &field.data_type)
        })
        .collect()
}

fn compare_partition_values(left: &[Option<Datum>], right: &[Option<Datum>]) -> Ordering {
    left.iter()
        .zip(right)
        .map(|(left, right)| compare_optional_datums(left, right))
        .find(|ordering| *ordering != Ordering::Equal)
        .unwrap_or_else(|| left.len().cmp(&right.len()))
}

#[derive(Debug)]
enum ManifestEntrySortKey {
    Partition(Vec<Option<Datum>>),
    Bucket {
        bucket: i32,
        partition: Vec<Option<Datum>>,
    },
    RowId {
        partition: Vec<Option<Datum>>,
        first_row_id: i64,
        range_end: i64,
        max_sequence_number: i64,
    },
}

#[derive(Debug, Clone)]
enum ManifestBound {
    Partition(Vec<Option<Datum>>),
    Bucket {
        bucket: Option<i32>,
        partition: Vec<Option<Datum>>,
    },
    RowId {
        partition: Vec<Option<Datum>>,
        row_id: i64,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct ManifestRange {
    pub(crate) meta: ManifestFileMeta,
    min: ManifestBound,
    max: ManifestBound,
}

#[derive(Debug, Clone)]
pub(crate) struct ManifestAdjacentSortedRun {
    pub(crate) level: i32,
    pub(crate) files: Vec<ManifestRange>,
    pub(crate) total_size: i64,
}

impl ManifestAdjacentSortedRun {
    fn from_sorted(files: Vec<ManifestRange>) -> Self {
        let total_size = files.iter().map(|file| file.meta.file_size()).sum();
        Self {
            level: -1,
            files,
            total_size,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ManifestSection {
    pub(crate) files: Vec<ManifestFileMeta>,
    pub(crate) total_size: i64,
    pub(crate) has_default_compaction_file: bool,
}

impl ManifestSection {
    fn merge(mut left: Self, right: Self) -> Self {
        left.files.extend(right.files);
        left.total_size += right.total_size;
        left.has_default_compaction_file |= right.has_default_compaction_file;
        left
    }
}

#[derive(Debug)]
pub(crate) struct ManifestRewritePlan {
    pub(crate) picked_file_names: HashSet<String>,
    pub(crate) sections: Vec<ManifestSection>,
}

pub(crate) fn compare_optional_datums(left: &Option<Datum>, right: &Option<Datum>) -> Ordering {
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        // Java's Float.compare / Double.compare canonicalize NaNs and put
        // -0.0 before +0.0; Rust's partial_cmp has no order for NaN.
        (Some(Datum::Float(left)), Some(Datum::Float(right))) => {
            canonical_float(*left).total_cmp(&canonical_float(*right))
        }
        (Some(Datum::Double(left)), Some(Datum::Double(right))) => {
            canonical_double(*left).total_cmp(&canonical_double(*right))
        }
        (Some(left), Some(right)) => {
            datum_cmp(left, right).expect("validated manifest sort field must have an ordering")
        }
    }
}

fn canonical_float(value: f32) -> f32 {
    if value.is_nan() {
        f32::NAN
    } else {
        value
    }
}

fn canonical_double(value: f64) -> f64 {
    if value.is_nan() {
        f64::NAN
    } else {
        value
    }
}

#[derive(Debug)]
struct SortRecord {
    key: ManifestEntrySortKey,
    encoded: Vec<u8>,
    entry: ManifestEntry,
}

impl SortRecord {
    fn new(entry: ManifestEntry, sort_key: &ManifestSortKey) -> Result<Self> {
        let key = sort_key.entry_key(&entry)?;
        let encoded =
            serde_json::to_vec(&entry).map_err(|error| crate::Error::UnexpectedError {
                message: "Failed to encode manifest entry for external sort".to_string(),
                source: Some(Box::new(error)),
            })?;
        Ok(Self {
            key,
            encoded,
            entry,
        })
    }

    fn from_encoded(encoded: Vec<u8>, sort_key: &ManifestSortKey) -> Result<Self> {
        let entry =
            serde_json::from_slice(&encoded).map_err(|error| crate::Error::UnexpectedError {
                message: "Failed to decode spilled manifest entry".to_string(),
                source: Some(Box::new(error)),
            })?;
        let key = sort_key.entry_key(&entry)?;
        Ok(Self {
            key,
            encoded,
            entry,
        })
    }
}

fn compare_sort_records(left: &SortRecord, right: &SortRecord) -> Ordering {
    let partition_order = match (&left.key, &right.key) {
        (ManifestEntrySortKey::Partition(left), ManifestEntrySortKey::Partition(right)) => {
            compare_partition_values(left, right)
        }
        (
            ManifestEntrySortKey::Bucket {
                bucket: left_bucket,
                partition: left_partition,
            },
            ManifestEntrySortKey::Bucket {
                bucket: right_bucket,
                partition: right_partition,
            },
        ) => left_bucket
            .cmp(right_bucket)
            .then_with(|| compare_partition_values(left_partition, right_partition)),
        (
            ManifestEntrySortKey::RowId {
                partition: left, ..
            },
            ManifestEntrySortKey::RowId {
                partition: right, ..
            },
        ) => compare_partition_values(left, right),
        _ => unreachable!("entries in one sorter use one whole-pass sort key"),
    };
    partition_order
        .then_with(|| file_kind_order(left.entry.kind()).cmp(&file_kind_order(right.entry.kind())))
        .then_with(|| match (&left.key, &right.key) {
            (
                ManifestEntrySortKey::RowId {
                    first_row_id: left_first,
                    range_end: left_end,
                    max_sequence_number: left_sequence,
                    ..
                },
                ManifestEntrySortKey::RowId {
                    first_row_id: right_first,
                    range_end: right_end,
                    max_sequence_number: right_sequence,
                    ..
                },
            ) => left_first
                .cmp(right_first)
                .then_with(|| left_end.cmp(right_end))
                .then_with(|| right_sequence.cmp(left_sequence)),
            _ => Ordering::Equal,
        })
        .then_with(|| {
            left.entry
                .file()
                .file_name
                .cmp(&right.entry.file().file_name)
        })
}

#[derive(Debug)]
struct SpillRun {
    path: PathBuf,
    bytes: u64,
}

/// Bounded manifest-entry sorter.
///
/// Entries are sorted in memory up to `spill_buffer_size`, then written as
/// length-prefixed JSON records under an owned temporary directory. The JSON
/// representation is an internal spill format only; persisted Paimon
/// manifests remain Avro. Runs are merged until the final fan-in is bounded by
/// `max_file_handles`.
pub(crate) struct SpillableManifestSorter {
    sort_key: ManifestSortKey,
    buffer: Vec<SortRecord>,
    buffer_bytes: usize,
    spill_buffer_size: usize,
    spill_max_disk_size: u64,
    max_file_handles: usize,
    temp_dir: tempfile::TempDir,
    runs: Vec<SpillRun>,
    disk_bytes: u64,
    next_run_id: u64,
}

impl SpillableManifestSorter {
    pub(crate) fn new(sort_key: &ManifestSortKey, config: &ManifestSortConfig) -> Result<Self> {
        let temp_dir = tempfile::tempdir().map_err(|error| crate::Error::UnexpectedError {
            message: "Failed to create manifest-sort spill directory".to_string(),
            source: Some(Box::new(error)),
        })?;
        Ok(Self {
            sort_key: sort_key.clone(),
            buffer: Vec::new(),
            buffer_bytes: 0,
            spill_buffer_size: config.spill_buffer_size.max(1),
            spill_max_disk_size: config.spill_max_disk_size,
            max_file_handles: config.max_file_handles.max(2),
            temp_dir,
            runs: Vec::new(),
            disk_bytes: 0,
            next_run_id: 0,
        })
    }

    pub(crate) fn push(&mut self, entry: ManifestEntry) -> Result<()> {
        let record = SortRecord::new(entry, &self.sort_key)?;
        self.buffer_bytes = self
            .buffer_bytes
            .saturating_add(record.encoded.len().saturating_add(8));
        self.buffer.push(record);
        if self.buffer_bytes >= self.spill_buffer_size {
            self.spill_buffer()?;
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<SortedManifestEntries> {
        if self.runs.is_empty() {
            self.buffer.sort_by(compare_sort_records);
            return Ok(SortedManifestEntries::Memory(
                self.buffer
                    .into_iter()
                    .map(|record| record.entry)
                    .collect::<Vec<_>>()
                    .into_iter(),
            ));
        }
        self.spill_buffer()?;
        self.reduce_run_count()?;
        SpilledManifestEntries::new(self.temp_dir, self.runs, self.sort_key)
            .map(SortedManifestEntries::Spilled)
    }

    fn spill_buffer(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.buffer.sort_by(compare_sort_records);
        let path = self.next_run_path();
        let records = std::mem::take(&mut self.buffer);
        self.buffer_bytes = 0;
        let run = self.write_run(&path, records.into_iter())?;
        self.runs.push(run);
        Ok(())
    }

    fn reduce_run_count(&mut self) -> Result<()> {
        while self.runs.len() > self.max_file_handles {
            let old_runs = std::mem::take(&mut self.runs);
            let mut merged = Vec::new();
            let mut iter = old_runs.into_iter();
            loop {
                let group = iter
                    .by_ref()
                    .take(self.max_file_handles)
                    .collect::<Vec<_>>();
                if group.is_empty() {
                    break;
                }
                if group.len() == 1 {
                    merged.push(group.into_iter().next().expect("one run"));
                    continue;
                }
                let path = self.next_run_path();
                let run = self.merge_runs(&path, &group)?;
                for input in group {
                    std::fs::remove_file(&input.path).map_err(|error| {
                        crate::Error::UnexpectedError {
                            message: format!(
                                "Failed to remove merged manifest-sort spill run '{}'",
                                input.path.display()
                            ),
                            source: Some(Box::new(error)),
                        }
                    })?;
                    self.disk_bytes = self.disk_bytes.saturating_sub(input.bytes);
                }
                merged.push(run);
            }
            self.runs = merged;
        }
        Ok(())
    }

    fn merge_runs(&mut self, path: &Path, runs: &[SpillRun]) -> Result<SpillRun> {
        let mut readers = runs
            .iter()
            .map(|run| SpillRunReader::open(&run.path))
            .collect::<Result<Vec<_>>>()?;
        let mut heap = BinaryHeap::new();
        for (source, reader) in readers.iter_mut().enumerate() {
            if let Some(record) = reader.next_record(&self.sort_key)? {
                heap.push(HeapRecord { record, source });
            }
        }

        let sort_key = self.sort_key.clone();
        let records = std::iter::from_fn(|| {
            let item = heap.pop()?;
            let source = item.source;
            let next = readers[source].next_record(&sort_key);
            match next {
                Ok(Some(record)) => heap.push(HeapRecord { record, source }),
                Ok(None) => {}
                Err(error) => return Some(Err(error)),
            }
            Some(Ok(item.record))
        });
        self.write_run_results(path, records)
    }

    fn write_run<I>(&mut self, path: &Path, records: I) -> Result<SpillRun>
    where
        I: Iterator<Item = SortRecord>,
    {
        self.write_run_results(path, records.map(Ok))
    }

    fn write_run_results<I>(&mut self, path: &Path, records: I) -> Result<SpillRun>
    where
        I: Iterator<Item = Result<SortRecord>>,
    {
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .map_err(|error| crate::Error::UnexpectedError {
                message: format!(
                    "Failed to create manifest-sort spill run '{}'",
                    path.display()
                ),
                source: Some(Box::new(error)),
            })?;
        let mut writer = BufWriter::new(file);
        let mut written = 0u64;
        for record in records {
            let record = record?;
            let encoded_len =
                u64::try_from(record.encoded.len()).map_err(|_| crate::Error::UnexpectedError {
                    message: "Manifest-sort spill record length overflows u64".to_string(),
                    source: None,
                })?;
            let record_size = 8u64.saturating_add(encoded_len);
            if self
                .disk_bytes
                .saturating_add(written)
                .saturating_add(record_size)
                > self.spill_max_disk_size
            {
                return Err(crate::Error::UnexpectedError {
                    message: format!(
                        "Manifest sort exceeds local spill disk limit of {} bytes",
                        self.spill_max_disk_size
                    ),
                    source: None,
                });
            }
            writer
                .write_all(&encoded_len.to_le_bytes())
                .and_then(|_| writer.write_all(&record.encoded))
                .map_err(|error| crate::Error::UnexpectedError {
                    message: format!(
                        "Failed to write manifest-sort spill run '{}'",
                        path.display()
                    ),
                    source: Some(Box::new(error)),
                })?;
            written = written.saturating_add(record_size);
        }
        writer
            .flush()
            .map_err(|error| crate::Error::UnexpectedError {
                message: format!(
                    "Failed to flush manifest-sort spill run '{}'",
                    path.display()
                ),
                source: Some(Box::new(error)),
            })?;
        self.disk_bytes = self.disk_bytes.saturating_add(written);
        Ok(SpillRun {
            path: path.to_path_buf(),
            bytes: written,
        })
    }

    fn next_run_path(&mut self) -> PathBuf {
        let path = self
            .temp_dir
            .path()
            .join(format!("manifest-sort-run-{}", self.next_run_id));
        self.next_run_id += 1;
        path
    }
}

pub(crate) enum SortedManifestEntries {
    Memory(std::vec::IntoIter<ManifestEntry>),
    Spilled(SpilledManifestEntries),
}

impl SortedManifestEntries {
    pub(crate) fn next_entry(&mut self) -> Result<Option<ManifestEntry>> {
        match self {
            Self::Memory(entries) => Ok(entries.next()),
            Self::Spilled(entries) => entries.next_entry(),
        }
    }
}

pub(crate) struct SpilledManifestEntries {
    _temp_dir: tempfile::TempDir,
    readers: Vec<SpillRunReader>,
    heap: BinaryHeap<HeapRecord>,
    sort_key: ManifestSortKey,
}

impl SpilledManifestEntries {
    fn new(
        temp_dir: tempfile::TempDir,
        runs: Vec<SpillRun>,
        sort_key: ManifestSortKey,
    ) -> Result<Self> {
        let mut readers = runs
            .iter()
            .map(|run| SpillRunReader::open(&run.path))
            .collect::<Result<Vec<_>>>()?;
        let mut heap = BinaryHeap::new();
        for (source, reader) in readers.iter_mut().enumerate() {
            if let Some(record) = reader.next_record(&sort_key)? {
                heap.push(HeapRecord { record, source });
            }
        }
        Ok(Self {
            _temp_dir: temp_dir,
            readers,
            heap,
            sort_key,
        })
    }

    fn next_entry(&mut self) -> Result<Option<ManifestEntry>> {
        let Some(item) = self.heap.pop() else {
            return Ok(None);
        };
        if let Some(record) = self.readers[item.source].next_record(&self.sort_key)? {
            self.heap.push(HeapRecord {
                record,
                source: item.source,
            });
        }
        Ok(Some(item.record.entry))
    }
}

struct SpillRunReader {
    reader: BufReader<File>,
}

impl SpillRunReader {
    fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).map_err(|error| crate::Error::UnexpectedError {
            message: format!(
                "Failed to open manifest-sort spill run '{}'",
                path.display()
            ),
            source: Some(Box::new(error)),
        })?;
        Ok(Self {
            reader: BufReader::new(file),
        })
    }

    fn next_record(&mut self, sort_key: &ManifestSortKey) -> Result<Option<SortRecord>> {
        let mut length = [0u8; 8];
        let first =
            self.reader
                .read(&mut length[..1])
                .map_err(|error| crate::Error::UnexpectedError {
                    message: "Failed to read manifest-sort spill record length".to_string(),
                    source: Some(Box::new(error)),
                })?;
        if first == 0 {
            return Ok(None);
        }
        self.reader.read_exact(&mut length[1..]).map_err(|error| {
            crate::Error::UnexpectedError {
                message: "Truncated manifest-sort spill record length".to_string(),
                source: Some(Box::new(error)),
            }
        })?;
        let length = usize::try_from(u64::from_le_bytes(length)).map_err(|_| {
            crate::Error::UnexpectedError {
                message: "Manifest-sort spill record length overflows usize".to_string(),
                source: None,
            }
        })?;
        let mut encoded = vec![0u8; length];
        self.reader
            .read_exact(&mut encoded)
            .map_err(|error| crate::Error::UnexpectedError {
                message: "Truncated manifest-sort spill record".to_string(),
                source: Some(Box::new(error)),
            })?;
        SortRecord::from_encoded(encoded, sort_key).map(Some)
    }
}

struct HeapRecord {
    record: SortRecord,
    source: usize,
}

impl PartialEq for HeapRecord {
    fn eq(&self, other: &Self) -> bool {
        compare_sort_records(&self.record, &other.record) == Ordering::Equal
            && self.source == other.source
    }
}

impl Eq for HeapRecord {}

impl PartialOrd for HeapRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_sort_records(&other.record, &self.record)
            .then_with(|| other.source.cmp(&self.source))
    }
}

pub(crate) fn reaches_full_compaction_threshold(
    input: &[ManifestFileMeta],
    target_size: i64,
    threshold_size: i64,
) -> bool {
    input
        .iter()
        .filter(|file| file.num_deleted_files() > 0 || file.file_size() < target_size)
        .map(ManifestFileMeta::file_size)
        .sum::<i64>()
        >= threshold_size
}

/// Return whether a concrete partition may be covered by a manifest's
/// partition statistics.
///
/// Missing or legacy statistics are treated conservatively as overlapping so
/// full compaction never drops a DELETE while retaining its ADD in an
/// unre-written manifest.
pub(crate) fn manifest_may_contain_partition(
    meta: &ManifestFileMeta,
    partition: &[u8],
    partition_fields: &[DataField],
) -> Result<bool> {
    if partition_fields.is_empty() {
        return Ok(true);
    }
    let partition = BinaryRow::from_serialized_bytes(partition)?;
    let min = match BinaryRow::from_serialized_bytes(meta.partition_stats().min_values()) {
        Ok(row) if row.arity() as usize >= partition_fields.len() => row,
        _ => return Ok(true),
    };
    let max = match BinaryRow::from_serialized_bytes(meta.partition_stats().max_values()) {
        Ok(row) if row.arity() as usize >= partition_fields.len() => row,
        _ => return Ok(true),
    };

    for (index, field) in partition_fields.iter().enumerate() {
        let point = extract_datum(&partition, index, field.data_type())?;
        let min_value = extract_datum(&min, index, field.data_type())?;
        let max_value = extract_datum(&max, index, field.data_type())?;
        match point {
            None => match meta.partition_stats().null_counts().get(index) {
                Some(Some(count)) if *count == 0 => return Ok(false),
                _ => {}
            },
            Some(point) => match (min_value, max_value) {
                (Some(min_value), Some(max_value)) => {
                    if datum_cmp(&point, &min_value) == Some(Ordering::Less)
                        || datum_cmp(&point, &max_value) == Some(Ordering::Greater)
                    {
                        return Ok(false);
                    }
                }
                _ => match meta.partition_stats().null_counts().get(index) {
                    // Only a known all-null field can exclude a non-null value.
                    Some(Some(count))
                        if *count == meta.num_added_files() + meta.num_deleted_files() =>
                    {
                        return Ok(false)
                    }
                    // Unknown legacy stats must not be used for exclusion.
                    _ => {}
                },
            },
        }
    }
    Ok(true)
}

pub(crate) fn plan_rewrite(
    input: &[ManifestFileMeta],
    default_compaction: &HashMap<String, bool>,
    sort_key: &ManifestSortKey,
    config: &ManifestSortConfig,
) -> Result<Option<ManifestRewritePlan>> {
    let lsm_files = input
        .iter()
        .filter(|file| !default_compaction.contains_key(file.file_name()))
        .cloned()
        .map(|file| sort_key.range(file))
        .collect::<Result<Vec<_>>>()?;
    let level_runs = build_level_sorted_runs(lsm_files, sort_key);
    let picked_runs = ManifestPickStrategy::new(
        config.max_size_amplification_percent,
        config.sorted_run_size_ratio,
    )
    .pick(&level_runs);

    let mut picked_file_names = HashSet::new();
    let mut picked_files = Vec::new();
    for run_index in picked_runs {
        for file in &level_runs[run_index].files {
            picked_file_names.insert(file.meta.file_name().to_string());
            picked_files.push(file.clone());
        }
    }
    for file in input {
        if default_compaction.contains_key(file.file_name()) {
            picked_file_names.insert(file.file_name().to_string());
            picked_files.push(sort_key.range(file.clone())?);
        }
    }
    if picked_files.is_empty() {
        return Ok(None);
    }

    let sections = merge_small_adjacent_sections(
        split_into_sections(picked_files, default_compaction, sort_key),
        config.target_size,
    );
    Ok(Some(ManifestRewritePlan {
        picked_file_names,
        sections,
    }))
}

fn build_level_sorted_runs(
    mut input: Vec<ManifestRange>,
    sort_key: &ManifestSortKey,
) -> Vec<ManifestAdjacentSortedRun> {
    input.sort_by(|left, right| compare_ranges(left, right, sort_key));
    let mut runs: Vec<Vec<ManifestRange>> = Vec::new();
    for file in input {
        let earliest = runs
            .iter()
            .enumerate()
            .min_by(|(_, left), (_, right)| {
                sort_key.compare_bounds(
                    &left.last().expect("non-empty run").max,
                    &right.last().expect("non-empty run").max,
                )
            })
            .map(|(index, _)| index);
        match earliest {
            Some(index)
                if sort_key
                    .is_after_max(&file.min, &runs[index].last().expect("non-empty run").max) =>
            {
                runs[index].push(file);
            }
            _ => runs.push(vec![file]),
        }
    }

    let mut result = runs
        .into_iter()
        .map(ManifestAdjacentSortedRun::from_sorted)
        .collect::<Vec<_>>();
    result.sort_by_key(|run| run.total_size);
    let first_leveled = result.len().saturating_sub(MAX_SORTED_RUN_LEVEL as usize);
    for (index, run) in result.iter_mut().enumerate() {
        run.level = if index >= first_leveled {
            (index - first_leveled + 1) as i32
        } else {
            0
        };
    }
    result
}

fn split_into_sections(
    mut picked_files: Vec<ManifestRange>,
    default_compaction: &HashMap<String, bool>,
    sort_key: &ManifestSortKey,
) -> Vec<ManifestSection> {
    picked_files.sort_by(|left, right| compare_ranges(left, right, sort_key));
    let mut sections = Vec::new();
    let mut current_files = Vec::new();
    let mut current_size = 0;
    let mut current_has_default = false;
    let mut current_max: Option<ManifestBound> = None;

    for file in picked_files {
        let starts_new = current_max
            .as_ref()
            .is_some_and(|max| sort_key.is_after_max(&file.min, max));
        if starts_new {
            sections.push(ManifestSection {
                files: current_files,
                total_size: current_size,
                has_default_compaction_file: current_has_default,
            });
            current_files = Vec::new();
            current_size = 0;
            current_has_default = false;
            current_max = None;
        }

        current_size += file.meta.file_size();
        current_has_default |= default_compaction.contains_key(file.meta.file_name());
        if current_max
            .as_ref()
            .is_none_or(|max| sort_key.compare_bounds(&file.max, max) == Ordering::Greater)
        {
            current_max = Some(file.max.clone());
        }
        current_files.push(file.meta);
    }

    if !current_files.is_empty() {
        sections.push(ManifestSection {
            files: current_files,
            total_size: current_size,
            has_default_compaction_file: current_has_default,
        });
    }
    sections
}

fn merge_small_adjacent_sections(
    sections: Vec<ManifestSection>,
    target_size: i64,
) -> Vec<ManifestSection> {
    let mut result = Vec::new();
    let mut pending: Option<ManifestSection> = None;
    for section in sections {
        pending = match pending {
            None => Some(section),
            Some(previous)
                if previous.total_size < target_size || section.total_size < target_size =>
            {
                Some(ManifestSection::merge(previous, section))
            }
            Some(previous) => {
                result.push(previous);
                Some(section)
            }
        };
    }
    if let Some(pending) = pending {
        result.push(pending);
    }
    result
}

struct ManifestPickStrategy {
    size_amplification_threshold: i32,
    size_ratio_threshold: i32,
}

impl ManifestPickStrategy {
    fn new(size_amplification_threshold: i32, size_ratio_threshold: i32) -> Self {
        Self {
            size_amplification_threshold,
            size_ratio_threshold,
        }
    }

    /// Return indexes into `level_runs`.
    fn pick(&self, level_runs: &[ManifestAdjacentSortedRun]) -> Vec<usize> {
        if level_runs.len() <= MAX_SORTED_RUN_LEVEL as usize {
            return Vec::new();
        }

        let highest = level_runs.last().expect("non-empty runs");
        let lower_total = level_runs
            .iter()
            .filter(|run| run.level < highest.level)
            .map(|run| run.total_size)
            .sum::<i64>();
        if highest.level > 0
            && lower_total.saturating_mul(100)
                > i64::from(self.size_amplification_threshold).saturating_mul(highest.total_size)
        {
            return (0..level_runs.len()).collect();
        }

        let mut picked = vec![0];
        let mut picked_size = level_runs[0].total_size;
        for (index, run) in level_runs.iter().enumerate().skip(1) {
            if run.level <= 1
                || picked_size.saturating_mul(i64::from(100 + self.size_ratio_threshold))
                    >= run.total_size.saturating_mul(100)
            {
                picked.push(index);
                picked_size += run.total_size;
            }
        }
        if picked.len() == 1 {
            Vec::new()
        } else {
            picked
        }
    }
}

fn compare_ranges(
    left: &ManifestRange,
    right: &ManifestRange,
    sort_key: &ManifestSortKey,
) -> Ordering {
    sort_key
        .compare_bounds(&left.min, &right.min)
        .then_with(|| sort_key.compare_bounds(&left.max, &right.max))
        .then_with(|| left.meta.file_name().cmp(right.meta.file_name()))
}

fn file_kind_order(kind: &FileKind) -> u8 {
    match kind {
        FileKind::Add => 0,
        FileKind::Delete => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{BinaryRowBuilder, DataFileMeta, DataType, IntType};

    fn row(value: i32) -> Vec<u8> {
        let mut builder = BinaryRowBuilder::new(1);
        builder.write_int(0, value);
        builder.build().to_serialized_bytes()
    }

    fn meta(name: &str, size: i64, min: i32, max: i32) -> ManifestFileMeta {
        ManifestFileMeta::new(
            name.to_string(),
            size,
            1,
            0,
            BinaryTableStats::new(row(min), row(max), vec![Some(0)]),
            0,
        )
    }

    fn row_id_meta(
        name: &str,
        size: i64,
        partition: i32,
        min_row_id: i64,
        max_row_id: i64,
    ) -> ManifestFileMeta {
        meta(name, size, partition, partition).with_row_id_stats(Some(min_row_id), Some(max_row_id))
    }

    fn bucket_meta(
        name: &str,
        size: i64,
        partition: i32,
        min_bucket: i32,
        max_bucket: i32,
    ) -> ManifestFileMeta {
        meta(name, size, partition, partition).with_bucket_level_stats(
            Some(min_bucket),
            Some(max_bucket),
            None,
            None,
        )
    }

    fn sort_key() -> ManifestSortKey {
        ManifestSortKey::create(
            &[DataField::new(
                0,
                "pt".to_string(),
                DataType::Int(IntType::new()),
            )],
            None,
            false,
            &[],
            false,
        )
        .unwrap()
        .unwrap()
    }

    fn entry(kind: FileKind, partition: i32, file_name: &str) -> ManifestEntry {
        row_id_entry(kind, partition, file_name, None, 1, 0)
    }

    fn bucket_entry(partition: i32, bucket: i32, file_name: &str) -> ManifestEntry {
        ManifestEntry::new(
            FileKind::Add,
            row(partition),
            bucket,
            4,
            DataFileMeta {
                file_name: file_name.to_string(),
                file_size: 1,
                row_count: 1,
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
                first_row_id: None,
                write_cols: None,
                column_max_sequence_numbers: None,
            },
            2,
        )
    }

    fn row_id_entry(
        kind: FileKind,
        partition: i32,
        file_name: &str,
        first_row_id: Option<i64>,
        row_count: i64,
        max_sequence_number: i64,
    ) -> ManifestEntry {
        ManifestEntry::new(
            kind,
            row(partition),
            0,
            1,
            DataFileMeta {
                file_name: file_name.to_string(),
                file_size: 1,
                row_count,
                min_key: Vec::new(),
                max_key: Vec::new(),
                key_stats: BinaryTableStats::empty(),
                value_stats: BinaryTableStats::empty(),
                min_sequence_number: 0,
                max_sequence_number,
                schema_id: 0,
                level: 0,
                extra_files: Vec::new(),
                creation_time: None,
                delete_row_count: None,
                embedded_index: None,
                file_source: None,
                value_stats_cols: None,
                external_path: None,
                first_row_id,
                write_cols: None,
                column_max_sequence_numbers: None,
            },
            2,
        )
    }

    fn spill_config(
        spill_buffer_size: usize,
        max_file_handles: usize,
        spill_max_disk_size: u64,
    ) -> ManifestSortConfig {
        ManifestSortConfig {
            target_size: 1024,
            merge_min_count: 2,
            full_compaction_threshold_size: 1024,
            max_rewrite_size: 1024,
            max_size_amplification_percent: 200,
            sorted_run_size_ratio: 1,
            spill_buffer_size,
            spill_max_disk_size,
            max_file_handles,
        }
    }

    #[test]
    fn boundary_equality_shares_run_but_splits_sections() {
        let key = sort_key();
        let ranges = vec![
            key.range(meta("a", 100, 0, 10)).unwrap(),
            key.range(meta("b", 100, 10, 20)).unwrap(),
        ];
        let runs = build_level_sorted_runs(ranges.clone(), &key);
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].files.len(), 2);

        let sections = split_into_sections(ranges, &HashMap::new(), &key);
        assert_eq!(sections.len(), 2);
    }

    #[test]
    fn row_id_boundary_equality_overlaps_runs_and_sections() {
        let manifests = vec![
            row_id_meta("a", 100, 0, 0, 10),
            row_id_meta("b", 100, 0, 10, 20),
        ];
        let key = ManifestSortKey::create(
            &[DataField::new(
                0,
                "pt".to_string(),
                DataType::Int(IntType::new()),
            )],
            None,
            true,
            &manifests,
            false,
        )
        .unwrap()
        .unwrap();
        assert!(matches!(key, ManifestSortKey::RowId { .. }));

        let ranges = manifests
            .into_iter()
            .map(|meta| key.range(meta))
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let runs = build_level_sorted_runs(ranges.clone(), &key);
        assert_eq!(runs.len(), 2);

        let sections = split_into_sections(ranges, &HashMap::new(), &key);
        assert_eq!(sections.len(), 1);
    }

    #[test]
    fn row_id_sort_orders_kind_range_sequence_and_name() {
        let manifests = vec![row_id_meta("m", 100, 0, 10, 20)];
        let key = ManifestSortKey::create(
            &[DataField::new(
                0,
                "pt".to_string(),
                DataType::Int(IntType::new()),
            )],
            None,
            true,
            &manifests,
            false,
        )
        .unwrap()
        .unwrap();
        let mut sorter = SpillableManifestSorter::new(&key, &spill_config(1, 2, u64::MAX)).unwrap();
        sorter
            .push(row_id_entry(FileKind::Delete, 0, "delete", Some(1), 1, 0))
            .unwrap();
        sorter
            .push(row_id_entry(FileKind::Add, 0, "later", Some(2), 1, 0))
            .unwrap();
        sorter
            .push(row_id_entry(FileKind::Add, 0, "old", Some(1), 1, 3))
            .unwrap();
        sorter
            .push(row_id_entry(FileKind::Add, 0, "new", Some(1), 1, 5))
            .unwrap();

        let mut sorted = sorter.finish().unwrap();
        let mut actual = Vec::new();
        while let Some(entry) = sorted.next_entry().unwrap() {
            actual.push((*entry.kind(), entry.file().file_name.clone()));
        }
        assert_eq!(
            actual,
            vec![
                (FileKind::Add, "new".to_string()),
                (FileKind::Add, "old".to_string()),
                (FileKind::Add, "later".to_string()),
                (FileKind::Delete, "delete".to_string()),
            ]
        );
    }

    #[test]
    fn bucket_sort_orders_bucket_before_partition() {
        let manifests = vec![bucket_meta("m", 100, 0, 0, 3)];
        let key = ManifestSortKey::create(
            &[DataField::new(
                0,
                "pt".to_string(),
                DataType::Int(IntType::new()),
            )],
            None,
            false,
            &manifests,
            true,
        )
        .unwrap()
        .unwrap();
        assert!(matches!(key, ManifestSortKey::Bucket { .. }));

        let mut sorter = SpillableManifestSorter::new(&key, &spill_config(1, 2, u64::MAX)).unwrap();
        sorter.push(bucket_entry(0, 2, "bucket-2")).unwrap();
        sorter
            .push(bucket_entry(10, 1, "bucket-1-partition-10"))
            .unwrap();
        sorter
            .push(bucket_entry(0, 1, "bucket-1-partition-0"))
            .unwrap();

        let mut sorted = sorter.finish().unwrap();
        let mut actual = Vec::new();
        while let Some(entry) = sorted.next_entry().unwrap() {
            actual.push(entry.file().file_name.clone());
        }
        assert_eq!(
            actual,
            vec![
                "bucket-1-partition-0".to_string(),
                "bucket-1-partition-10".to_string(),
                "bucket-2".to_string(),
            ]
        );
    }

    #[test]
    fn bucket_ranges_fall_back_when_legacy_manifests_lack_bucket_stats() {
        let manifests = vec![meta("a", 100, 0, 0), meta("b", 100, 0, 0)];
        let key = ManifestSortKey::create(&[], None, false, &manifests, true)
            .unwrap()
            .unwrap();
        let ranges = manifests
            .into_iter()
            .map(|meta| key.range(meta))
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(build_level_sorted_runs(ranges.clone(), &key).len(), 2);
        assert_eq!(split_into_sections(ranges, &HashMap::new(), &key).len(), 1);
    }

    #[test]
    fn data_evolution_row_id_sort_precedes_bucket_sort() {
        let manifests = vec![row_id_meta("m", 100, 0, 10, 20).with_bucket_level_stats(
            Some(0),
            Some(3),
            None,
            None,
        )];
        let key = ManifestSortKey::create(&[], None, true, &manifests, true)
            .unwrap()
            .unwrap();
        assert!(matches!(key, ManifestSortKey::RowId { .. }));
    }

    #[test]
    fn pick_strategy_forces_low_levels_once_run_count_exceeds_four() {
        let key = sort_key();
        let files = [100, 200, 1_000, 2_000, 4_000]
            .into_iter()
            .enumerate()
            .map(|(index, size)| key.range(meta(&format!("m-{index}"), size, 0, 10)).unwrap())
            .collect();
        let runs = build_level_sorted_runs(files, &key);
        assert_eq!(runs.len(), 5);
        let picked = ManifestPickStrategy::new(1_000, 1).pick(&runs);
        assert_eq!(picked, vec![0, 1]);
    }

    #[test]
    fn small_adjacent_sections_are_merged() {
        let sections = vec![
            ManifestSection {
                files: vec![meta("a", 40, 0, 1)],
                total_size: 40,
                has_default_compaction_file: false,
            },
            ManifestSection {
                files: vec![meta("b", 200, 2, 3)],
                total_size: 200,
                has_default_compaction_file: true,
            },
        ];
        let merged = merge_small_adjacent_sections(sections, 100);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].files.len(), 2);
        assert!(merged[0].has_default_compaction_file);
    }

    #[test]
    fn spill_sort_is_bounded_and_merges_runs_in_order() {
        let key = sort_key();
        let mut sorter = SpillableManifestSorter::new(&key, &spill_config(1, 2, u64::MAX)).unwrap();
        sorter.push(entry(FileKind::Delete, 1, "b")).unwrap();
        sorter.push(entry(FileKind::Add, 2, "c")).unwrap();
        sorter.push(entry(FileKind::Add, 1, "a")).unwrap();
        sorter.push(entry(FileKind::Add, 1, "b")).unwrap();

        let mut sorted = sorter.finish().unwrap();
        let mut actual = Vec::new();
        while let Some(entry) = sorted.next_entry().unwrap() {
            actual.push((
                entry.partition().to_vec(),
                *entry.kind(),
                entry.file().file_name.clone(),
            ));
        }
        assert_eq!(
            actual,
            vec![
                (row(1), FileKind::Add, "a".to_string()),
                (row(1), FileKind::Add, "b".to_string()),
                (row(1), FileKind::Delete, "b".to_string()),
                (row(2), FileKind::Add, "c".to_string()),
            ]
        );
    }

    #[test]
    fn spill_sort_enforces_disk_limit() {
        let key = sort_key();
        let mut sorter = SpillableManifestSorter::new(&key, &spill_config(1, 2, 1)).unwrap();
        let error = sorter
            .push(entry(FileKind::Add, 1, "too-large"))
            .unwrap_err();
        assert!(error.to_string().contains("spill disk limit"));
    }
}
