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

//! Real per-partition row counts, computed from manifests alone in bounded memory.
//!
//! [`Table::partition_stats`] and the `$files` system table both materialize every
//! manifest entry together with its per-column statistics, which does not fit in
//! memory for very large tables, and summing `row_count` over files over-counts
//! data-evolution tables, where several column-group files cover the same rows.
//!
//! This module instead:
//! - decodes manifests through [`SlimManifestEntry`], which borrows the handful of
//!   fields it needs and skips statistics in place, so nothing per-file is retained;
//! - for data-evolution tables, counts files carrying a `first_row_id` by the
//!   *union* of their row-id ranges, collapsing overlapping column-group/blob files;
//! - nets ADD/DELETE entries with a delete set built only from the manifests that
//!   contain deletes, the same semantics as the scan's manifest merge.
//!
//! Peak memory is bounded by partitions, live DELETE entries, up to one million
//! retained ADD identities, disjoint row-id ranges, and in-flight manifest buffers
//! rather than all live file metadata and column statistics. Highly fragmented
//! row-id space or large embedded indexes may still increase retained state.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};

use crate::io::FileIO;
use crate::spec::avro::{
    visit_slim_index_manifest_entries, visit_slim_manifest_entries, SharedSchemaCache,
    SlimManifestEntry,
};
use crate::spec::{
    BinaryRow, CoreOptions, DataField, FileKind, ManifestFileMeta, ManifestList, Predicate,
};
use crate::table::partition_filter::PartitionFilter;
use crate::table::read_builder::split_scan_predicates;
use crate::table::stats_filter::FileStatsRows;
use crate::table::Table;

/// Independent I/O and blocking decode limits for data manifests.
const MANIFEST_READ_CONCURRENCY: usize = 32;

/// ADD identities retained from delete-bearing manifests to avoid fetching them twice.
/// This bounds entry count, not embedded-index payload bytes.
const RETAINED_ADD_BUDGET: i64 = 1_000_000;

fn manifest_decode_concurrency() -> usize {
    std::thread::available_parallelism()
        .map_or(2, |parallelism| parallelism.get())
        .clamp(2, MANIFEST_READ_CONCURRENCY)
}

const DELETION_VECTORS_INDEX_TYPE: &str = "DELETION_VECTORS";

/// Real row count of one partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionRowCount {
    /// The partition's typed values, one field per partition key.
    pub partition_row: BinaryRow,
    /// Rows in the partition: overlapping data-evolution files are counted once
    /// and deletion-vector rows are subtracted. For primary-key tables this is
    /// the physical count, before merging keys. `None` when it cannot be known
    /// exactly (a file without a row count, or a deletion vector without a
    /// cardinality) — never a guess.
    pub record_count: Option<i64>,
}

/// Disjoint set of inclusive row-id ranges, coalescing overlapping and adjacent ones.
#[derive(Debug, Default)]
struct RowRangeSet {
    ranges: BTreeMap<i64, i64>,
}

impl RowRangeSet {
    fn insert(&mut self, mut start: i64, mut end: i64) {
        if start > end {
            return;
        }
        if let Some((&prev_start, &prev_end)) = self.ranges.range(..=start).next_back() {
            if prev_end >= end {
                return;
            }
            if prev_end.saturating_add(1) >= start {
                start = prev_start;
                self.ranges.remove(&prev_start);
            }
        }
        while let Some((&next_start, &next_end)) =
            self.ranges.range(start..=end.saturating_add(1)).next()
        {
            end = end.max(next_end);
            self.ranges.remove(&next_start);
        }
        self.ranges.insert(start, end);
    }

    fn merge(&mut self, other: RowRangeSet) {
        if self.ranges.is_empty() {
            self.ranges = other.ranges;
            return;
        }
        for (start, end) in other.ranges {
            self.insert(start, end);
        }
    }

    fn total(&self) -> i128 {
        self.ranges
            .iter()
            .map(|(start, end)| i128::from(*end) - i128::from(*start) + 1)
            .sum()
    }
}

#[derive(Debug, Default)]
struct PartitionAccum {
    /// Rows of files without a `first_row_id`.
    plain_rows: i128,
    /// Row-id ranges of files with a `first_row_id`.
    row_ranges: RowRangeSet,
    row_count_unknown: bool,
}

impl PartitionAccum {
    fn add_file(
        &mut self,
        row_count: i64,
        first_row_id: Option<i64>,
        data_evolution_enabled: bool,
    ) {
        if row_count < 0 {
            self.row_count_unknown = true;
            return;
        }
        if data_evolution_enabled {
            if let Some(first) = first_row_id {
                if row_count > 0 {
                    let Some(last) = first.checked_add(row_count - 1) else {
                        self.row_count_unknown = true;
                        return;
                    };
                    self.row_ranges.insert(first, last);
                }
                return;
            }
        }
        self.plain_rows += i128::from(row_count);
    }

    fn merge(&mut self, other: PartitionAccum) {
        self.plain_rows += other.plain_rows;
        self.row_ranges.merge(other.row_ranges);
        self.row_count_unknown |= other.row_count_unknown;
    }

    fn record_count(&self, deleted: Option<Option<i128>>) -> Option<i64> {
        if self.row_count_unknown {
            return None;
        }
        let deleted = deleted.unwrap_or(Some(0))?;
        let rows = self.plain_rows + self.row_ranges.total() - deleted;
        (rows >= 0).then(|| i64::try_from(rows).ok()).flatten()
    }
}

type PartitionAccums = HashMap<Vec<u8>, PartitionAccum>;

fn accumulate(
    accums: &mut PartitionAccums,
    partition: &[u8],
    row_count: i64,
    first_row_id: Option<i64>,
    data_evolution_enabled: bool,
) {
    let accum = match accums.get_mut(partition) {
        Some(accum) => accum,
        None => accums.entry(partition.to_vec()).or_default(),
    };
    accum.add_file(row_count, first_row_id, data_evolution_enabled);
}

/// Identifiers of deleted files, matching the full Paimon `Identifier` semantics.
///
/// Nested so ADD lookups borrow partition/file-name bytes from the decode buffer
/// and each partition is stored only once.
#[derive(Debug, Default)]
struct DeleteSet {
    by_partition: HashMap<Box<[u8]>, DeletedFiles>,
}

type DeletedFiles = HashMap<Box<str>, Vec<DeletedFile>>;

#[derive(Debug, PartialEq, Eq)]
struct DeletedFile {
    bucket: i32,
    level: i32,
    extra_files: Vec<Box<str>>,
    // Exact identity matching requires the payload. A spill-backed delete set can
    // replace this copy if embedded-index memory becomes a measured bottleneck.
    embedded_index: Option<Box<[u8]>>,
    external_path: Option<Box<str>>,
}

impl DeletedFile {
    fn from_entry(entry: &SlimManifestEntry<'_>) -> Self {
        Self {
            bucket: entry.bucket,
            level: entry.level,
            extra_files: entry
                .extra_files
                .iter()
                .map(|value| Box::from(*value))
                .collect(),
            embedded_index: entry.embedded_index.map(Box::from),
            external_path: entry.external_path.map(Box::from),
        }
    }

    fn matches(&self, entry: &SlimManifestEntry<'_>) -> bool {
        self.bucket == entry.bucket
            && self.level == entry.level
            && self.embedded_index.as_deref() == entry.embedded_index
            && self.external_path.as_deref() == entry.external_path
            && self.extra_files.len() == entry.extra_files.len()
            && self
                .extra_files
                .iter()
                .zip(&entry.extra_files)
                .all(|(left, right)| &**left == *right)
    }
}

struct RetainedAdd {
    partition: Arc<[u8]>,
    file_name: Box<str>,
    identity: DeletedFile,
    row_count: i64,
    first_row_id: Option<i64>,
}

#[derive(Default)]
struct PartitionInterner {
    last: Option<Arc<[u8]>>,
}

impl PartitionInterner {
    fn intern(&mut self, partition: &[u8]) -> Arc<[u8]> {
        match &self.last {
            Some(last) if &**last == partition => Arc::clone(last),
            _ => {
                let partition = Arc::from(partition);
                self.last = Some(Arc::clone(&partition));
                partition
            }
        }
    }
}

impl DeleteSet {
    fn is_empty(&self) -> bool {
        self.by_partition.is_empty()
    }

    fn insert(&mut self, entry: &SlimManifestEntry<'_>) {
        let files = match self.by_partition.get_mut(entry.partition) {
            Some(files) => files,
            None => self
                .by_partition
                .entry(Box::from(entry.partition))
                .or_default(),
        };
        let deleted = DeletedFile::from_entry(entry);
        let slots = files.entry(Box::from(entry.file_name)).or_default();
        if !slots.contains(&deleted) {
            slots.push(deleted);
        }
    }

    fn contains(&self, entry: &SlimManifestEntry<'_>) -> bool {
        self.by_partition
            .get(entry.partition)
            .and_then(|files| files.get(entry.file_name))
            .is_some_and(|slots| slots.iter().any(|deleted| deleted.matches(entry)))
    }

    fn contains_retained(&self, add: &RetainedAdd) -> bool {
        self.by_partition
            .get(&*add.partition)
            .and_then(|files| files.get(&*add.file_name))
            .is_some_and(|slots| slots.contains(&add.identity))
    }

    fn merge(&mut self, other: DeleteSet) {
        for (partition, files) in other.by_partition {
            let target = self.by_partition.entry(partition).or_default();
            for (file_name, deleted) in files {
                let slots = target.entry(file_name).or_default();
                for entry in deleted {
                    if !slots.contains(&entry) {
                        slots.push(entry);
                    }
                }
            }
        }
    }
}

/// Entry-level partition filter, remembering the last verdict because manifests
/// list long runs of files from the same partition.
struct PartitionMatcher<'a> {
    filter: Option<&'a PartitionFilter>,
    last_partition: Vec<u8>,
    last_verdict: Option<bool>,
}

impl<'a> PartitionMatcher<'a> {
    fn new(filter: Option<&'a PartitionFilter>) -> Self {
        Self {
            filter,
            last_partition: Vec::new(),
            last_verdict: None,
        }
    }

    fn matches(&mut self, partition: &[u8]) -> crate::Result<bool> {
        let Some(filter) = self.filter else {
            return Ok(true);
        };
        if let Some(verdict) = self.last_verdict {
            if self.last_partition == partition {
                return Ok(verdict);
            }
        }
        let verdict = filter.matches_entry(partition)?;
        self.last_partition.clear();
        self.last_partition.extend_from_slice(partition);
        self.last_verdict = Some(verdict);
        Ok(verdict)
    }
}

struct DeleteManifestSummary {
    deletes: DeleteSet,
    /// `None` when the retention budget ran out and the manifest must be re-read.
    adds: Option<Vec<RetainedAdd>>,
}

fn summarize_delete_manifest(
    bytes: &[u8],
    cache: &SharedSchemaCache,
    budget: &AtomicI64,
    filter: Option<&PartitionFilter>,
) -> crate::Result<DeleteManifestSummary> {
    let mut matcher = PartitionMatcher::new(filter);
    let mut interner = PartitionInterner::default();
    let mut deletes = DeleteSet::default();
    let mut adds = Some(Vec::new());
    visit_slim_manifest_entries(bytes, cache, &mut |entry| {
        if !matcher.matches(entry.partition)? {
            return Ok(());
        }
        match entry.kind {
            FileKind::Delete => deletes.insert(&entry),
            FileKind::Add => {
                if let Some(retained) = adds.as_mut() {
                    if budget.fetch_sub(1, Ordering::Relaxed) > 0 {
                        retained.push(RetainedAdd {
                            partition: interner.intern(entry.partition),
                            file_name: Box::from(entry.file_name),
                            identity: DeletedFile::from_entry(&entry),
                            row_count: entry.row_count,
                            first_row_id: entry.first_row_id,
                        });
                    } else {
                        budget.fetch_add(retained.len() as i64 + 1, Ordering::Relaxed);
                        adds = None;
                    }
                }
            }
        }
        Ok(())
    })?;
    Ok(DeleteManifestSummary { deletes, adds })
}

/// Second-pass result of one manifest: its live ADD files, already aggregated.
fn aggregate_manifest(
    bytes: &[u8],
    cache: &SharedSchemaCache,
    deletes: Option<&DeleteSet>,
    filter: Option<&PartitionFilter>,
    data_evolution_enabled: bool,
) -> crate::Result<PartitionAccums> {
    let mut matcher = PartitionMatcher::new(filter);
    let mut accums = PartitionAccums::new();
    visit_slim_manifest_entries(bytes, cache, &mut |entry| {
        if entry.kind == FileKind::Add
            && matcher.matches(entry.partition)?
            && !deletes.is_some_and(|deletes| deletes.contains(&entry))
        {
            accumulate(
                &mut accums,
                entry.partition,
                entry.row_count,
                entry.first_row_id,
                data_evolution_enabled,
            );
        }
        Ok(())
    })?;
    Ok(accums)
}

/// Fetch and decode at most 32 manifests in one pipeline. A semaphore limits
/// CPU-heavy Avro work on Tokio's blocking pool without adding another buffer.
fn read_manifests<T, F>(
    file_io: &FileIO,
    manifest_dir: &str,
    manifests: Vec<ManifestFileMeta>,
    decode: F,
) -> impl futures::Stream<Item = crate::Result<(ManifestFileMeta, T)>>
where
    T: Send + 'static,
    F: Fn(&[u8]) -> crate::Result<T> + Send + Sync + 'static,
{
    let file_io = file_io.clone();
    let manifest_dir = manifest_dir.to_string();
    let decode = Arc::new(decode);
    let decode_permits = Arc::new(tokio::sync::Semaphore::new(manifest_decode_concurrency()));
    futures::stream::iter(manifests)
        .map(move |meta| {
            let file_io = file_io.clone();
            let path = format!("{}/{}", manifest_dir, meta.file_name());
            let decode = Arc::clone(&decode);
            let decode_permits = Arc::clone(&decode_permits);
            async move {
                let bytes = file_io.new_input(&path)?.read().await?;
                let permit = decode_permits.acquire_owned().await.map_err(|error| {
                    crate::Error::UnexpectedError {
                        message: format!("manifest decode semaphore closed: {error}"),
                        source: Some(Box::new(error)),
                    }
                })?;
                let decoded = tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    decode(&bytes)
                })
                .await
                .map_err(|error| crate::Error::UnexpectedError {
                    message: format!("manifest decode task failed: {error}"),
                    source: Some(Box::new(error)),
                })??;
                Ok((meta, decoded))
            }
        })
        .buffer_unordered(MANIFEST_READ_CONCURRENCY)
}

async fn aggregate_manifests(
    file_io: &FileIO,
    manifest_dir: &str,
    manifests: Vec<ManifestFileMeta>,
    filter: Option<Arc<PartitionFilter>>,
    data_evolution_enabled: bool,
    retained_add_budget: i64,
) -> crate::Result<BTreeMap<Vec<u8>, PartitionAccum>> {
    let cache = SharedSchemaCache::new();
    let (with_deletes, mut second_pass): (Vec<_>, Vec<_>) = manifests
        .into_iter()
        .partition(|meta| meta.num_deleted_files() > 0);

    // Keep a bounded number of ADDs while collecting the global delete set.
    // Only manifests that exceed the shared budget are fetched again.
    let mut deletes = DeleteSet::default();
    let mut retained_adds = Vec::new();
    {
        let cache = cache.clone();
        let filter = filter.clone();
        let budget = AtomicI64::new(retained_add_budget);
        let mut summaries = std::pin::pin!(read_manifests(
            file_io,
            manifest_dir,
            with_deletes,
            move |bytes| summarize_delete_manifest(bytes, &cache, &budget, filter.as_deref()),
        ));
        while let Some((meta, summary)) = summaries.try_next().await? {
            deletes.merge(summary.deletes);
            match summary.adds {
                Some(adds) => retained_adds.extend(adds),
                None => second_pass.push(meta),
            }
        }
    }

    let mut totals: BTreeMap<Vec<u8>, PartitionAccum> = BTreeMap::new();
    let mut retained_accums = PartitionAccums::new();
    for add in retained_adds {
        if !deletes.contains_retained(&add) {
            accumulate(
                &mut retained_accums,
                &add.partition,
                add.row_count,
                add.first_row_id,
                data_evolution_enabled,
            );
        }
    }
    for (partition, accum) in retained_accums {
        totals.entry(partition).or_default().merge(accum);
    }

    // Aggregate manifests without DELETEs and those that exceeded the ADD budget.
    let deletes = (!deletes.is_empty()).then(|| Arc::new(deletes));
    let mut aggregated = std::pin::pin!(read_manifests(
        file_io,
        manifest_dir,
        second_pass,
        move |bytes| {
            aggregate_manifest(
                bytes,
                &cache,
                deletes.as_deref(),
                filter.as_deref(),
                data_evolution_enabled,
            )
        },
    ));
    while let Some((_, accums)) = aggregated.try_next().await? {
        for (partition, accum) in accums {
            totals.entry(partition).or_default().merge(accum);
        }
    }

    Ok(totals)
}

/// Deleted rows per matching partition from deletion vectors; `None` when some
/// vector does not record its cardinality.
async fn deleted_rows_by_partition(
    file_io: &FileIO,
    index_manifest_path: &str,
    filter: Option<Arc<PartitionFilter>>,
) -> crate::Result<HashMap<Vec<u8>, Option<i128>>> {
    let bytes = file_io.new_input(index_manifest_path)?.read().await?;
    tokio::task::spawn_blocking(move || {
        let mut matcher = PartitionMatcher::new(filter.as_deref());
        let mut deleted: HashMap<Vec<u8>, Option<i128>> = HashMap::new();
        visit_slim_index_manifest_entries(&bytes, &SharedSchemaCache::new(), &mut |entry| {
            if entry.kind != FileKind::Add
                || entry.index_type != DELETION_VECTORS_INDEX_TYPE
                || !matcher.matches(entry.partition)?
            {
                return Ok(());
            }
            let Some(cardinality) = entry.deletion_vector_cardinality else {
                return Ok(());
            };
            let slot = match deleted.get_mut(entry.partition) {
                Some(slot) => slot,
                None => deleted.entry(entry.partition.to_vec()).or_insert(Some(0)),
            };
            *slot = match (*slot, cardinality) {
                (Some(total), Some(cardinality)) => total.checked_add(cardinality),
                _ => None,
            };
            Ok(())
        })?;
        Ok(deleted)
    })
    .await
    .map_err(|e| crate::Error::UnexpectedError {
        message: format!("index manifest decode task failed: {e}"),
        source: Some(Box::new(e)),
    })?
}

/// Drop manifests whose partition value range cannot match `filter`.
fn retain_matching_manifests(
    manifests: &mut Vec<ManifestFileMeta>,
    filter: &PartitionFilter,
    partition_fields: &[DataField],
) {
    if partition_fields.is_empty() {
        return;
    }
    manifests.retain(|meta| {
        let stats = meta.partition_stats();
        let file_stats = FileStatsRows::for_manifest_partition(
            meta.num_added_files() + meta.num_deleted_files(),
            BinaryRow::from_serialized_bytes(stats.min_values()).ok(),
            BinaryRow::from_serialized_bytes(stats.max_values()).ok(),
            stats.null_counts().clone(),
        );
        filter.matches_manifest(&file_stats, partition_fields)
    });
}

impl Table {
    /// Real row count of every partition in the latest (or time-travelled) snapshot.
    ///
    /// Reads manifests only — never data files — without retaining every live
    /// file or its column statistics. Memory is bounded by partitions, live
    /// DELETE entries, up to one million retained ADD identities, disjoint
    /// data-evolution row-id ranges, and in-flight manifest buffers.
    /// Data-evolution files sharing a row-id range contribute once. Results are
    /// ordered by serialized partition bytes.
    ///
    /// Returns an empty Vec when the table has no snapshots yet.
    pub async fn partition_row_counts(&self) -> crate::Result<Vec<PartitionRowCount>> {
        self.partition_row_counts_with_filter(None).await
    }

    /// [`Table::partition_row_counts`] restricted to the partitions matching `filter`.
    ///
    /// `filter` may only reference partition columns: anything else cannot be
    /// decided from manifests, and is rejected rather than ignored. Manifests
    /// whose partition range cannot match are never fetched.
    pub async fn partition_row_counts_with_filter(
        &self,
        filter: Option<Predicate>,
    ) -> crate::Result<Vec<PartitionRowCount>> {
        let schema = self.schema();
        let core = CoreOptions::new(schema.options());
        // Manifests carry partition values.
        core.ensure_read_authorized()?;

        let file_io = self.file_io();
        let Some(snapshot) = super::time_travel::resolve_snapshot(self).await? else {
            return Ok(Vec::new());
        };

        let manifest_sm = self.snapshot_manager();
        let base_path = manifest_sm.manifest_path(snapshot.base_manifest_list());
        let delta_path = manifest_sm.manifest_path(snapshot.delta_manifest_list());
        let (mut manifests, delta) = futures::try_join!(
            ManifestList::read(file_io, &base_path),
            ManifestList::read(file_io, &delta_path),
        )?;
        manifests.extend(delta);

        let partition_fields = schema.partition_fields();
        let partition_filter = match filter {
            None => None,
            Some(filter) => {
                let (partition_predicate, data_predicates) = split_scan_predicates(self, filter);
                if !data_predicates.is_empty() {
                    return Err(crate::Error::Unsupported {
                        message: "partition row counts can only be filtered by partition columns"
                            .to_string(),
                    });
                }
                partition_predicate
                    .map(|predicate| PartitionFilter::from_predicate(predicate, &partition_fields))
            }
        };
        if let Some(filter) = &partition_filter {
            retain_matching_manifests(&mut manifests, filter, &partition_fields);
        }

        let partition_filter = partition_filter.map(Arc::new);
        let totals = aggregate_manifests(
            file_io,
            &manifest_sm.manifest_dir(),
            manifests,
            partition_filter.clone(),
            core.data_evolution_enabled(),
            RETAINED_ADD_BUDGET,
        )
        .await?;

        let deleted_rows = match snapshot.index_manifest() {
            Some(index_manifest) if core.deletion_vectors_enabled() => {
                deleted_rows_by_partition(
                    file_io,
                    &manifest_sm.manifest_path(index_manifest),
                    partition_filter,
                )
                .await?
            }
            _ => HashMap::new(),
        };

        let mut out = Vec::with_capacity(totals.len());
        for (partition_bytes, accum) in totals {
            out.push(PartitionRowCount {
                partition_row: BinaryRow::from_serialized_bytes(&partition_bytes)?,
                record_count: accum.record_count(deleted_rows.get(&partition_bytes).copied()),
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{
        DataFileMeta, DeletionVectorMeta, IndexFileMeta, IndexManifest, IndexManifestEntry,
        Manifest, ManifestEntry,
    };

    const MANIFEST_DIR: &str = "memory:/partition_row_count/manifest";

    fn partition(value: u8) -> Vec<u8> {
        vec![0, 0, 0, 1, value, 0, 0, 0]
    }

    fn file(name: &str, level: i32, row_count: i64, first_row_id: Option<i64>) -> DataFileMeta {
        let stats = BinaryTableStats::empty();
        DataFileMeta {
            file_name: name.to_string(),
            file_size: 100,
            row_count,
            min_key: vec![],
            max_key: vec![],
            key_stats: stats.clone(),
            value_stats: stats,
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id: 0,
            level,
            extra_files: vec![],
            creation_time: None,
            delete_row_count: None,
            embedded_index: None,
            file_source: None,
            value_stats_cols: None,
            external_path: None,
            first_row_id,
            write_cols: None,
            column_max_sequence_numbers: None,
        }
    }

    fn add(partition: Vec<u8>, file: DataFileMeta) -> ManifestEntry {
        ManifestEntry::new(FileKind::Add, partition, 0, 1, file, 2)
    }

    fn delete(partition: Vec<u8>, file: DataFileMeta) -> ManifestEntry {
        ManifestEntry::new(FileKind::Delete, partition, 0, 1, file, 2)
    }

    async fn write_manifest(
        file_io: &FileIO,
        name: &str,
        entries: &[ManifestEntry],
    ) -> ManifestFileMeta {
        Manifest::write(file_io, &format!("{MANIFEST_DIR}/{name}"), entries)
            .await
            .unwrap();
        let deleted = entries
            .iter()
            .filter(|e| *e.kind() == FileKind::Delete)
            .count() as i64;
        ManifestFileMeta::new(
            name.to_string(),
            1,
            entries.len() as i64 - deleted,
            deleted,
            BinaryTableStats::empty(),
            0,
        )
    }

    #[tokio::test]
    async fn test_deletion_vector_counts_respect_partition_filter() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        file_io.mkdirs(&format!("{MANIFEST_DIR}/")).await.unwrap();
        let path = format!("{MANIFEST_DIR}/filtered-index-manifest");
        let entry = |value, cardinality| IndexManifestEntry {
            version: 1,
            kind: FileKind::Add,
            partition: partition(value),
            bucket: 0,
            index_file: IndexFileMeta {
                index_type: DELETION_VECTORS_INDEX_TYPE.to_string(),
                file_name: format!("index-{value}"),
                file_size: 1,
                row_count: 1,
                deletion_vectors_ranges: Some(indexmap::IndexMap::from([(
                    format!("data-{value}"),
                    DeletionVectorMeta {
                        offset: 0,
                        length: 1,
                        cardinality: Some(cardinality),
                    },
                )])),
                external_path: None,
                global_index_meta: None,
            },
        };
        IndexManifest::write(&file_io, &path, &[entry(1, 3), entry(2, 7)])
            .await
            .unwrap();
        let filter = PartitionFilter::PartitionSet {
            partitions: std::collections::HashSet::from([partition(1)]),
            bounds: Vec::new(),
        };

        let deleted = deleted_rows_by_partition(&file_io, &path, Some(Arc::new(filter)))
            .await
            .unwrap();

        assert_eq!(deleted, HashMap::from([(partition(1), Some(3))]));
    }

    async fn counts(
        test: &str,
        manifests: Vec<Vec<ManifestEntry>>,
        data_evolution_enabled: bool,
        retained_add_budget: i64,
    ) -> BTreeMap<Vec<u8>, i128> {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        file_io.mkdirs(&format!("{MANIFEST_DIR}/")).await.unwrap();
        let mut metas = Vec::new();
        for (i, entries) in manifests.iter().enumerate() {
            metas.push(write_manifest(&file_io, &format!("{test}-{i}"), entries).await);
        }
        aggregate_manifests(
            &file_io,
            MANIFEST_DIR,
            metas,
            None,
            data_evolution_enabled,
            retained_add_budget,
        )
        .await
        .unwrap()
        .into_iter()
        .map(|(partition, accum)| {
            assert!(!accum.row_count_unknown);
            (partition, accum.plain_rows + accum.row_ranges.total())
        })
        .collect()
    }

    #[test]
    fn test_row_range_set_coalesces_overlapping_and_adjacent_ranges() {
        let mut set = RowRangeSet::default();
        set.insert(0, 99);
        set.insert(0, 99); // column-group file over the same rows
        set.insert(0, 39); // blob file inside the data file's range
        set.insert(200, 299);
        assert_eq!(set.total(), 200);
        assert_eq!(set.ranges.len(), 2);

        set.insert(100, 199); // adjacent on both sides
        assert_eq!(set.total(), 300);
        assert_eq!(set.ranges.len(), 1);

        set.insert(250, 349); // partial overlap
        set.insert(5, 4); // empty
        assert_eq!(set.total(), 350);
        assert_eq!(set.ranges.len(), 1);

        let mut other = RowRangeSet::default();
        other.insert(340, 399);
        other.insert(1000, 1009);
        set.merge(other);
        assert_eq!(set.total(), 410);
        assert_eq!(set.ranges.len(), 2);
    }

    #[tokio::test]
    async fn test_data_evolution_files_sharing_rows_count_once() {
        let result = counts(
            "de",
            vec![
                vec![
                    add(partition(1), file("base-0", 0, 100, Some(0))),
                    add(partition(1), file("base-1", 0, 50, Some(100))),
                    add(partition(2), file("other", 0, 7, Some(150))),
                ],
                // Column-group files written later over the same row-id ranges.
                vec![
                    add(partition(1), file("cols-0", 0, 100, Some(0))),
                    add(partition(1), file("cols-1", 0, 50, Some(100))),
                ],
            ],
            true,
            RETAINED_ADD_BUDGET,
        )
        .await;

        assert_eq!(result[&partition(1)], 150);
        assert_eq!(result[&partition(2)], 7);
    }

    #[tokio::test]
    async fn test_row_ranges_are_not_merged_without_data_evolution() {
        let result = counts(
            "row-tracking",
            vec![vec![
                add(partition(1), file("a", 0, 100, Some(0))),
                add(partition(1), file("b", 0, 100, Some(0))),
            ]],
            false,
            RETAINED_ADD_BUDGET,
        )
        .await;

        assert_eq!(result[&partition(1)], 200);
    }

    #[tokio::test]
    async fn test_deleted_files_are_netted_by_complete_identity() {
        for budget in [RETAINED_ADD_BUDGET, 0, 1] {
            let mut external_add = file("external", 0, 1, None);
            external_add.external_path = Some("file:///a".to_string());
            let mut external_delete = external_add.clone();
            external_delete.external_path = Some("file:///b".to_string());

            let mut extra_add = file("extra", 0, 1, None);
            extra_add.extra_files = vec!["a.idx".to_string()];
            let mut extra_delete = extra_add.clone();
            extra_delete.extra_files = vec!["b.idx".to_string()];

            let mut embedded_add = file("embedded", 0, 1, None);
            embedded_add.embedded_index = Some(vec![1]);
            let mut embedded_delete = embedded_add.clone();
            embedded_delete.embedded_index = Some(vec![2]);
            let mut embedded_match = file("embedded-match", 0, 1, None);
            embedded_match.embedded_index = Some(vec![3]);
            let embedded_match_delete = embedded_match.clone();

            let result = counts(
                &format!("net-{budget}"),
                vec![
                    vec![
                        add(partition(1), file("a", 0, 10, None)),
                        add(partition(1), file("b", 0, 20, None)),
                        add(partition(2), file("gone", 0, 5, None)),
                    ],
                    vec![
                        delete(partition(1), file("a", 0, 10, None)),
                        delete(partition(1), file("b", 0, 20, None)),
                        delete(partition(1), external_delete),
                        delete(partition(1), extra_delete),
                        delete(partition(1), embedded_delete),
                        delete(partition(1), embedded_match_delete),
                        add(partition(1), external_add),
                        add(partition(1), extra_add),
                        add(partition(1), embedded_add),
                        add(partition(1), embedded_match),
                        add(partition(1), file("c", 5, 30, None)),
                        add(partition(1), file("up", 0, 4, None)),
                        delete(partition(2), file("gone", 0, 5, None)),
                    ],
                    vec![
                        delete(partition(1), file("up", 0, 4, None)),
                        add(partition(1), file("up", 5, 4, None)),
                    ],
                ],
                false,
                budget,
            )
            .await;

            assert_eq!(result[&partition(1)], 37, "budget {budget}");
            assert!(!result.contains_key(&partition(2)), "budget {budget}");
        }
    }

    #[tokio::test]
    async fn test_unknown_row_count_is_reported_as_unknown() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        file_io.mkdirs(&format!("{MANIFEST_DIR}/")).await.unwrap();
        let meta = write_manifest(
            &file_io,
            "unknown-0",
            &[
                add(partition(1), file("known", 0, 10, None)),
                add(
                    partition(1),
                    file("unknown", 0, DataFileMeta::ROW_COUNT_UNKNOWN, None),
                ),
            ],
        )
        .await;
        let totals = aggregate_manifests(
            &file_io,
            MANIFEST_DIR,
            vec![meta],
            None,
            false,
            RETAINED_ADD_BUDGET,
        )
        .await
        .unwrap();
        assert!(totals[&partition(1)].row_count_unknown);
    }

    #[test]
    fn test_invalid_row_id_range_is_unknown() {
        let mut invalid = PartitionAccum::default();
        invalid.add_file(2, Some(i64::MAX), true);
        assert_eq!(invalid.record_count(None), None);

        let mut valid = PartitionAccum::default();
        valid.add_file(1, Some(i64::MAX), true);
        assert_eq!(valid.record_count(None), Some(1));
    }

    /// The slim decoder must agree with the full decoder on every field it reads.
    #[test]
    fn test_slim_decode_matches_full_decode() {
        let path = std::env::current_dir()
            .unwrap()
            .join("tests/fixtures/manifest/manifest-8ded1f09-fcda-489e-9167-582ac0f9f846-0");
        let bytes = std::fs::read(path).unwrap();
        let full = crate::spec::avro::from_avro_bytes_fast::<ManifestEntry>(&bytes).unwrap();

        let mut slim = Vec::new();
        visit_slim_manifest_entries(&bytes, &SharedSchemaCache::new(), &mut |e| {
            let file = full[slim.len()].file();
            assert_eq!(
                e.extra_files,
                file.extra_files
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
            );
            assert_eq!(e.embedded_index, file.embedded_index.as_deref());
            assert_eq!(e.external_path, file.external_path.as_deref());
            slim.push((
                e.kind,
                e.partition.to_vec(),
                e.bucket,
                e.level,
                e.file_name.to_string(),
                e.row_count,
                e.first_row_id,
            ));
            Ok(())
        })
        .unwrap();

        let expected: Vec<_> = full
            .iter()
            .map(|e| {
                let f = e.file();
                (
                    *e.kind(),
                    e.partition().to_vec(),
                    e.bucket(),
                    f.level,
                    f.file_name.clone(),
                    f.row_count,
                    f.first_row_id,
                )
            })
            .collect();
        assert!(!expected.is_empty());
        assert_eq!(slim, expected);
    }
}
