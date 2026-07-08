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

//! TableScan for full table scan.
//!
//! Reference: [pypaimon.read.table_scan.TableScan](https://github.com/apache/paimon/blob/release-1.3/paimon-python/pypaimon/read/table_scan.py)
//! and [FullStartingScanner](https://github.com/apache/paimon/blob/release-1.3/paimon-python/pypaimon/read/scanner/full_starting_scanner.py).

use super::bucket_filter::compute_target_buckets;
use super::kv_file_reader::retain_primary_key_conjuncts;
use super::partition_filter::PartitionFilter;
use super::stats_filter::{
    data_evolution_group_matches_predicates, data_file_matches_predicates,
    data_file_matches_predicates_for_table, group_by_overlapping_row_id, FileStatsRows,
    ResolvedStatsSchema,
};
use super::Table;
use crate::io::FileIO;
use crate::spec::{
    avro::SharedSchemaCache, bucket_dir_name, BinaryRow, BucketFunctionType, CoreOptions,
    DataField, DataFileMeta, FileKind, GlobalIndexSearchMode, IndexManifest, ManifestEntry,
    PartitionComputer, Predicate, Snapshot, ROW_ID_FIELD_ID, ROW_ID_FIELD_NAME,
    SEQUENCE_NUMBER_FIELD_ID, SEQUENCE_NUMBER_FIELD_NAME, VALUE_KIND_FIELD_ID,
    VALUE_KIND_FIELD_NAME,
};
use crate::table::bin_pack::split_for_batch;
use crate::table::merge_tree_split_generator::{
    merge_tree_split_for_batch, KeyComparator, SplitGroup,
};
use crate::table::schema_manager::SchemaManager;
use crate::table::source::{
    any_range_overlaps_file, intersect_ranges_with_file, merge_row_ranges, DataSplit,
    DataSplitBuilder, DeletionFile, PartitionBucket, Plan, RowRange,
};
use crate::table::ScanTrace;
use crate::table::SnapshotManager;
use futures::{StreamExt, TryStreamExt};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Path segment for manifest directory under table.
const MANIFEST_DIR: &str = "manifest";
/// Path segment for index directory under table.
const INDEX_DIR: &str = "index";

#[derive(Debug, Default)]
struct ManifestReadCounters {
    entries_read: usize,
    pruned_by_bucket: usize,
    pruned_by_partition: usize,
    after_entry_pruning: usize,
    pruned_by_level: usize,
    pruned_by_data_stats: usize,
    after_manifest_filters: usize,
}

impl ManifestReadCounters {
    fn merge(&mut self, other: Self) {
        self.entries_read += other.entries_read;
        self.pruned_by_bucket += other.pruned_by_bucket;
        self.pruned_by_partition += other.pruned_by_partition;
        self.after_entry_pruning += other.after_entry_pruning;
        self.pruned_by_level += other.pruned_by_level;
        self.pruned_by_data_stats += other.pruned_by_data_stats;
        self.after_manifest_filters += other.after_manifest_filters;
    }
}

/// Reads a manifest list file (Avro) and returns manifest file metas.
async fn read_manifest_list(
    file_io: &FileIO,
    table_path: &str,
    list_name: &str,
) -> crate::Result<Vec<crate::spec::ManifestFileMeta>> {
    if list_name.is_empty() {
        return Ok(Vec::new());
    }
    let path = format!(
        "{}/{}/{}",
        table_path.trim_end_matches('/'),
        MANIFEST_DIR,
        list_name
    );
    let input = file_io.new_input(&path)?;
    let bytes = input.read().await?;
    crate::spec::avro::from_avro_bytes_fast::<crate::spec::ManifestFileMeta>(&bytes)
}

/// Reads all manifest entries for a snapshot (base + delta manifest lists, then each manifest file).
/// Applies filters during concurrent manifest reading to reduce entries early:
/// - Manifest-file-level partition stats pruning (skip entire manifest files)
/// - Level-0 filtering per entry (DV mode or FirstRow engine)
/// - Partition predicate filtering per entry
/// - Data-level stats pruning per entry (current schema only, cross-schema fail-open)
#[allow(clippy::too_many_arguments)]
async fn read_all_manifest_entries(
    file_io: &FileIO,
    table_path: &str,
    snapshot: &Snapshot,
    skip_level_zero: bool,
    scan_all_files: bool,
    has_primary_keys: bool,
    partition_filter: Option<&PartitionFilter>,
    partition_fields: &[DataField],
    data_predicates: &[Predicate],
    current_schema_id: i64,
    schema_fields: &[DataField],
    bucket_predicate: Option<&Predicate>,
    bucket_key_fields: &[DataField],
    bucket_function_type: BucketFunctionType,
    trace: Option<&mut ScanTrace>,
) -> crate::Result<Vec<ManifestEntry>> {
    let (mut manifest_files, delta) = futures::try_join!(
        read_manifest_list(file_io, table_path, snapshot.base_manifest_list()),
        read_manifest_list(file_io, table_path, snapshot.delta_manifest_list()),
    )?;
    let mut trace = trace;
    if let Some(trace) = trace.as_deref_mut() {
        trace.record_manifest_lists(manifest_files.len(), delta.len());
    }
    manifest_files.extend(delta);

    // Manifest-file-level partition stats pruning: skip entire manifest files
    // whose partition range doesn't overlap the partition predicate.
    let manifest_files_before_partition_pruning = manifest_files.len();
    if let Some(pf) = partition_filter {
        if !partition_fields.is_empty() {
            manifest_files.retain(|meta| {
                let stats = meta.partition_stats();
                let min_values = BinaryRow::from_serialized_bytes(stats.min_values()).ok();
                let max_values = BinaryRow::from_serialized_bytes(stats.max_values()).ok();
                let null_counts = stats.null_counts().clone();
                let file_stats = FileStatsRows::for_manifest_partition(
                    meta.num_added_files() + meta.num_deleted_files(),
                    min_values,
                    max_values,
                    null_counts,
                );
                pf.matches_manifest(&file_stats, partition_fields)
            });
        }
    }
    if let Some(trace) = trace.as_deref_mut() {
        trace.manifest_files_before_partition_pruning = manifest_files_before_partition_pruning;
        trace.manifest_files_after_partition_pruning = manifest_files.len();
    }

    let manifest_path_prefix = format!("{}/{}", table_path.trim_end_matches('/'), MANIFEST_DIR);
    let shared_cache = SharedSchemaCache::new();
    let manifest_results: Vec<(Vec<ManifestEntry>, ManifestReadCounters)> =
        futures::stream::iter(manifest_files)
            .map(|meta| {
                let path = format!("{}/{}", manifest_path_prefix, meta.file_name());
                let cache = shared_cache.clone();
                async move {
                    let input_file = file_io.new_input(&path)?;
                    let content = input_file.read().await?;

                    // Per-task bucket cache (few distinct total_buckets values per manifest).
                    let mut bucket_cache: HashMap<i32, Option<HashSet<i32>>> = HashMap::new();
                    let mut counters = ManifestReadCounters::default();

                    let entries = crate::spec::avro::from_manifest_bytes_filtered_shared(
                        &content,
                        &cache,
                        &mut |_kind, partition_bytes, bucket, total_buckets| {
                            counters.entries_read += 1;
                            // Bucket filter (negative bucket = unassigned)
                            if has_primary_keys && !scan_all_files && bucket < 0 {
                                counters.pruned_by_bucket += 1;
                                return false;
                            }
                            if let Some(pred) = bucket_predicate {
                                let targets =
                                    bucket_cache.entry(total_buckets).or_insert_with(|| {
                                        compute_target_buckets(
                                            pred,
                                            bucket_key_fields,
                                            bucket_function_type,
                                            total_buckets,
                                        )
                                    });
                                if let Some(targets) = targets {
                                    if !targets.contains(&bucket) {
                                        counters.pruned_by_bucket += 1;
                                        return false;
                                    }
                                }
                            }

                            // Partition filter
                            if let Some(pf) = partition_filter {
                                match pf.matches_entry(partition_bytes) {
                                    Ok(false) => {
                                        counters.pruned_by_partition += 1;
                                        return false;
                                    }
                                    Ok(true) => {}
                                    Err(_) => {}
                                }
                            }

                            true
                        },
                    )?;
                    counters.after_entry_pruning = entries.len();

                    // Post-filter: level-0 and data predicates (need DataFileMeta)
                    let mut filtered = Vec::with_capacity(entries.len());
                    for entry in entries {
                        if skip_level_zero && has_primary_keys && entry.file().level == 0 {
                            counters.pruned_by_level += 1;
                            continue;
                        }
                        if !data_predicates.is_empty()
                            && !data_file_matches_predicates(
                                entry.file(),
                                data_predicates,
                                current_schema_id,
                                schema_fields,
                            )
                        {
                            counters.pruned_by_data_stats += 1;
                            continue;
                        }
                        filtered.push(entry);
                    }
                    counters.after_manifest_filters = filtered.len();
                    Ok::<_, crate::Error>((filtered, counters))
                }
            })
            .buffered(64)
            .try_collect::<Vec<_>>()
            .await?;

    let mut counters = ManifestReadCounters::default();
    let mut all_entries = Vec::new();
    for (entries, manifest_counters) in manifest_results {
        counters.merge(manifest_counters);
        all_entries.extend(entries);
    }
    if let Some(trace) = trace {
        trace.manifest_entries_read = counters.entries_read;
        trace.manifest_entries_pruned_by_bucket = counters.pruned_by_bucket;
        trace.manifest_entries_pruned_by_partition = counters.pruned_by_partition;
        trace.manifest_entries_after_entry_pruning = counters.after_entry_pruning;
        trace.manifest_entries_pruned_by_level = counters.pruned_by_level;
        trace.manifest_entries_pruned_by_data_stats = counters.pruned_by_data_stats;
        trace.manifest_entries_after_manifest_filters = counters.after_manifest_filters;
    }
    Ok(all_entries)
}

/// Builds a map from (partition, bucket) to (data_file_name -> DeletionFile) from index manifest entries.
/// Only considers ADD entries with index_type "DELETION_VECTORS" and their deletion_vectors_ranges.
fn build_deletion_files_map(
    index_entries: &[crate::spec::IndexManifestEntry],
    table_path: &str,
) -> HashMap<PartitionBucket, HashMap<String, DeletionFile>> {
    use crate::spec::FileKind;
    let table_path = table_path.trim_end_matches('/');
    let index_path_prefix = format!("{table_path}/{INDEX_DIR}");
    let mut map: HashMap<PartitionBucket, HashMap<String, DeletionFile>> =
        HashMap::with_capacity(index_entries.len());
    for entry in index_entries {
        if entry.kind != FileKind::Add {
            continue;
        }
        if entry.index_file.index_type != "DELETION_VECTORS" {
            continue;
        }
        let ranges = match &entry.index_file.deletion_vectors_ranges {
            Some(r) if !r.is_empty() => r,
            _ => continue,
        };
        let key = PartitionBucket::new(entry.partition.clone(), entry.bucket);
        let dv_path = format!("{}/{}", index_path_prefix, entry.index_file.file_name);
        let per_bucket = map.entry(key).or_default();
        for (data_file_name, meta) in ranges {
            per_bucket.insert(
                data_file_name.clone(),
                DeletionFile::new(
                    dv_path.clone(),
                    meta.offset as i64,
                    meta.length as i64,
                    meta.cardinality,
                ),
            );
        }
    }
    map
}

/// Nets add/delete manifest entries for a scan, returning only the live ADD set.
///
/// Mirrors Java `AbstractFileStoreScan.readAndMergeFileEntries`: first collect
/// the full [`Identifier`] of every DELETE entry, then keep the ADD entries
/// whose identifier is not in that set. The identity is the complete Paimon file
/// identity (`partition, bucket, level, file_name, extra_files, embedded_index,
/// external_path`, matching Java `FileEntry.Identifier`).
///
/// Keying on file name alone is wrong: a single-run compaction upgrades a file
/// *in place* — `DELETE f@oldLevel` plus `ADD f@newLevel` with the same file
/// name (`PojoDataFileMeta.upgrade` reuses the name, only changing `level`). An
/// identity without `level` lets the DELETE cancel the upgraded ADD, dropping
/// the file from the scan and silently losing its rows on read.
///
/// Collecting deletes first (rather than insert/remove while iterating) makes
/// the result independent of ADD/DELETE ordering, matching the Java scan path.
fn merge_manifest_entries(entries: Vec<ManifestEntry>) -> Vec<ManifestEntry> {
    use crate::spec::Identifier;
    let deleted: HashSet<Identifier> = entries
        .iter()
        .filter(|e| *e.kind() == FileKind::Delete)
        .map(|e| e.identifier())
        .collect();
    entries
        .into_iter()
        .filter(|e| *e.kind() == FileKind::Add && !deleted.contains(&e.identifier()))
        .collect()
}

/// Whether scan-owned pruning still preserves `merged_row_count()` as a safe
/// row-count hint.
///
/// Data predicates and row ranges can reduce rows within a split after planning,
/// so split-level row counts stop being a conservative bound for final rows.
pub(super) fn can_push_down_limit_hint_for_scan(
    data_predicates: &[Predicate],
    row_ranges: Option<&[RowRange]>,
) -> bool {
    data_predicates.is_empty() && row_ranges.is_none()
}

#[derive(Debug)]
struct LimitPushdownResult {
    splits: Vec<DataSplit>,
    split_candidates_built: usize,
    limit_early_stopped: bool,
}

#[derive(Debug)]
struct LimitPushdownAccumulator {
    limit: usize,
    fallback_splits: Vec<DataSplit>,
    limited_splits: Vec<DataSplit>,
    scanned_row_count: i64,
    limit_early_stopped: bool,
}

impl LimitPushdownAccumulator {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            fallback_splits: Vec::new(),
            limited_splits: Vec::new(),
            scanned_row_count: 0,
            limit_early_stopped: limit == 0,
        }
    }

    fn push(&mut self, split: DataSplit) -> bool {
        if self.limit_early_stopped {
            return true;
        }

        if let Some(merged_count) = split.merged_row_count() {
            self.fallback_splits.push(split.clone());
            self.limited_splits.push(split);
            self.scanned_row_count += merged_count;
            self.limit_early_stopped = self.scanned_row_count >= self.limit as i64;
        } else {
            self.fallback_splits.push(split);
        }

        self.limit_early_stopped
    }

    fn finish(self) -> LimitPushdownResult {
        let split_candidates_built = self.fallback_splits.len();
        let splits = if self.limit_early_stopped {
            self.limited_splits
        } else {
            self.fallback_splits
        };

        LimitPushdownResult {
            splits,
            split_candidates_built,
            limit_early_stopped: self.limit_early_stopped,
        }
    }
}

type BucketDataFileGroups = HashMap<(Vec<u8>, i32), (i32, Vec<DataFileMeta>)>;

fn global_index_detail_data_ranges(groups: &BucketDataFileGroups) -> Vec<RowRange> {
    let mut ranges = Vec::new();
    for (_, data_files) in groups.values() {
        for file in data_files {
            if let Some((from, to)) = file.row_id_range() {
                ranges.push(RowRange::new(from, to));
            }
        }
    }
    merge_row_ranges(ranges)
}

fn should_skip_level_zero_for_scan(
    scan_all_files: bool,
    has_primary_keys: bool,
    deletion_vectors_enabled: bool,
    merge_engine: crate::Result<crate::spec::MergeEngine>,
) -> bool {
    if scan_all_files {
        return false;
    }
    if !has_primary_keys {
        return false;
    }

    deletion_vectors_enabled || merge_engine.is_ok_and(|e| e == crate::spec::MergeEngine::FirstRow)
}

fn is_system_field_id(field_id: i32) -> bool {
    matches!(
        field_id,
        ROW_ID_FIELD_ID | SEQUENCE_NUMBER_FIELD_ID | VALUE_KIND_FIELD_ID
    )
}

fn is_system_field_name(name: &str) -> bool {
    matches!(
        name,
        ROW_ID_FIELD_NAME | SEQUENCE_NUMBER_FIELD_NAME | VALUE_KIND_FIELD_NAME
    )
}

fn is_vector_store_file_name(file_name: &str) -> bool {
    file_name.to_ascii_lowercase().contains(".vector.")
}

fn is_normal_data_file(file: &DataFileMeta) -> bool {
    !crate::table::dedicated_format_file_writer::is_blob_file_name(&file.file_name)
        && !is_vector_store_file_name(&file.file_name)
}

type DataFileFieldIdsCache = HashMap<(i64, Option<Vec<String>>), HashSet<i32>>;

fn data_evolution_representative_file(group: &[DataFileMeta]) -> crate::Result<usize> {
    let mut representative: Option<usize> = None;
    for (idx, file) in group.iter().enumerate() {
        if !is_normal_data_file(file) {
            continue;
        }
        let should_replace = match representative {
            None => true,
            Some(current_idx) => {
                let current = &group[current_idx];
                (file.max_sequence_number, file.file_name.as_str())
                    < (current.max_sequence_number, current.file_name.as_str())
            }
        };
        if should_replace {
            representative = Some(idx);
        }
    }
    representative.ok_or_else(|| crate::Error::DataInvalid {
        message: "Data-evolution row range group requires at least one normal data file."
            .to_string(),
        source: None,
    })
}

async fn resolve_data_file_field_ids(
    table_schema_id: i64,
    table_fields: &[DataField],
    schema_manager: &SchemaManager,
    file: &DataFileMeta,
) -> crate::Result<HashSet<i32>> {
    let schema;
    let fields = if file.schema_id == table_schema_id {
        table_fields
    } else {
        schema = schema_manager.schema(file.schema_id).await?;
        schema.fields()
    };

    let field_id_by_name = fields
        .iter()
        .map(|field| (field.name(), field.id()))
        .collect::<HashMap<_, _>>();

    let mut field_ids = HashSet::new();
    match file.write_cols.as_ref() {
        None => {
            field_ids.extend(
                fields
                    .iter()
                    .filter(|field| !is_system_field_id(field.id()))
                    .map(|field| field.id()),
            );
        }
        Some(write_cols) => {
            for col in write_cols {
                if is_system_field_name(col) {
                    continue;
                }
                let Some(field_id) = field_id_by_name.get(col.as_str()) else {
                    return Err(crate::Error::DataInvalid {
                        message: format!(
                            "Cannot find write column '{}' in schema {}.",
                            col, file.schema_id
                        ),
                        source: None,
                    });
                };
                if !is_system_field_id(*field_id) {
                    field_ids.insert(*field_id);
                }
            }
        }
    }
    Ok(field_ids)
}

async fn data_file_field_ids(
    table_schema_id: i64,
    table_fields: &[DataField],
    schema_manager: &SchemaManager,
    file: &DataFileMeta,
    field_ids_cache: &mut DataFileFieldIdsCache,
) -> crate::Result<HashSet<i32>> {
    let key = (file.schema_id, file.write_cols.clone());
    if let Some(field_ids) = field_ids_cache.get(&key) {
        return Ok(field_ids.clone());
    }

    let field_ids =
        resolve_data_file_field_ids(table_schema_id, table_fields, schema_manager, file).await?;
    field_ids_cache.insert(key, field_ids.clone());
    Ok(field_ids)
}

async fn prune_data_evolution_group_by_read_fields(
    group: Vec<DataFileMeta>,
    read_field_ids: &HashSet<i32>,
    deletion_vectors_enabled: bool,
    table_schema_id: i64,
    table_fields: &[DataField],
    schema_manager: &SchemaManager,
    field_ids_cache: &mut DataFileFieldIdsCache,
) -> crate::Result<Vec<DataFileMeta>> {
    if read_field_ids.is_empty() || group.len() <= 1 {
        return Ok(group);
    }

    let anchor_idx = if deletion_vectors_enabled {
        Some(data_evolution_representative_file(&group)?)
    } else {
        None
    };

    let mut keep = Vec::with_capacity(group.len());
    for (idx, file) in group.iter().enumerate() {
        let file_field_ids = data_file_field_ids(
            table_schema_id,
            table_fields,
            schema_manager,
            file,
            field_ids_cache,
        )
        .await?;
        if file_field_ids
            .iter()
            .any(|field_id| read_field_ids.contains(field_id))
        {
            keep.push(idx);
        }
    }
    if let Some(anchor_idx) = anchor_idx {
        if !keep.contains(&anchor_idx) {
            keep.push(anchor_idx);
        }
    }

    if keep.is_empty() {
        keep.push(data_evolution_representative_file(&group)?);
    } else if keep.iter().any(|idx| !is_normal_data_file(&group[*idx]))
        && !keep.iter().any(|idx| is_normal_data_file(&group[*idx]))
    {
        let representative_idx = data_evolution_representative_file(&group)?;
        if !keep.contains(&representative_idx) {
            keep.push(representative_idx);
        }
    }

    let mut files = group.into_iter().map(Some).collect::<Vec<_>>();
    Ok(keep
        .into_iter()
        .filter_map(|idx| files.get_mut(idx).and_then(Option::take))
        .collect())
}

/// TableScan for full table scan (no incremental, no predicate).
///
/// Reference: [pypaimon.read.table_scan.TableScan](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/read/table_scan.py)
#[derive(Debug, Clone)]
pub struct TableScan<'a> {
    table: &'a Table,
    partition_filter: Option<PartitionFilter>,
    data_predicates: Vec<Predicate>,
    bucket_predicate: Option<Predicate>,
    /// Optional limit on the number of rows to return.
    /// When set, the scan will try to return only enough splits to satisfy the limit.
    limit: Option<usize>,
    row_ranges: Option<Vec<RowRange>>,
    /// When true, disables level-0 filtering so all files are visible.
    /// Used by non-read paths (overwrite, truncate, writer restore) that need
    /// the complete file set. Normal read scans leave this as `false`.
    scan_all_files: bool,
    projected_read_field_ids: Option<HashSet<i32>>,
}

impl<'a> TableScan<'a> {
    pub(crate) fn new(
        table: &'a Table,
        partition_filter: Option<PartitionFilter>,
        data_predicates: Vec<Predicate>,
        bucket_predicate: Option<Predicate>,
        limit: Option<usize>,
        row_ranges: Option<Vec<RowRange>>,
    ) -> Self {
        Self {
            table,
            partition_filter,
            data_predicates,
            bucket_predicate,
            limit,
            row_ranges,
            scan_all_files: false,
            projected_read_field_ids: None,
        }
    }

    /// Disable level-0 filtering so all files are visible.
    ///
    /// Used by non-read paths (overwrite, truncate, writer restore) that need
    /// the complete file set regardless of merge engine or DV settings.
    pub fn with_scan_all_files(mut self) -> Self {
        self.scan_all_files = true;
        self.projected_read_field_ids = None;
        self
    }

    /// Set row ranges for scan-time filtering.
    ///
    /// This replaces any existing row_ranges. Typically used to inject
    /// results from global index lookups (e.g. full-text search).
    pub fn with_row_ranges(mut self, ranges: Vec<RowRange>) -> Self {
        self.row_ranges = if ranges.is_empty() {
            None
        } else {
            Some(ranges)
        };
        self
    }

    pub(super) fn with_projected_read_field_ids(
        mut self,
        projected_read_field_ids: Option<HashSet<i32>>,
    ) -> Self {
        self.projected_read_field_ids = projected_read_field_ids;
        self
    }

    /// Plan the full scan: resolve snapshot (via options or latest), then read manifests and build DataSplits.
    ///
    /// Time travel is resolved from table options:
    /// - only one of `scan.version`, `scan.timestamp-millis`,
    ///   `scan.snapshot-id`, `scan.tag-name` may be set
    /// - `scan.version` → tag name (if exists) → snapshot id (if parseable) →
    ///   error (ambiguous by design, like SQL `VERSION AS OF`)
    /// - `scan.snapshot-id` → snapshot id only (never a tag lookup)
    /// - `scan.tag-name` → tag name only (never parsed as a snapshot id)
    /// - `scan.timestamp-millis` → find the latest snapshot <= that timestamp
    /// - otherwise → read the latest snapshot
    ///
    /// Reference: [TimeTravelUtil.tryTravelToSnapshot](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/table/source/snapshot/TimeTravelUtil.java)
    /// for `scan.version`; the strict selectors mirror Java's typed
    /// `scan.snapshot-id` / `scan.tag-name` handling.
    pub async fn plan(&self) -> crate::Result<Plan> {
        self.ensure_query_auth_allowed()?;
        let data_evolution_read_field_ids = self.projected_read_field_ids()?;
        let snapshot = match self.resolve_snapshot().await? {
            Some(snapshot) => snapshot,
            None => return Ok(Plan::new(Vec::new())),
        };
        self.plan_snapshot(snapshot, data_evolution_read_field_ids.as_ref(), None)
            .await
    }

    /// Plan the full scan and return metadata-pruning trace counters.
    pub async fn plan_with_trace(&self) -> crate::Result<(Plan, ScanTrace)> {
        self.ensure_query_auth_allowed()?;
        let data_evolution_read_field_ids = self.projected_read_field_ids()?;
        let mut trace = ScanTrace {
            limit: self.limit,
            ..Default::default()
        };
        let snapshot = match self.resolve_snapshot().await? {
            Some(snapshot) => snapshot,
            None => return Ok((Plan::new(Vec::new()), trace)),
        };
        trace.snapshot_id = Some(snapshot.id());
        let plan = self
            .plan_snapshot(
                snapshot,
                data_evolution_read_field_ids.as_ref(),
                Some(&mut trace),
            )
            .await?;
        Ok((plan, trace))
    }

    /// Fail closed for a `query-auth.enabled` table: scan planning — including
    /// `with_scan_all_files`, which read-facing system tables like `files` use —
    /// exposes file paths, row counts, and stats the client can't authorize.
    fn ensure_query_auth_allowed(&self) -> crate::Result<()> {
        CoreOptions::new(self.table.schema().options()).ensure_read_authorized()
    }

    fn projected_read_field_ids(&self) -> crate::Result<Option<HashSet<i32>>> {
        Ok(self.projected_read_field_ids.clone())
    }

    async fn resolve_snapshot(&self) -> crate::Result<Option<Snapshot>> {
        // A table copy produced by `copy_with_time_travel` already resolved
        // the selector in its options; reuse it instead of re-reading
        // tag/snapshot files on every plan.
        if let Some(snapshot) = self.table.travel_snapshot() {
            return Ok(Some(snapshot.clone()));
        }
        // A time-travelled schema without its resolved snapshot means the
        // selector was changed after the travel (`copy_with_options`).
        // Resolving the new selector here would evolve a different snapshot's
        // files to the stale historical schema, so fail instead.
        if self.table.is_time_traveled() {
            return Err(crate::Error::DataInvalid {
                message: "Table options changed after time travel; \
                          use copy_with_time_travel to re-resolve the snapshot and schema"
                    .to_string(),
                source: None,
            });
        }

        let file_io = self.table.file_io();
        let table_path = self.table.location();

        match super::time_travel::travel_to_snapshot(
            file_io,
            table_path,
            self.table.schema().options(),
        )
        .await?
        {
            Some(snapshot) => Ok(Some(snapshot)),
            None => {
                let snapshot_manager =
                    SnapshotManager::new(file_io.clone(), table_path.to_string());
                snapshot_manager.get_latest_snapshot().await
            }
        }
    }

    /// Apply a limit-pushdown hint to the generated splits.
    ///
    /// Mirrors Java `DataTableBatchScan#applyPushDownLimit`: splits whose
    /// `merged_row_count()` is unknown (for example merge-needed PK splits or
    /// unknown deletion cardinality) are skipped — they contribute an unknown
    /// number of rows, so they cannot help satisfy the limit. Pruning is
    /// committed only once the accumulated known row count reaches the limit;
    /// if it never does, the original split list is returned unchanged. The
    /// caller or query engine must still enforce the final LIMIT.
    #[cfg(test)]
    fn apply_limit_pushdown(&self, splits: Vec<DataSplit>) -> Vec<DataSplit> {
        let limit = match self.limit {
            Some(l) => l,
            None => return splits,
        };
        let mut accumulator = LimitPushdownAccumulator::new(limit);

        for split in splits {
            if accumulator.push(split) {
                break;
            }
        }

        accumulator.finish().splits
    }

    /// Read all manifest entries from a snapshot, applying filters and merging.
    ///
    /// This is the shared entry point used by both `plan_snapshot` (scan) and
    /// `TableCommit` (overwrite). Filters include partition predicate, data
    /// predicates, and bucket predicate.
    pub(crate) async fn plan_manifest_entries(
        &self,
        snapshot: &Snapshot,
    ) -> crate::Result<Vec<ManifestEntry>> {
        self.plan_manifest_entries_with_trace(snapshot, None).await
    }

    async fn plan_manifest_entries_with_trace(
        &self,
        snapshot: &Snapshot,
        mut trace: Option<&mut ScanTrace>,
    ) -> crate::Result<Vec<ManifestEntry>> {
        let file_io = self.table.file_io();
        let table_path = self.table.location();
        let core_options = CoreOptions::new(self.table.schema().options());
        let data_evolution_enabled = core_options.data_evolution_enabled();

        let has_primary_keys = !self.table.schema().primary_keys().is_empty();
        let deletion_vectors_enabled = core_options.deletion_vectors_enabled();

        // Skip level-0 files for PK tables when:
        // - DV mode: level-0 files are unmerged, DV handles dedup at higher levels
        // - FirstRow engine without DV: reads go through DataFileReader (no merge),
        //   so only compacted (level > 0) files are safe to read directly
        // Deduplicate engine always uses KeyValueFileReader which handles level-0
        // via sort-merge, so level-0 files must remain visible.
        //
        // Non-read paths (overwrite, truncate, writer restore) set scan_all_files=true
        // to see all files including level-0, matching Java's CommitScanner behavior.
        let skip_level_zero = should_skip_level_zero_for_scan(
            self.scan_all_files,
            has_primary_keys,
            deletion_vectors_enabled,
            core_options.merge_engine(),
        );

        let partition_fields = self.table.schema().partition_fields();

        let pushdown_data_predicates = if data_evolution_enabled {
            Vec::new()
        } else {
            self.stats_pruning_predicates()
        };

        let bucket_key_fields: Vec<DataField> = if self.bucket_predicate.is_none() {
            Vec::new()
        } else {
            let bucket_keys = core_options.bucket_key().unwrap_or_else(|| {
                if has_primary_keys {
                    self.table.schema().trimmed_primary_keys()
                } else {
                    Vec::new()
                }
            });
            bucket_keys
                .iter()
                .filter_map(|key| {
                    self.table
                        .schema()
                        .fields()
                        .iter()
                        .find(|f| f.name() == key)
                        .cloned()
                })
                .collect::<Vec<_>>()
        };
        let bucket_function_type = core_options.bucket_function_type()?;

        let entries = read_all_manifest_entries(
            file_io,
            table_path,
            snapshot,
            skip_level_zero,
            self.scan_all_files,
            has_primary_keys,
            self.partition_filter.as_ref(),
            &partition_fields,
            &pushdown_data_predicates,
            self.table.schema().id(),
            self.table.schema().fields(),
            self.bucket_predicate.as_ref(),
            &bucket_key_fields,
            bucket_function_type,
            trace.as_deref_mut(),
        )
        .await?;
        let merged = merge_manifest_entries(entries);
        if let Some(trace) = trace {
            trace.manifest_entries_after_merge = merged.len();
        }
        Ok(merged)
    }

    fn can_push_down_limit_hint(&self, row_ranges: Option<&[RowRange]>) -> bool {
        can_push_down_limit_hint_for_scan(&self.data_predicates, row_ranges)
    }

    /// The predicate set that may prune WHOLE FILES by their stats.
    ///
    /// For primary-key tables read by merging, only key conjuncts are safe: a
    /// key's versions agree on the key columns but not on value columns, so a
    /// value conjunct could prune the file holding the newest version and
    /// resurrect an older one from a surviving file. The dropped conjuncts
    /// are still enforced exactly by the post-merge residual filter in
    /// `KeyValueFileReader`.
    ///
    /// Exempt (full predicates kept):
    /// - Deletion-vector tables: they read raw with per-row masks, stats are
    ///   a superset of live rows, full pruning stays safe.
    /// - `merge-engine=first-row`: planned with `skip_level_zero` and read
    ///   via `DataFileReader` (see `TableRead::to_arrow`), no merge on the
    ///   read path — pruning a file drops exactly the rows the raw path's
    ///   exact residual filter would drop anyway. If first-row ever gains a
    ///   merge read path, this exemption must be revisited.
    fn stats_pruning_predicates(&self) -> Vec<Predicate> {
        let has_primary_keys = !self.table.schema().primary_keys().is_empty();
        let core_options = CoreOptions::new(self.table.schema().options());
        let deletion_vectors_enabled = core_options.deletion_vectors_enabled();
        // An unknown merge engine stays conservative (key-only pruning); the
        // read side fails on it anyway before returning rows.
        let first_row = matches!(
            core_options.merge_engine(),
            Ok(crate::spec::MergeEngine::FirstRow)
        );
        if has_primary_keys && !deletion_vectors_enabled && !first_row {
            retain_primary_key_conjuncts(
                &self.data_predicates,
                self.table.schema().fields(),
                &self.table.schema().trimmed_primary_keys(),
            )
        } else {
            self.data_predicates.clone()
        }
    }

    async fn plan_snapshot(
        &self,
        snapshot: Snapshot,
        data_evolution_read_field_ids: Option<&HashSet<i32>>,
        mut trace: Option<&mut ScanTrace>,
    ) -> crate::Result<Plan> {
        let file_io = self.table.file_io();
        let table_path = self.table.location();
        let table_schema_id = self.table.schema().id();
        let table_fields = self.table.schema().fields();
        let schema_manager = self.table.schema_manager();
        let core_options = CoreOptions::new(self.table.schema().options());
        let data_evolution_enabled = core_options.data_evolution_enabled();
        let deletion_vectors_enabled = core_options.deletion_vectors_enabled();
        let target_split_size = core_options.source_split_target_size();
        let open_file_cost = core_options.source_split_open_file_cost();
        let partition_keys = self.table.schema().partition_keys();

        let entries = self
            .plan_manifest_entries_with_trace(&snapshot, trace.as_deref_mut())
            .await?;
        if entries.is_empty() {
            if let Some(trace) = trace {
                trace.record_final_plan(0, 0, 0);
            }
            return Ok(Plan::new(Vec::new()));
        }

        // For non-data-evolution tables, cross-schema files were kept (fail-open)
        // by the pushdown. Apply the full schema-aware filter for those files.
        let stats_pruning_predicates = self.stats_pruning_predicates();
        let entries = if stats_pruning_predicates.is_empty() || data_evolution_enabled {
            entries
        } else {
            let current_schema_id = self.table.schema().id();
            let has_cross_schema = entries
                .iter()
                .any(|e| e.file().schema_id != current_schema_id);
            if !has_cross_schema {
                if let Some(trace) = trace.as_deref_mut() {
                    trace.manifest_entries_after_cross_schema_stats = entries.len();
                }
                entries
            } else {
                let before = entries.len();
                let mut kept = Vec::with_capacity(entries.len());
                let mut schema_cache: HashMap<i64, Option<Arc<ResolvedStatsSchema>>> =
                    HashMap::new();
                for entry in entries {
                    if entry.file().schema_id == current_schema_id
                        || data_file_matches_predicates_for_table(
                            self.table,
                            entry.file(),
                            &stats_pruning_predicates,
                            &mut schema_cache,
                        )
                        .await
                    {
                        kept.push(entry);
                    }
                }
                if let Some(trace) = trace.as_deref_mut() {
                    trace.manifest_entries_pruned_by_cross_schema_stats += before - kept.len();
                    trace.manifest_entries_after_cross_schema_stats = kept.len();
                }
                kept
            }
        };
        if entries.is_empty() {
            if let Some(trace) = trace {
                trace.record_final_plan(0, 0, 0);
            }
            return Ok(Plan::new(Vec::new()));
        } else if let Some(trace) = trace.as_deref_mut() {
            if trace.manifest_entries_after_cross_schema_stats == 0 {
                trace.manifest_entries_after_cross_schema_stats = entries.len();
            }
        }

        if matches!(self.limit, Some(0)) {
            if let Some(trace) = trace {
                trace.record_final_plan_with_limit(0, 0, 0, 0, true);
            }
            return Ok(Plan::new(Vec::new()));
        }

        // Group by (partition, bucket), decomposing entries to avoid cloning partition.
        let mut groups: BucketDataFileGroups = HashMap::with_capacity(entries.len());
        for e in entries {
            let (partition, bucket, total_buckets, file) = e.into_parts();
            let entry = groups
                .entry((partition, bucket))
                .or_insert_with(|| (total_buckets, Vec::new()));
            entry.1.push(file);
        }

        let global_index_search_mode = if data_evolution_enabled
            && core_options.global_index_enabled()
            && !self.data_predicates.is_empty()
        {
            Some(core_options.global_index_search_mode()?)
        } else {
            None
        };
        let global_index_detail_data_ranges = if matches!(
            global_index_search_mode,
            Some(GlobalIndexSearchMode::Detail)
        ) {
            global_index_detail_data_ranges(&groups)
        } else {
            Vec::new()
        };
        let btree_index_fallback_scan_max_size =
            core_options.btree_index_fallback_scan_max_size()?;
        let bitmap_index_fallback_scan_max_size =
            core_options.bitmap_index_fallback_scan_max_size()?;

        let snapshot_id = snapshot.id();
        let base_path = table_path.trim_end_matches('/');
        let mut splits = Vec::with_capacity(groups.len());

        let partition_computer = if !partition_keys.is_empty() {
            Some(PartitionComputer::new(
                partition_keys,
                self.table.schema().fields(),
                core_options.partition_default_name(),
                core_options.legacy_partition_name(),
            )?)
        } else {
            None
        };

        // Primary-key tables must keep key-overlapping files in one split so the
        // sort-merge reader sees every version of a key. The comparator decodes
        // the trimmed-PK min/max keys written by the kv writer.
        //
        // Deletion-vector and first-row tables read without merging (stale rows
        // are masked by DVs / level-0 is skipped), so they keep plain size-based
        // packing like Java's MergeTreeSplitGenerator fast path.
        let read_merges_overlapping_keys = !core_options.deletion_vectors_enabled()
            && !matches!(
                core_options.merge_engine(),
                Ok(crate::spec::MergeEngine::FirstRow)
            );
        let pk_comparator = if read_merges_overlapping_keys {
            KeyComparator::from_table_schema(self.table.schema())
        } else {
            None
        };

        // Read deletion vector index manifest once (like Java generateSplits / scanDvIndex).
        let (deletion_files_map, effective_row_ranges) =
            if let Some(index_manifest_name) = snapshot.index_manifest() {
                let index_manifest_path = format!("{base_path}/{MANIFEST_DIR}");
                let path = format!("{index_manifest_path}/{index_manifest_name}");
                let index_entries = IndexManifest::read(file_io, &path).await?;
                let dv_map = build_deletion_files_map(&index_entries, base_path);

                // Use pushed-down row_ranges first; otherwise try global index.
                let row_ranges = if self.row_ranges.is_some() {
                    self.row_ranges.clone()
                } else if let Some(search_mode) = global_index_search_mode {
                    super::global_index_scanner::evaluate_global_index(
                        super::global_index_scanner::GlobalIndexEvaluation {
                            file_io,
                            table_path: base_path,
                            index_entries: &index_entries,
                            predicates: &self.data_predicates,
                            schema_fields: self.table.schema().fields(),
                            search_mode,
                            btree_fallback_scan_max_size: btree_index_fallback_scan_max_size,
                            bitmap_fallback_scan_max_size: bitmap_index_fallback_scan_max_size,
                            next_row_id: snapshot.next_row_id(),
                            data_ranges: &global_index_detail_data_ranges,
                        },
                    )
                    .await?
                } else {
                    None
                };

                (Some(dv_map), row_ranges)
            } else {
                (None, self.row_ranges.clone())
            };

        let mut data_file_field_ids_cache = DataFileFieldIdsCache::new();
        let can_push_down_limit = self.can_push_down_limit_hint(effective_row_ranges.as_deref());
        let mut limit_accumulator = match self.limit {
            Some(limit) if limit > 0 && can_push_down_limit => {
                Some(LimitPushdownAccumulator::new(limit))
            }
            _ => None,
        };

        'groups: for ((partition, bucket), (total_buckets, data_files)) in groups {
            let partition_row = BinaryRow::from_serialized_bytes(&partition)?;
            let bucket_path = if let Some(ref computer) = partition_computer {
                let partition_path = computer.generate_partition_path(&partition_row)?;
                format!("{base_path}/{partition_path}{}", bucket_dir_name(bucket))
            } else {
                format!("{base_path}/{}", bucket_dir_name(bucket))
            };

            // Original `partition` Vec consumed by PartitionBucket for DV map lookup.
            let per_bucket_deletion_map = deletion_files_map
                .as_ref()
                .and_then(|map| map.get(&PartitionBucket::new(partition, bucket)));

            // Data-evolution tables merge overlapping row-id groups column-wise during read.
            // Keep that split boundary intact and only bin-pack single-file groups.
            // Apply group-level predicate filtering after grouping by row_id range.
            let file_groups: Vec<SplitGroup> = if data_evolution_enabled {
                let row_id_groups = group_by_overlapping_row_id(data_files);
                if let Some(trace) = trace.as_deref_mut() {
                    trace.data_evolution_groups_before_stats += row_id_groups.len();
                }

                // Filter groups by merged stats before splitting.
                let row_id_groups: Vec<Vec<DataFileMeta>> = if self.data_predicates.is_empty() {
                    row_id_groups
                } else {
                    let before = row_id_groups.len();
                    let groups = row_id_groups
                        .into_iter()
                        .filter(|group| {
                            data_evolution_group_matches_predicates(
                                group,
                                &self.data_predicates,
                                self.table.schema().fields(),
                            )
                        })
                        .collect::<Vec<_>>();
                    if let Some(trace) = trace.as_deref_mut() {
                        trace.data_evolution_groups_pruned_by_stats += before - groups.len();
                    }
                    groups
                };

                // Filter groups by row ID ranges.
                let row_id_groups = if let Some(ref ranges) = effective_row_ranges {
                    let before = row_id_groups.len();
                    let groups = row_id_groups
                        .into_iter()
                        .filter(|group| group.iter().any(|f| any_range_overlaps_file(ranges, f)))
                        .collect::<Vec<_>>();
                    if let Some(trace) = trace.as_deref_mut() {
                        trace.data_evolution_groups_pruned_by_row_ranges += before - groups.len();
                    }
                    groups
                } else {
                    row_id_groups
                };

                let row_id_groups = if let Some(read_field_ids) = data_evolution_read_field_ids {
                    if read_field_ids.is_empty() {
                        row_id_groups
                    } else {
                        let mut pruned = Vec::with_capacity(row_id_groups.len());
                        for group in row_id_groups {
                            pruned.push(
                                prune_data_evolution_group_by_read_fields(
                                    group,
                                    read_field_ids,
                                    deletion_vectors_enabled,
                                    table_schema_id,
                                    table_fields,
                                    schema_manager,
                                    &mut data_file_field_ids_cache,
                                )
                                .await?,
                            );
                        }
                        pruned
                    }
                } else {
                    row_id_groups
                };

                let (singles, multis): (Vec<_>, Vec<_>) = row_id_groups
                    .into_iter()
                    .partition(|group| group.len() == 1);

                let mut result = Vec::new();
                for group in multis {
                    // Files sharing a row-id range hold column slices of the
                    // same logical rows; physical counts overcount them
                    // (Java DataEvolutionSplitGenerator: not raw convertible).
                    result.push(SplitGroup {
                        files: group,
                        raw_convertible: false,
                    });
                }

                let single_files: Vec<DataFileMeta> = singles.into_iter().flatten().collect();
                for file_group in split_for_batch(single_files, target_split_size, open_file_cost) {
                    result.push(SplitGroup {
                        files: file_group,
                        raw_convertible: true,
                    });
                }

                result
            } else if let Some(ref comparator) = pk_comparator {
                // Merge-tree path: keep key-overlapping files in one split and
                // mark which splits the sort-merge reader can skip (mirrors
                // Java MergeTreeSplitGenerator#splitForBatch). Only engines
                // whose writer deduplicates at flush guarantee a file never
                // holds two rows of one key, so only they may mark groups raw
                // convertible; see merge_tree_split_for_batch. (First-row
                // tables do not take this path today, but its writer dedups
                // too, so keep the gate accurate.)
                let file_keys_unique = matches!(
                    core_options.merge_engine(),
                    Ok(crate::spec::MergeEngine::Deduplicate)
                        | Ok(crate::spec::MergeEngine::FirstRow)
                );
                merge_tree_split_for_batch(
                    data_files,
                    comparator,
                    target_split_size,
                    open_file_cost,
                    file_keys_unique,
                )
            } else {
                split_for_batch(data_files, target_split_size, open_file_cost)
                    .into_iter()
                    .map(|files| SplitGroup {
                        files,
                        raw_convertible: true,
                    })
                    .collect()
            };

            for group in file_groups {
                let SplitGroup {
                    files: file_group,
                    raw_convertible,
                } = group;
                let data_deletion_files = per_bucket_deletion_map.map(|per_bucket| {
                    file_group
                        .iter()
                        .map(|f| per_bucket.get(&f.file_name).cloned())
                        .collect::<Vec<Option<DeletionFile>>>()
                });

                // Compute row_ranges before moving file_group to avoid clone
                let split_row_ranges = if let Some(ref ranges) = effective_row_ranges {
                    let mut split_ranges = Vec::new();
                    for file in &file_group {
                        split_ranges.extend(intersect_ranges_with_file(ranges, file));
                    }
                    let split_ranges = merge_row_ranges(split_ranges);
                    if split_ranges.is_empty() {
                        None
                    } else {
                        Some(split_ranges)
                    }
                } else {
                    None
                };

                let mut builder = DataSplitBuilder::new()
                    .with_snapshot(snapshot_id)
                    .with_partition(partition_row.clone())
                    .with_bucket(bucket)
                    .with_bucket_path(bucket_path.clone())
                    .with_total_buckets(total_buckets)
                    .with_data_files(file_group)
                    .with_raw_convertible(raw_convertible);
                if let Some(files) = data_deletion_files {
                    builder = builder.with_data_deletion_files(files);
                }
                if let Some(row_ranges) = split_row_ranges {
                    builder = builder.with_row_ranges(row_ranges);
                }
                let split = builder.build()?;
                if let Some(accumulator) = limit_accumulator.as_mut() {
                    if accumulator.push(split) {
                        break 'groups;
                    }
                } else {
                    splits.push(split);
                }
            }
        }

        let (splits, split_candidates_built, limit_early_stopped) =
            if let Some(accumulator) = limit_accumulator {
                let result = accumulator.finish();
                (
                    result.splits,
                    result.split_candidates_built,
                    result.limit_early_stopped,
                )
            } else {
                let split_candidates_built = splits.len();
                (splits, split_candidates_built, false)
            };
        let splits_before_limit = split_candidates_built;
        if let Some(trace) = trace {
            let final_files = splits.iter().map(|split| split.data_files().len()).sum();
            trace.record_final_plan_with_limit(
                split_candidates_built,
                splits_before_limit,
                splits.len(),
                final_files,
                limit_early_stopped,
            );
        }

        Ok(Plan::new(splits))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        prune_data_evolution_group_by_read_fields, should_skip_level_zero_for_scan,
        LimitPushdownAccumulator, TableScan,
    };
    use crate::catalog::Identifier;
    use crate::io::FileIOBuilder;
    use crate::spec::{
        stats::BinaryTableStats, ArrayType, BinaryRow, BinaryRowBuilder, BucketFunctionType,
        DataField, DataFileMeta, DataType, Datum, DeletionVectorMeta, FileKind, IndexFileMeta,
        IndexManifestEntry, IntType, Predicate, PredicateBuilder, PredicateOperator,
        Schema as PaimonSchema, TableSchema, VarCharType,
    };
    use crate::table::bucket_filter::{compute_target_buckets, extract_predicate_for_keys};
    use crate::table::partition_filter::PartitionFilter;
    use crate::table::source::{DataSplit, DataSplitBuilder, DeletionFile};
    use crate::table::stats_filter::{
        data_evolution_group_matches_predicates, data_file_matches_predicates,
        group_by_overlapping_row_id,
    };
    use crate::table::{CommitMessage, Table, TableCommit};
    use crate::Error;
    use bytes::Bytes;
    use chrono::{DateTime, Utc};
    use std::collections::{HashMap, HashSet};

    /// Helper to build a DataFileMeta with data evolution fields.
    fn make_evo_file(
        name: &str,
        file_size: i64,
        row_count: i64,
        max_seq: i64,
        first_row_id: Option<i64>,
    ) -> DataFileMeta {
        DataFileMeta {
            file_name: name.to_string(),
            file_size,
            row_count,
            min_key: Vec::new(),
            max_key: Vec::new(),
            key_stats: BinaryTableStats::new(Vec::new(), Vec::new(), Vec::new()),
            value_stats: BinaryTableStats::new(Vec::new(), Vec::new(), Vec::new()),
            min_sequence_number: 0,
            max_sequence_number: max_seq,
            schema_id: 0,
            level: 0,
            extra_files: Vec::new(),
            creation_time: DateTime::<Utc>::from_timestamp(0, 0),
            delete_row_count: None,
            embedded_index: None,
            first_row_id,
            write_cols: None,
            external_path: None,
            file_source: None,
            value_stats_cols: None,
        }
    }

    fn make_evo_file_with_cols(
        name: &str,
        row_count: i64,
        max_seq: i64,
        first_row_id: i64,
        write_cols: &[&str],
    ) -> DataFileMeta {
        let mut file = make_evo_file(name, 10, row_count, max_seq, Some(first_row_id));
        file.write_cols = Some(write_cols.iter().map(|col| (*col).to_string()).collect());
        file
    }

    fn data_evolution_test_table(table_path: &str, schema: TableSchema) -> Table {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let schema = schema.copy_with_options(HashMap::from([(
            "data-evolution.enabled".to_string(),
            "true".to_string(),
        )]));
        Table::new(
            file_io,
            Identifier::new("test_db", "de_table"),
            table_path.to_string(),
            schema,
            None,
        )
    }

    fn two_column_schema(id: i64, left: &str, right: &str) -> TableSchema {
        TableSchema::new(
            id,
            &PaimonSchema::builder()
                .column(left, DataType::Int(IntType::new()))
                .column(right, DataType::Int(IntType::new()))
                .build()
                .unwrap(),
        )
    }

    async fn write_schema_file(table: &Table, schema: &TableSchema) {
        let path = table.schema_manager().schema_path(schema.id());
        let dir = path.rsplit_once('/').map(|(dir, _)| dir).unwrap();
        table.file_io().mkdirs(dir).await.unwrap();
        let json = serde_json::to_vec(schema).unwrap();
        table
            .file_io()
            .new_output(&path)
            .unwrap()
            .write(Bytes::from(json))
            .await
            .unwrap();
    }

    fn file_names_from_files(files: &[DataFileMeta]) -> Vec<&str> {
        files.iter().map(|file| file.file_name.as_str()).collect()
    }

    #[test]
    fn test_merge_manifest_entries_keeps_in_place_upgraded_file() {
        // Reproduces a single-run compaction "upgrade": the SAME file name is
        // deleted at level 0 and re-added at a higher level (Paimon promotes a
        // lone sorted run in place instead of rewriting it). Netting ADD/DELETE
        // by file name alone (ignoring `level`) wrongly drops the upgraded file.
        // Matches Java `FileEntry.Identifier`, which includes `level`.
        use super::merge_manifest_entries;
        use crate::spec::ManifestEntry;

        let entry = |kind: FileKind, name: &str, level: i32| -> ManifestEntry {
            let mut file = make_evo_file(name, 1, 1, 1, None);
            file.level = level;
            ManifestEntry::new(kind, Vec::new(), 0, 1, file, 2)
        };

        let entries = vec![
            entry(FileKind::Add, "f.parquet", 0), // original level-0 write
            entry(FileKind::Delete, "f.parquet", 0), // compaction removes the L0 version
            entry(FileKind::Add, "f.parquet", 5), // same file upgraded to level 5
            entry(FileKind::Add, "g.parquet", 0), // unrelated fresh file
        ];

        let mut live: Vec<(String, i32)> = merge_manifest_entries(entries)
            .into_iter()
            .map(|e| (e.file().file_name.clone(), e.file().level))
            .collect();
        live.sort();
        assert_eq!(
            live,
            vec![("f.parquet".to_string(), 5), ("g.parquet".to_string(), 0)],
            "upgraded file (f@L5) must survive; only f@L0 is cancelled by the DELETE"
        );
    }

    fn file_names(groups: &[Vec<DataFileMeta>]) -> Vec<Vec<&str>> {
        groups
            .iter()
            .map(|g| g.iter().map(|f| f.file_name.as_str()).collect())
            .collect()
    }

    fn int_stats_row(value: Option<i32>) -> Vec<u8> {
        let mut builder = BinaryRowBuilder::new(1);
        match value {
            Some(value) => builder.write_int(0, value),
            None => builder.set_null_at(0),
        }
        builder.build_serialized()
    }

    fn partition_string_field() -> Vec<DataField> {
        vec![DataField::new(
            0,
            "dt".to_string(),
            DataType::VarChar(VarCharType::default()),
        )]
    }

    fn int_field() -> Vec<DataField> {
        vec![DataField::new(
            0,
            "id".to_string(),
            DataType::Int(IntType::new()),
        )]
    }

    fn test_data_file_meta(
        min_values: Vec<u8>,
        max_values: Vec<u8>,
        null_counts: Vec<Option<i64>>,
        row_count: i64,
    ) -> DataFileMeta {
        test_data_file_meta_with_schema(
            min_values,
            max_values,
            null_counts,
            row_count,
            0, // default schema_id
        )
    }

    fn test_data_file_meta_with_schema(
        min_values: Vec<u8>,
        max_values: Vec<u8>,
        null_counts: Vec<Option<i64>>,
        row_count: i64,
        schema_id: i64,
    ) -> DataFileMeta {
        DataFileMeta {
            file_name: "test.parquet".into(),
            file_size: 128,
            row_count,
            min_key: Vec::new(),
            max_key: Vec::new(),
            key_stats: BinaryTableStats::new(Vec::new(), Vec::new(), Vec::new()),
            value_stats: BinaryTableStats::new(min_values, max_values, null_counts),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id,
            level: 1,
            extra_files: Vec::new(),
            creation_time: Some(Utc::now()),
            delete_row_count: None,
            embedded_index: None,
            first_row_id: None,
            write_cols: None,
            external_path: None,
            file_source: None,
            value_stats_cols: None,
        }
    }

    fn scan_trace_test_table(table_path: &str) -> Table {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let schema = PaimonSchema::builder()
            .column("id", DataType::Int(IntType::new()))
            .build()
            .unwrap();
        let table_schema = TableSchema::new(0, &schema);
        Table::new(
            file_io,
            Identifier::new("test_db", "scan_trace"),
            table_path.to_string(),
            table_schema,
            None,
        )
    }

    fn scan_trace_small_split_table(table_path: &str) -> Table {
        scan_trace_test_table(table_path).copy_with_options(HashMap::from([
            ("source.split.target-size".to_string(), "1b".to_string()),
            ("source.split.open-file-cost".to_string(), "1b".to_string()),
        ]))
    }

    async fn setup_scan_trace_dirs(table: &Table) {
        table
            .file_io()
            .mkdirs(&format!("{}/snapshot/", table.location()))
            .await
            .unwrap();
        table
            .file_io()
            .mkdirs(&format!("{}/manifest/", table.location()))
            .await
            .unwrap();
    }

    fn stats_trace_file(name: &str, min_id: i32, max_id: i32) -> DataFileMeta {
        let mut file = test_data_file_meta(
            int_stats_row(Some(min_id)),
            int_stats_row(Some(max_id)),
            vec![Some(0)],
            2,
        );
        file.file_name = name.to_string();
        file
    }

    fn limit_test_table() -> Table {
        let file_io = FileIOBuilder::new("file").build().unwrap();
        let schema = PaimonSchema::builder().build().unwrap();
        let table_schema = TableSchema::new(0, &schema);
        Table::new(
            file_io,
            Identifier::new("test_db", "test_table"),
            "/tmp/test-table".to_string(),
            table_schema,
            None,
        )
    }

    fn limit_test_split(file_name: &str, row_count: i64) -> DataSplit {
        let mut file = test_data_file_meta(Vec::new(), Vec::new(), Vec::new(), row_count);
        file.file_name = file_name.to_string();

        DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(format!("file:/tmp/{file_name}"))
            .with_total_buckets(1)
            .with_data_files(vec![file])
            .build()
            .unwrap()
    }

    fn limit_test_split_with_unknown_merged_row_count(
        file_name: &str,
        row_count: i64,
    ) -> DataSplit {
        let mut file = test_data_file_meta(Vec::new(), Vec::new(), Vec::new(), row_count);
        file.file_name = file_name.to_string();

        DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(format!("file:/tmp/{file_name}"))
            .with_total_buckets(1)
            .with_data_files(vec![file])
            .with_data_deletion_files(vec![Some(DeletionFile::new(
                format!("file:/tmp/{file_name}.dv"),
                0,
                0,
                None,
            ))])
            .build()
            .unwrap()
    }

    fn split_file_names(splits: &[DataSplit]) -> Vec<&str> {
        splits
            .iter()
            .map(|split| split.data_files()[0].file_name.as_str())
            .collect()
    }

    #[test]
    fn test_apply_limit_pushdown_zero_returns_empty() {
        let table = limit_test_table();
        let scan = TableScan::new(&table, None, vec![], None, Some(0), None);
        let splits = vec![
            limit_test_split("a.parquet", 2),
            limit_test_split("b.parquet", 3),
        ];

        let pruned = scan.apply_limit_pushdown(splits);

        assert!(pruned.is_empty());
    }

    /// Java semantics: unknown-count splits are skipped — they cannot prove
    /// progress toward the limit — and pruning commits once the counted
    /// splits alone cover the limit.
    #[test]
    fn test_apply_limit_pushdown_skips_unknown_merged_row_count() {
        let table = limit_test_table();
        let scan = TableScan::new(&table, None, vec![], None, Some(3), None);
        let splits = vec![
            limit_test_split("a.parquet", 2),
            limit_test_split_with_unknown_merged_row_count("b.parquet", 4),
            limit_test_split("c.parquet", 3),
        ];

        let pruned = scan.apply_limit_pushdown(splits);

        assert_eq!(split_file_names(&pruned), vec!["a.parquet", "c.parquet"]);
    }

    /// When counted splits never reach the limit, the original split list is
    /// returned unchanged (mirrors Java `applyPushDownLimit`).
    #[test]
    fn test_apply_limit_pushdown_returns_all_when_limit_not_reached() {
        let table = limit_test_table();
        let scan = TableScan::new(&table, None, vec![], None, Some(100), None);
        let splits = vec![
            limit_test_split("a.parquet", 2),
            limit_test_split_with_unknown_merged_row_count("b.parquet", 4),
            limit_test_split("c.parquet", 3),
        ];

        let pruned = scan.apply_limit_pushdown(splits);

        assert_eq!(
            split_file_names(&pruned),
            vec!["a.parquet", "b.parquet", "c.parquet"]
        );
    }

    /// A non-raw-convertible split (merge-needed PK split) has an unknown
    /// merged row count: its physical rows overcount merged versions, so it
    /// must not satisfy the limit on its own.
    #[test]
    fn test_apply_limit_pushdown_treats_merge_splits_as_unknown() {
        let table = limit_test_table();
        let scan = TableScan::new(&table, None, vec![], None, Some(3), None);

        let mut merge_file = test_data_file_meta(Vec::new(), Vec::new(), Vec::new(), 10);
        merge_file.file_name = "merge.parquet".to_string();
        let merge_split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("file:/tmp/merge.parquet".to_string())
            .with_total_buckets(1)
            .with_data_files(vec![merge_file])
            .with_raw_convertible(false)
            .build()
            .unwrap();
        assert_eq!(merge_split.merged_row_count(), None);

        let splits = vec![merge_split, limit_test_split("a.parquet", 3)];
        let pruned = scan.apply_limit_pushdown(splits);

        // The merge split is skipped; only the counted split commits pruning.
        assert_eq!(split_file_names(&pruned), vec!["a.parquet"]);
    }

    #[test]
    fn test_incremental_limit_accumulator_stops_after_known_count_reaches_limit() {
        let mut accumulator = LimitPushdownAccumulator::new(3);

        assert!(!accumulator.push(limit_test_split("a.parquet", 2)));
        assert!(
            !accumulator.push(limit_test_split_with_unknown_merged_row_count(
                "b.parquet",
                4
            ))
        );
        assert!(accumulator.push(limit_test_split("c.parquet", 3)));

        let result = accumulator.finish();
        assert!(result.limit_early_stopped);
        assert_eq!(result.split_candidates_built, 3);
        assert_eq!(
            split_file_names(&result.splits),
            vec!["a.parquet", "c.parquet"]
        );
    }

    #[test]
    fn test_incremental_limit_accumulator_returns_fallback_when_limit_not_reached() {
        let mut accumulator = LimitPushdownAccumulator::new(100);

        assert!(!accumulator.push(limit_test_split("a.parquet", 2)));
        assert!(
            !accumulator.push(limit_test_split_with_unknown_merged_row_count(
                "b.parquet",
                4
            ))
        );
        assert!(!accumulator.push(limit_test_split("c.parquet", 3)));

        let result = accumulator.finish();
        assert!(!result.limit_early_stopped);
        assert_eq!(result.split_candidates_built, 3);
        assert_eq!(
            split_file_names(&result.splits),
            vec!["a.parquet", "b.parquet", "c.parquet"]
        );
    }

    #[test]
    fn test_first_row_skips_level_zero_by_default() {
        assert!(should_skip_level_zero_for_scan(
            false,
            true,
            false,
            Ok(crate::spec::MergeEngine::FirstRow),
        ));
    }

    #[test]
    fn test_scan_all_files_disables_first_row_level_zero_skip() {
        assert!(!should_skip_level_zero_for_scan(
            true,
            true,
            false,
            Ok(crate::spec::MergeEngine::FirstRow),
        ));
    }

    #[test]
    fn test_partition_filter_decode_failure_fails_open() {
        let fields = partition_string_field();
        let predicate = PredicateBuilder::new(&fields)
            .equal("dt", Datum::String("2024-01-01".into()))
            .unwrap();

        // Range predicate to force Predicate variant (fail-open path)
        let filter = PartitionFilter::Predicate(predicate);
        assert!(filter.matches_entry(&[0xFF, 0x00]).unwrap());
    }

    #[test]
    fn test_partition_filter_eval_error_fails_fast() {
        let mut builder = BinaryRowBuilder::new(1);
        builder.write_string(0, "2024-01-01");
        let serialized = builder.build_serialized();

        let predicate = Predicate::Leaf {
            column: "dt".into(),
            index: 0,
            data_type: DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
            op: PredicateOperator::Eq,
            literals: vec![Datum::Int(42)],
        };

        let filter = PartitionFilter::Predicate(predicate);
        let err = filter
            .matches_entry(&serialized)
            .expect_err("eval_row error should propagate");

        assert!(
            matches!(&err, Error::Unsupported { message } if message.contains("extract_datum")),
            "Expected extract_datum unsupported error, got: {err:?}"
        );
    }

    const TEST_SCHEMA_ID: i64 = 0;
    fn test_schema_fields() -> Vec<DataField> {
        int_field()
    }

    #[test]
    fn test_group_by_overlapping_row_id_empty() {
        let result = group_by_overlapping_row_id(vec![]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_group_by_overlapping_row_id_no_row_ids() {
        let files = vec![
            make_evo_file("a", 10, 100, 1, None),
            make_evo_file("b", 10, 100, 2, None),
        ];
        let groups = group_by_overlapping_row_id(files);
        assert_eq!(file_names(&groups), vec![vec!["b"], vec!["a"]]);
    }

    #[test]
    fn test_group_by_overlapping_row_id_same_range() {
        let files = vec![
            make_evo_file("a", 10, 100, 2, Some(0)),
            make_evo_file("b", 10, 100, 1, Some(0)),
        ];
        let groups = group_by_overlapping_row_id(files);
        assert_eq!(groups.len(), 1);
        assert_eq!(file_names(&groups), vec![vec!["a", "b"]]);
    }

    #[tokio::test]
    async fn test_data_evolution_prunes_files_without_projected_columns() {
        let table =
            data_evolution_test_table("memory:/de_prune_cols", two_column_schema(0, "id", "name"));
        let read_field_ids = HashSet::from([1]);
        let files = vec![
            make_evo_file_with_cols("id.parquet", 10, 1, 0, &["id"]),
            make_evo_file_with_cols("name.parquet", 10, 2, 0, &["name"]),
        ];
        let mut field_ids_cache = HashMap::new();

        let pruned = prune_data_evolution_group_by_read_fields(
            files,
            &read_field_ids,
            false,
            table.schema().id(),
            table.schema().fields(),
            table.schema_manager(),
            &mut field_ids_cache,
        )
        .await
        .unwrap();

        assert_eq!(file_names_from_files(&pruned), vec!["name.parquet"]);
    }

    #[tokio::test]
    async fn test_data_evolution_pruning_keeps_dv_anchor() {
        let table =
            data_evolution_test_table("memory:/de_prune_dv", two_column_schema(0, "id", "name"));
        let read_field_ids = HashSet::from([1]);
        let files = vec![
            make_evo_file_with_cols("new-name.parquet", 10, 5, 0, &["name"]),
            make_evo_file_with_cols("old-id.parquet", 10, 1, 0, &["id"]),
        ];
        let mut field_ids_cache = HashMap::new();

        let pruned = prune_data_evolution_group_by_read_fields(
            files,
            &read_field_ids,
            true,
            table.schema().id(),
            table.schema().fields(),
            table.schema_manager(),
            &mut field_ids_cache,
        )
        .await
        .unwrap();

        assert_eq!(
            file_names_from_files(&pruned),
            vec!["new-name.parquet", "old-id.parquet"]
        );
    }

    #[tokio::test]
    async fn test_data_evolution_pruning_keeps_row_count_representative() {
        let table = data_evolution_test_table(
            "memory:/de_prune_representative",
            two_column_schema(0, "id", "name"),
        );
        let read_field_ids = HashSet::from([2]);
        let files = vec![
            make_evo_file_with_cols("new-name.parquet", 10, 5, 0, &["name"]),
            make_evo_file_with_cols("old-id.parquet", 10, 1, 0, &["id"]),
        ];
        let mut field_ids_cache = HashMap::new();

        let pruned = prune_data_evolution_group_by_read_fields(
            files,
            &read_field_ids,
            false,
            table.schema().id(),
            table.schema().fields(),
            table.schema_manager(),
            &mut field_ids_cache,
        )
        .await
        .unwrap();

        assert_eq!(file_names_from_files(&pruned), vec!["old-id.parquet"]);
    }

    #[tokio::test]
    async fn test_data_evolution_pruning_matches_renamed_columns_by_field_id() {
        let schema_v0 = two_column_schema(0, "id", "old_name");
        let schema_v1 = two_column_schema(1, "id", "new_name");
        let table = data_evolution_test_table("memory:/de_prune_rename", schema_v1);
        write_schema_file(&table, &schema_v0).await;

        let read_field_ids = HashSet::from([1]);
        let mut file = make_evo_file_with_cols("renamed.parquet", 10, 1, 0, &["old_name"]);
        file.schema_id = 0;
        let pruned = prune_data_evolution_group_by_read_fields(
            vec![
                make_evo_file_with_cols("id.parquet", 10, 2, 0, &["id"]),
                file,
            ],
            &read_field_ids,
            false,
            table.schema().id(),
            table.schema().fields(),
            table.schema_manager(),
            &mut HashMap::new(),
        )
        .await
        .unwrap();

        assert_eq!(file_names_from_files(&pruned), vec!["renamed.parquet"]);
    }

    #[tokio::test]
    async fn test_data_evolution_pruning_keeps_normal_representative_for_vector_file() {
        let table =
            data_evolution_test_table("memory:/de_prune_vector", two_column_schema(0, "id", "emb"));
        let read_field_ids = HashSet::from([1]);
        let files = vec![
            make_evo_file_with_cols("data.parquet", 10, 1, 0, &["id"]),
            make_evo_file_with_cols("emb.vector.parquet", 10, 2, 0, &["emb"]),
        ];
        let mut field_ids_cache = HashMap::new();

        let pruned = prune_data_evolution_group_by_read_fields(
            files,
            &read_field_ids,
            false,
            table.schema().id(),
            table.schema().fields(),
            table.schema_manager(),
            &mut field_ids_cache,
        )
        .await
        .unwrap();

        assert_eq!(
            file_names_from_files(&pruned),
            vec!["emb.vector.parquet", "data.parquet"]
        );
    }

    #[tokio::test]
    async fn test_data_evolution_pruning_rejects_group_without_normal_representative() {
        let table = data_evolution_test_table(
            "memory:/de_prune_no_normal",
            two_column_schema(0, "id", "emb"),
        );
        let read_field_ids = HashSet::from([1]);
        let files = vec![
            make_evo_file_with_cols("emb-1.vector.parquet", 10, 1, 0, &["emb"]),
            make_evo_file_with_cols("emb-2.vector.parquet", 10, 2, 0, &["emb"]),
        ];
        let mut field_ids_cache = HashMap::new();

        let err = prune_data_evolution_group_by_read_fields(
            files,
            &read_field_ids,
            false,
            table.schema().id(),
            table.schema().fields(),
            table.schema_manager(),
            &mut field_ids_cache,
        )
        .await
        .unwrap_err();

        match err {
            Error::DataInvalid { message, .. } => {
                assert!(message.contains("requires at least one normal data file"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_group_by_overlapping_row_id_overlapping_ranges() {
        let files = vec![
            make_evo_file("a", 10, 100, 1, Some(0)),
            make_evo_file("b", 10, 100, 2, Some(50)),
        ];
        let groups = group_by_overlapping_row_id(files);
        assert_eq!(groups.len(), 1);
        assert_eq!(file_names(&groups), vec![vec!["a", "b"]]);
    }

    #[test]
    fn test_group_by_overlapping_row_id_non_overlapping() {
        let files = vec![
            make_evo_file("a", 10, 100, 1, Some(0)),
            make_evo_file("b", 10, 100, 2, Some(100)),
        ];
        let groups = group_by_overlapping_row_id(files);
        assert_eq!(groups.len(), 2);
        assert_eq!(file_names(&groups), vec![vec!["a"], vec!["b"]]);
    }

    #[test]
    fn test_group_by_overlapping_row_id_mixed() {
        let files = vec![
            make_evo_file("a", 10, 100, 1, Some(0)),
            make_evo_file("b", 10, 100, 2, Some(0)),
            make_evo_file("c", 10, 100, 3, None),
            make_evo_file("d", 10, 100, 4, Some(200)),
        ];
        let groups = group_by_overlapping_row_id(files);
        assert_eq!(
            file_names(&groups),
            vec![vec!["c"], vec!["b", "a"], vec!["d"]]
        );
    }

    #[test]
    fn test_group_by_overlapping_row_id_sorted_by_seq() {
        let files = vec![
            make_evo_file("a", 10, 100, 1, Some(0)),
            make_evo_file("b", 10, 100, 3, Some(0)),
            make_evo_file("c", 10, 100, 2, Some(0)),
        ];
        let groups = group_by_overlapping_row_id(files);
        assert_eq!(groups.len(), 1);
        assert_eq!(file_names(&groups), vec![vec!["b", "c", "a"]]);
    }

    #[test]
    fn test_data_file_matches_eq_prunes_out_of_range() {
        let fields = int_field();
        let file = test_data_file_meta(
            int_stats_row(Some(10)),
            int_stats_row(Some(20)),
            vec![Some(0)],
            5,
        );
        let predicate = PredicateBuilder::new(&fields)
            .equal("id", Datum::Int(30))
            .unwrap();

        assert!(!data_file_matches_predicates(
            &file,
            &[predicate],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[tokio::test]
    async fn test_plan_with_trace_records_between_data_stats_pruning() {
        let table_path = "memory:/test_plan_with_trace_records_between_data_stats_pruning";
        let table = scan_trace_test_table(table_path);
        setup_scan_trace_dirs(&table).await;

        TableCommit::new(table.clone(), "scan-trace-test".to_string())
            .commit(vec![CommitMessage::new(
                BinaryRowBuilder::new(0).build_serialized(),
                0,
                vec![
                    stats_trace_file("stats-1.parquet", 1, 2),
                    stats_trace_file("stats-2.parquet", 10, 20),
                    stats_trace_file("stats-3.parquet", 100, 101),
                ],
            )])
            .await
            .unwrap();

        let fields = int_field();
        let pb = PredicateBuilder::new(&fields);
        let between = Predicate::and(vec![
            pb.greater_or_equal("id", Datum::Int(10)).unwrap(),
            pb.less_or_equal("id", Datum::Int(20)).unwrap(),
        ]);
        let mut reader = table.new_read_builder();
        reader.with_filter(between);
        let (_plan, trace) = reader.new_scan().plan_with_trace().await.unwrap();

        assert_eq!(
            trace.final_files, 1,
            "BETWEEN should keep only the overlapping stats range: {trace:?}"
        );
        assert!(
            trace.manifest_entries_pruned_by_data_stats >= 2,
            "BETWEEN should prune files outside the min/max range: {trace:?}"
        );
    }

    fn pk_stats_gate_table(table_path: &str) -> Table {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let schema = PaimonSchema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .primary_key(["id"])
            .option("bucket", "1")
            .build()
            .unwrap();
        Table::new(
            file_io,
            Identifier::new("test_db", "pk_stats_gate"),
            table_path.to_string(),
            TableSchema::new(0, &schema),
            None,
        )
    }

    fn two_int_stats_row(id: Option<i32>, value: Option<i32>) -> Vec<u8> {
        let mut builder = BinaryRowBuilder::new(2);
        match id {
            Some(id) => builder.write_int(0, id),
            None => builder.set_null_at(0),
        }
        match value {
            Some(value) => builder.write_int(1, value),
            None => builder.set_null_at(1),
        }
        builder.build_serialized()
    }

    fn pk_stats_file(name: &str, id_range: (i32, i32), value_range: (i32, i32)) -> DataFileMeta {
        let mut file = test_data_file_meta(
            two_int_stats_row(Some(id_range.0), Some(value_range.0)),
            two_int_stats_row(Some(id_range.1), Some(value_range.1)),
            vec![Some(0), Some(0)],
            2,
        );
        file.file_name = name.to_string();
        file
    }

    /// Merge reads combine versions of a key across files, so scan planning
    /// must not prune a PK table's files by NON-key conjuncts: dropping the
    /// file that holds the newest version resurrects an older version from a
    /// surviving file — an error no post-merge residual can repair. Key
    /// conjuncts stay safe (every version of a key shares the key columns)
    /// and must still prune.
    #[tokio::test]
    async fn test_pk_table_stats_pruning_ignores_non_key_conjuncts() {
        let table_path = "memory:/test_pk_stats_gate";
        let table = pk_stats_gate_table(table_path);
        setup_scan_trace_dirs(&table).await;

        // Both files cover key id=1; the newer version's value (50) falls
        // outside the value predicate while the older one (150) matches.
        TableCommit::new(table.clone(), "pk-gate-test".to_string())
            .commit(vec![CommitMessage::new(
                BinaryRowBuilder::new(0).build_serialized(),
                0,
                vec![
                    pk_stats_file("old-version.parquet", (1, 5), (100, 200)),
                    pk_stats_file("new-version.parquet", (1, 5), (10, 60)),
                ],
            )])
            .await
            .unwrap();

        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "value".to_string(), DataType::Int(IntType::new())),
        ];
        let pb = PredicateBuilder::new(&fields);

        // Non-key conjunct: must NOT prune any file of a PK table.
        let value_filter = pb.greater_than("value", Datum::Int(90)).unwrap();
        let mut reader = table.new_read_builder();
        reader.with_filter(value_filter);
        let (plan, trace) = reader.new_scan().plan_with_trace().await.unwrap();
        assert_eq!(
            trace.manifest_entries_pruned_by_data_stats, 0,
            "non-key conjuncts must not file-prune a PK table: {trace:?}"
        );
        let planned_files: usize = plan.splits().iter().map(|s| s.data_files().len()).sum();
        assert_eq!(
            planned_files, 2,
            "both versions must reach the merge reader"
        );

        // Key conjunct: still prunes (id=9 outside both files' key range).
        let key_filter = pb.equal("id", Datum::Int(9)).unwrap();
        let mut reader = table.new_read_builder();
        reader.with_filter(key_filter);
        let (_plan, trace) = reader.new_scan().plan_with_trace().await.unwrap();
        assert!(
            trace.manifest_entries_pruned_by_data_stats >= 2,
            "key conjuncts must still prune PK-table files: {trace:?}"
        );
    }

    /// `merge-engine=first-row` PK tables read raw (no merge on the read
    /// path: planned with `skip_level_zero`, read via `DataFileReader`), so
    /// pruning a file by a non-key conjunct cannot resurrect anything — it
    /// drops exactly the rows the raw path's exact residual filter would
    /// drop. The key-only gate must exempt first-row and keep full-predicate
    /// stats pruning, matching the split-generation path.
    #[tokio::test]
    async fn test_first_row_table_stats_pruning_keeps_non_key_conjuncts() {
        let table_path = "memory:/test_first_row_stats_gate";
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let schema = PaimonSchema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .primary_key(["id"])
            .option("bucket", "1")
            .option("merge-engine", "first-row")
            .build()
            .unwrap();
        let table = Table::new(
            file_io,
            Identifier::new("test_db", "first_row_stats_gate"),
            table_path.to_string(),
            TableSchema::new(0, &schema),
            None,
        );
        setup_scan_trace_dirs(&table).await;

        // Compacted (level 1) files: first-row planning skips level 0, so the
        // fixture files must sit above it to be planned at all. Distinct key
        // ranges; only file A's value range can match `value > 90`.
        TableCommit::new(table.clone(), "first-row-gate-test".to_string())
            .commit(vec![CommitMessage::new(
                BinaryRowBuilder::new(0).build_serialized(),
                0,
                vec![
                    pk_stats_file("file-a.parquet", (1, 5), (100, 200)),
                    pk_stats_file("file-b.parquet", (6, 9), (10, 60)),
                ],
            )])
            .await
            .unwrap();

        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "value".to_string(), DataType::Int(IntType::new())),
        ];
        let pb = PredicateBuilder::new(&fields);

        // Non-key conjunct: first-row reads raw, so full-predicate pruning
        // stays enabled — file-b (value stats [10, 60]) must be pruned.
        let value_filter = pb.greater_than("value", Datum::Int(90)).unwrap();
        let mut reader = table.new_read_builder();
        reader.with_filter(value_filter);
        let (plan, trace) = reader.new_scan().plan_with_trace().await.unwrap();
        assert!(
            trace.manifest_entries_pruned_by_data_stats >= 1,
            "first-row tables must keep full-predicate stats pruning: {trace:?}"
        );
        let planned_files: usize = plan.splits().iter().map(|s| s.data_files().len()).sum();
        assert_eq!(
            planned_files, 1,
            "only the value-matching file should be planned on first-row"
        );
    }

    #[tokio::test]
    async fn test_plan_with_trace_records_limit_early_stop_during_split_construction() {
        let table_path =
            "memory:/test_plan_with_trace_records_limit_early_stop_during_split_construction";
        let table = scan_trace_small_split_table(table_path);
        setup_scan_trace_dirs(&table).await;

        TableCommit::new(table.clone(), "scan-trace-limit-test".to_string())
            .commit(vec![CommitMessage::new(
                BinaryRowBuilder::new(0).build_serialized(),
                0,
                vec![
                    stats_trace_file("limit-1.parquet", 1, 1),
                    stats_trace_file("limit-2.parquet", 2, 2),
                    stats_trace_file("limit-3.parquet", 3, 3),
                ],
            )])
            .await
            .unwrap();

        let (_full_plan, full_trace) = table
            .new_read_builder()
            .new_scan()
            .plan_with_trace()
            .await
            .unwrap();
        let mut limited_reader = table.new_read_builder();
        limited_reader.with_limit(2);
        let (_limited_plan, limited_trace) =
            limited_reader.new_scan().plan_with_trace().await.unwrap();

        assert_eq!(
            full_trace.final_splits, 3,
            "fixture should build three splits: {full_trace:?}"
        );
        assert!(!full_trace.limit_early_stopped);
        assert_eq!(full_trace.split_candidates_built, full_trace.final_splits);
        assert!(limited_trace.limit_early_stopped);
        assert!(
            limited_trace.split_candidates_built < full_trace.split_candidates_built,
            "limited trace should show construction-time stop: full={full_trace:?}, limited={limited_trace:?}"
        );
        assert_eq!(limited_trace.final_splits, 1);
    }

    #[tokio::test]
    async fn test_plan_with_trace_zero_limit_records_no_split_candidates() {
        let table_path = "memory:/test_plan_with_trace_zero_limit_records_no_split_candidates";
        let table = scan_trace_small_split_table(table_path);
        setup_scan_trace_dirs(&table).await;

        TableCommit::new(table.clone(), "scan-trace-zero-limit-test".to_string())
            .commit(vec![CommitMessage::new(
                BinaryRowBuilder::new(0).build_serialized(),
                0,
                vec![
                    stats_trace_file("zero-1.parquet", 1, 1),
                    stats_trace_file("zero-2.parquet", 2, 2),
                ],
            )])
            .await
            .unwrap();

        let mut reader = table.new_read_builder();
        reader.with_limit(0);
        let (plan, trace) = reader.new_scan().plan_with_trace().await.unwrap();

        assert!(plan.splits().is_empty());
        assert!(trace.limit_early_stopped);
        assert_eq!(trace.split_candidates_built, 0);
        assert_eq!(trace.final_splits, 0);
    }

    #[test]
    fn test_data_file_matches_in_prunes_when_all_literals_out_of_range() {
        let fields = int_field();
        let file = test_data_file_meta(
            int_stats_row(Some(10)),
            int_stats_row(Some(20)),
            vec![Some(0)],
            5,
        );
        let predicate = PredicateBuilder::new(&fields)
            .is_in("id", vec![Datum::Int(1), Datum::Int(30)])
            .unwrap();

        assert!(!data_file_matches_predicates(
            &file,
            &[predicate],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[test]
    fn test_data_file_matches_in_keeps_when_any_literal_in_range() {
        let fields = int_field();
        let file = test_data_file_meta(
            int_stats_row(Some(10)),
            int_stats_row(Some(20)),
            vec![Some(0)],
            5,
        );
        let predicate = PredicateBuilder::new(&fields)
            .is_in("id", vec![Datum::Int(1), Datum::Int(15), Datum::Int(30)])
            .unwrap();

        assert!(data_file_matches_predicates(
            &file,
            &[predicate],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[test]
    fn test_data_file_matches_in_prunes_all_null_file() {
        let fields = int_field();
        let file = test_data_file_meta(int_stats_row(None), int_stats_row(None), vec![Some(5)], 5);
        let predicate = PredicateBuilder::new(&fields)
            .is_in("id", vec![Datum::Int(10)])
            .unwrap();

        assert!(!data_file_matches_predicates(
            &file,
            &[predicate],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[test]
    fn test_data_file_matches_in_with_corrupt_stats_fails_open() {
        let fields = int_field();
        let file = test_data_file_meta(Vec::new(), Vec::new(), vec![Some(0)], 5);
        let predicate = PredicateBuilder::new(&fields)
            .is_in("id", vec![Datum::Int(30)])
            .unwrap();

        assert!(data_file_matches_predicates(
            &file,
            &[predicate],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[test]
    fn test_data_file_matches_in_with_inverted_stats_fails_open() {
        let fields = int_field();
        let file = test_data_file_meta(
            int_stats_row(Some(20)),
            int_stats_row(Some(10)),
            vec![Some(0)],
            5,
        );
        let predicate = PredicateBuilder::new(&fields)
            .is_in("id", vec![Datum::Int(15)])
            .unwrap();

        assert!(data_file_matches_predicates(
            &file,
            &[predicate],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[test]
    fn test_data_file_matches_not_in_fails_open() {
        let fields = int_field();
        let file = test_data_file_meta(
            int_stats_row(Some(10)),
            int_stats_row(Some(20)),
            vec![Some(0)],
            5,
        );
        let predicate = PredicateBuilder::new(&fields)
            .is_not_in("id", vec![Datum::Int(10), Datum::Int(20)])
            .unwrap();

        assert!(data_file_matches_predicates(
            &file,
            &[predicate],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[test]
    fn test_data_file_matches_is_null_prunes_when_null_count_is_zero() {
        let fields = int_field();
        let file = test_data_file_meta(
            int_stats_row(Some(10)),
            int_stats_row(Some(20)),
            vec![Some(0)],
            5,
        );
        let predicate = PredicateBuilder::new(&fields).is_null("id").unwrap();

        assert!(!data_file_matches_predicates(
            &file,
            &[predicate],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[test]
    fn test_data_file_matches_is_not_null_prunes_all_null_file() {
        let fields = int_field();
        let file = test_data_file_meta(int_stats_row(None), int_stats_row(None), vec![Some(5)], 5);
        let predicate = PredicateBuilder::new(&fields).is_not_null("id").unwrap();

        assert!(!data_file_matches_predicates(
            &file,
            &[predicate],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[test]
    fn test_data_file_matches_or_prunes_when_no_child_matches() {
        let fields = int_field();
        let file = test_data_file_meta(
            int_stats_row(Some(10)),
            int_stats_row(Some(20)),
            vec![Some(0)],
            5,
        );
        let pb = PredicateBuilder::new(&fields);
        let predicate = Predicate::or(vec![
            pb.less_than("id", Datum::Int(5)).unwrap(),
            pb.greater_than("id", Datum::Int(25)).unwrap(),
        ]);

        assert!(!data_file_matches_predicates(
            &file,
            &[predicate],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[test]
    fn test_data_file_matches_or_keeps_when_any_child_matches() {
        let fields = int_field();
        let file = test_data_file_meta(
            int_stats_row(Some(10)),
            int_stats_row(Some(20)),
            vec![Some(0)],
            5,
        );
        let pb = PredicateBuilder::new(&fields);
        let predicate = Predicate::or(vec![
            pb.less_than("id", Datum::Int(15)).unwrap(),
            pb.greater_than("id", Datum::Int(25)).unwrap(),
        ]);

        assert!(data_file_matches_predicates(
            &file,
            &[predicate],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[test]
    fn test_data_file_matches_not_fails_open() {
        let fields = int_field();
        let file = test_data_file_meta(
            int_stats_row(Some(10)),
            int_stats_row(Some(20)),
            vec![Some(0)],
            5,
        );
        let predicate = Predicate::negate(
            PredicateBuilder::new(&fields)
                .less_than("id", Datum::Int(5))
                .unwrap(),
        );

        assert!(data_file_matches_predicates(
            &file,
            &[predicate],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[test]
    fn test_data_evolution_group_matches_or_prunes_when_no_child_matches() {
        let fields = int_field();
        let file = test_data_file_meta(
            int_stats_row(Some(10)),
            int_stats_row(Some(20)),
            vec![Some(0)],
            5,
        );
        let pb = PredicateBuilder::new(&fields);
        let predicate = Predicate::or(vec![
            pb.less_than("id", Datum::Int(5)).unwrap(),
            pb.greater_than("id", Datum::Int(25)).unwrap(),
        ]);

        assert!(!data_evolution_group_matches_predicates(
            &[file],
            &[predicate],
            &fields,
        ));
    }

    #[test]
    fn test_data_evolution_group_matches_or_keeps_when_any_child_matches() {
        let fields = int_field();
        let file = test_data_file_meta(
            int_stats_row(Some(10)),
            int_stats_row(Some(20)),
            vec![Some(0)],
            5,
        );
        let pb = PredicateBuilder::new(&fields);
        let predicate = Predicate::or(vec![
            pb.less_than("id", Datum::Int(15)).unwrap(),
            pb.greater_than("id", Datum::Int(25)).unwrap(),
        ]);

        assert!(data_evolution_group_matches_predicates(
            &[file],
            &[predicate],
            &fields,
        ));
    }

    #[test]
    fn test_data_file_matches_corrupt_stats_fails_open() {
        let fields = int_field();
        let file = test_data_file_meta(Vec::new(), Vec::new(), vec![Some(0)], 5);
        let predicate = PredicateBuilder::new(&fields)
            .equal("id", Datum::Int(30))
            .unwrap();

        assert!(data_file_matches_predicates(
            &file,
            &[predicate],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[test]
    fn test_data_file_matches_schema_mismatch_fails_open() {
        let fields = int_field();
        let file = test_data_file_meta_with_schema(
            int_stats_row(Some(10)),
            int_stats_row(Some(20)),
            vec![Some(0)],
            5,
            5,
        );
        let predicate = PredicateBuilder::new(&fields)
            .equal("id", Datum::Int(30))
            .unwrap();

        assert!(data_file_matches_predicates(
            &file,
            &[predicate],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[test]
    fn test_data_file_matches_always_false_prunes_despite_schema_mismatch() {
        let file = test_data_file_meta_with_schema(
            int_stats_row(Some(10)),
            int_stats_row(Some(20)),
            vec![Some(0)],
            5,
            99,
        );

        assert!(!data_file_matches_predicates(
            &file,
            &[Predicate::AlwaysFalse],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[test]
    fn test_data_file_matches_always_true_keeps_file_despite_schema_mismatch() {
        let file = test_data_file_meta_with_schema(
            int_stats_row(Some(10)),
            int_stats_row(Some(20)),
            vec![Some(0)],
            5,
            99,
        );

        assert!(data_file_matches_predicates(
            &file,
            &[Predicate::AlwaysTrue],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
        ));
    }

    #[test]
    fn test_build_deletion_files_map_preserves_cardinality() {
        let entries = vec![IndexManifestEntry {
            version: 1,
            kind: FileKind::Add,
            partition: vec![1, 2, 3],
            bucket: 7,
            index_file: IndexFileMeta {
                index_type: "DELETION_VECTORS".into(),
                file_name: "index-file".into(),
                file_size: 128,
                row_count: 1,
                deletion_vectors_ranges: Some(indexmap::IndexMap::from([(
                    "data-file.parquet".into(),
                    DeletionVectorMeta {
                        offset: 11,
                        length: 22,
                        cardinality: Some(33),
                    },
                )])),
                global_index_meta: None,
            },
        }];

        let map = super::build_deletion_files_map(&entries, "file:/tmp/table");

        let by_bucket = map
            .get(&super::PartitionBucket::new(vec![1, 2, 3], 7))
            .expect("partition bucket should exist");
        let deletion_file = by_bucket
            .get("data-file.parquet")
            .expect("deletion file should exist");

        assert_eq!(
            deletion_file,
            &DeletionFile::new("file:/tmp/table/index/index-file".into(), 11, 22, Some(33))
        );
    }

    // ======================== Bucket predicate filtering ========================

    fn bucket_key_fields() -> Vec<DataField> {
        vec![DataField::new(
            0,
            "id".to_string(),
            DataType::Int(IntType::new()),
        )]
    }

    #[test]
    fn test_extract_predicate_for_keys_eq() {
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "name".to_string(),
                DataType::VarChar(VarCharType::default()),
            ),
        ];
        let pb = PredicateBuilder::new(&fields);
        let filter = Predicate::and(vec![
            pb.equal("id", Datum::Int(42)).unwrap(),
            pb.equal("name", Datum::String("alice".into())).unwrap(),
        ]);

        let keys = vec!["id".to_string()];
        let extracted = extract_predicate_for_keys(&filter, &fields, &keys);
        assert!(extracted.is_some());
        match extracted.unwrap() {
            Predicate::Leaf {
                column, index, op, ..
            } => {
                assert_eq!(column, "id");
                assert_eq!(index, 0); // remapped to key index
                assert_eq!(op, PredicateOperator::Eq);
            }
            other => panic!("expected Leaf, got {other:?}"),
        }
    }

    #[test]
    fn test_extract_predicate_for_keys_no_match() {
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "name".to_string(),
                DataType::VarChar(VarCharType::default()),
            ),
        ];
        let pb = PredicateBuilder::new(&fields);
        let filter = pb.equal("name", Datum::String("alice".into())).unwrap();

        let keys = vec!["id".to_string()];
        let extracted = extract_predicate_for_keys(&filter, &fields, &keys);
        assert!(extracted.is_none());
    }

    #[test]
    fn test_compute_target_buckets_single_eq() {
        let fields = bucket_key_fields();
        // Build a bucket predicate (already projected to bucket key space, index=0)
        let pred = Predicate::Leaf {
            column: "id".into(),
            index: 0,
            data_type: DataType::Int(IntType::new()),
            op: PredicateOperator::Eq,
            literals: vec![Datum::Int(42)],
        };

        let buckets = compute_target_buckets(&pred, &fields, BucketFunctionType::Default, 4);
        assert!(buckets.is_some());
        let buckets = buckets.unwrap();
        assert_eq!(buckets.len(), 1);
        // The bucket should be deterministic
        let bucket = *buckets.iter().next().unwrap();
        assert!((0..4).contains(&bucket));
    }

    #[test]
    fn test_compute_target_buckets_in_predicate() {
        let fields = bucket_key_fields();
        let pred = Predicate::Leaf {
            column: "id".into(),
            index: 0,
            data_type: DataType::Int(IntType::new()),
            op: PredicateOperator::In,
            literals: vec![Datum::Int(1), Datum::Int(2), Datum::Int(3)],
        };

        let buckets = compute_target_buckets(&pred, &fields, BucketFunctionType::Default, 4);
        assert!(buckets.is_some());
        let buckets = buckets.unwrap();
        // Should have at most 3 buckets (could be fewer if some hash to the same bucket)
        assert!(!buckets.is_empty());
        assert!(buckets.len() <= 3);
        for &b in &buckets {
            assert!((0..4).contains(&b));
        }
    }

    #[test]
    fn test_compute_target_buckets_range_returns_none() {
        let fields = bucket_key_fields();
        let pred = Predicate::Leaf {
            column: "id".into(),
            index: 0,
            data_type: DataType::Int(IntType::new()),
            op: PredicateOperator::Gt,
            literals: vec![Datum::Int(10)],
        };

        let buckets = compute_target_buckets(&pred, &fields, BucketFunctionType::Default, 4);
        assert!(
            buckets.is_none(),
            "Range predicates cannot determine target buckets"
        );
    }

    #[test]
    fn test_compute_target_buckets_composite_key() {
        let fields = vec![
            DataField::new(0, "a".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "b".to_string(), DataType::Int(IntType::new())),
        ];
        let pred = Predicate::And(vec![
            Predicate::Leaf {
                column: "a".into(),
                index: 0,
                data_type: DataType::Int(IntType::new()),
                op: PredicateOperator::Eq,
                literals: vec![Datum::Int(1)],
            },
            Predicate::Leaf {
                column: "b".into(),
                index: 1,
                data_type: DataType::Int(IntType::new()),
                op: PredicateOperator::Eq,
                literals: vec![Datum::Int(2)],
            },
        ]);

        let buckets = compute_target_buckets(&pred, &fields, BucketFunctionType::Default, 8);
        assert!(buckets.is_some());
        let buckets = buckets.unwrap();
        assert_eq!(buckets.len(), 1);
        let bucket = *buckets.iter().next().unwrap();
        assert!((0..8).contains(&bucket));
    }

    #[test]
    fn test_compute_target_buckets_partial_key_returns_none() {
        // Only one of two bucket key fields has an eq predicate
        let fields = vec![
            DataField::new(0, "a".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "b".to_string(), DataType::Int(IntType::new())),
        ];
        let pred = Predicate::Leaf {
            column: "a".into(),
            index: 0,
            data_type: DataType::Int(IntType::new()),
            op: PredicateOperator::Eq,
            literals: vec![Datum::Int(1)],
        };

        let buckets = compute_target_buckets(&pred, &fields, BucketFunctionType::Default, 8);
        assert!(
            buckets.is_none(),
            "Partial bucket key should not determine target buckets"
        );
    }

    #[test]
    fn test_compute_target_buckets_string_key() {
        let fields = vec![DataField::new(
            0,
            "name".to_string(),
            DataType::VarChar(VarCharType::default()),
        )];
        let pred = Predicate::Leaf {
            column: "name".into(),
            index: 0,
            data_type: DataType::VarChar(VarCharType::default()),
            op: PredicateOperator::Eq,
            literals: vec![Datum::String("alice".into())],
        };

        let buckets = compute_target_buckets(&pred, &fields, BucketFunctionType::Default, 4);
        assert!(buckets.is_some());
        let buckets = buckets.unwrap();
        assert_eq!(buckets.len(), 1);
        let bucket = *buckets.iter().next().unwrap();
        assert!((0..4).contains(&bucket));
    }

    #[test]
    fn test_compute_target_buckets_mod_function() {
        let fields = bucket_key_fields();
        let pred = Predicate::Leaf {
            column: "id".into(),
            index: 0,
            data_type: DataType::Int(IntType::new()),
            op: PredicateOperator::Eq,
            literals: vec![Datum::Int(-3)],
        };

        let buckets = compute_target_buckets(&pred, &fields, BucketFunctionType::Mod, 5);
        assert_eq!(buckets, Some(HashSet::from([2])));
    }

    #[test]
    fn test_compute_target_buckets_hive_function() {
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "name".to_string(),
                DataType::VarChar(VarCharType::default()),
            ),
        ];
        let pred = Predicate::And(vec![
            Predicate::Leaf {
                column: "id".into(),
                index: 0,
                data_type: DataType::Int(IntType::new()),
                op: PredicateOperator::Eq,
                literals: vec![Datum::Int(7)],
            },
            Predicate::Leaf {
                column: "name".into(),
                index: 1,
                data_type: DataType::VarChar(VarCharType::default()),
                op: PredicateOperator::Eq,
                literals: vec![Datum::String("hello".into())],
            },
        ]);

        let buckets = compute_target_buckets(&pred, &fields, BucketFunctionType::Hive, 8);
        assert_eq!(buckets, Some(HashSet::from([3])));
    }

    #[test]
    fn test_compute_target_buckets_is_null() {
        let fields = bucket_key_fields();
        let pred = Predicate::Leaf {
            column: "id".into(),
            index: 0,
            data_type: DataType::Int(IntType::new()),
            op: PredicateOperator::IsNull,
            literals: vec![],
        };

        let buckets = compute_target_buckets(&pred, &fields, BucketFunctionType::Default, 4);
        assert!(buckets.is_some(), "IsNull should determine a target bucket");
        let buckets = buckets.unwrap();
        assert_eq!(buckets.len(), 1);
        let bucket = *buckets.iter().next().unwrap();
        assert!((0..4).contains(&bucket));

        // Verify it matches the expected bucket from a null BinaryRow
        let mut builder = BinaryRowBuilder::new(1);
        builder.set_null_at(0);
        let expected = (builder.build().hash_code() % 4).abs();
        assert_eq!(bucket, expected);
    }

    #[test]
    fn test_compute_target_buckets_composite_key_with_null() {
        let fields = vec![
            DataField::new(0, "a".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "b".to_string(), DataType::Int(IntType::new())),
        ];
        // a = 1 AND b IS NULL
        let pred = Predicate::And(vec![
            Predicate::Leaf {
                column: "a".into(),
                index: 0,
                data_type: DataType::Int(IntType::new()),
                op: PredicateOperator::Eq,
                literals: vec![Datum::Int(1)],
            },
            Predicate::Leaf {
                column: "b".into(),
                index: 1,
                data_type: DataType::Int(IntType::new()),
                op: PredicateOperator::IsNull,
                literals: vec![],
            },
        ]);

        let buckets = compute_target_buckets(&pred, &fields, BucketFunctionType::Default, 8);
        assert!(
            buckets.is_some(),
            "Composite key with IsNull should determine a target bucket"
        );
        let buckets = buckets.unwrap();
        assert_eq!(buckets.len(), 1);
        let bucket = *buckets.iter().next().unwrap();
        assert!((0..8).contains(&bucket));
    }

    #[tokio::test]
    async fn test_plan_fails_closed_when_query_auth_enabled() {
        // Every scan-planning path must fail closed, including `with_scan_all_files`
        // (read-facing system tables like `files` use it to expose metadata).
        let table = crate::table::query_auth_table();
        let rb = table.new_read_builder();
        for scan in [rb.new_scan(), rb.new_scan().with_scan_all_files()] {
            let err = scan.plan().await.unwrap_err();
            assert!(
                matches!(err, crate::Error::Unsupported { ref message } if message.contains("query-auth.enabled")),
                "scan planning must fail closed (scan_all_files or not)"
            );
        }
    }

    #[tokio::test]
    async fn test_dynamic_option_cannot_disable_query_auth_at_plan() {
        // Copying the table with the option off must not weaken a stored `true`.
        let table =
            crate::table::query_auth_table().copy_with_options(std::collections::HashMap::from([
                ("query-auth.enabled".to_string(), "false".to_string()),
            ]));
        let err = table
            .new_read_builder()
            .new_scan()
            .plan()
            .await
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::Unsupported { ref message } if message.contains("query-auth.enabled")),
            "a dynamic override must not disable query-auth"
        );
    }
}
