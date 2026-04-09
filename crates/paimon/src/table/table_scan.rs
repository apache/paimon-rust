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
use super::stats_filter::{
    data_evolution_group_matches_predicates, data_file_matches_predicates,
    data_file_matches_predicates_for_table, group_by_overlapping_row_id, FileStatsRows,
    ResolvedStatsSchema,
};
use super::Table;
use crate::io::FileIO;
use crate::predicate_stats::data_leaf_may_match;
use crate::spec::{
    eval_row, BinaryRow, CoreOptions, DataField, DataFileMeta, FileKind, IndexManifest,
    ManifestEntry, ManifestFileMeta, PartitionComputer, Predicate, Snapshot, TimeTravelSelector,
};
use crate::table::bin_pack::split_for_batch;
use crate::table::source::{
    any_range_overlaps_file, intersect_ranges_with_file, merge_row_ranges, DataSplit,
    DataSplitBuilder, DeletionFile, PartitionBucket, Plan, RowRange,
};
use crate::table::SnapshotManager;
use crate::table::TagManager;
use crate::Error;
use futures::{StreamExt, TryStreamExt};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Path segment for manifest directory under table.
const MANIFEST_DIR: &str = "manifest";
/// Path segment for index directory under table.
const INDEX_DIR: &str = "index";

/// Reads a manifest list file (Avro) and returns manifest file metas.
async fn read_manifest_list(
    file_io: &FileIO,
    table_path: &str,
    list_name: &str,
) -> crate::Result<Vec<crate::spec::ManifestFileMeta>> {
    let path = format!(
        "{}/{}/{}",
        table_path.trim_end_matches('/'),
        MANIFEST_DIR,
        list_name
    );
    let input = file_io.new_input(&path)?;
    if !input.exists().await? {
        return Ok(Vec::new());
    }
    let bytes = input.read().await?;
    crate::spec::from_avro_bytes::<crate::spec::ManifestFileMeta>(&bytes)
}

/// Check whether a manifest file *may* contain entries matching the partition predicate,
/// using the manifest-level partition stats (min/max over all entries in the manifest).
///
/// The `predicate` must already be projected to partition-field indices.
fn manifest_file_matches_partition_predicate(
    meta: &ManifestFileMeta,
    predicate: &Predicate,
    partition_fields: &[DataField],
) -> bool {
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

    manifest_partition_predicate_may_match(predicate, &file_stats, partition_fields)
}

fn manifest_partition_predicate_may_match(
    predicate: &Predicate,
    stats: &FileStatsRows,
    partition_fields: &[DataField],
) -> bool {
    match predicate {
        Predicate::AlwaysTrue => true,
        Predicate::AlwaysFalse => false,
        Predicate::And(children) => children
            .iter()
            .all(|child| manifest_partition_predicate_may_match(child, stats, partition_fields)),
        Predicate::Or(_) | Predicate::Not(_) => true,
        Predicate::Leaf {
            index,
            data_type,
            op,
            literals,
            ..
        } => {
            let stats_data_type = match partition_fields.get(*index) {
                Some(f) => f.data_type(),
                None => return true,
            };
            data_leaf_may_match(*index, stats_data_type, data_type, *op, literals, stats)
        }
    }
}

/// Reads all manifest entries for a snapshot (base + delta manifest lists, then each manifest file).
/// Applies filters during concurrent manifest reading to reduce entries early:
/// - Manifest-file-level partition stats pruning (skip entire manifest files)
/// - DV level-0 filtering per entry
/// - Partition predicate filtering per entry
/// - Data-level stats pruning per entry (current schema only, cross-schema fail-open)
#[allow(clippy::too_many_arguments)]
async fn read_all_manifest_entries(
    file_io: &FileIO,
    table_path: &str,
    snapshot: &Snapshot,
    deletion_vectors_enabled: bool,
    has_primary_keys: bool,
    partition_predicate: Option<&Predicate>,
    partition_fields: &[DataField],
    data_predicates: &[Predicate],
    current_schema_id: i64,
    schema_fields: &[DataField],
    bucket_predicate: Option<&Predicate>,
    bucket_key_fields: &[DataField],
) -> crate::Result<Vec<ManifestEntry>> {
    let mut manifest_files =
        read_manifest_list(file_io, table_path, snapshot.base_manifest_list()).await?;
    let delta = read_manifest_list(file_io, table_path, snapshot.delta_manifest_list()).await?;
    manifest_files.extend(delta);

    // Manifest-file-level partition stats pruning: skip entire manifest files
    // whose partition range doesn't overlap the partition predicate.
    if let Some(pred) = partition_predicate {
        if !partition_fields.is_empty() {
            manifest_files.retain(|meta| {
                manifest_file_matches_partition_predicate(meta, pred, partition_fields)
            });
        }
    }

    let manifest_path_prefix = format!("{}/{}", table_path.trim_end_matches('/'), MANIFEST_DIR);
    let all_entries: Vec<ManifestEntry> = futures::stream::iter(manifest_files)
        .map(|meta| {
            let path = format!("{}/{}", manifest_path_prefix, meta.file_name());
            async move {
                let entries = crate::spec::Manifest::read(file_io, &path).await?;
                // Per-task bucket cache (few distinct total_buckets values per manifest).
                let mut bucket_cache: HashMap<i32, Option<HashSet<i32>>> = HashMap::new();
                let filtered: Vec<ManifestEntry> = entries
                    .into_iter()
                    .filter(|entry| {
                        if deletion_vectors_enabled && has_primary_keys && entry.file().level == 0 {
                            return false;
                        }
                        if has_primary_keys && entry.bucket() < 0 {
                            return false;
                        }
                        if let Some(pred) = bucket_predicate {
                            let total = entry.total_buckets();
                            let targets = bucket_cache.entry(total).or_insert_with(|| {
                                compute_target_buckets(pred, bucket_key_fields, total)
                            });
                            if let Some(targets) = targets {
                                if !targets.contains(&entry.bucket()) {
                                    return false;
                                }
                            }
                        }
                        if let Some(pred) = partition_predicate {
                            match partition_matches_predicate(entry.partition(), pred) {
                                Ok(false) => return false,
                                Ok(true) => {}
                                Err(_) => {}
                            }
                        }
                        if !data_predicates.is_empty()
                            && !data_file_matches_predicates(
                                entry.file(),
                                data_predicates,
                                current_schema_id,
                                schema_fields,
                            )
                        {
                            return false;
                        }
                        true
                    })
                    .collect();
                Ok::<_, crate::Error>(filtered)
            }
        })
        .buffered(64)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .flatten()
        .collect();
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
    let mut map: HashMap<PartitionBucket, HashMap<String, DeletionFile>> = HashMap::new();
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

/// Merges add/delete manifest entries following pypaimon's `adds - deletes` behavior.
///
/// The identifier must be rich enough to match Paimon's file identity, otherwise a delete
/// for one file version can incorrectly remove another with the same file name.
fn merge_manifest_entries(entries: Vec<ManifestEntry>) -> Vec<ManifestEntry> {
    let mut deleted_entry_keys = HashSet::new();
    let mut added_entries = Vec::new();

    for entry in entries {
        match entry.kind() {
            FileKind::Add => added_entries.push(entry),
            FileKind::Delete => {
                deleted_entry_keys.insert(entry.identifier());
            }
        }
    }

    added_entries
        .into_iter()
        .filter(|entry| !deleted_entry_keys.contains(&entry.identifier()))
        .collect()
}

/// Evaluate a partition predicate against serialized manifest partition bytes.
///
/// - `BinaryRow::from_serialized_bytes` failure → fail-open (`Ok(true)`)
/// - `eval_row` failure → fail-fast (`Err(_)`)
fn partition_matches_predicate(
    serialized_partition: &[u8],
    predicate: &Predicate,
) -> crate::Result<bool> {
    match BinaryRow::from_serialized_bytes(serialized_partition) {
        Ok(row) => eval_row(predicate, &row),
        Err(_) => Ok(true),
    }
}

/// TableScan for full table scan (no incremental, no predicate).
///
/// Reference: [pypaimon.read.table_scan.TableScan](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/read/table_scan.py)
#[derive(Debug, Clone)]
pub struct TableScan<'a> {
    table: &'a Table,
    partition_predicate: Option<Predicate>,
    data_predicates: Vec<Predicate>,
    bucket_predicate: Option<Predicate>,
    /// Optional limit on the number of rows to return.
    /// When set, the scan will try to return only enough splits to satisfy the limit.
    limit: Option<usize>,
    row_ranges: Option<Vec<RowRange>>,
}

impl<'a> TableScan<'a> {
    pub fn new(
        table: &'a Table,
        partition_predicate: Option<Predicate>,
        data_predicates: Vec<Predicate>,
        bucket_predicate: Option<Predicate>,
        limit: Option<usize>,
        row_ranges: Option<Vec<RowRange>>,
    ) -> Self {
        Self {
            table,
            partition_predicate,
            data_predicates,
            bucket_predicate,
            limit,
            row_ranges,
        }
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

    /// Plan the full scan: resolve snapshot (via options or latest), then read manifests and build DataSplits.
    ///
    /// Time travel is resolved from table options:
    /// - only one of `scan.tag-name`, `scan.snapshot-id`, `scan.timestamp-millis` may be set
    /// - `scan.tag-name` → read the snapshot pinned by that tag
    /// - `scan.snapshot-id` → read that specific snapshot
    /// - `scan.timestamp-millis` → find the latest snapshot <= that timestamp
    /// - otherwise → read the latest snapshot
    ///
    /// Reference: [TimeTravelUtil.tryTravelToSnapshot](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/table/source/snapshot/TimeTravelUtil.java)
    pub async fn plan(&self) -> crate::Result<Plan> {
        let snapshot = match self.resolve_snapshot().await? {
            Some(snapshot) => snapshot,
            None => return Ok(Plan::new(Vec::new())),
        };
        self.plan_snapshot(snapshot).await
    }

    async fn resolve_snapshot(&self) -> crate::Result<Option<Snapshot>> {
        let file_io = self.table.file_io();
        let table_path = self.table.location();
        let snapshot_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let core_options = CoreOptions::new(self.table.schema().options());

        match core_options.try_time_travel_selector()? {
            Some(TimeTravelSelector::TagName(tag_name)) => {
                let tag_manager = TagManager::new(file_io.clone(), table_path.to_string());
                match tag_manager.get(tag_name).await? {
                    Some(s) => Ok(Some(s)),
                    None => Err(Error::DataInvalid {
                        message: format!("Tag '{tag_name}' doesn't exist."),
                        source: None,
                    }),
                }
            }
            Some(TimeTravelSelector::SnapshotId(id)) => {
                snapshot_manager.get_snapshot(id).await.map(Some)
            }
            Some(TimeTravelSelector::TimestampMillis(ts)) => {
                match snapshot_manager.earlier_or_equal_time_mills(ts).await? {
                    Some(s) => Ok(Some(s)),
                    None => Err(Error::DataInvalid {
                        message: format!("No snapshot found with timestamp <= {ts}"),
                        source: None,
                    }),
                }
            }
            None => snapshot_manager.get_latest_snapshot().await,
        }
    }

    /// Apply a limit-pushdown hint to the generated splits.
    ///
    /// Iterates through splits and accumulates `merged_row_count()` until the
    /// limit hint is reached. Returns only the splits likely needed to satisfy
    /// that hint.
    ///
    /// This does not guarantee an exact final row count. If a split's
    /// `merged_row_count()` is `None` (for example because of unknown deletion
    /// cardinality), that split is kept even though its contribution to the
    /// limit is unknown. Planning may still stop early later if the
    /// accumulated known `merged_row_count()` reaches the limit, and the
    /// caller or query engine must enforce the final LIMIT.
    fn apply_limit_pushdown(&self, splits: Vec<DataSplit>) -> Vec<DataSplit> {
        let limit = match self.limit {
            Some(l) => l,
            None => return splits,
        };

        if splits.is_empty() {
            return splits;
        }

        let mut limited_splits = Vec::new();
        let mut scanned_row_count: i64 = 0;

        for split in splits {
            match split.merged_row_count() {
                Some(merged_count) => {
                    limited_splits.push(split);
                    scanned_row_count += merged_count;
                    if scanned_row_count >= limit as i64 {
                        // We likely have enough rows for the limit hint.
                        return limited_splits;
                    }
                }
                None => {
                    // Can't compute merged row count, so keep this split and
                    // rely on the caller or query engine to enforce the final
                    // LIMIT.
                    limited_splits.push(split);
                }
            }
        }

        limited_splits
    }

    async fn plan_snapshot(&self, snapshot: Snapshot) -> crate::Result<Plan> {
        let file_io = self.table.file_io();
        let table_path = self.table.location();
        let core_options = CoreOptions::new(self.table.schema().options());
        let deletion_vectors_enabled = core_options.deletion_vectors_enabled();
        let data_evolution_enabled = core_options.data_evolution_enabled();
        let target_split_size = core_options.source_split_target_size();
        let open_file_cost = core_options.source_split_open_file_cost();

        // Resolve partition fields for manifest-file-level stats pruning.
        let partition_keys = self.table.schema().partition_keys();
        let partition_fields: Vec<DataField> = self
            .table
            .schema()
            .partition_keys()
            .iter()
            .filter_map(|key| {
                self.table
                    .schema()
                    .fields()
                    .iter()
                    .find(|f| f.name() == key)
                    .cloned()
            })
            .collect();

        // Data-evolution tables must not prune data files independently.
        let pushdown_data_predicates = if data_evolution_enabled {
            &[][..]
        } else {
            self.data_predicates.as_slice()
        };

        let has_primary_keys = !self.table.schema().primary_keys().is_empty();

        // Compute bucket predicate and key fields for per-entry bucket pruning.
        // Only supported for the default bucket function (MurmurHash3-based).
        let bucket_key_fields: Vec<DataField> =
            if self.bucket_predicate.is_none() || !core_options.is_default_bucket_function() {
                Vec::new()
            } else {
                let bucket_keys = core_options.bucket_key().unwrap_or_else(|| {
                    if has_primary_keys {
                        self.table
                            .schema()
                            .primary_keys()
                            .iter()
                            .map(|s| s.to_string())
                            .collect()
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

        let entries = read_all_manifest_entries(
            file_io,
            table_path,
            &snapshot,
            deletion_vectors_enabled,
            has_primary_keys,
            self.partition_predicate.as_ref(),
            &partition_fields,
            pushdown_data_predicates,
            self.table.schema().id(),
            self.table.schema().fields(),
            self.bucket_predicate.as_ref(),
            &bucket_key_fields,
        )
        .await?;
        let entries = merge_manifest_entries(entries);
        if entries.is_empty() {
            return Ok(Plan::new(Vec::new()));
        }

        // For non-data-evolution tables, cross-schema files were kept (fail-open)
        // by the pushdown. Apply the full schema-aware filter for those files.
        let entries = if self.data_predicates.is_empty() || data_evolution_enabled {
            entries
        } else {
            let current_schema_id = self.table.schema().id();
            let has_cross_schema = entries
                .iter()
                .any(|e| e.file().schema_id != current_schema_id);
            if !has_cross_schema {
                entries
            } else {
                let mut kept = Vec::with_capacity(entries.len());
                let mut schema_cache: HashMap<i64, Option<Arc<ResolvedStatsSchema>>> =
                    HashMap::new();
                for entry in entries {
                    if entry.file().schema_id == current_schema_id
                        || data_file_matches_predicates_for_table(
                            self.table,
                            entry.file(),
                            &self.data_predicates,
                            &mut schema_cache,
                        )
                        .await
                    {
                        kept.push(entry);
                    }
                }
                kept
            }
        };
        if entries.is_empty() {
            return Ok(Plan::new(Vec::new()));
        }

        // Group by (partition, bucket). Key = (partition_bytes, bucket).
        let mut groups: HashMap<(Vec<u8>, i32), Vec<ManifestEntry>> = HashMap::new();
        for e in entries {
            let key = (e.partition().to_vec(), e.bucket());
            groups.entry(key).or_default().push(e);
        }

        let snapshot_id = snapshot.id();
        let base_path = table_path.trim_end_matches('/');
        let mut splits = Vec::new();

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
                } else if data_evolution_enabled
                    && core_options.global_index_enabled()
                    && !self.data_predicates.is_empty()
                {
                    super::global_index_scanner::evaluate_global_index(
                        file_io,
                        base_path,
                        &index_entries,
                        &self.data_predicates,
                        self.table.schema().fields(),
                    )
                    .await?
                } else {
                    None
                };

                (Some(dv_map), row_ranges)
            } else {
                (None, self.row_ranges.clone())
            };

        for ((partition, bucket), group_entries) in groups {
            let partition_row = BinaryRow::from_serialized_bytes(&partition)?;

            let total_buckets = group_entries
                .first()
                .map(|e| e.total_buckets())
                .ok_or_else(|| Error::UnexpectedError {
                    message: format!("Manifest entry group for bucket {bucket} is empty, cannot determine total_buckets"),
                    source: None,
                })?;
            let data_files: Vec<_> = group_entries
                .into_iter()
                .map(|e| {
                    let ManifestEntry { file, .. } = e;
                    file
                })
                .collect();

            let bucket_path = if let Some(ref computer) = partition_computer {
                let partition_path = computer.generate_partition_path(&partition_row)?;
                format!("{base_path}/{partition_path}bucket-{bucket}")
            } else {
                format!("{base_path}/bucket-{bucket}")
            };

            // Original `partition` Vec consumed by PartitionBucket for DV map lookup.
            let per_bucket_deletion_map = deletion_files_map
                .as_ref()
                .and_then(|map| map.get(&PartitionBucket::new(partition, bucket)));

            // Data-evolution tables merge overlapping row-id groups column-wise during read.
            // Keep that split boundary intact and only bin-pack single-file groups.
            // Apply group-level predicate filtering after grouping by row_id range.
            let file_groups_with_raw: Vec<(Vec<DataFileMeta>, bool)> = if data_evolution_enabled {
                let row_id_groups = group_by_overlapping_row_id(data_files);

                // Filter groups by merged stats before splitting.
                let row_id_groups: Vec<Vec<DataFileMeta>> = if self.data_predicates.is_empty() {
                    row_id_groups
                } else {
                    row_id_groups
                        .into_iter()
                        .filter(|group| {
                            data_evolution_group_matches_predicates(
                                group,
                                &self.data_predicates,
                                self.table.schema().fields(),
                            )
                        })
                        .collect()
                };

                // Filter groups by row ID ranges.
                let row_id_groups = if let Some(ref ranges) = effective_row_ranges {
                    row_id_groups
                        .into_iter()
                        .filter(|group| group.iter().any(|f| any_range_overlaps_file(ranges, f)))
                        .collect()
                } else {
                    row_id_groups
                };

                let (singles, multis): (Vec<_>, Vec<_>) = row_id_groups
                    .into_iter()
                    .partition(|group| group.len() == 1);

                let mut result = Vec::new();
                for group in multis {
                    result.push((group, false));
                }

                let single_files: Vec<DataFileMeta> = singles.into_iter().flatten().collect();
                for file_group in split_for_batch(single_files, target_split_size, open_file_cost) {
                    result.push((file_group, true));
                }

                result
            } else {
                split_for_batch(data_files, target_split_size, open_file_cost)
                    .into_iter()
                    .map(|group| (group, true))
                    .collect()
            };

            for (file_group, raw_convertible) in file_groups_with_raw {
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
                splits.push(builder.build()?);
            }
        }

        // With data predicates or row_ranges, merged_row_count() reflects pre-filter
        // row counts, so stopping early could return fewer rows than the limit.
        let splits = if self.data_predicates.is_empty() && effective_row_ranges.is_none() {
            self.apply_limit_pushdown(splits)
        } else {
            splits
        };

        Ok(Plan::new(splits))
    }
}

#[cfg(test)]
mod tests {
    use super::partition_matches_predicate;
    use crate::spec::{
        stats::BinaryTableStats, ArrayType, BinaryRowBuilder, DataField, DataFileMeta, DataType,
        Datum, DeletionVectorMeta, FileKind, IndexFileMeta, IndexManifestEntry, IntType, Predicate,
        PredicateBuilder, PredicateOperator, VarCharType,
    };
    use crate::table::bucket_filter::{compute_target_buckets, extract_predicate_for_keys};
    use crate::table::source::DeletionFile;
    use crate::table::stats_filter::{data_file_matches_predicates, group_by_overlapping_row_id};
    use crate::Error;
    use chrono::{DateTime, Utc};

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

    #[test]
    fn test_partition_matches_predicate_decode_failure_fails_open() {
        let predicate = PredicateBuilder::new(&partition_string_field())
            .equal("dt", Datum::String("2024-01-01".into()))
            .unwrap();

        assert!(partition_matches_predicate(&[0xFF, 0x00], &predicate).unwrap());
    }

    #[test]
    fn test_partition_matches_predicate_eval_error_fails_fast() {
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

        let err = partition_matches_predicate(&serialized, &predicate)
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
    fn test_data_file_matches_unsupported_predicate_fails_open() {
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

        assert!(data_file_matches_predicates(
            &file,
            &[predicate],
            TEST_SCHEMA_ID,
            &test_schema_fields(),
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

        let buckets = compute_target_buckets(&pred, &fields, 4);
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

        let buckets = compute_target_buckets(&pred, &fields, 4);
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

        let buckets = compute_target_buckets(&pred, &fields, 4);
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

        let buckets = compute_target_buckets(&pred, &fields, 8);
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

        let buckets = compute_target_buckets(&pred, &fields, 8);
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

        let buckets = compute_target_buckets(&pred, &fields, 4);
        assert!(buckets.is_some());
        let buckets = buckets.unwrap();
        assert_eq!(buckets.len(), 1);
        let bucket = *buckets.iter().next().unwrap();
        assert!((0..4).contains(&bucket));
    }
}
