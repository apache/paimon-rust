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

use super::Table;
use crate::arrow::schema_evolution::create_index_mapping;
use crate::io::FileIO;
use crate::spec::{
    eval_row, extract_datum, field_idx_to_partition_idx, BinaryRow, CoreOptions, DataField,
    DataFileMeta, DataType, Datum, FileKind, IndexManifest, ManifestEntry, ManifestFileMeta,
    PartitionComputer, Predicate, PredicateOperator, Snapshot,
};
use crate::table::bin_pack::split_for_batch;
use crate::table::source::{DataSplit, DataSplitBuilder, DeletionFile, PartitionBucket, Plan};
use crate::table::SnapshotManager;
use crate::table::TagManager;
use crate::Error;
use futures::{StreamExt, TryStreamExt};
use std::cmp::Ordering;
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
    let num_fields = partition_fields.len();

    let Some(file_stats) = ({
        let min_values = BinaryRow::from_serialized_bytes(stats.min_values()).ok();
        let max_values = BinaryRow::from_serialized_bytes(stats.max_values()).ok();
        let null_counts = stats.null_counts().clone();

        let stats = FileStatsRows {
            row_count: meta.num_added_files() + meta.num_deleted_files(),
            min_values,
            max_values,
            null_counts,
            stats_col_mapping: None,
        };
        stats.arity_matches(num_fields).then_some(stats)
    }) else {
        return true;
    };

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
                        if deletion_vectors_enabled
                            && has_primary_keys
                            && entry.file().level == 0
                        {
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

/// Groups data files by overlapping `row_id_range` for data evolution.
///
/// Files are sorted by `(first_row_id, -max_sequence_number)`. Files whose row ID ranges
/// overlap are merged into the same group (they contain different columns for the same rows).
/// Files without `first_row_id` become their own group.
///
/// Reference: [DataEvolutionSplitGenerator](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/table/source/splitread/DataEvolutionSplitGenerator.java)
pub(crate) fn group_by_overlapping_row_id(mut files: Vec<DataFileMeta>) -> Vec<Vec<DataFileMeta>> {
    files.sort_by(|a, b| {
        let a_row_id = a.first_row_id.unwrap_or(i64::MIN);
        let b_row_id = b.first_row_id.unwrap_or(i64::MIN);
        a_row_id
            .cmp(&b_row_id)
            .then_with(|| b.max_sequence_number.cmp(&a.max_sequence_number))
    });

    let mut result: Vec<Vec<DataFileMeta>> = Vec::new();
    let mut current_group: Vec<DataFileMeta> = Vec::new();
    // Track the end of the current merged row_id range.
    let mut current_range_end: i64 = i64::MIN;

    for file in files {
        match file.row_id_range() {
            None => {
                // Files without first_row_id become their own group.
                if !current_group.is_empty() {
                    result.push(std::mem::take(&mut current_group));
                    current_range_end = i64::MIN;
                }
                result.push(vec![file]);
            }
            Some((start, end)) => {
                if current_group.is_empty() || start <= current_range_end {
                    // Overlaps with current range — merge into current group.
                    if end > current_range_end {
                        current_range_end = end;
                    }
                    current_group.push(file);
                } else {
                    // No overlap — start a new group.
                    result.push(std::mem::take(&mut current_group));
                    current_range_end = end;
                    current_group.push(file);
                }
            }
        }
    }
    if !current_group.is_empty() {
        result.push(current_group);
    }
    result
}

#[derive(Debug, Clone)]
struct FileStatsRows {
    row_count: i64,
    min_values: Option<BinaryRow>,
    max_values: Option<BinaryRow>,
    null_counts: Vec<i64>,
    /// Maps schema field index → stats index. `None` means identity mapping
    /// (stats cover all schema fields in order). `Some` is used when
    /// `value_stats_cols` is present (dense mode).
    stats_col_mapping: Option<Vec<Option<usize>>>,
}

impl FileStatsRows {
    /// Build file stats from a data file, respecting `value_stats_cols`.
    ///
    /// When `value_stats_cols` is `None`, stats cover all fields in `schema_fields` order.
    /// When `value_stats_cols` is `Some`, stats are in dense mode — only covering those
    /// columns, and the mapping from schema field index to stats index is built by name.
    fn try_from_data_file(file: &DataFileMeta, schema_fields: &[DataField]) -> Option<Self> {
        // Determine which columns the stats cover and build the mapping.
        // Priority: value_stats_cols > write_cols > all schema fields.
        let (expected_fields, stats_col_mapping) = if let Some(cols) = &file.value_stats_cols {
            let mapping: Vec<Option<usize>> = schema_fields
                .iter()
                .map(|field| cols.iter().position(|c| c == field.name()))
                .collect();
            (cols.len(), Some(mapping))
        } else if let Some(cols) = &file.write_cols {
            let mapping: Vec<Option<usize>> = schema_fields
                .iter()
                .map(|field| cols.iter().position(|c| c == field.name()))
                .collect();
            (cols.len(), Some(mapping))
        } else {
            (schema_fields.len(), None)
        };

        let stats = Self {
            row_count: file.row_count,
            min_values: BinaryRow::from_serialized_bytes(file.value_stats.min_values()).ok(),
            max_values: BinaryRow::from_serialized_bytes(file.value_stats.max_values()).ok(),
            null_counts: file.value_stats.null_counts().clone(),
            stats_col_mapping,
        };

        stats.arity_matches(expected_fields).then_some(stats)
    }

    /// Resolve a schema field index to the corresponding stats index.
    fn stats_index(&self, schema_index: usize) -> Option<usize> {
        match &self.stats_col_mapping {
            None => Some(schema_index),
            Some(mapping) => mapping.get(schema_index).copied().flatten(),
        }
    }

    fn null_count(&self, stats_index: usize) -> Option<i64> {
        self.null_counts.get(stats_index).copied()
    }

    /// Check whether the stats rows have the expected number of fields.
    fn arity_matches(&self, expected_fields: usize) -> bool {
        let min_ok = self
            .min_values
            .as_ref()
            .is_none_or(|r| r.arity() as usize == expected_fields);
        let max_ok = self
            .max_values
            .as_ref()
            .is_none_or(|r| r.arity() as usize == expected_fields);
        let null_ok = self.null_counts.is_empty() || self.null_counts.len() == expected_fields;
        min_ok && max_ok && null_ok
    }
}

#[derive(Debug)]
struct ResolvedStatsSchema {
    file_fields: Vec<DataField>,
    field_mapping: Vec<Option<usize>>,
}

fn identity_field_mapping(num_fields: usize) -> Vec<Option<usize>> {
    (0..num_fields).map(Some).collect()
}

fn normalize_field_mapping(mapping: Option<Vec<i32>>, num_fields: usize) -> Vec<Option<usize>> {
    mapping
        .map(|field_mapping| {
            field_mapping
                .into_iter()
                .map(|index| usize::try_from(index).ok())
                .collect()
        })
        .unwrap_or_else(|| identity_field_mapping(num_fields))
}

fn split_partition_and_data_predicates(
    filter: Predicate,
    fields: &[DataField],
    partition_keys: &[String],
) -> (Option<Predicate>, Vec<Predicate>) {
    let mapping = field_idx_to_partition_idx(fields, partition_keys);
    let mut partition_predicates = Vec::new();
    let mut data_predicates = Vec::new();

    for conjunct in filter.split_and() {
        let strict_partition_only = conjunct.references_only_mapped_fields(&mapping);

        if let Some(projected) = conjunct.project_field_index_inclusive(&mapping) {
            partition_predicates.push(projected);
        }

        // Keep any conjunct that is not fully partition-only for data-level
        // stats pruning, even if part of it contributed to partition pruning.
        if !strict_partition_only {
            data_predicates.push(conjunct);
        }
    }

    let partition_predicate = if partition_predicates.is_empty() {
        None
    } else {
        Some(Predicate::and(partition_predicates))
    };

    (partition_predicate, data_predicates)
}

/// Extract a predicate projected onto the given key columns.
///
/// This is a generic utility that works for both partition keys and bucket keys.
/// Returns `None` if no conjuncts reference the given keys.
fn extract_predicate_for_keys(
    filter: &Predicate,
    fields: &[DataField],
    keys: &[String],
) -> Option<Predicate> {
    if keys.is_empty() {
        return None;
    }
    let mapping = field_idx_to_partition_idx(fields, keys);
    let projected: Vec<Predicate> = filter
        .clone()
        .split_and()
        .into_iter()
        .filter_map(|conjunct| conjunct.project_field_index_inclusive(&mapping))
        .collect();
    if projected.is_empty() {
        None
    } else {
        Some(Predicate::and(projected))
    }
}

/// Compute the set of target buckets from a bucket predicate.
///
/// Extracts equal-value literals for each bucket key field from the predicate,
/// builds a BinaryRow, hashes it, and returns the target bucket(s).
///
/// Supports:
/// - `key = value` (single bucket)
/// - `key IN (v1, v2, ...)` (multiple buckets)
/// - AND of the above for composite bucket keys
///
/// Returns `None` if the predicate cannot determine target buckets (fail-open).
fn compute_target_buckets(
    bucket_predicate: &Predicate,
    bucket_key_fields: &[DataField],
    total_buckets: i32,
) -> Option<HashSet<i32>> {
    if total_buckets <= 0 || bucket_key_fields.is_empty() {
        return None;
    }

    // Collect equal-value candidates per bucket key field (by projected index).
    // Each field can have one value (Eq) or multiple values (In).
    let num_keys = bucket_key_fields.len();
    let mut field_candidates: Vec<Option<Vec<&Datum>>> = vec![None; num_keys];

    collect_eq_candidates(bucket_predicate, &mut field_candidates);

    // All bucket key fields must have candidates.
    let candidates: Vec<&Vec<&Datum>> =
        field_candidates.iter().filter_map(|c| c.as_ref()).collect();
    if candidates.len() != num_keys {
        return None;
    }

    // Compute cartesian product of candidates and hash each combination.
    let mut buckets = HashSet::new();
    let mut combo: Vec<usize> = vec![0; num_keys];
    loop {
        let datums: Vec<(&Datum, &DataType)> = (0..num_keys)
            .map(|i| {
                let vals = field_candidates[i].as_ref().unwrap();
                (vals[combo[i]], bucket_key_fields[i].data_type())
            })
            .collect();

        if let Some(bucket) = BinaryRow::compute_bucket_from_datums(&datums, total_buckets) {
            buckets.insert(bucket);
        } else {
            return None;
        }

        // Advance the combination counter (rightmost first).
        let mut carry = true;
        for i in (0..num_keys).rev() {
            if carry {
                combo[i] += 1;
                if combo[i] < field_candidates[i].as_ref().unwrap().len() {
                    carry = false;
                } else {
                    combo[i] = 0;
                }
            }
        }
        if carry {
            break;
        }
    }

    if buckets.is_empty() {
        None
    } else {
        Some(buckets)
    }
}

/// Recursively collect Eq/In literal candidates from a predicate for each bucket key field.
fn collect_eq_candidates<'a>(
    predicate: &'a Predicate,
    field_candidates: &mut Vec<Option<Vec<&'a Datum>>>,
) {
    match predicate {
        Predicate::And(children) => {
            for child in children {
                collect_eq_candidates(child, field_candidates);
            }
        }
        Predicate::Leaf {
            index,
            op,
            literals,
            ..
        } => {
            if *index < field_candidates.len() {
                match op {
                    PredicateOperator::Eq => {
                        if let Some(lit) = literals.first() {
                            field_candidates[*index] = Some(vec![lit]);
                        }
                    }
                    PredicateOperator::In => {
                        if !literals.is_empty() {
                            field_candidates[*index] = Some(literals.iter().collect());
                        }
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }
}

/// Check whether a data file *may* contain rows matching all `predicates`.
///
/// Pruning is evaluated per file and fails open when stats cannot be
/// interpreted safely, including schema mismatches, incompatible stats arity,
/// and missing or corrupted stats. Mixed-schema tables can still prune files
/// written with the current schema; unsupported or inconclusive predicates are
/// conservatively kept.
fn data_file_matches_predicates(
    file: &DataFileMeta,
    predicates: &[Predicate],
    current_schema_id: i64,
    schema_fields: &[DataField],
) -> bool {
    if predicates.is_empty() {
        return true;
    }

    // Evaluate constant predicates before consulting stats.
    if predicates
        .iter()
        .any(|p| matches!(p, Predicate::AlwaysFalse))
    {
        return false;
    }
    if predicates
        .iter()
        .all(|p| matches!(p, Predicate::AlwaysTrue))
    {
        return true;
    }

    if file.schema_id != current_schema_id {
        return true;
    }

    // Fail open if schema evolution or stats layout make index-based access unsafe.
    let Some(stats) = FileStatsRows::try_from_data_file(file, schema_fields) else {
        return true;
    };

    predicates
        .iter()
        .all(|predicate| data_predicate_may_match(predicate, &stats))
}

async fn resolve_stats_schema(
    table: &Table,
    file_schema_id: i64,
    schema_cache: &mut HashMap<i64, Option<Arc<ResolvedStatsSchema>>>,
) -> Option<Arc<ResolvedStatsSchema>> {
    if let Some(cached) = schema_cache.get(&file_schema_id) {
        return cached.clone();
    }

    let table_schema = table.schema();
    let current_fields = table_schema.fields();
    let resolved = if file_schema_id == table_schema.id() {
        Some(Arc::new(ResolvedStatsSchema {
            file_fields: current_fields.to_vec(),
            field_mapping: identity_field_mapping(current_fields.len()),
        }))
    } else {
        let file_schema = table.schema_manager().schema(file_schema_id).await.ok()?;
        let file_fields = file_schema.fields().to_vec();
        Some(Arc::new(ResolvedStatsSchema {
            field_mapping: normalize_field_mapping(
                create_index_mapping(current_fields, &file_fields),
                current_fields.len(),
            ),
            file_fields,
        }))
    };

    schema_cache.insert(file_schema_id, resolved.clone());
    resolved
}

async fn data_file_matches_predicates_for_table(
    table: &Table,
    file: &DataFileMeta,
    predicates: &[Predicate],
    schema_cache: &mut HashMap<i64, Option<Arc<ResolvedStatsSchema>>>,
) -> bool {
    if predicates.is_empty() {
        return true;
    }

    if file.schema_id == table.schema().id() {
        return data_file_matches_predicates(
            file,
            predicates,
            table.schema().id(),
            table.schema().fields(),
        );
    }

    let Some(resolved) = resolve_stats_schema(table, file.schema_id, schema_cache).await else {
        return true;
    };

    let Some(stats) = FileStatsRows::try_from_data_file(file, &resolved.file_fields) else {
        return true;
    };

    predicates.iter().all(|predicate| {
        data_predicate_may_match_with_schema(
            predicate,
            &stats,
            &resolved.field_mapping,
            &resolved.file_fields,
        )
    })
}

fn data_predicate_may_match(predicate: &Predicate, stats: &FileStatsRows) -> bool {
    match predicate {
        Predicate::AlwaysTrue => true,
        Predicate::AlwaysFalse => false,
        Predicate::And(children) => children
            .iter()
            .all(|child| data_predicate_may_match(child, stats)),
        // Keep the first version conservative: only prune simple leaves and conjunctions.
        Predicate::Or(_) | Predicate::Not(_) => true,
        Predicate::Leaf {
            index,
            data_type,
            op,
            literals,
            ..
        } => {
            // Resolve schema field index → stats index via value_stats_cols mapping.
            // If the field is not covered by stats, fail open.
            let Some(stats_idx) = stats.stats_index(*index) else {
                return true;
            };
            data_leaf_may_match(stats_idx, data_type, data_type, *op, literals, stats)
        }
    }
}

fn data_predicate_may_match_with_schema(
    predicate: &Predicate,
    stats: &FileStatsRows,
    field_mapping: &[Option<usize>],
    file_fields: &[DataField],
) -> bool {
    match predicate {
        Predicate::AlwaysTrue => true,
        Predicate::AlwaysFalse => false,
        Predicate::And(children) => children.iter().all(|child| {
            data_predicate_may_match_with_schema(child, stats, field_mapping, file_fields)
        }),
        // Keep the first version conservative: only prune simple leaves and conjunctions.
        Predicate::Or(_) | Predicate::Not(_) => true,
        Predicate::Leaf {
            index,
            data_type,
            op,
            literals,
            ..
        } => match field_mapping.get(*index).copied().flatten() {
            Some(file_index) => {
                let Some(file_field) = file_fields.get(file_index) else {
                    return true;
                };
                // Resolve file schema index → stats index via value_stats_cols mapping.
                let Some(stats_idx) = stats.stats_index(file_index) else {
                    return true;
                };
                data_leaf_may_match(
                    stats_idx,
                    file_field.data_type(),
                    data_type,
                    *op,
                    literals,
                    stats,
                )
            }
            None => missing_field_may_match(*op, stats.row_count),
        },
    }
}

fn data_leaf_may_match(
    index: usize,
    stats_data_type: &DataType,
    predicate_data_type: &DataType,
    op: PredicateOperator,
    literals: &[Datum],
    stats: &FileStatsRows,
) -> bool {
    let row_count = stats.row_count;
    if row_count <= 0 {
        return false;
    }

    let null_count = stats.null_count(index);
    let all_null = null_count.map(|count| count == row_count);

    match op {
        PredicateOperator::IsNull => {
            return null_count.is_none_or(|count| count > 0);
        }
        PredicateOperator::IsNotNull => {
            return all_null != Some(true);
        }
        PredicateOperator::In | PredicateOperator::NotIn => {
            return true;
        }
        PredicateOperator::Eq
        | PredicateOperator::NotEq
        | PredicateOperator::Lt
        | PredicateOperator::LtEq
        | PredicateOperator::Gt
        | PredicateOperator::GtEq => {}
    }

    if all_null == Some(true) {
        return false;
    }

    let literal = match literals.first() {
        Some(literal) => literal,
        None => return true,
    };

    let min_value = match stats
        .min_values
        .as_ref()
        .and_then(|row| extract_stats_datum(row, index, stats_data_type))
        .and_then(|datum| coerce_stats_datum_for_predicate(datum, predicate_data_type))
    {
        Some(value) => value,
        None => return true,
    };
    let max_value = match stats
        .max_values
        .as_ref()
        .and_then(|row| extract_stats_datum(row, index, stats_data_type))
        .and_then(|datum| coerce_stats_datum_for_predicate(datum, predicate_data_type))
    {
        Some(value) => value,
        None => return true,
    };

    match op {
        PredicateOperator::Eq => {
            !matches!(literal.partial_cmp(&min_value), Some(Ordering::Less))
                && !matches!(literal.partial_cmp(&max_value), Some(Ordering::Greater))
        }
        PredicateOperator::NotEq => !(min_value == *literal && max_value == *literal),
        PredicateOperator::Lt => !matches!(
            min_value.partial_cmp(literal),
            Some(Ordering::Greater | Ordering::Equal)
        ),
        PredicateOperator::LtEq => {
            !matches!(min_value.partial_cmp(literal), Some(Ordering::Greater))
        }
        PredicateOperator::Gt => !matches!(
            max_value.partial_cmp(literal),
            Some(Ordering::Less | Ordering::Equal)
        ),
        PredicateOperator::GtEq => !matches!(max_value.partial_cmp(literal), Some(Ordering::Less)),
        PredicateOperator::IsNull
        | PredicateOperator::IsNotNull
        | PredicateOperator::In
        | PredicateOperator::NotIn => true,
    }
}

fn missing_field_may_match(op: PredicateOperator, row_count: i64) -> bool {
    if row_count <= 0 {
        return false;
    }

    matches!(op, PredicateOperator::IsNull)
}

fn coerce_stats_datum_for_predicate(datum: Datum, predicate_data_type: &DataType) -> Option<Datum> {
    match (datum, predicate_data_type) {
        (datum @ Datum::Bool(_), DataType::Boolean(_))
        | (datum @ Datum::TinyInt(_), DataType::TinyInt(_))
        | (datum @ Datum::SmallInt(_), DataType::SmallInt(_))
        | (datum @ Datum::Int(_), DataType::Int(_))
        | (datum @ Datum::Long(_), DataType::BigInt(_))
        | (datum @ Datum::Float(_), DataType::Float(_))
        | (datum @ Datum::Double(_), DataType::Double(_))
        | (datum @ Datum::String(_), DataType::VarChar(_))
        | (datum @ Datum::String(_), DataType::Char(_))
        | (datum @ Datum::Bytes(_), DataType::Binary(_))
        | (datum @ Datum::Bytes(_), DataType::VarBinary(_))
        | (datum @ Datum::Date(_), DataType::Date(_))
        | (datum @ Datum::Time(_), DataType::Time(_))
        | (datum @ Datum::Timestamp { .. }, DataType::Timestamp(_))
        | (datum @ Datum::LocalZonedTimestamp { .. }, DataType::LocalZonedTimestamp(_))
        | (datum @ Datum::Decimal { .. }, DataType::Decimal(_)) => Some(datum),
        (Datum::TinyInt(value), DataType::SmallInt(_)) => Some(Datum::SmallInt(value as i16)),
        (Datum::TinyInt(value), DataType::Int(_)) => Some(Datum::Int(value as i32)),
        (Datum::TinyInt(value), DataType::BigInt(_)) => Some(Datum::Long(value as i64)),
        (Datum::SmallInt(value), DataType::Int(_)) => Some(Datum::Int(value as i32)),
        (Datum::SmallInt(value), DataType::BigInt(_)) => Some(Datum::Long(value as i64)),
        (Datum::Int(value), DataType::BigInt(_)) => Some(Datum::Long(value as i64)),
        (Datum::Float(value), DataType::Double(_)) => Some(Datum::Double(value as f64)),
        _ => None,
    }
}

fn extract_stats_datum(row: &BinaryRow, index: usize, data_type: &DataType) -> Option<Datum> {
    let min_row_len = BinaryRow::cal_fix_part_size_in_bytes(row.arity()) as usize;
    if index >= row.arity() as usize || row.data().len() < min_row_len {
        return None;
    }

    match extract_datum(row, index, data_type) {
        Ok(Some(datum)) => Some(datum),
        Ok(None) | Err(_) => None,
    }
}

/// Check whether a data-evolution file group *may* contain rows matching all `predicates`.
///
/// In data evolution mode, a logical row can be spread across multiple files with
/// different column sets. After `group_by_overlapping_row_id`, each group contains
/// files covering the same row ID range. Stats for each field come from the file
/// with the highest `max_sequence_number` that actually contains that field.
///
/// Reference: [DataEvolutionFileStoreScan.evolutionStats](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/operation/DataEvolutionFileStoreScan.java)
fn data_evolution_group_matches_predicates(
    group: &[DataFileMeta],
    predicates: &[Predicate],
    table_fields: &[DataField],
) -> bool {
    if predicates.is_empty() || group.is_empty() {
        return true;
    }

    if predicates
        .iter()
        .any(|p| matches!(p, Predicate::AlwaysFalse))
    {
        return false;
    }
    if predicates
        .iter()
        .all(|p| matches!(p, Predicate::AlwaysTrue))
    {
        return true;
    }

    // Sort files by max_sequence_number descending so the highest-seq file wins per field.
    let mut sorted_files: Vec<&DataFileMeta> = group.iter().collect();
    sorted_files.sort_by(|a, b| b.max_sequence_number.cmp(&a.max_sequence_number));

    // For each table field, find which file (index in sorted_files) provides it,
    // and the field's offset within that file's stats.
    let field_sources: Vec<Option<(usize, usize)>> = table_fields
        .iter()
        .map(|field| {
            for (file_idx, file) in sorted_files.iter().enumerate() {
                let file_columns = file_stats_columns(file, table_fields);
                for (stats_idx, col_name) in file_columns.iter().enumerate() {
                    if *col_name == field.name() {
                        return Some((file_idx, stats_idx));
                    }
                }
            }
            None
        })
        .collect();

    // Build per-file stats (lazily, only parse once per file).
    let file_stats: Vec<Option<FileStatsRows>> = sorted_files
        .iter()
        .map(|file| FileStatsRows::try_from_data_file(file, table_fields))
        .collect();

    // row_count is the max across the group (overlapping row ranges).
    let row_count = group.iter().map(|f| f.row_count).max().unwrap_or(0);

    predicates.iter().all(|predicate| {
        data_evolution_predicate_may_match(
            predicate,
            table_fields,
            &field_sources,
            &file_stats,
            row_count,
        )
    })
}

/// Resolve which columns a file's value stats cover.
/// If `value_stats_cols` is set, those are the stats columns. Otherwise, the file's stats
/// cover all table fields (or `write_cols` if present).
fn file_stats_columns<'a>(file: &'a DataFileMeta, table_fields: &'a [DataField]) -> Vec<&'a str> {
    if let Some(cols) = &file.value_stats_cols {
        return cols.iter().map(|s| s.as_str()).collect();
    }
    match &file.write_cols {
        Some(cols) => cols.iter().map(|s| s.as_str()).collect(),
        None => table_fields.iter().map(|f| f.name()).collect(),
    }
}

fn data_evolution_predicate_may_match(
    predicate: &Predicate,
    table_fields: &[DataField],
    field_sources: &[Option<(usize, usize)>],
    file_stats: &[Option<FileStatsRows>],
    row_count: i64,
) -> bool {
    match predicate {
        Predicate::AlwaysTrue => true,
        Predicate::AlwaysFalse => false,
        Predicate::And(children) => children.iter().all(|child| {
            data_evolution_predicate_may_match(
                child,
                table_fields,
                field_sources,
                file_stats,
                row_count,
            )
        }),
        Predicate::Or(_) | Predicate::Not(_) => true,
        Predicate::Leaf {
            index,
            data_type,
            op,
            literals,
            ..
        } => {
            let Some(source) = field_sources.get(*index).copied().flatten() else {
                // Field not found in any file — treat as all-null column.
                return missing_field_may_match(*op, row_count);
            };
            let (file_idx, stats_idx) = source;
            let Some(stats) = &file_stats[file_idx] else {
                return true; // fail-open if stats unavailable
            };
            let stats_data_type = table_fields
                .get(*index)
                .map(|f| f.data_type())
                .unwrap_or(data_type);
            data_leaf_may_match(stats_idx, stats_data_type, data_type, *op, literals, stats)
        }
    }
}

/// TableScan for full table scan (no incremental, no predicate).
///
/// Reference: [pypaimon.read.table_scan.TableScan](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/read/table_scan.py)
#[derive(Debug, Clone)]
pub struct TableScan<'a> {
    table: &'a Table,
    filter: Option<Predicate>,
    /// Optional limit on the number of rows to return.
    /// When set, the scan will try to return only enough splits to satisfy the limit.
    limit: Option<usize>,
}

impl<'a> TableScan<'a> {
    pub fn new(table: &'a Table, filter: Option<Predicate>, limit: Option<usize>) -> Self {
        Self {
            table,
            filter,
            limit,
        }
    }

    /// Plan the full scan: resolve snapshot (via options or latest), then read manifests and build DataSplits.
    ///
    /// Time travel is resolved from table options:
    /// - `scan.snapshot-id` → read that specific snapshot
    /// - `scan.timestamp-millis` → find the latest snapshot <= that timestamp
    /// - otherwise → read the latest snapshot
    ///
    /// Reference: [TimeTravelUtil.tryTravelToSnapshot](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/table/source/snapshot/TimeTravelUtil.java)
    pub async fn plan(&self) -> crate::Result<Plan> {
        let file_io = self.table.file_io();
        let table_path = self.table.location();
        let snapshot_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let core_options = CoreOptions::new(self.table.schema().options());

        let snapshot = if let Some(tag_name) = core_options.scan_tag_name() {
            let tag_manager = TagManager::new(file_io.clone(), table_path.to_string());
            match tag_manager.get(tag_name).await? {
                Some(s) => s,
                None => {
                    return Err(Error::DataInvalid {
                        message: format!("Tag '{tag_name}' doesn't exist."),
                        source: None,
                    })
                }
            }
        } else if let Some(id) = core_options.scan_snapshot_id() {
            snapshot_manager.get_snapshot(id).await?
        } else if let Some(ts) = core_options.scan_timestamp_millis() {
            match snapshot_manager.earlier_or_equal_time_mills(ts).await? {
                Some(s) => s,
                None => {
                    return Err(Error::DataInvalid {
                        message: format!("No snapshot found with timestamp <= {ts}"),
                        source: None,
                    })
                }
            }
        } else {
            match snapshot_manager.get_latest_snapshot().await? {
                Some(s) => s,
                None => return Ok(Plan::new(Vec::new())),
            }
        };
        self.plan_snapshot(snapshot).await
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

        // Compute predicates before reading manifests so they can be pushed down.
        let partition_keys = self.table.schema().partition_keys();
        let (partition_predicate, data_predicates) = if let Some(filter) = self.filter.clone() {
            if partition_keys.is_empty() {
                (None, filter.split_and())
            } else {
                split_partition_and_data_predicates(
                    filter,
                    self.table.schema().fields(),
                    partition_keys,
                )
            }
        } else {
            (None, Vec::new())
        };

        // Resolve partition fields for manifest-file-level stats pruning.
        let partition_fields: Vec<DataField> = partition_keys
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
            &data_predicates
        };

        let has_primary_keys = !self.table.schema().primary_keys().is_empty();

        // Compute bucket predicate and key fields for per-entry bucket pruning.
        // Only supported for the default bucket function (MurmurHash3-based).
        let (bucket_predicate, bucket_key_fields): (Option<Predicate>, Vec<DataField>) =
            if !core_options.is_default_bucket_function() {
                (None, Vec::new())
            } else if let Some(filter) = &self.filter {
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
                if bucket_keys.is_empty() {
                    (None, Vec::new())
                } else {
                    let fields: Vec<DataField> = bucket_keys
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
                    if fields.len() == bucket_keys.len() {
                        let pred = extract_predicate_for_keys(
                            filter,
                            self.table.schema().fields(),
                            &bucket_keys,
                        );
                        (pred, fields)
                    } else {
                        (None, Vec::new())
                    }
                }
            } else {
                (None, Vec::new())
            };

        let entries = read_all_manifest_entries(
            file_io,
            table_path,
            &snapshot,
            deletion_vectors_enabled,
            has_primary_keys,
            partition_predicate.as_ref(),
            &partition_fields,
            pushdown_data_predicates,
            self.table.schema().id(),
            self.table.schema().fields(),
            bucket_predicate.as_ref(),
            &bucket_key_fields,
        )
        .await?;
        let entries = merge_manifest_entries(entries);
        if entries.is_empty() {
            return Ok(Plan::new(Vec::new()));
        }

        // For non-data-evolution tables, cross-schema files were kept (fail-open)
        // by the pushdown. Apply the full schema-aware filter for those files.
        let entries = if data_predicates.is_empty() || data_evolution_enabled {
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
                            &data_predicates,
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
        let deletion_files_map = if let Some(index_manifest_name) = snapshot.index_manifest() {
            let index_manifest_path = format!("{base_path}/{MANIFEST_DIR}");
            let path = format!("{index_manifest_path}/{index_manifest_name}");
            let index_entries = IndexManifest::read(file_io, &path).await?;
            Some(build_deletion_files_map(&index_entries, base_path))
        } else {
            None
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
                let row_id_groups: Vec<Vec<DataFileMeta>> = if data_predicates.is_empty() {
                    row_id_groups
                } else {
                    row_id_groups
                        .into_iter()
                        .filter(|group| {
                            data_evolution_group_matches_predicates(
                                group,
                                &data_predicates,
                                self.table.schema().fields(),
                            )
                        })
                        .collect()
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
                splits.push(builder.build()?);
            }
        }

        // Apply limit pushdown to reduce the number of splits if possible
        let splits = self.apply_limit_pushdown(splits);

        Ok(Plan::new(splits))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        compute_target_buckets, data_file_matches_predicates, extract_predicate_for_keys,
        group_by_overlapping_row_id, partition_matches_predicate,
    };
    use crate::spec::{
        stats::BinaryTableStats, ArrayType, DataField, DataFileMeta, DataType, Datum,
        DeletionVectorMeta, FileKind, IndexFileMeta, IndexManifestEntry, IntType, Predicate,
        PredicateBuilder, PredicateOperator, VarCharType,
    };
    use crate::table::source::DeletionFile;
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

    struct SerializedBinaryRowBuilder {
        arity: i32,
        null_bits_size: usize,
        data: Vec<u8>,
    }

    impl SerializedBinaryRowBuilder {
        fn new(arity: i32) -> Self {
            let null_bits_size = crate::spec::BinaryRow::cal_bit_set_width_in_bytes(arity) as usize;
            let fixed_part_size = null_bits_size + (arity as usize) * 8;
            Self {
                arity,
                null_bits_size,
                data: vec![0u8; fixed_part_size],
            }
        }

        fn field_offset(&self, pos: usize) -> usize {
            self.null_bits_size + pos * 8
        }

        fn write_string(&mut self, pos: usize, value: &str) {
            let var_offset = self.data.len();
            self.data.extend_from_slice(value.as_bytes());
            let encoded = ((var_offset as u64) << 32) | (value.len() as u64);
            let offset = self.field_offset(pos);
            self.data[offset..offset + 8].copy_from_slice(&encoded.to_le_bytes());
        }

        fn build_serialized(self) -> Vec<u8> {
            let mut serialized = Vec::with_capacity(4 + self.data.len());
            serialized.extend_from_slice(&self.arity.to_be_bytes());
            serialized.extend_from_slice(&self.data);
            serialized
        }
    }

    struct RawBinaryRowBuilder {
        arity: i32,
        null_bits_size: usize,
        data: Vec<u8>,
    }

    impl RawBinaryRowBuilder {
        fn new(arity: i32) -> Self {
            let null_bits_size = crate::spec::BinaryRow::cal_bit_set_width_in_bytes(arity) as usize;
            let fixed_part_size = null_bits_size + (arity as usize) * 8;
            Self {
                arity,
                null_bits_size,
                data: vec![0u8; fixed_part_size],
            }
        }

        fn field_offset(&self, pos: usize) -> usize {
            self.null_bits_size + pos * 8
        }

        fn set_null_at(&mut self, pos: usize) {
            let bit_index = pos + crate::spec::BinaryRow::HEADER_SIZE_IN_BYTES as usize;
            let byte_index = bit_index / 8;
            let bit_offset = bit_index % 8;
            self.data[byte_index] |= 1 << bit_offset;

            let offset = self.field_offset(pos);
            self.data[offset..offset + 8].fill(0);
        }

        fn write_int(&mut self, pos: usize, value: i32) {
            let offset = self.field_offset(pos);
            self.data[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
        }

        fn build(self) -> Vec<u8> {
            debug_assert_eq!(
                self.data.len(),
                self.null_bits_size + (self.arity as usize) * 8
            );
            self.data
        }
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

    fn int_stats_row(value: Option<i32>) -> Vec<u8> {
        let mut builder = RawBinaryRowBuilder::new(1);
        match value {
            Some(value) => builder.write_int(0, value),
            None => builder.set_null_at(0),
        }
        let raw = builder.build();
        let mut serialized = Vec::with_capacity(4 + raw.len());
        serialized.extend_from_slice(&(1_i32).to_be_bytes());
        serialized.extend_from_slice(&raw);
        serialized
    }

    fn test_data_file_meta(
        min_values: Vec<u8>,
        max_values: Vec<u8>,
        null_counts: Vec<i64>,
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
        null_counts: Vec<i64>,
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
        let mut builder = SerializedBinaryRowBuilder::new(1);
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
        let file =
            test_data_file_meta(int_stats_row(Some(10)), int_stats_row(Some(20)), vec![0], 5);
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
        let file =
            test_data_file_meta(int_stats_row(Some(10)), int_stats_row(Some(20)), vec![0], 5);
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
        let file = test_data_file_meta(int_stats_row(None), int_stats_row(None), vec![5], 5);
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
        let file =
            test_data_file_meta(int_stats_row(Some(10)), int_stats_row(Some(20)), vec![0], 5);
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
        let file = test_data_file_meta(Vec::new(), Vec::new(), vec![0], 5);
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
            vec![0],
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
    fn test_data_file_matches_dense_stats_arity_mismatch_fails_open() {
        let mut builder = RawBinaryRowBuilder::new(3);
        builder.write_int(0, 10);
        builder.write_int(1, 100);
        builder.write_int(2, 200);
        let raw = builder.build();
        let mut min_serialized = Vec::with_capacity(4 + raw.len());
        min_serialized.extend_from_slice(&(3_i32).to_be_bytes());
        min_serialized.extend_from_slice(&raw);

        let mut builder = RawBinaryRowBuilder::new(3);
        builder.write_int(0, 20);
        builder.write_int(1, 200);
        builder.write_int(2, 300);
        let raw = builder.build();
        let mut max_serialized = Vec::with_capacity(4 + raw.len());
        max_serialized.extend_from_slice(&(3_i32).to_be_bytes());
        max_serialized.extend_from_slice(&raw);

        let fields = int_field();
        let file = test_data_file_meta(min_serialized, max_serialized, vec![0, 0, 0], 5);
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
            vec![0],
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
            vec![0],
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
