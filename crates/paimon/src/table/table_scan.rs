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
use crate::io::FileIO;
use crate::spec::{
    eval_row, field_idx_to_partition_idx, BinaryRow, CoreOptions, DataFileMeta, FileKind,
    IndexManifest, ManifestEntry, PartitionComputer, Predicate, Snapshot,
};
use crate::table::bin_pack::split_for_batch;
use crate::table::source::{DataSplitBuilder, DeletionFile, PartitionBucket, Plan};
use crate::table::SnapshotManager;
use crate::Error;
use std::collections::{HashMap, HashSet};

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

/// Reads all manifest entries for a snapshot (base + delta manifest lists, then each manifest file).
async fn read_all_manifest_entries(
    file_io: &FileIO,
    table_path: &str,
    snapshot: &Snapshot,
) -> crate::Result<Vec<ManifestEntry>> {
    let mut manifest_files =
        read_manifest_list(file_io, table_path, snapshot.base_manifest_list()).await?;
    let delta = read_manifest_list(file_io, table_path, snapshot.delta_manifest_list()).await?;
    manifest_files.extend(delta);

    let manifest_path_prefix = format!("{}/{}", table_path.trim_end_matches('/'), MANIFEST_DIR);
    let mut all_entries = Vec::new();
    // todo: consider use multiple-threads read manifest
    for meta in manifest_files {
        let path = format!("{}/{}", manifest_path_prefix, meta.file_name());
        let entries = crate::spec::Manifest::read(file_io, &path).await?;
        all_entries.extend(entries);
    }
    Ok(all_entries)
}

fn filter_manifest_entries(
    entries: Vec<ManifestEntry>,
    deletion_vectors_enabled: bool,
) -> Vec<ManifestEntry> {
    entries
        .into_iter()
        .filter(|entry| !(deletion_vectors_enabled && entry.file().level == 0))
        .collect()
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
        for (data_file_name, (offset, length)) in ranges {
            per_bucket.insert(
                data_file_name.clone(),
                DeletionFile::new(
                    dv_path.clone(),
                    *offset as i64,
                    *length as i64,
                    // todo: consider cardinality
                    None,
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
fn group_by_overlapping_row_id(mut files: Vec<DataFileMeta>) -> Vec<Vec<DataFileMeta>> {
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

/// TableScan for full table scan (no incremental, no predicate).
///
/// Reference: [pypaimon.read.table_scan.TableScan](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/read/table_scan.py)
#[derive(Debug, Clone)]
pub struct TableScan<'a> {
    table: &'a Table,
    filter: Option<Predicate>,
}

impl<'a> TableScan<'a> {
    pub fn new(table: &'a Table, filter: Option<Predicate>) -> Self {
        Self { table, filter }
    }

    /// Plan the full scan: read latest snapshot, manifest list, manifest entries, then build DataSplits using bin packing.
    pub async fn plan(&self) -> crate::Result<Plan> {
        let file_io = self.table.file_io();
        let table_path = self.table.location();
        let snapshot_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());

        let snapshot = match snapshot_manager.get_latest_snapshot().await? {
            Some(s) => s,
            None => return Ok(Plan::new(Vec::new())),
        };
        self.plan_snapshot(snapshot).await
    }

    async fn plan_snapshot(&self, snapshot: Snapshot) -> crate::Result<Plan> {
        let file_io = self.table.file_io();
        let table_path = self.table.location();
        let core_options = CoreOptions::new(self.table.schema().options());
        let deletion_vectors_enabled = core_options.deletion_vectors_enabled();
        let data_evolution_enabled = core_options.data_evolution_enabled();
        let target_split_size = core_options.source_split_target_size();
        let open_file_cost = core_options.source_split_open_file_cost();
        let entries = read_all_manifest_entries(file_io, table_path, &snapshot).await?;
        let entries = filter_manifest_entries(entries, deletion_vectors_enabled);
        let entries = merge_manifest_entries(entries);
        if entries.is_empty() {
            return Ok(Plan::new(Vec::new()));
        }

        // --- Partition predicate extraction ---
        let partition_keys = self.table.schema().partition_keys();
        let partition_predicate = if !partition_keys.is_empty() {
            self.filter.clone().and_then(|filter| {
                let mapping =
                    field_idx_to_partition_idx(self.table.schema().fields(), partition_keys);
                let conjuncts = filter.split_and();
                let remapped: Vec<Predicate> = conjuncts
                    .into_iter()
                    .filter_map(|c| c.remap_field_index(&mapping))
                    .collect();
                if remapped.is_empty() {
                    None
                } else {
                    Some(Predicate::and(remapped))
                }
            })
        } else {
            None
        };

        // --- Partition pruning: filter manifest entries before grouping ---
        //
        // Note: split construction later still requires a decodable BinaryRow
        // and will fail on corrupt partition bytes. Pruning is intentionally
        // best-effort; split construction is mandatory.
        let entries = if let Some(ref pred) = partition_predicate {
            let mut kept = Vec::with_capacity(entries.len());
            // Cache: partition bytes → accept/reject to avoid re-decoding.
            let mut cache: HashMap<Vec<u8>, bool> = HashMap::new();
            for e in entries {
                let accept = match cache.get(e.partition()) {
                    Some(&cached) => cached,
                    None => {
                        let partition_bytes = e.partition();
                        let accept = partition_matches_predicate(partition_bytes, pred)?;
                        cache.insert(partition_bytes.to_vec(), accept);
                        accept
                    }
                };
                if accept {
                    kept.push(e);
                }
            }
            kept
        } else {
            entries
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

            // Split files into groups: data evolution merges overlapping row_id ranges;
            // multi-file groups need column-wise merge, single-file groups can be bin-packed.
            let file_groups_with_raw: Vec<(Vec<DataFileMeta>, bool)> = if data_evolution_enabled {
                let row_id_groups = group_by_overlapping_row_id(data_files);
                let (singles, multis): (Vec<_>, Vec<_>) =
                    row_id_groups.into_iter().partition(|g| g.len() == 1);

                let mut result: Vec<(Vec<DataFileMeta>, bool)> = Vec::new();

                // Multi-file groups: each becomes its own split, raw_convertible=false
                for group in multis {
                    result.push((group, false));
                }

                // Single-file groups: flatten and bin-pack, raw_convertible=true
                let single_files: Vec<DataFileMeta> = singles.into_iter().flatten().collect();
                for file_group in split_for_batch(single_files, target_split_size, open_file_cost) {
                    result.push((file_group, true));
                }

                result
            } else {
                split_for_batch(data_files, target_split_size, open_file_cost)
                    .into_iter()
                    .map(|g| (g, true))
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
        Ok(Plan::new(splits))
    }
}

#[cfg(test)]
mod tests {
    use super::{group_by_overlapping_row_id, partition_matches_predicate};
    use crate::spec::{
        stats::BinaryTableStats, ArrayType, DataField, DataFileMeta, DataType, Datum, IntType,
        Predicate, PredicateBuilder, PredicateOperator, VarCharType,
    };
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
            creation_time: DateTime::<Utc>::from_timestamp(0, 0).unwrap(),
            delete_row_count: None,
            embedded_index: None,
            first_row_id,
            write_cols: None,
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

    fn partition_string_field() -> Vec<DataField> {
        vec![DataField::new(
            0,
            "dt".to_string(),
            DataType::VarChar(VarCharType::default()),
        )]
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

    // ==================== group_by_overlapping_row_id tests ====================

    #[test]
    fn test_group_by_overlapping_row_id_empty() {
        let result = group_by_overlapping_row_id(vec![]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_group_by_overlapping_row_id_no_row_ids() {
        // Files without first_row_id each become their own group.
        // Sorted by (i64::MIN, -max_seq), so b(seq=2) before a(seq=1).
        let files = vec![
            make_evo_file("a", 10, 100, 1, None),
            make_evo_file("b", 10, 100, 2, None),
        ];
        let groups = group_by_overlapping_row_id(files);
        assert_eq!(file_names(&groups), vec![vec!["b"], vec!["a"]]);
    }

    #[test]
    fn test_group_by_overlapping_row_id_same_range() {
        // Two files with the same first_row_id and row_count → same range → one group.
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
        // File a: rows [0, 99], file b: rows [50, 149] → overlapping → one group.
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
        // File a: rows [0, 99], file b: rows [100, 199] → no overlap → two groups.
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
        // a: [0,99], b: [0,99] (overlap), c: None (own group), d: [200,299]
        // After sort: c(None→MIN) comes first, then b(seq=2), a(seq=1), d.
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
        // Within a group, files are sorted by (first_row_id, -max_sequence_number).
        let files = vec![
            make_evo_file("a", 10, 100, 1, Some(0)),
            make_evo_file("b", 10, 100, 3, Some(0)),
            make_evo_file("c", 10, 100, 2, Some(0)),
        ];
        let groups = group_by_overlapping_row_id(files);
        assert_eq!(groups.len(), 1);
        // Sorted by descending max_sequence_number: b(3), c(2), a(1)
        assert_eq!(file_names(&groups), vec![vec!["b", "c", "a"]]);
    }
}
