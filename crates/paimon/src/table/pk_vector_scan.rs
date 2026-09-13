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

//! Primary-key vector search planning: resolve a snapshot, plan its data splits,
//! scan the index manifest for this column's ANN segments, and accumulate one
//! search split per bucket. Mirror of Java `PrimaryKeyVectorScan` and
//! `PrimaryKeyIndexSourcePolicy`.

use std::collections::{BTreeMap, HashMap, HashSet};

use indexmap::IndexMap;

use crate::spec::{
    should_read_pk_index_source, BinaryRow, DataFileMeta, FileKind, IndexManifest, Predicate,
    PrimaryKeyIndexSourceFile, PrimaryKeyIndexSourceMeta,
};
use crate::table::bucket_filter::split_partition_and_data_predicates;
use crate::table::index_file_path::IndexFileLocation;
use crate::table::partition_filter::PartitionFilter;
use crate::table::pk_vector_bucket_split::BucketVectorSearchSplit;
use crate::table::pk_vector_orchestrator::PkVectorSearchSplit;
use crate::table::source::{merge_row_ranges, DataSplit, DataSplitBuilder, DeletionFile, RowRange};
use crate::table::Table;
use crate::vindex::pkvector::bucket::{BucketActiveFile, BucketAnnSegment};

/// A payload whose bucket-local path is resolved in planning Phase C, once the
/// owning bucket's data split (and directory) is known.
struct UnresolvedAnnSegment {
    source_meta: PrimaryKeyIndexSourceMeta,
    file_name: String,
    external_path: Option<String>,
    file_size: u64,
    index_meta: Vec<u8>,
}

/// A bucket's identity across planning inputs: the partition's serialized bytes
/// (`BinaryRow` is not hashable) paired with the bucket number.
type BucketKey = (Vec<u8>, i32);

fn data_invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

fn source_files_unique(files: &[PrimaryKeyIndexSourceFile]) -> bool {
    let mut seen = HashSet::new();
    files.iter().all(|file| seen.insert(file.file_name()))
}

fn current_ann_segments(
    active_data_files: &[DataFileMeta],
    ann_segments: Vec<BucketAnnSegment>,
) -> crate::Result<Vec<BucketAnnSegment>> {
    let mut sources_by_level: BTreeMap<i32, Vec<PrimaryKeyIndexSourceFile>> = BTreeMap::new();
    for file in active_data_files {
        if should_read_pk_index_source(file) {
            sources_by_level
                .entry(file.level)
                .or_default()
                .push(PrimaryKeyIndexSourceFile::new(
                    file.file_name.clone(),
                    file.row_count,
                )?);
        }
    }
    for sources in sources_by_level.values_mut() {
        sources.sort_by(|a, b| a.file_name().cmp(b.file_name()));
    }

    let mut segments_by_level: BTreeMap<i32, Vec<BucketAnnSegment>> = BTreeMap::new();
    for segment in ann_segments {
        let source_meta = &segment.source_meta;
        let Some(desired) = sources_by_level.get(&source_meta.data_level()) else {
            continue;
        };
        if source_files_unique(source_meta.source_files())
            && desired.as_slice() == source_meta.source_files()
        {
            segments_by_level
                .entry(source_meta.data_level())
                .or_default()
                .push(segment);
        }
    }

    let mut current = Vec::new();
    for mut level_segments in segments_by_level.into_values() {
        if level_segments.len() == 1 {
            current.push(level_segments.remove(0));
        }
    }
    Ok(current)
}

/// Combines one bucket's data splits into a single split, keeping data files and
/// deletion files in strict parallel order and rejecting duplicate file names.
struct BucketAccumulator {
    snapshot_id: i64,
    partition: BinaryRow,
    bucket: i32,
    bucket_path: Option<String>,
    total_buckets: Option<i32>,
    data_files: Vec<DataFileMeta>,
    deletion_files: Vec<Option<DeletionFile>>,
    seen: HashSet<String>,
    any_deletion: bool,
}

impl BucketAccumulator {
    fn new(snapshot_id: i64, partition: BinaryRow, bucket: i32) -> Self {
        Self {
            snapshot_id,
            partition,
            bucket,
            bucket_path: None,
            total_buckets: None,
            data_files: Vec::new(),
            deletion_files: Vec::new(),
            seen: HashSet::new(),
            any_deletion: false,
        }
    }

    fn add(&mut self, split: &DataSplit) -> crate::Result<()> {
        if split.snapshot_id() != self.snapshot_id {
            return Err(data_invalid(
                "data split snapshot id does not match plan snapshot",
            ));
        }
        if split.partition().to_serialized_bytes() != self.partition.to_serialized_bytes() {
            return Err(data_invalid(
                "data split partition does not match bucket group",
            ));
        }
        if split.bucket() != self.bucket {
            return Err(data_invalid(
                "data split bucket does not match bucket group",
            ));
        }
        match &self.bucket_path {
            Some(p) if p != split.bucket_path() => {
                return Err(data_invalid("inconsistent bucket path within bucket group"))
            }
            None => self.bucket_path = Some(split.bucket_path().to_string()),
            _ => {}
        }
        match self.total_buckets {
            Some(tb) if tb != split.total_buckets() => {
                return Err(data_invalid(
                    "inconsistent total buckets within bucket group",
                ))
            }
            None => self.total_buckets = Some(split.total_buckets()),
            _ => {}
        }
        let dvs = split.data_deletion_files();
        for (i, file) in split.data_files().iter().enumerate() {
            if !self.seen.insert(file.file_name.clone()) {
                return Err(data_invalid(format!(
                    "duplicate data file in bucket group: {}",
                    file.file_name
                )));
            }
            self.data_files.push(file.clone());
            let df = dvs.and_then(|d| d.get(i).cloned().flatten());
            if df.is_some() {
                self.any_deletion = true;
            }
            self.deletion_files.push(df);
        }
        Ok(())
    }

    fn build(self) -> crate::Result<DataSplit> {
        let mut builder = DataSplitBuilder::new()
            .with_snapshot(self.snapshot_id)
            .with_partition(self.partition)
            .with_bucket(self.bucket)
            .with_bucket_path(
                self.bucket_path
                    .ok_or_else(|| data_invalid("bucket group has no bucket path"))?,
            )
            .with_total_buckets(self.total_buckets.unwrap_or(1))
            .with_data_files(self.data_files)
            .with_raw_convertible(false);
        if self.any_deletion {
            builder = builder.with_data_deletion_files(self.deletion_files);
        }
        builder.build()
    }
}

/// The per-bucket search splits produced by planning.
pub(crate) struct PkVectorScanPlan {
    // The snapshot the plan resolved during planning (pinned before the index
    // manifest is read). It is authoritative even when planning yields zero
    // searchable splits, so a cross-route consistency guard can require one
    // pinned snapshot across routes. It is `0` only for a table with no snapshot
    // at all (never written), which also yields empty `splits`.
    pub snapshot_id: i64,
    pub splits: Vec<PkVectorSearchSplit>,
    // Per-split allow-list of physical rows, indexed parallel to `splits`, holding
    // ONLY the files the engine's split actually restricted. Three states per file,
    // as in Java `rowRangesByFile`: absent means every row is readable, an empty
    // list means none is, and a non-empty list means exactly those. Ranges rather
    // than materialized positions, because this is what a read is limited to, and
    // because the row counts these arrive with are untrusted — sizing an allow-list
    // from one is unbounded work.
    // Each list is normalized: sorted, non-overlapping, inclusive, file-local.
    // Populated when the plan was built from engine-supplied bucket splits, which
    // carry row ranges the engine's own planner already resolved -- possibly an
    // empty map, when that planner narrowed nothing. `None` for a plan read from
    // this table's index manifest, which places no positional restriction at all.
    pub physical_row_ranges_by_split: Option<Vec<HashMap<String, Vec<RowRange>>>>,
}

pub(crate) struct PkVectorScan<'a> {
    table: &'a Table,
    vector_field_id: i32,
    index_type: String,
    filter: Option<Predicate>,
}

impl<'a> PkVectorScan<'a> {
    pub(crate) fn new(
        table: &'a Table,
        vector_field_id: i32,
        index_type: String,
        filter: Option<Predicate>,
    ) -> Self {
        Self {
            table,
            vector_field_id,
            index_type,
            filter,
        }
    }

    pub(crate) async fn plan(&self) -> crate::Result<PkVectorScanPlan> {
        let snapshot_manager = self.table.snapshot_manager();

        // Data splits first, via the table's own scan resolution (which honors
        // time travel / scan.snapshot-id). Deriving the snapshot from the scan's
        // own output — rather than resolving `get_latest_snapshot()` separately —
        // keeps the index manifest and the data splits on ONE snapshot, matching
        // Java `PrimaryKeyVectorScan` (resolve one snapshot up front, read data and
        // index from it). It also avoids a time-travel mismatch (data from the
        // travelled snapshot, index from latest) and a TOCTOU where a concurrent
        // commit lands between two independent resolutions.
        //
        // The residual scalar filter, when set, is pushed into the read builder so
        // scan planning drops files whose stats cannot match the predicate, mirroring
        // Java `PrimaryKeyVectorScan` applying the filter at scan time. Files that
        // survive are still residual-filtered per row downstream; this only avoids
        // re-reading files the predicate already excludes.
        let mut read_builder = self.table.new_read_builder();
        if let Some(filter) = &self.filter {
            read_builder.with_filter(filter.clone());
        }
        // Plan the data splits and capture the snapshot the scan pinned in one
        // pass. The trace carries the resolved snapshot id even when the scan
        // yields zero data splits, so the plan reports its real snapshot id
        // (required by the cross-route snapshot-consistency guard) instead of
        // deriving it from a first split that may not exist.
        let (data_plan, trace) = read_builder
            .new_scan()
            .with_scan_all_files()
            .plan_with_trace()
            .await?;
        let data_splits = data_plan.splits().to_vec();

        // No snapshot at all (table never written): nothing to search and no
        // snapshot to pin. The empty split list makes every downstream consumer
        // treat this as "no candidates", so this is the only plan without a real
        // snapshot id.
        let Some(snapshot_id) = trace.snapshot_id else {
            return Ok(PkVectorScanPlan {
                snapshot_id: 0,
                splits: Vec::new(),
                physical_row_ranges_by_split: None,
            });
        };
        let snapshot = snapshot_manager.get_snapshot(snapshot_id).await?;

        // Index-manifest scan into filtered ANN payload tuples.
        let table_path = self.table.location().trim_end_matches('/');
        let mut entries = Vec::new();
        if let Some(name) = snapshot.index_manifest() {
            let path = snapshot_manager.manifest_path(name);
            for entry in IndexManifest::read(self.table.file_io(), &path).await? {
                // The on-disk index manifest is combined to live ADD entries only.
                // A non-ADD entry means a malformed manifest; fail loud rather than
                // silently drop it (mirrors Java `checkArgument(kind == ADD)`).
                if entry.kind != FileKind::Add {
                    return Err(data_invalid(format!(
                        "index manifest entry {} is not active (kind {:?})",
                        entry.index_file.file_name, entry.kind
                    )));
                }
                if entry.index_file.index_type != self.index_type {
                    continue;
                }
                let Some(gim) = entry.index_file.global_index_meta.clone() else {
                    continue;
                };
                if gim.index_field_id != self.vector_field_id {
                    continue;
                }
                if gim.source_meta.is_none() {
                    continue;
                }
                let partition = BinaryRow::from_serialized_bytes(&entry.partition)?;
                let file_size = u64::try_from(entry.index_file.file_size)
                    .map_err(|_| data_invalid("index file size must not be negative"))?;
                let source_meta =
                    PrimaryKeyIndexSourceMeta::from_global_index_meta(&gim).map_err(|_| {
                        data_invalid(format!(
                            "index file {} is not active",
                            entry.index_file.file_name
                        ))
                    })?;
                entries.push((
                    partition,
                    entry.bucket,
                    UnresolvedAnnSegment {
                        source_meta,
                        file_name: entry.index_file.file_name.clone(),
                        external_path: entry.index_file.external_path.clone(),
                        file_size,
                        // The Lumina reader consumes this as its serialized index
                        // metadata; the vindex reader ignores it and loads metadata
                        // from the segment file bytes. Absent value defaults to empty.
                        index_meta: gim.index_meta.clone().unwrap_or_default(),
                    },
                ));
            }
        }

        let index_file_in_data_file_dir = self
            .table
            .schema()
            .core_options()
            .index_file_in_data_file_dir();
        let splits = plan_from_inputs(
            snapshot_id,
            data_splits,
            entries,
            table_path,
            index_file_in_data_file_dir,
        )?;
        Ok(PkVectorScanPlan {
            snapshot_id,
            splits,
            physical_row_ranges_by_split: None,
        })
    }

    /// Build a plan from bucket splits an engine planned elsewhere, instead of from
    /// this table's index manifest.
    ///
    /// The splits are the planning input and are taken as authoritative: their
    /// payload files, their per-file row ranges, and the snapshot they pin are used
    /// as given, and no index manifest is read. Only the partition conjuncts of this
    /// scan's filter are re-applied, because a caller may narrow the query further
    /// than the planner that produced the splits.
    ///
    /// Mirrors what Java's `PrimaryKeyVectorRead` does with a
    /// `BucketVectorSearchSplit`: search the payloads the split names, over the rows
    /// the split allows.
    pub(crate) fn plan_for_bucket_vector_splits(
        &self,
        splits: Vec<BucketVectorSearchSplit>,
    ) -> crate::Result<PkVectorScanPlan> {
        // Partition conjuncts only. Data conjuncts stay a per-row residual applied
        // during the search: pruning a whole bucket on them would drop rows that
        // still match.
        let partition_filter = self.filter.as_ref().and_then(|filter| {
            let (partition_predicate, _data_predicates) = split_partition_and_data_predicates(
                filter.clone(),
                self.table.schema().fields(),
                self.table.schema().partition_keys(),
            );
            partition_predicate.map(|predicate| {
                PartitionFilter::from_predicate(predicate, &self.table.schema().partition_fields())
            })
        });
        plan_from_bucket_splits(
            &self.index_type,
            self.vector_field_id,
            partition_filter.as_ref(),
            self.table.location().trim_end_matches('/'),
            self.table
                .schema()
                .core_options()
                .index_file_in_data_file_dir(),
            splits,
        )
    }
}

/// The `Table`-independent core of [`PkVectorScan::plan_for_bucket_vector_splits`],
/// so planning from engine-supplied splits is testable the same way planning from a
/// manifest is.
fn plan_from_bucket_splits(
    index_type: &str,
    vector_field_id: i32,
    partition_filter: Option<&PartitionFilter>,
    table_path: &str,
    index_file_in_data_file_dir: bool,
    splits: Vec<BucketVectorSearchSplit>,
) -> crate::Result<PkVectorScanPlan> {
    // A plan's snapshot id stays authoritative even when nothing is searchable, and
    // empty input pins no snapshot to report. Reject rather than invent one.
    if splits.is_empty() {
        return Err(data_invalid(
            "bucket-split planning requires at least one bucket split",
        ));
    }

    let mut snapshot_id: Option<i64> = None;
    let mut seen_buckets: HashSet<BucketKey> = HashSet::new();
    let mut data_splits: Vec<DataSplit> = Vec::with_capacity(splits.len());
    let mut index_entries: Vec<(BinaryRow, i32, UnresolvedAnnSegment)> = Vec::new();
    let mut listed_ranges: HashMap<BucketKey, IndexMap<String, Vec<RowRange>>> = HashMap::new();

    for split in splits {
        let (data_split, payload_files, row_ranges_by_file) = split.into_parts();

        // Row ranges belong to the bucket form, one list per data file. A nested
        // split carrying its own would be a second authority over which physical
        // rows are readable, free to disagree with the first. Java's planner
        // builds the nested split without them.
        if data_split.row_ranges().is_some() {
            return Err(data_invalid(
                "a bucket split's nested data split must not carry row ranges",
            ));
        }

        // One snapshot across every split: candidates found under different
        // snapshots cannot be merged into a single Top-K. Checked before pruning,
        // so a mismatch is reported even when the offending split would have been
        // pruned away and the inconsistency left no trace.
        match snapshot_id {
            None => snapshot_id = Some(data_split.snapshot_id()),
            Some(pinned) if pinned != data_split.snapshot_id() => {
                return Err(data_invalid(format!(
                    "bucket splits pin different snapshots: {} and {}",
                    pinned,
                    data_split.snapshot_id()
                )));
            }
            Some(_) => {}
        }

        // Java emits exactly one split per (partition, bucket). Buffers decoded
        // independently cannot enforce that between them, and two splits for one
        // bucket would search its rows twice.
        let key: BucketKey = (
            data_split.partition().to_serialized_bytes(),
            data_split.bucket(),
        );
        if !seen_buckets.insert(key.clone()) {
            return Err(data_invalid(format!(
                "bucket splits repeat bucket {} of one partition",
                data_split.bucket()
            )));
        }

        if let Some(filter) = partition_filter {
            if !filter.matches_entry(&key.0)? {
                continue;
            }
        }

        for payload in payload_files {
            let parts = payload.into_parts();
            // The same three filters the manifest route applies: this column's
            // index type, this column's field id, and a payload that carries the
            // source metadata a search needs to map ordinals back to rows.
            if parts.index_type != index_type
                || parts.global_index_meta.index_field_id != vector_field_id
                || parts.global_index_meta.source_meta.is_none()
            {
                continue;
            }
            // Java writes the size as a signed long, so the wire allows a
            // negative value the segment addressing cannot represent.
            let file_size = u64::try_from(parts.file_size)
                .map_err(|_| data_invalid("index file size must not be negative"))?;
            let source_meta =
                PrimaryKeyIndexSourceMeta::from_global_index_meta(&parts.global_index_meta)
                    .map_err(|_| {
                        data_invalid(format!("index file {} is not active", parts.file_name))
                    })?;
            let index_meta = parts
                .global_index_meta
                .index_meta
                .clone()
                .unwrap_or_default();
            index_entries.push((
                data_split.partition().clone(),
                data_split.bucket(),
                UnresolvedAnnSegment {
                    source_meta,
                    file_name: parts.file_name,
                    external_path: parts.external_path,
                    file_size,
                    index_meta,
                },
            ));
        }

        listed_ranges.insert(key, row_ranges_by_file);
        data_splits.push(data_split);
    }

    // Non-empty input always pins one: the first split sets it and a mismatch
    // returns early.
    let snapshot_id = snapshot_id.expect("non-empty bucket-split input pins a snapshot");

    let splits = plan_from_inputs(
        snapshot_id,
        data_splits,
        index_entries,
        table_path,
        index_file_in_data_file_dir,
    )?;

    // Re-key the row ranges against the planned splits, which are grouped by bucket
    // and so may be ordered differently from the input.
    //
    // Only what the message actually listed is carried. A file it omits gets NO
    // entry, which is what "every row" is spelled as throughout the search kernel
    // and in Java (`rowRangesByFile.get(file) == null`). Synthesizing an explicit
    // `[0, row_count - 1]` for it instead would look equivalent and is not: the
    // row count arrives on the wire, and sizing an allow-list from an untrusted
    // number is unbounded work. An explicitly empty list stays empty — that is the
    // separate "no rows" state.
    let physical_row_ranges_by_split = splits
        .iter()
        .map(|split| {
            let listed = listed_ranges.get(&(
                split.data_split.partition().to_serialized_bytes(),
                split.data_split.bucket(),
            ));
            split
                .data_split
                .data_files()
                .iter()
                .filter_map(|file| {
                    let ranges = listed.and_then(|ranges| ranges.get(&file.file_name))?;
                    // Decoding checks each range's bounds but not their order or
                    // whether they overlap, and a read needs them normalized.
                    Some((file.file_name.clone(), merge_row_ranges(ranges.clone())))
                })
                .collect::<HashMap<String, Vec<RowRange>>>()
        })
        .collect::<Vec<_>>();

    Ok(PkVectorScanPlan {
        snapshot_id,
        splits,
        physical_row_ranges_by_split: Some(physical_row_ranges_by_split),
    })
}

/// Pure planning core, drivable without a live snapshot: group ANN payloads and
/// data splits by `(partition, bucket)`, then assemble one search split per
/// bucket that has data. Index-only buckets are dropped, not errored.
fn plan_from_inputs(
    snapshot_id: i64,
    data_splits: Vec<DataSplit>,
    index_entries: Vec<(BinaryRow, i32, UnresolvedAnnSegment)>,
    table_path: &str,
    index_file_in_data_file_dir: bool,
) -> crate::Result<Vec<PkVectorSearchSplit>> {
    type Key = (Vec<u8>, i32);

    // Phase A: group unresolved ANN payloads by (partition, bucket). The on-disk
    // path is resolved in Phase C, once the bucket's data split (and thus its
    // bucket directory) is known.
    let mut payloads_by_bucket: BTreeMap<Key, Vec<UnresolvedAnnSegment>> = BTreeMap::new();
    for (partition, bucket, payload) in index_entries {
        let key = (partition.to_serialized_bytes(), bucket);
        payloads_by_bucket.entry(key).or_default().push(payload);
    }

    // Phase B: group data splits by (partition, bucket).
    let mut accum_by_bucket: BTreeMap<Key, BucketAccumulator> = BTreeMap::new();
    for split in &data_splits {
        let key = (split.partition().to_serialized_bytes(), split.bucket());
        let acc = accum_by_bucket.entry(key).or_insert_with(|| {
            BucketAccumulator::new(snapshot_id, split.partition().clone(), split.bucket())
        });
        acc.add(split)?;
    }

    // Phase C: assemble one split per bucket that has data, resolving each
    // payload's path against the bucket directory now that it is known.
    let mut out = Vec::new();
    for (key, acc) in accum_by_bucket {
        let data_split = acc.build()?;
        let location = IndexFileLocation::BucketLocal {
            table_path,
            bucket_path: data_split.bucket_path(),
            index_file_in_data_file_dir,
        };
        let resolved_segments: Vec<BucketAnnSegment> = payloads_by_bucket
            .remove(&key)
            .unwrap_or_default()
            .into_iter()
            .map(|p| BucketAnnSegment {
                source_meta: p.source_meta,
                path: location.resolve(&p.file_name, p.external_path.as_deref()),
                file_size: p.file_size,
                index_meta: p.index_meta,
            })
            .collect();
        let ann_segments = current_ann_segments(data_split.data_files(), resolved_segments)?;
        let active_files: Vec<BucketActiveFile> = data_split
            .data_files()
            .iter()
            .filter(|f| should_read_pk_index_source(f))
            .map(|f| BucketActiveFile {
                file_name: f.file_name.clone(),
                row_count: f.row_count,
            })
            .collect();
        out.push(PkVectorSearchSplit {
            data_split,
            ann_segments,
            active_files,
        });
    }
    // Index-only buckets left in payloads_by_bucket are intentionally dropped.
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{BinaryRow, DataFileMeta, GlobalIndexMeta};
    use crate::spec::{DataField, DeletionVectorMeta};
    use crate::table::pk_vector_bucket_split::BucketVectorPayload;
    use crate::table::source::{DataSplitBuilder, DeletionFile};

    fn dfm(name: &str, rows: i64, level: i32, file_source: Option<i32>) -> DataFileMeta {
        DataFileMeta {
            file_name: name.into(),
            file_size: 1,
            row_count: rows,
            min_key: Vec::new(),
            max_key: Vec::new(),
            key_stats: BinaryTableStats::empty(),
            value_stats: BinaryTableStats::empty(),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id: 1,
            level,
            extra_files: Vec::new(),
            creation_time: None,
            delete_row_count: None,
            embedded_index: None,
            file_source,
            value_stats_cols: None,
            external_path: None,
            first_row_id: Some(0),
            write_cols: None,
            column_max_sequence_numbers: None,
        }
    }

    #[test]
    fn should_read_matches_java_policy() {
        assert!(should_read_pk_index_source(&dfm("a", 1, 1, Some(1)))); // COMPACT + level>0
        assert!(!should_read_pk_index_source(&dfm("a", 1, 0, Some(1)))); // COMPACT + level==0
        assert!(!should_read_pk_index_source(&dfm("a", 1, 3, Some(0)))); // APPEND
        assert!(!should_read_pk_index_source(&dfm("a", 1, 3, None))); // absent -> false
    }

    /// Build one Java `DataOutput#writeUTF` value (u16-BE length + modified
    /// UTF-8) for the ASCII test file names used here.
    fn java_write_utf(s: &str) -> Vec<u8> {
        let mut body = Vec::new();
        for c in s.encode_utf16() {
            if (0x0001..=0x007F).contains(&c) {
                body.push(c as u8);
            } else if c > 0x07FF {
                body.push(0xE0 | (c >> 12) as u8);
                body.push(0x80 | ((c >> 6) & 0x3F) as u8);
                body.push(0x80 | (c & 0x3F) as u8);
            } else {
                body.push(0xC0 | (c >> 6) as u8);
                body.push(0x80 | (c & 0x3F) as u8);
            }
        }
        let mut out = (body.len() as u16).to_be_bytes().to_vec();
        out.extend_from_slice(&body);
        out
    }

    /// Build a `_SOURCE_META` blob the way `PrimaryKeyIndexSourceMeta::deserialize`
    /// expects it. There is no public serializer, so we mirror the frame used by
    /// `pk_index_source.rs`'s own round-trip tests.
    fn source_meta_bytes(data_level: i32, files: &[(&str, i64)]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&1i32.to_be_bytes()); // version
        out.extend_from_slice(&data_level.to_be_bytes());
        out.extend_from_slice(&(files.len() as i32).to_be_bytes());
        for (name, rows) in files {
            out.extend_from_slice(&java_write_utf(name));
            out.extend_from_slice(&rows.to_be_bytes());
        }
        out
    }

    fn gim(field_id: i32, data_level: i32, source_files: &[(&str, i64)]) -> GlobalIndexMeta {
        GlobalIndexMeta {
            row_range_start: 0,
            row_range_end: 0,
            index_field_id: field_id,
            extra_field_ids: None,
            index_meta: Some(vec![1, 2, 3]),
            source_meta: Some(source_meta_bytes(data_level, source_files)),
        }
    }

    fn ann_segment(data_level: i32, path: &str, source_files: &[(&str, i64)]) -> BucketAnnSegment {
        BucketAnnSegment {
            source_meta: PrimaryKeyIndexSourceMeta::new(
                data_level,
                source_files
                    .iter()
                    .map(|(name, rows)| {
                        PrimaryKeyIndexSourceFile::new((*name).to_string(), *rows).unwrap()
                    })
                    .collect(),
            )
            .unwrap(),
            path: path.to_string(),
            file_size: 1,
            index_meta: Vec::new(),
        }
    }

    /// An unresolved ANN payload as the manifest loop builds one.
    fn payload(
        file_name: &str,
        gim: GlobalIndexMeta,
        external_path: Option<&str>,
    ) -> UnresolvedAnnSegment {
        UnresolvedAnnSegment {
            source_meta: PrimaryKeyIndexSourceMeta::from_global_index_meta(&gim).unwrap(),
            file_name: file_name.to_string(),
            external_path: external_path.map(str::to_string),
            file_size: 10,
            index_meta: gim.index_meta.clone().unwrap_or_default(),
        }
    }

    #[test]
    fn drops_index_only_bucket_without_error() {
        // Payload for (part=[], bucket 0) but NO data split -> no split, no error.
        let entries = vec![(
            BinaryRow::new(0),
            0,
            payload("seg0", gim(2, 5, &[("d0", 3)]), None),
        )];
        let splits = plan_from_inputs(1, Vec::new(), entries, "memory:/t", false).unwrap();
        assert!(splits.is_empty());
    }

    /// One split whose bucket directory is not derivable from the table root, so a
    /// resolved segment path proves the split's own bucket path was used.
    fn bucket_split(bucket_path: &str) -> DataSplit {
        DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket_path.to_string())
            .with_total_buckets(1)
            .with_data_files(vec![dfm("d0", 3, 5, Some(1))])
            .build()
            .unwrap()
    }

    #[test]
    fn builds_one_split_per_bucket_with_data() {
        let entries = vec![(
            BinaryRow::new(0),
            0,
            payload("seg0", gim(2, 5, &[("d0", 3)]), None),
        )];
        let data = bucket_split("memory:/t/bucket-0");
        let splits = plan_from_inputs(1, vec![data], entries, "memory:/t", false).unwrap();
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].ann_segments.len(), 1);
        let seg = &splits[0].ann_segments[0];
        assert_eq!(seg.path, "memory:/t/index/seg0");
        assert_eq!(seg.file_size, 10);
        assert_eq!(seg.source_meta.resolve(0).unwrap(), ("d0".to_string(), 0));
        assert_eq!(splits[0].active_files.len(), 1); // d0 is COMPACT + level>0
        assert_eq!(splits[0].active_files[0].file_name, "d0");
    }

    #[test]
    fn ann_segment_resolves_into_the_split_bucket_directory() {
        // The reported failure: a table with `index-file-in-data-file-dir` keeps its
        // ANN segments beside the bucket's data files, and the search opened
        // `<table>/index/<name>` instead. The directory must come from the split, so
        // a bucket path that is not derivable from the table root still resolves.
        let entries = vec![(
            BinaryRow::new(0),
            0,
            payload("seg0", gim(2, 5, &[("d0", 3)]), None),
        )];
        let data = bucket_split("s3://elsewhere/t/pt=1/bucket-0");
        let splits = plan_from_inputs(1, vec![data], entries, "memory:/t", true).unwrap();
        assert_eq!(
            splits[0].ann_segments[0].path,
            "s3://elsewhere/t/pt=1/bucket-0/seg0"
        );
    }

    #[test]
    fn ann_segment_external_path_wins_over_both_layouts() {
        for index_file_in_data_file_dir in [false, true] {
            let entries = vec![(
                BinaryRow::new(0),
                0,
                payload("seg0", gim(2, 5, &[("d0", 3)]), Some("s3://other/ann/seg0")),
            )];
            let data = bucket_split("memory:/t/bucket-0");
            let splits = plan_from_inputs(
                1,
                vec![data],
                entries,
                "memory:/t",
                index_file_in_data_file_dir,
            )
            .unwrap();
            assert_eq!(splits[0].ann_segments[0].path, "s3://other/ann/seg0");
        }
    }

    #[test]
    fn current_segments_require_exact_level_source_set() {
        let active = vec![
            dfm("b", 2, 5, Some(1)),
            dfm("a", 1, 5, Some(1)),
            dfm("c", 3, 6, Some(1)),
        ];
        let current = current_ann_segments(
            &active,
            vec![
                // Matches level 5 after active files are sorted by file name.
                ann_segment(5, "current-l5", &[("a", 1), ("b", 2)]),
                // Wrong level for the same source files -> stale.
                ann_segment(4, "wrong-level", &[("a", 1), ("b", 2)]),
                // Incomplete level 6 coverage -> stale.
                ann_segment(6, "partial-l6", &[("c", 2)]),
            ],
        )
        .unwrap();
        assert_eq!(current.len(), 1);
        assert_eq!(current[0].path, "current-l5");
    }

    #[test]
    fn current_segments_drop_all_when_level_has_multiple_matches() {
        let active = vec![dfm("a", 1, 5, Some(1))];
        let current = current_ann_segments(
            &active,
            vec![
                ann_segment(5, "first", &[("a", 1)]),
                ann_segment(5, "second", &[("a", 1)]),
            ],
        )
        .unwrap();
        assert!(current.is_empty());
    }

    #[test]
    fn rejects_data_split_with_wrong_snapshot() {
        let data = DataSplitBuilder::new()
            .with_snapshot(2)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("memory:/t/bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(vec![dfm("d0", 3, 5, Some(1))])
            .build()
            .unwrap();
        assert!(plan_from_inputs(1, vec![data], Vec::new(), "memory:/t", false).is_err());
    }

    #[test]
    fn accumulator_rejects_duplicate_file_name() {
        // Two splits in the SAME (partition, bucket) carrying a data file with the
        // same name must fail loud via the accumulator's duplicate-file guard.
        let split_a = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("memory:/t/bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(vec![dfm("dup", 3, 5, Some(1))])
            .build()
            .unwrap();
        let split_b = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("memory:/t/bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(vec![dfm("dup", 3, 5, Some(1))])
            .build()
            .unwrap();
        assert!(
            plan_from_inputs(1, vec![split_a, split_b], Vec::new(), "memory:/t", false).is_err()
        );
    }

    #[test]
    fn accumulator_keeps_deletion_files_in_parallel_order() {
        // One split, two data files; only the second carries a deletion file. The
        // built split must preserve the [None, Some] alignment parallel to
        // data_files.
        let dv = DeletionFile::new("dv".to_string(), 0, 1, Some(1));
        let data = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("memory:/t/bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(vec![dfm("d0", 3, 5, Some(1)), dfm("d1", 3, 5, Some(1))])
            .with_data_deletion_files(vec![None, Some(dv)])
            .build()
            .unwrap();
        let splits = plan_from_inputs(1, vec![data], Vec::new(), "memory:/t", false).unwrap();
        assert_eq!(splits.len(), 1);
        let dvs = splits[0]
            .data_split
            .data_deletion_files()
            .expect("deletion files preserved");
        assert_eq!(dvs.len(), 2);
        assert!(dvs[0].is_none());
        assert!(dvs[1].is_some());
        // Both files are COMPACT + level>0, so both appear as active files.
        assert_eq!(splits[0].active_files.len(), 2);
    }

    // ---- Real-table planning tests for filter push-down ----
    //
    // Gated off Windows: these fixtures build a table at a `file://` URL derived
    // from a temp dir path, which `FileIO` cannot resolve on Windows (see #397).
    #[cfg(not(windows))]
    mod prune_pushdown_tests {
        use super::*;
        use crate::catalog::Identifier;
        use crate::io::{FileIO, FileIOBuilder};
        use crate::spec::stats::compute_column_stats;
        use crate::spec::{
            DataType, Datum, FloatType, IntType, PredicateBuilder, Schema, TableSchema, VectorType,
        };
        use crate::table::{CommitMessage, SchemaManager, Table, TableCommit, TableWrite};
        use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
        use arrow_array::{ArrayRef, Int32Array, RecordBatch};
        use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
        use std::sync::Arc;

        /// Vector dimension for the pruning fixtures.
        const PRUNE_DIM: usize = 4;
        /// The primary-key vector column name.
        const PRUNE_VECTOR_COLUMN: &str = "embedding";
        /// vindex index type string; only used to route `PkVectorScan::new`, no index
        /// segment is built for these tests.
        const PRUNE_INDEX_TYPE: &str = "ivf-flat";
        /// Number of rows written; `id`/`score` values live in `0..PRUNE_ROWS`.
        const PRUNE_ROWS: i32 = 4;
        /// A predicate literal guaranteed to fall outside the written `id`/`score`
        /// range, so file stats cannot match it.
        const OUT_OF_RANGE: i32 = 1_000_000;

        /// Schema `(id INT PRIMARY KEY, score INT, embedding VECTOR<FLOAT>)`. When
        /// `with_deletion_vectors`, enable deletion vectors (merge-on-read left at the
        /// default `false`) so a non-PK scalar predicate also stats-prunes; otherwise a
        /// plain PK table where only PK-column conjuncts prune.
        fn prune_schema(with_deletion_vectors: bool) -> TableSchema {
            let mut builder = Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("score", DataType::Int(IntType::new()))
                .column(
                    PRUNE_VECTOR_COLUMN,
                    DataType::Vector(
                        VectorType::try_new(
                            true,
                            PRUNE_DIM as u32,
                            DataType::Float(FloatType::new()),
                        )
                        .unwrap(),
                    ),
                )
                .primary_key(["id"])
                .option("bucket".to_string(), "1".to_string());
            if with_deletion_vectors {
                builder =
                    builder.option("deletion-vectors.enabled".to_string(), "true".to_string());
            }
            TableSchema::new(0, &builder.build().unwrap())
        }

        /// Arrow batch matching the schema: `id` and `score` both equal the physical
        /// position (`0..n`), plus a `FixedSizeList<Float32>` vector column.
        fn prune_data_batch(n: usize) -> RecordBatch {
            let ids: Vec<i32> = (0..n as i32).collect();
            let scores: Vec<i32> = (0..n as i32).collect();

            let element_field = Arc::new(ArrowField::new("element", ArrowDataType::Float32, true));
            let mut vector_builder =
                FixedSizeListBuilder::new(Float32Builder::new(), PRUNE_DIM as i32)
                    .with_field(element_field.clone());
            for i in 0..n {
                for d in 0..PRUNE_DIM {
                    vector_builder.values().append_value((i + d) as f32);
                }
                vector_builder.append(true);
            }

            let schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new("id", ArrowDataType::Int32, false),
                ArrowField::new("score", ArrowDataType::Int32, false),
                ArrowField::new(
                    PRUNE_VECTOR_COLUMN,
                    ArrowDataType::FixedSizeList(element_field, PRUNE_DIM as i32),
                    true,
                ),
            ]));
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int32Array::from(ids)) as ArrayRef,
                    Arc::new(Int32Array::from(scores)) as ArrayRef,
                    Arc::new(vector_builder.finish()) as ArrayRef,
                ],
            )
            .unwrap()
        }

        async fn prune_open_table(file_io: &FileIO, location: &str) -> Table {
            let schema = SchemaManager::new(file_io.clone(), location.to_string())
                .latest()
                .await
                .expect("failed to list schemas")
                .expect("table has no schema");
            Table::new(
                file_io.clone(),
                Identifier::new("default", "pkvector_prune"),
                location.to_string(),
                (*schema).clone(),
                None,
            )
        }

        /// Build a real single-file primary-key table via the public write path.
        ///
        /// The Rust key-value writer records primary-key stats in `key_stats`. For the
        /// deletion-vector test, also populate `value_stats` so its non-key predicate
        /// has the same metadata a Java primary-key writer produces.
        async fn build_pruning_test_table(
            with_deletion_vectors: bool,
        ) -> (tempfile::TempDir, Table) {
            let tmp = tempfile::tempdir().expect("create temp dir");
            let location = format!("file://{}", tmp.path().display());
            let file_io = FileIOBuilder::new("file").build().unwrap();

            for dir in ["schema", "snapshot", "manifest", "index"] {
                file_io.mkdirs(&format!("{location}/{dir}")).await.unwrap();
            }
            let schema = prune_schema(with_deletion_vectors);
            file_io
                .new_output(&format!("{location}/schema/schema-{}", schema.id()))
                .unwrap()
                .write(bytes::Bytes::from(serde_json::to_vec(&schema).unwrap()))
                .await
                .unwrap();

            let table = prune_open_table(&file_io, &location).await;

            let batch = prune_data_batch(PRUNE_ROWS as usize);
            let mut writer = TableWrite::new(&table, "pkvector-prune".to_string()).unwrap();
            writer.write_arrow_batch(&batch).await.unwrap();
            let messages = writer.prepare_commit().await.unwrap();
            assert_eq!(messages.len(), 1, "single bucket -> one write message");
            let written = &messages[0];
            assert_eq!(written.new_files.len(), 1, "single data file expected");
            let base_meta = written.new_files[0].clone();
            let bucket = written.bucket;
            let partition = written.partition.clone();
            assert_eq!(base_meta.key_stats.null_counts(), &vec![Some(0)]);
            assert!(base_meta.value_stats.null_counts().is_empty());
            assert_eq!(base_meta.value_stats_cols, Some(vec![]));

            let file_meta = if with_deletion_vectors {
                let int = DataType::Int(IntType::new());
                let value_stats: BinaryTableStats =
                    compute_column_stats(&batch, &[0, 1], &[int.clone(), int]).unwrap();
                DataFileMeta {
                    value_stats,
                    value_stats_cols: Some(vec!["id".to_string(), "score".to_string()]),
                    ..base_meta
                }
            } else {
                base_meta
            };

            let message = CommitMessage::new(partition, bucket, vec![file_meta]);
            TableCommit::new(table.clone(), "pkvector-prune".to_string())
                .commit(vec![message])
                .await
                .unwrap();

            (tmp, table)
        }

        fn prune_vector_field_id(table: &Table) -> i32 {
            table
                .schema()
                .fields()
                .iter()
                .find(|f| f.name() == PRUNE_VECTOR_COLUMN)
                .expect("vector field present")
                .id()
        }

        fn prune_equal(table: &Table, column: &str, value: i32) -> Predicate {
            PredicateBuilder::new(table.schema().fields())
                .equal(column, Datum::Int(value))
                .unwrap()
        }

        #[tokio::test]
        async fn plan_prunes_rust_pk_file_using_key_stats() {
            // Real PK table, one data file with id in [0, PRUNE_ROWS). A predicate
            // `id = OUT_OF_RANGE` cannot match the file's id stats, so the scan drops
            // the file and plan() returns no splits. Control (no filter) returns one.
            let (_tmp, table) = build_pruning_test_table(false).await;
            let field_id = prune_vector_field_id(&table);

            let unfiltered =
                PkVectorScan::new(&table, field_id, PRUNE_INDEX_TYPE.to_string(), None)
                    .plan()
                    .await
                    .unwrap();
            assert_eq!(
                unfiltered.splits.len(),
                1,
                "control: file present without a filter"
            );

            let out_of_range = prune_equal(&table, "id", OUT_OF_RANGE);
            let filtered = PkVectorScan::new(
                &table,
                field_id,
                PRUNE_INDEX_TYPE.to_string(),
                Some(out_of_range),
            )
            .plan()
            .await
            .unwrap();
            assert!(
                filtered.splits.is_empty(),
                "pk predicate stats-excludes the only file"
            );
        }

        #[tokio::test]
        async fn plan_prunes_file_on_non_pk_predicate_under_deletion_vectors() {
            // Under deletion vectors (merge-on-read off), a non-PK column's stats also
            // prune. A `score` predicate outside the written range drops the file.
            let (_tmp, table) = build_pruning_test_table(true).await;
            let field_id = prune_vector_field_id(&table);

            // Control: without a filter the file is present.
            let unfiltered =
                PkVectorScan::new(&table, field_id, PRUNE_INDEX_TYPE.to_string(), None)
                    .plan()
                    .await
                    .unwrap();
            assert_eq!(
                unfiltered.splits.len(),
                1,
                "control: file present without a filter"
            );

            let out_of_range = prune_equal(&table, "score", OUT_OF_RANGE);
            let filtered = PkVectorScan::new(
                &table,
                field_id,
                PRUNE_INDEX_TYPE.to_string(),
                Some(out_of_range),
            )
            .plan()
            .await
            .unwrap();
            assert!(
                filtered.splits.is_empty(),
                "non-pk predicate stats-excludes the file under deletion vectors"
            );
        }
    }

    // ---- planning from engine-supplied bucket splits ----

    const BUCKET_SPLIT_GOLDEN: &[u8] = include_bytes!("goldens/bucket_vector_search_split_v1.bin");

    /// The Java-planned fixture #771's read test uses: one bucket, no pre-filter,
    /// so `rangeFileCount == 0`. Provenance is in
    /// `tests/pk_vector_bucket_split_read_test.rs`.
    const BUCKET_SPLIT_NO_PREFILTER: &[u8] =
        include_bytes!("../../testdata/pkvector_split/bucket_split_0.bin");

    fn int_partition(value: i32) -> BinaryRow {
        let mut builder = crate::spec::BinaryRowBuilder::new(1);
        builder.write_int(0, value);
        BinaryRow::from_serialized_bytes(&builder.build_serialized()).unwrap()
    }

    /// A bucket split as an engine would hand one over. Its data files are COMPACT
    /// above level 0, so they are exact-fallback eligible, and the caller's payload
    /// metadata is expected to name exactly that level's source set.
    fn engine_split(
        snapshot: i64,
        bucket: i32,
        partition: BinaryRow,
        files: Vec<DataFileMeta>,
        payloads: Vec<BucketVectorPayload>,
        ranges: &[(&str, &[(i64, i64)])],
    ) -> BucketVectorSearchSplit {
        let data_split = DataSplitBuilder::new()
            .with_snapshot(snapshot)
            .with_partition(partition)
            .with_bucket(bucket)
            .with_bucket_path(format!("bucket-{bucket}"))
            .with_total_buckets(1)
            .with_data_files(files)
            .build()
            .unwrap();
        BucketVectorSearchSplit::new_for_test(
            data_split,
            payloads,
            ranges
                .iter()
                .map(|(name, bounds)| {
                    (
                        (*name).to_string(),
                        bounds
                            .iter()
                            .map(|(from, to)| RowRange::new(*from, *to))
                            .collect(),
                    )
                })
                .collect(),
        )
    }

    fn engine_payload(meta: GlobalIndexMeta) -> BucketVectorPayload {
        BucketVectorPayload::new_for_test("ivf-pq", "seg0", 1, 4, None, None, meta)
    }

    /// One bucket holding `d0`, with a payload whose source set matches it.
    fn one_file_split(
        snapshot: i64,
        bucket: i32,
        ranges: &[(&str, &[(i64, i64)])],
    ) -> BucketVectorSearchSplit {
        engine_split(
            snapshot,
            bucket,
            BinaryRow::new(0),
            vec![dfm("d0", 4, 5, Some(1))],
            vec![engine_payload(gim(2, 5, &[("d0", 4)]))],
            ranges,
        )
    }

    /// The positions a file's normalized ranges allow, for assertions that read
    /// better as a row list than as ranges. Test-only: the production path never
    /// expands a range into positions.
    fn allowed(map: &HashMap<String, Vec<RowRange>>, file: &str) -> Vec<i64> {
        map.get(file)
            .map(|ranges| {
                ranges
                    .iter()
                    .flat_map(|range| range.from()..=range.to())
                    .collect()
            })
            .unwrap_or_default()
    }

    #[test]
    fn plans_the_java_golden_bucket_split() {
        let split = BucketVectorSearchSplit::deserialize(BUCKET_SPLIT_GOLDEN).unwrap();
        let plan = plan_from_bucket_splits("ivf-pq", 7, None, "/tbl", false, vec![split]).unwrap();

        // The snapshot the split pins, not one re-resolved from the table.
        assert_eq!(plan.snapshot_id, 11);
        assert_eq!(plan.splits.len(), 1);
        let planned = &plan.splits[0];

        // The payload's own external path wins over both directory layouts.
        assert_eq!(planned.ann_segments.len(), 1);
        assert_eq!(planned.ann_segments[0].path, "s3://vector-bucket/ann-0.idx");
        assert_eq!(planned.ann_segments[0].file_size, 5_000_000_000);
        assert_eq!(planned.ann_segments[0].source_meta.data_level(), 1);

        // `data-1.orc` is COMPACT above level 0, so exact fallback may read it.
        assert_eq!(planned.active_files.len(), 1);
        assert_eq!(planned.active_files[0].file_name, "data-1.orc");

        // The message allows rows 0-1 and 4-5 of a six-row file.
        let ranges = plan
            .physical_row_ranges_by_split
            .expect("a split-driven plan restricts positions");
        assert_eq!(allowed(&ranges[0], "data-1.orc"), vec![0, 1, 4, 5]);
    }

    #[test]
    fn an_unlisted_file_is_left_out_rather_than_expanded_from_its_row_count() {
        // The row count comes off the wire. Turning an unlisted file into an explicit
        // [0, row_count - 1] hands that number to the allow-list materialization, and
        // i64::MAX does not come back. Java never writes the entry: absence already
        // means "every row", so nothing has to be sized from the count.
        let split = engine_split(
            11,
            0,
            BinaryRow::new(0),
            vec![dfm("d0", i64::MAX, 5, Some(1))],
            vec![engine_payload(gim(2, 5, &[("d0", i64::MAX)]))],
            &[],
        );
        let plan = plan_from_bucket_splits("ivf-pq", 2, None, "/tbl", false, vec![split]).unwrap();
        let selections = plan
            .physical_row_ranges_by_split
            .expect("split-driven plan");
        assert!(
            !selections[0].contains_key("d0"),
            "an unlisted file must stay absent (unrestricted), not be expanded"
        );
    }

    #[test]
    fn the_java_fixture_lists_no_ranges_so_the_plan_restricts_nothing() {
        // The committed Java-planned fixture is the no-pre-filter shape an engine
        // ships by default. Nothing in it is restricted, so the plan must carry no
        // per-file entry at all -- that absence is what lets the search skip the
        // filtered ANN path.
        let split = BucketVectorSearchSplit::deserialize(BUCKET_SPLIT_NO_PREFILTER).unwrap();
        assert!(
            split.row_ranges_by_file().is_empty(),
            "fixture provenance: Java recorded no pre-filter ranges"
        );
        // Derived from the fixture itself so the test cannot drift from it.
        let payload = &split.payload_files()[0];
        let index_type = payload.index_type().to_string();
        let field_id = payload.global_index_meta().index_field_id;

        let plan = plan_from_bucket_splits(&index_type, field_id, None, "/tbl", false, vec![split])
            .unwrap();
        let selections = plan
            .physical_row_ranges_by_split
            .expect("split-driven plan");
        assert_eq!(selections.len(), 1);
        assert!(
            selections[0].is_empty(),
            "a split that lists no ranges restricts no file"
        );
    }

    #[test]
    fn the_java_fixture_reaches_the_ann_layer_with_no_mask_at_all() {
        // The chain the two tests above only cover in halves: the committed
        // Java-planned fixture, through planning, into the function that decides
        // whether the ANN backend is handed an `include_row_ids` filter. `None` here
        // is what makes Lumina call `search` rather than `search_with_filter`.
        // Result equality against the manifest route cannot see this difference --
        // both return the same rows for this fixture.
        let split = BucketVectorSearchSplit::deserialize(BUCKET_SPLIT_NO_PREFILTER).unwrap();
        let payload = &split.payload_files()[0];
        let index_type = payload.index_type().to_string();
        let field_id = payload.global_index_meta().index_field_id;
        let source_meta =
            PrimaryKeyIndexSourceMeta::from_global_index_meta(payload.global_index_meta()).unwrap();

        let plan = plan_from_bucket_splits(&index_type, field_id, None, "/tbl", false, vec![split])
            .unwrap();
        let selections: crate::vindex::pkvector::FileRowSelections = plan
            .physical_row_ranges_by_split
            .expect("split-driven plan")
            .remove(0)
            .into_iter()
            .map(|(file, ranges)| {
                (
                    file,
                    crate::vindex::pkvector::FileRowSelection::Ranges(ranges),
                )
            })
            .collect();

        let active: HashSet<String> = source_meta
            .source_files()
            .iter()
            .map(|file| file.file_name().to_string())
            .collect();
        assert!(
            crate::vindex::pkvector::ann::build_live_row_ids(
                source_meta.source_files(),
                &active,
                &HashMap::new(),
                Some(&selections),
            )
            .unwrap()
            .is_none(),
            "the no-pre-filter fixture must not put the backend on the filtered path"
        );
    }

    #[test]
    fn rejects_empty_bucket_split_input() {
        let error = plan_from_bucket_splits("ivf-pq", 2, None, "/tbl", false, Vec::new())
            .map(|_| ())
            .expect_err("empty input pins no snapshot to report");
        assert!(
            error.to_string().contains("at least one bucket split"),
            "{error}"
        );
    }

    #[test]
    fn rejects_bucket_splits_pinning_different_snapshots() {
        let error = plan_from_bucket_splits(
            "ivf-pq",
            2,
            None,
            "/tbl",
            false,
            vec![one_file_split(11, 0, &[]), one_file_split(12, 1, &[])],
        )
        .map(|_| ())
        .expect_err("candidates from two snapshots cannot merge into one Top-K");
        assert!(
            error.to_string().contains("pin different snapshots"),
            "{error}"
        );
    }

    #[test]
    fn rejects_two_splits_for_one_bucket() {
        let error = plan_from_bucket_splits(
            "ivf-pq",
            2,
            None,
            "/tbl",
            false,
            vec![one_file_split(11, 0, &[]), one_file_split(11, 0, &[])],
        )
        .map(|_| ())
        .expect_err("one bucket twice would search its rows twice");
        assert!(error.to_string().contains("repeat bucket 0"), "{error}");
    }

    #[test]
    fn rejects_nested_data_split_carrying_row_ranges() {
        let data_split = DataSplitBuilder::new()
            .with_snapshot(11)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(vec![dfm("d0", 4, 5, Some(1))])
            .with_row_ranges(vec![RowRange::new(0, 1)])
            .build()
            .unwrap();
        let split = BucketVectorSearchSplit::new_for_test(
            data_split,
            vec![engine_payload(gim(2, 5, &[("d0", 4)]))],
            IndexMap::new(),
        );
        let error = plan_from_bucket_splits("ivf-pq", 2, None, "/tbl", false, vec![split])
            .map(|_| ())
            .expect_err("two row-range authorities may disagree");
        assert!(
            error.to_string().contains("must not carry row ranges"),
            "{error}"
        );
    }

    #[test]
    fn unlisted_file_is_unrestricted_and_an_empty_list_excludes_one() {
        // Java records ranges only for the files its own pre-filter narrowed, so an
        // omitted file means "all rows". An explicitly empty list means "no rows",
        // and the two must not collapse into each other.
        let split = engine_split(
            11,
            0,
            BinaryRow::new(0),
            vec![dfm("d0", 4, 5, Some(1)), dfm("d1", 3, 5, Some(1))],
            vec![engine_payload(gim(2, 5, &[("d0", 4), ("d1", 3)]))],
            &[("d0", &[])],
        );
        let plan = plan_from_bucket_splits("ivf-pq", 2, None, "/tbl", false, vec![split]).unwrap();
        let ranges = plan
            .physical_row_ranges_by_split
            .expect("split-driven plan");

        // Listed empty: present and excluded. Unlisted: absent, meaning every row.
        assert_eq!(ranges[0].get("d0").map(Vec::as_slice), Some(&[][..]));
        assert!(!ranges[0].contains_key("d1"));
    }

    #[test]
    fn rejects_negative_payload_file_size() {
        let split = engine_split(
            11,
            0,
            BinaryRow::new(0),
            vec![dfm("d0", 4, 5, Some(1))],
            vec![BucketVectorPayload::new_for_test(
                "ivf-pq",
                "seg0",
                -1,
                4,
                None,
                None,
                gim(2, 5, &[("d0", 4)]),
            )],
            &[],
        );
        let error = plan_from_bucket_splits("ivf-pq", 2, None, "/tbl", false, vec![split])
            .map(|_| ())
            .expect_err("a signed wire size can be negative, segment addressing cannot");
        assert!(
            error.to_string().contains("must not be negative"),
            "{error}"
        );
    }

    #[test]
    fn ignores_payload_deletion_vector_ranges() {
        // The field belongs to deletion-vector index files. Java builds a vector
        // payload through the overload that leaves it null, and a read takes its
        // deletion vectors from the bucket's data split, so a value here describes
        // something this payload is not.
        let mut dv = IndexMap::new();
        dv.insert(
            "d0".to_string(),
            DeletionVectorMeta {
                offset: 0,
                length: 8,
                cardinality: Some(1),
            },
        );
        let split = engine_split(
            11,
            0,
            BinaryRow::new(0),
            vec![dfm("d0", 4, 5, Some(1))],
            vec![BucketVectorPayload::new_for_test(
                "ivf-pq",
                "seg0",
                1,
                4,
                Some(dv),
                None,
                gim(2, 5, &[("d0", 4)]),
            )],
            &[],
        );
        let plan = plan_from_bucket_splits("ivf-pq", 2, None, "/tbl", false, vec![split]).unwrap();
        assert_eq!(plan.splits.len(), 1);
        assert_eq!(plan.splits[0].ann_segments.len(), 1);
        let ranges = plan
            .physical_row_ranges_by_split
            .expect("split-driven plan");
        // Unaffected: the file stays unlisted, which is "every row".
        assert!(!ranges[0].contains_key("d0"));
    }

    #[test]
    fn skips_payloads_for_another_column_or_index_type() {
        let split = engine_split(
            11,
            0,
            BinaryRow::new(0),
            vec![dfm("d0", 4, 5, Some(1))],
            vec![
                // Another column's vector index.
                engine_payload(gim(99, 5, &[("d0", 4)])),
                // This column, but another index type.
                BucketVectorPayload::new_for_test(
                    "flat",
                    "seg1",
                    1,
                    4,
                    None,
                    None,
                    gim(2, 5, &[("d0", 4)]),
                ),
            ],
            &[],
        );
        let plan = plan_from_bucket_splits("ivf-pq", 2, None, "/tbl", false, vec![split]).unwrap();
        assert_eq!(plan.splits.len(), 1);
        assert!(plan.splits[0].ann_segments.is_empty());
        // Still exact-fallback eligible: no ANN segment covers the file.
        assert_eq!(plan.splits[0].active_files.len(), 1);
    }

    fn partition_filter_on_dt(keep: i32) -> PartitionFilter {
        let fields = vec![DataField::new(
            0,
            "dt".to_string(),
            crate::spec::DataType::Int(crate::spec::IntType::new()),
        )];
        let builder = crate::spec::PredicateBuilder::new(&fields);
        let predicate = builder.equal("dt", crate::spec::Datum::Int(keep)).unwrap();
        PartitionFilter::from_predicate(predicate, &fields)
    }

    #[test]
    fn snapshot_mismatch_is_rejected_before_partition_pruning() {
        // Both splits are pruned by this filter. The mismatch must still be reported:
        // pruning first would hide an inconsistent input behind an empty plan.
        let filter = partition_filter_on_dt(3);
        let error = plan_from_bucket_splits(
            "ivf-pq",
            2,
            Some(&filter),
            "/tbl",
            false,
            vec![
                engine_split(
                    11,
                    0,
                    int_partition(1),
                    vec![dfm("d0", 4, 5, Some(1))],
                    vec![engine_payload(gim(2, 5, &[("d0", 4)]))],
                    &[],
                ),
                engine_split(
                    12,
                    1,
                    int_partition(2),
                    vec![dfm("d0", 4, 5, Some(1))],
                    vec![engine_payload(gim(2, 5, &[("d0", 4)]))],
                    &[],
                ),
            ],
        )
        .map(|_| ())
        .expect_err("a snapshot mismatch outranks pruning");
        assert!(
            error.to_string().contains("pin different snapshots"),
            "{error}"
        );
    }

    #[test]
    fn pruning_every_split_keeps_the_pinned_snapshot() {
        let filter = partition_filter_on_dt(3);
        let plan = plan_from_bucket_splits(
            "ivf-pq",
            2,
            Some(&filter),
            "/tbl",
            false,
            vec![engine_split(
                11,
                0,
                int_partition(1),
                vec![dfm("d0", 4, 5, Some(1))],
                vec![engine_payload(gim(2, 5, &[("d0", 4)]))],
                &[],
            )],
        )
        .unwrap();
        assert!(plan.splits.is_empty());
        // Still authoritative with nothing left to search.
        assert_eq!(plan.snapshot_id, 11);
        assert_eq!(
            plan.physical_row_ranges_by_split.as_deref(),
            Some([].as_slice())
        );
    }

    #[test]
    fn resolves_a_payload_without_an_external_path_into_the_bucket_directory() {
        let split = one_file_split(11, 0, &[]);
        let plan = plan_from_bucket_splits("ivf-pq", 2, None, "/tbl", true, vec![split]).unwrap();
        assert_eq!(plan.splits[0].ann_segments[0].path, "bucket-0/seg0");

        let split = one_file_split(11, 0, &[]);
        let plan = plan_from_bucket_splits("ivf-pq", 2, None, "/tbl", false, vec![split]).unwrap();
        assert_eq!(plan.splits[0].ann_segments[0].path, "/tbl/index/seg0");
    }
}
