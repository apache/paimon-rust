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

use std::collections::{BTreeMap, HashSet};

use crate::spec::{
    should_read_pk_index_source, BinaryRow, DataFileMeta, FileKind, GlobalIndexMeta, IndexManifest,
    Predicate, PrimaryKeyIndexSourceFile, PrimaryKeyIndexSourceMeta,
};
use crate::table::pk_vector_orchestrator::PkVectorSearchSplit;
use crate::table::source::{DataSplit, DataSplitBuilder, DeletionFile};
use crate::table::Table;
use crate::vindex::pkvector::bucket::{BucketActiveFile, BucketAnnSegment};

const INDEX_DIR: &str = "index";

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
                let resolved_path =
                    format!("{table_path}/{INDEX_DIR}/{}", entry.index_file.file_name);
                let file_size = u64::try_from(entry.index_file.file_size)
                    .map_err(|_| data_invalid("index file size must not be negative"))?;
                entries.push((
                    partition,
                    entry.bucket,
                    gim,
                    resolved_path,
                    file_size,
                    entry.index_file.file_name.clone(),
                ));
            }
        }

        let splits = plan_from_inputs(snapshot_id, data_splits, entries)?;
        Ok(PkVectorScanPlan {
            snapshot_id,
            splits,
        })
    }
}

/// Pure planning core, drivable without a live snapshot: group ANN payloads and
/// data splits by `(partition, bucket)`, then assemble one search split per
/// bucket that has data. Index-only buckets are dropped, not errored.
#[allow(clippy::type_complexity)]
fn plan_from_inputs(
    snapshot_id: i64,
    data_splits: Vec<DataSplit>,
    index_entries: Vec<(BinaryRow, i32, GlobalIndexMeta, String, u64, String)>,
) -> crate::Result<Vec<PkVectorSearchSplit>> {
    type Key = (Vec<u8>, i32);

    // Phase A: group ANN payloads by (partition, bucket).
    let mut segments_by_bucket: BTreeMap<Key, Vec<BucketAnnSegment>> = BTreeMap::new();
    for (partition, bucket, gim, path, file_size, file_name) in index_entries {
        let source_meta = PrimaryKeyIndexSourceMeta::from_global_index_meta(&gim)
            .map_err(|_| data_invalid(format!("index file {file_name} is not active")))?;
        let key = (partition.to_serialized_bytes(), bucket);
        segments_by_bucket
            .entry(key)
            .or_default()
            .push(BucketAnnSegment {
                source_meta,
                path,
                file_size,
                // The Lumina reader consumes this as its serialized index
                // metadata; the vindex reader ignores it and loads metadata from
                // the segment file bytes. Absent value defaults to an empty vec.
                index_meta: gim.index_meta.clone().unwrap_or_default(),
            });
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

    // Phase C: assemble one split per bucket that has data.
    let mut out = Vec::new();
    for (key, acc) in accum_by_bucket {
        let data_split = acc.build()?;
        let ann_segments = current_ann_segments(
            data_split.data_files(),
            segments_by_bucket.remove(&key).unwrap_or_default(),
        )?;
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
    // Index-only buckets left in segments_by_bucket are intentionally dropped.
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{BinaryRow, DataFileMeta, GlobalIndexMeta};
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

    #[test]
    fn drops_index_only_bucket_without_error() {
        // Payload for (part=[], bucket 0) but NO data split -> no split, no error.
        let entries = vec![(
            BinaryRow::new(0),
            0,
            gim(2, 5, &[("d0", 3)]),
            "idx/seg0".to_string(),
            10u64,
            "seg0".to_string(),
        )];
        let splits = plan_from_inputs(1, Vec::new(), entries).unwrap();
        assert!(splits.is_empty());
    }

    #[test]
    fn builds_one_split_per_bucket_with_data() {
        let entries = vec![(
            BinaryRow::new(0),
            0,
            gim(2, 5, &[("d0", 3)]),
            "idx/seg0".to_string(),
            10u64,
            "seg0".to_string(),
        )];
        let data = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("memory:/t/bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(vec![dfm("d0", 3, 5, Some(1))])
            .build()
            .unwrap();
        let splits = plan_from_inputs(1, vec![data], entries).unwrap();
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].ann_segments.len(), 1);
        let seg = &splits[0].ann_segments[0];
        assert_eq!(seg.path, "idx/seg0");
        assert_eq!(seg.file_size, 10);
        assert_eq!(seg.source_meta.resolve(0).unwrap(), ("d0".to_string(), 0));
        assert_eq!(splits[0].active_files.len(), 1); // d0 is COMPACT + level>0
        assert_eq!(splits[0].active_files[0].file_name, "d0");
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
        assert!(plan_from_inputs(1, vec![data], Vec::new()).is_err());
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
        assert!(plan_from_inputs(1, vec![split_a, split_b], Vec::new()).is_err());
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
        let splits = plan_from_inputs(1, vec![data], Vec::new()).unwrap();
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
}
