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
    BinaryRow, DataFileMeta, FileKind, GlobalIndexMeta, IndexManifest, PkVectorSourceFile,
    PkVectorSourceMeta,
};
use crate::table::pk_vector_orchestrator::PkVectorSearchSplit;
use crate::table::source::{DataSplit, DataSplitBuilder, DeletionFile};
use crate::table::Table;
use crate::vindex::pkvector::bucket::{BucketActiveFile, BucketAnnSegment};

const INDEX_DIR: &str = "index";
const FILE_SOURCE_COMPACT: i32 = 1;

fn data_invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

/// Mirror of `PrimaryKeyIndexSourcePolicy.shouldRead`: only compacted, non-level-0
/// files back the PK-vector index; an absent file source reads as false.
fn should_read_pk_index_source(file: &DataFileMeta) -> bool {
    matches!(file.file_source, Some(src) if src == FILE_SOURCE_COMPACT) && file.level > 0
}

fn source_files_unique(files: &[PkVectorSourceFile]) -> bool {
    let mut seen = HashSet::new();
    files.iter().all(|file| seen.insert(file.file_name()))
}

fn current_ann_segments(
    active_data_files: &[DataFileMeta],
    ann_segments: Vec<BucketAnnSegment>,
) -> crate::Result<Vec<BucketAnnSegment>> {
    let mut sources_by_level: BTreeMap<i32, Vec<PkVectorSourceFile>> = BTreeMap::new();
    for file in active_data_files {
        if should_read_pk_index_source(file) {
            sources_by_level
                .entry(file.level)
                .or_default()
                .push(PkVectorSourceFile::new(
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
    pub splits: Vec<PkVectorSearchSplit>,
}

pub(crate) struct PkVectorScan<'a> {
    table: &'a Table,
    vector_field_id: i32,
    index_type: String,
}

impl<'a> PkVectorScan<'a> {
    pub(crate) fn new(table: &'a Table, vector_field_id: i32, index_type: String) -> Self {
        Self {
            table,
            vector_field_id,
            index_type,
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
        let data_splits = self
            .table
            .new_read_builder()
            .new_scan()
            .with_scan_all_files()
            .plan()
            .await?
            .splits()
            .to_vec();

        // No data files -> nothing to search.
        let Some(first_split) = data_splits.first() else {
            return Ok(PkVectorScanPlan { splits: Vec::new() });
        };
        let snapshot_id = first_split.snapshot_id();
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
        Ok(PkVectorScanPlan { splits })
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
        let source_meta = PkVectorSourceMeta::from_global_index_meta(&gim)
            .map_err(|_| data_invalid(format!("index file {file_name} is not active")))?;
        let key = (partition.to_serialized_bytes(), bucket);
        segments_by_bucket
            .entry(key)
            .or_default()
            .push(BucketAnnSegment {
                source_meta,
                path,
                file_size,
                // Not consumed on the search path: the vindex reader loads its
                // metadata from the index file bytes and ignores this field, so an
                // absent value defaulting to an empty vec is acceptable.
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

    /// Build a `_SOURCE_META` blob the way `PkVectorSourceMeta::deserialize`
    /// expects it. There is no public serializer, so we mirror the frame used by
    /// `pk_vector_source.rs`'s own round-trip tests.
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
            source_meta: PkVectorSourceMeta::new(
                data_level,
                source_files
                    .iter()
                    .map(|(name, rows)| {
                        PkVectorSourceFile::new((*name).to_string(), *rows).unwrap()
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
}
