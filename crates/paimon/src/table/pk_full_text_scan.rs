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

//! Primary-key full-text search planning: resolve a snapshot, plan its data
//! splits, scan the index manifest for this column's full-text payloads, and
//! accumulate one search split per bucket. Mirror of Java
//! `PrimaryKeyFullTextScan` and `PrimaryKeyFullTextSearchSplit`.

use std::collections::{BTreeMap, HashSet};

use crate::spec::{
    should_read_pk_index_source, BinaryRow, DataFileMeta, FileKind, IndexFileMeta, IndexManifest,
    Predicate, PrimaryKeyIndexSourceMeta,
};
use crate::table::pk_full_text_bucket_state::{PkFullTextBucketState, PK_FULL_TEXT_INDEX_TYPE};
use crate::table::source::{DataSplit, DataSplitBuilder, DeletionFile};
use crate::table::Table;

fn data_invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

/// Compaction-visible data files and full-text payloads for one snapshot bucket.
///
/// Mirror of Java `PrimaryKeyFullTextSearchSplit`. The constructor enforces the
/// bucket-split invariants (unique data files; each payload covers ≥1 active
/// source; no active file double-covered; uncovered files active/non-duplicate/
/// not-covered; and covered ∪ uncovered == every active data file in the split).
pub(crate) struct PrimaryKeyFullTextSearchSplit {
    pub data_split: DataSplit,
    pub current_payloads: Vec<IndexFileMeta>,
    // Carried for parity with Java `PrimaryKeyFullTextSearchSplit` (which preserves
    // the uncovered files) and validated by the split constructor's completeness
    // invariant. FAST mode never searches these files, so nothing reads the field
    // after construction.
    #[allow(dead_code)]
    pub uncovered_data_files: Vec<String>,
}

impl PrimaryKeyFullTextSearchSplit {
    pub(crate) fn new(
        data_split: DataSplit,
        current_payloads: Vec<IndexFileMeta>,
        uncovered_data_files: Vec<String>,
    ) -> crate::Result<Self> {
        // Unique data files in the split.
        let mut source_files: HashSet<String> = HashSet::new();
        for data_file in data_split.data_files() {
            if !source_files.insert(data_file.file_name.clone()) {
                return Err(data_invalid(format!(
                    "data file {} appears more than once in a full-text bucket split",
                    data_file.file_name
                )));
            }
        }

        // Each current payload must cover ≥1 active source, and no active source
        // may be covered by more than one payload.
        let mut covered: HashSet<String> = HashSet::new();
        for payload in &current_payloads {
            let gim = payload.global_index_meta.as_ref().ok_or_else(|| {
                data_invalid(format!(
                    "full-text payload {} has no global index meta",
                    payload.file_name
                ))
            })?;
            let source_meta = PrimaryKeyIndexSourceMeta::from_global_index_meta(gim)?;
            let mut covers_active_source = false;
            for source in source_meta.source_files() {
                if !source_files.contains(source.file_name()) {
                    continue;
                }
                covers_active_source = true;
                if !covered.insert(source.file_name().to_string()) {
                    return Err(data_invalid(format!(
                        "data file {} is covered by more than one full-text payload",
                        source.file_name()
                    )));
                }
            }
            if !covers_active_source {
                return Err(data_invalid(format!(
                    "full-text payload {} does not cover an active data file in its bucket split",
                    payload.file_name
                )));
            }
        }

        // Uncovered files must be active, non-duplicate, and not covered.
        let mut uncovered: HashSet<String> = HashSet::new();
        for source in &uncovered_data_files {
            if !source_files.contains(source) {
                return Err(data_invalid(format!(
                    "uncovered full-text data file {source} is outside its bucket split"
                )));
            }
            if !uncovered.insert(source.clone()) {
                return Err(data_invalid(format!(
                    "uncovered full-text data file {source} appears more than once"
                )));
            }
            if covered.contains(source) {
                return Err(data_invalid(format!(
                    "data file {source} cannot be both indexed and uncovered"
                )));
            }
        }

        // Completeness: covered ∪ uncovered == every active data file in the split.
        if covered.len() + uncovered.len() != source_files.len() {
            return Err(data_invalid(
                "every full-text source file must be indexed or explicitly uncovered",
            ));
        }

        Ok(Self {
            data_split,
            current_payloads,
            uncovered_data_files,
        })
    }
}

/// One bucket's should-read data files combined into a single split, keeping data
/// files and deletion files in strict parallel order and rejecting duplicate file
/// names. Mirrors Java `PrimaryKeyFullTextScan.BucketAccumulator`: only files that
/// pass `should_read_pk_index_source` are retained.
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
            // Only compaction-visible sources take part in full-text planning,
            // mirroring Java's `PrimaryKeyIndexSourcePolicy.shouldRead` filter.
            if !should_read_pk_index_source(file) {
                continue;
            }
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

    fn is_empty(&self) -> bool {
        self.data_files.is_empty()
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
pub(crate) struct PrimaryKeyFullTextScanPlan {
    // The snapshot the plan resolved during planning (pinned before the index
    // manifest is read); the read guards every split against it before searching.
    // It is authoritative even when planning yields zero splits, and is `0` only
    // for a table with no snapshot at all (never written).
    pub snapshot_id: i64,
    pub splits: Vec<PrimaryKeyFullTextSearchSplit>,
}

pub(crate) struct PrimaryKeyFullTextScan<'a> {
    table: &'a Table,
    text_field_id: i32,
    filter: Option<Predicate>,
}

impl<'a> PrimaryKeyFullTextScan<'a> {
    pub(crate) fn new(table: &'a Table, text_field_id: i32, filter: Option<Predicate>) -> Self {
        Self {
            table,
            text_field_id,
            filter,
        }
    }

    pub(crate) async fn plan(&self) -> crate::Result<PrimaryKeyFullTextScanPlan> {
        let snapshot_manager = self.table.snapshot_manager();

        // Data splits first, via the table's own scan resolution (time travel /
        // scan.snapshot-id aware), then derive the snapshot from the scan output so
        // the index manifest and the data splits stay on ONE snapshot (mirror Java
        // `PrimaryKeyFullTextScan`, which resolves a single snapshot up front). The
        // residual filter is pushed into the read builder so scan planning drops
        // files whose stats cannot match the predicate.
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
            return Ok(PrimaryKeyFullTextScanPlan {
                snapshot_id: 0,
                splits: Vec::new(),
            });
        };
        let snapshot = snapshot_manager.get_snapshot(snapshot_id).await?;

        let mut entries: Vec<(BinaryRow, i32, IndexFileMeta)> = Vec::new();
        if let Some(name) = snapshot.index_manifest() {
            let path = snapshot_manager.manifest_path(name);
            let index_entries = super::global_index_build_common::retain_schema_compatible_entries(
                self.table,
                IndexManifest::read(self.table.file_io(), &path).await?,
                |entry| {
                    entry.index_file.index_type == PK_FULL_TEXT_INDEX_TYPE
                        && entry
                            .index_file
                            .global_index_meta
                            .as_ref()
                            .is_some_and(|meta| meta.index_field_id == self.text_field_id)
                },
            )
            .await?;
            for entry in index_entries {
                // The on-disk index manifest is combined to live ADD entries only.
                if entry.kind != FileKind::Add {
                    return Err(data_invalid(format!(
                        "index manifest entry {} is not active (kind {:?})",
                        entry.index_file.file_name, entry.kind
                    )));
                }
                // Planner guards (mirror Java `matchesDefinition`): full-text index
                // type, global index meta + source meta present, matching field id.
                if entry.index_file.index_type != PK_FULL_TEXT_INDEX_TYPE {
                    continue;
                }
                let Some(gim) = entry.index_file.global_index_meta.as_ref() else {
                    continue;
                };
                if gim.source_meta.is_none() {
                    continue;
                }
                if gim.index_field_id != self.text_field_id {
                    continue;
                }
                let partition = BinaryRow::from_serialized_bytes(&entry.partition)?;
                entries.push((partition, entry.bucket, entry.index_file.clone()));
            }
        }

        let splits = plan_from_inputs(snapshot_id, data_splits, entries, self.text_field_id)?;
        Ok(PrimaryKeyFullTextScanPlan {
            snapshot_id,
            splits,
        })
    }
}

/// Pure planning core, drivable without a live snapshot: group full-text payloads
/// and data splits by `(partition, bucket)`, then assemble one search split per
/// bucket that has active data. Index-only buckets are dropped, not errored.
fn plan_from_inputs(
    snapshot_id: i64,
    data_splits: Vec<DataSplit>,
    index_entries: Vec<(BinaryRow, i32, IndexFileMeta)>,
    text_field_id: i32,
) -> crate::Result<Vec<PrimaryKeyFullTextSearchSplit>> {
    type Key = (Vec<u8>, i32);

    // Phase A: group full-text payloads by (partition, bucket).
    let mut payloads_by_bucket: BTreeMap<Key, Vec<IndexFileMeta>> = BTreeMap::new();
    for (partition, bucket, payload) in index_entries {
        let key = (partition.to_serialized_bytes(), bucket);
        payloads_by_bucket.entry(key).or_default().push(payload);
    }

    // Phase B: group data splits by (partition, bucket).
    let mut accum_by_bucket: BTreeMap<Key, BucketAccumulator> = BTreeMap::new();
    for split in &data_splits {
        // Skip negative-bucket data splits before grouping, mirroring Java
        // `PrimaryKeyFullTextScan.plan`: only real buckets take part in
        // full-text search planning.
        if split.bucket() < 0 {
            continue;
        }
        let key = (split.partition().to_serialized_bytes(), split.bucket());
        let acc = accum_by_bucket.entry(key).or_insert_with(|| {
            BucketAccumulator::new(snapshot_id, split.partition().clone(), split.bucket())
        });
        acc.add(split)?;
    }

    // Phase C: assemble one split per bucket that has active data.
    let mut out = Vec::new();
    for (key, acc) in accum_by_bucket {
        if acc.is_empty() {
            continue;
        }
        let payloads = payloads_by_bucket.remove(&key).unwrap_or_default();
        let data_split = acc.build()?;
        let active_data_files = data_split.data_files().to_vec();
        let state = PkFullTextBucketState::from_active_data_files(
            text_field_id,
            &active_data_files,
            payloads,
        )?;

        // Covered = active source files a current payload maps. Since the state is
        // fed exactly the split's active files, this is the full current mapping;
        // the active-name guard mirrors Java's defensive `activeSources.contains`.
        let active_names: HashSet<&str> = active_data_files
            .iter()
            .map(|f| f.file_name.as_str())
            .collect();
        let covered: HashSet<String> = state
            .payload_by_source_file()
            .keys()
            .filter(|source| active_names.contains(source.as_str()))
            .cloned()
            .collect();
        let uncovered: Vec<String> = active_data_files
            .iter()
            .filter(|f| !covered.contains(&f.file_name))
            .map(|f| f.file_name.clone())
            .collect();

        out.push(PrimaryKeyFullTextSearchSplit::new(
            data_split,
            state.current_payloads().to_vec(),
            uncovered,
        )?);
    }
    // Index-only buckets left in payloads_by_bucket are intentionally dropped.
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::GlobalIndexMeta;

    /// COMPACT file source discriminant (matches Java `FileSource.COMPACT`).
    const FILE_SOURCE_COMPACT: i32 = 1;

    /// A COMPACT data file at `level` (>0 to be a live source by default).
    fn dfm(name: &str, rows: i64, level: i32) -> DataFileMeta {
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
            file_source: Some(FILE_SOURCE_COMPACT),
            value_stats_cols: None,
            external_path: None,
            first_row_id: Some(0),
            write_cols: None,
        }
    }

    /// One Java `DataOutput#writeUTF` value (u16-BE length + modified UTF-8).
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

    /// A valid `_SOURCE_META` frame (version 1) for the given level and files.
    fn frame(data_level: i32, files: &[(&str, i64)]) -> Vec<u8> {
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

    fn gim(field_id: i32, start: i64, end: i64, source_meta: Option<Vec<u8>>) -> GlobalIndexMeta {
        GlobalIndexMeta {
            row_range_start: start,
            row_range_end: end,
            index_field_id: field_id,
            extra_field_ids: None,
            index_meta: None,
            source_meta,
            build_schema_id: None,
        }
    }

    fn payload(
        file_name: &str,
        index_type: &str,
        row_count: i32,
        global_index_meta: Option<GlobalIndexMeta>,
    ) -> IndexFileMeta {
        IndexFileMeta {
            index_type: index_type.into(),
            file_name: file_name.into(),
            file_size: 1,
            row_count,
            deletion_vectors_ranges: None,
            global_index_meta,
        }
    }

    fn ft_payload(file_name: &str, level: i32, files: &[(&str, i64)]) -> IndexFileMeta {
        let total: i64 = files.iter().map(|(_, r)| *r).sum();
        payload(
            file_name,
            PK_FULL_TEXT_INDEX_TYPE,
            total as i32,
            Some(gim(7, 0, total - 1, Some(frame(level, files)))),
        )
    }

    fn data_split(bucket: i32, files: Vec<DataFileMeta>) -> DataSplit {
        DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(bucket)
            .with_bucket_path(format!("memory:/t/bucket-{bucket}"))
            .with_total_buckets(1)
            .with_data_files(files)
            .build()
            .unwrap()
    }

    // (a) planning groups payloads/data by (partition,bucket) and builds a split.
    #[test]
    fn builds_one_split_per_bucket_with_data() {
        let entries = vec![
            (BinaryRow::new(0), 0, ft_payload("ft-0", 1, &[("d0", 3)])),
            (BinaryRow::new(0), 1, ft_payload("ft-1", 1, &[("d1", 4)])),
        ];
        let splits = plan_from_inputs(
            1,
            vec![
                data_split(0, vec![dfm("d0", 3, 1)]),
                data_split(1, vec![dfm("d1", 4, 1)]),
            ],
            entries,
            7,
        )
        .unwrap();
        assert_eq!(splits.len(), 2);
        let b0 = splits.iter().find(|s| s.data_split.bucket() == 0).unwrap();
        assert_eq!(b0.current_payloads.len(), 1);
        assert_eq!(b0.current_payloads[0].file_name, "ft-0");
        assert!(b0.uncovered_data_files.is_empty());
        let b1 = splits.iter().find(|s| s.data_split.bucket() == 1).unwrap();
        assert_eq!(b1.current_payloads[0].file_name, "ft-1");
    }

    // (b) split carries current_payloads + uncovered_data_files: one file covered
    // by a current payload, another active file with no matching payload -> uncovered.
    #[test]
    fn split_carries_current_and_uncovered() {
        // d0 (level 1) is covered by ft-0; d1 (level 2) has no payload -> uncovered.
        let entries = vec![(BinaryRow::new(0), 0, ft_payload("ft-0", 1, &[("d0", 3)]))];
        let splits = plan_from_inputs(
            1,
            vec![data_split(0, vec![dfm("d0", 3, 1), dfm("d1", 5, 2)])],
            entries,
            7,
        )
        .unwrap();
        assert_eq!(splits.len(), 1);
        let s = &splits[0];
        assert_eq!(s.current_payloads.len(), 1);
        assert_eq!(s.current_payloads[0].file_name, "ft-0");
        assert_eq!(s.uncovered_data_files, vec!["d1".to_string()]);
        // Completeness: covered (d0) ∪ uncovered (d1) == active files (d0, d1).
        assert_eq!(s.data_split.data_files().len(), 2);
    }

    // (c) completeness invariant holds through the plan, and a constructed
    // violation is rejected by the split constructor.
    #[test]
    fn split_new_rejects_incomplete_cover() {
        // Data split has d0 + d1 active, ft-0 covers only d0, and uncovered is
        // empty -> d1 is neither covered nor uncovered -> Err.
        let ds = data_split(0, vec![dfm("d0", 3, 1), dfm("d1", 5, 2)]);
        let err = PrimaryKeyFullTextSearchSplit::new(
            ds,
            vec![ft_payload("ft-0", 1, &[("d0", 3)])],
            Vec::new(),
        );
        assert!(err.is_err(), "incomplete covered∪uncovered must fail loud");
    }

    #[test]
    fn split_new_accepts_complete_cover() {
        let ds = data_split(0, vec![dfm("d0", 3, 1), dfm("d1", 5, 2)]);
        let ok = PrimaryKeyFullTextSearchSplit::new(
            ds,
            vec![ft_payload("ft-0", 1, &[("d0", 3)])],
            vec!["d1".to_string()],
        );
        assert!(ok.is_ok());
    }

    #[test]
    fn split_new_rejects_double_covered_source() {
        // Two payloads both claim d0 as an active source -> double cover -> Err.
        let ds = data_split(0, vec![dfm("d0", 3, 1)]);
        let err = PrimaryKeyFullTextSearchSplit::new(
            ds,
            vec![
                ft_payload("ft-a", 1, &[("d0", 3)]),
                ft_payload("ft-b", 2, &[("d0", 3)]),
            ],
            Vec::new(),
        );
        assert!(err.is_err());
    }

    #[test]
    fn split_new_rejects_payload_covering_no_active_source() {
        // ft-x's only source (ghost) is not an active data file -> Err.
        let ds = data_split(0, vec![dfm("d0", 3, 1)]);
        let err = PrimaryKeyFullTextSearchSplit::new(
            ds,
            vec![
                ft_payload("ft-0", 1, &[("d0", 3)]),
                ft_payload("ft-x", 1, &[("ghost", 9)]),
            ],
            Vec::new(),
        );
        assert!(err.is_err());
    }

    // (d) a payload with the wrong index_type is excluded from the current set.
    #[test]
    fn wrong_index_type_payload_excluded() {
        let bad = payload(
            "gi-0",
            "GLOBAL_INDEX",
            3,
            Some(gim(7, 0, 2, Some(frame(1, &[("d0", 3)])))),
        );
        let splits = plan_from_inputs(
            1,
            vec![data_split(0, vec![dfm("d0", 3, 1)])],
            vec![(BinaryRow::new(0), 0, bad)],
            7,
        )
        .unwrap();
        assert_eq!(splits.len(), 1);
        // No current payload -> the sole active file is uncovered.
        assert!(splits[0].current_payloads.is_empty());
        assert_eq!(splits[0].uncovered_data_files, vec!["d0".to_string()]);
    }

    #[test]
    fn drops_index_only_bucket_without_error() {
        // Payload for bucket 0 but NO data split -> no split, no error.
        let entries = vec![(BinaryRow::new(0), 0, ft_payload("ft-0", 1, &[("d0", 3)]))];
        let splits = plan_from_inputs(1, Vec::new(), entries, 7).unwrap();
        assert!(splits.is_empty());
    }

    #[test]
    fn accumulator_skips_non_should_read_files() {
        // d0 level 0 (APPEND-like: level 0 COMPACT is not should_read) is skipped;
        // the bucket has only d1 as an active source.
        let entries = vec![(BinaryRow::new(0), 0, ft_payload("ft-1", 1, &[("d1", 4)]))];
        let splits = plan_from_inputs(
            1,
            vec![data_split(0, vec![dfm("d0", 3, 0), dfm("d1", 4, 1)])],
            entries,
            7,
        )
        .unwrap();
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].data_split.data_files().len(), 1);
        assert_eq!(splits[0].data_split.data_files()[0].file_name, "d1");
        assert_eq!(splits[0].current_payloads[0].file_name, "ft-1");
    }

    #[test]
    fn skips_negative_bucket_data_split() {
        // A negative-bucket data split is excluded from planning (mirror Java's
        // `if (dataSplit.bucket() < 0) continue;`); only the valid bucket ≥ 0
        // produces a split. (The builder reserves the -1 sentinel, so -2 stands
        // in for a negative bucket to exercise the `bucket < 0` skip.)
        let entries = vec![(BinaryRow::new(0), 0, ft_payload("ft-0", 1, &[("d0", 3)]))];
        let splits = plan_from_inputs(
            1,
            vec![
                data_split(-2, vec![dfm("d-neg", 9, 1)]),
                data_split(0, vec![dfm("d0", 3, 1)]),
            ],
            entries,
            7,
        )
        .unwrap();
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].data_split.bucket(), 0);
        assert_eq!(splits[0].data_split.data_files().len(), 1);
        assert_eq!(splits[0].data_split.data_files()[0].file_name, "d0");
        assert_eq!(splits[0].current_payloads[0].file_name, "ft-0");
    }
}
