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

use super::*;
use crate::arrow::build_target_arrow_schema;
use crate::arrow::format::FilePredicates;
use crate::io::FileIOBuilder;
use crate::spec::stats::BinaryTableStats;
use crate::spec::{
    BigIntType, BinaryRow, DataField, DataFileMeta, DataType, Datum, IntType, PredicateBuilder,
    ROW_ID_FIELD_ID, ROW_ID_FIELD_NAME,
};
use crate::table::data_file_reader::DataFileReader;
use crate::table::merge_row_ranges;
use crate::table::schema_manager::SchemaManager;
use crate::table::source::{DataSplit, DataSplitBuilder};
use crate::table::vector_search_common::take_only_result;
use arrow_array::{Int32Array, RecordBatch};
use bytes::Bytes;
use paimon_mosaic_core::spec::COMPRESSION_NONE;
use paimon_mosaic_core::writer::{MosaicWriter, OutputFile, WriterOptions};
use std::io;
use std::sync::Arc;

struct MemOutputFile {
    data: Vec<u8>,
}

impl OutputFile for MemOutputFile {
    fn write(&mut self, data: &[u8]) -> io::Result<()> {
        self.data.extend_from_slice(data);
        Ok(())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
    fn pos(&self) -> u64 {
        self.data.len() as u64
    }
}

fn id_field() -> DataField {
    DataField::new(0, "id".to_string(), DataType::Int(IntType::new()))
}

fn row_id_field() -> DataField {
    DataField::new(
        ROW_ID_FIELD_ID,
        ROW_ID_FIELD_NAME.to_string(),
        DataType::BigInt(BigIntType::new()),
    )
}

fn id_batch(ids: Vec<i32>) -> RecordBatch {
    let schema = build_target_arrow_schema(&[id_field()]).unwrap();
    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids))]).unwrap()
}

fn write_mosaic(batch: &RecordBatch) -> Bytes {
    let mut writer = MosaicWriter::new(
        MemOutputFile { data: Vec::new() },
        batch.schema().as_ref(),
        WriterOptions {
            compression: COMPRESSION_NONE,
            num_buckets: 2,
            row_group_max_size: u64::MAX,
            ..Default::default()
        },
    )
    .unwrap();
    writer.write_batch(batch).unwrap();
    writer.close().unwrap();
    Bytes::from(writer.output().data.to_vec())
}

fn data_file(
    file_name: &str,
    file_size: i64,
    row_count: i64,
    first_row_id: Option<i64>,
) -> DataFileMeta {
    DataFileMeta {
        file_name: file_name.to_string(),
        file_size,
        row_count,
        min_key: Vec::new(),
        max_key: Vec::new(),
        key_stats: BinaryTableStats::empty(),
        value_stats: BinaryTableStats::empty(),
        min_sequence_number: 0,
        max_sequence_number: 0,
        schema_id: 1,
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
    }
}

/// Build a predicate-free reader (read_type = `id` + `_ROW_ID`) over a split
/// containing `files` (each `(name, ids, first_row_id)`), written as Mosaic
/// data files in the same bucket. The returned active-file list covers every
/// file (all files active).
async fn build_reader_and_split(
    table_path: &str,
    files: &[(&str, Vec<i32>, i64)],
) -> (DataFileReader, DataSplit, Vec<BucketActiveFile>) {
    let file_io = FileIOBuilder::new("memory").build().unwrap();
    let bucket_path = format!("{table_path}/bucket-0");
    let mut metas = Vec::new();
    let mut active_files = Vec::new();
    for (name, ids, first_row_id) in files {
        let data = write_mosaic(&id_batch(ids.clone()));
        file_io
            .new_output(&format!("{bucket_path}/{name}"))
            .unwrap()
            .write(data.clone())
            .await
            .unwrap();
        metas.push(data_file(
            name,
            data.len() as i64,
            ids.len() as i64,
            Some(*first_row_id),
        ));
        active_files.push(BucketActiveFile {
            file_name: name.to_string(),
            row_count: ids.len() as i64,
        });
    }
    let split = DataSplitBuilder::new()
        .with_snapshot(1)
        .with_partition(BinaryRow::new(0))
        .with_bucket(0)
        .with_bucket_path(bucket_path)
        .with_total_buckets(1)
        .with_data_files(metas)
        .build()
        .unwrap();
    let reader = DataFileReader::new(
        file_io.clone(),
        SchemaManager::new(file_io, table_path.to_string()),
        1,
        vec![id_field()],
        vec![id_field(), row_id_field()],
        Vec::new(),
    );
    (reader, split, active_files)
}

/// `id > threshold`, with `file_fields` = `[id]` so the leaf index resolves.
fn residual_id_gt(threshold: i32) -> FilePredicates {
    let pred = PredicateBuilder::new(&[id_field()])
        .greater_than("id", Datum::Int(threshold))
        .unwrap();
    FilePredicates {
        predicates: vec![pred],
        row_filter_factory: None,
        file_fields: vec![id_field()],
    }
}

fn sorted(t: &roaring::RoaringTreemap) -> Vec<u64> {
    t.iter().collect()
}

#[tokio::test]
async fn test_residual_selects_matching_positions() {
    // ids [1,2,3,4,5] at first_row_id 0; id > 2 -> ids 3,4,5 -> positions 2,3,4.
    let (reader, split, active) = build_reader_and_split(
        "memory:/rpf_basic",
        &[("part-0.mosaic", vec![1, 2, 3, 4, 5], 0)],
    )
    .await;
    let map = residual_positions_by_file(&reader, &split, &active, &residual_id_gt(2), None)
        .await
        .unwrap();
    assert_eq!(sorted(&map["part-0.mosaic"]), vec![2, 3, 4]);
}

#[tokio::test]
async fn test_residual_only_evaluates_the_rows_the_plan_allows() {
    // ids [1,2,3,4,5]; the plan allows positions 3-4 only. `id > 2` matches 2,3,4
    // over the whole file, so a result of 3,4 is the plan's restriction taking
    // effect *before* evaluation: position 2 is never seen.
    //
    // This also cannot pass under a full read. The scan walks the selection in
    // step with the emitted rows, so a read that emitted all five would run the
    // selection dry and fail loudly rather than return a filtered answer.
    let (reader, split, active) = build_reader_and_split(
        "memory:/rpf_plan_ranges",
        &[("part-0.mosaic", vec![1, 2, 3, 4, 5], 0)],
    )
    .await;
    let allowed = HashMap::from([("part-0.mosaic".to_string(), vec![RowRange::new(3, 4)])]);
    let map =
        residual_positions_by_file(&reader, &split, &active, &residual_id_gt(2), Some(&allowed))
            .await
            .unwrap();
    assert_eq!(sorted(&map["part-0.mosaic"]), vec![3, 4]);
}

#[tokio::test]
async fn test_residual_does_not_read_a_file_the_plan_excludes() {
    // An EMPTY range list is how a plan says "no rows of this file": it is
    // registered empty and never opened. Absence means the opposite -- the plan
    // narrowed nothing there -- so the residual reads the whole file.
    let (reader, split, active) = build_reader_and_split(
        "memory:/rpf_plan_excludes",
        &[("part-0.mosaic", vec![1, 2, 3], 0)],
    )
    .await;

    let excluded = HashMap::from([("part-0.mosaic".to_string(), Vec::new())]);
    let map = residual_positions_by_file(
        &reader,
        &split,
        &active,
        &residual_id_gt(0),
        Some(&excluded),
    )
    .await
    .unwrap();
    assert!(map.contains_key("part-0.mosaic"));
    assert!(sorted(&map["part-0.mosaic"]).is_empty());

    let unrestricted = HashMap::new();
    let map = residual_positions_by_file(
        &reader,
        &split,
        &active,
        &residual_id_gt(0),
        Some(&unrestricted),
    )
    .await
    .unwrap();
    assert_eq!(sorted(&map["part-0.mosaic"]), vec![0, 1, 2]);
}

#[tokio::test]
async fn test_residual_matches_none_yields_empty_entry() {
    // id > 100 matches nothing; the file still gets a (present, empty) entry.
    let (reader, split, active) =
        build_reader_and_split("memory:/rpf_none", &[("part-0.mosaic", vec![1, 2, 3], 0)]).await;
    let map = residual_positions_by_file(&reader, &split, &active, &residual_id_gt(100), None)
        .await
        .unwrap();
    assert!(map.contains_key("part-0.mosaic"));
    assert!(map["part-0.mosaic"].is_empty());
}

#[tokio::test]
async fn test_residual_matches_all_yields_full_set() {
    let (reader, split, active) =
        build_reader_and_split("memory:/rpf_all", &[("part-0.mosaic", vec![1, 2, 3], 0)]).await;
    let map = residual_positions_by_file(&reader, &split, &active, &residual_id_gt(0), None)
        .await
        .unwrap();
    assert_eq!(sorted(&map["part-0.mosaic"]), vec![0, 1, 2]);
}

#[tokio::test]
async fn test_residual_positions_are_file_local_across_files() {
    // Two files with distinct first_row_id; positions must be 0-based within
    // each file, not global. id > 3 keeps ids 4,5 in both -> positions {3,4}.
    let (reader, split, active) = build_reader_and_split(
        "memory:/rpf_multi",
        &[
            ("part-0.mosaic", vec![1, 2, 3, 4, 5], 0),
            ("part-1.mosaic", vec![1, 2, 3, 4, 5], 100),
        ],
    )
    .await;
    let map = residual_positions_by_file(&reader, &split, &active, &residual_id_gt(3), None)
        .await
        .unwrap();
    assert_eq!(sorted(&map["part-0.mosaic"]), vec![3, 4]);
    assert_eq!(sorted(&map["part-1.mosaic"]), vec![3, 4]);
}

#[tokio::test]
async fn test_non_active_files_are_skipped() {
    // Two files in the split, but only `part-0.mosaic` is active. The bucket
    // search never recalls from `part-1.mosaic` (level-0 / non-active), so it
    // must not appear in the residual map — and even though it lacks a
    // `first_row_id`, the query still succeeds because non-active files are
    // skipped before the guard.
    let (reader, split, mut active) = build_reader_and_split(
        "memory:/rpf_nonactive",
        &[("part-0.mosaic", vec![1, 2, 3, 4, 5], 0)],
    )
    .await;
    // Append a non-active file (missing first_row_id) directly to the split's
    // data files, but leave it out of the active list.
    let file_io = FileIOBuilder::new("memory").build().unwrap();
    let bucket_path = "memory:/rpf_nonactive/bucket-0";
    let data = write_mosaic(&id_batch(vec![9, 9, 9]));
    file_io
        .new_output(&format!("{bucket_path}/part-1.mosaic"))
        .unwrap()
        .write(data.clone())
        .await
        .unwrap();
    let mut metas = split.data_files().to_vec();
    metas.push(data_file("part-1.mosaic", data.len() as i64, 3, None));
    // `active` already lists only part-0.mosaic; keep it that way.
    let _ = &mut active;
    let split = DataSplitBuilder::new()
        .with_snapshot(1)
        .with_partition(BinaryRow::new(0))
        .with_bucket(0)
        .with_bucket_path(bucket_path.to_string())
        .with_total_buckets(1)
        .with_data_files(metas)
        .build()
        .unwrap();
    let map = residual_positions_by_file(&reader, &split, &active, &residual_id_gt(2), None)
        .await
        .unwrap();
    assert_eq!(sorted(&map["part-0.mosaic"]), vec![2, 3, 4]);
    assert!(
        !map.contains_key("part-1.mosaic"),
        "non-active file must be skipped"
    );
}

#[tokio::test]
async fn test_missing_first_row_id_recovers_local_positions() {
    // Real primary-key data files carry no `first_row_id`. Positions are
    // recovered from each row's ordinal in the scan, so the residual still
    // works: ids [1,2,3] with id > 0 -> all match -> local positions [0,1,2].
    let (reader, split, active) = build_reader_and_split_no_first_row_id().await;
    let map = residual_positions_by_file(&reader, &split, &active, &residual_id_gt(0), None)
        .await
        .expect("missing first_row_id must not fail the residual read");
    assert_eq!(sorted(&map["part-0.mosaic"]), vec![0, 1, 2]);
}

async fn build_reader_and_split_no_first_row_id(
) -> (DataFileReader, DataSplit, Vec<BucketActiveFile>) {
    let table_path = "memory:/rpf_nofrid";
    let file_io = FileIOBuilder::new("memory").build().unwrap();
    let bucket_path = format!("{table_path}/bucket-0");
    let data = write_mosaic(&id_batch(vec![1, 2, 3]));
    file_io
        .new_output(&format!("{bucket_path}/part-0.mosaic"))
        .unwrap()
        .write(data.clone())
        .await
        .unwrap();
    let split = DataSplitBuilder::new()
        .with_snapshot(1)
        .with_partition(BinaryRow::new(0))
        .with_bucket(0)
        .with_bucket_path(bucket_path)
        .with_total_buckets(1)
        .with_data_files(vec![data_file("part-0.mosaic", data.len() as i64, 3, None)])
        .build()
        .unwrap();
    let reader = DataFileReader::new(
        file_io.clone(),
        SchemaManager::new(file_io, table_path.to_string()),
        1,
        vec![id_field()],
        vec![id_field(), row_id_field()],
        Vec::new(),
    );
    // The lone file is active and carries no first_row_id, exercising the
    // ordinal-based position recovery.
    let active = vec![BucketActiveFile {
        file_name: "part-0.mosaic".to_string(),
        row_count: 3,
    }];
    (reader, split, active)
}

// ---- combining the plan's positional restriction with the residual ----

fn allow_list(entries: &[(&str, &[u64])]) -> HashMap<String, RoaringTreemap> {
    entries
        .iter()
        .map(|(file, positions)| ((*file).to_string(), positions.iter().copied().collect()))
        .collect()
}

/// The plan side carries ranges, so its fixtures are built from the positions
/// each file allows and coalesced the way the planner normalizes them.
fn range_allow_list(entries: &[(&str, &[u64])]) -> HashMap<String, Vec<RowRange>> {
    entries
        .iter()
        .map(|(file, positions)| {
            let ranges = positions
                .iter()
                .map(|p| RowRange::new(*p as i64, *p as i64))
                .collect();
            ((*file).to_string(), merge_row_ranges(ranges))
        })
        .collect()
}

/// The positions a merged selection allows, expanded for readable assertions.
/// Test-only: the production path never expands a range.
fn listed(map: &FileRowSelections, file: &str) -> Vec<u64> {
    match map.get(file) {
        None => Vec::new(),
        Some(FileRowSelection::Positions(positions)) => positions.iter().collect(),
        Some(FileRowSelection::Ranges(ranges)) => ranges
            .iter()
            .flat_map(|range| (range.from() as u64)..=(range.to() as u64))
            .collect(),
    }
}

#[test]
fn no_restriction_on_either_side_stays_unrestricted() {
    assert!(intersect_row_allow_lists(None, None, 1).unwrap().is_none());
}

#[test]
fn one_side_alone_passes_through() {
    let physical = vec![range_allow_list(&[("d0", &[1, 2])])];
    let only_physical = intersect_row_allow_lists(Some(&physical), None, 1)
        .unwrap()
        .expect("a plan restriction survives on its own");
    assert_eq!(listed(&only_physical[0], "d0"), vec![1, 2]);
    // Still intervals. Expanding them here is the unbounded step the plan side
    // must never take, and the positions above cannot tell the two apart.
    assert!(
        matches!(only_physical[0]["d0"], FileRowSelection::Ranges(_)),
        "the plan's ranges must reach the search as ranges"
    );

    let residual = vec![allow_list(&[("d0", &[3])])];
    let only_residual = intersect_row_allow_lists(None, Some(residual), 1)
        .unwrap()
        .expect("a residual survives on its own");
    assert_eq!(listed(&only_residual[0], "d0"), vec![3]);
}

#[test]
fn both_sides_intersect_and_the_residual_stays_fail_closed() {
    // `d0`: both restrict it, so only the shared positions survive. `d1`: the
    // residual says nothing about it. The residual registers EVERY file the
    // search can read from, so its silence is "no rows" -- the plan's ranges
    // must not resurrect the file, and neither may its absence make it
    // unrestricted.
    let physical = vec![range_allow_list(&[("d0", &[1, 2, 3]), ("d1", &[0, 1])])];
    let residual = vec![allow_list(&[("d0", &[2, 3, 4])])];
    let combined = intersect_row_allow_lists(Some(&physical), Some(residual), 1)
        .unwrap()
        .expect("both sides restrict");
    assert_eq!(listed(&combined[0], "d0"), vec![2, 3]);
    assert!(
        combined[0]["d1"].is_excluded(),
        "a file the residual omits must stay excluded"
    );
}

#[test]
fn a_file_neither_side_restricts_stays_absent() {
    // Absence is how "every row" is spelled. A merged map must not invent an
    // entry for a file no one narrowed, or the ANN backend takes the filtered
    // path for a query that filters nothing.
    let physical = vec![range_allow_list(&[("d0", &[1])])];
    let combined = intersect_row_allow_lists(Some(&physical), None, 1)
        .unwrap()
        .expect("the plan restricts d0");
    assert!(!combined[0].contains_key("d1"));

    let residual = vec![allow_list(&[("d0", &[1])])];
    let combined = intersect_row_allow_lists(Some(&physical), Some(residual), 1)
        .unwrap()
        .expect("both restrict d0");
    assert!(!combined[0].contains_key("d1"));
    assert!(!combined[0].contains_key("d2"));
}

#[test]
fn a_plan_that_restricts_nothing_produces_an_empty_selection_map() {
    // The no-pre-filter split: the plan carries a map with no entries at all,
    // and that must survive the merge as an empty map (which the ANN layer reads
    // as "nothing to mask"), not become a per-file all-permitting mask.
    let physical = vec![HashMap::new()];
    let combined = intersect_row_allow_lists(Some(&physical), None, 1)
        .unwrap()
        .expect("a split-driven plan is always Some");
    assert!(combined[0].is_empty());
}

/// The batch terminals here are handed exactly one query, so a result vector of any
/// other length means the batch ran the wrong number of searches. The
/// `debug_assert_eq!` this replaced was compiled out of release builds, where an
/// empty vector panicked on `remove(0)` and a longer one silently returned another
/// query's result.
#[test]
fn take_only_result_rejects_bad_batch_arity() {
    assert_eq!(take_only_result(vec![7], "test").unwrap(), 7);
    assert!(take_only_result::<i32>(Vec::new(), "test").is_err());
    assert!(take_only_result(vec![1, 2], "test").is_err());
}

#[test]
fn rejects_allow_lists_that_do_not_cover_every_split() {
    let physical = vec![range_allow_list(&[("d0", &[1])])];
    let error = intersect_row_allow_lists(Some(&physical), None, 2)
        .map(|_| ())
        .expect_err("an allow-list per split is what makes the index meaningful");
    assert!(error.to_string().contains("for 2 splits"), "{error}");

    let residual = vec![allow_list(&[("d0", &[1])])];
    let error = intersect_row_allow_lists(Some(&physical), Some(residual), 2)
        .map(|_| ())
        .expect_err("the residual must cover every split too");
    assert!(error.to_string().contains("for 2 splits"), "{error}");
}
