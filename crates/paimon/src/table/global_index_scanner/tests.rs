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

use super::entry::*;
use super::evaluator::try_fold_bounded;
use super::predicates::*;
use super::row_ranges::*;
use super::*;
use crate::btree::test_util::VecFileWrite;
use crate::btree::{serialize_datum, BTreeIndexWriter, BlockCompressionType};
use crate::fm_index::{FMGlobalIndexWriter, FMWriteOptions};
use crate::spec::{DataType, Datum, IndexFileMeta, PredicateOperator};
use crate::table::bitmap_global_index_format::{
    make_bitmap_key_comparator, serialize_bitmap_datum,
};
use crate::table::bitmap_global_index_writer::BitmapGlobalIndexWriter;
use roaring::RoaringTreemap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;

#[tokio::test]
async fn test_try_fold_bounded_respects_concurrency_limit() {
    for limit in [1, 3] {
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let futures = (0..9usize).map(|value| {
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            async move {
                let current = active.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                peak.fetch_max(current, AtomicOrdering::SeqCst);
                tokio::task::yield_now().await;
                active.fetch_sub(1, AtomicOrdering::SeqCst);
                Ok::<_, crate::Error>(value)
            }
        });

        let mut values = try_fold_bounded(futures, limit, Vec::new(), |values, value| {
            values.push(value)
        })
        .await
        .unwrap();
        values.sort_unstable();

        assert_eq!(values, (0..9).collect::<Vec<_>>());
        assert_eq!(peak.load(AtomicOrdering::SeqCst), limit);
    }
}

#[test]
fn test_bitmap_to_ranges() {
    assert_eq!(
        bitmap_to_ranges(&RoaringTreemap::new()),
        Vec::<RowRange>::new()
    );

    let mut bm = RoaringTreemap::new();
    bm.insert(5);
    assert_eq!(bitmap_to_ranges(&bm), vec![RowRange::new(5, 5)]);

    let mut bm = RoaringTreemap::new();
    for id in [1, 2, 3, 5, 6, 10] {
        bm.insert(id);
    }
    assert_eq!(
        bitmap_to_ranges(&bm),
        vec![
            RowRange::new(1, 3),
            RowRange::new(5, 6),
            RowRange::new(10, 10),
        ]
    );
}

#[test]
fn test_intersect_sorted_ranges() {
    let a = vec![RowRange::new(0, 10), RowRange::new(20, 30)];
    let b = vec![RowRange::new(5, 25)];
    let result = intersect_sorted_ranges(&a, &b);
    assert_eq!(result, vec![RowRange::new(5, 10), RowRange::new(20, 25)]);
}

#[test]
fn test_intersect_no_overlap() {
    let a = vec![RowRange::new(0, 5)];
    let b = vec![RowRange::new(10, 20)];
    assert!(intersect_sorted_ranges(&a, &b).is_empty());
}

#[test]
fn test_serialize_datum_int() {
    let key = serialize_datum(&Datum::Int(42), &DataType::Int(crate::spec::IntType::new()));
    assert_eq!(key, 42i32.to_le_bytes().to_vec());
}

#[test]
fn test_serialize_datum_string() {
    let key = serialize_datum(
        &Datum::String("hello".to_string()),
        &DataType::VarChar(crate::spec::VarCharType::new(100).unwrap()),
    );
    assert_eq!(key, b"hello".to_vec());
}

fn assert_bitmap_floating_meta_policy(
    data_type: DataType,
    min: Datum,
    max: Datum,
    outside: Datum,
    nan: Datum,
) {
    let cmp = make_bitmap_key_comparator(&data_type);
    let min_key = serialize_bitmap_datum(&min, &data_type);
    let max_key = serialize_bitmap_datum(&max, &data_type);
    let outside_key = serialize_bitmap_datum(&outside, &data_type);
    let nan_key = serialize_bitmap_datum(&nan, &data_type);
    let meta = BTreeIndexMeta::new(Some(min_key.clone()), Some(max_key), false);

    assert!(!bitmap_meta_may_match(
        &meta,
        PredicateOperator::Eq,
        &data_type,
        std::slice::from_ref(&outside_key),
        cmp.as_ref(),
    ));
    assert!(!bitmap_meta_may_match(
        &meta,
        PredicateOperator::In,
        &data_type,
        std::slice::from_ref(&outside_key),
        cmp.as_ref(),
    ));
    assert!(!bitmap_meta_may_match(
        &meta,
        PredicateOperator::IsNull,
        &data_type,
        &[],
        cmp.as_ref(),
    ));
    assert!(bitmap_meta_may_match(
        &meta,
        PredicateOperator::IsNotNull,
        &data_type,
        &[],
        cmp.as_ref(),
    ));

    let nan_meta = BTreeIndexMeta::new(Some(min_key), Some(nan_key.clone()), false);
    assert!(bitmap_meta_may_match(
        &nan_meta,
        PredicateOperator::Eq,
        &data_type,
        std::slice::from_ref(&nan_key),
        cmp.as_ref(),
    ));
    assert!(bitmap_meta_may_match(
        &nan_meta,
        PredicateOperator::In,
        &data_type,
        std::slice::from_ref(&nan_key),
        cmp.as_ref(),
    ));

    assert!(bitmap_meta_may_match(
        &meta,
        PredicateOperator::Gt,
        &data_type,
        std::slice::from_ref(&outside_key),
        cmp.as_ref(),
    ));
    assert!(bitmap_meta_may_match_between(
        &meta,
        &data_type,
        &outside_key,
        &outside_key,
        cmp.as_ref(),
    ));

    let only_nulls = BTreeIndexMeta::new(None, None, true);
    assert!(bitmap_meta_may_match(
        &only_nulls,
        PredicateOperator::IsNull,
        &data_type,
        &[],
        cmp.as_ref(),
    ));
    assert!(!bitmap_meta_may_match(
        &only_nulls,
        PredicateOperator::IsNotNull,
        &data_type,
        &[],
        cmp.as_ref(),
    ));
    assert!(!bitmap_meta_may_match(
        &only_nulls,
        PredicateOperator::NotEq,
        &data_type,
        std::slice::from_ref(&outside_key),
        cmp.as_ref(),
    ));
    assert!(!bitmap_meta_may_match_between(
        &only_nulls,
        &data_type,
        &outside_key,
        &outside_key,
        cmp.as_ref(),
    ));
}

#[test]
fn test_bitmap_floating_meta_prunes_equality_and_fails_open_for_ranges() {
    assert_bitmap_floating_meta_policy(
        DataType::Float(crate::spec::FloatType::new()),
        Datum::Float(-1.0),
        Datum::Float(1.0),
        Datum::Float(2.0),
        Datum::Float(f32::NAN),
    );
    assert_bitmap_floating_meta_policy(
        DataType::Double(crate::spec::DoubleType::new()),
        Datum::Double(-1.0),
        Datum::Double(1.0),
        Datum::Double(2.0),
        Datum::Double(f64::NAN),
    );
}

#[test]
fn test_row_range_index_merges_overlapping() {
    let idx = RowRangeIndex::create(vec![
        RowRange::new(0, 5),
        RowRange::new(3, 10),
        RowRange::new(20, 30),
    ]);
    assert_eq!(idx.ranges().len(), 2);
    assert_eq!(idx.ranges()[0], RowRange::new(0, 10));
    assert_eq!(idx.ranges()[1], RowRange::new(20, 30));
}

#[test]
fn test_row_range_index_merges_adjacent() {
    let idx = RowRangeIndex::create(vec![RowRange::new(0, 5), RowRange::new(6, 10)]);
    assert_eq!(idx.ranges().len(), 1);
    assert_eq!(idx.ranges()[0], RowRange::new(0, 10));
}

#[test]
fn test_row_range_index_intersects() {
    let idx = RowRangeIndex::create(vec![RowRange::new(10, 20), RowRange::new(30, 40)]);
    assert!(idx.intersects(15, 25));
    assert!(idx.intersects(5, 10));
    assert!(idx.intersects(20, 30));
    assert!(!idx.intersects(0, 9));
    assert!(!idx.intersects(21, 29));
    assert!(!idx.intersects(41, 50));
}

#[test]
fn test_row_range_index_intersected_ranges() {
    let idx = RowRangeIndex::create(vec![
        RowRange::new(10, 20),
        RowRange::new(30, 40),
        RowRange::new(50, 60),
    ]);
    let result = idx.intersected_ranges(15, 55);
    assert_eq!(
        result,
        vec![
            RowRange::new(15, 20),
            RowRange::new(30, 40),
            RowRange::new(50, 55),
        ]
    );
}

#[test]
fn test_row_range_index_intersection_row_count() {
    let idx = RowRangeIndex::create(vec![
        RowRange::new(10, 20),
        RowRange::new(30, 40),
        RowRange::new(50, 60),
    ]);

    assert_eq!(idx.intersection_row_count(15, 55), 23);
    assert_eq!(idx.intersection_row_count(21, 29), 0);
    assert_eq!(idx.intersection_row_count(55, 15), 0);
}

#[test]
fn test_search_limit_with_deleted_rows_expands_and_caps() {
    let idx = RowRangeIndex::create(vec![RowRange::new(2, 4), RowRange::new(8, 10)]);

    assert_eq!(search_limit_with_deleted_rows(5, 0, 19, Some(&idx)), 11);
    assert_eq!(search_limit_with_deleted_rows(18, 0, 19, Some(&idx)), 20);
    assert_eq!(search_limit_with_deleted_rows(5, 0, 19, None), 5);
}

#[test]
fn test_row_range_index_empty() {
    let idx = RowRangeIndex::create(Vec::new());
    assert!(!idx.intersects(0, 100));
    assert!(idx.intersected_ranges(0, 100).is_empty());
}

fn le_int_key(v: i32) -> Vec<u8> {
    v.to_le_bytes().to_vec()
}

/// Set up a temp dir with `index/{file_name}` containing the btree testdata file,
/// and return (FileIO, table_path, file_name, _tmp_dir).
fn setup_testdata_table(testdata_name: &str) -> (FileIO, String, String, tempfile::TempDir) {
    let src = format!(
        "{}/testdata/btree/{testdata_name}",
        env!("CARGO_MANIFEST_DIR")
    );
    let tmp = tempfile::tempdir().unwrap();
    let index_dir = tmp.path().join("index");
    std::fs::create_dir_all(&index_dir).unwrap();
    std::fs::copy(&src, index_dir.join(testdata_name)).unwrap();

    let table_path = format!("file://{}", tmp.path().display());
    let file_io = crate::io::FileIOBuilder::new("file").build().unwrap();
    (file_io, table_path, testdata_name.to_string(), tmp)
}

type BitmapTestdataTable = (FileIO, String, String, BTreeIndexMeta, tempfile::TempDir);

fn setup_bitmap_testdata_table(file_name: &str) -> BitmapTestdataTable {
    let src = format!("{}/testdata/bitmap/{file_name}", env!("CARGO_MANIFEST_DIR"));
    let meta_src = format!(
        "{}/testdata/bitmap/{file_name}.meta",
        env!("CARGO_MANIFEST_DIR")
    );
    let tmp = tempfile::tempdir().unwrap();
    let index_dir = tmp.path().join("index");
    std::fs::create_dir_all(&index_dir).unwrap();
    std::fs::copy(&src, index_dir.join(file_name)).unwrap();
    let meta = BTreeIndexMeta::deserialize(&std::fs::read(meta_src).unwrap()).unwrap();

    let table_path = format!("file://{}", tmp.path().display());
    let file_io = crate::io::FileIOBuilder::new("file").build().unwrap();
    (file_io, table_path, file_name.to_string(), meta, tmp)
}

fn setup_java_bitmap_testdata_table() -> BitmapTestdataTable {
    setup_bitmap_testdata_table("bitmap_varchar_java.index")
}

fn make_global_index_entry(
    file_name: &str,
    field_id: i32,
    row_range_start: i64,
    row_range_end: i64,
    meta: &BTreeIndexMeta,
) -> crate::spec::IndexManifestEntry {
    make_global_index_entry_with_type(
        BTREE_GLOBAL_INDEX_TYPE,
        file_name,
        field_id,
        row_range_start,
        row_range_end,
        meta,
    )
}

fn make_global_index_entry_with_type(
    index_type: &str,
    file_name: &str,
    field_id: i32,
    row_range_start: i64,
    row_range_end: i64,
    meta: &BTreeIndexMeta,
) -> crate::spec::IndexManifestEntry {
    use crate::spec::{GlobalIndexMeta, IndexFileMeta};
    IndexManifestEntry {
        version: 1,
        kind: FileKind::Add,
        partition: vec![],
        bucket: 0,
        index_file: IndexFileMeta {
            index_type: index_type.to_string(),
            file_name: file_name.to_string(),
            file_size: 0,
            row_count: 0,
            deletion_vectors_ranges: None,
            global_index_meta: Some(GlobalIndexMeta {
                row_range_start,
                row_range_end,
                index_field_id: field_id,
                extra_field_ids: None,
                source_meta: None,
                index_meta: Some(meta.serialize()),
            }),
        },
    }
}

async fn make_fm_index_entry(
    file_name: &str,
    field_id: i32,
    row_range_start: i64,
    row_range_end: i64,
    first_row_id: u64,
    row_count: u64,
) -> IndexManifestEntry {
    let output = VecFileWrite::new();
    let mut writer = FMGlobalIndexWriter::new(
        Box::new(output.clone()),
        FMWriteOptions {
            compression: BlockCompressionType::None,
            ..FMWriteOptions::default()
        },
    )
    .unwrap();
    for row_id in first_row_id..first_row_id + row_count {
        writer.write(Some(b"value"), row_id).await.unwrap();
    }
    let result = writer.finish().await.unwrap();
    IndexManifestEntry {
        version: 1,
        kind: FileKind::Add,
        partition: vec![],
        bucket: 0,
        index_file: IndexFileMeta {
            index_type: FM_GLOBAL_INDEX_TYPE.to_string(),
            file_name: file_name.to_string(),
            file_size: output.to_vec().len() as i64,
            row_count: result.row_count as i64,
            deletion_vectors_ranges: None,
            global_index_meta: Some(crate::spec::GlobalIndexMeta {
                row_range_start,
                row_range_end,
                index_field_id: field_id,
                extra_field_ids: None,
                source_meta: None,
                index_meta: Some(result.index_meta),
            }),
        },
    }
}

#[tokio::test]
async fn test_fm_file_set_must_exactly_cover_source_range() {
    let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
    let first = make_fm_index_entry("first.fm", 1, 10, 13, 0, 2).await;
    let error = match GlobalIndexScanner::create(
        &file_io,
        "memory:/table",
        1,
        i64::MAX,
        i64::MAX,
        std::slice::from_ref(&first),
        &string_schema_fields(),
    ) {
        Err(error) => error,
        Ok(_) => panic!("a partial Java FM file set must fail closed"),
    };
    assert!(
        matches!(error, Error::DataInvalid { message, .. } if message.contains("cover 2 rows, expected 4"))
    );

    let second = make_fm_index_entry("second.fm", 1, 10, 13, 2, 2).await;
    assert!(GlobalIndexScanner::create(
        &file_io,
        "memory:/table",
        1,
        i64::MAX,
        i64::MAX,
        &[first, second],
        &string_schema_fields(),
    )
    .unwrap()
    .is_some());
}

#[test]
fn test_mixed_fm_and_btree_select_compatible_index_family() {
    let btree = GlobalIndexEntry {
        file_name: "name.btree".to_string(),
        index_type: GlobalIndexFileKind::BTree,
        file_size: 1,
        row_range_start: 0,
        row_range_end: 9,
        meta: GlobalIndexEntryMeta::Sorted(BTreeIndexMeta::new(None, None, false)),
    };
    let fm = GlobalIndexEntry {
        file_name: "name.fm".to_string(),
        index_type: GlobalIndexFileKind::FM,
        file_size: 1,
        row_range_start: 0,
        row_range_end: 9,
        meta: GlobalIndexEntryMeta::FM {
            bytes: Vec::new(),
            first_row_id: 0,
            row_count: 10,
        },
    };
    let entries = [btree, fm];
    let data_type = DataType::VarChar(crate::spec::VarCharType::string_type());
    let literal = [Datum::String("needle".to_string())];

    let contains = [(PredicateOperator::Contains, literal.as_slice(), &data_type)];
    let selected = select_entries_for_predicates(&entries, &contains);
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].index_type, GlobalIndexFileKind::FM);

    let equals = [(PredicateOperator::Eq, literal.as_slice(), &data_type)];
    let selected = select_entries_for_predicates(&entries, &equals);
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].index_type, GlobalIndexFileKind::BTree);
}

fn int_schema_fields() -> Vec<DataField> {
    vec![DataField::new(
        1,
        "id".to_string(),
        DataType::Int(crate::spec::IntType::new()),
    )]
}

fn string_schema_fields() -> Vec<DataField> {
    vec![DataField::new(
        1,
        "name".to_string(),
        DataType::VarChar(crate::spec::VarCharType::string_type()),
    )]
}

async fn evaluate_global_index_fast(
    file_io: &FileIO,
    table_path: &str,
    entries: &[IndexManifestEntry],
    predicates: &[Predicate],
    fields: &[DataField],
) -> Result<Option<Vec<RowRange>>> {
    evaluate_global_index_fast_with_fallback_size(
        file_io,
        table_path,
        entries,
        predicates,
        fields,
        i64::MAX,
        i64::MAX,
    )
    .await
}

async fn evaluate_global_index_fast_with_fallback_size(
    file_io: &FileIO,
    table_path: &str,
    entries: &[IndexManifestEntry],
    predicates: &[Predicate],
    fields: &[DataField],
    btree_fallback_scan_max_size: i64,
    bitmap_fallback_scan_max_size: i64,
) -> Result<Option<Vec<RowRange>>> {
    super::evaluate_global_index(super::GlobalIndexEvaluation {
        file_io,
        table_path,
        index_entries: entries,
        predicates,
        schema_fields: fields,
        search_mode: GlobalIndexSearchMode::Fast,
        global_index_thread_num: 32,
        btree_fallback_scan_max_size,
        bitmap_fallback_scan_max_size,
        fm_read_options: FMReadOptions::default(),
        next_row_id: None,
        data_ranges: &[],
    })
    .await
}

fn two_field_schema_fields() -> Vec<DataField> {
    vec![
        DataField::new(
            1,
            "id".to_string(),
            DataType::Int(crate::spec::IntType::new()),
        ),
        DataField::new(
            2,
            "value".to_string(),
            DataType::Int(crate::spec::IntType::new()),
        ),
    ]
}

fn int_eq(column: &str, index: usize, value: i32) -> Predicate {
    Predicate::Leaf {
        column: column.to_string(),
        index,
        data_type: DataType::Int(crate::spec::IntType::new()),
        op: PredicateOperator::Eq,
        literals: vec![Datum::Int(value)],
    }
}

#[test]
fn test_unindexed_ranges_fast_mode_empty() {
    let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
    let meta = BTreeIndexMeta::new(None, None, false);
    let entries = vec![make_global_index_entry("idx", 1, 0, 49, &meta)];
    let fields = int_schema_fields();
    let scanner = GlobalIndexScanner::create(
        &file_io,
        "memory:/t",
        32,
        i64::MAX,
        i64::MAX,
        &entries,
        &fields,
    )
    .expect("create scanner")
    .expect("scanner");

    let ranges = scanner
        .unindexed_ranges(
            &int_eq("id", 0, 7),
            GlobalIndexSearchMode::Fast,
            Some(100),
            &[RowRange::new(50, 99)],
        )
        .unwrap();
    assert!(ranges.is_empty());
}

#[test]
fn test_unindexed_ranges_full_uses_snapshot_next_row_id() {
    let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
    let meta = BTreeIndexMeta::new(None, None, false);
    let entries = vec![make_global_index_entry("idx", 1, 0, 49, &meta)];
    let fields = int_schema_fields();
    let scanner = GlobalIndexScanner::create(
        &file_io,
        "memory:/t",
        32,
        i64::MAX,
        i64::MAX,
        &entries,
        &fields,
    )
    .expect("create scanner")
    .expect("scanner");

    let ranges = scanner
        .unindexed_ranges(
            &int_eq("id", 0, 7),
            GlobalIndexSearchMode::Full,
            Some(100),
            &[],
        )
        .unwrap();
    assert_eq!(ranges, vec![RowRange::new(50, 99)]);
}

#[test]
fn test_unindexed_ranges_detail_uses_data_file_ranges() {
    let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
    let meta = BTreeIndexMeta::new(None, None, false);
    let entries = vec![make_global_index_entry("idx", 1, 0, 49, &meta)];
    let fields = int_schema_fields();
    let scanner = GlobalIndexScanner::create(
        &file_io,
        "memory:/t",
        32,
        i64::MAX,
        i64::MAX,
        &entries,
        &fields,
    )
    .expect("create scanner")
    .expect("scanner");

    let ranges = scanner
        .unindexed_ranges(
            &int_eq("id", 0, 7),
            GlobalIndexSearchMode::Detail,
            Some(100),
            &[
                RowRange::new(0, 10),
                RowRange::new(40, 60),
                RowRange::new(80, 90),
            ],
        )
        .unwrap();
    assert_eq!(ranges, vec![RowRange::new(50, 60), RowRange::new(80, 90)]);
}

#[test]
fn test_unindexed_ranges_uses_all_predicate_field_coverage() {
    let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
    let meta = BTreeIndexMeta::new(None, None, false);
    let entries = vec![
        make_global_index_entry("idx_id", 1, 0, 49, &meta),
        make_global_index_entry("idx_value", 2, 0, 99, &meta),
    ];
    let fields = two_field_schema_fields();
    let scanner = GlobalIndexScanner::create(
        &file_io,
        "memory:/t",
        32,
        i64::MAX,
        i64::MAX,
        &entries,
        &fields,
    )
    .expect("create scanner")
    .expect("scanner");
    let predicate = Predicate::and(vec![int_eq("id", 0, 7), int_eq("value", 1, 8)]);

    let ranges = scanner
        .unindexed_ranges(&predicate, GlobalIndexSearchMode::Full, Some(100), &[])
        .unwrap();
    assert_eq!(ranges, vec![RowRange::new(50, 99)]);
}

#[test]
fn test_unindexed_ranges_missing_field_coverage_reads_all_data_ranges() {
    let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
    let meta = BTreeIndexMeta::new(None, None, false);
    let entries = vec![make_global_index_entry("idx_id", 1, 0, 49, &meta)];
    let fields = two_field_schema_fields();
    let scanner = GlobalIndexScanner::create(
        &file_io,
        "memory:/t",
        32,
        i64::MAX,
        i64::MAX,
        &entries,
        &fields,
    )
    .expect("create scanner")
    .expect("scanner");
    let predicate = Predicate::and(vec![int_eq("id", 0, 7), int_eq("value", 1, 8)]);

    let ranges = scanner
        .unindexed_ranges(&predicate, GlobalIndexSearchMode::Full, Some(100), &[])
        .unwrap();
    assert_eq!(ranges, vec![RowRange::new(0, 99)]);
}

#[test]
fn test_unindexed_ranges_counts_extra_field_coverage() {
    let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
    let meta = BTreeIndexMeta::new(None, None, false);
    let mut entry = make_global_index_entry("idx_id_value", 1, 0, 99, &meta);
    entry
        .index_file
        .global_index_meta
        .as_mut()
        .unwrap()
        .extra_field_ids = Some(vec![2]);
    let fields = two_field_schema_fields();
    let scanner = GlobalIndexScanner::create(
        &file_io,
        "memory:/t",
        32,
        i64::MAX,
        i64::MAX,
        &[entry],
        &fields,
    )
    .expect("create scanner")
    .expect("scanner");

    let ranges = scanner
        .unindexed_ranges(
            &int_eq("value", 1, 8),
            GlobalIndexSearchMode::Full,
            Some(100),
            &[],
        )
        .unwrap();
    assert!(ranges.is_empty());
}

#[tokio::test]
async fn test_evaluate_extra_field_only_without_composite_reader_falls_back() {
    let (file_io, table_path, file_name, _tmp) =
        setup_testdata_table("btree_int_100_no_compress.bin");
    let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);
    let mut entry = make_global_index_entry(&file_name, 1, 0, 99, &meta);
    entry
        .index_file
        .global_index_meta
        .as_mut()
        .unwrap()
        .extra_field_ids = Some(vec![2]);
    let fields = two_field_schema_fields();
    let predicates = vec![int_eq("value", 1, 50)];

    let result = evaluate_global_index_fast(&file_io, &table_path, &[entry], &predicates, &fields)
        .await
        .unwrap();
    assert!(
        result.is_none(),
        "extra-field-only predicates must fall back until composite-key btree reads are supported"
    );
}

#[tokio::test]
async fn test_evaluate_global_index_eq() {
    let (file_io, table_path, file_name, tmp) =
        setup_testdata_table("btree_int_100_no_compress.bin");
    let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);
    let mut entry = make_global_index_entry(&file_name, 1, 0, 99, &meta);
    entry.index_file.file_size = std::fs::metadata(tmp.path().join("index").join(&file_name))
        .unwrap()
        .len() as i64;
    let entries = vec![entry];
    let fields = int_schema_fields();

    // key=50 -> row_id=25, offset by row_range_start=0 -> global row_id=25
    let predicates = vec![Predicate::Leaf {
        column: "id".to_string(),
        index: 0,
        data_type: DataType::Int(crate::spec::IntType::new()),
        op: PredicateOperator::Eq,
        literals: vec![Datum::Int(50)],
    }];

    let result = evaluate_global_index_fast(&file_io, &table_path, &entries, &predicates, &fields)
        .await
        .unwrap();
    let ranges = result.unwrap();
    assert_eq!(ranges, vec![RowRange::new(25, 25)]);
}

#[tokio::test]
async fn test_evaluate_global_index_uses_known_file_size() {
    let (file_io, table_path, file_name, _tmp) =
        setup_testdata_table("btree_int_100_no_compress.bin");
    let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);
    let mut entry = make_global_index_entry(&file_name, 1, 0, 99, &meta);
    entry.index_file.file_size = 1;

    let error = evaluate_global_index_fast(
        &file_io,
        &table_path,
        &[entry],
        &[int_eq("id", 0, 50)],
        &int_schema_fields(),
    )
    .await
    .expect_err("the known file size should be used without a metadata lookup");

    assert!(matches!(
        error,
        crate::Error::DataInvalid { message, .. }
            if message.contains("Failed to open BTree index file")
    ));
}

#[tokio::test]
async fn test_missing_index_meta_returns_error() {
    let (file_io, table_path, file_name, tmp) =
        setup_testdata_table("btree_int_100_no_compress.bin");
    let second_file_name = "btree_int_100_no_compress_2.bin";
    std::fs::copy(
        tmp.path().join("index").join(&file_name),
        tmp.path().join("index").join(second_file_name),
    )
    .unwrap();
    let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);
    let valid_entry = make_global_index_entry(&file_name, 1, 0, 99, &meta);
    let mut invalid_entry = make_global_index_entry(second_file_name, 1, 100, 199, &meta);
    invalid_entry
        .index_file
        .global_index_meta
        .as_mut()
        .unwrap()
        .index_meta = None;

    let error = evaluate_global_index_fast(
        &file_io,
        &table_path,
        &[valid_entry, invalid_entry],
        &[int_eq("id", 0, 50)],
        &int_schema_fields(),
    )
    .await
    .expect_err("missing sorted index metadata must fail the scan");

    assert!(matches!(
        error,
        crate::Error::DataInvalid { message, .. }
            if message.contains(second_file_name)
    ));
}

#[tokio::test]
async fn test_invalid_index_meta_returns_error() {
    let (file_io, table_path, file_name, tmp) =
        setup_testdata_table("btree_int_100_no_compress.bin");
    let second_file_name = "btree_int_100_no_compress_2.bin";
    std::fs::copy(
        tmp.path().join("index").join(&file_name),
        tmp.path().join("index").join(second_file_name),
    )
    .unwrap();
    let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);
    let valid_entry = make_global_index_entry(&file_name, 1, 0, 99, &meta);
    let mut invalid_entry = make_global_index_entry(second_file_name, 1, 100, 199, &meta);
    let mut invalid_meta = vec![0; 9];
    invalid_meta[..4].copy_from_slice(&10i32.to_le_bytes());
    invalid_entry
        .index_file
        .global_index_meta
        .as_mut()
        .unwrap()
        .index_meta = Some(invalid_meta);

    let error = evaluate_global_index_fast(
        &file_io,
        &table_path,
        &[valid_entry, invalid_entry],
        &[int_eq("id", 0, 50)],
        &int_schema_fields(),
    )
    .await
    .expect_err("invalid sorted index metadata must fail the scan");

    assert!(matches!(
        error,
        crate::Error::DataInvalid {
            message,
            source: Some(_),
        } if message.contains(second_file_name)
    ));
}

#[tokio::test]
async fn test_missing_global_index_meta_returns_error() {
    let (file_io, table_path, file_name, tmp) =
        setup_testdata_table("btree_int_100_no_compress.bin");
    let second_file_name = "btree_int_100_no_compress_2.bin";
    std::fs::copy(
        tmp.path().join("index").join(&file_name),
        tmp.path().join("index").join(second_file_name),
    )
    .unwrap();
    let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);
    let valid_entry = make_global_index_entry(&file_name, 1, 0, 99, &meta);
    let mut invalid_entry = make_global_index_entry(second_file_name, 1, 100, 199, &meta);
    invalid_entry.index_file.global_index_meta = None;

    let error = evaluate_global_index_fast(
        &file_io,
        &table_path,
        &[valid_entry, invalid_entry],
        &[int_eq("id", 0, 50)],
        &int_schema_fields(),
    )
    .await
    .expect_err("missing global index metadata must fail the scan");

    assert!(matches!(
        error,
        crate::Error::DataInvalid { message, .. }
            if message.contains(second_file_name)
    ));
}

#[tokio::test]
async fn test_evaluate_java_bitmap_golden_index_eq_and_null() {
    let data_type = DataType::VarChar(crate::spec::VarCharType::string_type());
    let (file_io, table_path, file_name, meta, tmp) = setup_java_bitmap_testdata_table();
    let mut entry =
        make_global_index_entry_with_type(BITMAP_GLOBAL_INDEX_TYPE, &file_name, 1, 100, 109, &meta);
    entry.index_file.file_size = std::fs::metadata(tmp.path().join("index").join(&file_name))
        .unwrap()
        .len() as i64;
    let entries = vec![entry];
    let fields = string_schema_fields();
    assert_eq!(meta.first_key, Some(b"alpha".to_vec()));
    assert_eq!(meta.last_key, Some(b"office".to_vec()));
    assert!(meta.has_nulls);

    let eq_predicates = vec![Predicate::Leaf {
        column: "name".to_string(),
        index: 0,
        data_type: data_type.clone(),
        op: PredicateOperator::Eq,
        literals: vec![Datum::String("k2".to_string())],
    }];
    let eq_result =
        evaluate_global_index_fast(&file_io, &table_path, &entries, &eq_predicates, &fields)
            .await
            .unwrap();
    assert_eq!(eq_result.unwrap(), vec![RowRange::new(105, 106)]);

    let null_predicates = vec![Predicate::Leaf {
        column: "name".to_string(),
        index: 0,
        data_type,
        op: PredicateOperator::IsNull,
        literals: vec![],
    }];
    let null_result =
        evaluate_global_index_fast(&file_io, &table_path, &entries, &null_predicates, &fields)
            .await
            .unwrap();
    assert_eq!(null_result.unwrap(), vec![RowRange::new(104, 104)]);
}

async fn assert_bitmap_int_fixture(file_name: &str) {
    let data_type = DataType::Int(crate::spec::IntType::new());
    let (file_io, table_path, file_name, meta, _tmp) = setup_bitmap_testdata_table(file_name);
    let entries = vec![make_global_index_entry_with_type(
        BITMAP_GLOBAL_INDEX_TYPE,
        &file_name,
        1,
        100,
        105,
        &meta,
    )];
    let fields = int_schema_fields();
    assert_eq!(meta.first_key, Some(le_int_key(-1)));
    assert_eq!(meta.last_key, Some(le_int_key(256)));
    assert!(meta.has_nulls);

    let cases = [
        (
            PredicateOperator::Eq,
            vec![Datum::Int(0)],
            vec![RowRange::new(101, 102)],
        ),
        (
            PredicateOperator::Eq,
            vec![Datum::Int(256)],
            vec![RowRange::new(104, 104)],
        ),
        (
            PredicateOperator::In,
            vec![Datum::Int(-1), Datum::Int(1), Datum::Int(256)],
            vec![RowRange::new(100, 100), RowRange::new(103, 104)],
        ),
        (
            PredicateOperator::NotEq,
            vec![Datum::Int(0)],
            vec![RowRange::new(100, 100), RowRange::new(103, 104)],
        ),
        (
            PredicateOperator::NotIn,
            vec![Datum::Int(-1), Datum::Int(1), Datum::Int(256)],
            vec![RowRange::new(101, 102)],
        ),
        (
            PredicateOperator::IsNull,
            vec![],
            vec![RowRange::new(105, 105)],
        ),
    ];

    for (op, literals, expected) in cases {
        let predicates = vec![Predicate::Leaf {
            column: "id".to_string(),
            index: 0,
            data_type: data_type.clone(),
            op,
            literals,
        }];
        let result =
            evaluate_global_index_fast(&file_io, &table_path, &entries, &predicates, &fields)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(result, expected, "{file_name}: {op}");
    }
}

#[tokio::test]
async fn test_evaluate_java_logical_order_bitmap_int_fixture() {
    assert_bitmap_int_fixture("bitmap_int_logical_java.index").await;
}

async fn assert_bitmap_nan_equality_uses_direct_lookup(
    data_type: DataType,
    nan_literals: [Datum; 3],
    zero: Datum,
) {
    let output = VecFileWrite::new();
    let captured = output.clone();
    let mut writer = BitmapGlobalIndexWriter::new(
        Box::new(output),
        1,
        BlockCompressionType::None,
        make_bitmap_key_comparator(&data_type),
    );
    for (row_id, literal) in nan_literals.iter().enumerate() {
        let key = serialize_bitmap_datum(literal, &data_type);
        writer.write(Some(&key), row_id as i64).unwrap();
    }
    let zero_key = serialize_bitmap_datum(&zero, &data_type);
    writer.write(Some(&zero_key), 3).unwrap();
    let write_result = writer.finish().await.unwrap();
    let bytes = captured.to_vec();

    let tmp = tempfile::tempdir().unwrap();
    let index_dir = tmp.path().join("index");
    std::fs::create_dir_all(&index_dir).unwrap();
    let file_name = "bitmap-current.index";
    std::fs::write(index_dir.join(file_name), &bytes).unwrap();
    let table_path = format!("file://{}", tmp.path().display());
    let file_io = crate::io::FileIOBuilder::new("file").build().unwrap();

    let mut entry = make_global_index_entry_with_type(
        BITMAP_GLOBAL_INDEX_TYPE,
        file_name,
        1,
        100,
        103,
        &write_result.meta,
    );
    entry.index_file.file_size = bytes.len() as i64;
    let entries = vec![entry];
    let fields = vec![DataField::new(1, "id".to_string(), data_type.clone())];
    let cases = [
        (PredicateOperator::Eq, vec![nan_literals[0].clone()]),
        (
            PredicateOperator::In,
            vec![nan_literals[1].clone(), nan_literals[2].clone()],
        ),
    ];

    for (op, literals) in cases {
        let predicates = vec![Predicate::Leaf {
            column: "id".to_string(),
            index: 0,
            data_type: data_type.clone(),
            op,
            literals,
        }];
        let result = evaluate_global_index_fast_with_fallback_size(
            &file_io,
            &table_path,
            &entries,
            &predicates,
            &fields,
            i64::MAX,
            0,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result, vec![RowRange::new(100, 102)], "{data_type:?}: {op}");
    }
}

#[tokio::test]
async fn test_bitmap_nan_equality_uses_direct_lookup_with_fallback_scan_disabled() {
    assert_bitmap_nan_equality_uses_direct_lookup(
        DataType::Float(crate::spec::FloatType::new()),
        [
            Datum::Float(f32::from_bits(0xffc0_0001)),
            Datum::Float(f32::from_bits(0x7fc0_0010)),
            Datum::Float(f32::NAN),
        ],
        Datum::Float(0.0),
    )
    .await;
    assert_bitmap_nan_equality_uses_direct_lookup(
        DataType::Double(crate::spec::DoubleType::new()),
        [
            Datum::Double(f64::from_bits(0xfff8_0000_0000_0001)),
            Datum::Double(f64::from_bits(0x7ff8_0000_0000_0010)),
            Datum::Double(f64::NAN),
        ],
        Datum::Double(0.0),
    )
    .await;
}

fn legacy_floating_comparator(data_type: &DataType) -> BoxedCmp {
    match data_type {
        DataType::Float(_) => Box::new(|left, right| {
            let left = f32::from_le_bytes(left.try_into().unwrap());
            let right = f32::from_le_bytes(right.try_into().unwrap());
            left.total_cmp(&right)
        }),
        DataType::Double(_) => Box::new(|left, right| {
            let left = f64::from_le_bytes(left.try_into().unwrap());
            let right = f64::from_le_bytes(right.try_into().unwrap());
            left.total_cmp(&right)
        }),
        _ => unreachable!("legacy floating comparator requires Float or Double"),
    }
}

async fn assert_legacy_floating_btree(
    file_name: &str,
    data_type: DataType,
    nan_keys: Vec<Vec<u8>>,
    nan_literals: Vec<Datum>,
    zero_key: Vec<u8>,
    zero_literal: Datum,
) {
    let mut rows = nan_keys
        .into_iter()
        .enumerate()
        .map(|(row_id, key)| (key, row_id as i64))
        .collect::<Vec<_>>();
    rows.push((zero_key, 3));
    let cmp = legacy_floating_comparator(&data_type);
    rows.sort_by(|left, right| cmp(&left.0, &right.0));
    let expected_first_key = rows.first().unwrap().0.clone();
    let expected_last_key = rows.last().unwrap().0.clone();

    let output = VecFileWrite::new();
    let captured = output.clone();
    let mut writer =
        BTreeIndexWriter::with_comparator(Box::new(output), 1, BlockCompressionType::None, cmp);
    for (key, row_id) in rows {
        writer.write(Some(&key), row_id).await.unwrap();
    }
    let write_result = writer.finish().await.unwrap();
    assert_eq!(write_result.meta.first_key, Some(expected_first_key));
    assert_eq!(write_result.meta.last_key, Some(expected_last_key));

    let tmp = tempfile::tempdir().unwrap();
    let index_dir = tmp.path().join("index");
    std::fs::create_dir_all(&index_dir).unwrap();
    std::fs::write(index_dir.join(file_name), captured.to_vec()).unwrap();
    let table_path = format!("file://{}", tmp.path().display());
    let file_io = crate::io::FileIOBuilder::new("file").build().unwrap();
    let entries = vec![make_global_index_entry(
        file_name,
        1,
        100,
        103,
        &write_result.meta,
    )];
    let fields = vec![DataField::new(1, "id".to_string(), data_type.clone())];
    let cases = [
        (
            PredicateOperator::Eq,
            vec![zero_literal.clone()],
            vec![RowRange::new(103, 103)],
        ),
        (
            PredicateOperator::Eq,
            vec![nan_literals[0].clone()],
            vec![RowRange::new(100, 100)],
        ),
        (
            PredicateOperator::In,
            vec![
                nan_literals[0].clone(),
                nan_literals[1].clone(),
                zero_literal,
            ],
            vec![RowRange::new(100, 101), RowRange::new(103, 103)],
        ),
    ];

    for (op, literals, expected) in cases {
        let predicates = vec![Predicate::Leaf {
            column: "id".to_string(),
            index: 0,
            data_type: data_type.clone(),
            op,
            literals,
        }];
        let result =
            evaluate_global_index_fast(&file_io, &table_path, &entries, &predicates, &fields)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(result, expected, "{file_name}: {op}");
    }
}

#[tokio::test]
async fn test_evaluate_legacy_float_btree() {
    let nan_bits = [0xffc0_0001u32, 0xffc0_0010, 0xffff_1234];
    assert_legacy_floating_btree(
        "btree_float_legacy_rust.index",
        DataType::Float(crate::spec::FloatType::new()),
        nan_bits
            .iter()
            .map(|bits| bits.to_le_bytes().to_vec())
            .collect(),
        nan_bits
            .iter()
            .map(|bits| Datum::Float(f32::from_bits(*bits)))
            .collect(),
        0.0f32.to_le_bytes().to_vec(),
        Datum::Float(0.0),
    )
    .await;
}

#[tokio::test]
async fn test_evaluate_legacy_double_btree() {
    let nan_bits = [
        0xfff8_0000_0000_0001u64,
        0xfff8_0000_0000_0010,
        0xffff_1234_5678_9abc,
    ];
    assert_legacy_floating_btree(
        "btree_double_legacy_rust.index",
        DataType::Double(crate::spec::DoubleType::new()),
        nan_bits
            .iter()
            .map(|bits| bits.to_le_bytes().to_vec())
            .collect(),
        nan_bits
            .iter()
            .map(|bits| Datum::Double(f64::from_bits(*bits)))
            .collect(),
        0.0f64.to_le_bytes().to_vec(),
        Datum::Double(0.0),
    )
    .await;
}

#[tokio::test]
async fn test_evaluate_java_bitmap_golden_index_string_fallback_scan() {
    let data_type = DataType::VarChar(crate::spec::VarCharType::string_type());
    let (file_io, table_path, file_name, meta, tmp) = setup_java_bitmap_testdata_table();
    let file_size = std::fs::metadata(tmp.path().join("index").join(&file_name))
        .unwrap()
        .len() as i64;
    let mut entry =
        make_global_index_entry_with_type(BITMAP_GLOBAL_INDEX_TYPE, &file_name, 1, 100, 109, &meta);
    entry.index_file.file_size = file_size;
    let entries = vec![entry];
    let fields = string_schema_fields();

    let ends_with_predicates = vec![Predicate::Leaf {
        column: "name".to_string(),
        index: 0,
        data_type: data_type.clone(),
        op: PredicateOperator::EndsWith,
        literals: vec![Datum::String("ta".to_string())],
    }];
    let ends_with_result = evaluate_global_index_fast(
        &file_io,
        &table_path,
        &entries,
        &ends_with_predicates,
        &fields,
    )
    .await
    .unwrap();
    assert_eq!(
        ends_with_result.unwrap(),
        vec![RowRange::new(101, 101), RowRange::new(103, 103)]
    );

    let contains_predicates = vec![Predicate::Leaf {
        column: "name".to_string(),
        index: 0,
        data_type: data_type.clone(),
        op: PredicateOperator::Contains,
        literals: vec![Datum::String("ph".to_string())],
    }];
    let contains_result = evaluate_global_index_fast(
        &file_io,
        &table_path,
        &entries,
        &contains_predicates,
        &fields,
    )
    .await
    .unwrap();
    assert_eq!(
        contains_result.unwrap(),
        vec![RowRange::new(100, 100), RowRange::new(102, 102)]
    );

    let like_predicates = vec![Predicate::Leaf {
        column: "name".to_string(),
        index: 0,
        data_type: data_type.clone(),
        op: PredicateOperator::Like,
        literals: vec![Datum::String("%ha%".to_string())],
    }];
    let like_result =
        evaluate_global_index_fast(&file_io, &table_path, &entries, &like_predicates, &fields)
            .await
            .unwrap();
    assert_eq!(
        like_result.unwrap(),
        vec![RowRange::new(100, 100), RowRange::new(102, 102)]
    );

    let less_than_predicates = vec![Predicate::Leaf {
        column: "name".to_string(),
        index: 0,
        data_type: data_type.clone(),
        op: PredicateOperator::Lt,
        literals: vec![Datum::String("delta".to_string())],
    }];
    let less_than_result = evaluate_global_index_fast(
        &file_io,
        &table_path,
        &entries,
        &less_than_predicates,
        &fields,
    )
    .await
    .unwrap();
    assert_eq!(less_than_result.unwrap(), vec![RowRange::new(100, 102)]);

    let mut over_limit_entry =
        make_global_index_entry_with_type(BITMAP_GLOBAL_INDEX_TYPE, &file_name, 1, 100, 109, &meta);
    over_limit_entry.index_file.file_size = file_size;
    let over_limit_entries = vec![over_limit_entry];
    let over_limit_less_than = evaluate_global_index_fast_with_fallback_size(
        &file_io,
        &table_path,
        &over_limit_entries,
        &less_than_predicates,
        &fields,
        i64::MAX,
        file_size - 1,
    )
    .await
    .unwrap();
    assert!(
        over_limit_less_than.is_none(),
        "range predicates require fallback dictionary scans and should be unsupported over budget"
    );

    let no_match_contains = vec![Predicate::Leaf {
        column: "name".to_string(),
        index: 0,
        data_type: data_type.clone(),
        op: PredicateOperator::Contains,
        literals: vec![Datum::String("zz".to_string())],
    }];
    let over_limit_result = evaluate_global_index_fast_with_fallback_size(
        &file_io,
        &table_path,
        &over_limit_entries,
        &no_match_contains,
        &fields,
        i64::MAX,
        file_size - 1,
    )
    .await
    .unwrap();
    assert!(
        over_limit_result.is_none(),
        "fallback scans over budget should be unsupported instead of returning full coverage"
    );

    let direct_with_over_limit_fallback = vec![Predicate::and(vec![
        Predicate::Leaf {
            column: "name".to_string(),
            index: 0,
            data_type: data_type.clone(),
            op: PredicateOperator::Eq,
            literals: vec![Datum::String("k2".to_string())],
        },
        Predicate::Leaf {
            column: "name".to_string(),
            index: 0,
            data_type,
            op: PredicateOperator::Contains,
            literals: vec![Datum::String("zz".to_string())],
        },
    ])];
    let direct_result = evaluate_global_index_fast_with_fallback_size(
        &file_io,
        &table_path,
        &over_limit_entries,
        &direct_with_over_limit_fallback,
        &fields,
        i64::MAX,
        file_size - 1,
    )
    .await
    .unwrap();
    assert_eq!(direct_result.unwrap(), vec![RowRange::new(105, 106)]);
}

#[tokio::test]
async fn test_btree_fallback_scan_over_limit_is_unsupported() {
    let (file_io, table_path, file_name, tmp) =
        setup_testdata_table("btree_varchar_100_no_compress.bin");
    let meta = BTreeIndexMeta::new(Some(b"a".to_vec()), Some(b"yyyy".to_vec()), false);
    let fields = string_schema_fields();
    let data_type = DataType::VarChar(crate::spec::VarCharType::string_type());
    let predicates = vec![Predicate::Leaf {
        column: "name".to_string(),
        index: 0,
        data_type,
        op: PredicateOperator::Contains,
        literals: vec![Datum::String("not-present".to_string())],
    }];

    let entries = vec![make_global_index_entry(&file_name, 1, 0, 99, &meta)];
    let exact_result = evaluate_global_index_fast_with_fallback_size(
        &file_io,
        &table_path,
        &entries,
        &predicates,
        &fields,
        i64::MAX,
        i64::MAX,
    )
    .await
    .unwrap();
    assert_eq!(exact_result.unwrap(), Vec::<RowRange>::new());

    let mut over_limit_entries = vec![make_global_index_entry(&file_name, 1, 0, 99, &meta)];
    over_limit_entries[0].index_file.file_size = 2;
    let over_limit_result = evaluate_global_index_fast_with_fallback_size(
        &file_io,
        &table_path,
        &over_limit_entries,
        &predicates,
        &fields,
        1,
        i64::MAX,
    )
    .await
    .unwrap();
    assert!(
        over_limit_result.is_none(),
        "fallback scans over budget should be unsupported instead of returning full coverage"
    );

    let second_file_name = "btree_varchar_100_no_compress_2.bin";
    std::fs::copy(
        tmp.path().join("index").join(&file_name),
        tmp.path().join("index").join(second_file_name),
    )
    .unwrap();
    let mut first = make_global_index_entry(&file_name, 1, 0, 99, &meta);
    first.index_file.file_size = 1;
    let mut second = make_global_index_entry(second_file_name, 1, 100, 199, &meta);
    second.index_file.file_size = 1;
    let total_over_limit_result = evaluate_global_index_fast_with_fallback_size(
        &file_io,
        &table_path,
        &[first, second],
        &predicates,
        &fields,
        1,
        i64::MAX,
    )
    .await
    .unwrap();
    assert!(
        total_over_limit_result.is_none(),
        "fallback budget should use selected files' total size, not per-file size"
    );
}

#[tokio::test]
async fn test_fallback_scan_over_limit_with_mixed_index_kinds_is_unsupported() {
    let (file_io, table_path, file_name, _tmp) =
        setup_testdata_table("btree_varchar_100_no_compress.bin");
    let btree_meta = BTreeIndexMeta::new(Some(b"a".to_vec()), Some(b"yyyy".to_vec()), false);
    let bitmap_meta = BTreeIndexMeta::new(Some(b"m".to_vec()), Some(b"z".to_vec()), false);
    let fields = string_schema_fields();
    let predicates = vec![Predicate::Leaf {
        column: "name".to_string(),
        index: 0,
        data_type: DataType::VarChar(crate::spec::VarCharType::string_type()),
        op: PredicateOperator::Lt,
        literals: vec![Datum::String("delta".to_string())],
    }];

    let mut btree = make_global_index_entry_with_type(
        BTREE_GLOBAL_INDEX_TYPE,
        &file_name,
        1,
        0,
        99,
        &btree_meta,
    );
    btree.index_file.file_size = 2;
    let mut bitmap = make_global_index_entry_with_type(
        BITMAP_GLOBAL_INDEX_TYPE,
        "bitmap-no-match.index",
        1,
        100,
        199,
        &bitmap_meta,
    );
    bitmap.index_file.file_size = 1;

    let result = evaluate_global_index_fast_with_fallback_size(
        &file_io,
        &table_path,
        &[btree, bitmap],
        &predicates,
        &fields,
        1,
        i64::MAX,
    )
    .await
    .unwrap();
    assert!(
            result.is_none(),
            "an over-budget selected BTree file must stay unsupported even if bitmap files are pruned by metadata"
        );
}

#[tokio::test]
async fn test_fallback_preflight_happens_before_shard_io() {
    let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
    let table_path = "memory:/missing-index-files";
    let meta = BTreeIndexMeta::new(Some(b"a".to_vec()), Some(b"z".to_vec()), false);
    let fields = string_schema_fields();
    let predicates = vec![Predicate::Leaf {
        column: "name".to_string(),
        index: 0,
        data_type: DataType::VarChar(crate::spec::VarCharType::string_type()),
        op: PredicateOperator::Contains,
        literals: vec![Datum::String("middle".to_string())],
    }];

    let mut btree = make_global_index_entry_with_type(
        BTREE_GLOBAL_INDEX_TYPE,
        "missing-btree.index",
        1,
        0,
        99,
        &meta,
    );
    btree.index_file.file_size = 1;
    let mut bitmap = make_global_index_entry_with_type(
        BITMAP_GLOBAL_INDEX_TYPE,
        "missing-bitmap.index",
        1,
        100,
        199,
        &meta,
    );
    bitmap.index_file.file_size = 1;

    let result = evaluate_global_index_fast_with_fallback_size(
        &file_io,
        table_path,
        &[btree, bitmap],
        &predicates,
        &fields,
        1,
        0,
    )
    .await
    .expect("fallback must be decided before opening an earlier shard");

    assert!(result.is_none());
}

#[tokio::test]
async fn test_evaluate_global_index_full_mode_includes_unindexed_tail() {
    let (file_io, table_path, file_name, _tmp) =
        setup_testdata_table("btree_int_100_no_compress.bin");
    let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);
    let entries = vec![make_global_index_entry(&file_name, 1, 0, 99, &meta)];
    let fields = int_schema_fields();
    let predicates = vec![int_eq("id", 0, 50)];

    let result = super::evaluate_global_index(super::GlobalIndexEvaluation {
        file_io: &file_io,
        table_path: &table_path,
        index_entries: &entries,
        predicates: &predicates,
        schema_fields: &fields,
        search_mode: GlobalIndexSearchMode::Full,
        global_index_thread_num: 32,
        btree_fallback_scan_max_size: i64::MAX,
        bitmap_fallback_scan_max_size: i64::MAX,
        fm_read_options: FMReadOptions::default(),
        next_row_id: Some(150),
        data_ranges: &[],
    })
    .await
    .unwrap();

    assert_eq!(
        result.unwrap(),
        vec![RowRange::new(25, 25), RowRange::new(100, 149)]
    );
}

#[tokio::test]
async fn test_evaluate_global_index_and_uses_evaluated_field_coverage_for_raw_fallback() {
    let src = format!(
        "{}/testdata/btree/btree_int_100_no_compress.bin",
        env!("CARGO_MANIFEST_DIR")
    );
    let tmp = tempfile::tempdir().unwrap();
    let index_dir = tmp.path().join("index");
    std::fs::create_dir_all(&index_dir).unwrap();
    std::fs::copy(&src, index_dir.join("index_part1.bin")).unwrap();
    std::fs::copy(&src, index_dir.join("index_part2.bin")).unwrap();

    let table_path = format!("file://{}", tmp.path().display());
    let file_io = crate::io::FileIOBuilder::new("file").build().unwrap();
    let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);

    let mut first = make_global_index_entry("index_part1.bin", 1, 0, 49, &meta);
    first
        .index_file
        .global_index_meta
        .as_mut()
        .unwrap()
        .extra_field_ids = Some(vec![2]);
    let second = make_global_index_entry("index_part2.bin", 1, 50, 99, &meta);
    let entries = vec![first, second];
    let fields = two_field_schema_fields();

    let predicates = vec![Predicate::and(vec![
        int_eq("id", 0, 50),
        int_eq("value", 1, 8),
    ])];
    let result = super::evaluate_global_index(super::GlobalIndexEvaluation {
        file_io: &file_io,
        table_path: &table_path,
        index_entries: &entries,
        predicates: &predicates,
        schema_fields: &fields,
        search_mode: GlobalIndexSearchMode::Full,
        global_index_thread_num: 32,
        btree_fallback_scan_max_size: i64::MAX,
        bitmap_fallback_scan_max_size: i64::MAX,
        fm_read_options: FMReadOptions::default(),
        next_row_id: Some(100),
        data_ranges: &[],
    })
    .await
    .unwrap();

    assert_eq!(
        result.unwrap(),
        vec![RowRange::new(25, 25), RowRange::new(75, 75)],
        "raw fallback should use only the id field that was actually evaluated; \
             the unevaluated extra field must not widen or narrow fallback coverage"
    );
}

#[tokio::test]
async fn test_evaluate_global_index_detail_mode_uses_data_ranges() {
    let (file_io, table_path, file_name, _tmp) =
        setup_testdata_table("btree_int_100_no_compress.bin");
    let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);
    let entries = vec![make_global_index_entry(&file_name, 1, 0, 99, &meta)];
    let fields = int_schema_fields();
    let predicates = vec![int_eq("id", 0, 50)];

    let data_ranges = [RowRange::new(90, 120), RowRange::new(140, 145)];
    let result = super::evaluate_global_index(super::GlobalIndexEvaluation {
        file_io: &file_io,
        table_path: &table_path,
        index_entries: &entries,
        predicates: &predicates,
        schema_fields: &fields,
        search_mode: GlobalIndexSearchMode::Detail,
        global_index_thread_num: 32,
        btree_fallback_scan_max_size: i64::MAX,
        bitmap_fallback_scan_max_size: i64::MAX,
        fm_read_options: FMReadOptions::default(),
        next_row_id: Some(150),
        data_ranges: &data_ranges,
    })
    .await
    .unwrap();

    assert_eq!(
        result.unwrap(),
        vec![
            RowRange::new(25, 25),
            RowRange::new(100, 120),
            RowRange::new(140, 145),
        ]
    );
}

#[tokio::test]
async fn test_evaluate_global_index_range() {
    let (file_io, table_path, file_name, _tmp) =
        setup_testdata_table("btree_int_100_no_compress.bin");
    let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);
    let entries = vec![make_global_index_entry(&file_name, 1, 0, 99, &meta)];
    let fields = int_schema_fields();

    // keys 10..=20 -> keys 10,12,14,16,18,20 -> row_ids 5,6,7,8,9,10
    let predicates = vec![
        Predicate::Leaf {
            column: "id".to_string(),
            index: 0,
            data_type: DataType::Int(crate::spec::IntType::new()),
            op: PredicateOperator::GtEq,
            literals: vec![Datum::Int(10)],
        },
        Predicate::Leaf {
            column: "id".to_string(),
            index: 0,
            data_type: DataType::Int(crate::spec::IntType::new()),
            op: PredicateOperator::LtEq,
            literals: vec![Datum::Int(20)],
        },
    ];

    let result = evaluate_global_index_fast(&file_io, &table_path, &entries, &predicates, &fields)
        .await
        .unwrap();
    let ranges = result.unwrap();
    assert_eq!(ranges, vec![RowRange::new(5, 10)]);

    let mut over_limit_entries = vec![make_global_index_entry(&file_name, 1, 0, 99, &meta)];
    over_limit_entries[0].index_file.file_size = 2;
    let over_limit_result = evaluate_global_index_fast_with_fallback_size(
        &file_io,
        &table_path,
        &over_limit_entries,
        &predicates,
        &fields,
        1,
        i64::MAX,
    )
    .await
    .unwrap();
    assert!(
        over_limit_result.is_none(),
        "between/range predicates require fallback scans and should be unsupported over budget"
    );
}

#[tokio::test]
async fn test_evaluate_global_index_in() {
    let (file_io, table_path, file_name, _tmp) =
        setup_testdata_table("btree_int_100_no_compress.bin");
    let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);
    let entries = vec![make_global_index_entry(&file_name, 1, 0, 99, &meta)];
    let fields = int_schema_fields();

    // IN(0, 50, 198) -> row_ids 0, 25, 99
    let predicates = vec![Predicate::Leaf {
        column: "id".to_string(),
        index: 0,
        data_type: DataType::Int(crate::spec::IntType::new()),
        op: PredicateOperator::In,
        literals: vec![Datum::Int(0), Datum::Int(50), Datum::Int(198)],
    }];

    let result = evaluate_global_index_fast(&file_io, &table_path, &entries, &predicates, &fields)
        .await
        .unwrap();
    let ranges = result.unwrap();
    assert_eq!(
        ranges,
        vec![
            RowRange::new(0, 0),
            RowRange::new(25, 25),
            RowRange::new(99, 99)
        ]
    );
}

#[tokio::test]
async fn test_evaluate_global_index_no_match() {
    let (file_io, table_path, file_name, _tmp) =
        setup_testdata_table("btree_int_100_no_compress.bin");
    let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);
    let entries = vec![make_global_index_entry(&file_name, 1, 0, 99, &meta)];
    let fields = int_schema_fields();

    // key=999 doesn't exist
    let predicates = vec![Predicate::Leaf {
        column: "id".to_string(),
        index: 0,
        data_type: DataType::Int(crate::spec::IntType::new()),
        op: PredicateOperator::Eq,
        literals: vec![Datum::Int(999)],
    }];

    let result = evaluate_global_index_fast(&file_io, &table_path, &entries, &predicates, &fields)
        .await
        .unwrap();
    let ranges = result.unwrap();
    assert!(ranges.is_empty());
}

#[tokio::test]
async fn test_evaluate_global_index_with_row_offset() {
    let (file_io, table_path, file_name, _tmp) =
        setup_testdata_table("btree_int_100_no_compress.bin");
    let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);
    // row_range_start=1000 simulates an offset
    let entries = vec![make_global_index_entry(&file_name, 1, 1000, 1099, &meta)];
    let fields = int_schema_fields();

    // key=50 -> local row_id=25, offset -> global row_id=1025
    let predicates = vec![Predicate::Leaf {
        column: "id".to_string(),
        index: 0,
        data_type: DataType::Int(crate::spec::IntType::new()),
        op: PredicateOperator::Eq,
        literals: vec![Datum::Int(50)],
    }];

    let result = evaluate_global_index_fast(&file_io, &table_path, &entries, &predicates, &fields)
        .await
        .unwrap();
    let ranges = result.unwrap();
    assert_eq!(ranges, vec![RowRange::new(1025, 1025)]);
}

#[tokio::test]
async fn test_evaluate_global_index_unknown_column() {
    let (file_io, table_path, file_name, _tmp) =
        setup_testdata_table("btree_int_100_no_compress.bin");
    let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);
    let entries = vec![make_global_index_entry(&file_name, 1, 0, 99, &meta)];
    let fields = int_schema_fields();

    // Column "unknown" not in schema -> None (can't evaluate)
    let predicates = vec![Predicate::Leaf {
        column: "unknown".to_string(),
        index: 0,
        data_type: DataType::Int(crate::spec::IntType::new()),
        op: PredicateOperator::Eq,
        literals: vec![Datum::Int(50)],
    }];

    let result = evaluate_global_index_fast(&file_io, &table_path, &entries, &predicates, &fields)
        .await
        .unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_evaluate_global_index_multi_field_and() {
    // Two fields, each with its own btree index file (same data, different field_id).
    // btree_int_100_no_compress.bin: keys 0,2,4,...,198 -> row_ids 0,1,...,99
    let src = format!(
        "{}/testdata/btree/btree_int_100_no_compress.bin",
        env!("CARGO_MANIFEST_DIR")
    );
    let tmp = tempfile::tempdir().unwrap();
    let index_dir = tmp.path().join("index");
    std::fs::create_dir_all(&index_dir).unwrap();
    std::fs::copy(&src, index_dir.join("index_field1.bin")).unwrap();
    std::fs::copy(&src, index_dir.join("index_field2.bin")).unwrap();

    let table_path = format!("file://{}", tmp.path().display());
    let file_io = crate::io::FileIOBuilder::new("file").build().unwrap();

    let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);

    let fields = vec![
        DataField::new(
            1,
            "id".to_string(),
            DataType::Int(crate::spec::IntType::new()),
        ),
        DataField::new(
            2,
            "value".to_string(),
            DataType::Int(crate::spec::IntType::new()),
        ),
    ];

    let entries = vec![
        make_global_index_entry("index_field1.bin", 1, 0, 99, &meta),
        make_global_index_entry("index_field2.bin", 2, 0, 99, &meta),
    ];

    // id >= 40 AND id <= 60 → keys 40,42,...,60 → row_ids 20..30
    // value >= 44 AND value <= 52 → keys 44,46,48,50,52 → row_ids 22..26
    // AND → intersect [20..30] and [22..26] = [22..26]
    let predicates = vec![
        Predicate::Leaf {
            column: "id".to_string(),
            index: 0,
            data_type: DataType::Int(crate::spec::IntType::new()),
            op: PredicateOperator::GtEq,
            literals: vec![Datum::Int(40)],
        },
        Predicate::Leaf {
            column: "id".to_string(),
            index: 0,
            data_type: DataType::Int(crate::spec::IntType::new()),
            op: PredicateOperator::LtEq,
            literals: vec![Datum::Int(60)],
        },
        Predicate::Leaf {
            column: "value".to_string(),
            index: 1,
            data_type: DataType::Int(crate::spec::IntType::new()),
            op: PredicateOperator::GtEq,
            literals: vec![Datum::Int(44)],
        },
        Predicate::Leaf {
            column: "value".to_string(),
            index: 1,
            data_type: DataType::Int(crate::spec::IntType::new()),
            op: PredicateOperator::LtEq,
            literals: vec![Datum::Int(52)],
        },
    ];

    let result = evaluate_global_index_fast(&file_io, &table_path, &entries, &predicates, &fields)
        .await
        .unwrap();
    let ranges = result.unwrap();
    assert_eq!(ranges, vec![RowRange::new(22, 26)]);
}

#[tokio::test]
async fn test_multi_field_and_shares_query_concurrency_budget() {
    let src = format!(
        "{}/testdata/btree/btree_int_100_no_compress.bin",
        env!("CARGO_MANIFEST_DIR")
    );
    let tmp = tempfile::tempdir().unwrap();
    let index_dir = tmp.path().join("index");
    std::fs::create_dir_all(&index_dir).unwrap();
    let file_names: Vec<_> = (1..=4)
        .map(|field_id| {
            let file_name = format!("index_field{field_id}.bin");
            std::fs::copy(&src, index_dir.join(&file_name)).unwrap();
            file_name
        })
        .collect();

    let table_path = format!("file://{}", tmp.path().display());
    let file_io = crate::io::FileIOBuilder::new("file").build().unwrap();
    let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);
    let fields: Vec<_> = (0..4)
        .map(|index| {
            let field_id = index + 1;
            DataField::new(
                field_id,
                format!("field{field_id}"),
                DataType::Int(crate::spec::IntType::new()),
            )
        })
        .collect();
    let entries: Vec<_> = file_names
        .iter()
        .enumerate()
        .map(|(index, file_name)| {
            make_global_index_entry(file_name, index as i32 + 1, 0, 99, &meta)
        })
        .collect();
    let predicate = Predicate::and(
        (0..4)
            .map(|index| int_eq(&format!("field{}", index + 1), index, 50))
            .collect(),
    );

    for (thread_num, expected_peak) in [(1, 1), (2, 2)] {
        let mut scanner = GlobalIndexScanner::create(
            &file_io,
            &table_path,
            thread_num,
            i64::MAX,
            i64::MAX,
            &entries,
            &fields,
        )
        .unwrap()
        .unwrap();
        let probe = Arc::new(QueryIoProbe::default());
        scanner.query_io_probe = Some(Arc::clone(&probe));

        let result = scanner.evaluate(&predicate).await.unwrap().unwrap();

        assert_eq!(result.row_ranges, vec![RowRange::new(25, 25)]);
        assert_eq!(result.evaluated_field_ids, HashSet::from([1, 2, 3, 4]));
        assert_eq!(probe.peak(), expected_peak);
    }
}

/// Regression for the Between+remaining bug in `evaluate_leaf`. When a
/// native `Between` leaf is paired with another conjunct (e.g. `id >= 0`),
/// and the file's b-tree key range falls **outside** the Between range
/// but is still matched by the remaining predicate, the whole AND must
/// produce zero rows. Before the fix, `file_result` was initialized from
/// the remaining predicate's bitmap and the Between conjunct was silently
/// dropped — the test would observe the file's full row id set instead of
/// the empty set.
#[tokio::test]
async fn test_between_unmatched_file_drops_remaining_match() {
    let (file_io, table_path, file_name, _tmp) =
        setup_testdata_table("btree_int_100_no_compress.bin");
    // File covers keys [0, 198] (row_ids 0..99). Pick a Between range
    // entirely below 0 so `may_match_between` is false, and a `>= 0`
    // conjunct that would otherwise scoop up every row in the file.
    let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);
    let entries = vec![make_global_index_entry(&file_name, 1, 0, 99, &meta)];
    let fields = int_schema_fields();

    let predicates = vec![Predicate::and(vec![
        Predicate::Leaf {
            column: "id".to_string(),
            index: 0,
            data_type: DataType::Int(crate::spec::IntType::new()),
            op: PredicateOperator::Between,
            literals: vec![Datum::Int(-100), Datum::Int(-50)],
        },
        Predicate::Leaf {
            column: "id".to_string(),
            index: 0,
            data_type: DataType::Int(crate::spec::IntType::new()),
            op: PredicateOperator::GtEq,
            literals: vec![Datum::Int(0)],
        },
    ])];

    let result = evaluate_global_index_fast(&file_io, &table_path, &entries, &predicates, &fields)
        .await
        .unwrap();
    let ranges = result.unwrap();
    assert!(
        ranges.is_empty(),
        "Between(-100..-50) AND id>=0 must produce zero rows on a file \
             whose key range is [0, 198] — got {ranges:?}"
    );
}
