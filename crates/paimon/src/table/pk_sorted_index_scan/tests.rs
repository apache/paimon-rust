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
use crate::btree::{
    make_key_comparator, serialize_datum, test_util::VecFileWrite, BTreeIndexWriter,
    BlockCompressionType,
};
use crate::catalog::Identifier;
use crate::io::FileIOBuilder;
use crate::spec::{
    stats::BinaryTableStats, BinaryRow, DataType, Datum, GlobalIndexMeta, IndexFileMeta, IntType,
    PredicateBuilder, Schema, TableSchema,
};
use crate::table::bitmap_global_index_format::{
    make_bitmap_key_comparator, serialize_bitmap_datum,
};
use crate::table::bitmap_global_index_writer::BitmapGlobalIndexWriter;
use crate::table::{DataSplitBuilder, DeletionFile};

fn table(extra: &[(&str, &str)]) -> Table {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("a", DataType::Int(IntType::new()))
        .column("b", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("pk-btree.index.columns", "a")
        .option("pk-bitmap.index.columns", "b")
        .options(extra.iter().map(|(k, v)| (k.to_string(), v.to_string())))
        .build()
        .unwrap();
    Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("db", "t"),
        "memory:/table".into(),
        TableSchema::new(0, &schema),
        None,
    )
}

fn file(name: &str, rows: i64) -> DataFileMeta {
    DataFileMeta {
        file_name: name.into(),
        file_size: 100,
        row_count: rows,
        min_key: vec![],
        max_key: vec![],
        key_stats: BinaryTableStats::new(vec![], vec![], vec![]),
        value_stats: BinaryTableStats::new(vec![], vec![], vec![]),
        min_sequence_number: 0,
        max_sequence_number: 0,
        schema_id: 0,
        level: 1,
        extra_files: vec![],
        creation_time: None,
        delete_row_count: Some(0),
        embedded_index: None,
        file_source: Some(1),
        value_stats_cols: None,
        external_path: None,
        first_row_id: None,
        write_cols: None,
        column_max_sequence_numbers: None,
    }
}

fn split(files: Vec<DataFileMeta>, raw: bool) -> DataSplit {
    DataSplitBuilder::new()
        .with_snapshot(7)
        .with_partition(BinaryRow::new(0))
        .with_bucket(0)
        .with_bucket_path("memory:/table/bucket-0".into())
        .with_total_buckets(1)
        .with_data_files(files)
        .with_raw_convertible(raw)
        .build()
        .unwrap()
}

fn source_meta(files: &[(&str, i64)], level: i32) -> Vec<u8> {
    let mut bytes = vec![];
    bytes.extend_from_slice(&1i32.to_be_bytes());
    bytes.extend_from_slice(&level.to_be_bytes());
    bytes.extend_from_slice(&(files.len() as i32).to_be_bytes());
    for (name, count) in files {
        assert!(name.is_ascii());
        bytes.extend_from_slice(&(name.len() as u16).to_be_bytes());
        bytes.extend_from_slice(name.as_bytes());
        bytes.extend_from_slice(&count.to_be_bytes());
    }
    bytes
}

async fn payload(
    table: &Table,
    field: &str,
    sources: &[(&str, i64)],
    values: &[Option<i32>],
    name: &str,
    path: &str,
) -> IndexManifestEntry {
    let field = table
        .schema()
        .fields()
        .iter()
        .find(|f| f.name() == field)
        .unwrap();
    let ty = field.data_type();
    let output = VecFileWrite::new();
    let (meta, row_count) = if field.name() == "a" {
        let mut writer = BTreeIndexWriter::with_comparator(
            Box::new(output.clone()),
            256,
            BlockCompressionType::None,
            make_key_comparator(ty),
        );
        let mut sorted_values: Vec<_> = values.iter().enumerate().collect();
        sorted_values.sort_by_key(|(_, value)| **value);
        for (position, value) in sorted_values {
            let key = value.map(|v| serialize_datum(&Datum::Int(v), ty));
            writer.write(key.as_deref(), position as i64).await.unwrap();
        }
        let result = writer.finish().await.unwrap();
        (result.meta, result.row_count as i64)
    } else {
        let mut writer = BitmapGlobalIndexWriter::new(
            Box::new(output.clone()),
            256,
            BlockCompressionType::None,
            make_bitmap_key_comparator(ty),
        );
        for (position, value) in values.iter().enumerate() {
            let key = value.map(|v| serialize_bitmap_datum(&Datum::Int(v), ty));
            writer.write(key.as_deref(), position as i64).unwrap();
        }
        let result = writer.finish().await.unwrap();
        (result.meta, result.row_count as i64)
    };
    let bytes = output.to_vec();
    table
        .file_io()
        .new_output(path)
        .unwrap()
        .write(bytes::Bytes::from(bytes.clone()))
        .await
        .unwrap();
    IndexManifestEntry {
        version: 1,
        kind: FileKind::Add,
        partition: BinaryRow::new(0).to_serialized_bytes(),
        bucket: 0,
        index_file: IndexFileMeta {
            index_type: if field.name() == "a" {
                "btree"
            } else {
                "bitmap"
            }
            .into(),
            file_name: name.into(),
            file_size: bytes.len() as i64,
            row_count,
            deletion_vectors_ranges: None,
            external_path: None,
            global_index_meta: Some(GlobalIndexMeta {
                row_range_start: 0,
                row_range_end: row_count - 1,
                index_field_id: field.id(),
                extra_field_ids: None,
                index_meta: Some(meta.serialize()),
                source_meta: Some(source_meta(sources, 1)),
            }),
        },
    }
}

async fn run(
    table: &Table,
    splits: Vec<DataSplit>,
    entries: &[IndexManifestEntry],
    predicate: Predicate,
) -> Vec<DataSplit> {
    let defs = definitions(table.schema().fields(), table.schema().options()).unwrap();
    refine(table, splits, entries, &defs, &[predicate])
        .await
        .unwrap()
}

fn equal(table: &Table, column: &str, value: i32) -> Predicate {
    PredicateBuilder::new(table.schema().fields())
        .equal(column, Datum::Int(value))
        .unwrap()
}

#[tokio::test]
async fn retired_sources_keep_offsets_and_uncovered_sources_scan() {
    let table = table(&[]);
    let entry = payload(
        &table,
        "a",
        &[("a-retired", 2), ("b-active", 3)],
        &[Some(9), Some(9), Some(1), Some(9), Some(1)],
        "a.idx",
        "memory:/table/index/a.idx",
    )
    .await;
    let result = run(
        &table,
        vec![split(vec![file("b-active", 3), file("c-new", 2)], true)],
        &[entry],
        equal(&table, "a", 9),
    )
    .await;
    assert_eq!(result.len(), 2);
    assert_eq!(
        result[0].row_ranges(),
        Some([RowRange::new(1, 1)].as_slice())
    );
    assert_eq!(result[1].row_ranges(), None);
    assert!(result.iter().all(|s| !s.raw_convertible()));
}

#[tokio::test]
async fn predicates_combine_after_each_fields_source_offset_is_removed() {
    let table = table(&[]);
    let a = payload(
        &table,
        "a",
        &[("a-retired", 1), ("c-active", 3)],
        &[Some(5), Some(5), Some(5), Some(0)],
        "a.idx",
        "memory:/table/index/a.idx",
    )
    .await;
    let b = payload(
        &table,
        "b",
        &[("b-retired", 2), ("c-active", 3)],
        &[Some(5), Some(5), Some(0), Some(5), Some(5)],
        "b.idx",
        "memory:/table/index/b.idx",
    )
    .await;
    let input = vec![split(vec![file("c-active", 3)], true)];
    let entries = [a, b];
    let and = run(
        &table,
        input.clone(),
        &entries,
        Predicate::and(vec![equal(&table, "a", 5), equal(&table, "b", 5)]),
    )
    .await;
    assert_eq!(and[0].row_ranges(), Some([RowRange::new(1, 1)].as_slice()));
    let or = run(
        &table,
        input.clone(),
        &entries,
        Predicate::or(vec![equal(&table, "a", 5), equal(&table, "b", 5)]),
    )
    .await;
    assert_eq!(or[0].row_ranges(), Some([RowRange::new(0, 2)].as_slice()));
    let unsupported_or = run(
        &table,
        input,
        &entries,
        Predicate::or(vec![equal(&table, "a", 5), equal(&table, "id", 8)]),
    )
    .await;
    assert_eq!(unsupported_or[0].row_ranges(), None);
}

#[tokio::test]
async fn non_raw_merge_inputs_stay_whole_even_when_index_returns_no_matches() {
    let table = table(&[]);
    let entry = payload(
        &table,
        "a",
        &[("old", 2)],
        &[Some(0), Some(0)],
        "a.idx",
        "memory:/table/index/a.idx",
    )
    .await;
    let mut newer = file("new", 1);
    newer.level = 0;
    newer.file_source = Some(0);
    let original = split(vec![file("old", 2), newer], false);
    let result = run(
        &table,
        vec![original.clone()],
        &[entry],
        equal(&table, "a", 7),
    )
    .await;
    assert_eq!(result, vec![original]);
}

#[tokio::test]
async fn index_paths_and_aligned_deletion_files_are_preserved() {
    for (bucket_local, external) in [(false, false), (true, false), (true, true)] {
        let table = table(&[(
            "index-file-in-data-file-dir",
            if bucket_local { "true" } else { "false" },
        )]);
        let path = if external {
            "memory:/outside/index"
        } else if bucket_local {
            "memory:/table/bucket-0/a.idx"
        } else {
            "memory:/table/index/a.idx"
        };
        let mut entry = payload(
            &table,
            "a",
            &[("a", 2), ("b", 2)],
            &[Some(0), Some(1), Some(1), Some(0)],
            "a.idx",
            path,
        )
        .await;
        if external {
            entry.index_file.external_path = Some(path.into());
        }
        let deletion = DeletionFile::new("memory:/dv".into(), 3, 9, Some(1));
        let input = DataSplitBuilder::new()
            .with_snapshot(7)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("memory:/table/bucket-0".into())
            .with_data_files(vec![file("a", 2), file("b", 2)])
            .with_data_deletion_files(vec![None, Some(deletion.clone())])
            .build()
            .unwrap();
        let result = run(&table, vec![input], &[entry], equal(&table, "a", 1)).await;
        assert_eq!(
            result[0].row_ranges(),
            Some([RowRange::new(1, 1)].as_slice())
        );
        assert_eq!(
            result[1].row_ranges(),
            Some([RowRange::new(0, 0)].as_slice())
        );
        assert_eq!(result[0].data_deletion_files(), Some([None].as_slice()));
        assert_eq!(
            result[1].data_deletion_files(),
            Some([Some(deletion)].as_slice())
        );
    }
}

#[tokio::test]
async fn malformed_or_ambiguous_coverage_never_drops_data() {
    let table = table(&[]);
    let entry = payload(
        &table,
        "a",
        &[("a", 2)],
        &[Some(1), Some(2)],
        "a.idx",
        "memory:/table/index/a.idx",
    )
    .await;
    let input = vec![split(vec![file("a", 2)], true)];
    for case in 0..12 {
        let mut broken = entry.clone();
        let mut active = input.clone();
        let meta = broken.index_file.global_index_meta.as_mut().unwrap();
        match case {
            0 => meta.source_meta = Some(source_meta(&[("a", 1)], 1)),
            1 => meta.source_meta = Some(source_meta(&[("retired", 2)], 1)),
            2 => meta.source_meta = Some(source_meta(&[("a", 1), ("a", 1)], 1)),
            3 => meta.source_meta = Some(source_meta(&[("z", 1), ("a", 1)], 1)),
            4 => meta.row_range_start = 1,
            5 => meta.row_range_end = 99,
            6 => broken.index_file.row_count = 1,
            7 => {
                active = vec![split(
                    vec![{
                        let mut f = file("a", 2);
                        f.file_source = Some(0);
                        f
                    }],
                    true,
                )]
            }
            8 => meta.source_meta = Some(source_meta(&[("a", 2)], 2)),
            9 => broken.bucket = 1,
            10 => broken.partition = BinaryRow::new(1).to_serialized_bytes(),
            11 => meta.index_field_id = 0,
            _ => unreachable!(),
        }
        assert_eq!(
            run(&table, active.clone(), &[broken], equal(&table, "a", 99)).await,
            active,
            "case {case}"
        );
    }
    let mut duplicate = entry.clone();
    duplicate.index_file.file_name = "other.idx".into();
    assert_eq!(
        run(
            &table,
            input.clone(),
            &[entry, duplicate],
            equal(&table, "a", 99)
        )
        .await,
        input
    );
}

#[tokio::test]
async fn corrupt_payload_and_unsupported_and_conjunct_fall_back_safely() {
    let table = table(&[]);
    let entry = payload(
        &table,
        "a",
        &[("a", 2)],
        &[Some(1), Some(2)],
        "a.idx",
        "memory:/table/index/a.idx",
    )
    .await;
    let input = vec![split(vec![file("a", 2)], true)];
    let and = run(
        &table,
        input.clone(),
        std::slice::from_ref(&entry),
        Predicate::and(vec![equal(&table, "a", 2), equal(&table, "id", 1)]),
    )
    .await;
    assert_eq!(and[0].row_ranges(), Some([RowRange::new(1, 1)].as_slice()));
    table
        .file_io()
        .new_output("memory:/table/index/a.idx")
        .unwrap()
        .write(bytes::Bytes::from_static(b"corrupt"))
        .await
        .unwrap();
    let result = run(&table, input, &[entry], equal(&table, "a", 2)).await;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].row_ranges(), None);
}

#[test]
fn per_column_read_options_follow_java_resolution_and_conflicts() {
    let table = table(&[(
        "fields.a.pk-btree.index.options",
        r#"{"fallback-scan-max-size":"17 kb","cache-size":"0"}"#,
    )]);
    let defs = definitions(table.schema().fields(), table.schema().options()).unwrap();
    let opts = CoreOptions::new(&defs[0].options);
    assert_eq!(
        opts.btree_index_fallback_scan_max_size().unwrap(),
        17 * 1024
    );
    assert_eq!(opts.btree_index_data_block_cache_size().unwrap(), 0);
    for json in [
        "[]",
        "null",
        r#"{"a":null}"#,
        r#"{" ":"v"}"#,
        r#"{"fallback-scan-max-size":"10 kb"}"#,
    ] {
        let options = Options::from([
            ("pk-btree.index.columns".into(), "a".into()),
            ("btree-index.fallback-scan-max-size".into(), "1 kb".into()),
            ("fields.a.pk-btree.index.options".into(), json.into()),
        ]);
        assert!(
            definitions(table.schema().fields(), &options).is_err(),
            "{json}"
        );
    }
    for options in [
        Options::from([("pk-btree.index.columns".into(), "a, a".into())]),
        Options::from([
            ("pk-btree.index.columns".into(), "a".into()),
            ("pk-bitmap.index.columns".into(), "a".into()),
        ]),
    ] {
        assert!(definitions(table.schema().fields(), &options).is_err());
    }
}

#[tokio::test]
async fn combined_bounds_respect_per_column_fallback_scan_budget() {
    for budget in ["0", "17 kb"] {
        let json = format!(r#"{{"fallback-scan-max-size":"{budget}"}}"#);
        let table = table(&[("fields.a.pk-btree.index.options", &json)]);
        let entry = payload(
            &table,
            "a",
            &[("a", 4)],
            &[Some(1), Some(2), Some(3), Some(4)],
            "a.idx",
            "memory:/table/index/a.idx",
        )
        .await;
        let pb = PredicateBuilder::new(table.schema().fields());
        let predicate = Predicate::and(vec![
            pb.greater_or_equal("a", Datum::Int(2)).unwrap(),
            pb.less_or_equal("a", Datum::Int(3)).unwrap(),
        ]);
        let result = run(
            &table,
            vec![split(vec![file("a", 4)], true)],
            &[entry],
            predicate,
        )
        .await;
        if budget == "0" {
            assert_eq!(result[0].row_ranges(), None);
        } else {
            assert_eq!(
                result[0].row_ranges(),
                Some([RowRange::new(1, 2)].as_slice())
            );
        }
    }
}

#[tokio::test]
async fn shared_query_is_cached_before_localizing_each_source() {
    let table = table(&[]);
    let entry = payload(
        &table,
        "b",
        &[("a", 2), ("b", 2)],
        &[Some(1), Some(0), Some(0), Some(1)],
        "b.idx",
        "memory:/table/index/b.idx",
    )
    .await;
    let defs = definitions(table.schema().fields(), table.schema().options()).unwrap();
    let input = split(vec![file("a", 2), file("b", 2)], true);
    let (groups, sources) = plan_groups(&table, std::slice::from_ref(&input), &[entry], &defs);
    let mut evaluation = Evaluation {
        table: &table,
        definitions: &defs,
        groups,
        sources,
    };
    let predicate = equal(&table, "b", 1);
    assert_eq!(
        evaluation
            .evaluate_field(&input, &input.data_files()[0], "b", &predicate)
            .await
            .unwrap(),
        Some(vec![RowRange::new(0, 0)])
    );
    table
        .file_io()
        .delete_file("memory:/table/index/b.idx")
        .await
        .unwrap();
    assert_eq!(
        evaluation
            .evaluate_field(&input, &input.data_files()[1], "b", &predicate)
            .await
            .unwrap(),
        Some(vec![RowRange::new(1, 1)])
    );
}

#[tokio::test]
async fn invalid_payload_positions_and_fragmentation_keep_full_file() {
    let table = table(&[]);
    let mut invalid = payload(
        &table,
        "b",
        &[("a", 2)],
        &[Some(0), Some(0), Some(1)],
        "bad.idx",
        "memory:/table/index/bad.idx",
    )
    .await;
    invalid.index_file.row_count = 2;
    invalid
        .index_file
        .global_index_meta
        .as_mut()
        .unwrap()
        .row_range_end = 1;
    let result = run(
        &table,
        vec![split(vec![file("a", 2)], true)],
        &[invalid],
        equal(&table, "b", 1),
    )
    .await;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].row_ranges(), None);
    for matching_rows in [MAX_POSITION_RANGES, MAX_POSITION_RANGES + 1] {
        let row_count = matching_rows * 2;
        let values: Vec<_> = (0..row_count).map(|i| Some((i % 2) as i32)).collect();
        let entry = payload(
            &table,
            "b",
            &[("a", row_count as i64)],
            &values,
            "fragmented.idx",
            "memory:/table/index/fragmented.idx",
        )
        .await;
        let result = run(
            &table,
            vec![split(vec![file("a", row_count as i64)], true)],
            &[entry],
            equal(&table, "b", 1),
        )
        .await;
        if matching_rows == MAX_POSITION_RANGES {
            assert_eq!(result[0].row_ranges().unwrap().len(), MAX_POSITION_RANGES);
        } else {
            assert_eq!(result[0].row_ranges(), None);
        }
    }
}

#[tokio::test]
async fn nulls_and_empty_matches_preserve_index_semantics() {
    let table = table(&[]);
    for column in ["a", "b"] {
        let name = format!("{column}.idx");
        let entry = payload(
            &table,
            column,
            &[("a", 4)],
            &[None, Some(1), None, Some(2)],
            &name,
            &format!("memory:/table/index/{name}"),
        )
        .await;
        let pb = PredicateBuilder::new(table.schema().fields());
        let result = run(
            &table,
            vec![split(vec![file("a", 4)], true)],
            std::slice::from_ref(&entry),
            pb.is_null(column).unwrap(),
        )
        .await;
        assert_eq!(
            result[0].row_ranges(),
            Some([RowRange::new(0, 0), RowRange::new(2, 2)].as_slice())
        );
        assert!(run(
            &table,
            vec![split(vec![file("a", 4)], true)],
            &[entry],
            equal(&table, column, 99)
        )
        .await
        .is_empty());
    }
}

#[tokio::test]
async fn positions_at_java_integer_max_are_allowed_but_larger_positions_fall_back() {
    let table = table(&[]);
    let ty = table.schema().fields()[2].data_type();
    for position in [i64::from(i32::MAX), i64::from(i32::MAX) + 1] {
        let row_count = position + 1;
        let output = VecFileWrite::new();
        let mut writer = BitmapGlobalIndexWriter::new(
            Box::new(output.clone()),
            256,
            BlockCompressionType::None,
            make_bitmap_key_comparator(ty),
        );
        // Sparse postings exercise the position boundary with a few bytes,
        // without allocating a multi-billion-row data file.
        writer
            .write(Some(&serialize_bitmap_datum(&Datum::Int(0), ty)), 0)
            .unwrap();
        writer
            .write(Some(&serialize_bitmap_datum(&Datum::Int(1), ty)), position)
            .unwrap();
        let result = writer
            .finish_with_source_row_count(row_count as u64)
            .await
            .unwrap();
        let bytes = output.to_vec();
        table
            .file_io()
            .new_output("memory:/table/index/large.idx")
            .unwrap()
            .write(bytes::Bytes::from(bytes.clone()))
            .await
            .unwrap();
        let entry = IndexManifestEntry {
            version: 1,
            kind: FileKind::Add,
            partition: BinaryRow::new(0).to_serialized_bytes(),
            bucket: 0,
            index_file: IndexFileMeta {
                index_type: "bitmap".into(),
                file_name: "large.idx".into(),
                file_size: bytes.len() as i64,
                row_count,
                deletion_vectors_ranges: None,
                external_path: None,
                global_index_meta: Some(GlobalIndexMeta {
                    row_range_start: 0,
                    row_range_end: row_count - 1,
                    index_field_id: 2,
                    extra_field_ids: None,
                    index_meta: Some(result.meta.serialize()),
                    source_meta: Some(source_meta(&[("a", row_count)], 1)),
                }),
            },
        };
        let selected = run(
            &table,
            vec![split(vec![file("a", row_count)], true)],
            &[entry],
            equal(&table, "b", 1),
        )
        .await;
        if position == i64::from(i32::MAX) {
            assert_eq!(
                selected[0].row_ranges(),
                Some([RowRange::new(position, position)].as_slice())
            );
        } else {
            assert_eq!(selected[0].row_ranges(), None);
        }
    }
}
