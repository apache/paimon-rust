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

use super::super::tests::{data_file_meta_with_path, write_schema_file, write_test_deletion_file};
use crate::catalog::Identifier;
use crate::common::CatalogOptions;
use crate::io::FileIOBuilder;
use crate::spec::{
    BinaryRow, DataFileMeta, DataType, Datum, IntType, Predicate, PredicateBuilder, Schema,
    SchemaChange, TableSchema,
};
use crate::table::data_file_index_writer::FileIndexOptions;
use crate::table::data_file_reader::DataFileReadTiming;
use crate::table::{DataSplit, DataSplitBuilder, RowRange, Table};
use crate::Error;
use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch};
use futures::TryStreamExt;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::ColumnIndexBuilder;
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::file::page_index::offset_index::PageLocation;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

const FIRST: i64 = 100_000;
const OPTION: &str = "data-evolution.merged-read.stats-pushdown.enabled";

async fn write_file(
    table: &Table,
    name: &str,
    version: i64,
    columns: Vec<(&str, ArrayRef)>,
    group_rows: usize,
    page_rows: usize,
    statistics: EnabledStatistics,
) -> DataFileMeta {
    let names = columns.iter().map(|(name, _)| *name).collect::<Vec<_>>();
    let batch = RecordBatch::try_from_iter(columns).unwrap();
    let properties = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_statistics_enabled(statistics)
        .set_offset_index_disabled(statistics != EnabledStatistics::Page)
        .set_max_row_group_row_count(Some(group_rows))
        .set_data_page_row_count_limit(page_rows)
        .set_write_batch_size(page_rows)
        .build();
    let mut writer = ArrowWriter::try_new(Vec::new(), batch.schema(), Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    let bytes = writer.into_inner().unwrap();
    let file = data_file_meta_with_path(
        name,
        FIRST,
        batch.num_rows() as i64,
        version,
        bytes.len() as i64,
        Some(names),
    );
    table
        .file_io()
        .new_output(&format!("{}/bucket-0/{name}", table.location()))
        .unwrap()
        .write(bytes.into())
        .await
        .unwrap();
    file
}

fn split_builder(table: &Table, files: Vec<DataFileMeta>) -> DataSplitBuilder {
    DataSplitBuilder::new()
        .with_snapshot(1)
        .with_partition(BinaryRow::new(0))
        .with_bucket(0)
        .with_bucket_path(format!("{}/bucket-0", table.location()))
        .with_total_buckets(1)
        .with_data_files(files)
}

async fn fixture() -> (Table, DataSplit) {
    fixture_with_stats(4096, EnabledStatistics::Page, 64).await
}

async fn fixture_with_stats(
    rows: i32,
    statistics: EnabledStatistics,
    score_page_rows: usize,
) -> (Table, DataSplit) {
    fixture_with_score_columns(rows, statistics, score_page_rows, 0).await
}

async fn fixture_with_score_columns(
    rows: i32,
    statistics: EnabledStatistics,
    score_page_rows: usize,
    extra_score_columns: usize,
) -> (Table, DataSplit) {
    let int = DataType::Int(IntType::new());
    let extra_names: Vec<_> = (0..extra_score_columns)
        .map(|index| format!("extra_{index}"))
        .collect();
    let mut schema = Schema::builder()
        .column("slot", int.clone())
        .column("id", int.clone())
        .column("score", int.clone())
        .option("data-evolution.enabled", "true")
        .option("row-tracking.enabled", "true");
    for name in &extra_names {
        schema = schema.column(name, int.clone());
    }
    let schema = TableSchema::new(0, &schema.build().unwrap());
    let table = Table::new(
        FileIOBuilder::new("memory")
            .with_prop(CatalogOptions::FILE_FORMAT_METADATA_CACHE_MAX_SIZE, "0")
            .build()
            .unwrap(),
        Identifier::new("default", "candidates"),
        "memory:/candidates".into(),
        schema.clone(),
        None,
    );
    write_schema_file(&table, &schema).await;
    let base = write_file(
        &table,
        "base.parquet",
        1,
        vec![
            ("id", Arc::new(Int32Array::from_iter_values(0..rows))),
            ("slot", Arc::new(Int32Array::from_value(7, rows as usize))),
            ("score", Arc::new(Int32Array::from_value(-1, rows as usize))),
        ],
        733,
        37,
        EnabledStatistics::Page,
    )
    .await;
    let obsolete = write_file(
        &table,
        "obsolete.parquet",
        2,
        vec![("score", Arc::new(Int32Array::from_value(-2, rows as usize)))],
        317,
        17,
        EnabledStatistics::Page,
    )
    .await;
    let scores: ArrayRef = Arc::new(Int32Array::from_iter(
        (0..rows).map(|i| (i % 257 != 0).then_some(i)),
    ));
    let mut score_columns = vec![("score", scores.clone())];
    score_columns.extend(
        extra_names
            .iter()
            .map(|name| (name.as_str(), scores.clone())),
    );
    let score = write_file(
        &table,
        "score.parquet",
        3,
        score_columns,
        1024,
        score_page_rows,
        statistics,
    )
    .await;
    let current = schema
        .apply_changes(vec![
            SchemaChange::rename_column("score".into(), "rating".into()),
            SchemaChange::drop_column("slot".into()),
            SchemaChange::add_column("slot".into(), int),
        ])
        .unwrap();
    let split = split_builder(&table, vec![base, obsolete, score])
        .build()
        .unwrap();
    (
        table.copy_with_resolved_schema(current, "main").unwrap(),
        split,
    )
}

struct Measurement {
    ids: Vec<i32>,
    data_bytes: u64,
    total_bytes: u64,
    separate_index_reads: usize,
    source_rows: u64,
    elapsed: Duration,
}

async fn read_batches(
    table: &Table,
    split: &DataSplit,
    predicate: &Predicate,
    limit: Option<usize>,
    timing: Arc<DataFileReadTiming>,
) -> crate::Result<Vec<RecordBatch>> {
    let mut builder = table.new_read_builder();
    builder.with_filter(predicate.clone());
    if table.schema().core_options().row_tracking_enabled() {
        builder.with_projection(&["id", "_ROW_ID"])?;
    } else {
        builder.with_projection(&["id"])?;
    }
    if let Some(limit) = limit {
        builder.with_limit(limit);
    }
    builder
        .new_read()?
        .with_data_file_read_timing(timing)
        .to_arrow(std::slice::from_ref(split))?
        .try_collect()
        .await
}

fn checked_ids(table: &Table, split: &DataSplit, batches: &[RecordBatch]) -> Vec<i32> {
    let first = split.data_files()[0].first_row_id.unwrap();
    let mut ids = Vec::new();
    for batch in batches {
        let id = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        if table.schema().core_options().row_tracking_enabled() {
            let row_id = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                assert_eq!(row_id.value(i), first + i64::from(id.value(i)));
            }
        }
        ids.extend_from_slice(id.values());
    }
    ids
}

async fn scan(
    table: &Table,
    split: &DataSplit,
    predicate: &Predicate,
    limit: Option<usize>,
) -> Measurement {
    let timing = Arc::new(DataFileReadTiming::default());
    let start = Instant::now();
    let batches = read_batches(table, split, predicate, limit, timing.clone())
        .await
        .unwrap();
    let elapsed = start.elapsed();
    let ids = checked_ids(table, split, &batches);
    let reads = std::mem::take(&mut *timing.read_ranges.lock().unwrap());
    let mut data_bytes = 0;
    let mut total_bytes = 0;
    let mut separate_index_reads = 0;
    for file in split.data_files() {
        let path = split.data_file_path(file);
        if !matches!(file.file_name.as_str(), "base.parquet" | "score.parquet") {
            assert!(!reads.contains_key(path.as_str()), "opened unused {path}");
            continue;
        }
        let Some(ranges) = reads.get(path.as_str()) else {
            assert!(ids.is_empty());
            continue;
        };
        let bytes = table
            .file_io()
            .new_input(&path)
            .unwrap()
            .read()
            .await
            .unwrap();
        let metadata = parquet::file::metadata::ParquetMetaDataReader::new()
            .parse_and_finish(&bytes)
            .unwrap();
        let data_end = metadata
            .row_groups()
            .iter()
            .flat_map(|rg| rg.columns())
            .map(|col| {
                let (start, length) = col.byte_range();
                start + length
            })
            .max()
            .unwrap();
        let mut footer_reads = 0;
        let mut footer_start = file.file_size as u64;
        let mut index_reads = 0;
        for range in ranges {
            let size = range.end - range.start;
            total_bytes += size;
            if range.end <= data_end {
                data_bytes += size;
            } else if range.end == file.file_size as u64 {
                footer_reads += 1;
                footer_start = range.start;
            } else {
                assert!(
                    range.start >= data_end && range.end <= footer_start,
                    "index read {range:?} outside {data_end}..{footer_start} for {path}"
                );
                index_reads += 1;
            }
        }
        assert_eq!(footer_reads, 1, "footer fetches for {path}");
        assert!(index_reads <= 1, "repeated index fetch for {path}");
        separate_index_reads += index_reads;
    }
    Measurement {
        ids,
        data_bytes,
        total_bytes,
        separate_index_reads,
        source_rows: timing.source_rows.load(Ordering::Relaxed),
        elapsed,
    }
}

fn disabled(table: &Table) -> Table {
    table.copy_with_options(HashMap::from([(OPTION.into(), "false".into())]))
}

async fn assert_candidates(
    table: &Table,
    split: &DataSplit,
    predicate: &Predicate,
    expected_ids: &[i32],
    expected_source_rows: Option<u64>,
) {
    let baseline = scan(&disabled(table), split, predicate, None).await;
    let pruned = scan(table, split, predicate, None).await;
    assert_eq!(baseline.ids, expected_ids, "{predicate:?}");
    assert_eq!(pruned.ids, baseline.ids, "{predicate:?}");
    if let Some(rows) = expected_source_rows {
        assert_eq!(pruned.source_rows, rows, "{predicate:?}");
    }
}

#[tokio::test]
async fn merged_candidates_share_pages_and_fall_back_to_row_groups_or_full_reads() {
    for (rows, stats, page_rows, expected_rows) in [
        (32_768, EnabledStatistics::Page, 1, 1),
        (4096, EnabledStatistics::Chunk, 64, 1024),
        (4096, EnabledStatistics::None, 64, 4096),
    ] {
        let (table, split) = fixture_with_stats(rows, stats, page_rows).await;
        let predicate = PredicateBuilder::new(table.schema().fields())
            .equal("rating", Datum::Int(2117))
            .unwrap();
        let on = scan(&table, &split, &predicate, None).await;
        let off = scan(&disabled(&table), &split, &predicate, None).await;
        assert_eq!(on.ids, [2117]);
        assert_eq!(on.ids, off.ids);
        assert_eq!(on.source_rows, 2 * expected_rows);
        assert_eq!(off.source_rows, 2 * rows as u64);
        assert_eq!(off.separate_index_reads, 0);
        assert_eq!(
            on.separate_index_reads,
            usize::from(stats == EnabledStatistics::Page)
        );
        if stats == EnabledStatistics::Page {
            let invalid =
                table.copy_with_options(HashMap::from([(OPTION.into(), "invalid".into())]));
            assert!(matches!(
                read_batches(&invalid, &split, &predicate, None, Default::default()).await,
                Err(Error::ConfigInvalid { message }) if message.contains(OPTION)
            ));
            let row_groups_only = table.copy_with_options(HashMap::from([(
                "parquet.filter.columnindex.enabled".into(),
                "false".into(),
            )]));
            let result = scan(&row_groups_only, &split, &predicate, None).await;
            assert_eq!(result.ids, off.ids);
            assert_eq!(result.source_rows, 2 * 1024);

            let null_id = PredicateBuilder::new(table.schema().fields())
                .is_null("id")
                .unwrap();
            let on = scan(&row_groups_only, &split, &null_id, None).await;
            let off = scan(&disabled(&row_groups_only), &split, &null_id, None).await;
            assert!(off.ids.is_empty());
            assert_eq!(on.ids, off.ids);
            assert_eq!(on.source_rows, 0);
            assert_eq!(off.source_rows, rows as u64);

            let selected = split
                .clone()
                .with_selected_row_ranges(vec![RowRange::new(FIRST + 2118, FIRST + 2118)]);
            assert_candidates(&table, &selected, &predicate, &[], Some(0)).await;
        }
        if stats != EnabledStatistics::None {
            assert!(on.total_bytes < off.total_bytes);
            assert!(
                on.data_bytes < off.data_bytes,
                "{} >= {}",
                on.data_bytes,
                off.data_bytes
            );
        }
    }
}

#[tokio::test]
async fn merged_page_pruning_checks_only_selected_row_groups_and_columns() {
    let (table, split) = fixture().await;
    let predicate = PredicateBuilder::new(table.schema().fields())
        .equal("rating", Datum::Int(2117))
        .unwrap();
    let reference = scan(&table, &split, &predicate, None).await;
    let mut files = split.data_files().to_vec();
    *files.last_mut().unwrap() = write_file(
        &table,
        "score.parquet",
        3,
        vec![
            (
                "score",
                Arc::new(Int32Array::from_iter(
                    (0..4096).map(|i| (i % 257 != 0).then_some(i)),
                )),
            ),
            ("slot", Arc::new(Int32Array::from_value(7, 4096))),
        ],
        1024,
        64,
        EnabledStatistics::Page,
    )
    .await;
    let path = split.data_file_path(files.last().unwrap());
    let io = table.file_io();
    let bytes = io.new_input(&path).unwrap().read().await.unwrap();
    let without_skipped_row_counts = rewrite_page_metadata(&bytes, |mut builder| {
        let mut indexes = builder.take_column_index().unwrap();
        indexes[0][0] = column_index_without_row_counts(&indexes[0][0]);
        builder.set_column_index(Some(indexes))
    });
    let off = disabled(&table);
    for bytes in [bytes, without_skipped_row_counts.into()] {
        files.last_mut().unwrap().file_size = bytes.len() as i64;
        io.new_output(&path).unwrap().write(bytes).await.unwrap();
        let split = split_builder(&table, files.clone()).build().unwrap();
        let result = scan(&table, &split, &predicate, None).await;
        assert_eq!(result.ids, [2117]);
        assert_eq!(result.ids, scan(&off, &split, &predicate, None).await.ids);
        assert_eq!(result.source_rows, reference.source_rows);
        assert_eq!(result.data_bytes, reference.data_bytes);
    }
}

#[tokio::test]
async fn nonselective_merged_stats_preserve_sparse_page_reads() {
    let (table, split) = fixture().await;
    let row_id = FIRST + 2117;
    let split = split_builder(&table, split.data_files()[..2].to_vec())
        .with_row_ranges(vec![RowRange::new(row_id, row_id + 1)])
        .build()
        .unwrap();
    let predicate = PredicateBuilder::new(table.schema().fields())
        .greater_or_equal("id", Datum::Int(0))
        .unwrap();
    let off = scan(&disabled(&table), &split, &predicate, None).await;
    let on = scan(&table, &split, &predicate, None).await;
    assert_eq!(off.ids, [2117, 2118]);
    assert_eq!(on.ids, off.ids);
    assert_eq!(on.data_bytes, off.data_bytes);

    let without_page_index = table.copy_with_options(HashMap::from([(
        "parquet.filter.columnindex.enabled".into(),
        "false".into(),
    )]));
    let full_chunk = scan(&without_page_index, &split, &predicate, None).await;
    assert!(on.data_bytes < full_chunk.data_bytes);
}

#[tokio::test]
async fn sparse_merged_reads_defer_column_indexes_when_footer_statistics_decide() {
    for (rows, page_rows, extra_columns) in [(32_768, 1, 0), (100_000, 64, 48)] {
        let (table, split) =
            fixture_with_score_columns(rows, EnabledStatistics::Page, page_rows, extra_columns)
                .await;
        let p = PredicateBuilder::new(table.schema().fields());
        let off = disabled(&table);
        let matching_ranges = [(0, 2117, 2117), (2048, 2056, 2056), (2048, 2117, 2118)];
        for (lower_bound, first, last) in matching_ranges {
            let predicate = Predicate::or(vec![
                p.is_null("rating").unwrap(),
                p.greater_or_equal("rating", Datum::Int(lower_bound))
                    .unwrap(),
            ]);
            let selected = split.clone().with_selected_row_ranges(vec![RowRange::new(
                FIRST + i64::from(first),
                FIRST + i64::from(last),
            )]);
            let baseline = scan(&off, &selected, &predicate, None).await;
            let result = scan(&table, &selected, &predicate, None).await;
            assert_eq!(result.ids, (first..=last).collect::<Vec<_>>());
            assert_eq!(result.ids, baseline.ids);
            assert_eq!(result.source_rows, 2 * (last - first + 1) as u64);
            assert_eq!(result.data_bytes, baseline.data_bytes);
            assert_eq!(result.total_bytes, baseline.total_bytes);
        }

        let predicate = p.equal("rating", Datum::Int(4095)).unwrap();
        let selected =
            split.with_selected_row_ranges(vec![RowRange::new(FIRST + 2117, FIRST + 2118)]);
        let baseline = scan(&off, &selected, &predicate, None).await;
        let result = scan(&table, &selected, &predicate, None).await;
        assert!(baseline.ids.is_empty());
        assert_eq!(result.ids, baseline.ids);
        assert_eq!(result.source_rows, 0);
        assert_eq!(result.data_bytes, 0);
        assert!(result.total_bytes < baseline.total_bytes);
    }
}

#[tokio::test]
async fn merged_candidates_use_winning_ids_and_preserve_boolean_residuals() {
    let (table, split) = fixture().await;
    let mut files = split.data_files().to_vec();
    for file in &mut files {
        file.first_row_id = Some(i64::MAX - (file.row_count - 1));
    }
    let split = split_builder(&table, files).build().unwrap();
    let p = PredicateBuilder::new(table.schema().fields());
    let id = |n| p.equal("id", Datum::Int(n)).unwrap();
    let rating = |n| p.equal("rating", Datum::Int(n)).unwrap();
    for (predicate, expected, candidate_rows) in [
        (
            Predicate::Not(Box::new(
                p.less_or_equal("rating", Datum::Int(4094)).unwrap(),
            )),
            vec![4095],
            Some(64),
        ),
        (
            Predicate::or(vec![rating(201), id(3011)]),
            vec![201, 3011],
            Some(4096),
        ),
        (
            Predicate::or(vec![rating(207), rating(3001)]),
            vec![207, 3001],
            Some(128),
        ),
        (
            Predicate::and(vec![id(2117), rating(2117)]),
            vec![2117],
            Some(64),
        ),
        (
            Predicate::and(vec![p.is_null("slot").unwrap(), rating(3001)]),
            vec![3001],
            Some(64),
        ),
        (
            Predicate::and(vec![
                p.is_null("rating").unwrap(),
                p.between("id", Datum::Int(400), Datum::Int(800)).unwrap(),
            ]),
            vec![514, 771],
            None,
        ),
        (
            Predicate::and(vec![
                p.is_in("id", vec![Datum::Int(12), Datum::Int(13)]).unwrap(),
                Predicate::Not(Box::new(Predicate::and(vec![id(12), rating(13)]))),
            ]),
            vec![12, 13],
            None,
        ),
        (Predicate::and(vec![id(222), rating(3011)]), vec![], Some(0)),
        (
            Predicate::or(vec![
                p.greater_or_equal("rating", Datum::Int(0)).unwrap(),
                p.is_null("rating").unwrap(),
            ]),
            (0..4096).collect(),
            Some(4096),
        ),
    ] {
        assert_candidates(
            &table,
            &split,
            &predicate,
            &expected,
            candidate_rows.map(|rows| 2 * rows),
        )
        .await;
    }
}

#[tokio::test]
async fn merged_candidates_intersect_bitmap_hits_requested_ranges_and_deletions_before_limit() {
    let (table, split) = fixture().await;
    let table = table.copy_with_options(HashMap::from([("read.batch-size".into(), "2".into())]));
    let dv =
        write_test_deletion_file(table.file_io(), "memory:/candidates/dv", &[2002, 2004]).await;
    let mut files = split.data_files().to_vec();
    let index_options = HashMap::from([("file-index.bitmap.columns".into(), "id".into())]);
    let mut index = FileIndexOptions::parse(&index_options, table.schema().fields())
        .unwrap()
        .unwrap()
        .create_writer()
        .unwrap();
    let ids = RecordBatch::try_from_iter([(
        "id",
        Arc::new(Int32Array::from_iter_values(0..4096)) as ArrayRef,
    )])
    .unwrap();
    index.write(&ids).unwrap();
    files[0].embedded_index = Some(index.serialize().unwrap().to_vec());
    let split = split_builder(&table, files)
        .with_data_deletion_files(vec![Some(dv), None, None])
        .with_row_ranges(vec![
            RowRange::new(FIRST + 2010, FIRST + 2020),
            RowRange::new(FIRST + 2002, FIRST + 2007),
            RowRange::new(FIRST + 1000, FIRST + 1001),
            RowRange::new(FIRST + 3000, FIRST + 3001),
        ])
        .build()
        .unwrap();
    let p = PredicateBuilder::new(table.schema().fields());
    let indexed_ids = [
        1000, 2000, 2002, 2003, 2004, 2006, 2007, 2010, 2012, 2018, 3001,
    ];
    let predicate = Predicate::and(vec![
        p.between("rating", Datum::Int(2000), Datum::Int(2015))
            .unwrap(),
        p.is_in("id", indexed_ids.map(Datum::Int).to_vec()).unwrap(),
    ]);
    let deleted = split
        .clone()
        .with_selected_row_ranges(vec![RowRange::new(FIRST + 2002, FIRST + 2002)]);
    let without_bitmap = table.copy_with_options(HashMap::from([(
        "file-index.read.enabled".into(),
        "false".into(),
    )]));
    for (table, candidates) in [(&table, 6), (&disabled(&table), 8), (&without_bitmap, 15)] {
        let full = scan(table, &split, &predicate, None).await;
        assert_eq!(full.ids, [2003, 2006, 2007, 2010, 2012]);
        assert_eq!(full.source_rows, 2 * candidates);
        assert_eq!(
            scan(table, &split, &predicate, Some(3)).await.ids,
            [2003, 2006, 2007]
        );
        let result = scan(table, &deleted, &predicate, None).await;
        assert!(result.ids.is_empty());
        assert_eq!(result.total_bytes, 0);
    }
}

#[tokio::test]
async fn merged_candidates_use_bounds_only_for_order_preserving_schema_changes() {
    use crate::spec::{BigIntType, TinyIntType};

    let (table, split) = fixture().await;
    for (ty, value, expected, candidates) in [
        (
            DataType::BigInt(BigIntType::new()),
            Datum::Long(2117),
            vec![2117],
            64,
        ),
        (
            DataType::TinyInt(TinyIntType::new()),
            Datum::TinyInt(-1),
            (255..4096).step_by(256).collect(),
            4096,
        ),
    ] {
        let schema = table
            .schema()
            .apply_changes(vec![SchemaChange::update_column_type("rating".into(), ty)])
            .unwrap();
        let table = table.copy_with_resolved_schema(schema, "main").unwrap();
        let predicate = PredicateBuilder::new(table.schema().fields())
            .equal("rating", value)
            .unwrap();
        assert_candidates(&table, &split, &predicate, &expected, Some(2 * candidates)).await;
    }
}

#[tokio::test]
async fn merged_candidates_require_row_tracking_unbucketed_tables_and_flat_parquet_groups() {
    use crate::spec::{BlobType, FloatType, VectorType};

    let (table, split) = fixture().await;
    let mut cases = Vec::new();
    for (key, value) in [
        ("row-tracking.enabled", "false"),
        ("bucket", "4"),
        ("data-evolution.nested-field.enabled", "true"),
    ] {
        let table = table.copy_with_options(HashMap::from([(key.into(), value.into())]));
        cases.push((table, split.clone()));
    }
    for (name, ty) in [
        ("extra.orc", DataType::Int(IntType::new())),
        ("extra.blob", DataType::Blob(BlobType::new())),
        (
            "extra.vector.parquet",
            DataType::Vector(
                VectorType::try_new(true, 2, DataType::Float(FloatType::new())).unwrap(),
            ),
        ),
    ] {
        let schema = table
            .schema()
            .apply_changes(vec![SchemaChange::add_column("extra".into(), ty)])
            .unwrap();
        let table = table.copy_with_resolved_schema(schema, "main").unwrap();
        let mut extra = data_file_meta_with_path(name, FIRST, 4096, 4, 1, Some(vec!["extra"]));
        extra.schema_id = table.schema().id();
        let mut files = split.data_files().to_vec();
        files.push(extra);
        let split = split_builder(&table, files).build().unwrap();
        cases.push((table, split));
    }
    for (table, split) in cases {
        let p = PredicateBuilder::new(table.schema().fields())
            .equal("rating", Datum::Int(2117))
            .unwrap();
        assert_candidates(&table, &split, &p, &[2117], Some(2 * 4096)).await;
    }
}

#[tokio::test]
async fn merged_candidates_require_compatible_physical_statistics() {
    use crate::spec::{FloatType, TimestampType};
    use arrow_array::{
        Float32Array, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampSecondArray,
    };

    let (table, split) = fixture().await;
    let timestamp = |precision| DataType::Timestamp(TimestampType::new(precision).unwrap());
    let instant = Datum::Timestamp {
        millis: 2_117_000,
        nanos: 0,
    };
    let seconds = 0..4096;
    let millis = TimestampMillisecondArray::from_iter_values(seconds.clone().map(|s| s * 1000));
    let micros =
        TimestampMicrosecondArray::from_iter_values(seconds.clone().map(|s| s * 1_000_000));
    let floats = Float32Array::from_iter_values(seconds.clone().map(|i| {
        if i == 0 {
            f32::NAN
        } else {
            i as f32
        }
    }));
    let seconds = TimestampSecondArray::from_iter_values(seconds);
    for (ty, column, value, eq_rows, or_rows) in [
        (
            timestamp(3),
            Arc::new(millis) as ArrayRef,
            instant.clone(),
            1024,
            2048,
        ),
        (timestamp(3), Arc::new(micros), instant.clone(), 4096, 4096),
        (timestamp(0), Arc::new(seconds), instant, 4096, 4096),
        (
            DataType::Float(FloatType::new()),
            Arc::new(floats),
            Datum::Float(2117.0),
            4096,
            4096,
        ),
    ] {
        let schema = table
            .schema()
            .apply_changes(vec![
                SchemaChange::drop_column("rating".into()),
                SchemaChange::add_column("rating".into(), ty),
                SchemaChange::add_column("rank".into(), DataType::Int(IntType::new())),
            ])
            .unwrap();
        let table = table.copy_with_resolved_schema(schema, "main").unwrap();
        let mut file = write_file(
            &table,
            "score.parquet",
            4,
            vec![
                ("rating", column),
                ("rank", Arc::new(Int32Array::from_iter_values(0..4096))),
            ],
            1024,
            64,
            EnabledStatistics::Page,
        )
        .await;
        file.schema_id = table.schema().id();
        let split = split_builder(&table, vec![split.data_files()[0].clone(), file])
            .build()
            .unwrap();
        let p = PredicateBuilder::new(table.schema().fields());
        let rating = p.equal("rating", value).unwrap();
        let rank = |value| p.equal("rank", Datum::Int(value)).unwrap();
        for (predicate, expected, candidates) in [
            (rating.clone(), vec![2117], eq_rows),
            (
                Predicate::and(vec![rating.clone(), rank(2117)]),
                vec![2117],
                1024,
            ),
            (
                Predicate::or(vec![rating, rank(15)]),
                vec![15, 2117],
                or_rows,
            ),
        ] {
            assert_candidates(&table, &split, &predicate, &expected, Some(2 * candidates)).await;
        }
    }
}

#[tokio::test]
async fn merged_candidates_require_consistent_source_row_counts() {
    let (table, split) = fixture().await;
    let off = disabled(&table);
    let mut files = split.data_files().to_vec();
    for file in &mut files {
        file.row_count += 1;
    }
    let wrong_count = split_builder(&table, files).build().unwrap();
    let mut short = write_file(
        &table,
        "short.parquet",
        1,
        vec![("id", Arc::new(Int32Array::from_iter_values(0..3000)))],
        733,
        37,
        EnabledStatistics::Page,
    )
    .await;
    short.row_count = 4096;
    let mut files = split.data_files().to_vec();
    files[0] = short;
    let short_source = split_builder(&table, files).build().unwrap();
    for (split, rating, expected_error) in [
        (&wrong_count, -10, "produced 4096 rows but expected 4097"),
        (&short_source, 2117, "exhausted at different row counts"),
    ] {
        let predicate = PredicateBuilder::new(table.schema().fields())
            .equal("rating", Datum::Int(rating))
            .unwrap();
        for table in [&off, &table] {
            let timing = Arc::new(DataFileReadTiming::default());
            let err = read_batches(table, split, &predicate, None, timing.clone())
                .await
                .unwrap_err();
            assert!(err.to_string().contains(expected_error), "{err}");
            let reads = timing.read_ranges.lock().unwrap();
            for file in split
                .data_files()
                .iter()
                .filter(|file| file.file_name != "obsolete.parquet")
            {
                let path = split.data_file_path(file);
                let footer_reads = reads[path.as_str()]
                    .iter()
                    .filter(|range| range.end == file.file_size as u64)
                    .count();
                assert_eq!(footer_reads, 1, "footer fetches for {path}");
            }
        }
    }
}

#[tokio::test]
async fn merged_candidates_keep_nulls_when_statistics_disagree_with_definition_levels() {
    use parquet::file::metadata::ColumnIndexBuilder;
    use parquet::file::page_index::column_index::ColumnIndexMetaData;
    use parquet::file::statistics::Statistics;

    let (table, split) = fixture_with_stats(128, EnabledStatistics::Page, 64).await;
    let mut files = split.data_files().to_vec();
    let score_index = files.len() - 1;
    let path = split.data_file_path(&files[score_index]);
    let file_io = table.file_io();
    let output = file_io.new_output(&path).unwrap();
    let bytes = file_io.new_input(&path).unwrap().read().await.unwrap();
    let predicate = PredicateBuilder::new(table.schema().fields())
        .is_null("rating")
        .unwrap();
    let reference = scan(&table, &split, &predicate, None).await;
    let unpruned = disabled(&table);

    for damage_page_stats in [true, false] {
        let damaged = rewrite_page_metadata(&bytes, |mut builder| {
            if damage_page_stats {
                let mut indexes = builder.take_column_index().unwrap();
                let ColumnIndexMetaData::INT32(index) = &indexes[0][0] else {
                    panic!("expected INT32 page statistics");
                };
                assert_eq!(index.null_count(0), Some(1));
                let mut replacement = ColumnIndexBuilder::new(parquet::basic::Type::INT32);
                for page in 0..index.num_pages() as usize {
                    replacement.append(
                        index.is_null_page(page),
                        index.min_values()[page].to_le_bytes().to_vec(),
                        index.max_values()[page].to_le_bytes().to_vec(),
                        0,
                    );
                    let levels = index.definition_level_histogram(page).unwrap().to_vec();
                    replacement.append_histograms(&None, &Some(levels.into()));
                }
                indexes[0][0] = replacement.build().unwrap();
                builder.set_column_index(Some(indexes))
            } else {
                let mut groups = builder.take_row_groups();
                let column = groups[0].column(0);
                assert_eq!(
                    column.definition_level_histogram().unwrap().values(),
                    [1, 127]
                );
                groups[0].columns_mut()[0] = column
                    .clone()
                    .into_builder()
                    .set_statistics(Statistics::int32(Some(1), Some(127), None, Some(0), false))
                    .build()
                    .unwrap();
                builder.set_row_groups(groups)
            }
        });
        files[score_index].file_size = damaged.len() as i64;
        let damaged_split = split_builder(&table, files.clone()).build().unwrap();
        output.write(damaged.into()).await.unwrap();
        let pruned = scan(&table, &damaged_split, &predicate, None).await;
        assert_eq!(
            scan(&unpruned, &damaged_split, &predicate, None).await.ids,
            [0]
        );
        assert_eq!(pruned.ids, [0]);
        assert_eq!(pruned.source_rows, 2 * 64);
        assert_eq!(pruned.data_bytes, reference.data_bytes);
    }
}

fn rewrite_page_locations(
    bytes: &bytes::Bytes,
    edit: impl FnOnce(&mut Vec<PageLocation>, i64),
) -> Vec<u8> {
    rewrite_page_metadata(bytes, |mut builder| {
        let row_count = builder.row_groups().last().unwrap().num_rows();
        let mut offsets = builder.take_offset_index().unwrap();
        edit(
            &mut offsets.last_mut().unwrap()[0].page_locations,
            row_count,
        );
        builder.set_offset_index(Some(offsets))
    })
}

fn rewrite_page_metadata(
    bytes: &bytes::Bytes,
    edit: impl FnOnce(
        parquet::file::metadata::ParquetMetaDataBuilder,
    ) -> parquet::file::metadata::ParquetMetaDataBuilder,
) -> Vec<u8> {
    use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader, ParquetMetaDataWriter};
    use parquet::file::writer::TrackedWrite;
    use std::io::Write;

    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .parse_and_finish(bytes)
        .unwrap();
    let group = metadata.row_groups().last().unwrap();
    let (start, len) = group.columns().last().unwrap().byte_range();
    let metadata = edit(metadata.into_builder()).build();
    let mut output = Vec::new();
    let mut writer = TrackedWrite::new(&mut output);
    writer.write_all(&bytes[..(start + len) as usize]).unwrap();
    ParquetMetaDataWriter::new_with_tracked(writer, &metadata)
        .finish()
        .unwrap();
    output
}

fn column_index_without_row_counts(index: &ColumnIndexMetaData) -> ColumnIndexMetaData {
    let ColumnIndexMetaData::INT32(index) = index else {
        panic!("expected INT32 page statistics");
    };
    let mut replacement = ColumnIndexBuilder::new(parquet::basic::Type::INT32);
    for page in 0..index.num_pages() as usize {
        replacement.append(
            index.is_null_page(page),
            index.min_values()[page].to_le_bytes().to_vec(),
            index.max_values()[page].to_le_bytes().to_vec(),
            index.null_count(page).unwrap(),
        );
    }
    replacement.build().unwrap()
}

#[tokio::test]
async fn merged_candidates_require_independent_projected_page_row_counts() {
    let (table, split) = fixture_with_stats(128, EnabledStatistics::Page, 1).await;
    let file = &split.data_files()[0];
    let path = split.data_file_path(file);
    let bytes = table
        .file_io()
        .new_input(&path)
        .unwrap()
        .read()
        .await
        .unwrap();
    let p = PredicateBuilder::new(table.schema().fields());
    let exact_rating = p.equal("rating", Datum::Int(36)).unwrap();
    let rating = p.between("rating", Datum::Int(36), Datum::Int(37)).unwrap();
    let predicates = [
        rating.clone(),
        Predicate::and(vec![
            rating,
            p.greater_or_equal("id", Datum::Int(0)).unwrap(),
        ]),
    ];
    for keep_column_index in [false, true] {
        let damaged = rewrite_page_metadata(&bytes, |mut builder| {
            let mut offsets = builder.take_offset_index().unwrap();
            assert_eq!(offsets[0][0].page_locations[1].first_row_index, 37);
            offsets[0][0].page_locations[1].first_row_index = 36;
            let mut indexes = builder.take_column_index().unwrap();
            if keep_column_index {
                indexes[0][0] = column_index_without_row_counts(&indexes[0][0]);
            } else {
                let mut groups = builder.take_row_groups();
                for group in &mut groups {
                    for column in group.columns_mut() {
                        *column = column
                            .clone()
                            .into_builder()
                            .set_column_index_offset(None)
                            .set_column_index_length(None)
                            .build()
                            .unwrap();
                    }
                }
                builder = builder.set_row_groups(groups);
            }
            builder
                .set_offset_index(Some(offsets))
                .set_column_index(keep_column_index.then_some(indexes))
        });
        let mut files = split.data_files().to_vec();
        files[0].file_size = damaged.len() as i64;
        let damaged_split = split_builder(&table, files).build().unwrap();
        table
            .file_io()
            .new_output(&path)
            .unwrap()
            .write(damaged.into())
            .await
            .unwrap();
        assert_candidates(&table, &damaged_split, &exact_rating, &[36], Some(2)).await;
        let point = damaged_split
            .clone()
            .with_selected_row_ranges(vec![RowRange::new(FIRST + 36, FIRST + 36)]);
        let result = scan(&table, &point, &exact_rating, None).await;
        assert_eq!(result.ids, [36]);
        assert_eq!(result.source_rows, 2);
        let selected = damaged_split
            .clone()
            .with_selected_row_ranges(vec![RowRange::new(FIRST + 36, FIRST + 37)]);
        for predicate in &predicates {
            let result = scan(&table, &selected, predicate, None).await;
            assert_eq!(result.ids, [36, 37]);
            assert_eq!(result.source_rows, 4);
        }
    }
}

#[tokio::test]
async fn merged_candidates_keep_normal_read_behavior_for_damaged_files() {
    type PageLocationEdit = fn(&mut Vec<PageLocation>, i64);

    let (table, split) = fixture().await;
    let off = disabled(&table);
    let predicates = PredicateBuilder::new(table.schema().fields());
    let p = predicates.equal("rating", Datum::Int(2117)).unwrap();
    let selected = split
        .clone()
        .with_selected_row_ranges(vec![RowRange::new(FIRST + 2116, FIRST + 2118)]);
    let read = |table| read_batches(table, &split, &p, None, Default::default());
    let io = table.file_io();
    let last = predicates.equal("rating", Datum::Int(4095)).unwrap();
    for name in ["score.parquet", "base.parquet"] {
        let file_index = split
            .data_files()
            .iter()
            .position(|f| f.file_name == name)
            .unwrap();
        let path = split.data_file_path(&split.data_files()[file_index]);
        let output = io.new_output(&path).unwrap();
        let bytes = io.new_input(&path).unwrap().read().await.unwrap();
        let mut edits: Vec<(PageLocationEdit, bool)> = vec![
            (
                |pages, rows| pages.last_mut().unwrap().first_row_index = rows,
                true,
            ),
            (
                |pages, _| pages.last_mut().unwrap().offset = pages[0].offset,
                true,
            ),
            (
                |pages, _| pages.last_mut().unwrap().compressed_page_size += 1,
                true,
            ),
            (
                |pages, _| pages.last_mut().unwrap().compressed_page_size -= 1,
                true,
            ),
            (|pages, _| pages[0].compressed_page_size -= 1, true),
            (
                |pages, _| {
                    pages[0].offset += 1;
                    pages[0].compressed_page_size -= 1;
                },
                true,
            ),
            (
                |pages, _| {
                    let last = pages.pop().unwrap();
                    pages.last_mut().unwrap().compressed_page_size += last.compressed_page_size;
                },
                false,
            ),
        ];
        if name == "score.parquet" {
            edits.push((
                |pages, _| pages.last_mut().unwrap().first_row_index += 1,
                false,
            ));
        }
        for (edit, offset_only_detects_damage) in edits {
            let damaged_bytes = rewrite_page_locations(&bytes, edit);
            let mut files = split.data_files().to_vec();
            files[file_index].file_size = damaged_bytes.len() as i64;
            let damaged = split_builder(&table, files).build().unwrap();
            output.write(damaged_bytes.into()).await.unwrap();
            let rows = if name == "score.parquet" { 1024 } else { 64 };
            assert_candidates(&table, &damaged, &last, &[4095], Some(2 * rows)).await;
            let last_rows =
                damaged.with_selected_row_ranges(vec![RowRange::new(FIRST + 4094, FIRST + 4095)]);
            if offset_only_detects_damage {
                assert_candidates(&table, &last_rows, &last, &[4095], Some(2 * 2)).await;
            } else {
                let result = scan(&table, &last_rows, &last, None).await;
                assert_eq!(result.ids, [4095]);
                assert_eq!(result.source_rows, 2 * 2);
            }
        }

        if name == "score.parquet" {
            let shifted = rewrite_page_locations(&bytes, |pages, _| {
                pages[2].compressed_page_size -= 1;
                pages[3].offset -= 1;
                pages[3].compressed_page_size += 1;
            });
            let mut files = split.data_files().to_vec();
            files[file_index].file_size = shifted.len() as i64;
            let damaged = split_builder(&table, files).build().unwrap();
            output.write(shifted.into()).await.unwrap();
            let matches_all = Predicate::or(vec![
                predicates
                    .greater_or_equal("rating", Datum::Int(0))
                    .unwrap(),
                predicates.is_null("rating").unwrap(),
            ]);
            let ids: Vec<_> = (0..4096).collect();
            assert_candidates(&table, &damaged, &matches_all, &ids, Some(2 * 4096)).await;

            let expected = [7, 3073, 3201];
            let selective = Predicate::or(
                expected
                    .map(|id| predicates.equal("rating", Datum::Int(id)).unwrap())
                    .to_vec(),
            );
            let batched =
                table.copy_with_options(HashMap::from([("read.batch-size".into(), "16".into())]));
            assert_candidates(&batched, &damaged, &selective, &expected, Some(2 * 3 * 64)).await;
        }

        let metadata = parquet::file::metadata::ParquetMetaDataReader::new()
            .parse_and_finish(&bytes)
            .unwrap();
        let column = metadata.row_group(0).column(0);
        let (offset, len) = if name == "score.parquet" {
            (column.column_index_offset(), column.column_index_length())
        } else {
            (column.offset_index_offset(), column.offset_index_length())
        };
        let start = offset.unwrap() as usize;
        let mut corrupt = bytes.to_vec();
        corrupt[start..start + len.unwrap() as usize].fill(0xff);
        output.write(corrupt.into()).await.unwrap();
        for split in [&split, &selected] {
            assert_candidates(&table, split, &p, &[2117], None).await;
        }
        let mut corrupt_footer = bytes.to_vec();
        *corrupt_footer.last_mut().unwrap() = 0;
        output.write(corrupt_footer.into()).await.unwrap();
        assert_eq!(
            read(&table).await.unwrap_err().to_string(),
            read(&off).await.unwrap_err().to_string(),
            "{name}"
        );
        io.delete_file(&path).await.unwrap();
        assert_eq!(
            read(&table).await.unwrap_err().to_string(),
            read(&off).await.unwrap_err().to_string(),
            "{name}"
        );
        if name == "base.parquet" {
            let outside = split
                .clone()
                .with_selected_row_ranges(vec![RowRange::new(FIRST + 3000, FIRST + 3001)]);
            let result = scan(&table, &outside, &p, None).await;
            assert!(result.ids.is_empty());
            assert_eq!(result.data_bytes, 0);
        }
        output.write(bytes).await.unwrap();
    }
}

#[tokio::test]
#[ignore = "run with --ignored --nocapture to measure cold-metadata merged reads"]
async fn benchmark_merged_candidates() {
    let rows = 262_144;
    let (table, split) = fixture_with_stats(rows, EnabledStatistics::Page, 64).await;
    let off = disabled(&table);
    let p = PredicateBuilder::new(table.schema().fields());
    for (name, predicate, expected_ids) in [
        (
            "selective",
            p.equal("rating", Datum::Int(2117)).unwrap(),
            vec![2117],
        ),
        (
            "nonselective",
            Predicate::or(vec![
                p.greater_or_equal("rating", Datum::Int(0)).unwrap(),
                p.is_null("rating").unwrap(),
            ]),
            (0..rows).collect(),
        ),
    ] {
        let mut times = [Vec::new(), Vec::new()];
        for i in 0..7 {
            let order = if i % 2 == 0 { [0, 1] } else { [1, 0] };
            for mode in order {
                let result = scan([&off, &table][mode], &split, &predicate, None).await;
                assert_eq!(result.ids, expected_ids);
                times[mode].push(result.elapsed);
                if i == 6 {
                    times[mode].sort_unstable();
                    println!("{name} enabled={} rows={} source_rows={} data_bytes={} total_bytes={} median={:?}",
                        mode == 1, result.ids.len(), result.source_rows, result.data_bytes, result.total_bytes, times[mode][3]);
                }
            }
        }
    }
}
