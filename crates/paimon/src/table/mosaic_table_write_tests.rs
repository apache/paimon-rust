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

//! Verify Mosaic writes through ordinary append and primary-key tables.

use std::sync::Arc;

use arrow_array::{Array, Int32Array, Int8Array, RecordBatch, StringArray, TimestampSecondArray};
use arrow_schema::{DataType as ArrowType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;

use super::{Table, TableCommit, TableWrite};
use crate::catalog::Identifier;
use crate::io::{FileIO, FileIOBuilder};
use crate::spec::{
    BinaryRow, DataType, Datum, IntType, PredicateBuilder, Schema, TableSchema, TimestampType,
    VarCharType,
};

fn memory_io() -> FileIO {
    FileIOBuilder::new("memory").build().unwrap()
}

async fn setup_dirs(io: &FileIO, path: &str) {
    io.mkdirs(&format!("{path}/snapshot/")).await.unwrap();
    io.mkdirs(&format!("{path}/manifest/")).await.unwrap();
}

fn table(io: &FileIO, path: &str, primary_key: bool, options: &[(&str, &str)]) -> Table {
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .option("file.format", "mosaic");
    if primary_key {
        schema = schema.primary_key(["id"]).option("bucket", "1");
    }
    for &(key, value) in options {
        schema = schema.option(key, value);
    }
    let schema = schema.build().unwrap();
    Table::new(
        io.clone(),
        Identifier::new("default", "mosaic_table"),
        path.to_owned(),
        TableSchema::new(0, &schema),
        None,
    )
}

fn batch(ids: Vec<i32>, values: Vec<i32>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowType::Int32, true),
            Field::new("value", ArrowType::Int32, true),
        ])),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(Int32Array::from(values)),
        ],
    )
    .unwrap()
}

async fn read_pairs(table: &Table) -> Vec<(i32, i32)> {
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let read = table.new_read_builder().new_read().unwrap();
    let batches: Vec<RecordBatch> = read
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut pairs = batches
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            (0..batch.num_rows())
                .map(|row| (ids.value(row), values.value(row)))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    pairs.sort_unstable();
    pairs
}

#[tokio::test]
async fn append_write_commit_scan_and_read_mosaic() {
    let io = memory_io();
    let path = "memory:/native_mosaic_append";
    setup_dirs(&io, path).await;
    let table = table(
        &io,
        path,
        false,
        &[
            ("mosaic.num-buckets", "2"),
            ("file.block-size", "64"),
            ("mosaic.stats-columns", "id,value"),
        ],
    );
    let mut writer = TableWrite::new(&table, "mosaic-test".into()).unwrap();
    writer
        .write_arrow_batch(&batch(vec![3, 1], vec![30, 10]))
        .await
        .unwrap();
    writer
        .write_arrow_batch(&batch(vec![2, 4], vec![20, 40]))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].new_files.len(), 1);
    let file = &messages[0].new_files[0];
    assert!(file.file_name.ends_with(".mosaic"));
    assert_eq!(file.row_count, 4);
    assert_eq!(
        file.value_stats_cols.as_ref().unwrap(),
        &vec!["id", "value"]
    );
    assert_eq!(file.value_stats.null_counts(), &vec![Some(0), Some(0)]);
    let min = BinaryRow::from_serialized_bytes(file.value_stats.min_values()).unwrap();
    let max = BinaryRow::from_serialized_bytes(file.value_stats.max_values()).unwrap();
    assert_eq!(min.get_int(0).unwrap(), 1);
    assert_eq!(max.get_int(0).unwrap(), 4);
    assert_eq!(min.get_int(1).unwrap(), 10);
    assert_eq!(max.get_int(1).unwrap(), 40);

    TableCommit::new(table.clone(), "mosaic-test".into())
        .commit(messages)
        .await
        .unwrap();
    assert_eq!(
        read_pairs(&table).await,
        vec![(1, 10), (2, 20), (3, 30), (4, 40)]
    );
}

#[tokio::test]
async fn primary_key_mosaic_deduplicates_across_commits() {
    let io = memory_io();
    let path = "memory:/native_mosaic_pk";
    setup_dirs(&io, path).await;
    let table = table(
        &io,
        path,
        true,
        &[
            ("mosaic.num-buckets", "2"),
            ("mosaic.stats-columns", "value"),
        ],
    );
    let commit = TableCommit::new(table.clone(), "mosaic-test".into());

    let mut first = TableWrite::new(&table, "mosaic-test".into()).unwrap();
    first
        .write_arrow_batch(&batch(vec![3, 1, 2], vec![30, 10, 20]))
        .await
        .unwrap();
    let messages = first.prepare_commit().await.unwrap();
    assert_eq!(messages.len(), 1);
    let file = &messages[0].new_files[0];
    assert!(file.file_name.ends_with(".mosaic"));
    assert_eq!(file.row_count, 3);
    assert_eq!(file.value_stats_cols.as_ref().unwrap(), &vec!["value"]);
    commit.commit(messages).await.unwrap();

    let mut second = TableWrite::new(&table, "mosaic-test".into()).unwrap();
    second
        .write_arrow_batch(&batch(vec![2, 4], vec![200, 40]))
        .await
        .unwrap();
    commit
        .commit(second.prepare_commit().await.unwrap())
        .await
        .unwrap();
    assert_eq!(
        read_pairs(&table).await,
        vec![(1, 10), (2, 200), (3, 30), (4, 40)]
    );
}

#[tokio::test]
async fn append_mosaic_without_stats_does_not_claim_pruning_data() {
    let io = memory_io();
    let path = "memory:/native_mosaic_no_stats";
    setup_dirs(&io, path).await;
    let table = table(&io, path, false, &[]);
    let mut writer = TableWrite::new(&table, "mosaic-test".into()).unwrap();
    writer
        .write_arrow_batch(&batch(vec![1, 2], vec![10, 20]))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    let file = &messages[0].new_files[0];
    assert_eq!(
        file.value_stats_cols.as_ref().unwrap(),
        &Vec::<String>::new()
    );
    assert!(file.value_stats.null_counts().is_empty());
    TableCommit::new(table.clone(), "mosaic-test".into())
        .commit(messages)
        .await
        .unwrap();
    assert_eq!(read_pairs(&table).await, vec![(1, 10), (2, 20)]);
}

#[tokio::test]
async fn partitioned_mosaic_writes_files_to_each_partition() {
    let io = memory_io();
    let path = "memory:/native_mosaic_partitioned";
    setup_dirs(&io, path).await;
    let schema = Schema::builder()
        .column("pt", DataType::VarChar(VarCharType::string_type()))
        .column("id", DataType::Int(IntType::new()))
        .partition_keys(["pt"])
        .option("file.format", "mosaic")
        .build()
        .unwrap();
    let table = Table::new(
        io.clone(),
        Identifier::new("default", "mosaic_partitioned"),
        path.to_owned(),
        TableSchema::new(0, &schema),
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("pt", ArrowType::Utf8, true),
            Field::new("id", ArrowType::Int32, true),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["east", "west", "east"])),
            Arc::new(Int32Array::from(vec![1, 2, 3])),
        ],
    )
    .unwrap();
    let mut writer = TableWrite::new(&table, "mosaic-test".into()).unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages.len(), 2);
    assert!(messages.iter().all(|message| message.new_files.len() == 1));
    assert!(messages
        .iter()
        .flat_map(|message| &message.new_files)
        .all(|file| file.file_name.ends_with(".mosaic")));
    TableCommit::new(table.clone(), "mosaic-test".into())
        .commit(messages)
        .await
        .unwrap();
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let read = table.new_read_builder().new_read().unwrap();
    let batches: Vec<RecordBatch> = read
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut rows = batches
        .iter()
        .flat_map(|batch| {
            let pt = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let id = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            (0..batch.num_rows())
                .map(|row| (pt.value(row).to_owned(), id.value(row)))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    rows.sort_unstable();
    assert_eq!(
        rows,
        vec![("east".into(), 1), ("east".into(), 3), ("west".into(), 2)]
    );
}

#[tokio::test]
async fn append_mosaic_rolls_files_at_target_size_and_preserves_all_rows() {
    let io = memory_io();
    let path = "memory:/native_mosaic_rolling";
    setup_dirs(&io, path).await;
    let table = table(
        &io,
        path,
        false,
        &[("target-file-size", "1b"), ("mosaic.stats-columns", "id")],
    );
    let mut writer = TableWrite::new(&table, "mosaic-test".into()).unwrap();
    writer
        .write_arrow_batch(&batch(vec![1, 2], vec![10, 20]))
        .await
        .unwrap();
    writer
        .write_arrow_batch(&batch(vec![3, 4], vec![30, 40]))
        .await
        .unwrap();
    writer
        .write_arrow_batch(&batch(vec![5, 6], vec![50, 60]))
        .await
        .unwrap();

    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages.len(), 1);
    let files = &messages[0].new_files;
    assert_eq!(files.len(), 3);
    assert_eq!(
        files.iter().map(|file| file.row_count).collect::<Vec<_>>(),
        vec![2, 2, 2]
    );
    assert!(files.iter().all(|file| file.file_name.ends_with(".mosaic")));
    for (index, file) in files.iter().enumerate() {
        let min = BinaryRow::from_serialized_bytes(file.value_stats.min_values()).unwrap();
        let max = BinaryRow::from_serialized_bytes(file.value_stats.max_values()).unwrap();
        assert_eq!(min.get_int(0).unwrap(), (index * 2 + 1) as i32);
        assert_eq!(max.get_int(0).unwrap(), (index * 2 + 2) as i32);
    }
    TableCommit::new(table.clone(), "mosaic-test".into())
        .commit(messages)
        .await
        .unwrap();
    assert_eq!(
        read_pairs(&table).await,
        vec![(1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60)]
    );
}

#[tokio::test]
async fn append_mosaic_stats_prune_manifest_files() {
    let io = memory_io();
    let path = "memory:/native_mosaic_pruning";
    setup_dirs(&io, path).await;
    let table = table(&io, path, false, &[("mosaic.stats-columns", "id")]);
    let commit = TableCommit::new(table.clone(), "mosaic-test".into());

    for (ids, values) in [
        (vec![1, 2], vec![10, 20]),
        (vec![100, 101], vec![1000, 1010]),
    ] {
        let mut writer = TableWrite::new(&table, "mosaic-test".into()).unwrap();
        writer.write_arrow_batch(&batch(ids, values)).await.unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        assert_eq!(
            messages[0].new_files[0].value_stats_cols.as_ref().unwrap(),
            &["id"]
        );
        commit.commit(messages).await.unwrap();
    }

    let predicate = PredicateBuilder::new(table.schema().fields())
        .greater_than("id", Datum::Int(10))
        .unwrap();
    let mut read = table.new_read_builder();
    read.with_filter(predicate);
    let (plan, trace) = read.new_scan().plan_with_trace().await.unwrap();
    assert_eq!(trace.final_files, 1, "scan trace: {trace:?}");
    assert!(trace.manifest_entries_pruned_by_data_stats >= 1);
    let batches: Vec<RecordBatch> = read
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let ids = batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            (0..array.len())
                .map(|index| array.value(index))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![100, 101]);
}

#[tokio::test]
async fn primary_key_mosaic_applies_retract_rows() {
    let io = memory_io();
    let path = "memory:/native_mosaic_retract";
    setup_dirs(&io, path).await;
    let table = table(&io, path, true, &[]);
    let commit = TableCommit::new(table.clone(), "mosaic-test".into());

    let mut first = TableWrite::new(&table, "mosaic-test".into()).unwrap();
    first
        .write_arrow_batch(&batch(vec![1, 2, 3], vec![10, 20, 30]))
        .await
        .unwrap();
    commit
        .commit(first.prepare_commit().await.unwrap())
        .await
        .unwrap();

    let input = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowType::Int32, true),
            Field::new("value", ArrowType::Int32, true),
            Field::new("_VALUE_KIND", ArrowType::Int8, false),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![2, 3, 4])),
            Arc::new(Int32Array::from(vec![20, 300, 40])),
            Arc::new(Int8Array::from(vec![3, 0, 0])),
        ],
    )
    .unwrap();
    let mut second = TableWrite::new(&table, "mosaic-test".into()).unwrap();
    second.write_arrow_batch(&input).await.unwrap();
    let messages = second.prepare_commit().await.unwrap();
    assert_eq!(messages[0].new_files[0].delete_row_count, Some(1));
    commit.commit(messages).await.unwrap();
    assert_eq!(read_pairs(&table).await, vec![(1, 10), (3, 300), (4, 40)]);
}

#[tokio::test]
async fn primary_key_mosaic_merges_latest_values_across_buffer_flushes() {
    let io = memory_io();
    let path = "memory:/native_mosaic_pk_rolling";
    setup_dirs(&io, path).await;
    let table = table(
        &io,
        path,
        true,
        &[
            ("target-file-size", "1b"),
            ("write-buffer-size", "1b"),
            ("mosaic.num-buckets", "2"),
        ],
    );
    let mut writer = TableWrite::new(&table, "mosaic-test".into()).unwrap();
    writer
        .write_arrow_batch(&batch(vec![1, 2], vec![10, 20]))
        .await
        .unwrap();
    writer
        .write_arrow_batch(&batch(vec![2, 3], vec![200, 30]))
        .await
        .unwrap();
    writer
        .write_arrow_batch(&batch(vec![1, 4], vec![100, 40]))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages.len(), 1);
    assert!(!messages[0].new_files.is_empty());
    assert!(messages[0]
        .new_files
        .iter()
        .all(|file| file.file_name.ends_with(".mosaic")));
    TableCommit::new(table.clone(), "mosaic-test".into())
        .commit(messages)
        .await
        .unwrap();
    assert_eq!(
        read_pairs(&table).await,
        vec![(1, 100), (2, 200), (3, 30), (4, 40)]
    );
}

#[tokio::test]
async fn append_mosaic_timestamp_zero_round_trips_through_table_reader() {
    let io = memory_io();
    let path = "memory:/native_mosaic_timestamp_zero";
    setup_dirs(&io, path).await;
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "event_time",
            DataType::Timestamp(TimestampType::new(0).unwrap()),
        )
        .option("file.format", "mosaic")
        .option("mosaic.stats-columns", "id")
        .build()
        .unwrap();
    let table = Table::new(
        io,
        Identifier::new("default", "mosaic_time"),
        path.to_owned(),
        TableSchema::new(0, &schema),
        None,
    );
    let input = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowType::Int32, true),
            Field::new(
                "event_time",
                ArrowType::Timestamp(arrow_schema::TimeUnit::Second, None),
                true,
            ),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(TimestampSecondArray::from(vec![Some(-1), None, Some(42)])),
        ],
    )
    .unwrap();
    let mut writer = TableWrite::new(&table, "mosaic-test".into()).unwrap();
    writer.write_arrow_batch(&input).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages[0].new_files[0].row_count, 3);
    TableCommit::new(table.clone(), "mosaic-test".into())
        .commit(messages)
        .await
        .unwrap();
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    let times = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<TimestampSecondArray>()
        .unwrap();
    assert_eq!(times.value(0), -1);
    assert!(times.is_null(1));
    assert_eq!(times.value(2), 42);
}

#[tokio::test]
async fn append_mosaic_rejects_invalid_table_options_before_commit() {
    for (case, options) in [
        ("compression", vec![("file.compression", "snappy")]),
        ("buckets", vec![("mosaic.num-buckets", "0")]),
        ("stats", vec![("mosaic.stats-columns", "missing")]),
    ] {
        let io = memory_io();
        let path = format!("memory:/native_mosaic_invalid_{case}");
        setup_dirs(&io, &path).await;
        let table = table(&io, &path, false, &options);
        let mut writer = TableWrite::new(&table, "mosaic-test".into()).unwrap();
        let error = writer
            .write_arrow_batch(&batch(vec![1], vec![10]))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains(case)
                || error.to_string().contains("zstd")
                || error.to_string().contains("positive")
                || error.to_string().contains("not in the file schema"),
            "{case}: {error}"
        );
    }
}

#[tokio::test]
async fn primary_key_input_changelog_writes_mosaic_data_and_changelog_files() {
    let io = memory_io();
    let path = "memory:/native_mosaic_input_changelog";
    setup_dirs(&io, path).await;
    let table = table(
        &io,
        path,
        true,
        &[
            ("changelog-producer", "input"),
            ("mosaic.stats-columns", "value"),
        ],
    );
    let input = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowType::Int32, true),
            Field::new("value", ArrowType::Int32, true),
            Field::new("_VALUE_KIND", ArrowType::Int8, false),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            Arc::new(Int8Array::from(vec![0, 3, 0])),
        ],
    )
    .unwrap();
    let mut writer = TableWrite::new(&table, "mosaic-test".into()).unwrap();
    writer.write_arrow_batch(&input).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].new_files.len(), 1);
    assert_eq!(messages[0].new_changelog_files.len(), 1);
    assert!(messages[0].new_files[0].file_name.ends_with(".mosaic"));
    assert!(messages[0].new_changelog_files[0]
        .file_name
        .ends_with(".mosaic"));
    assert_eq!(messages[0].new_files[0].delete_row_count, Some(1));
    assert_eq!(messages[0].new_changelog_files[0].delete_row_count, Some(1));
    TableCommit::new(table.clone(), "mosaic-test".into())
        .commit(messages)
        .await
        .unwrap();
    assert_eq!(read_pairs(&table).await, vec![(1, 10), (3, 30)]);
}

#[tokio::test]
async fn append_mosaic_write_can_create_file_index() {
    let io = memory_io();
    let path = "memory:/native_mosaic_bitmap_index";
    setup_dirs(&io, path).await;
    let table = table(
        &io,
        path,
        false,
        &[
            ("file-index.bitmap.columns", "id"),
            ("file-index.in-manifest-threshold", "0 B"),
        ],
    );
    let mut writer = TableWrite::new(&table, "mosaic-test".into()).unwrap();
    writer
        .write_arrow_batch(&batch(vec![1, 3], vec![10, 30]))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages[0].new_files.len(), 1);
    let file = &messages[0].new_files[0];
    assert!(file.file_name.ends_with(".mosaic"));
    assert_eq!(file.extra_files, vec![file.data_file_index_file_name()]);
    let index_path = format!(
        "{path}/{}/{}",
        crate::spec::bucket_dir_name(messages[0].bucket),
        file.data_file_index_file_name()
    );
    assert!(io.exists(&index_path).await.unwrap());
    TableCommit::new(table.clone(), "mosaic-test".into())
        .commit(messages)
        .await
        .unwrap();

    for (key, expected_rows) in [(2, 0), (3, 1)] {
        let predicate = PredicateBuilder::new(table.schema().fields())
            .equal("id", Datum::Int(key))
            .unwrap();
        let mut reader = table.new_read_builder();
        reader.with_filter(predicate);
        let plan = reader.new_scan().plan().await.unwrap();
        let batches: Vec<RecordBatch> = reader
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            expected_rows
        );
    }
}

#[tokio::test]
async fn append_mosaic_without_stats_keeps_files_in_filtered_plan() {
    let io = memory_io();
    let path = "memory:/native_mosaic_untracked_filter";
    setup_dirs(&io, path).await;
    let table = table(&io, path, false, &[]);
    let commit = TableCommit::new(table.clone(), "mosaic-test".into());
    for (ids, values) in [
        (vec![1, 2], vec![10, 20]),
        (vec![100, 101], vec![1000, 1010]),
    ] {
        let mut writer = TableWrite::new(&table, "mosaic-test".into()).unwrap();
        writer.write_arrow_batch(&batch(ids, values)).await.unwrap();
        commit
            .commit(writer.prepare_commit().await.unwrap())
            .await
            .unwrap();
    }
    let predicate = PredicateBuilder::new(table.schema().fields())
        .greater_than("id", Datum::Int(10))
        .unwrap();
    let mut reader = table.new_read_builder();
    reader.with_filter(predicate);
    let (_plan, trace) = reader.new_scan().plan_with_trace().await.unwrap();
    assert_eq!(trace.final_files, 2, "scan trace: {trace:?}");
    assert_eq!(trace.manifest_entries_pruned_by_data_stats, 0);
}

#[tokio::test]
async fn append_mosaic_all_null_statistics_remain_conservative() {
    let io = memory_io();
    let path = "memory:/native_mosaic_null_stats";
    setup_dirs(&io, path).await;
    let table = table(&io, path, false, &[("mosaic.stats-columns", "value")]);
    let input = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowType::Int32, true),
            Field::new("value", ArrowType::Int32, true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![None, None])),
        ],
    )
    .unwrap();
    let mut writer = TableWrite::new(&table, "mosaic-test".into()).unwrap();
    writer.write_arrow_batch(&input).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    let file = &messages[0].new_files[0];
    assert_eq!(file.value_stats_cols.as_ref().unwrap(), &["value"]);
    assert_eq!(file.value_stats.null_counts(), &[Some(2)]);
    let min = BinaryRow::from_serialized_bytes(file.value_stats.min_values()).unwrap();
    let max = BinaryRow::from_serialized_bytes(file.value_stats.max_values()).unwrap();
    assert!(min.is_null_at(0));
    assert!(max.is_null_at(0));
    TableCommit::new(table.clone(), "mosaic-test".into())
        .commit(messages)
        .await
        .unwrap();

    let predicate = PredicateBuilder::new(table.schema().fields())
        .is_null("value")
        .unwrap();
    let mut reader = table.new_read_builder();
    reader.with_filter(predicate);
    let (plan, trace) = reader.new_scan().plan_with_trace().await.unwrap();
    assert_eq!(trace.final_files, 1);
    let batches: Vec<RecordBatch> = reader
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    for batch in batches {
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.null_count(), values.len());
    }
}

#[tokio::test]
async fn partitioned_primary_key_mosaic_keeps_same_id_in_distinct_partitions() {
    let io = memory_io();
    let path = "memory:/native_mosaic_partitioned_pk";
    setup_dirs(&io, path).await;
    let schema = Schema::builder()
        .column("pt", DataType::VarChar(VarCharType::string_type()))
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .partition_keys(["pt"])
        .primary_key(["pt", "id"])
        .option("bucket", "1")
        .option("file.format", "mosaic")
        .build()
        .unwrap();
    let table = Table::new(
        io,
        Identifier::new("default", "mosaic_partitioned_pk"),
        path.to_owned(),
        TableSchema::new(0, &schema),
        None,
    );
    let make_input = |partitions: Vec<&str>, values: Vec<i32>| {
        let count = partitions.len();
        RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("pt", ArrowType::Utf8, true),
                Field::new("id", ArrowType::Int32, true),
                Field::new("value", ArrowType::Int32, true),
            ])),
            vec![
                Arc::new(StringArray::from(partitions)),
                Arc::new(Int32Array::from(vec![1; count])),
                Arc::new(Int32Array::from(values)),
            ],
        )
        .unwrap()
    };
    let commit = TableCommit::new(table.clone(), "mosaic-test".into());
    let mut first = TableWrite::new(&table, "mosaic-test".into()).unwrap();
    first
        .write_arrow_batch(&make_input(vec!["east", "west"], vec![10, 20]))
        .await
        .unwrap();
    let messages = first.prepare_commit().await.unwrap();
    assert_eq!(messages.len(), 2);
    commit.commit(messages).await.unwrap();

    let mut second = TableWrite::new(&table, "mosaic-test".into()).unwrap();
    second
        .write_arrow_batch(&make_input(vec!["east"], vec![100]))
        .await
        .unwrap();
    commit
        .commit(second.prepare_commit().await.unwrap())
        .await
        .unwrap();
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut rows = batches
        .iter()
        .flat_map(|batch| {
            let partitions = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let values = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            (0..batch.num_rows())
                .map(|row| (partitions.value(row).to_owned(), values.value(row)))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    rows.sort_unstable();
    assert_eq!(rows, vec![("east".into(), 100), ("west".into(), 20)]);
}

#[tokio::test]
async fn append_mosaic_filters_rows_when_predicate_column_has_no_stats() {
    let io = memory_io();
    let path = "memory:/native_mosaic_residual_filter";
    setup_dirs(&io, path).await;
    let table = table(&io, path, false, &[("mosaic.stats-columns", "id")]);
    let mut writer = TableWrite::new(&table, "mosaic-test".into()).unwrap();
    writer
        .write_arrow_batch(&batch(vec![1, 2, 3, 4], vec![5, 20, 10, 30]))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(
        messages[0].new_files[0].value_stats_cols.as_ref().unwrap(),
        &["id"]
    );
    TableCommit::new(table.clone(), "mosaic-test".into())
        .commit(messages)
        .await
        .unwrap();

    let predicate = PredicateBuilder::new(table.schema().fields())
        .greater_than("value", Datum::Int(10))
        .unwrap();
    let mut reader = table.new_read_builder();
    reader.with_filter(predicate);
    let (plan, trace) = reader.new_scan().plan_with_trace().await.unwrap();
    assert_eq!(trace.final_files, 1);
    let batches: Vec<RecordBatch> = reader
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut result = batches
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            (0..batch.num_rows())
                .map(|row| (ids.value(row), values.value(row)))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    result.sort_unstable();
    assert_eq!(result, vec![(2, 20), (4, 30)]);
}
