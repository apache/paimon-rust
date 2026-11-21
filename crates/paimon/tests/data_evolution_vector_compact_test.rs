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
//   Unless required by applicable law or agreed to in writing,
//   software distributed under the License is distributed on an
//   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//   KIND, either express or implied.  See the License for the
//   specific language governing permissions and limitations
//   under the License.

mod common;

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{
    types::Float32Type, Array, ArrayRef, FixedSizeListArray, Float32Array, Int32Array, Int64Array,
    LargeBinaryArray, RecordBatch,
};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs, write_batch};
use futures::TryStreamExt;
use paimon::spec::{
    BlobType, DataFileMeta, DataType, FloatType, IndexManifest, IntType, Schema, SchemaChange,
    TableSchema, VectorType,
};
use paimon::table::{
    CommitCallback, CommitCallbackContext, CommitMessage, CompactPlan, CompactResult,
    DataEvolutionDeleteWriter, RowRange, Table,
};

fn vector_type() -> DataType {
    DataType::Vector(VectorType::try_new(true, 2, DataType::Float(FloatType::new())).unwrap())
}

async fn table(options: &[(&str, &str)]) -> Table {
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("a", vector_type())
        .column("b", vector_type())
        .column("blob", DataType::Blob(BlobType::new()))
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .option("vector.file.format", "parquet")
        .option("compaction.min.file-num", "2")
        .option("read.batch-size", "2");
    for (key, value) in options {
        schema = schema.option(*key, *value);
    }
    let location = format!("memory:/vector_compact/{}", uuid::Uuid::new_v4());
    let (io, table) = memory_table(&location, TableSchema::new(0, &schema.build().unwrap()));
    setup_dirs(&io, &location).await;
    persist_table_schema(&io, &location, table.schema()).await;
    table
}

fn vector(values: &[Option<f32>]) -> ArrayRef {
    Arc::new(
        FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
            values
                .iter()
                .map(|value| value.map(|v| vec![Some(v), Some(-v)])),
            2,
        ),
    )
}

async fn append(table: &Table, first: i32, count: i32) {
    let values = (first..first + count)
        .map(|v| Some(v as f32))
        .collect::<Vec<_>>();
    write_batch(
        table,
        &RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int32Array::from_iter_values(first..first + count)) as ArrayRef,
            ),
            ("a", vector(&values)),
            ("b", vector(&values)),
            (
                "blob",
                Arc::new(LargeBinaryArray::from(vec![
                    Some(b"payload".as_slice());
                    count as usize
                ])) as ArrayRef,
            ),
        ])
        .unwrap(),
    )
    .await;
}

async fn update(table: &Table, field: &str, row: i64, value: Option<f32>) {
    let builder = table.new_write_builder();
    let messages = builder
        .new_update()
        .unwrap()
        .update_by_arrow_with_row_id(vec![RecordBatch::try_from_iter([
            ("_ROW_ID", Arc::new(Int64Array::from(vec![row])) as ArrayRef),
            (field, vector(&[value])),
        ])
        .unwrap()])
        .await
        .unwrap();
    builder.new_commit().commit(messages).await.unwrap();
}

async fn delete(table: &Table, row: i64) {
    let mut writer = DataEvolutionDeleteWriter::new(table).unwrap();
    writer.add_row_ids([row]).unwrap();
    table
        .new_write_builder()
        .new_commit()
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();
}

async fn rows(table: &Table, columns: &[&str]) -> Vec<(i64, Vec<Option<f32>>)> {
    rows_in_ranges(table, columns, None).await
}

async fn rows_in_ranges(
    table: &Table,
    columns: &[&str],
    ranges: Option<Vec<RowRange>>,
) -> Vec<(i64, Vec<Option<f32>>)> {
    let mut read = table.new_read_builder();
    let mut projection = vec!["_ROW_ID"];
    projection.extend_from_slice(columns);
    read.with_projection(&projection).unwrap();
    if let Some(ranges) = ranges {
        read.with_row_ranges(ranges);
    }
    let plan = read.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = read
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                ids.value(row),
                batch.columns()[1..]
                    .iter()
                    .map(|column| {
                        let array = column
                            .as_any()
                            .downcast_ref::<FixedSizeListArray>()
                            .unwrap();
                        (!array.is_null(row)).then(|| {
                            array
                                .value(row)
                                .as_any()
                                .downcast_ref::<Float32Array>()
                                .unwrap()
                                .value(0)
                        })
                    })
                    .collect(),
            ));
        }
    }
    rows.sort_by_key(|row| row.0);
    rows
}

async fn full_rows(table: &Table) -> Vec<String> {
    let builder = table.new_read_builder();
    let plan = builder.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| format!("{:?}", column.slice(row, 1)))
                    .collect::<Vec<_>>()
                    .join("|"),
            );
        }
    }
    rows.sort();
    rows
}

async fn snapshot(table: &Table) -> i64 {
    table
        .snapshot_manager()
        .get_latest_snapshot_id()
        .await
        .unwrap()
        .unwrap()
}

async fn plan(table: &Table) -> CompactPlan {
    let plan = table
        .new_compact_builder()
        .with_vector_compaction(true)
        .new_planner()
        .unwrap()
        .plan()
        .await
        .unwrap();
    serde_json::from_value(serde_json::to_value(plan).unwrap()).unwrap()
}

async fn execute(table: &Table, plan: &CompactPlan) -> Vec<CompactResult> {
    let executor = table.new_compact_builder().new_executor().unwrap();
    let mut results = Vec::new();
    for task in plan.tasks() {
        results.push(executor.execute(task).await.unwrap());
    }
    serde_json::from_value(serde_json::to_value(results).unwrap()).unwrap()
}

async fn compact(table: &Table) -> Vec<CompactResult> {
    let results = execute(table, &plan(table).await).await;
    table
        .new_compact_builder()
        .new_commit()
        .unwrap()
        .commit_compact(results.clone())
        .await
        .unwrap();
    results
}

async fn physical_files(table: &Table) -> HashSet<String> {
    table
        .file_io()
        .list_status_recursive(table.location())
        .await
        .unwrap()
        .into_iter()
        .filter(|entry| {
            entry.path.ends_with(".parquet")
                || entry.path.ends_with(".blob")
                || entry.path.ends_with(".vortex")
        })
        .map(|entry| entry.path)
        .collect()
}

async fn active_files(table: &Table) -> Vec<DataFileMeta> {
    table
        .new_read_builder()
        .new_scan()
        .plan()
        .await
        .unwrap()
        .splits()
        .iter()
        .flat_map(|split| split.data_files().to_vec())
        .collect()
}

async fn indexes(table: &Table) -> Vec<paimon::spec::IndexManifestEntry> {
    let manager = table.snapshot_manager();
    let snapshot = manager.get_latest_snapshot().await.unwrap().unwrap();
    IndexManifest::read(
        table.file_io(),
        &manager.manifest_path(snapshot.index_manifest().unwrap()),
    )
    .await
    .unwrap()
}

async fn evolve(table: &Table, changes: Vec<SchemaChange>) -> Table {
    let schema = table.schema().apply_changes(changes).unwrap();
    persist_table_schema(table.file_io(), table.location(), &schema).await;
    table.copy_with_resolved_schema(schema, "main").unwrap()
}

async fn write_vector_range(
    table: &Table,
    first: i64,
    columns: &[(&str, &[Option<f32>])],
) -> DataFileMeta {
    let batch =
        RecordBatch::try_from_iter(columns.iter().map(|(name, values)| (*name, vector(values))))
            .unwrap();
    let mut bytes = Vec::new();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(&mut bytes, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let mut meta = active_files(table)
        .await
        .into_iter()
        .find(|file| file.file_name.contains(".vector."))
        .unwrap();
    meta.file_name = format!("{}.vector.parquet", uuid::Uuid::new_v4());
    meta.file_size = bytes.len() as i64;
    meta.row_count = batch.num_rows() as i64;
    meta.first_row_id = Some(first);
    meta.schema_id = table.schema().id();
    meta.min_sequence_number = 0;
    meta.max_sequence_number = 0;
    meta.column_max_sequence_numbers = None;
    meta.value_stats = meta.key_stats.clone();
    meta.write_cols = Some(columns.iter().map(|(name, _)| name.to_string()).collect());
    table
        .file_io()
        .new_output(&format!("{}/bucket-0/{}", table.location(), meta.file_name))
        .unwrap()
        .write(bytes.into())
        .await
        .unwrap();
    meta
}

async fn publish_vector_range(table: &Table, first: i64, columns: &[(&str, &[Option<f32>])]) {
    let mut message = CommitMessage::new(
        paimon::spec::BinaryRow::new(0).to_serialized_bytes(),
        0,
        vec![write_vector_range(table, first, columns).await],
    );
    message.check_from_snapshot = Some(snapshot(table).await);
    table
        .new_write_builder()
        .new_commit()
        .commit(vec![message])
        .await
        .unwrap();
}

#[tokio::test]
async fn overlapping_field_versions_nulls_history_indexes_and_recovery() {
    struct FailAfterPublication;
    #[async_trait::async_trait]
    impl CommitCallback for FailAfterPublication {
        async fn call(&self, _: &CommitCallbackContext) -> paimon::Result<()> {
            Err(paimon::Error::UnexpectedError {
                message: "injected vector callback failure".into(),
                source: None,
            })
        }
    }
    let table = table(&[
        ("deletion-vectors.enabled", "true"),
        ("global-index.column-update-action", "ignore"),
    ])
    .await;
    append(&table, 0, 6).await;
    let original = rows(&table, &["a", "b"]).await;
    publish_vector_range(&table, 1, &[("a", &[Some(11.0), None, Some(13.0)])]).await;
    publish_vector_range(&table, 3, &[("b", &[Some(23.0), Some(24.0)])]).await;
    publish_vector_range(&table, 2, &[("a", &[Some(12.0), None])]).await;
    table
        .new_global_index_build_builder()
        .with_index_type("bitmap")
        .with_index_column("id")
        .execute()
        .await
        .unwrap();
    let current = rows(&table, &["a", "b"]).await;
    assert_eq!(
        current.iter().map(|row| row.1.clone()).collect::<Vec<_>>(),
        vec![
            vec![Some(0.0), Some(0.0)],
            vec![Some(11.0), Some(1.0)],
            vec![Some(12.0), Some(2.0)],
            vec![None, Some(23.0)],
            vec![Some(4.0), Some(24.0)],
            vec![Some(5.0), Some(5.0)]
        ]
    );
    let history = table.copy_with_options([("scan.snapshot-id".into(), "1".into())].into());
    let captured = plan(&table).await;
    assert_eq!(captured.tasks().len(), 1);
    let before = physical_files(&table).await;
    let aborted = execute(&table, &captured).await;
    table
        .new_compact_builder()
        .new_commit()
        .unwrap()
        .abort_compact(&aborted)
        .await
        .unwrap();
    assert_eq!(physical_files(&table).await, before);
    let results = execute(&table, &captured).await;
    assert_eq!(results[0].metrics().files_removed, 4);
    assert_eq!(results[0].metrics().files_added, 1);
    assert_eq!(
        results[0].files()[0].column_max_sequence_numbers,
        Some(vec![4, 3])
    );
    delete(&table, 4).await;
    let expected = rows(&table, &["a", "b"]).await;
    let ranges = Some(vec![RowRange::new(0, 0), RowRange::new(2, 5)]);
    let selected = vec![
        (0, vec![Some(0.0)]),
        (2, vec![Some(12.0)]),
        (3, vec![None]),
        (5, vec![Some(5.0)]),
    ];
    assert_eq!(
        rows_in_ranges(&table, &["a"], ranges.clone()).await,
        selected
    );
    let full = full_rows(&table).await;
    let expected_indexes = indexes(&table).await;
    let mut commit = table
        .new_compact_builder()
        .with_commit_user("vector-retry")
        .unwrap()
        .new_commit()
        .unwrap();
    commit
        .add_commit_callback(Arc::new(FailAfterPublication))
        .unwrap();
    assert!(commit
        .commit_compact(results.clone())
        .await
        .unwrap_err()
        .to_string()
        .contains("injected vector callback"));
    assert_eq!(rows(&table, &["a", "b"]).await, expected);
    assert_eq!(rows_in_ranges(&table, &["a"], ranges).await, selected);
    assert_eq!(full_rows(&table).await, full);
    assert_eq!(indexes(&table).await, expected_indexes);
    assert_eq!(rows(&history, &["a", "b"]).await, original);
    assert!(before.is_subset(&physical_files(&table).await));
    assert!(plan(&table).await.tasks().is_empty());
    update(&table, "a", 0, Some(99.0)).await;
    update(&table, "b", 3, None).await;
    let mut updated = expected;
    updated[0].1[0] = Some(99.0);
    updated[3].1[1] = None;
    assert_eq!(rows(&table, &["a", "b"]).await, updated);
    let latest = snapshot(&table).await;
    table
        .new_compact_builder()
        .with_commit_user("vector-retry")
        .unwrap()
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap();
    assert_eq!(snapshot(&table).await, latest);
    compact(&table).await;
    assert_eq!(rows(&table, &["a", "b"]).await, updated);
}

#[tokio::test]
async fn same_sequence_vector_files_require_disjoint_ranges_per_field() {
    for (field, first) in [("a", 2), ("b", 1), ("a", 1)] {
        let table = table(&[]).await;
        append(&table, 0, 4).await;
        let mut message = CommitMessage::new(
            paimon::spec::BinaryRow::new(0).to_serialized_bytes(),
            0,
            vec![
                write_vector_range(&table, 1, &[("a", &[Some(10.0)])]).await,
                write_vector_range(&table, first, &[(field, &[Some(20.0)])]).await,
            ],
        );
        message.check_from_snapshot = Some(snapshot(&table).await);
        table
            .new_write_builder()
            .new_commit()
            .commit(vec![message])
            .await
            .unwrap();
        let captured = plan(&table).await;
        assert_eq!(captured.tasks().len(), 1);
        if field == "a" && first == 1 {
            let before = physical_files(&table).await;
            let error = table
                .new_compact_builder()
                .new_executor()
                .unwrap()
                .execute(&captured.tasks()[0])
                .await
                .unwrap_err();
            assert!(
                error.to_string().contains("same sequence overlap"),
                "{error}"
            );
            assert_eq!(physical_files(&table).await, before);
            assert_eq!(snapshot(&table).await, 2);
        } else {
            let mut expected = (0..4)
                .map(|id| (id, vec![Some(id as f32), Some(id as f32)]))
                .collect::<Vec<_>>();
            expected[1].1[0] = Some(10.0);
            expected[first as usize].1[usize::from(field == "b")] = Some(20.0);
            assert_eq!(rows(&table, &["a", "b"]).await, expected);
            let results = compact(&table).await;
            assert_eq!(results[0].metrics().files_removed, 3);
            assert_eq!(results[0].metrics().files_added, 1);
            assert_eq!(rows(&table, &["a", "b"]).await, expected);
        }
    }
}

#[tokio::test]
async fn rolling_vector_outputs_with_blob_and_normal_merges_preserve_deleted_slots() {
    let table = table(&[
        ("deletion-vectors.enabled", "true"),
        ("vector.target-file-size", "1 b"),
    ])
    .await;
    append(&table, 0, 4).await;
    append(&table, 4, 4).await;
    update(&table, "a", 1, None).await;
    let captured = table
        .new_compact_builder()
        .with_vector_compaction(true)
        .with_blob_compaction(true)
        .new_planner()
        .unwrap()
        .plan()
        .await
        .unwrap();
    assert_eq!(captured.tasks().len(), 3);
    let results = execute(&table, &captured).await;
    let vector_result = results
        .iter()
        .find(|result| result.files()[0].file_name.contains(".vector."))
        .unwrap();
    assert!(vector_result.files().len() > 1);
    assert_eq!(
        vector_result
            .files()
            .iter()
            .map(|file| file.row_count)
            .sum::<i64>(),
        8
    );
    let mut next = 0;
    for file in vector_result.files() {
        assert_eq!(file.first_row_id, Some(next));
        assert!(file.file_name.ends_with(".vector.parquet"));
        next += file.row_count;
    }
    delete(&table, 6).await;
    let before = rows(&table, &["a", "b"]).await;
    let full = full_rows(&table).await;
    table
        .new_compact_builder()
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap();
    assert_eq!(rows(&table, &["a", "b"]).await, before);
    assert_eq!(full_rows(&table).await, full);
    assert!(
        plan(&table).await.tasks().is_empty(),
        "rolled files already meet target"
    );
    update(&table, "a", 7, Some(70.0)).await;
    let updated = full_rows(&table).await;
    compact(&table).await;
    assert_eq!(full_rows(&table).await, updated);
    assert!(plan(&table).await.tasks().is_empty());
}

#[tokio::test]
async fn compressed_rolled_outputs_are_not_rewritten_without_new_inputs() {
    for format in [
        "parquet",
        #[cfg(feature = "vortex")]
        "vortex",
    ] {
        let table = table(&[
            ("vector.target-file-size", "16 kb"),
            ("vector.file.format", format),
            ("file.compression", "zstd"),
            ("read.batch-size", "1024"),
        ])
        .await;
        append(&table, 0, 4096).await;
        append(&table, 4096, 4096).await;
        update(&table, "a", 0, Some(-1.0)).await;
        let before = rows(&table, &["a", "b"]).await;
        let results = compact(&table).await;
        let vector_result = results
            .iter()
            .find(|result| result.files()[0].file_name.contains(".vector."))
            .unwrap();
        assert!(vector_result.files().len() > 1);
        assert_eq!(rows(&table, &["a", "b"]).await, before);
        assert!(
            plan(&table).await.tasks().is_empty(),
            "{format} completed outputs: {:?}",
            vector_result
                .files()
                .iter()
                .map(|file| (file.row_count, file.file_size))
                .collect::<Vec<_>>()
        );
        update(&table, "b", 8191, Some(-2.0)).await;
        assert!(!plan(&table).await.tasks().is_empty());
        let updated = rows(&table, &["a", "b"]).await;
        compact(&table).await;
        assert_eq!(rows(&table, &["a", "b"]).await, updated);
        assert!(plan(&table).await.tasks().is_empty());
    }
}

#[tokio::test]
async fn cross_group_outputs_require_normal_merge_and_subsets_can_commit_later() {
    let table = table(&[]).await;
    append(&table, 0, 2).await;
    append(&table, 2, 2).await;
    let results = execute(&table, &plan(&table).await).await;
    assert_eq!(results.len(), 2);
    let (vectors, normals): (Vec<_>, Vec<_>) = results
        .into_iter()
        .partition(|result| result.files()[0].file_name.contains(".vector."));
    let commit = table.new_compact_builder().new_commit().unwrap();
    let error = commit.commit_compact(vectors.clone()).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("not covered by one data file range"),
        "{error}"
    );
    assert_eq!(snapshot(&table).await, 2);
    commit.commit_compact(normals).await.unwrap();
    let expected = full_rows(&table).await;
    commit.commit_compact(vectors).await.unwrap();
    assert_eq!(full_rows(&table).await, expected);
    assert!(plan(&table).await.tasks().is_empty());
}

#[tokio::test]
async fn independent_normal_groups_and_gaps_are_not_combined() {
    let table = table(&[("target-file-size", "1 b")]).await;
    append(&table, 0, 2).await;
    append(&table, 2, 2).await;
    append(&table, 4, 2).await;
    update(&table, "a", 0, Some(10.0)).await;
    update(&table, "a", 4, Some(20.0)).await;
    let captured = plan(&table).await;
    assert_eq!(captured.tasks().len(), 2);
    assert!(captured.tasks().iter().all(|task| task
        .files()
        .iter()
        .all(|file| file.file_name.contains(".vector."))));
    let results = execute(&table, &captured).await;
    let expected = rows(&table, &["a", "b"]).await;
    let commit = table.new_compact_builder().new_commit().unwrap();
    commit
        .commit_compact(vec![results[1].clone()])
        .await
        .unwrap();
    commit
        .commit_compact(vec![results[0].clone()])
        .await
        .unwrap();
    assert_eq!(rows(&table, &["a", "b"]).await, expected);
}

#[tokio::test]
async fn incomplete_versions_fields_and_forged_outputs_cannot_publish() {
    let table = table(&[]).await;
    append(&table, 0, 4).await;
    publish_vector_range(&table, 1, &[("a", &[Some(10.0)])]).await;
    publish_vector_range(&table, 2, &[("b", &[Some(20.0)])]).await;
    let captured = plan(&table).await;
    let executor = table.new_compact_builder().new_executor().unwrap();
    let before = physical_files(&table).await;
    for (key, value) in [
        ("vector_field_ids", serde_json::json!(null)),
        ("vector_field_ids", serde_json::json!([])),
        ("vector_field_ids", serde_json::json!([0])),
        ("vector_field_ids", serde_json::json!([1])),
        ("vector_field_ids", serde_json::json!([1, 1])),
        ("vector_field_ids", serde_json::json!([2, 1])),
        ("blob_field_id", serde_json::json!(3)),
    ] {
        let mut task = serde_json::to_value(&captured.tasks()[0]).unwrap();
        task[key] = value;
        assert!(executor
            .execute(&serde_json::from_value(task).unwrap())
            .await
            .is_err());
        assert_eq!(physical_files(&table).await, before);
    }
    let mut task = serde_json::to_value(&captured.tasks()[0]).unwrap();
    task["files"].as_array_mut().unwrap().remove(1);
    let error = table
        .new_compact_builder()
        .new_executor()
        .unwrap()
        .execute(&serde_json::from_value(task).unwrap())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("omit overlapping"), "{error}");
    let results = execute(&table, &captured).await;
    let commit = table.new_compact_builder().new_commit().unwrap();
    let mut invalid_outputs = Vec::new();
    for (key, value) in [
        ("_FIRST_ROW_ID", serde_json::json!(1)),
        ("_ROW_COUNT", serde_json::json!(3)),
        ("_MIN_SEQUENCE_NUMBER", serde_json::json!(2)),
        ("_MAX_SEQUENCE_NUMBER", serde_json::json!(100)),
        ("_WRITE_COLS_SEQUENCES", serde_json::json!([1, 1])),
        ("_WRITE_COLS", serde_json::json!(["a"])),
        ("_FILE_NAME", serde_json::json!("wrong.parquet")),
    ] {
        let mut files = serde_json::to_value(results[0].files()).unwrap();
        files[0][key] = value;
        invalid_outputs.push((key, files));
    }
    invalid_outputs.extend([
        ("empty outputs", serde_json::json!([])),
        (
            "overlapping outputs",
            serde_json::json!([results[0].files()[0], results[0].files()[0]]),
        ),
    ]);
    for (case, files) in invalid_outputs {
        let mut broken = serde_json::to_value(&results).unwrap();
        broken[0]["files"] = files;
        assert!(
            commit
                .commit_compact(serde_json::from_value(broken).unwrap())
                .await
                .is_err(),
            "{case}"
        );
        assert_eq!(snapshot(&table).await, 3);
    }
    let mut missing = serde_json::to_value(&results).unwrap();
    missing[0]["task"]["files"]
        .as_array_mut()
        .unwrap()
        .remove(1);
    assert!(commit
        .commit_compact(serde_json::from_value(missing).unwrap())
        .await
        .is_err());
    let before = rows(&table, &["a", "b"]).await;
    commit.commit_compact(results).await.unwrap();
    assert_eq!(rows(&table, &["a", "b"]).await, before);
}

#[tokio::test]
async fn concurrent_vector_writes_conflict_but_disjoint_appends_and_normal_updates_commute() {
    let table = table(&[]).await;
    append(&table, 0, 4).await;
    update(&table, "a", 0, Some(10.0)).await;
    let results = execute(&table, &plan(&table).await).await;
    publish_vector_range(&table, 1, &[("b", &[Some(20.0)])]).await;
    let commit = table.new_compact_builder().new_commit().unwrap();
    assert!(commit
        .commit_compact(results.clone())
        .await
        .unwrap_err()
        .to_string()
        .contains("omit overlapping"));
    for result in &results {
        for file in result.files() {
            assert!(table
                .file_io()
                .exists(&format!("{}/bucket-0/{}", table.location(), file.file_name))
                .await
                .unwrap());
        }
    }
    let results = execute(&table, &plan(&table).await).await;
    append(&table, 4, 2).await;
    let builder = table.new_write_builder();
    let messages = builder
        .new_update()
        .unwrap()
        .update_by_arrow_with_row_id(vec![RecordBatch::try_from_iter([
            ("_ROW_ID", Arc::new(Int64Array::from(vec![0])) as ArrayRef),
            ("id", Arc::new(Int32Array::from(vec![100])) as ArrayRef),
        ])
        .unwrap()])
        .await
        .unwrap();
    builder.new_commit().commit(messages).await.unwrap();
    let before = full_rows(&table).await;
    commit.commit_compact(results).await.unwrap();
    assert_eq!(full_rows(&table).await, before);
}

#[tokio::test]
async fn schema_identity_renames_dropped_recreated_and_absent_fields() {
    let table = table(&[]).await;
    append(&table, 0, 4).await;
    let table = evolve(
        &table,
        vec![SchemaChange::RenameColumn {
            field_names: vec!["a".into()],
            new_name: "renamed".into(),
        }],
    )
    .await;
    update(&table, "renamed", 1, Some(10.0)).await;
    let table = evolve(
        &table,
        vec![SchemaChange::DropColumn {
            field_names: vec!["b".into()],
        }],
    )
    .await;
    let table = evolve(
        &table,
        vec![SchemaChange::AddColumn {
            field_names: vec!["b".into()],
            data_type: vector_type(),
            comment: None,
            column_move: None,
        }],
    )
    .await;
    let expected = rows(&table, &["renamed", "b"]).await;
    assert!(expected.iter().all(|row| row.1[1].is_none()));
    let results = compact(&table).await;
    assert_eq!(
        results[0].files()[0].write_cols,
        Some(vec!["renamed".into()])
    );
    assert_eq!(rows(&table, &["renamed", "b"]).await, expected);
    update(&table, "b", 0, Some(99.0)).await;
    let expected = rows(&table, &["renamed", "b"]).await;
    compact(&table).await;
    assert_eq!(rows(&table, &["renamed", "b"]).await, expected);
}

#[tokio::test]
async fn partial_field_coverage_is_skipped_and_cannot_be_forged() {
    let table = table(&[]).await;
    append(&table, 0, 4).await;
    update(&table, "a", 0, Some(10.0)).await;
    let captured = plan(&table).await;
    let table = evolve(
        &table,
        vec![SchemaChange::AddColumn {
            field_names: vec!["c".into()],
            data_type: vector_type(),
            comment: None,
            column_move: None,
        }],
    )
    .await;
    publish_vector_range(&table, 1, &[("c", &[Some(77.0)])]).await;
    assert!(plan(&table).await.tasks().is_empty());
    let mut task = serde_json::to_value(&captured.tasks()[0]).unwrap();
    task["context"]["schema"] = serde_json::to_value(table.schema()).unwrap();
    task["context"]["snapshot"] = serde_json::to_value(
        table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    task["files"] = serde_json::to_value(
        active_files(&table)
            .await
            .into_iter()
            .filter(|file| file.file_name.contains(".vector."))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let before = physical_files(&table).await;
    assert!(table
        .new_compact_builder()
        .new_executor()
        .unwrap()
        .execute(&serde_json::from_value(task).unwrap())
        .await
        .is_err());
    assert_eq!(physical_files(&table).await, before);
}

#[tokio::test]
async fn partial_combined_coverage_falls_back_to_original_normal_groups() {
    for normal_first in [false, true] {
        let table = table(&[]).await;
        append(&table, 0, 2).await;
        append(&table, 2, 2).await;
        let table = evolve(
            &table,
            vec![SchemaChange::AddColumn {
                field_names: vec!["c".into()],
                data_type: vector_type(),
                comment: None,
                column_move: None,
            }],
        )
        .await;
        publish_vector_range(&table, 0, &[("c", &[Some(30.0), None])]).await;
        let captured = plan(&table).await;
        assert_eq!(captured.tasks().len(), 2);
        let (vectors, normals): (Vec<_>, Vec<_>) = execute(&table, &captured)
            .await
            .into_iter()
            .partition(|result| result.files()[0].file_name.contains(".vector."));
        assert_eq!(vectors.len(), 1);
        assert_eq!(vectors[0].files()[0].row_id_range(), Some((0, 1)));
        assert_eq!(
            vectors[0].files()[0].write_cols,
            Some(vec!["a".into(), "b".into(), "c".into()])
        );
        let before = full_rows(&table).await;
        let commit = table.new_compact_builder().new_commit().unwrap();
        let subsets = if normal_first {
            [normals, vectors]
        } else {
            [vectors, normals]
        };
        for subset in subsets {
            commit.commit_compact(subset).await.unwrap();
            assert_eq!(full_rows(&table).await, before);
        }
        assert!(plan(&table).await.tasks().is_empty());
        update(&table, "c", 2, Some(40.0)).await;
        let expected = vec![
            (0, vec![Some(30.0)]),
            (1, vec![None]),
            (2, vec![Some(40.0)]),
            (3, vec![None]),
        ];
        assert_eq!(rows(&table, &["c"]).await, expected);
        compact(&table).await;
        assert_eq!(rows(&table, &["c"]).await, expected);
    }
}

#[tokio::test]
async fn late_read_failure_cleans_rolled_outputs() {
    let table = table(&[("vector.target-file-size", "1 b")]).await;
    append(&table, 0, 4).await;
    publish_vector_range(&table, 3, &[("a", &[Some(10.0)])]).await;
    let captured = plan(&table).await;
    let missing = captured.tasks()[0]
        .files()
        .iter()
        .max_by_key(|file| file.max_sequence_number)
        .unwrap();
    table
        .file_io()
        .delete_file(&format!(
            "{}/bucket-0/{}",
            table.location(),
            missing.file_name
        ))
        .await
        .unwrap();
    let before = physical_files(&table).await;
    assert!(table
        .new_compact_builder()
        .new_executor()
        .unwrap()
        .execute(&captured.tasks()[0])
        .await
        .is_err());
    assert_eq!(physical_files(&table).await, before);
    assert_eq!(snapshot(&table).await, 2);
}

#[tokio::test]
async fn vector_compaction_is_opt_in_and_requires_valid_configuration() {
    let table = table(&[]).await;
    append(&table, 0, 4).await;
    update(&table, "a", 0, Some(10.0)).await;
    assert!(table
        .new_compact_builder()
        .new_planner()
        .unwrap()
        .plan()
        .await
        .unwrap()
        .tasks()
        .is_empty());
    let minimum = table.copy_with_options([("compaction.min.file-num".into(), "3".into())].into());
    assert!(plan(&minimum).await.tasks().is_empty());
    assert_eq!(plan(&table).await.tasks().len(), 1);
    for (key, value) in [
        ("vector.target-file-size", "0 b"),
        ("vector.file.format", ""),
    ] {
        let invalid = table.copy_with_options([(key.into(), value.into())].into());
        assert!(invalid
            .new_compact_builder()
            .with_vector_compaction(true)
            .new_planner()
            .unwrap()
            .plan()
            .await
            .is_err());
    }
    let plain = table.copy_with_options(
        [
            ("data-evolution.enabled".into(), "false".into()),
            ("row-tracking.enabled".into(), "false".into()),
        ]
        .into(),
    );
    assert!(plain
        .new_compact_builder()
        .with_vector_compaction(true)
        .new_planner()
        .is_err());
}

#[tokio::test]
async fn compacts_java_parquet_vectors_and_accepts_later_updates() {
    let dtype =
        DataType::Vector(VectorType::try_new(true, 3, DataType::Float(FloatType::new())).unwrap());
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("f_vector_float", dtype)
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .option("vector.file.format", "parquet")
        .option("compaction.min.file-num", "2")
        .build()
        .unwrap();
    let mut schema = serde_json::to_value(TableSchema::new(0, &schema)).unwrap();
    schema["fields"][1]["id"] = 6.into();
    schema["highestFieldId"] = 6.into();
    let (io, table) = memory_table(
        "memory:/java_vector_compaction",
        serde_json::from_value(schema).unwrap(),
    );
    setup_dirs(&io, table.location()).await;
    persist_table_schema(&io, table.location(), table.schema()).await;
    let values = Arc::new(
        FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
            [
                Some(vec![Some(1.25), Some(-2.5), Some(3.75)]),
                Some(vec![Some(4.25), Some(5.5), Some(6.75)]),
            ],
            3,
        ),
    ) as ArrayRef;
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
        ("f_vector_float", values),
    ])
    .unwrap();
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    let mut messages = writer.prepare_commit().await.unwrap();
    let file = messages
        .iter_mut()
        .flat_map(|m| &mut m.new_files)
        .find(|file| file.file_name.contains(".vector."))
        .unwrap();
    let java = include_bytes!("../testdata/vector_compaction/java-vectors.parquet");
    io.new_output(&format!("{}/bucket-0/{}", table.location(), file.file_name))
        .unwrap()
        .write(bytes::Bytes::from_static(java))
        .await
        .unwrap();
    file.file_size = java.len() as i64;
    let mut second = file.clone();
    builder.new_commit().commit(messages).await.unwrap();
    second.file_name = "second.vector.parquet".into();
    second.first_row_id = Some(0);
    io.new_output(&format!(
        "{}/bucket-0/{}",
        table.location(),
        second.file_name
    ))
    .unwrap()
    .write(bytes::Bytes::from_static(java))
    .await
    .unwrap();
    builder
        .new_commit()
        .commit(vec![CommitMessage::new(
            paimon::spec::BinaryRow::new(0).to_serialized_bytes(),
            0,
            vec![second],
        )])
        .await
        .unwrap();
    let before = full_rows(&table).await;
    let results = compact(&table).await;
    assert_eq!(results[0].files().len(), 1);
    assert_eq!(full_rows(&table).await, before);
    let output = &results[0].files()[0];
    let path = format!("{}/bucket-0/{}", table.location(), output.file_name);
    let bytes = io.new_input(&path).unwrap().read().await.unwrap();
    let batches = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes)
        .unwrap()
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let expected = batch
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .unwrap();
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .unwrap();
    assert_eq!(array.len(), 2);
    for row in 0..2 {
        assert_eq!(array.value(row).to_data(), expected.value(row).to_data());
    }
    let patch = RecordBatch::try_from_iter([
        ("_ROW_ID", Arc::new(Int64Array::from(vec![0])) as ArrayRef),
        (
            "f_vector_float",
            Arc::new(
                FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    [Some(vec![Some(9.0), Some(8.0), Some(7.0)])],
                    3,
                ),
            ) as ArrayRef,
        ),
    ])
    .unwrap();
    let messages = builder
        .new_update()
        .unwrap()
        .update_by_arrow_with_row_id(vec![patch])
        .await
        .unwrap();
    builder.new_commit().commit(messages).await.unwrap();
    assert_eq!(
        rows(&table, &["f_vector_float"]).await,
        vec![(0, vec![Some(9.0)]), (1, vec![Some(4.25)])]
    );
}

#[tokio::test]
async fn global_vector_search_results_survive_storage_compaction() {
    let table = table(&[
        ("global-index.enabled", "true"),
        ("global-index.row-count-per-shard", "1000"),
        ("global-index.column-update-action", "ignore"),
        ("vector-index.search-mode", "fast"),
        ("ivf-flat.dimension", "2"),
        ("ivf-flat.nlist", "1"),
        ("ivf-flat.distance.metric", "l2"),
    ])
    .await;
    append(&table, 0, 128).await;
    update(&table, "a", 120, Some(140.0)).await;
    table
        .new_global_index_build_builder()
        .with_index_column("a")
        .with_index_type("ivf-flat")
        .execute()
        .await
        .unwrap();
    async fn search(table: &Table) -> (Vec<u64>, Vec<f32>) {
        let mut builder = table.new_vector_search_builder();
        builder
            .with_vector_column("a")
            .with_query_vector(vec![3.0, -3.0])
            .with_limit(4)
            .with_options([("ivf-flat.nprobe".into(), "1".into())].into());
        let result = builder.execute().await.unwrap();
        let ids = result.row_ids().unwrap();
        (ids.row_ids.clone(), ids.scores.clone())
    }
    update(&table, "b", 5, Some(50.0)).await;
    let before = search(&table).await;
    assert_eq!(before.0.len(), 4);
    assert_eq!(before.1.len(), 4);
    assert_eq!(before.0[0], 3);
    assert_eq!(before.1[0], 1.0);
    let before_indexes = indexes(&table).await;
    assert!(!before_indexes.is_empty());
    let results = compact(&table).await;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].metrics().files_removed, 3);
    assert_eq!(results[0].metrics().files_added, 1);
    assert_eq!(search(&table).await, before);
    assert_eq!(indexes(&table).await, before_indexes);
}

#[cfg(feature = "vortex")]
#[tokio::test]
async fn mixed_vector_formats_preserve_values_and_use_configured_output() {
    for (input_format, output_format) in [("parquet", "vortex"), ("vortex", "parquet")] {
        let table = table(&[("vector.file.format", input_format)]).await;
        append(&table, 0, 8).await;
        let table = evolve(
            &table,
            vec![SchemaChange::SetOption {
                key: "vector.file.format".into(),
                value: output_format.into(),
            }],
        )
        .await;
        update(&table, "a", 1, None).await;
        update(&table, "b", 4, Some(40.0)).await;
        let before = full_rows(&table).await;
        let results = compact(&table).await;
        let suffix = format!(".vector.{output_format}");
        assert!(results[0]
            .files()
            .iter()
            .all(|file| file.file_name.ends_with(&suffix)));
        assert_eq!(full_rows(&table).await, before);
        assert!(plan(&table).await.tasks().is_empty());
    }
}
