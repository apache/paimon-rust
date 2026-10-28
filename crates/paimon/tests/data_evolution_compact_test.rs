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

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, RecordBatch};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs, write_batch};
use futures::TryStreamExt;
use paimon::spec::{CommitKind, DataType, IntType, Schema, TableSchema};
use paimon::table::{CompactPlan, CompactResult, DataEvolutionDeleteWriter, Table};

async fn table(options: &[(&str, &str)]) -> Table {
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .option("compaction.min.file-num", "2");
    for (key, value) in options {
        schema = schema.option(*key, *value);
    }
    create_table(schema.build().unwrap()).await
}

async fn create_table(schema: Schema) -> Table {
    let location = format!("memory:/de_compact/{}", uuid::Uuid::new_v4());
    let (io, table) = memory_table(&location, TableSchema::new(0, &schema));
    setup_dirs(&io, &location).await;
    persist_table_schema(&io, &location, table.schema()).await;
    table
}

async fn append(table: &Table, start: i32) {
    write_batch(
        table,
        &RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int32Array::from(vec![start, start + 1])) as ArrayRef,
            ),
            (
                "value",
                Arc::new(Int32Array::from(vec![Some(10), None])) as ArrayRef,
            ),
        ])
        .unwrap(),
    )
    .await;
}

async fn update(table: &Table, row_id: i64, value: Option<i32>) {
    let builder = table.new_write_builder();
    let mut writer = builder
        .new_data_evolution_writer(vec!["value".into()])
        .unwrap();
    writer
        .add_matched_batch(
            RecordBatch::try_from_iter([
                (
                    "_ROW_ID",
                    Arc::new(Int64Array::from(vec![row_id])) as ArrayRef,
                ),
                ("value", Arc::new(Int32Array::from(vec![value])) as ArrayRef),
            ])
            .unwrap(),
        )
        .unwrap();
    builder
        .new_commit()
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();
}

async fn delete(table: &Table, ids: &[i64]) {
    let mut writer = DataEvolutionDeleteWriter::new(table).unwrap();
    writer.add_row_ids(ids.iter().copied()).unwrap();
    table
        .new_write_builder()
        .new_commit()
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();
}

async fn rows(table: &Table) -> Vec<(i64, i32, Option<i32>)> {
    let mut builder = table.new_read_builder();
    builder
        .with_projection(&["_ROW_ID", "id", "value"])
        .unwrap();
    let plan = builder.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut result = vec![];
    for batch in batches {
        let row_ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        result.extend((0..batch.num_rows()).map(|i| {
            (
                row_ids.value(i),
                ids.value(i),
                (!values.is_null(i)).then(|| values.value(i)),
            )
        }));
    }
    result.sort_unstable();
    result
}

async fn execute(table: &Table) -> (CompactPlan, Vec<CompactResult>) {
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let plan: CompactPlan = serde_json::from_slice(&serde_json::to_vec(&plan).unwrap()).unwrap();
    let executor = builder.new_executor().unwrap();
    let mut results = vec![];
    for task in plan.tasks() {
        results.push(executor.execute(task).await.unwrap());
    }
    (
        plan,
        serde_json::from_slice(&serde_json::to_vec(&results).unwrap()).unwrap(),
    )
}

#[tokio::test]
async fn compact_merges_contiguous_ranges_and_latest_columns_without_reassigning_rows() {
    for format in ["parquet", "orc", "avro", "row"] {
        let table = table(&[
            ("file.format", format),
            ("file.compression", "none"),
            ("target-file-row-num", "2"),
        ])
        .await;
        append(&table, 0).await;
        append(&table, 2).await;
        update(&table, 0, Some(99)).await;
        let expected = rows(&table).await;
        let before = table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        let (plan, results) = execute(&table).await;
        assert_eq!(plan.tasks().len(), 1, "{format}");
        assert_eq!(plan.tasks()[0].files().len(), 3);
        assert_eq!(
            results[0].files().len(),
            1,
            "write-time row limit must not split a compact range"
        );
        let output = &results[0].files()[0];
        assert_eq!(output.first_row_id, Some(0));
        assert_eq!(output.row_count, 4);
        assert_eq!(
            (output.min_sequence_number, output.max_sequence_number),
            (1, 3)
        );
        let committer = table.new_compact_builder().new_commit().unwrap();
        let metrics = committer.commit_compact(results.clone()).await.unwrap();
        assert_eq!(metrics.rows_written, 4);
        assert_eq!(metrics.files_removed, 3);
        assert_eq!(rows(&table).await, expected, "{format}");
        assert_eq!(
            rows(&table.copy_with_pinned_snapshot(Some(&before))).await,
            expected
        );
        let after = table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.next_row_id(), before.next_row_id());
        assert_eq!(after.commit_kind(), &CommitKind::COMPACT);
        assert_eq!(committer.commit_compact(results).await.unwrap(), metrics);
        assert!(execute(&table).await.0.tasks().is_empty());
        update(&table, 3, Some(123)).await;
        append(&table, 4).await;
        assert_eq!(
            rows(&table).await,
            vec![
                (0, 0, Some(99)),
                (1, 1, None),
                (2, 2, Some(10)),
                (3, 3, Some(123)),
                (4, 4, Some(10)),
                (5, 5, None)
            ]
        );
    }
}

#[tokio::test]
async fn compact_rejects_concurrent_column_updates_but_accepts_disjoint_appends() {
    let table = table(&[]).await;
    append(&table, 0).await;
    update(&table, 0, Some(99)).await;
    let (_, stale) = execute(&table).await;
    update(&table, 1, Some(55)).await;
    let committer = table.new_compact_builder().new_commit().unwrap();
    assert!(committer.commit_compact(stale).await.is_err());
    assert_eq!(rows(&table).await, vec![(0, 0, Some(99)), (1, 1, Some(55))]);
    let (_, fresh) = execute(&table).await;
    append(&table, 2).await;
    committer.commit_compact(fresh).await.unwrap();
    assert_eq!(
        rows(&table).await,
        vec![
            (0, 0, Some(99)),
            (1, 1, Some(55)),
            (2, 2, Some(10)),
            (3, 3, None)
        ]
    );
}

#[tokio::test]
async fn compact_moves_latest_deletion_vectors_and_keeps_physical_slots() {
    for in_data_dir in ["true", "false"] {
        let table = table(&[
            ("deletion-vectors.enabled", "true"),
            ("index-file-in-data-file-dir", in_data_dir),
        ])
        .await;
        append(&table, 0).await;
        append(&table, 2).await;
        update(&table, 0, Some(99)).await;
        delete(&table, &[1]).await;
        let before = table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        let (_, results) = execute(&table).await;
        assert_eq!(
            results[0].files()[0].row_count,
            4,
            "deleted positions must be written too"
        );
        delete(&table, &[2]).await;
        table
            .new_compact_builder()
            .new_commit()
            .unwrap()
            .commit_compact(results)
            .await
            .unwrap();
        let snapshot = table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        let indexes = paimon::spec::IndexManifest::read(
            table.file_io(),
            &table
                .snapshot_manager()
                .manifest_path(snapshot.index_manifest().unwrap()),
        )
        .await
        .unwrap();
        assert_eq!(indexes.len(), 1);
        let name = &indexes[0].index_file.file_name;
        let bucket_path = format!("{}/bucket-0/{name}", table.location());
        let index_path = format!("{}/index/{name}", table.location());
        assert_eq!(
            table.file_io().exists(&bucket_path).await.unwrap(),
            in_data_dir == "true"
        );
        assert_eq!(
            table.file_io().exists(&index_path).await.unwrap(),
            in_data_dir == "false"
        );
        assert_eq!(rows(&table).await, vec![(0, 0, Some(99)), (3, 3, None)]);
        assert_eq!(
            rows(&table.copy_with_pinned_snapshot(Some(&before))).await,
            vec![(0, 0, Some(99)), (2, 2, Some(10)), (3, 3, None)]
        );
        update(&table, 3, Some(77)).await;
        delete(&table, &[0]).await;
        assert_eq!(rows(&table).await, vec![(3, 3, Some(77))]);
    }
}

#[tokio::test]
async fn compact_retains_dedicated_blob_and_vector_files() {
    use arrow_array::{types::Float32Type, FixedSizeListArray, LargeBinaryArray};
    use paimon::spec::{BlobType, FloatType, VectorType};

    let table = create_table(
        Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .column("payload", DataType::Blob(BlobType::new()))
            .column(
                "embedding",
                DataType::Vector(
                    VectorType::try_new(true, 2, DataType::Float(FloatType::new())).unwrap(),
                ),
            )
            .option("row-tracking.enabled", "true")
            .option("data-evolution.enabled", "true")
            .option("compaction.min.file-num", "2")
            .option("vector.file.format", "parquet")
            .build()
            .unwrap(),
    )
    .await;
    for id in [0, 1] {
        write_batch(
            &table,
            &RecordBatch::try_from_iter([
                ("id", Arc::new(Int32Array::from(vec![id])) as ArrayRef),
                (
                    "value",
                    Arc::new(Int32Array::from(vec![id * 10])) as ArrayRef,
                ),
                (
                    "payload",
                    Arc::new(LargeBinaryArray::from(vec![Some(b"retained".as_slice())]))
                        as ArrayRef,
                ),
                (
                    "embedding",
                    Arc::new(
                        FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                            vec![Some(vec![Some(id as f32), Some(1.0)])],
                            2,
                        ),
                    ) as ArrayRef,
                ),
            ])
            .unwrap(),
        )
        .await;
    }
    let mut read = table.new_read_builder();
    read.with_projection(&["id", "payload", "embedding"])
        .unwrap();
    let original = read.new_scan().plan().await.unwrap();
    let blobs = original
        .splits()
        .iter()
        .flat_map(|split| split.data_files())
        .filter(|file| file.file_name.ends_with(".blob") || file.file_name.contains(".vector."))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(blobs.len(), 4);
    let (_, results) = execute(&table).await;
    assert_eq!(
        results[0].files()[0].write_cols,
        Some(vec!["id".into(), "value".into()])
    );
    table
        .new_compact_builder()
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap();
    let latest = read.new_scan().plan().await.unwrap();
    for blob in blobs {
        assert!(latest
            .splits()
            .iter()
            .flat_map(|split| split.data_files())
            .any(|file| file == &blob));
    }
    let batches: Vec<RecordBatch> = read
        .new_read()
        .unwrap()
        .to_arrow(latest.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        2
    );
    for batch in batches {
        let payload = batch
            .column(1)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert!(payload
            .iter()
            .all(|value| value == Some(b"retained".as_slice())));
        let vectors = batch
            .column(2)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let vector = vectors.value(row);
            let values = vector
                .as_any()
                .downcast_ref::<arrow_array::Float32Array>()
                .unwrap();
            assert_eq!(values.values().as_ref(), &[ids.value(row) as f32, 1.0]);
        }
    }
}

#[tokio::test]
async fn compact_keeps_global_indexes_and_column_freshness() {
    use paimon::spec::{Datum, IndexManifest, PredicateBuilder};

    let table = table(&[("global-index.column-update-action", "ignore")]).await;
    append(&table, 0).await;
    append(&table, 2).await;
    table
        .new_global_index_build_builder()
        .with_index_type("bitmap")
        .with_index_column("id")
        .execute()
        .await
        .unwrap();
    update(&table, 0, Some(99)).await;
    let snapshot = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    let indexes = IndexManifest::read(
        table.file_io(),
        &table
            .snapshot_manager()
            .manifest_path(snapshot.index_manifest().unwrap()),
    )
    .await
    .unwrap();
    let (_, results) = execute(&table).await;
    assert_eq!(
        results[0].files()[0].column_max_sequence_numbers,
        Some(vec![2, 4])
    );
    table
        .new_compact_builder()
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap();
    let latest = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        IndexManifest::read(
            table.file_io(),
            &table
                .snapshot_manager()
                .manifest_path(latest.index_manifest().unwrap())
        )
        .await
        .unwrap(),
        indexes
    );
    let mut builder = table.new_read_builder();
    builder.with_filter(
        PredicateBuilder::new(table.schema().fields())
            .equal("id", Datum::Int(0))
            .unwrap(),
    );
    let plan = builder.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
    assert_eq!(
        batches[0]
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        99
    );
}

#[tokio::test]
async fn compact_preserves_inline_blob_descriptors_without_fetching_payloads() {
    use arrow_array::LargeBinaryArray;
    use paimon::spec::{BlobDescriptor, BlobType};
    use std::collections::HashMap;

    let table = create_table(
        Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("payload", DataType::Blob(BlobType::new()))
            .option("row-tracking.enabled", "true")
            .option("data-evolution.enabled", "true")
            .option("blob-descriptor-field", "payload")
            .option("compaction.min.file-num", "2")
            .build()
            .unwrap(),
    )
    .await;
    let descriptor = BlobDescriptor::new("memory:/absent/upstream".into(), 3, 20).serialize();
    for id in [0, 1] {
        write_batch(
            &table,
            &RecordBatch::try_from_iter([
                ("id", Arc::new(Int32Array::from(vec![id])) as ArrayRef),
                (
                    "payload",
                    Arc::new(LargeBinaryArray::from(vec![Some(descriptor.as_slice())])) as ArrayRef,
                ),
            ])
            .unwrap(),
        )
        .await;
    }
    let (_, results) = execute(&table).await;
    table
        .new_compact_builder()
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap();
    let descriptors = table.copy_with_options(HashMap::from([(
        "blob-as-descriptor".into(),
        "true".into(),
    )]));
    let read = descriptors.new_read_builder();
    let plan = read.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = read
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        2
    );
    for batch in batches {
        let payload = batch
            .column_by_name("payload")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert!(payload
            .iter()
            .all(|value| value == Some(descriptor.as_slice())));
    }
}

#[tokio::test]
async fn compact_rejects_incomplete_serialized_groups_and_changed_output_ranges() {
    let table = table(&[]).await;
    append(&table, 0).await;
    update(&table, 0, Some(99)).await;
    update(&table, 1, Some(88)).await;
    let (plan, results) = execute(&table).await;
    assert_eq!(
        plan.tasks()[0].files().len(),
        3,
        "shadowed versions must still be removed"
    );
    let mut modified = serde_json::to_value(&plan).unwrap();
    modified["tasks"][0]["files"]
        .as_array_mut()
        .unwrap()
        .remove(0);
    let modified: CompactPlan = serde_json::from_value(modified).unwrap();
    assert!(table
        .new_compact_builder()
        .new_executor()
        .unwrap()
        .execute(&modified.tasks()[0])
        .await
        .is_err());
    let mut modified = serde_json::to_value(&results).unwrap();
    modified[0]["files"][0]["_FIRST_ROW_ID"] = 1.into();
    let modified: Vec<CompactResult> = serde_json::from_value(modified).unwrap();
    assert!(table
        .new_compact_builder()
        .new_commit()
        .unwrap()
        .commit_compact(modified)
        .await
        .is_err());
    assert_eq!(rows(&table).await, vec![(0, 0, Some(99)), (1, 1, Some(88))]);
}

#[tokio::test]
async fn compact_partition_filter_and_subsets_keep_unselected_files_and_deletions() {
    use paimon::spec::{Datum, PredicateBuilder};

    let table = create_table(
        Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .partition_keys(["id"])
            .option("row-tracking.enabled", "true")
            .option("data-evolution.enabled", "true")
            .option("deletion-vectors.enabled", "true")
            .option("compaction.min.file-num", "2")
            .build()
            .unwrap(),
    )
    .await;
    for id in [10, 20] {
        for value in [1, 2] {
            write_batch(
                &table,
                &RecordBatch::try_from_iter([
                    ("id", Arc::new(Int32Array::from(vec![id])) as ArrayRef),
                    ("value", Arc::new(Int32Array::from(vec![value])) as ArrayRef),
                ])
                .unwrap(),
            )
            .await;
        }
    }
    delete(&table, &[1, 3]).await;
    let expected = rows(&table).await;
    let builder = table
        .new_compact_builder()
        .with_partition_filter(
            PredicateBuilder::new(table.schema().fields())
                .equal("id", Datum::Int(10))
                .unwrap(),
        )
        .unwrap();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    assert_eq!(plan.tasks().len(), 1);
    let result = builder
        .new_executor()
        .unwrap()
        .execute(&plan.tasks()[0])
        .await
        .unwrap();
    builder
        .new_commit()
        .unwrap()
        .commit_compact(vec![result])
        .await
        .unwrap();
    assert_eq!(rows(&table).await, expected);
    let (remaining, results) = execute(&table).await;
    assert_eq!(remaining.tasks().len(), 1);
    assert_eq!(remaining.tasks()[0].files()[0].first_row_id, Some(2));
    table
        .new_compact_builder()
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap();
    assert_eq!(rows(&table).await, expected);

    // Multiple tasks sharing one DV index must also be independently committable.
    let table = self::table(&[
        ("target-file-size", "1b"),
        ("deletion-vectors.enabled", "true"),
    ])
    .await;
    append(&table, 0).await;
    append(&table, 2).await;
    update(&table, 0, Some(7)).await;
    update(&table, 2, Some(8)).await;
    delete(&table, &[1, 3]).await;
    let (plan, mut results) = execute(&table).await;
    assert_eq!(plan.tasks().len(), 2);
    let expected = rows(&table).await;
    let commit = table.new_compact_builder().new_commit().unwrap();
    let first = results.remove(0);
    commit.commit_compact(vec![first.clone()]).await.unwrap();
    assert_eq!(rows(&table).await, expected);
    commit.commit_compact(results).await.unwrap();
    commit.commit_compact(vec![first]).await.unwrap();
    assert_eq!(rows(&table).await, expected);
}

#[tokio::test]
async fn compact_preserves_field_ids_through_rename_drop_readd_and_type_promotion() {
    use paimon::spec::{BigIntType, SchemaChange};

    let original = table(&[]).await;
    append(&original, 0).await;
    update(&original, 0, Some(99)).await;
    let schema = original
        .schema()
        .apply_changes(vec![
            SchemaChange::rename_column("value".into(), "renamed".into()),
            SchemaChange::update_column_type("renamed".into(), DataType::BigInt(BigIntType::new())),
            SchemaChange::add_column("value".into(), DataType::Int(IntType::new())),
        ])
        .unwrap();
    persist_table_schema(original.file_io(), original.location(), &schema).await;
    let table = original.copy_with_resolved_schema(schema, "main").unwrap();
    write_batch(
        &table,
        &RecordBatch::try_from_iter([
            ("id", Arc::new(Int32Array::from(vec![2])) as ArrayRef),
            ("renamed", Arc::new(Int64Array::from(vec![123])) as ArrayRef),
            ("value", Arc::new(Int32Array::from(vec![77])) as ArrayRef),
        ])
        .unwrap(),
    )
    .await;
    let (_, results) = execute(&table).await;
    assert_eq!(results.len(), 1);
    table
        .new_compact_builder()
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap();
    let read = table.new_read_builder();
    let plan = read.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = read
        .new_read()
        .unwrap()
        .with_ordered(true)
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let merged = arrow_select::concat::concat_batches(&batches[0].schema(), &batches).unwrap();
    assert_eq!(
        merged
            .column_by_name("renamed")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap(),
        &Int64Array::from(vec![Some(99), None, Some(123)])
    );
    assert_eq!(
        merged
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap(),
        &Int32Array::from(vec![None, None, Some(77)])
    );
    assert_eq!(
        rows(&original.copy_with_pinned_snapshot(Some(
            &original.snapshot_manager().get_snapshot(2).await.unwrap()
        )))
        .await,
        vec![(0, 0, Some(99)), (1, 1, None)]
    );
}

#[tokio::test]
async fn compact_merges_nested_updates_without_replacing_unwritten_children() {
    use arrow_array::{StringArray, StructArray};
    use paimon::spec::{DataField, RowType, VarCharType};

    let table = create_table(
        Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column(
                "profile",
                DataType::Row(RowType::new(vec![
                    DataField::new(0, "age".into(), DataType::Int(IntType::new())),
                    DataField::new(
                        1,
                        "name".into(),
                        DataType::VarChar(VarCharType::string_type()),
                    ),
                ])),
            )
            .option("row-tracking.enabled", "true")
            .option("data-evolution.enabled", "true")
            .option("data-evolution.nested-field.enabled", "true")
            .option("compaction.min.file-num", "2")
            .build()
            .unwrap(),
    )
    .await;
    let profile = StructArray::from(vec![
        (
            Arc::new(arrow_schema::Field::new(
                "age",
                arrow_schema::DataType::Int32,
                true,
            )),
            Arc::new(Int32Array::from(vec![Some(10), None])) as ArrayRef,
        ),
        (
            Arc::new(arrow_schema::Field::new(
                "name",
                arrow_schema::DataType::Utf8,
                true,
            )),
            Arc::new(StringArray::from(vec![Some("alice"), Some("bob")])) as ArrayRef,
        ),
    ]);
    write_batch(
        &table,
        &RecordBatch::try_from_iter([
            ("id", Arc::new(Int32Array::from(vec![0, 1])) as ArrayRef),
            ("profile", Arc::new(profile) as ArrayRef),
        ])
        .unwrap(),
    )
    .await;
    let builder = table.new_write_builder();
    let mut writer = builder
        .new_data_evolution_writer(vec!["profile.age".into()])
        .unwrap();
    writer
        .add_matched_batch(
            RecordBatch::try_from_iter([
                ("_ROW_ID", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
                (
                    "profile.age",
                    Arc::new(Int32Array::from(vec![Some(42)])) as ArrayRef,
                ),
            ])
            .unwrap(),
        )
        .unwrap();
    builder
        .new_commit()
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();
    let (_, results) = execute(&table).await;
    builder.new_commit().commit_compact(results).await.unwrap();
    let read = table.new_read_builder();
    let plan = read.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = read
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let merged = arrow_select::concat::concat_batches(&batches[0].schema(), &batches).unwrap();
    let profile = merged
        .column_by_name("profile")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(
        profile
            .column_by_name("age")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap(),
        &Int32Array::from(vec![10, 42])
    );
    assert_eq!(
        profile
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap(),
        &StringArray::from(vec!["alice", "bob"])
    );
}

#[tokio::test]
async fn java_minimum_one_allows_single_file_schema_materialization() {
    use paimon::spec::SchemaChange;

    let original = table(&[("compaction.min.file-num", "1")]).await;
    append(&original, 0).await;
    let schema = original
        .schema()
        .apply_changes(vec![SchemaChange::add_column(
            "added".into(),
            DataType::Int(IntType::new()),
        )])
        .unwrap();
    persist_table_schema(original.file_io(), original.location(), &schema).await;
    let table = original.copy_with_resolved_schema(schema, "main").unwrap();
    let (plan, results) = execute(&table).await;
    assert_eq!(plan.tasks().len(), 1);
    assert_eq!(plan.tasks()[0].files().len(), 1);
    assert_eq!(results[0].files()[0].schema_id, table.schema().id());
    assert_eq!(results[0].files()[0].first_row_id, Some(0));
    table
        .new_compact_builder()
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap();
    assert_eq!(rows(&table).await, vec![(0, 0, Some(10)), (1, 1, None)]);
    let read = table.new_read_builder();
    let plan = read.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = read
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(batches
        .iter()
        .all(|batch| batch.column_by_name("added").unwrap().null_count() == batch.num_rows()));
}

#[tokio::test]
async fn compact_subset_preserves_unrelated_deletion_vector_indexes_without_opening_them() {
    use paimon::spec::IndexManifest;
    use paimon::table::CommitMessage;

    let table = table(&[
        ("target-file-size", "1b"),
        ("deletion-vectors.enabled", "true"),
    ])
    .await;
    append(&table, 0).await;
    append(&table, 2).await;
    update(&table, 0, Some(99)).await;
    delete(&table, &[1]).await;
    let scan = table
        .new_read_builder()
        .new_scan()
        .plan_all()
        .await
        .unwrap();
    let other = scan
        .splits()
        .iter()
        .flat_map(|split| split.data_files())
        .find(|file| file.first_row_id == Some(2))
        .unwrap();
    let snapshot = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    let indexes = IndexManifest::read(
        table.file_io(),
        &table
            .snapshot_manager()
            .manifest_path(snapshot.index_manifest().unwrap()),
    )
    .await
    .unwrap();
    assert_eq!(indexes.len(), 1);
    let original = &indexes[0];
    let original_bytes = table
        .file_io()
        .new_input(&format!(
            "{}/index/{}",
            table.location(),
            original.index_file.file_name
        ))
        .unwrap()
        .read()
        .await
        .unwrap();
    let mut unrelated = original.index_file.clone();
    unrelated.file_name = format!("independent-dv-{}", uuid::Uuid::new_v4());
    let ranges = unrelated.deletion_vectors_ranges.as_mut().unwrap();
    let meta = ranges.values().next().unwrap().clone();
    ranges.clear();
    ranges.insert(other.file_name.clone(), meta);
    let unrelated_path = format!("{}/index/{}", table.location(), unrelated.file_name);
    table
        .file_io()
        .new_output(&unrelated_path)
        .unwrap()
        .write(original_bytes.clone())
        .await
        .unwrap();
    let mut message = CommitMessage::new(original.partition.clone(), original.bucket, vec![]);
    message.new_index_files.push(unrelated.clone());
    table
        .new_write_builder()
        .new_commit()
        .commit(vec![message])
        .await
        .unwrap();

    let (plan, results) = execute(&table).await;
    assert_eq!(
        plan.tasks().len(),
        1,
        "only the updated A group is selected"
    );
    table.file_io().delete_file(&unrelated_path).await.unwrap();
    table
        .new_compact_builder()
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap();
    let latest = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    let indexes = IndexManifest::read(
        table.file_io(),
        &table
            .snapshot_manager()
            .manifest_path(latest.index_manifest().unwrap()),
    )
    .await
    .unwrap();
    assert!(
        indexes.iter().any(|entry| entry.index_file == unrelated),
        "Java leaves wholly untouched DV index entries verbatim"
    );
    table
        .file_io()
        .new_output(&unrelated_path)
        .unwrap()
        .write(original_bytes)
        .await
        .unwrap();
    assert_eq!(rows(&table).await, vec![(0, 0, Some(99)), (2, 2, Some(10))]);
}
