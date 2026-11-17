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
use paimon::spec::{IndexManifest, IndexManifestEntry, SchemaChange};
use paimon::table::{
    CommitCallback, CommitCallbackContext, CompactPlan, CompactResult, DataEvolutionDeleteWriter,
};

async fn compact_plan(table: &Table) -> CompactPlan {
    let plan = table
        .new_compact_builder()
        .with_blob_compaction(true)
        .new_planner()
        .unwrap()
        .plan()
        .await
        .unwrap();
    serde_json::from_slice(&serde_json::to_vec(&plan).unwrap()).unwrap()
}

async fn execute(table: &Table, plan: &CompactPlan) -> Vec<CompactResult> {
    let executor = table.new_compact_builder().new_executor().unwrap();
    let mut results = Vec::new();
    for task in plan.tasks() {
        results.push(executor.execute(task).await.unwrap());
    }
    serde_json::from_slice(&serde_json::to_vec(&results).unwrap()).unwrap()
}

async fn update(table: &Table, id: i64, value: Option<&[u8]>) {
    let messages = apply(table, matched(vec![id], vec![value])).await;
    table
        .new_write_builder()
        .new_commit()
        .commit(messages)
        .await
        .unwrap();
}

async fn delete(table: &Table, id: i64) {
    let mut writer = DataEvolutionDeleteWriter::new(table).unwrap();
    writer.add_row_ids([id]).unwrap();
    table
        .new_write_builder()
        .new_commit()
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();
}

async fn indexes(table: &Table) -> Vec<IndexManifestEntry> {
    let manager = table.snapshot_manager();
    let snapshot = manager.get_latest_snapshot().await.unwrap().unwrap();
    IndexManifest::read(
        table.file_io(),
        &manager.manifest_path(snapshot.index_manifest().unwrap()),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn blob_compaction_rejects_unsupported_tables_and_layouts() {
    let (_, ordinary) = memory_table(
        "memory:/ordinary",
        TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .build()
                .unwrap(),
        ),
    );
    let error = ordinary
        .new_compact_builder()
        .with_blob_compaction(true)
        .new_planner()
        .err()
        .unwrap();
    assert!(error
        .to_string()
        .contains("requires a Data Evolution table"));
    let original = table(&[("compaction.min.file-num", "2")]).await;
    seed(&original).await;
    update(&original, 1, Some(b"updated")).await;
    let plan = compact_plan(&original).await;
    let results = execute(&original, &plan).await;
    let before = physical_files(&original).await;
    for (key, value, expected) in [
        (
            "blob.target-file-size",
            "0 b",
            "positive blob.target-file-size",
        ),
        ("video-frame-field", "payload", "video-file compaction"),
    ] {
        let table = original.copy_with_options([(key.into(), value.into())].into());
        let builder = table.new_compact_builder().with_blob_compaction(true);
        let error = match builder.new_planner() {
            Ok(planner) => planner.plan().await.unwrap_err(),
            Err(error) => error,
        };
        assert!(error.to_string().contains(expected), "{key}: {error}");
        let mut task = serde_json::to_value(&plan.tasks()[0]).unwrap();
        task["context"]["schema"] = serde_json::to_value(table.schema()).unwrap();
        let error = builder
            .new_executor()
            .unwrap()
            .execute(&serde_json::from_value(task.clone()).unwrap())
            .await
            .unwrap_err();
        assert!(error.to_string().contains(expected), "{key}: {error}");
        let mut forged = serde_json::to_value(&results).unwrap();
        forged[0]["task"] = task;
        let error = builder
            .new_commit()
            .unwrap()
            .commit_compact(serde_json::from_value(forged).unwrap())
            .await
            .unwrap_err();
        assert!(error.to_string().contains(expected), "{key}: {error}");
        assert_eq!(physical_files(&table).await, before);
        assert_eq!(
            table
                .snapshot_manager()
                .get_latest_snapshot_id()
                .await
                .unwrap(),
            Some(2)
        );
    }
    let inline =
        original.copy_with_options([("blob-descriptor-field".into(), "payload".into())].into());
    assert!(compact_plan(&inline).await.tasks().is_empty());
    assert_eq!(physical_files(&inline).await, before);

    let mut schema = serde_json::to_value(original.schema()).unwrap();
    schema["id"] = (original.schema().id() + 1).into();
    schema["fields"][1]["type"] = serde_json::to_value(DataType::Array(ArrayType::new(
        DataType::Blob(BlobType::new()),
    )))
    .unwrap();
    let schema: TableSchema = serde_json::from_value(schema).unwrap();
    persist_table_schema(original.file_io(), original.location(), &schema).await;
    let evolved = original.copy_with_resolved_schema(schema, "main").unwrap();
    let plan = compact_plan(&evolved).await;
    assert_eq!(plan.tasks().len(), 1);
    let error = evolved
        .new_compact_builder()
        .new_executor()
        .unwrap()
        .execute(&plan.tasks()[0])
        .await
        .unwrap_err();
    assert!(
        matches!(error, paimon::Error::Unsupported { .. }),
        "{error}"
    );
    assert!(
        error.to_string().contains("cannot migrate field types"),
        "{error}"
    );
    assert_eq!(physical_files(&evolved).await, before);
    assert_eq!(
        evolved
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        Some(2)
    );
    let mut forged = serde_json::to_value(results).unwrap();
    forged[0]["task"]["context"]["schema"] = serde_json::to_value(evolved.schema()).unwrap();
    forged[0]["files"][0]["_SCHEMA_ID"] = evolved.schema().id().into();
    let error = evolved
        .new_compact_builder()
        .new_commit()
        .unwrap()
        .commit_compact(serde_json::from_value(forged).unwrap())
        .await
        .unwrap_err();
    assert!(
        matches!(error, paimon::Error::Unsupported { .. }),
        "{error}"
    );
    assert!(
        error.to_string().contains("cannot migrate field types"),
        "{error}"
    );
    assert_eq!(physical_files(&evolved).await, before);
    assert_eq!(
        evolved
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        Some(2)
    );
}

#[tokio::test]
async fn rolled_sparse_updates_compact_without_losing_fallbacks() {
    let table = table(&[("read.batch-size", "1")]).await;
    seed(&table).await;
    let table = table.copy_with_options([("blob.target-file-size".into(), "20 B".into())].into());
    let messages = apply(
        &table,
        matched(vec![0, 4], vec![Some(b"first"), Some(b"last")]),
    )
    .await;
    let mut ranges = messages
        .iter()
        .flat_map(|message| &message.new_files)
        .map(|file| file.row_id_range().unwrap())
        .collect::<Vec<_>>();
    ranges.sort_unstable();
    assert_eq!(ranges, vec![(0, 0), (1, 4)]);
    table
        .new_write_builder()
        .new_commit()
        .commit(messages)
        .await
        .unwrap();
    let expected = rows(&table).await;
    assert_eq!(
        expected
            .iter()
            .map(|row| row.1.as_deref())
            .collect::<Vec<_>>(),
        vec![
            Some(b"first".as_slice()),
            None,
            Some(b"".as_slice()),
            Some(b"old-3".as_slice()),
            Some(b"last".as_slice()),
        ]
    );
    let plan = compact_plan(&table).await;
    assert_eq!(plan.tasks().len(), 1);
    assert_eq!(plan.tasks()[0].files().len(), 3);
    let results = execute(&table, &plan).await;
    assert_eq!(results[0].files().len(), 1);
    assert_eq!(results[0].files()[0].row_id_range(), Some((0, 4)));
    assert!(results[0].files()[0].file_size > 20);
    table
        .new_compact_builder()
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap();
    assert_eq!(rows(&table).await, expected);
    assert!(compact_plan(&table).await.tasks().is_empty());
}

#[tokio::test]
async fn blob_compaction_rejects_historical_video_sources() {
    async fn publish_video(table: &Table) {
        let mut messages = apply(table, matched(vec![6], vec![Some(b"video")])).await;
        let file = &mut messages[0].new_files[0];
        let path = format!("{}/bucket-0/{}", table.location(), file.file_name);
        table.file_io().delete_file(&path).await.unwrap();
        file.file_name = file.file_name.replace(".blob", ".video");
        let fixture = include_str!("../../testdata/video/video-v1.hex");
        let bytes = hex::decode(
            fixture
                .lines()
                .find(|line| !line.is_empty() && !line.starts_with('#'))
                .unwrap(),
        )
        .unwrap();
        file.file_size = bytes.len() as i64;
        assert_eq!(file.row_count, 7);
        table
            .file_io()
            .new_output(&format!("{}/bucket-0/{}", table.location(), file.file_name))
            .unwrap()
            .write(bytes.into())
            .await
            .unwrap();
        table
            .new_write_builder()
            .new_commit()
            .commit(messages)
            .await
            .unwrap();
    }

    for video_before_planning in [true, false] {
        let table = table(&[]).await;
        common::incremental_helpers::write_batch(
            &table,
            &RecordBatch::try_from_iter([
                (
                    "id",
                    Arc::new(Int32Array::from_iter_values(0..7)) as ArrayRef,
                ),
                (
                    "payload",
                    Arc::new(LargeBinaryArray::from(vec![Some(b"base".as_slice()); 7])) as ArrayRef,
                ),
                (
                    "value",
                    Arc::new(Int32Array::from_iter_values(0..7)) as ArrayRef,
                ),
            ])
            .unwrap(),
        )
        .await;
        if video_before_planning {
            publish_video(&table).await;
        }
        update(&table, 1, Some(b"new-1")).await;
        update(&table, 6, Some(b"new-6")).await;
        let builder = table.new_compact_builder().with_blob_compaction(true);
        let before = physical_files(&table).await;
        let error = if video_before_planning {
            builder.new_planner().unwrap().plan().await.unwrap_err()
        } else {
            let plan = compact_plan(&table).await;
            let results = execute(&table, &plan).await;
            publish_video(&table).await;
            let expected = rows(&table).await;
            let error = builder
                .new_commit()
                .unwrap()
                .commit_compact(results)
                .await
                .unwrap_err();
            assert_eq!(rows(&table).await, expected);
            error
        };
        assert!(
            matches!(error, paimon::Error::Unsupported { .. }),
            "{error}"
        );
        assert!(
            error.to_string().contains("video-file compaction"),
            "{error}"
        );
        assert!(before.is_subset(&physical_files(&table).await));
    }
}

#[tokio::test]
async fn sparse_blob_compaction_preserves_payloads_deletions_versions_and_history() {
    struct FailAfterPublication;

    #[async_trait::async_trait]
    impl CommitCallback for FailAfterPublication {
        async fn call(&self, _: &CommitCallbackContext) -> paimon::Result<()> {
            Err(paimon::Error::UnexpectedError {
                message: "injected BLOB compaction callback failure".into(),
                source: None,
            })
        }
    }

    let table = table(&[("compaction.min.file-num", "2"), ("read.batch-size", "2")]).await;
    seed(&table).await;
    update(&table, 1, Some(b"new-1")).await;
    update(&table, 3, None).await;
    table
        .new_global_index_build_builder()
        .with_index_type("bitmap")
        .with_index_column("id")
        .execute()
        .await
        .unwrap();
    delete(&table, 4).await;
    let before = rows(&table).await;
    let snapshot = table
        .snapshot_manager()
        .get_latest_snapshot_id()
        .await
        .unwrap()
        .unwrap();
    let historical =
        table.copy_with_options([("scan.snapshot-id".into(), snapshot.to_string())].into());
    let plan = compact_plan(&table).await;
    assert_eq!(plan.tasks().len(), 1, "the normal anchor needs no rewrite");
    assert_eq!(plan.tasks()[0].files().len(), 3);
    let original_files = physical_files(&table).await;
    let aborted = execute(&table, &plan).await;
    table
        .new_compact_builder()
        .new_commit()
        .unwrap()
        .abort_compact(&aborted)
        .await
        .unwrap();
    assert_eq!(physical_files(&table).await, original_files);
    let results = execute(&table, &plan).await;
    assert_eq!(results[0].metrics().files_removed, 3);
    assert_eq!(results[0].metrics().files_added, 1);
    assert_eq!(results[0].metrics().rows_written, 5);
    let output = &results[0].files()[0];
    assert_eq!(output.row_id_range(), Some((0, 4)));
    assert_eq!(
        (output.min_sequence_number, output.max_sequence_number),
        (1, 3)
    );
    assert_eq!(output.write_cols, Some(vec!["payload".into()]));
    delete(&table, 2).await;
    let expected = rows(&table).await;
    let expected_indexes = indexes(&table).await;
    assert!(expected_indexes
        .iter()
        .any(|entry| entry.index_file.global_index_meta.is_some()));
    let mut commit = table
        .new_compact_builder()
        .with_commit_user("blob-compact")
        .unwrap()
        .new_commit()
        .unwrap();
    commit
        .add_commit_callback(Arc::new(FailAfterPublication))
        .unwrap();
    let error = commit.commit_compact(results.clone()).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("injected BLOB compaction callback failure"),
        "{error}"
    );
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        Some(snapshot + 2)
    );
    let published_files = physical_files(&table).await;
    assert!(original_files.is_subset(&published_files));
    assert_eq!(published_files.len(), original_files.len() + 1);
    assert_eq!(rows(&table).await, expected);
    assert_eq!(indexes(&table).await, expected_indexes);
    assert_eq!(rows(&historical).await, before);
    assert!(compact_plan(&table).await.tasks().is_empty());
    update(&table, 0, Some(b"after-compaction")).await;
    assert_eq!(rows(&table).await[0].1, Some(b"after-compaction".to_vec()));
    let expected = rows(&table).await;
    let published = table
        .snapshot_manager()
        .get_latest_snapshot_id()
        .await
        .unwrap();
    table
        .new_compact_builder()
        .with_commit_user("blob-compact")
        .unwrap()
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap();
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        published
    );
    assert_eq!(rows(&table).await, expected);
    assert_eq!(rows(&historical).await, before);
    assert!(published_files.is_subset(&physical_files(&table).await));
}

#[tokio::test]
async fn cross_range_blob_result_requires_its_normal_merge_and_remaps_latest_deletions() {
    for commit_normal_first in [false, true] {
        let table = table(&[
            ("target-file-row-num", "2"),
            ("compaction.min.file-num", "2"),
        ])
        .await;
        seed_chunks(&table, 1).await;
        let expected = rows(&table).await;
        let plan = compact_plan(&table).await;
        assert_eq!(plan.tasks().len(), 2);
        let mut results = execute(&table, &plan).await;
        assert!(results[0].files()[0].file_name.ends_with(".parquet"));
        assert!(results[1].files()[0].file_name.ends_with(".blob"));
        let commit = table.new_compact_builder().new_commit().unwrap();
        let error = commit
            .commit_compact(vec![results[1].clone()])
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("not covered by one data file range"),
            "{error}"
        );
        assert_eq!(rows(&table).await, expected);
        delete(&table, 2).await;
        let expected = rows(&table).await;
        if commit_normal_first {
            commit
                .commit_compact(vec![results.remove(0)])
                .await
                .unwrap();
            assert_eq!(rows(&table).await, expected);
        }
        commit.commit_compact(results).await.unwrap();
        assert_eq!(rows(&table).await, expected);
        let read = table.new_read_builder();
        let files = read.new_scan().plan().await.unwrap();
        assert_eq!(
            files
                .splits()
                .iter()
                .flat_map(|split| split.data_files())
                .count(),
            2
        );
        assert!(compact_plan(&table).await.tasks().is_empty());
    }
}

#[tokio::test]
async fn blob_tasks_stay_with_unmerged_normal_groups_and_commit_independently() {
    let table = table(&[
        ("target-file-row-num", "2"),
        ("compaction.min.file-num", "100"),
    ])
    .await;
    seed_chunks(&table, 1).await;
    assert!(compact_plan(&table).await.tasks().is_empty());
    update(&table, 0, Some(b"first-group")).await;
    update(&table, 4, Some(b"last-group")).await;
    let plan = compact_plan(&table).await;
    assert_eq!(plan.tasks().len(), 2);
    for task in plan.tasks() {
        let range = task.files()[0].row_id_range().unwrap();
        assert!(range == (0, 1) || range == (4, 4), "{range:?}");
        assert!(task.files().iter().all(|file| {
            file.file_name.ends_with(".blob")
                && file
                    .row_id_range()
                    .is_some_and(|(first, last)| first >= range.0 && last <= range.1)
        }));
    }
    let results = execute(&table, &plan).await;
    update(&table, 2, Some(b"middle-group")).await;
    let expected = rows(&table).await;
    let commit = table.new_compact_builder().new_commit().unwrap();
    for result in results.into_iter().rev() {
        commit.commit_compact(vec![result]).await.unwrap();
        assert_eq!(rows(&table).await, expected);
    }
    let remaining = compact_plan(&table).await;
    assert_eq!(remaining.tasks().len(), 1);
    assert!(remaining.tasks()[0]
        .files()
        .iter()
        .all(|file| file.first_row_id == Some(2)));
    let results = execute(&table, &remaining).await;
    assert_eq!(results[0].files()[0].row_id_range(), Some((2, 3)));
    commit.commit_compact(results).await.unwrap();
    assert_eq!(rows(&table).await, expected);
    assert!(compact_plan(&table).await.tasks().is_empty());
}

#[tokio::test]
async fn omitted_versions_and_forged_output_metadata_are_rejected() {
    let table = table(&[("compaction.min.file-num", "2")]).await;
    seed(&table).await;
    update(&table, 1, Some(b"new")).await;
    update(&table, 3, None).await;
    let expected = rows(&table).await;
    let plan = compact_plan(&table).await;
    let executor = table.new_compact_builder().new_executor().unwrap();
    let before = physical_files(&table).await;
    for (key, value, expected) in [
        (
            "blob_field_id",
            serde_json::Value::Null,
            "missing its field identity",
        ),
        (
            "blob_field_id",
            serde_json::json!(-1),
            "not a current dedicated BLOB field",
        ),
        (
            "files",
            serde_json::json!(&plan.tasks()[0].files()[..1]),
            "at least two",
        ),
    ] {
        let mut task = serde_json::to_value(&plan.tasks()[0]).unwrap();
        task[key] = value;
        let error = executor
            .execute(&serde_json::from_value(task).unwrap())
            .await
            .unwrap_err();
        assert!(error.to_string().contains(expected), "{key}: {error}");
        assert_eq!(physical_files(&table).await, before);
    }
    for (key, value) in [
        ("_FIRST_ROW_ID", serde_json::json!(10)),
        ("_ROW_COUNT", serde_json::json!(0)),
        ("_MIN_SEQUENCE_NUMBER", serde_json::json!(0)),
        ("_MAX_SEQUENCE_NUMBER", serde_json::json!(0)),
    ] {
        let mut task = serde_json::to_value(&plan.tasks()[0]).unwrap();
        task["files"][2][key] = value;
        let error = executor
            .execute(&serde_json::from_value(task).unwrap())
            .await
            .unwrap_err();
        assert!(
            matches!(error, paimon::Error::DataInvalid { .. }),
            "{key}: {error}"
        );
        assert_eq!(physical_files(&table).await, before);
    }
    let mut task = serde_json::to_value(&plan.tasks()[0]).unwrap();
    task["files"].as_array_mut().unwrap().remove(1);
    let task = serde_json::from_value(task).unwrap();
    let error = executor.execute(&task).await.unwrap_err();
    assert!(error.to_string().contains("omit overlapping"), "{error}");
    let results = execute(&table, &plan).await;
    let commit = table.new_compact_builder().new_commit().unwrap();
    let mut incomplete = serde_json::to_value(&results).unwrap();
    incomplete[0]["task"]["files"]
        .as_array_mut()
        .unwrap()
        .remove(1);
    let error = commit
        .commit_compact(serde_json::from_value(incomplete).unwrap())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("omit overlapping"), "{error}");
    for (key, value) in [
        ("_FIRST_ROW_ID", serde_json::json!(1)),
        ("_ROW_COUNT", serde_json::json!(4)),
        ("_MIN_SEQUENCE_NUMBER", serde_json::json!(2)),
        ("_MAX_SEQUENCE_NUMBER", serde_json::json!(100)),
        ("_WRITE_COLS_SEQUENCES", serde_json::json!([1])),
        ("_WRITE_COLS", serde_json::json!(["value"])),
        ("_FILE_NAME", serde_json::json!("wrong-format.parquet")),
    ] {
        let mut broken = serde_json::to_value(&results).unwrap();
        broken[0]["files"][0][key] = value;
        let error = commit
            .commit_compact(serde_json::from_value(broken).unwrap())
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("preserve its row-ID range"),
            "{key}: {error}"
        );
    }
    commit.commit_compact(results).await.unwrap();
    assert_eq!(rows(&table).await, expected);
}

#[tokio::test]
async fn blob_compaction_revalidates_concurrent_writes_and_retains_submitted_outputs() {
    for (concurrent_write, added_files, conflicts) in [
        ("append", 3, false),
        ("payload", 2, true),
        ("value", 2, false),
    ] {
        let table = table(&[("compaction.min.file-num", "2")]).await;
        seed(&table).await;
        update(&table, 1, Some(b"new")).await;
        let plan = compact_plan(&table).await;
        let original = physical_files(&table).await;
        let results = execute(&table, &plan).await;
        assert_eq!(physical_files(&table).await.len(), original.len() + 1);
        match concurrent_write {
            "payload" => update(&table, 3, Some(b"concurrent")).await,
            "value" => {
                let messages = apply(
                    &table,
                    RecordBatch::try_from_iter([
                        ("_ROW_ID", Arc::new(Int64Array::from(vec![3])) as ArrayRef),
                        ("value", Arc::new(Int32Array::from(vec![99])) as ArrayRef),
                    ])
                    .unwrap(),
                )
                .await;
                table
                    .new_write_builder()
                    .new_commit()
                    .commit(messages)
                    .await
                    .unwrap();
            }
            "append" => {
                common::incremental_helpers::write_batch(
                    &table,
                    &RecordBatch::try_from_iter([
                        ("id", Arc::new(Int32Array::from(vec![5])) as ArrayRef),
                        (
                            "payload",
                            Arc::new(LargeBinaryArray::from(vec![Some(b"appended".as_slice())]))
                                as ArrayRef,
                        ),
                        ("value", Arc::new(Int32Array::from(vec![50])) as ArrayRef),
                    ])
                    .unwrap(),
                )
                .await;
            }
            _ => unreachable!(),
        }
        let expected = rows(&table).await;
        let commit = table.new_compact_builder().new_commit().unwrap();
        let outcome = commit.commit_compact(results).await;
        if conflicts {
            let error = outcome.unwrap_err();
            assert!(error.to_string().contains("omit overlapping"), "{error}");
        } else {
            outcome.unwrap();
        }
        let remaining = physical_files(&table).await;
        assert!(original.is_subset(&remaining));
        assert_eq!(remaining.len(), original.len() + added_files);
        assert_eq!(rows(&table).await, expected);
    }
}

#[tokio::test]
async fn blob_compaction_resolves_field_ids_after_renames_and_name_reuse() {
    let original = table(&[("compaction.min.file-num", "2")]).await;
    seed(&original).await;
    let mut imported_schema = serde_json::to_value(original.schema()).unwrap();
    imported_schema["fields"][1]["name"] = "renamed".into();
    let imported_schema: TableSchema = serde_json::from_value(imported_schema).unwrap();
    let schema = imported_schema
        .apply_changes(vec![SchemaChange::add_column(
            "payload".into(),
            DataType::Blob(BlobType::new()),
        )])
        .unwrap();
    persist_table_schema(original.file_io(), original.location(), &schema).await;
    let table = original.copy_with_resolved_schema(schema, "main").unwrap();
    let batch = RecordBatch::try_from_iter([
        ("_ROW_ID", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        (
            "renamed",
            Arc::new(LargeBinaryArray::from(vec![Some(b"renamed".as_slice())])) as ArrayRef,
        ),
        (
            "payload",
            Arc::new(LargeBinaryArray::from(vec![Some(b"other".as_slice())])) as ArrayRef,
        ),
    ])
    .unwrap();
    let messages = apply(&table, batch).await;
    table
        .new_write_builder()
        .new_commit()
        .commit(messages)
        .await
        .unwrap();
    update(&table, 2, Some(b"other-2")).await;
    let before = read_batch(&table).await;
    let plan = compact_plan(&table).await;
    assert_eq!(plan.tasks().len(), 2);
    let results = execute(&table, &plan).await;
    assert_eq!(
        results[0].files()[0].write_cols,
        Some(vec!["renamed".into()])
    );
    assert_eq!(
        results[1].files()[0].write_cols,
        Some(vec!["payload".into()])
    );
    let mut forged = serde_json::to_value(&results[..1]).unwrap();
    forged[0]["task"]["blob_field_id"] = table
        .schema()
        .fields()
        .iter()
        .find(|field| field.name() == "payload")
        .unwrap()
        .id()
        .into();
    forged[0]["files"][0]["_WRITE_COLS"] = serde_json::json!(["payload"]);
    let files_before = physical_files(&table).await;
    let error = table
        .new_compact_builder()
        .new_executor()
        .unwrap()
        .execute(&serde_json::from_value(forged[0]["task"].clone()).unwrap())
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("field ID does not match"),
        "{error}"
    );
    assert_eq!(physical_files(&table).await, files_before);
    let commit = table.new_compact_builder().new_commit().unwrap();
    let error = commit
        .commit_compact(serde_json::from_value(forged).unwrap())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("same field ID"), "{error}");
    let mut expected = before;
    for (index, result) in results.into_iter().rev().enumerate() {
        commit.commit_compact(vec![result]).await.unwrap();
        assert_eq!(read_batch(&table).await, expected);
        if index == 0 {
            update(&table, 0, Some(b"concurrent-other-field")).await;
            expected = read_batch(&table).await;
        }
    }
    let batch = read_batch(&table).await;
    let renamed = batch
        .column_by_name("renamed")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert_eq!(
        renamed.iter().collect::<Vec<_>>(),
        vec![
            Some(b"old-0".as_slice()),
            Some(b"renamed".as_slice()),
            Some(b"".as_slice()),
            Some(b"old-3".as_slice()),
            Some(b"old-4".as_slice())
        ]
    );
    let payload = batch
        .column_by_name("payload")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert_eq!(
        payload.iter().collect::<Vec<_>>(),
        vec![
            Some(b"concurrent-other-field".as_slice()),
            Some(b"other".as_slice()),
            Some(b"other-2".as_slice()),
            None,
            None,
        ]
    );
}

#[tokio::test]
async fn collection_updates_and_compaction_preserve_nulls_empty_values_and_fallbacks() {
    for map in [false, true] {
        for rolling in [false, true] {
            let blob = DataType::Blob(BlobType::new());
            let kind = if map {
                DataType::Map(MapType::new(
                    DataType::VarChar(VarCharType::new(100).unwrap()),
                    blob,
                ))
            } else {
                DataType::Array(ArrayType::new(blob))
            };
            let table = typed_table(
                kind,
                &[
                    (
                        "blob.target-file-size",
                        if rolling { "20 B" } else { "1 MB" },
                    ),
                    ("read.batch-size", "1"),
                ],
            )
            .await;
            let original = collection_values(
                map,
                &[
                    Some(vec![Some(b"zero"), None, Some(b"")]),
                    None,
                    Some(vec![]),
                    Some(vec![Some(b"three")]),
                    Some(vec![Some(b"four")]),
                ],
            );
            let builder = table.new_write_builder();
            let mut writer = builder.new_write().unwrap();
            writer
                .write_arrow_batch(
                    &RecordBatch::try_from_iter([
                        (
                            "id",
                            Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4])) as ArrayRef,
                        ),
                        ("payload", original.clone()),
                        (
                            "value",
                            Arc::new(Int32Array::from(vec![10, 11, 12, 13, 14])) as ArrayRef,
                        ),
                    ])
                    .unwrap(),
                )
                .await
                .unwrap();
            builder
                .new_commit()
                .commit(writer.prepare_commit().await.unwrap())
                .await
                .unwrap();
            let values = collection_values(
                map,
                &[
                    Some(vec![Some(b"unused")]),
                    Some(vec![Some(b"new"), None, Some(b"")]),
                    None,
                    Some(vec![Some(b"one")]),
                ],
            );
            let batch = RecordBatch::try_from_iter([
                (
                    "_ROW_ID",
                    Arc::new(Int64Array::from(vec![-1, 4, 3, 1])) as ArrayRef,
                ),
                ("payload", values),
            ])
            .unwrap()
            .slice(1, 3);
            let messages = apply(&table, batch).await;
            assert!(messages
                .iter()
                .flat_map(|message| &message.new_files)
                .all(|file| file.file_name.ends_with(".blob")));
            builder.new_commit().commit(messages).await.unwrap();
            let batch = read_batch(&table).await;
            let expected = collection_values(
                map,
                &[
                    Some(vec![Some(b"zero"), None, Some(b"")]),
                    Some(vec![Some(b"one")]),
                    Some(vec![]),
                    None,
                    Some(vec![Some(b"new"), None, Some(b"")]),
                ],
            );
            let expected = arrow_cast::cast(&expected, batch.column(1).data_type()).unwrap();
            assert_eq!(batch.column(1).to_data(), expected.to_data());
            let descriptors = table.copy_with_options(
                [("blob-as-descriptor".into(), "true".into())]
                    .into_iter()
                    .collect(),
            );
            let batch = read_batch(&descriptors).await;
            let payload = batch.column(1);
            assert!(payload.is_null(3));
            let (children, empty) = if map {
                let map = payload.as_any().downcast_ref::<MapArray>().unwrap();
                (map.value(4).column(1).clone(), map.value_length(2))
            } else {
                let list = payload.as_any().downcast_ref::<ListArray>().unwrap();
                (list.value(4), list.value_length(2))
            };
            assert_eq!(empty, 0);
            let values = children
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap();
            assert!(values.is_null(1));
            assert_eq!(
                BlobDescriptor::deserialize(values.value(0))
                    .unwrap()
                    .length(),
                3
            );
            assert_eq!(
                BlobDescriptor::deserialize(values.value(2))
                    .unwrap()
                    .length(),
                0
            );
            let before = read_batch(&table).await;
            let table =
                table.copy_with_options([("blob.target-file-size".into(), "1 MB".into())].into());
            let plan = compact_plan(&table).await;
            assert_eq!(plan.tasks().len(), 1);
            assert!(plan.tasks()[0].files().len() >= 2);
            let results = execute(&table, &plan).await;
            assert_eq!(results[0].files().len(), 1);
            assert_eq!(results[0].files()[0].row_id_range(), Some((0, 4)));
            assert!(results[0].files()[0].file_name.ends_with(".blob"));
            table
                .new_compact_builder()
                .new_commit()
                .unwrap()
                .commit_compact(results)
                .await
                .unwrap();
            assert_eq!(read_batch(&table).await, before);
            assert!(compact_plan(&table).await.tasks().is_empty());
        }
    }
}

#[tokio::test]
async fn late_blob_read_failure_cleans_partial_payload_output() {
    let table = table(&[
        ("blob.target-file-size", "1 b"),
        ("compaction.min.file-num", "2"),
        ("read.batch-size", "1"),
    ])
    .await;
    seed_chunks(&table, 1).await;
    let table = table.copy_with_options([("blob.target-file-size".into(), "1 mb".into())].into());
    let plan = compact_plan(&table).await;
    assert_eq!(plan.tasks().len(), 1);
    let last = plan.tasks()[0].files().last().unwrap();
    let missing = format!("{}/bucket-0/{}", table.location(), last.file_name);
    table.file_io().delete_file(&missing).await.unwrap();
    let before = physical_files(&table).await;
    assert!(table
        .new_compact_builder()
        .new_executor()
        .unwrap()
        .execute(&plan.tasks()[0])
        .await
        .is_err());
    assert_eq!(physical_files(&table).await, before);
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        Some(1)
    );
}

#[tokio::test]
async fn compact_java_blob_payload_and_placeholder_fixtures() {
    let table = table(&[("compaction.min.file-num", "2")]).await;
    let builder = table.new_write_builder();
    let batch = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int32Array::from(vec![0, 1, 2, 3])) as ArrayRef,
        ),
        (
            "payload",
            Arc::new(LargeBinaryArray::from(vec![
                Some(b"hello".as_slice()),
                None,
                Some(b"world".as_slice()),
                Some(b"".as_slice()),
            ])) as ArrayRef,
        ),
        (
            "value",
            Arc::new(Int32Array::from(vec![10, 11, 12, 13])) as ArrayRef,
        ),
    ])
    .unwrap();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    let mut messages = writer.prepare_commit().await.unwrap();
    let file = messages
        .iter_mut()
        .flat_map(|message| &mut message.new_files)
        .find(|file| file.file_name.ends_with(".blob"))
        .unwrap();
    let java = include_bytes!("../../testdata/blob/blob-basic.blob");
    table
        .file_io()
        .new_output(&format!("{}/bucket-0/{}", table.location(), file.file_name))
        .unwrap()
        .write(bytes::Bytes::from_static(java))
        .await
        .unwrap();
    file.file_size = java.len() as i64;
    builder.new_commit().commit(messages).await.unwrap();
    let original = rows(&table).await;
    let mut messages = apply(&table, matched(vec![2], vec![Some(b"latest-3")])).await;
    let file = &mut messages[0].new_files[0];
    let java = include_bytes!("../../testdata/blob/blob-placeholder.blob");
    table
        .file_io()
        .new_output(&format!("{}/bucket-0/{}", table.location(), file.file_name))
        .unwrap()
        .write(bytes::Bytes::from_static(java))
        .await
        .unwrap();
    file.file_size = java.len() as i64;
    file.row_count = 4;
    file.min_sequence_number = 1;
    file.max_sequence_number = 2;
    file.column_max_sequence_numbers = Some(vec![1]);
    builder.new_commit().commit(messages).await.unwrap();
    let before = rows(&table).await;
    assert_eq!(
        before
            .iter()
            .map(|row| row.1.as_deref())
            .collect::<Vec<_>>(),
        vec![
            Some(b"hello".as_slice()),
            None,
            Some(b"latest-3".as_slice()),
            Some(b"".as_slice())
        ]
    );
    let plan = compact_plan(&table).await;
    let results = execute(&table, &plan).await;
    assert_eq!(
        results[0].files()[0].column_max_sequence_numbers,
        Some(vec![1])
    );
    builder.new_commit().commit_compact(results).await.unwrap();
    assert_eq!(rows(&table).await, before);
    let history = table.copy_with_options([("scan.snapshot-id".into(), "1".into())].into());
    assert_eq!(rows(&history).await, original);
    update(&table, 0, Some(b"after-java-compaction")).await;
    let mut expected = before;
    expected[0].1 = Some(b"after-java-compaction".to_vec());
    assert_eq!(rows(&table).await, expected);
    assert_eq!(rows(&history).await, original);
}

#[tokio::test]
async fn dropped_blob_field_is_not_compacted_into_reused_name() {
    let original = table(&[("compaction.min.file-num", "2")]).await;
    seed(&original).await;
    update(&original, 1, Some(b"old-field")).await;
    let historical = original.copy_with_options([("scan.snapshot-id".into(), "2".into())].into());
    let historical_rows = rows(&historical).await;
    let schema = original
        .schema()
        .apply_changes(vec![
            SchemaChange::drop_column("payload".into()),
            SchemaChange::add_column("payload".into(), DataType::Blob(BlobType::new())),
        ])
        .unwrap();
    persist_table_schema(original.file_io(), original.location(), &schema).await;
    let table = original.copy_with_resolved_schema(schema, "main").unwrap();
    update(&table, 1, Some(b"new-field")).await;
    update(&table, 2, Some(b"new-field-2")).await;
    let expected = vec![
        (0, None, 10),
        (1, Some(b"new-field".to_vec()), 11),
        (2, Some(b"new-field-2".to_vec()), 12),
        (3, None, 13),
        (4, None, 14),
    ];
    assert_eq!(rows(&table).await, expected);
    let plan = compact_plan(&table).await;
    assert_eq!(plan.tasks().len(), 1);
    assert!(plan.tasks()[0]
        .files()
        .iter()
        .all(|file| file.schema_id == table.schema().id()));
    let results = execute(&table, &plan).await;
    table
        .new_compact_builder()
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap();
    assert_eq!(rows(&table).await, expected);
    assert_eq!(rows(&historical).await, historical_rows);
}
