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

async fn compact(table: &Table) {
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    builder
        .new_commit()
        .unwrap()
        .commit_compact(execute(table, &plan).await)
        .await
        .unwrap();
}

#[tokio::test]
async fn postpone_compaction_routes_unsorted_rows_and_publishes_atomically() {
    let table = table(&[
        ("bucket", "-2"),
        ("postpone.target-row-num-per-bucket", "2"),
        ("read.batch-size", "2"),
    ])
    .await;
    write_batch(
        &table,
        &make_batch_with_kinds(
            vec![5, 1, 3, 1, 3],
            vec![50, 10, 30, 11, 30],
            vec![0, 0, 0, 2, 3],
        ),
    )
    .await;
    assert!(rows(&table).await.is_empty());
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    assert_eq!(plan.tasks().len(), 1);
    let results = execute(&table, &plan).await;
    assert!(rows(&table).await.is_empty());
    let commit = builder.new_commit().unwrap();
    let metrics = commit.commit_compact(results.clone()).await.unwrap();
    assert_eq!(metrics.files_removed, 1);
    assert_eq!(rows(&table).await, vec![(1, 11), (5, 50)]);
    assert_eq!(commit.commit_compact(results).await.unwrap(), metrics);
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    assert!(plan
        .splits()
        .iter()
        .all(|s| s.bucket() >= 0 && s.total_buckets() == 3));
    assert!(builder
        .new_planner()
        .unwrap()
        .plan()
        .await
        .unwrap()
        .tasks()
        .is_empty());
}

#[tokio::test]
async fn postpone_compaction_replays_after_existing_state_and_keeps_new_pending_files() {
    let table = table(&[("bucket", "-2"), ("postpone.default-bucket-num", "2")]).await;
    let fixed = table.new_postpone_fixed_bucket_write_builder().unwrap();
    let mut writer = fixed.new_write().unwrap();
    writer
        .write_arrow_batch(&make_batch(vec![1, 2], vec![10, 20]))
        .await
        .unwrap();
    fixed
        .new_commit()
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    builder
        .new_commit()
        .unwrap()
        .commit_compact(execute(&table, &plan).await)
        .await
        .unwrap();
    assert_eq!(rows(&table).await, vec![(1, 10), (2, 20)]);
    write_batch(
        &table,
        &make_batch_with_kinds(vec![2, 1], vec![20, 11], vec![3, 2]),
    )
    .await;
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let results = execute(&table, &plan).await;
    write_batch(&table, &make_batch(vec![4], vec![40])).await;
    builder
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap();
    assert_eq!(rows(&table).await, vec![(1, 11)]);
    let next = builder.new_planner().unwrap().plan().await.unwrap();
    builder
        .new_commit()
        .unwrap()
        .commit_compact(execute(&table, &next).await)
        .await
        .unwrap();
    assert_eq!(rows(&table).await, vec![(1, 11), (4, 40)]);
}

#[tokio::test]
async fn postpone_compaction_preserves_merge_engines_across_formats() {
    for format in ["parquet", "orc", "avro", "row"] {
        for (engine, first, second) in [
            ("deduplicate", 20, 5),
            ("first-row", 10, 10),
            ("partial-update", 20, 5),
            ("aggregation", 30, 35),
        ] {
            let mut options = vec![
                ("bucket", "-2"),
                ("merge-engine", engine),
                ("postpone.default-bucket-num", "2"),
                ("file.format", format),
                ("file.compression", "none"),
                ("read.batch-size", "1"),
            ];
            if engine == "aggregation" {
                options.push(("fields.value.aggregate-function", "sum"));
            }
            let table = table(&options).await;
            write_batch(&table, &make_batch(vec![1, 1], vec![10, 20])).await;
            compact(&table).await;
            assert_eq!(rows(&table).await, vec![(1, first)], "{engine}/{format}");
            write_batch(&table, &make_batch(vec![1], vec![5])).await;
            compact(&table).await;
            assert_eq!(rows(&table).await, vec![(1, second)], "{engine}/{format}");
            assert!(table
                .file_io()
                .list_status_recursive(table.location())
                .await
                .unwrap()
                .iter()
                .all(|file| !file.path.contains("postpone-sort-")));
        }
    }
}

#[tokio::test]
async fn postpone_lookup_compaction_emits_history_images() {
    let table = table(&[("bucket", "-2"), ("changelog-producer", "lookup")]).await;
    write_batch(&table, &make_batch(vec![2, 1], vec![20, 10])).await;
    compact(&table).await;
    let first = table
        .snapshot_manager()
        .get_latest_snapshot_id()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        lookup_changelog_tests::events(&table, 0, first).await,
        vec![("+I".into(), 1, 10), ("+I".into(), 2, 20)]
    );
    write_batch(
        &table,
        &make_batch_with_kinds(vec![1, 2, 3], vec![11, 999, 30], vec![2, 3, 0]),
    )
    .await;
    compact(&table).await;
    let second = table
        .snapshot_manager()
        .get_latest_snapshot_id()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        lookup_changelog_tests::events(&table, first, second).await,
        vec![
            ("+I".into(), 3, 30),
            ("+U".into(), 1, 11),
            ("-D".into(), 2, 20),
            ("-U".into(), 1, 10)
        ]
    );
    assert_eq!(rows(&table).await, vec![(1, 11), (3, 30)]);
}

#[tokio::test]
async fn postpone_compaction_preserves_writer_order_when_file_times_tie() {
    let table = table(&[("bucket", "-2"), ("read.batch-size", "1")]).await;
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    for value in 0..13 {
        writer
            .write_arrow_batch(&make_batch(vec![1], vec![value]))
            .await
            .unwrap();
        let mut messages = writer.prepare_commit().await.unwrap();
        for file in messages
            .iter_mut()
            .flat_map(|message| &mut message.new_files)
        {
            file.creation_time = chrono::DateTime::from_timestamp_millis(42);
        }
        builder.new_commit().commit(messages).await.unwrap();
    }
    compact(&table).await;
    assert_eq!(rows(&table).await, vec![(1, 12)]);
}

#[tokio::test]
async fn postpone_compaction_rejects_concurrent_real_bucket_writes_and_overwrites() {
    for overwrite in [false, true] {
        let table = table(&[("bucket", "-2"), ("postpone.default-bucket-num", "1")]).await;
        write_batch(&table, &make_batch(vec![1], vec![10])).await;
        let builder = table.new_compact_builder();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        let results = execute(&table, &plan).await;
        let fixed = table.new_postpone_fixed_bucket_write_builder().unwrap();
        let fixed = if overwrite {
            fixed.with_overwrite()
        } else {
            fixed
        };
        let mut writer = fixed.new_write().unwrap();
        writer
            .write_arrow_batch(&make_batch(vec![2], vec![20]))
            .await
            .unwrap();
        fixed
            .new_commit()
            .commit(writer.prepare_commit().await.unwrap())
            .await
            .unwrap();
        let snapshot = table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap();
        assert!(builder
            .new_commit()
            .unwrap()
            .commit_compact(results)
            .await
            .is_err());
        assert_eq!(
            table
                .snapshot_manager()
                .get_latest_snapshot_id()
                .await
                .unwrap(),
            snapshot
        );
        compact(&table).await;
        assert_eq!(
            rows(&table).await,
            if overwrite {
                vec![(2, 20)]
            } else {
                vec![(1, 10), (2, 20)]
            }
        );
    }
}

#[tokio::test]
async fn postpone_partition_tasks_support_subsets_and_reject_incomplete_mappings() {
    let schema = Schema::builder()
        .column("pt", DataType::VarChar(VarCharType::string_type()))
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .primary_key(["pt", "id"])
        .partition_keys(["pt"])
        .option("bucket", "-2")
        .option("postpone.default-bucket-num", "2")
        .build()
        .unwrap();
    let table = table_with_schema(TableSchema::new(0, &schema)).await;
    write_batch(
        &table,
        &make_partitioned_batch(vec!["a", "b"], vec![1, 2], vec![10, 20]),
    )
    .await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    assert_eq!(plan.tasks().len(), 2);
    let mut results = execute(&table, &plan).await;
    let mut invalid = serde_json::to_value(&results).unwrap();
    invalid[0].as_object_mut().unwrap().remove("file_buckets");
    let invalid: Vec<CompactResult> = serde_json::from_value(invalid).unwrap();
    assert!(builder
        .new_commit()
        .unwrap()
        .commit_compact(invalid)
        .await
        .is_err());
    let first = results.remove(0);
    builder
        .new_commit()
        .unwrap()
        .commit_compact(vec![first])
        .await
        .unwrap();
    assert_eq!(rows(&table).await, vec![(1, 10)]);
    builder
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap();
    assert_eq!(rows(&table).await, vec![(1, 10), (2, 20)]);
    write_batch(
        &table,
        &make_partitioned_batch(vec!["a", "b"], vec![1, 2], vec![11, 21]),
    )
    .await;
    let filter = PredicateBuilder::new(table.schema().fields())
        .equal("pt", Datum::String("b".into()))
        .unwrap();
    let filtered = table
        .new_compact_builder()
        .with_partition_filter(filter)
        .unwrap();
    let plan = filtered.new_planner().unwrap().plan().await.unwrap();
    assert_eq!(plan.tasks().len(), 1);
    let mut task = serde_json::to_value(&plan.tasks()[0]).unwrap();
    task["files"].as_array_mut().unwrap().pop();
    task["input_buckets"].as_array_mut().unwrap().pop();
    let task: CompactTask = serde_json::from_value(task).unwrap();
    assert!(filtered
        .new_executor()
        .unwrap()
        .execute(&task)
        .await
        .is_err());
    filtered
        .new_commit()
        .unwrap()
        .commit_compact(execute(&table, &plan).await)
        .await
        .unwrap();
    assert_eq!(rows(&table).await, vec![(1, 10), (2, 21)]);
}

#[tokio::test]
async fn postpone_compaction_merges_many_sort_runs_without_reducing_raw_updates() {
    let table = table(&[
        ("bucket", "-2"),
        ("read.batch-size", "1"),
        ("write-buffer-size", "1b"),
        ("merge-engine", "aggregation"),
        ("fields.value.aggregate-function", "sum"),
    ])
    .await;
    write_batch(&table, &make_batch(vec![1; 600], vec![1; 600])).await;
    compact(&table).await;
    assert_eq!(rows(&table).await, vec![(1, 600)]);
    assert!(table
        .file_io()
        .list_status_recursive(table.location())
        .await
        .unwrap()
        .iter()
        .all(|file| !file.path.contains("postpone-sort-")));
}

#[tokio::test]
async fn postpone_compaction_aborts_staged_and_output_files_on_merge_failure() {
    let table = table(&[("bucket", "-2"), ("merge-engine", "first-row")]).await;
    let legacy = table.copy_with_options(std::collections::HashMap::from([(
        "merge-engine".into(),
        "deduplicate".into(),
    )]));
    write_batch(
        &legacy,
        &make_batch_with_kinds(vec![1, 2], vec![10, 20], vec![0, 3]),
    )
    .await;
    let before = table
        .file_io()
        .list_status_recursive(table.location())
        .await
        .unwrap()
        .len();
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let error = builder
        .new_executor()
        .unwrap()
        .execute(&plan.tasks()[0])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("does not support DELETE"));
    assert_eq!(
        table
            .file_io()
            .list_status_recursive(table.location())
            .await
            .unwrap()
            .len(),
        before
    );
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
async fn postpone_compaction_reads_java_kv_physical_layout() {
    for format in ["parquet", "orc", "avro", "row"] {
        let table = table(&[
            ("bucket", "-2"),
            ("file.format", format),
            ("file.compression", "none"),
        ])
        .await;
        let fixed_table = table.copy_with_options(std::collections::HashMap::from([(
            "bucket".into(),
            "1".into(),
        )]));
        let fixed = fixed_table.new_write_builder();
        let mut writer = fixed.new_write().unwrap();
        writer
            .write_arrow_batch(&make_batch(vec![2, 1], vec![20, 10]))
            .await
            .unwrap();
        let mut messages = writer.prepare_commit().await.unwrap();
        for message in &mut messages {
            for file in &mut message.new_files {
                file.external_path =
                    Some(format!("{}/bucket-0/{}", table.location(), file.file_name));
                file.file_name = format!("data--u-java-s-0-w-{}", file.file_name);
            }
            message.bucket = -2;
            message.total_buckets = None;
        }
        table
            .new_write_builder()
            .new_commit()
            .commit(messages)
            .await
            .unwrap();
        compact(&table).await;
        assert_eq!(rows(&table).await, vec![(1, 10), (2, 20)], "{format}");
    }
}

#[tokio::test]
async fn postpone_compaction_keeps_existing_layout_over_configured_defaults() {
    let table = table(&[
        ("bucket", "-2"),
        ("postpone.default-bucket-num", "7"),
        ("postpone.batch-write-fixed-bucket.max-parallelism", "2"),
    ])
    .await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    compact(&table).await;
    let changed = table.copy_with_options(std::collections::HashMap::from([(
        "postpone.default-bucket-num".into(),
        "1".into(),
    )]));
    write_batch(&changed, &make_batch(vec![1, 2], vec![11, 20])).await;
    compact(&changed).await;
    let plan = changed.new_read_builder().new_scan().plan().await.unwrap();
    assert!(plan.splits().iter().all(|split| split.total_buckets() == 7));
    assert_eq!(rows(&changed).await, vec![(1, 11), (2, 20)]);
}

#[tokio::test]
async fn postpone_dv_compaction_rejects_other_merge_engines() {
    let table = table(&[
        ("bucket", "-2"),
        ("deletion-vectors.enabled", "true"),
        ("merge-engine", "aggregation"),
    ])
    .await;
    let error = table.new_compact_builder().new_planner().err().unwrap();
    assert!(error
        .to_string()
        .contains("Primary-key deletion-vector compaction requires deduplicate"));
}

#[tokio::test]
async fn postpone_java_row_null_payload_cannot_be_decoded_as_legacy_layout() {
    use std::sync::Arc;

    let table = table(&[("bucket", "-2"), ("file.format", "row")]).await;
    let fixed_table = table.copy_with_options(std::collections::HashMap::from([(
        "bucket".into(),
        "1".into(),
    )]));
    let builder = fixed_table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    let batch = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int32Array::from(vec![1])) as arrow_array::ArrayRef,
        ),
        (
            "value",
            Arc::new(Int32Array::from(vec![None::<i32>])) as arrow_array::ArrayRef,
        ),
    ])
    .unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    let mut messages = writer.prepare_commit().await.unwrap();
    for message in &mut messages {
        for file in &mut message.new_files {
            file.external_path = Some(format!("{}/bucket-0/{}", table.location(), file.file_name));
            file.file_name = format!("data--u-java-s-0-w-{}", file.file_name);
        }
        message.bucket = -2;
        message.total_buckets = None;
    }
    table
        .new_write_builder()
        .new_commit()
        .commit(messages)
        .await
        .unwrap();
    compact(&table).await;
    let batches = read(&table, None).await;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let batch = batches.iter().find(|batch| batch.num_rows() != 0).unwrap();
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.value(0), 1);
    assert!(batch.column_by_name("value").unwrap().is_null(0));
}

#[tokio::test]
async fn postpone_equal_time_java_files_keep_manifest_order() {
    let table = table(&[("bucket", "-2")]).await;
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer
        .write_arrow_batch(&make_batch(vec![1], vec![10]))
        .await
        .unwrap();
    let mut messages = writer.prepare_commit().await.unwrap();
    writer
        .write_arrow_batch(&make_batch_with_kinds(vec![1], vec![10], vec![3]))
        .await
        .unwrap();
    let later = writer.prepare_commit().await.unwrap();
    messages[0]
        .new_files
        .extend(later.into_iter().flat_map(|message| message.new_files));
    for (file, suffix) in messages[0].new_files.iter_mut().zip(["z", "a"]) {
        file.external_path = Some(format!(
            "{}/bucket-postpone/{}",
            table.location(),
            file.file_name
        ));
        let (prefix, _) = file.file_name.rsplit_once("-w-").unwrap();
        file.file_name = format!("{prefix}-w-{suffix}.parquet");
        file.creation_time = chrono::DateTime::from_timestamp_millis(42);
        // Java postpone writers use UNKNOWN_SEQUENCE across these files.
        file.min_sequence_number = -1;
        file.max_sequence_number = -1;
    }
    builder.new_commit().commit(messages).await.unwrap();
    compact(&table).await;
    assert!(
        rows(&table).await.is_empty(),
        "the later DELETE must win over the earlier INSERT"
    );
}

async fn java_null_row_fixture(thin: bool, real_bucket: bool) -> Table {
    use paimon::spec::BinaryTableStats;
    use std::sync::Arc;

    let table = table(&[
        ("bucket", "-2"),
        ("file.format", "row"),
        ("postpone.default-bucket-num", "1"),
        ("data-file.thin-mode", if thin { "true" } else { "false" }),
    ])
    .await;
    let batch = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int32Array::from(vec![1])) as arrow_array::ArrayRef,
        ),
        (
            "value",
            Arc::new(Int32Array::from(vec![None::<i32>])) as arrow_array::ArrayRef,
        ),
    ])
    .unwrap();
    let ordinary = table.new_write_builder();
    let fixed = table.new_postpone_fixed_bucket_write_builder().unwrap();
    let mut messages = if real_bucket {
        let mut writer = fixed.new_write().unwrap();
        writer.write_arrow_batch(&batch).await.unwrap();
        writer.prepare_commit().await.unwrap()
    } else {
        let mut writer = ordinary.new_write().unwrap();
        writer.write_arrow_batch(&batch).await.unwrap();
        writer.prepare_commit().await.unwrap()
    };
    let bytes: &[u8] = if thin {
        include_bytes!("../../testdata/postpone_row/java_postpone_thin_null.row")
    } else {
        include_bytes!("../../testdata/postpone_row/java_postpone_kv_null.row")
    };
    for message in &mut messages {
        for file in &mut message.new_files {
            let directory = if real_bucket {
                "bucket-0"
            } else {
                "bucket-postpone"
            };
            table
                .file_io()
                .new_output(&format!(
                    "{}/{directory}/{}",
                    table.location(),
                    file.file_name
                ))
                .unwrap()
                .write(bytes::Bytes::copy_from_slice(bytes))
                .await
                .unwrap();
            file.file_size = bytes.len() as i64;
            file.min_sequence_number = -1;
            file.max_sequence_number = -1;
            file.key_stats =
                BinaryTableStats::new(file.min_key.clone(), file.max_key.clone(), vec![Some(0)]);
        }
    }
    if real_bucket {
        fixed.new_commit().commit(messages).await.unwrap();
    } else {
        ordinary.new_commit().commit(messages).await.unwrap();
    }
    table
}

async fn disable_thin_mode(table: &Table) -> Table {
    let schema = table
        .schema()
        .apply_changes(vec![paimon::spec::SchemaChange::set_option(
            "data-file.thin-mode".into(),
            "false".into(),
        )])
        .unwrap();
    persist_table_schema(table.file_io(), table.location(), &schema).await;
    table.copy_with_resolved_schema(schema, "main").unwrap()
}

#[tokio::test]
async fn postpone_reads_java_generated_null_row_fixtures() {
    for thin in [false, true] {
        let original = java_null_row_fixture(thin, false).await;
        let table = if thin {
            disable_thin_mode(&original).await
        } else {
            original
        };
        compact(&table).await;
        let batches = read(&table, None).await;
        let batch = batches.iter().find(|batch| batch.num_rows() != 0).unwrap();
        assert_eq!(
            batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            1
        );
        assert!(batch.column_by_name("value").unwrap().is_null(0));
    }
}

#[tokio::test]
async fn postpone_rejects_unsupported_thin_row_output_and_real_history() {
    for real_bucket in [false, true] {
        let original = java_null_row_fixture(true, real_bucket).await;
        assert!(original.new_compact_builder().new_planner().is_err());
        // A temporary option must not make Java read full KV output as thin ROW.
        let temporary = original.copy_with_options(std::collections::HashMap::from([(
            "data-file.thin-mode".into(),
            "false".into(),
        )]));
        let builder = temporary.new_compact_builder();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        let error = builder
            .new_executor()
            .unwrap()
            .execute(&plan.tasks()[0])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("cannot write thin-mode ROW"));
        if real_bucket {
            let changed = disable_thin_mode(&original).await;
            let builder = changed.new_compact_builder();
            let plan = builder.new_planner().unwrap().plan().await.unwrap();
            let error = builder
                .new_executor()
                .unwrap()
                .execute(&plan.tasks()[0])
                .await
                .unwrap_err();
            assert!(error.to_string().contains("real-bucket thin-mode ROW"));
        }
        assert_eq!(
            original
                .snapshot_manager()
                .get_latest_snapshot_id()
                .await
                .unwrap(),
            Some(1)
        );
    }
}

#[tokio::test]
async fn postpone_mixed_row_changelog_obeys_persisted_thin_layout() {
    let original = table(&[
        ("bucket", "-2"),
        ("file.format", "parquet"),
        ("changelog-file.format", "row"),
        ("changelog-producer", "lookup"),
        ("data-file.thin-mode", "true"),
    ])
    .await;
    write_batch(&original, &make_batch(vec![1], vec![10])).await;
    assert!(
        original.new_compact_builder().new_planner().is_err(),
        "ROW changelog must use the same layout guard as ROW data"
    );
    let temporary = original.copy_with_options(std::collections::HashMap::from([(
        "data-file.thin-mode".into(),
        "false".into(),
    )]));
    let builder = temporary.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let error = builder
        .new_executor()
        .unwrap()
        .execute(&plan.tasks()[0])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("cannot write thin-mode ROW"));
    assert_eq!(
        original
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        Some(1)
    );
    let changed = disable_thin_mode(&original).await;
    let builder = changed.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let results = execute(&changed, &plan).await;
    assert!(results[0]
        .changelog_files()
        .iter()
        .all(|file| file.file_name.ends_with(".row")));
    builder
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap();
    assert_eq!(
        lookup_changelog_tests::events(
            &changed,
            0,
            changed
                .snapshot_manager()
                .get_latest_snapshot_id()
                .await
                .unwrap()
                .unwrap()
        )
        .await,
        vec![("+I".into(), 1, 10)]
    );
}
