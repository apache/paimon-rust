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

async fn lookup_table(options: &[(&str, &str)]) -> Table {
    let mut configured = vec![("changelog-producer", "lookup")];
    configured.extend_from_slice(options);
    table(&configured).await
}

async fn compact(table: &Table) -> i64 {
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    builder
        .new_commit()
        .unwrap()
        .commit_compact(execute(table, &plan).await)
        .await
        .unwrap();
    table
        .snapshot_manager()
        .get_latest_snapshot_id()
        .await
        .unwrap()
        .unwrap()
}

pub(super) async fn events(table: &Table, start: i64, end: i64) -> Vec<(String, i32, i32)> {
    let plan = table
        .new_read_builder()
        .new_incremental_scan(IncrementalScanMode::Auto, start, end)
        .plan_combined()
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow_with_row_kind(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut events = Vec::new();
    for batch in batches {
        let kinds = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
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
        events.extend((0..batch.num_rows()).map(|row| {
            (
                kinds.value(row).to_string(),
                ids.value(row),
                values.value(row),
            )
        }));
    }
    events.sort();
    events
}

fn expected(events: &[(&str, i32, i32)]) -> Vec<(String, i32, i32)> {
    let mut events = events
        .iter()
        .map(|(kind, key, value)| (kind.to_string(), *key, *value))
        .collect::<Vec<_>>();
    events.sort();
    events
}

#[tokio::test]
async fn lookup_compaction_publishes_before_after_images_for_fixed_dynamic_and_dv_tables() {
    for dv in ["false", "true"] {
        for bucket in ["1", "-1"] {
            let table = lookup_table(&[
                ("deletion-vectors.enabled", dv),
                ("bucket", bucket),
                ("read.batch-size", "1"),
            ])
            .await;
            write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
            assert!(events(&table, 0, 1).await.is_empty());
            let first = compact(&table).await;
            assert_eq!(
                events(&table, 0, first).await,
                expected(&[("+I", 1, 10), ("+I", 2, 20)])
            );
            write_batch(
                &table,
                &make_batch_with_kinds(vec![1, 2, 3], vec![11, 999, 30], vec![0, 3, 0]),
            )
            .await;
            let second = compact(&table).await;
            assert_eq!(
                events(&table, first, second).await,
                expected(&[("-U", 1, 10), ("+U", 1, 11), ("-D", 2, 20), ("+I", 3, 30)])
            );
            assert_eq!(rows(&table).await, vec![(1, 11), (3, 30)]);
            assert_eq!(
                compact(&table).await,
                second,
                "an already compacted bucket emits nothing"
            );
            assert_eq!(events(&table, first, second).await.len(), 4);
        }
    }
}

#[tokio::test]
async fn lookup_compaction_respects_java_changelog_flags_and_default_duplicate_updates() {
    for (options, expected_events) in [
        (
            vec![],
            expected(&[("-U", 1, 10), ("+U", 1, 10), ("-D", 2, 20)]),
        ),
        (
            vec![("changelog-producer.row-deduplicate", "true")],
            expected(&[("-D", 2, 20)]),
        ),
        (
            vec![("changelog-producer.ignore-update-before", "true")],
            expected(&[("+U", 1, 10), ("-D", 2, 20)]),
        ),
        (
            vec![("changelog-producer.ignore-delete", "true")],
            expected(&[("-U", 1, 10), ("+U", 1, 10)]),
        ),
        (
            vec![
                ("changelog-producer.row-deduplicate", "true"),
                ("changelog-producer.row-deduplicate-ignore-fields", "value"),
                ("changelog-producer.ignore-delete", "true"),
            ],
            vec![],
        ),
    ] {
        let table = lookup_table(&options).await;
        write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
        let first = compact(&table).await;
        let updated_value = if options
            .iter()
            .any(|(key, _)| *key == "changelog-producer.row-deduplicate-ignore-fields")
        {
            11
        } else {
            10
        };
        write_batch(
            &table,
            &make_batch_with_kinds(vec![1, 2], vec![updated_value, 0], vec![0, 3]),
        )
        .await;
        let second = compact(&table).await;
        assert_eq!(
            events(&table, first, second).await,
            expected_events,
            "{options:?}"
        );
        assert_eq!(rows(&table).await, vec![(1, updated_value)]);
    }
}

#[tokio::test]
async fn lookup_compaction_first_row_only_emits_new_keys() {
    let table = lookup_table(&[("merge-engine", "first-row")]).await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    let first = compact(&table).await;
    assert_eq!(events(&table, 0, first).await, expected(&[("+I", 1, 10)]));
    write_batch(&table, &make_batch(vec![1, 2], vec![99, 20])).await;
    let second = compact(&table).await;
    assert_eq!(
        events(&table, first, second).await,
        expected(&[("+I", 2, 20)])
    );
    assert_eq!(rows(&table).await, vec![(1, 10), (2, 20)]);
}

#[tokio::test]
async fn lookup_compaction_materializes_aggregation_history_once_and_ignores_high_level_only_keys()
{
    for dv in ["false", "true"] {
        let table = lookup_table(&[
            ("deletion-vectors.enabled", dv),
            ("merge-engine", "aggregation"),
            ("fields.value.aggregate-function", "sum"),
        ])
        .await;
        write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
        let first = compact(&table).await;
        write_batch(&table, &make_batch(vec![1, 3], vec![5, 30])).await;
        let second = compact(&table).await;
        assert_eq!(
            events(&table, first, second).await,
            expected(&[("-U", 1, 10), ("+U", 1, 15), ("+I", 3, 30)])
        );
        write_batch(&table, &make_batch(vec![1], vec![7])).await;
        let third = compact(&table).await;
        assert_eq!(
            events(&table, second, third).await,
            expected(&[("-U", 1, 15), ("+U", 1, 22)])
        );
        assert_eq!(rows(&table).await, vec![(1, 22), (2, 20), (3, 30)]);
    }
}

async fn append_at_level(table: &Table, value: i32, level: i32) {
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer
        .write_arrow_batch(&make_batch(vec![1], vec![value]))
        .await
        .unwrap();
    let mut messages = writer.prepare_commit().await.unwrap();
    for file in messages
        .iter_mut()
        .flat_map(|message| &mut message.new_files)
    {
        file.level = level;
    }
    builder.new_commit().commit(messages).await.unwrap();
    writer.close().await;
}

#[tokio::test]
async fn lookup_compaction_selects_the_lowest_positive_level_as_materialized_history() {
    let table = lookup_table(&[
        ("merge-engine", "aggregation"),
        ("fields.value.aggregate-function", "sum"),
    ])
    .await;
    // Simulate Java lookup states: L2 already includes L3. Reducing both would
    // produce 30 rather than 20 when the L0 delta is applied.
    append_at_level(&table, 10, 3).await;
    append_at_level(&table, 15, 2).await;
    let history = table
        .snapshot_manager()
        .get_latest_snapshot_id()
        .await
        .unwrap()
        .unwrap();
    write_batch(&table, &make_batch(vec![1], vec![5])).await;
    assert_eq!(rows(&table).await, vec![(1, 20)]);
    let end = compact(&table).await;
    assert_eq!(
        events(&table, history, end).await,
        expected(&[("-U", 1, 15), ("+U", 1, 20)])
    );
    assert_eq!(rows(&table).await, vec![(1, 20)]);
}

#[tokio::test]
async fn lookup_compaction_uses_user_sequence_and_tombstones_for_changelog_images() {
    for dv in ["false", "true"] {
        let table = lookup_table(&[
            ("sequence.field", "value"),
            ("deletion-vectors.enabled", dv),
            ("changelog-producer.row-deduplicate", "true"),
        ])
        .await;
        write_batch(&table, &make_batch(vec![1], vec![100])).await;
        let first = compact(&table).await;
        write_batch(
            &table,
            &make_batch_with_kinds(vec![1, 9], vec![99, 99], vec![3, 3]),
        )
        .await;
        let second = compact(&table).await;
        assert!(events(&table, first, second).await.is_empty());
        assert_eq!(rows(&table).await, vec![(1, 100)]);
        write_batch(&table, &make_batch_with_kinds(vec![1], vec![101], vec![3])).await;
        let third = compact(&table).await;
        assert_eq!(
            events(&table, second, third).await,
            expected(&[("-D", 1, 100)])
        );
        assert!(rows(&table).await.is_empty());
        write_batch(&table, &make_batch(vec![1], vec![102])).await;
        let fourth = compact(&table).await;
        assert_eq!(
            events(&table, third, fourth).await,
            expected(&[("+I", 1, 102)])
        );
    }
}

#[tokio::test]
async fn lookup_compaction_subsets_and_retries_publish_each_changelog_once_with_concurrent_l0() {
    for dv in ["false", "true"] {
        let table = lookup_table(&[
            ("bucket", "-1"),
            ("dynamic-bucket.target-row-num", "1"),
            ("deletion-vectors.enabled", dv),
        ])
        .await;
        write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
        let first = compact(&table).await;
        write_batch(&table, &make_batch(vec![1, 2], vec![11, 21])).await;
        let builder = table
            .new_compact_builder()
            .with_commit_user("lookup-retry")
            .unwrap();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        assert_eq!(plan.tasks().len(), 2);
        let results = execute(&table, &plan).await;
        assert_eq!(
            results
                .iter()
                .map(|result| result.metrics().changelog_rows_written)
                .sum::<u64>(),
            4
        );
        write_batch(&table, &make_batch(vec![1], vec![12])).await;
        let commit = builder.new_commit().unwrap();
        for result in results {
            let metrics = commit.commit_compact(vec![result.clone()]).await.unwrap();
            let snapshot = table
                .snapshot_manager()
                .get_latest_snapshot_id()
                .await
                .unwrap();
            assert_eq!(commit.commit_compact(vec![result]).await.unwrap(), metrics);
            assert_eq!(
                table
                    .snapshot_manager()
                    .get_latest_snapshot_id()
                    .await
                    .unwrap(),
                snapshot
            );
        }
        let second = table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            events(&table, first, second).await,
            expected(&[("-U", 1, 10), ("+U", 1, 11), ("-U", 2, 20), ("+U", 2, 21)])
        );
        let third = compact(&table).await;
        assert_eq!(
            events(&table, second, third).await,
            expected(&[("-U", 1, 11), ("+U", 1, 12)])
        );
        assert_eq!(rows(&table).await, vec![(1, 12), (2, 21)]);
    }
}

#[tokio::test]
async fn lookup_compaction_rejects_concurrent_positive_level_history_at_another_level() {
    let table = lookup_table(&[]).await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    compact(&table).await;
    write_batch(&table, &make_batch(vec![1], vec![11])).await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let results = execute(&table, &plan).await;
    append_at_level(&table, 12, 3).await;
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
        .unwrap_err()
        .to_string()
        .contains("history changed"));
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        snapshot
    );
}

#[tokio::test]
async fn lookup_compaction_aborts_data_and_changelog_files_on_a_late_read_failure() {
    let table = lookup_table(&[
        ("read.batch-size", "1"),
        ("write-buffer-size", "1b"),
        ("target-file-row-num", "1"),
    ])
    .await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    write_batch(&table, &make_batch(vec![2], vec![20])).await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let scan = table.new_read_builder().new_scan().plan().await.unwrap();
    let second = plan.tasks()[0]
        .files()
        .iter()
        .find(|file| file.max_sequence_number == 1)
        .unwrap();
    table
        .file_io()
        .delete_file(&format!(
            "{}/{}",
            scan.splits()[0].bucket_path(),
            second.file_name
        ))
        .await
        .unwrap();
    let paths = || async {
        table
            .file_io()
            .list_status_recursive(table.location())
            .await
            .unwrap()
            .into_iter()
            .map(|file| file.path)
            .collect::<std::collections::BTreeSet<_>>()
    };
    let before = paths().await;
    assert!(builder
        .new_executor()
        .unwrap()
        .execute(&plan.tasks()[0])
        .await
        .is_err());
    assert_eq!(paths().await, before);
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        Some(2)
    );
}

#[tokio::test]
async fn lookup_compaction_writes_rolled_changelogs_in_each_supported_format() {
    for format in ["parquet", "orc", "avro", "row"] {
        let table = lookup_table(&[
            ("file.format", "parquet"),
            ("changelog-file.format", format),
            ("changelog-file.compression", "none"),
            ("changelog-file.prefix", "changes-"),
            ("target-file-row-num", "1"),
            ("write-buffer-size", "1b"),
        ])
        .await;
        write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
        let builder = table.new_compact_builder();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        let results = execute(&table, &plan).await;
        let logs = results[0].changelog_files();
        assert_eq!(logs.len(), 2, "{format}");
        assert!(logs
            .iter()
            .all(|file| file.file_name.starts_with("changes-")
                && file.file_name.ends_with(format)
                && file.row_count == 1
                && file.embedded_index.is_none()));
        let metrics = builder
            .new_commit()
            .unwrap()
            .commit_compact(results)
            .await
            .unwrap();
        assert_eq!(metrics.changelog_rows_written, 2);
        assert_eq!(metrics.changelog_files_added, 2);
        let end = table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            events(&table, 0, end).await,
            expected(&[("+I", 1, 10), ("+I", 2, 20)])
        );
    }
}

#[tokio::test]
async fn lookup_compaction_preserves_sparse_updates_and_nullable_before_after_images() {
    for dv in ["false", "true"] {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .column("text", DataType::VarChar(VarCharType::string_type()))
            .primary_key(["id"])
            .option("bucket", "1")
            .option("merge-engine", "partial-update")
            .option("changelog-producer", "lookup")
            .option("changelog-producer.row-deduplicate", "true")
            .option("deletion-vectors.enabled", dv)
            .build()
            .unwrap();
        let table = table_with_schema(TableSchema::new(0, &schema)).await;
        let sparse = |value, text| {
            RecordBatch::try_from_iter([
                (
                    "id",
                    std::sync::Arc::new(Int32Array::from(vec![1])) as arrow_array::ArrayRef,
                ),
                (
                    "value",
                    std::sync::Arc::new(Int32Array::from(vec![value])) as arrow_array::ArrayRef,
                ),
                (
                    "text",
                    std::sync::Arc::new(StringArray::from(vec![text])) as arrow_array::ArrayRef,
                ),
            ])
            .unwrap()
        };
        write_batch(&table, &sparse(None, Some("old"))).await;
        let first = compact(&table).await;
        write_batch(&table, &sparse(Some(0), None)).await;
        let second = compact(&table).await;
        let plan = table
            .new_read_builder()
            .new_incremental_scan(IncrementalScanMode::Changelog, first, second)
            .plan_combined()
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = table
            .new_read_builder()
            .new_read()
            .unwrap()
            .to_arrow_with_row_kind(plan.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let mut changes = Vec::new();
        for batch in batches {
            let kinds = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let values = batch
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let texts = batch
                .column_by_name("text")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                changes.push((
                    kinds.value(row).to_string(),
                    values.is_valid(row).then(|| values.value(row)),
                    texts.value(row).to_string(),
                ));
            }
        }
        changes.sort();
        assert_eq!(
            changes,
            vec![
                ("+U".into(), Some(0), "old".into()),
                ("-U".into(), None, "old".into())
            ]
        );
        let batches = read(&table, None).await;
        assert_eq!(
            batches[0]
                .column_by_name("text")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "old"
        );
    }
}

#[tokio::test]
async fn lookup_compaction_changelogs_are_available_to_streaming_follow_up_without_replaying_l0() {
    let table = lookup_table(&[]).await;
    let mut scan = table.new_stream_scan().unwrap();
    assert!(scan.plan().await.unwrap().is_none());
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    // Initial lookup state contains only materialized positive-level files.
    let initial = scan.plan().await.unwrap().unwrap();
    assert!(initial.splits().is_empty());
    compact(&table).await;
    let changes = scan.plan().await.unwrap().unwrap();
    let batches: Vec<RecordBatch> = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow_with_row_kind(changes.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "+I"
    );
    assert!(scan.plan().await.unwrap().is_none());
    write_batch(&table, &make_batch(vec![1], vec![11])).await;
    assert!(scan.plan().await.unwrap().is_none());
    compact(&table).await;
    let changes = scan.plan().await.unwrap().unwrap();
    let batches: Vec<RecordBatch> = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow_with_row_kind(changes.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    assert!(scan.plan().await.unwrap().is_none());
}

#[tokio::test]
async fn lookup_compaction_validates_changelog_flags_before_execution() {
    for key in [
        "changelog-producer.row-deduplicate",
        "changelog-producer.ignore-update-before",
        "changelog-producer.ignore-delete",
    ] {
        let table = lookup_table(&[(key, "invalid")]).await;
        assert!(table
            .new_compact_builder()
            .new_planner()
            .err()
            .unwrap()
            .to_string()
            .contains(key));
    }
}

#[tokio::test]
async fn lookup_compaction_rejects_stale_dvs_then_emits_insert_for_the_deleted_history_key() {
    let table = lookup_table(&[
        ("deletion-vectors.enabled", "true"),
        ("merge-engine", "aggregation"),
        ("fields.value.aggregate-function", "sum"),
    ])
    .await;
    write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
    let first = compact(&table).await;
    write_batch(&table, &make_batch(vec![1], vec![5])).await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let results = execute(&table, &plan).await;
    let history = &plan.tasks()[0]
        .files()
        .iter()
        .find(|file| file.level > 0)
        .unwrap()
        .file_name;
    super::deletion_vector_tests::delete_positions(&table, history, &[0]).await;
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
        .unwrap_err()
        .to_string()
        .contains("deletion vectors changed"));
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        snapshot
    );
    let end = compact(&table).await;
    assert_eq!(events(&table, first, end).await, expected(&[("+I", 1, 5)]));
    assert_eq!(rows(&table).await, vec![(1, 5), (2, 20)]);
}

#[tokio::test]
async fn lookup_compaction_preserves_before_after_sequence_metadata_and_pair_order() {
    let table = lookup_table(&[]).await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    let first = compact(&table).await;
    write_batch(&table, &make_batch(vec![1], vec![11])).await;
    let second = compact(&table).await;
    let mut read = table.new_read_builder();
    read.with_projection(&["id", "_SEQUENCE_NUMBER", "value"])
        .unwrap();
    let plan = read
        .new_incremental_scan(IncrementalScanMode::Changelog, first, second)
        .plan_combined()
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = read
        .new_read()
        .unwrap()
        .to_arrow_with_row_kind(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut metadata = Vec::new();
    for batch in batches {
        let kinds = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let seqs = batch
            .column_by_name("_SEQUENCE_NUMBER")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            metadata.push((kinds.value(row).to_string(), seqs.value(row)));
        }
    }
    assert_eq!(metadata, vec![("-U".into(), 0), ("+U".into(), 1)]);
}
