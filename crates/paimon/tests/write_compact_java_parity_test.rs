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

mod common;

use std::collections::HashMap;

use arrow_array::{Int32Array, RecordBatch};
use common::incremental_helpers::{
    make_batch, make_batch_with_kinds, make_partitioned_batch, persist_table_schema, setup_dirs,
    write_batch,
};
use futures::TryStreamExt;
use paimon::catalog::Identifier;
use paimon::io::FileIOBuilder;
use paimon::spec::{CommitKind, DataType, IntType, Schema, Snapshot, TableSchema, VarCharType};
use paimon::table::{CommitMessage, Table};

async fn table(partitioned: bool, bucket: &str, options: &[(&str, &str)]) -> Table {
    table_with_cache(partitioned, bucket, options, false).await
}

async fn table_with_cache(
    partitioned: bool,
    bucket: &str,
    options: &[(&str, &str)],
    cache: bool,
) -> Table {
    let mut schema = Schema::builder();
    if partitioned {
        schema = schema.column("pt", DataType::VarChar(VarCharType::string_type()));
    }
    schema = schema
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .option("bucket", bucket);
    schema = if partitioned {
        schema.partition_keys(["pt"]).primary_key(["pt", "id"])
    } else {
        schema.primary_key(["id"])
    };
    for (key, value) in options {
        schema = schema.option(*key, *value);
    }
    let path = format!("memory:/write_compact_java/{}", uuid::Uuid::new_v4());
    let io = if cache {
        let mut cache_options = paimon::Options::new();
        cache_options.set("warehouse", &path);
        cache_options.set("local-cache.enabled", "true");
        paimon::catalog::FileSystemCatalog::new(cache_options)
            .unwrap()
            .file_io()
            .clone()
    } else {
        FileIOBuilder::new("memory").build().unwrap()
    };
    let table = Table::new(
        io.clone(),
        Identifier::new("default", "write_compact_java"),
        path.clone(),
        TableSchema::new(0, &schema.build().unwrap()),
        None,
    );
    setup_dirs(&io, &path).await;
    persist_table_schema(&io, &path, table.schema()).await;
    table
}

async fn latest(table: &Table) -> Snapshot {
    table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap()
}

async fn stage(table: &Table, batch: RecordBatch, bucket: Option<i32>) -> Vec<CommitMessage> {
    let mut writer = table.new_write_builder().new_write().unwrap();
    writer.write_arrow(&[batch], bucket).await.unwrap();
    writer.prepare_commit().await.unwrap()
}

async fn group_files(table: &Table, partition: &[u8], bucket: i32) -> Vec<(String, i32)> {
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let mut files = plan
        .splits()
        .iter()
        .filter(|split| {
            split.partition().to_serialized_bytes() == partition && split.bucket() == bucket
        })
        .flat_map(|split| split.data_files())
        .map(|file| (file.file_name.clone(), file.level))
        .collect::<Vec<_>>();
    files.sort_unstable();
    files
}

async fn rows(table: &Table) -> Vec<(i32, i32)> {
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
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        rows.extend((0..batch.num_rows()).map(|index| (ids.value(index), values.value(index))));
    }
    rows.sort_unstable();
    rows
}

#[tokio::test]
async fn default_primary_key_auto_compaction_waits_for_java_run_trigger() {
    // Java uses UniversalCompaction by default. Batch prepare waits for work
    // already selected; waiting does not enable ForceUpLevel0Compaction.
    for bucket in ["1", "-1"] {
        for options in [vec![], vec![("compaction.force-up-level-0", "false")]] {
            let table = table(false, bucket, &options).await;
            write_batch(&table, &make_batch(vec![1], vec![10])).await;
            let snapshot = latest(&table).await;
            assert_eq!(snapshot.id(), 1);
            assert_eq!(snapshot.commit_kind(), &CommitKind::APPEND);
            let plan = table.new_read_builder().new_scan().plan().await.unwrap();
            let files = plan
                .splits()
                .iter()
                .flat_map(|split| split.data_files())
                .collect::<Vec<_>>();
            assert_eq!(files.len(), 1);
            assert_eq!(files[0].level, 0);
            assert_eq!(rows(&table).await, vec![(1, 10)]);
        }
    }
}

#[tokio::test]
async fn primary_key_auto_compaction_honors_configured_java_run_trigger() {
    for bucket in ["1", "-1"] {
        let table = table(false, bucket, &[("num-sorted-run.compaction-trigger", "2")]).await;
        write_batch(&table, &make_batch(vec![1], vec![10])).await;
        assert_eq!(latest(&table).await.commit_kind(), &CommitKind::APPEND);
        write_batch(&table, &make_batch(vec![2], vec![20])).await;
        let snapshot = latest(&table).await;
        assert_eq!(snapshot.id(), 3);
        assert_eq!(snapshot.commit_kind(), &CommitKind::COMPACT);
        let plan = table.new_read_builder().new_scan().plan().await.unwrap();
        assert!(plan
            .splits()
            .iter()
            .flat_map(|split| split.data_files())
            .all(|file| file.level > 0));
        assert_eq!(rows(&table).await, vec![(1, 10), (2, 20)]);
    }
}

#[tokio::test]
async fn explicit_force_up_level_zero_auto_compacts_a_singleton() {
    for bucket in ["1", "-1"] {
        let table = table(false, bucket, &[("compaction.force-up-level-0", "true")]).await;
        write_batch(&table, &make_batch(vec![1], vec![10])).await;
        let snapshot = latest(&table).await;
        assert_eq!(snapshot.id(), 2);
        assert_eq!(snapshot.commit_kind(), &CommitKind::COMPACT);
        let plan = table.new_read_builder().new_scan().plan().await.unwrap();
        assert!(plan
            .splits()
            .iter()
            .flat_map(|split| split.data_files())
            .all(|file| file.level > 0));
        assert_eq!(rows(&table).await, vec![(1, 10)]);
    }
}

#[tokio::test]
async fn first_row_automatic_compaction_does_not_require_catalog_cache_opt_in() {
    let table = table(false, "1", &[("merge-engine", "first-row")]).await;
    for value in [10, 20] {
        write_batch(&table, &make_batch(vec![1], vec![value])).await;
        assert_eq!(latest(&table).await.commit_kind(), &CommitKind::COMPACT);
        assert_eq!(rows(&table).await, vec![(1, 10)]);
    }
}

#[tokio::test]
async fn deletion_vector_automatic_compaction_does_not_require_catalog_cache_opt_in() {
    let table = table(false, "1", &[("deletion-vectors.enabled", "true")]).await;
    for value in [10, 20] {
        write_batch(&table, &make_batch(vec![1], vec![value])).await;
        assert_eq!(latest(&table).await.commit_kind(), &CommitKind::COMPACT);
        assert_eq!(rows(&table).await, vec![(1, value)]);
    }
}

#[tokio::test]
async fn lookup_automatic_compaction_does_not_require_catalog_cache_opt_in() {
    for options in [
        vec![("changelog-producer", "lookup")],
        vec![
            ("changelog-producer", "lookup"),
            ("lookup.cache-max-disk-size", "4mb"),
        ],
    ] {
        let table = table(false, "1", &options).await;
        write_batch(&table, &make_batch(vec![1], vec![10])).await;
        assert_eq!(latest(&table).await.commit_kind(), &CommitKind::COMPACT);
        assert_eq!(latest(&table).await.changelog_record_count(), Some(1));
        write_batch(&table, &make_batch(vec![1], vec![20])).await;
        assert_eq!(latest(&table).await.commit_kind(), &CommitKind::COMPACT);
        assert_eq!(latest(&table).await.changelog_record_count(), Some(2));
        assert_eq!(rows(&table).await, vec![(1, 20)]);
    }
}

#[tokio::test]
async fn overwrite_lookup_compaction_suppresses_changelog_without_affecting_later_appends() {
    // A cache is explicitly available here so the overwrite regression is
    // independent from the automatic executor's default-cache behavior.
    let table = table_with_cache(false, "1", &[("changelog-producer", "lookup")], true).await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    assert_eq!(latest(&table).await.changelog_record_count(), Some(1));

    let overwrite = table.new_write_builder().with_overwrite();
    let mut writer = overwrite.new_write().unwrap();
    writer
        .write_arrow_batch(&make_batch(vec![1], vec![20]))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    overwrite
        .new_commit()
        .overwrite(messages, None)
        .await
        .unwrap();
    let snapshot = latest(&table).await;
    assert_eq!(snapshot.commit_kind(), &CommitKind::COMPACT);
    assert_eq!(snapshot.changelog_record_count(), None);
    assert_eq!(rows(&table).await, vec![(1, 20)]);

    write_batch(&table, &make_batch(vec![1], vec![30])).await;
    assert_eq!(latest(&table).await.changelog_record_count(), Some(2));
    assert_eq!(rows(&table).await, vec![(1, 30)]);
}

#[tokio::test]
async fn bounded_bucket_compaction_preserves_lookup_changes_and_deletion_vectors() {
    for parallelism in ["1", "2"] {
        for dv in ["false", "true"] {
            let table = table(
                false,
                "4",
                &[
                    ("changelog-producer", "lookup"),
                    ("deletion-vectors.enabled", dv),
                    ("write.compaction.parallelism", parallelism),
                ],
            )
            .await;
            let ids = (0..32).collect::<Vec<_>>();
            for version in [10, 20] {
                write_batch(&table, &make_batch(ids.clone(), vec![version; ids.len()])).await;
                let snapshot = latest(&table).await;
                assert_eq!(snapshot.commit_kind(), &CommitKind::COMPACT);
                assert_eq!(
                    snapshot.changelog_record_count(),
                    Some(if version == 10 { 32 } else { 64 })
                );
                assert_eq!(
                    rows(&table).await,
                    ids.iter().map(|id| (*id, version)).collect::<Vec<_>>()
                );
                let plan = table.new_read_builder().new_scan().plan().await.unwrap();
                assert_eq!(
                    plan.splits()
                        .iter()
                        .map(|split| split.bucket())
                        .collect::<std::collections::HashSet<_>>()
                        .len(),
                    4
                );
                assert!(plan
                    .splits()
                    .iter()
                    .flat_map(|split| split.data_files())
                    .all(|file| file.level > 0));
            }
        }
    }
}

#[tokio::test]
async fn automatic_gentle_lookup_rejects_before_publication_without_changing_manual_compaction() {
    for lookup_mode in [
        ("merge-engine", "first-row"),
        ("deletion-vectors.enabled", "true"),
        ("changelog-producer", "lookup"),
    ] {
        let table = table_with_cache(
            false,
            "1",
            &[lookup_mode, ("lookup-compact", "gentle")],
            true,
        )
        .await;
        let messages = stage(&table, make_batch(vec![1], vec![10]), None).await;
        let error = table
            .new_write_builder()
            .new_commit()
            .commit(messages.clone())
            .await
            .unwrap_err();
        assert!(
            matches!(error, paimon::Error::Unsupported { .. }),
            "{error}"
        );
        assert!(error.to_string().contains("lookup-compact"), "{error}");
        assert!(
            error.to_string().to_ascii_lowercase().contains("gentle"),
            "{error}"
        );
        assert_eq!(
            table
                .snapshot_manager()
                .get_latest_snapshot_id()
                .await
                .unwrap(),
            None
        );

        let write_only =
            table.copy_with_options(HashMap::from([("write-only".into(), "true".into())]));
        write_only
            .new_write_builder()
            .new_commit()
            .commit(messages)
            .await
            .unwrap();
        assert_eq!(latest(&table).await.id(), 1);
        assert_eq!(latest(&table).await.commit_kind(), &CommitKind::APPEND);

        // The explicit batch compaction API retains its established forced-L0
        // behavior; only automatic use of unsupported interval policy fails.
        let compact = table.new_compact_builder();
        let plan = compact.new_planner().unwrap().plan().await.unwrap();
        assert_eq!(plan.tasks().len(), 1);
        let executor = compact.new_executor().unwrap();
        let result = executor.execute(&plan.tasks()[0]).await.unwrap();
        compact
            .new_commit()
            .unwrap()
            .commit_compact(vec![result])
            .await
            .unwrap();
        assert_eq!(latest(&table).await.commit_kind(), &CommitKind::COMPACT);
        assert_eq!(rows(&table).await, vec![(1, 10)]);
    }
}

#[tokio::test]
async fn automatic_lookup_rejects_unknown_policy_only_when_lookup_is_required() {
    for lookup_mode in [
        ("merge-engine", "first-row"),
        ("deletion-vectors.enabled", "true"),
        ("changelog-producer", "lookup"),
    ] {
        let table = table(false, "1", &[lookup_mode, ("lookup-compact", "unknown")]).await;
        let messages = stage(&table, make_batch(vec![1], vec![10]), None).await;
        let error = table
            .new_write_builder()
            .new_commit()
            .commit(messages)
            .await
            .unwrap_err();
        assert!(
            matches!(error, paimon::Error::ConfigInvalid { .. }),
            "{error}"
        );
        assert!(error.to_string().contains("lookup-compact"), "{error}");
        assert_eq!(
            table
                .snapshot_manager()
                .get_latest_snapshot_id()
                .await
                .unwrap(),
            None
        );
    }
    let table = table(false, "1", &[("lookup-compact", "unknown")]).await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    assert_eq!(latest(&table).await.commit_kind(), &CommitKind::APPEND);
    assert_eq!(rows(&table).await, vec![(1, 10)]);
}

#[tokio::test]
async fn unrepresentable_plain_retract_state_preserves_data_without_automatic_compaction() {
    for (engine, aggregate, delete_option, before_value, later_value) in [
        (
            "aggregation",
            "first_value",
            "aggregation.remove-record-on-delete",
            10,
            20,
        ),
        (
            "partial-update",
            "last_non_null_value",
            "partial-update.remove-record-on-delete",
            13,
            30,
        ),
    ] {
        let table = table(
            false,
            "1",
            &[
                ("merge-engine", engine),
                ("fields.value.aggregate-function", aggregate),
                (delete_option, "true"),
            ],
        )
        .await;
        let write_only =
            table.copy_with_options(HashMap::from([("write-only".into(), "true".into())]));
        let mut partition = Vec::new();
        for value in 10..14 {
            let messages = stage(&write_only, make_batch(vec![1], vec![value]), None).await;
            partition = messages[0].partition.clone();
            write_only
                .new_write_builder()
                .new_commit()
                .commit(messages)
                .await
                .unwrap();
        }
        assert_eq!(rows(&table).await, vec![(1, before_value)]);
        let mut expected = group_files(&table, &partition, 0).await;
        assert_eq!(expected.len(), 4);

        // This fifth run reaches Java's normal trigger, but the current Rust
        // reducer deliberately refuses to encode its retract accumulator.
        let messages = stage(
            &table,
            make_batch_with_kinds(vec![1], vec![20], vec![3]),
            None,
        )
        .await;
        expected.extend(
            messages
                .iter()
                .flat_map(|message| &message.new_files)
                .map(|file| (file.file_name.clone(), file.level)),
        );
        expected.sort_unstable();
        table
            .new_write_builder()
            .new_commit()
            .commit(messages)
            .await
            .unwrap();
        assert_eq!(latest(&table).await.id(), 5);
        assert_eq!(latest(&table).await.commit_kind(), &CommitKind::APPEND);
        assert_eq!(group_files(&table, &partition, 0).await, expected);
        assert!(rows(&table).await.is_empty());

        let compact = table.new_compact_builder();
        let plan = compact.new_planner().unwrap().plan().await.unwrap();
        assert_eq!(plan.tasks().len(), 1);
        let error = compact
            .new_executor()
            .unwrap()
            .execute(&plan.tasks()[0])
            .await
            .unwrap_err();
        assert!(
            matches!(error, paimon::Error::Unsupported { .. }),
            "{error}"
        );
        assert!(error.to_string().contains("aggregate state"), "{error}");
        assert_eq!(group_files(&table, &partition, 0).await, expected);

        write_batch(&table, &make_batch(vec![1], vec![30])).await;
        // The next INSERT can make the state representable again. A healthy
        // automatic COMPACT is allowed once the retained accumulator is safe.
        let snapshot = latest(&table).await;
        let data_snapshot_id = if snapshot.commit_kind() == &CommitKind::COMPACT {
            snapshot.id() - 1
        } else {
            snapshot.id()
        };
        assert_eq!(data_snapshot_id, 6);
        assert_eq!(rows(&table).await, vec![(1, later_value)]);
    }
}

#[tokio::test]
async fn automatic_compaction_does_not_rewrite_an_untouched_partition() {
    let table = table(true, "1", &[("num-sorted-run.compaction-trigger", "2")]).await;
    let write_only = table.copy_with_options(HashMap::from([("write-only".into(), "true".into())]));
    let mut untouched_partition = Vec::new();
    for id in [1, 2] {
        let messages = stage(
            &write_only,
            make_partitioned_batch(vec!["a"], vec![id], vec![id * 10]),
            None,
        )
        .await;
        untouched_partition = messages[0].partition.clone();
        write_only
            .new_write_builder()
            .new_commit()
            .commit(messages)
            .await
            .unwrap();
    }
    let before = group_files(&table, &untouched_partition, 0).await;
    assert_eq!(before.len(), 2);
    assert!(before.iter().all(|(_, level)| *level == 0));

    let mut messages = Vec::new();
    for id in [10, 20] {
        messages.extend(
            stage(
                &table,
                make_partitioned_batch(vec!["b"], vec![id], vec![id * 10]),
                None,
            )
            .await,
        );
    }
    let touched_partition = messages[0].partition.clone();
    table
        .new_write_builder()
        .new_commit()
        .commit(messages)
        .await
        .unwrap();
    assert_eq!(latest(&table).await.commit_kind(), &CommitKind::COMPACT);
    assert_eq!(group_files(&table, &untouched_partition, 0).await, before);
    let after = group_files(&table, &touched_partition, 0).await;
    assert_eq!(after.len(), 1);
    assert!(after.iter().all(|(_, level)| *level > 0));
    assert_eq!(
        rows(&table).await,
        vec![(1, 10), (2, 20), (10, 100), (20, 200)]
    );
}

#[tokio::test]
async fn automatic_compaction_does_not_rewrite_an_untouched_bucket() {
    let table = table(false, "2", &[("num-sorted-run.compaction-trigger", "2")]).await;
    let write_only = table.copy_with_options(HashMap::from([("write-only".into(), "true".into())]));
    let mut partition = Vec::new();
    for id in [1, 2] {
        let messages = stage(&write_only, make_batch(vec![id], vec![id * 10]), Some(0)).await;
        partition = messages[0].partition.clone();
        write_only
            .new_write_builder()
            .new_commit()
            .commit(messages)
            .await
            .unwrap();
    }
    let before = group_files(&table, &partition, 0).await;
    assert_eq!(before.len(), 2);
    assert!(before.iter().all(|(_, level)| *level == 0));

    let mut messages = Vec::new();
    for id in [10, 20] {
        messages.extend(stage(&table, make_batch(vec![id], vec![id * 10]), Some(1)).await);
    }
    table
        .new_write_builder()
        .new_commit()
        .commit(messages)
        .await
        .unwrap();
    assert_eq!(latest(&table).await.commit_kind(), &CommitKind::COMPACT);
    assert_eq!(group_files(&table, &partition, 0).await, before);
    let after = group_files(&table, &partition, 1).await;
    assert_eq!(after.len(), 1);
    assert!(after.iter().all(|(_, level)| *level > 0));
    assert_eq!(
        rows(&table).await,
        vec![(1, 10), (2, 20), (10, 100), (20, 200)]
    );
}
