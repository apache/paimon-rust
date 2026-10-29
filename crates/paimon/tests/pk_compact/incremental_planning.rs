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

async fn compact_once(table: &Table) -> Vec<CompactResult> {
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let results = execute(table, &plan).await;
    builder
        .new_commit()
        .unwrap()
        .commit_compact(results.clone())
        .await
        .unwrap();
    results
}

async fn active_files(table: &Table) -> Vec<paimon::spec::DataFileMeta> {
    let scan = table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan()
        .await
        .unwrap();
    scan.splits()
        .iter()
        .flat_map(|split| split.data_files())
        .cloned()
        .collect()
}

#[tokio::test]
async fn incremental_deduplicate_keeps_large_history_and_preserves_rows_and_snapshots() {
    for producer in ["none", "input"] {
        let table = table(&[("bucket", "1"), ("changelog-producer", producer)]).await;
        let original = (0..1000).map(|key| (key, key)).collect::<Vec<_>>();
        write_batch(
            &table,
            &make_batch((0..1000).collect(), (0..1000).collect()),
        )
        .await;
        let first = compact_once(&table).await;
        let anchor = first[0].files()[0].clone();
        assert_eq!(anchor.level, 5);
        let snapshot = table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        let historical = table.copy_with_pinned_snapshot(Some(&snapshot));
        for key in [42, 43] {
            write_batch(
                &table,
                &make_batch_with_kinds(
                    vec![key, key + 100],
                    vec![10000 + key, key + 100],
                    vec![0, 3],
                ),
            )
            .await;
            let before = rows(&table).await;
            let before_sequences = sequences(&table).await;
            let builder = table.new_compact_builder();
            let plan = builder.new_planner().unwrap().plan().await.unwrap();
            assert_eq!(plan.tasks().len(), 1);
            assert!(plan.tasks()[0]
                .files()
                .iter()
                .all(|file| file.file_name != anchor.file_name));
            let results = execute(&table, &plan).await;
            assert!(results[0].files().iter().all(|file| file.level == 4));
            builder
                .new_commit()
                .unwrap()
                .commit_compact(results)
                .await
                .unwrap();
            assert!(active_files(&table).await.contains(&anchor));
            assert_eq!(rows(&table).await, before);
            assert_eq!(sequences(&table).await, before_sequences);
            assert_eq!(rows(&historical).await, original);
            assert!(builder
                .new_planner()
                .unwrap()
                .plan()
                .await
                .unwrap()
                .tasks()
                .is_empty());
        }
        let actual = rows(&table).await;
        assert_eq!(actual.len(), 998);
        assert!(actual.contains(&(42, 10042)) && actual.contains(&(43, 10043)));
        assert!(!actual.iter().any(|(key, _)| [142, 143].contains(key)));
    }
}

fn sequenced_batch(ids: Vec<i32>, values: Vec<i32>, seq: i32, kind: i8) -> RecordBatch {
    let count = ids.len();
    RecordBatch::try_from_iter([
        (
            "id",
            std::sync::Arc::new(Int32Array::from(ids)) as arrow_array::ArrayRef,
        ),
        (
            "value",
            std::sync::Arc::new(Int32Array::from(values)) as arrow_array::ArrayRef,
        ),
        (
            "seq",
            std::sync::Arc::new(Int32Array::from(vec![seq; count])) as arrow_array::ArrayRef,
        ),
        (
            "_VALUE_KIND",
            std::sync::Arc::new(arrow_array::Int8Array::from(vec![kind; count]))
                as arrow_array::ArrayRef,
        ),
    ])
    .unwrap()
}

#[tokio::test]
async fn incremental_deduplicate_keeps_user_sequence_winners_and_tombstones() {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .column("seq", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("sequence.field", "seq")
        .build()
        .unwrap();
    let table = table_with_schema(TableSchema::new(0, &schema)).await;
    write_batch(
        &table,
        &sequenced_batch((0..1000).collect(), (0..1000).collect(), 100, 0),
    )
    .await;
    let anchor = compact_once(&table).await[0].files()[0].clone();
    for (sequence, kind, expected) in [
        (50, 0, Some(42)),
        (150, 0, Some(150)),
        (200, 3, None),
        (180, 0, None),
        (300, 0, Some(300)),
    ] {
        write_batch(
            &table,
            &sequenced_batch(vec![42], vec![sequence], sequence, kind),
        )
        .await;
        let before = rows(&table).await;
        let results = compact_once(&table).await;
        assert!(!results.is_empty());
        assert!(results
            .iter()
            .flat_map(|result| result.files())
            .all(|file| file.level == 4));
        assert_eq!(rows(&table).await, before);
        assert_eq!(
            before
                .iter()
                .find(|(key, _)| *key == 42)
                .map(|(_, value)| *value),
            expected
        );
        assert!(active_files(&table).await.contains(&anchor));
    }
}

#[tokio::test]
async fn incremental_selection_is_validated_for_execution_and_commit() {
    let table = table(&[("bucket", "1")]).await;
    write_batch(
        &table,
        &make_batch((0..1000).collect(), (0..1000).collect()),
    )
    .await;
    let anchor = compact_once(&table).await[0].files()[0].clone();
    write_batch(&table, &make_batch(vec![42], vec![4200])).await;
    write_batch(&table, &make_batch(vec![43], vec![4300])).await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let captured = plan.snapshot_id();
    let results = execute(&table, &plan).await;
    for mutation in [
        "omit-input",
        "include-history",
        "output-level",
        "missing-selection",
    ] {
        let mutate = |task: &mut serde_json::Value| match mutation {
            "omit-input" => {
                task["files"].as_array_mut().unwrap().pop();
            }
            "include-history" => task["files"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::to_value(&anchor).unwrap()),
            "output-level" => task["primary_key"]["output_level"] = 5.into(),
            "missing-selection" => {
                task.as_object_mut().unwrap().remove("primary_key");
            }
            _ => unreachable!(),
        };
        let mut encoded = serde_json::to_value(&plan.tasks()[0]).unwrap();
        mutate(&mut encoded);
        let task: CompactTask = serde_json::from_value(encoded).unwrap();
        assert!(
            builder
                .new_executor()
                .unwrap()
                .execute(&task)
                .await
                .is_err(),
            "execute {mutation}"
        );
        let mut encoded = serde_json::to_value(&results).unwrap();
        mutate(&mut encoded[0]["task"]);
        if mutation == "output-level" {
            for file in encoded[0]["files"].as_array_mut().unwrap() {
                file["level"] = 5.into();
            }
        }
        let changed: Vec<CompactResult> = serde_json::from_value(encoded).unwrap();
        assert!(
            builder
                .new_commit()
                .unwrap()
                .commit_compact(changed)
                .await
                .is_err(),
            "commit {mutation}"
        );
        assert_eq!(
            table
                .snapshot_manager()
                .get_latest_snapshot_id()
                .await
                .unwrap(),
            captured
        );
    }
    builder
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap();
    assert_eq!(
        rows(&table).await.iter().find(|(key, _)| *key == 42),
        Some(&(42, 4200))
    );
    assert!(active_files(&table).await.contains(&anchor));
}

#[tokio::test]
async fn incremental_compaction_preserves_concurrent_l0_appends_and_retry_identity() {
    let table = table(&[("bucket", "1")]).await;
    write_batch(
        &table,
        &make_batch((0..1000).collect(), (0..1000).collect()),
    )
    .await;
    let anchor = compact_once(&table).await[0].files()[0].clone();
    write_batch(&table, &make_batch(vec![42], vec![4200])).await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let results = execute(&table, &plan).await;
    write_batch(&table, &make_batch(vec![42], vec![4201])).await;
    let committer = builder.new_commit().unwrap();
    committer.commit_compact(results.clone()).await.unwrap();
    let snapshot = table
        .snapshot_manager()
        .get_latest_snapshot_id()
        .await
        .unwrap();
    committer.commit_compact(results).await.unwrap();
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        snapshot
    );
    assert_eq!(
        rows(&table).await.iter().find(|(key, _)| *key == 42),
        Some(&(42, 4201))
    );
    assert!(active_files(&table).await.contains(&anchor));
    compact_once(&table).await;
    assert_eq!(
        rows(&table).await.iter().find(|(key, _)| *key == 42),
        Some(&(42, 4201))
    );
}

#[tokio::test]
async fn order_sensitive_aggregation_keeps_its_history_in_the_merge() {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .column("seq", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("sequence.field", "seq")
        .option("merge-engine", "aggregation")
        .option("fields.value.aggregate-function", "first_value")
        .build()
        .unwrap();
    let table = table_with_schema(TableSchema::new(0, &schema)).await;
    write_batch(
        &table,
        &sequenced_batch((0..1000).collect(), vec![20; 1000], 20, 0),
    )
    .await;
    let anchor = compact_once(&table).await[0].files()[0].clone();
    write_batch(&table, &sequenced_batch(vec![42], vec![10], 10, 0)).await;
    write_batch(&table, &sequenced_batch(vec![42], vec![30], 30, 0)).await;
    let before = rows(&table).await;
    assert_eq!(before.iter().find(|(key, _)| *key == 42), Some(&(42, 10)));
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    assert!(plan.tasks()[0].files().contains(&anchor));
    let results = execute(&table, &plan).await;
    builder
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap();
    assert_eq!(rows(&table).await, before);
}

#[tokio::test]
async fn legacy_completed_results_recover_but_uncommitted_tasks_require_replanning() {
    let table = table(&[("bucket", "1")]).await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    compact_once(&table).await;
    write_batch(
        &table,
        &make_batch((0..1000).collect(), (0..1000).collect()),
    )
    .await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    assert_eq!(plan.tasks()[0].files().len(), 2);
    let results = execute(&table, &plan).await;
    assert!(results[0].files().iter().all(|file| file.level == 5));
    // Older serialized tasks have no selection metadata. Their full-bucket
    // outputs are recoverable once committed, but must never execute anew.
    let mut legacy = serde_json::to_value(&results).unwrap();
    legacy[0]["task"]
        .as_object_mut()
        .unwrap()
        .remove("primary_key");
    let legacy: Vec<CompactResult> = serde_json::from_value(legacy).unwrap();
    let committer = builder.new_commit().unwrap();
    assert!(committer.commit_compact(legacy.clone()).await.is_err());
    committer.commit_compact(results).await.unwrap();
    let snapshot = table
        .snapshot_manager()
        .get_latest_snapshot_id()
        .await
        .unwrap();
    committer.commit_compact(legacy).await.unwrap();
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        snapshot
    );
    assert_eq!(
        rows(&table).await,
        (0..1000).map(|key| (key, key)).collect::<Vec<_>>()
    );
}
