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

#[tokio::test]
async fn compaction_recovery_does_not_read_snapshots_older_than_its_plan() {
    let table = table(schema(&[])).await;
    append_files(&table, 3).await;
    let builder = table
        .new_compact_builder()
        .with_commit_user("bounded-recovery")
        .unwrap();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let results = execute(&table, &plan).await;
    // Old snapshot contents are irrelevant to a fresh plan's identity. Fail
    // reads deterministically instead of relying on remote request timings.
    table
        .file_io()
        .new_output(&format!("{}/snapshot/snapshot-1", table.location()))
        .unwrap()
        .write(bytes::Bytes::from_static(b"unreadable old snapshot"))
        .await
        .unwrap();
    let commit = builder.new_commit().unwrap();
    commit.commit_compact(results.clone()).await.unwrap();
    append_files(&table, 1).await;
    commit.commit_compact(results).await.unwrap();
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        Some(5)
    );
    assert_eq!(rows(&table, None).await.len(), 8);
}

#[tokio::test]
async fn rollback_invalidates_a_prepared_compaction_plan() {
    let table = table(schema(&[])).await;
    append_files(&table, 2).await;
    table
        .new_write_builder()
        .new_commit()
        .with_ignore_empty_commit(false)
        .commit(vec![])
        .await
        .unwrap();
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    assert_eq!(plan.snapshot_id(), Some(3));
    let results = execute(&table, &plan).await;
    // Equivalent to Java rollback: lower LATEST and remove later snapshots.
    table
        .file_io()
        .delete_file(&table.snapshot_manager().snapshot_path(3))
        .await
        .unwrap();
    table.snapshot_manager().write_latest_hint(2).await.unwrap();
    let error = builder
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("rolled back"), "{error}");
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        Some(2)
    );
    assert_eq!(rows(&table, None).await.len(), 4);
}

#[tokio::test]
async fn append_plan_waits_for_the_default_minimum_and_has_no_file_count_cap() {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .option("bucket", "-1")
        .build()
        .unwrap();
    let table = table(TableSchema::new(0, &schema)).await;
    append_files(&table, 4).await;
    let planner = table.new_compact_builder().new_planner().unwrap();
    assert!(planner.plan().await.unwrap().tasks().is_empty());
    append_files(&table, 1).await;
    let plan = planner.plan().await.unwrap();
    assert_eq!(plan.tasks().len(), 1);
    assert_eq!(plan.tasks()[0].files().len(), 5);
    append_files(&table, 35).await;
    let plan = planner.plan().await.unwrap();
    assert_eq!(plan.tasks().len(), 1);
    assert_eq!(plan.tasks()[0].files().len(), 40);
}

#[tokio::test]
async fn append_compaction_groups_are_independent_of_read_split_sizes() {
    let table = table(schema(&[
        ("target-file-size", "8kb"),
        ("source.split.open-file-cost", "8kb"),
        ("source.split.target-size", "1b"),
    ]))
    .await;
    append_files(&table, 5).await;
    let read_plan = table.new_read_builder().new_scan().plan().await.unwrap();
    assert_eq!(read_plan.splits().len(), 5);
    let plan = table
        .new_compact_builder()
        .new_planner()
        .unwrap()
        .plan()
        .await
        .unwrap();
    assert_eq!(plan.tasks().len(), 2);
    assert!(plan.tasks().iter().all(|task| task.files().len() == 2));
    let larger_splits = table.copy_with_options(HashMap::from([(
        "source.split.target-size".into(),
        "1mb".into(),
    )]));
    let read_plan = larger_splits
        .new_read_builder()
        .new_scan()
        .plan()
        .await
        .unwrap();
    assert_eq!(read_plan.splits().len(), 1);
    let larger_plan = larger_splits
        .new_compact_builder()
        .new_planner()
        .unwrap()
        .plan()
        .await
        .unwrap();
    let groups = |plan: &CompactPlan| {
        plan.tasks()
            .iter()
            .map(|task| {
                task.files()
                    .iter()
                    .map(|file| file.file_name.clone())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(groups(&plan), groups(&larger_plan));
    let result = execute(&table, &plan).await;
    table
        .new_compact_builder()
        .new_commit()
        .unwrap()
        .commit_compact(result)
        .await
        .unwrap();
    assert_eq!(rows(&table, None).await.len(), 10);
}
