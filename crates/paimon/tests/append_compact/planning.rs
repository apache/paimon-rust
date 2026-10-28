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
