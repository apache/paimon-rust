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

use super::{
    MergeAssignment, MergeCondition, MergeSource, Table, TableCommit, UpdateAssignment,
    WhenMatched, WhenNotMatched,
};
use crate::catalog::Identifier;
use crate::io::FileIOBuilder;
use crate::spec::{DataType, IntType, Schema, TableSchema};
use arrow_array::{Array, ArrayRef, BooleanArray, Int32Array, RecordBatch};
use futures::TryStreamExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn table() -> Table {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .option("data-evolution.enabled", "true")
        .option("row-tracking.enabled", "true")
        .option("deletion-vectors.enabled", "true")
        .build()
        .unwrap();
    Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("db", "t"),
        "memory:/merge".into(),
        TableSchema::new(0, &schema),
        None,
    )
}

fn batch(ids: Vec<Option<i32>>, values: Vec<Option<i32>>) -> RecordBatch {
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(ids)) as ArrayRef),
        ("value", Arc::new(Int32Array::from(values)) as ArrayRef),
    ])
    .unwrap()
}

async fn seed(table: &Table, data: &RecordBatch) {
    let mut writer = table
        .new_write_builder()
        .with_commit_user("merge")
        .unwrap()
        .new_write()
        .unwrap();
    writer.write_arrow_batch(data).await.unwrap();
    TableCommit::new(table.clone(), "merge".into())
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();
    writer.close().await;
}

async fn rows(table: &Table) -> Vec<(Option<i32>, Option<i32>)> {
    let builder = table.new_read_builder();
    let plan = builder.new_scan().plan().await.unwrap();
    let mut stream = builder.new_read().unwrap().to_arrow(plan.splits()).unwrap();
    let mut values = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let data = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        values.extend(ids.iter().zip(data.iter()));
    }
    values.sort();
    values
}

fn update(condition: Option<MergeCondition>) -> WhenMatched {
    WhenMatched {
        condition,
        delete: false,
        assignments: vec![(
            "value".into(),
            MergeAssignment::SourceColumn("value".into()),
        )],
    }
}

#[tokio::test]
async fn core_merge_preserves_row_ids_and_handles_sql_null_keys() {
    let table = table();
    seed(
        &table,
        &batch(
            vec![Some(1), Some(2), None],
            vec![Some(10), Some(20), Some(90)],
        ),
    )
    .await;
    let source = batch(
        vec![Some(1), Some(3), None],
        vec![Some(11), Some(30), Some(91)],
    );
    let updater = table
        .new_write_builder()
        .with_commit_user("merge")
        .unwrap()
        .new_update()
        .unwrap();
    let messages = updater
        .merge_into(
            MergeSource::Batches(vec![source]),
            vec![("id".into(), "id".into())],
            vec![update(None)],
            vec![WhenNotMatched {
                condition: None,
                assignments: vec![
                    ("id".into(), MergeAssignment::SourceColumn("id".into())),
                    (
                        "value".into(),
                        MergeAssignment::SourceColumn("value".into()),
                    ),
                ],
            }],
        )
        .await
        .unwrap();
    assert!(messages
        .iter()
        .filter(|message| !message.new_files.is_empty())
        .any(|message| message.check_from_snapshot == Some(1)));
    TableCommit::new(table.clone(), "merge".into())
        .commit(messages)
        .await
        .unwrap();
    assert_eq!(
        rows(&table).await,
        vec![
            (None, Some(90)),
            (None, Some(91)),
            (Some(1), Some(11)),
            (Some(2), Some(20)),
            (Some(3), Some(30))
        ]
    );
}

#[tokio::test]
async fn duplicate_source_fails_before_condition_evaluation_and_staging() {
    let table = table();
    seed(&table, &batch(vec![Some(1)], vec![Some(10)])).await;
    let files = table
        .file_io()
        .list_status_recursive(table.location())
        .await
        .unwrap()
        .len();
    let calls = Arc::new(AtomicUsize::new(0));
    let evaluated = calls.clone();
    let condition = MergeCondition {
        target_columns: Vec::new(),
        source_columns: Vec::new(),
        evaluate: Arc::new(move |batch| {
            evaluated.fetch_add(1, Ordering::Relaxed);
            Box::pin(async move { Ok(BooleanArray::from(vec![false; batch.num_rows()])) })
        }),
    };
    let updater = table.new_write_builder().new_update().unwrap();
    let error = updater
        .merge_into(
            MergeSource::Batches(vec![batch(
                vec![Some(1), Some(1)],
                vec![Some(11), Some(12)],
            )]),
            vec![("id".into(), "id".into())],
            vec![update(Some(condition))],
            Vec::new(),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("multiple source rows"));
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert_eq!(
        table
            .file_io()
            .list_status_recursive(table.location())
            .await
            .unwrap()
            .len(),
        files
    );
}

#[tokio::test]
async fn unconditional_delete_collapses_duplicate_source_matches() {
    let table = table();
    seed(
        &table,
        &batch(vec![Some(1), Some(2)], vec![Some(10), Some(20)]),
    )
    .await;
    let updater = table.new_write_builder().new_update().unwrap();
    let messages = updater
        .merge_into(
            MergeSource::Batches(vec![batch(
                vec![Some(1), Some(1)],
                vec![Some(11), Some(12)],
            )]),
            vec![("id".into(), "id".into())],
            vec![WhenMatched {
                condition: None,
                delete: true,
                assignments: Vec::new(),
            }],
            Vec::new(),
        )
        .await
        .unwrap();
    TableCommit::new(table.clone(), "merge".into())
        .commit(messages)
        .await
        .unwrap();
    assert_eq!(rows(&table).await, vec![(Some(2), Some(20))]);
}

#[tokio::test]
async fn self_merge_null_condition_falls_through_to_next_clause() {
    let table = table();
    seed(
        &table,
        &batch(vec![Some(1), Some(2)], vec![Some(10), Some(20)]),
    )
    .await;
    let null_condition = MergeCondition {
        target_columns: Vec::new(),
        source_columns: Vec::new(),
        evaluate: Arc::new(|batch| {
            Box::pin(async move { Ok(BooleanArray::from(vec![None; batch.num_rows()])) })
        }),
    };
    let value = MergeAssignment::Value(UpdateAssignment::Scalar(Arc::new(Int32Array::from(vec![
        77,
    ]))));
    let updater = table.new_write_builder().new_update().unwrap();
    let messages = updater
        .merge_into(
            MergeSource::SelfTable,
            vec![("_ROW_ID".into(), "_ROW_ID".into())],
            vec![
                update(Some(null_condition)),
                WhenMatched {
                    condition: None,
                    delete: false,
                    assignments: vec![("value".into(), value)],
                },
            ],
            Vec::new(),
        )
        .await
        .unwrap();
    TableCommit::new(table.clone(), "merge".into())
        .commit(messages)
        .await
        .unwrap();
    assert_eq!(
        rows(&table).await,
        vec![(Some(1), Some(77)), (Some(2), Some(77))]
    );
}

#[tokio::test]
async fn core_merge_preserves_all_assignment_chunks() {
    let table = table();
    seed(
        &table,
        &batch(vec![Some(1), Some(2)], vec![Some(10), Some(20)]),
    )
    .await;
    let values = MergeAssignment::Value(UpdateAssignment::Array(vec![
        Arc::new(Int32Array::from(vec![100])),
        Arc::new(Int32Array::from(vec![200])),
    ]));
    let messages = table
        .new_write_builder()
        .new_update()
        .unwrap()
        .merge_into(
            MergeSource::Batches(vec![batch(vec![Some(1), Some(2)], vec![Some(0), Some(0)])]),
            vec![("id".into(), "id".into())],
            vec![WhenMatched {
                condition: None,
                delete: false,
                assignments: vec![("value".into(), values)],
            }],
            Vec::new(),
        )
        .await
        .unwrap();
    TableCommit::new(table.clone(), "merge".into())
        .commit(messages)
        .await
        .unwrap();
    assert_eq!(
        rows(&table).await,
        vec![(Some(1), Some(100)), (Some(2), Some(200))]
    );
}

#[tokio::test]
async fn core_merge_rejects_inconsistent_source_schemas_before_staging() {
    let table = table();
    seed(&table, &batch(vec![Some(1)], vec![Some(10)])).await;
    let source = batch(vec![Some(1)], vec![Some(11)]);
    let reversed = source.project(&[1, 0]).unwrap();
    let updater = table.new_write_builder().new_update().unwrap();
    let error = updater
        .merge_into(
            MergeSource::Batches(vec![source, reversed]),
            vec![("id".into(), "id".into())],
            vec![update(None)],
            Vec::new(),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("must share a schema"));
    assert_eq!(rows(&table).await, vec![(Some(1), Some(10))]);
}
