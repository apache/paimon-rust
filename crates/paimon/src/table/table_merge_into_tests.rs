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
    table_at("merge")
}

fn table_at(name: &str) -> Table {
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
        Identifier::new("db", name),
        format!("memory:/{name}"),
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

#[tokio::test]
async fn core_merge_consumes_assignments_across_all_selected_files() {
    for chunked in [false, true] {
        let table = table();
        for id in 1..=3 {
            seed(&table, &batch(vec![Some(id)], vec![Some(id * 10)])).await;
        }
        let values: Vec<ArrayRef> = if chunked {
            vec![
                Arc::new(Int32Array::from(vec![100])),
                Arc::new(Int32Array::from(vec![200])),
            ]
        } else {
            vec![Arc::new(Int32Array::from(vec![100, 200]))]
        };
        let condition = MergeCondition {
            target_columns: Vec::new(),
            source_columns: vec!["id".into()],
            evaluate: Arc::new(|batch| {
                Box::pin(async move {
                    let ids = batch
                        .column_by_name("s.id")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    Ok(BooleanArray::from(
                        ids.iter().map(|id| id.map(|id| id < 3)).collect::<Vec<_>>(),
                    ))
                })
            }),
        };
        let messages = table
            .new_write_builder()
            .new_update()
            .unwrap()
            .merge_into(
                MergeSource::Batches(vec![batch(
                    vec![Some(1), Some(2), Some(3)],
                    vec![Some(11), Some(22), Some(33)],
                )]),
                vec![("id".into(), "id".into())],
                vec![
                    WhenMatched {
                        condition: Some(condition),
                        delete: false,
                        assignments: vec![
                            ("id".into(), MergeAssignment::TargetColumn("id".into())),
                            (
                                "value".into(),
                                MergeAssignment::Value(UpdateAssignment::Array(values)),
                            ),
                        ],
                    },
                    update(None),
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
            vec![
                (Some(1), Some(100)),
                (Some(2), Some(200)),
                (Some(3), Some(33))
            ]
        );
    }
}

#[tokio::test]
async fn core_merge_evaluates_functions_once_for_the_complete_clause() {
    let table = table();
    for id in 1..=2 {
        seed(&table, &batch(vec![Some(id)], vec![Some(id * 10)])).await;
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let evaluated = calls.clone();
    let values = UpdateAssignment::Function(Arc::new(move |batches| {
        evaluated.fetch_add(1, Ordering::Relaxed);
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        Ok(batches
            .iter()
            .map(|batch| {
                let ids = batch
                    .column_by_name("s.id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                Arc::new(Int32Array::from(
                    ids.iter()
                        .map(|id| id.map(|id| id * 100))
                        .collect::<Vec<_>>(),
                )) as ArrayRef
            })
            .collect())
    }));
    let messages = table
        .new_write_builder()
        .new_update()
        .unwrap()
        .merge_into(
            MergeSource::Batches(vec![batch(
                vec![Some(1), Some(2)],
                vec![Some(11), Some(22)],
            )]),
            vec![("id".into(), "id".into())],
            vec![WhenMatched {
                condition: None,
                delete: false,
                assignments: vec![("value".into(), MergeAssignment::Value(values))],
            }],
            Vec::new(),
        )
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 1);
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
async fn core_merge_reads_table_source_at_the_selected_snapshot() {
    let target = table();
    let source = table_at("source");
    seed(&target, &batch(vec![Some(1)], vec![Some(10)])).await;
    seed(&source, &batch(vec![Some(1)], vec![Some(11)])).await;
    seed(&source, &batch(vec![Some(2)], vec![Some(22)])).await;
    let historical = source.copy_with_options(std::collections::HashMap::from([(
        "scan.snapshot-id".into(),
        "1".into(),
    )]));
    let messages = target
        .new_write_builder()
        .new_update()
        .unwrap()
        .merge_into(
            MergeSource::Table(Arc::new(historical)),
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
    TableCommit::new(target.clone(), "merge".into())
        .commit(messages)
        .await
        .unwrap();
    assert_eq!(rows(&target).await, vec![(Some(1), Some(11))]);
    assert_eq!(
        rows(&source).await,
        vec![(Some(1), Some(11)), (Some(2), Some(22))]
    );
}

#[tokio::test]
async fn core_merge_empty_table_source_keeps_no_match_assignments_lazy() {
    let target = table();
    seed(&target, &batch(vec![Some(1)], vec![Some(10)])).await;
    let source = table_at("empty_source");
    let called = Arc::new(AtomicUsize::new(0));
    let evaluated = called.clone();
    let value = UpdateAssignment::DeferredScalar(Arc::new(move || {
        evaluated.fetch_add(1, Ordering::Relaxed);
        Err(crate::Error::DataInvalid {
            message: "Must remain lazy".into(),
            source: None,
        })
    }));
    let messages = target
        .new_write_builder()
        .new_update()
        .unwrap()
        .merge_into(
            MergeSource::Table(Arc::new(source)),
            vec![("id".into(), "id".into())],
            vec![WhenMatched {
                condition: None,
                delete: false,
                assignments: vec![("value".into(), MergeAssignment::Value(value))],
            }],
            Vec::new(),
        )
        .await
        .unwrap();
    assert!(messages.is_empty());
    assert_eq!(called.load(Ordering::Relaxed), 0);
    assert_eq!(rows(&target).await, vec![(Some(1), Some(10))]);
}

#[tokio::test]
async fn core_merge_empty_table_source_obeys_reader_authorization() {
    let target = table();
    seed(&target, &batch(vec![Some(1)], vec![Some(10)])).await;
    let source = table_at("empty_source").copy_with_options(std::collections::HashMap::from([(
        "query-auth.enabled".into(),
        "true".into(),
    )]));
    let error = target
        .new_write_builder()
        .new_update()
        .unwrap()
        .merge_into(
            MergeSource::Table(Arc::new(source)),
            vec![("id".into(), "id".into())],
            vec![update(None)],
            Vec::new(),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("query-auth.enabled"));
    assert_eq!(rows(&target).await, vec![(Some(1), Some(10))]);
}

#[tokio::test]
async fn core_merge_checks_source_duplicates_across_batches() {
    let target = table();
    seed(&target, &batch(vec![Some(1)], vec![Some(10)])).await;
    let source = vec![
        batch(vec![Some(1)], vec![Some(11)]),
        batch(vec![Some(1)], vec![Some(12)]),
    ];
    let error = target
        .new_write_builder()
        .new_update()
        .unwrap()
        .merge_into(
            MergeSource::Batches(source),
            vec![("id".into(), "id".into())],
            vec![update(None)],
            Vec::new(),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("multiple source rows"));
    assert_eq!(rows(&target).await, vec![(Some(1), Some(10))]);
}

#[tokio::test]
async fn core_merge_does_not_reread_a_source_advanced_by_a_condition() {
    let target = table();
    let source = Arc::new(table_at("source_advanced"));
    seed(&target, &batch(vec![Some(1)], vec![Some(10)])).await;
    seed(&source, &batch(vec![Some(1)], vec![Some(11)])).await;
    let advancing = source.clone();
    let condition = MergeCondition {
        target_columns: Vec::new(),
        source_columns: Vec::new(),
        evaluate: Arc::new(move |input| {
            let source = advancing.clone();
            Box::pin(async move {
                seed(&source, &batch(vec![Some(2)], vec![Some(22)])).await;
                Ok(BooleanArray::from(vec![true; input.num_rows()]))
            })
        }),
    };
    let messages = target
        .new_write_builder()
        .new_update()
        .unwrap()
        .merge_into(
            MergeSource::Table(source.clone()),
            vec![("id".into(), "id".into())],
            vec![update(Some(condition))],
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
    TableCommit::new(target.clone(), "merge".into())
        .commit(messages)
        .await
        .unwrap();
    assert_eq!(rows(&target).await, vec![(Some(1), Some(11))]);
    assert_eq!(
        rows(&source).await,
        vec![(Some(1), Some(11)), (Some(2), Some(22))]
    );
}

#[tokio::test]
async fn core_merge_insert_arrays_cover_selected_rows_across_source_batches() {
    let target = table();
    let messages = target
        .new_write_builder()
        .new_update()
        .unwrap()
        .merge_into(
            MergeSource::Batches(vec![
                batch(vec![Some(1)], vec![Some(11)]),
                batch(vec![Some(2)], vec![Some(22)]),
            ]),
            vec![("id".into(), "id".into())],
            Vec::new(),
            vec![WhenNotMatched {
                condition: None,
                assignments: vec![
                    ("id".into(), MergeAssignment::SourceColumn("id".into())),
                    (
                        "value".into(),
                        MergeAssignment::Value(UpdateAssignment::Array(vec![Arc::new(
                            Int32Array::from(vec![100, 200]),
                        )])),
                    ),
                ],
            }],
        )
        .await
        .unwrap();
    TableCommit::new(target.clone(), "merge".into())
        .commit(messages)
        .await
        .unwrap();
    assert_eq!(
        rows(&target).await,
        vec![(Some(1), Some(100)), (Some(2), Some(200))]
    );
}
