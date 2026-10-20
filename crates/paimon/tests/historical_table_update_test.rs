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
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, RecordBatch};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs, write_batch};
use futures::TryStreamExt;
use paimon::spec::{DataType, Datum, IntType, PredicateBuilder, Schema, TableSchema};
use paimon::table::{
    CommitMessage, MergeAssignment, MergeSource, Table, UpdateAssignment, WhenMatched,
    WhenNotMatched,
};

fn batch(names: &[&str], values: &[Vec<i32>]) -> RecordBatch {
    RecordBatch::try_from_iter(names.iter().zip(values).map(|(name, values)| {
        (
            *name,
            Arc::new(Int32Array::from(values.clone())) as ArrayRef,
        )
    }))
    .unwrap()
}

async fn evolved_table(path: &str) -> (Table, Table) {
    let schema = |extra| {
        let mut builder = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .column("keep", DataType::Int(IntType::new()))
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true")
            .option("deletion-vectors.enabled", "true");
        if extra {
            builder = builder.column("extra", DataType::Int(IntType::new()));
        }
        TableSchema::new(i64::from(extra), &builder.build().unwrap())
    };
    let (io, original) = memory_table(path, schema(false));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, original.schema()).await;
    write_batch(
        &original,
        &batch(
            &["id", "value", "keep"],
            &[vec![1, 2], vec![10, 20], vec![100, 200]],
        ),
    )
    .await;
    let latest_schema = schema(true);
    persist_table_schema(&io, path, &latest_schema).await;
    let latest = original
        .copy_with_resolved_schema(latest_schema, "main")
        .unwrap();
    write_batch(
        &latest,
        &batch(
            &["id", "value", "keep", "extra"],
            &[vec![3], vec![30], vec![300], vec![999]],
        ),
    )
    .await;
    let historical = latest
        .copy_with_time_travel(HashMap::from([("scan.snapshot-id".into(), "1".into())]))
        .await
        .unwrap();
    assert_eq!(historical.schema().id(), 0);
    assert!(historical.is_time_traveled());
    (latest, historical)
}

async fn commit(table: &Table, messages: Vec<CommitMessage>) {
    table
        .new_write_builder()
        .new_commit()
        .commit(messages)
        .await
        .unwrap();
}

async fn rows(table: &Table) -> Vec<Vec<Option<i64>>> {
    let builder = table.new_read_builder();
    let plan = builder.new_scan().plan().await.unwrap();
    let batches: Vec<_> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut rows: Vec<Vec<Option<i64>>> = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|array| {
                        array.is_valid(row).then(|| {
                            if let Some(array) = array.as_any().downcast_ref::<Int32Array>() {
                                i64::from(array.value(row))
                            } else {
                                array
                                    .as_any()
                                    .downcast_ref::<Int64Array>()
                                    .unwrap()
                                    .value(row)
                            }
                        })
                    })
                    .collect(),
            );
        }
    }
    rows.sort_by_key(|row| row[0]);
    rows
}

fn changed() -> RecordBatch {
    RecordBatch::try_from_iter([
        ("_ROW_ID", Arc::new(Int64Array::from(vec![0])) as ArrayRef),
        ("value", Arc::new(Int32Array::from(vec![11])) as ArrayRef),
    ])
    .unwrap()
}

#[tokio::test]
async fn historical_updates_keep_schema_and_conflict_baseline() {
    for operation in [
        "row_id",
        "incremental",
        "groups",
        "predicate",
        "shard",
        "merge",
    ] {
        let (latest, historical) =
            evolved_table(&format!("memory:/historical_update_{operation}")).await;
        let mut update = historical.new_write_builder().new_update().unwrap();
        update.with_update_type(vec!["value".into()]).unwrap();
        let messages = match operation {
            "row_id" => update
                .update_by_arrow_with_row_id(vec![changed()])
                .await
                .unwrap(),
            "groups" => update
                .update_by_arrow_batches_with_row_id([Ok(vec![changed()])])
                .await
                .unwrap(),
            "incremental" => update
                .new_update_by_row_id()
                .await
                .unwrap()
                .update_columns(vec![changed()], vec!["value".into()])
                .await
                .unwrap(),
            "predicate" => update
                .update_by_predicate(
                    Some(
                        PredicateBuilder::new(historical.schema().fields())
                            .equal("id", Datum::Int(1))
                            .unwrap(),
                    ),
                    vec![(
                        "value".into(),
                        UpdateAssignment::Array(vec![Arc::new(Int32Array::from(vec![11]))]),
                    )],
                    Vec::new(),
                )
                .await
                .unwrap(),
            "merge" => update
                .merge_into(
                    MergeSource::Batches(vec![batch(&["id", "value"], &[vec![1], vec![11]])]),
                    vec![("id".into(), "id".into())],
                    vec![WhenMatched {
                        condition: None,
                        delete: false,
                        assignments: vec![(
                            "value".into(),
                            MergeAssignment::SourceColumn("value".into()),
                        )],
                    }],
                    Vec::new(),
                )
                .await
                .unwrap(),
            "shard" => {
                let mut shard = update.new_shard_updater(0, 1, None).await.unwrap();
                let inputs: Vec<_> = shard.arrow_reader().unwrap().try_collect().await.unwrap();
                assert_eq!(inputs.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
                for input in inputs {
                    let ids = input
                        .column_by_name("id")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    shard
                        .update_by_arrow_batch(batch(
                            &["value"],
                            &[ids
                                .values()
                                .iter()
                                .map(|id| if *id == 1 { 11 } else { 20 })
                                .collect()],
                        ))
                        .await
                        .unwrap();
                }
                shard.prepare_commit().await.unwrap()
            }
            _ => unreachable!(),
        };
        assert!(!messages.is_empty(), "{operation}");
        for message in &messages {
            assert_eq!(message.check_from_snapshot, Some(1), "{operation}");
            for file in &message.new_files {
                assert_eq!(file.schema_id, 0);
                assert_eq!(file.write_cols, Some(vec!["value".into()]));
            }
        }
        commit(&latest, messages).await;
        let expected = vec![
            vec![Some(1), Some(11), Some(100), None],
            vec![Some(2), Some(20), Some(200), None],
            vec![Some(3), Some(30), Some(300), Some(999)],
        ];
        assert_eq!(rows(&latest).await, expected, "{operation}");
        // Selection and check_from_snapshot must both keep snapshot 1. A
        // second update of the same old file must not silently overwrite it.
        let stale = update
            .update_by_arrow_with_row_id(vec![changed()])
            .await
            .unwrap();
        assert!(stale
            .iter()
            .all(|message| message.check_from_snapshot == Some(1)));
        let error = latest
            .new_write_builder()
            .new_commit()
            .commit(stale)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("conflict"),
            "{operation}: {error}"
        );
        assert_eq!(rows(&latest).await, expected);
    }
}

#[tokio::test]
async fn historical_deletes_preserve_newer_rows() {
    for operation in ["row_id", "predicate"] {
        let (latest, historical) =
            evolved_table(&format!("memory:/historical_delete_{operation}")).await;
        let update = historical.new_write_builder().new_update().unwrap();
        let messages = if operation == "row_id" {
            update.delete_by_row_id(vec![0, 0]).await.unwrap()
        } else {
            update
                .delete_by_predicate(Some(
                    PredicateBuilder::new(historical.schema().fields())
                        .equal("id", Datum::Int(1))
                        .unwrap(),
                ))
                .await
                .unwrap()
        };
        assert!(!messages.is_empty());
        assert!(messages
            .iter()
            .all(|message| message.check_from_snapshot == Some(1)));
        commit(&latest, messages).await;
        assert_eq!(
            rows(&latest).await,
            vec![
                vec![Some(2), Some(20), Some(200), None],
                vec![Some(3), Some(30), Some(300), Some(999)],
            ]
        );
    }
}

#[tokio::test]
async fn historical_table_can_publish_ordinary_append_with_old_schema() {
    let (latest, historical) = evolved_table("memory:/historical_ordinary_append").await;
    let builder = historical.new_write_builder();
    let mut write = builder.new_write().unwrap();
    write
        .write_arrow_batch(&batch(
            &["id", "value", "keep"],
            &[vec![4], vec![40], vec![400]],
        ))
        .await
        .unwrap();
    let messages = write.prepare_commit().await.unwrap();
    assert!(messages
        .iter()
        .flat_map(|message| &message.new_files)
        .all(|file| file.schema_id == historical.schema().id()));
    builder
        .try_new_commit()
        .unwrap()
        .commit(messages)
        .await
        .unwrap();
    assert_eq!(
        latest
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap()
            .schema_id(),
        latest.schema().id()
    );
    assert_eq!(
        rows(&latest).await,
        vec![
            vec![Some(1), Some(10), Some(100), None],
            vec![Some(2), Some(20), Some(200), None],
            vec![Some(3), Some(30), Some(300), Some(999)],
            vec![Some(4), Some(40), Some(400), None],
        ]
    );
    assert_eq!(
        rows(&historical).await,
        vec![
            vec![Some(1), Some(10), Some(100)],
            vec![Some(2), Some(20), Some(200)],
        ]
    );
}

async fn append_mutation(table: &Table, operation: &str, input: RecordBatch) -> Vec<CommitMessage> {
    let mut update = table.new_write_builder().new_update().unwrap();
    update.with_update_type(vec!["value".into()]).unwrap();
    if operation == "upsert" {
        update
            .upsert_by_arrow_with_key(vec![input], vec!["id".into()])
            .await
            .unwrap()
    } else {
        update
            .merge_into(
                MergeSource::Batches(vec![input]),
                vec![("id".into(), "id".into())],
                vec![WhenMatched {
                    condition: None,
                    delete: false,
                    assignments: vec![(
                        "value".into(),
                        MergeAssignment::SourceColumn("value".into()),
                    )],
                }],
                vec![WhenNotMatched {
                    condition: None,
                    assignments: ["id", "value", "keep"]
                        .into_iter()
                        .map(|name| (name.into(), MergeAssignment::SourceColumn(name.into())))
                        .collect(),
                }],
            )
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn historical_upsert_and_merge_append_with_the_read_schema() {
    for operation in ["upsert", "merge"] {
        for only_matched in [false, true] {
            let (latest, historical) = evolved_table(&format!(
                "memory:/historical_append_{operation}_{only_matched}"
            ))
            .await;
            let input = || {
                if only_matched {
                    batch(&["id", "value", "keep"], &[vec![1], vec![11], vec![101]])
                } else {
                    batch(
                        &["id", "value", "keep"],
                        &[vec![1, 4], vec![11, 40], vec![101, 400]],
                    )
                }
            };
            let messages = append_mutation(&historical, operation, input()).await;
            assert_eq!(messages.len(), if only_matched { 1 } else { 2 });
            for message in &messages {
                for file in &message.new_files {
                    assert_eq!(file.schema_id, 0);
                    if message.check_from_snapshot.is_some() {
                        assert_eq!(message.check_from_snapshot, Some(1));
                        assert_eq!(file.first_row_id, Some(0));
                        assert_eq!(file.write_cols, Some(vec!["value".into()]));
                    } else {
                        // Only publication assigns an appended row's new identity.
                        assert_eq!(file.first_row_id, None);
                    }
                }
            }
            commit(&latest, messages).await;
            let mut expected = vec![
                vec![Some(1), Some(11), Some(100), None],
                vec![Some(2), Some(20), Some(200), None],
                vec![Some(3), Some(30), Some(300), Some(999)],
            ];
            if !only_matched {
                expected.push(vec![Some(4), Some(40), Some(400), None]);
            }
            assert_eq!(rows(&latest).await, expected);
            let stale = append_mutation(&historical, operation, input()).await;
            let error = latest
                .new_write_builder()
                .new_commit()
                .commit(stale)
                .await
                .unwrap_err();
            assert!(
                error.to_string().to_lowercase().contains("conflict"),
                "{error}"
            );
            assert_eq!(rows(&latest).await, expected);
        }
    }
}

#[tokio::test]
async fn captured_empty_upsert_and_merge_only_append() {
    for operation in ["upsert", "merge"] {
        let (latest, historical) =
            evolved_table(&format!("memory:/captured_empty_append_{operation}")).await;
        let empty = historical.copy_with_pinned_snapshot(None);
        let messages = append_mutation(
            &empty,
            operation,
            batch(&["id", "value", "keep"], &[vec![4], vec![40], vec![400]]),
        )
        .await;
        assert!(!messages.is_empty());
        assert!(messages
            .iter()
            .all(|message| message.check_from_snapshot.is_none()));
        assert!(messages
            .iter()
            .flat_map(|message| &message.new_files)
            .all(|file| file.schema_id == 0 && file.first_row_id.is_none()));
        commit(&latest, messages).await;
        assert_eq!(
            rows(&latest).await,
            vec![
                vec![Some(1), Some(10), Some(100), None],
                vec![Some(2), Some(20), Some(200), None],
                vec![Some(3), Some(30), Some(300), Some(999)],
                vec![Some(4), Some(40), Some(400), None],
            ]
        );
    }
}

#[tokio::test]
async fn historical_append_mutations_keep_original_field_ids() {
    for operation in ["upsert", "merge"] {
        for change in ["rename", "drop_readd", "widen"] {
            let (table, historical) = evolved_table(&format!(
                "memory:/historical_append_fields_{operation}_{change}"
            ))
            .await;
            let mut schema = serde_json::to_value(table.schema()).unwrap();
            schema["id"] = 2.into();
            match change {
                "rename" => schema["fields"][1]["name"] = "renamed".into(),
                "drop_readd" => {
                    schema["fields"][1]["id"] = 4.into();
                    schema["highestFieldId"] = 4.into();
                }
                "widen" => schema["fields"][1]["type"] = "BIGINT".into(),
                _ => unreachable!(),
            }
            let schema: TableSchema = serde_json::from_value(schema).unwrap();
            persist_table_schema(table.file_io(), table.location(), &schema).await;
            let latest = table.copy_with_resolved_schema(schema, "main").unwrap();
            let messages = append_mutation(
                &historical,
                operation,
                batch(
                    &["id", "value", "keep"],
                    &[vec![1, 4], vec![11, 40], vec![101, 400]],
                ),
            )
            .await;
            assert!(messages
                .iter()
                .flat_map(|message| &message.new_files)
                .all(|file| file.schema_id == 0));
            // The committer's historical read schema must not replace the
            // latest schema used for publication or conflict validation.
            commit(&historical, messages).await;
            let values = if change == "drop_readd" {
                [None; 4]
            } else {
                [Some(11), Some(20), Some(30), Some(40)]
            };
            let expected = vec![
                vec![Some(1), values[0], Some(100), None],
                vec![Some(2), values[1], Some(200), None],
                vec![Some(3), values[2], Some(300), Some(999)],
                vec![Some(4), values[3], Some(400), None],
            ];
            assert_eq!(rows(&latest).await, expected, "{operation} {change}");
            let stale = append_mutation(
                &historical,
                operation,
                batch(
                    &["id", "value", "keep"],
                    &[vec![1, 4], vec![12, 41], vec![101, 400]],
                ),
            )
            .await;
            let error = historical
                .new_write_builder()
                .new_commit()
                .commit(stale)
                .await
                .unwrap_err();
            assert!(
                error.to_string().to_lowercase().contains("conflict"),
                "{operation} {change}: {error}"
            );
            assert_eq!(rows(&latest).await, expected);
        }
    }
}

#[tokio::test]
async fn historical_partial_files_follow_field_ids_after_schema_changes() {
    for change in ["rename", "drop_readd", "widen"] {
        let (table, historical) =
            evolved_table(&format!("memory:/historical_fields_{change}")).await;
        let mut schema = serde_json::to_value(table.schema()).unwrap();
        schema["id"] = 2.into();
        match change {
            "rename" => schema["fields"][1]["name"] = "renamed".into(),
            "drop_readd" => {
                schema["fields"][1]["id"] = 4.into();
                schema["highestFieldId"] = 4.into();
            }
            "widen" => schema["fields"][1]["type"] = "BIGINT".into(),
            _ => unreachable!(),
        }
        let schema: TableSchema = serde_json::from_value(schema).unwrap();
        persist_table_schema(table.file_io(), table.location(), &schema).await;
        let latest = table.copy_with_resolved_schema(schema, "main").unwrap();
        let value = if change == "widen" { 1i64 << 40 } else { 40 };
        let values: ArrayRef = if change == "widen" {
            Arc::new(Int64Array::from(vec![value]))
        } else {
            Arc::new(Int32Array::from(vec![40]))
        };
        let input = RecordBatch::try_new(
            paimon::arrow::build_target_arrow_schema(latest.schema().fields()).unwrap(),
            vec![
                Arc::new(Int32Array::from(vec![4])),
                values,
                Arc::new(Int32Array::from(vec![400])),
                Arc::new(Int32Array::from(vec![1000])),
            ],
        )
        .unwrap();
        write_batch(&latest, &input).await;
        let messages = historical
            .new_write_builder()
            .new_update()
            .unwrap()
            .update_by_arrow_with_row_id(vec![changed()])
            .await
            .unwrap();
        assert!(messages
            .iter()
            .all(|message| message.check_from_snapshot == Some(1)));
        assert!(messages
            .iter()
            .flat_map(|message| &message.new_files)
            .all(|file| file.schema_id == 0 && file.write_cols == Some(vec!["value".into()])));
        commit(&historical, messages).await;
        let expected = if change == "drop_readd" {
            [None, None, None]
        } else {
            [Some(11), Some(20), Some(30)]
        };
        assert_eq!(
            rows(&latest).await,
            vec![
                vec![Some(1), expected[0], Some(100), None],
                vec![Some(2), expected[1], Some(200), None],
                vec![Some(3), expected[2], Some(300), Some(999)],
                vec![Some(4), Some(value), Some(400), Some(1000)],
            ],
            "{change}"
        );
    }
}

#[tokio::test]
async fn historical_deletion_vectors_cannot_restore_a_concurrent_deletion() {
    let (latest, historical) = evolved_table("memory:/historical_delete_conflict").await;
    let newer = latest
        .new_write_builder()
        .new_update()
        .unwrap()
        .delete_by_row_id(vec![1])
        .await
        .unwrap();
    commit(&latest, newer).await;
    let stale = historical
        .new_write_builder()
        .new_update()
        .unwrap()
        .delete_by_row_id(vec![0])
        .await
        .unwrap();
    assert!(stale
        .iter()
        .all(|message| message.check_from_snapshot == Some(1)));
    let error = latest
        .new_write_builder()
        .new_commit()
        .commit(stale)
        .await
        .unwrap_err();
    assert!(
        error.to_string().to_ascii_lowercase().contains("conflict"),
        "{error}"
    );
    assert_eq!(
        rows(&latest).await,
        vec![
            vec![Some(1), Some(10), Some(100), None],
            vec![Some(3), Some(30), Some(300), Some(999)],
        ]
    );
}

#[tokio::test]
async fn captured_empty_update_views_do_not_observe_later_rows() {
    let (latest, _) = evolved_table("memory:/captured_empty_update").await;
    let empty = latest.copy_with_pinned_snapshot(None);
    let update = empty.new_write_builder().new_update().unwrap();
    let messages = update
        .update_by_predicate(
            None,
            vec![(
                "value".into(),
                UpdateAssignment::Function(Arc::new(|_| panic!("empty view evaluated a callback"))),
            )],
            vec!["value".into()],
        )
        .await
        .unwrap();
    assert!(messages.is_empty());
    assert!(update.delete_by_predicate(None).await.unwrap().is_empty());
    let mut shard = update.new_shard_updater(0, 1, None).await.unwrap();
    let batches: Vec<_> = shard.arrow_reader().unwrap().try_collect().await.unwrap();
    assert!(batches.is_empty());
    assert!(shard.prepare_commit().await.unwrap().is_empty());
    let messages = update
        .merge_into(
            MergeSource::Batches(vec![batch(&["id", "value"], &[vec![1], vec![11]])]),
            vec![("id".into(), "id".into())],
            vec![WhenMatched {
                condition: None,
                delete: false,
                assignments: vec![(
                    "value".into(),
                    MergeAssignment::SourceColumn("value".into()),
                )],
            }],
            Vec::new(),
        )
        .await
        .unwrap();
    assert!(messages.is_empty());
    let error = update
        .update_by_arrow_with_row_id(vec![changed()])
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("No files with row tracking"),
        "{error}"
    );
    assert_eq!(rows(&latest).await.len(), 3);
}

#[tokio::test]
async fn row_id_merge_reads_the_independently_selected_source_view() {
    for source_view in ["current", "snapshot", "empty"] {
        let (latest, historical) =
            evolved_table(&format!("memory:/merge_source_view_{source_view}")).await;
        let changed = RecordBatch::try_from_iter([
            ("_ROW_ID", Arc::new(Int64Array::from(vec![0])) as ArrayRef),
            ("value", Arc::new(Int32Array::from(vec![99])) as ArrayRef),
        ])
        .unwrap();
        commit(
            &latest,
            latest
                .new_write_builder()
                .new_update()
                .unwrap()
                .update_by_arrow_with_row_id(vec![changed])
                .await
                .unwrap(),
        )
        .await;
        let source = match source_view {
            "snapshot" => latest
                .copy_with_time_travel(HashMap::from([("scan.snapshot-id".into(), "3".into())]))
                .await
                .unwrap(),
            "empty" => latest.copy_with_pinned_snapshot(None),
            _ => latest.clone(),
        };
        let assignment = if source_view == "empty" {
            MergeAssignment::Value(UpdateAssignment::Function(Arc::new(|_| {
                panic!("an empty MERGE source evaluated an assignment")
            })))
        } else {
            MergeAssignment::SourceColumn("value".into())
        };
        let messages = historical
            .new_write_builder()
            .new_update()
            .unwrap()
            .merge_into(
                MergeSource::Table(Arc::new(source)),
                vec![("_ROW_ID".into(), "_ROW_ID".into())],
                vec![WhenMatched {
                    condition: None,
                    delete: false,
                    assignments: vec![("keep".into(), assignment)],
                }],
                Vec::new(),
            )
            .await
            .unwrap();
        let kept = if source_view == "empty" {
            assert!(messages.is_empty());
            [100, 200]
        } else {
            assert!(!messages.is_empty());
            assert!(messages
                .iter()
                .all(|message| message.check_from_snapshot == Some(1)));
            commit(&latest, messages).await;
            [99, 20]
        };
        assert_eq!(
            rows(&latest).await,
            vec![
                vec![Some(1), Some(99), Some(kept[0]), None],
                vec![Some(2), Some(20), Some(kept[1]), None],
                vec![Some(3), Some(30), Some(300), Some(999)],
            ],
            "{source_view}"
        );
    }
}
