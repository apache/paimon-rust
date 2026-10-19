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

//! Mutation readers require an unrestricted, live whole-table REST grant.

use super::*;
use arrow_array::{ArrayRef, Int64Array};

const EVOLUTION_OPTIONS: &[(&str, &str)] = &[
    ("row-tracking.enabled", "true"),
    ("data-evolution.enabled", "true"),
    ("deletion-vectors.enabled", "true"),
];

fn row_update(row_id: i64, value: i32) -> RecordBatch {
    RecordBatch::try_from_iter([
        (
            "_ROW_ID",
            Arc::new(Int64Array::from(vec![row_id])) as ArrayRef,
        ),
        ("value", Arc::new(Int32Array::from(vec![value])) as ArrayRef),
    ])
    .unwrap()
}

fn callback(calls: Arc<AtomicUsize>) -> UpdateAssignment {
    UpdateAssignment::Function(Arc::new(move |batches| {
        calls.fetch_add(1, Ordering::SeqCst);
        Ok(batches
            .iter()
            .map(|batch| Arc::new(Int32Array::from(vec![99; batch.num_rows()])) as ArrayRef)
            .collect())
    }))
}

async fn mutate(
    table: &Table,
    operation: &str,
    calls: Arc<AtomicUsize>,
) -> Result<Vec<CommitMessage>> {
    let mut update = table.new_write_builder().new_update()?;
    match operation {
        "direct" => {
            let mut writer = DataEvolutionWriter::new(table, vec!["value".into()])?;
            writer.add_matched_batch(row_update(0, 99))?;
            writer.prepare_commit().await
        }
        "row-id" => {
            update
                .update_by_arrow_with_row_id(vec![row_update(0, 99)])
                .await
        }
        "grouped" => {
            update
                .update_by_arrow_batches_with_row_id([
                    Ok(vec![row_update(0, 99)]),
                    Ok(vec![row_update(1, 98)]),
                ])
                .await
        }
        "incremental" => {
            let mut writer = update.new_update_by_row_id().await?;
            writer
                .update_columns(vec![row_update(0, 99)], vec!["value".into()])
                .await?;
            writer
                .update_columns(vec![row_update(1, 98)], vec!["value".into()])
                .await
        }
        "predicate" => {
            update
                .update_by_predicate(
                    None,
                    vec![("value".into(), callback(calls))],
                    vec!["value".into()],
                )
                .await
        }
        "upsert" => {
            update
                .upsert_by_arrow_with_key(
                    vec![batch(table, &[(1, "a", 99), (3, "a", 30)])],
                    vec!["id".into()],
                )
                .await
        }
        "delete" => update.delete_by_row_id(vec![0]).await,
        "delete-predicate" => {
            update
                .delete_by_predicate(Some(
                    crate::spec::PredicateBuilder::new(table.schema().fields())
                        .equal("id", Datum::Int(1))?,
                ))
                .await
        }
        "delete-partition" => {
            update
                .delete_by_predicate(Some(
                    crate::spec::PredicateBuilder::new(table.schema().fields())
                        .equal("p", Datum::String("a".into()))?,
                ))
                .await
        }
        "shard" => {
            update.with_update_type(vec!["value".into()])?;
            let fields = vec![table.schema().fields()[2].clone()];
            let mut shard = update.new_shard_updater(0, 1, Some(fields)).await?;
            let mut reader = shard.arrow_reader()?;
            while let Some(input) = reader.try_next().await? {
                let values = RecordBatch::try_from_iter([(
                    "value",
                    Arc::new(Int32Array::from(vec![99; input.num_rows()])) as ArrayRef,
                )])
                .unwrap();
                shard.update_by_arrow_batch(values).await?;
            }
            shard.prepare_commit().await
        }
        "merge" => {
            update
                .merge_into(
                    MergeSource::Batches(vec![batch(table, &[(1, "a", 99)])]),
                    vec![("id".into(), "id".into())],
                    vec![WhenMatched {
                        condition: None,
                        delete: false,
                        assignments: vec![(
                            "value".into(),
                            MergeAssignment::Value(callback(calls)),
                        )],
                    }],
                    vec![],
                )
                .await
        }
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn unrestricted_rest_grants_support_existing_mutation_apis() {
    for operation in [
        "direct",
        "row-id",
        "grouped",
        "incremental",
        "predicate",
        "upsert",
        "delete",
        "delete-predicate",
        "delete-partition",
        "shard",
        "merge",
    ] {
        let fixture = RestTable::new(false, EVOLUTION_OPTIONS).await;
        // Two file groups ensure callbacks and original-row rewrites preserve
        // the grant when a plan is split into smaller physical read groups.
        fixture.append("seed-one", &[(1, "a", 10)], false).await;
        fixture.append("seed-two", &[(2, "a", 20)], false).await;
        fixture.state.deny_query.store(false, Ordering::SeqCst);
        let calls = Arc::new(AtomicUsize::new(0));
        let messages = mutate(&fixture.table, operation, calls.clone())
            .await
            .unwrap_or_else(|error| panic!("{operation}: {error}"));
        TableCommit::new(fixture.table.clone(), operation.into())
            .commit(messages)
            .await
            .unwrap();
        let mut expected = match operation {
            "delete" | "delete-predicate" => vec![(2, "a".into(), 20)],
            "delete-partition" => vec![],
            "grouped" | "incremental" => vec![(1, "a".into(), 99), (2, "a".into(), 98)],
            "predicate" | "shard" => vec![(1, "a".into(), 99), (2, "a".into(), 99)],
            _ => vec![(1, "a".into(), 99), (2, "a".into(), 20)],
        };
        if operation == "upsert" {
            expected.push((3, "a".into(), 30));
        }
        assert_eq!(fixture.stored_rows().await, expected, "{operation}");
        assert!(
            fixture.state.query_requests.load(Ordering::SeqCst) > 0,
            "{operation}"
        );
        if operation == "predicate" {
            assert_eq!(calls.load(Ordering::SeqCst), 2);
        }
        if operation == "merge" {
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        }
    }
}

fn restricted_responses() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({"filter": [serde_json::json!({
            "kind": "LEAF", "transform": {"name": "FIELD_REF", "fieldRef": {
                "index": 0, "name": "id", "type": "INT"
            }}, "function": "EQUAL", "literals": [1]
        }).to_string()]}),
        // Even an identity mask is a server restriction; accepting it here
        // would allow a future nonidentity mask to corrupt unmatched rows.
        serde_json::json!({"columnMasking": {"value": serde_json::json!({
            "name": "FIELD_REF", "fieldRef": {"index": 2, "name": "value", "type": "INT"}
        }).to_string()}}),
        // Ordinary reads ignore masks on hidden system fields. A mutation
        // requires the original whole-table response to be unrestricted too.
        serde_json::json!({"columnMasking": {"_ROW_ID": serde_json::json!({
            "name": "FIELD_REF", "fieldRef": {"index": 2, "name": "value", "type": "INT"}
        }).to_string()}}),
    ]
}

#[tokio::test]
async fn restricted_or_denied_mutations_never_evaluate_callbacks_or_stage_files() {
    for operation in [
        "direct",
        "row-id",
        "grouped",
        "incremental",
        "predicate",
        "upsert",
        "delete",
        "delete-predicate",
        "delete-partition",
        "shard",
        "merge",
    ] {
        for response in [None]
            .into_iter()
            .chain(restricted_responses().into_iter().map(Some))
        {
            let fixture = RestTable::new(false, EVOLUTION_OPTIONS).await;
            fixture.append("seed", &[(1, "a", 10)], false).await;
            if let Some(response) = response {
                fixture.state.deny_query.store(false, Ordering::SeqCst);
                *fixture.state.query_response.lock().await = response;
            }
            let before = fixture
                .table
                .file_io()
                .list_status_recursive(fixture.table.location())
                .await
                .unwrap()
                .len();
            let calls = Arc::new(AtomicUsize::new(0));
            assert!(
                mutate(&fixture.table, operation, calls.clone())
                    .await
                    .is_err(),
                "{operation}"
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0, "{operation}");
            assert_eq!(
                fixture
                    .table
                    .file_io()
                    .list_status_recursive(fixture.table.location())
                    .await
                    .unwrap()
                    .len(),
                before,
                "{operation}"
            );
            assert_eq!(fixture.stored_rows().await, vec![(1, "a".into(), 10)]);
            assert!(
                fixture.state.query_requests.load(Ordering::SeqCst) > 0,
                "{operation}"
            );
        }
    }
}

#[tokio::test]
async fn empty_target_does_not_bypass_mutation_authorization() {
    for operation in [
        "direct",
        "row-id",
        "predicate",
        "upsert",
        "delete",
        "delete-predicate",
        "delete-partition",
        "shard",
        "merge",
    ] {
        let fixture = RestTable::new(false, EVOLUTION_OPTIONS).await;
        let calls = Arc::new(AtomicUsize::new(0));
        assert!(
            mutate(&fixture.table, operation, calls.clone())
                .await
                .is_err(),
            "{operation}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(
            fixture.state.query_requests.load(Ordering::SeqCst) > 0,
            "{operation}"
        );
        assert_eq!(fixture.state.commit_requests.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn assembled_handles_cannot_mutate_or_replay_a_live_grant() {
    let fixture = RestTable::new(false, EVOLUTION_OPTIONS).await;
    fixture.append("seed", &[(1, "a", 10)], false).await;
    fixture.state.deny_query.store(false, Ordering::SeqCst);
    let forged = Table::new(
        fixture.table.file_io().clone(),
        fixture.table.identifier().clone(),
        fixture.table.location().into(),
        fixture.table.schema().clone(),
        None,
    );
    let calls = Arc::new(AtomicUsize::new(0));
    for operation in [
        "direct",
        "row-id",
        "predicate",
        "upsert",
        "delete",
        "shard",
        "merge",
    ] {
        assert!(
            mutate(&forged, operation, calls.clone()).await.is_err(),
            "{operation}"
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        fixture.state.query_requests.load(Ordering::SeqCst),
        0,
        "an assembled handle must be refused before contacting the catalog"
    );
    assert_eq!(fixture.stored_rows().await, vec![(1, "a".into(), 10)]);

    let plan = fixture
        .table
        .new_read_builder()
        .new_scan()
        .plan_all_files()
        .await
        .unwrap();
    let mut builder = fixture.table.new_read_builder();
    builder.with_projection(&["_ROW_ID"]).unwrap();
    assert!(
        builder.new_read().unwrap().to_arrow(plan.splits()).is_err(),
        "whole-table grants do not expose system fields through public readers"
    );
    assert!(
        TableRead::new(&forged, vec![crate::spec::row_id_data_field()], vec![])
            .for_update()
            .to_arrow(plan.splits())
            .is_err(),
        "internal mutation readers cannot replay a live grant on a forged handle"
    );
}

#[tokio::test]
async fn only_unrestricted_live_grants_allow_internal_row_id_reads() {
    let fixture = RestTable::new(false, EVOLUTION_OPTIONS).await;
    fixture.append("seed", &[(1, "a", 10)], false).await;
    fixture.state.deny_query.store(false, Ordering::SeqCst);
    let mut builder = fixture.table.new_read_builder();
    builder.with_projection(&["_ROW_ID"]).unwrap();
    let read = builder.new_read().unwrap().for_update();
    for response in restricted_responses() {
        *fixture.state.query_response.lock().await = response;
        // Ordinary reads may plan restricted splits, but their grant cannot
        // be repurposed to authorize a physical-row mutation read.
        let plan = fixture
            .table
            .new_read_builder()
            .new_scan()
            .plan()
            .await
            .unwrap();
        assert!(read.to_arrow(plan.splits()).is_err());
    }
    *fixture.state.query_response.lock().await = serde_json::json!({});
    let plan = fixture
        .table
        .new_read_builder()
        .new_scan()
        .plan_all_files()
        .await
        .unwrap();
    let serialized = serde_json::to_vec(plan.splits()).unwrap();
    let replay: Vec<DataSplit> = serde_json::from_slice(&serialized).unwrap();
    assert!(
        read.to_arrow(&replay).is_err(),
        "serialized metadata drops the live grant"
    );
}

#[tokio::test]
async fn authorized_rewrites_preserve_physical_rows_hidden_by_deletion_vectors() {
    let fixture = RestTable::new(false, EVOLUTION_OPTIONS).await;
    fixture
        .append("seed", &[(1, "a", 10), (2, "a", 20)], false)
        .await;
    fixture.state.deny_query.store(false, Ordering::SeqCst);
    let update = fixture.table.new_write_builder().new_update().unwrap();
    let deleted = update.delete_by_row_id(vec![0]).await.unwrap();
    TableCommit::new(fixture.table.clone(), "delete".into())
        .commit(deleted)
        .await
        .unwrap();
    let changed = update
        .update_by_arrow_with_row_id(vec![row_update(1, 99)])
        .await
        .unwrap();
    TableCommit::new(fixture.table.clone(), "update".into())
        .commit(changed)
        .await
        .unwrap();
    assert_eq!(fixture.stored_rows().await, vec![(2, "a".into(), 99)]);
}

#[tokio::test]
async fn authorized_empty_targets_allow_insert_only_upsert_and_merge() {
    for operation in ["upsert", "merge"] {
        let fixture = RestTable::new(false, EVOLUTION_OPTIONS).await;
        fixture.state.deny_query.store(false, Ordering::SeqCst);
        let update = fixture.table.new_write_builder().new_update().unwrap();
        let source = vec![batch(&fixture.table, &[(1, "a", 10)])];
        let messages = if operation == "upsert" {
            update
                .upsert_by_arrow_with_key(source, vec!["id".into()])
                .await
                .unwrap()
        } else {
            update
                .merge_into(
                    MergeSource::Batches(source),
                    vec![("id".into(), "id".into())],
                    vec![],
                    vec![WhenNotMatched {
                        condition: None,
                        assignments: ["id", "p", "value"]
                            .into_iter()
                            .map(|name| (name.into(), MergeAssignment::SourceColumn(name.into())))
                            .collect(),
                    }],
                )
                .await
                .unwrap()
        };
        TableCommit::new(fixture.table.clone(), operation.into())
            .commit(messages)
            .await
            .unwrap();
        assert_eq!(fixture.stored_rows().await, vec![(1, "a".into(), 10)]);
        assert!(fixture.state.query_requests.load(Ordering::SeqCst) > 0);
    }
}
