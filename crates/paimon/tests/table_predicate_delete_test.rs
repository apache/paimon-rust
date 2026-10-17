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

use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs, write_batch};
use futures::TryStreamExt;
use paimon::spec::{
    DataType, Datum, IndexManifest, IntType, Predicate, PredicateBuilder, Schema, TableSchema,
    VarCharType,
};
use paimon::table::{CommitMessage, Table};

async fn table(options: &[(&str, &str)]) -> Table {
    table_with_primary_keys(options, false).await
}

async fn table_with_primary_keys(options: &[(&str, &str)], pk: bool) -> Table {
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("p", DataType::VarChar(VarCharType::string_type()))
        .column("v", DataType::Int(IntType::new()))
        .column("q", DataType::Int(IntType::new()))
        .partition_keys(["q", "p"])
        .primary_key(if pk { vec!["id", "q", "p"] } else { vec![] })
        .option("data-evolution.enabled", "true")
        .option("row-tracking.enabled", "true")
        .option("deletion-vectors.enabled", "true");
    for (key, value) in options {
        schema = schema.option(*key, *value);
    }
    let path = "memory:/predicate_delete";
    let (io, table) = memory_table(path, TableSchema::new(0, &schema.build().unwrap()));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    table
}

fn batch(id: Vec<i32>, p: Vec<Option<&str>>, v: Vec<i32>, q: Vec<i32>) -> RecordBatch {
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(id)) as ArrayRef),
        ("p", Arc::new(StringArray::from(p)) as ArrayRef),
        ("v", Arc::new(Int32Array::from(v)) as ArrayRef),
        ("q", Arc::new(Int32Array::from(q)) as ArrayRef),
    ])
    .unwrap()
}

async fn seed(table: &Table) {
    write_batch(
        table,
        &batch(
            vec![0, 1, 2, 3],
            vec![
                Some("a"),
                Some("a"),
                Some("b"),
                if table.schema().primary_keys().is_empty() {
                    None
                } else {
                    Some("c")
                },
            ],
            vec![10, 20, 30, 40],
            vec![1, 1, 2, 2],
        ),
    )
    .await;
}

async fn commit(table: &Table, messages: Vec<CommitMessage>) {
    table
        .new_write_builder()
        .new_commit()
        .commit(messages)
        .await
        .unwrap();
}

async fn ids(table: &Table) -> Vec<i32> {
    // Ordinary FULL reads let the assertion include unindexed rows even if
    // the operation under test was configured for PARTIAL/DETAIL search.
    let table = table.copy_with_options(HashMap::from([(
        "scalar-index.search-mode".into(),
        "FULL".into(),
    )]));
    let mut builder = table.new_read_builder();
    builder.with_projection(&["id"]).unwrap();
    let plan = builder.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut ids: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    ids.sort_unstable();
    ids
}

async fn snapshot_id(table: &Table) -> i64 {
    table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap()
        .id()
}

async fn index_names(table: &Table) -> Vec<String> {
    let snapshot = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    let Some(name) = snapshot.index_manifest() else {
        return Vec::new();
    };
    let mut names: Vec<_> = IndexManifest::read(
        table.file_io(),
        &table.snapshot_manager().manifest_path(name),
    )
    .await
    .unwrap()
    .into_iter()
    .map(|entry| entry.index_file.file_name)
    .collect();
    names.sort();
    names
}

#[tokio::test]
async fn data_predicate_uses_latest_partial_column_values_and_existing_deletes() {
    let table = table(&[("read.batch-size", "1")]).await;
    seed(&table).await;
    let fields = table.schema().fields_with_read_metadata();
    let pb = PredicateBuilder::new(&fields);
    let mut read = table.new_read_builder();
    read.with_projection(&["id", "_ROW_ID"]).unwrap();
    let plan = read.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = read
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut row_ids = HashMap::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let rows = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            row_ids.insert(ids.value(row), rows.value(row));
        }
    }
    let update = table.new_write_builder().new_update().unwrap();
    let updates = RecordBatch::try_from_iter([
        (
            "_ROW_ID",
            Arc::new(Int64Array::from(vec![row_ids[&0]])) as ArrayRef,
        ),
        ("v", Arc::new(Int32Array::from(vec![99])) as ArrayRef),
    ])
    .unwrap();
    commit(
        &table,
        update
            .update_by_arrow_with_row_id(vec![updates])
            .await
            .unwrap(),
    )
    .await;
    commit(
        &table,
        update.delete_by_row_id(vec![row_ids[&1]]).await.unwrap(),
    )
    .await;
    let baseline = snapshot_id(&table).await;
    let messages = update
        .delete_by_predicate(Some(pb.greater_than("v", Datum::Int(50)).unwrap()))
        .await
        .unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].check_from_snapshot, Some(baseline));
    assert!(messages[0].deleted_files.is_empty());
    assert_eq!(messages[0].new_index_files.len(), 1);
    assert_eq!(messages[0].deleted_index_files.len(), 1);
    assert_eq!(ids(&table).await, vec![0, 2, 3]);
    commit(&table, messages).await;
    assert_eq!(ids(&table).await, vec![2, 3]);
    assert!(update
        .delete_by_predicate(Some(pb.greater_than("v", Datum::Int(50)).unwrap()))
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn partition_predicates_remap_all_leaves_and_delete_without_dv() {
    for (predicate, expected) in [
        (0, vec![2]),
        (1, vec![2, 3]),
        (2, vec![0, 1, 2]),
        (3, vec![0, 1]),
    ] {
        let table = table(&[("deletion-vectors.enabled", "false")]).await;
        seed(&table).await;
        let pb = PredicateBuilder::new(table.schema().fields());
        let predicate = match predicate {
            0 => Predicate::or(vec![
                pb.equal("p", Datum::String("a".into())).unwrap(),
                pb.is_null("p").unwrap(),
            ]),
            1 => Predicate::and(vec![
                pb.equal("q", Datum::Int(1)).unwrap(),
                pb.equal("p", Datum::String("a".into())).unwrap(),
            ]),
            2 => pb.is_null("p").unwrap(),
            _ => Predicate::negate(pb.equal("q", Datum::Int(1)).unwrap()),
        };
        let baseline = snapshot_id(&table).await;
        let messages = table
            .new_write_builder()
            .new_update()
            .unwrap()
            .delete_by_predicate(Some(predicate))
            .await
            .unwrap();
        assert!(messages
            .iter()
            .all(|message| message.check_from_snapshot == Some(baseline)
                && message.new_index_files.is_empty()
                && !message.deleted_files.is_empty()));
        assert_eq!(ids(&table).await, vec![0, 1, 2, 3]);
        commit(&table, messages).await;
        assert_eq!(ids(&table).await, expected);
    }
}

#[tokio::test]
async fn mixed_and_or_and_not_predicates_delete_rows_not_whole_partitions() {
    for predicate_kind in 0..3 {
        let table = table(&[]).await;
        seed(&table).await;
        let pb = PredicateBuilder::new(table.schema().fields());
        let partition = pb.equal("p", Datum::String("a".into())).unwrap();
        let data = pb.equal("v", Datum::Int(20)).unwrap();
        let (predicate, expected) = match predicate_kind {
            0 => (Predicate::and(vec![partition, data]), vec![0, 2, 3]),
            1 => (
                Predicate::or(vec![partition, pb.equal("v", Datum::Int(40)).unwrap()]),
                vec![2],
            ),
            _ => (
                Predicate::negate(Predicate::and(vec![partition, data])),
                vec![1],
            ),
        };
        let messages = table
            .new_write_builder()
            .new_update()
            .unwrap()
            .delete_by_predicate(Some(predicate))
            .await
            .unwrap();
        assert!(messages
            .iter()
            .all(|message| message.deleted_files.is_empty()));
        commit(&table, messages).await;
        assert_eq!(ids(&table).await, expected);
    }
}

#[tokio::test]
async fn data_delete_requires_dv_before_empty_or_no_match_shortcuts() {
    for seeded in [false, true] {
        let table = table(&[("deletion-vectors.enabled", "false")]).await;
        if seeded {
            seed(&table).await;
        }
        let pb = PredicateBuilder::new(table.schema().fields());
        for predicate in [
            None,
            Some(pb.equal("id", Datum::Int(999)).unwrap()),
            Some(Predicate::and(vec![
                pb.equal("p", Datum::String("a".into())).unwrap(),
                pb.equal("id", Datum::Int(999)).unwrap(),
            ])),
        ] {
            let error = table
                .new_write_builder()
                .new_update()
                .unwrap()
                .delete_by_predicate(predicate)
                .await
                .unwrap_err();
            assert!(
                error.to_string().contains("deletion-vectors.enabled"),
                "{error}"
            );
        }
    }
}

#[tokio::test]
async fn empty_table_no_match_and_delete_all() {
    let table = table(&[]).await;
    let update = table.new_write_builder().new_update().unwrap();
    assert!(update.delete_by_predicate(None).await.unwrap().is_empty());
    seed(&table).await;
    let pb = PredicateBuilder::new(table.schema().fields());
    for predicate in [
        pb.equal("id", Datum::Int(999)).unwrap(),
        pb.equal("p", Datum::String("missing".into())).unwrap(),
    ] {
        assert!(update
            .delete_by_predicate(Some(predicate))
            .await
            .unwrap()
            .is_empty());
    }
    commit(&table, update.delete_by_predicate(None).await.unwrap()).await;
    assert!(ids(&table).await.is_empty());
    assert!(update.delete_by_predicate(None).await.unwrap().is_empty());
}

#[tokio::test]
async fn partition_delete_removes_dv_and_global_indexes_and_retains_other_partition_indexes() {
    let table = table(&[("global-index.enabled", "true")]).await;
    seed(&table).await;
    table
        .copy_with_options(HashMap::from([(
            "deletion-vectors.enabled".into(),
            "false".into(),
        )]))
        .new_global_index_build_builder()
        .with_index_column("v")
        .with_index_type("bitmap")
        .execute()
        .await
        .unwrap();
    let pb = PredicateBuilder::new(table.schema().fields());
    let update = table.new_write_builder().new_update().unwrap();
    commit(
        &table,
        update
            .delete_by_predicate(Some(pb.equal("id", Datum::Int(0)).unwrap()))
            .await
            .unwrap(),
    )
    .await;
    let before = index_names(&table).await;
    let messages = update
        .delete_by_predicate(Some(pb.equal("p", Datum::String("a".into())).unwrap()))
        .await
        .unwrap();
    let deleted: Vec<_> = messages
        .iter()
        .flat_map(|message| &message.deleted_index_files)
        .map(|index| index.file_name.clone())
        .collect();
    assert_eq!(deleted.len(), 2);
    commit(&table, messages).await;
    let mut expected: Vec<_> = before
        .into_iter()
        .filter(|name| !deleted.contains(name))
        .collect();
    expected.sort();
    assert_eq!(index_names(&table).await, expected);
    assert_eq!(ids(&table).await, vec![2, 3]);
}

#[tokio::test]
async fn data_predicate_delete_includes_unindexed_rows_in_partial_and_detail_modes() {
    for mode in ["PARTIAL", "DETAIL"] {
        let table = table(&[("global-index.enabled", "true")]).await;
        seed(&table).await;
        table
            .copy_with_options(HashMap::from([(
                "deletion-vectors.enabled".into(),
                "false".into(),
            )]))
            .new_global_index_build_builder()
            .with_index_column("v")
            .with_index_type("bitmap")
            .execute()
            .await
            .unwrap();
        write_batch(&table, &batch(vec![4], vec![Some("a")], vec![20], vec![1])).await;
        let table = table.copy_with_options(HashMap::from([(
            "scalar-index.search-mode".into(),
            mode.into(),
        )]));
        let pb = PredicateBuilder::new(table.schema().fields());
        let messages = table
            .new_write_builder()
            .new_update()
            .unwrap()
            .delete_by_predicate(Some(pb.equal("v", Datum::Int(20)).unwrap()))
            .await
            .unwrap();
        commit(&table, messages).await;
        assert_eq!(ids(&table).await, vec![0, 2, 3]);
    }
}

#[tokio::test]
async fn partition_delete_preserves_concurrent_append_in_selected_partition() {
    let table = table(&[("deletion-vectors.enabled", "false")]).await;
    seed(&table).await;
    let pb = PredicateBuilder::new(table.schema().fields());
    let messages = table
        .new_write_builder()
        .new_update()
        .unwrap()
        .delete_by_predicate(Some(pb.equal("p", Datum::String("a".into())).unwrap()))
        .await
        .unwrap();
    write_batch(&table, &batch(vec![4], vec![Some("a")], vec![50], vec![1])).await;
    commit(&table, messages).await;
    assert_eq!(ids(&table).await, vec![2, 3, 4]);
}

#[tokio::test]
async fn row_delete_rejects_concurrent_dv_replacement() {
    let table = table(&[]).await;
    seed(&table).await;
    let pb = PredicateBuilder::new(table.schema().fields());
    let update = table.new_write_builder().new_update().unwrap();
    let first = update
        .delete_by_predicate(Some(pb.equal("id", Datum::Int(0)).unwrap()))
        .await
        .unwrap();
    commit(
        &table,
        update
            .delete_by_predicate(Some(pb.equal("id", Datum::Int(1)).unwrap()))
            .await
            .unwrap(),
    )
    .await;
    let error = table
        .new_write_builder()
        .new_commit()
        .commit(first)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("Conflicting deletion vectors"),
        "{error}"
    );
    assert_eq!(ids(&table).await, vec![0, 2, 3]);
}

#[tokio::test]
async fn predicate_delete_rejects_time_travel_auth_and_inconsistent_field_indices() {
    let table = table(&[]).await;
    seed(&table).await;
    let pb = PredicateBuilder::new(table.schema().fields());
    let predicate = pb.equal("p", Datum::String("a".into())).unwrap();
    let mut wrong = predicate.clone();
    if let Predicate::Leaf { index, .. } = &mut wrong {
        *index = 3;
    }
    let error = table
        .new_write_builder()
        .new_update()
        .unwrap()
        .delete_by_predicate(Some(wrong))
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("does not match the table schema"));
    let historical =
        table.copy_with_options(HashMap::from([("scan.snapshot-id".into(), "1".into())]));
    assert!(historical.new_write_builder().new_update().is_err());
    let auth = table.copy_with_options(HashMap::from([(
        "query-auth.enabled".into(),
        "true".into(),
    )]));
    let error = auth
        .new_write_builder()
        .new_update()
        .unwrap()
        .delete_by_predicate(Some(predicate))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("query-auth.enabled"));
    assert_eq!(ids(&table).await, vec![0, 1, 2, 3]);
}

#[tokio::test]
async fn partition_deletes_support_append_pk_and_pending_files_without_row_tracking() {
    for bucket in ["-1", "1", "-2"] {
        let table = table_with_primary_keys(
            &[
                ("data-evolution.enabled", "false"),
                ("row-tracking.enabled", "false"),
                ("deletion-vectors.enabled", "false"),
                ("bucket", bucket),
            ],
            bucket != "-1",
        )
        .await;
        seed(&table).await;
        let pb = PredicateBuilder::new(table.schema().fields());
        let messages = table
            .new_write_builder()
            .new_update()
            .unwrap()
            .delete_by_predicate(Some(pb.equal("p", Datum::String("a".into())).unwrap()))
            .await
            .unwrap();
        assert_eq!(messages.len(), 1);
        assert!(!messages[0].deleted_files.is_empty());
        assert_eq!(messages[0].bucket, if bucket == "-2" { -2 } else { 0 });
        commit(&table, messages).await;
        if bucket != "-2" {
            assert_eq!(ids(&table).await, vec![2, 3]);
        } else {
            let latest = table
                .snapshot_manager()
                .get_latest_snapshot()
                .await
                .unwrap()
                .unwrap();
            assert_eq!(latest.total_record_count(), Some(2));
        }
    }
}

type NegateCase = (fn(&PredicateBuilder) -> Predicate, Vec<i32>);

#[tokio::test]
async fn negated_predicates_keep_null_rows() {
    // Seeded id 3 has p = NULL. NOT(p = 'a') is unknown for it, so it must
    // survive, as in Java where the negation is `p <> 'a'`.
    let cases: Vec<NegateCase> = vec![
        (
            |pb| Predicate::negate(pb.equal("p", Datum::String("a".into())).unwrap()),
            vec![0, 1, 3],
        ),
        (
            |pb| {
                Predicate::negate(Predicate::and(vec![
                    pb.equal("q", Datum::Int(2)).unwrap(),
                    pb.equal("p", Datum::String("b".into())).unwrap(),
                ]))
            },
            vec![2, 3],
        ),
        (
            |pb| Predicate::negate(pb.not_equal("p", Datum::String("a".into())).unwrap()),
            vec![2, 3],
        ),
    ];
    for (predicate, expected) in cases {
        // Partition-only predicates drop files; adding a data conjunct goes
        // through deletion vectors.
        for data_conjunct in [false, true] {
            let table = table(&[]).await;
            seed(&table).await;
            let pb = PredicateBuilder::new(&table.schema().fields_with_read_metadata());
            let mut predicate = predicate(&pb);
            if data_conjunct {
                predicate = Predicate::and(vec![
                    predicate,
                    pb.greater_than("v", Datum::Int(0)).unwrap(),
                ]);
            }
            let update = table.new_write_builder().new_update().unwrap();
            commit(
                &table,
                update
                    .delete_by_predicate(Some(predicate.clone()))
                    .await
                    .unwrap(),
            )
            .await;
            assert_eq!(
                ids(&table).await,
                expected,
                "{predicate} data={data_conjunct}"
            );
        }
    }
}

#[tokio::test]
async fn negated_data_predicate_keeps_null_rows() {
    let table = table(&[]).await;
    write_batch(
        &table,
        &RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int32Array::from(vec![0, 1, 2, 3])) as ArrayRef,
            ),
            (
                "p",
                Arc::new(StringArray::from(vec![Some("a"); 4])) as ArrayRef,
            ),
            (
                "v",
                Arc::new(Int32Array::from(vec![Some(10), Some(20), Some(30), None])) as ArrayRef,
            ),
            ("q", Arc::new(Int32Array::from(vec![1; 4])) as ArrayRef),
        ])
        .unwrap(),
    )
    .await;
    let pb = PredicateBuilder::new(&table.schema().fields_with_read_metadata());
    let predicate = Predicate::negate(pb.equal("v", Datum::Int(10)).unwrap());

    let mut read = table.new_read_builder();
    read.with_projection(&["id"]).unwrap();
    read.with_filter(predicate.clone());
    let plan = read.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = read
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut read_ids: Vec<i32> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    read_ids.sort_unstable();
    assert_eq!(read_ids, vec![1, 2]);

    let update = table.new_write_builder().new_update().unwrap();
    commit(
        &table,
        update.delete_by_predicate(Some(predicate)).await.unwrap(),
    )
    .await;
    assert_eq!(ids(&table).await, vec![0, 3]);
}
