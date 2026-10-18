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

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int32Array, Int8Array, RecordBatch};
use arrow_schema::{DataType as ArrowType, Field, Schema as ArrowSchema};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs};
use futures::TryStreamExt;
use paimon::spec::{DataType, IntType, RowKind, Schema, TableSchema, VALUE_KIND_FIELD_NAME};
use paimon::table::Table;

const FIELDS: [&str; 6] = [
    "id",
    "seq_left",
    "left_value",
    "seq_right",
    "right_value",
    "seq_tie",
];

#[derive(Clone, Copy, Debug)]
struct Event {
    values: [Option<i32>; 6],
    kind: RowKind,
}

const INITIAL: Event = Event {
    values: [Some(1), Some(1), Some(10), Some(1), Some(20), Some(1)],
    kind: RowKind::Insert,
};
const DELETED: Event = Event {
    values: [Some(1), Some(2), Some(10), None, None, Some(1)],
    kind: RowKind::Delete,
};
const RESTORED: Event = Event {
    values: [Some(1), Some(3), Some(40), None, None, Some(1)],
    kind: RowKind::Insert,
};

fn event(
    kind: RowKind,
    left: (Option<i32>, Option<i32>),
    right: (Option<i32>, Option<i32>),
    tie: Option<i32>,
) -> Event {
    Event {
        values: [Some(1), left.0, left.1, right.0, right.1, tie],
        kind,
    }
}

fn group_options(composite: bool, aggregate: bool) -> Vec<(&'static str, &'static str)> {
    let mut options = vec![
        (
            if composite {
                "fields.seq_left,seq_tie.sequence-group"
            } else {
                "fields.seq_left.sequence-group"
            },
            "left_value",
        ),
        ("fields.seq_right.sequence-group", "right_value"),
        ("partial-update.remove-record-on-sequence-group", "seq_left"),
    ];
    if aggregate {
        options.push(("fields.right_value.aggregate-function", "sum"));
    }
    options
}

async fn table(options: &[(&str, &str)]) -> Table {
    let mut builder = Schema::builder()
        .primary_key(["id"])
        .option("bucket", "1")
        .option("merge-engine", "partial-update");
    for field in FIELDS {
        builder = builder.column(field, DataType::Int(IntType::new()));
    }
    for (key, value) in options {
        builder = builder.option(*key, *value);
    }
    let schema = TableSchema::new(0, &builder.build().unwrap());
    let path = format!("memory:/partial-update-delete-{}", uuid::Uuid::new_v4());
    let (file_io, table) = memory_table(&path, schema);
    setup_dirs(&file_io, &path).await;
    persist_table_schema(&file_io, &path, table.schema()).await;
    table
}

async fn write(table: &Table, events: &[Event]) {
    let mut fields = FIELDS
        .iter()
        .map(|name| Field::new(*name, ArrowType::Int32, *name != "id"))
        .collect::<Vec<_>>();
    fields.push(Field::new(VALUE_KIND_FIELD_NAME, ArrowType::Int8, false));
    let mut columns = (0..FIELDS.len())
        .map(|index| {
            Arc::new(Int32Array::from(
                events
                    .iter()
                    .map(|event| event.values[index])
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        })
        .collect::<Vec<_>>();
    columns.push(Arc::new(Int8Array::from(
        events
            .iter()
            .map(|event| event.kind.to_value())
            .collect::<Vec<_>>(),
    )));
    let batch = RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).unwrap();
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    builder.new_commit().commit(messages).await.unwrap();
}

async fn write_layout(table: &Table, events: &[Event], boundaries: usize) {
    let mut start = 0;
    for end in 1..=events.len() {
        if end == events.len() || boundaries & (1 << (end - 1)) != 0 {
            write(table, &events[start..end]).await;
            start = end;
        }
    }
}

async fn assert_projections(table: &Table, expected: Option<[Option<i32>; 6]>, boundaries: usize) {
    for projection in [FIELDS.to_vec(), vec!["id"], vec!["right_value"], vec![]] {
        let mut builder = table.new_read_builder();
        builder.with_projection(&projection).unwrap();
        let plan = builder.new_scan().plan().await.unwrap();
        let batches: Vec<RecordBatch> = builder
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let mut actual = Vec::new();
        for batch in batches {
            for row_index in 0..batch.num_rows() {
                actual.push(
                    batch
                        .columns()
                        .iter()
                        .map(|column| {
                            let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
                            (!array.is_null(row_index)).then(|| array.value(row_index))
                        })
                        .collect::<Vec<_>>(),
                );
            }
        }
        let expected_rows: Vec<Vec<Option<i32>>> = expected
            .map(|values| {
                vec![projection
                    .iter()
                    .map(|name| values[FIELDS.iter().position(|field| field == name).unwrap()])
                    .collect()]
            })
            .unwrap_or_default();
        assert_eq!(
            actual, expected_rows,
            "boundaries={boundaries:b}, projection={projection:?}"
        );
    }
}

async fn assert_deleted_then_restored(
    options: &[(&str, &str)],
    events: &[Event],
    restored: Event,
    expected: [Option<i32>; 6],
) {
    for boundaries in 0..(1 << (events.len() - 1)) {
        let table = table(options).await;
        write_layout(&table, events, boundaries).await;
        assert_projections(&table, None, boundaries).await;
        write(&table, &[restored]).await;
        assert_projections(&table, Some(expected), boundaries).await;
    }
}

async fn assert_all_layouts(
    options: &[(&str, &str)],
    events: &[Event],
    expected: [Option<i32>; 6],
) {
    for boundaries in 0..(1 << (events.len() - 1)) {
        let table = table(options).await;
        write_layout(&table, events, boundaries).await;
        assert_projections(&table, Some(expected), boundaries).await;
    }
}

#[tokio::test]
async fn delete_state_survives_other_group_updates() {
    for composite in [false, true] {
        for kind in [RowKind::Insert, RowKind::UpdateAfter] {
            let trailing = event(kind, (None, None), (Some(3), Some(30)), None);
            assert_deleted_then_restored(
                &group_options(composite, false),
                &[INITIAL, DELETED, trailing],
                RESTORED,
                [Some(1), Some(3), Some(40), Some(3), Some(30), Some(1)],
            )
            .await;
        }
    }
}

#[tokio::test]
async fn delete_state_survives_old_selected_group_updates() {
    for composite in [false, true] {
        for kind in [RowKind::Insert, RowKind::UpdateAfter] {
            let trailing = event(kind, (Some(1), Some(99)), (None, None), Some(1));
            assert_deleted_then_restored(
                &group_options(composite, false),
                &[INITIAL, DELETED, trailing],
                RESTORED,
                RESTORED.values,
            )
            .await;
        }
    }
}

#[tokio::test]
async fn delete_state_survives_subsequent_retractions() {
    for composite in [false, true] {
        for kind in [RowKind::Delete, RowKind::UpdateBefore] {
            for (trailing, right_sequence) in [
                (
                    event(kind, (None, None), (Some(3), Some(30)), None),
                    Some(3),
                ),
                (
                    event(kind, (Some(1), Some(99)), (None, None), Some(1)),
                    None,
                ),
                (event(kind, (None, None), (None, None), None), None),
            ] {
                assert_deleted_then_restored(
                    &group_options(composite, false),
                    &[INITIAL, DELETED, trailing],
                    RESTORED,
                    [Some(1), Some(3), Some(40), right_sequence, None, Some(1)],
                )
                .await;
            }
        }
    }
}

#[tokio::test]
async fn whole_row_delete_ignores_update_before_payload() {
    let before = event(
        RowKind::UpdateBefore,
        (Some(9), Some(99)),
        (Some(9), Some(99)),
        Some(9),
    );
    let restored = event(RowKind::Insert, (None, None), (Some(3), Some(30)), None);
    assert_deleted_then_restored(
        &[("partial-update.remove-record-on-delete", "true")],
        &[INITIAL, DELETED, before],
        restored,
        [Some(1), Some(2), Some(10), Some(3), Some(30), Some(1)],
    )
    .await;
}

#[tokio::test]
async fn accepted_group_updates_restore_all_fields_across_merge_boundaries() {
    let other = event(RowKind::Insert, (None, None), (Some(3), Some(30)), None);
    for composite in [false, true] {
        for kind in [RowKind::Insert, RowKind::UpdateAfter] {
            for (sequence, tie) in [(2, 1), (2, 2), (3, 0)] {
                let restored = event(kind, (Some(sequence), Some(40)), (None, None), Some(tie));
                let old_delete = event(RowKind::Delete, (Some(1), Some(99)), (None, None), Some(1));
                assert_all_layouts(
                    &group_options(composite, false),
                    &[INITIAL, DELETED, other, restored, old_delete],
                    [
                        Some(1),
                        Some(sequence),
                        Some(40),
                        Some(3),
                        Some(30),
                        Some(tie),
                    ],
                )
                .await;
            }
        }
    }
}

#[tokio::test]
async fn deleted_aggregate_state_survives_merge_boundaries() {
    let other = event(RowKind::Insert, (None, None), (Some(3), Some(30)), None);
    let retract = event(RowKind::Delete, (None, None), (Some(4), Some(5)), None);
    for composite in [false, true] {
        assert_deleted_then_restored(
            &group_options(composite, true),
            &[INITIAL, DELETED, other, retract],
            RESTORED,
            [Some(1), Some(3), Some(40), Some(4), Some(25), Some(1)],
        )
        .await;
    }
}

#[tokio::test]
async fn ignore_delete_preserves_existing_group_values() {
    let mut options = group_options(true, false);
    options.retain(|(key, _)| *key != "partial-update.remove-record-on-sequence-group");
    options.push(("ignore-delete", "true"));
    let other = event(RowKind::Insert, (None, None), (Some(3), Some(30)), None);
    assert_all_layouts(
        &options,
        &[INITIAL, DELETED, other],
        [Some(1), Some(1), Some(10), Some(3), Some(30), Some(1)],
    )
    .await;
}
