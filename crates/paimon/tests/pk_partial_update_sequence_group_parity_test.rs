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

//! Persisted parity for Java `PartialUpdateMergeFunctionTest` sequence-group
//! cases. These exercise the write buffer and independent files, then read
//! through both full and projected table readers.

#[path = "common/rowkind_helpers.rs"]
mod helpers;

use std::sync::Arc;

use arrow_array::{Array, Int32Array, Int8Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use futures::StreamExt;
use helpers::{memory_table, persist_table_schema, setup_dirs, write_batch};
use paimon::spec::{DataType, IntType, Schema, TableSchema, VALUE_KIND_FIELD_NAME};
use paimon::table::Table;

#[derive(Clone, Copy, Debug)]
struct Update {
    seq_a: Option<i32>,
    seq_b: Option<i32>,
    amount: Option<i32>,
    first: Option<i32>,
    last: Option<i32>,
    aux_seq: Option<i32>,
    aux_value: Option<i32>,
    free: Option<i32>,
}

impl Update {
    fn new(
        seq: (Option<i32>, Option<i32>),
        grouped: (Option<i32>, Option<i32>, Option<i32>),
        aux: (Option<i32>, Option<i32>),
        free: Option<i32>,
    ) -> Self {
        Self {
            seq_a: seq.0,
            seq_b: seq.1,
            amount: grouped.0,
            first: grouped.1,
            last: grouped.2,
            aux_seq: aux.0,
            aux_value: aux.1,
            free,
        }
    }
}

fn batch(updates: &[Update]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(
            [
                "id",
                "seq_a",
                "seq_b",
                "amount",
                "first",
                "last",
                "aux_seq",
                "aux_value",
                "free",
            ]
            .into_iter()
            .map(|name| ArrowField::new(name, ArrowDataType::Int32, name != "id"))
            .collect::<Vec<_>>(),
        )),
        vec![
            Arc::new(Int32Array::from(vec![1; updates.len()])),
            Arc::new(Int32Array::from(
                updates.iter().map(|row| row.seq_a).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                updates.iter().map(|row| row.seq_b).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                updates.iter().map(|row| row.amount).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                updates.iter().map(|row| row.first).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                updates.iter().map(|row| row.last).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                updates.iter().map(|row| row.aux_seq).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                updates.iter().map(|row| row.aux_value).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                updates.iter().map(|row| row.free).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

async fn table(path: &str) -> Table {
    let mut builder = Schema::builder().column("id", DataType::Int(IntType::new()));
    for field in [
        "seq_a",
        "seq_b",
        "amount",
        "first",
        "last",
        "aux_seq",
        "aux_value",
        "free",
    ] {
        builder = builder.column(field, DataType::Int(IntType::new()));
    }
    let schema = builder
        .primary_key(["id"])
        .option("bucket", "1")
        .option("merge-engine", "partial-update")
        .option("fields.seq_a,seq_b.sequence-group", "amount,first,last")
        .option("fields.aux_seq.sequence-group", "aux_value")
        .option("fields.amount.aggregate-function", "sum")
        .option("fields.first.aggregate-function", "first_value")
        .option("fields.last.aggregate-function", "last_value")
        .option("fields.aux_value.aggregate-function", "last_non_null_value")
        .build()
        .unwrap();
    let (file_io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&file_io, path).await;
    persist_table_schema(&file_io, path, table.schema()).await;
    table
}

async fn scan_optional(
    table: &Table,
    projection: Option<&[&str]>,
) -> Option<Vec<(String, Option<i32>)>> {
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let mut builder = table.new_read_builder();
    if let Some(projection) = projection {
        builder.with_projection(projection).unwrap();
    }
    let mut stream = builder.new_read().unwrap().to_arrow(plan.splits()).unwrap();
    let batch = stream.next().await?.unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert!(stream.next().await.is_none());
    Some(
        batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let column = batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                (
                    field.name().clone(),
                    (!column.is_null(0)).then(|| column.value(0)),
                )
            })
            .collect(),
    )
}

async fn scan_one(table: &Table, projection: Option<&[&str]>) -> Vec<(String, Option<i32>)> {
    scan_optional(table, projection)
        .await
        .expect("expected one current-state row")
}

#[tokio::test]
async fn composite_sequence_group_aggregates_older_inputs_and_independent_groups() {
    // The three updates mirror Java's multi-sequence first/last test, with an
    // additional SUM and a second independent sequence group.
    let inputs = [
        Update::new(
            (Some(1), Some(1)),
            (Some(1), Some(1), Some(1)),
            (Some(1), Some(1)),
            Some(10),
        ),
        Update::new(
            (Some(2), Some(2)),
            (Some(1), Some(2), Some(2)),
            (Some(0), Some(2)),
            Some(20),
        ),
        Update::new(
            (Some(0), Some(1)),
            (Some(3), Some(3), Some(3)),
            (Some(2), None),
            Some(30),
        ),
    ];
    for separate_commits in [false, true] {
        let path = format!("memory:/partial_update_parity/group_{separate_commits}");
        let table = table(&path).await;
        if separate_commits {
            for input in inputs {
                write_batch(&table, &batch(&[input])).await;
            }
        } else {
            write_batch(&table, &batch(&inputs)).await;
        }
        let actual = scan_one(&table, None).await;
        assert_eq!(
            actual,
            vec![
                ("id".into(), Some(1)),
                ("seq_a".into(), Some(2)),
                ("seq_b".into(), Some(2)),
                ("amount".into(), Some(5)),
                ("first".into(), Some(3)),
                ("last".into(), Some(2)),
                ("aux_seq".into(), Some(2)),
                ("aux_value".into(), Some(1)),
                ("free".into(), Some(30)),
            ],
            "separate_commits={separate_commits}"
        );
        let projected = scan_one(
            &table,
            Some(&["id", "amount", "first", "last", "aux_value", "free"]),
        )
        .await;
        assert_eq!(
            projected,
            vec![
                ("id".into(), Some(1)),
                ("amount".into(), Some(5)),
                ("first".into(), Some(3)),
                ("last".into(), Some(2)),
                ("aux_value".into(), Some(1)),
                ("free".into(), Some(30)),
            ],
            "projection, separate_commits={separate_commits}"
        );
    }
}

async fn partial_delete_table(path: &str) -> Table {
    let mut builder = Schema::builder().column("id", DataType::Int(IntType::new()));
    for field in ["a", "b", "seq_left", "c", "d", "seq_right"] {
        builder = builder.column(field, DataType::Int(IntType::new()));
    }
    let schema = builder
        .primary_key(["id"])
        .option("bucket", "1")
        .option("merge-engine", "partial-update")
        .option("fields.seq_left.sequence-group", "a,b")
        .option("fields.seq_right.sequence-group", "c,d")
        .option(
            "partial-update.remove-record-on-sequence-group",
            "seq_right",
        )
        .build()
        .unwrap();
    let (file_io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&file_io, path).await;
    persist_table_schema(&file_io, path, table.schema()).await;
    table
}

fn partial_delete_batch(values: [Option<i32>; 6], kind: i8) -> RecordBatch {
    let mut columns: Vec<Arc<dyn Array>> = vec![Arc::new(Int32Array::from(vec![1]))];
    columns.extend(
        values
            .into_iter()
            .map(|value| Arc::new(Int32Array::from(vec![value])) as Arc<dyn Array>),
    );
    columns.push(Arc::new(Int8Array::from(vec![kind])));
    let mut fields = ["id", "a", "b", "seq_left", "c", "d", "seq_right"]
        .into_iter()
        .map(|name| ArrowField::new(name, ArrowDataType::Int32, name != "id"))
        .collect::<Vec<_>>();
    fields.push(ArrowField::new(
        VALUE_KIND_FIELD_NAME,
        ArrowDataType::Int8,
        false,
    ));
    RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).unwrap()
}

#[tokio::test]
async fn sequence_group_delete_matches_java_across_commits_and_projection() {
    // Java PartialUpdateMergeFunctionTest.testSequenceGroupPartialDelete:
    // a delete with a sequence for one group clears that group's fields;
    // another group retains its latest values. The configured right-hand
    // sequence group can later remove the entire record.
    let path = "memory:/partial_update_parity/group_delete";
    let table = partial_delete_table(path).await;
    let steps = [
        ([Some(1), Some(1), Some(1), Some(1), Some(1), Some(1)], 0),
        ([Some(2), Some(2), Some(2), Some(2), Some(2), None], 0),
        ([Some(3), Some(3), Some(1), Some(3), Some(3), Some(3)], 0),
        ([Some(1), Some(1), Some(3), Some(1), Some(1), None], 3),
        ([Some(1), Some(1), Some(3), Some(1), Some(1), Some(4)], 3),
        ([Some(4), Some(4), Some(4), Some(5), Some(5), Some(5)], 0),
        ([Some(1), Some(1), Some(6), Some(1), Some(1), Some(6)], 3),
    ];
    let expected = [
        [Some(1), Some(1), Some(1), Some(1), Some(1), Some(1)],
        [Some(2), Some(2), Some(2), Some(1), Some(1), Some(1)],
        [Some(2), Some(2), Some(2), Some(3), Some(3), Some(3)],
        [None, None, Some(3), Some(3), Some(3), Some(3)],
        [Some(1), Some(1), Some(3), Some(1), Some(1), Some(4)],
        [Some(4), Some(4), Some(4), Some(5), Some(5), Some(5)],
        [Some(1), Some(1), Some(6), Some(1), Some(1), Some(6)],
    ];
    for (step, ((values, kind), expected)) in steps.into_iter().zip(expected).enumerate() {
        write_batch(&table, &partial_delete_batch(values, kind)).await;
        if step == 4 || step == 6 {
            // Java's merge result contains the DELETE payload at this point,
            // but a current-state table scan must hide the tombstone.
            assert!(scan_optional(&table, None).await.is_none());
            assert!(scan_optional(&table, Some(&["id", "b", "d"]))
                .await
                .is_none());
            continue;
        }
        let actual = scan_optional(&table, None)
            .await
            .unwrap_or_else(|| panic!("expected a current-state row at step {step}"));
        let values = actual
            .into_iter()
            .skip(1)
            .map(|(_, value)| value)
            .collect::<Vec<_>>();
        assert_eq!(values, expected, "step {step}");
        let projected = scan_one(&table, Some(&["id", "b", "d"])).await;
        assert_eq!(
            projected,
            vec![
                ("id".into(), Some(1)),
                ("b".into(), expected[1]),
                ("d".into(), expected[4]),
            ],
            "projected step {step}"
        );
    }
}
