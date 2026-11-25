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

//! A partial-update merge that starts with a retraction must not count the
//! retracted value twice (apache/paimon#11273).

mod common;

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, Int8Array, RecordBatch};
use arrow_schema::{DataType as ArrowType, Field, Schema as ArrowSchema};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs};
use futures::TryStreamExt;
use paimon::spec::{
    BigIntType, DataType, IntType, RowKind, Schema, TableSchema, VALUE_KIND_FIELD_NAME,
};
use paimon::table::{CompactResult, Table};
use RowKind::{Insert, UpdateAfter, UpdateBefore};

async fn table(required: bool) -> Table {
    let schema = Schema::builder()
        .primary_key(["id"])
        .option("bucket", "1")
        .option("merge-engine", "partial-update")
        .option("fields.seq.sequence-group", "amount")
        .option("fields.amount.aggregate-function", "sum")
        .column("id", DataType::Int(IntType::with_nullable(false)))
        .column(
            "amount",
            DataType::BigInt(BigIntType::with_nullable(!required)),
        )
        .column("seq", DataType::Int(IntType::new()))
        .build()
        .unwrap();
    let path = format!("memory:/pu-first-retract-{}", uuid::Uuid::new_v4());
    let (file_io, table) = memory_table(&path, TableSchema::new(0, &schema));
    setup_dirs(&file_io, &path).await;
    persist_table_schema(&file_io, &path, table.schema()).await;
    table
}

async fn write(table: &Table, rows: &[(RowKind, i64, Option<i32>)], required: bool) {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", ArrowType::Int32, false),
        Field::new("amount", ArrowType::Int64, !required),
        Field::new("seq", ArrowType::Int32, true),
        Field::new(VALUE_KIND_FIELD_NAME, ArrowType::Int8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1; rows.len()])) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
            Arc::new(Int8Array::from(
                rows.iter().map(|row| row.0.to_value()).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    builder.new_commit().commit(messages).await.unwrap();
}

async fn compact(table: &Table) {
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let executor = builder.new_executor().unwrap();
    let mut results: Vec<CompactResult> = Vec::new();
    for task in plan.tasks() {
        results.push(executor.execute(task).await.unwrap());
    }
    builder
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap();
}

async fn amounts(table: &Table) -> Vec<Option<i64>> {
    let builder = table.new_read_builder();
    let plan = builder.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    batches
        .iter()
        .flat_map(|batch| {
            let amounts = batch
                .column_by_name("amount")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .clone();
            (0..amounts.len()).map(move |row| (!amounts.is_null(row)).then(|| amounts.value(row)))
        })
        .collect()
}

#[tokio::test]
async fn retraction_and_update_in_one_commit() {
    for (required, compacted, amount) in [
        (false, false, 90),
        (false, false, 60),
        (true, false, 60),
        (false, true, 60),
    ] {
        let table = table(required).await;
        write(&table, &[(Insert, 90, Some(1))], required).await;
        if compacted {
            compact(&table).await;
        }
        write(
            &table,
            &[(UpdateBefore, 90, Some(2)), (UpdateAfter, amount, Some(3))],
            required,
        )
        .await;
        assert_eq!(amounts(&table).await, vec![Some(amount)]);
        compact(&table).await;
        assert_eq!(amounts(&table).await, vec![Some(amount)]);
    }
}

#[tokio::test]
async fn retraction_only_commit_before_update() {
    for required in [false, true] {
        let table = table(required).await;
        write(
            &table,
            &[(Insert, 40, Some(1)), (Insert, 50, Some(2))],
            required,
        )
        .await;
        write(
            &table,
            &[(UpdateBefore, 40, Some(3)), (UpdateBefore, 50, Some(4))],
            required,
        )
        .await;
        assert_eq!(amounts(&table).await, vec![Some(0)]);
        write(&table, &[(UpdateAfter, 60, Some(5))], required).await;
        assert_eq!(amounts(&table).await, vec![Some(60)]);
        compact(&table).await;
        assert_eq!(amounts(&table).await, vec![Some(60)]);
    }
}

/// Retractions whose sequence group is skipped leave a required sum unchanged.
#[tokio::test]
async fn required_sum_with_skipped_sequence_group() {
    let table = table(true).await;
    write(&table, &[(Insert, 17, Some(1))], true).await;
    write(
        &table,
        &[(UpdateBefore, 1, None), (UpdateBefore, 2, None)],
        true,
    )
    .await;
    assert_eq!(amounts(&table).await, vec![Some(17)]);
    write(
        &table,
        &[(UpdateBefore, 3, None), (UpdateAfter, 5, Some(2))],
        true,
    )
    .await;
    assert_eq!(amounts(&table).await, vec![Some(22)]);
}
