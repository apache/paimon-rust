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

//! Public Table API coverage for partial-update sequence-group + nested_update
//! equal-sequence policy. `overwrite-on-equal=false` is rejected because
//! sequence-group reverse aggregation makes equal-sequence retention
//! flush/commit-group dependent; `true` (and the default) must stay consistent
//! across groupings.

#[path = "common/rowkind_helpers.rs"]
mod helpers;

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int32Array, ListArray, RecordBatch, StructArray};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use arrow_select::concat::concat_batches;
use futures::StreamExt;
use helpers::{memory_table, persist_table_schema, setup_dirs, write_batch};
use paimon::arrow::paimon_type_to_arrow;
use paimon::spec::{ArrayType, DataField, DataType, IntType, RowType, Schema, TableSchema};
use paimon::table::Table;

fn items_type() -> DataType {
    DataType::Array(ArrayType::new(DataType::Row(RowType::new(vec![
        DataField::new(3, "item_id".into(), DataType::Int(IntType::new())),
        DataField::new(4, "amount".into(), DataType::Int(IntType::new())),
    ]))))
}

fn schema(overwrite_on_equal: Option<&str>) -> Result<Schema, paimon::Error> {
    let mut builder = Schema::builder()
        .column("pk", DataType::Int(IntType::new()))
        .column("version", DataType::Int(IntType::new()))
        .column("items", items_type())
        .primary_key(["pk"])
        .option("bucket", "1")
        .option("merge-engine", "partial-update")
        .option("fields.version.sequence-group", "items")
        .option("fields.items.aggregate-function", "nested_update")
        .option("fields.items.nested-key", "item_id")
        .option("fields.items.nested-sequence-field", "item_id");
    if let Some(value) = overwrite_on_equal {
        builder = builder.option("fields.items.nested-sequence-overwrite-on-equal", value);
    }
    builder.build()
}

fn row_batch(version: i32, item_id: i32, amount: i32) -> RecordBatch {
    let items_arrow_type = paimon_type_to_arrow(&items_type()).unwrap();
    let ArrowDataType::List(element) = &items_arrow_type else {
        panic!("expected ARRAY<ROW>");
    };
    let ArrowDataType::Struct(fields) = element.data_type() else {
        panic!("expected ROW element");
    };
    let rows = StructArray::try_new(
        fields.clone(),
        vec![
            Arc::new(Int32Array::from(vec![item_id])) as ArrayRef,
            Arc::new(Int32Array::from(vec![amount])) as ArrayRef,
        ],
        None,
    )
    .unwrap();
    let items = ListArray::try_new(
        element.clone(),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
        Arc::new(rows),
        None,
    )
    .unwrap();
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("pk", ArrowDataType::Int32, false),
            ArrowField::new("version", ArrowDataType::Int32, false),
            ArrowField::new("items", items_arrow_type, true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![version])),
            Arc::new(items),
        ],
    )
    .unwrap()
}

async fn open_table(path: &str, overwrite_on_equal: Option<&str>) -> Table {
    let schema = schema(overwrite_on_equal).unwrap();
    let (file_io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&file_io, path).await;
    persist_table_schema(&file_io, path, table.schema()).await;
    table
}

async fn write_grouping(table: &Table, single_commit: bool) {
    let rows = [
        row_batch(3, 1, 30),
        row_batch(1, 1, 10),
        row_batch(2, 1, 20),
    ];
    if single_commit {
        let batch = concat_batches(&rows[0].schema(), &rows).unwrap();
        write_batch(table, &batch).await;
        return;
    }
    for batch in &rows {
        write_batch(table, batch).await;
    }
}

async fn read_amount(table: &Table) -> i32 {
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let mut stream = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap();
    let batch = stream.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    let items = batch
        .column_by_name("items")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(0);
    let rows = items.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(rows.len(), 1);
    let amounts = rows
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert!(stream.next().await.is_none());
    amounts.value(0)
}

#[tokio::test]
async fn overwrite_on_equal_false_rejected_at_schema_create() {
    // Create-time validation rejects the unsafe combination before any write
    // path can observe grouping-dependent equal-sequence retention.
    let err = schema(Some("false")).unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("nested-sequence-overwrite-on-equal=false"),
        "got {message}"
    );
    assert!(message.contains("sequence-group"), "got {message}");
}

#[tokio::test]
async fn overwrite_on_equal_true_consistent_across_commit_groupings() {
    for mode in [None, Some("true")] {
        let label = mode.unwrap_or("unset");
        for single_commit in [true, false] {
            let path = format!("memory:/partial_update_nested_seq/{label}_{single_commit}");
            let table = open_table(&path, mode).await;
            write_grouping(&table, single_commit).await;
            assert_eq!(
                read_amount(&table).await,
                30,
                "mode={label}, single_commit={single_commit}"
            );
        }
    }
}
