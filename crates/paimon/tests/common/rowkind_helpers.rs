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

//! Minimal helpers for `rowkind.field` integration tests (no compact APIs).

#![allow(dead_code)]

use arrow_array::{Array, Int32Array, Int8Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use futures::StreamExt;
use paimon::catalog::Identifier;
use paimon::io::FileIOBuilder;
use paimon::spec::{DataType, IntType, Schema, TableSchema, VarCharType, VALUE_KIND_FIELD_NAME};
use paimon::table::Table;
use std::collections::BTreeMap;
use std::sync::Arc;

pub async fn setup_dirs(file_io: &paimon::io::FileIO, table_path: &str) {
    file_io
        .mkdirs(&format!("{table_path}/schema"))
        .await
        .unwrap();
    file_io
        .mkdirs(&format!("{table_path}/snapshot"))
        .await
        .unwrap();
}

pub async fn persist_table_schema(
    file_io: &paimon::io::FileIO,
    table_path: &str,
    schema: &TableSchema,
) {
    use bytes::Bytes;

    let path = format!("{table_path}/schema/schema-{}", schema.id());
    let json = serde_json::to_vec(schema).unwrap();
    file_io
        .new_output(&path)
        .unwrap()
        .write(Bytes::from(json))
        .await
        .unwrap();
}

pub fn rowkind_field_schema(kind_col: &str, options: &[(&str, &str)]) -> TableSchema {
    let mut builder = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .column(kind_col, DataType::VarChar(VarCharType::string_type()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("rowkind.field", kind_col);
    for (k, v) in options {
        builder = builder.option(*k, *v);
    }
    TableSchema::new(0, &builder.build().unwrap())
}

pub fn memory_table(path: &str, schema: TableSchema) -> (paimon::io::FileIO, Table) {
    let file_io = FileIOBuilder::new("memory").build().unwrap();
    let table = Table::new(
        file_io.clone(),
        Identifier::new("default", "rowkind_test"),
        path.to_string(),
        schema,
        None,
    );
    (file_io, table)
}

pub fn make_batch_with_rowkind(
    ids: Vec<i32>,
    values: Vec<i32>,
    kinds: Vec<&str>,
    kind_col: &str,
) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("value", ArrowDataType::Int32, false),
        ArrowField::new(kind_col, ArrowDataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(Int32Array::from(values)),
            Arc::new(StringArray::from(kinds)),
        ],
    )
    .unwrap()
}

pub fn make_batch_with_rowkind_and_value_kind(
    ids: Vec<i32>,
    values: Vec<i32>,
    kinds: Vec<&str>,
    kind_col: &str,
) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("value", ArrowDataType::Int32, false),
        ArrowField::new(kind_col, ArrowDataType::Utf8, false),
        ArrowField::new(VALUE_KIND_FIELD_NAME, ArrowDataType::Int8, false),
    ]));
    let n = kinds.len();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(Int32Array::from(values)),
            Arc::new(StringArray::from(kinds)),
            Arc::new(Int8Array::from(vec![0i8; n])),
        ],
    )
    .unwrap()
}

pub async fn write_batch(table: &Table, batch: &RecordBatch) {
    let builder = table.new_write_builder();
    let mut w = builder.new_write().unwrap();
    w.write_arrow_batch(batch).await.unwrap();
    let msgs = w.prepare_commit().await.unwrap();
    builder.new_commit().commit(msgs).await.unwrap();
}

pub async fn write_batch_expect_err(table: &Table, batch: &RecordBatch) -> paimon::Error {
    let builder = table.new_write_builder();
    let mut w = builder.new_write().unwrap();
    w.write_arrow_batch(batch).await.unwrap_err()
}

pub async fn scan_id_values(table: &Table) -> BTreeMap<i32, i32> {
    let scan = table.new_read_builder().new_scan();
    let plan = scan.plan().await.unwrap();
    let read = table.new_read_builder().new_read().unwrap();
    let mut stream = read.to_arrow(plan.splits()).unwrap();
    let mut out = BTreeMap::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..ids.len() {
            out.insert(ids.value(i), values.value(i));
        }
    }
    out
}

pub async fn scan_pk_value_kind(table: &Table, kind_col: &str) -> Vec<(i32, i32, String)> {
    let scan = table.new_read_builder().new_scan();
    let plan = scan.plan().await.unwrap();
    let read = table.new_read_builder().new_read().unwrap();
    let mut stream = read.to_arrow(plan.splits()).unwrap();
    let mut out = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let kinds = batch
            .column_by_name(kind_col)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..ids.len() {
            out.push((ids.value(i), values.value(i), kinds.value(i).to_string()));
        }
    }
    out.sort_unstable();
    out
}
