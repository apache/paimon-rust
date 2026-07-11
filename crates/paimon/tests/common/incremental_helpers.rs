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

//! Minimal helpers for batch incremental scan tests (no compact/lookup APIs).

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use paimon::catalog::Identifier;
use paimon::io::FileIOBuilder;
use paimon::spec::{DataType, IntType, Schema, TableSchema, VarCharType};
use paimon::table::Table;
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

/// Primary-key schema `(id, value)` with caller-supplied options.
pub fn pk_schema(options: &[(&str, &str)]) -> TableSchema {
    let mut builder = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "1");
    for (k, v) in options {
        builder = builder.option(*k, *v);
    }
    TableSchema::new(0, &builder.build().unwrap())
}

pub fn partitioned_pk_schema(bucket: &str) -> TableSchema {
    let schema = Schema::builder()
        .column("pt", DataType::VarChar(VarCharType::string_type()))
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .partition_keys(["pt"])
        .option("bucket", bucket)
        .option("target-file-size", "1b")
        .option("num-sorted-run.compaction-trigger", "2")
        .build()
        .unwrap();
    TableSchema::new(0, &schema)
}

pub fn memory_table(path: &str, schema: TableSchema) -> (paimon::io::FileIO, Table) {
    let file_io = FileIOBuilder::new("memory").build().unwrap();
    let table = Table::new(
        file_io.clone(),
        Identifier::new("default", "incremental_test"),
        path.to_string(),
        schema,
        None,
    );
    (file_io, table)
}

pub fn make_batch(ids: Vec<i32>, values: Vec<i32>) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("value", ArrowDataType::Int32, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(Int32Array::from(values)),
        ],
    )
    .unwrap()
}

pub fn make_partitioned_batch(pts: Vec<&str>, ids: Vec<i32>, values: Vec<i32>) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("pt", ArrowDataType::Utf8, false),
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("value", ArrowDataType::Int32, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(pts)),
            Arc::new(Int32Array::from(ids)),
            Arc::new(Int32Array::from(values)),
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

pub async fn write_partitioned(table: &Table, batch: RecordBatch) {
    let builder = table.new_write_builder();
    let mut w = builder.new_write().unwrap();
    w.write_arrow_batch(&batch).await.unwrap();
    let msgs = w.prepare_commit().await.unwrap();
    builder.new_commit().commit(msgs).await.unwrap();
}
