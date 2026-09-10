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

//! Regression coverage for schema evolution *inside* a ROW column.
//!
//! Mirrors a primary-key table whose `ALTER TABLE ... ADD COLUMN
//! media.color_transfer` landed as a new schema version: data files written
//! before the change carry a struct with fewer children than the table schema
//! declares. Reading them must fill the added child with NULL instead of
//! failing, which is what Java does via
//! `SchemaEvolutionUtil.createRowCastExecutor`.

// Gated off Windows: the fixture table location is a `file://` URL built from a
// temp dir path, which `FileIO` cannot derive on Windows (see #397). The sibling
// `pk_vector_baseline_test` / `rest_catalog_test` gate their `file://` tempdir
// tests the same way.
#![cfg(not(windows))]

use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use bytes::Bytes;
use futures::TryStreamExt;
use paimon::catalog::Identifier;
use paimon::io::{FileIO, FileIOBuilder};
use paimon::table::{SchemaManager, Table, TableCommit};
use std::sync::Arc;

/// schema-0: `id INT NOT NULL` (pk) + `media ROW<codec STRING>`.
fn schema_v0() -> serde_json::Value {
    serde_json::json!({
        "version": 3,
        "id": 0,
        "fields": [
            {"id": 0, "name": "id", "type": "INT NOT NULL"},
            {"id": 1, "name": "media", "type": {
                "type": "ROW",
                "fields": [{"id": 2, "name": "codec", "type": "STRING"}]
            }}
        ],
        "highestFieldId": 2,
        "partitionKeys": [],
        "primaryKeys": ["id"],
        "options": {"bucket": "1"},
        "timeMillis": 1_700_000_000_000i64
    })
}

/// schema-1: adds `media.color_transfer STRING` (field id 3) inside the ROW.
fn schema_v1() -> serde_json::Value {
    serde_json::json!({
        "version": 3,
        "id": 1,
        "fields": [
            {"id": 0, "name": "id", "type": "INT NOT NULL"},
            {"id": 1, "name": "media", "type": {
                "type": "ROW",
                "fields": [
                    {"id": 2, "name": "codec", "type": "STRING"},
                    {"id": 3, "name": "color_transfer", "type": "STRING"}
                ]
            }}
        ],
        "highestFieldId": 3,
        "partitionKeys": [],
        "primaryKeys": ["id"],
        "options": {"bucket": "1"},
        "timeMillis": 1_700_000_001_000i64
    })
}

async fn write_schema(file_io: &FileIO, location: &str, schema: serde_json::Value) {
    let id = schema.get("id").and_then(|v| v.as_i64()).unwrap();
    file_io
        .new_output(&format!("{location}/schema/schema-{id}"))
        .unwrap()
        .write(Bytes::from(serde_json::to_vec(&schema).unwrap()))
        .await
        .unwrap();
}

async fn open_table(file_io: &FileIO, location: &str) -> Table {
    let schema = SchemaManager::new(file_io.clone(), location.to_string())
        .latest()
        .await
        .expect("failed to list schemas")
        .expect("table has no schema");
    Table::new(
        file_io.clone(),
        Identifier::new("default", "nested_evo"),
        location.to_string(),
        (*schema).clone(),
        None,
    )
}

/// One row whose `media` struct has only `codec` (the schema-0 shape).
fn batch_v0(id: i32, codec: &str) -> RecordBatch {
    let media_fields = vec![ArrowField::new("codec", ArrowDataType::Utf8, true)];
    let media: ArrayRef = Arc::new(StructArray::from(vec![(
        Arc::new(media_fields[0].clone()),
        Arc::new(StringArray::from(vec![Some(codec)])) as ArrayRef,
    )]));
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("media", ArrowDataType::Struct(media_fields.into()), true),
    ]));
    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![id])), media]).unwrap()
}

/// One row whose `media` struct has both children (the schema-1 shape).
fn batch_v1(id: i32, codec: &str, color_transfer: &str) -> RecordBatch {
    let media_fields = vec![
        ArrowField::new("codec", ArrowDataType::Utf8, true),
        ArrowField::new("color_transfer", ArrowDataType::Utf8, true),
    ];
    let media: ArrayRef = Arc::new(StructArray::from(vec![
        (
            Arc::new(media_fields[0].clone()),
            Arc::new(StringArray::from(vec![Some(codec)])) as ArrayRef,
        ),
        (
            Arc::new(media_fields[1].clone()),
            Arc::new(StringArray::from(vec![Some(color_transfer)])) as ArrayRef,
        ),
    ]));
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("media", ArrowDataType::Struct(media_fields.into()), true),
    ]));
    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![id])), media]).unwrap()
}

async fn commit(table: &Table, batch: &RecordBatch) {
    let write_builder = table.new_write_builder();
    let mut writer = write_builder.new_write().unwrap();
    writer.write_arrow_batch(batch).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    TableCommit::new(table.clone(), "nested-evo".to_string())
        .commit(messages)
        .await
        .unwrap();
}

#[tokio::test]
async fn added_nested_field_reads_as_null_in_older_files() {
    let tmp = tempfile::tempdir().unwrap();
    let location = format!("file://{}", tmp.path().display());
    let file_io = FileIOBuilder::new("file").build().unwrap();
    for dir in ["schema", "snapshot", "manifest"] {
        file_io.mkdirs(&format!("{location}/{dir}")).await.unwrap();
    }

    // Snapshot 1 under schema-0: `media` has only `codec`.
    write_schema(&file_io, &location, schema_v0()).await;
    let table_v0 = open_table(&file_io, &location).await;
    assert_eq!(table_v0.schema().id(), 0);
    commit(&table_v0, &batch_v0(1, "h264")).await;

    // ALTER TABLE ... ADD COLUMN media.color_transfer -> schema-1, then a second
    // snapshot whose file carries the wider struct.
    write_schema(&file_io, &location, schema_v1()).await;
    let table_v1 = open_table(&file_io, &location).await;
    assert_eq!(table_v1.schema().id(), 1);
    commit(&table_v1, &batch_v1(2, "h265", "bt709")).await;

    // Full read across both files.
    let read_builder = table_v1.new_read_builder();
    let scan = read_builder.new_scan();
    let plan = scan.plan().await.unwrap();
    let read = read_builder.new_read().unwrap();
    let stream = read.to_arrow(plan.splits()).unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

    let mut rows: Vec<(i32, String, Option<String>)> = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(batch.schema().index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .clone();
        let media = batch
            .column(batch.schema().index_of("media").unwrap())
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .clone();
        let codec = media
            .column_by_name("codec")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        let color = media
            .column_by_name("color_transfer")
            .expect("color_transfer must be present in the read output")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        for i in 0..batch.num_rows() {
            rows.push((
                ids.value(i),
                codec.value(i).to_string(),
                if color.is_null(i) {
                    None
                } else {
                    Some(color.value(i).to_string())
                },
            ));
        }
    }
    rows.sort_by_key(|r| r.0);

    assert_eq!(
        rows,
        vec![
            (1, "h264".to_string(), None),
            (2, "h265".to_string(), Some("bt709".to_string())),
        ]
    );
}
