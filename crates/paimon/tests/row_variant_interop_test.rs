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

#![cfg(all(feature = "storage-fs", not(windows)))]

use arrow_array::cast::AsArray;
use arrow_array::{
    Array, BinaryArray, Int32Array, ListArray, MapArray, RecordBatch, StringArray, StructArray,
};
use arrow_select::concat::concat_batches;
use futures::TryStreamExt;
use paimon::catalog::Identifier;
use paimon::io::FileIOBuilder;
use paimon::spec::{BinaryRow, BinaryTableStats, DataFileMeta, TableSchema};
use paimon::table::DataSplitBuilder;
use paimon::variant::{GenericVariant, VariantKind};
use paimon::Table;
use serde::Deserialize;
use sha2::{Digest, Sha256};

#[derive(Deserialize)]
struct VariantDigest {
    value: String,
    metadata: String,
}

fn assert_variant(array: &StructArray, row: usize, expected: Option<&VariantDigest>) {
    let Some(expected) = expected else {
        assert!(array.is_null(row), "SQL null at row {row}");
        return;
    };
    assert!(array.is_valid(row), "non-null Variant at row {row}");
    for (index, expected) in [&expected.value, &expected.metadata].iter().enumerate() {
        let child = array
            .column(index)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert!(child.is_valid(row));
        assert_eq!(
            &format!("{:x}", Sha256::digest(child.value(row))),
            *expected
        );
    }
}

fn assert_values(batch: &RecordBatch, expected: &[Option<VariantDigest>]) {
    assert_eq!(batch.num_rows(), expected.len());
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let root = batch
        .column(1)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let nested = batch
        .column(2)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let nested_values = nested
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let notes = nested
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let items = batch
        .column(3)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let attrs = batch.column(4).as_any().downcast_ref::<MapArray>().unwrap();
    let extra = batch
        .column(5)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let keys = attrs.keys().as_any().downcast_ref::<StringArray>().unwrap();
    let values = attrs
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let variant_null = expected[2].as_ref();
    assert!(variant_null.is_some());
    for (row, expected) in expected.iter().enumerate() {
        assert_eq!(ids.value(row), row as i32);
        assert_variant(root, row, expected.as_ref());
        assert_variant(extra, row, expected.as_ref());
        assert_eq!(nested.is_null(row), row == 3);
        if nested.is_valid(row) {
            assert_variant(nested_values, row, expected.as_ref());
            assert_eq!(notes.value(row), "ok");
        }
        assert_eq!(items.is_null(row), row == 5);
        if items.is_valid(row) {
            let elements = items.value(row);
            let elements = elements.as_any().downcast_ref::<StructArray>().unwrap();
            assert_eq!(elements.len(), 3);
            assert_variant(elements, 0, expected.as_ref());
            assert_variant(elements, 1, None);
            assert_variant(elements, 2, variant_null);
        }
        assert_eq!(attrs.is_null(row), row == 6);
        if attrs.is_valid(row) {
            let start = attrs.value_offsets()[row] as usize;
            assert_eq!(attrs.value_offsets()[row + 1] as usize - start, 3);
            for (index, key) in ["a", "b", "c"].iter().enumerate() {
                assert_eq!(keys.value(start + index), *key);
            }
            assert_variant(values, start, expected.as_ref());
            assert_variant(values, start + 1, None);
            assert_variant(values, start + 2, variant_null);
        }
    }
}

async fn read_fixture(
    name: &str,
    bytes: &[u8],
    schema: TableSchema,
    row_count: usize,
    temp: &tempfile::TempDir,
) -> (Table, RecordBatch) {
    std::fs::create_dir(temp.path().join("schema")).unwrap();
    std::fs::write(
        temp.path().join("schema/schema-0"),
        serde_json::to_vec(&schema).unwrap(),
    )
    .unwrap();
    std::fs::write(temp.path().join("fixture.row"), bytes).unwrap();
    let location = format!("file://{}", temp.path().display());
    let table = Table::new(
        FileIOBuilder::new("file").build().unwrap(),
        Identifier::new("default", name),
        location.clone(),
        schema,
        None,
    );
    let file = DataFileMeta {
        file_name: "fixture.row".into(),
        file_size: bytes.len() as i64,
        row_count: row_count as i64,
        min_key: Vec::new(),
        max_key: Vec::new(),
        key_stats: BinaryTableStats::empty(),
        value_stats: BinaryTableStats::empty(),
        min_sequence_number: 0,
        max_sequence_number: 8,
        schema_id: 0,
        level: 0,
        extra_files: Vec::new(),
        creation_time: None,
        delete_row_count: Some(0),
        embedded_index: None,
        file_source: None,
        value_stats_cols: None,
        external_path: None,
        first_row_id: None,
        write_cols: None,
        column_max_sequence_numbers: None,
    };
    let split = DataSplitBuilder::new()
        .with_snapshot(1)
        .with_partition(BinaryRow::new(0))
        .with_bucket(0)
        .with_bucket_path(location)
        .with_total_buckets(1)
        .with_data_files(vec![file])
        .with_raw_convertible(true)
        .build()
        .unwrap();
    let batches = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow(&[split])
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let batch = concat_batches(&batches[0].schema(), &batches).unwrap();
    (table, batch)
}

async fn rewrite_fixture(
    table: &Table,
    batch: &RecordBatch,
    name: &str,
    version: u8,
) -> RecordBatch {
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(batch).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].new_files.len(), 1);
    let file = &messages[0].new_files[0];
    let bytes = table
        .file_io()
        .new_input(&file.data_file_path(&format!("{}/bucket-0", table.location())))
        .unwrap()
        .read()
        .await
        .unwrap();
    assert_eq!(bytes[bytes.len() - 8], version);

    // Export an actual Rust file for Java and Python readers when regenerating fixtures.
    if let Some(output) = std::env::var_os("PAIMON_ROW_INTEROP_OUTPUT") {
        let output = std::path::Path::new(&output);
        std::fs::create_dir_all(output).unwrap();
        std::fs::write(output.join(format!("rust_{name}.row")), bytes).unwrap();
    }

    writer.close().await;
    builder.new_commit().commit(messages).await.unwrap();
    let read_builder = table.new_read_builder();
    let plan = read_builder.new_scan().plan().await.unwrap();
    let rewritten = read_builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    concat_batches(&rewritten[0].schema(), &rewritten).unwrap()
}

#[tokio::test]
async fn java_and_python_row_variant_files_round_trip_through_rust() {
    let fixtures: [(&str, &[u8], &str); 2] = [
        (
            "java",
            include_bytes!("fixtures/row_variant_interop/java.row"),
            include_str!("fixtures/row_variant_interop/java.json"),
        ),
        (
            "python",
            include_bytes!("fixtures/row_variant_interop/python.row"),
            include_str!("fixtures/row_variant_interop/python.json"),
        ),
    ];
    for (name, bytes, expected) in fixtures {
        let expected: Vec<Option<VariantDigest>> = serde_json::from_str(expected).unwrap();
        let schema: TableSchema =
            serde_json::from_str(include_str!("fixtures/row_variant_interop/schema.json")).unwrap();
        let temp = tempfile::tempdir().unwrap();
        assert_eq!(bytes[bytes.len() - 8], 1, "ROW v1 fixture");
        let (table, batch) = read_fixture(name, bytes, schema, expected.len(), &temp).await;
        assert_values(&batch, &expected);
        assert_values(&rewrite_fixture(&table, &batch, name, 1).await, &expected);
    }
}

#[tokio::test]
async fn java_and_python_shredded_row_v2_files_round_trip_through_rust() {
    let fixtures = [
        (
            "v2_java",
            include_str!("fixtures/row_variant_interop/java-v2.hex"),
        ),
        (
            "v2_python",
            include_str!("fixtures/row_variant_interop/python-v2.hex"),
        ),
    ];
    let shredding_schema = serde_json::json!({
        "type": "ROW",
        "fields": [{"name": "payload", "type": {
            "type": "ROW", "fields": [
                {"name": "score", "type": "INT"},
                {"name": "ts", "type": "TIMESTAMP(3)"}
            ]
        }}]
    });
    let schema: TableSchema = serde_json::from_value(serde_json::json!({
        "version": 3,
        "id": 0,
        "fields": [
            {"id": 7, "name": "id", "type": "INT"},
            {"id": 11, "name": "tag", "type": "VARCHAR(17)"},
            {"id": 42, "name": "payload", "type": "VARIANT"},
            {"id": 85, "name": "context", "type": {
                "type": "ROW", "fields": [{"id": 97, "name": "code", "type": "VARCHAR(9)"}]
            }}
        ],
        "highestFieldId": 97,
        "partitionKeys": [],
        "primaryKeys": [],
        "options": {
            "file.format": "row",
            "write-only": "true",
            "parquet.variant.shreddingSchema": shredding_schema.to_string()
        }
    }))
    .unwrap();
    for (name, fixture) in fixtures {
        let temp = tempfile::tempdir().unwrap();
        let hex = fixture
            .lines()
            .find(|line| !line.is_empty() && !line.starts_with('#'))
            .unwrap();
        let bytes = hex::decode(hex).unwrap();
        assert_eq!(bytes[bytes.len() - 8], 2, "shredded ROW v2 fixture");
        let (table, batch) = read_fixture(name, &bytes, schema.clone(), 4, &temp).await;
        assert_v2_values(&batch);
        assert_v2_values(&rewrite_fixture(&table, &batch, name, 2).await);
    }
}

fn assert_v2_values(batch: &RecordBatch) {
    assert_eq!(batch.num_rows(), 4);
    let ids = batch
        .column(0)
        .as_primitive::<arrow_array::types::Int32Type>();
    let tags = batch.column(1).as_string::<i32>();
    let payloads = batch.column(2).as_struct();
    let values = payloads.column(0).as_binary::<i32>();
    let metadata = payloads.column(1).as_binary::<i32>();
    let context = batch.column(3).as_struct();
    let codes = context.column(0).as_string::<i32>();
    for (row, id) in (3..=6).enumerate() {
        assert_eq!(ids.value(row), id);
        assert_eq!(tags.value(row), format!("tag-{id}"));
        assert_eq!(context.is_null(row), id == 6);
        if id == 6 {
            assert!(payloads.is_null(row), "SQL NULL");
            continue;
        }
        assert_eq!(codes.value(row), format!("ctx-{id}"));
        assert!(payloads.is_valid(row));
        let variant =
            GenericVariant::from_parts(values.value(row).to_vec(), metadata.value(row).to_vec())
                .unwrap();
        if id == 5 {
            assert_eq!(variant.to_json().unwrap(), "null", "Variant null");
            continue;
        }
        let (score, extra, expected_micros) = if id == 3 {
            ("7", "\"mixed\"", 1_234_000)
        } else {
            ("\"8\"", "\"residual\"", 1_234_567)
        };
        assert_eq!(
            variant
                .get_path("$.score")
                .unwrap()
                .unwrap()
                .to_json()
                .unwrap(),
            score
        );
        assert_eq!(
            variant
                .get_path("$.extra")
                .unwrap()
                .unwrap()
                .to_json()
                .unwrap(),
            extra
        );
        let timestamp = variant.get_path("$.ts").unwrap().unwrap();
        assert_eq!(timestamp.kind().unwrap(), VariantKind::TimestampNtz);
        assert_eq!(timestamp.get_long().unwrap(), expected_micros);
    }
}
