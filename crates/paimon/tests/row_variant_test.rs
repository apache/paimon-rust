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

use arrow_array::{
    Array, ArrayRef, BinaryArray, Int32Array, RecordBatch, StringArray, StructArray,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use arrow_select::concat::concat_batches;
use futures::TryStreamExt;
use paimon::arrow::variant_arrow_type;
use paimon::catalog::{Catalog, Identifier};
use paimon::common::Options;
use paimon::spec::{
    variant_extraction_row, DataField, DataType, IntType, RowType, Schema, VarCharType, VariantType,
};
use paimon::variant::GenericVariant;
use paimon::{CatalogOptions, FileSystemCatalog, Table};
use std::collections::HashMap;
use std::sync::Arc;

async fn create_table(kind: &str) -> (tempfile::TempDir, Table) {
    let temp = tempfile::tempdir().unwrap();
    let mut options = Options::new();
    options.set(
        CatalogOptions::WAREHOUSE,
        format!("file://{}", temp.path().display()),
    );
    let catalog = FileSystemCatalog::new(options).unwrap();
    catalog
        .create_database("default", true, HashMap::new())
        .await
        .unwrap();
    let mut schema = Schema::builder()
        .option("write-only", "true")
        .column("id", DataType::Int(IntType::new()))
        .column("payload", DataType::Variant(VariantType::new()))
        .column(
            "nested",
            DataType::Row(RowType::new(vec![
                DataField::new(0, "payload".into(), DataType::Variant(VariantType::new())),
                DataField::new(
                    1,
                    "tag".into(),
                    DataType::VarChar(VarCharType::string_type()),
                ),
            ])),
        )
        .option("file.format", "row");
    if kind == "primary_key" {
        schema = schema.primary_key(["id"]).option("bucket", "1");
    } else if kind == "data_evolution" {
        schema = schema
            .option("row-tracking.enabled", "true")
            .option("data-evolution.enabled", "true");
    }
    let identifier = Identifier::new("default", kind);
    catalog
        .create_table(&identifier, schema.build().unwrap(), false)
        .await
        .unwrap();
    let table = catalog.get_table(&identifier).await.unwrap();
    (temp, table)
}

fn variants(values: &[Option<GenericVariant>]) -> StructArray {
    let ArrowDataType::Struct(fields) = variant_arrow_type() else {
        unreachable!()
    };
    StructArray::new(
        fields,
        vec![
            Arc::new(BinaryArray::from_iter_values(values.iter().map(|value| {
                value.as_ref().map_or(&[][..], GenericVariant::value)
            }))),
            Arc::new(BinaryArray::from_iter_values(values.iter().map(|value| {
                value.as_ref().map_or(&[][..], GenericVariant::metadata)
            }))),
        ],
        Some(NullBuffer::from(
            values.iter().map(Option::is_some).collect::<Vec<_>>(),
        )),
    )
}

fn input_batch() -> (RecordBatch, Vec<Option<GenericVariant>>) {
    let values = [
        Some(r#"{"x":1,"nested":{"items":[true,null,"text"]}}"#),
        Some(r#"[1,{"x":"value"},null]"#),
        Some("null"),
        None,
        Some(r#""scalar""#),
    ]
    .into_iter()
    .map(|json| json.map(|json| GenericVariant::parse_json(json).unwrap()))
    .collect::<Vec<_>>();
    let nested_values = vec![
        values[0].clone(),
        None,
        values[2].clone(),
        values[1].clone(),
        values[4].clone(),
    ];
    let nested = StructArray::new(
        vec![
            Arc::new(Field::new("payload", variant_arrow_type(), true)),
            Arc::new(Field::new("tag", ArrowDataType::Utf8, true)),
        ]
        .into(),
        vec![
            Arc::new(variants(&nested_values)),
            Arc::new(StringArray::from(vec![
                "object",
                "sql-null",
                "variant-null",
                "parent-null",
                "scalar",
            ])),
        ],
        Some(NullBuffer::from(vec![true, true, true, false, true])),
    );
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("payload", variant_arrow_type(), true),
            Field::new("nested", nested.data_type().clone(), true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4])),
            Arc::new(variants(&values)),
            Arc::new(nested),
        ],
    )
    .unwrap();
    (batch, values)
}

async fn write_and_commit(table: &Table, batch: &RecordBatch) {
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(batch).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert!(!messages.is_empty());
    for file in messages.iter().flat_map(|message| &message.new_files) {
        assert!(file.file_name.ends_with(".row"));
        let path = file.data_file_path(&format!("{}/bucket-0", table.location()));
        let bytes = table
            .file_io()
            .new_input(&path)
            .unwrap()
            .read()
            .await
            .unwrap();
        assert_eq!(bytes[bytes.len() - 8], 1, "ordinary Variant keeps ROW v1");
    }
    writer.close().await;
    builder.new_commit().commit(messages).await.unwrap();
}

async fn read(table: &Table, projection: &[&str]) -> RecordBatch {
    let mut builder = table.new_read_builder();
    builder.with_projection(projection).unwrap();
    let plan = builder.new_scan().plan().await.unwrap();
    let batches = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap_or_else(|err| panic!("reading {} with {projection:?}: {err}", table.location()));
    concat_batches(&batches[0].schema(), &batches).unwrap()
}

fn assert_variants(column: &ArrayRef, expected: &[Option<GenericVariant>]) {
    let column = column.as_any().downcast_ref::<StructArray>().unwrap();
    let values = column
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    let metadata = column
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(column.len(), expected.len());
    for (row, expected) in expected.iter().enumerate() {
        if let Some(expected) = expected {
            assert!(column.is_valid(row));
            let actual = GenericVariant::from_parts(
                values.value(row).to_vec(),
                metadata.value(row).to_vec(),
            )
            .unwrap();
            assert_eq!(&actual, expected, "row {row}");
            assert_eq!(actual.to_json().unwrap(), expected.to_json().unwrap());
        } else {
            assert!(column.is_null(row), "row {row}");
        }
    }
}

#[tokio::test]
async fn ordinary_variant_row_tables_preserve_values_and_reordered_projection() {
    for kind in ["append", "primary_key", "data_evolution"] {
        let (_temp, table) = create_table(kind).await;
        let (input, values) = input_batch();
        write_and_commit(&table, &input).await;

        let payload_only = read(&table, &["id", "payload"]).await;
        assert_variants(payload_only.column(1), &values);
        let result = read(&table, &["id", "payload", "nested"]).await;
        assert_eq!(result.num_rows(), 5);
        assert_eq!(result.column(0).to_data(), input.column(0).to_data());
        assert_variants(result.column(1), &values);
        assert_eq!(result.column(2).to_data(), input.column(2).to_data());

        let projected = read(&table, &["payload", "id"]).await;
        assert_eq!(projected.schema().field(0).name(), "payload");
        assert_eq!(projected.schema().field(1).name(), "id");
        assert_variants(projected.column(0), &values);
        assert_eq!(projected.column(1).to_data(), input.column(0).to_data());

        if kind == "primary_key" {
            // Overlapping key ranges must use the KV merge reader rather than
            // the single-file raw shortcut exercised above.
            write_and_commit(&table, &input).await;
            let plan = table.new_read_builder().new_scan().plan().await.unwrap();
            assert!(!plan.splits().is_empty());
            assert!(plan.splits().iter().all(|split| !split.raw_convertible()));

            let merged = read(&table, &["id", "payload", "nested"]).await;
            assert_eq!(merged.num_rows(), 5);
            assert_eq!(merged.column(0).to_data(), input.column(0).to_data());
            assert_variants(merged.column(1), &values);
            assert_eq!(merged.column(2).to_data(), input.column(2).to_data());

            let projected = read(&table, &["payload", "id"]).await;
            assert_eq!(projected.schema().field(0).name(), "payload");
            assert_eq!(projected.schema().field(1).name(), "id");
            assert_variants(projected.column(0), &values);
            assert_eq!(projected.column(1).to_data(), input.column(0).to_data());
        }
    }
}

fn extraction_type() -> DataType {
    DataType::Row(
        variant_extraction_row(
            true,
            ["$.x", "$.missing"].map(|path| {
                (
                    DataType::Int(IntType::new()),
                    path.into(),
                    false,
                    "UTC".into(),
                )
            }),
        )
        .unwrap(),
    )
}

async fn assert_variant_extraction_read_type(kind: &str, force_merge: bool) {
    let (_temp, table) = create_table(kind).await;
    let (input, _) = input_batch();
    write_and_commit(&table, &input).await;
    if force_merge {
        write_and_commit(&table, &input).await;
    }
    let fields = table.schema().fields();
    let payload = &fields[1];
    let nested = &fields[2];
    let DataType::Row(nested_row) = nested.data_type() else {
        unreachable!()
    };
    let nested_payload = &nested_row.fields()[0];
    let mut builder = table.new_read_builder();
    builder.with_read_type(vec![
        DataField::new(
            nested.id(),
            nested.name().into(),
            DataType::Row(RowType::new(vec![DataField::new(
                nested_payload.id(),
                nested_payload.name().into(),
                extraction_type(),
            )])),
        ),
        fields[0].clone(),
        DataField::new(payload.id(), payload.name().into(), extraction_type()),
    ]);
    let plan = builder.new_scan().plan().await.unwrap();
    if force_merge {
        assert!(!plan.splits().is_empty());
        assert!(plan.splits().iter().all(|split| !split.raw_convertible()));
    }
    let batches = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let result = concat_batches(&batches[0].schema(), &batches).unwrap();
    assert_eq!(result.schema().field(0).name(), "nested");
    assert_eq!(result.schema().field(1).name(), "id");
    let payload = result
        .column(2)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let x = payload
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(x, &Int32Array::from(vec![Some(1), None, None, None, None]));
    assert_eq!(payload.column(1).null_count(), 5);
    assert!(
        payload.is_valid(2),
        "Variant null retains its extraction row"
    );
    assert!(payload.is_null(3), "SQL NULL remains a null extraction row");

    let nested = result
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(nested.num_columns(), 1);
    assert!(nested.is_null(3));
    let nested_payload = nested
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert!(nested_payload.is_null(1));
    assert!(nested_payload.is_valid(2));
    assert_eq!(
        nested_payload
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap(),
        &Int32Array::from(vec![Some(1), None, None, None, None])
    );
    assert_eq!(nested_payload.column(1).null_count(), 5);
}

#[tokio::test]
async fn ordinary_variant_row_read_type_extracts_top_level_and_nested_fields() {
    for (kind, force_merge) in [
        ("append", false),
        ("primary_key", false),
        ("primary_key", true),
    ] {
        assert_variant_extraction_read_type(kind, force_merge).await;
    }
}
