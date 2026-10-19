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

use arrow_array::{Array, ArrayRef, BinaryArray, Int32Array, Int64Array, RecordBatch, StructArray};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType as ArrowDataType, Field};
use arrow_select::concat::concat_batches;
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs};
use futures::TryStreamExt;
use paimon::arrow::variant_arrow_type;
use paimon::spec::{
    variant_extraction_row, BigIntType, DataField, DataType, Datum, IntType, Predicate,
    PredicateBuilder, RowType, Schema, SchemaChange, TableSchema, VariantType, ROW_ID_FIELD_ID,
    ROW_ID_FIELD_NAME,
};
use paimon::table::{CommitMessage, Table};
use paimon::variant::GenericVariant;
use std::sync::Arc;

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

fn values(start: i32, count: i32, updated: bool) -> Vec<Option<GenericVariant>> {
    (start..start + count)
        .map(|id| {
            let json = if updated && id == 5 {
                Some(r#"{"x":999,"other":[true,null]}"#.to_string())
            } else if id == 50 {
                // An unselected value must not fail the strict extraction.
                Some(r#"{"x":"invalid integer"}"#.to_string())
            } else {
                match id.rem_euclid(5) {
                    1 => None,
                    2 => Some("null".to_string()),
                    4 => Some(r#""scalar""#.to_string()),
                    _ => Some(format!(r#"{{"x":{id},"other":[true,null]}}"#)),
                }
            };
            json.map(|json| GenericVariant::parse_json(&json).unwrap())
        })
        .collect()
}

fn input(start: i32, count: i32) -> RecordBatch {
    let nested_values = (start..start + count)
        .map(|id| {
            if id.rem_euclid(5) == 1 {
                None
            } else if id.rem_euclid(5) == 2 {
                Some(GenericVariant::parse_json("null").unwrap())
            } else {
                Some(GenericVariant::parse_json(&format!(r#"{{"x":{}}}"#, id + 100)).unwrap())
            }
        })
        .collect::<Vec<_>>();
    let nested = StructArray::new(
        vec![Arc::new(Field::new("detail", variant_arrow_type(), true))].into(),
        vec![Arc::new(variants(&nested_values))],
        Some(NullBuffer::from(
            (start..start + count)
                .map(|id| id.rem_euclid(5) != 3)
                .collect::<Vec<_>>(),
        )),
    );
    RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int32Array::from_iter_values(start..start + count)) as ArrayRef,
        ),
        (
            "payload",
            Arc::new(variants(&values(start, count, false))) as ArrayRef,
        ),
        ("nested", Arc::new(nested) as ArrayRef),
    ])
    .unwrap()
}

async fn append(table: &Table, batch: &RecordBatch) -> Vec<CommitMessage> {
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(batch).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    builder.new_commit().commit(messages.clone()).await.unwrap();
    messages
}

fn extraction_type() -> DataType {
    DataType::Row(
        variant_extraction_row(
            true,
            ["$.x", "$.missing"].map(|path| {
                (
                    DataType::Int(IntType::new()),
                    path.into(),
                    true,
                    "UTC".into(),
                )
            }),
        )
        .unwrap(),
    )
}

async fn query(table: &Table, extraction: bool) -> RecordBatch {
    let fields = table.schema().fields();
    let mut builder = table.new_read_builder();
    if extraction {
        let DataType::Row(nested) = fields[2].data_type() else {
            unreachable!()
        };
        let detail = &nested.fields()[0];
        builder.with_read_type(vec![
            DataField::new(
                fields[2].id(),
                "nested".into(),
                DataType::Row(RowType::new(vec![DataField::new(
                    detail.id(),
                    "detail".into(),
                    extraction_type(),
                )])),
            ),
            DataField::new(
                ROW_ID_FIELD_ID,
                ROW_ID_FIELD_NAME.into(),
                DataType::BigInt(BigIntType::new()),
            ),
            DataField::new(fields[1].id(), fields[1].name().into(), extraction_type()),
            fields[0].clone(),
        ]);
    } else {
        builder
            .with_projection(&["nested", "_ROW_ID", fields[1].name(), "id"])
            .unwrap();
    }
    let predicates = PredicateBuilder::new(fields);
    builder.with_filter(Predicate::and(vec![
        predicates.greater_or_equal("id", Datum::Int(5)).unwrap(),
        predicates.less_or_equal("id", Datum::Int(9)).unwrap(),
    ]));
    let plan = builder.new_scan().plan().await.unwrap();
    assert!(!plan.splits().is_empty());
    for split in plan.splits() {
        let ranges = split
            .row_ranges()
            .expect("the real global index must select row IDs");
        assert!(ranges
            .iter()
            .all(|range| range.from() >= 25 && range.to() <= 29));
        assert!(split
            .data_files()
            .iter()
            .all(|file| file.file_name.ends_with(".parquet")));
        assert!(split
            .data_files()
            .iter()
            .all(|file| file.first_row_id == Some(20)));
    }
    let batches = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    concat_batches(&batches[0].schema(), &batches).unwrap()
}

fn assert_result(batch: &RecordBatch, updated: bool, extraction: bool, deleted: bool) {
    let expected_ids = if deleted {
        vec![5, 6, 7, 9]
    } else {
        vec![5, 6, 7, 8, 9]
    };
    let ids = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.values().as_ref(), expected_ids.as_slice());
    let row_ids = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        row_ids.values().as_ref(),
        expected_ids
            .iter()
            .map(|id| i64::from(*id) + 20)
            .collect::<Vec<_>>()
    );
    let payload = batch
        .column(2)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert!(payload.is_null(1), "SQL NULL keeps a null Variant parent");
    assert!(payload.is_valid(2), "JSON null remains a non-null Variant");
    if extraction {
        let x = payload
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let expected = expected_ids
            .iter()
            .map(|id| match *id {
                5 => Some(if updated { 999 } else { 5 }),
                8 => Some(8),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(x, &Int32Array::from(expected));
        assert_eq!(payload.column(1).null_count(), expected_ids.len());
        let nested = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let detail = nested
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let x = detail
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for (row, id) in expected_ids.iter().enumerate() {
            if *id == 5 || *id == 9 {
                assert!(detail.is_valid(row));
                assert_eq!(x.value(row), id + 100);
            }
            if *id == 6 {
                assert!(nested.is_valid(row));
                assert!(detail.is_null(row), "nested SQL NULL keeps a null parent");
            }
            if *id == 7 {
                assert!(nested.is_valid(row));
                assert!(detail.is_valid(row), "nested JSON null keeps its parent");
                assert!(x.is_null(row));
            }
            if *id == 8 {
                assert!(nested.is_null(row));
            } else {
                assert!(detail.column(1).is_null(row), "missing paths remain NULL");
            }
        }
    } else {
        let expected = values(0, 100, updated);
        let actual_values = payload
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let metadata = payload
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        for (row, id) in expected_ids.iter().enumerate() {
            if let Some(expected) = &expected[*id as usize] {
                assert_eq!(actual_values.value(row), expected.value());
                assert_eq!(metadata.value(row), expected.metadata());
            }
        }
        let nested = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let detail = nested
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let detail_values = detail
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let detail_metadata = detail
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        for (row, id) in expected_ids.iter().enumerate() {
            assert_eq!(nested.is_null(row), id.rem_euclid(5) == 3);
            if nested.is_null(row) {
                continue;
            }
            assert_eq!(detail.is_null(row), *id == 6);
            if detail.is_valid(row) {
                let expected = GenericVariant::parse_json(&if *id == 7 {
                    "null".to_string()
                } else {
                    format!(r#"{{"x":{}}}"#, id + 100)
                })
                .unwrap();
                assert_eq!(detail_values.value(row), expected.value());
                assert_eq!(detail_metadata.value(row), expected.metadata());
            }
        }
    }
}

#[tokio::test]
async fn global_indexes_read_variant_sidecars_after_column_updates_and_rename() {
    for (index_type, shredded) in [("bitmap", false), ("btree", true)] {
        let mut schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("payload", DataType::Variant(VariantType::new()))
            .column(
                "nested",
                DataType::Row(RowType::new(vec![DataField::new(
                    0,
                    "detail".into(),
                    DataType::Variant(VariantType::new()),
                )])),
            )
            .option("file.format", "parquet")
            .option("row-tracking.enabled", "true")
            .option("data-evolution.enabled", "true")
            .option("deletion-vectors.enabled", "true")
            .option("data-evolution.row-sidecar.enabled", "true")
            .option("global-index.enabled", "true");
        if shredded {
            schema = schema.option("parquet.variant.shreddingSchema",
                r#"{"type":"ROW","fields":[{"name":"payload","type":{"type":"ROW","fields":[{"name":"x","type":"INT"}]}}]}"#);
        }
        let (io, table) = memory_table(
            &format!("memory:/variant_sidecar_{index_type}"),
            TableSchema::new(0, &schema.build().unwrap()),
        );
        setup_dirs(&io, table.location()).await;
        persist_table_schema(&io, table.location(), table.schema()).await;
        append(&table, &input(-20, 20)).await;
        let initial = append(&table, &input(0, 100)).await;
        assert!(
            table
                .new_global_index_build_builder()
                .with_index_column("id")
                .with_index_type(index_type)
                .execute()
                .await
                .unwrap()
                > 0
        );
        assert_result(&query(&table, false).await, false, false, false);
        let file = &initial[0].new_files[0];
        assert_eq!(file.extra_files, vec![format!("{}.row", file.file_name)]);
        let path = file.data_file_path(&format!("{}/bucket-0", table.location()));
        let primary = io.new_input(&path).unwrap().read().await.unwrap();
        io.delete_file(&path).await.unwrap();
        for extraction in [false, true] {
            assert_result(&query(&table, extraction).await, false, extraction, false);
        }
        // Restore only for producing the column patch; reads below remove all providers again.
        io.new_output(&path).unwrap().write(primary).await.unwrap();
        let patch = RecordBatch::try_from_iter([
            (
                "_ROW_ID",
                Arc::new(Int64Array::from_iter_values(20..120)) as ArrayRef,
            ),
            (
                "payload",
                Arc::new(variants(&values(0, 100, true))) as ArrayRef,
            ),
        ])
        .unwrap();
        let update = table.new_write_builder().new_update().unwrap();
        let updates = update
            .update_by_arrow_with_row_id(vec![patch])
            .await
            .unwrap();
        assert!(updates.iter().flat_map(|m| &m.new_files).all(|f| f
            .write_cols
            .as_ref()
            .is_some_and(|columns| columns == &["payload"])));
        table
            .new_write_builder()
            .new_commit()
            .commit(updates.clone())
            .await
            .unwrap();
        let schema = table
            .schema()
            .apply_changes(vec![SchemaChange::rename_column(
                "payload".into(),
                "renamed_payload".into(),
            )])
            .unwrap();
        persist_table_schema(&io, table.location(), &schema).await;
        let table = table.copy_with_resolved_schema(schema, "main").unwrap();
        for file in initial.iter().chain(&updates).flat_map(|m| &m.new_files) {
            assert_eq!(file.row_count, 100);
            io.delete_file(&file.data_file_path(&format!("{}/bucket-0", table.location())))
                .await
                .unwrap();
        }
        for extraction in [false, true] {
            assert_result(&query(&table, extraction).await, true, extraction, false);
        }
        let deletions = table
            .new_write_builder()
            .new_update()
            .unwrap()
            .delete_by_row_id(vec![28])
            .await
            .unwrap();
        table
            .new_write_builder()
            .new_commit()
            .commit(deletions)
            .await
            .unwrap();
        for extraction in [false, true] {
            assert_result(&query(&table, extraction).await, true, extraction, true);
        }
    }
}
