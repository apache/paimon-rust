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
use arrow_schema::DataType as ArrowDataType;
use arrow_select::concat::concat_batches;
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs};
use futures::TryStreamExt;
use paimon::arrow::variant_arrow_type;
use paimon::spec::{
    DataType, Datum, IntType, Predicate, PredicateBuilder, Schema, SchemaChange, TableSchema,
    VariantType,
};
use paimon::table::{CommitMessage, RowRange, Table};
use paimon::variant::GenericVariant;
use std::sync::Arc;

async fn table(name: &str) -> Table {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("a", DataType::Int(IntType::new()))
        .column("b", DataType::Int(IntType::new()))
        .column("payload", DataType::Variant(VariantType::new()))
        .option("file.format", "parquet")
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .option("deletion-vectors.enabled", "true")
        .option("data-evolution.row-sidecar.enabled", "true")
        .option("global-index.enabled", "false")
        .option("file-index.bitmap.columns", "id,a,b")
        .option("file-index.in-manifest-threshold", "1mb")
        .build()
        .unwrap();
    let (io, table) = memory_table(
        &format!("memory:/bitmap_merge_{name}"),
        TableSchema::new(0, &schema),
    );
    setup_dirs(&io, table.location()).await;
    persist_table_schema(&io, table.location(), table.schema()).await;
    table
}

fn values(start: i32, count: i32, updated: bool) -> Vec<Option<GenericVariant>> {
    (start..start + count)
        .map(|id| {
            let json = match id.rem_euclid(5) {
                1 => return None,
                2 => "null".to_string(),
                3 => format!(r#""{}""#, if updated { "updated" } else { "scalar" }),
                4 => format!("[{},null,true]", id + if updated { 1000 } else { 0 }),
                _ => format!(
                    r#"{{"x":{},"nested":{{"items":[true,null,"中"]}}}}"#,
                    id + if updated { 1000 } else { 0 }
                ),
            };
            Some(GenericVariant::parse_json(&json).unwrap())
        })
        .collect()
}

fn variants(values: &[Option<GenericVariant>]) -> StructArray {
    let ArrowDataType::Struct(fields) = variant_arrow_type() else {
        unreachable!()
    };
    StructArray::new(
        fields,
        vec![
            Arc::new(BinaryArray::from_iter_values(
                values
                    .iter()
                    .map(|v| v.as_ref().map_or(&[][..], GenericVariant::value)),
            )),
            Arc::new(BinaryArray::from_iter_values(
                values
                    .iter()
                    .map(|v| v.as_ref().map_or(&[][..], GenericVariant::metadata)),
            )),
        ],
        Some(NullBuffer::from(
            values.iter().map(Option::is_some).collect::<Vec<_>>(),
        )),
    )
}

async fn append(table: &Table, start: i32, count: i32) -> Vec<CommitMessage> {
    let ids = (start..start + count).collect::<Vec<_>>();
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(ids.clone())) as ArrayRef),
        ("a", Arc::new(Int32Array::from(ids.clone())) as ArrayRef),
        ("b", Arc::new(Int32Array::from(ids)) as ArrayRef),
        (
            "payload",
            Arc::new(variants(&values(start, count, false))) as ArrayRef,
        ),
    ])
    .unwrap();
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    writer.close().await;
    builder.new_commit().commit(messages.clone()).await.unwrap();
    messages
}

async fn patch(table: &Table, column: &str, values: ArrayRef) -> Vec<CommitMessage> {
    let batch = RecordBatch::try_from_iter([
        (
            "_ROW_ID",
            Arc::new(Int64Array::from_iter_values(20..120)) as ArrayRef,
        ),
        (column, values),
    ])
    .unwrap();
    let messages = table
        .new_write_builder()
        .new_update()
        .unwrap()
        .update_by_arrow_with_row_id(vec![batch])
        .await
        .unwrap();
    let files = messages
        .iter()
        .flat_map(|m| &m.new_files)
        .collect::<Vec<_>>();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].row_count, 100);
    assert_eq!(files[0].first_row_id, Some(20));
    assert_eq!(
        files[0].write_cols.as_deref(),
        Some(&[column.to_string()][..])
    );
    table
        .new_write_builder()
        .new_commit()
        .commit(messages.clone())
        .await
        .unwrap();
    messages
}

async fn remove_primaries(table: &Table, messages: &[CommitMessage]) {
    for file in messages.iter().flat_map(|m| &m.new_files) {
        assert!(file.file_name.ends_with(".parquet"));
        assert_eq!(file.extra_files, vec![format!("{}.row", file.file_name)]);
        assert!(table
            .file_io()
            .exists(&file.aligned_file_path(
                &format!("{}/bucket-0", table.location()),
                &file.extra_files[0]
            ))
            .await
            .unwrap());
        table
            .file_io()
            .delete_file(&file.data_file_path(&format!("{}/bucket-0", table.location())))
            .await
            .unwrap();
    }
}

async fn query(table: &Table, predicate: Predicate, projection: &[&str]) -> RecordBatch {
    let mut builder = table.new_read_builder();
    builder.with_filter(predicate);
    builder.with_projection(projection).unwrap();
    let plan = builder.new_scan().plan().await.unwrap();
    assert!(!plan.splits().is_empty());
    for split in plan.splits() {
        assert!(
            split.row_ranges().is_none(),
            "no global index or caller row selection"
        );
        assert!(split
            .data_files()
            .iter()
            .all(|f| f.first_row_id == Some(20)));
    }
    let batches = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert!(!batches.is_empty());
    concat_batches(&batches[0].schema(), &batches).unwrap()
}

fn assert_payload(batch: &RecordBatch, ids: &[i32], updated: bool) {
    let payload = batch
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let row_ids = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(batch.num_rows(), ids.len());
    for (row, id) in ids.iter().enumerate() {
        assert_eq!(row_ids.value(row), i64::from(*id) + 20);
        let expected = values(*id, 1, updated).pop().unwrap();
        assert_eq!(payload.is_null(row), expected.is_none());
        if let Some(expected) = expected {
            for (column, expected) in [expected.value(), expected.metadata()].iter().enumerate() {
                let actual = payload
                    .column(column)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap();
                assert!(actual.is_valid(row));
                assert_eq!(actual.value(row), *expected);
            }
        }
    }
}

#[tokio::test]
async fn bitmap_reads_variant_column_merge_sidecars_without_requested_ranges() {
    let table = table("variant").await;
    append(&table, -20, 20).await;
    let mut messages = append(&table, 0, 100).await;
    assert!(messages
        .iter()
        .flat_map(|m| &m.new_files)
        .all(|f| f.embedded_index.is_some()));
    messages.extend(patch(&table, "payload", Arc::new(variants(&values(0, 100, true)))).await);
    remove_primaries(&table, &messages).await;
    let predicate = PredicateBuilder::new(table.schema().fields())
        .is_in("id", (5..10).map(Datum::Int).collect())
        .unwrap();
    // The indexed predicate field is absent from the output. The same sparse
    // physical selection must still reach both the ID and Variant providers.
    let batch = query(&table, predicate, &["payload", "_ROW_ID"]).await;
    assert_payload(&batch, &[5, 6, 7, 8, 9], true);
}

#[tokio::test]
async fn bitmap_uses_latest_column_provider_instead_of_overwritten_values() {
    let table = table("latest").await;
    append(&table, -20, 20).await;
    let mut messages = append(&table, 0, 100).await;
    let updated = (0..100)
        .map(|id| match id {
            5 => 999,
            42 => 5,
            _ => id,
        })
        .collect::<Vec<_>>();
    let patches = patch(&table, "b", Arc::new(Int32Array::from(updated))).await;
    assert!(patches
        .iter()
        .flat_map(|m| &m.new_files)
        .all(|f| f.embedded_index.is_some()));
    messages.extend(patches);
    let schema = table
        .schema()
        .apply_changes(vec![SchemaChange::rename_column(
            "b".into(),
            "renamed_b".into(),
        )])
        .unwrap();
    persist_table_schema(table.file_io(), table.location(), &schema).await;
    let table = table.copy_with_resolved_schema(schema, "main").unwrap();
    remove_primaries(&table, &messages).await;
    let predicates = PredicateBuilder::new(table.schema().fields());
    // The old index says b=5 is row 5, whereas the winning column provider
    // moved it to row 42. The old file has no b=999 at all, so consulting its
    // index as an independent conjunct would incorrectly prune the second hit.
    for (value, expected_id) in [(5, 42), (999, 5)] {
        let batch = query(
            &table,
            predicates.equal("renamed_b", Datum::Int(value)).unwrap(),
            &["payload", "_ROW_ID"],
        )
        .await;
        assert_payload(&batch, &[expected_id], false);
    }
}

#[tokio::test]
async fn bitmap_boolean_predicates_select_the_same_rows_for_every_column() {
    let table = table("boolean").await;
    append(&table, -20, 20).await;
    let mut messages = append(&table, 0, 100).await;
    messages.extend(
        patch(
            &table,
            "b",
            Arc::new(Int32Array::from_iter_values(1000..1100)),
        )
        .await,
    );
    messages.extend(patch(&table, "payload", Arc::new(variants(&values(0, 100, true)))).await);
    let predicates = PredicateBuilder::new(table.schema().fields());
    // A cross-provider OR has no safe per-file necessary condition. Reading
    // the primaries must retain the union rather than intersect its branches.
    let pure_or = Predicate::or(vec![
        predicates.equal("a", Datum::Int(5)).unwrap(),
        predicates.equal("b", Datum::Int(1007)).unwrap(),
    ]);
    let fallback = query(&table, pure_or, &["payload", "_ROW_ID"]).await;
    assert_payload(&fallback, &[5, 7], true);
    let cases = [
        (
            Predicate::and(vec![
                predicates
                    .is_in("a", vec![Datum::Int(5), Datum::Int(6)])
                    .unwrap(),
                predicates
                    .is_in("b", vec![Datum::Int(1006), Datum::Int(1007)])
                    .unwrap(),
            ]),
            vec![6],
        ),
        (
            Predicate::and(vec![
                Predicate::or(vec![
                    predicates.equal("a", Datum::Int(5)).unwrap(),
                    predicates.equal("b", Datum::Int(1007)).unwrap(),
                ]),
                predicates
                    .is_in("id", vec![Datum::Int(5), Datum::Int(6), Datum::Int(7)])
                    .unwrap(),
            ]),
            vec![5, 7],
        ),
        (
            Predicate::and(vec![
                Predicate::negate(Predicate::and(vec![
                    predicates.equal("a", Datum::Int(5)).unwrap(),
                    predicates.equal("b", Datum::Int(1007)).unwrap(),
                ])),
                predicates
                    .is_in("id", (5..10).map(Datum::Int).collect())
                    .unwrap(),
            ]),
            // No row satisfies both inner predicates. Negating only one
            // provider's half would incorrectly discard row 5 or row 7.
            vec![5, 6, 7, 8, 9],
        ),
    ];
    let without_index = table.copy_with_options(std::collections::HashMap::from([(
        "file-index.read.enabled".into(),
        "false".into(),
    )]));
    for (predicate, expected_ids) in &cases {
        let batch = query(&without_index, predicate.clone(), &["payload", "_ROW_ID"]).await;
        assert_payload(&batch, expected_ids, true);
    }
    remove_primaries(&table, &messages).await;
    for (predicate, expected_ids) in cases {
        // Only the third provider is projected; predicates require both the
        // original provider (a/id) and a later scalar-only provider (b).
        let batch = query(&table, predicate, &["payload", "_ROW_ID"]).await;
        assert_payload(&batch, &expected_ids, true);
    }
}

#[tokio::test]
async fn missing_latest_bitmap_keeps_primary_and_does_not_reuse_stale_index() {
    use std::collections::HashMap;

    let table = table("missing_latest_index").await;
    append(&table, -20, 20).await;
    let mut messages = append(&table, 0, 100).await;
    let unindexed_writer = table.copy_with_options(HashMap::from([(
        "file-index.bitmap.columns".into(),
        "id,a".into(),
    )]));
    let updated = (0..100)
        .map(|id| match id {
            5 => 999,
            42 => 5,
            _ => id,
        })
        .collect::<Vec<_>>();
    let patches = patch(&unindexed_writer, "b", Arc::new(Int32Array::from(updated))).await;
    assert!(patches
        .iter()
        .flat_map(|m| &m.new_files)
        .all(|f| f.embedded_index.is_none()));
    messages.extend(patches);
    for file in messages.iter().flat_map(|m| &m.new_files) {
        assert_eq!(file.extra_files.len(), 1);
        table
            .file_io()
            .delete_file(&file.aligned_file_path(
                &format!("{}/bucket-0", table.location()),
                &file.extra_files[0],
            ))
            .await
            .unwrap();
    }
    let predicates = PredicateBuilder::new(table.schema().fields());
    // The latest b has no bitmap. Fall back to reading primary providers;
    // neither an old index result nor an empty result is a valid replacement.
    for (value, expected_id) in [(5, 42), (999, 5)] {
        let batch = query(
            &table,
            predicates.equal("b", Datum::Int(value)).unwrap(),
            &["payload", "_ROW_ID"],
        )
        .await;
        assert_payload(&batch, &[expected_id], false);
    }
    let batch = query(
        &table,
        Predicate::or(vec![
            predicates.equal("a", Datum::Int(5)).unwrap(),
            predicates.equal("b", Datum::Int(5)).unwrap(),
        ]),
        &["payload", "_ROW_ID"],
    )
    .await;
    assert_payload(&batch, &[5, 42], false);
}

#[tokio::test]
async fn bitmap_requested_ranges_deletions_and_limit_keep_merged_providers_aligned() {
    use std::collections::HashMap;

    for (name, max_rows, sidecar) in [
        ("aligned", "4", true),
        ("after_dv", "3", true),
        ("before_limit", "2", false),
    ] {
        let table = table(name).await.copy_with_options(HashMap::from([(
            "data-evolution.row-sidecar.max-selected-rows".into(),
            max_rows.into(),
        )]));
        append(&table, -20, 20).await;
        let mut messages = append(&table, 0, 100).await;
        messages.extend(patch(&table, "payload", Arc::new(variants(&values(0, 100, true)))).await);
        let deletions = table
            .new_write_builder()
            .new_update()
            .unwrap()
            .delete_by_row_id(vec![27])
            .await
            .unwrap();
        table
            .new_write_builder()
            .new_commit()
            .commit(deletions)
            .await
            .unwrap();
        if sidecar {
            remove_primaries(&table, &messages).await;
        } else {
            for file in messages.iter().flat_map(|m| &m.new_files) {
                table
                    .file_io()
                    .delete_file(&file.aligned_file_path(
                        &format!("{}/bucket-0", table.location()),
                        &file.extra_files[0],
                    ))
                    .await
                    .unwrap();
            }
        }
        let mut builder = table.new_read_builder();
        builder.with_projection(&["payload", "_ROW_ID"]).unwrap();
        builder.with_filter(
            PredicateBuilder::new(table.schema().fields())
                .is_in("id", (5..10).map(Datum::Int).collect())
                .unwrap(),
        );
        builder.with_row_ranges(vec![RowRange::new(26, 29)]);
        builder.with_limit(2);
        let plan = builder.new_scan().plan().await.unwrap();
        assert!(!plan.splits().is_empty());
        let batches = builder
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(!batches.is_empty());
        // Four index hits remain after requested-range intersection. Deleting
        // global row 27 must remove local selected position 1 from every
        // provider, then LIMIT returns rows 26/28, including a SQL-null Variant.
        // DV reduces the bitmap's four candidates to three before choosing
        // ROW. LIMIT must not reduce that threshold further to two.
        let batch = concat_batches(&batches[0].schema(), &batches).unwrap();
        assert_payload(&batch, &[6, 8], true);
    }
}

#[tokio::test]
async fn deletions_narrow_sidecar_candidates_only_for_a_selective_bitmap() {
    use std::collections::HashMap;

    for merged in [false, true] {
        for selective in [false, true] {
            let table = table(&format!("bitmap_dv_{merged}_{selective}"))
                .await
                .copy_with_options(HashMap::from([(
                    "data-evolution.row-sidecar.max-selected-rows".into(),
                    "3".into(),
                )]));
            append(&table, -20, 20).await;
            let mut messages = append(&table, 0, 100).await;
            if merged {
                messages.extend(
                    patch(&table, "payload", Arc::new(variants(&values(0, 100, true)))).await,
                );
            }
            let deleted = if selective {
                vec![25, 27]
            } else {
                (20..119).collect()
            };
            let deletions = table
                .new_write_builder()
                .new_update()
                .unwrap()
                .delete_by_row_id(deleted)
                .await
                .unwrap();
            table
                .new_write_builder()
                .new_commit()
                .commit(deletions)
                .await
                .unwrap();
            if selective {
                remove_primaries(&table, &messages).await;
            } else {
                // Java normalizes a full-coverage bitmap to REMAIN. Even
                // though DV leaves one live row, it cannot choose ROW alone.
                for file in messages.iter().flat_map(|m| &m.new_files) {
                    table
                        .file_io()
                        .delete_file(&file.aligned_file_path(
                            &format!("{}/bucket-0", table.location()),
                            &file.extra_files[0],
                        ))
                        .await
                        .unwrap();
                }
            }
            let predicates = PredicateBuilder::new(table.schema().fields());
            let predicate = if selective {
                predicates
                    .is_in("id", (5..10).map(Datum::Int).collect())
                    .unwrap()
            } else {
                predicates.greater_or_equal("id", Datum::Int(0)).unwrap()
            };
            let batch = query(&table, predicate, &["payload", "_ROW_ID"]).await;
            let expected = if selective { vec![6, 8, 9] } else { vec![99] };
            assert_payload(&batch, &expected, merged);
        }
    }
}
