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

use arrow_array::types::Float32Type;
use arrow_array::{
    Array, ArrayRef, BinaryArray, Float32Array, Int32Array, Int64Array, ListArray, RecordBatch,
    StringArray, StructArray,
};
use arrow_buffer::NullBuffer;
use arrow_schema::DataType as ArrowDataType;
use arrow_select::concat::concat_batches;
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs};
use futures::TryStreamExt;
use paimon::arrow::variant_arrow_type;
use paimon::spec::{
    ArrayType, DataType, Datum, FloatType, IntType, PredicateBuilder, Schema, TableSchema,
    VarCharType, VariantType,
};
use paimon::table::{CommitMessage, GlobalIndexResult, Table};
use paimon::variant::GenericVariant;
use std::collections::HashMap;
use std::sync::Arc;

fn value(id: i32, updated: bool) -> Option<GenericVariant> {
    let json = match id.rem_euclid(5) {
        1 => return None,
        2 => "null".into(),
        3 => r#""scalar""#.into(),
        4 => "[1,null,true]".into(),
        _ => format!(
            r#"{{"x":{},"nested":{{"items":[true,null,"中"]}}}}"#,
            id + if updated { 1000 } else { 0 }
        ),
    };
    Some(GenericVariant::parse_json(&json).unwrap())
}

fn variants(start: i32, count: i32, updated: bool) -> StructArray {
    let values = (start..start + count)
        .map(|id| value(id, updated))
        .collect::<Vec<_>>();
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

fn input(start: i32, count: i32) -> RecordBatch {
    let embeddings =
        ListArray::from_iter_primitive::<Float32Type, _, _>((start..start + count).map(|id| {
            let x = match id {
                5 => 0.0,
                7 => 1.0,
                6 => 2.0,
                8 => 3.0,
                _ => 1000.0 + id.abs() as f32,
            };
            Some(vec![Some(x), Some(0.0)])
        }));
    let text = StringArray::from_iter_values((start..start + count).map(|id| match id {
        5 => "needle spare spare spare spare spare spare spare",
        6 => "needle",
        7 => "needle needle needle needle",
        _ => "unrelated",
    }));
    RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int32Array::from_iter_values(start..start + count)) as ArrayRef,
        ),
        ("embedding", Arc::new(embeddings) as ArrayRef),
        ("text", Arc::new(text) as ArrayRef),
        (
            "payload",
            Arc::new(variants(start, count, false)) as ArrayRef,
        ),
    ])
    .unwrap()
}

async fn append(table: &Table, batch: &RecordBatch) -> Vec<CommitMessage> {
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(batch).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    writer.close().await;
    builder.new_commit().commit(messages.clone()).await.unwrap();
    messages
}

async fn fixture(name: &str, full_text: bool) -> Table {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "embedding",
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        )
        .column("text", DataType::VarChar(VarCharType::string_type()))
        .column("payload", DataType::Variant(VariantType::new()))
        .option("file.format", "parquet")
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        // Full-text index builds currently require DV to be disabled. The
        // vector fixture separately verifies deletion-aware Top-K and reads.
        .option(
            "deletion-vectors.enabled",
            if full_text { "false" } else { "true" },
        )
        .option("data-evolution.row-sidecar.enabled", "true")
        .option("global-index.enabled", "true")
        .option("global-index.row-count-per-shard", "1000")
        .option("file-index.bitmap.columns", "id")
        .option("file-index.in-manifest-threshold", "1mb")
        .option("vector-index.search-mode", "fast")
        .option("full-text-index.search-mode", "fast")
        .option("ivf-flat.dimension", "2")
        .option("ivf-flat.nlist", "1")
        .option("ivf-flat.distance.metric", "l2")
        .build()
        .unwrap();
    let (io, table) = memory_table(
        &format!("memory:/search_sidecar_{name}"),
        TableSchema::new(0, &schema),
    );
    setup_dirs(&io, table.location()).await;
    persist_table_schema(&io, table.location(), table.schema()).await;
    let mut messages = append(&table, &input(-20, 20)).await;
    messages.extend(append(&table, &input(0, 100)).await);
    assert!(
        table
            .new_global_index_build_builder()
            .with_index_column("embedding")
            .with_index_type("ivf-flat")
            .execute()
            .await
            .unwrap()
            > 0
    );
    if full_text {
        assert!(
            table
                .new_global_index_build_builder()
                .with_index_column("text")
                .with_index_type("full-text")
                .execute()
                .await
                .unwrap()
                > 0
        );
    }
    let patch = RecordBatch::try_from_iter([
        (
            "_ROW_ID",
            Arc::new(Int64Array::from_iter_values(20..120)) as ArrayRef,
        ),
        ("payload", Arc::new(variants(0, 100, true)) as ArrayRef),
    ])
    .unwrap();
    let updates = table
        .new_write_builder()
        .new_update()
        .unwrap()
        .update_by_arrow_with_row_id(vec![patch])
        .await
        .unwrap();
    assert_eq!(updates.iter().map(|m| m.new_files.len()).sum::<usize>(), 1);
    let file = &updates[0].new_files[0];
    assert_eq!(file.row_count, 100);
    assert_eq!(file.first_row_id, Some(20));
    assert_eq!(
        file.write_cols.as_deref(),
        Some(&["payload".to_string()][..])
    );
    table
        .new_write_builder()
        .new_commit()
        .commit(updates.clone())
        .await
        .unwrap();
    messages.extend(updates);
    for file in messages.iter().flat_map(|m| &m.new_files) {
        assert!(file.file_name.ends_with(".parquet"));
        assert_eq!(file.extra_files, vec![format!("{}.row", file.file_name)]);
        assert!(io
            .exists(&file.aligned_file_path(
                &format!("{}/bucket-0", table.location()),
                &file.extra_files[0]
            ))
            .await
            .unwrap());
        io.delete_file(&file.data_file_path(&format!("{}/bucket-0", table.location())))
            .await
            .unwrap();
    }
    table
}

async fn assert_materialized(result: &GlobalIndexResult) {
    let expected = result.row_ids().unwrap();
    assert_eq!(expected.row_ids.len(), 3);
    let batches = result
        .new_read_builder()
        .with_projection(&["payload", "id"])
        .read()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert!(!batches.is_empty());
    let batch = concat_batches(&batches[0].schema(), &batches).unwrap();
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        vec!["payload", "id", "__paimon_search_score"]
    );
    assert_eq!(batch.num_rows(), expected.row_ids.len());
    let ids = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let scores = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    let payload = batch
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    for (row, (&global_id, &score)) in expected.row_ids.iter().zip(&expected.scores).enumerate() {
        let id = i32::try_from(global_id - 20).unwrap();
        assert_eq!(ids.value(row), id, "materialized order follows search rank");
        assert!(
            (scores.value(row) - score).abs() < 1e-6,
            "score belongs to the same global row ID"
        );
        let expected = value(id, true);
        assert_eq!(payload.is_null(row), expected.is_none());
        if let Some(expected) = expected {
            for (index, expected) in [expected.value(), expected.metadata()].iter().enumerate() {
                let child = payload
                    .column(index)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap();
                assert!(child.is_valid(row));
                assert_eq!(
                    child.value(row),
                    *expected,
                    "latest Variant buffer at row ID {global_id}"
                );
            }
        }
    }
}

#[tokio::test]
async fn real_vector_hits_read_updated_variant_sidecars_in_score_order_with_deletions() {
    let table = fixture("vector", false).await;
    let mut builder = table.new_vector_search_builder();
    builder
        .with_vector_column("embedding")
        .with_query_vector(vec![0.0, 0.0])
        .with_limit(3)
        .with_options(vector_options());
    let result = builder.execute().await.unwrap();
    assert_eq!(result.row_ids().unwrap().row_ids, vec![25, 27, 26]);
    for (actual, expected) in result.row_ids().unwrap().scores.iter().zip([1.0, 0.5, 0.2]) {
        assert!((*actual - expected).abs() < 1e-6);
    }
    // Physical rows are 5/6/7; ranking is 5/7/6, crossing JSON and SQL null.
    // Both the original ID provider and the latest Variant provider must read
    // their ROW sidecars because every primary Parquet has been deleted.
    assert_materialized(&result).await;

    // Scalar search filters materialize matching row IDs before searching.
    // That internal `_ROW_ID` projection must retain bitmap pruning and ROW
    // access when its internal projection contains only the row-ID column.
    let mut filtered = table.new_vector_search_builder();
    filtered
        .with_vector_column("embedding")
        .with_query_vector(vec![0.0, 0.0])
        .with_limit(3)
        .with_options(vector_options())
        .with_filter(
            PredicateBuilder::new(table.schema().fields())
                .is_in("id", (6..10).map(Datum::Int).collect())
                .unwrap(),
        );
    let filtered = filtered.execute().await.unwrap();
    assert_eq!(filtered.row_ids().unwrap().row_ids, vec![27, 26, 28]);
    assert_materialized(&filtered).await;

    let deletions = table
        .new_write_builder()
        .new_update()
        .unwrap()
        .delete_by_row_id(vec![25])
        .await
        .unwrap();
    table
        .new_write_builder()
        .new_commit()
        .commit(deletions)
        .await
        .unwrap();
    // Also exercise the public Scan -> Plan -> Read API, without rerouting
    // materialization through a hand-constructed row-range request.
    let result = builder
        .new_read()
        .unwrap()
        .read(builder.new_scan().unwrap().plan().await.unwrap())
        .await
        .unwrap();
    assert_eq!(result.row_ids().unwrap().row_ids, vec![27, 26, 28]);
    for (actual, expected) in result.row_ids().unwrap().scores.iter().zip([0.5, 0.2, 0.1]) {
        assert!((*actual - expected).abs() < 1e-6);
    }
    assert_materialized(&result).await;
}

fn vector_options() -> HashMap<String, String> {
    HashMap::from([
        ("index_type".into(), "ivf-flat".into()),
        ("ivf.nprobe".into(), "1".into()),
    ])
}

#[cfg(feature = "fulltext")]
#[tokio::test]
async fn full_text_and_hybrid_hits_read_latest_variant_sidecars_with_ranked_scores() {
    let table = fixture("full_text_hybrid", true).await;
    let text = table
        .new_full_text_search_builder()
        .with_text_column("text")
        .with_query_text("needle")
        .with_limit(3)
        .execute_scored()
        .await
        .unwrap();
    let text_hits = text.row_ids().unwrap();
    let mut ids = text_hits.row_ids.clone();
    ids.sort_unstable();
    assert_eq!(ids, vec![25, 26, 27]);
    assert_ne!(
        text_hits.row_ids,
        vec![25, 26, 27],
        "different document lengths must produce a nonphysical relevance order"
    );
    assert!(text_hits.scores.iter().all(|s| s.is_finite() && *s > 0.0));
    assert!(text_hits.scores.windows(2).all(|s| s[0] >= s[1]));
    assert_materialized(&text).await;

    let vector = table
        .new_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vector(vec![0.0, 0.0])
        .with_limit(3)
        .with_options(vector_options())
        .execute()
        .await
        .unwrap();
    assert_eq!(vector.row_ids().unwrap().row_ids, vec![25, 27, 26]);
    let mut expected_scores = HashMap::<u64, f32>::new();
    for hits in [vector.row_ids().unwrap(), text_hits] {
        for (rank, row_id) in hits.row_ids.iter().enumerate() {
            *expected_scores.entry(*row_id).or_default() += 1.0 / (60.0 + rank as f32 + 1.0);
        }
    }
    let mut expected = expected_scores.into_iter().collect::<Vec<_>>();
    expected.sort_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    let mut hybrid = table.new_hybrid_search_builder();
    hybrid
        .add_vector_route("embedding", vec![0.0, 0.0], 3, 1.0, vector_options())
        .unwrap()
        .add_full_text_route(
            "text",
            r#"{"match":{"query":"needle"}}"#,
            3,
            1.0,
            HashMap::new(),
        )
        .unwrap()
        .with_limit(3);
    let fused = hybrid.execute_scored().await.unwrap();
    let hits = fused.row_ids().unwrap();
    assert_eq!(
        hits.row_ids,
        expected.iter().map(|(id, _)| *id).collect::<Vec<_>>()
    );
    for (score, (_, expected)) in hits.scores.iter().zip(expected) {
        assert!((*score - expected).abs() < 1e-6);
    }
    assert_materialized(&fused).await;
}
