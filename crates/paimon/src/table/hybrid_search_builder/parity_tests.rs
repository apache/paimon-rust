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

//! Global hybrid execution against real indexes, matching Java route semantics.

use super::*;
use crate::catalog::Identifier;
use crate::io::FileIOBuilder;
use crate::spec::{
    ArrayType, DataType, Datum, FloatType, IntType, PredicateBuilder, Schema, TableSchema,
    VarCharType,
};
use crate::table::{TableCommit, TableWrite};
use arrow_array::types::Float32Type;
use arrow_array::{ArrayRef, Int32Array, ListArray, StringArray};
use arrow_schema::{DataType as ArrowType, Field, Schema as ArrowSchema};
use std::sync::Arc;

fn table() -> Table {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("text", DataType::VarChar(VarCharType::string_type()))
        .column(
            "embedding",
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        )
        .column("label", DataType::VarChar(VarCharType::string_type()))
        .column("pt", DataType::Int(IntType::new()))
        .partition_keys(["pt"])
        .options(HashMap::from([
            ("bucket".into(), "-1".into()),
            ("row-tracking.enabled".into(), "true".into()),
            ("data-evolution.enabled".into(), "true".into()),
            ("global-index.enabled".into(), "true".into()),
            ("scalar-index.search-mode".into(), "full".into()),
            ("vector-index.search-mode".into(), "full".into()),
            ("full-text-index.search-mode".into(), "full".into()),
        ]))
        .build()
        .unwrap();
    Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("default", "hybrid"),
        format!("memory:/hybrid-parity-{}", uuid::Uuid::new_v4()),
        TableSchema::new(0, &schema),
        None,
    )
}

async fn append(table: &Table, start: i32) {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![
            start,
            start + 1,
            start + 2,
            start + 3,
        ])),
        Arc::new(StringArray::from(vec![
            Some("paimon"),
            Some("paimon paimon paimon"),
            Some("paimon lake"),
            None,
        ])),
        Arc::new(ListArray::from_iter_primitive::<Float32Type, _, _>(
            (start..start + 4).map(|id| Some(vec![Some(id as f32), Some(0.0)])),
        )),
        Arc::new(StringArray::from(vec![
            Some("drop"),
            Some("keep"),
            Some("keep"),
            None,
        ])),
        Arc::new(Int32Array::from(vec![start / 4; 4])),
    ];
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", ArrowType::Int32, true),
        Field::new("text", ArrowType::Utf8, true),
        Field::new("embedding", columns[2].data_type().clone(), true),
        Field::new("label", ArrowType::Utf8, true),
        Field::new("pt", ArrowType::Int32, true),
    ]));
    let mut writer = TableWrite::new(table, "hybrid".into()).unwrap();
    writer
        .write_arrow_batch(&RecordBatch::try_new(schema, columns).unwrap())
        .await
        .unwrap();
    TableCommit::new(table.clone(), "hybrid".into())
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();
}

async fn index(table: &Table, kind: &str, column: &str) {
    let mut builder = table.new_global_index_build_builder();
    builder.with_index_type(kind).with_index_column(column);
    if kind == "ivf-flat" {
        builder.with_options(HashMap::from([
            ("ivf-flat.dimension".into(), "2".into()),
            ("ivf-flat.nlist".into(), "1".into()),
            ("ivf-flat.distance.metric".into(), "l2".into()),
        ]));
    }
    builder.execute().await.unwrap();
}

#[tokio::test]
async fn full_text_route_uses_java_structured_query() {
    let table = table();
    append(&table, 0).await;
    index(&table, "full-text", "text").await;
    let mut builder = table.new_hybrid_search_builder();
    builder
        .add_full_text_route(
            "text",
            r#"{"match":{"query":"paimon lake","operator":"And"}}"#,
            4,
            1.0,
            HashMap::new(),
        )
        .unwrap();
    builder.with_limit(4);
    assert_eq!(builder.execute_scored().await.unwrap().row_ids, vec![2]);
}

#[tokio::test]
async fn full_text_route_rejects_plain_text_as_java_does() {
    let table = table();
    append(&table, 0).await;
    index(&table, "full-text", "text").await;
    let mut builder = table.new_hybrid_search_builder();
    builder
        .add_full_text_route("text", "paimon", 4, 1.0, HashMap::new())
        .unwrap();
    builder.with_limit(4);
    assert!(builder.execute_scored().await.is_err());
}

fn mixed_query(table: &Table, route_limit: usize) -> HybridSearchBuilder<'_> {
    let mut builder = table.new_hybrid_search_builder();
    builder
        .add_vector_route(
            "embedding",
            vec![0.0, 0.0],
            route_limit,
            1.0,
            HashMap::new(),
        )
        .unwrap()
        .add_full_text_route(
            "text",
            r#"{"match":{"query":"paimon"}}"#,
            route_limit,
            1.0,
            HashMap::new(),
        )
        .unwrap()
        .with_limit(16);
    builder
}

#[tokio::test]
async fn both_routes_filter_before_top_k_and_accumulate_predicates() {
    let table = table();
    append(&table, 0).await;
    append(&table, 4).await;
    index(&table, "ivf-flat", "embedding").await;
    index(&table, "full-text", "text").await;
    let pb = PredicateBuilder::new(table.schema().fields());
    let mut builder = mixed_query(&table, 1);
    builder
        .with_filter(pb.equal("label", Datum::String("keep".into())).unwrap())
        .with_filter(pb.equal("id", Datum::Int(6)).unwrap())
        .with_partition_filter(pb.equal("pt", Datum::Int(1)).unwrap())
        .unwrap();
    let result = builder.execute_scored().await.unwrap();
    assert_eq!(result.row_ids, vec![6]);
    assert!((result.scores[0] - 2.0 / 61.0).abs() < 1e-6);
    builder
        .with_partition_filter(pb.equal("pt", Datum::Int(0)).unwrap())
        .unwrap();
    assert!(builder.execute_scored().await.unwrap().is_empty());
}

#[tokio::test]
async fn scalar_coverage_and_refinement_are_shared_by_both_routes() {
    let table = table();
    append(&table, 0).await;
    index(&table, "ivf-flat", "embedding").await;
    index(&table, "full-text", "text").await;
    let pb = PredicateBuilder::new(table.schema().fields());
    let keep = pb.equal("label", Datum::String("keep".into())).unwrap();
    for mode in ["fast", "full", "detail"] {
        let table = table.copy_with_options(HashMap::from([(
            "scalar-index.search-mode".into(),
            mode.into(),
        )]));
        let mut ids = mixed_query(&table, 4)
            .with_filter(keep.clone())
            .execute_scored()
            .await
            .unwrap()
            .row_ids;
        ids.sort_unstable();
        // Vector FAST reads indexed ranges from data when no scalar index can
        // evaluate the filter; full-text FAST excludes those ranges instead.
        assert_eq!(ids, vec![1, 2], "scalar mode {mode}");
        let mut text = table.new_full_text_search_builder();
        text.with_query("text", r#"{"match":{"query":"paimon"}}"#)
            .with_filter(keep.clone())
            .with_limit(4);
        assert_eq!(
            text.execute_scored().await.unwrap().len(),
            if mode == "fast" { 0 } else { 2 }
        );
    }
    index(&table, "bitmap", "label").await;
    let candidate = pb.contains("label", Datum::String("eep".into())).unwrap();
    for refine in [false, true] {
        let table = table.copy_with_options(HashMap::from([
            (
                "global-index.filter.refine-from-data".into(),
                refine.to_string(),
            ),
            ("scalar-index.search-mode".into(), "fast".into()),
        ]));
        let mut ids = mixed_query(&table, 4)
            .with_filter(candidate.clone())
            .execute_scored()
            .await
            .unwrap()
            .row_ids;
        ids.sort_unstable();
        assert_eq!(ids, if refine { vec![1, 2] } else { vec![] });
    }
}

#[tokio::test]
async fn concurrent_routes_preserve_weights_limits_and_java_fusion() {
    let table = table();
    append(&table, 0).await;
    index(&table, "ivf-flat", "embedding").await;
    for (ranker, ids, scores) in [
        (
            "rrf",
            vec![3, 2, 0, 1],
            vec![2.0 / 61.0, 2.0 / 62.0, 1.0 / 61.0, 1.0 / 62.0],
        ),
        ("mrr", vec![3, 0, 2, 1], vec![2.0, 1.0, 1.0, 0.5]),
        ("weighted_score", vec![3, 0, 1, 2], vec![2.0, 1.0, 0.0, 0.0]),
    ] {
        let mut builder = table.new_hybrid_search_builder();
        builder
            .add_vector_route("embedding", vec![0.0, 0.0], 2, 1.0, HashMap::new())
            .unwrap()
            .add_vector_route("embedding", vec![3.0, 0.0], 2, 2.0, HashMap::new())
            .unwrap()
            .with_limit(4)
            .with_ranker(ranker)
            .unwrap();
        let result = builder.execute_scored().await.unwrap();
        assert_eq!(result.row_ids, ids, "ranker {ranker}");
        for (actual, expected) in result.scores.iter().zip(scores) {
            assert!(
                (actual - expected).abs() < 1e-6,
                "ranker {ranker}: {actual} != {expected}"
            );
        }
        builder.with_limit(1);
        assert_eq!(builder.execute_scored().await.unwrap().row_ids, vec![3]);
    }
}

#[tokio::test]
async fn retained_tag_keeps_both_routes_on_the_expired_snapshot() {
    let table = table();
    append(&table, 0).await;
    index(&table, "ivf-flat", "embedding").await;
    index(&table, "full-text", "text").await;
    let snapshot = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    table
        .tag_manager()
        .create("hybrid-before", &snapshot)
        .await
        .unwrap();
    append(&table, 4).await;
    table
        .file_io()
        .delete_file(&table.snapshot_manager().snapshot_path(snapshot.id()))
        .await
        .unwrap();
    let tagged = table.copy_with_options(HashMap::from([(
        "scan.tag-name".into(),
        "hybrid-before".into(),
    )]));
    let mut old = mixed_query(&tagged, 16)
        .execute_scored()
        .await
        .unwrap()
        .row_ids;
    old.sort_unstable();
    assert_eq!(old, vec![0, 1, 2, 3]);
    assert_eq!(
        mixed_query(&table, 16)
            .execute_scored()
            .await
            .unwrap()
            .len(),
        8
    );
}

#[tokio::test]
async fn caller_pinned_empty_and_snapshot_views_do_not_read_latest() {
    let table = table();
    append(&table, 0).await;
    index(&table, "ivf-flat", "embedding").await;
    index(&table, "full-text", "text").await;
    let snapshot = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    append(&table, 4).await;
    assert!(mixed_query(&table, 16)
        .with_snapshot(None)
        .execute_scored()
        .await
        .unwrap()
        .is_empty());
    table
        .file_io()
        .delete_file(&table.snapshot_manager().snapshot_path(snapshot.id()))
        .await
        .unwrap();
    let mut old = mixed_query(&table, 16)
        .with_snapshot(Some(&snapshot))
        .execute_scored()
        .await
        .unwrap()
        .row_ids;
    old.sort_unstable();
    assert_eq!(old, vec![0, 1, 2, 3]);
}

#[tokio::test]
async fn empty_tables_still_validate_route_and_partition_configuration() {
    let table = table();
    assert!(mixed_query(&table, 4)
        .execute_scored()
        .await
        .unwrap()
        .is_empty());
    let pb = PredicateBuilder::new(table.schema().fields());
    assert!(mixed_query(&table, 4)
        .with_partition_filter(pb.equal("id", Datum::Int(1)).unwrap())
        .is_err());
    let mut invalid = table.new_hybrid_search_builder();
    invalid
        .add_full_text_route("text", "plain text", 4, 1.0, HashMap::new())
        .unwrap()
        .with_limit(4);
    // Java does not parse the DSL until a route actually evaluates a corpus.
    assert!(invalid.execute_scored().await.unwrap().is_empty());
    append(&table, 0).await;
    index(&table, "full-text", "text").await;
    assert!(invalid.execute_scored().await.is_err());
    let mut missing = table.new_hybrid_search_builder();
    missing
        .add_vector_route("missing", vec![0.0, 0.0], 4, 1.0, HashMap::new())
        .unwrap()
        .with_limit(4);
    assert!(missing.execute_scored().await.is_err());
}
