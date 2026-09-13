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

use super::*;
use crate::io::{FileIO, FileIOBuilder};
use crate::lumina::{LEGACY_LUMINA_VECTOR_ANN_IDENTIFIER, LUMINA_IDENTIFIER};
use crate::spec::{
    BinaryRow, CoreOptions, DataField, DataType, FileKind, GlobalIndexMeta, IndexFileMeta,
    IndexManifestEntry, IntType, ROW_ID_FIELD_NAME,
};
use crate::table::pk_vector_position_read::{PKEY_VECTOR_POSITION_COLUMN, SEARCH_SCORE_COLUMN};
use crate::table::vector_search_common::VectorIndexBackend;
use crate::table::vector_search_common::{collect_ranked_rows, reorder_and_strip_position};
use crate::table::vector_search_test_utils::{
    build_vindex_segment_bytes, de_vector_table, id_gt_filter, pk_vector_table,
};
use crate::table::{find_field_id_by_name, RowRange};
use crate::vector_search::{ScoredRowIds, VectorSearch};
use crate::vindex::pkvector::metric::VectorSearchMetric;
use crate::vindex::IVF_FLAT_IDENTIFIER;
use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
use arrow_array::{Array, ArrayRef, Float32Array, Int32Array, Int64Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use futures::TryStreamExt;
use roaring::RoaringTreemap;
use std::collections::HashMap;
use std::sync::Arc;

fn l2_score(distance: f32) -> f32 {
    VectorSearchMetric::L2.distance_to_score(distance)
}

fn make_field(id: i32, name: &str) -> DataField {
    DataField::new(id, name.to_string(), DataType::Int(IntType::default()))
}

fn eval_context<'a>(
    file_io: &'a FileIO,
    options: &'a HashMap<String, String>,
    fields: &'a [DataField],
    next_row_id: Option<i64>,
) -> VectorSearchEvaluation<'a> {
    VectorSearchEvaluation {
        table: None,
        file_io,
        table_path: "memory:///test_table",
        table_options: options,
        schema_fields: fields,
        next_row_id,
    }
}

fn make_lumina_entry(
    file_name: &str,
    index_type: &str,
    kind: FileKind,
    index_field_id: i32,
) -> IndexManifestEntry {
    IndexManifestEntry {
        kind,
        partition: vec![],
        bucket: 0,
        index_file: IndexFileMeta {
            index_type: index_type.to_string(),
            file_name: file_name.to_string(),
            file_size: 100,
            row_count: 10,
            deletion_vectors_ranges: None,
            external_path: None,
            global_index_meta: Some(GlobalIndexMeta {
                row_range_start: 0,
                row_range_end: 9,
                index_field_id,
                extra_field_ids: None,
                source_meta: None,
                index_meta: None,
            }),
        },
        version: 1,
    }
}

// ---- Task B: search-and-read (`SearchResultReadBuilder::read`) tests ----

/// Build a small materialization batch: user column `id: Int32`, the internal
/// `_PKEY_VECTOR_POSITION: Int64`, and `__paimon_search_score: Float32` (mirroring
/// what `PkVectorIndexedSplitRead` emits for a single file).
fn materialized_batch(rows: &[(i32, i64, f32)]) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new(PKEY_VECTOR_POSITION_COLUMN, ArrowDataType::Int64, false),
        ArrowField::new(SEARCH_SCORE_COLUMN, ArrowDataType::Float32, false),
    ]));
    let ids = Int32Array::from(rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>());
    let positions = Int64Array::from(rows.iter().map(|(_, pos, _)| *pos).collect::<Vec<_>>());
    let scores = Float32Array::from(rows.iter().map(|(_, _, s)| *s).collect::<Vec<_>>());
    RecordBatch::try_new(
        schema,
        vec![Arc::new(ids), Arc::new(positions), Arc::new(scores)],
    )
    .unwrap()
}

fn i32_col(batch: &RecordBatch, name: &str) -> Vec<i32> {
    let idx = batch.schema().index_of(name).unwrap();
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .values()
        .to_vec()
}

fn f32_col(batch: &RecordBatch, name: &str) -> Vec<f32> {
    let idx = batch.schema().index_of(name).unwrap();
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap()
        .values()
        .to_vec()
}

#[test]
fn vindex_concurrency_limits_are_independent() {
    let default_options = HashMap::new();
    let default_core = CoreOptions::new(&default_options);
    assert_eq!(
        vindex_concurrency_limits(&default_core, 1, 32).unwrap(),
        (1, 64)
    );
    assert_eq!(
        vindex_concurrency_limits(&default_core, 8, 4).unwrap(),
        (4, 64)
    );

    let options = HashMap::from([(
        "global-index.vindex.read-thread-num".to_string(),
        "48".to_string(),
    )]);
    let core = CoreOptions::new(&options);
    assert_eq!(vindex_concurrency_limits(&core, 1, 32).unwrap(), (1, 48));
    assert_eq!(vindex_concurrency_limits(&core, 8, 4).unwrap(), (4, 48));
}

#[test]
fn test_find_field_id_by_name() {
    let fields = vec![make_field(1, "id"), make_field(2, "embedding")];
    assert_eq!(find_field_id_by_name(&fields, "embedding"), Some(2));
    assert_eq!(find_field_id_by_name(&fields, "nonexistent"), None);
}

#[test]
fn shared_include_filter_is_localized_once_per_index_shard() {
    let include_row_ids = RoaringTreemap::from_iter([101, 205, 999]);
    let localized =
        localize_shared_include_row_ids(&include_row_ids, &[(100, 109), (200, 209), (300, 309)])
            .unwrap();

    assert_eq!(
        localized[0].as_ref().unwrap().iter().collect::<Vec<_>>(),
        vec![1]
    );
    assert_eq!(
        localized[1].as_ref().unwrap().iter().collect::<Vec<_>>(),
        vec![5]
    );
    assert!(localized[2].is_none(), "an empty shard must be skipped");
}

#[test]
fn shared_batch_include_filter_requires_the_same_arc() {
    let shared = Arc::new(RoaringTreemap::from_iter([1, 2, 3]));
    let mut shared_searches = vec![
        VectorSearch::new(vec![1.0, 0.0], 2, "embedding".to_string()).unwrap(),
        VectorSearch::new(vec![0.0, 1.0], 2, "embedding".to_string()).unwrap(),
    ];
    for search in &mut shared_searches {
        search.set_shared_include_row_ids(Arc::clone(&shared));
    }
    let detected = shared_batch_include_row_ids(&shared_searches).unwrap();
    assert!(Arc::ptr_eq(detected, &shared));

    let mut equal_but_distinct = shared_searches.clone();
    equal_but_distinct[1]
        .set_shared_include_row_ids(Arc::new(RoaringTreemap::from_iter([1, 2, 3])));
    assert!(shared_batch_include_row_ids(&equal_but_distinct).is_none());

    let mut owned = shared_searches;
    owned[1] = owned[1]
        .clone()
        .with_include_row_ids(RoaringTreemap::from_iter([1, 2, 3]));
    assert!(shared_batch_include_row_ids(&owned).is_none());
}

#[test]
fn shared_raw_filter_does_not_expand_row_query_associations() {
    let shared = Arc::new(RoaringTreemap::from_iter(0..1_000));
    let mut searches = (0..128)
        .map(|_| VectorSearch::new(vec![1.0, 0.0], 2, "embedding".to_string()).unwrap())
        .collect::<Vec<_>>();
    for search in &mut searches {
        search.set_shared_include_row_ids(Arc::clone(&shared));
    }

    let plan = RawScoringPlan::new(&searches, RawVectorMetric::L2);
    let expanded_associations = plan
        .candidate_query_indices
        .values()
        .map(Vec::len)
        .sum::<usize>();

    assert_eq!(
        expanded_associations, 0,
        "one shared bitmap must stay O(B + Q), not expand to O(B * Q)"
    );
    assert_eq!(plan.shared_filter_groups.len(), 1);
    assert!(Arc::ptr_eq(
        &plan.shared_filter_groups[0].include_row_ids,
        &shared
    ));
    assert_eq!(plan.shared_filter_groups[0].query_indices.len(), 128);
}

#[test]
fn shared_raw_filter_prunes_unindexed_ranges_before_reading() {
    let shared = Arc::new(RoaringTreemap::from_iter([7, 1_000, 1_001, 900_000]));
    let mut searches = (0..128)
        .map(|_| VectorSearch::new(vec![1.0, 0.0], 2, "embedding".to_string()).unwrap())
        .collect::<Vec<_>>();
    for search in &mut searches {
        search.set_shared_include_row_ids(Arc::clone(&shared));
    }

    let raw_ranges = vec![RowRange::new(0, 999_999)];
    assert_eq!(
        prune_raw_ranges_by_include_row_ids(&raw_ranges, &searches).unwrap(),
        vec![
            RowRange::new(7, 7),
            RowRange::new(1_000, 1_001),
            RowRange::new(900_000, 900_000),
        ]
    );
}

#[test]
fn test_raw_vector_score_matches_java_metric_semantics() {
    let l2 = compute_raw_vector_score(&[1.0, 2.0], &[1.0, 4.0], RawVectorMetric::L2);
    assert!((l2 - 0.2).abs() < 1e-6);
    assert_eq!(
        compute_raw_vector_score(&[1.0, 2.0], &[3.0, 4.0], RawVectorMetric::InnerProduct),
        11.0
    );
    let cosine = compute_raw_vector_score(&[1.0, 0.0], &[1.0, 1.0], RawVectorMetric::Cosine);
    assert!((cosine - std::f32::consts::FRAC_1_SQRT_2).abs() < 1e-6);
    assert_eq!(
        compute_raw_vector_score(&[0.0, 0.0], &[1.0, 1.0], RawVectorMetric::Cosine),
        0.0
    );
}

#[test]
fn test_raw_vector_score_matrix_matches_scalar_metrics() {
    let stored = vec![1.0, 2.0, 3.0, 4.0, 0.0, 0.0];
    let queries = vec![1.0, 1.0, -1.0, 2.0];
    let query_indices = vec![0, 1];
    let query_l2_squared_norms = vec![2.0, 5.0];

    for metric in [
        RawVectorMetric::L2,
        RawVectorMetric::Cosine,
        RawVectorMetric::InnerProduct,
    ] {
        let matrix_scores = compute_raw_vector_score_matrix(
            &stored,
            3,
            &queries,
            2,
            2,
            &query_l2_squared_norms,
            &query_indices,
            metric,
        )
        .unwrap();
        for (row_index, stored_vector) in stored.as_chunks::<2>().0.iter().enumerate() {
            for (query_index, query) in queries.as_chunks::<2>().0.iter().enumerate() {
                let expected = compute_raw_vector_score(query, stored_vector, metric);
                let actual = matrix_scores[query_index * 3 + row_index];
                assert!(
                    (actual - expected).abs() < 1e-5,
                    "metric={metric:?}, row={row_index}, query={query_index}: {actual} != {expected}"
                );
            }
        }
    }

    let non_finite_score = compute_raw_vector_score_matrix(
        &[f32::INFINITY, 0.0],
        1,
        &[1.0, 0.0],
        1,
        2,
        &[1.0],
        &[0],
        RawVectorMetric::L2,
    )
    .unwrap()[0];
    assert_eq!(non_finite_score, 0.0);
}

#[test]
fn test_raw_vector_score_matrix_l2_preserves_large_finite_distances() {
    let dimension = 128;
    let query = vec![1.0e10_f32; dimension];
    let mut nearby = query.clone();
    nearby[0] += 1024.0;
    let mut stored = query.clone();
    stored.extend_from_slice(&nearby);
    let queries = query.repeat(4);
    let query_l2_squared_norm = query.iter().map(|value| value * value).sum::<f32>();
    let query_l2_squared_norms = vec![query_l2_squared_norm; 4];
    let query_indices = vec![0, 1, 2, 3];

    let matrix_scores = compute_raw_vector_score_matrix(
        &stored,
        2,
        &queries,
        4,
        dimension,
        &query_l2_squared_norms,
        &query_indices,
        RawVectorMetric::L2,
    )
    .unwrap();
    let exact_score = compute_raw_vector_score(&query, &query, RawVectorMetric::L2);
    let nearby_score = compute_raw_vector_score(&query, &nearby, RawVectorMetric::L2);

    for query_index in 0..4 {
        assert_eq!(matrix_scores[query_index * 2], exact_score);
        assert_eq!(matrix_scores[query_index * 2 + 1], nearby_score);
        assert!(matrix_scores[query_index * 2] > matrix_scores[query_index * 2 + 1]);
    }
}

#[test]
fn test_raw_vector_cosine_avoids_squared_norm_product_overflow() {
    let query = vec![1.0e15_f32, 0.0];
    let query_l2_squared_norm = query.iter().map(|value| value * value).sum::<f32>();
    assert!(query_l2_squared_norm.is_finite());
    let values = Float32Array::from(query.clone());
    let scalar_score = compute_raw_vector_score_from_values(
        &query,
        query_l2_squared_norm,
        &values,
        0,
        2,
        RawVectorMetric::Cosine,
    );
    assert!((scalar_score - 1.0).abs() < 1e-6);

    let queries = query.repeat(4);
    let matrix_scores = compute_raw_vector_score_matrix(
        &query,
        1,
        &queries,
        4,
        2,
        &[query_l2_squared_norm; 4],
        &[0, 1, 2, 3],
        RawVectorMetric::Cosine,
    )
    .unwrap();
    assert!(matrix_scores
        .iter()
        .all(|score| (*score - 1.0).abs() < 1e-6));
}

#[test]
fn test_raw_score_top_k_matches_full_sort_with_linear_partial_selection() {
    let limit = 7;
    let mut top_k = RawScoreTopK::new(limit);
    let mut batched_top_k = RawScoreTopK::new(limit);
    let mut expected = Vec::new();
    for row_id in 0..10_000 {
        let score = ((row_id * 37) % 101) as f32 / 10.0;
        let candidate = RawScoredRow { row_id, score };
        expected.push(candidate);
        top_k.offer(row_id, score);
        batched_top_k.offer_many(std::iter::once(candidate));
        assert!(top_k.candidates.len() < top_k.partition_size());
        assert!(batched_top_k.candidates.len() < batched_top_k.partition_size());
    }
    expected.sort_unstable_by(RawScoredRow::strongest_first);
    expected.truncate(limit);

    let result = top_k.into_search_result();
    let batched_result = batched_top_k.into_search_result();
    assert_eq!(
        result.row_ids,
        expected.iter().map(|row| row.row_id).collect::<Vec<_>>()
    );
    assert_eq!(
        result.scores,
        expected.iter().map(|row| row.score).collect::<Vec<_>>()
    );
    assert_eq!(batched_result.row_ids, result.row_ids);
    assert_eq!(batched_result.scores, result.scores);
}

#[test]
fn test_configured_raw_vector_metric_precedence_and_conflict_default() {
    let mut options = HashMap::new();
    options.insert(
        "fields.embedding.distance.metric".to_string(),
        "inner-product".to_string(),
    );
    options.insert("metric".to_string(), "cosine".to_string());
    assert_eq!(
        configured_raw_vector_metric(&options, "embedding").unwrap(),
        RawVectorMetric::InnerProduct
    );

    options.clear();
    options.insert("foo.metric".to_string(), "cosine".to_string());
    options.insert("bar.distance.metric".to_string(), "l2".to_string());
    assert_eq!(
        configured_raw_vector_metric(&options, "embedding").unwrap(),
        RawVectorMetric::L2
    );
}

#[tokio::test]
async fn test_resolve_raw_vector_metric_uses_vindex_manifest_metadata() {
    let file_io = FileIOBuilder::new("memory").build().unwrap();
    let mut entry = make_lumina_entry("missing.idx", IVF_FLAT_IDENTIFIER, FileKind::Add, 2);
    let index_meta = serde_json::to_vec(&HashMap::from([(
        "metric".to_string(),
        "cosine".to_string(),
    )]))
    .unwrap();
    entry
        .index_file
        .global_index_meta
        .as_mut()
        .unwrap()
        .index_meta = Some(index_meta);

    let metric = resolve_raw_vector_metric(
        &file_io,
        "memory:///test_table",
        &HashMap::new(),
        &[entry],
        2,
        "embedding",
    )
    .await
    .unwrap();

    assert_eq!(metric, RawVectorMetric::Cosine);
}

#[tokio::test]
async fn test_resolve_raw_vector_metric_falls_back_to_vindex_header() {
    let file_io = FileIOBuilder::new("memory").build().unwrap();
    let index = build_vindex_segment_bytes("inner_product");
    file_io
        .new_output("memory:///test_table/index/test.idx")
        .unwrap()
        .write(bytes::Bytes::from(index.clone()))
        .await
        .unwrap();
    for (file_size, index_meta) in [
        (index.len() as i64, br#"{"metric":"euclidean"}"#.to_vec()),
        (0, b"{}".to_vec()),
        (-1, b"{}".to_vec()),
    ] {
        let mut entry = make_lumina_entry("test.idx", IVF_FLAT_IDENTIFIER, FileKind::Add, 2);
        entry.index_file.file_size = file_size;
        entry
            .index_file
            .global_index_meta
            .as_mut()
            .unwrap()
            .index_meta = Some(index_meta);

        let metric = resolve_raw_vector_metric(
            &file_io,
            "memory:///test_table",
            &HashMap::new(),
            &[entry],
            2,
            "embedding",
        )
        .await
        .unwrap();

        assert_eq!(metric, RawVectorMetric::InnerProduct);
    }
}

#[test]
fn test_configured_refine_factor_precedence_and_aliases() {
    let table_options = HashMap::from([(
        "fields.embedding.ivf.refine-factor".to_string(),
        "3".to_string(),
    )]);
    let search_options = HashMap::from([(
        "fields.embedding.ivf_flat.rerank_factor".to_string(),
        "2".to_string(),
    )]);
    assert_eq!(
        configured_refine_factor(
            &search_options,
            &table_options,
            "embedding",
            IVF_FLAT_IDENTIFIER,
        )
        .unwrap(),
        2
    );

    assert_eq!(
        configured_refine_factor(
            &HashMap::new(),
            &table_options,
            "embedding",
            IVF_FLAT_IDENTIFIER,
        )
        .unwrap(),
        3
    );

    let global_options = HashMap::from([("rerank-factor".to_string(), "4".to_string())]);
    assert_eq!(
        configured_refine_factor(
            &HashMap::new(),
            &global_options,
            "embedding",
            LUMINA_IDENTIFIER,
        )
        .unwrap(),
        4
    );
}

#[test]
fn test_configured_refine_factor_rejects_invalid_values() {
    let zero_options = HashMap::from([("refine_factor".to_string(), "0".to_string())]);
    let err = configured_refine_factor(
        &zero_options,
        &HashMap::new(),
        "embedding",
        LUMINA_IDENTIFIER,
    )
    .unwrap_err();
    assert!(err.to_string().contains("must be positive"));

    let invalid_options = HashMap::from([("refine_factor".to_string(), "abc".to_string())]);
    let err = configured_refine_factor(
        &invalid_options,
        &HashMap::new(),
        "embedding",
        LUMINA_IDENTIFIER,
    )
    .unwrap_err();
    assert!(err.to_string().contains("Must be an integer"));

    assert!(indexed_search_limit(i32::MAX as usize, 2).is_err());
}

#[test]
fn test_collect_raw_batch_vector_batch_preserves_query_order() {
    let element_field = Arc::new(ArrowField::new("element", ArrowDataType::Float32, true));
    let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), 2).with_field(element_field);
    for vector in [[1.0, 0.0], [0.0, 1.0], [0.8, 0.2]] {
        builder.values().append_value(vector[0]);
        builder.values().append_value(vector[1]);
        builder.append(true);
    }
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new(
            "embedding",
            ArrowDataType::FixedSizeList(
                Arc::new(ArrowField::new("element", ArrowDataType::Float32, true)),
                2,
            ),
            true,
        ),
        ArrowField::new(ROW_ID_FIELD_NAME, ArrowDataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(builder.finish()) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(10), Some(11), Some(12)])) as ArrayRef,
        ],
    )
    .unwrap();
    let searches = vec![
        VectorSearch::new(vec![1.0, 0.0], 1, "embedding".to_string()).unwrap(),
        VectorSearch::new(vec![0.0, 1.0], 1, "embedding".to_string()).unwrap(),
        VectorSearch::new(vec![0.8, 0.2], 1, "embedding".to_string()).unwrap(),
        VectorSearch::new(vec![0.5, 0.5], 1, "embedding".to_string()).unwrap(),
    ];
    let scoring_plan = RawScoringPlan::new(&searches, RawVectorMetric::L2);
    let mut top_k = searches
        .iter()
        .map(|search| RawScoreTopK::new(search.limit))
        .collect::<Vec<_>>();

    collect_raw_batch_vector_batch(
        &batch,
        &searches,
        RawVectorMetric::L2,
        &scoring_plan,
        &mut top_k,
    )
    .unwrap();
    let results = top_k
        .into_iter()
        .map(RawScoreTopK::into_search_result)
        .collect::<Vec<_>>();

    assert_eq!(results[0].row_ids, vec![10]);
    assert_eq!(results[1].row_ids, vec![11]);
    assert_eq!(results[2].row_ids, vec![12]);
    assert_eq!(results[3].row_ids, vec![12]);
}

#[test]
fn test_collect_raw_batch_vector_batch_respects_fixed_size_list_offset() {
    let element_field = Arc::new(ArrowField::new("element", ArrowDataType::Float32, true));
    let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), 2).with_field(element_field);
    for vector in [[1.0, 0.0], [0.0, 1.0], [0.8, 0.2]] {
        builder.values().append_value(vector[0]);
        builder.values().append_value(vector[1]);
        builder.append(true);
    }
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new(
            "embedding",
            ArrowDataType::FixedSizeList(
                Arc::new(ArrowField::new("element", ArrowDataType::Float32, true)),
                2,
            ),
            true,
        ),
        ArrowField::new(ROW_ID_FIELD_NAME, ArrowDataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(builder.finish()) as ArrayRef,
            Arc::new(Int64Array::from(vec![10, 11, 12])) as ArrayRef,
        ],
    )
    .unwrap()
    .slice(1, 2);
    let searches = vec![VectorSearch::new(vec![0.0, 1.0], 1, "embedding".to_string()).unwrap()];
    let scoring_plan = RawScoringPlan::new(&searches, RawVectorMetric::L2);
    let mut top_k = vec![RawScoreTopK::new(1)];

    collect_raw_batch_vector_batch(
        &batch,
        &searches,
        RawVectorMetric::L2,
        &scoring_plan,
        &mut top_k,
    )
    .unwrap();

    assert_eq!(top_k.pop().unwrap().into_search_result().row_ids, vec![11]);
}

#[test]
fn test_collect_raw_batch_vector_batch_scores_only_include_row_ids() {
    let element_field = Arc::new(ArrowField::new("element", ArrowDataType::Float32, true));
    let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), 2).with_field(element_field);
    for vector in [[1.0, 0.0], [0.0, 1.0], [0.8, 0.2]] {
        builder.values().append_value(vector[0]);
        builder.values().append_value(vector[1]);
        builder.append(true);
    }
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new(
            "embedding",
            ArrowDataType::FixedSizeList(
                Arc::new(ArrowField::new("element", ArrowDataType::Float32, true)),
                2,
            ),
            true,
        ),
        ArrowField::new(ROW_ID_FIELD_NAME, ArrowDataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(builder.finish()) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(10), Some(11), Some(12)])) as ArrayRef,
        ],
    )
    .unwrap();
    let mut include_row_ids = RoaringTreemap::new();
    include_row_ids.insert(12);
    let searches = vec![
        VectorSearch::new(vec![1.0, 0.0], 2, "embedding".to_string())
            .unwrap()
            .with_include_row_ids(include_row_ids),
    ];
    let scoring_plan = RawScoringPlan::new(&searches, RawVectorMetric::L2);
    let mut top_k = searches
        .iter()
        .map(|search| RawScoreTopK::new(search.limit))
        .collect::<Vec<_>>();

    collect_raw_batch_vector_batch(
        &batch,
        &searches,
        RawVectorMetric::L2,
        &scoring_plan,
        &mut top_k,
    )
    .unwrap();
    let results = top_k
        .into_iter()
        .map(RawScoreTopK::into_search_result)
        .collect::<Vec<_>>();

    assert_eq!(results[0].row_ids, vec![12]);
    assert_eq!(results[0].scores.len(), 1);
}

#[tokio::test]
async fn test_batch_evaluate_no_matching_field_returns_empty_per_query() {
    let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
    let fields = vec![make_field(1, "id")];
    let searches = vec![
        VectorSearch::new(vec![1.0], 10, "embedding".to_string()).unwrap(),
        VectorSearch::new(vec![0.0], 10, "embedding".to_string()).unwrap(),
    ];
    let options = HashMap::new();

    let entry = make_lumina_entry(
        "test.idx",
        LEGACY_LUMINA_VECTOR_ANN_IDENTIFIER,
        FileKind::Add,
        99,
    );

    let results = evaluate_batch_vector_search(
        eval_context(&file_io, &options, &fields, None),
        &[entry],
        &searches,
    )
    .await
    .unwrap();

    assert_eq!(results.len(), searches.len());
    assert!(results.iter().all(ScoredRowIds::is_empty));
}

#[tokio::test]
async fn test_evaluate_no_matching_entries() {
    let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
    let fields = vec![make_field(1, "id"), make_field(2, "embedding")];
    let vs = VectorSearch::new(vec![1.0, 2.0], 10, "embedding".to_string()).unwrap();
    let options = HashMap::new();

    let entry = IndexManifestEntry {
        kind: FileKind::Add,
        partition: vec![],
        bucket: 0,
        index_file: IndexFileMeta {
            index_type: "btree".to_string(),
            file_name: "test.idx".to_string(),
            file_size: 100,
            row_count: 10,
            deletion_vectors_ranges: None,
            external_path: None,
            global_index_meta: None,
        },
        version: 1,
    };

    let result = evaluate_vector_search(
        eval_context(&file_io, &options, &fields, None),
        &[entry],
        &vs,
    )
    .await
    .unwrap();
    assert!(result.is_empty());
}

#[tokio::test]
async fn test_evaluate_ignores_non_vector_index_type() {
    let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
    let fields = vec![make_field(2, "embedding")];
    let vs = VectorSearch::new(vec![1.0], 10, "embedding".to_string()).unwrap();
    let options = HashMap::new();

    let entry = make_lumina_entry("test.idx", "btree", FileKind::Add, 2);

    let result = evaluate_vector_search(
        eval_context(&file_io, &options, &fields, None),
        &[entry],
        &vs,
    )
    .await
    .unwrap();
    assert!(result.is_empty());
}

#[tokio::test]
async fn test_evaluate_full_mode_without_vector_entries_uses_raw_path() {
    let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
    let fields = vec![make_field(2, "embedding")];
    let vs = VectorSearch::new(vec![1.0], 10, "embedding".to_string()).unwrap();
    let options = HashMap::from([("vector-index.search-mode".to_string(), "full".to_string())]);

    let err = evaluate_vector_search(
        eval_context(&file_io, &options, &fields, Some(10)),
        &[],
        &vs,
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("Vector raw search requires table context"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_evaluate_no_matching_field() {
    let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
    let fields = vec![make_field(1, "id")];
    let vs = VectorSearch::new(vec![1.0], 10, "embedding".to_string()).unwrap();
    let options = HashMap::new();

    let entry = make_lumina_entry(
        "test.idx",
        LEGACY_LUMINA_VECTOR_ANN_IDENTIFIER,
        FileKind::Add,
        99,
    );

    let result = evaluate_vector_search(
        eval_context(&file_io, &options, &fields, None),
        &[entry],
        &vs,
    )
    .await
    .unwrap();
    assert!(result.is_empty());
}

#[tokio::test]
async fn test_evaluate_skips_delete_entries() {
    let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
    let fields = vec![make_field(2, "embedding")];
    let vs = VectorSearch::new(vec![1.0], 10, "embedding".to_string()).unwrap();
    let options = HashMap::new();

    let entry = make_lumina_entry(
        "test.idx",
        LEGACY_LUMINA_VECTOR_ANN_IDENTIFIER,
        FileKind::Delete,
        2,
    );

    let result = evaluate_vector_search(
        eval_context(&file_io, &options, &fields, None),
        &[entry],
        &vs,
    )
    .await
    .unwrap();
    assert!(result.is_empty());
}

#[tokio::test]
async fn test_evaluate_accepts_canonical_lumina_index_type() {
    let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
    let fields = vec![make_field(2, "embedding")];
    let vs = VectorSearch::new(vec![1.0], 10, "embedding".to_string()).unwrap();
    let options = HashMap::new();

    let entry = make_lumina_entry("missing.idx", LUMINA_IDENTIFIER, FileKind::Add, 2);

    let err = evaluate_vector_search(
        eval_context(&file_io, &options, &fields, None),
        &[entry],
        &vs,
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("Failed to read Lumina index file 'missing.idx'"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_evaluate_accepts_legacy_lumina_index_type() {
    let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
    let fields = vec![make_field(2, "embedding")];
    let vs = VectorSearch::new(vec![1.0], 10, "embedding".to_string()).unwrap();
    let options = HashMap::new();

    let entry = make_lumina_entry(
        "missing.idx",
        LEGACY_LUMINA_VECTOR_ANN_IDENTIFIER,
        FileKind::Add,
        2,
    );

    let err = evaluate_vector_search(
        eval_context(&file_io, &options, &fields, None),
        &[entry],
        &vs,
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("Failed to read Lumina index file 'missing.idx'"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_evaluate_accepts_vindex_index_type() {
    let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
    let fields = vec![make_field(2, "embedding")];
    let vs = VectorSearch::new(vec![1.0], 10, "embedding".to_string()).unwrap();
    let options = HashMap::new();

    let entry = make_lumina_entry("missing.idx", IVF_FLAT_IDENTIFIER, FileKind::Add, 2);

    let err = evaluate_vector_search(
        eval_context(&file_io, &options, &fields, None),
        &[entry],
        &vs,
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("Failed to read vindex index file 'missing.idx'"),
        "unexpected error: {err}"
    );
    assert!(
        std::error::Error::source(&err).is_some(),
        "wrapped vindex read errors should retain their source: {err:?}"
    );
}

#[test]
fn test_single_vindex_outside_tokio_returns_error() {
    futures::executor::block_on(async {
        let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        file_io
            .new_output("memory:///test_table/index/test.idx")
            .unwrap()
            .write(bytes::Bytes::from_static(b"index"))
            .await
            .unwrap();
        let fields = vec![make_field(2, "embedding")];
        let vs = VectorSearch::new(vec![1.0], 10, "embedding".to_string()).unwrap();
        let options = HashMap::new();
        let entry = make_lumina_entry("test.idx", IVF_FLAT_IDENTIFIER, FileKind::Add, 2);

        let err = evaluate_vector_search(
            eval_context(&file_io, &options, &fields, None),
            &[entry],
            &vs,
        )
        .await
        .expect_err("vindex range reads outside Tokio should fail without panicking");

        assert!(
            matches!(err, crate::Error::UnexpectedError { ref message, .. }
                if message.contains("requires a Tokio runtime")),
            "unexpected error: {err:?}"
        );
    });
}

#[test]
fn test_batch_vindex_outside_tokio_uses_buffered_fallback() {
    futures::executor::block_on(async {
        let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let index = build_vindex_segment_bytes("l2");
        file_io
            .new_output("memory:///test_table/index/test.idx")
            .unwrap()
            .write(bytes::Bytes::from(index.clone()))
            .await
            .unwrap();
        let fields = vec![make_field(2, "embedding")];
        let searches = vec![
            VectorSearch::new(vec![1.0, 0.0], 2, "embedding".to_string()).unwrap(),
            VectorSearch::new(vec![0.0, 1.0], 2, "embedding".to_string()).unwrap(),
        ];
        let options = HashMap::new();
        let mut entry = make_lumina_entry("test.idx", IVF_FLAT_IDENTIFIER, FileKind::Add, 2);
        entry.index_file.file_size = index.len() as i64;
        entry.index_file.row_count = 3;
        entry
            .index_file
            .global_index_meta
            .as_mut()
            .unwrap()
            .row_range_end = 2;

        let results = evaluate_batch_vector_search(
            eval_context(&file_io, &options, &fields, None),
            &[entry],
            &searches,
        )
        .await
        .expect("batch vindex search should fall back to buffered I/O outside Tokio");

        assert_eq!(results.len(), searches.len());
        assert!(results.iter().all(|result| !result.is_empty()));
    });
}

#[test]
fn from_index_type_classifies_lumina_and_vindex() {
    assert_eq!(
        VectorIndexBackend::from_index_type("lumina"),
        Some(VectorIndexBackend::Lumina)
    );
    assert_eq!(
        VectorIndexBackend::from_index_type("lumina-vector-ann"),
        Some(VectorIndexBackend::Lumina)
    );
    assert_eq!(
        VectorIndexBackend::from_index_type("ivf-flat"),
        Some(VectorIndexBackend::Vindex)
    );
    for index_type in ["ivf-sq", "ivf-rq", "diskann"] {
        assert_eq!(
            VectorIndexBackend::from_index_type(index_type),
            Some(VectorIndexBackend::Vindex)
        );
    }
}

#[tokio::test]
async fn execute_filter_on_empty_de_path_returns_empty() {
    // No PK-vector index and no snapshot: the request follows the
    // data-evolution path. Scalar pre-filter support must not turn an empty
    // table into an error.
    let table = pk_vector_table(&[]);
    let filter = id_gt_filter(&table, 2);
    let result = table
        .new_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vector(vec![1.0])
        .with_limit(5)
        .with_filter(filter)
        .execute()
        .await
        .expect("an empty data-evolution search with a filter should succeed");
    assert!(result.is_empty());
}

#[test]
fn reorder_and_strip_position_recovers_best_first_and_drops_position() {
    // Single file, one bucket. The materialization reader emits rows in
    // ascending physical position [pos0, pos1, pos2] -> ids [40,41,42]. The
    // search candidates ranked them best-first as pos1(rank0), pos2(rank1),
    // pos0(rank2), which is NEITHER position order nor score order-by-batch.
    // The reorder must yield ids [41,42,40] and drop _PKEY_VECTOR_POSITION.
    let batch = materialized_batch(&[
        (40, 0, l2_score(9.0)),
        (41, 1, l2_score(1.0)),
        (42, 2, l2_score(4.0)),
    ]);
    let batches = vec![batch];
    let part = BinaryRow::new(0).to_serialized_bytes();
    let mut rank_of: HashMap<(Vec<u8>, i32, String, i64), usize> = HashMap::new();
    rank_of.insert((part.clone(), 0, "o.mosaic".to_string(), 1), 0);
    rank_of.insert((part.clone(), 0, "o.mosaic".to_string(), 2), 1);
    rank_of.insert((part.clone(), 0, "o.mosaic".to_string(), 0), 2);

    let mut ranked = Vec::new();
    collect_ranked_rows(&batches[0], 0, &part, 0, "o.mosaic", &rank_of, &mut ranked).unwrap();
    let out = reorder_and_strip_position(&batches, ranked).unwrap();
    assert_eq!(out.len(), 1);
    let out = &out[0];

    // Best-first row order, not ascending position order.
    assert_eq!(i32_col(out, "id"), vec![41, 42, 40]);
    // Score column preserved and aligned to the reordered rows.
    assert_eq!(
        f32_col(out, SEARCH_SCORE_COLUMN),
        vec![l2_score(1.0), l2_score(4.0), l2_score(9.0)]
    );
    // Position column dropped; _ROW_ID never present.
    assert!(out.schema().index_of(PKEY_VECTOR_POSITION_COLUMN).is_err());
    assert!(out.schema().index_of("_ROW_ID").is_err());
}

#[test]
fn reorder_and_strip_position_merges_rows_across_files() {
    // Two files (two materialization batches). Best-first interleaves them:
    // file-b pos0 (rank0), file-a pos1 (rank1), file-a pos0 (rank2). The
    // reorder must pull rows from both batches into one best-first output.
    let batch_a = materialized_batch(&[(10, 0, l2_score(9.0)), (11, 1, l2_score(1.0))]);
    let batch_b = materialized_batch(&[(20, 0, l2_score(0.5))]);
    let batches = vec![batch_a, batch_b];
    let part = BinaryRow::new(0).to_serialized_bytes();
    let mut rank_of: HashMap<(Vec<u8>, i32, String, i64), usize> = HashMap::new();
    rank_of.insert((part.clone(), 0, "b".to_string(), 0), 0);
    rank_of.insert((part.clone(), 0, "a".to_string(), 1), 1);
    rank_of.insert((part.clone(), 0, "a".to_string(), 0), 2);

    let mut ranked = Vec::new();
    collect_ranked_rows(&batches[0], 0, &part, 0, "a", &rank_of, &mut ranked).unwrap();
    collect_ranked_rows(&batches[1], 1, &part, 0, "b", &rank_of, &mut ranked).unwrap();
    let out = reorder_and_strip_position(&batches, ranked).unwrap();
    assert_eq!(i32_col(&out[0], "id"), vec![20, 11, 10]);
    assert_eq!(
        f32_col(&out[0], SEARCH_SCORE_COLUMN),
        vec![l2_score(0.5), l2_score(1.0), l2_score(9.0)]
    );
}

#[test]
fn reorder_and_strip_position_empty_yields_no_batches() {
    let out = reorder_and_strip_position(&[], Vec::new()).unwrap();
    assert!(out.is_empty());
}

#[test]
fn collect_ranked_rows_missing_candidate_fails_loud() {
    // A materialized position with no candidate rank must fail loud rather than
    // silently drop the row.
    let batch = materialized_batch(&[(40, 7, l2_score(1.0))]);
    let part = BinaryRow::new(0).to_serialized_bytes();
    let rank_of: HashMap<(Vec<u8>, i32, String, i64), usize> = HashMap::new();
    let mut ranked = Vec::new();
    let err = collect_ranked_rows(&batch, 0, &part, 0, "f", &rank_of, &mut ranked)
        .expect_err("missing candidate must fail loud");
    assert!(
        matches!(err, crate::Error::DataInvalid { ref message, .. } if message.contains("no matching search candidate")),
        "unexpected error: {err:?}"
    );
}

#[test]
fn attach_scores_reorders_by_rank_not_score() {
    use arrow_array::{Int32Array, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    // Two rows materialized in row-id order [10, 20]; ranks say 20 is best (rank 0),
    // 10 is rank 1. Scores tie at 0.5 to prove ordering follows rank, not score.
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(ROW_ID_FIELD_NAME, DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![100, 200])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )
    .unwrap();
    let mut map = HashMap::new();
    map.insert(20i64, (0usize, 0.5f32));
    map.insert(10i64, (1usize, 0.5f32));

    let out = attach_scores_by_row_id(&[batch], &map, 2).unwrap();
    assert_eq!(out.len(), 1);
    let b = &out[0];
    // _ROW_ID stripped, score appended.
    assert!(b.schema().index_of(ROW_ID_FIELD_NAME).is_err());
    let score_idx = b.schema().index_of("__paimon_search_score").unwrap();
    assert_eq!(
        b.schema().field(score_idx).data_type(),
        &arrow_schema::DataType::Float32
    );
    // Row order is rank order: id 200 (rank 0) first, then id 100 (rank 1).
    let ids = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(ids.values(), &[200, 100]);
}

#[test]
fn attach_scores_fails_on_unknown_row_id() {
    use arrow_array::{Int32Array, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(ROW_ID_FIELD_NAME, DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![99])),
        ],
    )
    .unwrap();
    let map: HashMap<i64, (usize, f32)> = HashMap::new(); // no entry for 99
    let err = attach_scores_by_row_id(&[batch], &map, 1).unwrap_err();
    assert!(matches!(err, crate::Error::DataInvalid { .. }));
}

#[test]
fn attach_scores_fails_on_count_mismatch() {
    use arrow_array::{Int32Array, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(ROW_ID_FIELD_NAME, DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![10])),
        ],
    )
    .unwrap();
    let mut map = HashMap::new();
    map.insert(10i64, (0usize, 0.5f32));
    // expected_len 2 but only 1 row materialized.
    let err = attach_scores_by_row_id(&[batch], &map, 2).unwrap_err();
    assert!(matches!(err, crate::Error::DataInvalid { .. }));
}

#[test]
fn attach_scores_fails_on_null_row_id() {
    use arrow_array::{Int32Array, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    // _ROW_ID column has a NULL at row 1; the map contains the non-null id, so
    // the failure is specifically the null (not an unknown id).
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(ROW_ID_FIELD_NAME, DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![Some(10i64), None])),
        ],
    )
    .unwrap();
    let mut map = HashMap::new();
    map.insert(10i64, (0usize, 0.5f32));
    let err = attach_scores_by_row_id(&[batch], &map, 2).unwrap_err();
    assert!(matches!(err, crate::Error::DataInvalid { .. }));
}

#[test]
fn attach_scores_fails_on_wrong_type_row_id() {
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    // _ROW_ID column is Int32, not Int64: the downcast fails loud.
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(ROW_ID_FIELD_NAME, DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![10])),
        ],
    )
    .unwrap();
    let mut map = HashMap::new();
    map.insert(10i64, (0usize, 0.5f32));
    let err = attach_scores_by_row_id(&[batch], &map, 1).unwrap_err();
    assert!(matches!(err, crate::Error::DataInvalid { .. }));
}

#[tokio::test]
async fn de_result_read_materializes_rows_with_score() {
    // A data-evolution vector table with a committed global index: result_read
    // must materialize one row per scored hit and carry the unified score
    // column, in best-first rank order.
    let table = de_vector_table().await;
    let query = vec![1.0, 0.0];

    let scored = table
        .new_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vector(query.clone())
        .with_limit(3)
        .execute()
        .await
        .unwrap();
    assert!(!scored.is_empty(), "DE search must return hits");

    let mut stream = async {
        table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(query)
            .with_limit(3)
            .execute()
            .await?
            .new_read_builder()
            .read()
            .await
    }
    .await
    .unwrap();

    let mut rows = 0usize;
    let mut saw_score = false;
    while let Some(batch) = stream.try_next().await.unwrap() {
        rows += batch.num_rows();
        saw_score |= batch.schema().index_of(SEARCH_SCORE_COLUMN).is_ok();
    }
    assert_eq!(
        rows,
        scored.len(),
        "DE read must emit exactly the scored result count"
    );
    assert!(
        saw_score,
        "DE read output must carry the search score column"
    );
}

#[tokio::test]
async fn de_result_read_applies_scalar_filter_before_top_k() {
    // Row id=1 is the closest vector to [1, 0], but the scalar filter excludes
    // it. Filter-before-Top-K must return the best rows among ids > 1 instead
    // of recalling id=1 first and filtering it after the search.
    let table = de_vector_table().await;
    let filter = id_gt_filter(&table, 1);
    let mut stream = async {
        table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0, 0.0])
            .with_limit(2)
            .with_filter(filter)
            .execute()
            .await?
            .new_read_builder()
            .read()
            .await
    }
    .await
    .expect("DE vector search should support a scalar pre-filter");

    let mut ids = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let id = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        ids.extend((0..id.len()).map(|row| id.value(row)));
    }

    assert_eq!(ids, vec![3, 2]);
}
