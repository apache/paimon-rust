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

use crate::table::vector_search_test_utils::{de_vector_table, id_gt_filter, pk_vector_table};
use crate::vindex::IVF_FLAT_IDENTIFIER;
use arrow_array::{Float32Array, Int32Array, RecordBatch};
use futures::TryStreamExt;
use roaring::RoaringTreemap;

#[tokio::test]
async fn de_batch_results_read_projection_and_scores_from_the_search_snapshot() {
    let table = de_vector_table().await;
    let results = table
        .new_batch_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vectors(vec![vec![1.0, 0.0], vec![0.0, 1.0]])
        .with_limit(2)
        .execute()
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    let snapshot_id = results[0].snapshot_id().unwrap();
    assert_eq!(results[1].snapshot_id(), Some(snapshot_id));
    assert_eq!(results[0].row_ids().unwrap().row_ids, vec![0, 2]);
    assert_eq!(results[1].row_ids().unwrap().row_ids, vec![1, 2]);
    assert!(results[0].positions().is_err());

    // The result owns the resolved snapshot, so reading does not resolve latest.
    let manager = table.snapshot_manager();
    for id in manager.list_all_ids().await.unwrap() {
        table
            .file_io()
            .delete_file(&manager.snapshot_path(id))
            .await
            .unwrap();
    }
    for (result, expected_ids) in results.iter().zip([vec![1, 3], vec![2, 3]]) {
        let batches: Vec<RecordBatch> = result
            .new_read_builder()
            .with_projection(&["id"])
            .read()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let mut ids = Vec::new();
        let mut scores = Vec::new();
        for batch in batches {
            assert_eq!(
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>(),
                vec!["id", "__paimon_search_score"]
            );
            ids.extend_from_slice(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values(),
            );
            scores.extend_from_slice(
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .values(),
            );
        }
        assert_eq!(ids, expected_ids);
        assert_eq!(scores, result.row_ids().unwrap().scores);
    }
}

#[tokio::test]
async fn de_empty_filter_result_retains_snapshot_and_validates_projection() {
    let table = de_vector_table().await;
    let result = table
        .new_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vector(vec![1.0, 0.0])
        .with_limit(2)
        .with_filter(id_gt_filter(&table, 100))
        .execute()
        .await
        .unwrap();
    assert!(result.is_empty());
    assert!(result.snapshot_id().is_some());
    assert!(result
        .new_read_builder()
        .read()
        .await
        .unwrap()
        .try_next()
        .await
        .unwrap()
        .is_none());
    assert!(result
        .new_read_builder()
        .with_projection(&["_ROW_ID"])
        .read()
        .await
        .is_err());
}

#[tokio::test]
async fn pk_rejects_global_row_id_allow_lists_even_for_an_empty_table() {
    let table = pk_vector_table(&[
        ("pk-vector.index.columns", "embedding"),
        ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
        ("fields.embedding.pk-vector.distance.metric", "l2"),
        ("fields.embedding.dimension", "4"),
    ]);
    let err = table
        .new_batch_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vectors(vec![vec![1.0; 4]])
        .with_limit(2)
        .with_include_row_ids(RoaringTreemap::from_iter([0]))
        .execute()
        .await
        .unwrap_err();
    assert!(err.to_string().contains("global row-ID filters"));
}

#[tokio::test]
async fn public_plan_can_be_reused_by_single_and_batch_readers_after_builder_drop() {
    let table = de_vector_table().await;
    let mut builder = table.new_vector_search_builder();
    builder.with_vector_column("embedding");
    // Scan configuration does not depend on a query vector or Top-K.
    let scan = builder.new_scan().unwrap();
    assert!(builder.new_read().is_err());
    builder.with_query_vector(vec![1.0, 0.0]).with_limit(2);
    let read = builder.new_read().unwrap();
    drop(builder);
    let plan = scan.plan().await.unwrap();
    let snapshot_id = plan.snapshot_id();
    drop(scan);
    let single = read.read(plan.clone()).await.unwrap();
    assert_eq!(single.row_ids().unwrap().row_ids, vec![0, 2]);
    assert_eq!(single.snapshot_id(), snapshot_id);
    let batch = table
        .new_batch_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vectors(vec![vec![-1.0, 1.0], vec![1.0, -1.0]])
        .with_limit(1)
        .new_read()
        .unwrap();
    let results = batch.read(plan).await.unwrap();
    assert_eq!(results.len(), 2);
    for (result, expected) in results.iter().zip([vec![1], vec![0]]) {
        assert_eq!(result.row_ids().unwrap().row_ids, expected);
        assert_eq!(result.snapshot_id(), snapshot_id);
    }
}

#[tokio::test]
async fn public_reader_rejects_a_plan_for_another_table_or_filter() {
    let table = de_vector_table().await;
    let mut builder = table.new_vector_search_builder();
    builder
        .with_vector_column("embedding")
        .with_query_vector(vec![1.0, 0.0])
        .with_limit(1);
    let plan = builder.new_scan().unwrap().plan().await.unwrap();
    builder.with_filter(id_gt_filter(&table, 1));
    let error = builder
        .new_read()
        .unwrap()
        .read(plan.clone())
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("same table, column and pre-filter"));
    let other = crate::table::vector_search_test_utils::vector_test_table_at(
        "memory:/different_plan_table",
    );
    let read = other
        .new_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vector(vec![1.0, 0.0])
        .with_limit(1)
        .new_read()
        .unwrap();
    let error = read.read(plan).await.unwrap_err();
    assert!(error
        .to_string()
        .contains("same table, column and pre-filter"));
}
