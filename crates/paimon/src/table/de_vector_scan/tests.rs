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
use crate::spec::CoreOptions;
use crate::table::de_vector_read::DeVectorRead;
use crate::table::vector_read::Read;
use crate::table::vector_search_test_utils::{
    de_vector_table, id_gt_filter, vector_test_table, vector_test_table_at,
};
use crate::table::{TableCommit, TableWrite};
use crate::vector_search::SearchResult;
use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{ArrayRef, Float32Array, Int32Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use roaring::RoaringTreemap;
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn reader_uses_planned_snapshot_after_snapshot_files_are_removed() {
    let table = de_vector_table().await;
    let options = HashMap::new();
    let scan = DeVectorScan::new(&table, None, None, None);
    let read = DeVectorRead::new("embedding", &[&[1.0, 0.0]], 2, &options).unwrap();
    let plan = scan.plan().await.unwrap();
    let snapshot_id = plan.table.travel_snapshot().unwrap().id();
    let manager = table.snapshot_manager();
    for id in manager.list_all_ids().await.unwrap() {
        table
            .file_io()
            .delete_file(&manager.snapshot_path(id))
            .await
            .unwrap();
    }
    assert!(manager.get_snapshot(snapshot_id).await.is_err());

    // Re-resolving "latest" here would lose the scan's index and row-ID context.
    let result = read.read(plan).await.unwrap();
    let result = result.into_iter().next().unwrap();
    assert_eq!(result.snapshot_id(), Some(snapshot_id));
    let batches: Vec<RecordBatch> = result
        .new_read_builder()
        .read()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);
    let ids = batches[0]
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let scores = batches[0]
        .column_by_name("__paimon_search_score")
        .unwrap()
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert_eq!(ids.values().as_ref(), &[1, 3]);
    assert_eq!(scores.values().as_ref(), &[1.0, 1.0]);
}

#[tokio::test]
async fn reader_reuses_queries_without_leaking_plan_filters() {
    let table = de_vector_table().await;
    let read =
        DeVectorRead::new("embedding", &[&[1.0, 0.0], &[0.0, 1.0]], 2, &HashMap::new()).unwrap();
    let filter = id_gt_filter(&table, 1);
    let empty_filter = id_gt_filter(&table, 99);
    // One reader executes filtered, empty, and unfiltered plans. The plan owns
    // the predicate; neither it nor the empty result can alter later queries.
    for (filter, expected) in [
        (Some(&filter), [vec![2, 1], vec![1, 2]]),
        (Some(&empty_filter), [vec![], vec![]]),
        (None, [vec![0, 2], vec![1, 2]]),
    ] {
        let plan = DeVectorScan::new(&table, filter, None, None)
            .plan()
            .await
            .unwrap();
        let snapshot_id = plan.table.travel_snapshot().unwrap().id();
        let results = read.read(plan).await.unwrap();
        assert_eq!(results.len(), 2, "empty plans must preserve query count");
        for (result, row_ids) in results.iter().zip(expected) {
            assert_eq!(result.snapshot_id(), Some(snapshot_id));
            assert_eq!(result.row_ids().unwrap().row_ids, row_ids);
        }
    }
}

#[tokio::test]
async fn empty_prepared_filter_retains_its_snapshot_without_resolving_latest() {
    let table = de_vector_table().await;
    let prepared = table
        .prepare_vector_search_filter(id_gt_filter(&table, 99))
        .await
        .unwrap();
    let snapshot_id = prepared.table().travel_snapshot().unwrap().id();
    assert!(prepared.include_row_ids().is_empty());
    let manager = table.snapshot_manager();
    for id in manager.list_all_ids().await.unwrap() {
        table
            .file_io()
            .delete_file(&manager.snapshot_path(id))
            .await
            .unwrap();
    }

    let results = table
        .new_batch_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vectors(vec![vec![1.0, 0.0], vec![0.0, 1.0]])
        .with_limit(2)
        .with_prepared_filter(prepared)
        .execute()
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    for result in results {
        assert!(result.is_empty());
        assert_eq!(result.snapshot_id(), Some(snapshot_id));
        assert!(result
            .new_read_builder()
            .read()
            .await
            .unwrap()
            .try_next()
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn prepared_filter_cannot_bypass_builder_target_query_auth() {
    let source = vector_test_table();
    let prepared = source
        .prepare_vector_search_filter(id_gt_filter(&source, 0))
        .await
        .unwrap();
    let target = source.copy_with_options(HashMap::from([(
        "query-auth.enabled".to_string(),
        "true".to_string(),
    )]));

    let err = target
        .new_batch_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vectors(vec![vec![1.0, 0.0]])
        .with_limit(1)
        .with_prepared_filter(prepared)
        .execute()
        .await
        .expect_err("a stale prepared filter must not bypass current target authorization");

    assert!(
        matches!(err, crate::Error::Unsupported { ref message } if message.contains("query-auth.enabled")),
        "builder target authorization must remain authoritative, got: {err:?}"
    );
}

#[tokio::test]
async fn de_vector_search_uses_time_travel_snapshot() {
    let table = de_vector_table().await;
    let latest = table
        .new_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vector(vec![1.0, 0.0])
        .with_limit(3)
        .execute()
        .await
        .unwrap();
    assert!(
        !latest.is_empty(),
        "latest snapshot should contain the committed vector index"
    );

    let traveled = table
        .copy_with_time_travel(HashMap::from([(
            crate::spec::SCAN_VERSION_OPTION.to_string(),
            "1".to_string(),
        )]))
        .await
        .unwrap();
    assert_eq!(
        traveled.travel_snapshot().map(|snapshot| snapshot.id()),
        Some(1)
    );

    let historical = traveled
        .new_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vector(vec![1.0, 0.0])
        .with_limit(3)
        .execute()
        .await
        .unwrap();
    assert!(
        historical.is_empty(),
        "snapshot 1 predates the vector index and should return no hits"
    );
}

#[tokio::test]
async fn resolved_vector_snapshot_can_be_reused_by_all_read_stages() {
    let table = de_vector_table().await;
    let snapshot = crate::table::time_travel::resolve_snapshot(&table)
        .await
        .unwrap()
        .unwrap();
    let pinned = table.copy_with_resolved_snapshot(&snapshot).await.unwrap();

    assert_eq!(
        pinned.travel_snapshot().map(|snapshot| snapshot.id()),
        Some(snapshot.id())
    );
    let options = CoreOptions::new(pinned.schema().options());
    let selector = options.try_time_travel_selector().unwrap().unwrap();
    assert!(matches!(
        selector,
        crate::spec::TimeTravelSelector::SnapshotId {
            value,
            option_name: crate::spec::SCAN_SNAPSHOT_ID_OPTION,
        } if value == snapshot.id().to_string()
    ));
}

#[tokio::test]
async fn de_scalar_filter_with_no_matching_rows_returns_empty() {
    let table = de_vector_table().await;
    let filter = id_gt_filter(&table, 99);

    let result = table
        .new_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vector(vec![1.0, 0.0])
        .with_limit(2)
        .with_filter(filter.clone())
        .execute()
        .await
        .unwrap();
    assert!(result.is_empty());

    let results = table
        .new_batch_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vectors(vec![vec![1.0, 0.0], vec![0.0, 1.0]])
        .with_limit(2)
        .with_filter(filter)
        .execute()
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(SearchResult::is_empty));
}

#[tokio::test]
async fn prepared_de_scalar_filter_can_be_reused_by_batch_search() {
    let table = de_vector_table().await;
    let prepared = table
        .prepare_vector_search_filter(id_gt_filter(&table, 1))
        .await
        .unwrap();
    let results = table
        .new_batch_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vectors(vec![vec![1.0, 0.0], vec![0.0, 1.0]])
        .with_limit(2)
        .with_prepared_filter(prepared)
        .execute()
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].row_ids().unwrap().row_ids, vec![2, 1]);
    assert_eq!(results[1].row_ids().unwrap().row_ids, vec![1, 2]);
}

#[tokio::test]
async fn prepared_filter_from_different_table_is_rejected() {
    let prepared = PreparedVectorSearchFilter {
        table: vector_test_table_at("memory:/prepared_filter_source"),
        include_row_ids: Arc::new(RoaringTreemap::from_iter([1])),
    };
    let target = vector_test_table_at("memory:/prepared_filter_target");

    let error = target
        .new_batch_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vectors(vec![vec![1.0, 0.0]])
        .with_limit(1)
        .with_prepared_filter(prepared)
        .execute()
        .await
        .expect_err("a prepared filter must not retarget the builder to another table");

    assert!(
        error.to_string().contains("different table"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn de_scalar_filter_applies_to_unindexed_raw_fallback() {
    let table = de_vector_table().await;
    let element_field = Arc::new(ArrowField::new("element", ArrowDataType::Float32, true));
    let mut vector_builder =
        ListBuilder::new(Float32Builder::new()).with_field(element_field.clone());
    vector_builder.values().append_value(1.0);
    vector_builder.values().append_value(0.0);
    vector_builder.append(true);
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("embedding", ArrowDataType::List(element_field), true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![4])) as ArrayRef,
            Arc::new(vector_builder.finish()) as ArrayRef,
        ],
    )
    .unwrap();
    let mut writer = TableWrite::new(&table, "test-user".to_string()).unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    TableCommit::new(table.clone(), "test-user".to_string())
        .commit(messages)
        .await
        .unwrap();

    let table = table.copy_with_options(HashMap::from([
        ("vector-index.search-mode".to_string(), "full".to_string()),
        ("scalar-index.search-mode".to_string(), "full".to_string()),
    ]));
    let result = table
        .new_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vector(vec![1.0, 0.0])
        .with_limit(1)
        .with_filter(id_gt_filter(&table, 3))
        .execute()
        .await
        .unwrap();

    assert_eq!(result.row_ids().unwrap().row_ids, vec![3]);
}

#[tokio::test]
async fn unevaluable_scalar_filter_routes_covered_ranges_to_raw_in_every_mode() {
    let table = de_vector_table().await;
    for mode in ["fast", "full", "detail"] {
        let table = table.copy_with_options(HashMap::from([(
            "vector-index.search-mode".to_string(),
            mode.to_string(),
        )]));
        let filter = id_gt_filter(&table, 0);
        let mut plan = DeVectorScan::new(&table, Some(&filter), None, None)
            .plan()
            .await
            .unwrap();
        // Keep the committed IVF shards but remove the scalar index from this
        // plan. A data-derived ANN allow-list can lose hits with nprobe=1.
        plan.index_entries
            .retain(|entry| entry.index_file.index_type == "ivf-flat");
        let read = DeVectorRead::new(
            "embedding",
            &[&[1.0, 0.0], &[0.0, 1.0]],
            3,
            &HashMap::from([("ivf.nprobe".to_string(), "1".to_string())]),
        )
        .unwrap();
        let results = read.read(plan).await.unwrap();
        for result in results {
            let mut ids = result.row_ids().unwrap().row_ids.clone();
            ids.sort_unstable();
            assert_eq!(ids, vec![0, 1, 2], "{mode}");
        }
    }
}

#[tokio::test]
async fn exact_and_partial_scalar_indexes_obey_refinement_switch() {
    let table = de_vector_table().await;
    let predicate = crate::spec::PredicateBuilder::new(table.schema().fields());
    let exact = id_gt_filter(&table, 1);
    let partial = Predicate::and(vec![
        exact.clone(),
        predicate.is_not_null("embedding").unwrap(),
    ]);
    let contradiction = Predicate::and(vec![exact.clone(), Predicate::negate(exact.clone())]);
    for mode in ["fast", "full", "detail"] {
        for refine in [false, true] {
            let table = table.copy_with_options(HashMap::from([
                ("vector-index.search-mode".to_string(), mode.to_string()),
                (
                    "global-index.filter.refine-from-data".to_string(),
                    refine.to_string(),
                ),
            ]));
            for (filter, expected) in [
                (&exact, 2),
                (&partial, if refine { 2 } else { 0 }),
                (&contradiction, 0),
            ] {
                let results = table
                    .new_batch_vector_search_builder()
                    .with_vector_column("embedding")
                    .with_query_vectors(vec![vec![1.0, 0.0], vec![0.0, 1.0]])
                    .with_limit(3)
                    .with_options(HashMap::from([("ivf.nprobe".to_string(), "2".to_string())]))
                    .with_filter(filter.clone())
                    .execute()
                    .await
                    .unwrap();
                assert_eq!(results.len(), 2);
                for result in results {
                    assert_eq!(
                        result.row_ids().unwrap().len(),
                        expected,
                        "{mode}, refine={refine}"
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn scalar_filter_ignores_composite_index_definitions() {
    let table = de_vector_table().await.copy_with_options(HashMap::from([
        ("vector-index.search-mode".to_string(), "fast".to_string()),
        (
            "global-index.filter.refine-from-data".to_string(),
            "false".to_string(),
        ),
    ]));
    let filter = id_gt_filter(&table, 0);
    let mut plan = DeVectorScan::new(&table, Some(&filter), None, None)
        .plan()
        .await
        .unwrap();
    let embedding_id =
        crate::table::find_field_id_by_name(table.schema().fields(), "embedding").unwrap();
    for entry in &mut plan.index_entries {
        if entry.index_file.index_type == "btree" {
            // A multi-field definition cannot supply Java's per-column scalar
            // coverage, regardless of whether its leading field is indexed.
            entry
                .index_file
                .global_index_meta
                .as_mut()
                .unwrap()
                .extra_field_ids = Some(vec![embedding_id]);
        }
    }
    let results = DeVectorRead::new("embedding", &[&[1.0, 0.0]], 3, &HashMap::new())
        .unwrap()
        .read(plan)
        .await
        .unwrap();
    assert_eq!(results[0].row_ids().unwrap().len(), 3);
}

#[tokio::test]
async fn scalar_coverage_gaps_follow_scalar_and_vector_modes() {
    for vector_covers_tail in [false, true] {
        let table = de_vector_table().await;
        let element = Arc::new(ArrowField::new("element", ArrowDataType::Float32, true));
        let mut vectors = ListBuilder::new(Float32Builder::new()).with_field(element.clone());
        vectors.values().append_slice(&[3.0, 3.0]);
        vectors.append(true);
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                ArrowField::new("id", ArrowDataType::Int32, false),
                ArrowField::new("embedding", ArrowDataType::List(element), true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![4])) as ArrayRef,
                Arc::new(vectors.finish()) as ArrayRef,
            ],
        )
        .unwrap();
        let mut writer = TableWrite::new(&table, "coverage-test".to_string()).unwrap();
        writer.write_arrow_batch(&batch).await.unwrap();
        TableCommit::new(table.clone(), "coverage-test".to_string())
            .commit(writer.prepare_commit().await.unwrap())
            .await
            .unwrap();
        if vector_covers_tail {
            table
                .new_vindex_index_build_builder(crate::vindex::IVF_FLAT_IDENTIFIER)
                .with_index_column("embedding")
                .with_options(HashMap::from([(
                    "ivf-flat.nlist".to_string(),
                    "1".to_string(),
                )]))
                .execute()
                .await
                .unwrap();
        }
        let filter = id_gt_filter(&table, 3);
        for scalar_mode in ["fast", "full", "detail"] {
            for vector_mode in ["fast", "full", "detail"] {
                for builder_options in [false, true] {
                    let modes = HashMap::from([
                        (
                            "vector-index.search-mode".to_string(),
                            vector_mode.to_string(),
                        ),
                        (
                            "scalar-index.search-mode".to_string(),
                            scalar_mode.to_string(),
                        ),
                    ]);
                    let mut table_options = if builder_options {
                        HashMap::from([
                            ("vector-index.search-mode".to_string(), "fast".to_string()),
                            ("scalar-index.search-mode".to_string(), "fast".to_string()),
                        ])
                    } else {
                        modes.clone()
                    };
                    table_options.insert(
                        "global-index.filter.refine-from-data".to_string(),
                        "false".to_string(),
                    );
                    let table = table.copy_with_options(table_options);
                    let mut builder = table.new_batch_vector_search_builder();
                    builder
                        .with_vector_column("embedding")
                        .with_query_vectors(vec![vec![0.0, 0.0], vec![1.0, 0.0]])
                        .with_limit(10)
                        .with_filter(filter.clone());
                    if builder_options {
                        builder.with_options(modes);
                    }
                    let expected = if (vector_covers_tail && scalar_mode != "fast")
                        || (!vector_covers_tail && vector_mode != "fast")
                    {
                        vec![3]
                    } else {
                        vec![]
                    };
                    let results = builder.execute().await.unwrap();
                    assert_eq!(results.len(), 2);
                    for result in results {
                        assert_eq!(result.row_ids().unwrap().row_ids, expected,
                            "scalar={scalar_mode}, vector={vector_mode}, vector_covers_tail={vector_covers_tail}, builder_options={builder_options}");
                    }
                }
            }
        }
    }
}
