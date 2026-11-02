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
use crate::catalog::Identifier;
use crate::io::FileIOBuilder;
use crate::spec::{DataType, IntType, Schema, TableSchema};

fn scored(ids: &[u64], scores: &[f32], weight: f32) -> HybridSearchWeightedResult {
    HybridSearchWeightedResult::new(
        GlobalIndexResult::from_scored_row_ids(ScoredRowIds::new(ids.to_vec(), scores.to_vec())),
        weight,
    )
    .unwrap()
}

#[test]
fn ranker_fuses_external_results_with_weights_and_deterministic_ties() {
    let results = [
        scored(&[1, 2, 3], &[10.0, 5.0, 0.0], 2.0),
        scored(&[2, 3, 4], &[100.0, 50.0, 0.0], 1.0),
    ];
    for (ranker, ids, expected) in [
        (
            HybridSearchRanker::Rrf,
            vec![2, 3, 1],
            vec![2.0 / 62.0 + 1.0 / 61.0, 2.0 / 63.0 + 1.0 / 62.0, 2.0 / 61.0],
        ),
        (
            HybridSearchRanker::WeightedScore,
            vec![1, 2, 3],
            vec![2.0, 2.0, 0.5],
        ),
        (
            HybridSearchRanker::Mrr,
            vec![1, 2, 3],
            vec![2.0, 2.0, 2.0 / 3.0 + 0.5],
        ),
    ] {
        let ranked = ranker.rank(&results, 3).unwrap();
        let hits = ranked.row_ids().unwrap();
        assert_eq!(hits.row_ids, ids, "ranker {ranker:?}");
        for (actual, expected) in hits.scores.iter().zip(expected) {
            assert!((actual - expected).abs() < 1e-6, "ranker {ranker:?}");
        }
        assert!(ranked.table().is_none());
    }

    // Callers may rank an arbitrary subset or repeat a result. Only each
    // supplied result's weight participates, independently of a route list.
    let repeated = HybridSearchRanker::Mrr
        .rank(&[results[0].clone(), results[0].clone()], 1)
        .unwrap();
    assert_eq!(repeated.row_ids().unwrap().row_ids, vec![1]);
    assert_eq!(repeated.row_ids().unwrap().scores, vec![4.0]);
}

#[test]
fn weighted_results_normalize_empty_selections_and_reject_invalid_inputs() {
    let empty =
        HybridSearchWeightedResult::new(GlobalIndexResult::from_row_ranges(vec![]), 2.0).unwrap();
    assert_eq!(empty.weight(), 2.0);
    assert!(empty.result().row_ids().unwrap().is_empty());
    assert!(HybridSearchRanker::Rrf
        .rank(&[empty], 2)
        .unwrap()
        .is_empty());
    assert!(HybridSearchRanker::Mrr.rank(&[], 2).unwrap().is_empty());
    assert!(HybridSearchRanker::Rrf.rank(&[], 0).is_err());
    assert!(HybridSearchWeightedResult::new(
        GlobalIndexResult::from_row_ranges(vec![RowRange::new(1, 1)]),
        1.0,
    )
    .is_err());
    assert!(
        HybridSearchWeightedResult::new(GlobalIndexResult::from_splits(Some(1), vec![]), 1.0,)
            .is_err()
    );
    for weight in [0.0, -1.0, f32::INFINITY] {
        assert!(HybridSearchWeightedResult::new(
            GlobalIndexResult::from_scored_row_ids(ScoredRowIds::empty()),
            weight,
        )
        .is_err());
    }
}

#[test]
fn builder_ranks_supplied_results_without_authentication_or_query_validation() {
    // No metadata exists and this query could never execute: rank consumes
    // evaluated scores only, just as Java HybridSearchBuilder.rank does.
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .option("query-auth.enabled", "true")
        .build()
        .unwrap();
    let table = Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("default", "rank-only"),
        "memory:/rank-only-no-metadata".into(),
        TableSchema::new(0, &schema),
        None,
    );
    let route = HybridSearchRoute::vector("missing", vec![0.0], 1, 2.0, HashMap::new()).unwrap();
    let result = HybridSearchRouteResult::new(
        route.clone(),
        GlobalIndexResult::from_scored_row_ids(ScoredRowIds::new(vec![7], vec![0.5])),
    )
    .unwrap();
    assert_eq!(result.route().field_name(), "missing");
    let mut builder = table.new_hybrid_search_builder();
    builder.add_route(route).with_limit(1).with_mrr_ranker();
    let ranked = builder.rank(&[result]).unwrap();
    assert_eq!(ranked.row_ids().unwrap().row_ids, vec![7]);
    assert_eq!(ranked.row_ids().unwrap().scores, vec![2.0]);
    builder.with_limit(0);
    assert!(builder.rank(&[]).is_err());
    assert!(table.new_hybrid_search_builder().rank(&[]).is_err());
}

mod captured_routes {
    use super::*;
    use crate::spec::{Datum, PredicateBuilder};

    use super::super::parity_tests::{append, index, mixed_query, table};

    #[tokio::test]
    async fn routes_share_one_snapshot_and_remain_owned_after_later_commits() {
        let table = table();
        append(&table, 0).await;
        index(&table, "ivf-flat", "embedding").await;
        index(&table, "full-text", "text").await;
        let predicates = PredicateBuilder::new(table.schema().fields());
        let keep = predicates
            .equal("label", Datum::String("keep".into()))
            .unwrap();
        let (routes, expected) = {
            let mut builder = mixed_query(&table, 4);
            builder.with_filter(keep);
            let expected = builder.execute().await.unwrap();
            (builder.route_builders().await.unwrap(), expected)
        };
        assert_eq!(routes.len(), 2);
        assert!(Arc::ptr_eq(routes[0].table(), routes[1].table()));
        assert_eq!(
            routes[0].table().travel_snapshot().map(|s| s.id()),
            expected.snapshot_id()
        );
        assert!(routes[0].vector_search_builder().is_ok());
        assert!(routes[0].full_text_search_builder().is_err());
        assert!(routes[1].full_text_search_builder().is_ok());
        assert!(routes[1].vector_search_builder().is_err());

        append(&table, 4).await;
        let mut results = vec![];
        for route in &routes {
            let result = route.execute().await.unwrap();
            assert_eq!(result.snapshot_id(), expected.snapshot_id());
            results.push(HybridSearchRouteResult::new(route.route().clone(), result).unwrap());
        }
        let builder = mixed_query(&table, 4);
        let ranked = builder.rank(&results).unwrap();
        assert_eq!(ranked.snapshot_id(), expected.snapshot_id());
        assert_eq!(
            ranked.row_ids().unwrap().row_ids,
            expected.row_ids().unwrap().row_ids
        );
        assert_eq!(
            ranked.row_ids().unwrap().scores,
            expected.row_ids().unwrap().scores
        );
        assert!(ranked
            .row_ids()
            .unwrap()
            .row_ids
            .iter()
            .all(|id| [1, 2].contains(id)));
        // Repeating a route uses its retained metadata rather than latest.
        assert_eq!(
            routes[0].execute().await.unwrap().snapshot_id(),
            expected.snapshot_id()
        );
        assert_ne!(
            builder.execute().await.unwrap().snapshot_id(),
            expected.snapshot_id()
        );
    }

    #[tokio::test]
    async fn empty_route_view_stays_empty_after_a_first_commit() {
        let table = table();
        let builder = mixed_query(&table, 4);
        let routes = builder.route_builders().await.unwrap();
        assert!(Arc::ptr_eq(routes[0].table(), routes[1].table()));
        assert!(routes[0].table().is_time_traveled());
        assert!(routes[0].table().travel_snapshot().is_none());
        append(&table, 0).await;
        let mut results = vec![];
        for route in &routes {
            results.push(
                builder
                    .to_route_result(route, route.execute().await.unwrap())
                    .unwrap(),
            );
        }
        let ranked = builder.rank(&results).unwrap();
        assert!(ranked.is_empty());
        assert!(ranked.table().unwrap().is_time_traveled());
        assert!(ranked.snapshot_id().is_none());
    }

    #[tokio::test]
    async fn vector_route_getters_share_query_limit_and_filter_updates() {
        let table = table();
        append(&table, 0).await;
        index(&table, "ivf-flat", "embedding").await;
        let routes = mixed_query(&table, 4).route_builders().await.unwrap();
        let route = routes[0].clone();
        let snapshot_id = route.table().travel_snapshot().unwrap().id();
        let mut vector = route.vector_search_builder().unwrap();
        vector.with_query_vector(vec![3.0, 0.0]).with_limit(2);
        assert_eq!(
            route.execute().await.unwrap().row_ids().unwrap().row_ids,
            vec![3, 2]
        );

        let predicates = PredicateBuilder::new(table.schema().fields());
        route.vector_search_builder().unwrap().with_filter(
            predicates
                .equal("label", Datum::String("keep".into()))
                .unwrap(),
        );
        assert_eq!(
            vector.execute().await.unwrap().row_ids().unwrap().row_ids,
            vec![2, 1]
        );
        let mut repeated = route.vector_search_builder().unwrap();
        repeated.with_query_vector(vec![0.0, 0.0]).with_limit(1);
        // Handles and cloned routes keep the shared query state and owned view.
        drop(routes);
        append(&table, 4).await;
        for result in [
            route.execute().await.unwrap(),
            vector.execute().await.unwrap(),
        ] {
            assert_eq!(result.row_ids().unwrap().row_ids, vec![1]);
            assert_eq!(result.snapshot_id(), Some(snapshot_id));
        }
    }

    #[tokio::test]
    async fn full_text_route_getters_share_query_limit_and_filter_updates() {
        let table = table();
        append(&table, 0).await;
        index(&table, "full-text", "text").await;
        let routes = mixed_query(&table, 4).route_builders().await.unwrap();
        let route = routes[1].clone();
        let snapshot_id = route.table().travel_snapshot().unwrap().id();
        let mut text = route.full_text_search_builder().unwrap();
        text.with_limit(1);
        assert_eq!(route.execute().await.unwrap().len(), 1);

        route
            .full_text_search_builder()
            .unwrap()
            .with_query("text", r#"{"match":{"query":"lake"}}"#);
        assert_eq!(
            text.execute().await.unwrap().row_ids().unwrap().row_ids,
            vec![2]
        );
        let predicates = PredicateBuilder::new(table.schema().fields());
        let mut repeated = route.full_text_search_builder().unwrap();
        repeated
            .with_query("text", r#"{"match":{"query":"paimon"}}"#)
            .with_filter(predicates.equal("id", Datum::Int(1)).unwrap());
        drop(routes);
        append(&table, 4).await;
        for result in [
            route.execute().await.unwrap(),
            text.execute().await.unwrap(),
        ] {
            assert_eq!(result.row_ids().unwrap().row_ids, vec![1]);
            assert_eq!(result.snapshot_id(), Some(snapshot_id));
        }
    }

    struct UpdatingExecutor {
        table: Table,
        update: std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>>,
    }

    #[async_trait::async_trait]
    impl crate::table::SearchTaskExecutor for UpdatingExecutor {
        async fn execute(
            &self,
            tasks: Vec<crate::table::SearchTask>,
        ) -> crate::Result<Vec<crate::table::SearchTaskResult>> {
            let update = self.update.lock().unwrap().take();
            if let Some(update) = update {
                update();
            }
            let (context, frames) = crate::table::SearchTask::serialize_batch(&tasks)?;
            let mut results = Vec::new();
            for frame in frames {
                results.push(crate::table::SearchTaskResult::deserialize(
                    &self.table.execute_search_task(&frame, &context).await?,
                )?);
            }
            Ok(results)
        }
    }

    #[tokio::test]
    async fn active_route_execution_keeps_its_configuration_while_getters_are_mutated() {
        fn require_send<T: Send>(value: T) -> T {
            value
        }

        let table = table();
        append(&table, 0).await;
        index(&table, "ivf-flat", "embedding").await;
        index(&table, "full-text", "text").await;
        let routes = mixed_query(&table, 4).route_builders().await.unwrap();
        let predicates = PredicateBuilder::new(table.schema().fields());
        for route in routes {
            let (update, expected, later): (Box<dyn FnOnce() + Send>, _, _) =
                match route.route().kind() {
                    HybridSearchRouteKind::Vector => {
                        let mut vector = route.vector_search_builder().unwrap();
                        let filter = predicates.equal("id", Datum::Int(3)).unwrap();
                        (
                            Box::new(move || {
                                vector
                                    .with_query_vector(vec![3.0, 0.0])
                                    .with_limit(1)
                                    .with_filter(filter);
                            }),
                            vec![0, 1, 2, 3],
                            vec![3],
                        )
                    }
                    HybridSearchRouteKind::FullText => {
                        let mut text = route.full_text_search_builder().unwrap();
                        let filter = predicates.equal("id", Datum::Int(2)).unwrap();
                        (
                            Box::new(move || {
                                text.with_query("text", r#"{"match":{"query":"lake"}}"#)
                                    .with_limit(1)
                                    .with_filter(filter);
                            }),
                            vec![0, 1, 2],
                            vec![2],
                        )
                    }
                };
            let executor = UpdatingExecutor {
                table: table.clone(),
                update: std::sync::Mutex::new(Some(update)),
            };
            // Mutating through a worker callback must succeed while execution is
            // awaiting I/O. The in-flight request keeps its original settings.
            let result = require_send(route.execute_internal(Some(&executor)))
                .await
                .unwrap();
            let mut ids = result.into_row_ids().unwrap().row_ids;
            ids.sort_unstable();
            assert_eq!(ids, expected);
            assert_eq!(
                route.execute().await.unwrap().row_ids().unwrap().row_ids,
                later
            );
        }
    }

    #[tokio::test]
    async fn incomplete_queries_can_be_finished_on_captured_route_builders() {
        let table = table();
        append(&table, 0).await;
        index(&table, "ivf-flat", "embedding").await;
        index(&table, "full-text", "text").await;
        let snapshot = table.resolve_read_snapshot().await.unwrap().unwrap();
        let mut builder = table.new_hybrid_search_builder();
        builder
            .add_vector_route("embedding", vec![], 4, 1.0, HashMap::new())
            .unwrap()
            .add_full_text_route("text", "", 4, 1.0, HashMap::new())
            .unwrap()
            .with_limit(16);
        let routes = builder.route_builders().await.unwrap();
        assert!(routes[0].route().vector_value().unwrap().is_empty());
        assert_eq!(routes[1].route().full_text_query(), Some(""));
        // Preparing routes does not execute their incomplete queries.
        assert!(builder.execute().await.is_err());
        let mut empty_text = table.new_hybrid_search_builder();
        empty_text
            .add_full_text_route("text", "", 4, 1.0, HashMap::new())
            .unwrap()
            .with_limit(4);
        assert!(empty_text.execute().await.is_err());

        append(&table, 4).await;
        let mut vector = routes[0].vector_search_builder().unwrap();
        vector.with_query_vector(vec![0.0, 0.0]);
        let mut text = routes[1].full_text_search_builder().unwrap();
        text.with_query("text", r#"{"match":{"query":"paimon"}}"#);
        let results = [
            builder
                .to_route_result(&routes[0], routes[0].execute().await.unwrap())
                .unwrap(),
            builder
                .to_route_result(&routes[1], routes[1].execute().await.unwrap())
                .unwrap(),
        ];
        let actual = builder.rank(&results).unwrap();
        let expected = mixed_query(&table, 4)
            .with_snapshot(Some(&snapshot))
            .execute()
            .await
            .unwrap();
        assert_eq!(actual.snapshot_id(), Some(snapshot.id()));
        assert_eq!(
            actual.row_ids().unwrap().row_ids,
            expected.row_ids().unwrap().row_ids
        );
        assert_eq!(
            actual.row_ids().unwrap().scores,
            expected.row_ids().unwrap().scores
        );
    }

    #[tokio::test]
    async fn rank_drops_read_context_for_different_snapshots_or_external_hits() {
        let table = table();
        append(&table, 0).await;
        let first = table.resolve_read_snapshot().await.unwrap().unwrap();
        let captured = table.copy_with_pinned_snapshot(Some(&first));
        let left = HybridSearchWeightedResult::new(
            GlobalIndexResult::from_global_search(
                Arc::new(captured.clone()),
                ScoredRowIds::new(vec![1], vec![0.9]),
            ),
            2.0,
        )
        .unwrap();
        let sibling = HybridSearchWeightedResult::new(
            GlobalIndexResult::from_global_search(
                Arc::new(captured),
                ScoredRowIds::new(vec![2], vec![0.8]),
            ),
            1.0,
        )
        .unwrap();
        let compatible = HybridSearchRanker::Mrr
            .rank(&[left.clone(), sibling], 2)
            .unwrap();
        assert_eq!(compatible.snapshot_id(), Some(first.id()));
        assert!(compatible.table().is_some());

        append(&table, 4).await;
        let later = table.resolve_read_snapshot().await.unwrap().unwrap();
        let right = HybridSearchWeightedResult::new(
            GlobalIndexResult::from_global_search(
                Arc::new(table.copy_with_pinned_snapshot(Some(&later))),
                ScoredRowIds::new(vec![2], vec![0.8]),
            ),
            1.0,
        )
        .unwrap();
        for right in [right, scored(&[2], &[0.8], 1.0)] {
            let ranked = HybridSearchRanker::Mrr
                .rank(&[left.clone(), right], 2)
                .unwrap();
            assert_eq!(ranked.row_ids().unwrap().row_ids, vec![1, 2]);
            assert_eq!(ranked.row_ids().unwrap().scores, vec![2.0, 1.0]);
            assert!(ranked.table().is_none());
            assert!(ranked.snapshot_id().is_none());
            assert!(ranked.new_read_builder().read().await.is_err());
        }
    }

    #[derive(Debug)]
    struct PartitionScopedFileIO {
        delegate: crate::io::FileIO,
        denied_partition: &'static str,
    }

    #[async_trait::async_trait]
    impl crate::io::FileIOProvider for PartitionScopedFileIO {
        async fn create(&self, path: &str) -> crate::Result<(opendal::Operator, String)> {
            if path.contains(self.denied_partition) {
                return Err(crate::Error::IoUnsupported {
                    message: "partition is outside this FileIO credential scope".into(),
                });
            }
            self.delegate.create_static(path)
        }
    }

    #[tokio::test]
    async fn rank_drops_read_context_across_partition_scoped_file_io() {
        let table = table();
        append(&table, 0).await;
        append(&table, 4).await;
        let snapshot = table.resolve_read_snapshot().await.unwrap().unwrap();
        let predicates = PredicateBuilder::new(table.schema().fields());
        let mut results = Vec::new();
        for (partition, denied_partition) in [(0, "/pt=1/"), (1, "/pt=0/")] {
            // Both handles read exactly the same immutable dataset. Only their
            // credentials differ: each can read one partition's data files.
            let file_io = table
                .file_io()
                .clone()
                .with_provider(Arc::new(PartitionScopedFileIO {
                    delegate: table.file_io().clone(),
                    denied_partition,
                }));
            let scoped_table = Table::new(
                file_io,
                table.identifier().clone(),
                table.location().to_string(),
                table.schema().clone(),
                None,
            )
            .copy_with_pinned_snapshot(Some(&snapshot));
            let result = scoped_table
                .new_vector_search_builder()
                .with_vector_column("embedding")
                .with_query_vector(vec![0.0, 0.0])
                .with_limit(4)
                .with_partition_filter(predicates.equal("pt", Datum::Int(partition)).unwrap())
                .unwrap()
                .execute()
                .await
                .unwrap();
            assert_eq!(result.len(), 4);
            // Each route can materialize its own hits using its own credentials.
            let batches = result
                .new_read_builder()
                .with_projection(&["id"])
                .read()
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
            let weighted = HybridSearchWeightedResult::new(result, 1.0).unwrap();
            let same_scope = HybridSearchRanker::Mrr
                .rank(&[weighted.clone(), weighted.clone()], 4)
                .unwrap();
            assert!(same_scope.table().is_some());
            assert_eq!(same_scope.snapshot_id(), Some(snapshot.id()));
            results.push(weighted);
        }

        let ranked = HybridSearchRanker::Mrr.rank(&results, 8).unwrap();
        let mut row_ids = ranked.row_ids().unwrap().row_ids.clone();
        row_ids.sort_unstable();
        assert_eq!(row_ids, (0..8).collect::<Vec<_>>());
        assert!(ranked.table().is_none());
        assert!(ranked.snapshot_id().is_none());
        match ranked.new_read_builder().read().await {
            Err(crate::Error::Unsupported { message }) => {
                assert!(message
                    .contains("Direct result reads require a result returned by a table search"));
            }
            Err(error) => panic!("expected missing read context, got: {error}"),
            Ok(_) => panic!("mixed credential scopes must not retain one route's read context"),
        }
    }

    #[tokio::test]
    async fn route_preparation_validates_before_resolving_snapshots() {
        let table = table();
        let guarded = table.copy_with_options(HashMap::from([(
            "query-auth.enabled".into(),
            "true".into(),
        )]));
        let error = mixed_query(&guarded, 4).route_builders().await.unwrap_err();
        assert!(format!("{error:?}").contains("query-auth.enabled"));
        assert!(table
            .new_hybrid_search_builder()
            .route_builders()
            .await
            .is_err());
        let mut missing = table.new_hybrid_search_builder();
        missing
            .add_vector_route("missing", vec![0.0], 4, 1.0, HashMap::new())
            .unwrap()
            .with_limit(4);
        assert!(missing.route_builders().await.is_err());
        let mut zero = mixed_query(&table, 4);
        zero.with_limit(0);
        assert!(zero.route_builders().await.is_err());
    }
}
