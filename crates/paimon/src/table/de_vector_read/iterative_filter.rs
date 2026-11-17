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

use crate::vector_search::{ScoredRowIds, VectorSearch};
use roaring::RoaringTreemap;
use std::collections::HashMap;
use std::future::Future;
use tokio_util::sync::CancellationToken;

const ENABLED: &str = "vector-index.iterative-filter.enabled";
const MAX_CANDIDATES_FACTOR: &str = "vector-index.iterative-filter.max-candidates-factor";

pub(super) struct Options {
    pub(super) enabled: bool,
    pub(super) max_candidates_factor: usize,
}

impl Options {
    pub(super) fn parse(options: &HashMap<String, String>) -> crate::Result<Self> {
        let enabled = match options.get(ENABLED) {
            None => true,
            Some(value) if value.eq_ignore_ascii_case("true") => true,
            Some(value) if value.eq_ignore_ascii_case("false") => false,
            Some(value) => {
                return Err(crate::Error::ConfigInvalid {
                    message: format!("{ENABLED} must be true or false, got {value}"),
                })
            }
        };
        let max_candidates_factor = match options.get(MAX_CANDIDATES_FACTOR) {
            None => 100,
            Some(value) => value
                .parse::<usize>()
                .ok()
                .filter(|n| *n > 0 && *n <= i32::MAX as usize)
                .ok_or_else(|| crate::Error::ConfigInvalid {
                    message: format!(
                        "{MAX_CANDIDATES_FACTOR} must be between 1 and {}, got {value}",
                        i32::MAX
                    ),
                })?,
        };
        Ok(Self {
            enabled,
            max_candidates_factor,
        })
    }
}

pub(super) struct Result {
    pub(super) rows: Vec<ScoredRowIds>,
    pub(super) fallback: Vec<(usize, VectorSearch)>,
    pub(super) rounds: usize,
    pub(super) checked: u64,
}

pub(super) fn ensure_active(cancellation: &CancellationToken) -> crate::Result<()> {
    if cancellation.is_cancelled() {
        Err(crate::table::search_task::invalid(
            "Iterative vector search was cancelled",
        ))
    } else {
        Ok(())
    }
}

/// Started native jobs outlive their waiter. Cancel the interruptible scalar
/// I/O and prevent further native rounds when the caller drops this future.
pub(super) async fn execute_cancellable<T, G, F>(
    panic_context: &'static str,
    guard: G,
    task: F,
) -> crate::Result<T>
where
    T: Send + 'static,
    G: Send + 'static,
    F: FnOnce(CancellationToken) -> crate::Result<T> + Send + 'static,
{
    let cancellation = CancellationToken::new();
    let _cancel_on_drop = cancellation.clone().drop_guard();
    crate::vindex::executor::execute_global_index_with_guard(panic_context, guard, move || {
        task(cancellation)
    })
    .await
}

pub(super) async fn read_or_cancel<T>(
    cancellation: &CancellationToken,
    read: impl Future<Output = crate::Result<T>>,
) -> crate::Result<T> {
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => {
            Err(crate::table::search_task::invalid("Iterative vector search was cancelled"))
        }
        result = read => result,
    }
}

/// IDs are local to one vector shard. Only new candidate IDs are sent to the
/// scalar reader, shared across the batch; completed queries leave the loop.
/// Reuse the caller's open index reader across wider searches. The current
/// backend API does not expose a cursor across these calls, so ANN traversal
/// may repeat, even though scalar reads and index metadata loading do not.
pub(super) fn search(
    queries: Vec<VectorSearch>,
    total_vectors: usize,
    top_ks: &[usize],
    max_candidates_factor: usize,
    mut retrieve: impl FnMut(&[VectorSearch]) -> crate::Result<Vec<ScoredRowIds>>,
    mut check: impl FnMut(&RoaringTreemap) -> crate::Result<RoaringTreemap>,
) -> crate::Result<Result> {
    if top_ks.len() != queries.len() || top_ks.contains(&0) {
        return Err(crate::table::search_task::invalid(
            "Iterative vector filtering requires the original Top-K of each query",
        ));
    }
    let mut rows = vec![ScoredRowIds::empty(); queries.len()];
    let targets: Vec<_> = queries.iter().map(|query| query.limit).collect();
    let budgets: Vec<_> = queries
        .iter()
        .zip(top_ks)
        // Multiply the original result limit, not the reranking/deletion
        // allowance or batch size. Saturate before applying backend limits.
        .map(|(q, &k)| {
            k.saturating_mul(max_candidates_factor)
                .max(q.limit)
                .min(total_vectors)
                .min(i32::MAX as usize)
        })
        .collect();
    let mut limits: Vec<_> = queries
        .iter()
        .zip(&budgets)
        .map(|(q, budget)| q.limit.saturating_mul(2).max(32).min(*budget))
        .collect();
    let (mut active, mut active_queries): (Vec<_>, Vec<_>) = queries
        .into_iter()
        .enumerate()
        .filter(|(_, q)| q.limit != 0 && total_vectors != 0)
        .unzip();
    for (&i, query) in active.iter().zip(&mut active_queries) {
        query.limit = limits[i];
    }
    let mut checked = RoaringTreemap::new();
    let mut matched = RoaringTreemap::new();
    let mut fallback = Vec::new();
    let mut rounds = 0;
    while !active.is_empty() {
        rounds += 1;
        let candidates = retrieve(&active_queries)?;
        if candidates.len() != active.len() {
            return Err(crate::table::search_task::invalid(
                "Iterative vector search returned wrong query count",
            ));
        }
        let mut pending = RoaringTreemap::new();
        for candidate in &candidates {
            pending.extend(candidate.row_ids.iter().copied());
        }
        pending -= &checked;
        if !pending.is_empty() {
            // A scalar reader must not expand the requested candidate scope.
            matched |= check(&pending)? & &pending;
            checked |= &pending;
        }
        let mut next = Vec::new();
        let mut next_queries = Vec::new();
        for ((i, mut query), candidate) in active.into_iter().zip(active_queries).zip(candidates) {
            let count = candidate.len();
            let (ids, scores) = candidate
                .row_ids
                .into_iter()
                .zip(candidate.scores)
                .filter(|(id, _)| matched.contains(*id))
                .unzip();
            rows[i] = rows[i]
                .or(&ScoredRowIds::new(ids, scores))
                .top_k(targets[i]);
            if rows[i].len() >= targets[i] {
                continue;
            }
            let corpus = query
                .effective_include_row_ids()
                .map_or(total_vectors, |ids| (ids.len() as usize).min(total_vectors));
            if count >= corpus {
                // All eligible indexed vectors were checked, including the
                // legitimate case where fewer than K live rows match.
                continue;
            }
            if limits[i] >= budgets[i] {
                query.limit = targets[i];
                fallback.push((i, query));
            } else {
                limits[i] = limits[i].saturating_mul(2).min(budgets[i]);
                query.limit = limits[i];
                next.push(i);
                next_queries.push(query);
            }
        }
        active = next;
        active_queries = next_queries;
    }
    Ok(Result {
        rows,
        fallback,
        rounds,
        checked: checked.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancelling_waiter_stops_scalar_io_and_releases_worker_permit() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        use std::time::Duration;
        use tokio::sync::{oneshot, Semaphore};

        struct ReadDrop(Option<oneshot::Sender<()>>);
        impl Drop for ReadDrop {
            fn drop(&mut self) {
                let _ = self.0.take().unwrap().send(());
            }
        }

        let permits = Arc::new(Semaphore::new(1));
        let permit = permits.clone().acquire_owned().await.unwrap();
        let runtime = tokio::runtime::Handle::current();
        let (started, read_started) = oneshot::channel();
        let (dropped, read_dropped) = oneshot::channel();
        let rounds = Arc::new(AtomicUsize::new(0));
        let worker_rounds = rounds.clone();
        let waiter = tokio::spawn(execute_cancellable(
            "cancellation regression",
            permit,
            move |token| {
                let mut signals = Some((started, dropped));
                search(
                    vec![VectorSearch::new(vec![0.0], 2, "v".into()).unwrap()],
                    128,
                    &[2],
                    100,
                    |_| {
                        worker_rounds.fetch_add(1, Ordering::SeqCst);
                        Ok(vec![ScoredRowIds::new(vec![0], vec![0.0])])
                    },
                    |_| {
                        let (started, dropped) = signals.take().unwrap();
                        runtime.block_on(read_or_cancel(&token, async move {
                            let _drop = ReadDrop(Some(dropped));
                            let _ = started.send(());
                            std::future::pending::<crate::Result<RoaringTreemap>>().await
                        }))
                    },
                )
            },
        ));
        tokio::time::timeout(Duration::from_secs(10), read_started)
            .await
            .unwrap()
            .unwrap();
        waiter.abort();
        assert!(matches!(waiter.await, Err(error) if error.is_cancelled()));
        tokio::time::timeout(Duration::from_secs(10), read_dropped)
            .await
            .unwrap()
            .unwrap();
        let _released = tokio::time::timeout(Duration::from_secs(10), permits.acquire())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rounds.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn refills_each_query_and_reads_only_new_candidates() {
        let queries: Vec<_> = [0.0, 1.0]
            .into_iter()
            .map(|q| VectorSearch::new(vec![q], 2, "v".into()).unwrap())
            .collect();
        let mut batches = Vec::new();
        let allocations: Vec<_> = queries.iter().map(|q| q.vector.as_ptr()).collect();
        let mut checked = RoaringTreemap::new();
        let result = search(
            queries,
            128,
            &[2, 2],
            64,
            |queries| {
                for query in queries {
                    assert_eq!(
                        query.vector.as_ptr(),
                        allocations[query.vector[0] as usize],
                        "candidate rounds must retain the original vector allocation"
                    );
                }
                batches.push(queries.len());
                Ok(queries
                    .iter()
                    .map(|q| {
                        let ids: Vec<_> = if q.vector[0] == 0.0 {
                            (0..q.limit as u64).collect()
                        } else {
                            (128 - q.limit as u64..128).rev().collect()
                        };
                        let scores = ids
                            .iter()
                            .map(|&id| -(id as f32 - q.vector[0] * 128.0).abs())
                            .collect();
                        ScoredRowIds::new(ids, scores)
                    })
                    .collect())
            },
            |ids| {
                assert!((ids & &checked).is_empty(), "scalar reads must not repeat");
                checked |= ids;
                Ok(ids.iter().filter(|id| (48..=120).contains(id)).collect())
            },
        )
        .unwrap();
        assert_eq!(result.rows[0].row_ids, vec![48, 49]);
        assert_eq!(result.rows[1].row_ids, vec![120, 119]);
        assert_eq!(batches, vec![2, 1]);
        assert_eq!(result.rounds, 2);
        assert_eq!(result.checked, 96);
        assert!(result.fallback.is_empty());
    }

    #[test]
    fn wider_ann_results_keep_matches_from_earlier_rounds() {
        let queries = [VectorSearch::new(vec![0.0], 2, "v".into()).unwrap()];
        let mut round = 0;
        let result = search(
            queries.to_vec(),
            128,
            &[2],
            64,
            |_| {
                round += 1;
                Ok(vec![if round == 1 {
                    ScoredRowIds::new(vec![0], vec![1.0])
                } else {
                    // Wider ANN searches need not return a superset of the old hits.
                    ScoredRowIds::new(vec![100], vec![2.0])
                }])
            },
            |ids| Ok(ids.clone()),
        )
        .unwrap();
        assert_eq!(result.rows[0].row_ids, vec![100, 0]);
        assert_eq!(result.rounds, 2);
        assert!(result.fallback.is_empty());
    }

    #[test]
    fn exhausted_matches_return_short_but_unexplored_candidates_fall_back() {
        let queries = [VectorSearch::new(vec![0.0], 2, "v".into()).unwrap()];
        let result = search(
            queries.to_vec(),
            128,
            &[2],
            16,
            |_| Ok(vec![ScoredRowIds::new(vec![0], vec![1.0])]),
            |_| Ok(RoaringTreemap::new()),
        )
        .unwrap();
        assert_eq!(
            result.fallback.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
            vec![0]
        );
        assert_eq!(result.fallback[0].1.limit, 2);
        assert_eq!(result.rounds, 1);

        let result = search(
            queries.to_vec(),
            2,
            &[2],
            16,
            |_| Ok(vec![ScoredRowIds::new(vec![0, 1], vec![1.0, 0.0])]),
            |_| Ok(RoaringTreemap::from_iter([1])),
        )
        .unwrap();
        assert_eq!(result.rows[0].row_ids, vec![1]);
        assert!(result.fallback.is_empty());
    }

    #[test]
    fn candidate_budget_scales_each_original_top_k_and_preserves_required_slack() {
        // The index limits include reranking/deletion slack. The caller's
        // original Top-K values are 2 and 10, independent of this batch size.
        let queries = [
            VectorSearch::new(vec![0.0], 6, "v".into()).unwrap(),
            VectorSearch::new(vec![1.0], 20, "v".into()).unwrap(),
        ];
        for (factor, expected) in [
            (10, vec![vec![20, 40], vec![80], vec![100]]),
            (1, vec![vec![6, 20]]),
        ] {
            let mut limits = Vec::new();
            let result = search(
                queries.to_vec(),
                1000,
                &[2, 10],
                factor,
                |queries| {
                    limits.push(queries.iter().map(|q| q.limit).collect::<Vec<_>>());
                    Ok(vec![ScoredRowIds::empty(); queries.len()])
                },
                |_| panic!("empty candidate batches must not read scalar data"),
            )
            .unwrap();
            assert_eq!(limits, expected, "factor={factor}");
            assert_eq!(
                result.fallback.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
                vec![0, 1]
            );
        }
    }

    #[test]
    fn candidate_budget_clamps_default_factor_and_overflow_to_shard_size() {
        let options = Options::parse(&HashMap::new()).unwrap();
        let queries = [VectorSearch::new(vec![0.0], 2, "v".into()).unwrap()];
        for (top_k, total, expected) in [
            (2, 1000, vec![32, 64, 128, 200]),
            (usize::MAX, 64, vec![32, 64]),
        ] {
            let mut limits = Vec::new();
            let result = search(
                queries.to_vec(),
                total,
                &[top_k],
                options.max_candidates_factor,
                |queries| {
                    limits.push(queries[0].limit);
                    Ok(vec![ScoredRowIds::empty()])
                },
                |_| unreachable!(),
            )
            .unwrap();
            assert_eq!(limits, expected);
            assert_eq!(
                result.fallback.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
                vec![0]
            );
        }
        for original_limits in [vec![], vec![0]] {
            assert!(search(
                queries.to_vec(),
                64,
                &original_limits,
                100,
                |_| unreachable!(),
                |_| unreachable!()
            )
            .is_err());
        }
    }

    #[test]
    fn rejects_invalid_enabled_values_and_unbounded_budgets() {
        for (key, value) in [
            (ENABLED, "iterative"),
            (ENABLED, "raw"),
            (MAX_CANDIDATES_FACTOR, "0"),
            (MAX_CANDIDATES_FACTOR, "-1"),
            (MAX_CANDIDATES_FACTOR, "2147483648"),
        ] {
            assert!(Options::parse(&HashMap::from([(key.into(), value.into())])).is_err());
        }
    }

    #[test]
    fn task_literal_transport_preserves_nan_signed_zero_and_decimal() {
        use crate::spec::Datum;
        for bits in [f32::NAN.to_bits(), (-0.0f32).to_bits()] {
            let json = serde_json::to_value(Datum::Float(f32::from_bits(bits))).unwrap();
            let Datum::Float(value) = serde_json::from_value(json).unwrap() else {
                panic!()
            };
            assert_eq!(value.to_bits(), bits);
        }
        for bits in [f64::NAN.to_bits(), (-0.0f64).to_bits()] {
            let json = serde_json::to_value(Datum::Double(f64::from_bits(bits))).unwrap();
            let Datum::Double(value) = serde_json::from_value(json).unwrap() else {
                panic!()
            };
            assert_eq!(value.to_bits(), bits);
        }
        let decimal = Datum::Decimal {
            unscaled: i128::MAX,
            precision: 38,
            scale: 2,
        };
        let encoded = serde_json::to_value(&decimal).unwrap();
        assert_eq!(serde_json::from_value::<Datum>(encoded).unwrap(), decimal);
    }
}
