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

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use super::data_invalid;
use super::metric::java_float_compare;
#[cfg(test)]
use super::metric::VectorSearchMetric;
#[cfg(test)]
use super::reader::PkVectorReader;
use super::result::PkVectorSearchResult;

/// A candidate wrapped so a max-heap keeps the WORST candidate on top:
/// worst = largest distance, ties broken by largest row_position. Popping the
/// top therefore evicts the least-wanted candidate. Uses `java_float_compare`
/// for a deterministic total order over f32 that ranks NaN distances as worst
/// (largest), so a NaN is evicted before any finite candidate (no panic).
///
/// Ordered by distance then row position only (a single data file, so the file
/// name is constant across all its candidates).
pub(crate) struct WorstFirst(PkVectorSearchResult);

impl PartialEq for WorstFirst {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for WorstFirst {}
impl PartialOrd for WorstFirst {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for WorstFirst {
    fn cmp(&self, other: &Self) -> Ordering {
        java_float_compare(self.0.distance, other.0.distance)
            .then_with(|| self.0.row_position.cmp(&other.0.row_position))
    }
}

/// True if `candidate` ranks strictly better (BEST_FIRST) than the current
/// worst-on-heap `weakest`: smaller distance, ties broken by smaller position.
pub(crate) fn is_better_than(
    candidate: &PkVectorSearchResult,
    weakest: &PkVectorSearchResult,
) -> bool {
    java_float_compare(candidate.distance, weakest.distance)
        .then_with(|| candidate.row_position.cmp(&weakest.row_position))
        == Ordering::Less
}

/// Add `candidate` to a bounded (size `limit`) BEST_FIRST Top-K max-heap over one
/// file's candidates: push if under capacity, else replace the current worst iff
/// the candidate beats it. `O(log limit)` per call. The single shared push step
/// so the whole-file `exact_search` and the streaming per-file search cannot
/// drift in how they bound their heaps.
pub(crate) fn push_bounded(
    heap: &mut BinaryHeap<WorstFirst>,
    candidate: PkVectorSearchResult,
    limit: usize,
) {
    if heap.len() < limit {
        heap.push(WorstFirst(candidate));
    } else if heap
        .peek()
        .is_some_and(|worst| is_better_than(&candidate, &worst.0))
    {
        heap.pop();
        heap.push(WorstFirst(candidate));
    }
}

/// Drain a bounded Top-K heap into a BEST_FIRST-sorted result list: distance
/// ASC, then row_position ASC (single file, so data_file_name is constant).
pub(crate) fn drain_best_first(heap: BinaryHeap<WorstFirst>) -> Vec<PkVectorSearchResult> {
    let mut results: Vec<PkVectorSearchResult> = heap.into_iter().map(|w| w.0).collect();
    results.sort_by(|a, b| {
        java_float_compare(a.distance, b.distance).then_with(|| a.row_position.cmp(&b.row_position))
    });
    results
}

/// Validate one query vector against the index dimension: length must match and
/// every element must be finite. Shared by the whole-file `exact_search` and the
/// table-layer streaming per-file search so both reject the same malformed
/// queries with the same messages (before any read is performed).
pub(crate) fn validate_query(query: &[f32], dimension: usize) -> crate::Result<()> {
    if query.len() != dimension {
        return Err(data_invalid(format!(
            "query vector dimension does not match: index expects {}, got {}",
            dimension,
            query.len()
        )));
    }
    if let Some(i) = query.iter().position(|v| !v.is_finite()) {
        return Err(data_invalid(format!(
            "query vector element at position {i} must be finite"
        )));
    }
    Ok(())
}

/// Exact Top-K over one sequential physical-row vector source. Mirrors Java
/// `PkVectorExactSearcher.search`. Results are sorted BEST_FIRST: distance ASC,
/// then row_position ASC (single file, so data_file_name is constant).
///
/// No longer the production read path (the table layer streams the vector column
/// one Arrow batch at a time via the per-file search closure); kept as a tested
/// reference the streaming search must stay byte-identical to.
#[cfg(test)]
pub(crate) fn exact_search(
    data_file_name: &str,
    reader: &mut dyn PkVectorReader,
    query: &[f32],
    metric: VectorSearchMetric,
    limit: usize,
    is_excluded: &dyn Fn(i64) -> bool,
) -> crate::Result<Vec<PkVectorSearchResult>> {
    validate_query(query, reader.dimension())?;
    if limit == 0 {
        return Err(data_invalid("vector search limit must be positive"));
    }
    let row_count = reader.row_count();
    if row_count < 0 {
        return Err(data_invalid(format!(
            "vector reader row count must not be negative: {row_count}"
        )));
    }

    let query = metric.prepare_query(query);
    let mut reuse = vec![0.0f32; reader.dimension()];
    let mut heap: BinaryHeap<WorstFirst> = BinaryHeap::with_capacity(limit + 1);
    for position in 0..row_count {
        let has_vector = reader.read_next_vector(&mut reuse)?;
        if !has_vector || is_excluded(position) {
            continue;
        }
        let candidate = PkVectorSearchResult {
            data_file_name: data_file_name.to_string(),
            row_position: position,
            distance: query.compute_distance(&reuse),
        };
        push_bounded(&mut heap, candidate, limit);
    }

    Ok(drain_best_first(heap))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vindex::pkvector::metric::VectorSearchMetric;
    use crate::vindex::pkvector::reader::test_support::ArrayReader;

    fn no_exclusion() -> impl Fn(i64) -> bool {
        |_| false
    }

    #[test]
    fn test_distances_for_supported_metrics() {
        // Java testDistancesForSupportedMetrics: q=[2,0] over stored [1,0].
        for (metric, expected) in [
            (VectorSearchMetric::L2, 1.0f32),
            (VectorSearchMetric::Cosine, 0.0),
            (VectorSearchMetric::InnerProduct, -2.0),
        ] {
            let mut reader = ArrayReader::new(2, vec![Some(vec![1.0, 0.0])]);
            let results = exact_search(
                "data-file",
                &mut reader,
                &[2.0, 0.0],
                metric,
                1,
                &no_exclusion(),
            )
            .unwrap();
            assert_eq!(results[0].distance, expected);
        }
    }

    #[test]
    fn cosine_top_one_preserves_a_distance_below_the_similarity_precision() {
        let mut reader = ArrayReader::new(2, vec![Some(vec![3.0015, 4.0]), Some(vec![3.0, 4.0])]);
        let results = exact_search(
            "data-file",
            &mut reader,
            &[3.0, 4.0],
            VectorSearchMetric::Cosine,
            1,
            &no_exclusion(),
        )
        .unwrap();
        assert_eq!(results[0].row_position, 1);
        assert_eq!(results[0].distance, 0.0);
    }

    #[test]
    fn nan_distance_candidate_never_beats_finite_top1() {
        // Row 0's stored vector contains NaN → its inner-product distance is a
        // negative NaN (sign flipped by the metric's negation), which
        // `f32::total_cmp` would rank as best (smallest) and select as Top-1.
        // `java_float_compare` ranks NaN worst, so the finite row 1 wins the
        // single Top-1 slot instead.
        let mut reader = ArrayReader::new(2, vec![Some(vec![f32::NAN, 0.0]), Some(vec![1.0, 0.0])]);
        let results = exact_search(
            "data-file",
            &mut reader,
            &[1.0, 0.0],
            VectorSearchMetric::InnerProduct,
            1,
            &no_exclusion(),
        )
        .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].row_position, 1);
        assert_eq!(results[0].distance, -1.0);
    }

    #[test]
    fn test_rejects_dimension_mismatch() {
        let mut reader = ArrayReader::new(2, vec![Some(vec![1.0, 0.0])]);
        let err = exact_search(
            "data-file",
            &mut reader,
            &[1.0],
            VectorSearchMetric::L2,
            1,
            &no_exclusion(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("dimension"));
    }

    #[test]
    fn test_rejects_non_positive_limit() {
        let mut reader = ArrayReader::new(2, vec![Some(vec![1.0, 0.0])]);
        let err = exact_search(
            "data-file",
            &mut reader,
            &[1.0, 0.0],
            VectorSearchMetric::L2,
            0,
            &no_exclusion(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("positive"));
    }

    #[test]
    fn test_rejects_non_finite_query() {
        let mut reader = ArrayReader::new(2, vec![Some(vec![1.0, 0.0])]);
        let err = exact_search(
            "data-file",
            &mut reader,
            &[f32::NAN, 0.0],
            VectorSearchMetric::L2,
            1,
            &no_exclusion(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("finite"));
    }

    #[test]
    fn test_rejects_negative_row_count() {
        struct NegativeReader;
        impl PkVectorReader for NegativeReader {
            fn dimension(&self) -> usize {
                2
            }
            fn row_count(&self) -> i64 {
                -1
            }
            fn read_next_vector(&mut self, _reuse: &mut [f32]) -> crate::Result<bool> {
                unreachable!()
            }
        }
        let mut reader = NegativeReader;
        let err = exact_search(
            "data-file",
            &mut reader,
            &[1.0, 0.0],
            VectorSearchMetric::L2,
            1,
            &no_exclusion(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("row count") || err.to_string().contains("-1"));
    }

    #[test]
    fn test_preserves_null_and_excluded_physical_positions() {
        // Java testPreservesNullAndDeletedPhysicalPositions:
        // vectors [{3,0}, null, {1,0}, {2,0}], q=[0,0], l2, limit 2, exclude pos==2.
        let mut reader = ArrayReader::new(
            2,
            vec![
                Some(vec![3.0, 0.0]),
                None,
                Some(vec![1.0, 0.0]),
                Some(vec![2.0, 0.0]),
            ],
        );
        let results = exact_search(
            "data-file",
            &mut reader,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            2,
            &|pos| pos == 2,
        )
        .unwrap();
        assert_eq!(
            results,
            vec![
                PkVectorSearchResult {
                    data_file_name: "data-file".into(),
                    row_position: 3,
                    distance: 4.0
                },
                PkVectorSearchResult {
                    data_file_name: "data-file".into(),
                    row_position: 0,
                    distance: 9.0
                },
            ]
        );
    }

    #[test]
    fn test_tie_break_prefers_smaller_row_position() {
        // Equal distances -> smaller row_position ranks first.
        let mut reader =
            ArrayReader::new(1, vec![Some(vec![1.0]), Some(vec![1.0]), Some(vec![1.0])]);
        let results = exact_search(
            "data-file",
            &mut reader,
            &[0.0],
            VectorSearchMetric::L2,
            2,
            &no_exclusion(),
        )
        .unwrap();
        assert_eq!(
            results.iter().map(|r| r.row_position).collect::<Vec<_>>(),
            vec![0, 1]
        );
    }
}
