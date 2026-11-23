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

//! Deterministic reciprocal-rank fusion for physical primary-key search positions.
//!
//! Rust mirror of Java
//! `org.apache.paimon.table.source.PrimaryKeySearchRanker`. It fuses the
//! per-route rankings produced by the vector and full-text search routes (each a
//! list of [`PrimaryKeySearchPosition`]) into a single globally ranked list.
//!
//! Positions are combined ACROSS routes by their physical identity: the
//! `PrimaryKeySearchPosition` [`Eq`]/[`Hash`] deliberately exclude the score, so
//! the same physical row hit by two routes collapses to one fused entry. A
//! duplicate physical position WITHIN a single ranking is a caller error and the
//! weighted rankers fail loud, exactly like the Java `checkArgument`s.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use num_rational::BigRational;
use num_traits::ToPrimitive;

use crate::table::hybrid_search_builder::normalize_route_score;
use crate::table::pk_search_position::PrimaryKeySearchPosition;

/// Reciprocal-rank-fusion smoothing constant. Matches Java's
/// `PrimaryKeySearchRanker.DEFAULT_RRF_K` (an `int` with value `60`).
pub(crate) const DEFAULT_RRF_K: i32 = 60;

fn data_invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

/// One locally scored ranking and its route weight. Mirrors Java
/// `PrimaryKeySearchRanker.Ranking`.
#[derive(Clone, Debug)]
pub(crate) struct Ranking {
    positions: Vec<PrimaryKeySearchPosition>,
    weight: f64,
    cosine_distance_scores: bool,
}

impl Ranking {
    /// Build a ranking, rejecting a non-finite or non-positive `weight`.
    /// Mirrors the `checkArgument` in the Java `Ranking` constructor.
    pub(crate) fn new(
        positions: Vec<PrimaryKeySearchPosition>,
        weight: f64,
    ) -> crate::Result<Self> {
        if !weight.is_finite() || weight <= 0.0 {
            return Err(data_invalid(format!(
                "Search route weight must be finite and positive: {weight}."
            )));
        }
        Ok(Self {
            positions,
            weight,
            cosine_distance_scores: false,
        })
    }

    pub(crate) fn with_cosine_distance_scores(mut self, cosine_distance_scores: bool) -> Self {
        self.cosine_distance_scores = cosine_distance_scores;
        self
    }

    pub(crate) fn positions(&self) -> &[PrimaryKeySearchPosition] {
        &self.positions
    }

    pub(crate) fn weight(&self) -> f64 {
        self.weight
    }
}

/// Java `PrimaryKeySearchPosition.compareTo`: partition bytes (unsigned,
/// shorter-is-less on a common prefix) → bucket → data file name → row position.
fn compare_to(left: &PrimaryKeySearchPosition, right: &PrimaryKeySearchPosition) -> Ordering {
    left.partition()
        .to_serialized_bytes()
        .cmp(&right.partition().to_serialized_bytes())
        .then_with(|| left.bucket().cmp(&right.bucket()))
        .then_with(|| left.data_file_name().cmp(right.data_file_name()))
        .then_with(|| left.row_position().cmp(&right.row_position()))
}

/// Java `LOCAL_BEST_FIRST`: score descending, then the physical tie-break.
/// `total_cmp` reproduces `Float.compare` for the finite scores these positions
/// are validated to carry.
fn best_first(left: &PrimaryKeySearchPosition, right: &PrimaryKeySearchPosition) -> Ordering {
    right
        .score()
        .total_cmp(&left.score())
        .then_with(|| compare_to(left, right))
}

/// Selects globally highest-scored physical positions without rewriting their
/// scores. Mirrors Java `topKByScore`.
#[allow(dead_code)]
pub(crate) fn top_k_by_score(
    rankings: &[Vec<PrimaryKeySearchPosition>],
    limit: usize,
) -> crate::Result<Vec<PrimaryKeySearchPosition>> {
    check_limit(limit, "Search result limit")?;
    let mut unique: HashMap<PrimaryKeySearchPosition, PrimaryKeySearchPosition> = HashMap::new();
    for ranking in rankings {
        for position in ranking {
            match unique.get(position) {
                Some(previous) if best_first(position, previous) != Ordering::Less => {}
                _ => {
                    unique.insert(position.clone(), position.clone());
                }
            }
        }
    }
    let mut result: Vec<PrimaryKeySearchPosition> = unique.into_values().collect();
    result.sort_by(best_first);
    result.truncate(limit);
    Ok(result)
}

/// Convenience wrapper for equally weighted routes. Mirrors Java `rrf`.
#[allow(dead_code)]
pub(crate) fn rrf(
    rankings: &[Vec<PrimaryKeySearchPosition>],
    limit: usize,
) -> crate::Result<Vec<PrimaryKeySearchPosition>> {
    let weighted: Vec<Ranking> = rankings
        .iter()
        .map(|ranking| Ranking::new(ranking.clone(), 1.0))
        .collect::<crate::Result<_>>()?;
    weighted_rrf(&weighted, limit)
}

/// Weighted reciprocal-rank fusion. Mirrors Java `weightedRrf`.
pub(crate) fn weighted_rrf(
    rankings: &[Ranking],
    limit: usize,
) -> crate::Result<Vec<PrimaryKeySearchPosition>> {
    check_limit(limit, "RRF result limit")?;
    let mut fused_scores: HashMap<PrimaryKeySearchPosition, f64> = HashMap::new();
    for ranking in rankings {
        add_ranking(&mut fused_scores, ranking)?;
    }
    top_k(fused_scores, limit)
}

/// Fuses heterogeneous route scores by normalizing each route to `[0, 1]`
/// as in Java `weightedScore`. Cosine routes retain precision through
/// normalization and fused ordering. A single cosine route keeps its
/// distance order, including when normalized scores round to a tie.
pub(crate) fn weighted_score(
    rankings: &[Ranking],
    limit: usize,
) -> crate::Result<Vec<PrimaryKeySearchPosition>> {
    check_limit(limit, "Weighted-score result limit")?;
    let has_cosine = rankings
        .iter()
        .any(|ranking| ranking.cosine_distance_scores);
    if has_cosine && rankings.len() > 1 {
        return weighted_score_with_cosine(rankings, limit);
    }

    let mut fused_scores: HashMap<PrimaryKeySearchPosition, f64> = HashMap::new();
    for ranking in rankings {
        let (min, max) = score_bounds(ranking)?;
        for position in ranking.positions() {
            let normalized = if ranking.cosine_distance_scores && max > min {
                (f64::from(position.score()) - f64::from(min)) / (f64::from(max) - f64::from(min))
            } else {
                f64::from(normalize_route_score(position.score(), min, max))
            };
            *fused_scores.entry(position.clone()).or_insert(0.0) += ranking.weight() * normalized;
        }
    }
    if has_cosine {
        // The normalized score is monotone within one cosine route. Order
        // directly by its original score so rounding cannot introduce ties.
        let mut ranked = fused_scores
            .into_iter()
            .map(|(position, score)| Ok((position.with_score(score as f32)?, position.score())))
            .collect::<crate::Result<Vec<_>>>()?;
        ranked.sort_by(|(left, left_order), (right, right_order)| {
            right_order
                .total_cmp(left_order)
                .then_with(|| compare_to(left, right))
        });
        ranked.truncate(limit);
        return Ok(ranked.into_iter().map(|(position, _)| position).collect());
    }
    top_k(fused_scores, limit)
}

fn score_bounds(ranking: &Ranking) -> crate::Result<(f32, f32)> {
    let mut unique = HashSet::new();
    let mut min = f32::INFINITY;
    let mut max = f32::NEG_INFINITY;
    for position in ranking.positions() {
        if !unique.insert(position) {
            return Err(data_invalid(format!(
                "One weighted-score ranking contains duplicate physical position {}.",
                describe(position)
            )));
        }
        min = min.min(position.score());
        max = max.max(position.score());
    }
    Ok((min, max))
}

fn weighted_score_with_cosine(
    rankings: &[Ranking],
    limit: usize,
) -> crate::Result<Vec<PrimaryKeySearchPosition>> {
    let mut fused_scores: HashMap<PrimaryKeySearchPosition, BigRational> = HashMap::new();
    for ranking in rankings {
        let (min, max) = score_bounds(ranking)?;
        let cosine_scale = (ranking.cosine_distance_scores && max > min).then(|| {
            // Min-max normalization is a ratio of finite binary floats. Keep
            // it exact so tiny distances survive normalization near one and
            // the sum of route contributions is independent of route order.
            let min = BigRational::from_float(min).expect("finite route score");
            let max = BigRational::from_float(max).expect("finite route score");
            let weight = BigRational::from_float(ranking.weight()).expect("finite route weight");
            let scale = weight / (max - &min);
            (min, scale)
        });
        for position in ranking.positions() {
            let contribution = match &cosine_scale {
                Some((min, scale)) => {
                    (BigRational::from_float(position.score()).expect("finite route score") - min)
                        * scale
                }
                None => {
                    let normalized = normalize_route_score(position.score(), min, max);
                    BigRational::from_float(ranking.weight() * f64::from(normalized))
                        .expect("finite weighted route score")
                }
            };
            *fused_scores.entry(position.clone()).or_default() += contribution;
        }
    }
    let mut ranked = fused_scores
        .into_iter()
        .map(|(position, exact_score)| {
            let score = exact_score.to_f64().expect("valid ratio");
            Ok((position.with_score(score as f32)?, score, exact_score))
        })
        .collect::<crate::Result<Vec<_>>>()?;
    ranked.sort_unstable_by(
        |(left, left_score, left_exact), (right, right_score, right_exact)| {
            right_score
                .total_cmp(left_score)
                .then_with(|| right_exact.cmp(left_exact))
                .then_with(|| compare_to(left, right))
        },
    );
    ranked.truncate(limit);
    Ok(ranked
        .into_iter()
        .map(|(position, _, _)| position)
        .collect())
}

/// Fuses routes using weighted reciprocal rank without the RRF smoothing
/// constant. Mirrors Java `weightedMrr`.
pub(crate) fn weighted_mrr(
    rankings: &[Ranking],
    limit: usize,
) -> crate::Result<Vec<PrimaryKeySearchPosition>> {
    check_limit(limit, "MRR result limit")?;
    let mut fused_scores: HashMap<PrimaryKeySearchPosition, f64> = HashMap::new();
    for ranking in rankings {
        let mut sorted = ranking.positions().to_vec();
        sorted.sort_by(best_first);
        let mut unique: HashSet<PrimaryKeySearchPosition> = HashSet::new();
        for (i, position) in sorted.iter().enumerate() {
            if !unique.insert(position.clone()) {
                return Err(data_invalid(format!(
                    "One MRR ranking contains duplicate physical position {}.",
                    describe(position)
                )));
            }
            *fused_scores.entry(position.clone()).or_insert(0.0) +=
                ranking.weight() / (i as f64 + 1.0);
        }
    }
    top_k(fused_scores, limit)
}

/// Java `checkArgument(limit > 0, ...)`.
fn check_limit(limit: usize, what: &str) -> crate::Result<()> {
    if limit == 0 {
        return Err(data_invalid(format!("{what} must be positive: {limit}.")));
    }
    Ok(())
}

fn describe(position: &PrimaryKeySearchPosition) -> String {
    format!(
        "PrimaryKeySearchPosition{{bucket={}, dataFileName='{}', rowPosition={}, score={}}}",
        position.bucket(),
        position.data_file_name(),
        position.row_position(),
        position.score()
    )
}

/// Mirrors Java `addRanking`: sort a route best-first, assign 1-based ranks that
/// ties share, and accumulate `weight / (DEFAULT_RRF_K + rank)` per position.
fn add_ranking(
    fused_scores: &mut HashMap<PrimaryKeySearchPosition, f64>,
    ranking: &Ranking,
) -> crate::Result<()> {
    let mut sorted = ranking.positions().to_vec();
    sorted.sort_by(best_first);
    let mut unique: HashSet<PrimaryKeySearchPosition> = HashSet::new();
    let mut rank = 0i32;
    let mut previous_score = f32::NAN;
    for (i, position) in sorted.iter().enumerate() {
        if !unique.insert(position.clone()) {
            return Err(data_invalid(format!(
                "One RRF ranking contains duplicate physical position {}.",
                describe(position)
            )));
        }
        if i == 0 || position.score().total_cmp(&previous_score) != Ordering::Equal {
            rank = i as i32 + 1;
            previous_score = position.score();
        }
        let contribution = ranking.weight() / f64::from(DEFAULT_RRF_K + rank);
        *fused_scores.entry(position.clone()).or_insert(0.0) += contribution;
    }
    Ok(())
}

/// Mirrors Java `topK`: rewrite each fused key's score to its fused value and
/// return the best `limit` positions, ordered best-first.
fn top_k(
    fused_scores: HashMap<PrimaryKeySearchPosition, f64>,
    limit: usize,
) -> crate::Result<Vec<PrimaryKeySearchPosition>> {
    let mut result: Vec<PrimaryKeySearchPosition> = fused_scores
        .into_iter()
        .map(|(position, score)| position.with_score(score as f32))
        .collect::<crate::Result<_>>()?;
    result.sort_by(best_first);
    result.truncate(limit);
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::BinaryRow;

    /// Mirrors the Java test's `position(dataFileName, rowPosition, score)`
    /// helper: `BinaryRow.EMPTY_ROW`, bucket `0`.
    fn position(data_file_name: &str, row_position: i64, score: f32) -> PrimaryKeySearchPosition {
        PrimaryKeySearchPosition::new(
            BinaryRow::new(0),
            0,
            data_file_name.to_string(),
            row_position,
            score,
        )
        .unwrap()
    }

    fn ranking(positions: Vec<PrimaryKeySearchPosition>, weight: f64) -> Ranking {
        Ranking::new(positions, weight).unwrap()
    }

    fn file_names(positions: &[PrimaryKeySearchPosition]) -> Vec<&str> {
        positions.iter().map(|p| p.data_file_name()).collect()
    }

    fn close(actual: f32, expected: f32) {
        assert!(
            (actual - expected).abs() < 0.000001,
            "expected {expected}, got {actual}"
        );
    }

    // Port of Java `testFusesDuplicatePositionsAndAssignsTheSameRankToLocalTies`.
    #[test]
    fn fuses_duplicate_positions_and_assigns_the_same_rank_to_local_ties() {
        let a = position("a", 0, 10.0);
        let b = position("b", 0, 10.0);
        let c = position("c", 0, 5.0);
        let fused = rrf(
            &[
                vec![c.clone(), b.clone(), a.clone()],
                vec![a.with_score(1.0).unwrap(), c.with_score(8.0).unwrap()],
            ],
            3,
        )
        .unwrap();

        assert_eq!(file_names(&fused), vec!["a", "c", "b"]);
        close(fused[0].score(), (1.0 / 61.0 + 1.0 / 62.0) as f32);
        close(fused[1].score(), (1.0 / 63.0 + 1.0 / 61.0) as f32);
        close(fused[2].score(), (1.0 / 61.0) as f32);
    }

    // Port of Java `testUsesRouteWeightsAndDeterministicPhysicalTieBreaking`.
    #[test]
    fn uses_route_weights_and_deterministic_physical_tie_breaking() {
        let a = position("a", 0, 1.0);
        let b = position("b", 0, 1.0);
        let fused = weighted_rrf(
            &[ranking(vec![a.clone()], 2.0), ranking(vec![b.clone()], 2.0)],
            1,
        )
        .unwrap();

        assert_eq!(fused.len(), 1);
        assert_eq!(fused[0].data_file_name(), "a");
        close(fused[0].score(), (2.0 / 61.0) as f32);
    }

    // Port of Java `testWeightedScoreNormalizesEachPhysicalRoute`.
    #[test]
    fn weighted_score_normalizes_each_physical_route() {
        let a = position("a", 0, 0.0);
        let b = position("b", 0, 10.0);
        let c = position("c", 0, 0.0);

        let fused = weighted_score(
            &[
                ranking(vec![a.clone(), b.clone()], 1.0),
                ranking(vec![a.with_score(100.0).unwrap(), c.clone()], 2.0),
            ],
            3,
        )
        .unwrap();

        assert_eq!(file_names(&fused), vec!["a", "b", "c"]);
        let scores: Vec<f32> = fused.iter().map(|p| p.score()).collect();
        assert_eq!(scores, vec![2.0, 1.0, 0.0]);
    }

    #[test]
    fn weighted_score_handles_finite_scores_with_overflowing_range() {
        let ranked = weighted_score(
            &[ranking(
                vec![
                    position("a", 0, -f32::MAX),
                    position("b", 0, 0.0),
                    position("c", 0, f32::MAX),
                ],
                4.0,
            )],
            2,
        )
        .unwrap();

        assert_eq!(file_names(&ranked), vec!["c", "b"]);
        assert_eq!(ranked[0].score(), 4.0);
        assert_eq!(ranked[1].score(), 2.0);
    }

    #[test]
    fn weighted_score_uses_physical_ties_for_non_cosine_routes() {
        let route = ranking(
            vec![
                position("file", 0, 1.0_f32.next_down()),
                position("file", 1, 1.0),
                position("file", 2, -1.0),
            ],
            1.0,
        );
        let fused = weighted_score(&[route], 1).unwrap();
        assert_eq!(fused[0].row_position(), 0);
        assert_eq!(fused[0].score(), 1.0);
    }

    #[test]
    fn rrf_k_matches_java_value() {
        assert_eq!(DEFAULT_RRF_K, 60);
    }

    // Negative and positive zero are distinct scores under `Float.compare`
    // semantics, so they must NOT share a fused rank: +0.0 ranks ahead of -0.0.
    #[test]
    fn signed_zero_scores_do_not_share_rank() {
        let a = position("a", 0, 0.0); // +0.0, best-first
        let b = position("b", 0, -0.0); // -0.0
        let fused = weighted_rrf(&[ranking(vec![a, b], 1.0)], 2).unwrap();

        assert_eq!(file_names(&fused), vec!["a", "b"]);
        close(fused[0].score(), (1.0 / 61.0) as f32); // rank 1
        close(fused[1].score(), (1.0 / 62.0) as f32); // rank 2 (would be 1/61 under `!=`)
    }

    #[test]
    fn duplicate_within_ranking_fails_for_weighted_rankers() {
        let a = position("a", 0, 3.0);
        let dup = a.with_score(9.0).unwrap();

        assert!(weighted_rrf(&[ranking(vec![a.clone(), dup.clone()], 1.0)], 3).is_err());
        assert!(rrf(&[vec![a.clone(), dup.clone()]], 3).is_err());
        assert!(weighted_score(&[ranking(vec![a.clone(), dup.clone()], 1.0)], 3).is_err());
        assert!(weighted_mrr(&[ranking(vec![a, dup], 1.0)], 3).is_err());
    }

    #[test]
    fn cross_ranking_combines_by_physical_key() {
        // The same physical position in two routes fuses into one entry whose
        // RRF score is the sum of both single-position contributions.
        let a = position("a", 0, 5.0);
        let fused = rrf(&[vec![a.clone()], vec![a.with_score(0.1).unwrap()]], 5).unwrap();
        assert_eq!(fused.len(), 1);
        assert_eq!(fused[0].data_file_name(), "a");
        close(fused[0].score(), (1.0 / 61.0 + 1.0 / 61.0) as f32);
    }

    #[test]
    fn weighted_mrr_uses_ordinal_rank() {
        // Ranks are 1-based ordinals of the score-sorted list; scores 3,2,1 give
        // MRR contributions 1/1, 1/2, 1/3 with weight 1.
        let a = position("a", 0, 3.0);
        let b = position("b", 0, 2.0);
        let c = position("c", 0, 1.0);
        let fused = weighted_mrr(&[ranking(vec![c, b, a], 1.0)], 3).unwrap();
        assert_eq!(file_names(&fused), vec!["a", "b", "c"]);
        close(fused[0].score(), 1.0);
        close(fused[1].score(), 0.5);
        close(fused[2].score(), (1.0 / 3.0) as f32);
    }

    #[test]
    fn top_k_by_score_keeps_best_instance_without_rewriting() {
        let a = position("a", 0, 2.0);
        let b = position("b", 0, 9.0);
        // Duplicate physical "a" across rankings: keep the higher raw score (7).
        let fused = top_k_by_score(
            &[vec![a.clone(), b.clone()], vec![a.with_score(7.0).unwrap()]],
            5,
        )
        .unwrap();
        assert_eq!(file_names(&fused), vec!["b", "a"]);
        assert_eq!(fused[0].score(), 9.0);
        assert_eq!(fused[1].score(), 7.0);
    }

    #[test]
    fn non_positive_limit_fails() {
        let a = position("a", 0, 1.0);
        assert!(rrf(&[vec![a.clone()]], 0).is_err());
        assert!(weighted_rrf(&[ranking(vec![a.clone()], 1.0)], 0).is_err());
        assert!(weighted_score(&[ranking(vec![a.clone()], 1.0)], 0).is_err());
        assert!(weighted_mrr(&[ranking(vec![a.clone()], 1.0)], 0).is_err());
        assert!(top_k_by_score(&[vec![a]], 0).is_err());
    }

    #[test]
    fn ranking_rejects_non_positive_or_non_finite_weight() {
        let a = position("a", 0, 1.0);
        assert!(Ranking::new(vec![a.clone()], 0.0).is_err());
        assert!(Ranking::new(vec![a.clone()], -1.0).is_err());
        assert!(Ranking::new(vec![a.clone()], f64::NAN).is_err());
        assert!(Ranking::new(vec![a.clone()], f64::INFINITY).is_err());
        assert!(Ranking::new(vec![a], 1.0).is_ok());
    }
}
