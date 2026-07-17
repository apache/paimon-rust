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

use super::data_invalid;
use std::cmp::Ordering;

/// Order two distances the way Java `Float.compare` does: every NaN sorts after
/// all numeric values (so a NaN distance is always ranked worst, never best),
/// while non-NaN values keep `total_cmp`'s IEEE total order (which also matches
/// Java in ranking `-0.0` before `+0.0`). `f32::total_cmp` alone is unsuitable
/// here because it orders a negative NaN before every finite value, which would
/// let a NaN distance (e.g. from a non-finite stored vector under inner product)
/// win Top-1.
pub(crate) fn java_float_compare(a: f32, b: f32) -> Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => a.total_cmp(&b),
    }
}

/// Normalize a metric name: lowercase and `-` → `_`. NO trim (deliberately
/// stricter than the build-side `vindex::normalize_metric`, to match Java
/// `VectorSearchMetric.normalize`).
pub(crate) fn normalize_metric(metric: &str) -> String {
    metric.to_ascii_lowercase().replace('-', "_")
}

/// Numeric semantics for a supported vector search metric. Mirrors Java
/// `org.apache.paimon.globalindex.VectorSearchMetric`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum VectorSearchMetric {
    L2,
    Cosine,
    InnerProduct,
}

impl VectorSearchMetric {
    /// Map a vindex-core metric to this enum. Mirrors the build-side
    /// `RawVectorMetric::from_vindex`; lets the read path compare the metric a
    /// segment was trained with against the configured metric.
    pub(crate) fn from_vindex(metric: paimon_vindex_core::distance::MetricType) -> Self {
        match metric {
            paimon_vindex_core::distance::MetricType::L2 => Self::L2,
            paimon_vindex_core::distance::MetricType::Cosine => Self::Cosine,
            paimon_vindex_core::distance::MetricType::InnerProduct => Self::InnerProduct,
        }
    }

    /// Normalize, validate, and map to the enum. Errors on an unsupported metric.
    pub(crate) fn parse(metric: &str) -> crate::Result<Self> {
        match normalize_metric(metric).as_str() {
            "l2" => Ok(Self::L2),
            "cosine" => Ok(Self::Cosine),
            "inner_product" => Ok(Self::InnerProduct),
            other => Err(data_invalid(format!(
                "unsupported vector distance metric: {other}"
            ))),
        }
    }

    /// Canonical lowercase name, matching the vindex-core `MetricType::as_str`
    /// spelling so mismatch diagnostics read consistently on both sides.
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::L2 => "l2",
            Self::Cosine => "cosine",
            Self::InnerProduct => "inner_product",
        }
    }

    /// Lower-is-better distance for exact vector search.
    pub(crate) fn compute_distance(&self, query: &[f32], stored: &[f32]) -> f32 {
        match self {
            Self::L2 => squared_l2(query, stored),
            Self::Cosine => cosine_distance(cosine_similarity(query, stored)),
            Self::InnerProduct => -inner_product(query, stored),
        }
    }

    /// Convert a higher-is-better standardized index score to a lower-is-better
    /// distance. For L2 with `score == 0.0` this yields `inf` (natural f32
    /// behavior, matching Java — no clamp).
    pub(crate) fn score_to_distance(&self, score: f32) -> f32 {
        match self {
            Self::L2 => 1.0 / score - 1.0,
            Self::Cosine => cosine_distance(score),
            Self::InnerProduct => -score,
        }
    }

    /// Convert a lower-is-better canonical distance (as produced by
    /// `bucket_search`) to a higher-is-better score. Mirrors Java
    /// `PrimaryKeyVectorResult.score(distance)`. No clamping — natural f32
    /// behavior (L2 with `distance=inf` -> `0.0`), consistent with the sibling
    /// `score_to_distance`.
    /// NOTE: cosine here is `1 - distance` applied directly to the canonical
    /// distance; it does NOT reuse `score_to_distance`'s clamp.
    pub(crate) fn distance_to_score(&self, distance: f32) -> f32 {
        match self {
            Self::L2 => 1.0 / (1.0 + distance),
            Self::Cosine => 1.0 - distance,
            Self::InnerProduct => -distance,
        }
    }
}

fn squared_l2(query: &[f32], stored: &[f32]) -> f32 {
    let mut squared = 0.0f32;
    for i in 0..query.len() {
        let delta = query[i] - stored[i];
        squared += delta * delta;
    }
    squared
}

fn cosine_similarity(query: &[f32], stored: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    let mut query_norm = 0.0f32;
    let mut stored_norm = 0.0f32;
    for i in 0..query.len() {
        dot += query[i] * stored[i];
        query_norm += query[i] * query[i];
        stored_norm += stored[i] * stored[i];
    }
    let denominator = ((query_norm as f64).sqrt() * (stored_norm as f64).sqrt()) as f32;
    if denominator == 0.0 {
        0.0
    } else {
        dot / denominator
    }
}

fn inner_product(query: &[f32], stored: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    for i in 0..query.len() {
        dot += query[i] * stored[i];
    }
    dot
}

fn cosine_distance(similarity: f32) -> f32 {
    1.0 - similarity.clamp(-1.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A negative NaN (sign bit set): `f32::total_cmp` would order this *before*
    /// every finite value; `java_float_compare` must order it *after*.
    const NEGATIVE_NAN: f32 = f32::from_bits(0xffc00000);

    #[test]
    fn java_float_compare_sorts_all_nan_after_finite() {
        // Positive NaN after a finite value.
        assert_eq!(java_float_compare(f32::NAN, 1.0), Ordering::Greater);
        assert_eq!(java_float_compare(1.0, f32::NAN), Ordering::Less);
        // Negative NaN also after a finite value (the total_cmp trap).
        assert!(NEGATIVE_NAN.is_nan());
        assert_eq!(java_float_compare(NEGATIVE_NAN, 1.0), Ordering::Greater);
        assert_eq!(java_float_compare(1.0, NEGATIVE_NAN), Ordering::Less);
        // NaN after +inf too.
        assert_eq!(
            java_float_compare(f32::NAN, f32::INFINITY),
            Ordering::Greater
        );
        // Two NaNs compare equal.
        assert_eq!(java_float_compare(f32::NAN, NEGATIVE_NAN), Ordering::Equal);
    }

    #[test]
    fn java_float_compare_keeps_java_numeric_order() {
        // Ascending finite order preserved.
        assert_eq!(java_float_compare(1.0, 2.0), Ordering::Less);
        assert_eq!(java_float_compare(2.0, 1.0), Ordering::Greater);
        assert_eq!(java_float_compare(1.0, 1.0), Ordering::Equal);
        // -0.0 sorts before +0.0, matching Java Float.compare.
        assert_eq!(java_float_compare(-0.0, 0.0), Ordering::Less);
        assert_eq!(java_float_compare(0.0, -0.0), Ordering::Greater);
    }

    #[test]
    fn java_float_compare_ranks_negative_nan_worst_when_sorting() {
        // A -NaN distance must never sort ahead of a finite one: sorting ascending
        // (smallest = best) puts finite first, NaN last.
        let mut xs = [NEGATIVE_NAN, 3.0, 1.0, 2.0];
        xs.sort_by(|a, b| java_float_compare(*a, *b));
        assert_eq!(xs[0], 1.0);
        assert_eq!(xs[1], 2.0);
        assert_eq!(xs[2], 3.0);
        assert!(xs[3].is_nan());
    }

    #[test]
    fn test_normalize_lowercases_and_replaces_hyphens_without_trimming() {
        assert_eq!(normalize_metric("Inner-Product"), "inner_product");
        assert_eq!(normalize_metric("L2"), "l2");
        // No trim: surrounding whitespace is preserved (unlike the build-side helper).
        assert_eq!(normalize_metric(" l2 "), " l2 ");
    }

    #[test]
    fn test_parse_rejects_unsupported_metric() {
        assert!(VectorSearchMetric::parse("l2").is_ok());
        assert!(VectorSearchMetric::parse("cosine").is_ok());
        assert!(VectorSearchMetric::parse("inner_product").is_ok());
        assert!(VectorSearchMetric::parse("manhattan").is_err());
    }

    #[test]
    fn test_compute_distance_matches_java_anchor() {
        // Java PkVectorExactSearcherTest.testDistancesForSupportedMetrics:
        // q=[2,0], s=[1,0] -> l2=1.0, cosine=0.0, inner_product=-2.0
        let q = [2.0f32, 0.0];
        let s = [1.0f32, 0.0];
        assert_eq!(VectorSearchMetric::L2.compute_distance(&q, &s), 1.0);
        assert_eq!(VectorSearchMetric::Cosine.compute_distance(&q, &s), 0.0);
        assert_eq!(
            VectorSearchMetric::InnerProduct.compute_distance(&q, &s),
            -2.0
        );
    }

    #[test]
    fn test_cosine_zero_norm_similarity_is_zero() {
        let zero = [0.0f32, 0.0];
        let s = [1.0f32, 0.0];
        assert_eq!(VectorSearchMetric::Cosine.compute_distance(&zero, &s), 1.0);
    }

    #[test]
    fn test_cosine_non_perfect_square_norm_uses_f64_sqrt() {
        // query [0,3] -> norm 9, stored [1,2] -> norm 5; sqrt(5) is irrational
        // so the f32-sqrt path (each sqrt taken in f32, then widened) and the
        // f64-sqrt path (widen first, sqrt in f64) produce different f32 bits:
        // buggy similarity 0.8944271 vs correct 0.8944272. Encode the f64 contract
        // in the expected value (dot / (sqrt(9.0f64) * sqrt(5.0f64)) as f32) rather
        // than a magic literal, so this pins the Java-matching f64 arithmetic. The
        // cosine distance is `1 - similarity`, so the sqrt path is exercised here.
        let q = [0.0f32, 3.0];
        let s = [1.0f32, 2.0];
        let dot = 6.0f32;
        let denominator = ((9.0f64).sqrt() * (5.0f64).sqrt()) as f32;
        let expected_similarity = dot / denominator;
        assert_eq!(
            VectorSearchMetric::Cosine.compute_distance(&q, &s),
            1.0 - expected_similarity
        );
    }

    #[test]
    fn test_score_to_distance_l2_zero_score_is_infinite() {
        assert!(VectorSearchMetric::L2.score_to_distance(0.0).is_infinite());
        assert_eq!(VectorSearchMetric::L2.score_to_distance(0.5), 1.0); // 1/0.5 - 1
        assert_eq!(
            VectorSearchMetric::InnerProduct.score_to_distance(2.0),
            -2.0
        );
        assert_eq!(VectorSearchMetric::Cosine.score_to_distance(1.0), 0.0);
    }

    #[test]
    fn test_distance_to_score_per_metric_formula() {
        // L2: 1/(1+d); Cosine: 1-d; InnerProduct: -d.
        assert_eq!(VectorSearchMetric::L2.distance_to_score(0.0), 1.0);
        assert_eq!(VectorSearchMetric::L2.distance_to_score(1.0), 0.5);
        assert_eq!(VectorSearchMetric::L2.distance_to_score(3.0), 0.25);
        assert_eq!(VectorSearchMetric::Cosine.distance_to_score(0.0), 1.0);
        assert_eq!(VectorSearchMetric::Cosine.distance_to_score(1.0), 0.0);
        assert_eq!(VectorSearchMetric::InnerProduct.distance_to_score(0.0), 0.0);
        assert_eq!(
            VectorSearchMetric::InnerProduct.distance_to_score(3.0),
            -3.0
        );
    }

    #[test]
    fn test_distance_to_score_inverts_score_to_distance_off_boundary() {
        // Avoid boundary values (no L2 score=0/distance=inf, no cosine/ip infinities):
        // distance_to_score(score_to_distance(s)) == s for representative s.
        for (metric, s) in [
            (VectorSearchMetric::L2, 0.5f32),
            (VectorSearchMetric::Cosine, 0.25f32),
            (VectorSearchMetric::InnerProduct, 2.0f32),
        ] {
            let d = metric.score_to_distance(s);
            assert_eq!(metric.distance_to_score(d), s, "metric {metric:?}");
        }
    }

    #[test]
    fn test_from_vindex_maps_every_variant() {
        use paimon_vindex_core::distance::MetricType;
        assert_eq!(
            VectorSearchMetric::from_vindex(MetricType::L2),
            VectorSearchMetric::L2
        );
        assert_eq!(
            VectorSearchMetric::from_vindex(MetricType::Cosine),
            VectorSearchMetric::Cosine
        );
        assert_eq!(
            VectorSearchMetric::from_vindex(MetricType::InnerProduct),
            VectorSearchMetric::InnerProduct
        );
    }
}
