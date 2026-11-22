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

use num_bigint::{BigInt, BigUint, Sign};
use num_traits::{FromPrimitive, ToPrimitive};

use super::data_invalid;

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

/// Metric names and distance conventions follow Java
/// `org.apache.paimon.globalindex.VectorSearchMetric`. Rust's exact cosine
/// scorer rounds the distance after subtraction, so close matches can rank
/// differently from Java.
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

    /// Prepare the query once for exact scoring against multiple rows.
    pub(crate) fn prepare_query(self, query: &[f32]) -> PreparedVectorQuery<'_> {
        PreparedVectorQuery {
            metric: self,
            query,
            cosine_norm: if self == Self::Cosine {
                cosine_norm(query)
            } else {
                0.0
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn compute_distance(&self, query: &[f32], stored: &[f32]) -> f32 {
        self.prepare_query(query).compute_distance(stored)
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

pub(crate) struct PreparedVectorQuery<'a> {
    metric: VectorSearchMetric,
    query: &'a [f32],
    cosine_norm: f64,
}

impl PreparedVectorQuery<'_> {
    /// Lower-is-better distance for exact vector search.
    pub(crate) fn compute_distance(&self, stored: &[f32]) -> f32 {
        match self.metric {
            VectorSearchMetric::L2 => squared_l2(self.query, stored),
            VectorSearchMetric::Cosine => {
                cosine_score(self.query, self.cosine_norm, stored, CosineOutput::Distance)
            }
            VectorSearchMetric::InnerProduct => -inner_product(self.query, stored),
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

pub(crate) fn cosine_norm<T: Copy + Into<f64>>(vector: &[T]) -> f64 {
    vector
        .iter()
        .map(|&value| {
            let value = value.into();
            value * value
        })
        .sum::<f64>()
        .sqrt()
}

pub(crate) fn cosine_similarity(query: &[f32], query_norm: f64, stored: &[f32]) -> f32 {
    cosine_score(query, query_norm, stored, CosineOutput::Similarity)
}

#[inline]
fn cosine_score(query: &[f32], query_norm: f64, stored: &[f32], output: CosineOutput) -> f32 {
    let mut dot = 0.0_f64;
    let mut absolute_dot = 0.0_f64;
    let mut stored_squared_norm = 0.0_f64;
    for (&query, &stored) in query.iter().zip(stored) {
        let q = f64::from(query);
        let s = f64::from(stored);
        let product = q * s;
        dot += product;
        if matches!(output, CosineOutput::Similarity) {
            absolute_dot += product.abs();
        }
        stored_squared_norm += s * s;
    }
    let stored_norm = stored_squared_norm.sqrt();
    let norm_product = query_norm * stored_norm;
    let absolute_dot_bound = match output {
        CosineOutput::Similarity => absolute_dot,
        CosineOutput::Distance => norm_product,
    };
    let dot = CosineDot::Approximate {
        value: dot,
        absolute_sum_bound: absolute_dot_bound,
    };
    let mut bounds = match output.try_round_from_dot(dot, norm_product, query.len()) {
        Ok(rounded) => return rounded,
        Err(bounds) => bounds,
    };
    if matches!(output, CosineOutput::Distance) && bounds.1 <= 0.5 {
        if bounds.0 == 0.0 && (query == stored || positively_proportional(query, stored)) {
            return 0.0;
        }
        let (lower, upper) = normalized_distance_bounds(query, stored, query_norm, stored_norm);
        bounds = (bounds.0.max(lower), bounds.1.min(upper));
        if bounds.0 == bounds.1 {
            return bounds.0;
        }
    }
    output.round_exact(dot, norm_product, query, stored, bounds, &mut Vec::new())
}

fn positively_proportional(query: &[f32], stored: &[f32]) -> bool {
    let Some((&query_pivot, &stored_pivot)) = query.iter().zip(stored).find(|(q, _)| **q != 0.0)
    else {
        return false;
    };
    let query_pivot = f64::from(query_pivot);
    let stored_pivot = f64::from(stored_pivot);
    // Both cross products are exact because the coordinates are finite f32.
    query_pivot * stored_pivot > 0.0
        && query
            .iter()
            .zip(stored)
            .all(|(&q, &s)| f64::from(q) * stored_pivot == f64::from(s) * query_pivot)
}

/// Bound the cosine distance as half the squared difference of unit vectors.
/// The distance interval narrows as the difference approaches zero.
fn normalized_distance_bounds(
    query: &[f32],
    stored: &[f32],
    query_norm: f64,
    stored_norm: f64,
) -> (f32, f32) {
    let roundoff = (query.len() as f64 + 8.0) * f64::EPSILON;
    if roundoff >= 0.5 {
        return (0.0, 2.0);
    }
    let inverse_query_norm = query_norm.recip();
    let inverse_stored_norm = stored_norm.recip();
    let mut squared_length = 0.0_f64;
    for (&q, &s) in query.iter().zip(stored) {
        let difference = f64::from(q) * inverse_query_norm - f64::from(s) * inverse_stored_norm;
        squared_length += difference * difference;
    }
    let length = squared_length.sqrt();
    // Each input has unit length. Bound the normalization errors in both
    // inputs and the relative error in the length of their difference.
    let error = (1.0 + length) * roundoff / (1.0 - roundoff);
    let lower = (length - error).max(0.0);
    let upper = (length + error).min(2.0);
    ((0.5 * lower * lower) as f32, (0.5 * upper * upper) as f32)
}

#[derive(Clone, Copy)]
pub(crate) enum CosineDot {
    Exact(f64),
    Approximate { value: f64, absolute_sum_bound: f64 },
}

#[derive(Clone, Copy)]
pub(crate) enum CosineOutput {
    Similarity,
    Distance,
}

impl CosineOutput {
    fn value(self, similarity: f64) -> f64 {
        match self {
            Self::Similarity => similarity,
            Self::Distance => 1.0 - similarity,
        }
    }

    #[inline]
    pub(crate) fn round_from_dot<T: Copy + Into<f64>>(
        self,
        dot_product: CosineDot,
        norm_product: f64,
        query: &[T],
        stored: &[T],
        partials: &mut Vec<f64>,
    ) -> f32 {
        self.try_round_from_dot(dot_product, norm_product, query.len())
            .unwrap_or_else(|bounds| {
                self.round_exact(dot_product, norm_product, query, stored, bounds, partials)
            })
    }

    #[inline]
    pub(crate) fn try_round_from_dot(
        self,
        dot_product: CosineDot,
        norm_product: f64,
        dimension: usize,
    ) -> Result<f32, (f32, f32)> {
        let (dot, absolute_dot_bound) = match dot_product {
            CosineDot::Exact(value) => (value, 0.0),
            CosineDot::Approximate {
                value,
                absolute_sum_bound,
            } => (value, absolute_sum_bound),
        };
        if norm_product == 0.0 {
            return Ok(self.value(0.0) as f32);
        }
        if !norm_product.is_finite() {
            return Ok(self.value(dot / norm_product) as f32);
        }
        let inverse_norm = norm_product.recip();
        let estimate = self.value(dot * inverse_norm);
        // The products of two f32 coordinates are exact in f64. Bound the
        // accumulation error in the dot and both squared norms, then allow
        // for the square roots, normalization, and distance subtraction.
        let error = 8.0
            * f64::EPSILON
            * (dimension as f64 + 2.0)
            * (absolute_dot_bound + dot.abs())
            * inverse_norm
            + f64::EPSILON * estimate.abs();
        let lower = (estimate - error) as f32;
        let upper = (estimate + error) as f32;
        if lower.to_bits() == upper.to_bits() {
            return Ok(lower);
        }
        let lower = self.clamp(lower);
        let upper = self.clamp(upper);
        if lower.to_bits() == upper.to_bits() {
            return Ok(lower);
        }
        Err((lower, upper))
    }

    fn clamp(self, value: f32) -> f32 {
        match self {
            Self::Similarity => value.clamp(-1.0, 1.0),
            Self::Distance => value.clamp(0.0, 2.0),
        }
    }

    #[cold]
    pub(crate) fn round_exact<T: Copy + Into<f64>>(
        self,
        dot_product: CosineDot,
        norm_product: f64,
        query: &[T],
        stored: &[T],
        mut bounds: (f32, f32),
        partials: &mut Vec<f64>,
    ) -> f32 {
        let dot = match dot_product {
            CosineDot::Exact(dot) => scale_f32_product(dot),
            CosineDot::Approximate { .. } => exact_dot(query, stored, partials),
        };
        if dot.sign() == Sign::NoSign {
            return self.value(0.0) as f32;
        }
        let recovered_dot = match (self, dot_product) {
            (Self::Similarity, CosineDot::Approximate { .. }) => exact_dot_as_f64(&dot),
            _ => None,
        };
        if let Some(dot) = recovered_dot {
            // Cancellation error is gone. The approximate norms may already
            // be sufficient to decide the rounded similarity.
            match self.try_round_from_dot(CosineDot::Exact(dot), norm_product, query.len()) {
                Ok(score) => return score,
                Err((lower, upper)) => bounds = (bounds.0.max(lower), bounds.1.min(upper)),
            }
        }
        let negative = dot.sign() == Sign::Minus;
        let norms_squared = exact_dot(query, query, partials).into_parts().1
            * exact_dot(stored, stored, partials).into_parts().1;
        let dot_squared = dot.magnitude() * dot.magnitude();
        if dot_squared == norms_squared {
            return self.value(if negative { -1.0 } else { 1.0 }) as f32;
        }

        // Every midpoint between adjacent f32 results is a multiple of 2^-150.
        // Compare the exact cosine with each midpoint by squaring both sides.
        // For a distance midpoint m, compare the cosine with 1 - m, retaining
        // the tiny difference from 1 as an integer even if f64 would lose it.
        const MIDPOINT_SCALE_EXPONENT: usize = 150;
        let scale = 2.0_f64.powi(MIDPOINT_SCALE_EXPONENT as i32);
        let one = BigUint::from(1_u8) << MIDPOINT_SCALE_EXPONENT;
        let scaled_dot_squared = dot_squared << (2 * MIDPOINT_SCALE_EXPONENT);
        let compare_magnitude =
            |threshold: &BigUint| scaled_dot_squared.cmp(&(&norms_squared * threshold * threshold));
        let (lower, upper) = if matches!(self, Self::Similarity) && negative {
            (-bounds.1, -bounds.0)
        } else {
            bounds
        };
        let mut first = lower.max(0.0).abs().to_bits();
        let mut last = upper.max(0.0).abs().to_bits();
        while first < last {
            let bits = first + (last - first) / 2;
            let lower = f32::from_bits(bits);
            let midpoint = (f64::from(lower) + f64::from(lower.next_up())) * 0.5;
            let threshold = BigUint::from_f64(midpoint * scale).expect("finite f32 midpoint");
            let ordering = match self {
                Self::Similarity => compare_magnitude(&threshold),
                Self::Distance if !negative && threshold < one => {
                    compare_magnitude(&(&one - &threshold)).reverse()
                }
                Self::Distance if negative && threshold > one => {
                    compare_magnitude(&(&threshold - &one))
                }
                Self::Distance if negative => Ordering::Greater,
                Self::Distance => Ordering::Less,
            };
            if ordering == Ordering::Greater || (ordering == Ordering::Equal && bits & 1 != 0) {
                first = bits + 1;
            } else {
                last = bits;
            }
        }
        let rounded = f32::from_bits(first);
        if matches!(self, Self::Similarity) && negative {
            -rounded
        } else {
            rounded
        }
    }
}

/// Sum the products exactly in units of 2^-298, the square of the smallest
/// nonzero f32. Keep the floating-point residuals until conversion so the loop
/// over coordinates does not need arbitrary-precision arithmetic.
fn exact_dot<T: Copy + Into<f64>>(query: &[T], stored: &[T], partials: &mut Vec<f64>) -> BigInt {
    partials.clear();
    for (&query, &stored) in query.iter().zip(stored) {
        let mut term = query.into() * stored.into();
        let mut kept = 0;
        for i in 0..partials.len() {
            let mut part = partials[i];
            if term.abs() < part.abs() {
                std::mem::swap(&mut term, &mut part);
            }
            // With |term| >= |part|, FastTwoSum gives an exact residual.
            let sum = term + part;
            let residual = part - (sum - term);
            if residual != 0.0 {
                partials[kept] = residual;
                kept += 1;
            }
            term = sum;
        }
        partials.truncate(kept);
        if term != 0.0 {
            partials.push(term);
        }
    }
    partials.iter().copied().map(scale_f32_product).sum()
}

fn scale_f32_product(value: f64) -> BigInt {
    BigInt::from_f64(value * 2.0_f64.powi(298)).expect("finite f32 products")
}

fn exact_dot_as_f64(dot: &BigInt) -> Option<f64> {
    // The remaining bits must fit the f64 significand after removing the
    // common power of two. Dot products are in units of 2^-298.
    let significant_bits = dot.bits() - dot.trailing_zeros().unwrap_or(0);
    if significant_bits > u64::from(f64::MANTISSA_DIGITS) {
        return None;
    }
    dot.to_f64().map(|scaled| scaled * 2.0_f64.powi(-298))
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
    fn test_recovered_dot_resolves_cosine_with_approximate_norms() {
        let tiny = 2.0_f32.powi(-40);
        let query = [1.0, tiny, 1.0];
        let query_norm = cosine_norm(&query);
        let mut partials = Vec::new();
        let similarity = CosineOutput::Similarity;
        for sign in [1.0, -1.0] {
            let stored = [1.0, sign * tiny, -1.0];
            let norm_product = query_norm * cosine_norm(&stored);
            assert!(similarity
                .try_round_from_dot(
                    CosineDot::Approximate {
                        value: 0.0,
                        absolute_sum_bound: 2.0,
                    },
                    norm_product,
                    query.len(),
                )
                .is_err());
            let dot = exact_dot_as_f64(&exact_dot(&query, &stored, &mut partials)).unwrap();
            assert_eq!(dot, f64::from(sign) * 2.0_f64.powi(-80));
            let expected = sign * 2.0_f32.powi(-81);
            assert_eq!(
                similarity.try_round_from_dot(CosineDot::Exact(dot), norm_product, query.len()),
                Ok(expected)
            );
            assert_eq!(cosine_similarity(&query, query_norm, &stored), expected);
        }
        let inexact = exact_dot(&[1.0, tiny], &[1.0, tiny], &mut partials);
        assert_eq!(exact_dot_as_f64(&inexact), None);
    }

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
    fn test_cosine_zero_preserves_nonfinite_scores() {
        assert_eq!(cosine_similarity(&[0.0, 0.0], 0.0, &[1.0, 2.0]), 0.0);
        assert!(cosine_similarity(&[0.0, 0.0], 0.0, &[f32::INFINITY, 0.0]).is_nan());
    }

    #[test]
    fn test_cosine_exact_distance_is_invariant_to_vector_scale() {
        for scale in [1.0e20_f32, 1.0e-23_f32] {
            let query = [scale, 0.0];
            assert_eq!(
                VectorSearchMetric::Cosine.compute_distance(&query, &query),
                0.0
            );
            assert_eq!(
                VectorSearchMetric::Cosine.compute_distance(&query, &[-scale, 0.0]),
                2.0
            );
        }
    }

    #[test]
    fn test_cosine_distance_orders_near_parallel_with_underflowing_squares() {
        let scale = 2.0_f32.powi(-63);
        let mut query = [0.0; 257];
        query[256] = scale;
        let mut nearly_parallel = [scale * 2.0_f32.powi(-12); 257];
        nearly_parallel[256] = scale;

        let metric = VectorSearchMetric::Cosine;
        assert_eq!(metric.compute_distance(&query, &query), 0.0);
        assert!(metric.compute_distance(&query, &nearly_parallel) > 0.0);
    }

    #[test]
    fn test_cosine_distance_orders_near_orthogonal_with_underflowing_dot() {
        let similarity = 2.0_f32.powi(-24);
        let metric = VectorSearchMetric::Cosine;
        for scale in [1.0, 2.0_f32.powi(-63)] {
            let query = [scale, 0.0];
            let orthogonal = [0.0, scale];
            let nearly_orthogonal = [scale * similarity, scale];
            assert_eq!(metric.compute_distance(&query, &orthogonal), 1.0);
            assert_eq!(
                metric.compute_distance(&query, &nearly_orthogonal),
                1.0 - similarity
            );
        }
    }

    #[test]
    fn test_cosine_distance_orders_mixed_scale_candidates() {
        let query = [3.0, 13.0];
        let exact = [895.0 * query[0], 895.0 * query[1]];
        let metric = VectorSearchMetric::Cosine;
        // The squared norm product exceeds the squared dot by one:
        // (3 * 173 - 13 * 40)^2 == 1.
        let denominator = (178.0_f64 * 31_529.0).sqrt();
        let expected_distance = (1.0 / (denominator * (denominator + 2369.0))) as f32;
        for scale in [1.0, 2.0_f32.powi(64)] {
            let nearby = [40.0 * scale, 173.0 * scale];
            let nearby_distance = metric.compute_distance(&query, &nearby);
            assert_eq!(nearby_distance, expected_distance);
            assert_eq!(metric.compute_distance(&query, &exact), 0.0);
        }
    }

    #[test]
    fn test_cosine_distance_preserves_angle_below_f64_similarity_precision() {
        let tiny = 2.0_f32.powi(-50);
        let distance = VectorSearchMetric::Cosine.compute_distance(&[1.0, 0.0], &[1.0, tiny]);
        assert_eq!(distance, 2.0_f32.powi(-101));
    }

    #[test]
    fn test_normalized_distance_resolves_near_parallel_rounding() {
        let query = [1.0; 256];
        let query_norm = cosine_norm(&query);
        for k in 1..=13 {
            let mut stored = query;
            let delta = k as f32 * 2.0_f32.powi(-12);
            stored[0] += delta;
            let stored_norm = cosine_norm(&stored);

            // For q = [1, ..., 1] and s = q + delta*e_0, the squared norm
            // product minus the squared dot is (dimension - 1) * delta^2.
            let delta = f64::from(delta);
            let norm_product = query_norm * stored_norm;
            let dot = 256.0 + delta;
            let expected = (255.0 * delta * delta / (norm_product * (norm_product + dot))) as f32;
            assert!(CosineOutput::Distance
                .try_round_from_dot(
                    CosineDot::Approximate {
                        value: dot,
                        absolute_sum_bound: norm_product,
                    },
                    norm_product,
                    query.len(),
                )
                .is_err());
            assert_eq!(
                normalized_distance_bounds(&query, &stored, query_norm, stored_norm),
                (expected, expected)
            );
            assert_eq!(
                VectorSearchMetric::Cosine.compute_distance(&query, &stored),
                expected
            );
        }
    }

    #[test]
    fn test_cosine_distance_for_exact_rescalings() {
        let mut query: Vec<_> = (0..256).map(|i| ((31 * i % 27) - 13) as f32).collect();
        query[0] = 0.0;
        let prepared = VectorSearchMetric::Cosine.prepare_query(&query);
        for scale in 2..=17 {
            let stored: Vec<_> = query.iter().map(|&value| value * scale as f32).collect();
            assert!(positively_proportional(&query, &stored));
            assert_eq!(prepared.compute_distance(&stored), 0.0);
            let opposite: Vec<_> = stored.iter().map(|&value| -value).collect();
            assert!(!positively_proportional(&query, &opposite));
            assert_eq!(prepared.compute_distance(&opposite), 2.0);
        }
        let mut nearby = query.clone();
        nearby[0] = 2.0_f32.powi(-50);
        assert!(!positively_proportional(&query, &nearby));
        assert!(prepared.compute_distance(&nearby) > 0.0);
    }

    #[test]
    fn test_cosine_similarity_rounds_signed_subnormal_midpoints_to_even() {
        let tiny = 2.0_f32.powi(-75);
        let mut query = vec![tiny];
        let mut stored = vec![tiny];
        // Pad each squared norm to exactly one using disjoint coordinates.
        // The only shared product is 2^-150, the first f32 rounding midpoint.
        for i in 1..=75 {
            let value = 2.0_f32.powi(-i);
            query.extend([value, value, value, 0.0, 0.0, 0.0]);
            stored.extend([0.0, 0.0, 0.0, value, value, value]);
        }
        let norm = cosine_norm(&query);
        assert_eq!(
            cosine_similarity(&query, norm, &stored).to_bits(),
            0.0_f32.to_bits()
        );
        stored[0] = -tiny;
        assert_eq!(
            cosine_similarity(&query, norm, &stored).to_bits(),
            (-0.0_f32).to_bits()
        );
    }

    #[test]
    fn test_cosine_non_perfect_square_norm_uses_f64_sqrt() {
        // Taking the square roots in f32 can already lose a bit of the score.
        let q = [0.0f32, 3.0];
        let s = [1.0f32, 2.0];
        let denominator = (9.0_f64).sqrt() * (5.0_f64).sqrt();
        let expected_distance = (1.0 - 6.0 / denominator) as f32;
        assert_eq!(
            VectorSearchMetric::Cosine.compute_distance(&q, &s),
            expected_distance
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
