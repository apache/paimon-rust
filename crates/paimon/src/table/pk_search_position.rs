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

//! Shared scored physical row position for primary-key search.
//!
//! Rust mirror of Java
//! `org.apache.paimon.table.source.PrimaryKeySearchPosition`: a physical row
//! address `(partition, bucket, data_file_name, row_position)` carrying a search
//! `score`. Equality and hashing are on the physical identity ONLY (the score is
//! excluded) so hits fused from different search routes (vector, full-text) that
//! resolve to the same physical row collapse to one position regardless of their
//! per-route scores.

use std::hash::{Hash, Hasher};

use crate::spec::BinaryRow;

fn data_invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

/// A scored physical row position in a primary-key table snapshot. Mirrors Java
/// `PrimaryKeySearchPosition`.
#[derive(Clone, Debug)]
pub(crate) struct PrimaryKeySearchPosition {
    partition: BinaryRow,
    bucket: i32,
    data_file_name: String,
    row_position: i64,
    score: f32,
}

impl PrimaryKeySearchPosition {
    /// Build a position, rejecting a negative `row_position` or a non-finite
    /// `score` (NaN / ±Infinity). A negative position means a bogus physical
    /// address and a non-finite score would poison any score ordering, so fail
    /// loud rather than propagate it. Mirrors the `checkArgument`s in Java
    /// `PrimaryKeySearchPosition`.
    pub(crate) fn new(
        partition: BinaryRow,
        bucket: i32,
        data_file_name: String,
        row_position: i64,
        score: f32,
    ) -> crate::Result<Self> {
        if row_position < 0 {
            return Err(data_invalid(format!(
                "row position must not be negative, got {row_position} for {data_file_name}"
            )));
        }
        if !score.is_finite() {
            return Err(data_invalid(format!(
                "search score must be finite, got {score} for {data_file_name} @ {row_position}"
            )));
        }
        Ok(Self {
            partition,
            bucket,
            data_file_name,
            row_position,
            score,
        })
    }

    pub(crate) fn partition(&self) -> &BinaryRow {
        &self.partition
    }

    pub(crate) fn bucket(&self) -> i32 {
        self.bucket
    }

    pub(crate) fn data_file_name(&self) -> &str {
        &self.data_file_name
    }

    pub(crate) fn row_position(&self) -> i64 {
        self.row_position
    }

    pub(crate) fn score(&self) -> f32 {
        self.score
    }

    /// Returns a copy of this physical position carrying `new_score`, re-running
    /// the finite-score validation. Mirrors Java `PrimaryKeySearchPosition.withScore`.
    pub(crate) fn with_score(&self, new_score: f32) -> crate::Result<Self> {
        Self::new(
            self.partition.clone(),
            self.bucket,
            self.data_file_name.clone(),
            self.row_position,
            new_score,
        )
    }

    pub(crate) fn from_vector_position(
        position: &crate::vector_search::PrimaryKeySearchPosition,
    ) -> crate::Result<Self> {
        Self::new(
            position.partition.clone(),
            position.bucket,
            position.data_file_name.clone(),
            position.row_position,
            position.score,
        )
    }

    #[cfg(feature = "fulltext")]
    pub(crate) fn from_full_text_candidate(
        candidate: &crate::table::pk_full_text_read::PrimaryKeyFullTextCandidate,
    ) -> crate::Result<Self> {
        Self::new(
            candidate.partition.clone(),
            candidate.bucket,
            candidate.data_file_name.clone(),
            candidate.row_position,
            candidate.score,
        )
    }
}

impl PartialEq for PrimaryKeySearchPosition {
    /// Physical identity only: `(partition, bucket, data_file_name,
    /// row_position)`. The `score` is deliberately excluded (mirrors Java
    /// `PrimaryKeySearchPosition.equals`). The partition is compared by its
    /// serialized bytes, matching how the vector/full-text routes group
    /// partitions and keeping equality consistent with [`Hash`].
    fn eq(&self, other: &Self) -> bool {
        self.bucket == other.bucket
            && self.row_position == other.row_position
            && self.data_file_name == other.data_file_name
            && self.partition.to_serialized_bytes() == other.partition.to_serialized_bytes()
    }
}

impl Eq for PrimaryKeySearchPosition {}

impl Hash for PrimaryKeySearchPosition {
    /// Hashes the same physical-identity fields [`PartialEq`] compares (score
    /// excluded). `BinaryRow` is not `Hash`, so the partition is hashed via its
    /// serialized bytes, exactly the comparator the vector route uses.
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.partition.to_serialized_bytes().hash(state);
        self.bucket.hash(state);
        self.data_file_name.hash(state);
        self.row_position.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn pos(row_position: i64, score: f32) -> crate::Result<PrimaryKeySearchPosition> {
        PrimaryKeySearchPosition::new(BinaryRow::new(0), 0, "f".to_string(), row_position, score)
    }

    #[test]
    fn new_rejects_negative_row_position() {
        assert!(pos(-1, 1.0).is_err());
        assert!(pos(0, 1.0).is_ok());
    }

    #[test]
    fn new_rejects_non_finite_score() {
        assert!(pos(0, f32::NAN).is_err());
        assert!(pos(0, f32::INFINITY).is_err());
        assert!(pos(0, f32::NEG_INFINITY).is_err());
        assert!(pos(0, 1.5).is_ok());
    }

    #[test]
    fn equal_and_hash_equal_ignoring_score() {
        let a = pos(3, 0.1).unwrap();
        let b = pos(3, 0.9).unwrap();
        assert_eq!(a, b, "positions differing only in score must be equal");
        let mut set = HashSet::new();
        set.insert(a);
        set.insert(b);
        assert_eq!(set.len(), 1, "score must not affect hash-identity");
    }

    #[test]
    fn differing_physical_identity_is_not_equal() {
        let a = pos(3, 0.5).unwrap();
        let b = pos(4, 0.5).unwrap();
        assert_ne!(a, b);
        let mut set = HashSet::new();
        set.insert(a);
        set.insert(b);
        assert_eq!(set.len(), 2);
    }

    fn vector_position(score: f32) -> crate::vector_search::PrimaryKeySearchPosition {
        crate::vector_search::PrimaryKeySearchPosition {
            partition: BinaryRow::new(0),
            bucket: 0,
            data_file_name: "f".to_string(),
            row_position: 0,
            score,
        }
    }

    #[test]
    fn from_vector_position_preserves_score_and_validates_position() {
        // SearchResult already converted distance to score; fusion must preserve it.
        let mut hit = vector_position(0.5);
        let position = PrimaryKeySearchPosition::from_vector_position(&hit).unwrap();
        assert_eq!(position.score(), 0.5);
        assert_eq!(position.row_position(), 0);
        assert_eq!(position.data_file_name(), "f");
        for score in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            hit.score = score;
            assert!(PrimaryKeySearchPosition::from_vector_position(&hit).is_err());
        }
        hit.score = 0.5;
        hit.row_position = -1;
        assert!(PrimaryKeySearchPosition::from_vector_position(&hit).is_err());
    }

    #[cfg(feature = "fulltext")]
    #[test]
    fn from_full_text_candidate_preserves_raw_score() {
        use crate::table::pk_full_text_read::PrimaryKeyFullTextCandidate;
        let candidate =
            PrimaryKeyFullTextCandidate::new(0, BinaryRow::new(0), 0, 0.75, "f".to_string(), 2)
                .unwrap();
        let position = PrimaryKeySearchPosition::from_full_text_candidate(&candidate).unwrap();
        assert_eq!(position.score(), 0.75);
        assert_eq!(position.row_position(), 2);
    }
}
