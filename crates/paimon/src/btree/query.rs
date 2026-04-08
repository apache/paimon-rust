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

//! BTree index query abstraction.
//!
//! Provides the `IndexQuery` trait for evaluating predicates against index readers,
//! and query optimization utilities like between-pattern detection.

use crate::btree::key_serde::serialize_datum;
use crate::btree::reader::BTreeIndexReader;
use crate::spec::{DataType, Datum, PredicateOperator};
use roaring::RoaringTreemap;
use std::cmp::Ordering;
use std::io;

/// Trait for index readers that can evaluate predicates and return row ID bitmaps.
#[async_trait::async_trait]
pub trait IndexQuery: Send + Sync {
    /// Evaluate a predicate and return matching row IDs.
    async fn query(
        &self,
        op: PredicateOperator,
        literals: &[Datum],
        data_type: &DataType,
    ) -> io::Result<RoaringTreemap>;
}

#[async_trait::async_trait]
impl<F> IndexQuery for BTreeIndexReader<F>
where
    F: Fn(&[u8], &[u8]) -> Ordering + Send + Sync,
{
    async fn query(
        &self,
        op: PredicateOperator,
        literals: &[Datum],
        data_type: &DataType,
    ) -> io::Result<RoaringTreemap> {
        match op {
            PredicateOperator::Eq => {
                let key = serialize_datum(&literals[0], data_type);
                self.query_equal(&key).await
            }
            PredicateOperator::Lt => {
                let key = serialize_datum(&literals[0], data_type);
                self.query_less_than(&key).await
            }
            PredicateOperator::LtEq => {
                let key = serialize_datum(&literals[0], data_type);
                self.query_less_or_equal(&key).await
            }
            PredicateOperator::Gt => {
                let key = serialize_datum(&literals[0], data_type);
                self.query_greater_than(&key).await
            }
            PredicateOperator::GtEq => {
                let key = serialize_datum(&literals[0], data_type);
                self.query_greater_or_equal(&key).await
            }
            PredicateOperator::In => {
                let keys: Vec<Vec<u8>> = literals
                    .iter()
                    .map(|lit| serialize_datum(lit, data_type))
                    .collect();
                let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
                self.query_in(&key_refs).await
            }
            PredicateOperator::IsNull => Ok(self.null_bitmap().clone()),
            PredicateOperator::IsNotNull => self.all_non_null_rows().await,
            PredicateOperator::NotEq => {
                let key = serialize_datum(&literals[0], data_type);
                self.query_not_equal(&key).await
            }
            PredicateOperator::NotIn => {
                let mut all_non_null = self.all_non_null_rows().await?;
                let keys: Vec<Vec<u8>> = literals
                    .iter()
                    .map(|lit| serialize_datum(lit, data_type))
                    .collect();
                let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
                let excluded = self.query_in(&key_refs).await?;
                all_non_null -= excluded;
                Ok(all_non_null)
            }
        }
    }
}

/// Detected between pattern from predicate pairs.
pub(crate) struct BetweenInfo<'a> {
    pub from: &'a Datum,
    pub to: &'a Datum,
    pub from_inclusive: bool,
    pub to_inclusive: bool,
    pub data_type: &'a DataType,
}

pub(crate) type ExtractBetweenResult<'a> = (
    Option<BetweenInfo<'a>>,
    Vec<(PredicateOperator, &'a [Datum], &'a DataType)>,
);

/// Try to extract a between pattern (lower + upper bound) from predicates.
/// Returns (between_info, remaining_predicates).
pub(crate) fn extract_between<'a>(
    predicates: &[(PredicateOperator, &'a [Datum], &'a DataType)],
) -> ExtractBetweenResult<'a> {
    if predicates.len() < 2 {
        return (None, predicates.to_vec());
    }

    let mut lower: Option<(usize, bool)> = None; // (index, inclusive)
    let mut upper: Option<(usize, bool)> = None;

    for (i, (op, literals, _)) in predicates.iter().enumerate() {
        if literals.len() != 1 {
            continue;
        }
        match op {
            PredicateOperator::GtEq if lower.is_none() => lower = Some((i, true)),
            PredicateOperator::Gt if lower.is_none() => lower = Some((i, false)),
            PredicateOperator::LtEq if upper.is_none() => upper = Some((i, true)),
            PredicateOperator::Lt if upper.is_none() => upper = Some((i, false)),
            _ => {}
        }
    }

    match (lower, upper) {
        (Some((li, from_inclusive)), Some((ui, to_inclusive))) => {
            let between = BetweenInfo {
                from: &predicates[li].1[0],
                to: &predicates[ui].1[0],
                from_inclusive,
                to_inclusive,
                data_type: predicates[li].2,
            };
            let remaining: Vec<_> = predicates
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != li && *i != ui)
                .map(|(_, p)| *p)
                .collect();
            (Some(between), remaining)
        }
        _ => (None, predicates.to_vec()),
    }
}
