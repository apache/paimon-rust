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
use crate::spec::{like_match, DataType, Datum, PredicateOperator};
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
    F: Fn(&[u8], &[u8]) -> crate::Result<Ordering> + Send + Sync,
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
            PredicateOperator::Between => {
                let from = serialize_datum(&literals[0], data_type);
                let to = serialize_datum(&literals[1], data_type);
                self.range_query(&from, &to, true, true).await
            }
            PredicateOperator::NotBetween => {
                let mut all_non_null = self.all_non_null_rows().await?;
                let from = serialize_datum(&literals[0], data_type);
                let to = serialize_datum(&literals[1], data_type);
                let inside = self.range_query(&from, &to, true, true).await?;
                all_non_null -= inside;
                Ok(all_non_null)
            }
            PredicateOperator::StartsWith => {
                let key = serialize_datum(&literals[0], data_type);
                self.query_prefix(&key).await
            }
            PredicateOperator::EndsWith => {
                ensure_character_string(data_type, op)?;
                let suffix = serialize_datum(&literals[0], data_type);
                if suffix.is_empty() {
                    return self.all_non_null_rows().await;
                }
                self.scan_entries(move |key| key.ends_with(&suffix)).await
            }
            PredicateOperator::Contains => {
                ensure_character_string(data_type, op)?;
                let needle = serialize_datum(&literals[0], data_type);
                if needle.is_empty() {
                    return self.all_non_null_rows().await;
                }
                self.scan_entries(move |key| contains_bytes(key, &needle))
                    .await
            }
            PredicateOperator::Like => {
                ensure_character_string(data_type, op)?;
                let pattern = string_literal(literals, op)?.to_string();
                self.scan_entries(move |key| {
                    std::str::from_utf8(key).is_ok_and(|value| like_match(value, &pattern))
                })
                .await
            }
            PredicateOperator::ArrayContains
            | PredicateOperator::ArraysOverlap
            | PredicateOperator::ArrayContainsAll => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("BTree index does not support {op}"),
            )),
        }
    }
}

fn ensure_character_string(data_type: &DataType, op: PredicateOperator) -> io::Result<()> {
    if matches!(data_type, DataType::Char(_) | DataType::VarChar(_)) {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("BTree index {op} only supports string columns"),
        ))
    }
}

fn string_literal(literals: &[Datum], op: PredicateOperator) -> io::Result<&str> {
    match literals.first() {
        Some(Datum::String(value)) => Ok(value),
        Some(other) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("BTree index {op} requires a string literal, got {other}"),
        )),
        None => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("BTree index {op} requires one literal"),
        )),
    }
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

/// Detected between pattern from predicate pairs.
pub(crate) struct BetweenInfo<'a> {
    pub from: &'a Datum,
    pub to: &'a Datum,
    pub from_inclusive: bool,
    pub to_inclusive: bool,
    pub data_type: &'a DataType,
}

impl BetweenInfo<'_> {
    pub(crate) fn is_empty(&self) -> bool {
        let cmp = crate::btree::make_key_comparator(self.data_type);
        match cmp(
            &serialize_datum(self.from, self.data_type),
            &serialize_datum(self.to, self.data_type),
        ) {
            Ok(Ordering::Greater) => true,
            Ok(Ordering::Equal) => !self.from_inclusive || !self.to_inclusive,
            Ok(Ordering::Less) => false,
            // Degradation: pure optimisation. This only shortcuts a provably empty
            // range to "no rows"; without an ordering we cannot prove that, so say
            // "not empty" and let the index read decide. Answering "empty" on an
            // error would drop rows.
            Err(_) => false,
        }
    }
}

pub(crate) type ExtractBetweenResult<'a> = (
    Option<BetweenInfo<'a>>,
    Vec<(PredicateOperator, &'a [Datum], &'a DataType)>,
);

/// Tighten all lower/upper bounds on one field into a single range query.
/// Returns (between_info, remaining_predicates).
///
/// Native `Between` leaves and explicit comparisons participate together.
/// Float comparisons intentionally follow the residual filter's bit-preserving
/// total order, not bitmap dictionary ordering (which canonicalizes all NaNs).
/// Bitmap floating ranges conservatively return all non-null candidates.
pub(crate) fn extract_between<'a>(
    predicates: &[(PredicateOperator, &'a [Datum], &'a DataType)],
) -> ExtractBetweenResult<'a> {
    // Degradation: pure optimisation. Merging bounds needs an ordering on the
    // serialized literals; without one, hand every predicate back so each is
    // evaluated on its own, exactly as when no complete range was found.
    try_extract_between(predicates).unwrap_or_else(|_| (None, predicates.to_vec()))
}

fn try_extract_between<'a>(
    predicates: &[(PredicateOperator, &'a [Datum], &'a DataType)],
) -> crate::Result<ExtractBetweenResult<'a>> {
    let Some((_, _, data_type)) = predicates.first() else {
        return Ok((None, Vec::new()));
    };
    if predicates.len() == 1 && predicates[0].0 != PredicateOperator::Between {
        return Ok((None, predicates.to_vec()));
    }
    let cmp = crate::btree::make_key_comparator(data_type);
    let mut lower: Option<(&Datum, Vec<u8>, bool)> = None;
    let mut upper: Option<(&Datum, Vec<u8>, bool)> = None;
    let mut remaining = Vec::new();
    for &(op, literals, ty) in predicates {
        if ty != *data_type {
            remaining.push((op, literals, ty));
            continue;
        }
        let (from, to) = match (op, literals) {
            (PredicateOperator::GtEq, [value]) => (Some((value, true)), None),
            (PredicateOperator::Gt, [value]) => (Some((value, false)), None),
            (PredicateOperator::LtEq, [value]) => (None, Some((value, true))),
            (PredicateOperator::Lt, [value]) => (None, Some((value, false))),
            (PredicateOperator::Between, [from, to]) => (Some((from, true)), Some((to, true))),
            _ => {
                remaining.push((op, literals, ty));
                continue;
            }
        };
        for (candidate, bound, tighter) in [
            (from, &mut lower, Ordering::Greater),
            (to, &mut upper, Ordering::Less),
        ] {
            if let Some((value, inclusive)) = candidate {
                let key = serialize_datum(value, data_type);
                match bound {
                    Some((_, existing, current_inclusive)) => match cmp(&key, existing)? {
                        Ordering::Equal => *current_inclusive &= inclusive,
                        order if order == tighter => *bound = Some((value, key, inclusive)),
                        _ => {}
                    },
                    None => *bound = Some((value, key, inclusive)),
                }
            }
        }
    }
    Ok(match (lower, upper) {
        (Some((from, _, from_inclusive)), Some((to, _, to_inclusive))) => (
            Some(BetweenInfo {
                from,
                to,
                from_inclusive,
                to_inclusive,
                data_type,
            }),
            remaining,
        ),
        _ => (None, predicates.to_vec()),
    })
}
