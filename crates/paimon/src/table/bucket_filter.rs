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

//! Bucket and partition predicate extraction and bucket hash pruning.

use crate::spec::{
    field_idx_to_partition_idx, BinaryRow, DataField, DataType, Datum, Predicate, PredicateOperator,
};
use std::collections::HashSet;

pub(super) fn split_partition_and_data_predicates(
    filter: Predicate,
    fields: &[DataField],
    partition_keys: &[String],
) -> (Option<Predicate>, Vec<Predicate>) {
    let mapping = field_idx_to_partition_idx(fields, partition_keys);
    let mut partition_predicates = Vec::new();
    let mut data_predicates = Vec::new();

    for conjunct in filter.split_and() {
        let strict_partition_only = conjunct.references_only_mapped_fields(&mapping);

        if let Some(projected) = conjunct.project_field_index_inclusive(&mapping) {
            partition_predicates.push(projected);
        }

        // Keep any conjunct that is not fully partition-only for data-level
        // stats pruning, even if part of it contributed to partition pruning.
        if !strict_partition_only {
            data_predicates.push(conjunct);
        }
    }

    let partition_predicate = if partition_predicates.is_empty() {
        None
    } else {
        Some(Predicate::and(partition_predicates))
    };

    (partition_predicate, data_predicates)
}

/// Extract a predicate projected onto the given key columns.
///
/// This is a generic utility that works for both partition keys and bucket keys.
/// Returns `None` if no conjuncts reference the given keys.
pub(super) fn extract_predicate_for_keys(
    filter: &Predicate,
    fields: &[DataField],
    keys: &[String],
) -> Option<Predicate> {
    if keys.is_empty() {
        return None;
    }
    let mapping = field_idx_to_partition_idx(fields, keys);
    let projected: Vec<Predicate> = filter
        .clone()
        .split_and()
        .into_iter()
        .filter_map(|conjunct| conjunct.project_field_index_inclusive(&mapping))
        .collect();
    if projected.is_empty() {
        None
    } else {
        Some(Predicate::and(projected))
    }
}

/// Compute the set of target buckets from a bucket predicate.
///
/// Extracts equal-value literals for each bucket key field from the predicate,
/// builds a BinaryRow, hashes it, and returns the target bucket(s).
///
/// Supports:
/// - `key = value` (single bucket)
/// - `key IN (v1, v2, ...)` (multiple buckets)
/// - AND of the above for composite bucket keys
///
/// Returns `None` if the predicate cannot determine target buckets (fail-open).
pub(super) fn compute_target_buckets(
    bucket_predicate: &Predicate,
    bucket_key_fields: &[DataField],
    total_buckets: i32,
) -> Option<HashSet<i32>> {
    if total_buckets <= 0 || bucket_key_fields.is_empty() {
        return None;
    }

    // Collect equal-value candidates per bucket key field (by projected index).
    // Each field can have one value (Eq) or multiple values (In).
    let num_keys = bucket_key_fields.len();
    let mut field_candidates: Vec<Option<Vec<&Datum>>> = vec![None; num_keys];

    collect_eq_candidates(bucket_predicate, &mut field_candidates);

    // All bucket key fields must have candidates.
    let candidates: Vec<&Vec<&Datum>> =
        field_candidates.iter().filter_map(|c| c.as_ref()).collect();
    if candidates.len() != num_keys {
        return None;
    }

    // Compute cartesian product of candidates and hash each combination.
    let mut buckets = HashSet::new();
    let mut combo: Vec<usize> = vec![0; num_keys];
    loop {
        let datums: Vec<(&Datum, &DataType)> = (0..num_keys)
            .map(|i| {
                let vals = field_candidates[i].as_ref().unwrap();
                (vals[combo[i]], bucket_key_fields[i].data_type())
            })
            .collect();

        if let Some(bucket) = BinaryRow::compute_bucket_from_datums(&datums, total_buckets) {
            buckets.insert(bucket);
        } else {
            return None;
        }

        // Advance the combination counter (rightmost first).
        let mut carry = true;
        for i in (0..num_keys).rev() {
            if carry {
                combo[i] += 1;
                if combo[i] < field_candidates[i].as_ref().unwrap().len() {
                    carry = false;
                } else {
                    combo[i] = 0;
                }
            }
        }
        if carry {
            break;
        }
    }

    if buckets.is_empty() {
        None
    } else {
        Some(buckets)
    }
}

/// Recursively collect Eq/In literal candidates from a predicate for each bucket key field.
fn collect_eq_candidates<'a>(
    predicate: &'a Predicate,
    field_candidates: &mut Vec<Option<Vec<&'a Datum>>>,
) {
    match predicate {
        Predicate::And(children) => {
            for child in children {
                collect_eq_candidates(child, field_candidates);
            }
        }
        Predicate::Leaf {
            index,
            op,
            literals,
            ..
        } => {
            if *index < field_candidates.len() {
                match op {
                    PredicateOperator::Eq => {
                        if let Some(lit) = literals.first() {
                            field_candidates[*index] = Some(vec![lit]);
                        }
                    }
                    PredicateOperator::In => {
                        if !literals.is_empty() {
                            field_candidates[*index] = Some(literals.iter().collect());
                        }
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }
}
