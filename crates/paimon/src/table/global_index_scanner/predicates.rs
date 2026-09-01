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

//! Predicate capability checks and fallback-policy decisions.

use super::entry::{GlobalIndexEntry, GlobalIndexFileKind};
use crate::spec::{DataType, Datum, PredicateOperator};

/// Whether the sorted global index can evaluate this operator directly.
/// Operators that fall outside this set bypass the index and are evaluated
/// later in the read pipeline (stats prune + parquet row filter).
pub(super) fn is_sorted_global_index_supported_op(op: PredicateOperator) -> bool {
    matches!(
        op,
        PredicateOperator::Eq
            | PredicateOperator::NotEq
            | PredicateOperator::Lt
            | PredicateOperator::LtEq
            | PredicateOperator::Gt
            | PredicateOperator::GtEq
            | PredicateOperator::In
            | PredicateOperator::NotIn
            | PredicateOperator::IsNull
            | PredicateOperator::IsNotNull
            | PredicateOperator::Between
            | PredicateOperator::NotBetween
            | PredicateOperator::StartsWith
            | PredicateOperator::EndsWith
            | PredicateOperator::Contains
            | PredicateOperator::Like
            | PredicateOperator::ArrayContains
            | PredicateOperator::ArraysOverlap
            | PredicateOperator::ArrayContainsAll
    )
}

pub(super) fn is_multivalue_predicate(op: PredicateOperator) -> bool {
    matches!(
        op,
        PredicateOperator::ArrayContains
            | PredicateOperator::ArraysOverlap
            | PredicateOperator::ArrayContainsAll
    )
}

pub(super) fn entry_supports_predicate(
    entry: &GlobalIndexEntry,
    op: PredicateOperator,
    literals: &[Datum],
) -> bool {
    match entry.index_type {
        GlobalIndexFileKind::Multivalue => {
            is_multivalue_predicate(op)
                && !(matches!(op, PredicateOperator::ArrayContainsAll) && literals.is_empty())
        }
        GlobalIndexFileKind::FM => {
            matches!(op, PredicateOperator::IsNull | PredicateOperator::IsNotNull)
                || (op == PredicateOperator::Contains && literals.len() == 1)
        }
        GlobalIndexFileKind::BTree | GlobalIndexFileKind::Bitmap => !is_multivalue_predicate(op),
    }
}

pub(super) fn entries_support_predicate(
    entries: &[GlobalIndexEntry],
    op: PredicateOperator,
    literals: &[Datum],
) -> bool {
    entries
        .iter()
        .any(|entry| entry_supports_predicate(entry, op, literals))
}

pub(super) fn select_entries_for_predicates<'a>(
    entries: &'a [GlobalIndexEntry],
    predicates: &[(PredicateOperator, &[Datum], &DataType)],
) -> Vec<&'a GlobalIndexEntry> {
    let compatible = entries
        .iter()
        .filter(|entry| {
            predicates
                .iter()
                .all(|(op, literals, _)| entry_supports_predicate(entry, *op, literals))
        })
        .collect::<Vec<_>>();

    if predicates
        .iter()
        .any(|(op, _, _)| *op == PredicateOperator::Contains)
        && compatible
            .iter()
            .any(|entry| entry.index_type == GlobalIndexFileKind::FM)
    {
        compatible
            .into_iter()
            .filter(|entry| entry.index_type == GlobalIndexFileKind::FM)
            .collect()
    } else {
        compatible
    }
}
