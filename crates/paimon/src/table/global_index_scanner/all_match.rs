// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Prove complete scalar BTree domains match without opening index files.

use super::entry::{sorted_entry_meta, GlobalIndexEntry, GlobalIndexFileKind};
use crate::btree::key_serde::KeyComparator;
use crate::btree::{make_key_comparator, serialize_datum, BTreeIndexMeta};
use crate::spec::{DataType, Datum, PredicateOperator};
use std::cmp::Ordering::{Equal, Greater, Less};
use std::collections::HashMap;

pub(super) fn all_matching_entries(
    entries: &[&GlobalIndexEntry],
    predicates: &[(PredicateOperator, &[Datum], &DataType)],
) -> Vec<Vec<bool>> {
    let mut groups: HashMap<(i64, i64), Vec<usize>> = HashMap::new();
    for (index, entry) in entries.iter().enumerate() {
        if entry.index_type == GlobalIndexFileKind::BTree {
            groups
                .entry((entry.row_range_start, entry.row_range_end))
                .or_default()
                .push(index);
        }
    }
    let comparisons: Vec<_> = predicates
        .iter()
        .map(|(op, literals, ty)| {
            (
                *op,
                literals
                    .iter()
                    .map(|literal| serialize_datum(literal, ty))
                    .collect::<Vec<_>>(),
                make_key_comparator(ty),
            )
        })
        .collect();
    let mut result = vec![vec![false; entries.len()]; predicates.len()];
    for ((start, end), files) in groups {
        let Some(expected_count) = end
            .checked_sub(start)
            .and_then(|len| len.checked_add(1))
            .filter(|len| *len > 0 && start >= 0)
        else {
            continue;
        };
        // Multiple files can partition keys within the SAME source row domain.
        // A single file's min/max never proves that it contains every source row.
        let count = files.iter().try_fold(0i64, |count, &index| {
            let rows = entries[index].row_count;
            (rows > 0).then(|| count.checked_add(rows)).flatten()
        });
        if count != Some(expected_count) {
            continue;
        }
        for (predicate_index, (op, values, cmp)) in comparisons.iter().enumerate() {
            let all_match = files.iter().all(|&index| {
                // Degradation: pure optimisation. This only proves that a file's whole
                // row range matches so the posting list need not be decoded. Without an
                // ordering nothing is proven, so the entry is simply not an all-match
                // and the ordinary query path handles it.
                entry_all_matches(sorted_entry_meta(entries[index]), *op, values, cmp)
                    .unwrap_or(false)
            });
            if all_match {
                for &index in &files {
                    result[predicate_index][index] = true;
                }
            }
        }
    }
    result
}

fn entry_all_matches(
    meta: &BTreeIndexMeta,
    op: PredicateOperator,
    values: &[Vec<u8>],
    cmp: &KeyComparator,
) -> crate::Result<bool> {
    let (Some(first), Some(last)) = (&meta.first_key, &meta.last_key) else {
        return Ok(false);
    };
    if meta.has_nulls || cmp(first, last)? == Greater {
        return Ok(false);
    }
    Ok(match (op, values) {
        (PredicateOperator::Eq, [value]) => {
            cmp(first, value)? == Equal && cmp(last, value)? == Equal
        }
        (PredicateOperator::Lt, [value]) => cmp(last, value)? == Less,
        (PredicateOperator::LtEq, [value]) => cmp(last, value)? != Greater,
        (PredicateOperator::Gt, [value]) => cmp(first, value)? == Greater,
        (PredicateOperator::GtEq, [value]) => cmp(first, value)? != Less,
        (PredicateOperator::Between, [from, to]) => {
            cmp(first, from)? != Less && cmp(last, to)? != Greater
        }
        _ => false,
    })
}
