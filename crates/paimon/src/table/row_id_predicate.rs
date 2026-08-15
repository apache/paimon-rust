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

//! Extract `_ROW_ID` predicates from a filter and convert them to row ranges.
//!
//! Reference: [org.apache.paimon.predicate.RowIdPredicateVisitor](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/predicate/RowIdPredicateVisitor.java)

use crate::spec::{is_row_id_column, Datum, Predicate, PredicateOperator};
use crate::table::RowRange;

/// Extract row ranges from `_ROW_ID` predicates in the given filter.
/// Returns `None` if no `_ROW_ID` predicates are found.
pub(crate) fn extract_row_id_ranges(predicate: &Predicate) -> Option<Vec<RowRange>> {
    match predicate {
        Predicate::Leaf {
            column,
            op,
            literals,
            ..
        } if is_row_id_column(column) => leaf_to_ranges(*op, literals),
        Predicate::And(children) => {
            // AND: intersect all _ROW_ID ranges
            let mut result: Option<Vec<RowRange>> = None;
            for child in children {
                if let Some(ranges) = extract_row_id_ranges(child) {
                    result = Some(match result {
                        None => ranges,
                        Some(existing) => intersect_range_lists(&existing, &ranges),
                    });
                }
            }
            result
        }
        Predicate::Or(children) => {
            // OR: union all _ROW_ID ranges (all children must have _ROW_ID predicates).
            // An empty union is `Some(vec![])`, not `None`: the branches convert
            // to "no rows", and reporting that as "nothing extracted" would let
            // an enclosing AND skip the subtree and overstate its ranges.
            let mut all_ranges: Vec<RowRange> = Vec::new();
            for child in children {
                let ranges = extract_row_id_ranges(child)?;
                all_ranges.extend(ranges);
            }
            Some(super::merge_row_ranges(all_ranges))
        }
        _ => None,
    }
}

/// Whether the extracted row ranges represent `conjunct` exactly, so it can be
/// dropped from the residual.
///
/// Two things must hold. The conjunct must be built entirely from convertible
/// `_ROW_ID` conditions — extraction ignores whatever it cannot convert, so its
/// ranges are merely a superset and anything mixed leaves a remainder they do
/// not carry. And extraction must actually have produced ranges *for this
/// conjunct*: an `Or` whose branches are all empty yields `None`, which the
/// enclosing `And` silently skips, so its ranges say nothing about it.
pub(crate) fn ranges_represent_conjunct(conjunct: &Predicate) -> bool {
    convertible_row_id_only(conjunct)
        && extract_row_id_ranges(conjunct).is_some_and(|ranges| !ranges.is_empty())
}

fn convertible_row_id_only(predicate: &Predicate) -> bool {
    match predicate {
        Predicate::Leaf {
            column,
            op,
            literals,
            ..
        } => is_row_id_column(column) && leaf_to_ranges(*op, literals).is_some(),
        Predicate::And(children) | Predicate::Or(children) => {
            children.iter().all(convertible_row_id_only)
        }
        _ => false,
    }
}

/// The error for a read that cannot evaluate a `_ROW_ID` predicate. `read` names
/// the kind of read.
///
/// Only data-evolution reads attach the column; everywhere else the predicate is
/// unenforceable, and every alternative is a silent wrong answer — a dropped
/// conjunct, an all-null synthesized column, or a scan skipped entirely.
pub(crate) fn unsupported_row_id_filter(read: &str) -> crate::Error {
    crate::Error::Unsupported {
        message: format!(
            "filtering on '_ROW_ID' is not supported by {read}; it is available on \
             data-evolution reads, or via row ranges"
        ),
    }
}

/// Reject a `_ROW_ID` predicate on a read that cannot synthesize row ids.
pub(crate) fn reject_row_id_filter(predicates: &[Predicate], read: &str) -> crate::Result<()> {
    if predicates.iter().any(references_row_id) {
        return Err(unsupported_row_id_filter(read));
    }
    Ok(())
}

/// Whether any leaf of `predicate` references `_ROW_ID`.
pub(crate) fn references_row_id(predicate: &Predicate) -> bool {
    match predicate {
        Predicate::Leaf { column, .. } => is_row_id_column(column),
        Predicate::And(children) | Predicate::Or(children) => {
            children.iter().any(references_row_id)
        }
        Predicate::Not(inner) => references_row_id(inner),
        Predicate::AlwaysTrue | Predicate::AlwaysFalse => false,
    }
}

fn datum_to_i64(datum: &Datum) -> Option<i64> {
    match datum {
        Datum::Long(v) => Some(*v),
        Datum::Int(v) => Some(*v as i64),
        _ => None,
    }
}

fn leaf_to_ranges(op: PredicateOperator, literals: &[Datum]) -> Option<Vec<RowRange>> {
    match op {
        PredicateOperator::Eq => {
            let v = datum_to_i64(literals.first()?)?;
            Some(vec![RowRange::new(v, v)])
        }
        PredicateOperator::GtEq => {
            let v = datum_to_i64(literals.first()?)?;
            Some(vec![RowRange::new(v, i64::MAX)])
        }
        PredicateOperator::Gt => {
            let v = datum_to_i64(literals.first()?)?;
            if v == i64::MAX {
                return Some(Vec::new());
            }
            Some(vec![RowRange::new(v + 1, i64::MAX)])
        }
        PredicateOperator::LtEq => {
            let v = datum_to_i64(literals.first()?)?;
            if v < 0 {
                return Some(Vec::new());
            }
            Some(vec![RowRange::new(0, v)])
        }
        PredicateOperator::Lt => {
            let v = datum_to_i64(literals.first()?)?;
            if v <= 0 {
                return Some(Vec::new());
            }
            Some(vec![RowRange::new(0, v - 1)])
        }
        PredicateOperator::In => {
            // EVERY literal, or none: ranges standing for only part of the leaf
            // would still read as the whole of it and drop the conjunct.
            let mut ranges: Vec<RowRange> = literals
                .iter()
                .map(|d| datum_to_i64(d).map(|v| RowRange::new(v, v)))
                .collect::<Option<Vec<_>>>()?;
            if ranges.is_empty() {
                return None;
            }
            ranges.sort_by_key(|r| r.from());
            Some(ranges)
        }
        _ => None,
    }
}

/// Intersect two sorted range lists.
/// Intersect two range lists that are already sorted and merged.
pub(crate) fn intersect_sorted_ranges(a: &[RowRange], b: &[RowRange]) -> Vec<RowRange> {
    intersect_range_lists(a, b)
}

fn intersect_range_lists(a: &[RowRange], b: &[RowRange]) -> Vec<RowRange> {
    let mut result = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        let from = a[i].from().max(b[j].from());
        let to = a[i].to().min(b[j].to());
        if from <= to {
            result.push(RowRange::new(from, to));
        }
        if a[i].to() < b[j].to() {
            i += 1;
        } else {
            j += 1;
        }
    }
    result
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_a_partially_convertible_in_is_not_converted_at_all() {
        let partial = row_id_leaf(
            PredicateOperator::In,
            vec![Datum::Long(5), Datum::String("7".into())],
        );
        assert_eq!(extract_row_id_ranges(&partial), None);
        assert!(!ranges_represent_conjunct(&partial));

        let whole = row_id_leaf(PredicateOperator::In, vec![Datum::Long(5), Datum::Long(7)]);
        assert_eq!(
            extract_row_id_ranges(&whole),
            Some(vec![RowRange::new(5, 5), RowRange::new(7, 7)])
        );
        assert!(ranges_represent_conjunct(&whole));
    }
    use super::*;
    use crate::spec::row_id_leaf;
    use crate::spec::{BigIntType, DataType};

    #[test]
    fn test_references_row_id_finds_nested_leaves() {
        let other = Predicate::Leaf {
            column: "v".to_string(),
            index: 0,
            data_type: DataType::BigInt(BigIntType::new()),
            op: PredicateOperator::Eq,
            literals: vec![Datum::Long(7)],
        };
        let nested = Predicate::Not(Box::new(Predicate::or(vec![
            row_id_leaf(PredicateOperator::Eq, vec![Datum::Long(1)]),
            other.clone(),
        ])));
        assert!(references_row_id(&nested));
        assert!(!references_row_id(&other));
    }

    fn data_leaf() -> Predicate {
        Predicate::Leaf {
            column: "value".to_string(),
            index: 1,
            data_type: DataType::BigInt(BigIntType::new()),
            op: PredicateOperator::Eq,
            literals: vec![Datum::Long(42)],
        }
    }

    #[test]
    fn test_extract_eq() {
        let p = row_id_leaf(PredicateOperator::Eq, vec![Datum::Long(10)]);
        let ranges = extract_row_id_ranges(&p).unwrap();
        assert_eq!(ranges, vec![RowRange::new(10, 10)]);
    }

    #[test]
    fn test_extract_gte_lte() {
        let p = Predicate::and(vec![
            row_id_leaf(PredicateOperator::GtEq, vec![Datum::Long(10)]),
            row_id_leaf(PredicateOperator::LtEq, vec![Datum::Long(20)]),
        ]);
        let ranges = extract_row_id_ranges(&p).unwrap();
        assert_eq!(ranges, vec![RowRange::new(10, 20)]);
    }

    #[test]
    fn test_extract_in() {
        let p = row_id_leaf(
            PredicateOperator::In,
            vec![Datum::Long(5), Datum::Long(10), Datum::Long(15)],
        );
        let ranges = extract_row_id_ranges(&p).unwrap();
        assert_eq!(
            ranges,
            vec![
                RowRange::new(5, 5),
                RowRange::new(10, 10),
                RowRange::new(15, 15),
            ]
        );
    }

    #[test]
    fn test_extract_none_for_non_row_id() {
        let p = data_leaf();
        assert!(extract_row_id_ranges(&p).is_none());
    }

    #[test]
    fn test_extract_and_mixed() {
        let p = Predicate::and(vec![
            row_id_leaf(PredicateOperator::GtEq, vec![Datum::Long(10)]),
            data_leaf(),
        ]);
        let ranges = extract_row_id_ranges(&p).unwrap();
        assert_eq!(ranges, vec![RowRange::new(10, i64::MAX)]);
    }

    #[test]
    fn test_ranges_represent_only_a_pure_row_id_conjunct() {
        assert!(ranges_represent_conjunct(&row_id_leaf(
            PredicateOperator::GtEq,
            vec![Datum::Long(10)]
        )));
        assert!(!ranges_represent_conjunct(&row_id_leaf(
            PredicateOperator::NotEq,
            vec![Datum::Long(10)]
        )));
        assert!(!ranges_represent_conjunct(&data_leaf()));
        assert!(!ranges_represent_conjunct(&Predicate::or(vec![
            Predicate::and(vec![
                row_id_leaf(PredicateOperator::Eq, vec![Datum::Long(1)]),
                data_leaf(),
            ]),
            data_leaf(),
        ])));
        assert!(ranges_represent_conjunct(&Predicate::or(vec![
            row_id_leaf(PredicateOperator::Eq, vec![Datum::Long(1)]),
            row_id_leaf(PredicateOperator::Eq, vec![Datum::Long(2)]),
        ])));
    }
}
