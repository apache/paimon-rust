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

use crate::spec::{Datum, Predicate, PredicateOperator, ROW_ID_FIELD_NAME};
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
        } if column == ROW_ID_FIELD_NAME => leaf_to_ranges(*op, literals),
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
            // OR: union all _ROW_ID ranges (all children must have _ROW_ID predicates)
            let mut all_ranges: Vec<RowRange> = Vec::new();
            for child in children {
                let ranges = extract_row_id_ranges(child)?;
                all_ranges.extend(ranges);
            }
            if all_ranges.is_empty() {
                None
            } else {
                Some(super::merge_row_ranges(all_ranges))
            }
        }
        _ => None,
    }
}

/// Remove `_ROW_ID` predicates from a filter, returning the remaining filter.
/// Returns `None` if the entire filter is a `_ROW_ID` predicate.
pub(crate) fn remove_row_id_filter(predicate: &Predicate) -> Option<Predicate> {
    match predicate {
        Predicate::Leaf { column, .. } if column == ROW_ID_FIELD_NAME => None,
        Predicate::And(children) => {
            let filtered: Vec<Predicate> =
                children.iter().filter_map(remove_row_id_filter).collect();
            match filtered.len() {
                0 => None,
                1 => Some(filtered.into_iter().next().unwrap()),
                _ => Some(Predicate::and(filtered)),
            }
        }
        Predicate::Or(children) => {
            let filtered: Vec<Predicate> =
                children.iter().filter_map(remove_row_id_filter).collect();
            if filtered.len() != children.len() {
                // If any child was entirely _ROW_ID, the OR semantics change;
                // conservatively keep the whole OR.
                Some(predicate.clone())
            } else {
                Some(Predicate::or(filtered))
            }
        }
        other => Some(other.clone()),
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
            let mut ranges: Vec<RowRange> = literals
                .iter()
                .filter_map(|d| datum_to_i64(d).map(|v| RowRange::new(v, v)))
                .collect();
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
    use super::*;
    use crate::spec::{BigIntType, DataType};

    fn row_id_leaf(op: PredicateOperator, literals: Vec<Datum>) -> Predicate {
        Predicate::Leaf {
            column: ROW_ID_FIELD_NAME.to_string(),
            index: 0,
            data_type: DataType::BigInt(BigIntType::new()),
            op,
            literals,
        }
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
    fn test_remove_row_id_filter_leaf() {
        let p = row_id_leaf(PredicateOperator::Eq, vec![Datum::Long(10)]);
        assert!(remove_row_id_filter(&p).is_none());
    }

    #[test]
    fn test_remove_row_id_filter_and() {
        let p = Predicate::and(vec![
            row_id_leaf(PredicateOperator::GtEq, vec![Datum::Long(10)]),
            data_leaf(),
        ]);
        let result = remove_row_id_filter(&p).unwrap();
        assert_eq!(result, data_leaf());
    }

    #[test]
    fn test_remove_row_id_filter_keeps_non_row_id() {
        let p = data_leaf();
        assert_eq!(remove_row_id_filter(&p).unwrap(), data_leaf());
    }
}
