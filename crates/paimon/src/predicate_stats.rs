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

use crate::spec::{is_row_id_column, DataField, DataType, Datum, Predicate, PredicateOperator};
use std::cmp::Ordering;

pub(crate) trait StatsAccessor {
    fn row_count(&self) -> i64;
    fn null_count(&self, index: usize) -> Option<i64>;
    fn min_value(&self, index: usize, data_type: &DataType) -> Option<Datum>;
    fn max_value(&self, index: usize, data_type: &DataType) -> Option<Datum>;
    fn supports_in_min_max_pruning(&self) -> bool {
        false
    }
}

pub(crate) fn predicates_may_match_with_schema<T: StatsAccessor>(
    predicates: &[Predicate],
    stats: &T,
    field_mapping: &[Option<usize>],
    file_fields: &[DataField],
) -> bool {
    predicates.iter().all(|predicate| {
        predicate_may_match_with_schema(predicate, stats, field_mapping, file_fields)
    })
}

pub(crate) fn data_leaf_may_match<T: StatsAccessor>(
    index: usize,
    stats_data_type: &DataType,
    predicate_data_type: &DataType,
    op: PredicateOperator,
    literals: &[Datum],
    stats: &T,
) -> bool {
    let row_count = stats.row_count();
    if row_count <= 0 {
        return false;
    }

    let null_count = stats.null_count(index);
    let all_null = null_count.map(|count| count == row_count);

    match op {
        PredicateOperator::IsNull => {
            return null_count.is_none_or(|count| count > 0);
        }
        PredicateOperator::IsNotNull => {
            return all_null != Some(true);
        }
        PredicateOperator::In if stats.supports_in_min_max_pruning() => {}
        PredicateOperator::In => {
            return true;
        }
        PredicateOperator::ArrayContains => {
            return all_null != Some(true);
        }
        PredicateOperator::ArraysOverlap => {
            return !literals.is_empty() && all_null != Some(true);
        }
        PredicateOperator::ArrayContainsAll => {
            return all_null != Some(true);
        }
        PredicateOperator::EndsWith | PredicateOperator::Contains => {
            // String min/max ordering carries no information about suffix /
            // substring matches, so fail open.
            return true;
        }
        PredicateOperator::Between | PredicateOperator::NotBetween => {
            return between_may_match(
                index,
                stats_data_type,
                predicate_data_type,
                op,
                literals,
                stats,
                all_null,
            );
        }
        PredicateOperator::Eq
        | PredicateOperator::NotEq
        | PredicateOperator::NotIn
        | PredicateOperator::Lt
        | PredicateOperator::LtEq
        | PredicateOperator::Gt
        | PredicateOperator::GtEq
        | PredicateOperator::StartsWith
        | PredicateOperator::Like => {}
    }

    if all_null == Some(true) {
        return false;
    }

    let literal = match literals.first() {
        Some(literal) => literal,
        None => return true,
    };

    let min_value = match stats
        .min_value(index, stats_data_type)
        .and_then(|datum| coerce_stats_datum_for_predicate(datum, predicate_data_type))
    {
        Some(value) => value,
        None => return true,
    };
    let max_value = match stats
        .max_value(index, stats_data_type)
        .and_then(|datum| coerce_stats_datum_for_predicate(datum, predicate_data_type))
    {
        Some(value) => value,
        None => return true,
    };

    match op {
        PredicateOperator::In => {
            if !matches!(
                min_value.partial_cmp(&max_value),
                Some(Ordering::Less | Ordering::Equal)
            ) {
                return true;
            }
            literals.iter().any(|literal| {
                !matches!(literal.partial_cmp(&min_value), Some(Ordering::Less))
                    && !matches!(literal.partial_cmp(&max_value), Some(Ordering::Greater))
            })
        }
        PredicateOperator::Eq => {
            !matches!(literal.partial_cmp(&min_value), Some(Ordering::Less))
                && !matches!(literal.partial_cmp(&max_value), Some(Ordering::Greater))
        }
        // Skipping a file because every non-null value in it is forbidden needs
        // two things that do not hold for FLOAT and DOUBLE, so both operators
        // below fail open there (see `equality_exclusion_is_sound`).
        PredicateOperator::NotEq => {
            !(equality_exclusion_is_sound(&min_value)
                && min_value == *literal
                && max_value == *literal)
        }
        // The n-ary form of `NotEq` above: a file can only be skipped when some
        // literal equals both bounds. Java `NotIn#test` does the same
        // (`compareLiteral(lit, min) == 0 && compareLiteral(lit, max) == 0`). Its
        // extra `literal == null` arm has no counterpart because `Datum` has no
        // null variant at all: the REST parser folds `NOT IN (.., null)` into
        // `AlwaysFalse`, and DataFusion declines to push the predicate down. Files
        // that are entirely null were already skipped by the shared `all_null`
        // check, and files that are partly null are still pruned -- a null row
        // satisfies neither `NotIn` nor the equality that prunes it.
        PredicateOperator::NotIn => {
            !(equality_exclusion_is_sound(&min_value)
                && literals
                    .iter()
                    .any(|literal| min_value == *literal && max_value == *literal))
        }
        PredicateOperator::Lt => !matches!(
            min_value.partial_cmp(literal),
            Some(Ordering::Greater | Ordering::Equal)
        ),
        PredicateOperator::LtEq => {
            !matches!(min_value.partial_cmp(literal), Some(Ordering::Greater))
        }
        PredicateOperator::Gt => !matches!(
            max_value.partial_cmp(literal),
            Some(Ordering::Less | Ordering::Equal)
        ),
        PredicateOperator::GtEq => !matches!(max_value.partial_cmp(literal), Some(Ordering::Less)),
        PredicateOperator::StartsWith => {
            // pat lives in [min, max] iff max >= pat AND min < pat_next, where
            // pat_next is pat with its last codepoint incremented. If we can't
            // compute pat_next (last char is char::MAX, increments into the
            // UTF-16 surrogate range, etc.), fail open.
            let (pat, min_str, max_str) = match (literal, &min_value, &max_value) {
                (Datum::String(p), Datum::String(lo), Datum::String(hi)) => {
                    (p.as_str(), lo.as_str(), hi.as_str())
                }
                _ => return true,
            };
            // If the file's max is below the pattern (lexicographically), no
            // string in the file can start with `pat`.
            if max_str < pat {
                return false;
            }
            // Compute pat_next; if we can, use the [pat, pat_next) range to
            // also rule out files whose min is already past every pat-prefixed
            // string. Otherwise just trust the upper bound check.
            match next_string_for_prefix(pat) {
                Some(pat_next) => min_str.as_bytes() < pat_next.as_slice(),
                None => true,
            }
        }
        PredicateOperator::Like => {
            // Try to extract a literal prefix from the LIKE pattern (the
            // characters before the first unescaped wildcard). If we get one,
            // prune as if it were StartsWith; otherwise fail open.
            let (pattern, min_str, max_str) = match (literal, &min_value, &max_value) {
                (Datum::String(p), Datum::String(lo), Datum::String(hi)) => {
                    (p.as_str(), lo.as_str(), hi.as_str())
                }
                _ => return true,
            };
            let Some(pat) = like_pattern_literal_prefix(pattern) else {
                return true;
            };
            if pat.is_empty() {
                return true;
            }
            if max_str < pat.as_str() {
                return false;
            }
            match next_string_for_prefix(&pat) {
                Some(pat_next) => min_str.as_bytes() < pat_next.as_slice(),
                None => true,
            }
        }
        PredicateOperator::IsNull
        | PredicateOperator::IsNotNull
        | PredicateOperator::EndsWith
        | PredicateOperator::Contains
        | PredicateOperator::Between
        | PredicateOperator::NotBetween
        | PredicateOperator::ArrayContains
        | PredicateOperator::ArraysOverlap
        | PredicateOperator::ArrayContainsAll => true,
    }
}

/// Whether `min == max == literal` is enough to conclude that no row in a file
/// can satisfy `!= literal`.
///
/// The conclusion needs two properties, and FLOAT and DOUBLE have neither:
///
/// * `Datum` equality must agree with the row-level filter. `datum_cmp` compares
///   floats with IEEE `partial_cmp`, so it reports `-0.0 == +0.0`, while the
///   residual filter tells them apart. Java is unaffected here because
///   `CompareUtils#compareLiteral` goes through `Double.compareTo`, which orders
///   `-0.0` below `+0.0`.
/// * min and max must cover every non-null row. The writer leaves NaN out of
///   min/max without counting it as null, so a file holding `[1.0, NaN]` reports
///   `min == max == 1.0` over two rows.
///
/// Either one alone drops rows: on those two files `<> 0.0` and `<> 1.0` skipped
/// the file even though the filter would have returned the row.
fn equality_exclusion_is_sound(bound: &Datum) -> bool {
    !matches!(bound, Datum::Float(_) | Datum::Double(_))
}

pub(crate) fn data_leaf_must_match<T: StatsAccessor>(
    index: usize,
    stats_data_type: &DataType,
    predicate_data_type: &DataType,
    op: PredicateOperator,
    literals: &[Datum],
    stats: &T,
) -> bool {
    let row_count = stats.row_count();
    if row_count <= 0 {
        return false;
    }

    let null_count = stats.null_count(index);
    match op {
        PredicateOperator::IsNull => return null_count == Some(row_count),
        PredicateOperator::IsNotNull => return null_count == Some(0),
        _ => {
            if null_count != Some(0) {
                return false;
            }
        }
    }

    let min_value = match stats
        .min_value(index, stats_data_type)
        .and_then(|datum| coerce_stats_datum_for_predicate(datum, predicate_data_type))
    {
        Some(value) => value,
        None => return false,
    };
    let max_value = match stats
        .max_value(index, stats_data_type)
        .and_then(|datum| coerce_stats_datum_for_predicate(datum, predicate_data_type))
    {
        Some(value) => value,
        None => return false,
    };
    if !matches!(
        min_value.partial_cmp(&max_value),
        Some(Ordering::Less | Ordering::Equal)
    ) {
        return false;
    }

    match op {
        PredicateOperator::Eq => literals
            .first()
            .is_some_and(|literal| min_value == *literal && max_value == *literal),
        PredicateOperator::NotEq => literals.first().is_some_and(|literal| {
            matches!(literal.partial_cmp(&min_value), Some(Ordering::Less))
                || matches!(literal.partial_cmp(&max_value), Some(Ordering::Greater))
        }),
        PredicateOperator::Lt => literals
            .first()
            .is_some_and(|literal| matches!(max_value.partial_cmp(literal), Some(Ordering::Less))),
        PredicateOperator::LtEq => literals.first().is_some_and(|literal| {
            matches!(
                max_value.partial_cmp(literal),
                Some(Ordering::Less | Ordering::Equal)
            )
        }),
        PredicateOperator::Gt => literals.first().is_some_and(|literal| {
            matches!(min_value.partial_cmp(literal), Some(Ordering::Greater))
        }),
        PredicateOperator::GtEq => literals.first().is_some_and(|literal| {
            matches!(
                min_value.partial_cmp(literal),
                Some(Ordering::Greater | Ordering::Equal)
            )
        }),
        PredicateOperator::In => min_value == max_value && literals.contains(&min_value),
        PredicateOperator::Between => {
            let (Some(low), Some(high)) = (literals.first(), literals.get(1)) else {
                return false;
            };
            let min_ge_low = !matches!(min_value.partial_cmp(low), Some(Ordering::Less));
            let max_le_high = !matches!(max_value.partial_cmp(high), Some(Ordering::Greater));
            min_ge_low && max_le_high
        }
        PredicateOperator::NotBetween => {
            let (Some(low), Some(high)) = (literals.first(), literals.get(1)) else {
                return false;
            };
            let max_lt_low = matches!(max_value.partial_cmp(low), Some(Ordering::Less));
            let min_gt_high = matches!(min_value.partial_cmp(high), Some(Ordering::Greater));
            max_lt_low || min_gt_high
        }
        PredicateOperator::IsNull
        | PredicateOperator::IsNotNull
        | PredicateOperator::NotIn
        | PredicateOperator::StartsWith
        | PredicateOperator::EndsWith
        | PredicateOperator::Contains
        | PredicateOperator::Like
        | PredicateOperator::ArrayContains
        | PredicateOperator::ArraysOverlap
        | PredicateOperator::ArrayContainsAll => false,
    }
}

/// Return the literal prefix of a SQL LIKE pattern up to the first unescaped
/// `%` or `_`. A backslash escapes the next character (which is appended
/// literally, mirroring arrow's `like` kernel); a trailing backslash is a
/// literal backslash.
fn like_pattern_literal_prefix(pattern: &str) -> Option<String> {
    let mut out = String::with_capacity(pattern.len());
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '%' | '_' => return Some(out),
            '\\' => match chars.next() {
                Some(next) => out.push(next),
                None => out.push('\\'),
            },
            other => out.push(other),
        }
    }
    Some(out)
}

/// Compute the smallest string strictly greater than every string with `prefix`
/// as a prefix, by incrementing the last codepoint. Returns `None` if the last
/// codepoint cannot be incremented within valid Unicode (e.g. `char::MAX`).
fn next_string_for_prefix(prefix: &str) -> Option<Vec<u8>> {
    let last_char = prefix.chars().next_back()?;
    let mut next_code = last_char as u32 + 1;
    // Skip over the UTF-16 surrogate range, which is not valid scalar Unicode.
    if (0xD800..=0xDFFF).contains(&next_code) {
        next_code = 0xE000;
    }
    let next_char = char::from_u32(next_code)?;
    let mut bytes = prefix.as_bytes()[..prefix.len() - last_char.len_utf8()].to_vec();
    let mut buf = [0u8; 4];
    bytes.extend_from_slice(next_char.encode_utf8(&mut buf).as_bytes());
    Some(bytes)
}

pub(crate) fn missing_field_may_match(op: PredicateOperator, row_count: i64) -> bool {
    if row_count <= 0 {
        return false;
    }

    matches!(op, PredicateOperator::IsNull)
}

pub(crate) fn missing_field_must_match(op: PredicateOperator, row_count: i64) -> bool {
    row_count > 0 && matches!(op, PredicateOperator::IsNull)
}

/// Stats-prune `field BETWEEN low AND high` (and its negation) by treating it
/// as the conjunction `field >= low AND field <= high`:
/// * `Between` may match iff the file's `[min, max]` overlaps `[low, high]`.
/// * `NotBetween` may match iff some row could fall outside `[low, high]`,
///   i.e. unless the file's `[min, max]` is entirely inside `[low, high]`.
///
/// All-null files are pruned for both ops (NULL comparisons resolve to NULL,
/// which the evaluator treats as false).
fn between_may_match<T: StatsAccessor>(
    index: usize,
    stats_data_type: &DataType,
    predicate_data_type: &DataType,
    op: PredicateOperator,
    literals: &[Datum],
    stats: &T,
    all_null: Option<bool>,
) -> bool {
    if all_null == Some(true) {
        return false;
    }
    let (Some(low), Some(high)) = (literals.first(), literals.get(1)) else {
        return true;
    };
    let min_value = match stats
        .min_value(index, stats_data_type)
        .and_then(|datum| coerce_stats_datum_for_predicate(datum, predicate_data_type))
    {
        Some(value) => value,
        None => return true,
    };
    let max_value = match stats
        .max_value(index, stats_data_type)
        .and_then(|datum| coerce_stats_datum_for_predicate(datum, predicate_data_type))
    {
        Some(value) => value,
        None => return true,
    };

    let max_ge_low = !matches!(max_value.partial_cmp(low), Some(Ordering::Less));
    let min_le_high = !matches!(min_value.partial_cmp(high), Some(Ordering::Greater));
    let overlaps = max_ge_low && min_le_high;

    match op {
        PredicateOperator::Between => overlaps,
        PredicateOperator::NotBetween => {
            // Prune only when [min, max] is entirely inside [low, high].
            let min_ge_low = !matches!(min_value.partial_cmp(low), Some(Ordering::Less));
            let max_le_high = !matches!(max_value.partial_cmp(high), Some(Ordering::Greater));
            !(min_ge_low && max_le_high)
        }
        _ => unreachable!("between_may_match is only called for Between/NotBetween"),
    }
}

fn predicate_may_match_with_schema<T: StatsAccessor>(
    predicate: &Predicate,
    stats: &T,
    field_mapping: &[Option<usize>],
    file_fields: &[DataField],
) -> bool {
    match predicate {
        Predicate::AlwaysTrue => true,
        Predicate::AlwaysFalse => false,
        Predicate::And(children) => children
            .iter()
            .all(|child| predicate_may_match_with_schema(child, stats, field_mapping, file_fields)),
        Predicate::Or(children) => children
            .iter()
            .any(|child| predicate_may_match_with_schema(child, stats, field_mapping, file_fields)),
        Predicate::Not(inner) => {
            !predicate_must_match_with_schema(inner, stats, field_mapping, file_fields)
        }
        // `_ROW_ID` has no column stats, so never prune on it.
        Predicate::Leaf { column, .. } if is_row_id_column(column) => true,
        Predicate::Leaf {
            index,
            data_type,
            op,
            literals,
            ..
        } => match field_mapping.get(*index).copied().flatten() {
            Some(file_index) => {
                let Some(file_field) = file_fields.get(file_index) else {
                    return true;
                };
                data_leaf_may_match(
                    file_index,
                    file_field.data_type(),
                    data_type,
                    *op,
                    literals,
                    stats,
                )
            }
            None => missing_field_may_match(*op, stats.row_count()),
        },
    }
}

fn predicate_must_match_with_schema<T: StatsAccessor>(
    predicate: &Predicate,
    stats: &T,
    field_mapping: &[Option<usize>],
    file_fields: &[DataField],
) -> bool {
    match predicate {
        Predicate::AlwaysTrue => stats.row_count() > 0,
        Predicate::AlwaysFalse => false,
        Predicate::And(children) => children.iter().all(|child| {
            predicate_must_match_with_schema(child, stats, field_mapping, file_fields)
        }),
        Predicate::Or(children) => children.iter().any(|child| {
            predicate_must_match_with_schema(child, stats, field_mapping, file_fields)
        }),
        Predicate::Not(inner) => {
            !predicate_may_match_with_schema(inner, stats, field_mapping, file_fields)
        }
        // Stats cannot decide `_ROW_ID`, so it never provably matches.
        Predicate::Leaf { column, .. } if is_row_id_column(column) => false,
        Predicate::Leaf {
            index,
            data_type,
            op,
            literals,
            ..
        } => match field_mapping.get(*index).copied().flatten() {
            Some(file_index) => {
                let Some(file_field) = file_fields.get(file_index) else {
                    return false;
                };
                data_leaf_must_match(
                    file_index,
                    file_field.data_type(),
                    data_type,
                    *op,
                    literals,
                    stats,
                )
            }
            None => missing_field_must_match(*op, stats.row_count()),
        },
    }
}

fn coerce_stats_datum_for_predicate(datum: Datum, predicate_data_type: &DataType) -> Option<Datum> {
    match (datum, predicate_data_type) {
        (datum @ Datum::Bool(_), DataType::Boolean(_))
        | (datum @ Datum::TinyInt(_), DataType::TinyInt(_))
        | (datum @ Datum::SmallInt(_), DataType::SmallInt(_))
        | (datum @ Datum::Int(_), DataType::Int(_))
        | (datum @ Datum::Long(_), DataType::BigInt(_))
        | (datum @ Datum::Float(_), DataType::Float(_))
        | (datum @ Datum::Double(_), DataType::Double(_))
        | (datum @ Datum::String(_), DataType::VarChar(_))
        | (datum @ Datum::String(_), DataType::Char(_))
        | (datum @ Datum::Bytes(_), DataType::Binary(_))
        | (datum @ Datum::Bytes(_), DataType::VarBinary(_))
        | (datum @ Datum::Date(_), DataType::Date(_))
        | (datum @ Datum::Time(_), DataType::Time(_))
        | (datum @ Datum::Timestamp { .. }, DataType::Timestamp(_))
        | (datum @ Datum::LocalZonedTimestamp { .. }, DataType::LocalZonedTimestamp(_))
        | (datum @ Datum::Decimal { .. }, DataType::Decimal(_)) => Some(datum),
        (Datum::TinyInt(value), DataType::SmallInt(_)) => Some(Datum::SmallInt(value as i16)),
        (Datum::TinyInt(value), DataType::Int(_)) => Some(Datum::Int(value as i32)),
        (Datum::TinyInt(value), DataType::BigInt(_)) => Some(Datum::Long(value as i64)),
        (Datum::SmallInt(value), DataType::Int(_)) => Some(Datum::Int(value as i32)),
        (Datum::SmallInt(value), DataType::BigInt(_)) => Some(Datum::Long(value as i64)),
        (Datum::Int(value), DataType::BigInt(_)) => Some(Datum::Long(value as i64)),
        (Datum::Float(value), DataType::Double(_)) => Some(Datum::Double(value as f64)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{BinaryType, DoubleType, IntType, VarCharType};

    struct MockStats {
        row_count: i64,
        null_count: Option<i64>,
        min: Option<Datum>,
        max: Option<Datum>,
    }

    impl StatsAccessor for MockStats {
        fn row_count(&self) -> i64 {
            self.row_count
        }
        fn null_count(&self, _index: usize) -> Option<i64> {
            self.null_count
        }
        fn min_value(&self, _index: usize, _data_type: &DataType) -> Option<Datum> {
            self.min.clone()
        }
        fn max_value(&self, _index: usize, _data_type: &DataType) -> Option<Datum> {
            self.max.clone()
        }
    }

    fn varchar() -> DataType {
        DataType::VarChar(VarCharType::default())
    }

    fn string_stats(min: &str, max: &str) -> MockStats {
        MockStats {
            row_count: 10,
            null_count: Some(0),
            min: Some(Datum::String(min.to_string())),
            max: Some(Datum::String(max.to_string())),
        }
    }

    fn run(op: PredicateOperator, lit: &str, stats: &MockStats) -> bool {
        let dt = varchar();
        data_leaf_may_match(0, &dt, &dt, op, &[Datum::String(lit.to_string())], stats)
    }

    #[test]
    fn starts_with_prunes_when_max_below_pattern() {
        let stats = string_stats("aaa", "fooa");
        assert!(!run(PredicateOperator::StartsWith, "foob", &stats));
    }

    #[test]
    fn starts_with_prunes_when_min_past_pattern_range() {
        // [foo, fop) is the pat range. min "fop" is already past it.
        let stats = string_stats("fop", "zzz");
        assert!(!run(PredicateOperator::StartsWith, "foo", &stats));
    }

    #[test]
    fn starts_with_keeps_when_pattern_inside_range() {
        let stats = string_stats("aaa", "zzz");
        assert!(run(PredicateOperator::StartsWith, "foo", &stats));
    }

    #[test]
    fn starts_with_keeps_when_min_equals_pattern() {
        let stats = string_stats("foo", "foozzz");
        assert!(run(PredicateOperator::StartsWith, "foo", &stats));
    }

    #[test]
    fn starts_with_falls_open_when_stats_missing() {
        let stats = MockStats {
            row_count: 5,
            null_count: Some(0),
            min: None,
            max: None,
        };
        assert!(run(PredicateOperator::StartsWith, "foo", &stats));
    }

    #[test]
    fn ends_with_and_contains_fall_open() {
        let stats = string_stats("aaa", "zzz");
        assert!(run(PredicateOperator::EndsWith, "foo", &stats));
        assert!(run(PredicateOperator::Contains, "foo", &stats));
    }

    #[test]
    fn like_with_literal_prefix_prunes_like_starts_with() {
        // pattern "foo%" → prefix "foo"; max "fooa" already past pattern end?
        // No: max = "fooa" >= "foo" and min "aaa" < "fop". So this case keeps.
        let stats = string_stats("aaa", "fooa");
        assert!(run(PredicateOperator::Like, "foo%", &stats));
        // pattern "foo%": [foo, fop). file [zaa, zzz] — max < pat → prune.
        let stats = string_stats("zaa", "zzz");
        assert!(!run(PredicateOperator::Like, "foo%", &stats));
        // file [fop, zzz] — min already past prefix range → prune.
        let stats = string_stats("fop", "zzz");
        assert!(!run(PredicateOperator::Like, "foo%", &stats));
    }

    #[test]
    fn like_without_literal_prefix_falls_open() {
        let stats = string_stats("aaa", "ccc");
        // Leading wildcard → no prefix → fail open.
        assert!(run(PredicateOperator::Like, "%foo%", &stats));
        // Leading underscore → no prefix → fail open.
        assert!(run(PredicateOperator::Like, "_oo", &stats));
    }

    #[test]
    fn like_with_escaped_wildcard_in_prefix_is_decoded() {
        // "100\%foo" → literal prefix "100%foo".
        let stats = string_stats("100", "100%fzz");
        assert!(run(PredicateOperator::Like, r"100\%foo", &stats));
        let stats = string_stats("zzz0", "zzz9");
        assert!(!run(PredicateOperator::Like, r"100\%foo", &stats));
    }

    #[test]
    fn missing_field_returns_false_for_string_ops() {
        // Only IsNull is allowed when the field is missing.
        for op in [
            PredicateOperator::StartsWith,
            PredicateOperator::EndsWith,
            PredicateOperator::Contains,
            PredicateOperator::Like,
        ] {
            assert!(!missing_field_may_match(op, 5));
        }
    }

    // Sanity check: integer ops keep their existing semantics after the new
    // string variants are interleaved into the dispatcher.
    #[test]
    fn integer_eq_still_prunes_outside_range() {
        let dt = DataType::Int(IntType::new());
        let stats = MockStats {
            row_count: 10,
            null_count: Some(0),
            min: Some(Datum::Int(0)),
            max: Some(Datum::Int(100)),
        };
        assert!(!data_leaf_may_match(
            0,
            &dt,
            &dt,
            PredicateOperator::Eq,
            &[Datum::Int(500)],
            &stats,
        ));
    }

    #[test]
    fn binary_range_pruning_uses_unsigned_byte_order() {
        let dt = DataType::Binary(BinaryType::new(1).unwrap());
        let stats = MockStats {
            row_count: 10,
            null_count: Some(0),
            min: Some(Datum::Bytes(vec![0x80])),
            max: Some(Datum::Bytes(vec![0xFF])),
        };

        assert!(data_leaf_may_match(
            0,
            &dt,
            &dt,
            PredicateOperator::Gt,
            &[Datum::Bytes(vec![0x00])],
            &stats,
        ));
        assert!(!data_leaf_may_match(
            0,
            &dt,
            &dt,
            PredicateOperator::Lt,
            &[Datum::Bytes(vec![0x00])],
            &stats,
        ));
    }

    fn int_stats(min: i32, max: i32) -> MockStats {
        nullable_int_stats(Some(min), Some(max), Some(0))
    }

    fn nullable_int_stats(
        min: Option<i32>,
        max: Option<i32>,
        null_count: Option<i64>,
    ) -> MockStats {
        MockStats {
            row_count: 10,
            null_count,
            min: min.map(Datum::Int),
            max: max.map(Datum::Int),
        }
    }

    fn run_int(op: PredicateOperator, lits: &[Datum], stats: &MockStats) -> bool {
        let dt = DataType::Int(IntType::new());
        data_leaf_may_match(0, &dt, &dt, op, lits, stats)
    }

    #[test]
    fn in_falls_open_when_accessor_does_not_opt_in() {
        let stats = int_stats(10, 20);
        assert!(run_int(PredicateOperator::In, &[Datum::Int(30)], &stats));
    }

    /// Stage 3 invariant: a `Between` leaf and the equivalent `GtEq+LtEq`
    /// conjunction must produce identical stats-prune verdicts. If they
    /// diverge, the DataFusion translator switch (And-of-comparisons →
    /// Between leaf) silently changes pruning behavior in production.
    #[test]
    fn between_matches_gteq_lteq_conjunction() {
        let cases: &[(i32, i32, i32, i32, bool)] = &[
            // (min, max, low, high, expected_may_match)
            (0, 100, 50, 60, true),    // overlap inside
            (0, 100, 200, 300, false), // entirely above
            (0, 100, -50, -1, false),  // entirely below
            (0, 100, 100, 100, true),  // boundary high
            (0, 100, 0, 0, true),      // boundary low
            (50, 100, 0, 49, false),   // low < min < high < max impossible — fully below
            (50, 100, 0, 200, true),   // file fully inside [low, high]
        ];
        for &(min, max, low, high, expected) in cases {
            let stats = int_stats(min, max);
            let between = run_int(
                PredicateOperator::Between,
                &[Datum::Int(low), Datum::Int(high)],
                &stats,
            );
            let gteq = run_int(PredicateOperator::GtEq, &[Datum::Int(low)], &stats);
            let lteq = run_int(PredicateOperator::LtEq, &[Datum::Int(high)], &stats);
            assert_eq!(
                between,
                gteq && lteq,
                "Between vs GtEq+LtEq divergence at ({min},{max}) ∩ [{low},{high}]"
            );
            assert_eq!(
                between, expected,
                "Between unexpected at {min},{max} ∩ [{low},{high}]"
            );
        }
    }

    #[test]
    fn not_between_prunes_only_when_file_fully_inside_range() {
        // file [10, 20] ⊆ [0, 100] → all rows are within [0, 100], so NOT
        // BETWEEN can prune.
        let stats = int_stats(10, 20);
        assert!(!run_int(
            PredicateOperator::NotBetween,
            &[Datum::Int(0), Datum::Int(100)],
            &stats,
        ));
        // file [0, 100] ⊃ [10, 20] → some rows lie outside, can't prune.
        let stats = int_stats(0, 100);
        assert!(run_int(
            PredicateOperator::NotBetween,
            &[Datum::Int(10), Datum::Int(20)],
            &stats,
        ));
        // file disjoint with [50, 60] → all rows are outside, can't prune.
        let stats = int_stats(0, 10);
        assert!(run_int(
            PredicateOperator::NotBetween,
            &[Datum::Int(50), Datum::Int(60)],
            &stats,
        ));
    }

    #[test]
    fn not_predicate_prunes_only_when_inner_must_match() {
        let dt = DataType::Int(IntType::new());
        let fields = vec![DataField::new(0, "id".to_string(), dt.clone())];
        let mapping = vec![Some(0)];
        let stats = MockStats {
            row_count: 5,
            null_count: Some(0),
            min: Some(Datum::Int(10)),
            max: Some(Datum::Int(10)),
        };
        let predicate = Predicate::negate(Predicate::Leaf {
            column: "id".to_string(),
            index: 0,
            data_type: dt,
            op: PredicateOperator::Eq,
            literals: vec![Datum::Int(10)],
        });

        assert!(!predicates_may_match_with_schema(
            &[predicate],
            &stats,
            &mapping,
            &fields,
        ));
    }

    #[test]
    fn not_predicate_fails_open_when_nulls_or_stats_make_inner_uncertain() {
        let dt = DataType::Int(IntType::new());
        let fields = vec![DataField::new(0, "id".to_string(), dt.clone())];
        let mapping = vec![Some(0)];
        let predicate = Predicate::negate(Predicate::Leaf {
            column: "id".to_string(),
            index: 0,
            data_type: dt,
            op: PredicateOperator::Gt,
            literals: vec![Datum::Int(5)],
        });

        let with_nulls = MockStats {
            row_count: 5,
            null_count: Some(1),
            min: Some(Datum::Int(10)),
            max: Some(Datum::Int(20)),
        };
        assert!(predicates_may_match_with_schema(
            std::slice::from_ref(&predicate),
            &with_nulls,
            &mapping,
            &fields,
        ));

        let missing_stats = MockStats {
            row_count: 5,
            null_count: Some(0),
            min: None,
            max: Some(Datum::Int(20)),
        };
        assert!(predicates_may_match_with_schema(
            &[predicate],
            &missing_stats,
            &mapping,
            &fields,
        ));
    }

    #[test]
    fn not_predicate_prunes_inner_range_that_must_match() {
        let dt = DataType::Int(IntType::new());
        let fields = vec![DataField::new(0, "id".to_string(), dt.clone())];
        let mapping = vec![Some(0)];
        let stats = int_stats(10, 20);
        let predicate = Predicate::negate(Predicate::Leaf {
            column: "id".to_string(),
            index: 0,
            data_type: dt,
            op: PredicateOperator::Between,
            literals: vec![Datum::Int(0), Datum::Int(100)],
        });

        assert!(!predicates_may_match_with_schema(
            &[predicate],
            &stats,
            &mapping,
            &fields,
        ));
    }

    #[test]
    fn between_with_all_null_file_is_pruned() {
        let dt = DataType::Int(IntType::new());
        let stats = MockStats {
            row_count: 10,
            null_count: Some(10),
            min: None,
            max: None,
        };
        assert!(!data_leaf_may_match(
            0,
            &dt,
            &dt,
            PredicateOperator::Between,
            &[Datum::Int(0), Datum::Int(100)],
            &stats,
        ));
        assert!(!data_leaf_may_match(
            0,
            &dt,
            &dt,
            PredicateOperator::NotBetween,
            &[Datum::Int(0), Datum::Int(100)],
            &stats,
        ));
    }

    #[test]
    fn array_contains_prunes_all_null_file_like_java() {
        let dt = DataType::Array(crate::spec::ArrayType::new(DataType::Int(IntType::new())));
        let stats = MockStats {
            row_count: 10,
            null_count: Some(10),
            min: None,
            max: None,
        };
        assert!(!data_leaf_may_match(
            0,
            &dt,
            &dt,
            PredicateOperator::ArrayContains,
            &[Datum::Int(1)],
            &stats,
        ));
    }

    #[test]
    fn between_falls_open_when_stats_missing() {
        let dt = DataType::Int(IntType::new());
        let stats = MockStats {
            row_count: 5,
            null_count: Some(0),
            min: None,
            max: None,
        };
        assert!(data_leaf_may_match(
            0,
            &dt,
            &dt,
            PredicateOperator::Between,
            &[Datum::Int(0), Datum::Int(100)],
            &stats,
        ));
    }

    #[test]
    fn not_in_prunes_a_file_holding_only_a_forbidden_value() {
        let stats = nullable_int_stats(Some(7), Some(7), Some(0));
        assert!(!run_int(PredicateOperator::NotIn, &[Datum::Int(7)], &stats));
        // One matching literal is enough, wherever it sits in the list.
        assert!(!run_int(
            PredicateOperator::NotIn,
            &[Datum::Int(1), Datum::Int(7), Datum::Int(9)],
            &stats
        ));
    }

    #[test]
    fn not_in_keeps_a_file_whose_bounds_differ() {
        let stats = nullable_int_stats(Some(10), Some(20), Some(0));
        assert!(run_int(
            PredicateOperator::NotIn,
            &[Datum::Int(10), Datum::Int(20)],
            &stats
        ));
        assert!(run_int(PredicateOperator::NotIn, &[Datum::Int(15)], &stats));
    }

    #[test]
    fn not_in_prunes_an_all_null_file() {
        // Every row is null, and a null row never satisfies NOT IN.
        let stats = MockStats {
            row_count: 10,
            null_count: Some(10),
            min: None,
            max: None,
        };
        assert!(!run_int(PredicateOperator::NotIn, &[Datum::Int(7)], &stats));
    }

    #[test]
    fn not_in_prunes_a_partially_null_constant_file() {
        // Nulls do not rescue the file: the non-null rows all hold 7, and the
        // null rows do not satisfy NOT IN either.
        let stats = nullable_int_stats(Some(7), Some(7), Some(4));
        assert!(!run_int(PredicateOperator::NotIn, &[Datum::Int(7)], &stats));
        // Same when the null count is unknown: min/max already describe every
        // non-null row, so an unknown number of nulls cannot rescue the file.
        let unknown_nulls = nullable_int_stats(Some(7), Some(7), None);
        assert!(!run_int(
            PredicateOperator::NotIn,
            &[Datum::Int(7)],
            &unknown_nulls
        ));
    }

    #[test]
    fn not_in_falls_open_without_usable_stats() {
        assert!(run_int(
            PredicateOperator::NotIn,
            &[Datum::Int(7)],
            &nullable_int_stats(None, None, Some(0))
        ));
        assert!(run_int(
            PredicateOperator::NotIn,
            &[Datum::Int(7)],
            &nullable_int_stats(Some(7), None, Some(0))
        ));
        // Inverted bounds: no literal can equal two different bounds, so the
        // rule structurally cannot fire -- unlike `In`, which needs an explicit
        // guard here.
        assert!(run_int(
            PredicateOperator::NotIn,
            &[Datum::Int(7)],
            &nullable_int_stats(Some(20), Some(10), Some(0))
        ));
        // No literals at all — Java's NotIn returns true here as well.
        assert!(run_int(
            PredicateOperator::NotIn,
            &[],
            &nullable_int_stats(Some(7), Some(7), Some(0))
        ));
    }

    /// `x NOT IN (l)` is `x <> l`, so the two must always return the same
    /// verdict. Changing one rule without the other would let the same file be
    /// pruned by one spelling and kept by the other.
    #[test]
    fn not_in_with_one_literal_agrees_with_not_eq() {
        for (min, max, null_count) in [
            (Some(7), Some(7), Some(0)),
            (Some(10), Some(20), Some(0)),
            (Some(7), Some(7), Some(4)),
            (None, None, Some(0)),
            (Some(20), Some(10), Some(0)),
            (Some(7), Some(7), None),
            // All-null is the one shape where Rust deliberately parts from Java:
            // `NotEqual` there takes no null count, while the shared check here
            // prunes. Both operators must still agree with each other.
            (None, None, Some(10)),
        ] {
            let stats = nullable_int_stats(min, max, null_count);
            for literal in [7, 10, 20, 15] {
                assert_eq!(
                    run_int(PredicateOperator::NotIn, &[Datum::Int(literal)], &stats),
                    run_int(PredicateOperator::NotEq, &[Datum::Int(literal)], &stats),
                    "NotIn and NotEq disagree on ({min:?}, {max:?}, \
                     null_count={null_count:?}, literal={literal})"
                );
            }
        }
    }

    fn double_stats(min: f64, max: f64) -> MockStats {
        MockStats {
            row_count: 2,
            null_count: Some(0),
            min: Some(Datum::Double(min)),
            max: Some(Datum::Double(max)),
        }
    }

    fn run_double(op: PredicateOperator, literals: &[f64], stats: &MockStats) -> bool {
        let dt = DataType::Double(DoubleType::new());
        let literals: Vec<Datum> = literals.iter().copied().map(Datum::Double).collect();
        data_leaf_may_match(0, &dt, &dt, op, &literals, stats)
    }

    /// FLOAT and DOUBLE fail open, because neither property the rule rests on
    /// holds for them. Both shapes below are what the writer really produces: a
    /// file holding `[1.0, NaN]` reports `min == max == 1.0` over two rows, and a
    /// file holding only `-0.0` reports `min = -0.0, max = 0.0`.
    #[test]
    fn equality_exclusion_falls_open_for_floats() {
        let nan_file = double_stats(1.0, 1.0);
        assert!(run_double(PredicateOperator::NotEq, &[1.0], &nan_file));
        assert!(run_double(PredicateOperator::NotIn, &[1.0, 2.0], &nan_file));

        let signed_zero_file = double_stats(-0.0, 0.0);
        assert!(run_double(
            PredicateOperator::NotEq,
            &[0.0],
            &signed_zero_file
        ));
        assert!(run_double(
            PredicateOperator::NotIn,
            &[0.0],
            &signed_zero_file
        ));

        // Ordering-based rules are unaffected and must keep pruning.
        assert!(!run_double(PredicateOperator::Gt, &[5.0], &nan_file));
        assert!(!run_double(PredicateOperator::Lt, &[0.5], &nan_file));
        // So is `Eq`, whose direction of error is to keep files.
        assert!(run_double(PredicateOperator::Eq, &[1.0], &nan_file));
    }

    /// The equivalence between `NOT IN (l)` and `<> l` has to survive the float
    /// carve-out: both fail open together, rather than one of them pruning.
    #[test]
    fn not_in_agrees_with_not_eq_on_floats_too() {
        for (min, max) in [(1.0, 1.0), (-0.0, 0.0), (0.0, 0.0), (1.0, 5.0)] {
            let stats = double_stats(min, max);
            for literal in [1.0, 0.0, -0.0, 5.0] {
                assert_eq!(
                    run_double(PredicateOperator::NotIn, &[literal], &stats),
                    run_double(PredicateOperator::NotEq, &[literal], &stats),
                    "NotIn and NotEq disagree on ({min}, {max}, literal={literal})"
                );
            }
        }
    }
}
