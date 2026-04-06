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

use crate::spec::{DataField, DataType, Datum, Predicate, PredicateOperator};
use std::cmp::Ordering;

pub(crate) trait StatsAccessor {
    fn row_count(&self) -> i64;
    fn null_count(&self, index: usize) -> Option<i64>;
    fn min_value(&self, index: usize, data_type: &DataType) -> Option<Datum>;
    fn max_value(&self, index: usize, data_type: &DataType) -> Option<Datum>;
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
        PredicateOperator::In | PredicateOperator::NotIn => {
            return true;
        }
        PredicateOperator::Eq
        | PredicateOperator::NotEq
        | PredicateOperator::Lt
        | PredicateOperator::LtEq
        | PredicateOperator::Gt
        | PredicateOperator::GtEq => {}
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
        PredicateOperator::Eq => {
            !matches!(literal.partial_cmp(&min_value), Some(Ordering::Less))
                && !matches!(literal.partial_cmp(&max_value), Some(Ordering::Greater))
        }
        PredicateOperator::NotEq => !(min_value == *literal && max_value == *literal),
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
        PredicateOperator::IsNull
        | PredicateOperator::IsNotNull
        | PredicateOperator::In
        | PredicateOperator::NotIn => true,
    }
}

pub(crate) fn missing_field_may_match(op: PredicateOperator, row_count: i64) -> bool {
    if row_count <= 0 {
        return false;
    }

    matches!(op, PredicateOperator::IsNull)
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
        Predicate::Or(_) | Predicate::Not(_) => true,
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
