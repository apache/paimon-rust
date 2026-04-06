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

use crate::arrow::schema_evolution::create_index_mapping;
pub(crate) use crate::predicate_stats::{predicates_may_match_with_schema, StatsAccessor};
use crate::spec::{DataField, Predicate, PredicateOperator};

pub(crate) fn reader_pruning_predicates(data_predicates: Vec<Predicate>) -> Vec<Predicate> {
    data_predicates
        .into_iter()
        .filter(predicate_supported_for_reader_pruning)
        .collect()
}

pub(crate) fn build_field_mapping(
    table_fields: &[DataField],
    file_fields: &[DataField],
) -> Vec<Option<usize>> {
    normalize_field_mapping(
        create_index_mapping(table_fields, file_fields),
        table_fields.len(),
    )
}

fn predicate_supported_for_reader_pruning(predicate: &Predicate) -> bool {
    match predicate {
        Predicate::AlwaysFalse => true,
        Predicate::Leaf { op, .. } => {
            matches!(
                op,
                PredicateOperator::IsNull
                    | PredicateOperator::IsNotNull
                    | PredicateOperator::Eq
                    | PredicateOperator::NotEq
                    | PredicateOperator::Lt
                    | PredicateOperator::LtEq
                    | PredicateOperator::Gt
                    | PredicateOperator::GtEq
                    | PredicateOperator::In
                    | PredicateOperator::NotIn
            )
        }
        Predicate::AlwaysTrue | Predicate::And(_) | Predicate::Or(_) | Predicate::Not(_) => false,
    }
}

fn identity_field_mapping(num_fields: usize) -> Vec<Option<usize>> {
    (0..num_fields).map(Some).collect()
}

fn normalize_field_mapping(mapping: Option<Vec<i32>>, num_fields: usize) -> Vec<Option<usize>> {
    mapping
        .map(|field_mapping| {
            field_mapping
                .into_iter()
                .map(|index| usize::try_from(index).ok())
                .collect()
        })
        .unwrap_or_else(|| identity_field_mapping(num_fields))
}
