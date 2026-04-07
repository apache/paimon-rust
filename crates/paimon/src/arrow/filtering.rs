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

/// Remap predicates from table-level indices to file-level indices.
/// Predicates referencing fields not present in the file are resolved based on
/// NULL semantics: the missing column is treated as all-NULL, so `IS NULL`
/// becomes `AlwaysTrue` and all other operators become `AlwaysFalse`.
pub(crate) fn remap_predicates_to_file(
    predicates: &[Predicate],
    table_fields: &[DataField],
    file_fields: &[DataField],
) -> Vec<Predicate> {
    let mapping = build_field_mapping(table_fields, file_fields);
    predicates
        .iter()
        .map(|p| remap_predicate(p, &mapping))
        .collect()
}

fn remap_predicate(predicate: &Predicate, mapping: &[Option<usize>]) -> Predicate {
    match predicate {
        Predicate::Leaf {
            column,
            index,
            data_type,
            op,
            literals,
        } => {
            match mapping.get(*index).copied().flatten() {
                Some(file_index) => Predicate::Leaf {
                    column: column.clone(),
                    index: file_index,
                    data_type: data_type.clone(),
                    op: *op,
                    literals: literals.clone(),
                },
                // Column missing from file → all values are NULL.
                None => match op {
                    PredicateOperator::IsNull => Predicate::AlwaysTrue,
                    _ => Predicate::AlwaysFalse,
                },
            }
        }
        Predicate::And(children) => {
            let remapped: Vec<_> = children
                .iter()
                .map(|c| remap_predicate(c, mapping))
                .collect();
            if remapped.iter().any(|p| matches!(p, Predicate::AlwaysFalse)) {
                Predicate::AlwaysFalse
            } else {
                let filtered: Vec<_> = remapped
                    .into_iter()
                    .filter(|p| !matches!(p, Predicate::AlwaysTrue))
                    .collect();
                match filtered.len() {
                    0 => Predicate::AlwaysTrue,
                    1 => filtered.into_iter().next().unwrap(),
                    _ => Predicate::and(filtered),
                }
            }
        }
        Predicate::Or(children) => {
            let remapped: Vec<_> = children
                .iter()
                .map(|c| remap_predicate(c, mapping))
                .collect();
            if remapped.iter().any(|p| matches!(p, Predicate::AlwaysTrue)) {
                Predicate::AlwaysTrue
            } else {
                let filtered: Vec<_> = remapped
                    .into_iter()
                    .filter(|p| !matches!(p, Predicate::AlwaysFalse))
                    .collect();
                match filtered.len() {
                    0 => Predicate::AlwaysFalse,
                    1 => filtered.into_iter().next().unwrap(),
                    _ => Predicate::or(filtered),
                }
            }
        }
        Predicate::Not(inner) => {
            let remapped = remap_predicate(inner, mapping);
            match remapped {
                Predicate::AlwaysTrue => Predicate::AlwaysFalse,
                Predicate::AlwaysFalse => Predicate::AlwaysTrue,
                other => Predicate::Not(Box::new(other)),
            }
        }
        Predicate::AlwaysTrue => Predicate::AlwaysTrue,
        Predicate::AlwaysFalse => Predicate::AlwaysFalse,
    }
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
