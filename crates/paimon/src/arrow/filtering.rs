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
use crate::spec::{is_row_tracking_column, DataField, Predicate, PredicateOperator};

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
            // Row-tracking metadata has no logical-table position, so
            // mapping its placeholder index would collapse the leaf to a
            // constant. Keep it; the residual resolves it by name.
            if is_row_tracking_column(column) {
                return predicate.clone();
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{DataType, Datum, IntType, PredicateBuilder, PredicateOperator};

    #[test]
    fn test_a_row_id_leaf_survives_per_file_remapping() {
        let table_fields = vec![
            DataField::new(1, "added".to_string(), DataType::Int(IntType::new())),
            DataField::new(0, "base".to_string(), DataType::Int(IntType::new())),
        ];
        let file_fields = vec![table_fields[1].clone()];
        let leaf = crate::spec::row_id_leaf(PredicateOperator::NotEq, vec![Datum::Long(102)]);

        assert_eq!(
            remap_predicates_to_file(std::slice::from_ref(&leaf), &table_fields, &file_fields),
            vec![leaf]
        );
    }

    #[test]
    fn test_a_row_id_branch_can_die_during_per_file_remapping() {
        let table_fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "added".to_string(), DataType::Int(IntType::new())),
        ];
        let file_fields = vec![table_fields[0].clone()];
        let filter = Predicate::or(vec![
            PredicateBuilder::new(&table_fields)
                .is_null("added")
                .unwrap(),
            crate::spec::row_id_leaf(PredicateOperator::Eq, vec![Datum::Long(5)]),
        ]);

        assert_eq!(
            remap_predicates_to_file(&[filter], &table_fields, &file_fields),
            vec![Predicate::AlwaysTrue]
        );
    }
}
