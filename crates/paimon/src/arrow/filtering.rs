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

use crate::arrow::schema_evolution::{create_index_mapping, same_type_ignoring_nullability};
pub(crate) use crate::predicate_stats::{predicates_may_match_with_schema, StatsAccessor};
use crate::spec::{is_row_id_column, DataField, Predicate, PredicateOperator};

/// Remap predicates from table-level indices to file-level indices.
/// Predicates referencing fields not present in the file are resolved based on
/// NULL semantics: the missing column is treated as all-NULL, so `IS NULL`
/// becomes `AlwaysTrue` and all other operators become `AlwaysFalse`.
/// Predicates on a field whose physical file type differs from the current
/// logical type are omitted unless their literals can be safely devolved. This
/// implementation deliberately does not devolve literals yet.
pub(crate) struct FilePredicateRemap {
    /// Predicates that are safe to evaluate against the physical file schema.
    pub(crate) predicates: Vec<Predicate>,
    /// Whether any predicate was omitted because it references a type-evolved
    /// field. The caller must evaluate the original current-schema predicates
    /// after converting the decoded batch when this is true.
    pub(crate) requires_current_schema_residual: bool,
}

/// Result of remapping one predicate subtree.
///
/// `predicate` may contain a safe weakening for positive filtering contexts.
/// `exact` is false when any descendant was omitted, which prevents that
/// weakening from being inverted under `NOT`.
struct RemapResult {
    predicate: Option<Predicate>,
    exact: bool,
}

impl RemapResult {
    fn exact(predicate: Predicate) -> Self {
        Self {
            predicate: Some(predicate),
            exact: true,
        }
    }

    fn omitted() -> Self {
        Self {
            predicate: None,
            exact: false,
        }
    }
}

pub(crate) fn remap_predicates_to_file(
    predicates: &[Predicate],
    table_fields: &[DataField],
    file_fields: &[DataField],
) -> FilePredicateRemap {
    let mapping = build_field_mapping(table_fields, file_fields);
    let mut requires_current_schema_residual = false;
    let predicates = predicates
        .iter()
        .filter_map(|predicate| {
            remap_predicate(
                predicate,
                &mapping,
                table_fields,
                file_fields,
                &mut requires_current_schema_residual,
            )
            .predicate
        })
        .collect();
    FilePredicateRemap {
        predicates,
        requires_current_schema_residual,
    }
}

fn remap_predicate(
    predicate: &Predicate,
    mapping: &[Option<usize>],
    table_fields: &[DataField],
    file_fields: &[DataField],
    requires_current_schema_residual: &mut bool,
) -> RemapResult {
    match predicate {
        Predicate::Leaf {
            column,
            index,
            op,
            literals,
            ..
        } => {
            // `_ROW_ID` is not a file column and has no per-file position, so
            // mapping its placeholder index would collapse the leaf to a
            // constant. Keep it; the residual resolves it by name.
            if is_row_id_column(column) {
                return RemapResult::exact(predicate.clone());
            }
            match mapping.get(*index).copied().flatten() {
                Some(file_index) => {
                    let Some(table_field) = table_fields.get(*index) else {
                        return RemapResult::omitted();
                    };
                    let Some(file_field) = file_fields.get(file_index) else {
                        return RemapResult::omitted();
                    };
                    if !same_type_ignoring_nullability(
                        table_field.data_type(),
                        file_field.data_type(),
                    ) {
                        *requires_current_schema_residual = true;
                        return RemapResult::omitted();
                    }
                    RemapResult::exact(Predicate::Leaf {
                        column: file_field.name().to_string(),
                        index: file_index,
                        data_type: file_field.data_type().clone(),
                        op: *op,
                        literals: literals.clone(),
                    })
                }
                // Column missing from file → all values are NULL.
                None => match op {
                    PredicateOperator::IsNull => RemapResult::exact(Predicate::AlwaysTrue),
                    _ => RemapResult::exact(Predicate::AlwaysFalse),
                },
            }
        }
        Predicate::And(children) => {
            let remapped: Vec<_> = children
                .iter()
                .map(|child| {
                    remap_predicate(
                        child,
                        mapping,
                        table_fields,
                        file_fields,
                        requires_current_schema_residual,
                    )
                })
                .collect();
            let exact = remapped.iter().all(|result| result.exact);
            let remapped: Vec<_> = remapped
                .into_iter()
                .filter_map(|result| result.predicate)
                .collect();
            let predicate = if remapped
                .iter()
                .any(|predicate| matches!(predicate, Predicate::AlwaysFalse))
            {
                Some(Predicate::AlwaysFalse)
            } else {
                let filtered: Vec<_> = remapped
                    .into_iter()
                    .filter(|predicate| !matches!(predicate, Predicate::AlwaysTrue))
                    .collect();
                match filtered.len() {
                    0 => exact.then_some(Predicate::AlwaysTrue),
                    1 => filtered.into_iter().next(),
                    _ => Some(Predicate::and(filtered)),
                }
            };
            RemapResult { predicate, exact }
        }
        Predicate::Or(children) => {
            let remapped: Vec<_> = children
                .iter()
                .map(|child| {
                    remap_predicate(
                        child,
                        mapping,
                        table_fields,
                        file_fields,
                        requires_current_schema_residual,
                    )
                })
                .collect();
            let exact = remapped.iter().all(|result| result.exact);
            if remapped.iter().any(|result| result.predicate.is_none()) {
                return RemapResult {
                    predicate: None,
                    exact,
                };
            }
            let remapped: Vec<_> = remapped
                .into_iter()
                .filter_map(|result| result.predicate)
                .collect();
            let predicate = if remapped
                .iter()
                .any(|predicate| matches!(predicate, Predicate::AlwaysTrue))
            {
                Some(Predicate::AlwaysTrue)
            } else {
                let filtered: Vec<_> = remapped
                    .into_iter()
                    .filter(|predicate| !matches!(predicate, Predicate::AlwaysFalse))
                    .collect();
                match filtered.len() {
                    0 => Some(Predicate::AlwaysFalse),
                    1 => filtered.into_iter().next(),
                    _ => Some(Predicate::or(filtered)),
                }
            };
            RemapResult { predicate, exact }
        }
        Predicate::Not(inner) => {
            let remapped = remap_predicate(
                inner,
                mapping,
                table_fields,
                file_fields,
                requires_current_schema_residual,
            );
            if !remapped.exact {
                return RemapResult::omitted();
            }
            match remapped.predicate {
                Some(predicate) => RemapResult::exact(Predicate::negate(predicate)),
                None => RemapResult::omitted(),
            }
        }
        Predicate::AlwaysTrue => RemapResult::exact(Predicate::AlwaysTrue),
        Predicate::AlwaysFalse => RemapResult::exact(Predicate::AlwaysFalse),
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
    use crate::spec::{BigIntType, DataType, Datum, IntType, PredicateBuilder, PredicateOperator};

    fn fields(value_type: DataType) -> Vec<DataField> {
        vec![
            DataField::new(1, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(2, "value".to_string(), value_type),
        ]
    }

    #[test]
    fn test_type_evolved_leaf_is_not_passed_to_file_reader() {
        let table_fields = fields(DataType::BigInt(BigIntType::new()));
        let file_fields = fields(DataType::Int(IntType::new()));
        let predicate = PredicateBuilder::new(&table_fields)
            .equal("value", Datum::Long(7))
            .unwrap();

        let remapped = remap_predicates_to_file(&[predicate], &table_fields, &file_fields);
        assert!(remapped.predicates.is_empty());
        assert!(remapped.requires_current_schema_residual);
    }

    #[test]
    fn test_and_keeps_safe_conjunct_but_or_fails_open() {
        let table_fields = fields(DataType::BigInt(BigIntType::new()));
        let file_fields = fields(DataType::Int(IntType::new()));
        let builder = PredicateBuilder::new(&table_fields);
        let stable = builder.equal("id", Datum::Int(1)).unwrap();
        let evolved = builder.equal("value", Datum::Long(7)).unwrap();

        let remapped = remap_predicates_to_file(
            &[Predicate::and(vec![stable.clone(), evolved.clone()])],
            &table_fields,
            &file_fields,
        );
        assert_eq!(remapped.predicates.len(), 1);
        assert!(matches!(
            remapped.predicates[0],
            Predicate::Leaf { index: 0, .. }
        ));
        assert!(remapped.requires_current_schema_residual);

        let remapped = remap_predicates_to_file(
            &[Predicate::or(vec![stable, evolved])],
            &table_fields,
            &file_fields,
        );
        assert!(remapped.predicates.is_empty());
        assert!(remapped.requires_current_schema_residual);
    }

    #[test]
    fn test_not_fails_open_when_descendant_cannot_be_remapped() {
        let table_fields = fields(DataType::BigInt(BigIntType::new()));
        let file_fields = fields(DataType::Int(IntType::new()));
        let builder = PredicateBuilder::new(&table_fields);
        let stable = builder.equal("id", Datum::Int(1)).unwrap();
        let evolved = builder.equal("value", Datum::Long(7)).unwrap();
        let predicate = Predicate::negate(Predicate::and(vec![stable, evolved]));

        let remapped = remap_predicates_to_file(&[predicate], &table_fields, &file_fields);

        assert!(remapped.predicates.is_empty());
        assert!(remapped.requires_current_schema_residual);
    }

    #[test]
    fn test_not_preserves_exact_true_from_missing_columns() {
        let table_fields = vec![
            DataField::new(1, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(2, "new1".to_string(), DataType::Int(IntType::new())),
            DataField::new(3, "new2".to_string(), DataType::Int(IntType::new())),
        ];
        let file_fields = vec![table_fields[0].clone()];
        let builder = PredicateBuilder::new(&table_fields);
        let predicate = Predicate::negate(Predicate::and(vec![
            builder.is_null("new1").unwrap(),
            builder.is_null("new2").unwrap(),
        ]));

        let remapped = remap_predicates_to_file(&[predicate], &table_fields, &file_fields);

        assert_eq!(remapped.predicates, vec![Predicate::AlwaysFalse]);
        assert!(!remapped.requires_current_schema_residual);
    }

    #[test]
    fn test_a_row_id_leaf_survives_per_file_remapping() {
        let table_fields = vec![
            DataField::new(1, "added".to_string(), DataType::Int(IntType::new())),
            DataField::new(0, "base".to_string(), DataType::Int(IntType::new())),
        ];
        let file_fields = vec![table_fields[1].clone()];
        let leaf = crate::spec::row_id_leaf(PredicateOperator::NotEq, vec![Datum::Long(102)]);

        let remapped =
            remap_predicates_to_file(std::slice::from_ref(&leaf), &table_fields, &file_fields);
        assert_eq!(remapped.predicates, vec![leaf]);
        assert!(!remapped.requires_current_schema_residual);
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

        let remapped = remap_predicates_to_file(&[filter], &table_fields, &file_fields);
        assert_eq!(remapped.predicates, vec![Predicate::AlwaysTrue]);
        assert!(!remapped.requires_current_schema_residual);
    }
}
