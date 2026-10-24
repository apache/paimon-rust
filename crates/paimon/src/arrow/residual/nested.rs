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

//! Exact nested ROW predicates. Metadata pruning must never interpret the
//! enclosing ROW's statistics as statistics of a descendant.

use crate::spec::{DataField, DataType, Datum, PredicateOperator};
use crate::{Error, Result};
use arrow_array::{
    make_array, new_null_array, Array, ArrayRef, BooleanArray, RecordBatch, StructArray,
};
use arrow_buffer::NullBuffer;

fn invalid(message: impl Into<String>) -> Error {
    Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

pub(super) fn evaluate(
    batch: &RecordBatch,
    root: &DataField,
    path: &[DataField],
    op: PredicateOperator,
    literals: &[Datum],
    file_fields: &[DataField],
    scan_fields: &[DataField],
) -> Result<BooleanArray> {
    let leaf = path
        .last()
        .ok_or_else(|| invalid("Nested field path must not be empty"))?;
    let target = crate::arrow::paimon_type_to_arrow(leaf.data_type())?;
    let source = scan_fields
        .iter()
        .chain(file_fields)
        .find(|f| f.id() == root.id());
    let position = batch
        .schema()
        .fields()
        .iter()
        .position(|f| crate::arrow::arrow_field_id(f) == Some(root.id()))
        .or_else(|| source.and_then(|f| batch.schema().index_of(f.name()).ok()));
    let values = if let Some(position) = position {
        descend(
            batch.column(position).clone(),
            source.unwrap_or(root).data_type(),
            path,
            &target,
        )?
    } else if file_fields.iter().any(|f| f.id() == root.id()) {
        return Err(invalid(format!(
            "Nested predicate root '{}' is missing from the scanned batch",
            root.name()
        )));
    } else {
        new_null_array(&target, batch.num_rows())
    };
    let values = if matches!(op, PredicateOperator::IsNull | PredicateOperator::IsNotNull)
        || values.data_type() == &target
    {
        values
    } else {
        arrow_cast::cast_with_options(
            values.as_ref(),
            &target,
            &arrow_cast::CastOptions {
                safe: false,
                ..Default::default()
            },
        )
        .map_err(|e| {
            invalid(format!(
                "Cannot evolve nested predicate field '{}': {e}",
                leaf.name()
            ))
        })?
    };
    super::evaluate_exact_leaf_predicate(&values, leaf.data_type(), op, literals)
        .map_err(|e| invalid(format!("Cannot evaluate nested predicate: {e}")))
}

fn descend<'a>(
    mut values: ArrayRef,
    mut source_type: &'a DataType,
    path: &'a [DataField],
    target: &arrow_schema::DataType,
) -> Result<ArrayRef> {
    let mut nulls = None;
    let len = values.len();
    for expected in path {
        let row = values
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| invalid("Nested predicate requires ROW input"))?;
        nulls = NullBuffer::union(nulls.as_ref(), row.nulls());
        let DataType::Row(logical) = source_type else {
            return Err(invalid("Nested predicate source has a non-ROW type"));
        };
        let source = logical.fields().iter().find(|f| f.id() == expected.id());
        let metadata = row
            .fields()
            .iter()
            .any(|f| crate::arrow::arrow_field_id(f).is_some());
        let index = if metadata {
            row.fields()
                .iter()
                .position(|f| crate::arrow::arrow_field_id(f) == Some(expected.id()))
        } else {
            source.and_then(|f| row.fields().iter().position(|a| a.name() == f.name()))
        };
        let Some(index) = index else {
            if source.is_some() {
                return Err(invalid(format!(
                    "Nested predicate field '{}' is missing from the scanned ROW",
                    expected.name()
                )));
            }
            return Ok(new_null_array(target, len));
        };
        let next = row.column(index).clone();
        source_type = source.map_or(expected.data_type(), DataField::data_type);
        values = next;
    }
    let nulls = NullBuffer::union(nulls.as_ref(), values.nulls());
    let data = values
        .to_data()
        .into_builder()
        .nulls(nulls)
        .build()
        .map_err(|e| invalid(e.to_string()))?;
    Ok(make_array(data))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::format::FilePredicates;
    use crate::arrow::residual::{evaluate_predicate_mask, widen_scan_fields};
    use crate::spec::{
        BigIntType, BinaryRowBuilder, IntType, Predicate, PredicateBuilder, RowType,
    };
    use arrow_array::{Int32Array, Int64Array};
    use arrow_schema::{DataType as ArrowType, Field, Schema};
    use std::sync::Arc;

    fn int(id: i32, name: &str) -> DataField {
        DataField::new(id, name.into(), DataType::Int(IntType::new()))
    }

    fn row(id: i32, name: &str, fields: Vec<DataField>) -> DataField {
        DataField::new(id, name.into(), DataType::Row(RowType::new(fields)))
    }

    fn schema() -> Vec<DataField> {
        vec![row(
            10,
            "root",
            vec![row(20, "detail", vec![int(30, "value")])],
        )]
    }

    fn predicate(fields: &[DataField], op: PredicateOperator, literals: Vec<Datum>) -> Predicate {
        let builder = PredicateBuilder::new(fields);
        let field = builder.nested_field("root", &["detail", "value"]).unwrap();
        match op {
            PredicateOperator::Eq => builder.equal(&field, literals[0].clone()),
            PredicateOperator::IsNull => builder.is_null(&field),
            PredicateOperator::In => builder.is_in(&field, literals),
            PredicateOperator::NotIn => builder.is_not_in(&field, literals),
            _ => unreachable!("test fixture operator"),
        }
        .unwrap()
    }

    fn batch(fields: &[DataField], values: Vec<ArrayRef>) -> RecordBatch {
        let schema = fields
            .iter()
            .map(|field| {
                crate::arrow::paimon_type_to_arrow(field.data_type())
                    .map(|ty| Field::new(field.name(), ty, true))
            })
            .collect::<Result<Vec<_>>>()
            .unwrap();
        RecordBatch::try_new(Arc::new(Schema::new(schema)), values).unwrap()
    }

    fn mask(batch: &RecordBatch, predicate: &Predicate, fields: &[DataField]) -> Vec<bool> {
        evaluate_predicate_mask(batch, predicate, fields, fields)
            .unwrap()
            .unwrap()
            .values()
            .iter()
            .collect()
    }

    #[test]
    fn nested_predicate_masks_null_ancestors_and_sliced_children() {
        let fields = schema();
        // NULL parents deliberately contain non-NULL physical children. Their
        // values must never leak into equality or IS NOT NULL results.
        let detail = StructArray::new(
            vec![Arc::new(Field::new("value", ArrowType::Int32, true))].into(),
            vec![Arc::new(Int32Array::from(vec![
                Some(99),
                Some(7),
                Some(7),
                None,
                Some(7),
            ]))],
            Some(NullBuffer::from(vec![true, false, true, true, true])),
        );
        let root = StructArray::new(
            vec![Arc::new(Field::new(
                "detail",
                detail.data_type().clone(),
                true,
            ))]
            .into(),
            vec![Arc::new(detail)],
            Some(NullBuffer::from(vec![true, true, false, true, true])),
        );
        let batch = batch(&fields, vec![Arc::new(root)]).slice(1, 4);
        let equal = predicate(&fields, PredicateOperator::Eq, vec![Datum::Int(7)]);
        assert_eq!(mask(&batch, &equal, &fields), [false, false, false, true]);
        let is_null = predicate(&fields, PredicateOperator::IsNull, vec![]);
        assert_eq!(mask(&batch, &is_null, &fields), [true, true, true, false]);
        let not_in = predicate(&fields, PredicateOperator::NotIn, vec![Datum::Int(9)]);
        assert_eq!(mask(&batch, &not_in, &fields), [false, false, false, true]);
        let empty_in = predicate(&fields, PredicateOperator::In, vec![]);
        assert_eq!(
            mask(&batch, &Predicate::negate(empty_in), &fields),
            [false, false, false, true]
        );
    }

    #[test]
    fn nested_predicate_resolves_file_ids_after_rename_and_promotion() {
        let old = schema();
        let new = vec![row(
            10,
            "renamed_root",
            vec![row(
                20,
                "renamed_detail",
                vec![DataField::new(
                    30,
                    "renamed_value".into(),
                    DataType::BigInt(BigIntType::new()),
                )],
            )],
        )];
        let detail = StructArray::new(
            vec![Arc::new(Field::new("value", ArrowType::Int32, true))].into(),
            vec![Arc::new(Int32Array::from(vec![Some(7), Some(8), None]))],
            None,
        );
        let root = StructArray::new(
            vec![Arc::new(Field::new(
                "detail",
                detail.data_type().clone(),
                true,
            ))]
            .into(),
            vec![Arc::new(detail)],
            None,
        );
        let batch = batch(&old, vec![Arc::new(root)]);
        let builder = PredicateBuilder::new(&new);
        let field = builder
            .nested_field("renamed_root", &["renamed_detail", "renamed_value"])
            .unwrap();
        let pred = builder.equal(&field, Datum::Long(7)).unwrap();
        assert_eq!(mask(&batch, &pred, &old), [true, false, false]);
        let pred = builder
            .greater_than(&field, Datum::Long(i32::MAX as i64 + 1))
            .unwrap();
        assert_eq!(mask(&batch, &pred, &old), [false, false, false]);
    }

    #[test]
    fn nested_predicate_treats_readded_same_name_as_missing() {
        let old = vec![row(
            10,
            "root",
            vec![row(20, "detail", vec![int(31, "value")])],
        )];
        let new = schema();
        let detail = StructArray::new(
            vec![Arc::new(Field::new("value", ArrowType::Int32, true))].into(),
            vec![Arc::new(Int32Array::from(vec![7, 8]))],
            None,
        );
        let root = StructArray::new(
            vec![Arc::new(Field::new(
                "detail",
                detail.data_type().clone(),
                true,
            ))]
            .into(),
            vec![Arc::new(detail)],
            None,
        );
        let batch = batch(&old, vec![Arc::new(root)]);
        let pred = predicate(&new, PredicateOperator::IsNull, vec![]);
        assert_eq!(mask(&batch, &pred, &old), [true, true]);
        let pred = predicate(&new, PredicateOperator::Eq, vec![Datum::Int(7)]);
        assert_eq!(mask(&batch, &pred, &old), [false, false]);
    }

    #[test]
    fn nested_predicate_prefers_arrow_ids_to_projected_positions() {
        let fields = schema();
        let physical = |id: i32, name: &str| {
            Arc::new(Field::new(name, ArrowType::Int32, true).with_metadata(
                std::collections::HashMap::from([("PARQUET:field_id".into(), id.to_string())]),
            ))
        };
        let detail = StructArray::new(
            vec![physical(31_i32, "other"), physical(30_i32, "renamed")].into(),
            vec![
                Arc::new(Int32Array::from(vec![7, 7])),
                Arc::new(Int32Array::from(vec![8, 7])),
            ],
            None,
        );
        let root = StructArray::new(
            vec![Arc::new(Field::new(
                "detail",
                detail.data_type().clone(),
                true,
            ))]
            .into(),
            vec![Arc::new(detail)],
            None,
        );
        let batch = RecordBatch::try_from_iter([("root", Arc::new(root) as ArrayRef)]).unwrap();
        assert_eq!(
            mask(
                &batch,
                &predicate(&fields, PredicateOperator::Eq, vec![Datum::Int(7)]),
                &fields
            ),
            [false, true]
        );
    }

    #[test]
    fn nested_predicate_widens_pruned_roots_for_unselected_filter_fields() {
        let full = schema();
        let projected = vec![row(10, "root", vec![row(20, "detail", vec![])])];
        let predicate = predicate(&full, PredicateOperator::Eq, vec![Datum::Int(7)]);
        let predicates = FilePredicates {
            predicates: vec![Predicate::or(vec![predicate, Predicate::AlwaysFalse])],
            file_fields: full.clone(),
            row_filter_factory: None,
        };
        assert_eq!(widen_scan_fields(&projected, Some(&predicates)), full);
    }

    #[test]
    fn nested_predicate_missing_decoded_child_errors_instead_of_returning_null() {
        let fields = schema();
        let detail = StructArray::new_empty_fields(2, None);
        let root = StructArray::new(
            vec![Arc::new(Field::new(
                "detail",
                detail.data_type().clone(),
                true,
            ))]
            .into(),
            vec![Arc::new(detail)],
            None,
        );
        let batch = RecordBatch::try_from_iter([("root", Arc::new(root) as ArrayRef)]).unwrap();
        assert!(evaluate_predicate_mask(
            &batch,
            &predicate(&fields, PredicateOperator::IsNull, vec![]),
            &fields,
            &fields
        )
        .unwrap_err()
        .to_string()
        .contains("missing from the scanned ROW"));
    }

    #[test]
    fn nested_predicate_statistics_never_read_parent_bounds() {
        struct ParentStats;
        impl crate::predicate_stats::StatsAccessor for ParentStats {
            fn row_count(&self) -> i64 {
                2
            }
            fn null_count(&self, _: usize) -> Option<i64> {
                panic!("parent null count")
            }
            fn min_value(&self, _: usize, _: &DataType) -> Option<Datum> {
                panic!("parent min")
            }
            fn max_value(&self, _: usize, _: &DataType) -> Option<Datum> {
                panic!("parent max")
            }
        }
        let fields = schema();
        let pred = predicate(&fields, PredicateOperator::Eq, vec![Datum::Int(7)]);
        for pred in [
            pred.clone(),
            Predicate::negate(pred.clone()),
            Predicate::and(vec![pred.clone(), Predicate::AlwaysTrue]),
            Predicate::or(vec![pred, Predicate::AlwaysFalse]),
        ] {
            assert!(crate::predicate_stats::predicates_may_match_with_schema(
                &[pred],
                &ParentStats,
                &[Some(0)],
                &fields
            ));
        }
    }

    #[test]
    fn nested_predicate_builder_checks_paths_literals_and_stable_identity() {
        let fields = vec![row(
            10,
            "Root",
            vec![int(30, "a.b"), int(31, "Value"), int(32, "value")],
        )];
        let builder = PredicateBuilder::new(&fields);
        assert!(builder.nested_field("Root", &[]).is_err());
        let field = builder.nested_field("Root", &["a.b"]).unwrap();
        let pred = builder.equal(&field, Datum::Int(7)).unwrap();
        pred.validate_nested_fields(&fields).unwrap();
        assert!(builder.nested_field("Root", &["a", "b"]).is_err());
        assert!(builder.nested_field("Root", &["a.b", "child"]).is_err());
        assert!(builder.equal(&field, Datum::Long(7)).is_err());
        let insensitive = PredicateBuilder::new_with_case_sensitive(&fields, false);
        assert!(insensitive.nested_field("root", &["VALUE"]).is_err());
        let renamed = insensitive.nested_field("root", &["A.B"]).unwrap();
        insensitive
            .is_null(&renamed)
            .unwrap()
            .validate_nested_fields(&fields)
            .unwrap();
        let changed = vec![row(
            10,
            "Root",
            vec![int(35, "a.b"), int(31, "Value"), int(32, "value")],
        )];
        assert!(pred.validate_nested_fields(&changed).is_err());
    }

    #[test]
    fn nested_predicate_binary_rows_follow_parent_nulls() {
        let fields = schema();
        let pred = predicate(&fields, PredicateOperator::Eq, vec![Datum::Int(7)]);
        let is_null = predicate(&fields, PredicateOperator::IsNull, vec![]);
        let mut leaf = BinaryRowBuilder::new(1);
        leaf.write_int(0, 7);
        let mut detail = BinaryRowBuilder::new(1);
        detail.write_binary(0, leaf.build().data());
        let mut row = BinaryRowBuilder::new(1);
        row.write_binary(0, detail.build().data());
        assert!(crate::spec::eval_row(&pred, &row.build()).unwrap());
        let mut row = BinaryRowBuilder::new(1);
        row.set_null_at(0);
        assert!(crate::spec::eval_row(&is_null, &row.build()).unwrap());
        let mut detail = BinaryRowBuilder::new(1);
        detail.set_null_at(0);
        let mut row = BinaryRowBuilder::new(1);
        row.write_binary(0, detail.build().data());
        assert!(!crate::spec::eval_row(&pred, &row.build()).unwrap());
        // Keep an unrelated primitive column to exercise a schema with no root.
        let batch = RecordBatch::try_from_iter([(
            "other",
            Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef,
        )])
        .unwrap();
        assert_eq!(mask(&batch, &is_null, &[]), [true]);
    }
}
