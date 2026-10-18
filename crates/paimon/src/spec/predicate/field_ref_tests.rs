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

use super::{Datum, Predicate, PredicateBuilder, PredicateOperator};
use crate::spec::{ArrayType, DataField, DataType, IntType, RowType, VarCharType};

fn fields(root_id: i32, score_id: i32, score_type: DataType) -> Vec<DataField> {
    vec![
        DataField::new(1, "id".into(), DataType::Int(IntType::new())),
        DataField::new(
            root_id,
            "profile".into(),
            DataType::Row(RowType::new(vec![DataField::new(
                11,
                "detail".into(),
                DataType::Row(RowType::new(vec![
                    DataField::new(score_id, "score".into(), score_type),
                    DataField::new(
                        13,
                        "label".into(),
                        DataType::VarChar(VarCharType::new(100).unwrap()),
                    ),
                    DataField::new(
                        14,
                        "values".into(),
                        DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
                    ),
                ])),
            )])),
        ),
    ]
}

fn schema() -> Vec<DataField> {
    fields(10, 12, DataType::Int(IntType::new()))
}

fn assert_nested(predicate: Predicate, expected: PredicateOperator, literals: &[Datum]) {
    match predicate {
        Predicate::NestedLeaf {
            root,
            index,
            path,
            op,
            literals: actual,
        } => {
            assert_eq!(root.id(), 10);
            assert_eq!(index, 1);
            assert_eq!(path.iter().map(DataField::id).collect::<Vec<_>>()[0], 11);
            assert_eq!(path.len(), 2);
            assert_eq!(op, expected);
            assert_eq!(actual, literals);
        }
        other => panic!("expected a nested comparison, got {other:?}"),
    }
}

#[test]
fn field_references_preserve_string_callers_and_can_be_reused() {
    let builder = PredicateBuilder::new(&schema());
    let name = String::from("id");
    let id = builder.field(&name).unwrap();
    let expected = builder.equal("id", Datum::Int(7)).unwrap();
    assert_eq!(
        builder.equal(name.clone(), Datum::Int(7)).unwrap(),
        expected
    );
    assert_eq!(builder.equal(&name, Datum::Int(7)).unwrap(), expected);
    assert_eq!(builder.equal(&id, Datum::Int(7)).unwrap(), expected);
    assert_eq!(builder.equal(id, Datum::Int(7)).unwrap(), expected);

    let score = builder
        .nested_field("profile", &["detail", "score"])
        .unwrap();
    assert_eq!(score.data_type(), &DataType::Int(IntType::new()));
    let same_schema = PredicateBuilder::new(&schema());
    assert_eq!(
        same_schema.equal(&score, Datum::Int(7)).unwrap(),
        builder.equal(score, Datum::Int(7)).unwrap()
    );
}

#[test]
fn nested_field_uses_all_ordinary_comparisons_and_null_checks() {
    let builder = PredicateBuilder::new(&schema());
    let score = builder
        .nested_field("profile", &["detail", "score"])
        .unwrap();
    let literal = Datum::Int(7);
    for (op, predicate) in [
        (
            PredicateOperator::Eq,
            builder.equal(&score, literal.clone()),
        ),
        (
            PredicateOperator::NotEq,
            builder.not_equal(&score, literal.clone()),
        ),
        (
            PredicateOperator::Lt,
            builder.less_than(&score, literal.clone()),
        ),
        (
            PredicateOperator::LtEq,
            builder.less_or_equal(&score, literal.clone()),
        ),
        (
            PredicateOperator::Gt,
            builder.greater_than(&score, literal.clone()),
        ),
        (
            PredicateOperator::GtEq,
            builder.greater_or_equal(&score, literal.clone()),
        ),
    ] {
        assert_nested(predicate.unwrap(), op, std::slice::from_ref(&literal));
    }
    assert_nested(
        builder.is_null(&score).unwrap(),
        PredicateOperator::IsNull,
        &[],
    );
    assert_nested(
        builder.is_not_null(score).unwrap(),
        PredicateOperator::IsNotNull,
        &[],
    );
}

#[test]
fn nested_set_and_range_helpers_keep_java_null_semantics() {
    let builder = PredicateBuilder::new(&schema());
    let score = builder
        .nested_field("profile", &["detail", "score"])
        .unwrap();
    let values = vec![Datum::Int(1), Datum::Int(7)];
    assert_nested(
        builder.is_in(&score, values.clone()).unwrap(),
        PredicateOperator::In,
        &values,
    );
    assert_nested(
        builder.is_not_in(&score, values.clone()).unwrap(),
        PredicateOperator::NotIn,
        &values,
    );
    assert_nested(
        builder
            .is_in_with_nulls(&score, vec![None, Some(Datum::Int(7))])
            .unwrap(),
        PredicateOperator::In,
        &[Datum::Int(7)],
    );
    assert_nested(
        builder.is_in_with_nulls(&score, vec![None]).unwrap(),
        PredicateOperator::In,
        &[],
    );
    assert!(matches!(
        builder
            .is_not_in_with_nulls(&score, vec![Some(Datum::Int(7)), None])
            .unwrap(),
        Predicate::AlwaysFalse
    ));
    assert_nested(
        builder
            .between(&score, values[0].clone(), values[1].clone())
            .unwrap(),
        PredicateOperator::Between,
        &values,
    );
    assert_nested(
        builder
            .not_between(&score, values[0].clone(), values[1].clone())
            .unwrap(),
        PredicateOperator::NotBetween,
        &values,
    );
    assert!(matches!(
        builder
            .between(&score, Datum::Int(7), Datum::Int(1))
            .unwrap(),
        Predicate::AlwaysFalse
    ));
    assert_nested(
        builder
            .not_between(&score, Datum::Int(7), Datum::Int(1))
            .unwrap(),
        PredicateOperator::IsNotNull,
        &[],
    );
}

#[test]
fn nested_like_optimization_and_string_helpers_retain_the_reference() {
    let builder = PredicateBuilder::new(&schema());
    let label = builder
        .nested_field("profile", &["detail", "label"])
        .unwrap();
    for (pattern, op, literal) in [
        ("a", PredicateOperator::Eq, "a"),
        ("a%", PredicateOperator::StartsWith, "a"),
        ("%a", PredicateOperator::EndsWith, "a"),
        ("%a%", PredicateOperator::Contains, "a"),
        ("a_b", PredicateOperator::Like, "a_b"),
    ] {
        assert_nested(
            builder
                .like(&label, Datum::String(pattern.into()), None)
                .unwrap(),
            op,
            &[Datum::String(literal.into())],
        );
    }
    for (op, predicate) in [
        (
            PredicateOperator::StartsWith,
            builder.starts_with(&label, Datum::String("".into())),
        ),
        (
            PredicateOperator::EndsWith,
            builder.ends_with(&label, Datum::String("".into())),
        ),
        (
            PredicateOperator::Contains,
            builder.contains(&label, Datum::String("".into())),
        ),
    ] {
        assert_nested(predicate.unwrap(), op, &[Datum::String("".into())]);
    }
    assert!(builder
        .like(&label, Datum::String("a%".into()), Some('!'))
        .is_err());
    assert!(builder.contains(&label, Datum::Int(7)).is_err());
    let score = builder
        .nested_field("profile", &["detail", "score"])
        .unwrap();
    assert!(builder.contains(score, Datum::String("7".into())).is_err());
}

#[test]
fn nested_array_helpers_validate_the_element_type() {
    let builder = PredicateBuilder::new(&schema());
    let values = builder
        .nested_field("profile", &["detail", "values"])
        .unwrap();
    assert_nested(
        builder.array_contains(&values, Datum::Int(7)).unwrap(),
        PredicateOperator::ArrayContains,
        &[Datum::Int(7)],
    );
    assert_nested(
        builder
            .arrays_overlap(&values, vec![Datum::Int(1), Datum::Int(7)])
            .unwrap(),
        PredicateOperator::ArraysOverlap,
        &[Datum::Int(1), Datum::Int(7)],
    );
    assert_nested(
        builder.array_contains_all(&values, vec![]).unwrap(),
        PredicateOperator::ArrayContainsAll,
        &[],
    );
    assert!(builder
        .array_contains(&values, Datum::String("7".into()))
        .is_err());
    assert!(builder
        .arrays_overlap(&values, vec![Datum::String("7".into())])
        .is_err());
    assert!(builder
        .array_contains_all(values, vec![Datum::String("7".into())])
        .is_err());
    let score = builder
        .nested_field("profile", &["detail", "score"])
        .unwrap();
    assert!(builder.array_contains(score, Datum::Int(7)).is_err());
}

#[test]
fn field_references_reject_changed_identity_and_position_before_constant_folding() {
    let builder = PredicateBuilder::new(&schema());
    let score = builder
        .nested_field("profile", &["detail", "score"])
        .unwrap();
    let mut reordered = schema();
    reordered.swap(0, 1);
    for changed in [
        fields(100, 12, DataType::Int(IntType::new())),
        fields(10, 120, DataType::Int(IntType::new())),
        fields(10, 12, DataType::VarChar(VarCharType::new(100).unwrap())),
        reordered,
    ] {
        let other = PredicateBuilder::new(&changed);
        assert!(other.equal(&score, Datum::Int(7)).is_err());
        assert!(other.is_null(&score).is_err());
        assert!(other.is_in(&score, vec![]).is_err());
        assert!(other.is_in_with_nulls(&score, vec![None]).is_err());
        assert!(other.is_not_in_with_nulls(&score, vec![None]).is_err());
        assert!(other.between(&score, Datum::Int(7), Datum::Int(1)).is_err());
        assert!(other
            .not_between(&score, Datum::Int(7), Datum::Int(1))
            .is_err());
    }
    let id = builder.field("id").unwrap();
    let mut changed = schema();
    changed[0] = DataField::new(100, "id".into(), DataType::Int(IntType::new()));
    assert!(PredicateBuilder::new(&changed)
        .equal(id, Datum::Int(7))
        .is_err());
    assert!(builder
        .between(&score, Datum::String("z".into()), Datum::String("a".into()))
        .is_err());
    assert!(builder
        .not_between(&score, Datum::String("z".into()), Datum::String("a".into()))
        .is_err());
}

#[test]
fn nested_field_resolution_uses_components_and_canonical_names() {
    let fields = vec![DataField::new(
        10,
        "Profile.Root".into(),
        DataType::Row(RowType::new(vec![DataField::new(
            11,
            "Score.Value".into(),
            DataType::Int(IntType::new()),
        )])),
    )];
    let sensitive = PredicateBuilder::new(&fields);
    assert!(sensitive
        .nested_field("profile.root", &["Score.Value"])
        .is_err());
    assert!(sensitive.nested_field("Profile.Root", &[]).is_err());
    assert!(sensitive
        .nested_field("Profile.Root", &["Score", "Value"])
        .is_err());
    assert!(sensitive
        .nested_field("Profile.Root", &["Score.Value", "child"])
        .is_err());
    let insensitive = PredicateBuilder::new_with_case_sensitive(&fields, false);
    let reference = insensitive
        .nested_field("profile.root", &["score.value"])
        .unwrap();
    assert_eq!(
        insensitive.equal(&reference, Datum::Int(7)).unwrap(),
        sensitive
            .equal(
                sensitive
                    .nested_field("Profile.Root", &["Score.Value"])
                    .unwrap(),
                Datum::Int(7)
            )
            .unwrap()
    );
    let ambiguous = vec![DataField::new(
        10,
        "profile".into(),
        DataType::Row(RowType::new(vec![
            DataField::new(11, "Score".into(), DataType::Int(IntType::new())),
            DataField::new(12, "score".into(), DataType::Int(IntType::new())),
        ])),
    )];
    assert!(PredicateBuilder::new_with_case_sensitive(&ambiguous, false)
        .nested_field("profile", &["score"])
        .is_err());
}
