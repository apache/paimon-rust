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

use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{Between, BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use paimon::spec::{DataField, DataType, Datum, Predicate, PredicateBuilder};

pub(crate) fn classify_filter_pushdown(
    filter: &Expr,
    fields: &[DataField],
    partition_keys: &[String],
) -> TableProviderFilterPushDown {
    let translator = FilterTranslator::new(fields);
    if translator.translate(filter).is_some() {
        let partition_translator = FilterTranslator::for_allowed_columns(fields, partition_keys);
        if partition_translator.translate(filter).is_some() {
            TableProviderFilterPushDown::Exact
        } else {
            TableProviderFilterPushDown::Inexact
        }
    } else if split_conjunction(filter)
        .into_iter()
        .any(|expr| translator.translate(expr).is_some())
    {
        TableProviderFilterPushDown::Inexact
    } else {
        TableProviderFilterPushDown::Unsupported
    }
}

pub(crate) fn build_pushed_predicate(filters: &[Expr], fields: &[DataField]) -> Option<Predicate> {
    let translator = FilterTranslator::new(fields);
    let pushed: Vec<_> = filters
        .iter()
        .flat_map(split_conjunction)
        .filter_map(|filter| translator.translate(filter))
        .collect();

    if pushed.is_empty() {
        None
    } else {
        Some(Predicate::and(pushed))
    }
}

fn split_conjunction(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => {
            let mut conjuncts = split_conjunction(left.as_ref());
            conjuncts.extend(split_conjunction(right.as_ref()));
            conjuncts
        }
        other => vec![other],
    }
}

struct FilterTranslator<'a> {
    fields: &'a [DataField],
    allowed_columns: Option<&'a [String]>,
    predicate_builder: PredicateBuilder,
}

impl<'a> FilterTranslator<'a> {
    fn new(fields: &'a [DataField]) -> Self {
        Self {
            fields,
            allowed_columns: None,
            predicate_builder: PredicateBuilder::new(fields),
        }
    }

    fn for_allowed_columns(fields: &'a [DataField], allowed_columns: &'a [String]) -> Self {
        Self {
            fields,
            allowed_columns: Some(allowed_columns),
            predicate_builder: PredicateBuilder::new(fields),
        }
    }

    fn translate(&self, expr: &Expr) -> Option<Predicate> {
        match expr {
            Expr::BinaryExpr(binary) => self.translate_binary(binary),
            // NOT is intentionally not translated: Predicate::Not uses two-valued
            // logic (!bool), which incorrectly returns true when the inner predicate
            // evaluates NULL to false. Combined with Exact pushdown precision,
            // DataFusion would remove the residual filter, producing wrong results.
            Expr::Not(_) => None,
            Expr::IsNull(inner) => {
                let field = self.resolve_field(inner.as_ref())?;
                self.predicate_builder.is_null(field.name()).ok()
            }
            Expr::IsNotNull(inner) => {
                let field = self.resolve_field(inner.as_ref())?;
                self.predicate_builder.is_not_null(field.name()).ok()
            }
            Expr::InList(in_list) => self.translate_in_list(in_list),
            Expr::Between(between) => self.translate_between(between),
            _ => None,
        }
    }

    fn translate_binary(&self, binary: &BinaryExpr) -> Option<Predicate> {
        match binary.op {
            Operator::And => Some(Predicate::and(vec![
                self.translate(binary.left.as_ref())?,
                self.translate(binary.right.as_ref())?,
            ])),
            Operator::Or => Some(Predicate::or(vec![
                self.translate(binary.left.as_ref())?,
                self.translate(binary.right.as_ref())?,
            ])),
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => self.translate_comparison(binary),
            _ => None,
        }
    }

    fn translate_comparison(&self, binary: &BinaryExpr) -> Option<Predicate> {
        if let Some(predicate) = self.translate_column_literal_comparison(
            binary.left.as_ref(),
            binary.op,
            binary.right.as_ref(),
        ) {
            return Some(predicate);
        }

        let reversed = reverse_comparison_operator(binary.op)?;
        self.translate_column_literal_comparison(
            binary.right.as_ref(),
            reversed,
            binary.left.as_ref(),
        )
    }

    fn translate_column_literal_comparison(
        &self,
        column_expr: &Expr,
        op: Operator,
        literal_expr: &Expr,
    ) -> Option<Predicate> {
        let field = self.resolve_field(column_expr)?;
        let scalar = extract_scalar_literal(literal_expr)?;
        let datum = scalar_to_datum(scalar, field.data_type())?;

        match op {
            Operator::Eq => self.predicate_builder.equal(field.name(), datum).ok(),
            Operator::NotEq => self.predicate_builder.not_equal(field.name(), datum).ok(),
            Operator::Lt => self.predicate_builder.less_than(field.name(), datum).ok(),
            Operator::LtEq => self
                .predicate_builder
                .less_or_equal(field.name(), datum)
                .ok(),
            Operator::Gt => self
                .predicate_builder
                .greater_than(field.name(), datum)
                .ok(),
            Operator::GtEq => self
                .predicate_builder
                .greater_or_equal(field.name(), datum)
                .ok(),
            _ => None,
        }
    }

    fn translate_in_list(&self, in_list: &InList) -> Option<Predicate> {
        let field = self.resolve_field(in_list.expr.as_ref())?;
        let literals: Option<Vec<_>> = in_list
            .list
            .iter()
            .map(|expr| {
                let scalar = extract_scalar_literal(expr)?;
                scalar_to_datum(scalar, field.data_type())
            })
            .collect();
        let literals = literals?;

        if in_list.negated {
            self.predicate_builder
                .is_not_in(field.name(), literals)
                .ok()
        } else {
            self.predicate_builder.is_in(field.name(), literals).ok()
        }
    }

    fn translate_between(&self, between: &Between) -> Option<Predicate> {
        let field = self.resolve_field(between.expr.as_ref())?;
        let low = scalar_to_datum(
            extract_scalar_literal(between.low.as_ref())?,
            field.data_type(),
        )?;
        let high = scalar_to_datum(
            extract_scalar_literal(between.high.as_ref())?,
            field.data_type(),
        )?;

        let predicate = Predicate::and(vec![
            self.predicate_builder
                .greater_or_equal(field.name(), low)
                .ok()?,
            self.predicate_builder
                .less_or_equal(field.name(), high)
                .ok()?,
        ]);

        if between.negated {
            // Same concern as Expr::Not: negation wraps in Predicate::Not
            // which has incorrect NULL semantics for Exact pushdown.
            None
        } else {
            Some(predicate)
        }
    }

    fn resolve_field(&self, expr: &Expr) -> Option<&'a DataField> {
        let Expr::Column(Column { name, .. }) = expr else {
            return None;
        };

        if let Some(allowed_columns) = self.allowed_columns {
            if !allowed_columns.iter().any(|column| column == name) {
                return None;
            }
        }

        self.fields.iter().find(|field| field.name() == name)
    }
}

fn extract_scalar_literal(expr: &Expr) -> Option<&ScalarValue> {
    match expr {
        Expr::Literal(scalar, _) if !scalar.is_null() => Some(scalar),
        _ => None,
    }
}

fn reverse_comparison_operator(op: Operator) -> Option<Operator> {
    match op {
        Operator::Eq => Some(Operator::Eq),
        Operator::NotEq => Some(Operator::NotEq),
        Operator::Lt => Some(Operator::Gt),
        Operator::LtEq => Some(Operator::GtEq),
        Operator::Gt => Some(Operator::Lt),
        Operator::GtEq => Some(Operator::LtEq),
        _ => None,
    }
}

fn scalar_to_datum(scalar: &ScalarValue, data_type: &DataType) -> Option<Datum> {
    match data_type {
        DataType::Boolean(_) => match scalar {
            ScalarValue::Boolean(Some(value)) => Some(Datum::Bool(*value)),
            _ => None,
        },
        DataType::TinyInt(_) => scalar_to_i128(scalar)
            .and_then(|value| i8::try_from(value).ok())
            .map(Datum::TinyInt),
        DataType::SmallInt(_) => scalar_to_i128(scalar)
            .and_then(|value| i16::try_from(value).ok())
            .map(Datum::SmallInt),
        DataType::Int(_) => scalar_to_i128(scalar)
            .and_then(|value| i32::try_from(value).ok())
            .map(Datum::Int),
        DataType::BigInt(_) => scalar_to_i128(scalar)
            .and_then(|value| i64::try_from(value).ok())
            .map(Datum::Long),
        DataType::Float(_) => match scalar {
            ScalarValue::Float32(Some(value)) => Some(Datum::Float(*value)),
            _ => None,
        },
        DataType::Double(_) => match scalar {
            ScalarValue::Float64(Some(value)) => Some(Datum::Double(*value)),
            ScalarValue::Float32(Some(value)) => Some(Datum::Double(*value as f64)),
            _ => None,
        },
        DataType::Char(_) | DataType::VarChar(_) => match scalar {
            ScalarValue::Utf8(Some(value))
            | ScalarValue::Utf8View(Some(value))
            | ScalarValue::LargeUtf8(Some(value)) => Some(Datum::String(value.clone())),
            _ => None,
        },
        DataType::Date(_) => match scalar {
            ScalarValue::Date32(Some(value)) => Some(Datum::Date(*value)),
            _ => None,
        },
        DataType::Decimal(decimal) => match scalar {
            ScalarValue::Decimal128(Some(unscaled), precision, scale)
                if u32::from(*precision) <= decimal.precision() && i32::from(*scale) >= 0 =>
            {
                let scale = u32::try_from(i32::from(*scale)).ok()?;
                if scale != decimal.scale() {
                    return None;
                }
                Some(Datum::Decimal {
                    unscaled: *unscaled,
                    precision: decimal.precision(),
                    scale: decimal.scale(),
                })
            }
            _ => None,
        },
        DataType::Binary(_) | DataType::VarBinary(_) => match scalar {
            ScalarValue::Binary(Some(value))
            | ScalarValue::BinaryView(Some(value))
            | ScalarValue::LargeBinary(Some(value)) => Some(Datum::Bytes(value.clone())),
            ScalarValue::FixedSizeBinary(_, Some(value)) => Some(Datum::Bytes(value.clone())),
            _ => None,
        },
        _ => None,
    }
}

fn scalar_to_i128(scalar: &ScalarValue) -> Option<i128> {
    match scalar {
        ScalarValue::Int8(Some(value)) => Some(i128::from(*value)),
        ScalarValue::Int16(Some(value)) => Some(i128::from(*value)),
        ScalarValue::Int32(Some(value)) => Some(i128::from(*value)),
        ScalarValue::Int64(Some(value)) => Some(i128::from(*value)),
        ScalarValue::UInt8(Some(value)) => Some(i128::from(*value)),
        ScalarValue::UInt16(Some(value)) => Some(i128::from(*value)),
        ScalarValue::UInt32(Some(value)) => Some(i128::from(*value)),
        ScalarValue::UInt64(Some(value)) => Some(i128::from(*value)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::Column;
    use datafusion::logical_expr::{expr::InList, lit, TableProviderFilterPushDown};
    use paimon::spec::{IntType, VarCharType};

    fn test_fields() -> Vec<DataField> {
        vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "dt".to_string(),
                DataType::VarChar(VarCharType::string_type()),
            ),
            DataField::new(2, "hr".to_string(), DataType::Int(IntType::new())),
        ]
    }

    fn partition_keys() -> Vec<String> {
        vec!["dt".to_string(), "hr".to_string()]
    }

    #[test]
    fn test_translate_partition_equality_filter() {
        let fields = test_fields();
        let filter = Expr::Column(Column::from_name("dt")).eq(lit("2024-01-01"));

        let predicate =
            build_pushed_predicate(&[filter], &fields).expect("partition filter should translate");

        assert_eq!(predicate.to_string(), "dt = '2024-01-01'");
    }

    #[test]
    fn test_classify_partition_filter_as_exact() {
        let fields = test_fields();
        let filter = Expr::Column(Column::from_name("dt")).eq(lit("2024-01-01"));

        assert_eq!(
            classify_filter_pushdown(&filter, &fields, &partition_keys()),
            TableProviderFilterPushDown::Exact
        );
    }

    #[test]
    fn test_translate_reversed_partition_comparison() {
        let fields = test_fields();
        let filter = lit(10).lt(Expr::Column(Column::from_name("hr")));

        let predicate = build_pushed_predicate(&[filter], &fields)
            .expect("reversed comparison should translate");

        assert_eq!(predicate.to_string(), "hr > 10");
    }

    #[test]
    fn test_translate_partition_in_list() {
        let fields = test_fields();
        let filter = Expr::InList(InList::new(
            Box::new(Expr::Column(Column::from_name("dt"))),
            vec![lit("2024-01-01"), lit("2024-01-02")],
            false,
        ));

        let predicate =
            build_pushed_predicate(&[filter], &fields).expect("in-list filter should translate");

        assert_eq!(predicate.to_string(), "dt IN ('2024-01-01', '2024-01-02')");
    }

    #[test]
    fn test_translate_mixed_or_filter() {
        let fields = test_fields();
        let filter = Expr::Column(Column::from_name("dt"))
            .eq(lit("2024-01-01"))
            .or(Expr::Column(Column::from_name("id")).gt(lit(10)));

        let predicate =
            build_pushed_predicate(&[filter], &fields).expect("mixed OR filter should translate");

        assert_eq!(predicate.to_string(), "(dt = '2024-01-01' OR id > 10)");
    }

    #[test]
    fn test_translate_non_partition_filter() {
        let fields = test_fields();
        let filter = Expr::Column(Column::from_name("id")).gt(lit(10));

        let predicate =
            build_pushed_predicate(&[filter], &fields).expect("data filter should translate");

        assert_eq!(predicate.to_string(), "id > 10");
    }

    #[test]
    fn test_classify_non_partition_filter_as_inexact() {
        let fields = test_fields();
        let filter = Expr::Column(Column::from_name("id")).gt(lit(10));

        assert_eq!(
            classify_filter_pushdown(&filter, &fields, &partition_keys()),
            TableProviderFilterPushDown::Inexact
        );
    }

    #[test]
    fn test_translate_mixed_and_filter() {
        let fields = test_fields();
        let filter = Expr::Column(Column::from_name("dt"))
            .eq(lit("2024-01-01"))
            .and(Expr::Column(Column::from_name("id")).gt(lit(10)));

        let predicate =
            build_pushed_predicate(&[filter], &fields).expect("mixed filter should translate");

        assert_eq!(predicate.to_string(), "(dt = '2024-01-01' AND id > 10)");
    }

    #[test]
    fn test_classify_mixed_and_filter_as_inexact() {
        let fields = test_fields();
        let filter = Expr::Column(Column::from_name("dt"))
            .eq(lit("2024-01-01"))
            .and(Expr::Column(Column::from_name("id")).gt(lit(10)));

        assert_eq!(
            classify_filter_pushdown(&filter, &fields, &partition_keys()),
            TableProviderFilterPushDown::Inexact
        );
    }

    #[test]
    fn test_translate_not_is_not_supported() {
        let fields = test_fields();
        let filter = Expr::Not(Box::new(
            Expr::Column(Column::from_name("dt")).eq(lit("2024-01-01")),
        ));

        assert!(
            build_pushed_predicate(&[filter], &fields).is_none(),
            "NOT expressions should not translate due to NULL semantics"
        );
    }

    #[test]
    fn test_classify_not_filter_as_unsupported() {
        let fields = test_fields();
        let filter = Expr::Not(Box::new(
            Expr::Column(Column::from_name("dt")).eq(lit("2024-01-01")),
        ));

        assert_eq!(
            classify_filter_pushdown(&filter, &fields, &partition_keys()),
            TableProviderFilterPushDown::Unsupported
        );
    }

    #[test]
    fn test_translate_negated_between_is_not_supported() {
        let fields = test_fields();
        let filter = Expr::Between(Between::new(
            Box::new(Expr::Column(Column::from_name("hr"))),
            true, // negated
            Box::new(lit(1)),
            Box::new(lit(20)),
        ));

        assert!(
            build_pushed_predicate(&[filter], &fields).is_none(),
            "Negated BETWEEN should not translate due to NULL semantics"
        );
    }

    #[test]
    fn test_translate_boolean_literal_is_not_supported() {
        let fields = test_fields();

        for value in [true, false] {
            let filter = Expr::Literal(ScalarValue::Boolean(Some(value)), None);
            assert!(
                build_pushed_predicate(&[filter], &fields).is_none(),
                "Boolean literal ({value}) is not a partition predicate and must not be translated"
            );
        }
    }
}
