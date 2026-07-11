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
use datafusion::logical_expr::expr::{InList, ScalarFunction};
use datafusion::logical_expr::{
    Between, BinaryExpr, Expr, Like, Operator, TableProviderFilterPushDown,
};
use paimon::spec::{DataField, DataType, Datum, Predicate, PredicateBuilder};

#[derive(Debug)]
struct SingleFilterAnalysis {
    translated_predicates: Vec<Predicate>,
    requires_residual: bool,
}

#[derive(Debug)]
pub(crate) struct FilterPushdownAnalysis {
    pub(crate) pushed_predicate: Option<Predicate>,
    pub(crate) requires_residual: bool,
}

#[derive(Debug)]
struct TranslatedPredicate {
    predicate: Predicate,
    requires_residual: bool,
}

fn analyze_filter(
    filter: &Expr,
    fields: &[DataField],
    case_sensitive: bool,
) -> SingleFilterAnalysis {
    let translator = FilterTranslator::new(fields, case_sensitive);
    if let Some(translated) = translator.translate(filter) {
        return SingleFilterAnalysis {
            translated_predicates: vec![translated.predicate],
            requires_residual: translated.requires_residual,
        };
    }

    let translated = split_conjunction(filter)
        .into_iter()
        .filter_map(|expr| translator.translate(expr))
        .collect::<Vec<_>>();
    SingleFilterAnalysis {
        translated_predicates: translated.iter().map(|t| t.predicate.clone()).collect(),
        requires_residual: true,
    }
}

pub(crate) fn analyze_filters(
    filters: &[Expr],
    fields: &[DataField],
    case_sensitive: bool,
) -> FilterPushdownAnalysis {
    let mut translated_predicates = Vec::new();
    let mut requires_residual = false;

    for filter in filters {
        let analysis = analyze_filter(filter, fields, case_sensitive);
        translated_predicates.extend(analysis.translated_predicates);
        requires_residual |= analysis.requires_residual;
    }

    FilterPushdownAnalysis {
        pushed_predicate: if translated_predicates.is_empty() {
            None
        } else {
            Some(Predicate::and(translated_predicates))
        },
        requires_residual,
    }
}

#[cfg(test)]
pub(crate) fn build_pushed_predicate(filters: &[Expr], fields: &[DataField]) -> Option<Predicate> {
    analyze_filters(filters, fields, true).pushed_predicate
}

pub(crate) fn classify_filter_pushdown<F>(
    filter: &Expr,
    fields: &[DataField],
    case_sensitive: bool,
    is_exact_filter_pushdown: F,
) -> TableProviderFilterPushDown
where
    F: Fn(&Predicate) -> bool,
{
    // `FilterTranslator` still supports case-insensitive column resolution for
    // direct ReadBuilder API callers (and its own unit tests), but the DataFusion
    // TableProvider/SQL path always passes `case_sensitive = true`: the planner
    // resolves columns against the schema before `scan`, so SQL reads are
    // case-sensitive. Reporting `Exact` tells DataFusion to drop its residual
    // filter, so it must only be returned when column resolution is unambiguous.
    // Under case-sensitive resolution a reference matches exactly one field, so
    // ASCII case-folding collisions elsewhere in the schema (e.g. an unrelated
    // `Name`/`name` pair) never make a resolved filter ambiguous and must not
    // downgrade its classification.
    let translator = FilterTranslator::new(fields, case_sensitive);
    if let Some(translated) = translator.translate(filter) {
        if translated.requires_residual {
            TableProviderFilterPushDown::Inexact
        } else if is_exact_filter_pushdown(&translated.predicate) {
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
    predicate_builder: PredicateBuilder,
    case_sensitive: bool,
}

impl<'a> FilterTranslator<'a> {
    fn new(fields: &'a [DataField], case_sensitive: bool) -> Self {
        Self {
            fields,
            predicate_builder: PredicateBuilder::new_with_case_sensitive(fields, case_sensitive),
            case_sensitive,
        }
    }

    fn translate(&self, expr: &Expr) -> Option<TranslatedPredicate> {
        match expr {
            Expr::BinaryExpr(binary) => self.translate_binary(binary),
            // Predicate::Not uses Paimon's two-valued predicate semantics, so
            // translating SQL NOT is only safe as Inexact pushdown: DataFusion
            // must keep its residual filter for NULL / three-valued semantics.
            Expr::Not(inner) => {
                let inner = self.translate(inner.as_ref())?;
                Some(TranslatedPredicate {
                    predicate: Predicate::negate(inner.predicate),
                    requires_residual: true,
                })
            }
            Expr::IsNull(inner) => {
                let field = self.resolve_field(inner.as_ref())?;
                self.exact(self.predicate_builder.is_null(field.name()).ok()?)
            }
            Expr::IsNotNull(inner) => {
                let field = self.resolve_field(inner.as_ref())?;
                self.exact(self.predicate_builder.is_not_null(field.name()).ok()?)
            }
            Expr::InList(in_list) => self.translate_in_list(in_list),
            Expr::Between(between) => self.translate_between(between),
            Expr::ScalarFunction(func) => self.translate_scalar_function(func),
            Expr::Like(like) => self.translate_like(like),
            _ => None,
        }
    }

    fn translate_binary(&self, binary: &BinaryExpr) -> Option<TranslatedPredicate> {
        match binary.op {
            Operator::And | Operator::Or => {
                let left = self.translate(binary.left.as_ref())?;
                let right = self.translate(binary.right.as_ref())?;
                let predicate = if binary.op == Operator::And {
                    Predicate::and(vec![left.predicate, right.predicate])
                } else {
                    Predicate::or(vec![left.predicate, right.predicate])
                };
                Some(TranslatedPredicate {
                    predicate,
                    requires_residual: left.requires_residual || right.requires_residual,
                })
            }
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => self.translate_comparison(binary),
            _ => None,
        }
    }

    fn translate_comparison(&self, binary: &BinaryExpr) -> Option<TranslatedPredicate> {
        if let Some(predicate) = self.translate_column_literal_comparison(
            binary.left.as_ref(),
            binary.op,
            binary.right.as_ref(),
        ) {
            return self.exact(predicate);
        }

        let reversed = reverse_comparison_operator(binary.op)?;
        self.exact(self.translate_column_literal_comparison(
            binary.right.as_ref(),
            reversed,
            binary.left.as_ref(),
        )?)
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

    fn translate_in_list(&self, in_list: &InList) -> Option<TranslatedPredicate> {
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

        self.exact(if in_list.negated {
            self.predicate_builder
                .is_not_in(field.name(), literals)
                .ok()?
        } else {
            self.predicate_builder.is_in(field.name(), literals).ok()?
        })
    }

    fn translate_between(&self, between: &Between) -> Option<TranslatedPredicate> {
        let field = self.resolve_field(between.expr.as_ref())?;
        let low = scalar_to_datum(
            extract_scalar_literal(between.low.as_ref())?,
            field.data_type(),
        )?;
        let high = scalar_to_datum(
            extract_scalar_literal(between.high.as_ref())?,
            field.data_type(),
        )?;

        // Native Between / NotBetween leaf: lets the planner / b-tree
        // recognize the range as a single op (see `btree::query::extract_between`).
        // NotBetween is safe to push because its evaluator, stats prune and
        // Parquet row filter all treat a NULL operand as non-matching (SQL
        // three-valued logic), and a data-column range stays Inexact so
        // DataFusion keeps the residual filter.
        self.exact(if between.negated {
            self.predicate_builder
                .not_between(field.name(), low, high)
                .ok()?
        } else {
            self.predicate_builder
                .between(field.name(), low, high)
                .ok()?
        })
    }

    fn translate_scalar_function(&self, func: &ScalarFunction) -> Option<TranslatedPredicate> {
        // DataFusion built-in UDFs surfaced from `LIKE 'x%' / '%x' / '%x%'`
        // rewrites and direct `starts_with(col, 'x') / ends_with / contains`
        // calls. Only `(col, literal)` shapes are handled; anything else
        // (transform on either side, non-string args) falls open to None.
        if func.args.len() != 2 {
            return None;
        }
        let field = self.resolve_field(&func.args[0])?;
        let scalar = extract_scalar_literal(&func.args[1])?;
        let datum = scalar_to_datum(scalar, field.data_type())?;

        let predicate = match func.name() {
            "starts_with" => self
                .predicate_builder
                .starts_with(field.name(), datum)
                .ok()?,
            "ends_with" => self.predicate_builder.ends_with(field.name(), datum).ok()?,
            "contains" => self.predicate_builder.contains(field.name(), datum).ok()?,
            _ => return None,
        };
        self.exact(predicate)
    }

    fn translate_like(&self, like: &Like) -> Option<TranslatedPredicate> {
        // ILIKE has no equivalent in Paimon's predicate model.
        if like.case_insensitive {
            return None;
        }
        let predicate = self.translate_positive_like(like)?;
        if like.negated {
            Some(TranslatedPredicate {
                predicate: Predicate::negate(predicate),
                requires_residual: true,
            })
        } else {
            self.exact(predicate)
        }
    }

    fn translate_positive_like(&self, like: &Like) -> Option<Predicate> {
        let field = self.resolve_field(like.expr.as_ref())?;
        let scalar = extract_scalar_literal(like.pattern.as_ref())?;
        let datum = scalar_to_datum(scalar, field.data_type())?;
        // PredicateBuilder::like rejects escape characters other than `\`,
        // so unsupported escapes naturally fall open via `.ok() -> None`.
        self.predicate_builder
            .like(field.name(), datum, like.escape_char)
            .ok()
    }

    fn exact(&self, predicate: Predicate) -> Option<TranslatedPredicate> {
        Some(TranslatedPredicate {
            predicate,
            requires_residual: false,
        })
    }

    fn resolve_field(&self, expr: &Expr) -> Option<&'a DataField> {
        let Expr::Column(Column { name, .. }) = expr else {
            return None;
        };

        if self.case_sensitive {
            return self.fields.iter().find(|field| field.name() == name);
        }
        // Case-insensitive: ASCII-fold and require a unique match. An ambiguous
        // (2+) collision returns None so the filter is left as a residual for
        // DataFusion to apply exactly — safe, just not pushed.
        let mut matches = self
            .fields
            .iter()
            .filter(|field| field.name().eq_ignore_ascii_case(name));
        let first = matches.next()?;
        if matches.next().is_some() {
            return None;
        }
        Some(first)
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
        DataType::Time(_) => scalar_to_time_datum(scalar),
        DataType::Timestamp(_) => scalar_to_timestamp_datum(scalar),
        DataType::LocalZonedTimestamp(_) => scalar_to_local_zoned_timestamp_datum(scalar),
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

fn scalar_to_time_datum(scalar: &ScalarValue) -> Option<Datum> {
    match scalar {
        ScalarValue::Time32Millisecond(Some(value)) => Some(Datum::Time(*value)),
        _ => None,
    }
}

fn scalar_to_timestamp_parts(scalar: &ScalarValue) -> Option<(bool, i64, i32)> {
    match scalar {
        ScalarValue::TimestampSecond(Some(value), timezone) => {
            Some((timezone.is_some(), value.checked_mul(1_000)?, 0))
        }
        ScalarValue::TimestampMillisecond(Some(value), timezone) => {
            Some((timezone.is_some(), *value, 0))
        }
        ScalarValue::TimestampMicrosecond(Some(value), timezone) => Some((
            timezone.is_some(),
            value.div_euclid(1_000),
            (value.rem_euclid(1_000) * 1_000) as i32,
        )),
        ScalarValue::TimestampNanosecond(Some(value), timezone) => Some((
            timezone.is_some(),
            value.div_euclid(1_000_000),
            value.rem_euclid(1_000_000) as i32,
        )),
        _ => None,
    }
}

fn scalar_to_timestamp_datum(scalar: &ScalarValue) -> Option<Datum> {
    let (has_timezone, millis, nanos) = scalar_to_timestamp_parts(scalar)?;
    if has_timezone {
        None
    } else {
        Some(Datum::Timestamp { millis, nanos })
    }
}

fn scalar_to_local_zoned_timestamp_datum(scalar: &ScalarValue) -> Option<Datum> {
    let (has_timezone, millis, nanos) = scalar_to_timestamp_parts(scalar)?;
    if has_timezone {
        Some(Datum::LocalZonedTimestamp { millis, nanos })
    } else {
        None
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
    use paimon::catalog::Identifier;
    use paimon::io::FileIOBuilder;
    use paimon::spec::{
        IntType, LocalZonedTimestampType, Schema, TableSchema, TimeType, TimestampType, VarCharType,
    };
    use paimon::table::Table;

    fn test_table() -> Table {
        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("dt", DataType::VarChar(VarCharType::string_type()))
                .column("hr", DataType::Int(IntType::new()))
                .column("time_col", DataType::Time(TimeType::new(3).unwrap()))
                .column(
                    "ts_col",
                    DataType::Timestamp(TimestampType::new(9).unwrap()),
                )
                .column(
                    "lzts_col",
                    DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(9).unwrap()),
                )
                .partition_keys(["dt", "hr"])
                .build()
                .unwrap(),
        );
        Table::new(
            file_io,
            Identifier::new("default", "t"),
            "/tmp/test-filter-pushdown".to_string(),
            table_schema,
            None,
        )
    }

    fn test_fields() -> Vec<DataField> {
        test_table().schema().fields().to_vec()
    }

    fn is_exact_filter_pushdown(predicate: &Predicate) -> bool {
        test_table()
            .new_read_builder()
            .is_exact_filter_pushdown(predicate)
    }

    fn translated_literal(filter: Expr) -> Datum {
        let fields = test_fields();
        let predicate =
            build_pushed_predicate(&[filter], &fields).expect("temporal literal should translate");
        match predicate {
            Predicate::Leaf { mut literals, .. } => {
                assert_eq!(literals.len(), 1);
                literals.remove(0)
            }
            other => panic!("expected Leaf, got {other:?}"),
        }
    }

    #[test]
    fn test_translate_time32_millisecond_literal() {
        let filter = Expr::Column(Column::from_name("time_col")).eq(Expr::Literal(
            ScalarValue::Time32Millisecond(Some(12_345)),
            None,
        ));

        assert_eq!(translated_literal(filter), Datum::Time(12_345));
    }

    #[test]
    fn test_translate_timestamp_millisecond_literal() {
        let filter = Expr::Column(Column::from_name("ts_col")).eq(Expr::Literal(
            ScalarValue::TimestampMillisecond(Some(1_234_567), None),
            None,
        ));

        assert_eq!(
            translated_literal(filter),
            Datum::Timestamp {
                millis: 1_234_567,
                nanos: 0,
            }
        );
    }

    #[test]
    fn test_translate_timestamp_second_literal() {
        let filter = Expr::Column(Column::from_name("ts_col")).eq(Expr::Literal(
            ScalarValue::TimestampSecond(Some(-2), None),
            None,
        ));

        assert_eq!(
            translated_literal(filter),
            Datum::Timestamp {
                millis: -2_000,
                nanos: 0,
            }
        );
    }

    #[test]
    fn test_translate_timestamp_microsecond_literal() {
        let filter = Expr::Column(Column::from_name("ts_col")).eq(Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(-1_234_567), None),
            None,
        ));

        assert_eq!(
            translated_literal(filter),
            Datum::Timestamp {
                millis: -1_235,
                nanos: 433_000,
            }
        );
    }

    #[test]
    fn test_translate_timestamp_nanosecond_literal() {
        let filter = Expr::Column(Column::from_name("ts_col")).eq(Expr::Literal(
            ScalarValue::TimestampNanosecond(Some(-1_234_567_890), None),
            None,
        ));

        assert_eq!(
            translated_literal(filter),
            Datum::Timestamp {
                millis: -1_235,
                nanos: 432_110,
            }
        );
    }

    #[test]
    fn test_translate_local_zoned_timestamp_literal() {
        let filter = Expr::Column(Column::from_name("lzts_col")).eq(Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(1_234_567), Some("UTC".into())),
            None,
        ));

        assert_eq!(
            translated_literal(filter),
            Datum::LocalZonedTimestamp {
                millis: 1_234,
                nanos: 567_000,
            }
        );
    }

    #[test]
    fn test_translate_local_zoned_timestamp_nanosecond_literal() {
        let filter = Expr::Column(Column::from_name("lzts_col")).eq(Expr::Literal(
            ScalarValue::TimestampNanosecond(Some(-1_234_567_890), Some("UTC".into())),
            None,
        ));

        assert_eq!(
            translated_literal(filter),
            Datum::LocalZonedTimestamp {
                millis: -1_235,
                nanos: 432_110,
            }
        );
    }

    #[test]
    fn test_translate_timestamp_timezone_mismatch_falls_open() {
        let fields = test_fields();
        let timestamp_with_timezone = Expr::Column(Column::from_name("ts_col")).eq(Expr::Literal(
            ScalarValue::TimestampMillisecond(Some(1_234), Some("UTC".into())),
            None,
        ));
        let local_zoned_without_timezone = Expr::Column(Column::from_name("lzts_col")).eq(
            Expr::Literal(ScalarValue::TimestampMillisecond(Some(1_234), None), None),
        );

        assert!(build_pushed_predicate(&[timestamp_with_timezone], &fields).is_none());
        assert!(build_pushed_predicate(&[local_zoned_without_timezone], &fields).is_none());
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
            classify_filter_pushdown(&filter, &fields, true, is_exact_filter_pushdown),
            TableProviderFilterPushDown::Exact
        );
    }

    #[test]
    fn test_classify_exact_for_case_colliding_unrelated_schema() {
        use paimon::spec::{DataField, DataType, IntType};
        // The SQL path is case-sensitive, so an unrelated `Name`/`name` pair
        // that only collides under ASCII case-folding must not affect the
        // classification of a filter on a different column: the partition
        // column `dt` resolves to exactly one field and stays `Exact`.
        let mut fields = test_fields();
        let next_id = fields.len() as i32;
        fields.push(DataField::new(
            next_id,
            "Name".to_string(),
            DataType::Int(IntType::new()),
        ));
        fields.push(DataField::new(
            next_id + 1,
            "name".to_string(),
            DataType::Int(IntType::new()),
        ));
        let filter = Expr::Column(Column::from_name("dt")).eq(lit("2024-01-01"));

        assert_eq!(
            classify_filter_pushdown(&filter, &fields, true, is_exact_filter_pushdown),
            TableProviderFilterPushDown::Exact
        );
    }

    #[test]
    fn test_analyze_filters_for_supported_data_filter_has_no_untranslated_residual() {
        let fields = test_fields();
        let filters = vec![Expr::Column(Column::from_name("id")).gt(lit(10))];
        let analysis = analyze_filters(&filters, &fields, true);

        assert_eq!(
            analysis
                .pushed_predicate
                .expect("data filter should translate")
                .to_string(),
            "id > 10"
        );
        assert!(!analysis.requires_residual);
    }

    #[test]
    fn test_analyze_filters_pushes_not_and_marks_residual_required() {
        let fields = test_fields();
        let filters = vec![Expr::Column(Column::from_name("dt"))
            .eq(lit("2024-01-01"))
            .and(Expr::Not(Box::new(
                Expr::Column(Column::from_name("hr")).eq(lit(10)),
            )))];
        let analysis = analyze_filters(&filters, &fields, true);

        assert_eq!(
            analysis
                .pushed_predicate
                .expect("supported conjunct should still translate")
                .to_string(),
            "(dt = '2024-01-01' AND NOT (hr = 10))"
        );
        assert!(analysis.requires_residual);
    }

    #[test]
    fn test_analyze_filters_pushes_not_filter_and_marks_residual_required() {
        let fields = test_fields();
        let filters = vec![Expr::Not(Box::new(
            Expr::Column(Column::from_name("dt")).eq(lit("2024-01-01")),
        ))];
        let analysis = analyze_filters(&filters, &fields, true);

        assert_eq!(
            analysis
                .pushed_predicate
                .expect("NOT partition predicate should translate inexactly")
                .to_string(),
            "NOT (dt = '2024-01-01')"
        );
        assert!(analysis.requires_residual);
    }

    /// Fields whose only string column is spelled `Name` (mixed case), used to
    /// prove case-insensitive column resolution in pushdown.
    fn mixed_case_fields() -> Vec<DataField> {
        vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "Name".to_string(),
                DataType::VarChar(VarCharType::string_type()),
            ),
        ]
    }

    #[test]
    fn test_case_insensitive_pushdown_translates_to_canonical_name() {
        let fields = mixed_case_fields();
        // Request uses the lowercase spelling `name`; schema field is `Name`.
        let filters = vec![Expr::Column(Column::from_name("name")).eq(lit("bob"))];

        // Case-sensitive (default): no match, so nothing is pushed.
        assert!(
            analyze_filters(&filters, &fields, true)
                .pushed_predicate
                .is_none(),
            "exact matching must not resolve a differently-cased column"
        );

        // Case-insensitive: resolves to the canonical `Name` and pushes.
        let analysis = analyze_filters(&filters, &fields, false);
        assert_eq!(
            analysis
                .pushed_predicate
                .expect("case-insensitive filter should push")
                .to_string(),
            "Name = 'bob'"
        );
        assert!(
            !analysis.requires_residual,
            "a translated equality is exact, not residual-only"
        );
    }

    #[test]
    fn test_case_insensitive_pushdown_ambiguous_falls_open() {
        // Two fields collide under ASCII folding: resolution is ambiguous, so the
        // filter is left as a residual (not pushed) rather than picking one.
        let fields = vec![
            DataField::new(0, "Col".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "col".to_string(), DataType::Int(IntType::new())),
        ];
        let filters = vec![Expr::Column(Column::from_name("COL")).eq(lit(1))];
        assert!(
            analyze_filters(&filters, &fields, false)
                .pushed_predicate
                .is_none(),
            "ambiguous case-insensitive column must not be pushed"
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
            classify_filter_pushdown(&filter, &fields, true, is_exact_filter_pushdown),
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
            classify_filter_pushdown(&filter, &fields, true, is_exact_filter_pushdown),
            TableProviderFilterPushDown::Inexact
        );
    }

    #[test]
    fn test_translate_not_pushes_negated_predicate() {
        let fields = test_fields();
        let filter = Expr::Not(Box::new(
            Expr::Column(Column::from_name("dt")).eq(lit("2024-01-01")),
        ));

        assert_eq!(
            build_pushed_predicate(&[filter], &fields)
                .expect("NOT should translate as an inexact pushed predicate")
                .to_string(),
            "NOT (dt = '2024-01-01')"
        );
    }

    #[test]
    fn test_classify_not_filter_as_inexact_even_when_partition_only() {
        let fields = test_fields();
        let filter = Expr::Not(Box::new(
            Expr::Column(Column::from_name("dt")).eq(lit("2024-01-01")),
        ));

        assert_eq!(
            classify_filter_pushdown(&filter, &fields, true, is_exact_filter_pushdown),
            TableProviderFilterPushDown::Inexact
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

    #[test]
    fn test_translate_starts_with_udf() {
        let fields = test_fields();
        let filter = datafusion::functions::string::expr_fn::starts_with(
            Expr::Column(Column::from_name("dt")),
            lit("2024"),
        );
        let predicate =
            build_pushed_predicate(&[filter], &fields).expect("starts_with should translate");
        match predicate {
            Predicate::Leaf { op, literals, .. } => {
                assert_eq!(op, paimon::spec::PredicateOperator::StartsWith);
                assert_eq!(literals, vec![Datum::String("2024".to_string())]);
            }
            other => panic!("expected Leaf, got {other:?}"),
        }
    }

    #[test]
    fn test_translate_ends_with_udf() {
        let fields = test_fields();
        let filter = datafusion::functions::string::expr_fn::ends_with(
            Expr::Column(Column::from_name("dt")),
            lit("01-01"),
        );
        let predicate =
            build_pushed_predicate(&[filter], &fields).expect("ends_with should translate");
        match predicate {
            Predicate::Leaf { op, .. } => {
                assert_eq!(op, paimon::spec::PredicateOperator::EndsWith);
            }
            other => panic!("expected Leaf, got {other:?}"),
        }
    }

    #[test]
    fn test_translate_contains_udf() {
        let fields = test_fields();
        let filter = datafusion::functions::string::expr_fn::contains(
            Expr::Column(Column::from_name("dt")),
            lit("01"),
        );
        let predicate =
            build_pushed_predicate(&[filter], &fields).expect("contains should translate");
        match predicate {
            Predicate::Leaf { op, .. } => {
                assert_eq!(op, paimon::spec::PredicateOperator::Contains);
            }
            other => panic!("expected Leaf, got {other:?}"),
        }
    }

    #[test]
    fn test_translate_starts_with_on_non_string_column_falls_open() {
        let fields = test_fields();
        // `id` is Int — datum coercion fails and translation returns None.
        let filter = datafusion::functions::string::expr_fn::starts_with(
            Expr::Column(Column::from_name("id")),
            lit("foo"),
        );
        assert!(
            build_pushed_predicate(&[filter], &fields).is_none(),
            "starts_with on non-string column must not translate"
        );
    }

    fn like_filter(pattern: &str, negated: bool, case_insensitive: bool) -> Expr {
        Expr::Like(Like::new(
            negated,
            Box::new(Expr::Column(Column::from_name("dt"))),
            Box::new(lit(pattern)),
            None,
            case_insensitive,
        ))
    }

    #[test]
    fn test_translate_like_rewrites_to_starts_with() {
        let fields = test_fields();
        let predicate = build_pushed_predicate(&[like_filter("2024%", false, false)], &fields)
            .expect("LIKE prefix% should translate");
        match predicate {
            Predicate::Leaf { op, literals, .. } => {
                assert_eq!(op, paimon::spec::PredicateOperator::StartsWith);
                assert_eq!(literals, vec![Datum::String("2024".to_string())]);
            }
            other => panic!("expected Leaf, got {other:?}"),
        }
    }

    #[test]
    fn test_translate_like_rewrites_to_ends_with() {
        let fields = test_fields();
        let predicate = build_pushed_predicate(&[like_filter("%01-01", false, false)], &fields)
            .expect("LIKE %suffix should translate");
        match predicate {
            Predicate::Leaf { op, .. } => {
                assert_eq!(op, paimon::spec::PredicateOperator::EndsWith);
            }
            other => panic!("expected Leaf, got {other:?}"),
        }
    }

    #[test]
    fn test_translate_like_rewrites_to_contains() {
        let fields = test_fields();
        let predicate = build_pushed_predicate(&[like_filter("%01%", false, false)], &fields)
            .expect("LIKE %mid% should translate");
        match predicate {
            Predicate::Leaf { op, .. } => {
                assert_eq!(op, paimon::spec::PredicateOperator::Contains);
            }
            other => panic!("expected Leaf, got {other:?}"),
        }
    }

    #[test]
    fn test_translate_like_no_wildcards_rewrites_to_eq() {
        let fields = test_fields();
        let predicate = build_pushed_predicate(&[like_filter("2024-01-01", false, false)], &fields)
            .expect("LIKE without wildcards should translate to Eq");
        match predicate {
            Predicate::Leaf { op, .. } => {
                assert_eq!(op, paimon::spec::PredicateOperator::Eq);
            }
            other => panic!("expected Leaf, got {other:?}"),
        }
    }

    #[test]
    fn test_translate_like_residual_keeps_like_leaf() {
        let fields = test_fields();
        let predicate = build_pushed_predicate(&[like_filter("a%b%c", false, false)], &fields)
            .expect("complex LIKE should translate as a Like leaf");
        match predicate {
            Predicate::Leaf { op, literals, .. } => {
                assert_eq!(op, paimon::spec::PredicateOperator::Like);
                assert_eq!(literals, vec![Datum::String("a%b%c".to_string())]);
            }
            other => panic!("expected Leaf, got {other:?}"),
        }
    }

    #[test]
    fn test_translate_negated_like_pushes_inexact_not() {
        let fields = test_fields();
        let predicate = build_pushed_predicate(&[like_filter("a%", true, false)], &fields)
            .expect("NOT LIKE should translate as inexact NOT over LIKE");
        assert_eq!(predicate.to_string(), "NOT (dt STARTS_WITH 'a')");
        assert_eq!(
            classify_filter_pushdown(
                &like_filter("a%", true, false),
                &fields,
                true,
                is_exact_filter_pushdown
            ),
            TableProviderFilterPushDown::Inexact
        );
    }

    #[test]
    fn test_translate_ilike_falls_open() {
        let fields = test_fields();
        assert!(
            build_pushed_predicate(&[like_filter("a%", false, true)], &fields).is_none(),
            "ILIKE must not translate (case-insensitive not modeled)"
        );
    }

    #[test]
    fn test_translate_between_produces_native_between_leaf() {
        let fields = test_fields();
        let filter = Expr::Between(Between::new(
            Box::new(Expr::Column(Column::from_name("hr"))),
            false,
            Box::new(lit(1)),
            Box::new(lit(20)),
        ));
        let predicate =
            build_pushed_predicate(&[filter], &fields).expect("BETWEEN should translate");
        match predicate {
            Predicate::Leaf { op, literals, .. } => {
                assert_eq!(op, paimon::spec::PredicateOperator::Between);
                assert_eq!(literals, vec![Datum::Int(1), Datum::Int(20)]);
            }
            other => panic!(
                "expected native Between leaf, got {other:?} (Stage 3 must not produce \
                 the legacy GtEq+LtEq And shape)"
            ),
        }
    }

    #[test]
    fn test_translate_not_between_produces_native_not_between_leaf() {
        let fields = test_fields();
        let filter = Expr::Between(Between::new(
            Box::new(Expr::Column(Column::from_name("hr"))),
            true,
            Box::new(lit(1)),
            Box::new(lit(20)),
        ));
        let predicate =
            build_pushed_predicate(&[filter], &fields).expect("NOT BETWEEN should translate");
        match predicate {
            Predicate::Leaf { op, literals, .. } => {
                assert_eq!(op, paimon::spec::PredicateOperator::NotBetween);
                assert_eq!(literals, vec![Datum::Int(1), Datum::Int(20)]);
            }
            other => panic!("expected native NotBetween leaf, got {other:?}"),
        }
    }
}
