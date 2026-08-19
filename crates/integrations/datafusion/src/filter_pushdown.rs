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

use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::DataType as ArrowDataType;
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
                // A positive inexact predicate is only guaranteed to be a
                // conservative superset. Negating it would turn that into a
                // subset and could remove rows before DataFusion's residual
                // runs (notably for floating-array NaN payload semantics).
                if inner.requires_residual {
                    return None;
                }
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
        if matches!(
            func.name(),
            "array_has"
                | "list_has"
                | "array_has_any"
                | "list_has_any"
                | "arrays_overlap"
                | "array_has_all"
                | "list_has_all"
        ) {
            return self.translate_array_function(func);
        }
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

    fn translate_array_function(&self, func: &ScalarFunction) -> Option<TranslatedPredicate> {
        if func.args.len() != 2 {
            return None;
        }
        let (field, comparison_element_type) = self.resolve_array_field(&func.args[0])?;
        let DataType::Array(array_type) = field.data_type() else {
            return None;
        };
        let predicate = match func.name() {
            "array_has" | "list_has" => {
                let literal = extract_array_scalar_literal(
                    &func.args[1],
                    array_type.element_type(),
                    &comparison_element_type,
                )?;
                self.predicate_builder
                    .array_contains(field.name(), literal)
                    .ok()?
            }
            "array_has_any" | "list_has_any" | "arrays_overlap" => {
                let literals = extract_array_literals(
                    &func.args[1],
                    array_type.element_type(),
                    &comparison_element_type,
                )?;
                self.predicate_builder
                    .arrays_overlap(field.name(), literals)
                    .ok()?
            }
            "array_has_all" | "list_has_all" => {
                let literals = extract_array_literals(
                    &func.args[1],
                    array_type.element_type(),
                    &comparison_element_type,
                )?;
                // DataFusion 54's empty-needle fast path currently returns true
                // even for a NULL haystack, while Paimon/Java ARRAY_CONTAINS_ALL
                // returns false for NULL arrays. Pushing it would remove rows
                // before DataFusion can apply its own semantics.
                if literals.is_empty() {
                    return None;
                }
                self.predicate_builder
                    .array_contains_all(field.name(), literals)
                    .ok()?
            }
            _ => return None,
        };
        Some(TranslatedPredicate {
            predicate,
            // Paimon's core residual follows Java Float.compare / Double.compare
            // and canonicalizes all NaNs. DataFusion's Arrow equality keeps NaN
            // payloads distinct, so retain its residual for floating arrays.
            requires_residual: matches!(
                array_type.element_type(),
                DataType::Float(_) | DataType::Double(_)
            ),
        })
    }

    /// Resolve an ARRAY column, accepting only the lossless element-wise casts
    /// inserted by DataFusion's array function type coercion.
    fn resolve_array_field(&self, expr: &Expr) -> Option<(&'a DataField, ArrowDataType)> {
        match expr {
            Expr::Column(_) => {
                let field = self.resolve_field(expr)?;
                let DataType::Array(array_type) = field.data_type() else {
                    return None;
                };
                let comparison_type =
                    paimon::arrow::paimon_type_to_arrow(array_type.element_type()).ok()?;
                Some((field, comparison_type))
            }
            Expr::Cast(cast) => {
                let field = self.resolve_field(cast.expr.as_ref())?;
                let DataType::Array(array_type) = field.data_type() else {
                    return None;
                };
                // Paimon ARRAY columns are Arrow List values. DataFusion's
                // numeric coercion keeps that container and only widens its
                // element. In particular, List -> FixedSizeList is
                // value-dependent and must not be erased here.
                let ArrowDataType::List(target_field) = cast.field.data_type() else {
                    return None;
                };
                if array_type.element_type().is_nullable() && !target_field.is_nullable() {
                    return None;
                }
                let target_element = target_field.data_type();
                if !is_lossless_array_element_cast(array_type.element_type(), target_element) {
                    return None;
                }
                Some((field, target_element.clone()))
            }
            _ => None,
        }
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

fn extract_array_scalar_literal(
    expr: &Expr,
    element_type: &DataType,
    comparison_element_type: &ArrowDataType,
) -> Option<Datum> {
    match expr {
        Expr::Literal(scalar, _) if !scalar.is_null() => {
            scalar_to_array_datum(scalar, element_type)
        }
        Expr::Cast(cast) if cast.field.data_type() == comparison_element_type => {
            let scalar = extract_scalar_literal(cast.expr.as_ref())?;
            if !is_lossless_arrow_scalar_cast(&scalar.data_type(), comparison_element_type) {
                return None;
            }
            scalar_to_array_datum(scalar, element_type)
        }
        _ => None,
    }
}

fn extract_array_literals(
    expr: &Expr,
    element_type: &DataType,
    comparison_element_type: &ArrowDataType,
) -> Option<Vec<Datum>> {
    match expr {
        Expr::ScalarFunction(function) if matches!(function.name(), "make_array" | "make_list") => {
            function
                .args
                .iter()
                .map(|expr| {
                    extract_array_scalar_literal(expr, element_type, comparison_element_type)
                })
                .collect()
        }
        Expr::Cast(cast)
            if arrow_list_element_type(cast.field.data_type()) == Some(comparison_element_type) =>
        {
            extract_array_literals(cast.expr.as_ref(), element_type, comparison_element_type)
        }
        Expr::Literal(scalar, _) => {
            let values = match scalar {
                ScalarValue::List(list) if !list.is_null(0) => list.value(0),
                ScalarValue::LargeList(list) if !list.is_null(0) => list.value(0),
                ScalarValue::FixedSizeList(list) if !list.is_null(0) => list.value(0),
                _ => return None,
            };
            (0..values.len())
                .map(|index| {
                    let scalar = ScalarValue::try_from_array(values.as_ref(), index).ok()?;
                    if scalar.is_null() {
                        return None;
                    }
                    scalar_to_array_datum(&scalar, element_type)
                })
                .collect()
        }
        _ => None,
    }
}

fn scalar_to_array_datum(scalar: &ScalarValue, element_type: &DataType) -> Option<Datum> {
    if let (DataType::Float(_), ScalarValue::Float64(Some(value))) = (element_type, scalar) {
        let narrowed = *value as f32;
        return ((narrowed as f64).to_bits() == value.to_bits()).then_some(Datum::Float(narrowed));
    }
    scalar_to_datum(scalar, element_type)
}

fn arrow_list_element_type(data_type: &ArrowDataType) -> Option<&ArrowDataType> {
    match data_type {
        ArrowDataType::List(field) => Some(field.data_type()),
        _ => None,
    }
}

fn is_lossless_array_element_cast(source: &DataType, target: &ArrowDataType) -> bool {
    if paimon::arrow::paimon_type_to_arrow(source).ok().as_ref() == Some(target) {
        return true;
    }
    matches!(
        (source, target),
        (
            DataType::TinyInt(_),
            ArrowDataType::Int16 | ArrowDataType::Int32 | ArrowDataType::Int64
        ) | (
            DataType::SmallInt(_),
            ArrowDataType::Int32 | ArrowDataType::Int64
        ) | (DataType::Int(_), ArrowDataType::Int64)
            | (DataType::Float(_), ArrowDataType::Float64)
    )
}

fn is_lossless_arrow_scalar_cast(source: &ArrowDataType, target: &ArrowDataType) -> bool {
    source == target
        || matches!(
            (source, target),
            (
                ArrowDataType::Int8,
                ArrowDataType::Int16 | ArrowDataType::Int32 | ArrowDataType::Int64
            ) | (
                ArrowDataType::Int16,
                ArrowDataType::Int32 | ArrowDataType::Int64
            ) | (ArrowDataType::Int32, ArrowDataType::Int64)
                | (ArrowDataType::Float32, ArrowDataType::Float64)
        )
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

pub(crate) fn scalar_to_datum(scalar: &ScalarValue, data_type: &DataType) -> Option<Datum> {
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
        ArrayType, BigIntType, FloatType, IntType, LocalZonedTimestampType, PredicateOperator,
        Schema, SmallIntType, TableSchema, TimeType, TimestampType, VarCharType,
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
                .column(
                    "items",
                    DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
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
    fn test_translate_datafusion_array_membership_functions() {
        use datafusion::functions_nested::expr_fn::{
            array_has, array_has_all, array_has_any, make_array,
        };

        let column = Expr::Column(Column::from_name("items"));
        let cases = [
            (
                array_has(column.clone(), lit(2)),
                PredicateOperator::ArrayContains,
                vec![Datum::Int(2)],
            ),
            (
                array_has_any(column.clone(), make_array(vec![lit(1), lit(3)])),
                PredicateOperator::ArraysOverlap,
                vec![Datum::Int(1), Datum::Int(3)],
            ),
            (
                array_has_all(column, make_array(vec![lit(2), lit(2), lit(4)])),
                PredicateOperator::ArrayContainsAll,
                vec![Datum::Int(2), Datum::Int(2), Datum::Int(4)],
            ),
        ];

        let fields = test_fields();
        for (filter, expected_op, expected_literals) in cases {
            let predicate = build_pushed_predicate(&[filter], &fields)
                .expect("array membership function should translate");
            assert!(matches!(
                predicate,
                Predicate::Leaf { op, literals, .. }
                    if op == expected_op && literals == expected_literals
            ));
        }
    }

    #[test]
    fn test_empty_array_has_all_falls_open_for_datafusion_null_semantics() {
        use datafusion::functions_nested::expr_fn::{array_has_all, make_array};

        let filter = array_has_all(
            Expr::Column(Column::from_name("items")),
            make_array(Vec::<Expr>::new()),
        );
        let fields = test_fields();

        assert!(build_pushed_predicate(std::slice::from_ref(&filter), &fields).is_none());
        assert_eq!(
            classify_filter_pushdown(&filter, &fields, true, is_exact_filter_pushdown),
            TableProviderFilterPushDown::Unsupported
        );
    }

    #[test]
    fn test_float_array_membership_keeps_datafusion_residual() {
        use datafusion::functions_nested::expr_fn::array_has;

        let fields = vec![DataField::new(
            1,
            "items".to_string(),
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        )];
        let filter = array_has(
            Expr::Column(Column::from_name("items")),
            lit(f32::from_bits(0x7fc0_1234)),
        );
        let analysis = analyze_filters(std::slice::from_ref(&filter), &fields, true);

        assert!(analysis.pushed_predicate.is_some());
        assert!(analysis.requires_residual);
        assert_eq!(
            classify_filter_pushdown(&filter, &fields, true, |_| true),
            TableProviderFilterPushDown::Inexact
        );
    }

    #[test]
    fn test_negated_inexact_float_array_membership_falls_open() {
        use datafusion::functions_nested::expr_fn::array_has;

        let fields = vec![DataField::new(
            1,
            "items".to_string(),
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        )];
        let filter = Expr::Not(Box::new(array_has(
            Expr::Column(Column::from_name("items")),
            lit(f32::from_bits(0xffc0_1234)),
        )));

        // Paimon/Java considers every NaN payload equal, while DataFusion
        // distinguishes payloads. Negating that inexact positive predicate
        // would turn its safe superset into a subset and silently drop rows.
        assert!(build_pushed_predicate(std::slice::from_ref(&filter), &fields).is_none());
        assert_eq!(
            classify_filter_pushdown(&filter, &fields, true, |_| true),
            TableProviderFilterPushDown::Unsupported
        );
    }

    #[test]
    fn test_translate_analyzer_inserted_array_widening_casts() {
        use datafusion::arrow::datatypes::DataType as ArrowDataType;
        use datafusion::functions_nested::expr_fn::{array_has, array_has_any, make_array};
        use datafusion::logical_expr::Cast;

        let fields = test_fields();
        let widened_items = Expr::Cast(Cast::new(
            Box::new(Expr::Column(Column::from_name("items"))),
            ArrowDataType::new_list(ArrowDataType::Int64, true),
        ));
        let cases = [
            (
                array_has(widened_items.clone(), lit(2_i64)),
                PredicateOperator::ArrayContains,
                vec![Datum::Int(2)],
            ),
            (
                array_has_any(widened_items, make_array(vec![lit(1_i64), lit(3_i64)])),
                PredicateOperator::ArraysOverlap,
                vec![Datum::Int(1), Datum::Int(3)],
            ),
        ];

        for (filter, expected_op, expected_literals) in cases {
            let predicate = build_pushed_predicate(&[filter], &fields)
                .expect("lossless analyzer-inserted widening cast should translate");
            assert!(matches!(
                predicate,
                Predicate::Leaf { op, literals, .. }
                    if op == expected_op && literals == expected_literals
            ));
        }

        let out_of_range = array_has(
            Expr::Cast(Cast::new(
                Box::new(Expr::Column(Column::from_name("items"))),
                ArrowDataType::new_list(ArrowDataType::Int64, true),
            )),
            lit(i64::MAX),
        );
        assert!(build_pushed_predicate(&[out_of_range], &fields).is_none());

        let fixed_size_cast = array_has(
            Expr::Cast(Cast::new(
                Box::new(Expr::Column(Column::from_name("items"))),
                ArrowDataType::new_fixed_size_list(ArrowDataType::Int64, 2, true),
            )),
            lit(2_i64),
        );
        assert!(build_pushed_predicate(&[fixed_size_cast], &fields).is_none());

        let non_nullable_elements = array_has(
            Expr::Cast(Cast::new(
                Box::new(Expr::Column(Column::from_name("items"))),
                ArrowDataType::new_list(ArrowDataType::Int64, false),
            )),
            lit(2_i64),
        );
        assert!(build_pushed_predicate(&[non_nullable_elements], &fields).is_none());

        let fixed_size_literals = array_has_any(
            Expr::Column(Column::from_name("items")),
            Expr::Cast(Cast::new(
                Box::new(make_array(vec![lit(1_i32), lit(3_i32), lit(5_i32)])),
                ArrowDataType::new_fixed_size_list(ArrowDataType::Int32, 2, true),
            )),
        );
        assert!(build_pushed_predicate(&[fixed_size_literals], &fields).is_none());

        let long_fields = vec![DataField::new(
            1,
            "items".to_string(),
            DataType::Array(ArrayType::new(DataType::BigInt(BigIntType::new()))),
        )];
        let widened_literal = Expr::Cast(Cast::new(Box::new(lit(2_i32)), ArrowDataType::Int64));
        let predicate = build_pushed_predicate(
            &[array_has(
                Expr::Column(Column::from_name("items")),
                widened_literal,
            )],
            &long_fields,
        )
        .expect("losslessly widened scalar literal should translate");
        assert!(matches!(
            predicate,
            Predicate::Leaf { literals, .. } if literals == vec![Datum::Long(2)]
        ));

        let float_fields = vec![DataField::new(
            1,
            "items".to_string(),
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        )];
        let widened_floats = Expr::Cast(Cast::new(
            Box::new(Expr::Column(Column::from_name("items"))),
            ArrowDataType::new_list(ArrowDataType::Float64, true),
        ));
        assert!(build_pushed_predicate(
            &[array_has(widened_floats.clone(), lit(1.0_f64))],
            &float_fields,
        )
        .is_some());
        assert!(
            build_pushed_predicate(&[array_has(widened_floats, lit(1.1_f64))], &float_fields)
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_translate_array_membership_after_datafusion_sql_analysis() {
        use datafusion::arrow::datatypes::{
            DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
        };
        use datafusion::datasource::empty::EmptyTable;
        use datafusion::logical_expr::LogicalPlan;
        use datafusion::prelude::SessionContext;
        use std::sync::Arc;

        fn filter_expr(plan: &LogicalPlan) -> Option<&Expr> {
            match plan {
                LogicalPlan::Filter(filter) => Some(&filter.predicate),
                LogicalPlan::TableScan(scan) => scan.filters.first(),
                other => other.inputs().into_iter().find_map(filter_expr),
            }
        }

        let ctx = SessionContext::new();
        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "items",
            ArrowDataType::new_list(ArrowDataType::Int32, true),
            true,
        )]));
        ctx.register_table("t", Arc::new(EmptyTable::new(arrow_schema)))
            .unwrap();
        let fields = vec![DataField::new(
            1,
            "items".to_string(),
            DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
        )];
        let cases = [
            (
                "SELECT * FROM t WHERE array_has(items, 2)",
                PredicateOperator::ArrayContains,
                vec![Datum::Int(2)],
            ),
            (
                "SELECT * FROM t WHERE array_has_any(items, [1, 3])",
                PredicateOperator::ArraysOverlap,
                vec![Datum::Int(1), Datum::Int(3)],
            ),
            (
                "SELECT * FROM t WHERE array_has_all(items, [1, 3])",
                PredicateOperator::ArrayContainsAll,
                vec![Datum::Int(1), Datum::Int(3)],
            ),
        ];
        for (sql, expected_op, expected_literals) in cases {
            let plan = ctx.state().create_logical_plan(sql).await.unwrap();
            let plan = ctx.state().optimize(&plan).unwrap();
            let filter = filter_expr(&plan).expect("optimized plan should retain the filter");
            assert!(filter.to_string().contains("CAST"));

            let predicate = build_pushed_predicate(std::slice::from_ref(filter), &fields)
                .expect("analyzed SQL array predicate should translate");
            assert!(matches!(
                predicate,
                Predicate::Leaf { op, literals, .. }
                    if op == expected_op && literals == expected_literals
            ));
        }

        let numeric_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new(
                "small_items",
                ArrowDataType::new_list(ArrowDataType::Int16, true),
                true,
            ),
            ArrowField::new(
                "float_items",
                ArrowDataType::new_list(ArrowDataType::Float32, true),
                true,
            ),
        ]));
        ctx.register_table("numeric_arrays", Arc::new(EmptyTable::new(numeric_schema)))
            .unwrap();
        let numeric_fields = vec![
            DataField::new(
                1,
                "small_items".to_string(),
                DataType::Array(ArrayType::new(DataType::SmallInt(SmallIntType::new()))),
            ),
            DataField::new(
                2,
                "float_items".to_string(),
                DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
            ),
        ];
        for sql in [
            "SELECT * FROM numeric_arrays WHERE array_has(small_items, 2)",
            "SELECT * FROM numeric_arrays WHERE array_has(float_items, 1.0)",
        ] {
            let plan = ctx.state().create_logical_plan(sql).await.unwrap();
            let plan = ctx.state().optimize(&plan).unwrap();
            let filter = filter_expr(&plan).expect("optimized plan should retain the filter");
            assert!(filter.to_string().contains("CAST"));
            assert!(
                build_pushed_predicate(std::slice::from_ref(filter), &numeric_fields).is_some(),
                "analyzed numeric ARRAY predicate should translate: {filter}"
            );
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
