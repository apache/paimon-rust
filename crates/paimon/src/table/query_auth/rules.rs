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

//! The server's row filter, parsed against the schema it ruled on and applied
//! to read batches, as Java `TableQueryAuthResult` does.

use super::unsupported;
use crate::api::AuthTableQueryResponse;
use crate::arrow::residual::{evaluate_exact_leaf_predicate, sanitize_filter_mask};
use crate::spec::{DataField, Predicate};
use crate::Result;
use arrow_array::{Array, ArrayRef, BooleanArray, Float32Array, Float64Array, RecordBatch};
use std::collections::HashSet;
use std::sync::Arc;

/// The rules of one grant; leaf indices are table-schema positions.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct Rules {
    pub(crate) filters: Vec<Predicate>,
}

impl Rules {
    /// Validated like Java `validateAgainstSchema`: anything unknown fails closed.
    pub(crate) fn parse(response: &AuthTableQueryResponse, fields: &[DataField]) -> Result<Self> {
        let mut filters = Vec::new();
        for json in response.filter.iter().flatten() {
            if json.is_empty() {
                return Err(unsupported("the server sent an empty row filter"));
            }
            let filter =
                Predicate::from_rest_json(json, fields).map_err(
                    |e| match mentioned_system_column(json) {
                        Some(name) => unsupported(&format!(
                            "the server's row filter reads the system column '{name}', which a \
                         query-auth read never projects"
                        )),
                        None => unsupported(&format!("cannot parse the server's row filter: {e}")),
                    },
                )?;
            filters.push(filter);
        }

        // Not applied yet: refusing beats returning raw values.
        if response
            .column_masking
            .as_ref()
            .is_some_and(|masks| !masks.is_empty())
        {
            return Err(unsupported("this client does not apply column masking yet"));
        }
        Ok(Self { filters })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.filters.is_empty()
    }

    /// Table-schema indices the row filter reads.
    pub(crate) fn filter_columns(&self) -> HashSet<usize> {
        let mut out = HashSet::new();
        for filter in &self.filters {
            filter.collect_leaf_field_indices(&mut out);
        }
        out
    }
}

fn mentioned_system_column(json: &str) -> Option<&'static str> {
    [
        crate::spec::ROW_ID_FIELD_NAME,
        crate::spec::SEQUENCE_NUMBER_FIELD_NAME,
        crate::spec::VALUE_KIND_FIELD_NAME,
        crate::spec::ROW_KIND_FIELD_NAME,
    ]
    .into_iter()
    .find(|name| json.contains(name))
}

// ---------------------------------------------------------------------------
// Row filter
// ---------------------------------------------------------------------------

/// Keeps the rows passing every filter. `batch` columns pair 1:1 with
/// `batch_fields`; leaf indices point into `schema_fields`. Anything that
/// cannot be evaluated is an error, never a kept row.
pub(crate) fn filter_batch(
    batch: &RecordBatch,
    filters: &[Predicate],
    schema_fields: &[DataField],
    batch_fields: &[DataField],
) -> Result<RecordBatch> {
    let mut keep: Option<BooleanArray> = None;
    for filter in filters {
        let mask = rule_mask(batch, filter, schema_fields, batch_fields)?;
        keep = Some(match keep {
            Some(existing) => combine(&existing, &mask, false)?,
            None => mask,
        });
    }
    let Some(keep) = keep else {
        return Ok(batch.clone());
    };
    arrow_select::filter::filter_record_batch(batch, &keep).map_err(|e| eval_err(&e))
}

/// Java leaf semantics: a null value fails every leaf but `IS NULL`, for the
/// negated functions too. The REST parser builds no `NOT`, so two-valued
/// AND/OR over those leaves is exactly Java's `test`.
fn rule_mask(
    batch: &RecordBatch,
    predicate: &Predicate,
    schema_fields: &[DataField],
    batch_fields: &[DataField],
) -> Result<BooleanArray> {
    match predicate {
        Predicate::AlwaysTrue => Ok(BooleanArray::from(vec![true; batch.num_rows()])),
        Predicate::AlwaysFalse => Ok(BooleanArray::from(vec![false; batch.num_rows()])),
        Predicate::And(children) | Predicate::Or(children) => {
            let or = matches!(predicate, Predicate::Or(_));
            let mut combined: Option<BooleanArray> = None;
            for child in children {
                let mask = rule_mask(batch, child, schema_fields, batch_fields)?;
                combined = Some(match combined {
                    Some(existing) => combine(&existing, &mask, or)?,
                    None => mask,
                });
            }
            combined.ok_or_else(|| unsupported("the server's row filter has an empty AND/OR"))
        }
        // Its null semantics differ from Java's negated leaves.
        Predicate::Not(_) => Err(unsupported("the server's row filter has a NOT")),
        Predicate::Leaf {
            index,
            op,
            literals,
            ..
        } => {
            let field = schema_fields
                .get(*index)
                .ok_or_else(|| unsupported("the server's row filter reads an unknown column"))?;
            let column = column_of(batch, field, batch_fields)?;
            let column = canonicalize_nan(&column);
            let mask = evaluate_exact_leaf_predicate(&column, field.data_type(), *op, literals)
                .map_err(|e| eval_err(&e))?;
            Ok(sanitize_filter_mask(mask))
        }
    }
}

fn combine(left: &BooleanArray, right: &BooleanArray, or: bool) -> Result<BooleanArray> {
    let combined = if or {
        arrow_arith::boolean::or(left, right)
    } else {
        arrow_arith::boolean::and(left, right)
    };
    combined.map_err(|e| eval_err(&e))
}

/// The column holding `field`, matched by id and name so an alias cannot stand in.
fn column_of(
    batch: &RecordBatch,
    field: &DataField,
    batch_fields: &[DataField],
) -> Result<ArrayRef> {
    let position = batch_fields
        .iter()
        .position(|f| f.id() == field.id() && f.name() == field.name())
        .ok_or_else(|| {
            unsupported(&format!(
                "the read does not carry '{}', which the server's rules need",
                field.name()
            ))
        })?;
    let column = batch.column(position);
    let declared = crate::arrow::paimon_type_to_arrow(field.data_type())?;
    if column.data_type() == &declared {
        return Ok(Arc::clone(column));
    }
    arrow_cast::cast(column, &declared).map_err(|e| eval_err(&e))
}

/// Java's `Float.compare` puts every NaN above every number; Arrow's total
/// order puts a negative NaN below them, so `f < 0` would admit it.
fn canonicalize_nan(column: &ArrayRef) -> ArrayRef {
    match column.data_type() {
        arrow_schema::DataType::Float32 => match column.as_any().downcast_ref::<Float32Array>() {
            Some(values) if values.iter().any(|v| v.is_some_and(f32::is_nan)) => {
                Arc::new(values.unary::<_, arrow_array::types::Float32Type>(|v| {
                    if v.is_nan() {
                        f32::NAN
                    } else {
                        v
                    }
                }))
            }
            _ => Arc::clone(column),
        },
        arrow_schema::DataType::Float64 => match column.as_any().downcast_ref::<Float64Array>() {
            Some(values) if values.iter().any(|v| v.is_some_and(f64::is_nan)) => {
                Arc::new(values.unary::<_, arrow_array::types::Float64Type>(|v| {
                    if v.is_nan() {
                        f64::NAN
                    } else {
                        v
                    }
                }))
            }
            _ => Arc::clone(column),
        },
        _ => Arc::clone(column),
    }
}

fn eval_err(e: &dyn std::fmt::Display) -> crate::Error {
    unsupported(&format!("cannot evaluate the server's rules: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{
        BigIntType, BooleanType, DataType, DateType, Datum, DecimalType, DoubleType, FloatType,
        IntType, PredicateBuilder, PredicateOperator, TimestampType, VarBinaryType, VarCharType,
    };
    use arrow_array::{
        Date32Array, Decimal128Array, Int32Array, Int64Array, RecordBatchOptions, StringArray,
        TimestampMillisecondArray,
    };
    use arrow_schema::{Field, Schema};

    const NULL: &str = r#"{"name":"NULL"}"#;

    fn field(id: i32, name: &str, data_type: DataType) -> DataField {
        DataField::new(id, name.to_string(), data_type)
    }

    fn string() -> DataType {
        DataType::VarChar(VarCharType::string_type())
    }

    fn batch(fields: &[DataField], columns: Vec<ArrayRef>) -> RecordBatch {
        let schema: Vec<Field> = fields
            .iter()
            .map(|f| {
                let data_type = crate::arrow::paimon_type_to_arrow(f.data_type()).unwrap();
                Field::new(f.name(), data_type, f.data_type().is_nullable())
            })
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(schema)), columns).unwrap()
    }

    fn parse(filters: &[&str], masks: &[(&str, &str)], fields: &[DataField]) -> Result<Rules> {
        let response = AuthTableQueryResponse {
            filter: Some(filters.iter().map(|f| f.to_string()).collect()),
            column_masking: Some(
                masks
                    .iter()
                    .map(|(column, json)| (column.to_string(), json.to_string()))
                    .collect(),
            ),
        };
        Rules::parse(&response, fields)
    }

    fn refusal<T: std::fmt::Debug>(result: Result<T>) -> String {
        match result {
            Err(crate::Error::Unsupported { message }) => message,
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    /// Java `LeafPredicate` JSON; the field resolves by name and the local type wins.
    fn leaf(function: &str, field: &str, literals: &str) -> String {
        format!(
            r#"{{"kind":"LEAF","transform":{{"name":"FIELD_REF","fieldRef":{{"index":0,"name":"{field}","type":"INT"}}}},"function":"{function}","literals":{literals}}}"#
        )
    }

    fn compound(function: &str, children: &[String]) -> String {
        format!(
            r#"{{"kind":"COMPOUND","function":"{function}","children":[{}]}}"#,
            children.join(",")
        )
    }

    fn constant(function: &str) -> String {
        format!(
            r#"{{"kind":"LEAF","transform":{{"name":"NULL"}},"function":"{function}","literals":[]}}"#
        )
    }

    fn ids(batch: &RecordBatch) -> Vec<i32> {
        let ids = batch.column(0).as_any().downcast_ref::<Int32Array>();
        ids.unwrap().values().to_vec()
    }

    // ---------------------------------------------------------------------
    // Parsing
    // ---------------------------------------------------------------------

    fn rule_fields() -> Vec<DataField> {
        let bytes = VarBinaryType::new(VarBinaryType::MAX_LENGTH).unwrap();
        let not_null = VarCharType::with_nullable(false, VarCharType::MAX_LENGTH).unwrap();
        vec![
            field(0, "id", DataType::Int(IntType::new())),
            field(1, "name", string()),
            field(2, "alias", string()),
            field(3, "bin", DataType::VarBinary(bytes)),
            field(4, "nn", DataType::VarChar(not_null)),
        ]
    }

    #[test]
    fn test_parse_reads_the_rules() {
        let fields = rule_fields();
        let rules = parse(
            &[
                &leaf("GREATER_THAN", "id", "[1]"),
                &leaf("IS_NOT_NULL", "alias", "[]"),
            ],
            &[],
            &fields,
        )
        .unwrap();
        assert_eq!(rules.filters.len(), 2);
        assert_eq!(rules.filter_columns(), HashSet::from([0, 2]));

        let none = AuthTableQueryResponse::default();
        assert!(Rules::parse(&none, &fields).unwrap().is_empty());
        assert!(parse(&[], &[], &fields).unwrap().is_empty());
    }

    #[test]
    fn test_any_mask_is_refused() {
        let message = refusal(parse(&[], &[("name", NULL)], &rule_fields()));
        assert!(message.contains("column masking"), "{message}");
    }

    #[test]
    fn test_parse_fails_closed() {
        let fields = rule_fields();
        for (filter, reason) in [
            (String::new(), "empty row filter"),
            (" ".to_string(), "cannot parse"),
            ("null".to_string(), "cannot parse"),
            ("{".to_string(), "cannot parse"),
            (leaf("EQUAL", "missing", "[1]"), "unknown field `missing`"),
            (leaf("EQUAL", "id", r#"["one"]"#), "cannot parse"),
            (compound("AND", &[]), "empty children"),
        ] {
            let message = refusal(parse(&[&filter], &[], &fields));
            assert!(message.contains(reason), "{filter:?}: {message}");
        }
        // One bad entry spoils the list.
        let good = leaf("EQUAL", "id", "[1]");
        assert!(refusal(parse(&[&good, ""], &[], &fields)).contains("empty row filter"));
    }

    #[test]
    fn test_a_filter_on_a_system_column_is_refused() {
        let fields = rule_fields();
        for column in ["_ROW_ID", "_SEQUENCE_NUMBER", "_VALUE_KIND", "rowkind"] {
            let message = refusal(parse(&[&leaf("EQUAL", column, "[1]")], &[], &fields));
            assert!(
                message.contains(&format!("system column '{column}'")),
                "{message}"
            );
        }
    }

    // ---------------------------------------------------------------------
    // Row filter
    // ---------------------------------------------------------------------

    /// `k` names the row; every other column is null in the last one.
    fn typed_fields() -> Vec<DataField> {
        vec![
            field(0, "k", DataType::Int(IntType::with_nullable(false))),
            field(1, "i", DataType::Int(IntType::new())),
            field(2, "b", DataType::BigInt(BigIntType::new())),
            field(3, "d", DataType::Double(DoubleType::new())),
            field(4, "f", DataType::Float(FloatType::new())),
            field(5, "s", string()),
            field(6, "m", DataType::Decimal(DecimalType::new(5, 2).unwrap())),
            field(7, "dt", DataType::Date(DateType::new())),
            field(8, "ts", DataType::Timestamp(TimestampType::new(3).unwrap())),
            field(9, "flag", DataType::Boolean(BooleanType::new())),
        ]
    }

    /// Four values, then a null.
    fn with_null<T>(values: [T; 4]) -> Vec<Option<T>> {
        values.into_iter().map(Some).chain([None]).collect()
    }

    fn typed_batch() -> RecordBatch {
        let decimals = Decimal128Array::from(with_null([110, 200, 250, 1000]));
        batch(
            &typed_fields(),
            vec![
                Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4])),
                Arc::new(Int32Array::from(with_null([1, 2, 3, 4]))),
                Arc::new(Int64Array::from(with_null([10, 20, 30, 40]))),
                Arc::new(Float64Array::from(with_null([-1.5, 0.0, -0.0, f64::NAN]))),
                Arc::new(Float32Array::from(with_null([-1.5, 0.0, -0.0, f32::NAN]))),
                Arc::new(StringArray::from(with_null([
                    "apple", "apricot", "banana", "a_c",
                ]))),
                Arc::new(decimals.with_precision_and_scale(5, 2).unwrap()),
                Arc::new(Date32Array::from(with_null([19000, 19001, 19002, 19003]))),
                Arc::new(TimestampMillisecondArray::from(with_null([
                    1000, 2000, 3000, 4000,
                ]))),
                Arc::new(BooleanArray::from(with_null([true, false, true, false]))),
            ],
        )
    }

    /// The rows of the typed batch that `filters` keep.
    fn kept(filters: &[Predicate]) -> Vec<i32> {
        let fields = typed_fields();
        ids(&filter_batch(&typed_batch(), filters, &fields, &fields).unwrap())
    }

    #[test]
    fn test_filter_matrix_follows_java_leaf_semantics() {
        let fields = typed_fields();
        // A null value fails every leaf but IS_NULL, the negated ones too.
        let cases = [
            (leaf("EQUAL", "i", "[2]"), vec![1]),
            (leaf("NOT_EQUAL", "i", "[2]"), vec![0, 2, 3]),
            (leaf("LESS_THAN", "i", "[3]"), vec![0, 1]),
            (leaf("GREATER_OR_EQUAL", "i", "[3]"), vec![2, 3]),
            (leaf("IN", "i", "[1,3,null]"), vec![0, 2]),
            (leaf("NOT_IN", "i", "[1,3]"), vec![1, 3]),
            (leaf("NOT_IN", "i", "[1,null]"), vec![]),
            (leaf("BETWEEN", "i", "[2,3]"), vec![1, 2]),
            (leaf("NOT_BETWEEN", "i", "[2,3]"), vec![0, 3]),
            (leaf("IS_NULL", "i", "[]"), vec![4]),
            (leaf("IS_NOT_NULL", "i", "[]"), vec![0, 1, 2, 3]),
            (leaf("EQUAL", "i", "[null]"), vec![]),
            (leaf("GREATER_OR_EQUAL", "b", "[30]"), vec![2, 3]),
            (leaf("NOT_IN", "b", "[10,40]"), vec![1, 2]),
            (leaf("BETWEEN", "b", "[15,35]"), vec![1, 2]),
            // `Double.compare`: -0.0 sorts below 0.0, NaN above every number.
            (leaf("EQUAL", "d", "[0.0]"), vec![1]),
            (leaf("EQUAL", "d", "[-0.0]"), vec![2]),
            (leaf("NOT_EQUAL", "d", "[0.0]"), vec![0, 2, 3]),
            (leaf("LESS_THAN", "d", "[0.0]"), vec![0, 2]),
            (leaf("GREATER_OR_EQUAL", "d", "[0.0]"), vec![1, 3]),
            (leaf("IN", "d", "[0.0,-1.5]"), vec![0, 1]),
            (leaf("NOT_IN", "d", "[0.0]"), vec![0, 2, 3]),
            (leaf("BETWEEN", "d", "[-1.0,1.0]"), vec![1, 2]),
            (leaf("NOT_BETWEEN", "d", "[-1.0,1.0]"), vec![0, 3]),
            (leaf("EQUAL", "f", "[-1.5]"), vec![0]),
            (leaf("NOT_EQUAL", "f", "[-0.0]"), vec![0, 1, 3]),
            (leaf("LESS_THAN", "f", "[0.0]"), vec![0, 2]),
            (leaf("GREATER_OR_EQUAL", "f", "[0.0]"), vec![1, 3]),
            (leaf("EQUAL", "s", r#"["banana"]"#), vec![2]),
            (leaf("NOT_EQUAL", "s", r#"["banana"]"#), vec![0, 1, 3]),
            (leaf("LESS_THAN", "s", r#"["apricot"]"#), vec![0, 3]),
            (leaf("GREATER_OR_EQUAL", "s", r#"["apricot"]"#), vec![1, 2]),
            (leaf("IN", "s", r#"["apple","banana"]"#), vec![0, 2]),
            (leaf("NOT_IN", "s", r#"["apple"]"#), vec![1, 2, 3]),
            (leaf("BETWEEN", "s", r#"["apple","b"]"#), vec![0, 1]),
            (leaf("NOT_BETWEEN", "s", r#"["apple","b"]"#), vec![2, 3]),
            (leaf("STARTS_WITH", "s", r#"["ap"]"#), vec![0, 1]),
            (leaf("LIKE", "s", r#"["%an%"]"#), vec![2]),
            (leaf("LIKE", "s", r#"["a_p%"]"#), vec![0]),
            (leaf("LIKE", "s", r#"["a\\_c"]"#), vec![3]),
            (leaf("IS_NULL", "s", "[]"), vec![4]),
            (leaf("EQUAL", "flag", "[true]"), vec![0, 2]),
            (leaf("NOT_EQUAL", "flag", "[true]"), vec![1, 3]),
            (leaf("IN", "flag", "[false]"), vec![1, 3]),
            (leaf("IS_NOT_NULL", "flag", "[]"), vec![0, 1, 2, 3]),
        ];
        for (json, expected) in cases {
            let rules = parse(&[&json], &[], &fields).unwrap();
            assert_eq!(kept(&rules.filters), expected, "{json}");
        }
    }

    #[test]
    fn test_filter_compares_decimals_dates_and_timestamps_by_value() {
        // The REST parser takes no such literal, so these leaves are built directly.
        let fields = typed_fields();
        let b = PredicateBuilder::new(&fields);
        let dec = |unscaled, scale| Datum::Decimal {
            unscaled,
            precision: 10,
            scale,
        };
        let ts = |millis, nanos| Datum::Timestamp { millis, nanos };
        let cases = [
            (b.equal("m", dec(2, 0)), vec![1]),
            (b.equal("m", dec(20, 1)), vec![1]),
            (b.not_equal("m", dec(25, 1)), vec![0, 1, 3]),
            (b.greater_than("m", dec(2001, 3)), vec![2, 3]),
            (b.less_or_equal("m", dec(11, 1)), vec![0]),
            (b.between("m", dec(11, 1), dec(25, 1)), vec![0, 1, 2]),
            (b.not_between("m", dec(11, 1), dec(25, 1)), vec![3]),
            (b.is_in("m", vec![dec(10, 0), dec(1100, 3)]), vec![0, 3]),
            (b.is_not_in("m", vec![dec(2, 0)]), vec![0, 2, 3]),
            (b.greater_than("dt", Datum::Date(19001)), vec![2, 3]),
            (
                b.between("dt", Datum::Date(19000), Datum::Date(19001)),
                vec![0, 1],
            ),
            (b.not_equal("dt", Datum::Date(19000)), vec![1, 2, 3]),
            (
                b.is_in("dt", vec![Datum::Date(19000), Datum::Date(19003)]),
                vec![0, 3],
            ),
            (b.is_null("dt"), vec![4]),
            (b.less_than("ts", ts(2000, 0)), vec![0]),
            // One nanosecond past a millisecond is not rounded away.
            (b.greater_or_equal("ts", ts(2000, 1)), vec![2, 3]),
            (b.between("ts", ts(1500, 0), ts(3000, 0)), vec![1, 2]),
            (b.not_between("ts", ts(1500, 0), ts(3000, 0)), vec![0, 3]),
            (b.is_not_in("ts", vec![ts(1000, 0)]), vec![1, 2, 3]),
        ];
        for (predicate, expected) in cases {
            let predicate = predicate.unwrap();
            assert_eq!(
                kept(std::slice::from_ref(&predicate)),
                expected,
                "{predicate}"
            );
        }
    }

    #[test]
    fn test_every_nan_sorts_above_every_number() {
        let fields = vec![
            field(0, "k", DataType::Int(IntType::with_nullable(false))),
            field(1, "d", DataType::Double(DoubleType::new())),
            field(2, "f", DataType::Float(FloatType::new())),
        ];
        // Arrow's total order alone would put the sign-bit NaN below every number.
        let rows = batch(
            &fields,
            vec![
                Arc::new(Int32Array::from(vec![0, 1, 2, 3])),
                Arc::new(Float64Array::from(vec![-f64::NAN, f64::NAN, -1.0, 1.0])),
                Arc::new(Float32Array::from(vec![-f32::NAN, f32::NAN, -1.0, 1.0])),
            ],
        );
        for column in ["d", "f"] {
            for (function, literals, expected) in [
                ("LESS_THAN", "[0.0]", vec![2]),
                ("GREATER_THAN", "[0.0]", vec![0, 1, 3]),
                ("GREATER_THAN", "[1.0]", vec![0, 1]),
                ("NOT_BETWEEN", "[-2.0,2.0]", vec![0, 1]),
            ] {
                let json = leaf(function, column, literals);
                let rules = parse(&[&json], &[], &fields).unwrap();
                let out = filter_batch(&rows, &rules.filters, &fields, &fields).unwrap();
                assert_eq!(ids(&out), expected, "{json}");
            }
        }
    }

    #[test]
    fn test_filters_combine_with_and_or() {
        let fields = typed_fields();
        let i_is = |value: &str| leaf("EQUAL", "i", &format!("[{value}]"));
        let cases = [
            (
                vec![compound(
                    "OR",
                    &[i_is("1"), leaf("EQUAL", "s", r#"["banana"]"#)],
                )],
                vec![0, 2],
            ),
            (
                vec![compound(
                    "AND",
                    &[
                        leaf("GREATER_OR_EQUAL", "b", "[20]"),
                        leaf("EQUAL", "flag", "[true]"),
                    ],
                )],
                vec![2],
            ),
            // A null fails NOT_EQUAL but passes IS_NULL.
            (
                vec![compound(
                    "OR",
                    &[leaf("NOT_EQUAL", "i", "[2]"), leaf("IS_NULL", "s", "[]")],
                )],
                vec![0, 2, 3, 4],
            ),
            (
                vec![compound(
                    "AND",
                    &[
                        compound("OR", &[i_is("1"), i_is("4")]),
                        leaf("NOT_EQUAL", "s", r#"["apple"]"#),
                    ],
                )],
                vec![3],
            ),
            // The server's entries are ANDed.
            (
                vec![
                    leaf("GREATER_THAN", "i", "[1]"),
                    leaf("EQUAL", "flag", "[true]"),
                ],
                vec![2],
            ),
            (vec![constant("TRUE")], vec![0, 1, 2, 3, 4]),
            (vec![constant("FALSE"), leaf("IS_NULL", "i", "[]")], vec![]),
        ];
        for (filters, expected) in cases {
            let filters: Vec<&str> = filters.iter().map(String::as_str).collect();
            let rules = parse(&filters, &[], &fields).unwrap();
            assert_eq!(kept(&rules.filters), expected, "{filters:?}");
        }
        assert_eq!(kept(&[]), vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_filter_refuses_what_it_cannot_evaluate() {
        let fields = typed_fields();
        let rows = typed_batch();
        let json = leaf("EQUAL", "i", "[1]");
        let filters = parse(&[&json], &[], &fields).unwrap().filters;

        // Carried under its id and its name, or not at all.
        for carried in [
            field(1, "renamed", DataType::Int(IntType::new())),
            field(99, "i", DataType::Int(IntType::new())),
        ] {
            let mut batch_fields = fields.clone();
            batch_fields[1] = carried;
            let message = refusal(filter_batch(&rows, &filters, &fields, &batch_fields));
            assert!(message.contains("does not carry 'i'"), "{message}");
        }
        let narrow = rows.project(&[0, 2]).unwrap();
        let narrow_fields = [fields[0].clone(), fields[2].clone()];
        let message = refusal(filter_batch(&narrow, &filters, &fields, &narrow_fields));
        assert!(message.contains("does not carry 'i'"), "{message}");
        // Any position will do.
        let reordered = rows.project(&[1, 0]).unwrap();
        let reordered_fields = [fields[1].clone(), fields[0].clone()];
        let out = filter_batch(&reordered, &filters, &fields, &reordered_fields).unwrap();
        assert_eq!(out.num_rows(), 1);

        let mistyped = Predicate::Leaf {
            column: "i".to_string(),
            index: 1,
            data_type: DataType::Int(IntType::new()),
            op: PredicateOperator::Eq,
            literals: vec![Datum::String("1".to_string())],
        };
        for (filter, reason) in [
            (Predicate::Not(Box::new(filters[0].clone())), "has a NOT"),
            (Predicate::And(Vec::new()), "empty AND/OR"),
            (mistyped, "cannot evaluate"),
        ] {
            let message = refusal(filter_batch(&rows, &[filter], &fields, &fields));
            assert!(message.contains(reason), "{message}");
        }
        let message = refusal(filter_batch(&rows, &filters, &fields[..1], &fields));
        assert!(message.contains("unknown column"), "{message}");
    }

    #[test]
    fn test_a_zero_column_batch_keeps_its_row_count() {
        let fields = rule_fields();
        let options = RecordBatchOptions::new().with_row_count(Some(3));
        let empty =
            RecordBatch::try_new_with_options(Arc::new(Schema::empty()), Vec::new(), &options)
                .unwrap();
        for (filter, rows) in [(constant("TRUE"), 3), (constant("FALSE"), 0)] {
            let rules = parse(&[&filter], &[], &fields).unwrap();
            let out = filter_batch(&empty, &rules.filters, &fields, &[]).unwrap();
            assert_eq!(out.num_rows(), rows, "{filter}");
        }
        assert_eq!(
            filter_batch(&empty, &[], &fields, &[]).unwrap().num_rows(),
            3
        );
    }
}
