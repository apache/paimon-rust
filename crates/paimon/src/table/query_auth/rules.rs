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

//! Apply the server's row filters and column masks to read batches.

use super::unsupported;
use crate::api::AuthTableQueryResponse;
use crate::arrow::residual::{evaluate_exact_leaf_predicate, sanitize_filter_mask};
use crate::spec::{DataField, Predicate, Transform, TransformInput};
use crate::Result;
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, BooleanArray, Float32Array, Float64Array, RecordBatch};
use std::collections::HashSet;
use std::sync::Arc;

/// Masks one column, by table-schema index.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ColumnMask {
    pub(crate) column: usize,
    pub(crate) transform: Transform,
}

/// One grant's rules, indexed by table schema.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct Rules {
    pub(crate) filters: Vec<Predicate>,
    pub(crate) masks: Vec<ColumnMask>,
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

        let mut masks = Vec::new();
        for (column, json) in response.column_masking.iter().flatten() {
            if column.is_empty() || json.is_empty() {
                return Err(unsupported("the server sent an empty column mask"));
            }
            // Unprojected system columns need no masking; `_KEY_` names are not exempt.
            if crate::spec::is_reserved_system_field_name(column) && !column.starts_with("_KEY_") {
                // Still reject malformed masks.
                let parsed = serde_json::from_str::<serde_json::Value>(json);
                if !parsed.is_ok_and(|value| !value.is_null()) {
                    return Err(unsupported(&format!(
                        "cannot parse the server's mask on '{column}'"
                    )));
                }
                continue;
            }
            let target = fields
                .iter()
                .position(|f| f.name() == column)
                .ok_or_else(|| {
                    unsupported(&format!(
                        "the server masks '{column}', which is not a column"
                    ))
                })?;
            let transform = Transform::from_rest_json(json, fields).map_err(|e| {
                unsupported(&format!(
                    "cannot parse the server's mask on '{column}': {e}"
                ))
            })?;
            check_mask_fits(&transform, &fields[target], fields)?;
            masks.push(ColumnMask {
                column: target,
                transform,
            });
        }
        masks.sort_by_key(|m| m.column);

        // Reading another masked column would expose its raw value.
        let targets: HashSet<usize> = masks.iter().map(|m| m.column).collect();
        for mask in &masks {
            if let Some(other) = mask_inputs(mask)
                .into_iter()
                .find(|i| *i != mask.column && targets.contains(i))
            {
                return Err(unsupported(&format!(
                    "the mask on '{}' reads '{}', which is masked too, so it would expose the \
                     raw value",
                    fields[mask.column].name(),
                    fields[other].name()
                )));
            }
        }
        Ok(Self { filters, masks })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.filters.is_empty() && self.masks.is_empty()
    }

    /// Table-schema indices of masked columns.
    pub(crate) fn masked_columns(&self) -> HashSet<usize> {
        self.masks.iter().map(|m| m.column).collect()
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

/// Table-schema indices a mask reads.
pub(crate) fn mask_inputs(mask: &ColumnMask) -> HashSet<usize> {
    let mut out = HashSet::new();
    mask.transform.collect_field_indices(&mut out);
    out
}

/// Preserve the target's type and nullability.
fn check_mask_fits(transform: &Transform, target: &DataField, fields: &[DataField]) -> Result<()> {
    let column_type = crate::arrow::paimon_type_to_arrow(target.data_type())?;
    if let Some(output) = mask_output_type(transform, fields) {
        if output != column_type {
            return Err(unsupported(&format!(
                "the mask on '{}' produces {output:?}, but the column is {column_type:?}",
                target.name()
            )));
        }
    }
    if !target.data_type().is_nullable() && mask_can_be_null(transform, fields) {
        return Err(unsupported(&format!(
            "the mask on '{}' can produce null, but the column is NOT NULL",
            target.name()
        )));
    }
    if let Transform::Cast(index, to) = transform {
        let from = fields[*index].data_type();
        if !cast_agrees_with_java(from, to) {
            return Err(unsupported(&format!(
                "the mask on '{}' casts {from:?} to {to:?}, which this client does not convert as \
                 Java does",
                target.name()
            )));
        }
    }
    Ok(())
}

/// Arrow and Java disagree on casts such as INT -> TIMESTAMP.
fn cast_agrees_with_java(from: &crate::spec::DataType, to: &crate::spec::DataType) -> bool {
    use crate::spec::DataType as Paimon;
    let string = |t: &Paimon| matches!(t, Paimon::Char(_) | Paimon::VarChar(_));
    let bytes = |t: &Paimon| matches!(t, Paimon::Binary(_) | Paimon::VarBinary(_));
    let integer_bits = |t: &Paimon| match t {
        Paimon::TinyInt(_) => Some(8),
        Paimon::SmallInt(_) => Some(16),
        Paimon::Int(_) => Some(32),
        Paimon::BigInt(_) => Some(64),
        _ => None,
    };
    let same = matches!(
        (from.copy_with_nullable(true), to.copy_with_nullable(true)),
        (Ok(from), Ok(to)) if from == to
    );
    let widening = matches!((integer_bits(from), integer_bits(to)), (Some(a), Some(b)) if a < b);
    same || widening
        || string(to)
            && (string(from) || integer_bits(from).is_some() || matches!(from, Paimon::Boolean(_)))
        || bytes(to) && (bytes(from) || string(from))
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

// Column masking

/// Replace every copy of a masked column using the original batch's values.
pub(crate) fn mask_batch(
    batch: &RecordBatch,
    masks: &[ColumnMask],
    schema_fields: &[DataField],
    batch_fields: &[DataField],
) -> Result<RecordBatch> {
    if masks.is_empty() {
        return Ok(batch.clone());
    }
    let input = |index: usize| -> Result<ArrayRef> {
        let field = schema_fields
            .get(index)
            .ok_or_else(|| unsupported("the server's mask reads an unknown column"))?;
        column_of(batch, field, batch_fields)
    };
    let mut columns = batch.columns().to_vec();
    for mask in masks {
        let field = &schema_fields[mask.column];
        // By id: the caller may project the column twice.
        let targets = batch_fields
            .iter()
            .enumerate()
            .filter_map(|(position, f)| (f.id() == field.id()).then_some(position));
        let Some(first) = targets.clone().next() else {
            return Err(unsupported(&format!(
                "the read does not carry '{}', which the server masks",
                field.name()
            )));
        };
        let target_type = batch.schema().field(first).data_type().clone();
        let masked: ArrayRef = match &mask.transform {
            Transform::Null => arrow_array::new_null_array(&target_type, batch.num_rows()),
            Transform::FieldRef(index) => input(*index)?,
            Transform::Cast(index, to) => {
                let cast = cast_to(&input(*index)?, &crate::arrow::paimon_type_to_arrow(to)?)?;
                // Arrow strings do not enforce CHAR/VARCHAR width.
                apply_declared_width(&cast, to)
            }
            Transform::Upper(_)
            | Transform::Lower(_)
            | Transform::Concat(_)
            | Transform::ConcatWs(_) => string_mask(batch, &mask.transform, &input)?,
        };
        let masked = cast_to(&masked, &target_type)?;
        for position in targets {
            columns[position] = Arc::clone(&masked);
        }
    }
    RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(|e| eval_err(&e))
}

fn mask_can_be_null(transform: &Transform, fields: &[DataField]) -> bool {
    let nullable = |input: &TransformInput| match input {
        TransformInput::Literal(literal) => literal.is_none(),
        TransformInput::Field(index) => fields[*index].data_type().is_nullable(),
    };
    match transform {
        Transform::Null => true,
        Transform::FieldRef(index) | Transform::Cast(index, _) => {
            fields[*index].data_type().is_nullable()
        }
        Transform::Upper(inputs) | Transform::Lower(inputs) | Transform::Concat(inputs) => {
            inputs.iter().any(nullable)
        }
        // Only a null separator makes CONCAT_WS null.
        Transform::ConcatWs(inputs) => inputs.first().is_some_and(nullable),
    }
}

/// `None` lets a NULL mask adopt the target's type.
fn mask_output_type(transform: &Transform, fields: &[DataField]) -> Option<arrow_schema::DataType> {
    match transform {
        Transform::Null => None,
        Transform::FieldRef(index) => {
            crate::arrow::paimon_type_to_arrow(fields[*index].data_type()).ok()
        }
        Transform::Cast(_, to) => crate::arrow::paimon_type_to_arrow(to).ok(),
        Transform::Upper(_)
        | Transform::Lower(_)
        | Transform::Concat(_)
        | Transform::ConcatWs(_) => Some(arrow_schema::DataType::Utf8),
    }
}

/// Match Java's truncation and fixed-width padding.
fn apply_declared_width(array: &ArrayRef, to: &crate::spec::DataType) -> ArrayRef {
    use crate::spec::DataType as Paimon;
    match to {
        Paimon::VarChar(t) => fit_strings(array, t.length() as usize, false),
        Paimon::Char(t) => fit_strings(array, t.length(), true),
        Paimon::VarBinary(t) => fit_bytes(array, t.length() as usize, false),
        Paimon::Binary(t) => fit_bytes(array, t.length(), true),
        _ => Arc::clone(array),
    }
}

/// Counts characters, as Java does.
fn fit_strings(array: &ArrayRef, length: usize, pad: bool) -> ArrayRef {
    let Some(strings) = array.as_any().downcast_ref::<arrow_array::StringArray>() else {
        return Arc::clone(array);
    };
    let fitted: arrow_array::StringArray = strings
        .iter()
        .map(|value| {
            value.map(|value| {
                let chars = value.chars().count();
                if chars > length {
                    value.chars().take(length).collect::<String>()
                } else if pad && chars < length {
                    format!("{value}{}", " ".repeat(length - chars))
                } else {
                    value.to_string()
                }
            })
        })
        .collect();
    Arc::new(fitted)
}

fn fit_bytes(array: &ArrayRef, length: usize, pad: bool) -> ArrayRef {
    let Some(bytes) = array.as_any().downcast_ref::<arrow_array::BinaryArray>() else {
        return Arc::clone(array);
    };
    let fitted: arrow_array::BinaryArray = bytes
        .iter()
        .map(|value| {
            value.map(|value| {
                let mut value = value.to_vec();
                if value.len() > length || pad {
                    value.resize(length, 0);
                }
                value
            })
        })
        .collect();
    Arc::new(fitted)
}

/// Fail on invalid casts, as Java does.
fn cast_to(array: &ArrayRef, to: &arrow_schema::DataType) -> Result<ArrayRef> {
    if array.data_type() == to {
        return Ok(Arc::clone(array));
    }
    let options = arrow_cast::CastOptions {
        safe: false,
        ..Default::default()
    };
    arrow_cast::cast_with_options(array, to, &options).map_err(|e| eval_err(&e))
}

enum StringInput<'a> {
    Literal(Option<&'a str>),
    Column(arrow_array::StringArray),
}

impl StringInput<'_> {
    fn value(&self, row: usize) -> Option<&str> {
        match self {
            Self::Literal(value) => *value,
            Self::Column(values) => values.is_valid(row).then(|| values.value(row)),
        }
    }
}

fn string_mask(
    batch: &RecordBatch,
    transform: &Transform,
    input: &dyn Fn(usize) -> Result<ArrayRef>,
) -> Result<ArrayRef> {
    use arrow_array::builder::StringBuilder;
    use std::fmt::Write;

    let inputs = match transform {
        Transform::Upper(inputs) | Transform::Lower(inputs) if inputs.len() == 1 => inputs,
        Transform::Concat(inputs) => inputs,
        Transform::ConcatWs(inputs) if inputs.len() >= 2 => inputs,
        _ => return Err(unsupported("the server's string mask has the wrong inputs")),
    };
    let resolved: Vec<StringInput<'_>> = inputs
        .iter()
        .map(|i| match i {
            TransformInput::Literal(literal) => Ok(StringInput::Literal(literal.as_deref())),
            TransformInput::Field(index) => Ok(StringInput::Column(
                cast_to(&input(*index)?, &arrow_schema::DataType::Utf8)?
                    .as_string::<i32>()
                    .clone(),
            )),
        })
        .collect::<Result<_>>()?;
    if matches!(transform, Transform::Upper(_) | Transform::Lower(_)) {
        let upper = matches!(transform, Transform::Upper(_));
        let literal = match &resolved[0] {
            StringInput::Column(array) => return Ok(Arc::new(string_case(array, upper))),
            StringInput::Literal(value) => value,
        };
        let mut values = StringBuilder::with_capacity(batch.num_rows(), 0);
        match literal {
            Some(value) => values.append_value_n(
                if upper {
                    value.to_uppercase()
                } else {
                    value.to_lowercase()
                },
                batch.num_rows(),
            ),
            None => values.append_nulls(batch.num_rows()),
        }
        return Ok(Arc::new(values.finish()));
    }
    let mut values = StringBuilder::with_capacity(batch.num_rows(), 0);
    for row in 0..batch.num_rows() {
        let (parts, separator) = if matches!(transform, Transform::ConcatWs(_)) {
            let Some(separator) = resolved[0].value(row) else {
                values.append_null();
                continue;
            };
            (&resolved[1..], separator)
        } else {
            if resolved.iter().any(|value| value.value(row).is_none()) {
                values.append_null();
                continue;
            }
            (resolved.as_slice(), "")
        };
        let mut first = true;
        for part in parts.iter().filter_map(|value| value.value(row)) {
            if !first {
                values
                    .write_str(separator)
                    .expect("infallible string write");
            }
            values.write_str(part).expect("infallible string write");
            first = false;
        }
        values.append_value("");
    }
    Ok(Arc::new(values.finish()))
}

fn string_case(array: &arrow_array::StringArray, upper: bool) -> arrow_array::StringArray {
    let offsets = array.value_offsets();
    let start = offsets[0];
    let bytes = &array.value_data()[start as usize..offsets[array.len()] as usize];
    if bytes.is_ascii() {
        let mut values = bytes.to_vec();
        if upper {
            values.make_ascii_uppercase();
        } else {
            values.make_ascii_lowercase();
        }
        let offsets = if start == 0 {
            array.offsets().clone()
        } else {
            arrow_buffer::OffsetBuffer::new(
                offsets.iter().map(|o| o - start).collect::<Vec<_>>().into(),
            )
        };
        return arrow_array::StringArray::new(offsets, values.into(), array.nulls().cloned());
    }
    // Unicode lowercasing needs context, e.g. the final Greek sigma.
    array
        .iter()
        .map(|value| {
            value.map(|value| {
                if upper {
                    value.to_uppercase()
                } else {
                    value.to_lowercase()
                }
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{
        BigIntType, BooleanType, DataType, DateType, Datum, DecimalType, DoubleType, FloatType,
        IntType, PredicateBuilder, PredicateOperator, TimestampType, VarBinaryType, VarCharType,
    };
    use arrow_array::{
        BinaryArray, Date32Array, Decimal128Array, Int16Array, Int32Array, Int64Array, Int8Array,
        RecordBatchOptions, StringArray, TimestampMillisecondArray,
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

    /// A Java `FieldRef`, as a transform input.
    fn input(field: &str) -> String {
        format!(r#"{{"index":0,"name":"{field}","type":"STRING"}}"#)
    }

    fn field_ref(field: &str) -> String {
        format!(r#"{{"name":"FIELD_REF","fieldRef":{}}}"#, input(field))
    }

    fn cast(field: &str, to: &str) -> String {
        format!(
            r#"{{"name":"CAST","fieldRef":{},"type":"{to}"}}"#,
            input(field)
        )
    }

    fn string_transform(name: &str, inputs: &[&str]) -> String {
        format!(r#"{{"name":"{name}","inputs":[{}]}}"#, inputs.join(","))
    }

    fn ids(batch: &RecordBatch) -> Vec<i32> {
        let ids = batch.column(0).as_any().downcast_ref::<Int32Array>();
        ids.unwrap().values().to_vec()
    }

    fn strings(batch: &RecordBatch, column: usize) -> Vec<Option<&str>> {
        let strings = batch.column(column).as_any().downcast_ref::<StringArray>();
        strings.unwrap().iter().collect()
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
    fn test_parse_reads_the_masks() {
        let fields = rule_fields();
        let upper = string_transform("UPPER", &[&input("name")]);
        let rules = parse(&[], &[("name", &upper), ("id", NULL)], &fields).unwrap();
        // Masks follow schema order.
        assert_eq!(
            rules.masks,
            vec![
                ColumnMask {
                    column: 0,
                    transform: Transform::Null
                },
                ColumnMask {
                    column: 1,
                    transform: Transform::Upper(vec![TransformInput::Field(1)])
                },
            ]
        );
        assert_eq!(rules.masked_columns(), HashSet::from([0, 1]));
        assert_eq!(mask_inputs(&rules.masks[1]), HashSet::from([1]));
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
    fn test_parse_refuses_a_bad_mask() {
        let fields = rule_fields();
        let missing_input = field_ref("missing");
        for (column, mask, reason) in [
            ("", NULL, "empty column mask"),
            ("name", "", "empty column mask"),
            ("name", "null", "cannot parse"),
            ("name", "{", "cannot parse"),
            ("name", r#"{"name":"ROT13"}"#, "cannot parse"),
            ("missing", NULL, "'missing', which is not a column"),
            ("name", &missing_input, "unknown field `missing`"),
        ] {
            let message = refusal(parse(&[], &[(column, mask)], &fields));
            assert!(message.contains(reason), "{column:?} {mask:?}: {message}");
        }
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

    #[test]
    fn test_a_mask_on_a_system_column_is_inert() {
        let fields = rule_fields();
        for column in [
            "_ROW_ID",
            "_SEQUENCE_NUMBER",
            "_VALUE_KIND",
            "_LEVEL",
            "rowkind",
        ] {
            let rules = parse(&[], &[(column, NULL)], &fields).unwrap();
            assert!(rules.is_empty(), "{column}");
        }
        // `_KEY_` names are not exempt system fields.
        let message = refusal(parse(&[], &[("_KEY_id", NULL)], &fields));
        assert!(
            message.contains("'_KEY_id', which is not a column"),
            "{message}"
        );
        // Skipped masks must still parse.
        for mask in ["null", "{"] {
            let message = refusal(parse(&[], &[("_ROW_ID", mask)], &fields));
            assert!(message.contains("cannot parse"), "{mask}: {message}");
        }
    }

    #[test]
    fn test_a_mask_must_keep_the_column_type() {
        let fields = rule_fields();
        for (column, mask) in [
            ("id", string_transform("UPPER", &[&input("name")])),
            ("id", string_transform("CONCAT", &[r#""x""#])),
            ("id", field_ref("name")),
            ("id", cast("id", "STRING")),
            ("id", cast("id", "BIGINT")),
            ("name", cast("name", "BINARY(3)")),
        ] {
            let message = refusal(parse(&[], &[(column, &mask)], &fields));
            assert!(message.contains("produces"), "{mask}: {message}");
        }
        for (column, mask) in [
            ("id", NULL.to_string()),
            ("name", cast("id", "VARCHAR(2)")),
            ("bin", cast("name", "BINARY(3)")),
        ] {
            assert!(parse(&[], &[(column, &mask)], &fields).is_ok(), "{mask}");
        }
    }

    #[test]
    fn test_a_not_null_column_takes_only_a_mask_that_cannot_be_null() {
        let fields = rule_fields();
        let (name, nn) = (input("name"), input("nn"));
        for (mask, fits) in [
            (NULL.to_string(), false),
            (field_ref("name"), false),
            (field_ref("nn"), true),
            (cast("name", "STRING"), false),
            (cast("nn", "VARCHAR(3)"), true),
            (string_transform("UPPER", &[&nn]), true),
            (string_transform("CONCAT", &[&nn, "null"]), false),
            (string_transform("CONCAT_WS", &["null", &nn]), false),
            // Only the separator can make CONCAT_WS null.
            (string_transform("CONCAT_WS", &[r#""-""#, &nn, &name]), true),
        ] {
            let parsed = parse(&[], &[("nn", &mask)], &fields);
            if fits {
                assert!(parsed.is_ok(), "{mask}: {parsed:?}");
            } else {
                assert!(refusal(parsed).contains("can produce null"), "{mask}");
            }
        }
    }

    #[test]
    fn test_a_mask_may_not_read_another_masked_column() {
        let fields = rule_fields();
        let upper_name = string_transform("UPPER", &[&input("name")]);
        assert!(parse(&[], &[("name", &upper_name)], &fields).is_ok());
        assert!(parse(&[], &[("name", &upper_name), ("id", NULL)], &fields).is_ok());
        // `alias` would expose the unmasked `name`.
        let message = refusal(parse(
            &[],
            &[("name", NULL), ("alias", &upper_name)],
            &fields,
        ));
        assert!(
            message.contains("reads 'name', which is masked too"),
            "{message}"
        );
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

    // Column masking

    fn mask_rows() -> RecordBatch {
        let bytes: Vec<Option<&[u8]>> = vec![Some(&[1, 2, 3, 4, 5, 6]), Some(&[1, 2]), None];
        batch(
            &rule_fields(),
            vec![
                Arc::new(Int32Array::from(vec![Some(12345), Some(7), None])),
                Arc::new(StringArray::from(vec![
                    Some("supersecret"),
                    Some("ab"),
                    None,
                ])),
                Arc::new(StringArray::from(vec![Some("MiXed"), None, Some("x")])),
                Arc::new(BinaryArray::from(bytes)),
                Arc::new(StringArray::from(vec!["héllo wörld", "q", "z"])),
            ],
        )
    }

    fn masked(column: &str, mask: &str) -> RecordBatch {
        let fields = rule_fields();
        let rules = parse(&[], &[(column, mask)], &fields).unwrap();
        mask_batch(&mask_rows(), &rules.masks, &fields, &fields).unwrap()
    }

    #[test]
    fn test_string_masks_propagate_nulls_as_java_does() {
        let (name, alias) = (input("name"), input("alias"));
        let cases = [
            (NULL.to_string(), vec![None, None, None]),
            (field_ref("alias"), vec![Some("MiXed"), None, Some("x")]),
            (
                string_transform("UPPER", &[&alias]),
                vec![Some("MIXED"), None, Some("X")],
            ),
            (
                string_transform("LOWER", &[&alias]),
                vec![Some("mixed"), None, Some("x")],
            ),
            (
                string_transform("UPPER", &[r#""Straße""#]),
                vec![Some("STRASSE"); 3],
            ),
            (string_transform("LOWER", &[r#""ΟΣ""#]), vec![Some("ος"); 3]),
            (string_transform("UPPER", &["null"]), vec![None; 3]),
            (string_transform("CONCAT", &[]), vec![Some(""); 3]),
            (
                string_transform("CONCAT", &[&name, r#""-""#, &alias]),
                vec![Some("supersecret-MiXed"), None, None],
            ),
            (
                string_transform("CONCAT", &[r#""****""#]),
                vec![Some("****"); 3],
            ),
            (string_transform("CONCAT", &[&name, "null"]), vec![None; 3]),
            // CONCAT_WS skips null payloads and is null on a null separator.
            (
                string_transform("CONCAT_WS", &[r#""-""#, r#""x""#, &name, "null", &alias]),
                vec![Some("x-supersecret-MiXed"), Some("x-ab"), Some("x-x")],
            ),
            (
                string_transform("CONCAT_WS", &[&alias, &name, r#""y""#]),
                vec![Some("supersecretMiXedy"), None, Some("y")],
            ),
            (
                string_transform("CONCAT_WS", &[r#""-""#, "null", "null"]),
                vec![Some(""); 3],
            ),
        ];
        for (mask, expected) in cases {
            let out = masked("name", &mask);
            assert_eq!(strings(&out, 1), expected, "{mask}");
            assert_eq!(strings(&out, 2), [Some("MiXed"), None, Some("x")], "{mask}");
        }
    }

    #[test]
    fn test_a_cast_java_converts_differently_is_refused() {
        let fields = vec![
            field(0, "n", DataType::BigInt(crate::spec::BigIntType::new())),
            field(
                1,
                "ts",
                DataType::Timestamp(crate::spec::TimestampType::new(3).unwrap()),
            ),
            field(2, "d", DataType::Double(crate::spec::DoubleType::new())),
            field(3, "s", string()),
            field(4, "b", DataType::VarBinary(VarBinaryType::new(10).unwrap())),
            field(5, "i", DataType::Int(IntType::new())),
        ];
        for (target, mask) in [
            ("ts", cast("n", "TIMESTAMP(3)")),
            ("s", cast("d", "VARCHAR(10)")),
            ("s", cast("b", "VARCHAR(10)")),
            // Java trims the string first.
            ("n", cast("s", "BIGINT")),
            ("i", cast("n", "INT")),
        ] {
            let message = refusal(parse(&[], &[(target, &mask)], &fields));
            assert!(message.contains("as Java does"), "{mask}: {message}");
        }
        for (target, mask) in [
            ("s", cast("n", "VARCHAR(10)")),
            ("b", cast("s", "VARBINARY(10)")),
            ("ts", cast("ts", "TIMESTAMP(3)")),
        ] {
            assert!(parse(&[], &[(target, &mask)], &fields).is_ok(), "{mask}");
        }
    }

    #[test]
    fn test_integer_cast_masks_widen_exactly() {
        let tiny: ArrayRef = Arc::new(Int8Array::from(vec![Some(i8::MIN), None, Some(i8::MAX)]));
        let small: ArrayRef =
            Arc::new(Int16Array::from(vec![Some(i16::MIN), None, Some(i16::MAX)]));
        let int: ArrayRef = Arc::new(Int32Array::from(vec![Some(i32::MIN), None, Some(i32::MAX)]));
        let cases: [(&str, &str, ArrayRef, ArrayRef); 6] = [
            (
                "TINYINT",
                "SMALLINT",
                tiny.clone(),
                Arc::new(Int16Array::from(vec![Some(-128), None, Some(127)])),
            ),
            (
                "TINYINT",
                "INT",
                tiny.clone(),
                Arc::new(Int32Array::from(vec![Some(-128), None, Some(127)])),
            ),
            (
                "TINYINT",
                "BIGINT",
                tiny,
                Arc::new(Int64Array::from(vec![Some(-128), None, Some(127)])),
            ),
            (
                "SMALLINT",
                "INT",
                small.clone(),
                Arc::new(Int32Array::from(vec![Some(-32768), None, Some(32767)])),
            ),
            (
                "SMALLINT",
                "BIGINT",
                small,
                Arc::new(Int64Array::from(vec![Some(-32768), None, Some(32767)])),
            ),
            (
                "INT",
                "BIGINT",
                int,
                Arc::new(Int64Array::from(vec![
                    Some(-2147483648),
                    None,
                    Some(2147483647),
                ])),
            ),
        ];
        for (from, to, values, expected) in cases {
            let fields = vec![
                field(
                    0,
                    "input",
                    serde_json::from_value(serde_json::json!(from)).unwrap(),
                ),
                field(
                    1,
                    "wide",
                    serde_json::from_value(serde_json::json!(to)).unwrap(),
                ),
            ];
            let target =
                arrow_cast::cast(&Int64Array::from(vec![999; 3]), expected.data_type()).unwrap();
            let rows = batch(&fields, vec![values, target]);
            let mask = serde_json::json!({"name":"CAST", "fieldRef": {
                "index":0, "name":"input", "type":from,
            }, "type":to})
            .to_string();
            let rules = parse(&[], &[("wide", &mask)], &fields).unwrap();
            let out = mask_batch(&rows, &rules.masks, &fields, &fields).unwrap();
            assert_eq!(out.column(1), &expected, "{from} -> {to}");
            assert_eq!(out.column(0), rows.column(0));
        }
    }

    #[test]
    fn test_string_masks_preserve_unicode_and_sliced_inputs() {
        let fields = vec![field(0, "value", string())];
        let input = serde_json::json!({"index":0, "name":"value", "type":"STRING"}).to_string();
        for (values, upper, lower) in [
            (
                vec![
                    Some("prefix"),
                    Some("aZ"),
                    None,
                    Some(""),
                    Some("q"),
                    Some("suffix"),
                ],
                vec![Some("AZ"), None, Some(""), Some("Q")],
                vec![Some("az"), None, Some(""), Some("q")],
            ),
            (
                vec![
                    Some("prefix"),
                    Some("ΟΣ"),
                    Some("Straße"),
                    None,
                    Some("İ"),
                    Some("suffix"),
                ],
                vec![Some("ΟΣ"), Some("STRASSE"), None, Some("İ")],
                vec![Some("ος"), Some("straße"), None, Some("i\u{307}")],
            ),
            (vec![None; 6], vec![None; 4], vec![None; 4]),
        ] {
            let rows = batch(
                &fields,
                vec![Arc::new(StringArray::from(values).slice(1, 4))],
            );
            for (operation, expected) in [("UPPER", upper), ("LOWER", lower)] {
                let mask = string_transform(operation, &[&input]);
                let rules = parse(&[], &[("value", &mask)], &fields).unwrap();
                let out = mask_batch(&rows, &rules.masks, &fields, &fields).unwrap();
                assert_eq!(strings(&out, 0), expected);
                let empty = mask_batch(&rows.slice(2, 0), &rules.masks, &fields, &fields).unwrap();
                assert_eq!(empty.num_rows(), 0);
            }
        }
    }

    #[test]
    fn test_a_cast_mask_applies_the_declared_width() {
        for (mask, expected) in [
            (
                cast("name", "VARCHAR(3)"),
                vec![Some("sup"), Some("ab"), None],
            ),
            (
                cast("name", "CHAR(5)"),
                vec![Some("super"), Some("ab   "), None],
            ),
            // Characters, not bytes.
            (
                cast("nn", "VARCHAR(3)"),
                vec![Some("hél"), Some("q"), Some("z")],
            ),
            (cast("id", "VARCHAR(2)"), vec![Some("12"), Some("7"), None]),
        ] {
            assert_eq!(strings(&masked("name", &mask), 1), expected, "{mask}");
        }
        let cases: [(String, Vec<Option<&[u8]>>); 3] = [
            (
                cast("bin", "BINARY(4)"),
                vec![Some(&[1, 2, 3, 4]), Some(&[1, 2, 0, 0]), None],
            ),
            (
                cast("bin", "VARBINARY(4)"),
                vec![Some(&[1, 2, 3, 4]), Some(&[1, 2]), None],
            ),
            (
                cast("name", "BINARY(3)"),
                vec![Some(b"sup"), Some(b"ab\0"), None],
            ),
        ];
        for (mask, expected) in cases {
            let out = masked("bin", &mask);
            let bytes = out.column(3).as_any().downcast_ref::<BinaryArray>();
            assert_eq!(
                bytes.unwrap().iter().collect::<Vec<_>>(),
                expected,
                "{mask}"
            );
        }
    }

    #[test]
    fn test_every_copy_of_a_masked_column_is_masked() {
        let fields = rule_fields();
        let rows = mask_rows().project(&[1, 0, 1]).unwrap();
        let rows_fields = [fields[1].clone(), fields[0].clone(), fields[1].clone()];
        let mask = string_transform("CONCAT", &[&input("name"), r#""!""#]);
        let rules = parse(&[], &[("name", &mask)], &fields).unwrap();
        let out = mask_batch(&rows, &rules.masks, &fields, &rows_fields).unwrap();
        for column in [0, 2] {
            assert_eq!(
                strings(&out, column),
                [Some("supersecret!"), Some("ab!"), None]
            );
        }
        assert_eq!(out.column(1), rows.column(1));
    }

    #[test]
    fn test_masks_read_the_batch_as_read() {
        // Bypass parsing to verify both masks read the original batch.
        let fields = rule_fields();
        let masks = [
            ColumnMask {
                column: 0,
                transform: Transform::Null,
            },
            ColumnMask {
                column: 1,
                transform: Transform::Cast(0, string()),
            },
        ];
        let out = mask_batch(&mask_rows(), &masks, &fields, &fields).unwrap();
        assert_eq!(out.column(0).null_count(), 3);
        assert_eq!(strings(&out, 1), [Some("12345"), Some("7"), None]);
    }

    #[test]
    fn test_a_mask_refuses_a_batch_without_its_columns() {
        let fields = rule_fields();
        let rows = mask_rows();
        let mask = field_ref("alias");
        let masks = parse(&[], &[("name", &mask)], &fields).unwrap().masks;
        let without_target = rows.project(&[0, 2]).unwrap();
        let message = refusal(mask_batch(
            &without_target,
            &masks,
            &fields,
            &[fields[0].clone(), fields[2].clone()],
        ));
        assert!(message.contains("does not carry 'name'"), "{message}");
        let without_input = rows.project(&[0, 1]).unwrap();
        let message = refusal(mask_batch(&without_input, &masks, &fields, &fields[..2]));
        assert!(message.contains("does not carry 'alias'"), "{message}");
    }

    #[test]
    fn test_no_mask_keeps_a_zero_column_batch_whole() {
        let options = RecordBatchOptions::new().with_row_count(Some(3));
        let empty =
            RecordBatch::try_new_with_options(Arc::new(Schema::empty()), Vec::new(), &options)
                .unwrap();
        let out = mask_batch(&empty, &[], &rule_fields(), &[]).unwrap();
        assert_eq!(out.num_rows(), 3);
    }
}
