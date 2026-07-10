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

use paimon::spec::{DataField, DataType, Datum, DecimalType, Predicate, PredicateBuilder};
use pyo3::exceptions::{PyNotImplementedError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyTzInfoAccess;
use pyo3::types::{PyBool, PyDateTime, PyDict, PyInt, PyList, PyString, PyTime, PyTzInfo};

/// Convert a single Python literal into a typed [`Datum`] driven by the target
/// [`DataType`].
///
/// Conversion is strictly DataType-driven (never inferred from the Python type):
/// the field's declared type decides how the literal is interpreted and validated.
///
/// Rules:
/// - `Boolean` accepts only a Python `bool`.
/// - Integer types (`TinyInt`/`SmallInt`/`Int`/`BigInt`) reject Python `bool`
///   (which is an `int` subclass) and enforce the target range.
/// - `Float`/`Double` accept Python `int` or `float` but reject `bool`.
/// - `Char`/`VarChar` accept only a Python `str` (no implicit stringification).
/// - `Date` accepts only a `datetime.date` that is not a `datetime.datetime`
///   (accepting the subclass would silently drop the time-of-day part).
/// - `Time` accepts only a naive `datetime.time` with whole-millisecond
///   microseconds (`Datum::Time` carries millis-of-day; sub-millisecond values
///   would be silently truncated).
/// - `Timestamp` accepts only a naive `datetime.datetime` (the type is
///   timezone-less); `LocalZonedTimestamp` accepts only a timezone-aware one
///   (converted to UTC). This mirrors the DataFusion pushdown path, which
///   refuses zoned literals for TIMESTAMP and naive ones for TIMESTAMP_LTZ.
/// - `Decimal` accepts a `decimal.Decimal` or an `int`, rescaled losslessly to
///   the column's scale; anything needing rounding, exceeding the column's
///   precision, non-finite, or a binary `float` is rejected.
/// - All other types (Bytes/complex) are not supported yet and raise
///   `NotImplementedError`.
///
/// Errors:
/// - `ValueError` for type mismatches, out-of-range integers, zoned/naive
///   timestamp mismatches, and lossy decimal/time conversions.
/// - `NotImplementedError` for unsupported field types (message names the type).
pub(crate) fn py_to_datum(value: &Bound<'_, PyAny>, data_type: &DataType) -> PyResult<Datum> {
    match data_type {
        DataType::Boolean(_) => {
            let b = value
                .cast::<PyBool>()
                .map_err(|_| PyValueError::new_err("expected a bool literal for Boolean field"))?;
            Ok(Datum::Bool(b.is_true()))
        }
        DataType::TinyInt(_) => int_datum(value, i8::MIN as i64, i8::MAX as i64, |v| {
            Datum::TinyInt(v as i8)
        }),
        DataType::SmallInt(_) => int_datum(value, i16::MIN as i64, i16::MAX as i64, |v| {
            Datum::SmallInt(v as i16)
        }),
        DataType::Int(_) => int_datum(value, i32::MIN as i64, i32::MAX as i64, |v| {
            Datum::Int(v as i32)
        }),
        DataType::BigInt(_) => int_datum(value, i64::MIN, i64::MAX, Datum::Long),
        DataType::Float(_) => Ok(Datum::Float(float_val(value)? as f32)),
        DataType::Double(_) => Ok(Datum::Double(float_val(value)?)),
        DataType::Char(_) | DataType::VarChar(_) => {
            let s = value
                .cast::<PyString>()
                .map_err(|_| PyValueError::new_err("expected a str literal for String field"))?;
            Ok(Datum::String(s.to_str()?.to_string()))
        }
        DataType::Date(_) => date_datum(value),
        DataType::Time(_) => time_datum(value),
        DataType::Timestamp(_) => timestamp_datum(value),
        DataType::LocalZonedTimestamp(_) => local_zoned_timestamp_datum(value),
        DataType::Decimal(dec) => decimal_datum(value, dec),
        other => Err(PyNotImplementedError::new_err(format!(
            "literal conversion for type {other:?} is not supported yet"
        ))),
    }
}

/// Extract an integer literal, rejecting Python `bool` (an `int` subclass) and
/// enforcing the inclusive `[lo, hi]` range before building the `Datum`.
fn int_datum(
    value: &Bound<'_, PyAny>,
    lo: i64,
    hi: i64,
    make: impl Fn(i64) -> Datum,
) -> PyResult<Datum> {
    if value.is_instance_of::<PyBool>() {
        return Err(PyValueError::new_err("bool is not a valid integer literal"));
    }
    let v: i64 = value
        .extract()
        .map_err(|_| PyValueError::new_err("expected an int literal"))?;
    if v < lo || v > hi {
        return Err(PyValueError::new_err(format!(
            "integer literal {v} out of range [{lo}, {hi}]"
        )));
    }
    Ok(make(v))
}

/// Extract a floating-point literal from a Python `int` or `float`, rejecting
/// `bool`.
fn float_val(value: &Bound<'_, PyAny>) -> PyResult<f64> {
    if value.is_instance_of::<PyBool>() {
        return Err(PyValueError::new_err("bool is not a valid float literal"));
    }
    value
        .extract::<f64>()
        .map_err(|_| PyValueError::new_err("expected a numeric literal"))
}

/// Convert a `datetime.date` into `Datum::Date` (epoch days).
///
/// `datetime.datetime` is a `date` subclass but is rejected: accepting it would
/// silently drop the time-of-day part.
fn date_datum(value: &Bound<'_, PyAny>) -> PyResult<Datum> {
    if value.is_instance_of::<PyDateTime>() {
        return Err(PyValueError::new_err(
            "expected a datetime.date literal for Date field, got datetime.datetime \
             (the time-of-day part would be dropped)",
        ));
    }
    let date: chrono::NaiveDate = value
        .extract()
        .map_err(|_| PyValueError::new_err("expected a datetime.date literal for Date field"))?;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let days = date.signed_duration_since(epoch).num_days();
    let days = i32::try_from(days)
        .map_err(|_| PyValueError::new_err(format!("date literal {date} out of range")))?;
    Ok(Datum::Date(days))
}

/// Convert a naive `datetime.time` into `Datum::Time` (millis of day).
///
/// Timezone-aware times are rejected (TIME has no timezone), as are
/// sub-millisecond microseconds (`Datum::Time` carries millis; truncating
/// would silently change comparison semantics).
fn time_datum(value: &Bound<'_, PyAny>) -> PyResult<Datum> {
    let time = value
        .cast::<PyTime>()
        .map_err(|_| PyValueError::new_err("expected a datetime.time literal for Time field"))?;
    if time.get_tzinfo().is_some() {
        return Err(PyValueError::new_err(
            "expected a naive datetime.time literal for Time field (tzinfo must be None)",
        ));
    }
    let time: chrono::NaiveTime = value
        .extract()
        .map_err(|_| PyValueError::new_err("expected a datetime.time literal for Time field"))?;
    use chrono::Timelike;
    if !time.nanosecond().is_multiple_of(1_000_000) {
        return Err(PyValueError::new_err(
            "time literal has sub-millisecond microseconds, which TIME comparisons \
             cannot represent without truncation",
        ));
    }
    let millis = time.num_seconds_from_midnight() * 1_000 + time.nanosecond() / 1_000_000;
    Ok(Datum::Time(millis as i32))
}

/// Split epoch microseconds into `(millis, nanos-of-milli)` with a Euclidean
/// split so pre-epoch instants keep a non-negative nano remainder, matching
/// `BinaryRow::get_timestamp_raw` and the DataFusion pushdown conversion.
fn micros_to_millis_nanos(micros: i64) -> (i64, i32) {
    (
        micros.div_euclid(1_000),
        (micros.rem_euclid(1_000) * 1_000) as i32,
    )
}

/// Convert a naive `datetime.datetime` into `Datum::Timestamp`.
///
/// TIMESTAMP (without time zone) is timezone-less, so timezone-aware datetimes
/// are rejected rather than silently reinterpreted — parity with the DataFusion
/// pushdown path, which refuses zoned literals for this type.
fn timestamp_datum(value: &Bound<'_, PyAny>) -> PyResult<Datum> {
    let dt = value.cast::<PyDateTime>().map_err(|_| {
        PyValueError::new_err("expected a datetime.datetime literal for Timestamp field")
    })?;
    if dt.get_tzinfo().is_some() {
        return Err(PyValueError::new_err(
            "expected a naive datetime.datetime literal for Timestamp field (tzinfo must \
             be None); TIMESTAMP has no timezone — use a TIMESTAMP WITH LOCAL TIME ZONE \
             column for zoned instants",
        ));
    }
    let naive: chrono::NaiveDateTime = value.extract().map_err(|_| {
        PyValueError::new_err("expected a datetime.datetime literal for Timestamp field")
    })?;
    let (millis, nanos) = micros_to_millis_nanos(naive.and_utc().timestamp_micros());
    Ok(Datum::Timestamp { millis, nanos })
}

/// Convert a timezone-aware `datetime.datetime` into `Datum::LocalZonedTimestamp`
/// (the UTC instant).
///
/// Aware follows Python's definition: `tzinfo` is set AND `utcoffset()` is not
/// `None`. A tzinfo whose `utcoffset()` returns `None` is naive — passing it to
/// `astimezone` would interpret the value as process-local time, normalizing
/// the same literal differently per machine. Naive datetimes are rejected
/// rather than assumed to be in any timezone — parity with the DataFusion
/// pushdown path, which refuses naive literals for TIMESTAMP WITH LOCAL TIME
/// ZONE.
fn local_zoned_timestamp_datum(value: &Bound<'_, PyAny>) -> PyResult<Datum> {
    let py = value.py();
    let dt = value.cast::<PyDateTime>().map_err(|_| {
        PyValueError::new_err("expected a datetime.datetime literal for LocalZonedTimestamp field")
    })?;
    // Python's aware/naive rule (datetime docs): aware iff tzinfo is not None
    // and utcoffset() does not return None.
    let aware =
        dt.get_tzinfo().is_some() && !dt.call_method0(pyo3::intern!(py, "utcoffset"))?.is_none();
    if !aware {
        return Err(PyValueError::new_err(
            "expected a timezone-aware datetime.datetime literal for LocalZonedTimestamp \
             field (naive datetimes, including tzinfo with utcoffset() = None, are \
             ambiguous)",
        ));
    }
    // Normalize through `astimezone(utc)` so any tzinfo implementation
    // (zoneinfo, pytz, fixed offsets) is handled by Python itself.
    let utc = PyTzInfo::utc(py)?;
    let as_utc = dt.call_method1(pyo3::intern!(py, "astimezone"), (utc,))?;
    let instant: chrono::DateTime<chrono::Utc> = as_utc.extract()?;
    let (millis, nanos) = micros_to_millis_nanos(instant.timestamp_micros());
    Ok(Datum::LocalZonedTimestamp { millis, nanos })
}

/// Convert a `decimal.Decimal` or `int` literal into `Datum::Decimal` at the
/// column's precision and scale.
///
/// The conversion is exact: values that would need rounding at the column's
/// scale, or whose unscaled magnitude exceeds the column's precision, are
/// rejected. Binary `float`s are rejected outright (they are not exact decimal
/// values), as are non-finite decimals (NaN/Infinity).
fn decimal_datum(value: &Bound<'_, PyAny>, dec: &DecimalType) -> PyResult<Datum> {
    let precision = dec.precision();
    let scale = dec.scale();
    if value.is_instance_of::<PyBool>() {
        return Err(PyValueError::new_err("bool is not a valid decimal literal"));
    }

    let unscaled = if value.is_instance_of::<PyInt>() {
        let v: i128 = value.extract().map_err(|_| {
            PyValueError::new_err("int literal out of supported range for Decimal field")
        })?;
        v.checked_mul(10i128.pow(scale)).ok_or_else(|| {
            PyValueError::new_err("int literal out of supported range for Decimal field")
        })?
    } else {
        let decimal_cls = value.py().import("decimal")?.getattr("Decimal")?;
        if !value.is_instance(&decimal_cls)? {
            return Err(PyValueError::new_err(
                "expected a decimal.Decimal or int literal for Decimal field (float is \
                 not an exact decimal value)",
            ));
        }
        decimal_to_unscaled(value, scale)?
    };

    // 10^precision fits i128 for the supported precision range (<= 38).
    if unscaled.unsigned_abs() >= 10u128.pow(precision) {
        return Err(PyValueError::new_err(format!(
            "decimal literal does not fit DECIMAL({precision}, {scale})"
        )));
    }
    Ok(Datum::Decimal {
        unscaled,
        precision,
        scale,
    })
}

/// Rescale a finite `decimal.Decimal` to `scale` exactly via `as_tuple()`
/// (sign, digits, exponent), avoiding Decimal arithmetic whose context
/// precision could silently round.
fn decimal_to_unscaled(value: &Bound<'_, PyAny>, scale: u32) -> PyResult<i128> {
    let tuple = value.call_method0("as_tuple")?;
    let sign: u8 = tuple.getattr("sign")?.extract()?;
    // For NaN/Infinity the exponent is a string ('n'/'N'/'F') and extraction fails.
    let exponent: i64 = tuple.getattr("exponent")?.extract().map_err(|_| {
        PyValueError::new_err("non-finite Decimal (NaN/Infinity) is not a valid literal")
    })?;
    let digits: Vec<u32> = tuple.getattr("digits")?.extract()?;

    let overflow =
        || PyValueError::new_err("decimal literal out of supported range for Decimal field");
    let mut magnitude: i128 = 0;
    for d in digits {
        magnitude = magnitude
            .checked_mul(10)
            .and_then(|m| m.checked_add(d as i128))
            .ok_or_else(overflow)?;
    }

    // value = ±magnitude * 10^exponent; unscaled = ±magnitude * 10^(exponent + scale).
    let shift = exponent + scale as i64;
    let magnitude = if shift >= 0 {
        u32::try_from(shift)
            .ok()
            .filter(|s| *s <= 38)
            .and_then(|s| magnitude.checked_mul(10i128.pow(s)))
            .ok_or_else(overflow)?
    } else if magnitude == 0 {
        0
    } else {
        let down = u32::try_from(-shift).ok().filter(|s| *s <= 38).ok_or_else(
            // More than 38 digits would have to be truncated; magnitude != 0
            // means that is always lossy.
            || rounding_error(scale),
        )?;
        let divisor = 10i128.pow(down);
        if magnitude % divisor != 0 {
            return Err(rounding_error(scale));
        }
        magnitude / divisor
    };
    Ok(if sign == 1 { -magnitude } else { magnitude })
}

fn rounding_error(scale: u32) -> PyErr {
    PyValueError::new_err(format!(
        "decimal literal cannot be represented at scale {scale} without rounding"
    ))
}

/// Operators recognized by the lightweight dict format but not translatable to a
/// Rust [`Predicate`] for pushdown.
const METHOD_NOT_SUPPORTED: &[&str] = &["not"];

/// Recursively convert a lightweight dict predicate into a Rust [`Predicate`].
///
/// The dict shape mirrors the Python predicate tree:
/// - Leaf: `{"method": <op>, "field": <name>, "literals": [..]}`
/// - Compound: `{"method": "and"|"or", "children": [<dict>, ..]}`
///
/// Field types are resolved authoritatively from `fields` (the table schema); any
/// `index`/`data_type` present in the dict is ignored. Literal conversion is
/// delegated to [`py_to_datum`], driven by the resolved [`DataType`].
///
/// There is no partial pushdown: in `and`/`or`, every child is converted and any
/// failure propagates, failing the whole predicate.
///
/// Errors:
/// - `ValueError` for unknown fields, missing keys, wrong literal counts, `None`
///   literals, empty/missing `children`, non-dict children, or non-list
///   `literals`/`children`.
/// - `NotImplementedError` for unsupported operators or unsupported literal types.
pub(crate) fn dict_to_predicate(
    node: &Bound<'_, PyDict>,
    fields: &[DataField],
    case_sensitive: bool,
) -> PyResult<Predicate> {
    let method: String = node
        .get_item("method")?
        .ok_or_else(|| PyValueError::new_err("predicate dict missing 'method'"))?
        .extract()?;

    match method.as_str() {
        "and" | "or" => {
            let children = node
                .get_item("children")?
                .ok_or_else(|| PyValueError::new_err(format!("'{method}' requires 'children'")))?;
            let list = children
                .cast::<PyList>()
                .map_err(|_| PyValueError::new_err("'children' must be a list"))?;
            if list.is_empty() {
                return Err(PyValueError::new_err(format!(
                    "'{method}' requires non-empty 'children'"
                )));
            }
            let mut preds = Vec::with_capacity(list.len());
            for child in list.iter() {
                let child_dict = child
                    .cast::<PyDict>()
                    .map_err(|_| PyValueError::new_err("each child must be a dict"))?;
                // Unsupported child propagates → no partial pushdown.
                preds.push(dict_to_predicate(child_dict, fields, case_sensitive)?);
            }
            Ok(if method == "and" {
                Predicate::and(preds)
            } else {
                Predicate::or(preds)
            })
        }
        m if METHOD_NOT_SUPPORTED.contains(&m) => Err(PyNotImplementedError::new_err(format!(
            "predicate operator '{m}' is not supported for Rust pushdown"
        ))),
        _ => leaf_to_predicate(&method, node, fields, case_sensitive),
    }
}

/// Resolve a leaf's field name to its schema [`DataType`] under the given case
/// sensitivity.
///
/// Case-sensitive: exact match. Case-insensitive: ASCII-fold and require a
/// unique match. Absent → `ValueError("Column '{field}' not found in schema")`;
/// ambiguous (2+ fields collide under folding) → `ValueError` naming the clash.
fn resolve_leaf_field_type(
    fields: &[DataField],
    field: &str,
    case_sensitive: bool,
) -> PyResult<DataType> {
    let not_found = || PyValueError::new_err(format!("Column '{field}' not found in schema"));
    if case_sensitive {
        return fields
            .iter()
            .find(|f| f.name() == field)
            .map(|f| f.data_type().clone())
            .ok_or_else(not_found);
    }
    let mut matches = fields
        .iter()
        .filter(|f| f.name().eq_ignore_ascii_case(field));
    let first = matches.next().ok_or_else(not_found)?;
    if matches.next().is_some() {
        return Err(PyValueError::new_err(format!(
            "Ambiguous column '{field}': multiple schema fields match case-insensitively"
        )));
    }
    Ok(first.data_type().clone())
}

/// Convert a single leaf dict (already known not to be `and`/`or`) into a
/// [`Predicate`], resolving the field type from the schema.
fn leaf_to_predicate(
    method: &str,
    node: &Bound<'_, PyDict>,
    fields: &[DataField],
    case_sensitive: bool,
) -> PyResult<Predicate> {
    let field: String = node
        .get_item("field")?
        .ok_or_else(|| PyValueError::new_err(format!("'{method}' leaf requires 'field'")))?
        .extract()?;

    // Resolve field DataType from schema (authoritative) for literal conversion.
    let data_type = resolve_leaf_field_type(fields, &field, case_sensitive)?;

    let literals_obj = node.get_item("literals")?;
    let pb = PredicateBuilder::new_with_case_sensitive(fields, case_sensitive);

    // Convert literals (DataType-driven), wrapping NotImplemented type messages
    // with field context.
    let to_datums = |obj: Option<Bound<'_, PyAny>>| -> PyResult<Vec<Datum>> {
        let mut out = Vec::new();
        if let Some(obj) = obj {
            let list = obj
                .cast::<PyList>()
                .map_err(|_| PyValueError::new_err("'literals' must be a list"))?;
            for item in list.iter() {
                if item.is_none() {
                    return Err(PyValueError::new_err(
                        "None is not a valid comparison literal; use isNull/isNotNull",
                    ));
                }
                out.push(
                    py_to_datum(&item, &data_type)
                        .map_err(|e| with_field_context(e, &field, &data_type))?,
                );
            }
        }
        Ok(out)
    };

    let result = match method {
        "equal" => pb.equal(&field, one(to_datums(literals_obj)?)?),
        "notEqual" => pb.not_equal(&field, one(to_datums(literals_obj)?)?),
        "lessThan" => pb.less_than(&field, one(to_datums(literals_obj)?)?),
        "lessOrEqual" => pb.less_or_equal(&field, one(to_datums(literals_obj)?)?),
        "greaterThan" => pb.greater_than(&field, one(to_datums(literals_obj)?)?),
        "greaterOrEqual" => pb.greater_or_equal(&field, one(to_datums(literals_obj)?)?),
        "isNull" => {
            ensure_no_literals(method, literals_obj)?;
            pb.is_null(&field)
        }
        "isNotNull" => {
            ensure_no_literals(method, literals_obj)?;
            pb.is_not_null(&field)
        }
        "in" => {
            let ds = to_datums(literals_obj)?;
            if ds.is_empty() {
                return Err(PyValueError::new_err("'in' requires at least 1 literal"));
            }
            pb.is_in(&field, ds)
        }
        "notIn" => {
            let ds = to_datums(literals_obj)?;
            if ds.is_empty() {
                return Err(PyValueError::new_err("'notIn' requires at least 1 literal"));
            }
            pb.is_not_in(&field, ds)
        }
        "startsWith" => pb.starts_with(&field, one(to_datums(literals_obj)?)?),
        "endsWith" => pb.ends_with(&field, one(to_datums(literals_obj)?)?),
        "contains" => pb.contains(&field, one(to_datums(literals_obj)?)?),
        "like" => {
            // 1 literal: pattern with the default '\' escape.
            // 2 literals: [pattern, escape] where escape is a single character
            // (SQL `LIKE .. ESCAPE ..`).
            let mut ds = to_datums(literals_obj)?;
            let escape = match ds.len() {
                1 => None,
                2 => Some(escape_char(ds.pop().unwrap())?),
                n => {
                    return Err(PyValueError::new_err(format!(
                        "'like' expects 1 or 2 literals (pattern[, escape]), got {n}"
                    )));
                }
            };
            pb.like(&field, ds.pop().unwrap(), escape)
        }
        other => {
            return Err(PyNotImplementedError::new_err(format!(
                "unknown or unsupported predicate operator '{other}'"
            )));
        }
    };
    result.map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Extract exactly one literal for comparison/equality operators.
fn one(mut ds: Vec<Datum>) -> PyResult<Datum> {
    if ds.len() != 1 {
        return Err(PyValueError::new_err(format!(
            "expected exactly 1 literal, got {}",
            ds.len()
        )));
    }
    Ok(ds.pop().unwrap())
}

/// Validate that a null-check operator (`isNull`/`isNotNull`) carries exactly 0
/// literals. A missing `literals` key, or one present as an empty list `[]`, is
/// accepted; a non-empty list raises `ValueError` (count mismatch); a present
/// non-list value raises `ValueError` (mirroring how comparison ops reject a
/// non-list `literals`).
fn ensure_no_literals(method: &str, obj: Option<Bound<'_, PyAny>>) -> PyResult<()> {
    if let Some(obj) = obj {
        let list = obj
            .cast::<PyList>()
            .map_err(|_| PyValueError::new_err("'literals' must be a list"))?;
        if !list.is_empty() {
            return Err(PyValueError::new_err(format!(
                "{method} expects 0 literals, got {}",
                list.len()
            )));
        }
    }
    Ok(())
}

/// Re-wrap an unsupported-literal-type `NotImplementedError` from [`py_to_datum`]
/// with field-name context; pass other errors through unchanged.
fn with_field_context(err: PyErr, field: &str, data_type: &DataType) -> PyErr {
    Python::attach(|py| {
        if err.is_instance_of::<PyNotImplementedError>(py) {
            PyNotImplementedError::new_err(format!(
                "literal conversion for field '{field}' of type {data_type:?} is not supported yet"
            ))
        } else {
            err
        }
    })
}

/// Extract a single-character `like` ESCAPE literal from an already-converted
/// string `Datum`.
fn escape_char(datum: Datum) -> PyResult<char> {
    let Datum::String(s) = datum else {
        return Err(PyValueError::new_err("'like' escape must be a str literal"));
    };
    let mut chars = s.chars();
    match (chars.next(), chars.next()) {
        (Some(c), None) => Ok(c),
        _ => Err(PyValueError::new_err(format!(
            "'like' escape must be a single character, got {s:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paimon::spec::{
        DataField, DataType, Datum, IntType, Predicate, PredicateOperator, VarCharType,
    };
    use pyo3::IntoPyObject;
    use pyo3::Python;

    fn test_fields() -> Vec<DataField> {
        vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "name".to_string(),
                DataType::VarChar(VarCharType::default()),
            ),
        ]
    }

    /// Build a leaf predicate dict: {"method": .., "field": .., "literals": [..]}.
    fn leaf_dict<'py>(
        py: Python<'py>,
        method: &str,
        field: &str,
        literals: &[i64],
    ) -> Bound<'py, PyDict> {
        let d = PyDict::new(py);
        d.set_item("method", method).unwrap();
        d.set_item("field", field).unwrap();
        let lits = PyList::empty(py);
        for v in literals {
            lits.append(*v).unwrap();
        }
        d.set_item("literals", lits).unwrap();
        d
    }

    #[test]
    fn equal_leaf_converts() {
        Python::attach(|py| {
            let fields = test_fields();
            let dict = leaf_dict(py, "equal", "id", &[1]);
            let pred = dict_to_predicate(&dict, &fields, true).unwrap();
            match &pred {
                Predicate::Leaf {
                    column,
                    op,
                    literals,
                    ..
                } => {
                    assert_eq!(column, "id");
                    assert_eq!(*op, PredicateOperator::Eq);
                    assert_eq!(literals, &[Datum::Int(1)]);
                }
                other => panic!("expected Leaf, got {other:?}"),
            }
        });
    }

    #[test]
    fn unsupported_operator_not_raises_not_implemented() {
        Python::attach(|py| {
            let fields = test_fields();
            let dict = leaf_dict(py, "not", "id", &[1]);
            let err = dict_to_predicate(&dict, &fields, true).unwrap_err();
            assert!(err.is_instance_of::<PyNotImplementedError>(py));
        });
    }

    // ---- string operators ----

    /// Build a leaf dict with string literals.
    fn str_leaf_dict<'py>(
        py: Python<'py>,
        method: &str,
        field: &str,
        literals: &[&str],
    ) -> Bound<'py, PyDict> {
        let d = PyDict::new(py);
        d.set_item("method", method).unwrap();
        d.set_item("field", field).unwrap();
        let lits = PyList::empty(py);
        for v in literals {
            lits.append(*v).unwrap();
        }
        d.set_item("literals", lits).unwrap();
        d
    }

    fn expect_leaf_op(pred: &Predicate, expected: PredicateOperator) {
        match pred {
            Predicate::Leaf { op, .. } => assert_eq!(*op, expected),
            other => panic!("expected Leaf, got {other:?}"),
        }
    }

    #[test]
    fn starts_with_leaf_converts() {
        Python::attach(|py| {
            let fields = test_fields();
            let dict = str_leaf_dict(py, "startsWith", "name", &["ab"]);
            let pred = dict_to_predicate(&dict, &fields, true).unwrap();
            expect_leaf_op(&pred, PredicateOperator::StartsWith);
        });
    }

    #[test]
    fn ends_with_leaf_converts() {
        Python::attach(|py| {
            let fields = test_fields();
            let dict = str_leaf_dict(py, "endsWith", "name", &["ab"]);
            let pred = dict_to_predicate(&dict, &fields, true).unwrap();
            expect_leaf_op(&pred, PredicateOperator::EndsWith);
        });
    }

    #[test]
    fn contains_leaf_converts() {
        Python::attach(|py| {
            let fields = test_fields();
            let dict = str_leaf_dict(py, "contains", "name", &["ab"]);
            let pred = dict_to_predicate(&dict, &fields, true).unwrap();
            expect_leaf_op(&pred, PredicateOperator::Contains);
        });
    }

    /// Fields whose string column is spelled `Name` (mixed case).
    fn mixed_case_fields() -> Vec<DataField> {
        vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "Name".to_string(),
                DataType::VarChar(VarCharType::default()),
            ),
        ]
    }

    #[test]
    fn case_insensitive_leaf_resolves_differently_cased_field() {
        Python::attach(|py| {
            let fields = mixed_case_fields();
            // Request uses `name`; the schema field is `Name`.
            let dict = str_leaf_dict(py, "equal", "name", &["bob"]);

            // Case-sensitive (default): `name` != `Name` → column not found.
            let err = dict_to_predicate(&dict, &fields, true).unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));

            // Case-insensitive: resolves to the canonical `Name`.
            let pred = dict_to_predicate(&dict, &fields, false).unwrap();
            match &pred {
                Predicate::Leaf { column, op, .. } => {
                    assert_eq!(
                        column, "Name",
                        "canonical schema name is stored in the leaf"
                    );
                    assert_eq!(*op, PredicateOperator::Eq);
                }
                other => panic!("expected Leaf, got {other:?}"),
            }
        });
    }

    #[test]
    fn case_insensitive_ambiguous_field_raises_value_error() {
        Python::attach(|py| {
            let fields = vec![
                DataField::new(0, "Col".to_string(), DataType::Int(IntType::new())),
                DataField::new(1, "col".to_string(), DataType::Int(IntType::new())),
            ];
            let dict = leaf_dict(py, "equal", "COL", &[1]);
            let err = dict_to_predicate(&dict, &fields, false).unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn like_prefix_pattern_optimizes_to_starts_with() {
        Python::attach(|py| {
            let fields = test_fields();
            let dict = str_leaf_dict(py, "like", "name", &["ab%"]);
            let pred = dict_to_predicate(&dict, &fields, true).unwrap();
            expect_leaf_op(&pred, PredicateOperator::StartsWith);
        });
    }

    #[test]
    fn like_residual_pattern_stays_like() {
        Python::attach(|py| {
            let fields = test_fields();
            let dict = str_leaf_dict(py, "like", "name", &["a%b%c"]);
            let pred = dict_to_predicate(&dict, &fields, true).unwrap();
            expect_leaf_op(&pred, PredicateOperator::Like);
        });
    }

    #[test]
    fn like_accepts_backslash_escape_literal() {
        Python::attach(|py| {
            let fields = test_fields();
            let dict = str_leaf_dict(py, "like", "name", &["100\\%%", "\\"]);
            let pred = dict_to_predicate(&dict, &fields, true).unwrap();
            // Escaped-wildcard patterns are not rewritten by the core's LIKE
            // optimization; they stay as a residual Like leaf.
            expect_leaf_op(&pred, PredicateOperator::Like);
        });
    }

    #[test]
    fn like_rejects_non_backslash_escape() {
        Python::attach(|py| {
            let fields = test_fields();
            let dict = str_leaf_dict(py, "like", "name", &["100!%%", "!"]);
            let err = dict_to_predicate(&dict, &fields, true).unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn like_rejects_multi_char_escape() {
        Python::attach(|py| {
            let fields = test_fields();
            let dict = str_leaf_dict(py, "like", "name", &["a%", "ab"]);
            let err = dict_to_predicate(&dict, &fields, true).unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn like_rejects_three_literals() {
        Python::attach(|py| {
            let fields = test_fields();
            let dict = str_leaf_dict(py, "like", "name", &["a%", "\\", "x"]);
            let err = dict_to_predicate(&dict, &fields, true).unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn string_op_empty_pattern_folds_to_is_not_null() {
        Python::attach(|py| {
            let fields = test_fields();
            for method in ["startsWith", "endsWith", "contains"] {
                let dict = str_leaf_dict(py, method, "name", &[""]);
                let pred = dict_to_predicate(&dict, &fields, true).unwrap();
                expect_leaf_op(&pred, PredicateOperator::IsNotNull);
            }
        });
    }

    #[test]
    fn string_op_on_non_string_column_raises_value_error() {
        Python::attach(|py| {
            let fields = test_fields();
            for method in ["startsWith", "endsWith", "contains", "like"] {
                let dict = str_leaf_dict(py, method, "id", &["a"]);
                let err = dict_to_predicate(&dict, &fields, true).unwrap_err();
                assert!(err.is_instance_of::<PyValueError>(py), "{method}");
            }
        });
    }

    #[test]
    fn string_op_wrong_literal_count_raises_value_error() {
        Python::attach(|py| {
            let fields = test_fields();
            for method in ["startsWith", "endsWith", "contains", "like"] {
                let dict = str_leaf_dict(py, method, "name", &[]);
                let err = dict_to_predicate(&dict, &fields, true).unwrap_err();
                assert!(err.is_instance_of::<PyValueError>(py), "{method} zero");
            }
            for method in ["startsWith", "endsWith", "contains"] {
                let dict = str_leaf_dict(py, method, "name", &["a", "b"]);
                let err = dict_to_predicate(&dict, &fields, true).unwrap_err();
                assert!(err.is_instance_of::<PyValueError>(py), "{method} two");
            }
        });
    }

    #[test]
    fn string_op_unknown_field_raises_value_error() {
        Python::attach(|py| {
            let fields = test_fields();
            // Now that string operators are supported, they follow the normal
            // leaf path: field resolution happens first, so an unknown field is
            // a ValueError (not NotImplementedError as before).
            let dict = str_leaf_dict(py, "like", "nope", &["x"]);
            let err = dict_to_predicate(&dict, &fields, true).unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn unsupported_operator_not_without_field_raises_not_implemented() {
        Python::attach(|py| {
            let fields = test_fields();
            // 'not' with no 'field', only empty 'children': operator support is
            // decided before any shape validation.
            let dict = PyDict::new(py);
            dict.set_item("method", "not").unwrap();
            dict.set_item("children", PyList::empty(py)).unwrap();
            let err = dict_to_predicate(&dict, &fields, true).unwrap_err();
            assert!(err.is_instance_of::<PyNotImplementedError>(py));
        });
    }

    #[test]
    fn unknown_field_raises_value_error() {
        Python::attach(|py| {
            let fields = test_fields();
            let dict = leaf_dict(py, "equal", "nope", &[1]);
            let err = dict_to_predicate(&dict, &fields, true).unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn empty_children_raises_value_error() {
        Python::attach(|py| {
            let fields = test_fields();
            let dict = PyDict::new(py);
            dict.set_item("method", "and").unwrap();
            dict.set_item("children", PyList::empty(py)).unwrap();
            let err = dict_to_predicate(&dict, &fields, true).unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn compound_with_unsupported_child_fails() {
        Python::attach(|py| {
            let fields = test_fields();
            let ok = leaf_dict(py, "equal", "id", &[1]);
            let bad = leaf_dict(py, "not", "name", &[]);
            let children = PyList::empty(py);
            children.append(ok).unwrap();
            children.append(bad).unwrap();
            let dict = PyDict::new(py);
            dict.set_item("method", "and").unwrap();
            dict.set_item("children", children).unwrap();
            let err = dict_to_predicate(&dict, &fields, true).unwrap_err();
            // No partial pushdown: the unsupported child propagates.
            assert!(err.is_instance_of::<PyNotImplementedError>(py));
        });
    }

    #[test]
    fn and_compound_converts() {
        Python::attach(|py| {
            let fields = test_fields();
            let c1 = leaf_dict(py, "equal", "id", &[1]);
            let c2 = leaf_dict(py, "greaterThan", "id", &[0]);
            let children = PyList::empty(py);
            children.append(c1).unwrap();
            children.append(c2).unwrap();
            let dict = PyDict::new(py);
            dict.set_item("method", "and").unwrap();
            dict.set_item("children", children).unwrap();
            let pred = dict_to_predicate(&dict, &fields, true).unwrap();
            match &pred {
                Predicate::And(ch) => assert_eq!(ch.len(), 2),
                other => panic!("expected And, got {other:?}"),
            }
        });
    }

    #[test]
    fn or_compound_converts() {
        Python::attach(|py| {
            let fields = test_fields();
            let c1 = leaf_dict(py, "equal", "id", &[1]);
            let c2 = leaf_dict(py, "equal", "id", &[2]);
            let children = PyList::empty(py);
            children.append(c1).unwrap();
            children.append(c2).unwrap();
            let dict = PyDict::new(py);
            dict.set_item("method", "or").unwrap();
            dict.set_item("children", children).unwrap();
            let pred = dict_to_predicate(&dict, &fields, true).unwrap();
            match &pred {
                Predicate::Or(ch) => assert_eq!(ch.len(), 2),
                other => panic!("expected Or, got {other:?}"),
            }
        });
    }

    #[test]
    fn none_literal_raises_value_error() {
        Python::attach(|py| {
            let fields = test_fields();
            let d = PyDict::new(py);
            d.set_item("method", "equal").unwrap();
            d.set_item("field", "id").unwrap();
            let lits = PyList::empty(py);
            lits.append(py.None()).unwrap();
            d.set_item("literals", lits).unwrap();
            let err = dict_to_predicate(&d, &fields, true).unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn wrong_literal_count_raises_value_error() {
        Python::attach(|py| {
            let fields = test_fields();
            let dict = leaf_dict(py, "equal", "id", &[1, 2]);
            let err = dict_to_predicate(&dict, &fields, true).unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn null_check_with_literals_raises_value_error() {
        Python::attach(|py| {
            let fields = test_fields();
            for method in ["isNull", "isNotNull"] {
                let dict = leaf_dict(py, method, "name", &[1]);
                let err = dict_to_predicate(&dict, &fields, true).unwrap_err();
                assert!(err.is_instance_of::<PyValueError>(py));
            }
        });
    }

    #[test]
    fn null_check_accepts_empty_and_missing_literals() {
        Python::attach(|py| {
            let fields = test_fields();
            // Empty literals list is accepted.
            let with_empty = leaf_dict(py, "isNull", "name", &[]);
            assert!(dict_to_predicate(&with_empty, &fields, true).is_ok());
            // Missing literals key is accepted.
            let no_lits = PyDict::new(py);
            no_lits.set_item("method", "isNotNull").unwrap();
            no_lits.set_item("field", "name").unwrap();
            assert!(dict_to_predicate(&no_lits, &fields, true).is_ok());
        });
    }

    #[test]
    fn int_field_accepts_in_range_int() {
        Python::attach(|py| {
            let v = 42i64.into_pyobject(py).unwrap();
            let d = py_to_datum(&v, &DataType::Int(Default::default())).unwrap();
            assert_eq!(d, Datum::Int(42));
        });
    }

    #[test]
    fn int_field_rejects_out_of_range() {
        Python::attach(|py| {
            let v = 9_999_999_999i64.into_pyobject(py).unwrap();
            assert!(py_to_datum(&v, &DataType::Int(Default::default())).is_err());
        });
    }

    #[test]
    fn int_field_rejects_bool() {
        Python::attach(|py| {
            let v = true.into_pyobject(py).unwrap();
            assert!(py_to_datum(v.as_any(), &DataType::Int(Default::default())).is_err());
        });
    }

    #[test]
    fn boolean_field_accepts_bool() {
        Python::attach(|py| {
            let v = true.into_pyobject(py).unwrap();
            let d = py_to_datum(v.as_any(), &DataType::Boolean(Default::default())).unwrap();
            assert_eq!(d, Datum::Bool(true));
        });
    }

    #[test]
    fn boolean_field_rejects_non_bool() {
        Python::attach(|py| {
            let v = 1i64.into_pyobject(py).unwrap();
            assert!(py_to_datum(&v, &DataType::Boolean(Default::default())).is_err());
        });
    }

    #[test]
    fn tinyint_range_enforced() {
        Python::attach(|py| {
            let ok = 127i64.into_pyobject(py).unwrap();
            assert_eq!(
                py_to_datum(&ok, &DataType::TinyInt(Default::default())).unwrap(),
                Datum::TinyInt(127)
            );
            let bad = 128i64.into_pyobject(py).unwrap();
            assert!(py_to_datum(&bad, &DataType::TinyInt(Default::default())).is_err());
        });
    }

    #[test]
    fn smallint_range_enforced() {
        Python::attach(|py| {
            let ok = (-32768i64).into_pyobject(py).unwrap();
            assert_eq!(
                py_to_datum(&ok, &DataType::SmallInt(Default::default())).unwrap(),
                Datum::SmallInt(-32768)
            );
            let bad = 32768i64.into_pyobject(py).unwrap();
            assert!(py_to_datum(&bad, &DataType::SmallInt(Default::default())).is_err());
        });
    }

    #[test]
    fn bigint_accepts_long() {
        Python::attach(|py| {
            let v = 9_999_999_999i64.into_pyobject(py).unwrap();
            assert_eq!(
                py_to_datum(&v, &DataType::BigInt(Default::default())).unwrap(),
                Datum::Long(9_999_999_999)
            );
        });
    }

    #[test]
    fn float_accepts_int_and_float_rejects_bool() {
        Python::attach(|py| {
            let from_int = 3i64.into_pyobject(py).unwrap();
            assert_eq!(
                py_to_datum(&from_int, &DataType::Float(Default::default())).unwrap(),
                Datum::Float(3.0)
            );
            let from_float = 2.5f64.into_pyobject(py).unwrap();
            assert_eq!(
                py_to_datum(&from_float, &DataType::Double(Default::default())).unwrap(),
                Datum::Double(2.5)
            );
            let b = true.into_pyobject(py).unwrap();
            assert!(py_to_datum(b.as_any(), &DataType::Double(Default::default())).is_err());
        });
    }

    #[test]
    fn string_field_accepts_str() {
        Python::attach(|py| {
            let v = "hello".into_pyobject(py).unwrap();
            assert_eq!(
                py_to_datum(v.as_any(), &DataType::VarChar(Default::default())).unwrap(),
                Datum::String("hello".to_string())
            );
        });
    }

    #[test]
    fn string_field_rejects_non_str() {
        Python::attach(|py| {
            let v = 5i64.into_pyobject(py).unwrap();
            assert!(py_to_datum(&v, &DataType::VarChar(Default::default())).is_err());
        });
    }

    #[test]
    fn timestamp_field_rejects_non_datetime() {
        Python::attach(|py| {
            let v = 0i64.into_pyobject(py).unwrap();
            let err = py_to_datum(&v, &DataType::Timestamp(Default::default())).unwrap_err();
            assert!(err.is_instance_of::<pyo3::exceptions::PyValueError>(py));
        });
    }

    // ---- temporal literals ----

    /// `datetime.date(2024, 1, 1)` epoch days (1970-01-01 = 0).
    const D_2024_01_01: i32 = 19723;

    fn py_date<'py>(py: Python<'py>, y: i32, m: u8, d: u8) -> Bound<'py, PyAny> {
        pyo3::types::PyDate::new(py, y, m, d).unwrap().into_any()
    }

    fn py_time<'py>(
        py: Python<'py>,
        h: u8,
        min: u8,
        s: u8,
        micro: u32,
        tz: Option<&Bound<'py, pyo3::types::PyTzInfo>>,
    ) -> Bound<'py, PyAny> {
        pyo3::types::PyTime::new(py, h, min, s, micro, tz)
            .unwrap()
            .into_any()
    }

    #[allow(clippy::too_many_arguments)]
    fn py_datetime<'py>(
        py: Python<'py>,
        y: i32,
        mo: u8,
        d: u8,
        h: u8,
        mi: u8,
        s: u8,
        micro: u32,
        tz: Option<&Bound<'py, pyo3::types::PyTzInfo>>,
    ) -> Bound<'py, PyAny> {
        pyo3::types::PyDateTime::new(py, y, mo, d, h, mi, s, micro, tz)
            .unwrap()
            .into_any()
    }

    fn utc(py: Python<'_>) -> Bound<'_, pyo3::types::PyTzInfo> {
        pyo3::types::PyTzInfo::utc(py).unwrap().to_owned()
    }

    fn fixed_offset(py: Python<'_>, seconds: i32) -> Bound<'_, pyo3::types::PyTzInfo> {
        let delta = pyo3::types::PyDelta::new(py, 0, seconds, 0, false).unwrap();
        pyo3::types::PyTzInfo::fixed_offset(py, &delta).unwrap()
    }

    #[test]
    fn date_field_accepts_date() {
        Python::attach(|py| {
            let v = py_date(py, 2024, 1, 1);
            assert_eq!(
                py_to_datum(&v, &DataType::Date(Default::default())).unwrap(),
                Datum::Date(D_2024_01_01)
            );
        });
    }

    #[test]
    fn date_field_accepts_pre_epoch_date() {
        Python::attach(|py| {
            let v = py_date(py, 1969, 12, 31);
            assert_eq!(
                py_to_datum(&v, &DataType::Date(Default::default())).unwrap(),
                Datum::Date(-1)
            );
        });
    }

    #[test]
    fn date_field_rejects_datetime() {
        Python::attach(|py| {
            // datetime is a date subclass; accepting it would silently drop the
            // time-of-day part.
            let v = py_datetime(py, 2024, 1, 1, 0, 0, 0, 0, None);
            let err = py_to_datum(&v, &DataType::Date(Default::default())).unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn date_field_rejects_str() {
        Python::attach(|py| {
            let v = "2024-01-01".into_pyobject(py).unwrap();
            let err = py_to_datum(v.as_any(), &DataType::Date(Default::default())).unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn time_field_accepts_naive_time() {
        Python::attach(|py| {
            let v = py_time(py, 12, 34, 56, 789_000, None);
            assert_eq!(
                py_to_datum(&v, &DataType::Time(Default::default())).unwrap(),
                Datum::Time(45_296_789)
            );
        });
    }

    #[test]
    fn time_field_rejects_sub_millisecond() {
        Python::attach(|py| {
            // Datum::Time carries millis-of-day; silently truncating micros
            // would change equality semantics.
            let v = py_time(py, 0, 0, 0, 500, None);
            let err = py_to_datum(&v, &DataType::Time(Default::default())).unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn time_field_rejects_aware_time() {
        Python::attach(|py| {
            let tz = utc(py);
            let v = py_time(py, 1, 2, 3, 0, Some(&tz));
            let err = py_to_datum(&v, &DataType::Time(Default::default())).unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn timestamp_field_accepts_naive_datetime() {
        Python::attach(|py| {
            let v = py_datetime(py, 2024, 1, 1, 0, 0, 0, 123_456, None);
            assert_eq!(
                py_to_datum(&v, &DataType::Timestamp(Default::default())).unwrap(),
                Datum::Timestamp {
                    millis: 1_704_067_200_123,
                    nanos: 456_000,
                }
            );
        });
    }

    #[test]
    fn timestamp_field_pre_epoch_uses_euclidean_split() {
        Python::attach(|py| {
            // 1969-12-31 23:59:59.999999 = -1 µs from epoch
            // → floor millis -1, non-negative nanos remainder 999_000.
            let v = py_datetime(py, 1969, 12, 31, 23, 59, 59, 999_999, None);
            assert_eq!(
                py_to_datum(&v, &DataType::Timestamp(Default::default())).unwrap(),
                Datum::Timestamp {
                    millis: -1,
                    nanos: 999_000,
                }
            );
        });
    }

    #[test]
    fn timestamp_field_rejects_aware_datetime() {
        Python::attach(|py| {
            // TIMESTAMP is timezone-less; parity with the DataFusion path which
            // refuses zoned literals for it.
            let tz = utc(py);
            let v = py_datetime(py, 2024, 1, 1, 0, 0, 0, 0, Some(&tz));
            let err = py_to_datum(&v, &DataType::Timestamp(Default::default())).unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn timestamp_field_rejects_date() {
        Python::attach(|py| {
            let v = py_date(py, 2024, 1, 1);
            let err = py_to_datum(&v, &DataType::Timestamp(Default::default())).unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn local_zoned_timestamp_field_accepts_aware_datetime() {
        Python::attach(|py| {
            // 2024-01-01T08:00:00+08:00 == 2024-01-01T00:00:00Z.
            let tz = fixed_offset(py, 8 * 3600);
            let v = py_datetime(py, 2024, 1, 1, 8, 0, 0, 0, Some(&tz));
            assert_eq!(
                py_to_datum(&v, &DataType::LocalZonedTimestamp(Default::default())).unwrap(),
                Datum::LocalZonedTimestamp {
                    millis: 1_704_067_200_000,
                    nanos: 0,
                }
            );
        });
    }

    #[test]
    fn local_zoned_timestamp_field_rejects_naive_datetime() {
        Python::attach(|py| {
            let v = py_datetime(py, 2024, 1, 1, 0, 0, 0, 0, None);
            let err =
                py_to_datum(&v, &DataType::LocalZonedTimestamp(Default::default())).unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    /// Build a datetime whose tzinfo subclass returns None from utcoffset() —
    /// naive by Python's definition (docs: "aware if tzinfo is not None AND
    /// utcoffset() does not return None") despite tzinfo being set.
    fn pseudo_naive_datetime(py: Python<'_>) -> Bound<'_, PyAny> {
        let ns = PyDict::new(py);
        py.run(
            c"import datetime
class FloatingTz(datetime.tzinfo):
    def utcoffset(self, dt): return None
    def dst(self, dt): return None
    def tzname(self, dt): return 'FLOATING'
value = datetime.datetime(2024, 1, 1, tzinfo=FloatingTz())",
            None,
            Some(&ns),
        )
        .unwrap();
        ns.get_item("value").unwrap().unwrap()
    }

    #[test]
    fn local_zoned_timestamp_field_rejects_pseudo_naive_tzinfo() {
        Python::attach(|py| {
            // tzinfo is set but utcoffset() is None: astimezone(utc) would
            // interpret the value as process-local time, so the same literal
            // would normalize differently per machine. Must be rejected.
            let v = pseudo_naive_datetime(py);
            let err =
                py_to_datum(&v, &DataType::LocalZonedTimestamp(Default::default())).unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn timestamp_field_rejects_pseudo_naive_tzinfo() {
        Python::attach(|py| {
            // Timestamp deliberately rejects ANY tzinfo, even one whose
            // utcoffset() is None: stricter than Python's naive/aware
            // definition, but explicit — the caller strips tzinfo to state
            // wall-clock intent.
            let v = pseudo_naive_datetime(py);
            let err = py_to_datum(&v, &DataType::Timestamp(Default::default())).unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    // ---- decimal literals ----

    fn py_decimal<'py>(py: Python<'py>, repr: &str) -> Bound<'py, PyAny> {
        py.import("decimal")
            .unwrap()
            .getattr("Decimal")
            .unwrap()
            .call1((repr,))
            .unwrap()
    }

    fn decimal_type(precision: u32, scale: u32) -> DataType {
        DataType::Decimal(paimon::spec::DecimalType::new(precision, scale).unwrap())
    }

    #[test]
    fn decimal_field_accepts_exact_scale() {
        Python::attach(|py| {
            let v = py_decimal(py, "12.34");
            assert_eq!(
                py_to_datum(&v, &decimal_type(10, 2)).unwrap(),
                Datum::Decimal {
                    unscaled: 1234,
                    precision: 10,
                    scale: 2,
                }
            );
        });
    }

    #[test]
    fn decimal_field_rescales_losslessly() {
        Python::attach(|py| {
            let v = py_decimal(py, "1.5");
            assert_eq!(
                py_to_datum(&v, &decimal_type(10, 2)).unwrap(),
                Datum::Decimal {
                    unscaled: 150,
                    precision: 10,
                    scale: 2,
                }
            );
        });
    }

    #[test]
    fn decimal_field_accepts_scientific_notation() {
        Python::attach(|py| {
            let v = py_decimal(py, "1E+2");
            assert_eq!(
                py_to_datum(&v, &decimal_type(10, 2)).unwrap(),
                Datum::Decimal {
                    unscaled: 10_000,
                    precision: 10,
                    scale: 2,
                }
            );
        });
    }

    #[test]
    fn decimal_field_negative_value() {
        Python::attach(|py| {
            let v = py_decimal(py, "-12.34");
            assert_eq!(
                py_to_datum(&v, &decimal_type(10, 2)).unwrap(),
                Datum::Decimal {
                    unscaled: -1234,
                    precision: 10,
                    scale: 2,
                }
            );
        });
    }

    #[test]
    fn decimal_field_accepts_int_literal() {
        Python::attach(|py| {
            let v = 7i64.into_pyobject(py).unwrap();
            assert_eq!(
                py_to_datum(&v, &decimal_type(10, 2)).unwrap(),
                Datum::Decimal {
                    unscaled: 700,
                    precision: 10,
                    scale: 2,
                }
            );
        });
    }

    #[test]
    fn decimal_field_rejects_rounding() {
        Python::attach(|py| {
            // 0.005 cannot be represented at scale 2 without rounding.
            let v = py_decimal(py, "0.005");
            let err = py_to_datum(&v, &decimal_type(10, 2)).unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn decimal_field_rejects_precision_overflow() {
        Python::attach(|py| {
            let v = py_decimal(py, "123.45");
            let err = py_to_datum(&v, &decimal_type(4, 2)).unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn decimal_field_rejects_non_finite() {
        Python::attach(|py| {
            for repr in ["NaN", "Infinity", "-Infinity"] {
                let v = py_decimal(py, repr);
                let err = py_to_datum(&v, &decimal_type(10, 2)).unwrap_err();
                assert!(err.is_instance_of::<PyValueError>(py), "{repr}");
            }
        });
    }

    #[test]
    fn decimal_field_rejects_float() {
        Python::attach(|py| {
            // Binary floats are not exact decimal values; requiring
            // decimal.Decimal keeps the conversion lossless.
            let v = 1.5f64.into_pyobject(py).unwrap();
            let err = py_to_datum(&v, &decimal_type(10, 2)).unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn decimal_field_rejects_bool() {
        Python::attach(|py| {
            let v = true.into_pyobject(py).unwrap();
            let err = py_to_datum(v.as_any(), &decimal_type(10, 2)).unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn unsupported_field_type_still_not_implemented() {
        Python::attach(|py| {
            // Bytes/complex types remain out of scope for literal conversion.
            let v = 0i64.into_pyobject(py).unwrap();
            for dt in [
                DataType::Binary(Default::default()),
                DataType::Array(paimon::spec::ArrayType::new(DataType::Int(IntType::new()))),
            ] {
                let err = py_to_datum(&v, &dt).unwrap_err();
                assert!(err.is_instance_of::<pyo3::exceptions::PyNotImplementedError>(py));
            }
        });
    }

    #[test]
    fn value_errors_use_pyvalueerror() {
        Python::attach(|py| {
            let v = 9_999_999_999i64.into_pyobject(py).unwrap();
            let err = py_to_datum(&v, &DataType::Int(Default::default())).unwrap_err();
            assert!(err.is_instance_of::<pyo3::exceptions::PyValueError>(py));
        });
    }
}
