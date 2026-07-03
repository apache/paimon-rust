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

use paimon::spec::{DataField, DataType, Datum, Predicate, PredicateBuilder};
use pyo3::exceptions::{PyNotImplementedError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyList, PyString};

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
/// - All other types (Date/Time/Timestamp/Decimal/Bytes/complex) are not
///   supported yet and raise `NotImplementedError`.
///
/// Errors:
/// - `ValueError` for type mismatches and out-of-range integers.
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

/// Operators recognized by the lightweight dict format but not translatable to a
/// Rust [`Predicate`] for pushdown.
const METHOD_NOT_SUPPORTED: &[&str] = &["like", "startsWith", "endsWith", "contains", "not"];

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
                preds.push(dict_to_predicate(child_dict, fields)?);
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
        _ => leaf_to_predicate(&method, node, fields),
    }
}

/// Convert a single leaf dict (already known not to be `and`/`or`) into a
/// [`Predicate`], resolving the field type from the schema.
fn leaf_to_predicate(
    method: &str,
    node: &Bound<'_, PyDict>,
    fields: &[DataField],
) -> PyResult<Predicate> {
    let field: String = node
        .get_item("field")?
        .ok_or_else(|| PyValueError::new_err(format!("'{method}' leaf requires 'field'")))?
        .extract()?;

    // Resolve field DataType from schema (authoritative) for literal conversion.
    let data_type = fields
        .iter()
        .find(|f| f.name() == field)
        .map(|f| f.data_type().clone())
        .ok_or_else(|| PyValueError::new_err(format!("Column '{field}' not found in schema")))?;

    let literals_obj = node.get_item("literals")?;
    let pb = PredicateBuilder::new(fields);

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
            let pred = dict_to_predicate(&dict, &fields).unwrap();
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
    fn unsupported_operator_like_raises_not_implemented() {
        Python::attach(|py| {
            let fields = test_fields();
            let dict = leaf_dict(py, "like", "name", &[]);
            let err = dict_to_predicate(&dict, &fields).unwrap_err();
            assert!(err.is_instance_of::<PyNotImplementedError>(py));
        });
    }

    #[test]
    fn unsupported_operator_not_raises_not_implemented() {
        Python::attach(|py| {
            let fields = test_fields();
            let dict = leaf_dict(py, "not", "id", &[1]);
            let err = dict_to_predicate(&dict, &fields).unwrap_err();
            assert!(err.is_instance_of::<PyNotImplementedError>(py));
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
            let err = dict_to_predicate(&dict, &fields).unwrap_err();
            assert!(err.is_instance_of::<PyNotImplementedError>(py));
        });
    }

    #[test]
    fn unsupported_operator_like_with_unknown_field_raises_not_implemented() {
        Python::attach(|py| {
            let fields = test_fields();
            // 'like' with an unknown field: unsupported operator precedes field
            // resolution, so NotImplementedError (not the unknown-field ValueError).
            let dict = leaf_dict(py, "like", "nope", &[]);
            let err = dict_to_predicate(&dict, &fields).unwrap_err();
            assert!(err.is_instance_of::<PyNotImplementedError>(py));
        });
    }

    #[test]
    fn unknown_field_raises_value_error() {
        Python::attach(|py| {
            let fields = test_fields();
            let dict = leaf_dict(py, "equal", "nope", &[1]);
            let err = dict_to_predicate(&dict, &fields).unwrap_err();
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
            let err = dict_to_predicate(&dict, &fields).unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn compound_with_unsupported_child_fails() {
        Python::attach(|py| {
            let fields = test_fields();
            let ok = leaf_dict(py, "equal", "id", &[1]);
            let bad = leaf_dict(py, "like", "name", &[]);
            let children = PyList::empty(py);
            children.append(ok).unwrap();
            children.append(bad).unwrap();
            let dict = PyDict::new(py);
            dict.set_item("method", "and").unwrap();
            dict.set_item("children", children).unwrap();
            let err = dict_to_predicate(&dict, &fields).unwrap_err();
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
            let pred = dict_to_predicate(&dict, &fields).unwrap();
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
            let pred = dict_to_predicate(&dict, &fields).unwrap();
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
            let err = dict_to_predicate(&d, &fields).unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn wrong_literal_count_raises_value_error() {
        Python::attach(|py| {
            let fields = test_fields();
            let dict = leaf_dict(py, "equal", "id", &[1, 2]);
            let err = dict_to_predicate(&dict, &fields).unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn null_check_with_literals_raises_value_error() {
        Python::attach(|py| {
            let fields = test_fields();
            for method in ["isNull", "isNotNull"] {
                let dict = leaf_dict(py, method, "name", &[1]);
                let err = dict_to_predicate(&dict, &fields).unwrap_err();
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
            assert!(dict_to_predicate(&with_empty, &fields).is_ok());
            // Missing literals key is accepted.
            let no_lits = PyDict::new(py);
            no_lits.set_item("method", "isNotNull").unwrap();
            no_lits.set_item("field", "name").unwrap();
            assert!(dict_to_predicate(&no_lits, &fields).is_ok());
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
    fn timestamp_field_is_not_implemented() {
        Python::attach(|py| {
            let v = 0i64.into_pyobject(py).unwrap();
            let err = py_to_datum(&v, &DataType::Timestamp(Default::default())).unwrap_err();
            assert!(err.is_instance_of::<pyo3::exceptions::PyNotImplementedError>(py));
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
