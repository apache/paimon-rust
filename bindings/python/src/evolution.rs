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

use arrow::array::{make_array, Array, ArrayData};
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use paimon::spec::{DataField, DataType};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::error::to_py_err;

/// Arrow and logical-type transport only; evolution belongs to Rust core.
#[pyfunction]
fn cast_array_for_schema_evolution(
    py: Python<'_>,
    column: &Bound<'_, PyAny>,
    source_type: &str,
    target_type: &str,
) -> PyResult<Py<PyAny>> {
    let parse = |value| -> PyResult<DataType> {
        serde_json::from_str(value)
            .map_err(|error| PyValueError::new_err(format!("Invalid Paimon type: {error}")))
    };
    let source = parse(source_type)?;
    let target = parse(target_type)?;
    let data = ArrayData::from_pyarrow_bound(column)?;
    data.validate_full().map_err(|error| {
        PyValueError::new_err(format!("Invalid Arrow evolution input: {error}"))
    })?;
    let column = make_array(data);
    let output = py
        .detach(|| paimon::arrow::cast_array_for_schema_evolution(&column, &source, &target))
        .map_err(to_py_err)?;
    Ok(output.to_data().to_pyarrow(py)?.unbind())
}

/// Transport the core runtime schema; Python does not duplicate read nullability.
#[pyfunction]
fn read_arrow_schema(py: Python<'_>, fields: &str) -> PyResult<Py<PyAny>> {
    let fields: Vec<DataField> = serde_json::from_str(fields)
        .map_err(|error| PyValueError::new_err(format!("Invalid Paimon fields: {error}")))?;
    let schema = paimon::arrow::build_read_arrow_schema(&fields).map_err(to_py_err)?;
    Ok(schema.as_ref().to_pyarrow(py)?.unbind())
}

/// Reuse the writer's existing normalization and value-sensitive constraints.
#[pyfunction]
fn normalize_array_for_write(
    py: Python<'_>,
    column: &Bound<'_, PyAny>,
    target_type: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let target = arrow::datatypes::DataType::from_pyarrow_bound(target_type)?;
    let data = ArrayData::from_pyarrow_bound(column)?;
    data.validate_full()
        .map_err(|error| PyValueError::new_err(format!("Invalid Arrow write input: {error}")))?;
    let column = make_array(data);
    let output = py
        .detach(|| paimon::table::normalize_write_array(&column, &target))
        .map_err(to_py_err)?;
    Ok(output.to_data().to_pyarrow(py)?.unbind())
}

pub(crate) fn register_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let data = m.getattr("data")?.cast_into::<PyModule>()?;
    data.add_function(wrap_pyfunction!(cast_array_for_schema_evolution, &data)?)?;
    data.add_function(wrap_pyfunction!(read_arrow_schema, &data)?)?;
    data.add_function(wrap_pyfunction!(normalize_array_for_write, &data)?)
}
