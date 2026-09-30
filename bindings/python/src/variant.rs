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

use arrow::array::{make_array, Array, ArrayData, StructArray};
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use pyo3::prelude::*;

use crate::error::to_py_err;

/// Extract literal top-level numeric Variant fields into a row-major float32 array.
#[pyfunction]
fn variant_get_float32(
    py: Python<'_>,
    column: &Bound<'_, PyAny>,
    fields: Vec<String>,
) -> PyResult<Py<PyAny>> {
    let column = make_array(ArrayData::from_pyarrow_bound(column)?);
    let output = py
        .detach(|| {
            let column = column
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| paimon::Error::DataInvalid {
                    message: "Expected a PyArrow Variant StructArray".to_string(),
                    source: None,
                })?;
            paimon::arrow::variant_get_float32(column, &fields)
        })
        .map_err(to_py_err)?;
    Ok(output.to_data().to_pyarrow(py)?.unbind())
}

pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let this = PyModule::new(py, "data")?;
    this.add_function(wrap_pyfunction!(variant_get_float32, &this)?)?;
    m.add_submodule(&this)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("pypaimon_rust.data", this)?;
    Ok(())
}
