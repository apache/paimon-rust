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

//! Python object and callback conversion for core update assignments.

use arrow::array::{make_array, ArrayData, ArrayRef};
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use paimon::spec::DataField;
use paimon::table::UpdateAssignment;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::sync::{Arc, Mutex};

use crate::error::to_py_err;

type CallbackError = Arc<Mutex<Option<PyErr>>>;

fn bridge_error(error: PyErr, target: &CallbackError) -> paimon::Error {
    *target.lock().unwrap() = Some(error);
    paimon::Error::DataInvalid {
        message: "Python assignment evaluation failed".into(),
        source: None,
    }
}

fn arrow_arrays(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Vec<ArrayRef>> {
    let pa = py.import("pyarrow")?;
    if value.is_instance(&pa.getattr("Array")?)? {
        Ok(vec![make_array(ArrayData::from_pyarrow_bound(value)?)])
    } else if value.is_instance(&pa.getattr("ChunkedArray")?)? {
        value
            .getattr("chunks")?
            .try_iter()?
            .map(|chunk| Ok(make_array(ArrayData::from_pyarrow_bound(&chunk?)?)))
            .collect()
    } else {
        Err(PyValueError::new_err(
            "Callable assignment must return a pyarrow.Array or pyarrow.ChunkedArray",
        ))
    }
}

pub(crate) fn from_python(
    py: Python<'_>,
    assignments: &Bound<'_, PyDict>,
    fields: &[DataField],
    error: CallbackError,
) -> PyResult<Vec<(String, UpdateAssignment)>> {
    let schema = paimon::arrow::build_target_arrow_schema(fields).map_err(to_py_err)?;
    let pa = py.import("pyarrow")?;
    let array_type = pa.getattr("Array")?;
    let chunked_type = pa.getattr("ChunkedArray")?;
    assignments
        .iter()
        .map(|(name, value)| {
            let name: String = name.extract()?;
            let assignment = if value.is_callable() {
                let value = value.unbind();
                let error = error.clone();
                UpdateAssignment::Function(Arc::new(move |batches| {
                    // A Python callback may invoke another native read or write.
                    tokio::task::block_in_place(|| {
                        Python::attach(|py| -> PyResult<_> {
                            let pa = py.import("pyarrow")?;
                            let batches = batches
                                .iter()
                                .map(|batch| batch.to_pyarrow(py))
                                .collect::<PyResult<Vec<_>>>()?;
                            let matched = pa
                                .getattr("Table")?
                                .call_method1("from_batches", (PyList::new(py, batches)?,))?;
                            arrow_arrays(py, &value.bind(py).call1((matched,))?)
                        })
                    })
                    .map_err(|cause| bridge_error(cause, &error))
                }))
            } else if value.is_instance(&array_type)? || value.is_instance(&chunked_type)? {
                UpdateAssignment::Array(arrow_arrays(py, &value)?)
            } else {
                // Literal conversion stays lazy: a no-match update must neither
                // evaluate callbacks nor reject a scalar that is never assigned.
                let value = value.unbind();
                let name = name.clone();
                let schema = schema.clone();
                let error = error.clone();
                UpdateAssignment::DeferredScalar(Arc::new(move || {
                    tokio::task::block_in_place(|| {
                        Python::attach(|py| -> PyResult<_> {
                            let pa = py.import("pyarrow")?;
                            let value = value.bind(py);
                            let scalar = if value.is_instance(&pa.getattr("Scalar")?)? {
                                value.call_method0("as_py")?
                            } else {
                                value.clone()
                            };
                            let kwargs = PyDict::new(py);
                            kwargs.set_item(
                                "type",
                                schema
                                    .to_pyarrow(py)?
                                    .call_method1("field", (&name,))?
                                    .getattr("type")?,
                            )?;
                            let one = pa.call_method(
                                "array",
                                (PyList::new(py, [scalar])?,),
                                Some(&kwargs),
                            )?;
                            Ok(make_array(ArrayData::from_pyarrow_bound(&one)?))
                        })
                    })
                    .map_err(|cause| bridge_error(cause, &error))
                }))
            };
            Ok((name, assignment))
        })
        .collect()
}
