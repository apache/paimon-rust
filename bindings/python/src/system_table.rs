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

//! Thin bindings for core metadata reads.

use crate::error::to_py_err;
use crate::schema::PyDataField;
use arrow::pyarrow::ToPyArrow;
use paimon::table::SystemRead;
use paimon_datafusion::runtime::runtime;
use pyo3::prelude::*;
use pyo3::types::PyDict;

#[pyclass(name = "SystemRead", module = "pypaimon_rust.datafusion")]
pub struct PySystemRead {
    pub(crate) inner: SystemRead,
}

#[pymethods]
impl PySystemRead {
    fn read_type(&self) -> Vec<PyDataField> {
        self.inner
            .read_type()
            .into_iter()
            .map(PyDataField::new)
            .collect()
    }

    fn with_projection(
        mut slf: PyRefMut<'_, Self>,
        columns: Vec<String>,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let columns: Vec<_> = columns.iter().map(String::as_str).collect();
        slf.inner.with_projection(&columns).map_err(to_py_err)?;
        Ok(slf)
    }

    fn with_filter<'py>(
        mut slf: PyRefMut<'py, Self>,
        predicate: &Bound<'_, PyDict>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let predicate = crate::predicate::dict_to_predicate(
            predicate,
            slf.inner.full_row_type().fields(),
            true,
        )?;
        slf.inner.with_filter(predicate).map_err(to_py_err)?;
        Ok(slf)
    }

    fn with_limit(mut slf: PyRefMut<'_, Self>, limit: usize) -> PyRefMut<'_, Self> {
        slf.inner.with_limit(limit);
        slf
    }

    fn read(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let batch = py
            .detach(|| runtime().block_on(self.inner.read()))
            .map_err(to_py_err)?;
        Ok(batch.to_pyarrow(py)?.unbind())
    }
}
