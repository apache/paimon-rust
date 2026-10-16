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

//! Thin bindings for the core read-only system-table API.

use crate::error::to_py_err;
use crate::read::PyRecordBatchReader;
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use paimon::table::{
    SystemReadBuilder, SystemSplit, SystemTable, SystemTablePlan, SystemTableRead, SystemTableScan,
};
use paimon_datafusion::runtime::runtime;
use pyo3::prelude::*;
use pyo3::types::PyDict;

#[pyclass(name = "SystemTable", module = "pypaimon_rust.datafusion")]
pub struct PySystemTable {
    pub(crate) inner: SystemTable,
}

#[pymethods]
impl PySystemTable {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn schema(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self.inner.schema().to_pyarrow(py)?.unbind())
    }
    fn new_read_builder(&self) -> PySystemReadBuilder {
        PySystemReadBuilder {
            inner: self.inner.new_read_builder(),
        }
    }
}

#[pyclass(name = "SystemReadBuilder", module = "pypaimon_rust.datafusion")]
pub struct PySystemReadBuilder {
    inner: SystemReadBuilder,
}

#[pymethods]
impl PySystemReadBuilder {
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
        // Predicates must see the full system-table schema, including unprojected columns.
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
    fn new_scan(&self) -> PySystemTableScan {
        PySystemTableScan {
            inner: self.inner.new_scan(),
        }
    }
    fn new_read(&self) -> PySystemTableRead {
        PySystemTableRead {
            inner: self.inner.new_read(),
        }
    }
}

#[pyclass(name = "SystemTableScan", module = "pypaimon_rust.datafusion")]
pub struct PySystemTableScan {
    inner: SystemTableScan,
}

#[pymethods]
impl PySystemTableScan {
    fn plan(&self, py: Python<'_>) -> PyResult<PySystemTablePlan> {
        Ok(PySystemTablePlan {
            inner: py
                .detach(|| runtime().block_on(self.inner.plan()))
                .map_err(to_py_err)?,
        })
    }
}

#[pyclass(name = "SystemTablePlan", module = "pypaimon_rust.datafusion")]
pub struct PySystemTablePlan {
    inner: SystemTablePlan,
}

#[pymethods]
impl PySystemTablePlan {
    fn splits(&self) -> Vec<PySystemSplit> {
        self.inner
            .splits()
            .iter()
            .cloned()
            .map(|inner| PySystemSplit { inner })
            .collect()
    }
}

type SystemSplitPickle = (Py<PyAny>, (String, Py<PyAny>));

#[pyclass(name = "SystemSplit", module = "pypaimon_rust.datafusion")]
pub struct PySystemSplit {
    inner: SystemSplit,
}

#[pymethods]
impl PySystemSplit {
    fn row_count(&self) -> usize {
        self.inner.row_count()
    }

    #[new]
    fn new(name: &str, batch: &Bound<'_, PyAny>) -> PyResult<Self> {
        let batch = RecordBatch::from_pyarrow_bound(batch)?;
        Ok(Self {
            inner: SystemSplit::from_record_batch(name, batch).map_err(to_py_err)?,
        })
    }

    fn __reduce__<'py>(slf: &Bound<'py, Self>, py: Python<'py>) -> PyResult<SystemSplitPickle> {
        let inner = &slf.borrow().inner;
        Ok((
            slf.get_type().unbind().into_any(),
            (
                inner.name().to_string(),
                inner.record_batch().to_pyarrow(py)?.unbind(),
            ),
        ))
    }
}

#[pyclass(name = "SystemTableRead", module = "pypaimon_rust.datafusion")]
pub struct PySystemTableRead {
    inner: SystemTableRead,
}

#[pymethods]
impl PySystemTableRead {
    #[new]
    fn new(name: &str) -> PyResult<Self> {
        Ok(Self {
            inner: SystemTableRead::new(name).map_err(to_py_err)?,
        })
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

    fn read(&self, splits: &Bound<'_, PyAny>) -> PyResult<PyRecordBatchReader> {
        let splits = splits
            .try_iter()?
            .map(|item| Ok(item?.extract::<PyRef<'_, PySystemSplit>>()?.inner.clone()))
            .collect::<PyResult<Vec<_>>>()?;
        let stream = self.inner.read(&splits).map_err(to_py_err)?;
        Ok(PyRecordBatchReader::from_py_stream(
            Some(self.inner.schema()),
            Box::pin(stream.map_err(to_py_err)),
        ))
    }
}
