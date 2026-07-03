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

use std::sync::Arc;

use arrow::pyarrow::ToPyArrow;
use futures::TryStreamExt;
use paimon::spec::Predicate;
use paimon::table::{DataSplit, Table};
use paimon_datafusion::runtime::runtime;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

use crate::error::to_py_err;
use crate::predicate::dict_to_predicate;

/// Apply projection/limit/filter from a config snapshot onto a core ReadBuilder.
/// Shared by PyTableScan::plan and PyTableRead::read so scan and read stay consistent.
fn apply_read_config(
    builder: &mut paimon::table::ReadBuilder<'_>,
    projection: &Option<Vec<String>>,
    limit: Option<usize>,
    filter: &Option<Predicate>,
) {
    if let Some(projection) = projection {
        let cols: Vec<&str> = projection.iter().map(String::as_str).collect();
        builder.with_projection(&cols);
    }
    if let Some(limit) = limit {
        builder.with_limit(limit);
    }
    if let Some(filter) = filter {
        builder.with_filter(filter.clone());
    }
}

/// Extract a sequence of Python `Split` objects into core `DataSplit`s. Accepts
/// any iterable (list/tuple/generator). Runs under the GIL since it touches
/// Python objects. A non-iterable argument or a non-`Split` element raises
/// `TypeError`.
fn extract_splits(splits: &Bound<'_, PyAny>) -> PyResult<Vec<DataSplit>> {
    let iter = splits
        .try_iter()
        .map_err(|_| PyTypeError::new_err("read() expects a sequence of Split objects"))?;
    let mut out = Vec::new();
    for item in iter {
        let item = item?;
        let split: PyRef<PySplit> = item
            .extract()
            .map_err(|_| PyTypeError::new_err("read() expects a sequence of Split objects"))?;
        out.push(split.inner.clone());
    }
    Ok(out)
}

#[pyclass(name = "ReadBuilder", module = "pypaimon_rust.datafusion")]
pub struct PyReadBuilder {
    table: Arc<Table>,
    projection: Option<Vec<String>>,
    limit: Option<usize>,
    filter: Option<Predicate>,
}

impl PyReadBuilder {
    pub fn new(table: Arc<Table>) -> Self {
        Self {
            table,
            projection: None,
            limit: None,
            filter: None,
        }
    }
}

#[pymethods]
impl PyReadBuilder {
    fn with_projection(mut slf: PyRefMut<'_, Self>, columns: Vec<String>) -> PyRefMut<'_, Self> {
        slf.projection = Some(columns);
        slf
    }

    fn with_limit(mut slf: PyRefMut<'_, Self>, limit: usize) -> PyRefMut<'_, Self> {
        slf.limit = Some(limit);
        slf
    }

    /// Convert a lightweight dict predicate into a Rust [`Predicate`] and store
    /// it for pushdown. Conversion happens immediately, so conversion errors
    /// (unknown field, type mismatch, unsupported operator/type) surface at call
    /// time. Repeated calls overwrite the previously stored filter.
    fn with_filter<'py>(
        mut slf: PyRefMut<'py, Self>,
        predicate: &Bound<'_, PyDict>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let filter = dict_to_predicate(predicate, slf.table.schema().fields())?;
        slf.filter = Some(filter);
        Ok(slf)
    }

    fn new_scan(&self) -> PyTableScan {
        PyTableScan {
            table: Arc::clone(&self.table),
            projection: self.projection.clone(),
            limit: self.limit,
            filter: self.filter.clone(),
        }
    }

    fn new_read(&self) -> PyTableRead {
        PyTableRead {
            table: Arc::clone(&self.table),
            projection: self.projection.clone(),
            limit: self.limit,
            filter: self.filter.clone(),
        }
    }
}

#[pyclass(name = "TableScan", module = "pypaimon_rust.datafusion")]
pub struct PyTableScan {
    table: Arc<Table>,
    projection: Option<Vec<String>>,
    limit: Option<usize>,
    filter: Option<Predicate>,
}

#[pymethods]
impl PyTableScan {
    fn plan(&self, py: Python<'_>) -> PyResult<PyPlan> {
        let rt = runtime();
        let splits = py.detach(|| {
            rt.block_on(async {
                let mut builder = self.table.new_read_builder();
                apply_read_config(&mut builder, &self.projection, self.limit, &self.filter);
                let plan = builder.new_scan().plan().await.map_err(to_py_err)?;
                Ok::<_, PyErr>(plan.splits().to_vec())
            })
        })?;
        Ok(PyPlan { splits })
    }
}

#[pyclass(name = "TableRead", module = "pypaimon_rust.datafusion")]
pub struct PyTableRead {
    table: Arc<Table>,
    projection: Option<Vec<String>>,
    limit: Option<usize>,
    filter: Option<Predicate>,
}

#[pymethods]
impl PyTableRead {
    /// Read the given splits into a list of PyArrow RecordBatches.
    fn read(&self, py: Python<'_>, splits: &Bound<'_, PyAny>) -> PyResult<Vec<Py<PyAny>>> {
        let splits = extract_splits(splits)?;
        let rt = runtime();
        let batches = py.detach(|| {
            rt.block_on(async {
                let mut builder = self.table.new_read_builder();
                apply_read_config(&mut builder, &self.projection, self.limit, &self.filter);
                // Validate config (e.g. projection) before the empty-splits fast
                // path so an invalid projection fails consistently regardless of
                // how many splits are passed.
                let read = builder.new_read().map_err(to_py_err)?;
                if splits.is_empty() {
                    return Ok(Vec::new());
                }
                let stream = read.to_arrow(&splits).map_err(to_py_err)?;
                stream.try_collect::<Vec<_>>().await.map_err(to_py_err)
            })
        })?;
        batches
            .iter()
            .map(|batch| Ok(batch.to_pyarrow(py)?.unbind()))
            .collect()
    }
}

#[pyclass(name = "Plan", module = "pypaimon_rust.datafusion")]
pub struct PyPlan {
    splits: Vec<DataSplit>,
}

#[pymethods]
impl PyPlan {
    fn splits(&self) -> Vec<PySplit> {
        self.splits
            .iter()
            .cloned()
            .map(|inner| PySplit { inner })
            .collect()
    }

    fn __len__(&self) -> usize {
        self.splits.len()
    }
}

#[pyclass(name = "Split", module = "pypaimon_rust.datafusion")]
pub struct PySplit {
    pub(crate) inner: DataSplit,
}

impl PySplit {
    fn to_bytes(&self) -> PyResult<Vec<u8>> {
        serde_json::to_vec(&self.inner)
            .map_err(|e| PyValueError::new_err(format!("failed to serialize split: {e}")))
    }

    fn from_bytes(bytes: &[u8]) -> PyResult<DataSplit> {
        serde_json::from_slice(bytes)
            .map_err(|e| PyValueError::new_err(format!("failed to deserialize split: {e}")))
    }
}

#[pymethods]
impl PySplit {
    /// Physical row count: sum of data-file row counts (not a logical result count).
    fn row_count(&self) -> i64 {
        self.inner.row_count()
    }

    /// Reduce to `Split(bytes)` for pickle/copy. The bytes are an opaque,
    /// implementation-detail encoding; only same/compatible-version round-trip
    /// is guaranteed.
    fn __reduce__<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<(Py<PyAny>, (Py<PyBytes>,))> {
        let bytes = slf.borrow().to_bytes()?;
        let cls = slf.get_type().unbind().into_any();
        Ok((cls, (PyBytes::new(py, &bytes).unbind(),)))
    }

    /// Reconstruct a split from opaque bytes produced by pickling. Direct
    /// construction without those bytes is unsupported; obtain splits from
    /// `ReadBuilder.new_scan().plan()`.
    #[new]
    fn new(state: &Bound<'_, PyBytes>) -> PyResult<Self> {
        Ok(Self {
            inner: Self::from_bytes(state.as_bytes())?,
        })
    }
}
