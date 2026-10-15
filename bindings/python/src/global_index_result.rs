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

use crate::error::to_py_err;
use crate::read::extract_splits;
use arrow::pyarrow::ToPyArrow;
use futures::TryStreamExt;
use paimon::table::GlobalIndexResult;
use paimon::table::RowRange;
use paimon_datafusion::runtime::runtime;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::collections::HashMap;

#[pyclass(name = "GlobalIndexResult", module = "pypaimon_rust.datafusion")]
pub struct PyGlobalIndexResult {
    pub(crate) inner: GlobalIndexResult,
}

impl PyGlobalIndexResult {
    pub(crate) fn new(inner: GlobalIndexResult) -> Self {
        Self { inner }
    }
}

type PySearchPosition = (Py<PyBytes>, i32, String, i64, f32);

#[pymethods]
impl PyGlobalIndexResult {
    #[staticmethod]
    fn from_splits(snapshot_id: Option<i64>, splits: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            inner: GlobalIndexResult::from_splits(snapshot_id, extract_splits(splits)?),
        })
    }

    #[staticmethod]
    fn from_row_ranges(ranges: Vec<(i64, i64)>) -> PyResult<Self> {
        let ranges = ranges
            .into_iter()
            .map(|(from, to)| {
                if from > to {
                    Err(PyValueError::new_err("row range start must be <= end"))
                } else {
                    Ok(RowRange::new(from, to))
                }
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(Self {
            inner: GlobalIndexResult::from_row_ranges(ranges),
        })
    }

    #[staticmethod]
    fn from_scored_row_ids(scores: HashMap<u64, f32>) -> Self {
        Self {
            inner: GlobalIndexResult::from_scored_row_ids(
                paimon::vector_search::ScoredRowIds::from_scored_map(scores),
            ),
        }
    }
    fn snapshot_id(&self) -> Option<i64> {
        self.inner.snapshot_id()
    }
    fn new_read_builder(&self) -> PyGlobalIndexResultReadBuilder {
        PyGlobalIndexResultReadBuilder {
            result: self.inner.clone(),
            projection: None,
        }
    }
    fn __len__(&self) -> usize {
        self.inner.len()
    }
    fn row_ids(&self) -> PyResult<HashMap<u64, f32>> {
        let hits = self.inner.row_ids().map_err(to_py_err)?;
        Ok(hits
            .row_ids
            .iter()
            .copied()
            .zip(hits.scores.iter().copied())
            .collect())
    }
    fn positions(&self, py: Python<'_>) -> PyResult<Vec<PySearchPosition>> {
        Ok(self
            .inner
            .positions()
            .map_err(to_py_err)?
            .iter()
            .map(|position| {
                (
                    PyBytes::new(py, &position.partition.to_serialized_bytes()).unbind(),
                    position.bucket,
                    position.data_file_name.clone(),
                    position.row_position,
                    position.score,
                )
            })
            .collect())
    }
    fn splits(&self, py: Python<'_>) -> PyResult<Vec<Py<PyBytes>>> {
        Ok(self
            .inner
            .serialize_primary_key_splits()
            .map_err(to_py_err)?
            .iter()
            .map(|bytes| PyBytes::new(py, bytes).unbind())
            .collect())
    }
}

#[pyclass(
    name = "GlobalIndexResultReadBuilder",
    module = "pypaimon_rust.datafusion"
)]
pub struct PyGlobalIndexResultReadBuilder {
    result: GlobalIndexResult,
    projection: Option<Vec<String>>,
}

#[pymethods]
impl PyGlobalIndexResultReadBuilder {
    fn with_projection(mut slf: PyRefMut<'_, Self>, columns: Vec<String>) -> PyRefMut<'_, Self> {
        slf.projection = Some(columns);
        slf
    }
    fn read(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        let batches = py
            .detach(|| {
                runtime().block_on(async {
                    let mut builder = self.result.new_read_builder();
                    if let Some(projection) = &self.projection {
                        let columns: Vec<&str> = projection.iter().map(String::as_str).collect();
                        builder.with_projection(&columns);
                    }
                    builder.read().await?.try_collect::<Vec<_>>().await
                })
            })
            .map_err(to_py_err)?;
        batches
            .iter()
            .map(|batch| batch.to_pyarrow(py).map(Bound::unbind))
            .collect()
    }
}
