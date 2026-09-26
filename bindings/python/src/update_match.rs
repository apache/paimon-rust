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

//! Thin Python bridge for the core upsert key matcher.

use arrow::pyarrow::FromPyArrow;
use arrow::record_batch::RecordBatch;
use paimon::table::UpsertKeyMatcher;
use pyo3::prelude::*;

use crate::error::to_py_err;

#[pyclass(
    name = "UpsertKeyMatcher",
    module = "pypaimon_rust.datafusion",
    unsendable
)]
pub struct PyUpsertKeyMatcher {
    inner: UpsertKeyMatcher,
}

#[pymethods]
impl PyUpsertKeyMatcher {
    #[new]
    fn new(batch: &Bound<'_, PyAny>, keys: Vec<String>) -> PyResult<Self> {
        let batch = RecordBatch::from_pyarrow_bound(batch)?;
        Ok(Self {
            inner: UpsertKeyMatcher::new(&batch, keys).map_err(to_py_err)?,
        })
    }

    fn deduplicated_indices(&self) -> Vec<usize> {
        self.inner.deduplicated_indices()
    }

    fn add_existing_batch(&mut self, batch: &Bound<'_, PyAny>) -> PyResult<()> {
        let batch = RecordBatch::from_pyarrow_bound(batch)?;
        self.inner.add_existing_batch(&batch).map_err(to_py_err)
    }

    fn finish(&self) -> (Vec<usize>, Vec<i64>, Vec<usize>) {
        self.inner.finish()
    }
}
