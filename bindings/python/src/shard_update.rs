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

//! Arrow transport for core positional shard updates.

use arrow::pyarrow::FromPyArrow;
use arrow::record_batch::RecordBatch;
use paimon::table::ShardTableUpdater;
use paimon_datafusion::runtime::runtime;
use pyo3::prelude::*;

use crate::error::to_py_err;
use crate::read::PyRecordBatchReader;
use crate::write::{wrap_messages, PyCommitMessage};

#[pyclass(name = "ShardTableUpdator", module = "pypaimon_rust.datafusion")]
pub struct PyShardTableUpdator {
    inner: ShardTableUpdater,
    table_location: String,
    commit_user: String,
}

impl PyShardTableUpdator {
    pub(crate) fn new(
        inner: ShardTableUpdater,
        table_location: String,
        commit_user: String,
    ) -> Self {
        Self {
            inner,
            table_location,
            commit_user,
        }
    }
}

#[pymethods]
impl PyShardTableUpdator {
    fn arrow_reader(&mut self) -> PyResult<PyRecordBatchReader> {
        self.inner
            .arrow_reader()
            .map(PyRecordBatchReader::from_stream)
            .map_err(to_py_err)
    }

    fn update_by_arrow_batch(&mut self, py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<()> {
        let batch = RecordBatch::from_pyarrow_bound(data)?;
        py.detach(|| runtime().block_on(self.inner.update_by_arrow_batch(batch)))
            .map_err(to_py_err)
    }

    fn prepare_commit(&mut self, py: Python<'_>) -> PyResult<Vec<PyCommitMessage>> {
        let messages = py
            .detach(|| runtime().block_on(self.inner.prepare_commit()))
            .map_err(to_py_err)?;
        Ok(wrap_messages(
            messages,
            &self.table_location,
            &self.commit_user,
        ))
    }

    fn close(&mut self) {
        self.inner.close();
    }
}
