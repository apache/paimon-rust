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

use paimon::api::{GetTagResponse, TableSnapshot};
use paimon::table::Instant;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::snapshot::PySnapshot;

/// Typed rollback target; serialization and rollback semantics belong to core.
#[pyclass(name = "Instant", module = "pypaimon_rust.datafusion")]
pub(crate) struct PyInstant {
    pub(crate) inner: Instant,
}

#[pymethods]
impl PyInstant {
    #[staticmethod]
    fn snapshot(snapshot_id: i64) -> Self {
        Self {
            inner: Instant::snapshot(snapshot_id),
        }
    }

    #[staticmethod]
    fn tag(tag_name: String) -> Self {
        Self {
            inner: Instant::tag(tag_name),
        }
    }
}

#[pyclass(name = "GetTagResponse", module = "pypaimon_rust.datafusion")]
pub(crate) struct PyGetTagResponse {
    pub(crate) inner: GetTagResponse,
}

#[pymethods]
impl PyGetTagResponse {
    fn tag_name(&self) -> &str {
        &self.inner.tag_name
    }
    fn snapshot(&self) -> PySnapshot {
        PySnapshot::new(self.inner.snapshot.clone())
    }
    fn tag_create_time(&self) -> Option<i64> {
        self.inner.tag_create_time
    }
    fn tag_time_retained(&self) -> Option<&str> {
        self.inner.tag_time_retained.as_deref()
    }
    fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.inner).map_err(|error| PyValueError::new_err(error.to_string()))
    }
}

#[pyclass(name = "TableSnapshot", module = "pypaimon_rust.datafusion")]
pub(crate) struct PyTableSnapshot {
    pub(crate) inner: TableSnapshot,
}

#[pymethods]
impl PyTableSnapshot {
    fn snapshot(&self) -> PySnapshot {
        PySnapshot::new(self.inner.snapshot.clone())
    }
    fn record_count(&self) -> Option<i64> {
        self.inner.record_count
    }
    fn file_size_in_bytes(&self) -> Option<i64> {
        self.inner.file_size_in_bytes
    }
    fn file_count(&self) -> Option<i64> {
        self.inner.file_count
    }
    fn last_file_creation_time(&self) -> Option<i64> {
        self.inner.last_file_creation_time
    }
    fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.inner).map_err(|error| PyValueError::new_err(error.to_string()))
    }
}
