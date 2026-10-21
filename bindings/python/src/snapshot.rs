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

use std::collections::HashMap;

use paimon::spec::Snapshot;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[pyclass(name = "Snapshot", module = "pypaimon_rust.datafusion")]
pub struct PySnapshot {
    pub(crate) inner: Snapshot,
}

impl PySnapshot {
    pub fn new(inner: Snapshot) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PySnapshot {
    #[staticmethod]
    fn _from_json(value: &str) -> PyResult<Self> {
        serde_json::from_str(value)
            .map(Self::new)
            .map_err(|err| PyValueError::new_err(format!("Invalid snapshot metadata: {err}")))
    }

    fn _to_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.inner)
            .map_err(|err| PyValueError::new_err(format!("Cannot serialize snapshot: {err}")))
    }

    fn __reduce__(slf: &Bound<'_, Self>) -> PyResult<(Py<PyAny>, (String,))> {
        let value = slf.borrow()._to_json()?;
        let restore = slf.get_type().getattr("_from_json")?.unbind();
        Ok((restore, (value,)))
    }

    fn id(&self) -> i64 {
        self.inner.id()
    }

    fn commit_time_ms(&self) -> u64 {
        self.inner.time_millis()
    }

    fn total_record_count(&self) -> Option<i64> {
        self.inner.total_record_count()
    }

    fn delta_record_count(&self) -> Option<i64> {
        self.inner.delta_record_count()
    }

    fn schema_id(&self) -> i64 {
        self.inner.schema_id()
    }

    fn commit_user(&self) -> String {
        self.inner.commit_user().to_string()
    }

    fn commit_identifier(&self) -> i64 {
        self.inner.commit_identifier()
    }

    fn next_row_id(&self) -> Option<i64> {
        self.inner.next_row_id()
    }

    fn properties(&self) -> Option<HashMap<String, String>> {
        self.inner.properties().cloned()
    }

    fn uuid(&self) -> Option<String> {
        self.inner.uuid().map(str::to_string)
    }

    fn commit_kind(&self) -> String {
        self.inner.commit_kind().to_string()
    }
}
