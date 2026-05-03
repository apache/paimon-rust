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

use paimon::spec::Snapshot;
use pyo3::prelude::*;

#[pyclass(name = "Snapshot", module = "pypaimon_rust.datafusion")]
pub struct PySnapshot {
    inner: Snapshot,
}

impl PySnapshot {
    pub fn new(inner: Snapshot) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PySnapshot {
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

    fn commit_kind(&self) -> String {
        self.inner.commit_kind().to_string()
    }
}
