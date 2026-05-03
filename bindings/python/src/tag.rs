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

use pyo3::prelude::*;

#[pyclass(name = "Tag", module = "pypaimon_rust.datafusion")]
pub struct PyTag {
    name: String,
    snapshot_id: i64,
}

impl PyTag {
    pub fn new(name: String, snapshot_id: i64) -> Self {
        Self { name, snapshot_id }
    }
}

#[pymethods]
impl PyTag {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }
}
