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

use paimon::table::PartitionStat;
use pyo3::prelude::*;

#[pyclass(name = "PartitionStat", module = "pypaimon_rust.datafusion")]
pub struct PyPartitionStat {
    inner: PartitionStat,
}

impl From<PartitionStat> for PyPartitionStat {
    fn from(inner: PartitionStat) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyPartitionStat {
    fn partition(&self) -> HashMap<String, String> {
        self.inner.partition.clone()
    }

    fn record_count(&self) -> i64 {
        self.inner.record_count
    }

    fn file_count(&self) -> u64 {
        self.inner.file_count
    }

    fn total_size_bytes(&self) -> u64 {
        self.inner.total_size_bytes
    }
}
