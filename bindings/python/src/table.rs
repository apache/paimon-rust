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
use std::sync::Arc;

use paimon::table::{SnapshotManager, TagManager};
use paimon_datafusion::runtime::runtime;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::error::to_py_err;
use crate::partition::PyPartitionStat;
use crate::read::PyReadBuilder;
use crate::schema::PyTableSchema;
use crate::snapshot::PySnapshot;
use crate::tag::PyTag;
use crate::write::PyWriteBuilder;

#[pyclass(name = "Table", module = "pypaimon_rust.datafusion")]
pub struct PyTable {
    pub(crate) inner: Arc<paimon::table::Table>,
}

impl PyTable {
    pub fn new(inner: Arc<paimon::table::Table>) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyTable {
    fn identifier(&self) -> String {
        let id = self.inner.identifier();
        format!("{}.{}", id.database(), id.object())
    }

    fn location(&self) -> String {
        self.inner.location().to_string()
    }

    fn schema(&self) -> PyTableSchema {
        PyTableSchema::new(self.inner.schema().clone())
    }

    /// Create a [`PyReadBuilder`]. With `options`, resolves scan options (incl.
    /// time travel) before building, so filters validate against the resolved
    /// schema. Empty/absent options are a zero-cost latest read.
    #[pyo3(signature = (options=None))]
    fn new_read_builder(&self, options: Option<&Bound<'_, PyDict>>) -> PyResult<PyReadBuilder> {
        match options {
            Some(dict) if !dict.is_empty() => {
                let opts = crate::read::extract_options(dict)?;
                PyReadBuilder::from_options(Arc::clone(&self.inner), opts)
            }
            _ => Ok(PyReadBuilder::new(Arc::clone(&self.inner))),
        }
    }

    /// Create a [`PyWriteBuilder`] for the batch write loop.
    fn new_write_builder(&self) -> PyWriteBuilder {
        PyWriteBuilder::new(Arc::clone(&self.inner))
    }

    // ---------------- #285: observability ----------------
    fn latest_snapshot(&self) -> PyResult<Option<PySnapshot>> {
        let sm = SnapshotManager::new(
            self.inner.file_io().clone(),
            self.inner.location().to_string(),
        );
        let snap = runtime()
            .block_on(sm.get_latest_snapshot())
            .map_err(to_py_err)?;
        Ok(snap.map(PySnapshot::new))
    }

    fn list_snapshots(&self) -> PyResult<Vec<PySnapshot>> {
        let sm = SnapshotManager::new(
            self.inner.file_io().clone(),
            self.inner.location().to_string(),
        );
        let snaps = runtime().block_on(sm.list_all()).map_err(to_py_err)?;
        Ok(snaps.into_iter().rev().map(PySnapshot::new).collect())
    }

    fn list_tags(&self) -> PyResult<Vec<PyTag>> {
        let tm = TagManager::new(
            self.inner.file_io().clone(),
            self.inner.location().to_string(),
        );
        let tags = runtime().block_on(tm.list_all()).map_err(to_py_err)?;
        Ok(tags
            .into_iter()
            .map(|(name, snap)| PyTag::new(name, snap.id()))
            .collect())
    }

    fn list_partitions(&self) -> PyResult<Vec<HashMap<String, String>>> {
        let stats = runtime()
            .block_on(self.inner.partition_stats())
            .map_err(to_py_err)?;
        Ok(stats.into_iter().map(|s| s.partition).collect())
    }

    fn partition_stats(&self) -> PyResult<Vec<PyPartitionStat>> {
        let stats = runtime()
            .block_on(self.inner.partition_stats())
            .map_err(to_py_err)?;
        Ok(stats.into_iter().map(PyPartitionStat::from).collect())
    }
}
