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
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

use paimon::spec::Datum;
use paimon::table::SnapshotManager;
use paimon_datafusion::runtime::runtime;

use crate::error::to_py_err;
use crate::predicate::py_to_datum;
use crate::read::PyReadBuilder;
use crate::schema::PyTableSchema;

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

    /// Create a [`PyReadBuilder`] for DataFrame-style scan planning.
    fn new_read_builder(&self) -> PyReadBuilder {
        PyReadBuilder::new(Arc::clone(&self.inner))
    }
    fn expire_snapshots(&self, py: Python<'_>, older_than_ms: i64) -> PyResult<i64> {
        let rt = runtime();
        py.detach(|| {
            rt.block_on(async {
                let snapshot_manager = SnapshotManager::new(
                    self.inner.file_io().clone(),
                    self.inner.location().to_string(),
                );
                snapshot_manager
                    .expire_snapshots_earlier_than(older_than_ms)
                    .await
                    .map_err(to_py_err)
            })
        })
    }

    fn remove_orphan_files(&self, py: Python<'_>) -> PyResult<i64> {
        let rt = runtime();
        py.detach(|| {
            rt.block_on(async {
                let snapshot_manager = SnapshotManager::new(
                    self.inner.file_io().clone(),
                    self.inner.location().to_string(),
                );
                snapshot_manager
                    .remove_orphan_files()
                    .await
                    .map_err(to_py_err)
            })
        })
    }

    fn drop_partition(&self, partition: HashMap<String, Bound<'_, PyAny>>) -> PyResult<()> {
        let partition_fields = self.inner.schema().partition_fields();
        let mut spec: HashMap<String, Option<Datum>> = HashMap::with_capacity(partition.len());
        for (k, v) in &partition {
            let datum = if v.is_none() {
                None
            } else {
                let field = partition_fields
                    .iter()
                    .find(|f| f.name() == k)
                    .ok_or_else(|| {
                        PyValueError::new_err(format!("Partition field {} not found in schema", k))
                    })?;
                Some(py_to_datum(v, field.data_type())?)
            };
            spec.insert(k.clone(), datum);
        }
        runtime().block_on(async {
            let commit = self.inner.new_write_builder().new_commit();
            commit.drop_partitions(vec![spec]).await.map_err(to_py_err)
        })
    }

    fn trigger_compaction(&self, _full_compact: bool) -> PyResult<()> {
        todo!()
    }
}
