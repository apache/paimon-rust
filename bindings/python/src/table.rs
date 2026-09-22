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

use paimon::catalog::Identifier;
use paimon::io::FileIO;
use paimon::spec::TableSchema;
use paimon_datafusion::runtime::runtime;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::error::to_py_err;
use crate::partition::PyPartitionStat;
use crate::read::PyReadBuilder;
use crate::schema::PyTableSchema;
use crate::snapshot::PySnapshot;
use crate::tag::PyTag;
use crate::write::{PyBatchWriteBuilder, PyStreamWriteBuilder};

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
    /// Construct a filesystem table from a Java-format TableSchema JSON document.
    ///
    /// Preserves the caller's resolved schema and complete table options without
    /// loading catalog metadata or resolving a different schema for time travel.
    /// `options` configures FileIO, not table reads. `branch` selects the metadata
    /// namespace. REST authorization and credential refresh require a catalog.
    #[staticmethod]
    #[pyo3(signature = (location, schema_json, *, database="default", table="table", branch="main", options=None))]
    fn from_resolved_schema(
        location: String,
        schema_json: &str,
        database: &str,
        table: &str,
        branch: &str,
        options: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        let schema: TableSchema = serde_json::from_str(schema_json)
            .map_err(|err| PyValueError::new_err(format!("Invalid table schema JSON: {err}")))?;
        let properties = options
            .map(crate::read::extract_options)
            .transpose()?
            .unwrap_or_default();
        let file_io = FileIO::from_path(&location)
            .and_then(|builder| builder.with_props(properties).build())
            .map_err(to_py_err)?;
        let table = paimon::table::Table::from_resolved_schema(
            file_io,
            Identifier::new(database, table),
            location,
            schema,
            branch,
        )
        .map_err(to_py_err)?;
        Ok(Self::new(Arc::new(table)))
    }

    /// Replace the complete schema while retaining FileIO, REST credentials and branch.
    /// The caller has already resolved fields and options; no schema is reloaded.
    #[pyo3(signature = (schema_json, *, branch=None))]
    fn copy_with_resolved_schema(&self, schema_json: &str, branch: Option<&str>) -> PyResult<Self> {
        let schema: TableSchema = serde_json::from_str(schema_json)
            .map_err(|err| PyValueError::new_err(format!("Invalid table schema JSON: {err}")))?;
        let table = self
            .inner
            .copy_with_resolved_schema(schema, branch.unwrap_or(self.inner.branch()))
            .map_err(to_py_err)?;
        Ok(Self::new(Arc::new(table)))
    }

    fn identifier(&self) -> String {
        let id = self.inner.identifier();
        format!("{}.{}", id.database(), id.object())
    }

    /// Branch whose schema, snapshots, and tags this table reads.
    fn branch(&self) -> &str {
        self.inner.branch()
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
    fn new_read_builder(
        &self,
        py: Python<'_>,
        options: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyReadBuilder> {
        match options {
            Some(dict) if !dict.is_empty() => {
                let opts = crate::read::extract_options(dict)?;
                let table = Arc::clone(&self.inner);
                py.detach(|| PyReadBuilder::from_options(table, opts))
            }
            _ => Ok(PyReadBuilder::new(Arc::clone(&self.inner))),
        }
    }

    fn new_batch_write_builder(&self) -> PyBatchWriteBuilder {
        PyBatchWriteBuilder::new(Arc::clone(&self.inner))
    }

    fn new_stream_write_builder(&self) -> PyStreamWriteBuilder {
        PyStreamWriteBuilder::new(Arc::clone(&self.inner))
    }

    // ---------------- #285: observability ----------------
    fn latest_snapshot(&self, py: Python<'_>) -> PyResult<Option<PySnapshot>> {
        let sm = self.inner.snapshot_manager();
        let snap = py.detach(|| {
            runtime()
                .block_on(sm.get_latest_snapshot())
                .map_err(to_py_err)
        })?;
        Ok(snap.map(PySnapshot::new))
    }

    fn list_snapshots(&self) -> PyResult<Vec<PySnapshot>> {
        let sm = self.inner.snapshot_manager();
        let snaps = runtime().block_on(sm.list_all()).map_err(to_py_err)?;
        Ok(snaps.into_iter().rev().map(PySnapshot::new).collect())
    }

    fn list_tags(&self) -> PyResult<Vec<PyTag>> {
        let tm = self.inner.tag_manager();
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
