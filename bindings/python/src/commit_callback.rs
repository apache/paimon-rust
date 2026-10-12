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

//! Typed views of core commit notifications. Wire conversion is private to the
//! PyPaimon adapter; callback users receive domain objects.

use chrono::{DateTime, Utc};
use paimon::spec::{BinaryTableStats, DataFileMeta, FileKind, ManifestEntry};
use paimon::table::{CommitCallbackContext, CommitMessage};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::snapshot::PySnapshot;
use crate::write::PyCommitMessage;

/// Own a copy of the core notification so Python can retain it after call().
#[pyclass(
    name = "CommitCallbackContext",
    module = "pypaimon_rust.datafusion",
    frozen
)]
pub struct PyCommitCallbackContext {
    inner: CommitCallbackContext,
}

impl PyCommitCallbackContext {
    pub(crate) fn new(inner: CommitCallbackContext) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyCommitCallbackContext {
    #[getter]
    fn snapshot(&self) -> PySnapshot {
        PySnapshot::new(self.inner.snapshot.clone())
    }

    #[getter]
    fn commit_entries(&self) -> Vec<PyManifestEntry> {
        self.inner
            .commit_entries
            .iter()
            .cloned()
            .map(|inner| PyManifestEntry { inner })
            .collect()
    }

    #[getter]
    fn identifier(&self) -> i64 {
        self.inner.identifier
    }

    /// Internal lossless Snapshot conversion for the PyPaimon adapter.
    fn _snapshot_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.inner.snapshot).map_err(|error| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "Cannot serialize callback snapshot: {error}"
            ))
        })
    }
}

#[pyclass(name = "ManifestEntry", module = "pypaimon_rust.datafusion", frozen)]
pub struct PyManifestEntry {
    inner: ManifestEntry,
}

#[pymethods]
impl PyManifestEntry {
    #[getter]
    fn kind(&self) -> i32 {
        match self.inner.kind() {
            FileKind::Add => 0,
            FileKind::Delete => 1,
        }
    }

    /// Core BinaryRow bytes; callers can inspect metadata without decoding them.
    #[getter]
    fn partition<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.inner.partition())
    }

    #[getter]
    fn bucket(&self) -> i32 {
        self.inner.bucket()
    }

    #[getter]
    fn total_buckets(&self) -> i32 {
        self.inner.total_buckets()
    }

    #[getter]
    fn file(&self) -> PyDataFileMeta {
        PyDataFileMeta {
            inner: self.inner.file().clone(),
        }
    }

    /// Internal v14 bridge preserves the complete file metadata for PyPaimon.
    fn _to_commit_message(&self) -> PyCommitMessage {
        PyCommitMessage::new(CommitMessage::new(
            self.inner.partition().to_vec(),
            self.inner.bucket(),
            vec![self.inner.file().clone()],
        ))
    }
}

#[pyclass(name = "DataFileMeta", module = "pypaimon_rust.datafusion", frozen)]
pub struct PyDataFileMeta {
    inner: DataFileMeta,
}

#[pymethods]
impl PyDataFileMeta {
    #[getter]
    fn file_name(&self) -> String {
        self.inner.file_name.clone()
    }

    #[getter]
    fn file_size(&self) -> i64 {
        self.inner.file_size
    }

    #[getter]
    fn row_count(&self) -> i64 {
        self.inner.row_count
    }

    #[getter]
    fn min_sequence_number(&self) -> i64 {
        self.inner.min_sequence_number
    }

    #[getter]
    fn max_sequence_number(&self) -> i64 {
        self.inner.max_sequence_number
    }

    #[getter]
    fn schema_id(&self) -> i64 {
        self.inner.schema_id
    }

    #[getter]
    fn level(&self) -> i32 {
        self.inner.level
    }

    #[getter]
    fn extra_files(&self) -> Vec<String> {
        self.inner.extra_files.clone()
    }

    #[getter]
    fn creation_time(&self) -> Option<DateTime<Utc>> {
        self.inner.creation_time
    }

    #[getter]
    fn delete_row_count(&self) -> Option<i64> {
        self.inner.delete_row_count
    }

    #[getter]
    fn file_source(&self) -> Option<i32> {
        self.inner.file_source
    }

    #[getter]
    fn value_stats_cols(&self) -> Option<Vec<String>> {
        self.inner.value_stats_cols.clone()
    }

    #[getter]
    fn external_path(&self) -> Option<String> {
        self.inner.external_path.clone()
    }

    #[getter]
    fn first_row_id(&self) -> Option<i64> {
        self.inner.first_row_id
    }

    #[getter]
    fn write_cols(&self) -> Option<Vec<String>> {
        self.inner.write_cols.clone()
    }

    #[getter]
    fn column_max_sequence_numbers(&self) -> Option<Vec<i64>> {
        self.inner.column_max_sequence_numbers.clone()
    }

    #[getter]
    fn min_key<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.inner.min_key)
    }

    #[getter]
    fn max_key<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.inner.max_key)
    }

    #[getter]
    fn embedded_index<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        self.inner
            .embedded_index
            .as_ref()
            .map(|bytes| PyBytes::new(py, bytes))
    }

    #[getter]
    fn key_stats(&self) -> PyBinaryTableStats {
        PyBinaryTableStats {
            inner: self.inner.key_stats.clone(),
        }
    }

    #[getter]
    fn value_stats(&self) -> PyBinaryTableStats {
        PyBinaryTableStats {
            inner: self.inner.value_stats.clone(),
        }
    }
}

#[pyclass(name = "BinaryTableStats", module = "pypaimon_rust.datafusion", frozen)]
pub struct PyBinaryTableStats {
    inner: BinaryTableStats,
}

#[pymethods]
impl PyBinaryTableStats {
    #[getter]
    fn min_values<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.inner.min_values())
    }

    #[getter]
    fn max_values<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.inner.max_values())
    }

    #[getter]
    fn null_counts(&self) -> Vec<Option<i64>> {
        self.inner.null_counts().clone()
    }
}
