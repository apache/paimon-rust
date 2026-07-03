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

use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::pyarrow::FromPyArrow;
use arrow::record_batch::RecordBatch;
use paimon::table::{CommitMessage, Table, TableCommit, TableWrite};
use paimon_datafusion::runtime::runtime;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;

use crate::error::to_py_err;

/// Validate an incoming batch schema against the table's target Arrow schema:
/// field count, order, and names must match, and types must match exactly. The
/// nullable flag is intentionally NOT compared, since `build_target_arrow_schema`
/// derives nullability from the Paimon field while pyarrow-constructed batches
/// infer nullable=true. No cast — callers supply correctly-typed batches.
///
/// Type matching is strict (no binary-family interchange): the lower write path
/// downcasts to the exact Arrow array for each Paimon type (e.g. a `Binary` /
/// `VarBinary` field requires `arrow_array::BinaryArray`, not `LargeBinary` /
/// `FixedSizeBinary`). Accepting a near-equivalent type here would pass
/// validation but then fail deeper with a type-mismatch (or write files whose
/// Arrow schema differs from the table), so it is rejected up front.
fn validate_batch_schema(input: &ArrowSchema, target: &ArrowSchema) -> PyResult<()> {
    let mismatch = || {
        PyValueError::new_err(format!(
            "Input schema is not consistent with the table schema. \
             input: {input:?}, table: {target:?}"
        ))
    };
    if input.fields().len() != target.fields().len() {
        return Err(mismatch());
    }
    for (i, t) in input.fields().iter().zip(target.fields().iter()) {
        if i.name() != t.name() {
            return Err(mismatch());
        }
        if i.data_type() != t.data_type() {
            return Err(mismatch());
        }
    }
    Ok(())
}

/// Builder for the batch write loop, created via [`crate::table::PyTable::new_write_builder`].
///
/// Holds the owning table plus a single fixed `commit_user`, generated once and
/// shared by both `new_write()` and `new_commit()` so that writers and the
/// committer agree on the commit user (Paimon uses it for duplicate-commit
/// detection). Creating a fresh `WriteBuilder` per call would otherwise mint a
/// new random UUID each time.
#[pyclass(name = "WriteBuilder", module = "pypaimon_rust.datafusion")]
pub struct PyWriteBuilder {
    table: Arc<Table>,
    commit_user: String,
}

impl PyWriteBuilder {
    pub fn new(table: Arc<Table>) -> Self {
        let commit_user = table.new_write_builder().commit_user().to_string();
        Self { table, commit_user }
    }
}

#[pymethods]
impl PyWriteBuilder {
    /// Create a writer for accumulating Arrow batches.
    fn new_write(&self) -> PyResult<PyTableWrite> {
        let builder = self
            .table
            .new_write_builder()
            .with_commit_user(self.commit_user.clone())
            .map_err(to_py_err)?;
        let target_schema = paimon::arrow::build_target_arrow_schema(self.table.schema().fields())
            .map_err(to_py_err)?;
        Ok(PyTableWrite {
            inner: builder.new_write().map_err(to_py_err)?,
            target_schema,
            table_location: self.table.location().to_string(),
            commit_user: self.commit_user.clone(),
        })
    }

    /// Create a committer for persisting prepared commit messages.
    fn new_commit(&self) -> PyResult<PyTableCommit> {
        let builder = self
            .table
            .new_write_builder()
            .with_commit_user(self.commit_user.clone())
            .map_err(to_py_err)?;
        Ok(PyTableCommit {
            inner: builder.new_commit(),
            table_location: self.table.location().to_string(),
            commit_user: self.commit_user.clone(),
        })
    }
}

/// A stateful writer that accumulates Arrow batches until `prepare_commit`.
///
/// Marked `unsendable`: the underlying `TableWrite` holds file writers that are
/// not `Sync`, so the object enforces single-thread access at runtime.
#[pyclass(name = "TableWrite", module = "pypaimon_rust.datafusion", unsendable)]
pub struct PyTableWrite {
    inner: TableWrite,
    /// The table's target Arrow schema, used to validate incoming batches.
    target_schema: Arc<ArrowSchema>,
    /// The owning table's location, stamped onto produced commit messages so a
    /// committer can reject messages prepared for a different table.
    table_location: String,
    /// The originating builder's `commit_user`, stamped onto produced messages so
    /// a committer can reject messages prepared by a different `WriteBuilder`
    /// (writers and committers from the same builder must share one commit_user;
    /// it drives snapshot duplicate detection and postpone-bucket file naming).
    commit_user: String,
}

#[pymethods]
impl PyTableWrite {
    /// Write a single PyArrow RecordBatch into the table's writers.
    fn write_arrow(&mut self, py: Python<'_>, batch: &Bound<'_, PyAny>) -> PyResult<()> {
        let batch = RecordBatch::from_pyarrow_bound(batch)?;
        validate_batch_schema(&batch.schema(), &self.target_schema)?;
        let rt = runtime();
        py.detach(|| rt.block_on(async { self.inner.write_arrow_batch(&batch).await }))
            .map_err(to_py_err)
    }

    /// Close writers and return the commit messages (opaque; pass to commit()).
    fn prepare_commit(&mut self, py: Python<'_>) -> PyResult<Vec<PyCommitMessage>> {
        let rt = runtime();
        let messages = py
            .detach(|| rt.block_on(async { self.inner.prepare_commit().await }))
            .map_err(to_py_err)?;
        Ok(messages
            .into_iter()
            .map(|inner| PyCommitMessage {
                inner,
                table_location: self.table_location.clone(),
                commit_user: self.commit_user.clone(),
            })
            .collect())
    }
}

/// A committer that persists prepared commit messages as a snapshot.
#[pyclass(name = "TableCommit", module = "pypaimon_rust.datafusion")]
pub struct PyTableCommit {
    inner: TableCommit,
    /// The owning table's location, used to reject commit messages that were
    /// prepared for a different table (which would otherwise persist a snapshot
    /// referencing data files written under another table).
    table_location: String,
    /// The committer's `commit_user`, used to reject messages prepared by a
    /// different `WriteBuilder` — even for the same table — since the writer and
    /// committer must share one commit_user.
    commit_user: String,
}

#[pymethods]
impl PyTableCommit {
    /// Commit the given commit messages. Empty input is a no-op success.
    fn commit(&self, py: Python<'_>, messages: &Bound<'_, PyAny>) -> PyResult<()> {
        let mut inner_messages = Vec::new();
        let iter = messages.try_iter().map_err(|_| {
            PyTypeError::new_err("commit() expects a sequence of CommitMessage objects")
        })?;
        for item in iter {
            let item = item?;
            let msg: PyRef<PyCommitMessage> = item.extract().map_err(|_| {
                PyTypeError::new_err("commit() expects a sequence of CommitMessage objects")
            })?;
            if msg.table_location != self.table_location {
                return Err(PyValueError::new_err(format!(
                    "commit message was prepared for a different table \
                     (message table '{}', committer table '{}')",
                    msg.table_location, self.table_location
                )));
            }
            if msg.commit_user != self.commit_user {
                return Err(PyValueError::new_err(
                    "commit message was prepared by a different WriteBuilder \
                     (writer and committer must come from the same \
                     table.new_write_builder() so they share one commit_user)"
                        .to_string(),
                ));
            }
            inner_messages.push(msg.inner.clone());
        }
        let rt = runtime();
        py.detach(|| rt.block_on(async { self.inner.commit(inner_messages).await }))
            .map_err(to_py_err)
    }
}

/// An opaque commit message produced by `prepare_commit`, consumed by `commit`.
/// PR1 supports same-process transfer only (no pickle/serialization).
///
/// Carries the originating table's location and builder `commit_user` so a
/// committer can reject messages prepared for a different table or by a
/// different `WriteBuilder`.
#[pyclass(name = "CommitMessage", module = "pypaimon_rust.datafusion")]
pub struct PyCommitMessage {
    pub(crate) inner: CommitMessage,
    pub(crate) table_location: String,
    pub(crate) commit_user: String,
}
