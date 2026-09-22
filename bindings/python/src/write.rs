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

use arrow::datatypes::Schema as ArrowSchema;
use arrow::pyarrow::FromPyArrow;
use arrow::record_batch::RecordBatch;
use paimon::spec::{CoreOptions, Datum};
use paimon::table::{
    CommitMessage, Table, TableCommit, TableWrite, COMMIT_MESSAGE_SERIALIZER_VERSION,
};
use paimon_datafusion::runtime::runtime;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyString};

use crate::error::to_py_err;
use crate::predicate::py_to_datum;

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
/// Holds the owning table plus a single fixed `commit_user`, chosen once and
/// shared by both `new_write()` and `new_commit()` so that writers and the
/// committer agree on the commit user (Paimon uses it for duplicate-commit
/// detection). Creating a fresh `WriteBuilder` per call would otherwise mint a
/// new random UUID each time.
#[pyclass(name = "WriteBuilder", module = "pypaimon_rust.datafusion")]
pub struct PyWriteBuilder {
    table: Arc<Table>,
    commit_user: String,
    overwrite: bool,
}

impl PyWriteBuilder {
    pub fn new(table: Arc<Table>, commit_user: Option<String>, overwrite: bool) -> PyResult<Self> {
        let mut builder = table.new_write_builder();
        if let Some(user) = commit_user {
            builder = builder.with_commit_user(user).map_err(to_py_err)?;
        }
        let commit_user = builder.commit_user().to_string();
        Ok(Self {
            table,
            commit_user,
            overwrite,
        })
    }
}

#[pymethods]
impl PyWriteBuilder {
    /// Import a Java body with trusted source table and operation context.
    #[pyo3(signature = (data, source_table_location, *, version=COMMIT_MESSAGE_SERIALIZER_VERSION, overwrite=false))]
    fn deserialize_commit_message(
        &self,
        data: &Bound<'_, PyBytes>,
        source_table_location: &str,
        version: i32,
        overwrite: bool,
    ) -> PyResult<PyCommitMessage> {
        PyCommitMessage::from_serialized(
            data.as_bytes(),
            source_table_location,
            self.table.location(),
            &self.commit_user,
            version,
            overwrite,
        )
    }

    /// Create a writer for accumulating Arrow batches.
    fn new_write(&self) -> PyResult<PyTableWrite> {
        let builder = self
            .table
            .new_write_builder()
            .with_commit_user(self.commit_user.clone())
            .map_err(to_py_err)?;
        let builder = if self.overwrite {
            builder.with_overwrite()
        } else {
            builder
        };
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
        PyTableCommit::new(Arc::clone(&self.table), self.commit_user.clone())
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
    table: Arc<Table>,
    /// The owning table's location, used to reject commit messages that were
    /// prepared for a different table (which would otherwise persist a snapshot
    /// referencing data files written under another table).
    table_location: String,
    /// The committer's `commit_user`, used to reject messages prepared by a
    /// different commit user, even when the table is the same.
    commit_user: String,
}

/// Collect and validate commit messages from a Python iterable, returning the
/// inner Rust `CommitMessage` values for every operation accepting messages.
fn collect_and_validate_messages<'py>(
    messages: &Bound<'py, PyAny>,
    table_location: &str,
    commit_user: &str,
    method: &str,
) -> PyResult<Vec<CommitMessage>> {
    let mut inner_messages = Vec::new();
    let iter = messages.try_iter().map_err(|_| {
        PyTypeError::new_err(format!(
            "{method}() expects a sequence of CommitMessage objects"
        ))
    })?;
    for item in iter {
        let item = item?;
        let msg: PyRef<'py, PyCommitMessage> = item.extract().map_err(|_| {
            PyTypeError::new_err(format!(
                "{method}() expects a sequence of CommitMessage objects"
            ))
        })?;
        if msg.table_location != table_location {
            return Err(PyValueError::new_err(format!(
                "commit message was prepared for a different table \
                 (message table '{}', committer table '{}')",
                msg.table_location, table_location
            )));
        }
        if msg.commit_user != commit_user {
            return Err(PyValueError::new_err(
                "commit message has a different commit_user \
                 (writer and committer must share one commit_user)"
                    .to_string(),
            ));
        }
        inner_messages.push(msg.inner.clone());
    }
    Ok(inner_messages)
}

impl PyTableCommit {
    pub fn new(table: Arc<Table>, commit_user: String) -> PyResult<Self> {
        let inner = table
            .new_write_builder()
            .with_commit_user(commit_user.clone())
            .map_err(to_py_err)?
            .try_new_commit()
            .map_err(to_py_err)?;
        Ok(Self {
            inner,
            table_location: table.location().to_string(),
            table,
            commit_user,
        })
    }

    fn partition_spec(&self, spec: &Bound<'_, PyDict>) -> PyResult<HashMap<String, Option<Datum>>> {
        let fields = self.table.schema().partition_fields();
        let default_name = CoreOptions::new(self.table.schema().options())
            .partition_default_name()
            .to_string();
        spec.iter()
            .map(|(key, value)| {
                let key: String = key.extract()?;
                let field = fields
                    .iter()
                    .find(|field| field.name() == key)
                    .ok_or_else(|| {
                        PyValueError::new_err(format!(
                            "Partition spec key '{key}' is not a partition column"
                        ))
                    })?;
                let is_default = value
                    .cast::<PyString>()
                    .is_ok_and(|s| s.to_str().is_ok_and(|s| s == default_name));
                let datum = if value.is_none() || is_default {
                    None
                } else {
                    Some(py_to_datum(&value, field.data_type())?)
                };
                Ok((key, datum))
            })
            .collect()
    }
}

#[pymethods]
impl PyTableCommit {
    /// Import an unframed Java body. Source table, version and overwrite mode
    /// are trusted out-of-band context; none is encoded in the message body.
    #[pyo3(signature = (data, source_table_location, *, version=COMMIT_MESSAGE_SERIALIZER_VERSION, overwrite=false))]
    fn deserialize_commit_message(
        &self,
        data: &Bound<'_, PyBytes>,
        source_table_location: &str,
        version: i32,
        overwrite: bool,
    ) -> PyResult<PyCommitMessage> {
        PyCommitMessage::from_serialized(
            data.as_bytes(),
            source_table_location,
            &self.table_location,
            &self.commit_user,
            version,
            overwrite,
        )
    }

    /// Commit messages with an optional monotonically increasing identifier.
    /// Use filter_and_commit to retry an uncertain result.
    #[pyo3(signature = (messages, commit_identifier=None))]
    fn commit(
        &self,
        py: Python<'_>,
        messages: &Bound<'_, PyAny>,
        commit_identifier: Option<i64>,
    ) -> PyResult<()> {
        let messages = collect_and_validate_messages(
            messages,
            &self.table_location,
            &self.commit_user,
            "commit",
        )?;
        py.detach(|| {
            runtime().block_on(async {
                match commit_identifier {
                    Some(id) => self.inner.commit_with_identifier(messages, id).await,
                    None => self.inner.commit(messages).await,
                }
            })
        })
        .map_err(to_py_err)
    }

    /// Skip an already committed identifier before retrying an uncertain commit.
    fn filter_and_commit(
        &self,
        py: Python<'_>,
        messages: &Bound<'_, PyAny>,
        commit_identifier: i64,
    ) -> PyResult<()> {
        let messages = collect_and_validate_messages(
            messages,
            &self.table_location,
            &self.commit_user,
            "filter_and_commit",
        )?;
        py.detach(|| {
            runtime().block_on(
                self.inner
                    .filter_and_commit_with_identifier(messages, commit_identifier),
            )
        })
        .map_err(to_py_err)
    }

    /// Overwrite touched partitions, or partitions matching a static spec.
    /// Partition values use the Python types of the table fields; None is null.
    /// An explicit identifier makes retries skip an already committed operation.
    #[pyo3(signature = (messages, static_partitions=None, *, commit_identifier=None))]
    fn overwrite(
        &self,
        py: Python<'_>,
        messages: &Bound<'_, PyAny>,
        static_partitions: Option<&Bound<'_, PyDict>>,
        commit_identifier: Option<i64>,
    ) -> PyResult<()> {
        let messages = collect_and_validate_messages(
            messages,
            &self.table_location,
            &self.commit_user,
            "overwrite",
        )?;
        let partitions = static_partitions
            .map(|spec| self.partition_spec(spec))
            .transpose()?;
        py.detach(|| {
            runtime().block_on(async {
                match commit_identifier {
                    Some(id) => {
                        self.inner
                            .overwrite_with_identifier(messages, partitions, id)
                            .await
                    }
                    None => self.inner.overwrite(messages, partitions).await,
                }
            })
        })
        .map_err(to_py_err)
    }

    /// Truncate matching partitions. Reject an empty list, as Java does.
    #[pyo3(signature = (partitions, commit_identifier=None))]
    fn truncate_partitions(
        &self,
        py: Python<'_>,
        partitions: Vec<Bound<'_, PyDict>>,
        commit_identifier: Option<i64>,
    ) -> PyResult<()> {
        let partitions = partitions
            .iter()
            .map(|spec| self.partition_spec(spec))
            .collect::<PyResult<Vec<_>>>()?;
        py.detach(|| {
            runtime().block_on(async {
                match commit_identifier {
                    Some(id) => {
                        self.inner
                            .drop_partitions_with_identifier(partitions, id)
                            .await
                    }
                    None => self.inner.drop_partitions(partitions).await,
                }
            })
        })
        .map_err(to_py_err)
    }

    /// Truncate the whole table, optionally filtering a repeated identifier.
    #[pyo3(signature = (commit_identifier=None))]
    fn truncate_table(&self, py: Python<'_>, commit_identifier: Option<i64>) -> PyResult<()> {
        py.detach(|| {
            runtime().block_on(async {
                match commit_identifier {
                    Some(id) => self.inner.truncate_table_with_identifier(id).await,
                    None => self.inner.truncate_table().await,
                }
            })
        })
        .map_err(to_py_err)
    }

    /// Abort a prepared commit by deleting newly written data, changelog and
    /// index files. Deletion is best-effort: missing files or storage errors
    /// are silently ignored so abort cleanup never masks the original write
    /// failure. After abort the data must not be committed — the files no
    /// longer exist.
    fn abort(&self, py: Python<'_>, messages: &Bound<'_, PyAny>) -> PyResult<()> {
        let inner_messages = collect_and_validate_messages(
            messages,
            &self.table_location,
            &self.commit_user,
            "abort",
        )?;
        let rt = runtime();
        py.detach(|| rt.block_on(async { self.inner.abort(&inner_messages).await }))
            .map_err(to_py_err)
    }
}

/// A commit message produced by `prepare_commit` or imported from the Java v14 wire format.
///
/// Carries the table location and builder `commit_user` used by the Rust
/// committer. Imported Java bodies have their source table checked by
/// `deserialize_commit_message` before they receive this context.
#[pyclass(name = "CommitMessage", module = "pypaimon_rust.datafusion")]
pub struct PyCommitMessage {
    pub(crate) inner: CommitMessage,
    pub(crate) table_location: String,
    pub(crate) commit_user: String,
}

impl PyCommitMessage {
    fn from_serialized(
        data: &[u8],
        source_table_location: &str,
        table_location: &str,
        commit_user: &str,
        version: i32,
        overwrite: bool,
    ) -> PyResult<Self> {
        if source_table_location != table_location {
            return Err(PyValueError::new_err(
                "commit message source table does not match the target table",
            ));
        }
        let inner = if overwrite {
            CommitMessage::deserialize_for_fixed_bucket_overwrite(version, data)
        } else {
            CommitMessage::deserialize(version, data)
        }
        .map_err(to_py_err)?;
        Ok(Self {
            inner,
            table_location: table_location.to_string(),
            commit_user: commit_user.to_string(),
        })
    }
}

#[pymethods]
impl PyCommitMessage {
    /// Export the Java `CommitMessageSerializer` v14 body (no version header).
    fn serialize<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = self.inner.serialize().map_err(to_py_err)?;
        Ok(PyBytes::new(py, &bytes))
    }
}
