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
use paimon::spec::{CoreOptions, DataType, Datum};
use paimon::table::{
    CommitMessage, Table, TableCommit, TableWrite, COMMIT_MESSAGE_SERIALIZER_VERSION,
};
use paimon_datafusion::runtime::runtime;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
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

type PartitionSpec = HashMap<String, Option<Datum>>;
type PythonPartitionSpec = HashMap<String, Py<PyAny>>;

/// Shared implementation state; public builders keep batch and stream contracts separate.
struct WriteContext {
    table: Arc<Table>,
    commit_user: String,
}

impl WriteContext {
    fn new(table: Arc<Table>) -> Self {
        let commit_user = table.new_write_builder().commit_user().to_string();
        Self { table, commit_user }
    }

    fn new_write(&self, overwrite: bool) -> PyResult<WriteState> {
        let builder = self
            .table
            .new_write_builder()
            .with_commit_user(self.commit_user.clone())
            .map_err(to_py_err)?;
        let builder = if overwrite {
            builder.with_overwrite()
        } else {
            builder
        };
        Ok(WriteState {
            inner: Some(builder.new_write().map_err(to_py_err)?),
            target_schema: paimon::arrow::build_target_arrow_schema(self.table.schema().fields())
                .map_err(to_py_err)?,
            table_location: self.table.location().to_string(),
            commit_user: self.commit_user.clone(),
        })
    }
}

fn boolean_option(table: &Table, key: &str, default: bool) -> PyResult<bool> {
    match table.schema().options().get(key) {
        None => Ok(default),
        Some(value) if value.eq_ignore_ascii_case("true") => Ok(true),
        Some(value) if value.eq_ignore_ascii_case("false") => Ok(false),
        Some(value) => Err(PyValueError::new_err(format!(
            "Invalid boolean option {key}: {value}"
        ))),
    }
}

/// Java partition specs may encode numeric values as strings. Other Python
/// literals use the same schema-driven conversion as the predicate API.
fn partition_value(value: &Bound<'_, PyAny>, data_type: &DataType) -> PyResult<Datum> {
    if let Ok(text) = value.cast::<PyString>() {
        let text = text.to_str()?;
        let invalid = || {
            PyValueError::new_err(format!(
                "Invalid partition value '{text}' for {data_type:?}"
            ))
        };
        match data_type {
            DataType::TinyInt(_) => return text.parse().map(Datum::TinyInt).map_err(|_| invalid()),
            DataType::SmallInt(_) => {
                return text.parse().map(Datum::SmallInt).map_err(|_| invalid())
            }
            DataType::Int(_) => return text.parse().map(Datum::Int).map_err(|_| invalid()),
            DataType::BigInt(_) => return text.parse().map(Datum::Long).map_err(|_| invalid()),
            DataType::Float(_) => return text.parse().map(Datum::Float).map_err(|_| invalid()),
            DataType::Double(_) => return text.parse().map(Datum::Double).map_err(|_| invalid()),
            _ => {}
        }
    }
    py_to_datum(value, data_type)
}

fn partition_spec(
    py: Python<'_>,
    table: &Table,
    spec: PythonPartitionSpec,
) -> PyResult<PartitionSpec> {
    let fields = table.schema().partition_fields();
    let default_name = CoreOptions::new(table.schema().options())
        .partition_default_name()
        .to_string();
    spec.into_iter()
        .map(|(key, value)| {
            let field = fields
                .iter()
                .find(|field| field.name() == key)
                .ok_or_else(|| {
                    PyValueError::new_err(format!(
                        "Partition spec key '{key}' is not a partition column"
                    ))
                })?;
            let value = value.bind(py);
            let is_default = value
                .cast::<PyString>()
                .is_ok_and(|s| s.to_str().is_ok_and(|s| s == default_name));
            let datum = if value.is_none() || is_default {
                None
            } else {
                Some(partition_value(value, field.data_type())?)
            };
            Ok((key, datum))
        })
        .collect()
}

/// Java BatchWriteBuilder: the overwrite spec configures both writer and committer.
#[pyclass(name = "BatchWriteBuilder", module = "pypaimon_rust.datafusion")]
pub struct PyBatchWriteBuilder {
    context: WriteContext,
    static_partition: Option<PartitionSpec>,
}

impl PyBatchWriteBuilder {
    pub fn new(table: Arc<Table>) -> Self {
        Self {
            context: WriteContext::new(table),
            static_partition: None,
        }
    }
}

#[pymethods]
impl PyBatchWriteBuilder {
    /// No argument enables overwrite with an empty spec; explicit None restores append.
    #[pyo3(signature = (static_partition=Some(HashMap::new())))]
    fn with_overwrite<'py>(
        mut slf: PyRefMut<'py, Self>,
        py: Python<'py>,
        static_partition: Option<PythonPartitionSpec>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.static_partition = static_partition
            .map(|spec| partition_spec(py, &slf.context.table, spec))
            .transpose()?;
        Ok(slf)
    }

    fn new_write(&self) -> PyResult<PyBatchTableWrite> {
        Ok(PyBatchTableWrite {
            state: self.context.new_write(self.static_partition.is_some())?,
            prepared: false,
        })
    }

    fn new_commit(&self) -> PyResult<PyBatchTableCommit> {
        let table = &self.context.table;
        let ignore_empty = boolean_option(table, "snapshot.ignore-empty-commit", true)?;
        let dynamic = boolean_option(table, "dynamic-partition-overwrite", true)?
            && !table.schema().partition_keys().is_empty();
        Ok(PyBatchTableCommit {
            context: CommitContext::new(table, &self.context.commit_user, ignore_empty)?,
            overwrite: self.static_partition.is_some(),
            static_partition: if dynamic {
                None
            } else {
                self.static_partition.clone()
            },
            committed: false,
        })
    }
}

/// Java StreamWriteBuilder: stable commit identity belongs on the builder.
#[pyclass(name = "StreamWriteBuilder", module = "pypaimon_rust.datafusion")]
pub struct PyStreamWriteBuilder {
    context: WriteContext,
}

impl PyStreamWriteBuilder {
    pub fn new(table: Arc<Table>) -> Self {
        Self {
            context: WriteContext::new(table),
        }
    }
}

#[pymethods]
impl PyStreamWriteBuilder {
    fn commit_user(&self) -> &str {
        &self.context.commit_user
    }

    fn with_commit_user(
        mut slf: PyRefMut<'_, Self>,
        commit_user: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.context
            .table
            .new_write_builder()
            .with_commit_user(commit_user.clone())
            .map_err(to_py_err)?;
        slf.context.commit_user = commit_user;
        Ok(slf)
    }

    fn new_write(&self) -> PyResult<PyStreamTableWrite> {
        Ok(PyStreamTableWrite {
            state: self.context.new_write(false)?,
        })
    }

    fn new_commit(&self) -> PyResult<PyStreamTableCommit> {
        Ok(PyStreamTableCommit {
            context: CommitContext::new(&self.context.table, &self.context.commit_user, false)?,
        })
    }
}

struct WriteState {
    inner: Option<TableWrite>,
    target_schema: Arc<ArrowSchema>,
    table_location: String,
    commit_user: String,
}

impl WriteState {
    fn write_arrow(&mut self, py: Python<'_>, batch: &Bound<'_, PyAny>) -> PyResult<()> {
        let batch = RecordBatch::from_pyarrow_bound(batch)?;
        validate_batch_schema(&batch.schema(), &self.target_schema)?;
        let inner = self
            .inner
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("TableWrite is closed"))?;
        py.detach(|| runtime().block_on(inner.write_arrow_batch(&batch)))
            .map_err(to_py_err)
    }

    fn prepare_commit(&mut self, py: Python<'_>) -> PyResult<Vec<PyCommitMessage>> {
        let inner = self
            .inner
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("TableWrite is closed"))?;
        let messages = py
            .detach(|| runtime().block_on(inner.prepare_commit()))
            .map_err(to_py_err)?;
        Ok(messages
            .into_iter()
            .map(|inner| PyCommitMessage {
                inner,
                origin: Some(MessageOrigin {
                    table_location: self.table_location.clone(),
                    commit_user: self.commit_user.clone(),
                }),
            })
            .collect())
    }
}

#[pyclass(
    name = "BatchTableWrite",
    module = "pypaimon_rust.datafusion",
    unsendable
)]
pub struct PyBatchTableWrite {
    state: WriteState,
    prepared: bool,
}

#[pymethods]
impl PyBatchTableWrite {
    fn close(&mut self, py: Python<'_>) {
        if let Some(mut writer) = self.state.inner.take() {
            py.detach(|| runtime().block_on(writer.close()));
        }
    }

    fn write_arrow(&mut self, py: Python<'_>, batch: &Bound<'_, PyAny>) -> PyResult<()> {
        self.state.write_arrow(py, batch)
    }

    fn prepare_commit(&mut self, py: Python<'_>) -> PyResult<Vec<PyCommitMessage>> {
        if self.prepared {
            return Err(PyRuntimeError::new_err(
                "BatchTableWrite only supports one-time committing.",
            ));
        }
        self.prepared = true;
        self.state.prepare_commit(py)
    }
}

#[pyclass(
    name = "StreamTableWrite",
    module = "pypaimon_rust.datafusion",
    unsendable
)]
pub struct PyStreamTableWrite {
    state: WriteState,
}

#[pymethods]
impl PyStreamTableWrite {
    fn close(&mut self, py: Python<'_>) {
        if let Some(mut writer) = self.state.inner.take() {
            py.detach(|| runtime().block_on(writer.close()));
        }
    }

    fn write_arrow(&mut self, py: Python<'_>, batch: &Bound<'_, PyAny>) -> PyResult<()> {
        self.state.write_arrow(py, batch)
    }

    /// Rust currently flushes synchronously and has no background compaction.
    /// The identifier accompanies the returned messages in StreamTableCommit.commit.
    fn prepare_commit(
        &mut self,
        py: Python<'_>,
        wait_compaction: bool,
        commit_identifier: i64,
    ) -> PyResult<Vec<PyCommitMessage>> {
        let _ = (wait_compaction, commit_identifier);
        self.state.prepare_commit(py)
    }
}

struct CommitContext {
    inner: TableCommit,
    table: Arc<Table>,
    commit_user: String,
}

impl CommitContext {
    fn new(table: &Arc<Table>, commit_user: &str, ignore_empty: bool) -> PyResult<Self> {
        let inner = table
            .new_write_builder()
            .with_commit_user(commit_user)
            .map_err(to_py_err)?
            .try_new_commit()
            .map_err(to_py_err)?
            .with_ignore_empty_commit(ignore_empty);
        Ok(Self {
            inner,
            table: Arc::clone(table),
            commit_user: commit_user.to_string(),
        })
    }

    fn messages(
        &self,
        messages: &Bound<'_, PyAny>,
        method: &str,
        overwrite: bool,
    ) -> PyResult<Vec<CommitMessage>> {
        collect_and_validate_messages(
            messages,
            self.table.location(),
            &self.commit_user,
            method,
            overwrite,
        )
    }

    fn abort(&self, py: Python<'_>, messages: &Bound<'_, PyAny>) -> PyResult<()> {
        let messages = self.messages(messages, "abort", false)?;
        py.detach(|| runtime().block_on(self.inner.abort(&messages)))
            .map_err(to_py_err)
    }
}

#[pyclass(name = "BatchTableCommit", module = "pypaimon_rust.datafusion")]
pub struct PyBatchTableCommit {
    context: CommitContext,
    overwrite: bool,
    static_partition: Option<PartitionSpec>,
    committed: bool,
}

impl PyBatchTableCommit {
    fn check_committed(&mut self) -> PyResult<()> {
        if self.committed {
            return Err(PyRuntimeError::new_err(
                "BatchTableCommit only supports one-time committing.",
            ));
        }
        self.committed = true;
        Ok(())
    }
}

#[pymethods]
impl PyBatchTableCommit {
    /// Rust committers have no background resources to shut down.
    fn close(&self) {}

    fn commit(&mut self, py: Python<'_>, messages: &Bound<'_, PyAny>) -> PyResult<()> {
        let messages = self.context.messages(messages, "commit", self.overwrite)?;
        self.check_committed()?;
        py.detach(|| {
            runtime().block_on(async {
                if self.overwrite {
                    self.context
                        .inner
                        .overwrite(messages, self.static_partition.clone())
                        .await
                } else {
                    self.context.inner.commit(messages).await
                }
            })
        })
        .map_err(to_py_err)
    }

    fn truncate_table(&mut self, py: Python<'_>) -> PyResult<()> {
        self.check_committed()?;
        py.detach(|| runtime().block_on(self.context.inner.truncate_table()))
            .map_err(to_py_err)
    }

    fn truncate_partitions(
        &self,
        py: Python<'_>,
        partitions: Vec<PythonPartitionSpec>,
    ) -> PyResult<()> {
        let partitions = partitions
            .into_iter()
            .map(|spec| partition_spec(py, &self.context.table, spec))
            .collect::<PyResult<Vec<_>>>()?;
        py.detach(|| runtime().block_on(self.context.inner.drop_partitions(partitions)))
            .map_err(to_py_err)
    }

    fn abort(&self, py: Python<'_>, messages: &Bound<'_, PyAny>) -> PyResult<()> {
        self.context.abort(py, messages)
    }
}

#[pyclass(name = "StreamTableCommit", module = "pypaimon_rust.datafusion")]
pub struct PyStreamTableCommit {
    context: CommitContext,
}

#[pymethods]
impl PyStreamTableCommit {
    /// Rust committers have no background resources to shut down.
    fn close(&self) {}

    /// Internal PyPaimon bridge; Python owns the batch/stream lifecycle.
    fn _overwrite(
        &self,
        py: Python<'_>,
        commit_identifier: i64,
        messages: &Bound<'_, PyAny>,
        static_partition: PythonPartitionSpec,
    ) -> PyResult<()> {
        let table = &self.context.table;
        let dynamic = boolean_option(table, "dynamic-partition-overwrite", true)?
            && !table.schema().partition_keys().is_empty();
        let partitions = if dynamic {
            None
        } else {
            Some(partition_spec(py, table, static_partition)?)
        };
        let messages = self.context.messages(messages, "overwrite", true)?;
        py.detach(|| {
            runtime().block_on(async {
                if commit_identifier == i64::MAX {
                    self.context.inner.overwrite(messages, partitions).await
                } else {
                    self.context
                        .inner
                        .overwrite_with_identifier(messages, partitions, commit_identifier)
                        .await
                }
            })
        })
        .map_err(to_py_err)
    }

    fn commit(
        &self,
        py: Python<'_>,
        commit_identifier: i64,
        messages: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let messages = self.context.messages(messages, "commit", false)?;
        py.detach(|| {
            runtime().block_on(
                self.context
                    .inner
                    .commit_with_identifier(messages, commit_identifier),
            )
        })
        .map_err(to_py_err)
    }

    /// Sort identifiers, filter already committed groups, and return the number committed.
    fn filter_and_commit(
        &self,
        py: Python<'_>,
        commit_identifiers_and_messages: &Bound<'_, PyDict>,
    ) -> PyResult<usize> {
        let commits = commit_identifiers_and_messages
            .iter()
            .map(|(id, messages)| {
                Ok((
                    id.extract::<i64>()?,
                    self.context
                        .messages(&messages, "filter_and_commit", false)?,
                ))
            })
            .collect::<PyResult<Vec<_>>>()?;
        py.detach(|| runtime().block_on(self.context.inner.filter_and_commit(commits)))
            .map_err(to_py_err)
    }

    fn abort(&self, py: Python<'_>, messages: &Bound<'_, PyAny>) -> PyResult<()> {
        self.context.abort(py, messages)
    }
}

/// Collect and validate commit messages from a Python iterable, returning the
/// inner Rust `CommitMessage` values for every operation accepting messages.
fn collect_and_validate_messages<'py>(
    messages: &Bound<'py, PyAny>,
    table_location: &str,
    commit_user: &str,
    method: &str,
    overwrite: bool,
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
        let mut inner = msg.inner.clone();
        if let Some(origin) = &msg.origin {
            if origin.table_location != table_location {
                return Err(PyValueError::new_err(format!(
                    "commit message was prepared for a different table \
                     (message table '{}', committer table '{}')",
                    origin.table_location, table_location
                )));
            }
            if origin.commit_user != commit_user {
                return Err(PyValueError::new_err(
                    "commit message has a different commit_user \
                     (writer and committer must share one commit_user)"
                        .to_string(),
                ));
            }
        } else if overwrite {
            // The Java body has no operation flag. Apply the target committer's
            // mode to imported messages without changing the Python object.
            inner.mark_fixed_bucket_overwrite();
        }
        inner_messages.push(inner);
    }
    Ok(inner_messages)
}

/// Origin information retained for messages returned directly by a local writer.
struct MessageOrigin {
    table_location: String,
    commit_user: String,
}

/// A commit message produced by `prepare_commit` or decoded from the Java v14 body.
/// Serialized messages contain no table, commit user, or operation context.
#[pyclass(name = "CommitMessage", module = "pypaimon_rust.datafusion")]
pub struct PyCommitMessage {
    inner: CommitMessage,
    origin: Option<MessageOrigin>,
}

#[pymethods]
impl PyCommitMessage {
    /// Export the Java `CommitMessageSerializer` v14 body (no version header).
    fn serialize<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = self.inner.serialize().map_err(to_py_err)?;
        Ok(PyBytes::new(py, &bytes))
    }

    /// Decode a Java body independently of a table or write builder.
    #[staticmethod]
    #[pyo3(signature = (data, *, version=COMMIT_MESSAGE_SERIALIZER_VERSION))]
    fn deserialize(data: &Bound<'_, PyBytes>, version: i32) -> PyResult<Self> {
        Ok(Self {
            inner: CommitMessage::deserialize(version, data.as_bytes()).map_err(to_py_err)?,
            origin: None,
        })
    }
}
