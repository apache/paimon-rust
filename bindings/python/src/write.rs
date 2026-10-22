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
use std::sync::{Arc, Mutex};

use arrow::datatypes::Schema as ArrowSchema;
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow::record_batch::RecordBatch;
use paimon::spec::{BlobConsumer, BlobDescriptor, CoreOptions, DataType, Datum, RowType};
use paimon::table::{
    CommitCallback, CommitCallbackContext, CommitMessage, PostponeBucketPlan,
    PostponeFixedBucketTableWrite, Table, TableCommit, TableUpdate, TableUpdateByRowId, TableWrite,
    COMMIT_MESSAGE_SERIALIZER_VERSION,
};
use paimon_datafusion::runtime::runtime;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyString};
use pyo3::{PyTraverseError, PyVisit};

use crate::blob_uri_reader::{callback_error, optional_uri_reader_factory, to_callback_error};
use crate::commit_callback::PyCommitCallbackContext;
use crate::error::to_py_err;
use crate::predicate::{dict_to_table_predicate, py_to_datum};

type PartitionSpec = HashMap<String, Option<Datum>>;
type PythonPartitionSpec = HashMap<String, Py<PyAny>>;

/// Shared implementation state; public builders keep batch and stream contracts separate.
struct WriteContext {
    table: Arc<Table>,
    commit_user: String,
    restore_snapshot_id: Option<i64>,
}

impl WriteContext {
    fn new(table: Arc<Table>) -> Self {
        let commit_user = table.new_write_builder().commit_user().to_string();
        Self {
            table,
            commit_user,
            restore_snapshot_id: None,
        }
    }

    fn with_restore_snapshot(&mut self, snapshot_id: i64) -> PyResult<()> {
        self.table
            .new_write_builder()
            .with_restore_snapshot(snapshot_id)
            .map_err(to_py_err)?;
        self.restore_snapshot_id = Some(snapshot_id);
        Ok(())
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
        let builder = match self.restore_snapshot_id {
            Some(id) => builder.with_restore_snapshot(id).map_err(to_py_err)?,
            None => builder,
        };
        Ok(WriteState {
            inner: Some(WriteTarget::Table(Box::new(
                builder.new_write().map_err(to_py_err)?,
            ))),
            table_location: self.table.location().to_string(),
            commit_user: self.commit_user.clone(),
            blob_consumers: Vec::new(),
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

/// Java partition specs encode schema-typed values as strings. Other Python
/// literals use the same schema-driven conversion as the predicate API.
fn partition_value(value: &Bound<'_, PyAny>, data_type: &DataType) -> PyResult<Option<Datum>> {
    if let Ok(text) = value.cast::<PyString>() {
        let text = text.to_str()?;
        return paimon::table::parse_partition_spec_value(text, data_type).map_err(to_py_err);
    }
    py_to_datum(value, data_type).map(Some)
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
                partition_value(value, field.data_type())?
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
    /// Internal PyPaimon bridge: retain the identity of the external Python writer.
    fn _with_commit_user(
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

    fn with_restore_snapshot(
        mut slf: PyRefMut<'_, Self>,
        snapshot_id: i64,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.context.with_restore_snapshot(snapshot_id)?;
        Ok(slf)
    }

    fn new_write(&self) -> PyResult<PyBatchTableWrite> {
        Ok(PyBatchTableWrite {
            state: self.context.new_write(self.static_partition.is_some())?,
            prepared: false,
        })
    }

    fn new_update(&self) -> PyResult<PyBatchTableUpdate> {
        if self.static_partition.is_some() {
            return Err(PyValueError::new_err(
                "BatchTableUpdate does not support overwrite",
            ));
        }
        Ok(PyBatchTableUpdate {
            context: UpdateContext::new(&self.context)?,
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

    fn with_restore_snapshot(
        mut slf: PyRefMut<'_, Self>,
        snapshot_id: i64,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.context.with_restore_snapshot(snapshot_id)?;
        Ok(slf)
    }

    fn new_write(&self) -> PyResult<PyStreamTableWrite> {
        Ok(PyStreamTableWrite {
            state: self.context.new_write(false)?,
        })
    }

    fn new_update(&self) -> PyResult<PyStreamTableUpdate> {
        Ok(PyStreamTableUpdate {
            context: UpdateContext::new(&self.context)?,
        })
    }

    fn new_commit(&self) -> PyResult<PyStreamTableCommit> {
        Ok(PyStreamTableCommit {
            context: CommitContext::new(&self.context.table, &self.context.commit_user, false)?,
        })
    }
}

/// Keep conversion, ownership and commit-message wrapping shared by both writers.
/// All routing and validation remain in the Rust core implementations.
enum WriteTarget {
    Table(Box<TableWrite>),
    PostponeFixed(Box<PostponeFixedBucketTableWrite>),
}

impl WriteTarget {
    fn with_blob_uri_reader_factory(
        &mut self,
        factory: Option<Arc<dyn paimon::io::UriReaderFactory>>,
    ) -> paimon::Result<()> {
        match self {
            Self::Table(writer) => {
                writer.with_blob_uri_reader_factory(factory)?;
            }
            Self::PostponeFixed(writer) => {
                writer.with_blob_uri_reader_factory(factory)?;
            }
        }
        Ok(())
    }

    fn with_blob_consumer(
        &mut self,
        consumer: Option<Arc<dyn BlobConsumer>>,
    ) -> paimon::Result<()> {
        match self {
            Self::Table(writer) => {
                writer.with_blob_consumer(consumer)?;
                Ok(())
            }
            Self::PostponeFixed(writer) => {
                writer.with_blob_consumer(consumer)?;
                Ok(())
            }
        }
    }

    fn with_write_type(&mut self, columns: Vec<String>) -> paimon::Result<()> {
        match self {
            Self::Table(writer) => {
                writer.with_write_type(columns)?;
                Ok(())
            }
            Self::PostponeFixed(_) => Err(paimon::Error::Unsupported {
                message: "with_write_type requires a Paimon append table".into(),
            }),
        }
    }

    async fn write_arrow(
        &mut self,
        batch: &RecordBatch,
        bucket: Option<i32>,
    ) -> paimon::Result<()> {
        match (self, bucket) {
            (Self::Table(writer), bucket) => {
                writer
                    .write_arrow(std::slice::from_ref(batch), bucket)
                    .await
            }
            (Self::PostponeFixed(writer), None) => writer.write_arrow_batch(batch).await,
            (Self::PostponeFixed(_), Some(_)) => Err(paimon::Error::DataInvalid {
                message: "Precomputed bucket writes require HASH_FIXED or HASH_DYNAMIC tables"
                    .into(),
                source: None,
            }),
        }
    }

    async fn prepare_commit(
        &mut self,
        commit_identifier: i64,
    ) -> paimon::Result<Vec<CommitMessage>> {
        match self {
            Self::Table(writer) => {
                paimon::table::internal::prepare_commit_with_identifier(writer, commit_identifier)
                    .await
            }
            Self::PostponeFixed(writer) => writer.prepare_commit().await,
        }
    }

    async fn close(&mut self) {
        match self {
            Self::Table(writer) => writer.close().await,
            Self::PostponeFixed(writer) => writer.close().await,
        }
    }
}

/// The public batch contract is shared with PyPaimon's specialized builder.
#[pyclass(
    name = "PostponeFixedBucketWriteBuilder",
    module = "pypaimon_rust.datafusion"
)]
pub struct PyPostponeFixedBucketWriteBuilder {
    batch: PyBatchWriteBuilder,
    plan: Option<PostponeBucketPlan>,
}

impl PyPostponeFixedBucketWriteBuilder {
    pub fn new(table: Arc<Table>) -> PyResult<Self> {
        table
            .new_postpone_fixed_bucket_write_builder()
            .map_err(to_py_err)?;
        Ok(Self {
            batch: PyBatchWriteBuilder::new(table),
            plan: None,
        })
    }
}

#[pymethods]
impl PyPostponeFixedBucketWriteBuilder {
    /// Existing partitions and their bucket counts, using writer metadata access.
    fn bucket_plan(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let table = &self.batch.context.table;
        let mut builder = table
            .new_postpone_fixed_bucket_write_builder()
            .map_err(to_py_err)?;
        if let Some(plan) = &self.plan {
            builder = builder.with_bucket_plan(plan.clone());
        }
        if self.batch.static_partition.is_some() {
            builder = builder.with_overwrite();
        }
        let plan = py
            .detach(|| runtime().block_on(builder.bucket_plan()))
            .map_err(to_py_err)?;
        plan.to_arrow(table)
            .map_err(to_py_err)?
            .to_pyarrow(py)
            .map(Bound::unbind)
    }

    fn _with_commit_user(
        mut slf: PyRefMut<'_, Self>,
        commit_user: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.batch
            .context
            .table
            .new_write_builder()
            .with_commit_user(commit_user.clone())
            .map_err(to_py_err)?;
        slf.batch.context.commit_user = commit_user;
        Ok(slf)
    }

    /// Partition fields followed by a non-null Int32 total_buckets column.
    fn with_bucket_plan<'py>(
        mut slf: PyRefMut<'py, Self>,
        plan: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let plan = RecordBatch::from_pyarrow_bound(plan)?;
        slf.plan = Some(
            PostponeBucketPlan::from_arrow(&slf.batch.context.table, &plan).map_err(to_py_err)?,
        );
        Ok(slf)
    }

    #[pyo3(signature = (static_partition=Some(HashMap::new())))]
    fn with_overwrite<'py>(
        mut slf: PyRefMut<'py, Self>,
        py: Python<'py>,
        static_partition: Option<PythonPartitionSpec>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.batch.static_partition = static_partition
            .map(|spec| partition_spec(py, &slf.batch.context.table, spec))
            .transpose()?;
        Ok(slf)
    }

    fn new_write(&self) -> PyResult<PyBatchTableWrite> {
        let context = &self.batch.context;
        let mut builder = context
            .table
            .new_postpone_fixed_bucket_write_builder()
            .map_err(to_py_err)?
            .with_commit_user(context.commit_user.clone())
            .map_err(to_py_err)?;
        if let Some(plan) = &self.plan {
            builder = builder.with_bucket_plan(plan.clone());
        }
        if self.batch.static_partition.is_some() {
            builder = builder.with_overwrite();
        }
        Ok(PyBatchTableWrite {
            state: WriteState {
                inner: Some(WriteTarget::PostponeFixed(Box::new(
                    builder.new_write().map_err(to_py_err)?,
                ))),
                table_location: context.table.location().to_string(),
                commit_user: context.commit_user.clone(),
                blob_consumers: Vec::new(),
            },
            prepared: false,
        })
    }

    fn new_commit(&self) -> PyResult<PyBatchTableCommit> {
        self.batch.new_commit()
    }
}

struct WriteState {
    inner: Option<WriteTarget>,
    table_location: String,
    commit_user: String,
    // Core keeps prior consumers in captured partition/bucket settings. Expose
    // every owned Python reference to GC, including replaced consumers.
    blob_consumers: Vec<Arc<PythonBlobConsumer>>,
}

/// Descriptor serialization and exception transport only; record boundaries,
/// callback timing and requested flushes belong to Rust core.
struct PythonBlobConsumer {
    callback: Mutex<Option<Py<PyAny>>>,
}

impl BlobConsumer for PythonBlobConsumer {
    fn accept(
        &self,
        field_name: &str,
        descriptor: Option<&BlobDescriptor>,
    ) -> paimon::Result<bool> {
        let result = tokio::task::block_in_place(|| {
            Python::attach(|py| {
                let encoded =
                    descriptor.map(|descriptor| PyBytes::new(py, &descriptor.serialize()));
                let callback = self
                    .callback
                    .lock()
                    .unwrap()
                    .as_ref()
                    .map(|callback| callback.clone_ref(py))
                    .ok_or_else(|| PyRuntimeError::new_err("Blob consumer has been cleared"))?;
                callback.bind(py).call1((field_name, encoded))?.is_truthy()
            })
        });
        result.map_err(callback_error)
    }
}

pub(crate) fn wrap_messages(
    messages: Vec<CommitMessage>,
    table_location: &str,
    commit_user: &str,
) -> Vec<PyCommitMessage> {
    messages
        .into_iter()
        .map(|inner| PyCommitMessage {
            inner,
            origin: Some(MessageOrigin {
                table_location: table_location.to_string(),
                commit_user: commit_user.to_string(),
            }),
        })
        .collect()
}

fn arrow_table_batches(table: &Bound<'_, PyAny>) -> PyResult<Vec<RecordBatch>> {
    let mut batches = table
        .call_method0("to_batches")?
        .try_iter()?
        .map(|batch| crate::arrow_input::write_batch_from_pyarrow(&batch?))
        .collect::<PyResult<Vec<_>>>()?;
    // Preserve empty input schema so core validates columns even with no rows.
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(Arc::new(
            ArrowSchema::from_pyarrow_bound(&table.getattr("schema")?)?,
        )));
    } else if batches.len() > 1 {
        // One Arrow Table has one schema, even when NULLs occur in only some
        // chunks. The write importer relaxes transport fields per batch; union
        // those flags so core can still require a uniform MERGE source schema.
        let first = batches[0].schema();
        let mut fields = first.fields().to_vec();
        for batch in batches.iter().skip(1) {
            for (field, actual) in fields.iter_mut().zip(batch.schema().fields()) {
                if actual.is_nullable() && !field.is_nullable() {
                    *field = Arc::new(field.as_ref().clone().with_nullable(true));
                }
            }
        }
        let schema = Arc::new(ArrowSchema::new_with_metadata(
            fields,
            first.metadata().clone(),
        ));
        for batch in &mut batches {
            if batch.schema() != schema {
                *batch = RecordBatch::try_new_with_options(
                    schema.clone(),
                    batch.columns().to_vec(),
                    &arrow::array::RecordBatchOptions::default()
                        .with_row_count(Some(batch.num_rows())),
                )
                .map_err(|error| PyValueError::new_err(error.to_string()))?;
            }
        }
    }
    Ok(batches)
}

/// Preserve each batch's supplied fields; core owns matching, deduplication
/// and the distinction between missing columns and explicit NULL values.
fn arrow_upsert_batches(input: &Bound<'_, PyAny>) -> PyResult<Vec<RecordBatch>> {
    if input.hasattr("to_batches")? {
        return arrow_table_batches(input);
    }
    input
        .try_iter()?
        .map(|batch| crate::arrow_input::write_batch_from_pyarrow(&batch?))
        .collect()
}

struct UpdateContext {
    inner: TableUpdate,
    table: Arc<Table>,
    table_location: String,
    commit_user: String,
}

impl UpdateContext {
    fn with_blob_uri_reader_factory(
        &mut self,
        factory: Option<Py<PyAny>>,
        py: Python<'_>,
    ) -> PyResult<()> {
        self.inner
            .with_blob_uri_reader_factory(optional_uri_reader_factory(factory, py)?);
        Ok(())
    }

    fn new(context: &WriteContext) -> PyResult<Self> {
        Ok(Self {
            inner: context
                .table
                .new_write_builder()
                .with_commit_user(context.commit_user.clone())
                .map_err(to_py_err)?
                .new_update()
                .map_err(to_py_err)?,
            table: context.table.clone(),
            table_location: context.table.location().to_string(),
            commit_user: context.commit_user.clone(),
        })
    }

    fn merge_source_schema(
        &self,
        source: &paimon::table::MergeSource,
    ) -> PyResult<Arc<ArrowSchema>> {
        let table = match source {
            paimon::table::MergeSource::Batches(batches) => return Ok(batches[0].schema()),
            paimon::table::MergeSource::Table(table) => table,
            paimon::table::MergeSource::SelfTable => &self.table,
        };
        // Compile conditions against the same metadata schema that core uses
        // when projecting a table source, including independently pinned views.
        paimon::arrow::build_target_arrow_schema(&table.schema().fields_with_read_metadata())
            .map_err(to_py_err)
    }

    fn merge_into(
        &self,
        py: Python<'_>,
        source: Option<&Bound<'_, PyAny>>,
        on: Vec<(String, String)>,
        when_matched: &Bound<'_, PyAny>,
        when_not_matched: &Bound<'_, PyAny>,
    ) -> PyResult<Vec<PyCommitMessage>> {
        let source = match source {
            Some(source) => match source.extract::<PyRef<crate::table::PyTable>>() {
                Ok(table) => paimon::table::MergeSource::Table(table.inner.clone()),
                Err(_) => paimon::table::MergeSource::Batches(arrow_table_batches(source)?),
            },
            None => paimon::table::MergeSource::SelfTable,
        };
        let source_schema = self.merge_source_schema(&source)?;
        let errors = Arc::new(Mutex::new(None));
        let (matched, not_matched) = crate::merge_into::from_python(
            py,
            &self.table,
            source_schema,
            when_matched,
            when_not_matched,
            errors.clone(),
        )?;
        let result = py
            .detach(|| runtime().block_on(self.inner.merge_into(source, on, matched, not_matched)));
        if let Some(error) = errors.lock().unwrap().take() {
            return Err(error);
        }
        Ok(wrap_messages(
            result.map_err(to_callback_error)?,
            &self.table_location,
            &self.commit_user,
        ))
    }

    fn update_by_predicate(
        &self,
        py: Python<'_>,
        predicate: Option<&Bound<'_, PyDict>>,
        assignments: &Bound<'_, PyDict>,
        read_columns: Option<Vec<String>>,
    ) -> PyResult<Vec<PyCommitMessage>> {
        let predicate = predicate
            .map(|predicate| dict_to_table_predicate(predicate, self.table.schema(), true))
            .transpose()?;
        let callback_error = Arc::new(Mutex::new(None));
        let assignments = crate::update_assignment::from_python(
            py,
            assignments,
            self.table.schema().fields(),
            callback_error.clone(),
        )?;
        let result = py.detach(|| {
            runtime().block_on(self.inner.update_by_predicate(
                predicate,
                assignments,
                read_columns.unwrap_or_default(),
            ))
        });
        if let Some(error) = callback_error.lock().unwrap().take() {
            return Err(error);
        }
        Ok(wrap_messages(
            result.map_err(to_callback_error)?,
            &self.table_location,
            &self.commit_user,
        ))
    }

    fn update_by_arrow_with_row_id(
        &self,
        py: Python<'_>,
        table: &Bound<'_, PyAny>,
    ) -> PyResult<Vec<PyCommitMessage>> {
        let batches = arrow_table_batches(table)?;
        let messages = py
            .detach(|| runtime().block_on(self.inner.update_by_arrow_with_row_id(batches)))
            .map_err(to_callback_error)?;
        Ok(wrap_messages(
            messages,
            &self.table_location,
            &self.commit_user,
        ))
    }

    fn update_by_arrow_batches_with_row_id(
        &self,
        py: Python<'_>,
        tables: &Bound<'_, PyAny>,
    ) -> PyResult<Vec<PyCommitMessage>> {
        let input_error = Mutex::new(None);
        let iterator = tables.try_iter()?.unbind();
        let groups = std::iter::from_fn(|| {
            // The generator may itself invoke a native operation. Leave the
            // async execution context while calling Python so nested block_on
            // calls are supported. The outer operation releases the GIL for I/O.
            tokio::task::block_in_place(|| {
                Python::attach(|py| {
                    iterator.bind(py).clone().next().map(|table| {
                        table
                            .and_then(|table| arrow_table_batches(&table))
                            .map_err(|error| {
                                *input_error.lock().unwrap() = Some(error);
                                paimon::Error::DataInvalid {
                                    message: "Failed to read an input Arrow table".into(),
                                    source: None,
                                }
                            })
                    })
                })
            })
        });
        let result = py
            .detach(|| runtime().block_on(self.inner.update_by_arrow_batches_with_row_id(groups)));
        if let Some(error) = input_error.into_inner().unwrap() {
            return Err(error);
        }
        Ok(wrap_messages(
            result.map_err(to_callback_error)?,
            &self.table_location,
            &self.commit_user,
        ))
    }

    fn new_shard_updator(
        &self,
        py: Python<'_>,
        shard: usize,
        count: usize,
        read_type: Option<&str>,
    ) -> PyResult<crate::shard_update::PyShardTableUpdator> {
        let read_type = read_type
            .map(|json| {
                serde_json::from_str::<RowType>(json)
                    .map(|row| row.fields().to_vec())
                    .map_err(|e| PyValueError::new_err(format!("invalid Paimon read type: {e}")))
            })
            .transpose()?;
        let inner = py
            .detach(|| runtime().block_on(self.inner.new_shard_updater(shard, count, read_type)))
            .map_err(to_py_err)?;
        Ok(crate::shard_update::PyShardTableUpdator::new(
            inner,
            self.table_location.clone(),
            self.commit_user.clone(),
        ))
    }

    fn new_update_by_row_id(&self, py: Python<'_>) -> PyResult<PyTableUpdateByRowId> {
        let inner = py
            .detach(|| runtime().block_on(self.inner.new_update_by_row_id()))
            .map_err(to_py_err)?;
        Ok(PyTableUpdateByRowId {
            inner,
            table_location: self.table_location.clone(),
            commit_user: self.commit_user.clone(),
        })
    }

    fn upsert_by_arrow_with_key(
        &self,
        py: Python<'_>,
        input: &Bound<'_, PyAny>,
        keys: Vec<String>,
    ) -> PyResult<Vec<PyCommitMessage>> {
        let batches = arrow_upsert_batches(input)?;
        let messages = py
            .detach(|| runtime().block_on(self.inner.upsert_by_arrow_with_key(batches, keys)))
            .map_err(to_callback_error)?;
        Ok(wrap_messages(
            messages,
            &self.table_location,
            &self.commit_user,
        ))
    }

    fn delete_by_predicate(
        &self,
        py: Python<'_>,
        predicate: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Vec<PyCommitMessage>> {
        let predicate = predicate
            .map(|predicate| dict_to_table_predicate(predicate, self.table.schema(), true))
            .transpose()?;
        let messages = py
            .detach(|| runtime().block_on(self.inner.delete_by_predicate(predicate)))
            .map_err(to_py_err)?;
        Ok(wrap_messages(
            messages,
            &self.table_location,
            &self.commit_user,
        ))
    }

    fn delete_by_row_id(
        &self,
        py: Python<'_>,
        row_ids: Vec<i64>,
    ) -> PyResult<Vec<PyCommitMessage>> {
        let messages = py
            .detach(|| runtime().block_on(self.inner.delete_by_row_id(row_ids)))
            .map_err(to_py_err)?;
        Ok(wrap_messages(
            messages,
            &self.table_location,
            &self.commit_user,
        ))
    }
}

impl WriteState {
    fn with_blob_uri_reader_factory(
        &mut self,
        factory: Option<Py<PyAny>>,
        py: Python<'_>,
    ) -> PyResult<()> {
        let factory = optional_uri_reader_factory(factory, py)?;
        self.inner
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("TableWrite is closed"))?
            .with_blob_uri_reader_factory(factory)
            .map_err(to_py_err)
    }

    fn with_blob_consumer(&mut self, callback: Option<Py<PyAny>>, py: Python<'_>) -> PyResult<()> {
        let consumer = callback
            .map(|callback| {
                if !callback.bind(py).is_callable() {
                    return Err(PyTypeError::new_err("blob_consumer must be callable"));
                }
                Ok(Arc::new(PythonBlobConsumer {
                    callback: Mutex::new(Some(callback)),
                }))
            })
            .transpose()?;
        self.inner
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("TableWrite is closed"))?
            .with_blob_consumer(
                consumer
                    .clone()
                    .map(|consumer| consumer as Arc<dyn BlobConsumer>),
            )
            .map_err(to_py_err)?;
        // Release replaced callbacks which no bucket has captured. A snapshot
        // retained by core contributes another Arc, without another Py ref.
        self.prune_blob_consumers();
        if let Some(consumer) = consumer {
            self.blob_consumers.push(consumer);
        }
        Ok(())
    }

    fn prune_blob_consumers(&mut self) {
        let (retained, retired): (Vec<_>, Vec<_>) = std::mem::take(&mut self.blob_consumers)
            .into_iter()
            .partition(|consumer| Arc::strong_count(consumer) > 1);
        self.blob_consumers = retained;
        // Python destructors can re-enter Native. No callback mutex is held,
        // and the retained references are restored before any destructor runs.
        drop(retired);
    }

    fn traverse_callbacks(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        for consumer in &self.blob_consumers {
            visit.call(&*consumer.callback.lock().unwrap())?;
        }
        Ok(())
    }

    fn clear_callbacks(&self) {
        let references = self
            .blob_consumers
            .iter()
            .map(|consumer| consumer.callback.lock().unwrap().take())
            .collect::<Vec<_>>();
        // Destructors may re-enter Native. Drop Python references outside locks.
        drop(references);
    }

    fn write_result(&self, result: paimon::Result<()>) -> PyResult<()> {
        result.map_err(to_callback_error)
    }

    fn with_write_type(&mut self, columns: Vec<String>) -> PyResult<()> {
        self.inner
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("TableWrite is closed"))?
            .with_write_type(columns)
            .map_err(to_py_err)
    }

    fn write_arrow(
        &mut self,
        py: Python<'_>,
        batch: &Bound<'_, PyAny>,
        bucket: Option<i32>,
    ) -> PyResult<()> {
        let batch = crate::arrow_input::write_batch_from_pyarrow(batch)?;
        let inner = self
            .inner
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("TableWrite is closed"))?;
        let result = py.detach(|| runtime().block_on(inner.write_arrow(&batch, bucket)));
        self.write_result(result)
    }

    fn prepare_commit(
        &mut self,
        py: Python<'_>,
        commit_identifier: i64,
    ) -> PyResult<Vec<PyCommitMessage>> {
        let inner = self
            .inner
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("TableWrite is closed"))?;
        let result = py.detach(|| runtime().block_on(inner.prepare_commit(commit_identifier)));
        self.prune_blob_consumers();
        let messages = result.map_err(to_callback_error)?;
        Ok(wrap_messages(
            messages,
            &self.table_location,
            &self.commit_user,
        ))
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

#[pyclass(name = "BatchTableUpdate", module = "pypaimon_rust.datafusion")]
pub struct PyBatchTableUpdate {
    context: UpdateContext,
}

#[pyclass(name = "TableUpdateByRowId", module = "pypaimon_rust.datafusion")]
pub struct PyTableUpdateByRowId {
    inner: TableUpdateByRowId,
    table_location: String,
    commit_user: String,
}

#[pymethods]
impl PyTableUpdateByRowId {
    fn _with_blob_uri_reader_factory(
        &mut self,
        py: Python<'_>,
        factory: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        let factory = optional_uri_reader_factory(factory, py)?;
        self.inner.with_blob_uri_reader_factory(factory);
        Ok(())
    }

    fn update_columns(
        &mut self,
        py: Python<'_>,
        data: &Bound<'_, PyAny>,
        column_names: Vec<String>,
    ) -> PyResult<Vec<PyCommitMessage>> {
        let batches = arrow_table_batches(data)?;
        let messages = py
            .detach(|| runtime().block_on(self.inner.update_columns(batches, column_names)))
            .map_err(to_callback_error)?;
        Ok(wrap_messages(
            messages,
            &self.table_location,
            &self.commit_user,
        ))
    }

    #[getter]
    fn commit_messages(&self) -> Vec<PyCommitMessage> {
        wrap_messages(
            self.inner.commit_messages().to_vec(),
            &self.table_location,
            &self.commit_user,
        )
    }

    fn _abort(&mut self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| runtime().block_on(self.inner.abort()))
            .map_err(to_py_err)
    }
}

#[pyclass(name = "StreamTableUpdate", module = "pypaimon_rust.datafusion")]
pub struct PyStreamTableUpdate {
    context: UpdateContext,
}

#[pymethods]
impl PyStreamTableUpdate {
    fn _with_blob_uri_reader_factory(
        &mut self,
        py: Python<'_>,
        factory: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        self.context.with_blob_uri_reader_factory(factory, py)
    }

    #[pyo3(signature = (source, *, on, when_matched, when_not_matched, commit_identifier))]
    fn merge_into(
        &self,
        py: Python<'_>,
        source: Option<&Bound<'_, PyAny>>,
        on: Vec<(String, String)>,
        when_matched: &Bound<'_, PyAny>,
        when_not_matched: &Bound<'_, PyAny>,
        commit_identifier: i64,
    ) -> PyResult<Vec<PyCommitMessage>> {
        // The stream committer applies the identifier to the returned messages.
        let _ = commit_identifier;
        self.context
            .merge_into(py, source, on, when_matched, when_not_matched)
    }

    #[pyo3(signature = (predicate, assignments, commit_identifier, read_columns=None))]
    fn update_by_predicate(
        &self,
        py: Python<'_>,
        predicate: Option<&Bound<'_, PyDict>>,
        assignments: &Bound<'_, PyDict>,
        commit_identifier: i64,
        read_columns: Option<Vec<String>>,
    ) -> PyResult<Vec<PyCommitMessage>> {
        let _ = commit_identifier;
        self.context
            .update_by_predicate(py, predicate, assignments, read_columns)
    }

    fn update_by_arrow_with_row_id(
        &self,
        py: Python<'_>,
        table: &Bound<'_, PyAny>,
        commit_identifier: i64,
    ) -> PyResult<Vec<PyCommitMessage>> {
        let _ = commit_identifier;
        self.context.update_by_arrow_with_row_id(py, table)
    }

    fn new_update_by_row_id(
        &self,
        py: Python<'_>,
        commit_identifier: i64,
    ) -> PyResult<PyTableUpdateByRowId> {
        let _ = commit_identifier;
        self.context.new_update_by_row_id(py)
    }

    #[pyo3(signature = (shard_num, total_shard_count, *, read_type=None))]
    fn new_shard_updator(
        &self,
        py: Python<'_>,
        shard_num: usize,
        total_shard_count: usize,
        read_type: Option<&str>,
    ) -> PyResult<crate::shard_update::PyShardTableUpdator> {
        self.context
            .new_shard_updator(py, shard_num, total_shard_count, read_type)
    }

    fn with_update_type<'py>(
        mut slf: PyRefMut<'py, Self>,
        update_cols: Vec<String>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.context
            .inner
            .with_update_type(update_cols)
            .map_err(to_py_err)?;
        Ok(slf)
    }

    fn upsert_by_arrow_with_key(
        &self,
        py: Python<'_>,
        table: &Bound<'_, PyAny>,
        upsert_keys: Vec<String>,
        commit_identifier: i64,
    ) -> PyResult<Vec<PyCommitMessage>> {
        // The stream committer applies the identifier to the returned messages.
        let _ = commit_identifier;
        self.context
            .upsert_by_arrow_with_key(py, table, upsert_keys)
    }

    fn delete_by_row_id(
        &self,
        py: Python<'_>,
        row_ids: Vec<i64>,
        commit_identifier: i64,
    ) -> PyResult<Vec<PyCommitMessage>> {
        // The stream committer applies the identifier to the returned messages.
        let _ = commit_identifier;
        self.context.delete_by_row_id(py, row_ids)
    }

    #[pyo3(signature = (predicate, commit_identifier))]
    fn delete_by_predicate(
        &self,
        py: Python<'_>,
        predicate: Option<&Bound<'_, PyDict>>,
        commit_identifier: i64,
    ) -> PyResult<Vec<PyCommitMessage>> {
        let _ = commit_identifier;
        self.context.delete_by_predicate(py, predicate)
    }
}

#[pymethods]
impl PyBatchTableUpdate {
    #[pyo3(signature = (predicate))]
    fn delete_by_predicate(
        &self,
        py: Python<'_>,
        predicate: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Vec<PyCommitMessage>> {
        self.context.delete_by_predicate(py, predicate)
    }

    fn _with_blob_uri_reader_factory(
        &mut self,
        py: Python<'_>,
        factory: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        self.context.with_blob_uri_reader_factory(factory, py)
    }

    #[pyo3(signature = (source, *, on, when_matched, when_not_matched))]
    fn merge_into(
        &self,
        py: Python<'_>,
        source: Option<&Bound<'_, PyAny>>,
        on: Vec<(String, String)>,
        when_matched: &Bound<'_, PyAny>,
        when_not_matched: &Bound<'_, PyAny>,
    ) -> PyResult<Vec<PyCommitMessage>> {
        self.context
            .merge_into(py, source, on, when_matched, when_not_matched)
    }

    #[pyo3(signature = (predicate, assignments, read_columns=None))]
    fn update_by_predicate(
        &self,
        py: Python<'_>,
        predicate: Option<&Bound<'_, PyDict>>,
        assignments: &Bound<'_, PyDict>,
        read_columns: Option<Vec<String>>,
    ) -> PyResult<Vec<PyCommitMessage>> {
        self.context
            .update_by_predicate(py, predicate, assignments, read_columns)
    }

    fn update_by_arrow_with_row_id(
        &self,
        py: Python<'_>,
        table: &Bound<'_, PyAny>,
    ) -> PyResult<Vec<PyCommitMessage>> {
        self.context.update_by_arrow_with_row_id(py, table)
    }

    fn update_by_arrow_batches_with_row_id(
        &self,
        py: Python<'_>,
        tables: &Bound<'_, PyAny>,
    ) -> PyResult<Vec<PyCommitMessage>> {
        self.context.update_by_arrow_batches_with_row_id(py, tables)
    }

    fn new_update_by_row_id(&self, py: Python<'_>) -> PyResult<PyTableUpdateByRowId> {
        self.context.new_update_by_row_id(py)
    }

    #[pyo3(signature = (shard_num, total_shard_count, *, read_type=None))]
    fn new_shard_updator(
        &self,
        py: Python<'_>,
        shard_num: usize,
        total_shard_count: usize,
        read_type: Option<&str>,
    ) -> PyResult<crate::shard_update::PyShardTableUpdator> {
        self.context
            .new_shard_updator(py, shard_num, total_shard_count, read_type)
    }

    fn with_update_type<'py>(
        mut slf: PyRefMut<'py, Self>,
        update_cols: Vec<String>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.context
            .inner
            .with_update_type(update_cols)
            .map_err(to_py_err)?;
        Ok(slf)
    }

    fn upsert_by_arrow_with_key(
        &self,
        py: Python<'_>,
        table: &Bound<'_, PyAny>,
        upsert_keys: Vec<String>,
    ) -> PyResult<Vec<PyCommitMessage>> {
        self.context
            .upsert_by_arrow_with_key(py, table, upsert_keys)
    }

    fn delete_by_row_id(
        &self,
        py: Python<'_>,
        row_ids: Vec<i64>,
    ) -> PyResult<Vec<PyCommitMessage>> {
        self.context.delete_by_row_id(py, row_ids)
    }
}

#[pymethods]
impl PyBatchTableWrite {
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        self.state.traverse_callbacks(visit)
    }

    fn __clear__(&mut self) {
        self.state.clear_callbacks();
    }

    #[pyo3(signature = (factory))]
    fn with_blob_uri_reader_factory(
        mut slf: PyRefMut<'_, Self>,
        factory: Option<Py<PyAny>>,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let py = slf.py();
        slf.state.with_blob_uri_reader_factory(factory, py)?;
        Ok(slf)
    }

    #[pyo3(signature = (callback))]
    fn with_blob_consumer(
        mut slf: PyRefMut<'_, Self>,
        callback: Option<Py<PyAny>>,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let py = slf.py();
        slf.state.with_blob_consumer(callback, py)?;
        Ok(slf)
    }

    fn with_write_type(
        mut slf: PyRefMut<'_, Self>,
        columns: Vec<String>,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.state.with_write_type(columns)?;
        Ok(slf)
    }

    fn close(&mut self, py: Python<'_>) {
        if let Some(mut writer) = self.state.inner.take() {
            py.detach(|| runtime().block_on(writer.close()));
        }
        self.state.clear_callbacks();
    }

    #[pyo3(signature = (batch, bucket=None))]
    fn write_arrow(
        &mut self,
        py: Python<'_>,
        batch: &Bound<'_, PyAny>,
        bucket: Option<i32>,
    ) -> PyResult<()> {
        self.state.write_arrow(py, batch, bucket)
    }

    fn prepare_commit(&mut self, py: Python<'_>) -> PyResult<Vec<PyCommitMessage>> {
        if self.prepared {
            return Err(PyRuntimeError::new_err(
                "BatchTableWrite only supports one-time committing.",
            ));
        }
        self.prepared = true;
        self.state.prepare_commit(py, i64::MAX)
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
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        self.state.traverse_callbacks(visit)
    }

    fn __clear__(&mut self) {
        self.state.clear_callbacks();
    }

    #[pyo3(signature = (factory))]
    fn with_blob_uri_reader_factory(
        mut slf: PyRefMut<'_, Self>,
        factory: Option<Py<PyAny>>,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let py = slf.py();
        slf.state.with_blob_uri_reader_factory(factory, py)?;
        Ok(slf)
    }

    #[pyo3(signature = (callback))]
    fn with_blob_consumer(
        mut slf: PyRefMut<'_, Self>,
        callback: Option<Py<PyAny>>,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let py = slf.py();
        slf.state.with_blob_consumer(callback, py)?;
        Ok(slf)
    }

    fn with_write_type(
        mut slf: PyRefMut<'_, Self>,
        columns: Vec<String>,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.state.with_write_type(columns)?;
        Ok(slf)
    }

    fn close(&mut self, py: Python<'_>) {
        if let Some(mut writer) = self.state.inner.take() {
            py.detach(|| runtime().block_on(writer.close()));
        }
        self.state.clear_callbacks();
    }

    #[pyo3(signature = (batch, bucket=None))]
    fn write_arrow(
        &mut self,
        py: Python<'_>,
        batch: &Bound<'_, PyAny>,
        bucket: Option<i32>,
    ) -> PyResult<()> {
        self.state.write_arrow(py, batch, bucket)
    }

    /// Rust currently flushes synchronously and has no background compaction.
    /// The identifier also bounds retirement of captured bucket configuration.
    fn prepare_commit(
        &mut self,
        py: Python<'_>,
        wait_compaction: bool,
        commit_identifier: i64,
    ) -> PyResult<Vec<PyCommitMessage>> {
        let _ = wait_compaction;
        self.state.prepare_commit(py, commit_identifier)
    }
}

/// Transport only: publication and retry notification belong to Rust core.
struct PythonCommitCallback {
    // One Python reference shared by the core hook and the GC-visible wrapper.
    callback: Mutex<Option<Py<PyAny>>>,
}

#[async_trait::async_trait]
impl CommitCallback for PythonCommitCallback {
    async fn call(&self, context: &CommitCallbackContext) -> paimon::Result<()> {
        // Suspend the runtime context so Python can call Native again, for
        // example to read the snapshot which triggered this callback.
        tokio::task::block_in_place(|| {
            Python::attach(|py| {
                let callback = self
                    .callback
                    .lock()
                    .unwrap()
                    .as_ref()
                    .map(|callback| callback.clone_ref(py));
                let Some(callback) = callback else {
                    return Ok(());
                };
                callback
                    .bind(py)
                    .call1((PyCommitCallbackContext::new(context.clone()),))?;
                Ok(())
            })
        })
        .map_err(callback_error)
    }
}

struct CommitContext {
    inner: TableCommit,
    callbacks: Vec<Arc<PythonCommitCallback>>,
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
            callbacks: Vec::new(),
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

    fn add_commit_callback(&mut self, py: Python<'_>, callback: Py<PyAny>) -> PyResult<()> {
        if !callback.bind(py).is_callable() {
            return Err(PyTypeError::new_err("Commit callback must be callable"));
        }
        let callback = Arc::new(PythonCommitCallback {
            callback: Mutex::new(Some(callback)),
        });
        self.inner
            .add_commit_callback(callback.clone())
            .map_err(to_py_err)?;
        self.callbacks.push(callback);
        Ok(())
    }

    fn traverse_callbacks(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        for callback in &self.callbacks {
            visit.call(&*callback.callback.lock().unwrap())?;
        }
        Ok(())
    }

    fn clear_callbacks(&self) {
        let references = self
            .callbacks
            .iter()
            .map(|callback| callback.callback.lock().unwrap().take())
            .collect::<Vec<_>>();
        // Destructors can re-enter Native. Release every lock before dropping
        // references. PyPaimon owns close() on the original callback objects.
        drop(references);
    }

    fn committer(&self, properties: Option<HashMap<String, String>>) -> TableCommit {
        // Properties belong to this call, including retries. The reusable
        // stream context must not carry them into a later checkpoint.
        self.inner
            .clone()
            .with_snapshot_properties(properties.unwrap_or_default())
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
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        self.context.traverse_callbacks(visit)
    }

    fn __clear__(&mut self) {
        self.context.clear_callbacks();
    }

    /// Register an idempotent callback receiving the committed core context.
    fn add_commit_callback(&mut self, py: Python<'_>, callback: Py<PyAny>) -> PyResult<()> {
        self.context.add_commit_callback(py, callback)
    }

    /// Release callback references; callback resources belong to the caller.
    fn close(&self) {
        self.context.clear_callbacks();
    }

    #[pyo3(signature = (messages, snapshot_properties=None))]
    fn commit(
        &mut self,
        py: Python<'_>,
        messages: &Bound<'_, PyAny>,
        snapshot_properties: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        let messages = self.context.messages(messages, "commit", self.overwrite)?;
        self.check_committed()?;
        let committer = self.context.committer(snapshot_properties);
        py.detach(|| {
            runtime().block_on(async {
                if self.overwrite {
                    committer
                        .overwrite(messages, self.static_partition.clone())
                        .await
                } else {
                    committer.commit(messages).await
                }
            })
        })
        .map_err(to_callback_error)
    }

    fn truncate_table(&mut self, py: Python<'_>) -> PyResult<()> {
        self.check_committed()?;
        py.detach(|| runtime().block_on(self.context.inner.truncate_table()))
            .map_err(to_callback_error)
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
            .map_err(to_callback_error)
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
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        self.context.traverse_callbacks(visit)
    }

    fn __clear__(&mut self) {
        self.context.clear_callbacks();
    }

    /// Register an idempotent callback receiving the committed core context.
    fn add_commit_callback(&mut self, py: Python<'_>, callback: Py<PyAny>) -> PyResult<()> {
        self.context.add_commit_callback(py, callback)
    }

    /// Release callback references; callback resources belong to the caller.
    fn close(&self) {
        self.context.clear_callbacks();
    }

    #[pyo3(signature = (commit_identifier, messages, snapshot_properties=None))]
    fn commit(
        &self,
        py: Python<'_>,
        commit_identifier: i64,
        messages: &Bound<'_, PyAny>,
        snapshot_properties: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        let messages = self.context.messages(messages, "commit", false)?;
        let committer = self.context.committer(snapshot_properties);
        py.detach(|| {
            runtime().block_on(committer.commit_with_identifier(messages, commit_identifier))
        })
        .map_err(to_callback_error)
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
            .map_err(to_callback_error)
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

impl PyCommitMessage {
    pub(crate) fn new(inner: CommitMessage) -> Self {
        Self {
            inner,
            origin: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paimon::catalog::Identifier;
    use paimon::io::FileIOBuilder;
    use paimon::spec::{BigIntType, IntType, Schema, TableSchema, VarCharType};
    use pyo3::ffi::c_str;

    #[test]
    fn postpone_bucket_plan_binding_round_trips_arrow_and_overwrite_precedence() {
        Python::attach(|py| {
            let schema = Schema::builder()
                .column("pt", DataType::VarChar(VarCharType::string_type()))
                .column("id", DataType::Int(IntType::new()))
                .partition_keys(["pt"])
                .primary_key(["pt", "id"])
                .option("bucket", "-2")
                .option("postpone.default-bucket-num", "7")
                .build()
                .unwrap();
            let table = Arc::new(Table::new(
                FileIOBuilder::new("memory").build().unwrap(),
                Identifier::new("default", "bucket_plan_binding"),
                "memory:/bucket_plan_binding".into(),
                TableSchema::new(0, &schema),
                None,
            ));
            let builder = Py::new(
                py,
                PyPostponeFixedBucketWriteBuilder::new(table.clone()).unwrap(),
            )
            .unwrap();
            let overwrite =
                Py::new(py, PyPostponeFixedBucketWriteBuilder::new(table).unwrap()).unwrap();
            let locals = PyDict::new(py);
            locals.set_item("builder", builder).unwrap();
            locals.set_item("overwrite", overwrite).unwrap();
            py.run(
                c_str!(
                    r#"
import pyarrow as pa
empty = builder.bucket_plan()
assert isinstance(empty, pa.RecordBatch)
assert empty.schema.names == ['pt', 'total_buckets']
assert empty.num_rows == 0
assert empty.schema.field('total_buckets') == pa.field('total_buckets', pa.int32(), nullable=False)
plan = pa.RecordBatch.from_arrays([pa.array(['b', 'a']), pa.array([3, 2], type=pa.int32())],
                                 names=['pt', 'total_buckets'])
builder.with_bucket_plan(plan)
assert sorted(zip(*builder.bucket_plan().to_pydict().values())) == [('a', 2), ('b', 3)]
builder.with_overwrite()
assert builder.bucket_plan().num_rows == 2
overwrite.with_overwrite()
assert overwrite.bucket_plan().num_rows == 0
"#
                ),
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
        });
    }

    #[test]
    fn replaced_blob_consumers_are_visible_to_writer_cycle_collection() {
        Python::attach(|py| {
            for batch in [false, true] {
                let schema = Schema::builder()
                    .column("id", DataType::Int(IntType::new()))
                    .build()
                    .unwrap();
                let table = Arc::new(Table::new(
                    FileIOBuilder::new("memory").build().unwrap(),
                    Identifier::new("default", "blob_callback_gc"),
                    "memory:/blob_callback_gc".into(),
                    TableSchema::new(0, &schema),
                    None,
                ));
                let context = WriteContext::new(table);
                let writer = if batch {
                    Py::new(
                        py,
                        PyBatchTableWrite {
                            state: context.new_write(false).unwrap(),
                            prepared: false,
                        },
                    )
                    .unwrap()
                    .into_any()
                } else {
                    Py::new(
                        py,
                        PyStreamTableWrite {
                            state: context.new_write(false).unwrap(),
                        },
                    )
                    .unwrap()
                    .into_any()
                };
                let locals = PyDict::new(py);
                locals.set_item("writer", writer).unwrap();
                py.run(
                    c_str!(
                        r#"
import gc
finalized = []
class Callback:
    def __init__(self, writer, label):
        self.writer = writer
        self.label = label
    def __call__(self, field, descriptor):
        return False
    def __del__(self):
        finalized.append(self.label)
writer.with_blob_consumer(Callback(writer, 'first'))
"#
                    ),
                    Some(&locals),
                    Some(&locals),
                )
                .unwrap();
                // Capture the first callback in core without importing PyArrow
                // or building/installing the extension for a Python test.
                let input = RecordBatch::try_from_iter([(
                    "id",
                    Arc::new(arrow::array::Int32Array::from(vec![1])) as arrow::array::ArrayRef,
                )])
                .unwrap();
                let object = locals.get_item("writer").unwrap().unwrap();
                if batch {
                    let mut writer = object.extract::<PyRefMut<'_, PyBatchTableWrite>>().unwrap();
                    let inner = writer.state.inner.as_mut().unwrap();
                    py.detach(|| runtime().block_on(inner.write_arrow(&input, None)))
                        .unwrap();
                } else {
                    let mut writer = object
                        .extract::<PyRefMut<'_, PyStreamTableWrite>>()
                        .unwrap();
                    let inner = writer.state.inner.as_mut().unwrap();
                    py.detach(|| runtime().block_on(inner.write_arrow(&input, None)))
                        .unwrap();
                }
                drop(object);
                py.run(
                    c_str!(
                        r#"
writer.with_blob_consumer(Callback(writer, 'second'))
assert finalized == []
del writer
gc.collect()
assert sorted(finalized) == ['first', 'second'], finalized
"#
                    ),
                    Some(&locals),
                    Some(&locals),
                )
                .unwrap();
            }
        });
    }

    #[test]
    fn idle_bucket_prepare_releases_only_retired_python_blob_consumers() {
        Python::attach(|py| {
            let schema = Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("pt", DataType::Int(IntType::new()))
                .partition_keys(["pt"])
                .option("bucket", "-1")
                .build()
                .unwrap();
            let table = Arc::new(Table::new(
                FileIOBuilder::new("memory").build().unwrap(),
                Identifier::new("default", "retired_callbacks"),
                "memory:/retired_callbacks".into(),
                TableSchema::new(0, &schema),
                None,
            ));
            let writer = Py::new(
                py,
                PyStreamTableWrite {
                    state: WriteContext::new(table.clone()).new_write(false).unwrap(),
                },
            )
            .unwrap();
            let locals = PyDict::new(py);
            locals.set_item("writer", &writer).unwrap();
            py.run(
                c_str!(
                    r#"
import gc
import weakref
finalized = []
class Callback:
    def __init__(self, label):
        self.label = label
    def __call__(self, field, descriptor):
        return False
    def __del__(self):
        finalized.append(self.label)
def install(label):
    callback = Callback(label)
    writer.with_blob_consumer(callback)
    return weakref.ref(callback)
first = install('first')
"#
                ),
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            let write_partition = |partition| {
                let input = RecordBatch::try_from_iter([
                    (
                        "id",
                        Arc::new(arrow::array::Int32Array::from(vec![1])) as arrow::array::ArrayRef,
                    ),
                    (
                        "pt",
                        Arc::new(arrow::array::Int32Array::from(vec![partition]))
                            as arrow::array::ArrayRef,
                    ),
                ])
                .unwrap();
                let mut borrowed = writer.borrow_mut(py);
                let inner = borrowed.state.inner.as_mut().unwrap();
                py.detach(|| runtime().block_on(inner.write_arrow(&input, None)))
                    .unwrap();
            };
            write_partition(1);
            py.run(
                c_str!("second = install('second')"),
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            write_partition(2);
            py.run(
                c_str!("current = install('current')"),
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            assert_eq!(
                writer
                    .borrow_mut(py)
                    .state
                    .prepare_commit(py, 1)
                    .unwrap()
                    .len(),
                2
            );
            let prepared_files = py
                .detach(|| {
                    runtime().block_on(table.file_io().list_status_recursive(table.location()))
                })
                .unwrap();
            assert!(!prepared_files.is_empty());
            py.run(
                c_str!("gc.collect(); assert first() is not None and second() is not None and current() is not None"),
                Some(&locals), Some(&locals),
            ).unwrap();
            write_partition(2);
            assert_eq!(
                writer
                    .borrow_mut(py)
                    .state
                    .prepare_commit(py, 2)
                    .unwrap()
                    .len(),
                1
            );
            py.run(
                c_str!("gc.collect(); assert first() is None; assert second() is not None and current() is not None; assert finalized == ['first']"),
                Some(&locals), Some(&locals),
            ).unwrap();
            for file in prepared_files.iter().filter(|file| !file.is_dir) {
                assert!(py
                    .detach(|| runtime().block_on(table.file_io().exists(&file.path)))
                    .unwrap());
            }
            writer.borrow_mut(py).close(py);
            py.run(
                c_str!("gc.collect(); assert second() is None and current() is None"),
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            for file in prepared_files.iter().filter(|file| !file.is_dir) {
                assert!(py
                    .detach(|| runtime().block_on(table.file_io().exists(&file.path)))
                    .unwrap());
            }
        });
    }

    #[test]
    fn merge_table_source_conditions_resolve_read_metadata() {
        Python::attach(|py| {
            for metadata in ["_ROW_ID", "_SEQUENCE_NUMBER"] {
                let schema = Schema::builder()
                    .column("id", DataType::BigInt(BigIntType::new()))
                    .column("value", DataType::Int(IntType::new()))
                    .option("row-tracking.enabled", "true")
                    .option("data-evolution.enabled", "true")
                    .build()
                    .unwrap();
                let file_io = FileIOBuilder::new("memory").build().unwrap();
                let table = Arc::new(Table::new(
                    file_io.clone(),
                    Identifier::new("default", "metadata_merge_target"),
                    "memory:/metadata-merge-target".into(),
                    TableSchema::new(0, &schema),
                    None,
                ));
                let source = Arc::new(Table::new(
                    file_io,
                    Identifier::new("default", "metadata_merge_source"),
                    "memory:/metadata-merge-source".into(),
                    TableSchema::new(0, &schema),
                    None,
                ));
                let source = Py::new(py, crate::table::PyTable::new(source)).unwrap();
                let context = UpdateContext::new(&WriteContext::new(table)).unwrap();
                let locals = PyDict::new(py);
                locals.set_item("metadata", metadata).unwrap();
                py.run(
                    c_str!(
                        r#"
condition = dict(sql='"s.' + metadata + '" >= 0')
matched = [dict(assignments=[('value', 'source', 'value')], condition=condition, delete=False)]
not_matched = [dict(assignments=[('id', 'source', metadata), ('value', 'source', 'value')],
                    condition=condition)]
"#
                    ),
                    Some(&locals),
                    Some(&locals),
                )
                .unwrap();
                let messages = context
                    .merge_into(
                        py,
                        Some(source.bind(py).as_any()),
                        vec![("id".into(), metadata.into())],
                        &locals.get_item("matched").unwrap().unwrap(),
                        &locals.get_item("not_matched").unwrap().unwrap(),
                    )
                    .unwrap();
                // Conditions must compile even when both snapshots are empty;
                // empty input prepares no mutation files or synthetic rows.
                assert!(messages.is_empty());
            }
        });
    }

    #[test]
    fn merge_table_source_metadata_conditions_update_and_insert() {
        use arrow::array::{Array, Int32Array, Int64Array};
        use futures::TryStreamExt;

        async fn seed(table: &Table, ids: Vec<i64>, values: Vec<i32>) {
            let batch = RecordBatch::try_new(
                paimon::arrow::build_target_arrow_schema(table.schema().fields()).unwrap(),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(Int32Array::from(values)),
                ],
            )
            .unwrap();
            let builder = table.new_write_builder();
            let mut writer = builder.new_write().unwrap();
            writer.write_arrow_batch(&batch).await.unwrap();
            builder
                .new_commit()
                .commit(writer.prepare_commit().await.unwrap())
                .await
                .unwrap();
            writer.close().await;
        }

        async fn read(table: &Table, projection: &[&str]) -> Vec<RecordBatch> {
            let mut builder = table.new_read_builder();
            builder.with_projection(projection).unwrap();
            let plan = builder.new_scan().plan().await.unwrap();
            builder
                .new_read()
                .unwrap()
                .to_arrow(plan.splits())
                .unwrap()
                .try_collect()
                .await
                .unwrap()
        }

        Python::attach(|py| {
            for metadata in ["_ROW_ID", "_SEQUENCE_NUMBER"] {
                let schema = Schema::builder()
                    .column("id", DataType::BigInt(BigIntType::new()))
                    .column("value", DataType::Int(IntType::new()))
                    .option("row-tracking.enabled", "true")
                    .option("data-evolution.enabled", "true")
                    .build()
                    .unwrap();
                let file_io = FileIOBuilder::new("memory").build().unwrap();
                let target = Arc::new(Table::new(
                    file_io.clone(),
                    Identifier::new("default", "metadata_data_target"),
                    "memory:/metadata-data-target".into(),
                    TableSchema::new(0, &schema),
                    None,
                ));
                let source = Arc::new(Table::new(
                    file_io,
                    Identifier::new("default", "metadata_data_source"),
                    "memory:/metadata-data-source".into(),
                    TableSchema::new(0, &schema),
                    None,
                ));
                let (first, second) = py.detach(|| {
                    runtime().block_on(async {
                        seed(&source, vec![9], vec![91]).await;
                        seed(&source, vec![8], vec![82]).await;
                        let mut identities = HashMap::new();
                        for batch in read(&source, &[metadata, "value"]).await {
                            let keys = batch
                                .column(0)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap();
                            let values = batch
                                .column(1)
                                .as_any()
                                .downcast_ref::<Int32Array>()
                                .unwrap();
                            assert_eq!(keys.null_count(), 0);
                            for row in 0..batch.num_rows() {
                                identities.insert(values.value(row), keys.value(row));
                            }
                        }
                        let first = identities[&91];
                        let second = identities[&82];
                        assert_ne!(first, second);
                        seed(&target, vec![first, first + 100], vec![10, 20]).await;
                        (first, second)
                    })
                });
                let source = Py::new(py, crate::table::PyTable::new(source)).unwrap();
                let context = UpdateContext::new(&WriteContext::new(target.clone())).unwrap();
                let locals = PyDict::new(py);
                locals.set_item("metadata", metadata).unwrap();
                locals.set_item("first", first).unwrap();
                locals.set_item("second", second).unwrap();
                py.run(
                    c_str!(
                        r#"
matched = [dict(assignments=[('value', 'source', 'value')],
                condition=dict(sql='"s.' + metadata + '" = ' + str(first)), delete=False)]
not_matched = [dict(assignments=[('id', 'source', metadata), ('value', 'source', 'value')],
                    condition=dict(sql='"s.' + metadata + '" = ' + str(second)))]
"#
                    ),
                    Some(&locals),
                    Some(&locals),
                )
                .unwrap();
                let messages = context
                    .merge_into(
                        py,
                        Some(source.bind(py).as_any()),
                        vec![("id".into(), metadata.into())],
                        &locals.get_item("matched").unwrap().unwrap(),
                        &locals.get_item("not_matched").unwrap().unwrap(),
                    )
                    .unwrap();
                assert!(messages
                    .iter()
                    .any(|message| message.inner.check_from_snapshot == Some(1)));
                assert!(messages.iter().any(|message| {
                    message.inner.check_from_snapshot.is_none()
                        && message
                            .inner
                            .new_files
                            .iter()
                            .all(|file| file.first_row_id.is_none())
                }));
                let rows = py.detach(|| {
                    runtime().block_on(async {
                        target
                            .new_write_builder()
                            .new_commit()
                            .commit(messages.into_iter().map(|message| message.inner).collect())
                            .await
                            .unwrap();
                        let mut rows = Vec::new();
                        for batch in read(&target, &["id", "value", "_ROW_ID"]).await {
                            let ids = batch
                                .column(0)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap();
                            let values = batch
                                .column(1)
                                .as_any()
                                .downcast_ref::<Int32Array>()
                                .unwrap();
                            let row_ids = batch
                                .column(2)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap();
                            for row in 0..batch.num_rows() {
                                rows.push((ids.value(row), values.value(row), row_ids.value(row)));
                            }
                        }
                        rows.sort_by_key(|row| row.2);
                        rows
                    })
                });
                assert_eq!(
                    rows,
                    [(first, 91, 0), (first + 100, 20, 1), (second, 82, 2)]
                );
            }
        });
    }

    #[test]
    fn update_inputs_preserve_source_nulls_until_core_selects_payloads() {
        use arrow::array::{Array, Int32Array};

        Python::attach(|py| {
            let locals = PyDict::new(py);
            py.run(
                c_str!(
                    r#"
import pyarrow as pa
schema = pa.schema([
    pa.field('id', pa.int32(), nullable=False),
    pa.field('value', pa.int32(), nullable=False),
    pa.field('unused', pa.int32(), nullable=False, metadata={'role': 'unselected'}),
], metadata={'origin': 'update'})
source = pa.Table.from_pylist([
    {'id': 0, 'value': 0, 'unused': 0},
    {'id': 1, 'value': None, 'unused': None},
    {'id': 1, 'value': 42, 'unused': None},
], schema=schema).slice(1, 2)
source.validate(full=True)
batches = source.to_batches()
"#
                ),
                None,
                Some(&locals),
            )
            .unwrap();
            let table = locals.get_item("source").unwrap().unwrap();
            let batches = locals.get_item("batches").unwrap().unwrap();
            for imported in [
                arrow_table_batches(&table).unwrap(),
                arrow_upsert_batches(&batches).unwrap(),
            ] {
                assert_eq!(imported.len(), 1);
                let batch = &imported[0];
                assert_eq!(batch.num_rows(), 2);
                assert!(!batch.schema().field(0).is_nullable());
                assert!(batch.schema().field(1).is_nullable());
                assert!(batch.schema().field(2).is_nullable());
                assert_eq!(batch.schema().metadata().get("origin").unwrap(), "update");
                assert_eq!(
                    batch.schema().field(2).metadata().get("role").unwrap(),
                    "unselected"
                );
                let values = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                assert!(values.is_null(0));
                assert_eq!(values.value(1), 42);
                assert_eq!(batch.column(2).null_count(), 2);
            }
            let original =
                ArrowSchema::from_pyarrow_bound(&table.getattr("schema").unwrap()).unwrap();
            assert!(original.fields().iter().all(|field| !field.is_nullable()));
        });
    }

    #[test]
    fn upsert_binding_validates_only_surviving_target_values() {
        Python::attach(|py| {
            let schema = Schema::builder()
                .column("id", DataType::Int(IntType::with_nullable(false)))
                .column("value", DataType::Int(IntType::with_nullable(false)))
                .column("keep", DataType::Int(IntType::with_nullable(false)))
                .option("row-tracking.enabled", "true")
                .option("data-evolution.enabled", "true")
                .build()
                .unwrap();
            let table = Arc::new(Table::new(
                FileIOBuilder::new("memory").build().unwrap(),
                Identifier::new("default", "upsert_source_nullability"),
                "memory:/upsert-source-nullability".into(),
                TableSchema::new(0, &schema),
                None,
            ));
            let context = UpdateContext::new(&WriteContext::new(table)).unwrap();
            let locals = PyDict::new(py);
            py.run(
                c_str!(
                    r#"
import pyarrow as pa
schema = pa.schema([pa.field(name, pa.int32(), nullable=False)
                    for name in ('id', 'value', 'keep')])
source = pa.Table.from_pylist([
    {'id': 1, 'value': None, 'keep': None},
    {'id': 1, 'value': 42, 'keep': 100},
], schema=schema)
invalid = source.slice(0, 1)
"#
                ),
                None,
                Some(&locals),
            )
            .unwrap();
            let source = locals.get_item("source").unwrap().unwrap();
            let messages = context
                .upsert_by_arrow_with_key(py, &source, vec!["id".into()])
                .unwrap();
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].inner.new_files.len(), 1);
            assert_eq!(messages[0].inner.new_files[0].row_count, 1);
            let invalid = locals.get_item("invalid").unwrap().unwrap();
            let error = context
                .upsert_by_arrow_with_key(py, &invalid, vec!["id".into()])
                .err()
                .expect("a surviving NULL must still fail the declared target schema")
                .to_string();
            assert!(error.contains("null"), "{error}");
        });
    }

    #[test]
    fn merge_binding_preserves_one_schema_across_source_chunks() {
        use arrow::array::Int32Array;

        Python::attach(|py| {
            for reverse in [false, true] {
                let schema = Schema::builder()
                    .column("id", DataType::Int(IntType::with_nullable(false)))
                    .column("value", DataType::Int(IntType::with_nullable(false)))
                    .column("keep", DataType::Int(IntType::with_nullable(false)))
                    .option("row-tracking.enabled", "true")
                    .option("data-evolution.enabled", "true")
                    .build()
                    .unwrap();
                let table = Arc::new(Table::new(
                    FileIOBuilder::new("memory").build().unwrap(),
                    Identifier::new("default", "merge_chunk_nullability"),
                    "memory:/merge-chunk-nullability".into(),
                    TableSchema::new(0, &schema),
                    None,
                ));
                let seed = RecordBatch::try_new(
                    paimon::arrow::build_target_arrow_schema(table.schema().fields()).unwrap(),
                    vec![
                        Arc::new(Int32Array::from(vec![1])),
                        Arc::new(Int32Array::from(vec![10])),
                        Arc::new(Int32Array::from(vec![100])),
                    ],
                )
                .unwrap();
                py.detach(|| {
                    runtime().block_on(async {
                        let builder = table.new_write_builder();
                        let mut writer = builder.new_write().unwrap();
                        writer.write_arrow_batch(&seed).await.unwrap();
                        builder
                            .new_commit()
                            .commit(writer.prepare_commit().await.unwrap())
                            .await
                            .unwrap();
                        writer.close().await;
                    })
                });
                let context = UpdateContext::new(&WriteContext::new(table)).unwrap();
                let locals = PyDict::new(py);
                locals.set_item("reverse", reverse).unwrap();
                py.run(
                    c_str!(
                        r#"
import pyarrow as pa
schema = pa.schema([
    pa.field('id', pa.int32(), nullable=False),
    pa.field('value', pa.int32(), nullable=False),
    pa.field('keep', pa.int32(), nullable=False, metadata={'role': 'unselected'}),
], metadata={'origin': 'chunked-merge'})
def chunk(row):
    return pa.RecordBatch.from_pylist([
        {'id': 0, 'value': 0, 'keep': 0}, row], schema=schema).slice(1)
chunks = [chunk({'id': 1, 'value': 42, 'keep': 100}),
          chunk({'id': 2, 'value': None, 'keep': None}),
          chunk({'id': None, 'value': 30, 'keep': 100})]
source = pa.Table.from_batches(chunks[::-1] if reverse else chunks)
encoded = pa.chunked_array([pa.DictionaryArray.from_arrays(
    pa.array([0], type=pa.int8()), pa.array([None], type=pa.int32())) for _ in chunks])
source = source.append_column(pa.field('encoded_unused', encoded.type, nullable=False), encoded)
assert source.column('encoded_unused').null_count == 0
assert source.column('encoded_unused').to_pylist() == [None, None, None]
matched = [dict(assignments=[('value', 'source', 'value')], condition=None, delete=False)]
not_matched = []
"#
                    ),
                    Some(&locals),
                    Some(&locals),
                )
                .unwrap();
                let source = locals.get_item("source").unwrap().unwrap();
                let matched = locals.get_item("matched").unwrap().unwrap();
                let not_matched = locals.get_item("not_matched").unwrap().unwrap();
                let messages = context
                    .merge_into(
                        py,
                        Some(&source),
                        vec![("id".into(), "id".into())],
                        &matched,
                        &not_matched,
                    )
                    .unwrap();
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].inner.new_files.len(), 1);
                let imported = arrow_table_batches(&source).unwrap();
                assert_eq!(imported.len(), 3);
                for batch in &imported {
                    assert_eq!(batch.schema(), imported[0].schema());
                    assert!(batch.schema().fields()[..3]
                        .iter()
                        .all(|field| field.is_nullable()));
                    assert!(!batch.schema().field(3).is_nullable());
                    assert_eq!(
                        batch.schema().metadata().get("origin").unwrap(),
                        "chunked-merge"
                    );
                    assert_eq!(
                        batch.schema().field(2).metadata().get("role").unwrap(),
                        "unselected"
                    );
                }
                let original =
                    ArrowSchema::from_pyarrow_bound(&source.getattr("schema").unwrap()).unwrap();
                assert!(original.fields().iter().all(|field| !field.is_nullable()));
            }
        });
    }

    #[test]
    fn ignored_delete_null_payload_reaches_core_nullability_checks() {
        Python::attach(|py| {
            let schema = Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("op", DataType::VarChar(VarCharType::string_type()))
                .column("payload", DataType::Int(IntType::with_nullable(false)))
                .primary_key(["id"])
                .option("bucket", "1")
                .option("rowkind.field", "op")
                .option("ignore-delete", "true")
                .build()
                .unwrap();
            let table = Arc::new(Table::new(
                FileIOBuilder::new("memory").build().unwrap(),
                Identifier::new("default", "nullable_transport"),
                "memory:/nullable-transport".into(),
                TableSchema::new(0, &schema),
                None,
            ));
            let locals = PyDict::new(py);
            for large_string in [false, true] {
                locals.set_item("large_string", large_string).unwrap();
                for (kind, id, payload, error_column) in [
                    ("-D", Some(1), None, None),
                    ("-U", Some(1), None, Some("payload")),
                    ("+I", Some(1), None, Some("payload")),
                    ("-D", None, Some(42), Some("id")),
                ] {
                    locals.set_item("kind", kind).unwrap();
                    locals.set_item("row_id", id).unwrap();
                    locals.set_item("payload", payload).unwrap();
                    py.run(
                        c_str!(
                            r#"
import pyarrow as pa
schema = pa.schema([
    pa.field('id', pa.int32(), nullable=False),
    pa.field('op', pa.large_string() if large_string else pa.string()),
    pa.field('payload', pa.int32(), nullable=False),
])
batch = pa.RecordBatch.from_pylist([
    {'id': row_id, 'op': kind, 'payload': payload},
], schema=schema)
"#
                        ),
                        None,
                        Some(&locals),
                    )
                    .unwrap();
                    let batch = locals.get_item("batch").unwrap().unwrap();
                    let mut state = WriteContext::new(table.clone()).new_write(false).unwrap();
                    let result = state.write_arrow(py, &batch, None);
                    if let Some(column) = error_column {
                        let error = result.unwrap_err().to_string();
                        assert!(
                            error.contains(&format!(
                                "Cannot write null to non-null column({column})"
                            )),
                            "{error}"
                        );
                    } else {
                        result.unwrap();
                    }
                    assert!(state.prepare_commit(py, i64::MAX).unwrap().is_empty());
                }
            }
        });
    }
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
