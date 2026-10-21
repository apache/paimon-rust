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

use paimon::spec::Predicate;
use paimon::table::{BatchVectorRead, Table, VectorRead, VectorScan, VectorScanPlan};
use paimon_datafusion::runtime::runtime;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

use crate::error::to_py_err;
use crate::global_index_result::PyGlobalIndexResult;
use crate::predicate::dict_to_table_predicate;
use crate::search_plan::{vector_splits_from_python, vector_splits_to_python};
use crate::snapshot::PySnapshot;

#[derive(Clone, Default)]
struct SearchConfig {
    column: Option<String>,
    limit: Option<usize>,
    options: Vec<HashMap<String, String>>,
    filters: Vec<Predicate>,
    partition_filters: Vec<Predicate>,
}

impl SearchConfig {
    fn single<'a>(
        &self,
        table: &'a Table,
    ) -> paimon::Result<paimon::table::VectorSearchBuilder<'a>> {
        let mut builder = table.new_vector_search_builder();
        if let Some(column) = &self.column {
            builder.with_vector_column(column);
        }
        if let Some(limit) = self.limit {
            builder.with_limit(limit);
        }
        for options in &self.options {
            builder.with_options(options.clone());
        }
        for filter in &self.filters {
            builder.with_filter(filter.clone());
        }
        for filter in &self.partition_filters {
            builder.with_partition_filter(filter.clone())?;
        }
        Ok(builder)
    }

    fn batch<'a>(
        &self,
        table: &'a Table,
    ) -> paimon::Result<paimon::table::BatchVectorSearchBuilder<'a>> {
        let mut builder = table.new_batch_vector_search_builder();
        if let Some(column) = &self.column {
            builder.with_vector_column(column);
        }
        if let Some(limit) = self.limit {
            builder.with_limit(limit);
        }
        for options in &self.options {
            builder.with_options(options.clone());
        }
        for filter in &self.filters {
            builder.with_filter(filter.clone());
        }
        for filter in &self.partition_filters {
            builder.with_partition_filter(filter.clone())?;
        }
        Ok(builder)
    }
}

#[pyclass(name = "VectorSearchBuilder", module = "pypaimon_rust.datafusion")]
pub struct PyVectorSearchBuilder {
    table: Arc<Table>,
    config: SearchConfig,
    query: Option<Vec<f32>>,
}

impl PyVectorSearchBuilder {
    pub(crate) fn new(table: Arc<Table>) -> Self {
        Self {
            table,
            config: SearchConfig::default(),
            query: None,
        }
    }
}

#[pymethods]
impl PyVectorSearchBuilder {
    fn with_vector_column(mut slf: PyRefMut<'_, Self>, name: String) -> PyRefMut<'_, Self> {
        slf.config.column = Some(name);
        slf
    }
    fn with_limit(mut slf: PyRefMut<'_, Self>, limit: usize) -> PyRefMut<'_, Self> {
        slf.config.limit = Some(limit);
        slf
    }
    fn with_query_vector(mut slf: PyRefMut<'_, Self>, vector: Vec<f32>) -> PyRefMut<'_, Self> {
        slf.query = Some(vector);
        slf
    }
    fn with_option(mut slf: PyRefMut<'_, Self>, key: String, value: String) -> PyRefMut<'_, Self> {
        slf.config.options.push(HashMap::from([(key, value)]));
        slf
    }
    fn with_options(
        mut slf: PyRefMut<'_, Self>,
        options: HashMap<String, String>,
    ) -> PyRefMut<'_, Self> {
        slf.config.options.push(options);
        slf
    }
    fn with_filter<'py>(
        mut slf: PyRefMut<'py, Self>,
        predicate: &Bound<'_, PyDict>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let predicate = dict_to_table_predicate(predicate, slf.table.schema(), true)?;
        slf.config.filters.push(predicate);
        Ok(slf)
    }
    fn with_partition_filter<'py>(
        mut slf: PyRefMut<'py, Self>,
        predicate: &Bound<'_, PyDict>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let predicate = dict_to_table_predicate(predicate, slf.table.schema(), true)?;
        slf.config
            .single(&slf.table)
            .map_err(to_py_err)?
            .with_partition_filter(predicate.clone())
            .map_err(to_py_err)?;
        slf.config.partition_filters.push(predicate);
        Ok(slf)
    }
    fn new_vector_search_scan(&self) -> PyResult<PyVectorScan> {
        let inner = self
            .config
            .single(&self.table)
            .map_err(to_py_err)?
            .new_scan()
            .map_err(to_py_err)?;
        Ok(PyVectorScan { inner })
    }
    fn new_vector_search_read(&self) -> PyResult<PyVectorRead> {
        let mut builder = self.config.single(&self.table).map_err(to_py_err)?;
        if let Some(query) = &self.query {
            builder.with_query_vector(query.clone());
        }
        let inner = builder.new_read().map_err(to_py_err)?;
        Ok(PyVectorRead { inner })
    }
    fn execute_local(&self, py: Python<'_>) -> PyResult<PyGlobalIndexResult> {
        let mut builder = self.config.single(&self.table).map_err(to_py_err)?;
        if let Some(query) = &self.query {
            builder.with_query_vector(query.clone());
        }
        let results = py
            .detach(|| runtime().block_on(builder.execute()))
            .map_err(to_py_err)?;
        Ok(PyGlobalIndexResult::new(results))
    }
}

#[pyclass(name = "BatchVectorSearchBuilder", module = "pypaimon_rust.datafusion")]
pub struct PyBatchVectorSearchBuilder {
    table: Arc<Table>,
    config: SearchConfig,
    query: Option<Vec<Vec<f32>>>,
}

impl PyBatchVectorSearchBuilder {
    pub(crate) fn new(table: Arc<Table>) -> Self {
        Self {
            table,
            config: SearchConfig::default(),
            query: None,
        }
    }
}

#[pymethods]
impl PyBatchVectorSearchBuilder {
    fn with_vector_column(mut slf: PyRefMut<'_, Self>, name: String) -> PyRefMut<'_, Self> {
        slf.config.column = Some(name);
        slf
    }
    fn with_limit(mut slf: PyRefMut<'_, Self>, limit: usize) -> PyRefMut<'_, Self> {
        slf.config.limit = Some(limit);
        slf
    }
    fn with_query_vectors(
        mut slf: PyRefMut<'_, Self>,
        vectors: Vec<Vec<f32>>,
    ) -> PyRefMut<'_, Self> {
        slf.query = Some(vectors);
        slf
    }
    fn with_option(mut slf: PyRefMut<'_, Self>, key: String, value: String) -> PyRefMut<'_, Self> {
        slf.config.options.push(HashMap::from([(key, value)]));
        slf
    }
    fn with_options(
        mut slf: PyRefMut<'_, Self>,
        options: HashMap<String, String>,
    ) -> PyRefMut<'_, Self> {
        slf.config.options.push(options);
        slf
    }
    fn with_filter<'py>(
        mut slf: PyRefMut<'py, Self>,
        predicate: &Bound<'_, PyDict>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let predicate = dict_to_table_predicate(predicate, slf.table.schema(), true)?;
        slf.config.filters.push(predicate);
        Ok(slf)
    }
    fn with_partition_filter<'py>(
        mut slf: PyRefMut<'py, Self>,
        predicate: &Bound<'_, PyDict>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let predicate = dict_to_table_predicate(predicate, slf.table.schema(), true)?;
        slf.config
            .batch(&slf.table)
            .map_err(to_py_err)?
            .with_partition_filter(predicate.clone())
            .map_err(to_py_err)?;
        slf.config.partition_filters.push(predicate);
        Ok(slf)
    }
    fn new_vector_search_scan(&self) -> PyResult<PyVectorScan> {
        let inner = self
            .config
            .batch(&self.table)
            .map_err(to_py_err)?
            .new_scan()
            .map_err(to_py_err)?;
        Ok(PyVectorScan { inner })
    }
    fn new_batch_vector_search_read(&self) -> PyResult<PyBatchVectorRead> {
        let mut builder = self.config.batch(&self.table).map_err(to_py_err)?;
        if let Some(query) = &self.query {
            builder.with_query_vectors(query.clone());
        }
        let inner = builder.new_read().map_err(to_py_err)?;
        Ok(PyBatchVectorRead { inner })
    }
    fn execute_batch_local(&self, py: Python<'_>) -> PyResult<Vec<PyGlobalIndexResult>> {
        let mut builder = self.config.batch(&self.table).map_err(to_py_err)?;
        if let Some(query) = &self.query {
            builder.with_query_vectors(query.clone());
        }
        let results = py
            .detach(|| runtime().block_on(builder.execute()))
            .map_err(to_py_err)?;
        Ok(results.into_iter().map(PyGlobalIndexResult::new).collect())
    }
}

#[pyclass(name = "VectorScan", module = "pypaimon_rust.datafusion")]
pub struct PyVectorScan {
    inner: VectorScan,
}

#[pymethods]
impl PyVectorScan {
    fn scan(&self, py: Python<'_>) -> PyResult<PyVectorScanPlan> {
        let inner = py
            .detach(|| runtime().block_on(self.inner.plan()))
            .map_err(to_py_err)?;
        Ok(PyVectorScanPlan { inner })
    }
}

#[pyclass(name = "VectorScanPlan", module = "pypaimon_rust.datafusion")]
pub struct PyVectorScanPlan {
    inner: VectorScanPlan,
}

#[pymethods]
impl PyVectorScanPlan {
    #[new]
    #[pyo3(signature = (splits, snapshot=None))]
    fn new(splits: &Bound<'_, PyAny>, snapshot: Option<PyRef<'_, PySnapshot>>) -> PyResult<Self> {
        Ok(Self {
            inner: VectorScanPlan::new(
                snapshot.map(|snapshot| snapshot.inner.clone()),
                vector_splits_from_python(splits)?,
            )
            .map_err(to_py_err)?,
        })
    }

    fn snapshot(&self) -> Option<PySnapshot> {
        self.inner.snapshot().cloned().map(PySnapshot::new)
    }

    fn splits(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        vector_splits_to_python(py, self.inner.splits())
    }

    fn __reduce__(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<(Py<PyAny>, Py<PyTuple>)> {
        let plan = slf.borrow();
        let args = (plan.splits(py)?, plan.snapshot())
            .into_pyobject(py)?
            .unbind();
        Ok((slf.get_type().unbind().into_any(), args))
    }
}

#[pyclass(name = "VectorRead", module = "pypaimon_rust.datafusion")]
pub struct PyVectorRead {
    inner: VectorRead,
}

#[pymethods]
impl PyVectorRead {
    fn read_plan(&self, py: Python<'_>, plan: &PyVectorScanPlan) -> PyResult<PyGlobalIndexResult> {
        py.detach(|| runtime().block_on(self.inner.read(plan.inner.clone())))
            .map(PyGlobalIndexResult::new)
            .map_err(to_py_err)
    }

    fn _read_plan_with_executor(
        &self,
        py: Python<'_>,
        plan: &PyVectorScanPlan,
        executor: Py<PyAny>,
    ) -> PyResult<PyGlobalIndexResult> {
        let executor = crate::search_task::PythonSearchTaskExecutor::new(executor);
        let result = py.detach(|| {
            runtime().block_on(paimon::table::internal::read_vector_with_executor(
                &self.inner,
                plan.inner.clone(),
                &executor,
            ))
        });
        if let Some(error) = executor.take_error() {
            return Err(error);
        }
        result.map(PyGlobalIndexResult::new).map_err(to_py_err)
    }
}

#[pyclass(name = "BatchVectorRead", module = "pypaimon_rust.datafusion")]
pub struct PyBatchVectorRead {
    inner: BatchVectorRead,
}

#[pymethods]
impl PyBatchVectorRead {
    fn read_batch_plan(
        &self,
        py: Python<'_>,
        plan: &PyVectorScanPlan,
    ) -> PyResult<Vec<PyGlobalIndexResult>> {
        py.detach(|| runtime().block_on(self.inner.read(plan.inner.clone())))
            .map(|results| results.into_iter().map(PyGlobalIndexResult::new).collect())
            .map_err(to_py_err)
    }

    fn _read_batch_plan_with_executor(
        &self,
        py: Python<'_>,
        plan: &PyVectorScanPlan,
        executor: Py<PyAny>,
    ) -> PyResult<Vec<PyGlobalIndexResult>> {
        let executor = crate::search_task::PythonSearchTaskExecutor::new(executor);
        let result = py.detach(|| {
            runtime().block_on(paimon::table::internal::read_batch_vector_with_executor(
                &self.inner,
                plan.inner.clone(),
                &executor,
            ))
        });
        if let Some(error) = executor.take_error() {
            return Err(error);
        }
        result
            .map(|results| results.into_iter().map(PyGlobalIndexResult::new).collect())
            .map_err(to_py_err)
    }
}

#[cfg(test)]
mod source_plan_tests {
    use super::*;
    use crate::search_plan::*;
    use paimon::spec::{BinaryRow, CommitKind, GlobalIndexMeta, IndexFileMeta, Snapshot};
    use paimon::table::{DataSplit, RowRange, SearchScanSplit};
    use pyo3::types::{PyBytes, PyList, PyModule};

    fn snapshot() -> Snapshot {
        Snapshot::builder()
            .version(3)
            .id(7)
            .schema_id(2)
            .base_manifest_list("base".into())
            .delta_manifest_list("delta".into())
            .commit_user("test".into())
            .commit_identifier(1)
            .commit_kind(CommitKind::APPEND)
            .time_millis(0)
            .next_row_id(Some(10))
            .build()
    }

    fn index_file() -> IndexFileMeta {
        IndexFileMeta {
            index_type: "lucene-vector".into(),
            file_name: "index-1".into(),
            file_size: 1024,
            row_count: 10,
            deletion_vectors_ranges: None,
            external_path: Some("s3://bucket/index-1".into()),
            global_index_meta: Some(GlobalIndexMeta {
                row_range_start: 0,
                row_range_end: 9,
                index_field_id: 2,
                extra_field_ids: Some(vec![3]),
                index_meta: Some(vec![1, 2, 3]),
                source_meta: None,
            }),
        }
    }

    fn module(py: Python<'_>) -> Bound<'_, PyModule> {
        // Initialize the actual binding module in this embedded interpreter.
        // This exercises class registration and pickle names without installing
        // an extension into the PyPaimon test environment.
        let package = PyModule::new(py, "pypaimon_rust").unwrap();
        crate::pypaimon_rust(py, &package).unwrap();
        py.import("sys")
            .unwrap()
            .getattr("modules")
            .unwrap()
            .set_item("pypaimon_rust", &package)
            .unwrap();
        package.getattr("datafusion").unwrap().cast_into().unwrap()
    }

    #[test]
    fn source_metadata_reuses_java_index_and_split_codecs() {
        Python::attach(|py| {
            let module = module(py);
            let data_split = DataSplit::builder()
                .with_snapshot(7)
                .with_partition(BinaryRow::new(0))
                .with_bucket(0)
                .with_bucket_path("file:/tmp/bucket-0".into())
                .with_total_buckets(1)
                .with_data_files(vec![])
                .build()
                .unwrap();
            let sources = vec![
                SearchScanSplit::Index {
                    column: "embedding".into(),
                    row_range: RowRange::new(0, 9),
                    index_files: vec![index_file()],
                    scalar_index_files: vec![],
                },
                SearchScanSplit::Raw {
                    row_ranges: vec![RowRange::new(2, 4)],
                    scalar_index_files: vec![index_file()],
                    index_type: Some("lucene-vector".into()),
                },
                SearchScanSplit::PrimaryKey {
                    data_split: data_split.clone(),
                    payloads: vec![index_file()],
                    uncovered_data_files: vec!["data-1.parquet".into()],
                    row_ranges_by_file: HashMap::from([(
                        "data-1.parquet".into(),
                        vec![RowRange::new(1, 2)],
                    )]),
                },
            ];
            let locals = PyDict::new(py);
            locals.set_item("sdk", &module).unwrap();
            locals
                .set_item("snapshot", PySnapshot::new(snapshot()))
                .unwrap();
            locals
                .set_item(
                    "index_bytes",
                    PyBytes::new(py, &index_file().to_serialized_row_data().unwrap()),
                )
                .unwrap();
            locals
                .set_item(
                    "split_bytes",
                    PyBytes::new(py, &data_split.serialize_split_v1().unwrap()),
                )
                .unwrap();
            for full_text in [false, true] {
                let objects = if full_text {
                    full_text_splits_to_python(py, &sources).unwrap()
                } else {
                    vector_splits_to_python(py, &sources).unwrap()
                };
                let values = PyList::new(py, &objects).unwrap();
                let restored = if full_text {
                    full_text_splits_from_python(values.as_any()).unwrap()
                } else {
                    vector_splits_from_python(values.as_any()).unwrap()
                };
                assert_eq!(
                    serde_json::to_value(restored).unwrap(),
                    serde_json::to_value(&sources).unwrap()
                );
                locals.set_item("sources", values).unwrap();
                locals.set_item("full_text", full_text).unwrap();
                py.run(
                    cr#"
import pickle
cls = sdk.FullTextScanPlan if full_text else sdk.VectorScanPlan
pk_cls = sdk.PrimaryKeyFullTextSearchSplit if full_text else sdk.PrimaryKeyVectorSearchSplit
for source in sources:
    copied = pickle.loads(pickle.dumps(source))
    assert type(copied) is type(source)
    assert copied._serialize() == source._serialize()
meta = sdk.IndexFileMeta._deserialize(index_bytes)
assert meta.file_name() == 'index-1'
assert pickle.loads(pickle.dumps(meta))._serialize() == index_bytes
pk = sources[2]
assert pk.data_split().serialize() == split_bytes
rebuilt = pk_cls(pk.data_split(), pk.payloads(), pk.uncovered_data_files(), pk.row_ranges_by_file())
plan = cls([rebuilt], snapshot)
restored = pickle.loads(pickle.dumps(plan))
assert restored.snapshot().id() == 7
assert restored.splits()[0].data_split().serialize() == split_bytes
assert restored.splits()[0].payloads()[0]._serialize() == index_bytes
assert restored.splits()[0].row_ranges_by_file() == {'data-1.parquet': [(1, 2)]}
"#,
                    Some(&locals),
                    Some(&locals),
                )
                .unwrap();
            }
        });
    }

    #[test]
    fn python_source_plan_constructors_capture_typed_snapshot_and_splits() {
        Python::attach(|py| {
            let locals = PyDict::new(py);
            locals.set_item("sdk", module(py)).unwrap();
            locals
                .set_item("snapshot", PySnapshot::new(snapshot()))
                .unwrap();
            locals
                .set_item(
                    "meta",
                    PyIndexFileMeta {
                        inner: index_file(),
                    },
                )
                .unwrap();
            py.run(cr#"
import pickle
for cls, raw_cls, indexed in (
    (sdk.VectorScanPlan, sdk.RawVectorSearchSplit, sdk.IndexVectorSearchSplit(0, 9, [meta])),
    (sdk.FullTextScanPlan, sdk.RawFullTextSearchSplit, sdk.IndexFullTextSearchSplit('body', 0, 9, [meta])),
):
    ranges = [(2, 4)]
    raw = raw_cls(ranges)
    sources = [indexed, raw]
    plan = cls(sources, snapshot)
    sources.clear()
    ranges.clear()
    assert plan.snapshot().id() == 7
    assert len(plan.splits()) == 2
    assert plan.splits()[1].row_ranges() == [(2, 4)]
    plan.splits().clear()
    assert len(plan.splits()) == 2
    assert len(cls(tuple(plan.splits()), snapshot).splits()) == 2
    restored = pickle.loads(pickle.dumps(plan))
    assert restored.snapshot()._to_json() == snapshot._to_json()
    assert restored.splits()[0].row_range_start() == 0
    assert restored.splits()[1].row_ranges() == [(2, 4)]
    assert not hasattr(plan, 'metadata')
    empty = cls([])
    assert empty.snapshot() is None and empty.splits() == []
    assert pickle.loads(pickle.dumps(empty)).snapshot() is None
"#, Some(&locals), Some(&locals)).unwrap();
        });
    }

    #[test]
    fn source_plan_constructors_reject_wire_values_and_wrong_source_types() {
        Python::attach(|py| {
            let locals = PyDict::new(py);
            locals.set_item("sdk", module(py)).unwrap();
            locals
                .set_item("snapshot", PySnapshot::new(snapshot()))
                .unwrap();
            py.run(
                cr#"
def rejects(error, fn, *args):
    try:
        fn(*args)
    except error:
        return
    raise AssertionError('Invalid source input was accepted')
for cls, raw_cls, wrong_cls in (
    (sdk.VectorScanPlan, sdk.RawVectorSearchSplit, sdk.RawFullTextSearchSplit),
    (sdk.FullTextScanPlan, sdk.RawFullTextSearchSplit, sdk.RawVectorSearchSplit),
):
    rejects(TypeError, cls, '[]', snapshot)
    rejects(TypeError, cls, '', snapshot)
    rejects(TypeError, cls, {}, snapshot)
    rejects(TypeError, cls, b'', snapshot)
    rejects(TypeError, cls, bytearray(), snapshot)
    rejects(TypeError, cls, [{'kind': 'raw'}], snapshot)
    rejects(TypeError, cls, [wrong_cls([(0, 1)])], snapshot)
    rejects(TypeError, cls, [], '{}')
    rejects(ValueError, cls, [raw_cls([(0, 1)])])
    rejects(ValueError, raw_cls, [(-1, 2)])
    rejects(ValueError, raw_cls, [(2, 1)])
rejects(ValueError, sdk.IndexFileMeta._deserialize, b'bad')
rejects(ValueError, sdk.Split.deserialize, b'bad')
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
        });
    }
}
