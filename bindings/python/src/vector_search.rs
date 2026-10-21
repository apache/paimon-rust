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
use paimon::table::{
    BatchVectorRead, DataSplit, RowRange, SearchScanSplit, Table, VectorRead, VectorScan,
    VectorScanPlan,
};
use paimon_datafusion::runtime::runtime;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::error::to_py_err;
use crate::global_index_result::PyGlobalIndexResult;
use crate::predicate::dict_to_table_predicate;

pub(crate) fn search_plan_metadata_json(
    snapshot: Option<&paimon::spec::Snapshot>,
    snapshot_id: Option<i64>,
    splits: &[SearchScanSplit],
) -> PyResult<String> {
    use serde_json::json;
    let files = |files: &[paimon::spec::IndexFileMeta]| {
        files
            .iter()
            .map(|file| file.to_serialized_row_data().map_err(to_py_err))
            .collect::<PyResult<Vec<_>>>()
    };
    let splits = splits.iter().map(|split| Ok(match split {
        SearchScanSplit::Index { column, row_range, index_files, scalar_index_files } => json!({
            "kind": "index", "column": column, "row_range": row_range,
            "index_files": files(index_files)?, "scalar_index_files": files(scalar_index_files)?,
        }),
        SearchScanSplit::Raw { row_ranges, scalar_index_files, index_type } => json!({
            "kind": "raw", "row_ranges": row_ranges, "scalar_index_files": files(scalar_index_files)?, "index_type": index_type,
        }),
        SearchScanSplit::PrimaryKey { data_split, payloads, uncovered_data_files, row_ranges_by_file } => json!({
            "kind": "primary-key", "data_split": data_split.serialize_split_v1().map_err(to_py_err)?,
            "payloads": files(payloads)?, "uncovered_data_files": uncovered_data_files,
            "row_ranges_by_file": row_ranges_by_file,
        }),
    })).collect::<PyResult<Vec<_>>>()?;
    serde_json::to_string(
        &json!({"snapshot": snapshot, "snapshot_id": snapshot_id, "splits": splits}),
    )
    .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))
}

/// Decode the same Java metadata codecs used by source-plan export. The binding
/// does not infer work from the snapshot or bind a plan to a live reader.
pub(crate) fn search_plan_from_json(
    snapshot_json: Option<&str>,
    splits_json: &str,
) -> PyResult<(Option<paimon::spec::Snapshot>, Vec<SearchScanSplit>)> {
    use pyo3::exceptions::PyValueError;
    use serde_json::Value;

    let invalid = |error: serde_json::Error| PyValueError::new_err(error.to_string());
    let snapshot = snapshot_json
        .map(serde_json::from_str)
        .transpose()
        .map_err(invalid)?;
    let values: Vec<Value> = serde_json::from_str(splits_json).map_err(invalid)?;
    let field = |value: &Value, name: &str| -> PyResult<Value> {
        value
            .get(name)
            .cloned()
            .ok_or_else(|| PyValueError::new_err(format!("Search split is missing '{name}'")))
    };
    let files = |value: Value| -> PyResult<Vec<paimon::spec::IndexFileMeta>> {
        let bytes: Vec<Vec<u8>> = serde_json::from_value(value).map_err(invalid)?;
        bytes
            .iter()
            .map(|bytes| {
                paimon::spec::IndexFileMeta::from_serialized_row_data(bytes).map_err(to_py_err)
            })
            .collect()
    };
    let validate_ranges = |ranges: &[RowRange]| -> PyResult<()> {
        if ranges
            .iter()
            .any(|range| range.from() < 0 || range.from() > range.to())
        {
            return Err(PyValueError::new_err(
                "Search row ranges must be nonnegative and ordered",
            ));
        }
        Ok(())
    };
    let splits = values
        .iter()
        .map(|value| -> PyResult<SearchScanSplit> {
            let kind: String = serde_json::from_value(field(value, "kind")?).map_err(invalid)?;
            Ok(match kind.as_str() {
                "index" => {
                    let row_range: RowRange =
                        serde_json::from_value(field(value, "row_range")?).map_err(invalid)?;
                    validate_ranges(std::slice::from_ref(&row_range))?;
                    SearchScanSplit::Index {
                        column: serde_json::from_value(field(value, "column")?).map_err(invalid)?,
                        row_range,
                        index_files: files(field(value, "index_files")?)?,
                        scalar_index_files: files(field(value, "scalar_index_files")?)?,
                    }
                }
                "raw" => {
                    let row_ranges: Vec<RowRange> =
                        serde_json::from_value(field(value, "row_ranges")?).map_err(invalid)?;
                    validate_ranges(&row_ranges)?;
                    SearchScanSplit::Raw {
                        row_ranges,
                        scalar_index_files: files(field(value, "scalar_index_files")?)?,
                        index_type: serde_json::from_value(field(value, "index_type")?)
                            .map_err(invalid)?,
                    }
                }
                "primary-key" => {
                    let bytes: Vec<u8> =
                        serde_json::from_value(field(value, "data_split")?).map_err(invalid)?;
                    let row_ranges_by_file: HashMap<String, Vec<RowRange>> =
                        serde_json::from_value(field(value, "row_ranges_by_file")?)
                            .map_err(invalid)?;
                    for ranges in row_ranges_by_file.values() {
                        validate_ranges(ranges)?;
                    }
                    SearchScanSplit::PrimaryKey {
                        data_split: DataSplit::deserialize_split_v1(&bytes).map_err(to_py_err)?,
                        payloads: files(field(value, "payloads")?)?,
                        uncovered_data_files: serde_json::from_value(field(
                            value,
                            "uncovered_data_files",
                        )?)
                        .map_err(invalid)?,
                        row_ranges_by_file,
                    }
                }
                _ => {
                    return Err(PyValueError::new_err(format!(
                        "Unknown search split kind '{kind}'"
                    )))
                }
            })
        })
        .collect::<PyResult<Vec<_>>>()?;
    Ok((snapshot, splits))
}

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
    #[pyo3(signature = (snapshot_json, splits_json))]
    fn new(snapshot_json: Option<&str>, splits_json: &str) -> PyResult<Self> {
        let (snapshot, splits) = search_plan_from_json(snapshot_json, splits_json)?;
        Ok(Self {
            inner: VectorScanPlan::new(snapshot, splits).map_err(to_py_err)?,
        })
    }

    fn metadata(&self) -> PyResult<String> {
        search_plan_metadata_json(
            self.inner.snapshot(),
            self.inner.snapshot_id(),
            self.inner.splits(),
        )
    }

    fn snapshot_id(&self) -> Option<i64> {
        self.inner.snapshot_id()
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
    use crate::full_text_search::PyFullTextScanPlan;
    use paimon::spec::{BinaryRow, CommitKind, IndexFileMeta, Snapshot};
    use pyo3::types::PyModule;
    use serde_json::{json, Value};

    fn snapshot_json() -> String {
        serde_json::to_string(
            &Snapshot::builder()
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
                .build(),
        )
        .unwrap()
    }

    fn index_file() -> IndexFileMeta {
        IndexFileMeta {
            index_type: "lucene-vector".into(),
            file_name: "index-1".into(),
            file_size: 1024,
            row_count: 10,
            deletion_vectors_ranges: None,
            external_path: Some("s3://bucket/index-1".into()),
            global_index_meta: None,
        }
    }

    #[test]
    fn source_metadata_reuses_java_index_and_split_codecs() {
        Python::attach(|_| {
            let split = DataSplit::builder()
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
                    data_split: split,
                    payloads: vec![index_file()],
                    uncovered_data_files: vec!["data-1.parquet".into()],
                    row_ranges_by_file: HashMap::from([(
                        "data-1.parquet".into(),
                        vec![RowRange::new(1, 2)],
                    )]),
                },
            ];
            let snapshot: Snapshot = serde_json::from_str(&snapshot_json()).unwrap();
            let metadata = search_plan_metadata_json(Some(&snapshot), Some(7), &sources).unwrap();
            let value: Value = serde_json::from_str(&metadata).unwrap();
            let (restored_snapshot, restored) =
                search_plan_from_json(Some(&snapshot_json()), &value["splits"].to_string())
                    .unwrap();
            assert_eq!(restored_snapshot.as_ref(), Some(&snapshot));
            assert_eq!(
                serde_json::from_str::<Value>(
                    &search_plan_metadata_json(restored_snapshot.as_ref(), Some(7), &restored)
                        .unwrap()
                )
                .unwrap(),
                value,
            );
            assert_eq!(
                value["splits"][0]["index_files"][0],
                json!(index_file().to_serialized_row_data().unwrap())
            );
        });
    }

    #[test]
    fn python_source_plan_constructors_capture_snapshot_and_splits() {
        Python::attach(|py| {
            let module = PyModule::new(py, "plans").unwrap();
            module.add_class::<PyVectorScanPlan>().unwrap();
            module.add_class::<PyFullTextScanPlan>().unwrap();
            let sources = json!([{
                "kind": "raw", "row_ranges": [{"from": 2, "to": 4}],
                "scalar_index_files": [], "index_type": null,
            }])
            .to_string();
            for name in ["VectorScanPlan", "FullTextScanPlan"] {
                let class = module.getattr(name).unwrap();
                let plan = class.call1((snapshot_json(), &sources)).unwrap();
                assert_eq!(
                    plan.call_method0("snapshot_id")
                        .unwrap()
                        .extract::<i64>()
                        .unwrap(),
                    7
                );
                let metadata: String = plan.call_method0("metadata").unwrap().extract().unwrap();
                let value: Value = serde_json::from_str(&metadata).unwrap();
                assert_eq!(value["splits"].to_string(), sources);
                assert_eq!(value["snapshot"]["id"], json!(7));
                let copied = class
                    .call1((value["snapshot"].to_string(), value["splits"].to_string()))
                    .unwrap();
                let copied_metadata: String =
                    copied.call_method0("metadata").unwrap().extract().unwrap();
                assert_eq!(metadata, copied_metadata);
                let empty = class.call1((py.None(), "[]")).unwrap();
                assert!(empty.call_method0("snapshot_id").unwrap().is_none());
                assert!(class.call1((py.None(), &sources)).is_err());
            }
        });
    }

    #[test]
    fn source_plan_decode_rejects_corrupt_codecs_and_ranges() {
        Python::attach(|_| {
            for value in [
                json!([{"kind": "unknown"}]),
                json!([{"kind": "index", "column": "v", "row_range": {"from": 2, "to": 1}, "index_files": [], "scalar_index_files": []}]),
                json!([{"kind": "raw", "row_ranges": [{"from": -1, "to": 1}], "scalar_index_files": [], "index_type": null}]),
                json!([{"kind": "index", "column": "v", "row_range": {"from": 0, "to": 1}, "index_files": [[1, 2, 3]], "scalar_index_files": []}]),
                json!([{"kind": "primary-key", "data_split": [1, 2, 3], "payloads": [], "uncovered_data_files": [], "row_ranges_by_file": {}}]),
            ] {
                assert!(
                    search_plan_from_json(Some(&snapshot_json()), &value.to_string()).is_err(),
                    "{value}"
                );
            }
            assert!(search_plan_from_json(Some("invalid snapshot"), "[]").is_err());
        });
    }
}
