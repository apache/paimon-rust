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
use pyo3::types::PyDict;

use crate::error::to_py_err;
use crate::global_index_result::PyGlobalIndexResult;
use crate::predicate::dict_to_table_predicate;

pub(crate) fn search_plan_metadata_json(
    snapshot: Option<&paimon::spec::Snapshot>,
    snapshot_id: Option<i64>,
    splits: Vec<paimon::table::SearchScanSplit>,
) -> PyResult<String> {
    use paimon::table::SearchScanSplit;
    use serde_json::json;
    let files = |files: &[paimon::spec::IndexFileMeta]| {
        files
            .iter()
            .map(|file| file.to_serialized_row_data().map_err(to_py_err))
            .collect::<PyResult<Vec<_>>>()
    };
    let splits = splits.into_iter().map(|split| Ok(match split {
        SearchScanSplit::Index { column, row_range, index_files, scalar_index_files } => json!({
            "kind": "index", "column": column, "row_range": row_range,
            "index_files": files(&index_files)?, "scalar_index_files": files(&scalar_index_files)?,
        }),
        SearchScanSplit::Raw { row_ranges, scalar_index_files, index_type } => json!({
            "kind": "raw", "row_ranges": row_ranges, "scalar_index_files": files(&scalar_index_files)?, "index_type": index_type,
        }),
        SearchScanSplit::PrimaryKey { data_split, payloads, uncovered_data_files, row_ranges_by_file } => json!({
            "kind": "primary-key", "data_split": data_split.serialize_split_v1().map_err(to_py_err)?,
            "payloads": files(&payloads)?, "uncovered_data_files": uncovered_data_files,
            "row_ranges_by_file": row_ranges_by_file,
        }),
    })).collect::<PyResult<Vec<_>>>()?;
    serde_json::to_string(
        &json!({"snapshot": snapshot, "snapshot_id": snapshot_id, "splits": splits}),
    )
    .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))
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
        Ok(PyVectorScan {
            inner,
            options: self
                .config
                .options
                .iter()
                .flat_map(|options| options.clone())
                .collect(),
        })
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
        Ok(PyVectorScan {
            inner,
            options: self
                .config
                .options
                .iter()
                .flat_map(|options| options.clone())
                .collect(),
        })
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
    options: HashMap<String, String>,
}

#[pymethods]
impl PyVectorScan {
    fn scan(&self, py: Python<'_>) -> PyResult<PyVectorScanPlan> {
        let inner = py
            .detach(|| runtime().block_on(self.inner.plan()))
            .map_err(to_py_err)?;
        Ok(PyVectorScanPlan {
            inner,
            options: self.options.clone(),
        })
    }
}

#[pyclass(name = "VectorScanPlan", module = "pypaimon_rust.datafusion")]
pub struct PyVectorScanPlan {
    inner: VectorScanPlan,
    options: HashMap<String, String>,
}

#[pymethods]
impl PyVectorScanPlan {
    fn _metadata(&self, py: Python<'_>) -> PyResult<String> {
        let splits = py
            .detach(|| runtime().block_on(self.inner.splits(&self.options)))
            .map_err(to_py_err)?;
        search_plan_metadata_json(self.inner.snapshot(), self.inner.snapshot_id(), splits)
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
}
