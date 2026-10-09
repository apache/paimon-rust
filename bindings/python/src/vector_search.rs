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

use arrow::pyarrow::ToPyArrow;
use futures::TryStreamExt;
use paimon::spec::Predicate;
use paimon::table::{BatchVectorRead, Table, VectorRead, VectorScan, VectorScanPlan};
use paimon::vector_search::SearchResult;
use paimon_datafusion::runtime::runtime;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

use crate::error::to_py_err;
use crate::predicate::dict_to_table_predicate;

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
    fn with_query_vector(mut slf: PyRefMut<'_, Self>, vectors: Vec<f32>) -> PyRefMut<'_, Self> {
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
    fn execute_local(&self, py: Python<'_>) -> PyResult<PySearchResult> {
        let mut builder = self.config.single(&self.table).map_err(to_py_err)?;
        if let Some(query) = &self.query {
            builder.with_query_vector(query.clone());
        }
        let results = py
            .detach(|| runtime().block_on(builder.execute()))
            .map_err(to_py_err)?;
        Ok(PySearchResult::new(results))
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
    fn execute_batch_local(&self, py: Python<'_>) -> PyResult<Vec<PySearchResult>> {
        let mut builder = self.config.batch(&self.table).map_err(to_py_err)?;
        if let Some(query) = &self.query {
            builder.with_query_vectors(query.clone());
        }
        let results = py
            .detach(|| runtime().block_on(builder.execute()))
            .map_err(to_py_err)?;
        Ok(results.into_iter().map(PySearchResult::new).collect())
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
    fn read_plan(&self, py: Python<'_>, plan: &PyVectorScanPlan) -> PyResult<PySearchResult> {
        py.detach(|| runtime().block_on(self.inner.read(plan.inner.clone())))
            .map(PySearchResult::new)
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
    ) -> PyResult<Vec<PySearchResult>> {
        py.detach(|| runtime().block_on(self.inner.read(plan.inner.clone())))
            .map(|results| results.into_iter().map(PySearchResult::new).collect())
            .map_err(to_py_err)
    }
}

#[pyclass(name = "SearchResult", module = "pypaimon_rust.datafusion")]
pub struct PySearchResult {
    inner: SearchResult,
}

impl PySearchResult {
    fn new(inner: SearchResult) -> Self {
        Self { inner }
    }
}

type PySearchPosition = (Py<PyBytes>, i32, String, i64, f32);

#[pymethods]
impl PySearchResult {
    fn snapshot_id(&self) -> Option<i64> {
        self.inner.snapshot_id()
    }
    fn new_read_builder(&self) -> PySearchResultReadBuilder {
        PySearchResultReadBuilder {
            result: self.inner.clone(),
            projection: None,
        }
    }
    fn __len__(&self) -> usize {
        self.inner.len()
    }
    fn row_ids(&self) -> PyResult<HashMap<u64, f32>> {
        let hits = self.inner.row_ids().map_err(to_py_err)?;
        Ok(hits
            .row_ids
            .iter()
            .copied()
            .zip(hits.scores.iter().copied())
            .collect())
    }
    fn positions(&self, py: Python<'_>) -> PyResult<Vec<PySearchPosition>> {
        Ok(self
            .inner
            .positions()
            .map_err(to_py_err)?
            .iter()
            .map(|position| {
                (
                    PyBytes::new(py, &position.partition.to_serialized_bytes()).unbind(),
                    position.bucket,
                    position.data_file_name.clone(),
                    position.row_position,
                    position.score,
                )
            })
            .collect())
    }
    fn splits(&self, py: Python<'_>) -> PyResult<Vec<Py<PyBytes>>> {
        Ok(self
            .inner
            .serialize_primary_key_splits()
            .map_err(to_py_err)?
            .iter()
            .map(|bytes| PyBytes::new(py, bytes).unbind())
            .collect())
    }
}

#[pyclass(name = "SearchResultReadBuilder", module = "pypaimon_rust.datafusion")]
pub struct PySearchResultReadBuilder {
    result: SearchResult,
    projection: Option<Vec<String>>,
}

#[pymethods]
impl PySearchResultReadBuilder {
    fn with_projection(mut slf: PyRefMut<'_, Self>, columns: Vec<String>) -> PyRefMut<'_, Self> {
        slf.projection = Some(columns);
        slf
    }
    fn read(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        let batches = py
            .detach(|| {
                runtime().block_on(async {
                    let mut builder = self.result.new_read_builder();
                    if let Some(projection) = &self.projection {
                        let columns: Vec<&str> = projection.iter().map(String::as_str).collect();
                        builder.with_projection(&columns);
                    }
                    builder.read().await?.try_collect::<Vec<_>>().await
                })
            })
            .map_err(to_py_err)?;
        batches
            .iter()
            .map(|batch| batch.to_pyarrow(py).map(Bound::unbind))
            .collect()
    }
}
