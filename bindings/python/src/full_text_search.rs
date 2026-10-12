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

//! Thin bindings over core full-text Scan -> Plan -> Read.

use crate::error::to_py_err;
use crate::predicate::dict_to_table_predicate;
use paimon::spec::Predicate;
use paimon::table::{FullTextRead, FullTextScan, FullTextScanPlan, Table};
use paimon_datafusion::runtime::runtime;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Default)]
struct Config {
    query: Option<(String, String)>,
    limit: Option<usize>,
    filters: Vec<Predicate>,
}

impl Config {
    fn builder<'a>(
        &self,
        table: &'a Table,
    ) -> paimon::Result<paimon::table::FullTextSearchBuilder<'a>> {
        let mut builder = table.new_full_text_search_builder();
        if let Some((column, query)) = &self.query {
            builder.with_query(column, query);
        }
        if let Some(limit) = self.limit {
            builder.with_limit(limit);
        }
        for filter in &self.filters {
            builder.with_filter(filter.clone());
        }
        Ok(builder)
    }
}

#[pyclass(name = "FullTextSearchBuilder", module = "pypaimon_rust.datafusion")]
pub struct PyFullTextSearchBuilder {
    table: Arc<Table>,
    config: Config,
}

impl PyFullTextSearchBuilder {
    pub(crate) fn new(table: Arc<Table>) -> Self {
        Self {
            table,
            config: Config::default(),
        }
    }
}

#[pymethods]
impl PyFullTextSearchBuilder {
    fn with_query(
        mut slf: PyRefMut<'_, Self>,
        field_name: String,
        query: String,
    ) -> PyRefMut<'_, Self> {
        slf.config.query = Some((field_name, query));
        slf
    }
    fn with_limit(mut slf: PyRefMut<'_, Self>, limit: usize) -> PyRefMut<'_, Self> {
        slf.config.limit = Some(limit);
        slf
    }
    fn with_filter<'py>(
        mut slf: PyRefMut<'py, Self>,
        predicate: &Bound<'_, PyDict>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let filter = dict_to_table_predicate(predicate, slf.table.schema(), true)?;
        slf.config.filters.push(filter);
        Ok(slf)
    }
    fn with_partition_filter<'py>(
        mut slf: PyRefMut<'py, Self>,
        predicate: &Bound<'_, PyDict>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let filter = dict_to_table_predicate(predicate, slf.table.schema(), true)?;
        slf.config
            .builder(&slf.table)
            .map_err(to_py_err)?
            .with_partition_filter(filter.clone())
            .map_err(to_py_err)?;
        slf.config.filters.push(filter);
        Ok(slf)
    }
    fn new_full_text_scan(&self) -> PyResult<PyFullTextScan> {
        Ok(PyFullTextScan {
            inner: self
                .config
                .builder(&self.table)
                .map_err(to_py_err)?
                .new_scan()
                .map_err(to_py_err)?,
        })
    }
    fn new_full_text_read(&self) -> PyResult<PyFullTextRead> {
        Ok(PyFullTextRead {
            inner: self
                .config
                .builder(&self.table)
                .map_err(to_py_err)?
                .new_read()
                .map_err(to_py_err)?,
        })
    }
    fn execute_local(&self, py: Python<'_>) -> PyResult<PyFullTextSearchResult> {
        let builder = self.config.builder(&self.table).map_err(to_py_err)?;
        py.detach(|| runtime().block_on(builder.execute_scored()))
            .map(|inner| PyFullTextSearchResult { inner })
            .map_err(to_py_err)
    }
}

#[pyclass(name = "FullTextScan", module = "pypaimon_rust.datafusion")]
pub struct PyFullTextScan {
    inner: FullTextScan,
}
#[pymethods]
impl PyFullTextScan {
    fn scan(&self, py: Python<'_>) -> PyResult<PyFullTextScanPlan> {
        py.detach(|| runtime().block_on(self.inner.scan()))
            .map(|inner| PyFullTextScanPlan { inner })
            .map_err(to_py_err)
    }
}

#[pyclass(name = "FullTextScanPlan", module = "pypaimon_rust.datafusion")]
pub struct PyFullTextScanPlan {
    inner: FullTextScanPlan,
}
#[pymethods]
impl PyFullTextScanPlan {
    fn snapshot_id(&self) -> Option<i64> {
        self.inner.snapshot_id()
    }
}

#[pyclass(name = "FullTextRead", module = "pypaimon_rust.datafusion")]
pub struct PyFullTextRead {
    inner: FullTextRead,
}
#[pymethods]
impl PyFullTextRead {
    fn read_plan(
        &self,
        py: Python<'_>,
        plan: &PyFullTextScanPlan,
    ) -> PyResult<PyFullTextSearchResult> {
        py.detach(|| runtime().block_on(self.inner.read(plan.inner.clone())))
            .map(|inner| PyFullTextSearchResult { inner })
            .map_err(to_py_err)
    }
}

#[pyclass(name = "FullTextSearchResult", module = "pypaimon_rust.datafusion")]
pub struct PyFullTextSearchResult {
    inner: paimon::full_text::SearchResult,
}
#[pymethods]
impl PyFullTextSearchResult {
    fn __len__(&self) -> usize {
        self.inner.len()
    }
    fn row_ids(&self) -> HashMap<u64, f32> {
        self.inner
            .row_ids
            .iter()
            .copied()
            .zip(self.inner.scores.iter().copied())
            .collect()
    }
}
