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
use crate::global_index_result::PyGlobalIndexResult;
use crate::predicate::dict_to_table_predicate;
use crate::search_plan::{full_text_splits_from_python, full_text_splits_to_python};
use crate::snapshot::PySnapshot;
use paimon::spec::Predicate;
use paimon::table::{FullTextRead, FullTextScan, FullTextScanPlan, Table};
use paimon_datafusion::runtime::runtime;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
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
    fn execute(&self, py: Python<'_>) -> PyResult<PyGlobalIndexResult> {
        let builder = self.config.builder(&self.table).map_err(to_py_err)?;
        py.detach(|| runtime().block_on(builder.execute_scored()))
            .map(PyGlobalIndexResult::new)
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
    #[new]
    #[pyo3(signature = (splits, snapshot=None))]
    fn new(splits: &Bound<'_, PyAny>, snapshot: Option<PyRef<'_, PySnapshot>>) -> PyResult<Self> {
        Ok(Self {
            inner: FullTextScanPlan::new(
                snapshot.map(|snapshot| snapshot.inner.clone()),
                full_text_splits_from_python(splits)?,
            )
            .map_err(to_py_err)?,
        })
    }

    fn snapshot(&self) -> Option<PySnapshot> {
        self.inner.snapshot().cloned().map(PySnapshot::new)
    }

    fn splits(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        full_text_splits_to_python(py, self.inner.splits())
    }

    fn __reduce__(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<(Py<PyAny>, Py<PyTuple>)> {
        let plan = slf.borrow();
        let args = (plan.splits(py)?, plan.snapshot())
            .into_pyobject(py)?
            .unbind();
        Ok((slf.get_type().unbind().into_any(), args))
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
    ) -> PyResult<PyGlobalIndexResult> {
        py.detach(|| runtime().block_on(self.inner.read(plan.inner.clone())))
            .map(PyGlobalIndexResult::new)
            .map_err(to_py_err)
    }

    fn _read_plan_with_executor(
        &self,
        py: Python<'_>,
        plan: &PyFullTextScanPlan,
        executor: Py<PyAny>,
    ) -> PyResult<PyGlobalIndexResult> {
        let executor = crate::search_task::PythonSearchTaskExecutor::new(executor);
        let result = py.detach(|| {
            runtime().block_on(paimon::table::internal::read_full_text_with_executor(
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
