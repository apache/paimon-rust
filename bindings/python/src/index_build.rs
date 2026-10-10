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

//! Parameter and commit-message wrappers for the core global-index builder.

use std::collections::HashMap;
use std::sync::Arc;

use paimon::spec::Predicate;
use paimon::table::{GlobalIndexBuildBuilder, Table};
use paimon_datafusion::runtime::runtime;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::error::to_py_err;
use crate::predicate::dict_to_table_predicate;
use crate::write::PyCommitMessage;

enum PyIndexColumns {
    Column(String),
    Columns(Vec<String>),
}

#[pyclass(name = "GlobalIndexBuildBuilder", module = "pypaimon_rust.datafusion")]
pub struct PyGlobalIndexBuildBuilder {
    table: Arc<Table>,
    columns: Option<PyIndexColumns>,
    index_type: String,
    options: HashMap<String, String>,
    partition_filters: Vec<Predicate>,
}

impl PyGlobalIndexBuildBuilder {
    pub(crate) fn new(table: Arc<Table>) -> Self {
        Self {
            table,
            columns: None,
            index_type: "btree".into(),
            options: HashMap::new(),
            partition_filters: vec![],
        }
    }

    fn builder(&self) -> paimon::Result<GlobalIndexBuildBuilder<'_>> {
        let mut builder = self.table.new_global_index_build_builder();
        match &self.columns {
            Some(PyIndexColumns::Column(column)) => {
                builder.with_index_column(column);
            }
            Some(PyIndexColumns::Columns(columns)) => {
                builder.with_index_columns(&columns.iter().map(String::as_str).collect::<Vec<_>>());
            }
            None => {}
        }
        builder
            .with_index_type(&self.index_type)
            .with_options(self.options.clone());
        for filter in &self.partition_filters {
            builder.with_partition_filter(filter.clone())?;
        }
        Ok(builder)
    }
}

#[pymethods]
impl PyGlobalIndexBuildBuilder {
    #[staticmethod]
    fn supports_index_type(index_type: &str) -> bool {
        GlobalIndexBuildBuilder::supports_index_type(index_type)
    }

    fn with_index_columns(mut slf: PyRefMut<'_, Self>, columns: Vec<String>) -> PyRefMut<'_, Self> {
        slf.columns = Some(PyIndexColumns::Columns(columns));
        slf
    }

    fn with_index_column(mut slf: PyRefMut<'_, Self>, column: String) -> PyRefMut<'_, Self> {
        slf.columns = Some(PyIndexColumns::Column(column));
        slf
    }

    fn with_index_type(mut slf: PyRefMut<'_, Self>, index_type: String) -> PyRefMut<'_, Self> {
        slf.index_type = index_type;
        slf
    }

    fn with_options(
        mut slf: PyRefMut<'_, Self>,
        options: HashMap<String, String>,
    ) -> PyRefMut<'_, Self> {
        slf.options = options;
        slf
    }

    fn with_partition_filter<'py>(
        mut slf: PyRefMut<'py, Self>,
        predicate: &Bound<'_, PyDict>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let filter = dict_to_table_predicate(predicate, slf.table.schema(), true)?;
        // Validation and conjunction belong to the core builder.
        let mut builder = slf.builder().map_err(to_py_err)?;
        builder
            .with_partition_filter(filter.clone())
            .map_err(to_py_err)?;
        slf.partition_filters.push(filter);
        Ok(slf)
    }

    fn build(&self, py: Python<'_>) -> PyResult<Vec<PyCommitMessage>> {
        let builder = self.builder().map_err(to_py_err)?;
        let messages = py
            .detach(|| runtime().block_on(builder.build()))
            .map_err(to_py_err)?;
        Ok(messages.into_iter().map(PyCommitMessage::new).collect())
    }

    fn execute(&self, py: Python<'_>) -> PyResult<usize> {
        let builder = self.builder().map_err(to_py_err)?;
        py.detach(|| runtime().block_on(builder.execute()))
            .map_err(to_py_err)
    }
}
