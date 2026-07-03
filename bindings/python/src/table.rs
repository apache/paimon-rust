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

use std::sync::Arc;

use pyo3::prelude::*;

use crate::read::PyReadBuilder;
use crate::schema::PyTableSchema;
use crate::write::PyWriteBuilder;

#[pyclass(name = "Table", module = "pypaimon_rust.datafusion")]
pub struct PyTable {
    pub(crate) inner: Arc<paimon::table::Table>,
}

impl PyTable {
    pub fn new(inner: Arc<paimon::table::Table>) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyTable {
    fn identifier(&self) -> String {
        let id = self.inner.identifier();
        format!("{}.{}", id.database(), id.object())
    }

    fn location(&self) -> String {
        self.inner.location().to_string()
    }

    fn schema(&self) -> PyTableSchema {
        PyTableSchema::new(self.inner.schema().clone())
    }

    /// Create a [`PyReadBuilder`] for DataFrame-style scan planning.
    fn new_read_builder(&self) -> PyReadBuilder {
        PyReadBuilder::new(Arc::clone(&self.inner))
    }

    /// Create a [`PyWriteBuilder`] for the batch write loop.
    fn new_write_builder(&self) -> PyWriteBuilder {
        PyWriteBuilder::new(Arc::clone(&self.inner))
    }
}
