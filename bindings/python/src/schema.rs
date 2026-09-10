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

use paimon::spec::{DataField, TableSchema};
use pyo3::prelude::*;

#[pyclass(name = "TableSchema", module = "pypaimon_rust.datafusion")]
pub struct PyTableSchema {
    inner: TableSchema,
}

impl PyTableSchema {
    pub fn new(inner: TableSchema) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyTableSchema {
    fn fields(&self) -> Vec<PyDataField> {
        self.inner
            .fields()
            .iter()
            .cloned()
            .map(PyDataField::new)
            .collect()
    }

    fn partition_keys(&self) -> Vec<String> {
        self.inner.partition_keys().to_vec()
    }

    fn primary_keys(&self) -> Vec<String> {
        self.inner.primary_keys().to_vec()
    }

    fn options(&self) -> HashMap<String, String> {
        self.inner.options().clone()
    }

    fn comment(&self) -> Option<String> {
        self.inner.comment().map(str::to_string)
    }
}

#[pyclass(name = "DataField", module = "pypaimon_rust.datafusion")]
pub struct PyDataField {
    inner: DataField,
}

impl PyDataField {
    pub fn new(inner: DataField) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyDataField {
    fn name(&self) -> String {
        self.inner.name().to_string()
    }

    fn field_type(&self) -> String {
        // Java `DataField.type().asSQLString()`, which is what
        // `Display for DataType` renders.
        self.inner.data_type().to_string()
    }

    fn is_nullable(&self) -> bool {
        self.inner.data_type().is_nullable()
    }

    fn description(&self) -> Option<String> {
        self.inner.description().map(str::to_string)
    }
}
