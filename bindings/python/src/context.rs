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

use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use datafusion::catalog::CatalogProvider;
use datafusion_ffi::catalog_provider::FFI_CatalogProvider;
use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use paimon::{CatalogFactory, Options};
use paimon_datafusion::{PaimonCatalogProvider, SQLContext};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

use crate::error::{df_to_py_err, to_py_err};
use paimon_datafusion::runtime::runtime;

fn build_paimon_catalog_provider(
    catalog_options: HashMap<String, String>,
) -> PyResult<Arc<PaimonCatalogProvider>> {
    let rt = runtime();
    rt.block_on(async {
        let options = Options::from_map(catalog_options);
        let catalog = CatalogFactory::create(options).await.map_err(to_py_err)?;
        Ok::<_, PyErr>(Arc::new(PaimonCatalogProvider::new(catalog)))
    })
}

fn ffi_logical_codec_from_pycapsule(obj: Bound<'_, PyAny>) -> PyResult<FFI_LogicalExtensionCodec> {
    let attr_name = "__datafusion_logical_extension_codec__";
    let capsule = if obj.hasattr(attr_name)? {
        obj.getattr(attr_name)?.call0()?
    } else {
        obj
    };

    let capsule = capsule.cast::<PyCapsule>()?;
    let expected_name = c"datafusion_logical_extension_codec";
    let ptr = capsule.pointer_checked(Some(expected_name))?;
    let codec = unsafe { ptr.cast::<FFI_LogicalExtensionCodec>().as_ref() };

    Ok(codec.clone())
}

/// A Paimon catalog exportable to Python DataFusion `SessionContext`.
#[pyclass(name = "PaimonCatalog")]
pub struct PaimonCatalog {
    provider: Arc<PaimonCatalogProvider>,
}

#[pymethods]
impl PaimonCatalog {
    /// Create a Paimon catalog that can be registered into a DataFusion session.
    #[new]
    fn new(catalog_options: HashMap<String, String>) -> PyResult<Self> {
        Ok(Self {
            provider: build_paimon_catalog_provider(catalog_options)?,
        })
    }

    /// Export this catalog as a DataFusion catalog provider PyCapsule.
    fn __datafusion_catalog_provider__<'py>(
        &self,
        py: Python<'py>,
        session: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let name = cr"datafusion_catalog_provider".into();
        let provider = Arc::clone(&self.provider) as Arc<dyn CatalogProvider + Send>;
        let codec = ffi_logical_codec_from_pycapsule(session)?;
        let provider = FFI_CatalogProvider::new_with_ffi_codec(provider, Some(runtime()), codec);
        PyCapsule::new(py, provider, Some(name))
    }
}

/// A SQL context that supports registering multiple Paimon catalogs and executing SQL.
#[pyclass(name = "SQLContext")]
pub struct PySQLContext {
    inner: SQLContext,
}

#[pymethods]
impl PySQLContext {
    #[new]
    fn new() -> Self {
        Self {
            inner: SQLContext::new(),
        }
    }

    fn register_catalog(
        &mut self,
        catalog_name: String,
        catalog_options: HashMap<String, String>,
    ) -> PyResult<()> {
        let rt = runtime();
        rt.block_on(async {
            let options = Options::from_map(catalog_options);
            let catalog = CatalogFactory::create(options).await.map_err(to_py_err)?;
            self.inner
                .register_catalog(catalog_name, catalog)
                .await
                .map_err(df_to_py_err)
        })
    }

    fn set_current_catalog(&mut self, catalog_name: String) -> PyResult<()> {
        let rt = runtime();
        rt.block_on(async {
            self.inner
                .set_current_catalog(catalog_name)
                .await
                .map_err(df_to_py_err)
        })
    }

    fn set_current_database(&self, database_name: String) -> PyResult<()> {
        let rt = runtime();
        rt.block_on(async {
            self.inner
                .set_current_database(&database_name)
                .await
                .map_err(df_to_py_err)
        })
    }

    fn register_batch(&self, name: String, batch: Bound<'_, PyAny>) -> PyResult<()> {
        let batch = datafusion::arrow::record_batch::RecordBatch::from_pyarrow_bound(&batch)?;
        let schema = batch.schema();
        let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]])
            .map_err(df_to_py_err)?;
        self.inner
            .register_temp_table(&name, Arc::new(mem_table))
            .map_err(df_to_py_err)
    }

    fn sql(&self, py: Python<'_>, sql: String) -> PyResult<Vec<Py<PyAny>>> {
        let rt = runtime();
        let batches = rt.block_on(async {
            let df = self.inner.sql(&sql).await.map_err(df_to_py_err)?;
            df.collect().await.map_err(df_to_py_err)
        })?;
        batches
            .iter()
            .map(|batch| Ok(batch.to_pyarrow(py)?.unbind()))
            .collect()
    }
}

pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let this = PyModule::new(py, "datafusion")?;
    this.add_class::<PaimonCatalog>()?;
    this.add_class::<PySQLContext>()?;
    m.add_submodule(&this)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("pypaimon_rust.datafusion", this)?;
    Ok(())
}
