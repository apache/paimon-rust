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

use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use datafusion::catalog::CatalogProvider;
use datafusion::logical_expr::{Signature, TypeSignature, Volatility};
use datafusion_ffi::catalog_provider::FFI_CatalogProvider;
use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use paimon::catalog::Identifier;
use paimon::{Catalog, CatalogFactory, Options};
use paimon_datafusion::{PaimonCatalogProvider, SQLContext};
use pyo3::exceptions::{PyRuntimeWarning, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

use crate::blob::PyBlobReaderRegistry;
use crate::error::{df_to_py_err, to_py_err};
use crate::table::PyTable;
use crate::udf::{build_python_scalar_udf, udf, PyPythonScalarUDFObject};
use paimon_datafusion::runtime::runtime;

fn build_paimon_catalog(catalog_options: HashMap<String, String>) -> PyResult<Arc<dyn Catalog>> {
    let rt = runtime();
    rt.block_on(async {
        let options = Options::from_map(catalog_options);
        let catalog = CatalogFactory::create(options).await.map_err(to_py_err)?;
        Ok::<_, PyErr>(catalog)
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
    catalog: Arc<dyn Catalog>,
    provider: Arc<PaimonCatalogProvider>,
}

#[pymethods]
impl PaimonCatalog {
    /// Create a Paimon catalog that can be registered into a DataFusion session.
    #[new]
    fn new(catalog_options: HashMap<String, String>) -> PyResult<Self> {
        let catalog = build_paimon_catalog(catalog_options)?;
        let provider = Arc::new(PaimonCatalogProvider::new(
            None,
            Arc::clone(&catalog),
            Default::default(),
            Default::default(),
            None,
        ));
        Ok(Self { catalog, provider })
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

    /// List all databases in this catalog.
    fn list_databases(&self) -> PyResult<Vec<String>> {
        runtime()
            .block_on(self.catalog.list_databases())
            .map_err(to_py_err)
    }

    /// List all tables in the given database.
    fn list_tables(&self, database_name: &str) -> PyResult<Vec<String>> {
        runtime()
            .block_on(self.catalog.list_tables(database_name))
            .map_err(to_py_err)
    }

    /// Get a table handle by `"db.table"` identifier.
    fn get_table(&self, identifier: &str) -> PyResult<PyTable> {
        let parts: Vec<&str> = identifier.splitn(2, '.').collect();
        if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
            return Err(PyValueError::new_err(format!(
                "expected identifier in 'db.table' format, got '{identifier}'"
            )));
        }
        let id = Identifier::new(parts[0], parts[1]);
        let table = runtime()
            .block_on(self.catalog.get_table(&id))
            .map_err(to_py_err)?;
        Ok(PyTable::new(Arc::new(table)))
    }
}

/// A SQL context that supports registering multiple Paimon catalogs and executing SQL.
#[pyclass(name = "SQLContext")]
pub struct PySQLContext {
    inner: SQLContext,
}

impl PySQLContext {
    fn vector_float32_type() -> ArrowDataType {
        ArrowDataType::List(Arc::new(ArrowField::new(
            "item",
            ArrowDataType::Float32,
            true,
        )))
    }

    fn register_multimodal_builtins(&self, py: Python<'_>) -> PyResult<()> {
        let functions = py.import("pypaimon_rust.functions")?;
        let blob_reader_registry = Py::new(
            py,
            PyBlobReaderRegistry::new(self.inner.blob_reader_registry()),
        )?;

        let media_info_blob_reader_registry = blob_reader_registry.clone_ref(py);
        let media_info_func = functions
            .getattr("_make_media_info")?
            .call1((media_info_blob_reader_registry,))?
            .unbind();
        let media_info_udf = build_python_scalar_udf(
            "media_info".to_string(),
            media_info_func,
            ArrowDataType::Utf8,
            Signature::exact(vec![ArrowDataType::Binary], Volatility::Volatile),
        );
        self.inner.ctx().register_udf(media_info_udf);

        let thumbnail_blob_reader_registry = blob_reader_registry.clone_ref(py);
        let thumbnail_func = functions
            .getattr("_make_media_thumbnail")?
            .call1(("PNG", thumbnail_blob_reader_registry))?
            .unbind();
        let thumbnail_signature = Signature::one_of(
            vec![
                TypeSignature::Exact(vec![ArrowDataType::Binary]),
                TypeSignature::Exact(vec![
                    ArrowDataType::Binary,
                    ArrowDataType::Int32,
                    ArrowDataType::Int32,
                ]),
                TypeSignature::Exact(vec![
                    ArrowDataType::Binary,
                    ArrowDataType::Int64,
                    ArrowDataType::Int64,
                ]),
            ],
            Volatility::Volatile,
        );
        let thumbnail_udf = build_python_scalar_udf(
            "media_thumbnail".to_string(),
            thumbnail_func,
            ArrowDataType::Binary,
            thumbnail_signature,
        );
        self.inner.ctx().register_udf(thumbnail_udf);

        let snapshot_blob_reader_registry = blob_reader_registry.clone_ref(py);
        let snapshot_func = functions
            .getattr("_make_video_snapshot")?
            .call1(("PNG", snapshot_blob_reader_registry))?
            .unbind();
        let snapshot_signature = Signature::one_of(
            vec![
                TypeSignature::Exact(vec![ArrowDataType::Binary]),
                TypeSignature::Exact(vec![ArrowDataType::Binary, ArrowDataType::Int32]),
                TypeSignature::Exact(vec![ArrowDataType::Binary, ArrowDataType::Int64]),
            ],
            Volatility::Volatile,
        );
        let snapshot_udf = build_python_scalar_udf(
            "video_snapshot".to_string(),
            snapshot_func,
            ArrowDataType::Binary,
            snapshot_signature,
        );
        self.inner.ctx().register_udf(snapshot_udf);

        let frame_func = functions
            .getattr("_make_video_frame")?
            .call1(("PNG", blob_reader_registry))?
            .unbind();
        let frame_signature = Signature::one_of(
            vec![
                TypeSignature::Exact(vec![ArrowDataType::Binary, ArrowDataType::Int32]),
                TypeSignature::Exact(vec![ArrowDataType::Binary, ArrowDataType::Int64]),
            ],
            Volatility::Volatile,
        );
        let frame_udf = build_python_scalar_udf(
            "video_frame".to_string(),
            frame_func,
            ArrowDataType::Binary,
            frame_signature,
        );
        self.inner.ctx().register_udf(frame_udf);

        let vector_from_json_func = functions
            .getattr("_make_vector_from_json")?
            .call0()?
            .unbind();
        let vector_from_json_signature = Signature::one_of(
            vec![
                TypeSignature::Exact(vec![ArrowDataType::Utf8]),
                TypeSignature::Exact(vec![ArrowDataType::LargeUtf8]),
            ],
            Volatility::Immutable,
        );
        let vector_from_json_udf = build_python_scalar_udf(
            "vector_from_json".to_string(),
            vector_from_json_func,
            Self::vector_float32_type(),
            vector_from_json_signature,
        );
        self.inner.ctx().register_udf(vector_from_json_udf);

        let vector_to_json_func = functions.getattr("_make_vector_to_json")?.call0()?.unbind();
        let vector_to_json_udf = build_python_scalar_udf(
            "vector_to_json".to_string(),
            vector_to_json_func,
            ArrowDataType::Utf8,
            Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        );
        self.inner.ctx().register_udf(vector_to_json_udf);
        Ok(())
    }

    fn warn_multimodal_builtin_registration_failure(py: Python<'_>, err: PyErr) {
        if let Ok(warnings) = py.import("warnings") {
            let category = py.get_type::<PyRuntimeWarning>();
            let _ = warnings.call_method1(
                "warn",
                (
                    format!("multimodal built-ins could not be registered: {err}"),
                    category,
                ),
            );
        }
    }
}

#[pymethods]
impl PySQLContext {
    #[new]
    fn new(py: Python<'_>) -> PyResult<Self> {
        let ctx = Self {
            inner: SQLContext::new(),
        };
        if let Err(err) = ctx.register_multimodal_builtins(py) {
            Self::warn_multimodal_builtin_registration_failure(py, err);
        }
        Ok(ctx)
    }

    /// Registers a Paimon catalog under the given name.
    ///
    /// `default_database`: omitted / `None` → use `"default"` (back-compat);
    /// `""` → skip default-db init (for principals without DESCRIBE on `default`);
    /// `"name"` → use `name`.
    #[pyo3(signature = (catalog_name, catalog_options, default_database=None))]
    fn register_catalog(
        &mut self,
        py: Python<'_>,
        catalog_name: String,
        catalog_options: HashMap<String, String>,
        default_database: Option<String>,
    ) -> PyResult<()> {
        let rt = runtime();
        py.detach(|| {
            rt.block_on(async {
                let options = Options::from_map(catalog_options);
                let catalog = CatalogFactory::create(options).await.map_err(to_py_err)?;
                let default_db: Option<String> = match default_database {
                    None => Some("default".to_string()),
                    Some(s) if s.is_empty() => None,
                    Some(s) => Some(s),
                };
                self.inner
                    .register_catalog_with_default_db(catalog_name, catalog, default_db.as_deref())
                    .await
                    .map_err(df_to_py_err)
            })
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

    fn register_udf(&self, udf: &PyPythonScalarUDFObject) -> PyResult<()> {
        self.inner.ctx().register_udf(udf.datafusion_udf());
        Ok(())
    }

    fn sql(&self, py: Python<'_>, sql: String) -> PyResult<Vec<Py<PyAny>>> {
        let rt = runtime();
        let batches = py.detach(|| {
            rt.block_on(async {
                let df = self.inner.sql(&sql).await.map_err(df_to_py_err)?;
                df.collect().await.map_err(df_to_py_err)
            })
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
    this.add_class::<crate::table::PyTable>()?;
    this.add_class::<crate::read::PyReadBuilder>()?;
    this.add_class::<crate::read::PyTableScan>()?;
    this.add_class::<crate::read::PyPlan>()?;
    this.add_class::<crate::read::PyTableRead>()?;
    this.add_class::<crate::read::PySplit>()?;
    this.add_class::<crate::schema::PyTableSchema>()?;
    this.add_class::<crate::schema::PyDataField>()?;
    this.add_class::<PyPythonScalarUDFObject>()?;
    this.add_class::<PySQLContext>()?;
    this.add_class::<crate::write::PyWriteBuilder>()?;
    this.add_class::<crate::write::PyTableWrite>()?;
    this.add_class::<crate::write::PyTableCommit>()?;
    this.add_class::<crate::write::PyCommitMessage>()?;
    this.add_function(wrap_pyfunction!(udf, &this)?)?;
    m.add_submodule(&this)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("pypaimon_rust.datafusion", this)?;
    Ok(())
}
