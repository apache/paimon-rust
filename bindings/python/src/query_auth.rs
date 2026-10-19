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

//! Python callable adaptation for core table query authorization.

use async_trait::async_trait;
use paimon::api::AuthTableQueryResponse;
use paimon::table::TableQueryAuth;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

pub(crate) struct PythonTableQueryAuth {
    callback: Py<PyAny>,
}

impl std::fmt::Debug for PythonTableQueryAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PythonTableQueryAuth")
    }
}

impl PythonTableQueryAuth {
    pub(crate) fn new(callback: Py<PyAny>) -> PyResult<Self> {
        Python::attach(|py| {
            if !callback.bind(py).is_callable() {
                return Err(PyTypeError::new_err("query_auth must be callable"));
            }
            Ok(Self { callback })
        })
    }
}

#[async_trait]
impl TableQueryAuth for PythonTableQueryAuth {
    async fn auth(
        &self,
        select: Option<Vec<String>>,
    ) -> paimon::Result<Option<AuthTableQueryResponse>> {
        tokio::task::block_in_place(|| {
            Python::attach(|py| {
                let result = self.callback.bind(py).call1((select,))?;
                if result.is_none() {
                    return Ok(None);
                }
                Ok(Some(AuthTableQueryResponse {
                    filter: result.getattr("filter")?.extract()?,
                    column_masking: result.getattr("column_masking")?.extract()?,
                }))
            })
        })
        .map_err(crate::blob_uri_reader::callback_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    fn authorizer(source: &str) -> PythonTableQueryAuth {
        Python::attach(|py| {
            PythonTableQueryAuth::new(
                py.eval(&CString::new(source).unwrap(), None, None)
                    .unwrap()
                    .unbind(),
            )
            .unwrap()
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn python_authorizer_passes_selection_and_nullable_grant() {
        let auth = authorizer(
            "lambda select: None if select is None else __import__('types').SimpleNamespace(filter=select, column_masking={'value': 'mask'})",
        );
        assert!(auth.auth(None).await.unwrap().is_none());
        let response = auth.auth(Some(vec!["id".into()])).await.unwrap().unwrap();
        assert_eq!(response.filter, Some(vec!["id".into()]));
        assert_eq!(response.column_masking.unwrap()["value"], "mask");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn python_authorizer_preserves_original_callback_exception() {
        let auth =
            authorizer("lambda select: (_ for _ in ()).throw(PermissionError('query denied'))");
        let error = crate::blob_uri_reader::to_callback_error(auth.auth(None).await.unwrap_err());
        Python::attach(|py| {
            assert!(error.is_instance_of::<pyo3::exceptions::PyPermissionError>(py));
            assert_eq!(
                error.value(py).str().unwrap().to_str().unwrap(),
                "query denied"
            );
        });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn python_authorizer_rejects_malformed_response() {
        let auth = authorizer(
            "lambda select: __import__('types').SimpleNamespace(filter=7, column_masking=None)",
        );
        assert!(auth.auth(None).await.is_err());
        Python::attach(|py| {
            assert!(
                PythonTableQueryAuth::new(7i32.into_pyobject(py).unwrap().into_any().unbind())
                    .is_err()
            );
        });
    }
}
