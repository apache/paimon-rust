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

use pyo3::exceptions::{PyNotImplementedError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;

pyo3::create_exception!(
    pypaimon_rust,
    ForkSafetyError,
    PyRuntimeError,
    "Raised when native state inherited across process fork cannot be used safely."
);

pyo3::create_exception!(
    pypaimon_rust,
    CatalogError,
    PyValueError,
    "A catalog failure with code, resource_type, resource_name and message attributes."
);

pyo3::create_exception!(
    pypaimon_rust,
    RowIdExistenceConflict,
    PyRuntimeError,
    "A staged file no longer matches the current row-ID layout."
);

pub fn register_module(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("ForkSafetyError", py.get_type::<ForkSafetyError>())?;
    module.add("CatalogError", py.get_type::<CatalogError>())?;
    module.add(
        "RowIdExistenceConflict",
        py.get_type::<RowIdExistenceConflict>(),
    )
}

/// Preserve the catalog error contract across the FFI without parsing display strings.
pub(crate) fn to_catalog_py_err(error: paimon::Error) -> PyErr {
    use paimon::api::RestError;
    use paimon::Error;

    if error.is_process_fork_unsupported() {
        return to_py_err(error);
    }
    let snapshot_name = match &error {
        Error::SnapshotNotExist { snapshot_id } => Some(snapshot_id.to_string()),
        _ => None,
    };
    let (code, resource_type, resource_name, message) = match &error {
        Error::BranchAlreadyExist { branch_name, .. } => (
            409,
            Some("BRANCH"),
            Some(branch_name.as_str()),
            error.to_string(),
        ),
        Error::BranchNotExist { branch_name, .. } => (
            404,
            Some("BRANCH"),
            Some(branch_name.as_str()),
            error.to_string(),
        ),
        Error::TableNotExist { full_name } => (
            404,
            Some("TABLE"),
            Some(full_name.as_str()),
            error.to_string(),
        ),
        Error::TagNotExist { tag_name } => {
            (404, Some("TAG"), Some(tag_name.as_str()), error.to_string())
        }
        Error::TagAlreadyExist { tag_name } => {
            (409, Some("TAG"), Some(tag_name.as_str()), error.to_string())
        }
        Error::SnapshotNotExist { .. } => (
            404,
            Some("SNAPSHOT"),
            snapshot_name.as_deref(),
            error.to_string(),
        ),
        Error::DataInvalid { message, .. } => (400, None, None, message.clone()),
        Error::RestApi { source } => {
            let (code, resource_type, resource_name, message) = match source {
                RestError::BadRequest { message } => (400, None, None, message),
                RestError::NotAuthorized { message } => (401, None, None, message),
                RestError::Forbidden { message } => (403, None, None, message),
                RestError::NoSuchResource {
                    resource_type,
                    resource_name,
                    message,
                } => (
                    404,
                    resource_type.as_deref(),
                    resource_name.as_deref(),
                    message,
                ),
                RestError::AlreadyExists {
                    resource_type,
                    resource_name,
                    message,
                } => (
                    409,
                    resource_type.as_deref(),
                    resource_name.as_deref(),
                    message,
                ),
                RestError::ServiceFailure { message } => (500, None, None, message),
                RestError::NotImplemented { message } => (501, None, None, message),
                RestError::ServiceUnavailable { message } => (503, None, None, message),
                RestError::Unexpected { code, message } => (*code, None, None, message),
            };
            (code, resource_type, resource_name, message.clone())
        }
        Error::UnexpectedError { .. } | Error::IoUnexpected { .. } => {
            (0, None, None, error.to_string())
        }
        _ => return to_py_err(error),
    };
    Python::attach(|py| {
        let exception = CatalogError::new_err(error.to_string());
        let value = exception.value(py);
        value.setattr("code", code)?;
        value.setattr("resource_type", resource_type)?;
        value.setattr("resource_name", resource_name)?;
        value.setattr("message", message)?;
        Ok::<_, PyErr>(exception)
    })
    .unwrap_or_else(|error| error)
}

pub fn to_py_err(err: paimon::Error) -> PyErr {
    if err.is_process_fork_unsupported() {
        return ForkSafetyError::new_err(err.to_string());
    }
    if let paimon::Error::RowIdExistenceConflict {
        file_name,
        first_row_id,
        row_count,
        bucket,
        current_first_row_id,
        current_row_count,
    } = &err
    {
        return Python::attach(|py| {
            let exception = RowIdExistenceConflict::new_err(err.to_string());
            let value = exception.value(py);
            value.setattr("file_name", file_name)?;
            value.setattr("first_row_id", first_row_id)?;
            value.setattr("row_count", row_count)?;
            value.setattr("bucket", bucket)?;
            value.setattr("current_first_row_id", current_first_row_id)?;
            value.setattr("current_row_count", current_row_count)?;
            Ok::<_, PyErr>(exception)
        })
        .unwrap_or_else(|error| error);
    }
    match err {
        // Unimplemented scan semantics: distinct from malformed input so upper
        // layers can catch NotImplementedError and decide on a fallback.
        paimon::Error::Unsupported { .. } => PyNotImplementedError::new_err(err.to_string()),
        _ => PyValueError::new_err(err.to_string()),
    }
}

pub fn df_to_py_err(err: datafusion::error::DataFusionError) -> PyErr {
    let mut causes: Vec<&(dyn std::error::Error + 'static)> = vec![&err];
    while let Some(cause) = causes.pop() {
        if cause
            .downcast_ref::<paimon::Error>()
            .is_some_and(paimon::Error::is_process_fork_unsupported)
        {
            return ForkSafetyError::new_err(err.to_string());
        }
        if let Some(datafusion::error::DataFusionError::Collection(errors)) =
            cause.downcast_ref::<datafusion::error::DataFusionError>()
        {
            causes.extend(
                errors
                    .iter()
                    .map(|error| error as &(dyn std::error::Error + 'static)),
            );
        } else if let Some(source) = cause.source() {
            causes.push(source);
        }
    }
    PyValueError::new_err(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_id_existence_error_preserves_structured_layout() {
        Python::attach(|py| {
            let error = to_py_err(paimon::Error::RowIdExistenceConflict {
                file_name: "opaque '(4): file.parquet".into(),
                first_row_id: Some(4),
                row_count: 8,
                bucket: 3,
                current_first_row_id: None,
                current_row_count: Some(9),
            });
            assert!(error.is_instance_of::<RowIdExistenceConflict>(py));
            let value = error.value(py);
            assert_eq!(
                value
                    .getattr("file_name")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "opaque '(4): file.parquet"
            );
            assert_eq!(
                value
                    .getattr("first_row_id")
                    .unwrap()
                    .extract::<i64>()
                    .unwrap(),
                4
            );
            assert_eq!(
                value
                    .getattr("row_count")
                    .unwrap()
                    .extract::<i64>()
                    .unwrap(),
                8
            );
            assert_eq!(
                value.getattr("bucket").unwrap().extract::<i32>().unwrap(),
                3
            );
            assert!(value.getattr("current_first_row_id").unwrap().is_none());
            assert_eq!(
                value
                    .getattr("current_row_count")
                    .unwrap()
                    .extract::<i64>()
                    .unwrap(),
                9
            );
        });
    }

    #[test]
    fn row_id_existence_error_survives_python_worker_serialization() {
        Python::attach(|py| {
            let modules = py
                .import("sys")
                .unwrap()
                .getattr("modules")
                .unwrap()
                .cast_into::<pyo3::types::PyDict>()
                .unwrap();
            let previous = modules.get_item("pypaimon_rust").unwrap();
            let module = PyModule::new(py, "pypaimon_rust").unwrap();
            register_module(py, &module).unwrap();
            modules.set_item("pypaimon_rust", &module).unwrap();
            let result = (|| -> PyResult<()> {
                let error = to_py_err(paimon::Error::RowIdExistenceConflict {
                    file_name: "worker:staged.parquet".into(),
                    first_row_id: Some(4),
                    row_count: 8,
                    bucket: 3,
                    current_first_row_id: None,
                    current_row_count: Some(9),
                });
                let pickle = py.import("pickle")?;
                let bytes = pickle.call_method1("dumps", (error.value(py),))?;
                let restored = pickle.call_method1("loads", (bytes,))?;
                assert!(restored.is_instance_of::<RowIdExistenceConflict>());
                assert_eq!(
                    restored.getattr("file_name")?.extract::<String>()?,
                    "worker:staged.parquet"
                );
                assert_eq!(restored.getattr("first_row_id")?.extract::<i64>()?, 4);
                assert_eq!(restored.getattr("row_count")?.extract::<i64>()?, 8);
                assert_eq!(restored.getattr("bucket")?.extract::<i32>()?, 3);
                assert!(restored.getattr("current_first_row_id")?.is_none());
                assert_eq!(restored.getattr("current_row_count")?.extract::<i64>()?, 9);
                Ok(())
            })();
            match previous {
                Some(previous) => modules.set_item("pypaimon_rust", previous).unwrap(),
                None => modules.del_item("pypaimon_rust").unwrap(),
            }
            result.unwrap();
        });
    }

    #[test]
    fn unknown_commit_outcome_does_not_expose_a_rewritable_conflict() {
        Python::attach(|py| {
            let error = to_py_err(paimon::Error::UnexpectedError {
                message: "Commit outcome may be unknown; retain prepared files".into(),
                source: Some(Box::new(paimon::Error::RowIdExistenceConflict {
                    file_name: "staged.parquet".into(),
                    first_row_id: Some(0),
                    row_count: 10,
                    bucket: 0,
                    current_first_row_id: None,
                    current_row_count: None,
                })),
            });
            assert!(error.is_instance_of::<PyValueError>(py));
            assert!(!error.is_instance_of::<RowIdExistenceConflict>(py));
        });
    }

    #[test]
    fn process_fork_error_has_distinct_python_type() {
        Python::attach(|py| {
            let error = to_py_err(paimon::Error::ProcessForkUnsupported {
                message: "fork is not supported".to_string(),
            });
            assert!(error.is_instance_of::<ForkSafetyError>(py));
        });
    }

    #[test]
    fn oss_cpp_fork_error_has_distinct_python_type() {
        Python::attach(|py| {
            let error = paimon::Error::ProcessForkUnsupported {
                message: "OSS C++ SDK cannot be used after fork; use spawn workers".to_string(),
            };
            assert!(to_py_err(error).is_instance_of::<ForkSafetyError>(py));

            let error = datafusion::error::DataFusionError::External(Box::new(
                paimon::Error::ProcessForkUnsupported {
                    message: "OSS C++ SDK cannot be used after fork; use spawn workers".to_string(),
                },
            ))
            .context("SQL planning failed");
            assert!(df_to_py_err(error).is_instance_of::<ForkSafetyError>(py));
        });
    }

    #[test]
    fn wrapped_process_fork_error_has_distinct_python_type() {
        Python::attach(|py| {
            let error = to_py_err(paimon::Error::UnexpectedError {
                message: "read failed".to_string(),
                source: Some(Box::new(paimon::Error::ProcessForkUnsupported {
                    message: "fork is not supported".to_string(),
                })),
            });
            assert!(error.is_instance_of::<ForkSafetyError>(py));
        });
    }

    #[test]
    fn datafusion_fork_error_has_distinct_python_type() {
        Python::attach(|py| {
            let error = datafusion::error::DataFusionError::External(Box::new(
                paimon::Error::ProcessForkUnsupported {
                    message: "fork is not supported".to_string(),
                },
            ));
            assert!(df_to_py_err(error).is_instance_of::<ForkSafetyError>(py));
        });
    }

    #[test]
    fn datafusion_context_preserves_fork_error_type() {
        Python::attach(|py| {
            let error = datafusion::error::DataFusionError::External(Box::new(
                paimon::Error::ProcessForkUnsupported {
                    message: "fork is not supported".to_string(),
                },
            ))
            .context("SQL collection failed");
            assert!(df_to_py_err(error).is_instance_of::<ForkSafetyError>(py));
        });
    }

    #[test]
    fn datafusion_collection_preserves_fork_error_type() {
        Python::attach(|py| {
            let error = datafusion::error::DataFusionError::Collection(vec![
                datafusion::error::DataFusionError::Plan("invalid SQL".to_string()),
                datafusion::error::DataFusionError::External(Box::new(
                    paimon::Error::ProcessForkUnsupported {
                        message: "fork is not supported".to_string(),
                    },
                )),
            ])
            .context("SQL planning failed");
            assert!(df_to_py_err(error).is_instance_of::<ForkSafetyError>(py));
        });
    }

    #[test]
    fn unrelated_datafusion_error_remains_value_error() {
        Python::attach(|py| {
            let error = datafusion::error::DataFusionError::Plan("invalid SQL".to_string());
            assert!(df_to_py_err(error).is_instance_of::<PyValueError>(py));
        });
    }
}
