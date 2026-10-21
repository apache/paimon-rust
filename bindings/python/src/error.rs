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

pub fn register_module(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("ForkSafetyError", py.get_type::<ForkSafetyError>())?;
    module.add("CatalogError", py.get_type::<CatalogError>())
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
