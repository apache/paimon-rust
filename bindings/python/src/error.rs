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

pub fn register_module(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("ForkSafetyError", py.get_type::<ForkSafetyError>())
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
}
