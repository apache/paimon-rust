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

use paimon::table::{SearchTask, SearchTaskExecutor, SearchTaskResult};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};
use std::sync::Mutex;

pub(crate) struct PythonSearchTaskExecutor {
    callback: Py<PyAny>,
    error: Mutex<Option<PyErr>>,
}

impl PythonSearchTaskExecutor {
    pub(crate) fn new(callback: Py<PyAny>) -> Self {
        Self {
            callback,
            error: Mutex::new(None),
        }
    }
    pub(crate) fn take_error(&self) -> Option<PyErr> {
        self.error.lock().unwrap().take()
    }
}

#[async_trait::async_trait]
impl SearchTaskExecutor for PythonSearchTaskExecutor {
    async fn execute(&self, tasks: Vec<SearchTask>) -> paimon::Result<Vec<SearchTaskResult>> {
        let (context, bytes) = SearchTask::serialize_batch(&tasks)?;
        let callback = Python::attach(|py| self.callback.clone_ref(py));
        // A callback may wait for remote workers. Yield this route's future so
        // the hybrid driver can poll and dispatch its other routes as well.
        let result: PyResult<Vec<SearchTaskResult>> = tokio::task::spawn_blocking(move || {
            Python::attach(|py| {
                let input = PyList::new(py, bytes.iter().map(|task| PyBytes::new(py, task)))?;
                let output = callback
                    .bind(py)
                    .call1((PyBytes::new(py, &context), input))?;
                let result = output
                    .try_iter()?
                    .map(|value| {
                        let bytes: Vec<u8> = value?.extract()?;
                        SearchTaskResult::deserialize(&bytes).map_err(crate::error::to_py_err)
                    })
                    .collect::<PyResult<Vec<_>>>();
                if result.is_err() {
                    // A retained scheduler generator still owns pending workers.
                    // Run its finally block, preserving the primary Python error.
                    if let Ok(close) = output.getattr("close") {
                        let _ = close.call0();
                    }
                }
                result
            })
        })
        .await
        .map_err(|error| paimon::Error::UnexpectedError {
            message: "Search task callback join failed".into(),
            source: Some(Box::new(error)),
        })?;
        result.map_err(|error| {
            let mut first = self.error.lock().unwrap();
            if first.is_none() {
                *first = Some(error);
            }
            paimon::Error::UnexpectedError {
                message: "Search task executor failed".into(),
                source: None,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::types::PyDict;

    #[tokio::test(flavor = "current_thread")]
    async fn scheduled_python_callbacks_yield_for_other_routes() {
        let callback = Python::attach(|py| {
            let globals = PyDict::new(py);
            py.run(
                c"
import threading
barrier = threading.Barrier(2)
def execute(context, tasks):
    barrier.wait(timeout=5)
    return []
",
                Some(&globals),
                None,
            )
            .unwrap();
            globals.get_item("execute").unwrap().unwrap().unbind()
        });
        let executor = PythonSearchTaskExecutor::new(callback);
        // Both routes must dispatch before either finishes, even with only one
        // async runtime thread. A blocking inline callback breaks the barrier.
        let (left, right) =
            futures::join!(executor.execute(Vec::new()), executor.execute(Vec::new()));
        assert!(left.unwrap().is_empty());
        assert!(right.unwrap().is_empty());
        assert!(executor.take_error().is_none());
    }

    #[tokio::test]
    async fn scheduled_python_executor_preserves_cancellation_exception_identity() {
        let (callback, failure) = Python::attach(|py| {
            let globals = PyDict::new(py);
            py.run(
                c"
failure = KeyboardInterrupt('cancelled search')
def execute(context, tasks):
    raise failure
",
                Some(&globals),
                None,
            )
            .unwrap();
            (
                globals.get_item("execute").unwrap().unwrap().unbind(),
                globals.get_item("failure").unwrap().unwrap().unbind(),
            )
        });
        let executor = PythonSearchTaskExecutor::new(callback);
        assert!(executor.execute(Vec::new()).await.is_err());
        let error = executor.take_error().unwrap();
        Python::attach(|py| assert!(error.value(py).is(failure.bind(py))));
    }

    #[tokio::test]
    async fn scheduled_python_executor_closes_result_iterator_after_decode_error() {
        let (callback, globals) = Python::attach(|py| {
            let globals = PyDict::new(py);
            py.run(
                c"
closed = []
def execute(context, tasks):
    try:
        yield b'invalid task result'
    finally:
        closed.append(True)
",
                Some(&globals),
                None,
            )
            .unwrap();
            (
                globals.get_item("execute").unwrap().unwrap().unbind(),
                globals.unbind(),
            )
        });
        let executor = PythonSearchTaskExecutor::new(callback);
        assert!(executor.execute(Vec::new()).await.is_err());
        Python::attach(|py| {
            let closed: Vec<bool> = globals
                .bind(py)
                .get_item("closed")
                .unwrap()
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(closed, vec![true]);
        });
    }
}
