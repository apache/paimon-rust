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

//! Python URI objects and exception transport; source reuse and copy semantics
//! are implemented by Rust core.

use bytes::Bytes;
use paimon::io::{UriInputStream, UriReader, UriReaderFactory};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

struct PythonCallbackError(PyErr);

impl std::fmt::Debug for PythonCallbackError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PythonCallbackError")
    }
}

impl std::fmt::Display for PythonCallbackError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Python Blob callback failed")
    }
}
impl std::error::Error for PythonCallbackError {}

pub(crate) fn callback_error(error: PyErr) -> paimon::Error {
    paimon::Error::UnexpectedError {
        message: "Python Blob callback failed".into(),
        source: Some(Box::new(PythonCallbackError(error))),
    }
}

pub(crate) fn to_callback_error(error: paimon::Error) -> PyErr {
    let mut cause: &(dyn std::error::Error + 'static) = &error;
    loop {
        if let Some(original) = cause.downcast_ref::<PythonCallbackError>() {
            return Python::attach(|py| original.0.clone_ref(py));
        }
        // Paimon UnexpectedError deliberately omits its source from the
        // standard chain. Inspect that explicit source without stringifying
        // Python errors (which can contain URI credentials).
        let next = match cause.downcast_ref::<paimon::Error>() {
            Some(
                paimon::Error::UnexpectedError { source, .. }
                | paimon::Error::DataInvalid { source, .. },
            ) => source
                .as_deref()
                .map(|source| source as &(dyn std::error::Error + 'static)),
            _ => cause.source(),
        };
        match next {
            Some(source) => cause = source,
            None => break,
        }
    }
    crate::error::to_py_err(error)
}

pub(crate) struct PythonUriReaderFactory {
    factory: Py<PyAny>,
    // Preserve object identity across interleaved physical Blob writers without
    // retaining readers after core releases them. URI caching belongs to the factory.
    readers: Mutex<HashMap<usize, Weak<PythonUriReader>>>,
}

impl PythonUriReaderFactory {
    pub(crate) fn new(factory: Py<PyAny>, py: Python<'_>) -> PyResult<Self> {
        if !factory.bind(py).getattr("create")?.is_callable() {
            return Err(PyTypeError::new_err(
                "URI reader factory must provide callable create",
            ));
        }
        Ok(Self {
            factory,
            readers: Mutex::new(HashMap::new()),
        })
    }
}

impl UriReaderFactory for PythonUriReaderFactory {
    fn create(&self, uri: &str) -> paimon::Result<Arc<dyn UriReader>> {
        tokio::task::block_in_place(|| {
            Python::attach(|py| {
                let reader = self
                    .factory
                    .bind(py)
                    .call_method1("create", (uri,))?
                    .unbind();
                if !reader.bind(py).getattr("new_input_stream")?.is_callable() {
                    return Err(PyTypeError::new_err(
                        "URI reader must provide callable new_input_stream",
                    ));
                }
                let identity = reader.as_ptr() as usize;
                let mut cached = self.readers.lock().unwrap();
                if let Some(previous) = cached.get(&identity).and_then(Weak::upgrade) {
                    return Ok(previous as Arc<dyn UriReader>);
                }
                cached.retain(|_, reader| reader.strong_count() != 0);
                let reader = Arc::new(PythonUriReader { reader });
                cached.insert(identity, Arc::downgrade(&reader));
                Ok(reader as Arc<dyn UriReader>)
            })
        })
        .map_err(callback_error)
    }
}

struct PythonUriReader {
    reader: Py<PyAny>,
}

#[async_trait::async_trait]
impl UriReader for PythonUriReader {
    async fn new_input_stream(&self, uri: &str) -> paimon::Result<Box<dyn UriInputStream>> {
        tokio::task::block_in_place(|| {
            Python::attach(|py| {
                let stream = self
                    .reader
                    .bind(py)
                    .call_method1("new_input_stream", (uri,))?
                    .unbind();
                Ok(Box::new(PythonUriInputStream {
                    stream: Some(stream),
                }) as Box<dyn UriInputStream>)
            })
        })
        .map_err(callback_error)
    }
}

struct PythonUriInputStream {
    stream: Option<Py<PyAny>>,
}

#[async_trait::async_trait]
impl UriInputStream for PythonUriInputStream {
    async fn read(&mut self, length: usize) -> paimon::Result<Bytes> {
        tokio::task::block_in_place(|| {
            Python::attach(|py| {
                let bytes = self
                    .stream
                    .as_ref()
                    .expect("open stream")
                    .bind(py)
                    .call_method1("read", (length,))?
                    .extract::<Vec<u8>>()?;
                Ok(Bytes::from(bytes))
            })
        })
        .map_err(callback_error)
    }

    async fn seek(&mut self, offset: u64) -> paimon::Result<()> {
        tokio::task::block_in_place(|| {
            Python::attach(|py| {
                self.stream
                    .as_ref()
                    .expect("open stream")
                    .bind(py)
                    .call_method1("seek", (offset,))?;
                Ok(())
            })
        })
        .map_err(callback_error)
    }

    async fn close(&mut self) -> paimon::Result<()> {
        match self.stream.take() {
            Some(stream) => tokio::task::block_in_place(|| {
                Python::attach(|py| {
                    stream.bind(py).call_method0("close")?;
                    Ok(())
                })
            })
            .map_err(callback_error),
            None => Ok(()),
        }
    }
}

impl Drop for PythonUriInputStream {
    fn drop(&mut self) {
        // Async close is explicit in core. On cancellation, still release a
        // Python stream without replacing the triggering error.
        if let Some(stream) = self.stream.take() {
            Python::attach(|py| {
                let _ = stream.bind(py).call_method0("close");
            });
        }
    }
}
