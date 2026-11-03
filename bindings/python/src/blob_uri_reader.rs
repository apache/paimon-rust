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
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::{PyTraverseError, PyVisit};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

struct PythonCallbackError(PyErr);

impl std::fmt::Debug for PythonCallbackError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PythonCallbackError")
    }
}

impl std::fmt::Display for PythonCallbackError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Python callback failed")
    }
}
impl std::error::Error for PythonCallbackError {}

pub(crate) fn callback_error(error: PyErr) -> paimon::Error {
    paimon::Error::UnexpectedError {
        message: "Python callback failed".into(),
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
            // std::io::Error::source skips its custom error itself. Inspect
            // that value too, since positional index I/O wraps Paimon errors.
            _ => match cause.downcast_ref::<std::io::Error>() {
                Some(error) => error
                    .get_ref()
                    .map(|error| error as &(dyn std::error::Error + 'static))
                    .or_else(|| cause.source()),
                None => cause.source(),
            },
        };
        match next {
            Some(source) => cause = source,
            None => break,
        }
    }
    crate::error::to_py_err(error)
}

struct PythonUriReference {
    value: Mutex<Option<Py<PyAny>>>,
    stream: bool,
}

impl PythonUriReference {
    fn new(
        value: Py<PyAny>,
        stream: bool,
        references: Option<&Arc<PythonUriReferences>>,
    ) -> Arc<Self> {
        let reference = Arc::new(Self {
            value: Mutex::new(Some(value)),
            stream,
        });
        if let Some(references) = references {
            references.register(reference.clone());
        }
        reference
    }

    fn clone_ref(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.value
            .lock()
            .unwrap()
            .as_ref()
            .map(|value| value.clone_ref(py))
            .ok_or_else(|| PyRuntimeError::new_err("URI callback has been cleared or closed"))
    }

    fn take(&self) -> Option<Py<PyAny>> {
        self.value.lock().unwrap().take()
    }

    fn close(&self) -> PyResult<()> {
        match self.take() {
            Some(stream) => tokio::task::block_in_place(move || {
                Python::attach(move |py| {
                    stream.bind(py).call_method0("close")?;
                    Ok(())
                })
            }),
            None => Ok(()),
        }
    }
}

/// Expose every Python reference retained by a writer's URI adapters to GC.
/// The registry owns slots strongly, so traversal never upgrades and drops the
/// last owner of a Python reference while attaching to Python is forbidden.
#[derive(Default)]
pub(crate) struct PythonUriReferences {
    references: Mutex<Vec<Arc<PythonUriReference>>>,
    prune_at: AtomicUsize,
}

impl PythonUriReferences {
    fn register(&self, reference: Arc<PythonUriReference>) {
        let prune = {
            let mut references = self.references.lock().unwrap();
            references.push(reference);
            references.len() >= self.prune_at.load(Ordering::Relaxed)
        };
        if prune {
            self.prune();
        }
    }

    pub(crate) fn traverse(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        for reference in self.references.lock().unwrap().iter() {
            // Each slot owns one Py reference, including when several slots
            // happen to point at the same Python object.
            visit.call(&*reference.value.lock().unwrap())?;
        }
        Ok(())
    }

    pub(crate) fn prune(&self) {
        let retired = {
            let mut references = self.references.lock().unwrap();
            let (retained, retired): (Vec<_>, Vec<_>) = std::mem::take(&mut *references)
                .into_iter()
                .partition(|reference| Arc::strong_count(reference) > 1);
            // Bound retired readers/streams within one large write without
            // rescanning every live partition's slots for each new reference.
            self.prune_at.store(
                retained.len().saturating_mul(2).saturating_add(16),
                Ordering::Relaxed,
            );
            *references = retained;
            retired
        };
        // Python finalizers may re-enter Native or the registry itself.
        drop(retired);
    }

    pub(crate) fn clear(&self) {
        let references = std::mem::take(&mut *self.references.lock().unwrap());
        // Take every slot before any Python finalizer or stream close runs.
        // A core close/Drop racing this clear shares the same take operation.
        let values = references
            .iter()
            .filter_map(|reference| reference.take().map(|value| (reference.stream, value)))
            .collect::<Vec<_>>();
        drop(references);
        // Cancellation may run on a runtime thread. Close callbacks and Python
        // finalizers can call Native again, just like ordinary source callbacks.
        tokio::task::block_in_place(move || {
            Python::attach(move |py| {
                for (stream, value) in values {
                    if stream {
                        // Match the cancellation Drop path: cleanup errors must
                        // not replace the error which caused the writer to clear.
                        let _ = value.bind(py).call_method0("close");
                    }
                    // Drop Python refs inside the suspended runtime context.
                    drop(value);
                }
            });
        });
    }
}

pub(crate) struct PythonUriReaderFactory {
    factory: Arc<PythonUriReference>,
    references: Option<Arc<PythonUriReferences>>,
    // Preserve object identity across interleaved physical Blob writers without
    // retaining readers after core releases them. URI caching belongs to the factory.
    readers: Mutex<HashMap<usize, Weak<PythonUriReader>>>,
}

pub(crate) fn optional_uri_reader_factory(
    factory: Option<Py<PyAny>>,
    py: Python<'_>,
) -> PyResult<Option<Arc<dyn UriReaderFactory>>> {
    factory
        .map(|factory| {
            PythonUriReaderFactory::new(factory, py)
                .map(|factory| Arc::new(factory) as Arc<dyn UriReaderFactory>)
        })
        .transpose()
}

impl PythonUriReaderFactory {
    pub(crate) fn new(factory: Py<PyAny>, py: Python<'_>) -> PyResult<Self> {
        Self::with_references(factory, py, None)
    }

    pub(crate) fn tracked(
        factory: Py<PyAny>,
        py: Python<'_>,
        references: Arc<PythonUriReferences>,
    ) -> PyResult<Self> {
        Self::with_references(factory, py, Some(references))
    }

    fn with_references(
        factory: Py<PyAny>,
        py: Python<'_>,
        references: Option<Arc<PythonUriReferences>>,
    ) -> PyResult<Self> {
        if !factory.bind(py).getattr("create")?.is_callable() {
            return Err(PyTypeError::new_err(
                "URI reader factory must provide callable create",
            ));
        }
        Ok(Self {
            factory: PythonUriReference::new(factory, false, references.as_ref()),
            references,
            readers: Mutex::new(HashMap::new()),
        })
    }
}

impl UriReaderFactory for PythonUriReaderFactory {
    fn supports_uri(&self, uri: &str) -> paimon::Result<bool> {
        tokio::task::block_in_place(|| {
            Python::attach(|py| {
                let factory = self.factory.clone_ref(py)?;
                let factory = factory.bind(py);
                if factory.hasattr("_supports_uri")? {
                    factory.call_method1("_supports_uri", (uri,))?.extract()
                } else {
                    Ok(true)
                }
            })
        })
        .map_err(callback_error)
    }

    fn create(&self, uri: &str) -> paimon::Result<Arc<dyn UriReader>> {
        tokio::task::block_in_place(|| {
            Python::attach(|py| {
                let factory = self.factory.clone_ref(py)?;
                let reader = factory.bind(py).call_method1("create", (uri,))?.unbind();
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
                let reader = Arc::new(PythonUriReader {
                    reader: PythonUriReference::new(reader, false, None),
                    references: self.references.clone(),
                });
                cached.insert(identity, Arc::downgrade(&reader));
                // Registration may prune retired slots and run Python
                // finalizers. Release the identity cache before that happens.
                drop(cached);
                if let Some(references) = &self.references {
                    references.register(reader.reader.clone());
                }
                Ok(reader as Arc<dyn UriReader>)
            })
        })
        .map_err(callback_error)
    }
}

struct PythonUriReader {
    reader: Arc<PythonUriReference>,
    references: Option<Arc<PythonUriReferences>>,
}

#[async_trait::async_trait]
impl UriReader for PythonUriReader {
    async fn new_input_stream(&self, uri: &str) -> paimon::Result<Box<dyn UriInputStream>> {
        tokio::task::block_in_place(|| {
            Python::attach(|py| {
                let reader = self.reader.clone_ref(py)?;
                let stream = reader
                    .bind(py)
                    .call_method1("new_input_stream", (uri,))?
                    .unbind();
                Ok(Box::new(PythonUriInputStream {
                    stream: PythonUriReference::new(stream, true, self.references.as_ref()),
                }) as Box<dyn UriInputStream>)
            })
        })
        .map_err(callback_error)
    }
}

struct PythonUriInputStream {
    stream: Arc<PythonUriReference>,
}

#[async_trait::async_trait]
impl UriInputStream for PythonUriInputStream {
    async fn read(&mut self, length: usize) -> paimon::Result<Bytes> {
        tokio::task::block_in_place(|| {
            Python::attach(|py| {
                let stream = self.stream.clone_ref(py)?;
                let bytes = stream
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
                let stream = self.stream.clone_ref(py)?;
                stream.bind(py).call_method1("seek", (offset,))?;
                Ok(())
            })
        })
        .map_err(callback_error)
    }

    async fn close(&mut self) -> paimon::Result<()> {
        self.stream.close().map_err(callback_error)
    }
}

impl Drop for PythonUriInputStream {
    fn drop(&mut self) {
        // Async close is explicit in core. On cancellation, still release a
        // Python stream without replacing the triggering error.
        let _ = self.stream.close();
    }
}

#[cfg(test)]
mod tests {
    use pyo3::ffi::c_str;
    use pyo3::types::PyDict;

    use super::*;

    #[pyfunction]
    fn reenter_native(py: Python<'_>) -> usize {
        py.detach(|| paimon_datafusion::runtime::runtime().block_on(async { 1 }))
    }

    #[test]
    fn cancellation_cleanup_can_reenter_native_from_a_runtime_thread() {
        Python::attach(|py| {
            for clear in [false, true] {
                let locals = PyDict::new(py);
                locals
                    .set_item("native", wrap_pyfunction!(reenter_native, py).unwrap())
                    .unwrap();
                py.run(
                    c_str!(
                        r#"
calls = []
class Stream:
    def close(self):
        calls.append(native())
stream = Stream()
"#
                    ),
                    Some(&locals),
                    Some(&locals),
                )
                .unwrap();
                let references = Arc::new(PythonUriReferences::default());
                let stream = PythonUriInputStream {
                    stream: PythonUriReference::new(
                        locals.get_item("stream").unwrap().unwrap().unbind(),
                        true,
                        Some(&references),
                    ),
                };
                py.detach(|| {
                    paimon_datafusion::runtime::runtime().block_on(async {
                        if clear {
                            references.clear();
                        }
                        drop(stream);
                    })
                });
                assert_eq!(
                    locals
                        .get_item("calls")
                        .unwrap()
                        .unwrap()
                        .extract::<Vec<usize>>()
                        .unwrap(),
                    vec![1],
                    "close must complete its Native callback, clear={clear}"
                );
            }
        });
    }

    #[test]
    fn registry_retires_only_slots_released_by_core() {
        Python::attach(|py| {
            for tracked in [false, true] {
                let references = Arc::new(PythonUriReferences::default());
                let object = py
                    .eval(c_str!("type('Callback', (), {})()"), None, None)
                    .unwrap();
                let weak = py
                    .import("weakref")
                    .unwrap()
                    .getattr("ref")
                    .unwrap()
                    .call1((&object,))
                    .unwrap();
                let slot =
                    PythonUriReference::new(object.unbind(), false, tracked.then_some(&references));
                references.prune();
                assert!(!weak.call0().unwrap().is_none());
                drop(slot);
                assert_eq!(weak.call0().unwrap().is_none(), !tracked);
                references.prune();
                assert!(weak.call0().unwrap().is_none());
            }
        });
    }

    #[test]
    fn registration_bounds_retired_readers_within_one_write() {
        Python::attach(|py| {
            let references = Arc::new(PythonUriReferences::default());
            let weakref = py.import("weakref").unwrap().getattr("ref").unwrap();
            let mut readers = Vec::new();
            for _ in 0..100 {
                let object = py
                    .eval(c_str!("type('Reader', (), {})()"), None, None)
                    .unwrap();
                readers.push(weakref.call1((&object,)).unwrap());
                let slot = PythonUriReference::new(object.unbind(), false, Some(&references));
                drop(slot);
            }
            assert!(
                readers
                    .iter()
                    .filter(|reader| !reader.call0().unwrap().is_none())
                    .count()
                    < 32,
                "a large write must not retain every retired Python reader"
            );
            references.prune();
            assert!(readers
                .iter()
                .all(|reader| reader.call0().unwrap().is_none()));
        });
    }

    #[test]
    fn registry_clear_and_stream_close_drop_share_one_cleanup() {
        Python::attach(|py| {
            for clear_first in [false, true] {
                let locals = PyDict::new(py);
                py.run(
                    c_str!(
                        r#"
calls = []
class Stream:
    def close(self):
        calls.append('close')
        raise ValueError('cleanup failed')
stream = Stream()
"#
                    ),
                    Some(&locals),
                    Some(&locals),
                )
                .unwrap();
                let references = Arc::new(PythonUriReferences::default());
                let slot = PythonUriReference::new(
                    locals.get_item("stream").unwrap().unwrap().unbind(),
                    true,
                    Some(&references),
                );
                let stream = PythonUriInputStream {
                    stream: slot.clone(),
                };
                if clear_first {
                    references.clear();
                } else {
                    assert!(slot
                        .close()
                        .unwrap_err()
                        .is_instance_of::<pyo3::exceptions::PyValueError>(py));
                }
                references.clear();
                slot.close().unwrap();
                drop(stream);
                references.prune();
                assert_eq!(
                    locals
                        .get_item("calls")
                        .unwrap()
                        .unwrap()
                        .extract::<Vec<String>>()
                        .unwrap(),
                    vec!["close"]
                );
                assert!(slot.clone_ref(py).is_err());
                assert!(references.references.lock().unwrap().is_empty());
            }
        });
    }
}
