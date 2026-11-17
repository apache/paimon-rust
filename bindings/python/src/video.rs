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

//! Descriptor and stream transport for PyPaimon; packed-video semantics live in core.

use crate::blob_uri_reader::{
    callback_error, to_callback_error, PythonUriReaderFactory, PythonUriReferences,
};
use crate::error::to_py_err;
use arrow::array::{ArrayRef, LargeBinaryArray, RecordBatch};
use bytes::Bytes;
use paimon::io::{FileIOBuilder, FileWrite, UriReaderFactory};
use paimon::spec::VideoFrameDescriptor;
use paimon::table::internal::PackedVideoWriter;
use paimon_datafusion::runtime::runtime;
use pyo3::exceptions::{PyIOError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::{PyTraverseError, PyVisit};
use std::sync::{Arc, Mutex};

#[pyclass(
    name = "VideoFrameDescriptor",
    module = "pypaimon_rust.data",
    frozen,
    eq,
    skip_from_py_object
)]
#[derive(Clone, PartialEq)]
struct PyVideoFrameDescriptor {
    inner: VideoFrameDescriptor,
}

#[pymethods]
impl PyVideoFrameDescriptor {
    #[new]
    #[pyo3(signature = (uri, offset, length, frame_index, moov_offset=0, moov_length=0))]
    fn new(
        uri: String,
        offset: i64,
        length: i64,
        frame_index: i64,
        moov_offset: i64,
        moov_length: i64,
    ) -> PyResult<Self> {
        let inner = VideoFrameDescriptor::new(uri, offset, length, frame_index)
            .and_then(|frame| frame.with_moov(moov_offset, moov_length))
            .map_err(to_py_err)?;
        inner.payload_descriptor().validate().map_err(to_py_err)?;
        Ok(Self { inner })
    }
    #[getter]
    fn uri(&self) -> &str {
        self.inner.payload_descriptor().uri()
    }
    #[getter]
    fn offset(&self) -> i64 {
        self.inner.payload_descriptor().offset()
    }
    #[getter]
    fn length(&self) -> i64 {
        self.inner.payload_descriptor().length()
    }
    #[getter]
    fn frame_index(&self) -> i64 {
        self.inner.frame_index()
    }
    #[getter]
    fn moov_offset(&self) -> i64 {
        self.inner.moov_offset()
    }
    #[getter]
    fn moov_length(&self) -> i64 {
        self.inner.moov_length()
    }
    fn serialize<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.inner.serialize())
    }
    #[staticmethod]
    fn deserialize(value: &[u8]) -> PyResult<Self> {
        Ok(Self {
            inner: VideoFrameDescriptor::deserialize(value).map_err(to_py_err)?,
        })
    }
}

type OutputSlot = Arc<Mutex<Option<Py<PyAny>>>>;

struct PythonOutput(OutputSlot);

impl PythonOutput {
    fn close_stream(&self) -> PyResult<()> {
        let stream = self.0.lock().unwrap().take();
        if let Some(stream) = stream {
            let flush: PyResult<()> = tokio::task::block_in_place(|| {
                Python::attach(|py| {
                    if stream.bind(py).hasattr("flush")? {
                        stream.bind(py).call_method0("flush")?;
                    }
                    Ok(())
                })
            });
            let close: PyResult<()> = tokio::task::block_in_place(|| {
                Python::attach(|py| {
                    stream.bind(py).call_method0("close")?;
                    Ok(())
                })
            });
            flush?;
            close?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl FileWrite for PythonOutput {
    async fn write(&mut self, bytes: Bytes) -> paimon::Result<()> {
        tokio::task::block_in_place(|| {
            Python::attach(|py| {
                let stream = self
                    .0
                    .lock()
                    .unwrap()
                    .as_ref()
                    .map(|stream| stream.clone_ref(py))
                    .ok_or_else(|| PyRuntimeError::new_err("Video output stream is closed"))?;
                let written = stream
                    .bind(py)
                    .call_method1("write", (PyBytes::new(py, &bytes),))?;
                if !written.is_none() && written.extract::<usize>()? != bytes.len() {
                    return Err(PyIOError::new_err("Short write to video output stream"));
                }
                Ok(())
            })
        })
        .map_err(callback_error)
    }
    async fn close(&mut self) -> paimon::Result<()> {
        self.close_stream().map_err(callback_error)
    }
}

impl Drop for PythonOutput {
    fn drop(&mut self) {
        let _ = self.close_stream();
    }
}

#[pyclass(name = "VideoFormatWriter", module = "pypaimon_rust.data")]
struct PyVideoFormatWriter {
    inner: Mutex<Option<PackedVideoWriter>>,
    output: OutputSlot,
    references: Arc<PythonUriReferences>,
    completed_size: Option<u64>,
}

impl PyVideoFormatWriter {
    fn write_batch(&mut self, py: Python<'_>, batch: RecordBatch) -> PyResult<()> {
        let writer = self
            .inner
            .get_mut()
            .unwrap()
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("Video writer is closed"))?;
        let result = py.detach(|| runtime().block_on(writer.write(&batch)));
        self.references.prune();
        if let Err(error) = result {
            self.abort(py);
            return Err(to_callback_error(error));
        }
        Ok(())
    }
}

#[pymethods]
impl PyVideoFormatWriter {
    #[new]
    #[pyo3(signature = (output_stream, *, uri_reader_factory=None, copy_buffer_size=4096))]
    fn new(
        py: Python<'_>,
        output_stream: Py<PyAny>,
        uri_reader_factory: Option<Py<PyAny>>,
        copy_buffer_size: usize,
    ) -> PyResult<Self> {
        if copy_buffer_size == 0 {
            return Err(PyValueError::new_err(
                "Video copy buffer size must be positive",
            ));
        }
        for name in ["write", "close"] {
            if !output_stream.bind(py).getattr(name)?.is_callable() {
                return Err(PyTypeError::new_err(format!(
                    "output_stream.{name} must be callable"
                )));
            }
        }
        let references = Arc::new(PythonUriReferences::default());
        let factory = uri_reader_factory
            .map(|factory| {
                PythonUriReaderFactory::tracked(factory, py, references.clone())
                    .map(|factory| Arc::new(factory) as Arc<dyn UriReaderFactory>)
            })
            .transpose()?;
        let io = FileIOBuilder::new("fs").build().map_err(to_py_err)?;
        let output = Arc::new(Mutex::new(Some(output_stream)));
        let inner = PackedVideoWriter::new(
            Box::new(PythonOutput(output.clone())),
            Some(io),
            factory,
            copy_buffer_size,
        )
        .map_err(to_py_err)?;
        Ok(Self {
            inner: Mutex::new(Some(inner)),
            output,
            references,
            completed_size: None,
        })
    }

    /// Write serialized frame descriptors (including bytes from PyPaimon) and NULLs.
    fn write_frames(&mut self, py: Python<'_>, frames: Vec<Option<Vec<u8>>>) -> PyResult<()> {
        let values = LargeBinaryArray::from(
            frames
                .iter()
                .map(|frame| frame.as_deref())
                .collect::<Vec<_>>(),
        );
        let batch = RecordBatch::try_from_iter([("frame", Arc::new(values) as ArrayRef)])
            .map_err(|error| PyTypeError::new_err(error.to_string()))?;
        self.write_batch(py, batch)
    }

    /// Write one large_binary Arrow column of serialized frame descriptors.
    fn write_arrow(&mut self, py: Python<'_>, batch: &Bound<'_, PyAny>) -> PyResult<()> {
        self.write_batch(py, crate::arrow_input::batch_from_pyarrow(batch)?)
    }

    fn num_bytes(&self) -> usize {
        self.completed_size.map_or_else(
            || {
                self.inner
                    .lock()
                    .unwrap()
                    .as_ref()
                    .map_or(0, |writer| writer.num_bytes())
            },
            |size| size as usize,
        )
    }

    /// Finalize indexes, close the owned output stream, and return its total size.
    fn close(&mut self, py: Python<'_>) -> PyResult<u64> {
        if let Some(size) = self.completed_size {
            return Ok(size);
        }
        let mut writer = self
            .inner
            .get_mut()
            .unwrap()
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("Video writer is closed"))?;
        let result = py.detach(|| runtime().block_on(writer.close()));
        self.references.clear();
        let size = result.map_err(to_callback_error)?;
        self.completed_size = Some(size);
        Ok(size)
    }

    /// Close without a footer. The caller removes any incomplete destination.
    fn abort(&mut self, py: Python<'_>) {
        if let Some(mut writer) = self.inner.get_mut().unwrap().take() {
            let _ = py.detach(|| runtime().block_on(writer.abort()));
        }
        self.references.clear();
    }

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        visit.call(&*self.output.lock().unwrap())?;
        self.references.traverse(visit)
    }

    fn __clear__(&mut self) {
        self.inner.get_mut().unwrap().take();
        self.references.clear();
    }
}

pub(crate) fn register_module(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let data = module.getattr("data")?.cast_into::<PyModule>()?;
    data.add_class::<PyVideoFrameDescriptor>()?;
    data.add_class::<PyVideoFormatWriter>()
}
