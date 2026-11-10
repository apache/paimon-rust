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

use std::future::Future;

use paimon::io::{FileIO, FileRead};
use paimon::spec::{BlobDescriptor, VideoFrameDescriptor};
use paimon_datafusion::runtime::runtime;
use paimon_datafusion::BlobReaderRegistry;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::error::to_py_err;

fn block_on_runtime<F>(future: F, panic_error: &'static str) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        let handle = runtime();
        std::thread::spawn(move || handle.block_on(future))
            .join()
            .expect(panic_error)
    } else {
        runtime().block_on(future)
    }
}

#[pyclass(name = "BlobReaderRegistry", skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct PyBlobReaderRegistry {
    inner: BlobReaderRegistry,
}

impl PyBlobReaderRegistry {
    pub(crate) fn new(inner: BlobReaderRegistry) -> Self {
        Self { inner }
    }

    fn open_descriptor_stream(
        &self,
        descriptor: &BlobDescriptor,
    ) -> PyResult<Option<PyBlobInputStream>> {
        let Some(file_io) = self.inner.resolve(descriptor.uri()) else {
            return Ok(None);
        };

        Ok(Some(PyBlobInputStream::new(file_io, descriptor)?))
    }

    fn video_frame_descriptor(raw_value: &[u8]) -> PyResult<Option<VideoFrameDescriptor>> {
        if !VideoFrameDescriptor::is_video_frame_descriptor(raw_value) {
            return Ok(None);
        }
        VideoFrameDescriptor::deserialize(raw_value)
            .map(Some)
            .map_err(to_py_err)
    }
}

type VideoFrameInput = (PyBlobInputStream, Option<(i64, i64)>);
type VideoFrameLocation = ((String, i64, i64), i64);

#[pymethods]
impl PyBlobReaderRegistry {
    fn open_blob_descriptor_stream(&self, raw_value: &[u8]) -> PyResult<Option<PyBlobInputStream>> {
        if !BlobDescriptor::is_blob_descriptor(raw_value) {
            return Ok(None);
        }

        let descriptor = BlobDescriptor::deserialize(raw_value).map_err(to_py_err)?;
        self.open_descriptor_stream(&descriptor)
    }

    fn open_video_frame_descriptor_stream(
        &self,
        raw_value: &[u8],
    ) -> PyResult<Option<VideoFrameInput>> {
        let Some(frame) = Self::video_frame_descriptor(raw_value)? else {
            return Ok(None);
        };

        let stream = self
            .open_descriptor_stream(frame.payload_descriptor())?
            .ok_or_else(|| {
                PyValueError::new_err(
                    "VideoFrameDescriptor requires a registered Paimon table FileIO",
                )
            })?;
        let moov_range =
            (frame.moov_length() > 0).then(|| (frame.moov_offset(), frame.moov_length()));
        Ok(Some((stream, moov_range)))
    }

    fn describe_video_frame(&self, raw_value: &[u8]) -> PyResult<Option<VideoFrameLocation>> {
        Ok(Self::video_frame_descriptor(raw_value)?.map(|frame| {
            let payload = frame.payload_descriptor();
            (
                (
                    payload.uri().to_string(),
                    payload.offset(),
                    payload.length(),
                ),
                frame.frame_index(),
            )
        }))
    }
}

#[pyclass(name = "BlobInputStream")]
struct PyBlobInputStream {
    file_io: FileIO,
    uri: String,
    offset: u64,
    length: Option<u64>,
    position: u64,
    closed: bool,
}

impl PyBlobInputStream {
    fn new(file_io: FileIO, descriptor: &BlobDescriptor) -> PyResult<Self> {
        if descriptor.offset() < 0 {
            return Err(PyValueError::new_err(format!(
                "BlobDescriptor has negative offset: {}",
                descriptor.offset()
            )));
        }
        if descriptor.length() < -1 {
            return Err(PyValueError::new_err(format!(
                "BlobDescriptor has invalid length: {}",
                descriptor.length()
            )));
        }

        Ok(Self {
            file_io,
            uri: descriptor.uri().to_string(),
            offset: descriptor.offset() as u64,
            length: (descriptor.length() >= 0).then_some(descriptor.length() as u64),
            position: 0,
            closed: false,
        })
    }

    fn ensure_open(&self) -> PyResult<()> {
        if self.closed {
            Err(PyValueError::new_err("I/O operation on closed file."))
        } else {
            Ok(())
        }
    }

    fn stream_length(&self, py: Python<'_>) -> PyResult<u64> {
        if let Some(length) = self.length {
            return Ok(length);
        }

        let file_io = self.file_io.clone();
        let uri = self.uri.clone();
        let offset = self.offset;
        py.detach(|| {
            block_on_runtime(
                async move {
                    let input = file_io.new_input(&uri).map_err(to_py_err)?;
                    let metadata = input.metadata().await.map_err(to_py_err)?;
                    Ok(metadata.size.saturating_sub(offset))
                },
                "paimon blob metadata read thread panicked",
            )
        })
    }

    fn read_bytes(&mut self, py: Python<'_>, size: isize) -> PyResult<Vec<u8>> {
        self.ensure_open()?;
        let stream_length = self.stream_length(py)?;
        let remaining = stream_length.saturating_sub(self.position);
        if remaining == 0 || size == 0 {
            return Ok(Vec::new());
        }

        let to_read = if size < 0 {
            remaining
        } else {
            remaining.min(size as u64)
        };
        let start = self.offset + self.position;
        let end = start + to_read;
        let file_io = self.file_io.clone();
        let uri = self.uri.clone();
        let bytes = py.detach(|| {
            block_on_runtime(
                async move {
                    let input = file_io.new_input(&uri).map_err(to_py_err)?;
                    let reader = input.reader().await.map_err(to_py_err)?;
                    let bytes = reader.read(start..end).await.map_err(to_py_err)?;
                    Ok::<_, PyErr>(bytes.to_vec())
                },
                "paimon blob range read thread panicked",
            )
        })?;
        self.position += bytes.len() as u64;
        Ok(bytes)
    }
}

#[pymethods]
impl PyBlobInputStream {
    fn readable(&self) -> bool {
        true
    }

    fn seekable(&self) -> bool {
        true
    }

    fn tell(&self) -> u64 {
        self.position
    }

    #[getter]
    fn closed(&self) -> bool {
        self.closed
    }

    fn __enter__(slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf
    }

    fn __exit__(
        &mut self,
        _exc_type: &Bound<'_, PyAny>,
        _exc: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> bool {
        self.close();
        false
    }

    #[pyo3(signature = (size = -1))]
    fn read<'py>(&mut self, py: Python<'py>, size: isize) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = self.read_bytes(py, size)?;
        Ok(PyBytes::new(py, &bytes))
    }

    #[pyo3(signature = (pos, whence = 0))]
    fn seek(&mut self, py: Python<'_>, pos: i64, whence: i32) -> PyResult<u64> {
        self.ensure_open()?;
        let base = match whence {
            0 => 0,
            1 => self.position as i64,
            2 => self.stream_length(py)? as i64,
            other => return Err(PyValueError::new_err(format!("Invalid whence: {other}"))),
        };
        let target = base
            .checked_add(pos)
            .ok_or_else(|| PyValueError::new_err("Seek position overflow"))?;
        if target < 0 {
            return Err(PyValueError::new_err(format!(
                "Negative seek position: {target}"
            )));
        }
        self.position = target as u64;
        Ok(self.position)
    }

    fn close(&mut self) {
        self.closed = true;
    }
}
