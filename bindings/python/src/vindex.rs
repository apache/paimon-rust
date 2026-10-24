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

//! Python positional I/O over the reusable Rust core vector-index reader.

use crate::blob_uri_reader::{callback_error, to_callback_error};
use crate::error::to_py_err;
use paimon::vector_search::{GlobalIndexIOMeta, VectorSearch};
use paimon::vindex::reader::VindexVectorGlobalIndexReader;
use paimon_vindex_core::io::{ReadRequest, SeekRead};
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use roaring::RoaringTreemap;
use std::collections::HashMap;
use std::io::{self, Cursor};
use std::sync::Mutex;

struct PythonIndexInput(Py<PyAny>);

impl SeekRead for PythonIndexInput {
    fn pread(&mut self, ranges: &mut [ReadRequest<'_>]) -> io::Result<()> {
        Python::attach(|py| {
            let requests = ranges
                .iter()
                .map(|range| (range.pos, range.buf.len()))
                .collect::<Vec<_>>();
            let chunks = self
                .0
                .bind(py)
                .call_method1("pread_many", (requests,))?
                .extract::<Vec<Vec<u8>>>()?;
            if chunks.len() != ranges.len() {
                return Err(PyValueError::new_err(
                    "Index input returned a different number of ranges",
                ));
            }
            for (chunk, range) in chunks.iter().zip(ranges) {
                if chunk.len() != range.buf.len() {
                    return Err(PyValueError::new_err(format!(
                        "Index input short read at {}: expected {} bytes, got {}",
                        range.pos,
                        range.buf.len(),
                        chunk.len()
                    )));
                }
                range.buf.copy_from_slice(chunk);
            }
            Ok(())
        })
        .map_err(|error| io::Error::other(callback_error(error)))
    }
}

/// The input only supplies positional I/O; core owns query and score semantics.
#[pyclass(
    name = "VindexVectorGlobalIndexReader",
    module = "pypaimon_rust.datafusion"
)]
pub struct PyVindexVectorGlobalIndexReader {
    reader: Mutex<Option<VindexVectorGlobalIndexReader>>,
    metric: String,
    process_id: u32,
}

impl PyVindexVectorGlobalIndexReader {
    fn ensure_process(&self) -> PyResult<()> {
        if self.process_id != std::process::id() {
            return Err(to_py_err(paimon::Error::ProcessForkUnsupported {
                message: "An inherited vector-index reader cannot be used after fork; reopen it in the child process".into(),
            }));
        }
        Ok(())
    }
}

#[pymethods]
impl PyVindexVectorGlobalIndexReader {
    #[new]
    #[pyo3(signature = (input, file_path, file_size, options=None))]
    fn new(
        py: Python<'_>,
        input: Py<PyAny>,
        file_path: String,
        file_size: u64,
        options: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        if !input.bind(py).getattr("pread_many")?.is_callable() {
            return Err(PyTypeError::new_err(
                "Index input must provide callable pread_many",
            ));
        }
        let mut reader = VindexVectorGlobalIndexReader::new(
            GlobalIndexIOMeta::new(file_path, file_size, Vec::new()),
            options.unwrap_or_default(),
        );
        py.detach(|| reader.load(|_| Ok(PythonIndexInput(input))))
            .map_err(to_callback_error)?;
        let metric = reader.metadata().map_err(to_py_err)?.metric.as_str().into();
        Ok(Self {
            reader: Mutex::new(Some(reader)),
            metric,
            process_id: std::process::id(),
        })
    }

    /// Returns the native index metric, including before the first query.
    fn metric(&self, py: Python<'_>) -> PyResult<String> {
        self.ensure_process()?;
        py.detach(|| {
            let reader = self
                .reader
                .lock()
                .map_err(|_| PyRuntimeError::new_err("Vector-index reader lock was poisoned"))?;
            if reader.is_none() {
                return Err(PyValueError::new_err("Vector-index reader is closed"));
            }
            Ok(self.metric.clone())
        })
    }

    /// Each query returns its actual hits, which may be fewer than the limit.
    #[pyo3(signature = (vectors, limit, filter_bytes=None, options=None))]
    fn search(
        &self,
        py: Python<'_>,
        vectors: Vec<Vec<f32>>,
        limit: usize,
        filter_bytes: Option<Vec<u8>>,
        options: Option<HashMap<String, String>>,
    ) -> PyResult<Vec<Option<HashMap<u64, f32>>>> {
        self.ensure_process()?;
        let filter = filter_bytes
            .map(|bytes| {
                let mut input = Cursor::new(&bytes);
                let filter = RoaringTreemap::deserialize_from(&mut input)
                    .map_err(|error| PyValueError::new_err(error.to_string()))?;
                if input.position() != bytes.len() as u64 {
                    return Err(PyValueError::new_err(
                        "Trailing bytes after vector-index row-ID filter",
                    ));
                }
                Ok(filter)
            })
            .transpose()?;
        let options = options.unwrap_or_default();
        let searches = vectors
            .into_iter()
            .map(|vector| {
                let mut search = VectorSearch::new(vector, limit, "vector".into())?
                    .with_options(options.clone());
                if let Some(filter) = &filter {
                    search = search.with_include_row_ids(filter.clone());
                }
                Ok(search)
            })
            .collect::<paimon::Result<Vec<_>>>()
            .map_err(to_py_err)?;
        py.detach(|| {
            let mut reader = self
                .reader
                .lock()
                .map_err(|_| PyRuntimeError::new_err("Vector-index reader lock was poisoned"))?;
            reader
                .as_mut()
                .ok_or_else(|| PyValueError::new_err("Vector-index reader is closed"))?
                .visit_batch_vector_search::<PythonIndexInput>(&searches, |_| {
                    unreachable!("the index was opened by the constructor")
                })
                .map_err(to_callback_error)
        })
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        self.ensure_process()?;
        py.detach(|| {
            self.reader
                .lock()
                .map_err(|_| PyRuntimeError::new_err("Vector-index reader lock was poisoned"))?
                .take();
            Ok(())
        })
    }
}
