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

//! Adapt the already resolved PyPaimon FileIO. Storage routing, credentials and
//! custom loaders remain owned by that object; mutation algorithms stay in core.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use bytes::Buf;
use opendal::raw::{
    oio, OpCopier, OpCopy, OpCreateDir, OpDelete, OpList, OpPresign, OpRead, OpRename, OpStat,
    OpWrite, RpCreateDir, RpPresign, RpRead, RpRename, RpStat, Service, ServiceInfo, Timestamp,
};
use opendal::{
    Buffer, BytesRange, Capability, EntryMode, Error, ErrorKind, Metadata, OperationContext,
    Operator,
};
use paimon::io::{FileIO, FileIOBuilder, FileIOProvider};
use pyo3::exceptions::{PyFileExistsError, PyFileNotFoundError, PyPermissionError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

const READ_CHUNK_SIZE: u64 = 1024 * 1024;

pub(crate) fn python_file_io(python_io: Py<PyAny>) -> paimon::Result<FileIO> {
    FileIOBuilder::new("python")
        .with_provider(Arc::new(PythonFileIOProvider {
            io: Arc::new(python_io),
            operators: Mutex::new(HashMap::new()),
        }))
        .build()
}

#[derive(Debug)]
struct PythonFileIOProvider {
    io: Arc<Py<PyAny>>,
    operators: Mutex<HashMap<String, Operator>>,
}

#[async_trait::async_trait]
impl FileIOProvider for PythonFileIOProvider {
    async fn create(&self, path: &str) -> paimon::Result<(Operator, String)> {
        let (prefix, relative) = split_path(path);
        if !relative.is_empty() && opendal::raw::normalize_path(&relative) != relative {
            return Err(paimon::Error::ConfigInvalid {
                message: "Python FileIO path would be changed by OpenDAL normalization".to_string(),
            });
        }
        let mut operators = self
            .operators
            .lock()
            .map_err(|_| paimon::Error::UnexpectedError {
                message: "Python FileIO operator lock is poisoned".to_string(),
                source: None,
            })?;
        let operator = operators.entry(prefix.clone()).or_insert_with(|| {
            Operator::from_parts(
                OperationContext::default(),
                Arc::new(PythonService {
                    io: Arc::clone(&self.io),
                    prefix: prefix.clone(),
                }),
            )
        });
        Ok((operator.clone(), relative))
    }
}

// Do not parse as URL: object names may contain literal %, ? and # characters.
fn split_path(path: &str) -> (String, String) {
    if let Some(scheme) = path.find("://") {
        let authority = scheme + 3;
        return match path[authority..].find('/') {
            Some(offset) => {
                let (prefix, relative) = path.split_at(authority + offset + 1);
                (prefix.to_string(), relative.to_string())
            }
            None => (format!("{path}/"), String::new()),
        };
    }
    if let Some(scheme) = path.find(":/") {
        let (prefix, relative) = path.split_at(scheme + 2);
        return (prefix.to_string(), relative.to_string());
    }
    if let Some(relative) = path.strip_prefix('/') {
        ("/".to_string(), relative.to_string())
    } else {
        (String::new(), path.to_string())
    }
}

#[derive(Debug)]
struct PythonService {
    io: Arc<Py<PyAny>>,
    prefix: String,
}

impl PythonService {
    fn path(&self, relative: &str) -> String {
        if relative == "/" {
            self.prefix.clone()
        } else {
            format!("{}{relative}", self.prefix)
        }
    }
}

impl Service for PythonService {
    type Reader = PythonReader;
    type Writer = PythonWriter;
    type Lister = PythonLister;
    type Deleter = PythonDeleter;
    type Copier = ();

    fn info(&self) -> ServiceInfo {
        ServiceInfo::new("python", "/", &self.prefix)
    }

    fn capability(&self) -> Capability {
        Capability {
            stat: true,
            read: true,
            write: true,
            write_can_multi: true,
            create_dir: true,
            delete: true,
            delete_with_recursive: true,
            list: true,
            list_with_recursive: true,
            rename: true,
            ..Default::default()
        }
    }

    async fn create_dir(
        &self,
        _ctx: &OperationContext,
        path: &str,
        _args: OpCreateDir,
    ) -> opendal::Result<RpCreateDir> {
        let path = self.path(path);
        blocking_python_io(|py| {
            let io = self.io.bind(py);
            let created = io.call_method1("mkdirs", (&path,))?.extract::<bool>()?;
            if !created && !io.call_method1("is_dir", (&path,))?.extract::<bool>()? {
                return Err(pyo3::exceptions::PyOSError::new_err(
                    "mkdirs returned false",
                ));
            }
            Ok(RpCreateDir::default())
        })
        .map_err(python_error)
    }

    async fn stat(
        &self,
        _ctx: &OperationContext,
        path: &str,
        _args: OpStat,
    ) -> opendal::Result<RpStat> {
        blocking_python_io(|py| {
            let status = self
                .io
                .bind(py)
                .call_method1("get_file_status", (self.path(path),))?;
            metadata(&status).map(RpStat::new)
        })
        .map_err(python_error)
    }

    fn read(
        &self,
        _ctx: &OperationContext,
        path: &str,
        _args: OpRead,
    ) -> opendal::Result<Self::Reader> {
        Ok(PythonReader {
            io: Arc::clone(&self.io),
            path: self.path(path),
        })
    }

    fn write(
        &self,
        _ctx: &OperationContext,
        path: &str,
        args: OpWrite,
    ) -> opendal::Result<Self::Writer> {
        if args.append()
            || args.if_not_exists()
            || args.if_match().is_some()
            || args.if_none_match().is_some()
        {
            return Err(unsupported("conditional or append write"));
        }
        let stream = blocking_python_io(|py| {
            self.io
                .bind(py)
                .call_method1("new_output_stream", (self.path(path),))
                .map(Bound::unbind)
        })
        .map_err(python_error)?;
        Ok(PythonWriter {
            stream: Some(stream),
            size: 0,
        })
    }

    fn delete(&self, _ctx: &OperationContext) -> opendal::Result<Self::Deleter> {
        Ok(PythonDeleter {
            io: Arc::clone(&self.io),
            prefix: self.prefix.clone(),
        })
    }

    fn list(
        &self,
        _ctx: &OperationContext,
        path: &str,
        args: OpList,
    ) -> opendal::Result<Self::Lister> {
        if args.start_after().is_some() || args.versions() {
            return Err(unsupported("versioned or paginated list"));
        }
        Ok(PythonLister {
            io: Arc::clone(&self.io),
            prefix: self.prefix.clone(),
            pending: VecDeque::from([path.to_string()]),
            entries: VecDeque::new(),
            recursive: args.recursive(),
        })
    }

    fn copy(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpCopy,
        _opts: OpCopier,
    ) -> opendal::Result<Self::Copier> {
        Err(unsupported("server-side copy"))
    }

    async fn rename(
        &self,
        _ctx: &OperationContext,
        from: &str,
        to: &str,
        _args: OpRename,
    ) -> opendal::Result<RpRename> {
        let from = self.path(from);
        let to = self.path(to);
        blocking_python_io(|py| {
            if !self
                .io
                .bind(py)
                .call_method1("rename", (&from, &to))?
                .extract::<bool>()?
            {
                return Err(pyo3::exceptions::PyOSError::new_err(
                    "rename returned false",
                ));
            }
            Ok(RpRename::default())
        })
        .map_err(python_error)
    }

    async fn presign(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpPresign,
    ) -> opendal::Result<RpPresign> {
        Err(unsupported("presign"))
    }
}

// Storage implementations are synchronous Python stream APIs. On a multithread
// runtime, hand off the async worker before entering them, as the URI reader
// adapter does. Besides avoiding worker starvation, this allows a provider to
// call another native operation which enters the same Tokio runtime.
fn blocking_io<T>(call: impl FnOnce() -> T) -> T {
    if tokio::runtime::Handle::try_current()
        .is_ok_and(|handle| handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread)
    {
        tokio::task::block_in_place(call)
    } else {
        call()
    }
}

fn blocking_python_io<T>(call: impl for<'py> FnOnce(Python<'py>) -> PyResult<T>) -> PyResult<T> {
    blocking_io(|| Python::attach(call))
}

fn unsupported(operation: &str) -> Error {
    Error::new(
        ErrorKind::Unsupported,
        format!("Python FileIO does not support {operation}"),
    )
}

fn python_error(error: PyErr) -> Error {
    let kind = Python::attach(|py| {
        if error.is_instance_of::<PyFileNotFoundError>(py) {
            ErrorKind::NotFound
        } else if error.is_instance_of::<PyPermissionError>(py) {
            ErrorKind::PermissionDenied
        } else if error.is_instance_of::<PyFileExistsError>(py) {
            ErrorKind::AlreadyExists
        } else {
            ErrorKind::Unexpected
        }
    });
    Error::new(kind, "Python FileIO operation failed")
        .set_source(crate::blob_uri_reader::callback_error(error))
}

fn metadata(status: &Bound<'_, PyAny>) -> PyResult<Metadata> {
    let mode = match status.getattr("type")?.extract::<i32>()? {
        0 => {
            return Err(PyFileNotFoundError::new_err(
                "Python FileIO status is NotFound",
            ))
        }
        2 => EntryMode::FILE,
        3 => EntryMode::DIR,
        _ => {
            return Err(pyo3::exceptions::PyOSError::new_err(
                "Unknown Python FileIO entry type",
            ))
        }
    };
    let mut result = Metadata::new(mode);
    if mode == EntryMode::FILE {
        result = result.with_content_length(status.getattr("size")?.extract::<u64>()?);
    }
    let mtime = status.getattr("mtime")?;
    if !mtime.is_none() {
        let seconds = if let Ok(value) = mtime.extract::<f64>() {
            value
        } else {
            mtime.call_method0("timestamp")?.extract::<f64>()?
        };
        if !seconds.is_finite() || seconds < i64::MIN as f64 || seconds >= i64::MAX as f64 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Invalid FileIO modification time",
            ));
        }
        let second = seconds.trunc() as i64;
        let nanos = ((seconds - second as f64) * 1_000_000_000.0).trunc() as i32;
        result = result.with_last_modified(
            Timestamp::new(second, nanos)
                .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))?,
        );
    }
    Ok(result)
}

struct PythonReader {
    io: Arc<Py<PyAny>>,
    path: String,
}

impl PythonReader {
    fn range(&self, range: BytesRange) -> opendal::Result<(u64, u64)> {
        let start = range.offset();
        // Bounded Parquet reads already carry the known file length. Avoid an
        // additional storage stat for every footer/page range.
        if let Some(length) = range.size() {
            return start
                .checked_add(length)
                .map(|end| (start, end))
                .ok_or_else(|| {
                    Error::new(ErrorKind::RangeNotSatisfied, "Read range overflows u64")
                });
        }
        let size = blocking_python_io(|py| {
            self.io
                .bind(py)
                .call_method1("get_file_size", (&self.path,))?
                .extract::<u64>()
        })
        .map_err(python_error)?;
        if start > size {
            return Err(Error::new(
                ErrorKind::RangeNotSatisfied,
                "Read offset exceeds file size",
            ));
        }
        Ok((start, size))
    }

    fn read_range(&self, start: u64, end: u64) -> opendal::Result<Buffer> {
        let result = blocking_python_io(|py| {
            self.io
                .bind(py)
                .call_method1("read_file_range", (&self.path, start, end - start))?
                .extract::<Vec<u8>>()
                .map(Buffer::from)
        })
        .map_err(python_error)?;
        if result.len() as u64 != end - start {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Python FileIO returned a short read",
            ));
        }
        Ok(result)
    }
}

impl oio::Read for PythonReader {
    async fn open(
        &self,
        range: BytesRange,
    ) -> opendal::Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let (start, end) = self.range(range)?;
        Ok((
            RpRead::default(),
            Box::new(PythonReadStream {
                reader: PythonReader {
                    io: Arc::clone(&self.io),
                    path: self.path.clone(),
                },
                position: start,
                end,
            }),
        ))
    }

    async fn read(&self, range: BytesRange) -> opendal::Result<(RpRead, Buffer)> {
        let (start, end) = self.range(range)?;
        Ok((RpRead::default(), self.read_range(start, end)?))
    }
}

struct PythonReadStream {
    reader: PythonReader,
    position: u64,
    end: u64,
}

impl oio::ReadStream for PythonReadStream {
    async fn read(&mut self) -> opendal::Result<Buffer> {
        if self.position >= self.end {
            return Ok(Buffer::new());
        }
        let end = self.position.saturating_add(READ_CHUNK_SIZE).min(self.end);
        let result = self.reader.read_range(self.position, end)?;
        if result.is_empty() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Python FileIO returned a short read",
            ));
        }
        self.position += result.len() as u64;
        Ok(result)
    }
}

struct PythonWriter {
    stream: Option<Py<PyAny>>,
    size: u64,
}

impl oio::Write for PythonWriter {
    async fn write(&mut self, mut buffer: Buffer) -> opendal::Result<()> {
        let stream = self
            .stream
            .as_ref()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Python FileIO writer is closed"))?;
        while !buffer.is_empty() {
            let bytes = buffer.chunk();
            blocking_python_io(|py| {
                let stream = stream.bind(py);
                let mut offset = 0;
                while offset < bytes.len() {
                    let data = PyBytes::new(py, &bytes[offset..]);
                    let written = stream.call_method1("write", (data,))?;
                    let count = if written.is_none() {
                        bytes.len() - offset
                    } else {
                        written.extract::<usize>()?
                    };
                    if count == 0 || count > bytes.len() - offset {
                        return Err(pyo3::exceptions::PyOSError::new_err(
                            "Invalid stream write length",
                        ));
                    }
                    offset += count;
                }
                Ok(())
            })
            .map_err(python_error)?;
            self.size += bytes.len() as u64;
            let size = bytes.len();
            buffer.advance(size);
        }
        Ok(())
    }

    async fn close(&mut self) -> opendal::Result<Metadata> {
        let stream = self
            .stream
            .take()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Python FileIO writer is closed"))?;
        blocking_python_io(|py| stream.bind(py).call_method0("close").map(|_| ()))
            .map_err(python_error)?;
        Ok(Metadata::new(EntryMode::FILE).with_content_length(self.size))
    }

    async fn abort(&mut self) -> opendal::Result<()> {
        // Python streams have close(), not a portable abort contract. Never
        // delete here: even a failed close can have published its output.
        if let Some(stream) = self.stream.take() {
            blocking_python_io(|py| stream.bind(py).call_method0("close").map(|_| ()))
                .map_err(python_error)?;
        }
        Ok(())
    }
}

impl Drop for PythonWriter {
    fn drop(&mut self) {
        if let Some(stream) = self.stream.take() {
            let _ = blocking_io(|| {
                Python::try_attach(|py| stream.bind(py).call_method0("close").map(|_| ()))
            });
        }
    }
}

struct PythonDeleter {
    io: Arc<Py<PyAny>>,
    prefix: String,
}

impl oio::Delete for PythonDeleter {
    async fn delete(&mut self, path: &str, args: OpDelete) -> opendal::Result<()> {
        let path = if path == "/" {
            self.prefix.clone()
        } else {
            format!("{}{path}", self.prefix)
        };
        blocking_python_io(|py| {
            let io = self.io.bind(py);
            let deleted = io
                .call_method1("delete", (&path, args.recursive()))?
                .extract::<bool>()?;
            if !deleted && io.call_method1("exists", (&path,))?.extract::<bool>()? {
                return Err(pyo3::exceptions::PyOSError::new_err(
                    "delete returned false",
                ));
            }
            Ok(())
        })
        .map_err(python_error)
    }

    async fn close(&mut self) -> opendal::Result<()> {
        Ok(())
    }
}

struct PythonLister {
    io: Arc<Py<PyAny>>,
    prefix: String,
    pending: VecDeque<String>,
    entries: VecDeque<oio::Entry>,
    recursive: bool,
}

impl oio::List for PythonLister {
    async fn next(&mut self) -> opendal::Result<Option<oio::Entry>> {
        loop {
            if let Some(entry) = self.entries.pop_front() {
                return Ok(Some(entry));
            }
            let Some(directory) = self.pending.pop_front() else {
                return Ok(None);
            };
            let path = if directory == "/" {
                self.prefix.clone()
            } else {
                format!("{}{directory}", self.prefix)
            };
            let entries = blocking_python_io(|py| {
                let mut entries = VecDeque::new();
                for status in self
                    .io
                    .bind(py)
                    .call_method1("list_status", (&path,))?
                    .try_iter()?
                {
                    let status = status?;
                    let meta = metadata(&status)?;
                    let name = status.getattr("base_name")?.extract::<String>()?;
                    if name.is_empty() || name == "." || name == ".." || name.contains('/') {
                        return Err(pyo3::exceptions::PyValueError::new_err(
                            "Invalid FileIO listing basename",
                        ));
                    }
                    let mut relative = if directory == "/" {
                        name
                    } else {
                        format!("{directory}{name}")
                    };
                    if meta.is_dir() {
                        relative.push('/');
                    }
                    entries.push_back(oio::Entry::new(&relative, meta));
                }
                Ok(entries)
            })
            .map_err(python_error)?;
            if self.recursive {
                self.pending.extend(
                    entries
                        .iter()
                        .filter(|entry| entry.metadata().is_dir())
                        .map(|entry| entry.path().to_string()),
                );
            }
            self.entries = entries;
        }
    }
}

#[cfg(test)]
mod tests;
