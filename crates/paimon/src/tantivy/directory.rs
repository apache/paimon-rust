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

//! Archive-backed Tantivy Directory implementation.
//!
//! Reads a Tantivy index packed into a single archive file. The archive format
//! (Big-Endian, compatible with Java Paimon):
//!
//! ```text
//! [fileCount: 4 bytes BE]
//! for each file:
//!   [nameLen: 4 bytes BE]
//!   [name: nameLen bytes UTF-8]
//!   [dataLen: 8 bytes BE]
//!   [data: dataLen bytes]
//! ```
//!
//! Reference: `org.apache.paimon.tantivy.index.TantivyFullTextGlobalIndexWriter.packIndex()`
//! Reference: `org.apache.paimon.tantivy.index.TantivyFullTextGlobalIndexReader.parseArchiveHeader()`

use bytes::Bytes;
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::Directory;
use tantivy::directory::{
    AntiCallToken, DirectoryLock, FileHandle, Lock, OwnedBytes, TerminatingWrite, WatchCallback,
    WatchHandle, WritePtr,
};
use tantivy::HasLen;

use crate::io::FileRead;

/// Metadata for a single file within the archive.
#[derive(Clone, Debug)]
struct FileMeta {
    /// Absolute byte offset of the file data within the archive.
    offset: u64,
    length: usize,
}

/// A read-only Tantivy `Directory` backed by an archive file.
///
/// Only the archive header (file names, offsets, lengths) is parsed eagerly.
/// Actual file data is read on demand via `FileRead`.
#[derive(Clone)]
pub struct ArchiveDirectory {
    files: Arc<HashMap<PathBuf, FileMeta>>,
    reader: Arc<dyn FileRead>,
    /// In-memory storage for atomic_write (used by Tantivy for meta.json).
    atomic_data: Arc<Mutex<HashMap<PathBuf, Vec<u8>>>>,
}

impl ArchiveDirectory {
    /// Create an `ArchiveDirectory` from an async `FileRead`.
    ///
    /// Only the archive header (file names, offsets, lengths) is read eagerly.
    /// Actual file data is read on demand when Tantivy requests it.
    pub async fn from_reader(reader: impl FileRead, file_size: u64) -> io::Result<Self> {
        let reader: Arc<dyn FileRead> = Arc::new(reader);

        if file_size < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Archive too small to contain file count",
            ));
        }

        // Read file count (4 bytes).
        let buf = reader
            .read(0..4)
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
        let file_count = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if file_count < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Negative file count in archive: {file_count}"),
            ));
        }
        let file_count = file_count as usize;

        let mut pos: u64 = 4;
        let mut files = HashMap::with_capacity(file_count);

        for _ in 0..file_count {
            // Read name_len (4 bytes).
            let buf = reader
                .read(pos..pos + 4)
                .await
                .map_err(|e| io::Error::other(e.to_string()))?;
            let name_len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
            if name_len < 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Negative name length in archive: {name_len}"),
                ));
            }
            let name_len = name_len as u64;
            pos += 4;

            // Read name + data_len together in a single IO call.
            let meta_buf = reader
                .read(pos..pos + name_len + 8)
                .await
                .map_err(|e| io::Error::other(e.to_string()))?;

            let name = String::from_utf8(meta_buf[..name_len as usize].to_vec()).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid UTF-8 in file name: {}", e),
                )
            })?;

            let dl = name_len as usize;
            let data_len = i64::from_be_bytes([
                meta_buf[dl],
                meta_buf[dl + 1],
                meta_buf[dl + 2],
                meta_buf[dl + 3],
                meta_buf[dl + 4],
                meta_buf[dl + 5],
                meta_buf[dl + 6],
                meta_buf[dl + 7],
            ]);
            if data_len < 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Negative data length in archive: {data_len}"),
                ));
            }
            let data_len = data_len as u64;
            pos += name_len + 8;

            let data_offset = pos;
            files.insert(
                PathBuf::from(&name),
                FileMeta {
                    offset: data_offset,
                    length: data_len as usize,
                },
            );

            // Skip past file data — do NOT read it.
            pos += data_len;
        }

        Ok(Self {
            files: Arc::new(files),
            reader,
            atomic_data: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

/// Bridge sync Tantivy `FileHandle::read_bytes` to async `FileRead::read`.
///
/// Uses `block_in_place` on multi-threaded tokio runtimes (no thread spawn
/// overhead). Falls back to a scoped thread for current-thread runtimes.
fn block_on_read(reader: &Arc<dyn FileRead>, range: Range<u64>) -> io::Result<Bytes> {
    let handle = tokio::runtime::Handle::current();
    let do_read = || {
        handle
            .block_on(reader.read(range.clone()))
            .map_err(|e| io::Error::other(e.to_string()))
    };

    match handle.runtime_flavor() {
        tokio::runtime::RuntimeFlavor::MultiThread => tokio::task::block_in_place(do_read),
        _ => {
            // Current-thread runtime: block_in_place is not available,
            // fall back to a scoped thread.
            let reader = Arc::clone(reader);
            std::thread::scope(|s| {
                s.spawn(move || {
                    handle
                        .block_on(reader.read(range))
                        .map_err(|e| io::Error::other(e.to_string()))
                })
                .join()
                .map_err(|_| io::Error::other("reader thread panicked"))?
            })
        }
    }
}

impl fmt::Debug for ArchiveDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArchiveDirectory")
            .field("files", &self.files.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// A `FileHandle` for a single file within the archive.
#[derive(Clone)]
struct ArchiveFileHandle {
    reader: Arc<dyn FileRead>,
    file_offset: u64,
    file_length: usize,
}

impl fmt::Debug for ArchiveFileHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArchiveFileHandle")
            .field("offset", &self.file_offset)
            .field("length", &self.file_length)
            .finish()
    }
}

impl HasLen for ArchiveFileHandle {
    fn len(&self) -> usize {
        self.file_length
    }
}

impl FileHandle for ArchiveFileHandle {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        if range.end > self.file_length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Read range {:?} exceeds file length {}",
                    range, self.file_length
                ),
            ));
        }

        let abs_start = self.file_offset + range.start as u64;
        let abs_end = self.file_offset + range.end as u64;
        let data = block_on_read(&self.reader, abs_start..abs_end)?;
        Ok(OwnedBytes::new(data.to_vec()))
    }
}

impl Directory for ArchiveDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let meta = self
            .files
            .get(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;

        Ok(Arc::new(ArchiveFileHandle {
            reader: self.reader.clone(),
            file_offset: meta.offset,
            file_length: meta.length,
        }))
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        Ok(self.files.contains_key(path) || self.atomic_data.lock().unwrap().contains_key(path))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        if let Some(data) = self.atomic_data.lock().unwrap().get(path) {
            return Ok(data.clone());
        }
        let meta = self
            .files
            .get(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;

        let data = block_on_read(&self.reader, meta.offset..meta.offset + meta.length as u64)
            .map_err(|e| OpenReadError::wrap_io_error(e, path.to_path_buf()))?;
        Ok(data.to_vec())
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        self.atomic_data
            .lock()
            .unwrap()
            .insert(path.to_path_buf(), data.to_vec());
        Ok(())
    }

    fn delete(&self, _path: &Path) -> Result<(), DeleteError> {
        Ok(())
    }

    fn open_write(&self, _path: &Path) -> Result<WritePtr, OpenWriteError> {
        Ok(io::BufWriter::new(Box::new(
            VecTerminatingWrite(Vec::new()),
        )))
    }

    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }

    fn acquire_lock(&self, _lock: &Lock) -> Result<DirectoryLock, LockError> {
        Ok(DirectoryLock::from(Box::new(())))
    }

    fn watch(&self, _watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(WatchHandle::empty())
    }
}

/// Dummy writer for lock file support.
struct VecTerminatingWrite(Vec<u8>);

impl io::Write for VecTerminatingWrite {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl TerminatingWrite for VecTerminatingWrite {
    fn terminate_ref(&mut self, _token: AntiCallToken) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;
    use crate::tantivy::writer::TantivyFullTextWriter;

    async fn make_test_dir() -> ArchiveDirectory {
        let file_io = FileIOBuilder::new("memory").build().unwrap();

        let mut writer = TantivyFullTextWriter::new().unwrap();
        writer.add_document(0, Some("hello")).unwrap();
        writer.add_document(1, Some("world")).unwrap();
        let output = file_io.new_output("/test_archive.bin").unwrap();
        writer.finish(&output).await.unwrap();

        let input = output.to_input_file();
        let metadata = input.metadata().await.unwrap();
        let reader = input.reader().await.unwrap();
        ArchiveDirectory::from_reader(reader, metadata.size)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_parse_archive() {
        let dir = make_test_dir().await;
        // Tantivy index files should be present.
        assert!(!dir.files.is_empty());
    }

    #[tokio::test]
    async fn test_read_file_from_archive() {
        let dir = make_test_dir().await;
        // Read any file from the archive and verify it's non-empty.
        let first_path = dir.files.keys().next().unwrap().clone();
        let handle = dir.get_file_handle(&first_path).unwrap();
        assert!(handle.len() > 0);
        let data = handle.read_bytes(0..handle.len()).unwrap();
        assert_eq!(data.len(), handle.len());
    }

    #[tokio::test]
    async fn test_atomic_read_write() {
        let dir = make_test_dir().await;

        // atomic_write + atomic_read
        dir.atomic_write(Path::new("meta.json"), b"{}").unwrap();
        let data = dir.atomic_read(Path::new("meta.json")).unwrap();
        assert_eq!(&data, b"{}");
    }

    #[tokio::test]
    async fn test_file_not_found() {
        let dir = make_test_dir().await;
        assert!(dir.get_file_handle(Path::new("missing.txt")).is_err());
    }
}
