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

use crate::error::*;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use opendal::Operator;
use snafu::ResultExt;
use url::Url;

use super::Storage;

#[derive(Clone, Debug)]
pub struct FileIO {
    storage: Arc<Storage>,
}

impl FileIO {
    /// Try to infer file io scheme from path.
    ///
    /// The input HashMap is paimon-java's [`Options`](https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/options/Options.java#L60)
    pub fn from_url(path: &str) -> crate::Result<FileIOBuilder> {
        let url = Url::parse(path).map_err(|_| Error::ConfigInvalid {
            message: format!("Invalid URL: {}", path),
        })?;

        Ok(FileIOBuilder::new(url.scheme()))
    }

    /// Create a new input file to read data.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L76>
    pub fn new_input(&self, path: &str) -> crate::Result<InputFile> {
        let (op, relative_path) = self.storage.create(path)?;
        let path = path.to_string();
        let relative_path_pos = path.len() - relative_path.len();
        Ok(InputFile {
            op,
            path,
            relative_path_pos,
        })
    }

    /// Create a new output file to write data.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L87>
    pub fn new_output(&self, path: &str) -> Result<OutputFile> {
        let (op, relative_path) = self.storage.create(path)?;
        let path = path.to_string();
        let relative_path_pos = path.len() - relative_path.len();
        Ok(OutputFile {
            op,
            path,
            relative_path_pos,
        })
    }

    /// Return a file status object that represents the path.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L97>
    pub async fn get_status(&self, path: &str) -> Result<FileStatus> {
        let (op, relative_path) = self.storage.create(path)?;
        let meta = op.stat(relative_path).await.context(IoUnexpectedSnafu {
            message: format!("Failed to get file status for '{}'", path),
        })?;

        Ok(FileStatus {
            size: meta.content_length(),
            is_dir: meta.is_dir(),
            last_modified: meta.last_modified(),
            path: path.to_string(),
        })
    }

    /// List the statuses of the files/directories in the given path if the path is a directory.
    ///
    /// References: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L105>
    ///
    /// FIXME: how to handle large dir? Better to return a stream instead?
    pub async fn list_status(&self, path: &str) -> Result<Vec<FileStatus>> {
        let (op, relative_path) = self.storage.create(path)?;

        let entries = op.list(relative_path).await.context(IoUnexpectedSnafu {
            message: format!("Failed to list files in '{}'", path),
        })?;

        let mut statuses = Vec::new();

        for entry in entries {
            let meta = entry.metadata();
            statuses.push(FileStatus {
                size: meta.content_length(),
                is_dir: meta.is_dir(),
                path: path.to_string(),
                last_modified: meta.last_modified(),
            });
        }

        Ok(statuses)
    }

    /// Check if exists.
    ///
    /// References: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L128>
    pub async fn exists(&self, path: &str) -> Result<bool> {
        let (op, relative_path) = self.storage.create(path)?;

        op.is_exist(relative_path).await.context(IoUnexpectedSnafu {
            message: format!("Failed to check existence of '{}'", path),
        })
    }

    /// Delete a file.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L139>
    pub async fn delete_file(&self, path: &str) -> Result<()> {
        let (op, relative_path) = self.storage.create(path)?;

        op.delete(relative_path).await.context(IoUnexpectedSnafu {
            message: format!("Failed to delete file '{}'", path),
        })?;

        Ok(())
    }

    /// Delete a dir recursively.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L139>
    pub async fn delete_dir(&self, path: &str) -> Result<()> {
        let (op, relative_path) = self.storage.create(path)?;

        op.remove_all(relative_path)
            .await
            .context(IoUnexpectedSnafu {
                message: format!("Failed to delete directory '{}'", path),
            })?;

        Ok(())
    }

    /// Make the given file and all non-existent parents into directories.
    ///
    /// Has the semantics of Unix 'mkdir -p'. Existence of the directory hierarchy is not an error.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L150>
    pub async fn mkdirs(&self, path: &str) -> Result<()> {
        let (op, relative_path) = self.storage.create(path)?;

        op.create_dir(relative_path)
            .await
            .context(IoUnexpectedSnafu {
                message: format!("Failed to create directory '{}'", path),
            })?;

        Ok(())
    }

    /// Renames the file/directory src to dst.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L159>
    pub async fn rename(&self, src: &str, dst: &str) -> Result<()> {
        let (op_src, relative_path_src) = self.storage.create(src)?;
        let (_, relative_path_dst) = self.storage.create(dst)?;

        op_src
            .rename(relative_path_src, relative_path_dst)
            .await
            .context(IoUnexpectedSnafu {
                message: format!("Failed to rename '{}' to '{}'", src, dst),
            })?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct FileIOBuilder {
    scheme_str: Option<String>,
    props: HashMap<String, String>,
}

impl FileIOBuilder {
    pub fn new(scheme_str: impl ToString) -> Self {
        Self {
            scheme_str: Some(scheme_str.to_string()),
            props: HashMap::default(),
        }
    }

    pub fn new_fs_io_builder() -> Self {
        Self {
            scheme_str: None,
            props: HashMap::default(),
        }
    }

    pub(crate) fn into_parts(self) -> (String, HashMap<String, String>) {
        (self.scheme_str.unwrap_or_default(), self.props)
    }

    pub fn with_prop(mut self, key: impl ToString, value: impl ToString) -> Self {
        self.props.insert(key.to_string(), value.to_string());
        self
    }

    pub fn with_props(
        mut self,
        args: impl IntoIterator<Item = (impl ToString, impl ToString)>,
    ) -> Self {
        self.props
            .extend(args.into_iter().map(|e| (e.0.to_string(), e.1.to_string())));
        self
    }

    pub fn build(self) -> crate::Result<FileIO> {
        let storage = Storage::build(self)?;
        Ok(FileIO {
            storage: Arc::new(storage),
        })
    }
}

#[async_trait::async_trait]
pub trait FileRead: Send + Unpin + 'static {
    async fn read(&self, range: Range<u64>) -> crate::Result<Bytes>;
}

#[async_trait::async_trait]
impl FileRead for opendal::Reader {
    async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
        // TODO: build a error type
        Ok(opendal::Reader::read(self, range)
            .await
            .expect("read error")
            .to_bytes())
    }
}

#[async_trait::async_trait]
pub trait FileWrite: Send + Unpin + 'static {
    async fn write(&mut self, bs: Bytes) -> crate::Result<()>;

    async fn close(&mut self) -> crate::Result<()>;
}

#[async_trait::async_trait]
impl FileWrite for opendal::Writer {
    async fn write(&mut self, bs: Bytes) -> crate::Result<()> {
        Ok(opendal::Writer::write(self, bs).await?)
    }

    async fn close(&mut self) -> crate::Result<()> {
        Ok(opendal::Writer::close(self).await?)
    }
}

#[derive(Clone, Debug)]
pub struct FileStatus {
    pub size: u64,
    pub is_dir: bool,
    pub path: String,
    pub last_modified: Option<DateTime<Utc>>,
}

#[derive(Debug)]
pub struct InputFile {
    op: Operator,
    path: String,
    relative_path_pos: usize,
}

impl InputFile {
    pub fn location(&self) -> &str {
        &self.path
    }

    pub async fn exists(&self) -> crate::Result<bool> {
        Ok(self
            .op
            .is_exist(&self.path[self.relative_path_pos..])
            .await?)
    }

    pub async fn metadata(&self) -> crate::Result<FileStatus> {
        let meta = self.op.stat(&self.path[self.relative_path_pos..]).await?;

        Ok(FileStatus {
            size: meta.content_length(),
            is_dir: meta.is_dir(),
            path: self.path.clone(),
            last_modified: meta.last_modified(),
        })
    }

    pub async fn read(&self) -> crate::Result<Bytes> {
        Ok(self
            .op
            .read(&self.path[self.relative_path_pos..])
            .await?
            .to_bytes())
    }

    pub async fn reader(&self) -> crate::Result<impl FileRead> {
        Ok(self.op.reader(&self.path[self.relative_path_pos..]).await?)
    }
}

#[derive(Debug)]
pub struct OutputFile {
    op: Operator,
    path: String,
    relative_path_pos: usize,
}

impl OutputFile {
    pub fn location(&self) -> &str {
        &self.path
    }

    pub async fn exists(&self) -> crate::Result<bool> {
        Ok(self
            .op
            .is_exist(&self.path[self.relative_path_pos..])
            .await?)
    }

    pub fn to_input_file(self) -> InputFile {
        InputFile {
            op: self.op,
            path: self.path,
            relative_path_pos: self.relative_path_pos,
        }
    }

    pub async fn write(&self, bs: Bytes) -> crate::Result<()> {
        let mut writer = self.writer().await?;
        writer.write(bs).await?;
        writer.close().await
    }

    pub async fn writer(&self) -> crate::Result<Box<dyn FileWrite>> {
        Ok(Box::new(
            self.op.writer(&self.path[self.relative_path_pos..]).await?,
        ))
    }
}

#[cfg(test)]
mod file_action_test {
    use std::fs;

    use super::*;
    use bytes::Bytes;

    fn setup_memory_file_io() -> FileIO {
        let storage = Storage::Memory;
        FileIO {
            storage: Arc::new(storage),
        }
    }

    fn setup_fs_file_io() -> FileIO {
        let storage = Storage::LocalFs;
        FileIO {
            storage: Arc::new(storage),
        }
    }

    async fn common_test_get_status(file_io: &FileIO, path: &str) {
        let output = file_io.new_output(path).unwrap();
        output.write(Bytes::from("hello world")).await.unwrap();

        let status = file_io.get_status(path).await.unwrap();
        assert_eq!(status.size, 11);

        file_io.delete_file(path).await.unwrap();
    }

    async fn common_test_exists(file_io: &FileIO, path: &str) {
        let output = file_io.new_output(path).unwrap();
        output.write(Bytes::from("hello world")).await.unwrap();

        let exists = file_io.exists(path).await.unwrap();
        assert!(exists);

        file_io.delete_file(path).await.unwrap();
    }

    async fn common_test_delete_file(file_io: &FileIO, path: &str) {
        let output = file_io.new_output(path).unwrap();
        output.write(Bytes::from("hello world")).await.unwrap();

        file_io.delete_file(path).await.unwrap();

        let exists = file_io.exists(path).await.unwrap();
        assert!(!exists);
    }

    async fn common_test_mkdirs(file_io: &FileIO, dir_path: &str) {
        file_io.mkdirs(dir_path).await.unwrap();

        let exists = file_io.exists(dir_path).await.unwrap();
        assert!(exists);

        let _ = fs::remove_dir_all(dir_path.strip_prefix("file:/").unwrap());
    }

    async fn common_test_rename(file_io: &FileIO, src: &str, dst: &str) {
        let output = file_io.new_output(src).unwrap();
        output.write(Bytes::from("hello world")).await.unwrap();

        file_io.rename(src, dst).await.unwrap();

        let exists_old = file_io.exists(src).await.unwrap();
        let exists_new = file_io.exists(dst).await.unwrap();
        assert!(!exists_old);
        assert!(exists_new);

        file_io.delete_file(dst).await.unwrap();
    }

    #[tokio::test]
    async fn test_delete_file_memory() {
        let file_io = setup_memory_file_io();
        common_test_delete_file(&file_io, "memory:/test_file_delete_mem").await;
    }

    #[tokio::test]
    async fn test_get_status_fs() {
        let file_io = setup_fs_file_io();
        common_test_get_status(&file_io, "file:/tmp/test_file_get_status_fs").await;
    }

    #[tokio::test]
    async fn test_exists_fs() {
        let file_io = setup_fs_file_io();
        common_test_exists(&file_io, "file:/tmp/test_file_exists_fs").await;
    }

    #[tokio::test]
    async fn test_delete_file_fs() {
        let file_io = setup_fs_file_io();
        common_test_delete_file(&file_io, "file:/tmp/test_file_delete_fs").await;
    }

    #[tokio::test]
    async fn test_mkdirs_fs() {
        let file_io = setup_fs_file_io();
        common_test_mkdirs(&file_io, "file:/tmp/test_fs_dir/").await;
    }

    #[tokio::test]
    async fn test_rename_fs() {
        let file_io = setup_fs_file_io();
        common_test_rename(
            &file_io,
            "file:/tmp/test_file_fs_z",
            "file:/tmp/new_test_file_fs_o",
        )
        .await;
    }
}

#[cfg(test)]
mod input_output_test {
    use super::*;
    use bytes::Bytes;

    fn setup_memory_file_io() -> FileIO {
        let storage = Storage::Memory;
        FileIO {
            storage: Arc::new(storage),
        }
    }

    fn setup_fs_file_io() -> FileIO {
        let storage = Storage::LocalFs;
        FileIO {
            storage: Arc::new(storage),
        }
    }

    async fn common_test_output_file_write_and_read(file_io: &FileIO, path: &str) {
        let output = file_io.new_output(path).unwrap();
        output.write(Bytes::from("hello world")).await.unwrap();

        let input = output.to_input_file();
        let content = input.read().await.unwrap();

        assert_eq!(&content[..], b"hello world");

        file_io.delete_file(path).await.unwrap();
    }

    async fn common_test_output_file_exists(file_io: &FileIO, path: &str) {
        let output = file_io.new_output(path).unwrap();
        output.write(Bytes::from("hello world")).await.unwrap();

        let exists = output.exists().await.unwrap();
        assert!(exists);

        file_io.delete_file(path).await.unwrap();
    }

    async fn common_test_input_file_metadata(file_io: &FileIO, path: &str) {
        let output = file_io.new_output(path).unwrap();
        output.write(Bytes::from("hello world")).await.unwrap();

        let input = output.to_input_file();
        let metadata = input.metadata().await.unwrap();

        assert_eq!(metadata.size, 11);

        file_io.delete_file(path).await.unwrap();
    }

    async fn common_test_input_file_partial_read(file_io: &FileIO, path: &str) {
        let output = file_io.new_output(path).unwrap();
        output.write(Bytes::from("hello world")).await.unwrap();

        let input = output.to_input_file();
        let reader = input.reader().await.unwrap();
        let partial_content = reader.read(0..5).await.unwrap(); // 读取 "hello"

        assert_eq!(&partial_content[..], b"hello");

        file_io.delete_file(path).await.unwrap();
    }

    #[tokio::test]
    async fn test_output_file_write_and_read_memory() {
        let file_io = setup_memory_file_io();
        common_test_output_file_write_and_read(&file_io, "memory:/test_file_rw_mem").await;
    }

    #[tokio::test]
    async fn test_output_file_exists_memory() {
        let file_io = setup_memory_file_io();
        common_test_output_file_exists(&file_io, "memory:/test_file_exist_mem").await;
    }

    #[tokio::test]
    async fn test_input_file_metadata_memory() {
        let file_io = setup_memory_file_io();
        common_test_input_file_metadata(&file_io, "memory:/test_file_meta_mem").await;
    }

    #[tokio::test]
    async fn test_input_file_partial_read_memory() {
        let file_io = setup_memory_file_io();
        common_test_input_file_partial_read(&file_io, "memory:/test_file_part_read_mem").await;
    }

    #[tokio::test]
    async fn test_output_file_write_and_read_fs() {
        let file_io = setup_fs_file_io();
        common_test_output_file_write_and_read(&file_io, "file:/tmp/test_file_fs_rw").await;
    }

    #[tokio::test]
    async fn test_output_file_exists_fs() {
        let file_io = setup_fs_file_io();
        common_test_output_file_exists(&file_io, "file:/tmp/test_file_exists").await;
    }

    #[tokio::test]
    async fn test_input_file_metadata_fs() {
        let file_io = setup_fs_file_io();
        common_test_input_file_metadata(&file_io, "file:/tmp/test_file_meta").await;
    }

    #[tokio::test]
    async fn test_input_file_partial_read_fs() {
        let file_io = setup_fs_file_io();
        common_test_input_file_partial_read(&file_io, "file:/tmp/test_file_read_fs").await;
    }
}
