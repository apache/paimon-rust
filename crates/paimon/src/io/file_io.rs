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
use opendal::services::MemoryConfig;
use opendal::{Metakey, Operator};
use snafu::ResultExt;
use url::Url;

use super::Storage;

#[derive(Clone, Debug)]
pub struct FileIO {
    // op: Operator,
    inner_stroage: Arc<Storage>,
}

impl FileIO {
    /// Try to infer file io scheme from path.
    ///
    /// The input HashMap is paimon-java's [`Options`](https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/options/Options.java#L60)
    pub fn from_path(path: impl AsRef<str>) -> Result<FileIOBuilder> {
        let url = Url::parse(path.as_ref())
            .map_err(Error::from)
            .or_else(|e| {
                Url::from_file_path(path.as_ref()).map_err(|_| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Input is neither a valid url nor path",
                    )
                    .with_context("input", path.as_ref().to_string())
                    .with_source(e)
                })
            })?;

        Ok(FileIOBuilder::new(url.scheme()))
    }

    /// Create a new input file to read data.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L76>
    pub fn new_input(&self, path: &str) -> InputFile {
        InputFile {
            _op: self.op.clone(),
            path: path.to_string(),
        }
    }

    /// Create a new output file to write data.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L87>
    pub fn new_output(&self, path: &str) -> OutputFile {
        OutputFile {
            _op: self.op.clone(),
            path: path.to_string(),
        }
    }

    /// Return a file status object that represents the path.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L97>
    pub async fn get_status(&self, path: &str) -> Result<FileStatus> {
        let meta = self.op.stat(path).await.context(IoUnexpectedSnafu {
            message: "Failed to get file status".to_string(),
        })?;

        Ok(FileStatus {
            size: meta.content_length(),
        })
    }

    /// List the statuses of the files/directories in the given path if the path is a directory.
    ///
    /// References: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L105>
    ///
    /// FIXME: how to handle large dir? Better to return a stream instead?
    pub async fn list_status(&self, path: &str) -> Result<Vec<FileStatus>> {
        let entries = self
            .op
            .list_with(path)
            .metakey(Metakey::ContentLength)
            .await
            .context(IoUnexpectedSnafu {
                message: "Failed to list file status".to_string(),
            })?;

        Ok(entries
            .into_iter()
            .map(|meta| FileStatus {
                size: meta.metadata().content_length(),
            })
            .collect())
    }

    /// Check if exists.
    ///
    /// References: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L128>
    pub async fn exists(&self, path: &str) -> Result<bool> {
        self.op.is_exist(path).await.context(IoUnexpectedSnafu {
            message: "Failed to check file existence".to_string(),
        })
    }

    /// Delete a file.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L139>
    pub async fn delete_file(&self, path: &str) -> Result<()> {
        self.op.delete(path).await.context(IoUnexpectedSnafu {
            message: "Failed to delete file".to_string(),
        })?;

        Ok(())
    }

    /// Delete a dir recursively.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L139>
    pub async fn delete_dir(&self, path: &str) -> Result<()> {
        self.op.remove_all(path).await.context(IoUnexpectedSnafu {
            message: "Failed to delete dir".to_string(),
        })?;
        Ok(())
    }

    /// Make the given file and all non-existent parents into directories.
    ///
    /// Has the semantics of Unix 'mkdir -p'. Existence of the directory hierarchy is not an error.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L150>
    pub async fn mkdirs(&self, path: &str) -> Result<()> {
        self.op.create_dir(path).await.context(IoUnexpectedSnafu {
            message: "Failed to create dir".to_string(),
        })?;
        Ok(())
    }

    /// Renames the file/directory src to dst.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L159>
    pub async fn rename(&self, src: &str, dst: &str) -> Result<()> {
        self.op.rename(src, dst).await.context(IoUnexpectedSnafu {
            message: "Failed to rename file".to_string(),
        })?;
        Ok(())
    }
}

/// Builder for [`FileIO`].
#[derive(Debug)]
pub struct FileIOBuilder {
    /// This is used to infer scheme of operator.
    ///
    /// If this is `None`, then [`FileIOBuilder::build`](FileIOBuilder::build) will build a local file io.
    scheme_str: Option<String>,
    /// Arguments for operator.
    props: HashMap<String, String>,
}

impl FileIOBuilder {
    /// Creates a new builder with scheme.
    pub fn new(scheme_str: impl ToString) -> Self {
        Self {
            scheme_str: Some(scheme_str.to_string()),
            props: HashMap::default(),
        }
    }

    /// Creates a new builder for local file io.
    pub fn new_fs_io() -> Self {
        Self {
            scheme_str: None,
            props: HashMap::default(),
        }
    }

    /// Fetch the scheme string.
    ///
    /// The scheme_str will be empty if it's None.
    pub(crate) fn into_parts(self) -> (String, HashMap<String, String>) {
        (self.scheme_str.unwrap_or_default(), self.props)
    }

    /// Add argument for operator.
    pub fn with_prop(mut self, key: impl ToString, value: impl ToString) -> Self {
        self.props.insert(key.to_string(), value.to_string());
        self
    }

    /// Add argument for operator.
    pub fn with_props(
        mut self,
        args: impl IntoIterator<Item = (impl ToString, impl ToString)>,
    ) -> Self {
        self.props
            .extend(args.into_iter().map(|e| (e.0.to_string(), e.1.to_string())));
        self
    }

    /// Builds [`FileIO`].
    pub fn build(self) -> crate::Result<FileIO> {
        let storage = Storage::build(self)?;
        Ok(FileIO {
            inner: Arc::new(storage),
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

/// FileStatus represents the status of a file.
#[derive(Clone, Debug)]
pub struct FileStatus {
    pub size: u64,
}

/// Input file represents a file that can be read from.
#[derive(Clone, Debug)]
pub struct InputFile {
    _op: Operator,
    path: String,
}

impl InputFile {
    /// Get the path of given input file.
    pub fn path(&self) -> &str {
        &self.path
    }
}

/// Output file represents a file that can be written to.
#[derive(Clone, Debug)]
pub struct OutputFile {
    _op: Operator,
    path: String,
}

impl OutputFile {
    /// Get the path of given output file.
    pub fn path(&self) -> &str {
        &self.path
    }
}
