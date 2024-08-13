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

use chrono::offset::Utc;
use chrono::DateTime;
use opendal::services::Fs;
use opendal::{Metakey, Operator};
use snafu::ResultExt;

#[derive(Clone, Debug)]
pub struct FileIO {
    op: Operator,
}

impl FileIO {
    /// Create a new FileIO.
    ///
    /// The input HashMap is paimon-java's [`Options`](https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/options/Options.java#L60)
    ///
    /// TODO: Support building Operator from HashMap via options.
    pub fn new(_: HashMap<String, String>) -> Result<Self> {
        let op = Operator::new(Fs::default().root("/"))
            .context(IoUnexpectedSnafu {
                message: "Failed to create operator".to_string(),
            })?
            .finish();
        Ok(Self { op })
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
        let entries = self
            .op
            .list_with(path)
            .metakey(Metakey::ContentLength | Metakey::LastModified)
            .await
            .context(IoUnexpectedSnafu {
                message: "Failed to list file status".to_string(),
            })?;

        Ok(entries
            .into_iter()
            .map(|meta| FileStatus {
                size: meta.metadata().content_length(),
                is_dir: meta.metadata().is_dir(),
                last_modified: meta.metadata().last_modified(),
                path: format!("{}{}", path, meta.name()),
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

/// FileStatus represents the status of a file.
#[derive(Clone, Debug)]
pub struct FileStatus {
    pub size: u64,
    pub is_dir: bool,
    pub path: String,
    pub last_modified: Option<DateTime<Utc>>,
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
