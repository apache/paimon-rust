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

//! FileIO trait for abstracting file I/O operations.
//!
//! This trait allows different FileIO implementations (e.g., DefaultFileIO,
//! RESTTokenFileIO) to be used interchangeably.

use crate::Result;

use super::{FileStatus, InputFile, OutputFile};

/// A trait for providing file I/O operations.
///
/// This trait abstracts file system operations, allowing implementations
/// that may need to refresh credentials (like RESTTokenFileIO) or perform
/// other operations before each file access.
#[async_trait::async_trait]
pub trait FileIO: Send + Sync + std::fmt::Debug {
    /// Create a new input file to read data.
    async fn new_input(&self, path: &str) -> Result<InputFile>;

    /// Create a new output file to write data.
    async fn new_output(&self, path: &str) -> Result<OutputFile>;

    /// Return a file status object that represents the path.
    async fn get_status(&self, path: &str) -> Result<FileStatus>;

    /// List the statuses of the files/directories in the given path if the path is a directory.
    async fn list_status(&self, path: &str) -> Result<Vec<FileStatus>>;

    /// Check if exists.
    async fn exists(&self, path: &str) -> Result<bool>;

    /// Delete a file.
    async fn delete_file(&self, path: &str) -> Result<()>;

    /// Delete a dir recursively.
    async fn delete_dir(&self, path: &str) -> Result<()>;

    /// Make the given file and all non-existent parents into directories.
    async fn mkdirs(&self, path: &str) -> Result<()>;

    /// Renames the file/directory src to dst.
    async fn rename(&self, src: &str, dst: &str) -> Result<()>;
}
