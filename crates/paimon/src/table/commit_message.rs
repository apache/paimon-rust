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

use crate::spec::DataFileMeta;
use crate::spec::IndexFileMeta;

/// A commit message representing new files to be committed for a specific partition and bucket.
///
/// Reference: [org.apache.paimon.table.sink.CommitMessage](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/table/sink/CommitMessageImpl.java)
#[derive(Debug, Clone)]
pub struct CommitMessage {
    /// Binary row bytes for the partition.
    pub partition: Vec<u8>,
    /// Bucket id.
    pub bucket: i32,
    /// New data files to be added.
    pub new_files: Vec<DataFileMeta>,
    /// New index files to be added (used by dynamic bucket mode).
    pub new_index_files: Vec<IndexFileMeta>,
}

impl CommitMessage {
    pub fn new(partition: Vec<u8>, bucket: i32, new_files: Vec<DataFileMeta>) -> Self {
        Self {
            partition,
            bucket,
            new_files,
            new_index_files: Vec::new(),
        }
    }
}
