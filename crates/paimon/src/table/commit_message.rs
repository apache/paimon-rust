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
    /// Per-partition bucket count for fixed-bucket postpone writes.
    pub total_buckets: Option<i32>,
    /// New data files to be added.
    pub new_files: Vec<DataFileMeta>,
    /// Snapshot id from which state-dependent write conflicts should be checked.
    pub check_from_snapshot: Option<i64>,
    /// New changelog files to be added.
    pub new_changelog_files: Vec<DataFileMeta>,
    /// New index files to be added (used by dynamic bucket mode).
    pub new_index_files: Vec<IndexFileMeta>,
    /// Index files to be removed from the current index manifest.
    pub deleted_index_files: Vec<IndexFileMeta>,
    /// Files to be deleted (copy-on-write rewrite: old files replaced by new_files).
    pub deleted_files: Vec<DataFileMeta>,
    fixed_bucket_overwrite: bool,
}

impl CommitMessage {
    pub fn new(partition: Vec<u8>, bucket: i32, new_files: Vec<DataFileMeta>) -> Self {
        Self {
            partition,
            bucket,
            total_buckets: None,
            new_files,
            check_from_snapshot: None,
            new_changelog_files: Vec::new(),
            new_index_files: Vec::new(),
            deleted_index_files: Vec::new(),
            deleted_files: Vec::new(),
            fixed_bucket_overwrite: false,
        }
    }

    pub(crate) fn mark_fixed_bucket_overwrite(&mut self) {
        self.fixed_bucket_overwrite = true;
    }

    pub(crate) fn is_fixed_bucket_overwrite(&self) -> bool {
        self.fixed_bucket_overwrite
    }
}
