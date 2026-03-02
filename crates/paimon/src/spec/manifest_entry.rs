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

use crate::spec::manifest_common::FileKind;
use crate::spec::DataFileMeta;
use serde::{Deserialize, Serialize};

/// The same {@link Identifier} indicates that the {@link ManifestEntry} refers to the same data file.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/manifest/FileEntry.java#L58>
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Identifier {
    pub partition: Vec<u8>,
    pub bucket: i32,
    pub level: i32,
    pub file_name: String,
}

/// Entry of a manifest file, representing an addition / deletion of a data file.
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/manifest/ManifestEntry.java>
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestEntry {
    #[serde(rename = "_KIND")]
    kind: FileKind,

    #[serde(rename = "_PARTITION", with = "serde_bytes")]
    partition: Vec<u8>,

    #[serde(rename = "_BUCKET")]
    bucket: i32,

    #[serde(rename = "_TOTAL_BUCKETS")]
    total_buckets: i32,

    #[serde(rename = "_FILE")]
    file: DataFileMeta,

    #[serde(rename = "_VERSION")]
    version: i32,
}

#[allow(dead_code)]
impl ManifestEntry {
    pub(crate) fn kind(&self) -> &FileKind {
        &self.kind
    }

    fn partition(&self) -> &Vec<u8> {
        &self.partition
    }

    pub(crate) fn bucket(&self) -> i32 {
        self.bucket
    }

    fn level(&self) -> i32 {
        self.file.level
    }

    fn file_name(&self) -> &str {
        &self.file.file_name
    }

    fn min_key(&self) -> &Vec<u8> {
        &self.file.min_key
    }

    fn max_key(&self) -> &Vec<u8> {
        &self.file.max_key
    }

    fn identifier(&self) -> Identifier {
        Identifier {
            partition: self.partition.clone(),
            bucket: self.bucket,
            level: self.file.level,
            file_name: self.file.file_name.clone(),
        }
    }

    pub fn total_buckets(&self) -> i32 {
        self.total_buckets
    }

    pub fn file(&self) -> &DataFileMeta {
        &self.file
    }

    pub fn new(
        kind: FileKind,
        partition: Vec<u8>,
        bucket: i32,
        total_buckets: i32,
        file: DataFileMeta,
        version: i32,
    ) -> Self {
        ManifestEntry {
            kind,
            partition,
            bucket,
            total_buckets,
            file,
            version,
        }
    }
}
