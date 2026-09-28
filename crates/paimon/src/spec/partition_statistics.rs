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

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Partition-level statistics for snapshot commits.
///
/// Reference: [org.apache.paimon.partition.PartitionStatistics](https://github.com/apache/paimon)
/// and [pypaimon snapshot_commit.py PartitionStatistics](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/snapshot/snapshot_commit.py)
///
/// The same shape reports what a partition holds when a Format Table's partitions are measured.
/// There a negative field is unknown rather than a decrement, which is why
/// `last_file_creation_time` is signed like the other counts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionStatistics {
    pub spec: HashMap<String, String>,
    pub record_count: i64,
    pub file_size_in_bytes: i64,
    pub file_count: i64,
    pub last_file_creation_time: i64,
    /// Defaults to 0 when absent, e.g. statistics serialized by an older Paimon
    /// version that predates this field (matches Java `PartitionStatistics`).
    #[serde(default)]
    pub total_buckets: i32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_without_total_buckets() {
        // Statistics written by an older Paimon version omit `totalBuckets`; it
        // must default to 0 rather than failing to deserialize.
        let json = r#"{
            "spec": {"dt": "2024-01-01"},
            "recordCount": 10,
            "fileSizeInBytes": 2048,
            "fileCount": 3,
            "lastFileCreationTime": 1700000000000
        }"#;
        let stats: PartitionStatistics = serde_json::from_str(json).unwrap();
        assert_eq!(stats.total_buckets, 0);
        assert_eq!(stats.record_count, 10);
        assert_eq!(stats.file_count, 3);
    }

    #[test]
    fn deserializes_with_total_buckets() {
        let json = r#"{
            "spec": {},
            "recordCount": 1,
            "fileSizeInBytes": 1,
            "fileCount": 1,
            "lastFileCreationTime": 0,
            "totalBuckets": 8
        }"#;
        let stats: PartitionStatistics = serde_json::from_str(json).unwrap();
        assert_eq!(stats.total_buckets, 8);
    }
}
