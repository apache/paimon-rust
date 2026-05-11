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

//! Mirrors Java [Partition](https://github.com/apache/paimon/blob/release-1.4/paimon-api/src/main/java/org/apache/paimon/partition/Partition.java)
//! and its [PartitionStatistics](https://github.com/apache/paimon/blob/release-1.4/paimon-api/src/main/java/org/apache/paimon/partition/PartitionStatistics.java) base.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A partition with aggregate statistics and audit metadata, as tracked by the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Partition {
    pub spec: HashMap<String, String>,
    pub record_count: i64,
    pub file_size_in_bytes: i64,
    pub file_count: i64,
    pub last_file_creation_time: i64,
    #[serde(default)]
    pub total_buckets: i32,
    #[serde(default)]
    pub done: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub options: Option<HashMap<String, String>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_roundtrip_minimal() {
        let original = Partition {
            spec: HashMap::from([("dt".to_string(), "2024-01-01".to_string())]),
            record_count: 100,
            file_size_in_bytes: 2048,
            file_count: 3,
            last_file_creation_time: 1700000000000,
            total_buckets: 4,
            done: false,
            created_at: None,
            created_by: None,
            updated_at: None,
            updated_by: None,
            options: None,
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: Partition = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_partition_roundtrip_full() {
        let original = Partition {
            spec: HashMap::from([
                ("dt".to_string(), "2024-01-01".to_string()),
                ("hr".to_string(), "12".to_string()),
            ]),
            record_count: 100,
            file_size_in_bytes: 2048,
            file_count: 3,
            last_file_creation_time: 1700000000000,
            total_buckets: 4,
            done: true,
            created_at: Some(1699000000000),
            created_by: Some("user-a".to_string()),
            updated_at: Some(1700000000000),
            updated_by: Some("user-b".to_string()),
            options: Some(HashMap::from([("k".to_string(), "v".to_string())])),
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: Partition = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_partition_decode_with_unknown_fields() {
        let json = r#"{
            "spec": {"dt": "2024-01-01"},
            "recordCount": 1,
            "fileSizeInBytes": 1,
            "fileCount": 1,
            "lastFileCreationTime": 0,
            "newField": "ignored"
        }"#;
        let decoded: Partition = serde_json::from_str(json).unwrap();
        assert_eq!(decoded.spec.get("dt"), Some(&"2024-01-01".to_string()));
    }
}
