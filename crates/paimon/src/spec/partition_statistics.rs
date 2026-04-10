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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionStatistics {
    pub spec: HashMap<String, String>,
    pub record_count: i64,
    pub file_size_in_bytes: i64,
    pub file_count: i64,
    pub last_file_creation_time: u64,
    pub total_buckets: i32,
}
