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

//! Table rollback targets, matching Java's snapshot and tag instants.

use serde::{Deserialize, Serialize};

/// A snapshot or tag to which a catalog rolls back a table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Instant {
    #[serde(rename = "snapshot")]
    Snapshot {
        #[serde(rename = "snapshotId")]
        snapshot_id: i64,
    },
    #[serde(rename = "tag")]
    Tag {
        #[serde(rename = "tagName")]
        tag_name: String,
    },
}

impl Instant {
    pub fn snapshot(snapshot_id: i64) -> Self {
        Self::Snapshot { snapshot_id }
    }

    pub fn tag(tag_name: impl Into<String>) -> Self {
        Self::Tag {
            tag_name: tag_name.into(),
        }
    }
}
