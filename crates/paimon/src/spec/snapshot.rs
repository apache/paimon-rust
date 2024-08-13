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
use typed_builder::TypedBuilder;

/// Snapshot for paimon.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/Snapshot.java#L68>.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, TypedBuilder)]
#[serde(rename_all = "camelCase")]
pub struct Snapshot {
    /// version of snapshot
    version: i32,
    id: i64,
    schema_id: i64,
    /// a manifest list recording all changes from the previous snapshots
    base_manifest_list: String,
    /// a manifest list recording all new changes occurred in this snapshot
    delta_manifest_list: String,
    /// a manifest list recording all changelog produced in this snapshot
    #[builder(default = None)]
    changelog_manifest_list: Option<String>,
    /// a manifest recording all index files of this table
    #[builder(default = None)]
    index_manifest: Option<String>,
    /// user who committed this snapshot
    commit_user: String,
    /// Mainly for snapshot deduplication.
    ///
    /// If multiple snapshots have the same commitIdentifier, reading from any of these snapshots
    /// must produce the same table.
    ///
    /// If snapshot A has a smaller commitIdentifier than snapshot B, then snapshot A must be
    /// committed before snapshot B, and thus snapshot A must contain older records than snapshot B.
    commit_identifier: i64,
    /// timestamp of this snapshot
    time_millis: u64,
    /// log offsets of all changes occurred in this snapshot
    #[builder(default = None)]
    log_offsets: Option<HashMap<i32, i64>>,
    /// record count of all changes occurred in this snapshot
    #[builder(default = None)]
    total_record_count: Option<i64>,
    /// record count of all new changes occurred in this snapshot
    #[builder(default = None)]
    delta_record_count: Option<i64>,
    /// record count of all changelog produced in this snapshot
    #[builder(default = None)]
    changelog_record_count: Option<i64>,
    /// watermark for input records
    #[builder(default = None)]
    watermark: Option<i64>,
    /// stats file name for statistics of this table
    #[builder(default = None)]
    statistics: Option<String>,
}

impl Snapshot {
    /// Get the version of this snapshot.
    #[inline]
    pub fn version(&self) -> i32 {
        self.version
    }

    /// Get the id of this snapshot.
    #[inline]
    pub fn id(&self) -> i64 {
        self.id
    }

    /// Get the schema id of this snapshot.
    #[inline]
    pub fn schema_id(&self) -> i64 {
        self.schema_id
    }

    /// Get the base manifest list of this snapshot.
    #[inline]
    pub fn base_manifest_list(&self) -> &str {
        &self.base_manifest_list
    }

    /// Get the delta manifest list of this snapshot.
    #[inline]
    pub fn delta_manifest_list(&self) -> &str {
        &self.delta_manifest_list
    }

    /// Get the changelog manifest list of this snapshot.
    #[inline]
    pub fn changelog_manifest_list(&self) -> Option<&str> {
        self.changelog_manifest_list.as_deref()
    }

    /// Get the index manifest of this snapshot.
    #[inline]
    pub fn index_manifest(&self) -> Option<&str> {
        self.index_manifest.as_deref()
    }

    /// Get the commit user of this snapshot.
    #[inline]
    pub fn commit_user(&self) -> &str {
        &self.commit_user
    }

    /// Get the commit time of this snapshot.
    #[inline]
    pub fn time_millis(&self) -> u64 {
        self.time_millis
    }

    /// Get the commit identifier of this snapshot.
    #[inline]
    pub fn commit_identifier(&self) -> i64 {
        self.commit_identifier
    }

    /// Get the log offsets of this snapshot.
    #[inline]
    pub fn log_offsets(&self) -> Option<&HashMap<i32, i64>> {
        self.log_offsets.as_ref()
    }

    /// Get the total record count of this snapshot.
    #[inline]
    pub fn total_record_count(&self) -> Option<i64> {
        self.total_record_count
    }

    /// Get the delta record count of this snapshot.
    #[inline]
    pub fn delta_record_count(&self) -> Option<i64> {
        self.delta_record_count
    }

    /// Get the changelog record count of this snapshot.
    #[inline]
    pub fn changelog_record_count(&self) -> Option<i64> {
        self.changelog_record_count
    }

    /// Get the watermark of this snapshot.
    #[inline]
    pub fn watermark(&self) -> Option<i64> {
        self.watermark
    }

    /// Get the statistics of this snapshot.
    #[inline]
    pub fn statistics(&self) -> Option<&str> {
        self.statistics.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_snapshot_serialization_deserialization() {
        let data = r#"
        {
          "version" : 3,
          "id" : 1,
          "schemaId" : 0,
          "baseManifestList" : "manifest-list-4091b92b-01d3-4b91-8e47-8c9f61847d2f-0",
          "deltaManifestList" : "manifest-list-4091b92b-01d3-4b91-8e47-8c9f61847d2f-1",
          "changelogManifestList" : null,
          "indexManifest" : "index-manifest-55e3e815-08ab-4a70-a808-a8df5d275cb4-0",
          "commitUser" : "cf568e07-05ad-4943-b4bd-37461bc58729",
          "commitIdentifier" : 9223372036854775807,
          "timeMillis" : 1721287833568,
          "logOffsets" : { },
          "totalRecordCount" : 1,
          "deltaRecordCount" : 1,
          "changelogRecordCount" : 0,
          "watermark" : -9223372036854775808
        }
        "#;

        let snapshot: Snapshot =
            serde_json::from_str(data).expect("Failed to deserialize Snapshot");

        assert_eq!(snapshot.version(), 3);
        assert_eq!(snapshot.id(), 1);
        assert_eq!(snapshot.schema_id(), 0);
        assert_eq!(
            snapshot.base_manifest_list(),
            "manifest-list-4091b92b-01d3-4b91-8e47-8c9f61847d2f-0"
        );
        assert_eq!(
            snapshot.delta_manifest_list(),
            "manifest-list-4091b92b-01d3-4b91-8e47-8c9f61847d2f-1"
        );
        assert_eq!(snapshot.changelog_manifest_list(), None);
        assert_eq!(
            snapshot.index_manifest(),
            Some("index-manifest-55e3e815-08ab-4a70-a808-a8df5d275cb4-0")
        );
        assert_eq!(
            snapshot.commit_user(),
            "cf568e07-05ad-4943-b4bd-37461bc58729"
        );
        assert_eq!(snapshot.commit_identifier(), 9223372036854775807);
        assert_eq!(snapshot.time_millis(), 1721287833568);
        assert!(snapshot.changelog_manifest_list().is_none());
        assert_eq!(snapshot.total_record_count(), Some(1));
        assert_eq!(snapshot.delta_record_count(), Some(1));
        assert_eq!(snapshot.changelog_record_count(), Some(0));
        assert_eq!(snapshot.watermark(), Some(-9223372036854775808));

        let serialized = serde_json::to_string(&snapshot).expect("Failed to serialize Snapshot");

        let deserialized: Snapshot =
            serde_json::from_str(&serialized).expect("Failed to deserialize serialized Snapshot");

        assert_eq!(snapshot, deserialized);
    }
}
