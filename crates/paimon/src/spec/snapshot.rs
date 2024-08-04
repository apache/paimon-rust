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
    commit_user: String,
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

    #[test]
    fn test_snapshot_creation() {
        let snapshot = Snapshot::builder()
            .version(1)
            .id(1001)
            .schema_id(2002)
            .base_manifest_list("base_manifest".to_string())
            .delta_manifest_list("delta_manifest".to_string())
            .commit_user("user1".to_string())
            .build();

        assert_eq!(snapshot.version(), 1);
        assert_eq!(snapshot.id(), 1001);
        assert_eq!(snapshot.schema_id(), 2002);
        assert_eq!(snapshot.base_manifest_list(), "base_manifest");
        assert_eq!(snapshot.delta_manifest_list(), "delta_manifest");
        assert_eq!(snapshot.commit_user(), "user1");
        assert!(snapshot.changelog_manifest_list().is_none());
        assert!(snapshot.index_manifest().is_none());
        assert!(snapshot.total_record_count().is_none());
        assert!(snapshot.delta_record_count().is_none());
        assert!(snapshot.changelog_record_count().is_none());
        assert!(snapshot.watermark().is_none());
        assert!(snapshot.statistics().is_none());
    }

    #[test]
    fn test_snapshot_with_optional_fields() {
        let snapshot = Snapshot::builder()
            .version(2)
            .id(1002)
            .schema_id(2003)
            .base_manifest_list("base_manifest_v2".to_string())
            .delta_manifest_list("delta_manifest_v2".to_string())
            .changelog_manifest_list(Some("changelog_manifest_v2".to_string()))
            .index_manifest(Some("index_manifest_v2".to_string()))
            .commit_user("user2".to_string())
            .total_record_count(Some(500))
            .delta_record_count(Some(200))
            .changelog_record_count(Some(50))
            .watermark(Some(123456789))
            .statistics(Some("statistics_v2".to_string()))
            .build();

        assert_eq!(snapshot.version(), 2);
        assert_eq!(snapshot.id(), 1002);
        assert_eq!(snapshot.schema_id(), 2003);
        assert_eq!(snapshot.base_manifest_list(), "base_manifest_v2");
        assert_eq!(snapshot.delta_manifest_list(), "delta_manifest_v2");
        assert_eq!(
            snapshot.changelog_manifest_list(),
            Some("changelog_manifest_v2")
        );
        assert_eq!(snapshot.index_manifest(), Some("index_manifest_v2"));
        assert_eq!(snapshot.commit_user(), "user2");
        assert_eq!(snapshot.total_record_count(), Some(500));
        assert_eq!(snapshot.delta_record_count(), Some(200));
        assert_eq!(snapshot.changelog_record_count(), Some(50));
        assert_eq!(snapshot.watermark(), Some(123456789));
        assert_eq!(snapshot.statistics(), Some("statistics_v2"));
    }

    #[test]
    fn test_snapshot_default_values() {
        let snapshot = Snapshot::builder()
            .version(3)
            .id(1003)
            .schema_id(2004)
            .base_manifest_list("base_manifest_v3".to_string())
            .delta_manifest_list("delta_manifest_v3".to_string())
            .commit_user("user3".to_string())
            .build();

        assert_eq!(snapshot.version(), 3);
        assert_eq!(snapshot.id(), 1003);
        assert_eq!(snapshot.schema_id(), 2004);
        assert_eq!(snapshot.base_manifest_list(), "base_manifest_v3");
        assert_eq!(snapshot.delta_manifest_list(), "delta_manifest_v3");
        assert_eq!(snapshot.commit_user(), "user3");
        assert!(snapshot.changelog_manifest_list().is_none());
        assert!(snapshot.index_manifest().is_none());
        assert!(snapshot.total_record_count().is_none());
        assert!(snapshot.delta_record_count().is_none());
        assert!(snapshot.changelog_record_count().is_none());
        assert!(snapshot.watermark().is_none());
        assert!(snapshot.statistics().is_none());
    }
}
