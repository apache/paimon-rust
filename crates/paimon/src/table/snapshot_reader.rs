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

//! Per-snapshot planning, matching Java `SnapshotReader` and `ScanMode`.

use super::{Plan, TableScan};
use crate::Result;
use std::sync::Arc;

/// Which manifest lists to read from a single snapshot.
///
/// Unlike a batch incremental range, a DELTA or CHANGELOG reader does not
/// select snapshots by commit kind. The caller's starting/follow-up scanner
/// decides which snapshot to consume (Java `FollowUpScanner`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ScanMode {
    /// Full live file set, with batch split packing, including level zero.
    #[default]
    All,
    /// This snapshot's delta manifest list, with streaming split packing.
    Delta,
    /// This snapshot's physical changelog, including an OVERWRITE changelog.
    Changelog,
}

#[derive(Clone)]
pub(super) struct BucketFilter(Arc<dyn Fn(i32) -> Result<bool> + Send + Sync>);

impl std::fmt::Debug for BucketFilter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("BucketFilter")
    }
}

impl BucketFilter {
    pub(super) fn test(&self, bucket: i32) -> Result<bool> {
        (self.0)(bucket)
    }
}

/// Plan one explicitly selected snapshot for a starting or follow-up scan.
///
/// Create this from a read builder to retain its filter, read type, row ranges
/// and limit. ALL uses the full live file set; DELTA and CHANGELOG preserve
/// physical events. Snapshot selection is independent of table startup options.
/// An explicitly requested missing snapshot is an error, never a latest scan.
/// This lower-level reader currently refuses query-auth tables, like the
/// existing incremental planner; it cannot bypass authorization.
#[derive(Debug, Clone)]
pub struct SnapshotReader<'a> {
    scan: TableScan<'a>,
    snapshot_id: Option<i64>,
    mode: ScanMode,
}

impl<'a> SnapshotReader<'a> {
    pub(crate) fn new(scan: TableScan<'a>) -> Self {
        Self {
            scan,
            snapshot_id: None,
            mode: ScanMode::All,
        }
    }

    /// Pin the snapshot instead of resolving the latest one at read time.
    pub fn with_snapshot(mut self, snapshot_id: i64) -> Result<Self> {
        if snapshot_id < 1 {
            return Err(crate::Error::DataInvalid {
                message: "snapshot id must be positive".into(),
                source: None,
            });
        }
        self.snapshot_id = Some(snapshot_id);
        Ok(self)
    }

    pub fn with_mode(mut self, mode: ScanMode) -> Self {
        self.mode = mode;
        self
    }

    /// Hide negative (unassigned) buckets, as Java starting/follow-up scanners
    /// require for postpone tables with a changelog producer.
    pub fn only_read_real_buckets(mut self) -> Self {
        self.scan = self.scan.only_read_snapshot_real_buckets();
        self
    }

    /// Select buckets before split packing and LIMIT. Replaces the old filter.
    /// Filter errors fail the read rather than returning a partially selected plan.
    pub fn with_bucket_filter(
        mut self,
        filter: impl Fn(i32) -> Result<bool> + Send + Sync + 'static,
    ) -> Self {
        self.scan = self
            .scan
            .with_snapshot_bucket_filter(BucketFilter(Arc::new(filter)));
        self
    }

    /// Use Java's file-name or bucket distribution before splitting.
    pub fn with_shard(mut self, index: usize, count: usize) -> Result<Self> {
        self.scan = self.scan.with_shard(index, count)?;
        Ok(self)
    }

    /// Read the configured manifest lists and build ordinary table splits.
    pub async fn read(&self) -> Result<Plan> {
        self.scan.read_snapshot(self.snapshot_id, self.mode).await
    }
}
