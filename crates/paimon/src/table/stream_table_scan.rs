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

//! Stateful snapshot selection and progress for continuous table reads.

use super::table_scan::SnapshotScanMode;
use super::{Plan, TableScan};
use crate::spec::{ChangelogProducer, CommitKind, CoreOptions, Snapshot};
use crate::{Error, Result};

/// Continuous table scan. `checkpoint` is the next snapshot to consume.
///
/// The first plan reads the latest full state. Follow-up plans read APPEND
/// deltas when no changelog producer is configured, or physical changelogs
/// otherwise. Empty follow-up plans and irrelevant commits are skipped, as in
/// Java. `None` means there is no available next snapshot; clients can poll.
/// Progress is persisted only by an explicit `notify_checkpoint_complete`.
#[derive(Debug)]
pub struct StreamTableScan {
    scan: TableScan<'static>,
    next_snapshot_id: Option<i64>,
    watermark: Option<i64>,
    consumer_id: Option<String>,
    follow_up_mode: SnapshotScanMode,
    consumer_restored: bool,
    missing_snapshot_polls: usize,
}

impl StreamTableScan {
    pub(super) fn new(scan: TableScan<'_>) -> Result<Self> {
        let mut scan = scan.into_owned_paimon_scan()?;
        let options = CoreOptions::new(scan.snapshot_table().schema().options());
        let producer = options.try_changelog_producer()?;
        let follow_up_mode = if producer == ChangelogProducer::None {
            SnapshotScanMode::Delta
        } else {
            SnapshotScanMode::Changelog
        };
        if options.bucket() == -2 && producer != ChangelogProducer::None {
            scan = scan.only_read_snapshot_real_buckets();
        }
        let consumer_id = scan
            .snapshot_table()
            .schema()
            .options()
            .get("consumer-id")
            .cloned();
        if let Some(id) = &consumer_id {
            super::ConsumerManager::validate_consumer_id(id)?;
        }
        Ok(Self {
            scan,
            next_snapshot_id: None,
            watermark: None,
            consumer_id,
            follow_up_mode,
            consumer_restored: false,
            missing_snapshot_polls: 0,
        })
    }

    /// Set the consumer whose acknowledged checkpoint will be restored.
    pub fn with_consumer_id(&mut self, consumer_id: impl Into<String>) -> Result<&mut Self> {
        let consumer_id = consumer_id.into();
        super::ConsumerManager::validate_consumer_id(&consumer_id)?;
        self.consumer_id = Some(consumer_id);
        Ok(self)
    }

    /// Select buckets before split packing. Replaces the previous filter.
    pub fn with_bucket_filter(
        &mut self,
        filter: impl Fn(i32) -> Result<bool> + Send + Sync + 'static,
    ) -> &mut Self {
        self.scan = self.scan.clone().with_snapshot_bucket_filter(filter);
        self
    }

    /// Distribute files or buckets using Java's shard rules.
    pub fn with_shard(&mut self, index: usize, count: usize) -> Result<&mut Self> {
        self.scan = self.scan.clone().with_shard(index, count)?;
        Ok(self)
    }

    pub fn checkpoint(&self) -> Option<i64> {
        self.next_snapshot_id
    }

    pub fn watermark(&self) -> Option<i64> {
        self.watermark
    }

    /// Restore the next snapshot to consume. `None` resets to the configured starting position, including consumer progress.
    pub fn restore(&mut self, next_snapshot_id: Option<i64>) -> Result<()> {
        if next_snapshot_id.is_some_and(|id| id < 1) {
            return Err(Error::DataInvalid {
                message: "next snapshot id must be positive".into(),
                source: None,
            });
        }
        self.next_snapshot_id = next_snapshot_id;
        self.consumer_restored = next_snapshot_id.is_some();
        self.missing_snapshot_polls = 0;
        Ok(())
    }

    pub async fn plan(&mut self) -> Result<Option<Plan>> {
        // Validate even when the table is empty or no follow-up is available.
        self.scan.validate_snapshot_read()?;
        if self.next_snapshot_id.is_none() && !self.consumer_restored {
            if let Some(consumer_id) = &self.consumer_id {
                let next = self
                    .scan
                    .snapshot_table()
                    .consumer_manager()
                    .get(consumer_id)
                    .await?;
                if let Some(next) = next {
                    self.restore(Some(next))?;
                }
            }
            self.consumer_restored = true;
        }
        let manager = self.scan.snapshot_table().snapshot_manager();
        if self.next_snapshot_id.is_none() {
            let Some(snapshot) = manager.get_latest_snapshot().await? else {
                return Ok(None);
            };
            let next = snapshot.id() + 1;
            let watermark = snapshot.watermark();
            let plan = self.plan_initial_snapshot(snapshot).await?;
            self.next_snapshot_id = Some(next);
            self.watermark = watermark;
            return Ok(Some(plan));
        }
        let mode = self.follow_up_mode;
        loop {
            let id = self.next_snapshot_id.expect("initial phase completed");
            let snapshot = match manager.get_snapshot(id).await {
                Ok(snapshot) => snapshot,
                Err(Error::SnapshotNotExist { .. }) => {
                    // Like NextSnapshotFetcher, wait for publication but detect
                    // expired checkpoints / recreated tables periodically.
                    self.missing_snapshot_polls += 1;
                    if self.missing_snapshot_polls.is_multiple_of(16) {
                        let earliest = manager.earliest_snapshot_id().await?;
                        let latest = manager.get_latest_snapshot_id().await?;
                        if earliest.is_some_and(|first| first > id)
                            || latest.is_some_and(|last| id > last + 1)
                            || (latest.is_none() && id > 1)
                        {
                            return Err(Error::DataInvalid {
                                message: format!("stream checkpoint {id} is outside the available snapshots {earliest:?}..{latest:?}"),
                                source: None,
                            });
                        }
                    }
                    return Ok(None);
                }
                Err(error) => return Err(error),
            };
            self.missing_snapshot_polls = 0;
            let should_scan = match mode {
                SnapshotScanMode::Delta => snapshot.commit_kind() == &CommitKind::APPEND,
                SnapshotScanMode::Changelog => snapshot.changelog_manifest_list().is_some(),
                SnapshotScanMode::All => unreachable!("follow-up mode"),
            };
            if !should_scan {
                self.next_snapshot_id = Some(id + 1);
                continue;
            }
            let watermark = snapshot.watermark();
            // Keep the failing snapshot retryable if planning or a callback fails.
            let plan = self.scan.plan_selected_snapshot(snapshot, mode).await?;
            self.next_snapshot_id = Some(id + 1);
            self.watermark = watermark;
            if !plan.splits().is_empty() {
                return Ok(Some(plan));
            }
        }
    }

    async fn plan_initial_snapshot(&self, snapshot: Snapshot) -> Result<Plan> {
        let options = self.scan.snapshot_table().schema().options();
        let mut scan = self.scan.clone();
        let parse = |key, default| -> Result<i32> {
            options
                .get(key)
                .map(|value| {
                    value.parse().map_err(|error| crate::Error::DataInvalid {
                        message: format!("invalid {key}: {value}"),
                        source: Some(Box::new(error)),
                    })
                })
                .unwrap_or(Ok(default))
        };
        match self
            .scan
            .snapshot_table()
            .schema()
            .core_options()
            .try_changelog_producer()?
        {
            ChangelogProducer::Lookup => scan = scan.with_snapshot_levels(1..=i32::MAX),
            ChangelogProducer::FullCompaction => {
                // Java numLevels defaults to incrementSafely(compactionTrigger).
                let levels = if options.contains_key("num-levels") {
                    parse("num-levels", 0)?
                } else {
                    parse("num-sorted-run.compaction-trigger", 5)?.saturating_add(1)
                };
                let last = levels
                    .checked_sub(1)
                    .ok_or_else(|| crate::Error::DataInvalid {
                        message: "num-levels is out of range".into(),
                        source: None,
                    })?;
                scan = scan.with_snapshot_levels(last..=last);
            }
            ChangelogProducer::None | ChangelogProducer::Input => {}
        }
        scan.plan_selected_snapshot(snapshot, SnapshotScanMode::All)
            .await
    }

    /// Acknowledge a checkpoint after the caller has processed its plan.
    pub async fn notify_checkpoint_complete(&self, next_snapshot: Option<i64>) -> Result<()> {
        if let (Some(consumer_id), Some(next)) = (&self.consumer_id, next_snapshot) {
            self.scan
                .snapshot_table()
                .consumer_manager()
                .reset(consumer_id, next)
                .await?;
        }
        Ok(())
    }
}
