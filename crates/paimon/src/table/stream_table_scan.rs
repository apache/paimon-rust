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
use super::{Plan, SnapshotManager, TableScan};
use crate::spec::{ChangelogProducer, CommitKind, CoreOptions, Snapshot, TimeTravelSelector};
use crate::{Error, Result};
use std::collections::HashMap;

/// Continuous table scan. `checkpoint` is the next snapshot to consume.
///
/// Without a checkpoint or consumer progress, the first plan resolves its
/// start from `scan.mode`; every plan still validates the scan options. A
/// full state holds the merged live rows of its snapshot as inserts. Follow-up
/// plans keep the physical events of APPEND deltas when no changelog producer
/// is configured, or of changelogs otherwise. Empty follow-up plans and
/// irrelevant commits are skipped, as in Java. `None` means there is no
/// available next snapshot; clients can poll.
/// Progress is persisted only by an explicit `notify_checkpoint_complete`.
/// Clones retain the current progress and read configuration; subsequent
/// planning, restoration and configuration changes are independent.
#[derive(Debug, Clone)]
pub struct StreamTableScan {
    scan: TableScan<'static>,
    next_snapshot_id: Option<i64>,
    watermark: Option<i64>,
    consumer_id: Option<String>,
    follow_up_mode: SnapshotScanMode,
    consumer_restored: bool,
    missing_snapshot_polls: usize,
}

/// Where a stream starts without a checkpoint or consumer progress, as Java
/// `AbstractDataTableScan#createStartingScanner` in streaming mode.
#[derive(Debug, Clone, Copy)]
enum StartupMode {
    /// The latest full state, then follow-ups.
    LatestFull,
    /// Follow-ups after the latest snapshot.
    Latest,
    /// Follow-ups from this snapshot, without an initial state.
    FromSnapshot(i64),
    /// This snapshot's full state, then follow-ups.
    FromSnapshotFull(i64),
    /// Follow-ups from the first snapshot committed at or after this time, or
    /// from the next commit, whatever its time, when every snapshot is older.
    FromTimestamp(i64),
}

/// Where the first plan starts: a full state, or follow-ups from a snapshot.
enum StartingPosition {
    Full(Box<Snapshot>),
    Next(i64),
}

impl StartupMode {
    /// Resolve `scan.mode` and its selector as Java `CoreOptions#startupMode`.
    /// Options a stream cannot honor fail instead of reading the latest state.
    fn resolve(options: &HashMap<String, String>) -> Result<Self> {
        let core = CoreOptions::new(options);
        core.validate_scan_options()?;
        // A stream supports neither option, even next to a selector.
        for key in [
            "scan.file-creation-time-millis",
            "scan.creation-time-millis",
        ] {
            if options.contains_key(key) {
                return Err(Error::Unsupported {
                    message: format!("Stream scans do not yet support {key}"),
                });
            }
        }
        let selector = core.try_time_travel_selector()?;
        let mode = options
            .get("scan.mode")
            .map(|mode| mode.to_ascii_lowercase());
        // Validation guarantees each remaining mode's selector; in particular,
        // from-snapshot-full has only scan.snapshot-id.
        let snapshot_id_mode: fn(i64) -> Self = match mode.as_deref().unwrap_or("default") {
            "latest-full" => return Ok(Self::LatestFull),
            "latest" => return Ok(Self::Latest),
            "incremental" => return Err(incremental_in_streaming()),
            "from-snapshot-full" => Self::FromSnapshotFull,
            "default" | "from-snapshot" | "from-timestamp" => Self::FromSnapshot,
            other => {
                return Err(Error::Unsupported {
                    message: format!("Stream scans do not yet support scan.mode={other}"),
                })
            }
        };
        match selector {
            Some(TimeTravelSelector::TimestampMillis(timestamp)) => {
                Ok(Self::FromTimestamp(timestamp))
            }
            Some(TimeTravelSelector::SnapshotId { value, option_name }) => {
                parse_snapshot_id(value, option_name).map(snapshot_id_mode)
            }
            Some(TimeTravelSelector::TagName { .. }) => Err(Error::DataInvalid {
                message: "Cannot scan from tag in streaming mode.".into(),
                source: None,
            }),
            Some(TimeTravelSelector::Watermark(_)) => Err(Error::DataInvalid {
                message: "Cannot scan from watermark in streaming mode.".into(),
                source: None,
            }),
            // Java resolves scan.version against tags before choosing a mode.
            Some(TimeTravelSelector::Version { .. }) => Err(Error::Unsupported {
                message: "Stream scans do not yet support scan.version; use scan.snapshot-id"
                    .into(),
            }),
            None if core.incremental_timestamp_window()?.is_some() => {
                Err(incremental_in_streaming())
            }
            None => Ok(Self::LatestFull),
        }
    }
}

fn parse_snapshot_id(value: &str, option_name: &str) -> Result<i64> {
    value.parse().map_err(|_| Error::DataInvalid {
        message: format!("{option_name} '{value}' is not a valid snapshot id."),
        source: None,
    })
}

fn incremental_in_streaming() -> Error {
    Error::DataInvalid {
        message: "Cannot read incremental in streaming mode.".into(),
        source: None,
    }
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

    /// Distribute initial primary-key state by bucket so all key versions
    /// merge together. Follow-up physical events use Java's shard rules.
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
        let scan = self.scan.authorize_snapshot_read().await?;
        if self.next_snapshot_id.is_none() && !self.consumer_restored {
            if let Some(consumer_id) = &self.consumer_id {
                let next = self
                    .scan
                    .snapshot_table()
                    .consumer_manager()
                    .get(consumer_id)
                    .await?;
                if let Some(next) = next {
                    log::info!(
                        "Stream scan of {} resumes consumer '{consumer_id}' at snapshot {next}",
                        self.scan.snapshot_table().identifier().full_name()
                    );
                    self.restore(Some(next))?;
                }
            }
            self.consumer_restored = true;
        }
        let manager = self.scan.snapshot_table().snapshot_manager();
        if self.next_snapshot_id.is_none() {
            match self.starting_position(&manager).await? {
                None => return Ok(None),
                Some(StartingPosition::Full(snapshot)) => {
                    let next = snapshot.id() + 1;
                    let watermark = snapshot.watermark();
                    let plan = self.plan_initial_snapshot(*snapshot, scan).await?;
                    self.next_snapshot_id = Some(next);
                    self.watermark = watermark;
                    return Ok(Some(plan));
                }
                // Record the position so a checkpoint taken before data
                // arrives restores to it, then read follow-ups.
                Some(StartingPosition::Next(next)) => self.next_snapshot_id = Some(next),
            }
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
            let plan = scan.plan_selected_snapshot(snapshot, mode).await?;
            self.next_snapshot_id = Some(id + 1);
            self.watermark = watermark;
            if !plan.splits().is_empty() {
                return Ok(Some(plan));
            }
        }
    }

    /// Resolve the starting position from `scan.mode`, or `None` to wait for a
    /// snapshot. A start below the earliest snapshot moves up to it, as in Java.
    async fn starting_position(
        &self,
        manager: &SnapshotManager,
    ) -> Result<Option<StartingPosition>> {
        let table = self.scan.snapshot_table();
        let startup = StartupMode::resolve(table.schema().options())?;
        let position = match startup {
            StartupMode::LatestFull => manager
                .get_latest_snapshot()
                .await?
                .map(|snapshot| StartingPosition::Full(Box::new(snapshot))),
            StartupMode::FromSnapshotFull(id) => match manager.earliest_snapshot_id().await? {
                Some(earliest) => Some(StartingPosition::Full(Box::new(
                    manager.get_snapshot(id.max(earliest)).await?,
                ))),
                None => None,
            },
            // A table without snapshots streams everything from the first one.
            StartupMode::Latest => Some(StartingPosition::Next(
                manager
                    .get_latest_snapshot_id()
                    .await?
                    .map_or(1, |id| id + 1),
            )),
            StartupMode::FromSnapshot(id) => manager
                .earliest_snapshot_id()
                .await?
                .map(|earliest| StartingPosition::Next(id.max(earliest))),
            StartupMode::FromTimestamp(timestamp) => manager
                .first_id_at_or_after_time_millis(timestamp)
                .await?
                .map(StartingPosition::Next),
        };
        match &position {
            Some(StartingPosition::Full(snapshot)) => log::info!(
                "Stream scan of {} starts from {startup:?} with the full state of snapshot {}",
                table.identifier().full_name(),
                snapshot.id()
            ),
            Some(StartingPosition::Next(next)) => log::info!(
                "Stream scan of {} starts from {startup:?} at snapshot {next}",
                table.identifier().full_name()
            ),
            None => {}
        }
        Ok(position)
    }

    /// Plan the full state of `snapshot`. Lookup tables skip level 0 and
    /// full-compaction tables read only the last level, as in Java; rows in
    /// the other levels arrive later as changelogs from compaction.
    async fn plan_initial_snapshot(
        &self,
        snapshot: Snapshot,
        mut scan: TableScan<'static>,
    ) -> Result<Plan> {
        let table = scan.snapshot_table().clone();
        let options = table.schema().options();
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
