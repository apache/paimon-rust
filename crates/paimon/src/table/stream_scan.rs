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

//! Stateful continuous snapshot scan.
//!
//! The cursor has the same meaning as Java `DataTableStreamScan`: planning a
//! snapshot advances `next_snapshot_id` immediately. Callers which hand plans
//! to asynchronous workers must therefore persist their own safe checkpoint
//! only after the planned work has completed.

use std::collections::HashSet;

use super::incremental_scan::{IncrementalPlan, IncrementalScanMode, IncrementalSplit};
use super::partition_filter::PartitionFilter;
use super::table_scan::SnapshotLevelFilter;
use super::{Plan, RowRange, SnapshotManager, Table, TableScan};
use crate::spec::{ChangelogProducer, CommitKind, Predicate, Snapshot};

const FIRST_SNAPSHOT_ID: i64 = 1;
const RANGE_READ_ATTEMPTS: usize = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AvailableRange {
    Empty,
    Range { earliest: i64, latest: i64 },
    Transient,
}

/// How a continuous scan chooses its first snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamScanStartupMode {
    /// Read the latest table state in full, then follow later snapshots.
    LatestFull,
    /// Ignore snapshots which already exist when the scan starts and follow
    /// snapshots committed afterwards.
    ///
    /// When the table has no snapshot at startup, snapshot 1 is consumed as an
    /// incremental snapshot once it appears, matching Java Paimon.
    Latest,
    /// Read `snapshot_id` inclusively as the first incremental snapshot.
    FromSnapshot(i64),
    /// Read `snapshot_id` as a full table state, then follow later snapshots.
    FromSnapshotFull(i64),
}

/// How snapshots after the startup phase are planned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamScanFollowUpMode {
    /// Use delta manifests for `changelog-producer=none`; otherwise use
    /// changelog manifests.
    Auto,
    /// Read APPEND snapshot delta manifests.
    Delta,
    /// Read changelog manifests.
    Changelog,
}

/// A plan emitted by a continuous scan.
#[derive(Debug)]
pub enum StreamPlan {
    /// A complete table state at one snapshot.
    Full {
        snapshot_id: i64,
        watermark: Option<i64>,
        next_snapshot_id: i64,
        plan: Plan,
    },
    /// Delta or changelog work for one snapshot.
    Incremental {
        snapshot_id: i64,
        watermark: Option<i64>,
        next_snapshot_id: i64,
        plan: IncrementalPlan,
    },
}

impl StreamPlan {
    /// Snapshot represented by this plan.
    pub fn snapshot_id(&self) -> i64 {
        match self {
            Self::Full { snapshot_id, .. } | Self::Incremental { snapshot_id, .. } => *snapshot_id,
        }
    }

    /// Snapshot watermark, when one was committed.
    pub fn watermark(&self) -> Option<i64> {
        match self {
            Self::Full { watermark, .. } | Self::Incremental { watermark, .. } => *watermark,
        }
    }

    /// Cursor immediately after this plan was produced.
    pub fn next_snapshot_id(&self) -> i64 {
        match self {
            Self::Full {
                next_snapshot_id, ..
            }
            | Self::Incremental {
                next_snapshot_id, ..
            } => *next_snapshot_id,
        }
    }

    pub fn full_plan(&self) -> Option<&Plan> {
        match self {
            Self::Full { plan, .. } => Some(plan),
            Self::Incremental { .. } => None,
        }
    }

    pub fn incremental_plan(&self) -> Option<&IncrementalPlan> {
        match self {
            Self::Incremental { plan, .. } => Some(plan),
            Self::Full { .. } => None,
        }
    }

    pub fn into_full_plan(self) -> Option<Plan> {
        match self {
            Self::Full { plan, .. } => Some(plan),
            Self::Incremental { .. } => None,
        }
    }

    pub fn into_incremental_plan(self) -> Option<IncrementalPlan> {
        match self {
            Self::Incremental { plan, .. } => Some(plan),
            Self::Full { .. } => None,
        }
    }
}

/// Result of one non-blocking continuous-scan poll.
#[derive(Debug)]
pub enum StreamScanPoll {
    /// Work is available.
    Data(StreamPlan),
    /// The next expected snapshot has not been committed yet.
    Waiting,
    /// The configured bounded scan is complete.
    ///
    /// Bounded-watermark configuration is not implemented in the first
    /// version, so this variant is reserved for forward-compatible consumers.
    End,
}

/// An owned, stateful continuous scanner.
///
/// The scanner clones the table and all scan-time predicates at construction,
/// so it remains valid after the originating [`Table`] or read builder is
/// dropped. It does not spawn a background task; callers control polling and
/// backpressure.
#[derive(Debug)]
pub struct StreamScan {
    table: Table,
    snapshot_manager: SnapshotManager,
    partition_filter: Option<PartitionFilter>,
    data_predicates: Vec<Predicate>,
    bucket_predicate: Option<Predicate>,
    row_ranges: Option<Vec<RowRange>>,
    projected_read_field_ids: Option<HashSet<i32>>,
    startup_mode: StreamScanStartupMode,
    follow_up_mode: IncrementalScanMode,
    startup_complete: bool,
    next_snapshot_id: Option<i64>,
    current_watermark: Option<i64>,
}

impl StreamScan {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        table: Table,
        partition_filter: Option<PartitionFilter>,
        data_predicates: Vec<Predicate>,
        bucket_predicate: Option<Predicate>,
        row_ranges: Option<Vec<RowRange>>,
        projected_read_field_ids: Option<HashSet<i32>>,
        startup_mode: StreamScanStartupMode,
        follow_up_mode: StreamScanFollowUpMode,
    ) -> crate::Result<Self> {
        match startup_mode {
            StreamScanStartupMode::FromSnapshot(snapshot_id)
            | StreamScanStartupMode::FromSnapshotFull(snapshot_id)
                if snapshot_id < FIRST_SNAPSHOT_ID =>
            {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Stream scan starting snapshot id must be at least {FIRST_SNAPSHOT_ID}, got {snapshot_id}"
                    ),
                    source: None,
                });
            }
            _ => {}
        }

        if table.is_format_table() {
            return Err(crate::Error::Unsupported {
                message: "Continuous stream scan is not supported for format tables".to_string(),
            });
        }

        let core_options = table.schema().core_options();
        let changelog_producer = core_options.try_changelog_producer()?;
        let deletion_vectors_enabled = core_options.deletion_vectors_enabled();
        let follow_up_mode = match (follow_up_mode, changelog_producer) {
            (StreamScanFollowUpMode::Delta, ChangelogProducer::Lookup)
                if deletion_vectors_enabled
                    && matches!(
                        startup_mode,
                        StreamScanStartupMode::LatestFull
                            | StreamScanStartupMode::FromSnapshotFull(_)
                    ) =>
            {
                return Err(crate::Error::Unsupported {
                    message: "Deletion-vector lookup tables require changelog follow-up"
                        .to_string(),
                });
            }
            (StreamScanFollowUpMode::Delta, _) => IncrementalScanMode::Delta,
            (StreamScanFollowUpMode::Changelog, ChangelogProducer::None) => {
                return Err(crate::Error::Unsupported {
                    message: "Changelog stream follow-up requires a changelog producer".to_string(),
                });
            }
            (StreamScanFollowUpMode::Changelog, _) => IncrementalScanMode::Changelog,
            (StreamScanFollowUpMode::Auto, ChangelogProducer::None) => IncrementalScanMode::Delta,
            (StreamScanFollowUpMode::Auto, _) => IncrementalScanMode::Changelog,
        };
        let snapshot_manager = table.snapshot_manager();

        Ok(Self {
            table,
            snapshot_manager,
            partition_filter,
            data_predicates,
            bucket_predicate,
            row_ranges,
            projected_read_field_ids,
            startup_mode,
            follow_up_mode,
            startup_complete: false,
            next_snapshot_id: None,
            current_watermark: None,
        })
    }

    /// Resolved follow-up mode. `Auto` is collapsed during construction.
    pub fn follow_up_mode(&self) -> IncrementalScanMode {
        self.follow_up_mode
    }

    /// The next snapshot which will be considered, suitable for checkpointing.
    ///
    /// This cursor advances when a plan is produced, not when its splits finish.
    pub fn checkpoint(&self) -> Option<i64> {
        self.next_snapshot_id
    }

    /// Restore a previously checkpointed next snapshot id.
    ///
    /// `Some(id)` bypasses startup selection. `None` resets the scanner and
    /// applies its configured startup mode again.
    pub fn restore(&mut self, next_snapshot_id: Option<i64>) -> crate::Result<()> {
        if next_snapshot_id.is_some_and(|id| id < FIRST_SNAPSHOT_ID) {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Stream scan checkpoint must be at least {FIRST_SNAPSHOT_ID}, got {}",
                    next_snapshot_id.unwrap()
                ),
                source: None,
            });
        }
        self.next_snapshot_id = next_snapshot_id;
        self.startup_complete = next_snapshot_id.is_some();
        self.current_watermark = None;
        Ok(())
    }

    /// Most recent watermark observed on a planned (including empty) snapshot.
    pub fn watermark(&self) -> Option<i64> {
        self.current_watermark
    }

    /// Freeze startup state which is observable at scanner creation time.
    ///
    /// `Latest` must remember whether the table was empty when the source was
    /// created. Without this explicit async initialization, a snapshot
    /// committed between construction and the first poll could be mistaken for
    /// pre-existing data and skipped. Other startup modes resolve their first
    /// plan during polling and need no eager IO.
    pub async fn initialize(&mut self) -> crate::Result<()> {
        if self.startup_complete || self.startup_mode != StreamScanStartupMode::Latest {
            return Ok(());
        }
        self.table
            .schema()
            .core_options()
            .ensure_read_authorized()?;
        let next_snapshot_id = match self.snapshot_manager.get_latest_snapshot_id().await? {
            Some(latest) => next_id(latest)?,
            None => FIRST_SNAPSHOT_ID,
        };
        self.next_snapshot_id = Some(next_snapshot_id);
        self.startup_complete = true;
        Ok(())
    }

    /// Poll once without waiting for future snapshots.
    pub async fn poll_next(&mut self) -> crate::Result<StreamScanPoll> {
        self.table
            .schema()
            .core_options()
            .ensure_read_authorized()?;
        self.initialize().await?;
        if !self.startup_complete {
            return self.poll_startup().await;
        }
        self.poll_follow_up().await
    }

    fn table_scan(&self) -> TableScan<'_> {
        TableScan::new(
            &self.table,
            self.partition_filter.clone(),
            self.data_predicates.clone(),
            self.bucket_predicate.clone(),
            None,
            self.row_ranges.clone(),
        )
        .with_projected_read_field_ids(self.projected_read_field_ids.clone())
    }

    async fn poll_startup(&mut self) -> crate::Result<StreamScanPoll> {
        match self.startup_mode {
            StreamScanStartupMode::LatestFull => {
                let Some(snapshot) = self.snapshot_manager.get_latest_snapshot().await? else {
                    return Ok(StreamScanPoll::Waiting);
                };
                self.plan_full_startup(snapshot).await
            }
            StreamScanStartupMode::Latest => {
                unreachable!("Latest startup is resolved by initialize")
            }
            StreamScanStartupMode::FromSnapshot(snapshot_id) => {
                let (earliest, latest) = match self.available_range().await? {
                    AvailableRange::Empty | AvailableRange::Transient => {
                        return Ok(StreamScanPoll::Waiting)
                    }
                    AvailableRange::Range { earliest, latest } => (earliest, latest),
                };
                validate_incremental_start(snapshot_id, earliest, latest)?;
                self.next_snapshot_id = Some(snapshot_id);
                self.startup_complete = true;
                Ok(StreamScanPoll::Waiting)
            }
            StreamScanStartupMode::FromSnapshotFull(snapshot_id) => {
                let (earliest, latest) = match self.available_range().await? {
                    AvailableRange::Empty | AvailableRange::Transient => {
                        return Ok(StreamScanPoll::Waiting)
                    }
                    AvailableRange::Range { earliest, latest } => (earliest, latest),
                };
                validate_full_start(snapshot_id, earliest, latest)?;
                let Some(snapshot) = self.try_get_snapshot(snapshot_id).await? else {
                    return Ok(StreamScanPoll::Waiting);
                };
                self.plan_full_startup(snapshot).await
            }
        }
    }

    async fn plan_full_startup(&mut self, snapshot: Snapshot) -> crate::Result<StreamScanPoll> {
        // Reading a full state at an overwrite snapshot is well-defined. The
        // overwrite restriction applies only to follow-up change plans.
        let snapshot_id = snapshot.id();
        let watermark = snapshot.watermark();
        let next_snapshot_id = self.full_start_next_snapshot_id(snapshot_id)?;
        let level_filter = self.full_start_level_filter()?;
        let plan = self
            .table_scan()
            .plan_snapshot_full(&snapshot, level_filter)
            .await?;
        self.next_snapshot_id = Some(next_snapshot_id);
        self.current_watermark = watermark;
        self.startup_complete = true;
        Ok(StreamScanPoll::Data(StreamPlan::Full {
            snapshot_id,
            watermark,
            next_snapshot_id,
            plan,
        }))
    }

    fn full_start_level_filter(&self) -> crate::Result<Option<SnapshotLevelFilter>> {
        let options = self.table.schema().core_options();
        // Lookup-style tables expose their stable materialized state above
        // level 0. Deletion-vector-only tables replay the starting snapshot in
        // the incremental phase so its un-compacted level-0 changes are not
        // lost.
        if options.deletion_vectors_enabled() {
            return Ok(Some(SnapshotLevelFilter::GreaterThan(0)));
        }
        if self.follow_up_mode != IncrementalScanMode::Changelog {
            return Ok(None);
        }
        match options.try_changelog_producer()? {
            // Lookup compaction will emit level-0 input through a later
            // changelog. Reading it in the full phase would emit it twice.
            ChangelogProducer::Lookup => Ok(Some(SnapshotLevelFilter::GreaterThan(0))),
            // Full-compaction changelog covers all changes since the previous
            // last-level state. Start from that materialized state only.
            ChangelogProducer::FullCompaction => {
                Ok(Some(SnapshotLevelFilter::Equal(options.num_levels()? - 1)))
            }
            ChangelogProducer::None | ChangelogProducer::Input => Ok(None),
        }
    }

    fn full_start_next_snapshot_id(&self, snapshot_id: i64) -> crate::Result<i64> {
        let options = self.table.schema().core_options();
        if options.deletion_vectors_enabled()
            && options.try_changelog_producer()? != ChangelogProducer::Lookup
        {
            // The full plan deliberately excludes level 0. Revisit this same
            // snapshot once through Delta/Changelog to emit those changes.
            Ok(snapshot_id)
        } else {
            next_id(snapshot_id)
        }
    }

    async fn poll_follow_up(&mut self) -> crate::Result<StreamScanPoll> {
        loop {
            let snapshot_id =
                self.next_snapshot_id
                    .ok_or_else(|| crate::Error::UnexpectedError {
                        message: "Stream scan startup completed without a next snapshot id"
                            .to_string(),
                        source: None,
                    })?;
            let Some(snapshot) = self.next_snapshot(snapshot_id).await? else {
                return Ok(StreamScanPoll::Waiting);
            };

            if snapshot.commit_kind() == &CommitKind::OVERWRITE {
                return Err(crate::Error::Unsupported {
                    message: format!(
                        "Streaming follow-up scan cannot safely consume OVERWRITE snapshot {snapshot_id}"
                    ),
                });
            }

            let should_scan = match self.follow_up_mode {
                IncrementalScanMode::Delta => snapshot.commit_kind() == &CommitKind::APPEND,
                IncrementalScanMode::Changelog => snapshot.changelog_manifest_list().is_some(),
                IncrementalScanMode::Auto | IncrementalScanMode::Diff => {
                    unreachable!("stream follow-up mode must resolve to Delta or Changelog")
                }
            };

            let following_snapshot_id = next_id(snapshot_id)?;
            if !should_scan {
                self.next_snapshot_id = Some(following_snapshot_id);
                continue;
            }

            let raw_plan = match self.follow_up_mode {
                IncrementalScanMode::Delta => {
                    self.table_scan()
                        .plan_snapshot_delta_streaming(&snapshot)
                        .await?
                }
                IncrementalScanMode::Changelog => {
                    self.table_scan()
                        .plan_snapshot_changelog_streaming(&snapshot)
                        .await?
                }
                IncrementalScanMode::Auto | IncrementalScanMode::Diff => unreachable!(),
            };
            let splits = raw_plan
                .into_splits()
                .into_iter()
                .map(IncrementalSplit::Data)
                .collect();
            let plan = IncrementalPlan::try_new(self.follow_up_mode, splits)?;

            // Match Java DataTableStreamScan: the checkpoint cursor advances as
            // soon as planning succeeds. Empty snapshots also advance and the
            // same poll keeps looking for useful work.
            self.next_snapshot_id = Some(following_snapshot_id);
            self.current_watermark = snapshot.watermark();
            if plan.splits().is_empty() {
                continue;
            }

            return Ok(StreamScanPoll::Data(StreamPlan::Incremental {
                snapshot_id,
                watermark: snapshot.watermark(),
                next_snapshot_id: following_snapshot_id,
                plan,
            }));
        }
    }

    async fn next_snapshot(&mut self, snapshot_id: i64) -> crate::Result<Option<Snapshot>> {
        if let Some(snapshot) = self.try_get_snapshot(snapshot_id).await? {
            return Ok(Some(snapshot));
        }

        // The snapshot may be committed after the first lookup but before the
        // range observation. Re-read it before classifying the miss as a gap.
        let range = self.available_range().await?;
        if let Some(snapshot) = self.try_get_snapshot(snapshot_id).await? {
            return Ok(Some(snapshot));
        }

        match range {
            AvailableRange::Transient => Ok(None),
            AvailableRange::Empty if snapshot_id == FIRST_SNAPSHOT_ID => Ok(None),
            AvailableRange::Empty => Err(crate::Error::DataInvalid {
                message: format!(
                    "Next expected snapshot {snapshot_id} is out of range because the table currently has no snapshots"
                ),
                source: None,
            }),
            AvailableRange::Range { earliest, latest } if snapshot_id < earliest => {
                Err(crate::Error::DataInvalid {
                    message: format!(
                        "Next expected snapshot {snapshot_id} has expired; available snapshot range is [{earliest}, {latest}]"
                    ),
                    source: None,
                })
            }
            // A range hint/listing can become visible before the snapshot
            // object itself on an eventually consistent backend. Polling
            // frequency must never turn that transient state into data loss.
            AvailableRange::Range { latest, .. } if snapshot_id <= latest => Ok(None),
            AvailableRange::Range { latest, .. }
                if latest.checked_add(1) == Some(snapshot_id) =>
            {
                Ok(None)
            }
            AvailableRange::Range { earliest, latest } => Err(crate::Error::DataInvalid {
                message: format!(
                    "Next expected snapshot {snapshot_id} is too large; available snapshot range is [{earliest}, {latest}]"
                ),
                source: None,
            }),
        }
    }

    async fn try_get_snapshot(&self, snapshot_id: i64) -> crate::Result<Option<Snapshot>> {
        match self.snapshot_manager.get_snapshot(snapshot_id).await {
            Ok(snapshot) => Ok(Some(snapshot)),
            Err(crate::Error::SnapshotNotExist {
                snapshot_id: missing,
            }) if missing == snapshot_id => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn available_range(&mut self) -> crate::Result<AvailableRange> {
        let mut last_observation = (None, None);
        for _ in 0..RANGE_READ_ATTEMPTS {
            let earliest = self.snapshot_manager.earliest_snapshot_id().await?;
            let latest = self.snapshot_manager.get_latest_snapshot_id().await?;
            match (earliest, latest) {
                (None, None) => {
                    return Ok(AvailableRange::Empty);
                }
                (Some(earliest), Some(latest)) if earliest <= latest => {
                    return Ok(AvailableRange::Range { earliest, latest });
                }
                observation => last_observation = observation,
            }
        }

        let _ = last_observation;
        Ok(AvailableRange::Transient)
    }
}

fn next_id(snapshot_id: i64) -> crate::Result<i64> {
    snapshot_id
        .checked_add(1)
        .ok_or_else(|| crate::Error::DataInvalid {
            message: format!("Snapshot id {snapshot_id} cannot be advanced"),
            source: None,
        })
}

fn validate_incremental_start(snapshot_id: i64, earliest: i64, latest: i64) -> crate::Result<()> {
    if snapshot_id < earliest {
        return Err(crate::Error::DataInvalid {
            message: format!(
                "Stream starting snapshot {snapshot_id} has expired; available snapshot range is [{earliest}, {latest}]"
            ),
            source: None,
        });
    }
    if snapshot_id > latest.saturating_add(1) {
        return Err(crate::Error::DataInvalid {
            message: format!(
                "Stream starting snapshot {snapshot_id} is too large; available snapshot range is [{earliest}, {latest}]"
            ),
            source: None,
        });
    }
    Ok(())
}

fn validate_full_start(snapshot_id: i64, earliest: i64, latest: i64) -> crate::Result<()> {
    if snapshot_id < earliest {
        return Err(crate::Error::DataInvalid {
            message: format!(
                "Full stream starting snapshot {snapshot_id} has expired; available snapshot range is [{earliest}, {latest}]"
            ),
            source: None,
        });
    }
    if snapshot_id > latest {
        return Err(crate::Error::SnapshotNotExist { snapshot_id });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        validate_full_start, validate_incremental_start, StreamPlan, StreamScanFollowUpMode,
        StreamScanPoll, StreamScanStartupMode,
    };

    #[test]
    fn start_range_validation_is_explicit() {
        assert!(validate_incremental_start(4, 4, 6).is_ok());
        assert!(validate_incremental_start(7, 4, 6).is_ok());
        assert!(matches!(
            validate_incremental_start(3, 4, 6),
            Err(crate::Error::DataInvalid { .. })
        ));
        assert!(matches!(
            validate_incremental_start(8, 4, 6),
            Err(crate::Error::DataInvalid { .. })
        ));
        assert!(matches!(
            validate_full_start(7, 4, 6),
            Err(crate::Error::SnapshotNotExist { snapshot_id: 7 })
        ));
    }

    #[test]
    fn public_modes_and_poll_are_matchable() {
        let _ = [
            StreamScanStartupMode::LatestFull,
            StreamScanStartupMode::Latest,
            StreamScanStartupMode::FromSnapshot(1),
            StreamScanStartupMode::FromSnapshotFull(1),
        ];
        let _ = [
            StreamScanFollowUpMode::Auto,
            StreamScanFollowUpMode::Delta,
            StreamScanFollowUpMode::Changelog,
        ];
        let waiting = StreamScanPoll::Waiting;
        assert!(matches!(waiting, StreamScanPoll::Waiting));
        let end = StreamScanPoll::End;
        assert!(matches!(end, StreamScanPoll::End));
        let _: Option<StreamPlan> = None;
    }
}
