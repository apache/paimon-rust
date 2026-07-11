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

use super::{DataSplit, SnapshotManager, Table, TableScan};
use crate::spec::{CommitKind, CoreOptions};

/// Batch incremental scan mode.
///
/// Range semantics: `(start_exclusive, end_inclusive]` — start is exclusive and
/// end is inclusive. An empty range (`start == end`) yields an empty plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrementalScanMode {
    /// Read data files from APPEND snapshots in the range (delta manifests).
    Delta,
    /// Read changelog manifest files in the range.
    ///
    /// Not fully implemented in this release; planning returns
    /// [`Error::Unsupported`](crate::Error::Unsupported).
    Changelog,
    /// Resolve to [`Delta`](Self::Delta) when `changelog-producer=none`,
    /// otherwise to [`Changelog`](Self::Changelog).
    Auto,
    /// Diff before/after snapshots.
    ///
    /// Not fully implemented in this release; planning returns
    /// [`Error::Unsupported`](crate::Error::Unsupported).
    Diff,
}

/// A unit of work produced by an incremental plan.
#[derive(Debug, Clone)]
pub enum IncrementalSplit {
    Data(DataSplit),
    /// Per-(partition, bucket) diff pair. Memory bounded by one bucket's data.
    DiffPair {
        before: Vec<DataSplit>,
        after: Vec<DataSplit>,
    },
}

/// Planned incremental scan: resolved mode plus splits.
#[derive(Debug, Clone)]
pub struct IncrementalPlan {
    mode: IncrementalScanMode,
    splits: Vec<IncrementalSplit>,
}

impl IncrementalPlan {
    pub fn new(mode: IncrementalScanMode, splits: Vec<IncrementalSplit>) -> Self {
        Self { mode, splits }
    }

    /// Resolved mode (`Auto` already collapsed to `Delta` / `Changelog`).
    pub fn mode(&self) -> IncrementalScanMode {
        self.mode
    }

    pub fn splits(&self) -> &[IncrementalSplit] {
        &self.splits
    }

    pub fn data_splits(&self) -> Vec<DataSplit> {
        self.splits
            .iter()
            .filter_map(|split| match split {
                IncrementalSplit::Data(data) => Some(data.clone()),
                IncrementalSplit::DiffPair { .. } => None,
            })
            .collect()
    }
}

/// Batch incremental scan over a snapshot id range.
pub struct IncrementalScan<'a> {
    table: &'a Table,
    scan: TableScan<'a>,
    snapshot_manager: SnapshotManager,
    mode: IncrementalScanMode,
    start_exclusive: i64,
    end_inclusive: i64,
}

impl<'a> IncrementalScan<'a> {
    pub(crate) fn for_table(
        table: &'a Table,
        mode: IncrementalScanMode,
        start_exclusive: i64,
        end_inclusive: i64,
    ) -> Self {
        let scan = TableScan::new(table, None, Vec::new(), None, None, None);
        Self::new(table, scan, mode, start_exclusive, end_inclusive)
    }

    pub(crate) fn new(
        table: &'a Table,
        scan: TableScan<'a>,
        mode: IncrementalScanMode,
        start_exclusive: i64,
        end_inclusive: i64,
    ) -> Self {
        let snapshot_manager =
            SnapshotManager::new(table.file_io().clone(), table.location().to_string());
        Self {
            table,
            scan,
            snapshot_manager,
            mode,
            start_exclusive,
            end_inclusive,
        }
    }

    pub async fn plan(&self) -> crate::Result<IncrementalPlan> {
        let mode = self.resolve_mode();
        self.validate_snapshot_range(mode).await?;
        if self.start_exclusive == self.end_inclusive {
            return Ok(IncrementalPlan::new(mode, Vec::new()));
        }
        match mode {
            IncrementalScanMode::Delta => self.plan_delta(mode).await,
            IncrementalScanMode::Changelog => self.plan_changelog(mode).await,
            IncrementalScanMode::Auto => unreachable!("Auto must resolve before planning"),
            IncrementalScanMode::Diff => self.plan_diff(mode).await,
        }
    }

    fn resolve_mode(&self) -> IncrementalScanMode {
        match self.mode {
            IncrementalScanMode::Auto => {
                let core_options = CoreOptions::new(self.table.schema().options());
                let producer = core_options.changelog_producer();
                if producer.eq_ignore_ascii_case("none") {
                    IncrementalScanMode::Delta
                } else {
                    IncrementalScanMode::Changelog
                }
            }
            mode => mode,
        }
    }

    async fn validate_snapshot_range(&self, mode: IncrementalScanMode) -> crate::Result<()> {
        let earliest = self
            .snapshot_manager
            .earliest_snapshot_id()
            .await?
            .ok_or_else(|| crate::Error::DataInvalid {
                message: "No snapshots available for incremental scan".to_string(),
                source: None,
            })?;
        let latest = self
            .snapshot_manager
            .get_latest_snapshot_id()
            .await?
            .ok_or_else(|| crate::Error::DataInvalid {
                message: "No snapshots available for incremental scan".to_string(),
                source: None,
            })?;
        let min_start = match mode {
            IncrementalScanMode::Diff => earliest,
            IncrementalScanMode::Delta | IncrementalScanMode::Changelog => earliest - 1,
            IncrementalScanMode::Auto => unreachable!("Auto must resolve before validation"),
        };
        if self.start_exclusive < min_start
            || self.end_inclusive > latest
            || self.start_exclusive > self.end_inclusive
        {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Incremental snapshot range [{}, {}] is out of available range [{}, {}] for {:?}",
                    self.start_exclusive, self.end_inclusive, min_start, latest, mode
                ),
                source: None,
            });
        }
        Ok(())
    }

    async fn plan_delta(&self, mode: IncrementalScanMode) -> crate::Result<IncrementalPlan> {
        let mut splits = Vec::new();
        for snapshot_id in (self.start_exclusive + 1)..=self.end_inclusive {
            let snapshot = self.snapshot_manager.get_snapshot(snapshot_id).await?;
            if snapshot.commit_kind() != &CommitKind::APPEND {
                continue;
            }
            let plan = self.scan.plan_snapshot_delta(&snapshot).await?;
            splits.extend(plan.splits().iter().cloned().map(IncrementalSplit::Data));
        }
        Ok(IncrementalPlan::new(mode, splits))
    }

    async fn plan_changelog(&self, mode: IncrementalScanMode) -> crate::Result<IncrementalPlan> {
        let _ = mode;
        Err(crate::Error::Unsupported {
            message: "Batch incremental Changelog scan is not implemented yet".to_string(),
        })
    }

    async fn plan_diff(&self, mode: IncrementalScanMode) -> crate::Result<IncrementalPlan> {
        let _ = mode;
        Err(crate::Error::Unsupported {
            message: "Batch incremental Diff scan is not implemented yet".to_string(),
        })
    }
}
