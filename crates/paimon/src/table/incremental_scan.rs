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
    /// Read existing changelog manifest files in the range.
    ///
    /// Skips [`OVERWRITE`](crate::spec::CommitKind::OVERWRITE) snapshots and
    /// snapshots without a `changelog_manifest_list`. Does not generate
    /// changelogs (no compact/lookup producer path).
    Changelog,
    /// Resolve to [`Delta`](Self::Delta) when `changelog-producer=none`,
    /// otherwise to [`Changelog`](Self::Changelog).
    Auto,
    /// Diff before/after snapshot states for PK tables.
    ///
    /// Phase 1 supports only `merge-engine=deduplicate`. Planning compares the
    /// full table state at `start_exclusive` vs `end_inclusive` and yields
    /// per-(partition, bucket) [`IncrementalSplit::DiffPair`] units.
    Diff,
}

/// A unit of work produced by an incremental plan.
#[derive(Debug, Clone)]
pub enum IncrementalSplit {
    Data(DataSplit),
    /// Per-(partition, bucket) diff pair.
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

    pub fn try_new(
        mode: IncrementalScanMode,
        splits: Vec<IncrementalSplit>,
    ) -> crate::Result<Self> {
        let plan = Self::new(mode, splits);
        plan.validate()?;
        Ok(plan)
    }

    /// Validate the plan at every point it crosses into a reader.
    ///
    /// `new` is retained for source compatibility, so callers can still build
    /// an invalid plan. Readers must call this method instead of assuming a
    /// plan came from the scanner.
    pub fn validate(&self) -> crate::Result<()> {
        if self.mode == IncrementalScanMode::Auto {
            return Err(crate::Error::DataInvalid {
                message: "Incremental plan mode Auto must be resolved before consumption"
                    .to_string(),
                source: None,
            });
        }
        if self.mode == IncrementalScanMode::Diff {
            let mut before_snapshot_id = None;
            let mut after_snapshot_id = None;
            for split in &self.splits {
                let IncrementalSplit::DiffPair { before, after } = split else {
                    return Err(crate::Error::DataInvalid {
                        message: "Diff incremental plan contains a Data split".to_string(),
                        source: None,
                    });
                };
                validate_diff_pair(before, after)?;
                if let Some(snapshot_id) = before.first().map(DataSplit::snapshot_id) {
                    if before_snapshot_id.is_some_and(|expected| expected != snapshot_id) {
                        return Err(crate::Error::DataInvalid {
                            message: "Diff plan contains different before snapshots".to_string(),
                            source: None,
                        });
                    }
                    before_snapshot_id = Some(snapshot_id);
                }
                if let Some(snapshot_id) = after.first().map(DataSplit::snapshot_id) {
                    if after_snapshot_id.is_some_and(|expected| expected != snapshot_id) {
                        return Err(crate::Error::DataInvalid {
                            message: "Diff plan contains different after snapshots".to_string(),
                            source: None,
                        });
                    }
                    after_snapshot_id = Some(snapshot_id);
                }
            }
            if let (Some(before), Some(after)) = (before_snapshot_id, after_snapshot_id) {
                if before >= after {
                    return Err(crate::Error::DataInvalid {
                        message: "Diff plan before snapshot must be earlier than after snapshot"
                            .to_string(),
                        source: None,
                    });
                }
            }
        } else if self
            .splits
            .iter()
            .any(|split| matches!(split, IncrementalSplit::DiffPair { .. }))
        {
            return Err(crate::Error::DataInvalid {
                message: "Non-Diff incremental plan contains a DiffPair".to_string(),
                source: None,
            });
        }
        Ok(())
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

pub(crate) fn validate_diff_pair(before: &[DataSplit], after: &[DataSplit]) -> crate::Result<()> {
    if before
        .iter()
        .chain(after)
        .any(|split| split.row_ranges().is_some())
    {
        return Err(crate::Error::DataInvalid {
            message: "Diff pair must not contain physical row ranges".to_string(),
            source: None,
        });
    }
    let first = before.first().or(after.first());
    let Some(first) = first else {
        return Ok(());
    };
    for side in [before, after] {
        if let Some(first_in_side) = side.first() {
            if side
                .iter()
                .any(|split| split.snapshot_id() != first_in_side.snapshot_id())
            {
                return Err(crate::Error::DataInvalid {
                    message: "Diff pair side contains splits from different snapshots".to_string(),
                    source: None,
                });
            }
        }
    }
    for split in before.iter().chain(after) {
        if split.partition() != first.partition()
            || split.bucket() != first.bucket()
            || split.bucket_path() != first.bucket_path()
            || split.total_buckets() != first.total_buckets()
        {
            return Err(crate::Error::DataInvalid {
                message: "Diff pair contains splits from different partition buckets".to_string(),
                source: None,
            });
        }
    }
    Ok(())
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
        IncrementalPlan::try_new(mode, splits)
    }

    async fn plan_changelog(&self, mode: IncrementalScanMode) -> crate::Result<IncrementalPlan> {
        let mut splits = Vec::new();
        for snapshot_id in (self.start_exclusive + 1)..=self.end_inclusive {
            let snapshot = self.snapshot_manager.get_snapshot(snapshot_id).await?;
            // OVERWRITE rewrites table contents and does not contribute changelog
            // files for batch incremental reads (Java IncrementalChangelogStartingScanner).
            if snapshot.commit_kind() == &CommitKind::OVERWRITE {
                continue;
            }
            if snapshot.changelog_manifest_list().is_none() {
                continue;
            }
            let plan = self.scan.plan_snapshot_changelog(&snapshot).await?;
            splits.extend(plan.splits().iter().cloned().map(IncrementalSplit::Data));
        }
        IncrementalPlan::try_new(mode, splits)
    }

    async fn plan_diff(&self, mode: IncrementalScanMode) -> crate::Result<IncrementalPlan> {
        if self.table.schema().primary_keys().is_empty() {
            return Err(crate::Error::Unsupported {
                message: "Batch incremental Diff requires a table with primary keys".to_string(),
            });
        }
        let core_options = CoreOptions::new(self.table.schema().options());
        if core_options.merge_engine()? != crate::spec::MergeEngine::Deduplicate {
            return Err(crate::Error::Unsupported {
                message: "Batch incremental Diff only supports merge-engine=deduplicate in Phase 1"
                    .to_string(),
            });
        }
        let before = self
            .snapshot_manager
            .get_snapshot(self.start_exclusive)
            .await?;
        let after = self
            .snapshot_manager
            .get_snapshot(self.end_inclusive)
            .await?;
        let (before_plan, after_plan) = self.scan.plan_snapshot_diff(&before, &after).await?;

        use std::collections::BTreeMap;
        type PBKey = (Vec<u8>, i32);

        let mut before_map: BTreeMap<PBKey, Vec<DataSplit>> = BTreeMap::new();
        for split in before_plan.splits() {
            let key = (split.partition().to_serialized_bytes(), split.bucket());
            before_map.entry(key).or_default().push(split.clone());
        }

        let mut after_map: BTreeMap<PBKey, Vec<DataSplit>> = BTreeMap::new();
        for split in after_plan.splits() {
            let key = (split.partition().to_serialized_bytes(), split.bucket());
            after_map.entry(key).or_default().push(split.clone());
        }

        let mut keys: std::collections::BTreeSet<PBKey> = before_map.keys().cloned().collect();
        keys.extend(after_map.keys().cloned());

        let mut splits = Vec::new();
        for key in keys {
            let before = before_map.remove(&key).unwrap_or_default();
            let after = after_map.remove(&key).unwrap_or_default();
            if before.is_empty() && after.is_empty() {
                continue;
            }
            splits.push(IncrementalSplit::DiffPair { before, after });
        }

        IncrementalPlan::try_new(mode, splits)
    }
}
