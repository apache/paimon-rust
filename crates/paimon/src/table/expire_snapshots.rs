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

//! Snapshot expiration.
//!
//! Reference: Java `ExpireSnapshotsImpl`, configured like the Flink and Spark
//! `expire_snapshots` procedures (`ProcedureUtils.fillInSnapshotOptions`).
//!
//! Expiring snapshots `[earliest, end)` removes, in order:
//! 1. data files deleted by the delta manifests of `(earliest, end]` that no
//!    tag still reads;
//! 2. changelog files added by `[earliest, end)`;
//! 3. manifest lists, manifests, index manifests, index files, statistics and
//!    reassign plans of `[earliest, end)` that neither `end` nor a tag in the
//!    range still references;
//! 4. the snapshot files themselves, then the EARLIEST hint moves to `end`.
//!
//! Snapshot files go last, so an interrupted run leaves every remaining
//! snapshot readable and a later run finishes the job.

use crate::spec::{CoreOptions, Snapshot};
use crate::table::snapshot_deletion::{DataFileKey, SnapshotDeletion};
use crate::table::Table;
use crate::{Error, Result};
use futures::{stream, StreamExt, TryStreamExt};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

const SNAPSHOT_READ_CONCURRENCY: usize = 16;

/// Retention rules for one expiration run. Java `ExpireConfig` (snapshot part).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExpireConfig {
    retain_max: i32,
    retain_min: i32,
    /// Snapshots whose successor was committed at or after this time are kept.
    older_than_millis: i64,
    max_deletes: i32,
}

/// Expire old snapshots of a table and delete the files only they reference.
///
/// Defaults come from the table options `snapshot.num-retained.min`,
/// `snapshot.num-retained.max`, `snapshot.time-retained` and
/// `snapshot.expire.limit`; each can be overridden per run, like the arguments
/// of Java's `expire_snapshots` procedure.
pub struct ExpireSnapshots<'a> {
    table: &'a Table,
    retain_max: Option<i32>,
    retain_min: Option<i32>,
    older_than_millis: Option<i64>,
    max_deletes: Option<i32>,
    current_time_millis: Option<i64>,
}

impl<'a> ExpireSnapshots<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            retain_max: None,
            retain_min: None,
            older_than_millis: None,
            max_deletes: None,
            current_time_millis: None,
        }
    }

    /// Maximum number of completed snapshots to retain.
    pub fn with_retain_max(&mut self, retain_max: i32) -> &mut Self {
        self.retain_max = Some(retain_max);
        self
    }

    /// Minimum number of completed snapshots to retain.
    pub fn with_retain_min(&mut self, retain_min: i32) -> &mut Self {
        self.retain_min = Some(retain_min);
        self
    }

    /// Expire snapshots older than this epoch-millisecond timestamp instead of
    /// `snapshot.time-retained`.
    pub fn with_older_than_millis(&mut self, older_than_millis: i64) -> &mut Self {
        self.older_than_millis = Some(older_than_millis);
        self
    }

    /// Maximum number of snapshots to expire in this run.
    pub fn with_max_deletes(&mut self, max_deletes: i32) -> &mut Self {
        self.max_deletes = Some(max_deletes);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_current_time_millis(&mut self, current_time_millis: i64) -> &mut Self {
        self.current_time_millis = Some(current_time_millis);
        self
    }

    fn config(&self) -> Result<ExpireConfig> {
        let core = CoreOptions::new(self.table.schema().options());
        let retain_max = match self.retain_max {
            Some(value) => positive("retain_max", value)?,
            None => core.snapshot_num_retained_max()?,
        };
        let retain_min = match self.retain_min {
            Some(value) => positive("retain_min", value)?,
            None => core.snapshot_num_retained_min()?,
        };
        if retain_max < retain_min {
            return Err(Error::DataInvalid {
                message: format!(
                    "retainMax ({retain_max}) must not be less than retainMin ({retain_min})."
                ),
                source: None,
            });
        }
        let max_deletes = match self.max_deletes {
            Some(value) => positive("max_deletes", value)?,
            None => core.snapshot_expire_limit()?,
        };
        let older_than_millis = match self.older_than_millis {
            Some(value) => value,
            None => {
                let now = self.current_time_millis.unwrap_or_else(current_time_millis);
                let retained = i64::try_from(core.snapshot_time_retained_ms()?).unwrap_or(i64::MAX);
                now.saturating_sub(retained)
            }
        };
        Ok(ExpireConfig {
            retain_max,
            retain_min,
            older_than_millis,
            max_deletes,
        })
    }

    /// Expire snapshots and return how many snapshot files were removed.
    pub async fn execute(&self) -> Result<usize> {
        self.table.ensure_not_branch_reference_for_write()?;
        let config = self.config()?;

        let snapshot_manager = self.table.snapshot_manager();
        let Some(latest) = snapshot_manager.get_latest_snapshot().await? else {
            return Ok(0);
        };
        let latest = latest.id();
        let Some(earliest) = snapshot_manager.earliest_snapshot_id().await? else {
            return Ok(0);
        };

        // The oldest snapshot `snapshot.num-retained.max` lets us keep.
        let min = (latest - i64::from(config.retain_max) + 1).max(earliest);
        // `snapshot.num-retained.min` protects the newest snapshots.
        let mut max_exclusive = latest - i64::from(config.retain_min) + 1;
        // A snapshot a consumer still reads from cannot be deleted.
        let consumers = self.table.consumer_manager().list_all().await?;
        if let Some(min_next) = consumers.iter().map(|(_, next)| *next).min() {
            max_exclusive = max_exclusive.min(min_next);
        }
        // `snapshot.expire.limit` bounds one run.
        max_exclusive = max_exclusive.min(earliest.saturating_add(i64::from(config.max_deletes)));

        for id in min..max_exclusive {
            // A snapshot expires only once its successor has also outlived the
            // retention time.
            if let Some(next) = snapshot_manager.try_get_snapshot(id + 1).await? {
                if config.older_than_millis <= next.time_millis() as i64 {
                    return self.expire_until(earliest, id).await;
                }
            }
        }
        self.expire_until(earliest, max_exclusive).await
    }

    async fn expire_until(&self, earliest: i64, end_exclusive: i64) -> Result<usize> {
        let snapshot_manager = self.table.snapshot_manager();
        if end_exclusive <= earliest {
            // Nothing expires; record the earliest snapshot so later runs find
            // it without listing the directory.
            if !snapshot_manager.earliest_hint_exists().await? {
                snapshot_manager.write_earliest_hint(earliest).await?;
            }
            return Ok(0);
        }

        // Futures are built eagerly: a borrowing closure inside the stream
        // would make callers' futures lose `Send`.
        let reads = (earliest..=end_exclusive)
            .map(|id| snapshot_manager.try_get_snapshot(id))
            .collect::<Vec<_>>();
        let snapshots_including_end = stream::iter(reads)
            .buffered(SNAPSHOT_READ_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let Some(first) = snapshots_including_end.first() else {
            return Ok(0);
        };
        let begin_inclusive = first.id();
        let snapshots_excluding_end = snapshots_including_end
            .iter()
            .filter(|snapshot| snapshot.id() != end_exclusive)
            .collect::<Vec<_>>();

        let tagged = self.tagged_snapshots().await?;
        let deletion = SnapshotDeletion::new(self.table)?;

        // Data files deleted by a snapshot are unused from that snapshot on,
        // so the range is (begin, end].
        let data_snapshots = snapshots_including_end
            .iter()
            .filter(|snapshot| snapshot.id() != begin_inclusive)
            .collect::<Vec<_>>();
        self.clean_data_files(&deletion, &data_snapshots, &tagged)
            .await;

        let mut changelog_files = Vec::new();
        for snapshot in &snapshots_excluding_end {
            changelog_files.extend(deletion.changelog_files(snapshot).await);
        }
        deletion.delete_quietly(changelog_files).await;

        let last = snapshots_including_end
            .last()
            .expect("checked non-empty above");
        if last.id() != end_exclusive {
            // The end snapshot is gone, so nothing protects the manifests it
            // would share; stop before deleting any of them.
            return Ok(0);
        }
        let mut skipping_snapshots = find_skipping_tags(&tagged, begin_inclusive, end_exclusive);
        skipping_snapshots.push(last);
        match deletion.manifest_skipping_set(&skipping_snapshots).await {
            Ok(mut skipping) => {
                let mut manifest_files = Vec::new();
                for snapshot in &snapshots_excluding_end {
                    manifest_files.extend(
                        deletion
                            .unused_manifest_paths(snapshot, &mut skipping)
                            .await?,
                    );
                }
                deletion.delete_quietly(manifest_files).await;
            }
            Err(error) => log::info!(
                "Skip cleaning manifest files because the skipping set cannot be built: {error}"
            ),
        }

        for snapshot in &snapshots_excluding_end {
            if let Err(error) = snapshot_manager.delete_snapshot(snapshot.id()).await {
                log::warn!("Failed to delete snapshot {}: {error}", snapshot.id());
            }
        }
        snapshot_manager.write_earliest_hint(end_exclusive).await?;
        Ok(snapshots_excluding_end.len())
    }

    /// Delete the data files each snapshot's delta removes, keeping the ones
    /// the closest earlier tag still reads. A file removed after a tag was taken
    /// is live in that tag and in no later snapshot, so the closest earlier tag
    /// is the only one that can still read it.
    async fn clean_data_files(
        &self,
        deletion: &SnapshotDeletion,
        snapshots: &[&Snapshot],
        tagged: &[Snapshot],
    ) {
        let plans = snapshots
            .iter()
            .map(|snapshot| deletion.plan_deleted_in_delta_manifest(snapshot))
            .collect::<Vec<_>>();
        let plans = stream::iter(plans)
            .buffered(SNAPSHOT_READ_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;

        let mut tag_files: HashMap<i64, Option<HashSet<DataFileKey>>> = HashMap::new();
        let mut paths = Vec::new();
        for (snapshot, plan) in snapshots.iter().zip(&plans) {
            let Some(tag) = previous_tag(tagged, snapshot.id()) else {
                paths.extend(plan.paths_to_delete(None));
                continue;
            };
            if let Entry::Vacant(slot) = tag_files.entry(tag.id()) {
                let files = match deletion.tag_data_files(tag).await {
                    Ok(files) => Some(files),
                    Err(error) => {
                        log::info!(
                            "Failed to read the data files of tag snapshot {}: {error}",
                            tag.id()
                        );
                        None
                    }
                };
                slot.insert(files);
            }
            match &tag_files[&tag.id()] {
                Some(files) => paths.extend(plan.paths_to_delete(Some(files))),
                None => log::info!(
                    "Skip cleaning data files of snapshot {} because tag snapshot {} cannot be read",
                    snapshot.id(),
                    tag.id()
                ),
            }
        }
        deletion.delete_quietly(paths).await;
    }

    /// Snapshots of all tags, sorted by id and deduplicated.
    async fn tagged_snapshots(&self) -> Result<Vec<Snapshot>> {
        let mut seen = HashSet::new();
        let mut snapshots = self
            .table
            .tag_manager()
            .list_all()
            .await?
            .into_iter()
            .map(|(_, snapshot)| snapshot)
            .filter(|snapshot| seen.insert(snapshot.id()))
            .collect::<Vec<_>>();
        snapshots.sort_by_key(Snapshot::id);
        Ok(snapshots)
    }
}

fn positive(name: &str, value: i32) -> Result<i32> {
    if value < 1 {
        return Err(Error::DataInvalid {
            message: format!("{name} must be at least 1, got {value}"),
            source: None,
        });
    }
    Ok(value)
}

fn current_time_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

/// The tagged snapshot with the largest id below `snapshot_id`.
fn previous_tag(sorted_tags: &[Snapshot], snapshot_id: i64) -> Option<&Snapshot> {
    let index = sorted_tags.partition_point(|tag| tag.id() < snapshot_id);
    index.checked_sub(1).map(|index| &sorted_tags[index])
}

/// Tags whose manifests must survive expiring `[begin_inclusive,
/// end_exclusive)`: every tag inside the range plus the closest one at or
/// before `begin_inclusive`. Java `ExpireSnapshotsImpl#findSkippingTags`.
fn find_skipping_tags(
    sorted_tags: &[Snapshot],
    begin_inclusive: i64,
    end_exclusive: i64,
) -> Vec<&Snapshot> {
    // Largest index with id < end_exclusive.
    let Some(right) = sorted_tags
        .partition_point(|tag| tag.id() < end_exclusive)
        .checked_sub(1)
    else {
        return Vec::new();
    };
    // Largest index with id <= begin_inclusive, or 0.
    let left = sorted_tags
        .partition_point(|tag| tag.id() <= begin_inclusive)
        .saturating_sub(1);
    sorted_tags[left..=right].iter().collect()
}

#[cfg(test)]
mod tests;
