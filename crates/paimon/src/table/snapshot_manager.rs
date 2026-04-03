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

//! Snapshot manager for reading snapshot metadata using FileIO.
//!
//! Reference:[org.apache.paimon.utils.SnapshotManager](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/utils/SnapshotManager.java).
use crate::io::FileIO;
use crate::spec::Snapshot;
use std::str;

const SNAPSHOT_DIR: &str = "snapshot";
const SNAPSHOT_PREFIX: &str = "snapshot-";
const LATEST_HINT: &str = "LATEST";
const EARLIEST_HINT: &str = "EARLIEST";

/// Manager for snapshot files using unified FileIO.
///
/// Reference: [org.apache.paimon.utils.SnapshotManager](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/utils/SnapshotManager.java).
#[derive(Debug, Clone)]
pub struct SnapshotManager {
    file_io: FileIO,
    table_path: String,
}

impl SnapshotManager {
    /// Create a snapshot manager for the given table path and FileIO.
    pub fn new(file_io: FileIO, table_path: String) -> Self {
        Self {
            file_io,
            table_path,
        }
    }

    /// Path to the snapshot directory (e.g. `table_path/snapshot`).
    pub fn snapshot_dir(&self) -> String {
        format!("{}/{}", self.table_path, SNAPSHOT_DIR)
    }

    /// Path to the LATEST hint file.
    fn latest_hint_path(&self) -> String {
        format!("{}/{}", self.snapshot_dir(), LATEST_HINT)
    }

    /// Path to the EARLIEST hint file.
    fn earliest_hint_path(&self) -> String {
        format!("{}/{}", self.snapshot_dir(), EARLIEST_HINT)
    }

    /// Path to the snapshot file for the given id (e.g. `snapshot/snapshot-1`).
    pub fn snapshot_path(&self, snapshot_id: i64) -> String {
        format!("{}/snapshot-{}", self.snapshot_dir(), snapshot_id)
    }

    /// Read a hint file and return the id, or None if the file does not exist,
    /// is being deleted, or contains invalid content.
    ///
    /// Reference: [HintFileUtils.readHint](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/utils/HintFileUtils.java)
    async fn read_hint(&self, path: &str) -> Option<i64> {
        let input = self.file_io.new_input(path).ok()?;
        // Try to read directly without exists() check to avoid TOCTOU race.
        // The file may be deleted or overwritten concurrently.
        let content = input.read().await.ok()?;
        let id_str = str::from_utf8(&content).ok()?;
        id_str.trim().parse().ok()
    }

    /// List snapshot files and find the id using the given reducer (min or max).
    async fn find_by_list_files(&self, reducer: fn(i64, i64) -> i64) -> crate::Result<Option<i64>> {
        let snapshot_dir = self.snapshot_dir();
        let statuses = self.file_io.list_status(&snapshot_dir).await?;
        let mut result: Option<i64> = None;
        for status in statuses {
            if status.is_dir {
                continue;
            }
            let name = status.path.rsplit('/').next().unwrap_or(&status.path);
            if let Some(id_str) = name.strip_prefix(SNAPSHOT_PREFIX) {
                if let Ok(id) = id_str.parse::<i64>() {
                    result = Some(match result {
                        Some(r) => reducer(r, id),
                        None => id,
                    });
                }
            }
        }
        Ok(result)
    }

    /// Get the latest snapshot id.
    ///
    /// First tries the LATEST hint file. If the hint is valid and no next snapshot
    /// exists, returns it. Otherwise falls back to listing snapshot files.
    ///
    /// Reference: [HintFileUtils.findLatest](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/utils/HintFileUtils.java)
    pub async fn get_latest_snapshot_id(&self) -> crate::Result<Option<i64>> {
        let hint_path = self.latest_hint_path();
        if let Some(hint_id) = self.read_hint(&hint_path).await {
            if hint_id > 0 {
                let next_path = self.snapshot_path(hint_id + 1);
                let next_input = self.file_io.new_input(&next_path)?;
                if !next_input.exists().await? {
                    return Ok(Some(hint_id));
                }
            }
        }
        self.find_by_list_files(i64::max).await
    }

    /// Get the earliest snapshot id.
    ///
    /// First tries the EARLIEST hint file. If the hint is valid and the snapshot
    /// file exists, returns it. Otherwise falls back to listing snapshot files.
    ///
    /// Reference: [HintFileUtils.findEarliest](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/utils/HintFileUtils.java)
    pub async fn earliest_snapshot_id(&self) -> crate::Result<Option<i64>> {
        let hint_path = self.earliest_hint_path();
        if let Some(hint_id) = self.read_hint(&hint_path).await {
            let snap_path = self.snapshot_path(hint_id);
            let snap_input = self.file_io.new_input(&snap_path)?;
            if snap_input.exists().await? {
                return Ok(Some(hint_id));
            }
        }
        self.find_by_list_files(i64::min).await
    }

    /// Get a snapshot by id. Returns an error if the snapshot file does not exist.
    pub async fn get_snapshot(&self, snapshot_id: i64) -> crate::Result<Snapshot> {
        let snapshot_path = self.snapshot_path(snapshot_id);
        let snap_input = self.file_io.new_input(&snapshot_path)?;
        if !snap_input.exists().await? {
            return Err(crate::Error::DataInvalid {
                message: format!("snapshot file does not exist: {snapshot_path}"),
                source: None,
            });
        }
        let snap_bytes = snap_input.read().await?;
        let snapshot: Snapshot =
            serde_json::from_slice(&snap_bytes).map_err(|e| crate::Error::DataInvalid {
                message: format!("snapshot JSON invalid: {e}"),
                source: Some(Box::new(e)),
            })?;
        if snapshot.id() != snapshot_id {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "snapshot file id mismatch: in file name is {snapshot_id}, but file contains snapshot id {}",
                    snapshot.id()
                ),
                source: None
            });
        }
        Ok(snapshot)
    }

    /// Get the latest snapshot, or None if no snapshots exist.
    pub async fn get_latest_snapshot(&self) -> crate::Result<Option<Snapshot>> {
        let snapshot_id = match self.get_latest_snapshot_id().await? {
            Some(id) => id,
            None => return Ok(None),
        };
        let snapshot = self.get_snapshot(snapshot_id).await?;
        Ok(Some(snapshot))
    }

    /// Returns the snapshot whose commit time is earlier than or equal to the given
    /// `timestamp_millis`. If no such snapshot exists, returns None.
    ///
    /// Uses binary search over snapshot IDs (assumes monotonically increasing commit times).
    ///
    /// Reference: [SnapshotManager.earlierOrEqualTimeMills](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/utils/SnapshotManager.java)
    pub async fn earlier_or_equal_time_mills(
        &self,
        timestamp_millis: i64,
    ) -> crate::Result<Option<Snapshot>> {
        let mut latest = match self.get_latest_snapshot_id().await? {
            Some(id) => id,
            None => return Ok(None),
        };

        let earliest_snapshot = match self.earliest_snapshot_id().await? {
            Some(id) => self.get_snapshot(id).await?,
            None => return Ok(None),
        };

        // If the earliest snapshot is already after the timestamp, no match.
        if (earliest_snapshot.time_millis() as i64) > timestamp_millis {
            return Ok(None);
        }
        let mut earliest = earliest_snapshot.id();

        let mut result: Option<Snapshot> = None;
        while earliest <= latest {
            let mid = earliest + (latest - earliest) / 2;
            let snapshot = self.get_snapshot(mid).await?;
            let commit_time = snapshot.time_millis() as i64;
            if commit_time > timestamp_millis {
                latest = mid - 1;
            } else if commit_time < timestamp_millis {
                earliest = mid + 1;
                result = Some(snapshot);
            } else {
                result = Some(snapshot);
                break;
            }
        }
        Ok(result)
    }
}
