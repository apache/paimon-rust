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

    pub fn file_io(&self) -> &FileIO {
        &self.file_io
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

    /// Path to the manifest directory.
    pub fn manifest_dir(&self) -> String {
        format!("{}/manifest", self.table_path)
    }

    /// Path to a manifest file.
    pub fn manifest_path(&self, manifest_name: &str) -> String {
        format!("{}/{}", self.manifest_dir(), manifest_name)
    }

    /// Read a hint file and return the id, or None if the file does not exist,
    /// is being deleted, or contains invalid content.
    ///
    /// Reference: [HintFileUtils.readHint](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/utils/HintFileUtils.java)
    async fn read_hint(&self, path: &str) -> Option<i64> {
        let input = self.file_io.new_input(path).ok()?;
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

    /// Get a snapshot by id.
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

    /// Atomically commit a snapshot.
    ///
    /// Writes the snapshot JSON to the target path. Returns `false` if the
    /// target already exists (another writer won the race).
    ///
    /// On file systems that support atomic rename, we write to a temp file
    /// first then rename. On backends where rename is not supported (e.g.
    /// memory, object stores), we fall back to a direct write after an
    /// existence check.
    pub async fn commit_snapshot(&self, snapshot: &Snapshot) -> crate::Result<bool> {
        let target_path = self.snapshot_path(snapshot.id());

        let json = serde_json::to_string(snapshot).map_err(|e| crate::Error::DataInvalid {
            message: format!("failed to serialize snapshot: {e}"),
            source: Some(Box::new(e)),
        })?;

        // Try rename-based atomic commit first, fall back to check-and-write.
        //
        // TODO: opendal's rename uses POSIX semantics which silently overwrites the target.
        //  The exists() check below narrows the race window but does not eliminate it.
        //  Java Paimon uses `lock.runWithLock(() -> !fileIO.exists(newPath) && callable.call())`
        //  for full mutual exclusion. We need an external lock mechanism (like Java's Lock
        //  interface) for backends without atomic rename-no-replace support.
        let tmp_path = format!("{}.tmp-{}", target_path, uuid::Uuid::new_v4());
        let output = self.file_io.new_output(&tmp_path)?;
        output.write(bytes::Bytes::from(json.clone())).await?;

        // Check before rename to avoid silent overwrite (opendal uses POSIX rename semantics)
        if self.file_io.exists(&target_path).await? {
            let _ = self.file_io.delete_file(&tmp_path).await;
            return Ok(false);
        }

        match self.file_io.rename(&tmp_path, &target_path).await {
            Ok(()) => {}
            Err(_) => {
                // Rename not supported (e.g. memory/object store).
                // Clean up temp file, then check-and-write.
                let _ = self.file_io.delete_file(&tmp_path).await;
                if self.file_io.exists(&target_path).await? {
                    return Ok(false);
                }
                let output = self.file_io.new_output(&target_path)?;
                output.write(bytes::Bytes::from(json)).await?;
            }
        }

        // Update LATEST hint (best-effort)
        let _ = self.write_latest_hint(snapshot.id()).await;
        Ok(true)
    }

    /// Update the LATEST hint file.
    pub async fn write_latest_hint(&self, snapshot_id: i64) -> crate::Result<()> {
        let hint_path = self.latest_hint_path();
        let output = self.file_io.new_output(&hint_path)?;
        output
            .write(bytes::Bytes::from(snapshot_id.to_string()))
            .await
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::CommitKind;

    fn test_file_io() -> FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    async fn setup(table_path: &str) -> (FileIO, SnapshotManager) {
        let file_io = test_file_io();
        file_io
            .mkdirs(&format!("{table_path}/snapshot/"))
            .await
            .unwrap();
        let sm = SnapshotManager::new(file_io.clone(), table_path.to_string());
        (file_io, sm)
    }

    fn test_snapshot(id: i64) -> Snapshot {
        Snapshot::builder()
            .version(3)
            .id(id)
            .schema_id(0)
            .base_manifest_list("base-list".to_string())
            .delta_manifest_list("delta-list".to_string())
            .commit_user("test-user".to_string())
            .commit_identifier(0)
            .commit_kind(CommitKind::APPEND)
            .time_millis(1000 * id as u64)
            .build()
    }

    #[tokio::test]
    async fn test_commit_snapshot_first() {
        let (_, sm) = setup("memory:/test_commit_first").await;
        let snap = test_snapshot(1);
        let result = sm.commit_snapshot(&snap).await.unwrap();
        assert!(result);

        let loaded = sm.get_snapshot(1).await.unwrap();
        assert_eq!(loaded.id(), 1);
    }

    #[tokio::test]
    async fn test_commit_snapshot_already_exists() {
        let (_, sm) = setup("memory:/test_commit_exists").await;
        let snap = test_snapshot(1);
        assert!(sm.commit_snapshot(&snap).await.unwrap());
        // Second commit to same id should return false
        let result = sm.commit_snapshot(&snap).await.unwrap();
        assert!(!result);
    }

    #[tokio::test]
    async fn test_commit_updates_latest_hint() {
        let (_, sm) = setup("memory:/test_commit_hint").await;
        let snap = test_snapshot(1);
        sm.commit_snapshot(&snap).await.unwrap();

        let latest_id = sm.get_latest_snapshot_id().await.unwrap();
        assert_eq!(latest_id, Some(1));
    }

    #[tokio::test]
    async fn test_write_latest_hint() {
        let (_, sm) = setup("memory:/test_write_hint").await;
        sm.write_latest_hint(42).await.unwrap();
        let hint = sm.read_hint(&sm.latest_hint_path()).await;
        assert_eq!(hint, Some(42));
    }
}
