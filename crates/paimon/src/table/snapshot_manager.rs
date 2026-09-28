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

//! Snapshot manager for reading file and catalog snapshot metadata.
//!
//! Reference:[org.apache.paimon.utils.SnapshotManager](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/utils/SnapshotManager.java).
use crate::catalog::DEFAULT_MAIN_BRANCH;
use crate::io::FileIO;
use crate::spec::Snapshot;
use futures::future::try_join_all;
use std::str;

const SNAPSHOT_DIR: &str = "snapshot";
const SNAPSHOT_PREFIX: &str = "snapshot-";
const LATEST_HINT: &str = "LATEST";
const EARLIEST_HINT: &str = "EARLIEST";

/// Manager for snapshot files and REST catalog snapshot resolution.
///
/// Reference: [org.apache.paimon.utils.SnapshotManager](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/utils/SnapshotManager.java).
#[derive(Debug, Clone)]
pub struct SnapshotManager {
    file_io: FileIO,
    table_path: String,
    branch: String,
    rest_env: Option<super::RESTEnv>,
}

impl SnapshotManager {
    /// Create a snapshot manager for the given table path and FileIO.
    pub fn new(file_io: FileIO, table_path: String) -> Self {
        Self {
            file_io,
            table_path,
            branch: DEFAULT_MAIN_BRANCH.to_string(),
            rest_env: None,
        }
    }

    pub(crate) fn with_rest_env(mut self, rest_env: Option<super::RESTEnv>) -> Self {
        self.rest_env = rest_env;
        self
    }

    pub fn file_io(&self) -> &FileIO {
        &self.file_io
    }

    /// Path to the snapshot directory (e.g. `table_path/snapshot`).
    pub fn snapshot_dir(&self) -> String {
        let branch_path = if self.branch == DEFAULT_MAIN_BRANCH {
            self.table_path.clone()
        } else {
            format!("{}/branch/branch-{}", self.table_path, self.branch)
        };
        format!("{branch_path}/{SNAPSHOT_DIR}")
    }

    /// Create a SnapshotManager for a branch of this table.
    pub fn with_branch(&self, branch_name: &str) -> Self {
        let branch = if branch_name.trim().is_empty() {
            DEFAULT_MAIN_BRANCH
        } else {
            branch_name
        };
        Self {
            file_io: self.file_io.clone(),
            table_path: self.table_path.clone(),
            branch: branch.to_string(),
            rest_env: self.rest_env.clone(),
        }
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
    /// REST tables use the catalog snapshot. Otherwise, first tries the LATEST
    /// hint file. If the hint is valid and no next snapshot exists, returns it.
    /// Otherwise falls back to listing snapshot files.
    ///
    /// Reference: [HintFileUtils.findLatest](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/utils/HintFileUtils.java)
    pub async fn get_latest_snapshot_id(&self) -> crate::Result<Option<i64>> {
        if self.rest_env.is_some() {
            return Ok(self
                .get_latest_snapshot()
                .await?
                .map(|snapshot| snapshot.id()));
        }
        self.latest_snapshot_id_from_filesystem().await
    }

    async fn latest_snapshot_id_from_filesystem(&self) -> crate::Result<Option<i64>> {
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

    /// List all snapshot ids sorted ascending. Returns an empty vector when
    /// the snapshot directory does not exist.
    pub async fn list_all_ids(&self) -> crate::Result<Vec<i64>> {
        let snapshot_dir = self.snapshot_dir();
        let statuses = match self.file_io.list_status(&snapshot_dir).await {
            Ok(s) => s,
            Err(crate::Error::IoUnexpected { ref source, .. })
                if source.kind() == opendal::ErrorKind::NotFound =>
            {
                return Ok(Vec::new());
            }
            Err(e) => return Err(e),
        };
        let mut ids: Vec<i64> = statuses
            .into_iter()
            .filter(|s| !s.is_dir)
            .filter_map(|s| {
                let name = s.path.rsplit('/').next().unwrap_or(&s.path);
                name.strip_prefix(SNAPSHOT_PREFIX)?.parse::<i64>().ok()
            })
            .collect();
        ids.sort_unstable();
        Ok(ids)
    }

    /// List all snapshots sorted by id ascending.
    pub async fn list_all(&self) -> crate::Result<Vec<Snapshot>> {
        let ids = self.list_all_ids().await?;
        try_join_all(ids.into_iter().map(|id| self.get_snapshot(id))).await
    }

    /// Get a snapshot by id.
    pub async fn get_snapshot(&self, snapshot_id: i64) -> crate::Result<Snapshot> {
        let snapshot_path = self.snapshot_path(snapshot_id);
        let snap_input = self.file_io.new_input(&snapshot_path)?;
        if !snap_input.exists().await? {
            return Err(crate::Error::SnapshotNotExist { snapshot_id });
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
        if let Some(env) = &self.rest_env {
            // Java REST NotImplementedException (HTTP 501) is a service error,
            // not SnapshotLoader's UnsupportedOperationException fallback.
            return env.load_snapshot(&self.branch).await;
        }
        let snapshot_id = match self.latest_snapshot_id_from_filesystem().await? {
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

    /// Delete a snapshot file by id.
    pub async fn delete_snapshot(&self, snapshot_id: i64) -> crate::Result<()> {
        let path = self.snapshot_path(snapshot_id);
        self.file_io.delete_file(&path).await
    }

    /// Update the EARLIEST hint file.
    pub async fn write_earliest_hint(&self, snapshot_id: i64) -> crate::Result<()> {
        let hint_path = self.earliest_hint_path();
        let output = self.file_io.new_output(&hint_path)?;
        output
            .write(bytes::Bytes::from(snapshot_id.to_string()))
            .await
    }

    /// Returns the first snapshot whose commit time is later than or equal to the given
    /// `timestamp_millis`. If no such snapshot exists, returns None.
    ///
    /// Uses binary search over the actual snapshot ID list to handle gaps from deleted snapshots.
    pub async fn later_or_equal_time_millis(
        &self,
        timestamp_millis: i64,
    ) -> crate::Result<Option<Snapshot>> {
        let ids = self.list_all_ids().await?;
        if ids.is_empty() {
            return Ok(None);
        }

        let latest_snapshot = self.get_snapshot(*ids.last().unwrap()).await?;
        if (latest_snapshot.time_millis() as i64) < timestamp_millis {
            return Ok(None);
        }

        let mut lo: usize = 0;
        let mut hi: usize = ids.len() - 1;
        let mut result: Option<Snapshot> = None;
        while lo <= hi {
            let mid = lo + (hi - lo) / 2;
            let snapshot = self.get_snapshot(ids[mid]).await?;
            let commit_time = snapshot.time_millis() as i64;
            if commit_time < timestamp_millis {
                lo = mid + 1;
            } else if commit_time > timestamp_millis {
                if mid == 0 {
                    result = Some(snapshot);
                    break;
                }
                hi = mid - 1;
                result = Some(snapshot);
            } else {
                result = Some(snapshot);
                break;
            }
        }
        Ok(result)
    }

    /// Returns the first snapshot whose watermark is later than or equal to the given
    /// `watermark`. Snapshots without a watermark — `None`, or `Some(i64::MIN)`,
    /// Flink's no-watermark sentinel — are skipped. If no such snapshot exists,
    /// returns None.
    ///
    /// Uses binary search over the actual snapshot ID list to handle gaps from
    /// deleted snapshots; watermarks are non-decreasing in snapshot order.
    ///
    /// Reference: [SnapshotManager.laterOrEqualWatermark](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/utils/SnapshotManager.java).
    /// The Java binary search can retain the raw mid snapshot after walking
    /// backwards over missing watermark metadata. This implementation only
    /// returns a snapshot whose own effective watermark satisfies the predicate,
    /// preserving the method's contract when watermark metadata is sparse.
    pub async fn later_or_equal_watermark(
        &self,
        watermark: i64,
    ) -> crate::Result<Option<Snapshot>> {
        fn effective_watermark(snapshot: &Snapshot) -> Option<i64> {
            snapshot.watermark().filter(|w| *w != i64::MIN)
        }

        let ids = self.list_all_ids().await?;
        if ids.is_empty() {
            return Ok(None);
        }

        // Find the first snapshot that carries a watermark.
        let mut lo: usize = 0;
        let (first, first_watermark) = loop {
            if lo >= ids.len() {
                return Ok(None);
            }
            let snapshot = self.get_snapshot(ids[lo]).await?;
            if let Some(w) = effective_watermark(&snapshot) {
                break (snapshot, w);
            }
            lo += 1;
        };
        if first_watermark >= watermark {
            return Ok(Some(first));
        }

        let mut hi: usize = ids.len() - 1;
        let mut result: Option<Snapshot> = None;
        while lo <= hi {
            let mid = lo + (hi - lo) / 2;
            // A snapshot without a watermark takes the ordering position of the
            // nearest earlier snapshot that carries one.
            let mut pos = mid;
            let mut snapshot = self.get_snapshot(ids[pos]).await?;
            while effective_watermark(&snapshot).is_none() && pos > lo {
                pos -= 1;
                snapshot = self.get_snapshot(ids[pos]).await?;
            }
            match effective_watermark(&snapshot) {
                // No watermark-bearing snapshot in [lo, mid]: skip the range.
                None => lo = mid + 1,
                Some(w) if w >= watermark => {
                    result = Some(snapshot);
                    if pos == 0 {
                        break;
                    }
                    hi = pos - 1;
                }
                Some(_) => lo = mid + 1,
            }
        }
        Ok(result)
    }

    /// Returns the snapshot whose commit time is earlier than or equal to the given
    /// `timestamp_millis`. If no such snapshot exists, returns None.
    ///
    /// Uses binary search over the actual snapshot ID list to handle gaps from deleted snapshots.
    ///
    /// Reference: [SnapshotManager.earlierOrEqualTimeMills](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/utils/SnapshotManager.java)
    pub async fn earlier_or_equal_time_millis(
        &self,
        timestamp_millis: i64,
    ) -> crate::Result<Option<Snapshot>> {
        let ids = self.list_all_ids().await?;
        if ids.is_empty() {
            return Ok(None);
        }

        let earliest_snapshot = self.get_snapshot(ids[0]).await?;
        if (earliest_snapshot.time_millis() as i64) > timestamp_millis {
            return Ok(None);
        }

        let mut lo: usize = 0;
        let mut hi: usize = ids.len() - 1;
        let mut result: Option<Snapshot> = None;
        while lo <= hi {
            let mid = lo + (hi - lo) / 2;
            let snapshot = self.get_snapshot(ids[mid]).await?;
            let commit_time = snapshot.time_millis() as i64;
            if commit_time > timestamp_millis {
                if mid == 0 {
                    break;
                }
                hi = mid - 1;
            } else if commit_time < timestamp_millis {
                lo = mid + 1;
                result = Some(snapshot);
            } else {
                result = Some(snapshot);
                break;
            }
        }
        Ok(result)
    }

    #[deprecated(note = "Renamed to earlier_or_equal_time_millis")]
    pub async fn earlier_or_equal_time_mills(
        &self,
        timestamp_millis: i64,
    ) -> crate::Result<Option<Snapshot>> {
        self.earlier_or_equal_time_millis(timestamp_millis).await
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

    struct RestFixture {
        table: crate::table::Table,
        response: std::sync::Arc<std::sync::Mutex<(u16, serde_json::Value)>>,
        requests: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        server: tokio::task::JoinHandle<()>,
    }

    impl Drop for RestFixture {
        fn drop(&mut self) {
            self.server.abort();
        }
    }

    async fn rest_fixture() -> RestFixture {
        use crate::api::rest_api::RESTApi;
        use crate::catalog::Identifier;
        use crate::common::Options;
        use crate::spec::{DataType, IntType, Schema, TableSchema};
        use axum::{
            http::{HeaderMap, StatusCode, Uri},
            Json, Router,
        };
        use std::sync::{Arc, Mutex};

        let response = Arc::new(Mutex::new((
            200_u16,
            serde_json::json!({
                "snapshot": {"snapshot": test_snapshot(7), "recordCount": 10}
            }),
        )));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let handler_response = response.clone();
        let handler_requests = requests.clone();
        let app = Router::new().fallback(move |uri: Uri, headers: HeaderMap| {
            let (status, value) = handler_response.lock().unwrap().clone();
            handler_requests.lock().unwrap().push(uri.to_string());
            assert_eq!(headers["authorization"], "Bearer test-token");
            async move { (StatusCode::from_u16(status).unwrap(), Json(value)) }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut options = Options::new();
        options.set("uri", format!("http://{}", listener.local_addr().unwrap()));
        options.set("prefix", "test");
        options.set("token.provider", "bear");
        options.set("token", "test-token");
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let api = Arc::new(RESTApi::new(options.clone(), false).await.unwrap());
        let id = Identifier::new("database", "table");
        let metadata_cache =
            crate::io::FileFormatMetadataCacheContext::from_props(options.to_map()).unwrap();
        let env = crate::table::RESTEnv::new(
            id.clone(),
            "uuid".into(),
            api,
            options,
            false,
            None,
            metadata_cache,
        );
        let (io, manager) = setup("/rest-table").await;
        manager.commit_snapshot(&test_snapshot(2)).await.unwrap();
        let schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .build()
                .unwrap(),
        );
        let table = crate::table::Table::new(io, id, "/rest-table".into(), schema, Some(env));
        RestFixture {
            table,
            response,
            requests,
            server,
        }
    }

    #[tokio::test]
    async fn rest_latest_snapshot_and_id_use_catalog_without_snapshot_file() {
        let fixture = rest_fixture().await;
        let sm = fixture.table.snapshot_manager();
        assert_eq!(sm.get_latest_snapshot().await.unwrap().unwrap().id(), 7);
        assert_eq!(sm.get_latest_snapshot_id().await.unwrap(), Some(7));
        assert!(sm.get_snapshot(7).await.is_err());
        assert_eq!(
            *fixture.requests.lock().unwrap(),
            vec!["/v1/test/databases/database/tables/table/snapshot"; 2]
        );
    }

    #[tokio::test]
    async fn rest_empty_snapshot_is_authoritative() {
        let fixture = rest_fixture().await;
        let sm = fixture.table.snapshot_manager();
        for body in [serde_json::json!({"snapshot": null}), serde_json::json!({})] {
            *fixture.response.lock().unwrap() = (200, body);
            assert!(sm.get_latest_snapshot().await.unwrap().is_none());
            assert_eq!(sm.get_latest_snapshot_id().await.unwrap(), None);
        }
        *fixture.response.lock().unwrap() = (
            404,
            serde_json::json!({
                "code": 404, "resourceType": "SNAPSHOT", "message": "No snapshot"
            }),
        );
        assert!(sm.get_latest_snapshot().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn rest_snapshot_errors_never_fall_back_to_filesystem() {
        let fixture = rest_fixture().await;
        let sm = fixture.table.snapshot_manager();
        for code in [401, 403, 404, 500, 501, 503] {
            *fixture.response.lock().unwrap() = (
                code,
                serde_json::json!({
                    "code": code, "resourceType": "TABLE", "message": "unavailable"
                }),
            );
            assert!(sm.get_latest_snapshot().await.is_err(), "status {code}");
            assert!(sm.get_latest_snapshot_id().await.is_err(), "status {code}");
        }
        *fixture.response.lock().unwrap() = (200, serde_json::json!({"snapshot": {}}));
        assert!(sm.get_latest_snapshot().await.is_err());
    }

    #[tokio::test]
    async fn resolved_schema_copy_preserves_catalog_and_branch() {
        let fixture = rest_fixture().await;
        // Changing the resolved schema must not discard the catalog provider.
        let schema = fixture
            .table
            .schema()
            .copy_with_options(std::collections::HashMap::from([(
                "source.split.target-size".into(),
                "1mb".into(),
            )]));
        let table = fixture
            .table
            .copy_with_resolved_schema(schema, "main")
            .unwrap();
        assert_eq!(
            table
                .snapshot_manager()
                .get_latest_snapshot_id()
                .await
                .unwrap(),
            Some(7)
        );
        let sm = table.snapshot_manager().with_branch("dev");
        assert_eq!(sm.get_latest_snapshot_id().await.unwrap(), Some(7));
        assert_eq!(
            fixture.requests.lock().unwrap().last().unwrap(),
            "/v1/test/databases/database/tables/table%24branch_dev/snapshot"
        );
        assert_eq!(
            sm.with_branch("main")
                .get_latest_snapshot_id()
                .await
                .unwrap(),
            Some(7)
        );
        assert_eq!(
            fixture.requests.lock().unwrap().last().unwrap(),
            "/v1/test/databases/database/tables/table/snapshot"
        );
    }

    fn test_snapshot_with_watermark(id: i64, watermark: Option<i64>) -> Snapshot {
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
            .watermark(watermark)
            .build()
    }

    async fn pick_watermark(sm: &SnapshotManager, w: i64) -> Option<i64> {
        sm.later_or_equal_watermark(w)
            .await
            .unwrap()
            .map(|s| s.id())
    }

    #[tokio::test]
    async fn test_later_or_equal_watermark_empty() {
        let (_, sm) = setup("memory:/test_watermark_empty").await;
        assert!(sm.later_or_equal_watermark(100).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_later_or_equal_watermark_all_sentinel() {
        // Mirrors Java SnapshotManagerTest.testLaterOrEqualWatermark: snapshots
        // whose watermark is all the no-watermark sentinel never match.
        let (_, sm) = setup("memory:/test_watermark_sentinel").await;
        for id in 1..=3 {
            sm.commit_snapshot(&test_snapshot_with_watermark(id, Some(i64::MIN)))
                .await
                .unwrap();
        }
        assert!(sm.later_or_equal_watermark(100).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_later_or_equal_watermark_picks_earliest_match() {
        let (_, sm) = setup("memory:/test_watermark_earliest").await;
        for (id, w) in [(1, 100), (2, 200), (3, 200), (4, 300)] {
            sm.commit_snapshot(&test_snapshot_with_watermark(id, Some(w)))
                .await
                .unwrap();
        }

        assert_eq!(pick_watermark(&sm, 50).await, Some(1));
        assert_eq!(pick_watermark(&sm, 100).await, Some(1));
        assert_eq!(pick_watermark(&sm, 150).await, Some(2));
        // Equal watermarks still select the earliest matching snapshot.
        assert_eq!(pick_watermark(&sm, 200).await, Some(2));
        assert_eq!(pick_watermark(&sm, 201).await, Some(4));
        assert_eq!(pick_watermark(&sm, 300).await, Some(4));
        // Later than every watermark: no match.
        assert_eq!(pick_watermark(&sm, 301).await, None);
    }

    #[tokio::test]
    async fn test_later_or_equal_watermark_skips_missing_watermarks() {
        let (_, sm) = setup("memory:/test_watermark_skip_none").await;
        sm.commit_snapshot(&test_snapshot_with_watermark(1, None))
            .await
            .unwrap();
        sm.commit_snapshot(&test_snapshot_with_watermark(2, Some(200)))
            .await
            .unwrap();
        sm.commit_snapshot(&test_snapshot_with_watermark(3, None))
            .await
            .unwrap();
        sm.commit_snapshot(&test_snapshot_with_watermark(4, Some(300)))
            .await
            .unwrap();

        assert_eq!(pick_watermark(&sm, 50).await, Some(2));
        assert_eq!(pick_watermark(&sm, 200).await, Some(2));
        assert_eq!(pick_watermark(&sm, 250).await, Some(4));
        assert_eq!(pick_watermark(&sm, 301).await, None);
    }

    #[tokio::test]
    async fn test_later_or_equal_watermark_with_id_gaps() {
        // Deleted snapshots leave holes in the id list; selection must still work.
        let (_, sm) = setup("memory:/test_watermark_gaps").await;
        sm.commit_snapshot(&test_snapshot_with_watermark(2, Some(100)))
            .await
            .unwrap();
        sm.commit_snapshot(&test_snapshot_with_watermark(5, None))
            .await
            .unwrap();
        sm.commit_snapshot(&test_snapshot_with_watermark(9, Some(300)))
            .await
            .unwrap();

        assert_eq!(pick_watermark(&sm, 100).await, Some(2));
        assert_eq!(pick_watermark(&sm, 150).await, Some(9));
        assert_eq!(pick_watermark(&sm, 300).await, Some(9));
        assert_eq!(pick_watermark(&sm, 301).await, None);
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

    #[tokio::test]
    async fn test_list_all_ids_empty() {
        let (_, sm) = setup("memory:/test_list_empty").await;
        assert!(sm.list_all_ids().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_list_all_ids_missing_dir_returns_empty() {
        let file_io = test_file_io();
        let sm = SnapshotManager::new(file_io, "memory:/test_list_missing".to_string());
        assert!(sm.list_all_ids().await.unwrap().is_empty());
        assert!(sm.list_all().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_list_all_ids_sorted() {
        let (_, sm) = setup("memory:/test_list_sorted").await;
        for id in [3, 1, 2] {
            sm.commit_snapshot(&test_snapshot(id)).await.unwrap();
        }
        assert_eq!(sm.list_all_ids().await.unwrap(), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_list_all_loads_in_order() {
        let (_, sm) = setup("memory:/test_list_all").await;
        for id in [2, 1, 3] {
            sm.commit_snapshot(&test_snapshot(id)).await.unwrap();
        }
        let snaps = sm.list_all().await.unwrap();
        let ids: Vec<i64> = snaps.iter().map(|s| s.id()).collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn test_branch_scopes_snapshot_paths_only() {
        let sm = SnapshotManager::new(test_file_io(), "memory:/test_branch_paths".to_string());
        let branch_sm = sm.with_branch("b1");

        assert_eq!(
            branch_sm.snapshot_path(1),
            "memory:/test_branch_paths/branch/branch-b1/snapshot/snapshot-1"
        );
        assert_eq!(
            branch_sm.latest_hint_path(),
            "memory:/test_branch_paths/branch/branch-b1/snapshot/LATEST"
        );
        assert_eq!(
            branch_sm.manifest_path("manifest-list-1"),
            "memory:/test_branch_paths/manifest/manifest-list-1"
        );

        let other_branch_sm = branch_sm.with_branch("b2");
        assert_eq!(
            other_branch_sm.snapshot_dir(),
            "memory:/test_branch_paths/branch/branch-b2/snapshot"
        );
        assert_eq!(
            other_branch_sm.manifest_dir(),
            "memory:/test_branch_paths/manifest"
        );
        assert_eq!(
            other_branch_sm
                .with_branch(DEFAULT_MAIN_BRANCH)
                .snapshot_dir(),
            "memory:/test_branch_paths/snapshot"
        );
    }
}
