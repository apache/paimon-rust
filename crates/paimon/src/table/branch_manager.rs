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

//! Branch manager for reading and managing branch metadata using FileIO.
//!
//! Reference: [org.apache.paimon.utils.BranchManager](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/utils/BranchManager.java)
//! and [org.apache.paimon.utils.FileSystemBranchManager](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/utils/FileSystemBranchManager.java)

use crate::catalog::DEFAULT_MAIN_BRANCH;
use crate::io::FileIO;
use crate::table::{SchemaManager, SnapshotManager, TagManager};
use opendal::raw::get_basename;
use std::collections::HashMap;

/// Table option a reader falls back to when the primary branch has no data.
const SCAN_FALLBACK_BRANCH: &str = "scan.fallback-branch";
/// Table option naming the branch a reader treats as primary.
const SCAN_PRIMARY_BRANCH: &str = "scan.primary-branch";

const BRANCH_DIR: &str = "branch";
const BRANCH_PREFIX: &str = "branch-";

/// Manager for branch directories using unified FileIO.
///
/// Branches are stored as sub-directories at `{table_path}/branch/branch-{name}`.
///
/// Reference: [org.apache.paimon.utils.BranchManager](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/utils/BranchManager.java)
#[derive(Debug, Clone)]
pub struct BranchManager {
    file_io: FileIO,
    table_path: String,
}

impl BranchManager {
    pub fn new(file_io: FileIO, table_path: String) -> Self {
        Self {
            file_io,
            table_path,
        }
    }

    /// Path to the branch directory (e.g. `table_path/branch`).
    pub fn branch_directory(&self) -> String {
        format!("{}/{}", self.table_path, BRANCH_DIR)
    }

    /// Path to the branch sub-directory for the given name (e.g. `branch/branch-my_branch`).
    pub fn branch_path(&self, branch_name: &str) -> String {
        format!(
            "{}/{}{}",
            self.branch_directory(),
            BRANCH_PREFIX,
            branch_name
        )
    }

    /// Validate branch name format.
    ///
    /// Rules:
    /// - Cannot be "main"
    /// - Cannot be blank or whitespace only
    /// - Cannot be a pure numeric string
    fn validate_branch_name(branch_name: &str) -> crate::Result<()> {
        if branch_name == DEFAULT_MAIN_BRANCH {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Branch name '{}' is the default branch and cannot be used.",
                    DEFAULT_MAIN_BRANCH
                ),
                source: None,
            });
        }
        if branch_name.trim().is_empty() {
            return Err(crate::Error::DataInvalid {
                message: format!("Branch name '{}' is blank.", branch_name),
                source: None,
            });
        }
        if branch_name.chars().all(|c| c.is_ascii_digit()) {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Branch name cannot be pure numeric string but is '{}'.",
                    branch_name
                ),
                source: None,
            });
        }
        Ok(())
    }

    /// Validate branch name and ensure it does not already exist.
    pub async fn validate_branch(&self, branch_name: &str) -> crate::Result<()> {
        Self::validate_branch_name(branch_name)?;
        if self.branch_exists(branch_name).await? {
            return Err(crate::Error::DataInvalid {
                message: format!("Branch name '{}' already exists.", branch_name),
                source: None,
            });
        }
        Ok(())
    }

    /// Check if a branch exists.
    pub async fn branch_exists(&self, branch_name: &str) -> crate::Result<bool> {
        let path = format!("{}/", self.branch_path(branch_name));
        self.file_io.exists(&path).await
    }

    /// List all branch names sorted ascending. Returns an empty vector when the
    /// branch directory does not exist.
    pub async fn list_all(&self) -> crate::Result<Vec<String>> {
        let branch_dir = self.branch_directory();
        let statuses = match self.file_io.list_status(&branch_dir).await {
            Ok(s) => s,
            Err(crate::Error::IoUnexpected { ref source, .. })
                if source.kind() == opendal::ErrorKind::NotFound =>
            {
                return Ok(Vec::new());
            }
            Err(e) => return Err(e),
        };
        let mut names: Vec<String> = statuses
            .into_iter()
            .filter(|s| s.is_dir)
            .filter_map(|s| {
                get_basename(&s.path)
                    .strip_suffix("/")
                    .and_then(|base| base.strip_prefix(BRANCH_PREFIX))
                    .map(str::to_string)
            })
            .collect();
        names.sort_unstable();
        Ok(names)
    }

    /// Create a new branch by copying the latest schema to the branch directory.
    pub async fn create_branch(&self, branch_name: &str) -> crate::Result<()> {
        self.validate_branch(branch_name).await?;
        let schema_manager = SchemaManager::new(self.file_io.clone(), self.table_path.clone());
        if let Some(latest) = schema_manager.latest().await? {
            self.copy_schemas_to_branch(branch_name, latest.id())
                .await?;
        }
        Ok(())
    }

    /// Create a new branch from a tag by copying the tag, snapshot and schemas.
    pub async fn create_branch_from_tag(
        &self,
        branch_name: &str,
        tag_name: &str,
    ) -> crate::Result<()> {
        self.validate_branch(branch_name).await?;
        let tag_manager = TagManager::new(self.file_io.clone(), self.table_path.clone());
        let snapshot =
            tag_manager
                .get(tag_name)
                .await?
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: format!("Tag '{}' does not exist.", tag_name),
                    source: None,
                })?;

        let snapshot_manager = SnapshotManager::new(self.file_io.clone(), self.table_path.clone());

        // Copy tag file to branch
        let tag_src = tag_manager.tag_path(tag_name);
        let tag_dst = tag_manager.with_branch(branch_name).tag_path(tag_name);
        self.file_io.copy_file(&tag_src, &tag_dst).await?;

        // Copy snapshot file to branch
        let snap_src = snapshot_manager.snapshot_path(snapshot.id());
        let snap_dst = snapshot_manager
            .with_branch(branch_name)
            .snapshot_path(snapshot.id());
        self.file_io.copy_file(&snap_src, &snap_dst).await?;

        // Copy schemas to branch
        self.copy_schemas_to_branch(branch_name, snapshot.schema_id())
            .await?;

        Ok(())
    }

    /// Drop an existing branch.
    pub async fn drop_branch(&self, branch_name: &str) -> crate::Result<()> {
        if !self.branch_exists(branch_name).await? {
            return Err(crate::Error::DataInvalid {
                message: format!("Branch name '{}' doesn't exist.", branch_name),
                source: None,
            });
        }
        let path = self.branch_path(branch_name);
        self.file_io.delete_dir(&path).await?;
        Ok(())
    }

    /// Reject deleting a branch a reader is configured to consult.
    ///
    /// A branch named by `scan.primary-branch` or `scan.fallback-branch` is part
    /// of a table's read path; dropping it would break reads that fall back to
    /// it. Mirrors Java `AbstractFileStoreTable.deleteBranch`, which refuses the
    /// deletion and asks the caller to unset the option first. Callers pass the
    /// table options because these keys live on the schema, not the manager.
    pub fn ensure_branch_deletable(
        options: &HashMap<String, String>,
        branch_name: &str,
    ) -> crate::Result<()> {
        for key in [SCAN_PRIMARY_BRANCH, SCAN_FALLBACK_BRANCH] {
            if options
                .get(key)
                .is_some_and(|configured| configured == branch_name)
            {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Cannot delete branch '{branch_name}' because it is configured as '{key}'. Unset '{key}' first."
                    ),
                    source: None,
                });
            }
        }
        Ok(())
    }

    /// Rename an existing branch.
    pub async fn rename_branch(&self, from: &str, to: &str) -> crate::Result<()> {
        if from == DEFAULT_MAIN_BRANCH {
            return Err(crate::Error::DataInvalid {
                message: "Cannot rename the main branch.".to_string(),
                source: None,
            });
        }
        if !self.branch_exists(from).await? {
            return Err(crate::Error::DataInvalid {
                message: format!("Branch name '{}' doesn't exist.", from),
                source: None,
            });
        }
        Self::validate_branch_name(to)?;
        if self.branch_exists(to).await? {
            return Err(crate::Error::DataInvalid {
                message: format!("Branch name '{}' already exists.", to),
                source: None,
            });
        }
        let src = self.branch_path(from);
        let dst = self.branch_path(to);
        match self.file_io.rename(&src, &dst).await {
            Ok(()) => Ok(()),
            Err(crate::Error::IoUnexpected { ref source, .. })
                if source.kind() == opendal::ErrorKind::Unsupported =>
            {
                self.copy_dir_recursive(&src, &dst).await?;
                self.file_io.delete_dir(&src).await?;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Recursively copy a directory tree from src to dst.
    async fn copy_dir_recursive(&self, src: &str, dst: &str) -> crate::Result<()> {
        use std::collections::VecDeque;

        self.file_io.mkdirs(dst).await?;
        let mut queue = VecDeque::new();
        queue.push_back((src.to_string(), dst.to_string()));

        while let Some((current_src, current_dst)) = queue.pop_front() {
            self.file_io.mkdirs(&current_dst).await?;
            let statuses = self.file_io.list_status(&current_src).await?;
            for status in statuses {
                let name = get_basename(&status.path).trim_end_matches('/');
                let src_path = format!("{}/{}", current_src.trim_end_matches('/'), name);
                let dst_path = format!("{}/{}", current_dst.trim_end_matches('/'), name);
                if status.is_dir {
                    queue.push_back((src_path, dst_path));
                } else {
                    self.file_io.copy_file(&src_path, &dst_path).await?;
                }
            }
        }
        Ok(())
    }

    /// Copy all schemas with id <= schema_id to the branch directory.
    async fn copy_schemas_to_branch(&self, branch_name: &str, schema_id: i64) -> crate::Result<()> {
        let schema_manager = SchemaManager::new(self.file_io.clone(), self.table_path.clone());
        let ids = schema_manager.list_all_ids().await?;
        let branch_schema_manager = schema_manager.with_branch(branch_name);
        for id in ids {
            if id <= schema_id {
                let src = schema_manager.schema_path(id);
                let dst = branch_schema_manager.schema_path(id);
                self.file_io.copy_file(&src, &dst).await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::{CommitKind, Schema, Snapshot, TableSchema};
    use bytes::Bytes;

    fn test_file_io() -> FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    fn test_schema() -> TableSchema {
        let schema = Schema::builder().build().unwrap();
        TableSchema::new(0, &schema)
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

    async fn write_schema(file_io: &FileIO, schema_manager: &SchemaManager, schema: &TableSchema) {
        let path = schema_manager.schema_path(schema.id());
        let json = serde_json::to_string(schema).unwrap();
        let output = file_io.new_output(&path).unwrap();
        output.write(Bytes::from(json)).await.unwrap();
    }

    async fn write_snapshot(
        file_io: &FileIO,
        snapshot_manager: &SnapshotManager,
        snapshot: &Snapshot,
    ) {
        let path = snapshot_manager.snapshot_path(snapshot.id());
        let json = serde_json::to_string(snapshot).unwrap();
        let output = file_io.new_output(&path).unwrap();
        output.write(Bytes::from(json)).await.unwrap();
    }

    async fn write_tag(
        file_io: &FileIO,
        tag_manager: &TagManager,
        name: &str,
        snapshot: &Snapshot,
    ) {
        let path = tag_manager.tag_path(name);
        let json = serde_json::to_string(snapshot).unwrap();
        let output = file_io.new_output(&path).unwrap();
        output.write(Bytes::from(json)).await.unwrap();
    }

    #[tokio::test]
    async fn test_list_all_missing_dir_returns_empty() {
        let file_io = test_file_io();
        let bm = BranchManager::new(file_io, "memory:/test_branch_missing".to_string());
        assert!(bm.list_all().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_list_all_sorted() {
        let file_io = test_file_io();
        let table_path = "memory:/test_branch_sorted".to_string();
        let branch_dir = format!("{table_path}/branch");
        file_io.mkdirs(&branch_dir).await.unwrap();

        for name in ["b", "a", "c"] {
            let path = format!("{branch_dir}/branch-{name}");
            file_io.mkdirs(&path).await.unwrap();
        }

        let bm = BranchManager::new(file_io, table_path);
        assert_eq!(bm.list_all().await.unwrap(), vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn test_branch_exists() {
        let file_io = test_file_io();
        let table_path = "memory:/test_branch_exists".to_string();
        let branch_dir = format!("{table_path}/branch");
        file_io.mkdirs(&branch_dir).await.unwrap();
        file_io
            .mkdirs(&format!("{branch_dir}/branch-foo"))
            .await
            .unwrap();

        let bm = BranchManager::new(file_io, table_path);
        assert!(bm.branch_exists("foo").await.unwrap());
        assert!(!bm.branch_exists("bar").await.unwrap());
    }

    #[tokio::test]
    async fn test_validate_branch_name_rejects_main() {
        let result = BranchManager::validate_branch_name("main");
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("main"));
    }

    #[tokio::test]
    async fn test_validate_branch_name_rejects_blank() {
        let result = BranchManager::validate_branch_name("");
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("blank"));
    }

    #[tokio::test]
    async fn test_validate_branch_name_rejects_numeric() {
        let result = BranchManager::validate_branch_name("123");
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("numeric"));
    }

    #[tokio::test]
    async fn test_validate_branch_name_accepts_valid() {
        assert!(BranchManager::validate_branch_name("my_branch").is_ok());
        assert!(BranchManager::validate_branch_name("branch-1").is_ok());
    }

    #[tokio::test]
    async fn test_create_branch() {
        let file_io = test_file_io();
        let table_path = "memory:/test_create_branch".to_string();
        let schema_manager = SchemaManager::new(file_io.clone(), table_path.clone());
        write_schema(&file_io, &schema_manager, &test_schema()).await;

        let bm = BranchManager::new(file_io.clone(), table_path.clone());
        bm.create_branch("my_branch").await.unwrap();

        assert!(bm.branch_exists("my_branch").await.unwrap());
        // Verify schema was copied
        let branch_schema_manager = schema_manager.with_branch("my_branch");
        let schemas = branch_schema_manager.list_all().await.unwrap();
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].id(), 0);
    }

    #[tokio::test]
    async fn test_create_branch_already_exists() {
        let file_io = test_file_io();
        let table_path = "memory:/test_create_branch_exists".to_string();
        let schema_manager = SchemaManager::new(file_io.clone(), table_path.clone());
        write_schema(&file_io, &schema_manager, &test_schema()).await;

        let bm = BranchManager::new(file_io, table_path);
        bm.create_branch("my_branch").await.unwrap();
        let result = bm.create_branch("my_branch").await;
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("already exists"));
    }

    #[tokio::test]
    async fn test_create_branch_from_tag() {
        let file_io = test_file_io();
        let table_path = "memory:/test_create_branch_from_tag".to_string();
        let schema_manager = SchemaManager::new(file_io.clone(), table_path.clone());
        let snapshot_manager = SnapshotManager::new(file_io.clone(), table_path.clone());
        let tag_manager = TagManager::new(file_io.clone(), table_path.clone());

        write_schema(&file_io, &schema_manager, &test_schema()).await;
        let snap = test_snapshot(1);
        write_snapshot(&file_io, &snapshot_manager, &snap).await;
        write_tag(&file_io, &tag_manager, "v1", &snap).await;

        let bm = BranchManager::new(file_io.clone(), table_path.clone());
        bm.create_branch_from_tag("my_branch", "v1").await.unwrap();

        assert!(bm.branch_exists("my_branch").await.unwrap());

        // Verify snapshot was copied
        let branch_snap_manager = snapshot_manager.with_branch("my_branch");
        let copied = branch_snap_manager.get_snapshot(1).await.unwrap();
        assert_eq!(copied.id(), 1);

        // Verify tag was copied
        let branch_tag_manager = tag_manager.with_branch("my_branch");
        let tag_snap = branch_tag_manager.get("v1").await.unwrap();
        assert!(tag_snap.is_some());

        // Verify schema was copied
        let branch_schema_manager = schema_manager.with_branch("my_branch");
        let schemas = branch_schema_manager.list_all().await.unwrap();
        assert_eq!(schemas.len(), 1);
    }

    #[tokio::test]
    async fn test_drop_branch() {
        let file_io = test_file_io();
        let table_path = "memory:/test_drop_branch".to_string();
        let schema_manager = SchemaManager::new(file_io.clone(), table_path.clone());
        write_schema(&file_io, &schema_manager, &test_schema()).await;

        let bm = BranchManager::new(file_io, table_path);
        bm.create_branch("to_drop").await.unwrap();
        assert!(bm.branch_exists("to_drop").await.unwrap());

        bm.drop_branch("to_drop").await.unwrap();
        assert!(!bm.branch_exists("to_drop").await.unwrap());
    }

    #[tokio::test]
    async fn test_drop_nonexistent_branch() {
        let file_io = test_file_io();
        let bm = BranchManager::new(file_io, "memory:/test_drop_none".to_string());
        let result = bm.drop_branch("nonexistent").await;
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("doesn't exist"));
    }

    #[test]
    fn test_ensure_branch_deletable_rejects_scan_branches() {
        for key in ["scan.primary-branch", "scan.fallback-branch"] {
            let mut options = HashMap::new();
            options.insert(key.to_string(), "prod".to_string());

            // The branch the option points at is part of a read path.
            let err = BranchManager::ensure_branch_deletable(&options, "prod").unwrap_err();
            let msg = format!("{err}");
            assert!(msg.contains("prod"), "{msg}");
            assert!(msg.contains(key), "{msg}");

            // A different branch is unaffected even while the option is set,
            // so the guard keys on the configured value, not its mere presence.
            BranchManager::ensure_branch_deletable(&options, "feature").unwrap();
        }
    }

    #[test]
    fn test_ensure_branch_deletable_without_scan_options() {
        let options = HashMap::new();
        BranchManager::ensure_branch_deletable(&options, "any").unwrap();
    }

    #[tokio::test]
    async fn test_rename_branch() {
        let file_io = test_file_io();
        let table_path = "memory:/test_rename_branch".to_string();
        let schema_manager = SchemaManager::new(file_io.clone(), table_path.clone());
        write_schema(&file_io, &schema_manager, &test_schema()).await;

        let bm = BranchManager::new(file_io, table_path);
        bm.create_branch("old_branch").await.unwrap();
        assert!(bm.branch_exists("old_branch").await.unwrap());

        bm.rename_branch("old_branch", "new_branch").await.unwrap();
        assert!(!bm.branch_exists("old_branch").await.unwrap());
        assert!(bm.branch_exists("new_branch").await.unwrap());

        let branches = bm.list_all().await.unwrap();
        assert!(branches.contains(&"new_branch".to_string()));
        assert!(!branches.contains(&"old_branch".to_string()));
    }

    #[tokio::test]
    async fn test_rename_nonexistent_branch() {
        let file_io = test_file_io();
        let bm = BranchManager::new(file_io, "memory:/test_rename_none".to_string());
        let result = bm.rename_branch("nonexistent", "new_name").await;
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("doesn't exist"));
    }

    #[tokio::test]
    async fn test_rename_to_existing_branch() {
        let file_io = test_file_io();
        let table_path = "memory:/test_rename_to_existing".to_string();
        let schema_manager = SchemaManager::new(file_io.clone(), table_path.clone());
        write_schema(&file_io, &schema_manager, &test_schema()).await;

        let bm = BranchManager::new(file_io, table_path);
        bm.create_branch("branch1").await.unwrap();
        bm.create_branch("branch2").await.unwrap();

        let result = bm.rename_branch("branch1", "branch2").await;
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("already exists"));
    }

    #[tokio::test]
    async fn test_rename_main_branch_should_fail() {
        let file_io = test_file_io();
        let bm = BranchManager::new(file_io, "memory:/test_rename_main".to_string());
        let result = bm.rename_branch("main", "new_name").await;
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("Cannot rename the main branch"));
    }

    #[tokio::test]
    async fn test_rename_branch_multiple_times() {
        let file_io = test_file_io();
        let table_path = "memory:/test_rename_multi".to_string();
        let schema_manager = SchemaManager::new(file_io.clone(), table_path.clone());
        write_schema(&file_io, &schema_manager, &test_schema()).await;

        let bm = BranchManager::new(file_io, table_path);
        bm.create_branch("branch1").await.unwrap();
        bm.rename_branch("branch1", "branch2").await.unwrap();
        bm.rename_branch("branch2", "branch3").await.unwrap();

        assert!(!bm.branch_exists("branch1").await.unwrap());
        assert!(!bm.branch_exists("branch2").await.unwrap());
        assert!(bm.branch_exists("branch3").await.unwrap());
    }
}
