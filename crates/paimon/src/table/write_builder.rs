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

//! WriteBuilder for table write API.
//!
//! Reference: [pypaimon WriteBuilder](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/write/write_builder.py)

use crate::table::{Table, TableCommit, TableWrite};
use uuid::Uuid;

/// Builder for creating table writers and committers.
///
/// Provides `new_write` and `new_commit` methods, with optional
/// `overwrite` support for partition-level overwrites.
pub struct WriteBuilder<'a> {
    table: &'a Table,
    commit_user: String,
    overwrite: bool,
}

impl<'a> WriteBuilder<'a> {
    pub fn new(table: &'a Table) -> Self {
        Self {
            table,
            commit_user: Uuid::new_v4().to_string(),
            overwrite: false,
        }
    }

    /// Get the commit user shared by writers and committers created by this builder.
    ///
    /// This value is persisted in snapshot metadata and used for duplicate
    /// commit detection.
    pub fn commit_user(&self) -> &str {
        &self.commit_user
    }

    /// Set the commit user shared by writers and committers created by this builder.
    ///
    /// This value is persisted in snapshot metadata, used for duplicate commit
    /// detection, and embedded in postpone-bucket data file name prefixes. It
    /// should identify a unique commit attempt or job instance, and must be a
    /// safe file name segment.
    pub fn with_commit_user(mut self, commit_user: impl Into<String>) -> crate::Result<Self> {
        let commit_user = commit_user.into();
        validate_commit_user(&commit_user)?;
        self.commit_user = commit_user;
        Ok(self)
    }

    /// Mark writers created by this builder as overwrite-aware.
    ///
    /// The commit kind remains explicit at the commit call site.
    pub fn with_overwrite(mut self) -> Self {
        self.overwrite = true;
        self
    }

    /// Create a new TableCommit for committing write results.
    pub fn new_commit(&self) -> TableCommit {
        TableCommit::new(self.table.clone(), self.commit_user.clone())
    }

    /// Create a new TableWrite for writing Arrow data.
    ///
    /// For primary-key tables, sequence numbers are lazily scanned per partition
    /// when the first writer for that partition is created.
    pub fn new_write(&self) -> crate::Result<TableWrite> {
        let write = TableWrite::new(self.table, self.commit_user.clone())?;
        Ok(if self.overwrite {
            write.with_overwrite()
        } else {
            write
        })
    }
}

fn validate_commit_user(commit_user: &str) -> crate::Result<()> {
    let is_invalid = commit_user.is_empty()
        || commit_user == "."
        || commit_user == ".."
        || commit_user.trim() != commit_user
        || commit_user
            .chars()
            .any(|c| matches!(c, '/' | '\\') || c.is_control());

    if is_invalid {
        return Err(crate::Error::ConfigInvalid {
            message: "commit_user must be a safe file name segment".to_string(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Identifier;
    use crate::io::{FileIO, FileIOBuilder};
    use crate::spec::{CommitKind, DataType, IntType, Schema, TableSchema, POSTPONE_BUCKET};
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use std::sync::Arc;

    fn test_file_io() -> FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    async fn setup_dirs(file_io: &FileIO, table_path: &str) {
        file_io
            .mkdirs(&format!("{table_path}/snapshot/"))
            .await
            .unwrap();
        file_io
            .mkdirs(&format!("{table_path}/manifest/"))
            .await
            .unwrap();
    }

    fn make_batch(ids: Vec<i32>, values: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("value", ArrowDataType::Int32, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int32Array::from(values)),
            ],
        )
        .unwrap()
    }

    fn test_postpone_pk_table(file_io: &FileIO, table_path: &str) -> Table {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .primary_key(["id"])
            .option("bucket", "-2")
            .build()
            .unwrap();
        Table::new(
            file_io.clone(),
            Identifier::new("default", "test_postpone_table"),
            table_path.to_string(),
            TableSchema::new(0, &schema),
            None,
        )
    }

    fn input_changelog_pk_table(file_io: &FileIO, table_path: &str) -> Table {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .primary_key(["id"])
            .option("bucket", "1")
            .option("changelog-producer", "input")
            .build()
            .unwrap();
        Table::new(
            file_io.clone(),
            Identifier::new("default", "test_input_changelog"),
            table_path.to_string(),
            TableSchema::new(0, &schema),
            None,
        )
    }

    #[test]
    fn test_with_commit_user_rejects_invalid_file_name_segments() {
        let table = test_postpone_pk_table(&test_file_io(), "memory:/test_invalid_commit_user");
        for invalid_commit_user in [
            "",
            ".",
            "..",
            "job/1",
            "job\\1",
            " job",
            "job ",
            "job\n1",
            "job\u{7f}",
        ] {
            let err = match table
                .new_write_builder()
                .with_commit_user(invalid_commit_user)
            {
                Ok(_) => panic!("Expected commit_user {invalid_commit_user:?} to be rejected"),
                Err(err) => err,
            };
            assert!(
                matches!(err, crate::Error::ConfigInvalid { ref message }
                    if message.contains("commit_user") && message.contains("file name segment")),
                "Expected ConfigInvalid for commit_user {invalid_commit_user:?}, got: {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_custom_commit_user_is_shared_by_write_and_commit() {
        let file_io = test_file_io();
        let table_path = "memory:/test_write_builder_commit_user";
        setup_dirs(&file_io, table_path).await;

        let table = test_postpone_pk_table(&file_io, table_path);
        let wb = table
            .new_write_builder()
            .with_commit_user("my-commit-user")
            .unwrap();
        assert_eq!(wb.commit_user(), "my-commit-user");

        let mut write = wb.new_write().unwrap();
        write
            .write_arrow_batch(&make_batch(vec![3, 1, 2], vec![30, 10, 20]))
            .await
            .unwrap();

        let messages = write.prepare_commit().await.unwrap();
        assert_eq!(messages[0].bucket, POSTPONE_BUCKET);
        assert!(
            messages[0].new_files[0]
                .file_name
                .starts_with("data-u-my-commit-user-s-"),
            "Expected custom commit user in file name, got: {}",
            messages[0].new_files[0].file_name
        );

        wb.new_commit().commit(messages).await.unwrap();

        let snapshot_manager =
            crate::table::SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snapshot_manager
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.commit_user(), "my-commit-user");
    }

    #[tokio::test]
    async fn test_with_overwrite_marks_new_write_as_overwrite_aware() {
        let file_io = test_file_io();
        let table_path = "memory:/test_write_builder_overwrite";
        setup_dirs(&file_io, table_path).await;

        let table = input_changelog_pk_table(&file_io, table_path);
        let wb = table.new_write_builder().with_overwrite();
        let mut write = wb.new_write().unwrap();
        write
            .write_arrow_batch(&make_batch(vec![1], vec![10]))
            .await
            .unwrap();

        let messages = write.prepare_commit().await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].new_files.len(), 1);
        assert!(
            messages[0].new_changelog_files.is_empty(),
            "Overwrite-aware writer must not produce input changelog files"
        );

        wb.new_commit().commit(messages).await.unwrap();

        let snapshot_manager =
            crate::table::SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snapshot_manager
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.commit_kind(), &CommitKind::APPEND);
    }
}
