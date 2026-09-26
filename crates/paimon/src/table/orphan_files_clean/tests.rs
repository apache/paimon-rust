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

use super::current_time_millis;
use crate::catalog::Identifier;
use crate::io::FileIOBuilder;
use crate::spec::{DataType, IntType, Schema, TableSchema, VarCharType, SCAN_TAG_NAME_OPTION};
use crate::table::{BranchManager, OrphanFilesCleanResult, Table, TableCommit, TableWrite};
use crate::Error;
use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use futures::TryStreamExt;
use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

const DAY_MS: i64 = 24 * 60 * 60 * 1000;

fn local_table(dir: &TempDir, partitioned: bool) -> Table {
    let mut builder = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("dt", DataType::VarChar(VarCharType::string_type()))
        // Commits must not expire snapshots on their own in these tests.
        .option("write-only", "true");
    if partitioned {
        builder = builder.partition_keys(["dt"]);
    }
    let schema = builder.build().unwrap();
    Table::new(
        FileIOBuilder::new("file").build().unwrap(),
        Identifier::new("default", "orphan_table"),
        format!("file://{}", dir.path().display()),
        TableSchema::new(0, &schema),
        None,
    )
}

async fn setup(dir: &TempDir, partitioned: bool) -> Table {
    let table = local_table(dir, partitioned);
    for sub in ["snapshot", "manifest", "schema"] {
        std::fs::create_dir_all(dir.path().join(sub)).unwrap();
    }
    std::fs::write(
        dir.path().join("schema/schema-0"),
        serde_json::to_vec(table.schema()).unwrap(),
    )
    .unwrap();
    table
}

fn batch(ids: &[i32], dt: &str) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, true),
        ArrowField::new("dt", ArrowDataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(vec![dt; ids.len()])) as ArrayRef,
        ],
    )
    .unwrap()
}

async fn commit(table: &Table, ids: &[i32], dt: &str, overwrite: bool) {
    let mut table_write = TableWrite::new(table, "u".to_string()).unwrap();
    table_write
        .write_arrow_batch(&batch(ids, dt))
        .await
        .unwrap();
    let messages = table_write.prepare_commit().await.unwrap();
    let commit = TableCommit::new(table.clone(), "u".to_string());
    if overwrite {
        commit.overwrite(messages, None).await.unwrap();
    } else {
        commit.commit(messages).await.unwrap();
    }
}

async fn read_ids(table: &Table) -> Vec<i32> {
    let builder = table.new_read_builder();
    let plan = builder.new_scan().plan().await.unwrap();
    let batches = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let mut ids = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

/// Run a cleanup two days from now, so every existing file is older than the
/// default one-day cut-off.
async fn clean_later(table: &Table, dry_run: bool) -> OrphanFilesCleanResult {
    table
        .new_remove_orphan_files()
        .with_dry_run(dry_run)
        .with_current_time_millis(current_time_millis() + 2 * DAY_MS)
        .execute()
        .await
        .unwrap()
}

fn plant(dir: &TempDir, relative: &str) -> String {
    let path = dir.path().join(relative);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, b"orphan").unwrap();
    relative.to_string()
}

fn all_files(root: &Path) -> BTreeSet<String> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeSet<String>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, out);
            } else {
                out.insert(path.strip_prefix(root).unwrap().display().to_string());
            }
        }
    }
    let mut out = BTreeSet::new();
    walk(root, root, &mut out);
    out
}

/// Deleted paths relative to the table directory, whatever URI form the
/// file system reports them in.
fn relative(dir: &TempDir, result: &OrphanFilesCleanResult) -> BTreeSet<String> {
    let root = format!("{}/", dir.path().display());
    result
        .deleted_files
        .iter()
        .map(|path| {
            let start = path.find(&root).expect("path under the table directory");
            path[start + root.len()..].to_string()
        })
        .collect()
}

fn bucket_dir(dir: &TempDir) -> String {
    all_files(dir.path())
        .into_iter()
        .find(|path| path.contains("bucket-"))
        .map(|path| path.rsplit_once('/').unwrap().0.to_string())
        .expect("a data file exists")
}

#[tokio::test]
async fn test_removes_only_unreferenced_files() {
    let dir = tempfile::tempdir().unwrap();
    let table = setup(&dir, false).await;
    commit(&table, &[1], "a", false).await;
    commit(&table, &[2], "a", false).await;
    let referenced = all_files(dir.path());

    let bucket = bucket_dir(&dir);
    let orphans = BTreeSet::from([
        plant(&dir, &format!("{bucket}/data-orphan.parquet")),
        plant(&dir, "manifest/manifest-orphan-0"),
        plant(&dir, "index/index-orphan-0"),
        plant(&dir, "statistics/stat-orphan"),
        // Left behind by an interrupted commit.
        plant(&dir, "snapshot/.snapshot-3.tmp"),
    ]);

    let dry = clean_later(&table, true).await;
    assert_eq!(relative(&dir, &dry), orphans);
    assert_eq!(dry.deleted_file_count, 5);
    assert_eq!(dry.deleted_file_total_bytes, 5 * 6);
    // A dry run deletes nothing.
    assert!(orphans.iter().all(|path| dir.path().join(path).exists()));

    let result = clean_later(&table, false).await;
    assert_eq!(relative(&dir, &result), orphans);
    assert_eq!(all_files(dir.path()), referenced);
    assert_eq!(read_ids(&table).await, vec![1, 2]);
    // Nothing left to clean.
    assert_eq!(clean_later(&table, false).await.deleted_file_count, 0);
}

#[tokio::test]
async fn test_recent_files_are_kept() {
    let dir = tempfile::tempdir().unwrap();
    let table = setup(&dir, false).await;
    commit(&table, &[1], "a", false).await;
    let bucket = bucket_dir(&dir);
    plant(&dir, &format!("{bucket}/data-in-flight.parquet"));

    // Default cut-off: one day ago.
    let result = table.new_remove_orphan_files().execute().await.unwrap();
    assert_eq!(result.deleted_file_count, 0);

    let err = table
        .new_remove_orphan_files()
        .with_older_than_millis(current_time_millis() + DAY_MS)
        .execute()
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::DataInvalid { ref message, .. } if message.contains("earlier than now")),
        "{err:?}"
    );
}

#[tokio::test]
async fn test_files_of_snapshots_removed_without_cleanup_are_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let table = setup(&dir, false).await;
    commit(&table, &[1], "a", false).await;
    commit(&table, &[2], "a", true).await;
    // Snapshot 1 disappears without its files being cleaned, as after a
    // crashed expiration.
    std::fs::remove_file(dir.path().join("snapshot/snapshot-1")).unwrap();

    let result = clean_later(&table, false).await;
    let deleted = relative(&dir, &result);
    // Only snapshot 1's own manifest lists go. Its data file is still named by
    // the DELETE entry in snapshot 2's delta, and like Java every entry keeps
    // its file.
    assert!(!deleted.is_empty());
    assert!(
        deleted
            .iter()
            .all(|path| path.starts_with("manifest/manifest-list-")),
        "{deleted:?}"
    );
    assert_eq!(read_ids(&table).await, vec![2]);
    assert_eq!(clean_later(&table, false).await.deleted_file_count, 0);
}

#[tokio::test]
async fn test_tags_branches_and_changelogs_protect_their_files() {
    let dir = tempfile::tempdir().unwrap();
    let table = setup(&dir, false).await;
    commit(&table, &[1], "a", false).await;
    commit(&table, &[2], "a", true).await;
    commit(&table, &[3], "a", true).await;
    commit(&table, &[4], "a", true).await;
    let sm = table.snapshot_manager();

    // Snapshot 1 lives on in a tag, 2 in a branch, 3 in a long-lived changelog.
    table
        .tag_manager()
        .create("t1", &sm.get_snapshot(1).await.unwrap())
        .await
        .unwrap();
    table
        .tag_manager()
        .create("t2", &sm.get_snapshot(2).await.unwrap())
        .await
        .unwrap();
    BranchManager::new(table.file_io().clone(), table.location().to_string())
        .create_branch_from_tag("b2", "t2")
        .await
        .unwrap();
    table.tag_manager().delete("t2").await.unwrap();
    std::fs::create_dir_all(dir.path().join("changelog")).unwrap();
    std::fs::copy(
        dir.path().join("snapshot/snapshot-3"),
        dir.path().join("changelog/changelog-3"),
    )
    .unwrap();
    for id in 1..=3 {
        std::fs::remove_file(dir.path().join(format!("snapshot/snapshot-{id}"))).unwrap();
    }
    let before = all_files(dir.path());

    // Snapshot 2 is now referenced only by branch b2, snapshot 3 only by the
    // changelog; nothing may go.
    let result = clean_later(&table, false).await;
    assert_eq!(result.deleted_file_count, 0, "{:?}", result.deleted_files);
    assert_eq!(all_files(dir.path()), before);

    let at_tag = table.copy_with_options(HashMap::from([(
        SCAN_TAG_NAME_OPTION.to_string(),
        "t1".to_string(),
    )]));
    assert_eq!(read_ids(&at_tag).await, vec![1]);
    assert_eq!(read_ids(&table).await, vec![4]);

    // Once the changelog is gone, snapshot 3's data file is an orphan.
    std::fs::remove_file(dir.path().join("changelog/changelog-3")).unwrap();
    let result = clean_later(&table, false).await;
    assert!(result.deleted_file_count > 0);
    assert_eq!(read_ids(&at_tag).await, vec![1]);
}

#[tokio::test]
async fn test_partitioned_bucket_dirs_are_scanned() {
    let dir = tempfile::tempdir().unwrap();
    let table = setup(&dir, true).await;
    commit(&table, &[1], "x", false).await;
    let bucket = bucket_dir(&dir);
    assert!(bucket.starts_with("dt=x/"), "{bucket}");
    let orphan = plant(&dir, &format!("{bucket}/data-orphan.orc"));
    // Managed BLOB packs are never cleaned here.
    plant(&dir, &format!("{bucket}/pack-0.managed.blob"));

    let result = clean_later(&table, false).await;
    assert_eq!(relative(&dir, &result), BTreeSet::from([orphan]));
    assert_eq!(read_ids(&table).await, vec![1]);
}

#[tokio::test]
async fn test_missing_manifest_of_live_snapshot_aborts() {
    let dir = tempfile::tempdir().unwrap();
    let table = setup(&dir, false).await;
    commit(&table, &[1], "a", false).await;
    let snapshot = table.snapshot_manager().get_snapshot(1).await.unwrap();
    std::fs::remove_file(
        dir.path()
            .join("manifest")
            .join(snapshot.delta_manifest_list()),
    )
    .unwrap();
    let before = all_files(dir.path());

    let err = table
        .new_remove_orphan_files()
        .with_current_time_millis(current_time_millis() + 2 * DAY_MS)
        .execute()
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::DataInvalid { ref message, .. } if message.contains("missing file")),
        "{err:?}"
    );
    assert_eq!(all_files(dir.path()), before);
}

#[tokio::test]
async fn test_branch_without_schema_aborts() {
    let dir = tempfile::tempdir().unwrap();
    let table = setup(&dir, false).await;
    commit(&table, &[1], "a", false).await;
    std::fs::create_dir_all(dir.path().join("branch/branch-broken/snapshot")).unwrap();

    let err = table
        .new_remove_orphan_files()
        .with_current_time_millis(current_time_millis() + 2 * DAY_MS)
        .execute()
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::DataInvalid { ref message, .. } if message.contains("have no schemas")),
        "{err:?}"
    );
}
