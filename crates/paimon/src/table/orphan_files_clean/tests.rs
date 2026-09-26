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
    setup_table(dir, local_table(dir, partitioned)).await
}

async fn setup_with(dir: &TempDir, schema: Schema) -> Table {
    let table = Table::new(
        FileIOBuilder::new("file").build().unwrap(),
        Identifier::new("default", "orphan_table"),
        format!("file://{}", dir.path().display()),
        TableSchema::new(0, &schema),
        None,
    );
    setup_table(dir, table).await
}

async fn setup_table(dir: &TempDir, table: Table) -> Table {
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

fn base_schema() -> crate::spec::SchemaBuilder {
    Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("dt", DataType::VarChar(VarCharType::string_type()))
}

#[tokio::test]
async fn test_referenced_deletion_vectors_are_kept() {
    let dir = tempfile::tempdir().unwrap();
    let schema = base_schema()
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .option("deletion-vectors.enabled", "true")
        .option("index-file-in-data-file-dir", "true")
        .build()
        .unwrap();
    let table = setup_with(&dir, schema).await;
    commit(&table, &[1, 2, 3], "a", false).await;
    let mut deletion = table.new_write_builder().new_delete().unwrap();
    deletion.add_row_ids([0]).unwrap();
    let messages = deletion.prepare_commit().await.unwrap();
    TableCommit::new(table.clone(), "u".to_string())
        .commit(messages)
        .await
        .unwrap();
    let referenced = all_files(dir.path());
    let bucket = bucket_dir(&dir);
    let dv_in_bucket = referenced
        .iter()
        .filter(|path| path.starts_with(&bucket) && path.contains("index"))
        .count();
    assert_eq!(dv_in_bucket, 1, "{referenced:?}");
    let orphan = plant(&dir, &format!("{bucket}/index-orphan-dv-0"));

    let result = clean_later(&table, false).await;
    assert_eq!(relative(&dir, &result), BTreeSet::from([orphan]));
    assert_eq!(all_files(dir.path()), referenced);
    assert_eq!(read_ids(&table).await, vec![2, 3]);
}

#[tokio::test]
async fn test_referenced_global_index_files_are_kept() {
    let dir = tempfile::tempdir().unwrap();
    let schema = base_schema()
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .option("global-index.enabled", "true")
        .build()
        .unwrap();
    let table = setup_with(&dir, schema).await;
    commit(&table, &[1, 2], "a", false).await;
    table
        .new_btree_global_index_build_builder()
        .with_index_column("id")
        .execute()
        .await
        .unwrap();
    let referenced = all_files(dir.path());
    assert!(
        referenced.iter().any(|path| path.starts_with("index/")),
        "{referenced:?}"
    );
    let orphan = plant(&dir, "index/btree-global-index-orphan.index");

    let result = clean_later(&table, false).await;
    assert_eq!(relative(&dir, &result), BTreeSet::from([orphan]));
    assert_eq!(all_files(dir.path()), referenced);
}

#[tokio::test]
async fn test_external_paths_are_scanned_and_referenced_files_kept() {
    let dir = tempfile::tempdir().unwrap();
    let external = tempfile::tempdir().unwrap();
    let external_root = format!("file://{}", external.path().display());
    let schema = base_schema()
        .option("data-file.external-paths", external_root.as_str())
        .build()
        .unwrap();
    let table = setup_with(&dir, schema).await;
    // Move the written file under the external path, as a writer with
    // `data-file.external-paths` places it.
    let mut messages = {
        let mut table_write = TableWrite::new(&table, "u".to_string()).unwrap();
        table_write
            .write_arrow_batch(&batch(&[1], "a"))
            .await
            .unwrap();
        table_write.prepare_commit().await.unwrap()
    };
    for message in &mut messages {
        for file in &mut message.new_files {
            let local = dir
                .path()
                .join(format!("bucket-{}", message.bucket))
                .join(&file.file_name);
            let target = external
                .path()
                .join(format!("bucket-{}", message.bucket))
                .join(&file.file_name);
            std::fs::create_dir_all(target.parent().unwrap()).unwrap();
            std::fs::rename(&local, &target).unwrap();
            file.external_path = Some(format!("file://{}", target.display()));
        }
    }
    TableCommit::new(table.clone(), "u".to_string())
        .commit(messages)
        .await
        .unwrap();
    assert_eq!(read_ids(&table).await, vec![1]);
    let external_before = all_files(external.path());
    assert_eq!(external_before.len(), 1);
    std::fs::write(
        external.path().join("bucket-0/data-orphan.parquet"),
        b"orphan",
    )
    .unwrap();

    let result = clean_later(&table, false).await;
    assert_eq!(result.deleted_file_count, 1, "{:?}", result.deleted_files);
    assert!(result.deleted_files[0].ends_with("bucket-0/data-orphan.parquet"));
    assert_eq!(all_files(external.path()), external_before);
    assert_eq!(read_ids(&table).await, vec![1]);
}

#[tokio::test]
async fn test_nested_partition_dirs_are_scanned() {
    let dir = tempfile::tempdir().unwrap();
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("dt", DataType::VarChar(VarCharType::string_type()))
        .column("hr", DataType::VarChar(VarCharType::string_type()))
        .partition_keys(["dt", "hr"])
        .build()
        .unwrap();
    let table = setup_with(&dir, schema).await;
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, true),
        ArrowField::new("dt", ArrowDataType::Utf8, true),
        ArrowField::new("hr", ArrowDataType::Utf8, true),
    ]));
    let rows = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            Arc::new(StringArray::from(vec!["d1"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["h1"])) as ArrayRef,
        ],
    )
    .unwrap();
    let mut table_write = TableWrite::new(&table, "u".to_string()).unwrap();
    table_write.write_arrow_batch(&rows).await.unwrap();
    let messages = table_write.prepare_commit().await.unwrap();
    TableCommit::new(table.clone(), "u".to_string())
        .commit(messages)
        .await
        .unwrap();
    let bucket = bucket_dir(&dir);
    assert_eq!(bucket, "dt=d1/hr=h1/bucket-0");
    let orphan = plant(&dir, &format!("{bucket}/data-orphan.parquet"));

    let result = clean_later(&table, false).await;
    assert_eq!(relative(&dir, &result), BTreeSet::from([orphan]));
    assert_eq!(read_ids(&table).await, vec![1]);
}

#[tokio::test]
async fn test_temporary_changelog_files_are_removed() {
    let dir = tempfile::tempdir().unwrap();
    let table = setup(&dir, false).await;
    commit(&table, &[1], "a", false).await;
    let orphan = plant(&dir, "changelog/.changelog-7.tmp");
    // Hint files are never removed.
    plant(&dir, "changelog/EARLIEST");

    let result = clean_later(&table, false).await;
    assert_eq!(relative(&dir, &result), BTreeSet::from([orphan]));
}

#[tokio::test]
async fn test_vanished_owner_is_skipped_but_a_tag_aborts() {
    let dir = tempfile::tempdir().unwrap();
    let table = setup(&dir, false).await;
    commit(&table, &[1], "a", false).await;
    let snapshot = table.snapshot_manager().get_snapshot(1).await.unwrap();
    let clean = super::Clean {
        file_io: table.file_io().clone(),
        table_location: table.location().to_string(),
        older_than: 0,
    };
    let path = format!("{}/manifest/missing", table.location());

    // The snapshot was expired concurrently: nothing to protect.
    let gone = super::Owner {
        file: Some(format!("{}/snapshot/snapshot-99", table.location())),
        snapshot: snapshot.clone(),
    };
    assert!(clean.missing(&gone, &path).await.unwrap().is_none());
    // Still present, or a tag (which must always be readable): abort.
    let live = super::Owner {
        file: Some(format!("{}/snapshot/snapshot-1", table.location())),
        snapshot: snapshot.clone(),
    };
    assert!(clean.missing(&live, &path).await.is_err());
    let tag = super::Owner {
        file: None,
        snapshot,
    };
    assert!(clean.missing(&tag, &path).await.is_err());
}
