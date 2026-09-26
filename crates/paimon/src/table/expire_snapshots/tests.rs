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

use super::{find_skipping_tags, previous_tag};
use crate::catalog::Identifier;
use crate::io::FileIOBuilder;
use crate::spec::{
    DataType, FileKind, IndexManifest, IntType, ManifestList, Schema, Snapshot, TableSchema,
    VarCharType, SCAN_SNAPSHOT_ID_OPTION, SCAN_TAG_NAME_OPTION,
};
use crate::table::{CommitMessage, Table, TableCommit, TableWrite};
use crate::Error;
use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use futures::TryStreamExt;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

fn test_table(table_path: &str, options: &[(&str, &str)], partitioned: bool) -> Table {
    let mut builder = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("dt", DataType::VarChar(VarCharType::string_type()));
    if partitioned {
        builder = builder.partition_keys(["dt"]);
    }
    for (key, value) in options {
        builder = builder.option(*key, *value);
    }
    let schema = builder.build().unwrap();
    Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("default", "expire_table"),
        table_path.to_string(),
        TableSchema::new(0, &schema),
        None,
    )
}

async fn setup_dirs(table: &Table) {
    for dir in ["snapshot", "manifest"] {
        table
            .file_io()
            .mkdirs(&format!("{}/{dir}/", table.location()))
            .await
            .unwrap();
    }
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

async fn write(table: &Table, ids: &[i32], dt: &str) -> Vec<CommitMessage> {
    let mut table_write = TableWrite::new(table, "test-user".to_string()).unwrap();
    table_write
        .write_arrow_batch(&batch(ids, dt))
        .await
        .unwrap();
    table_write.prepare_commit().await.unwrap()
}

async fn append(table: &Table, ids: &[i32]) {
    let messages = write(table, ids, "a").await;
    TableCommit::new(table.clone(), "test-user".to_string())
        .commit(messages)
        .await
        .unwrap();
}

async fn overwrite(table: &Table, ids: &[i32]) {
    let messages = write(table, ids, "a").await;
    TableCommit::new(table.clone(), "test-user".to_string())
        .overwrite(messages, None)
        .await
        .unwrap();
}

async fn snapshot_ids(table: &Table) -> Vec<i64> {
    table.snapshot_manager().list_all_ids().await.unwrap()
}

async fn read_ids(table: &Table) -> Vec<i32> {
    let read_builder = table.new_read_builder();
    let plan = read_builder.new_scan().plan().await.unwrap();
    let batches = read_builder
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

async fn read_ids_at(table: &Table, option: &str, value: &str) -> Vec<i32> {
    let table = table.copy_with_options(HashMap::from([(option.to_string(), value.to_string())]));
    read_ids(&table).await
}

/// Expire with only `retain_min` limiting the run.
async fn expire_keeping(table: &Table, retain_min: i32) -> usize {
    table
        .new_expire_snapshots()
        .with_retain_min(retain_min)
        .with_older_than_millis(i64::MAX)
        .with_max_deletes(i32::MAX)
        .execute()
        .await
        .unwrap()
}

async fn file_names_under(table: &Table, dir: &str) -> BTreeSet<String> {
    let path = format!("{}/{dir}", table.location());
    if !table.file_io().exists_dir(&path).await.unwrap() {
        return BTreeSet::new();
    }
    table
        .file_io()
        .list_status_recursive(&path)
        .await
        .unwrap()
        .into_iter()
        .filter(|status| !status.is_dir)
        .map(|status| status.path.rsplit('/').next().unwrap().to_string())
        .collect()
}

/// Every file under a bucket directory, by name.
async fn physical_data_files(table: &Table) -> BTreeSet<String> {
    table
        .file_io()
        .list_status_recursive(table.location())
        .await
        .unwrap()
        .into_iter()
        .filter(|status| !status.is_dir && status.path.contains("/bucket-"))
        .map(|status| status.path.rsplit('/').next().unwrap().to_string())
        .collect()
}

/// Snapshots that must stay readable: every remaining snapshot and every tag.
async fn live_snapshots(table: &Table) -> Vec<Snapshot> {
    let mut snapshots = table.snapshot_manager().list_all().await.unwrap();
    snapshots.extend(
        table
            .tag_manager()
            .list_all()
            .await
            .unwrap()
            .into_iter()
            .map(|(_, snapshot)| snapshot),
    );
    snapshots
}

/// The core safety property of expiration: afterwards, the files on disk are
/// exactly the files some remaining snapshot or tag references. Nothing a
/// reader needs is gone, and nothing only expired snapshots used is left.
///
/// Live data files come from the scan planner, not from the deletion code.
async fn assert_files_match_references(table: &Table) {
    let mut data_files = BTreeSet::new();
    let mut manifest_files = BTreeSet::new();
    let mut index_files = BTreeSet::new();
    let file_io = table.file_io();
    let manifest_path = |name: &str| format!("{}/manifest/{name}", table.location());
    for snapshot in live_snapshots(table).await {
        let entries = table
            .new_read_builder()
            .new_scan()
            .with_scan_all_files()
            .plan_manifest_entries(&snapshot)
            .await
            .unwrap();
        for entry in entries {
            assert_eq!(*entry.kind(), FileKind::Add);
            data_files.insert(entry.file().file_name.clone());
            data_files.extend(entry.file().extra_files.iter().cloned());
        }
        let mut lists = vec![
            snapshot.base_manifest_list().to_string(),
            snapshot.delta_manifest_list().to_string(),
        ];
        lists.extend(snapshot.changelog_manifest_list().map(str::to_string));
        for list in lists {
            for manifest in ManifestList::read(file_io, &manifest_path(&list))
                .await
                .unwrap()
            {
                manifest_files.insert(manifest.file_name().to_string());
            }
            manifest_files.insert(list);
        }
        if let Some(index_manifest) = snapshot.index_manifest() {
            for entry in IndexManifest::read(file_io, &manifest_path(index_manifest))
                .await
                .unwrap()
            {
                index_files.insert(entry.index_file.file_name);
            }
            manifest_files.insert(index_manifest.to_string());
        }
    }
    assert_eq!(physical_data_files(table).await, data_files, "data files");
    assert_eq!(
        file_names_under(table, "manifest").await,
        manifest_files,
        "manifest files"
    );
    assert_eq!(
        file_names_under(table, "index").await,
        index_files,
        "index files"
    );
}

#[tokio::test]
async fn test_empty_table_expires_nothing() {
    let table = test_table("memory:/expire_empty", &[], false);
    setup_dirs(&table).await;
    assert_eq!(expire_keeping(&table, 1).await, 0);
}

#[tokio::test]
async fn test_append_only_expiration_keeps_every_data_file() {
    let table = test_table("memory:/expire_append", &[], false);
    setup_dirs(&table).await;
    for id in 1..=5 {
        append(&table, &[id]).await;
    }
    let data_before = physical_data_files(&table).await;

    assert_eq!(expire_keeping(&table, 2).await, 3);
    assert_eq!(snapshot_ids(&table).await, vec![4, 5]);
    assert_eq!(
        table
            .snapshot_manager()
            .earliest_snapshot_id()
            .await
            .unwrap(),
        Some(4)
    );
    // Appends never delete data, so every data file is still referenced.
    assert_eq!(physical_data_files(&table).await, data_before);
    assert_files_match_references(&table).await;
    assert_eq!(read_ids(&table).await, vec![1, 2, 3, 4, 5]);

    // Nothing left to expire.
    assert_eq!(expire_keeping(&table, 2).await, 0);
}

#[tokio::test]
async fn test_overwritten_data_files_are_deleted() {
    let table = test_table("memory:/expire_overwrite", &[], false);
    setup_dirs(&table).await;
    append(&table, &[1]).await;
    overwrite(&table, &[2]).await;
    overwrite(&table, &[3]).await;
    overwrite(&table, &[4]).await;
    assert_eq!(physical_data_files(&table).await.len(), 4);

    assert_eq!(expire_keeping(&table, 1).await, 3);
    assert_eq!(snapshot_ids(&table).await, vec![4]);
    assert_eq!(physical_data_files(&table).await.len(), 1);
    assert_files_match_references(&table).await;
    assert_eq!(read_ids(&table).await, vec![4]);
}

#[tokio::test]
async fn test_partitioned_overwrite_resolves_partition_paths() {
    let table = test_table("memory:/expire_partitioned", &[], true);
    setup_dirs(&table).await;
    let messages = write(&table, &[1], "x").await;
    TableCommit::new(table.clone(), "u".to_string())
        .commit(messages)
        .await
        .unwrap();
    let messages = write(&table, &[2], "y").await;
    TableCommit::new(table.clone(), "u".to_string())
        .commit(messages)
        .await
        .unwrap();
    // Overwrite only partition x.
    let messages = write(&table, &[3], "x").await;
    TableCommit::new(table.clone(), "u".to_string())
        .overwrite(
            messages,
            Some(HashMap::from([(
                "dt".to_string(),
                Some(crate::spec::Datum::String("x".to_string())),
            )])),
        )
        .await
        .unwrap();

    assert_eq!(expire_keeping(&table, 1).await, 2);
    assert_files_match_references(&table).await;
    assert_eq!(read_ids(&table).await, vec![2, 3]);
}

#[tokio::test]
async fn test_tag_keeps_its_data_files_and_manifests() {
    let table = test_table("memory:/expire_tag", &[], false);
    setup_dirs(&table).await;
    append(&table, &[1]).await;
    overwrite(&table, &[2]).await;
    let tagged = table.snapshot_manager().get_snapshot(2).await.unwrap();
    table.tag_manager().create("t2", &tagged).await.unwrap();
    overwrite(&table, &[3]).await;
    overwrite(&table, &[4]).await;

    assert_eq!(expire_keeping(&table, 1).await, 3);
    assert_eq!(snapshot_ids(&table).await, vec![4]);
    // Data of the tagged snapshot 2 survives; data of 1 and 3 is gone.
    assert_eq!(physical_data_files(&table).await.len(), 2);
    assert_files_match_references(&table).await;
    assert_eq!(
        read_ids_at(&table, SCAN_TAG_NAME_OPTION, "t2").await,
        vec![2]
    );
    assert_eq!(read_ids(&table).await, vec![4]);
}

#[tokio::test]
async fn test_consumer_protects_the_snapshot_it_reads() {
    let table = test_table("memory:/expire_consumer", &[], false);
    setup_dirs(&table).await;
    for id in 1..=5 {
        overwrite(&table, &[id]).await;
    }
    table
        .file_io()
        .new_output(&format!("{}/consumer/consumer-reader", table.location()))
        .unwrap()
        .write(bytes::Bytes::from(r#"{"nextSnapshot":3}"#))
        .await
        .unwrap();

    assert_eq!(expire_keeping(&table, 1).await, 2);
    assert_eq!(snapshot_ids(&table).await, vec![3, 4, 5]);
    assert_files_match_references(&table).await;
    assert_eq!(
        read_ids_at(&table, SCAN_SNAPSHOT_ID_OPTION, "3").await,
        vec![3]
    );
}

#[tokio::test]
async fn test_retention_rules() {
    let table = test_table(
        "memory:/expire_rules",
        &[
            ("snapshot.num-retained.min", "2"),
            ("snapshot.num-retained.max", "3"),
        ],
        false,
    );
    setup_dirs(&table).await;
    for id in 1..=6 {
        append(&table, &[id]).await;
    }
    let time_of = |snapshot: Snapshot| snapshot.time_millis() as i64;
    let sm = table.snapshot_manager();

    // Nothing is older than `snapshot.time-retained` (1 h) yet, and the table's
    // `snapshot.num-retained.max` = 3 still expires the snapshots beyond it.
    let now = time_of(sm.get_latest_snapshot().await.unwrap().unwrap());
    let expired = table
        .new_expire_snapshots()
        .with_current_time_millis(now)
        .execute()
        .await
        .unwrap();
    assert_eq!(expired, 3);
    assert_eq!(snapshot_ids(&table).await, vec![4, 5, 6]);

    // `older_than` expires a snapshot only once its successor is older: 4
    // stays because 5 was not committed before the cut-off.
    let snapshot_5 = time_of(sm.get_snapshot(5).await.unwrap());
    let expired = table
        .new_expire_snapshots()
        .with_older_than_millis(snapshot_5)
        .execute()
        .await
        .unwrap();
    assert_eq!(expired, 0, "snapshot 4 is kept while 5 is not older");

    // `max_deletes` bounds one run; `retain_min` is the floor.
    let expired = table
        .new_expire_snapshots()
        .with_retain_min(1)
        .with_older_than_millis(i64::MAX)
        .with_max_deletes(1)
        .execute()
        .await
        .unwrap();
    assert_eq!(expired, 1);
    assert_eq!(snapshot_ids(&table).await, vec![5, 6]);
    assert_files_match_references(&table).await;
    assert_eq!(read_ids(&table).await, vec![1, 2, 3, 4, 5, 6]);
}

#[tokio::test]
async fn test_rejects_invalid_retention() {
    let table = test_table("memory:/expire_invalid", &[], false);
    setup_dirs(&table).await;
    append(&table, &[1]).await;

    let err = table
        .new_expire_snapshots()
        .with_retain_max(1)
        .with_retain_min(2)
        .execute()
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::DataInvalid { ref message, .. } if message.contains("must not be less than")),
        "{err:?}"
    );
    let err = table
        .new_expire_snapshots()
        .with_max_deletes(0)
        .execute()
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::DataInvalid { ref message, .. } if message.contains("max_deletes")),
        "{err:?}"
    );
    let table = test_table(
        "memory:/expire_invalid_option",
        &[("snapshot.num-retained.min", "0")],
        false,
    );
    let err = table.new_expire_snapshots().execute().await.unwrap_err();
    assert!(matches!(err, Error::DataInvalid { .. }), "{err:?}");
}

#[tokio::test]
async fn test_level_upgrade_keeps_the_upgraded_file() {
    let table = test_table("memory:/expire_upgrade", &[], false);
    setup_dirs(&table).await;
    let messages = write(&table, &[1], "a").await;
    let file = messages[0].new_files[0].clone();
    TableCommit::new(table.clone(), "u".to_string())
        .commit(messages.clone())
        .await
        .unwrap();

    // Delete and re-add the same file in one delta, as a level upgrade does.
    let mut upgraded = file.clone();
    upgraded.level = 1;
    let mut upgrade = CommitMessage::new(
        messages[0].partition.clone(),
        messages[0].bucket,
        vec![upgraded],
    );
    upgrade.deleted_files = vec![file.clone()];
    TableCommit::new(table.clone(), "u".to_string())
        .commit(vec![upgrade])
        .await
        .unwrap();
    append(&table, &[2]).await;

    assert_eq!(expire_keeping(&table, 1).await, 2);
    assert!(physical_data_files(&table).await.contains(&file.file_name));
    assert_files_match_references(&table).await;
    assert_eq!(read_ids(&table).await, vec![1, 2]);
}

#[tokio::test]
async fn test_rewritten_files_are_deleted() {
    let table = test_table("memory:/expire_compact", &[], false);
    setup_dirs(&table).await;
    append(&table, &[1]).await;
    append(&table, &[2]).await;
    let before = table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan_manifest_entries(&table.snapshot_manager().get_snapshot(2).await.unwrap())
        .await
        .unwrap();

    // Replace both files with one rewritten file, as a copy-on-write rewrite
    // or a compaction does.
    let rewritten = write(&table, &[1, 2], "a").await;
    let mut compact = CommitMessage::new(
        rewritten[0].partition.clone(),
        rewritten[0].bucket,
        rewritten[0].new_files.clone(),
    );
    compact.deleted_files = before.iter().map(|entry| entry.file().clone()).collect();
    TableCommit::new(table.clone(), "u".to_string())
        .commit(vec![compact])
        .await
        .unwrap();
    append(&table, &[3]).await;

    assert_eq!(expire_keeping(&table, 1).await, 3);
    for entry in &before {
        assert!(!physical_data_files(&table)
            .await
            .contains(&entry.file().file_name));
    }
    assert_files_match_references(&table).await;
    assert_eq!(read_ids(&table).await, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_dropped_global_index_files_are_deleted() {
    let table = test_table(
        "memory:/expire_global_index",
        &[
            ("row-tracking.enabled", "true"),
            ("data-evolution.enabled", "true"),
            ("global-index.enabled", "true"),
        ],
        false,
    );
    setup_dirs(&table).await;
    append(&table, &[1, 2]).await;
    table
        .new_btree_global_index_build_builder()
        .with_index_column("id")
        .execute()
        .await
        .unwrap();
    let index_files = file_names_under(&table, "index").await;
    assert_eq!(index_files.len(), 1);
    table
        .new_global_index_drop_builder()
        .with_index_column("id")
        .execute()
        .await
        .unwrap();
    // A second index that stays referenced.
    table
        .new_btree_global_index_build_builder()
        .with_index_column("dt")
        .execute()
        .await
        .unwrap();
    append(&table, &[3]).await;

    expire_keeping(&table, 1).await;
    let remaining = file_names_under(&table, "index").await;
    assert_eq!(remaining.len(), 1);
    assert!(remaining.is_disjoint(&index_files));
    assert_files_match_references(&table).await;
}

#[tokio::test]
async fn test_missing_end_snapshot_keeps_manifests() {
    let table = test_table("memory:/expire_missing_end", &[], false);
    setup_dirs(&table).await;
    for id in 1..=3 {
        overwrite(&table, &[id]).await;
    }
    let manifests_before = file_names_under(&table, "manifest").await;
    // Snapshot 3 vanishes underneath the run.
    let sm = table.snapshot_manager();
    let expired = table
        .new_expire_snapshots()
        .expire_until(1, 4)
        .await
        .unwrap();
    assert_eq!(expired, 0);
    assert_eq!(snapshot_ids(&table).await, vec![1, 2, 3]);
    assert_eq!(file_names_under(&table, "manifest").await, manifests_before);
    drop(sm);
}

#[tokio::test]
async fn test_writes_earliest_hint_when_nothing_expires() {
    let table = test_table("memory:/expire_hint", &[], false);
    setup_dirs(&table).await;
    append(&table, &[1]).await;
    let sm = table.snapshot_manager();
    assert!(!sm.earliest_hint_exists().await.unwrap());
    assert_eq!(expire_keeping(&table, 1).await, 0);
    assert!(sm.earliest_hint_exists().await.unwrap());
}

fn snapshot_with_id(id: i64) -> Snapshot {
    serde_json::from_value(serde_json::json!({
        "version": 3,
        "id": id,
        "schemaId": 0,
        "baseManifestList": "base",
        "deltaManifestList": "delta",
        "commitUser": "u",
        "commitIdentifier": id,
        "commitKind": "APPEND",
        "timeMillis": 0,
    }))
    .unwrap()
}

#[test]
fn test_previous_tag_and_skipping_tags() {
    let tags = [2, 5, 9].map(snapshot_with_id);
    assert_eq!(previous_tag(&tags, 2).map(Snapshot::id), None);
    assert_eq!(previous_tag(&tags, 3).map(Snapshot::id), Some(2));
    assert_eq!(previous_tag(&tags, 9).map(Snapshot::id), Some(5));
    assert_eq!(previous_tag(&tags, 100).map(Snapshot::id), Some(9));

    let ids = |begin, end| {
        find_skipping_tags(&tags, begin, end)
            .into_iter()
            .map(Snapshot::id)
            .collect::<Vec<_>>()
    };
    // Tags inside [begin, end) plus the closest one at or before begin.
    assert_eq!(ids(3, 9), vec![2, 5]);
    assert_eq!(ids(5, 10), vec![5, 9]);
    assert_eq!(ids(1, 2), Vec::<i64>::new());
    assert_eq!(ids(1, 3), vec![2]);
    assert_eq!(ids(10, 20), vec![9]);
}
