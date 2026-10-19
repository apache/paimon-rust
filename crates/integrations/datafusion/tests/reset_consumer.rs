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

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use common::{assert_sql_error, create_sql_context, create_test_env, exec, string_value};
use futures::TryStreamExt;
use paimon::catalog::Identifier;
use paimon::spec::{DataType, IntType, Schema};
use paimon::table::{Plan, Table};
use paimon::Catalog;
use paimon_datafusion::SQLContext;

async fn table_with_snapshots() -> (tempfile::TempDir, SQLContext, Table) {
    let (tmp, catalog) = create_test_env();
    let context = create_sql_context(catalog.clone()).await;
    exec(&context, "CREATE SCHEMA paimon.test_db").await;
    exec(&context, "CREATE TABLE paimon.test_db.events (id INT)").await;
    for id in 1..=3 {
        exec(
            &context,
            &format!("INSERT INTO paimon.test_db.events VALUES ({id})"),
        )
        .await;
    }
    let table = catalog
        .get_table(&Identifier::new("test_db", "events"))
        .await
        .unwrap();
    (tmp, context, table)
}

async fn assert_ok(context: &SQLContext, sql: &str) {
    let batches = context.sql(sql).await.unwrap().collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(batches[0].num_columns(), 1);
    assert_eq!(batches[0].schema().field(0).name(), "result");
    assert_eq!(string_value(batches[0].column(0).as_ref(), 0), "OK");
}

fn table_files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut files = BTreeMap::new();
    for entry in std::fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files.extend(table_files(&path));
        } else {
            files.insert(path.clone(), std::fs::read(path).unwrap());
        }
    }
    files
}

async fn scan_ids(table: &Table, plan: Plan) -> Vec<i32> {
    let batches: Vec<_> = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut ids: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect();
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn reset_creates_overwrites_and_deletes_only_the_requested_consumer() {
    let (tmp, context, table) = table_with_snapshots().await;
    let manager = table.consumer_manager();
    manager.reset("other", 3).await.unwrap();
    let root = tmp.path().join("test_db.db/events");
    let consumer_path = root.join("consumer/consumer-job");
    let original_files = table_files(&root);
    assert!(original_files
        .keys()
        .any(|path| path.starts_with(root.join("snapshot"))));
    assert!(original_files
        .keys()
        .any(|path| path.starts_with(root.join("manifest"))));

    for id in [2, 1] {
        assert_ok(
            &context,
            &format!("CALL sys.reset_consumer(table => 'paimon.test_db.events', consumer_id => 'job', next_snapshot_id => {id})"),
        ).await;
        assert_eq!(manager.get("job").await.unwrap(), Some(id));
        let mut files = table_files(&root);
        let progress: serde_json::Value =
            serde_json::from_slice(&files.remove(&consumer_path).unwrap()).unwrap();
        assert_eq!(progress, serde_json::json!({"nextSnapshot": id}));
        assert_eq!(files, original_files);
    }
    for _ in 0..2 {
        assert_ok(
            &context,
            "CALL sys.reset_consumer(table => 'test_db.events', consumer_id => 'job')",
        )
        .await;
        assert_eq!(manager.get("job").await.unwrap(), None);
        assert_eq!(manager.get("other").await.unwrap(), Some(3));
        assert_eq!(table_files(&root), original_files);
    }
}

#[tokio::test]
async fn deleting_a_missing_consumer_on_an_empty_table_does_not_create_metadata() {
    let (tmp, catalog) = create_test_env();
    let context = create_sql_context(catalog).await;
    exec(&context, "CREATE SCHEMA paimon.test_db").await;
    exec(&context, "CREATE TABLE paimon.test_db.events (id INT)").await;
    let root = tmp.path().join("test_db.db/events");
    let original = table_files(&root);
    assert_ok(
        &context,
        "CALL sys.reset_consumer(table => 'test_db.events', consumer_id => 'job')",
    )
    .await;
    assert_eq!(table_files(&root), original);
    assert!(!root.join("consumer").exists());
    assert_sql_error(
        &context,
        "CALL sys.reset_consumer(table => 'test_db.events', consumer_id => 'job', next_snapshot_id => 1)",
        "does not exist",
    )
    .await;
    assert_eq!(table_files(&root), original);
    assert!(!root.join("consumer").exists());
}

#[tokio::test]
async fn trailing_whitespace_never_targets_another_consumers_progress() {
    let (tmp, context, table) = table_with_snapshots().await;
    let manager = table.consumer_manager();
    manager.reset("job", 2).await.unwrap();
    let root = tmp.path().join("test_db.db/events");
    let original = table_files(&root);
    for id in ["job ", "job\t", "job\n", "job\u{a0}", "job\u{3000}"] {
        assert_eq!(manager.get(id).await.unwrap(), None);
        for snapshot in ["", ", next_snapshot_id => 1"] {
            assert_sql_error(
                &context,
                &format!("CALL sys.reset_consumer(table => 'test_db.events', consumer_id => '{id}'{snapshot})"),
                "trailing whitespace",
            ).await;
            assert_eq!(manager.get("job").await.unwrap(), Some(2));
            assert_eq!(table_files(&root), original);
        }
    }
}

#[tokio::test]
async fn reset_rejects_invalid_arguments_without_changing_files() {
    let (tmp, context, table) = table_with_snapshots().await;
    table.consumer_manager().reset("job", 2).await.unwrap();
    let root = tmp.path().join("test_db.db/events");
    let original = table_files(&root);
    for (args, message) in [
        ("consumer_id => 'job', next_snapshot_id => 1", "Missing required argument: 'table'"),
        ("table => 'test_db.events'", "Missing required argument: 'consumer_id'"),
        ("table => 'test_db.events', consumer_id => 'job', next_snaphot_id => 1", "is unknown"),
        ("table => 'test_db.events', consumer_id => 'job', consumer_id => 'other'", "is duplicated"),
        ("table => 'test_db.events', consumer_id => 'job', next_snapshot_id => 1, NEXT_SNAPSHOT_ID => 2", "is duplicated"),
        ("'test_db.events', 'job', 1", "require named arguments"),
        ("table => 'test_db.events', consumer_id => 'job', next_snapshot_id => NULL", "Unsupported argument value"),
        ("table => 'other.test_db.events', consumer_id => 'job'", "Catalog name mismatch"),
    ] {
        assert_sql_error(&context, &format!("CALL sys.reset_consumer({args})"), message).await;
        assert_eq!(table_files(&root), original);
    }
    for id in [
        "0",
        "-1",
        "1.5",
        "9223372036854775808",
        "'invalid'",
        "''",
        "true",
    ] {
        assert_sql_error(
            &context,
            &format!("CALL sys.reset_consumer(table => 'test_db.events', consumer_id => 'job', next_snapshot_id => {id})"),
            "Invalid next_snapshot_id",
        ).await;
        assert_eq!(table_files(&root), original);
    }
    for id in ["", ".", "..", "../job", r"job\part"] {
        for snapshot in ["", ", next_snapshot_id => 1"] {
            assert_sql_error(
                &context,
                &format!("CALL sys.reset_consumer(table => 'test_db.events', consumer_id => '{id}'{snapshot})"),
                "consumer id must be",
            ).await;
            assert_eq!(table_files(&root), original);
        }
    }
    for id in [4, 99] {
        assert_sql_error(
            &context,
            &format!("CALL sys.reset_consumer(table => 'test_db.events', consumer_id => 'job', next_snapshot_id => {id})"),
            "does not exist",
        ).await;
        assert_eq!(table_files(&root), original);
    }
}

#[tokio::test]
async fn reset_rejects_expired_or_corrupt_snapshots_even_when_a_tag_retains_them() {
    for corrupt in [false, true] {
        let (tmp, context, table) = table_with_snapshots().await;
        exec(
            &context,
            "CALL sys.create_tag(table => 'test_db.events', tag => 'keep', snapshot_id => 1)",
        )
        .await;
        table.consumer_manager().reset("job", 2).await.unwrap();
        let snapshot_path = table.snapshot_manager().snapshot_path(1);
        if corrupt {
            table
                .file_io()
                .new_output(&snapshot_path)
                .unwrap()
                .write("{".into())
                .await
                .unwrap();
        } else {
            table.file_io().delete_file(&snapshot_path).await.unwrap();
        }
        assert_eq!(
            table.tag_manager().get("keep").await.unwrap().unwrap().id(),
            1
        );
        let root = tmp.path().join("test_db.db/events");
        let original = table_files(&root);
        assert_sql_error(
            &context,
            "CALL sys.reset_consumer(table => 'test_db.events', consumer_id => 'job', next_snapshot_id => 1)",
            if corrupt { "snapshot JSON invalid" } else { "does not exist" },
        ).await;
        assert_eq!(table.consumer_manager().get("job").await.unwrap(), Some(2));
        assert_eq!(table_files(&root), original);
    }
}

#[tokio::test]
async fn reset_rejects_format_tables_before_accessing_consumer_metadata() {
    let (tmp, catalog) = create_test_env();
    catalog
        .create_database("test_db", false, Default::default())
        .await
        .unwrap();
    let identifier = Identifier::new("test_db", "events");
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .option("type", "format-table")
        .option("file.format", "parquet")
        .build()
        .unwrap();
    catalog
        .create_table(&identifier, schema, false)
        .await
        .unwrap();
    let table = catalog.get_table(&identifier).await.unwrap();
    table.consumer_manager().reset("job", 2).await.unwrap();
    let context = create_sql_context(catalog).await;
    let root = tmp.path().join("test_db.db/events");
    let original = table_files(&root);
    for snapshot in ["", ", next_snapshot_id => 1"] {
        assert_sql_error(
            &context,
            &format!("CALL sys.reset_consumer(table => 'test_db.events', consumer_id => 'job'{snapshot})"),
            "requires a FileStoreTable",
        ).await;
        assert_eq!(table_files(&root), original);
    }
}

#[tokio::test]
async fn sql_reset_is_used_by_new_scans_and_explicit_restore_takes_precedence() {
    let (_tmp, context, table) = table_with_snapshots().await;
    assert_ok(&context, "CALL sys.reset_consumer(table => 'test_db.events', consumer_id => 'job', next_snapshot_id => 1)").await;
    let mut running = table.new_stream_scan().unwrap();
    running.with_consumer_id("job").unwrap();
    assert_eq!(
        scan_ids(&table, running.plan().await.unwrap().unwrap()).await,
        vec![1]
    );
    assert_ok(&context, "CALL sys.reset_consumer(table => 'test_db.events', consumer_id => 'job', next_snapshot_id => 3)").await;
    assert_eq!(
        scan_ids(&table, running.plan().await.unwrap().unwrap()).await,
        vec![2]
    );
    let mut fresh = table.new_stream_scan().unwrap();
    fresh.with_consumer_id("job").unwrap();
    let plan = fresh.plan().await.unwrap().unwrap();
    assert_eq!(plan.snapshot_id(), Some(3));
    assert!(plan.splits().iter().all(|split| split.is_streaming()));
    assert_eq!(scan_ids(&table, plan).await, vec![3]);
    let mut restored = table.new_stream_scan().unwrap();
    restored
        .with_consumer_id("job")
        .unwrap()
        .restore(Some(1))
        .unwrap();
    assert_eq!(
        scan_ids(&table, restored.plan().await.unwrap().unwrap()).await,
        vec![1]
    );
    assert_ok(&context, "CALL sys.reset_consumer(table => 'test_db.events', consumer_id => 'job', next_snapshot_id => 1)").await;
    running
        .notify_checkpoint_complete(running.checkpoint())
        .await
        .unwrap();
    assert_eq!(table.consumer_manager().get("job").await.unwrap(), Some(3));
}

#[tokio::test]
async fn sql_delete_restores_initial_scan_behavior_and_checkpoint_can_recreate_progress() {
    let (_tmp, context, table) = table_with_snapshots().await;
    let mut waiting = table.new_stream_scan().unwrap();
    waiting
        .with_consumer_id("job")
        .unwrap()
        .restore(Some(3))
        .unwrap();
    waiting.plan().await.unwrap().unwrap();
    waiting
        .notify_checkpoint_complete(waiting.checkpoint())
        .await
        .unwrap();
    assert_eq!(table.consumer_manager().get("job").await.unwrap(), Some(4));
    assert!(table.snapshot_manager().get_snapshot(4).await.is_err());
    assert_ok(
        &context,
        "CALL sys.reset_consumer(table => 'test_db.events', consumer_id => 'job')",
    )
    .await;
    assert_eq!(table.consumer_manager().get("job").await.unwrap(), None);
    let mut fresh = table.new_stream_scan().unwrap();
    fresh.with_consumer_id("job").unwrap();
    let plan = fresh.plan().await.unwrap().unwrap();
    assert_eq!(plan.snapshot_id(), Some(3));
    assert!(!plan.splits().is_empty());
    assert!(plan.splits().iter().all(|split| !split.is_streaming()));
    assert_eq!(scan_ids(&table, plan).await, vec![1, 2, 3]);
    assert!(waiting.plan().await.unwrap().is_none());
    assert_eq!(waiting.checkpoint(), Some(4));
    waiting
        .notify_checkpoint_complete(waiting.checkpoint())
        .await
        .unwrap();
    assert_eq!(table.consumer_manager().get("job").await.unwrap(), Some(4));
}
