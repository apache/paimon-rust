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
use paimon::catalog::Identifier;
use paimon::spec::{DataType, IntType, Schema};
use paimon::table::{BranchManager, Table};
use paimon::Catalog;
use paimon_datafusion::SQLContext;

async fn table_with_snapshot() -> (tempfile::TempDir, SQLContext, Table) {
    let (tmp, catalog) = create_test_env();
    let context = create_sql_context(catalog.clone()).await;
    exec(&context, "CREATE SCHEMA paimon.test_db").await;
    exec(&context, "CREATE TABLE paimon.test_db.events (id INT)").await;
    exec(&context, "INSERT INTO paimon.test_db.events VALUES (1)").await;
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

#[tokio::test]
async fn clear_filters_ids_and_preserves_exceptions_other_files_and_branches() {
    let (tmp, context, table) = table_with_snapshot().await;
    let manager = table.consumer_manager();
    manager.reset("job-old", 1).await.unwrap();
    manager.reset("job-keep", 2).await.unwrap();
    let snapshot = table.snapshot_manager().get_snapshot(1).await.unwrap();
    table.tag_manager().create("base", &snapshot).await.unwrap();
    BranchManager::new(table.file_io().clone(), table.location().to_string())
        .create_branch_from_tag("dev", "base")
        .await
        .unwrap();
    let branch = table.copy_with_branch("dev").await.unwrap();
    branch.consumer_manager().reset("job-old", 3).await.unwrap();

    let root = tmp.path().join("test_db.db/events");
    let directory = root.join("consumer");
    for name in [
        "consumer-job-old",
        "consumer-job-keep-extra",
        "consumer-other",
    ] {
        std::fs::write(directory.join(name), "{").unwrap();
    }
    std::fs::write(
        directory.join("consumer-job-keep"),
        "{ \"nextSnapshot\": 2 }\n",
    )
    .unwrap();
    std::fs::write(directory.join("notes"), "keep").unwrap();
    std::fs::write(directory.join("consumer-pre-job-old"), "keep").unwrap();
    std::fs::create_dir(directory.join("consumer-job-directory")).unwrap();
    std::fs::write(directory.join("consumer-job-directory/checkpoint"), "keep").unwrap();
    let mut expected = table_files(&root);
    expected
        .remove(&directory.join("consumer-job-old"))
        .unwrap();
    expected
        .remove(&directory.join("consumer-job-keep-extra"))
        .unwrap();

    for _ in 0..2 {
        assert_ok(&context, "CALL paimon.sys.clear_consumers(table => 'paimon.test_db.events', including_consumers => 'job-.*', excluding_consumers => 'job-keep')").await;
        assert_eq!(table_files(&root), expected);
        assert_eq!(manager.get("job-keep").await.unwrap(), Some(2));
        assert_eq!(
            branch.consumer_manager().get("job-old").await.unwrap(),
            Some(3)
        );
    }
    assert_sql_error(
        &context,
        "CALL sys.clear_consumers(table => 'test_db.events$branch_dev')",
        "does not exist",
    )
    .await;
    assert_eq!(table_files(&root), expected);
}

#[tokio::test]
async fn clear_accepts_defaults_and_does_not_create_a_missing_directory() {
    let (tmp, catalog) = create_test_env();
    let context = create_sql_context(catalog.clone()).await;
    exec(&context, "CREATE SCHEMA paimon.test_db").await;
    exec(&context, "CREATE TABLE paimon.test_db.events (id INT)").await;
    let table = catalog
        .get_table(&Identifier::new("test_db", "events"))
        .await
        .unwrap();
    let root = tmp.path().join("test_db.db/events");
    let original = table_files(&root);
    for _ in 0..2 {
        assert_ok(
            &context,
            "CALL sys.clear_consumers(table => 'test_db.events')",
        )
        .await;
        assert_eq!(table_files(&root), original);
        assert!(!root.join("consumer").exists());
    }
    for args in [
        "",
        ", including_consumers => ''",
        ", including_consumers => ' \t', excluding_consumers => ''",
    ] {
        table.consumer_manager().reset("job", 2).await.unwrap();
        assert_ok(
            &context,
            &format!("CALL sys.clear_consumers(table => 'test_db.events'{args})"),
        )
        .await;
        assert_eq!(table.consumer_manager().get("job").await.unwrap(), None);
        assert_eq!(table_files(&root), original);
    }
    table.consumer_manager().reset("job", 2).await.unwrap();
    assert_ok(
        &context,
        "CALL sys.clear_consumers(table => 'test_db.events', excluding_consumers => 'job')",
    )
    .await;
    assert_eq!(table.consumer_manager().get("job").await.unwrap(), Some(2));
}

#[tokio::test]
async fn clear_rejects_invalid_arguments_before_changing_files() {
    let (tmp, context, table) = table_with_snapshot().await;
    table.consumer_manager().reset("job", 1).await.unwrap();
    let root = tmp.path().join("test_db.db/events");
    let original = table_files(&root);
    for (args, message) in [
        ("", "Missing required argument: 'table'"),
        ("'test_db.events'", "require named arguments"),
        ("table => 'other.test_db.events'", "Catalog name mismatch"),
        ("table => 'test_db.events', including_consumer => 'job'", "is unknown"),
        ("table => 'test_db.events', excluding_consumer => 'job'", "is unknown"),
        ("table => 'test_db.events', including_consumers => 'job', INCLUDING_CONSUMERS => 'other'", "is duplicated"),
        ("table => 'test_db.events', excluding_consumers => 'job', excluding_consumers => 'other'", "is duplicated"),
        ("table => 'test_db.events', including_consumers => NULL", "Unsupported argument value"),
        ("table => 'test_db.events', excluding_consumers => NULL", "Unsupported argument value"),
        ("table => 'test_db.events', including_consumers => '['", "Invalid including_consumers regex"),
        ("table => 'test_db.events', excluding_consumers => '['", "Invalid excluding_consumers regex"),
        ("table => 'test_db.events', including_consumers => 'no-match', excluding_consumers => '(?=job)job'", "Invalid excluding_consumers regex"),
        (r"table => 'test_db.events', including_consumers => '(job)\1'", "Invalid including_consumers regex"),
    ] {
        assert_sql_error(&context, &format!("CALL sys.clear_consumers({args})"), message).await;
        assert_eq!(table_files(&root), original);
    }
}

#[tokio::test]
async fn clear_uses_whole_id_matching_with_rust_regex_flags() {
    let (_tmp, context, table) = table_with_snapshot().await;
    for (pattern, id, deleted) in [
        ("job", "job-old", false),
        ("a|ab", "ab", true),
        ("(?m)^job$", "job\nother", false),
        ("(?s)job.*", "job\nother", true),
        ("(?i)job", "JOB", true),
        ("(?x) job # trailing comment", "job", true),
        (" job", "job", false),
    ] {
        table.consumer_manager().reset(id, 2).await.unwrap();
        assert_ok(&context, &format!("CALL sys.clear_consumers(table => 'test_db.events', including_consumers => '{pattern}')")).await;
        assert_eq!(
            table.consumer_manager().get(id).await.unwrap(),
            if deleted { None } else { Some(2) },
            "{pattern:?}: {id:?}"
        );
        table.consumer_manager().delete(id).await.unwrap();
    }
}

#[tokio::test]
async fn clear_rejects_format_tables_without_changing_files() {
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
    catalog
        .get_table(&identifier)
        .await
        .unwrap()
        .consumer_manager()
        .reset("job", 2)
        .await
        .unwrap();
    let context = create_sql_context(catalog).await;
    let root = tmp.path().join("test_db.db/events");
    let original = table_files(&root);
    assert_sql_error(
        &context,
        "CALL sys.clear_consumers(table => 'test_db.events')",
        "requires a FileStoreTable",
    )
    .await;
    assert_eq!(table_files(&root), original);
}

#[tokio::test]
async fn clear_rejects_engine_served_table_types_without_changing_files() {
    for table_type in ["object-table", "iceberg-table", "lance-table"] {
        let (tmp, catalog) = create_test_env();
        catalog
            .create_database("test_db", false, Default::default())
            .await
            .unwrap();
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .option("type", table_type)
            .build()
            .unwrap();
        catalog
            .create_table(&Identifier::new("test_db", "events"), schema, false)
            .await
            .unwrap();
        let root = tmp.path().join("test_db.db/events");
        std::fs::create_dir(root.join("consumer")).unwrap();
        std::fs::write(root.join("consumer/consumer-job"), "{").unwrap();
        let original = table_files(&root);
        let context = create_sql_context(catalog).await;
        assert_sql_error(
            &context,
            "CALL sys.clear_consumers(table => 'test_db.events')",
            "cannot be read as a Paimon table",
        )
        .await;
        assert_eq!(table_files(&root), original);
    }
}

#[tokio::test]
async fn clear_preserves_running_scan_progress_and_checkpoint_protocol() {
    let (_tmp, context, table) = table_with_snapshot().await;
    table.consumer_manager().reset("keep", 2).await.unwrap();
    let mut running = table.new_stream_scan().unwrap();
    running
        .with_consumer_id("job")
        .unwrap()
        .restore(Some(1))
        .unwrap();
    running.plan().await.unwrap().unwrap();
    running
        .notify_checkpoint_complete(running.checkpoint())
        .await
        .unwrap();
    assert_eq!(table.consumer_manager().get("job").await.unwrap(), Some(2));

    assert_ok(
        &context,
        "CALL sys.clear_consumers(table => 'test_db.events', excluding_consumers => 'keep')",
    )
    .await;
    assert_eq!(table.consumer_manager().get("job").await.unwrap(), None);
    assert_eq!(table.consumer_manager().get("keep").await.unwrap(), Some(2));
    assert_eq!(running.checkpoint(), Some(2));
    assert!(running.plan().await.unwrap().is_none());
    let mut fresh = table.new_stream_scan().unwrap();
    fresh.with_consumer_id("job").unwrap();
    let plan = fresh.plan().await.unwrap().unwrap();
    assert_eq!(plan.snapshot_id(), Some(1));
    assert!(plan.splits().iter().all(|split| !split.is_streaming()));

    running
        .notify_checkpoint_complete(running.checkpoint())
        .await
        .unwrap();
    assert_eq!(table.consumer_manager().get("job").await.unwrap(), Some(2));
    assert_eq!(table.consumer_manager().get("keep").await.unwrap(), Some(2));
}
