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
#[path = "replace_tag/support.rs"]
mod support;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use common::{
    assert_sql_error, collect_id_name, create_sql_context, create_test_env, exec, string_value,
};
use paimon::catalog::Identifier;
use paimon::io::FileIOBuilder;
use paimon::spec::{DataType, IntType, Schema};
use paimon::table::{BranchManager, Table};
use paimon::Catalog;
use paimon_datafusion::SQLContext;

async fn setup() -> (tempfile::TempDir, SQLContext, Table) {
    let (tmp, catalog) = create_test_env();
    let context = create_sql_context(catalog.clone()).await;
    exec(&context, "CREATE SCHEMA paimon.test_db").await;
    exec(
        &context,
        "CREATE TABLE paimon.test_db.events (id INT, name STRING) \
         WITH ('tag.default-time-retained' = '7d')",
    )
    .await;
    for (id, name) in [(1, "alice"), (2, "bob")] {
        exec(
            &context,
            &format!("INSERT INTO paimon.test_db.events VALUES ({id}, '{name}')"),
        )
        .await;
    }
    let table = catalog
        .get_table(&Identifier::new("test_db", "events"))
        .await
        .unwrap();
    table
        .tag_manager()
        .create_with_retention(
            "release",
            &table.snapshot_manager().get_snapshot(1).await.unwrap(),
            Some("1d"),
        )
        .await
        .unwrap();
    (tmp, context, table)
}

async fn assert_ok(context: &SQLContext, args: &str) {
    let batches = context
        .sql(&format!("CALL sys.replace_tag({args})"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
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
async fn replace_tag_updates_snapshots_retention_and_time_travel() {
    let (tmp, context, table) = setup().await;
    let root = tmp.path().join("test_db.db/events");
    let tag_path = root.join("tag/tag-release");
    let mut original_files = table_files(&root);
    original_files.remove(&tag_path).unwrap();
    let first = vec![(1, "alice".to_string())];
    let latest = vec![(1, "alice".to_string()), (2, "bob".to_string())];
    let tagged_query = "SELECT id, name FROM paimon.test_db.events VERSION AS OF 'release'";
    assert_eq!(collect_id_name(&context, tagged_query).await, first);

    for (args, snapshot_id, retention, expected_rows) in [
        ("", 2, None, &latest),
        (
            ", snapshot_id => 1, time_retained => '2d'",
            1,
            Some("PT48H"),
            &first,
        ),
        (
            ", snapshot_id => 1, time_retained => '12h'",
            1,
            Some("PT12H"),
            &first,
        ),
        (", snapshot_id => 1", 1, None, &first),
    ] {
        assert_ok(
            &context,
            &format!("table => 'paimon.test_db.events', tag => 'release'{args}"),
        )
        .await;
        let batches = context.sql("SELECT snapshot_id, create_time, time_retained FROM paimon.test_db.`events$tags` WHERE tag_name = 'release'")
            .await.unwrap().collect().await.unwrap();
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            1
        );
        let row = &batches[0];
        let ids = row
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), snapshot_id);
        assert_eq!(row.column(1).is_null(0), retention.is_none());
        let ttl = row.column(2);
        assert_eq!(
            (!ttl.is_null(0)).then(|| string_value(ttl.as_ref(), 0)),
            retention
        );
        let value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&tag_path).unwrap()).unwrap();
        assert_eq!(value.get("tagCreateTime").is_some(), retention.is_some());
        assert_eq!(value.get("tagTimeRetained").is_some(), retention.is_some());
        assert_eq!(
            &collect_id_name(&context, tagged_query).await,
            expected_rows
        );
        assert_eq!(
            collect_id_name(&context, "SELECT id, name FROM paimon.test_db.events").await,
            latest
        );
        let mut actual_files = table_files(&root);
        actual_files.remove(&tag_path).unwrap();
        assert_eq!(actual_files, original_files);
    }
    assert_eq!(
        table
            .tag_manager()
            .get("release")
            .await
            .unwrap()
            .unwrap()
            .id(),
        1
    );
}

#[tokio::test]
async fn replace_tag_works_with_create_rename_and_delete() {
    let (tmp, context, _table) = setup().await;
    let root = tmp.path().join("test_db.db/events/tag");
    let original = std::fs::read(root.join("tag-release")).unwrap();
    exec(
        &context,
        "CALL sys.create_tag(table => 'test_db.events', tag => 'checkpoint', snapshot_id => 1)",
    )
    .await;
    assert_ok(
        &context,
        "table => 'test_db.events', tag => 'checkpoint', time_retained => '2d'",
    )
    .await;
    let replaced = std::fs::read(root.join("tag-checkpoint")).unwrap();
    exec(
        &context,
        "CALL sys.rename_tag(table => 'test_db.events', tag => 'checkpoint', target_tag => 'checkpoint_v2')",
    )
    .await;
    assert!(!root.join("tag-checkpoint").exists());
    assert_eq!(
        std::fs::read(root.join("tag-checkpoint_v2")).unwrap(),
        replaced
    );
    assert_eq!(
        collect_id_name(
            &context,
            "SELECT id, name FROM paimon.test_db.events VERSION AS OF 'checkpoint_v2'"
        )
        .await,
        vec![(1, "alice".to_string()), (2, "bob".to_string())]
    );
    exec(
        &context,
        "CALL sys.delete_tag(table => 'test_db.events', tag => 'checkpoint_v2')",
    )
    .await;
    assert!(!root.join("tag-checkpoint_v2").exists());
    assert_eq!(std::fs::read(root.join("tag-release")).unwrap(), original);
    assert_eq!(std::fs::read_dir(root).unwrap().count(), 1);
}

#[tokio::test]
async fn replace_tag_resolves_retained_snapshots_including_its_own() {
    let (tmp, context, table) = setup().await;
    let snapshot = table.snapshot_manager().get_snapshot(1).await.unwrap();
    table.tag_manager().create("keep", &snapshot).await.unwrap();
    assert_ok(&context, "table => 'test_db.events', tag => 'release'").await;
    std::fs::remove_file(tmp.path().join("test_db.db/events/snapshot/snapshot-1")).unwrap();
    assert_ok(
        &context,
        "table => 'test_db.events', tag => 'release', snapshot_id => 1",
    )
    .await;
    table.tag_manager().delete("keep").await.unwrap();
    assert_ok(
        &context,
        "table => 'test_db.events', tag => 'release', snapshot_id => 1, time_retained => '2d'",
    )
    .await;
    assert_eq!(
        collect_id_name(
            &context,
            "SELECT id, name FROM paimon.test_db.events VERSION AS OF 'release'"
        )
        .await,
        vec![(1, "alice".to_string())]
    );
}

#[tokio::test]
async fn replace_tag_rejects_invalid_arguments_without_writes() {
    let (tmp, context, _table) = setup().await;
    let root = tmp.path().join("test_db.db/events");
    let original = table_files(&root);
    for (args, message) in [
        ("tag => 'release'", "Missing required argument: 'table'"),
        (
            "table => 'test_db.events'",
            "Missing required argument: 'tag'",
        ),
        (
            "table => 'test_db.events', tag => 'missing'",
            "Tag missing does not exist",
        ),
        ("table => 'test_db.events', tag => ''", "Invalid tag name"),
        (
            "table => 'test_db.events', tag => '../release'",
            "Invalid tag name",
        ),
        (
            "table => 'test_db.events', tag => 'release '",
            "Invalid tag name",
        ),
        (
            "table => 'test_db.events', tag => 'release', snapshot_id => 'abc'",
            "Invalid snapshot_id",
        ),
        (
            "table => 'test_db.events', tag => 'release', snapshot_id => 1.5",
            "Invalid snapshot_id",
        ),
        (
            "table => 'test_db.events', tag => 'release', snapshot_id => 0",
            "Invalid snapshot_id",
        ),
        (
            "table => 'test_db.events', tag => 'release', snapshot_id => -1",
            "Invalid snapshot_id",
        ),
        (
            "table => 'test_db.events', tag => 'release', snapshot_id => 9223372036854775808",
            "Invalid snapshot_id",
        ),
        (
            "table => 'test_db.events', tag => 'release', snapshot_id => -9223372036854775809",
            "Invalid snapshot_id",
        ),
        (
            "table => 'test_db.events', tag => 'release', snapshot_id => 42",
            "Snapshot 42 does not exist",
        ),
        (
            "table => 'test_db.events', tag => 'release', snapshot => 1",
            "Argument snapshot is unknown",
        ),
        (
            "table => 'test_db.events', tag => 'release', time_retain => '1d'",
            "Argument time_retain is unknown",
        ),
        (
            "table => 'test_db.events', tag => 'release', snapshot_id => 1, snapshot_id => 2",
            "Procedure argument snapshot_id is duplicated",
        ),
        (
            "table => 'test_db.events', tag => 'release', tag => 'other'",
            "Procedure argument tag is duplicated",
        ),
        (
            "table => 'test_db.events', tag => 'release', time_retained => ''",
            "Invalid tag retention",
        ),
        (
            "table => 'test_db.events', tag => 'release', time_retained => '-1d'",
            "Invalid tag retention",
        ),
        (
            "table => 'test_db.events', tag => 'release', time_retained => 'invalid'",
            "Invalid tag retention",
        ),
        (
            "table => 'test_db.events', tag => 'release', time_retained => '106751991167301d'",
            "Invalid tag retention",
        ),
    ] {
        assert_sql_error(&context, &format!("CALL sys.replace_tag({args})"), message).await;
        assert_eq!(table_files(&root), original, "{args}");
    }
}

#[tokio::test]
async fn replace_tag_requires_a_live_latest_snapshot_when_id_is_omitted() {
    let (tmp, context, _table) = setup().await;
    let root = tmp.path().join("test_db.db/events");
    for id in [1, 2] {
        std::fs::remove_file(root.join(format!("snapshot/snapshot-{id}"))).unwrap();
    }
    for (remove_hint, message) in [
        (false, "Snapshot 2 does not exist"),
        (true, "No snapshots exist"),
    ] {
        if remove_hint {
            std::fs::remove_file(root.join("snapshot/LATEST")).unwrap();
        }
        let original = table_files(&root);
        assert_sql_error(
            &context,
            "CALL sys.replace_tag(table => 'test_db.events', tag => 'release')",
            message,
        )
        .await;
        assert_eq!(table_files(&root), original);
    }
    assert_ok(
        &context,
        "table => 'test_db.events', tag => 'release', snapshot_id => 1",
    )
    .await;
}

#[tokio::test]
async fn replace_tag_propagates_invalid_live_snapshot_instead_of_using_a_tag() {
    let (tmp, context, table) = setup().await;
    let root = tmp.path().join("test_db.db/events");
    let snapshot = table.snapshot_manager().get_snapshot(1).await.unwrap();
    for (content, message) in [
        (b"{".to_vec(), "snapshot JSON invalid"),
        (
            serde_json::to_vec(&table.snapshot_manager().get_snapshot(2).await.unwrap()).unwrap(),
            "snapshot file id mismatch",
        ),
    ] {
        std::fs::write(root.join("snapshot/snapshot-1"), content).unwrap();
        let original = table_files(&root);
        assert_sql_error(
            &context,
            "CALL sys.replace_tag(table => 'test_db.events', tag => 'release', snapshot_id => 1)",
            message,
        )
        .await;
        assert_eq!(table_files(&root), original);
        assert_eq!(
            table.tag_manager().get("release").await.unwrap().unwrap(),
            snapshot
        );
    }
}

#[tokio::test]
async fn replace_tag_preserves_other_branches_and_existing_name_resolution() {
    let (tmp, context, table) = setup().await;
    BranchManager::new(table.file_io().clone(), table.location().to_string())
        .create_branch_from_tag("dev", "release")
        .await
        .unwrap();
    let branch = table.copy_with_branch("dev").await.unwrap();
    branch
        .tag_manager()
        .create(
            "release",
            &table.snapshot_manager().get_snapshot(1).await.unwrap(),
        )
        .await
        .unwrap();
    let root = tmp.path().join("test_db.db/events");
    let original_branch = table_files(&root.join("branch"));
    assert_ok(&context, "table => 'test_db.events', tag => 'release'").await;
    assert_eq!(table_files(&root.join("branch")), original_branch);
    let original = table_files(&root);
    assert_sql_error(
        &context,
        "CALL sys.replace_tag(table => 'test_db.events$branch_dev', tag => 'release')",
        "does not exist",
    )
    .await;
    assert_eq!(table_files(&root), original);
}

#[tokio::test]
async fn replace_tag_uses_the_resolved_branch_for_snapshots_and_tags() {
    let (tmp, _context, table) = setup().await;
    BranchManager::new(table.file_io().clone(), table.location().to_string())
        .create_branch_from_tag("dev", "release")
        .await
        .unwrap();
    let branch = table.copy_with_branch("dev").await.unwrap();
    assert_eq!(branch.branch(), "dev");

    // Distinct metadata for the same ID also catches lookups in the wrong namespace.
    let mut value =
        serde_json::to_value(table.snapshot_manager().get_snapshot(1).await.unwrap()).unwrap();
    value["commitUser"] = serde_json::json!("dev-live");
    let live: paimon::spec::Snapshot = serde_json::from_value(value.clone()).unwrap();
    branch
        .file_io()
        .new_output(&branch.snapshot_manager().snapshot_path(1))
        .unwrap()
        .write(serde_json::to_vec(&live).unwrap().into())
        .await
        .unwrap();
    branch
        .tag_manager()
        .create_with_retention("release", &live, Some("1d"))
        .await
        .unwrap();
    value["id"] = serde_json::json!(3);
    value["commitUser"] = serde_json::json!("dev-retained");
    let retained: paimon::spec::Snapshot = serde_json::from_value(value.clone()).unwrap();
    branch
        .tag_manager()
        .create("keep", &retained)
        .await
        .unwrap();
    value["commitUser"] = serde_json::json!("main-retained");
    table
        .tag_manager()
        .create("keep", &serde_json::from_value(value.clone()).unwrap())
        .await
        .unwrap();
    value["id"] = serde_json::json!(4);
    table
        .tag_manager()
        .create("main_only", &serde_json::from_value(value).unwrap())
        .await
        .unwrap();

    let root = tmp.path().join("test_db.db/events");
    let branch_root = root.join("branch/branch-dev");
    let mut original_main = table_files(&root);
    original_main.retain(|path, _| !path.starts_with(&branch_root));
    let context = support::context_with_table(tmp.path(), branch.clone()).await;

    for (args, expected) in [
        ("", &live),
        (", snapshot_id => 1", &live),
        (", snapshot_id => 3", &retained),
    ] {
        assert_ok(
            &context,
            &format!("table => 'test_db.events', tag => 'release'{args}"),
        )
        .await;
        assert_eq!(
            branch.tag_manager().get("release").await.unwrap().as_ref(),
            Some(expected)
        );
        let mut current_main = table_files(&root);
        current_main.retain(|path, _| !path.starts_with(&branch_root));
        assert_eq!(current_main, original_main);
    }

    // Neither a main-only live snapshot nor a main-only retained snapshot may resolve.
    for id in [2, 4] {
        let original = table_files(&root);
        assert_sql_error(
            &context,
            &format!("CALL sys.replace_tag(table => 'test_db.events', tag => 'release', snapshot_id => {id})"),
            &format!("Snapshot {id} does not exist"),
        ).await;
        assert_eq!(table_files(&root), original);
    }
}

#[tokio::test]
async fn replace_tag_propagates_writer_creation_failure_through_core_and_sql() {
    let (tmp, _context, table) = setup().await;
    let snapshot = table.snapshot_manager().get_snapshot(1).await.unwrap();
    let operator = opendal::Operator::new(opendal::services::Memory::default()).unwrap();
    let file_io = FileIOBuilder::new("fs")
        .with_fs_operator(operator.clone())
        .build()
        .unwrap();
    let clean = Table::from_resolved_schema(
        file_io,
        table.identifier().clone(),
        "file:/write_failure".to_string(),
        table.schema().clone(),
        "main",
    )
    .unwrap();
    clean
        .snapshot_manager()
        .commit_snapshot(&snapshot)
        .await
        .unwrap();
    clean
        .tag_manager()
        .create("release", &snapshot)
        .await
        .unwrap();

    let (operator, attempts) = support::fail_tag_writes(operator);
    let file_io = FileIOBuilder::new("fs")
        .with_fs_operator(operator)
        .build()
        .unwrap();
    let failing = Table::from_resolved_schema(
        file_io,
        clean.identifier().clone(),
        clean.location().to_string(),
        clean.schema().clone(),
        "main",
    )
    .unwrap();
    assert_eq!(
        failing.tag_manager().get("release").await.unwrap(),
        Some(snapshot.clone())
    );
    assert_eq!(
        failing.snapshot_manager().get_snapshot(1).await.unwrap(),
        snapshot
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 0);

    let error = failing
        .tag_manager()
        .replace("release", &snapshot, Some("2d"))
        .await
        .unwrap_err();
    assert!(
        matches!(&error, paimon::Error::IoUnexpected { source, .. } if source.kind() == opendal::ErrorKind::Unexpected)
    );
    assert!(
        error.to_string().contains(support::WRITE_FAILURE),
        "{error}"
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    let context = support::context_with_table(tmp.path(), failing).await;
    assert_sql_error(
        &context,
        "CALL sys.replace_tag(table => 'test_db.events', tag => 'release', snapshot_id => 1, time_retained => '2d')",
        support::WRITE_FAILURE,
    ).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn replace_tag_rejects_unsupported_table_types_before_reading_metadata() {
    for table_type in [
        "format-table",
        "object-table",
        "iceberg-table",
        "lance-table",
    ] {
        let (tmp, catalog) = create_test_env();
        catalog
            .create_database("test_db", false, Default::default())
            .await
            .unwrap();
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .option("type", table_type)
            .option("file.format", "parquet")
            .build()
            .unwrap();
        catalog
            .create_table(&Identifier::new("test_db", "events"), schema, false)
            .await
            .unwrap();
        let root = tmp.path().join("test_db.db/events");
        std::fs::create_dir(root.join("tag")).unwrap();
        std::fs::write(root.join("tag/tag-release"), "{").unwrap();
        let original = table_files(&root);
        let context = create_sql_context(catalog).await;
        let message = if table_type == "format-table" {
            "requires a FileStoreTable"
        } else {
            "cannot be read as a Paimon table"
        };
        assert_sql_error(
            &context,
            "CALL sys.replace_tag(table => 'test_db.events', tag => 'release', snapshot_id => 1)",
            message,
        )
        .await;
        assert_eq!(table_files(&root), original);
    }
}
