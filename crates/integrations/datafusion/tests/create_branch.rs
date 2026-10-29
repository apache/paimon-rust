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
#[path = "create_branch/support.rs"]
mod support;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use common::{
    assert_sql_error, collect_id_name, create_sql_context, create_test_env, exec, row_count,
    string_value,
};
use paimon::catalog::Identifier;
use paimon::io::{FileIOBuilder, FileIOProvider};
use paimon::spec::{DataType, IntType, Schema};
use paimon::table::{BranchManager, Table};
use paimon::{Catalog, FileSystemCatalog};
use paimon_datafusion::SQLContext;

async fn setup() -> (tempfile::TempDir, Arc<FileSystemCatalog>, SQLContext, Table) {
    let (tmp, catalog) = create_test_env();
    let context = create_sql_context(catalog.clone()).await;
    exec(&context, "CREATE SCHEMA paimon.test_db").await;
    exec(
        &context,
        "CREATE TABLE paimon.test_db.events (id INT, name STRING)",
    )
    .await;
    exec(
        &context,
        "INSERT INTO paimon.test_db.events VALUES (1, 'alice')",
    )
    .await;
    exec(
        &context,
        "CALL sys.create_tag(table => 'test_db.events', tag => 'release', time_retained => '1d')",
    )
    .await;
    exec(
        &context,
        "ALTER TABLE paimon.test_db.events ADD COLUMN extra INT",
    )
    .await;
    exec(
        &context,
        "INSERT INTO paimon.test_db.events VALUES (2, 'bob', 20)",
    )
    .await;
    let table = catalog
        .get_table(&Identifier::new("test_db", "events"))
        .await
        .unwrap();
    (tmp, catalog, context, table)
}

async fn assert_ok(context: &SQLContext, args: &str) {
    let batches = context
        .sql(&format!("CALL paimon.sys.create_branch({args})"))
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

fn main_files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    table_files(root)
        .into_iter()
        .filter(|(path, _)| !path.strip_prefix(root).unwrap().starts_with("branch"))
        .collect()
}

#[tokio::test]
async fn empty_branch_uses_latest_schema_and_does_not_inherit_data_or_tags() {
    let (tmp, _, context, table) = setup().await;
    let root = tmp.path().join("test_db.db/events");
    let before = main_files(&root);
    assert_ok(
        &context,
        "table => 'paimon.test_db.events', branch => 'empty'",
    )
    .await;
    let branch = table.copy_with_branch("empty").await.unwrap();
    assert_eq!(branch.schema().id(), table.schema().id());
    assert_eq!(branch.schema().fields(), table.schema().fields());
    assert!(branch
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .is_none());
    assert!(!root.join("branch/branch-empty/snapshot").exists());
    assert!(!root.join("branch/branch-empty/tag").exists());
    assert_eq!(
        row_count(
            &context,
            "SELECT * FROM paimon.test_db.`events$branch_empty`"
        )
        .await,
        0
    );
    assert_eq!(
        row_count(
            &context,
            "SELECT * FROM paimon.test_db.`events$branches` WHERE branch_name = 'empty'"
        )
        .await,
        1
    );
    assert_eq!(main_files(&root), before);
    assert_eq!(
        collect_id_name(&context, "SELECT id, name FROM paimon.test_db.events").await,
        vec![(1, "alice".into()), (2, "bob".into())]
    );
}

#[tokio::test]
async fn empty_branch_can_be_created_before_the_first_commit() {
    let (tmp, catalog) = create_test_env();
    let context = create_sql_context(catalog).await;
    exec(&context, "CREATE SCHEMA paimon.test_db").await;
    exec(&context, "CREATE TABLE paimon.test_db.fresh (id INT)").await;
    let root = tmp.path().join("test_db.db/fresh");
    let before = main_files(&root);
    assert_ok(
        &context,
        "table => 'test_db.fresh', branch => 'empty', ignore_if_exists => true",
    )
    .await;
    assert_eq!(
        row_count(
            &context,
            "SELECT * FROM paimon.test_db.`fresh$branch_empty`"
        )
        .await,
        0
    );
    assert_eq!(
        row_count(
            &context,
            "SELECT * FROM paimon.test_db.`fresh$branches` WHERE branch_name = 'empty'"
        )
        .await,
        1
    );
    assert!(root.join("branch/branch-empty/schema/schema-0").is_file());
    assert_eq!(main_files(&root), before);
}

#[tokio::test]
async fn tag_branch_reads_historical_schema_and_rows_with_live_or_expired_snapshot() {
    let (tmp, _, context, table) = setup().await;
    let root = tmp.path().join("test_db.db/events");
    let tag = table.tag_manager().get("release").await.unwrap().unwrap();
    assert!(tag.schema_id() < table.schema().id());
    for (name, expire) in [("live", false), ("expired", true)] {
        if expire {
            table
                .file_io()
                .delete_file(&table.snapshot_manager().snapshot_path(tag.id()))
                .await
                .unwrap();
            assert!(!table
                .file_io()
                .exists(&table.snapshot_manager().snapshot_path(tag.id()))
                .await
                .unwrap());
        }
        let before = main_files(&root);
        assert_ok(&context, &format!("table => 'test_db.events', branch => '{name}', tag => 'release', ignore_if_exists => false")).await;
        let branch = table.copy_with_branch(name).await.unwrap();
        assert_eq!(branch.schema().id(), tag.schema_id());
        assert_eq!(branch.schema().fields().len(), 2);
        assert_eq!(
            collect_id_name(
                &context,
                &format!("SELECT id, name FROM paimon.test_db.`events$branch_{name}`")
            )
            .await,
            vec![(1, "alice".into())]
        );
        assert_eq!(
            row_count(
                &context,
                &format!(
                    "SELECT * FROM paimon.test_db.`events$branches` WHERE branch_name = '{name}'"
                )
            )
            .await,
            1
        );
        assert_eq!(
            std::fs::read(root.join(format!("branch/branch-{name}/tag/tag-release"))).unwrap(),
            before[&root.join("tag/tag-release")]
        );
        assert_eq!(main_files(&root), before);
        assert_eq!(
            collect_id_name(&context, "SELECT id, name FROM paimon.test_db.events").await,
            vec![(1, "alice".into()), (2, "bob".into())]
        );
        assert_eq!(
            collect_id_name(
                &context,
                "SELECT id, name FROM paimon.test_db.events VERSION AS OF 'release'"
            )
            .await,
            vec![(1, "alice".into())]
        );
    }
}

#[tokio::test]
async fn ignore_existing_preserves_empty_and_tagged_branches_without_resolving_tag() {
    let (tmp, _, context, _) = setup().await;
    for (branch, tag) in [("empty", ""), ("tagged", ", tag => 'release'")] {
        assert_ok(
            &context,
            &format!(
                "table => 'test_db.events', branch => '{branch}'{tag}, ignore_if_exists => 'TrUe'"
            ),
        )
        .await;
    }
    let root = tmp.path().join("test_db.db/events");
    let before = table_files(&root);
    for branch in ["empty", "tagged"] {
        for tag in ["", ", tag => 'release'", ", tag => 'missing'"] {
            for ignore in ["", ", ignore_if_exists => false"] {
                assert_sql_error(&context, &format!("CALL sys.create_branch(table => 'test_db.events', branch => '{branch}'{tag}{ignore})"),
                    "already exists").await;
                assert_eq!(table_files(&root), before);
            }
            assert_ok(&context, &format!("table => 'test_db.events', branch => '{branch}'{tag}, ignore_if_exists => true")).await;
            assert_eq!(table_files(&root), before);
        }
    }
    assert_eq!(
        row_count(
            &context,
            "SELECT * FROM paimon.test_db.`events$branch_empty`"
        )
        .await,
        0
    );
    assert_eq!(
        collect_id_name(
            &context,
            "SELECT id, name FROM paimon.test_db.`events$branch_tagged`"
        )
        .await,
        vec![(1, "alice".into())]
    );
}

#[tokio::test]
async fn invalid_names_are_rejected_before_ignore_existing_checks() {
    let (tmp, _, context, _) = setup().await;
    assert_ok(&context, "table => 'test_db.events', branch => 'keep'").await;
    let root = tmp.path().join("test_db.db/events");
    for name in ["main", "123"] {
        let path = root.join(format!("branch/branch-{name}"));
        std::fs::create_dir(&path).unwrap();
        std::fs::write(path.join("marker"), "keep").unwrap();
    }
    let before = table_files(&root);
    for (name, message) in [
        ("main", "default branch"),
        ("", "empty"),
        (" ", "whitespace"),
        ("123", "numeric"),
        (".", "cannot be"),
        ("..", "cannot be"),
        ("keep/schema", "path separators"),
        ("a\\b", "path separators"),
        ("a\n", "control characters"),
    ] {
        for ignore in [false, true] {
            for tag in ["", ", tag => 'release'"] {
                assert_sql_error(&context, &format!("CALL sys.create_branch(table => 'test_db.events', branch => '{name}'{tag}, ignore_if_exists => {ignore})"), message).await;
                assert_eq!(table_files(&root), before);
            }
        }
    }
}

#[tokio::test]
async fn sql_delimiter_in_branch_names_is_rejected_before_writing_or_ignoring() {
    let (tmp, _, context, table) = setup().await;
    let root = tmp.path().join("test_db.db/events");
    BranchManager::new(table.file_io().clone(), table.location().to_string())
        .create_branch_from_tag("legacy$blue", "release")
        .await
        .unwrap();
    let before = table_files(&root);

    for branch in ["dev$blue", "$dev", "dev$", "legacy$blue"] {
        for tag in [", tag => 'release'", ""] {
            for ignore in [
                "",
                ", ignore_if_exists => false",
                ", ignore_if_exists => true",
            ] {
                assert_sql_error(
                    &context,
                    &format!(
                        "CALL sys.create_branch(table => 'test_db.events', branch => '{branch}'{tag}{ignore})"
                    ),
                    "SQL suffix delimiter '$'",
                )
                .await;
                assert_eq!(table_files(&root), before);
                if branch != "legacy$blue" {
                    assert!(!root.join(format!("branch/branch-{branch}")).exists());
                }
            }
        }
    }
}

#[tokio::test]
async fn branch_names_preserve_spaces_and_tag_names_follow_core_validation() {
    let (tmp, _, context, table) = setup().await;
    let root = tmp.path().join("test_db.db/events");
    let before = main_files(&root);
    assert_ok(
        &context,
        "table => 'test_db.events', branch => 'dev', tag => 'release'",
    )
    .await;
    for name in [" dev", "dev "] {
        assert_ok(
            &context,
            &format!("table => 'test_db.events', branch => '{name}', ignore_if_exists => true"),
        )
        .await;
        assert_eq!(
            row_count(
                &context,
                &format!("SELECT * FROM paimon.test_db.`events$branch_{name}`")
            )
            .await,
            0
        );
    }
    assert_eq!(
        row_count(&context, "SELECT * FROM paimon.test_db.`events$branches`").await,
        3
    );
    assert_eq!(
        collect_id_name(
            &context,
            "SELECT id, name FROM paimon.test_db.`events$branch_dev`"
        )
        .await,
        vec![(1, "alice".into())]
    );
    assert_eq!(main_files(&root), before);
    exec(
        &context,
        "CALL sys.create_tag(table => 'test_db.events', tag => ' release')",
    )
    .await;
    let before = main_files(&root);
    assert_ok(
        &context,
        "table => 'test_db.events', branch => 'spaced_tag', tag => ' release'",
    )
    .await;
    assert_eq!(
        table
            .copy_with_branch("spaced_tag")
            .await
            .unwrap()
            .schema()
            .id(),
        table.schema().id()
    );
    assert_eq!(
        collect_id_name(
            &context,
            "SELECT id, name FROM paimon.test_db.`events$branch_spaced_tag`"
        )
        .await,
        vec![(1, "alice".into()), (2, "bob".into())]
    );
    assert_eq!(main_files(&root), before);
}

#[tokio::test]
async fn invalid_arguments_and_missing_tag_leave_metadata_unchanged() {
    let (tmp, _, context, _) = setup().await;
    assert_ok(&context, "table => 'test_db.events', branch => 'existing'").await;
    let root = tmp.path().join("test_db.db/events");
    let before = table_files(&root);
    for (arguments, message) in [
        ("branch => 'new'", "Missing required argument: 'table'"),
        ("table => 'test_db.events'", "Missing required argument: 'branch'"),
        ("table => 'test_db.events', branch => 'new', tag => 'missing'", "Tag 'missing' does not exist"),
        ("table => 'test_db.events', branch => 'new', tag => ''", "tag name"),
        ("table => 'test_db.events', branch => 'new', tag => 'release '", "Invalid tag name"),
        ("table => 'test_db.events', branch => 'new', ignore_if_exists => true, tag => 'missing'", "Tag 'missing' does not exist"),
        ("table => 'test_db.events', branch => 'existing', ignore_if_exists => 'yes'", "Invalid ignore_if_exists"),
        ("table => 'test_db.events', branch => 'existing', ignore_if_exists => 1", "Invalid ignore_if_exists"),
        ("table => 'test_db.events', branch => 'new', ignore_if_exists => NULL", "Unsupported argument value"),
        ("table => 'test_db.events', branch => 'existing', ignoreIfExists => true", "Argument ignoreifexists is unknown"),
        ("table => 'test_db.events', branch => 'existing', tags => 'release', ignore_if_exists => true", "Argument tags is unknown"),
        ("table => 'test_db.events', branch => 'new', branch => 'other'", "argument branch is duplicated"),
        ("'test_db.events', 'new'", "require named arguments"),
        ("table => 'paimon.other.events', branch => 'new'", "does not exist"),
        ("table => 'other.test_db.events', branch => 'new'", "Catalog name mismatch"),
    ] {
        assert_sql_error(&context, &format!("CALL sys.create_branch({arguments})"), message).await;
        assert_eq!(table_files(&root), before);
    }
    assert!(!root.join("branch/branch-new").exists());
}

#[tokio::test]
async fn unsupported_table_types_are_rejected_even_when_target_exists() {
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
        std::fs::create_dir_all(root.join("branch/branch-existing")).unwrap();
        std::fs::write(root.join("branch/branch-existing/marker"), "keep").unwrap();
        let before = table_files(&root);
        let context = create_sql_context(catalog).await;
        let message = if table_type == "format-table" {
            "requires a FileStoreTable"
        } else {
            "cannot be read as a Paimon table"
        };
        for branch in ["new", "existing"] {
            for tag in ["", ", tag => 'missing'"] {
                assert_sql_error(&context, &format!("CALL sys.create_branch(table => 'test_db.events', branch => '{branch}'{tag}, ignore_if_exists => true)"), message).await;
                assert_eq!(table_files(&root), before);
            }
        }
        assert!(!root.join("branch/branch-new").exists());
    }
}

#[tokio::test]
async fn branch_suffixes_are_not_resolved_and_non_main_tables_are_rejected() {
    let (tmp, catalog, context, table) = setup().await;
    assert_ok(
        &context,
        "table => 'test_db.events', branch => 'dev', tag => 'release'",
    )
    .await;
    let root = tmp.path().join("test_db.db/events");
    let before = table_files(&root);
    for suffix in ["$branch_dev", "$branch_main", "$branches"] {
        assert_sql_error(&context, &format!("CALL sys.create_branch(table => 'test_db.events{suffix}', branch => 'new', ignore_if_exists => true)"),
            "does not exist").await;
        assert_eq!(table_files(&root), before);
    }
    let branch = table.copy_with_branch("dev").await.unwrap();
    let context = support::context_with_table(catalog, branch).await;
    for branch in ["new", "dev"] {
        for tag in ["", ", tag => 'release'"] {
            assert_sql_error(&context, &format!("CALL sys.create_branch(table => 'test_db.events', branch => '{branch}'{tag}, ignore_if_exists => true)"),
                "requires a main-branch source table").await;
            assert_eq!(table_files(&root), before);
        }
    }
    assert!(!root.join("branch/branch-new").exists());
}

#[derive(Debug, Default)]
struct FailingProvider {
    calls: AtomicUsize,
}

#[async_trait::async_trait]
impl FileIOProvider for FailingProvider {
    async fn create(&self, _path: &str) -> paimon::Result<(opendal::Operator, String)> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(opendal::Error::new(
            opendal::ErrorKind::PermissionDenied,
            "injected branch storage failure",
        )
        .into())
    }
}

#[tokio::test]
async fn ignore_existing_does_not_swallow_storage_errors() {
    let (tmp, catalog, _, table) = setup().await;
    let root = tmp.path().join("test_db.db/events");
    let before = table_files(&root);
    let provider = Arc::new(FailingProvider::default());
    let io = FileIOBuilder::new("file")
        .with_provider(provider.clone())
        .build()
        .unwrap();
    let table = Table::new(
        io,
        table.identifier().clone(),
        table.location().into(),
        table.schema().clone(),
        None,
    );
    let context = support::context_with_table(catalog, table).await;
    for ignore in [false, true] {
        assert_sql_error(&context, &format!("CALL sys.create_branch(table => 'test_db.events', branch => 'new', ignore_if_exists => {ignore})"),
            "injected branch storage failure").await;
        assert_eq!(table_files(&root), before);
    }
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
}
