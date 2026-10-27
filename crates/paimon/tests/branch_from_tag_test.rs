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

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow_array::Int32Array;
use common::incremental_helpers::{make_batch, persist_table_schema, write_batch};
use futures::TryStreamExt;
use paimon::catalog::Identifier;
use paimon::io::{FileIOBuilder, FileIOProvider};
use paimon::spec::{DataType, IntType, Schema, TableSchema};
use paimon::table::{BranchManager, Table};

const TABLE_PATH: &str = "file:/table";
const SNAPSHOT_PATH: &str = "file:/table/snapshot/snapshot-1";
const BRANCH_SNAPSHOT_PATH: &str = "file:/table/branch/branch-dev/snapshot/snapshot-1";
const BRANCH_TAG_PATH: &str = "file:/table/branch/branch-dev/tag/tag-base";

async fn tagged_table() -> (tempfile::TempDir, Table, opendal::Operator) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = opendal_service_fs::FsConfig::default();
    config.root = Some(dir.path().to_str().unwrap().to_string());
    let operator = opendal::Operator::from_config(config).unwrap();
    let io = FileIOBuilder::new("file")
        .with_fs_operator(operator.clone())
        .build()
        .unwrap();
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .build()
        .unwrap();
    let table = Table::new(
        io.clone(),
        Identifier::new("default", "tagged"),
        TABLE_PATH.into(),
        TableSchema::new(0, &schema),
        None,
    );
    persist_table_schema(&io, TABLE_PATH, table.schema()).await;
    write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
    let snapshot = table.snapshot_manager().get_snapshot(1).await.unwrap();
    table.tag_manager().create("base", &snapshot).await.unwrap();
    // Distinguish copying the live JSON from serializing the tag's Snapshot.
    io.new_output(SNAPSHOT_PATH)
        .unwrap()
        .write(serde_json::to_vec_pretty(&snapshot).unwrap().into())
        .await
        .unwrap();
    write_batch(&table, &make_batch(vec![3], vec![30])).await;
    (dir, table, operator)
}

async fn rows(table: &Table) -> Vec<(i32, i32)> {
    let builder = table.new_read_builder();
    let plan = builder.new_scan().plan().await.unwrap();
    let batches: Vec<arrow_array::RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        rows.extend((0..batch.num_rows()).map(|i| (ids.value(i), values.value(i))));
    }
    rows.sort_unstable();
    rows
}

fn source_files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut files = BTreeMap::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            let relative = path.strip_prefix(root).unwrap();
            if relative.starts_with("branch") {
                continue;
            }
            if path.is_dir() {
                pending.push(path);
            } else {
                files.insert(relative.to_path_buf(), std::fs::read(path).unwrap());
            }
        }
    }
    files
}

#[tokio::test]
async fn live_snapshot_is_copied_verbatim_and_branch_reads_tagged_rows() {
    let (dir, table, _) = tagged_table().await;
    let root = dir.path().join("table");
    let before = source_files(&root);
    BranchManager::new(table.file_io().clone(), TABLE_PATH.into())
        .create_branch_from_tag("dev", "base")
        .await
        .unwrap();
    for path in ["snapshot/snapshot-1", "tag/tag-base", "schema/schema-0"] {
        assert_eq!(
            std::fs::read(root.join("branch/branch-dev").join(path)).unwrap(),
            before[Path::new(path)]
        );
    }
    let branch = table.copy_with_branch("dev").await.unwrap();
    assert_eq!(rows(&branch).await, vec![(1, 10), (2, 20)]);
    assert_eq!(rows(&table).await, vec![(1, 10), (2, 20), (3, 30)]);
    assert_eq!(source_files(&root), before);
}

#[tokio::test]
async fn expired_live_snapshot_can_create_branch_from_preserved_tag() {
    let (dir, table, _) = tagged_table().await;
    let root = dir.path().join("table");
    table.file_io().delete_file(SNAPSHOT_PATH).await.unwrap();
    assert!(!table.file_io().exists(SNAPSHOT_PATH).await.unwrap());
    let before = source_files(&root);
    let tagged = table
        .copy_with_time_travel_strict(HashMap::from([("scan.tag-name".into(), "base".into())]))
        .await
        .unwrap();
    assert_eq!(rows(&tagged).await, vec![(1, 10), (2, 20)]);

    let manager = BranchManager::new(table.file_io().clone(), TABLE_PATH.into());
    let result = manager.create_branch_from_tag("dev", "base").await;
    assert_eq!(source_files(&root), before);
    assert_eq!(rows(&table).await, vec![(1, 10), (2, 20), (3, 30)]);
    assert_eq!(rows(&tagged).await, vec![(1, 10), (2, 20)]);
    result.expect("a preserved tag must support branch creation after live snapshot expiry");

    for path in ["tag/tag-base", "schema/schema-0"] {
        assert_eq!(
            std::fs::read(root.join("branch/branch-dev").join(path)).unwrap(),
            before[Path::new(path)]
        );
    }
    let branch = table.copy_with_branch("dev").await.unwrap();
    assert_eq!(rows(&branch).await, vec![(1, 10), (2, 20)]);
    assert_eq!(
        serde_json::to_value(branch.snapshot_manager().get_snapshot(1).await.unwrap()).unwrap(),
        serde_json::to_value(table.tag_manager().get("base").await.unwrap().unwrap()).unwrap()
    );
    assert_eq!(source_files(&root), before);
}

#[tokio::test]
async fn expired_snapshot_preserves_java_metadata_like_live_copy() {
    let (dir, table, _) = tagged_table().await;
    let root = dir.path().join("table");
    table
        .new_write_builder()
        .try_new_commit()
        .unwrap()
        .truncate_table()
        .await
        .unwrap();
    assert!(rows(&table).await.is_empty());
    let snapshot = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    table
        .tag_manager()
        .create_with_retention("truncated", &snapshot, Some("1 d"))
        .await
        .unwrap();

    let snapshot_file = format!("snapshot/snapshot-{}", snapshot.id());
    let tag_file = "tag/tag-truncated";
    let mut expected: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join(&snapshot_file)).unwrap()).unwrap();
    let mut tag: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join(tag_file)).unwrap()).unwrap();
    // Current Java records the logical operation; preserve unknown snapshot fields too.
    for payload in [&mut expected, &mut tag] {
        payload["operation"] = serde_json::json!("TRUNCATE");
        payload["futureSnapshotMetadata"] = serde_json::json!({
            "sequence": u64::MAX,
            "flags": [true, null]
        });
    }
    std::fs::write(
        root.join(&snapshot_file),
        serde_json::to_vec_pretty(&expected).unwrap(),
    )
    .unwrap();
    std::fs::write(root.join(tag_file), serde_json::to_vec(&tag).unwrap()).unwrap();

    // Keep a newer live snapshot so the source table remains readable after expiry.
    write_batch(&table, &make_batch(vec![4], vec![40])).await;
    let manager = BranchManager::new(table.file_io().clone(), TABLE_PATH.into());
    let before_live = source_files(&root);
    manager
        .create_branch_from_tag("live", "truncated")
        .await
        .unwrap();
    assert_eq!(source_files(&root), before_live);
    let live_bytes = std::fs::read(root.join("branch/branch-live").join(&snapshot_file)).unwrap();
    assert_eq!(live_bytes, before_live[Path::new(&snapshot_file)]);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&live_bytes).unwrap(),
        expected
    );

    table
        .file_io()
        .delete_file(&table.snapshot_manager().snapshot_path(snapshot.id()))
        .await
        .unwrap();
    let before_expired = source_files(&root);
    manager
        .create_branch_from_tag("restored", "truncated")
        .await
        .unwrap();
    assert_eq!(source_files(&root), before_expired);
    let restored: serde_json::Value = serde_json::from_slice(
        &std::fs::read(root.join("branch/branch-restored").join(&snapshot_file)).unwrap(),
    )
    .unwrap();
    assert_eq!(restored, expected);
    assert!(restored.get("tagCreateTime").is_none());
    assert!(restored.get("tagTimeRetained").is_none());
    for branch_name in ["live", "restored"] {
        assert_eq!(
            std::fs::read(root.join(format!("branch/branch-{branch_name}/{tag_file}"))).unwrap(),
            before_expired[Path::new(tag_file)]
        );
        let branch = table.copy_with_branch(branch_name).await.unwrap();
        assert!(rows(&branch).await.is_empty());
    }
    assert_eq!(rows(&table).await, vec![(4, 40)]);
    assert_eq!(source_files(&root), before_expired);
}

#[tokio::test]
async fn missing_tag_is_rejected_before_creating_branch_files() {
    let (dir, table, _) = tagged_table().await;
    let root = dir.path().join("table");
    let before = source_files(&root);
    let manager = BranchManager::new(table.file_io().clone(), TABLE_PATH.into());
    let error = manager
        .create_branch_from_tag("dev", "missing")
        .await
        .unwrap_err();
    assert!(
        matches!(error, paimon::Error::DataInvalid { ref message, .. }
        if message == "Tag 'missing' does not exist.")
    );
    assert!(!manager.branch_exists("dev").await.unwrap());
    assert_eq!(source_files(&root), before);
}

#[derive(Debug)]
struct FailingSnapshotProvider {
    operator: opendal::Operator,
    kind: opendal::ErrorKind,
    path: &'static str,
    allowed_calls: usize,
    matching_calls: AtomicUsize,
}

#[async_trait::async_trait]
impl FileIOProvider for FailingSnapshotProvider {
    async fn create(&self, path: &str) -> paimon::Result<(opendal::Operator, String)> {
        if path == self.path {
            let calls = self.matching_calls.fetch_add(1, Ordering::SeqCst);
            if calls >= self.allowed_calls {
                return Err(
                    opendal::Error::new(self.kind, "injected snapshot storage failure").into(),
                );
            }
        }
        Ok((
            self.operator.clone(),
            path.strip_prefix("file:/").unwrap().into(),
        ))
    }
}

#[tokio::test]
async fn snapshot_storage_errors_are_propagated_without_fallback() {
    for kind in [
        opendal::ErrorKind::PermissionDenied,
        opendal::ErrorKind::Unexpected,
    ] {
        // Fail existence checking, copying, reading the copied tag, or fallback writing.
        for (missing_snapshot, path, allowed_calls) in [
            (false, SNAPSHOT_PATH, 0),
            (false, SNAPSHOT_PATH, 1),
            (true, BRANCH_TAG_PATH, 1),
            (true, BRANCH_SNAPSHOT_PATH, 0),
        ] {
            let (dir, table, operator) = tagged_table().await;
            let root = dir.path().join("table");
            if missing_snapshot {
                table.file_io().delete_file(SNAPSHOT_PATH).await.unwrap();
            }
            let before = source_files(&root);
            let provider = Arc::new(FailingSnapshotProvider {
                operator,
                kind,
                path,
                allowed_calls,
                matching_calls: AtomicUsize::new(0),
            });
            let io = FileIOBuilder::new("file")
                .with_provider(provider.clone())
                .build()
                .unwrap();
            let error = BranchManager::new(io, TABLE_PATH.into())
                .create_branch_from_tag("dev", "base")
                .await
                .unwrap_err();
            assert!(
                matches!(error, paimon::Error::IoUnexpected { ref source, .. }
                if source.kind() == kind && source.to_string().contains("injected snapshot storage failure"))
            );
            assert!(provider.matching_calls.load(Ordering::SeqCst) > allowed_calls);
            assert!(root.join("branch/branch-dev/tag/tag-base").is_file());
            assert!(!root.join("branch/branch-dev/snapshot/snapshot-1").exists());
            assert_eq!(source_files(&root), before);
            assert_eq!(rows(&table).await, vec![(1, 10), (2, 20), (3, 30)]);
        }
    }
}
