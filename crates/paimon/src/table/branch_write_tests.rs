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

use super::*;
use crate::io::FileIOBuilder;
use crate::spec::{DataType, IntType, Schema};
use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType as ArrowType, Field, Schema as ArrowSchema};
use std::sync::Arc;

async fn table(primary_key: bool, options: &[(&str, &str)]) -> (tempfile::TempDir, Table) {
    let dir = tempfile::tempdir().unwrap();
    // Scope the real filesystem operator to this temporary directory. OpenDAL's
    // default root "/" cannot list an absolute path on another Windows drive.
    let mut config = opendal_service_fs::FsConfig::default();
    config.root = Some(dir.path().to_str().unwrap().to_string());
    let file_io = FileIOBuilder::new("file")
        .with_fs_operator(opendal::Operator::from_config(config).unwrap())
        .build()
        .unwrap();
    let mut builder = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()));
    if primary_key {
        builder = builder.primary_key(vec!["id"]).option("bucket", "1");
    }
    for (key, value) in options {
        builder = builder.option(*key, *value);
    }
    let table = Table::new(
        file_io.clone(),
        Identifier::new("db", "t"),
        "file:/db.db/t".into(),
        TableSchema::new(0, &builder.build().unwrap()),
        None,
    );
    file_io
        .new_output(&table.schema_manager().schema_path(0))
        .unwrap()
        .write(bytes::Bytes::from(
            serde_json::to_vec(table.schema()).unwrap(),
        ))
        .await
        .unwrap();
    (dir, table)
}

fn batch(ids: Vec<i32>, values: Vec<i32>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowType::Int32, true),
            Field::new("value", ArrowType::Int32, true),
        ])),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(Int32Array::from(values)),
        ],
    )
    .unwrap()
}

async fn append(table: &Table, user: &str, data: RecordBatch) -> Vec<CommitMessage> {
    let builder = table.new_write_builder().with_commit_user(user).unwrap();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&data).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    builder
        .try_new_commit()
        .unwrap()
        .commit(messages.clone())
        .await
        .unwrap();
    messages
}

async fn tagged_branch(table: &Table) -> Table {
    let snapshot = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    table.tag_manager().create("base", &snapshot).await.unwrap();
    BranchManager::new(table.file_io().clone(), table.location().into())
        .create_branch_from_tag("dev", "base")
        .await
        .unwrap();
    table.copy_with_branch("dev").await.unwrap()
}

#[tokio::test]
async fn branch_writes_keep_append_and_primary_key_histories_isolated() {
    for primary_key in [false, true] {
        let (_dir, main) = table(primary_key, &[]).await;
        append(&main, "seed", batch(vec![1], vec![10])).await;
        let branch = tagged_branch(&main).await;
        append(&main, "writer", batch(vec![1], vec![100])).await;
        append(&branch, "writer", batch(vec![1], vec![20])).await;
        let main_rows = table_write::tests::read_id_value_rows(&main).await;
        let branch_rows = table_write::tests::read_id_value_rows(&branch).await;
        assert_eq!(
            main_rows,
            if primary_key {
                vec![(1, 100)]
            } else {
                vec![(1, 10), (1, 100)]
            }
        );
        assert_eq!(
            branch_rows,
            if primary_key {
                vec![(1, 20)]
            } else {
                vec![(1, 10), (1, 20)]
            }
        );
        let main_snapshot = main
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        let branch_snapshot = branch
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(main_snapshot.id(), 2);
        assert_eq!(branch_snapshot.id(), 2);
        assert_ne!(main_snapshot.uuid(), branch_snapshot.uuid());
    }
}

async fn stage(
    table: &Table,
    user: &str,
    data: RecordBatch,
    overwrite: bool,
) -> Vec<CommitMessage> {
    let builder = table.new_write_builder().with_commit_user(user).unwrap();
    let builder = if overwrite {
        builder.with_overwrite()
    } else {
        builder
    };
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&data).await.unwrap();
    writer.prepare_commit().await.unwrap()
}

async fn latest(table: &Table) -> crate::spec::Snapshot {
    table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn branch_write_checkpoint_recovery_is_scoped_to_the_branch() {
    let (_dir, main) = table(true, &[]).await;
    append(&main, "seed", batch(vec![1], vec![10])).await;
    let branch = tagged_branch(&main).await;
    for (target, value) in [(&main, 100), (&branch, 20)] {
        let messages = stage(target, "same-writer", batch(vec![1], vec![value]), false).await;
        let commit = target
            .new_write_builder()
            .with_commit_user("same-writer")
            .unwrap()
            .new_commit();
        commit
            .filter_and_commit_with_identifier(messages.clone(), 7)
            .await
            .unwrap();
        assert_eq!(latest(target).await.id(), 2);
        assert_eq!(latest(target).await.commit_identifier(), 7);
        commit
            .filter_and_commit_with_identifier(messages, 7)
            .await
            .unwrap();
        assert_eq!(latest(target).await.id(), 2);
    }
    assert_eq!(
        table_write::tests::read_id_value_rows(&main).await,
        vec![(1, 100)]
    );
    assert_eq!(
        table_write::tests::read_id_value_rows(&branch).await,
        vec![(1, 20)]
    );
}

#[tokio::test]
async fn branch_write_overwrite_and_truncate_preserve_main_and_shared_files() {
    for primary_key in [false, true] {
        let (_dir, main) = table(primary_key, &[]).await;
        let original = append(&main, "seed", batch(vec![1], vec![10])).await;
        let branch = tagged_branch(&main).await;
        let before = latest(&main).await;
        let messages = stage(&branch, "overwrite", batch(vec![2], vec![20]), true).await;
        let commit = branch
            .new_write_builder()
            .with_commit_user("overwrite")
            .unwrap()
            .new_commit();
        commit.overwrite(messages, None).await.unwrap();
        assert_eq!(
            table_write::tests::read_id_value_rows(&branch).await,
            vec![(2, 20)]
        );
        assert_eq!(latest(&main).await, before);
        commit.truncate_table().await.unwrap();
        assert_eq!(latest(&branch).await.id(), 3);
        assert!(table_write::tests::read_id_value_rows(&branch)
            .await
            .is_empty());
        assert_eq!(
            table_write::tests::read_id_value_rows(&main).await,
            vec![(1, 10)]
        );
        let scan = main.new_read_builder().new_scan().plan().await.unwrap();
        assert_eq!(
            scan.splits()
                .iter()
                .map(|split| split.row_count())
                .sum::<i64>(),
            1
        );
        assert!(!original.is_empty());
    }
}

#[tokio::test]
async fn branch_write_dynamic_buckets_load_only_the_branch_hash_index() {
    let (_dir, main) = table(
        true,
        &[("bucket", "-1"), ("dynamic-bucket.target-row-num", "1")],
    )
    .await;
    append(&main, "seed", batch(vec![1], vec![10])).await;
    let branch = tagged_branch(&main).await;
    append(&main, "main", batch(vec![2], vec![20])).await;
    append(&main, "main", batch(vec![3], vec![30])).await;
    let messages = append(&branch, "dev", batch(vec![4], vec![40])).await;
    assert_eq!(
        messages
            .iter()
            .map(|message| message.bucket)
            .collect::<Vec<_>>(),
        vec![1]
    );
    assert_eq!(
        table_write::tests::read_id_value_rows(&branch).await,
        vec![(1, 10), (4, 40)]
    );
    assert_eq!(
        table_write::tests::read_id_value_rows(&main).await,
        vec![(1, 10), (2, 20), (3, 30)]
    );
}

fn update_batch(row_id: i64, value: i32) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("_ROW_ID", ArrowType::Int64, false),
            Field::new("value", ArrowType::Int32, true),
        ])),
        vec![
            Arc::new(arrow_array::Int64Array::from(vec![row_id])),
            Arc::new(Int32Array::from(vec![value])),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn branch_write_row_ids_and_conflicts_use_branch_snapshot_history() {
    let (_dir, main) = table(
        false,
        &[
            ("data-evolution.enabled", "true"),
            ("row-tracking.enabled", "true"),
        ],
    )
    .await;
    append(&main, "seed", batch(vec![1], vec![10])).await;
    let branch = tagged_branch(&main).await;
    let stale_main = main
        .new_write_builder()
        .new_update()
        .unwrap()
        .update_by_arrow_with_row_id(vec![update_batch(0, 100)])
        .await
        .unwrap();
    let update = branch
        .new_write_builder()
        .new_update()
        .unwrap()
        .update_by_arrow_with_row_id(vec![update_batch(0, 20)])
        .await
        .unwrap();
    branch
        .new_write_builder()
        .new_commit()
        .commit(update)
        .await
        .unwrap();
    // A change to the same column in another branch is not a conflict.
    main.new_write_builder()
        .new_commit()
        .commit(stale_main)
        .await
        .unwrap();
    assert_eq!(
        table_write::tests::read_id_value_rows(&branch).await,
        vec![(1, 20)]
    );
    assert_eq!(
        table_write::tests::read_id_value_rows(&main).await,
        vec![(1, 100)]
    );
    let stale = branch
        .new_write_builder()
        .new_update()
        .unwrap()
        .update_by_arrow_with_row_id(vec![update_batch(0, 21)])
        .await
        .unwrap();
    let fresh = branch
        .new_write_builder()
        .new_update()
        .unwrap()
        .update_by_arrow_with_row_id(vec![update_batch(0, 22)])
        .await
        .unwrap();
    branch
        .new_write_builder()
        .new_commit()
        .commit(fresh)
        .await
        .unwrap();
    let error = branch
        .new_write_builder()
        .new_commit()
        .commit(stale)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("multiple MERGE INTO"), "{error}");
    assert_eq!(latest(&branch).await.id(), 3);
    assert_eq!(latest(&branch).await.next_row_id(), Some(1));
    assert_eq!(latest(&main).await.id(), 2);
    assert_eq!(
        table_write::tests::read_id_value_rows(&branch).await,
        vec![(1, 22)]
    );
}

#[tokio::test]
async fn branch_write_empty_branch_starts_its_own_row_id_space() {
    let (_dir, main) = table(
        false,
        &[
            ("data-evolution.enabled", "true"),
            ("row-tracking.enabled", "true"),
        ],
    )
    .await;
    append(&main, "seed", batch(vec![1, 2], vec![10, 20])).await;
    BranchManager::new(main.file_io().clone(), main.location().into())
        .create_branch("empty")
        .await
        .unwrap();
    let branch = main.copy_with_branch("empty").await.unwrap();
    append(&branch, "dev", batch(vec![3], vec![30])).await;
    assert_eq!(latest(&branch).await.id(), 1);
    assert_eq!(latest(&branch).await.next_row_id(), Some(1));
    assert_eq!(latest(&main).await.next_row_id(), Some(2));
    let update = branch
        .new_write_builder()
        .new_update()
        .unwrap()
        .update_by_arrow_with_row_id(vec![update_batch(0, 31)])
        .await
        .unwrap();
    branch
        .new_write_builder()
        .new_commit()
        .commit(update)
        .await
        .unwrap();
    assert_eq!(
        table_write::tests::read_id_value_rows(&branch).await,
        vec![(3, 31)]
    );
    assert_eq!(
        table_write::tests::read_id_value_rows(&main).await,
        vec![(1, 10), (2, 20)]
    );
}

#[tokio::test]
async fn branch_write_missing_schema_never_creates_a_branch() {
    let (_dir, main) = table(false, &[]).await;
    let branch = main
        .copy_with_resolved_schema(main.schema().clone(), "absent")
        .unwrap();
    let mut writer = branch.new_write_builder().new_write().unwrap();
    let error = writer
        .write_arrow_batch(&batch(vec![1], vec![10]))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("does not contain schema"),
        "{error}"
    );
    let messages = stage(&main, "writer", batch(vec![1], vec![10]), false).await;
    let error = branch
        .new_write_builder()
        .new_commit()
        .commit(messages)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("does not exist"), "{error}");
    assert!(branch
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .is_none());
    assert!(main
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .is_none());
    BranchManager::new(main.file_io().clone(), main.location().into())
        .create_branch("absent")
        .await
        .unwrap();
    let error = writer
        .write_arrow_batch(&batch(vec![1], vec![10]))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("cannot be reused"), "{error}");
    assert!(writer.prepare_commit().await.is_err());
}

#[tokio::test]
async fn branch_write_historical_handles_remain_read_only() {
    let (_dir, main) = table(false, &[]).await;
    append(&main, "seed", batch(vec![1], vec![10])).await;
    let branch = tagged_branch(&main).await;
    let snapshot = latest(&branch).await;
    let mut historical = branch.copy_with_pinned_snapshot(Some(&snapshot));
    // The pinned snapshot remains read-only even with no selector in its options.
    historical.schema = historical
        .schema
        .copy_with_replaced_options(branch.schema().options().clone());
    assert!(historical.new_write_builder().new_write().is_err());
    assert!(historical.new_write_builder().try_new_commit().is_err());
    assert!(historical
        .new_write_builder()
        .new_commit()
        .commit(vec![])
        .await
        .is_err());
    assert_eq!(latest(&branch).await, snapshot);
}

#[tokio::test]
async fn branch_write_renaming_publisher_routes_snapshot_and_hint_by_argument() {
    use super::snapshot_commit::{RenamingSnapshotCommit, SnapshotCommit};
    let (_dir, main) = table(false, &[]).await;
    append(&main, "seed", batch(vec![1], vec![10])).await;
    let original = latest(&main).await;
    BranchManager::new(main.file_io().clone(), main.location().into())
        .create_branch("dev")
        .await
        .unwrap();
    let publisher = RenamingSnapshotCommit::new(main.snapshot_manager());
    assert!(publisher.commit(None, &original, "dev", &[]).await.unwrap());
    let branch = main.copy_with_branch("dev").await.unwrap();
    assert_eq!(latest(&branch).await, original);
    assert!(!publisher.commit(None, &original, "dev", &[]).await.unwrap());
    assert_eq!(latest(&main).await, original);
    let branch_hint = format!("{}/LATEST", branch.snapshot_manager().snapshot_dir());
    assert_eq!(
        main.file_io()
            .new_input(&branch_hint)
            .unwrap()
            .read()
            .await
            .unwrap()
            .as_ref(),
        b"1"
    );
    assert!(publisher
        .commit(None, &original, "../invalid", &[])
        .await
        .is_err());
}

#[tokio::test]
async fn branch_write_schema_id_is_resolved_in_its_own_namespace() {
    let (_dir, main) = table(false, &[]).await;
    append(&main, "seed", batch(vec![1], vec![10])).await;
    let branch = tagged_branch(&main).await;
    let evolved = main
        .schema()
        .apply_changes(vec![crate::spec::SchemaChange::SetOption {
            key: "target-file-row-num".into(),
            value: "10".into(),
        }])
        .unwrap();
    main.file_io()
        .new_output(&main.schema_manager().schema_path(evolved.id()))
        .unwrap()
        .write(bytes::Bytes::from(serde_json::to_vec(&evolved).unwrap()))
        .await
        .unwrap();
    // A stale writer may publish under the latest schema ID, but that ID must
    // come from its branch, not from an independently evolving main branch.
    append(&main, "main", batch(vec![2], vec![20])).await;
    append(&branch, "dev", batch(vec![3], vec![30])).await;
    assert_eq!(latest(&main).await.schema_id(), 1);
    assert_eq!(latest(&branch).await.schema_id(), 0);
    assert_eq!(main.copy_with_branch("dev").await.unwrap().schema().id(), 0);
    let evolved = evolved
        .apply_changes(vec![crate::spec::SchemaChange::SetOption {
            key: "target-file-row-num".into(),
            value: "20".into(),
        }])
        .unwrap();
    branch
        .file_io()
        .new_output(&branch.schema_manager().schema_path(evolved.id()))
        .unwrap()
        .write(bytes::Bytes::from(serde_json::to_vec(&evolved).unwrap()))
        .await
        .unwrap();
    append(&branch, "dev", batch(vec![4], vec![40])).await;
    assert_eq!(latest(&branch).await.schema_id(), 2);
    assert_eq!(latest(&main).await.schema_id(), 1);
    assert_eq!(
        table_write::tests::read_id_value_rows(&branch).await,
        vec![(1, 10), (3, 30), (4, 40)]
    );
}

#[tokio::test]
async fn branch_write_rest_publisher_routes_by_argument_and_preserves_payload() {
    use crate::api::rest_api::RESTApi;
    use crate::table::snapshot_commit::{RESTSnapshotCommit, SnapshotCommit};
    use axum::{extract::Path, routing::post, Json, Router};
    use std::sync::Mutex;

    let requests = Arc::new(Mutex::new(Vec::new()));
    let recorded = requests.clone();
    let app = Router::new().route(
        "/v1/databases/:db/tables/:table/commit",
        post(
            move |Path((db, table)): Path<(String, String)>,
                  Json(body): Json<serde_json::Value>| {
                let recorded = recorded.clone();
                async move {
                    recorded.lock().unwrap().push((db, table, body));
                    Json(serde_json::json!({"success": true}))
                }
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut options = crate::Options::new();
    options.set("uri", format!("http://{address}"));
    options.set("warehouse", "test");
    options.set("token.provider", "bear");
    options.set("token", "test-token");
    let api = Arc::new(RESTApi::new(options, false).await.unwrap());
    // Selecting main must strip the old branch from the publisher's identifier.
    let publisher = RESTSnapshotCommit::new(
        api,
        Identifier::new("db", "t$branch_old"),
        "table-uuid".into(),
    );
    let (_dir, main) = table(false, &[]).await;
    append(&main, "seed", batch(vec![1], vec![10])).await;
    let snapshot = latest(&main).await;
    for branch in ["main", "dev", "audit"] {
        assert!(publisher
            .commit(Some("base-uuid"), &snapshot, branch, &[])
            .await
            .unwrap());
    }
    assert!(publisher
        .commit(None, &snapshot, "../bad", &[])
        .await
        .is_err());
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    for (request, name) in requests.iter().zip(["t", "t$branch_dev", "t$branch_audit"]) {
        assert_eq!(request.0, "db");
        assert_eq!(request.1, name);
        assert_eq!(request.2["tableId"], "table-uuid");
        assert_eq!(request.2["baseSnapshotUuid"], "base-uuid");
        assert_eq!(
            request.2["snapshot"],
            serde_json::to_value(&snapshot).unwrap()
        );
        assert_eq!(request.2["statistics"], serde_json::json!([]));
    }
    server.abort();
}

#[tokio::test]
async fn branch_write_sequence_numbers_ignore_main_advancement() {
    let (_dir, main) = table(true, &[]).await;
    append(&main, "seed", batch(vec![1], vec![10])).await;
    let branch = tagged_branch(&main).await;
    append(&main, "main", batch(vec![1], vec![100])).await;
    append(&main, "main", batch(vec![1], vec![200])).await;
    let messages = stage(&branch, "dev", batch(vec![1], vec![20]), false).await;
    assert_eq!(messages[0].new_files[0].min_sequence_number, 1);
    assert_eq!(messages[0].new_files[0].max_sequence_number, 1);
    branch
        .new_write_builder()
        .new_commit()
        .commit(messages)
        .await
        .unwrap();
    assert_eq!(latest(&main).await.id(), 3);
    assert_eq!(latest(&branch).await.id(), 2);
    assert_eq!(
        table_write::tests::read_id_value_rows(&main).await,
        vec![(1, 200)]
    );
    assert_eq!(
        table_write::tests::read_id_value_rows(&branch).await,
        vec![(1, 20)]
    );
}

async fn fixed_bucket_write(table: &Table, value: i32, buckets: i32, overwrite: bool) {
    let plan_batch = RecordBatch::try_from_iter([(
        "total_buckets",
        Arc::new(Int32Array::from(vec![buckets])) as arrow_array::ArrayRef,
    )])
    .unwrap();
    let plan = PostponeBucketPlan::from_arrow(table, &plan_batch).unwrap();
    let builder = table
        .new_postpone_fixed_bucket_write_builder()
        .unwrap()
        .with_bucket_plan(plan);
    let builder = if overwrite {
        builder.with_overwrite()
    } else {
        builder
    };
    let mut writer = builder.new_write().unwrap();
    writer
        .write_arrow_batch(&batch(vec![1], vec![value]))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    builder
        .try_new_commit()
        .unwrap()
        .commit(messages)
        .await
        .unwrap();
}

#[tokio::test]
async fn branch_write_postpone_restores_branch_bucket_layout_and_sequence() {
    let (_dir, main) = table(
        true,
        &[("bucket", "-2"), ("postpone.default-bucket-num", "2")],
    )
    .await;
    fixed_bucket_write(&main, 10, 2, false).await;
    let branch = tagged_branch(&main).await;
    // Main changes the partition's layout and advances beyond dev's baseline.
    fixed_bucket_write(&main, 100, 3, true).await;
    fixed_bucket_write(&main, 200, 3, false).await;
    let builder = branch.new_postpone_fixed_bucket_write_builder().unwrap();
    let mut writer = builder.new_write().unwrap();
    writer
        .write_arrow_batch(&batch(vec![1], vec![20]))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages[0].total_buckets, Some(2));
    assert_eq!(messages[0].check_from_snapshot, Some(1));
    assert_eq!(messages[0].new_files[0].min_sequence_number, 1);
    builder
        .try_new_commit()
        .unwrap()
        .commit(messages)
        .await
        .unwrap();
    assert_eq!(latest(&main).await.id(), 3);
    assert_eq!(latest(&branch).await.id(), 2);
    assert_eq!(
        table_write::tests::read_id_value_rows(&main).await,
        vec![(1, 200)]
    );
    assert_eq!(
        table_write::tests::read_id_value_rows(&branch).await,
        vec![(1, 20)]
    );
}
