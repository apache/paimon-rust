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

//! Read policies must not become SELECT requirements on ordinary REST writes.

use super::*;
use crate::io::FileIO;
use crate::spec::{DataType, Datum, IntType, Schema, VarCharType};
use arrow_array::{Int32Array, RecordBatch, StringArray};
use axum::{extract::State, http::StatusCode, routing::get, routing::post, Json, Router};
use futures::TryStreamExt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Mutex;

struct CatalogState {
    response: serde_json::Value,
    latest: Mutex<Option<Snapshot>>,
    io: Mutex<Option<FileIO>>,
    query_requests: AtomicUsize,
    commit_requests: AtomicUsize,
    deny_commit: AtomicBool,
    deny_query: AtomicBool,
    query_response: Mutex<serde_json::Value>,
}

struct RestTable {
    table: Table,
    state: Arc<CatalogState>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for RestTable {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl RestTable {
    async fn new(primary_key: bool, extra: &[(&str, &str)]) -> Self {
        let mut schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("p", DataType::VarChar(VarCharType::string_type()))
            .column("value", DataType::Int(IntType::new()))
            .partition_keys(vec!["p"])
            .option("query-auth.enabled", "true");
        if primary_key {
            schema = schema.primary_key(vec!["p", "id"]).option("bucket", "1");
        }
        for (key, value) in extra {
            schema = schema.option(*key, *value);
        }
        let state = Arc::new(CatalogState {
            response: serde_json::json!({
                "database": "db", "name": "t", "id": "table-uuid", "schemaId": 0,
                "path": "memory:/authorized-writes", "isExternal": false,
                "schema": schema.build().unwrap()
            }),
            latest: Mutex::new(None),
            io: Mutex::new(None),
            query_requests: AtomicUsize::new(0),
            commit_requests: AtomicUsize::new(0),
            deny_commit: AtomicBool::new(false),
            deny_query: AtomicBool::new(true),
            query_response: Mutex::new(serde_json::json!({})),
        });
        let app = Router::new()
            .route(
                "/v1/databases/db/tables/t",
                get(|State(state): State<Arc<CatalogState>>| async move {
                    Json(state.response.clone())
                }),
            )
            .route(
                "/v1/databases/db/tables/t/snapshot",
                get(|State(state): State<Arc<CatalogState>>| async move {
                    let latest = state.latest.lock().await;
                    Json(serde_json::json!({
                        "snapshot": latest.as_ref().map(|snapshot| {
                            serde_json::json!({"snapshot": snapshot})
                        })
                    }))
                }),
            )
            .route("/v1/databases/db/tables/t/commit", post(commit_snapshot))
            .route(
                "/v1/databases/db/tables/t/auth",
                post(
                    |State(state): State<Arc<CatalogState>>,
                     Json(body): Json<serde_json::Value>| async move {
                        state.query_requests.fetch_add(1, Ordering::SeqCst);
                        assert!(
                            body.get("select").is_none_or(serde_json::Value::is_null),
                            "mutation authorization must cover the entire table: {body}"
                        );
                        if state.deny_query.load(Ordering::SeqCst) {
                            (
                                StatusCode::FORBIDDEN,
                                Json(serde_json::json!({"message": "SELECT denied"})),
                            )
                        } else {
                            (
                                StatusCode::OK,
                                Json(state.query_response.lock().await.clone()),
                            )
                        }
                    },
                ),
            )
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut options = crate::Options::new();
        options.set("uri", format!("http://{address}"));
        options.set("warehouse", "test");
        options.set("token.provider", "bear");
        options.set("token", "test-token");
        let table = Table::from_rest_response(
            Identifier::new("db", "t"),
            serde_json::from_value(state.response.clone()).unwrap(),
            options,
        )
        .await
        .unwrap();
        *state.io.lock().await = Some(table.file_io().clone());
        Self {
            table,
            state,
            server,
        }
    }

    async fn prepare(&self, user: &str, rows: &[(i32, &str, i32)]) -> Vec<CommitMessage> {
        let batch = batch(&self.table, rows);
        let builder = self
            .table
            .new_write_builder()
            .with_commit_user(user)
            .unwrap();
        let mut writer = builder.new_write().unwrap();
        writer.write_arrow_batch(&batch).await.unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        writer.close().await;
        messages
    }

    async fn append(&self, user: &str, rows: &[(i32, &str, i32)], fixed: bool) {
        if fixed {
            let builder = self
                .table
                .new_postpone_fixed_bucket_write_builder()
                .unwrap()
                .with_commit_user(user)
                .unwrap();
            let mut writer = builder.new_write().unwrap();
            writer
                .write_arrow_batch(&batch(&self.table, rows))
                .await
                .unwrap();
            let messages = writer.prepare_commit().await.unwrap();
            builder.new_commit().commit(messages).await.unwrap();
            writer.close().await;
        } else {
            self.table
                .new_write_builder()
                .with_commit_user(user)
                .unwrap()
                .try_new_commit()
                .unwrap()
                .commit(self.prepare(user, rows).await)
                .await
                .unwrap();
        }
    }

    // Inspect physical contents independently of the user's SELECT policy.
    // This unguarded handle exists only inside the test; production reads must
    // use the original catalog handle and still contact /auth.
    async fn stored_rows(&self) -> Vec<(i32, String, i32)> {
        let mut options = self.table.schema().options().clone();
        options.insert("query-auth.enabled".into(), "false".into());
        let schema = self.table.schema().copy_with_replaced_options(options);
        let table = Table::new(
            self.table.file_io().clone(),
            self.table.identifier().clone(),
            self.table.location().into(),
            schema,
            None,
        );
        let builder = table.new_read_builder();
        let plan = builder.new_scan().plan().await.unwrap();
        let batches: Vec<RecordBatch> = builder
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
            let partitions = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let values = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                rows.push((ids.value(i), partitions.value(i).into(), values.value(i)));
            }
        }
        rows.sort();
        rows
    }
}

#[path = "authorized_update_tests.rs"]
mod updates;

async fn commit_snapshot(
    State(state): State<Arc<CatalogState>>,
    Json(body): Json<serde_json::Value>,
) -> (StatusCode, Json<serde_json::Value>) {
    state.commit_requests.fetch_add(1, Ordering::SeqCst);
    assert_eq!(body["tableId"], "table-uuid");
    if state.deny_commit.load(Ordering::SeqCst) {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({"message": "INSERT denied"})),
        );
    }
    let snapshot: Snapshot = serde_json::from_value(body["snapshot"].clone()).unwrap();
    let mut latest = state.latest.lock().await;
    assert_eq!(
        body["baseSnapshotUuid"].as_str(),
        latest.as_ref().and_then(Snapshot::uuid)
    );
    let io = state.io.lock().await.clone().unwrap();
    let manager = SnapshotManager::new(io, "memory:/authorized-writes".into());
    let success = manager.commit_snapshot(&snapshot).await.unwrap();
    if success {
        *latest = Some(snapshot);
    }
    (
        StatusCode::OK,
        Json(serde_json::json!({"success": success})),
    )
}

fn batch(table: &Table, rows: &[(i32, &str, i32)]) -> RecordBatch {
    RecordBatch::try_new(
        crate::arrow::build_target_arrow_schema(table.schema().fields()).unwrap(),
        vec![
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.2))),
        ],
    )
    .unwrap()
}

#[derive(Default)]
struct Callback(Mutex<Vec<(i64, usize)>>);

#[async_trait::async_trait]
impl CommitCallback for Callback {
    async fn call(&self, context: &CommitCallbackContext) -> Result<()> {
        self.0
            .lock()
            .await
            .push((context.identifier, context.commit_entries.len()));
        Ok(())
    }
}

#[tokio::test]
async fn rest_postpone_resolve_bucket_plan_requires_write_identity_without_select_permission() {
    let fixture = RestTable::new(
        true,
        &[("bucket", "-2"), ("postpone.default-bucket-num", "2")],
    )
    .await;
    fixture
        .append("fixed", &[(1, "a", 10), (2, "b", 20)], true)
        .await;
    let table = fixture.table.copy_with_pinned_snapshot(None);
    // The REST response already carries the selected snapshot. The filesystem
    // copy is not required to inspect its manifests through a writer builder.
    table
        .file_io()
        .delete_file(&table.snapshot_manager().snapshot_path(1))
        .await
        .unwrap();
    let exported = table
        .new_postpone_fixed_bucket_write_builder()
        .unwrap()
        .resolve_bucket_plan()
        .await
        .unwrap()
        .to_arrow(&table)
        .unwrap();
    assert_eq!(exported.num_rows(), 2);
    assert_eq!(exported.schema().field(0).name(), "p");
    assert_eq!(
        exported
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values(),
        &[2, 2]
    );
    assert_eq!(fixture.state.query_requests.load(Ordering::SeqCst), 0);
    let mut schema = serde_json::to_value(table.schema()).unwrap();
    schema["fields"][2]["name"] = serde_json::json!("another_value");
    let unbound = table
        .copy_with_resolved_schema(serde_json::from_value(schema).unwrap(), "main")
        .unwrap();
    assert!(unbound.query_auth_session().is_none());
    assert!(unbound
        .new_postpone_fixed_bucket_write_builder()
        .unwrap()
        .resolve_bucket_plan()
        .await
        .is_err());
    assert_eq!(fixture.state.query_requests.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn rest_stream_recovery_and_callbacks_do_not_require_select_permission() {
    let fixture = RestTable::new(false, &[]).await;
    let first = fixture.prepare("stream", &[(1, "a", 10)]).await;
    let second = fixture.prepare("stream", &[(2, "b", 20)]).await;
    let callback = Arc::new(Callback::default());
    let mut commit = TableCommit::new(fixture.table.clone(), "stream".into());
    commit.add_commit_callback(callback.clone()).unwrap();
    commit
        .commit_with_identifier(first.clone(), 7)
        .await
        .unwrap();
    assert_eq!(
        commit
            .filter_and_commit(vec![(8, second), (7, first)])
            .await
            .unwrap(),
        1
    );
    assert_eq!(*callback.0.lock().await, vec![(7, 1), (8, 1)]);
    assert_eq!(
        fixture.stored_rows().await,
        vec![(1, "a".into(), 10), (2, "b".into(), 20)]
    );
    assert_eq!(fixture.state.query_requests.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn rest_writes_restore_state_without_select_permission() {
    for (primary_key, options, fixed) in [
        (false, vec![], false),
        (
            false,
            vec![
                ("row-tracking.enabled", "true"),
                ("data-evolution.enabled", "true"),
            ],
            false,
        ),
        (true, vec![], false),
        (
            true,
            vec![("bucket", "-1"), ("dynamic-bucket.target-row-num", "1")],
            false,
        ),
        (
            true,
            vec![("bucket", "-2"), ("postpone.default-bucket-num", "2")],
            true,
        ),
    ] {
        let fixture = RestTable::new(primary_key, &options).await;
        fixture
            .append("first", &[(1, "a", 10), (2, "b", 20)], fixed)
            .await;
        fixture.append("second", &[(1, "a", 99)], fixed).await;
        let mut expected = vec![(1, "a".into(), 99), (2, "b".into(), 20)];
        if !primary_key {
            expected.insert(0, (1, "a".into(), 10));
        }
        assert_eq!(fixture.stored_rows().await, expected);
        assert_eq!(fixture.state.query_requests.load(Ordering::SeqCst), 0);
        assert!(fixture
            .table
            .new_read_builder()
            .new_scan()
            .plan()
            .await
            .is_err());
        assert_eq!(fixture.state.query_requests.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn rest_overwrite_and_truncate_do_not_require_select_permission() {
    let fixture = RestTable::new(false, &[]).await;
    fixture
        .append("seed", &[(1, "a", 10), (2, "b", 20)], false)
        .await;
    let messages = fixture.prepare("replace", &[(3, "a", 30)]).await;
    TableCommit::new(fixture.table.clone(), "replace".into())
        .overwrite(
            messages,
            Some(HashMap::from([(
                "p".into(),
                Some(Datum::String("a".into())),
            )])),
        )
        .await
        .unwrap();
    assert_eq!(
        fixture.stored_rows().await,
        vec![(2, "b".into(), 20), (3, "a".into(), 30)]
    );
    TableCommit::new(fixture.table.clone(), "truncate-partition".into())
        .truncate_partitions(vec![HashMap::from([(
            "p".into(),
            Some(Datum::String("b".into())),
        )])])
        .await
        .unwrap();
    assert_eq!(fixture.stored_rows().await, vec![(3, "a".into(), 30)]);
    TableCommit::new(fixture.table.clone(), "truncate-table".into())
        .truncate_table()
        .await
        .unwrap();
    assert!(fixture.stored_rows().await.is_empty());
    assert_eq!(fixture.state.query_requests.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn rest_commit_denial_preserves_prepared_files() {
    let fixture = RestTable::new(false, &[]).await;
    let messages = fixture.prepare("denied", &[(1, "a", 10)]).await;
    let file = &messages[0].new_files[0];
    let path = format!(
        "{}/p=a/bucket-0/{}",
        fixture.table.location(),
        file.file_name
    );
    assert!(fixture.table.file_io().exists(&path).await.unwrap());
    fixture.state.deny_commit.store(true, Ordering::SeqCst);
    assert!(TableCommit::new(fixture.table.clone(), "denied".into())
        .commit(messages)
        .await
        .is_err());
    assert!(fixture.table.file_io().exists(&path).await.unwrap());
    assert!(fixture.state.latest.lock().await.is_none());
    assert!(fixture.state.commit_requests.load(Ordering::SeqCst) > 0);
    assert_eq!(fixture.state.query_requests.load(Ordering::SeqCst), 0);
}
