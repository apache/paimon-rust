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

//! A resolved storage provider must preserve the REST publication route.

use super::*;
use crate::io::{FileIOBuilder, FileIOProvider};
use crate::spec::{DataType, IntType, Schema, Snapshot, VarBinaryType};
use crate::table::SnapshotManager;
use arrow_array::{BinaryArray, Int32Array, RecordBatch};
use axum::extract::{Path, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::TryStreamExt;
use opendal::Operator;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use tokio::sync::Mutex as AsyncMutex;

const ROOT: &str = "custom://bucket/root";

#[derive(Debug)]
struct CustomStorage {
    operator: Operator,
    paths: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl FileIOProvider for CustomStorage {
    async fn create(&self, path: &str) -> Result<(Operator, String)> {
        self.paths.lock().unwrap().push(path.into());
        let relative =
            path.strip_prefix("custom://bucket/")
                .ok_or_else(|| Error::ConfigInvalid {
                    message: format!("wrong storage route: {path}"),
                })?;
        Ok((self.operator.clone(), relative.into()))
    }
}

struct CatalogState {
    response: serde_json::Value,
    io: FileIO,
    latest: AsyncMutex<HashMap<String, Snapshot>>,
    commits: Mutex<Vec<String>>,
    snapshots: AtomicUsize,
    response_version: AtomicUsize,
}

async fn table_response(
    Path(object): Path<String>,
    State(state): State<Arc<CatalogState>>,
) -> Json<serde_json::Value> {
    let mut response = state.response.clone();
    response["name"] = serde_json::json!(object);
    if object == "other" {
        response["id"] = serde_json::json!("other-uuid");
        response["path"] = serde_json::json!("custom://bucket/other");
    }
    match state.response_version.load(Ordering::SeqCst) {
        1 => response["id"] = serde_json::json!("replacement-uuid"),
        2 => {
            response.as_object_mut().unwrap().remove("id");
        }
        _ => {}
    }
    Json(response)
}

async fn snapshot_response(
    Path(object): Path<String>,
    State(state): State<Arc<CatalogState>>,
) -> Json<serde_json::Value> {
    state.snapshots.fetch_add(1, Ordering::SeqCst);
    Json(serde_json::json!({
        "snapshot": state.latest.lock().await.get(&object).map(|snapshot| {
            serde_json::json!({"snapshot": snapshot})
        })
    }))
}

async fn commit_snapshot(
    Path(object): Path<String>,
    State(state): State<Arc<CatalogState>>,
    Json(body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    assert_eq!(body["tableId"], "table-uuid");
    let snapshot: Snapshot = serde_json::from_value(body["snapshot"].clone()).unwrap();
    let mut latest = state.latest.lock().await;
    assert_eq!(
        body["baseSnapshotUuid"].as_str(),
        latest.get(&object).and_then(Snapshot::uuid)
    );
    let identifier = Identifier::new("db", &object);
    let parsed = identifier.parsed_object_name().unwrap();
    let manager =
        SnapshotManager::new(state.io.clone(), ROOT.into()).with_branch(parsed.branch_or_default());
    let success = manager.commit_snapshot(&snapshot).await.unwrap();
    if success {
        latest.insert(object.clone(), snapshot);
        state.commits.lock().unwrap().push(object);
    }
    Json(serde_json::json!({"success": success}))
}

async fn write(table: &Table, id: i32) {
    let batch = RecordBatch::try_new(
        crate::arrow::build_target_arrow_schema(table.schema().fields()).unwrap(),
        vec![
            Arc::new(Int32Array::from(vec![id])),
            Arc::new(BinaryArray::from(vec![b"a/b".as_slice()])),
        ],
    )
    .unwrap();
    let builder = table
        .new_write_builder()
        .with_commit_user("custom-storage")
        .unwrap();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert!(!messages.is_empty());
    builder
        .try_new_commit()
        .unwrap()
        .commit(messages)
        .await
        .unwrap();
    writer.close().await;
}

async fn read_ids(table: &Table) -> Vec<i32> {
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
    batches
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
        .collect()
}

#[tokio::test]
async fn resolved_file_io_preserves_rest_publication_and_branch_routing() {
    // The built-in routing has no backend for this scheme. The provider is
    // deliberately able to access other paths, so leaking it is observable.
    assert!(FileIO::from_path(ROOT)
        .and_then(|builder| builder.build())
        .is_err());
    let storage = Arc::new(CustomStorage {
        operator: Operator::from_config(opendal::services::MemoryConfig::default()).unwrap(),
        paths: Mutex::new(Vec::new()),
    });
    let io = FileIOBuilder::new("application-storage")
        .with_provider(storage.clone())
        .build()
        .unwrap();
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("p", DataType::VarBinary(VarBinaryType::new(16).unwrap()))
        .partition_keys(vec!["p"])
        .build()
        .unwrap();
    let state = Arc::new(CatalogState {
        response: serde_json::json!({
            "database": "db", "name": "t", "id": "table-uuid", "schemaId": 0,
            "path": ROOT, "isExternal": false, "schema": schema
        }),
        io: io.clone(),
        latest: AsyncMutex::new(HashMap::new()),
        commits: Mutex::new(Vec::new()),
        snapshots: AtomicUsize::new(0),
        response_version: AtomicUsize::new(0),
    });
    let app = Router::new()
        .route("/v1/databases/db/tables/:object", get(table_response))
        .route(
            "/v1/databases/db/tables/:object/snapshot",
            get(snapshot_response),
        )
        .route(
            "/v1/databases/db/tables/:object/commit",
            post(commit_snapshot),
        )
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut options = Options::new();
    options.set("uri", format!("http://{address}"));
    options.set("warehouse", "test");
    options.set("token.provider", "bear");
    options.set("token", "test-token");
    // No table-token endpoint exists. A resolved provider owns credential
    // refresh; enabling catalog tokens must not initialize a built-in backend.
    options.set("data-token.enabled", "true");
    let table = Table::from_rest_response_with_file_io(
        Identifier::new("db", "t"),
        serde_json::from_value(state.response.clone()).unwrap(),
        options,
        io,
    )
    .await
    .unwrap();
    write(&table, 7).await;
    assert_eq!(read_ids(&table).await, vec![7]);

    let env = table.rest_env().unwrap();
    let branch = env
        .get_table(&Identifier::new("db", "t$branch_dev"))
        .await
        .unwrap();
    assert_eq!(branch.branch(), "dev");
    assert!(read_ids(&branch).await.is_empty());
    write(&branch, 9).await;
    assert_eq!(read_ids(&branch).await, vec![9]);
    assert_eq!(read_ids(&table).await, vec![7]);
    let reopened = branch
        .rest_env()
        .unwrap()
        .get_table(&Identifier::new("db", "t$branch_dev"))
        .await
        .unwrap();
    assert_eq!(read_ids(&reopened).await, vec![9]);
    assert_eq!(*state.commits.lock().unwrap(), vec!["t", "t$branch_dev"]);
    assert!(state.snapshots.load(Ordering::SeqCst) > 0);
    assert!(storage
        .paths
        .lock()
        .unwrap()
        .iter()
        .any(|path| path.contains("/p=a%2Fb/")));

    let before = storage.paths.lock().unwrap().len();
    assert!(env
        .get_table(&Identifier::new("db", "other"))
        .await
        .is_err());
    assert_eq!(storage.paths.lock().unwrap().len(), before);
    // Replacing a catalog table under the same name must not bind the new
    // entity to credentials that were resolved for the previous table UUID.
    for version in [1, 2] {
        state.response_version.store(version, Ordering::SeqCst);
        assert!(matches!(
            env.get_table(&Identifier::new("db", "t$branch_dev")).await,
            Err(Error::DataInvalid { .. })
        ));
        assert_eq!(storage.paths.lock().unwrap().len(), before);
    }
    server.abort();
}
