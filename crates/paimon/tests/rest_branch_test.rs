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

//! Java REST branch protocol, exercised through the public Catalog contract.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use paimon::api::RestError;
use paimon::catalog::{Catalog, Identifier, RESTCatalog};
use paimon::common::Options;
use paimon::Error;
use serde_json::{json, Value};

#[derive(Debug, PartialEq)]
struct Request {
    method: Method,
    uri: String,
    body: Option<Value>,
}

#[derive(Default)]
struct ServerState {
    responses: VecDeque<(StatusCode, Value)>,
    requests: Vec<Request>,
}

struct TestCatalog {
    catalog: RESTCatalog,
    state: Arc<Mutex<ServerState>>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for TestCatalog {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl TestCatalog {
    async fn new(responses: Vec<(StatusCode, Value)>) -> Self {
        async fn handle(
            State(state): State<Arc<Mutex<ServerState>>>,
            method: Method,
            uri: Uri,
            headers: HeaderMap,
            bytes: Bytes,
        ) -> Response {
            assert_eq!(headers["authorization"], "Bearer test-token");
            let mut state = state.lock().unwrap();
            state.requests.push(Request {
                method,
                uri: uri.to_string(),
                body: (!bytes.is_empty()).then(|| serde_json::from_slice(&bytes).unwrap()),
            });
            let (status, value) = state.responses.pop_front().expect("unexpected request");
            if status == StatusCode::NO_CONTENT {
                status.into_response()
            } else {
                (status, Json(value)).into_response()
            }
        }
        let state = Arc::new(Mutex::new(ServerState {
            responses: responses.into(),
            requests: vec![],
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = Router::new().fallback(handle).with_state(state.clone());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let mut options = Options::new();
        options.set("uri", format!("http://{address}"));
        options.set("warehouse", "warehouse");
        options.set("prefix", "tenant /一");
        options.set("token.provider", "bear");
        options.set("token", "test-token");
        let catalog = RESTCatalog::new(options, false).await.unwrap();
        Self {
            catalog,
            state,
            server,
        }
    }

    fn requests(&self) -> Vec<Request> {
        let mut state = self.state.lock().unwrap();
        assert!(
            state.responses.is_empty(),
            "not all expected requests were sent"
        );
        std::mem::take(&mut state.requests)
    }
}

#[tokio::test]
async fn branch_protocol_preserves_names_tags_and_server_order() {
    let ctx = TestCatalog::new(vec![
        (StatusCode::NO_CONTENT, Value::Null),
        (StatusCode::OK, json!({})),
        (StatusCode::OK, json!({"branches": ["z", "a", "main"]})),
        (StatusCode::NO_CONTENT, Value::Null),
        (StatusCode::NO_CONTENT, Value::Null),
    ])
    .await;
    let catalog: &dyn Catalog = &ctx.catalog;
    let identifier = Identifier::new("db /", "table?$branch_dev");
    catalog
        .create_branch(&identifier, "branch /?#", None)
        .await
        .unwrap();
    catalog
        .create_branch(&identifier, "from-tag", Some("tag /一"))
        .await
        .unwrap();
    assert_eq!(
        catalog.list_branches(&identifier).await.unwrap(),
        ["z", "a", "main"]
    );
    catalog
        .fast_forward(&identifier, "branch /?#")
        .await
        .unwrap();
    catalog
        .drop_branch(&identifier, "branch /?#")
        .await
        .unwrap();
    let base = "/v1/tenant+%2F%E4%B8%80/databases/db+%2F/tables/table%3F%24branch_dev/branches";
    assert_eq!(
        ctx.requests(),
        vec![
            Request {
                method: Method::POST,
                uri: base.into(),
                body: Some(json!({"branch": "branch /?#", "fromTag": null}))
            },
            Request {
                method: Method::POST,
                uri: base.into(),
                body: Some(json!({"branch": "from-tag", "fromTag": "tag /一"}))
            },
            Request {
                method: Method::GET,
                uri: base.into(),
                body: None
            },
            Request {
                method: Method::POST,
                uri: format!("{base}/branch+%2F%3F%23/forward"),
                body: Some(json!({}))
            },
            Request {
                method: Method::DELETE,
                uri: format!("{base}/branch+%2F%3F%23"),
                body: None
            },
        ]
    );
}

#[tokio::test]
async fn null_or_absent_branches_are_empty_like_java() {
    let ctx = TestCatalog::new(vec![
        (StatusCode::OK, json!({"branches": null})),
        (StatusCode::OK, json!({})),
    ])
    .await;
    for _ in 0..2 {
        assert!(ctx
            .catalog
            .list_branches(&Identifier::new("db", "t"))
            .await
            .unwrap()
            .is_empty());
    }
    assert_eq!(ctx.requests().len(), 2);
}

#[tokio::test]
async fn create_branch_maps_only_java_catalog_errors() {
    let ctx = TestCatalog::new(vec![
        (
            StatusCode::NOT_FOUND,
            json!({"code": 404, "resourceType": "TABLE", "message": "table missing"}),
        ),
        (
            StatusCode::NOT_FOUND,
            json!({"code": 404, "resourceType": "TAG", "message": "tag missing"}),
        ),
        (
            StatusCode::CONFLICT,
            json!({"code": 409, "message": "branch exists"}),
        ),
        (
            StatusCode::BAD_REQUEST,
            json!({"code": 400, "message": "invalid branch"}),
        ),
        (
            StatusCode::NOT_FOUND,
            json!({"code": 404, "resourceType": "OTHER", "message": "unknown resource"}),
        ),
    ])
    .await;
    let id = Identifier::new("db", "t");
    assert!(
        matches!(ctx.catalog.create_branch(&id, "dev", None).await, Err(Error::TableNotExist { full_name }) if full_name == "db.t")
    );
    assert!(
        matches!(ctx.catalog.create_branch(&id, "dev", Some("base")).await, Err(Error::TagNotExist { tag_name }) if tag_name == "base")
    );
    assert!(
        matches!(ctx.catalog.create_branch(&id, "dev", None).await, Err(Error::BranchAlreadyExist { branch_name, .. }) if branch_name == "dev")
    );
    assert!(
        matches!(ctx.catalog.create_branch(&id, "dev", None).await, Err(Error::DataInvalid { message, .. }) if message == "invalid branch")
    );
    assert!(matches!(
        ctx.catalog.create_branch(&id, "dev", None).await,
        Err(Error::RestApi {
            source: RestError::NoSuchResource { .. }
        })
    ));
    assert_eq!(ctx.requests().len(), 5);
}

#[tokio::test]
async fn missing_branch_and_table_errors_are_distinct() {
    let missing = (
        StatusCode::NOT_FOUND,
        json!({"code": 404, "message": "missing"}),
    );
    let ctx = TestCatalog::new(vec![missing.clone(), missing.clone(), missing]).await;
    let id = Identifier::new("db", "t");
    assert!(
        matches!(ctx.catalog.drop_branch(&id, "dev").await, Err(Error::BranchNotExist { full_name, branch_name }) if full_name == "db.t" && branch_name == "dev")
    );
    assert!(
        matches!(ctx.catalog.fast_forward(&id, "dev").await, Err(Error::BranchNotExist { full_name, branch_name }) if full_name == "db.t" && branch_name == "dev")
    );
    assert!(
        matches!(ctx.catalog.list_branches(&id).await, Err(Error::TableNotExist { full_name }) if full_name == "db.t")
    );
    assert_eq!(ctx.requests().len(), 3);
}

#[tokio::test]
async fn branch_operations_preserve_forbidden_and_unsupported_responses() {
    for status in [StatusCode::FORBIDDEN, StatusCode::NOT_IMPLEMENTED] {
        let ctx = TestCatalog::new(vec![
            (
                status,
                json!({"code": status.as_u16(), "message": "denied"})
            );
            4
        ])
        .await;
        let id = Identifier::new("db", "t");
        let results = [
            ctx.catalog.create_branch(&id, "dev", None).await,
            ctx.catalog.drop_branch(&id, "dev").await,
            ctx.catalog.fast_forward(&id, "dev").await,
            ctx.catalog.list_branches(&id).await.map(|_| ()),
        ];
        for result in results {
            assert!(matches!(
                result,
                Err(Error::RestApi {
                    source: RestError::Forbidden { .. } | RestError::NotImplemented { .. }
                })
            ));
        }
        assert_eq!(ctx.requests().len(), 4);
    }
}

#[tokio::test]
async fn branch_paths_cannot_address_parent_resources() {
    let ctx = TestCatalog::new(vec![]).await;
    let id = Identifier::new("db", "t");
    for name in ["", " ", ".", ".."] {
        assert!(matches!(
            ctx.catalog.drop_branch(&id, name).await,
            Err(Error::ConfigInvalid { .. })
        ));
        assert!(matches!(
            ctx.catalog.fast_forward(&id, name).await,
            Err(Error::ConfigInvalid { .. })
        ));
    }
    for id in [Identifier::new("..", "t"), Identifier::new("db", ".")] {
        assert!(matches!(
            ctx.catalog.create_branch(&id, "dev", None).await,
            Err(Error::ConfigInvalid { .. })
        ));
        assert!(matches!(
            ctx.catalog.drop_branch(&id, "dev").await,
            Err(Error::ConfigInvalid { .. })
        ));
        assert!(matches!(
            ctx.catalog.fast_forward(&id, "dev").await,
            Err(Error::ConfigInvalid { .. })
        ));
        assert!(matches!(
            ctx.catalog.list_branches(&id).await,
            Err(Error::ConfigInvalid { .. })
        ));
    }
    assert!(ctx.requests().is_empty());
}
