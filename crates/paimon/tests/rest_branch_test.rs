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

//! Java REST branch and version protocols, exercised through the public Catalog contract.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{header, HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::Router;
use paimon::api::RestError;
use paimon::catalog::{Catalog, Identifier, RESTCatalog};
use paimon::common::Options;
use paimon::Error;
use serde_json::{json, Value};

fn version_snapshot(id: i64) -> Value {
    json!({
        "version": 3, "id": id, "schemaId": 0,
        "baseManifestList": "base", "deltaManifestList": "delta",
        "baseManifestListSize": 321, "deltaManifestListSize": 0,
        "commitUser": "writer", "commitIdentifier": 1, "commitKind": "APPEND",
        "timeMillis": 123, "uuid": "snapshot-uuid", "totalRecordCount": 8,
    })
}

#[tokio::test]
async fn rollback_instants_and_optional_guards_match_java() {
    use paimon::table::Instant;
    for instant in [Instant::snapshot(7), Instant::tag("release /一")] {
        for from_snapshot in [None, Some(0), Some(9)] {
            for table_uuid in [None, Some("table-incarnation")] {
                let ctx = TestCatalog::new_raw(vec![(StatusCode::OK, "OK".to_string())]).await;
                let id = Identifier::new("db /一", "table$branch_dev /一");
                ctx.catalog
                    .rollback_to(&id, &instant, from_snapshot, table_uuid)
                    .await
                    .unwrap();
                let requests = ctx.requests();
                assert_eq!(requests.len(), 1);
                assert_eq!(requests[0].method, Method::POST);
                assert!(requests[0]
                    .uri
                    .ends_with("/tables/table%24branch_dev+%2F%E4%B8%80/rollback"));
                let mut expected = json!({"instant": instant});
                if let Some(from_snapshot) = from_snapshot {
                    expected["fromSnapshot"] = json!(from_snapshot);
                }
                if let Some(uuid) = table_uuid {
                    expected["tableId"] = json!(uuid);
                }
                assert_eq!(requests[0].body, Some(expected));
            }
        }
    }
}

#[tokio::test]
async fn rejected_rollback_preserves_guards_without_reload_or_retry() {
    use paimon::table::Instant;
    for status in [400, 403, 409, 429, 500, 501, 503] {
        let ctx = TestCatalog::new(vec![(
            StatusCode::from_u16(status).unwrap(),
            json!({"message": "rejected"}),
        )])
        .await;
        let id = Identifier::new("db", "t");
        let error = ctx
            .catalog
            .rollback_to(&id, &Instant::snapshot(1), Some(2), Some("old-table"))
            .await
            .unwrap_err();
        assert!(matches!(error, Error::RestApi { .. }), "{error:?}");
        let requests = ctx.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].body,
            Some(json!({
                "instant": {"type":"snapshot", "snapshotId":1},
                "fromSnapshot":2, "tableId":"old-table",
            }))
        );
    }
}

#[tokio::test]
async fn rollback_missing_targets_remain_distinct_from_missing_tables() {
    use paimon::table::Instant;
    for resource in ["SNAPSHOT", "TAG", "TABLE", "OTHER"] {
        let name = if resource == "SNAPSHOT" {
            "7"
        } else {
            "release"
        };
        let ctx = TestCatalog::new(vec![(
            StatusCode::NOT_FOUND,
            json!({
                "resourceType":resource, "resourceName":name, "message":"missing",
            }),
        )])
        .await;
        let error = ctx
            .catalog
            .rollback_to(
                &Identifier::new("db", "t"),
                &Instant::tag("release"),
                None,
                None,
            )
            .await
            .unwrap_err();
        match resource {
            "SNAPSHOT" => assert!(matches!(error, Error::SnapshotNotExist { snapshot_id: 7 })),
            "TAG" => {
                assert!(matches!(error, Error::TagNotExist { tag_name } if tag_name == "release"))
            }
            _ => {
                assert!(matches!(error, Error::TableNotExist { full_name } if full_name == "db.t"))
            }
        }
        assert_eq!(ctx.requests().len(), 1);
    }
}

#[tokio::test]
async fn missing_latest_snapshot_is_not_an_invalid_tag_request() {
    for name in [Some("null"), None] {
        let ctx = TestCatalog::new(vec![(
            StatusCode::NOT_FOUND,
            json!({"resourceType":"SNAPSHOT", "resourceName":name, "message":"empty table"}),
        )])
        .await;
        let error = ctx
            .catalog
            .create_tag(&Identifier::new("db", "t"), "release", None, false)
            .await
            .unwrap_err();
        assert!(matches!(error, Error::RestApi {
            source: RestError::NoSuchResource { resource_type, resource_name, .. }
        } if resource_type.as_deref() == Some("SNAPSHOT") && resource_name.as_deref() == name));
        assert_eq!(ctx.requests().len(), 1);
    }
}

#[tokio::test]
async fn tag_pages_preserve_query_encoding_order_duplicates_and_tokens() {
    let token = "next +/%一";
    let prefix = "release +/%一";
    let ctx = TestCatalog::new(vec![(
        StatusCode::OK,
        json!({
            "tags":["z","a","z","一"], "nextPageToken":token,
        }),
    )])
    .await;
    let page = ctx
        .catalog
        .list_tags_paged(
            &Identifier::new("db", "t$branch_dev /一"),
            Some(3),
            Some(token),
            Some(prefix),
        )
        .await
        .unwrap();
    assert_eq!(page.elements, vec!["z", "a", "z", "一"]);
    assert_eq!(page.next_page_token.as_deref(), Some(token));
    let requests = ctx.requests();
    assert_eq!(requests.len(), 1);
    let url = reqwest::Url::parse(&format!("http://server{}", requests[0].uri)).unwrap();
    let query: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
    assert_eq!(
        query,
        std::collections::HashMap::from([
            ("maxResults".to_string(), "3".to_string()),
            ("pageToken".to_string(), token.to_string()),
            ("tagNamePrefix".to_string(), prefix.to_string()),
        ])
    );
    assert!(url.path().ends_with("t%24branch_dev+%2F%E4%B8%80/tags"));
}

#[tokio::test]
async fn tag_page_nulls_and_empty_parameters_match_java() {
    for (body, expected_token) in [
        (json!({"tags":null,"nextPageToken":"discard"}), None),
        (json!({"nextPageToken":"discard"}), None),
        (json!({"tags":[],"nextPageToken":"keep"}), Some("keep")),
    ] {
        for max in [None, Some(0), Some(-1)] {
            let ctx = TestCatalog::new(vec![(StatusCode::OK, body.clone())]).await;
            let page = ctx
                .catalog
                .list_tags_paged(&Identifier::new("db", "t"), max, Some(""), Some(""))
                .await
                .unwrap();
            assert!(page.elements.is_empty());
            assert_eq!(page.next_page_token.as_deref(), expected_token);
            let requests = ctx.requests();
            assert!(
                requests[0].uri.ends_with("/tags?pageToken="),
                "{requests:?}"
            );
        }
    }
}

#[tokio::test]
async fn tag_page_errors_are_table_errors_and_keep_permissions() {
    for status in [StatusCode::NOT_FOUND, StatusCode::FORBIDDEN] {
        let ctx = TestCatalog::new(vec![(
            status,
            json!({"resourceType":"TABLE","message":"denied"}),
        )])
        .await;
        let error = ctx
            .catalog
            .list_tags_paged(&Identifier::new("db", "t"), None, None, None)
            .await
            .unwrap_err();
        if status == StatusCode::NOT_FOUND {
            assert!(matches!(error, Error::TableNotExist { .. }));
        } else {
            assert!(matches!(
                error,
                Error::RestApi {
                    source: RestError::Forbidden { .. }
                }
            ));
        }
        assert_eq!(ctx.requests().len(), 1);
    }
}

#[tokio::test]
async fn catalog_snapshot_keeps_all_statistics_and_snapshot_metadata() {
    let ctx = TestCatalog::new(vec![(
        StatusCode::OK,
        json!({"snapshot":{
            "snapshot": version_snapshot(7), "recordCount":0,"fileSizeInBytes":123,
            "fileCount":2,"lastFileCreationTime":456,
        }}),
    )])
    .await;
    let snapshot = ctx
        .catalog
        .load_snapshot(&Identifier::new("db", "t$branch_dev"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.snapshot.id(), 7);
    assert_eq!(snapshot.snapshot.uuid(), Some("snapshot-uuid"));
    assert_eq!(snapshot.snapshot.base_manifest_list_size(), Some(321));
    assert_eq!(snapshot.snapshot.delta_manifest_list_size(), Some(0));
    assert_eq!(snapshot.record_count, Some(0));
    assert_eq!(snapshot.file_size_in_bytes, Some(123));
    assert_eq!(snapshot.file_count, Some(2));
    assert_eq!(snapshot.last_file_creation_time, Some(456));
    assert_eq!(ctx.requests().len(), 1);
}

#[tokio::test]
async fn absent_catalog_snapshot_is_empty_but_missing_table_is_not() {
    for body in [Value::Null, json!({"snapshot":null}), json!({})] {
        let ctx = TestCatalog::new(vec![(StatusCode::OK, body)]).await;
        assert!(ctx
            .catalog
            .load_snapshot(&Identifier::new("db", "t"))
            .await
            .unwrap()
            .is_none());
        assert_eq!(ctx.requests().len(), 1);
    }
    let ctx = TestCatalog::new(vec![(
        StatusCode::NOT_FOUND,
        json!({"resourceType":"SNAPSHOT","message":"empty"}),
    )])
    .await;
    assert!(ctx
        .catalog
        .load_snapshot(&Identifier::new("db", "t"))
        .await
        .unwrap()
        .is_none());
    assert_eq!(ctx.requests().len(), 1);
    let ctx = TestCatalog::new(vec![(
        StatusCode::NOT_FOUND,
        json!({"resourceType":"TABLE","message":"missing"}),
    )])
    .await;
    assert!(matches!(
        ctx.catalog.load_snapshot(&Identifier::new("db", "t")).await,
        Err(Error::TableNotExist { .. })
    ));
    assert_eq!(ctx.requests().len(), 1);
}

#[tokio::test]
async fn tag_retention_and_metadata_are_opaque_and_mutations_ignore_success_bodies() {
    let name = "release /一";
    let ctx = TestCatalog::new_raw(vec![
        (StatusCode::OK, "created".to_string()),
        (
            StatusCode::OK,
            json!({"tagName":name,"snapshot":version_snapshot(7),
            "tagCreateTime":123,"tagTimeRetained":"2 d"})
            .to_string(),
        ),
        (StatusCode::NO_CONTENT, String::new()),
    ])
    .await;
    let id = Identifier::new("db", "t");
    ctx.catalog
        .create_tag_with_retention(&id, name, Some(7), Some("2 d"), false)
        .await
        .unwrap();
    let response = ctx.catalog.get_tag(&id, name).await.unwrap();
    assert_eq!(response.snapshot.id(), 7);
    let serialized = serde_json::to_value(&response.snapshot).unwrap();
    assert_eq!(serialized["baseManifestListSize"], 321);
    assert_eq!(serialized["deltaManifestListSize"], 0);
    assert_eq!(response.tag_create_time, Some(123));
    assert_eq!(response.tag_time_retained.as_deref(), Some("2 d"));
    ctx.catalog.delete_tag(&id, name, false).await.unwrap();
    let requests = ctx.requests();
    assert_eq!(
        requests[0].body,
        Some(json!({"tagName":name,"snapshotId":7,"timeRetained":"2 d"}))
    );
    assert!(requests[1].uri.ends_with("/tags/release+%2F%E4%B8%80"));
    assert_eq!(requests[2].method, Method::DELETE);
}

#[tokio::test]
async fn version_paths_reject_parent_resources_before_requests() {
    use paimon::table::Instant;
    let ctx = TestCatalog::new(vec![]).await;
    for id in [Identifier::new("..", "t"), Identifier::new("db", ".")] {
        assert!(ctx
            .catalog
            .rollback_to(&id, &Instant::snapshot(1), None, None)
            .await
            .is_err());
        assert!(ctx.catalog.load_snapshot(&id).await.is_err());
        assert!(ctx
            .catalog
            .list_tags_paged(&id, None, None, None)
            .await
            .is_err());
        assert!(ctx
            .catalog
            .create_tag(&id, "release", None, false)
            .await
            .is_err());
    }
    for name in ["", " ", ".", ".."] {
        assert!(ctx
            .catalog
            .get_tag(&Identifier::new("db", "t"), name)
            .await
            .is_err());
        assert!(ctx
            .catalog
            .delete_tag(&Identifier::new("db", "t"), name, false)
            .await
            .is_err());
    }
    assert!(ctx.requests().is_empty());
}

#[derive(Debug, PartialEq)]
struct Request {
    method: Method,
    uri: String,
    body: Option<Value>,
}

#[derive(Default)]
struct ServerState {
    responses: VecDeque<(StatusCode, String)>,
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
        Self::new_raw(
            responses
                .into_iter()
                .map(|(status, body)| (status, body.to_string()))
                .collect(),
        )
        .await
    }

    async fn new_raw(responses: Vec<(StatusCode, String)>) -> Self {
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
            } else if matches!(status.as_u16(), 429 | 503) {
                (status, [(header::RETRY_AFTER, "1")], value).into_response()
            } else {
                (status, value).into_response()
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
async fn branch_mutations_ignore_success_response_bodies() {
    let bodies = ["OK", "", "not JSON", "{}"];
    let ctx = TestCatalog::new_raw(
        bodies
            .into_iter()
            .flat_map(|body| vec![(StatusCode::OK, body.to_string()); 3])
            .collect(),
    )
    .await;
    let id = Identifier::new("db", "t");
    for _ in bodies {
        ctx.catalog.create_branch(&id, "dev", None).await.unwrap();
        ctx.catalog.drop_branch(&id, "dev").await.unwrap();
        ctx.catalog.fast_forward(&id, "dev").await.unwrap();
    }
    assert_eq!(ctx.requests().len(), bodies.len() * 3);
}

#[tokio::test]
async fn branch_listing_retries_only_retryable_statuses() {
    for status in [
        StatusCode::TOO_MANY_REQUESTS,
        StatusCode::SERVICE_UNAVAILABLE,
    ] {
        let ctx = TestCatalog::new(vec![
            (
                status,
                json!({"code": status.as_u16(), "message": "try again"}),
            ),
            (StatusCode::OK, json!({"branches": ["dev"]})),
        ])
        .await;
        assert_eq!(
            ctx.catalog
                .list_branches(&Identifier::new("db", "t"))
                .await
                .unwrap(),
            ["dev"]
        );
        assert_eq!(ctx.requests().len(), 2);
    }
    let ctx = TestCatalog::new(vec![(
        StatusCode::BAD_GATEWAY,
        json!({"code": 502, "message": "gateway failed"}),
    )])
    .await;
    assert!(matches!(
        ctx.catalog.list_branches(&Identifier::new("db", "t")).await,
        Err(Error::RestApi {
            source: RestError::Unexpected { code: 502, .. }
        })
    ));
    assert_eq!(ctx.requests().len(), 1);
}

#[tokio::test]
async fn branch_mutations_do_not_retry_temporary_failures() {
    let id = Identifier::new("db", "t");
    for status in [
        StatusCode::TOO_MANY_REQUESTS,
        StatusCode::SERVICE_UNAVAILABLE,
    ] {
        let ctx = TestCatalog::new(vec![
            (
                status,
                json!({"code": status.as_u16(), "message": "uncertain result"})
            );
            3
        ])
        .await;
        assert!(ctx.catalog.create_branch(&id, "dev", None).await.is_err());
        assert!(ctx.catalog.drop_branch(&id, "dev").await.is_err());
        assert!(ctx.catalog.fast_forward(&id, "dev").await.is_err());
        assert_eq!(ctx.requests().len(), 3);
    }
}

#[tokio::test]
async fn branch_listing_stops_after_five_retries() {
    let ctx = TestCatalog::new(vec![
        (
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"code": 503, "message": "unavailable"})
        );
        6
    ])
    .await;
    assert!(matches!(
        ctx.catalog.list_branches(&Identifier::new("db", "t")).await,
        Err(Error::RestApi {
            source: RestError::ServiceUnavailable { .. }
        })
    ));
    assert_eq!(ctx.requests().len(), 6);
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
