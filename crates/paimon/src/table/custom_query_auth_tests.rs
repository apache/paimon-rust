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

//! Custom authorizers keep policy and table-binding enforcement in core.

use super::*;
use crate::api::AuthTableQueryResponse;
use crate::table::TableQueryAuth;
use crate::Error;
use std::sync::Mutex as StdMutex;

#[derive(Debug, Default)]
struct Authorizer {
    calls: StdMutex<Vec<Option<Vec<String>>>>,
    response: StdMutex<Option<AuthTableQueryResponse>>,
    denied: AtomicBool,
}

#[async_trait::async_trait]
impl TableQueryAuth for Authorizer {
    async fn auth(&self, select: Option<Vec<String>>) -> Result<Option<AuthTableQueryResponse>> {
        self.calls.lock().unwrap().push(select);
        if self.denied.load(Ordering::SeqCst) {
            return Err(Error::DataInvalid {
                message: "custom authorization denied".into(),
                source: None,
            });
        }
        Ok(self.response.lock().unwrap().clone())
    }
}

#[tokio::test]
async fn custom_query_auth_refreshes_policy_and_preserves_selection() {
    let fixture = RestTable::new(false, &[]).await;
    let auth = Arc::new(Authorizer::default());
    let table = fixture.table.with_query_auth(auth.clone()).unwrap();
    assert!(table.authorize_read(false, None).await.unwrap().is_none());
    assert!(auth.calls.lock().unwrap().is_empty());
    let grant = table
        .authorize_read(true, Some(vec!["value".into()]))
        .await
        .unwrap()
        .unwrap();
    assert!(grant.is_unrestricted());
    assert_eq!(grant.select(), Some(["value".to_string()].as_slice()));
    assert!(grant.matches_table(&table));
    assert!(!grant.matches_table(&fixture.table));
    let replacement = table.with_query_auth(auth.clone()).unwrap();
    assert!(!grant.matches_table(&replacement));

    *auth.response.lock().unwrap() = Some(
        serde_json::from_value(serde_json::json!({
            "filter": [serde_json::json!({
                "kind": "LEAF", "transform": {"name": "FIELD_REF", "fieldRef": {
                    "index": 0, "name": "id", "type": "INT"
                }}, "function": "EQUAL", "literals": [1]
            }).to_string()]
        }))
        .unwrap(),
    );
    let refreshed = table.authorize_read(true, None).await.unwrap().unwrap();
    assert!(!refreshed.is_unrestricted());
    assert_eq!(
        *auth.calls.lock().unwrap(),
        vec![Some(vec!["value".into()]), None]
    );
    // The standard endpoint is denied in this fixture; the custom hook replaces
    // transport rather than providing a fallback after authorization failure.
    assert_eq!(fixture.state.query_requests.load(Ordering::SeqCst), 0);
    assert!(fixture.table.authorize_read(true, None).await.is_err());
}

#[tokio::test]
async fn custom_query_auth_refuses_invalid_policy_and_errors_without_fallback() {
    let fixture = RestTable::new(false, &[]).await;
    let auth = Arc::new(Authorizer::default());
    let table = fixture.table.with_query_auth(auth.clone()).unwrap();
    *auth.response.lock().unwrap() = Some(AuthTableQueryResponse {
        filter: Some(vec!["".into()]),
        column_masking: None,
    });
    assert!(table.authorize_read(true, None).await.is_err());
    *auth.response.lock().unwrap() = Some(AuthTableQueryResponse {
        filter: None,
        column_masking: Some(HashMap::from([("missing".into(), "null".into())])),
    });
    assert!(table.authorize_read(true, None).await.is_err());
    auth.denied.store(true, Ordering::SeqCst);
    let error = table.authorize_read(true, None).await.unwrap_err();
    assert!(error.to_string().contains("custom authorization denied"));
    assert_eq!(fixture.state.query_requests.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn custom_query_auth_requires_loaded_identity_and_matching_branch() {
    let auth = Arc::new(Authorizer::default());
    assert!(crate::table::query_auth_table()
        .with_query_auth(auth.clone())
        .is_err());
    let fixture = RestTable::new(false, &[]).await;
    let table = fixture.table.with_query_auth(auth.clone()).unwrap();
    let mut mismatched = table.clone();
    mismatched.branch = "other".into();
    assert!(mismatched.authorize_read(true, None).await.is_err());
    let schema = crate::spec::Schema::builder()
        .column("other", DataType::Int(IntType::new()))
        .column("p", DataType::VarChar(VarCharType::string_type()))
        .column("value", DataType::Int(IntType::new()))
        .partition_keys(vec!["p"])
        .option("query-auth.enabled", "true")
        .build()
        .unwrap();
    let replaced = table
        .copy_with_resolved_schema(TableSchema::new(0, &schema), "main")
        .unwrap();
    assert!(replaced.authorize_read(true, None).await.is_err());
    assert!(auth.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn custom_query_auth_mutations_require_unrestricted_whole_table() {
    let fixture = RestTable::new(
        false,
        &[
            ("row-tracking.enabled", "true"),
            ("data-evolution.enabled", "true"),
            ("deletion-vectors.enabled", "true"),
        ],
    )
    .await;
    fixture.append("seed", &[(1, "a", 10)], false).await;
    let auth = Arc::new(Authorizer::default());
    let table = fixture.table.with_query_auth(auth.clone()).unwrap();
    let before = table
        .file_io()
        .list_status_recursive(table.location())
        .await
        .unwrap()
        .len();
    // An identity mask is still a restriction on a physical mutation.
    *auth.response.lock().unwrap() = Some(
        serde_json::from_value(serde_json::json!({
            "columnMasking": {"value": serde_json::json!({
                "name": "FIELD_REF", "fieldRef": {"index": 2, "name": "value", "type": "INT"}
            }).to_string()}
        }))
        .unwrap(),
    );
    let update = table.new_write_builder().new_update().unwrap();
    assert!(update.delete_by_row_id(vec![0]).await.is_err());
    assert_eq!(
        table
            .file_io()
            .list_status_recursive(table.location())
            .await
            .unwrap()
            .len(),
        before
    );
    assert!(auth.calls.lock().unwrap().iter().all(Option::is_none));
    *auth.response.lock().unwrap() = None;
    let messages = update.delete_by_row_id(vec![0]).await.unwrap();
    TableCommit::new(table, "custom-delete".into())
        .commit(messages)
        .await
        .unwrap();
    assert!(fixture.stored_rows().await.is_empty());
    assert_eq!(fixture.state.query_requests.load(Ordering::SeqCst), 0);
}

#[derive(Debug)]
struct EvolvingAuthorizer(Arc<AtomicBool>);

#[async_trait::async_trait]
impl TableQueryAuth for EvolvingAuthorizer {
    async fn auth(&self, _: Option<Vec<String>>) -> Result<Option<AuthTableQueryResponse>> {
        self.0.store(true, Ordering::SeqCst);
        Ok(None)
    }
}

#[tokio::test]
async fn custom_query_auth_revalidates_current_schema_after_callback() {
    let fixture = RestTable::new(false, &[]).await;
    let changed = Arc::new(AtomicBool::new(false));
    let response = fixture.state.response.clone();
    let state = changed.clone();
    let app = Router::new().route(
        "/v1/databases/db/tables/t",
        get(move || {
            let mut response = response.clone();
            let changed = state.clone();
            async move {
                if changed.load(Ordering::SeqCst) {
                    response["schemaId"] = serde_json::json!(1);
                }
                Json(response)
            }
        }),
    );
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
        serde_json::from_value(fixture.state.response.clone()).unwrap(),
        options,
    )
    .await
    .unwrap()
    .with_query_auth(Arc::new(EvolvingAuthorizer(changed.clone())))
    .unwrap();
    assert!(table.authorize_read(true, None).await.is_err());
    assert!(changed.load(Ordering::SeqCst));
    server.abort();
}

#[tokio::test]
async fn explicit_custom_query_auth_enforces_policy_when_default_factory_disabled() {
    let fixture = RestTable::new(false, &[]).await;
    let auth = Arc::new(Authorizer::default());
    auth.denied.store(true, Ordering::SeqCst);
    let ordinary = fixture.table.copy_with_options(HashMap::from([(
        "query-auth.enabled".into(),
        "false".into(),
    )]));
    let table = ordinary.with_query_auth(auth.clone()).unwrap();
    assert!(table.schema().core_options().query_auth_enabled());
    assert!(table.new_read_builder().new_scan().plan().await.is_err());
    assert_eq!(auth.calls.lock().unwrap().len(), 1);
    assert_eq!(fixture.state.query_requests.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn custom_query_auth_is_not_silently_dropped_or_reused_on_another_branch() {
    let fixture = RestTable::new(false, &[]).await;
    let auth = Arc::new(Authorizer::default());
    let table = fixture.table.with_query_auth(auth.clone()).unwrap();
    let copy = table.copy_with_branch("main").await.unwrap();
    assert_eq!(copy.query_auth_session(), table.query_auth_session());
    let error = table.copy_with_branch("other").await.unwrap_err();
    assert!(error.to_string().contains("custom query authorizer"));
    assert!(auth.calls.lock().unwrap().is_empty());
    assert_eq!(fixture.state.query_requests.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn custom_query_auth_survives_options_schema_and_snapshot_copies() {
    let fixture = RestTable::new(false, &[]).await;
    let auth = Arc::new(Authorizer::default());
    auth.denied.store(true, Ordering::SeqCst);
    let table = fixture.table.with_query_auth(auth.clone()).unwrap();
    let schema = table.schema().copy_with_replaced_options(HashMap::from([(
        "query-auth.enabled".into(),
        "false".into(),
    )]));
    let copies = [
        table.copy_with_options(HashMap::from([(
            "query-auth.enabled".into(),
            "false".into(),
        )])),
        table.copy_with_resolved_schema(schema, "main").unwrap(),
        table.copy_with_pinned_snapshot(None),
    ];
    for copy in copies {
        assert!(copy.schema().core_options().query_auth_enabled());
        let error = copy.authorize_read(true, None).await.unwrap_err();
        assert!(error.to_string().contains("custom authorization denied"));
    }
    assert_eq!(auth.calls.lock().unwrap().len(), 3);
    assert_eq!(fixture.state.query_requests.load(Ordering::SeqCst), 0);
}
