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

#[path = "../../../paimon/tests/mock_server.rs"]
mod mock_server;

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{Array, RecordBatch};
use paimon::api::{ConfigResponse, CreateTagRequest};
use paimon::catalog::RESTCatalog;
use paimon::spec::{DataType, IntType, Schema};
use paimon::{CatalogOptions, Options};
use paimon_datafusion::SQLContext;

use mock_server::{start_mock_server, RESTServer};

const DATABASE: &str = "sales";
const TABLE: &str = "orders";
const WAREHOUSE: &str = "test_warehouse";

async fn start_server(warehouse: &Path) -> RESTServer {
    start_mock_server(
        WAREHOUSE.to_string(),
        warehouse.to_string_lossy().into_owned(),
        ConfigResponse::new(HashMap::from([(
            CatalogOptions::PREFIX.to_string(),
            "mock-test".to_string(),
        )])),
        vec![DATABASE.to_string()],
    )
    .await
}

async fn connect(server: &RESTServer) -> (Arc<RESTCatalog>, SQLContext) {
    let mut options = Options::new();
    options.set(CatalogOptions::URI, server.url().unwrap());
    options.set(CatalogOptions::WAREHOUSE, WAREHOUSE);
    options.set(CatalogOptions::TOKEN_PROVIDER, "bear");
    options.set(CatalogOptions::TOKEN, "test-token");
    let catalog = Arc::new(RESTCatalog::new(options, true).await.unwrap());
    let mut context = SQLContext::new();
    context
        .register_catalog("paimon", catalog.clone())
        .await
        .unwrap();
    (catalog, context)
}

async fn setup() -> (tempfile::TempDir, RESTServer, SQLContext) {
    let temp_dir = tempfile::tempdir().unwrap();
    let server = start_server(temp_dir.path()).await;
    server.add_table(DATABASE, TABLE);
    let (_, context) = connect(&server).await;
    (temp_dir, server, context)
}

async fn call(context: &SQLContext, sql: &str) -> Vec<RecordBatch> {
    context
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("planning '{sql}' failed: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("'{sql}' failed: {e}"))
}

async fn columns_of(context: &SQLContext, sql: &str) -> Vec<String> {
    context
        .sql(sql)
        .await
        .unwrap()
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}

fn rows(batches: &[RecordBatch]) -> Vec<Vec<Option<String>>> {
    batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(move |row| {
                (0..batch.num_columns())
                    .map(|column| {
                        let array = batch.column(column);
                        array
                            .is_valid(row)
                            .then(|| common::string_value(array.as_ref(), row).to_string())
                    })
                    .collect()
            })
        })
        .collect()
}

fn cell(rows: &[Vec<Option<String>>], row: usize, column: usize) -> Option<&str> {
    rows[row][column].as_deref()
}

async fn assert_ok_row(context: &SQLContext, sql: &str) {
    let batches = call(context, sql).await;
    assert_eq!(rows(&batches), vec![vec![Some("OK".to_string())]], "{sql}");
}

async fn grant_table_select(context: &SQLContext, principal: &str) {
    assert_ok_row(
        context,
        &format!(
            "CALL sys.grant_permission(resource_type => 'TABLE', access => 'SELECT', \
             principal => '{principal}', database => '{DATABASE}', table => '{TABLE}')"
        ),
    )
    .await;
}

async fn create_row_filter(context: &SQLContext, principal: &str, predicate: &str) {
    assert_ok_row(
        context,
        &format!(
            "CALL sys.create_policy(database => '{DATABASE}', table => '{TABLE}', \
             policy_type => 'ROW_FILTER', principal => '{principal}', \
             predicate_json => '{predicate}')"
        ),
    )
    .await;
}

#[tokio::test]
async fn test_grant_permission_sends_assignment() {
    let (_tmp, server, context) = setup().await;

    assert_ok_row(
        &context,
        &format!(
            "CALL sys.grant_permission(resource_type => 'TABLE', access => 'select', \
             principal => 'role:analyst', database => '{DATABASE}', table => '{TABLE}', \
             expire_time => '2028-01-01T00:00:00Z')"
        ),
    )
    .await;

    let bodies = server.grant_permission_bodies();
    assert_eq!(bodies.len(), 1);
    assert_eq!(
        bodies[0],
        serde_json::json!({
            "resource": {"type": "TABLE", "database": "sales", "table": "orders"},
            "access": "SELECT",
            "principal": "role:analyst",
            "expireTime": "2028-01-01T00:00:00Z",
        })
    );
    assert_eq!(server.permissions().len(), 1);
}

#[tokio::test]
async fn test_revoke_permission_sends_identity() {
    let (_tmp, server, context) = setup().await;
    grant_table_select(&context, "role:analyst").await;

    assert_ok_row(
        &context,
        &format!(
            "CALL sys.revoke_permission(resource_type => 'TABLE', access => 'SELECT', \
             principal => 'role:analyst', database => '{DATABASE}', table => '{TABLE}')"
        ),
    )
    .await;

    let bodies = server.revoke_permission_bodies();
    assert_eq!(bodies.len(), 1);
    assert_eq!(
        bodies[0],
        serde_json::json!({
            "resource": {"type": "TABLE", "database": "sales", "table": "orders"},
            "access": "SELECT",
            "principal": "role:analyst",
        })
    );
    assert!(server.permissions().is_empty());
}

#[tokio::test]
async fn test_list_permissions_columns_and_rows() {
    let (_tmp, server, context) = setup().await;
    assert_ok_row(
        &context,
        &format!(
            "CALL sys.grant_permission(resource_type => 'TABLE', access => 'SELECT', \
             principal => 'role:analyst', database => '{DATABASE}', table => '{TABLE}', \
             expire_time => '2028-01-01T00:00:00Z')"
        ),
    )
    .await;

    let sql = format!(
        "CALL sys.list_permissions(resource_type => 'TABLE', database => '{DATABASE}', \
         table => '{TABLE}', principal => 'role:analyst', access => 'SELECT')"
    );
    assert_eq!(
        columns_of(&context, &sql).await,
        vec![
            "resource_type",
            "database",
            "table",
            "function",
            "view",
            "access",
            "principal",
            "column_names",
            "excluded_column_names",
            "expire_time",
            "next_page_token",
        ]
    );

    let listed = rows(&call(&context, &sql).await);
    assert_eq!(
        listed,
        vec![vec![
            Some("TABLE".to_string()),
            Some("sales".to_string()),
            Some("orders".to_string()),
            None,
            None,
            Some("SELECT".to_string()),
            Some("role:analyst".to_string()),
            None,
            None,
            Some("2028-01-01T00:00:00Z".to_string()),
            None,
        ]]
    );

    let query = server.list_permissions_queries().pop().unwrap();
    assert_eq!(query.get("resourceType").unwrap(), "TABLE");
    assert_eq!(query.get("database").unwrap(), "sales");
    assert_eq!(query.get("table").unwrap(), "orders");
    assert_eq!(query.get("principal").unwrap(), "role:analyst");
    assert_eq!(query.get("access").unwrap(), "SELECT");
}

#[tokio::test]
async fn test_create_policy_row_filter_and_column_mask() {
    let (_tmp, server, context) = setup().await;

    create_row_filter(
        &context,
        "role:analyst",
        r#"{"kind":"LEAF","name":"region"}"#,
    )
    .await;
    assert_ok_row(
        &context,
        &format!(
            "CALL sys.create_policy(database => '{DATABASE}', table => '{TABLE}', \
             policy_type => 'COLUMN_MASKING', principal => 'role:support', \
             on_column => 'email', transform_json => '{{\"name\":\"CONCAT\"}}')"
        ),
    )
    .await;

    let bodies = server.create_policy_bodies();
    assert_eq!(bodies.len(), 2);
    assert_eq!(
        bodies[0],
        serde_json::json!({
            "rowFilter": {"predicate": r#"{"kind":"LEAF","name":"region"}"#},
            "principal": "role:analyst",
        })
    );
    assert_eq!(
        bodies[1],
        serde_json::json!({
            "columnMask": {"onColumn": "email", "transform": r#"{"name":"CONCAT"}"#},
            "principal": "role:support",
        })
    );
    assert_eq!(server.table_policies(DATABASE, TABLE).len(), 2);
}

#[tokio::test]
async fn test_drop_policy_sends_identity() {
    let (_tmp, server, context) = setup().await;
    create_row_filter(&context, "role:analyst", r#"{"kind":"LEAF"}"#).await;

    assert_ok_row(
        &context,
        &format!(
            "CALL sys.drop_policy(database => '{DATABASE}', table => '{TABLE}', \
             policy_type => 'ROW_FILTER', principal => 'role:analyst')"
        ),
    )
    .await;

    assert_eq!(
        server.drop_policy_bodies(),
        vec![serde_json::json!({"type": "ROW_FILTER", "principal": "role:analyst"})]
    );
    assert!(server.table_policies(DATABASE, TABLE).is_empty());
}

#[tokio::test]
async fn test_list_policies_columns_and_rows() {
    let (_tmp, server, context) = setup().await;
    create_row_filter(&context, "role:analyst", r#"{"kind":"LEAF"}"#).await;
    assert_ok_row(
        &context,
        &format!(
            "CALL sys.create_policy(database => '{DATABASE}', table => '{TABLE}', \
             policy_type => 'COLUMN_MASKING', principal => 'role:support', \
             on_column => 'email', transform_json => '{{\"name\":\"CONCAT\"}}')"
        ),
    )
    .await;

    let sql = format!("CALL sys.list_policies(database => '{DATABASE}', table => '{TABLE}')");
    assert_eq!(
        columns_of(&context, &sql).await,
        vec![
            "database",
            "table",
            "policy_type",
            "principal",
            "predicate_json",
            "on_column",
            "transform_json",
            "next_page_token",
        ]
    );

    let listed = rows(&call(&context, &sql).await);
    assert_eq!(listed.len(), 2);
    assert_eq!(
        listed[0],
        vec![
            Some("sales".to_string()),
            Some("orders".to_string()),
            Some("ROW_FILTER".to_string()),
            Some("role:analyst".to_string()),
            Some(r#"{"kind":"LEAF"}"#.to_string()),
            None,
            None,
            None,
        ]
    );
    assert_eq!(
        listed[1],
        vec![
            Some("sales".to_string()),
            Some("orders".to_string()),
            Some("COLUMN_MASKING".to_string()),
            Some("role:support".to_string()),
            None,
            Some("email".to_string()),
            Some(r#"{"name":"CONCAT"}"#.to_string()),
            None,
        ]
    );

    assert_eq!(
        server.list_policies_queries().pop().unwrap(),
        HashMap::new()
    );
}

#[tokio::test]
async fn test_list_policies_filters() {
    let (_tmp, server, context) = setup().await;
    create_row_filter(&context, "role:analyst", r#"{"kind":"LEAF"}"#).await;
    assert_ok_row(
        &context,
        &format!(
            "CALL sys.create_policy(database => '{DATABASE}', table => '{TABLE}', \
             policy_type => 'COLUMN_MASKING', principal => 'role:support', \
             on_column => 'email', transform_json => '{{\"name\":\"CONCAT\"}}')"
        ),
    )
    .await;

    let listed = rows(
        &call(
            &context,
            &format!(
                "CALL sys.list_policies(database => '{DATABASE}', table => '{TABLE}', \
             policy_type => 'COLUMN_MASKING', principal => 'role:support', column => 'email')"
            ),
        )
        .await,
    );
    assert_eq!(listed.len(), 1);
    assert_eq!(cell(&listed, 0, 3), Some("role:support"));

    let query = server.list_policies_queries().pop().unwrap();
    assert_eq!(query.get("type").unwrap(), "COLUMN_MASKING");
    assert_eq!(query.get("principal").unwrap(), "role:support");
    assert_eq!(query.get("column").unwrap(), "email");
}

#[tokio::test]
async fn test_list_permissions_pagination_repeats_token_on_every_row() {
    let (_tmp, server, context) = setup().await;
    for principal in ["role:a", "role:b", "role:c"] {
        grant_table_select(&context, principal).await;
    }

    let first = rows(
        &call(
            &context,
            &format!(
                "CALL sys.list_permissions(resource_type => 'TABLE', database => '{DATABASE}', \
             table => '{TABLE}', max_results => 2)"
            ),
        )
        .await,
    );
    assert_eq!(first.len(), 2);
    let token = cell(&first, 0, 10).expect("next_page_token").to_string();
    assert_eq!(cell(&first, 1, 10), Some(token.as_str()));
    assert_eq!(cell(&first, 0, 6), Some("role:a"));
    assert_eq!(cell(&first, 1, 6), Some("role:b"));

    let second = rows(
        &call(
            &context,
            &format!(
                "CALL sys.list_permissions(resource_type => 'TABLE', database => '{DATABASE}', \
             table => '{TABLE}', max_results => 2, page_token => '{token}')"
            ),
        )
        .await,
    );
    assert_eq!(second.len(), 1);
    assert_eq!(cell(&second, 0, 6), Some("role:c"));
    assert_eq!(cell(&second, 0, 10), None);

    let query = server.list_permissions_queries().pop().unwrap();
    assert_eq!(query.get("maxResults").unwrap(), "2");
    assert_eq!(query.get("pageToken").unwrap(), &token);

    let past_end = call(
        &context,
        &format!(
            "CALL sys.list_permissions(resource_type => 'TABLE', database => '{DATABASE}', \
             table => '{TABLE}', max_results => 2, page_token => '3')"
        ),
    )
    .await;
    assert_eq!(rows(&past_end), Vec::<Vec<Option<String>>>::new());
}

#[tokio::test]
async fn test_list_policies_pagination_repeats_token_on_every_row() {
    let (_tmp, _server, context) = setup().await;
    for principal in ["role:a", "role:b", "role:c"] {
        create_row_filter(&context, principal, r#"{"kind":"LEAF"}"#).await;
    }

    let first = rows(
        &call(
            &context,
            &format!(
            "CALL sys.list_policies(database => '{DATABASE}', table => '{TABLE}', max_results => 2)"
        ),
        )
        .await,
    );
    assert_eq!(first.len(), 2);
    let token = cell(&first, 0, 7).expect("next_page_token").to_string();
    assert_eq!(cell(&first, 1, 7), Some(token.as_str()));

    let second = rows(
        &call(
            &context,
            &format!(
                "CALL sys.list_policies(database => '{DATABASE}', table => '{TABLE}', \
             max_results => 2, page_token => '{token}')"
            ),
        )
        .await,
    );
    assert_eq!(second.len(), 1);
    assert_eq!(cell(&second, 0, 3), Some("role:c"));
    assert_eq!(cell(&second, 0, 7), None);
}

#[tokio::test]
async fn test_column_lists_are_comma_separated_and_java_trimmed() {
    let (_tmp, server, context) = setup().await;

    let empty = call(
        &context,
        &format!("CALL sys.list_policies(database => '{DATABASE}', table => '{TABLE}')"),
    )
    .await;
    assert_eq!(rows(&empty), Vec::<Vec<Option<String>>>::new());
    let list = format!(
        "CALL sys.list_permissions(resource_type => 'COLUMN', database => '{DATABASE}', \
         table => '{TABLE}')"
    );

    assert_ok_row(
        &context,
        &format!(
            "CALL sys.grant_permission(resource_type => 'COLUMN', access => 'SELECT', \
             principal => 'role:analyst', database => '{DATABASE}', table => '{TABLE}', \
             column_names => ' id , region ,\u{a0}')"
        ),
    )
    .await;
    assert_eq!(
        server.grant_permission_bodies()[0]["columns"],
        serde_json::json!({"columnNames": ["id", "region", "\u{a0}"]})
    );
    let listed = rows(&call(&context, &list).await);
    assert_eq!(cell(&listed, 0, 7), Some("id,region,\u{a0}"));
    assert_eq!(cell(&listed, 0, 8), None);

    assert_ok_row(
        &context,
        &format!(
            "CALL sys.grant_permission(resource_type => 'COLUMN', access => 'SELECT', \
             principal => 'role:analyst', database => '{DATABASE}', table => '{TABLE}', \
             excluded_column_names => 'email,\tphone ')"
        ),
    )
    .await;
    assert_eq!(
        server.grant_permission_bodies()[1]["columns"],
        serde_json::json!({"excludedColumnNames": ["email", "phone"]})
    );
    let listed = rows(&call(&context, &list).await);
    assert_eq!(listed.len(), 1);
    assert_eq!(cell(&listed, 0, 7), None);
    assert_eq!(cell(&listed, 0, 8), Some("email,phone"));
}

#[tokio::test]
async fn test_both_column_lists_rejected() {
    let (_tmp, _server, context) = setup().await;
    common::assert_sql_error(
        &context,
        &format!(
            "CALL sys.grant_permission(resource_type => 'COLUMN', access => 'SELECT', \
             principal => 'role:analyst', database => '{DATABASE}', table => '{TABLE}', \
             column_names => 'id', excluded_column_names => 'email')"
        ),
        "exactly one of column_names or excluded_column_names",
    )
    .await;
}

#[tokio::test]
async fn test_drop_policy_if_exists_swallows_missing_policy() {
    let (_tmp, _server, context) = setup().await;

    assert_ok_row(
        &context,
        &format!(
            "CALL sys.drop_policy(database => '{DATABASE}', table => '{TABLE}', \
             policy_type => 'ROW_FILTER', principal => 'role:ghost', if_exists => true)"
        ),
    )
    .await;

    common::assert_sql_error(
        &context,
        &format!(
            "CALL sys.drop_policy(database => '{DATABASE}', table => '{TABLE}', \
             policy_type => 'ROW_FILTER', principal => 'role:ghost', if_exists => false)"
        ),
        "Policy does not exist",
    )
    .await;

    common::assert_sql_error(
        &context,
        &format!(
            "CALL sys.drop_policy(database => '{DATABASE}', table => '{TABLE}', \
             policy_type => 'ROW_FILTER', principal => 'role:ghost')"
        ),
        "Policy does not exist",
    )
    .await;

    common::assert_sql_error(
        &context,
        &format!(
            "CALL sys.drop_policy(database => '{DATABASE}', table => '{TABLE}', \
             policy_type => 'ROW_FILTER', principal => 'role:ghost', if_exists => 'yes')"
        ),
        "Invalid if_exists 'yes'",
    )
    .await;
}

#[tokio::test]
async fn test_policy_type_rejects_the_other_type_fields() {
    let (_tmp, _server, context) = setup().await;

    common::assert_sql_error(
        &context,
        &format!(
            "CALL sys.create_policy(database => '{DATABASE}', table => '{TABLE}', \
             policy_type => 'ROW_FILTER', principal => 'p', predicate_json => '{{}}', \
             on_column => 'email')"
        ),
        "ROW_FILTER policy cannot specify on_column.",
    )
    .await;

    common::assert_sql_error(
        &context,
        &format!(
            "CALL sys.create_policy(database => '{DATABASE}', table => '{TABLE}', \
             policy_type => 'COLUMN_MASKING', principal => 'p', predicate_json => '{{}}', \
             on_column => 'email', transform_json => '{{}}')"
        ),
        "COLUMN_MASKING policy cannot specify predicate_json.",
    )
    .await;
}

#[tokio::test]
async fn test_non_rest_catalog_is_rejected_by_every_procedure() {
    let (_tmp, catalog) = common::create_test_env();
    let context = common::create_sql_context(catalog).await;

    for sql in [
        "CALL sys.grant_permission(resource_type => 'CATALOG', access => 'CREATEDATABASE', principal => 'admin')",
        "CALL sys.revoke_permission(resource_type => 'CATALOG', access => 'CREATEDATABASE', principal => 'admin')",
        "CALL sys.list_permissions(resource_type => 'CATALOG')",
        "CALL sys.create_policy(database => 'sales', table => 'orders', policy_type => 'ROW_FILTER', principal => 'p', predicate_json => '{}')",
        "CALL sys.drop_policy(database => 'sales', table => 'orders', policy_type => 'ROW_FILTER', principal => 'p')",
        "CALL sys.list_policies(database => 'sales', table => 'orders')",
    ] {
        common::assert_sql_error(
            &context,
            sql,
            "does not support permission or policy management",
        )
        .await;
    }
}

#[tokio::test]
async fn test_consumer_procedures_rejected_on_rest_catalog_without_changing_progress() {
    let (tmp, server, context) = setup().await;
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .build()
        .unwrap();
    let path = format!("file://{}/{DATABASE}.db/{TABLE}", tmp.path().display());
    server.add_table_with_schema(DATABASE, TABLE, schema, &path);
    let consumer_path = tmp
        .path()
        .join(format!("{DATABASE}.db/{TABLE}/consumer/consumer-job"));
    std::fs::create_dir_all(consumer_path.parent().unwrap()).unwrap();
    let original = br#"{"nextSnapshot":2}"#;
    std::fs::write(&consumer_path, original).unwrap();

    for snapshot in ["", ", next_snapshot_id => 1"] {
        common::assert_sql_error(
            &context,
            &format!("CALL sys.reset_consumer(table => '{DATABASE}.{TABLE}', consumer_id => 'job'{snapshot})"),
            "reset_consumer is not supported for tables managed by a REST catalog",
        ).await;
        assert_eq!(std::fs::read(&consumer_path).unwrap(), original);
    }
    common::assert_sql_error(
        &context,
        &format!("CALL sys.clear_consumers(table => '{DATABASE}.{TABLE}')"),
        "clear_consumers is not supported for tables managed by a REST catalog",
    )
    .await;
    assert_eq!(std::fs::read(&consumer_path).unwrap(), original);
}

fn register_rest_table(
    tmp: &tempfile::TempDir,
    server: &RESTServer,
    options: &[(&str, &str)],
) -> PathBuf {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .options(
            options
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string())),
        )
        .build()
        .unwrap();
    let root = tmp.path().join(format!("{DATABASE}.db/{TABLE}"));
    let schema_path = root.join("schema/schema-0");
    std::fs::create_dir_all(schema_path.parent().unwrap()).unwrap();
    std::fs::write(
        &schema_path,
        serde_json::to_vec(&paimon::spec::TableSchema::new(0, &schema)).unwrap(),
    )
    .unwrap();
    server.add_table_with_schema(
        DATABASE,
        TABLE,
        schema,
        &format!("file://{}", root.display()),
    );
    root
}

fn error_response(code: i32, message: &str) -> paimon::api::ErrorResponse {
    paimon::api::ErrorResponse::new(None, None, Some(message.to_string()), Some(code))
}

fn table_files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut files = BTreeMap::new();
    for entry in std::fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files.extend(table_files(&path));
        } else {
            let content = std::fs::read(&path).unwrap();
            files.insert(path, content);
        }
    }
    files
}

#[tokio::test]
async fn test_create_branch_goes_through_rest_catalog() {
    let (tmp, server, context) = setup().await;
    let root = register_rest_table(&tmp, &server, &[]);
    let storage_only = root.join("branch/branch-stale/schema/schema-0");
    std::fs::create_dir_all(storage_only.parent().unwrap()).unwrap();
    std::fs::write(&storage_only, b"stale").unwrap();
    let original_files = table_files(&root);
    let tag = " release +一";
    server.add_tag(DATABASE, TABLE, tag);
    let create =
        |args: &str| format!("CALL sys.create_branch(table => '{DATABASE}.{TABLE}', {args})");

    assert_ok_row(&context, &create("branch => 'dev'")).await;
    assert_ok_row(&context, &create("branch => 'stale'")).await;
    assert_ok_row(
        &context,
        &create("branch => 'fresh', ignore_if_exists => true"),
    )
    .await;
    for branch in [" dev", "dev ", "dev+", "一"] {
        assert_ok_row(
            &context,
            &create(&format!("branch => '{branch}', tag => '{tag}'")),
        )
        .await;
    }

    server.set_list_branches_error(Some(error_response(403, "denied")));
    for branch in ["dev", "new"] {
        common::assert_sql_error(
            &context,
            &create(&format!("branch => '{branch}', ignore_if_exists => true")),
            "Forbidden: denied",
        )
        .await;
    }
    assert_ok_row(&context, &create("branch => 'new'")).await;
    server.set_list_branches_error(None);
    let expected = ["dev", "stale", "fresh", " dev", "dev ", "dev+", "一", "new"];
    assert_eq!(server.branches(DATABASE, TABLE), expected);

    common::assert_sql_error(&context, &create("branch => 'dev'"), "already exists").await;
    for ignore in ["false", "true"] {
        common::assert_sql_error(
            &context,
            &create(&format!(
                "branch => 'other', tag => 'missing', ignore_if_exists => {ignore}"
            )),
            "Tag missing does not exist",
        )
        .await;
    }

    server.set_branch_mutation_error(Some(error_response(409, "conflict")));
    assert_ok_row(
        &context,
        &create("branch => 'raced', ignore_if_exists => true"),
    )
    .await;

    server.set_branch_mutation_error(Some(error_response(403, "denied")));
    for ignore in ["false", "true"] {
        common::assert_sql_error(
            &context,
            &create(&format!("branch => 'denied', ignore_if_exists => {ignore}")),
            "Forbidden: denied",
        )
        .await;
    }
    assert_ok_row(
        &context,
        &create("branch => 'dev', tag => 'missing', ignore_if_exists => true"),
    )
    .await;
    assert_eq!(server.branches(DATABASE, TABLE), expected);
    assert_eq!(table_files(&root), original_files);
}

#[tokio::test]
async fn test_delete_branch_goes_through_rest_catalog() {
    let (tmp, server, context) = setup().await;
    let root = register_rest_table(
        &tmp,
        &server,
        &[
            ("scan.primary-branch", "primary"),
            ("scan.fallback-branch", "fallback"),
        ],
    );
    for branch in ["dev", "qa", "keep", "primary", "fallback", "keep ", "keep+"] {
        server.add_branch(DATABASE, TABLE, branch);
    }
    let dev_dir = root.join("branch/branch-dev");
    std::fs::create_dir_all(&dev_dir).unwrap();
    std::fs::write(dev_dir.join("marker"), b"catalog-owned").unwrap();
    let original_files = table_files(&root);
    let delete = |branches: &str| {
        format!("CALL sys.delete_branch(table => '{DATABASE}.{TABLE}', branch => '{branches}')")
    };

    assert_ok_row(&context, &delete("dev,missing,,qa,dev,keep ,keep+")).await;
    assert_eq!(
        server.branches(DATABASE, TABLE),
        ["keep", "primary", "fallback"]
    );
    let expected_calls = ["dev", "missing", "qa", "dev", "keep ", "keep+"]
        .map(|branch| (DATABASE.to_string(), TABLE.to_string(), branch.to_string()));
    assert_eq!(server.drop_branch_calls(), expected_calls);

    for (branch, option) in [
        ("primary", "scan.primary-branch"),
        ("fallback", "scan.fallback-branch"),
    ] {
        common::assert_sql_error(&context, &delete(branch), option).await;
    }
    assert_eq!(server.drop_branch_calls(), expected_calls);

    server.set_branch_mutation_error(Some(error_response(403, "denied")));
    common::assert_sql_error(&context, &delete("keep,missing"), "Forbidden: denied").await;
    let mut expected_calls = expected_calls.to_vec();
    expected_calls.push((DATABASE.to_string(), TABLE.to_string(), "keep".to_string()));
    assert_eq!(server.drop_branch_calls(), expected_calls);
    assert_eq!(
        server.branches(DATABASE, TABLE),
        ["keep", "primary", "fallback"]
    );

    server.set_branch_mutation_error(None);
    common::assert_sql_error(
        &context,
        &delete("keep,primary,fallback"),
        "scan.primary-branch",
    )
    .await;
    assert_eq!(server.branches(DATABASE, TABLE), ["primary", "fallback"]);
    expected_calls.push((DATABASE.to_string(), TABLE.to_string(), "keep".to_string()));
    assert_eq!(server.drop_branch_calls(), expected_calls);
    assert_eq!(table_files(&root), original_files);
}

#[tokio::test]
async fn test_fast_forward_goes_through_rest_catalog() {
    let (tmp, server, context) = setup().await;
    let root = register_rest_table(&tmp, &server, &[]);
    let original_files = table_files(&root);
    let forward = |branch: &str| {
        format!("CALL sys.fast_forward(table => '{DATABASE}.{TABLE}', branch => '{branch}')")
    };

    let mut expected = Vec::new();
    for branch in ["dev", "dev +一", "forward"] {
        server.add_branch(DATABASE, TABLE, branch);
        assert_ok_row(&context, &forward(branch)).await;
        expected.push((DATABASE.to_string(), TABLE.to_string(), branch.to_string()));
    }
    assert_eq!(server.fast_forward_calls(), expected);

    common::assert_sql_error(
        &context,
        &forward("missing"),
        "Branch missing of table sales.orders does not exist",
    )
    .await;

    server.set_branch_mutation_error(Some(error_response(403, "denied")));
    common::assert_sql_error(&context, &forward("dev"), "Forbidden: denied").await;
    assert_eq!(server.fast_forward_calls(), expected);
    assert_eq!(table_files(&root), original_files);
}

#[tokio::test]
async fn test_branch_procedures_validate_names_on_rest_catalog() {
    let (tmp, server, context) = setup().await;
    let root = register_rest_table(&tmp, &server, &[]);
    let original_files = table_files(&root);
    server.add_branch(DATABASE, TABLE, "main");
    for procedure in ["create_branch", "delete_branch", "fast_forward"] {
        for (branch, error) in [
            ("main", "default branch"),
            (" ", "cannot be empty or whitespace"),
            ("123", "pure numeric"),
            ("..", "cannot be '.' or '..'"),
            ("dev/schema", "path separators"),
            ("dev\\schema", "path separators"),
            ("dev\n", "control characters"),
        ] {
            common::assert_sql_error(
                &context,
                &format!(
                    "CALL sys.{procedure}(table => '{DATABASE}.{TABLE}', branch => '{branch}')"
                ),
                error,
            )
            .await;
        }
    }
    common::assert_sql_error(
        &context,
        &format!("CALL sys.create_branch(table => '{DATABASE}.{TABLE}', branch => 'main', ignore_if_exists => true)"),
        "default branch",
    )
    .await;
    common::assert_sql_error(
        &context,
        &format!("CALL sys.create_branch(table => '{DATABASE}.{TABLE}', branch => 'dev$tags', ignore_if_exists => true)"),
        "cannot contain the SQL suffix delimiter",
    )
    .await;
    assert_eq!(server.branches(DATABASE, TABLE), ["main"]);
    assert!(server.drop_branch_calls().is_empty());
    assert!(server.fast_forward_calls().is_empty());
    assert_eq!(table_files(&root), original_files);
}

#[tokio::test]
async fn test_branch_procedures_reject_unsupported_rest_tables() {
    let (tmp, server, context) = setup().await;
    server.add_branch(DATABASE, TABLE, "dev");
    let procedure_sql = |procedure: &str, table: &str| {
        let extra = if procedure == "create_branch" {
            ", ignore_if_exists => true"
        } else {
            ""
        };
        format!("CALL sys.{procedure}(table => '{DATABASE}.{table}', branch => 'dev'{extra})")
    };

    for procedure in ["create_branch", "delete_branch", "fast_forward"] {
        common::assert_sql_error(
            &context,
            &procedure_sql(procedure, "missing"),
            "Table sales.missing does not exist",
        )
        .await;
    }

    let root = register_rest_table(&tmp, &server, &[]);
    let branch_table = format!("{TABLE}$branch_dev");
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .build()
        .unwrap();
    server.add_table_with_schema(
        DATABASE,
        &branch_table,
        schema,
        &format!("file://{}", root.display()),
    );
    for procedure in ["create_branch", "delete_branch", "fast_forward"] {
        common::assert_sql_error(
            &context,
            &procedure_sql(procedure, &branch_table),
            &format!("{procedure} requires a main-branch source table"),
        )
        .await;
    }

    for table_type in ["object-table", "lance-table", "iceberg-table"] {
        let root = register_rest_table(&tmp, &server, &[("type", table_type)]);
        let original_files = table_files(&root);
        for procedure in ["create_branch", "delete_branch", "fast_forward"] {
            common::assert_sql_error(
                &context,
                &procedure_sql(procedure, TABLE),
                &format!("is declared '{table_type}' and cannot be read as a Paimon table"),
            )
            .await;
        }
        assert_eq!(table_files(&root), original_files);
    }

    let root = register_rest_table(&tmp, &server, &[("type", "format-table")]);
    let original_files = table_files(&root);
    common::assert_sql_error(
        &context,
        &procedure_sql("create_branch", TABLE),
        "format-table is not supported",
    )
    .await;
    assert_eq!(table_files(&root), original_files);
    assert!(server.branches(DATABASE, "missing").is_empty());
    assert_eq!(server.branches(DATABASE, TABLE), ["dev"]);
    assert!(server.drop_branch_calls().is_empty());
    assert!(server.fast_forward_calls().is_empty());
}

#[tokio::test]
async fn test_rename_branch_rejected_on_rest_catalog() {
    // A REST catalog is authoritative for branch metadata and has no rename
    // endpoint, so `rename_branch` must refuse rather than move the physical
    // branch directory behind the catalog's back (Java `RESTCatalog.renameBranch`
    // throws `UnsupportedOperationException`). Without the guard the filesystem
    // `BranchManager` runs against the REST table's location and reports a
    // "branch ... does not exist" error instead, so this assertion is non-vacuous.
    let (tmp, server, context) = setup().await;

    // `get_table` needs a schema-bearing response; register one so resolution
    // reaches the REST guard rather than failing earlier on a missing schema.
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .build()
        .unwrap();
    let path = format!("file://{}/{DATABASE}.db/{TABLE}", tmp.path().display());
    server.add_table_with_schema(DATABASE, TABLE, schema, &path);

    common::assert_sql_error(
        &context,
        &format!(
            "CALL sys.rename_branch(table => '{DATABASE}.{TABLE}', from_branch => 'b1', to_branch => 'b2')"
        ),
        "REST catalog",
    )
    .await;
}

fn tag_names(server: &RESTServer) -> Vec<String> {
    server
        .tags(DATABASE, TABLE)
        .into_iter()
        .map(|tag| tag.tag_name)
        .collect()
}

#[tokio::test]
async fn test_create_tag_goes_through_rest_catalog() {
    let (tmp, server, context) = setup().await;
    let root = register_rest_table(&tmp, &server, &[]);
    let storage_only = root.join("tag/tag-stale");
    std::fs::create_dir_all(storage_only.parent().unwrap()).unwrap();
    std::fs::write(&storage_only, b"{").unwrap();
    let original_files = table_files(&root);
    let create = |args: &str| format!("CALL sys.create_tag(table => '{DATABASE}.{TABLE}', {args})");

    for args in [
        "tag => 'latest'",
        "tag => 'stale'",
        "tag => 'retained', time_retained => ' 12 HOURS '",
        "tag => ' keep +一', snapshot_id => 1, time_retained => '1d'",
    ] {
        let sql = create(args);
        assert_ok_row(&context, &sql).await;
        assert_eq!(table_files(&root), original_files, "{sql}");
    }
    let tags = server.tags(DATABASE, TABLE);
    let summary: Vec<_> = tags
        .iter()
        .map(|tag| {
            (
                tag.tag_name.as_str(),
                tag.snapshot.id(),
                tag.tag_time_retained.as_deref(),
            )
        })
        .collect();
    assert_eq!(
        summary,
        [
            (" keep +一", 1, Some("1d")),
            ("latest", 1, None),
            ("retained", 1, Some(" 12 HOURS ")),
            ("stale", 1, None),
        ]
    );

    for (args, message) in [
        (
            "tag => 'retained', snapshot_id => 1, time_retained => '2d'",
            "Tag 'retained' already exists",
        ),
        (
            "tag => 'other', snapshot_id => 2",
            "Snapshot 2 does not exist",
        ),
        ("tag => '.'", "tag name must not be a dot path segment"),
        ("tag => '..'", "tag name must not be a dot path segment"),
    ] {
        let sql = create(args);
        common::assert_sql_error(&context, &sql, message).await;
        assert_eq!(server.tags(DATABASE, TABLE), tags, "{sql}");
        assert_eq!(table_files(&root), original_files, "{sql}");
    }

    let request = |tag: &str, snapshot_id, retention: Option<&str>| {
        (
            DATABASE.to_string(),
            TABLE.to_string(),
            CreateTagRequest {
                tag_name: tag.to_string(),
                snapshot_id,
                time_retained: retention.map(str::to_string),
            },
        )
    };
    assert_eq!(
        server.create_tag_calls(),
        [
            request("latest", None, None),
            request("stale", None, None),
            request("retained", None, Some(" 12 HOURS ")),
            request(" keep +一", Some(1), Some("1d")),
            request("retained", Some(1), Some("2d")),
            request("other", Some(2), None),
        ]
    );
}

#[tokio::test]
async fn test_delete_tag_goes_through_rest_catalog() {
    let (tmp, server, context) = setup().await;
    let root = register_rest_table(&tmp, &server, &[]);
    for tag in ["v1", "v2", "keep", "a+b c"] {
        server.add_tag(DATABASE, TABLE, tag);
    }
    let tag_dir = root.join("tag");
    std::fs::create_dir_all(&tag_dir).unwrap();
    std::fs::write(tag_dir.join("tag-v1"), b"{").unwrap();
    std::fs::write(tag_dir.join("tag-storage_only"), b"{").unwrap();
    let original_files = table_files(&root);
    let delete =
        |tags: &str| format!("CALL sys.delete_tag(table => '{DATABASE}.{TABLE}', tag => '{tags}')");
    let call = |tag: &str| (DATABASE.to_string(), TABLE.to_string(), tag.to_string());

    assert_ok_row(&context, &delete("v1, missing,,a+b c ,v1")).await;
    assert_eq!(tag_names(&server), ["keep", "v2"]);
    let mut expected_calls: Vec<_> = ["v1", "missing", "a+b c", "v1"].map(call).to_vec();
    assert_eq!(server.delete_tag_calls(), expected_calls);
    assert_eq!(table_files(&root), original_files);

    assert_ok_row(&context, &delete("storage_only")).await;
    expected_calls.push(call("storage_only"));
    assert_eq!(server.delete_tag_calls(), expected_calls);
    assert_eq!(table_files(&root), original_files);

    assert_ok_row(&context, &delete(" , ,")).await;
    assert_eq!(server.delete_tag_calls(), expected_calls);
    assert_eq!(tag_names(&server), ["keep", "v2"]);
    assert_eq!(table_files(&root), original_files);

    common::assert_sql_error(
        &context,
        &delete("keep,..,v2"),
        "tag name must not be a dot path segment",
    )
    .await;
    expected_calls.push(call("keep"));
    assert_eq!(server.delete_tag_calls(), expected_calls);
    assert_eq!(tag_names(&server), ["v2"]);
    assert_eq!(table_files(&root), original_files);
}

async fn rest_table_with_two_snapshots() -> (
    tempfile::TempDir,
    RESTServer,
    Arc<RESTCatalog>,
    SQLContext,
    PathBuf,
) {
    use paimon::catalog::Identifier;
    use paimon::Catalog;

    let (tmp, fs_catalog) = common::create_test_env();
    let identifier = Identifier::new(DATABASE, TABLE);
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .build()
        .unwrap();
    fs_catalog
        .create_database(DATABASE, false, HashMap::new())
        .await
        .unwrap();
    fs_catalog
        .create_table(&identifier, schema.clone(), false)
        .await
        .unwrap();
    let writer = common::create_sql_context(fs_catalog.clone()).await;
    for id in [1, 2] {
        common::exec(
            &writer,
            &format!("INSERT INTO paimon.{DATABASE}.{TABLE} VALUES ({id})"),
        )
        .await;
    }
    let location = fs_catalog
        .get_table(&identifier)
        .await
        .unwrap()
        .location()
        .to_string();
    let root = tmp.path().join(format!("{DATABASE}.db/{TABLE}"));
    let server = start_server(tmp.path()).await;
    server.add_table_with_schema(DATABASE, TABLE, schema, &location);
    let (catalog, context) = connect(&server).await;
    (tmp, server, catalog, context, root)
}

fn write_storage_tag(root: &Path, tag: &str, snapshot_id: i64) {
    let tag_dir = root.join("tag");
    std::fs::create_dir_all(&tag_dir).unwrap();
    std::fs::copy(
        root.join(format!("snapshot/snapshot-{snapshot_id}")),
        tag_dir.join(format!("tag-{tag}")),
    )
    .unwrap();
}

fn id_rows(ids: &[&str]) -> Vec<Vec<Option<String>>> {
    ids.iter().map(|id| vec![Some(id.to_string())]).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_tag_reads_go_through_rest_catalog() {
    use paimon::catalog::Identifier;
    use paimon::Catalog;

    let (_tmp, server, catalog, context, root) = rest_table_with_two_snapshots().await;
    write_storage_tag(&root, "release", 2);
    write_storage_tag(&root, "storage_only", 1);
    let catalog_latest_path = root.join("snapshot/snapshot-2");
    let catalog_latest: paimon::spec::Snapshot =
        serde_json::from_slice(&std::fs::read(&catalog_latest_path).unwrap()).unwrap();
    server.set_catalog_snapshot(DATABASE, TABLE, catalog_latest.clone());
    std::fs::remove_file(catalog_latest_path).unwrap();

    let original_files = table_files(&root);
    for args in [
        "tag => 'release', snapshot_id => 1",
        "tag => 'catalog_only'",
        "tag => 'retained', snapshot_id => 1, time_retained => '1d'",
    ] {
        assert_ok_row(
            &context,
            &format!("CALL sys.create_tag(table => '{DATABASE}.{TABLE}', {args})"),
        )
        .await;
    }
    assert_eq!(table_files(&root), original_files);
    let catalog_only = server
        .tags(DATABASE, TABLE)
        .into_iter()
        .find(|tag| tag.tag_name == "catalog_only")
        .unwrap();
    assert_eq!(catalog_only.snapshot, catalog_latest);

    std::fs::remove_file(root.join("snapshot/snapshot-1")).unwrap();
    let original_files = table_files(&root);

    let tags_sql = format!(
        "SELECT tag_name, CAST(snapshot_id AS VARCHAR) AS snapshot, time_retained \
         FROM paimon.{DATABASE}.`{TABLE}$tags` ORDER BY tag_name"
    );
    let row = |tag: &str, snapshot: &str, retained: Option<&str>| {
        vec![
            Some(tag.to_string()),
            Some(snapshot.to_string()),
            retained.map(str::to_string),
        ]
    };
    assert_eq!(
        rows(&call(&context, &tags_sql).await),
        [
            row("catalog_only", "2", None),
            row("release", "1", None),
            row("retained", "1", Some("PT24H")),
        ]
    );

    let select_ids = |clause: &str| {
        format!(
            "SELECT CAST(id AS VARCHAR) AS id_text FROM paimon.{DATABASE}.{TABLE} \
             {clause} ORDER BY id_text"
        )
    };
    let ids = |version: &str| select_ids(&format!("VERSION AS OF '{version}'"));
    assert_eq!(
        rows(&call(&context, &ids("release")).await),
        id_rows(&["1"])
    );
    assert_eq!(
        rows(&call(&context, &ids("catalog_only")).await),
        id_rows(&["1", "2"])
    );
    common::assert_sql_error(&context, &ids("storage_only"), "storage_only").await;

    call(&context, "SET 'paimon.scan.tag-name' = 'release'").await;
    assert_eq!(
        rows(&call(&context, &select_ids("")).await),
        id_rows(&["1"])
    );
    call(&context, "RESET 'paimon.scan.tag-name'").await;

    let tag_manager = catalog
        .get_table(&Identifier::new(DATABASE, TABLE))
        .await
        .unwrap()
        .tag_manager();
    let snapshot = tag_manager.get("release").await.unwrap().unwrap();
    assert_eq!(snapshot.id(), 1);
    for result in [
        tag_manager.create("blocked", &snapshot).await,
        tag_manager.replace("release", &snapshot, None).await,
        tag_manager.rename("release", "blocked").await,
        tag_manager.delete("catalog_only").await,
    ] {
        let error = result.unwrap_err();
        assert!(
            matches!(error, paimon::Error::Unsupported { .. }),
            "{error}"
        );
    }
    assert_eq!(tag_names(&server), ["catalog_only", "release", "retained"]);
    assert_eq!(table_files(&root), original_files);

    assert_ok_row(
        &context,
        &format!("CALL sys.delete_tag(table => '{DATABASE}.{TABLE}', tag => 'release')"),
    )
    .await;
    assert_eq!(table_files(&root), original_files);
    common::assert_sql_error(&context, &ids("release"), "release").await;
    assert_eq!(
        rows(&call(&context, &tags_sql).await),
        [
            row("catalog_only", "2", None),
            row("retained", "1", Some("PT24H")),
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_snapshot_versions_resolve_without_rest_tag_endpoint() {
    use paimon::catalog::Identifier;
    use paimon::Catalog;

    let (_tmp, server, catalog, context, root) = rest_table_with_two_snapshots().await;
    for id in [1, 2] {
        let path = root.join(format!("snapshot/snapshot-{id}"));
        let mut snapshot: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        snapshot["watermark"] = serde_json::json!(id * 1000);
        std::fs::write(path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
    }
    for tag in ["2", "watermark-2000"] {
        assert_ok_row(
            &context,
            &format!(
                "CALL sys.create_tag(table => '{DATABASE}.{TABLE}', tag => '{tag}', snapshot_id => 1)"
            ),
        )
        .await;
    }
    let original_files = table_files(&root);
    let table = catalog
        .get_table(&Identifier::new(DATABASE, TABLE))
        .await
        .unwrap();
    for version in ["2", "watermark-2000"] {
        let calls_before = server.get_tag_calls().len();
        let view = table
            .copy_with_time_travel_strict(HashMap::from([(
                "scan.version".to_string(),
                version.to_string(),
            )]))
            .await
            .unwrap();
        assert_eq!(
            view.travel_snapshot().map(|snapshot| snapshot.id()),
            Some(1)
        );
        assert_eq!(
            &server.get_tag_calls()[calls_before..],
            &[(DATABASE.to_string(), TABLE.to_string(), version.to_string())]
        );
    }
    let versions = ["2", "'watermark-2000'"].map(|version| {
        format!(
            "SELECT CAST(id AS VARCHAR) AS id_text FROM paimon.{DATABASE}.{TABLE} \
             VERSION AS OF {version} ORDER BY id_text"
        )
    });

    for sql in &versions {
        assert_eq!(rows(&call(&context, sql).await), id_rows(&["1"]), "{sql}");
    }
    server.set_tag_read_error(Some(error_response(501, "tags unsupported")));
    for sql in &versions {
        assert_eq!(
            rows(&call(&context, sql).await),
            id_rows(&["1", "2"]),
            "{sql}"
        );
    }
    let error = table
        .copy_with_time_travel_strict(HashMap::from([(
            "scan.tag-name".to_string(),
            "2".to_string(),
        )]))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("tags unsupported"), "{error}");
    server.set_tag_read_error(Some(error_response(403, "tag access denied")));
    for sql in &versions {
        common::assert_sql_error(&context, sql, "tag access denied").await;
    }
    assert_eq!(table_files(&root), original_files);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_referenced_files_size_counts_rest_catalog_tags() {
    let (_tmp, server, _catalog, context, root) = rest_table_with_two_snapshots().await;
    let sql = format!(
        "SELECT CAST(manifest_file_count AS VARCHAR), CAST(manifest_file_size AS VARCHAR), \
         CAST(data_file_count AS VARCHAR), CAST(data_file_size AS VARCHAR), \
         CAST(index_file_count AS VARCHAR), CAST(index_file_size AS VARCHAR) \
         FROM paimon.{DATABASE}.`{TABLE}$referenced_files_size` WHERE source = 'total'"
    );
    let files = table_files(&root);
    let count_and_size = |dir: &str| {
        let sizes: Vec<_> = files
            .iter()
            .filter(|(path, _)| path.starts_with(root.join(dir)))
            .map(|(_, content)| content.len())
            .collect();
        (sizes.len(), sizes.iter().sum::<usize>())
    };
    let (manifest_count, manifest_size) = count_and_size("manifest");
    let (data_count, data_size) = count_and_size("bucket-0");
    assert_eq!((manifest_count, data_count), (6, 2));
    let expected = vec![[manifest_count, manifest_size, data_count, data_size, 0, 0]
        .map(|value| Some(value.to_string()))
        .to_vec()];
    assert_eq!(rows(&call(&context, &sql).await), expected);

    assert_ok_row(
        &context,
        &format!(
            "CALL sys.create_tag(table => '{DATABASE}.{TABLE}', tag => 'keep', snapshot_id => 1)"
        ),
    )
    .await;
    assert!(!root.join("tag/tag-keep").exists());
    std::fs::remove_file(root.join("snapshot/snapshot-1")).unwrap();
    let original_files = table_files(&root);
    assert_eq!(rows(&call(&context, &sql).await), expected);

    for (code, message) in [(403, "tag access denied"), (501, "tags unsupported")] {
        server.set_tag_read_error(Some(error_response(code, message)));
        common::assert_sql_error(&context, &sql, message).await;
        assert_eq!(tag_names(&server), ["keep"]);
        assert_eq!(table_files(&root), original_files);
    }
    server.set_tag_read_error(None);
    assert_eq!(rows(&call(&context, &sql).await), expected);
    assert_eq!(table_files(&root), original_files);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_referenced_files_size_counts_catalog_only_branch_tags() {
    use paimon::catalog::Identifier;
    use paimon::Catalog;

    let (_tmp, server, catalog, context, root) = rest_table_with_two_snapshots().await;
    let summary = |table: &str, columns: &str, filter: &str| {
        format!("SELECT {columns} FROM paimon.{DATABASE}.`{table}$referenced_files_size` {filter}")
    };
    let counts = "CAST(manifest_file_count AS VARCHAR), CAST(data_file_count AS VARCHAR)";
    let sql = summary(TABLE, counts, "WHERE source = 'total'");
    let before = rows(&call(&context, &sql).await);

    let branch_table = format!("{TABLE}$branch_dev");
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .build()
        .unwrap();
    server.add_table_with_schema(
        DATABASE,
        &branch_table,
        schema,
        &format!("file://{}", root.display()),
    );
    for name in ["dev", "main", "dev"] {
        server.add_branch(DATABASE, TABLE, name);
    }
    let identifier = Identifier::new(DATABASE, &branch_table);
    catalog
        .create_tag(&identifier, "keep", Some(1), false)
        .await
        .unwrap();
    let branch = catalog.get_table(&identifier).await.unwrap();
    assert_eq!(
        branch
            .tag_manager()
            .get("keep")
            .await
            .unwrap()
            .unwrap()
            .id(),
        1
    );
    assert!(!root.join("branch/branch-dev").exists());
    std::fs::remove_file(root.join("snapshot/snapshot-1")).unwrap();

    let stale_branch = root.join("branch/branch-stale/snapshot");
    std::fs::create_dir_all(&stale_branch).unwrap();
    std::fs::write(stale_branch.join("snapshot-1"), b"{").unwrap();
    let original_files = table_files(&root);
    let expected_scopes =
        ["branch:dev", "branch:main", "total"].map(|source| vec![Some(source.to_string())]);
    for table in [TABLE, branch_table.as_str()] {
        assert_eq!(
            rows(&call(&context, &summary(table, counts, "WHERE source = 'total'")).await),
            before
        );
        assert_eq!(
            rows(&call(&context, &summary(table, "source", "ORDER BY source")).await),
            expected_scopes
        );
    }
    for (code, message) in [(403, "branch access denied"), (501, "branches unsupported")] {
        server.set_list_branches_error(Some(error_response(code, message)));
        common::assert_sql_error(&context, &sql, message).await;
    }
    assert_eq!(table_files(&root), original_files);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_referenced_files_size_counts_catalog_only_current_snapshots() {
    let (_tmp, server, _catalog, context, root) = rest_table_with_two_snapshots().await;
    let sql = format!(
        "SELECT CAST(manifest_file_count AS VARCHAR), CAST(data_file_count AS VARCHAR) \
         FROM paimon.{DATABASE}.`{TABLE}$referenced_files_size` WHERE source = 'total'"
    );
    let before = rows(&call(&context, &sql).await);
    let snapshot = |id| -> paimon::spec::Snapshot {
        serde_json::from_slice(
            &std::fs::read(root.join(format!("snapshot/snapshot-{id}"))).unwrap(),
        )
        .unwrap()
    };
    let first = snapshot(1);
    let second = snapshot(2);
    server.set_catalog_snapshot(DATABASE, TABLE, second.clone());
    std::fs::remove_file(root.join("snapshot/snapshot-2")).unwrap();
    let original_files = table_files(&root);
    assert_eq!(rows(&call(&context, &sql).await), before);

    let branch_table = format!("{TABLE}$branch_dev");
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .build()
        .unwrap();
    server.add_table_with_schema(
        DATABASE,
        &branch_table,
        schema,
        &format!("file://{}", root.display()),
    );
    server.add_branch(DATABASE, TABLE, "dev");
    server.set_catalog_snapshot(DATABASE, TABLE, first);
    server.set_catalog_snapshot(DATABASE, &branch_table, second);
    assert!(!root.join("branch/branch-dev").exists());
    assert_eq!(rows(&call(&context, &sql).await), before);
    assert_eq!(table_files(&root), original_files);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_referenced_files_size_rejects_unaddressable_catalog_branches() {
    for (name, message) in [
        ("..", "branch name cannot be '.' or '..'"),
        ("../escape", "branch name cannot contain path separators"),
        ("dev$tags", "unaddressable branch name"),
    ] {
        let (tmp, server, context) = setup().await;
        let root = register_rest_table(&tmp, &server, &[]);
        server.add_branch(DATABASE, TABLE, name);
        let original_files = table_files(&root);
        let sql = format!("SELECT * FROM paimon.{DATABASE}.`{TABLE}$referenced_files_size`");
        common::assert_sql_error(&context, &sql, message).await;
        assert_eq!(table_files(&root), original_files);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_legacy_dot_segment_tags_on_rest_tables() {
    use paimon::catalog::Identifier;
    use paimon::Catalog;

    const DOT_SEGMENT: &str = "tag name must not be a dot path segment";
    let (_tmp, server, catalog, context, root) = rest_table_with_two_snapshots().await;
    write_storage_tag(&root, "..", 2);
    std::fs::write(root.join("tag/tag-."), b"{").unwrap();
    let original_files = table_files(&root);
    let tag_manager = catalog
        .get_table(&Identifier::new(DATABASE, TABLE))
        .await
        .unwrap()
        .tag_manager();
    let tags = |filter: &str| {
        format!(
            "SELECT tag_name, CAST(snapshot_id AS VARCHAR) AS snapshot \
             FROM paimon.{DATABASE}.`{TABLE}$tags` {filter} ORDER BY tag_name"
        )
    };
    let release = vec![Some("release".to_string()), Some("1".to_string())];

    for (catalog_tags, readable) in [
        (vec![], vec![]),
        (vec![".", "..", "release"], vec![release]),
    ] {
        for tag in &catalog_tags {
            server.add_tag(DATABASE, TABLE, tag);
        }
        assert_eq!(tag_manager.list_all_names().await.unwrap(), catalog_tags);
        let listed: Vec<_> = tag_manager
            .list_all()
            .await
            .unwrap()
            .into_iter()
            .map(|(name, snapshot)| vec![Some(name), Some(snapshot.id().to_string())])
            .collect();
        assert_eq!(listed, readable);
        assert_eq!(rows(&call(&context, &tags("")).await), readable);
        assert_eq!(
            rows(&call(&context, &tags("WHERE tag_name = 'release'")).await),
            readable
        );

        for name in [".", ".."] {
            for error in [
                tag_manager.tag_exists(name).await.unwrap_err(),
                tag_manager.get(name).await.unwrap_err(),
                tag_manager.get_with_metadata(name).await.unwrap_err(),
            ] {
                assert!(error.to_string().contains(DOT_SEGMENT), "{error}");
            }
            let sql = format!("SELECT * FROM paimon.{DATABASE}.{TABLE} VERSION AS OF '{name}'");
            common::assert_sql_error(&context, &sql, DOT_SEGMENT).await;
            call(&context, &format!("SET 'paimon.scan.tag-name' = '{name}'")).await;
            common::assert_sql_error(
                &context,
                &format!("SELECT * FROM paimon.{DATABASE}.{TABLE}"),
                DOT_SEGMENT,
            )
            .await;
            call(&context, "RESET 'paimon.scan.tag-name'").await;
        }
    }
    common::assert_sql_error(
        &context,
        &format!("SELECT * FROM paimon.{DATABASE}.`{TABLE}$referenced_files_size`"),
        DOT_SEGMENT,
    )
    .await;
    assert_eq!(table_files(&root), original_files);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_rest_tag_metadata_uses_local_timestamp_and_branch() {
    use arrow_array::{StringArray, TimestampMillisecondArray};
    use chrono::TimeZone;
    use paimon::catalog::Identifier;
    use paimon::Catalog;

    let (tmp, server, _) = setup().await;
    let root = register_rest_table(&tmp, &server, &[]);
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .build()
        .unwrap();
    let branch_table = format!("{TABLE}$branch_dev");
    server.add_table_with_schema(
        DATABASE,
        &branch_table,
        schema,
        &format!("file://{}", root.display()),
    );
    let (catalog, context) = connect(&server).await;
    for (table_name, only_tag, epoch_millis, retention, seconds) in [
        (TABLE, "main_only", 1_704_067_200_000, "PT24H", 86_400.0),
        (
            branch_table.as_str(),
            "branch_only",
            1_704_153_600_000,
            "PT2H",
            7_200.0,
        ),
    ] {
        server.add_tag(DATABASE, table_name, "release");
        server.add_tag(DATABASE, table_name, only_tag);
        server.set_tag_metadata(
            DATABASE,
            table_name,
            "release",
            Some(epoch_millis),
            Some(retention),
        );
        let table = catalog
            .get_table(&Identifier::new(DATABASE, table_name))
            .await
            .unwrap();
        let tag_manager = table.tag_manager();
        assert_eq!(
            tag_manager.list_all_names().await.unwrap(),
            [only_tag, "release"]
        );
        let offset = chrono::Local
            .timestamp_millis_opt(epoch_millis)
            .single()
            .unwrap()
            .offset()
            .local_minus_utc();
        let expected_time = epoch_millis + i64::from(offset) * 1_000;
        let (_, created, retained) = tag_manager
            .get_with_metadata("release")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(created, Some(expected_time));
        assert_eq!(retained, Some(seconds));
        assert_eq!(
            server
                .tags(DATABASE, table_name)
                .iter()
                .find(|tag| tag.tag_name == "release")
                .unwrap()
                .tag_create_time,
            Some(epoch_millis)
        );

        let batches = call(
            &context,
            &format!(
                "SELECT tag_name, create_time, time_retained FROM paimon.{DATABASE}.`{table_name}$tags` ORDER BY tag_name"
            ),
        ).await;
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2);
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), only_tag);
        assert_eq!(names.value(1), "release");
        let times = batch
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert!(times.is_null(0));
        assert_eq!(times.value(1), expected_time);
        let retentions = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(retentions.is_null(0));
        assert_eq!(retentions.value(1), retention);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_tag_catalog_errors_do_not_fall_back_to_storage() {
    let (tmp, server, context) = setup().await;
    let root = register_rest_table(&tmp, &server, &[]);
    server.add_tag(DATABASE, TABLE, "release");
    let tag_path = root.join("tag/tag-release");
    std::fs::create_dir_all(tag_path.parent().unwrap()).unwrap();
    std::fs::write(tag_path, b"{").unwrap();
    let original_files = table_files(&root);
    let reads = [
        format!("SELECT * FROM paimon.{DATABASE}.`{TABLE}$tags`"),
        format!("SELECT * FROM paimon.{DATABASE}.{TABLE} VERSION AS OF 'release'"),
    ];
    let snapshot_read = format!("SELECT * FROM paimon.{DATABASE}.{TABLE} VERSION AS OF 1");
    let mutations = [
        format!("CALL sys.create_tag(table => '{DATABASE}.{TABLE}', tag => 'new')"),
        format!("CALL sys.delete_tag(table => '{DATABASE}.{TABLE}', tag => 'release')"),
    ];

    for (code, message) in [
        (400, "invalid request"),
        (403, "denied"),
        (404, "Table sales.orders does not exist"),
        (501, "remote operation unsupported"),
    ] {
        let error = Some(error_response(code, message));
        server.set_tag_read_error(error.clone());
        for sql in &reads {
            common::assert_sql_error(&context, sql, message).await;
        }
        if code != 501 {
            common::assert_sql_error(&context, &snapshot_read, message).await;
        }
        server.set_tag_read_error(None);
        server.set_tag_mutation_error(error);
        for sql in &mutations {
            common::assert_sql_error(&context, sql, message).await;
        }
        server.set_tag_mutation_error(None);
        assert_eq!(tag_names(&server), ["release"]);
        assert_eq!(table_files(&root), original_files);
    }
    assert_eq!(server.create_tag_calls().len(), 4);
    assert_eq!(server.delete_tag_calls().len(), 4);
}

#[tokio::test]
async fn test_unsupported_tag_procedures_rejected_on_rest_catalog() {
    let (tmp, server, context) = setup().await;
    let root = register_rest_table(&tmp, &server, &[]);
    server.add_tag(DATABASE, TABLE, "v1");
    for file in ["tag/tag-v1", "snapshot/snapshot-1", "snapshot/LATEST"] {
        let path = root.join(file);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, b"{").unwrap();
    }
    let original_files = table_files(&root);
    let table = format!("{DATABASE}.{TABLE}");

    for (procedure, args) in [
        ("replace_tag", "tag => 'v1'"),
        ("replace_tag", "tag => 'v1', snapshot_id => 1"),
        ("rename_tag", "tag => 'v1', target_tag => 'v2'"),
        ("create_tag_from_timestamp", "tag => 'v2', timestamp => 0"),
        ("rollback_to_timestamp", "timestamp => 0"),
    ] {
        let sql = format!("CALL sys.{procedure}(table => '{table}', {args})");
        let error = context.sql(&sql).await.unwrap_err();
        assert!(
            matches!(error, datafusion::error::DataFusionError::NotImplemented(_)),
            "{sql}: {error}"
        );
        assert!(
            error.to_string().contains(&format!(
                "{procedure} is not supported for tables managed by a REST catalog"
            )),
            "{sql}: {error}"
        );
        assert_eq!(tag_names(&server), ["v1"], "{sql}");
        assert_eq!(table_files(&root), original_files, "{sql}");
    }
    assert!(server.create_tag_calls().is_empty());
    assert!(server.delete_tag_calls().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_snapshot_selection_procedures_reject_readable_rest_catalog_table() {
    use arrow_array::Int32Array;
    use axum::http::{Method, StatusCode, Uri};
    use axum::{Json, Router};
    use paimon::catalog::Identifier;
    use paimon::Catalog;
    use std::sync::Mutex;

    let (tmp, catalog) = common::create_test_env();
    let writer = common::create_sql_context(catalog.clone()).await;
    common::exec(&writer, "CREATE SCHEMA paimon.test_db").await;
    common::exec(&writer, "CREATE TABLE paimon.test_db.t1 (id INT)").await;
    let table_dir = tmp.path().join("test_db.db/t1");
    for id in [1, 2] {
        common::exec(
            &writer,
            &format!("INSERT INTO paimon.test_db.t1 VALUES ({id})"),
        )
        .await;
        let path = table_dir.join(format!("snapshot/snapshot-{id}"));
        let mut snapshot: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        snapshot["watermark"] = serde_json::json!(id * 1000);
        std::fs::write(path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
    }
    common::exec(
        &writer,
        "CALL sys.create_tag(table => 'test_db.t1', tag => 'keep', snapshot_id => 1, time_retained => '1d')",
    )
    .await;
    let table = catalog
        .get_table(&Identifier::new("test_db", "t1"))
        .await
        .unwrap();
    assert_eq!(table.schema().core_options().file_format(), "parquet");
    let snapshot = table.snapshot_manager().get_snapshot(2).await.unwrap();
    let snapshot_response = serde_json::json!({"snapshot": {"snapshot": snapshot}});
    let table_response = serde_json::json!({
        "id": "table-uuid", "database": "test_db", "name": "t1",
        "path": table.location(), "isExternal": false,
        "schemaId": table.schema().id(), "schema": table.schema(),
    });
    // Only the catalog serves snapshot 2; the physical snapshot directory is stale.
    std::fs::remove_file(table_dir.join("snapshot/snapshot-2")).unwrap();
    assert_eq!(table.snapshot_manager().list_all_ids().await.unwrap(), [1]);

    let requests = Arc::new(Mutex::new(Vec::new()));
    let handler_requests = requests.clone();
    let table_path = "/v1/test/databases/test_db/tables/t1";
    let snapshot_path = format!("{table_path}/snapshot");
    let app = Router::new().fallback(move |method: Method, uri: Uri| {
        handler_requests
            .lock()
            .unwrap()
            .push((method, uri.path().to_string()));
        let (status, body) = match uri.path() {
            "/v1/config" => (
                StatusCode::OK,
                serde_json::json!({"defaults": {"prefix": "test"}}),
            ),
            "/v1/test/databases/test_db" => (
                StatusCode::OK,
                serde_json::json!({"name": "test_db", "options": {}}),
            ),
            path if path == table_path => (StatusCode::OK, table_response.clone()),
            path if path == snapshot_path => (StatusCode::OK, snapshot_response.clone()),
            _ => (
                StatusCode::NOT_FOUND,
                serde_json::json!({"code": 404, "message": "Unexpected request"}),
            ),
        };
        async move { (status, Json(body)) }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut options = Options::new();
    options.set(
        CatalogOptions::URI,
        format!("http://{}", listener.local_addr().unwrap()),
    );
    options.set(CatalogOptions::WAREHOUSE, "test_warehouse");
    options.set(CatalogOptions::TOKEN_PROVIDER, "bear");
    options.set(CatalogOptions::TOKEN, "test-token");
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let catalog = Arc::new(RESTCatalog::new(options, true).await.unwrap());
    let mut context = SQLContext::new();
    context
        .register_catalog_with_default_db("paimon", catalog, None)
        .await
        .unwrap();

    let batches = context
        .sql("SELECT id FROM paimon.test_db.t1 ORDER BY id")
        .await
        .unwrap_or_else(|error| panic!("{error}; requests: {:?}", requests.lock().unwrap()))
        .collect()
        .await
        .unwrap();
    let ids: Vec<_> = batches
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
        .collect();
    assert_eq!(ids, [1, 2]);
    assert!(requests
        .lock()
        .unwrap()
        .iter()
        .any(|(_, path)| path.ends_with("/snapshot")));
    requests.lock().unwrap().clear();

    let original_files = table_files(&table_dir);
    for (procedure, args) in [
        (
            "create_tag_from_watermark",
            "tag => 'checkpoint', watermark => 2000",
        ),
        (
            "create_tag_from_watermark",
            "tag => 'older', watermark => 1000",
        ),
        (
            "create_tag_from_watermark",
            "tag => 'keep', watermark => 1000",
        ),
        (
            "create_tag_from_timestamp",
            "tag => 'checkpoint', timestamp => 0",
        ),
        ("rollback_to_timestamp", "timestamp => 9223372036854775807"),
    ] {
        let sql = format!("CALL sys.{procedure}(table => 'test_db.t1', {args})");
        let error = context.sql(&sql).await.unwrap_err();
        assert!(
            matches!(error, datafusion::error::DataFusionError::NotImplemented(_)),
            "{sql}: {error}"
        );
        assert!(
            error.to_string().contains(&format!(
                "{procedure} is not supported for tables managed by a REST catalog"
            )),
            "{sql}: {error}"
        );
        assert_eq!(table_files(&table_dir), original_files, "{sql}");
    }
    let requests = requests.lock().unwrap();
    assert!(!requests.is_empty());
    assert!(requests
        .iter()
        .all(|(method, path)| method == Method::GET && path == table_path));
    server.abort();
}

#[tokio::test]
async fn test_argument_validation_follows_java() {
    let (_tmp, server, context) = setup().await;

    let error = context
        .sql(&format!(
            "CALL sys.grant_permission(resource_type => 'TABLE', database => '{DATABASE}', \
             table => '{TABLE}', access => 'SELECT', principal => 'analyst', \
             expire_time => '\u{a0}')"
        ))
        .await
        .expect_err("a non-blank expire_time that is not a timestamp must be rejected")
        .to_string();
    assert!(error.contains("ISO-8601"), "{error}");

    context
        .sql(&format!(
            "CALL sys.grant_permission(resource_type => 'TABLE', database => '{DATABASE}', \
             table => '{TABLE}', access => 'SELECT', principal => 'analyst', \
             expire_time => '\u{0}')"
        ))
        .await
        .unwrap();
    let body = server.grant_permission_bodies().pop().unwrap();
    assert!(
        body.get("expireTime").is_none(),
        "a NUL-only expire_time is blank to Java and must not be sent: {body}"
    );

    let error = context
        .sql("CALL sys.list_permissions(resource_type => ' TABLE ')")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("Invalid resource_type"), "{error}");
    let error = context
        .sql("CALL sys.list_permissions(resource_type => '  ')")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("resource_type cannot be empty"), "{error}");

    for (sql, expected) in [
        (
            "CALL sys.grant_permission(resource_type => 'TABEL', access => 'SELECT', \
             principal => 'role:analyst')"
                .to_string(),
            "Invalid resource_type 'TABEL'. Expected one of [CATALOG, CATALOG_ALL, \
             DATABASE, DATABASE_ALL, TABLE, COLUMN, VIEW, FUNCTION].",
        ),
        (
            format!(
                "CALL sys.list_policies(database => '{DATABASE}', table => '{TABLE}', \
                 policy_type => 'ROW_FILTERS')"
            ),
            "Invalid policy_type 'ROW_FILTERS'. Expected one of [ROW_FILTER, COLUMN_MASKING].",
        ),
        (
            "CALL sys.list_permissions(database => 'sales')".to_string(),
            "Missing required argument: 'resource_type'",
        ),
    ] {
        common::assert_sql_error(&context, &sql, expected).await;
    }
}

#[tokio::test]
async fn test_unknown_and_duplicate_arguments_are_rejected() {
    let (_tmp, server, context) = setup().await;

    let grant = |extra: &str| {
        format!(
            "CALL sys.grant_permission(resource_type => 'TABLE', database => '{DATABASE}', \
             table => '{TABLE}', access => 'SELECT', principal => 'role:analyst', {extra})"
        )
    };

    common::assert_sql_error(
        &context,
        &grant("expiretime => '2030-01-01T00:00:00Z'"),
        "Argument expiretime is unknown.",
    )
    .await;

    common::assert_sql_error(
        &context,
        &grant("principal => 'role:other'"),
        "Procedure argument principal is duplicated.",
    )
    .await;

    common::assert_sql_error(
        &context,
        &format!(
            "CALL sys.list_policies(database => '{DATABASE}', table => '{TABLE}', \
             page_tokn => '1')"
        ),
        "Argument page_tokn is unknown.",
    )
    .await;

    assert!(server.grant_permission_bodies().is_empty());
    assert!(server.list_policies_queries().is_empty());
}

#[tokio::test]
async fn test_view_and_function_resources_round_trip() {
    let (_tmp, server, context) = setup().await;

    for (resource_type, locator, name) in [("VIEW", "view", "v1"), ("FUNCTION", "function", "f1")] {
        assert_ok_row(
            &context,
            &format!(
                "CALL sys.grant_permission(resource_type => '{resource_type}', \
                 database => '{DATABASE}', {locator} => '{name}', access => 'SELECT', \
                 principal => 'role:analyst')"
            ),
        )
        .await;

        let listed = rows(
            &call(
                &context,
                &format!(
                    "CALL sys.list_permissions(resource_type => '{resource_type}', \
                     database => '{DATABASE}', {locator} => '{name}')"
                ),
            )
            .await,
        );
        assert_eq!(cell(&listed, 0, 0), Some(resource_type));
        assert_eq!(cell(&listed, 0, 1), Some(DATABASE));
        assert_eq!(cell(&listed, 0, 2), None);
        // Column 3 is `function` and column 4 is `view`, in Java's order.
        let (function, view) = (cell(&listed, 0, 3), cell(&listed, 0, 4));
        if resource_type == "VIEW" {
            assert_eq!((function, view), (None, Some(name)));
        } else {
            assert_eq!((function, view), (Some(name), None));
        }
    }
    assert_eq!(server.grant_permission_bodies().len(), 2);
}

#[tokio::test]
async fn test_all_three_procedure_name_forms_resolve() {
    let (_tmp, server, context) = setup().await;

    for name in [
        "grant_permission",
        "sys.grant_permission",
        "paimon.sys.grant_permission",
    ] {
        assert_ok_row(
            &context,
            &format!(
                "CALL {name}(resource_type => 'TABLE', access => 'SELECT', \
                 principal => 'role:analyst', database => '{DATABASE}', table => '{TABLE}')"
            ),
        )
        .await;
    }
    assert_eq!(server.grant_permission_bodies().len(), 3);
    assert_eq!(server.permissions().len(), 1);
}
