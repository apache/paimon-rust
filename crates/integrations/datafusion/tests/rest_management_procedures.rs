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

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch};
use paimon::api::ConfigResponse;
use paimon::catalog::RESTCatalog;
use paimon::{CatalogOptions, Options};
use paimon_datafusion::SQLContext;

use mock_server::{start_mock_server, RESTServer};

const DATABASE: &str = "sales";
const TABLE: &str = "orders";
const WAREHOUSE: &str = "test_warehouse";

async fn setup() -> (tempfile::TempDir, RESTServer, SQLContext) {
    let temp_dir = tempfile::tempdir().unwrap();
    let server = start_mock_server(
        WAREHOUSE.to_string(),
        temp_dir.path().to_string_lossy().into_owned(),
        ConfigResponse::new(HashMap::from([(
            CatalogOptions::PREFIX.to_string(),
            "mock-test".to_string(),
        )])),
        vec![DATABASE.to_string()],
    )
    .await;
    server.add_table(DATABASE, TABLE);

    let mut options = Options::new();
    options.set(CatalogOptions::URI, server.url().unwrap());
    options.set(CatalogOptions::WAREHOUSE, WAREHOUSE);
    options.set(CatalogOptions::TOKEN_PROVIDER, "bear");
    options.set(CatalogOptions::TOKEN, "test-token");
    let catalog = Arc::new(RESTCatalog::new(options, true).await.unwrap());
    let mut context = SQLContext::new();
    context.register_catalog("paimon", catalog).await.unwrap();
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
