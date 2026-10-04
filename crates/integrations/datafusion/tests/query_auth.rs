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

//! SQL over a `query-auth.enabled` table whose REST server sets a row filter
//! for the current user.

mod common;

#[path = "../../../paimon/tests/mock_server.rs"]
mod mock_server;

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use paimon::api::{AuthTableQueryResponse, ConfigResponse};
use paimon::catalog::{Catalog, Identifier, RESTCatalog};
use paimon::spec::{DataType, IntType, Schema, VarCharType, VariantType};
use paimon::{CatalogOptions, FileSystemCatalog, Options};
use paimon_datafusion::SQLContext;
use serde_json::json;

use mock_server::{start_mock_server, RESTServer};

const TABLE: &str = "paimon.default.people";

fn schema(options: &[(&str, &str)]) -> Schema {
    let mut builder = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("name", DataType::VarChar(VarCharType::new(255).unwrap()))
        .column("secret", DataType::VarChar(VarCharType::new(255).unwrap()));
    for (key, value) in options {
        builder = builder.option(*key, *value);
    }
    builder.build().unwrap()
}

/// Ids 1..=10 written through a filesystem catalog, served by a mock REST
/// catalog that admits `id > 6` and applies `column_masking`.
async fn restricted_people(
    column_masking: Option<HashMap<String, String>>,
) -> (tempfile::TempDir, RESTServer, SQLContext) {
    let tmp = tempfile::tempdir().unwrap();
    let mut fs_options = Options::new();
    fs_options.set(
        CatalogOptions::WAREHOUSE,
        format!("file://{}", tmp.path().display()),
    );
    let fs_catalog = FileSystemCatalog::new(fs_options).unwrap();
    fs_catalog
        .create_database("default", true, HashMap::new())
        .await
        .unwrap();
    let identifier = Identifier::new("default", "people");
    fs_catalog
        .create_table(&identifier, schema(&[]), false)
        .await
        .unwrap();
    let table = fs_catalog.get_table(&identifier).await.unwrap();
    let names = [
        "alice", "bob", "carol", "dave", "erin", "frank", "grace", "heidi", "ivan", "judy",
    ];
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowDataType::Int32, true),
            Field::new("name", ArrowDataType::Utf8, true),
            Field::new("secret", ArrowDataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int32Array::from_iter_values(1..=10)),
            Arc::new(StringArray::from_iter_values(names)),
            Arc::new(StringArray::from_iter_values(
                names.iter().map(|n| format!("{n}-ssn")),
            )),
        ],
    )
    .unwrap();
    let write_builder = table.new_write_builder();
    let mut write = write_builder.new_write().unwrap();
    write.write_arrow_batch(&batch).await.unwrap();
    let messages = write.prepare_commit().await.unwrap();
    write_builder.new_commit().commit(messages).await.unwrap();

    let server = start_mock_server(
        "test_warehouse".to_string(),
        tmp.path().to_string_lossy().into_owned(),
        ConfigResponse::new(HashMap::from([(
            CatalogOptions::PREFIX.to_string(),
            "mock-test".to_string(),
        )])),
        vec!["default".to_string()],
    )
    .await;
    server.add_table_with_schema(
        "default",
        "people",
        schema(&[("query-auth.enabled", "true")]),
        table.location(),
    );
    server.set_auth_response(
        "default",
        "people",
        AuthTableQueryResponse {
            filter: Some(vec![json!({
                "kind": "LEAF",
                "transform": {
                    "name": "FIELD_REF",
                    "fieldRef": {"index": 0, "name": "id", "type": "INT"},
                },
                "function": "GREATER_THAN",
                "literals": [6],
            })
            .to_string()]),
            column_masking,
        },
    );

    let mut options = Options::new();
    options.set(CatalogOptions::URI, server.url().unwrap());
    options.set(CatalogOptions::WAREHOUSE, "test_warehouse");
    options.set(CatalogOptions::TOKEN_PROVIDER, "bear");
    options.set(CatalogOptions::TOKEN, "test-token");
    let catalog = Arc::new(RESTCatalog::new(options, true).await.unwrap());
    let mut context = SQLContext::new();
    context.register_catalog("paimon", catalog).await.unwrap();
    (tmp, server, context)
}

async fn query(context: &SQLContext, sql: &str) -> Vec<RecordBatch> {
    context.sql(sql).await.unwrap().collect().await.unwrap()
}

fn id_names(batches: &[RecordBatch]) -> Vec<(i32, String)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                ids.value(row),
                common::string_value(batch.column(1).as_ref(), row).to_string(),
            ));
        }
    }
    rows.sort();
    rows
}

/// The one value of a one-row, one-column result.
fn single(batches: &[RecordBatch]) -> Arc<dyn Array> {
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        1,
        "{batches:?}"
    );
    let batch = batches.iter().find(|b| b.num_rows() == 1).unwrap();
    assert_eq!(batch.num_columns(), 1);
    Arc::clone(batch.column(0))
}

fn count(batches: &[RecordBatch]) -> i64 {
    single(batches)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_query_auth_queries_see_only_the_rows_the_filter_admits() {
    let (_tmp, _server, context) = restricted_people(None).await;

    assert_eq!(
        id_names(&query(&context, &format!("SELECT id, name FROM {TABLE}")).await),
        vec![
            (7, "grace".to_string()),
            (8, "heidi".to_string()),
            (9, "ivan".to_string()),
            (10, "judy".to_string()),
        ]
    );
    // Manifest statistics would answer 10 and 1.
    assert_eq!(
        count(&query(&context, &format!("SELECT COUNT(*) FROM {TABLE}")).await),
        4
    );
    let min = single(&query(&context, &format!("SELECT MIN(id) FROM {TABLE}")).await);
    assert_eq!(
        min.as_any().downcast_ref::<Int32Array>().unwrap().value(0),
        7
    );
}

/// Under the decoder, `10 / (id - 3)` would divide by zero on a row the filter drops.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_query_auth_where_runs_on_the_admitted_rows_only() {
    let (_tmp, _server, context) = restricted_people(None).await;

    assert_eq!(
        id_names(
            &query(
                &context,
                &format!("SELECT id, name FROM {TABLE} WHERE 10 / (id - 3) > 0")
            )
            .await
        ),
        vec![
            (7, "grace".to_string()),
            (8, "heidi".to_string()),
            (9, "ivan".to_string()),
            (10, "judy".to_string()),
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_query_auth_explain_shows_the_restriction_but_not_the_files() {
    let (_tmp, _server, context) = restricted_people(None).await;

    let plan = datafusion::arrow::util::pretty::pretty_format_batches(
        &query(
            &context,
            &format!("EXPLAIN SELECT id, name FROM {TABLE} WHERE name = 'grace'"),
        )
        .await,
    )
    .unwrap()
    .to_string();
    assert!(plan.contains("query-auth=restricted"), "{plan}");
    assert!(!plan.contains("files="), "{plan}");
}

/// Rows `(1, {"x":"bad"})` and `(2, {"x":1.5})`, served under the rule `id > 1`.
async fn restricted_variants() -> (tempfile::TempDir, RESTServer, SQLContext) {
    let schema = |options: &[(&str, &str)]| {
        let mut builder = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("payload", DataType::Variant(VariantType::new()));
        for (key, value) in options {
            builder = builder.option(*key, *value);
        }
        builder.build().unwrap()
    };
    let tmp = tempfile::tempdir().unwrap();
    let mut fs_options = Options::new();
    fs_options.set(
        CatalogOptions::WAREHOUSE,
        format!("file://{}", tmp.path().display()),
    );
    let fs_catalog = Arc::new(FileSystemCatalog::new(fs_options).unwrap());
    fs_catalog
        .create_database("default", true, HashMap::new())
        .await
        .unwrap();
    let identifier = Identifier::new("default", "vguard");
    fs_catalog
        .create_table(&identifier, schema(&[]), false)
        .await
        .unwrap();
    let mut writer = SQLContext::new();
    writer
        .register_catalog("paimon", fs_catalog.clone())
        .await
        .unwrap();
    common::exec(
        &writer,
        r#"INSERT INTO paimon.default.vguard
           SELECT 1, parse_json('{"x":"bad"}') UNION ALL SELECT 2, parse_json('{"x":1.5}')"#,
    )
    .await;
    let location = fs_catalog
        .get_table(&identifier)
        .await
        .unwrap()
        .location()
        .to_string();

    let server = start_mock_server(
        "test_warehouse".to_string(),
        tmp.path().to_string_lossy().into_owned(),
        ConfigResponse::new(HashMap::from([(
            CatalogOptions::PREFIX.to_string(),
            "mock-test".to_string(),
        )])),
        vec!["default".to_string()],
    )
    .await;
    server.add_table_with_schema(
        "default",
        "vguard",
        schema(&[("query-auth.enabled", "true")]),
        &location,
    );
    server.set_auth_response(
        "default",
        "vguard",
        AuthTableQueryResponse {
            filter: Some(vec![json!({
                "kind": "LEAF",
                "transform": {
                    "name": "FIELD_REF",
                    "fieldRef": {"index": 0, "name": "id", "type": "INT"},
                },
                "function": "GREATER_THAN",
                "literals": [1],
            })
            .to_string()]),
            column_masking: None,
        },
    );
    let mut options = Options::new();
    options.set(CatalogOptions::URI, server.url().unwrap());
    options.set(CatalogOptions::WAREHOUSE, "test_warehouse");
    options.set(CatalogOptions::TOKEN_PROVIDER, "bear");
    options.set(CatalogOptions::TOKEN, "test-token");
    let catalog = Arc::new(RESTCatalog::new(options, true).await.unwrap());
    let mut context = SQLContext::new();
    context.register_catalog("paimon", catalog).await.unwrap();
    (tmp, server, context)
}

/// Pushed into the scan, `variant_get` would cast `{"x":"bad"}`, the row the rule drops.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_query_auth_variant_get_runs_on_the_admitted_rows_only() {
    let (_tmp, _server, context) = restricted_variants().await;

    let value = single(
        &query(
            &context,
            "SELECT variant_get(payload, '$.x', 'FLOAT') FROM paimon.default.vguard",
        )
        .await,
    );
    assert_eq!(
        datafusion::arrow::util::display::array_value_to_string(&value, 0).unwrap(),
        "1.5"
    );
}
