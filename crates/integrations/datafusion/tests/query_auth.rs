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

//! Query-auth enforcement through the DataFusion provider: SQL over a REST
//! catalog table with `query-auth.enabled` must apply the per-user grant
//! (row filter + column masking) fetched at scan-plan time, and COUNT(*)
//! must not shortcut to unfiltered statistics.
//!
//! This is the same provider path the Python binding
//! (`pypaimon_rust.datafusion`) drives via FFI.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, Int64Array, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
use datafusion::arrow::record_batch::RecordBatch;
use paimon::api::{AuthTableQueryResponse, ConfigResponse};
use paimon::catalog::{Identifier, RESTCatalog};
use paimon::spec::{BigIntType, DataType, IntType, Schema, VarCharType};
use paimon::{Catalog, CatalogOptions, FileSystemCatalog, Options, Table};
use paimon_datafusion::SQLContext;

// Shared REST catalog mock (same source the paimon crate's integration tests
// use); only a subset of its helpers is exercised here.
#[allow(dead_code)]
#[path = "../../../paimon/tests/mock_server.rs"]
mod mock_server;
use mock_server::start_mock_server;

async fn write_batch(table: &Table, batch: RecordBatch, commit_user: &str) {
    let write_builder = table
        .new_write_builder()
        .with_commit_user(commit_user)
        .expect("valid commit user");
    let mut write = write_builder.new_write().expect("create writer");
    write.write_arrow_batch(&batch).await.expect("write batch");
    let messages = write.prepare_commit().await.expect("prepare commit");
    write_builder
        .new_commit()
        .commit(messages)
        .await
        .expect("commit batch");
}

// Multi-threaded: the provider's `block_on_with_runtime` bridges park the
// current thread, which must not be the only thread serving the mock server.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_query_auth_grant_enforced_via_sql() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = format!("file://{}", tmp.path().display());

    // Write demo_employees (id, name, salary) through a plain FileSystemCatalog.
    let mut fs_options = Options::new();
    fs_options.set(CatalogOptions::WAREHOUSE, &warehouse);
    let fs_catalog = FileSystemCatalog::new(fs_options).expect("create filesystem catalog");
    fs_catalog
        .create_database("default", true, HashMap::new())
        .await
        .unwrap();
    let columns = || {
        Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("name", DataType::VarChar(VarCharType::new(255).unwrap()))
            .column("salary", DataType::BigInt(BigIntType::new()))
            .option("bucket", "1")
            .option("bucket-key", "id")
    };
    let identifier = Identifier::new("default", "qa_emp");
    fs_catalog
        .create_table(&identifier, columns().build().unwrap(), false)
        .await
        .unwrap();
    let writer = fs_catalog.get_table(&identifier).await.unwrap();
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, true),
        ArrowField::new("name", ArrowDataType::Utf8, true),
        ArrowField::new("salary", ArrowDataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "alice", "bob", "charlie", "diana", "eve",
            ])),
            Arc::new(Int64Array::from(vec![120000, 85000, 95000, 70000, 99000])),
        ],
    )
    .unwrap();
    write_batch(&writer, batch, "u1").await;

    // Serve the same files through a mock REST catalog whose grant applies a
    // row filter (salary >= 90000) and masks name -> UPPER(name).
    let mut defaults = HashMap::new();
    defaults.insert("prefix".to_string(), "mock-test".to_string());
    let server = start_mock_server(
        "test_warehouse".to_string(),
        "/tmp/test_warehouse".to_string(),
        ConfigResponse::new(defaults),
        vec!["default".to_string()],
    )
    .await;
    server.add_table_with_schema(
        "default",
        "qa_emp",
        columns()
            .option("query-auth.enabled", "true")
            .build()
            .unwrap(),
        &format!("{warehouse}/default.db/qa_emp"),
    );
    server.set_auth_response(
        "default",
        "qa_emp",
        AuthTableQueryResponse {
            filter: Some(vec![
                r#"{"kind":"LEAF","transform":{"name":"FIELD_REF","fieldRef":{"index":2,"name":"salary","type":"BIGINT"}},"function":"GREATER_OR_EQUAL","literals":[90000]}"#
                    .to_string(),
            ]),
            column_masking: Some(HashMap::from([(
                "name".to_string(),
                r#"{"name":"UPPER","inputs":[{"index":1,"name":"name","type":"STRING"}]}"#
                    .to_string(),
            )])),
        },
    );

    let mut rest_options = Options::new();
    rest_options.set("uri", server.url().expect("server url"));
    rest_options.set("warehouse", "test_warehouse");
    rest_options.set("token.provider", "bear");
    rest_options.set("token", "test_token");
    let rest_catalog = RESTCatalog::new(rest_options, true)
        .await
        .expect("create REST catalog");

    // Sanity: the mock-served table must resolve through the Catalog trait.
    rest_catalog
        .get_table(&identifier)
        .await
        .expect("REST get_table(default.qa_emp)");

    let mut ctx = SQLContext::new();
    ctx.register_catalog("paimon", Arc::new(rest_catalog))
        .await
        .expect("register catalog");

    // Row filter + masking must both be applied on the SQL result.
    let batches = ctx
        .sql("SELECT id, name, salary FROM paimon.default.qa_emp ORDER BY id")
        .await
        .expect("plan select")
        .collect()
        .await
        .expect("execute select");
    let mut rows = Vec::new();
    for b in &batches {
        let ids = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        // DataFusion may hand back Utf8View for string columns; normalize.
        let names = cast(b.column(1), &ArrowDataType::Utf8).expect("cast name to Utf8");
        let names = names.as_any().downcast_ref::<StringArray>().unwrap();
        let sal = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        for r in 0..b.num_rows() {
            rows.push((ids.value(r), names.value(r).to_string(), sal.value(r)));
        }
    }
    assert_eq!(
        rows,
        vec![
            (1, "ALICE".to_string(), 120000),
            (3, "CHARLIE".to_string(), 95000),
            (5, "EVE".to_string(), 99000),
        ],
        "grant must drop salary<90000 rows and uppercase name"
    );

    // COUNT(*) must reflect the filtered row count, not unfiltered statistics
    // (the aggregate-statistics optimization must be disabled by the inexact
    // plan row counts).
    let batches = ctx
        .sql("SELECT COUNT(*) FROM paimon.default.qa_emp")
        .await
        .expect("plan count")
        .collect()
        .await
        .expect("execute count");
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 3, "COUNT(*) must not use unfiltered statistics");
}
