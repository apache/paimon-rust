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

//! `information_schema` queries whose plan spawns tasks, so the synchronous catalog callbacks
//! run on a worker of the process runtime. Driven like the Python binding, from a plain thread.

#[path = "../../../paimon/tests/mock_server.rs"]
mod mock_server;

use std::collections::HashMap;
use std::sync::{mpsc, Arc};
use std::time::Duration;

use arrow_array::RecordBatch;
use paimon::api::ConfigResponse;
use paimon::catalog::RESTCatalog;
use paimon::spec::{DataType, IntType, Schema};
use paimon::{CatalogOptions, Options};
use paimon_datafusion::SQLContext;

use mock_server::start_mock_server;

const WAREHOUSE: &str = "test_warehouse";
const DATABASES: [&str; 2] = ["db_a", "db_b"];

/// `(query, rows)`. Every table has two columns and every database has one table.
const QUERIES: [(&str, usize); 2] = [
    // With two or more target partitions the filter sits on a `RepartitionExec`.
    (
        "SELECT column_name FROM paimon.information_schema.columns WHERE table_schema = 'db_a'",
        2,
    ),
    // Two output partitions are merged by spawned tasks whatever the number of cores.
    (
        "SELECT column_name FROM paimon.information_schema.columns \
         UNION ALL SELECT column_name FROM paimon.information_schema.columns",
        8,
    ),
];

#[test]
fn information_schema_queries_finish_when_the_plan_spawns_tasks() {
    // The catalog service gets its own runtime, so the runtime under test cannot starve it.
    let server_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let server = server_runtime.block_on(start_mock_server(
        WAREHOUSE.to_string(),
        temp_dir.path().to_string_lossy().into_owned(),
        ConfigResponse::new(HashMap::from([(
            CatalogOptions::PREFIX.to_string(),
            "mock-test".to_string(),
        )])),
        DATABASES.iter().map(|name| name.to_string()).collect(),
    ));
    for database in DATABASES {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("v", DataType::Int(IntType::new()))
            .build()
            .unwrap();
        let path = format!("file://{}/{database}.db/t", temp_dir.path().display());
        server.add_table_with_schema(database, "t", schema, &path);
    }
    let url = server.url().unwrap();

    let (rows_tx, rows_rx) = mpsc::channel();
    // No runtime is entered on this thread, so `runtime()` is the process runtime.
    std::thread::spawn(move || {
        paimon_datafusion::runtime::runtime().block_on(async move {
            let mut options = Options::new();
            options.set(CatalogOptions::URI, url);
            options.set(CatalogOptions::WAREHOUSE, WAREHOUSE);
            options.set(CatalogOptions::TOKEN_PROVIDER, "bear");
            options.set(CatalogOptions::TOKEN, "test-token");
            let catalog = Arc::new(RESTCatalog::new(options, true).await.unwrap());
            let mut context = SQLContext::new();
            context.register_catalog("paimon", catalog).await.unwrap();

            for (query, _) in QUERIES {
                let batches = context.sql(query).await.unwrap().collect().await.unwrap();
                let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
                rows_tx.send(rows).unwrap();
            }
        });
    });

    for (query, expected_rows) in QUERIES {
        // A timer of the blocked runtime would never fire, so the deadline lives on this thread.
        let rows = rows_rx
            .recv_timeout(Duration::from_secs(60))
            .unwrap_or_else(|_| panic!("a catalog callback blocked a runtime worker: {query}"));
        assert_eq!(rows, expected_rows, "{query}");
    }
}
