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

//! Shared test helpers for DataFusion integration tests.

use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::prelude::SessionContext;
use paimon::{CatalogOptions, FileSystemCatalog, Options};
use paimon_datafusion::{PaimonCatalogProvider, PaimonRelationPlanner, PaimonSqlHandler};
use tempfile::TempDir;

pub fn create_test_env() -> (TempDir, Arc<FileSystemCatalog>) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let warehouse = format!("file://{}", temp_dir.path().display());
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    let catalog = FileSystemCatalog::new(options).expect("Failed to create catalog");
    (temp_dir, Arc::new(catalog))
}

pub fn create_handler(catalog: Arc<FileSystemCatalog>) -> PaimonSqlHandler {
    let ctx = SessionContext::new();
    ctx.register_catalog(
        "paimon",
        Arc::new(PaimonCatalogProvider::new(catalog.clone())),
    );
    ctx.register_relation_planner(Arc::new(PaimonRelationPlanner::new()))
        .expect("Failed to register relation planner");
    PaimonSqlHandler::new(ctx, catalog, "paimon")
}

pub async fn setup_handler() -> (TempDir, PaimonSqlHandler) {
    let (tmp, catalog) = create_test_env();
    let handler = create_handler(catalog);
    handler
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA failed");
    (tmp, handler)
}

pub async fn collect_id_name(handler: &PaimonSqlHandler, sql: &str) -> Vec<(i32, String)> {
    let batches = handler.sql(sql).await.unwrap().collect().await.unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("id column");
        let names = batch
            .column_by_name("name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("name column");
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i), names.value(i).to_string()));
        }
    }
    rows.sort_by_key(|(id, _)| *id);
    rows
}

pub async fn collect_id_value(handler: &PaimonSqlHandler, sql: &str) -> Vec<(i32, i32)> {
    let batches = handler.sql(sql).await.unwrap().collect().await.unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("id column");
        let vals = batch
            .column_by_name("value")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("value column");
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i), vals.value(i)));
        }
    }
    rows.sort_by_key(|(id, _)| *id);
    rows
}

#[allow(dead_code)]
pub async fn row_count(handler: &PaimonSqlHandler, sql: &str) -> usize {
    let batches = handler.sql(sql).await.unwrap().collect().await.unwrap();
    batches.iter().map(|b| b.num_rows()).sum()
}
