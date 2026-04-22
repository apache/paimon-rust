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

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::prelude::SessionContext;
use paimon::{CatalogOptions, FileSystemCatalog, Options};
use paimon_datafusion::{
    DynamicOptions, PaimonCatalogProvider, PaimonRelationPlanner, PaimonSqlHandler,
};
use tempfile::TempDir;

use arrow_array::{Array, RecordBatch, UInt64Array};

pub fn create_test_env() -> (TempDir, Arc<FileSystemCatalog>) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let warehouse = format!("file://{}", temp_dir.path().display());
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    let catalog = FileSystemCatalog::new(options).expect("Failed to create catalog");
    (temp_dir, Arc::new(catalog))
}

pub fn create_handler(catalog: Arc<FileSystemCatalog>) -> PaimonSqlHandler {
    let dynamic_options: DynamicOptions = Arc::new(RwLock::new(HashMap::new()));
    let ctx = SessionContext::new();
    ctx.register_catalog(
        "paimon",
        Arc::new(PaimonCatalogProvider::new(
            catalog.clone(),
            dynamic_options.clone(),
        )),
    );
    ctx.register_relation_planner(Arc::new(PaimonRelationPlanner::new()))
        .expect("Failed to register relation planner");
    PaimonSqlHandler::new(ctx, catalog, "paimon", dynamic_options)
}

#[allow(dead_code)]
pub async fn setup_handler() -> (TempDir, PaimonSqlHandler) {
    let (tmp, catalog) = create_test_env();
    let handler = create_handler(catalog);
    handler
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA failed");
    (tmp, handler)
}

#[allow(dead_code)]
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

#[allow(dead_code)]
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

/// Execute SQL and collect results, discarding the output.
#[allow(dead_code)]
pub async fn exec(handler: &PaimonSqlHandler, s: &str) {
    handler.sql(s).await.unwrap().collect().await.unwrap();
}

/// Execute SQL on the raw DataFusion context (for non-Paimon source tables).
#[allow(dead_code)]
pub async fn ctx_exec(handler: &PaimonSqlHandler, s: &str) {
    handler.ctx().sql(s).await.unwrap().collect().await.unwrap();
}

/// Extract the count from a DML result (returns a single UInt64 column).
#[allow(dead_code)]
pub async fn dml_count(handler: &PaimonSqlHandler, sql_str: &str) -> u64 {
    let result = handler.sql(sql_str).await.unwrap().collect().await.unwrap();
    result[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0)
}

/// Collect (i32, i32, String) rows from batches, sorted by (col0, col1).
#[allow(dead_code)]
pub fn collect_int_int_str(batches: &[RecordBatch]) -> Vec<(i32, i32, String)> {
    let mut rows = Vec::new();
    for batch in batches {
        let col0 = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let col1 = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let col2 = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((col0.value(i), col1.value(i), col2.value(i).to_string()));
        }
    }
    rows.sort_by_key(|r| (r.0, r.1));
    rows
}

/// Collect (i32, String) rows from batches, sorted by col0.
#[allow(dead_code)]
pub fn collect_int_str(batches: &[RecordBatch]) -> Vec<(i32, String)> {
    let mut rows = Vec::new();
    for batch in batches {
        let col0 = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let col1 = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((col0.value(i), col1.value(i).to_string()));
        }
    }
    rows.sort_by_key(|r| r.0);
    rows
}

/// Collect (i32, i32, i32) rows from batches, sorted by (col2, col0).
#[allow(dead_code)]
pub fn collect_three_ints(batches: &[RecordBatch]) -> Vec<(i32, i32, i32)> {
    let mut rows = Vec::new();
    for batch in batches {
        let col0 = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let col1 = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let col2 = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((col0.value(i), col1.value(i), col2.value(i)));
        }
    }
    rows.sort_by_key(|r| (r.2, r.0));
    rows
}

/// Collect (i32, String, i32) rows from batches, sorted by col0.
#[allow(dead_code)]
pub fn collect_int_str_int(batches: &[RecordBatch]) -> Vec<(i32, String, i32)> {
    let mut rows = Vec::new();
    for batch in batches {
        let col0 = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let col1 = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let col2 = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((col0.value(i), col1.value(i).to_string(), col2.value(i)));
        }
    }
    rows.sort_by_key(|r| r.0);
    rows
}

/// Query a 3-column (i32, String, i32) table and return sorted rows.
#[allow(dead_code)]
pub async fn query_int_str_int(handler: &PaimonSqlHandler, sql: &str) -> Vec<(i32, String, i32)> {
    let batches = handler.sql(sql).await.unwrap().collect().await.unwrap();
    collect_int_str_int(&batches)
}
