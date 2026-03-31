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

//! Example: REST Catalog — Read Append Table Data
//!
//! This example demonstrates how to use `RESTCatalog` to read data from an
//! append-only table with the following schema:
//!
//! | Column   | Type    |
//! |----------|---------|
//! | user_id  | INT     |
//! | item_id  | BIGINT  |
//! | behavior | VARCHAR |
//! | dt       | VARCHAR |
//!
//! Partition key: `dt`
//!
//! The table `default.test_t` must already exist and contain data on the
//! REST catalog server.
//!
//! # Usage
//! ```bash
//! # With DLF authentication:
//! DLF_ACCESS_KEY_ID=xxx DLF_ACCESS_KEY_SECRET=yyy \
//!   cargo run -p paimon --example rest_catalog_read_append_example
//!
//! # With Bearer token authentication:
//! PAIMON_REST_TOKEN=zzz \
//!   cargo run -p paimon --example rest_catalog_read_append_example
//! ```

use futures::TryStreamExt;

use paimon::catalog::{Catalog, Identifier, RESTCatalog};
use paimon::common::{CatalogOptions, Options};

#[tokio::main]
async fn main() {
    // ==================== Configuration ====================
    let mut options = Options::new();

    // Basic configuration — replace with your actual server URL and warehouse
    options.set(CatalogOptions::METASTORE, "rest");
    options.set(CatalogOptions::WAREHOUSE, "pypaimon_catalog");
    options.set(CatalogOptions::URI, "http://sample.net/");

    // --- Authentication (choose one) ---

    // Option A: DLF authentication (Alibaba Cloud)
    options.set(CatalogOptions::TOKEN_PROVIDER, "dlf");
    options.set("dlf.region", "cn-hangzhou");
    options.set(
        "dlf.access-key-id",
        std::env::var("DLF_ACCESS_KEY_ID").expect("DLF_ACCESS_KEY_ID env var not set"),
    );
    options.set(
        "dlf.access-key-secret",
        std::env::var("DLF_ACCESS_KEY_SECRET").expect("DLF_ACCESS_KEY_SECRET env var not set"),
    );

    // Option B: Bearer token authentication (uncomment to use)
    // options.set(CatalogOptions::TOKEN_PROVIDER, "bearer");
    // options.set("token", std::env::var("PAIMON_REST_TOKEN")
    //     .expect("PAIMON_REST_TOKEN env var not set"));

    // ==================== Create RESTCatalog ====================
    println!("Creating RESTCatalog instance...");
    let catalog = match RESTCatalog::new(options, true).await {
        Ok(catalog) => catalog,
        Err(err) => {
            eprintln!("Failed to create RESTCatalog: {}", err);
            return;
        }
    };

    // ==================== Get Table ====================
    let table_identifier = Identifier::new("default", "test_t");
    println!("Getting table '{}'...", table_identifier);

    let table = match catalog.get_table(&table_identifier).await {
        Ok(table) => {
            println!("Table retrieved successfully");
            println!("  Location: {}", table.location());
            println!("  Schema fields: {:?}", table.schema().fields());
            table
        }
        Err(err) => {
            eprintln!("Failed to get table: {}", err);
            return;
        }
    };

    // ==================== Scan Table ====================
    println!("\n=== Scanning Table ===\n");

    let read_builder = table.new_read_builder();
    let scan = read_builder.new_scan();

    let plan = match scan.plan().await {
        Ok(plan) => {
            println!("Scan plan created successfully");
            println!("  Number of splits: {}", plan.splits().len());
            plan
        }
        Err(err) => {
            eprintln!("Failed to plan scan: {}", err);
            return;
        }
    };

    if plan.splits().is_empty() {
        println!("No data splits found — the table may be empty.");
        return;
    }

    // ==================== Read Table Data ====================
    println!("\n=== Reading Table Data ===\n");

    let read = match read_builder.new_read() {
        Ok(read) => read,
        Err(err) => {
            eprintln!("Failed to create table read: {}", err);
            return;
        }
    };

    let stream = match read.to_arrow(plan.splits()) {
        Ok(stream) => stream,
        Err(err) => {
            eprintln!("Failed to create arrow stream: {}", err);
            return;
        }
    };

    let batches: Vec<_> = match stream.try_collect().await {
        Ok(batches) => batches,
        Err(err) => {
            eprintln!("Failed to collect record batches: {}", err);
            return;
        }
    };

    println!("Collected {} record batch(es)", batches.len());

    let mut total_rows = 0;
    for (batch_index, batch) in batches.iter().enumerate() {
        let num_rows = batch.num_rows();
        total_rows += num_rows;
        println!(
            "\n--- Batch {} ({} rows, {} columns) ---",
            batch_index,
            num_rows,
            batch.num_columns()
        );
        println!("Schema: {}", batch.schema());

        // Print up to 20 rows per batch for readability
        let display_rows = num_rows.min(20);
        for row in 0..display_rows {
            let mut row_values = Vec::new();
            for col in 0..batch.num_columns() {
                let column = batch.column(col);
                row_values.push(array_value_to_string(column, row));
            }
            println!("  Row {}: [{}]", row, row_values.join(", "));
        }
        if num_rows > display_rows {
            println!("  ... ({} more rows omitted)", num_rows - display_rows);
        }
    }

    println!("\n=== Summary ===");
    println!("Total rows read: {}", total_rows);
    println!("Total batches: {}", batches.len());
    println!("\nExample completed!");
}

/// Format a single cell value from an Arrow array at the given row index.
/// Supports INT (Int32), BIGINT (Int64), and VARCHAR (String/LargeString).
fn array_value_to_string(array: &dyn arrow_array::Array, row: usize) -> String {
    use arrow_array::*;

    if array.is_null(row) {
        return "null".to_string();
    }

    if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
        return arr.value(row).to_string();
    }

    format!("<unsupported type: {:?}>", array.data_type())
}
