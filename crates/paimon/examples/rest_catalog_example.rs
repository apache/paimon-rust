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

//! Example: REST Catalog Operations (Complete)
//!
//! This example demonstrates how to use `CatalogFactory` to create a REST catalog and:
//! 1. Database operations (create, list, get, drop)
//! 2. Table operations (create, list, get, rename, drop)
//! 3. Data reading from append-only tables
//!
//! # Usage
//! ```bash
//! # With DLF authentication:
//! DLF_ACCESS_KEY_ID=xxx DLF_ACCESS_KEY_SECRET=yyy \
//!   cargo run -p paimon --example rest_catalog_example
//!
//! # With Bearer token authentication:
//! PAIMON_REST_TOKEN=zzz \
//!   cargo run -p paimon --example rest_catalog_example
//! ```

use std::collections::HashMap;

use futures::TryStreamExt;

use paimon::catalog::Identifier;
use paimon::common::{CatalogOptions, Options};
use paimon::spec::{DataType, IntType, Schema, VarCharType};
use paimon::CatalogFactory;

/// Create a simple test schema with `id` (INT) and `name` (VARCHAR) columns.
fn create_test_schema() -> Schema {
    Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("name", DataType::VarChar(VarCharType::new(255).unwrap()))
        .build()
        .expect("Failed to build schema")
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

#[tokio::main]
async fn main() {
    // ==================== Configuration ====================
    let mut options = Options::new();

    // Basic configuration — replace with your actual server URL and warehouse
    options.set(CatalogOptions::METASTORE, "rest");
    options.set(CatalogOptions::WAREHOUSE, "paimon_catalog");
    options.set(CatalogOptions::URI, "http://sample.net/");

    // --- Authentication (choose one) ---

    // DLF authentication (Alibaba Cloud)
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

    // ==================== Create RESTCatalog ====================
    println!("Creating RESTCatalog instance...");
    let catalog = match CatalogFactory::create(options).await {
        Ok(catalog) => catalog,
        Err(err) => {
            eprintln!("Failed to create RESTCatalog: {err}");
            return;
        }
    };

    // ==================== Part 1: Database Operations ====================
    println!("\n=== Part 1: Database Operations ===\n");

    // List databases
    println!("Listing databases...");
    match catalog.list_databases().await {
        Ok(databases) => {
            println!("Databases found: {databases:?}");
            println!("Total count: {}", databases.len());
        }
        Err(err) => {
            eprintln!("Failed to list databases: {err}");
        }
    }

    // Create database
    println!("\nCreating database 'example_db'...");
    match catalog
        .create_database("example_db", false, HashMap::new())
        .await
    {
        Ok(()) => println!("Database created successfully"),
        Err(err) => eprintln!("Failed to create database: {err}"),
    }

    // Get database info
    println!("\nGetting database info for 'example_db'...");
    match catalog.get_database("example_db").await {
        Ok(database) => println!("Database: {database:?}"),
        Err(err) => eprintln!("Failed to get database: {err}"),
    }

    // ==================== Part 2: Table Operations ====================
    println!("\n=== Part 2: Table Operations ===\n");

    // Create table
    let table_identifier = Identifier::new("example_db", "users");
    println!("Creating table '{table_identifier}'...");
    let schema = create_test_schema();
    match catalog.create_table(&table_identifier, schema, false).await {
        Ok(()) => println!("Table created successfully"),
        Err(err) => eprintln!("Failed to create table: {err}"),
    }

    // List tables
    println!("\nListing tables in 'example_db'...");
    match catalog.list_tables("example_db").await {
        Ok(tables) => {
            println!("Tables found: {tables:?}");
        }
        Err(err) => {
            eprintln!("Failed to list tables: {err}");
        }
    }

    // Get table info
    println!("\nGetting table info for '{table_identifier}'...");
    match catalog.get_table(&table_identifier).await {
        Ok(table) => {
            println!("Table location: {}", table.location());
            println!("Table schema fields: {:?}", table.schema().fields());
        }
        Err(err) => eprintln!("Failed to get table: {err}"),
    }

    // Rename table
    let renamed_identifier = Identifier::new("example_db", "users_renamed");
    println!("\nRenaming table '{table_identifier}' to '{renamed_identifier}'...");
    match catalog
        .rename_table(&table_identifier, &renamed_identifier, false)
        .await
    {
        Ok(()) => println!("Table renamed successfully"),
        Err(err) => eprintln!("Failed to rename table: {err}"),
    }

    // ==================== Part 3: Read Data from Existing Table ====================
    println!("\n=== Part 3: Read Data from Existing Table ===\n");

    // Try to read from an existing table (example_db.users_renamed)
    // This table must already exist on the REST catalog server
    let read_table_identifier = Identifier::new("example_db", "users_renamed");
    println!("Attempting to read from table '{read_table_identifier}'...");

    match catalog.get_table(&read_table_identifier).await {
        Ok(table) => {
            println!("Table retrieved successfully");
            println!("  Location: {}", table.location());
            println!("  Schema fields: {:?}", table.schema().fields());

            // Scan table
            println!("\nScanning table...");
            let read_builder = table.new_read_builder();
            let scan = read_builder.new_scan();

            match scan.plan().await {
                Ok(plan) => {
                    println!("  Number of splits: {}", plan.splits().len());

                    if plan.splits().is_empty() {
                        println!("No data splits found — the table may be empty.");
                    } else {
                        // Read table data
                        println!("\nReading table data...");
                        match read_builder.new_read() {
                            Ok(read) => match read.to_arrow(plan.splits()) {
                                Ok(stream) => {
                                    let batches: Vec<_> =
                                        stream.try_collect().await.unwrap_or_default();
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

                                        // Print up to 10 rows per batch
                                        let display_rows = num_rows.min(10);
                                        for row in 0..display_rows {
                                            let mut row_values = Vec::new();
                                            for col in 0..batch.num_columns() {
                                                let column = batch.column(col);
                                                row_values.push(array_value_to_string(column, row));
                                            }
                                            println!("  Row {}: [{}]", row, row_values.join(", "));
                                        }
                                        if num_rows > display_rows {
                                            println!(
                                                "  ... ({} more rows omitted)",
                                                num_rows - display_rows
                                            );
                                        }
                                    }

                                    println!("\n=== Read Summary ===");
                                    println!("Total rows read: {total_rows}");
                                    println!("Total batches: {}", batches.len());
                                }
                                Err(err) => {
                                    eprintln!("Failed to create arrow stream: {err}");
                                }
                            },
                            Err(err) => {
                                eprintln!("Failed to create table read: {err}");
                            }
                        }
                    }
                }
                Err(err) => {
                    eprintln!("Failed to plan scan: {err}");
                }
            }
        }
        Err(err) => {
            eprintln!(
                "Failed to get table '{read_table_identifier}' (this is expected if the table doesn't exist): {err}"
            );
        }
    }

    // ==================== Cleanup ====================
    println!("\n=== Cleanup ===\n");
    // Drop table
    println!("\nDropping table '{renamed_identifier}'...");
    match catalog.drop_table(&renamed_identifier, false).await {
        Ok(()) => println!("Table dropped successfully"),
        Err(err) => eprintln!("Failed to drop table: {err}"),
    }
    // Drop database (cascade = true to force drop even if not empty)
    println!("Dropping database 'example_db'...");
    match catalog.drop_database("example_db", false, true).await {
        Ok(()) => println!("Database dropped successfully"),
        Err(err) => eprintln!("Failed to drop database: {err}"),
    }

    println!("\nExample completed!");
}
