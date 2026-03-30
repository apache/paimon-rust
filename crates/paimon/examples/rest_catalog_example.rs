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

//! Example: REST Catalog Operations
//!
//! This example demonstrates how to use `RestCatalog` for database and table operations
//! via the Paimon REST catalog API.
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

use paimon::catalog::{Catalog, Identifier, RestCatalog};
use paimon::common::{CatalogOptions, Options};
use paimon::spec::{DataType, IntType, Schema, VarCharType};

/// Create a simple test schema with `id` (INT) and `name` (VARCHAR) columns.
fn create_test_schema() -> Schema {
    Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("name", DataType::VarChar(VarCharType::new(255).unwrap()))
        .build()
        .expect("Failed to build schema")
}

#[tokio::main]
async fn main() {
    // ==================== Configuration ====================
    let mut options = Options::new();

    // Basic configuration — replace with your actual server URL and warehouse
    options.set(CatalogOptions::METASTORE, "rest");
    options.set(CatalogOptions::WAREHOUSE, "pypaimon_catalog");
    options.set(
        CatalogOptions::URI,
        "http://sample.net/",
    );

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

    // ==================== Create RestCatalog ====================
    println!("Creating RestCatalog instance...");
    let catalog = match RestCatalog::new(options, true).await {
        Ok(catalog) => catalog,
        Err(err) => {
            eprintln!("Failed to create RestCatalog: {}", err);
            return;
        }
    };

    // ==================== Database Operations ====================
    println!("\n=== Database Operations ===\n");

    // List databases
    println!("Listing databases...");
    match catalog.list_databases().await {
        Ok(databases) => {
            println!("Databases found: {:?}", databases);
            println!("Total count: {}", databases.len());
        }
        Err(err) => {
            eprintln!("Failed to list databases: {}", err);
        }
    }

    // Create database
    println!("\nCreating database 'example_db'...");
    match catalog
        .create_database("example_db", false, HashMap::new())
        .await
    {
        Ok(()) => println!("Database created successfully"),
        Err(err) => eprintln!("Failed to create database: {}", err),
    }

    // Get database info
    println!("\nGetting database info for 'example_db'...");
    match catalog.get_database("example_db").await {
        Ok(database) => println!("Database: {:?}", database),
        Err(err) => eprintln!("Failed to get database: {}", err),
    }

    // ==================== Table Operations ====================
    println!("\n=== Table Operations ===\n");

    // Create table
    let table_identifier = Identifier::new("example_db", "users");
    println!("Creating table '{}'...", table_identifier);
    let schema = create_test_schema();
    match catalog
        .create_table(&table_identifier, schema, false)
        .await
    {
        Ok(()) => println!("Table created successfully"),
        Err(err) => eprintln!("Failed to create table: {}", err),
    }

    // List tables
    println!("\nListing tables in 'example_db'...");
    match catalog.list_tables("example_db").await {
        Ok(tables) => {
            println!("Tables found: {:?}", tables);
        }
        Err(err) => {
            eprintln!("Failed to list tables: {}", err);
        }
    }

    // Get table info
    println!("\nGetting table info for '{}'...", table_identifier);
    match catalog.get_table(&table_identifier).await {
        Ok(table) => println!("Table: {:?}", table),
        Err(err) => eprintln!("Failed to get table: {}", err),
    }

    // Rename table
    let renamed_identifier = Identifier::new("example_db", "users_renamed");
    println!(
        "\nRenaming table '{}' to '{}'...",
        table_identifier, renamed_identifier
    );
    match catalog
        .rename_table(&table_identifier, &renamed_identifier, false)
        .await
    {
        Ok(()) => println!("Table renamed successfully"),
        Err(err) => eprintln!("Failed to rename table: {}", err),
    }

    // Drop table
    println!("\nDropping table '{}'...", renamed_identifier);
    match catalog.drop_table(&renamed_identifier, false).await {
        Ok(()) => println!("Table dropped successfully"),
        Err(err) => eprintln!("Failed to drop table: {}", err),
    }

    // ==================== Cleanup ====================
    println!("\n=== Cleanup ===\n");

    // Drop database (cascade = true to force drop even if not empty)
    println!("Dropping database 'example_db'...");
    match catalog.drop_database("example_db", false, true).await {
        Ok(()) => println!("Database dropped successfully"),
        Err(err) => eprintln!("Failed to drop database: {}", err),
    }

    println!("\nExample completed!");
}
