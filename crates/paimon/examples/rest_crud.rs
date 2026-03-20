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

//! Example: REST API Operations
//!
//! This example demonstrates how to use RESTApi for database and table operations.
//!
//! # Usage
//! ```bash
//! cargo run -p paimon --example rest_api_example
//! ```

use std::collections::HashMap;

use paimon::api::rest_api::RESTApi;
use paimon::common::{CatalogOptions, Options};
use paimon::spec::{DataType, IntType, Schema, VarCharType};

/// Create a simple test schema.
fn create_test_schema() -> Schema {
    Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("name", DataType::VarChar(VarCharType::new(255).unwrap()))
        .build()
        .expect("Failed to build schema")
}

#[tokio::main]
async fn main() {
    // Create configuration options
    let mut options = Options::new();

    // Basic configuration - replace with your actual server URL
    options.set(CatalogOptions::METASTORE, "rest");
    options.set(CatalogOptions::WAREHOUSE, "your-catalog-name");
    options.set(CatalogOptions::URI, "your-rest-api-server-url");

    // DLF authentication (reads from environment variables)
    options.set(CatalogOptions::TOKEN_PROVIDER, "dlf");
    options.set("dlf.region", "your-region");
    options.set("dlf.access-key-id", "your-access-key-id");
    options.set("dlf.access-key-secret", "your-access-key-secret");

    // Create RESTApi instance
    // config_required = true means it will fetch config from server
    println!("Creating RESTApi instance...");
    let api = match RESTApi::new(options, true).await {
        Ok(api) => api,
        Err(e) => {
            eprintln!("Failed to create RESTApi: {}", e);
            return;
        }
    };

    // ==================== Database Operations ====================
    println!("\n=== Database Operations ===\n");

    // List databases
    println!("Listing databases...");
    match api.list_databases().await {
        Ok(databases) => {
            println!("Databases found: {:?}", databases);
            println!("Total count: {}", databases.len());
        }
        Err(e) => {
            eprintln!("Failed to list databases: {}", e);
        }
    }

    // Create database
    println!("\nCreating database 'example_db'...");
    match api
        .create_database("example_db", Some(HashMap::new()))
        .await
    {
        Ok(()) => println!("Database created successfully"),
        Err(e) => eprintln!("Failed to create database: {}", e),
    }

    // Get database info
    println!("\nGetting database info for 'example_db'...");
    match api.get_database("example_db").await {
        Ok(db) => println!("Database: {:?}", db),
        Err(e) => eprintln!("Failed to get database: {}", e),
    }

    // ==================== Table Operations ====================
    println!("\n=== Table Operations ===\n");

    // Create table
    println!("Creating table 'example_db.users'...");
    let schema = create_test_schema();
    match api.create_table("example_db", "users", schema).await {
        Ok(()) => println!("Table created successfully"),
        Err(e) => eprintln!("Failed to create table: {}", e),
    }

    // List tables
    println!("\nListing tables in 'example_db'...");
    match api.list_tables("example_db").await {
        Ok(tables) => {
            println!("Tables found: {:?}", tables);
        }
        Err(e) => {
            eprintln!("Failed to list tables: {}", e);
        }
    }

    // Get table info
    println!("\nGetting table info for 'example_db.users'...");
    match api.get_table("example_db", "users").await {
        Ok(table) => println!("Table: {:?}", table),
        Err(e) => eprintln!("Failed to get table: {}", e),
    }

    // Rename table
    println!("\nRenaming table 'users' to 'users_renamed'...");
    match api
        .rename_table("example_db", "users", "example_db", "users_renamed")
        .await
    {
        Ok(()) => println!("Table renamed successfully"),
        Err(e) => eprintln!("Failed to rename table: {}", e),
    }

    // Drop table
    println!("\nDropping table 'example_db.users_renamed'...");
    match api.drop_table("example_db", "users_renamed").await {
        Ok(()) => println!("Table dropped successfully"),
        Err(e) => eprintln!("Failed to drop table: {}", e),
    }

    // Drop database
    println!("\nDropping database 'example_db'...");
    match api.drop_database("example_db").await {
        Ok(()) => println!("Database dropped successfully"),
        Err(e) => eprintln!("Failed to drop database: {}", e),
    }

    println!("\nExample completed!");
}
