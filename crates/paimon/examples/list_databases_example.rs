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

//! Example: List databases using RESTApi
//!
//! This example demonstrates how to create a RESTApi instance
//! and call the list_databases() API to retrieve all databases.
//!
//! # Usage
//! ```bash
//! # Set environment variables first
//! export DLF_ACCESS_KEY_ID=your_access_key_id
//! export DLF_ACCESS_KEY_SECRET=your_access_key_secret
//! 
//! cargo run -p paimon --example list_databases_example
//! ```

use std::env;

use paimon::api::rest_api::RESTApi;
use paimon::common::{CatalogOptions, Options};

#[tokio::main]
async fn main() {
    // Create configuration options
    let mut options = Options::new();

    // Basic configuration
    options.set(CatalogOptions::METASTORE, "rest");
    options.set(CatalogOptions::WAREHOUSE, "pypaimon_test1");
    options.set(CatalogOptions::URI, "http://dlf-regres-test-cn-hangzhou-vpc.taobao.net/");

    // DLF configuration
    options.set(CatalogOptions::DLF_REGION, "cn-hangzhou");
    options.set(CatalogOptions::TOKEN_PROVIDER, "dlf");

    // Read DLF credentials from environment variables
    let dlf_access_key_id = env::var("DLF_ACCESS_KEY_ID")
        .expect("DLF_ACCESS_KEY_ID environment variable not set");
    let dlf_access_key_secret = env::var("DLF_ACCESS_KEY_SECRET")
        .expect("DLF_ACCESS_KEY_SECRET environment variable not set");

    options.set(CatalogOptions::DLF_ACCESS_KEY_ID, &dlf_access_key_id);
    options.set(CatalogOptions::DLF_ACCESS_KEY_SECRET, &dlf_access_key_secret);

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

    // Call list_databases() API
    println!("Calling list_databases()...");
    match api.list_databases().await {
        Ok(databases) => {
            println!("Databases found: {:?}", databases);
            println!("Total count: {}", databases.len());
        }
        Err(e) => {
            eprintln!("Failed to list databases: {}", e);
        }
    }

    // Call list_tables() API for each database
    println!("\nCalling list_tables() for each database...");
    match api.list_databases().await {
        Ok(databases) => {
            for db in databases {
                println!("\n--- Database: {} ---", db);
                match api.list_tables(&db).await {
                    Ok(tables) => {
                        if tables.is_empty() {
                            println!("  No tables found.");
                        } else {
                            println!("  Tables found: {:?}", tables);
                            println!("  Total count: {}", tables.len());
                        }
                    }
                    Err(e) => {
                        eprintln!("  Failed to list tables: {}", e);
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("Failed to get databases: {}", e);
        }
    }
}
