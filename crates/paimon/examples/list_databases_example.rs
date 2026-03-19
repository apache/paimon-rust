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
//! cargo run -p paimon --example list_databases_example
//! ```

use paimon::api::rest_api::RESTApi;
use paimon::common::{CatalogOptions, Options};

#[tokio::main]
async fn main() {
    // Create configuration options
    let mut options = Options::new();

    // Basic configuration - replace with your actual server URL
    options.set(CatalogOptions::METASTORE, "rest");
    options.set(CatalogOptions::WAREHOUSE, "your_warehouse");
    options.set(CatalogOptions::URI, "http://localhost:8080/");

    // Bearer token authentication (optional)
    // options.set(CatalogOptions::TOKEN_PROVIDER, "bear");
    // options.set(CatalogOptions::TOKEN, "your_token");

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
}
