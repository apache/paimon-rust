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

//! Standalone Paimon REST catalog server backed by a local filesystem warehouse.
//!
//! # Usage
//! ```bash
//! REST_WAREHOUSE=/tmp/paimon-warehouse REST_HOST=127.0.0.1 REST_PORT=8080 \
//!   cargo run -p paimon-rest-server
//! ```
//!
//! Then point a client (e.g. the `rest_local_smoke` example, or Java) at it:
//! ```bash
//! REST_URI=http://localhost:8080 REST_WAREHOUSE=/tmp/paimon-warehouse \
//!   cargo run -p paimon --example rest_local_smoke
//! ```

use paimon_rest_server::{BoxError, FsRestCatalogServer};

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let warehouse = env_or("REST_WAREHOUSE", "/tmp/paimon-warehouse");
    let host = env_or("REST_HOST", "127.0.0.1");
    let port: u16 = env_or("REST_PORT", "8080")
        .parse()
        .map_err(|e| format!("invalid REST_PORT: {e}"))?;
    let prefix = env_or("REST_PREFIX", "");

    // Ensure the warehouse directory exists so FileIO can list it.
    std::fs::create_dir_all(&warehouse)?;

    let server = FsRestCatalogServer::start_on(warehouse.clone(), &prefix, &host, port).await?;

    println!("Paimon REST catalog server (filesystem-backed)");
    println!("  warehouse : {warehouse}");
    println!("  listening : {}", server.url());
    if !prefix.is_empty() {
        println!("  prefix    : {prefix}");
    }
    println!("Press Ctrl-C to stop.");

    tokio::signal::ctrl_c().await?;
    println!("\nShutting down.");
    Ok(())
}
