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

//! Build an IVF-PQ index through the production Paimon path.
//!
//! ```text
//! PAIMON_CATALOG_OPTIONS='{"metastore":"filesystem","warehouse":"/tmp/warehouse"}' \
//! PAIMON_LOG_VECTOR_INDEX_BUILD_TIMING=1 \
//! cargo run --release -p paimon --example ivfpq_build_benchmark -- \
//!   <database> <table> <vector-column> [--drop-existing]
//! ```

use std::collections::HashMap;
use std::error::Error;
use std::time::Instant;

use paimon::catalog::Identifier;
use paimon::{CatalogFactory, Options};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let database = required_arg(&mut args, "database")?;
    let table_name = required_arg(&mut args, "table")?;
    let column = required_arg(&mut args, "vector-column")?;
    let drop_existing = args.any(|arg| arg == "--drop-existing");

    let catalog_options = std::env::var("PAIMON_CATALOG_OPTIONS")?;
    let catalog =
        CatalogFactory::create(Options::from_map(serde_json::from_str(&catalog_options)?)).await?;
    let table = catalog
        .get_table(&Identifier::new(&database, &table_name))
        .await?;

    let dropped_index_files = if drop_existing {
        let mut builder = table.new_global_index_drop_builder();
        builder.with_index_column(&column).with_index_type("ivf-pq");
        builder.execute().await?
    } else {
        0
    };

    let options = HashMap::from([
        ("dimension".to_string(), "768".to_string()),
        ("metric".to_string(), "cosine".to_string()),
        ("nlist".to_string(), "4096".to_string()),
        ("pq.m".to_string(), "192".to_string()),
    ]);
    let started = Instant::now();
    let built_shards = table
        .new_global_index_build_builder()
        .with_index_type("ivf-pq")
        .with_index_column(&column)
        .with_options(options.clone())
        .execute()
        .await?;

    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "database": database,
            "table": table_name,
            "column": column,
            "index_type": "ivf-pq",
            "build_options": options,
            "dropped_index_files": dropped_index_files,
            "built_shards": built_shards,
            "duration_seconds": started.elapsed().as_secs_f64(),
        }))?
    );
    Ok(())
}

fn required_arg(
    args: &mut impl Iterator<Item = String>,
    name: &str,
) -> Result<String, Box<dyn Error>> {
    args.next()
        .ok_or_else(|| format!("missing <{name}> argument").into())
}
