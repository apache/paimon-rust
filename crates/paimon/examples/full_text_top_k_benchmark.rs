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

//! Compare certified and exhaustive FTS on the same local table and snapshot.
//! cargo run --release -p paimon --example full_text_top_k_benchmark -- [rows] [iterations] [k] [rows-per-shard]

use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowType, Field, Schema as ArrowSchema};
use paimon::catalog::Identifier;
use paimon::spec::{DataType, Schema, VarCharType};
use paimon::table::Table;
use paimon::vector_search::ScoredRowIds;
use paimon::{CatalogFactory, Options};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

async fn search(table: &Table, enabled: bool, k: usize) -> paimon::Result<ScoredRowIds> {
    table
        .copy_with_options(HashMap::from([(
            "full-text-index.top-k.enabled".into(),
            enabled.to_string(),
        )]))
        .new_full_text_search_builder()
        .with_text_column("body")
        .with_query_text("paimon")
        .with_limit(k)
        .execute()
        .await?
        .into_row_ids()
}

fn percentile(samples: &mut [f64], percentile: usize) -> f64 {
    samples.sort_by(f64::total_cmp);
    samples[(samples.len() * percentile).div_ceil(100).saturating_sub(1)]
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let defaults = [20_000, 10, 10, 5_000];
    let mut args = std::env::args().skip(1);
    let mut config = defaults;
    for (value, default) in config.iter_mut().zip(defaults) {
        *value = args
            .next()
            .map(|s| s.parse::<usize>())
            .transpose()?
            .unwrap_or(default);
        if *value == 0 {
            return Err("all arguments must be positive".into());
        }
    }
    if args.next().is_some() {
        return Err("expected rows iterations k rows-per-shard".into());
    }
    let [rows, iterations, k, shard_rows] = config;
    let directory = tempfile::tempdir()?;
    let catalog = CatalogFactory::create(Options::from_map(HashMap::from([(
        "warehouse".into(),
        directory.path().to_string_lossy().into_owned(),
    )])))
    .await?;
    catalog
        .create_database("bench", false, HashMap::new())
        .await?;
    for tied in [false, true] {
        let name = if tied { "tied" } else { "skewed" };
        let identifier = Identifier::new("bench", name);
        let schema = Schema::builder()
            .column("body", DataType::VarChar(VarCharType::string_type()))
            .option("row-tracking.enabled", "true")
            .option("data-evolution.enabled", "true")
            .option("global-index.enabled", "true")
            .option("global-index.row-count-per-shard", shard_rows.to_string())
            .build()?;
        catalog.create_table(&identifier, schema, false).await?;
        let table = catalog.get_table(&identifier).await?;
        let builder = table.new_write_builder();
        let mut writer = builder.new_write()?;
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "body",
            ArrowType::Utf8,
            true,
        )]));
        for start in (0..rows).step_by(4096) {
            let texts = (start..(start + 4096).min(rows))
                .map(|row| {
                    let position = row % shard_rows;
                    if tied {
                        "paimon".to_string()
                    } else if position < 64 {
                        "paimon ".repeat(64 - position)
                    } else {
                        "paimon padding padding padding padding padding padding padding".to_string()
                    }
                })
                .collect::<Vec<_>>();
            writer
                .write_arrow_batch(&RecordBatch::try_new(
                    arrow_schema.clone(),
                    vec![Arc::new(StringArray::from(texts))],
                )?)
                .await?;
        }
        builder
            .try_new_commit()?
            .commit(writer.prepare_commit().await?)
            .await?;
        let shards = table
            .new_global_index_build_builder()
            .with_index_type("full-text")
            .with_index_column("body")
            .execute()
            .await?;
        // Warm both paths and assert IDs and scores before collecting timings.
        let expected = search(&table, false, k).await?;
        let actual = search(&table, true, k).await?;
        assert_eq!(actual.row_ids, expected.row_ids);
        assert_eq!(actual.scores, expected.scores);
        let mut timings = [Vec::new(), Vec::new()];
        for iteration in 0..iterations {
            for mode in [iteration % 2, 1 - iteration % 2] {
                let started = Instant::now();
                let actual = search(&table, mode == 1, k).await?;
                let elapsed = started.elapsed().as_secs_f64() * 1000.0;
                assert_eq!(actual.row_ids, expected.row_ids);
                assert_eq!(actual.scores, expected.scores);
                timings[mode].push(elapsed);
            }
        }
        println!(
            "{}",
            serde_json::json!({
                "corpus": name, "rows": rows, "k": k, "shards": shards,
                "iterations": iterations, "optimized_build": !cfg!(debug_assertions),
                "cache": "warm; alternating path order", "storage": "local temporary filesystem",
                "exhaustive_ms": timings[0], "certified_ms": timings[1],
                "exhaustive_p50_ms": percentile(&mut timings[0], 50),
                "exhaustive_p95_ms": percentile(&mut timings[0], 95),
                "certified_p50_ms": percentile(&mut timings[1], 50),
                "certified_p95_ms": percentile(&mut timings[1], 95),
            })
        );
    }
    Ok(())
}
