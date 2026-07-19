// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::ValueEnum;
use serde::{Deserialize, Serialize};

use crate::{BenchmarkRuntimeConfig, QueryRunConfig, QueryRunResult};

pub const NON_TPC_DISCLOSURE: &str =
    "TPC-DS-derived non-TPC benchmark; these results are not official TPC results.";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    Paimon,
    Parquet,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkReport {
    pub schema_version: u32,
    pub disclosure: String,
    pub engine: String,
    pub datafusion_version: String,
    pub paimon_version: String,
    pub created_unix_ms: u128,
    pub source: SourceKind,
    pub runtime: BenchmarkRuntimeConfig,
    pub query_run: QueryRunConfig,
    pub warehouse: String,
    pub data_root: String,
    pub query_dir: String,
    pub database: String,
    pub queries: Vec<QueryRunResult>,
}

impl BenchmarkReport {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source: SourceKind,
        runtime: BenchmarkRuntimeConfig,
        query_run: QueryRunConfig,
        warehouse: String,
        data_root: String,
        query_dir: String,
        database: String,
        queries: Vec<QueryRunResult>,
    ) -> Self {
        Self {
            schema_version: 1,
            disclosure: NON_TPC_DISCLOSURE.to_string(),
            engine: "datafusion+paimon-rust".to_string(),
            datafusion_version: datafusion::DATAFUSION_VERSION.to_string(),
            paimon_version: env!("CARGO_PKG_VERSION").to_string(),
            created_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
            source,
            runtime,
            query_run,
            warehouse,
            data_root,
            query_dir,
            database,
            queries,
        }
    }

    pub fn has_failures(&self) -> bool {
        self.queries.iter().any(|query| {
            !query.warmup_failures.is_empty()
                || query
                    .iterations
                    .iter()
                    .any(|iteration| iteration.error.is_some())
        })
    }
}

pub fn write_report(
    report: &BenchmarkReport,
    path: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    std::fs::write(path, serde_json::to_vec_pretty(report)?)?;
    Ok(())
}
