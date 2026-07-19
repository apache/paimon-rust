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

use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::error::Result as DataFusionResult;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionStateBuilder;
use paimon::{CatalogOptions, FileSystemCatalog, Options};
use paimon_datafusion::SQLContext;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BenchmarkRuntimeConfig {
    pub target_partitions: usize,
    pub memory_limit_bytes: Option<usize>,
    pub spill_dir: Option<PathBuf>,
    pub max_spill_bytes: Option<u64>,
}

impl Default for BenchmarkRuntimeConfig {
    fn default() -> Self {
        Self {
            target_partitions: std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1),
            memory_limit_bytes: None,
            spill_dir: None,
            max_spill_bytes: None,
        }
    }
}

pub fn build_sql_context(config: &BenchmarkRuntimeConfig) -> DataFusionResult<SQLContext> {
    let mut runtime = RuntimeEnvBuilder::new();
    if let Some(memory_limit) = config.memory_limit_bytes {
        runtime = runtime.with_memory_limit(memory_limit, 1.0);
    }
    if let Some(spill_dir) = &config.spill_dir {
        runtime = runtime.with_temp_file_path(spill_dir);
    }
    if let Some(max_spill_bytes) = config.max_spill_bytes {
        runtime = runtime.with_max_temp_directory_size(max_spill_bytes);
    }
    let sql = SQLContext::new();
    let state_ref = sql.ctx().state_ref();
    let current_state = state_ref.read().clone();
    let session_config = current_state
        .config()
        .clone()
        .with_target_partitions(config.target_partitions.max(1));
    let state = SessionStateBuilder::from(current_state)
        .with_config(session_config)
        .with_runtime_env(Arc::new(runtime.build()?))
        .build();
    *state_ref.write() = state;
    drop(state_ref);
    Ok(sql)
}

pub struct CatalogSession {
    pub sql: SQLContext,
    pub catalog: Arc<FileSystemCatalog>,
    pub catalog_name: String,
    pub database: String,
}

pub async fn open_catalog_session(
    runtime_config: &BenchmarkRuntimeConfig,
    warehouse: &Path,
    database: &str,
) -> Result<CatalogSession, Box<dyn std::error::Error + Send + Sync>> {
    std::fs::create_dir_all(warehouse)?;
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse.display().to_string());
    let catalog = Arc::new(FileSystemCatalog::new(options)?);
    let mut sql = build_sql_context(runtime_config)?;
    sql.register_catalog_with_default_db("paimon", catalog.clone(), Some(database))
        .await?;
    Ok(CatalogSession {
        sql,
        catalog,
        catalog_name: "paimon".to_string(),
        database: database.to_string(),
    })
}
