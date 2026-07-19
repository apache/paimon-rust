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
use std::time::Instant;

use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::ParquetReadOptions;
use serde::{Deserialize, Serialize};

use crate::context::CatalogSession;
use crate::QueryFile;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub async fn register_parquet_tables<S: AsRef<str>>(
    session: &CatalogSession,
    data_root: &Path,
    tables: &[S],
) -> Result<(), BoxError> {
    for table in tables {
        let table = table.as_ref();
        let path = data_root.join(format!("{table}.parquet"));
        let path = path
            .to_str()
            .ok_or_else(|| format!("source path is not valid UTF-8: {}", path.display()))?;
        let frame = session
            .sql
            .ctx()
            .read_parquet(path, ParquetReadOptions::default())
            .await?;
        let table_reference = format!(
            "{}.{}.{}",
            quote_identifier(&session.catalog_name),
            quote_identifier(&session.database),
            quote_identifier(table)
        );
        if session.sql.temp_table_exist(table_reference.as_str())? {
            session
                .sql
                .deregister_temp_table(table_reference.as_str())?;
        }
        session
            .sql
            .register_temp_table(table_reference.as_str(), frame.into_view())?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryRunConfig {
    pub warmup_iterations: usize,
    pub measured_iterations: usize,
}

impl Default for QueryRunConfig {
    fn default() -> Self {
        Self {
            warmup_iterations: 1,
            measured_iterations: 3,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhysicalMetrics {
    pub spill_count: u64,
    pub spilled_rows: u64,
    pub spilled_bytes: u64,
    pub bytes_scanned: u64,
    pub operator_peak_memory_bytes: u64,
}

impl PhysicalMetrics {
    fn add_plan(&mut self, plan: &dyn ExecutionPlan) {
        if let Some(metrics) = plan.metrics() {
            self.spill_count += metrics.spill_count().unwrap_or(0) as u64;
            self.spilled_rows += metrics.spilled_rows().unwrap_or(0) as u64;
            self.spilled_bytes += metrics.spilled_bytes().unwrap_or(0) as u64;
            self.bytes_scanned += metrics
                .sum_by_name("bytes_scanned")
                .map(|value| value.as_usize() as u64)
                .unwrap_or(0);
            self.operator_peak_memory_bytes += metrics
                .sum_by_name("peak_mem_used")
                .map(|value| value.as_usize() as u64)
                .unwrap_or(0);
        }
        for child in plan.children() {
            self.add_plan(child.as_ref());
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IterationResult {
    pub iteration: usize,
    pub logical_planning_ms: f64,
    pub physical_planning_ms: f64,
    pub execution_ms: f64,
    pub total_ms: f64,
    pub output_rows: u64,
    pub metrics: PhysicalMetrics,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryRunResult {
    pub query: u32,
    pub path: String,
    pub warmup_failures: Vec<String>,
    pub iterations: Vec<IterationResult>,
}

pub async fn run_query_file(
    session: &CatalogSession,
    query: &QueryFile,
    config: &QueryRunConfig,
) -> QueryRunResult {
    let mut warmup_failures = Vec::new();
    for iteration in 0..config.warmup_iterations {
        let result = execute_iteration(session, query, iteration).await;
        if let Some(error) = result.error {
            warmup_failures.push(error);
        }
    }

    let mut iterations = Vec::with_capacity(config.measured_iterations);
    for iteration in 0..config.measured_iterations {
        iterations.push(execute_iteration(session, query, iteration + 1).await);
    }

    QueryRunResult {
        query: query.number,
        path: query.path.display().to_string(),
        warmup_failures,
        iterations,
    }
}

async fn execute_iteration(
    session: &CatalogSession,
    query: &QueryFile,
    iteration: usize,
) -> IterationResult {
    let total_started = Instant::now();
    let mut logical_planning_ms = 0.0;
    let mut physical_planning_ms = 0.0;
    let mut execution_ms = 0.0;
    let mut output_rows = 0u64;
    let mut metrics = PhysicalMetrics::default();

    for statement in &query.statements {
        let logical_started = Instant::now();
        let frame = match session.sql.sql(statement).await {
            Ok(frame) => frame,
            Err(error) => {
                logical_planning_ms += elapsed_ms(logical_started);
                return failed_iteration(
                    iteration,
                    logical_planning_ms,
                    physical_planning_ms,
                    execution_ms,
                    total_started,
                    output_rows,
                    metrics,
                    error.to_string(),
                );
            }
        };
        logical_planning_ms += elapsed_ms(logical_started);

        let physical_started = Instant::now();
        let plan = match frame.create_physical_plan().await {
            Ok(plan) => plan,
            Err(error) => {
                physical_planning_ms += elapsed_ms(physical_started);
                return failed_iteration(
                    iteration,
                    logical_planning_ms,
                    physical_planning_ms,
                    execution_ms,
                    total_started,
                    output_rows,
                    metrics,
                    error.to_string(),
                );
            }
        };
        physical_planning_ms += elapsed_ms(physical_started);

        let execution_started = Instant::now();
        let batches = match collect(plan.clone(), session.sql.ctx().task_ctx()).await {
            Ok(batches) => batches,
            Err(error) => {
                execution_ms += elapsed_ms(execution_started);
                metrics.add_plan(plan.as_ref());
                return failed_iteration(
                    iteration,
                    logical_planning_ms,
                    physical_planning_ms,
                    execution_ms,
                    total_started,
                    output_rows,
                    metrics,
                    error.to_string(),
                );
            }
        };
        execution_ms += elapsed_ms(execution_started);
        output_rows += batches
            .iter()
            .map(|batch| batch.num_rows() as u64)
            .sum::<u64>();
        metrics.add_plan(plan.as_ref());
    }

    IterationResult {
        iteration,
        logical_planning_ms,
        physical_planning_ms,
        execution_ms,
        total_ms: elapsed_ms(total_started),
        output_rows,
        metrics,
        error: None,
    }
}

#[allow(clippy::too_many_arguments)]
fn failed_iteration(
    iteration: usize,
    logical_planning_ms: f64,
    physical_planning_ms: f64,
    execution_ms: f64,
    total_started: Instant,
    output_rows: u64,
    metrics: PhysicalMetrics,
    error: String,
) -> IterationResult {
    IterationResult {
        iteration,
        logical_planning_ms,
        physical_planning_ms,
        execution_ms,
        total_ms: elapsed_ms(total_started),
        output_rows,
        metrics,
        error: Some(error),
    }
}

fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1_000.0
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}
