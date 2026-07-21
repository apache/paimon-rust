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

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};

use crate::{BenchmarkRuntimeConfig, ExistingTablePolicy, SourceKind};

#[derive(Debug, Parser)]
#[command(
    name = "paimon-tpcds-bench",
    about = "TPC-DS-derived non-TPC benchmark for DataFusion and Paimon"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Load generated TPC-DS Parquet data into append-only Paimon tables.
    Load(LoadArgs),
    /// Run external TPC-DS-derived query files against Paimon or Parquet.
    Run(RunArgs),
}

#[derive(Debug, Args)]
pub struct LoadArgs {
    /// Directory containing <table>.parquet paths.
    #[arg(long)]
    pub data: PathBuf,
    /// Local Paimon filesystem warehouse directory.
    #[arg(long)]
    pub warehouse: PathBuf,
    /// Paimon database name.
    #[arg(long, default_value = "tpcds")]
    pub database: String,
    /// Comma-separated table names; defaults to all 24 tables.
    #[arg(long)]
    pub tables: Option<String>,
    /// Behavior when a target table already exists.
    #[arg(long, value_enum, default_value_t = ExistingPolicyArg::Error)]
    pub if_exists: ExistingPolicyArg,
    #[command(flatten)]
    pub runtime: RuntimeArgs,
}

#[derive(Debug, Args)]
pub struct RunArgs {
    /// Source being measured.
    #[arg(long, value_enum)]
    pub source: SourceKind,
    /// Parquet data directory. Required when --source=parquet.
    #[arg(long)]
    pub data: Option<PathBuf>,
    /// Local Paimon warehouse, or a temporary catalog directory for Parquet.
    #[arg(long)]
    pub warehouse: PathBuf,
    /// Directory containing q1.sql through q99.sql.
    #[arg(long)]
    pub queries: PathBuf,
    /// JSON output path.
    #[arg(long)]
    pub output: PathBuf,
    /// Catalog database containing the Paimon tables.
    #[arg(long, default_value = "tpcds")]
    pub database: String,
    /// Query numbers, comma lists, or inclusive ranges (for example 1,3-5).
    #[arg(long)]
    pub query: Option<String>,
    /// Parquet tables to register; defaults to all 24 tables.
    #[arg(long)]
    pub tables: Option<String>,
    /// Warmup executions per query file.
    #[arg(long, default_value_t = 1)]
    pub warmup: usize,
    /// Measured executions per query file.
    #[arg(long, default_value_t = 3)]
    pub iterations: usize,
    #[command(flatten)]
    pub runtime: RuntimeArgs,
}

impl RunArgs {
    pub fn validate(&self) -> Result<(), String> {
        if self.source == SourceKind::Parquet && self.data.is_none() {
            return Err("--data is required when --source=parquet".to_string());
        }
        if self.iterations == 0 {
            return Err("--iterations must be greater than zero".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Args)]
pub struct RuntimeArgs {
    /// DataFusion execution partitions. Defaults to available CPUs.
    #[arg(long)]
    pub target_partitions: Option<usize>,
    /// Record batch size used by both DataFusion and Paimon file readers.
    #[arg(long, default_value_t = 8192)]
    pub batch_size: usize,
    /// Evaluate pushed filters during Parquet scans, in addition to statistics pruning.
    #[arg(long)]
    pub parquet_pushdown_filters: bool,
    /// DataFusion memory limit in GiB. Omit for an unbounded pool.
    #[arg(long)]
    pub memory_limit_gib: Option<u64>,
    /// Directory used for DataFusion spill files.
    #[arg(long)]
    pub spill_dir: Option<PathBuf>,
    /// Maximum spill-directory usage in GiB.
    #[arg(long)]
    pub max_spill_gib: Option<u64>,
}

impl RuntimeArgs {
    pub fn to_config(&self) -> Result<BenchmarkRuntimeConfig, String> {
        if self.batch_size == 0 {
            return Err("--batch-size must be greater than zero".to_string());
        }
        let defaults = BenchmarkRuntimeConfig::default();
        Ok(BenchmarkRuntimeConfig {
            target_partitions: self
                .target_partitions
                .unwrap_or(defaults.target_partitions)
                .max(1),
            batch_size: self.batch_size,
            parquet_pushdown_filters: self.parquet_pushdown_filters,
            memory_limit_bytes: self.memory_limit_gib.map(gib_to_usize).transpose()?,
            spill_dir: self.spill_dir.clone(),
            max_spill_bytes: self.max_spill_gib.map(gib_to_u64).transpose()?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ExistingPolicyArg {
    Error,
    Skip,
    Overwrite,
}

impl From<ExistingPolicyArg> for ExistingTablePolicy {
    fn from(value: ExistingPolicyArg) -> Self {
        match value {
            ExistingPolicyArg::Error => ExistingTablePolicy::Error,
            ExistingPolicyArg::Skip => ExistingTablePolicy::Skip,
            ExistingPolicyArg::Overwrite => ExistingTablePolicy::Overwrite,
        }
    }
}

fn gib_to_usize(gib: u64) -> Result<usize, String> {
    usize::try_from(gib_to_u64(gib)?).map_err(|_| format!("{gib} GiB exceeds usize"))
}

fn gib_to_u64(gib: u64) -> Result<u64, String> {
    gib.checked_mul(1024 * 1024 * 1024)
        .ok_or_else(|| format!("{gib} GiB exceeds u64"))
}
