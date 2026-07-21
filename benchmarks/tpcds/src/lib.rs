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

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

mod cli;
mod command;
mod context;
mod load;
mod report;
mod run;

pub use cli::{Cli, Command, ExistingPolicyArg, LoadArgs, RunArgs, RuntimeArgs};
pub use command::execute_command;
pub use context::{
    build_sql_context, open_catalog_session, BenchmarkRuntimeConfig, CatalogSession,
};
pub use load::{load_parquet_table, ExistingTablePolicy, LoadStatus, TableLoadResult};
pub use report::{write_report, BenchmarkReport, SourceKind, NON_TPC_DISCLOSURE};
pub use run::{
    register_parquet_tables, run_query_file, IterationResult, PhysicalMetrics, QueryRunConfig,
    QueryRunResult,
};

pub const TPCDS_TABLES: [&str; 24] = [
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "time_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryFile {
    pub number: u32,
    pub path: PathBuf,
    pub statements: Vec<String>,
}

pub fn load_query_files(query_dir: &Path, queries: &[u32]) -> Result<Vec<QueryFile>, String> {
    queries
        .iter()
        .map(|number| {
            let path = query_dir.join(format!("q{number}.sql"));
            let sql = fs::read_to_string(&path)
                .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
            let statements = Parser::parse_sql(&GenericDialect, &sql)
                .map_err(|error| format!("failed to parse {}: {error}", path.display()))?
                .into_iter()
                .map(|statement| statement.to_string())
                .collect::<Vec<_>>();
            if statements.is_empty() {
                return Err(format!("query file {} is empty", path.display()));
            }
            Ok(QueryFile {
                number: *number,
                path,
                statements,
            })
        })
        .collect()
}

pub fn parse_number_selection(
    selection: Option<&str>,
    min: u32,
    max: u32,
) -> Result<Vec<u32>, String> {
    let mut values = BTreeSet::new();
    let selection = selection.unwrap_or("");
    for part in selection
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        if let Some((start, end)) = part.split_once('-') {
            let start = start
                .trim()
                .parse::<u32>()
                .map_err(|_| format!("invalid selection value '{start}'"))?;
            let end = end
                .trim()
                .parse::<u32>()
                .map_err(|_| format!("invalid selection value '{end}'"))?;
            if start > end {
                return Err(format!("invalid descending range '{part}'"));
            }
            values.extend(start..=end);
        } else {
            values.insert(
                part.parse::<u32>()
                    .map_err(|_| format!("invalid selection value '{part}'"))?,
            );
        }
    }

    if values.is_empty() {
        values.extend(min..=max);
    }
    if let Some(value) = values.iter().find(|value| **value < min || **value > max) {
        return Err(format!("selection value {value} is outside {min}..={max}"));
    }
    Ok(values.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use clap::Parser;
    use datafusion::execution::memory_pool::MemoryLimit;

    use super::{
        build_sql_context, load_query_files, parse_number_selection, BenchmarkReport,
        BenchmarkRuntimeConfig, Cli, Command, QueryRunConfig, SourceKind, TPCDS_TABLES,
    };
    use tempfile::TempDir;

    #[test]
    fn query_selection_supports_lists_and_ranges() {
        assert_eq!(
            parse_number_selection(Some("1,3-5"), 1, 99).unwrap(),
            vec![1, 3, 4, 5]
        );
    }

    #[test]
    fn query_files_are_external_and_can_contain_multiple_statements() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("q1.sql"), "SELECT 1; SELECT 2;").unwrap();
        fs::write(dir.path().join("q3.sql"), "SELECT 3;").unwrap();

        let files = load_query_files(dir.path(), &[1, 3]).unwrap();

        assert_eq!(
            files.iter().map(|file| file.number).collect::<Vec<_>>(),
            vec![1, 3]
        );
        assert_eq!(files[0].statements, vec!["SELECT 1", "SELECT 2"]);
        assert_eq!(files[1].statements, vec!["SELECT 3"]);
    }

    #[test]
    fn runtime_config_controls_parallelism_memory_and_spill_path() {
        let spill_dir = TempDir::new().unwrap();
        let ctx = build_sql_context(&BenchmarkRuntimeConfig {
            target_partitions: 3,
            batch_size: 4096,
            parquet_pushdown_filters: true,
            memory_limit_bytes: Some(32 * 1024 * 1024),
            spill_dir: Some(spill_dir.path().to_path_buf()),
            max_spill_bytes: Some(64 * 1024 * 1024),
        })
        .unwrap();

        assert_eq!(
            ctx.ctx()
                .state()
                .config_options()
                .execution
                .target_partitions,
            3
        );
        assert_eq!(
            ctx.ctx().state().config_options().execution.batch_size,
            4096
        );
        assert!(
            ctx.ctx()
                .state()
                .config_options()
                .execution
                .parquet
                .pushdown_filters
        );
        assert!(matches!(
            ctx.ctx().runtime_env().memory_pool.memory_limit(),
            MemoryLimit::Finite(size) if size == 32 * 1024 * 1024
        ));
        assert!(
            ctx.ctx().runtime_env().disk_manager.temp_dir_paths()[0].starts_with(spill_dir.path())
        );
        assert_eq!(
            ctx.ctx()
                .runtime_env()
                .disk_manager
                .max_temp_directory_size(),
            64 * 1024 * 1024
        );
    }

    #[test]
    fn canonical_table_list_contains_all_24_tpcds_tables() {
        assert_eq!(TPCDS_TABLES.len(), 24);
        assert_eq!(TPCDS_TABLES.first(), Some(&"call_center"));
        assert_eq!(TPCDS_TABLES.last(), Some(&"web_site"));
        assert!(TPCDS_TABLES.contains(&"store_sales"));
    }

    #[test]
    fn report_json_contains_non_tpc_disclosure_and_round_trips() {
        let report = BenchmarkReport::new(
            SourceKind::Paimon,
            BenchmarkRuntimeConfig::default(),
            QueryRunConfig::default(),
            "/warehouse".to_string(),
            "/data".to_string(),
            "/queries".to_string(),
            "tpcds".to_string(),
            vec![],
        );

        let json = serde_json::to_string(&report).unwrap();
        let decoded: BenchmarkReport = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.source, SourceKind::Paimon);
        assert!(decoded.disclosure.contains("non-TPC"));
        assert_eq!(decoded.datafusion_version, "54.0.0");
    }

    #[test]
    fn cli_parses_a_paimon_run_without_a_parquet_data_path() {
        let cli = Cli::try_parse_from([
            "paimon-tpcds-bench",
            "run",
            "--source",
            "paimon",
            "--warehouse",
            "/warehouse",
            "--queries",
            "/queries",
            "--output",
            "/report.json",
            "--query",
            "1,3-5",
        ])
        .unwrap();

        let Command::Run(args) = cli.command else {
            panic!("expected run command");
        };
        assert_eq!(args.source, SourceKind::Paimon);
        assert_eq!(args.query.as_deref(), Some("1,3-5"));
        assert!(args.data.is_none());
    }

    #[test]
    fn parquet_run_requires_a_data_path() {
        let cli = Cli::try_parse_from([
            "paimon-tpcds-bench",
            "run",
            "--source",
            "parquet",
            "--warehouse",
            "/warehouse",
            "--queries",
            "/queries",
            "--output",
            "/report.json",
        ])
        .unwrap();
        let Command::Run(args) = cli.command else {
            panic!("expected run command");
        };

        assert!(args.validate().unwrap_err().contains("--data"));
    }
}
