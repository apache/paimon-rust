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

use std::collections::HashSet;
use std::io::{Error as IoError, ErrorKind};

use crate::{
    load_parquet_table, load_query_files, open_catalog_session, parse_number_selection,
    register_parquet_tables, run_query_file, write_report, BenchmarkReport, Command,
    ExistingTablePolicy, QueryRunConfig, SourceKind, TPCDS_TABLES,
};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub async fn execute_command(command: Command) -> Result<(), BoxError> {
    match command {
        Command::Load(args) => {
            let runtime = args.runtime.to_config().map_err(invalid_input)?;
            let tables = select_tables(args.tables.as_deref())?;
            let session = open_catalog_session(&runtime, &args.warehouse, &args.database).await?;
            let policy: ExistingTablePolicy = args.if_exists.into();
            for table in tables {
                println!("Loading {table} ...");
                let result = load_parquet_table(&session, &args.data, table, policy).await?;
                println!(
                    "  status={:?} rows={} elapsed_ms={}",
                    result.status, result.rows, result.elapsed_ms
                );
            }
            Ok(())
        }
        Command::Run(args) => {
            args.validate().map_err(invalid_input)?;
            let runtime = args.runtime.to_config().map_err(invalid_input)?;
            let session = open_catalog_session(&runtime, &args.warehouse, &args.database).await?;
            if args.source == SourceKind::Parquet {
                let tables = select_tables(args.tables.as_deref())?;
                let data = args
                    .data
                    .as_deref()
                    .ok_or_else(|| invalid_input("--data is required when --source=parquet"))?;
                register_parquet_tables(&session, data, &tables).await?;
            }

            let query_numbers =
                parse_number_selection(args.query.as_deref(), 1, 99).map_err(invalid_input)?;
            let query_files =
                load_query_files(&args.queries, &query_numbers).map_err(invalid_input)?;
            let query_run = QueryRunConfig {
                warmup_iterations: args.warmup,
                measured_iterations: args.iterations,
            };
            let mut query_results = Vec::with_capacity(query_files.len());
            for query in &query_files {
                println!("Running q{} ...", query.number);
                let result = run_query_file(&session, query, &query_run).await;
                for iteration in &result.iterations {
                    if let Some(error) = &iteration.error {
                        println!(
                            "  iteration={} failed after {:.3} ms: {error}",
                            iteration.iteration, iteration.total_ms
                        );
                    } else {
                        println!(
                            "  iteration={} total_ms={:.3} rows={} spilled_bytes={}",
                            iteration.iteration,
                            iteration.total_ms,
                            iteration.output_rows,
                            iteration.metrics.spilled_bytes
                        );
                    }
                }
                query_results.push(result);
            }

            let report = BenchmarkReport::new(
                args.source,
                runtime,
                query_run,
                args.warehouse.display().to_string(),
                args.data
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_default(),
                args.queries.display().to_string(),
                args.database,
                query_results,
            );
            write_report(&report, &args.output)?;
            println!("Report written to {}", args.output.display());
            if report.has_failures() {
                return Err(IoError::other(format!(
                    "one or more queries failed; see {}",
                    args.output.display()
                ))
                .into());
            }
            Ok(())
        }
    }
}

fn select_tables(selection: Option<&str>) -> Result<Vec<&'static str>, BoxError> {
    let Some(selection) = selection else {
        return Ok(TPCDS_TABLES.to_vec());
    };
    let selected = selection
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .collect::<HashSet<_>>();
    if selected.is_empty() {
        return Ok(TPCDS_TABLES.to_vec());
    }
    if let Some(unknown) = selected.iter().find(|name| !TPCDS_TABLES.contains(name)) {
        return Err(invalid_input(format!("unknown TPC-DS table '{unknown}'")));
    }
    Ok(TPCDS_TABLES
        .iter()
        .copied()
        .filter(|name| selected.contains(name))
        .collect())
}

fn invalid_input(message: impl Into<String>) -> BoxError {
    IoError::new(ErrorKind::InvalidInput, message.into()).into()
}
