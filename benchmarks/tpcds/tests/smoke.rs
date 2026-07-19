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

use std::fs::{self, File};
use std::sync::Arc;

use arrow_array::{Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use paimon_tpcds_bench::{
    execute_command, load_parquet_table, load_query_files, open_catalog_session,
    register_parquet_tables, run_query_file, BenchmarkReport, BenchmarkRuntimeConfig, Command,
    ExistingPolicyArg, ExistingTablePolicy, LoadArgs, LoadStatus, QueryRunConfig, RunArgs,
    RuntimeArgs, SourceKind,
};
use parquet::arrow::ArrowWriter;
use tempfile::TempDir;

fn write_fixture(root: &TempDir, table: &str) {
    let table_dir = root.path().join(format!("{table}.parquet"));
    fs::create_dir_all(&table_dir).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(
        File::create(table_dir.join("part-0.parquet")).unwrap(),
        schema,
        None,
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

#[tokio::test]
async fn parquet_fixture_loads_into_paimon() {
    let data = TempDir::new().unwrap();
    let warehouse = TempDir::new().unwrap();
    write_fixture(&data, "store_sales");
    let session = open_catalog_session(
        &BenchmarkRuntimeConfig::default(),
        warehouse.path(),
        "tpcds",
    )
    .await
    .unwrap();

    let loaded = load_parquet_table(
        &session,
        data.path(),
        "store_sales",
        ExistingTablePolicy::Error,
    )
    .await
    .unwrap();

    assert_eq!(loaded.status, LoadStatus::Loaded);
    assert_eq!(loaded.rows, 3);
    let batches = session
        .sql
        .sql("SELECT COUNT(*) FROM paimon.tpcds.store_sales")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let counts = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(counts.value(0), 3);
}

#[tokio::test]
async fn loaded_paimon_table_runs_warmups_and_measured_iterations() {
    let data = TempDir::new().unwrap();
    let warehouse = TempDir::new().unwrap();
    let queries = TempDir::new().unwrap();
    write_fixture(&data, "store_sales");
    fs::write(
        queries.path().join("q1.sql"),
        "SELECT COUNT(*), SUM(id) FROM store_sales;",
    )
    .unwrap();
    let session = open_catalog_session(
        &BenchmarkRuntimeConfig::default(),
        warehouse.path(),
        "tpcds",
    )
    .await
    .unwrap();
    load_parquet_table(
        &session,
        data.path(),
        "store_sales",
        ExistingTablePolicy::Error,
    )
    .await
    .unwrap();
    let query = load_query_files(queries.path(), &[1]).unwrap().remove(0);

    let result = run_query_file(
        &session,
        &query,
        &QueryRunConfig {
            warmup_iterations: 1,
            measured_iterations: 2,
        },
    )
    .await;

    assert_eq!(result.query, 1);
    assert_eq!(result.warmup_failures.len(), 0);
    assert_eq!(result.iterations.len(), 2);
    assert!(result.iterations.iter().all(|iteration| {
        iteration.error.is_none() && iteration.output_rows == 1 && iteration.total_ms > 0.0
    }));
}

#[tokio::test]
async fn parquet_baseline_uses_the_same_query_runner() {
    let data = TempDir::new().unwrap();
    let catalog = TempDir::new().unwrap();
    let queries = TempDir::new().unwrap();
    write_fixture(&data, "store_sales");
    fs::write(
        queries.path().join("q1.sql"),
        "SELECT COUNT(*), SUM(id) FROM store_sales;",
    )
    .unwrap();
    let session = open_catalog_session(&BenchmarkRuntimeConfig::default(), catalog.path(), "tpcds")
        .await
        .unwrap();
    register_parquet_tables(&session, data.path(), &["store_sales"])
        .await
        .unwrap();
    let query = load_query_files(queries.path(), &[1]).unwrap().remove(0);

    let result = run_query_file(
        &session,
        &query,
        &QueryRunConfig {
            warmup_iterations: 0,
            measured_iterations: 1,
        },
    )
    .await;

    assert_eq!(result.iterations[0].output_rows, 1);
    assert!(result.iterations[0].error.is_none());
}

#[tokio::test]
async fn command_orchestration_loads_and_writes_a_run_report() {
    let data = TempDir::new().unwrap();
    let warehouse = TempDir::new().unwrap();
    let queries = TempDir::new().unwrap();
    let output = warehouse.path().join("report.json");
    write_fixture(&data, "store_sales");
    fs::write(
        queries.path().join("q1.sql"),
        "SELECT COUNT(*) FROM store_sales;",
    )
    .unwrap();
    let runtime = RuntimeArgs {
        target_partitions: Some(2),
        memory_limit_gib: None,
        spill_dir: None,
        max_spill_gib: None,
    };

    execute_command(Command::Load(LoadArgs {
        data: data.path().to_path_buf(),
        warehouse: warehouse.path().to_path_buf(),
        database: "tpcds".to_string(),
        tables: Some("store_sales".to_string()),
        if_exists: ExistingPolicyArg::Error,
        runtime: runtime.clone(),
    }))
    .await
    .unwrap();
    execute_command(Command::Run(RunArgs {
        source: SourceKind::Paimon,
        data: None,
        warehouse: warehouse.path().to_path_buf(),
        queries: queries.path().to_path_buf(),
        output: output.clone(),
        database: "tpcds".to_string(),
        query: Some("1".to_string()),
        tables: None,
        warmup: 0,
        iterations: 1,
        runtime,
    }))
    .await
    .unwrap();

    let report: BenchmarkReport = serde_json::from_slice(&fs::read(output).unwrap()).unwrap();
    assert_eq!(report.source, SourceKind::Paimon);
    assert_eq!(report.queries.len(), 1);
    assert!(!report.has_failures());
}
