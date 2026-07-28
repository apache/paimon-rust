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

//! Statistics-driven aggregates over `type=format-table` tables.
//!
//! A format table has no manifest, so scan planning cannot fill in per-file
//! row counts. Those counts must be reported as *unknown*; if the placeholder
//! is reported as an exact statistic instead, DataFusion's
//! `aggregate_statistics` rule answers `COUNT(*)` from it and never opens a
//! single data file — a silent wrong answer.

mod common;

use std::path::Path;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use paimon::catalog::{Catalog, Identifier};
use paimon::spec::{BigIntType, DataType, Schema, SchemaBuilder, VarCharType};
use paimon_datafusion::SQLContext;
use parquet::arrow::ArrowWriter;
use tempfile::TempDir;

const DATABASE: &str = "test_db";
const TABLE: &str = "events";

/// Creates a `type=format-table` table in a filesystem catalog and returns the
/// table directory, where callers drop raw data files.
async fn setup_format_table() -> (TempDir, SQLContext, std::path::PathBuf) {
    setup_table(Schema::builder().column("id", DataType::BigInt(BigIntType::new()))).await
}

/// Same, partitioned by a single `dt` column (Hive layout: the partition value
/// lives in the directory name, not in the data file).
async fn setup_partitioned_format_table() -> (TempDir, SQLContext, std::path::PathBuf) {
    setup_table(
        Schema::builder()
            .column("dt", DataType::VarChar(VarCharType::new(32).unwrap()))
            .column("id", DataType::BigInt(BigIntType::new()))
            .partition_keys(vec!["dt".to_string()]),
    )
    .await
}

async fn setup_table(builder: SchemaBuilder) -> (TempDir, SQLContext, std::path::PathBuf) {
    let (tmp, catalog) = common::create_test_env();
    catalog
        .create_database(DATABASE, false, Default::default())
        .await
        .expect("CREATE DATABASE failed");
    let schema = builder
        .option("type", "format-table")
        .option("file.format", "parquet")
        .build()
        .unwrap();
    catalog
        .create_table(&Identifier::new(DATABASE, TABLE), schema, false)
        .await
        .expect("CREATE TABLE failed");
    let table_dir = tmp.path().join(format!("{DATABASE}.db")).join(TABLE);
    let context = common::create_sql_context(catalog).await;
    (tmp, context, table_dir)
}

fn write_parquet(path: &Path, values: &[i64]) {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "id",
        ArrowDataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(values.to_vec()))],
    )
    .unwrap();
    let file = std::fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

async fn scalar_count(context: &SQLContext, sql: &str) -> i64 {
    let batches = context.sql(sql).await.unwrap().collect().await.unwrap();
    let batch = batches
        .iter()
        .find(|batch| batch.num_rows() > 0)
        .expect("aggregate must return one row");
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count column must be int64")
        .value(0)
}

async fn scanned_rows(context: &SQLContext, sql: &str) -> i64 {
    let batches = context.sql(sql).await.unwrap().collect().await.unwrap();
    batches.iter().map(|batch| batch.num_rows() as i64).sum()
}

/// `COUNT(*)` on a format table must agree with what an actual scan returns.
/// Regression for the silent `COUNT(*) = 0`: the placeholder
/// `DataFileMeta::row_count` was reported as an exact statistic.
#[tokio::test]
async fn test_format_table_count_star_matches_scanned_rows() {
    let (_tmp, context, table_dir) = setup_format_table().await;
    write_parquet(&table_dir.join("part-0.parquet"), &[1, 2, 3]);
    write_parquet(&table_dir.join("part-1.parquet"), &[4, 5]);

    let scanned = scanned_rows(&context, "SELECT id FROM paimon.test_db.events").await;
    assert_eq!(scanned, 5, "the scan itself must see all rows");

    let counted = scalar_count(&context, "SELECT COUNT(*) AS c FROM paimon.test_db.events").await;
    assert_eq!(
        counted, scanned,
        "COUNT(*) must not be answered from the placeholder row count"
    );

    let counted_col =
        scalar_count(&context, "SELECT COUNT(id) AS c FROM paimon.test_db.events").await;
    assert_eq!(
        counted_col, scanned,
        "COUNT(col) must not be short-circuited"
    );
}

/// A format table with no data files really has zero rows; reporting the row
/// count as unknown must not turn that into a wrong answer either.
#[tokio::test]
async fn test_empty_format_table_counts_zero() {
    let (_tmp, context, _table_dir) = setup_format_table().await;

    assert_eq!(
        scalar_count(&context, "SELECT COUNT(*) AS c FROM paimon.test_db.events").await,
        0
    );
}

/// The partition-pruned plan reaches `partition_statistics()` too, so a pruned
/// `COUNT(*)` must also come from the data and not from the placeholder.
///
/// Only predicated queries are asserted here. An unfiltered scan over a
/// partitioned format table on a `file://` warehouse currently finds no splits
/// at all — `table_path` keeps the `file:///` form while the listed status
/// paths come back as `file:/`, so the `strip_prefix` in
/// `partition_row_from_path` (paimon/src/table/format_table_scan.rs:487-493)
/// drops every file. That is a separate defect from the one under test.
#[tokio::test]
async fn test_partitioned_format_table_count_with_partition_predicate() {
    let (_tmp, context, table_dir) = setup_partitioned_format_table().await;
    for (dt, values) in [
        ("2026-07-21", vec![1i64, 2, 3]),
        ("2026-07-22", vec![4i64, 5]),
    ] {
        let partition_dir = table_dir.join(format!("dt={dt}"));
        std::fs::create_dir_all(&partition_dir).unwrap();
        write_parquet(&partition_dir.join("part-0.parquet"), &values);
    }

    for (dt, expected) in [("2026-07-21", 3), ("2026-07-22", 2), ("2026-07-23", 0)] {
        let sql = format!("SELECT id FROM paimon.test_db.events WHERE dt = '{dt}'");
        let scanned = scanned_rows(&context, &sql).await;
        assert_eq!(scanned, expected, "scan of dt={dt}");

        let sql = format!("SELECT COUNT(*) AS c FROM paimon.test_db.events WHERE dt = '{dt}'");
        assert_eq!(
            scalar_count(&context, &sql).await,
            expected,
            "count of dt={dt}"
        );
    }
}

/// A data file that genuinely holds zero rows must also count 0.
#[tokio::test]
async fn test_format_table_with_empty_file_counts_zero() {
    let (_tmp, context, table_dir) = setup_format_table().await;
    write_parquet(&table_dir.join("part-0.parquet"), &[]);

    assert_eq!(
        scalar_count(&context, "SELECT COUNT(*) AS c FROM paimon.test_db.events").await,
        0
    );
}
