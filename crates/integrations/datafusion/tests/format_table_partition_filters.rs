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

//! Filters on the partition columns of a `type=format-table` table.
//!
//! A format table keeps partition values in its directory names only; the data
//! files do not hold those columns. A filter the scan cannot turn into a
//! partition predicate still has to see the value from the directory name.

use std::path::Path;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use paimon::catalog::{Catalog, FileSystemCatalog, Identifier};
use paimon::spec::{BigIntType, BooleanType, DataType, Schema, VarCharType};
use paimon::{CatalogOptions, Options};
use paimon_datafusion::SQLContext;
use parquet::arrow::ArrowWriter;
use tempfile::TempDir;

const DATABASE: &str = "test_db";
const TABLE: &str = "events";

/// A format table partitioned by `(dt, active)` with one data file per partition.
///
/// The warehouse is given as `file:/...`, the form in which listed file paths come back,
/// so that a scan listing from the table root can relate each file to its partition
/// directory.
async fn setup_table() -> (TempDir, SQLContext) {
    let tmp = TempDir::new().expect("Failed to create temp dir");
    let mut options = Options::new();
    options.set(
        CatalogOptions::WAREHOUSE,
        format!("file:{}", tmp.path().display()),
    );
    let catalog = Arc::new(FileSystemCatalog::new(options).expect("Failed to create catalog"));
    catalog
        .create_database(DATABASE, false, Default::default())
        .await
        .expect("CREATE DATABASE failed");
    let schema = Schema::builder()
        .column("dt", DataType::VarChar(VarCharType::new(32).unwrap()))
        .column("active", DataType::Boolean(BooleanType::new()))
        .column("id", DataType::BigInt(BigIntType::new()))
        .partition_keys(vec!["dt".to_string(), "active".to_string()])
        .option("type", "format-table")
        .option("file.format", "parquet")
        .build()
        .unwrap();
    catalog
        .create_table(&Identifier::new(DATABASE, TABLE), schema, false)
        .await
        .expect("CREATE TABLE failed");
    let table_dir = tmp.path().join(format!("{DATABASE}.db")).join(TABLE);
    for (dt, active, id) in [("a", true, 1), ("b", false, 2)] {
        write_ids(&table_dir.join(format!("dt={dt}/active={active}")), &[id]);
    }
    let mut context = SQLContext::new();
    context.register_catalog("paimon", catalog).await.unwrap();
    (tmp, context)
}

fn write_ids(directory: &Path, ids: &[i64]) {
    std::fs::create_dir_all(directory).unwrap();
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "id",
        ArrowDataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(ids.to_vec()))],
    )
    .unwrap();
    let file = std::fs::File::create(directory.join("part-0.parquet")).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

async fn ids(context: &SQLContext, sql: &str) -> Vec<i64> {
    let mut ids = Vec::new();
    for batch in context.sql(sql).await.unwrap().collect().await.unwrap() {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        ids.extend(values.iter().flatten());
    }
    ids.sort_unstable();
    ids
}

/// A filter on a partition column that is not a partition predicate, such as a
/// bare boolean column (DataFusion simplifies `active = true` to `active`) or a
/// function over the column, must be evaluated with the value from the
/// directory name rather than dropping every row as a missing column.
#[tokio::test]
async fn test_filter_on_a_partition_column_reads_the_directory_value() {
    let (_tmp, context) = setup_table().await;

    let mut mismatches = Vec::new();
    for (predicate, expected) in [
        ("dt IN ('a', 'b')", vec![1, 2]),
        ("active", vec![1]),
        ("active = true", vec![1]),
        ("NOT active", vec![2]),
        ("upper(dt) = 'B'", vec![2]),
        ("concat(dt, '-') = 'a-'", vec![1]),
    ] {
        let actual = ids(
            &context,
            &format!("SELECT id FROM paimon.{DATABASE}.{TABLE} WHERE {predicate}"),
        )
        .await;
        if actual != expected {
            mismatches.push(format!(
                "{predicate}: expected {expected:?}, got {actual:?}"
            ));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
}
