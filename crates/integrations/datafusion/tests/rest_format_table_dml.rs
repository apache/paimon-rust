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

//! Format Table inserts and the row-level mutations that still need a
//! dedicated copy-on-write implementation.

mod common;

#[path = "../../../paimon/tests/mock_server.rs"]
mod mock_server;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use paimon::api::ConfigResponse;
use paimon::catalog::{Catalog, FileSystemCatalog, Identifier, RESTCatalog};
use paimon::spec::{BigIntType, DataType, Schema, VarCharType};
use paimon::{CatalogOptions, Options};
use paimon_datafusion::SQLContext;
use parquet::arrow::ArrowWriter;
use tempfile::TempDir;

use mock_server::start_mock_server;

const DATABASE: &str = "default";
const TABLE: &str = "events";
const TABLE_NAME: &str = "paimon.default.events";
const WAREHOUSE: &str = "test_warehouse";

/// A Format Table with one file in each of `dt=a` and `dt=b`, whose partitions a mock REST
/// catalog manages. Its data directory is the root of `temp_dir`.
async fn catalog_managed_format_table(temp_dir: &TempDir) -> SQLContext {
    let server = start_mock_server(
        WAREHOUSE.to_string(),
        temp_dir.path().to_string_lossy().into_owned(),
        ConfigResponse::new(HashMap::from([(
            CatalogOptions::PREFIX.to_string(),
            "mock-test".to_string(),
        )])),
        vec![DATABASE.to_string()],
    )
    .await;
    server.add_table_with_schema(
        DATABASE,
        TABLE,
        format_table_schema(true),
        &format!("file://{}", temp_dir.path().display()),
    );
    server.set_table_external(DATABASE, TABLE, false);
    server.set_table_partitions(
        DATABASE,
        TABLE,
        ["a", "b"]
            .map(|value| HashMap::from([("dt".to_string(), value.to_string())]))
            .to_vec(),
    );
    write_ids(&temp_dir.path().join("dt=a"), &[1, 2]);
    write_ids(&temp_dir.path().join("dt=b"), &[3]);

    let mut options = Options::new();
    options.set(CatalogOptions::URI, server.url().unwrap());
    options.set(CatalogOptions::WAREHOUSE, WAREHOUSE);
    options.set(CatalogOptions::TOKEN_PROVIDER, "bear");
    options.set(CatalogOptions::TOKEN, "test-token");
    let catalog = Arc::new(RESTCatalog::new(options, true).await.unwrap());
    let mut context = SQLContext::new();
    context.register_catalog("paimon", catalog).await.unwrap();
    context
}

/// The same table with its partitions discovered from the directory layout, in a filesystem
/// catalog. Its data directory is the table directory below the warehouse.
async fn directory_partitioned_format_table(
    temp_dir: &TempDir,
) -> (std::path::PathBuf, SQLContext) {
    let mut options = Options::new();
    options.set(
        CatalogOptions::WAREHOUSE,
        format!("file:{}", temp_dir.path().display()),
    );
    let catalog = Arc::new(FileSystemCatalog::new(options).unwrap());
    catalog
        .create_database(DATABASE, false, Default::default())
        .await
        .unwrap();
    catalog
        .create_table(
            &Identifier::new(DATABASE, TABLE),
            format_table_schema(false),
            false,
        )
        .await
        .unwrap();
    let table_dir = temp_dir.path().join(format!("{DATABASE}.db")).join(TABLE);
    write_ids(&table_dir.join("dt=a"), &[1, 2]);
    write_ids(&table_dir.join("dt=b"), &[3]);
    let mut context = SQLContext::new();
    context.register_catalog("paimon", catalog).await.unwrap();
    (table_dir, context)
}

fn format_table_schema(catalog_managed: bool) -> Schema {
    let mut builder = Schema::builder()
        .column("dt", DataType::VarChar(VarCharType::new(255).unwrap()))
        .column("id", DataType::BigInt(BigIntType::new()))
        .partition_keys(["dt"])
        .option("type", "format-table")
        .option("file.format", "parquet");
    if catalog_managed {
        builder = builder.option("metastore.partitioned-table", "true");
    }
    builder.build().unwrap()
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

/// Every file below `root`, so a statement that wrote one cannot pass as a statement that failed.
fn files(root: &Path) -> Vec<String> {
    let mut paths = Vec::new();
    let mut frontier = vec![root.to_path_buf()];
    while let Some(directory) = frontier.pop() {
        for entry in std::fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                frontier.push(path);
            } else {
                paths.push(
                    path.strip_prefix(root)
                        .unwrap()
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        }
    }
    paths.sort();
    paths
}

async fn ids(context: &SQLContext, table_name: &str) -> Vec<i64> {
    let batches = context
        .sql(&format!("SELECT id FROM {table_name}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut values = Vec::new();
    for batch in batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        values.extend(column.iter().flatten());
    }
    values.sort_unstable();
    values
}

fn statements(table_name: &str) -> [String; 3] {
    [
        format!("DELETE FROM {table_name} WHERE id = 1"),
        format!("UPDATE {table_name} SET id = 7 WHERE id = 1"),
        format!(
            "MERGE INTO {table_name} t USING (SELECT 1 AS id) s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET id = 7"
        ),
    ]
}

#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_row_level_dml_is_refused_on_a_catalog_managed_format_table() {
    let temp_dir = tempfile::tempdir().unwrap();
    let context = catalog_managed_format_table(&temp_dir).await;
    let seeded = files(temp_dir.path());

    for statement in statements(TABLE_NAME) {
        // A rewrite used to leave Paimon bucket files behind, which the scan then read as data.
        common::assert_sql_error(&context, &statement, "format table").await;
        assert_eq!(files(temp_dir.path()), seeded, "{statement}");
        assert_eq!(ids(&context, TABLE_NAME).await, [1, 2, 3], "{statement}");
    }
}

#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_row_level_dml_is_refused_on_a_format_table_without_catalog_managed_partitions() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (table_dir, context) = directory_partitioned_format_table(&temp_dir).await;
    let table_name = format!("paimon.{DATABASE}.{TABLE}");
    let seeded = files(&table_dir);

    for statement in statements(&table_name) {
        common::assert_sql_error(&context, &statement, "format table").await;
        assert_eq!(files(&table_dir), seeded, "{statement}");
        assert_eq!(ids(&context, &table_name).await, [1, 2, 3], "{statement}");
    }
}

#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_insert_into_catalog_managed_format_table_registers_partition() {
    let temp_dir = tempfile::tempdir().unwrap();
    let context = catalog_managed_format_table(&temp_dir).await;
    context
        .sql(&format!(
            "INSERT INTO {TABLE_NAME} (dt, id) VALUES ('a', 10), ('c', 11)"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(ids(&context, TABLE_NAME).await, [1, 2, 3, 10, 11]);
    assert_eq!(
        files(&temp_dir.path().join("dt=c"))
            .iter()
            .filter(|file| file.ends_with(".parquet"))
            .count(),
        1
    );
}

#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_insert_into_directory_format_table_publishes_visible_files() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (table_dir, context) = directory_partitioned_format_table(&temp_dir).await;
    let table_name = format!("paimon.{DATABASE}.{TABLE}");
    context
        .sql(&format!(
            "INSERT INTO {table_name} (dt, id) VALUES ('b', 10), ('c', 11)"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(ids(&context, &table_name).await, [1, 2, 3, 10, 11]);
    assert_eq!(
        files(&table_dir.join("dt=b"))
            .iter()
            .filter(|file| file.ends_with(".parquet"))
            .count(),
        2
    );
    assert_eq!(
        files(&table_dir.join("dt=c"))
            .iter()
            .filter(|file| file.ends_with(".parquet"))
            .count(),
        1
    );
}

#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_overwrite_only_replaces_touched_catalog_partition() {
    let temp_dir = tempfile::tempdir().unwrap();
    let context = catalog_managed_format_table(&temp_dir).await;
    context
        .sql(&format!(
            "INSERT OVERWRITE {TABLE_NAME} VALUES ('a', 10), ('a', 11)"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(ids(&context, TABLE_NAME).await, [3, 10, 11]);
    assert_eq!(
        files(&temp_dir.path().join("dt=a"))
            .iter()
            .filter(|file| file.ends_with(".parquet"))
            .count(),
        1
    );
    assert_eq!(
        files(&temp_dir.path().join("dt=b"))
            .iter()
            .filter(|file| file.ends_with(".parquet"))
            .count(),
        1
    );
}

#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_static_partition_overwrite_with_empty_source_keeps_other_data() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (_table_dir, context) = directory_partitioned_format_table(&temp_dir).await;
    let table_name = format!("paimon.{DATABASE}.{TABLE}");
    context
        .sql(&format!(
            "INSERT OVERWRITE {table_name} PARTITION (dt = 'a') \
             SELECT CAST(0 AS BIGINT) AS id WHERE FALSE"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(ids(&context, &table_name).await, [3]);
}
