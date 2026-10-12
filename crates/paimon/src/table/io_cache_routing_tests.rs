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

//! io-cache routing of the files that FileSystemCatalog tables and their writers produce.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use bytes::Bytes;
use futures::TryStreamExt;

use super::Table;
use crate::catalog::{Catalog, FileSystemCatalog, Identifier, LoadedTable};
use crate::common::{CatalogOptions, Options};
use crate::io::oss_test_server::TestOss;
use crate::io::FileIOBuilder;
use crate::spec::{DataType, IntType, Schema};

const WAREHOUSE: &str = "bkt/warehouse";
const FILE_NAME: &str = "data-123e4567-e89b-12d3-a456-426614174000-1.parquet";

/// Options that make `cache` the only io-cache target and send everything else to `origin`.
fn io_cache_options(origin: &TestOss, cache: &TestOss, options: &[(&str, &str)]) -> Options {
    let mut io_cache_options = Options::new();
    io_cache_options.set(CatalogOptions::WAREHOUSE, format!("oss://{WAREHOUSE}"));
    for (key, value) in [
        ("fs.oss.endpoint", origin.endpoint()),
        ("fs.oss.accessKeyId", "ak"),
        ("fs.oss.accessKeySecret", "sk"),
        ("fs.oss.path-style-access", "true"),
        ("io-cache.enabled", "true"),
        ("io-cache.endpoint", cache.endpoint()),
        ("io-cache.target.default.path-style-access", "true"),
    ]
    .iter()
    .chain(options)
    {
        io_cache_options.set(*key, *value);
    }
    io_cache_options
}

async fn catalog(origin: &TestOss, cache: &TestOss, options: &[(&str, &str)]) -> FileSystemCatalog {
    let catalog = FileSystemCatalog::new(io_cache_options(origin, cache, options)).unwrap();
    catalog
        .create_database("db", false, HashMap::new())
        .await
        .unwrap();
    catalog
}

async fn create_table(catalog: &FileSystemCatalog, name: &str, schema: Schema) -> Table {
    let identifier = Identifier::new("db", name);
    catalog
        .create_table(&identifier, schema, false)
        .await
        .unwrap();
    catalog.get_table(&identifier).await.unwrap()
}

/// Writes `content` to `path` on `server` only, so the other endpoint keeps what it had.
async fn put(server: &TestOss, path: &str, content: Bytes) {
    let file_io = FileIOBuilder::new("oss")
        .with_props([
            ("fs.oss.endpoint", server.endpoint()),
            ("fs.oss.accessKeyId", "ak"),
            ("fs.oss.accessKeySecret", "sk"),
            ("fs.oss.path-style-access", "true"),
        ])
        .build()
        .unwrap();
    file_io
        .new_output(path)
        .unwrap()
        .write(content)
        .await
        .unwrap();
}

fn parquet(ids: &[i32]) -> Bytes {
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "id",
        ArrowDataType::Int32,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(ids.to_vec()))],
    )
    .unwrap();
    let mut bytes = Vec::new();
    let mut writer = parquet::arrow::ArrowWriter::try_new(&mut bytes, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    Bytes::from(bytes)
}

async fn read(table: &Table) -> Vec<RecordBatch> {
    let builder = table.new_read_builder();
    let plan = builder.new_scan().plan().await.unwrap();
    builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

async fn ids(table: &Table) -> Vec<i32> {
    let mut ids: Vec<i32> = read(table)
        .await
        .iter()
        .flat_map(|batch| {
            let column = batch.column_by_name("id").unwrap();
            let column = column.as_any().downcast_ref::<Int32Array>().unwrap();
            column.values().to_vec()
        })
        .collect();
    ids.sort();
    ids
}

#[tokio::test]
async fn format_table_reads_replaced_files_from_origin() {
    let origin = TestOss::start().await;
    // Its own storage stands in for a cache that kept the file before origin replaced it.
    let cache = TestOss::start().await;
    let policy = [("io-cache.policy", "meta,read")];
    let catalog = catalog(&origin, &cache, &policy).await;
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .option("type", "format-table")
        .option("file.format", "parquet")
        .build()
        .unwrap();
    let table = create_table(&catalog, "external", schema).await;

    // An upstream job replaced the file under the same, Paimon-shaped name.
    let file = format!("{}/{FILE_NAME}", table.location());
    put(&cache, &file, parquet(&[7])).await;
    put(&origin, &file, parquet(&[9])).await;
    cache.take_requests();

    assert_eq!(ids(&table).await, [9]);
    let copy = table.copy_with_options(HashMap::from([(
        "read.batch-size".to_string(),
        "16".to_string(),
    )]));
    assert_eq!(ids(&copy).await, [9]);
    // A caller that resolved the schema itself gets the same view of the catalog's FileIO.
    let file_io = FileIOBuilder::new("oss")
        .with_props(io_cache_options(&origin, &cache, &policy).to_map().iter())
        .build()
        .unwrap();
    let resolved = Table::from_resolved_schema(
        file_io,
        table.identifier().clone(),
        table.location().to_string(),
        table.schema().clone(),
        "main",
    )
    .unwrap();
    assert_eq!(ids(&resolved).await, [9]);
    assert_eq!(cache.take_requests(), Vec::<String>::new());
}

#[tokio::test]
async fn object_table_reads_objects_from_origin() {
    let origin = TestOss::start().await;
    let cache = TestOss::start().await;
    let catalog = catalog(&origin, &cache, &[("io-cache.policy", "meta,read")]).await;
    let identifier = Identifier::new("db", "objects");
    let schema = Schema::builder()
        .option("type", "object-table")
        .build()
        .unwrap();
    catalog
        .create_table(&identifier, schema, false)
        .await
        .unwrap();
    let LoadedTable::Object(table) = catalog.load_table(&identifier).await.unwrap() else {
        panic!("expected an object table");
    };

    let object = format!("{}/{FILE_NAME}", table.location());
    put(&cache, &object, Bytes::from("old")).await;
    put(&origin, &object, Bytes::from("new")).await;
    cache.take_requests();

    let input = table.file_io().new_input(&object).unwrap();
    assert_eq!(input.read().await.unwrap(), "new");
    assert_eq!(input.metadata().await.unwrap().size, 3);
    let objects = table.list_objects().await.unwrap();
    assert!(
        objects.iter().any(|entry| entry.name() == FILE_NAME),
        "{objects:?}"
    );
    assert_eq!(cache.take_requests(), Vec::<String>::new());
}
