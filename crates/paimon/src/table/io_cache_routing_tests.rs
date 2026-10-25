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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow_array::{Array, Int32Array, LargeBinaryArray, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};
use bytes::Bytes;
use futures::TryStreamExt;

use super::table_write::tests::make_batch;
use super::{BlobReader, BranchManager, RESTEnv, Table, TableCommit, TableWrite};
use crate::api::rest_api::RESTApi;
use crate::api::{GetTableResponse, GetTableTokenResponse};
use crate::catalog::{Catalog, FileSystemCatalog, FusePathResolver, Identifier, LoadedTable};
use crate::common::{CatalogOptions, Options};
use crate::io::oss_test_server::TestOss;
use crate::io::{FileIOBuilder, FileIOCacheContext};
use crate::spec::{
    BlobDescriptor, BlobType, DataType, IntType, Schema, SchemaBuilder, SchemaChange,
};

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

/// Options of a client without io-cache routing, which only talks to `server`.
fn plain_options(server: &TestOss) -> [(&str, &str); 4] {
    [
        ("fs.oss.endpoint", server.endpoint()),
        ("fs.oss.accessKeyId", "ak"),
        ("fs.oss.accessKeySecret", "sk"),
        ("fs.oss.path-style-access", "true"),
    ]
}

/// Writes `content` to `path` on `server` only, so the other endpoint keeps what it had.
async fn put(server: &TestOss, path: &str, content: Bytes) {
    let file_io = FileIOBuilder::new("oss")
        .with_props(plain_options(server))
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

/// Routing that lets data files be written to and read from the target.
const DATA_TARGET: [(&str, &str); 2] = [
    ("io-cache.policy", "meta,read,write"),
    ("io-cache.whitelist", "data"),
];

/// A catalog whose data files may be written to and read from the target.
async fn data_target_catalog(origin: &TestOss, cache: &TestOss) -> FileSystemCatalog {
    catalog(origin, cache, &DATA_TARGET).await
}

fn int_schema() -> SchemaBuilder {
    Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
}

/// Writes and commits `batch`, returning the names of the new data files.
async fn write_and_commit(table: &Table, batch: &RecordBatch) -> Vec<String> {
    let mut write = TableWrite::new(table, "io-cache-user".to_string()).unwrap();
    write.write_arrow_batch(batch).await.unwrap();
    let messages = write.prepare_commit().await.unwrap();
    let files = messages
        .iter()
        .flat_map(|message| message.new_files.iter().map(|file| file.file_name.clone()))
        .collect();
    TableCommit::new(table.clone(), "io-cache-user".to_string())
        .commit(messages)
        .await
        .unwrap();
    files
}

/// Methods of the requests on keys containing `fragment`, in order.
fn methods(requests: &[String], fragment: &str) -> Vec<String> {
    requests
        .iter()
        .filter(|request| request.contains(fragment))
        .map(|request| request.split_once(' ').unwrap().0.to_string())
        .collect()
}

#[track_caller]
fn assert_reads_and_writes(requests: &[String], fragment: &str) {
    let methods = methods(requests, fragment);
    assert!(
        methods.iter().any(|m| m == "PUT"),
        "{fragment}: {methods:?}"
    );
    assert!(
        methods.iter().any(|m| m == "GET"),
        "{fragment}: {methods:?}"
    );
}

#[tokio::test]
async fn data_files_named_with_the_schema_prefix_use_the_target() {
    let origin = TestOss::start().await;
    let cache = TestOss::start_sharing(&origin).await;
    let catalog = data_target_catalog(&origin, &cache).await;
    // The prefix is only in the table schema, not in the catalog options.
    let schema = int_schema()
        .option("data-file.prefix", "custom-")
        .build()
        .unwrap();
    let table = create_table(&catalog, "custom_prefix", schema).await;

    let files = write_and_commit(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
    assert!(
        files.iter().all(|file| file.starts_with("custom-")),
        "{files:?}"
    );
    assert_eq!(ids(&table).await, [1, 2]);
    assert_reads_and_writes(&cache.take_requests(), "/custom-");
    assert_eq!(
        methods(&origin.take_requests(), "/custom-"),
        Vec::<String>::new()
    );

    // A dynamic prefix adds to the table's own, so both sets of files keep using the target.
    let dynamic = table.copy_with_options(HashMap::from([(
        "data-file.prefix".to_string(),
        "dynamic-".to_string(),
    )]));
    let files = write_and_commit(&dynamic, &make_batch(vec![3], vec![30])).await;
    assert!(
        files.iter().all(|file| file.starts_with("dynamic-")),
        "{files:?}"
    );
    assert_eq!(ids(&dynamic).await, [1, 2, 3]);
    let requests = cache.take_requests();
    assert_reads_and_writes(&requests, "/dynamic-");
    assert!(methods(&requests, "/custom-").iter().any(|m| m == "GET"));
    let requests = origin.take_requests();
    assert_eq!(methods(&requests, "/custom-"), Vec::<String>::new());
    assert_eq!(methods(&requests, "/dynamic-"), Vec::<String>::new());
}

#[tokio::test]
async fn postpone_bucket_files_use_the_target() {
    let origin = TestOss::start().await;
    let cache = TestOss::start_sharing(&origin).await;
    let catalog = data_target_catalog(&origin, &cache).await;
    let schema = int_schema()
        .primary_key(["id"])
        .option("bucket", "-2")
        .build()
        .unwrap();
    let table = create_table(&catalog, "postpone", schema).await;

    let files = write_and_commit(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
    let [file] = files.as_slice() else {
        panic!("expected one postpone file, got {files:?}");
    };
    assert!(file.starts_with("data--u-io-cache-user-s-"), "{file}");
    // Postpone files are read once compaction assigns their rows to buckets.
    let path = format!("{}/bucket-postpone/{file}", table.location());
    table
        .file_io()
        .new_input(&path)
        .unwrap()
        .read()
        .await
        .unwrap();

    assert_eq!(methods(&cache.take_requests(), file), ["PUT", "GET"]);
    assert_eq!(methods(&origin.take_requests(), file), Vec::<String>::new());
}

fn blob_schema() -> SchemaBuilder {
    Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("payload", DataType::Blob(BlobType::new()))
}

/// A primary-key table, which copies its BLOBs into managed packs.
fn pk_blob_schema() -> SchemaBuilder {
    blob_schema()
        .primary_key(["id"])
        .option("bucket", "1")
        .option("file.format", "parquet")
}

fn blob_batch(ids: &[i32], payloads: &[&[u8]]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("payload", ArrowDataType::LargeBinary, true),
        ])),
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(LargeBinaryArray::from(payloads.to_vec())),
        ],
    )
    .unwrap()
}

async fn payloads(table: &Table) -> Vec<Vec<u8>> {
    read(table)
        .await
        .iter()
        .flat_map(|batch| {
            let column = batch.column_by_name("payload").unwrap();
            let column = column.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            (0..column.len())
                .map(|row| column.value(row).to_vec())
                .collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test]
async fn managed_blob_packs_use_the_target() {
    let origin = TestOss::start().await;
    let cache = TestOss::start_sharing(&origin).await;
    let catalog = data_target_catalog(&origin, &cache).await;
    let table = create_table(&catalog, "managed_blob", pk_blob_schema().build().unwrap()).await;

    write_and_commit(&table, &blob_batch(&[1, 2], &[b"hello", b"world"])).await;
    assert_eq!(
        payloads(&table).await,
        [b"hello".to_vec(), b"world".to_vec()]
    );

    assert_reads_and_writes(&cache.take_requests(), ".managed.blob");
    assert_eq!(
        methods(&origin.take_requests(), ".managed.blob"),
        Vec::<String>::new()
    );
}

#[tokio::test]
async fn blob_descriptor_sources_are_read_from_origin() {
    let origin = TestOss::start().await;
    let cache = TestOss::start_sharing(&origin).await;
    let catalog = data_target_catalog(&origin, &cache).await;
    let mut plain = Options::new();
    plain.set(CatalogOptions::WAREHOUSE, format!("oss://{WAREHOUSE}"));
    for (key, value) in plain_options(&origin) {
        plain.set(key, value);
    }
    let origin_catalog = FileSystemCatalog::new(plain).unwrap();
    // Primary-key tables copy descriptor sources into managed packs, append tables into .blob files.
    let append_blob_schema = blob_schema()
        .option("data-evolution.enabled", "true")
        .option("row-tracking.enabled", "true");
    for (name, schema) in [
        ("pk_blob", pk_blob_schema()),
        ("append_blob", append_blob_schema),
    ] {
        let table = create_table(&catalog, name, schema.build().unwrap()).await;
        // An import job owns the source; the target kept the copy from before origin replaced it.
        let source =
            format!("oss://bkt/import/{name}/data-123e4567-e89b-12d3-a456-426614174000-1.png");
        let key = source.strip_prefix("oss://").unwrap();
        put(&origin, &source, Bytes::from("NEW")).await;
        cache.retain(key, Bytes::from("OLD"));
        origin.take_requests();
        cache.take_requests();

        let descriptor = BlobDescriptor::new(source.clone(), 0, 3).serialize();
        write_and_commit(&table, &blob_batch(&[1], &[&descriptor])).await;
        let (origin_requests, cache_requests) = (origin.take_requests(), cache.take_requests());
        assert_eq!(methods(&origin_requests, key), ["GET"], "{name}");
        assert_eq!(
            methods(&cache_requests, key),
            Vec::<String>::new(),
            "{name}"
        );
        // The copy is a new immutable file of the table, so it is still written to the target.
        let blob_puts = |requests: &[String]| {
            requests
                .iter()
                .filter(|request| request.starts_with("PUT ") && request.ends_with(".blob"))
                .count()
        };
        assert_eq!(blob_puts(&cache_requests), 1, "{name}: {cache_requests:?}");
        assert_eq!(
            blob_puts(&origin_requests),
            0,
            "{name}: {origin_requests:?}"
        );

        assert_eq!(payloads(&table).await, [b"NEW".to_vec()], "{name}");
        let identifier = Identifier::new("db", name);
        let origin_table = origin_catalog.get_table(&identifier).await.unwrap();
        assert_eq!(payloads(&origin_table).await, [b"NEW".to_vec()], "{name}");
    }
}

/// Creates `name` with `data-file.prefix=older-`, commits a row, then persists `newer-`.
async fn table_with_altered_prefix(catalog: &FileSystemCatalog, name: &str) -> (Table, String) {
    let schema = int_schema()
        .option("data-file.prefix", "older-")
        .build()
        .unwrap();
    let table = create_table(catalog, name, schema).await;
    let files = write_and_commit(&table, &make_batch(vec![1], vec![10])).await;
    let [file] = files.as_slice() else {
        panic!("expected one data file, got {files:?}");
    };
    assert!(file.starts_with("older-"), "{file}");
    let change = SchemaChange::set_option("data-file.prefix".to_string(), "newer-".to_string());
    catalog
        .alter_table(table.identifier(), vec![change], false)
        .await
        .unwrap();
    (table, file.clone())
}

/// The request that lists the schemas of `table`.
fn schema_listing(table: &Table) -> String {
    format!(
        "LIST {}/schema/",
        table.location().strip_prefix("oss://").unwrap()
    )
}

#[tokio::test]
async fn altered_data_file_prefix_keeps_routing_after_reloads() {
    let origin = TestOss::start().await;
    let cache = TestOss::start_sharing(&origin).await;
    let catalog = data_target_catalog(&origin, &cache).await;
    let (table, older) = table_with_altered_prefix(&catalog, "altered_prefix").await;
    let identifier = table.identifier().clone();

    // Fresh loads only see the latest schema in their options, as after a restart.
    let LoadedTable::Paimon(table) = catalog.load_table(&identifier).await.unwrap() else {
        panic!("expected a Paimon table");
    };
    let newer = write_and_commit(&table, &make_batch(vec![2], vec![20])).await;
    assert!(
        newer.iter().all(|file| file.starts_with("newer-")),
        "{newer:?}"
    );
    let restarted =
        FileSystemCatalog::new(io_cache_options(&origin, &cache, &DATA_TARGET)).unwrap();
    let table = restarted.get_table(&identifier).await.unwrap();
    cache.take_requests();
    origin.take_requests();

    assert_eq!(ids(&table).await, [1, 2]);
    let (cache_requests, origin_requests) = (cache.take_requests(), origin.take_requests());
    // The first request that may use io-cache loads the schemas, from origin.
    let listing = schema_listing(&table);
    assert!(origin_requests.contains(&listing), "{origin_requests:?}");
    assert!(!cache_requests.contains(&listing), "{cache_requests:?}");
    for file in newer.iter().chain([&older]) {
        let methods_on_target = methods(&cache_requests, file);
        assert!(
            methods_on_target.iter().any(|m| m == "GET"),
            "{file}: {methods_on_target:?}"
        );
        assert_eq!(
            methods(&origin_requests, file),
            Vec::<String>::new(),
            "{file}"
        );
    }

    // Without io-cache routing a table never loads its schema history.
    let mut plain = Options::new();
    plain.set(CatalogOptions::WAREHOUSE, format!("oss://{WAREHOUSE}"));
    for (key, value) in plain_options(&origin) {
        plain.set(key, value);
    }
    let table = FileSystemCatalog::new(plain)
        .unwrap()
        .get_table(&identifier)
        .await
        .unwrap();
    origin.take_requests();
    assert_eq!(ids(&table).await, [1, 2]);
    let origin_requests = origin.take_requests();
    assert!(!origin_requests.contains(&listing), "{origin_requests:?}");
}

/// The token a REST catalog vends, and the number of token requests it served.
type TokenState = (Arc<HashMap<String, String>>, Arc<AtomicUsize>);

async fn io_cache_token(
    State((token, requests)): State<TokenState>,
) -> Json<GetTableTokenResponse> {
    requests.fetch_add(1, Ordering::SeqCst);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    Json(GetTableTokenResponse {
        token: token.as_ref().clone(),
        expires_at_millis: Some(now + 24 * 3_600_000),
    })
}

#[tokio::test]
async fn rest_tables_keep_routing_for_prefixes_of_older_schemas() {
    let origin = TestOss::start().await;
    let cache = TestOss::start_sharing(&origin).await;
    let catalog = data_target_catalog(&origin, &cache).await;
    let (table, older) = table_with_altered_prefix(&catalog, "rest_prefix").await;
    let older = format!("{}/bucket-0/{older}", table.location());

    // The REST catalog vends io-cache routing with the table token and returns the latest schema.
    let token: HashMap<String, String> = io_cache_options(&origin, &cache, &DATA_TARGET)
        .to_map()
        .clone();
    let token_requests = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route(
            "/v1/databases/db/tables/rest_prefix/token",
            get(io_cache_token),
        )
        .with_state((Arc::new(token), token_requests.clone()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut options = Options::new();
    options.set(CatalogOptions::URI, format!("http://{address}"));
    options.set(CatalogOptions::TOKEN_PROVIDER, "bear");
    options.set(CatalogOptions::TOKEN, "test-token");
    let api = Arc::new(RESTApi::new(options.clone(), false).await.unwrap());
    let mut response: GetTableResponse = serde_json::from_value(serde_json::json!({
        "id": "rest-prefix-uuid",
        "database": "db",
        "name": "rest_prefix",
        "path": table.location(),
        "isExternal": false,
        "schemaId": 1,
    }))
    .unwrap();
    let latest = int_schema()
        .option("data-file.prefix", "newer-")
        .build()
        .unwrap();
    response.schema = Some(latest);
    let fuse_resolver = Arc::new(FusePathResolver::new(api.options()));
    let file_io_cache = FileIOCacheContext::from_props(options.to_map()).unwrap();
    origin.take_requests();
    cache.take_requests();
    let rest_table = RESTEnv::build_table(
        table.identifier(),
        response,
        api,
        options,
        true,
        None,
        file_io_cache,
        fuse_resolver,
    )
    .await
    .unwrap();
    // Loading reads no schemas; its only token request is the one RESTTokenFileIO makes anyway.
    assert_eq!(origin.take_requests(), Vec::<String>::new());
    assert_eq!(cache.take_requests(), Vec::<String>::new());
    assert_eq!(token_requests.load(Ordering::SeqCst), 1);

    let input = rest_table.file_io().new_input(&older).unwrap();
    input.read().await.unwrap();
    let file = older.rsplit('/').next().unwrap();
    assert_eq!(methods(&cache.take_requests(), file), ["GET"]);
    let origin_requests = origin.take_requests();
    assert_eq!(methods(&origin_requests, file), Vec::<String>::new());
    assert!(
        origin_requests.contains(&schema_listing(&table)),
        "{origin_requests:?}"
    );
    assert_eq!(token_requests.load(Ordering::SeqCst), 1);
    server.abort();
}

#[tokio::test]
async fn blob_descriptor_fields_read_their_sources_from_origin() {
    let origin = TestOss::start().await;
    let cache = TestOss::start_sharing(&origin).await;
    let catalog = data_target_catalog(&origin, &cache).await;
    // The table keeps the descriptors; a ROW sidecar also copies the payloads when writing.
    for (name, row_sidecar) in [
        ("descriptor_field", "false"),
        ("descriptor_sidecar", "true"),
    ] {
        let schema = blob_schema()
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true")
            .option("blob-descriptor-field", "payload")
            .option("data-evolution.row-sidecar.enabled", row_sidecar)
            .build()
            .unwrap();
        let table = create_table(&catalog, name, schema).await;
        let source =
            format!("oss://bkt/import/{name}/data-123e4567-e89b-12d3-a456-426614174000-1.png");
        let key = source.strip_prefix("oss://").unwrap();
        put(&origin, &source, Bytes::from("NEW")).await;
        cache.retain(key, Bytes::from("OLD"));
        origin.take_requests();
        cache.take_requests();

        let descriptor = BlobDescriptor::new(source.clone(), 0, 3).serialize();
        let files = write_and_commit(&table, &blob_batch(&[1], &[&descriptor])).await;
        assert_eq!(payloads(&table).await, [b"NEW".to_vec()], "{name}");
        let (origin_requests, cache_requests) = (origin.take_requests(), cache.take_requests());
        let source_reads = methods(&origin_requests, key);
        assert!(
            source_reads.iter().all(|m| m == "GET"),
            "{name}: {source_reads:?}"
        );
        assert!(!source_reads.is_empty(), "{name}");
        assert_eq!(
            methods(&cache_requests, key),
            Vec::<String>::new(),
            "{name}"
        );
        // The table's own data files keep using the target.
        for file in &files {
            assert_reads_and_writes(&cache_requests, file);
            assert_eq!(
                methods(&origin_requests, file),
                Vec::<String>::new(),
                "{file}"
            );
        }
    }
}

#[tokio::test]
async fn primary_key_descriptor_fields_read_their_sources_from_origin() {
    let origin = TestOss::start().await;
    let cache = TestOss::start_sharing(&origin).await;
    let catalog = data_target_catalog(&origin, &cache).await;
    // `payload` keeps descriptors of an import job's objects; `picture` is copied into managed packs.
    let schema = pk_blob_schema()
        .column("picture", DataType::Blob(BlobType::new()))
        .option("blob-descriptor-field", "payload")
        .build()
        .unwrap();
    let table = create_table(&catalog, "pk_descriptor_field", schema).await;
    let source = "oss://bkt/import/pk/data-123e4567-e89b-12d3-a456-426614174000-1.png";
    let key = source.strip_prefix("oss://").unwrap();
    put(&origin, source, Bytes::from("NEW")).await;
    cache.retain(key, Bytes::from("OLD"));
    let descriptor = BlobDescriptor::new(source.to_string(), 0, 3).serialize();
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("payload", ArrowDataType::LargeBinary, true),
            ArrowField::new("picture", ArrowDataType::LargeBinary, true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(LargeBinaryArray::from(vec![descriptor.as_slice()])),
            Arc::new(LargeBinaryArray::from(vec![b"pixels".as_slice()])),
        ],
    )
    .unwrap();
    write_and_commit(&table, &batch).await;
    origin.take_requests();
    cache.take_requests();

    let batches = read(&table).await;
    let values = |name: &str| {
        batches
            .iter()
            .flat_map(|batch| {
                let column = batch.column_by_name(name).unwrap();
                let column = column.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                (0..column.len())
                    .map(|row| column.value(row).to_vec())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(values("payload"), [b"NEW".to_vec()]);
    assert_eq!(values("picture"), [b"pixels".to_vec()]);
    let (origin_requests, cache_requests) = (origin.take_requests(), cache.take_requests());
    assert_eq!(methods(&cache_requests, key), Vec::<String>::new());
    assert!(methods(&origin_requests, key).iter().any(|m| m == "GET"));
    // The table's own managed packs keep using the target.
    assert!(methods(&cache_requests, ".managed.blob")
        .iter()
        .any(|m| m == "GET"));
    assert_eq!(
        methods(&origin_requests, ".managed.blob"),
        Vec::<String>::new()
    );
}

#[tokio::test]
async fn blob_readers_read_objects_outside_the_table_from_origin() {
    let origin = TestOss::start().await;
    let cache = TestOss::start_sharing(&origin).await;
    let catalog = data_target_catalog(&origin, &cache).await;
    let table = create_table(&catalog, "blob_reader", pk_blob_schema().build().unwrap()).await;
    let owned = format!(
        "{}/bucket-0/data-123e4567-e89b-12d3-a456-426614174000.managed.blob",
        table.location()
    );
    put(&origin, &owned, Bytes::from("OWN")).await;
    let external = "oss://bkt/import/reader/data-123e4567-e89b-12d3-a456-426614174000-1.png";
    let key = external.strip_prefix("oss://").unwrap();
    put(&origin, external, Bytes::from("NEW")).await;
    cache.retain(key, Bytes::from("OLD"));
    let descriptors = [owned.as_str(), external]
        .map(|uri| BlobDescriptor::new(uri.to_string(), 0, 3).serialize());
    let expected = [b"OWN".to_vec(), b"NEW".to_vec()];
    let owned_key = owned.strip_prefix("oss://").unwrap();
    origin.take_requests();
    cache.take_requests();

    // The C binding's paimon_table_new_blob_reader reads through the table's FileIO this way.
    let reader = BlobReader::from_file_io(table.file_io().clone());
    assert_eq!(reader.read_blobs(&descriptors).await.unwrap(), expected);
    let mut stream = reader.open_blob(&descriptors[1]).unwrap();
    assert_eq!(stream.read(16).await.unwrap(), b"NEW");
    let (origin_requests, cache_requests) = (origin.take_requests(), cache.take_requests());
    assert_eq!(methods(&cache_requests, key), Vec::<String>::new());
    assert!(methods(&origin_requests, key).iter().any(|m| m == "GET"));
    assert!(methods(&cache_requests, owned_key)
        .iter()
        .any(|m| m == "GET"));
    assert_eq!(methods(&origin_requests, owned_key), Vec::<String>::new());

    // A reader built from storage options serves no table, so it never uses the target.
    let options = io_cache_options(&origin, &cache, &DATA_TARGET)
        .to_map()
        .clone();
    let reader = BlobReader::new(options);
    assert_eq!(reader.read_blobs(&descriptors).await.unwrap(), expected);
    let mut stream = reader.open_blob(&descriptors[1]).unwrap();
    assert_eq!(stream.read(16).await.unwrap(), b"NEW");
    assert_eq!(cache.take_requests(), Vec::<String>::new());
}

#[tokio::test]
async fn every_table_copy_routes_files_named_with_the_schema_prefix() {
    let origin = TestOss::start().await;
    let cache = TestOss::start_sharing(&origin).await;
    let catalog = data_target_catalog(&origin, &cache).await;
    let schema = int_schema()
        .option("data-file.prefix", "custom-")
        .build()
        .unwrap();
    let table = create_table(&catalog, "copies", schema).await;
    let files = write_and_commit(&table, &make_batch(vec![1], vec![10])).await;
    let snapshot = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    table.tag_manager().create("base", &snapshot).await.unwrap();
    BranchManager::new(table.file_io().clone(), table.location().into())
        .create_branch_from_tag("dev", "base")
        .await
        .unwrap();
    let option = |key: &str, value: &str| HashMap::from([(key.to_string(), value.to_string())]);
    let file_io = FileIOBuilder::new("oss")
        .with_props(
            io_cache_options(&origin, &cache, &DATA_TARGET)
                .to_map()
                .iter(),
        )
        .build()
        .unwrap();
    let copies = [
        (
            "from_resolved_schema",
            Table::from_resolved_schema(
                file_io,
                table.identifier().clone(),
                table.location().to_string(),
                table.schema().clone(),
                "main",
            )
            .unwrap(),
        ),
        (
            "copy_with_options",
            table.copy_with_options(option("read.batch-size", "16")),
        ),
        (
            "snapshot",
            table
                .copy_with_time_travel(option("scan.snapshot-id", "1"))
                .await
                .unwrap(),
        ),
        (
            "tag",
            table
                .copy_with_time_travel(option("scan.tag-name", "base"))
                .await
                .unwrap(),
        ),
        (
            "copy_with_resolved_snapshot",
            table.copy_with_resolved_snapshot(&snapshot).await.unwrap(),
        ),
        (
            "copy_with_pinned_snapshot",
            table.copy_with_pinned_snapshot(Some(&snapshot)),
        ),
        (
            "copy_with_resolved_schema",
            table
                .copy_with_resolved_schema(table.schema().clone(), "main")
                .unwrap(),
        ),
        (
            "copy_with_resolved_schema on a branch",
            table
                .copy_with_resolved_schema(table.schema().clone(), "dev")
                .unwrap(),
        ),
        (
            "copy_with_branch",
            table.copy_with_branch("dev").await.unwrap(),
        ),
        (
            "branch option",
            table
                .copy_with_time_travel(option("branch", "dev"))
                .await
                .unwrap(),
        ),
    ];
    for (name, copy) in copies {
        origin.take_requests();
        cache.take_requests();
        assert_eq!(ids(&copy).await, [1], "{name}");
        let (origin_requests, cache_requests) = (origin.take_requests(), cache.take_requests());
        for file in &files {
            let on_target = methods(&cache_requests, file);
            assert!(
                on_target.iter().any(|m| m == "GET"),
                "{name}: {on_target:?}"
            );
            assert_eq!(
                methods(&origin_requests, file),
                Vec::<String>::new(),
                "{name}"
            );
        }
    }
}

/// Tables a REST catalog serves by name, and the io-cache token it vends for each of them.
type RestTables = (
    Arc<HashMap<String, GetTableResponse>>,
    Arc<HashMap<String, String>>,
);

async fn rest_table(
    State((tables, _)): State<RestTables>,
    Path((_, name)): Path<(String, String)>,
) -> Json<GetTableResponse> {
    Json(tables[&name].clone())
}

async fn rest_token(State((_, token)): State<RestTables>) -> Json<GetTableTokenResponse> {
    Json(GetTableTokenResponse {
        token: token.as_ref().clone(),
        expires_at_millis: Some(i64::MAX / 2),
    })
}

fn rest_table_response(id: &str, name: &str, location: &str, prefix: &str) -> GetTableResponse {
    let mut response: GetTableResponse = serde_json::from_value(serde_json::json!({
        "id": id,
        "database": "db",
        "name": name,
        "path": location,
        "isExternal": false,
        "schemaId": 0,
    }))
    .unwrap();
    response.schema = Some(
        int_schema()
            .option("data-file.prefix", prefix)
            .build()
            .unwrap(),
    );
    response
}

#[tokio::test]
async fn rest_branch_copies_route_files_named_with_the_branch_prefix() {
    let origin = TestOss::start().await;
    let cache = TestOss::start_sharing(&origin).await;
    // The catalog keeps the branch schema; storage has no schema directory for it.
    let location = format!("oss://{WAREHOUSE}/db.db/rest_branch");
    let file = "branch-123e4567-e89b-12d3-a456-426614174000-1.parquet";
    let path = format!("{location}/bucket-0/{file}");
    put(&origin, &path, parquet(&[1])).await;
    let tables = HashMap::from([
        (
            "rest_branch".to_string(),
            rest_table_response("main-uuid", "rest_branch", &location, "main-"),
        ),
        (
            "rest_branch$branch_dev".to_string(),
            rest_table_response(
                "branch-uuid",
                "rest_branch$branch_dev",
                &location,
                "branch-",
            ),
        ),
    ]);
    let token = io_cache_options(&origin, &cache, &DATA_TARGET)
        .to_map()
        .clone();
    let app = Router::new()
        .route("/v1/databases/:db/tables/:name", get(rest_table))
        .route("/v1/databases/:db/tables/:name/token", get(rest_token))
        .with_state((Arc::new(tables.clone()), Arc::new(token)));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut options = Options::new();
    options.set(CatalogOptions::URI, format!("http://{address}"));
    options.set(CatalogOptions::TOKEN_PROVIDER, "bear");
    options.set(CatalogOptions::TOKEN, "test-token");
    let api = Arc::new(RESTApi::new(options.clone(), false).await.unwrap());
    let fuse_resolver = Arc::new(FusePathResolver::new(api.options()));
    let file_io_cache = FileIOCacheContext::from_props(options.to_map()).unwrap();
    let load = |name: &str, identifier: Identifier| {
        let (response, api, options) = (tables[name].clone(), api.clone(), options.clone());
        let (file_io_cache, fuse_resolver) = (file_io_cache.clone(), fuse_resolver.clone());
        async move {
            RESTEnv::build_table(
                &identifier,
                response,
                api,
                options,
                true,
                None,
                file_io_cache,
                fuse_resolver,
            )
            .await
        }
    };
    let main = load("rest_branch", Identifier::new("db", "rest_branch"))
        .await
        .unwrap();
    let branch_identifier = Identifier::new("db", "rest_branch")
        .with_branch("dev")
        .unwrap();
    let loaded_branch = load("rest_branch$branch_dev", branch_identifier)
        .await
        .unwrap();
    let copies = [
        ("loaded branch", loaded_branch),
        (
            "copy_with_branch",
            main.copy_with_branch("dev").await.unwrap(),
        ),
        (
            "branch option",
            main.copy_with_time_travel(HashMap::from([("branch".to_string(), "dev".to_string())]))
                .await
                .unwrap(),
        ),
    ];
    for (name, branch) in copies {
        origin.take_requests();
        cache.take_requests();
        branch
            .file_io()
            .new_input(&path)
            .unwrap()
            .read()
            .await
            .unwrap();
        assert_eq!(methods(&cache.take_requests(), file), ["GET"], "{name}");
        assert_eq!(
            methods(&origin.take_requests(), file),
            Vec::<String>::new(),
            "{name}"
        );
    }
    server.abort();
}

#[tokio::test]
async fn data_files_under_external_paths_use_the_origin() {
    let origin = TestOss::start().await;
    let cache = TestOss::start_sharing(&origin).await;
    let catalog = data_target_catalog(&origin, &cache).await;
    // The target serves the table's location; files the table writes elsewhere stay on origin.
    let schema = int_schema()
        .option("data-file.external-paths", "oss://bkt/external")
        .option("data-file.external-paths.strategy", "round-robin")
        .build()
        .unwrap();
    let table = create_table(&catalog, "external_paths", schema).await;
    let files = write_and_commit(&table, &make_batch(vec![1], vec![10])).await;
    assert_eq!(ids(&table).await, [1]);
    let (origin_requests, cache_requests) = (origin.take_requests(), cache.take_requests());
    for file in &files {
        let external = "bkt/external/";
        let on_origin: Vec<_> = origin_requests
            .iter()
            .filter(|request| request.contains(file.as_str()))
            .collect();
        assert!(
            on_origin.iter().all(|request| request.contains(external)),
            "{on_origin:?}"
        );
        assert_reads_and_writes(&origin_requests, file);
        assert_eq!(
            methods(&cache_requests, file),
            Vec::<String>::new(),
            "{file}"
        );
    }
}
