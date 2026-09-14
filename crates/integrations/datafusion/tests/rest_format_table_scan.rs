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

//! SQL reads of a Format Table whose partitions a REST catalog manages.
//!
//! Partitions are registered through the catalog API, the way any writer registers them, so
//! these tests do not depend on how a partition came to be registered.

#[path = "../../../paimon/tests/mock_server.rs"]
mod mock_server;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use axum::http::StatusCode;
use paimon::api::ConfigResponse;
use paimon::catalog::{Catalog, Identifier, RESTCatalog};
use paimon::spec::{BigIntType, BooleanType, DataType, IntType, Schema, VarCharType};
use paimon::{CatalogOptions, Options};
use paimon_datafusion::SQLContext;
use parquet::arrow::ArrowWriter;
use tempfile::TempDir;

use mock_server::{start_mock_server, RESTServer};

const DATABASE: &str = "default";
const TABLE: &str = "events";
const WAREHOUSE: &str = "test_warehouse";

/// A catalog-managed Format Table served by a mock REST catalog, reading its data from a
/// temporary directory.
struct ManagedTable {
    server: RESTServer,
    catalog: Arc<RESTCatalog>,
    context: SQLContext,
}

impl ManagedTable {
    async fn new(temp_dir: &TempDir, partition_columns: &[(&str, DataType)]) -> Self {
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
            format_table_schema(partition_columns),
            &format!("file://{}", temp_dir.path().display()),
        );
        server.set_table_external(DATABASE, TABLE, false);

        let mut options = Options::new();
        options.set(CatalogOptions::URI, server.url().unwrap());
        options.set(CatalogOptions::WAREHOUSE, WAREHOUSE);
        options.set(CatalogOptions::TOKEN_PROVIDER, "bear");
        options.set(CatalogOptions::TOKEN, "test-token");
        let catalog = Arc::new(RESTCatalog::new(options, true).await.unwrap());
        let mut context = SQLContext::new();
        context
            .register_catalog("paimon", catalog.clone())
            .await
            .unwrap();
        Self {
            server,
            catalog,
            context,
        }
    }

    /// Register partitions through the catalog API.
    async fn register(&self, partitions: &[&[(&str, &str)]]) {
        self.catalog
            .create_partitions(
                &Identifier::new(DATABASE, TABLE),
                partitions.iter().map(|values| spec(values)).collect(),
                true,
            )
            .await
            .unwrap();
    }

    /// The sorted ids of the rows a filtered SELECT returns.
    async fn ids(&self, predicate: &str) -> Vec<i64> {
        let sql = format!("SELECT id FROM paimon.{DATABASE}.{TABLE} WHERE {predicate}");
        let mut ids = Vec::new();
        for batch in self
            .context
            .sql(&sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
        {
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

    /// The error a filtered SELECT fails with, whether planning or execution reports it.
    async fn error(&self, predicate: &str) -> String {
        let sql = format!("SELECT id FROM paimon.{DATABASE}.{TABLE} WHERE {predicate}");
        match self.context.sql(&sql).await {
            Ok(frame) => frame.collect().await.unwrap_err(),
            Err(error) => error,
        }
        .to_string()
    }

    /// How many listings the table has received from each endpoint: plain, then by filter.
    fn listing_counts(&self) -> (usize, usize) {
        (
            self.server
                .table_partition_list_name_patterns(DATABASE, TABLE)
                .len(),
            self.server
                .table_partition_list_by_filter_requests(DATABASE, TABLE)
                .len(),
        )
    }

    /// The name patterns of the listings received since `seen`, whichever endpoint served them.
    fn name_patterns_since(&self, seen: (usize, usize)) -> Vec<Option<String>> {
        let mut patterns = self
            .server
            .table_partition_list_name_patterns(DATABASE, TABLE)
            .split_off(seen.0);
        patterns.extend(
            self.server
                .table_partition_list_by_filter_requests(DATABASE, TABLE)
                .into_iter()
                .skip(seen.1)
                .map(|request| request.partition_name_pattern),
        );
        patterns
    }
}

fn format_table_schema(partition_columns: &[(&str, DataType)]) -> Schema {
    let partition_keys = partition_columns
        .iter()
        .map(|(name, _)| (*name).to_string())
        .collect::<Vec<_>>();
    partition_columns
        .iter()
        .fold(Schema::builder(), |builder, (name, data_type)| {
            builder.column(*name, data_type.clone())
        })
        .column("id", DataType::BigInt(BigIntType::new()))
        .partition_keys(partition_keys)
        .option("type", "format-table")
        .option("file.format", "parquet")
        .option("metastore.partitioned-table", "true")
        .build()
        .unwrap()
}

fn varchar() -> DataType {
    DataType::VarChar(VarCharType::new(255).unwrap())
}

fn spec(values: &[(&str, &str)]) -> HashMap<String, String> {
    values
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
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

#[cfg(not(windows))]
// Planning a SELECT resolves the table on a blocking catalog-access thread, so the mock
// server needs a runtime thread of its own to answer while that one waits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_managed_scan_reads_only_registered_partitions() {
    let temp_dir = tempfile::tempdir().unwrap();
    let table = ManagedTable::new(&temp_dir, &[("dt", varchar())]).await;
    for (dt, id) in [("a", 1), ("b", 2), ("c", 3)] {
        write_ids(&temp_dir.path().join(format!("dt={dt}")), &[id]);
    }
    table.register(&[&[("dt", "a")], &[("dt", "c")]]).await;

    // `dt=b` holds a file but nobody registered it, so it is not part of the table.
    assert_eq!(table.ids("TRUE").await, vec![1, 3]);
    assert!(table.ids("dt = 'b'").await.is_empty());

    table.register(&[&[("dt", "b")]]).await;
    assert_eq!(table.ids("TRUE").await, vec![1, 2, 3]);
}

#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_managed_scan_pushes_a_partition_name_pattern() {
    let temp_dir = tempfile::tempdir().unwrap();
    let table = ManagedTable::new(&temp_dir, &[("dt", varchar()), ("hh", varchar())]).await;
    table
        .register(&[
            &[("dt", "20260722"), ("hh", "10")],
            &[("dt", "20260722"), ("hh", "11")],
            &[("dt", "20260723"), ("hh", "10")],
        ])
        .await;

    for (predicate, expected) in [
        ("dt = '20260722' AND hh = '10'", Some("dt=20260722/hh=10")),
        ("dt = '20260722'", Some("dt=20260722/%")),
        (
            "dt = '20260722' AND hh IN ('10', '11')",
            Some("dt=20260722/%"),
        ),
        // Only a leading run of equalities becomes a prefix pattern.
        ("hh = '10'", None),
        ("dt > '20260722'", None),
    ] {
        let seen = table.listing_counts();
        table.ids(predicate).await;
        let pushed = table.name_patterns_since(seen);
        assert!(!pushed.is_empty(), "{predicate} listed no partitions");
        assert!(
            pushed.iter().all(|pattern| pattern.as_deref() == expected),
            "{predicate} pushed {pushed:?}, expected {expected:?}"
        );
    }
}

#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_managed_scan_sends_its_partition_predicate_as_a_filter() {
    let temp_dir = tempfile::tempdir().unwrap();
    let table = ManagedTable::new(&temp_dir, &[("dt", varchar()), ("hh", varchar())]).await;
    for (dt, hh, id) in [
        ("20260722", "10", 1),
        ("20260722", "11", 2),
        ("20260723", "10", 3),
    ] {
        write_ids(&temp_dir.path().join(format!("dt={dt}/hh={hh}")), &[id]);
    }
    table
        .register(&[
            &[("dt", "20260722"), ("hh", "10")],
            &[("dt", "20260722"), ("hh", "11")],
            &[("dt", "20260723"), ("hh", "10")],
        ])
        .await;

    // No leading equality, so only the filter can narrow what the catalog returns.
    assert_eq!(table.ids("hh = '10'").await, vec![1, 3]);
    let requests = table
        .server
        .table_partition_list_by_filter_requests(DATABASE, TABLE);
    let request = requests.last().expect("the scan should list by filter");
    assert_eq!(request.partition_name_pattern, None);
    assert_eq!(request.max_results, Some(1000));
    let filter: serde_json::Value = serde_json::from_str(&request.filter).unwrap();
    assert_eq!(filter["function"], "EQUAL");
    assert_eq!(filter["transform"]["fieldRef"]["name"], "hh");
    assert_eq!(filter["transform"]["fieldRef"]["index"], 1);
    assert_eq!(filter["literals"], serde_json::json!(["10"]));

    // A catalog that cannot list by filter is still asked, by pattern; the partition set never
    // comes from the directory tree.
    table
        .server
        .set_list_partitions_by_filter_error_status(Some(StatusCode::NOT_IMPLEMENTED));
    let listed = table
        .server
        .table_partition_list_name_patterns(DATABASE, TABLE)
        .len();
    assert_eq!(table.ids("dt = '20260722' AND hh > '10'").await, vec![2]);
    assert_eq!(
        table
            .server
            .table_partition_list_name_patterns(DATABASE, TABLE)[listed..],
        [Some("dt=20260722/%".to_string())]
    );
}

#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_managed_scan_keeps_registrations_spelled_unlike_the_filter() {
    let temp_dir = tempfile::tempdir().unwrap();
    let table = ManagedTable::new(
        &temp_dir,
        &[
            ("month", DataType::Int(IntType::new())),
            ("active", DataType::Boolean(BooleanType::new())),
        ],
    )
    .await;
    // Another engine, or a repair that keeps directory values as they are, can register
    // spellings a typed literal never formats to.
    write_ids(&temp_dir.path().join("month=01/active=TRUE"), &[1]);
    write_ids(&temp_dir.path().join("month=2/active=false"), &[2]);
    table
        .register(&[
            &[("month", "01"), ("active", "TRUE")],
            &[("month", "2"), ("active", "false")],
        ])
        .await;

    for (predicate, expected) in [
        ("month = 1", vec![1]),
        ("month = 1 AND active = true", vec![1]),
        ("month = 2 AND active = false", vec![2]),
    ] {
        assert_eq!(table.ids(predicate).await, expected, "{predicate}");
    }
    // A pattern built from `month = 1` would have dropped `month=01` on the catalog side.
    assert!(table
        .name_patterns_since((0, 0))
        .iter()
        .all(Option::is_none));
}

#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_managed_scan_filter_on_a_partition_column_reads_the_registered_value() {
    let temp_dir = tempfile::tempdir().unwrap();
    let table = ManagedTable::new(
        &temp_dir,
        &[
            ("dt", varchar()),
            ("active", DataType::Boolean(BooleanType::new())),
        ],
    )
    .await;
    for (dt, active, id) in [("a", "true", 1), ("b", "false", 2)] {
        write_ids(
            &temp_dir.path().join(format!("dt={dt}/active={active}")),
            &[id],
        );
    }
    table
        .register(&[
            &[("dt", "a"), ("active", "true")],
            &[("dt", "b"), ("active", "false")],
        ])
        .await;

    // The data files hold no partition columns. A filter the scan cannot turn into a partition
    // predicate still has to see the partition's value, not a missing column.
    for (predicate, expected) in [
        ("active", vec![1]),
        ("NOT active", vec![2]),
        ("upper(dt) = 'B'", vec![2]),
        ("concat(dt, '-') = 'a-'", vec![1]),
    ] {
        assert_eq!(table.ids(predicate).await, expected, "{predicate}");
    }
}

#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_managed_scan_refuses_a_partition_at_a_custom_location() {
    let temp_dir = tempfile::tempdir().unwrap();
    let external_dir = tempfile::tempdir().unwrap();
    let table = ManagedTable::new(&temp_dir, &[("dt", varchar())]).await;
    table.register(&[&[("dt", "a")], &[("dt", "b")]]).await;
    write_ids(&temp_dir.path().join("dt=a"), &[1]);
    // Another engine registered dt=b somewhere else; the table directory still has a stale copy.
    write_ids(&temp_dir.path().join("dt=b"), &[2]);
    write_ids(external_dir.path(), &[3]);
    table.server.set_table_partition_options(
        DATABASE,
        TABLE,
        &spec(&[("dt", "b")]),
        HashMap::from([(
            "path".to_string(),
            format!("file://{}", external_dir.path().display()),
        )]),
    );

    // Reading the default directory would return the stale row, so a scan that reaches the
    // partition fails instead. One that does not reach it is unaffected.
    for predicate in ["dt = 'b'", "TRUE"] {
        let error = table.error(predicate).await;
        assert!(error.contains("custom location"), "{predicate}: {error}");
    }
    assert_eq!(table.ids("dt = 'a'").await, vec![1]);
}

#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_managed_scan_fails_rather_than_reading_directories_when_listing_fails() {
    let temp_dir = tempfile::tempdir().unwrap();
    let table = ManagedTable::new(&temp_dir, &[("dt", varchar())]).await;
    write_ids(&temp_dir.path().join("dt=a"), &[1]);
    table.register(&[&[("dt", "a")]]).await;
    assert_eq!(table.ids("TRUE").await, vec![1]);

    table
        .server
        .set_list_partitions_error_status(Some(StatusCode::NOT_IMPLEMENTED));

    // The directory still holds the row, but only the catalog says which partitions exist.
    let error = table.error("TRUE").await;
    assert!(
        error.to_ascii_lowercase().contains("not implemented"),
        "{error}"
    );
}
