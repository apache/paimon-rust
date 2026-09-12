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

mod common;

#[path = "../../../paimon/tests/mock_server.rs"]
mod mock_server;

use std::collections::HashMap;
use std::sync::Arc;

use paimon::api::ConfigResponse;
use paimon::catalog::RESTCatalog;
use paimon::spec::{BigIntType, BooleanType, DataType, DateType, IntType, Schema, VarCharType};
use paimon::{CatalogOptions, Options};
use paimon_datafusion::SQLContext;
use tempfile::TempDir;

use mock_server::{start_mock_server, RESTServer};

const DATABASE: &str = "default";
const TABLE: &str = "events";
const TABLE_NAME: &str = "paimon.default.events";
const WAREHOUSE: &str = "test_warehouse";

async fn setup_rest_table(temp_dir: &TempDir, schema: Schema) -> (RESTServer, SQLContext) {
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
        schema,
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
    context.register_catalog("paimon", catalog).await.unwrap();
    (server, context)
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

/// A `(dt, hh)` table with the given partitions added through ADD PARTITION.
async fn dt_hh_table(partitions: &[(&str, &str)]) -> (TempDir, RESTServer, SQLContext) {
    let temp_dir = tempfile::tempdir().unwrap();
    let schema = format_table_schema(&[("dt", varchar()), ("hh", varchar())]);
    let (server, context) = setup_rest_table(&temp_dir, schema).await;
    for (dt, hh) in partitions {
        common::exec(
            &context,
            &format!(
                "ALTER TABLE {TABLE_NAME} ADD IF NOT EXISTS PARTITION (dt = '{dt}', hh = '{hh}')"
            ),
        )
        .await;
    }
    (temp_dir, server, context)
}

async fn show_partitions(context: &SQLContext, partition_clause: &str) -> Vec<String> {
    let batches = context
        .sql(&format!("SHOW PARTITIONS {TABLE_NAME}{partition_clause}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| common::string_value(batch.column(0), row).to_string())
        })
        .collect()
}

fn spec(values: &[(&str, &str)]) -> HashMap<String, String> {
    values
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

#[cfg(not(windows))]
#[tokio::test]
async fn test_partition_commands_update_rest_metadata_and_directories() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (_server, context) =
        setup_rest_table(&temp_dir, format_table_schema(&[("dt", varchar())])).await;

    common::exec(
        &context,
        &format!("ALTER TABLE {TABLE_NAME} ADD PARTITION (dt = 'a') PARTITION (dt = 'b')"),
    )
    .await;
    assert_eq!(show_partitions(&context, "").await, ["dt=a", "dt=b"]);
    assert_eq!(
        show_partitions(&context, " PARTITION (dt = 'b')").await,
        ["dt=b"]
    );
    assert!(temp_dir.path().join("dt=a").is_dir());

    common::exec(
        &context,
        &format!("ALTER TABLE {TABLE_NAME} DROP PARTITION (dt = 'a')"),
    )
    .await;
    assert_eq!(show_partitions(&context, "").await, ["dt=b"]);
    assert!(!temp_dir.path().join("dt=a").exists());
}

#[cfg(not(windows))]
#[tokio::test]
async fn test_partition_literals() {
    let temp_dir = tempfile::tempdir().unwrap();
    let schema = format_table_schema(&[
        ("dt", DataType::Date(DateType::new())),
        ("month", DataType::Int(IntType::new())),
        ("active", DataType::Boolean(BooleanType::new())),
        ("label", varchar()),
    ]);
    let (_server, context) = setup_rest_table(&temp_dir, schema).await;

    // Literals are read with the column type the way Java reads partition strings. Each case adds
    // a partition, checks its name and directory, and drops it again with the same literals.
    for (literals, name, directory) in [
        // A typed DATE, a zero-padded INT, a boolean in capitals and a number for a string
        // column. DATE directories hold Unix epoch days.
        (
            "dt = DATE '2026-07-22', month = '01', active = 'TRUE', label = 20260722",
            "dt=2026-07-22/month=1/active=true/label=20260722",
            "dt=20656/month=1/active=true/label=20260722",
        ),
        // Java's boolean spellings, and an unquoted zero-padded number.
        (
            "dt = '2026-07-23', month = 01, active = 'yes', label = 'a'",
            "dt=2026-07-23/month=1/active=true/label=a",
            "dt=20657/month=1/active=true/label=a",
        ),
        (
            "dt = '2026-07-24', month = -1, active = '0', label = 'b'",
            "dt=2026-07-24/month=-1/active=false/label=b",
            "dt=20658/month=-1/active=false/label=b",
        ),
        // NULL is the default partition.
        (
            "dt = NULL, month = NULL, active = NULL, label = NULL",
            "dt=null/month=null/active=null/label=null",
            "dt=__DEFAULT_PARTITION__/month=__DEFAULT_PARTITION__/\
             active=__DEFAULT_PARTITION__/label=__DEFAULT_PARTITION__",
        ),
    ] {
        common::exec(
            &context,
            &format!("ALTER TABLE {TABLE_NAME} ADD PARTITION ({literals})"),
        )
        .await;
        assert_eq!(show_partitions(&context, "").await, [name], "{literals}");
        assert!(temp_dir.path().join(directory).is_dir(), "{literals}");
        common::exec(
            &context,
            &format!("ALTER TABLE {TABLE_NAME} DROP PARTITION ({literals})"),
        )
        .await;
        assert!(show_partitions(&context, "").await.is_empty(), "{literals}");
    }

    for (literals, column) in [
        (
            "dt = '2026-07-22', month = '1.5', active = 'true', label = 'a'",
            "'month'",
        ),
        (
            "dt = '2026-07-22', month = 1, active = 'maybe', label = 'a'",
            "'active'",
        ),
        (
            "dt = 'yesterday', month = 1, active = 'true', label = 'a'",
            "'dt'",
        ),
    ] {
        common::assert_sql_error(
            &context,
            &format!("ALTER TABLE {TABLE_NAME} ADD PARTITION ({literals})"),
            column,
        )
        .await;
    }
}

/// A blank string for a string partition column is written to the default partition, so ADD and
/// DROP refuse it rather than address the NULL partition, as Java does.
#[cfg(not(windows))]
#[tokio::test]
async fn test_partition_ddl_refuses_blank_string_values() {
    let temp_dir = tempfile::tempdir().unwrap();
    let schema = format_table_schema(&[("label", varchar())]);
    let (_server, context) = setup_rest_table(&temp_dir, schema).await;
    common::exec(
        &context,
        &format!("ALTER TABLE {TABLE_NAME} ADD PARTITION (label = NULL)"),
    )
    .await;
    let default_directory = temp_dir.path().join("label=__DEFAULT_PARTITION__");
    assert!(default_directory.is_dir());

    for statement in [
        "DROP PARTITION (label = '')",
        "DROP IF EXISTS PARTITION (label = '   ')",
        "ADD PARTITION (label = '')",
        "ADD IF NOT EXISTS PARTITION (label = ' ')",
    ] {
        common::assert_sql_error(
            &context,
            &format!("ALTER TABLE {TABLE_NAME} {statement}"),
            "empty or whitespace-only string for partition column 'label'",
        )
        .await;
    }

    assert_eq!(show_partitions(&context, "").await, ["label=null"]);
    assert!(default_directory.is_dir());
}

#[cfg(not(windows))]
#[tokio::test]
async fn test_drop_partition_specifications() {
    const REGISTERED: &[(&str, &str)] =
        &[("20260722", "10"), ("20260722", "11"), ("20260723", "10")];
    const ALL: &[&str] = &[
        "dt=20260722/hh=10",
        "dt=20260722/hh=11",
        "dt=20260723/hh=10",
    ];

    for (operation, error, remaining) in [
        // A partial specification expands to every registered partition it matches.
        ("DROP PARTITION (dt = '20260722')", None, &["dt=20260723/hh=10"][..]),
        // The fixed keys need not be a leading prefix.
        ("DROP PARTITION (hh = '10')", None, &["dt=20260722/hh=11"][..]),
        // One statement may carry several specifications.
        (
            "DROP PARTITION (dt = '20260722', hh = '11'), DROP PARTITION (dt = '20260723')",
            None,
            &["dt=20260722/hh=10"][..],
        ),
        // A complete specification names one partition, so a missing one is an error,
        (
            "DROP PARTITION (dt = '20260724', hh = '10')",
            Some("does not exist"),
            ALL,
        ),
        // unless IF EXISTS is given.
        ("DROP IF EXISTS PARTITION (dt = '20260724', hh = '10')", None, ALL),
        // A partial specification describes a set that may come out empty.
        (
            "DROP PARTITION (dt = '20260724'), DROP PARTITION (dt = '20260723')",
            None,
            &["dt=20260722/hh=10", "dt=20260722/hh=11"][..],
        ),
        // One failing specification leaves the whole statement unapplied.
        (
            "DROP PARTITION (dt = '20260722', hh = '10'), DROP PARTITION (dt = '20260724', hh = '10')",
            Some("does not exist"),
            ALL,
        ),
        // Dropping partitions cannot be combined with a schema change.
        (
            "DROP PARTITION (dt = '20260722'), ADD COLUMN c INT",
            Some("must be used alone"),
            ALL,
        ),
    ] {
        let (temp_dir, _server, context) = dt_hh_table(REGISTERED).await;
        let sql = format!("ALTER TABLE {TABLE_NAME} {operation}");
        match error {
            None => common::exec(&context, &sql).await,
            Some(expected) => common::assert_sql_error(&context, &sql, expected).await,
        }
        assert_eq!(show_partitions(&context, "").await, remaining, "{operation}");
        // A dropped partition loses its directory and a kept one keeps it.
        for (dt, hh) in REGISTERED {
            let name = format!("dt={dt}/hh={hh}");
            assert_eq!(
                temp_dir.path().join(&name).exists(),
                remaining.contains(&name.as_str()),
                "{operation}: {name}"
            );
        }
    }
}

#[cfg(not(windows))]
#[tokio::test]
async fn test_drop_partition_matches_values_as_the_catalog_holds_them() {
    let temp_dir = tempfile::tempdir().unwrap();
    let schema = format_table_schema(&[
        ("year", varchar()),
        ("month", DataType::Int(IntType::new())),
    ]);
    let (server, context) = setup_rest_table(&temp_dir, schema).await;
    // Repair keeps directory spellings, so both registrations are legitimate and distinct.
    for directory in ["year=2025/month=01", "year=2026/month=1"] {
        std::fs::create_dir_all(temp_dir.path().join(directory)).unwrap();
    }
    server.set_table_partitions(
        DATABASE,
        TABLE,
        vec![
            spec(&[("year", "2025"), ("month", "01")]),
            spec(&[("year", "2026"), ("month", "1")]),
        ],
    );

    // SHOW PARTITIONS reads both with the column type.
    assert_eq!(
        show_partitions(&context, " PARTITION (month = 1)").await,
        ["year=2025/month=1", "year=2026/month=1"]
    );

    // A request is spelled the way ADD PARTITION registers it, so `month = 1` is not `month=01`.
    common::assert_sql_error(
        &context,
        &format!("ALTER TABLE {TABLE_NAME} DROP PARTITION (year = '2025', month = 1)"),
        "does not exist",
    )
    .await;
    common::exec(
        &context,
        &format!("ALTER TABLE {TABLE_NAME} DROP IF EXISTS PARTITION (year = '2025', month = 1)"),
    )
    .await;

    common::exec(
        &context,
        &format!("ALTER TABLE {TABLE_NAME} DROP PARTITION (month = 1)"),
    )
    .await;
    assert_eq!(
        server.table_partition_specs(DATABASE, TABLE),
        vec![spec(&[("year", "2025"), ("month", "01")])]
    );
    assert!(temp_dir.path().join("year=2025/month=01").is_dir());
    assert!(!temp_dir.path().join("year=2026/month=1").exists());
}

#[cfg(not(windows))]
#[tokio::test]
async fn test_drop_partition_looks_up_complete_specifications_by_name() {
    let (_temp_dir, server, context) =
        dt_hh_table(&[("20260722", "10"), ("20260722", "11"), ("20260723", "10")]).await;
    let listings = server
        .table_partition_list_name_patterns(DATABASE, TABLE)
        .len();

    common::exec(
        &context,
        &format!(
            "ALTER TABLE {TABLE_NAME} \
             DROP PARTITION (dt = '20260722', hh = '10'), DROP PARTITION (dt = '20260723', hh = '10')"
        ),
    )
    .await;
    assert_eq!(
        server
            .table_partition_list_name_patterns(DATABASE, TABLE)
            .len(),
        listings,
        "complete specifications should not read the registry"
    );
    assert_eq!(
        server.table_partition_list_by_names_calls(DATABASE, TABLE),
        vec![vec![
            spec(&[("dt", "20260722"), ("hh", "10")]),
            spec(&[("dt", "20260723"), ("hh", "10")]),
        ]]
    );

    // A partial specification needs the registry, and reads it once.
    common::exec(
        &context,
        &format!("ALTER TABLE {TABLE_NAME} DROP PARTITION (dt = '20260722')"),
    )
    .await;
    assert_eq!(
        server
            .table_partition_list_name_patterns(DATABASE, TABLE)
            .len(),
        listings + 1
    );
    assert_eq!(
        server
            .table_partition_list_by_names_calls(DATABASE, TABLE)
            .len(),
        1
    );
    assert!(server.table_partition_specs(DATABASE, TABLE).is_empty());
}

#[cfg(not(windows))]
#[tokio::test]
async fn test_drop_partition_leaves_a_custom_location_in_place() {
    let temp_dir = tempfile::tempdir().unwrap();
    let external_dir = tempfile::tempdir().unwrap();
    let (server, context) =
        setup_rest_table(&temp_dir, format_table_schema(&[("dt", varchar())])).await;
    common::exec(
        &context,
        &format!("ALTER TABLE {TABLE_NAME} ADD PARTITION (dt = 'a') PARTITION (dt = 'b')"),
    )
    .await;
    // Another engine registered dt=b somewhere else; the table directory still has its own dt=b.
    std::fs::write(external_dir.path().join("part-0.parquet"), b"data").unwrap();
    server.set_table_partition_options(
        DATABASE,
        TABLE,
        &spec(&[("dt", "b")]),
        HashMap::from([(
            "path".to_string(),
            format!("file://{}", external_dir.path().display()),
        )]),
    );

    // Dropping it unregisters it and deletes nothing, least of all its own data.
    common::exec(
        &context,
        &format!("ALTER TABLE {TABLE_NAME} DROP PARTITION (dt = 'b')"),
    )
    .await;
    assert_eq!(
        server.table_partition_specs(DATABASE, TABLE),
        vec![spec(&[("dt", "a")])]
    );
    assert!(external_dir.path().join("part-0.parquet").exists());
    assert!(temp_dir.path().join("dt=b").is_dir());
}

/// `SQLContext::sql` futures have to stay `Send` for callers that box them the way
/// `#[async_trait]` does, or spawn them. A stream over borrowed items anywhere below a statement
/// takes that away from every statement; this function stops compiling when that happens.
#[allow(dead_code)]
fn sql_future_is_send<'a>(
    context: &'a SQLContext,
    sql: &'a str,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
    Box::pin(async move {
        let _ = context.sql(sql).await;
    })
}
