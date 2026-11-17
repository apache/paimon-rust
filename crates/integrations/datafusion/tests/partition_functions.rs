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

use std::sync::Arc;

use datafusion::common::ScalarValue;
use paimon::{CatalogOptions, FileSystemCatalog, Options};
use paimon_datafusion::SQLContext;
use tempfile::TempDir;

async fn context() -> (TempDir, SQLContext) {
    let directory = TempDir::new().unwrap();
    let mut options = Options::new();
    options.set(
        CatalogOptions::WAREHOUSE,
        format!("file://{}", directory.path().display()),
    );
    let mut ctx = SQLContext::new();
    ctx.register_catalog("paimon", Arc::new(FileSystemCatalog::new(options).unwrap()))
        .await
        .unwrap();
    (directory, ctx)
}

async fn execute(ctx: &SQLContext, sql: &str) {
    Box::pin(ctx.sql(sql))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
}

async fn scalar(ctx: &SQLContext, sql: &str) -> ScalarValue {
    let batches = Box::pin(ctx.sql(sql))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    ScalarValue::try_from_array(batches[0].column(0), 0).unwrap()
}

async fn assert_error(ctx: &SQLContext, sql: &str, expected: &str) {
    let error = match Box::pin(ctx.sql(sql)).await {
        Err(error) => error,
        Ok(frame) => frame.collect().await.unwrap_err(),
    };
    assert!(error.to_string().contains(expected), "{sql}: {error}");
}

#[tokio::test]
async fn max_pt_uses_typed_top_level_partitions_and_refreshes_between_queries() {
    let (_directory, ctx) = context().await;
    execute(
        &ctx,
        "CREATE TABLE t (id INT, p INT, sub STRING) PARTITIONED BY (p, sub)",
    )
    .await;
    execute(
        &ctx,
        "INSERT INTO t VALUES (1, 9, 'z'), (2, 10, 'a'), (3, 10, 'b'), (4, NULL, 'z')",
    )
    .await;
    for call in [
        "max_pt('t')",
        "sys.max_pt('default.t')",
        "paimon.sys.max_pt('paimon.default.t')",
    ] {
        assert_eq!(
            scalar(&ctx, &format!("SELECT {call}")).await,
            ScalarValue::Utf8(Some("10".into()))
        );
    }
    assert_eq!(
        scalar(
            &ctx,
            "SELECT count(*) FROM t WHERE p = CAST(max_pt('t') AS INT)"
        )
        .await,
        ScalarValue::Int64(Some(2))
    );
    // Planning resolves the metadata function to a constant for partition pruning.
    let frame = ctx
        .sql("SELECT * FROM t WHERE p = CAST(max_pt('t') AS INT)")
        .await
        .unwrap();
    assert!(!frame
        .logical_plan()
        .display_indent()
        .to_string()
        .contains("max_pt"));
    execute(&ctx, "ALTER TABLE t DROP PARTITION (p=10, sub='a')").await;
    execute(&ctx, "ALTER TABLE t DROP PARTITION (p=10, sub='b')").await;
    assert_eq!(
        scalar(&ctx, "SELECT max_pt('t')").await,
        ScalarValue::Utf8(Some("9".into()))
    );
    execute(&ctx, "INSERT INTO t VALUES (5, 11, 'c')").await;
    assert_eq!(
        scalar(&ctx, "SELECT max_pt('t')").await,
        ScalarValue::Utf8(Some("11".into()))
    );
}

#[tokio::test]
async fn temporary_views_refresh_max_pt_including_nested_views_and_partition_filters() {
    let (_directory, ctx) = context().await;
    execute(&ctx, "CREATE TABLE t (id INT, p INT) PARTITIONED BY (p)").await;
    execute(&ctx, "INSERT INTO t VALUES (1, 10)").await;
    for (index, call) in [
        "max_pt('t')",
        "sys.max_pt('default.t')",
        "paimon.sys.max_pt('paimon.default.t')",
    ]
    .into_iter()
    .enumerate()
    {
        execute(
            &ctx,
            &format!("CREATE TEMPORARY VIEW latest_{index} AS SELECT {call} AS p"),
        )
        .await;
        execute(
            &ctx,
            &format!("CREATE TEMPORARY VIEW nested_{index} AS SELECT p FROM latest_{index}"),
        )
        .await;
    }
    execute(
        &ctx,
        "CREATE TEMPORARY VIEW latest_rows AS SELECT id FROM t WHERE p = CAST(max_pt('t') AS INT)",
    )
    .await;
    for (id, partition) in [(2, 11), (3, 12)] {
        execute(&ctx, &format!("INSERT INTO t VALUES ({id}, {partition})")).await;
        for index in 0..3 {
            for prefix in ["latest", "nested"] {
                assert_eq!(
                    scalar(&ctx, &format!("SELECT p FROM {prefix}_{index}")).await,
                    ScalarValue::Utf8(Some(partition.to_string()))
                );
            }
        }
        assert_eq!(
            scalar(&ctx, "SELECT id FROM latest_rows WHERE id > 1 LIMIT 1").await,
            ScalarValue::Int32(Some(id))
        );
    }
    let provider = ctx.ctx().table_provider("latest_0").await.unwrap();
    assert!(provider.get_table_definition().unwrap().contains("max_pt"));
}

#[tokio::test]
async fn temporary_metadata_views_keep_creation_namespace_and_use_current_scan_options() {
    let (_directory, ctx) = context().await;
    execute(&ctx, "CREATE DATABASE other").await;
    execute(&ctx, "CREATE TABLE t (id INT, p INT) PARTITIONED BY (p)").await;
    execute(&ctx, "INSERT INTO t VALUES (1, 10)").await;
    execute(
        &ctx,
        "CREATE TEMPORARY VIEW other.latest AS SELECT MAX_PT('t') AS p",
    )
    .await;
    execute(&ctx, "INSERT INTO t VALUES (2, 11)").await;
    ctx.set_current_database("other").await.unwrap();
    execute(&ctx, "CREATE TABLE t (id INT, p INT) PARTITIONED BY (p)").await;
    execute(&ctx, "INSERT INTO t VALUES (3, 99)").await;
    execute(
        &ctx,
        "SET datafusion.sql_parser.enable_ident_normalization = false",
    )
    .await;
    for (option, expected) in [
        ("SET 'paimon.scan.version' = '1'", "10"),
        ("RESET 'paimon.scan.version'", "11"),
    ] {
        execute(&ctx, option).await;
        assert_eq!(
            scalar(&ctx, "SELECT p FROM other.latest").await,
            ScalarValue::Utf8(Some(expected.into()))
        );
    }
    assert_error(
        &ctx,
        "SELECT max_pt('other.latest')",
        "requires a Paimon data table",
    )
    .await;
}

#[tokio::test]
async fn temporary_metadata_views_reject_schema_drift_and_recursive_expansion() {
    let (_directory, ctx) = context().await;
    execute(&ctx, "CREATE TABLE t (id INT, p INT) PARTITIONED BY (p)").await;
    execute(&ctx, "INSERT INTO t VALUES (1, 10)").await;
    execute(
        &ctx,
        "CREATE TEMPORARY VIEW latest AS SELECT * FROM t WHERE p = CAST(max_pt('t') AS INT)",
    )
    .await;
    execute(&ctx, "ALTER TABLE t ADD COLUMN extra INT").await;
    assert_error(
        &ctx,
        "SELECT * FROM latest",
        "schema changed since creation",
    )
    .await;
    execute(&ctx, "CREATE TABLE recursive_source (id INT)").await;
    execute(
        &ctx,
        "CREATE TEMPORARY VIEW recursive_source AS SELECT id FROM recursive_source WHERE max_pt('t') IS NOT NULL",
    )
    .await;
    // A metadata source view is rejected without recursively planning its body.
    assert_error(
        &ctx,
        "SELECT max_pt('recursive_source')",
        "requires a Paimon data table",
    )
    .await;
    assert_error(
        &ctx,
        "SELECT * FROM recursive_source",
        "recursive metadata-dependent temporary view",
    )
    .await;
}

#[tokio::test]
async fn max_pt_preserves_strings_dates_and_current_namespace() {
    let (_directory, ctx) = context().await;
    execute(&ctx, "CREATE DATABASE other").await;
    ctx.set_current_database("other").await.unwrap();
    execute(&ctx, "CREATE TABLE t (id INT, p STRING) PARTITIONED BY (p)").await;
    execute(&ctx, "INSERT INTO t VALUES (1, '9'), (2, '10')").await;
    assert_eq!(
        scalar(&ctx, "SELECT max_pt('t')").await,
        ScalarValue::Utf8(Some("9".into()))
    );
    execute(&ctx, "INSERT INTO t VALUES (3, 'it''s latest')").await;
    assert_eq!(
        scalar(&ctx, "SELECT max_pt('t')").await,
        ScalarValue::Utf8(Some("it's latest".into()))
    );
    execute(
        &ctx,
        "CREATE TABLE dates (id INT, p DATE) PARTITIONED BY (p)",
    )
    .await;
    execute(
        &ctx,
        "INSERT INTO dates VALUES (1, DATE '2024-02-29'), (2, DATE '2024-03-01')",
    )
    .await;
    assert_eq!(
        scalar(&ctx, "SELECT max_pt('other.dates')").await,
        ScalarValue::Utf8(Some("2024-03-01".into()))
    );
}

#[tokio::test]
async fn max_pt_rejects_invalid_arguments_and_tables_without_valid_partitions() {
    let (_directory, ctx) = context().await;
    execute(&ctx, "CREATE TABLE plain (id INT)").await;
    execute(
        &ctx,
        "CREATE TABLE empty (id INT, p INT) PARTITIONED BY (p)",
    )
    .await;
    for sql in [
        "SELECT max_pt()",
        "SELECT max_pt(NULL)",
        "SELECT max_pt(42)",
        "SELECT max_pt('empty', 'plain')",
        "SELECT max_pt(name) FROM (VALUES ('empty')) t(name)",
    ] {
        assert_error(&ctx, sql, "string literal").await;
    }
    assert_error(&ctx, "SELECT max_pt('plain')", "not a partitioned table").await;
    assert_error(
        &ctx,
        "SELECT max_pt('empty')",
        "no non-null partitions containing data files",
    )
    .await;
    execute(&ctx, "INSERT INTO empty VALUES (1, NULL)").await;
    assert_error(
        &ctx,
        "SELECT max_pt('empty')",
        "no non-null partitions containing data files",
    )
    .await;
    assert_error(
        &ctx,
        "SELECT max_pt('missing.default.t')",
        "Unknown catalog",
    )
    .await;
    assert_error(&ctx, "SELECT max_pt('missing')", "does not exist").await;
    assert_error(&ctx, "SELECT max_pt('empty$partitions')", "data table").await;
    assert_error(&ctx, "SELECT max_pt(DISTINCT 'empty')", "string literal").await;
    assert_error(&ctx, "SELECT max_pt('empty') OVER ()", "ordinary call").await;
    assert_error(
        &ctx,
        "SELECT max_pt('empty trailing')",
        "invalid table name",
    )
    .await;
}

#[tokio::test]
async fn max_pt_handles_quoted_tables_branches_and_insert_select() {
    let (_directory, ctx) = context().await;
    execute(
        &ctx,
        "CREATE TABLE \"Mixed.Table\" (id INT, p INT) PARTITIONED BY (p)",
    )
    .await;
    execute(&ctx, "INSERT INTO \"Mixed.Table\" VALUES (1, 9), (2, 10)").await;
    execute(
        &ctx,
        "CREATE TABLE branch_t (id INT, p INT) PARTITIONED BY (p)",
    )
    .await;
    execute(&ctx, "INSERT INTO branch_t VALUES (1, 10)").await;
    execute(
        &ctx,
        "CALL sys.create_tag(table => 'default.branch_t', tag => 'base')",
    )
    .await;
    execute(
        &ctx,
        "CALL sys.create_branch(table => 'default.branch_t', branch => 'test', tag => 'base')",
    )
    .await;
    execute(&ctx, "INSERT INTO branch_t VALUES (3, 42)").await;
    assert_eq!(
        scalar(&ctx, "SELECT max_pt('branch_t$branch_test')").await,
        ScalarValue::Utf8(Some("10".into()))
    );
    assert_eq!(
        scalar(&ctx, "SELECT max_pt('branch_t')").await,
        ScalarValue::Utf8(Some("42".into()))
    );
    assert_eq!(
        scalar(&ctx, "SELECT max_pt('\"Mixed.Table\"')").await,
        ScalarValue::Utf8(Some("10".into()))
    );
    execute(&ctx, "CREATE TABLE latest (p STRING)").await;
    execute(&ctx, "INSERT INTO latest SELECT max_pt('\"Mixed.Table\"')").await;
    assert_eq!(
        scalar(&ctx, "SELECT p FROM latest")
            .await
            .try_as_str()
            .flatten(),
        Some("10")
    );
    let batches = ctx
        .sql("SELECT max_pt('\"Mixed.Table\"')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        batches[0].schema().field(0).name(),
        "max_pt('\"Mixed.Table\"')"
    );
}

#[tokio::test]
async fn local_max_pt_registration_takes_precedence() {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::{create_udf, ColumnarValue, Volatility};

    let ctx = SQLContext::new();
    ctx.ctx().register_udf(create_udf(
        "max_pt",
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|_| {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                "local".into(),
            ))))
        }),
    ));
    for sql in [
        "SELECT max_pt('missing')",
        "SELECT MAX_PT('missing')",
        "SELECT \"max_pt\"('missing')",
    ] {
        assert_eq!(
            scalar(&ctx, sql).await,
            ScalarValue::Utf8(Some("local".into()))
        );
    }
    let aggregate = datafusion::functions_aggregate::first_last::first_value_udaf();
    ctx.ctx().deregister_udf("max_pt");
    ctx.ctx()
        .register_udaf(aggregate.as_ref().clone().with_aliases(["max_pt"]));
    assert_eq!(
        scalar(
            &ctx,
            "SELECT max_pt(CAST(v AS BIGINT)) FROM (VALUES (1), (2)) t(v)"
        )
        .await,
        ScalarValue::Int64(Some(1))
    );
}

#[tokio::test]
async fn max_pt_respects_disabled_identifier_normalization_for_catalogs() {
    let (directory, mut ctx) = context().await;
    let mut options = Options::new();
    options.set(
        CatalogOptions::WAREHOUSE,
        format!("file://{}", directory.path().display()),
    );
    ctx.register_catalog(
        "CaseCatalog",
        Arc::new(FileSystemCatalog::new(options).unwrap()),
    )
    .await
    .unwrap();
    execute(&ctx, "CREATE TABLE t (id INT, p INT) PARTITIONED BY (p)").await;
    execute(&ctx, "INSERT INTO t VALUES (1, 10)").await;
    execute(
        &ctx,
        "SET datafusion.sql_parser.enable_ident_normalization = false",
    )
    .await;
    assert_eq!(
        scalar(&ctx, "SELECT CaseCatalog.sys.max_pt('t')").await,
        ScalarValue::Utf8(Some("10".into()))
    );
}

#[tokio::test]
async fn max_pt_qualified_function_keeps_session_catalog_for_table_arguments() {
    let (_directory, mut ctx) = context().await;
    let other_directory = TempDir::new().unwrap();
    let mut options = Options::new();
    options.set(
        CatalogOptions::WAREHOUSE,
        format!("file://{}", other_directory.path().display()),
    );
    ctx.register_catalog("other", Arc::new(FileSystemCatalog::new(options).unwrap()))
        .await
        .unwrap();
    execute(
        &ctx,
        "CREATE TABLE paimon.default.t (id INT, p INT) PARTITIONED BY (p)",
    )
    .await;
    execute(
        &ctx,
        "CREATE TABLE other.default.t (id INT, p INT) PARTITIONED BY (p)",
    )
    .await;
    execute(&ctx, "INSERT INTO paimon.default.t VALUES (1, 11)").await;
    execute(&ctx, "INSERT INTO other.default.t VALUES (2, 22)").await;
    for sql in [
        "SELECT other.sys.max_pt('t')",
        "SELECT other.sys.max_pt('default.t')",
    ] {
        assert_eq!(
            scalar(&ctx, sql).await,
            ScalarValue::Utf8(Some("11".into()))
        );
    }
    assert_eq!(
        scalar(&ctx, "SELECT paimon.sys.max_pt('other.default.t')").await,
        ScalarValue::Utf8(Some("22".into()))
    );
}

#[tokio::test]
async fn max_pt_preserves_decimal_timestamp_and_floating_point_ordering() {
    let (_directory, ctx) = context().await;
    execute(
        &ctx,
        "CREATE TABLE decimals (id INT, p DECIMAL(10,2)) PARTITIONED BY (p)",
    )
    .await;
    execute(&ctx, "INSERT INTO decimals VALUES (1, 9.90), (2, 10.01)").await;
    assert_eq!(
        scalar(&ctx, "SELECT max_pt('decimals')").await,
        ScalarValue::Utf8(Some("10.01".into()))
    );
    execute(
        &ctx,
        "CREATE TABLE timestamps (id INT, p TIMESTAMP(6)) PARTITIONED BY (p)",
    )
    .await;
    execute(
        &ctx,
        "INSERT INTO timestamps VALUES (1, TIMESTAMP '2024-03-01 04:05:06.123400')",
    )
    .await;
    assert_eq!(
        scalar(&ctx, "SELECT max_pt('timestamps')").await,
        ScalarValue::Utf8(Some("2024-03-01 04:05:06.1234".into()))
    );
    execute(
        &ctx,
        "CREATE TABLE floats (id INT, p DOUBLE) PARTITIONED BY (p)",
    )
    .await;
    execute(
        &ctx,
        "INSERT INTO floats VALUES (1, 10.0), (2, CAST('NaN' AS DOUBLE))",
    )
    .await;
    assert_eq!(
        scalar(&ctx, "SELECT max_pt('floats')").await,
        ScalarValue::Utf8(Some("NaN".into()))
    );
}

#[tokio::test]
async fn max_pt_formats_local_zoned_timestamps_in_the_session_timezone() {
    let (_directory, ctx) = context().await;
    execute(
        &ctx,
        "CREATE TABLE zoned (id INT, p TIMESTAMP(6) WITH TIME ZONE) PARTITIONED BY (p)",
    )
    .await;
    execute(
        &ctx,
        "INSERT INTO zoned VALUES (1, to_timestamp('2024-03-01T04:05:06.123400Z'))",
    )
    .await;
    execute(&ctx, "SET datafusion.execution.time_zone = 'Asia/Shanghai'").await;
    assert_eq!(
        scalar(&ctx, "SELECT max_pt('zoned')").await,
        ScalarValue::Utf8(Some("2024-03-01 12:05:06.1234".into()))
    );
}

#[tokio::test]
async fn max_pt_uses_the_same_session_snapshot_as_table_scans() {
    let (_directory, ctx) = context().await;
    execute(&ctx, "CREATE TABLE t (id INT, p INT) PARTITIONED BY (p)").await;
    execute(&ctx, "INSERT INTO t VALUES (1, 1)").await;
    execute(&ctx, "INSERT INTO t VALUES (2, 2)").await;
    execute(&ctx, "SET 'paimon.scan.version' = '1'").await;
    assert_eq!(
        scalar(&ctx, "SELECT max_pt('t')").await,
        ScalarValue::Utf8(Some("1".into()))
    );
    assert_eq!(
        scalar(
            &ctx,
            "SELECT count(*) FROM t WHERE p = CAST(max_pt('t') AS INT)"
        )
        .await,
        ScalarValue::Int64(Some(1))
    );
    execute(&ctx, "RESET 'paimon.scan.version'").await;
    assert_eq!(
        scalar(&ctx, "SELECT max_pt('t')").await,
        ScalarValue::Utf8(Some("2".into()))
    );
}

#[tokio::test]
async fn max_pt_resolves_temporary_shadows_and_requires_a_paimon_data_table() {
    let (_directory, ctx) = context().await;
    execute(&ctx, "CREATE TABLE t (id INT, p INT) PARTITIONED BY (p)").await;
    execute(&ctx, "INSERT INTO t VALUES (1, 1)").await;
    execute(&ctx, "CREATE TEMPORARY VIEW t AS SELECT 100 AS p").await;
    assert_error(&ctx, "SELECT max_pt('t')", "requires a Paimon data table").await;
}

#[tokio::test]
async fn max_pt_decodes_binary_partition_values_as_paimon_strings() {
    let (_directory, ctx) = context().await;
    execute(
        &ctx,
        "CREATE TABLE t (id INT, p VARBINARY) PARTITIONED BY (p)",
    )
    .await;
    execute(&ctx, "INSERT INTO t VALUES (1, unhex('EDA080'))").await;
    assert_eq!(
        scalar(&ctx, "SELECT max_pt('t')").await,
        ScalarValue::Utf8(Some("\u{FFFD}".into()))
    );
}

#[tokio::test]
async fn max_pt_preserves_java_floating_point_strings() {
    let (_directory, ctx) = context().await;
    execute(
        &ctx,
        "CREATE TABLE singles (id INT, p FLOAT) PARTITIONED BY (p)",
    )
    .await;
    execute(&ctx, "INSERT INTO singles VALUES (1, 10000000.0)").await;
    assert_eq!(
        scalar(&ctx, "SELECT max_pt('singles')").await,
        ScalarValue::Utf8(Some("1.0E7".into()))
    );
    execute(
        &ctx,
        "CREATE TABLE doubles (id INT, p DOUBLE) PARTITIONED BY (p)",
    )
    .await;
    execute(&ctx, "INSERT INTO doubles VALUES (1, 10000000.0)").await;
    assert_eq!(
        scalar(&ctx, "SELECT max_pt('doubles')").await,
        ScalarValue::Utf8(Some("1.0E7".into()))
    );
}
