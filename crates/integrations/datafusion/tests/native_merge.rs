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

use arrow_array::UInt64Array;
use common::{collect_id_name, create_sql_context, create_test_env, exec};
use datafusion::logical_expr::{dml::WriteOp, LogicalPlan};

#[tokio::test]
async fn native_merge_defers_mutation_until_execution() {
    for options in [
        "",
        "WITH ('data-evolution.enabled' = 'true', 'row-tracking.enabled' = 'true')",
    ] {
        let (_tmp, catalog) = create_test_env();
        let ctx = create_sql_context(catalog).await;
        exec(&ctx, "CREATE SCHEMA paimon.test_db").await;
        exec(
            &ctx,
            &format!("CREATE TABLE paimon.test_db.target (id INT, name STRING) {options}"),
        )
        .await;
        exec(
            &ctx,
            "INSERT INTO paimon.test_db.target (id, name) VALUES (1, 'old'), (2, 'keep')",
        )
        .await;
        let sql = "MERGE INTO paimon.test_db.target AS t \
                   USING (VALUES (1, 'new'), (3, 'insert')) AS s(id, name) ON t.id = s.id \
                   WHEN MATCHED THEN UPDATE SET name = s.name \
                   WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)";
        let before = vec![(1, "old".to_string()), (2, "keep".to_string())];
        let query = "SELECT id, name FROM paimon.test_db.target ORDER BY id";

        let df = ctx.sql(sql).await.unwrap();
        assert!(
            matches!(df.logical_plan(), LogicalPlan::Dml(dml) if matches!(dml.op, WriteOp::MergeInto(_)))
        );
        assert_eq!(collect_id_name(&ctx, query).await, before);
        df.clone().create_physical_plan().await.unwrap();
        assert_eq!(collect_id_name(&ctx, query).await, before);
        exec(&ctx, &format!("EXPLAIN {sql}")).await;
        assert_eq!(collect_id_name(&ctx, query).await, before);

        let result = df.collect().await.unwrap();
        assert_eq!(
            result[0]
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            2
        );
        assert_eq!(
            collect_id_name(&ctx, query).await,
            vec![(1, "new".into()), (2, "keep".into()), (3, "insert".into())]
        );
    }
}

#[tokio::test]
async fn native_session_can_merge_registered_paimon_provider() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog).await;
    exec(&ctx, "CREATE SCHEMA paimon.test_db").await;
    exec(
        &ctx,
        "CREATE TABLE paimon.test_db.target (id INT, name STRING)",
    )
    .await;
    exec(&ctx, "INSERT INTO paimon.test_db.target VALUES (1, 'old')").await;
    // Bypass SQLContext: execution must be implemented by the table provider.
    ctx.ctx()
        .sql(
            "MERGE INTO paimon.test_db.target AS t \
                   USING (VALUES (1, 'new')) AS s(id, name) ON t.id = s.id \
                   WHEN MATCHED THEN UPDATE SET name = s.name",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        collect_id_name(&ctx, "SELECT id, name FROM paimon.test_db.target").await,
        vec![(1, "new".into())]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_merge_retries_with_fresh_target_and_one_source_capture() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use arrow_schema::DataType;
    use datafusion::logical_expr::{create_udf, Volatility};

    for (options, competing_sql, expected_value) in [
        ("", "UPDATE paimon.test_db.target SET value = 100 WHERE id = 1", 101),
        ("WITH ('row-tracking.enabled' = 'true')", "UPDATE paimon.test_db.target SET value = 100 WHERE id = 1", 101),
        ("WITH ('data-evolution.enabled' = 'true', 'row-tracking.enabled' = 'true', 'write-only' = 'true', 'compaction.min.file-num' = '2')", "CALL sys.compact(table => 'test_db.target')", 11),
    ] {
        let (_tmp, catalog) = create_test_env();
        let ctx = Arc::new(create_sql_context(catalog).await);
        exec(&ctx, "CREATE SCHEMA paimon.test_db").await;
        exec(
            &ctx,
            &format!("CREATE TABLE paimon.test_db.target (id INT, value INT) {options}"),
        )
        .await;
        exec(
            &ctx,
            "INSERT INTO paimon.test_db.target (id, value) VALUES (1, 10)",
        )
        .await;
        exec(&ctx, "INSERT INTO paimon.test_db.target (id, value) VALUES (3, 30)").await;

        let source_calls = Arc::new(AtomicUsize::new(0));
        let calls = source_calls.clone();
        ctx.ctx().register_udf(create_udf(
            "capture_source",
            vec![DataType::Int32],
            DataType::Int32,
            Volatility::Volatile,
            Arc::new(move |args| {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(args[0].clone())
            }),
        ));
        let target_calls = Arc::new(AtomicUsize::new(0));
        let calls = target_calls.clone();
        let weak = Arc::downgrade(&ctx);
        ctx.ctx().register_udf(create_udf(
            "interfere",
            vec![DataType::Int32],
            DataType::Int32,
            Volatility::Volatile,
            Arc::new(move |args| {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    let ctx = weak.upgrade().unwrap();
                    // Publish a competing change after matching and before the MERGE
                    // commit. No sleeps or scheduler-dependent race is needed.
                    tokio::task::block_in_place(|| {
                        tokio::runtime::Handle::current().block_on(async {
                            ctx.sql(competing_sql)
                                .await?
                                .collect()
                                .await
                        })
                    })?;
                }
                Ok(args[0].clone())
            }),
        ));

        let count = common::dml_count(&ctx,
            "MERGE INTO paimon.test_db.target AS t \
             USING (SELECT capture_source(id) AS id FROM (VALUES (CAST(1 AS INT)), (CAST(2 AS INT))) AS v(id)) AS s \
             ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET value = interfere(CAST(t.value + 1 AS INT)) \
             WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, 20)").await;
        assert_eq!(count, 2);
        assert_eq!(
            common::collect_id_value(&ctx, "SELECT id, value FROM paimon.test_db.target").await,
            vec![(1, expected_value), (2, 20), (3, 30)]
        );
        assert_eq!(
            source_calls.load(Ordering::SeqCst),
            1,
            "source must be replayed on retry"
        );
        assert_eq!(
            target_calls.load(Ordering::SeqCst),
            2,
            "the conflict must cause exactly one retry"
        );
    }
}

#[tokio::test]
async fn native_merge_duplicate_matches_do_not_publish_any_actions() {
    for options in [
        "",
        "WITH ('data-evolution.enabled' = 'true', 'row-tracking.enabled' = 'true')",
    ] {
        let (_tmp, catalog) = create_test_env();
        let ctx = create_sql_context(catalog).await;
        exec(&ctx, "CREATE SCHEMA paimon.test_db").await;
        exec(
            &ctx,
            &format!("CREATE TABLE paimon.test_db.target (id INT, name STRING) {options}"),
        )
        .await;
        exec(
            &ctx,
            "INSERT INTO paimon.test_db.target (id, name) VALUES (1, 'old')",
        )
        .await;
        common::assert_sql_error(
            &ctx,
            "MERGE INTO paimon.test_db.target AS t \
             USING (VALUES (1, 'a'), (1, 'b'), (2, 'insert')) AS s(id, name) ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
            "duplicate UPDATE",
        )
        .await;
        assert_eq!(
            collect_id_name(&ctx, "SELECT id, name FROM paimon.test_db.target").await,
            vec![(1, "old".into())]
        );
    }
}

#[tokio::test]
async fn native_merge_respects_dml_options() {
    use datafusion::execution::context::SQLOptions;

    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog).await;
    exec(&ctx, "CREATE SCHEMA paimon.test_db").await;
    exec(
        &ctx,
        "CREATE TABLE paimon.test_db.target (id INT, name STRING)",
    )
    .await;
    let result = ctx
        .sql_with_options(
            "MERGE INTO paimon.test_db.target AS t \
         USING (VALUES (1, 'new')) AS s(id, name) ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
            SQLOptions::new().with_allow_dml(false),
        )
        .await;
    assert!(result.is_err());
    assert!(
        collect_id_name(&ctx, "SELECT id, name FROM paimon.test_db.target")
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn data_evolution_positional_insert_and_explain_exclude_virtual_row_id() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog).await;
    exec(&ctx, "CREATE SCHEMA paimon.test_db").await;
    exec(&ctx, "CREATE TABLE paimon.test_db.target (id INT, name STRING) WITH ('data-evolution.enabled' = 'true', 'row-tracking.enabled' = 'true')").await;
    let sql = "MERGE INTO paimon.test_db.target AS t \
               USING (VALUES (1, 's.name')) AS s(id, name) ON t.id = s.id \
               WHEN NOT MATCHED AND id = 1 THEN INSERT VALUES (id, name)";
    exec(&ctx, &format!("EXPLAIN {sql}")).await;
    assert!(
        collect_id_name(&ctx, "SELECT id, name FROM paimon.test_db.target")
            .await
            .is_empty()
    );
    assert_eq!(common::dml_count(&ctx, sql).await, 1);
    assert_eq!(
        collect_id_name(&ctx, "SELECT id, name FROM paimon.test_db.target").await,
        vec![(1, "s.name".into())]
    );
    let batches = ctx
        .sql("SELECT \"_ROW_ID\" FROM paimon.test_db.target")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0),
        0
    );
}

#[tokio::test]
async fn cow_tracking_columns_do_not_shadow_user_columns() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog).await;
    exec(&ctx, "CREATE SCHEMA paimon.test_db").await;
    exec(&ctx, "CREATE TABLE paimon.test_db.target (id INT, value INT, \"__paimon_file_idx\" INT, \"__paimon_row_offset\" INT) WITH ('write-only' = 'true')").await;
    exec(
        &ctx,
        "INSERT INTO paimon.test_db.target VALUES (1, 10, 99, 98)",
    )
    .await;
    exec(
        &ctx,
        "INSERT INTO paimon.test_db.target VALUES (2, 20, 97, 96)",
    )
    .await;
    assert_eq!(
        common::dml_count(
            &ctx,
            "MERGE INTO paimon.test_db.target t USING (VALUES (1, 100)) AS s(id, value) \
         ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value"
        )
        .await,
        1
    );
    assert_eq!(
        common::collect_id_value(&ctx, "SELECT id, value FROM paimon.test_db.target").await,
        vec![(1, 100), (2, 20)]
    );
    let batches = ctx.sql("SELECT id, \"__paimon_file_idx\", \"__paimon_row_offset\" FROM paimon.test_db.target ORDER BY id")
        .await.unwrap().collect().await.unwrap();
    let mut rows = common::collect_three_ints(&batches);
    rows.sort_unstable();
    assert_eq!(rows, vec![(1, 99, 98), (2, 97, 96)]);
}

#[tokio::test]
async fn native_merge_retains_session_variable_providers() {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::common::ScalarValue;
    use datafusion::variable::{VarProvider, VarType};
    use std::sync::Arc;

    #[derive(Debug)]
    struct Variables;
    impl VarProvider for Variables {
        fn get_value(&self, _names: Vec<String>) -> datafusion::error::Result<ScalarValue> {
            Ok(ScalarValue::Int32(Some(7)))
        }
        fn get_type(&self, _names: &[String]) -> Option<DataType> {
            Some(DataType::Int32)
        }
    }
    for options in [
        "",
        "WITH ('data-evolution.enabled' = 'true', 'row-tracking.enabled' = 'true')",
    ] {
        let (_tmp, catalog) = create_test_env();
        let ctx = create_sql_context(catalog).await;
        ctx.ctx()
            .register_variable(VarType::UserDefined, Arc::new(Variables));
        ctx.ctx()
            .register_variable(VarType::System, Arc::new(Variables));
        exec(&ctx, "CREATE SCHEMA paimon.test_db").await;
        exec(
            &ctx,
            &format!("CREATE TABLE paimon.test_db.target (id INT, value INT) {options}"),
        )
        .await;
        exec(
            &ctx,
            "INSERT INTO paimon.test_db.target (id, value) VALUES (1, 10)",
        )
        .await;
        assert_eq!(
            common::dml_count(
                &ctx,
                "MERGE INTO paimon.test_db.target t USING (VALUES (1)) AS s(id) \
             ON t.id = s.id AND @@flag = 7 \
             WHEN MATCHED THEN UPDATE SET value = @delta"
            )
            .await,
            1
        );
        assert_eq!(
            common::dml_count(
                &ctx,
                "MERGE INTO paimon.test_db.target t USING (VALUES (1), (2)) AS s(id) \
             ON t.id = s.id AND @delta = 7 \
             WHEN MATCHED THEN UPDATE SET value = @delta \
             WHEN NOT MATCHED AND @delta = 7 THEN INSERT (id, value) VALUES (s.id, @delta)"
            )
            .await,
            2
        );
        assert_eq!(
            common::collect_id_value(&ctx, "SELECT id, value FROM paimon.test_db.target").await,
            vec![(1, 7), (2, 7)]
        );
    }
}

#[tokio::test]
async fn merge_insert_resolves_unqualified_nested_fields_against_source() {
    for options in [
        "",
        "WITH ('data-evolution.enabled' = 'true', 'row-tracking.enabled' = 'true')",
    ] {
        let (_tmp, catalog) = create_test_env();
        let ctx = create_sql_context(catalog).await;
        exec(&ctx, "CREATE SCHEMA paimon.test_db").await;
        exec(&ctx, &format!("CREATE TABLE paimon.test_db.target (id INT, value INT, payload STRUCT<foo INT>) {options}")).await;
        assert_eq!(
            common::dml_count(
                &ctx,
                "MERGE INTO paimon.test_db.target t \
             USING (SELECT 1 AS id, named_struct('foo', 42) AS payload) s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, value) VALUES (id, payload.foo)"
            )
            .await,
            1
        );
        assert_eq!(
            common::collect_id_value(&ctx, "SELECT id, value FROM paimon.test_db.target").await,
            vec![(1, 42)]
        );
    }
}

#[tokio::test]
async fn failed_merge_insert_discards_prepared_cow_files() {
    fn parquet_files(path: &std::path::Path) -> std::collections::BTreeSet<std::path::PathBuf> {
        let mut files = std::collections::BTreeSet::new();
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                files.extend(parquet_files(&path));
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                files.insert(path);
            }
        }
        files
    }
    let (tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog).await;
    exec(&ctx, "CREATE SCHEMA paimon.test_db").await;
    exec(
        &ctx,
        "CREATE TABLE paimon.test_db.target (id INT, value INT, pt STRING) PARTITIONED BY (pt)",
    )
    .await;
    exec(
        &ctx,
        "INSERT INTO paimon.test_db.target VALUES (1, 10, 'a')",
    )
    .await;
    // An unavailable destination affects only the new INSERT partition, after
    // rewriting the existing partition has succeeded.
    std::fs::write(
        tmp.path().join("test_db.db/target/pt=b"),
        b"blocked directory",
    )
    .unwrap();
    let before = parquet_files(tmp.path());
    common::assert_sql_error(
        &ctx,
        "MERGE INTO paimon.test_db.target t \
         USING (VALUES (1, 100, 'a'), (2, 200, 'b')) AS s(id, value, pt) \
         ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value \
         WHEN NOT MATCHED THEN INSERT (id, value, pt) VALUES (s.id, s.value, s.pt)",
        "Failed to create directory",
    )
    .await;
    assert_eq!(
        common::collect_id_value(&ctx, "SELECT id, value FROM paimon.test_db.target").await,
        vec![(1, 10)]
    );
    assert_eq!(parquet_files(tmp.path()), before);
}

#[tokio::test]
async fn merge_insert_preserves_lambda_scopes() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog).await;
    exec(&ctx, "SET datafusion.sql_parser.dialect = 'spark'").await;
    exec(&ctx, "CREATE SCHEMA paimon.test_db").await;
    exec(
        &ctx,
        "CREATE TABLE paimon.test_db.target (id INT, value INT)",
    )
    .await;
    assert_eq!(
        common::dml_count(
            &ctx,
            "MERGE INTO paimon.test_db.target t USING (SELECT 1 AS id) s ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT (id, value) \
         VALUES (id, array_length(array_filter([1, 2, 3], x -> x > 1)))"
        )
        .await,
        1
    );
    assert_eq!(
        common::collect_id_value(&ctx, "SELECT id, value FROM paimon.test_db.target").await,
        vec![(1, 2)]
    );
}

#[tokio::test]
async fn merge_unconditional_delete_collapses_duplicate_source_matches() {
    for options in ["", "WITH ('data-evolution.enabled' = 'true', 'row-tracking.enabled' = 'true', 'deletion-vectors.enabled' = 'true')"] {
        let (_tmp, catalog) = create_test_env();
        let ctx = create_sql_context(catalog).await;
        exec(&ctx, "CREATE SCHEMA paimon.test_db").await;
        exec(&ctx, &format!("CREATE TABLE paimon.test_db.target (id INT, value INT) {options}")).await;
        exec(&ctx, "INSERT INTO paimon.test_db.target (id, value) VALUES (1, 10), (3, 30)").await;
        assert_eq!(common::dml_count(&ctx,
            "MERGE INTO paimon.test_db.target t USING (VALUES (1), (1), (2)) AS s(id) \
             ON t.id = s.id WHEN MATCHED THEN DELETE \
             WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, 20)").await, 2);
        assert_eq!(common::collect_id_value(&ctx, "SELECT id, value FROM paimon.test_db.target").await, vec![(2, 20), (3, 30)]);
    }
}

#[tokio::test]
async fn merge_does_not_evaluate_not_matched_predicate_on_matched_rows() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog).await;
    exec(&ctx, "CREATE SCHEMA paimon.test_db").await;
    exec(
        &ctx,
        "CREATE TABLE paimon.test_db.target (id INT, value INT)",
    )
    .await;
    exec(&ctx, "INSERT INTO paimon.test_db.target VALUES (1, 10)").await;
    assert_eq!(
        common::dml_count(
            &ctx,
            "MERGE INTO paimon.test_db.target t USING (VALUES (1, 0), (2, 1)) AS s(id, divisor) \
         ON t.id = s.id WHEN NOT MATCHED AND 10 / s.divisor > 1 \
         THEN INSERT (id, value) VALUES (s.id, 20)"
        )
        .await,
        1
    );
    assert_eq!(
        common::collect_id_value(&ctx, "SELECT id, value FROM paimon.test_db.target").await,
        vec![(1, 10), (2, 20)]
    );
}

#[tokio::test]
async fn merge_not_matched_rejects_target_column_dependencies() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog).await;
    exec(&ctx, "CREATE SCHEMA paimon.test_db").await;
    exec(
        &ctx,
        "CREATE TABLE paimon.test_db.target (id INT, value INT)",
    )
    .await;
    common::assert_sql_error(
        &ctx,
        "MERGE INTO paimon.test_db.target t USING (VALUES (1)) AS s(id) ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, t.value)",
        "source columns",
    )
    .await;
}

#[tokio::test]
async fn native_merge_does_not_read_unused_source_blob_columns() {
    use paimon::spec::BlobDescriptor;
    for options in [
        "",
        "WITH ('data-evolution.enabled' = 'true', 'row-tracking.enabled' = 'true')",
    ] {
        let (tmp, catalog) = create_test_env();
        let ctx = create_sql_context(catalog).await;
        exec(&ctx, "CREATE SCHEMA paimon.test_db").await;
        exec(
            &ctx,
            &format!("CREATE TABLE paimon.test_db.target (id INT, name STRING) {options}"),
        )
        .await;
        exec(
            &ctx,
            "INSERT INTO paimon.test_db.target (id, name) VALUES (1, 'old')",
        )
        .await;
        exec(&ctx, "CREATE TABLE paimon.test_db.source (id INT, name STRING, picture BLOB, marker INT) WITH ('data-evolution.enabled' = 'true', 'row-tracking.enabled' = 'true', 'blob-descriptor-field' = 'picture')").await;
        let path = tmp.path().join("payload.bin");
        std::fs::write(&path, b"data").unwrap();
        let descriptor = BlobDescriptor::new(path.to_str().unwrap().to_string(), 0, 4).serialize();
        let hex = descriptor
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        exec(
            &ctx,
            &format!(
                "INSERT INTO paimon.test_db.source (id, name, picture, marker) VALUES (1, 'new', X'{hex}', 1)"
            ),
        )
        .await;
        std::fs::remove_file(path).unwrap();
        for (source, name) in [
            ("paimon.test_db.source", "name"),
            ("(SELECT * FROM paimon.test_db.source WHERE id = 1)", "name"),
            (
                "(SELECT * FROM paimon.test_db.source WHERE marker = 1)",
                "name",
            ),
            (
                "(SELECT name AS renamed, id, picture FROM paimon.test_db.source WHERE marker = 1)",
                "renamed",
            ),
        ] {
            let result = ctx
                .ctx()
                .sql(&format!(
                    "MERGE INTO paimon.test_db.target t USING {source} s \
                 ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = s.{name}"
                ))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            assert_eq!(
                result[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(0),
                1
            );
        }
        assert_eq!(
            collect_id_name(&ctx, "SELECT id, name FROM paimon.test_db.target").await,
            vec![(1, "new".into())]
        );
    }
}

#[tokio::test]
async fn native_merge_preserves_source_cardinality_with_zero_column_projection() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog).await;
    exec(&ctx, "CREATE SCHEMA paimon.test_db").await;
    exec(
        &ctx,
        "CREATE TABLE paimon.test_db.target (id INT, value INT)",
    )
    .await;
    exec(&ctx, "CREATE TABLE paimon.test_db.source (unused INT)").await;
    exec(&ctx, "INSERT INTO paimon.test_db.source VALUES (1), (2)").await;
    assert_eq!(
        common::dml_count(
            &ctx,
            "MERGE INTO paimon.test_db.target t USING paimon.test_db.source s ON FALSE \
         WHEN NOT MATCHED THEN INSERT (id, value) VALUES (42, 7)"
        )
        .await,
        2
    );
    assert_eq!(
        common::collect_id_value(&ctx, "SELECT id, value FROM paimon.test_db.target").await,
        vec![(42, 7), (42, 7)]
    );
}
