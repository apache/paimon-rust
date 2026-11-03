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

//! MERGE INTO unit and behavioral tests.

use super::*;
use datafusion::sql::sqlparser::ast::Merge;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use paimon::{CatalogOptions, FileSystemCatalog, Options};
use tempfile::TempDir;

use crate::SQLContext;

#[tokio::test]
async fn test_retry_limits_and_non_conflict_errors() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    for (message, classify) in [
        (
            "Row ID conflict",
            is_row_id_conflict as fn(&DataFusionError) -> bool,
        ),
        (
            "Delete conflict",
            is_delete_conflict as fn(&DataFusionError) -> bool,
        ),
    ] {
        let attempts = AtomicUsize::new(0);
        let result: DFResult<u64> = retry_on_conflict("MERGE INTO", classify, || async {
            attempts.fetch_add(1, Ordering::SeqCst);
            Err(DataFusionError::External(Box::new(std::io::Error::other(
                message,
            ))))
        })
        .await;
        assert_eq!(attempts.load(Ordering::SeqCst), 5);
        let err = result.unwrap_err().to_string();
        assert!(err.contains("after 5 retries"), "{err}");
        assert!(err.contains(message), "{err}");

        let attempts = AtomicUsize::new(0);
        let result: DFResult<u64> = retry_on_conflict("MERGE INTO", classify, || async {
            attempts.fetch_add(1, Ordering::SeqCst);
            Err(DataFusionError::Execution("unrelated failure".into()))
        })
        .await;
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert!(
            matches!(result, Err(DataFusionError::Execution(message)) if message == "unrelated failure")
        );
        let overlapping_update = to_datafusion_error(paimon::Error::DataInvalid {
            message: "For Data Evolution table, multiple MERGE INTO operations have encountered conflicts, updating the same file".into(),
            source: None,
        });
        assert!(!classify(&overlapping_update));
    }
}

async fn setup_sql_context() -> (TempDir, SQLContext) {
    let temp_dir = TempDir::new().unwrap();
    let warehouse = format!("file://{}", temp_dir.path().display());
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    let catalog = Arc::new(FileSystemCatalog::new(options).unwrap());

    let mut sql_context = SQLContext::new();
    sql_context
        .register_catalog("paimon", catalog)
        .await
        .unwrap();
    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .unwrap();

    (temp_dir, sql_context)
}

async fn setup_data_evolution_table(name: &str) -> (TempDir, SQLContext) {
    let (tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(&format!(
            "CREATE TABLE paimon.test_db.{name} (id INT, name VARCHAR, value INT) WITH ('data-evolution.enabled' = 'true', 'row-tracking.enabled' = 'true')"
        ))
        .await
        .unwrap();

    sql_context
        .sql(&format!(
            "INSERT INTO paimon.test_db.{name} (id, name, value) VALUES (1, 'alice', 10), (2, 'bob', 20), (3, 'charlie', 30)"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    (tmp, sql_context)
}

// Exercise the native planner and provider while retaining these behavioral fixtures.
async fn execute_merge_into(ctx: &SQLContext, merge: &Merge) -> DFResult<DataFrame> {
    let batches = ctx.sql(&merge.to_string()).await?.collect().await?;
    ctx.ctx().read_batches(batches)
}

fn parse_merge(sql: &str) -> Merge {
    let dialect = GenericDialect {};
    let stmts = Parser::parse_sql(&dialect, sql).unwrap();
    match stmts.into_iter().next().unwrap() {
        datafusion::sql::sqlparser::ast::Statement::Merge(m) => m,
        _ => panic!("Expected MERGE statement"),
    }
}

#[tokio::test]
async fn test_merge_into_updates_matched_rows() {
    let (_tmp, sql_context) = setup_data_evolution_table("t_merge").await;

    // Create source table with updates
    sql_context
        .sql("CREATE TABLE paimon.test_db.source (id INT, name VARCHAR)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    sql_context
        .sql("INSERT INTO paimon.test_db.source VALUES (1, 'ALICE'), (3, 'CHARLIE')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Execute MERGE INTO
    let merge = parse_merge(
        "MERGE INTO paimon.test_db.t_merge t USING paimon.test_db.source s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET NAME = s.name",
    );
    execute_merge_into(&sql_context, &merge).await.unwrap();

    let batches = sql_context
        .sql("SELECT id, name, value FROM paimon.test_db.t_merge ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringViewArray>()
            .unwrap();
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i), names.value(i).to_string(), values.value(i)));
        }
    }

    assert_eq!(
        rows,
        vec![
            (1, "ALICE".to_string(), 10),
            (2, "bob".to_string(), 20),
            (3, "CHARLIE".to_string(), 30),
        ]
    );
}

#[tokio::test]
async fn test_merge_into_no_matches() {
    let (_tmp, sql_context) = setup_data_evolution_table("t_merge2").await;

    sql_context
        .sql("CREATE TABLE paimon.test_db.source (id INT, name VARCHAR)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    sql_context
        .sql("INSERT INTO paimon.test_db.source VALUES (99, 'nobody')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let merge = parse_merge(
        "MERGE INTO paimon.test_db.t_merge2 t USING paimon.test_db.source s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET name = s.name",
    );
    let result = execute_merge_into(&sql_context, &merge).await.unwrap();
    let batches = result.collect().await.unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 0);
}

// -----------------------------------------------------------------------
// CoW MERGE INTO tests (append-only tables)
// -----------------------------------------------------------------------

async fn setup_append_only_table(name: &str) -> (TempDir, SQLContext) {
    let (tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(&format!(
            "CREATE TABLE paimon.test_db.{name} (id INT, name VARCHAR, value INT)"
        ))
        .await
        .unwrap();

    sql_context
        .sql(&format!(
            "INSERT INTO paimon.test_db.{name} (id, name, value) VALUES (1, 'alice', 10), (2, 'bob', 20), (3, 'charlie', 30)"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    (tmp, sql_context)
}

fn collect_rows(batches: &[RecordBatch]) -> Vec<(i32, String, i32)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringViewArray>()
            .unwrap();
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i), names.value(i).to_string(), values.value(i)));
        }
    }
    rows.sort_by_key(|r| r.0);
    rows
}

#[tokio::test]
async fn test_cow_merge_update_matched_rows() {
    let (_tmp, sql_context) = setup_append_only_table("t_cow_upd").await;

    sql_context
        .sql("CREATE TABLE paimon.test_db.source (id INT, name VARCHAR)")
        .await
        .unwrap();
    sql_context
        .sql("INSERT INTO paimon.test_db.source (id, name) VALUES (1, 'ALICE'), (3, 'CHARLIE')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let merge = parse_merge(
        "MERGE INTO paimon.test_db.t_cow_upd t USING paimon.test_db.source s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET NAME = s.name",
    );
    execute_merge_into(&sql_context, &merge).await.unwrap();

    let batches = sql_context
        .sql("SELECT id, name, value FROM paimon.test_db.t_cow_upd ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_rows(&batches);
    assert_eq!(
        rows,
        vec![
            (1, "ALICE".to_string(), 10),
            (2, "bob".to_string(), 20),
            (3, "CHARLIE".to_string(), 30),
        ]
    );
}

#[tokio::test]
async fn test_cow_merge_delete_matched_rows() {
    let (_tmp, sql_context) = setup_append_only_table("t_cow_del").await;

    sql_context
        .sql("CREATE TABLE paimon.test_db.source (id INT)")
        .await
        .unwrap();
    sql_context
        .sql("INSERT INTO paimon.test_db.source (id) VALUES (2)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let merge = parse_merge(
        "MERGE INTO paimon.test_db.t_cow_del t USING paimon.test_db.source s ON t.id = s.id \
         WHEN MATCHED THEN DELETE",
    );
    execute_merge_into(&sql_context, &merge).await.unwrap();

    let batches = sql_context
        .sql("SELECT id, name, value FROM paimon.test_db.t_cow_del ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_rows(&batches);
    assert_eq!(
        rows,
        vec![(1, "alice".to_string(), 10), (3, "charlie".to_string(), 30),]
    );
}

#[tokio::test]
async fn test_cow_merge_insert_not_matched() {
    let (_tmp, sql_context) = setup_append_only_table("t_cow_ins").await;

    sql_context
        .sql("CREATE TABLE paimon.test_db.source (id INT, name VARCHAR, value INT)")
        .await
        .unwrap();
    sql_context
        .sql("INSERT INTO paimon.test_db.source VALUES (4, 'dave', 40), (5, 'eve', 50)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let merge = parse_merge(
        "MERGE INTO paimon.test_db.t_cow_ins t USING paimon.test_db.source s ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (s.id, s.name, s.value)",
    );
    execute_merge_into(&sql_context, &merge).await.unwrap();

    let batches = sql_context
        .sql("SELECT id, name, value FROM paimon.test_db.t_cow_ins ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_rows(&batches);
    assert_eq!(
        rows,
        vec![
            (1, "alice".to_string(), 10),
            (2, "bob".to_string(), 20),
            (3, "charlie".to_string(), 30),
            (4, "dave".to_string(), 40),
            (5, "eve".to_string(), 50),
        ]
    );
}

#[tokio::test]
async fn test_cow_merge_insert_not_matched_without_columns() {
    let (_tmp, sql_context) = setup_append_only_table("t_cow_ins_nocols").await;

    sql_context
        .sql("CREATE TABLE paimon.test_db.source (id INT, name VARCHAR, value INT)")
        .await
        .unwrap();
    sql_context
        .sql("INSERT INTO paimon.test_db.source VALUES (4, 'dave', 40), (5, 'eve', 50)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // No column list: the VALUES map to the table's columns by position.
    // Before the fix this dropped the source values and inserted all-NULL rows.
    let merge = parse_merge(
        "MERGE INTO paimon.test_db.t_cow_ins_nocols t USING paimon.test_db.source s \
         ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.value)",
    );
    execute_merge_into(&sql_context, &merge).await.unwrap();

    let batches = sql_context
        .sql("SELECT id, name, value FROM paimon.test_db.t_cow_ins_nocols ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_rows(&batches);
    assert_eq!(
        rows,
        vec![
            (1, "alice".to_string(), 10),
            (2, "bob".to_string(), 20),
            (3, "charlie".to_string(), 30),
            (4, "dave".to_string(), 40),
            (5, "eve".to_string(), 50),
        ]
    );
}

#[tokio::test]
async fn test_cow_merge_insert_arity_validated_even_when_all_matched() {
    let (_tmp, sql_context) = setup_append_only_table("t_cow_ins_arity").await;

    sql_context
        .sql("CREATE TABLE paimon.test_db.source (id INT, name VARCHAR, value INT)")
        .await
        .unwrap();
    // id = 1 matches the target, so the NOT MATCHED INSERT branch is never
    // built — the arity check must still run upfront.
    sql_context
        .sql("INSERT INTO paimon.test_db.source VALUES (1, 'ALICE', 99)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // The column-less INSERT lists 2 values for a 3-column table. Even with no
    // row to insert, the invalid arity must be rejected before execution so the
    // matched UPDATE is not applied.
    let merge = parse_merge(
        "MERGE INTO paimon.test_db.t_cow_ins_arity t USING paimon.test_db.source s \
         ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET value = s.value \
         WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name)",
    );
    let err = execute_merge_into(&sql_context, &merge).await.unwrap_err();
    assert!(err.to_string().contains("column(s) but"), "got {err}");

    // The target is unchanged: the matched UPDATE did not apply.
    let batches = sql_context
        .sql("SELECT id, name, value FROM paimon.test_db.t_cow_ins_arity ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        collect_rows(&batches),
        vec![
            (1, "alice".to_string(), 10),
            (2, "bob".to_string(), 20),
            (3, "charlie".to_string(), 30),
        ]
    );
}

#[tokio::test]
async fn test_cow_merge_update_and_insert() {
    let (_tmp, sql_context) = setup_append_only_table("t_cow_upsert").await;

    sql_context
        .sql("CREATE TABLE paimon.test_db.source (id INT, name VARCHAR, value INT)")
        .await
        .unwrap();
    sql_context
        .sql("INSERT INTO paimon.test_db.source VALUES (2, 'BOB', 200), (4, 'dave', 40)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let merge = parse_merge(
        "MERGE INTO paimon.test_db.t_cow_upsert t USING paimon.test_db.source s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET name = s.name, value = s.value \
         WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (s.id, s.name, s.value)",
    );
    execute_merge_into(&sql_context, &merge).await.unwrap();

    let batches = sql_context
        .sql("SELECT id, name, value FROM paimon.test_db.t_cow_upsert ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_rows(&batches);
    assert_eq!(
        rows,
        vec![
            (1, "alice".to_string(), 10),
            (2, "BOB".to_string(), 200),
            (3, "charlie".to_string(), 30),
            (4, "dave".to_string(), 40),
        ]
    );
}

#[tokio::test]
async fn test_cow_merge_matched_null_predicates_fall_through() {
    for delete_fallback in [false, true] {
        let (_tmp, sql_context) = setup_append_only_table("t_null_matched").await;
        for sql in [
            "CREATE TABLE paimon.test_db.source (id INT, flag INT)",
            "INSERT INTO paimon.test_db.source VALUES (1, NULL), (2, 1), (3, 0)",
        ] {
            sql_context.sql(sql).await.unwrap().collect().await.unwrap();
        }

        let fallback = if delete_fallback {
            "DELETE"
        } else {
            "UPDATE SET name = 'fallback'"
        };
        let merge = parse_merge(&format!(
            "MERGE INTO paimon.test_db.t_null_matched t USING paimon.test_db.source s \
             ON t.id = s.id \
             WHEN MATCHED AND s.flag = 1 THEN UPDATE SET name = 'first' \
             WHEN MATCHED AND s.flag = 0 THEN UPDATE SET name = 'second' \
             WHEN MATCHED THEN {fallback}"
        ));
        let result = execute_merge_into(&sql_context, &merge)
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
            3,
        );

        let batches = sql_context
            .sql("SELECT id, name, value FROM paimon.test_db.t_null_matched ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut expected = vec![(2, "first".to_string(), 20), (3, "second".to_string(), 30)];
        if !delete_fallback {
            expected.insert(0, (1, "fallback".to_string(), 10));
        }
        assert_eq!(collect_rows(&batches), expected);
    }
}

#[tokio::test]
async fn test_merge_insert_null_predicates_fall_through() {
    for data_evolution in [false, true] {
        let (_tmp, sql_context) = if data_evolution {
            setup_data_evolution_table("t_null_insert").await
        } else {
            setup_append_only_table("t_null_insert").await
        };
        for sql in [
            "CREATE TABLE paimon.test_db.source (id INT, flag INT)",
            "INSERT INTO paimon.test_db.source VALUES (4, NULL), (5, 1), (6, 0)",
        ] {
            sql_context.sql(sql).await.unwrap().collect().await.unwrap();
        }

        let merge = parse_merge(
            "MERGE INTO paimon.test_db.t_null_insert t USING paimon.test_db.source s \
             ON t.id = s.id \
             WHEN NOT MATCHED AND s.flag = 1 THEN INSERT (id, name, value) VALUES (s.id, 'first', 100) \
             WHEN NOT MATCHED AND s.flag = 0 THEN INSERT (id, name, value) VALUES (s.id, 'second', 100) \
             WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (s.id, 'fallback', 100)",
        );
        let result = execute_merge_into(&sql_context, &merge)
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
            3,
        );

        let batches = sql_context
            .sql("SELECT id, name, value FROM paimon.test_db.t_null_insert ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            collect_rows(&batches),
            vec![
                (1, "alice".to_string(), 10),
                (2, "bob".to_string(), 20),
                (3, "charlie".to_string(), 30),
                (4, "fallback".to_string(), 100),
                (5, "first".to_string(), 100),
                (6, "second".to_string(), 100),
            ],
        );
    }
}

#[tokio::test]
async fn test_cow_merge_matched_or_predicates_preserve_clause_order() {
    for delete_second in [false, true] {
        let (_tmp, sql_context) = setup_append_only_table("t_or_matched").await;
        for sql in [
            "CREATE TABLE paimon.test_db.source (id INT, flag INT)",
            "INSERT INTO paimon.test_db.source VALUES (1, 1), (2, 0), (3, NULL)",
        ] {
            sql_context.sql(sql).await.unwrap().collect().await.unwrap();
        }

        let second_action = if delete_second {
            "DELETE"
        } else {
            "UPDATE SET name = 'second'"
        };
        let merge = parse_merge(&format!(
            "MERGE INTO paimon.test_db.t_or_matched t USING paimon.test_db.source s \
             ON t.id = s.id \
             WHEN MATCHED AND s.flag = 1 THEN UPDATE SET name = 'first' \
             WHEN MATCHED AND s.flag = 0 OR s.flag = 1 THEN {second_action} \
             WHEN MATCHED THEN UPDATE SET name = 'fallback'"
        ));
        let result = execute_merge_into(&sql_context, &merge)
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
            3,
        );

        let batches = sql_context
            .sql("SELECT id, name, value FROM paimon.test_db.t_or_matched ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut expected = vec![
            (1, "first".to_string(), 10),
            (3, "fallback".to_string(), 30),
        ];
        if !delete_second {
            expected.insert(1, (2, "second".to_string(), 20));
        }
        assert_eq!(collect_rows(&batches), expected);
    }
}

#[tokio::test]
async fn test_merge_insert_or_predicates_preserve_clause_order() {
    for data_evolution in [false, true] {
        let (_tmp, sql_context) = if data_evolution {
            setup_data_evolution_table("t_or_insert").await
        } else {
            setup_append_only_table("t_or_insert").await
        };
        for sql in [
            "CREATE TABLE paimon.test_db.source (id INT, flag INT)",
            "INSERT INTO paimon.test_db.source VALUES (4, 1), (5, 0), (6, NULL)",
        ] {
            sql_context.sql(sql).await.unwrap().collect().await.unwrap();
        }

        let merge = parse_merge(
            "MERGE INTO paimon.test_db.t_or_insert t USING paimon.test_db.source s \
             ON t.id = s.id \
             WHEN NOT MATCHED AND s.flag = 1 THEN INSERT (id, name, value) VALUES (s.id, 'first', 100) \
             WHEN NOT MATCHED AND s.flag = 0 OR s.flag = 1 THEN INSERT (id, name, value) VALUES (s.id, 'second', 100) \
             WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (s.id, 'fallback', 100)",
        );
        let result = execute_merge_into(&sql_context, &merge)
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
            3,
        );

        let batches = sql_context
            .sql("SELECT id, name, value FROM paimon.test_db.t_or_insert ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            collect_rows(&batches),
            vec![
                (1, "alice".to_string(), 10),
                (2, "bob".to_string(), 20),
                (3, "charlie".to_string(), 30),
                (4, "first".to_string(), 100),
                (5, "second".to_string(), 100),
                (6, "fallback".to_string(), 100),
            ],
        );
    }
}

#[tokio::test]
async fn test_cow_merge_preserves_string_literals() {
    for (on_predicate, matched_predicate, insert_predicate) in [
        ("", "", ""),
        (" AND s.name = 'paimon.test_db.source.payload'", "", ""),
        ("", " AND s.name = 'paimon.test_db.source.payload'", ""),
        ("", "", " AND s.name = 'paimon.test_db.source.payload'"),
    ] {
        let (_tmp, sql_context) = setup_append_only_table("target").await;
        for sql in [
            "CREATE TABLE paimon.test_db.source (id INT, name VARCHAR)",
            "INSERT INTO paimon.test_db.source VALUES (1, 'paimon.test_db.source.payload'), (4, 'paimon.test_db.source.payload')",
        ] {
            sql_context.sql(sql).await.unwrap().collect().await.unwrap();
        }

        let merge = parse_merge(&format!(
            "MERGE INTO paimon.test_db.target t USING paimon.test_db.source s \
             ON t.id = s.id{on_predicate} \
             WHEN MATCHED{matched_predicate} THEN UPDATE SET name = 'paimon.test_db.target.payload' \
             WHEN NOT MATCHED{insert_predicate} THEN INSERT (id, name, value) VALUES (s.id, 'paimon.test_db.source.payload', 40)"
        ));
        let result = execute_merge_into(&sql_context, &merge)
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
            2,
        );

        let batches = sql_context
            .sql("SELECT id, name, value FROM paimon.test_db.target ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            collect_rows(&batches),
            vec![
                (1, "paimon.test_db.target.payload".to_string(), 10),
                (2, "bob".to_string(), 20),
                (3, "charlie".to_string(), 30),
                (4, "paimon.test_db.source.payload".to_string(), 40),
            ],
            "ON: {on_predicate}, MATCHED: {matched_predicate}, INSERT: {insert_predicate}",
        );
    }
}

#[tokio::test]
async fn test_cow_merge_no_matches() {
    let (_tmp, sql_context) = setup_append_only_table("t_cow_nomatch").await;

    sql_context
        .sql("CREATE TABLE paimon.test_db.source (id INT, name VARCHAR)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let merge = parse_merge(
        "MERGE INTO paimon.test_db.t_cow_nomatch t USING paimon.test_db.source s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET name = s.name",
    );
    let result = execute_merge_into(&sql_context, &merge).await.unwrap();
    let batches = result.collect().await.unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 0);
}

#[test]
fn source_partition_pruning_requires_a_conjunctive_target_source_equality() {
    let target = column_name(1, 2);
    let source = column_name(2, 2);
    for on in [
        col(&target).eq(col(&source)),
        col(&source).eq(col(&target)),
        col(&target)
            .eq(col(&source))
            .and(col(column_name(0, 2)).gt(lit(0))),
    ] {
        assert_eq!(partition_source_index(&on, &target, 2, 4), Some(0));
    }
    for on in [
        col(&target)
            .eq(col(&source))
            .or(col(column_name(0, 2)).eq(lit(0))),
        col(&target).eq(col(column_name(0, 2))),
        col(column_name(0, 2)).eq(col(&source)),
    ] {
        assert_eq!(partition_source_index(&on, &target, 2, 4), None);
    }
}
