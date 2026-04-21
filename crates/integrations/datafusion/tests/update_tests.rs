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

//! UPDATE integration tests for append-only tables (CoW path).
//!
//! Test cases adapted from Java Paimon's `UpdateTableTestBase.scala`.

mod common;

use paimon_datafusion::PaimonSqlHandler;

use common::{create_handler, create_test_env, dml_count, exec, query_int_str_int};

// ======================= Helpers =======================

async fn setup() -> (tempfile::TempDir, PaimonSqlHandler) {
    let (tmp, catalog) = create_test_env();
    let handler = create_handler(catalog);
    handler.sql("CREATE SCHEMA paimon.test_db").await.unwrap();
    handler
        .sql("CREATE TABLE paimon.test_db.t (id INT, name VARCHAR, age INT)")
        .await
        .unwrap();
    exec(
        &handler,
        "INSERT INTO paimon.test_db.t VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (4, 'd', 40)",
    )
    .await;
    (tmp, handler)
}

async fn setup_partitioned() -> (tempfile::TempDir, PaimonSqlHandler) {
    let (tmp, catalog) = create_test_env();
    let handler = create_handler(catalog);
    handler.sql("CREATE SCHEMA paimon.test_db").await.unwrap();
    handler
        .sql("CREATE TABLE paimon.test_db.t (id INT, name VARCHAR, pt INT) PARTITIONED BY (pt)")
        .await
        .unwrap();
    exec(
        &handler,
        "INSERT INTO paimon.test_db.t VALUES (1, 'a', 1), (2, 'b', 1), (3, 'c', 2), (4, 'd', 2)",
    )
    .await;
    (tmp, handler)
}

async fn query(handler: &PaimonSqlHandler) -> Vec<(i32, String, i32)> {
    query_int_str_int(
        handler,
        "SELECT id, name, age FROM paimon.test_db.t ORDER BY id",
    )
    .await
}

async fn query_pt(handler: &PaimonSqlHandler) -> Vec<(i32, String, i32)> {
    query_int_str_int(
        handler,
        "SELECT id, name, pt FROM paimon.test_db.t ORDER BY id",
    )
    .await
}

// ======================= Basic UPDATE =======================

#[tokio::test]
async fn test_update_with_where() {
    let (_tmp, handler) = setup().await;

    exec(
        &handler,
        "UPDATE paimon.test_db.t SET name = 'a_new' WHERE id = 1",
    )
    .await;

    assert_eq!(
        query(&handler).await,
        vec![
            (1, "a_new".into(), 10),
            (2, "b".into(), 20),
            (3, "c".into(), 30),
            (4, "d".into(), 40),
        ]
    );
}

#[tokio::test]
async fn test_update_with_expression() {
    let (_tmp, handler) = setup().await;

    exec(
        &handler,
        "UPDATE paimon.test_db.t SET age = age + 1 WHERE id = 1",
    )
    .await;

    assert_eq!(
        query(&handler).await,
        vec![
            (1, "a".into(), 11),
            (2, "b".into(), 20),
            (3, "c".into(), 30),
            (4, "d".into(), 40),
        ]
    );
}

#[tokio::test]
async fn test_update_no_match_empty_commit() {
    let (_tmp, handler) = setup().await;

    let cnt = dml_count(
        &handler,
        "UPDATE paimon.test_db.t SET name = 'x' WHERE id = 99",
    )
    .await;
    assert_eq!(cnt, 0);

    assert_eq!(
        query(&handler).await,
        vec![
            (1, "a".into(), 10),
            (2, "b".into(), 20),
            (3, "c".into(), 30),
            (4, "d".into(), 40),
        ]
    );
}

#[tokio::test]
async fn test_update_without_where() {
    let (_tmp, handler) = setup().await;

    exec(&handler, "UPDATE paimon.test_db.t SET age = 0").await;

    assert_eq!(
        query(&handler).await,
        vec![
            (1, "a".into(), 0),
            (2, "b".into(), 0),
            (3, "c".into(), 0),
            (4, "d".into(), 0),
        ]
    );
}

// ======================= Partitioned table UPDATE =======================

#[tokio::test]
async fn test_update_partitioned_by_non_partition_column() {
    let (_tmp, handler) = setup_partitioned().await;

    exec(
        &handler,
        "UPDATE paimon.test_db.t SET name = 'x' WHERE id = 1",
    )
    .await;

    assert_eq!(
        query_pt(&handler).await,
        vec![
            (1, "x".into(), 1),
            (2, "b".into(), 1),
            (3, "c".into(), 2),
            (4, "d".into(), 2),
        ]
    );
}

#[tokio::test]
async fn test_update_partitioned_by_partition_column() {
    let (_tmp, handler) = setup_partitioned().await;

    exec(
        &handler,
        "UPDATE paimon.test_db.t SET name = 'x' WHERE pt = 1",
    )
    .await;

    assert_eq!(
        query_pt(&handler).await,
        vec![
            (1, "x".into(), 1),
            (2, "x".into(), 1),
            (3, "c".into(), 2),
            (4, "d".into(), 2),
        ]
    );
}

#[tokio::test]
async fn test_update_partitioned_by_both_columns() {
    let (_tmp, handler) = setup_partitioned().await;

    exec(
        &handler,
        "UPDATE paimon.test_db.t SET name = 'x' WHERE pt = 2 AND id = 3",
    )
    .await;

    assert_eq!(
        query_pt(&handler).await,
        vec![
            (1, "a".into(), 1),
            (2, "b".into(), 1),
            (3, "x".into(), 2),
            (4, "d".into(), 2),
        ]
    );
}

// ======================= IN / NOT IN =======================

#[tokio::test]
async fn test_update_in_condition() {
    let (_tmp, handler) = setup().await;

    exec(
        &handler,
        "UPDATE paimon.test_db.t SET name = 'x' WHERE id IN (1, 3)",
    )
    .await;

    assert_eq!(
        query(&handler).await,
        vec![
            (1, "x".into(), 10),
            (2, "b".into(), 20),
            (3, "x".into(), 30),
            (4, "d".into(), 40),
        ]
    );
}

#[tokio::test]
async fn test_update_not_in_condition() {
    let (_tmp, handler) = setup().await;

    exec(
        &handler,
        "UPDATE paimon.test_db.t SET name = 'x' WHERE id NOT IN (1, 3)",
    )
    .await;

    assert_eq!(
        query(&handler).await,
        vec![
            (1, "a".into(), 10),
            (2, "x".into(), 20),
            (3, "c".into(), 30),
            (4, "x".into(), 40),
        ]
    );
}

// ======================= Cross-column references =======================

#[tokio::test]
async fn test_update_cross_column_reference() {
    let (_tmp, handler) = setup().await;

    exec(
        &handler,
        "UPDATE paimon.test_db.t SET age = id * 100 WHERE id <= 2",
    )
    .await;

    assert_eq!(
        query(&handler).await,
        vec![
            (1, "a".into(), 100),
            (2, "b".into(), 200),
            (3, "c".into(), 30),
            (4, "d".into(), 40),
        ]
    );
}

#[tokio::test]
async fn test_update_multiple_columns_with_expressions() {
    let (_tmp, handler) = setup().await;

    exec(
        &handler,
        "UPDATE paimon.test_db.t SET name = 'updated', age = age + id WHERE id >= 3",
    )
    .await;

    assert_eq!(
        query(&handler).await,
        vec![
            (1, "a".into(), 10),
            (2, "b".into(), 20),
            (3, "updated".into(), 33),
            (4, "updated".into(), 44),
        ]
    );
}

// ======================= Successive updates =======================

#[tokio::test]
async fn test_successive_updates() {
    let (_tmp, handler) = setup().await;

    exec(&handler, "UPDATE paimon.test_db.t SET age = 0 WHERE id = 1").await;
    exec(
        &handler,
        "UPDATE paimon.test_db.t SET age = 99 WHERE id = 1",
    )
    .await;

    assert_eq!(
        query(&handler).await,
        vec![
            (1, "a".into(), 99),
            (2, "b".into(), 20),
            (3, "c".into(), 30),
            (4, "d".into(), 40),
        ]
    );
}

// ======================= OR condition =======================

#[tokio::test]
async fn test_update_or_condition() {
    let (_tmp, handler) = setup().await;

    exec(
        &handler,
        "UPDATE paimon.test_db.t SET name = 'x' WHERE id = 1 OR id = 4",
    )
    .await;

    assert_eq!(
        query(&handler).await,
        vec![
            (1, "x".into(), 10),
            (2, "b".into(), 20),
            (3, "c".into(), 30),
            (4, "x".into(), 40),
        ]
    );
}

// ======================= Range condition =======================

#[tokio::test]
async fn test_update_range_condition() {
    let (_tmp, handler) = setup().await;

    exec(
        &handler,
        "UPDATE paimon.test_db.t SET age = 0 WHERE id >= 2 AND id <= 3",
    )
    .await;

    assert_eq!(
        query(&handler).await,
        vec![
            (1, "a".into(), 10),
            (2, "b".into(), 0),
            (3, "c".into(), 0),
            (4, "d".into(), 40),
        ]
    );
}

// ======================= Subquery condition =======================

#[tokio::test]
async fn test_update_in_subquery() {
    let (_tmp, handler) = setup().await;

    handler
        .ctx()
        .sql("CREATE TABLE src (id INT) AS VALUES (1), (3)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    exec(
        &handler,
        "UPDATE paimon.test_db.t SET name = 'sub' WHERE id IN (SELECT id FROM src)",
    )
    .await;

    assert_eq!(
        query(&handler).await,
        vec![
            (1, "sub".into(), 10),
            (2, "b".into(), 20),
            (3, "sub".into(), 30),
            (4, "d".into(), 40),
        ]
    );
}

// ======================= NULL handling =======================

#[tokio::test]
async fn test_update_with_null_values() {
    let (tmp, catalog) = create_test_env();
    let handler = create_handler(catalog);
    handler.sql("CREATE SCHEMA paimon.test_db").await.unwrap();
    handler
        .sql("CREATE TABLE paimon.test_db.t (id INT, name VARCHAR, age INT)")
        .await
        .unwrap();
    handler
        .sql("INSERT INTO paimon.test_db.t VALUES (1, 'a', 10), (2, NULL, 20), (3, 'c', NULL)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    exec(
        &handler,
        "UPDATE paimon.test_db.t SET age = 0 WHERE name = 'a'",
    )
    .await;

    let rows = query(&handler).await;
    assert_eq!(rows[0], (1, "a".into(), 0));
    assert_eq!(rows[1].0, 2);
    assert_eq!(rows[1].2, 20);
    assert_eq!(rows[2].0, 3);
    drop(tmp);
}

// ======================= Empty table =======================

#[tokio::test]
async fn test_update_empty_table() {
    let (tmp, catalog) = create_test_env();
    let handler = create_handler(catalog);
    handler.sql("CREATE SCHEMA paimon.test_db").await.unwrap();
    handler
        .sql("CREATE TABLE paimon.test_db.t (id INT, name VARCHAR, age INT)")
        .await
        .unwrap();

    let cnt = dml_count(&handler, "UPDATE paimon.test_db.t SET name = 'x'").await;
    assert_eq!(cnt, 0);
    drop(tmp);
}
