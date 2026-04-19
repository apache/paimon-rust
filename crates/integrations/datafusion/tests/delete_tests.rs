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

//! DELETE integration tests for append-only tables (CoW path).
//!
//! Test cases adapted from Java Paimon's `DeleteFromTableTestBase.scala`.

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
        .sql("CREATE TABLE paimon.test_db.t (id INT, name VARCHAR, pt INT) PARTITIONED BY (pt INT)")
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

// ======================= Basic DELETE =======================

#[tokio::test]
async fn test_delete_by_non_partition_column() {
    let (_tmp, handler) = setup().await;

    exec(&handler, "DELETE FROM paimon.test_db.t WHERE name = 'b'").await;

    assert_eq!(
        query(&handler).await,
        vec![
            (1, "a".into(), 10),
            (3, "c".into(), 30),
            (4, "d".into(), 40),
        ]
    );
}

#[tokio::test]
async fn test_delete_no_match() {
    let (_tmp, handler) = setup().await;

    let cnt = dml_count(&handler, "DELETE FROM paimon.test_db.t WHERE id = 99").await;
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
async fn test_delete_full_table() {
    let (_tmp, handler) = setup().await;

    exec(&handler, "DELETE FROM paimon.test_db.t").await;

    assert_eq!(query(&handler).await, vec![]);
}

// ======================= Partitioned table DELETE =======================

#[tokio::test]
async fn test_delete_partitioned_by_name() {
    let (_tmp, handler) = setup_partitioned().await;

    exec(&handler, "DELETE FROM paimon.test_db.t WHERE name = 'a'").await;

    assert_eq!(
        query_pt(&handler).await,
        vec![(2, "b".into(), 1), (3, "c".into(), 2), (4, "d".into(), 2),]
    );
}

#[tokio::test]
async fn test_delete_partitioned_by_partition_column() {
    let (_tmp, handler) = setup_partitioned().await;

    exec(&handler, "DELETE FROM paimon.test_db.t WHERE pt = 1").await;

    assert_eq!(
        query_pt(&handler).await,
        vec![(3, "c".into(), 2), (4, "d".into(), 2),]
    );
}

#[tokio::test]
async fn test_delete_partitioned_in_list() {
    let (_tmp, handler) = setup_partitioned().await;

    exec(
        &handler,
        "DELETE FROM paimon.test_db.t WHERE pt IN (1, 2) AND id = 1",
    )
    .await;

    assert_eq!(
        query_pt(&handler).await,
        vec![(2, "b".into(), 1), (3, "c".into(), 2), (4, "d".into(), 2),]
    );
}

#[tokio::test]
async fn test_delete_partitioned_no_match() {
    let (_tmp, handler) = setup_partitioned().await;

    let cnt = dml_count(&handler, "DELETE FROM paimon.test_db.t WHERE pt = 99").await;
    assert_eq!(cnt, 0);

    assert_eq!(
        query_pt(&handler).await,
        vec![
            (1, "a".into(), 1),
            (2, "b".into(), 1),
            (3, "c".into(), 2),
            (4, "d".into(), 2),
        ]
    );
}

#[tokio::test]
async fn test_delete_partitioned_full_table() {
    let (_tmp, handler) = setup_partitioned().await;

    exec(&handler, "DELETE FROM paimon.test_db.t").await;

    assert_eq!(query_pt(&handler).await, vec![]);
}

#[tokio::test]
async fn test_delete_partitioned_or_condition() {
    let (_tmp, handler) = setup_partitioned().await;

    exec(
        &handler,
        "DELETE FROM paimon.test_db.t WHERE pt = 1 OR id = 3",
    )
    .await;

    assert_eq!(query_pt(&handler).await, vec![(4, "d".into(), 2)]);
}

// ======================= IN / NOT IN =======================

#[tokio::test]
async fn test_delete_in_condition() {
    let (_tmp, handler) = setup().await;

    exec(&handler, "DELETE FROM paimon.test_db.t WHERE id IN (1, 3)").await;

    assert_eq!(
        query(&handler).await,
        vec![(2, "b".into(), 20), (4, "d".into(), 40),]
    );
}

#[tokio::test]
async fn test_delete_not_in_condition() {
    let (_tmp, handler) = setup().await;

    exec(
        &handler,
        "DELETE FROM paimon.test_db.t WHERE id NOT IN (1, 3)",
    )
    .await;

    assert_eq!(
        query(&handler).await,
        vec![(1, "a".into(), 10), (3, "c".into(), 30),]
    );
}

// ======================= Subquery conditions =======================

#[tokio::test]
async fn test_delete_in_subquery() {
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
        "DELETE FROM paimon.test_db.t WHERE id IN (SELECT id FROM src)",
    )
    .await;

    assert_eq!(
        query(&handler).await,
        vec![(2, "b".into(), 20), (4, "d".into(), 40),]
    );
}

#[tokio::test]
async fn test_delete_scalar_subquery() {
    let (_tmp, handler) = setup().await;

    handler
        .ctx()
        .sql("CREATE TABLE src (id INT) AS VALUES (2)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    exec(
        &handler,
        "DELETE FROM paimon.test_db.t WHERE id >= (SELECT MAX(id) FROM src)",
    )
    .await;

    assert_eq!(query(&handler).await, vec![(1, "a".into(), 10)]);
}

// ======================= Range condition =======================

#[tokio::test]
async fn test_delete_range_condition() {
    let (_tmp, handler) = setup().await;

    exec(
        &handler,
        "DELETE FROM paimon.test_db.t WHERE id >= 2 AND id <= 3",
    )
    .await;

    assert_eq!(
        query(&handler).await,
        vec![(1, "a".into(), 10), (4, "d".into(), 40),]
    );
}

// ======================= NULL handling =======================

#[tokio::test]
async fn test_delete_should_not_remove_null_rows() {
    let (tmp, catalog) = create_test_env();
    let handler = create_handler(catalog);
    handler.sql("CREATE SCHEMA paimon.test_db").await.unwrap();
    handler
        .sql("CREATE TABLE paimon.test_db.t (id INT, name VARCHAR, age INT)")
        .await
        .unwrap();
    handler
        .sql("INSERT INTO paimon.test_db.t VALUES (1, 'a', 10), (2, NULL, 20), (3, 'c', 30)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    exec(&handler, "DELETE FROM paimon.test_db.t WHERE name = 'a'").await;

    let rows = query(&handler).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, 2);
    assert_eq!(rows[0].2, 20);
    assert_eq!(rows[1], (3, "c".into(), 30));
    drop(tmp);
}

// ======================= Successive deletes =======================

#[tokio::test]
async fn test_successive_deletes() {
    let (_tmp, handler) = setup().await;

    exec(&handler, "DELETE FROM paimon.test_db.t WHERE id = 1").await;
    exec(&handler, "DELETE FROM paimon.test_db.t WHERE id = 3").await;

    assert_eq!(
        query(&handler).await,
        vec![(2, "b".into(), 20), (4, "d".into(), 40),]
    );
}

// ======================= Delete then insert =======================

#[tokio::test]
async fn test_delete_then_insert() {
    let (_tmp, handler) = setup().await;

    exec(&handler, "DELETE FROM paimon.test_db.t WHERE id = 1").await;
    exec(&handler, "INSERT INTO paimon.test_db.t VALUES (5, 'e', 50)").await;

    assert_eq!(
        query(&handler).await,
        vec![
            (2, "b".into(), 20),
            (3, "c".into(), 30),
            (4, "d".into(), 40),
            (5, "e".into(), 50),
        ]
    );
}

#[tokio::test]
async fn test_delete_all_then_insert() {
    let (_tmp, handler) = setup().await;

    exec(&handler, "DELETE FROM paimon.test_db.t").await;
    assert_eq!(query(&handler).await, vec![]);

    exec(
        &handler,
        "INSERT INTO paimon.test_db.t VALUES (10, 'new', 100)",
    )
    .await;
    assert_eq!(query(&handler).await, vec![(10, "new".into(), 100)]);
}

// ======================= Empty table =======================

#[tokio::test]
async fn test_delete_empty_table() {
    let (tmp, catalog) = create_test_env();
    let handler = create_handler(catalog);
    handler.sql("CREATE SCHEMA paimon.test_db").await.unwrap();
    handler
        .sql("CREATE TABLE paimon.test_db.t (id INT, name VARCHAR, age INT)")
        .await
        .unwrap();

    let cnt = dml_count(&handler, "DELETE FROM paimon.test_db.t WHERE id = 1").await;
    assert_eq!(cnt, 0);
    drop(tmp);
}

// ======================= OR condition =======================

#[tokio::test]
async fn test_delete_or_condition() {
    let (_tmp, handler) = setup().await;

    exec(
        &handler,
        "DELETE FROM paimon.test_db.t WHERE id = 1 OR id = 4",
    )
    .await;

    assert_eq!(
        query(&handler).await,
        vec![(2, "b".into(), 20), (3, "c".into(), 30),]
    );
}

// ======================= Multiple rows from single file =======================

#[tokio::test]
async fn test_delete_multiple_rows_from_single_commit() {
    let (_tmp, handler) = setup().await;

    exec(&handler, "DELETE FROM paimon.test_db.t WHERE id >= 2").await;

    assert_eq!(query(&handler).await, vec![(1, "a".into(), 10)]);
}
