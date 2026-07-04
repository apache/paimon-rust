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

use paimon_datafusion::SQLContext;

use common::{
    assert_sql_error, create_sql_context, create_test_env, dml_count, exec, query_int_str_int,
};

// ======================= Helpers =======================

async fn setup() -> (tempfile::TempDir, SQLContext) {
    let (tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog).await;
    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .unwrap();
    sql_context
        .sql("CREATE TABLE paimon.test_db.t (id INT, name VARCHAR, age INT)")
        .await
        .unwrap();
    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.t VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (4, 'd', 40)",
    )
    .await;
    (tmp, sql_context)
}

async fn setup_partitioned() -> (tempfile::TempDir, SQLContext) {
    let (tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog).await;
    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .unwrap();
    sql_context
        .sql("CREATE TABLE paimon.test_db.t (id INT, name VARCHAR, pt INT) PARTITIONED BY (pt)")
        .await
        .unwrap();
    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.t VALUES (1, 'a', 1), (2, 'b', 1), (3, 'c', 2), (4, 'd', 2)",
    )
    .await;
    (tmp, sql_context)
}

async fn query(sql_context: &SQLContext) -> Vec<(i32, String, i32)> {
    query_int_str_int(
        sql_context,
        "SELECT id, name, age FROM paimon.test_db.t ORDER BY id",
    )
    .await
}

async fn query_pt(sql_context: &SQLContext) -> Vec<(i32, String, i32)> {
    query_int_str_int(
        sql_context,
        "SELECT id, name, pt FROM paimon.test_db.t ORDER BY id",
    )
    .await
}

// ======================= Basic DELETE =======================

#[tokio::test]
async fn test_delete_by_non_partition_column() {
    let (_tmp, sql_context) = setup().await;

    exec(
        &sql_context,
        "DELETE FROM paimon.test_db.t WHERE name = 'b'",
    )
    .await;

    assert_eq!(
        query(&sql_context).await,
        vec![
            (1, "a".into(), 10),
            (3, "c".into(), 30),
            (4, "d".into(), 40),
        ]
    );
}

#[tokio::test]
async fn test_delete_no_match() {
    let (_tmp, sql_context) = setup().await;

    let cnt = dml_count(&sql_context, "DELETE FROM paimon.test_db.t WHERE id = 99").await;
    assert_eq!(cnt, 0);

    assert_eq!(
        query(&sql_context).await,
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
    let (_tmp, sql_context) = setup().await;

    exec(&sql_context, "DELETE FROM paimon.test_db.t").await;

    assert_eq!(query(&sql_context).await, vec![]);
}

// ======================= Partitioned table DELETE =======================

#[tokio::test]
async fn test_delete_partitioned_by_name() {
    let (_tmp, sql_context) = setup_partitioned().await;

    exec(
        &sql_context,
        "DELETE FROM paimon.test_db.t WHERE name = 'a'",
    )
    .await;

    assert_eq!(
        query_pt(&sql_context).await,
        vec![(2, "b".into(), 1), (3, "c".into(), 2), (4, "d".into(), 2),]
    );
}

#[tokio::test]
async fn test_delete_partitioned_by_partition_column() {
    let (_tmp, sql_context) = setup_partitioned().await;

    exec(&sql_context, "DELETE FROM paimon.test_db.t WHERE pt = 1").await;

    assert_eq!(
        query_pt(&sql_context).await,
        vec![(3, "c".into(), 2), (4, "d".into(), 2),]
    );
}

#[tokio::test]
async fn test_delete_partitioned_in_list() {
    let (_tmp, sql_context) = setup_partitioned().await;

    exec(
        &sql_context,
        "DELETE FROM paimon.test_db.t WHERE pt IN (1, 2) AND id = 1",
    )
    .await;

    assert_eq!(
        query_pt(&sql_context).await,
        vec![(2, "b".into(), 1), (3, "c".into(), 2), (4, "d".into(), 2),]
    );
}

#[tokio::test]
async fn test_delete_partitioned_no_match() {
    let (_tmp, sql_context) = setup_partitioned().await;

    let cnt = dml_count(&sql_context, "DELETE FROM paimon.test_db.t WHERE pt = 99").await;
    assert_eq!(cnt, 0);

    assert_eq!(
        query_pt(&sql_context).await,
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
    let (_tmp, sql_context) = setup_partitioned().await;

    exec(&sql_context, "DELETE FROM paimon.test_db.t").await;

    assert_eq!(query_pt(&sql_context).await, vec![]);
}

#[tokio::test]
async fn test_delete_partitioned_or_condition() {
    let (_tmp, sql_context) = setup_partitioned().await;

    exec(
        &sql_context,
        "DELETE FROM paimon.test_db.t WHERE pt = 1 OR id = 3",
    )
    .await;

    assert_eq!(query_pt(&sql_context).await, vec![(4, "d".into(), 2)]);
}

// ======================= IN / NOT IN =======================

#[tokio::test]
async fn test_delete_in_condition() {
    let (_tmp, sql_context) = setup().await;

    exec(
        &sql_context,
        "DELETE FROM paimon.test_db.t WHERE id IN (1, 3)",
    )
    .await;

    assert_eq!(
        query(&sql_context).await,
        vec![(2, "b".into(), 20), (4, "d".into(), 40),]
    );
}

#[tokio::test]
async fn test_delete_not_in_condition() {
    let (_tmp, sql_context) = setup().await;

    exec(
        &sql_context,
        "DELETE FROM paimon.test_db.t WHERE id NOT IN (1, 3)",
    )
    .await;

    assert_eq!(
        query(&sql_context).await,
        vec![(1, "a".into(), 10), (3, "c".into(), 30),]
    );
}

// ======================= Subquery conditions =======================

#[tokio::test]
async fn test_delete_in_subquery() {
    let (_tmp, sql_context) = setup().await;

    exec(
        &sql_context,
        "CREATE TEMPORARY TABLE paimon.test_db.src AS SELECT * FROM (VALUES (1), (3)) AS t(id)",
    )
    .await;

    exec(
        &sql_context,
        "DELETE FROM paimon.test_db.t WHERE id IN (SELECT id FROM paimon.test_db.src)",
    )
    .await;

    assert_eq!(
        query(&sql_context).await,
        vec![(2, "b".into(), 20), (4, "d".into(), 40),]
    );
}

#[tokio::test]
async fn test_delete_scalar_subquery() {
    let (_tmp, sql_context) = setup().await;

    exec(
        &sql_context,
        "CREATE TEMPORARY TABLE paimon.test_db.src AS SELECT * FROM (VALUES (2)) AS t(id)",
    )
    .await;

    exec(
        &sql_context,
        "DELETE FROM paimon.test_db.t WHERE id >= (SELECT MAX(id) FROM paimon.test_db.src)",
    )
    .await;

    assert_eq!(query(&sql_context).await, vec![(1, "a".into(), 10)]);
}

// ======================= Range condition =======================

#[tokio::test]
async fn test_delete_range_condition() {
    let (_tmp, sql_context) = setup().await;

    exec(
        &sql_context,
        "DELETE FROM paimon.test_db.t WHERE id >= 2 AND id <= 3",
    )
    .await;

    assert_eq!(
        query(&sql_context).await,
        vec![(1, "a".into(), 10), (4, "d".into(), 40),]
    );
}

// ======================= NULL handling =======================

#[tokio::test]
async fn test_delete_should_not_remove_null_rows() {
    let (tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog).await;
    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .unwrap();
    sql_context
        .sql("CREATE TABLE paimon.test_db.t (id INT, name VARCHAR, age INT)")
        .await
        .unwrap();
    sql_context
        .sql("INSERT INTO paimon.test_db.t VALUES (1, 'a', 10), (2, NULL, 20), (3, 'c', 30)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    exec(
        &sql_context,
        "DELETE FROM paimon.test_db.t WHERE name = 'a'",
    )
    .await;

    let rows = query(&sql_context).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, 2);
    assert_eq!(rows[0].2, 20);
    assert_eq!(rows[1], (3, "c".into(), 30));
    drop(tmp);
}

// ======================= Successive deletes =======================

#[tokio::test]
async fn test_successive_deletes() {
    let (_tmp, sql_context) = setup().await;

    exec(&sql_context, "DELETE FROM paimon.test_db.t WHERE id = 1").await;
    exec(&sql_context, "DELETE FROM paimon.test_db.t WHERE id = 3").await;

    assert_eq!(
        query(&sql_context).await,
        vec![(2, "b".into(), 20), (4, "d".into(), 40),]
    );
}

// ======================= Delete then insert =======================

#[tokio::test]
async fn test_delete_then_insert() {
    let (_tmp, sql_context) = setup().await;

    exec(&sql_context, "DELETE FROM paimon.test_db.t WHERE id = 1").await;
    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.t VALUES (5, 'e', 50)",
    )
    .await;

    assert_eq!(
        query(&sql_context).await,
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
    let (_tmp, sql_context) = setup().await;

    exec(&sql_context, "DELETE FROM paimon.test_db.t").await;
    assert_eq!(query(&sql_context).await, vec![]);

    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.t VALUES (10, 'new', 100)",
    )
    .await;
    assert_eq!(query(&sql_context).await, vec![(10, "new".into(), 100)]);
}

// ======================= Empty table =======================

#[tokio::test]
async fn test_delete_empty_table() {
    let (tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog).await;
    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .unwrap();
    sql_context
        .sql("CREATE TABLE paimon.test_db.t (id INT, name VARCHAR, age INT)")
        .await
        .unwrap();

    let cnt = dml_count(&sql_context, "DELETE FROM paimon.test_db.t WHERE id = 1").await;
    assert_eq!(cnt, 0);
    drop(tmp);
}

// ======================= OR condition =======================

#[tokio::test]
async fn test_delete_or_condition() {
    let (_tmp, sql_context) = setup().await;

    exec(
        &sql_context,
        "DELETE FROM paimon.test_db.t WHERE id = 1 OR id = 4",
    )
    .await;

    assert_eq!(
        query(&sql_context).await,
        vec![(2, "b".into(), 20), (3, "c".into(), 30),]
    );
}

// ======================= Multiple rows from single file =======================

#[tokio::test]
async fn test_delete_multiple_rows_from_single_commit() {
    let (_tmp, sql_context) = setup().await;

    exec(&sql_context, "DELETE FROM paimon.test_db.t WHERE id >= 2").await;

    assert_eq!(query(&sql_context).await, vec![(1, "a".into(), 10)]);
}

// ======================= Unsupported cases =======================

#[tokio::test]
async fn test_delete_rejects_primary_key_table() {
    let (tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog).await;
    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .unwrap();
    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.pk_t (\
                id INT NOT NULL, name VARCHAR, PRIMARY KEY (id)\
            ) WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    assert_sql_error(
        &sql_context,
        "DELETE FROM paimon.test_db.pk_t WHERE id = 1",
        "DELETE on primary-key tables is not yet supported",
    )
    .await;
    drop(tmp);
}

#[tokio::test]
async fn test_delete_rejects_data_evolution_table_without_deletion_vectors() {
    let (tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog).await;
    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .unwrap();
    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.de_t (\
                id INT NOT NULL, name VARCHAR\
            ) WITH (\
                'row-tracking.enabled' = 'true',\
                'data-evolution.enabled' = 'true'\
            )",
        )
        .await
        .unwrap();

    assert_sql_error(
        &sql_context,
        "DELETE FROM paimon.test_db.de_t WHERE id = 1",
        "deletion-vectors.enabled",
    )
    .await;
    drop(tmp);
}

#[tokio::test]
async fn test_delete_data_evolution_table_with_deletion_vectors() {
    let (tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog).await;
    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .unwrap();
    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.de_t (\
                id INT NOT NULL, name VARCHAR, age INT\
            ) WITH (\
                'row-tracking.enabled' = 'true',\
                'data-evolution.enabled' = 'true',\
                'deletion-vectors.enabled' = 'true'\
            )",
        )
        .await
        .unwrap();
    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.de_t (id, name, age) VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)",
    )
    .await;

    let cnt = dml_count(&sql_context, "DELETE FROM paimon.test_db.de_t WHERE id = 2").await;
    assert_eq!(cnt, 1);

    assert_eq!(
        query_int_str_int(
            &sql_context,
            "SELECT id, name, age FROM paimon.test_db.de_t ORDER BY id",
        )
        .await,
        vec![(1, "a".into(), 10), (3, "c".into(), 30)]
    );

    let second_cnt = dml_count(&sql_context, "DELETE FROM paimon.test_db.de_t WHERE id = 2").await;
    assert_eq!(second_cnt, 0);
    drop(tmp);
}

#[tokio::test]
async fn test_delete_rejects_table_alias() {
    let (_tmp, sql_context) = setup().await;

    assert_sql_error(
        &sql_context,
        "DELETE FROM paimon.test_db.t AS target WHERE id = 1",
        "Table alias 'target' in DELETE is not yet supported",
    )
    .await;
}
