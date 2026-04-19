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

//! Copy-on-Write MERGE INTO integration tests for append-only tables.
//!
//! Covers the test cases from Java Paimon's `MergeIntoTableTestBase` adapted
//! for append-only (no primary key) tables using the CoW rewrite path.

mod common;

use arrow_array::{Array, Int32Array, StringArray};
use paimon_datafusion::PaimonSqlHandler;

use common::{
    collect_int_int_str, collect_int_str, collect_three_ints, create_handler, create_test_env,
    ctx_exec, exec,
};

// ======================= Helpers =======================

async fn setup(table_ddl: &str) -> (tempfile::TempDir, PaimonSqlHandler) {
    let (tmp, catalog) = create_test_env();
    let handler = create_handler(catalog);
    handler.sql("CREATE SCHEMA paimon.test_db").await.unwrap();
    handler.sql(table_ddl).await.unwrap();
    (tmp, handler)
}

async fn query_abc(handler: &PaimonSqlHandler) -> Vec<(i32, i32, String)> {
    let batches = handler
        .sql("SELECT a, b, c FROM paimon.test_db.target ORDER BY a, b")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    collect_int_int_str(&batches)
}

async fn setup_abc() -> (tempfile::TempDir, PaimonSqlHandler) {
    setup("CREATE TABLE paimon.test_db.target (a INT, b INT, c VARCHAR)").await
}

async fn setup_partitioned() -> (tempfile::TempDir, PaimonSqlHandler) {
    let (tmp, handler) =
        setup("CREATE TABLE paimon.test_db.target (a INT, b INT, pt INT) PARTITIONED BY (pt INT)")
            .await;
    exec(
        &handler,
        "INSERT INTO paimon.test_db.target VALUES (1, 10, 1), (2, 20, 1), (3, 30, 2), (4, 40, 2)",
    )
    .await;
    (tmp, handler)
}

async fn query_a_b_pt(handler: &PaimonSqlHandler) -> Vec<(i32, i32, i32)> {
    let batches = handler
        .sql("SELECT a, b, pt FROM paimon.test_db.target ORDER BY pt, a")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    collect_three_ints(&batches)
}

// ======================= Tests =======================

/// Paimon MergeInto: only update
#[tokio::test]
async fn test_only_update() {
    let (_tmp, handler) = setup_abc().await;
    exec(
        &handler,
        "INSERT INTO paimon.test_db.target VALUES (1, 10, 'c1'), (2, 20, 'c2')",
    )
    .await;
    ctx_exec(
        &handler,
        "CREATE TABLE source (a INT, b INT, c VARCHAR) AS VALUES (1, 100, 'c11'), (3, 300, 'c33')",
    )
    .await;

    exec(
        &handler,
        "MERGE INTO paimon.test_db.target \
         USING source ON paimon.test_db.target.a = source.a \
         WHEN MATCHED THEN UPDATE SET a = source.a, b = source.b, c = source.c",
    )
    .await;

    assert_eq!(
        query_abc(&handler).await,
        vec![(1, 100, "c11".into()), (2, 20, "c2".into()),]
    );
}

/// Paimon MergeInto: only delete
#[tokio::test]
async fn test_only_delete() {
    let (_tmp, handler) = setup_abc().await;
    exec(
        &handler,
        "INSERT INTO paimon.test_db.target VALUES (1, 10, 'c1'), (2, 20, 'c2')",
    )
    .await;
    ctx_exec(
        &handler,
        "CREATE TABLE source (a INT, b INT, c VARCHAR) AS VALUES (1, 100, 'c11'), (3, 300, 'c33')",
    )
    .await;

    exec(
        &handler,
        "MERGE INTO paimon.test_db.target \
         USING source ON paimon.test_db.target.a = source.a \
         WHEN MATCHED THEN DELETE",
    )
    .await;

    assert_eq!(query_abc(&handler).await, vec![(2, 20, "c2".into()),]);
}

/// Paimon MergeInto: only insert
#[tokio::test]
async fn test_only_insert() {
    let (_tmp, handler) = setup_abc().await;
    exec(
        &handler,
        "INSERT INTO paimon.test_db.target VALUES (1, 10, 'c1'), (2, 20, 'c2')",
    )
    .await;
    ctx_exec(
        &handler,
        "CREATE TABLE source (a INT, b INT, c VARCHAR) AS VALUES (1, 100, 'c11'), (3, 300, 'c33')",
    )
    .await;

    exec(
        &handler,
        "MERGE INTO paimon.test_db.target \
         USING source ON paimon.test_db.target.a = source.a \
         WHEN NOT MATCHED THEN INSERT (a, b, c) VALUES (a, b, c)",
    )
    .await;

    assert_eq!(
        query_abc(&handler).await,
        vec![
            (1, 10, "c1".into()),
            (2, 20, "c2".into()),
            (3, 300, "c33".into()),
        ]
    );
}

/// Paimon MergeInto: update + insert
#[tokio::test]
async fn test_update_and_insert() {
    let (_tmp, handler) = setup_abc().await;
    exec(
        &handler,
        "INSERT INTO paimon.test_db.target VALUES (1, 10, 'c1'), (2, 20, 'c2')",
    )
    .await;
    ctx_exec(
        &handler,
        "CREATE TABLE source (a INT, b INT, c VARCHAR) AS VALUES (1, 100, 'c11'), (3, 300, 'c33')",
    )
    .await;

    exec(
        &handler,
        "MERGE INTO paimon.test_db.target \
         USING source ON paimon.test_db.target.a = source.a \
         WHEN MATCHED THEN UPDATE SET a = source.a, b = source.b, c = source.c \
         WHEN NOT MATCHED THEN INSERT (a, b, c) VALUES (a, b, c)",
    )
    .await;

    assert_eq!(
        query_abc(&handler).await,
        vec![
            (1, 100, "c11".into()),
            (2, 20, "c2".into()),
            (3, 300, "c33".into()),
        ]
    );
}

/// Paimon MergeInto: delete + insert
#[tokio::test]
async fn test_delete_and_insert() {
    let (_tmp, handler) = setup_abc().await;
    exec(
        &handler,
        "INSERT INTO paimon.test_db.target VALUES (1, 10, 'c1'), (2, 20, 'c2')",
    )
    .await;
    ctx_exec(
        &handler,
        "CREATE TABLE source (a INT, b INT, c VARCHAR) AS VALUES (1, 100, 'c11'), (3, 300, 'c33')",
    )
    .await;

    exec(
        &handler,
        "MERGE INTO paimon.test_db.target \
         USING source ON paimon.test_db.target.a = source.a \
         WHEN MATCHED THEN DELETE \
         WHEN NOT MATCHED THEN INSERT (a, b, c) VALUES (a, b, c)",
    )
    .await;

    assert_eq!(
        query_abc(&handler).await,
        vec![(2, 20, "c2".into()), (3, 300, "c33".into()),]
    );
}

/// Paimon MergeInto: partial insert with null
#[tokio::test]
async fn test_partial_insert_with_null() {
    let (_tmp, handler) = setup_abc().await;
    exec(
        &handler,
        "INSERT INTO paimon.test_db.target VALUES (1, 10, 'c1'), (2, 20, 'c2')",
    )
    .await;
    ctx_exec(
        &handler,
        "CREATE TABLE source (a INT, b INT, c VARCHAR) AS VALUES (1, 100, 'c11'), (3, 300, 'c33')",
    )
    .await;

    exec(
        &handler,
        "MERGE INTO paimon.test_db.target \
         USING source ON paimon.test_db.target.a = source.a \
         WHEN NOT MATCHED THEN INSERT (a) VALUES (a)",
    )
    .await;

    let batches = handler
        .sql("SELECT a, b, c FROM paimon.test_db.target ORDER BY a, b")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let mut rows: Vec<(i32, Option<i32>, Option<String>)> = Vec::new();
    for batch in &batches {
        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let b = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let c = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((
                a.value(i),
                if b.is_null(i) { None } else { Some(b.value(i)) },
                if c.is_null(i) {
                    None
                } else {
                    Some(c.value(i).to_string())
                },
            ));
        }
    }
    rows.sort_by_key(|r| r.0);

    assert_eq!(
        rows,
        vec![
            (1, Some(10), Some("c1".into())),
            (2, Some(20), Some("c2".into())),
            (3, None, None),
        ]
    );
}

/// Paimon MergeInto: update value from both source and target table
#[tokio::test]
async fn test_update_from_both_source_and_target() {
    let (_tmp, handler) = setup_abc().await;
    exec(
        &handler,
        "INSERT INTO paimon.test_db.target VALUES (1, 10, 'c1'), (2, 20, 'c2')",
    )
    .await;
    ctx_exec(
        &handler,
        "CREATE TABLE source (a INT, b INT, c VARCHAR) AS VALUES (1, 100, 'c11'), (3, 300, 'c33')",
    )
    .await;

    exec(
        &handler,
        "MERGE INTO paimon.test_db.target t \
         USING source s ON t.a = s.a \
         WHEN MATCHED THEN UPDATE SET b = t.b * 11, c = s.c \
         WHEN NOT MATCHED THEN INSERT (a, b, c) VALUES (s.a, s.b * 2, s.c)",
    )
    .await;

    assert_eq!(
        query_abc(&handler).await,
        vec![
            (1, 110, "c11".into()),
            (2, 20, "c2".into()),
            (3, 600, "c33".into()),
        ]
    );
}

/// Paimon MergeInto: insert/update columns in wrong order
#[tokio::test]
async fn test_columns_in_wrong_order() {
    let (_tmp, handler) = setup_abc().await;
    exec(
        &handler,
        "INSERT INTO paimon.test_db.target VALUES (1, 10, 'c1'), (2, 20, 'c2')",
    )
    .await;
    ctx_exec(
        &handler,
        "CREATE TABLE source (a INT, b INT, c VARCHAR) AS VALUES (1, 100, 'c11'), (3, 300, 'c33')",
    )
    .await;

    exec(
        &handler,
        "MERGE INTO paimon.test_db.target t \
         USING source s ON t.a = s.a \
         WHEN MATCHED THEN UPDATE SET c = s.c, b = s.b \
         WHEN NOT MATCHED THEN INSERT (b, c, a) VALUES (b, c, a)",
    )
    .await;

    assert_eq!(
        query_abc(&handler).await,
        vec![
            (1, 100, "c11".into()),
            (2, 20, "c2".into()),
            (3, 300, "c33".into()),
        ]
    );
}

/// Paimon MergeInto: miss some columns in update
#[tokio::test]
async fn test_partial_update() {
    let (_tmp, handler) = setup_abc().await;
    exec(
        &handler,
        "INSERT INTO paimon.test_db.target VALUES (1, 10, 'c1'), (2, 20, 'c2')",
    )
    .await;
    ctx_exec(
        &handler,
        "CREATE TABLE source (a INT, b INT, c VARCHAR) AS VALUES (1, 100, 'c11'), (3, 300, 'c33')",
    )
    .await;

    exec(
        &handler,
        "MERGE INTO paimon.test_db.target t \
         USING source s ON t.a = s.a \
         WHEN MATCHED THEN UPDATE SET c = s.c \
         WHEN NOT MATCHED THEN INSERT (a, b, c) VALUES (s.a, s.b, s.c)",
    )
    .await;

    assert_eq!(
        query_abc(&handler).await,
        vec![
            (1, 10, "c11".into()),
            (2, 20, "c2".into()),
            (3, 300, "c33".into()),
        ]
    );
}

/// Paimon MergeInto: source is a query (subquery)
#[tokio::test]
async fn test_source_is_subquery() {
    let (_tmp, handler) = setup_abc().await;
    exec(
        &handler,
        "INSERT INTO paimon.test_db.target VALUES (1, 10, 'c1'), (2, 20, 'c2')",
    )
    .await;
    ctx_exec(&handler, "CREATE TABLE source (a INT, b INT, c VARCHAR) AS VALUES (1, 100, 'c11'), (3, 300, 'c33'), (4, 400, 'c44')").await;

    exec(
        &handler,
        "MERGE INTO paimon.test_db.target t \
         USING (SELECT a, b, c FROM source WHERE a % 2 = 1) AS src \
         ON t.a = src.a \
         WHEN MATCHED THEN UPDATE SET b = src.b, c = src.c \
         WHEN NOT MATCHED THEN INSERT (a, b, c) VALUES (src.a, src.b, src.c)",
    )
    .await;

    assert_eq!(
        query_abc(&handler).await,
        vec![
            (1, 100, "c11".into()),
            (2, 20, "c2".into()),
            (3, 300, "c33".into()),
        ]
    );
}

/// Paimon MergeInto: source and target are empty
#[tokio::test]
async fn test_source_and_target_empty() {
    let (_tmp, handler) = setup_abc().await;
    // target is empty, source is empty
    ctx_exec(&handler, "CREATE TABLE source (a INT, b INT, c VARCHAR) AS SELECT * FROM (VALUES (1, 1, 'x')) AS t(a, b, c) WHERE 1=0").await;

    exec(
        &handler,
        "MERGE INTO paimon.test_db.target t \
         USING source s ON t.a = s.a \
         WHEN MATCHED THEN UPDATE SET a = s.a, b = s.b, c = s.c \
         WHEN NOT MATCHED THEN INSERT (a, b, c) VALUES (s.a, s.b, s.c)",
    )
    .await;

    assert_eq!(query_abc(&handler).await, Vec::<(i32, i32, String)>::new());
}

/// Paimon MergeInto: merge into with alias
#[tokio::test]
async fn test_with_alias() {
    let (_tmp, handler) = setup_abc().await;
    exec(
        &handler,
        "INSERT INTO paimon.test_db.target VALUES (1, 10, 'c1'), (2, 20, 'c2')",
    )
    .await;
    ctx_exec(
        &handler,
        "CREATE TABLE source (a INT, b INT, c VARCHAR) AS VALUES (1, 100, 'c11'), (3, 300, 'c33')",
    )
    .await;

    exec(
        &handler,
        "MERGE INTO paimon.test_db.target t \
         USING source s ON t.a = s.a \
         WHEN MATCHED THEN UPDATE SET b = s.b, c = s.c \
         WHEN NOT MATCHED THEN INSERT (a, b, c) VALUES (s.a, s.b, s.c)",
    )
    .await;

    assert_eq!(
        query_abc(&handler).await,
        vec![
            (1, 100, "c11".into()),
            (2, 20, "c2".into()),
            (3, 300, "c33".into()),
        ]
    );
}

/// Paimon MergeInto: update on source eq target condition (reversed ON clause)
#[tokio::test]
async fn test_reversed_on_condition() {
    let (_tmp, handler) = setup_abc().await;
    exec(
        &handler,
        "INSERT INTO paimon.test_db.target VALUES (1, 10, 'c1'), (2, 20, 'c2')",
    )
    .await;
    ctx_exec(
        &handler,
        "CREATE TABLE source (a INT, b INT, c VARCHAR) AS VALUES (1, 100, 'c11'), (3, 300, 'c33')",
    )
    .await;

    exec(
        &handler,
        "MERGE INTO paimon.test_db.target t \
         USING source s ON s.a = t.a \
         WHEN MATCHED THEN UPDATE SET a = s.a, b = s.b, c = s.c",
    )
    .await;

    assert_eq!(
        query_abc(&handler).await,
        vec![(1, 100, "c11".into()), (2, 20, "c2".into()),]
    );
}

/// Paimon MergeInto: two paimon tables
#[tokio::test]
async fn test_two_paimon_tables() {
    let (_tmp, handler) = setup_abc().await;
    handler
        .sql("CREATE TABLE paimon.test_db.source (a INT, b INT, c VARCHAR)")
        .await
        .unwrap();
    exec(
        &handler,
        "INSERT INTO paimon.test_db.source VALUES (1, 100, 'c11'), (3, 300, 'c33')",
    )
    .await;
    exec(
        &handler,
        "INSERT INTO paimon.test_db.target VALUES (1, 10, 'c1'), (2, 20, 'c2')",
    )
    .await;

    exec(
        &handler,
        "MERGE INTO paimon.test_db.target t \
         USING paimon.test_db.source s ON t.a = s.a \
         WHEN MATCHED THEN UPDATE SET a = s.a, b = s.b, c = s.c",
    )
    .await;

    assert_eq!(
        query_abc(&handler).await,
        vec![(1, 100, "c11".into()), (2, 20, "c2".into()),]
    );
}

/// Paimon MergeInto: on clause has filter expression
#[tokio::test]
async fn test_on_clause_with_filter() {
    let (_tmp, handler) = setup_abc().await;
    handler
        .sql("CREATE TABLE paimon.test_db.source (a INT, b INT, c VARCHAR)")
        .await
        .unwrap();
    exec(&handler, "INSERT INTO paimon.test_db.source VALUES (1, 100, 'c11'), (3, 300, 'c11'), (5, 500, 'c55')").await;
    exec(
        &handler,
        "INSERT INTO paimon.test_db.target VALUES (1, 100, 'cc'), (2, 20, 'cc')",
    )
    .await;

    exec(
        &handler,
        "MERGE INTO paimon.test_db.target tgt \
         USING (SELECT a, b FROM paimon.test_db.source WHERE c = 'c11') AS src \
         ON tgt.a = src.a AND tgt.b = src.b AND tgt.c = 'cc' \
         WHEN MATCHED THEN DELETE",
    )
    .await;

    assert_eq!(query_abc(&handler).await, vec![(2, 20, "cc".into()),]);
}

/// Paimon MergeInto: merge into with varchar type
#[tokio::test]
async fn test_with_varchar() {
    let (_tmp, handler) = setup("CREATE TABLE paimon.test_db.target (a INT, b VARCHAR)").await;
    handler
        .sql("CREATE TABLE paimon.test_db.source (a INT, b VARCHAR)")
        .await
        .unwrap();
    exec(
        &handler,
        "INSERT INTO paimon.test_db.target VALUES (1, 'Alice'), (2, 'Bob')",
    )
    .await;
    exec(
        &handler,
        "INSERT INTO paimon.test_db.source VALUES (1, 'Eve'), (3, 'Cat')",
    )
    .await;

    exec(
        &handler,
        "MERGE INTO paimon.test_db.target t \
         USING paimon.test_db.source s ON t.a = s.a \
         WHEN MATCHED THEN UPDATE SET a = s.a, b = s.b",
    )
    .await;

    let batches = handler
        .sql("SELECT a, b FROM paimon.test_db.target ORDER BY a")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        collect_int_str(&batches),
        vec![(1, "Eve".into()), (2, "Bob".into()),]
    );
}

/// Paimon MergeInto: update with coalesce referencing both source and target columns
#[tokio::test]
async fn test_coalesce_source_and_target() {
    let (_tmp, handler) = setup("CREATE TABLE paimon.test_db.target (a INT, b VARCHAR)").await;
    exec(
        &handler,
        "INSERT INTO paimon.test_db.target VALUES (1, 'guid_tgt_1'), (2, 'guid_tgt_2')",
    )
    .await;
    ctx_exec(
        &handler,
        "CREATE TABLE source (a INT, b VARCHAR) AS VALUES (1, 'guid_src_1'), (3, 'guid_src_3')",
    )
    .await;

    exec(
        &handler,
        "MERGE INTO paimon.test_db.target AS dest \
         USING source AS src ON dest.a = src.a \
         WHEN MATCHED AND (nullif(cast(src.b as STRING), '') IS NOT NULL) THEN \
         UPDATE SET b = COALESCE(nullif(cast(src.b as STRING), ''), dest.b) \
         WHEN NOT MATCHED THEN INSERT (a, b) VALUES (src.a, src.b)",
    )
    .await;

    let batches = handler
        .sql("SELECT a, b FROM paimon.test_db.target ORDER BY a")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        collect_int_str(&batches),
        vec![
            (1, "guid_src_1".into()),
            (2, "guid_tgt_2".into()),
            (3, "guid_src_3".into()),
        ]
    );
}

/// Paimon MergeInto: subquery source with coalesce referencing both source and target
#[tokio::test]
async fn test_subquery_source_with_coalesce() {
    let (_tmp, handler) = setup("CREATE TABLE paimon.test_db.target (a INT, b VARCHAR)").await;
    exec(
        &handler,
        "INSERT INTO paimon.test_db.target VALUES (1, 'guid_tgt_1'), (2, 'guid_tgt_2')",
    )
    .await;
    ctx_exec(
        &handler,
        "CREATE TABLE source (a INT, b VARCHAR) AS VALUES (1, 'guid_src_1'), (3, 'guid_src_3')",
    )
    .await;

    exec(
        &handler,
        "MERGE INTO paimon.test_db.target AS dest \
         USING (SELECT * FROM source) AS src ON dest.a = src.a \
         WHEN MATCHED AND (nullif(cast(src.b as STRING), '') IS NOT NULL) THEN \
         UPDATE SET b = COALESCE(nullif(cast(src.b as STRING), ''), dest.b) \
         WHEN NOT MATCHED THEN INSERT (a, b) VALUES (src.a, src.b)",
    )
    .await;

    let batches = handler
        .sql("SELECT a, b FROM paimon.test_db.target ORDER BY a")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        collect_int_str(&batches),
        vec![
            (1, "guid_src_1".into()),
            (2, "guid_tgt_2".into()),
            (3, "guid_src_3".into()),
        ]
    );
}

/// Paimon MergeInto: non pk table insert-only commit kind is APPEND
#[tokio::test]
async fn test_insert_only_is_append_commit() {
    let (_tmp, handler) = setup_abc().await;
    exec(
        &handler,
        "INSERT INTO paimon.test_db.target VALUES (2, 2, 'c2')",
    )
    .await;
    ctx_exec(
        &handler,
        "CREATE TABLE source (a INT, b INT, c VARCHAR) AS VALUES (1, 1, 'c1')",
    )
    .await;

    exec(
        &handler,
        "MERGE INTO paimon.test_db.target t \
         USING source s ON t.a = s.a \
         WHEN NOT MATCHED THEN INSERT (a, b, c) VALUES (s.a, s.b, s.c)",
    )
    .await;

    assert_eq!(
        query_abc(&handler).await,
        vec![(1, 1, "c1".into()), (2, 2, "c2".into()),]
    );
}

/// Paimon MergeInto: successive merges on append-only table
#[tokio::test]
async fn test_successive_merges() {
    let (_tmp, handler) = setup_abc().await;
    exec(
        &handler,
        "INSERT INTO paimon.test_db.target VALUES (1, 10, 'c1'), (2, 20, 'c2')",
    )
    .await;

    // First merge: update b for a=1
    ctx_exec(
        &handler,
        "CREATE TABLE src1 (a INT, b INT) AS VALUES (1, 100)",
    )
    .await;
    exec(
        &handler,
        "MERGE INTO paimon.test_db.target t \
         USING src1 s ON t.a = s.a \
         WHEN MATCHED THEN UPDATE SET b = s.b",
    )
    .await;

    assert_eq!(
        query_abc(&handler).await,
        vec![(1, 100, "c1".into()), (2, 20, "c2".into()),]
    );

    // Second merge: update c for a=2
    ctx_exec(
        &handler,
        "CREATE TABLE src2 (a INT, c VARCHAR) AS VALUES (2, 'C2_UPDATED')",
    )
    .await;
    exec(
        &handler,
        "MERGE INTO paimon.test_db.target t \
         USING src2 s ON t.a = s.a \
         WHEN MATCHED THEN UPDATE SET c = s.c",
    )
    .await;

    assert_eq!(
        query_abc(&handler).await,
        vec![(1, 100, "c1".into()), (2, 20, "C2_UPDATED".into()),]
    );
}

/// Paimon MergeInto: no match produces no changes
#[tokio::test]
async fn test_no_match_no_change() {
    let (_tmp, handler) = setup_abc().await;
    exec(
        &handler,
        "INSERT INTO paimon.test_db.target VALUES (1, 10, 'c1'), (2, 20, 'c2')",
    )
    .await;
    ctx_exec(
        &handler,
        "CREATE TABLE source (a INT, b INT, c VARCHAR) AS VALUES (99, 990, 'c99')",
    )
    .await;

    exec(
        &handler,
        "MERGE INTO paimon.test_db.target t \
         USING source s ON t.a = s.a \
         WHEN MATCHED THEN UPDATE SET b = s.b, c = s.c",
    )
    .await;

    assert_eq!(
        query_abc(&handler).await,
        vec![(1, 10, "c1".into()), (2, 20, "c2".into()),]
    );
}

/// Paimon MergeInto: delete all matched rows
#[tokio::test]
async fn test_delete_all_rows() {
    let (_tmp, handler) = setup_abc().await;
    exec(
        &handler,
        "INSERT INTO paimon.test_db.target VALUES (1, 10, 'c1'), (2, 20, 'c2')",
    )
    .await;
    ctx_exec(&handler, "CREATE TABLE source (a INT) AS VALUES (1), (2)").await;

    exec(
        &handler,
        "MERGE INTO paimon.test_db.target t \
         USING source s ON t.a = s.a \
         WHEN MATCHED THEN DELETE",
    )
    .await;

    assert_eq!(query_abc(&handler).await, Vec::<(i32, i32, String)>::new());
}

/// Paimon MergeInto: multiple inserts from different batches
#[tokio::test]
async fn test_insert_many_rows() {
    let (_tmp, handler) = setup_abc().await;
    exec(
        &handler,
        "INSERT INTO paimon.test_db.target VALUES (1, 10, 'c1')",
    )
    .await;
    ctx_exec(&handler, "CREATE TABLE source (a INT, b INT, c VARCHAR) AS VALUES (2, 20, 'c2'), (3, 30, 'c3'), (4, 40, 'c4'), (5, 50, 'c5')").await;

    exec(
        &handler,
        "MERGE INTO paimon.test_db.target t \
         USING source s ON t.a = s.a \
         WHEN NOT MATCHED THEN INSERT (a, b, c) VALUES (s.a, s.b, s.c)",
    )
    .await;

    assert_eq!(
        query_abc(&handler).await,
        vec![
            (1, 10, "c1".into()),
            (2, 20, "c2".into()),
            (3, 30, "c3".into()),
            (4, 40, "c4".into()),
            (5, 50, "c5".into()),
        ]
    );
}

/// Paimon MergeInto: conditional update — rows matching predicate get updated, others get deleted
#[tokio::test]
async fn test_conditional_update() {
    let (_tmp, handler) = setup_abc().await;
    exec(
        &handler,
        "INSERT INTO paimon.test_db.target VALUES (1, 10, 'c1'), (2, 20, 'c2'), (3, 30, 'c3')",
    )
    .await;
    ctx_exec(
        &handler,
        "CREATE TABLE source (a INT, b INT, c VARCHAR) AS VALUES (1, 100, 'c11'), (3, 300, 'c33')",
    )
    .await;

    exec(
        &handler,
        "MERGE INTO paimon.test_db.target t \
         USING source s ON t.a = s.a \
         WHEN MATCHED AND s.b > 200 THEN UPDATE SET b = s.b, c = s.c \
         WHEN MATCHED THEN DELETE \
         WHEN NOT MATCHED THEN INSERT (a, b, c) VALUES (s.a, s.b, s.c)",
    )
    .await;

    assert_eq!(
        query_abc(&handler).await,
        vec![(2, 20, "c2".into()), (3, 300, "c33".into()),]
    );
}

/// Paimon MergeInto: conditional insert — only insert rows matching predicate
#[tokio::test]
async fn test_conditional_insert() {
    let (_tmp, handler) = setup_abc().await;
    exec(
        &handler,
        "INSERT INTO paimon.test_db.target VALUES (1, 10, 'c1'), (2, 20, 'c2')",
    )
    .await;
    ctx_exec(
        &handler,
        "CREATE TABLE source (a INT, b INT, c VARCHAR) AS VALUES (1, 100, 'c11'), (3, 300, 'c33')",
    )
    .await;

    exec(
        &handler,
        "MERGE INTO paimon.test_db.target t \
         USING source s ON t.a = s.a \
         WHEN MATCHED THEN UPDATE SET b = s.b, c = s.c \
         WHEN NOT MATCHED AND s.b < 300 THEN INSERT (a, b, c) VALUES (s.a, s.b, s.c)",
    )
    .await;

    // a=1 updated, a=3 NOT inserted (b=300 not < 300)
    assert_eq!(
        query_abc(&handler).await,
        vec![(1, 100, "c11".into()), (2, 20, "c2".into()),]
    );
}

/// Paimon MergeInto: conditional delete — only delete rows matching predicate
#[tokio::test]
async fn test_conditional_delete() {
    let (_tmp, handler) = setup_abc().await;
    exec(
        &handler,
        "INSERT INTO paimon.test_db.target VALUES (1, 10, 'c1'), (2, 20, 'c2')",
    )
    .await;
    ctx_exec(
        &handler,
        "CREATE TABLE source (a INT, b INT, c VARCHAR) AS VALUES (1, 100, 'c11'), (3, 300, 'c33')",
    )
    .await;

    exec(
        &handler,
        "MERGE INTO paimon.test_db.target t \
         USING source s ON t.a = s.a \
         WHEN MATCHED AND t.c < 'c1' THEN DELETE \
         WHEN NOT MATCHED THEN INSERT (a, b, c) VALUES (s.a, s.b, s.c)",
    )
    .await;

    // a=1 matched but c='c1' is NOT < 'c1', so not deleted
    assert_eq!(
        query_abc(&handler).await,
        vec![
            (1, 10, "c1".into()),
            (2, 20, "c2".into()),
            (3, 300, "c33".into()),
        ]
    );
}

/// Paimon MergeInto: multiple matched clauses with predicates
#[tokio::test]
async fn test_multiple_matched_clauses() {
    let (_tmp, handler) = setup_abc().await;
    exec(&handler, "INSERT INTO paimon.test_db.target VALUES (1, 10, 'c1'), (2, 20, 'c2'), (3, 30, 'c3'), (4, 40, 'c4'), (5, 50, 'c5')").await;
    ctx_exec(&handler, "CREATE TABLE source (a INT, b INT, c VARCHAR) AS VALUES (1, 100, 'c11'), (3, 300, 'c33'), (5, 500, 'c55'), (7, 700, 'c77'), (9, 900, 'c99')").await;

    exec(&handler,
        "MERGE INTO paimon.test_db.target t \
         USING source s ON t.a = s.a \
         WHEN MATCHED AND t.a = 5 THEN UPDATE SET b = s.b + t.b \
         WHEN MATCHED AND s.c > 'c2' THEN UPDATE SET a = s.a, b = s.b, c = s.c \
         WHEN MATCHED THEN DELETE \
         WHEN NOT MATCHED AND s.c > 'c9' THEN INSERT (a, b, c) VALUES (s.a, CAST(CAST(s.b AS DOUBLE) * 1.1 AS INT), s.c) \
         WHEN NOT MATCHED THEN INSERT (a, b, c) VALUES (s.a, s.b, s.c)").await;

    // a=1: matched, a!=5, c='c11' not > 'c2' → DELETE
    // a=3: matched, a!=5, c='c33' > 'c2' → UPDATE SET *
    // a=5: matched, a=5 → UPDATE SET b = 500+50 = 550
    // a=7: not matched, c='c77' not > 'c9' → INSERT *
    // a=9: not matched, c='c99' > 'c9' → INSERT with b=990
    assert_eq!(
        query_abc(&handler).await,
        vec![
            (2, 20, "c2".into()),
            (3, 300, "c33".into()),
            (4, 40, "c4".into()),
            (5, 550, "c5".into()),
            (7, 700, "c77".into()),
            (9, 990, "c99".into()),
        ]
    );
}

// ======================= Partitioned table tests =======================

#[tokio::test]
async fn test_partitioned_update_single_partition() {
    let (_tmp, handler) = setup_partitioned().await;
    ctx_exec(
        &handler,
        "CREATE TABLE source (a INT, b INT, pt INT) AS VALUES (1, 100, 1)",
    )
    .await;

    exec(
        &handler,
        "MERGE INTO paimon.test_db.target t \
         USING source s ON t.a = s.a AND t.pt = s.pt \
         WHEN MATCHED THEN UPDATE SET b = s.b",
    )
    .await;

    assert_eq!(
        query_a_b_pt(&handler).await,
        vec![(1, 100, 1), (2, 20, 1), (3, 30, 2), (4, 40, 2),]
    );
}

#[tokio::test]
async fn test_partitioned_update_multiple_partitions() {
    let (_tmp, handler) = setup_partitioned().await;
    ctx_exec(
        &handler,
        "CREATE TABLE source (a INT, b INT, pt INT) AS VALUES (1, 100, 1), (3, 300, 2)",
    )
    .await;

    exec(
        &handler,
        "MERGE INTO paimon.test_db.target t \
         USING source s ON t.a = s.a AND t.pt = s.pt \
         WHEN MATCHED THEN UPDATE SET b = s.b",
    )
    .await;

    assert_eq!(
        query_a_b_pt(&handler).await,
        vec![(1, 100, 1), (2, 20, 1), (3, 300, 2), (4, 40, 2),]
    );
}

#[tokio::test]
async fn test_partitioned_delete() {
    let (_tmp, handler) = setup_partitioned().await;
    ctx_exec(
        &handler,
        "CREATE TABLE source (a INT, pt INT) AS VALUES (1, 1), (3, 2)",
    )
    .await;

    exec(
        &handler,
        "MERGE INTO paimon.test_db.target t \
         USING source s ON t.a = s.a AND t.pt = s.pt \
         WHEN MATCHED THEN DELETE",
    )
    .await;

    assert_eq!(query_a_b_pt(&handler).await, vec![(2, 20, 1), (4, 40, 2),]);
}

#[tokio::test]
async fn test_partitioned_insert_new_partition() {
    let (_tmp, handler) = setup_partitioned().await;
    ctx_exec(
        &handler,
        "CREATE TABLE source (a INT, b INT, pt INT) AS VALUES (5, 50, 3), (6, 60, 3)",
    )
    .await;

    exec(
        &handler,
        "MERGE INTO paimon.test_db.target t \
         USING source s ON t.a = s.a AND t.pt = s.pt \
         WHEN NOT MATCHED THEN INSERT (a, b, pt) VALUES (s.a, s.b, s.pt)",
    )
    .await;

    assert_eq!(
        query_a_b_pt(&handler).await,
        vec![
            (1, 10, 1),
            (2, 20, 1),
            (3, 30, 2),
            (4, 40, 2),
            (5, 50, 3),
            (6, 60, 3),
        ]
    );
}

#[tokio::test]
async fn test_partitioned_update_and_insert() {
    let (_tmp, handler) = setup_partitioned().await;
    ctx_exec(
        &handler,
        "CREATE TABLE source (a INT, b INT, pt INT) AS VALUES (1, 100, 1), (5, 50, 2)",
    )
    .await;

    exec(
        &handler,
        "MERGE INTO paimon.test_db.target t \
         USING source s ON t.a = s.a AND t.pt = s.pt \
         WHEN MATCHED THEN UPDATE SET b = s.b \
         WHEN NOT MATCHED THEN INSERT (a, b, pt) VALUES (s.a, s.b, s.pt)",
    )
    .await;

    assert_eq!(
        query_a_b_pt(&handler).await,
        vec![(1, 100, 1), (2, 20, 1), (3, 30, 2), (4, 40, 2), (5, 50, 2),]
    );
}

#[tokio::test]
async fn test_partitioned_successive_merges() {
    let (_tmp, handler) = setup_partitioned().await;

    ctx_exec(
        &handler,
        "CREATE TABLE src1 (a INT, b INT, pt INT) AS VALUES (1, 100, 1)",
    )
    .await;
    exec(
        &handler,
        "MERGE INTO paimon.test_db.target t \
         USING src1 s ON t.a = s.a AND t.pt = s.pt \
         WHEN MATCHED THEN UPDATE SET b = s.b",
    )
    .await;

    ctx_exec(
        &handler,
        "CREATE TABLE src2 (a INT, b INT, pt INT) AS VALUES (3, 300, 2)",
    )
    .await;
    exec(
        &handler,
        "MERGE INTO paimon.test_db.target t \
         USING src2 s ON t.a = s.a AND t.pt = s.pt \
         WHEN MATCHED THEN UPDATE SET b = s.b",
    )
    .await;

    assert_eq!(
        query_a_b_pt(&handler).await,
        vec![(1, 100, 1), (2, 20, 1), (3, 300, 2), (4, 40, 2),]
    );
}
