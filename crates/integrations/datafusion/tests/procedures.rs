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

use common::{exec, row_count, setup_handler};

async fn setup_table_with_snapshots() -> (tempfile::TempDir, paimon_datafusion::PaimonSqlHandler) {
    let (tmp, handler) = setup_handler().await;
    exec(
        &handler,
        "CREATE TABLE paimon.test_db.t1 (id INT, name VARCHAR(100), PRIMARY KEY (id))",
    )
    .await;
    // Insert data to create snapshot 1
    exec(
        &handler,
        "INSERT INTO paimon.test_db.t1 VALUES (1, 'alice')",
    )
    .await;
    // Insert data to create snapshot 2
    exec(&handler, "INSERT INTO paimon.test_db.t1 VALUES (2, 'bob')").await;
    // Insert data to create snapshot 3
    exec(
        &handler,
        "INSERT INTO paimon.test_db.t1 VALUES (3, 'charlie')",
    )
    .await;
    (tmp, handler)
}

#[tokio::test]
async fn test_create_tag() {
    let (_tmp, handler) = setup_table_with_snapshots().await;

    // Create tag from latest snapshot
    exec(
        &handler,
        "CALL sys.create_tag(table => 'test_db.t1', tag => 'v1')",
    )
    .await;

    // Verify tag exists via $tags system table
    let count = row_count(
        &handler,
        "SELECT * FROM paimon.test_db.`t1$tags` WHERE tag_name = 'v1'",
    )
    .await;
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_create_tag_with_snapshot_id() {
    let (_tmp, handler) = setup_table_with_snapshots().await;

    exec(
        &handler,
        "CALL sys.create_tag(table => 'test_db.t1', tag => 'v1', snapshot_id => '1')",
    )
    .await;

    let count = row_count(
        &handler,
        "SELECT * FROM paimon.test_db.`t1$tags` WHERE tag_name = 'v1'",
    )
    .await;
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_create_tag_already_exists() {
    let (_tmp, handler) = setup_table_with_snapshots().await;

    exec(
        &handler,
        "CALL sys.create_tag(table => 'test_db.t1', tag => 'v1')",
    )
    .await;

    let result = handler
        .sql("CALL sys.create_tag(table => 'test_db.t1', tag => 'v1')")
        .await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("already exists"));
}

#[tokio::test]
async fn test_delete_tag() {
    let (_tmp, handler) = setup_table_with_snapshots().await;

    exec(
        &handler,
        "CALL sys.create_tag(table => 'test_db.t1', tag => 'v1')",
    )
    .await;
    exec(
        &handler,
        "CALL sys.delete_tag(table => 'test_db.t1', tag => 'v1')",
    )
    .await;

    let count = row_count(
        &handler,
        "SELECT * FROM paimon.test_db.`t1$tags` WHERE tag_name = 'v1'",
    )
    .await;
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_delete_multiple_tags() {
    let (_tmp, handler) = setup_table_with_snapshots().await;

    exec(
        &handler,
        "CALL sys.create_tag(table => 'test_db.t1', tag => 'v1')",
    )
    .await;
    exec(
        &handler,
        "CALL sys.create_tag(table => 'test_db.t1', tag => 'v2', snapshot_id => '1')",
    )
    .await;

    exec(
        &handler,
        "CALL sys.delete_tag(table => 'test_db.t1', tag => 'v1,v2')",
    )
    .await;

    let count = row_count(&handler, "SELECT * FROM paimon.test_db.`t1$tags`").await;
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_rollback_to_snapshot() {
    let (_tmp, handler) = setup_table_with_snapshots().await;

    // We have 3 snapshots. Rollback to snapshot 1.
    exec(
        &handler,
        "CALL sys.rollback_to(table => 'test_db.t1', snapshot_id => '1')",
    )
    .await;

    // After rollback, only snapshot 1 data should be visible
    let count = row_count(&handler, "SELECT * FROM paimon.test_db.t1").await;
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_rollback_to_tag() {
    let (_tmp, handler) = setup_table_with_snapshots().await;

    // Create tag on snapshot 1
    exec(
        &handler,
        "CALL sys.create_tag(table => 'test_db.t1', tag => 'v1', snapshot_id => '1')",
    )
    .await;

    // Rollback to tag
    exec(
        &handler,
        "CALL sys.rollback_to(table => 'test_db.t1', tag => 'v1')",
    )
    .await;

    let count = row_count(&handler, "SELECT * FROM paimon.test_db.t1").await;
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_rollback_to_timestamp() {
    let (_tmp, handler) = setup_table_with_snapshots().await;

    // Get the timestamp of snapshot 1 from $snapshots system table
    let batches = handler
        .sql("SELECT snapshot_id, commit_time FROM paimon.test_db.`t1$snapshots` ORDER BY snapshot_id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Use a timestamp between snapshot 1 and snapshot 2
    let snap1_time = batches[0]
        .column_by_name("commit_time")
        .unwrap()
        .as_any()
        .downcast_ref::<datafusion::arrow::array::TimestampMillisecondArray>()
        .unwrap()
        .value(0);

    exec(
        &handler,
        &format!(
            "CALL sys.rollback_to_timestamp(table => 'test_db.t1', timestamp => '{snap1_time}')"
        ),
    )
    .await;

    let count = row_count(&handler, "SELECT * FROM paimon.test_db.t1").await;
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_create_tag_from_timestamp() {
    let (_tmp, handler) = setup_table_with_snapshots().await;

    // Get the timestamp of snapshot 2
    let batches = handler
        .sql("SELECT snapshot_id, commit_time FROM paimon.test_db.`t1$snapshots` ORDER BY snapshot_id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let snap2_time = batches[0]
        .column_by_name("commit_time")
        .unwrap()
        .as_any()
        .downcast_ref::<datafusion::arrow::array::TimestampMillisecondArray>()
        .unwrap()
        .value(1);

    exec(
        &handler,
        &format!(
            "CALL sys.create_tag_from_timestamp(table => 'test_db.t1', tag => 'ts_tag', timestamp => '{snap2_time}')"
        ),
    )
    .await;

    let count = row_count(
        &handler,
        "SELECT * FROM paimon.test_db.`t1$tags` WHERE tag_name = 'ts_tag'",
    )
    .await;
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_rollback_cleans_newer_tags() {
    let (_tmp, handler) = setup_table_with_snapshots().await;

    // Create tags on snapshot 2 and 3
    exec(
        &handler,
        "CALL sys.create_tag(table => 'test_db.t1', tag => 'v2', snapshot_id => '2')",
    )
    .await;
    exec(
        &handler,
        "CALL sys.create_tag(table => 'test_db.t1', tag => 'v3', snapshot_id => '3')",
    )
    .await;

    // Rollback to snapshot 1 — tags v2 and v3 should be cleaned
    exec(
        &handler,
        "CALL sys.rollback_to(table => 'test_db.t1', snapshot_id => '1')",
    )
    .await;

    let count = row_count(&handler, "SELECT * FROM paimon.test_db.`t1$tags`").await;
    assert_eq!(count, 0);
}
