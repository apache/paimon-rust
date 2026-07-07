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

use common::{assert_sql_error, collect_id_name, exec, row_count, setup_sql_context};

async fn setup_table_with_snapshots() -> (tempfile::TempDir, paimon_datafusion::SQLContext) {
    let (tmp, sql_context) = setup_sql_context().await;
    exec(
        &sql_context,
        "CREATE TABLE paimon.test_db.t1 (id INT, name VARCHAR(100), PRIMARY KEY (id))",
    )
    .await;
    // Insert data to create snapshot 1
    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.t1 VALUES (1, 'alice')",
    )
    .await;
    // Insert data to create snapshot 2
    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.t1 VALUES (2, 'bob')",
    )
    .await;
    // Insert data to create snapshot 3
    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.t1 VALUES (3, 'charlie')",
    )
    .await;
    (tmp, sql_context)
}

#[tokio::test]
async fn test_create_tag() {
    let (_tmp, sql_context) = setup_table_with_snapshots().await;

    // Create tag from latest snapshot
    exec(
        &sql_context,
        "CALL sys.create_tag(table => 'test_db.t1', tag => 'v1')",
    )
    .await;

    // Verify tag exists via $tags system table
    let count = row_count(
        &sql_context,
        "SELECT * FROM paimon.test_db.`t1$tags` WHERE tag_name = 'v1'",
    )
    .await;
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_create_tag_with_snapshot_id() {
    let (_tmp, sql_context) = setup_table_with_snapshots().await;

    exec(
        &sql_context,
        "CALL sys.create_tag(table => 'test_db.t1', tag => 'v1', snapshot_id => '1')",
    )
    .await;

    let count = row_count(
        &sql_context,
        "SELECT * FROM paimon.test_db.`t1$tags` WHERE tag_name = 'v1'",
    )
    .await;
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_create_lumina_index_requires_index_column() {
    let (_tmp, sql_context) = setup_table_with_snapshots().await;

    assert_sql_error(
        &sql_context,
        "CALL sys.create_lumina_index(table => 'test_db.t1')",
        "Missing required argument: 'index_column'",
    )
    .await;
}

#[tokio::test]
async fn test_create_lumina_index_rejects_invalid_index_type() {
    let (_tmp, sql_context) = setup_table_with_snapshots().await;

    assert_sql_error(
        &sql_context,
        "CALL sys.create_lumina_index(table => 'test_db.t1', index_column => 'name', index_type => 'btree')",
        "Unsupported Lumina index type: btree",
    )
    .await;
}

#[tokio::test]
async fn test_create_lumina_index_rejects_invalid_options() {
    let (_tmp, sql_context) = setup_table_with_snapshots().await;

    assert_sql_error(
        &sql_context,
        "CALL sys.create_lumina_index(table => 'test_db.t1', index_column => 'name', options => 'lumina.index.dimension')",
        "Expected comma-separated key=value pairs",
    )
    .await;
}

async fn setup_btree_global_index_table(
    table_name: &str,
) -> (tempfile::TempDir, paimon_datafusion::SQLContext) {
    let (tmp, sql_context) = setup_sql_context().await;
    exec(
        &sql_context,
        &format!(
            "CREATE TABLE paimon.test_db.{table_name} (id INT, name VARCHAR(100)) WITH (\
                'row-tracking.enabled' = 'true',\
                'data-evolution.enabled' = 'true',\
                'global-index.enabled' = 'true',\
                'sorted-index.records-per-range' = '10'\
            )"
        ),
    )
    .await;
    (tmp, sql_context)
}

#[tokio::test]
async fn test_create_global_index_requires_index_column() {
    let (_tmp, sql_context) = setup_btree_global_index_table("btree_missing_col").await;

    assert_sql_error(
        &sql_context,
        "CALL sys.create_global_index(table => 'test_db.btree_missing_col')",
        "Missing required argument: 'index_column'",
    )
    .await;
}

#[tokio::test]
async fn test_create_global_index_rejects_unsupported_index_type() {
    let (_tmp, sql_context) = setup_btree_global_index_table("global_index_bad_type").await;

    assert_sql_error(
        &sql_context,
        "CALL sys.create_global_index(table => 'test_db.global_index_bad_type', index_column => 'id', index_type => 'full-text')",
        "only supports index_type => 'btree', 'bitmap'",
    )
    .await;
}

#[tokio::test]
async fn test_create_global_index_rejects_options() {
    let (_tmp, sql_context) = setup_btree_global_index_table("btree_options").await;

    assert_sql_error(
        &sql_context,
        "CALL sys.create_global_index(table => 'test_db.btree_options', index_column => 'id', options => 'x=y')",
        "options are not supported",
    )
    .await;
}

#[tokio::test]
async fn test_create_global_index_builds_btree_and_filter_reads() {
    let (_tmp, sql_context) = setup_btree_global_index_table("btree_build").await;
    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.btree_build (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')",
    )
    .await;

    exec(
        &sql_context,
        "CALL sys.create_global_index(table => 'test_db.btree_build', index_column => 'id', index_type => 'btree')",
    )
    .await;

    let index_count = row_count(
        &sql_context,
        "SELECT * FROM paimon.test_db.`btree_build$table_indexes` \
         WHERE index_type = 'btree' AND row_range_start = 0 AND row_range_end = 2 \
         AND index_field_name = 'id'",
    )
    .await;
    assert_eq!(index_count, 1);

    let rows = collect_id_name(
        &sql_context,
        "SELECT id, name FROM paimon.test_db.btree_build WHERE id = 2",
    )
    .await;
    assert_eq!(rows, vec![(2, "bob".to_string())]);
}

#[tokio::test]
async fn test_create_global_index_btree_string_fallback_scan_reads() {
    let (_tmp, sql_context) = setup_btree_global_index_table("btree_string_fallback").await;
    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.btree_string_fallback (id, name) VALUES \
         (1, 'alice'), (2, 'alpine'), (3, 'bob'), (4, 'carol'), (5, 'malice')",
    )
    .await;

    exec(
        &sql_context,
        "CALL sys.create_global_index(table => 'test_db.btree_string_fallback', index_column => 'name', index_type => 'btree')",
    )
    .await;

    let rows = collect_id_name(
        &sql_context,
        "SELECT id, name FROM paimon.test_db.btree_string_fallback WHERE name LIKE 'a%c_'",
    )
    .await;
    assert_eq!(rows, vec![(1, "alice".to_string())]);
}

#[tokio::test]
async fn test_create_global_index_builds_bitmap_with_java_format() {
    let (_tmp, sql_context) = setup_btree_global_index_table("bitmap_build").await;
    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.bitmap_build (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'alice')",
    )
    .await;

    exec(
        &sql_context,
        "CALL sys.create_global_index(table => 'test_db.bitmap_build', index_column => 'name', index_type => 'bitmap')",
    )
    .await;

    let index_count = row_count(
        &sql_context,
        "SELECT * FROM paimon.test_db.`bitmap_build$table_indexes` \
         WHERE index_type = 'bitmap' AND row_range_start = 0 AND row_range_end = 2 \
         AND index_field_name = 'name'",
    )
    .await;
    assert_eq!(index_count, 1);

    let rows = collect_id_name(
        &sql_context,
        "SELECT id, name FROM paimon.test_db.bitmap_build WHERE name = 'alice'",
    )
    .await;
    assert_eq!(
        rows,
        vec![(1, "alice".to_string()), (3, "alice".to_string())]
    );

    let contains_rows = collect_id_name(
        &sql_context,
        "SELECT id, name FROM paimon.test_db.bitmap_build WHERE name LIKE '%lic%'",
    )
    .await;
    assert_eq!(
        contains_rows,
        vec![(1, "alice".to_string()), (3, "alice".to_string())]
    );
}

#[tokio::test]
async fn test_drop_global_index_removes_btree_and_reads_fallback() {
    let (_tmp, sql_context) = setup_btree_global_index_table("btree_drop").await;
    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.btree_drop (id, name) VALUES (1, 'alice'), (2, 'bob')",
    )
    .await;
    exec(
        &sql_context,
        "CALL sys.create_global_index(table => 'test_db.btree_drop', index_column => 'id')",
    )
    .await;
    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.btree_drop (id, name) VALUES (3, 'carol')",
    )
    .await;

    let fast_rows = collect_id_name(
        &sql_context,
        "SELECT id, name FROM paimon.test_db.btree_drop WHERE id >= 2",
    )
    .await;
    assert_eq!(fast_rows, vec![(2, "bob".to_string())]);

    exec(
        &sql_context,
        "CALL sys.drop_global_index(table => 'test_db.btree_drop', index_column => 'id', index_type => 'btree')",
    )
    .await;

    let index_count = row_count(
        &sql_context,
        "SELECT * FROM paimon.test_db.`btree_drop$table_indexes` \
         WHERE index_type = 'btree' AND index_field_name = 'id'",
    )
    .await;
    assert_eq!(index_count, 0);

    let rows = collect_id_name(
        &sql_context,
        "SELECT id, name FROM paimon.test_db.btree_drop WHERE id >= 2",
    )
    .await;
    assert_eq!(rows, vec![(2, "bob".to_string()), (3, "carol".to_string())]);
}

#[tokio::test]
async fn test_drop_global_index_removes_bitmap() {
    let (_tmp, sql_context) = setup_btree_global_index_table("bitmap_drop").await;
    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.bitmap_drop (id, name) VALUES (1, 'alice'), (2, 'bob')",
    )
    .await;
    exec(
        &sql_context,
        "CALL sys.create_global_index(table => 'test_db.bitmap_drop', index_column => 'name', index_type => 'bitmap')",
    )
    .await;

    exec(
        &sql_context,
        "CALL sys.drop_global_index(table => 'test_db.bitmap_drop', index_column => 'name', index_type => 'bitmap')",
    )
    .await;

    let index_count = row_count(
        &sql_context,
        "SELECT * FROM paimon.test_db.`bitmap_drop$table_indexes` \
         WHERE index_type = 'bitmap' AND index_field_name = 'name'",
    )
    .await;
    assert_eq!(index_count, 0);
}

#[tokio::test]
async fn test_drop_global_index_requires_index_column() {
    let (_tmp, sql_context) = setup_btree_global_index_table("btree_drop_missing_col").await;

    assert_sql_error(
        &sql_context,
        "CALL sys.drop_global_index(table => 'test_db.btree_drop_missing_col')",
        "Missing required argument: 'index_column'",
    )
    .await;
}

#[tokio::test]
async fn test_drop_global_index_rejects_unsupported_index_type() {
    let (_tmp, sql_context) = setup_btree_global_index_table("global_index_drop_bad_type").await;

    assert_sql_error(
        &sql_context,
        "CALL sys.drop_global_index(table => 'test_db.global_index_drop_bad_type', index_column => 'id', index_type => 'full-text')",
        "only supports index_type => 'btree' or 'bitmap'",
    )
    .await;
}

#[tokio::test]
async fn test_drop_global_index_without_match_succeeds() {
    let (_tmp, sql_context) = setup_btree_global_index_table("btree_drop_no_match").await;
    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.btree_drop_no_match (id, name) VALUES (1, 'alice')",
    )
    .await;

    exec(
        &sql_context,
        "CALL sys.drop_global_index(table => 'test_db.btree_drop_no_match', index_column => 'id')",
    )
    .await;

    let rows = collect_id_name(
        &sql_context,
        "SELECT id, name FROM paimon.test_db.btree_drop_no_match WHERE id = 1",
    )
    .await;
    assert_eq!(rows, vec![(1, "alice".to_string())]);
}

#[tokio::test]
async fn test_create_global_index_fast_full_detail_after_append() {
    let (_tmp, sql_context) = setup_btree_global_index_table("btree_coverage").await;
    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.btree_coverage (id, name) VALUES (1, 'alice'), (2, 'bob')",
    )
    .await;
    exec(
        &sql_context,
        "CALL sys.create_global_index(table => 'test_db.btree_coverage', index_column => 'id')",
    )
    .await;
    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.btree_coverage (id, name) VALUES (3, 'carol')",
    )
    .await;

    let fast_rows = collect_id_name(
        &sql_context,
        "SELECT id, name FROM paimon.test_db.btree_coverage WHERE id >= 2",
    )
    .await;
    assert_eq!(fast_rows, vec![(2, "bob".to_string())]);

    exec(
        &sql_context,
        "SET 'paimon.global-index.search-mode' = 'full'",
    )
    .await;
    let full_rows = collect_id_name(
        &sql_context,
        "SELECT id, name FROM paimon.test_db.btree_coverage WHERE id >= 2",
    )
    .await;
    assert_eq!(
        full_rows,
        vec![(2, "bob".to_string()), (3, "carol".to_string())]
    );

    exec(
        &sql_context,
        "SET 'paimon.global-index.search-mode' = 'detail'",
    )
    .await;
    let detail_rows = collect_id_name(
        &sql_context,
        "SELECT id, name FROM paimon.test_db.btree_coverage WHERE id >= 2",
    )
    .await;
    assert_eq!(
        detail_rows,
        vec![(2, "bob".to_string()), (3, "carol".to_string())]
    );
}

#[tokio::test]
async fn test_create_tag_already_exists() {
    let (_tmp, sql_context) = setup_table_with_snapshots().await;

    exec(
        &sql_context,
        "CALL sys.create_tag(table => 'test_db.t1', tag => 'v1')",
    )
    .await;

    let result = sql_context
        .sql("CALL sys.create_tag(table => 'test_db.t1', tag => 'v1')")
        .await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("already exists"));
}

#[tokio::test]
async fn test_delete_tag() {
    let (_tmp, sql_context) = setup_table_with_snapshots().await;

    exec(
        &sql_context,
        "CALL sys.create_tag(table => 'test_db.t1', tag => 'v1')",
    )
    .await;
    exec(
        &sql_context,
        "CALL sys.delete_tag(table => 'test_db.t1', tag => 'v1')",
    )
    .await;

    let count = row_count(
        &sql_context,
        "SELECT * FROM paimon.test_db.`t1$tags` WHERE tag_name = 'v1'",
    )
    .await;
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_delete_multiple_tags() {
    let (_tmp, sql_context) = setup_table_with_snapshots().await;

    exec(
        &sql_context,
        "CALL sys.create_tag(table => 'test_db.t1', tag => 'v1')",
    )
    .await;
    exec(
        &sql_context,
        "CALL sys.create_tag(table => 'test_db.t1', tag => 'v2', snapshot_id => '1')",
    )
    .await;

    exec(
        &sql_context,
        "CALL sys.delete_tag(table => 'test_db.t1', tag => 'v1,v2')",
    )
    .await;

    let count = row_count(&sql_context, "SELECT * FROM paimon.test_db.`t1$tags`").await;
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_rollback_to_snapshot() {
    let (_tmp, sql_context) = setup_table_with_snapshots().await;

    // We have 3 snapshots. Rollback to snapshot 1.
    exec(
        &sql_context,
        "CALL sys.rollback_to(table => 'test_db.t1', snapshot_id => '1')",
    )
    .await;

    // After rollback, only snapshot 1 data should be visible
    let count = row_count(&sql_context, "SELECT * FROM paimon.test_db.t1").await;
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_rollback_to_tag() {
    let (_tmp, sql_context) = setup_table_with_snapshots().await;

    // Create tag on snapshot 1
    exec(
        &sql_context,
        "CALL sys.create_tag(table => 'test_db.t1', tag => 'v1', snapshot_id => '1')",
    )
    .await;

    // Rollback to tag
    exec(
        &sql_context,
        "CALL sys.rollback_to(table => 'test_db.t1', tag => 'v1')",
    )
    .await;

    let count = row_count(&sql_context, "SELECT * FROM paimon.test_db.t1").await;
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_rollback_to_timestamp() {
    let (_tmp, sql_context) = setup_table_with_snapshots().await;

    // Get the timestamp of snapshot 1 from $snapshots system table
    let batches = sql_context
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
        &sql_context,
        &format!(
            "CALL sys.rollback_to_timestamp(table => 'test_db.t1', timestamp => '{snap1_time}')"
        ),
    )
    .await;

    let count = row_count(&sql_context, "SELECT * FROM paimon.test_db.t1").await;
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_create_tag_from_timestamp() {
    let (_tmp, sql_context) = setup_table_with_snapshots().await;

    // Get the timestamp of snapshot 2
    let batches = sql_context
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
        &sql_context,
        &format!(
            "CALL sys.create_tag_from_timestamp(table => 'test_db.t1', tag => 'ts_tag', timestamp => '{snap2_time}')"
        ),
    )
    .await;

    let count = row_count(
        &sql_context,
        "SELECT * FROM paimon.test_db.`t1$tags` WHERE tag_name = 'ts_tag'",
    )
    .await;
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_rollback_cleans_newer_tags() {
    let (_tmp, sql_context) = setup_table_with_snapshots().await;

    // Create tags on snapshot 2 and 3
    exec(
        &sql_context,
        "CALL sys.create_tag(table => 'test_db.t1', tag => 'v2', snapshot_id => '2')",
    )
    .await;
    exec(
        &sql_context,
        "CALL sys.create_tag(table => 'test_db.t1', tag => 'v3', snapshot_id => '3')",
    )
    .await;

    // Rollback to snapshot 1 — tags v2 and v3 should be cleaned
    exec(
        &sql_context,
        "CALL sys.rollback_to(table => 'test_db.t1', snapshot_id => '1')",
    )
    .await;

    let count = row_count(&sql_context, "SELECT * FROM paimon.test_db.`t1$tags`").await;
    assert_eq!(count, 0);
}
