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

use common::{assert_sql_error, exec, files_under, row_count, setup_sql_context};
use datafusion::assert_batches_eq;

#[tokio::test]
async fn bbox_intersects_uses_the_geometry_index_in_sql() {
    let (_tmp, ctx) = setup_sql_context().await;
    let table = "paimon.test_db.spatial";
    exec(
        &ctx,
        &format!(
            "CREATE TABLE {table} (id INT, shape GEOMETRY('EPSG:3857')) WITH (
                'row-tracking.enabled'='true',
                'data-evolution.enabled'='true',
                'global-index.enabled'='true',
                'scalar-index.search-mode'='fast',
                'rtree-index.partition-row-count'='1'
            )"
        ),
    )
    .await;
    let wkb = "decode('0101000000000000000000F03F0000000000000040', 'hex')";
    exec(
        &ctx,
        &format!("INSERT INTO {table} (id, shape) VALUES (0, {wkb}), (1, NULL)"),
    )
    .await;
    exec(
        &ctx,
        "CALL sys.create_global_index(
            table => 'test_db.spatial', index_column => 'shape', index_type => 'rtree'
        )",
    )
    .await;
    exec(
        &ctx,
        &format!("INSERT INTO {table} (id, shape) VALUES (2, {wkb})"),
    )
    .await;
    let query = format!("SELECT id FROM {table} WHERE bbox_intersects(shape, 0, 0, 1, 2)");
    assert_eq!(row_count(&ctx, &query).await, 1);
    let between = format!("{query} AND \"_ROW_ID\" BETWEEN 0 AND 100");
    let in_list = format!("{query} AND \"_ROW_ID\" IN (2, 0)");
    for sql in [&between, &in_list] {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        assert_batches_eq!(
            &["+----+", "| id |", "+----+", "| 0  |", "+----+"],
            &batches
        );
    }
    let excludes_indexed_match = format!("{query} AND \"_ROW_ID\" IN (1, 2)");
    assert_eq!(row_count(&ctx, &excludes_indexed_match).await, 0);
    assert_eq!(
        row_count(
            &ctx,
            &format!("SELECT id FROM {table} WHERE bbox_intersects(shape, 3, 0, 5, 2)")
        )
        .await,
        0
    );
    for mode in ["full", "detail"] {
        exec(
            &ctx,
            &format!("ALTER TABLE {table} SET TBLPROPERTIES ('scalar-index.search-mode'='{mode}')"),
        )
        .await;
        assert_eq!(row_count(&ctx, &between).await, 2);
        let batches = ctx
            .sql(&excludes_indexed_match)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_batches_eq!(
            &["+----+", "| id |", "+----+", "| 2  |", "+----+"],
            &batches
        );
    }
    exec(
        &ctx,
        "CALL sys.drop_global_index(
            table => 'test_db.spatial', index_column => 'shape', index_type => 'rtree'
        )",
    )
    .await;
    assert_eq!(row_count(&ctx, &query).await, 2);
    assert_eq!(row_count(&ctx, &between).await, 2);
    assert_eq!(
        row_count(
            &ctx,
            &format!("SELECT id FROM {table} WHERE bbox_intersects(shape, 0, 0, 1, 2) IS NULL")
        )
        .await,
        1
    );
    assert_eq!(
        row_count(
            &ctx,
            &format!("SELECT id FROM {table} WHERE NOT bbox_intersects(shape, 3, 3, 4, 4)")
        )
        .await,
        2
    );
    exec(
        &ctx,
        "CREATE TABLE paimon.test_db.globe (shape GEOGRAPHY)
         WITH ('row-tracking.enabled'='true', 'data-evolution.enabled'='true')",
    )
    .await;
    for sql in [
        "SELECT bbox_intersects(shape, 0, 0, 1, 2) FROM paimon.test_db.globe",
        "CALL sys.create_global_index(table => 'test_db.globe', index_column => 'shape', index_type => 'rtree')",
    ] {
        assert_sql_error(&ctx, sql, "GEOMETRY").await;
    }
}

#[tokio::test]
async fn failed_build_removes_already_written_spatial_files() {
    let (warehouse, ctx) = setup_sql_context().await;
    exec(
        &ctx,
        "CREATE TABLE paimon.test_db.invalid_wkb (shape GEOMETRY) WITH (
            'row-tracking.enabled'='true',
            'data-evolution.enabled'='true',
            'rtree-index.partition-row-count'='1'
        )",
    )
    .await;
    let wkb = "decode('0101000000000000000000F03F0000000000000040', 'hex')";
    exec(
        &ctx,
        &format!(
            "INSERT INTO paimon.test_db.invalid_wkb (shape) VALUES ({wkb}), ({wkb}), (decode('01', 'hex'))"
        ),
    )
    .await;
    assert_sql_error(
        &ctx,
        "CALL sys.create_global_index(
            table => 'test_db.invalid_wkb', index_column => 'shape', index_type => 'rtree'
        )",
        "Index build failed",
    )
    .await;
    assert_eq!(
        row_count(
            &ctx,
            "SELECT * FROM paimon.test_db.invalid_wkb$table_indexes"
        )
        .await,
        0
    );
    assert!(!files_under(warehouse.path())
        .iter()
        .any(|file| file.extension().is_some_and(|ext| ext == "index")));
}
