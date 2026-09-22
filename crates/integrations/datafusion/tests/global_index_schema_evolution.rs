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

//! A sorted global index is read with a comparator built from the column's *current*
//! type, and nothing records the type the index was built with. Widening an indexed
//! column is allowed, so the index must be left alone once its keys can no longer
//! belong to that type.

mod common;

use common::{exec, row_count, setup_sql_context};

async fn setup_indexed_table(
    table_name: &str,
    column_type: &str,
) -> (tempfile::TempDir, paimon_datafusion::SQLContext) {
    let (tmp, sql_context) = setup_sql_context().await;
    exec(
        &sql_context,
        &format!(
            "CREATE TABLE paimon.test_db.{table_name} (id {column_type}, name VARCHAR(100)) WITH (\
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
async fn test_widening_an_indexed_int_column_keeps_answering_queries() {
    let (_tmp, sql_context) = setup_indexed_table("gi_widen_int", "INT").await;
    for id in 1..=40 {
        exec(
            &sql_context,
            &format!("INSERT INTO paimon.test_db.gi_widen_int (id, name) VALUES ({id}, 'n{id}')"),
        )
        .await;
    }
    exec(
        &sql_context,
        "CALL sys.create_global_index(table => 'test_db.gi_widen_int', index_column => 'id')",
    )
    .await;
    assert_eq!(
        row_count(
            &sql_context,
            "SELECT * FROM paimon.test_db.gi_widen_int WHERE id = 7"
        )
        .await,
        1,
        "the index must answer the query before the type change"
    );

    // Allowed: `UpdateColumnType` guards partition, primary-key, bucket-key and
    // primary-key-index columns, but not global-index columns.
    exec(
        &sql_context,
        "ALTER TABLE paimon.test_db.gi_widen_int ALTER COLUMN id TYPE BIGINT",
    )
    .await;

    // The index keys are still 4 bytes wide; the BIGINT comparator used to read 8 and
    // panic with "range end index 8 out of range for slice of length 4".
    assert_eq!(
        row_count(
            &sql_context,
            "SELECT * FROM paimon.test_db.gi_widen_int WHERE id = 7"
        )
        .await,
        1,
        "widening an indexed column must fall back to a scan, not panic"
    );
    assert_eq!(
        row_count(
            &sql_context,
            "SELECT * FROM paimon.test_db.gi_widen_int WHERE id > 35"
        )
        .await,
        5
    );
    assert_eq!(
        row_count(
            &sql_context,
            "SELECT * FROM paimon.test_db.gi_widen_int WHERE id = 99"
        )
        .await,
        0
    );
}
