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

#![cfg(feature = "vortex")]

//! Vortex file format SQL end-to-end tests.

mod common;

use std::path::Path;

#[tokio::test]
async fn test_vortex_file_format_sql_e2e() {
    let (tmp, sql_context) = common::setup_sql_context().await;

    common::exec(
        &sql_context,
        "CREATE TABLE paimon.test_db.t (
            id INT,
            name STRING
        ) WITH (
            'file.format' = 'vortex'
        )",
    )
    .await;

    common::exec(
        &sql_context,
        "INSERT INTO paimon.test_db.t VALUES (1, 'Alice'), (2, 'Bob')",
    )
    .await;

    assert!(
        contains_vortex_file(tmp.path()),
        "expected Vortex data file"
    );

    let rows = common::collect_id_name_in_batch_order(
        &sql_context,
        "SELECT id, name FROM paimon.test_db.t ORDER BY id",
    )
    .await;
    assert_eq!(rows, vec![(1, "Alice".to_string()), (2, "Bob".to_string())]);

    let filtered = common::collect_id_name_in_batch_order(
        &sql_context,
        "SELECT id, name FROM paimon.test_db.t WHERE id = 2",
    )
    .await;
    assert_eq!(filtered, vec![(2, "Bob".to_string())]);
}

fn contains_vortex_file(path: &Path) -> bool {
    let entries = std::fs::read_dir(path).expect("read warehouse dir");
    for entry in entries {
        let path = entry.expect("read dir entry").path();
        if path.is_dir() {
            if contains_vortex_file(&path) {
                return true;
            }
        } else if path.extension().is_some_and(|ext| ext == "vortex") {
            return true;
        }
    }
    false
}
