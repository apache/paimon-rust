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

//! Integration tests for `rowkind.field` (mirrors Java `BatchFileStoreITCase`).

#[path = "common/rowkind_helpers.rs"]
mod rowkind_helpers;

use rowkind_helpers::{
    make_batch_with_rowkind, make_batch_with_rowkind_and_value_kind, memory_table,
    persist_table_schema, rowkind_field_schema, scan_id_values, scan_pk_value_kind, setup_dirs,
    write_batch, write_batch_expect_err,
};
use std::collections::HashMap;

fn table_with_options(
    table: &paimon::table::Table,
    options: HashMap<String, String>,
) -> paimon::table::Table {
    table.copy_with_options(options)
}

#[tokio::test]
async fn rowkind_field_insert_then_delete() {
    let table_path = "memory:/rowkind_field/insert_delete";
    let schema = rowkind_field_schema("rf", &[]);
    let (file_io, table) = memory_table(table_path, schema);
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(
        &table,
        &make_batch_with_rowkind(vec![1], vec![1], vec!["+I"], "rf"),
    )
    .await;
    assert_eq!(
        scan_pk_value_kind(&table, "rf").await,
        vec![(1, 1, "+I".to_string())]
    );

    write_batch(
        &table,
        &make_batch_with_rowkind(vec![1], vec![2], vec!["-D"], "rf"),
    )
    .await;
    assert!(scan_id_values(&table).await.is_empty());
}

#[tokio::test]
async fn rowkind_field_update_tokens() {
    let table_path = "memory:/rowkind_field/update_tokens";
    let schema = rowkind_field_schema("rf", &[]);
    let (file_io, table) = memory_table(table_path, schema);
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(
        &table,
        &make_batch_with_rowkind(vec![1], vec![1], vec!["+I"], "rf"),
    )
    .await;
    write_batch(
        &table,
        &make_batch_with_rowkind(vec![1], vec![1], vec!["-U"], "rf"),
    )
    .await;
    assert!(scan_id_values(&table).await.is_empty());

    write_batch(
        &table,
        &make_batch_with_rowkind(vec![1], vec![10], vec!["+U"], "rf"),
    )
    .await;
    assert_eq!(
        scan_pk_value_kind(&table, "rf").await,
        vec![(1, 10, "+U".to_string())]
    );
}

#[tokio::test]
async fn rowkind_field_ignore_delete() {
    let table_path = "memory:/rowkind_field/ignore_delete";
    let schema = rowkind_field_schema("kind", &[("ignore-delete", "true")]);
    let (file_io, table) = memory_table(table_path, schema);
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(
        &table,
        &make_batch_with_rowkind(vec![1], vec![10], vec!["+I"], "kind"),
    )
    .await;
    assert_eq!(
        scan_pk_value_kind(&table, "kind").await,
        vec![(1, 10, "+I".to_string())]
    );

    write_batch(
        &table,
        &make_batch_with_rowkind(vec![1], vec![10], vec!["-D"], "kind"),
    )
    .await;
    assert_eq!(
        scan_pk_value_kind(&table, "kind").await,
        vec![(1, 10, "+I".to_string())]
    );

    write_batch(
        &table,
        &make_batch_with_rowkind(vec![1], vec![20], vec!["+I"], "kind"),
    )
    .await;
    assert_eq!(
        scan_pk_value_kind(&table, "kind").await,
        vec![(1, 20, "+I".to_string())]
    );
}

#[tokio::test]
async fn rowkind_field_ignore_update_before() {
    let table_path = "memory:/rowkind_field/ignore_update_before";
    let schema = rowkind_field_schema("kind", &[]);
    let (file_io, table) = memory_table(table_path, schema);
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(
        &table,
        &make_batch_with_rowkind(vec![1, 2], vec![10, 20], vec!["+I", "+I"], "kind"),
    )
    .await;
    write_batch(
        &table,
        &make_batch_with_rowkind(vec![2], vec![20], vec!["-U"], "kind"),
    )
    .await;
    assert_eq!(
        scan_pk_value_kind(&table, "kind").await,
        vec![(1, 10, "+I".to_string())]
    );

    let table = table_with_options(
        &table,
        HashMap::from([("ignore-update-before".to_string(), "true".to_string())]),
    );
    assert_eq!(
        scan_pk_value_kind(&table, "kind").await,
        vec![(1, 10, "+I".to_string())],
        "after option change before filtered -U"
    );

    write_batch(
        &table,
        &make_batch_with_rowkind(vec![1], vec![10], vec!["-U"], "kind"),
    )
    .await;
    assert_eq!(
        scan_pk_value_kind(&table, "kind").await,
        vec![(1, 10, "+I".to_string())]
    );

    write_batch(
        &table,
        &make_batch_with_rowkind(vec![1], vec![10], vec!["-D"], "kind"),
    )
    .await;
    assert!(scan_id_values(&table).await.is_empty());
}

#[tokio::test]
async fn rowkind_field_rejects_illegal_token() {
    let table_path = "memory:/rowkind_field/illegal_token";
    let schema = rowkind_field_schema("rf", &[]);
    let (file_io, table) = memory_table(table_path, schema);
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    let err = write_batch_expect_err(
        &table,
        &make_batch_with_rowkind(vec![1], vec![1], vec!["INSERT"], "rf"),
    )
    .await;
    assert!(
        matches!(err, paimon::Error::DataInvalid { ref message, .. }
            if message.contains("Unsupported short string")),
        "got {err:?}"
    );
}

#[tokio::test]
async fn rowkind_field_rejects_value_kind_conflict() {
    let table_path = "memory:/rowkind_field/value_kind_conflict";
    let schema = rowkind_field_schema("rf", &[]);
    let (file_io, table) = memory_table(table_path, schema);
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    let err = write_batch_expect_err(
        &table,
        &make_batch_with_rowkind_and_value_kind(vec![1], vec![1], vec!["+I"], "rf"),
    )
    .await;
    assert!(
        matches!(err, paimon::Error::DataInvalid { ref message, .. }
            if message.contains("_VALUE_KIND")),
        "got {err:?}"
    );
}
