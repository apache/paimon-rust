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

use crate::table::pk_vector_position_read::PKEY_VECTOR_POSITION_COLUMN;
use crate::table::vector_search_common::resolve_materialize_read_type;
use crate::table::vector_search_test_utils::{
    de_vector_table, pk_vector_table_with_extra_column, vector_test_table,
};
use std::collections::HashMap;

#[tokio::test]
async fn test_batch_vector_search_requires_vectors() {
    let table = vector_test_table();
    let err = table
        .new_batch_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vectors(Vec::new())
        .with_limit(1)
        .execute()
        .await
        .unwrap_err();

    assert!(
        err.to_string()
            .contains("Query vectors must be set via with_query_vectors()"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_batch_vector_search_rejects_zero_limit() {
    let table = vector_test_table();
    let err = table
        .new_batch_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vectors(vec![vec![1.0]])
        .with_limit(0)
        .execute()
        .await
        .unwrap_err();

    assert!(
        err.to_string().contains("Limit must be between 1"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn empty_prepared_filter_does_not_mask_an_invalid_query_in_the_batch() {
    use crate::table::vector_search_test_utils::id_gt_filter;

    let table = vector_test_table();
    let prepared = table
        .prepare_vector_search_filter(id_gt_filter(&table, 0))
        .await
        .unwrap();
    assert!(prepared.include_row_ids().is_empty());
    let error = table
        .new_batch_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vectors(vec![vec![1.0, 0.0], vec![]])
        .with_limit(2)
        .with_prepared_filter(prepared)
        .execute()
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("Search vector cannot be empty"),
        "{error}"
    );
}

#[tokio::test]
async fn single_and_batch_de_readers_preserve_query_option_precedence() {
    let table = de_vector_table().await.copy_with_options(HashMap::from([(
        "fields.embedding.ivf.refine-factor".to_string(),
        "invalid".to_string(),
    )]));
    let options = HashMap::from([("refine_factor".to_string(), "1".to_string())]);
    let mut single = table.new_vector_search_builder();
    single
        .with_vector_column("embedding")
        .with_query_vector(vec![0.0, 1.0])
        .with_limit(1);
    assert!(
        single.execute().await.is_err(),
        "the invalid table option must require an override"
    );
    let result = single
        .with_options(options.clone())
        .execute()
        .await
        .unwrap();
    assert_eq!(result.row_ids().unwrap().row_ids, vec![1]);

    let results = table
        .new_batch_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vectors(vec![vec![1.0, 0.0], vec![0.0, 1.0]])
        .with_limit(2)
        .with_options(options)
        .execute()
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    for (result, expected) in results.iter().zip([vec![0, 2], vec![1, 2]]) {
        assert_eq!(result.row_ids().unwrap().row_ids, expected);
    }
}

#[tokio::test]
async fn test_batch_execute_fails_closed_when_query_auth_enabled() {
    // The batch scored entry returns data-derived row ids/scores outside
    // `TableScan`/`TableRead`, so it must fail closed under
    // `query-auth.enabled` exactly like the single-query builder. Its config
    // is otherwise valid, so without the guard the empty-snapshot fast path
    // would return empty results and silently bypass authorization.
    let table = crate::table::query_auth_table();
    let err = table
        .new_batch_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vectors(vec![vec![1.0, 2.0]])
        .with_limit(5)
        .execute()
        .await
        .unwrap_err();
    assert!(
        matches!(err, crate::Error::Unsupported { ref message } if message.contains("query-auth.enabled")),
        "batch vector search must fail closed for a query-auth table, got: {err:?}"
    );
}

#[test]
fn batch_resolve_materialize_read_type_default_rejects_reserved_user_column() {
    // Same guard on the batch resolver.
    let table = pk_vector_table_with_extra_column(PKEY_VECTOR_POSITION_COLUMN);
    let err = resolve_materialize_read_type(&table, None).unwrap_err();
    assert!(
        matches!(err, crate::Error::DataInvalid { ref message, .. }
            if message.contains("reserved column")),
        "batch default projection must reject reserved user column, got: {err:?}"
    );
}
