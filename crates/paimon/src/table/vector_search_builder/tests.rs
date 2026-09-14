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

use crate::catalog::Identifier;
use crate::io::FileIOBuilder;
use crate::spec::{
    ArrayType, DataType, Datum, FloatType, IntType, Predicate, PredicateBuilder, Schema,
    TableSchema, ROW_ID_FIELD_NAME,
};
use crate::table::pk_vector_position_read::{PKEY_VECTOR_POSITION_COLUMN, SEARCH_SCORE_COLUMN};
use crate::table::vector_search_common::resolve_materialize_read_type;
use crate::table::vector_search_test_utils::{
    id_gt_filter, pk_vector_table, pk_vector_table_with_extra_column,
};
use crate::table::Table;
use crate::vindex::IVF_FLAT_IDENTIFIER;
use futures::TryStreamExt;
use std::collections::HashMap;

#[tokio::test]
async fn test_execute_fails_closed_when_query_auth_enabled() {
    let table = crate::table::query_auth_table();
    let err = table
        .new_vector_search_builder()
        .execute()
        .await
        .unwrap_err();
    assert!(
        matches!(err, crate::Error::Unsupported { ref message } if message.contains("query-auth.enabled")),
        "vector search must fail closed for a query-auth table"
    );
}

#[tokio::test]
async fn pk_branch_disabled_falls_through_to_de_path() {
    // No pk-vector.index.columns: behaves exactly as the DE path. With no
    // snapshot the DE path returns an empty result; the PK branch must not
    // intercept it.
    let table = pk_vector_table(&[]);
    let result = table
        .new_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vector(vec![1.0])
        .with_limit(5)
        .execute()
        .await
        .unwrap();
    assert!(result.is_empty());
}

#[tokio::test]
async fn pk_branch_execute_returns_physical_positions() {
    let table = pk_vector_table(&[
        ("pk-vector.index.columns", "embedding"),
        ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
        ("fields.embedding.pk-vector.distance.metric", "l2"),
        ("fields.embedding.dimension", "4"),
    ]);
    let result = table
        .new_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vector(vec![1.0; 4])
        .with_limit(5)
        .execute()
        .await
        .unwrap();
    assert!(result.positions().unwrap().is_empty());
    assert_eq!(result.snapshot_id(), None);
    assert!(result.row_ids().is_err());
}

#[tokio::test]
async fn pk_branch_other_column_falls_through_to_de_path() {
    // pk-vector index configured for "embedding", but the query targets a
    // different column -> the PK branch must not intercept; DE path (no
    // snapshot) yields empty. Discriminator: the PK column carries a
    // DELIBERATELY INVALID distance metric, which the PK branch parses eagerly
    // (`VectorSearchMetric::parse`) and would fail on. So a regression that
    // dropped the `pk_col == vector_column` guard and ran the PK branch for
    // "other" would surface as Err here, not Ok(empty) -- the assertion
    // therefore proves the DE path ran, not merely that the result is empty.
    let table = pk_vector_table(&[
        ("pk-vector.index.columns", "embedding"),
        ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
        (
            "fields.embedding.pk-vector.distance.metric",
            "not-a-real-metric",
        ),
    ]);
    let result = table
        .new_vector_search_builder()
        .with_vector_column("other")
        .with_query_vector(vec![1.0])
        .with_limit(5)
        .execute()
        .await
        .unwrap();
    assert!(result.is_empty());
}

#[tokio::test]
async fn pk_branch_multi_column_config_does_not_break_unrelated_de_query() {
    // A malformed multi-column PK-vector config ("a,b") must not abort an
    // unrelated DE vector query. The query targets a column NOT among the
    // configured PK-vector columns, so membership resolution short-circuits
    // before the exactly-one-column rule fires -- the query falls through to
    // the DE path (no snapshot -> empty) instead of surfacing the "must name
    // exactly one column" error.
    let table = pk_vector_table(&[
        ("pk-vector.index.columns", "a,b"),
        ("fields.a.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
        ("fields.a.pk-vector.distance.metric", "l2"),
    ]);
    let result = table
        .new_vector_search_builder()
        .with_vector_column("other")
        .with_query_vector(vec![1.0])
        .with_limit(5)
        .execute()
        .await;
    match result {
        Ok(search) => assert!(search.is_empty()),
        Err(err) => {
            panic!("unrelated DE query must not error on a malformed multi-column PK config: {err}")
        }
    }
}

#[tokio::test]
async fn result_read_filter_without_deletion_vectors_fails_loud() {
    let table = pk_vector_table(&[
        ("pk-vector.index.columns", "embedding"),
        ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
        ("fields.embedding.pk-vector.distance.metric", "l2"),
    ]);
    let filter = id_gt_filter(&table, 2);
    let err = async {
        table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .with_filter(filter)
            .execute()
            .await?
            .new_read_builder()
            .read()
            .await
    }
    .await
    .map(|_| ())
    .expect_err("read filter without deletion vectors must fail loud");
    assert!(
        matches!(err, crate::Error::DataInvalid { ref message, .. }
            if message.contains("deletion vectors without merge-on-read")),
        "unexpected error: {err:?}"
    );
}

#[tokio::test]
async fn result_read_filter_with_merge_on_read_fails_loud() {
    // Deletion vectors enabled BUT merge-on-read on: still rejected, because a
    // merge-on-read scan can surface stale key versions that a physical-row
    // filter cannot reconcile.
    let table = pk_vector_table(&[
        ("pk-vector.index.columns", "embedding"),
        ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
        ("fields.embedding.pk-vector.distance.metric", "l2"),
        ("deletion-vectors.enabled", "true"),
        ("deletion-vectors.merge-on-read", "true"),
    ]);
    let filter = id_gt_filter(&table, 2);
    let err = async {
        table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .with_filter(filter)
            .execute()
            .await?
            .new_read_builder()
            .read()
            .await
    }
    .await
    .map(|_| ())
    .expect_err("merge-on-read filter must fail loud");
    assert!(
        matches!(err, crate::Error::DataInvalid { ref message, .. }
            if message.contains("deletion vectors without merge-on-read")),
        "unexpected error: {err:?}"
    );
}

#[tokio::test]
async fn result_read_filter_with_deletion_vectors_passes_guard() {
    // Deletion vectors enabled, merge-on-read off (default): the residual guard
    // passes. With no snapshot the plan is empty, so the (guarded) filter path
    // simply yields an empty stream rather than erroring — proving the guard
    // admits a legal filtered query.
    let table = pk_vector_table(&[
        ("pk-vector.index.columns", "embedding"),
        ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
        ("fields.embedding.pk-vector.distance.metric", "l2"),
        ("deletion-vectors.enabled", "true"),
        // Pin the index dimension so the query vector below matches it; the
        // up-front dimension guard runs before this test's residual guard.
        ("fields.embedding.dimension", "4"),
    ]);
    let filter = id_gt_filter(&table, 2);
    let mut stream = async {
        table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0; 4])
            .with_limit(5)
            .with_filter(filter)
            .execute()
            .await?
            .new_read_builder()
            .read()
            .await
    }
    .await
    .expect("guarded filter query must be admitted");
    assert!(stream.try_next().await.unwrap().is_none());
}

/// A partition-only `with_filter` needs no per-row residual (partition pruning
/// happens in scan planning), so the deletion-vector pre-filter guard must NOT
/// reject it even when deletion vectors are off. Mirrors Java, where a
/// partition-only filter leaves `this.filter == null` and the scan guard is
/// skipped. Regression test for the guard keying on the whole filter rather
/// than its data conjuncts.
#[tokio::test]
async fn result_read_partition_only_filter_without_deletion_vectors_passes_guard() {
    use crate::spec::VarCharType;

    // Partitioned PK-vector table from old metadata, deletion vectors OFF.
    let schema = Schema::builder()
        .column("dt", DataType::VarChar(VarCharType::string_type()))
        .column("id", DataType::Int(IntType::new()))
        .column(
            "embedding",
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        )
        .partition_keys(["dt"])
        .primary_key(["id"])
        .option("bucket", "1")
        .build()
        .unwrap();
    let table_schema = TableSchema::new(0, &schema).copy_with_options(HashMap::from([
        (
            "pk-vector.index.columns".to_string(),
            "embedding".to_string(),
        ),
        (
            "fields.embedding.pk-vector.index.type".to_string(),
            IVF_FLAT_IDENTIFIER.to_string(),
        ),
        (
            "fields.embedding.pk-vector.distance.metric".to_string(),
            "l2".to_string(),
        ),
        ("fields.embedding.dimension".to_string(), "4".to_string()),
    ]));
    let table = Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("default", "pk_vector_partitioned"),
        "memory:/pk_vector_partitioned".to_string(),
        table_schema,
        None,
    );

    // Partition-only `dt = 'a'`: no data residual, so the guard admits it and
    // (with no snapshot) the query yields an empty stream instead of the
    // deletion-vector error.
    let filter = PredicateBuilder::new(table.schema().fields())
        .equal("dt", Datum::String("a".to_string()))
        .unwrap();
    let mut stream = async {
        table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0; 4])
            .with_limit(5)
            .with_filter(filter)
            .execute()
            .await?
            .new_read_builder()
            .read()
            .await
    }
    .await
    .expect("partition-only filter must be admitted without deletion vectors");
    assert!(stream.try_next().await.unwrap().is_none());

    // But a DATA conjunct (`id > 2`) on the same non-DV table must still fail
    // loud — the guard now keys on data predicates, not the whole filter.
    let data_filter = id_gt_filter(&table, 2);
    let err = async {
        table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0; 4])
            .with_limit(5)
            .with_filter(data_filter)
            .execute()
            .await?
            .new_read_builder()
            .read()
            .await
    }
    .await
    .map(|_| ())
    .expect_err("data filter without deletion vectors must still fail loud");
    assert!(
        matches!(err, crate::Error::DataInvalid { ref message, .. }
            if message.contains("deletion vectors without merge-on-read")),
        "unexpected error: {err:?}"
    );

    // `AND(partition, data)` still has a data conjunct after the split, so it
    // must fail loud on the non-DV table just like the data-only filter.
    let pb = PredicateBuilder::new(table.schema().fields());
    let and_filter = Predicate::and(vec![
        pb.equal("dt", Datum::String("a".to_string())).unwrap(),
        pb.greater_than("id", Datum::Int(2)).unwrap(),
    ]);
    let err = async {
        table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0; 4])
            .with_limit(5)
            .with_filter(and_filter)
            .execute()
            .await?
            .new_read_builder()
            .read()
            .await
    }
    .await
    .map(|_| ())
    .expect_err("AND(partition, data) without deletion vectors must fail loud");
    assert!(
        matches!(err, crate::Error::DataInvalid { ref message, .. }
            if message.contains("deletion vectors without merge-on-read")),
        "unexpected error: {err:?}"
    );

    // A mixed `OR(partition, data)` conjunct is not partition-only, so it stays
    // whole as a data predicate and must also fail loud without deletion vectors.
    let or_filter = Predicate::or(vec![
        pb.equal("dt", Datum::String("a".to_string())).unwrap(),
        pb.greater_than("id", Datum::Int(2)).unwrap(),
    ]);
    let err = async {
        table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0; 4])
            .with_limit(5)
            .with_filter(or_filter)
            .execute()
            .await?
            .new_read_builder()
            .read()
            .await
    }
    .await
    .map(|_| ())
    .expect_err("mixed OR(partition, data) without deletion vectors must fail loud");
    assert!(
        matches!(err, crate::Error::DataInvalid { ref message, .. }
            if message.contains("deletion vectors without merge-on-read")),
        "unexpected error: {err:?}"
    );
}

#[tokio::test]
async fn result_read_de_table_empty_snapshot_yields_empty_stream() {
    // No pk-vector index configured and no snapshot: result_read routes to the
    // data-evolution path, whose search finds nothing and returns an empty
    // stream (not an error).
    let table = pk_vector_table(&[]);
    let mut stream = async {
        table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .execute()
            .await?
            .new_read_builder()
            .read()
            .await
    }
    .await
    .expect("DE read over an empty table must succeed with no rows");
    let mut rows = 0usize;
    while let Some(batch) = stream.try_next().await.unwrap() {
        rows += batch.num_rows();
    }
    assert_eq!(rows, 0, "empty DE table must yield no rows");
}

#[tokio::test]
async fn result_read_unknown_column_fails_loud() {
    // pk-vector index configured for "embedding", but the query targets a
    // column that does not exist. The read path must fail loud rather than
    // fall through to the data-evolution path and return an empty stream (a
    // typo must not look like a normal empty read through the C API).
    let table = pk_vector_table(&[
        ("pk-vector.index.columns", "embedding"),
        ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
        ("fields.embedding.pk-vector.distance.metric", "l2"),
    ]);
    let err = match async {
        table
            .new_vector_search_builder()
            .with_vector_column("other")
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .execute()
            .await?
            .new_read_builder()
            .read()
            .await
    }
    .await
    {
        Ok(_) => panic!("unknown vector column must fail loud on result_read"),
        Err(e) => e,
    };
    assert!(
        matches!(&err, crate::Error::DataInvalid { message, .. } if message.contains("does not exist")),
        "expected a does-not-exist error, got: {err}"
    );
}

#[tokio::test]
async fn result_read_scalar_column_fails_loud() {
    // A scalar (non-vector) column targeted by a vector read must fail loud,
    // not return an empty data-evolution stream.
    let table = pk_vector_table(&[
        ("pk-vector.index.columns", "embedding"),
        ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
        ("fields.embedding.pk-vector.distance.metric", "l2"),
    ]);
    let err = match async {
        table
            .new_vector_search_builder()
            .with_vector_column("id") // scalar Int column
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .execute()
            .await?
            .new_read_builder()
            .read()
            .await
    }
    .await
    {
        Ok(_) => panic!("scalar vector column must fail loud on result_read"),
        Err(e) => e,
    };
    assert!(
        matches!(&err, crate::Error::DataInvalid { message, .. } if message.contains("must be a FLOAT vector column")),
        "expected a not-a-vector-column error, got: {err}"
    );
}

#[tokio::test]
async fn result_read_non_float_vector_column_fails_loud() {
    // An ARRAY<INT> column is not a searchable vector column (the index/search
    // operates on FLOAT elements). It must fail loud rather than fall through
    // to the DE path and return an empty stream.
    use crate::spec::{ArrayType, IntType, Schema, TableSchema};
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "embedding",
            DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
        )
        .build()
        .unwrap();
    let table = Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("default", "de_non_float_vector"),
        "memory:/de_non_float_vector".to_string(),
        TableSchema::new(0, &schema),
        None,
    );
    let err = match async {
        table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .execute()
            .await?
            .new_read_builder()
            .read()
            .await
    }
    .await
    {
        Ok(_) => panic!("ARRAY<INT> vector column must fail loud on result_read"),
        Err(e) => e,
    };
    assert!(
        matches!(&err, crate::Error::DataInvalid { message, .. } if message.contains("must be a FLOAT vector column")),
        "expected a FLOAT-vector-column error, got: {err}"
    );
}

#[tokio::test]
async fn result_read_empty_plan_reserved_projection_fails_loud() {
    // Empty plan (no snapshot) must still fail loud on a reserved-name
    // projection: projection validity does not depend on whether the search
    // matched any rows. A regression that resolved the projection only after
    // the `candidates.is_empty()` early return would yield an empty stream here
    // instead of an error.
    let table = pk_vector_table(&[
        ("pk-vector.index.columns", "embedding"),
        ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
        ("fields.embedding.pk-vector.distance.metric", "l2"),
        // Pin the index dimension so the query vector below matches it; the
        // up-front dimension guard runs before this test's reserved-projection
        // guard, so a mismatched query would mask the error under test.
        ("fields.embedding.dimension", "4"),
    ]);
    for reserved in [
        ROW_ID_FIELD_NAME,
        PKEY_VECTOR_POSITION_COLUMN,
        SEARCH_SCORE_COLUMN,
    ] {
        let mut builder = table.new_vector_search_builder();
        builder
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0; 4])
            .with_limit(5);
        let err = async {
            builder
                .execute()
                .await?
                .new_read_builder()
                .with_projection(&["id", reserved])
                .read()
                .await
        }
        .await
        .map(|_| ())
        .expect_err("empty plan + reserved projection must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("reserved column")),
            "unexpected error for {reserved}: {err:?}"
        );
    }
}

#[tokio::test]
async fn result_read_empty_plan_lumina_array_float_is_admitted() {
    // A Lumina PK-vector `ARRAY<FLOAT>` column is a valid configuration, but
    // batch query dimension validation routed every `ARRAY<FLOAT>` column
    // through the vindex resolver, which rejects `lumina` as an unsupported
    // index type before planning — failing even an empty table. The
    // dimension must be resolved per the configured backend, so a
    // well-formed Lumina query is admitted and (with no snapshot) yields an
    // empty stream rather than an "Unsupported vindex index type" error.
    let table = pk_vector_table(&[
        ("pk-vector.index.columns", "embedding"),
        (
            "fields.embedding.pk-vector.index.type",
            crate::lumina::LUMINA_IDENTIFIER,
        ),
        ("fields.embedding.pk-vector.distance.metric", "l2"),
        ("lumina.index.dimension", "4"),
    ]);
    let mut stream = async {
        table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0; 4])
            .with_limit(5)
            .execute()
            .await?
            .new_read_builder()
            .read()
            .await
    }
    .await
    .expect("Lumina ARRAY<FLOAT> query must be admitted, not rejected as unsupported vindex");
    assert!(stream.try_next().await.unwrap().is_none());
}

#[tokio::test]
async fn result_read_projection_reserved_name_fails_loud() {
    // Projecting a reserved metadata / row-id column must fail loud. The guard
    // lives in `resolve_materialize_read_type`, which `SearchResultReadBuilder::read` invokes
    // before the empty-plan early return; assert on the resolver directly here.
    let table = pk_vector_table(&[
        ("pk-vector.index.columns", "embedding"),
        ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
        ("fields.embedding.pk-vector.distance.metric", "l2"),
    ]);
    for reserved in [
        ROW_ID_FIELD_NAME,
        PKEY_VECTOR_POSITION_COLUMN,
        SEARCH_SCORE_COLUMN,
    ] {
        let err =
            resolve_materialize_read_type(&table, Some(&["id".to_string(), reserved.to_string()]))
                .expect_err("reserved projection must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("reserved column")),
            "unexpected error for {reserved}: {err:?}"
        );
    }
}

#[test]
fn resolve_materialize_read_type_default_is_all_user_columns() {
    // No with_projection -> every user table column (id + embedding).
    let table = pk_vector_table(&[
        ("pk-vector.index.columns", "embedding"),
        ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
        ("fields.embedding.pk-vector.distance.metric", "l2"),
    ]);
    let fields = resolve_materialize_read_type(&table, None).unwrap();
    let names: Vec<&str> = fields.iter().map(|f| f.name()).collect();
    assert_eq!(names, vec!["id", "embedding"]);
}

#[test]
fn resolve_materialize_read_type_default_rejects_reserved_user_column() {
    // The default (all-columns) projection must reject a user column whose
    // name collides with an injected metadata column, not only columns named
    // in an explicit projection. Otherwise it silently passes on an empty
    // result and collides with the metadata columns the read attaches.
    let table = pk_vector_table_with_extra_column(SEARCH_SCORE_COLUMN);
    let err = resolve_materialize_read_type(&table, None).unwrap_err();
    assert!(
        matches!(err, crate::Error::DataInvalid { ref message, .. }
            if message.contains("reserved column")),
        "single-query default projection must reject reserved user column, got: {err:?}"
    );
}

#[test]
fn resolve_materialize_read_type_projection_selects_named_columns() {
    let table = pk_vector_table(&[
        ("pk-vector.index.columns", "embedding"),
        ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
        ("fields.embedding.pk-vector.distance.metric", "l2"),
    ]);
    let fields = resolve_materialize_read_type(&table, Some(&["id".to_string()])).unwrap();
    let names: Vec<&str> = fields.iter().map(|f| f.name()).collect();
    assert_eq!(names, vec!["id"]);
}
