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

use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use datafusion::datasource::MemTable;
use paimon_datafusion::SQLContext;

async fn rows(ctx: &SQLContext, sql: &str) -> Vec<Vec<String>> {
    ctx.sql(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .collect()
        .await
        .unwrap()
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        ScalarValue::try_from_array(column, row)
                            .unwrap()
                            .to_string()
                    })
                    .collect()
            })
        })
        .collect()
}

#[tokio::test]
async fn sort_by_sorts_each_partition_without_merging() {
    let ctx = SQLContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
    let batch = |values: Vec<Option<i32>>| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(values))],
        )
        .unwrap()
    };
    let table = MemTable::try_new(
        schema.clone(),
        vec![
            vec![batch(vec![Some(8), None]), batch(vec![Some(2), Some(6)])],
            vec![batch(vec![Some(9), Some(1)]), batch(vec![Some(7), Some(3)])],
        ],
    )
    .unwrap();
    ctx.ctx()
        .register_table("numbers", Arc::new(table))
        .unwrap();
    let partitions = ctx
        .sql("SELECT v FROM numbers SORT BY v ASC NULLS FIRST")
        .await
        .unwrap()
        .collect_partitioned()
        .await
        .unwrap();
    let values = partitions
        .iter()
        .map(|batches| {
            batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .iter()
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        values,
        vec![
            vec![None, Some(2), Some(6), Some(8)],
            vec![Some(1), Some(3), Some(7), Some(9)]
        ]
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT v FROM numbers ORDER BY v DESC NULLS LAST LIMIT 3"
        )
        .await,
        vec![vec!["9"], vec!["8"], vec!["7"]]
    );
}

#[tokio::test]
async fn sort_by_handles_nested_queries_aliases_hidden_keys_and_limit() {
    let ctx = SQLContext::new();
    assert_eq!(
        rows(
            &ctx,
            "SELECT v AS x FROM (VALUES (3,1),(1,3),(2,2)) t(v,k) SORT BY k DESC LIMIT 2"
        )
        .await,
        vec![vec!["1"], vec!["2"]]
    );
    assert_eq!(
        rows(&ctx, "WITH t AS (SELECT v FROM (VALUES (3),(1),(2)) t(v) SORT BY v DESC) SELECT v + 10 AS x FROM t SORT BY x").await,
        vec![vec!["11"], vec!["12"], vec!["13"]]
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT v IS NULL AS missing FROM (VALUES (1),(NULL)) t(v) SORT BY v"
        )
        .await,
        vec![vec!["true"], vec!["false"]]
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT v IS NULL AS missing FROM (VALUES (NULL),(1)) t(v) SORT BY v DESC"
        )
        .await,
        vec![vec!["false"], vec!["true"]]
    );
    common::assert_sql_error(
        &ctx,
        "SELECT v FROM (VALUES (1)) t(v) SORT BY v ORDER BY v",
        "ORDER BY",
    )
    .await;
}

#[tokio::test]
async fn group_concat_preserves_distinct_order_separator_and_nulls() {
    let ctx = SQLContext::new();
    assert_eq!(
        rows(&ctx, "SELECT group_concat(DISTINCT v ORDER BY v DESC SEPARATOR '|') AS joined FROM (VALUES ('b'),('a'),('b'),(NULL),('')) t(v)").await,
        vec![vec!["b|a|"]]
    );
    assert_eq!(
        rows(&ctx, "SELECT group_concat(v ORDER BY k SEPARATOR '') AS joined FROM (VALUES ('a',2),('b',1),(NULL,3)) t(v,k)").await,
        vec![vec!["ba"]]
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT group_concat(v ORDER BY v) FROM (VALUES (2),(1),(NULL)) t(v)"
        )
        .await,
        vec![vec!["1,2"]]
    );
    assert_eq!(
        rows(&ctx, "SELECT group_concat(v SEPARATOR '|') IS NULL FROM (VALUES (CAST(NULL AS STRING))) t(v)").await,
        vec![vec!["true"]]
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT group_concat(v SEPARATOR '|') IS NULL FROM (VALUES ('x')) t(v) WHERE false"
        )
        .await,
        vec![vec!["true"]]
    );
    assert_eq!(
        rows(&ctx, "SELECT group_concat(DISTINCT v ORDER BY v SEPARATOR '|') FROM (VALUES (10),(2),(10),(NULL)) t(v)").await,
        vec![vec!["2|10"]]
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT group_concat(v SEPARATOR '|') = '' FROM (VALUES (''),(NULL)) t(v)"
        )
        .await,
        vec![vec!["true"]]
    );
}

#[tokio::test]
async fn group_concat_concatenates_row_arguments_and_preserves_filter() {
    let ctx = SQLContext::new();
    assert_eq!(
        rows(&ctx, "SELECT group_concat(a,b ORDER BY k SEPARATOR ';') FILTER (WHERE k < 4) AS joined FROM (VALUES ('a',1,2),('b',2,1),(NULL,3,3),('c',NULL,0),('d',4,4)) t(a,b,k)").await,
        vec![vec!["b2;a1"]]
    );
    common::assert_sql_error(&ctx, "SELECT group_concat('x' SEPARATOR NULL)", "SEPARATOR").await;
    common::assert_sql_error(
        &ctx,
        "SELECT group_concat(DISTINCT a,b SEPARATOR '|') FROM (VALUES ('a','bc'),('ab','c')) t(a,b)",
        "DISTINCT supports one expression",
    ).await;
}

#[tokio::test]
async fn show_create_temporary_view_returns_recreatable_definition() {
    let (_tmp, ctx) = common::setup_sql_context().await;
    ctx.sql("CREATE TEMPORARY VIEW paimon.test_db.\"view.dot\" AS SELECT 7 AS \"answer value\"")
        .await
        .unwrap();
    let shown = rows(&ctx, "SHOW CREATE VIEW paimon.test_db.\"view.dot\"").await;
    assert_eq!(shown.len(), 1);
    assert_eq!(&shown[0][..3], &["paimon", "test_db", "view.dot"]);
    let definition = &shown[0][3];
    assert!(
        definition.starts_with("CREATE TEMPORARY VIEW "),
        "{definition}"
    );
    assert!(definition.contains("\"answer value\""), "{definition}");
    assert!(
        definition.contains("SELECT 7 AS \"answer value\""),
        "{definition}"
    );
    ctx.sql("DROP TEMPORARY VIEW paimon.test_db.\"view.dot\"")
        .await
        .unwrap();
    ctx.sql(definition).await.unwrap();
    assert_eq!(
        rows(
            &ctx,
            "SELECT \"answer value\" FROM paimon.test_db.\"view.dot\""
        )
        .await,
        vec![vec!["7"]]
    );
}

#[tokio::test]
async fn show_create_view_rejects_temporary_tables() {
    let (_tmp, ctx) = common::setup_sql_context().await;
    ctx.sql("CREATE TEMPORARY TABLE paimon.test_db.v (id INT)")
        .await
        .unwrap();
    common::assert_sql_error(&ctx, "SHOW CREATE VIEW paimon.test_db.v", "not a view").await;
}

#[tokio::test]
async fn show_create_view_supports_native_datafusion_views() {
    let ctx = SQLContext::new();
    ctx.sql("CREATE VIEW native_view AS SELECT 7 AS id")
        .await
        .unwrap();
    let shown = rows(&ctx, "SHOW CREATE VIEW native_view").await;
    assert_eq!(&shown[0][..3], &["datafusion", "public", "native_view"]);
    assert!(shown[0][3].contains("AS SELECT 7 AS id"), "{}", shown[0][3]);
    ctx.sql("DROP VIEW native_view").await.unwrap();
    ctx.sql(&shown[0][3]).await.unwrap();
    assert_eq!(
        rows(&ctx, "SELECT id FROM native_view").await,
        vec![vec!["7"]]
    );
}

#[tokio::test]
async fn show_create_native_view_preserves_creation_namespace() {
    let ctx = SQLContext::new();
    ctx.ctx().sql("CREATE SCHEMA other").await.unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    for (name, id) in [
        ("datafusion.public.source", 1),
        ("datafusion.other.source", 2),
    ] {
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![id]))],
        )
        .unwrap();
        ctx.ctx().register_batch(name, batch).unwrap();
    }

    // The destination schema can differ from the namespace that binds the query.
    for destination in ["public", "other"] {
        ctx.sql("SET datafusion.catalog.default_schema = 'public'")
            .await
            .unwrap();
        let view = format!("datafusion.{destination}.bound_view");
        ctx.sql(&format!("CREATE VIEW {view} AS SELECT id FROM source"))
            .await
            .unwrap();
        ctx.sql("SET datafusion.catalog.default_schema = 'other'")
            .await
            .unwrap();
        assert_eq!(
            rows(&ctx, &format!("SELECT id FROM {view}")).await,
            vec![vec!["1"]]
        );

        let shown = rows(&ctx, &format!("SHOW CREATE VIEW {view}")).await;
        let definition = &shown[0][3];
        assert!(
            definition.contains("FROM \"datafusion\".\"public\".\"source\""),
            "{definition}"
        );
        ctx.sql(&format!("DROP VIEW {view}")).await.unwrap();
        ctx.sql(definition).await.unwrap();
        assert_eq!(
            rows(&ctx, &format!("SELECT id FROM {view}")).await,
            vec![vec!["1"]]
        );
    }
}

#[tokio::test]
async fn sort_by_limit_all_preserves_sort() {
    let ctx = SQLContext::new();
    assert_eq!(
        rows(
            &ctx,
            "SELECT v FROM (VALUES (3),(1),(2)) t(v) SORT BY v LIMIT ALL"
        )
        .await,
        vec![vec!["1"], vec!["2"], vec!["3"]]
    );
}

#[tokio::test]
async fn sort_by_preserves_pipe_limit_and_projection() {
    let ctx = SQLContext::new();
    assert_eq!(
        rows(
            &ctx,
            "SELECT v FROM (VALUES (3),(1),(2)) t(v) SORT BY v |> LIMIT 1"
        )
        .await,
        vec![vec!["1"]]
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT v FROM (VALUES (3),(1),(2)) t(v) SORT BY v |> SELECT v + 10 AS x"
        )
        .await,
        vec![vec!["11"], vec!["12"], vec!["13"]]
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT v FROM (VALUES (3),(1),(2)) t(v) SORT BY v DESC |> ORDER BY v ASC"
        )
        .await,
        vec![vec!["1"], vec!["2"], vec!["3"]]
    );
}

#[tokio::test]
async fn sort_by_distinct_on_reports_user_error() {
    let ctx = SQLContext::new();
    let error = ctx
        .sql("SELECT DISTINCT ON (v) v FROM (VALUES (3),(1),(2)) t(v) SORT BY v")
        .await
        .unwrap_err();
    assert!(
        matches!(error, datafusion::common::DataFusionError::Plan(_)),
        "{error}"
    );
}

#[tokio::test]
async fn sort_by_orders_all_union_partitions() {
    let ctx = SQLContext::new();
    let partitions = ctx.sql("SELECT v FROM (VALUES (3),(1)) t(v) UNION ALL SELECT v FROM (VALUES (2),(0)) u(v) SORT BY v")
        .await.unwrap().collect_partitioned().await.unwrap();
    let mut all = Vec::new();
    for batches in partitions {
        let values = batches
            .iter()
            .flat_map(|batch| {
                (0..batch.num_rows()).map(|row| {
                    match ScalarValue::try_from_array(batch.column(0), row).unwrap() {
                        ScalarValue::Int64(Some(value)) => value,
                        value => panic!("unexpected value: {value:?}"),
                    }
                })
            })
            .collect::<Vec<_>>();
        let mut sorted = values.clone();
        sorted.sort();
        assert_eq!(values, sorted);
        all.extend(values);
    }
    all.sort();
    assert_eq!(all, vec![0, 1, 2, 3]);
}

#[tokio::test]
async fn group_concat_order_position_refers_to_aggregate_arguments() {
    let ctx = SQLContext::new();
    assert_eq!(
        rows(
            &ctx,
            "SELECT group_concat(v ORDER BY 1 SEPARATOR '|') FROM (VALUES (1,'b'),(2,'a')) t(k,v)"
        )
        .await,
        vec![vec!["a|b"]]
    );
    assert_eq!(
        rows(&ctx, "SELECT group_concat(7 ORDER BY 1 SEPARATOR '|')").await,
        vec![vec!["7"]]
    );
    assert_eq!(rows(&ctx, "SELECT group_concat(a,b ORDER BY 2 SEPARATOR ';') FROM (VALUES ('a',2),('b',1)) t(a,b)").await,
        vec![vec!["b1;a2"]]);
    for position in [0, 2] {
        common::assert_sql_error(
            &ctx,
            &format!("SELECT group_concat('x' ORDER BY {position} SEPARATOR '|')"),
            "position must be between",
        )
        .await;
    }
}

#[tokio::test]
async fn group_concat_evaluates_volatile_row_arguments_once() {
    use datafusion::logical_expr::{create_udf, ColumnarValue, Volatility};
    use std::sync::atomic::{AtomicUsize, Ordering};
    let ctx = SQLContext::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    ctx.ctx().register_udf(create_udf(
        "probe_value",
        vec![DataType::Int64],
        DataType::Utf8,
        Volatility::Volatile,
        Arc::new(move |_| {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                (observed.fetch_add(1, Ordering::SeqCst) == 0).then(|| "x".to_string()),
            )))
        }),
    ));
    for expression in [
        "group_concat(probe_value(1), 'z' SEPARATOR '|')",
        "group_concat(probe_value(1), 'z' ORDER BY 2 SEPARATOR '|')",
        "group_concat(probe_value(1) SEPARATOR '|')",
        "group_concat(probe_value(1) SEPARATOR '|') OVER ()",
    ] {
        calls.store(0, Ordering::SeqCst);
        let expected = if expression.contains("'z'") {
            "xz"
        } else {
            "x"
        };
        assert_eq!(
            rows(&ctx, &format!("SELECT {expression} AS joined")).await,
            vec![vec![expected]],
            "{expression}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1, "{expression}");
    }
    calls.store(0, Ordering::SeqCst);
    for expression in [
        "group_concat(probe_value(1), 'z' ORDER BY 1 SEPARATOR '|')",
        "group_concat(probe_value(1) ORDER BY 1 SEPARATOR '|')",
    ] {
        common::assert_sql_error(&ctx, &format!("SELECT {expression}"), "volatile").await;
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(rows(&ctx,
        "SELECT group_concat(v, 'z' ORDER BY 1 SEPARATOR '|') FROM (SELECT probe_value(1) AS v) t").await,
        vec![vec!["xz"]]);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn group_concat_formats_single_and_multiple_arguments_consistently() {
    let ctx = SQLContext::new();
    for value in [
        "CAST(5 AS DOUBLE)",
        "CAST('-0.0' AS DOUBLE)",
        "CAST(1.25 AS DOUBLE)",
    ] {
        let result = rows(&ctx, &format!("SELECT group_concat(v SEPARATOR '|'), group_concat(v, '' SEPARATOR '|') FROM (VALUES ({value})) t(v)")).await;
        assert_eq!(result[0][0], result[0][1], "{value}: {result:?}");
    }
}

#[tokio::test]
async fn sort_by_preserves_ctes_in_pipe_operators() {
    let ctx = SQLContext::new();
    assert_eq!(rows(&ctx, "WITH t AS (SELECT 1 AS v), u AS (SELECT 2 AS w) SELECT v FROM t SORT BY v |> JOIN u ON true").await,
        vec![vec!["1", "2"]]);
}
