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

use datafusion::common::ScalarValue;
use paimon_datafusion::SQLContext;

async fn assert_true(ctx: &SQLContext, sql: &str) {
    let batches = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut seen = 0;
    for batch in batches {
        for row in 0..batch.num_rows() {
            seen += 1;
            for col in 0..batch.num_columns() {
                assert_eq!(
                    ScalarValue::try_from_array(batch.column(col), row).unwrap(),
                    ScalarValue::Boolean(Some(true)),
                    "{sql}, row {row}, column {col}"
                );
            }
        }
    }
    assert_eq!(seen, 1, "{sql}");
}

#[tokio::test]
async fn spark_function_families_are_available_without_a_catalog() {
    let ctx = SQLContext::new();
    assert_true(&ctx, "SELECT pmod(-5,3)=1, factorial(5)=120, soundex('Robert')='R163', last_day(DATE '2024-02-01')=DATE '2024-02-29', unix_date(DATE '1970-01-02')=1, crc32('abc')=891568578, sha1('abc')='a9993e364706816aba3e25717850c26c9cd0d89d', base64('f')='Zg=='").await;
    assert_true(
        &ctx,
        "SELECT array_length(collect_set(v))=2,try_sum(v)=4 FROM (VALUES (1),(1),(2),(NULL)) t(v)",
    )
    .await;
}

#[tokio::test]
async fn spark_date_part_preserves_epoch_types_and_fractional_arithmetic() {
    use datafusion::arrow::datatypes::DataType;

    let ctx = SQLContext::new();
    assert_true(
        &ctx,
        "SELECT EXTRACT(EPOCH FROM TIMESTAMP '1970-01-01 00:00:01.500') / 2 = 0.75",
    )
    .await;
    for expression in [
        "date_part('epoch', ts)",
        "datepart('EpOcH', ts)",
        "EXTRACT(EPOCH FROM ts)",
    ] {
        let sql = format!(
            "SELECT {expression} / 2 AS half FROM (VALUES \
             (TIMESTAMP '1970-01-01 00:00:01.500'), \
             (TIMESTAMP '1969-12-31 23:59:59.500'), \
             (CAST(NULL AS TIMESTAMP))) t(ts)"
        );
        let frame = ctx.sql(&sql).await.unwrap();
        assert_eq!(
            frame.schema().field(0).data_type(),
            &DataType::Float64,
            "{sql}"
        );
        let batches = frame.collect().await.unwrap();
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);
        for (row, expected) in [Some(0.75), Some(-0.25), None].into_iter().enumerate() {
            assert_eq!(
                ScalarValue::try_from_array(batch.column(0), row).unwrap(),
                ScalarValue::Float64(expected),
                "{sql}, row {row}"
            );
        }
    }
}

#[tokio::test]
async fn spark_date_part_preserves_nanosecond_types_and_arithmetic() {
    use datafusion::arrow::datatypes::DataType;

    let ctx = SQLContext::new();
    for expression in [
        "date_part('nanosecond', ts)",
        "datepart('NaNoSeCoNdS', ts)",
        "EXTRACT(NANOSECOND FROM ts)",
    ] {
        let sql = format!(
            "SELECT {expression} + CAST(1 AS INT) AS adjusted FROM (VALUES \
             (TIMESTAMP '1970-01-01 00:00:10.500'), \
             (CAST(NULL AS TIMESTAMP))) t(ts)"
        );
        let frame = ctx.sql(&sql).await.unwrap();
        assert_eq!(
            frame.schema().field(0).data_type(),
            &DataType::Int64,
            "{sql}"
        );
        let batches = frame.collect().await.unwrap();
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2);
        for (row, expected) in [Some(10_500_000_001), None].into_iter().enumerate() {
            assert_eq!(
                ScalarValue::try_from_array(batch.column(0), row).unwrap(),
                ScalarValue::Int64(expected),
                "{sql}, row {row}"
            );
        }
    }
}

#[tokio::test]
async fn spark_date_part_keeps_weekday_and_year_aliases() {
    let ctx = SQLContext::new();
    assert_true(
        &ctx,
        "SELECT date_part('dayofweek', DATE '1970-01-01') = 5, \
         EXTRACT(DOW FROM DATE '1970-01-01') = 5, \
         datepart('dayofweek_iso', DATE '1970-01-01') = 4, \
         date_part('yearofweek', DATE '2021-01-01') = 2020, \
         EXTRACT(YEAR FROM DATE '2021-01-01') = 2021",
    )
    .await;
}

#[tokio::test]
async fn upstream_aggregates_keep_distinct_null_empty_and_overflow_contracts() {
    let ctx = SQLContext::new();
    assert_true(&ctx,"SELECT array_length(collect_list(DISTINCT v))=2,array_length(collect_set(v))=2,try_sum(v)=4,avg(v)=CAST(4 AS DOUBLE)/3 FROM (VALUES (1),(1),(2),(NULL)) t(v)").await;
    assert_true(&ctx,"SELECT array_length(collect_list(v))=0,try_sum(v) IS NULL,avg(v) IS NULL FROM (VALUES (CAST(NULL AS BIGINT))) t(v) WHERE FALSE").await;
    assert_true(
        &ctx,
        "SELECT try_sum(v) IS NULL FROM (VALUES (9223372036854775807),(CAST(1 AS BIGINT))) t(v)",
    )
    .await;
    assert_true(
        &ctx,
        "SELECT try_sum(DISTINCT v)=3 FROM (VALUES (1),(1),(2)) t(v)",
    )
    .await;
    for sql in [
        "SELECT try_sum(DISTINCT v),try_sum(DISTINCT w) FROM (VALUES (1,10),(1,20),(2,20)) t(v,w)",
        "SELECT collect_list(v ORDER BY v DESC) FROM (VALUES (1),(2)) t(v)",
    ] {
        let result = ctx.sql(sql).await;
        let error = match result {
            Err(e) => e,
            Ok(df) => df.collect().await.unwrap_err(),
        };
        assert!(
            error.to_string().contains("not supported"),
            "{sql}: {error}"
        );
    }
}

#[tokio::test]
async fn spark_avg_honors_grouped_filters_including_null_predicates() {
    let ctx = SQLContext::new();
    assert_true(&ctx,"SELECT bool_and(result IS NOT DISTINCT FROM CASE g WHEN 1 THEN CAST(10 AS DOUBLE) ELSE CAST(50 AS DOUBLE) END) FROM (SELECT g,avg(v) FILTER(WHERE p) AS result FROM (VALUES (1,10,TRUE),(1,30,FALSE),(2,50,TRUE),(2,70,NULL)) t(g,v,p) GROUP BY g) a").await;
}

#[tokio::test]
async fn spark_aggregates_merge_filtered_batches_and_partial_bypass_states() {
    use datafusion::arrow::{
        array::{BooleanArray, Int64Array, RecordBatch},
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::datasource::MemTable;
    use std::sync::Arc;
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
        Field::new("p", DataType::Boolean, true),
    ]));
    let batch = |g: Vec<i64>, v: Vec<Option<i64>>, p: Vec<Option<bool>>| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(g)),
                Arc::new(Int64Array::from(v)),
                Arc::new(BooleanArray::from(p)),
            ],
        )
        .unwrap()
    };
    let partitions = vec![
        vec![
            batch(
                vec![1, 2],
                vec![Some(10), None],
                vec![Some(true), Some(true)],
            ),
            batch(
                vec![1, 3],
                vec![Some(30), Some(20)],
                vec![Some(false), Some(true)],
            ),
        ],
        vec![
            batch(
                vec![1, 2],
                vec![Some(14), Some(50)],
                vec![Some(true), Some(false)],
            ),
            batch(vec![3, 3], vec![Some(40), None], vec![Some(true), None]),
        ],
    ];
    let table = Arc::new(MemTable::try_new(schema, partitions).unwrap());
    for bypass in [false, true] {
        let ctx = SQLContext::new();
        ctx.ctx()
            .register_table("spark_inputs", table.clone())
            .unwrap();
        ctx.sql("SET datafusion.execution.target_partitions = 2")
            .await
            .unwrap();
        if bypass {
            ctx.sql(
                "SET datafusion.execution.skip_partial_aggregation_probe_ratio_threshold = 0.0",
            )
            .await
            .unwrap();
            ctx.sql("SET datafusion.execution.skip_partial_aggregation_probe_rows_threshold = 0")
                .await
                .unwrap();
        }
        assert_true(&ctx,"SELECT count(*)=3,bool_and(mean IS NOT DISTINCT FROM CASE g WHEN 1 THEN CAST(12 AS DOUBLE) WHEN 3 THEN CAST(30 AS DOUBLE) ELSE CAST(NULL AS DOUBLE) END) FROM (SELECT g,avg(v) FILTER(WHERE p) AS mean FROM spark_inputs GROUP BY g) a").await;
        assert_true(&ctx,"SELECT avg(v) FILTER(WHERE p)=21,try_sum(v) FILTER(WHERE p)=84,array_length(collect_list(v) FILTER(WHERE p))=4 FROM spark_inputs").await;
    }
}

#[tokio::test]
async fn unsupported_avg_suffix_frames_follow_upstream_limits() {
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    use datafusion_spark::SessionStateBuilderSpark;
    let native = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_spark_features()
            .build(),
    );
    let ctx = SQLContext::new();
    let sql="SELECT v,avg(v) OVER(ORDER BY v ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS mean FROM (SELECT v FROM (VALUES (1),(2)) t(v) ORDER BY v DESC) s";
    let native_error = native.sql(sql).await.unwrap().collect().await.unwrap_err();
    assert!(
        native_error.to_string().contains("retract_batch"),
        "{native_error}"
    );
    let error = ctx.sql(sql).await.unwrap().collect().await.unwrap_err();
    assert!(error.to_string().contains("retract_batch"), "{error}");
}

#[tokio::test]
async fn spark_collections_return_empty_arrays_for_empty_window_frames() {
    let ctx = SQLContext::new();
    assert_true(&ctx,"SELECT bool_and((items IS NOT DISTINCT FROM CASE v WHEN 1 THEN '[]' ELSE '[1]' END) AND (unique_items IS NOT DISTINCT FROM CASE v WHEN 1 THEN '[]' ELSE '[1]' END)) FROM (SELECT v,to_json(collect_list(v) OVER(ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS items,to_json(collect_set(v) OVER(ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS unique_items FROM (VALUES (1),(2)) t(v)) w").await;
}

#[tokio::test]
async fn spark_sum_window_defaults_keep_the_result_type() {
    let ctx = SQLContext::new();
    assert_true(&ctx,"SELECT bool_and(result IS NOT DISTINCT FROM CASE v WHEN 1 THEN CAST(NULL AS BIGINT) ELSE CAST(1 AS BIGINT) END) FROM (SELECT v,try_sum(v) OVER(ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS result FROM (VALUES (1),(2)) t(v)) w").await;
}

#[tokio::test]
async fn spark_date_overloads_and_session_overrides_remain_distinct() {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::{create_udf, ColumnarValue, Volatility};
    use std::sync::Arc;
    let ctx = SQLContext::new();
    assert_true(&ctx,"SELECT date_add('2024-02-28',1)=DATE '2024-02-29',dateadd('2024-02-28',1)=DATE '2024-02-29',date_sub('2024-03-01',1)=DATE '2024-02-29',date_diff(DATE '2024-03-01',DATE '2024-02-28')=2,datediff(DATE '2024-03-01',DATE '2024-02-28')=2,date_diff('hour',TIMESTAMP '2024-03-01 00:00:00',TIMESTAMP '2024-03-01 03:30:00')=3").await;
    ctx.ctx().register_udf(create_udf(
        "pmod",
        vec![DataType::Int64, DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(99))))),
    ));
    assert_true(&ctx, "SELECT pmod(-5,3)=99").await;
}

#[tokio::test]
async fn spark_semantics_replace_conflicting_native_defaults() {
    let ctx = SQLContext::new();
    assert_true(&ctx, "SELECT concat('a',CAST(NULL AS STRING)) IS NULL, mod(1,0) IS NULL, size(CAST(NULL AS INT[]))=-1 AS typed_size,size(NULL)=-1 AS bare_size, arrow_typeof(date_diff(DATE '2024-03-01',DATE '2024-02-28'))='Int32'").await;
    ctx.sql("SET datafusion.execution.enable_ansi_mode=true")
        .await
        .unwrap();
    let result = ctx.sql("SELECT mod(1,0)").await;
    let error = match result {
        Err(e) => e,
        Ok(df) => df.collect().await.unwrap_err(),
    };
    assert!(error.to_string().to_lowercase().contains("zero"), "{error}");
}

#[tokio::test]
async fn spark_substring_syntax_preserves_binary_values() {
    let ctx = SQLContext::new();
    assert_true(
        &ctx,
        "SELECT substring(decode('00ff01','hex') FROM 2 FOR 2)=decode('ff01','hex')",
    )
    .await;
}
