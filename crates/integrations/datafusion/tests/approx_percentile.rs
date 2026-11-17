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

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Float16Array, Int64Array};
use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Float16Type, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::datasource::MemTable;
use paimon_datafusion::SQLContext;

async fn value(ctx: &SQLContext, sql: &str) -> ScalarValue {
    let batches = Box::pin(ctx.sql(sql))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    ScalarValue::try_from_array(batches[0].column(0), 0).unwrap()
}

async fn assert_error(ctx: &SQLContext, sql: &str, expected: &str) {
    let error = match Box::pin(ctx.sql(sql)).await {
        Err(error) => error,
        Ok(frame) => frame.collect().await.unwrap_err(),
    };
    assert!(error.to_string().contains(expected), "{sql}: {error}");
}

fn ints(values: &[i64]) -> ScalarValue {
    ScalarValue::List(ScalarValue::new_list(
        &values
            .iter()
            .map(|v| ScalarValue::Int64(Some(*v)))
            .collect::<Vec<_>>(),
        &DataType::Int64,
        false,
    ))
}

#[tokio::test]
async fn spark_examples_preserve_discrete_results_array_order_and_input_type() {
    let ctx = SQLContext::new();
    assert_eq!(value(&ctx, "SELECT approx_percentile(v, array(0.5, 0.4, 0.1), 100) FROM (VALUES (0), (1), (2), (10)) t(v)").await, ints(&[1, 1, 0]));
    assert_eq!(value(&ctx, "SELECT percentile_approx(v, array(1.0, 0.0, 0.5, 0.5)) FROM (VALUES (0), (1), (2), (10)) t(v)").await, ints(&[10, 0, 1, 1]));
    assert_eq!(
        value(
            &ctx,
            "SELECT approx_percentile(v, 0.5) FROM (VALUES (0), (10)) t(v)"
        )
        .await,
        ScalarValue::Int64(Some(0))
    );
    assert_eq!(
        value(
            &ctx,
            "SELECT approx_percentile(v, 0.5, 100) FROM (VALUES (0), (6), (7), (9), (10)) t(v)"
        )
        .await,
        ScalarValue::Int64(Some(7))
    );
    assert_eq!(value(&ctx, "SELECT approx_percentile(v, array(0.0, 0.5, 1.0), 1) FROM (VALUES (0), (6), (10)) t(v)").await, ints(&[0, 0, 0]));
    assert_eq!(
        value(
            &ctx,
            "SELECT approx_percentile(v, 1.0 / 2, 50 * 2) FROM (VALUES (0), (6), (10)) t(v)"
        )
        .await,
        ScalarValue::Int64(Some(6))
    );
    assert_eq!(
        value(
            &ctx,
            "SELECT approx_percentile(v, '0.5') FROM (VALUES (0), (6), (10)) t(v)"
        )
        .await,
        ScalarValue::Int64(Some(6))
    );
    assert_eq!(
        value(
            &ctx,
            "SELECT approx_percentile_cont(v, 0.5) FROM (VALUES (0), (10)) t(v)"
        )
        .await,
        ScalarValue::Float64(Some(5.0))
    );
}

#[tokio::test]
async fn percentiles_validate_foldable_arguments_even_for_empty_input() {
    let ctx = SQLContext::new();
    for percentage in [
        "NULL",
        "array(0.5, NULL)",
        "-0.1",
        "1.1",
        "array(0.5, 1.1)",
        "array('0.5','1')",
    ] {
        assert_error(
            &ctx,
            &format!(
                "SELECT approx_percentile(v, {percentage}) FROM (VALUES (1)) t(v) WHERE false"
            ),
            "percentage",
        )
        .await;
    }
    for accuracy in ["NULL", "0", "-1", "2147483648", "1.0"] {
        assert_error(
            &ctx,
            &format!(
                "SELECT approx_percentile(v, 0.5, {accuracy}) FROM (VALUES (1)) t(v) WHERE false"
            ),
            "accuracy",
        )
        .await;
    }
    assert_error(
        &ctx,
        "SELECT approx_percentile(v, p) FROM (VALUES (1, 0.5), (2, 0.6)) t(v, p)",
        "constant",
    )
    .await;
    assert_error(
        &ctx,
        "SELECT approx_percentile(v, 0.5, a) FROM (VALUES (1, 100), (2, 200)) t(v, a)",
        "constant",
    )
    .await;
    assert_error(
        &ctx,
        "SELECT approx_percentile(v, random()) FROM (VALUES (1)) t(v)",
        "constant",
    )
    .await;
    assert_error(
        &ctx,
        "SELECT approx_percentile(v, CAST('NaN' AS DOUBLE)) FROM (VALUES (1)) t(v)",
        "percentage",
    )
    .await;
    assert_eq!(value(&ctx, "SELECT approx_percentile(v, CAST('NaN' AS DOUBLE)) IS NULL FROM (VALUES (1)) t(v) WHERE false").await, ScalarValue::Boolean(Some(true)));
}

#[tokio::test]
async fn invalid_parameters_are_rejected_before_limit_zero_eliminates_execution() {
    let ctx = SQLContext::new();
    for call in [
        "approx_percentile(v, -0.1)",
        "approx_percentile(v, 0.5, 0)",
        "approx_percentile(v, -0.1) OVER ()",
        "approx_percentile(v, 0.5, 0) OVER ()",
    ] {
        assert_error(
            &ctx,
            &format!("SELECT {call} FROM (VALUES (1)) t(v) LIMIT 0"),
            "approx_percentile",
        )
        .await;
    }
}

#[tokio::test]
async fn decimal_percentages_use_correctly_rounded_double_ranks() {
    let ctx = SQLContext::new();
    let rows = "(VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) t(v)";
    assert_eq!(
        value(
            &ctx,
            &format!("SELECT approx_percentile(v, CAST('0.3' AS DECIMAL(38,38))) FROM {rows}")
        )
        .await,
        ScalarValue::Int64(Some(2))
    );
    assert_eq!(value(&ctx, &format!("SELECT approx_percentile(v, array(CAST('0.3' AS DECIMAL(38,38)),CAST('0.7' AS DECIMAL(38,38)))) FROM {rows}")).await, ints(&[2,6]));
}

#[tokio::test]
async fn ansi_intervals_and_submicrosecond_timestamps_follow_spark_units() {
    let ctx = SQLContext::new();
    assert_eq!(value(&ctx, "SELECT approx_percentile(v, 0.5, 100) FROM (VALUES (INTERVAL '0' MONTH), (INTERVAL '1' MONTH), (INTERVAL '2' MONTH), (INTERVAL '10' MONTH)) t(v)").await, value(&ctx, "SELECT INTERVAL '1' MONTH").await);
    assert_eq!(value(&ctx, "SELECT approx_percentile(v, 0.5, 100) FROM (VALUES (INTERVAL '0' SECOND), (INTERVAL '1' SECOND), (INTERVAL '2' SECOND), (INTERVAL '10' SECOND)) t(v)").await, value(&ctx, "SELECT INTERVAL '1' SECOND").await);
    assert_eq!(
        value(
            &ctx,
            "SELECT approx_percentile(v, 0.5, 2147483647) FROM (VALUES (1), (2), (3)) t(v)"
        )
        .await,
        ScalarValue::Int64(Some(2))
    );
    assert_eq!(value(&ctx, "SELECT approx_percentile(v, 0.5) FROM (VALUES (TIMESTAMP '1969-12-31 23:59:59.999999999')) t(v)").await, value(&ctx, "SELECT TIMESTAMP '1969-12-31 23:59:59.999999'").await);
    assert_eq!(value(&ctx, "SELECT approx_percentile(DISTINCT v, 0.5) FROM (VALUES (TIMESTAMP '2024-01-01 00:00:00'), (TIMESTAMP '2024-01-01 00:00:00.000000001'), (TIMESTAMP '2024-01-01 00:00:00.000002'), (TIMESTAMP '2024-01-01 00:00:00.000003')) t(v)").await, value(&ctx, "SELECT TIMESTAMP '2024-01-01 00:00:00.000002'").await);
}

#[tokio::test]
async fn negative_submicrosecond_intervals_truncate_toward_zero() {
    let ctx = SQLContext::new();
    for (input, expected) in [("-0.000000001", "0"), ("-0.000001001", "-0.000001")] {
        let expected = value(&ctx, &format!("SELECT INTERVAL '{expected}' SECOND")).await;
        for call in [
            "approx_percentile(v, 0.5)",
            "approx_percentile(DISTINCT v, 0.5)",
            "approx_percentile(v, 0.5) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
        ] {
            let sql = format!("SELECT {call} FROM (VALUES (INTERVAL '{input}' SECOND)) t(v)");
            assert_eq!(value(&ctx, &sql).await, expected, "{sql}");
        }
    }
}

#[tokio::test]
async fn null_empty_and_filter_contracts_follow_spark() {
    let ctx = SQLContext::new();
    for query in [
        "SELECT approx_percentile(v, 0.5) IS NULL FROM (VALUES (CAST(NULL AS BIGINT))) t(v)",
        "SELECT approx_percentile(v, array(0.1, 0.9)) IS NULL FROM (VALUES (1)) t(v) WHERE false",
        "SELECT approx_percentile(v, array()) IS NULL FROM (VALUES (1)) t(v)",
    ] {
        assert_eq!(value(&ctx, query).await, ScalarValue::Boolean(Some(true)));
    }
    assert_eq!(value(&ctx, "SELECT approx_percentile(v, 0.5) FILTER (WHERE v < 10) FROM (VALUES (0), (2), (10), (NULL)) t(v)").await, ScalarValue::Int64(Some(0)));
    let batches = ctx.sql("SELECT g, approx_percentile(v, array(0.5, 1.0)) FROM (VALUES ('a', 0), ('a', 2), ('a', NULL), ('b', NULL)) t(g,v) GROUP BY g ORDER BY g").await.unwrap().collect().await.unwrap();
    assert_eq!(
        ScalarValue::try_from_array(batches[0].column(1), 0).unwrap(),
        ints(&[0, 2])
    );
    assert!(ScalarValue::try_from_array(batches[0].column(1), 1)
        .unwrap()
        .is_null());
}

#[tokio::test]
async fn numeric_and_temporal_results_keep_the_input_type() {
    let ctx = SQLContext::new();
    for (ty, expected) in [
        ("TINYINT", ScalarValue::Int8(Some(1))),
        ("SMALLINT", ScalarValue::Int16(Some(1))),
        ("INT", ScalarValue::Int32(Some(1))),
        ("BIGINT", ScalarValue::Int64(Some(1))),
        ("FLOAT", ScalarValue::Float32(Some(1.0))),
        ("DOUBLE", ScalarValue::Float64(Some(1.0))),
        ("DECIMAL(10,2)", ScalarValue::Decimal128(Some(100), 10, 2)),
    ] {
        assert_eq!(
            value(
                &ctx,
                &format!(
                    "SELECT approx_percentile(CAST(v AS {ty}), 0.5) FROM (VALUES (1), (2)) t(v)"
                )
            )
            .await,
            expected,
            "{ty}"
        );
    }
    assert_eq!(value(&ctx, "SELECT approx_percentile(v, 0.5) FROM (VALUES (DATE '2024-01-01'), (DATE '2024-01-03')) t(v)").await, value(&ctx, "SELECT DATE '2024-01-01'").await);
    assert_eq!(value(&ctx, "SELECT approx_percentile(v, 0.5) FROM (VALUES (TIMESTAMP '2024-01-01 01:02:03'), (TIMESTAMP '2024-01-03 01:02:03')) t(v)").await, value(&ctx, "SELECT TIMESTAMP '2024-01-01 01:02:03'").await);
    for fraction in ["000001", "123457"] {
        let timestamp = format!("TIMESTAMP '2024-01-01 01:02:03.{fraction}'");
        assert_eq!(
            value(
                &ctx,
                &format!("SELECT approx_percentile(v, 0.5) FROM (VALUES ({timestamp})) t(v)")
            )
            .await,
            value(&ctx, &format!("SELECT {timestamp}")).await
        );
    }
    assert_eq!(
        value(
            &ctx,
            "SELECT approx_percentile(v, 0.5) FROM (VALUES (9223372036854775807)) t(v)"
        )
        .await,
        ScalarValue::Int64(Some(i64::MAX))
    );
}

#[tokio::test]
async fn decimal_results_match_sparks_decimal_from_double_and_overflow_to_null() {
    let ctx = SQLContext::new();
    for (input, expected) in [
        ("123456789012345.67", "123456789012345.67"),
        ("90071992547409.93", "90071992547409.94"),
    ] {
        for modifier in ["", "DISTINCT "] {
            assert_eq!(
                value(
                    &ctx,
                    &format!(
                        "SELECT approx_percentile({modifier}CAST('{input}' AS DECIMAL(17,2)), 0.5)"
                    )
                )
                .await,
                value(&ctx, &format!("SELECT CAST('{expected}' AS DECIMAL(17,2))")).await,
            );
        }
    }
    for (input, expected) in [
        (
            "12345678901234567890.123456789012345678",
            "12345678901234567000.000000000000000000",
        ),
        ("0.123456789012345678", "0.123456789012345680"),
    ] {
        assert_eq!(
            value(
                &ctx,
                &format!("SELECT approx_percentile(CAST('{input}' AS DECIMAL(38,18)), 0.5)")
            )
            .await,
            value(
                &ctx,
                &format!("SELECT CAST('{expected}' AS DECIMAL(38,18))")
            )
            .await,
        );
    }
    for (input, ty) in [
        ("99999999999999999999999999999999999999", "DECIMAL(38,0)"),
        ("99999999999999999999.999999999999999999", "DECIMAL(38,18)"),
    ] {
        assert!(value(
            &ctx,
            &format!("SELECT approx_percentile(CAST('{input}' AS {ty}), 0.5)")
        )
        .await
        .is_null());
        let result = value(
            &ctx,
            &format!("SELECT approx_percentile(CAST('{input}' AS {ty}), array(0.5, 1.0))"),
        )
        .await;
        let ScalarValue::List(list) = result else {
            panic!("expected array");
        };
        assert_eq!(list.value(0).null_count(), 2);
    }
}

#[tokio::test]
async fn partial_merge_distinct_and_windows_keep_the_same_contract() {
    let ctx = SQLContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let partitions = [vec![0, 1, 1, 1], vec![1, 1, 2, 10]]
        .into_iter()
        .map(|rows| {
            vec![RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(rows)) as ArrayRef],
            )
            .unwrap()]
        })
        .collect();
    ctx.ctx()
        .register_table(
            "t",
            Arc::new(MemTable::try_new(schema, partitions).unwrap()),
        )
        .unwrap();
    assert_eq!(
        value(
            &ctx,
            "SELECT approx_percentile(v, array(0.0, 0.5, 1.0)) FROM t"
        )
        .await,
        ints(&[0, 1, 10])
    );
    assert_eq!(
        value(
            &ctx,
            "SELECT approx_percentile(DISTINCT v, array(0.5, 0.9)) FROM t"
        )
        .await,
        ints(&[1, 10])
    );
    assert_eq!(value(&ctx, "SELECT approx_percentile(DISTINCT v, 0.5) FROM (VALUES (CAST('-0.0' AS DOUBLE)), (0.0), (1.0), (2.0)) t(v)").await, ScalarValue::Float64(Some(1.0)));
    assert_eq!(value(&ctx, "SELECT approx_percentile(DISTINCT v, 0.5) FROM (VALUES (INTERVAL '1' DAY), (INTERVAL '24' HOUR), (INTERVAL '2' DAY), (INTERVAL '3' DAY)) t(v)").await, value(&ctx, "SELECT INTERVAL '2' DAY").await);
    let batches = ctx.sql("SELECT v, approx_percentile(v, 0.5) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS p FROM (VALUES (0), (1), (2), (10)) t(v) ORDER BY v").await.unwrap().collect().await.unwrap();
    let result = batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|i| ScalarValue::try_from_array(batch.column(1), i).unwrap())
        })
        .collect::<Vec<_>>();
    assert_eq!(result, [0, 0, 1, 1].map(|v| ScalarValue::Int64(Some(v))));
}

#[tokio::test]
async fn distinct_float16_normalizes_signed_zero_and_nan_payloads() {
    type Float16 = <Float16Type as ArrowPrimitiveType>::Native;

    let ctx = SQLContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("zeros", DataType::Float16, false),
        Field::new("nans", DataType::Float16, false),
    ]));
    let partitions = [
        (
            vec![-Float16::ZERO, Float16::ONE, Float16::ONE],
            vec![Float16::ZERO, Float16::ONE, Float16::NAN],
        ),
        (
            vec![
                Float16::ZERO,
                Float16::from_f32(2.0),
                Float16::from_f32(2.0),
            ],
            vec![
                Float16::from_f32(2.0),
                Float16::from_bits(0x7e01),
                Float16::from_f32(2.0),
            ],
        ),
    ]
    .into_iter()
    .map(|(zeros, nans)| {
        vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float16Array::from(zeros)) as ArrayRef,
                Arc::new(Float16Array::from(nans)) as ArrayRef,
            ],
        )
        .unwrap()]
    })
    .collect();
    ctx.ctx()
        .register_table(
            "half_values",
            Arc::new(MemTable::try_new(schema, partitions).unwrap()),
        )
        .unwrap();
    for column in ["zeros", "nans"] {
        assert_eq!(
            value(
                &ctx,
                &format!("SELECT approx_percentile(DISTINCT {column}, 0.5) FROM half_values"),
            )
            .await,
            ScalarValue::Float16(Some(Float16::ONE)),
            "{column}",
        );
    }
}
