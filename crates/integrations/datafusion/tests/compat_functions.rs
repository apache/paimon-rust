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

async fn assert_true(ctx: &SQLContext, expressions: &[&str]) {
    for expression in expressions {
        let batches = ctx
            .sql(&format!("SELECT {expression}"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            ScalarValue::try_from_array(batches[0].column(0), 0).unwrap(),
            ScalarValue::Boolean(Some(true)),
            "{expression}"
        );
    }
}

async fn assert_error(ctx: &SQLContext, expression: &str, expected: &str) {
    let result = ctx.sql(&format!("SELECT {expression}")).await;
    let error = match result {
        Err(error) => error,
        Ok(frame) => frame.collect().await.unwrap_err(),
    };
    assert!(
        error.to_string().contains(expected),
        "{expression}: {error}"
    );
}

#[tokio::test]
async fn mod_preserves_numeric_remainder_and_null_semantics() {
    let ctx = SQLContext::new();
    assert_true(
        &ctx,
        &[
            "mod(5, 2) = 1",
            "mod(-5, 2) = -1",
            "mod(5, -2) = 1",
            "mod(5.5, 2.0) = 5.5 % 2.0",
            "mod(CAST(7 AS BIGINT), 3) = 1",
            "mod(CAST(NULL AS BIGINT), 2) IS NULL",
            "mod(NULL, 2) IS NULL",
            "mod(2, NULL) IS NULL",
        ],
    )
    .await;
    assert_true(&ctx, &["mod(1, 0) IS NULL"]).await;
}

#[tokio::test]
async fn mod_uses_upstream_spark_decimal_coercion() {
    let ctx = SQLContext::new();
    let batches = ctx
        .sql("SELECT mod(CAST(5.55 AS DECIMAL(10,2)),CAST(2.1 AS DECIMAL(4,1)))")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        ScalarValue::try_from_array(batches[0].column(0), 0).unwrap(),
        ScalarValue::Decimal128(Some(135), 10, 2)
    );
}

#[tokio::test]
async fn date_sub_null_days_propagate_through_date_coercion() {
    let ctx = SQLContext::new();
    assert_true(
        &ctx,
        &["date_sub('2024-03-01', CAST(NULL AS BIGINT)) IS NULL"],
    )
    .await;
    let batches = ctx
        .sql(
            "SELECT date_sub(d, days) FROM (VALUES \
        ('2024-03-01', CAST(NULL AS BIGINT)), ('2024-03-01', CAST(1 AS BIGINT))) AS t(d, days)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        ScalarValue::try_from_array(batches[0].column(0), 0).unwrap(),
        ScalarValue::Date32(None)
    );
    assert_eq!(
        ScalarValue::try_from_array(batches[0].column(0), 1).unwrap(),
        ScalarValue::Date32(Some(19782))
    );
}

#[tokio::test]
async fn regexp_extraction_handles_groups_no_match_unicode_and_nulls() {
    let ctx = SQLContext::new();
    assert_true(
        &ctx,
        &[
            "regexp_extract('a12b', '([0-9]+)') = '12'",
            "regexp_extract('a12b', '([0-9]+)', 0) = '12'",
            "regexp_extract('no match', '([0-9]+)') = ''",
            "regexp_extract('b', '(a)?b') = ''",
            "regexp_extract('你好42', '(你好)([0-9]+)', 1) = '你好'",
            "regexp_extract_all('a1 b2', '([a-z])([0-9])', 2) = ['1', '2']",
            "cardinality(regexp_extract_all('no match', '([0-9]+)')) = 0",
            "regexp_extract(NULL, '(x)') IS NULL",
            "regexp_extract_all('x', NULL) IS NULL",
            "regexp_extract('x', '(x)', NULL) IS NULL",
        ],
    )
    .await;
    assert_error(
        &ctx,
        "regexp_extract('x', '[')",
        "Invalid regular expression",
    )
    .await;
    assert_error(&ctx, "regexp_extract('x', '(x)', -1)", "non-negative").await;
    assert_error(&ctx, "regexp_extract_all('x', '(x)', 2)", "group index").await;
}

#[tokio::test]
async fn regexp_extraction_evaluates_dynamic_patterns_and_indices() {
    let ctx = SQLContext::new();
    let batches = ctx
        .sql(
            "SELECT regexp_extract(s, p, i) FROM (VALUES \
        ('a12', '([0-9]+)', 1), ('b34', '(b)(34)', 2), ('c56', '(c)', 0), \
        (NULL, '(x)', 1)) AS t(s, p, i)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    for (row, expected) in [Some("12"), Some("34"), Some("c"), None]
        .into_iter()
        .enumerate()
    {
        assert_eq!(
            ScalarValue::try_from_array(batches[0].column(0), row).unwrap(),
            ScalarValue::Utf8(expected.map(str::to_string))
        );
    }
}

#[tokio::test]
async fn date_sub_handles_leap_days_negative_days_and_overflow() {
    let ctx = SQLContext::new();
    assert_true(
        &ctx,
        &[
            "date_sub(DATE '2024-03-01', 1) = DATE '2024-02-29'",
            "date_sub('2024-03-01', -1) = DATE '2024-03-02'",
            "date_sub(DATE '2024-03-01', 0) = DATE '2024-03-01'",
            "date_sub(CAST(NULL AS DATE), 1) IS NULL",
        ],
    )
    .await;
    assert_error(
        &ctx,
        "date_sub(DATE '2024-03-01', 9223372036854775807)",
        "Int32",
    )
    .await;
}

#[tokio::test]
async fn sha2_and_base64_preserve_padding_binary_and_null_contracts() {
    let ctx = SQLContext::new();
    assert_true(
        &ctx,
        &[
            "sha2('abc', 256) = 'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad'",
            "sha2('abc', 0) = sha2('abc', 256)",
            "length(sha2('abc', 224)) = 56",
            "length(sha2('abc', 384)) = 96",
            "length(sha2('abc', 512)) = 128",
            "sha2('abc', 123) IS NULL",
            "sha2(NULL, 256) IS NULL",
            "sha2('abc', NULL) IS NULL",
            "sha2(decode('616263', 'hex'), 256) = sha2('abc', 256)",
            "to_base64('f') = 'Zg=='",
            "to_base64('fo') = 'Zm8='",
            "to_base64('foo') = 'Zm9v'",
            "to_base64('') = ''",
            "to_base64(NULL) IS NULL",
            "to_base64(decode('00ff', 'hex')) = 'AP8='",
        ],
    )
    .await;
}

#[tokio::test]
async fn binary_encoding_preserves_non_utf8_bytes_across_arrow_types() {
    let ctx = SQLContext::new();
    assert_true(&ctx, &[
        "sha2(decode('00ff', 'hex'), CAST(256 AS INT)) = sha2(decode('00ff', 'hex'), 256)",
        "to_base64(arrow_cast(decode('00ff', 'hex'), 'LargeBinary')) = 'AP8='",
        "to_base64(arrow_cast(decode('00ff', 'hex'), 'BinaryView')) = 'AP8='",
        "to_base64(arrow_cast(decode('00ff', 'hex'), 'FixedSizeBinary(2)')) = 'AP8='",
        "sha2(arrow_cast(decode('00ff', 'hex'), 'LargeBinary'), 256) = sha2(decode('00ff', 'hex'), 256)",
        "sha2(arrow_cast(decode('00ff', 'hex'), 'BinaryView'), 256) = sha2(decode('00ff', 'hex'), 256)",
    ]).await;
}

#[tokio::test]
async fn binary_encoding_preserves_dictionary_bytes_and_value_nulls() {
    use datafusion::arrow::array::{Array, BinaryArray, DictionaryArray, Int8Array, RecordBatch};
    use datafusion::arrow::datatypes::{Field, Int8Type, Schema};
    use std::sync::Arc;
    let ctx = SQLContext::new();
    let values = Arc::new(BinaryArray::from(vec![Some(&[0_u8, 255][..]), None]));
    let encoded =
        DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![Some(0), Some(1), None]), values)
            .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "bytes",
        encoded.data_type().clone(),
        true,
    )]));
    ctx.ctx()
        .register_batch(
            "encoded_bytes",
            RecordBatch::try_new(schema, vec![Arc::new(encoded)]).unwrap(),
        )
        .unwrap();
    let batches = ctx.sql("SELECT to_base64(bytes), sha2(bytes, 256), sha2(decode('00ff', 'hex'), 256) FROM encoded_bytes")
        .await.unwrap().collect().await.unwrap();
    let batch = &batches[0];
    assert_eq!(
        ScalarValue::try_from_array(batch.column(0), 0).unwrap(),
        ScalarValue::Utf8(Some("AP8=".into()))
    );
    assert_eq!(
        ScalarValue::try_from_array(batch.column(1), 0).unwrap(),
        ScalarValue::try_from_array(batch.column(2), 0).unwrap()
    );
    for row in [1, 2] {
        for column in [0, 1] {
            assert_eq!(
                ScalarValue::try_from_array(batch.column(column), row).unwrap(),
                ScalarValue::Utf8(None)
            );
        }
    }
}

#[tokio::test]
async fn date_and_timestamp_aliases_follow_session_timezone_and_epoch_units() {
    let ctx = SQLContext::new();
    for timezone in ["+00:00", "Asia/Shanghai"] {
        ctx.sql(&format!("SET TIME ZONE = '{timezone}'"))
            .await
            .unwrap();
        assert_true(
            &ctx,
            &[
                "curdate() = current_date()",
                "timestamp_millis(1000) = to_timestamp_millis(1000)",
                "to_timestamp_ltz('2024-01-01 00:00:00') = to_timestamp('2024-01-01 00:00:00')",
                "to_timestamp_ltz(NULL) IS NULL",
            ],
        )
        .await;
        let batches = ctx
            .sql("SELECT to_timestamp_ltz('2024-01-01 00:00:00'), to_timestamp('2024-01-01 00:00:00')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        for column in batches[0].columns() {
            assert_eq!(
                ScalarValue::try_from_array(column, 0).unwrap(),
                ScalarValue::TimestampNanosecond(
                    Some(if timezone == "+00:00" {
                        1704067200000000000
                    } else {
                        1704038400000000000
                    }),
                    Some(timezone.into())
                )
            );
        }
    }
}
