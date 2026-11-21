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

use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use paimon_datafusion::SQLContext;

async fn values(sql: &str) -> Vec<ScalarValue> {
    let context = SQLContext::new();
    Box::pin(context.sql(sql))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows())
                .map(|row| ScalarValue::try_from_array(batch.column(1), row).unwrap())
        })
        .collect()
}

fn integer(value: Option<i64>) -> ScalarValue {
    ScalarValue::Int64(value)
}

fn integers(values: &[i64]) -> ScalarValue {
    ScalarValue::List(ScalarValue::new_list(
        &values
            .iter()
            .map(|value| integer(Some(*value)))
            .collect::<Vec<_>>(),
        &DataType::Int64,
        false,
    ))
}

#[tokio::test]
async fn sliding_rows_retract_old_values_and_include_following_rows() {
    let input = "FROM (VALUES (1, 10), (2, 1), (3, 20), (4, 2)) t(id, v) ORDER BY id";
    for (frame, expected) in [
        ("1 PRECEDING AND CURRENT ROW", [10, 1, 1, 2]),
        ("CURRENT ROW AND 1 FOLLOWING", [1, 1, 2, 2]),
    ] {
        let sql = format!(
            "SELECT id, approx_percentile(v, 0.5) OVER (ORDER BY id ROWS BETWEEN {frame}) {input}"
        );
        assert_eq!(
            values(&sql).await,
            expected.map(|value| integer(Some(value)))
        );
    }
}

#[tokio::test]
async fn sliding_range_keeps_peers_in_the_same_frame() {
    let actual = values(
        "SELECT id, approx_percentile(v, 0.5) OVER \
         (ORDER BY k RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) \
         FROM (VALUES (1, 1, 9), (2, 1, 1), (3, 2, 8), (4, 3, 2), (5, 3, 4)) t(id, k, v) ORDER BY id",
    )
    .await;
    assert_eq!(actual, [1, 1, 8, 4, 4].map(|value| integer(Some(value))));
}

#[tokio::test]
async fn sliding_filter_and_null_rows_leave_the_frame_consistently() {
    let actual = values(
        "SELECT id, approx_percentile(v, 0.5) FILTER (WHERE keep) OVER \
         (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) \
         FROM (VALUES (1, 9, true), (2, NULL, true), (3, 1, false), (4, 8, true), (5, 2, true)) t(id, v, keep) ORDER BY id",
    )
    .await;
    assert_eq!(
        actual,
        [Some(9), Some(9), None, Some(8), Some(2)].map(integer)
    );
}

#[tokio::test]
async fn sliding_distinct_retains_values_until_the_last_duplicate_leaves() {
    let actual = values(
        "SELECT id, approx_percentile(DISTINCT v, array(0.0, 0.5, 1.0)) OVER \
         (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) \
         FROM (VALUES (1, 1), (2, 1), (3, 2), (4, 3), (5, 4)) t(id, v) ORDER BY id",
    )
    .await;
    assert_eq!(
        actual,
        [
            integers(&[1, 1, 1]),
            integers(&[1, 1, 1]),
            integers(&[1, 1, 2]),
            integers(&[1, 2, 3]),
            integers(&[2, 3, 4]),
        ]
    );
}
