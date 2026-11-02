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
#[tokio::test]
async fn common_scalar_names_have_explicit_semantics() {
    let ctx = SQLContext::new();
    for expression in [
        "split('a,b,c', ',', 2) = ARRAY['a', 'b,c']",
        "split('a  b', ' +') = ARRAY['a','b']",
        "locate('汉', 'a汉字汉', 3) = 4",
        "locate('x', 'abc') = 0",
        "date('2026-10-29') = DATE '2026-10-29'",
        "typeof(CAST(1 AS BIGINT)) = 'bigint'",
        "hex('汉') = 'E6B189'",
        "unhex('f') = decode('0f','hex')",
        "unhex('zz') IS NULL",
        "size(ARRAY[1,NULL,3]) = 3",
        "size(CAST(NULL AS INT[])) = -1",
        "hour(TIMESTAMP '2026-10-29 12:34:56') = 12",
    ] {
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

#[tokio::test]
async fn scalar_boundaries_cover_nulls_encoding_and_temporal_types() {
    let ctx = SQLContext::new();
    for expression in [
        "split('a,b,', ',', 0) = ARRAY['a','b','']",
        "split('汉字', '') = ARRAY['汉','字']",
        "split('abc', '', 2) = ARRAY['a','bc']",
        "split('abc', '^') = ARRAY['abc']",
        "split(NULL, ',') IS NULL",
        "locate('', 'abc', 4) = 4",
        "locate('a', 'abc', 0) = 0",
        "locate('a', 'abc', 9223372036854775807) = 0",
        "locate(NULL, 'abc') IS NULL",
        "hex(-1) = 'FFFFFFFFFFFFFFFF'",
        "hex(decode('00ff','hex')) = '00FF'",
        "hex(NULL) IS NULL",
        "unhex('+f') IS NULL",
        "unhex('0+f') IS NULL",
        "unhex('aBc') = decode('0abc','hex')",
        "unhex('') = decode('','hex')",
        "unhex(NULL) IS NULL",
        "typeof(parse_json('{}')) = 'variant'",
        "typeof(ARRAY[CAST(1 AS INT)]) = 'array<int>'",
        "typeof(CAST(NULL AS DECIMAL(10,2))) = 'decimal(10,2)'",
        "size(map(ARRAY[1,2], ARRAY['a',NULL])) = 2",
        "size(ARRAY[]) = 0",
        "size(CAST(NULL AS INT[])) = -1",
        "hour(CAST(DATE '2026-10-29' AS TIMESTAMP)) = 0",
        "hour(NULL) IS NULL",
    ] {
        let batches = ctx
            .sql(&format!("SELECT {expression}"))
            .await
            .unwrap_or_else(|e| panic!("{expression}: {e}"))
            .collect()
            .await
            .unwrap_or_else(|e| panic!("{expression}: {e}"));
        assert_eq!(
            ScalarValue::try_from_array(batches[0].column(0), 0).unwrap(),
            ScalarValue::Boolean(Some(true)),
            "{expression}"
        );
    }
}
#[tokio::test]
async fn scalar_functions_evaluate_row_inputs() {
    let ctx = SQLContext::new();
    let sql="SELECT split(s,p,n) = expected FROM (VALUES ('a,b,c', ',',2,ARRAY['a','b,c']), ('a b',' +',0,ARRAY['a','b'])) t(s,p,n,expected)";
    for batch in ctx.sql(sql).await.unwrap().collect().await.unwrap() {
        for row in 0..batch.num_rows() {
            assert_eq!(
                ScalarValue::try_from_array(batch.column(0), row).unwrap(),
                ScalarValue::Boolean(Some(true))
            );
        }
    }
    for sql in [
        "SELECT split('abc','[')",
        "SELECT size('abc')",
        "SELECT hour(42)",
    ] {
        match ctx.sql(sql).await {
            Err(_) => {}
            Ok(frame) => assert!(frame.collect().await.is_err(), "{sql}"),
        }
    }
}

#[tokio::test]
async fn hour_uses_timestamp_timezone_metadata_and_hex_accepts_unsigned_bigint_range() {
    use datafusion::arrow::{
        array::{Array, ArrayRef, RecordBatch, TimestampMillisecondArray, UInt64Array},
        datatypes::{Field, Schema},
    };
    use std::sync::Arc;
    let ts = Arc::new(
        TimestampMillisecondArray::from(vec![Some(0), None]).with_timezone("Asia/Shanghai"),
    ) as ArrayRef;
    let nums = Arc::new(UInt64Array::from(vec![Some(i64::MAX as u64), None])) as ArrayRef;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("ts", ts.data_type().clone(), true),
            Field::new("n", nums.data_type().clone(), true),
        ])),
        vec![ts, nums],
    )
    .unwrap();
    let ctx = SQLContext::new();
    ctx.ctx().register_batch("inputs", batch).unwrap();
    let batches = ctx
        .sql("SELECT hour(ts), hex(n) FROM inputs")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        ScalarValue::try_from_array(batches[0].column(0), 0).unwrap(),
        ScalarValue::Int32(Some(8))
    );
    assert_eq!(
        ScalarValue::try_from_array(batches[0].column(1), 0).unwrap(),
        ScalarValue::Utf8(Some("7FFFFFFFFFFFFFFF".into()))
    );
    assert!(batches[0].column(0).is_null(1));
    assert!(batches[0].column(1).is_null(1));
}
