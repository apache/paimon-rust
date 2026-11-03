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
async fn binary_casts_preserve_bytes_and_apply_declared_width() {
    let ctx = SQLContext::new();
    for expression in [
        "CAST('汉' AS BINARY) = decode('e6b189','hex')",
        "CAST('ab' AS BINARY(4)) = decode('61620000','hex')",
        "CAST('abcd' AS BINARY(2)) = decode('6162','hex')",
        "CAST('abcd' AS VARBINARY(2)) = decode('6162','hex')",
        "CAST(decode('00ff','hex') AS BINARY) = decode('00ff','hex')",
        "CAST(decode('00ff','hex') AS VARBINARY) = decode('00ff','hex')",
        "CAST(NULL AS BINARY(2)) IS NULL",
        "TRY_CAST(NULL AS VARBINARY(2)) IS NULL",
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
async fn binary_casts_work_in_native_views_and_column_expressions() {
    let ctx = SQLContext::new();
    ctx.sql("CREATE VIEW bytes_view AS SELECT CAST('ab' AS BINARY(4)) AS b")
        .await
        .unwrap();
    for sql in [
        "SELECT to_base64(b) = 'YWIAAA==' FROM bytes_view",
        "SELECT to_base64(CAST(CAST('abcd' AS BINARY(6)) AS VARBINARY(2))) = 'YWI='",
        "SELECT CAST(s AS BINARY(3)) = decode(expected, 'hex') OR (s IS NULL AND CAST(s AS BINARY(3)) IS NULL) FROM (VALUES ('ab','616200'), ('abcd','616263'), (NULL,NULL)) t(s,expected)",
    ] {
        let batches=ctx.sql(sql).await.unwrap().collect().await.unwrap();
        for batch in batches { for row in 0..batch.num_rows() {
            assert_eq!(ScalarValue::try_from_array(batch.column(0),row).unwrap(),ScalarValue::Boolean(Some(true)),"{sql}");
        }}
    }
    for ty in [
        "BINARY(0)",
        "VARBINARY(0)",
        "BINARY(2147483648)",
        "VARBINARY(18446744073709551615)",
    ] {
        assert!(
            ctx.sql(&format!("SELECT CAST('x' AS {ty})")).await.is_err(),
            "{ty}"
        );
    }
}

#[tokio::test]
async fn binary_width_decodes_arrow_binary_views_and_dictionary_value_nulls() {
    use datafusion::arrow::{
        array::{
            Array, ArrayRef, BinaryArray, BinaryViewArray, DictionaryArray, Int8Array,
            LargeBinaryArray, RecordBatch,
        },
        datatypes::{Field, Int8Type, Schema},
    };
    use std::sync::Arc;
    let values = Arc::new(BinaryArray::from(vec![Some(&[0, 255][..]), None]));
    let dictionary =
        DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![Some(0), Some(1), None]), values)
            .unwrap();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(BinaryViewArray::from(vec![Some(&[0, 255][..]), None, None])),
        Arc::new(LargeBinaryArray::from(vec![
            Some(&[0, 255][..]),
            None,
            None,
        ])),
        Arc::new(dictionary),
    ];
    for input in arrays {
        let ctx = SQLContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "b",
            input.data_type().clone(),
            true,
        )]));
        ctx.ctx()
            .register_batch("bytes", RecordBatch::try_new(schema, vec![input]).unwrap())
            .unwrap();
        let batches = ctx
            .sql("SELECT to_base64(CAST(b AS BINARY(4))) FROM bytes")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let result = batches
            .iter()
            .flat_map(|b| {
                (0..b.num_rows()).map(|r| ScalarValue::try_from_array(b.column(0), r).unwrap())
            })
            .collect::<Vec<_>>();
        assert_eq!(
            result,
            vec![
                ScalarValue::Utf8(Some("AP8AAA==".into())),
                ScalarValue::Utf8(None),
                ScalarValue::Utf8(None)
            ]
        );
    }
}
