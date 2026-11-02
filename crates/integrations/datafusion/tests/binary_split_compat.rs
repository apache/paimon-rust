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

async fn check(ctx: &SQLContext, sql: &str, expected: &[&str]) {
    let batches = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let rows = batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                (0..batch.num_columns())
                    .map(|col| {
                        ScalarValue::try_from_array(batch.column(col), row)
                            .unwrap()
                            .to_string()
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(rows, expected, "{sql}");
}

#[tokio::test]
async fn binary_lengths_and_slices_preserve_bytes_and_spark_integer_results() {
    let ctx = SQLContext::new();
    for ty in ["Binary", "BinaryView", "LargeBinary", "FixedSizeBinary(2)"] {
        check(&ctx, &format!("SELECT octet_length(b),bit_length(b),length(b),hex(substring(b,2,1)),hex(SUBSTRING(b FROM 1 FOR 1)),arrow_typeof(octet_length(b)),arrow_typeof(bit_length(b)) FROM (SELECT arrow_cast(decode('00ff','hex'),'{ty}') b)"), &["2|16|2|FF|00|Int32|Int32"]).await;
    }
    check(&ctx, "SELECT octet_length('中a'),bit_length('中a'),length('中a'),octet_length(NULL),bit_length(NULL)", &["4|32|2|NULL|NULL"]).await;
    check(&ctx, "SELECT substring(length => 1, str => 'Spark SQL', pos => 5),substr(pos => 1, length => 2, str => '中ab')", &["k|中a"]).await;
    for ty in ["Utf8", "Utf8View", "LargeUtf8"] {
        check(&ctx, &format!("SELECT octet_length(arrow_cast('中a','{ty}')),arrow_typeof(octet_length(arrow_cast('中a','{ty}')))"), &["4|Int32"]).await;
    }
}

#[tokio::test]
async fn split_index_extends_without_changing_spark_split_part() {
    let ctx = SQLContext::new();
    check(&ctx, "SELECT split_index('a_b_c','_',0),split_index('a_b_c','_',1),split_index('a_b_c','_',2),split_index('a_b_c','_',3),split_index('a_b_c','_',-1)", &["a|b|c|NULL|NULL"]).await;
    check(&ctx, "SELECT split_index('a..b.','.',1),split_index('a..b.','.',3),split_index('中🙂末','🙂',1),split_index('abc','',0),split_index(NULL,'_',1),split_index('a','_',NULL)", &["||末|abc|NULL|NULL"]).await;
    check(
        &ctx,
        "SELECT split_part('a_b_c','_',1),split_part('a_b_c','_',-1),split_part('a_b_c','_',9)",
        &["a|c|"],
    )
    .await;
    check(&ctx, "SELECT split_index('',',',0),split_index('a  b ','',1),split_index('a  b ','',3),split_index('a b','',0)", &["NULL|||a b"]).await;
    check(&ctx, "SELECT split_index(s,d,i) FROM (VALUES ('a_b','_',1),('a.b','.',0),('x','_',9),(NULL,'_',0),('x',NULL,0),('x','_',NULL)) t(s,d,i)", &["b","a","NULL","NULL","NULL","NULL"]).await;
}

#[tokio::test]
async fn binary_functions_decode_dictionary_value_and_key_nulls() {
    use datafusion::arrow::array::{
        Array, ArrayRef, BinaryArray, DictionaryArray, Int8Array, RecordBatch,
    };
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::{DataType, Field, Int8Type, Schema};
    use std::sync::Arc;
    for ty in [
        DataType::Binary,
        DataType::BinaryView,
        DataType::LargeBinary,
        DataType::FixedSizeBinary(2),
    ] {
        let ctx = SQLContext::new();
        let bytes: ArrayRef = Arc::new(BinaryArray::from(vec![Some(&[0_u8, 255][..]), None]));
        let values = cast(&bytes, &ty).unwrap();
        let values = DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), Some(1), None]),
            values,
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "b",
            values.data_type().clone(),
            true,
        )]));
        ctx.ctx()
            .register_batch(
                "bytes",
                RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap(),
            )
            .unwrap();
        check(
            &ctx,
            "SELECT octet_length(b),bit_length(b),len(b),hex(SUBSTRING(b FROM 2 FOR 1)) FROM bytes",
            &["2|16|2|FF", "NULL|NULL|NULL|NULL", "NULL|NULL|NULL|NULL"],
        )
        .await;
    }
}
