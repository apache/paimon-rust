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

async fn rejects(ctx: &SQLContext, sql: &str) {
    match ctx.sql(sql).await {
        Err(_) => {}
        Ok(frame) => assert!(frame.collect().await.is_err(), "{sql}"),
    }
}

#[tokio::test]
async fn distinct_comparisons_respect_boolean_precedence_and_parentheses() {
    let ctx = SQLContext::new();
    check(&ctx, "SELECT TRUE IS DISTINCT FROM FALSE OR TRUE, FALSE IS NOT DISTINCT FROM FALSE OR TRUE, TRUE IS DISTINCT FROM FALSE AND FALSE", &["true|true|false"]).await;
    check(
        &ctx,
        "SELECT v IS DISTINCT FROM 1 OR v IS NULL FROM (VALUES (1),(2),(NULL)) t(v)",
        &["false", "true", "true"],
    )
    .await;
    check(&ctx, "SELECT NOT TRUE IS DISTINCT FROM FALSE OR TRUE, TRUE IS DISTINCT FROM (FALSE OR TRUE), TRUE IS DISTINCT FROM FALSE IS DISTINCT FROM TRUE", &["true|false|false"]).await;
    check(
        &ctx,
        "SELECT v FROM (VALUES (1),(2),(NULL)) t(v) WHERE v IS NOT DISTINCT FROM 1 OR v IS NULL",
        &["1", "NULL"],
    )
    .await;
    ctx.ctx()
        .sql("CREATE TABLE flags AS SELECT FALSE AS b WHERE FALSE")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    ctx.sql("INSERT INTO flags SELECT TRUE IS DISTINCT FROM FALSE OR TRUE")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    check(&ctx, "SELECT b FROM flags", &["true"]).await;
    ctx.sql("CREATE VIEW datafusion.public.flags_view AS SELECT FALSE IS NOT DISTINCT FROM FALSE OR TRUE AS b").await.unwrap().collect().await.unwrap();
    check(&ctx, "SELECT b FROM flags_view", &["true"]).await;
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
async fn approximate_percentiles_select_values_and_preserve_probability_order() {
    let ctx = SQLContext::new();
    check(&ctx, "SELECT approx_percentile(v,0.5),percentile_approx(v,0.5),arrow_typeof(approx_percentile(v,0.5)) FROM (VALUES (0),(10),(NULL)) t(v)", &["0|0|Int64"]).await;
    check(&ctx, "SELECT to_json(approx_percentile(v,array(0.5,0.4,0.1),100)),to_json(percentile_approx(v,array(1.0,0.0,0.5,0.5))) FROM (VALUES (0),(1),(2),(10)) t(v)", &["[1,1,0]|[10,0,1,1]"]).await;
    check(&ctx, "SELECT approx_percentile(v,0.5) IS NULL, approx_percentile(v,array(0.5)) IS NULL FROM (VALUES (CAST(NULL AS BIGINT))) t(v)", &["true|true"]).await;
    check(
        &ctx,
        "SELECT approx_percentile(v,array()) IS NULL FROM (VALUES (1)) t(v)",
        &["true"],
    )
    .await;
    check(
        &ctx,
        "SELECT approx_percentile(v,1.0,1) FROM (VALUES (0),(10)) t(v)",
        &["0"],
    )
    .await;
    check(
        &ctx,
        "SELECT approx_percentile(v,1.0/2,100*100) FROM (VALUES (0),(10)) t(v)",
        &["0"],
    )
    .await;
    check(
        &ctx,
        "SELECT approx_percentile(v,'0.5') FROM (VALUES (0),(10)) t(v)",
        &["0"],
    )
    .await;
    check(&ctx,"SELECT approx_percentile(v,date_part('day',current_date())/100.0),approx_percentile(v,date_part('day',curdate())/100.0) FROM (VALUES (0),(10)) t(v)",&["0|0"]).await;
}

#[tokio::test]
async fn approximate_percentiles_keep_numeric_date_and_interval_types() {
    let ctx = SQLContext::new();
    check(&ctx,"SELECT approx_percentile(v,0.5),arrow_typeof(approx_percentile(v,0.5)) FROM (VALUES (CAST(1 AS TINYINT)),(CAST(9 AS TINYINT))) t(v)",&["1|Int8"]).await;
    check(&ctx,"SELECT approx_percentile(v,0.5)=CAST(1.25 AS DECIMAL(10,2)),arrow_typeof(approx_percentile(v,0.5)) FROM (VALUES (CAST(1.25 AS DECIMAL(10,2))),(CAST(5.75 AS DECIMAL(10,2)))) t(v)",&["true|Decimal128(10, 2)"]).await;
    check(&ctx,"SELECT approx_percentile(v,0.5)=DATE '2024-01-01' FROM (VALUES (DATE '2024-01-01'),(DATE '2024-01-03')) t(v)",&["true"]).await;
    check(&ctx,"SELECT approx_percentile(v,0.5)=TIMESTAMP '2024-01-01 00:00:01.5' FROM (VALUES (TIMESTAMP '2024-01-01 00:00:01.5'),(TIMESTAMP '2024-01-01 00:00:03')) t(v)",&["true"]).await;
    check(&ctx,"SELECT approx_percentile(v,0.5)=INTERVAL '1' MONTH FROM (VALUES (INTERVAL '1' MONTH),(INTERVAL '3' MONTH)) t(v)",&["true"]).await;
    check(&ctx,"SELECT approx_percentile(v,0.5)=INTERVAL '1' SECOND FROM (VALUES (INTERVAL '1' SECOND),(INTERVAL '3' SECOND)) t(v)",&["true"]).await;
    check(&ctx,"SELECT CAST(approx_percentile(CAST('7018421793132.065041' AS DECIMAL(19,6)),0.5) AS VARCHAR)",&["7018421793132.065000"]).await;
    check(&ctx,"SELECT CAST(approx_percentile(CAST('100000000000000000000000' AS DECIMAL(38,0)),0.5) AS VARCHAR)",&["99999999999999990000000"]).await;
}

#[tokio::test]
async fn approximate_percentiles_validate_foldable_parameters_and_types() {
    let ctx = SQLContext::new();
    for sql in [
        "SELECT approx_percentile(1,-0.1)",
        "SELECT approx_percentile(1,1.1)",
        "SELECT approx_percentile(1,NULL)",
        "SELECT approx_percentile(1,array(0.5,NULL))",
        "SELECT approx_percentile(1,0.5,0)",
        "SELECT approx_percentile(1,0.5,2147483648)",
        "SELECT approx_percentile(1,0.5,1.5)",
        "SELECT approx_percentile(1,0.5,'100')",
        "SELECT approx_percentile(1,array(TRUE,FALSE))",
        "SELECT approx_percentile(1,array('0.5'))",
        "SELECT approx_percentile(v,1.1) OVER() FROM (VALUES (1)) t(v) WHERE FALSE",
        "SELECT approx_percentile(v,0.5,0) OVER() FROM (VALUES (1)) t(v) WHERE FALSE",
        "SELECT approx_percentile(v,v) OVER() FROM (VALUES (1)) t(v) WHERE FALSE",
        "SELECT approx_percentile(DISTINCT v,0.5) OVER() FROM (VALUES (1),(2)) t(v)",
        "SELECT approx_percentile(v,0.5) FILTER(WHERE v>1) OVER() FROM (VALUES (1),(2)) t(v)",
        "SELECT approx_percentile(v,0.5) IGNORE NULLS FROM (VALUES (1),(NULL),(2)) t(v)",
        "SELECT approx_percentile(v,0.5) IGNORE NULLS OVER() FROM (VALUES (1),(NULL),(2)) t(v)",
        "SELECT approx_percentile(v,0.5) IGNORE NULLS FROM (VALUES (1)) t(v) WHERE FALSE GROUP BY v",
        "CREATE VIEW datafusion.public.invalid_quantile AS SELECT approx_percentile(v,1.1) AS q FROM (VALUES (1)) t(v)",
        "SELECT approx_percentile(1,0.5,NULL)",
        "SELECT approx_percentile(v,p) FROM (VALUES (1,0.5),(2,0.9)) t(v,p)",
        "SELECT approx_percentile(v,0.5,v) FROM (VALUES (1),(2)) t(v)",
        "SELECT approx_percentile(v,1.1) FROM (VALUES (1)) t(v) WHERE FALSE GROUP BY v",
        "SELECT approx_percentile(v,0.5,0) FROM (VALUES (1)) t(v) WHERE FALSE GROUP BY v",
        "SELECT approx_percentile(v,p) FROM (VALUES (1,0.5)) t(v,p) WHERE FALSE GROUP BY v",
        "SELECT approx_percentile(INTERVAL '106751992' DAY,0.5)",
        "SELECT approx_percentile(CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)),array(0.5))",
    ] { rejects(&ctx, sql).await; }
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

#[tokio::test]
async fn approximate_percentiles_handle_distinct_filters_and_sliding_windows() {
    let ctx = SQLContext::new();
    check(
        &ctx,
        "SELECT approx_percentile(v,0.5) RESPECT NULLS FROM (VALUES (1),(NULL),(2)) t(v)",
        &["1"],
    )
    .await;
    check(
        &ctx,
        "SELECT approx_percentile(v,0.5) RESPECT NULLS OVER() FROM (VALUES (1),(NULL),(2)) t(v)",
        &["1", "1", "1"],
    )
    .await;
    check(&ctx,"SELECT approx_percentile(v,0.5) RESPECT NULLS OVER(ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM (VALUES (1,1),(2,NULL),(3,2)) t(i,v) ORDER BY i",&["1","1","2"]).await;
    check(&ctx,"SELECT approx_percentile(DISTINCT v,0.75),percentile_approx(DISTINCT w,0.75) FROM (VALUES (0,0),(0,0),(0,5),(10,10)) t(v,w)",&["10|10"]).await;
    check(&ctx,"SELECT approx_percentile(DISTINCT v,0.6) FROM (VALUES (CAST('-0.0' AS DOUBLE)),(CAST('0.0' AS DOUBLE)),(CAST('1.0' AS DOUBLE))) t(v)",&["1"]).await;
    check(&ctx,"SELECT approx_percentile(DISTINCT v,0.6)=INTERVAL '2' DAY FROM (VALUES (INTERVAL '1' DAY),(INTERVAL '24' HOUR),(INTERVAL '2' DAY)) t(v)",&["true"]).await;
    check(&ctx,"SELECT g,approx_percentile(v,0.75) FILTER(WHERE p) FROM (VALUES (1,0,TRUE),(1,10,FALSE),(1,20,TRUE),(2,30,NULL)) t(g,v,p) GROUP BY g ORDER BY g",&["1|20","2|NULL"]).await;
    check(&ctx,"SELECT approx_percentile(v,0.5) OVER(ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW),percentile_approx(v,0.5) OVER(ORDER BY i ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) FROM (VALUES (1,4),(2,1),(3,9),(4,NULL),(5,3)) t(i,v) ORDER BY i",&["4|1","1|9","1|NULL","9|3","3|NULL"]).await;
    check(&ctx,"SELECT i,approx_percentile(i,0.5) OVER(ORDER BY i RANGE BETWEEN 1 FOLLOWING AND 1 FOLLOWING) FROM (VALUES (1),(2),(4),(5)) t(i) ORDER BY i",&["1|2","2|NULL","4|5","5|NULL"]).await;
}

#[tokio::test]
async fn approximate_percentiles_merge_compressed_partitions_and_distinct_states() {
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use std::sync::Arc;
    let ctx = SQLContext::new();
    ctx.sql("SET datafusion.execution.target_partitions = 2")
        .await
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let batch = |start, end| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(start..end))],
        )
        .unwrap()
    };
    ctx.ctx()
        .register_table(
            "quantiles",
            Arc::new(
                MemTable::try_new(
                    Arc::clone(&schema),
                    vec![
                        vec![batch(0_i64, 25000), batch(25000, 50000)],
                        vec![batch(50000, 75000), batch(75000, 100001)],
                    ],
                )
                .unwrap(),
            ),
        )
        .unwrap();
    check(&ctx,"SELECT to_json(approx_percentile(v,array(0.0,0.1,0.5,0.9,1.0),10)),to_json(percentile_approx(v,array(0.1,0.5,0.9),10000)) FROM quantiles",&["[0,0,57013,100000,100000]|[9995,49999,89994]"]).await;
    // Duplicates cross partial states; merging only GK summaries would double-count.
    check(
        &ctx,
        "SELECT approx_percentile(DISTINCT CASE WHEN v=0 THEN 0 ELSE 10 END,0.5) FROM quantiles",
        &["0"],
    )
    .await;
    check(&ctx,"SELECT approx_percentile(DISTINCT CASE WHEN v=0 THEN CAST('-0.0' AS DOUBLE) WHEN v=50000 THEN CAST('0.0' AS DOUBLE) ELSE CAST('1.0' AS DOUBLE) END,0.6) FROM quantiles",&["1"]).await;
}

#[tokio::test]
async fn percentile_distinct_normalizes_nan_payloads_and_preserves_application_overrides() {
    use datafusion::arrow::array::{Float64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::{create_udf, ColumnarValue, Volatility};
    use std::sync::Arc;
    let ctx = SQLContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]));
    let values = Float64Array::from(vec![
        1.0,
        2.0,
        f64::from_bits(0x7ff8000000000001),
        f64::from_bits(0x7ff8000000000002),
    ]);
    ctx.ctx()
        .register_batch(
            "nan_values",
            RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap(),
        )
        .unwrap();
    check(
        &ctx,
        "SELECT approx_percentile(DISTINCT v,0.6)=2.0 FROM nan_values",
        &["true"],
    )
    .await;
    ctx.ctx().register_udf(create_udf(
        "stable_probability",
        vec![],
        DataType::Float64,
        Volatility::Stable,
        Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(0.5))))),
    ));
    rejects(&ctx, "SELECT approx_percentile(1,stable_probability())").await;
    let sum = datafusion::functions_aggregate::sum::sum_udaf();
    ctx.ctx()
        .register_udaf(sum.as_ref().clone().with_aliases(["approx_percentile"]));
    check(&ctx,"SELECT approx_percentile(v) AS g,approx_percentile(v) OVER() AS w FROM (VALUES (1),(2)) t(v) GROUP BY v ORDER BY v",&["1|3","2|3"]).await;
}
