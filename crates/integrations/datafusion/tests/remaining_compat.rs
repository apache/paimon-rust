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
use datafusion::common::ScalarValue;
use paimon::{catalog::Identifier, Catalog};
use paimon_datafusion::SQLContext;

async fn rows(ctx: &SQLContext, sql: &str) -> Vec<Vec<String>> {
    ctx.sql(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                (0..batch.num_columns())
                    .map(|col| {
                        ScalarValue::try_from_array(batch.column(col), row)
                            .unwrap()
                            .to_string()
                    })
                    .collect()
            })
        })
        .collect()
}
async fn rejects(ctx: &SQLContext, sql: &str) {
    if let Ok(frame) = ctx.sql(sql).await {
        assert!(frame.collect().await.is_err(), "{sql}");
    }
}

#[tokio::test]
async fn aggregate_rewrites_evaluate_volatile_values_once() {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::{create_udf, ColumnarValue, Volatility};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    let ctx = SQLContext::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&calls);
    ctx.ctx().register_udf(create_udf(
        "flip",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Volatile,
        Arc::new(move |args| {
            if counted.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(args[0].clone())
            } else {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(None)))
            }
        }),
    ));
    for sql in [
        "SELECT to_json(collect_list(flip(v))) FROM (VALUES (1)) t(v)",
        "SELECT to_json(collect_list(flip(v))) FROM (VALUES (1)) t(v) GROUP BY v",
        "SELECT arg_max(flip(v),v) FROM (VALUES (1)) t(v)",
        "SELECT max_by(v,flip(v)) FROM (VALUES (1)) t(v)",
        "SELECT min_by(v,flip(v)+0) FROM (VALUES (1)) t(v)",
        "SELECT arg_max(v,flip(v)) FROM (VALUES (1)) t(v) GROUP BY v",
    ] {
        calls.store(0, Ordering::SeqCst);
        let expected = if sql.contains("collect_list") {
            "[1]"
        } else {
            "1"
        };
        assert_eq!(rows(&ctx, sql).await, vec![vec![expected]], "{sql}");
        assert_eq!(calls.load(Ordering::SeqCst), 1, "{sql}");
    }
}

#[tokio::test]
async fn selection_aggregates_merge_native_state_across_batches_and_partitions() {
    use datafusion::arrow::{
        array::{Int64Array, RecordBatch},
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::datasource::MemTable;
    use std::sync::Arc;
    let ctx = SQLContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
        Field::new("k", DataType::Int64, true),
    ]));
    let batch = |g: Vec<i64>, v: Vec<Option<i64>>, k: Vec<Option<i64>>| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(g)),
                Arc::new(Int64Array::from(v)),
                Arc::new(Int64Array::from(k)),
            ],
        )
        .unwrap()
    };
    let partitions = vec![
        vec![
            batch(vec![1, 2], vec![Some(10), Some(200)], vec![Some(1), None]),
            batch(vec![1, 3], vec![None, Some(300)], vec![Some(4), Some(10)]),
        ],
        vec![
            batch(
                vec![1, 2, 3],
                vec![Some(30), Some(201), None],
                vec![Some(3), None, Some(11)],
            ),
            batch(vec![1, 3], vec![Some(999), Some(301)], vec![None, Some(12)]),
        ],
    ];
    ctx.ctx()
        .register_table(
            "selection_inputs",
            Arc::new(MemTable::try_new(schema, partitions).unwrap()),
        )
        .unwrap();
    assert_eq!(
        rows(
            &ctx,
            "SELECT g,arg_max(CAST(NULL AS BIGINT),1) FROM selection_inputs GROUP BY g ORDER BY g"
        )
        .await,
        vec![vec!["1", "NULL"], vec!["2", "NULL"], vec!["3", "NULL"]]
    );
    assert_eq!(rows(&ctx,"SELECT g,max_by(v,k),min_by(v,k),arg_max(v,k),max_by(v,k) FILTER (WHERE v<300 OR v IS NULL) FROM selection_inputs GROUP BY g ORDER BY g").await,vec![vec!["1","NULL","10","30","NULL"],vec!["2","NULL","NULL","NULL","NULL"],vec!["3","301","300","301","NULL"]]);
    assert_eq!(
        rows(
            &ctx,
            "SELECT max_by(v,k),min_by(v,k),arg_max(v,k) FROM selection_inputs"
        )
        .await,
        vec![vec!["301", "10", "301"]]
    );
}

#[tokio::test]
async fn selection_aggregates_respect_logical_nulls_and_filter_validity() {
    use datafusion::arrow::{
        array::{Array, BooleanArray, DictionaryArray, Int64Array, Int8Array, RecordBatch},
        datatypes::{DataType, Field, Int8Type, Schema},
    };
    use std::sync::Arc;
    let ctx = SQLContext::new();
    let v = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![0, 1]),
        Arc::new(Int64Array::from(vec![Some(10), None])),
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, false),
        Field::new("v", v.data_type().clone(), true),
        Field::new("k", DataType::Int64, false),
        Field::new("p", DataType::Boolean, true),
    ]));
    ctx.ctx()
        .register_batch(
            "nullable_selection",
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![1, 1])),
                    Arc::new(v),
                    Arc::new(Int64Array::from(vec![1, 2])),
                    Arc::new(BooleanArray::from(vec![Some(true), None])),
                ],
            )
            .unwrap(),
        )
        .unwrap();
    let global = rows(
        &ctx,
        "SELECT to_json(arg_max(v,k)),to_json(max_by(v,k)) FROM nullable_selection",
    )
    .await;
    let grouped = rows(
        &ctx,
        "SELECT to_json(arg_max(v,k)),to_json(max_by(v,k)) FROM nullable_selection GROUP BY g",
    )
    .await;
    let filtered = rows(
        &ctx,
        "SELECT max_by(k,k) FILTER(WHERE NOT p) FROM nullable_selection GROUP BY g",
    )
    .await;
    assert_eq!(
        (global, grouped, filtered),
        (
            vec![vec!["10".into(), "NULL".into()]],
            vec![vec!["10".into(), "NULL".into()]],
            vec![vec!["NULL".into()]]
        )
    );
}

#[tokio::test]
async fn aggregate_rewrites_keep_filters_constant_keys_and_local_registrations() {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::{create_udf, Volatility};
    use std::sync::Arc;
    let ctx = SQLContext::new();
    assert_eq!(rows(&ctx,"SELECT array_length(collect_list(v) FILTER (WHERE p OR q)) FROM (VALUES (CAST(NULL AS INT),TRUE,FALSE),(7,FALSE,TRUE)) t(v,p,q)").await,vec![vec!["1"]]);
    assert_eq!(
        rows(
            &ctx,
            "SELECT max_by(v,100),min_by(v,1.5),arg_max(v,0) FROM (VALUES ('a'),('a')) t(v)"
        )
        .await,
        vec![vec!["a", "a", "a"]]
    );
    assert_eq!(rows(&ctx,"SELECT array_length(collect_list(p OR q)) FROM (VALUES (NULL,FALSE),(FALSE,TRUE)) t(p,q)").await,vec![vec!["1"]]);
    ctx.ctx().register_udf(create_udf(
        "first",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| Ok(args[0].clone())),
    ));
    assert_eq!(
        rows(&ctx, "SELECT first(v) FROM (VALUES (1),(2)) t(v)").await,
        vec![vec!["1"], vec!["2"]]
    );
}

#[tokio::test]
async fn json_output_decodes_dictionary_value_nulls_and_preserves_nested_values() {
    use datafusion::arrow::{
        array::{Array, ArrayRef, DictionaryArray, Int8Array, RecordBatch, StringArray},
        datatypes::{Field, Int8Type, Schema},
    };
    use std::sync::Arc;
    let ctx = SQLContext::new();
    let strings = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![Some(0), Some(1), None]),
        Arc::new(StringArray::from(vec![None, Some("ok")])),
    )
    .unwrap();
    let strings: ArrayRef = Arc::new(strings);
    ctx.ctx()
        .register_batch(
            "dictionary_strings",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "s",
                    strings.data_type().clone(),
                    true,
                )])),
                vec![strings],
            )
            .unwrap(),
        )
        .unwrap();
    assert_eq!(
        rows(&ctx, "SELECT to_json(s) FROM dictionary_strings").await,
        vec![vec!["NULL"], vec!["\"ok\""], vec!["NULL"]]
    );
    assert_eq!(
        rows(&ctx, "SELECT to_json(NULL),to_json(named_struct('x',NULL))").await,
        vec![vec!["NULL", "{\"x\":null}"]]
    );
    let variant = ctx
        .sql("SELECT parse_json(s) AS v FROM (VALUES ('{\"a\":1}'),(NULL)) t(s)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let dictionary = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![Some(0), Some(1), None]),
        Arc::clone(variant[0].column(0)),
    )
    .unwrap();
    let dictionary: ArrayRef = Arc::new(dictionary);
    ctx.ctx()
        .register_batch(
            "dictionary_variants",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "v",
                    dictionary.data_type().clone(),
                    true,
                )])),
                vec![dictionary],
            )
            .unwrap(),
        )
        .unwrap();
    assert_eq!(
        rows(
            &ctx,
            "SELECT to_json(v),CAST(v AS STRING) AS cast_value,TRY_CAST(v AS STRING) AS try_value FROM dictionary_variants"
        )
        .await,
        vec![
            vec!["{\"a\":1}", "{\"a\":1}", "{\"a\":1}"],
            vec!["NULL", "NULL", "NULL"],
            vec!["NULL", "NULL", "NULL"]
        ]
    );
    let json=rows(&ctx,"SELECT to_json(named_struct('n',NULL,'d',CAST(1.25 AS DECIMAL(8,2)),'b',decode('00ff','hex')))").await;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&json[0][0]).unwrap(),
        serde_json::json!({"n":null,"d":1.25,"b":"AP8="})
    );
}

#[tokio::test]
async fn ddl_compatibility_respects_name_resolution_and_does_not_mutate_shadowed_tables() {
    let (_tmp, catalog) = common::create_test_env();
    let ctx = common::create_sql_context(catalog.clone()).await;
    ctx.sql("CREATE TABLE default.shadow (id INT)")
        .await
        .unwrap();
    let id = Identifier::new("default", "shadow");
    let before = catalog.get_table(&id).await.unwrap().schema().clone();
    assert!(rows(&ctx, "DESCRIBE EXTENDED DEFAULT.SHADOW")
        .await
        .iter()
        .any(|r| r[0] == "id"));
    ctx.sql("CREATE TEMP VIEW default.shadow AS SELECT 99 AS temp_only")
        .await
        .unwrap();
    assert!(rows(&ctx, "DESCRIBE TABLE EXTENDED DEFAULT.SHADOW")
        .await
        .iter()
        .any(|r| r[0] == "temp_only"));
    rejects(&ctx, "ALTER TABLE default.shadow MODIFY COLUMN id BIGINT").await;
    assert_eq!(
        catalog.get_table(&id).await.unwrap().schema().fields(),
        before.fields()
    );
    ctx.sql("CREATE TABLE default.nested (r STRUCT<a INT>)")
        .await
        .unwrap();
    ctx.sql("INSERT INTO default.nested VALUES (named_struct('a',7))")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    rejects(
        &ctx,
        "ALTER TABLE default.nested MODIFY COLUMN r STRUCT<a BIGINT>",
    )
    .await;
    assert_eq!(
        rows(&ctx, "SELECT r.a FROM default.nested").await,
        vec![vec!["7"]]
    );
    ctx.sql("CREATE TABLE default.\"MixedCase\" (id INT)")
        .await
        .unwrap();
    assert!(rows(&ctx, "DESCRIBE FORMATTED default.\"MixedCase\"")
        .await
        .iter()
        .any(|r| r[0] == "# Table" && r[1] == "MixedCase"));
    ctx.sql("SET datafusion.sql_parser.enable_ident_normalization = false")
        .await
        .unwrap();
    assert!(rows(&ctx, "DESCRIBE EXTENDED default.MixedCase")
        .await
        .iter()
        .any(|r| r[0] == "# Table" && r[1] == "MixedCase"));
    let native = SQLContext::new();
    native
        .sql("CREATE VIEW native_view AS SELECT 1 AS x")
        .await
        .unwrap();
    assert!(rows(&native, "DESCRIBE EXTENDED native_view")
        .await
        .iter()
        .any(|r| r[0] == "x"));
}

#[tokio::test]
async fn temporal_and_encoding_boundaries_are_explicit() {
    let ctx = SQLContext::new();
    assert_eq!(rows(&ctx,"SELECT conv('10',-10,2) IS NULL,conv('9223372036854775808',-10,10) IS NULL,conv('10',10,-2)").await,vec![vec!["true","true","1010"]]);
    for zone in ["+08:60", "-09:99", "+0860", "-0060", "+24:00"] {
        assert_eq!(
            rows(&ctx, &format!("SELECT convert_tz('2026-11-01 12:00:00','{zone}','UTC') IS NULL,convert_tz('2026-11-01 12:00:00','UTC','{zone}') IS NULL")).await,
            vec![vec!["true", "true"]],
            "{zone}"
        );
    }
    assert_eq!(rows(&ctx,"SELECT convert_tz('2026-11-01 12:00:00','+08:30','UTC')=TIMESTAMP '2026-11-01 03:30:00',convert_tz('2026-11-01 12:00:00','UTC','-0930')=TIMESTAMP '2026-11-01 02:30:00',convert_tz('2026-11-01 12:00:00','+08','UTC')=TIMESTAMP '2026-11-01 04:00:00'").await,vec![vec!["true","true","true"]]);
    assert_eq!(rows(&ctx,"SELECT date_diff('month',TIMESTAMP '2024-01-31 12:00:00',TIMESTAMP '2024-02-29 12:00:00'),date_diff('month',TIMESTAMP '2024-02-29 12:00:00',TIMESTAMP '2024-01-31 12:00:00'),date_diff('hour',TIMESTAMP '2026-11-01 04:00:00',TIMESTAMP '2026-11-01 01:30:00')").await,vec![vec!["1","-1","-2"]]);
    assert_eq!(rows(&ctx,"SELECT conv('18446744073709551616',10,16),conv('  +1z',10,2),conv('bad',10,16),conv('ff',-9223372036854775808,10) IS NULL,convert_tz('2026-11-01 01:30:00','America/New_York','UTC') IS NULL,convert_tz('2026-11-01 12:00:00','invalid','UTC') IS NULL").await,vec![vec!["FFFFFFFFFFFFFFFF","1","0","true","true","true"]]);
}

#[tokio::test]
async fn compatible_aggregates_preserve_null_filter_and_ordering_contracts() {
    let ctx = SQLContext::new();
    assert_eq!(rows(&ctx,"SELECT count_if(p), any(p), first(CASE WHEN v=8 THEN NULL ELSE v END,true), any_value(CASE WHEN v=8 THEN NULL ELSE v END,true), approx_count_distinct(v,0.05) FROM (VALUES (NULL,NULL),(FALSE,7),(TRUE,7),(NULL,8)) t(p,v)").await,vec![vec!["1","true","7","7","2"]]);
    assert_eq!(
        rows(
            &ctx,
            "SELECT arrow_typeof(approx_count_distinct(v)) FROM (VALUES (1),(2)) t(v)"
        )
        .await,
        vec![vec!["Int64"]]
    );
    assert_eq!(rows(&ctx,"SELECT max_by(v,k),min_by(v,k),arg_max(v,k) FROM (VALUES ('ignored',NULL),('a',1),('b',2),(NULL,3)) t(v,k)").await,vec![vec!["NULL","a","b"]]);
    assert_eq!(rows(&ctx,"SELECT max_by(v,k),min_by(v,k),arg_max(v,k) FROM (VALUES ('a',CAST(NULL AS INT))) t(v,k)").await,vec![vec!["NULL","NULL","NULL"]]);
    assert_eq!(rows(&ctx,"SELECT max_by(v,k) FILTER (WHERE k<3),listagg(v,',') WITHIN GROUP (ORDER BY k DESC) FROM (VALUES ('a',1),('b',2),(NULL,3)) t(v,k)").await,vec![vec!["b","b,a"]]);
    assert_eq!(rows(&ctx,"SELECT listagg(v,',') WITHIN GROUP (ORDER BY 100),listagg(v,',') WITHIN GROUP (ORDER BY 0) FROM (VALUES ('a'),('a')) t(v)").await,vec![vec!["a,a","a,a"]]);
    assert_eq!(rows(&ctx,"SELECT array_length(collect_list(v)),array_length(collect_list(DISTINCT v)),listagg(v,',') WITHIN GROUP (ORDER BY v) FROM (VALUES ('b'),(NULL),('a'),('a')) t(v)").await,vec![vec!["3","2","a,a,b"]]);
    assert_eq!(rows(&ctx,"SELECT array_length(collect_list(v)),count_if(p),any(p) FROM (VALUES (CAST(NULL AS INT),CAST(NULL AS BOOLEAN))) t(v,p) WHERE FALSE").await,vec![vec!["0","0","NULL"]]);
    assert_eq!(rows(&ctx,"SELECT k,count_if(p) OVER (ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM (VALUES (1,TRUE),(2,FALSE),(3,TRUE)) t(k,p) ORDER BY k").await,vec![vec!["1","1"],vec!["2","1"],vec!["3","2"]]);
    rejects(&ctx, "SELECT approx_count_distinct(1,0.0001)").await;
    rejects(&ctx, "SELECT first(1, 7)").await;
    assert_eq!(rows(&ctx,"SELECT to_json(collect_list(v IGNORE NULLS)),to_json(collect_list(v) IGNORE NULLS) FROM (VALUES (1),(NULL)) t(v)").await,vec![vec!["[1]","[1]"]]);
    rejects(
        &ctx,
        "SELECT collect_list(v) RESPECT NULLS FROM (VALUES (1),(NULL)) t(v)",
    )
    .await;
}

#[tokio::test]
async fn json_output_and_variant_cast_preserve_structure_and_nulls() {
    let ctx = SQLContext::new();
    assert_eq!(rows(&ctx,"SELECT CAST(parse_json('{\"a\":1}') AS STRING),CAST(parse_json('\"hello\"') AS STRING),CAST(parse_json('null') AS STRING),TRY_CAST(parse_json('true') AS STRING)").await,vec![vec!["{\"a\":1}","hello","NULL","true"]]);
    let r=rows(&ctx,"SELECT to_json(named_struct('a',ARRAY[1,NULL,3],'b',map(['x'],[TRUE]))),json_object('x',1,'y',NULL),to_json(parse_json('{\"s\":\"a\\nb\"}'))").await;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&r[0][0]).unwrap(),
        serde_json::json!({"a":[1,null,3],"b":{"x":true}})
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&r[0][1]).unwrap(),
        serde_json::json!({"x":1,"y":null})
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&r[0][2]).unwrap(),
        serde_json::json!({"s":"a\nb"})
    );
    assert_eq!(
        rows(&ctx, "SELECT to_json(CAST(NULL AS STRING)),json_object()").await,
        vec![vec!["NULL", "{}"]]
    );
    rejects(&ctx, "SELECT json_object(NULL,1)").await;
    rejects(&ctx, "SELECT json_object('a')").await;
}

#[tokio::test]
async fn custom_like_escape_supports_rows_unicode_and_negation() {
    let ctx = SQLContext::new();
    assert_eq!(rows(&ctx,"SELECT s LIKE p ESCAPE '!',s NOT LIKE p ESCAPE '!' FROM (VALUES ('a_x','a!_%'),('ab','a!_%'),('a!x','a!!%'),(NULL,'x')) t(s,p)").await,vec![vec!["true","false"],vec!["false","true"],vec!["true","false"],vec!["NULL","NULL"]]);
    assert_eq!(
        rows(
            &ctx,
            "SELECT '汉_字' LIKE '汉界_字' ESCAPE '界','Ab_' ILIKE 'a%!_' ESCAPE '!'"
        )
        .await,
        vec![vec!["true", "true"]]
    );
    rejects(&ctx, "SELECT 'a' LIKE 'a!' ESCAPE '!'").await;
    rejects(&ctx, "SELECT 'a' LIKE '%' ESCAPE '!!'").await;
}

#[tokio::test]
async fn variant_string_cast_rejects_corruption_without_reinterpreting_ordinary_structs() {
    use datafusion::arrow::{
        array::{Array, ArrayRef, BinaryArray, RecordBatch, StructArray},
        datatypes::{DataType, Field, Schema},
    };
    use std::sync::Arc;
    let ctx = SQLContext::new();
    let good = ctx
        .sql("SELECT parse_json('{}') AS v")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let DataType::Struct(fields) = good[0].column(0).data_type() else {
        panic!("Expected Variant struct")
    };
    let malformed: ArrayRef = Arc::new(StructArray::new(
        fields.clone(),
        vec![
            Arc::new(BinaryArray::from(vec![Some(&[255u8][..])])),
            Arc::new(BinaryArray::from(vec![Some(&[][..])])),
        ],
        None,
    ));
    ctx.ctx()
        .register_batch(
            "malformed_variants",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "v",
                    malformed.data_type().clone(),
                    true,
                )])),
                vec![malformed],
            )
            .unwrap(),
        )
        .unwrap();
    assert_eq!(
        rows(&ctx, "SELECT TRY_CAST(v AS STRING) FROM malformed_variants").await,
        vec![vec!["NULL"]]
    );
    rejects(&ctx, "SELECT CAST(v AS STRING) FROM malformed_variants").await;
    let ordinary = rows(
        &ctx,
        "SELECT to_json(named_struct('value',decode('01','hex'),'metadata',decode('02','hex')))",
    )
    .await;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&ordinary[0][0]).unwrap(),
        serde_json::json!({"value":"AQ==","metadata":"Ag=="})
    );
}

#[tokio::test]
async fn scalar_compatibility_handles_dates_timezones_bases_and_assertions() {
    let ctx = SQLContext::new();
    assert_eq!(rows(&ctx,"SELECT assert_true(TRUE),conv('ff',16,10),conv('-1',10,16),conv('FFFFFFFFFFFFFFFF',16,-10),date_diff(DATE '2026-11-02',DATE '2026-11-01'),date_diff('hour',TIMESTAMP '2026-11-01 01:30:00',TIMESTAMP '2026-11-01 04:00:00')").await,vec![vec!["NULL","255","FFFFFFFFFFFFFFFF","-1","1","2"]]);
    assert_eq!(rows(&ctx,"SELECT convert_tz('2026-11-01 12:00:00','UTC','Asia/Shanghai')=TIMESTAMP '2026-11-01 20:00:00',convert_tz('2026-03-08 02:30:00','America/New_York','UTC') IS NULL,conv('ff',1,10) IS NULL").await,vec![vec!["true","true","true"]]);
    rejects(&ctx, "SELECT assert_true(FALSE,'bad input')").await;
    rejects(&ctx, "SELECT assert_true(NULL)").await;
    rejects(
        &ctx,
        "SELECT date_diff('nonsense',DATE '2026-11-01',DATE '2026-11-02')",
    )
    .await;
}

#[tokio::test]
async fn modify_column_and_extended_describe_keep_data_and_field_identity() {
    let (_tmp, catalog) = common::create_test_env();
    let ctx = common::create_sql_context(catalog.clone()).await;
    ctx.sql("CREATE TABLE default.compat_ddl (id INT NOT NULL COMMENT 'identity',v STRING) COMMENT 'table info'").await.unwrap();
    ctx.sql("INSERT INTO default.compat_ddl VALUES (7,'x')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let id = Identifier::new("default", "compat_ddl");
    let before = catalog.get_table(&id).await.unwrap().schema().clone();
    ctx.sql("ALTER TABLE default.compat_ddl MODIFY COLUMN id BIGINT")
        .await
        .unwrap();
    let after = catalog.get_table(&id).await.unwrap().schema().clone();
    assert_eq!(before.fields()[0].id(), after.fields()[0].id());
    assert_eq!(after.fields()[0].description(), Some("identity"));
    assert!(!after.fields()[0].data_type().is_nullable());
    assert_eq!(
        rows(&ctx, "SELECT arrow_typeof(id) FROM default.compat_ddl").await,
        vec![vec!["Int64"]]
    );
    rejects(
        &ctx,
        "ALTER TABLE default.compat_ddl MODIFY COLUMN v STRING NOT NULL COMMENT 'new value'",
    )
    .await;
    assert_eq!(
        catalog.get_table(&id).await.unwrap().schema().id(),
        after.id()
    );
    ctx.sql(
        "ALTER TABLE default.compat_ddl SET ('alter-column-null-to-not-null.disabled'='false')",
    )
    .await
    .unwrap();
    ctx.sql("ALTER TABLE default.compat_ddl MODIFY COLUMN v STRING NOT NULL COMMENT 'new value'")
        .await
        .unwrap();
    let updated = catalog.get_table(&id).await.unwrap().schema().clone();
    assert_eq!(updated.fields()[1].id(), before.fields()[1].id());
    assert_eq!(updated.fields()[1].description(), Some("new value"));
    assert!(!updated.fields()[1].data_type().is_nullable());
    ctx.sql("ALTER TABLE default.compat_ddl MODIFY COLUMN v STRING NULL")
        .await
        .unwrap();
    assert!(catalog.get_table(&id).await.unwrap().schema().fields()[1]
        .data_type()
        .is_nullable());
    assert_eq!(
        rows(&ctx, "SELECT id,v FROM default.compat_ddl").await,
        vec![vec!["7", "x"]]
    );
    ctx.sql("ALTER TABLE default.compat_ddl SET ('test.secret'='hidden')")
        .await
        .unwrap();
    let describe = rows(&ctx, "DESCRIBE TABLE EXTENDED default.compat_ddl").await;
    let properties = describe.iter().find(|r| r[0] == "# Properties").unwrap();
    assert!(!properties[1].contains("hidden"));
    assert!(properties[1].contains("<redacted>"));
    assert!(describe.iter().any(|r| r[0] == "id" && r[2] == "identity"));
    assert!(describe
        .iter()
        .any(|r| r[0] == "# Comment" && r[1] == "table info"));
    assert_eq!(
        describe,
        rows(&ctx, "DESCRIBE EXTENDED default.compat_ddl").await
    );
    rejects(
        &ctx,
        "ALTER TABLE default.compat_ddl MODIFY COLUMN id INT FIRST",
    )
    .await;
    rejects(
        &ctx,
        "ALTER TABLE default.compat_ddl MODIFY COLUMN v INT DEFAULT 9",
    )
    .await;
    assert_eq!(
        rows(&ctx, "SELECT id,v FROM default.compat_ddl").await,
        vec![vec!["7", "x"]]
    );
}
