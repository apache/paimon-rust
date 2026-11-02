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

use datafusion::arrow::array::{
    ArrayRef, LargeStringArray, RecordBatch, StringArray, StringViewArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use paimon_datafusion::SQLContext;

async fn value(ctx: &SQLContext, sql: &str) -> ScalarValue {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    ScalarValue::try_from_array(batches[0].column(0), 0).unwrap()
}

#[tokio::test]
async fn json_extraction_distinguishes_scalars_fragments_and_nulls() {
    let ctx = SQLContext::new();
    for (expression, expected) in [
        (
            "json_value('{\"a\":[1,\"hello\",true,null]}', '$.a[1]')",
            Some("hello"),
        ),
        ("json_extract_scalar('{\"a\":42}', '$.a')", Some("42")),
        ("json_value('true', '$')", Some("true")),
        ("json_value('{\"a\":[1]}', '$.a')", None),
        ("get_json_object('{\"a\":[1,2]}', '$.a')", Some("[1,2]")),
        ("get_json_string('{\"a\":\"text\"}', '$.a')", Some("text")),
        ("json_extract_string('{\"a\":false}', '$.a')", Some("false")),
        ("json_get_str('{\"a\":\"text\"}', '$.a')", Some("text")),
        ("json_value('{\"a\":null}', '$.a')", None),
        ("get_json_object('{\"a\":1}', '$.missing')", None),
        ("json_value('invalid', '$')", None),
        ("get_json_object('{}', 'not_a_path')", None),
        ("json_value(NULL, '$')", None),
        ("get_json_object('{}', NULL)", None),
        (
            "json_value('{\"a.b\":[\"你好\"]}', '$[''a.b''][0]')",
            Some("你好"),
        ),
    ] {
        assert_eq!(
            value(&ctx, &format!("SELECT {expression}")).await,
            ScalarValue::Utf8(expected.map(str::to_string)),
            "{expression}"
        );
    }
}

#[tokio::test]
async fn json_validation_and_format_preserve_json_types() {
    let ctx = SQLContext::new();
    for (input, expected) in [
        ("'null'", Some(true)),
        ("'{}'", Some(true)),
        ("'1 trailing'", Some(false)),
        ("''", Some(false)),
        ("NULL", None),
    ] {
        assert_eq!(
            value(&ctx, &format!("SELECT json_valid({input})")).await,
            ScalarValue::Boolean(expected)
        );
    }
    for (input, expected) in [
        ("' [1, 2] '", Some("[1,2]")),
        ("'\"text\"'", Some("\"text\"")),
        ("'null'", Some("null")),
        ("parse_json('{\"a\":1}')", Some("{\"a\":1}")),
        ("parse_json('null')", Some("null")),
        ("try_parse_json('bad')", None),
        ("NULL", None),
    ] {
        assert_eq!(
            value(&ctx, &format!("SELECT json_format({input})")).await,
            ScalarValue::Utf8(expected.map(str::to_string))
        );
    }
    let error = ctx.sql("SELECT json_format('invalid')").await;
    let message = match error {
        Err(e) => e.to_string(),
        Ok(df) => df.collect().await.unwrap_err().to_string(),
    };
    assert!(message.contains("Invalid JSON"), "{message}");
    assert!(ctx.sql("SELECT json_format(42)").await.is_err());
    assert!(ctx
        .sql("SELECT json_format(decode('7b7d', 'hex'))")
        .await
        .is_err());
    assert!(ctx.sql("SELECT json_format(named_struct('value', decode('00', 'hex'), 'metadata', decode('00', 'hex')))").await.is_err());
}

#[tokio::test]
async fn json_functions_evaluate_string_arrays_and_dynamic_paths() {
    let ctx = SQLContext::new();
    let docs = [
        Some("{\"a\":1,\"b\":2}"),
        Some("{\"a\":3,\"b\":4}"),
        None,
        Some("bad"),
    ];
    for array in [
        Arc::new(StringArray::from(docs.to_vec())) as ArrayRef,
        Arc::new(LargeStringArray::from(docs.to_vec())),
        Arc::new(StringViewArray::from(docs.to_vec())),
    ] {
        let schema = Arc::new(Schema::new(vec![
            Field::new("doc", array.data_type().clone(), true),
            Field::new("path", DataType::Utf8, true),
        ]));
        let paths = Arc::new(StringArray::from(vec!["$.a", "$.b", "$", "$"]));
        ctx.ctx()
            .register_batch(
                "json_inputs",
                RecordBatch::try_new(schema, vec![array, paths]).unwrap(),
            )
            .unwrap();
        let batches = ctx
            .sql("SELECT json_value(doc, path), json_valid(doc) FROM json_inputs")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let result = &batches[0];
        for (row, expected) in [Some("1"), Some("4"), None, None].into_iter().enumerate() {
            assert_eq!(
                ScalarValue::try_from_array(result.column(0), row).unwrap(),
                ScalarValue::Utf8(expected.map(str::to_string))
            );
        }
        assert_eq!(
            ScalarValue::try_from_array(result.column(1), 3).unwrap(),
            ScalarValue::Boolean(Some(false))
        );
        ctx.ctx().deregister_table("json_inputs").unwrap();
    }
}

#[tokio::test]
async fn json_format_accepts_dictionary_encoded_text() {
    use datafusion::arrow::array::{Array, DictionaryArray, Int8Array};
    use datafusion::arrow::datatypes::Int8Type;
    let ctx = SQLContext::new();
    let array = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![Some(0), Some(1), None]),
        Arc::new(StringArray::from(vec!["{\"a\":1}", "[2,3]"])),
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "doc",
        array.data_type().clone(),
        true,
    )]));
    ctx.ctx()
        .register_batch(
            "dictionary_json",
            RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap(),
        )
        .unwrap();
    let batches = ctx
        .sql(
            "SELECT json_format(doc), json_value(doc, '$.a'), json_valid(doc) FROM dictionary_json",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    for (row, expected) in [Some("{\"a\":1}"), Some("[2,3]"), None]
        .into_iter()
        .enumerate()
    {
        assert_eq!(
            ScalarValue::try_from_array(batches[0].column(0), row).unwrap(),
            ScalarValue::Utf8(expected.map(str::to_string))
        );
    }
}
