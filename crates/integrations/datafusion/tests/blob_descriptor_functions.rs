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

use datafusion::arrow::array::{Array, BinaryArray, StringArray};
use paimon_datafusion::SQLContext;

const JAVA_V2_HEX: &str =
    "0243534544424f4c420d00000066696c653a2f2f2f746d702f610000000000000000ffffffffffffffff";
const JAVA_V2_STRING: &str = "BlobDescriptor{version=2, uri='file:///tmp/a', offset=0, length=-1}";
const JAVA_V1_HEX: &str = "010a0000002f746573742f706174686400000000000000c800000000000000";
const JAVA_V1_STRING: &str = "BlobDescriptor{version=1, uri='/test/path', offset=100, length=200}";

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

async fn query_error(ctx: &SQLContext, sql: &str) -> String {
    match ctx.sql(sql).await {
        Err(error) => error.to_string(),
        Ok(dataframe) => dataframe
            .collect()
            .await
            .expect_err("query should fail")
            .to_string(),
    }
}

#[tokio::test]
async fn test_blob_descriptor_functions_are_registered_with_aliases_and_java_format() {
    let ctx = SQLContext::new();
    let batches = ctx
        .sql(
            "SELECT \
             path_to_descriptor('file:///tmp/a'), \
             sys.path_to_descriptor('file:///tmp/a'), \
             descriptor_to_string(path_to_descriptor('file:///tmp/a')), \
             sys.descriptor_to_string(sys.path_to_descriptor('file:///tmp/a'))",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batch = &batches[0];
    for column in 0..2 {
        let descriptors = batch
            .column(column)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(to_hex(descriptors.value(0)), JAVA_V2_HEX);
    }
    for column in 2..4 {
        let strings = batch
            .column(column)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(strings.value(0), JAVA_V2_STRING);
    }
}

#[tokio::test]
async fn test_blob_descriptor_functions_propagate_nulls() {
    let ctx = SQLContext::new();
    let batches = ctx
        .sql(
            "SELECT id, \
             path_to_descriptor(path), \
             descriptor_to_string(path_to_descriptor(path)) \
             FROM (VALUES \
               (1, 'file:///tmp/a'), \
               (2, CAST(NULL AS VARCHAR)), \
               (3, 'file:///tmp/b') \
             ) AS inputs(id, path) \
             ORDER BY id",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batch = &batches[0];
    let descriptors = batch
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    let strings = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(to_hex(descriptors.value(0)), JAVA_V2_HEX);
    assert!(descriptors.is_null(1));
    assert!(strings.is_null(1));
    assert_eq!(
        strings.value(2),
        "BlobDescriptor{version=2, uri='file:///tmp/b', offset=0, length=-1}"
    );

    let alias_nulls = ctx
        .sql(
            "SELECT \
             path_to_descriptor(NULL), \
             sys.path_to_descriptor(NULL), \
             descriptor_to_string(NULL), \
             sys.descriptor_to_string(NULL)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let batch = &alias_nulls[0];
    for column in 0..4 {
        assert!(batch.column(column).is_null(0));
    }
}

#[tokio::test]
async fn test_descriptor_to_string_supports_java_v1() {
    let ctx = SQLContext::new();
    let sql = format!(
        "SELECT descriptor_to_string(X'{JAVA_V1_HEX}'), \
         sys.descriptor_to_string(X'{JAVA_V1_HEX}')"
    );
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let batch = &batches[0];
    for column in 0..2 {
        let strings = batch
            .column(column)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(strings.value(0), JAVA_V1_STRING);
    }
}

#[tokio::test]
async fn test_blob_descriptor_functions_reject_invalid_arguments() {
    let ctx = SQLContext::new();
    for (sql, function) in [
        ("SELECT path_to_descriptor()", "path_to_descriptor"),
        ("SELECT path_to_descriptor(1)", "path_to_descriptor"),
        (
            "SELECT descriptor_to_string('not binary')",
            "descriptor_to_string",
        ),
        (
            "SELECT descriptor_to_string(X'00', X'01')",
            "descriptor_to_string",
        ),
    ] {
        let error = query_error(&ctx, sql).await;
        assert!(
            error.contains(function),
            "expected error for {function}, got: {error}"
        );
    }

    let error = query_error(&ctx, "SELECT descriptor_to_string(X'00')").await;
    assert!(
        error.contains("BlobDescriptor bytes too short"),
        "unexpected malformed descriptor error: {error}"
    );
}
