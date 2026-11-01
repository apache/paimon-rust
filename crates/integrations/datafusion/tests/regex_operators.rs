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
async fn regex_operators_preserve_boolean_and_null_semantics() {
    let ctx = SQLContext::new();
    for expression in [
        "'abc' RLIKE '^a'",
        "'abc' REGEXP '^a'",
        "'abc' NOT RLIKE '^z'",
        "('abc' NOT RLIKE '^a') = FALSE",
        "('abc' NOT REGEXP '^a') = FALSE",
        "(NULL RLIKE '^a') IS NULL",
        "('abc' RLIKE NULL) IS NULL",
        "'汉字' RLIKE '^汉'",
        "'Ab' RLIKE '(?i)^a'",
        "'abc RLIKE x' = 'abc RLIKE x'",
    ] {
        let batches = ctx
            .sql(&format!("SELECT {expression}"))
            .await
            .unwrap_or_else(|error| panic!("{expression}: {error}"))
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
async fn regex_operators_cover_native_views_dynamic_inputs_and_parser_boundaries() {
    let ctx = SQLContext::new();
    ctx.sql("CREATE VIEW regex_view AS SELECT 'abc' RLIKE '^a' AS matched")
        .await
        .unwrap();
    for sql in [
        "SELECT matched FROM regex_view",
        "SELECT actual IS NOT DISTINCT FROM expected FROM (SELECT s RLIKE p AS actual, expected FROM (VALUES ('abc','^a',TRUE), ('abc','^z',FALSE), (NULL,'^a',NULL), ('abc',NULL,NULL)) t(s,p,expected)) q",
        "SELECT regexp IS NOT DISTINCT FROM 'x' FROM (SELECT regexp NULL FROM (VALUES ('x')) AS t(regexp)) q(regexp)",
        "SELECT original = 'x' AND matched IS NULL FROM (SELECT regexp NULL, ('abc' RLIKE NULL) AS matched FROM (VALUES ('x')) t(regexp)) q(original,matched)",
        "SELECT ('汉' RLIKE /* comment */ NULL) IS NULL",
        "SELECT (regexp RLIKE NULL) IS NULL FROM (VALUES ('x')) t(regexp)",
        "SELECT (rlike NOT REGEXP NULL) IS NULL FROM (VALUES ('x')) t(rlike)",
        "SELECT ('汉' NOT REGEXP\nNULL) IS NULL",
    ] {
        let batches=ctx.sql(sql).await.unwrap_or_else(|e|panic!("{sql}: {e}")).collect().await.unwrap();
        for batch in batches {for row in 0..batch.num_rows() {
            assert_eq!(ScalarValue::try_from_array(batch.column(0),row).unwrap(),ScalarValue::Boolean(Some(true)),"{sql}");
        }}
    }
    let result = ctx
        .sql("SELECT s RLIKE p FROM (VALUES ('a', '[')) t(s,p)")
        .await;
    match result {
        Err(_) => {}
        Ok(frame) => assert!(frame.collect().await.is_err()),
    }
}
