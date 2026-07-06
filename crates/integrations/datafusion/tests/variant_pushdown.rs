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

use datafusion::arrow::array::{Array, Float64Array, Int32Array, Int64Array, StringArray};
use datafusion::physical_plan::displayable;
use paimon_datafusion::SQLContext;

async fn setup_shredded_variant_table() -> (tempfile::TempDir, SQLContext) {
    let (tmp, sql_context) = common::setup_sql_context().await;
    common::exec(
        &sql_context,
        r#"
        CREATE TABLE paimon.test_db.t (
            id INT,
            payload VARIANT
        ) WITH (
            'file.format' = 'parquet',
            'variant.shreddingSchema' =
                '{"type":"ROW","fields":[{"name":"payload","type":{"type":"ROW","fields":[{"name":"age","type":"INT"},{"name":"city","type":"STRING"}]}}]}'
        )
        "#,
    )
    .await;
    (tmp, sql_context)
}

async fn setup_shredded_variant_table_with_rows() -> (tempfile::TempDir, SQLContext) {
    let (tmp, sql_context) = setup_shredded_variant_table().await;
    common::exec(
        &sql_context,
        r#"
        INSERT INTO paimon.test_db.t
        SELECT 1, parse_json('{"age":27,"age_text":"27","city":"Beijing","a;b":11,"unused":"large"}')
        UNION ALL
        SELECT 2, parse_json('{"age":32,"age_text":"32","city":"Hangzhou","a;b":22,"unused":"large"}')
        "#,
    )
    .await;
    (tmp, sql_context)
}

async fn setup_data_evolution_shredded_variant_table_with_rows() -> (tempfile::TempDir, SQLContext)
{
    let (tmp, sql_context) = common::setup_sql_context().await;
    common::exec(
        &sql_context,
        r#"
        CREATE TABLE paimon.test_db.de_t (
            id INT,
            payload VARIANT
        ) WITH (
            'file.format' = 'parquet',
            'data-evolution.enabled' = 'true',
            'variant.shreddingSchema' =
                '{"type":"ROW","fields":[{"name":"payload","type":{"type":"ROW","fields":[{"name":"age","type":"INT"},{"name":"city","type":"STRING"}]}}]}'
        )
        "#,
    )
    .await;
    common::exec(
        &sql_context,
        r#"
        INSERT INTO paimon.test_db.de_t (id, payload)
        SELECT 1, parse_json('{"age":27,"city":"Beijing"}')
        UNION ALL
        SELECT 2, parse_json('{"age":32,"city":"Hangzhou"}')
        "#,
    )
    .await;
    (tmp, sql_context)
}

#[tokio::test]
async fn variant_get_projection_pushes_extractions_into_scan() {
    let (_tmp, sql_context) = setup_shredded_variant_table_with_rows().await;
    let sql = r#"
        SELECT
            id,
            variant_get(payload, '$.age', 'int') AS age,
            variant_get(payload, '$.city', 'string') AS city
        FROM paimon.test_db.t
        ORDER BY id
    "#;
    let df = sql_context.sql(sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let plan_text = displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        plan_text.contains("PushedVariants=[payload=[$.age,$.city]]"),
        "plan should push variant extractions, got:\n{plan_text}"
    );

    let batches = sql_context.sql(sql).await.unwrap().collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    let ids = batches[0]
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let ages = batches[0]
        .column_by_name("age")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let cities = batches[0]
        .column_by_name("city")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(ids.values(), &[1, 2]);
    assert_eq!(ages.values(), &[27, 32]);
    assert_eq!(cities.value(0), "Beijing");
    assert_eq!(cities.value(1), "Hangzhou");
}

#[tokio::test]
async fn variant_get_filter_pushes_extraction_into_scan() {
    let (_tmp, sql_context) = setup_shredded_variant_table_with_rows().await;
    let sql = r#"
        SELECT id
        FROM paimon.test_db.t
        WHERE variant_get(payload, '$.age', 'int') >= 30
        ORDER BY id
    "#;

    let df = sql_context.sql(sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let plan_text = displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        plan_text.contains("PushedVariants=[payload=[$.age]]"),
        "plan should push filter variant extraction, got:\n{plan_text}"
    );

    let batches = sql_context.sql(sql).await.unwrap().collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    let ids = batches[0]
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.values(), &[2]);
}

#[tokio::test]
async fn variant_get_long_to_double_pushdown_matches_udf_cast() {
    let (_tmp, sql_context) = setup_shredded_variant_table_with_rows().await;
    let sql = r#"
        SELECT variant_get(payload, '$.age', 'double') AS value
        FROM paimon.test_db.t
        ORDER BY id
    "#;

    let df = sql_context.sql(sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let plan_text = displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        plan_text.contains("PushedVariants=[payload=[$.age]]"),
        "plan should push long-to-double variant extraction, got:\n{plan_text}"
    );

    let batches = sql_context.sql(sql).await.unwrap().collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    let values = batches[0]
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(values.values(), &[27.0, 32.0]);
}

#[tokio::test]
async fn try_variant_get_string_to_int_pushdown_matches_udf_cast() {
    let (_tmp, sql_context) = setup_shredded_variant_table_with_rows().await;
    let sql = r#"
        SELECT try_variant_get(payload, '$.age_text', 'int') AS value
        FROM paimon.test_db.t
        ORDER BY id
    "#;

    let df = sql_context.sql(sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let plan_text = displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        plan_text.contains("PushedVariants=[payload=[$.age_text]]"),
        "plan should push string-to-int try_variant_get extraction, got:\n{plan_text}"
    );

    let batches = sql_context.sql(sql).await.unwrap().collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    let values = batches[0]
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(values.values(), &[27, 32]);
}

#[tokio::test]
async fn variant_get_date_type_remains_unsupported() {
    let (_tmp, sql_context) = setup_shredded_variant_table_with_rows().await;
    let sql = r#"
        SELECT variant_get(payload, '$.age', 'date') AS value
        FROM paimon.test_db.t
    "#;

    let err = match sql_context.sql(sql).await {
        Ok(df) => df.collect().await.unwrap_err(),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("Unsupported variant_get type: date"),
        "expected public variant_get type parser to reject date, got: {err:?}"
    );
}

#[tokio::test]
async fn full_variant_projection_prevents_extraction_pushdown() {
    let (_tmp, sql_context) = setup_shredded_variant_table_with_rows().await;
    let sql = r#"
        SELECT
            payload,
            variant_get(payload, '$.age', 'int') AS age
        FROM paimon.test_db.t
        ORDER BY age
    "#;

    let df = sql_context.sql(sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let plan_text = displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        !plan_text.contains("PushedVariants="),
        "plan should keep full Variant reads when the query also projects payload, got:\n{plan_text}"
    );

    let batches = sql_context.sql(sql).await.unwrap().collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    let ages = batches[0]
        .column_by_name("age")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ages.values(), &[27, 32]);
}

#[tokio::test]
async fn data_evolution_row_id_survives_variant_extraction_pushdown() {
    let (_tmp, sql_context) = setup_data_evolution_shredded_variant_table_with_rows().await;
    let sql = r#"
        SELECT "_ROW_ID", variant_get(payload, '$.age', 'int') AS age
        FROM paimon.test_db.de_t
        ORDER BY "_ROW_ID"
    "#;

    let df = sql_context.sql(sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let plan_text = displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        plan_text.contains("PushedVariants=[payload=[$.age]]"),
        "plan should push variant extraction while preserving _ROW_ID, got:\n{plan_text}"
    );

    let batches = sql_context.sql(sql).await.unwrap().collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    let row_ids = batches[0]
        .column_by_name("_ROW_ID")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let ages = batches[0]
        .column_by_name("age")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    assert_eq!(row_ids.len(), 2);
    assert_eq!(ages.values(), &[27, 32]);
}

#[tokio::test]
async fn try_variant_get_invalid_path_pushdown_returns_null() {
    let (_tmp, sql_context) = setup_shredded_variant_table_with_rows().await;
    let sql = r#"
        SELECT try_variant_get(payload, 'invalid_path', 'int') AS value
        FROM paimon.test_db.t
        ORDER BY id
    "#;

    let df = sql_context.sql(sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let plan_text = displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        plan_text.contains("PushedVariants=[payload=[invalid_path]]"),
        "plan should push invalid-path try_variant_get extraction, got:\n{plan_text}"
    );

    let batches = sql_context.sql(sql).await.unwrap().collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    let values = batches[0]
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(values.len(), 2);
    assert!(values.is_null(0));
    assert!(values.is_null(1));
}

#[tokio::test]
async fn variant_get_path_with_semicolon_pushes_extraction() {
    let (_tmp, sql_context) = setup_shredded_variant_table_with_rows().await;
    let sql = r#"
        SELECT variant_get(payload, '$.a;b', 'int') AS value
        FROM paimon.test_db.t
        ORDER BY id
    "#;

    let df = sql_context.sql(sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let plan_text = displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        plan_text.contains("PushedVariants=[payload=[$.a;b]]"),
        "plan should push semicolon-path variant extraction, got:\n{plan_text}"
    );

    let batches = sql_context.sql(sql).await.unwrap().collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    let values = batches[0]
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(values.values(), &[11, 22]);
}
