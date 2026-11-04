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

use datafusion::common::JoinType;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::collect;
use std::sync::Arc;

fn join_type(plan: &LogicalPlan) -> Option<JoinType> {
    if let LogicalPlan::Join(join) = plan {
        return Some(join.join_type);
    }
    plan.inputs().into_iter().find_map(join_type)
}

#[tokio::test]
async fn strict_udfs_enable_outer_join_elimination_without_changing_variant_null() {
    let (_tmp, ctx) = common::setup_sql_context().await;
    for function in [
        "path_to_descriptor(r.path)",
        "descriptor_to_string(path_to_descriptor(r.path))",
        "parse_json(r.json)",
        "try_parse_json(r.json)",
        "variant_get(parse_json(r.json), '$.a')",
        "try_variant_get(parse_json(r.json), '$.a')",
        "blob_view('test_db.metrics', 0, r.id)",
    ] {
        let sql = format!("SELECT l.id FROM (VALUES (1), (2)) l(id) LEFT JOIN (VALUES (1, 'x', '{{\"a\": 1}}')) r(id, path, json) ON l.id = r.id WHERE {function} IS NOT NULL");
        let frame = ctx.sql(&sql).await.unwrap();
        let plan = frame.into_optimized_plan().unwrap();
        assert_eq!(
            join_type(&plan),
            Some(JoinType::Inner),
            "{function}: {plan:?}"
        );
        assert_eq!(common::row_count(&ctx, &sql).await, 1);
    }
    let sql = "SELECT l.id FROM (VALUES (1), (2)) l(id) LEFT JOIN (VALUES (1, 'null')) r(id, json) ON l.id = r.id WHERE NOT is_variant_null(parse_json(r.json))";
    assert_eq!(common::row_count(&ctx, sql).await, 1);
}

#[tokio::test]
async fn explain_pgjson_reports_actual_scan_output_rows() {
    let (_tmp, ctx) = common::setup_sql_context().await;
    common::exec(&ctx, "CREATE TABLE paimon.test_db.metrics (id INT)").await;
    common::exec(
        &ctx,
        "INSERT INTO paimon.test_db.metrics VALUES (1), (2), (3)",
    )
    .await;
    for (relation, scan_name, expected_scan_rows) in [
        ("metrics", "PaimonTableScan", 2),
        ("metrics$audit_log", "PaimonAuditLogScan", 2),
    ] {
        let query = format!("SELECT id FROM paimon.test_db.{relation} WHERE id > 1");
        let frame = ctx.sql(&query).await.unwrap();
        let plan = frame.create_physical_plan().await.unwrap();
        let batches = collect(Arc::clone(&plan), Arc::new(TaskContext::default()))
            .await
            .unwrap();
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            2
        );
        fn scan_rows(
            plan: &dyn datafusion::physical_plan::ExecutionPlan,
            name: &str,
        ) -> Option<usize> {
            if plan.name() == name {
                return plan.metrics().and_then(|metrics| metrics.output_rows());
            }
            plan.children()
                .into_iter()
                .find_map(|child| scan_rows(child.as_ref(), name))
        }
        assert_eq!(
            scan_rows(plan.as_ref(), scan_name),
            Some(expected_scan_rows)
        );
        let output = ctx
            .sql(&format!(
                "EXPLAIN (ANALYZE, FORMAT pgjson, METRICS 'rows') {query}"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_str(common::string_value(
            output[0].column_by_name("plan").unwrap().as_ref(),
            0,
        ))
        .unwrap();
        fn scan_node<'a>(
            value: &'a serde_json::Value,
            name: &str,
        ) -> Option<&'a serde_json::Value> {
            match value {
                serde_json::Value::Object(values) => {
                    if values
                        .get("Node Type")
                        .and_then(|v| v.as_str())
                        .is_some_and(|v| v.contains(name))
                    {
                        return Some(value);
                    }
                    values.values().find_map(|v| scan_node(v, name))
                }
                serde_json::Value::Array(values) => values.iter().find_map(|v| scan_node(v, name)),
                _ => None,
            }
        }
        let scan = scan_node(&value, scan_name).expect("Paimon scan in pgjson explain");
        assert_eq!(scan["Actual Rows"], expected_scan_rows, "{value}");
    }
}

#[tokio::test]
async fn new_array_functions_run_on_paimon_columns() {
    let (_tmp, ctx) = common::setup_sql_context().await;
    common::exec(
        &ctx,
        "CREATE TABLE paimon.test_db.arrays (id INT, values_ ARRAY<INT>)",
    )
    .await;
    common::exec(
        &ctx,
        "INSERT INTO paimon.test_db.arrays VALUES (1, [1, 2, 3]), (2, [])",
    )
    .await;
    let rows = common::collect_id_value(&ctx,
        "SELECT id, CAST(array_sum(array_scale(values_, 2)) AS INT) AS value FROM paimon.test_db.arrays WHERE id = 1").await;
    assert_eq!(rows, vec![(1, 12)]);
    assert_eq!(
        common::row_count(
            &ctx,
            "SELECT unnest_outer(values_) FROM paimon.test_db.arrays"
        )
        .await,
        4
    );
}
