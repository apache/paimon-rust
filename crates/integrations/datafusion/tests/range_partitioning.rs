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

use std::sync::Arc;

use arrow_array::{Array, Int32Array};
use datafusion::common::ScalarValue;
use datafusion::physical_plan::{
    collect_partitioned, displayable, ExecutionPlan, ExecutionPlanProperties, Partitioning,
};
use paimon_datafusion::SQLContext;

fn scan(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    fn find(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
        if matches!(plan.name(), "PaimonTableScan" | "PaimonAuditLogScan") {
            return Some(Arc::clone(plan));
        }
        plan.children().into_iter().find_map(find)
    }
    find(plan).expect("Paimon scan")
}

async fn plan(ctx: &SQLContext, query: &str) -> Arc<dyn ExecutionPlan> {
    ctx.sql(query)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap()
}

#[tokio::test]
async fn integer_ranges_keep_equal_keys_together_and_avoid_aggregation_shuffle() {
    let (_tmp, ctx) = common::setup_sql_context().await;
    common::exec(&ctx, "SET datafusion.execution.target_partitions = 2").await;
    common::exec(
        &ctx,
        "CREATE TABLE paimon.test_db.ranges (id INT, pt INT, value INT) PARTITIONED BY (pt)",
    )
    .await;
    // Two commits put equal keys in distinct files/splits. Numeric order differs
    // from directory-name order, and NULL must occupy the final range.
    for values in [
        "(1, 10, 6), (2, -10, 2), (3, 2, 4), (4, NULL, 5)",
        "(5, 2, 40), (6, 10, 60), (7, -10, 20), (8, NULL, 50)",
    ] {
        common::exec(
            &ctx,
            &format!("INSERT INTO paimon.test_db.ranges VALUES {values}"),
        )
        .await;
    }

    let scan = scan(&plan(&ctx, "SELECT id, pt, value FROM paimon.test_db.ranges").await);
    let Partitioning::Range(range) = scan.output_partitioning() else {
        panic!(
            "expected range partitioning, got {:?}",
            scan.output_partitioning()
        );
    };
    assert_eq!(range.partition_count(), 2);
    assert_eq!(range.ordering()[0].expr.to_string(), "pt@1");
    assert_eq!(
        range.split_points()[0].values(),
        &[ScalarValue::Int32(Some(10))]
    );
    assert!(
        scan.output_ordering().is_none(),
        "ranges do not imply sorted rows"
    );
    let partitions = collect_partitioned(scan, ctx.ctx().task_ctx())
        .await
        .unwrap();
    let mut actual = Vec::new();
    for batches in partitions {
        let mut keys = Vec::new();
        for batch in batches {
            let pt = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            keys.extend((0..pt.len()).map(|i| (!pt.is_null(i)).then(|| pt.value(i))));
        }
        keys.sort_by_key(|key| (key.is_none(), *key));
        actual.push(keys);
    }
    assert_eq!(
        actual,
        vec![
            vec![Some(-10), Some(-10), Some(2), Some(2)],
            vec![Some(10), Some(10), None, None]
        ]
    );

    let query = "SELECT CAST(coalesce(pt, -100) AS INT) AS id, CAST(sum(value) AS INT) AS value FROM paimon.test_db.ranges GROUP BY pt";
    let physical = plan(&ctx, query).await;
    let text = displayable(physical.as_ref()).indent(true).to_string();
    assert!(!text.contains("RepartitionExec"), "{text}");
    assert_eq!(
        common::collect_id_value(&ctx, query).await,
        vec![(-100, 55), (-10, 22), (2, 44), (10, 66)]
    );
    for (predicate, expected) in [
        ("pt < 10", vec![(2, 2), (3, 4), (5, 40), (7, 20)]),
        ("pt >= 10", vec![(1, 6), (6, 60)]),
        ("pt IS NULL", vec![(4, 5), (8, 50)]),
    ] {
        assert_eq!(
            common::collect_id_value(
                &ctx,
                &format!("SELECT id, value FROM paimon.test_db.ranges WHERE {predicate}")
            )
            .await,
            expected
        );
    }
    // Local sorting is still required, even though the partitions have ranges.
    let sorted = plan(
        &ctx,
        "SELECT id, pt FROM paimon.test_db.ranges ORDER BY pt, id",
    )
    .await;
    assert!(displayable(sorted.as_ref())
        .indent(true)
        .to_string()
        .contains("SortExec"));
}

#[tokio::test]
async fn aggregation_of_non_primary_partition_keys_keeps_unknown_distribution() {
    let (_tmp, ctx) = common::setup_sql_context().await;
    common::exec(&ctx, "SET datafusion.execution.target_partitions = 2").await;
    common::exec(&ctx, "CREATE TABLE paimon.test_db.aggregated_ranges (id INT NOT NULL, pt INT, value INT, PRIMARY KEY (id)) PARTITIONED BY (pt) WITH ('bucket' = '-1', 'merge-engine' = 'aggregation', 'fields.pt.aggregate-function' = 'sum', 'fields.value.aggregate-function' = 'sum')").await;
    common::exec(
        &ctx,
        "INSERT INTO paimon.test_db.aggregated_ranges VALUES (1, 1, 10), (2, 2, 30)",
    )
    .await;
    common::exec(
        &ctx,
        "INSERT INTO paimon.test_db.aggregated_ranges VALUES (1, 1, 20)",
    )
    .await;
    let query = "SELECT pt AS id, CAST(sum(value) AS INT) AS value FROM paimon.test_db.aggregated_ranges GROUP BY pt";
    assert_eq!(common::collect_id_value(&ctx, query).await, vec![(2, 60)]);
    let physical = plan(
        &ctx,
        "SELECT id, pt, value FROM paimon.test_db.aggregated_ranges",
    )
    .await;
    assert!(matches!(
        scan(&physical).output_partitioning(),
        Partitioning::UnknownPartitioning(_)
    ));
}

#[tokio::test]
async fn partial_update_of_non_primary_partition_keys_keeps_unknown_distribution() {
    let (_tmp, ctx) = common::setup_sql_context().await;
    common::exec(&ctx, "SET datafusion.execution.target_partitions = 2").await;
    common::exec(&ctx, "CREATE TABLE paimon.test_db.partial_ranges (id INT NOT NULL, pt INT, ver INT, value INT, PRIMARY KEY (id)) PARTITIONED BY (pt) WITH ('bucket' = '-1', 'merge-engine' = 'partial-update', 'fields.ver.sequence-group' = 'pt,value', 'fields.pt.aggregate-function' = 'sum', 'fields.value.aggregate-function' = 'sum')").await;
    common::exec(
        &ctx,
        "INSERT INTO paimon.test_db.partial_ranges VALUES (1, 1, 1, 10), (2, 2, 1, 30)",
    )
    .await;
    common::exec(
        &ctx,
        "INSERT INTO paimon.test_db.partial_ranges VALUES (1, 1, 2, 20)",
    )
    .await;
    let query = "SELECT pt AS id, CAST(sum(value) AS INT) AS value FROM paimon.test_db.partial_ranges GROUP BY pt";
    assert_eq!(common::collect_id_value(&ctx, query).await, vec![(2, 60)]);
    let physical = plan(
        &ctx,
        "SELECT id, pt, value FROM paimon.test_db.partial_ranges",
    )
    .await;
    assert!(matches!(
        scan(&physical).output_partitioning(),
        Partitioning::UnknownPartitioning(_)
    ));
}

#[tokio::test]
async fn historically_aggregated_non_primary_partition_keys_keep_unknown_distribution() {
    let (_tmp, ctx) = common::setup_sql_context().await;
    common::exec(&ctx, "SET datafusion.execution.target_partitions = 2").await;
    common::exec(&ctx, "CREATE TABLE paimon.test_db.historical_ranges (id INT NOT NULL, pt INT, value INT, PRIMARY KEY (id)) PARTITIONED BY (pt) WITH ('bucket' = '-1', 'merge-engine' = 'aggregation', 'fields.pt.aggregate-function' = 'sum', 'fields.value.aggregate-function' = 'sum')").await;
    // Writer-side aggregation persists pt=2 in a file assigned to partition 1.
    common::exec(
        &ctx,
        "INSERT INTO paimon.test_db.historical_ranges VALUES (1, 1, 10), (1, 1, 20), (2, 2, 30)",
    )
    .await;
    common::exec(
        &ctx,
        "ALTER TABLE paimon.test_db.historical_ranges SET ('merge-engine' = 'deduplicate')",
    )
    .await;
    let query = "SELECT pt AS id, CAST(sum(value) AS INT) AS value FROM paimon.test_db.historical_ranges GROUP BY pt";
    assert_eq!(common::collect_id_value(&ctx, query).await, vec![(2, 60)]);
    let physical = plan(
        &ctx,
        "SELECT id, pt, value FROM paimon.test_db.historical_ranges",
    )
    .await;
    assert!(matches!(
        scan(&physical).output_partitioning(),
        Partitioning::UnknownPartitioning(_)
    ));
}

#[tokio::test]
async fn merge_engines_preserve_ranges_for_partition_columns_in_the_primary_key() {
    let (_tmp, ctx) = common::setup_sql_context().await;
    common::exec(&ctx, "SET datafusion.execution.target_partitions = 2").await;
    for (name, engine, extra) in [
        (
            "aggregation_full_pk",
            "aggregation",
            ", 'fields.pt.aggregate-function' = 'sum'",
        ),
        (
            "partial_full_pk",
            "partial-update",
            ", 'fields.ver.sequence-group' = 'value'",
        ),
    ] {
        common::exec(&ctx, &format!("CREATE TABLE paimon.test_db.{name} (id INT NOT NULL, pt INT NOT NULL, ver INT, value INT, PRIMARY KEY (pt, id)) PARTITIONED BY (pt) WITH ('bucket' = '1', 'merge-engine' = '{engine}', 'fields.value.aggregate-function' = 'sum'{extra})")).await;
        common::exec(
            &ctx,
            &format!("INSERT INTO paimon.test_db.{name} VALUES (1, 1, 1, 10), (2, 2, 1, 30)"),
        )
        .await;
        common::exec(
            &ctx,
            &format!("INSERT INTO paimon.test_db.{name} VALUES (1, 1, 2, 20)"),
        )
        .await;
        let query = format!("SELECT pt AS id, CAST(sum(value) AS INT) AS value FROM paimon.test_db.{name} GROUP BY pt");
        let physical = plan(&ctx, &query).await;
        let text = displayable(physical.as_ref()).indent(true).to_string();
        assert!(
            matches!(
                scan(&physical).output_partitioning(),
                Partitioning::Range(_)
            ),
            "{text}"
        );
        assert!(!text.contains("RepartitionExec"), "{text}");
        assert_eq!(
            common::collect_id_value(&ctx, &query).await,
            vec![(1, 30), (2, 30)],
            "{engine}"
        );
    }
}

#[tokio::test]
async fn joins_align_different_ranges_after_primary_key_updates() {
    let (_tmp, ctx) = common::setup_sql_context().await;
    common::exec(&ctx, "SET datafusion.execution.target_partitions = 2").await;
    common::exec(&ctx, "CREATE TABLE paimon.test_db.pk_ranges (id INT NOT NULL, pt INT NOT NULL, value INT, PRIMARY KEY (pt, id)) PARTITIONED BY (pt) WITH ('bucket' = '2')").await;
    common::exec(&ctx, "INSERT INTO paimon.test_db.pk_ranges VALUES (1, 1, 10), (2, 2, 20), (3, 3, 30), (4, 4, 40)").await;
    common::exec(
        &ctx,
        "INSERT INTO paimon.test_db.pk_ranges VALUES (2, 2, 200)",
    )
    .await;
    common::exec(
        &ctx,
        "CREATE TABLE paimon.test_db.other_ranges (pt INT, value INT) PARTITIONED BY (pt)",
    )
    .await;
    common::exec(&ctx, "INSERT INTO paimon.test_db.other_ranges VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500), (6, 600)").await;

    let left = scan(&plan(&ctx, "SELECT * FROM paimon.test_db.pk_ranges").await);
    let right = scan(&plan(&ctx, "SELECT * FROM paimon.test_db.other_ranges").await);
    let (Partitioning::Range(left), Partitioning::Range(right)) =
        (left.output_partitioning(), right.output_partitioning())
    else {
        panic!("both inputs must have ranges");
    };
    assert_eq!(
        left.split_points()[0].values(),
        &[ScalarValue::Int32(Some(3))]
    );
    assert_eq!(
        right.split_points()[0].values(),
        &[ScalarValue::Int32(Some(4))]
    );
    for prefer_hash in [true, false] {
        common::exec(
            &ctx,
            &format!("SET datafusion.optimizer.prefer_hash_join = {prefer_hash}"),
        )
        .await;
        common::exec(
            &ctx,
            "SET datafusion.optimizer.hash_join_single_partition_threshold = 0",
        )
        .await;
        common::exec(
            &ctx,
            "SET datafusion.optimizer.hash_join_single_partition_threshold_rows = 0",
        )
        .await;
        let query = "SELECT l.id, l.value + r.value AS value FROM paimon.test_db.pk_ranges l JOIN paimon.test_db.other_ranges r ON l.pt = r.pt";
        let physical = plan(&ctx, query).await;
        let text = displayable(physical.as_ref()).indent(true).to_string();
        let join = if prefer_hash {
            "HashJoinExec"
        } else {
            "SortMergeJoinExec"
        };
        assert!(text.contains(join), "{text}");
        assert!(
            text.contains("RepartitionExec"),
            "different boundaries need alignment: {text}"
        );
        assert_eq!(
            common::collect_id_value(&ctx, query).await,
            vec![(1, 110), (2, 400), (3, 330), (4, 440)]
        );
    }
}

#[tokio::test]
async fn compound_date_and_string_keys_preserve_lexicographic_ranges() {
    let (_tmp, ctx) = common::setup_sql_context().await;
    common::exec(&ctx, "SET datafusion.execution.target_partitions = 8").await;
    common::exec(&ctx, "CREATE TABLE paimon.test_db.compound (id INT, dt DATE, region STRING) PARTITIONED BY (dt, region)").await;
    common::exec(&ctx, "INSERT INTO paimon.test_db.compound VALUES (4, DATE '2026-01-01', '中'), (2, DATE '1969-12-31', 'z'), (1, DATE '1969-12-31', 'a'), (3, DATE '2026-01-01', NULL)").await;
    let physical = plan(&ctx, "SELECT region, id, dt FROM paimon.test_db.compound").await;
    let scan = scan(&physical);
    let Partitioning::Range(range) = scan.output_partitioning() else {
        panic!("expected compound ranges: {:?}", scan.output_partitioning());
    };
    assert_eq!(range.partition_count(), 4);
    assert_eq!(range.ordering()[0].expr.to_string(), "dt@1");
    assert_eq!(range.ordering()[1].expr.to_string(), "region@2");
    assert_eq!(
        range.split_points()[0].values(),
        &[
            ScalarValue::Date32(Some(-1)),
            ScalarValue::Utf8View(Some("z".into()))
        ]
    );
    let Partitioning::Range(projected) = physical.output_partitioning() else {
        panic!("projection must preserve ranges");
    };
    assert_eq!(projected.ordering()[0].expr.to_string(), "dt@2");
    assert_eq!(projected.ordering()[1].expr.to_string(), "region@0");
    let partitions = collect_partitioned(physical, ctx.ctx().task_ctx())
        .await
        .unwrap();
    let ids: Vec<Vec<i32>> = partitions
        .into_iter()
        .map(|batches| {
            batches
                .into_iter()
                .flat_map(|batch| {
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect()
        })
        .collect();
    assert_eq!(ids, vec![vec![1], vec![2], vec![4], vec![3]]);
    let query = "SELECT sum(id) FROM paimon.test_db.compound GROUP BY dt, region";
    let physical = plan(&ctx, query).await;
    let text = displayable(physical.as_ref()).indent(true).to_string();
    assert!(!text.contains("RepartitionExec"), "{text}");
    assert_eq!(common::row_count(&ctx, query).await, 4);
}

#[tokio::test]
async fn supported_scalar_partition_types_use_typed_order_and_nulls_last() {
    let (_tmp, ctx) = common::setup_sql_context().await;
    common::exec(&ctx, "SET datafusion.execution.target_partitions = 8").await;
    for (name, data_type, low, high) in [
        ("bools", "BOOLEAN", "false", "true"),
        ("tiny", "TINYINT", "-10", "2"),
        ("small", "SMALLINT", "-10", "2"),
        ("big", "BIGINT", "-100000000000", "200000000000"),
        (
            "decimal",
            "DECIMAL(20,3)",
            "-12345678901234567.890",
            "2.001",
        ),
    ] {
        common::exec(
            &ctx,
            &format!(
                "CREATE TABLE paimon.test_db.{name} (id INT, pt {data_type}) PARTITIONED BY (pt)"
            ),
        )
        .await;
        common::exec(
            &ctx,
            &format!("INSERT INTO paimon.test_db.{name} VALUES (2, {high}), (3, NULL), (1, {low})"),
        )
        .await;
        let scan = scan(&plan(&ctx, &format!("SELECT id, pt FROM paimon.test_db.{name}")).await);
        assert!(
            matches!(scan.output_partitioning(), Partitioning::Range(_)),
            "{name}: {:?}",
            scan.output_partitioning()
        );
        let partitions = collect_partitioned(scan, ctx.ctx().task_ctx())
            .await
            .unwrap();
        let ids: Vec<Vec<i32>> = partitions
            .into_iter()
            .map(|batches| {
                batches
                    .into_iter()
                    .flat_map(|batch| {
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .values()
                            .to_vec()
                    })
                    .collect()
            })
            .collect();
        assert_eq!(ids, vec![vec![1], vec![2], vec![3]], "{name}");
    }
}

#[tokio::test]
async fn variant_pushdown_and_audit_scans_preserve_partition_ranges() {
    let (_tmp, ctx) = common::setup_sql_context().await;
    common::exec(&ctx, "SET datafusion.execution.target_partitions = 2").await;
    common::exec(&ctx, r#"CREATE TABLE paimon.test_db.variants (pt INT, payload VARIANT) PARTITIONED BY (pt) WITH ('variant.shreddingSchema' = '{"type":"ROW","fields":[{"name":"payload","type":{"type":"ROW","fields":[{"name":"value","type":"INT"}]}}]}')"#).await;
    common::exec(&ctx, r#"INSERT INTO paimon.test_db.variants SELECT 2, parse_json('{"value":20}') UNION ALL SELECT 1, parse_json('{"value":10}')"#).await;
    let query = "SELECT pt AS id, CAST(variant_get(payload, '$.value', 'int') AS INT) AS value FROM paimon.test_db.variants";
    let physical = plan(&ctx, query).await;
    let text = displayable(physical.as_ref()).indent(true).to_string();
    assert!(text.contains("PushedVariants"), "{text}");
    assert!(text.contains("partitioning=Range"), "{text}");
    assert!(matches!(
        scan(&physical).output_partitioning(),
        Partitioning::Range(_)
    ));
    assert_eq!(
        common::collect_id_value(&ctx, query).await,
        vec![(1, 10), (2, 20)]
    );

    let audit = "SELECT pt FROM paimon.test_db.variants$audit_log";
    let scan = scan(&plan(&ctx, audit).await);
    assert_eq!(scan.name(), "PaimonAuditLogScan");
    assert!(matches!(scan.output_partitioning(), Partitioning::Range(_)));
    assert_eq!(common::row_count(&ctx, audit).await, 2);
}

#[tokio::test]
async fn unsupported_or_unavailable_partition_keys_keep_unknown_partitioning() {
    let (_tmp, ctx) = common::setup_sql_context().await;
    common::exec(&ctx, "SET datafusion.execution.target_partitions = 2").await;
    for (name, definition, partitioning, values, columns) in [
        ("plain", "id INT, pt INT", "", "(1, 10), (2, 20)", "id, pt"),
        (
            "floats",
            "id INT, pt DOUBLE",
            "PARTITIONED BY (pt)",
            "(1, -1.0), (2, 2.0)",
            "id, pt",
        ),
        (
            "timestamps",
            "id INT, pt TIMESTAMP",
            "PARTITIONED BY (pt)",
            "(1, TIMESTAMP '1969-12-31 23:59:59'), (2, TIMESTAMP '2026-01-01 00:00:00')",
            "id, pt",
        ),
        (
            "projection",
            "id INT, pt INT",
            "PARTITIONED BY (pt)",
            "(1, 10), (2, 20)",
            "id",
        ),
        (
            "partial_key",
            "id INT, pt INT, region STRING",
            "PARTITIONED BY (pt, region)",
            "(1, 10, 'a'), (2, 20, 'b')",
            "id, pt",
        ),
    ] {
        common::exec(
            &ctx,
            &format!("CREATE TABLE paimon.test_db.{name} ({definition}) {partitioning}"),
        )
        .await;
        common::exec(
            &ctx,
            &format!("INSERT INTO paimon.test_db.{name} VALUES {values}"),
        )
        .await;
        let query = format!("SELECT {columns} FROM paimon.test_db.{name}");
        let scan = scan(&plan(&ctx, &query).await);
        assert!(
            matches!(
                scan.output_partitioning(),
                Partitioning::UnknownPartitioning(_)
            ),
            "{name}: {:?}",
            scan.output_partitioning()
        );
        assert_eq!(common::row_count(&ctx, &query).await, 2);
    }
    common::exec(
        &ctx,
        "CREATE TABLE paimon.test_db.empty_ranges (pt INT) PARTITIONED BY (pt)",
    )
    .await;
    let scan = scan(&plan(&ctx, "SELECT pt FROM paimon.test_db.empty_ranges").await);
    assert!(matches!(
        scan.output_partitioning(),
        Partitioning::UnknownPartitioning(1)
    ));
}
