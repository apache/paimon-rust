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

//! A partition-only filter is pushed down as `Exact`, so DataFusion keeps no
//! residual. These assert that the partition filter alone enforces the whole
//! predicate, including conjuncts a partition set cannot express.

mod common;

use common::setup_sql_context;
use paimon_datafusion::SQLContext;

const TABLE: &str = "paimon.test_db.t_partition_conjuncts";

async fn setup() -> (tempfile::TempDir, SQLContext) {
    let (tmp, sql_context) = setup_sql_context().await;
    sql_context
        .sql(&format!(
            "CREATE TABLE {TABLE} (dt STRING, id INT) PARTITIONED BY (dt)"
        ))
        .await
        .unwrap();
    sql_context
        .sql(&format!(
            "INSERT INTO {TABLE} VALUES \
             ('2024-01-01', 1), ('2024-01-02', 2), ('2024-01-03', 3), ('2024-01-04', 4)"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    (tmp, sql_context)
}

async fn partitions_matching(sql_context: &SQLContext, where_clause: &str) -> Vec<String> {
    let batches = sql_context
        .sql(&format!(
            "SELECT dt FROM {TABLE} WHERE {where_clause} ORDER BY dt"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut out = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            out.push(common::string_value(batch.column(0).as_ref(), row).to_string());
        }
    }
    out
}

async fn plan_of(sql_context: &SQLContext, where_clause: &str) -> String {
    let batches = sql_context
        .sql(&format!(
            "EXPLAIN SELECT dt FROM {TABLE} WHERE {where_clause}"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn contradictory_partition_conjuncts_match_nothing() {
    let (_tmp, sql_context) = setup().await;
    let where_clause = "dt = '2024-01-01' AND dt >= '2024-01-02'";

    let plan = plan_of(&sql_context, where_clause).await;
    assert!(
        !plan.contains("FilterExec"),
        "a partition-only filter is Exact both before and after this fix, \
         so the scan must be the only enforcement:\n{plan}"
    );

    assert!(
        partitions_matching(&sql_context, where_clause)
            .await
            .is_empty(),
        "no partition can be both = 2024-01-01 and >= 2024-01-02"
    );
}

/// The narrower `=` comes first, so the old second assignment overwrote it with
/// the wider `IN`. Order matters: with `IN` first the overwrite keeps the `=`
/// and the answer is accidentally right.
#[tokio::test]
async fn an_equality_before_a_wider_in_list_is_not_widened() {
    let (_tmp, sql_context) = setup().await;
    let where_clause =
        "dt = '2024-01-02' AND dt IN ('2024-01-01','2024-01-02','2024-01-03','2024-01-04')";

    assert_eq!(
        partitions_matching(&sql_context, where_clause).await,
        vec!["2024-01-02".to_string()]
    );
}

#[tokio::test]
async fn a_range_beside_an_in_list_still_narrows() {
    let (_tmp, sql_context) = setup().await;
    let where_clause =
        "dt >= '2024-01-03' AND dt IN ('2024-01-01','2024-01-02','2024-01-03','2024-01-04')";

    let plan = plan_of(&sql_context, where_clause).await;
    assert!(
        !plan.contains("FilterExec"),
        "a partition-only filter is Exact both before and after this fix, \
         so the scan must be the only enforcement:\n{plan}"
    );

    assert_eq!(
        partitions_matching(&sql_context, where_clause).await,
        vec!["2024-01-03".to_string(), "2024-01-04".to_string()]
    );
}
