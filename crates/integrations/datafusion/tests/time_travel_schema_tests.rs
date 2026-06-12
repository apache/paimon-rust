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

//! Time travel must read old snapshots with the snapshot's schema version,
//! not the latest one.

use std::sync::Arc;

use paimon::{CatalogOptions, FileSystemCatalog, Options};
use paimon_datafusion::SQLContext;
use tempfile::TempDir;

async fn create_sql_context(catalog: Arc<FileSystemCatalog>) -> SQLContext {
    let mut ctx = SQLContext::new();
    ctx.register_catalog("paimon", catalog).await.unwrap();
    ctx
}

/// Build a table with two schema versions and one snapshot per version:
/// snapshot 1 (schema 0: id, name) and snapshot 2 (schema 1: + age).
///
/// The second schema version is written directly as a `schema-1` file (column
/// DDL beyond options is not needed for this test and keeps it independent of
/// ALTER TABLE support).
async fn setup_evolved_table() -> (TempDir, SQLContext) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let warehouse = format!("file://{}", temp_dir.path().display());
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    let catalog = Arc::new(FileSystemCatalog::new(options).unwrap());
    let sql_context = create_sql_context(catalog).await;

    sql_context
        .sql("CREATE TABLE paimon.default.t (id INT, name STRING)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    sql_context
        .sql("INSERT INTO paimon.default.t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Evolve the schema: append an `age INT` column as schema-1.
    let schema_dir = temp_dir.path().join("default.db").join("t").join("schema");
    let schema0: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(schema_dir.join("schema-0")).unwrap())
            .unwrap();
    let mut schema1 = schema0.clone();
    schema1["id"] = serde_json::json!(1);
    schema1["fields"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({"id": 2, "name": "age", "type": "INT"}));
    schema1["highestFieldId"] = serde_json::json!(2);
    std::fs::write(
        schema_dir.join("schema-1"),
        serde_json::to_string(&schema1).unwrap(),
    )
    .unwrap();

    sql_context
        .sql("INSERT INTO paimon.default.t VALUES (4, 'd', 14), (5, 'e', 15)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    (temp_dir, sql_context)
}

fn column_names(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> Vec<String> {
    batches[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect()
}

fn total_rows(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

#[tokio::test]
async fn test_version_as_of_uses_snapshot_schema() {
    let (_tmp, sql_context) = setup_evolved_table().await;

    // Old snapshot: only the old columns, even with SELECT *.
    let batches = sql_context
        .sql("SELECT * FROM paimon.default.t VERSION AS OF 1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(column_names(&batches), vec!["id", "name"]);
    assert_eq!(total_rows(&batches), 3);

    // A column added later does not exist at snapshot 1.
    let err = sql_context
        .sql("SELECT age FROM paimon.default.t VERSION AS OF 1")
        .await
        .expect_err("selecting a column added after the snapshot should fail at planning");
    assert!(
        err.to_string().contains("age"),
        "error should mention the missing column: {err}"
    );

    // Latest read still sees the evolved schema and all rows.
    let batches = sql_context
        .sql("SELECT * FROM paimon.default.t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(column_names(&batches), vec!["id", "name", "age"]);
    assert_eq!(total_rows(&batches), 5);
}

#[tokio::test]
async fn test_session_scan_version_uses_snapshot_schema() {
    let (_tmp, sql_context) = setup_evolved_table().await;

    sql_context
        .sql("SET 'paimon.scan.version' = '1'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = sql_context
        .sql("SELECT * FROM paimon.default.t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(column_names(&batches), vec!["id", "name"]);
    assert_eq!(total_rows(&batches), 3);

    // Writing through a time-travelled table is rejected.
    let result = sql_context
        .sql("INSERT INTO paimon.default.t VALUES (6, 'f')")
        .await;
    let err = match result {
        Err(e) => e.to_string(),
        Ok(df) => match df.collect().await {
            Err(e) => e.to_string(),
            Ok(_) => panic!("INSERT into a time-travelled table should fail"),
        },
    };
    assert!(
        err.contains("time-travel option"),
        "error should mention time travel: {err}"
    );

    // Other write statements are rejected up front instead of silently
    // ignoring the active selector and writing to the latest state.
    for sql in [
        "UPDATE paimon.default.t SET name = 'x' WHERE id = 1",
        "DELETE FROM paimon.default.t WHERE id = 1",
        "TRUNCATE TABLE paimon.default.t",
    ] {
        let err = match sql_context.sql(sql).await {
            Err(e) => e.to_string(),
            Ok(df) => match df.collect().await {
                Err(e) => e.to_string(),
                Ok(_) => panic!("{sql} should fail while scan.version is set"),
            },
        };
        assert!(
            err.contains("time-travel option"),
            "{sql} should mention the active time-travel option: {err}"
        );
    }

    // A selector resolving to a snapshot with the current schema id pins the
    // read just the same, so INSERT stays rejected.
    sql_context
        .sql("SET 'paimon.scan.version' = '2'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let result = sql_context
        .sql("INSERT INTO paimon.default.t VALUES (6, 'f', 16)")
        .await;
    let err = match result {
        Err(e) => e.to_string(),
        Ok(df) => match df.collect().await {
            Err(e) => e.to_string(),
            Ok(_) => panic!("INSERT should fail while scan.version pins a same-schema snapshot"),
        },
    };
    assert!(
        err.contains("time-travel option"),
        "error should mention time travel: {err}"
    );

    sql_context
        .sql("RESET 'paimon.scan.version'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let batches = sql_context
        .sql("SELECT * FROM paimon.default.t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(column_names(&batches), vec!["id", "name", "age"]);
}

#[tokio::test]
async fn test_relation_planner_version_as_of_uses_snapshot_schema() {
    let (_tmp, sql_context) = setup_evolved_table().await;

    // Going through the raw SessionContext exercises PaimonRelationPlanner's
    // synchronous hook (and its runtime bridge) instead of the SQLContext
    // rewrite path. `VERSION AS OF` needs a dialect with table versioning.
    sql_context
        .ctx()
        .sql("SET datafusion.sql_parser.dialect = 'databricks'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let batches = sql_context
        .ctx()
        .sql("SELECT * FROM paimon.\"default\".t VERSION AS OF 1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(column_names(&batches), vec!["id", "name"]);
    assert_eq!(total_rows(&batches), 3);
}

#[tokio::test]
async fn test_timestamp_as_of_uses_snapshot_schema() {
    let (tmp, sql_context) = setup_evolved_table().await;

    // Both snapshots commit within milliseconds while `TIMESTAMP AS OF` has
    // second precision; rewrite snapshot 1's commit time to a fixed old value
    // so a timestamp between the two snapshots exists.
    let snapshot1_path = tmp
        .path()
        .join("default.db")
        .join("t")
        .join("snapshot")
        .join("snapshot-1");
    let mut snapshot1: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&snapshot1_path).unwrap()).unwrap();
    snapshot1["timeMillis"] = serde_json::json!(86_400_000u64); // 1970-01-02
    std::fs::write(&snapshot1_path, serde_json::to_string(&snapshot1).unwrap()).unwrap();

    let batches = sql_context
        .sql("SELECT * FROM paimon.default.t TIMESTAMP AS OF '1970-01-03 00:00:00'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(column_names(&batches), vec!["id", "name"]);
    assert_eq!(total_rows(&batches), 3);
}
