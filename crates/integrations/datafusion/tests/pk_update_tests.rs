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

use common::{assert_sql_error, create_sql_context, create_test_env, dml_count, exec, files_under};
use datafusion::arrow::array::{Array, Int32Array};
use paimon::catalog::Identifier;
use paimon::{Catalog, FileSystemCatalog};
use paimon_datafusion::SQLContext;
use std::sync::Arc;

async fn setup(
    bucket: i32,
    options: &str,
) -> (tempfile::TempDir, Arc<FileSystemCatalog>, SQLContext) {
    let (tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;
    exec(&ctx, "CREATE SCHEMA paimon.test_db").await;
    exec(
        &ctx,
        &format!(
            "CREATE TABLE paimon.test_db.t (id INT NOT NULL, v INT, w INT, PRIMARY KEY (id)) \
         WITH ('bucket' = '{bucket}', 'write-only' = 'true'{options})"
        ),
    )
    .await;
    (tmp, catalog, ctx)
}

async fn rows(ctx: &SQLContext) -> Vec<(i32, Option<i32>, Option<i32>)> {
    let batches = ctx
        .sql("SELECT id, v, w FROM paimon.test_db.t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let other = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        rows.extend((0..batch.num_rows()).map(|i| {
            (
                ids.value(i),
                values.is_valid(i).then(|| values.value(i)),
                other.is_valid(i).then(|| other.value(i)),
            )
        }));
    }
    rows.sort_unstable();
    rows
}

async fn snapshot(catalog: &FileSystemCatalog) -> Option<i64> {
    catalog
        .get_table(&Identifier::new("test_db", "t"))
        .await
        .unwrap()
        .snapshot_manager()
        .get_latest_snapshot_id()
        .await
        .unwrap()
}

#[tokio::test]
async fn primary_key_update_uses_logical_rows_and_original_assignment_values() {
    for bucket in [2, -1] {
        let (_tmp, catalog, ctx) = setup(bucket, "").await;
        exec(
            &ctx,
            "INSERT INTO paimon.test_db.t VALUES (1, 10, 100), (2, 20, 200), (3, NULL, 300), (4, 40, 400)",
        )
        .await;
        exec(&ctx, "INSERT INTO paimon.test_db.t VALUES (1, 11, 101)").await;
        assert_eq!(
            dml_count(&ctx, "DELETE FROM paimon.test_db.t WHERE id = 4").await,
            1
        );
        let before = snapshot(&catalog).await;
        for predicate in ["v = 10", "id = 4"] {
            assert_eq!(
                dml_count(
                    &ctx,
                    &format!("UPDATE paimon.test_db.t SET w = 0 WHERE {predicate}")
                )
                .await,
                0
            );
            assert_eq!(snapshot(&catalog).await, before);
        }
        assert_eq!(
            dml_count(
                &ctx,
                "UPDATE paimon.test_db.t SET v = v + 1, w = v WHERE t.v <> 11"
            )
            .await,
            1
        );
        assert_eq!(
            rows(&ctx).await,
            vec![
                (1, Some(11), Some(101)),
                (2, Some(21), Some(20)),
                (3, None, Some(300))
            ]
        );
        assert_eq!(
            dml_count(&ctx, "UPDATE paimon.test_db.t SET v = 5 WHERE v IS NULL").await,
            1
        );
        for expected in [
            vec![
                (1, Some(101), Some(11)),
                (2, Some(20), Some(21)),
                (3, Some(300), Some(5)),
            ],
            vec![
                (1, Some(11), Some(101)),
                (2, Some(21), Some(20)),
                (3, Some(5), Some(300)),
            ],
        ] {
            assert_eq!(
                dml_count(&ctx, "UPDATE paimon.test_db.t SET v = w, w = v").await,
                3
            );
            assert_eq!(rows(&ctx).await, expected);
        }
        let before_rows = rows(&ctx).await;
        assert_eq!(
            dml_count(&ctx, "UPDATE paimon.test_db.t SET v = v").await,
            3
        );
        assert_eq!(rows(&ctx).await, before_rows);
    }
}

#[tokio::test]
async fn primary_key_update_validates_assignments_before_writing() {
    let (tmp, catalog, ctx) = setup(2, "").await;
    let before_files = files_under(tmp.path());
    assert_eq!(
        dml_count(&ctx, "UPDATE paimon.test_db.t SET v = 1").await,
        0
    );
    assert_eq!(snapshot(&catalog).await, None);
    assert_eq!(files_under(tmp.path()), before_files);
    for populated in [false, true] {
        if populated {
            exec(&ctx, "INSERT INTO paimon.test_db.t VALUES (1, 10, 100)").await;
        }
        let before = snapshot(&catalog).await;
        let before_rows = rows(&ctx).await;
        let before_files = files_under(tmp.path());
        for (sql, error) in [
            ("UPDATE paimon.test_db.t SET missing = 1", "Unknown column"),
            ("UPDATE paimon.test_db.t SET \"V\" = 1", "Unknown column"),
            (
                "UPDATE paimon.test_db.t SET v = v, V = 1",
                "Duplicate column",
            ),
            (
                "UPDATE paimon.test_db.t SET v = no_such_column",
                "no_such_column",
            ),
        ] {
            assert_sql_error(&ctx, sql, error).await;
            assert_eq!(snapshot(&catalog).await, before);
            assert_eq!(rows(&ctx).await, before_rows);
            assert_eq!(files_under(tmp.path()), before_files);
        }
    }
}

#[tokio::test]
async fn primary_key_update_preserves_partition_composite_key_and_quoted_strings() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog).await;
    exec(&ctx, "CREATE SCHEMA paimon.test_db").await;
    exec(&ctx, "CREATE TABLE paimon.test_db.\"odd.name\" (\"Key.Id\" INT NOT NULL, \"Key.Part\" STRING NOT NULL, pt INT NOT NULL, \"Value.Name\" STRING, untouched STRING, PRIMARY KEY (\"Key.Id\", \"Key.Part\", pt)) PARTITIONED BY (pt) WITH ('bucket' = '2')").await;
    exec(
        &ctx,
        "INSERT INTO paimon.test_db.\"odd.name\" VALUES (1, 'a', 1, 'first', 'keep-first'), (1, 'b', 1, 'second', 'keep-second'), (1, 'a', 2, 'third', 'keep-third'), (2, 'a', 1, 'fourth', 'keep-fourth')",
    )
    .await;
    assert_eq!(dml_count(&ctx, "UPDATE paimon.test_db.\"odd.name\" SET \"Value.Name\" = concat(\"Value.Name\", '_new') WHERE \"Key.Id\" = 1 AND \"Key.Part\" = 'a' AND pt = 1").await, 1);
    assert_eq!(
        common::collect_id_name(
            &ctx,
            "SELECT \"Key.Id\" AS id, concat(\"Key.Part\", ':', pt, ':', \"Value.Name\", ':', untouched) AS name FROM paimon.test_db.\"odd.name\" ORDER BY id, name"
        )
        .await,
        vec![
            (1, "a:1:first_new:keep-first".into()),
            (1, "a:2:third:keep-third".into()),
            (1, "b:1:second:keep-second".into()),
            (2, "a:1:fourth:keep-fourth".into()),
        ]
    );
    for sql in [
        "UPDATE paimon.test_db.\"odd.name\" SET \"Key.Id\" = 3",
        "UPDATE paimon.test_db.\"odd.name\" SET \"Key.Part\" = 'c'",
        "UPDATE paimon.test_db.\"odd.name\" SET pt = 3",
    ] {
        assert_sql_error(&ctx, sql, "cannot modify primary-key or partition column").await;
    }
}

#[tokio::test]
async fn primary_key_update_supports_predicate_and_assignment_subqueries() {
    let (_tmp, _catalog, ctx) = setup(2, "").await;
    exec(
        &ctx,
        "INSERT INTO paimon.test_db.t VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300)",
    )
    .await;
    assert_eq!(dml_count(&ctx, "UPDATE paimon.test_db.t SET w = (SELECT MAX(v) FROM paimon.test_db.t) WHERE id IN (SELECT id FROM paimon.test_db.t WHERE v = 20)").await, 1);
    exec(&ctx, "CREATE TEMPORARY VIEW selected_ids AS SELECT 3 AS id").await;
    assert_eq!(dml_count(&ctx, "UPDATE paimon.test_db.t SET w = w + 1 WHERE EXISTS (SELECT 1 FROM selected_ids s WHERE s.id = t.id)").await, 1);
    assert_eq!(
        rows(&ctx).await,
        vec![
            (1, Some(10), Some(100)),
            (2, Some(20), Some(30)),
            (3, Some(30), Some(301))
        ]
    );
}

#[tokio::test]
async fn primary_key_update_preserves_and_assigns_sequence_fields() {
    for (options, losing, winning) in [
        (", 'sequence.field' = 'v'", 99, 101),
        (
            ", 'sequence.field' = 'v', 'sequence.field.sort-order' = 'descending'",
            101,
            99,
        ),
    ] {
        let (_tmp, _catalog, ctx) = setup(2, options).await;
        exec(&ctx, "INSERT INTO paimon.test_db.t VALUES (1, 100, 10)").await;
        assert_eq!(
            dml_count(&ctx, "UPDATE paimon.test_db.t SET w = 11").await,
            1
        );
        assert_eq!(rows(&ctx).await, vec![(1, Some(100), Some(11))]);
        assert_eq!(
            dml_count(
                &ctx,
                &format!("UPDATE paimon.test_db.t SET v = {losing}, w = 12")
            )
            .await,
            1
        );
        assert_eq!(rows(&ctx).await, vec![(1, Some(100), Some(11))]);
        assert_eq!(
            dml_count(
                &ctx,
                &format!("UPDATE paimon.test_db.t SET v = {winning}, w = 13")
            )
            .await,
            1
        );
        assert_eq!(rows(&ctx).await, vec![(1, Some(winning), Some(13))]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn primary_key_update_pins_snapshot_and_respects_concurrent_upserts() {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::{create_udf, Volatility};
    use std::sync::atomic::{AtomicUsize, Ordering};
    for (options, expected) in [
        ("", (Some(10), Some(101))),
        (", 'sequence.field' = 'v'", (Some(20), Some(200))),
        (
            ", 'sequence.field' = 'v', 'sequence.field.sort-order' = 'descending'",
            (Some(10), Some(101)),
        ),
    ] {
        let (_tmp, _catalog, ctx) = setup(2, options).await;
        let ctx = Arc::new(ctx);
        exec(&ctx, "INSERT INTO paimon.test_db.t VALUES (1, 10, 100)").await;
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let weak = Arc::downgrade(&ctx);
        ctx.ctx().register_udf(create_udf(
            "interfere",
            vec![DataType::Int32],
            DataType::Int32,
            Volatility::Volatile,
            Arc::new(move |args| {
                if counted.fetch_add(1, Ordering::SeqCst) == 0 {
                    let ctx = weak.upgrade().unwrap();
                    tokio::task::block_in_place(|| {
                        tokio::runtime::Handle::current().block_on(async {
                            ctx.sql("INSERT INTO paimon.test_db.t VALUES (1, 20, 200), (2, 10, 10)")
                                .await?
                                .collect()
                                .await
                        })
                    })?;
                }
                Ok(args[0].clone())
            }),
        ));
        let sql = "UPDATE paimon.test_db.t SET w = interfere(w) + 1";
        assert_eq!(dml_count(&ctx, sql).await, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            rows(&ctx).await,
            vec![(1, expected.0, expected.1), (2, Some(10), Some(10))]
        );
        assert_eq!(dml_count(&ctx, sql).await, 2);
        assert!(calls.load(Ordering::SeqCst) > 1);
        assert_eq!(
            rows(&ctx).await,
            vec![
                (1, expected.0, expected.1.map(|value| value + 1)),
                (2, Some(10), Some(11)),
            ]
        );
    }
}

#[tokio::test]
async fn primary_key_update_rejects_unsupported_targets_and_syntax() {
    for engine in ["partial-update", "aggregation", "first-row"] {
        let (_tmp, _catalog, ctx) = setup(2, &format!(", 'merge-engine' = '{engine}'")).await;
        assert_sql_error(
            &ctx,
            "UPDATE paimon.test_db.t SET v = 1",
            "requires merge-engine=deduplicate",
        )
        .await;
    }
    let (_tmp, catalog, ctx) = setup(2, "").await;
    exec(&ctx, "INSERT INTO paimon.test_db.t VALUES (1, 10, 100)").await;
    let before = snapshot(&catalog).await;
    for (sql, error) in [
        ("UPDATE paimon.test_db.t SET id = 2", "cannot modify"),
        (
            "UPDATE paimon.test_db.t SET t.v = 2",
            "top-level SET columns",
        ),
        (
            "UPDATE paimon.test_db.t SET (v, w) = (1, 2)",
            "top-level SET columns",
        ),
        ("UPDATE paimon.test_db.t AS x SET v = 1", "alias"),
        (
            "UPDATE paimon.test_db.t SET v = 1 RETURNING id",
            "optional WHERE",
        ),
        (
            "UPDATE paimon.test_db.t SET v = 1 FROM paimon.test_db.t other WHERE t.id = other.id",
            "optional WHERE",
        ),
        (
            "UPDATE paimon.test_db.t JOIN paimon.test_db.t other ON t.id = other.id SET v = 1",
            "optional WHERE",
        ),
        (
            "UPDATE paimon.test_db.t SET v = 1 OUTPUT inserted.id",
            "optional WHERE",
        ),
        (
            "UPDATE OR IGNORE paimon.test_db.t SET v = 1",
            "optional WHERE",
        ),
        (
            "UPDATE paimon.test_db.t WITH (NOLOCK) SET v = 1",
            "optional WHERE",
        ),
        (
            "UPDATE paimon.test_db.t SET v = 1 ORDER BY id LIMIT 1",
            "optional WHERE",
        ),
        (
            "UPDATE paimon.test_db.t SET v = 1 LIMIT 1",
            "optional WHERE",
        ),
    ] {
        assert_sql_error(&ctx, sql, error).await;
    }
    exec(
        &ctx,
        "CREATE TEMPORARY VIEW paimon.test_db.t AS SELECT 99 AS id",
    )
    .await;
    assert_sql_error(&ctx, "UPDATE paimon.test_db.t SET v = 1", "temporary").await;
    exec(&ctx, "DROP TEMPORARY VIEW paimon.test_db.t").await;
    assert_sql_error(
        &ctx,
        "UPDATE paimon.test_db.t$branch_dev SET v = 1",
        "branch",
    )
    .await;
    exec(&ctx, "SET 'paimon.scan.snapshot-id' = '1'").await;
    assert_sql_error(
        &ctx,
        "UPDATE paimon.test_db.t SET v = 1",
        "time-travel option",
    )
    .await;
    exec(&ctx, "RESET 'paimon.scan.snapshot-id'").await;
    exec(&ctx, "SET 'paimon.incremental-between-timestamp' = '0,1'").await;
    assert_sql_error(
        &ctx,
        "UPDATE paimon.test_db.t SET v = 1",
        "historical/incremental read options",
    )
    .await;
    exec(&ctx, "RESET 'paimon.incremental-between-timestamp'").await;
    assert_eq!(snapshot(&catalog).await, before);
    assert_eq!(rows(&ctx).await, vec![(1, Some(10), Some(100))]);
}

#[tokio::test]
async fn primary_key_update_rejects_overridden_read_semantics() {
    for (option, value) in [
        ("sequence.field", "v"),
        ("bucket", "-1"),
        ("deletion-vectors.enabled", "true"),
    ] {
        let (_tmp, catalog, ctx) = setup(2, "").await;
        exec(&ctx, "INSERT INTO paimon.test_db.t VALUES (1, 20, 100)").await;
        exec(&ctx, "INSERT INTO paimon.test_db.t VALUES (1, 10, 100)").await;
        let before = snapshot(&catalog).await;
        exec(&ctx, &format!("SET 'paimon.{option}' = '{value}'")).await;
        assert_sql_error(&ctx, "UPDATE paimon.test_db.t SET w = 1", "read options").await;
        exec(
            &ctx,
            "CREATE TEMPORARY VIEW captured_target AS SELECT * FROM paimon.test_db.t",
        )
        .await;
        exec(&ctx, &format!("RESET 'paimon.{option}'")).await;
        assert_sql_error(
            &ctx,
            "UPDATE paimon.test_db.t SET w = (SELECT MIN(w) FROM captured_target)",
            "read options",
        )
        .await;
        assert_eq!(snapshot(&catalog).await, before);
        assert_eq!(rows(&ctx).await, vec![(1, Some(10), Some(100))]);
    }
}

#[tokio::test]
async fn primary_key_update_discards_partial_output_when_an_expression_fails() {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::error::DataFusionError;
    use datafusion::logical_expr::{create_udf, Volatility};
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (tmp, catalog, ctx) = setup(
        2,
        ", 'read.batch-size' = '1', 'write.parquet-buffer-size' = '1'",
    )
    .await;
    exec(&ctx, "SET datafusion.execution.target_partitions = 1").await;
    exec(&ctx, "SET datafusion.execution.batch_size = 1").await;
    exec(&ctx, "INSERT INTO paimon.test_db.t VALUES (1, 10, 100)").await;
    exec(&ctx, "INSERT INTO paimon.test_db.t VALUES (2, 20, 200)").await;
    let before_files = files_under(tmp.path());
    let before_snapshot = snapshot(&catalog).await;
    let root = tmp.path().to_path_buf();
    let initial_files = before_files.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    ctx.ctx().register_udf(create_udf(
        "fail_after_batch",
        vec![DataType::Int32],
        DataType::Int32,
        Volatility::Volatile,
        Arc::new(move |args| {
            if counted.fetch_add(1, Ordering::SeqCst) > 0 {
                assert!(files_under(&root)
                    .difference(&initial_files)
                    .any(|path| path.extension().is_some_and(|ext| ext == "parquet")));
                return Err(DataFusionError::Execution("injected SET failure".into()));
            }
            Ok(args[0].clone())
        }),
    ));
    assert_sql_error(
        &ctx,
        "UPDATE paimon.test_db.t SET w = fail_after_batch(w) + 1",
        "injected SET failure",
    )
    .await;
    assert!(calls.load(Ordering::SeqCst) >= 2);
    assert_eq!(snapshot(&catalog).await, before_snapshot);
    assert_eq!(files_under(tmp.path()), before_files);
    assert_eq!(
        rows(&ctx).await,
        vec![(1, Some(10), Some(100)), (2, Some(20), Some(200))]
    );
}

#[tokio::test]
async fn primary_key_update_writes_update_after_and_coerces_assignments() {
    for options in [
        ", 'changelog-producer' = 'input'",
        ", 'ignore-delete' = 'true'",
    ] {
        let (_tmp, _catalog, ctx) = setup(2, options).await;
        exec(&ctx, "INSERT INTO paimon.test_db.t VALUES (1, 10, 100)").await;
        assert_eq!(
            dml_count(
                &ctx,
                "UPDATE paimon.test_db.t SET v = CAST(12 AS BIGINT), w = NULL"
            )
            .await,
            1
        );
        assert_eq!(rows(&ctx).await, vec![(1, Some(12), None)]);
        assert_eq!(
            common::collect_id_name(
                &ctx,
                "SELECT id, rowkind AS name FROM paimon.test_db.t$audit_log"
            )
            .await,
            vec![(1, "+U".into())]
        );
    }
}

#[tokio::test]
async fn primary_key_update_merges_uncompacted_deletion_vector_versions() {
    use paimon::{CatalogOptions, Options};
    for merge_on_read in [false, true] {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut options = Options::new();
        options.set(
            CatalogOptions::WAREHOUSE,
            format!("file://{}", tmp.path().join("warehouse").display()),
        );
        options.set("local-cache.enabled", "true");
        options.set(
            "local-cache.dir",
            tmp.path().join("cache").to_str().unwrap(),
        );
        let catalog = Arc::new(FileSystemCatalog::new(options).unwrap());
        let ctx = create_sql_context(catalog.clone()).await;
        exec(&ctx, "CREATE SCHEMA paimon.test_db").await;
        exec(&ctx, &format!(
            "CREATE TABLE paimon.test_db.t (id INT NOT NULL, v INT, w INT, PRIMARY KEY (id)) \
             WITH ('bucket' = '2', 'write-only' = 'true', 'deletion-vectors.enabled' = 'true', 'deletion-vectors.merge-on-read' = '{merge_on_read}')"
        )).await;
        exec(
            &ctx,
            "INSERT INTO paimon.test_db.t VALUES (1, 10, 100), (2, 30, 300)",
        )
        .await;
        exec(&ctx, "CALL sys.compact(table => 'test_db.t')").await;
        exec(
            &ctx,
            "INSERT INTO paimon.test_db.t VALUES (1, 20, 200), (3, 40, 400)",
        )
        .await;
        let before = snapshot(&catalog).await;
        assert_eq!(
            dml_count(&ctx, "UPDATE paimon.test_db.t SET w = 0 WHERE v = 10").await,
            0
        );
        assert_eq!(snapshot(&catalog).await, before);
        assert_eq!(dml_count(&ctx, "UPDATE paimon.test_db.t SET w = (SELECT MAX(w) FROM paimon.test_db.t) WHERE id IN (SELECT id FROM paimon.test_db.t WHERE v = 20)").await, 1);
        exec(&ctx, "SET 'paimon.deletion-vectors.merge-on-read' = 'true'").await;
        assert_eq!(
            rows(&ctx).await,
            vec![
                (1, Some(20), Some(400)),
                (2, Some(30), Some(300)),
                (3, Some(40), Some(400))
            ]
        );
    }
}

#[tokio::test]
async fn primary_key_update_respects_lexicographic_sequence_order() {
    for (order, losing, winning) in [
        ("ascending", (99, 1000), (101, 0)),
        ("descending", (101, 0), (99, 1000)),
    ] {
        let (_tmp, _catalog, ctx) = setup(
            2,
            &format!(", 'sequence.field' = 'v,w', 'sequence.field.sort-order' = '{order}'"),
        )
        .await;
        exec(&ctx, "INSERT INTO paimon.test_db.t VALUES (1, 100, 100)").await;
        let (losing_tie, winning_tie) = if order == "ascending" {
            (99, 101)
        } else {
            (101, 99)
        };
        for (assigned, expected) in [
            (losing, (100, 100)),
            ((100, losing_tie), (100, 100)),
            ((100, winning_tie), (100, winning_tie)),
            (winning, winning),
        ] {
            assert_eq!(
                dml_count(
                    &ctx,
                    &format!(
                        "UPDATE paimon.test_db.t SET v = {}, w = {}",
                        assigned.0, assigned.1
                    )
                )
                .await,
                1
            );
            assert_eq!(
                rows(&ctx).await,
                vec![(1, Some(expected.0), Some(expected.1))],
                "{order}: {assigned:?}"
            );
        }
    }
}

#[tokio::test]
async fn primary_key_update_assignment_failures_preserve_the_table() {
    let (tmp, catalog, ctx) = setup(2, "").await;
    exec(&ctx, "CREATE TABLE paimon.test_db.constrained (id INT NOT NULL, v INT NOT NULL, PRIMARY KEY (id)) WITH ('bucket' = '1')").await;
    exec(
        &ctx,
        "INSERT INTO paimon.test_db.constrained VALUES (1, 10)",
    )
    .await;
    exec(&ctx, "INSERT INTO paimon.test_db.t VALUES (1, 10, 100)").await;
    let before_snapshot = snapshot(&catalog).await;
    let before_files = files_under(tmp.path());
    for (sql, expected) in [
        (
            "UPDATE paimon.test_db.t SET v = CAST('invalid' AS INT)",
            "cast",
        ),
        ("UPDATE paimon.test_db.t SET v = 2147483648", "cast"),
        ("UPDATE paimon.test_db.constrained SET v = NULL", "non-null"),
    ] {
        assert_sql_error(&ctx, sql, expected).await;
        assert_eq!(files_under(tmp.path()), before_files);
    }
    assert_eq!(snapshot(&catalog).await, before_snapshot);
    assert_eq!(rows(&ctx).await, vec![(1, Some(10), Some(100))]);
    assert_eq!(
        common::collect_id_value(
            &ctx,
            "SELECT id, v AS value FROM paimon.test_db.constrained"
        )
        .await,
        vec![(1, 10)]
    );
}
