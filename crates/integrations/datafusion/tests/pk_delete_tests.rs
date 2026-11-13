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

use common::{assert_sql_error, create_sql_context, create_test_env, dml_count, exec};
use paimon::catalog::Identifier;
use paimon::{Catalog, FileSystemCatalog};
use paimon_datafusion::SQLContext;
use std::sync::Arc;

async fn setup(options: &str) -> (tempfile::TempDir, Arc<FileSystemCatalog>, SQLContext) {
    let (tmp, catalog) = create_test_env();
    let ctx = create_sql_context(Arc::clone(&catalog)).await;
    exec(&ctx, "CREATE SCHEMA paimon.test_db").await;
    exec(
        &ctx,
        &format!(
            "CREATE TABLE paimon.test_db.t (id INT NOT NULL, v INT, PRIMARY KEY (id)) \
         WITH ('bucket' = '2', 'write-only' = 'true'{options})"
        ),
    )
    .await;
    (tmp, catalog, ctx)
}

async fn rows(ctx: &SQLContext) -> Vec<(i32, Option<i32>)> {
    use datafusion::arrow::array::{Array, Int32Array};
    let batches = ctx
        .sql("SELECT id, v FROM paimon.test_db.t")
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
        rows.extend((0..batch.num_rows()).map(|row| {
            (
                ids.value(row),
                values.is_valid(row).then(|| values.value(row)),
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

fn files_under(path: &std::path::Path) -> std::collections::BTreeSet<std::path::PathBuf> {
    let mut files = std::collections::BTreeSet::new();
    for entry in std::fs::read_dir(path).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files.extend(files_under(&path));
        } else {
            files.insert(path);
        }
    }
    files
}

#[tokio::test]
async fn primary_key_delete_reads_latest_versions_and_counts_logical_rows() {
    let (_tmp, catalog, ctx) = setup("").await;
    exec(
        &ctx,
        "INSERT INTO paimon.test_db.t VALUES (1, 10), (2, 20), (3, NULL)",
    )
    .await;
    exec(&ctx, "INSERT INTO paimon.test_db.t VALUES (1, 11)").await;
    let before = snapshot(&catalog).await;
    assert_eq!(
        dml_count(&ctx, "DELETE FROM paimon.test_db.t WHERE v = 10").await,
        0
    );
    assert_eq!(snapshot(&catalog).await, before);
    assert_eq!(
        dml_count(&ctx, "DELETE FROM paimon.test_db.t WHERE t.v <> 11").await,
        1
    );
    assert_eq!(rows(&ctx).await, vec![(1, Some(11)), (3, None)]);
    assert_eq!(
        dml_count(&ctx, "DELETE FROM paimon.test_db.t WHERE v IS NULL").await,
        1
    );
    assert_eq!(dml_count(&ctx, "DELETE FROM paimon.test_db.t").await, 1);
    assert!(rows(&ctx).await.is_empty());
    let before = snapshot(&catalog).await;
    assert_eq!(dml_count(&ctx, "DELETE FROM paimon.test_db.t").await, 0);
    assert_eq!(snapshot(&catalog).await, before);
}

#[tokio::test]
async fn primary_key_delete_on_never_written_table_is_a_noop() {
    let (_tmp, catalog, ctx) = setup("").await;
    for sql in [
        "DELETE FROM paimon.test_db.t WHERE id = 1",
        "DELETE FROM paimon.test_db.t",
    ] {
        assert_eq!(dml_count(&ctx, sql).await, 0);
        assert_eq!(snapshot(&catalog).await, None);
    }
    assert_sql_error(
        &ctx,
        "DELETE FROM paimon.test_db.t WHERE missing_column = 1",
        "missing_column",
    )
    .await;
    assert_eq!(snapshot(&catalog).await, None);
}

#[tokio::test]
async fn primary_key_delete_supports_subqueries() {
    let (_tmp, _catalog, ctx) = setup("").await;
    exec(
        &ctx,
        "INSERT INTO paimon.test_db.t VALUES (1, 10), (2, 20), (3, 30)",
    )
    .await;
    assert_eq!(dml_count(&ctx, "DELETE FROM paimon.test_db.t WHERE id IN (SELECT id FROM paimon.test_db.t WHERE v = 20)").await, 1);
    assert_eq!(rows(&ctx).await, vec![(1, Some(10)), (3, Some(30))]);
    exec(&ctx, "CREATE TEMPORARY VIEW selected_ids AS SELECT 3 AS id").await;
    assert_eq!(dml_count(&ctx, "DELETE FROM paimon.test_db.t WHERE EXISTS (SELECT 1 FROM selected_ids s WHERE s.id = t.id)").await, 1);
    assert_eq!(rows(&ctx).await, vec![(1, Some(10))]);
    assert_eq!(
        dml_count(
            &ctx,
            "DELETE FROM paimon.test_db.t WHERE v = (SELECT MAX(v) FROM paimon.test_db.t)"
        )
        .await,
        1
    );
    assert!(rows(&ctx).await.is_empty());
}

#[tokio::test]
async fn primary_key_delete_handles_dynamic_buckets() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog).await;
    exec(&ctx, "CREATE SCHEMA paimon.test_db").await;
    exec(&ctx, "CREATE TABLE paimon.test_db.t (id INT NOT NULL, v INT, PRIMARY KEY (id)) WITH ('bucket' = '-1', 'write-only' = 'true')").await;
    exec(&ctx, "INSERT INTO paimon.test_db.t VALUES (1, 10), (2, 20)").await;
    assert_eq!(
        dml_count(&ctx, "DELETE FROM paimon.test_db.t WHERE id = 1").await,
        1
    );
    assert_eq!(rows(&ctx).await, vec![(2, Some(20))]);
    exec(&ctx, "INSERT INTO paimon.test_db.t VALUES (1, 30)").await;
    assert_eq!(rows(&ctx).await, vec![(1, Some(30)), (2, Some(20))]);
}

#[tokio::test]
async fn primary_key_delete_preserves_partition_and_composite_key() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog).await;
    exec(&ctx, "CREATE SCHEMA paimon.test_db").await;
    exec(&ctx, "CREATE TABLE paimon.test_db.t (id INT NOT NULL, name STRING NOT NULL, pt INT NOT NULL, v INT, PRIMARY KEY (id, name, pt)) PARTITIONED BY (pt) WITH ('bucket' = '2')").await;
    exec(
        &ctx,
        "INSERT INTO paimon.test_db.t VALUES (1, 'a', 1, 10), (1, 'b', 1, 11), (1, 'a', 2, 20), (2, 'a', 1, 30)",
    )
    .await;
    assert_eq!(
        dml_count(
            &ctx,
            "DELETE FROM paimon.test_db.t WHERE pt = 1 AND id = 1 AND name = 'a'"
        )
        .await,
        1
    );
    assert_eq!(
        rows(&ctx).await,
        vec![(1, Some(11)), (1, Some(20)), (2, Some(30))]
    );
}

#[tokio::test]
async fn primary_key_delete_preserves_sequence_fields_and_allows_later_upserts() {
    for (fields, order, older, newer) in [
        ("v", "ascending", 99, 101),
        ("v", "descending", 101, 99),
        ("id,v", "ascending", 99, 101),
        ("id,v", "descending", 101, 99),
    ] {
        let (_tmp, _catalog, ctx) = setup(&format!(
            ", 'sequence.field' = '{fields}', 'sequence.field.sort-order' = '{order}'"
        ))
        .await;
        exec(&ctx, "INSERT INTO paimon.test_db.t VALUES (1, 100)").await;
        assert_eq!(
            dml_count(&ctx, "DELETE FROM paimon.test_db.t WHERE id = 1").await,
            1
        );
        exec(
            &ctx,
            &format!("INSERT INTO paimon.test_db.t VALUES (1, {older})"),
        )
        .await;
        assert!(
            rows(&ctx).await.is_empty(),
            "an older sequence must not resurrect the row ({fields}, {order})"
        );
        exec(
            &ctx,
            &format!("INSERT INTO paimon.test_db.t VALUES (1, {newer})"),
        )
        .await;
        assert_eq!(
            rows(&ctx).await,
            vec![(1, Some(newer))],
            "{fields}, {order}"
        );
    }
}

#[tokio::test]
async fn primary_key_delete_preserves_lexicographic_sequence_order() {
    for (order, sequences) in [
        ("ascending", [(9, 99), (10, 19), (11, 0), (10, 21)]),
        ("descending", [(11, 0), (10, 21), (9, 99), (10, 19)]),
    ] {
        let (_tmp, catalog) = create_test_env();
        let ctx = create_sql_context(catalog).await;
        exec(&ctx, "CREATE SCHEMA paimon.test_db").await;
        exec(
            &ctx,
            &format!(
                "CREATE TABLE paimon.test_db.t (id INT NOT NULL, v INT, seq_major INT, seq_minor INT, PRIMARY KEY (id)) \
                 WITH ('bucket' = '2', 'write-only' = 'true', 'sequence.field' = 'seq_major,seq_minor', 'sequence.field.sort-order' = '{order}')"
            ),
        )
        .await;
        exec(
            &ctx,
            "INSERT INTO paimon.test_db.t VALUES (1, 10, 10, 20), (2, 20, 10, 20), (3, 30, 10, 20), (4, 40, 10, 20)",
        )
        .await;
        assert_eq!(dml_count(&ctx, "DELETE FROM paimon.test_db.t").await, 4);
        assert!(rows(&ctx).await.is_empty(), "{order}");

        let values = sequences
            .iter()
            .enumerate()
            .map(|(index, (major, minor))| {
                let id = index + 1;
                format!("({id}, {}, {major}, {minor})", id * 10)
            })
            .collect::<Vec<_>>()
            .join(", ");
        exec(
            &ctx,
            &format!("INSERT INTO paimon.test_db.t VALUES {values}"),
        )
        .await;
        assert_eq!(
            rows(&ctx).await,
            vec![(3, Some(30)), (4, Some(40))],
            "only newer sequence tuples may resurrect deleted rows ({order})"
        );
    }
}

#[tokio::test]
async fn primary_key_delete_rejects_unsupported_options() {
    for (options, expected_error) in [
        (
            ", 'merge-engine' = 'partial-update'",
            "requires merge-engine=deduplicate",
        ),
        (
            ", 'merge-engine' = 'aggregation', 'fields.v.aggregate-function' = 'sum'",
            "requires merge-engine=deduplicate",
        ),
        (
            ", 'merge-engine' = 'first-row'",
            "requires merge-engine=deduplicate",
        ),
        (", 'ignore-delete' = 'true'", "requires ignore-delete=false"),
        (
            ", 'deduplicate.ignore-delete' = 'true'",
            "requires ignore-delete=false",
        ),
    ] {
        let (_tmp, catalog, ctx) = setup(options).await;
        for (insert_rows, sql) in [
            (false, "DELETE FROM paimon.test_db.t"),
            (true, "DELETE FROM paimon.test_db.t WHERE id = 1"),
        ] {
            if insert_rows {
                exec(&ctx, "INSERT INTO paimon.test_db.t VALUES (1, 10), (2, 20)").await;
            }
            let before = snapshot(&catalog).await;
            let before_rows = rows(&ctx).await;
            assert_sql_error(&ctx, sql, expected_error).await;
            assert_eq!(snapshot(&catalog).await, before, "{options}: {sql}");
            assert_eq!(rows(&ctx).await, before_rows, "{options}: {sql}");
        }
    }

    let (_tmp, _catalog, ctx) = setup(
        ", 'ignore-delete' = 'false', 'deduplicate.ignore-delete' = 'true', 'ignore-update-before' = 'true'",
    )
    .await;
    exec(&ctx, "INSERT INTO paimon.test_db.t VALUES (1, 10)").await;
    assert_eq!(dml_count(&ctx, "DELETE FROM paimon.test_db.t").await, 1);
    assert!(rows(&ctx).await.is_empty());
}

#[tokio::test]
async fn primary_key_delete_rejects_extended_syntax() {
    let (_tmp, catalog, ctx) = setup("").await;
    exec(&ctx, "INSERT INTO paimon.test_db.t VALUES (1, 10)").await;
    let before = snapshot(&catalog).await;
    for suffix in [
        "RETURNING id",
        "USING paimon.test_db.t other WHERE t.id = other.id",
        "ORDER BY id LIMIT 1",
        "LIMIT 1",
        "JOIN paimon.test_db.t other ON t.id = other.id",
        ", paimon.test_db.t",
        "TABLESAMPLE BERNOULLI (50)",
        "OUTPUT deleted.id",
    ] {
        assert_sql_error(
            &ctx,
            &format!("DELETE FROM paimon.test_db.t {suffix}"),
            "optional WHERE",
        )
        .await;
        assert_eq!(snapshot(&catalog).await, before, "{suffix}");
        assert_eq!(rows(&ctx).await, vec![(1, Some(10))]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn primary_key_delete_pins_snapshot_and_respects_concurrent_upsert_sequence() {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::{create_udf, Volatility};
    use std::sync::atomic::{AtomicUsize, Ordering};

    for (options, expected) in [
        ("", vec![(2, Some(10))]),
        (
            ", 'sequence.field' = 'v'",
            vec![(1, Some(20)), (2, Some(10))],
        ),
        (
            ", 'sequence.field' = 'v', 'sequence.field.sort-order' = 'descending'",
            vec![(2, Some(10))],
        ),
    ] {
        let (_tmp, _catalog, ctx) = setup(options).await;
        let ctx = Arc::new(ctx);
        exec(&ctx, "INSERT INTO paimon.test_db.t VALUES (1, 10)").await;
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
                            ctx.sql("INSERT INTO paimon.test_db.t VALUES (1, 20), (2, 10)")
                                .await?
                                .collect()
                                .await
                        })
                    })?;
                }
                Ok(args[0].clone())
            }),
        ));
        let sql = "DELETE FROM paimon.test_db.t WHERE interfere(v) = 10";
        assert_eq!(dml_count(&ctx, sql).await, 1);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the statement must not re-scan"
        );
        assert_eq!(rows(&ctx).await, expected);

        assert_eq!(dml_count(&ctx, sql).await, 1);
        assert!(
            calls.load(Ordering::SeqCst) > 1,
            "re-executing DELETE must scan a new snapshot"
        );
        assert_eq!(
            rows(&ctx).await,
            expected
                .into_iter()
                .filter(|(id, _)| *id != 2)
                .collect::<Vec<_>>()
        );
    }
}

#[tokio::test]
async fn primary_key_delete_preserves_quoted_names_and_string_values() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog).await;
    exec(&ctx, "CREATE SCHEMA paimon.test_db").await;
    exec(&ctx, "CREATE TABLE paimon.test_db.\"odd.name\" (\"Key.Id\" INT NOT NULL, name STRING, PRIMARY KEY (\"Key.Id\")) WITH ('bucket' = '1')").await;
    exec(
        &ctx,
        "INSERT INTO paimon.test_db.\"odd.name\" VALUES (1, 'delete'), (2, 'keep')",
    )
    .await;
    assert_eq!(
        dml_count(
            &ctx,
            "DELETE FROM paimon.test_db.\"odd.name\" WHERE \"Key.Id\" = 1 AND name = 'delete'"
        )
        .await,
        1
    );
    assert_eq!(
        common::collect_id_name(
            &ctx,
            "SELECT \"Key.Id\" AS id, name FROM paimon.test_db.\"odd.name\""
        )
        .await,
        vec![(2, "keep".into())]
    );
}

#[tokio::test]
async fn primary_key_delete_keeps_existing_target_guards() {
    let (_tmp, _catalog, ctx) = setup("").await;
    exec(&ctx, "INSERT INTO paimon.test_db.t VALUES (1, 10)").await;
    exec(
        &ctx,
        "CREATE TEMPORARY VIEW paimon.test_db.t AS SELECT 99 AS id",
    )
    .await;
    assert_sql_error(&ctx, "DELETE FROM paimon.test_db.t", "temporary").await;
    exec(&ctx, "DROP TEMPORARY VIEW paimon.test_db.t").await;
    assert_sql_error(
        &ctx,
        "DELETE FROM paimon.test_db.t AS x WHERE x.id = 1",
        "alias",
    )
    .await;
    assert_sql_error(&ctx, "DELETE FROM paimon.test_db.t$branch_dev", "branch").await;
    exec(&ctx, "SET 'paimon.scan.snapshot-id' = '1'").await;
    assert_sql_error(&ctx, "DELETE FROM paimon.test_db.t", "time-travel option").await;
    exec(&ctx, "RESET 'paimon.scan.snapshot-id'").await;
    assert_eq!(rows(&ctx).await, vec![(1, Some(10))]);
}

#[tokio::test]
async fn primary_key_delete_discards_partial_output_when_scan_fails() {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::error::DataFusionError;
    use datafusion::logical_expr::{create_udf, Volatility};
    use std::sync::atomic::{AtomicUsize, Ordering};

    let (tmp, catalog, ctx) =
        setup(", 'read.batch-size' = '1', 'write.parquet-buffer-size' = '1'").await;
    exec(&ctx, "SET datafusion.execution.target_partitions = 1").await;
    exec(&ctx, "SET datafusion.execution.batch_size = 1").await;
    exec(&ctx, "INSERT INTO paimon.test_db.t VALUES (1, 10)").await;
    exec(&ctx, "INSERT INTO paimon.test_db.t VALUES (2, 20)").await;
    let before = files_under(tmp.path());
    let before_snapshot = snapshot(&catalog).await;
    let root = tmp.path().to_path_buf();
    let initial_files = before.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    ctx.ctx().register_udf(create_udf(
        "fail_after_batch",
        vec![DataType::Int32],
        DataType::Int32,
        Volatility::Volatile,
        Arc::new(move |args| {
            if counted.fetch_add(1, Ordering::SeqCst) > 0 {
                assert!(
                    files_under(&root)
                        .difference(&initial_files)
                        .any(|path| path.extension().is_some_and(|ext| ext == "parquet")),
                    "the first batch must have staged a data file before the injected error"
                );
                return Err(DataFusionError::Execution("injected scan failure".into()));
            }
            Ok(args[0].clone())
        }),
    ));
    assert_sql_error(
        &ctx,
        "DELETE FROM paimon.test_db.t WHERE fail_after_batch(id) > 0",
        "injected scan failure",
    )
    .await;
    assert!(calls.load(Ordering::SeqCst) >= 2);
    assert_eq!(snapshot(&catalog).await, before_snapshot);
    assert_eq!(
        files_under(tmp.path()),
        before,
        "uncommitted files must be removed"
    );
    assert_eq!(rows(&ctx).await, vec![(1, Some(10)), (2, Some(20))]);
}
