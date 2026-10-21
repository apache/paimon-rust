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

//! Integration tests for the `paimon_incremental_query` table function.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use futures::StreamExt;
use paimon::catalog::{Catalog, Identifier};
use paimon::table::{IncrementalScanMode, IncrementalSplit};
use paimon::{CatalogOptions, FileSystemCatalog, Options};
use paimon_datafusion::SQLContext;
use tempfile::TempDir;

async fn run(ctx: &SQLContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql).await.unwrap().collect().await.unwrap()
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

fn catalog(warehouse: &TempDir) -> Arc<FileSystemCatalog> {
    let mut options = Options::new();
    options.set(
        CatalogOptions::WAREHOUSE,
        format!("file://{}", warehouse.path().display()),
    );
    Arc::new(FileSystemCatalog::new(options).unwrap())
}

async fn context(warehouse: &TempDir) -> SQLContext {
    let mut ctx = SQLContext::new();
    ctx.register_catalog("paimon", catalog(warehouse))
        .await
        .unwrap();
    ctx
}

/// A session over `warehouse` whose memory pool admits `limit` bytes.
async fn context_with_memory_limit(
    warehouse: &TempDir,
    limit: usize,
) -> (SQLContext, Arc<GreedyMemoryPool>) {
    let pool = Arc::new(GreedyMemoryPool::new(limit));
    let runtime_env = RuntimeEnvBuilder::new()
        .with_memory_pool(pool.clone())
        .build_arc()
        .unwrap();
    let mut ctx = SQLContext::builder().with_runtime_env(runtime_env).build();
    ctx.register_catalog("paimon", catalog(warehouse))
        .await
        .unwrap();
    (ctx, pool)
}

/// Append-only table with one row per snapshot: snapshot 1 = (1,'a'),
/// snapshot 2 = (2,'b'), snapshot 3 = (3,'c').
async fn setup_three_snapshots() -> (TempDir, SQLContext) {
    let temp_dir = TempDir::new().expect("temp dir");
    let ctx = context(&temp_dir).await;

    run(&ctx, "CREATE TABLE paimon.default.t (id INT, name STRING)").await;
    run(&ctx, "INSERT INTO paimon.default.t VALUES (1, 'a')").await;
    run(&ctx, "INSERT INTO paimon.default.t VALUES (2, 'b')").await;
    run(&ctx, "INSERT INTO paimon.default.t VALUES (3, 'c')").await;
    (temp_dir, ctx)
}
/// Collects the `id` column across batches into a sorted vector.
fn sorted_ids(batches: &[RecordBatch]) -> Vec<i32> {
    use datafusion::arrow::array::{Array, Int32Array};
    let mut ids = Vec::new();
    for batch in batches {
        let col = batch
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id is Int32");
        for i in 0..col.len() {
            ids.push(col.value(i));
        }
    }
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn test_auto_range_is_start_exclusive_end_inclusive() {
    let (_tmp, ctx) = setup_three_snapshots().await;

    // (1, 3]: snapshots 2 and 3 only — start is exclusive, end inclusive.
    let batches = run(
        &ctx,
        "SELECT * FROM paimon_incremental_query('default.t', 1, 3)",
    )
    .await;
    assert_eq!(total_rows(&batches), 2);
    assert_eq!(sorted_ids(&batches), vec![2, 3]);

    // (0, 3]: every snapshot.
    let all = run(
        &ctx,
        "SELECT * FROM paimon_incremental_query('default.t', 0, 3)",
    )
    .await;
    assert_eq!(sorted_ids(&all), vec![1, 2, 3]);

    // (3, 3]: empty range.
    let empty = run(
        &ctx,
        "SELECT * FROM paimon_incremental_query('default.t', 3, 3)",
    )
    .await;
    assert_eq!(total_rows(&empty), 0);
}

#[tokio::test]
async fn test_explicit_delta_mode_matches_auto() {
    let (_tmp, ctx) = setup_three_snapshots().await;
    let batches = run(
        &ctx,
        "SELECT * FROM paimon_incremental_query('default.t', 0, 2, 'delta')",
    )
    .await;
    assert_eq!(sorted_ids(&batches), vec![1, 2]);
}

#[tokio::test]
async fn test_projection_selects_single_column() {
    let (_tmp, ctx) = setup_three_snapshots().await;
    let batches = run(
        &ctx,
        "SELECT id FROM paimon_incremental_query('default.t', 0, 3) ORDER BY id",
    )
    .await;
    assert_eq!(batches[0].num_columns(), 1);
    assert_eq!(sorted_ids(&batches), vec![1, 2, 3]);
}
#[tokio::test]
async fn test_audit_log_suffix_prepends_rowkind() {
    use datafusion::arrow::array::{Array, StringArray};
    let (_tmp, ctx) = setup_three_snapshots().await;
    let batches = run(
        &ctx,
        "SELECT * FROM paimon_incremental_query('default.t$audit_log', 0, 3, 'delta')",
    )
    .await;
    assert_eq!(total_rows(&batches), 3);
    let batch = &batches[0];
    assert_eq!(
        batch.schema().field(0).name(),
        "rowkind",
        "audit-log output must lead with the rowkind column"
    );
    let rowkind = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("rowkind is Utf8");
    // Append-only rows are all inserts.
    for i in 0..rowkind.len() {
        assert_eq!(rowkind.value(i), "+I");
    }
}

async fn expect_error(ctx: &SQLContext, sql: &str) -> String {
    match ctx.sql(sql).await {
        Err(e) => e.to_string(),
        Ok(df) => df
            .collect()
            .await
            .expect_err("expected the query to fail")
            .to_string(),
    }
}

#[tokio::test]
async fn test_rejects_unknown_mode() {
    let (_tmp, ctx) = setup_three_snapshots().await;
    let err = expect_error(
        &ctx,
        "SELECT * FROM paimon_incremental_query('default.t', 0, 1, 'bogus')",
    )
    .await;
    assert!(
        err.contains("unknown scan mode"),
        "error must name the bad mode: {err}"
    );
}

#[tokio::test]
async fn test_rejects_bad_arity() {
    let (_tmp, ctx) = setup_three_snapshots().await;
    let err = expect_error(
        &ctx,
        "SELECT * FROM paimon_incremental_query('default.t', 0)",
    )
    .await;
    assert!(
        err.contains("requires 3 or 4 arguments"),
        "error must explain the arity: {err}"
    );
}

#[tokio::test]
async fn test_rejects_end_before_start() {
    let (_tmp, ctx) = setup_three_snapshots().await;
    let err = expect_error(
        &ctx,
        "SELECT * FROM paimon_incremental_query('default.t', 5, 2)",
    )
    .await;
    assert!(
        err.contains("must be >="),
        "error must explain the range order: {err}"
    );
}

#[tokio::test]
async fn test_reads_reserve_memory_from_the_session_pool() {
    let (tmp, _ctx) = setup_three_snapshots().await;
    let (ctx, _pool) = context_with_memory_limit(&tmp, 0).await;
    // A pool that admits nothing refuses the ordinary scan, and must refuse
    // the incremental read the same way.
    for sql in [
        "SELECT * FROM paimon.default.t",
        "SELECT * FROM paimon_incremental_query('default.t', 0, 3)",
    ] {
        let err = expect_error(&ctx, sql).await;
        assert!(err.contains("Resources exhausted"), "{sql}: {err}");
    }
}

#[tokio::test]
async fn test_projection_skips_unselected_columns() {
    let tmp = TempDir::new().unwrap();
    let ctx = context(&tmp).await;
    run(
        &ctx,
        "CREATE TABLE paimon.default.wide (id INT, payload STRING)",
    )
    .await;
    run(
        &ctx,
        "INSERT INTO paimon.default.wide VALUES (1, repeat('x', 1048576)), (2, repeat('y', 1048576))",
    )
    .await;

    // The pool admits the `id` column but not the 1 MiB payloads, so only a
    // read that never decodes `payload` can succeed.
    let (ctx, pool) = context_with_memory_limit(&tmp, 64 * 1024).await;
    let ids = run(
        &ctx,
        "SELECT id FROM paimon_incremental_query('default.wide', 0, 1)",
    )
    .await;
    assert_eq!(sorted_ids(&ids), vec![1, 2]);
    // The reader releases its reservation; emitted batches belong to the consumer.
    assert_eq!(pool.reserved(), 0);
    let err = expect_error(
        &ctx,
        "SELECT * FROM paimon_incremental_query('default.wide', 0, 1)",
    )
    .await;
    assert!(err.contains("Resources exhausted"), "{err}");
}

#[tokio::test]
async fn test_batches_stream_before_the_whole_range_is_read() {
    let (tmp, ctx) = setup_three_snapshots().await;
    // Lose the range's last data file. Reading the whole range before emitting
    // anything would fail before the first batch.
    let table = catalog(&tmp)
        .get_table(&Identifier::new("default", "t"))
        .await
        .unwrap();
    let plan = table
        .new_read_builder()
        .new_incremental_scan(IncrementalScanMode::Delta, 0, 3)
        .plan()
        .await
        .unwrap();
    let Some(IncrementalSplit::Data(split)) = plan.splits().last() else {
        panic!("expected a data split");
    };
    let file = split.data_files().last().expect("a data file");
    table
        .file_io()
        .delete_file(&format!("{}/{}", split.bucket_path(), file.file_name))
        .await
        .unwrap();

    let mut stream = ctx
        .sql("SELECT * FROM paimon_incremental_query('default.t', 0, 3)")
        .await
        .unwrap()
        .execute_stream()
        .await
        .unwrap();
    assert!(stream.next().await.unwrap().is_ok());
    let mut rest = Vec::new();
    while let Some(batch) = stream.next().await {
        rest.push(batch);
    }
    assert!(
        rest.iter().any(Result::is_err),
        "the missing file still fails the read"
    );
}

#[tokio::test]
async fn test_audit_log_projection_keeps_column_offsets() {
    use datafusion::arrow::array::{Array, Int32Array, Int8Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    // With sequence numbers, core's audit batches lead with two audit columns,
    // so a projected data column sits two positions further right.
    let tmp = TempDir::new().unwrap();
    let ctx = context(&tmp).await;
    run(
        &ctx,
        "CREATE TABLE paimon.default.pk (id INT NOT NULL, name STRING, PRIMARY KEY (id)) WITH (
            'bucket' = '1',
            'changelog-producer' = 'input',
            'table-read.sequence-number.enabled' = 'true'
        )",
    )
    .await;
    run(
        &ctx,
        "INSERT INTO paimon.default.pk VALUES (1, 'a'), (2, 'b')",
    )
    .await;
    // Snapshot 2 updates id 1 and deletes id 2.
    let table = catalog(&tmp)
        .get_table(&Identifier::new("default", "pk"))
        .await
        .unwrap();
    let changes = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("_VALUE_KIND", DataType::Int8, false),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1, 1, 2])),
            Arc::new(StringArray::from(vec!["a", "a2", "b"])),
            // -U, +U, -D
            Arc::new(Int8Array::from(vec![1, 2, 3])),
        ],
    )
    .unwrap();
    let builder = table.new_write_builder();
    let mut write = builder.new_write().unwrap();
    write.write_arrow_batch(&changes).await.unwrap();
    let messages = write.prepare_commit().await.unwrap();
    builder.new_commit().commit(messages).await.unwrap();

    for mode in ["changelog", "diff"] {
        let batches = run(
            &ctx,
            &format!(
                "SELECT name, rowkind, id FROM paimon_incremental_query('default.pk$audit_log', 1, 2, '{mode}') ORDER BY id, rowkind"
            ),
        )
        .await;
        let mut rows = Vec::new();
        for batch in &batches {
            let name = batch.column(0).as_any().downcast_ref::<StringArray>();
            let rowkind = batch.column(1).as_any().downcast_ref::<StringArray>();
            let id = batch.column(2).as_any().downcast_ref::<Int32Array>();
            let (name, rowkind, id) = (name.unwrap(), rowkind.unwrap(), id.unwrap());
            for row in 0..batch.num_rows() {
                rows.push((
                    name.value(row).to_string(),
                    rowkind.value(row).to_string(),
                    id.value(row),
                ));
            }
        }
        let expected = [("a2", "+U", 1), ("a", "-U", 1), ("b", "-D", 2)]
            .map(|(name, rowkind, id)| (name.to_string(), rowkind.to_string(), id));
        assert_eq!(rows, expected, "{mode}");

        // Skipping `id` makes `name` the only read field, so its column comes
        // right after the audit columns rather than at its table position.
        let batches = run(
            &ctx,
            &format!(
                "SELECT rowkind, name FROM paimon_incremental_query('default.pk$audit_log', 1, 2, '{mode}') ORDER BY name, rowkind"
            ),
        )
        .await;
        let mut rows = Vec::new();
        for batch in &batches {
            let rowkind = batch.column(0).as_any().downcast_ref::<StringArray>();
            let name = batch.column(1).as_any().downcast_ref::<StringArray>();
            let (rowkind, name) = (rowkind.unwrap(), name.unwrap());
            for row in 0..batch.num_rows() {
                rows.push((rowkind.value(row).to_string(), name.value(row).to_string()));
            }
        }
        let expected = [("-U", "a"), ("+U", "a2"), ("-D", "b")]
            .map(|(rowkind, name)| (rowkind.to_string(), name.to_string()));
        assert_eq!(rows, expected, "{mode}");
    }
}

#[tokio::test]
async fn test_batches_match_the_declared_schema() {
    // `DataFrame::cache` keeps the batches only if the plan declares their schema.
    let (_tmp, ctx) = setup_three_snapshots().await;
    for sql in [
        "SELECT * FROM paimon_incremental_query('default.t', 0, 3)",
        "SELECT name, rowkind FROM paimon_incremental_query('default.t$audit_log', 0, 3)",
    ] {
        let cached = ctx.sql(sql).await.unwrap().cache().await.unwrap();
        assert_eq!(total_rows(&cached.collect().await.unwrap()), 3, "{sql}");
    }
}

#[tokio::test]
async fn test_blob_rows_match_the_ordinary_read() {
    use datafusion::arrow::util::pretty::pretty_format_batches;
    // BLOB values live apart from the row's other columns: in a file of their
    // own on a data-evolution table, behind a stored descriptor on a
    // primary-key table. Reading the range must return the ordinary read.
    let tmp = TempDir::new().unwrap();
    let ctx = context(&tmp).await;
    run(
        &ctx,
        "CREATE TABLE paimon.default.de_assets (id INT, picture BLOB) WITH (
            'row-tracking.enabled' = 'true',
            'data-evolution.enabled' = 'true'
        )",
    )
    .await;
    run(
        &ctx,
        "CREATE TABLE paimon.default.pk_assets (id INT NOT NULL, picture BLOB, PRIMARY KEY (id))
            WITH ('bucket' = '1')",
    )
    .await;
    for table in ["de_assets", "pk_assets"] {
        for values in ["(1, X'414141'), (2, X'4242')", "(3, X'43')"] {
            run(
                &ctx,
                &format!("INSERT INTO paimon.default.{table} (id, picture) VALUES {values}"),
            )
            .await;
        }
        let format =
            |batches: Vec<RecordBatch>| pretty_format_batches(&batches).unwrap().to_string();
        let expected = format(
            run(
                &ctx,
                &format!("SELECT id, picture FROM paimon.default.{table} ORDER BY id"),
            )
            .await,
        );
        for name in [table.to_string(), format!("{table}$audit_log")] {
            let actual = format(
                run(
                    &ctx,
                    &format!(
                        "SELECT id, picture FROM paimon_incremental_query('default.{name}', 0, 2) ORDER BY id"
                    ),
                )
                .await,
            );
            assert_eq!(actual, expected, "{name}");
        }
        for (start, rows) in [(0, 3), (1, 1)] {
            let batches = run(
                &ctx,
                &format!(
                    "SELECT count(*) AS n FROM paimon_incremental_query('default.{table}', {start}, 2)"
                ),
            )
            .await;
            let count = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .unwrap()
                .value(0);
            assert_eq!(count, rows, "{table} from {start}");
        }
    }
}

#[tokio::test]
async fn test_data_evolution_range_merges_column_updates() {
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::util::pretty::pretty_format_batches;
    // MERGE INTO on a data-evolution table writes only the updated `name` to a
    // file of its own. A range that also holds the rows it updates reads both
    // files together, so the update merges into them; read per commit, the
    // update would come back as rows whose NOT NULL `id` is missing.
    let tmp = TempDir::new().unwrap();
    let ctx = context(&tmp).await;
    run(
        &ctx,
        "CREATE TABLE paimon.default.de (id INT NOT NULL, name STRING, value INT) WITH (
            'row-tracking.enabled' = 'true',
            'data-evolution.enabled' = 'true'
        )",
    )
    .await;
    run(
        &ctx,
        "INSERT INTO paimon.default.de (id, name, value) VALUES (1, 'alice', 10), (2, 'bob', 20)",
    )
    .await;
    run(
        &ctx,
        "CREATE TEMPORARY TABLE paimon.default.renames AS
            SELECT * FROM (VALUES (1, 'ALICE')) AS t(id, name)",
    )
    .await;
    run(
        &ctx,
        "MERGE INTO paimon.default.de t USING paimon.default.renames s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET name = s.name",
    )
    .await;
    let format = |batches: Vec<RecordBatch>| pretty_format_batches(&batches).unwrap().to_string();
    let expected = format(
        run(
            &ctx,
            "SELECT id, name, value FROM paimon.default.de ORDER BY id",
        )
        .await,
    );
    // Auto is Delta on a data-evolution table, so both read the range as one batch.
    for mode in ["", ", 'delta'"] {
        for name in ["de", "de$audit_log"] {
            let actual = format(
                run(
                    &ctx,
                    &format!(
                        "SELECT id, name, value FROM paimon_incremental_query('default.{name}', 0, 2{mode}) ORDER BY id"
                    ),
                )
                .await,
            );
            assert_eq!(actual, expected, "{name}{mode}");
        }
        let batches = run(
            &ctx,
            &format!(
                "SELECT rowkind FROM paimon_incremental_query('default.de$audit_log', 0, 2{mode})"
            ),
        )
        .await;
        let kinds: Vec<&str> = batches
            .iter()
            .flat_map(|batch| {
                let kinds = batch.column(0).as_any().downcast_ref::<StringArray>();
                kinds.unwrap().iter().map(Option::unwrap)
            })
            .collect();
        assert_eq!(kinds, ["+I", "+I"], "{mode}");
    }
    // An empty range plans nothing, as on other tables.
    for (start, end, rows) in [(0, 2, 2), (0, 0, 0)] {
        let batches = run(
            &ctx,
            &format!(
                "SELECT count(*) AS n FROM paimon_incremental_query('default.de', {start}, {end})"
            ),
        )
        .await;
        let count = batches[0].column(0).as_any().downcast_ref::<Int64Array>();
        assert_eq!(count.unwrap().value(0), rows, "({start}, {end}]");
    }
}

#[tokio::test]
async fn test_data_evolution_range_of_column_updates_reads_unwritten_columns_as_null() {
    use datafusion::arrow::array::{Array, Int32Array, StringArray};
    // Snapshot 2 updates `name` and snapshot 3 updates `value`, so (1, 3]
    // holds two column files of the same rows and no file with `id`.
    let tmp = TempDir::new().unwrap();
    let ctx = context(&tmp).await;
    run(
        &ctx,
        "CREATE TABLE paimon.default.de (id INT NOT NULL, name STRING, value INT) WITH (
            'row-tracking.enabled' = 'true',
            'data-evolution.enabled' = 'true'
        )",
    )
    .await;
    run(
        &ctx,
        "INSERT INTO paimon.default.de (id, name, value) VALUES (1, 'alice', 10), (2, 'bob', 20)",
    )
    .await;
    for (source, values, column) in [
        ("renames", "(1, 'ALICE')", "name"),
        ("revalues", "(2, 99)", "value"),
    ] {
        run(
            &ctx,
            &format!(
                "CREATE TEMPORARY TABLE paimon.default.{source} AS
                    SELECT * FROM (VALUES {values}) AS t(id, {column})"
            ),
        )
        .await;
        run(
            &ctx,
            &format!(
                "MERGE INTO paimon.default.de t USING paimon.default.{source} s ON t.id = s.id
                    WHEN MATCHED THEN UPDATE SET {column} = s.{column}"
            ),
        )
        .await;
    }
    for mode in ["", ", 'delta'"] {
        for name in ["de", "de$audit_log"] {
            let batches = run(
                &ctx,
                &format!(
                    "SELECT id, name, value FROM paimon_incremental_query('default.{name}', 1, 3{mode})"
                ),
            )
            .await;
            let mut rows = Vec::new();
            for batch in &batches {
                let id = batch.column(0).as_any().downcast_ref::<Int32Array>();
                let name = batch.column(1).as_any().downcast_ref::<StringArray>();
                let value = batch.column(2).as_any().downcast_ref::<Int32Array>();
                let (id, name, value) = (id.unwrap(), name.unwrap(), value.unwrap());
                for row in 0..batch.num_rows() {
                    rows.push((
                        id.is_valid(row).then(|| id.value(row)),
                        name.value(row).to_string(),
                        value.value(row),
                    ));
                }
            }
            rows.sort();
            let expected = [
                (None, "ALICE".to_string(), 10),
                (None, "bob".to_string(), 99),
            ];
            assert_eq!(rows, expected, "{name}{mode}");
        }
    }
}

/// Asserts that, for `name`, the function reads the rows a `FROM` clause in
/// the same session reads.
async fn assert_reads_as_from(ctx: &SQLContext, name: &str) {
    let expected = sorted_ids(&run(ctx, &format!("SELECT id FROM {name}")).await);
    let actual = sorted_ids(
        &run(
            ctx,
            &format!("SELECT id FROM paimon_incremental_query('{name}', 0, 1)"),
        )
        .await,
    );
    assert_eq!(actual, expected, "{name}");
}

#[tokio::test]
async fn test_table_name_resolves_as_in_a_from_clause() {
    use datafusion::catalog::MemoryCatalogProvider;
    // Two warehouses with a `default.t` each, and a database in cat_a named
    // like the catalog cat_b.
    let (a, b) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let mut ctx = SQLContext::new();
    ctx.register_catalog("cat_a", catalog(&a)).await.unwrap();
    ctx.register_catalog("cat_b", catalog(&b)).await.unwrap();
    run(&ctx, "CREATE SCHEMA cat_a.cat_b").await;
    for (table, id) in [
        ("cat_a.default.t", 1),
        ("cat_b.default.t", 2),
        ("cat_a.cat_b.t", 3),
    ] {
        run(&ctx, &format!("CREATE TABLE {table} (id INT)")).await;
        run(&ctx, &format!("INSERT INTO {table} VALUES ({id})")).await;
    }

    // cat_a, registered first, is the current catalog.
    for name in [
        "cat_a.default.t",
        "cat_b.default.t",
        "default.t",
        "t",
        "cat_b.t",
    ] {
        assert_reads_as_from(&ctx, name).await;
    }
    run(&ctx, "USE cat_b.default").await;
    for name in ["t", "default.t", "cat_a.default.t"] {
        assert_reads_as_from(&ctx, name).await;
    }
    run(&ctx, "USE cat_a.cat_b").await;
    assert_reads_as_from(&ctx, "t").await;

    let err = expect_error(
        &ctx,
        "SELECT * FROM paimon_incremental_query('nope.default.t', 0, 1)",
    )
    .await;
    assert!(err.contains("Unknown catalog 'nope'"), "{err}");
    ctx.ctx()
        .register_catalog("mem", Arc::new(MemoryCatalogProvider::new()));
    let err = expect_error(
        &ctx,
        "SELECT * FROM paimon_incremental_query('mem.public.t', 0, 1)",
    )
    .await;
    assert!(
        err.contains("Catalog 'mem' is not a Paimon catalog"),
        "{err}"
    );
}
#[tokio::test]
async fn test_session_read_options_apply_as_in_an_ordinary_read() {
    use datafusion::arrow::util::pretty::pretty_format_batches;
    let tmp = TempDir::new().unwrap();
    let ctx = context(&tmp).await;
    run(
        &ctx,
        "CREATE TABLE paimon.default.de_assets (id INT, picture BLOB) WITH (
            'row-tracking.enabled' = 'true',
            'data-evolution.enabled' = 'true'
        )",
    )
    .await;
    run(
        &ctx,
        "CREATE TABLE paimon.default.pk_assets (id INT NOT NULL, picture BLOB, PRIMARY KEY (id))
            WITH ('bucket' = '1')",
    )
    .await;
    let format = |batches: Vec<RecordBatch>| pretty_format_batches(&batches).unwrap().to_string();
    for table in ["de_assets", "pk_assets"] {
        run(
            &ctx,
            &format!("INSERT INTO paimon.default.{table} (id, picture) VALUES (1, X'414141')"),
        )
        .await;
        let ordinary = format!("SELECT id, picture FROM paimon.default.{table}");
        let payloads = format(run(&ctx, &ordinary).await);
        run(&ctx, "SET 'paimon.blob-as-descriptor' = 'true'").await;
        let descriptors = format(run(&ctx, &ordinary).await);
        assert_ne!(
            descriptors, payloads,
            "{table}: the option changes the read"
        );
        for (setting, expected) in [
            ("SET 'paimon.blob-as-descriptor' = 'true'", &descriptors),
            ("RESET 'paimon.blob-as-descriptor'", &payloads),
        ] {
            run(&ctx, setting).await;
            for name in [table.to_string(), format!("{table}$audit_log")] {
                let sql = format!(
                    "SELECT id, picture FROM paimon_incremental_query('default.{name}', 0, 1)"
                );
                assert_eq!(
                    &format(run(&ctx, &sql).await),
                    expected,
                    "{name}: {setting}"
                );
            }
        }
    }

    // Descriptors the function returns can be read back through the session,
    // as those of an ordinary read can.
    let fresh = context(&tmp).await;
    run(&fresh, "SET 'paimon.blob-as-descriptor' = 'true'").await;
    run(
        &fresh,
        "SELECT * FROM paimon_incremental_query('default.pk_assets', 0, 1)",
    )
    .await;
    let location = catalog(&tmp)
        .get_table(&Identifier::new("default", "pk_assets"))
        .await
        .unwrap()
        .location()
        .to_string();
    assert!(fresh
        .blob_reader_registry()
        .resolve(&format!("{location}/bucket-0/data.blob"))
        .is_some());

    run(&ctx, "CREATE TABLE paimon.default.t (id INT)").await;
    run(
        &ctx,
        "INSERT INTO paimon.default.t VALUES (1), (2), (3), (4), (5)",
    )
    .await;
    run(&ctx, "SET 'paimon.read.batch-size' = '2'").await;
    let sizes = |batches: Vec<RecordBatch>| {
        batches
            .iter()
            .map(RecordBatch::num_rows)
            .filter(|rows| *rows > 0)
            .collect::<Vec<_>>()
    };
    let ordinary = sizes(run(&ctx, "SELECT id FROM paimon.default.t").await);
    assert_eq!(ordinary, [2, 2, 1]);
    let incremental = sizes(
        run(
            &ctx,
            "SELECT id FROM paimon_incremental_query('default.t', 0, 1)",
        )
        .await,
    );
    assert_eq!(incremental, ordinary);
}

#[tokio::test]
async fn test_options_that_choose_another_range_are_refused() {
    let (_tmp, ctx) = setup_three_snapshots().await;
    for (key, value) in [
        ("scan.snapshot-id", "1"),
        // A tag that does not exist: the option is refused before any lookup.
        ("scan.tag-name", "not_a_tag"),
        ("scan.version", "1"),
        ("scan.timestamp-millis", "0"),
        ("scan.timestamp", "2024-01-01 00:00:00"),
        ("scan.watermark", "0"),
        ("incremental-between", "1,2"),
        ("incremental-between-timestamp", "0,1"),
        ("incremental-between-scan-mode", "delta"),
        ("scan.file-creation-time-millis", "0"),
        ("scan.creation-time-millis", "0"),
        ("scan.mode", "latest"),
    ] {
        run(&ctx, &format!("SET 'paimon.{key}' = '{value}'")).await;
        for name in ["t", "t$audit_log"] {
            let err = expect_error(
                &ctx,
                &format!("SELECT * FROM paimon_incremental_query('default.{name}', 1, 3)"),
            )
            .await;
            assert!(
                err.contains(&format!(
                    "scan option '{key}' is set; RESET 'paimon.{key}' first"
                )),
                "{key} on {name}: {err}"
            );
        }
        run(&ctx, &format!("RESET 'paimon.{key}'")).await;
    }
    run(&ctx, "SET 'paimon.scan.mode' = 'default'").await;
    let batches = run(
        &ctx,
        "SELECT * FROM paimon_incremental_query('default.t', 1, 3)",
    )
    .await;
    assert_eq!(sorted_ids(&batches), vec![2, 3]);
    run(&ctx, "RESET 'paimon.scan.mode'").await;

    // The table's own options choose no range either.
    run(
        &ctx,
        "CREATE TABLE paimon.default.latest (id INT) WITH ('scan.mode' = 'latest')",
    )
    .await;
    run(&ctx, "INSERT INTO paimon.default.latest VALUES (1)").await;
    let err = expect_error(
        &ctx,
        "SELECT * FROM paimon_incremental_query('default.latest', 0, 1)",
    )
    .await;
    assert!(
        err.contains("table 'default.latest', which sets scan option 'scan.mode'"),
        "{err}"
    );
    // A stored time-travel selector is refused too, with or without other
    // session options.
    run(&ctx, "CREATE TABLE paimon.default.tagged (id INT)").await;
    run(&ctx, "INSERT INTO paimon.default.tagged VALUES (1)").await;
    run(
        &ctx,
        "ALTER TABLE paimon.default.tagged SET TBLPROPERTIES('scan.tag-name' = 'not_a_tag')",
    )
    .await;
    for setting in [None, Some("read.batch-size")] {
        if let Some(key) = setting {
            run(&ctx, &format!("SET 'paimon.{key}' = '2'")).await;
        }
        let err = expect_error(
            &ctx,
            "SELECT * FROM paimon_incremental_query('default.tagged', 0, 1)",
        )
        .await;
        assert!(
            err.contains("table 'default.tagged', which sets scan option 'scan.tag-name'"),
            "{setting:?}: {err}"
        );
        if let Some(key) = setting {
            run(&ctx, &format!("RESET 'paimon.{key}'")).await;
        }
    }
}

#[tokio::test]
async fn test_range_options_are_checked_on_the_branch_read() {
    use paimon::table::{BranchManager, SnapshotManager, TagManager};
    // A session `branch` reads that branch, so its options decide: `on_branch`
    // stores `scan.mode=latest` only on b1, `on_main` only on the main branch.
    let tmp = TempDir::new().unwrap();
    let ctx = context(&tmp).await;
    let warehouse = catalog(&tmp);
    for (table, created, altered) in [
        ("on_branch", "latest", "default"),
        ("on_main", "default", "latest"),
    ] {
        run(
            &ctx,
            &format!(
                "CREATE TABLE paimon.default.{table} (id INT) WITH ('scan.mode' = '{created}')"
            ),
        )
        .await;
        run(
            &ctx,
            &format!("INSERT INTO paimon.default.{table} VALUES (1)"),
        )
        .await;
        let loaded = warehouse
            .get_table(&Identifier::new("default", table))
            .await
            .unwrap();
        let (io, location) = (loaded.file_io().clone(), loaded.location().to_string());
        let snapshot = SnapshotManager::new(io.clone(), location.clone())
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        TagManager::new(io.clone(), location.clone())
            .create("base", &snapshot)
            .await
            .unwrap();
        BranchManager::new(io, location)
            .create_branch_from_tag("b1", "base")
            .await
            .unwrap();
        run(
            &ctx,
            &format!(
                "ALTER TABLE paimon.default.{table} SET TBLPROPERTIES('scan.mode' = '{altered}')"
            ),
        )
        .await;
    }

    let query =
        |table: &str| format!("SELECT id FROM paimon_incremental_query('default.{table}', 0, 1)");
    assert_eq!(sorted_ids(&run(&ctx, &query("on_branch")).await), vec![1]);
    let err = expect_error(&ctx, &query("on_main")).await;
    assert!(
        err.contains("table 'default.on_main', which sets scan option 'scan.mode'"),
        "{err}"
    );
    run(&ctx, "SET 'paimon.branch' = 'b1'").await;
    let err = expect_error(&ctx, &query("on_branch")).await;
    assert!(
        err.contains(
            "branch 'b1' of table 'default.on_branch', which sets scan option 'scan.mode'"
        ),
        "{err}"
    );
    assert_eq!(sorted_ids(&run(&ctx, &query("on_main")).await), vec![1]);
}

#[tokio::test]
async fn test_audit_log_refuses_the_session_options_its_ordinary_read_refuses() {
    let tmp = TempDir::new().unwrap();
    let ctx = context(&tmp).await;
    run(
        &ctx,
        "CREATE TABLE paimon.default.pk (id INT NOT NULL, PRIMARY KEY (id)) WITH ('bucket' = '1')",
    )
    .await;
    run(&ctx, "INSERT INTO paimon.default.pk VALUES (1)").await;
    run(
        &ctx,
        "SET 'paimon.table-read.sequence-number.enabled' = 'true'",
    )
    .await;
    for sql in [
        "SELECT * FROM paimon.default.pk$audit_log",
        "SELECT * FROM paimon_incremental_query('default.pk$audit_log', 0, 1)",
    ] {
        let err = expect_error(&ctx, sql).await;
        assert!(
            err.contains(
                "table-read.sequence-number.enabled is not supported by dynamic options for $audit_log"
            ),
            "{sql}: {err}"
        );
    }
}
