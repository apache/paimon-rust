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

use super::*;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType as DFTableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{create_udf, ColumnarValue, Expr, Volatility};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use paimon::catalog::Database;
use paimon::io::{FileIOBuilder, FileIOProvider};
use paimon::spec::SchemaChange;
use paimon::table::Table;
use paimon::FileSystemCatalog;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Fault {
    None,
    Lookup,
    Create,
    Race,
    LoadAfterCreate,
    Write,
    WriteAfterOutput,
    BeforePublication,
    LostResponse,
    PausePublication,
}

#[derive(Debug)]
struct InjectedIO {
    operator: opendal::Operator,
    fault: Fault,
    published: AtomicBool,
    data_paths: std::sync::Mutex<Vec<String>>,
    reached: Notify,
    resume: Notify,
}

fn injected_error(message: &str) -> paimon::Error {
    paimon::Error::UnexpectedError {
        message: message.into(),
        source: None,
    }
}

fn relative(path: &str) -> &str {
    path.trim_start_matches("file://").trim_start_matches('/')
}

#[async_trait]
impl FileIOProvider for InjectedIO {
    async fn create(&self, path: &str) -> paimon::Result<(opendal::Operator, String)> {
        if path.ends_with(".parquet") {
            let mut paths = self.data_paths.lock().unwrap();
            let index = paths
                .iter()
                .position(|entry| entry == path)
                .unwrap_or_else(|| {
                    paths.push(path.into());
                    paths.len() - 1
                });
            if self.fault == Fault::Write || self.fault == Fault::WriteAfterOutput && index == 1 {
                return Err(injected_error("injected data write failure"));
            }
        }
        if self.published.load(Ordering::SeqCst)
            && self.fault == Fault::LostResponse
            && path.contains("/snapshot/")
        {
            return Err(injected_error("injected snapshot read failure"));
        }
        Ok((self.operator.clone(), relative(path).into()))
    }

    async fn write_atomic_if_not_exists(
        &self,
        path: &str,
        bytes: Bytes,
    ) -> paimon::Result<Option<bool>> {
        if path.contains("/snapshot/snapshot-") {
            if self.fault == Fault::PausePublication {
                self.reached.notify_one();
                self.resume.notified().await;
                self.operator.write(relative(path), bytes).await.unwrap();
                self.published.store(true, Ordering::SeqCst);
                return Ok(Some(true));
            }
            if self.fault == Fault::BeforePublication {
                return Err(injected_error("injected publication failure"));
            }
            if self.fault == Fault::LostResponse {
                self.operator.write(relative(path), bytes).await.unwrap();
                self.published.store(true, Ordering::SeqCst);
                return Err(injected_error("injected lost commit response"));
            }
        }
        Ok(None)
    }
}

#[derive(Debug)]
struct TestCatalog {
    inner: Arc<FileSystemCatalog>,
    fault: Fault,
    creates: AtomicUsize,
    drops: AtomicUsize,
    io: Option<Arc<InjectedIO>>,
}

impl TestCatalog {
    fn new(inner: Arc<FileSystemCatalog>, fault: Fault) -> Arc<Self> {
        let io = matches!(
            fault,
            Fault::Write
                | Fault::WriteAfterOutput
                | Fault::BeforePublication
                | Fault::LostResponse
                | Fault::PausePublication
        )
        .then(|| {
            Arc::new(InjectedIO {
                operator:
                    opendal::Operator::from_config(opendal::services::MemoryConfig::default())
                        .unwrap(),
                fault,
                published: AtomicBool::new(false),
                data_paths: std::sync::Mutex::new(Vec::new()),
                reached: Notify::new(),
                resume: Notify::new(),
            })
        });
        Arc::new(Self {
            inner,
            fault,
            creates: AtomicUsize::new(0),
            drops: AtomicUsize::new(0),
            io,
        })
    }
}

async fn append(table: &Table, value: i64) {
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "id",
        ArrowType::Int64,
        false,
    )]));
    writer
        .write_arrow_batch(
            &RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![value]))]).unwrap(),
        )
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    builder.new_commit().commit(messages).await.unwrap();
}

#[async_trait]
impl Catalog for TestCatalog {
    async fn list_databases(&self) -> paimon::Result<Vec<String>> {
        self.inner.list_databases().await
    }
    async fn create_database(
        &self,
        name: &str,
        ignore: bool,
        properties: HashMap<String, String>,
    ) -> paimon::Result<()> {
        self.inner.create_database(name, ignore, properties).await
    }
    async fn get_database(&self, name: &str) -> paimon::Result<Database> {
        self.inner.get_database(name).await
    }
    async fn drop_database(&self, name: &str, ignore: bool, cascade: bool) -> paimon::Result<()> {
        self.inner.drop_database(name, ignore, cascade).await
    }
    async fn get_table(&self, id: &Identifier) -> paimon::Result<Table> {
        if self.fault == Fault::Lookup {
            return Err(injected_error("injected forbidden lookup"));
        }
        if self.fault == Fault::LoadAfterCreate && self.creates.load(Ordering::SeqCst) > 0 {
            return Err(injected_error("injected table load failure"));
        }
        let table = self.inner.get_table(id).await?;
        if let Some(io) = &self.io {
            let schema_path = format!("{}/schema/schema-0", table.location());
            if !io.operator.exists(relative(&schema_path)).await.unwrap() {
                io.operator
                    .write(
                        relative(&schema_path),
                        serde_json::to_vec(table.schema()).unwrap(),
                    )
                    .await
                    .unwrap();
            }
            Ok(Table::new(
                FileIOBuilder::new("memory")
                    .with_provider(io.clone())
                    .build()?,
                id.clone(),
                table.location().to_string(),
                table.schema().clone(),
                None,
            ))
        } else {
            Ok(table)
        }
    }
    async fn list_tables(&self, db: &str) -> paimon::Result<Vec<String>> {
        self.inner.list_tables(db).await
    }
    async fn create_table(
        &self,
        id: &Identifier,
        schema: Schema,
        ignore: bool,
    ) -> paimon::Result<()> {
        self.creates.fetch_add(1, Ordering::SeqCst);
        assert!(
            !ignore,
            "CTAS must use strict creation even with IF NOT EXISTS"
        );
        if self.fault == Fault::Create {
            return Err(injected_error("injected table creation failure"));
        }
        if self.fault == Fault::Race {
            self.inner.create_table(id, schema.clone(), false).await?;
            append(&self.inner.get_table(id).await?, 77).await;
        }
        self.inner.create_table(id, schema, ignore).await
    }
    async fn drop_table(&self, id: &Identifier, ignore: bool) -> paimon::Result<()> {
        self.drops.fetch_add(1, Ordering::SeqCst);
        self.inner.drop_table(id, ignore).await
    }
    async fn rename_table(
        &self,
        from: &Identifier,
        to: &Identifier,
        ignore: bool,
    ) -> paimon::Result<()> {
        self.inner.rename_table(from, to, ignore).await
    }
    async fn alter_table(
        &self,
        id: &Identifier,
        changes: Vec<SchemaChange>,
        ignore: bool,
    ) -> paimon::Result<()> {
        self.inner.alter_table(id, changes, ignore).await
    }
}

#[derive(Debug)]
struct ProbeSource {
    schema: Arc<ArrowSchema>,
    scans: AtomicUsize,
    executions: Arc<AtomicUsize>,
    fail: bool,
    concurrent_writer: Option<Arc<FileSystemCatalog>>,
}
impl ProbeSource {
    fn new(fail: bool, concurrent_writer: Option<Arc<FileSystemCatalog>>) -> Arc<Self> {
        Arc::new(Self {
            schema: Arc::new(ArrowSchema::new(vec![Field::new(
                "id",
                ArrowType::Int64,
                false,
            )])),
            scans: AtomicUsize::new(0),
            executions: Arc::new(AtomicUsize::new(0)),
            fail,
            concurrent_writer,
        })
    }
}
#[derive(Debug)]
struct ProbePartition {
    schema: Arc<ArrowSchema>,
    executions: Arc<AtomicUsize>,
    fail: bool,
    concurrent_writer: Option<Arc<FileSystemCatalog>>,
}
impl PartitionStream for ProbePartition {
    fn schema(&self) -> &Arc<ArrowSchema> {
        &self.schema
    }
    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        self.executions.fetch_add(1, Ordering::SeqCst);
        let first = RecordBatch::try_new(
            self.schema.clone(),
            vec![Arc::new(Int64Array::from(vec![11]))],
        )
        .unwrap();
        let concurrent_writer = self.concurrent_writer.clone();
        let fail = self.fail;
        let schema = self.schema.clone();
        let stream =
            futures::stream::once(async { Ok(first) }).chain(futures::stream::once(async move {
                if let Some(catalog) = concurrent_writer {
                    let table = catalog
                        .get_table(&Identifier::new("default", "target"))
                        .await
                        .unwrap();
                    append(&table, 88).await;
                }
                if fail {
                    Err(DataFusionError::Execution("injected source failure".into()))
                } else {
                    Ok(RecordBatch::try_new(
                        schema,
                        vec![Arc::new(Int64Array::from(vec![12]))],
                    )?)
                }
            }));
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
    }
}
#[async_trait]
impl TableProvider for ProbeSource {
    fn schema(&self) -> Arc<ArrowSchema> {
        self.schema.clone()
    }
    fn table_type(&self) -> DFTableType {
        DFTableType::Base
    }
    async fn scan(
        &self,
        _: &dyn Session,
        projection: Option<&Vec<usize>>,
        _: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.scans.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(StreamingTableExec::try_new(
            self.schema.clone(),
            vec![Arc::new(ProbePartition {
                schema: self.schema.clone(),
                executions: self.executions.clone(),
                fail: self.fail,
                concurrent_writer: self.concurrent_writer.clone(),
            })],
            projection,
            [],
            false,
            limit,
        )?))
    }
}

async fn context(catalog: Arc<TestCatalog>, source: Arc<ProbeSource>) -> SQLContext {
    let mut ctx = SQLContext::new();
    ctx.register_catalog("paimon", catalog).await.unwrap();
    ctx.register_temp_table("source", source).unwrap();
    ctx
}

#[tokio::test]
async fn one_source_plan_is_reused_for_inference_and_execution() {
    let (_tmp, inner) = create_test_env();
    let catalog = TestCatalog::new(inner, Fault::None);
    let source = ProbeSource::new(false, None);
    let ctx = context(catalog.clone(), source.clone()).await;
    let observed_rows = Arc::new(AtomicUsize::new(0));
    let counter = observed_rows.clone();
    ctx.ctx().register_udf(create_udf(
        "observe",
        vec![ArrowType::Int64],
        ArrowType::Int64,
        Volatility::Volatile,
        Arc::new(move |args| {
            let ColumnarValue::Array(ids) = &args[0] else {
                panic!("expected the source column");
            };
            counter.fetch_add(ids.len(), Ordering::SeqCst);
            Ok(args[0].clone())
        }),
    ));
    let query = "SELECT observe(id) AS id FROM source";
    exec(&ctx, &format!("CREATE TABLE target AS {query}")).await;
    assert_eq!(source.scans.load(Ordering::SeqCst), 1);
    assert_eq!(source.executions.load(Ordering::SeqCst), 1);
    assert_eq!(query_ids(&ctx, "SELECT * FROM target").await, [11, 12]);
    assert_eq!(catalog.creates.load(Ordering::SeqCst), 1);
    exec(
        &ctx,
        &format!("CREATE TABLE IF NOT EXISTS target AS {query}"),
    )
    .await;
    assert_eq!(source.scans.load(Ordering::SeqCst), 1);
    assert_eq!(source.executions.load(Ordering::SeqCst), 1);
    assert_eq!(observed_rows.load(Ordering::SeqCst), 2);
    assert_eq!(catalog.creates.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn create_race_never_populates_or_drops_the_winner() {
    for if_not_exists in [false, true] {
        let (_tmp, inner) = create_test_env();
        let catalog = TestCatalog::new(inner.clone(), Fault::Race);
        let source = ProbeSource::new(false, None);
        let ctx = context(catalog.clone(), source.clone()).await;
        let clause = if if_not_exists { "IF NOT EXISTS " } else { "" };
        let result = ctx
            .sql(&format!(
                "CREATE TABLE {clause}target AS SELECT * FROM source"
            ))
            .await;
        assert_eq!(result.is_ok(), if_not_exists);
        if let Err(error) = result {
            assert!(error.to_string().contains("already exists"), "{error}");
        }
        assert_eq!(catalog.creates.load(Ordering::SeqCst), 1);
        assert_eq!(source.executions.load(Ordering::SeqCst), 0);
        assert_eq!(catalog.drops.load(Ordering::SeqCst), 0);
        assert_eq!(query_ids(&ctx, "SELECT * FROM target").await, [77]);
    }
}

#[tokio::test]
async fn catalog_failures_do_not_execute_the_source_or_drop_the_target() {
    for (fault, expected_error) in [
        (Fault::Lookup, "injected forbidden lookup"),
        (Fault::Create, "injected table creation failure"),
        (Fault::LoadAfterCreate, "injected table load failure"),
    ] {
        for if_not_exists in [false, true] {
            let (_tmp, inner) = create_test_env();
            let catalog = TestCatalog::new(inner.clone(), fault);
            let source = ProbeSource::new(false, None);
            let ctx = context(catalog.clone(), source.clone()).await;
            let clause = if if_not_exists { "IF NOT EXISTS " } else { "" };
            assert_sql_error(
                &ctx,
                &format!("CREATE TABLE {clause}target AS SELECT * FROM source"),
                expected_error,
            )
            .await;
            let planned = usize::from(fault != Fault::Lookup);
            assert_eq!(catalog.creates.load(Ordering::SeqCst), planned);
            assert_eq!(source.scans.load(Ordering::SeqCst), planned);
            assert_eq!(source.executions.load(Ordering::SeqCst), 0);
            assert_eq!(catalog.drops.load(Ordering::SeqCst), 0);
            let target = inner.get_table(&Identifier::new("default", "target")).await;
            if fault == Fault::LoadAfterCreate {
                assert!(target
                    .unwrap()
                    .snapshot_manager()
                    .get_latest_snapshot()
                    .await
                    .unwrap()
                    .is_none());
            } else {
                assert!(matches!(target, Err(paimon::Error::TableNotExist { .. })));
            }
        }
    }
}

#[tokio::test]
async fn population_preserves_concurrent_rows_and_cleans_failed_output() {
    for (fail, concurrent) in [(true, false), (true, true), (false, true)] {
        let (tmp, inner) = create_test_env();
        let catalog = TestCatalog::new(inner.clone(), Fault::None);
        let source = ProbeSource::new(fail, concurrent.then_some(inner));
        let ctx = context(catalog.clone(), source.clone()).await;
        let sql = "CREATE TABLE target WITH ('target-file-size' = '1 b') AS SELECT * FROM source";
        if fail {
            assert_sql_error(&ctx, sql, "injected source failure").await;
        } else {
            exec(&ctx, sql).await;
        }
        assert_eq!(catalog.drops.load(Ordering::SeqCst), 0);
        let mut expected = if fail { vec![] } else { vec![11, 12] };
        if concurrent {
            expected.push(88);
        }
        assert_eq!(query_ids(&ctx, "SELECT * FROM target").await, expected);
        let data_files = common::files_under(tmp.path())
            .iter()
            .filter(|path| path.extension().is_some_and(|ext| ext == "parquet"))
            .count();
        assert_eq!(
            data_files,
            expected.len(),
            "only committed output should remain: fail={fail}, concurrent={concurrent}"
        );
        exec(
            &ctx,
            "CREATE TABLE IF NOT EXISTS target AS SELECT * FROM source",
        )
        .await;
        assert_eq!(catalog.creates.load(Ordering::SeqCst), 1);
        assert_eq!(source.executions.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn write_and_publication_failures_keep_the_table_and_uncertain_files() {
    for fault in [
        Fault::Write,
        Fault::WriteAfterOutput,
        Fault::BeforePublication,
        Fault::LostResponse,
    ] {
        let (_tmp, inner) = create_test_env();
        let catalog = TestCatalog::new(inner, fault);
        let source = ProbeSource::new(false, None);
        let ctx = context(catalog.clone(), source.clone()).await;
        let error = ctx
            .sql("CREATE TABLE target WITH ('target-file-size' = '1 b', 'commit.max-retries' = '0') AS SELECT * FROM source")
            .await
            .expect_err("injected failure")
            .to_string();
        assert!(error.contains("injected"), "{fault:?}: {error}");
        assert_eq!(catalog.drops.load(Ordering::SeqCst), 0);
        catalog
            .inner
            .get_table(&Identifier::new("default", "target"))
            .await
            .unwrap();
        let io = catalog.io.as_ref().unwrap();
        let entries = io.operator.list_with("/").recursive(true).await.unwrap();
        let data = entries
            .iter()
            .filter(|entry| entry.path().ends_with(".parquet"))
            .count();
        if matches!(fault, Fault::Write | Fault::WriteAfterOutput) {
            assert_eq!(data, 0);
            assert_eq!(
                io.data_paths.lock().unwrap().len(),
                if fault == Fault::Write { 1 } else { 2 }
            );
            assert!(error.contains("no commit was attempted"), "{error}");
        } else {
            assert!(
                data > 0,
                "prepared files must survive uncertain publication"
            );
            assert!(error.contains("publication may have succeeded"), "{error}");
        }
        assert_eq!(
            io.published.load(Ordering::SeqCst),
            fault == Fault::LostResponse
        );
        exec(
            &ctx,
            "CREATE TABLE IF NOT EXISTS target AS SELECT * FROM source",
        )
        .await;
        assert_eq!(catalog.creates.load(Ordering::SeqCst), 1);
        assert_eq!(source.executions.load(Ordering::SeqCst), 1);
        if fault == Fault::LostResponse {
            io.published.store(false, Ordering::SeqCst);
            assert_eq!(query_ids(&ctx, "SELECT * FROM target").await, [11, 12]);
        }
    }
}

#[derive(Debug)]
struct PausedSource {
    schema: Arc<ArrowSchema>,
    reached: Arc<Notify>,
    resume: Arc<Notify>,
    early_rows: usize,
}

impl PartitionStream for PausedSource {
    fn schema(&self) -> &Arc<ArrowSchema> {
        &self.schema
    }
    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        let batch = |value, rows| {
            RecordBatch::try_new(
                self.schema.clone(),
                vec![Arc::new(Int64Array::from(vec![value; rows]))],
            )
            .unwrap()
        };
        let first = batch(12, self.early_rows);
        let second = batch(11, 1);
        let reached = self.reached.clone();
        let resume = self.resume.clone();
        let input = futures::stream::iter([Ok(first)]).chain(futures::stream::once(async move {
            reached.notify_one();
            resume.notified().await;
            Ok(second)
        }));
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), input))
    }
}

fn paused_source(ctx: &SQLContext, early_rows: usize) -> (Arc<Notify>, Arc<Notify>) {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "id",
        ArrowType::Int64,
        false,
    )]));
    let reached = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    let source = PausedSource {
        schema: schema.clone(),
        reached: reached.clone(),
        resume: resume.clone(),
        early_rows,
    };
    let table = StreamingTable::try_new(schema, vec![Arc::new(source)]).unwrap();
    ctx.register_temp_table("probe", Arc::new(table)).unwrap();
    (reached, resume)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_discards_uncommitted_output() {
    let (tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;
    let (_reached, _resume) = paused_source(&ctx, 24576);
    let statement = tokio::spawn(async move {
        ctx.sql("CREATE TABLE result WITH ('target-file-size' = '1 b') AS SELECT id FROM probe")
            .await
            .map(|_| ())
    });
    let outputs = || {
        common::files_under(tmp.path())
            .into_iter()
            .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
            .collect::<Vec<_>>()
    };
    tokio::time::timeout(Duration::from_secs(10), async {
        while !outputs()
            .iter()
            .any(|path| path.metadata().is_ok_and(|metadata| metadata.len() > 0))
        {
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    })
    .await
    .unwrap();
    statement.abort();
    assert!(statement.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(10), async {
        while !outputs().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(outputs().is_empty(), "orphan files: {:?}", outputs());
    let table = catalog
        .get_table(&Identifier::new("default", "result"))
        .await
        .unwrap();
    assert!(table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .is_none());
    let fresh = create_sql_context(catalog).await;
    assert_eq!(row_count(&fresh, "SELECT * FROM result").await, 0);
}

#[tokio::test]
async fn cancellation_finishes_started_publication() {
    let (_tmp, inner) = create_test_env();
    let catalog = TestCatalog::new(inner, Fault::PausePublication);
    let ctx = Arc::new(context(catalog.clone(), ProbeSource::new(false, None)).await);
    let writer_ctx = ctx.clone();
    let statement = tokio::spawn(async move {
        writer_ctx
            .sql("CREATE TABLE target AS SELECT * FROM source")
            .await
            .map(|_| ())
    });
    let io = catalog.io.as_ref().unwrap();
    tokio::time::timeout(Duration::from_secs(10), io.reached.notified())
        .await
        .unwrap();
    statement.abort();
    assert!(statement.await.unwrap_err().is_cancelled());
    io.resume.notify_one();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !io.published.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(query_ids(&ctx, "SELECT id FROM target").await, [11, 12]);
}
