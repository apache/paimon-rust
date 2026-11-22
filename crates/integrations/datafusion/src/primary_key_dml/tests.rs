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
use crate::delete::execute_delete;
use datafusion::sql::sqlparser::ast::Delete;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;

use bytes::Bytes;
use datafusion::arrow::array::{Int32Array, UInt64Array};
use datafusion::arrow::datatypes::DataType as ArrowType;
use datafusion::logical_expr::{create_udf, Volatility};
use datafusion::sql::sqlparser::{ast::Statement, dialect::GenericDialect, parser::Parser};
use opendal::{services::Memory, Operator};
use paimon::catalog::Identifier;
use paimon::io::{FileIOBuilder, FileIOProvider};
use paimon::spec::{DataType as PaimonType, IntType, Schema as PaimonSchema, TableSchema};
use tokio::sync::oneshot;

#[derive(Clone, Copy, Debug)]
enum PublicationFault {
    Conflict,
    RepeatedConflict,
    LostReply,
    LostReplyAndReadFailure,
}

#[derive(Debug)]
struct PublicationBarrier {
    entered: oneshot::Sender<()>,
    resume: oneshot::Receiver<()>,
}

#[derive(Debug)]
struct FaultProvider {
    operator: Operator,
    publication_fault: Mutex<Option<PublicationFault>>,
    publication_barrier: Mutex<Option<PublicationBarrier>>,
    fail_next_data_output: AtomicBool,
    fail_snapshot_access: AtomicBool,
    publication_attempts: AtomicUsize,
}

#[async_trait::async_trait]
impl FileIOProvider for FaultProvider {
    async fn create(&self, path: &str) -> paimon::Result<(Operator, String)> {
        let relative_path = path.strip_prefix("memory:/").unwrap();
        if path.ends_with(".parquet")
            && self.fail_next_data_output.load(Ordering::SeqCst)
            && !self.operator.exists(relative_path).await?
            && self.fail_next_data_output.swap(false, Ordering::SeqCst)
        {
            return Err(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "injected mutation output failure",
            )
            .into());
        }
        if self.fail_snapshot_access.load(Ordering::SeqCst) && path.contains("/snapshot/") {
            return Err(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "injected snapshot read failure",
            )
            .into());
        }
        Ok((self.operator.clone(), relative_path.to_string()))
    }

    async fn write_atomic_if_not_exists(
        &self,
        path: &str,
        bytes: Bytes,
    ) -> paimon::Result<Option<bool>> {
        self.publication_attempts.fetch_add(1, Ordering::SeqCst);
        let fault = self.publication_fault.lock().unwrap().take();
        if matches!(fault, Some(PublicationFault::RepeatedConflict)) {
            *self.publication_fault.lock().unwrap() = fault;
            return Ok(Some(false));
        }
        if matches!(fault, Some(PublicationFault::Conflict)) {
            let barrier = self.publication_barrier.lock().unwrap().take().unwrap();
            barrier.entered.send(()).unwrap();
            barrier.resume.await.unwrap();
        }
        match self
            .operator
            .write_with(path.strip_prefix("memory:/").unwrap(), bytes)
            .if_not_exists(true)
            .await
        {
            Ok(_) => {}
            Err(error) if error.kind() == opendal::ErrorKind::AlreadyExists => {
                return Ok(Some(false))
            }
            Err(error) => return Err(error.into()),
        }
        if matches!(fault, Some(PublicationFault::LostReplyAndReadFailure)) {
            self.fail_snapshot_access.store(true, Ordering::SeqCst);
        }
        if matches!(
            fault,
            Some(PublicationFault::LostReply | PublicationFault::LostReplyAndReadFailure)
        ) {
            return Err(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "injected lost publication reply",
            )
            .into());
        }
        Ok(Some(true))
    }
}

async fn setup() -> (SQLContext, Table, Arc<FaultProvider>, Arc<AtomicUsize>) {
    let provider = Arc::new(FaultProvider {
        operator: Operator::new(Memory::default()).unwrap(),
        publication_fault: Mutex::new(None),
        publication_barrier: Mutex::new(None),
        fail_next_data_output: AtomicBool::new(false),
        fail_snapshot_access: AtomicBool::new(false),
        publication_attempts: AtomicUsize::new(0),
    });
    let io = FileIOBuilder::new("memory")
        .with_provider(provider.clone())
        .build()
        .unwrap();
    let schema = PaimonSchema::builder()
        .column("id", PaimonType::Int(IntType::new()))
        .column("v", PaimonType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("sequence.field", "v")
        .option("write-only", "true")
        .option("commit.max-retries", "2")
        .build()
        .unwrap();
    let table = Table::new(
        io.clone(),
        Identifier::new("default", "target"),
        "memory:/table".into(),
        TableSchema::new(0, &schema),
        None,
    );
    io.new_output(&table.schema_manager().schema_path(0))
        .unwrap()
        .write(serde_json::to_vec(table.schema()).unwrap().into())
        .await
        .unwrap();
    let ctx = SQLContext::new();
    ctx.ctx()
        .register_table(
            "target",
            Arc::new(PaimonTableProvider::try_new(table.clone()).unwrap()),
        )
        .unwrap();
    ctx.ctx()
        .sql("INSERT INTO target VALUES (1, 10), (2, 20)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    ctx.ctx().register_udf(create_udf(
        "count_scan",
        vec![ArrowType::Int32],
        ArrowType::Int32,
        Volatility::Volatile,
        Arc::new(move |args| {
            counted.fetch_add(1, Ordering::SeqCst);
            Ok(args[0].clone())
        }),
    ));
    (ctx, table, provider, calls)
}

fn parse_delete(sql: &str) -> Delete {
    let Statement::Delete(delete) = Parser::parse_sql(&GenericDialect {}, sql)
        .unwrap()
        .pop()
        .unwrap()
    else {
        unreachable!()
    };
    delete
}

async fn execute_counted_delete(ctx: &SQLContext, table: &Table) -> DFResult<DataFrame> {
    execute_delete(
        ctx,
        &parse_delete("DELETE FROM target WHERE count_scan(id) = 1"),
        table.clone(),
        "target",
    )
    .await
}

#[derive(Clone, Copy, Debug)]
enum Mutation {
    Delete,
    Update,
}

impl Mutation {
    async fn execute(self, ctx: &SQLContext, table: &Table) -> DFResult<DataFrame> {
        match self {
            Self::Delete => execute_counted_delete(ctx, table).await,
            Self::Update => {
                let Statement::Update(update) = Parser::parse_sql(
                    &GenericDialect {},
                    "UPDATE target SET v = v + 1 WHERE count_scan(id) = 1",
                )
                .unwrap()
                .pop()
                .unwrap() else {
                    unreachable!()
                };
                crate::update::execute_update(ctx, &update, table.clone(), true).await
            }
        }
    }

    async fn assert_committed(self, ctx: &SQLContext, table: &Table) {
        match self {
            Self::Delete => assert_table_state(ctx, table, 2, &[(2, 20)]).await,
            Self::Update => assert_table_state(ctx, table, 2, &[(1, 11), (2, 20)]).await,
        }
    }
}

async fn stored_files(provider: &FaultProvider) -> BTreeSet<String> {
    provider
        .operator
        .list_with("table/")
        .recursive(true)
        .await
        .unwrap()
        .into_iter()
        .map(|entry| entry.path().to_string())
        .collect()
}

async fn assert_table_state(
    ctx: &SQLContext,
    table: &Table,
    snapshot_id: i64,
    expected_rows: &[(i32, i32)],
) {
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        Some(snapshot_id)
    );
    let input = ctx
        .ctx()
        .sql("SELECT id, v FROM target ORDER BY id")
        .await
        .unwrap();
    assert_eq!(collect_rows(input).await, expected_rows);
}

async fn collect_rows(input: DataFrame) -> Vec<(i32, i32)> {
    let batches = input.collect().await.unwrap();
    let mut rows = batches
        .iter()
        .flat_map(|batch| {
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
            (0..batch.num_rows()).map(move |row| (ids.value(row), values.value(row)))
        })
        .collect::<Vec<_>>();
    rows.sort_unstable();
    rows
}

#[tokio::test]
async fn mutation_input_pins_live_self_references_and_preserves_selected_views() {
    for predicate in [
        None,
        Some("id IN (SELECT id FROM target WHERE v = 11)"),
        Some("v = (SELECT MIN(v) FROM target)"),
        Some("EXISTS (SELECT 1 FROM target other WHERE other.id = target.id AND other.v = 11)"),
        Some("id IN (SELECT id FROM target_view WHERE v = 11)"),
        Some("id IN (SELECT id FROM target_history WHERE v = 10)"),
    ] {
        let (ctx, table, _, _) = setup().await;
        let historical_snapshot = table.resolve_read_snapshot().await.unwrap().unwrap();
        ctx.ctx()
            .register_table(
                "target_history",
                Arc::new(
                    PaimonTableProvider::try_new(
                        table.copy_with_pinned_snapshot(Some(&historical_snapshot)),
                    )
                    .unwrap(),
                ),
            )
            .unwrap();
        for (name, view) in [
            ("target_empty", table.copy_with_pinned_snapshot(None)),
            (
                "target_incremental",
                table.copy_with_options(std::collections::HashMap::from([(
                    "incremental-between-timestamp".into(),
                    "0,1".into(),
                )])),
            ),
        ] {
            ctx.ctx()
                .register_table(name, Arc::new(PaimonTableProvider::try_new(view).unwrap()))
                .unwrap();
        }
        ctx.ctx()
            .sql("INSERT INTO target VALUES (1, 11)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let view = ctx
            .ctx()
            .sql("SELECT * FROM target")
            .await
            .unwrap()
            .into_view();
        ctx.ctx().register_table("target_view", view).unwrap();
        let sql = match predicate {
            Some(predicate) => format!("DELETE FROM target WHERE {predicate}"),
            None => "DELETE FROM target".to_string(),
        };
        let delete = parse_delete(&sql);
        let mut inputs = Vec::new();
        for (operation, projection, increment) in [
            ("DELETE", "*", 0),
            ("UPDATE", "id, v + (SELECT MIN(v) FROM target) AS v", 11),
            (
                "UPDATE",
                "id, v + (SELECT MIN(v) FROM target_view) AS v",
                11,
            ),
            (
                "UPDATE",
                "id, v + (SELECT MIN(v) FROM target_history) AS v",
                10,
            ),
            (
                "UPDATE",
                "id, v + COALESCE((SELECT MIN(v) FROM target_empty), CAST(0 AS INT)) AS v",
                0,
            ),
            (
                "UPDATE",
                "id, v + COALESCE((SELECT MIN(v) FROM target_incremental), CAST(0 AS INT)) AS v",
                0,
            ),
        ] {
            let input = plan_input(
                &ctx,
                &table,
                "target",
                projection,
                delete.selection.as_ref(),
                operation,
            )
            .await
            .unwrap();
            inputs.push((input, projection, increment));
        }
        ctx.ctx()
            .sql("INSERT INTO target VALUES (1, 30), (3, 5)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        for (input, projection, increment) in inputs {
            let expected = if predicate.is_some() {
                vec![(1, 11 + increment)]
            } else {
                vec![(1, 11 + increment), (2, 20 + increment)]
            };
            assert_eq!(collect_rows(input).await, expected, "{sql}: {projection}");
        }
    }
}

#[tokio::test]
async fn failed_mutation_staging_does_not_publish_or_leave_files() {
    for mutation in [Mutation::Delete, Mutation::Update] {
        let (ctx, table, provider, calls) = setup().await;
        let before_files = stored_files(&provider).await;
        let before_attempts = provider.publication_attempts.load(Ordering::SeqCst);
        provider.fail_next_data_output.store(true, Ordering::SeqCst);
        let error = mutation
            .execute(&ctx, &table)
            .await
            .expect_err("mutation unexpectedly succeeded");
        assert!(
            error
                .to_string()
                .contains("injected mutation output failure"),
            "{error}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            provider.publication_attempts.load(Ordering::SeqCst),
            before_attempts
        );
        assert_eq!(
            table
                .snapshot_manager()
                .get_latest_snapshot_id()
                .await
                .unwrap(),
            Some(1)
        );
        assert_eq!(stored_files(&provider).await, before_files);
        mutation.execute(&ctx, &table).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        mutation.assert_committed(&ctx, &table).await;
    }
}

#[tokio::test]
async fn publication_retries_do_not_repeat_the_mutation_scan() {
    for mutation in [Mutation::Delete, Mutation::Update] {
        for fault in [PublicationFault::Conflict, PublicationFault::LostReply] {
            let (ctx, table, provider, calls) = setup().await;
            let before = provider.publication_attempts.load(Ordering::SeqCst);
            let (entered, wait_for_publication) = oneshot::channel();
            let (resume, wait_for_upsert) = oneshot::channel();
            if matches!(fault, PublicationFault::Conflict) {
                *provider.publication_barrier.lock().unwrap() = Some(PublicationBarrier {
                    entered,
                    resume: wait_for_upsert,
                });
            }
            *provider.publication_fault.lock().unwrap() = Some(fault);
            let (result, ()) = tokio::try_join!(mutation.execute(&ctx, &table), async {
                if matches!(fault, PublicationFault::Conflict) {
                    wait_for_publication.await.unwrap();
                    ctx.ctx()
                        .sql("INSERT INTO target VALUES (1, 30), (3, 5)")
                        .await?
                        .collect()
                        .await?;
                    resume.send(()).unwrap();
                }
                Ok(())
            })
            .unwrap();
            let result = result.collect().await.unwrap();
            assert_eq!(
                result[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(0),
                1
            );
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let expected_attempts = if matches!(fault, PublicationFault::Conflict) {
                3
            } else {
                1
            };
            assert_eq!(
                provider.publication_attempts.load(Ordering::SeqCst) - before,
                expected_attempts
            );
            if matches!(fault, PublicationFault::Conflict) {
                assert_table_state(&ctx, &table, 3, &[(1, 30), (2, 20), (3, 5)]).await;
            } else {
                mutation.assert_committed(&ctx, &table).await;
            }
        }
    }
}

#[tokio::test]
async fn publication_errors_keep_prepared_mutations_without_rescanning() {
    for mutation in [Mutation::Delete, Mutation::Update] {
        for fault in [
            PublicationFault::RepeatedConflict,
            PublicationFault::LostReplyAndReadFailure,
        ] {
            let (ctx, table, provider, calls) = setup().await;
            let before_files = stored_files(&provider).await;
            let before_attempts = provider.publication_attempts.load(Ordering::SeqCst);
            *provider.publication_fault.lock().unwrap() = Some(fault);
            let error = mutation
                .execute(&ctx, &table)
                .await
                .expect_err("mutation unexpectedly succeeded");
            let DataFusionError::Context(context, source) = error else {
                panic!("expected commit failure context, got {error}");
            };
            assert!(context.contains("table 'default.target'"), "{context}");
            assert!(context.contains("matched_rows=1"), "{context}");
            assert!(context.contains("prepared files retained"), "{context}");
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let after_files = stored_files(&provider).await;
            assert!(before_files.is_subset(&after_files), "{fault:?}");
            assert_eq!(
                after_files
                    .difference(&before_files)
                    .filter(|path| path.ends_with(".parquet"))
                    .count(),
                1,
                "{fault:?} must retain the prepared mutation file"
            );
            provider.fail_snapshot_access.store(false, Ordering::SeqCst);
            let snapshot = table.resolve_read_snapshot().await.unwrap().unwrap();
            if matches!(fault, PublicationFault::RepeatedConflict) {
                assert!(source.to_string().contains("with 2 retries"), "{source}");
                assert_eq!(snapshot.id(), 1);
                assert_eq!(
                    provider.publication_attempts.load(Ordering::SeqCst) - before_attempts,
                    3
                );
            } else {
                assert!(source.to_string().contains("unknown"), "{source}");
                assert!(
                    context.contains(&format!("commit_user={}", snapshot.commit_user())),
                    "the failure must identify the published commit: {context}"
                );
                mutation.assert_committed(&ctx, &table).await;
            }
        }
    }
}

#[tokio::test]
async fn unsupported_primary_key_mutation_options_are_rejected_before_scanning() {
    for mutation in [Mutation::Delete, Mutation::Update] {
        let mut options = vec![
            ("query-auth.enabled", "true", "query-authorized"),
            ("rowkind.field", "v", "rowkind.field"),
            (
                "merge-engine",
                "partial-update",
                "requires merge-engine=deduplicate",
            ),
            (
                "merge-engine",
                "aggregation",
                "requires merge-engine=deduplicate",
            ),
            (
                "merge-engine",
                "first-row",
                "requires merge-engine=deduplicate",
            ),
        ];
        if matches!(mutation, Mutation::Delete) {
            options.extend([
                ("ignore-delete", "true", "ignore-delete"),
                ("first-row.ignore-delete", "true", "ignore-delete"),
                ("deduplicate.ignore-delete", "true", "ignore-delete"),
                ("partial-update.ignore-delete", "true", "ignore-delete"),
            ]);
        }
        for (option, value, expected_error) in options {
            let (ctx, table, provider, calls) = setup().await;
            let overridden = table.copy_with_options(std::collections::HashMap::from([(
                option.into(),
                value.into(),
            )]));
            let before = provider.publication_attempts.load(Ordering::SeqCst);
            let error = mutation
                .execute(&ctx, &overridden)
                .await
                .expect_err("mutation unexpectedly succeeded");
            assert!(
                error.to_string().contains(expected_error),
                "{mutation:?}, {option}: {error}"
            );
            ctx.ctx().deregister_table("target").unwrap();
            ctx.ctx()
                .register_table(
                    "target",
                    Arc::new(PaimonTableProvider::try_new(overridden).unwrap()),
                )
                .unwrap();
            let error = mutation
                .execute(&ctx, &table)
                .await
                .expect_err("mutation unexpectedly succeeded with overridden read options");
            assert!(
                error.to_string().contains("read options"),
                "{mutation:?}, {option}: {error}"
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0, "{mutation:?}, {option}");
            assert_eq!(
                provider.publication_attempts.load(Ordering::SeqCst),
                before,
                "{mutation:?}, {option}"
            );
        }
    }
}

#[tokio::test]
async fn replaced_target_cannot_supply_mutation_rows() {
    let (ctx, table, provider, calls) = setup().await;
    let mut replacements = Vec::new();
    for sql in [
        "SELECT CAST(1 AS INT) AS id, CAST(10 AS INT) AS v",
        "SELECT CAST(id - 1 AS INT) AS id, v FROM target",
    ] {
        replacements.push(ctx.ctx().sql(sql).await.unwrap().into_view());
    }
    let snapshot = table.resolve_read_snapshot().await.unwrap().unwrap();
    for replacement in [
        Table::new(
            table.file_io().clone(),
            table.identifier().clone(),
            "memory:/replacement".into(),
            table.schema().clone(),
            None,
        ),
        table.copy_with_pinned_snapshot(Some(&snapshot)),
        table.copy_with_options(std::collections::HashMap::from([(
            "scan.snapshot-id".into(),
            snapshot.id().to_string(),
        )])),
    ] {
        replacements.push(Arc::new(PaimonTableProvider::try_new(replacement).unwrap()));
    }
    let before = provider.publication_attempts.load(Ordering::SeqCst);
    for replacement in replacements {
        ctx.ctx().deregister_table("target").unwrap();
        ctx.ctx().register_table("target", replacement).unwrap();
        for mutation in [Mutation::Delete, Mutation::Update] {
            let error = mutation
                .execute(&ctx, &table)
                .await
                .expect_err("mutation unexpectedly succeeded");
            assert!(error.to_string().contains("target changed"), "{error}");
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(provider.publication_attempts.load(Ordering::SeqCst), before);
        assert_eq!(
            table
                .snapshot_manager()
                .get_latest_snapshot_id()
                .await
                .unwrap(),
            Some(1)
        );
    }
}

#[tokio::test]
async fn mutation_input_preserves_a_captured_empty_snapshot() {
    let (ctx, table, _, _) = setup().await;
    let empty = Table::new(
        table.file_io().clone(),
        Identifier::new("default", "empty"),
        "memory:/empty".into(),
        table.schema().clone(),
        None,
    );
    empty
        .file_io()
        .new_output(&empty.schema_manager().schema_path(0))
        .unwrap()
        .write(serde_json::to_vec(empty.schema()).unwrap().into())
        .await
        .unwrap();
    ctx.ctx()
        .register_table(
            "empty",
            Arc::new(PaimonTableProvider::try_new(empty.clone()).unwrap()),
        )
        .unwrap();
    let mut inputs = Vec::new();
    for (operation, projection) in [("DELETE", "*"), ("UPDATE", "id, v + 1 AS v")] {
        inputs.push(
            plan_input(&ctx, &empty, "empty", projection, None, operation)
                .await
                .unwrap(),
        );
    }
    ctx.ctx()
        .sql("INSERT INTO empty VALUES (1, 10)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    for input in inputs {
        assert!(collect_rows(input).await.is_empty());
    }
}
