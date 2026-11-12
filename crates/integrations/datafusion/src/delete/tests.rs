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

#[derive(Clone, Copy, Debug)]
enum PublicationFault {
    Conflict,
    LostReply,
    LostReplyAndReadFailure,
}

#[derive(Debug)]
struct FaultProvider {
    operator: Operator,
    publication_fault: Mutex<Option<PublicationFault>>,
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
                "injected tombstone output failure",
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
        if matches!(fault, Some(PublicationFault::Conflict)) {
            return Ok(Some(false));
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
        if fault.is_some() {
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
    execute_primary_key_delete(
        ctx,
        &parse_delete("DELETE FROM target WHERE count_scan(id) = 1"),
        table,
        "target",
    )
    .await
}

async fn assert_committed_delete(ctx: &SQLContext, table: &Table) {
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        Some(2)
    );
    let batches = ctx
        .ctx()
        .sql("SELECT id FROM target")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let ids = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![2]);
}

#[tokio::test]
async fn target_and_self_references_read_the_captured_snapshot() {
    for predicate in [
        "id IN (SELECT id FROM target WHERE v = 10)",
        "v = (SELECT MIN(v) FROM target)",
        "EXISTS (SELECT 1 FROM target other WHERE other.id = target.id AND other.v = 10)",
        "id IN (SELECT id FROM target_view WHERE v = 10)",
    ] {
        let (ctx, table, _, _) = setup().await;
        let view = ctx
            .ctx()
            .sql("SELECT * FROM target")
            .await
            .unwrap()
            .into_view();
        ctx.ctx().register_table("target_view", view).unwrap();
        let input = plan_primary_key_delete_input(
            &ctx,
            &parse_delete(&format!("DELETE FROM target WHERE {predicate}")),
            &table,
            "target",
        )
        .await
        .unwrap()
        .unwrap();
        ctx.ctx()
            .sql("INSERT INTO target VALUES (1, 30), (3, 5)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let batches = input.collect().await.unwrap();
        let rows = batches
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
        assert_eq!(rows, vec![(1, 10)], "{predicate}");
    }
}

#[tokio::test]
async fn failed_tombstone_staging_does_not_publish_or_leave_files() {
    let (ctx, table, provider, calls) = setup().await;
    let before_files = provider
        .operator
        .list_with("table/")
        .recursive(true)
        .await
        .unwrap();
    let before_attempts = provider.publication_attempts.load(Ordering::SeqCst);
    provider.fail_next_data_output.store(true, Ordering::SeqCst);
    let error = execute_counted_delete(&ctx, &table)
        .await
        .expect_err("DELETE unexpectedly succeeded");
    assert!(
        error
            .to_string()
            .contains("injected tombstone output failure"),
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
    let after_files = provider
        .operator
        .list_with("table/")
        .recursive(true)
        .await
        .unwrap();
    let paths = |entries: Vec<opendal::Entry>| {
        entries
            .into_iter()
            .map(|entry| entry.path().to_string())
            .collect::<std::collections::BTreeSet<_>>()
    };
    assert_eq!(paths(after_files), paths(before_files));
    execute_counted_delete(&ctx, &table).await.unwrap();
    assert_committed_delete(&ctx, &table).await;
}

#[tokio::test]
async fn publication_retries_do_not_repeat_the_delete_scan() {
    for fault in [PublicationFault::Conflict, PublicationFault::LostReply] {
        let (ctx, table, provider, calls) = setup().await;
        let before = provider.publication_attempts.load(Ordering::SeqCst);
        *provider.publication_fault.lock().unwrap() = Some(fault);
        let result = execute_counted_delete(&ctx, &table)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
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
            2
        } else {
            1
        };
        assert_eq!(
            provider.publication_attempts.load(Ordering::SeqCst) - before,
            expected_attempts
        );
        assert_committed_delete(&ctx, &table).await;
    }
}

#[tokio::test]
async fn uncertain_publication_keeps_committed_tombstones() {
    let (ctx, table, provider, calls) = setup().await;
    *provider.publication_fault.lock().unwrap() = Some(PublicationFault::LostReplyAndReadFailure);
    let error = execute_counted_delete(&ctx, &table)
        .await
        .expect_err("DELETE unexpectedly succeeded");
    let DataFusionError::Context(context, source) = error else {
        panic!("expected commit failure context, got {error}");
    };
    assert!(context.contains("table 'default.target'"), "{context}");
    assert!(context.contains("matched_rows=1"), "{context}");
    assert!(context.contains("prepared files retained"), "{context}");
    assert!(source.to_string().contains("unknown"), "{source}");
    provider.fail_snapshot_access.store(false, Ordering::SeqCst);
    let snapshot = table.resolve_read_snapshot().await.unwrap().unwrap();
    assert!(
        context.contains(&format!("commit_user={}", snapshot.commit_user())),
        "the failure must identify the published commit: {context}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_committed_delete(&ctx, &table).await;
}

#[tokio::test]
async fn unsupported_primary_key_delete_options_are_rejected_before_scanning() {
    for (option, value, expected_error) in [
        ("query-auth.enabled", "true", "query-authorized"),
        ("ignore-delete", "true", "ignore-delete"),
        ("deduplicate.ignore-delete", "true", "ignore-delete"),
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
    ] {
        let (ctx, table, provider, calls) = setup().await;
        let table = table.copy_with_options(std::collections::HashMap::from([(
            option.into(),
            value.into(),
        )]));
        let before = provider.publication_attempts.load(Ordering::SeqCst);
        let error = execute_counted_delete(&ctx, &table)
            .await
            .expect_err("DELETE unexpectedly succeeded");
        assert!(error.to_string().contains(expected_error), "{error}");
        assert_eq!(calls.load(Ordering::SeqCst), 0, "{option}");
        assert_eq!(
            provider.publication_attempts.load(Ordering::SeqCst),
            before,
            "{option}"
        );
    }
}

#[tokio::test]
async fn replaced_target_cannot_supply_delete_keys() {
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
    ] {
        replacements.push(Arc::new(PaimonTableProvider::try_new(replacement).unwrap()));
    }
    let before = provider.publication_attempts.load(Ordering::SeqCst);
    for replacement in replacements {
        ctx.ctx().deregister_table("target").unwrap();
        ctx.ctx().register_table("target", replacement).unwrap();
        let error = execute_counted_delete(&ctx, &table)
            .await
            .expect_err("DELETE unexpectedly succeeded");
        assert!(error.to_string().contains("target changed"), "{error}");
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
