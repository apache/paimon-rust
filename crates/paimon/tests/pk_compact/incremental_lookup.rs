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
use arrow_array::{
    cast::AsArray,
    types::{Int32Type, Int64Type},
};
use opendal::Operator;
use paimon::io::FileIOProvider;
use paimon::table::CommitMessage;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug)]
struct HistoryProbe {
    operator: Operator,
    history: Mutex<String>,
    opens: AtomicUsize,
    data_opens: Mutex<HashMap<String, usize>>,
}

#[async_trait::async_trait]
impl FileIOProvider for HistoryProbe {
    async fn create(&self, path: &str) -> paimon::Result<(Operator, String)> {
        if path.ends_with(".parquet") {
            *self
                .data_opens
                .lock()
                .unwrap()
                .entry(path.into())
                .or_default() += 1;
        }
        if path.rsplit('/').next() == Some(self.history.lock().unwrap().as_str()) {
            self.opens.fetch_add(1, Ordering::SeqCst);
        }
        Ok((
            self.operator.clone(),
            path.strip_prefix("memory:/").unwrap().into(),
        ))
    }

    async fn create_with_cache_namespace(
        &self,
        path: &str,
    ) -> paimon::Result<(Operator, String, Option<String>)> {
        let (operator, path) = self.create(path).await?;
        let namespace = format!("{:p}", Arc::as_ptr(operator.service()));
        Ok((operator, path, Some(namespace)))
    }
}

async fn probed_table() -> (Table, Arc<HistoryProbe>) {
    probed_table_with_options(&[]).await
}

async fn probed_table_with_options(properties: &[(&str, &str)]) -> (Table, Arc<HistoryProbe>) {
    let path = format!("memory:/incremental_lookup/{}", uuid::Uuid::new_v4());
    let mut options = paimon::Options::new();
    options.set("warehouse", &path);
    options.set("local-cache.enabled", "true");
    // Ordinary data blocks cannot explain the warm lookup result.
    options.set("local-cache.whitelist", "meta");
    let catalog = paimon::catalog::FileSystemCatalog::new(options).unwrap();
    let probe = Arc::new(HistoryProbe {
        operator: Operator::from_config(opendal::services::MemoryConfig::default()).unwrap(),
        history: Mutex::new(String::new()),
        opens: AtomicUsize::new(0),
        data_opens: Mutex::new(HashMap::new()),
    });
    let io = catalog.file_io().clone().with_provider(probe.clone());
    let mut configured = vec![("changelog-producer", "lookup")];
    configured.extend_from_slice(properties);
    let table = Table::new(
        io.clone(),
        paimon::catalog::Identifier::new("default", "incremental_lookup"),
        path.clone(),
        pk_schema(&configured),
        None,
    );
    setup_dirs(&io, &path).await;
    persist_table_schema(&io, &path, table.schema()).await;
    (table, probe)
}

async fn compact(table: &Table) -> Vec<CompactResult> {
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let results = execute(table, &plan).await;
    builder
        .new_commit()
        .unwrap()
        .commit_compact(results.clone())
        .await
        .unwrap();
    results
}

async fn seed(table: &Table) -> paimon::spec::DataFileMeta {
    let keys = (0..2000).step_by(2).collect::<Vec<_>>();
    write_batch(table, &make_batch(keys.clone(), keys)).await;
    let anchor = compact(table).await[0].files()[0].clone();
    assert_eq!(anchor.level, 5);
    anchor
}

async fn latest(table: &Table) -> i64 {
    table
        .snapshot_manager()
        .get_latest_snapshot_id()
        .await
        .unwrap()
        .unwrap()
}

async fn events(table: &Table, start: i64, end: i64) -> Vec<(String, i32, i64, i64)> {
    let id_name = table.schema().fields()[0].name();
    let value_name = table.schema().fields()[1].name();
    let mut reader = table.new_read_builder();
    reader
        .with_projection(&[id_name, value_name, "_SEQUENCE_NUMBER"])
        .unwrap();
    let plan = reader
        .new_incremental_scan(IncrementalScanMode::Auto, start, end)
        .plan_combined()
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = reader
        .new_read()
        .unwrap()
        .to_arrow_with_row_kind(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut result = Vec::new();
    for batch in batches {
        let kinds = batch.column(0).as_string::<i32>();
        let column = |name| batch.column_by_name(name).unwrap();
        let ids = column(id_name).as_primitive::<Int32Type>();
        let values = arrow_cast::cast(column(value_name), &arrow_schema::DataType::Int64).unwrap();
        let values = values.as_primitive::<Int64Type>();
        let seqs = column("_SEQUENCE_NUMBER").as_primitive::<Int64Type>();
        result.extend((0..batch.num_rows()).map(|i| {
            (
                kinds.value(i).to_string(),
                ids.value(i),
                values.value(i),
                seqs.value(i),
            )
        }));
    }
    result.sort();
    result
}

async fn cold_and_hot(
    table: &Table,
    plan: &CompactPlan,
    probe: &HistoryProbe,
    anchor: &paimon::spec::DataFileMeta,
) -> (Vec<CompactResult>, Vec<CompactResult>) {
    probe.opens.store(0, Ordering::SeqCst);
    let cold = execute(table, plan).await;
    assert!(
        probe.opens.load(Ordering::SeqCst) > 1,
        "cold lookup loads history data"
    );
    let history_path = format!("{}/bucket-0/{}", table.location(), anchor.file_name);
    let relative = history_path.strip_prefix("memory:/").unwrap();
    let backup = probe.operator.read(relative).await.unwrap();
    probe.operator.delete(relative).await.unwrap();
    probe.opens.store(0, Ordering::SeqCst);
    // A new executor must consume the generated lookup file: the original data
    // object is absent, and ordinary data caching is disabled by the whitelist.
    let hot = execute(table, plan).await;
    assert_eq!(
        probe.opens.load(Ordering::SeqCst),
        1,
        "only the identity opener may resolve history"
    );
    probe.operator.write(relative, backup).await.unwrap();
    (cold, hot)
}

#[tokio::test]
async fn incremental_lookup_consumes_cached_history_across_executors_and_retries() {
    let (table, probe) = probed_table().await;
    let anchor = seed(&table).await;
    let snapshot = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    let historical = table.copy_with_pinned_snapshot(Some(&snapshot));
    let initial = (0..1000).map(|row| (2 * row, 2 * row)).collect::<Vec<_>>();
    *probe.history.lock().unwrap() = anchor.file_name.clone();
    write_batch(
        &table,
        &make_batch_with_kinds(vec![42, 44, 43], vec![4200, 999, 4300], vec![0, 3, 0]),
    )
    .await;
    let builder = table
        .new_compact_builder()
        .with_commit_user("lookup-cache-retry")
        .unwrap();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    assert_eq!(plan.tasks().len(), 1);
    assert!(plan.tasks()[0].files().iter().all(|file| file.level == 0));
    let (cold, hot) = cold_and_hot(&table, &plan, &probe, &anchor).await;
    assert_eq!(cold[0].metrics().changelog_rows_written, 4);
    assert_eq!(hot[0].metrics().changelog_rows_written, 4);
    assert!(hot[0].files().iter().all(|file| file.level == 4));
    write_batch(&table, &make_batch(vec![42], vec![4201])).await;
    let commit = builder.new_commit().unwrap();
    let mut legacy = serde_json::to_value(&hot).unwrap();
    for result in legacy.as_array_mut().unwrap() {
        result["task"]
            .as_object_mut()
            .unwrap()
            .remove("output_level");
    }
    commit
        .commit_compact(serde_json::from_value(legacy).unwrap())
        .await
        .unwrap();
    let published = latest(&table).await;
    commit.commit_compact(cold).await.unwrap();
    commit.commit_compact(hot).await.unwrap();
    assert_eq!(latest(&table).await, published);
    let mut expected = vec![
        ("-U".into(), 42, 42, 21),
        ("+U".into(), 42, 4200, 1000),
        ("-D".into(), 44, 44, 22),
        ("+I".into(), 43, 4300, 1002),
    ];
    expected.sort();
    assert_eq!(events(&table, snapshot.id(), published).await, expected);
    let mut final_rows = initial.clone();
    final_rows.retain(|(key, _)| *key != 44);
    final_rows.iter_mut().find(|(key, _)| *key == 42).unwrap().1 = 4201;
    final_rows.push((43, 4300));
    final_rows.sort();
    assert_eq!(rows(&table).await, final_rows);
    assert_eq!(rows(&historical).await, initial);
    let scan = table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan()
        .await
        .unwrap();
    assert!(scan
        .splits()
        .iter()
        .flat_map(|split| split.data_files())
        .any(|file| file == &anchor));
    compact(&table).await;
    let last = latest(&table).await;
    assert_eq!(
        events(&table, published, last).await,
        vec![("+U".into(), 42, 4201, 1003), ("-U".into(), 42, 4200, 1000)]
    );
    assert_eq!(rows(&table).await, final_rows);
}

#[tokio::test]
async fn incremental_lookup_casts_old_history_after_rename_and_type_promotion() {
    use paimon::spec::{BigIntType, SchemaChange};
    let (original, probe) = probed_table().await;
    let anchor = seed(&original).await;
    *probe.history.lock().unwrap() = anchor.file_name.clone();
    write_batch(&original, &make_batch(vec![42], vec![4200])).await;
    compact(&original).await; // Prime the same physical history under schema 0.
    let start = latest(&original).await;
    let schema = original
        .schema()
        .apply_changes(vec![
            SchemaChange::rename_column("value".into(), "renamed".into()),
            SchemaChange::update_column_type("renamed".into(), DataType::BigInt(BigIntType::new())),
        ])
        .unwrap();
    persist_table_schema(original.file_io(), original.location(), &schema).await;
    let table = original.copy_with_resolved_schema(schema, "main").unwrap();
    write_batch(
        &table,
        &RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int32Array::from(vec![42, 43])) as arrow_array::ArrayRef,
            ),
            (
                "renamed",
                Arc::new(Int64Array::from(vec![4_300_000_000, 4301])) as arrow_array::ArrayRef,
            ),
        ])
        .unwrap(),
    )
    .await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let (cold, hot) = cold_and_hot(&table, &plan, &probe, &anchor).await;
    assert_eq!(cold[0].metrics().changelog_rows_written, 3);
    assert_eq!(hot[0].metrics().changelog_rows_written, 3);
    assert!(hot[0]
        .files()
        .iter()
        .all(|file| file.schema_id == table.schema().id()));
    builder
        .new_commit()
        .unwrap()
        .commit_compact(hot)
        .await
        .unwrap();
    let mut expected = vec![
        ("-U".into(), 42, 4200, 1000),
        ("+U".into(), 42, 4_300_000_000, 1001),
        ("+I".into(), 43, 4301, 1002),
    ];
    expected.sort();
    assert_eq!(events(&table, start, latest(&table).await).await, expected);
    let mut actual = Vec::new();
    for batch in read(&table, None).await {
        assert!(batch.column_by_name("value").is_none());
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_primitive::<Int32Type>();
        let values = batch
            .column_by_name("renamed")
            .unwrap()
            .as_primitive::<Int64Type>();
        actual.extend((0..batch.num_rows()).map(|row| (ids.value(row), values.value(row))));
    }
    actual.sort();
    let mut expected = (0..1000)
        .map(|row| (2 * row, i64::from(2 * row)))
        .collect::<Vec<_>>();
    expected[21].1 = 4_300_000_000;
    expected.push((43, 4301));
    expected.sort();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn incremental_lookup_allows_safe_rewrites_of_unselected_history() {
    for metadata_only in [false, true] {
        let table = table(&[("changelog-producer", "lookup")]).await;
        let anchor = seed(&table).await;
        write_batch(&table, &make_batch(vec![42], vec![9000])).await;
        let builder = table.new_compact_builder();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        assert!(plan.tasks()[0].files().iter().all(|file| file.level == 0));
        let results = execute(&table, &plan).await;
        assert!(results[0].files().iter().all(|file| file.level == 4));
        let mut replacement = anchor.clone();
        let mut change = CommitMessage::new(plan.tasks()[0].partition().to_vec(), 0, vec![]);
        if metadata_only {
            // Same-identity replacement uses two snapshots with the public API.
            change.deleted_files.push(anchor.clone());
            table
                .new_write_builder()
                .new_commit()
                .commit(vec![change])
                .await
                .unwrap();
            change = CommitMessage::new(plan.tasks()[0].partition().to_vec(), 0, vec![]);
            assert!(replacement.creation_time.take().is_some());
        } else {
            replacement.file_name = format!("data-{}-0.parquet", uuid::Uuid::new_v4());
            let bucket = format!("{}/bucket-0", table.location());
            table
                .file_io()
                .copy_file(
                    &format!("{bucket}/{}", anchor.file_name),
                    &format!("{bucket}/{}", replacement.file_name),
                )
                .await
                .unwrap();
            change.deleted_files.push(anchor);
        }
        change.new_files.push(replacement);
        table
            .new_write_builder()
            .new_commit()
            .commit(vec![change])
            .await
            .unwrap();
        let expected = rows(&table).await;
        let before = latest(&table).await;
        builder
            .new_commit()
            .unwrap()
            .commit_compact(results)
            .await
            .unwrap();
        assert_eq!(latest(&table).await, before + 1);
        assert_eq!(rows(&table).await, expected);
        assert_eq!(
            expected.iter().find(|(key, _)| *key == 42),
            Some(&(42, 9000))
        );
    }
}

#[tokio::test]
async fn incremental_lookup_prunes_old_schema_history_by_unchanged_keys() {
    use paimon::spec::SchemaChange;
    for change in [
        SchemaChange::rename_column("value".into(), "renamed".into()),
        SchemaChange::add_column("extra".into(), DataType::Int(IntType::new())),
        SchemaChange::rename_column("id".into(), "renamed_id".into()),
    ] {
        let (original, probe) = probed_table_with_options(&[("target-file-row-num", "100")]).await;
        let keys = (0..1000).collect::<Vec<_>>();
        write_batch(&original, &make_batch(keys.clone(), keys)).await;
        let history = compact(&original).await[0].files().to_vec();
        assert!(
            history.len() >= 5,
            "regression requires rolled positive-level files"
        );
        assert!(history.iter().all(|file| file.level == 5));
        let start = latest(&original).await;
        let schema = original.schema().apply_changes(vec![change]).unwrap();
        persist_table_schema(original.file_io(), original.location(), &schema).await;
        let table = original.copy_with_resolved_schema(schema, "main").unwrap();
        let mut columns = vec![
            (
                table.schema().fields()[0].name(),
                Arc::new(Int32Array::from(vec![42])) as arrow_array::ArrayRef,
            ),
            (
                table.schema().fields()[1].name(),
                Arc::new(Int32Array::from(vec![9000])) as arrow_array::ArrayRef,
            ),
        ];
        for field in &table.schema().fields()[2..] {
            columns.push((
                field.name(),
                Arc::new(Int32Array::from(vec![None])) as arrow_array::ArrayRef,
            ));
        }
        write_batch(&table, &RecordBatch::try_from_iter(columns).unwrap()).await;
        let builder = table.new_compact_builder();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        assert!(plan.tasks()[0].files().iter().all(|file| file.level == 0));
        probe.data_opens.lock().unwrap().clear();
        let results = execute(&table, &plan).await;
        let touched = {
            let opens = probe.data_opens.lock().unwrap();
            history
                .iter()
                .filter(|file| {
                    opens.contains_key(&format!("{}/bucket-0/{}", table.location(), file.file_name))
                })
                .count()
        };
        assert_eq!(
            touched, 1,
            "unchanged key layouts must only read the candidate history file"
        );
        builder
            .new_commit()
            .unwrap()
            .commit_compact(results)
            .await
            .unwrap();
        assert_eq!(
            events(&table, start, latest(&table).await).await,
            vec![("+U".into(), 42, 9000, 1000), ("-U".into(), 42, 42, 42)]
        );
    }
}
