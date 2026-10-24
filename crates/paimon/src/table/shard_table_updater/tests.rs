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
use crate::catalog::Identifier;
use crate::io::FileIOBuilder;
use crate::spec::{
    DataType as PaimonType, IntType, RowType, Schema as TableDefinition, TableSchema, VarCharType,
};
use crate::table::TableCommit;
use arrow_array::{Int32Array, StringArray, StructArray};

fn table(partitioned: bool) -> Table {
    table_with_io(partitioned, FileIOBuilder::new("memory").build().unwrap())
}

fn table_with_io(partitioned: bool, file_io: crate::io::FileIO) -> Table {
    let mut schema = TableDefinition::builder()
        .column("p", PaimonType::VarChar(VarCharType::string_type()))
        .column("id", PaimonType::Int(IntType::new()))
        .column("value", PaimonType::Int(IntType::new()))
        .option("data-evolution.enabled", "true")
        .option("row-tracking.enabled", "true")
        .option("deletion-vectors.enabled", "true");
    if partitioned {
        schema = schema.partition_keys(["p"]);
    }
    Table::new(
        file_io,
        Identifier::new("db", "shard"),
        "memory:/shard".into(),
        TableSchema::new(0, &schema.build().unwrap()),
        None,
    )
}

fn batch(partitions: Vec<&str>, ids: Vec<i32>, values: Vec<i32>) -> RecordBatch {
    RecordBatch::try_from_iter([
        ("p", Arc::new(StringArray::from(partitions)) as ArrayRef),
        ("id", Arc::new(Int32Array::from(ids)) as ArrayRef),
        ("value", Arc::new(Int32Array::from(values)) as ArrayRef),
    ])
    .unwrap()
}

fn values(values: Vec<i32>) -> RecordBatch {
    RecordBatch::try_from_iter([("value", Arc::new(Int32Array::from(values)) as ArrayRef)]).unwrap()
}

async fn commit(table: &Table, messages: Vec<CommitMessage>) {
    TableCommit::new(table.clone(), "shard".into())
        .commit(messages)
        .await
        .unwrap();
}

async fn seed(table: &Table, data: RecordBatch) {
    let mut writer = table
        .new_write_builder()
        .with_commit_user("shard")
        .unwrap()
        .new_write()
        .unwrap();
    writer.write_arrow_batch(&data).await.unwrap();
    commit(table, writer.prepare_commit().await.unwrap()).await;
    writer.close().await;
}

async fn updater(table: &Table, index: usize, count: usize) -> ShardTableUpdater {
    let mut update = table.new_write_builder().new_update().unwrap();
    update.with_update_type(vec!["value".into()]).unwrap();
    update
        .new_shard_updater(index, count, Some(vec![table.schema().fields()[1].clone()]))
        .await
        .unwrap()
}

async fn rows(table: &Table) -> Vec<(i32, i32)> {
    let builder = table.new_read_builder();
    let plan = builder.new_scan().plan().await.unwrap();
    let mut read = builder.new_read().unwrap().to_arrow(plan.splits()).unwrap();
    let mut result = Vec::new();
    while let Some(batch) = read.try_next().await.unwrap() {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        result.extend(
            ids.values()
                .iter()
                .zip(values.values())
                .map(|(a, b)| (*a, *b)),
        );
    }
    result.sort();
    result
}

#[tokio::test]
async fn balanced_shards_keep_overlapping_column_files_together() {
    let table = table(false);
    for id in 0..5 {
        seed(&table, batch(vec!["p"], vec![id], vec![id * 10])).await;
    }
    // Construct every shard against one snapshot, before any replacement files exist.
    let mut shards = Vec::new();
    for shard in 0..3 {
        shards.push(updater(&table, shard, 3).await);
    }
    let mut counts = Vec::new();
    for mut shard in shards {
        let mut read = shard.arrow_reader().unwrap();
        let mut count = 0;
        while let Some(batch) = read.try_next().await.unwrap() {
            assert_eq!(batch.schema().fields().len(), 1);
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            count += ids.len();
            shard
                .update_by_arrow_batch(values(ids.values().iter().map(|id| id + 100).collect()))
                .await
                .unwrap();
        }
        counts.push(count);
        let messages = shard.prepare_commit().await.unwrap();
        assert!(messages
            .iter()
            .all(|message| message.check_from_snapshot == Some(5)));
        for message in &messages {
            assert_eq!(message.bucket, 0);
            assert!(message
                .new_files
                .iter()
                .all(|file| file.write_cols == Some(vec!["value".into()])));
        }
        commit(&table, messages).await;
    }
    assert_eq!(counts, vec![2, 2, 1]);
    assert_eq!(
        rows(&table).await,
        (0..5).map(|id| (id, id + 100)).collect::<Vec<_>>()
    );
    // A subsequent scan has two column files per range, which still form five shards.
    let mut sizes = Vec::new();
    for shard in 0..7 {
        let mut update = updater(&table, shard, 7).await;
        sizes.push(
            update
                .arrow_reader()
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
        );
    }
    assert_eq!(sizes, vec![1, 1, 1, 1, 1, 0, 0]);
}

#[tokio::test]
async fn partitioned_shard_accepts_rebatched_values_and_pins_snapshot() {
    let table = table(true);
    seed(
        &table,
        batch(vec!["a", "b", "a"], vec![1, 2, 3], vec![10, 20, 30]),
    )
    .await;
    let mut update = updater(&table, 0, 1).await;
    seed(&table, batch(vec!["a"], vec![4], vec![40])).await;
    let data = update
        .arrow_reader()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let ids = data
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    assert_eq!(ids.len(), 3);
    update
        .update_by_arrow_batch(values(ids.iter().map(|id| id * 100).collect()))
        .await
        .unwrap();
    let messages = update.prepare_commit().await.unwrap();
    assert_eq!(messages.len(), 2);
    assert!(messages
        .iter()
        .all(|message| message.check_from_snapshot == Some(1)));
    assert!(messages.iter().all(|message| message
        .new_files
        .iter()
        .all(|file| file.first_row_id.is_some())));
    commit(&table, messages).await;
    assert_eq!(
        rows(&table).await,
        vec![(1, 100), (2, 200), (3, 300), (4, 40)]
    );
}

#[tokio::test]
async fn partitioned_shard_accepts_unchanged_partition_carriers() {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    for partition_only in [false, true] {
        let table = table(true);
        seed(&table, batch(vec!["a", "b"], vec![1, 2], vec![10, 20])).await;
        let mut configured = table.new_write_builder().new_update().unwrap();
        if partition_only {
            configured.with_update_type(vec!["p".into()]).unwrap();
        }
        let mut update = configured.new_shard_updater(0, 1, None).await.unwrap();
        let inputs: Vec<_> = update.arrow_reader().unwrap().try_collect().await.unwrap();
        assert_eq!(inputs.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        // Rebatch across partitions so both the first writer and each writer
        // created at a file-group boundary use the same carrier validation.
        let input = arrow_select::concat::concat_batches(&inputs[0].schema(), &inputs).unwrap();
        let output = if partition_only {
            input.project(&[0]).unwrap()
        } else {
            let ids = input
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            RecordBatch::try_from_iter([
                ("p", input.column_by_name("p").unwrap().clone()),
                ("id", input.column_by_name("id").unwrap().clone()),
                (
                    "value",
                    Arc::new(Int32Array::from_iter_values(
                        ids.values().iter().map(|id| id * 100),
                    )) as ArrayRef,
                ),
            ])
            .unwrap()
        };
        update.update_by_arrow_batch(output).await.unwrap();
        let messages = update.prepare_commit().await.unwrap();
        assert_eq!(messages.len(), 2);
        let files = table
            .file_io()
            .list_status_recursive(table.location())
            .await
            .unwrap();
        let mut ranges = Vec::new();
        for message in &messages {
            assert_eq!(message.check_from_snapshot, Some(1));
            for file in &message.new_files {
                assert_eq!(file.schema_id, 0);
                assert_eq!(file.row_count, 1);
                ranges.push(file.first_row_id.unwrap());
                assert_eq!(file.write_cols, partition_only.then(|| vec!["p".into()]));
                let path = &files
                    .iter()
                    .find(|status| status.path.ends_with(&file.file_name))
                    .unwrap()
                    .path;
                let bytes = table
                    .file_io()
                    .new_input(path)
                    .unwrap()
                    .read()
                    .await
                    .unwrap();
                let reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
                let schema = reader.schema();
                let names = schema
                    .fields()
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect::<Vec<_>>();
                assert_eq!(
                    names,
                    if partition_only {
                        vec!["p"]
                    } else {
                        vec!["p", "id", "value"]
                    }
                );
            }
        }
        ranges.sort_unstable();
        assert_eq!(ranges, vec![0, 1]);
        commit(&table, messages).await;
        assert_eq!(
            rows(&table).await,
            if partition_only {
                vec![(1, 10), (2, 20)]
            } else {
                vec![(1, 100), (2, 200)]
            }
        );
    }
}

#[tokio::test]
async fn partitioned_shard_rejects_moving_rows_to_another_partition() {
    let table = table(true);
    seed(&table, batch(vec!["a", "b"], vec![1, 2], vec![10, 20])).await;
    let mut update = table
        .new_write_builder()
        .new_update()
        .unwrap()
        .new_shard_updater(0, 1, None)
        .await
        .unwrap();
    let inputs: Vec<_> = update.arrow_reader().unwrap().try_collect().await.unwrap();
    let result: crate::Result<()> = async {
        for input in inputs {
            let ids = input
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            update
                .update_by_arrow_batch(batch(
                    vec!["changed"; input.num_rows()],
                    ids.values().to_vec(),
                    vec![99; input.num_rows()],
                ))
                .await?;
        }
        update.prepare_commit().await?;
        Ok(())
    }
    .await;
    let error = result.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Cannot change partition column 'p'"),
        "{error}"
    );
    assert!(update.prepare_commit().await.is_err());
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        Some(1)
    );
    assert_eq!(rows(&table).await, vec![(1, 10), (2, 20)]);
}

#[tokio::test]
async fn deleted_rows_do_not_shift_positional_assignments() {
    let table = table(false);
    seed(
        &table,
        batch(vec!["p"; 4], vec![1, 2, 3, 4], vec![10, 20, 30, 40]),
    )
    .await;
    let messages = table
        .new_write_builder()
        .new_update()
        .unwrap()
        .delete_by_row_id(vec![1, 3])
        .await
        .unwrap();
    commit(&table, messages).await;
    let mut update = updater(&table, 0, 1).await;
    let data = update
        .arrow_reader()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(data.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    update
        .update_by_arrow_batch(values(vec![111, 333]))
        .await
        .unwrap();
    let messages = update.prepare_commit().await.unwrap();
    assert!(messages
        .iter()
        .flat_map(|m| &m.new_files)
        .all(|file| file.row_count == 4 && file.first_row_id == Some(0)));
    commit(&table, messages).await;
    assert_eq!(rows(&table).await, vec![(1, 111), (3, 333)]);
}

#[tokio::test]
async fn count_and_schema_failures_preserve_pending_rows_for_correction() {
    let table = table(false);
    seed(&table, batch(vec!["p"; 3], vec![1, 2, 3], vec![10, 20, 30])).await;
    let mut update = updater(&table, 0, 1).await;
    assert!(update.update_by_arrow_batch(values(vec![9])).await.is_err());
    update
        .arrow_reader()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let bad =
        RecordBatch::try_from_iter([("wrong", Arc::new(Int32Array::from(vec![1])) as ArrayRef)])
            .unwrap();
    assert!(update.update_by_arrow_batch(bad).await.is_err());
    assert!(update
        .update_by_arrow_batch(values(vec![1, 2, 3, 4]))
        .await
        .is_err());
    update
        .update_by_arrow_batch(values(vec![100]))
        .await
        .unwrap();
    assert!(update
        .prepare_commit()
        .await
        .unwrap_err()
        .to_string()
        .contains("same number"));
    update
        .update_by_arrow_batch(values(vec![200, 300]))
        .await
        .unwrap();
    let messages = update.prepare_commit().await.unwrap();
    let repeated = update.prepare_commit().await.unwrap();
    assert_eq!(
        messages[0].new_files[0].file_name,
        repeated[0].new_files[0].file_name
    );
    assert!(update.update_by_arrow_batch(values(vec![])).await.is_err());
    update.close();
    // Prepared files survive explicit close and can still be committed.
    commit(&table, messages).await;
    assert_eq!(rows(&table).await, vec![(1, 100), (2, 200), (3, 300)]);
}

#[tokio::test]
async fn incomplete_reader_cannot_publish_partial_shard() {
    let table = table(false);
    seed(&table, batch(vec!["p"], vec![1], vec![10])).await;
    seed(&table, batch(vec!["p"], vec![2], vec![20])).await;
    let mut update = updater(&table, 0, 1).await;
    let mut read = update.arrow_reader().unwrap();
    let batch = read.try_next().await.unwrap().unwrap();
    update
        .update_by_arrow_batch(values(vec![99; batch.num_rows()]))
        .await
        .unwrap();
    drop(read);
    assert!(update.arrow_reader().is_err());
    assert!(update.prepare_commit().await.is_err());
    update.close();
    assert!(update.prepare_commit().await.is_err());
    assert_eq!(rows(&table).await, vec![(1, 10), (2, 20)]);
}

#[tokio::test]
async fn empty_shards_and_invalid_configuration() {
    let table = table(false);
    let mut empty = updater(&table, 2, 3).await;
    assert!(empty.prepare_commit().await.unwrap().is_empty());
    let update = table.new_write_builder().new_update().unwrap();
    assert!(update.new_shard_updater(0, 0, None).await.is_err());
    assert!(update.new_shard_updater(2, 2, None).await.is_err());
    let mut bad_columns = update;
    bad_columns.with_update_type(vec![]).unwrap();
    assert!(bad_columns.new_shard_updater(0, 1, None).await.is_err());
}

#[tokio::test]
async fn returned_reader_survives_updater_scope_and_close_stops_it() {
    let table = table(false);
    seed(&table, batch(vec!["p"], vec![1], vec![10])).await;
    let mut read = updater(&table, 0, 1).await.arrow_reader().unwrap();
    assert_eq!(read.try_next().await.unwrap().unwrap().num_rows(), 1);
    assert!(read.try_next().await.unwrap().is_none());
    let mut update = updater(&table, 0, 1).await;
    let mut read = update.arrow_reader().unwrap();
    update.close();
    assert!(read.try_next().await.is_err());
    assert!(update.prepare_commit().await.is_err());
}

#[test]
fn reader_eof_cannot_clear_failure() {
    let state = Mutex::new(ReadState::default());
    finish_read(&state, true).unwrap();
    finish_read(&state, false).unwrap();
    let state = state.lock().unwrap();
    assert!(state.failed);
    assert!(!state.complete);
}

#[derive(Debug)]
struct ControlledIo {
    operator: opendal::Operator,
    mode: std::sync::atomic::AtomicU8,
    entered: tokio::sync::Notify,
}

impl ControlledIo {
    fn table() -> (Table, Arc<Self>) {
        let provider = Arc::new(Self {
            operator: opendal::Operator::from_config(opendal::services::MemoryConfig::default())
                .unwrap(),
            mode: std::sync::atomic::AtomicU8::new(0),
            entered: tokio::sync::Notify::new(),
        });
        let io = FileIOBuilder::new("unused")
            .with_provider(provider.clone())
            .build()
            .unwrap();
        (table_with_io(false, io), provider)
    }
}

#[async_trait::async_trait]
impl crate::io::FileIOProvider for ControlledIo {
    async fn create(&self, path: &str) -> crate::Result<(opendal::Operator, String)> {
        use std::sync::atomic::Ordering;
        if path.ends_with(".parquet") {
            match self.mode.load(Ordering::SeqCst) {
                1 => {
                    self.entered.notify_one();
                    std::future::pending::<()>().await;
                }
                2 => {
                    self.mode.store(0, Ordering::SeqCst);
                    return Err(invalid("Injected shard I/O failure"));
                }
                _ => {}
            }
        }
        Ok((
            self.operator.clone(),
            path.trim_start_matches("memory:")
                .trim_start_matches('/')
                .into(),
        ))
    }
}

#[tokio::test]
async fn cancelled_prepare_cannot_retry_as_an_empty_success() {
    use std::sync::atomic::Ordering;
    let (table, provider) = ControlledIo::table();
    seed(&table, batch(vec!["p"], vec![1], vec![10])).await;
    let mut update = updater(&table, 0, 1).await;
    update
        .arrow_reader()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    update
        .update_by_arrow_batch(values(vec![99]))
        .await
        .unwrap();
    provider.mode.store(1, Ordering::SeqCst);
    let mut prepare = Box::pin(update.prepare_commit());
    tokio::select! {
        result = &mut prepare => panic!("Expected stalled I/O, got {result:?}"),
        result = tokio::time::timeout(std::time::Duration::from_secs(5), provider.entered.notified()) => {
            result.expect("prepare never reached storage I/O");
        }
    }
    drop(prepare);
    provider.mode.store(0, Ordering::SeqCst);
    assert!(update
        .prepare_commit()
        .await
        .unwrap_err()
        .to_string()
        .contains("failed"));
    assert!(update.update_by_arrow_batch(values(vec![])).await.is_err());
    assert_eq!(rows(&table).await, vec![(1, 10)]);
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap()
            .id(),
        1
    );
}

#[tokio::test]
async fn reader_eof_does_not_resurrect_a_failed_group_write() {
    use std::sync::atomic::Ordering;
    let (table, provider) = ControlledIo::table();
    seed(&table, batch(vec!["p"], vec![1], vec![10])).await;
    seed(&table, batch(vec!["p"], vec![2], vec![20])).await;
    let mut update = updater(&table, 0, 1).await;
    let mut read = update.arrow_reader().unwrap();
    assert_eq!(read.try_next().await.unwrap().unwrap().num_rows(), 1);
    update
        .update_by_arrow_batch(values(vec![100]))
        .await
        .unwrap();
    assert_eq!(read.try_next().await.unwrap().unwrap().num_rows(), 1);
    provider.mode.store(2, Ordering::SeqCst);
    let error = update
        .update_by_arrow_batch(values(vec![200]))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("Injected shard I/O failure"));
    assert!(read.try_next().await.unwrap().is_none());
    assert!(update
        .update_by_arrow_batch(values(vec![200]))
        .await
        .is_err());
    assert!(update.prepare_commit().await.is_err());
    assert_eq!(rows(&table).await, vec![(1, 10), (2, 20)]);
}

#[tokio::test]
async fn read_types_belong_to_each_operation_and_are_independent_of_updates() {
    let table = table(false);
    seed(&table, batch(vec!["p", "p"], vec![1, 2], vec![10, 20])).await;
    let mut update = table.new_write_builder().new_update().unwrap();
    update.with_update_type(vec!["value".into()]).unwrap();
    let id_field = table.schema().fields()[1].clone();
    let mut ids = update
        .new_shard_updater(0, 1, Some(vec![id_field]))
        .await
        .unwrap();
    let mut originals = update
        .new_shard_updater(
            0,
            1,
            Some(vec![
                table.schema().fields()[2].clone(),
                row_id_data_field(),
            ]),
        )
        .await
        .unwrap();
    let read = originals
        .arrow_reader()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(read[0].schema().field(0).name(), "value");
    assert_eq!(
        read[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values(),
        &[10, 20]
    );
    assert_eq!(
        read[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[0, 1]
    );
    originals.close();
    let mut read = ids.arrow_reader().unwrap();
    while let Some(batch) = read.try_next().await.unwrap() {
        assert_eq!(batch.schema().fields().len(), 1);
        assert_eq!(batch.schema().field(0).name(), "id");
        let source = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        ids.update_by_arrow_batch(values(source.values().iter().map(|id| id * 100).collect()))
            .await
            .unwrap();
    }
    commit(&table, ids.prepare_commit().await.unwrap()).await;
    assert_eq!(rows(&table).await, vec![(1, 100), (2, 200)]);
}

#[tokio::test]
async fn default_and_empty_read_types_preserve_row_counts() {
    let table = table(false);
    seed(&table, batch(vec!["p", "p"], vec![1, 2], vec![10, 20])).await;
    let mut update = table.new_write_builder().new_update().unwrap();
    update.with_update_type(vec!["value".into()]).unwrap();
    let mut full = update.new_shard_updater(0, 1, None).await.unwrap();
    let batches = full
        .arrow_reader()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        batches[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        vec!["p", "id", "value"]
    );
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    full.close();
    let mut empty = update.new_shard_updater(0, 1, Some(vec![])).await.unwrap();
    let mut read = empty.arrow_reader().unwrap();
    while let Some(batch) = read.try_next().await.unwrap() {
        assert_eq!(batch.num_columns(), 0);
        assert_eq!(batch.num_rows(), 2);
        empty
            .update_by_arrow_batch(values(vec![99; batch.num_rows()]))
            .await
            .unwrap();
    }
    commit(&table, empty.prepare_commit().await.unwrap()).await;
    assert_eq!(rows(&table).await, vec![(1, 99), (2, 99)]);
}

#[tokio::test]
async fn nested_read_type_prunes_children_and_retains_their_ids() {
    let definition = TableDefinition::builder()
        .column("p", PaimonType::VarChar(VarCharType::string_type()))
        .column("id", PaimonType::Int(IntType::new()))
        .column("value", PaimonType::Int(IntType::new()))
        .column(
            "payload",
            PaimonType::Row(RowType::new(vec![
                DataField::new(0, "a".into(), PaimonType::Int(IntType::new())),
                DataField::new(1, "b".into(), PaimonType::Int(IntType::new())),
            ])),
        )
        .option("data-evolution.enabled", "true")
        .option("row-tracking.enabled", "true")
        .build()
        .unwrap();
    let table = Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("db", "nested_shard"),
        "memory:/nested_shard".into(),
        TableSchema::new(0, &definition),
        None,
    );
    let data = batch(vec!["p", "p"], vec![1, 2], vec![10, 20]);
    let payload = StructArray::from(vec![
        (
            Arc::new(Field::new("a", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![100, 200])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("b", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![3, 4])) as ArrayRef,
        ),
    ]);
    let mut fields = data.schema().fields().to_vec();
    fields.push(Arc::new(Field::new(
        "payload",
        payload.data_type().clone(),
        true,
    )));
    let mut columns = data.columns().to_vec();
    columns.push(Arc::new(payload));
    seed(
        &table,
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap(),
    )
    .await;
    let source = &table.schema().fields()[3];
    let PaimonType::Row(row) = source.data_type() else {
        panic!("expected ROW")
    };
    let read_type = vec![DataField::new(
        source.id(),
        source.name().into(),
        PaimonType::Row(RowType::new(vec![row.fields()[1].clone()])),
    )];
    let mut update = table.new_write_builder().new_update().unwrap();
    update.with_update_type(vec!["value".into()]).unwrap();
    let mut shard = update
        .new_shard_updater(0, 1, Some(read_type))
        .await
        .unwrap();
    let mut read = shard.arrow_reader().unwrap();
    while let Some(batch) = read.try_next().await.unwrap() {
        let payload = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(payload.num_columns(), 1);
        assert_eq!(payload.fields()[0].name(), "b");
        let source = payload
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(source.values(), &[3, 4]);
        shard
            .update_by_arrow_batch(values(source.values().iter().map(|id| id * 100).collect()))
            .await
            .unwrap();
    }
    commit(&table, shard.prepare_commit().await.unwrap()).await;
    assert_eq!(rows(&table).await, vec![(1, 300), (2, 400)]);
}

#[tokio::test]
async fn read_type_cannot_override_the_internal_row_id() {
    let table = table(false);
    seed(&table, batch(vec!["p"], vec![1], vec![10])).await;
    let mut update = table.new_write_builder().new_update().unwrap();
    update.with_update_type(vec!["value".into()]).unwrap();
    for fields in [
        vec![DataField::new(
            crate::spec::ROW_ID_FIELD_ID,
            ROW_ID.into(),
            PaimonType::Int(IntType::new()),
        )],
        vec![row_id_data_field().with_id(0)],
        vec![row_id_data_field(), row_id_data_field()],
    ] {
        let error = update
            .new_shard_updater(0, 1, Some(fields))
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains("canonical BIGINT _ROW_ID"));
    }
    assert_eq!(rows(&table).await, vec![(1, 10)]);
}
