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
use crate::spec::{ArrayType, DataType, FloatType, IndexManifest, IntType, Schema, TableSchema};
use crate::table::maintenance_test_utils::{source_manifests, table};
use arrow_array::{types::Float32Type, ArrayRef, Int32Array, ListArray, RecordBatch};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn automatic_tasks_read_captured_metadata_once_and_keep_bucket_indexes() {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "4")
        .option("write-only", "true")
        .option("num-sorted-run.compaction-trigger", "2")
        .option("compaction.force-up-level0", "true")
        .option("deletion-vectors.enabled", "true")
        .option("deletion-vectors.merge-on-read", "false")
        .option("pk-btree.index.columns", "value")
        .build()
        .unwrap();
    let (table, counter) = table(TableSchema::new(0, &schema)).await;
    for value in [10, 20] {
        let batch = RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int32Array::from((0..64).collect::<Vec<_>>())) as ArrayRef,
            ),
            (
                "value",
                Arc::new(Int32Array::from(vec![value; 64])) as ArrayRef,
            ),
        ])
        .unwrap();
        let write = table.new_write_builder();
        let mut writer = write.new_write().unwrap();
        writer.write_arrow_batch(&batch).await.unwrap();
        write
            .new_commit()
            .commit(writer.prepare_commit().await.unwrap())
            .await
            .unwrap();
        let builder = table.new_compact_builder();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        assert_eq!(plan.tasks().len(), 4);
        let executor = builder.new_auto_executor().unwrap();
        if value == 10 {
            let mut results = Vec::new();
            for task in plan.tasks() {
                results.push(executor.execute(task).await.unwrap());
            }
            builder
                .new_commit()
                .unwrap()
                .commit_compact(results)
                .await
                .unwrap();
            continue;
        }
        let snapshot = table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        let mut watched = source_manifests(&table, &snapshot).await;
        watched.push(
            table
                .snapshot_manager()
                .manifest_path(snapshot.index_manifest().unwrap()),
        );
        let snapshot_path = table.snapshot_manager().snapshot_path(snapshot.id());
        watched.push(snapshot_path.clone());
        counter.watch(watched);
        let prepared = execute_auto_plan(&table, &plan, &executor, 2)
            .await
            .unwrap();
        let counts = counter.counts();
        assert!(counts.values().any(|count| *count > 0), "{counts:?}");
        assert!(
            counts.iter().all(|(path, count)| *count <= if path == &snapshot_path { 2 } else { 1 }),
            "one automatic round must read captured metadata once (snapshot existence + read): {counts:?}"
        );
        assert_eq!(
            prepared
                .entries
                .iter()
                .filter(|e| *e.kind() == crate::spec::FileKind::Add)
                .count(),
            4
        );
        let additions = prepared
            .index_entries
            .unwrap()
            .into_iter()
            .filter(|e| e.kind == crate::spec::FileKind::Add)
            .collect::<Vec<_>>();
        assert_eq!(additions.len(), 4);
        assert!(additions.iter().all(|e| e.index_file.index_type == "btree"));
        assert_eq!(
            additions
                .iter()
                .map(|e| e.index_file.row_count)
                .sum::<i64>(),
            64
        );
        // A later execution must not reuse this round's metadata after rollback
        // or snapshot replacement, even when it reuses the same executor.
        let mut replaced = serde_json::to_value(&snapshot).unwrap();
        replaced["deltaManifestList"] = serde_json::json!("replaced-manifest-list");
        let replaced: crate::spec::Snapshot = serde_json::from_value(replaced).unwrap();
        assert_eq!(replaced.delta_manifest_list(), "replaced-manifest-list");
        table
            .file_io()
            .new_output(&snapshot_path)
            .unwrap()
            .write(serde_json::to_vec(&replaced).unwrap().into())
            .await
            .unwrap();
        let error = executor.execute(&plan.tasks()[0]).await.unwrap_err();
        assert!(
            error.to_string().contains("snapshot was replaced"),
            "{error}"
        );
        let error = execute_auto_plan(&table, &plan, &executor, 2)
            .await
            .err()
            .unwrap();
        assert!(
            error.to_string().contains("snapshot was replaced"),
            "{error}"
        );
    }
}

#[tokio::test]
async fn automatic_pk_vector_header_reads_and_empty_stats_share_a_limit_across_buckets() {
    async fn append(table: &Table, ids: std::ops::Range<i32>, empty_first_bucket: bool) {
        let count = ids.len();
        let ids = (0..2).flat_map(|_| ids.clone()).collect::<Vec<_>>();
        let vectors = ListArray::from_iter_primitive::<Float32Type, _, _>(
            ids.iter().enumerate().map(|(position, id)| {
                (!empty_first_bucket || position >= count).then_some([Some(*id as f32), Some(0.)])
            }),
        );
        let batch = RecordBatch::try_from_iter([
            (
                "p",
                Arc::new(Int32Array::from_iter_values(
                    (0..2).flat_map(|partition| std::iter::repeat_n(partition, count)),
                )) as ArrayRef,
            ),
            ("id", Arc::new(Int32Array::from(ids)) as ArrayRef),
            ("embedding", Arc::new(vectors) as ArrayRef),
        ])
        .unwrap();
        let write = table.new_write_builder();
        let mut writer = write.new_write().unwrap();
        writer.write_arrow_batch(&batch).await.unwrap();
        write
            .new_commit()
            .commit(writer.prepare_commit().await.unwrap())
            .await
            .unwrap();
    }

    let schema = Schema::builder()
        .column("p", DataType::Int(IntType::new()))
        .column("id", DataType::Int(IntType::new()))
        .column(
            "embedding",
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        )
        .partition_keys(["p"])
        .primary_key(["p", "id"])
        .option("bucket", "1")
        .option("write-only", "true")
        .option("deletion-vectors.enabled", "true")
        .option("deletion-vectors.merge-on-read", "false")
        .option("global-index.ivf-flat.index-columns", "embedding")
        .option("ivf-flat.dimension", "2")
        .option("ivf-flat.nlist", "1")
        .option("global-index.thread-num", "1")
        .option("index-file-in-data-file-dir", "false")
        .build()
        .unwrap();
    let (table, counter) = table(TableSchema::new(0, &schema)).await;
    let builder = table.new_compact_builder();
    let executor = builder.new_auto_executor().unwrap();
    append(&table, 0..1000, true).await;
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    assert_eq!(plan.tasks().len(), 2);
    let mut results = Vec::new();
    for task in plan.tasks() {
        results.push(executor.execute(task).await.unwrap());
    }
    builder
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap();
    let snapshots = table.snapshot_manager();
    let snapshot = snapshots.get_latest_snapshot().await.unwrap().unwrap();
    let indexes = IndexManifest::read(
        table.file_io(),
        &snapshots.manifest_path(snapshot.index_manifest().unwrap()),
    )
    .await
    .unwrap();
    assert_eq!(
        indexes
            .iter()
            .filter(|entry| entry.index_file.file_size == 0)
            .count(),
        1
    );
    let paths = indexes
        .iter()
        .map(|entry| format!("{}/index/{}", table.location(), entry.index_file.file_name))
        .collect::<Vec<_>>();
    assert_eq!(paths.len(), 2);

    append(&table, 1000..1004, false).await;
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    assert_eq!(plan.tasks().len(), 2);
    // Overlapping rounds using the same executor each get one permit.
    for rounds in [1, 2] {
        counter.watch(paths.clone());
        let gate = counter.pause_watched_reads();
        let check_limit = async {
            gate.started.acquire_many(rounds).await.unwrap().forget();
            let excess = tokio::time::timeout(Duration::from_millis(250), gate.started.acquire())
                .await
                .is_ok();
            gate.release.add_permits(2 * rounds as usize);
            assert!(
                !excess,
                "retained header reads and empty-segment stats exceeded one permit per round"
            );
        };
        let executions = (0..rounds).map(|_| execute_auto_plan(&table, &plan, &executor, 2));
        let (prepared, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(futures::future::join_all(executions), check_limit)
        })
        .await
        .expect("each round must start an index check while the other rounds are paused");
        for result in prepared {
            assert_eq!(result.unwrap().index_entries.unwrap().len(), 2);
        }
        assert!(counter
            .counts()
            .values()
            .all(|&count| count == rounds as usize));
    }
}
