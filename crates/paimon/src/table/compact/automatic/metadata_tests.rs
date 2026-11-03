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
use crate::spec::{DataType, IntType, Schema, TableSchema};
use crate::table::maintenance_test_utils::{source_manifests, table};
use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use std::sync::Arc;

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
