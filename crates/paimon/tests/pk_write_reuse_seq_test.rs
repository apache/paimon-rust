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

use arrow_array::{Int32Array, RecordBatch};
use common::incremental_helpers::{
    make_batch, memory_table, persist_table_schema, pk_schema, setup_dirs,
};
use futures::TryStreamExt;
use paimon::table::{Plan, ReadBuilder};

async fn read_pairs(builder: &ReadBuilder<'_>, plan: &Plan) -> Vec<(i32, i32)> {
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut out = Vec::new();
    for b in &batches {
        let ids = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let vals = b.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        for i in 0..b.num_rows() {
            out.push((ids.value(i), vals.value(i)));
        }
    }
    out.sort();
    out
}

/// Reusing one `TableWrite` across commit cycles must not restart per-bucket
/// primary-key sequence numbers. A later update has to win the highest-sequence
/// dedup, matching Java `MergeTreeWriter` (whose counter advances across
/// commits). Before the fix the second cycle re-seeded from the stale cache and
/// the update was silently dropped on read.
#[tokio::test]
async fn reused_writer_update_wins_over_earlier_row() {
    let path = "memory:/pk_write_reuse_seq";
    let (io, table) = memory_table(path, pk_schema(&[("bucket", "1")]));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;

    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();

    // Cycle 1: id=1 is written after id=7, so it takes a non-zero sequence.
    writer
        .write_arrow_batch(&make_batch(vec![7, 1], vec![70, 10]))
        .await
        .unwrap();
    let m1 = writer.prepare_commit().await.unwrap();
    builder.new_commit().commit(m1).await.unwrap();

    // Cycle 2: reuse the same writer to update id=1.
    writer
        .write_arrow_batch(&make_batch(vec![1], vec![999]))
        .await
        .unwrap();
    let m2 = writer.prepare_commit().await.unwrap();
    builder.new_commit().commit(m2).await.unwrap();

    let rb = table.new_read_builder();
    let plan = rb.new_scan().plan().await.unwrap();
    assert_eq!(read_pairs(&rb, &plan).await, vec![(1, 999), (7, 70)]);
}
