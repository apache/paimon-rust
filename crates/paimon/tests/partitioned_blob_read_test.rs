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

use arrow_array::{ArrayRef, Int32Array, Int64Array, LargeBinaryArray, RecordBatch, StringArray};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs};
use futures::TryStreamExt;
use paimon::spec::{
    BlobType, DataType, Datum, IntType, Predicate, PredicateBuilder, Schema, TableSchema,
    VarCharType,
};
use std::sync::Arc;

#[tokio::test]
async fn blob_preselection_restores_omitted_partition_columns_before_filtering() {
    for nullable in [true, false] {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column(
                "p",
                DataType::VarChar(
                    VarCharType::with_nullable(nullable, VarCharType::MAX_LENGTH).unwrap(),
                ),
            )
            .column("payload", DataType::Blob(BlobType::new()))
            .partition_keys(["p"])
            .option("row-tracking.enabled", "true")
            .option("data-evolution.enabled", "true")
            .option("deletion-vectors.enabled", "true")
            .option("read.batch-size", "2")
            .build()
            .unwrap();
        let (io, table) = memory_table(
            "memory:/partitioned_blob_read",
            TableSchema::new(0, &schema),
        );
        setup_dirs(&io, table.location()).await;
        persist_table_schema(&io, table.location(), table.schema()).await;
        let batch = RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
            ),
            (
                "p",
                Arc::new(StringArray::from(vec!["a", "b", "a", "b"])) as ArrayRef,
            ),
            (
                "payload",
                Arc::new(LargeBinaryArray::from(vec![
                    b"one".as_slice(),
                    b"two",
                    b"three",
                    b"four",
                ])) as ArrayRef,
            ),
        ])
        .unwrap();
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer
            .with_write_type(vec!["id".into(), "payload".into()])
            .unwrap();
        writer.write_arrow_batch(&batch).await.unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        assert!(messages
            .iter()
            .flat_map(|message| &message.new_files)
            .all(|file| {
                !file
                    .write_cols
                    .as_ref()
                    .unwrap()
                    .iter()
                    .any(|column| column == "p")
            }));
        builder.new_commit().commit(messages).await.unwrap();
        let splits = table.new_read_builder().new_scan().plan().await.unwrap();
        let pb = PredicateBuilder::new(table.schema().fields());
        let predicate = Predicate::or(vec![
            pb.equal("p", Datum::String("a".into())).unwrap(),
            pb.equal("id", Datum::Int(999)).unwrap(),
        ]);
        for limit in [None, Some(1)] {
            let mut read = table.new_read_builder();
            read.with_projection(&["id", "payload"])
                .unwrap()
                .with_filter(predicate.clone());
            if let Some(limit) = limit {
                read.with_limit(limit);
            }
            let batches: Vec<RecordBatch> = read
                .new_read()
                .unwrap()
                .to_arrow(splits.splits())
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            let mut ids = batches
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
            ids.sort_unstable();
            assert_eq!(
                ids,
                if limit.is_some() { vec![1] } else { vec![1, 3] },
                "nullable={nullable}, limit={limit:?}"
            );
            let payloads = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<LargeBinaryArray>()
                        .unwrap()
                        .iter()
                        .map(|value| value.unwrap().to_vec())
                })
                .collect::<Vec<_>>();
            assert_eq!(
                payloads,
                if limit.is_some() {
                    vec![b"one".to_vec()]
                } else {
                    vec![b"one".to_vec(), b"three".to_vec()]
                }
            );
        }
        // A projection containing only partition columns still has one row
        // per visible row, without needing a physical provider for that column.
        let mut read = table.new_read_builder();
        read.with_projection(&["p"]).unwrap();
        let batches: Vec<RecordBatch> = read
            .new_read()
            .unwrap()
            .to_arrow(splits.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
        let mut values = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|value| value.unwrap().to_string())
            })
            .collect::<Vec<_>>();
        values.sort();
        assert_eq!(values, ["a", "a", "b", "b"]);

        let mut metadata = table.new_read_builder();
        metadata.with_projection(&["id", "_ROW_ID"]).unwrap();
        let rows: Vec<RecordBatch> = metadata
            .new_read()
            .unwrap()
            .to_arrow(splits.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let row_ids = rows
            .iter()
            .flat_map(|batch| {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let row_ids = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                ids.values()
                    .iter()
                    .copied()
                    .zip(row_ids.values().iter().copied())
            })
            .collect::<std::collections::HashMap<_, _>>();
        let mut update = builder.new_update().unwrap();
        update.with_update_type(vec!["id".into()]).unwrap();
        let delta = RecordBatch::try_from_iter([
            ("id", Arc::new(Int32Array::from(vec![11])) as ArrayRef),
            (
                "_ROW_ID",
                Arc::new(Int64Array::from(vec![row_ids[&1]])) as ArrayRef,
            ),
        ])
        .unwrap();
        let messages = update
            .update_by_arrow_with_row_id(vec![delta])
            .await
            .unwrap();
        assert!(messages
            .iter()
            .flat_map(|message| &message.new_files)
            .all(|file| file.write_cols == Some(vec!["id".to_string()])));
        builder.new_commit().commit(messages).await.unwrap();
        let deletions = builder
            .new_update()
            .unwrap()
            .delete_by_row_id(vec![row_ids[&3]])
            .await
            .unwrap();
        builder.new_commit().commit(deletions).await.unwrap();

        let latest = table.new_read_builder().new_scan().plan().await.unwrap();
        for projection in [vec!["id", "payload"], vec![], vec!["p"]] {
            let mut read = table.new_read_builder();
            read.with_projection(&projection)
                .unwrap()
                .with_filter(predicate.clone())
                .with_limit(1);
            let batches: Vec<RecordBatch> = read
                .new_read()
                .unwrap()
                .to_arrow(latest.splits())
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
            assert!(batches
                .iter()
                .all(|batch| batch.num_columns() == projection.len()));
            if projection == ["id", "payload"] {
                let batch = batches.iter().find(|batch| batch.num_rows() > 0).unwrap();
                assert_eq!(
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .value(0),
                    11
                );
                assert_eq!(
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<LargeBinaryArray>()
                        .unwrap()
                        .value(0),
                    b"one"
                );
            }
        }
    }
}
