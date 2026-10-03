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

use arrow_array::{Array, Int32Array, Int64Array, Int8Array};
use common::incremental_helpers::{make_batch_with_kinds, memory_table, pk_schema, setup_dirs};
use paimon::spec::{BinaryRow, POSTPONE_BUCKET};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

#[tokio::test]
async fn postpone_rolls_by_batch_row_limit_and_preserves_unsorted_retracts() {
    let (io, table) = memory_table(
        "memory:/postpone_row_limit",
        pk_schema(&[("bucket", "-2"), ("target-file-row-num", "3")]),
    );
    setup_dirs(&io, table.location()).await;
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    let ids = [5, 1, 3, 2, 1];
    let kinds = [0, 1, 2, 3, 0];
    for range in [0..2, 2..4, 4..5] {
        writer
            .write_arrow_batch(&make_batch_with_kinds(
                ids[range.clone()].to_vec(),
                vec![10; range.len()],
                kinds[range].to_vec(),
            ))
            .await
            .unwrap();
    }
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].bucket, POSTPONE_BUCKET);
    let files = messages[0].new_files.clone();
    assert_eq!(
        files.iter().map(|f| f.row_count).collect::<Vec<_>>(),
        vec![4, 1]
    );
    assert_eq!(
        files.iter().map(|f| f.delete_row_count).collect::<Vec<_>>(),
        vec![Some(2), Some(0)]
    );
    assert_eq!(
        files
            .iter()
            .map(|f| (f.min_sequence_number, f.max_sequence_number))
            .collect::<Vec<_>>(),
        vec![(0, 3), (4, 4)]
    );
    let bounds: Vec<_> = files
        .iter()
        .map(|file| {
            (
                BinaryRow::from_serialized_bytes(&file.min_key)
                    .unwrap()
                    .get_int(0)
                    .unwrap(),
                BinaryRow::from_serialized_bytes(&file.max_key)
                    .unwrap()
                    .get_int(0)
                    .unwrap(),
            )
        })
        .collect();
    // Java records the first and last arriving keys for pending files.
    assert_eq!(bounds, vec![(5, 2), (1, 1)]);
    let mut actual = Vec::new();
    for file in files {
        let path = format!("{}/bucket-postpone/{}", table.location(), file.file_name);
        let bytes = io.new_input(&path).unwrap().read().await.unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .build()
            .unwrap();
        for batch in reader {
            let batch = batch.unwrap();
            let seq = batch
                .column_by_name("_SEQUENCE_NUMBER")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let kind = batch
                .column_by_name("_VALUE_KIND")
                .unwrap()
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap();
            let id = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            actual
                .extend((0..batch.num_rows()).map(|i| (seq.value(i), kind.value(i), id.value(i))));
        }
    }
    assert_eq!(
        actual,
        vec![(0, 0, 5), (1, 1, 1), (2, 2, 3), (3, 3, 2), (4, 0, 1)]
    );
}

#[tokio::test]
async fn postpone_delete_counts_reset_after_prepare_commit() {
    let (io, table) = memory_table(
        "memory:/postpone_delete_counts",
        pk_schema(&[("bucket", "-2")]),
    );
    setup_dirs(&io, table.location()).await;
    let mut writer = table.new_write_builder().new_write().unwrap();
    for (kinds, expected) in [(vec![0, 3, 1], 2), (vec![0, 2], 0)] {
        let len = kinds.len();
        writer
            .write_arrow_batch(&make_batch_with_kinds(vec![1; len], vec![10; len], kinds))
            .await
            .unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        assert_eq!(messages[0].new_files[0].delete_row_count, Some(expected));
    }
}
