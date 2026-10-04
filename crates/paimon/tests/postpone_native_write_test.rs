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

// Java PostponeBucketWriter validates a retract with the actual merge function,
// not ReducerMergeFunctionWrapper's singleton shortcut.
#[tokio::test]
async fn postpone_retract_validation_matches_all_merge_engines() {
    type Options<'a> = &'a [(&'a str, &'a str)];
    let cases: &[(&str, Options<'_>, i8, bool)] = &[
        ("deduplicate", &[], 3, true),
        ("first-row", &[], 3, false),
        ("first-row", &[], 1, false),
        ("first-row", &[("ignore-delete", "true")], 3, true),
        ("partial-update", &[], 3, false),
        (
            "partial-update",
            &[("partial-update.remove-record-on-delete", "true")],
            3,
            true,
        ),
        (
            "partial-update",
            &[("partial-update.remove-record-on-delete", "true")],
            1,
            true,
        ),
        (
            "aggregation",
            &[("fields.value.aggregate-function", "sum")],
            3,
            true,
        ),
        (
            "aggregation",
            &[("fields.value.aggregate-function", "min")],
            3,
            false,
        ),
        (
            "aggregation",
            &[
                ("fields.value.aggregate-function", "min"),
                ("fields.value.ignore-retract", "true"),
            ],
            3,
            true,
        ),
        (
            "aggregation",
            &[
                ("fields.value.aggregate-function", "min"),
                ("aggregation.remove-record-on-delete", "true"),
            ],
            3,
            true,
        ),
    ];
    for (index, &(engine, extra, kind, accepted)) in cases.iter().enumerate() {
        let mut options = vec![
            ("bucket", "-2"),
            ("merge-engine", engine),
            ("target-file-row-num", "1"),
        ];
        options.extend_from_slice(extra);
        let (io, table) = memory_table(
            &format!("memory:/postpone_retract_{index}"),
            pk_schema(&options),
        );
        setup_dirs(&io, table.location()).await;
        let mut writer = table.new_write_builder().new_write().unwrap();
        writer
            .write_arrow_batch(&make_batch_with_kinds(vec![2], vec![10], vec![0]))
            .await
            .unwrap();
        let result = writer
            .write_arrow_batch(&make_batch_with_kinds(vec![1], vec![20], vec![kind]))
            .await;
        assert_eq!(
            result.is_ok(),
            accepted,
            "{engine}, {extra:?}, {kind}: {result:?}"
        );
        if accepted {
            let messages = writer.prepare_commit().await.unwrap();
            let expected = if extra.contains(&("ignore-delete", "true")) {
                1
            } else {
                2
            };
            assert_eq!(
                messages[0]
                    .new_files
                    .iter()
                    .map(|f| f.row_count)
                    .sum::<i64>(),
                expected
            );
        } else {
            assert!(writer.prepare_commit().await.is_err());
            assert!(writer
                .write_arrow_batch(&make_batch_with_kinds(vec![3], vec![30], vec![0]))
                .await
                .is_err());
            let files = io
                .list_status(&format!("{}/bucket-postpone/", table.location()))
                .await
                .unwrap();
            assert!(files.is_empty(), "failed writer left files: {files:?}");
        }
    }
}

#[tokio::test]
async fn postpone_validates_only_the_first_retract_even_across_checkpoints() {
    let (io, table) = memory_table(
        "memory:/postpone_retract_once",
        pk_schema(&[
            ("bucket", "-2"),
            ("merge-engine", "aggregation"),
            ("fields.value.aggregate-function", "min"),
            ("aggregation.remove-record-on-delete", "true"),
        ]),
    );
    setup_dirs(&io, table.location()).await;
    let mut writer = table.new_write_builder().new_write().unwrap();
    // DELETE validates successfully via remove-record-on-delete. A subsequent
    // UPDATE_BEFORE must be retained without validating min's retract again.
    for kind in [3, 1] {
        writer
            .write_arrow_batch(&make_batch_with_kinds(vec![1], vec![20], vec![kind]))
            .await
            .unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        assert_eq!(messages[0].new_files[0].delete_row_count, Some(1));
    }
}

#[tokio::test]
async fn postpone_preserves_inserts_without_merging_for_all_merge_engines() {
    for engine in ["deduplicate", "first-row", "partial-update", "aggregation"] {
        let mut options = vec![("bucket", "-2"), ("merge-engine", engine)];
        if engine == "aggregation" {
            options.push(("fields.value.aggregate-function", "sum"));
        }
        let (io, table) = memory_table(
            &format!("memory:/postpone_inserts_{engine}"),
            pk_schema(&options),
        );
        setup_dirs(&io, table.location()).await;
        let mut writer = table.new_write_builder().new_write().unwrap();
        writer
            .write_arrow_batch(&make_batch_with_kinds(
                vec![3, 1, 3],
                vec![10, 20, 30],
                vec![0, 0, 2],
            ))
            .await
            .unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        assert_eq!(messages[0].new_files[0].row_count, 3, "{engine}");
    }
}
