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

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch};
use futures::TryStreamExt;
use paimon::spec::{BlobType, DataType, IntType, Schema, TableSchema};
use paimon::table::Table;

use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs};

async fn table(evolution: bool, fixed_bucket: bool, blob: bool) -> Table {
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "omitted",
            if blob {
                DataType::Blob(BlobType::new())
            } else {
                DataType::Int(IntType::new())
            },
        )
        .column("pt", DataType::Int(IntType::new()))
        .partition_keys(["pt"])
        .option("row-tracking.enabled", evolution.to_string())
        .option("data-evolution.enabled", evolution.to_string());
    if fixed_bucket {
        schema = schema.option("bucket", "4").option("bucket-key", "id");
    }
    let (io, table) = memory_table(
        "memory:/partial_write",
        TableSchema::new(0, &schema.build().unwrap()),
    );
    setup_dirs(&io, table.location()).await;
    persist_table_schema(&io, table.location(), table.schema()).await;
    table
}

fn partial(ids: Vec<i32>, partitions: Vec<Option<i32>>) -> RecordBatch {
    RecordBatch::try_from_iter([
        ("pt", Arc::new(Int32Array::from(partitions)) as ArrayRef),
        ("id", Arc::new(Int32Array::from(ids)) as ArrayRef),
    ])
    .unwrap()
}

async fn rows(table: &Table) -> Vec<Vec<Option<i32>>> {
    let reader = table.new_read_builder();
    let plan = reader.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = reader
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        let column = column.as_any().downcast_ref::<Int32Array>().unwrap();
                        column.is_valid(row).then(|| column.value(row))
                    })
                    .collect(),
            );
        }
    }
    rows.sort();
    rows
}

#[tokio::test]
async fn partial_append_routes_projected_partition_and_bucket_indices() {
    for evolution in [false, true] {
        // Java disallows fixed buckets for data evolution tables.
        for fixed_bucket in [false, true]
            .into_iter()
            .filter(|fixed| !evolution || !fixed)
        {
            let table = table(evolution, fixed_bucket, false).await;
            let builder = table.new_write_builder();
            let mut writer = builder.new_write().unwrap();
            writer
                .with_write_type(vec!["pt".into(), "id".into()])
                .unwrap();
            writer
                .write_arrow_batch(&partial(vec![1, 2, 3], vec![Some(7), Some(8), None]))
                .await
                .unwrap();
            let messages = writer.prepare_commit().await.unwrap();
            assert_eq!(
                messages
                    .iter()
                    .flat_map(|m| &m.new_files)
                    .map(|f| f.row_count)
                    .sum::<i64>(),
                3
            );
            for file in messages.iter().flat_map(|m| &m.new_files) {
                assert_eq!(file.write_cols, Some(vec!["pt".into(), "id".into()]));
            }
            builder.new_commit().commit(messages).await.unwrap();
            // Stream checkpoints must retain the selected type.
            writer
                .write_arrow_batch(&partial(vec![4], vec![Some(7)]))
                .await
                .unwrap();
            builder
                .new_commit()
                .commit(writer.prepare_commit().await.unwrap())
                .await
                .unwrap();
            assert_eq!(
                rows(&table).await,
                vec![
                    vec![Some(1), None, Some(7)],
                    vec![Some(2), None, Some(8)],
                    vec![Some(3), None, None],
                    vec![Some(4), None, Some(7)],
                ]
            );
        }
    }
}

#[tokio::test]
async fn invalid_write_type_does_not_replace_previous_configuration() {
    let table = table(false, true, false).await;
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer
        .with_write_type(vec!["pt".into(), "id".into()])
        .unwrap();
    for columns in [vec![], vec!["pt", "id", "id"], vec!["pt", "missing"]] {
        assert!(writer
            .with_write_type(columns.into_iter().map(str::to_string).collect())
            .is_err());
    }
    // Empty batches do not lock the configuration; nonempty ones do, even
    // after their files have been prepared.
    writer
        .write_arrow_batch(&partial(vec![], vec![]))
        .await
        .unwrap();
    writer
        .with_write_type(vec!["pt".into(), "id".into()])
        .unwrap();
    writer
        .write_arrow_batch(&partial(vec![1], vec![Some(2)]))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    writer
        .with_write_type(vec!["pt".into(), "id".into()])
        .unwrap();
    assert!(writer
        .with_write_type(vec!["id".into(), "omitted".into(), "pt".into()])
        .is_err());
    builder.new_commit().commit(messages).await.unwrap();
    assert_eq!(rows(&table).await, vec![vec![Some(1), None, Some(2)]]);
}

#[tokio::test]
async fn append_write_type_stays_locked_across_partitions_and_prepares() {
    for fixed in [false, true] {
        let table = table(!fixed, fixed, false).await;
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer
            .with_write_type(vec!["pt".into(), "id".into()])
            .unwrap();
        writer
            .write_arrow(&[partial(vec![11], vec![Some(1)])], fixed.then_some(0))
            .await
            .unwrap();
        for columns in [vec!["pt", "omitted"], vec!["id", "pt"]] {
            let error = writer
                .with_write_type(columns.into_iter().map(str::to_string).collect())
                .err()
                .unwrap();
            assert!(error.to_string().contains("before any write operation"));
        }
        writer
            .with_write_type(vec!["pt".into(), "id".into()])
            .unwrap();
        let full = RecordBatch::try_from_iter([
            ("id", Arc::new(Int32Array::from(vec![22, 33])) as ArrayRef),
            (
                "omitted",
                Arc::new(Int32Array::from(vec![99, 99])) as ArrayRef,
            ),
            ("pt", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
        ])
        .unwrap();
        if fixed {
            for row in 0..2 {
                writer
                    .write_arrow(&[full.slice(row, 1)], Some(0))
                    .await
                    .unwrap();
            }
        } else {
            writer.write_arrow_batch(&full).await.unwrap();
        }
        let mut messages = writer.prepare_commit().await.unwrap();
        assert!(writer.prepare_commit().await.unwrap().is_empty());
        assert!(writer.with_write_type(vec!["omitted".into()]).is_err());
        writer
            .write_arrow(&[partial(vec![44], vec![Some(3)])], fixed.then_some(0))
            .await
            .unwrap();
        messages.extend(writer.prepare_commit().await.unwrap());
        builder.new_commit().commit(messages).await.unwrap();
        assert_eq!(
            rows(&table).await,
            vec![
                vec![Some(11), None, Some(1)],
                vec![Some(22), None, Some(1)],
                vec![Some(33), None, Some(2)],
                vec![Some(44), None, Some(3)],
            ]
        );
    }
}

#[tokio::test]
async fn selected_input_accepts_only_current_columns_and_routing_fields() {
    use paimon::spec::{BlobType, VarCharType};
    for fixed in [false, true] {
        let mut schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::with_nullable(false)))
            .column(
                "unused",
                if fixed {
                    DataType::Int(IntType::new())
                } else {
                    DataType::Blob(BlobType::new())
                },
            )
            .column("pt", DataType::Int(IntType::with_nullable(false)))
            .column("op", DataType::VarChar(VarCharType::string_type()))
            .partition_keys(["pt"])
            .option("rowkind.field", "op")
            .option("ignore-delete", "true")
            .option("data-evolution.enabled", (!fixed).to_string())
            .option("row-tracking.enabled", (!fixed).to_string());
        if fixed {
            schema = schema.option("bucket", "4").option("bucket-key", "id");
        }
        let (io, table) = memory_table(
            "memory:/selected_transport",
            TableSchema::new(0, &schema.build().unwrap()),
        );
        setup_dirs(&io, table.location()).await;
        persist_table_schema(&io, table.location(), table.schema()).await;
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer.with_write_type(vec!["value".into()]).unwrap();
        let transport = |values: Vec<Option<i32>>, kinds: Vec<&str>, extra_payload| {
            let count = values.len();
            let mut columns = vec![
                ("value", Arc::new(Int32Array::from(values)) as ArrayRef),
                (
                    "pt",
                    Arc::new(Int32Array::from(vec![Some(1); count])) as ArrayRef,
                ),
                (
                    "op",
                    Arc::new(arrow_array::StringArray::from(kinds)) as ArrayRef,
                ),
            ];
            if fixed {
                columns.push((
                    "id",
                    Arc::new(Int32Array::from(vec![10; count])) as ArrayRef,
                ));
            }
            if extra_payload {
                let unused: ArrayRef = if fixed {
                    Arc::new(Int32Array::from(vec![99; count]))
                } else {
                    Arc::new(arrow_array::LargeBinaryArray::from(vec![
                        b"bad".as_slice();
                        count
                    ]))
                };
                columns.push(("unused", unused));
            }
            RecordBatch::try_from_iter(columns).unwrap()
        };
        // Ignored events do not validate nested or omitted payload values, but
        // the current selected NOT NULL fields still have their logical rule.
        assert!(writer
            .write_arrow_batch(&transport(vec![None], vec!["-D"], false))
            .await
            .is_err());
        writer
            .write_arrow_batch(&transport(vec![Some(1)], vec!["-D"], false))
            .await
            .unwrap();
        writer
            .with_write_type(vec!["value".into(), "pt".into()])
            .unwrap();
        writer.with_write_type(vec!["value".into()]).unwrap();
        writer
            .write_arrow_batch(&transport(
                vec![Some(22), Some(33)],
                vec!["+I", "-D"],
                false,
            ))
            .await
            .unwrap();
        assert!(writer
            .write_arrow_batch(&transport(vec![Some(99)], vec!["+I"], true))
            .await
            .is_err());
        assert!(writer.with_write_type(vec!["id".into()]).is_err());
        let messages = writer.prepare_commit().await.unwrap();
        assert_eq!(
            messages
                .iter()
                .flat_map(|m| &m.new_files)
                .map(|file| file.row_count)
                .sum::<i64>(),
            1
        );
        assert!(messages
            .iter()
            .flat_map(|m| &m.new_files)
            .all(|file| !file.file_name.ends_with(".blob")));
        builder.new_commit().commit(messages).await.unwrap();
        let mut read = table.new_read_builder();
        read.with_projection(&["value", "pt"]).unwrap();
        let plan = read.new_scan().plan().await.unwrap();
        let batches: Vec<RecordBatch> = read
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!(
            batches[0]
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            22
        );
        assert_eq!(
            batches[0]
                .column_by_name("pt")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            1
        );
    }
}

#[tokio::test]
async fn omitted_managed_blob_does_not_create_dedicated_files() {
    let table = table(true, false, true).await;
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer
        .with_write_type(vec!["pt".into(), "id".into()])
        .unwrap();
    writer
        .write_arrow_batch(&partial(vec![1], vec![Some(2)]))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    let files = messages
        .iter()
        .flat_map(|m| &m.new_files)
        .collect::<Vec<_>>();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].write_cols, Some(vec!["pt".into(), "id".into()]));
    assert!(files[0].file_name.ends_with(".parquet"));
    builder.new_commit().commit(messages).await.unwrap();
    let reader = table.new_read_builder();
    let plan = reader.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = reader
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches[0].column(1).null_count(), 1);
}

#[tokio::test]
async fn blob_only_partial_write_has_no_empty_normal_file_and_keeps_row_count() {
    use arrow_array::LargeBinaryArray;
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("payload", DataType::Blob(BlobType::new()))
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .option("target-file-row-num", "2")
        .build()
        .unwrap();
    let (io, table) = memory_table("memory:/blob_only_partial", TableSchema::new(0, &schema));
    setup_dirs(&io, table.location()).await;
    persist_table_schema(&io, table.location(), table.schema()).await;
    let builder = table.new_write_builder();
    let mut seed = builder.new_write().unwrap();
    seed.with_write_type(vec!["id".into()]).unwrap();
    let seed_input =
        RecordBatch::try_from_iter([("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef)])
            .unwrap();
    seed.write_arrow_batch(&seed_input.slice(0, 2))
        .await
        .unwrap();
    seed.write_arrow_batch(&seed_input.slice(2, 1))
        .await
        .unwrap();
    builder
        .new_commit()
        .commit(seed.prepare_commit().await.unwrap())
        .await
        .unwrap();
    let mut writer = builder.new_write().unwrap();
    writer.with_write_type(vec!["payload".into()]).unwrap();
    let values = vec![Some(b"first".as_slice()), None, Some(b"third".as_slice())];
    let input = RecordBatch::try_from_iter([(
        "payload",
        Arc::new(LargeBinaryArray::from(values.clone())) as ArrayRef,
    )])
    .unwrap();
    writer.write_arrow_batch(&input).await.unwrap();
    let mut messages = writer.prepare_commit().await.unwrap();
    let files = messages
        .iter()
        .flat_map(|m| &m.new_files)
        .collect::<Vec<_>>();
    assert_eq!(
        files.iter().map(|f| f.row_count).collect::<Vec<_>>(),
        vec![2, 1]
    );
    assert!(files
        .iter()
        .all(|file| file.file_name.ends_with(".blob")
            && file.write_cols == Some(vec!["payload".into()])));
    // Dedicated-only files update an existing row range. As in Java, they
    // require explicit row IDs because no normal file allocates new IDs.
    let mut first_row_id = 0;
    for file in messages
        .iter_mut()
        .flat_map(|message| &mut message.new_files)
    {
        file.first_row_id = Some(first_row_id);
        first_row_id += file.row_count;
    }
    builder.new_commit().commit(messages).await.unwrap();
    let reader = table.new_read_builder();
    let plan = reader.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = reader
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut actual = Vec::new();
    for batch in batches {
        assert_eq!(batch.column(0).null_count(), 0);
        let payload = batch
            .column(1)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        actual.extend(
            (0..batch.num_rows())
                .map(|row| payload.is_valid(row).then(|| payload.value(row).to_vec())),
        );
    }
    let mut expected = values
        .into_iter()
        .map(|value| value.map(|bytes| bytes.to_vec()))
        .collect::<Vec<_>>();
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn selected_columns_validate_input_before_staging() {
    let table = table(false, false, false).await;
    let mut writer = table.new_write_builder().new_write().unwrap();
    writer
        .with_write_type(vec!["pt".into(), "id".into()])
        .unwrap();
    let input = partial(vec![1], vec![Some(2)]);
    for bad in [
        input.project(&[1, 0]).unwrap(),
        input.project(&[0]).unwrap(),
    ] {
        assert!(writer.write_arrow_batch(&bad).await.is_err());
    }
    assert!(table
        .file_io()
        .list_status_recursive(table.location())
        .await
        .unwrap()
        .iter()
        .all(|status| !status.path.ends_with(".parquet")));
    writer.write_arrow_batch(&input).await.unwrap();
    assert_eq!(
        writer.prepare_commit().await.unwrap()[0].new_files[0].row_count,
        1
    );
}

#[tokio::test]
async fn append_blob_consumer_is_captured_per_partition_across_prepares() {
    use paimon::spec::BlobDescriptor;
    use std::sync::Mutex;
    let table = table(true, false, true).await;
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let consumer = |name: &'static str| {
        let calls = calls.clone();
        Arc::new(move |_: &str, descriptor: Option<&BlobDescriptor>| {
            if descriptor.is_some() {
                calls.lock().unwrap().push(name);
            }
            Ok(false)
        }) as Arc<dyn paimon::spec::BlobConsumer>
    };
    let blob_batch = |partition| {
        RecordBatch::try_from_iter([
            ("id", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
            (
                "omitted",
                Arc::new(arrow_array::LargeBinaryArray::from(vec![
                    b"payload".as_slice()
                ])) as ArrayRef,
            ),
            (
                "pt",
                Arc::new(Int32Array::from(vec![partition])) as ArrayRef,
            ),
        ])
        .unwrap()
    };
    writer.with_blob_consumer(Some(consumer("first"))).unwrap();
    writer.write_arrow_batch(&blob_batch(1)).await.unwrap();
    writer.with_blob_consumer(Some(consumer("second"))).unwrap();
    writer.write_arrow_batch(&blob_batch(1)).await.unwrap();
    writer.write_arrow_batch(&blob_batch(2)).await.unwrap();
    let first = writer.prepare_commit().await.unwrap();
    builder.new_commit().commit(first).await.unwrap();
    writer.with_blob_consumer(None).unwrap();
    writer.write_arrow_batch(&blob_batch(1)).await.unwrap();
    writer.write_arrow_batch(&blob_batch(2)).await.unwrap();
    writer.write_arrow_batch(&blob_batch(3)).await.unwrap();
    let second = writer.prepare_commit().await.unwrap();
    writer.close().await;
    // Closing the writer cannot remove any successfully prepared payload.
    builder.new_commit().commit(second).await.unwrap();
    let mut received = calls.lock().unwrap().clone();
    received.sort();
    assert_eq!(
        received,
        vec!["first", "first", "first", "second", "second"]
    );
}
