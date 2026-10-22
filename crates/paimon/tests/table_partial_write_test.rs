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
    // Invalid requests preserve the factory configuration. Valid requests
    // remain allowed after writing and preparing existing bucket writers.
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
        .with_write_type(vec!["id".into(), "omitted".into(), "pt".into()])
        .unwrap();
    builder.new_commit().commit(messages).await.unwrap();
    assert_eq!(rows(&table).await, vec![vec![Some(1), None, Some(2)]]);
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

fn selected_value(values: Vec<i32>, partitions: Vec<i32>) -> RecordBatch {
    RecordBatch::try_from_iter([
        ("pt", Arc::new(Int32Array::from(partitions)) as ArrayRef),
        ("omitted", Arc::new(Int32Array::from(values)) as ArrayRef),
    ])
    .unwrap()
}

fn full_values(ids: Vec<i32>, values: Vec<i32>, partitions: Vec<i32>) -> RecordBatch {
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(ids)) as ArrayRef),
        ("omitted", Arc::new(Int32Array::from(values)) as ArrayRef),
        ("pt", Arc::new(Int32Array::from(partitions)) as ArrayRef),
    ])
    .unwrap()
}

#[tokio::test]
async fn append_write_type_is_captured_per_bucket_across_prepare_rounds() {
    for explicit_bucket in [false, true] {
        let table = table(!explicit_bucket, explicit_bucket, false).await;
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer
            .with_write_type(vec!["pt".into(), "id".into()])
            .unwrap();
        let bucket = explicit_bucket.then_some(0);
        writer
            .write_arrow(&[partial(vec![11], vec![Some(1)])], bucket)
            .await
            .unwrap();
        // Repeating the same type does not lock an already active writer.
        writer
            .with_write_type(vec!["pt".into(), "id".into()])
            .unwrap();
        writer
            .with_write_type(vec!["pt".into(), "omitted".into()])
            .unwrap();
        writer
            .write_arrow(&[selected_value(vec![22], vec![2])], bucket)
            .await
            .unwrap();
        // A selected input cannot be reinterpreted positionally in an old
        // bucket. Validation must preserve both its old data and the factory.
        assert!(writer
            .write_arrow(&[selected_value(vec![999], vec![1])], bucket)
            .await
            .is_err());
        writer
            .write_arrow(&[partial(vec![33], vec![Some(1)])], bucket)
            .await
            .unwrap();
        let first = writer.prepare_commit().await.unwrap();
        assert!(first
            .iter()
            .flat_map(|m| &m.new_files)
            .any(|f| f.write_cols == Some(vec!["pt".into(), "id".into()])));
        assert!(first
            .iter()
            .flat_map(|m| &m.new_files)
            .any(|f| f.write_cols == Some(vec!["pt".into(), "omitted".into()])));
        builder.new_commit().commit(first).await.unwrap();
        // Finished physical resources are gone, but captured settings remain.
        writer
            .with_write_type(vec!["pt".into(), "omitted".into()])
            .unwrap();
        writer
            .write_arrow(&[partial(vec![44], vec![Some(1)])], bucket)
            .await
            .unwrap();
        writer
            .write_arrow(&[selected_value(vec![55], vec![2])], bucket)
            .await
            .unwrap();
        writer
            .write_arrow(&[selected_value(vec![66], vec![3])], bucket)
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
                vec![None, Some(22), Some(2)],
                vec![None, Some(55), Some(2)],
                vec![None, Some(66), Some(3)],
                vec![Some(11), None, Some(1)],
                vec![Some(33), None, Some(1)],
                vec![Some(44), None, Some(1)],
            ]
        );
    }
}

#[tokio::test]
async fn reconfigured_mixed_input_is_preflighted_and_full_input_projects_each_bucket() {
    let table = table(true, false, false).await;
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer
        .with_write_type(vec!["pt".into(), "id".into()])
        .unwrap();
    writer
        .write_arrow_batch(&partial(vec![11], vec![Some(1)]))
        .await
        .unwrap();
    writer
        .with_write_type(vec!["pt".into(), "omitted".into()])
        .unwrap();
    let before = table
        .file_io()
        .list_status_recursive(table.location())
        .await
        .unwrap()
        .len();
    assert!(writer
        .write_arrow_batch(&selected_value(vec![999, 888], vec![1, 2]))
        .await
        .is_err());
    assert_eq!(
        table
            .file_io()
            .list_status_recursive(table.location())
            .await
            .unwrap()
            .len(),
        before
    );
    writer
        .write_arrow_batch(&selected_value(vec![22], vec![2]))
        .await
        .unwrap();
    writer
        .write_arrow_batch(&full_values(vec![33, 44], vec![333, 444], vec![1, 3]))
        .await
        .unwrap();
    builder
        .new_commit()
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();
    // Subsequent valid changes do not alter existing buckets' captured order.
    writer
        .with_write_type(vec!["id".into(), "pt".into()])
        .unwrap();
    writer
        .write_arrow_batch(&full_values(vec![55, 66], vec![555, 666], vec![1, 2]))
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
            vec![None, Some(22), Some(2)],
            vec![None, Some(444), Some(3)],
            vec![None, Some(666), Some(2)],
            vec![Some(11), None, Some(1)],
            vec![Some(33), None, Some(1)],
            vec![Some(55), None, Some(1)],
        ]
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

#[cfg(feature = "internal-api")]
#[tokio::test]
async fn append_row_field_resolution_is_read_only_and_retains_captured_layouts() {
    for fixed in [false, true] {
        let table = table(false, fixed, false).await;
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer
            .with_write_type(vec!["pt".into(), "id".into()])
            .unwrap();
        let routing = |partition| {
            RecordBatch::try_from_iter([
                (
                    "pt",
                    Arc::new(Int32Array::from(vec![partition])) as ArrayRef,
                ),
                ("id", Arc::new(Int32Array::from(vec![11])) as ArrayRef),
            ])
            .unwrap()
        };
        let names = |fields: Vec<paimon::spec::DataField>| {
            fields
                .into_iter()
                .map(|f| f.name().to_owned())
                .collect::<Vec<_>>()
        };
        let before = table
            .file_io()
            .list_status_recursive(table.location())
            .await
            .unwrap()
            .len();
        assert_eq!(
            names(paimon::table::internal::resolve_row_write_fields(&writer, &routing(1)).unwrap()),
            vec!["pt", "id"]
        );
        assert!(writer.prepare_commit().await.unwrap().is_empty());
        // A query does not create or freeze the partition writer.
        writer
            .with_write_type(vec!["pt".into(), "omitted".into()])
            .unwrap();
        assert_eq!(
            names(paimon::table::internal::resolve_row_write_fields(&writer, &routing(1)).unwrap()),
            vec!["pt", "omitted"]
        );
        assert_eq!(
            table
                .file_io()
                .list_status_recursive(table.location())
                .await
                .unwrap()
                .len(),
            before
        );
        writer
            .with_write_type(vec!["pt".into(), "id".into()])
            .unwrap();
        writer
            .write_arrow_batch(&partial(vec![11], vec![Some(1)]))
            .await
            .unwrap();
        writer
            .with_write_type(vec!["pt".into(), "omitted".into()])
            .unwrap();
        assert_eq!(
            names(paimon::table::internal::resolve_row_write_fields(&writer, &routing(1)).unwrap()),
            vec!["pt", "id"]
        );
        assert_eq!(
            names(paimon::table::internal::resolve_row_write_fields(&writer, &routing(2)).unwrap()),
            vec!["pt", "omitted"]
        );
        let messages = writer.prepare_commit().await.unwrap();
        assert_eq!(
            names(paimon::table::internal::resolve_row_write_fields(&writer, &routing(1)).unwrap()),
            vec!["pt", "id"]
        );
        for invalid in [
            routing(1).slice(0, 0),
            partial(vec![1, 2], vec![Some(1), Some(2)]),
        ] {
            assert!(paimon::table::internal::resolve_row_write_fields(&writer, &invalid).is_err());
        }
        let missing_partition =
            RecordBatch::try_from_iter([("id", Arc::new(Int32Array::from(vec![11])) as ArrayRef)])
                .unwrap();
        assert!(
            paimon::table::internal::resolve_row_write_fields(&writer, &missing_partition).is_err()
        );
        // Failed queries leave prepared outputs and future factory settings intact.
        assert_eq!(
            names(paimon::table::internal::resolve_row_write_fields(&writer, &routing(2)).unwrap()),
            vec!["pt", "omitted"]
        );
        writer.close().await;
        builder.new_commit().commit(messages).await.unwrap();
        assert_eq!(rows(&table).await, vec![vec![Some(11), None, Some(1)]]);
    }
}

#[cfg(feature = "internal-api")]
#[tokio::test]
async fn unpartitioned_row_field_resolution_accepts_zero_column_routing_row() {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("omitted", DataType::Int(IntType::new()))
        .build()
        .unwrap();
    let (_, table) = memory_table("memory:/row_fields", TableSchema::new(0, &schema));
    let mut writer = table.new_write_builder().new_write().unwrap();
    let routing = RecordBatch::try_new_with_options(
        Arc::new(arrow_schema::Schema::empty()),
        vec![],
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .unwrap();
    assert_eq!(
        paimon::table::internal::resolve_row_write_fields(&writer, &routing)
            .unwrap()
            .len(),
        2
    );
    writer.with_write_type(vec!["id".into()]).unwrap();
    assert_eq!(
        paimon::table::internal::resolve_row_write_fields(&writer, &routing).unwrap()[0].name(),
        "id"
    );
    assert!(writer.prepare_commit().await.unwrap().is_empty());
}

#[tokio::test]
async fn captured_row_transport_only_allows_additional_routing_and_event_fields() {
    for fixed in [false, true] {
        let mut schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("new", DataType::Int(IntType::with_nullable(false)))
            .column("unused", DataType::Int(IntType::new()))
            .column("pt", DataType::Int(IntType::new()))
            .column(
                "op",
                DataType::VarChar(paimon::spec::VarCharType::string_type()),
            )
            .partition_keys(["pt"])
            .option("rowkind.field", "op")
            .option("ignore-delete", "true")
            .option("data-evolution.enabled", (!fixed).to_string())
            .option("row-tracking.enabled", (!fixed).to_string());
        if fixed {
            schema = schema.option("bucket", "4").option("bucket-key", "id");
        }
        let (io, table) = memory_table(
            "memory:/captured_row_transport",
            TableSchema::new(0, &schema.build().unwrap()),
        );
        setup_dirs(&io, table.location()).await;
        persist_table_schema(&io, table.location(), table.schema()).await;
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer.with_write_type(vec!["id".into()]).unwrap();
        let original = RecordBatch::try_from_iter([
            ("id", Arc::new(Int32Array::from(vec![11])) as ArrayRef),
            ("new", Arc::new(Int32Array::from(vec![22])) as ArrayRef),
            ("unused", Arc::new(Int32Array::from(vec![33])) as ArrayRef),
            ("pt", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
            (
                "op",
                Arc::new(arrow_array::StringArray::from(vec!["+I"])) as ArrayRef,
            ),
        ])
        .unwrap();
        writer.write_arrow_batch(&original).await.unwrap();
        writer.with_write_type(vec!["new".into()]).unwrap();
        let old = |partitions: Vec<i32>, kinds: Vec<&str>, extra_payload| {
            let mut columns = vec![(
                "id",
                Arc::new(Int32Array::from(vec![11; partitions.len()])) as ArrayRef,
            )];
            if extra_payload {
                columns.push((
                    "unused",
                    Arc::new(Int32Array::from(vec![99; partitions.len()])) as ArrayRef,
                ));
            }
            columns.push(("pt", Arc::new(Int32Array::from(partitions)) as ArrayRef));
            columns.push((
                "op",
                Arc::new(arrow_array::StringArray::from(kinds)) as ArrayRef,
            ));
            RecordBatch::try_from_iter(columns).unwrap()
        };
        let before = io
            .list_status_recursive(table.location())
            .await
            .unwrap()
            .len();
        // The old subset is not valid for the new partition; no group may be staged.
        assert!(writer
            .write_arrow_batch(&old(vec![1, 2], vec!["+I", "+I"], false))
            .await
            .is_err());
        // A non-routing payload cannot be smuggled into a selected layout.
        assert!(writer
            .write_arrow_batch(&old(vec![1], vec!["+I"], true))
            .await
            .is_err());
        assert_eq!(
            io.list_status_recursive(table.location())
                .await
                .unwrap()
                .len(),
            before
        );
        // Missing the new factory's NOT NULL column is valid for the captured
        // old subset. Ignored rows neither create a bucket nor inspect payloads.
        writer
            .write_arrow_batch(&old(vec![1, 2], vec!["+I", "-D"], false))
            .await
            .unwrap();
        let mut columns = vec![];
        if fixed {
            columns.push(("id", original.column(0).clone()));
        }
        columns.extend([
            ("new", original.column(1).clone()),
            ("pt", Arc::new(Int32Array::from(vec![2])) as ArrayRef),
            ("op", original.column(4).clone()),
        ]);
        writer
            .write_arrow_batch(&RecordBatch::try_from_iter(columns).unwrap())
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
        builder.new_commit().commit(messages).await.unwrap();
        writer
            .write_arrow_batch(&old(vec![1], vec!["+I"], false))
            .await
            .unwrap();
        builder
            .new_commit()
            .commit(writer.prepare_commit().await.unwrap())
            .await
            .unwrap();
        let mut reader = table.new_read_builder();
        reader
            .with_projection(&["id", "new", "unused", "pt"])
            .unwrap();
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
            for row in 0..batch.num_rows() {
                actual.push(
                    batch
                        .columns()
                        .iter()
                        .map(|column| {
                            let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
                            array.is_valid(row).then(|| array.value(row))
                        })
                        .collect::<Vec<_>>(),
                );
            }
        }
        actual.sort();
        assert_eq!(
            actual,
            vec![
                vec![None, Some(22), None, Some(2)],
                vec![Some(11), None, None, Some(1)],
                vec![Some(11), None, None, Some(1)],
                vec![Some(11), None, None, Some(1)],
            ]
        );
    }
}

#[tokio::test]
async fn historical_row_layouts_do_not_randomly_validate_unwritten_routing_fields() {
    for _ in 0..4 {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::with_nullable(false)))
            .column("new", DataType::Int(IntType::with_nullable(false)))
            .column("pt", DataType::Int(IntType::with_nullable(false)))
            .column(
                "op",
                DataType::VarChar(paimon::spec::VarCharType::string_type()),
            )
            .partition_keys(["pt"])
            .option("rowkind.field", "op")
            .option("ignore-delete", "true")
            .option("row-tracking.enabled", "true")
            .option("data-evolution.enabled", "true")
            .build()
            .unwrap();
        let (io, table) = memory_table("memory:/historical_routing", TableSchema::new(0, &schema));
        setup_dirs(&io, table.location()).await;
        persist_table_schema(&io, table.location(), table.schema()).await;
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        let input = |id, partition| {
            RecordBatch::try_from_iter([
                ("id", Arc::new(Int32Array::from(vec![id])) as ArrayRef),
                ("new", Arc::new(Int32Array::from(vec![99])) as ArrayRef),
                (
                    "pt",
                    Arc::new(Int32Array::from(vec![partition])) as ArrayRef,
                ),
                (
                    "op",
                    Arc::new(arrow_array::StringArray::from(vec!["+I"])) as ArrayRef,
                ),
            ])
            .unwrap()
        };
        // Omitted routing columns do not impose their physical NOT NULL rule.
        writer.with_write_type(vec!["id".into()]).unwrap();
        writer.write_arrow_batch(&input(11, None)).await.unwrap();
        writer
            .with_write_type(vec!["id".into(), "pt".into()])
            .unwrap();
        writer.write_arrow_batch(&input(22, Some(1))).await.unwrap();
        writer.with_write_type(vec!["new".into()]).unwrap();
        // id,pt,op can represent either historical type. Actual destinations
        // decide whether pt is selected, rather than an arbitrary map entry.
        let rows = RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int32Array::from(vec![33, 44, 55, 66])) as ArrayRef,
            ),
            (
                "pt",
                Arc::new(Int32Array::from(vec![None, Some(1), None, Some(1)])) as ArrayRef,
            ),
            (
                "op",
                Arc::new(arrow_array::StringArray::from(vec!["+I", "+I", "-D", "-D"])) as ArrayRef,
            ),
        ])
        .unwrap();
        let ignored_null_payload = RecordBatch::try_from_iter([
            ("id", Arc::new(Int32Array::from(vec![None])) as ArrayRef),
            ("pt", Arc::new(Int32Array::from(vec![None])) as ArrayRef),
            (
                "op",
                Arc::new(arrow_array::StringArray::from(vec!["-D"])) as ArrayRef,
            ),
        ])
        .unwrap();
        // Logical NOT NULL payload checks still precede ignore-delete.
        assert!(writer
            .write_arrow_batch(&ignored_null_payload)
            .await
            .is_err());
        fn require_send<T: Send>(future: T) -> T {
            future
        }
        require_send(writer.write_arrow_batch(&rows)).await.unwrap();
        builder
            .new_commit()
            .commit(writer.prepare_commit().await.unwrap())
            .await
            .unwrap();
        let mut reader = table.new_read_builder();
        reader.with_projection(&["id", "pt"]).unwrap();
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
            for row in 0..batch.num_rows() {
                actual.push(
                    batch
                        .columns()
                        .iter()
                        .map(|column| {
                            let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
                            array.is_valid(row).then(|| array.value(row))
                        })
                        .collect::<Vec<_>>(),
                );
            }
        }
        actual.sort();
        assert_eq!(
            actual,
            vec![
                vec![Some(11), None],
                vec![Some(22), Some(1)],
                vec![Some(33), None],
                vec![Some(44), Some(1)]
            ]
        );
    }
}
