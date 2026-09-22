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

use arrow_array::Int32Array;
use common::incremental_helpers::{
    make_batch, memory_table, persist_table_schema, setup_dirs, write_batch,
};
use futures::{StreamExt, TryStreamExt};
use paimon::resource::ResourceContext;
use paimon::spec::{DataType, Datum, IntType, PredicateBuilder, Schema, TableSchema};
use paimon::table::{ArrowRecordBatchStream, AuditLogRead, IncrementalScanMode, Table};
use paimon::Error;

async fn parquet_table(primary_key: bool) -> Table {
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .option("file.format", "parquet");
    if primary_key {
        schema = schema.primary_key(["id"]).option("bucket", "1");
    }
    let path = "memory:/reader_resources";
    let (io, table) = memory_table(path, TableSchema::new(0, &schema.build().unwrap()));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    write_batch(&table, &make_batch(vec![1, 2, 3], vec![10, 20, 30])).await;
    table
}

#[tokio::test]
async fn parquet_output_reservation_survives_reader_and_batch() {
    for primary_key in [false, true] {
        let table = parquet_table(primary_key).await;
        let resources = ResourceContext::builder()
            .memory_limit(1024 * 1024)
            .build()
            .unwrap();
        let mut builder = table.new_read_builder();
        builder.with_resources(resources.clone());
        builder.with_projection(&["id"]).unwrap();
        let plan = builder.new_scan().plan().await.unwrap();
        let predicate = PredicateBuilder::new(table.schema().fields())
            .greater_or_equal("id", Datum::Int(2))
            .unwrap();
        // Builder cloning and subsequent consuming setters must preserve the context.
        let read = builder
            .clone()
            .new_read()
            .unwrap()
            .with_filter(predicate)
            .with_blob_parallelism(2)
            .unwrap()
            .with_parquet_read_budget(Arc::new(
                paimon::arrow::ReadBudget::new(2, 1024 * 1024).unwrap(),
            ));
        let mut stream = read.to_arrow(plan.splits()).unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.values().as_ref(), &[2, 3]);
        let bytes = resources.metrics().reserved_memory_bytes;
        assert!(bytes > 0);
        let escaped = batch.column(0).slice(1, 1);
        drop(batch);
        drop(stream);
        drop(read);
        drop(builder);
        let retained_bytes = resources.metrics().reserved_memory_bytes;
        assert!(retained_bytes > 0 && retained_bytes <= bytes);
        assert_eq!(
            escaped
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            3
        );
        drop(escaped);
        assert_eq!(resources.metrics().reserved_memory_bytes, 0);
    }
}

async fn assert_exhausted(mut stream: ArrowRecordBatchStream, resources: &ResourceContext) {
    assert!(matches!(
        stream.next().await.unwrap(),
        Err(Error::ResourceExhausted { .. })
    ));
    assert!(
        stream.next().await.is_none(),
        "exhaustion must end the stream"
    );
    assert_eq!(resources.metrics().reserved_memory_bytes, 0);
}

#[tokio::test]
async fn every_read_output_mode_honors_the_budget() {
    let table = parquet_table(false).await;
    let resources = ResourceContext::builder().memory_limit(0).build().unwrap();
    let mut builder = table.new_read_builder();
    builder.with_resources(resources.clone());
    let plan = builder.new_scan().plan().await.unwrap();
    let incremental = builder
        .new_incremental_scan(IncrementalScanMode::Delta, 0, 1)
        .plan()
        .await
        .unwrap();
    let read = builder.new_read().unwrap();
    assert_exhausted(read.to_arrow(plan.splits()).unwrap(), &resources).await;
    assert_exhausted(
        read.to_arrow_with_row_kind(plan.splits()).unwrap(),
        &resources,
    )
    .await;
    assert_exhausted(read.to_incremental_arrow(&incremental).unwrap(), &resources).await;
    assert_exhausted(read.to_audit_log_arrow(&incremental).unwrap(), &resources).await;
    assert_exhausted(
        AuditLogRead::new(read)
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap(),
        &resources,
    )
    .await;
    // No buffers are needed for a zero-column projection, even under a zero budget.
    builder.with_projection(&[]).unwrap();
    let batches: Vec<_> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        3
    );
    assert!(batches.iter().all(|batch| batch.num_columns() == 0));
}

#[tokio::test]
async fn independent_readers_share_outstanding_output_reservations() {
    let table = parquet_table(false).await;
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let resources = ResourceContext::builder()
        .memory_limit(1024 * 1024)
        .build()
        .unwrap();
    let read = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .with_resources(resources.clone());
    let mut stream = read.to_arrow(plan.splits()).unwrap();
    let retained = stream.next().await.unwrap().unwrap();
    drop(stream);
    let retained_bytes = resources.metrics().reserved_memory_bytes;
    assert!(retained_bytes > 0);
    // Occupy the rest with another consumer so the second reader cannot emit a batch.
    let mut other_consumer = resources.reservation();
    other_consumer
        .try_grow(1024 * 1024 - retained_bytes)
        .unwrap();
    let mut second = read.clone().to_arrow(plan.splits()).unwrap();
    assert!(matches!(
        second.next().await.unwrap(),
        Err(Error::ResourceExhausted { .. })
    ));
    assert!(second.next().await.is_none());
    drop(second);
    assert_eq!(resources.metrics().reserved_memory_bytes, 1024 * 1024);
    drop(other_consumer);
    drop(retained);
    assert_eq!(resources.metrics().reserved_memory_bytes, 0);
    // A fresh read can use capacity released by the failed read's siblings.
    let batches: Vec<_> = read
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        3
    );
    drop(batches);
    assert_eq!(resources.metrics().reserved_memory_bytes, 0);
}

#[tokio::test]
async fn format_table_reader_uses_the_same_resource_context() {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .option("type", "format-table")
        .option("file.format", "parquet")
        .build()
        .unwrap();
    let path = "memory:/format_reader_resources";
    let (io, table) = memory_table(path, TableSchema::new(0, &schema));
    io.mkdirs(path).await.unwrap();
    let input = make_batch(vec![1, 2], vec![10, 20]);
    let mut bytes = Vec::new();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(&mut bytes, input.schema(), None).unwrap();
    writer.write(&input).unwrap();
    writer.close().unwrap();
    io.new_output(&format!("{path}/data.parquet"))
        .unwrap()
        .write(bytes.into())
        .await
        .unwrap();
    let resources = ResourceContext::builder().memory_limit(0).build().unwrap();
    let mut builder = table.new_read_builder();
    builder.with_resources(resources.clone());
    let plan = builder.new_scan().plan().await.unwrap();
    let read = builder
        .clone()
        .new_read()
        .unwrap()
        .with_blob_parallelism(1)
        .unwrap();
    assert_exhausted(read.to_arrow(plan.splits()).unwrap(), &resources).await;
    assert_exhausted(
        read.to_arrow_with_row_kind(plan.splits()).unwrap(),
        &resources,
    )
    .await;

    let resources = ResourceContext::builder()
        .memory_limit(1024)
        .build()
        .unwrap();
    let output: Vec<_> = read
        .with_resources(resources.clone())
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].column(0).as_ref(), input.column(0).as_ref());
    assert!(resources.metrics().reserved_memory_bytes > 0);
    drop(output);
    assert_eq!(resources.metrics().reserved_memory_bytes, 0);
}
