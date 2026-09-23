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

use common::incremental_helpers::{
    make_batch, make_partitioned_batch, memory_table, partitioned_pk_schema, pk_schema, setup_dirs,
};
use paimon::resource::{MemoryPool, ResourceContext};
use paimon::spec::{DataType, IntType, Schema, TableSchema};
use paimon::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Debug)]
struct FailOncePool(AtomicBool);

impl MemoryPool for FailOncePool {
    fn try_reserve(&self, _bytes: usize) -> paimon::Result<()> {
        if self.0.swap(false, Ordering::SeqCst) {
            Err(Error::ResourceExhausted {
                message: "injected write-budget failure".to_string(),
            })
        } else {
            Ok(())
        }
    }

    fn release(&self, _bytes: usize) {}
}

async fn assert_no_data_files(io: &paimon::io::FileIO, path: &str) {
    let files = io.list_status_recursive(path).await.unwrap();
    assert!(
        files.iter().all(|file| !file.path.ends_with(".parquet")),
        "uncommitted files left behind: {files:?}"
    );
}

#[tokio::test]
async fn key_value_buffer_shares_limit_and_releases_on_failure() {
    let path = "memory:/writer_resources_pk";
    let (io, table) = memory_table(path, pk_schema(&[("file.format", "parquet")]));
    setup_dirs(&io, path).await;
    let batch = make_batch(vec![1, 2], vec![10, 20]);
    let bytes: usize = batch
        .columns()
        .iter()
        .map(|column| column.get_buffer_memory_size())
        .sum();
    let resources = ResourceContext::builder()
        .memory_limit(bytes)
        .build()
        .unwrap();
    let builder = table.new_write_builder().with_resources(resources.clone());
    let mut write = builder.new_write().unwrap();
    write.write_arrow_batch(&batch).await.unwrap();
    assert_eq!(resources.metrics().reserved_memory_bytes, bytes);
    assert!(matches!(
        write.write_arrow_batch(&batch).await,
        Err(Error::ResourceExhausted { .. })
    ));
    assert_eq!(resources.metrics().reserved_memory_bytes, 0);
    assert!(write.write_arrow_batch(&batch).await.is_err());
    assert!(write.prepare_commit().await.is_err());
    assert_eq!(resources.metrics().reserved_memory_bytes, 0);
    assert_no_data_files(&io, path).await;
}

#[tokio::test]
async fn key_value_buffer_releases_after_prepare_commit() {
    let path = "memory:/writer_resources_pk_commit";
    let (io, table) = memory_table(path, pk_schema(&[("file.format", "parquet")]));
    setup_dirs(&io, path).await;
    let resources = ResourceContext::builder()
        .memory_limit(1024 * 1024)
        .build()
        .unwrap();
    let builder = table.new_write_builder().with_resources(resources.clone());
    let mut write = builder.new_write().unwrap();
    write
        .write_arrow_batch(&make_batch(vec![1, 2], vec![10, 20]))
        .await
        .unwrap();
    assert!(resources.metrics().reserved_memory_bytes > 0);
    let messages = write.prepare_commit().await.unwrap();
    assert_eq!(resources.metrics().reserved_memory_bytes, 0);
    assert_eq!(messages.len(), 1);
}

#[tokio::test]
async fn append_writer_reserves_until_commit_preparation() {
    let path = "memory:/writer_resources_append";
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .option("file.format", "parquet")
        .build()
        .unwrap();
    let (io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&io, path).await;
    let resources = ResourceContext::builder()
        .memory_limit(1024 * 1024)
        .build()
        .unwrap();
    let builder = table.new_write_builder().with_resources(resources.clone());
    let mut write = builder.new_write().unwrap();
    write
        .write_arrow_batch(&make_batch(vec![1, 2], vec![10, 20]))
        .await
        .unwrap();
    assert!(resources.metrics().reserved_memory_bytes > 0);
    let messages = write.prepare_commit().await.unwrap();
    assert_eq!(resources.metrics().reserved_memory_bytes, 0);
    assert_eq!(messages.len(), 1);
}

#[tokio::test]
async fn append_writer_rejects_batch_before_format_write() {
    let path = "memory:/writer_resources_zero";
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .option("file.format", "parquet")
        .build()
        .unwrap();
    let (io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&io, path).await;
    let resources = ResourceContext::builder().memory_limit(0).build().unwrap();
    let mut write = table
        .new_write_builder()
        .with_resources(resources.clone())
        .new_write()
        .unwrap();
    assert!(matches!(
        write
            .write_arrow_batch(&make_batch(vec![1], vec![10]))
            .await,
        Err(Error::ResourceExhausted { .. })
    ));
    assert_eq!(resources.metrics().reserved_memory_bytes, 0);
    assert!(write.prepare_commit().await.is_err());
    assert_no_data_files(&io, path).await;
}

#[tokio::test]
async fn postpone_writer_uses_the_same_budget() {
    let path = "memory:/writer_resources_postpone";
    let (io, table) = memory_table(
        path,
        pk_schema(&[("bucket", "-2"), ("file.format", "parquet")]),
    );
    setup_dirs(&io, path).await;
    let resources = ResourceContext::builder().memory_limit(0).build().unwrap();
    let mut write = table
        .new_write_builder()
        .with_resources(resources.clone())
        .new_write()
        .unwrap();
    assert!(matches!(
        write
            .write_arrow_batch(&make_batch(vec![1], vec![10]))
            .await,
        Err(Error::ResourceExhausted { .. })
    ));
    assert_eq!(resources.metrics().reserved_memory_bytes, 0);
    assert!(write.prepare_commit().await.is_err());
    assert_no_data_files(&io, path).await;
}

#[tokio::test]
async fn append_budget_rejection_prevents_partial_commit() {
    let path = "memory:/writer_resources_append_retry";
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .option("file.format", "parquet")
        .build()
        .unwrap();
    let (io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&io, path).await;
    let limit = 1024 * 1024;
    let resources = ResourceContext::builder()
        .memory_limit(limit)
        .build()
        .unwrap();
    let mut write = table
        .new_write_builder()
        .with_resources(resources.clone())
        .new_write()
        .unwrap();
    write
        .write_arrow_batch(&make_batch(vec![1, 2], vec![10, 20]))
        .await
        .unwrap();
    let mut other = resources.reservation();
    other
        .try_grow(limit - resources.metrics().reserved_memory_bytes)
        .unwrap();
    assert!(matches!(
        write
            .write_arrow_batch(&make_batch(vec![3], vec![30]))
            .await,
        Err(Error::ResourceExhausted { .. })
    ));
    drop(other);
    assert!(write.prepare_commit().await.is_err());
    assert_eq!(resources.metrics().reserved_memory_bytes, 0);
    assert_no_data_files(&io, path).await;
}

#[tokio::test]
async fn postpone_budget_rejection_prevents_partial_commit() {
    let path = "memory:/writer_resources_postpone_retry";
    let (io, table) = memory_table(
        path,
        pk_schema(&[("bucket", "-2"), ("file.format", "parquet")]),
    );
    setup_dirs(&io, path).await;
    let limit = 1024 * 1024;
    let resources = ResourceContext::builder()
        .memory_limit(limit)
        .build()
        .unwrap();
    let mut write = table
        .new_write_builder()
        .with_resources(resources.clone())
        .new_write()
        .unwrap();
    write
        .write_arrow_batch(&make_batch(vec![1, 2], vec![10, 20]))
        .await
        .unwrap();
    let mut other = resources.reservation();
    other
        .try_grow(limit - resources.metrics().reserved_memory_bytes)
        .unwrap();
    assert!(matches!(
        write
            .write_arrow_batch(&make_batch(vec![3], vec![30]))
            .await,
        Err(Error::ResourceExhausted { .. })
    ));
    drop(other);
    assert!(write.prepare_commit().await.is_err());
    assert_eq!(resources.metrics().reserved_memory_bytes, 0);
    assert_no_data_files(&io, path).await;
}

#[tokio::test]
async fn key_value_flush_rejection_prevents_partial_commit() {
    let path = "memory:/writer_resources_pk_flush";
    let (io, table) = memory_table(
        path,
        pk_schema(&[
            ("write.parquet-buffer-size", "1b"),
            ("file.format", "parquet"),
        ]),
    );
    setup_dirs(&io, path).await;
    let batch = make_batch(vec![1, 2], vec![10, 20]);
    let bytes: usize = batch
        .columns()
        .iter()
        .map(|column| column.get_buffer_memory_size())
        .sum();
    let resources = ResourceContext::builder()
        .memory_limit(bytes)
        .build()
        .unwrap();
    let mut write = table
        .new_write_builder()
        .with_resources(resources.clone())
        .new_write()
        .unwrap();
    assert!(matches!(
        write.write_arrow_batch(&batch).await,
        Err(Error::ResourceExhausted { .. })
    ));
    assert!(write.prepare_commit().await.is_err());
    assert_eq!(resources.metrics().reserved_memory_bytes, 0);
    assert_no_data_files(&io, path).await;
}

#[tokio::test]
async fn key_value_prepare_rejection_cleans_uncommitted_file() {
    let path = "memory:/writer_resources_pk_prepare";
    let (io, table) = memory_table(path, pk_schema(&[("file.format", "parquet")]));
    setup_dirs(&io, path).await;
    let batch = make_batch(vec![1, 2], vec![10, 20]);
    let bytes: usize = batch
        .columns()
        .iter()
        .map(|column| column.get_buffer_memory_size())
        .sum();
    let resources = ResourceContext::builder()
        .memory_limit(bytes)
        .build()
        .unwrap();
    let mut write = table
        .new_write_builder()
        .with_resources(resources.clone())
        .new_write()
        .unwrap();
    write.write_arrow_batch(&batch).await.unwrap();
    assert!(matches!(
        write.prepare_commit().await,
        Err(Error::ResourceExhausted { .. })
    ));
    assert_eq!(resources.metrics().reserved_memory_bytes, 0);
    assert_no_data_files(&io, path).await;
    assert!(write.prepare_commit().await.is_err());
}

#[tokio::test]
async fn one_partition_prepare_failure_cleans_other_partition_outputs() {
    let path = "memory:/writer_resources_multi_partition_prepare";
    let (io, table) = memory_table(path, partitioned_pk_schema("1"));
    setup_dirs(&io, path).await;
    let pool = Arc::new(FailOncePool(AtomicBool::new(false)));
    let resources = ResourceContext::builder()
        .memory_pool(pool.clone())
        .build()
        .unwrap();
    let mut write = table
        .new_write_builder()
        .with_resources(resources.clone())
        .new_write()
        .unwrap();
    write
        .write_arrow_batch(&make_partitioned_batch(
            vec!["a", "b"],
            vec![1, 2],
            vec![10, 20],
        ))
        .await
        .unwrap();
    pool.0.store(true, Ordering::SeqCst);
    assert!(matches!(
        write.prepare_commit().await,
        Err(Error::ResourceExhausted { .. })
    ));
    assert_eq!(resources.metrics().reserved_memory_bytes, 0);
    assert_no_data_files(&io, path).await;
}
