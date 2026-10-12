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

use super::{Table, TableWrite};
use crate::catalog::Identifier;
use crate::io::{FileIOBuilder, UriInputStream, UriReader, UriReaderFactory};
use crate::spec::{BlobDescriptor, BlobType, DataType, IntType, Schema, TableSchema};
use crate::Result;
use arrow_array::{ArrayRef, Int32Array, LargeBinaryArray, RecordBatch};
use bytes::Bytes;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Default)]
struct Reader {
    opened: AtomicUsize,
    closed: Arc<AtomicUsize>,
}

struct Factory(Arc<Reader>);

impl UriReaderFactory for Factory {
    fn create(&self, uri: &str) -> Result<Arc<dyn UriReader>> {
        assert_eq!(uri, "custom://payload");
        Ok(self.0.clone())
    }
}

#[async_trait::async_trait]
impl UriReader for Reader {
    async fn new_input_stream(&self, _uri: &str) -> Result<Box<dyn UriInputStream>> {
        self.opened.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(Stream {
            position: 0,
            closed: self.closed.clone(),
        }))
    }
}

struct Stream {
    position: usize,
    closed: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl UriInputStream for Stream {
    async fn read(&mut self, length: usize) -> Result<Bytes> {
        let bytes = Bytes::from_static(b"payload");
        let end = (self.position + length).min(bytes.len());
        let result = bytes.slice(self.position..end);
        self.position = end;
        Ok(result)
    }

    async fn close(&mut self) -> Result<()> {
        self.closed.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

fn table(bucket: &str) -> Table {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("payload", DataType::Blob(BlobType::new()))
        .primary_key(["id"])
        .option("bucket", bucket)
        .option("postpone.default-bucket-num", "2")
        .option("blob.target-file-size", "1 MB")
        .build()
        .unwrap();
    Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("db", "t"),
        format!("memory:/uri-reader-{bucket}"),
        TableSchema::new(0, &schema),
        None,
    )
}

fn batch() -> RecordBatch {
    let descriptor = BlobDescriptor::new("custom://payload".into(), 0, 7).serialize();
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
        (
            "payload",
            Arc::new(LargeBinaryArray::from(vec![descriptor.as_slice()])) as ArrayRef,
        ),
    ])
    .unwrap()
}

#[tokio::test]
async fn managed_blob_abort_closes_custom_source_before_removing_unprepared_files() {
    for bucket in ["1", "-1", "-2"] {
        let table = table(bucket);
        let reader = Arc::new(Reader::default());
        let mut writer = TableWrite::new(&table, "uri-reader".into()).unwrap();
        writer
            .with_blob_uri_reader_factory(Some(Arc::new(Factory(reader.clone()))))
            .unwrap();
        writer.write_arrow_batch(&batch()).await.unwrap();
        assert_eq!(reader.opened.load(Ordering::Relaxed), 1);
        assert_eq!(reader.closed.load(Ordering::Relaxed), 0);
        writer.close().await;
        assert_eq!(reader.closed.load(Ordering::Relaxed), 1, "bucket {bucket}");
        writer.close().await;
        assert_eq!(reader.closed.load(Ordering::Relaxed), 1);
        assert!(table
            .file_io()
            .list_status_recursive(table.location())
            .await
            .unwrap()
            .is_empty());
    }
}

#[tokio::test]
async fn postpone_fixed_blob_reader_factory_cannot_change_after_writing() {
    let table = table("-2");
    let reader = Arc::new(Reader::default());
    let mut writer = table
        .new_postpone_fixed_bucket_write_builder()
        .unwrap()
        .new_write()
        .unwrap();
    writer
        .with_blob_uri_reader_factory(Some(Arc::new(Factory(reader.clone()))))
        .unwrap();
    writer.write_arrow_batch(&batch()).await.unwrap();
    let error = writer.with_blob_uri_reader_factory(None).err().unwrap();
    assert!(error.to_string().contains("before any write"));
    writer.close().await;
    assert_eq!(reader.opened.load(Ordering::Relaxed), 1);
    assert_eq!(reader.closed.load(Ordering::Relaxed), 1);
}
