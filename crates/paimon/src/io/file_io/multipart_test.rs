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
use crate::io::FileIOBuilder;
use opendal::raw::{
    oio, OpCopier, OpCopy, OpCreateDir, OpList, OpPresign, OpRead, OpRename, OpStat, OpWrite,
    RpCreateDir, RpPresign, RpRename, RpStat, Service, ServiceInfo,
};
use opendal::{Buffer, Capability, Metadata, OperationContext};
use std::collections::BTreeMap;
use std::sync::Mutex;
use tokio::io::AsyncWriteExt;
use tokio::sync::Notify;

const CHUNK: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) enum Fault {
    #[default]
    None,
    Part,
    Close,
}

#[derive(Debug, Default)]
pub(crate) struct UploadState {
    pub(crate) fault: Fault,
    pub(crate) fail_on_index: usize,
    pub(crate) index_writes: usize,
    pub(crate) concurrency: Vec<usize>,
    pub(crate) uploads: BTreeMap<String, BTreeMap<usize, Bytes>>,
    pub(crate) completed_parts: Vec<usize>,
}

/// Exercise OpenDAL's real multipart scheduler, storing completed objects in memory.
#[derive(Clone, Debug)]
pub(crate) struct MultipartProvider {
    memory: Operator,
    part_size: usize,
    pub(crate) state: Arc<Mutex<UploadState>>,
}

impl MultipartProvider {
    pub(crate) fn new(part_size: usize) -> Self {
        Self {
            memory: Operator::via_iter(opendal::services::MEMORY_SCHEME, []).unwrap(),
            part_size,
            state: Arc::default(),
        }
    }

    pub(crate) fn file_io(&self) -> FileIO {
        FileIOBuilder::new("memory")
            .build()
            .unwrap()
            .with_provider(Arc::new(self.clone()))
    }
}

#[async_trait::async_trait]
impl FileIOProvider for MultipartProvider {
    async fn create(&self, path: &str) -> crate::Result<(Operator, String)> {
        Ok((
            Operator::from_parts(OperationContext::default(), Arc::new(self.clone())),
            Url::parse(path)
                .unwrap()
                .path()
                .trim_start_matches('/')
                .to_string(),
        ))
    }
}

impl Service for MultipartProvider {
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;

    fn info(&self) -> ServiceInfo {
        self.memory.service().info()
    }

    fn capability(&self) -> Capability {
        Capability {
            write_can_multi: true,
            write_multi_max_size: Some(self.part_size),
            ..self.memory.service().capability()
        }
    }

    fn write(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpWrite,
    ) -> opendal::Result<Self::Writer> {
        let mut state = self.state.lock().unwrap();
        state.concurrency.push(args.concurrent());
        if !path.ends_with(".index") {
            return self.memory.service().write(ctx, path, args);
        }
        state.index_writes += 1;
        let fault = if state.index_writes == state.fail_on_index {
            state.fault
        } else {
            Fault::None
        };
        Ok(Box::new(oio::MultipartWriter::new(
            ctx.executor().clone(),
            Upload {
                provider: self.clone(),
                path: path.to_string(),
                fault,
                reorder: args.concurrent() > 1,
                second_part: Notify::new(),
            },
            args.concurrent(),
        )))
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> opendal::Result<RpCreateDir> {
        self.memory.service().create_dir(ctx, path, args).await
    }
    async fn stat(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpStat,
    ) -> opendal::Result<RpStat> {
        self.memory.service().stat(ctx, path, args).await
    }
    fn read(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRead,
    ) -> opendal::Result<Self::Reader> {
        self.memory.service().read(ctx, path, args)
    }
    fn delete(&self, ctx: &OperationContext) -> opendal::Result<Self::Deleter> {
        self.memory.service().delete(ctx)
    }
    fn list(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpList,
    ) -> opendal::Result<Self::Lister> {
        self.memory.service().list(ctx, path, args)
    }
    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
        opts: OpCopier,
    ) -> opendal::Result<Self::Copier> {
        self.memory.service().copy(ctx, from, to, args, opts)
    }
    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> opendal::Result<RpRename> {
        self.memory.service().rename(ctx, from, to, args).await
    }
    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> opendal::Result<RpPresign> {
        self.memory.service().presign(ctx, path, args).await
    }
}

struct Upload {
    provider: MultipartProvider,
    path: String,
    fault: Fault,
    reorder: bool,
    second_part: Notify,
}

fn injected_error() -> opendal::Error {
    opendal::Error::new(opendal::ErrorKind::Unexpected, "injected multipart failure")
}

impl oio::MultipartWrite for Upload {
    async fn write_once(&self, size: u64, body: Buffer) -> opendal::Result<Metadata> {
        self.provider.memory.write(&self.path, body).await?;
        Ok(Metadata::default().with_content_length(size))
    }

    async fn initiate_part(&self) -> opendal::Result<String> {
        self.provider
            .state
            .lock()
            .unwrap()
            .uploads
            .insert(self.path.clone(), BTreeMap::new());
        Ok(self.path.clone())
    }

    async fn write_part(
        &self,
        upload_id: &str,
        part_number: usize,
        size: u64,
        body: Buffer,
    ) -> opendal::Result<oio::MultipartPart> {
        if self.reorder && part_number == 0 {
            self.second_part.notified().await;
        }
        {
            let mut state = self.provider.state.lock().unwrap();
            state.completed_parts.push(part_number);
            state
                .uploads
                .get_mut(upload_id)
                .unwrap()
                .insert(part_number, body.to_bytes());
        }
        if part_number == 1 {
            self.second_part.notify_one();
        }
        if self.fault == Fault::Part && part_number == 0 {
            return Err(injected_error());
        }
        Ok(oio::MultipartPart {
            part_number,
            etag: part_number.to_string(),
            checksum: None,
            size: Some(size),
        })
    }

    async fn complete_part(
        &self,
        upload_id: &str,
        parts: &[oio::MultipartPart],
    ) -> opendal::Result<Metadata> {
        if self.fault == Fault::Close {
            return Err(injected_error());
        }
        let mut bytes = Vec::new();
        {
            let mut state = self.provider.state.lock().unwrap();
            let uploaded = state.uploads.remove(upload_id).unwrap();
            for part in parts {
                bytes.extend_from_slice(&uploaded[&part.part_number]);
            }
        }
        let size = bytes.len() as u64;
        self.provider.memory.write(&self.path, bytes).await?;
        Ok(Metadata::default().with_content_length(size))
    }

    async fn abort_part(&self, upload_id: &str) -> opendal::Result<()> {
        let mut state = self.provider.state.lock().unwrap();
        state.uploads.remove(upload_id);
        Ok(())
    }
}

async fn round_trip(file_io: &FileIO, path: &str, concurrency: usize) {
    // Different bytes within and between parts expose truncation and reordering.
    let data: Vec<u8> = (0..9 * CHUNK + 12345)
        .map(|i| ((i / CHUNK * 37 + i % 251) % 256) as u8)
        .collect();
    let output = file_io.new_output(path).unwrap();
    let mut writer = if concurrency == 1 {
        output.async_writer().await.unwrap()
    } else {
        output
            .async_writer_with_concurrency(concurrency)
            .await
            .unwrap()
    };
    writer.write_all(&data).await.unwrap();
    writer.shutdown().await.unwrap();
    assert_eq!(
        file_io.get_status(path).await.unwrap().size,
        data.len() as u64
    );
    assert_eq!(
        file_io
            .new_input(path)
            .unwrap()
            .read()
            .await
            .unwrap()
            .as_ref(),
        data
    );
}

#[tokio::test]
async fn streaming_upload_memory_and_fs() {
    let directory = tempfile::tempdir().unwrap();
    for (scheme, path) in [
        ("memory", "memory:/stream.index".to_string()),
        (
            "file",
            directory.path().join("stream.index").display().to_string(),
        ),
    ] {
        let file_io = FileIOBuilder::new(scheme).build().unwrap();
        for concurrency in [1, 8] {
            round_trip(&file_io, &path, concurrency).await;
        }
    }
}

#[tokio::test]
async fn streaming_upload_preserves_out_of_order_parts() {
    for concurrency in [1, 8] {
        let provider = MultipartProvider::new(CHUNK);
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            round_trip(&provider.file_io(), "memory:/ordered.index", concurrency),
        )
        .await
        .unwrap();
        let state = provider.state.lock().unwrap();
        assert_eq!(state.concurrency, [concurrency]);
        assert_eq!(state.completed_parts.len(), 10);
        if concurrency == 8 {
            assert!(
                state.completed_parts.iter().position(|&p| p == 1).unwrap()
                    < state.completed_parts.iter().position(|&p| p == 0).unwrap()
            );
        }
        assert!(state.uploads.is_empty());
    }
}

#[tokio::test]
#[ignore = "requires PAIMON_CATALOG_OPTIONS and PAIMON_UPLOAD_TEST_DATABASE/TABLE for OSS"]
async fn streaming_upload_oss() {
    use crate::catalog::Identifier;
    use crate::common::Options;
    use crate::CatalogFactory;
    use futures::FutureExt;

    let options = serde_json::from_str(&std::env::var("PAIMON_CATALOG_OPTIONS").unwrap()).unwrap();
    let catalog = CatalogFactory::create(Options::from_map(options))
        .await
        .unwrap();
    let table = catalog
        .get_table(&Identifier::new(
            std::env::var("PAIMON_UPLOAD_TEST_DATABASE").unwrap(),
            std::env::var("PAIMON_UPLOAD_TEST_TABLE").unwrap(),
        ))
        .await
        .unwrap();
    assert!(table.location().starts_with("oss://"));
    let path = format!(
        "{}/index/upload-check-{}.index",
        table.location(),
        uuid::Uuid::new_v4()
    );
    eprintln!("OSS multipart byte-for-byte check: {path}");
    let result = std::panic::AssertUnwindSafe(round_trip(table.file_io(), &path, 8))
        .catch_unwind()
        .await;
    table.file_io().delete_file(&path).await.unwrap();
    assert!(!table.file_io().exists(&path).await.unwrap());
    eprintln!("OSS scratch object removed: {path}");
    result.unwrap();
}
