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
use bytes::Bytes;
use opendal::raw::{
    oio, OpCopy, OpCreateDir, OpList, OpPresign, OpRead, OpRename, OpStat, OpWrite, RpCreateDir,
    RpPresign, RpRename, RpStat, Service, ServiceInfo,
};
use opendal::{Buffer, Capability, Metadata, MetadataBuilder, OperationContext, Operator};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
use tokio::sync::Notify;
use url::Url;

const CHUNK: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) enum Fault {
    #[default]
    None,
    Part,
    Close,
}

#[derive(Debug)]
pub(crate) struct UploadState {
    pub(crate) fault: Fault,
    pub(crate) fail_on_index: usize,
    pub(crate) index_writes: usize,
    pub(crate) concurrency: Vec<usize>,
    pub(crate) uploads: BTreeMap<String, BTreeMap<usize, Bytes>>,
    pub(crate) completed_parts: Vec<usize>,
    pub(crate) part_attempts: Vec<usize>,
    /// When false, stored part bytes are copies so payload owners can drop.
    pub(crate) share_payloads: bool,
    /// Delay part 0 until part 1 is accepted. Tests that retry part 0 turn this off.
    pub(crate) reorder: bool,
    /// Fail part 0 this many times with a temporary error, then accept it.
    pub(crate) temporary_failures: usize,
}

impl Default for UploadState {
    fn default() -> Self {
        Self {
            fault: Fault::None,
            fail_on_index: 0,
            index_writes: 0,
            concurrency: Vec::new(),
            uploads: BTreeMap::new(),
            completed_parts: Vec::new(),
            part_attempts: Vec::new(),
            share_payloads: true,
            reorder: true,
            temporary_failures: 0,
        }
    }
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
    type Composer = ();

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
        let reorder = state.reorder && args.concurrent() > 1;
        Ok(Box::new(oio::MultipartWriter::new(
            ctx.executor().clone(),
            Upload {
                provider: self.clone(),
                path: path.to_string(),
                fault,
                reorder,
                second_part: Notify::new(),
            },
            args.concurrent(),
        )))
    }

    async fn create_dir(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpCreateDir,
    ) -> opendal::Result<RpCreateDir> {
        // The in-memory map is flat. Directory creation only needs to succeed so
        // index writers can prepare `index/` before the multipart upload.
        Ok(RpCreateDir::default())
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
    ) -> opendal::Result<Self::Copier> {
        self.memory.service().copy(ctx, from, to, args)
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

fn file_metadata(size: u64) -> Metadata {
    MetadataBuilder::file(size).build()
}

impl oio::MultipartWrite for Upload {
    async fn write_once(&self, size: u64, body: Buffer) -> opendal::Result<Metadata> {
        self.provider.memory.write(&self.path, body).await?;
        Ok(file_metadata(size))
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
        let mut temporary = false;
        {
            let mut state = self.provider.state.lock().unwrap();
            state.part_attempts.push(part_number);
            state.completed_parts.push(part_number);
            let stored = if state.share_payloads {
                body.to_bytes()
            } else {
                Bytes::copy_from_slice(&body.to_bytes())
            };
            state
                .uploads
                .get_mut(upload_id)
                .unwrap()
                .insert(part_number, stored);
            if part_number == 0 && state.temporary_failures > 0 {
                state.temporary_failures -= 1;
                temporary = true;
            }
        }
        if part_number == 1 {
            self.second_part.notify_one();
        }
        if temporary {
            return Err(injected_error().set_temporary());
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
        Ok(file_metadata(size))
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

struct TrackedChunk {
    data: Vec<u8>,
    live: Arc<AtomicUsize>,
}

impl AsRef<[u8]> for TrackedChunk {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl Drop for TrackedChunk {
    fn drop(&mut self) {
        self.live.fetch_sub(self.data.len(), Ordering::SeqCst);
    }
}

fn rss_bytes() -> u64 {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return 0;
    };
    for line in status.lines() {
        let Some(rest) = line.strip_prefix("VmRSS:") else {
            continue;
        };
        let kb = rest
            .split_whitespace()
            .next()
            .unwrap_or("0")
            .parse::<u64>()
            .unwrap_or(0);
        return kb.saturating_mul(1024);
    }
    0
}

async fn settle(live: &AtomicUsize) -> usize {
    let mut stable = 0;
    let mut last = live.load(Ordering::SeqCst);
    for _ in 0..100 {
        tokio::task::yield_now().await;
        let now = live.load(Ordering::SeqCst);
        if now == last {
            stable += 1;
            if stable >= 5 {
                return now;
            }
        } else {
            stable = 0;
            last = now;
        }
    }
    last
}

/// OpenDAL 0.59 releases a successful part's input when that part task finishes,
/// instead of retaining it until `close` collects the result. Twelve 8 MiB parts
/// at concurrency 8 must therefore keep only the unsent tail, not the whole upload.
#[tokio::test]
async fn streaming_upload_releases_completed_part_buffers() {
    const PARTS: usize = 12;
    const CONCURRENCY: usize = 8;
    let provider = MultipartProvider::new(CHUNK);
    provider.state.lock().unwrap().share_payloads = false;
    let (op, relative) = FileIOProvider::create(&provider, "memory:/release.index")
        .await
        .unwrap();
    let live = Arc::new(AtomicUsize::new(0));
    let rss_before = rss_bytes();
    let mut rss_peak = rss_before;
    let mut writer = op
        .writer_with(&relative)
        .chunk(CHUNK)
        .concurrent(CONCURRENCY)
        .await
        .unwrap();

    let mut peak = 0usize;
    for part in 0..PARTS {
        live.fetch_add(CHUNK, Ordering::SeqCst);
        let bytes = Bytes::from_owner(TrackedChunk {
            data: vec![part as u8; CHUNK],
            live: Arc::clone(&live),
        });
        writer.write(bytes).await.unwrap();
        if part == 0 {
            let held = settle(&live).await;
            assert_eq!(
                held, CHUNK,
                "the first chunk stays buffered until the next part arrives"
            );
        }
        peak = peak.max(settle(&live).await);
        rss_peak = rss_peak.max(rss_bytes());
    }
    let before_close = settle(&live).await;
    let completed = provider.state.lock().unwrap().completed_parts.len();
    writer.close().await.unwrap();
    let after_close = settle(&live).await;
    rss_peak = rss_peak.max(rss_bytes());

    let scheduled = PARTS.saturating_sub(2);
    assert_eq!(
        completed, scheduled,
        "all but the generator tail and multipart cache should already have been uploaded"
    );
    assert!(
        before_close <= 4 * CHUNK,
        "completed part payloads were still live before close: {before_close} bytes, peak {peak}"
    );
    assert!(
        peak <= (CONCURRENCY + 3) * CHUNK,
        "peak live part payloads {peak} exceeded concurrency {CONCURRENCY}"
    );
    assert!(
        peak < PARTS * CHUNK,
        "peak live part payloads {peak} retained the whole {} byte upload",
        PARTS * CHUNK
    );
    assert_eq!(after_close, 0, "close must drop every part payload");

    let got = provider.memory.read(&relative).await.unwrap().to_vec();
    assert_eq!(got.len(), PARTS * CHUNK);
    for part in 0..PARTS {
        let start = part * CHUNK;
        assert_eq!(got[start], part as u8);
        assert_eq!(got[start + CHUNK - 1], part as u8);
    }
    eprintln!(
        "part-buffer release: parts={PARTS} chunk={CHUNK} concurrency={CONCURRENCY} \
         scheduled_before_close={completed} live_before_close={before_close} \
         peak_live={peak} rss_before={rss_before} rss_peak={rss_peak}"
    );
}

/// A temporary part failure keeps that part's bytes for the scheduler retry.
/// The caller only retries `close`; it does not submit the payload again.
#[tokio::test]
async fn streaming_upload_retries_temporary_part_failure() {
    const PARTS: usize = 4;
    const PART: usize = 64 * 1024;
    let provider = MultipartProvider::new(PART);
    {
        let mut state = provider.state.lock().unwrap();
        state.reorder = false;
        state.temporary_failures = 1;
        state.share_payloads = false;
    }
    let (op, relative) = FileIOProvider::create(&provider, "memory:/retry.index")
        .await
        .unwrap();
    let mut writer = op
        .writer_with(&relative)
        .chunk(PART)
        .concurrent(8)
        .await
        .unwrap();
    let mut data = Vec::with_capacity(PARTS * PART);
    for part in 0..PARTS {
        let chunk = vec![part as u8 + 1; PART];
        data.extend_from_slice(&chunk);
        writer.write(chunk).await.unwrap();
    }

    let mut closes = 0;
    loop {
        closes += 1;
        match writer.close().await {
            Ok(_) => break,
            Err(error) if error.is_temporary() && closes < 5 => continue,
            Err(error) => panic!("multipart close failed: {error}"),
        }
    }
    assert!(
        closes >= 2,
        "the first close must observe the temporary part failure, closes={closes}"
    );
    let attempts = provider
        .state
        .lock()
        .unwrap()
        .part_attempts
        .iter()
        .filter(|part| **part == 0)
        .count();
    assert_eq!(
        attempts, 2,
        "part 0 must be sent again from the retained input"
    );
    let got = provider.memory.read(&relative).await.unwrap().to_vec();
    assert_eq!(got, data);
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
