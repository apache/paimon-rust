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
use crate::io::{FileIOBuilder, UriReader};
use arrow_array::ArrayRef;
use std::sync::Mutex;

#[derive(Default)]
struct SourceState {
    events: Vec<String>,
    no_seek: bool,
    fail_read: bool,
    fail_close: bool,
    oversized_read: bool,
}

struct Reader(Arc<Mutex<SourceState>>);
struct Factory(Arc<Reader>);
struct Stream {
    state: Arc<Mutex<SourceState>>,
    bytes: Bytes,
    position: usize,
    uri: String,
}

impl UriReaderFactory for Factory {
    fn create(&self, _: &str) -> Result<Arc<dyn UriReader>> {
        Ok(self.0.clone())
    }
}

#[async_trait]
impl UriReader for Reader {
    async fn new_input_stream(&self, uri: &str) -> Result<Box<dyn UriInputStream>> {
        self.0.lock().unwrap().events.push(format!("open:{uri}"));
        Ok(Box::new(Stream {
            state: self.0.clone(),
            bytes: Bytes::from_static(b"0123456789"),
            position: 0,
            uri: uri.into(),
        }))
    }
}

#[async_trait]
impl UriInputStream for Stream {
    async fn read(&mut self, length: usize) -> Result<Bytes> {
        let mut state = self.state.lock().unwrap();
        state
            .events
            .push(format!("read:{}:{length}", self.position));
        if state.fail_read {
            return Err(invalid("source read failed"));
        }
        if state.oversized_read {
            return Ok(Bytes::from(vec![b'x'; length + 1]));
        }
        // Short, successful reads are valid InputStream behavior.
        let end = (self.position + length.min(2)).min(self.bytes.len());
        let bytes = self.bytes.slice(self.position..end);
        self.position = end;
        Ok(bytes)
    }
    async fn seek(&mut self, offset: u64) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state.events.push(format!("seek:{offset}"));
        if state.no_seek {
            return Err(invalid("seek unsupported"));
        }
        self.position = offset as usize;
        Ok(())
    }
    async fn close(&mut self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state.events.push(format!("close:{}", self.uri));
        if state.fail_close {
            return Err(invalid("source close failed"));
        }
        Ok(())
    }
}

fn reference(uri: &str, offset: i64, length: i64) -> Vec<u8> {
    BlobDescriptor::new(uri.into(), offset, length).serialize()
}

fn batch(values: &[Vec<u8>]) -> RecordBatch {
    RecordBatch::try_from_iter([(
        "payload",
        Arc::new(LargeBinaryArray::from(
            values
                .iter()
                .map(|value| Some(value.as_slice()))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
    )])
    .unwrap()
}

async fn writer(state: &Arc<Mutex<SourceState>>) -> (FileIO, Box<dyn FormatFileWriter>) {
    let io = FileIOBuilder::new("memory").build().unwrap();
    let output = io.new_output("memory:/output.blob").unwrap();
    let factory = BlobWriterFactory::new(None, None, None)
        .unwrap()
        .with_uri_reader_factory(Arc::new(Factory(Arc::new(Reader(state.clone())))));
    (io, factory.create_writer(&output, "").await.unwrap())
}

fn payloads(bytes: &Bytes, values: &[&[u8]]) {
    let mut position = 0;
    for value in values {
        assert_eq!(&bytes[position + 4..position + 4 + value.len()], *value);
        position += value.len() + 16;
    }
}

#[tokio::test]
async fn references_reuse_seekable_sources_without_reading_outside_windows() {
    let state = Arc::new(Mutex::new(SourceState::default()));
    let (io, mut writer) = writer(&state).await;
    writer
        .write(&batch(&[
            reference("custom:/a", 2, 3),
            reference("custom:/a", 5, 2),
            reference("custom:/a", 1, 2),
            b"inline".to_vec(),
            reference("custom:/a", 3, 0),
        ]))
        .await
        .unwrap();
    writer.close().await.unwrap();
    payloads(
        &io.new_input("memory:/output.blob")
            .unwrap()
            .read()
            .await
            .unwrap(),
        &[b"234", b"56", b"12", b"inline", b""],
    );
    assert_eq!(
        state.lock().unwrap().events,
        [
            "open:custom:/a",
            "seek:2",
            "read:2:3",
            "read:4:1",
            "read:5:2",
            "seek:1",
            "read:1:2",
            "close:custom:/a",
        ]
    );
}

#[tokio::test]
async fn non_seekable_sources_reuse_adjacent_windows_and_reopen_to_rewind() {
    let state = Arc::new(Mutex::new(SourceState {
        no_seek: true,
        ..Default::default()
    }));
    let (io, mut writer) = writer(&state).await;
    writer
        .write(&batch(&[
            reference("custom:/a", 0, 2),
            reference("custom:/a", 2, 2),
            reference("custom:/a", 0, 2),
        ]))
        .await
        .unwrap();
    writer.close().await.unwrap();
    payloads(
        &io.new_input("memory:/output.blob")
            .unwrap()
            .read()
            .await
            .unwrap(),
        &[b"01", b"23", b"01"],
    );
    assert_eq!(
        state.lock().unwrap().events,
        [
            "open:custom:/a",
            "read:0:2",
            "read:2:2",
            "seek:0",
            "close:custom:/a",
            "open:custom:/a",
            "read:0:2",
            "close:custom:/a",
        ]
    );
}

#[tokio::test]
async fn unknown_length_reads_to_eof_and_preserves_the_cached_bounded_source() {
    let state = Arc::new(Mutex::new(SourceState::default()));
    let (io, mut writer) = writer(&state).await;
    writer
        .write(&batch(&[
            reference("custom:/a", 0, 2),
            reference("custom:/a", 3, -1),
            reference("custom:/a", 2, 2),
        ]))
        .await
        .unwrap();
    writer.close().await.unwrap();
    payloads(
        &io.new_input("memory:/output.blob")
            .unwrap()
            .read()
            .await
            .unwrap(),
        &[b"01", b"3456789", b"23"],
    );
    let events = &state.lock().unwrap().events;
    assert_eq!(
        events
            .iter()
            .filter(|event| event.starts_with("open:"))
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.starts_with("close:"))
            .count(),
        2
    );
    assert_eq!(&events[events.len() - 2..], ["read:2:2", "close:custom:/a"]);
}

#[tokio::test]
async fn copy_failures_discard_sources_and_keep_the_copy_error_primary() {
    for (fail_read, oversized_read, length, message) in [
        (true, false, 3, "source read failed"),
        (false, false, 12, "Unexpected EOF"),
        (false, true, 3, "more bytes than requested"),
    ] {
        let state = Arc::new(Mutex::new(SourceState {
            fail_read,
            oversized_read,
            fail_close: true,
            ..Default::default()
        }));
        let (_, mut writer) = writer(&state).await;
        let error = writer
            .write(&batch(&[reference("custom:/a", 0, length)]))
            .await
            .unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
        writer.close().await.unwrap();
        assert_eq!(
            state
                .lock()
                .unwrap()
                .events
                .iter()
                .filter(|event| event.starts_with("close:"))
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn a_previous_source_close_failure_prevents_opening_a_different_uri() {
    let state = Arc::new(Mutex::new(SourceState {
        fail_close: true,
        ..Default::default()
    }));
    let (_, mut writer) = writer(&state).await;
    let error = writer
        .write(&batch(&[
            reference("custom:/a", 0, 2),
            reference("custom:/b", 0, 2),
        ]))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("source close failed"));
    writer.close().await.unwrap();
    assert_eq!(
        state.lock().unwrap().events,
        ["open:custom:/a", "read:0:2", "close:custom:/a"]
    );
}

#[tokio::test]
async fn failed_initial_seek_closes_the_opened_source() {
    let state = Arc::new(Mutex::new(SourceState {
        no_seek: true,
        fail_close: true,
        ..Default::default()
    }));
    let (_, mut writer) = writer(&state).await;
    let error = writer
        .write(&batch(&[reference("custom:/a", 1, 2)]))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("seek unsupported"));
    writer.close().await.unwrap();
    assert_eq!(
        state.lock().unwrap().events,
        ["open:custom:/a", "seek:1", "close:custom:/a"]
    );
}

#[tokio::test]
async fn source_close_failure_surfaces_at_format_prepare() {
    let state = Arc::new(Mutex::new(SourceState {
        fail_close: true,
        ..Default::default()
    }));
    let (io, mut writer) = writer(&state).await;
    writer
        .write(&batch(&[reference("custom:/a", 0, 2)]))
        .await
        .unwrap();
    assert!(writer
        .close()
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("source close failed"));
    // The destination still closes and drains its buffered payload.
    payloads(
        &io.new_input("memory:/output.blob")
            .unwrap()
            .read()
            .await
            .unwrap(),
        &[b"01"],
    );
}
