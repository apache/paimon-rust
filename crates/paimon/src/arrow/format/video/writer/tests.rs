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
use crate::arrow::format::video::IndexedVideoReader;
use crate::io::{FileIOBuilder, UriReader};
use arrow_array::ArrayRef;
use std::sync::Mutex;

fn batch(values: &[Option<Vec<u8>>]) -> RecordBatch {
    RecordBatch::try_from_iter([(
        "frame",
        Arc::new(LargeBinaryArray::from(
            values.iter().map(|v| v.as_deref()).collect::<Vec<_>>(),
        )) as ArrayRef,
    )])
    .unwrap()
}

fn frame(uri: &str, index: i64) -> Option<Vec<u8>> {
    Some(
        VideoFrameDescriptor::new(uri.into(), 0, -1, index)
            .unwrap()
            .serialize(),
    )
}

fn fixture(text: &str) -> Vec<u8> {
    hex::decode(
        text.lines()
            .find(|line| !line.is_empty() && !line.starts_with('#'))
            .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn video_writer_matches_java_python_golden_bytes_across_batches() {
    let io = FileIOBuilder::new("memory").build().unwrap();
    for (uri, bytes) in [
        ("memory:/a", b"abc".as_slice()),
        ("memory:/b", b"WXYZ".as_slice()),
    ] {
        io.new_output(uri)
            .unwrap()
            .write(Bytes::copy_from_slice(bytes))
            .await
            .unwrap();
    }
    let mut writer = Box::new(
        VideoFormatWriter::new(
            io.new_output("memory:/out.video")
                .unwrap()
                .writer()
                .await
                .unwrap(),
            Some(io.clone()),
            None,
            2,
        )
        .unwrap(),
    );
    writer
        .write(&batch(&[frame("memory:/a", 2)]))
        .await
        .unwrap();
    writer
        .write(&batch(&[frame("memory:/a", 3), None]))
        .await
        .unwrap();
    writer.append(-2, 0).unwrap();
    writer
        .write(&batch(&[
            frame("memory:/b", 7),
            frame("memory:/b", 8),
            frame("memory:/a", 10),
        ]))
        .await
        .unwrap();
    assert_eq!(
        writer.num_bytes(),
        7,
        "repeated sources must not copy payload bytes again"
    );
    let result = writer.close().await.unwrap();
    let actual = io
        .new_input("memory:/out.video")
        .unwrap()
        .read()
        .await
        .unwrap();
    assert_eq!(
        actual.as_ref(),
        fixture(include_str!("../../../../../testdata/video/video-v1.hex"))
    );
    assert_eq!(result.file_size, actual.len() as u64);
}

#[test]
fn moov_scanner_handles_chunk_boundaries_extended_sizes_and_invalid_boxes() {
    let mut ordinary = Vec::from(&b"\0\0\0\x10ftyp12345678\0\0\0\x08moov"[..]);
    let extended = b"\0\0\0\x01moov\0\0\0\0\0\0\0\x10";
    for (bytes, expected) in [
        (ordinary.as_slice(), (16, 8)),
        (extended.as_slice(), (0, 16)),
        (b"\0\0\0\0moovdata".as_slice(), (0, 12)),
        (b"not an mp4".as_slice(), (0, 0)),
        (b"\0\0\0\x09moov".as_slice(), (0, 0)),
    ] {
        for size in 1..=bytes.len() {
            let mut locator = MoovLocator::default();
            for chunk in bytes.chunks(size) {
                locator.feed(chunk);
            }
            assert_eq!(locator.finish(), expected, "chunk size {size}");
        }
    }
    ordinary.extend_from_slice(b"\0\0\0\x08moov");
    let mut locator = MoovLocator::default();
    locator.feed(&ordinary);
    assert_eq!(
        locator.finish(),
        (0, 0),
        "duplicate moov is not a trustworthy hint"
    );
}

#[tokio::test]
async fn video_writer_matches_moov_fixture_and_rejects_non_frame_values() {
    let io = FileIOBuilder::new("memory").build().unwrap();
    let expected = fixture(include_str!(
        "../../../../../testdata/video/video-v1-moov.hex"
    ));
    io.new_output("memory:/mp4")
        .unwrap()
        .write(Bytes::copy_from_slice(&expected[..24]))
        .await
        .unwrap();
    let output = io.new_output("memory:/out.video").unwrap();
    let mut writer = VideoWriterFactory::new(Some(io.clone()), None, None)
        .unwrap()
        .create_writer(&output, "")
        .await
        .unwrap();
    writer
        .write(&batch(&[frame("memory:/mp4", 7), frame("memory:/mp4", 8)]))
        .await
        .unwrap();
    writer.close().await.unwrap();
    let bytes = io
        .new_input("memory:/out.video")
        .unwrap()
        .read()
        .await
        .unwrap();
    assert_eq!(bytes.as_ref(), expected);
    for value in [
        b"raw mp4".to_vec(),
        BlobDescriptor::new("memory:/mp4".into(), 0, 24).serialize(),
        VideoFrameDescriptor::new("memory:/mp4".into(), -1, 24, 0)
            .unwrap()
            .serialize(),
        VideoFrameDescriptor::new("memory:/mp4".into(), 0, 25, 0)
            .unwrap()
            .serialize(),
    ] {
        let mut writer = VideoWriterFactory::new(Some(io.clone()), None, None)
            .unwrap()
            .create_writer(&output, "")
            .await
            .unwrap();
        assert!(writer.write(&batch(&[Some(value)])).await.is_err());
    }
    let mut writer = VideoWriterFactory::new(Some(io.clone()), None, None)
        .unwrap()
        .create_writer(&output, "")
        .await
        .unwrap();
    writer.write(&batch(&[None, None])).await.unwrap();
    writer.close().await.unwrap();
    let input = io.new_input("memory:/out.video").unwrap();
    let reader = input.reader().await.unwrap();
    assert_eq!(
        IndexedVideoReader::open(
            &reader,
            input.metadata().await.unwrap().size,
            "memory:/out.video".into()
        )
        .await
        .unwrap()
        .num_rows(),
        2
    );
}

struct Factory(Arc<Mutex<Vec<String>>>);
struct Reader(Arc<Mutex<Vec<String>>>);
struct Stream(Arc<Mutex<Vec<String>>>);
impl UriReaderFactory for Factory {
    fn create(&self, _: &str) -> Result<Arc<dyn UriReader>> {
        Ok(Arc::new(Reader(self.0.clone())))
    }
}
#[async_trait]
impl UriReader for Reader {
    async fn new_input_stream(&self, _: &str) -> Result<Box<dyn UriInputStream>> {
        self.0.lock().unwrap().push("open".into());
        Ok(Box::new(Stream(self.0.clone())))
    }
}
#[async_trait]
impl UriInputStream for Stream {
    async fn read(&mut self, _: usize) -> Result<Bytes> {
        Err(invalid("source read failed"))
    }
    async fn close(&mut self) -> Result<()> {
        self.0.lock().unwrap().push("close".into());
        Err(invalid("source close failed"))
    }
}

#[tokio::test]
async fn copy_failure_closes_source_and_preserves_original_error() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let io = FileIOBuilder::new("memory").build().unwrap();
    let mut writer = VideoWriterFactory::new(None, None, None)
        .unwrap()
        .with_uri_reader_factory(Some(Arc::new(Factory(events.clone()))))
        .create_writer(&io.new_output("memory:/failed.video").unwrap(), "")
        .await
        .unwrap();
    let error = writer
        .write(&batch(&[frame("custom:/a", 0)]))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("source read failed"));
    assert_eq!(*events.lock().unwrap(), ["open", "close"]);
}

#[tokio::test]
async fn frame_ordinals_and_row_counts_do_not_wrap() {
    let io = FileIOBuilder::new("memory").build().unwrap();
    let mut writer = VideoFormatWriter::new(
        io.new_output("memory:/boundary.video")
            .unwrap()
            .writer()
            .await
            .unwrap(),
        None,
        None,
        1,
    )
    .unwrap();
    writer.append(0, i64::MAX).unwrap();
    writer.append(0, 0).unwrap();
    writer.append(0, 1).unwrap();
    assert_eq!(writer.runs.len(), 2);
    assert_eq!(writer.runs[1].length, 2);
    writer.rows = i64::from(i32::MAX);
    assert!(writer.append(-1, 0).is_err());
    assert!(writer.write(&batch(&[None])).await.is_err());
}

struct FailedOutput(Arc<Mutex<Vec<String>>>);
#[async_trait]
impl FileWrite for FailedOutput {
    async fn write(&mut self, _: Bytes) -> Result<()> {
        Err(invalid("footer write failed"))
    }
    async fn close(&mut self) -> Result<()> {
        self.0.lock().unwrap().push("output close".into());
        Err(invalid("output close failed"))
    }
}

#[tokio::test]
async fn footer_failure_still_closes_output_and_keeps_the_first_error() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let writer = Box::new(
        VideoFormatWriter::new(Box::new(FailedOutput(events.clone())), None, None, 1).unwrap(),
    );
    let error = writer.close().await.err().unwrap();
    assert!(error.to_string().contains("footer write failed"));
    assert_eq!(*events.lock().unwrap(), ["output close"]);
}
