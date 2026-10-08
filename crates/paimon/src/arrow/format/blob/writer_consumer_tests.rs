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
use arrow_array::ArrayRef;
use std::sync::Mutex;

#[derive(Default)]
struct OutputState {
    bytes: Vec<u8>,
    writes: Vec<usize>,
    flushes: Vec<usize>,
    fail_flush: bool,
}

struct RecordingOutput(Arc<Mutex<OutputState>>);

#[async_trait]
impl FileWrite for RecordingOutput {
    async fn write(&mut self, bytes: Bytes) -> Result<()> {
        let mut output = self.0.lock().unwrap();
        output.writes.push(bytes.len());
        output.bytes.extend_from_slice(&bytes);
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        let mut output = self.0.lock().unwrap();
        let position = output.bytes.len();
        output.flushes.push(position);
        if output.fail_flush {
            Err(invalid("flush failed"))
        } else {
            Ok(())
        }
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

fn writer(kind: BlobFieldKind, output: &Arc<Mutex<OutputState>>) -> BlobFormatWriter {
    BlobFormatWriter {
        update_rows: None,
        writer: Box::new(RecordingOutput(output.clone())),
        file_io: None,
        kind,
        path: "memory:/payload.blob".into(),
        field_name: "payload".into(),
        consumer: None,
        uri_reader_factory: None,
        reference_streams: ReusingBlobRefStreamProvider::default(),
        copy_buffer_size: 4 * 1024,
        bytes_written: 0,
        lengths: Vec::new(),
    }
}

type Event = (Option<BlobDescriptor>, usize);

struct RecordingSource {
    bytes: Bytes,
    ranges: Mutex<Vec<std::ops::Range<u64>>>,
}

#[async_trait]
impl FileRead for RecordingSource {
    async fn read(&self, range: std::ops::Range<u64>) -> Result<Bytes> {
        self.ranges.lock().unwrap().push(range.clone());
        Ok(self.bytes.slice(range.start as usize..range.end as usize))
    }
}

fn consumer(
    events: &Arc<Mutex<Vec<Event>>>,
    output: &Arc<Mutex<OutputState>>,
) -> Arc<dyn BlobConsumer> {
    let events = events.clone();
    let output = output.clone();
    Arc::new(move |field: &str, descriptor: Option<&BlobDescriptor>| {
        assert_eq!(field, "payload");
        let position = output.lock().unwrap().bytes.len();
        events.lock().unwrap().push((descriptor.cloned(), position));
        // Only the first callback requests a flush. Later callbacks must
        // still run, and a collection must remember the earlier request.
        Ok(events.lock().unwrap().len() == 1 || descriptor.is_none())
    })
}

fn check_payloads(output: &OutputState, events: &[Event], expected: &[Option<&[u8]>]) {
    assert_eq!(events.len(), expected.len());
    for ((descriptor, _), expected) in events.iter().zip(expected) {
        match (descriptor, expected) {
            (Some(descriptor), Some(payload)) => {
                assert_eq!(descriptor.uri(), "memory:/payload.blob");
                assert_eq!(descriptor.length(), payload.len() as i64);
                let start = descriptor.offset() as usize;
                assert_eq!(&output.bytes[start..start + payload.len()], *payload);
            }
            (None, None) => {}
            other => panic!("unexpected callback: {other:?}"),
        }
    }
}

#[tokio::test]
async fn scalar_consumer_runs_after_record_and_null_does_not_flush() {
    let output = Arc::new(Mutex::new(OutputState::default()));
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut writer =
        writer(BlobFieldKind::Scalar, &output).with_consumer(Some(consumer(&events, &output)));
    let batch = RecordBatch::try_from_iter([(
        "payload",
        Arc::new(LargeBinaryArray::from(vec![
            Some(b"abc".as_slice()),
            Some(b""),
            None,
        ])) as ArrayRef,
    )])
    .unwrap();
    writer.write(&batch).await.unwrap();
    let output = output.lock().unwrap();
    let events = events.lock().unwrap();
    check_payloads(&output, &events, &[Some(b"abc"), Some(b""), None]);
    assert_eq!(
        events
            .iter()
            .map(|(_, position)| *position)
            .collect::<Vec<_>>(),
        [19, 35, 35]
    );
    assert_eq!(output.flushes, [19]);
    assert_eq!(writer.lengths, [19, 16, -1]);
}

#[tokio::test]
async fn sparse_delta_uses_java_tags_and_only_exposes_updated_values() {
    let output = Arc::new(Mutex::new(OutputState::default()));
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut writer = writer(BlobFieldKind::Scalar, &output)
        .with_consumer(Some(consumer(&events, &output)))
        .with_update_rows(Some(BlobUpdateRows {
            updated: vec![1, 3].into(),
            next_position: Arc::new(AtomicUsize::new(0)),
        }));
    let batch = RecordBatch::try_from_iter([(
        "payload",
        Arc::new(LargeBinaryArray::from(vec![
            Some(b"not copied".as_slice()),
            None,
            Some(b"also not copied"),
            Some(b"new"),
        ])) as ArrayRef,
    )])
    .unwrap();
    writer.write(&batch).await.unwrap();
    assert_eq!(writer.lengths, [-2, -1, -2, 19]);
    check_payloads(
        &output.lock().unwrap(),
        &events.lock().unwrap(),
        &[None, Some(b"new")],
    );
}

#[tokio::test]
async fn sparse_cursor_survives_physical_writer_recreation() {
    let rows = BlobUpdateRows {
        updated: vec![2].into(),
        next_position: Arc::new(AtomicUsize::new(0)),
    };
    let output = Arc::new(Mutex::new(OutputState::default()));
    let mut first = writer(BlobFieldKind::Scalar, &output).with_update_rows(Some(rows.clone()));
    let batch = RecordBatch::try_from_iter([(
        "payload",
        Arc::new(LargeBinaryArray::from(vec![
            None,
            None,
            Some(b"new".as_slice()),
        ])) as ArrayRef,
    )])
    .unwrap();
    first.write(&batch.slice(0, 2)).await.unwrap();
    assert_eq!(first.lengths, [-2, -2]);
    let mut second = writer(BlobFieldKind::Scalar, &output).with_update_rows(Some(rows));
    second.write(&batch.slice(2, 1)).await.unwrap();
    assert_eq!(second.lengths, [19]);
}

#[tokio::test]
async fn collection_consumer_skips_null_elements_and_flushes_after_trailer() {
    for keys in [
        None,
        Some(vec![
            Some(b"key-a".to_vec()),
            Some(b"key-null".to_vec()),
            Some(b"key-b".to_vec()),
        ]),
    ] {
        let output = Arc::new(Mutex::new(OutputState::default()));
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut writer =
            writer(BlobFieldKind::Array, &output).with_consumer(Some(consumer(&events, &output)));
        let values = LargeBinaryArray::from(vec![Some(b"aaa".as_slice()), None, Some(b"bb")]);
        writer.write_collection(&values, keys).await.unwrap();
        let bytes_after_record = writer.bytes_written as usize;
        writer
            .write_collection(&LargeBinaryArray::from(Vec::<Option<&[u8]>>::new()), None)
            .await
            .unwrap();
        let output = output.lock().unwrap();
        let events = events.lock().unwrap();
        check_payloads(&output, &events, &[Some(b"aaa"), Some(b"bb")]);
        assert!(events
            .iter()
            .all(|(_, position)| *position < bytes_after_record - 12));
        assert_eq!(output.flushes, [bytes_after_record]);
    }
}

#[tokio::test]
async fn callback_and_flush_errors_stop_writing() {
    for fail_flush in [false, true] {
        let output = Arc::new(Mutex::new(OutputState {
            fail_flush,
            ..Default::default()
        }));
        let calls = Arc::new(Mutex::new(0));
        let callback_calls = calls.clone();
        let consumer = Arc::new(move |_: &str, _: Option<&BlobDescriptor>| {
            *callback_calls.lock().unwrap() += 1;
            if fail_flush {
                Ok(true)
            } else {
                Err(invalid("callback failed"))
            }
        });
        let mut writer = writer(BlobFieldKind::Scalar, &output).with_consumer(Some(consumer));
        let batch = RecordBatch::try_from_iter([(
            "payload",
            Arc::new(LargeBinaryArray::from_iter_values([b"a", b"b"])) as ArrayRef,
        )])
        .unwrap();
        let error = writer.write(&batch).await.unwrap_err();
        assert!(error.to_string().contains(if fail_flush {
            "flush failed"
        } else {
            "callback failed"
        }));
        assert_eq!(*calls.lock().unwrap(), 1);
        assert_eq!(writer.lengths, [17]);
    }
}

#[tokio::test]
async fn copy_buffer_bounds_inline_and_descriptor_payload_chunks() {
    let io = FileIOBuilder::new("memory").build().unwrap();
    let source = "memory:/blob-consumer-copy-source";
    let payload = b"0123456789abcdef";
    io.new_output(source)
        .unwrap()
        .write(Bytes::copy_from_slice(payload))
        .await
        .unwrap();
    for descriptor in [false, true] {
        let output = Arc::new(Mutex::new(OutputState::default()));
        let mut writer = writer(BlobFieldKind::Scalar, &output).with_copy_buffer_size(5);
        writer.file_io = Some(io.clone());
        let encoded = BlobDescriptor::new(source.into(), 2, 12).serialize();
        let value = if descriptor {
            encoded.as_slice()
        } else {
            &payload[2..14]
        };
        writer.write_scalar(value).await.unwrap();
        let output = output.lock().unwrap();
        assert_eq!(&output.bytes[4..16], &payload[2..14]);
        assert_eq!(output.writes, [4, 5, 5, 2, 8, 4]);
    }
}

#[test]
fn copy_buffer_size_matches_java_default_and_positive_int_bounds() {
    let defaults = HashMap::new();
    assert_eq!(
        CoreOptions::new(&defaults).blob_copy_buffer_size().unwrap(),
        4096
    );
    for (text, bytes) in [
        ("1 B", 1),
        ("8 kb", 8192),
        ("2147483647 B", i32::MAX as usize),
    ] {
        let options = HashMap::from([("blob.copy-buffer-size".into(), text.into())]);
        assert_eq!(
            CoreOptions::new(&options).blob_copy_buffer_size().unwrap(),
            bytes
        );
    }
    for text in ["0 B", "-1 B", "2147483648 B", "no-size"] {
        let options = HashMap::from([("blob.copy-buffer-size".into(), text.into())]);
        assert!(BlobWriterFactory::new(None, None, Some(&options)).is_err());
    }
}

#[tokio::test]
async fn small_copy_buffer_does_not_reopen_a_remote_source_per_chunk() {
    let size = SOURCE_READ_SIZE as usize + 17;
    let source = RecordingSource {
        bytes: Bytes::from(vec![b'x'; size]),
        ranges: Mutex::new(Vec::new()),
    };
    let output = Arc::new(Mutex::new(OutputState::default()));
    let mut writer = writer(BlobFieldKind::Scalar, &output).with_copy_buffer_size(4096);
    writer
        .copy_payload(&source, 0..size as u64, &mut Hasher::new(), "source")
        .await
        .unwrap();
    assert_eq!(
        *source.ranges.lock().unwrap(),
        [0..SOURCE_READ_SIZE, SOURCE_READ_SIZE..size as u64]
    );
    let output = output.lock().unwrap();
    assert_eq!(output.bytes.len(), size);
    assert!(output.writes.iter().all(|size| *size <= 4096));
    assert_eq!(output.bytes.as_slice(), source.bytes.as_ref());
}
