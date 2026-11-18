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
use crate::spec::{BigIntType, Datum, DoubleType, Predicate, PredicateOperator, VarCharType};
use apache_avro::{from_avro_datum, Codec, Schema, Writer};
use futures::TryStreamExt;
use std::io::Cursor;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

#[derive(Default)]
struct ReadStats {
    requests: Mutex<Vec<Range<u64>>>,
    last_window: Mutex<Bytes>,
    dropped: AtomicBool,
}

impl ReadStats {
    fn bytes_read(&self) -> u64 {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .map(|r| r.end - r.start)
            .sum()
    }
}

struct CountingRead {
    bytes: Vec<u8>,
    stats: Arc<ReadStats>,
    fail_at: Option<u64>,
}

impl Drop for CountingRead {
    fn drop(&mut self) {
        self.stats.dropped.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl FileRead for CountingRead {
    async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
        self.stats.requests.lock().unwrap().push(range.clone());
        if self.fail_at.is_some_and(|offset| range.end > offset) {
            return Err(avro_invalid("injected Avro read failure"));
        }
        let start = (range.start as usize).min(self.bytes.len());
        let end = (range.end as usize).min(self.bytes.len());
        let window = Bytes::copy_from_slice(&self.bytes[start..end]);
        *self.stats.last_window.lock().unwrap() = window.clone();
        Ok(window)
    }
}

fn counting_read(bytes: Vec<u8>, fail_at: Option<u64>) -> (Box<dyn FileRead>, Arc<ReadStats>) {
    let stats = Arc::new(ReadStats::default());
    (
        Box::new(CountingRead {
            bytes,
            stats: stats.clone(),
            fail_at,
        }),
        stats,
    )
}

fn fields() -> Vec<DataField> {
    vec![
        DataField::new(0, "id".into(), DataType::BigInt(BigIntType::new())),
        DataField::new(
            1,
            "payload".into(),
            DataType::VarChar(VarCharType::string_type()),
        ),
    ]
}

fn schema() -> Schema {
    Schema::parse_str(
        r#"{"type":"record","name":"row","fields":[
        {"name":"id","type":"long"}, {"name":"payload","type":"string"}
    ]}"#,
    )
    .unwrap()
}

fn record(id: i64, payload_size: usize) -> Value {
    Value::Record(vec![
        ("id".into(), Value::Long(id)),
        (
            "payload".into(),
            Value::String(format!("{id:08}{}", "x".repeat(payload_size))),
        ),
    ])
}

fn supported_codecs() -> [Codec; 6] {
    [
        Codec::Null,
        Codec::Deflate(Default::default()),
        Codec::Snappy,
        Codec::Zstandard(Default::default()),
        Codec::Bzip2(Default::default()),
        Codec::Xz(Default::default()),
    ]
}

fn file(codec: Codec, blocks: usize, rows_per_block: usize, payload_size: usize) -> Vec<u8> {
    let schema = schema();
    let mut writer = Writer::builder()
        .schema(&schema)
        .writer(Vec::new())
        .codec(codec)
        .block_size(2 * 1024 * 1024)
        .marker([7; 16])
        .build();
    for block in 0..blocks {
        for row in 0..rows_per_block {
            writer
                .append(record((block * rows_per_block + row) as i64, payload_size))
                .unwrap();
        }
        writer.flush().unwrap();
    }
    writer.into_inner().unwrap()
}

struct BlockLocation {
    start: usize,
    payload: Range<usize>,
    end: usize,
}

fn layout(bytes: &[u8]) -> (usize, Vec<BlockLocation>) {
    let mut cursor = Cursor::new(bytes);
    drop(Reader::new(&mut cursor).unwrap());
    let header_end = cursor.position() as usize;
    let mut blocks = Vec::new();
    while (cursor.position() as usize) < bytes.len() {
        let start = cursor.position() as usize;
        from_avro_datum(&Schema::Long, &mut cursor, None).unwrap();
        let Value::Long(size) = from_avro_datum(&Schema::Long, &mut cursor, None).unwrap() else {
            unreachable!();
        };
        let payload_start = cursor.position() as usize;
        let payload_end = payload_start + size as usize;
        blocks.push(BlockLocation {
            start,
            payload: payload_start..payload_end,
            end: payload_end + 16,
        });
        cursor.set_position((payload_end + 16) as u64);
    }
    (header_end, blocks)
}

async fn stream(
    bytes: Vec<u8>,
    batch_size: usize,
    selection: Option<Vec<RowRange>>,
) -> (ArrowRecordBatchStream, Arc<ReadStats>) {
    let size = bytes.len() as u64;
    let (input, stats) = counting_read(bytes, None);
    let stream = AvroFormatReader
        .read_batch_stream(input, size, &fields(), None, Some(batch_size), selection)
        .await
        .unwrap();
    (stream, stats)
}

fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .map(|id| id.expect("non-null id"))
        })
        .collect()
}

async fn terminal_error(mut stream: ArrowRecordBatchStream, stats: &ReadStats) -> Error {
    let error = stream
        .by_ref()
        .try_for_each(|_| async { Ok(()) })
        .await
        .unwrap_err();
    assert!(stats.dropped.load(Ordering::SeqCst));
    {
        let window = stats.last_window.lock().unwrap();
        assert!(
            window.is_empty() || window.is_unique(),
            "the errored stream retained its storage window"
        );
    }
    assert!(stream.try_next().await.unwrap().is_none());
    error
}

#[tokio::test]
async fn dropping_before_or_after_the_first_batch_stops_io() {
    let bytes = file(Codec::Null, 8, 16, 8192);
    let (_, blocks) = layout(&bytes);
    let size = bytes.len() as u64;
    let (unpolled, stats) = stream(bytes.clone(), 3, None).await;
    drop(unpolled);
    assert!(stats.dropped.load(Ordering::SeqCst));
    assert_eq!(stats.bytes_read(), 0);

    for batch_size in [3, DEFAULT_BATCH_SIZE] {
        let (mut stream, stats) = stream(bytes.clone(), batch_size, None).await;
        assert_eq!(stats.bytes_read(), 0, "stream construction must be lazy");
        let first = stream.try_next().await.unwrap().unwrap();
        assert_eq!(
            ids(&[first]),
            (0..batch_size.min(16) as i64).collect::<Vec<_>>()
        );
        let read = stats.bytes_read();
        assert!(read < size);
        assert!(read <= blocks[0].end as u64 + INITIAL_READ_AHEAD);
        assert_eq!(stats.requests.lock().unwrap()[0], 0..INITIAL_READ_AHEAD);
        tokio::task::yield_now().await;
        assert_eq!(stats.bytes_read(), read);
        drop(stream);
        assert!(stats.dropped.load(Ordering::SeqCst));
        assert!(stats.last_window.lock().unwrap().is_unique());
        tokio::task::yield_now().await;
        assert_eq!(stats.bytes_read(), read);
    }
}

#[test]
fn sparse_selection_remains_cancellable_with_buffered_input_without_tokio() {
    futures::executor::block_on(async {
        let bytes = file(Codec::Null, 1, 256, 8);
        let size = bytes.len() as u64;
        let selection = Some(vec![RowRange::new(0, 0), RowRange::new(255, 255)]);
        let (mut stream, stats) = stream(bytes, 4, selection).await;
        assert_eq!(ids(&[stream.try_next().await.unwrap().unwrap()]), [0]);
        assert_eq!(stats.bytes_read(), size);

        let mut next = Box::pin(stream.try_next());
        assert!(futures::poll!(&mut next).is_pending());
        assert!(futures::poll!(&mut next).is_pending());
        drop(next);
        drop(stream);
        assert!(stats.dropped.load(Ordering::SeqCst));
        assert!(stats.last_window.lock().unwrap().is_unique());
        assert_eq!(stats.requests.lock().unwrap().len(), 1);
    });
}

#[tokio::test]
async fn full_and_near_full_scans_coalesce_storage_requests() {
    for (blocks, rows_per_block, excluded_row) in
        [(12, 128, None), (128, 8, None), (128, 8, Some(511))]
    {
        let total_rows = (blocks * rows_per_block) as i64;
        let selection = excluded_row.map(|row| {
            vec![
                RowRange::new(0, row - 1),
                RowRange::new(row + 1, total_rows - 1),
            ]
        });
        let expected: Vec<_> = (0..total_rows)
            .filter(|row| Some(*row) != excluded_row)
            .collect();
        let bytes = file(Codec::Null, blocks, rows_per_block, 8192);
        let size = bytes.len() as u64;
        let (mut stream, stats) = stream(bytes, 512, selection).await;
        let first = stream.try_next().await.unwrap().unwrap();
        let prefix_bytes = stats.bytes_read();
        let prefix_requests = stats.requests.lock().unwrap().len();
        assert!(prefix_bytes < size);
        assert_eq!(stats.requests.lock().unwrap()[0], 0..512 * 1024);
        if rows_per_block == 128 {
            assert_eq!(prefix_requests, 2, "header and first 1 MiB block");
            assert!(
                stats.last_window.lock().unwrap().is_unique(),
                "the stream retained the exhausted storage window"
            );
        } else {
            assert_eq!(prefix_requests, 1, "first block fits in the initial window");
        }

        let mut actual = ids(&[first]);
        while actual.len() < expected.len() {
            actual.extend(ids(&[stream.try_next().await.unwrap().unwrap()]));
        }
        assert_eq!(actual, expected);
        assert!(stats.dropped.load(Ordering::SeqCst));
        assert!(stream.try_next().await.unwrap().is_none());
        assert_eq!(stats.bytes_read(), size);
        let read_ahead = if excluded_row.is_some() {
            512 * 1024
        } else {
            MAX_READ_SIZE
        };
        let requests = stats.requests.lock().unwrap();
        assert_eq!(
            (requests.len() - prefix_requests) as u64,
            (size - prefix_bytes).div_ceil(read_ahead)
        );
        assert!(requests[prefix_requests..]
            .iter()
            .all(|range| range.end - range.start <= read_ahead));
    }
}

#[cfg(feature = "storage-memory")]
#[test]
fn reads_java_avro_from_memory_without_tokio() {
    futures::executor::block_on(async {
        let bytes = include_bytes!("../../../../testdata/avro_codec/java_bzip2.avro");
        let size = bytes.len() as u64;
        let io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/without-tokio.avro";
        let mut output = io.new_output(path).unwrap().writer().await.unwrap();
        output.write(Bytes::from_static(bytes)).await.unwrap();
        output.close().await.unwrap();
        let reader = io.new_input(path).unwrap().reader().await.unwrap();
        let batches = AvroFormatReader
            .read_batch_stream(Box::new(reader), size, &fields()[..1], None, Some(2), None)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(ids(&batches), [-7, 0, i64::MAX]);
    });
}

#[test]
fn dropping_decode_future_releases_and_aborts_queued_work() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    let task_ran = Arc::new(AtomicBool::new(false));
    runtime.block_on(async {
        let (started, running) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let occupied = tokio::task::spawn_blocking(move || {
            started.send(()).unwrap();
            let _ = wait.recv();
        });
        running.await.unwrap();
        let worker_flag = task_ran.clone();
        let mut decode = Box::pin(run_decode_task(move || {
            worker_flag.store(true, Ordering::SeqCst);
            Ok(())
        }));
        assert!(futures::poll!(&mut decode).is_pending());
        drop(decode);
        assert_eq!(
            Arc::strong_count(&task_ran),
            1,
            "queued decode retains its input"
        );
        release.send(()).unwrap();
        occupied.await.unwrap();
        tokio::task::spawn_blocking(|| {}).await.unwrap();
    });
    assert!(!task_ran.load(Ordering::SeqCst));
}

#[tokio::test]
async fn dropping_during_pending_or_ready_reads_stops_further_io() {
    struct NotifyingRead {
        stats: Arc<ReadStats>,
        entered: Arc<tokio::sync::Notify>,
        bytes: Option<Bytes>,
    }
    impl Drop for NotifyingRead {
        fn drop(&mut self) {
            self.stats.dropped.store(true, Ordering::SeqCst);
        }
    }
    #[async_trait]
    impl FileRead for NotifyingRead {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            let count = {
                let mut requests = self.stats.requests.lock().unwrap();
                requests.push(range.clone());
                requests.len()
            };
            if self.bytes.is_none() || count == 2 {
                self.entered.notify_one();
            }
            match &self.bytes {
                Some(bytes) => Ok(bytes.slice(range.start as usize..range.end as usize)),
                None => futures::future::pending().await,
            }
        }
    }
    let bytes = Bytes::from(file(
        Codec::Null,
        1,
        1,
        (MAX_READ_SIZE + 2 * INITIAL_READ_AHEAD) as usize,
    ));
    for ready in [false, true] {
        let stats = Arc::new(ReadStats::default());
        let entered = Arc::new(tokio::sync::Notify::new());
        let mut stream = AvroFormatReader
            .read_batch_stream(
                Box::new(NotifyingRead {
                    stats: stats.clone(),
                    entered: entered.clone(),
                    bytes: ready.then(|| bytes.clone()),
                }),
                bytes.len() as u64,
                &fields(),
                None,
                Some(2),
                None,
            )
            .await
            .unwrap();
        tokio::select! {
            biased;
            result = stream.try_next() => panic!("unexpected read completion: {result:?}"),
            _ = entered.notified() => {}
        }
        drop(stream);
        assert!(stats.dropped.load(Ordering::SeqCst));
        let requests = stats.requests.lock().unwrap();
        if ready {
            assert_eq!(
                requests[..],
                [
                    0..INITIAL_READ_AHEAD,
                    INITIAL_READ_AHEAD..INITIAL_READ_AHEAD + MAX_READ_SIZE
                ]
            );
        } else {
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0], 0..INITIAL_READ_AHEAD);
        }
    }
}

#[tokio::test]
async fn full_scans_and_cross_block_ranges_preserve_physical_order_for_every_codec() {
    for codec in supported_codecs() {
        for payload_size in [32, INITIAL_READ_AHEAD as usize + 17] {
            let bytes = file(codec, 5, 7, payload_size);
            let expected_records = Reader::new(bytes.as_slice())
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap();
            let fields = fields();
            let schema = build_target_arrow_schema(&fields).unwrap();
            let expected = records_to_batch(&expected_records, &fields, &schema).unwrap();
            let (full, _) = stream(bytes.clone(), 3, None).await;
            let batches = full.try_collect::<Vec<_>>().await.unwrap();
            assert!(batches.iter().all(|batch| batch.num_rows() <= 3));
            assert_eq!(
                arrow_select::concat::concat_batches(&schema, &batches).unwrap(),
                expected
            );

            let ranges = vec![
                RowRange::new(19, 23),
                RowRange::new(5, 9),
                RowRange::new(7, 11),
                RowRange::new(-4, -1),
                RowRange::new(-1, 0),
                RowRange::new(33, i64::MAX),
            ];
            let (selected, _) = stream(bytes, 3, Some(ranges)).await;
            let batches = selected.try_collect::<Vec<_>>().await.unwrap();
            assert_eq!(
                ids(&batches),
                [
                    vec![0],
                    (5..=11).collect(),
                    (19..=23).collect(),
                    vec![33, 34]
                ]
                .concat()
            );
        }
    }
}

#[tokio::test]
async fn projection_preserves_missing_fields_numeric_widening_and_null_second_unions() {
    let schema = Schema::parse_str(
        r#"{"type":"record","name":"projected","fields":[
        {"name":"skipped","type":"string"},
        {"name":"value","type":["int","null"]},
        {"name":"number","type":"float"}
    ]}"#,
    )
    .unwrap();
    let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Zstandard(Default::default()));
    for row in 0..6 {
        let value = if row % 2 == 0 {
            Value::Union(0, Box::new(Value::Int(row)))
        } else {
            Value::Union(1, Box::new(Value::Null))
        };
        writer
            .append(Value::Record(vec![
                ("skipped".into(), Value::String(format!("row-{row}"))),
                ("value".into(), value),
                ("number".into(), Value::Float(row as f32 + 0.5)),
            ]))
            .unwrap();
        if row % 2 == 1 {
            writer.flush().unwrap();
        }
    }
    let bytes = writer.into_inner().unwrap();
    let fields = vec![
        DataField::new(2, "number".into(), DataType::Double(DoubleType::new())),
        DataField::new(3, "missing".into(), DataType::Int(IntType::new())),
        DataField::new(1, "value".into(), DataType::BigInt(BigIntType::new())),
    ];
    let arrow_schema = build_target_arrow_schema(&fields).unwrap();
    let expected = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Float64Array::from(vec![0.5, 1.5, 2.5, 3.5, 4.5, 5.5])),
            Arc::new(Int32Array::new_null(6)),
            Arc::new(Int64Array::from(vec![
                Some(0),
                None,
                Some(2),
                None,
                Some(4),
                None,
            ])),
        ],
    )
    .unwrap();
    let size = bytes.len() as u64;
    let (reader, _) = counting_read(bytes, None);
    let batches = AvroFormatReader
        .read_batch_stream(reader, size, &fields, None, Some(1), None)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        arrow_select::concat::concat_batches(&arrow_schema, &batches).unwrap(),
        expected
    );
}

#[tokio::test]
async fn selection_skips_whole_block_payloads_and_stops_at_the_last_range() {
    let mut bytes = file(Codec::Null, 8, 16, INITIAL_READ_AHEAD as usize / 8);
    let (_, blocks) = layout(&bytes);
    bytes[blocks[1].payload.clone()].fill(0xff);
    let ranges = vec![RowRange::new(0, 0), RowRange::new(48, 50)];
    let (selected, stats) = stream(bytes.clone(), 4, Some(ranges.clone())).await;
    let batches = selected.try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(ids(&batches), [0, 48, 49, 50]);
    {
        let requests = stats.requests.lock().unwrap();
        for block in &blocks[1..3] {
            let middle = block.payload.start as u64 + INITIAL_READ_AHEAD;
            assert!(!requests.iter().any(|range| range.contains(&middle)));
        }
        assert!(requests
            .iter()
            .all(|range| range.end <= blocks[3].end as u64 + INITIAL_READ_AHEAD));
    }
    bytes[blocks[2].payload.end] ^= 1;
    let (selected, stats) = stream(bytes, 4, Some(ranges)).await;
    let error = terminal_error(selected, &stats).await;
    assert!(error.to_string().contains("sync marker mismatch"));
}

#[tokio::test]
async fn empty_selections_and_header_only_files_release_the_reader() {
    let bytes = file(Codec::Null, 1, 4, 8);
    let (header_end, _) = layout(&bytes);
    for ranges in [vec![], vec![RowRange::new(-5, -1)]] {
        let (mut empty, stats) = stream(bytes.clone(), 2, Some(ranges)).await;
        assert!(stats.dropped.load(Ordering::SeqCst));
        assert!(empty.try_next().await.unwrap().is_none());
        assert_eq!(stats.bytes_read(), 0);
    }
    for (bytes, ranges) in [
        (bytes[..header_end].to_vec(), None),
        (bytes, Some(vec![RowRange::new(10, 20)])),
    ] {
        let (mut empty, stats) = stream(bytes, 2, ranges).await;
        assert!(empty.try_next().await.unwrap().is_none());
        assert!(stats.dropped.load(Ordering::SeqCst));
    }
}

#[tokio::test]
async fn empty_projection_counts_regular_and_zero_width_records() {
    fn zero_width_blocks(counts: &[i64]) -> Vec<u8> {
        let schema = Schema::parse_str(r#"{"type":"record","name":"empty","fields":[]}"#).unwrap();
        let mut bytes = Writer::new(&schema, Vec::new()).into_inner().unwrap();
        let marker: [u8; 16] = *bytes.last_chunk().unwrap();
        for &count in counts {
            bytes.extend(apache_avro::to_avro_datum(&Schema::Long, Value::Long(count)).unwrap());
            bytes.push(0);
            bytes.extend(marker);
        }
        bytes
    }

    let large_block = zero_width_blocks(&[i64::MAX, 1]);
    for (bytes, batch_size, position, expected_rows) in [
        (file(Codec::Null, 1, 4, 8), 0, None, 4),
        (zero_width_blocks(&[4]), 0, None, 4),
        (zero_width_blocks(&[0, 2, 0, 2]), 0, None, 4),
        (large_block.clone(), usize::MAX, Some(0), 1),
        (large_block, usize::MAX, Some(i64::MAX), 1),
    ] {
        let size = bytes.len() as u64;
        let (reader, stats) = counting_read(bytes, None);
        let selection = position.map(|row| vec![RowRange::new(row, row)]);
        let mut stream = AvroFormatReader
            .read_batch_stream(reader, size, &[], None, Some(batch_size), selection)
            .await
            .unwrap();
        for _ in 0..expected_rows {
            let batch = stream.try_next().await.unwrap().unwrap();
            assert_eq!((batch.num_columns(), batch.num_rows()), (0, 1));
        }
        assert!(stats.dropped.load(Ordering::SeqCst));
        assert!(stream.try_next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn predicates_preserve_projection_and_selected_physical_positions() {
    let bytes = file(Codec::Null, 5, 7, 8);
    let size = bytes.len() as u64;
    let fields = fields();
    for (index, op, literal, matching_rows) in [
        (
            0,
            PredicateOperator::Gt,
            Datum::Long(8),
            (9..35).collect::<Vec<i64>>(),
        ),
        (0, PredicateOperator::Gt, Datum::Long(i64::MAX), vec![]),
        (
            1,
            PredicateOperator::Like,
            Datum::String("0000002%".into()),
            (20..30).collect(),
        ),
        (
            1,
            PredicateOperator::Like,
            Datum::String("missing%".into()),
            vec![],
        ),
    ] {
        let predicates = FilePredicates {
            predicates: vec![Predicate::Leaf {
                column: fields[index].name().to_string(),
                index,
                data_type: fields[index].data_type().clone(),
                op,
                literals: vec![literal],
            }],
            row_filter_factory: None,
            file_fields: fields.clone(),
        };
        let without_predicate_column = if index == 0 {
            &fields[1..]
        } else {
            &fields[..1]
        };
        for read_fields in [&fields[..], without_predicate_column] {
            let schema = build_target_arrow_schema(read_fields).unwrap();
            for selection in [
                None,
                Some(vec![RowRange::new(5, 11), RowRange::new(20, 22)]),
            ] {
                let expected_records: Vec<_> = matching_rows
                    .iter()
                    .copied()
                    .filter(|&row| {
                        selection.as_ref().is_none_or(|ranges| {
                            ranges
                                .iter()
                                .any(|range| row >= range.from() && row <= range.to())
                        })
                    })
                    .map(|row| record(row, 8))
                    .collect();
                let (reader, _) = counting_read(bytes.clone(), None);
                let batches = AvroFormatReader
                    .read_batch_stream(
                        reader,
                        size,
                        read_fields,
                        Some(&predicates),
                        Some(3),
                        selection,
                    )
                    .await
                    .unwrap()
                    .map_ok(|batch| {
                        let columns: Vec<_> = read_fields
                            .iter()
                            .map(|field| batch.schema_ref().index_of(field.name()).unwrap())
                            .collect();
                        batch.project(&columns).unwrap()
                    })
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap();
                assert_eq!(
                    arrow_select::concat::concat_batches(&schema, &batches).unwrap(),
                    records_to_batch(&expected_records, read_fields, &schema).unwrap()
                );
            }
        }
    }
}

#[tokio::test]
async fn nested_and_logical_values_match_eager_conversion() {
    use crate::spec::{ArrayType, DateType, DecimalType, TimestampType};
    let schema = Schema::parse_str(
        r#"{"type":"record","name":"nested","fields":[
        {"name":"row","type":["null",{"type":"record","name":"child","fields":[
            {"name":"items","type":{"type":"array","items":["null","int"]}}
        ]}]},
        {"name":"other","type":"child"},
        {"name":"map","type":{"type":"map","values":"long"}},
        {"name":"date","type":{"type":"int","logicalType":"date"}},
        {"name":"time","type":{"type":"long","logicalType":"timestamp-micros"}},
        {"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}}
    ]}"#,
    )
    .unwrap();
    let child_type = |items_id| {
        DataType::Row(RowType::new(vec![DataField::new(
            items_id,
            "items".into(),
            DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
        )]))
    };
    let fields = vec![
        DataField::new(0, "row".into(), child_type(5)),
        DataField::new(6, "other".into(), child_type(7)),
        DataField::new(
            1,
            "map".into(),
            DataType::Map(MapType::new(
                DataType::VarChar(VarCharType::string_type()),
                DataType::BigInt(BigIntType::new()),
            )),
        ),
        DataField::new(2, "date".into(), DataType::Date(DateType::new())),
        DataField::new(
            3,
            "time".into(),
            DataType::Timestamp(TimestampType::new(6).unwrap()),
        ),
        DataField::new(
            4,
            "amount".into(),
            DataType::Decimal(DecimalType::new(10, 2).unwrap()),
        ),
    ];
    let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Snappy);
    for row in 0..9 {
        let child = Value::Record(vec![(
            "items".into(),
            Value::Array(vec![
                Value::Union(0, Box::new(Value::Null)),
                Value::Union(1, Box::new(Value::Int(row))),
            ]),
        )]);
        writer
            .append(Value::Record(vec![
                (
                    "row".into(),
                    if row % 2 == 0 {
                        Value::Union(0, Box::new(Value::Null))
                    } else {
                        Value::Union(1, Box::new(child.clone()))
                    },
                ),
                ("other".into(), child),
                (
                    "map".into(),
                    Value::Map(std::collections::HashMap::from([(
                        "key".into(),
                        Value::Long(row as i64),
                    )])),
                ),
                ("date".into(), Value::Date(row)),
                (
                    "time".into(),
                    Value::TimestampMicros(row as i64 * 1_000_001),
                ),
                (
                    "amount".into(),
                    Value::Decimal(apache_avro::Decimal::from(vec![row as u8])),
                ),
            ]))
            .unwrap();
        if row % 3 == 2 {
            writer.flush().unwrap();
        }
    }
    let bytes = writer.into_inner().unwrap();
    let records = Reader::new(bytes.as_slice())
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    let arrow_schema = build_target_arrow_schema(&fields).unwrap();
    let expected = records_to_batch(&records, &fields, &arrow_schema).unwrap();
    let size = bytes.len() as u64;
    let (reader, _) = counting_read(bytes, None);
    let batches = AvroFormatReader
        .read_batch_stream(reader, size, &fields, None, Some(2), None)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        arrow_select::concat::concat_batches(&arrow_schema, &batches).unwrap(),
        expected
    );
}

#[tokio::test]
async fn corrupt_consumed_blocks_and_truncated_framing_return_errors() {
    let good = file(Codec::Null, 3, 4, 16);
    let (header_end, blocks) = layout(&good);
    let mut cases = vec![
        Vec::new(),
        good[..3].to_vec(),
        good[..header_end - 1].to_vec(),
    ];
    let mut bad_magic = good.clone();
    bad_magic[0] ^= 1;
    cases.push(bad_magic);
    let mut bad_codec = good.clone();
    let codec_offset = good[..header_end]
        .windows(4)
        .position(|bytes| bytes == b"null")
        .unwrap();
    bad_codec[codec_offset..codec_offset + 4].copy_from_slice(b"nope");
    cases.push(bad_codec);
    let mut missing_schema = b"Obj\x01\x00".to_vec();
    missing_schema.extend([7; 16]);
    cases.push(missing_schema);
    for end in [
        blocks[0].start + 1,
        blocks[0].payload.end - 1,
        blocks[0].end - 1,
    ] {
        cases.push(good[..end].to_vec());
    }
    let mut bad_marker = good.clone();
    bad_marker[blocks[0].payload.end] ^= 1;
    cases.push(bad_marker);
    let mut too_many_records = good.clone();
    too_many_records[blocks[0].start] = 5 << 1;
    cases.push(too_many_records);
    let mut negative_count = good[..header_end].to_vec();
    negative_count.push(1);
    cases.push(negative_count);
    let mut negative_size = good[..header_end].to_vec();
    negative_size.extend([2, 1]);
    cases.push(negative_size);
    let mut overflowing_count = good[..header_end].to_vec();
    overflowing_count.extend([0xff; 10]);
    cases.push(overflowing_count);
    let mut partial_next_count = good.clone();
    partial_next_count.push(0x80);
    cases.push(partial_next_count);
    for bytes in cases {
        let (stream, stats) = stream(bytes, 2, None).await;
        terminal_error(stream, &stats).await;
    }
}

#[test]
fn corrupt_payloads_return_errors_on_both_executors() {
    async fn check_codecs() {
        for codec in supported_codecs() {
            let mut bytes = file(codec, 2, 4, 16);
            let (_, blocks) = layout(&bytes);
            bytes[blocks[0].payload.clone()].fill(0xff);
            let (stream, stats) = stream(bytes, 2, None).await;
            terminal_error(stream, &stats).await;
        }
    }

    futures::executor::block_on(check_codecs());
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(check_codecs());
}

#[tokio::test]
async fn storage_errors_and_short_reads_release_the_reader() {
    let bytes = file(Codec::Null, 2, 16, INITIAL_READ_AHEAD as usize / 8);
    let size = bytes.len() as u64;
    for (bytes, fail_at) in [
        (bytes.clone(), Some(0)),
        (bytes.clone(), Some(INITIAL_READ_AHEAD)),
        (bytes.clone(), Some(3 * INITIAL_READ_AHEAD)),
        (bytes[..100].to_vec(), None),
    ] {
        let (reader, stats) = counting_read(bytes, fail_at);
        let stream = AvroFormatReader
            .read_batch_stream(reader, size, &fields(), None, Some(2), None)
            .await
            .unwrap();
        let error = terminal_error(stream, &stats).await;
        let expected = if fail_at.is_some() {
            "injected Avro read failure"
        } else {
            "Short read from Avro file"
        };
        assert!(matches!(error, Error::DataInvalid { message, .. } if message == expected));
    }
}

#[tokio::test]
async fn corruption_is_reported_only_when_the_affected_block_or_record_is_consumed() {
    let bytes = file(Codec::Null, 3, 4, 32);
    let (_, blocks) = layout(&bytes);
    let mut bad_marker = bytes.clone();
    bad_marker[blocks[1].payload.end] ^= 1;
    let mut bad_record = bytes;
    let record_size = apache_avro::to_avro_datum(&schema(), record(0, 32))
        .unwrap()
        .len();
    let third_record = blocks[0].payload.start + 2 * record_size;
    bad_record[third_record..third_record + record_size].fill(0xff);
    for (bytes, prefix_rows) in [(bad_marker, 4), (bad_record, 2)] {
        let (mut selected, stats) = stream(
            bytes.clone(),
            prefix_rows,
            Some(vec![RowRange::new(0, prefix_rows as i64 - 1)]),
        )
        .await;
        assert_eq!(
            ids(&[selected.try_next().await.unwrap().unwrap()]),
            (0..prefix_rows as i64).collect::<Vec<_>>()
        );
        assert!(stats.dropped.load(Ordering::SeqCst));
        assert!(selected.try_next().await.unwrap().is_none());
        let (mut stream, stats) = stream(bytes, prefix_rows, None).await;
        assert_eq!(
            ids(&[stream.try_next().await.unwrap().unwrap()]),
            (0..prefix_rows as i64).collect::<Vec<_>>()
        );
        assert!(stream.try_next().await.is_err());
        assert!(stats.dropped.load(Ordering::SeqCst));
        assert!(stream.try_next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn metadata_blocks_support_binary_values_and_validate_lengths() {
    let schema = schema();
    let mut metadata = std::collections::HashMap::new();
    metadata.insert(
        "avro.schema".into(),
        Value::Bytes(serde_json::to_vec(&schema).unwrap()),
    );
    metadata.insert(
        "user.binary".into(),
        Value::Bytes(vec![0xff; INITIAL_READ_AHEAD as usize + 17]),
    );
    let encoded =
        apache_avro::to_avro_datum(&Schema::map(Schema::Bytes), Value::Map(metadata)).unwrap();
    let entries = &encoded[1..encoded.len() - 1];
    for (negative_count, length_adjustment) in [(false, 0), (true, 0), (true, -1), (true, 1)] {
        let mut bytes = b"Obj\x01".to_vec();
        let count = if negative_count { -2 } else { 2 };
        bytes.extend(apache_avro::to_avro_datum(&Schema::Long, Value::Long(count)).unwrap());
        if negative_count {
            bytes.extend(
                apache_avro::to_avro_datum(
                    &Schema::Long,
                    Value::Long(entries.len() as i64 + length_adjustment),
                )
                .unwrap(),
            );
        }
        bytes.extend_from_slice(entries);
        bytes.push(0);
        bytes.extend([7; 16]);
        let mut writer = Writer::append_to(&schema, bytes, [7; 16]);
        writer.append(record(42, 16)).unwrap();
        let (stream, stats) = stream(writer.into_inner().unwrap(), 2, None).await;
        if length_adjustment == 0 {
            assert_eq!(ids(&stream.try_collect::<Vec<_>>().await.unwrap()), [42]);
        } else {
            let error = terminal_error(stream, &stats).await;
            assert!(error.to_string().contains("metadata block length"));
        }
    }
}
