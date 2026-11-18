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
use crate::btree::test_util::{BytesFileRead, VecFileWrite};
use crate::io::FileWrite;
use bytes::Bytes;
use rand::{Rng, SeedableRng};
use roaring::RoaringTreemap;
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
struct Output(Arc<Mutex<Vec<VecFileWrite>>>);

#[async_trait::async_trait]
impl RTreeOutput for Output {
    async fn create(&mut self) -> std::io::Result<(String, Box<dyn FileWrite>)> {
        let output = VecFileWrite::new();
        let mut files = self.0.lock().unwrap();
        let name = format!("rtree-{}", files.len());
        files.push(output.clone());
        Ok((name, Box::new(output)))
    }
}

fn point(x: f64, y: f64) -> Vec<u8> {
    let mut bytes = vec![1];
    bytes.extend_from_slice(&1_u32.to_le_bytes());
    bytes.extend_from_slice(&x.to_le_bytes());
    bytes.extend_from_slice(&y.to_le_bytes());
    bytes
}

fn line(bounds: BoundingBox) -> Vec<u8> {
    let mut bytes = vec![1];
    bytes.extend_from_slice(&2_u32.to_le_bytes());
    bytes.extend_from_slice(&2_u32.to_le_bytes());
    for value in bounds.coordinates {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

fn reader(
    output: &Output,
    meta: &[RTreeFileMeta],
    total_rows: u64,
    cache: Arc<RTreePageCache>,
) -> RTreeIndexReader {
    let bodies = output.0.lock().unwrap();
    let sources = meta
        .iter()
        .enumerate()
        .map(|(index, meta)| RTreeFileSource {
            meta: meta.clone(),
            reader: Arc::new(BytesFileRead(Bytes::from(bodies[index].to_vec()))),
        })
        .collect();
    RTreeIndexReader::new(sources, total_rows, cache, 1024 * 1024).unwrap()
}

#[tokio::test]
async fn randomized_partitions_match_independent_scan() {
    let mut random = rand::rngs::StdRng::seed_from_u64(20261117);
    let mut values = Vec::new();
    for _ in 0..513 {
        let x = random.gen_range(-100.0..100.0);
        let y = random.gen_range(-100.0..100.0);
        values.push(
            BoundingBox::new(
                x,
                y,
                x + random.gen_range(0.0..30.0),
                y + random.gen_range(0.0..30.0),
            )
            .unwrap(),
        );
    }
    for capacity in [2, 3, 16, 256] {
        let output = Output::default();
        let mut writer = RTreeIndexWriter::new(
            output.clone(),
            RTreeIndexOptions {
                page_size: capacity,
                partition_rows: 37,
            },
        )
        .unwrap();
        for (index, bounds) in values.iter().enumerate() {
            writer
                .write(Some(&line(*bounds)), index as u64 * 17 + (1 << 33))
                .await
                .unwrap();
        }
        let meta = writer.finish().await.unwrap();
        assert_eq!(meta.len(), 14);
        let reader = reader(
            &output,
            &meta,
            (1 << 33) + 513 * 17,
            Arc::new(RTreePageCache::new(4096)),
        );
        for _ in 0..64 {
            let x = random.gen_range(-150.0..150.0);
            let y = random.gen_range(-150.0..150.0);
            let query = BoundingBox::new(x, y, x + 30.0, y + 40.0).unwrap();
            let expected: RoaringTreemap = values
                .iter()
                .enumerate()
                .filter_map(|(index, value)| {
                    let [min_x, min_y, max_x, max_y] = value.coordinates;
                    (min_x <= query.max_x()
                        && max_x >= query.min_x()
                        && min_y <= query.max_y()
                        && max_y >= query.min_y())
                    .then_some(index as u64 * 17 + (1 << 33))
                })
                .collect();
            assert_eq!(reader.query(query).await.unwrap(), expected);
        }
    }
}

#[tokio::test]
async fn null_empty_touching_and_sparse_rows() {
    let output = Output::default();
    let mut writer = RTreeIndexWriter::new(
        output.clone(),
        RTreeIndexOptions {
            page_size: 2,
            partition_rows: 2,
        },
    )
    .unwrap();
    writer.write(None, 4).await.unwrap();
    writer
        .write(Some(&point(f64::NAN, f64::NAN)), 12)
        .await
        .unwrap();
    writer.write(Some(&point(1.0, 1.0)), 1 << 40).await.unwrap();
    let files = writer.finish().await.unwrap();
    assert_eq!(files.iter().map(|file| file.row_count).sum::<usize>(), 3);
    let reader = reader(
        &output,
        &files,
        (1 << 40) + 1,
        Arc::new(RTreePageCache::new(0)),
    );
    assert_eq!(
        reader
            .query_nulls(true)
            .await
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![4]
    );
    assert_eq!(
        reader
            .query_nulls(false)
            .await
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![12, 1 << 40]
    );
    assert_eq!(
        reader
            .query(BoundingBox::new(0.0, 0.0, 1.0, 1.0).unwrap())
            .await
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![1 << 40]
    );
    assert!(writer.write(None, 20).await.is_err());
    assert!(writer.finish().await.is_err());
}

fn fixture(text: &str) -> Vec<u8> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(text.trim())
        .unwrap()
}

fn fixture_meta(body: &[u8], rows: usize) -> RTreeFileMeta {
    RTreeFileMeta {
        name: "fixture".into(),
        header: body[..80].try_into().unwrap(),
        file_size: body.len() as u64,
        row_count: rows,
    }
}

fn open_fixture(body: Vec<u8>, rows: usize, total: u64) -> std::io::Result<RTreeIndexReader> {
    let source = RTreeFileSource {
        meta: fixture_meta(&body, rows),
        reader: Arc::new(BytesFileRead(body.into())),
    };
    RTreeIndexReader::new(vec![source], total, Arc::new(RTreePageCache::new(0)), 4096)
}

#[tokio::test]
async fn interleaved_children_follow_explicit_page_ids() {
    let body = fixture(include_str!("../../testdata/rtree/interleaved-v1.b64"));
    let index = open_fixture(body.clone(), 8, 8).unwrap();
    let query = BoundingBox::new(0.0, 0.0, 3.0, 0.0).unwrap();
    assert_eq!(index.query(query).await.unwrap(), (0..4).collect());
    for child in [0_u64, 4] {
        // wrong level or repeated child under the root
        let mut bad = body.clone();
        let root = &mut bad[80 + 6 * 92..80 + 7 * 92];
        root[80..88].copy_from_slice(&child.to_be_bytes());
        super::format::checksum(root);
        assert!(open_fixture(bad, 8, 8).unwrap().query(query).await.is_err());
    }
}

#[tokio::test]
async fn maximum_signed_row_domain() {
    let max = i64::MAX as u64;
    let output = Output::default();
    let mut writer = RTreeIndexWriter::new(output.clone(), RTreeIndexOptions::default()).unwrap();
    assert!(writer.write(None, max).await.is_err());
    writer.write(None, max - 1).await.unwrap();
    writer.finish().await.unwrap();
    let body = output.0.lock().unwrap()[0].to_vec();
    assert_eq!(&body[body.len() - 4..], &[0; 4]); // empty non-null block CRC
    assert!(open_fixture(body.clone(), 1, max - 1).is_err());
    assert!(open_fixture(body.clone(), 1, max + 1).is_err());
    let index = open_fixture(body, 1, max).unwrap();
    assert_eq!(
        index
            .query_nulls(true)
            .await
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![max - 1]
    );
    assert!(index.query_nulls(false).await.unwrap().is_empty());
}

#[tokio::test]
async fn corrupt_metadata_header_pages_and_rows_are_rejected() {
    for kind in 0..5 {
        let output = Output::default();
        let mut writer = RTreeIndexWriter::new(
            output.clone(),
            RTreeIndexOptions {
                page_size: 2,
                partition_rows: 10,
            },
        )
        .unwrap();
        writer.write(Some(&point(1.0, 2.0)), 0).await.unwrap();
        writer.write(None, 1).await.unwrap();
        let mut files = writer.finish().await.unwrap();
        let mut body = output.0.lock().unwrap()[0].to_vec();
        if kind == 0 {
            files[0].header[12] ^= 1;
        } else if kind == 1 {
            body[20] ^= 1;
        } else if kind == 2 {
            body[88] ^= 1;
        } else if kind == 3 {
            let at = body.len() - 5;
            body[at] ^= 1;
        } else {
            body.truncate(body.len() - 1);
        }
        struct Truncated(Bytes);
        #[async_trait::async_trait]
        impl crate::io::FileRead for Truncated {
            async fn read(&self, range: std::ops::Range<u64>) -> crate::Result<Bytes> {
                let start = (range.start as usize).min(self.0.len());
                let end = (range.end as usize).min(self.0.len());
                Ok(self.0.slice(start..end))
            }
        }
        let sources = vec![RTreeFileSource {
            meta: files.remove(0),
            reader: Arc::new(Truncated(Bytes::from(body))),
        }];
        let reader = RTreeIndexReader::new(sources, 2, Arc::new(RTreePageCache::new(0)), 4096);
        if kind == 0 {
            assert!(reader.is_err());
        } else if kind <= 2 {
            assert!(reader
                .unwrap()
                .query(BoundingBox::new(0.0, 0.0, 3.0, 3.0).unwrap())
                .await
                .is_err());
        } else {
            let reader = reader.unwrap();
            assert_eq!(reader.query_nulls(true).await.unwrap().len(), 1);
            assert!(reader.query_nulls(false).await.is_err());
        }
    }
}

#[test]
fn wkb_endian_collections_and_invalid_input() {
    assert!(BoundingBox::from_wkb(&point(f64::NAN, f64::NAN))
        .unwrap()
        .is_empty());
    let mut big = vec![0];
    big.extend_from_slice(&1_u32.to_be_bytes());
    big.extend_from_slice(&(-2_f64).to_be_bytes());
    big.extend_from_slice(&3_f64.to_be_bytes());
    let mut collection = vec![1];
    collection.extend_from_slice(&7_u32.to_le_bytes());
    collection.extend_from_slice(&2_u32.to_le_bytes());
    collection.extend_from_slice(&big);
    collection.extend_from_slice(&point(4.0, -1.0));
    assert_eq!(
        BoundingBox::from_wkb(&collection).unwrap().coordinates,
        [-2.0, -1.0, 4.0, 3.0]
    );
    for value in [
        point(f64::NAN, 1.0),
        point(f64::INFINITY, 1.0),
        vec![2, 0, 0, 0, 1],
    ] {
        assert!(BoundingBox::from_wkb(&value).is_err());
    }
    let mut trailing = point(1.0, 2.0);
    trailing.push(0);
    assert!(BoundingBox::from_wkb(&trailing).is_err());
    assert!(BoundingBox::from_wkb(&collection[..collection.len() - 1]).is_err());
}

#[test]
fn invalid_options_and_signed_row_range_are_rejected() {
    assert!(RTreeIndexWriter::new(
        Output::default(),
        RTreeIndexOptions {
            page_size: 1,
            partition_rows: 1
        }
    )
    .is_err());
    assert!(RTreeIndexWriter::new(
        Output::default(),
        RTreeIndexOptions {
            page_size: 2,
            partition_rows: 1_048_577
        }
    )
    .is_err());
}

#[tokio::test]
async fn java_multilevel_hilbert_fixture_is_byte_identical() {
    let golden = fixture(include_str!("../../testdata/rtree/mixed-v1.b64"));
    let output = Output::default();
    let mut writer = RTreeIndexWriter::new(
        output.clone(),
        RTreeIndexOptions {
            page_size: 3,
            partition_rows: 65536,
        },
    )
    .unwrap();
    for i in 0..64_u64 {
        let value = if i.is_multiple_of(7) {
            None
        } else if i.is_multiple_of(11) {
            Some(point(f64::NAN, f64::NAN))
        } else {
            Some(point((i % 9) as f64 - 4.0, (i * i % 13) as f64 - 6.0))
        };
        writer
            .write(value.as_deref(), (1 << 33) + i * 13)
            .await
            .unwrap();
    }
    let files = writer.finish().await.unwrap();
    assert_eq!(output.0.lock().unwrap()[0].to_vec(), golden);
    assert_eq!(files[0].file_size, 4032);
    let index = open_fixture(golden, 64, (1 << 33) + 64 * 13).unwrap();
    let ids = |p: fn(u64) -> bool| -> RoaringTreemap {
        (0..64)
            .filter(|&i| p(i))
            .map(|i| (1 << 33) + i * 13)
            .collect()
    };
    assert_eq!(
        index
            .query(BoundingBox::new(-4.0, -6.0, 4.0, 6.0).unwrap())
            .await
            .unwrap(),
        ids(|i| !i.is_multiple_of(7) && !i.is_multiple_of(11))
    );
    assert_eq!(
        index.query_nulls(true).await.unwrap(),
        ids(|i| i.is_multiple_of(7))
    );
    assert_eq!(
        index.query_nulls(false).await.unwrap(),
        ids(|i| !i.is_multiple_of(7))
    );
}

#[tokio::test]
async fn row_blocks_read_exact_ranges_and_reject_unknown_versions() {
    let golden = fixture(include_str!("../../testdata/rtree/mixed-v1.b64"));
    // 26 pages of 132 bytes, 10 nulls and 54 non-nulls.
    let rows = 80 + 26 * 132;
    struct Ranges {
        data: Bytes,
        calls: Mutex<Vec<std::ops::Range<u64>>>,
    }
    #[async_trait::async_trait]
    impl crate::io::FileRead for Ranges {
        async fn read(&self, r: std::ops::Range<u64>) -> crate::Result<Bytes> {
            self.calls.lock().unwrap().push(r.clone());
            Ok(self.data.slice(r.start as usize..r.end as usize))
        }
    }
    let total = (1 << 33) + 64 * 13;
    let data = Arc::new(Ranges {
        data: golden.clone().into(),
        calls: Mutex::default(),
    });
    let source = RTreeFileSource {
        meta: fixture_meta(&golden, 64),
        reader: data.clone(),
    };
    let index =
        RTreeIndexReader::new(vec![source], total, Arc::new(RTreePageCache::new(0)), 4096).unwrap();
    index.query_nulls(true).await.unwrap();
    index.query_nulls(false).await.unwrap();
    assert_eq!(
        *data.calls.lock().unwrap(),
        vec![0..80, rows..rows + 84, 0..80, rows + 84..4032]
    );

    for version in [0_u32, 2] {
        let mut bad = golden.clone();
        bad[4..8].copy_from_slice(&version.to_be_bytes());
        super::format::checksum(&mut bad[..80]);
        assert!(open_fixture(bad, 64, total).is_err());
    }
}

#[tokio::test]
async fn unordered_source_ids_must_be_unique_within_each_file() {
    for last in [3, 5] {
        let output = Output::default();
        let mut writer =
            RTreeIndexWriter::new(output.clone(), RTreeIndexOptions::default()).unwrap();
        writer.write(None, 5).await.unwrap();
        writer.write(Some(&point(0.0, 0.0)), last).await.unwrap();
        if last == 5 {
            assert!(writer
                .finish()
                .await
                .unwrap_err()
                .to_string()
                .contains("Duplicate"));
            assert!(output.0.lock().unwrap().is_empty());
        } else {
            let files = writer.finish().await.unwrap();
            let index = reader(&output, &files, 6, Arc::new(RTreePageCache::new(0)));
            assert_eq!(
                index
                    .query_nulls(false)
                    .await
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![3]
            );
        }
    }
}

#[tokio::test]
async fn empty_geometry_only_and_physical_row_limit() {
    let output = Output::default();
    let mut writer = RTreeIndexWriter::new(output.clone(), RTreeIndexOptions::default()).unwrap();
    writer
        .write(Some(&point(f64::NAN, f64::NAN)), 0)
        .await
        .unwrap();
    let mut file = writer.finish().await.unwrap().remove(0);
    let body = output.0.lock().unwrap()[0].to_vec();
    assert_eq!(body.len(), 96);
    assert_eq!(&body[80..84], &[0; 4]);
    let index = open_fixture(body, 1, 1).unwrap();
    assert!(index
        .query(BoundingBox::new(0.0, 0.0, 1.0, 1.0).unwrap())
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        index
            .query_nulls(false)
            .await
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![0]
    );
    for rows in [1_u32 << 20, (1 << 20) + 1] {
        file.header[24..28].copy_from_slice(&rows.to_be_bytes());
        file.header[36..44].copy_from_slice(&(u64::from(rows) - 1).to_be_bytes());
        super::format::checksum(&mut file.header);
        file.file_size = 88 + u64::from(rows) * 8;
        file.row_count = rows as usize;
        assert_eq!(
            super::format::Metadata::parse(&file, u64::from(rows)).is_ok(),
            rows == 1 << 20
        );
    }
}

struct CountedRead {
    bytes: Bytes,
    reads: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl crate::io::FileRead for CountedRead {
    async fn read(&self, range: std::ops::Range<u64>) -> crate::Result<Bytes> {
        self.reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(self.bytes.slice(range.start as usize..range.end as usize))
    }
}

#[tokio::test]
async fn shared_cache_survives_reader_drop_and_obeys_byte_budget() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let output = Output::default();
    let mut writer = RTreeIndexWriter::new(output.clone(), RTreeIndexOptions::default()).unwrap();
    for i in 0..1024 {
        writer.write(Some(&point(i as f64, 0.0)), i).await.unwrap();
    }
    let files = writer.finish().await.unwrap();
    let body = Bytes::from(output.0.lock().unwrap()[0].to_vec());
    let cache = Arc::new(RTreePageCache::new(65536));
    let reads = Arc::new(AtomicUsize::new(0));
    for _ in 0..2 {
        let sources = vec![RTreeFileSource {
            meta: files[0].clone(),
            reader: Arc::new(CountedRead {
                bytes: body.clone(),
                reads: reads.clone(),
            }),
        }];
        let index = RTreeIndexReader::new(sources, 1024, cache.clone(), 4096).unwrap();
        assert_eq!(
            index
                .query(BoundingBox::new(0.0, 0.0, 1023.0, 0.0).unwrap())
                .await
                .unwrap()
                .len(),
            1024
        );
        assert!(cache.cached_page_bytes() <= 65536);
        assert_eq!(reads.load(Ordering::Relaxed), 6); // header, root, four leaf pages
    }
    let small_cache = Arc::new(RTreePageCache::new(100));
    let index = reader(&output, &files, 1024, small_cache.clone());
    index
        .query(BoundingBox::new(0.0, 0.0, 1023.0, 0.0).unwrap())
        .await
        .unwrap();
    assert_eq!(small_cache.cached_page_bytes(), 0); // oversized pages are never retained
}

#[tokio::test]
async fn empty_null_lookup_and_nonintersecting_bounds_do_not_read() {
    struct NoRead;
    #[async_trait::async_trait]
    impl crate::io::FileRead for NoRead {
        async fn read(&self, _: std::ops::Range<u64>) -> crate::Result<Bytes> {
            panic!("Unexpected index IO");
        }
    }
    let output = Output::default();
    let mut writer = RTreeIndexWriter::new(output, RTreeIndexOptions::default()).unwrap();
    writer.write(Some(&point(1.0, 1.0)), 0).await.unwrap();
    let files = writer.finish().await.unwrap();
    let index = RTreeIndexReader::new(
        vec![RTreeFileSource {
            meta: files[0].clone(),
            reader: Arc::new(NoRead),
        }],
        1,
        Arc::new(RTreePageCache::new(0)),
        4096,
    )
    .unwrap();
    assert!(index.query_nulls(true).await.unwrap().is_empty());
    assert!(index
        .query(BoundingBox::new(2.0, 2.0, 3.0, 3.0).unwrap())
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn disjoint_reads_have_bounded_parallelism_and_release_on_failure() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct ParallelRead {
        bytes: Bytes,
        barrier: tokio::sync::Barrier,
        started: AtomicUsize,
        active: Arc<AtomicUsize>,
        maximum: AtomicUsize,
        fail: bool,
    }
    struct Active(Arc<AtomicUsize>);
    impl Drop for Active {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    #[async_trait::async_trait]
    impl crate::io::FileRead for ParallelRead {
        async fn read(&self, range: std::ops::Range<u64>) -> crate::Result<Bytes> {
            let running = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            let _active = Active(self.active.clone());
            self.maximum.fetch_max(running, Ordering::SeqCst);
            if range.start >= 80 && range.start < 80 + 1024 * 172 {
                let started = self.started.fetch_add(1, Ordering::SeqCst);
                if started < 4 {
                    self.barrier.wait().await;
                }
                if self.fail && started == 0 {
                    return Err(crate::Error::DataInvalid {
                        message: "injected read failure".into(),
                        source: None,
                    });
                }
            }
            Ok(self.bytes.slice(range.start as usize..range.end as usize))
        }
    }
    for fail in [false, true] {
        let output = Output::default();
        let mut writer = RTreeIndexWriter::new(
            output.clone(),
            RTreeIndexOptions {
                page_size: 4,
                partition_rows: 8192,
            },
        )
        .unwrap();
        for y in 0..64_u64 {
            for x in 0..64_u64 {
                writer
                    .write(Some(&point(x as f64, y as f64)), y * 64 + x)
                    .await
                    .unwrap();
            }
        }
        let files = writer.finish().await.unwrap();
        let input = Arc::new(ParallelRead {
            bytes: Bytes::from(output.0.lock().unwrap()[0].to_vec()),
            barrier: tokio::sync::Barrier::new(4),
            started: AtomicUsize::new(0),
            active: Arc::new(AtomicUsize::new(0)),
            maximum: AtomicUsize::new(0),
            fail,
        });
        let index = RTreeIndexReader::new(
            vec![RTreeFileSource {
                meta: files[0].clone(),
                reader: input.clone(),
            }],
            4096,
            Arc::new(RTreePageCache::new(0)),
            1024 * 1024,
        )
        .unwrap();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            index.query(BoundingBox::new(0.0, 32.0, 63.0, 32.0).unwrap()),
        )
        .await
        .unwrap();
        if fail {
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("injected read failure"));
        } else {
            assert_eq!(
                result.unwrap().iter().collect::<Vec<_>>(),
                (2048..2112).collect::<Vec<_>>()
            );
        }
        assert_eq!(input.active.load(Ordering::SeqCst), 0);
        assert_eq!(input.maximum.load(Ordering::SeqCst), 4);
    }
}

#[tokio::test]
async fn output_failure_prevents_unsafe_retry() {
    struct FailingOutput;
    #[async_trait::async_trait]
    impl RTreeOutput for FailingOutput {
        async fn create(&mut self) -> std::io::Result<(String, Box<dyn FileWrite>)> {
            Err(std::io::Error::other("injected output failure"))
        }
    }
    let mut writer = RTreeIndexWriter::new(
        FailingOutput,
        RTreeIndexOptions {
            page_size: 2,
            partition_rows: 1,
        },
    )
    .unwrap();
    assert!(writer.write(None, u64::MAX).await.is_err());
    writer.write(None, 0).await.unwrap();
    assert_eq!(
        writer.write(None, 1).await.unwrap_err().to_string(),
        "injected output failure"
    );
    assert!(writer.write(None, 1).await.is_err());
    assert!(writer.finish().await.is_err());
}

#[tokio::test]
async fn semantic_corruption_is_rejected_even_with_valid_checksums() {
    for kind in 0..4 {
        let output = Output::default();
        let mut writer = RTreeIndexWriter::new(
            output.clone(),
            RTreeIndexOptions {
                page_size: 2,
                partition_rows: 10,
            },
        )
        .unwrap();
        for row in 0..3 {
            writer
                .write(Some(&point(row as f64, 0.0)), row)
                .await
                .unwrap();
        }
        writer.write(None, 3).await.unwrap();
        let files = writer.finish().await.unwrap();
        let mut body = output.0.lock().unwrap()[0].to_vec();
        let page_size = super::format::page_bytes(2);
        if kind < 2 {
            let root = &mut body[80 + 2 * page_size..80 + 3 * page_size];
            if kind == 0 {
                root[40..48].copy_from_slice(&2_u64.to_be_bytes());
            } else {
                root[4..8].copy_from_slice(&2_u32.to_be_bytes());
            }
            super::format::checksum(root);
        } else if kind == 2 {
            let leaf = &mut body[80..80 + page_size];
            leaf[40..48].copy_from_slice(&100_u64.to_be_bytes());
            super::format::checksum(leaf);
        } else {
            let rows = &mut body[80 + 3 * page_size + 12..];
            rows[8..16].copy_from_slice(&0_u64.to_be_bytes());
            super::format::checksum(rows);
        }
        let index = RTreeIndexReader::new(
            vec![RTreeFileSource {
                meta: files[0].clone(),
                reader: Arc::new(BytesFileRead(Bytes::from(body))),
            }],
            4,
            Arc::new(RTreePageCache::new(0)),
            4096,
        )
        .unwrap();
        if kind < 3 {
            assert!(index
                .query(BoundingBox::new(-1.0, -1.0, 3.0, 1.0).unwrap())
                .await
                .is_err());
        } else {
            assert!(index
                .query_nulls(false)
                .await
                .unwrap_err()
                .to_string()
                .contains("Duplicate"));
        }
    }
}

#[tokio::test]
async fn bounded_file_queries_refill_merge_and_drain_after_error() {
    use std::ops::Range;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Poll;
    use tokio::sync::Semaphore;

    struct GatedFile {
        bytes: Bytes,
        gate: Arc<Semaphore>,
        opened: Arc<AtomicUsize>,
        corrupt: bool,
    }
    #[async_trait::async_trait]
    impl crate::io::FileRead for GatedFile {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            if range.start == 0 {
                self.opened.fetch_add(1, Ordering::SeqCst);
                self.gate.acquire().await.unwrap().forget();
                if self.corrupt {
                    return Ok(Bytes::new());
                }
            }
            Ok(self.bytes.slice(range.start as usize..range.end as usize))
        }
    }

    let output = Output::default();
    let mut writer = RTreeIndexWriter::new(
        output.clone(),
        RTreeIndexOptions {
            partition_rows: 2,
            ..Default::default()
        },
    )
    .unwrap();
    for id in 0..8 {
        let value = point(id as f64, 0.0);
        writer
            .write((id % 2 == 1).then_some(value.as_slice()), id)
            .await
            .unwrap();
    }
    let files = writer.finish().await.unwrap();
    let bounds = BoundingBox::new(0., 0., 5., 0.).unwrap();
    for (kind, corrupt) in [
        (None, false),
        (Some(true), false),
        (Some(false), false),
        (None, true),
    ] {
        let gates: Vec<_> = files.iter().map(|_| Arc::new(Semaphore::new(0))).collect();
        let opened = Arc::new(AtomicUsize::new(0));
        let sources = files
            .iter()
            .enumerate()
            .map(|(i, meta)| RTreeFileSource {
                meta: meta.clone(),
                reader: Arc::new(GatedFile {
                    bytes: Bytes::from(output.0.lock().unwrap()[i].to_vec()),
                    gate: gates[i].clone(),
                    opened: opened.clone(),
                    corrupt: corrupt && i == 0,
                }),
            })
            .collect();
        let index = RTreeIndexReader::new(sources, 8, Arc::new(RTreePageCache::new(0)), 4096)
            .unwrap()
            .with_file_concurrency(2)
            .unwrap();
        let mut query = Box::pin(async {
            match kind {
                Some(nulls) => index.query_nulls(nulls).await,
                None => index.query(bounds).await,
            }
        });
        assert!(matches!(futures::poll!(&mut query), Poll::Pending));
        assert_eq!(opened.load(Ordering::SeqCst), 2);
        // Another query on the same index shares the active-file limit. Cancelling
        // a waiter must leave the initial query free to refill its slots.
        let mut concurrent = Box::pin(index.query_nulls(true));
        assert!(matches!(futures::poll!(&mut concurrent), Poll::Pending));
        assert_eq!(opened.load(Ordering::SeqCst), 2);
        drop(concurrent);
        if corrupt {
            gates[0].add_permits(1);
            // The other admitted file is still open. The remaining files must not start.
            assert!(matches!(futures::poll!(&mut query), Poll::Pending));
            gates[1].add_permits(1);
            assert_eq!(
                query.await.unwrap_err().to_string(),
                "Truncated R-tree read"
            );
            assert_eq!(opened.load(Ordering::SeqCst), 2);
        } else {
            // The second file finishes first. Fill its slot without waiting for the first.
            gates[1].add_permits(1);
            assert!(matches!(futures::poll!(&mut query), Poll::Pending));
            assert_eq!(opened.load(Ordering::SeqCst), 3);
            for gate in &gates {
                gate.add_permits(1);
            }
            let expected = match kind {
                None => vec![1, 3, 5],
                Some(true) => vec![0, 2, 4, 6],
                Some(false) => vec![1, 3, 5, 7],
            };
            assert_eq!(query.await.unwrap().iter().collect::<Vec<_>>(), expected);
            assert_eq!(
                opened.load(Ordering::SeqCst),
                if kind.is_none() { 3 } else { 4 }
            );
        }
    }
    assert!(reader(&output, &files, 8, Arc::new(RTreePageCache::new(0)))
        .with_file_concurrency(0)
        .is_err());
}

#[test]
fn signed_zero_bounds_match_java_in_either_input_order() {
    for (a, b) in [(0.0, -0.0), (-0.0, 0.0)] {
        let first = BoundingBox::from_wkb(&point(a, b)).unwrap();
        let second = BoundingBox::from_wkb(&point(b, a)).unwrap();
        let result = first.union(second);
        assert_eq!(
            result.coordinates.map(f64::to_bits),
            [(-0.0_f64).to_bits(), (-0.0_f64).to_bits(), 0, 0]
        );
    }
}

#[tokio::test]
async fn reject_impossible_row_domains_before_pruning() {
    let output = Output::default();
    let mut writer = RTreeIndexWriter::new(output.clone(), RTreeIndexOptions::default()).unwrap();
    writer.write(None, 2).await.unwrap();
    writer.write(None, 8).await.unwrap();
    writer.finish().await.unwrap();
    let mut bytes = output.0.lock().unwrap()[0].to_vec();
    assert!(open_fixture(bytes.clone(), 2, 9).is_ok());
    bytes[36..44].copy_from_slice(&2u64.to_be_bytes());
    super::format::checksum(&mut bytes[..80]);
    assert!(open_fixture(bytes, 2, 9).is_err());

    let output = Output::default();
    let mut writer = RTreeIndexWriter::new(output.clone(), RTreeIndexOptions::default()).unwrap();
    writer.write(None, 2).await.unwrap();
    writer.finish().await.unwrap();
    let mut bytes = output.0.lock().unwrap()[0].to_vec();
    bytes[36..44].copy_from_slice(&3u64.to_be_bytes());
    super::format::checksum(&mut bytes[..80]);
    assert!(open_fixture(bytes, 1, 9).is_err());
}
