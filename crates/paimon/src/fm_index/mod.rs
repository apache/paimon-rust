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

//! Exact, partitioned FM global index compatible with Java Paimon's V1 format.

mod format;
mod options;
mod reader;
mod suffix_array;
mod writer;

pub(crate) use options::FMOptions;
pub(crate) use reader::{FMGlobalIndexReader, FMReadContext, FMReadOptions};
pub(crate) use writer::{FMGlobalIndexWriter, FMWriteOptions};

pub(crate) fn manifest_row_range(bytes: &[u8]) -> std::io::Result<(u64, u64)> {
    format::read_index_meta(bytes).map(|meta| (meta.first_row_id, meta.row_count))
}

#[cfg(test)]
pub(crate) fn validate_manifest_meta(bytes: &[u8]) -> std::io::Result<()> {
    manifest_row_range(bytes).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::test_util::{BytesFileRead, VecFileWrite};
    use crate::btree::BlockCompressionType;
    use bytes::Bytes;
    use rand::{rngs::StdRng, Rng, SeedableRng};
    use std::ops::Range;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    struct ConcurrentTrackingFileRead {
        bytes: Bytes,
        active: Arc<AtomicUsize>,
        maximum: Arc<AtomicUsize>,
    }

    struct RuntimeResponsiveFileWrite {
        heartbeat_ran: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl crate::io::FileWrite for RuntimeResponsiveFileWrite {
        async fn write(&mut self, _bytes: Bytes) -> crate::Result<()> {
            assert!(
                self.heartbeat_ran.load(Ordering::SeqCst),
                "FM partition encoding blocked the async runtime before file I/O"
            );
            Ok(())
        }

        async fn close(&mut self) -> crate::Result<()> {
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl crate::io::FileRead for ConcurrentTrackingFileRead {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.maximum.fetch_max(active, Ordering::SeqCst);
            tokio::task::yield_now().await;
            let bytes = self.bytes.slice(range.start as usize..range.end as usize);
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(bytes)
        }
    }

    async fn build(values: &[Option<Vec<u8>>], options: FMWriteOptions) -> (Vec<u8>, Vec<u8>) {
        let output = VecFileWrite::new();
        let mut writer = FMGlobalIndexWriter::new(Box::new(output.clone()), options).unwrap();
        for (row, value) in values.iter().enumerate() {
            writer.write(value.as_deref(), row as u64).await.unwrap();
        }
        let result = writer.finish().await.unwrap();
        assert_eq!(result.row_count, values.len() as u64);
        (output.to_vec(), result.index_meta)
    }

    async fn reader(bytes: Vec<u8>, meta: Vec<u8>) -> FMGlobalIndexReader {
        let len = bytes.len() as u64;
        FMGlobalIndexReader::open(
            Box::new(BytesFileRead(Bytes::from(bytes))),
            len,
            &meta,
            FMReadOptions {
                locate_cost_ratio: 1.0,
                ..FMReadOptions::default()
            },
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn exact_contains_null_empty_and_partitions() {
        let values = vec![
            Some(b"abcdef".to_vec()),
            Some(b"aaaaaa".to_vec()),
            None,
            Some(Vec::new()),
            Some("你好世界".as_bytes().to_vec()),
            Some(b"tail-abcdef-tail".to_vec()),
            Some(b"ab".to_vec()),
        ];
        let (bytes, meta) = build(
            &values,
            FMWriteOptions {
                partition_row_count: 3,
                sample_rate: 1,
                compression: BlockCompressionType::Lz4,
                ..FMWriteOptions::default()
            },
        )
        .await;
        let reader = reader(bytes, meta).await;
        assert_eq!(
            reader
                .contains(b"a")
                .await
                .unwrap()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![0, 1, 5, 6]
        );
        assert_eq!(
            reader
                .contains(b"abcdef")
                .await
                .unwrap()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![0, 5]
        );
        assert_eq!(
            reader
                .contains("好".as_bytes())
                .await
                .unwrap()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![4]
        );
        assert!(reader
            .contains(b"missing")
            .await
            .unwrap()
            .unwrap()
            .is_empty());
        assert_eq!(
            reader
                .contains(b"")
                .await
                .unwrap()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![0, 1, 3, 4, 5, 6]
        );
        assert_eq!(
            reader.is_null().await.unwrap().iter().collect::<Vec<_>>(),
            vec![2]
        );
    }

    #[tokio::test]
    async fn randomized_exactness_matches_byte_scan() {
        let mut random = StdRng::seed_from_u64(99173);
        let values = (0..300)
            .map(|_| {
                if random.gen_ratio(1, 11) {
                    None
                } else {
                    let mut value = vec![0u8; random.gen_range(0..40)];
                    random.fill(value.as_mut_slice());
                    Some(value)
                }
            })
            .collect::<Vec<_>>();
        let (bytes, meta) = build(
            &values,
            FMWriteOptions {
                partition_row_count: 47,
                partition_size: 1024,
                sample_rate: 1,
                compression: BlockCompressionType::None,
                ..FMWriteOptions::default()
            },
        )
        .await;
        let reader = reader(bytes, meta).await;
        for _ in 0..250 {
            let mut needle = vec![0u8; random.gen_range(0..8)];
            random.fill(needle.as_mut_slice());
            let expected = values
                .iter()
                .enumerate()
                .filter_map(|(row, value)| {
                    value
                        .as_ref()
                        .is_some_and(|value| {
                            needle.is_empty()
                                || value.windows(needle.len()).any(|window| window == needle)
                        })
                        .then_some(row as u64)
                })
                .collect::<Vec<_>>();
            assert_eq!(
                reader
                    .contains(&needle)
                    .await
                    .unwrap()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }

    #[tokio::test]
    async fn corruption_fails_closed_and_dense_query_declines() {
        let (mut bytes, meta) = build(
            &[Some(b"unique-needle".to_vec())],
            FMWriteOptions {
                sample_rate: 1,
                compression: BlockCompressionType::None,
                ..FMWriteOptions::default()
            },
        )
        .await;
        bytes[0] ^= 1;
        let len = bytes.len() as u64;
        let reader = FMGlobalIndexReader::open(
            Box::new(BytesFileRead(Bytes::from(bytes))),
            len,
            &meta,
            FMReadOptions {
                locate_cost_ratio: 1.0,
                ..FMReadOptions::default()
            },
        )
        .await
        .unwrap();
        assert!(reader.contains(b"unique").await.is_err());

        let repeated = vec![Some(vec![b'a'; 10_000]), Some(vec![b'a'; 10_000])];
        let (bytes, meta) = build(
            &repeated,
            FMWriteOptions {
                sample_rate: 4,
                compression: BlockCompressionType::None,
                ..FMWriteOptions::default()
            },
        )
        .await;
        let len = bytes.len() as u64;
        let reader = FMGlobalIndexReader::open(
            Box::new(BytesFileRead(Bytes::from(bytes))),
            len,
            &meta,
            FMReadOptions {
                locate_cost_ratio: 1.0,
                ..FMReadOptions::default()
            },
        )
        .await
        .unwrap();
        assert!(reader.contains(b"a").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn concurrent_opens_share_the_file_read_limit() {
        let (bytes, meta) = build(
            &[Some(b"bounded metadata reads".to_vec())],
            FMWriteOptions::default(),
        )
        .await;
        let bytes = Bytes::from(bytes);
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let options = FMReadOptions::default();
        let context = Arc::new(FMReadContext::new(options.cache_size));

        futures::future::try_join_all((0..16).map(|index| {
            FMGlobalIndexReader::open_with_context(
                Box::new(ConcurrentTrackingFileRead {
                    bytes: bytes.clone(),
                    active: Arc::clone(&active),
                    maximum: Arc::clone(&maximum),
                }),
                bytes.len() as u64,
                &meta,
                options,
                Arc::clone(&context),
                format!("index-{index}"),
            )
        }))
        .await
        .unwrap();

        assert!(maximum.load(Ordering::SeqCst) <= 8);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn partition_encoding_yields_the_async_runtime() {
        let heartbeat_ran = Arc::new(AtomicBool::new(false));
        let mut writer = FMGlobalIndexWriter::new(
            Box::new(RuntimeResponsiveFileWrite {
                heartbeat_ran: Arc::clone(&heartbeat_ran),
            }),
            FMWriteOptions {
                compression: BlockCompressionType::None,
                ..FMWriteOptions::default()
            },
        )
        .unwrap();
        let value = vec![b'a'; 1024 * 1024];
        writer.write(Some(&value), 0).await.unwrap();

        let finish = writer.finish();
        let heartbeat = async {
            heartbeat_ran.store(true, Ordering::SeqCst);
        };
        let (result, ()) = tokio::join!(finish, heartbeat);
        result.unwrap();
    }

    #[tokio::test]
    async fn java_generated_v1_golden_is_bidirectionally_compatible() {
        // Byte-for-byte output of Java's FMGlobalIndexWriter V1 for the values below.
        let golden = include_bytes!("goldens/fm_index_v1.bin").to_vec();
        let meta = format::read_container_index_meta(
            &BytesFileRead(Bytes::copy_from_slice(&golden)),
            golden.len() as u64,
        )
        .await
        .unwrap();
        let reader = reader(golden.clone(), format::write_index_meta(&meta).unwrap()).await;
        assert_eq!(
            reader
                .contains(b"banana")
                .await
                .unwrap()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![0, 2]
        );
        assert_eq!(
            reader.is_null().await.unwrap().iter().collect::<Vec<_>>(),
            vec![1]
        );

        let values = vec![
            Some(b"banana".to_vec()),
            None,
            Some(vec![0, 255, b'b', b'a', b'n', b'a', b'n', b'a']),
            Some(Vec::new()),
        ];
        let (rust_bytes, _) = build(
            &values,
            FMWriteOptions {
                partition_size: 100,
                partition_row_count: 100,
                sample_rate: 4,
                compression: BlockCompressionType::None,
                compression_level: 1,
            },
        )
        .await;
        assert_eq!(
            rust_bytes, golden,
            "Rust FM-index V1 output differs from the Java-generated golden fixture"
        );
    }
}
