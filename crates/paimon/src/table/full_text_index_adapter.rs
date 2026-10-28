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

use crate::ftindex::reader::FullTextArchiveReader;
use crate::io::FileRead;
use bytes::Bytes;
use futures::StreamExt;
use paimon_ftindex_core::io::{ReadRequest, SeekRead};
use paimon_ftindex_core::FullTextSearchResult;
use roaring::RoaringTreemap;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::{mpsc, Arc, Mutex};

const FULL_TEXT_RANGE_READ_CONCURRENCY: usize = 8;

#[cfg(test)]
pub(crate) async fn search_full_text_index(
    reader: Box<dyn FileRead>,
    file_name: String,
    query: String,
    limit: usize,
    include_row_ids: Option<RoaringTreemap>,
) -> crate::Result<FullTextSearchResult> {
    search_full_text_index_top_k(reader, file_name, query, limit, include_row_ids, None).await
}

/// The caller may request bounded candidates only when no later stage removes,
/// replaces or deduplicates hits across shards. Ties at the boundary fall back.
pub(crate) async fn search_full_text_index_top_k(
    reader: Box<dyn FileRead>,
    file_name: String,
    query: String,
    limit: usize,
    include_row_ids: Option<RoaringTreemap>,
    top_k: Option<usize>,
) -> crate::Result<FullTextSearchResult> {
    let handle =
        tokio::runtime::Handle::try_current().map_err(|error| crate::Error::UnexpectedError {
            message: "Full-text index reader requires a Tokio runtime".to_string(),
            source: Some(Box::new(error)),
        })?;
    let task_file_name = file_name.clone();
    tokio::task::spawn_blocking(move || {
        search_native_index(
            AsyncFileSeekRead::new(reader, handle),
            &task_file_name,
            &query,
            limit,
            include_row_ids,
            top_k,
        )
    })
    .await
    .map_err(|error| crate::Error::UnexpectedError {
        message: format!("Full-text index task for '{file_name}' failed: {error}"),
        source: None,
    })?
}

pub(crate) async fn search_full_text_file(
    file: File,
    file_name: String,
    query: String,
    limit: usize,
    include_row_ids: Option<RoaringTreemap>,
) -> crate::Result<FullTextSearchResult> {
    let task_file_name = file_name.clone();
    tokio::task::spawn_blocking(move || {
        search_native_index(
            StdFileSeekRead::new(file),
            &task_file_name,
            &query,
            limit,
            include_row_ids,
            None,
        )
    })
    .await
    .map_err(|error| crate::Error::UnexpectedError {
        message: format!("Full-text index task for '{file_name}' failed: {error}"),
        source: None,
    })?
}

fn search_native_index<R: SeekRead + 'static>(
    input: R,
    file_name: &str,
    query: &str,
    limit: usize,
    include_row_ids: Option<RoaringTreemap>,
    top_k: Option<usize>,
) -> crate::Result<FullTextSearchResult> {
    let reader = FullTextArchiveReader::from_seek_read(input)
        .map_err(|error| native_error(file_name, error))?;
    let search = |limit| {
        match &include_row_ids {
            Some(include) => reader.search_with_include(query, limit, include),
            None => reader.search(query, limit),
        }
        .map_err(|error| native_error(file_name, error))
    };
    let hits = if let Some(k) = top_k.filter(|k| *k > 0 && *k < limit) {
        let candidate_count = k.saturating_add(1).min(limit);
        let mut hits = search(candidate_count)?;
        if certify_top_k(&mut hits, k) {
            hits
        } else {
            search(limit)?
        }
    } else {
        search(limit)?
    };
    Ok(FullTextSearchResult {
        row_ids: hits.row_ids,
        scores: hits.scores,
    })
}

fn certify_top_k(hits: &mut crate::ftindex::reader::FullTextHits, k: usize) -> bool {
    if hits.row_ids.len() <= k {
        return true;
    }
    // Tantivy's tie-break uses document addresses, while Paimon uses row IDs.
    // Fetch one extra hit to prove that no excluded document can tie the cutoff.
    // Duplicate row IDs also require the exhaustive path before deduplication.
    let distinct = hits.row_ids[..k]
        .iter()
        .collect::<std::collections::HashSet<_>>();
    if distinct.len() != k
        || !hits.scores[k - 1].is_finite()
        || !hits.scores[k].is_finite()
        || hits.scores[k - 1] <= hits.scores[k]
    {
        return false;
    }
    hits.row_ids.truncate(k);
    hits.scores.truncate(k);
    true
}

fn native_error(file_name: &str, error: impl std::fmt::Display) -> crate::Error {
    crate::Error::UnexpectedError {
        message: format!("Failed to read full-text index '{file_name}': {error}"),
        source: None,
    }
}

struct AsyncFileSeekRead {
    reader: Arc<dyn FileRead>,
    handle: tokio::runtime::Handle,
    permits: Arc<tokio::sync::Semaphore>,
}

impl AsyncFileSeekRead {
    fn new(reader: Box<dyn FileRead>, handle: tokio::runtime::Handle) -> Self {
        Self {
            reader: Arc::from(reader),
            handle,
            permits: Arc::new(tokio::sync::Semaphore::new(
                FULL_TEXT_RANGE_READ_CONCURRENCY,
            )),
        }
    }
}

impl SeekRead for AsyncFileSeekRead {
    fn pread(&self, requests: &mut [ReadRequest<'_>]) -> io::Result<()> {
        if requests.is_empty() {
            return Ok(());
        }
        let ranges = requests
            .iter()
            .map(|request| {
                let length = u64::try_from(request.buf.len()).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "full-text read length overflow",
                    )
                })?;
                let end = request.pos.checked_add(length).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "full-text read range overflow")
                })?;
                Ok((request.pos, end))
            })
            .collect::<io::Result<Vec<_>>>()?;

        let reader = Arc::clone(&self.reader);
        let permits = Arc::clone(&self.permits);
        let (tx, rx) = mpsc::sync_channel(1);
        self.handle.spawn(async move {
            let results = futures::stream::iter(ranges.into_iter().map(|(start, end)| {
                let reader = Arc::clone(&reader);
                let permits = Arc::clone(&permits);
                async move {
                    if start == end {
                        return Ok(Bytes::new());
                    }
                    let _permit = permits
                        .acquire_owned()
                        .await
                        .map_err(|_| io::Error::other("full-text range reader closed"))?;
                    reader
                        .read(start..end)
                        .await
                        .map_err(|error| io::Error::other(error.to_string()))
                }
            }))
            .buffered(FULL_TEXT_RANGE_READ_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
            let _ = tx.send(results);
        });

        let results = rx
            .recv()
            .map_err(|_| io::Error::other("full-text async read task was cancelled"))?;
        for (request, result) in requests.iter_mut().zip(results) {
            let bytes = result?;
            if bytes.len() != request.buf.len() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!(
                        "full-text read expected {} bytes, got {}",
                        request.buf.len(),
                        bytes.len()
                    ),
                ));
            }
            request.buf.copy_from_slice(&bytes);
        }
        Ok(())
    }
}

struct StdFileSeekRead {
    file: Mutex<File>,
}

impl StdFileSeekRead {
    fn new(file: File) -> Self {
        Self {
            file: Mutex::new(file),
        }
    }
}

impl SeekRead for StdFileSeekRead {
    fn pread(&self, requests: &mut [ReadRequest<'_>]) -> io::Result<()> {
        let mut file = self
            .file
            .lock()
            .map_err(|_| io::Error::other("full-text temporary file lock poisoned"))?;
        for request in requests {
            file.seek(SeekFrom::Start(request.pos))?;
            file.read_exact(request.buf)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paimon_ftindex_core::io::PosWriter;
    use paimon_ftindex_core::{FullTextIndexConfig, FullTextIndexWriter};
    use std::ops::Range;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct BytesFileRead {
        data: Bytes,
        ranges: Arc<Mutex<Vec<Range<u64>>>>,
    }

    #[async_trait::async_trait]
    impl FileRead for BytesFileRead {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            self.ranges.lock().unwrap().push(range.clone());
            tokio::task::yield_now().await;
            let start = usize::try_from(range.start).unwrap();
            let end = usize::try_from(range.end).unwrap();
            Ok(self.data.slice(start..end))
        }
    }

    fn test_index_bytes() -> Vec<u8> {
        let mut writer = FullTextIndexWriter::new(FullTextIndexConfig::new()).unwrap();
        writer.add_document(7, "apache paimon").unwrap();
        writer.add_document(9, "unrelated").unwrap();
        let mut bytes = Vec::new();
        writer.write(&mut PosWriter::new(&mut bytes)).unwrap();
        bytes
    }

    #[test]
    fn certificate_requires_strict_scores_and_distinct_row_ids() {
        use crate::ftindex::reader::FullTextHits;
        let mut strict = FullTextHits {
            row_ids: vec![9, 3, 1],
            scores: vec![3.0, 2.0, 1.0],
        };
        assert!(certify_top_k(&mut strict, 2));
        assert_eq!(strict.row_ids, [9, 3]);
        for (row_ids, scores) in [
            (vec![9, 3, 1], vec![3.0, 2.0, 2.0]),
            (vec![9, 9, 1], vec![3.0, 2.0, 1.0]),
            (vec![9, 3, 1], vec![3.0, f32::NAN, 1.0]),
        ] {
            let mut hits = FullTextHits {
                row_ids: row_ids.clone(),
                scores,
            };
            assert!(!certify_top_k(&mut hits, 2));
            assert_eq!(hits.row_ids, row_ids);
        }
    }

    #[tokio::test]
    async fn bounded_candidates_match_exhaustive_and_ties_fall_back() {
        for tied in [false, true] {
            let mut writer = FullTextIndexWriter::new(FullTextIndexConfig::new()).unwrap();
            // Deliberately reverse row IDs so document-address ties cannot stand
            // in for the public row-ID tie-break.
            for (id, text) in [
                (9, "paimon"),
                (3, "paimon padding"),
                (1, "paimon padding padding padding"),
            ] {
                writer
                    .add_document(id, if tied { "paimon" } else { text })
                    .unwrap();
            }
            let mut data = Vec::new();
            writer.write(&mut PosWriter::new(&mut data)).unwrap();
            let query = r#"{"match":{"query":"paimon"}}"#.to_string();
            let search = |top_k| {
                search_full_text_index_top_k(
                    Box::new(BytesFileRead {
                        data: Bytes::from(data.clone()),
                        ranges: Default::default(),
                    }),
                    "bounded.index".into(),
                    query.clone(),
                    3,
                    None,
                    top_k,
                )
            };
            let exhaustive = search(None).await.unwrap();
            let bounded = search(Some(1)).await.unwrap();
            assert_eq!(bounded.row_ids.len(), if tied { 3 } else { 1 });
            let expected = crate::full_text::SearchResult::new(
                exhaustive.row_ids.into_iter().map(|id| id as u64).collect(),
                exhaustive.scores,
            )
            .top_k(1);
            let actual = crate::full_text::SearchResult::new(
                bounded.row_ids.into_iter().map(|id| id as u64).collect(),
                bounded.scores,
            )
            .top_k(1);
            assert_eq!(actual.row_ids, expected.row_ids);
            assert_eq!(actual.scores, expected.scores);
            if tied {
                assert_eq!(actual.row_ids, [1]);
            }
        }
    }

    #[test]
    fn test_range_backed_search_works_on_current_thread_runtime() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let data = Bytes::from(test_index_bytes());
            let ranges = Arc::new(Mutex::new(Vec::new()));
            let reader = BytesFileRead {
                data,
                ranges: Arc::clone(&ranges),
            };

            let result = search_full_text_index(
                Box::new(reader),
                "test.index".to_string(),
                r#"{"match":{"query":"paimon"}}"#.to_string(),
                10,
                None,
            )
            .await
            .unwrap();

            assert_eq!(result.row_ids, vec![7]);
            assert!(!ranges.lock().unwrap().is_empty());
        });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_range_backed_search_works_on_multi_thread_runtime() {
        let data = Bytes::from(test_index_bytes());
        let ranges = Arc::new(Mutex::new(Vec::new()));
        let result = search_full_text_index(
            Box::new(BytesFileRead {
                data,
                ranges: Arc::clone(&ranges),
            }),
            "test.index".to_string(),
            r#"{"match":{"query":"paimon"}}"#.to_string(),
            10,
            None,
        )
        .await
        .unwrap();

        assert_eq!(result.row_ids, vec![7]);
        assert!(!ranges.lock().unwrap().is_empty());
    }

    #[test]
    fn test_async_seek_reader_preserves_multi_range_order() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let data = Bytes::from_static(b"0123456789");
            let reader = AsyncFileSeekRead::new(
                Box::new(BytesFileRead {
                    data,
                    ranges: Arc::new(Mutex::new(Vec::new())),
                }),
                tokio::runtime::Handle::current(),
            );
            let result = tokio::task::spawn_blocking(move || {
                let mut first = [0u8; 3];
                let mut second = [0u8; 2];
                reader.pread(&mut [
                    ReadRequest::new(4, &mut first),
                    ReadRequest::new(1, &mut second),
                ])?;
                Ok::<_, io::Error>((first, second))
            })
            .await
            .unwrap()
            .unwrap();

            assert_eq!(&result.0, b"456");
            assert_eq!(&result.1, b"12");
        });
    }

    struct ShortFileRead;

    #[async_trait::async_trait]
    impl FileRead for ShortFileRead {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            let requested = usize::try_from(range.end - range.start).unwrap();
            Ok(Bytes::from(vec![0; requested.saturating_sub(1)]))
        }
    }

    #[tokio::test]
    async fn test_async_seek_reader_rejects_short_reads() {
        let reader =
            AsyncFileSeekRead::new(Box::new(ShortFileRead), tokio::runtime::Handle::current());
        let error = tokio::task::spawn_blocking(move || {
            let mut buffer = [0u8; 4];
            reader.pread(&mut [ReadRequest::new(0, &mut buffer)])
        })
        .await
        .unwrap()
        .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    }

    struct ConcurrentFileRead {
        active: Arc<AtomicUsize>,
        maximum: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl FileRead for ConcurrentFileRead {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.maximum.fetch_max(active, Ordering::SeqCst);
            tokio::task::yield_now().await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(Bytes::from(vec![
                0;
                usize::try_from(range.end - range.start)
                    .unwrap()
            ]))
        }
    }

    #[tokio::test]
    async fn test_async_seek_reader_bounds_range_concurrency() {
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let reader = AsyncFileSeekRead::new(
            Box::new(ConcurrentFileRead {
                active: Arc::clone(&active),
                maximum: Arc::clone(&maximum),
            }),
            tokio::runtime::Handle::current(),
        );
        tokio::task::spawn_blocking(move || {
            let mut buffers = [[0u8; 1]; FULL_TEXT_RANGE_READ_CONCURRENCY * 2];
            let mut requests = buffers
                .iter_mut()
                .enumerate()
                .map(|(position, buffer)| ReadRequest::new(position as u64, buffer))
                .collect::<Vec<_>>();
            reader.pread(&mut requests)
        })
        .await
        .unwrap()
        .unwrap();

        assert!(maximum.load(Ordering::SeqCst) <= FULL_TEXT_RANGE_READ_CONCURRENCY);
    }
}
