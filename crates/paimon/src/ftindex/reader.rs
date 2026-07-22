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

//! Reader over the `paimon-ftindex-core` v1 archive format.

use paimon_ftindex_core::io::{SeekRead, SliceReader};
use paimon_ftindex_core::{FullTextIndexReader, FullTextSearchResult};
use roaring::RoaringTreemap;

use crate::io::InputFile;

/// Search hits from a full-text archive: parallel row-id and score arrays,
/// ordered best-score-first (as returned by the engine).
#[derive(Debug, Clone)]
pub struct FullTextHits {
    pub row_ids: Vec<i64>,
    pub scores: Vec<f32>,
}

impl From<FullTextSearchResult> for FullTextHits {
    fn from(r: FullTextSearchResult) -> Self {
        Self {
            row_ids: r.row_ids,
            scores: r.scores,
        }
    }
}

/// Reader over a single full-text index archive, backed by the shared
/// `paimon-ftindex-core` engine (v1 archive format).
///
/// Generic over any `SeekRead` implementation, allowing both whole-file
/// (via `SliceReader`) and streaming/range-read strategies.
pub struct FullTextArchiveReader<R: SeekRead + 'static> {
    inner: FullTextIndexReader<R>,
}

impl<R: SeekRead + 'static> FullTextArchiveReader<R> {
    /// Open a full-text archive reader over any `SeekRead` implementation.
    ///
    /// Use this for streaming/range-read strategies (e.g., remote FileIO with
    /// bounded concurrency). For convenience when reading whole files into
    /// memory, see [`from_input_file`](Self::from_input_file).
    pub fn from_seek_read(reader: R) -> crate::Result<Self> {
        let inner = FullTextIndexReader::open(reader).map_err(map_ft_err)?;
        Ok(Self { inner })
    }

    /// Search with a JSON DSL query (see the engine's query spec), returning up
    /// to `limit` best-scoring hits.
    pub fn search(&self, query_json: &str, limit: usize) -> crate::Result<FullTextHits> {
        self.inner
            .search(query_json, limit)
            .map(Into::into)
            .map_err(map_ft_err)
    }

    /// Like [`search`](Self::search) but restricts results to `include_row_ids`
    /// (the live-row allow-list).
    pub fn search_with_include(
        &self,
        query_json: &str,
        limit: usize,
        include_row_ids: RoaringTreemap,
    ) -> crate::Result<FullTextHits> {
        let mut filter_bytes = Vec::with_capacity(include_row_ids.serialized_size());
        include_row_ids
            .serialize_into(&mut filter_bytes)
            .map_err(|e| crate::Error::UnexpectedError {
                message: format!("failed to serialize row-id filter: {e}"),
                source: Some(Box::new(e)),
            })?;
        self.inner
            .search_with_roaring_filter(query_json, limit, &filter_bytes)
            .map(Into::into)
            .map_err(map_ft_err)
    }
}

impl FullTextArchiveReader<SliceReader> {
    /// Read the whole archive from `input` into memory and open the engine
    /// reader over it. Archives are single index files, so a whole-read keeps
    /// peak memory at one archive.
    ///
    /// This is a convenience wrapper over [`from_seek_read`](Self::from_seek_read)
    /// for the common whole-file case.
    pub async fn from_input_file(input: &InputFile) -> crate::Result<Self> {
        let bytes = input.read().await?;
        Self::from_seek_read(SliceReader::new(bytes.to_vec()))
    }
}

fn map_ft_err(e: paimon_ftindex_core::FtIndexError) -> crate::Error {
    crate::Error::UnexpectedError {
        message: format!("full-text index engine error: {e}"),
        source: Some(Box::new(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;
    use bytes::Bytes;
    use paimon_ftindex_core::io::PosWriter;
    use paimon_ftindex_core::{FullTextIndexConfig, FullTextIndexWriter};

    // Build a tiny archive in memory using the core writer, return its bytes.
    fn build_archive(docs: &[(i64, &str)]) -> Vec<u8> {
        let mut writer = FullTextIndexWriter::new(FullTextIndexConfig::new()).unwrap();
        for (row_id, text) in docs {
            writer.add_document(*row_id, (*text).to_string()).unwrap();
        }
        let mut out = PosWriter::new(Vec::<u8>::new());
        writer.write(&mut out).unwrap();
        out.into_inner()
    }

    async fn archive_input(bytes: Vec<u8>) -> crate::io::InputFile {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/ft-pr1-roundtrip.archive";
        let output = file_io.new_output(path).unwrap();
        output.write(Bytes::from(bytes)).await.unwrap();
        output.to_input_file()
    }

    #[tokio::test]
    async fn test_round_trip_search_returns_expected_rows() {
        let bytes = build_archive(&[
            (0, "alpha bravo charlie"),
            (1, "bravo delta"),
            (2, "echo foxtrot"),
        ]);
        let input = archive_input(bytes).await;
        let reader = FullTextArchiveReader::from_input_file(&input)
            .await
            .unwrap();

        let hits = reader.search(r#"{"match":{"query":"bravo"}}"#, 10).unwrap();
        let mut ids = hits.row_ids.clone();
        ids.sort_unstable();
        assert_eq!(ids, vec![0, 1]);
        assert_eq!(hits.scores.len(), hits.row_ids.len());
    }

    #[tokio::test]
    async fn test_search_with_include_restricts_to_allow_list() {
        let bytes = build_archive(&[
            (0, "shared token here"),
            (1, "shared token here"),
            (2, "shared token here"),
        ]);
        let input = archive_input(bytes).await;
        let reader = FullTextArchiveReader::from_input_file(&input)
            .await
            .unwrap();

        // All three match, but restrict to row-ids {0, 2}.
        let mut include = roaring::RoaringTreemap::new();
        include.insert(0);
        include.insert(2);

        let hits = reader
            .search_with_include(r#"{"match":{"query":"token"}}"#, 10, include)
            .unwrap();
        let mut ids = hits.row_ids.clone();
        ids.sort_unstable();
        assert_eq!(ids, vec![0, 2]);
    }

    #[tokio::test]
    async fn test_search_with_include_empty_or_disjoint_returns_no_hits() {
        let bytes = build_archive(&[
            (0, "shared token here"),
            (1, "shared token here"),
            (2, "shared token here"),
        ]);
        let input = archive_input(bytes).await;
        let reader = FullTextArchiveReader::from_input_file(&input)
            .await
            .unwrap();

        // Empty allow-list: an empty include-set admits no rows (not all rows).
        let empty = roaring::RoaringTreemap::new();
        let hits = reader
            .search_with_include(r#"{"match":{"query":"token"}}"#, 10, empty)
            .unwrap();
        assert!(
            hits.row_ids.is_empty(),
            "empty include-set must admit no rows"
        );

        // Allow-list disjoint from the matches (row 99 never indexed): still nothing.
        let mut disjoint = roaring::RoaringTreemap::new();
        disjoint.insert(99);
        let hits = reader
            .search_with_include(r#"{"match":{"query":"token"}}"#, 10, disjoint)
            .unwrap();
        assert!(
            hits.row_ids.is_empty(),
            "include-set disjoint from matches must admit no rows"
        );
    }
}
