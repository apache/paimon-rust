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

use crate::lumina::reader::LuminaVectorGlobalIndexReader;
use crate::lumina::{is_lumina_index_type, GlobalIndexIOMeta, SearchResult, VectorSearch};
use crate::spec::{DataField, FileKind, IndexManifest};
use crate::table::snapshot_manager::SnapshotManager;
use crate::table::{find_field_id_by_name, RowRange, Table};
use std::collections::HashMap;
use std::io::Cursor;

const INDEX_DIR: &str = "index";

pub struct VectorSearchBuilder<'a> {
    table: &'a Table,
    vector_column: Option<String>,
    query_vector: Option<Vec<f32>>,
    limit: Option<usize>,
}

impl<'a> VectorSearchBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            vector_column: None,
            query_vector: None,
            limit: None,
        }
    }

    pub fn with_vector_column(&mut self, name: &str) -> &mut Self {
        self.vector_column = Some(name.to_string());
        self
    }

    pub fn with_query_vector(&mut self, vector: Vec<f32>) -> &mut Self {
        self.query_vector = Some(vector);
        self
    }

    pub fn with_limit(&mut self, limit: usize) -> &mut Self {
        self.limit = Some(limit);
        self
    }

    pub async fn execute(&self) -> crate::Result<Vec<RowRange>> {
        let vector_column =
            self.vector_column
                .as_deref()
                .ok_or_else(|| crate::Error::ConfigInvalid {
                    message: "Vector column must be set via with_vector_column()".to_string(),
                })?;
        let query_vector =
            self.query_vector
                .as_ref()
                .ok_or_else(|| crate::Error::ConfigInvalid {
                    message: "Query vector must be set via with_query_vector()".to_string(),
                })?;
        let limit = self.limit.ok_or_else(|| crate::Error::ConfigInvalid {
            message: "Limit must be set via with_limit()".to_string(),
        })?;

        let vector_search =
            VectorSearch::new(query_vector.clone(), limit, vector_column.to_string())?;

        let snapshot_manager = SnapshotManager::new(
            self.table.file_io().clone(),
            self.table.location().to_string(),
        );

        let snapshot = match snapshot_manager.get_latest_snapshot().await? {
            Some(s) => s,
            None => return Ok(Vec::new()),
        };

        let index_manifest_name = match snapshot.index_manifest() {
            Some(name) => name.to_string(),
            None => return Ok(Vec::new()),
        };

        let manifest_path = format!(
            "{}/manifest/{}",
            self.table.location().trim_end_matches('/'),
            index_manifest_name
        );
        let index_entries = IndexManifest::read(self.table.file_io(), &manifest_path).await?;

        evaluate_vector_search(
            self.table.file_io(),
            self.table.location(),
            self.table.schema().options(),
            &index_entries,
            &vector_search,
            self.table.schema().fields(),
        )
        .await
    }
}

async fn evaluate_vector_search(
    file_io: &crate::io::FileIO,
    table_path: &str,
    table_options: &HashMap<String, String>,
    index_entries: &[crate::spec::IndexManifestEntry],
    vector_search: &VectorSearch,
    schema_fields: &[DataField],
) -> crate::Result<Vec<RowRange>> {
    let table_path = table_path.trim_end_matches('/');

    let field_id = match find_field_id_by_name(schema_fields, &vector_search.field_name) {
        Some(id) => id,
        None => return Ok(Vec::new()),
    };

    let lumina_entries: Vec<_> = index_entries
        .iter()
        .filter(|e| {
            e.kind == FileKind::Add
                && is_lumina_index_type(&e.index_file.index_type)
                && e.index_file
                    .global_index_meta
                    .as_ref()
                    .is_some_and(|m| m.index_field_id == field_id)
        })
        .collect();

    if lumina_entries.is_empty() {
        return Ok(Vec::new());
    }

    let futures: Vec<_> = lumina_entries
        .into_iter()
        .map(|entry| {
            let global_meta = entry.index_file.global_index_meta.as_ref().unwrap();
            let path = format!("{table_path}/{INDEX_DIR}/{}", entry.index_file.file_name);
            let file_name = entry.index_file.file_name.clone();
            let file_size = entry.index_file.file_size as u64;
            let index_meta_bytes = global_meta.index_meta.clone().unwrap_or_default();
            let row_range_start = global_meta.row_range_start;
            let vector_search_clone = vector_search.clone();
            let options = table_options.clone();
            let input = file_io.new_input(&path);
            async move {
                let input = input?;
                let bytes = input.read().await.map_err(|e| crate::Error::DataInvalid {
                    message: format!("Failed to read Lumina index file '{}': {}", file_name, e),
                    source: None,
                })?;

                let io_meta =
                    GlobalIndexIOMeta::new(file_name.clone(), file_size, index_meta_bytes);
                let mut reader = LuminaVectorGlobalIndexReader::new(io_meta, options);
                let data = bytes.to_vec();
                let result =
                    reader.visit_vector_search(&vector_search_clone, |_| Ok(Cursor::new(data)))?;

                match result {
                    Some(scored_map) => Ok::<_, crate::Error>(
                        SearchResult::from_scored_map(scored_map).offset(row_range_start),
                    ),
                    None => Ok(SearchResult::empty()),
                }
            }
        })
        .collect();

    let results = futures::future::try_join_all(futures).await?;
    let mut merged = SearchResult::empty();
    for r in &results {
        merged = merged.or(r);
    }

    merged.top_k(vector_search.limit).to_row_ranges()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lumina::{LEGACY_LUMINA_VECTOR_ANN_IDENTIFIER, LUMINA_IDENTIFIER};
    use crate::spec::{DataType, GlobalIndexMeta, IndexFileMeta, IndexManifestEntry, IntType};

    fn make_field(id: i32, name: &str) -> DataField {
        DataField::new(id, name.to_string(), DataType::Int(IntType::default()))
    }

    #[test]
    fn test_find_field_id_by_name() {
        let fields = vec![make_field(1, "id"), make_field(2, "embedding")];
        assert_eq!(find_field_id_by_name(&fields, "embedding"), Some(2));
        assert_eq!(find_field_id_by_name(&fields, "nonexistent"), None);
    }

    #[tokio::test]
    async fn test_evaluate_no_matching_entries() {
        let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let fields = vec![make_field(1, "id"), make_field(2, "embedding")];
        let vs = VectorSearch::new(vec![1.0, 2.0], 10, "embedding".to_string()).unwrap();

        let entry = IndexManifestEntry {
            kind: FileKind::Add,
            partition: vec![],
            bucket: 0,
            index_file: IndexFileMeta {
                index_type: "btree".to_string(),
                file_name: "test.idx".to_string(),
                file_size: 100,
                row_count: 10,
                deletion_vectors_ranges: None,
                global_index_meta: None,
            },
            version: 1,
        };

        let result = evaluate_vector_search(
            &file_io,
            "memory:///test_table",
            &HashMap::new(),
            &[entry],
            &vs,
            &fields,
        )
        .await
        .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_evaluate_ignores_non_lumina_index_type() {
        let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let fields = vec![make_field(2, "embedding")];
        let vs = VectorSearch::new(vec![1.0], 10, "embedding".to_string()).unwrap();

        let entry = make_lumina_entry("test.idx", "btree", FileKind::Add, 2);

        let result = evaluate_vector_search(
            &file_io,
            "memory:///test_table",
            &HashMap::new(),
            &[entry],
            &vs,
            &fields,
        )
        .await
        .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_evaluate_no_matching_field() {
        let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let fields = vec![make_field(1, "id")];
        let vs = VectorSearch::new(vec![1.0], 10, "embedding".to_string()).unwrap();

        let entry = make_lumina_entry(
            "test.idx",
            LEGACY_LUMINA_VECTOR_ANN_IDENTIFIER,
            FileKind::Add,
            99,
        );

        let result = evaluate_vector_search(
            &file_io,
            "memory:///test_table",
            &HashMap::new(),
            &[entry],
            &vs,
            &fields,
        )
        .await
        .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_evaluate_skips_delete_entries() {
        let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let fields = vec![make_field(2, "embedding")];
        let vs = VectorSearch::new(vec![1.0], 10, "embedding".to_string()).unwrap();

        let entry = make_lumina_entry(
            "test.idx",
            LEGACY_LUMINA_VECTOR_ANN_IDENTIFIER,
            FileKind::Delete,
            2,
        );

        let result = evaluate_vector_search(
            &file_io,
            "memory:///test_table",
            &HashMap::new(),
            &[entry],
            &vs,
            &fields,
        )
        .await
        .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_evaluate_accepts_canonical_lumina_index_type() {
        let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let fields = vec![make_field(2, "embedding")];
        let vs = VectorSearch::new(vec![1.0], 10, "embedding".to_string()).unwrap();

        let entry = make_lumina_entry("missing.idx", LUMINA_IDENTIFIER, FileKind::Add, 2);

        let err = evaluate_vector_search(
            &file_io,
            "memory:///test_table",
            &HashMap::new(),
            &[entry],
            &vs,
            &fields,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("Failed to read Lumina index file 'missing.idx'"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_evaluate_accepts_legacy_lumina_index_type() {
        let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let fields = vec![make_field(2, "embedding")];
        let vs = VectorSearch::new(vec![1.0], 10, "embedding".to_string()).unwrap();

        let entry = make_lumina_entry(
            "missing.idx",
            LEGACY_LUMINA_VECTOR_ANN_IDENTIFIER,
            FileKind::Add,
            2,
        );

        let err = evaluate_vector_search(
            &file_io,
            "memory:///test_table",
            &HashMap::new(),
            &[entry],
            &vs,
            &fields,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("Failed to read Lumina index file 'missing.idx'"),
            "unexpected error: {err}"
        );
    }

    fn make_lumina_entry(
        file_name: &str,
        index_type: &str,
        kind: FileKind,
        index_field_id: i32,
    ) -> IndexManifestEntry {
        IndexManifestEntry {
            kind,
            partition: vec![],
            bucket: 0,
            index_file: IndexFileMeta {
                index_type: index_type.to_string(),
                file_name: file_name.to_string(),
                file_size: 100,
                row_count: 10,
                deletion_vectors_ranges: None,
                global_index_meta: Some(GlobalIndexMeta {
                    row_range_start: 0,
                    row_range_end: 9,
                    index_field_id,
                    extra_field_ids: None,
                    index_meta: None,
                }),
            },
            version: 1,
        }
    }
}
