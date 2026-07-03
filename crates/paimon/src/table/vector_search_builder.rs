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

use crate::io::FileIO;
use crate::lumina::reader::LuminaVectorGlobalIndexReader;
use crate::lumina::{is_lumina_index_type, LuminaIndexMeta, LuminaVectorMetric};
use crate::spec::{
    CoreOptions, DataField, FileKind, GlobalIndexSearchMode, IndexFileMeta, IndexManifest,
    IndexManifestEntry, ROW_ID_FIELD_NAME,
};
use crate::table::global_index_scanner::unindexed_ranges_for_global_index_entries;
use crate::table::snapshot_manager::SnapshotManager;
use crate::table::{find_field_id_by_name, merge_row_ranges, RowRange, Table};
use crate::vector_search::{GlobalIndexIOMeta, SearchResult, VectorSearch};
use crate::vindex::is_vindex_index_type;
use crate::vindex::reader::VindexVectorGlobalIndexReader;
use arrow_array::{Array, FixedSizeListArray, Float32Array, Int64Array, ListArray, RecordBatch};
use futures::TryStreamExt;
use paimon_vindex_core::distance::MetricType;
use paimon_vindex_core::index::VectorIndexReader as VIndexReader;
use std::collections::{HashMap, HashSet};
use std::io::Cursor;

const INDEX_DIR: &str = "index";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum VectorIndexBackend {
    Lumina,
    Vindex,
}

impl VectorIndexBackend {
    fn from_index_type(index_type: &str) -> Option<Self> {
        if is_lumina_index_type(index_type) {
            Some(Self::Lumina)
        } else if is_vindex_index_type(index_type) {
            Some(Self::Vindex)
        } else {
            None
        }
    }

    fn error_name(self) -> &'static str {
        match self {
            Self::Lumina => "Lumina",
            Self::Vindex => "vindex",
        }
    }
}

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

        let index_entries = match snapshot.index_manifest() {
            Some(index_manifest_name) => {
                let manifest_path = format!(
                    "{}/manifest/{}",
                    self.table.location().trim_end_matches('/'),
                    index_manifest_name
                );
                IndexManifest::read(self.table.file_io(), &manifest_path).await?
            }
            None => Vec::new(),
        };

        evaluate_vector_search(
            VectorSearchEvaluation {
                table: Some(self.table),
                file_io: self.table.file_io(),
                table_path: self.table.location(),
                table_options: self.table.schema().options(),
                schema_fields: self.table.schema().fields(),
                next_row_id: snapshot.next_row_id(),
            },
            &index_entries,
            &vector_search,
        )
        .await
    }
}

struct VectorSearchEvaluation<'a> {
    table: Option<&'a Table>,
    file_io: &'a FileIO,
    table_path: &'a str,
    table_options: &'a HashMap<String, String>,
    schema_fields: &'a [DataField],
    next_row_id: Option<i64>,
}

async fn evaluate_vector_search(
    evaluation: VectorSearchEvaluation<'_>,
    index_entries: &[IndexManifestEntry],
    vector_search: &VectorSearch,
) -> crate::Result<Vec<RowRange>> {
    let table_path = evaluation.table_path.trim_end_matches('/');
    let search_mode = CoreOptions::new(evaluation.table_options).global_index_search_mode()?;

    let field_id = match find_field_id_by_name(evaluation.schema_fields, &vector_search.field_name)
    {
        Some(id) => id,
        None => return Ok(Vec::new()),
    };

    let vector_entries: Vec<_> = index_entries
        .iter()
        .filter(|e| {
            e.kind == FileKind::Add
                && VectorIndexBackend::from_index_type(&e.index_file.index_type).is_some()
                && e.index_file
                    .global_index_meta
                    .as_ref()
                    .is_some_and(|m| m.index_field_id == field_id)
        })
        .collect();

    if vector_entries.is_empty() && search_mode == GlobalIndexSearchMode::Fast {
        return Ok(Vec::new());
    }

    let mut merged = SearchResult::empty();
    if !vector_entries.is_empty() {
        let futures: Vec<_> = vector_entries
            .into_iter()
            .map(|entry| {
                let global_meta = entry.index_file.global_index_meta.as_ref().unwrap();
                let backend = VectorIndexBackend::from_index_type(&entry.index_file.index_type)
                    .expect("filtered vector index type");
                let path = format!("{table_path}/{INDEX_DIR}/{}", entry.index_file.file_name);
                let file_name = entry.index_file.file_name.clone();
                let file_size = entry.index_file.file_size as u64;
                let index_meta_bytes = global_meta.index_meta.clone().unwrap_or_default();
                let row_range_start = global_meta.row_range_start;
                let vector_search_clone = vector_search.clone();
                let options = evaluation.table_options.clone();
                let input = evaluation.file_io.new_input(&path);
                async move {
                    let input = input?;
                    let bytes = input.read().await.map_err(|e| crate::Error::DataInvalid {
                        message: format!(
                            "Failed to read {} index file '{}': {}",
                            backend.error_name(),
                            file_name,
                            e
                        ),
                        source: None,
                    })?;

                    let io_meta =
                        GlobalIndexIOMeta::new(file_name.clone(), file_size, index_meta_bytes);
                    let data = bytes.to_vec();
                    let result = match backend {
                        VectorIndexBackend::Lumina => {
                            let mut reader = LuminaVectorGlobalIndexReader::new(io_meta, options);
                            reader.visit_vector_search(&vector_search_clone, |_| {
                                Ok(Cursor::new(data))
                            })?
                        }
                        VectorIndexBackend::Vindex => {
                            let mut reader = VindexVectorGlobalIndexReader::new(io_meta, options);
                            reader.visit_vector_search(&vector_search_clone, |_| {
                                Ok(Cursor::new(data))
                            })?
                        }
                    };

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
        for r in &results {
            merged = merged.or(r);
        }
    }

    if search_mode != GlobalIndexSearchMode::Fast {
        let detail_ranges = if search_mode == GlobalIndexSearchMode::Detail {
            let table = evaluation.table.ok_or_else(|| crate::Error::DataInvalid {
                message: "Vector raw search in detail mode requires table context".to_string(),
                source: None,
            })?;
            detail_data_ranges_for_table(table).await?
        } else {
            Vec::new()
        };
        let field_ids = HashSet::from([field_id]);
        let raw_ranges = unindexed_ranges_for_global_index_entries(
            index_entries,
            &field_ids,
            search_mode,
            evaluation.next_row_id,
            &detail_ranges,
            is_vector_global_index_file,
        );
        if !raw_ranges.is_empty() {
            let table = evaluation.table.ok_or_else(|| crate::Error::DataInvalid {
                message: "Vector raw search requires table context".to_string(),
                source: None,
            })?;
            let metric = resolve_raw_vector_metric(
                evaluation.file_io,
                table_path,
                evaluation.table_options,
                index_entries,
                field_id,
                &vector_search.field_name,
            )
            .await?;
            let raw_result =
                read_raw_vector_search(table, vector_search, &raw_ranges, metric).await?;
            merged = merged.or(&raw_result);
        }
    }

    merged.top_k(vector_search.limit).to_row_ranges()
}

fn is_vector_global_index_file(index_file: &IndexFileMeta) -> bool {
    VectorIndexBackend::from_index_type(&index_file.index_type).is_some()
}

async fn detail_data_ranges_for_table(table: &Table) -> crate::Result<Vec<RowRange>> {
    let plan = table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan()
        .await?;
    let mut ranges = Vec::new();
    for split in plan.splits() {
        for file in split.data_files() {
            if let Some((from, to)) = file.row_id_range() {
                ranges.push(RowRange::new(from, to));
            }
        }
    }
    Ok(merge_row_ranges(ranges))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RawVectorMetric {
    L2,
    Cosine,
    InnerProduct,
}

impl RawVectorMetric {
    fn parse(value: &str) -> crate::Result<Self> {
        Self::parse_normalized(&normalize_metric(value)).ok_or_else(|| crate::Error::DataInvalid {
            message: format!("Unknown vector search metric: {value}"),
            source: None,
        })
    }

    fn parse_normalized(value: &str) -> Option<Self> {
        match value {
            "l2" => Some(Self::L2),
            "cosine" => Some(Self::Cosine),
            "inner_product" => Some(Self::InnerProduct),
            _ => None,
        }
    }

    fn from_lumina(metric: LuminaVectorMetric) -> Self {
        match metric {
            LuminaVectorMetric::L2 => Self::L2,
            LuminaVectorMetric::Cosine => Self::Cosine,
            LuminaVectorMetric::InnerProduct => Self::InnerProduct,
        }
    }

    fn from_vindex(metric: MetricType) -> Self {
        match metric {
            MetricType::L2 => Self::L2,
            MetricType::Cosine => Self::Cosine,
            MetricType::InnerProduct => Self::InnerProduct,
        }
    }
}

fn normalize_metric(metric: &str) -> String {
    metric.to_ascii_lowercase().replace('-', "_")
}

async fn resolve_raw_vector_metric(
    file_io: &FileIO,
    table_path: &str,
    table_options: &HashMap<String, String>,
    index_entries: &[IndexManifestEntry],
    field_id: i32,
    field_name: &str,
) -> crate::Result<RawVectorMetric> {
    for entry in index_entries {
        if entry.kind != FileKind::Add {
            continue;
        }
        let Some(global_meta) = entry.index_file.global_index_meta.as_ref() else {
            continue;
        };
        if global_meta.index_field_id != field_id {
            continue;
        }
        let Some(backend) = VectorIndexBackend::from_index_type(&entry.index_file.index_type)
        else {
            continue;
        };
        match backend {
            VectorIndexBackend::Lumina => {
                if let Some(index_meta) = global_meta.index_meta.as_ref() {
                    if !index_meta.is_empty() {
                        let metric = LuminaIndexMeta::deserialize(index_meta)?.metric()?;
                        return Ok(RawVectorMetric::from_lumina(metric));
                    }
                }
            }
            VectorIndexBackend::Vindex => {
                let path = format!("{table_path}/{INDEX_DIR}/{}", entry.index_file.file_name);
                let input = file_io.new_input(&path)?;
                let bytes = input.read().await.map_err(|e| crate::Error::DataInvalid {
                    message: format!(
                        "Failed to read vindex index file '{}' for raw search metric: {}",
                        entry.index_file.file_name, e
                    ),
                    source: None,
                })?;
                let reader = VIndexReader::open(Cursor::new(bytes.to_vec())).map_err(|e| {
                    crate::Error::DataInvalid {
                        message: format!(
                            "Failed to open paimon-vindex-core reader for raw search metric: {}",
                            e
                        ),
                        source: Some(Box::new(e)),
                    }
                })?;
                return Ok(RawVectorMetric::from_vindex(reader.metadata().metric));
            }
        }
    }

    configured_raw_vector_metric(table_options, field_name)
}

fn configured_raw_vector_metric(
    options: &HashMap<String, String>,
    field_name: &str,
) -> crate::Result<RawVectorMetric> {
    let direct_keys = [
        format!("fields.{field_name}.distance.metric"),
        format!("fields.{field_name}.metric"),
        "test.vector.metric".to_string(),
        "lumina.distance.metric".to_string(),
        "distance.metric".to_string(),
        "metric".to_string(),
    ];
    for key in direct_keys {
        if let Some(value) = options.get(&key) {
            return RawVectorMetric::parse(value);
        }
    }

    let mut inferred = None;
    for (key, value) in options {
        if !(key.ends_with(".distance.metric") || key.ends_with(".metric")) {
            continue;
        }
        let normalized = normalize_metric(value);
        let Some(metric) = RawVectorMetric::parse_normalized(&normalized) else {
            continue;
        };
        if let Some(existing) = inferred {
            if existing != metric {
                return Ok(RawVectorMetric::L2);
            }
        } else {
            inferred = Some(metric);
        }
    }
    Ok(inferred.unwrap_or(RawVectorMetric::L2))
}

async fn read_raw_vector_search(
    table: &Table,
    vector_search: &VectorSearch,
    raw_ranges: &[RowRange],
    metric: RawVectorMetric,
) -> crate::Result<SearchResult> {
    if raw_ranges.is_empty() {
        return Ok(SearchResult::empty());
    }

    let mut read_builder = table.new_read_builder();
    read_builder
        .with_projection(&[vector_search.field_name.as_str(), ROW_ID_FIELD_NAME])
        .with_row_ranges(raw_ranges.to_vec());
    let plan = read_builder.new_scan().plan().await?;
    if plan.splits().is_empty() {
        return Ok(SearchResult::empty());
    }
    let read = read_builder.new_read()?;
    let mut stream = read.to_arrow(plan.splits())?;

    let mut row_ids = Vec::new();
    let mut scores = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        collect_raw_vector_batch(&batch, vector_search, metric, &mut row_ids, &mut scores)?;
    }

    Ok(SearchResult::new(row_ids, scores).top_k(vector_search.limit))
}

fn collect_raw_vector_batch(
    batch: &RecordBatch,
    vector_search: &VectorSearch,
    metric: RawVectorMetric,
    row_ids_out: &mut Vec<u64>,
    scores_out: &mut Vec<f32>,
) -> crate::Result<()> {
    let vector_index = batch
        .schema()
        .index_of(&vector_search.field_name)
        .map_err(|e| crate::Error::DataInvalid {
            message: format!(
                "Vector column '{}' not found in raw search batch: {}",
                vector_search.field_name, e
            ),
            source: None,
        })?;
    let row_id_index =
        batch
            .schema()
            .index_of(ROW_ID_FIELD_NAME)
            .map_err(|e| crate::Error::DataInvalid {
                message: format!("_ROW_ID column not found in raw search batch: {e}"),
                source: None,
            })?;

    let row_ids = batch
        .column(row_id_index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| crate::Error::DataInvalid {
            message: "Vector raw search requires non-null Int64 _ROW_ID".to_string(),
            source: None,
        })?;

    let column = batch.column(vector_index);
    enum VectorLayout<'a> {
        List(&'a ListArray),
        Fixed(&'a FixedSizeListArray),
    }
    let layout = if let Some(a) = column.as_any().downcast_ref::<ListArray>() {
        VectorLayout::List(a)
    } else if let Some(a) = column.as_any().downcast_ref::<FixedSizeListArray>() {
        VectorLayout::Fixed(a)
    } else {
        return Err(crate::Error::DataInvalid {
            message: "Vector raw search requires Arrow List<Float32> or FixedSizeList<Float32>"
                .to_string(),
            source: None,
        });
    };
    let values = match layout {
        VectorLayout::List(a) => a.values(),
        VectorLayout::Fixed(a) => a.values(),
    }
    .as_any()
    .downcast_ref::<Float32Array>()
    .ok_or_else(|| crate::Error::DataInvalid {
        message: "Vector raw search requires Float32 vector elements".to_string(),
        source: None,
    })?;

    for row in 0..batch.num_rows() {
        if row_ids.is_null(row) {
            return Err(crate::Error::DataInvalid {
                message: "Vector raw search found null _ROW_ID".to_string(),
                source: None,
            });
        }
        let row_id = row_id_to_u64(row_ids.value(row))?;
        if vector_search
            .include_row_ids
            .as_ref()
            .is_some_and(|include_row_ids| !include_row_ids.contains(row_id))
        {
            continue;
        }

        let is_null = match layout {
            VectorLayout::List(a) => a.is_null(row),
            VectorLayout::Fixed(a) => a.is_null(row),
        };
        if is_null {
            continue;
        }

        let (start, end) = match layout {
            VectorLayout::List(a) => {
                let offsets = a.value_offsets();
                (offsets[row] as usize, offsets[row + 1] as usize)
            }
            VectorLayout::Fixed(a) => {
                let len = a.value_length() as usize;
                (row * len, (row + 1) * len)
            }
        };
        if end - start != vector_search.vector.len() {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Query vector dimension mismatch: raw row has {}, but query has {}",
                    end - start,
                    vector_search.vector.len()
                ),
                source: None,
            });
        }

        let mut stored = Vec::with_capacity(vector_search.vector.len());
        for value_index in start..end {
            if values.is_null(value_index) {
                return Err(crate::Error::DataInvalid {
                    message: "Vector raw search found null vector element".to_string(),
                    source: None,
                });
            }
            stored.push(values.value(value_index));
        }
        row_ids_out.push(row_id);
        scores_out.push(compute_raw_vector_score(
            &vector_search.vector,
            &stored,
            metric,
        ));
    }

    Ok(())
}

fn row_id_to_u64(row_id: i64) -> crate::Result<u64> {
    u64::try_from(row_id).map_err(|_| crate::Error::DataInvalid {
        message: format!("Negative _ROW_ID {row_id} cannot be used for global index search"),
        source: None,
    })
}

fn compute_raw_vector_score(query: &[f32], stored: &[f32], metric: RawVectorMetric) -> f32 {
    match metric {
        RawVectorMetric::L2 => {
            let sum_sq = query
                .iter()
                .zip(stored.iter())
                .map(|(q, s)| {
                    let diff = q - s;
                    diff * diff
                })
                .sum::<f32>();
            1.0 / (1.0 + sum_sq)
        }
        RawVectorMetric::Cosine => {
            let mut dot = 0.0;
            let mut norm_a = 0.0;
            let mut norm_b = 0.0;
            for (q, s) in query.iter().zip(stored.iter()) {
                dot += q * s;
                norm_a += q * q;
                norm_b += s * s;
            }
            let denominator = norm_a.sqrt() * norm_b.sqrt();
            if denominator == 0.0 {
                0.0
            } else {
                dot / denominator
            }
        }
        RawVectorMetric::InnerProduct => query.iter().zip(stored.iter()).map(|(q, s)| q * s).sum(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lumina::{LEGACY_LUMINA_VECTOR_ANN_IDENTIFIER, LUMINA_IDENTIFIER};
    use crate::spec::{DataType, GlobalIndexMeta, IndexFileMeta, IndexManifestEntry, IntType};
    use crate::vindex::IVF_FLAT_IDENTIFIER;

    fn make_field(id: i32, name: &str) -> DataField {
        DataField::new(id, name.to_string(), DataType::Int(IntType::default()))
    }

    fn eval_context<'a>(
        file_io: &'a FileIO,
        options: &'a HashMap<String, String>,
        fields: &'a [DataField],
        next_row_id: Option<i64>,
    ) -> VectorSearchEvaluation<'a> {
        VectorSearchEvaluation {
            table: None,
            file_io,
            table_path: "memory:///test_table",
            table_options: options,
            schema_fields: fields,
            next_row_id,
        }
    }

    #[test]
    fn test_find_field_id_by_name() {
        let fields = vec![make_field(1, "id"), make_field(2, "embedding")];
        assert_eq!(find_field_id_by_name(&fields, "embedding"), Some(2));
        assert_eq!(find_field_id_by_name(&fields, "nonexistent"), None);
    }

    #[test]
    fn test_raw_vector_score_matches_java_metric_semantics() {
        let l2 = compute_raw_vector_score(&[1.0, 2.0], &[1.0, 4.0], RawVectorMetric::L2);
        assert!((l2 - 0.2).abs() < 1e-6);
        assert_eq!(
            compute_raw_vector_score(&[1.0, 2.0], &[3.0, 4.0], RawVectorMetric::InnerProduct),
            11.0
        );
        let cosine = compute_raw_vector_score(&[1.0, 0.0], &[1.0, 1.0], RawVectorMetric::Cosine);
        assert!((cosine - std::f32::consts::FRAC_1_SQRT_2).abs() < 1e-6);
        assert_eq!(
            compute_raw_vector_score(&[0.0, 0.0], &[1.0, 1.0], RawVectorMetric::Cosine),
            0.0
        );
    }

    #[test]
    fn test_configured_raw_vector_metric_precedence_and_conflict_default() {
        let mut options = HashMap::new();
        options.insert(
            "fields.embedding.distance.metric".to_string(),
            "inner-product".to_string(),
        );
        options.insert("metric".to_string(), "cosine".to_string());
        assert_eq!(
            configured_raw_vector_metric(&options, "embedding").unwrap(),
            RawVectorMetric::InnerProduct
        );

        options.clear();
        options.insert("foo.metric".to_string(), "cosine".to_string());
        options.insert("bar.distance.metric".to_string(), "l2".to_string());
        assert_eq!(
            configured_raw_vector_metric(&options, "embedding").unwrap(),
            RawVectorMetric::L2
        );
    }

    #[tokio::test]
    async fn test_evaluate_no_matching_entries() {
        let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let fields = vec![make_field(1, "id"), make_field(2, "embedding")];
        let vs = VectorSearch::new(vec![1.0, 2.0], 10, "embedding".to_string()).unwrap();
        let options = HashMap::new();

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
            eval_context(&file_io, &options, &fields, None),
            &[entry],
            &vs,
        )
        .await
        .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_evaluate_ignores_non_vector_index_type() {
        let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let fields = vec![make_field(2, "embedding")];
        let vs = VectorSearch::new(vec![1.0], 10, "embedding".to_string()).unwrap();
        let options = HashMap::new();

        let entry = make_lumina_entry("test.idx", "btree", FileKind::Add, 2);

        let result = evaluate_vector_search(
            eval_context(&file_io, &options, &fields, None),
            &[entry],
            &vs,
        )
        .await
        .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_evaluate_full_mode_without_vector_entries_uses_raw_path() {
        let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let fields = vec![make_field(2, "embedding")];
        let vs = VectorSearch::new(vec![1.0], 10, "embedding".to_string()).unwrap();
        let options = HashMap::from([("global-index.search-mode".to_string(), "full".to_string())]);

        let err = evaluate_vector_search(
            eval_context(&file_io, &options, &fields, Some(10)),
            &[],
            &vs,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("Vector raw search requires table context"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_evaluate_no_matching_field() {
        let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let fields = vec![make_field(1, "id")];
        let vs = VectorSearch::new(vec![1.0], 10, "embedding".to_string()).unwrap();
        let options = HashMap::new();

        let entry = make_lumina_entry(
            "test.idx",
            LEGACY_LUMINA_VECTOR_ANN_IDENTIFIER,
            FileKind::Add,
            99,
        );

        let result = evaluate_vector_search(
            eval_context(&file_io, &options, &fields, None),
            &[entry],
            &vs,
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
        let options = HashMap::new();

        let entry = make_lumina_entry(
            "test.idx",
            LEGACY_LUMINA_VECTOR_ANN_IDENTIFIER,
            FileKind::Delete,
            2,
        );

        let result = evaluate_vector_search(
            eval_context(&file_io, &options, &fields, None),
            &[entry],
            &vs,
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
        let options = HashMap::new();

        let entry = make_lumina_entry("missing.idx", LUMINA_IDENTIFIER, FileKind::Add, 2);

        let err = evaluate_vector_search(
            eval_context(&file_io, &options, &fields, None),
            &[entry],
            &vs,
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
        let options = HashMap::new();

        let entry = make_lumina_entry(
            "missing.idx",
            LEGACY_LUMINA_VECTOR_ANN_IDENTIFIER,
            FileKind::Add,
            2,
        );

        let err = evaluate_vector_search(
            eval_context(&file_io, &options, &fields, None),
            &[entry],
            &vs,
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
    async fn test_evaluate_accepts_vindex_index_type() {
        let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let fields = vec![make_field(2, "embedding")];
        let vs = VectorSearch::new(vec![1.0], 10, "embedding".to_string()).unwrap();
        let options = HashMap::new();

        let entry = make_lumina_entry("missing.idx", IVF_FLAT_IDENTIFIER, FileKind::Add, 2);

        let err = evaluate_vector_search(
            eval_context(&file_io, &options, &fields, None),
            &[entry],
            &vs,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("Failed to read vindex index file 'missing.idx'"),
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
