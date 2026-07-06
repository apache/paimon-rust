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
use crate::table::global_index_scanner::{
    deleted_row_ranges_for_data_evolution_dvs, search_limit_with_deleted_rows,
    unindexed_ranges_for_global_index_entries, RowRangeIndex,
};
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

pub struct BatchVectorSearchBuilder<'a> {
    table: &'a Table,
    vector_column: Option<String>,
    query_vectors: Option<Vec<Vec<f32>>>,
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
        // Fail closed: returns data-derived row ranges outside `TableScan`/`TableRead`.
        CoreOptions::new(self.table.schema().options()).ensure_read_authorized()?;
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

        let mut batch_builder = BatchVectorSearchBuilder::new(self.table);
        let mut results = batch_builder
            .with_vector_column(vector_column)
            .with_query_vectors(vec![query_vector.clone()])
            .with_limit(limit)
            .execute()
            .await?;

        debug_assert_eq!(results.len(), 1);
        results.remove(0).to_row_ranges()
    }
}

impl<'a> BatchVectorSearchBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            vector_column: None,
            query_vectors: None,
            limit: None,
        }
    }

    pub fn with_vector_column(&mut self, name: &str) -> &mut Self {
        self.vector_column = Some(name.to_string());
        self
    }

    pub fn with_query_vectors(&mut self, vectors: Vec<Vec<f32>>) -> &mut Self {
        self.query_vectors = Some(vectors);
        self
    }

    pub fn with_limit(&mut self, limit: usize) -> &mut Self {
        self.limit = Some(limit);
        self
    }

    pub async fn execute(&self) -> crate::Result<Vec<SearchResult>> {
        let vector_column =
            self.vector_column
                .as_deref()
                .ok_or_else(|| crate::Error::ConfigInvalid {
                    message: "Vector column must be set via with_vector_column()".to_string(),
                })?;
        if vector_column.is_empty() {
            return Err(crate::Error::ConfigInvalid {
                message: "Vector column must be set via with_vector_column()".to_string(),
            });
        }

        let query_vectors =
            self.query_vectors
                .as_ref()
                .ok_or_else(|| crate::Error::ConfigInvalid {
                    message: "Query vectors must be set via with_query_vectors()".to_string(),
                })?;
        if query_vectors.is_empty() {
            return Err(crate::Error::ConfigInvalid {
                message: "Query vectors must be set via with_query_vectors()".to_string(),
            });
        }

        let limit = self.limit.ok_or_else(|| crate::Error::ConfigInvalid {
            message: "Limit must be set via with_limit()".to_string(),
        })?;

        let vector_searches = query_vectors
            .iter()
            .map(|vector| VectorSearch::new(vector.clone(), limit, vector_column.to_string()))
            .collect::<crate::Result<Vec<_>>>()?;

        let snapshot_manager = SnapshotManager::new(
            self.table.file_io().clone(),
            self.table.location().to_string(),
        );

        let snapshot = match snapshot_manager.get_latest_snapshot().await? {
            Some(s) => s,
            None => return Ok(vec![SearchResult::empty(); vector_searches.len()]),
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

        evaluate_batch_vector_search(
            VectorSearchEvaluation {
                table: Some(self.table),
                file_io: self.table.file_io(),
                table_path: self.table.location(),
                table_options: self.table.schema().options(),
                schema_fields: self.table.schema().fields(),
                next_row_id: snapshot.next_row_id(),
            },
            &index_entries,
            &vector_searches,
        )
        .await
    }
}

#[derive(Clone, Copy)]
struct VectorSearchEvaluation<'a> {
    table: Option<&'a Table>,
    file_io: &'a FileIO,
    table_path: &'a str,
    table_options: &'a HashMap<String, String>,
    schema_fields: &'a [DataField],
    next_row_id: Option<i64>,
}

#[cfg(test)]
async fn evaluate_vector_search(
    evaluation: VectorSearchEvaluation<'_>,
    index_entries: &[IndexManifestEntry],
    vector_search: &VectorSearch,
) -> crate::Result<Vec<RowRange>> {
    let mut results = evaluate_batch_vector_search(
        evaluation,
        index_entries,
        std::slice::from_ref(vector_search),
    )
    .await?;
    debug_assert_eq!(results.len(), 1);
    results.remove(0).to_row_ranges()
}

async fn evaluate_batch_vector_search(
    evaluation: VectorSearchEvaluation<'_>,
    index_entries: &[IndexManifestEntry],
    vector_searches: &[VectorSearch],
) -> crate::Result<Vec<SearchResult>> {
    if vector_searches.is_empty() {
        return Ok(Vec::new());
    }

    let table_path = evaluation.table_path.trim_end_matches('/');
    let core_options = CoreOptions::new(evaluation.table_options);
    let search_mode = core_options.global_index_search_mode()?;
    let field_name = &vector_searches[0].field_name;
    if vector_searches
        .iter()
        .any(|vector_search| vector_search.field_name != *field_name)
    {
        return Err(crate::Error::DataInvalid {
            message: "Batch vector search requires all query vectors to use the same field"
                .to_string(),
            source: None,
        });
    }

    let field_id = match find_field_id_by_name(evaluation.schema_fields, field_name) {
        Some(id) => id,
        None => return Ok(vec![SearchResult::empty(); vector_searches.len()]),
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
        return Ok(vec![SearchResult::empty(); vector_searches.len()]);
    }

    let deleted_row_index = if core_options.data_evolution_enabled() {
        match evaluation.table {
            Some(table) => {
                let ranges =
                    deleted_row_ranges_for_data_evolution_dvs(table, index_entries).await?;
                (!ranges.is_empty()).then(|| RowRangeIndex::create(ranges))
            }
            None => None,
        }
    } else {
        None
    };

    let mut merged = vec![SearchResult::empty(); vector_searches.len()];
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
                let row_range_end = global_meta.row_range_end;
                let max_limit = vector_searches
                    .iter()
                    .map(|vector_search| vector_search.limit)
                    .max()
                    .unwrap_or(0);
                let index_limit = search_limit_with_deleted_rows(
                    max_limit,
                    row_range_start,
                    row_range_end,
                    deleted_row_index.as_ref(),
                );
                let mut vector_searches = vector_searches.to_vec();
                for vector_search in &mut vector_searches {
                    vector_search.limit = index_limit;
                }
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
                    let results = match backend {
                        VectorIndexBackend::Lumina => {
                            let mut reader = LuminaVectorGlobalIndexReader::new(io_meta, options);
                            reader.visit_batch_vector_search(&vector_searches, |_| {
                                Ok(Cursor::new(data))
                            })?
                        }
                        VectorIndexBackend::Vindex => {
                            let mut reader = VindexVectorGlobalIndexReader::new(io_meta, options);
                            reader.visit_batch_vector_search(&vector_searches, |_| {
                                Ok(Cursor::new(data))
                            })?
                        }
                    };
                    if results.len() != vector_searches.len() {
                        return Err(crate::Error::DataInvalid {
                            message: format!(
                                "Batch vector search backend returned {} results for {} query vectors",
                                results.len(),
                                vector_searches.len()
                            ),
                            source: None,
                        });
                    }

                    Ok::<_, crate::Error>(
                        results
                            .into_iter()
                            .map(|result| match result {
                                Some(scored_map) => SearchResult::from_scored_map(scored_map)
                                    .offset(row_range_start),
                                None => SearchResult::empty(),
                            })
                            .collect::<Vec<_>>(),
                    )
                }
            })
            .collect();

        let results = futures::future::try_join_all(futures).await?;
        for per_entry in &results {
            for (query_index, result) in per_entry.iter().enumerate() {
                merged[query_index] = merged[query_index].or(result);
            }
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
                field_name,
            )
            .await?;
            let raw_results =
                read_raw_batch_vector_search(table, vector_searches, &raw_ranges, metric).await?;
            for (query_index, result) in raw_results.iter().enumerate() {
                merged[query_index] = merged[query_index].or(result);
            }
        }
    }

    merged
        .into_iter()
        .zip(vector_searches)
        .map(|(result, vector_search)| {
            Ok(result
                .without_deleted_row_ranges(deleted_row_index.as_ref())?
                .top_k(vector_search.limit))
        })
        .collect()
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

async fn read_raw_batch_vector_search(
    table: &Table,
    vector_searches: &[VectorSearch],
    raw_ranges: &[RowRange],
    metric: RawVectorMetric,
) -> crate::Result<Vec<SearchResult>> {
    if vector_searches.is_empty() {
        return Ok(Vec::new());
    }
    if raw_ranges.is_empty() {
        return Ok(vec![SearchResult::empty(); vector_searches.len()]);
    }

    let field_name = &vector_searches[0].field_name;
    if vector_searches
        .iter()
        .any(|vector_search| vector_search.field_name != *field_name)
    {
        return Err(crate::Error::DataInvalid {
            message: "Batch vector raw search requires all query vectors to use the same field"
                .to_string(),
            source: None,
        });
    }

    let mut read_builder = table.new_read_builder();
    read_builder
        .with_projection(&[field_name.as_str(), ROW_ID_FIELD_NAME])?
        .with_row_ranges(raw_ranges.to_vec());
    let plan = read_builder.new_scan().plan().await?;
    if plan.splits().is_empty() {
        return Ok(vec![SearchResult::empty(); vector_searches.len()]);
    }
    let read = read_builder.new_read()?;
    let mut stream = read.to_arrow(plan.splits())?;

    let mut row_ids = vec![Vec::new(); vector_searches.len()];
    let mut scores = vec![Vec::new(); vector_searches.len()];
    while let Some(batch) = stream.try_next().await? {
        collect_raw_batch_vector_batch(&batch, vector_searches, metric, &mut row_ids, &mut scores)?;
    }

    Ok(row_ids
        .into_iter()
        .zip(scores)
        .zip(vector_searches)
        .map(|((row_ids, scores), vector_search)| {
            SearchResult::new(row_ids, scores).top_k(vector_search.limit)
        })
        .collect())
}

fn collect_raw_batch_vector_batch(
    batch: &RecordBatch,
    vector_searches: &[VectorSearch],
    metric: RawVectorMetric,
    row_ids_out: &mut [Vec<u64>],
    scores_out: &mut [Vec<f32>],
) -> crate::Result<()> {
    if vector_searches.is_empty() {
        return Ok(());
    }
    if row_ids_out.len() != vector_searches.len() || scores_out.len() != vector_searches.len() {
        return Err(crate::Error::DataInvalid {
            message: "Raw batch vector search output buffers must match query vector count"
                .to_string(),
            source: None,
        });
    }

    let field_name = &vector_searches[0].field_name;
    if vector_searches
        .iter()
        .any(|vector_search| vector_search.field_name != *field_name)
    {
        return Err(crate::Error::DataInvalid {
            message: "Batch vector raw search requires all query vectors to use the same field"
                .to_string(),
            source: None,
        });
    }

    let vector_index =
        batch
            .schema()
            .index_of(field_name)
            .map_err(|e| crate::Error::DataInvalid {
                message: format!(
                    "Vector column '{}' not found in raw search batch: {}",
                    field_name, e
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

        let mut stored = Vec::with_capacity(end - start);
        for value_index in start..end {
            if values.is_null(value_index) {
                return Err(crate::Error::DataInvalid {
                    message: "Vector raw search found null vector element".to_string(),
                    source: None,
                });
            }
            stored.push(values.value(value_index));
        }

        for (query_index, vector_search) in vector_searches.iter().enumerate() {
            if vector_search
                .include_row_ids
                .as_ref()
                .is_some_and(|include_row_ids| !include_row_ids.contains(row_id))
            {
                continue;
            }
            if stored.len() != vector_search.vector.len() {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Query vector dimension mismatch: raw row has {}, but query has {}",
                        stored.len(),
                        vector_search.vector.len()
                    ),
                    source: None,
                });
            }
            row_ids_out[query_index].push(row_id);
            scores_out[query_index].push(compute_raw_vector_score(
                &vector_search.vector,
                &stored,
                metric,
            ));
        }
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
    use crate::catalog::Identifier;
    use crate::io::FileIOBuilder;
    use crate::lumina::{LEGACY_LUMINA_VECTOR_ANN_IDENTIFIER, LUMINA_IDENTIFIER};
    use crate::spec::{
        ArrayType, DataType, FloatType, GlobalIndexMeta, IndexFileMeta, IndexManifestEntry,
        IntType, Schema, TableSchema,
    };
    use crate::vindex::IVF_FLAT_IDENTIFIER;
    use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
    use arrow_array::ArrayRef;
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use std::sync::Arc;

    fn make_field(id: i32, name: &str) -> DataField {
        DataField::new(id, name.to_string(), DataType::Int(IntType::default()))
    }

    fn vector_test_table() -> Table {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column(
                "embedding",
                DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
            )
            .build()
            .unwrap();
        Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("default", "vector_test"),
            "memory:/vector_test".to_string(),
            TableSchema::new(0, &schema),
            None,
        )
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

    #[test]
    fn test_collect_raw_batch_vector_batch_preserves_query_order() {
        let element_field = Arc::new(ArrowField::new("element", ArrowDataType::Float32, true));
        let mut builder =
            FixedSizeListBuilder::new(Float32Builder::new(), 2).with_field(element_field);
        for vector in [[1.0, 0.0], [0.0, 1.0], [0.8, 0.2]] {
            builder.values().append_value(vector[0]);
            builder.values().append_value(vector[1]);
            builder.append(true);
        }
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new(
                "embedding",
                ArrowDataType::FixedSizeList(
                    Arc::new(ArrowField::new("element", ArrowDataType::Float32, true)),
                    2,
                ),
                true,
            ),
            ArrowField::new(ROW_ID_FIELD_NAME, ArrowDataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(builder.finish()) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(10), Some(11), Some(12)])) as ArrayRef,
            ],
        )
        .unwrap();
        let searches = vec![
            VectorSearch::new(vec![1.0, 0.0], 1, "embedding".to_string()).unwrap(),
            VectorSearch::new(vec![0.0, 1.0], 1, "embedding".to_string()).unwrap(),
        ];
        let mut row_ids = vec![Vec::new(); searches.len()];
        let mut scores = vec![Vec::new(); searches.len()];

        collect_raw_batch_vector_batch(
            &batch,
            &searches,
            RawVectorMetric::L2,
            &mut row_ids,
            &mut scores,
        )
        .unwrap();
        let results = row_ids
            .into_iter()
            .zip(scores)
            .map(|(row_ids, scores)| SearchResult::new(row_ids, scores).top_k(1))
            .collect::<Vec<_>>();

        assert_eq!(results[0].row_ids, vec![10]);
        assert_eq!(results[1].row_ids, vec![11]);
    }

    #[tokio::test]
    async fn test_batch_vector_search_requires_vectors() {
        let table = vector_test_table();
        let err = table
            .new_batch_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vectors(Vec::new())
            .with_limit(1)
            .execute()
            .await
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("Query vectors must be set via with_query_vectors()"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_batch_vector_search_rejects_zero_limit() {
        let table = vector_test_table();
        let err = table
            .new_batch_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vectors(vec![vec![1.0]])
            .with_limit(0)
            .execute()
            .await
            .unwrap_err();

        assert!(
            err.to_string().contains("Limit must be between 1"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_batch_evaluate_no_matching_field_returns_empty_per_query() {
        let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let fields = vec![make_field(1, "id")];
        let searches = vec![
            VectorSearch::new(vec![1.0], 10, "embedding".to_string()).unwrap(),
            VectorSearch::new(vec![0.0], 10, "embedding".to_string()).unwrap(),
        ];
        let options = HashMap::new();

        let entry = make_lumina_entry(
            "test.idx",
            LEGACY_LUMINA_VECTOR_ANN_IDENTIFIER,
            FileKind::Add,
            99,
        );

        let results = evaluate_batch_vector_search(
            eval_context(&file_io, &options, &fields, None),
            &[entry],
            &searches,
        )
        .await
        .unwrap();

        assert_eq!(results.len(), searches.len());
        assert!(results.iter().all(SearchResult::is_empty));
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

    #[tokio::test]
    async fn test_execute_fails_closed_when_query_auth_enabled() {
        let table = crate::table::query_auth_table();
        let err = table
            .new_vector_search_builder()
            .execute()
            .await
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::Unsupported { ref message } if message.contains("query-auth.enabled")),
            "vector search must fail closed for a query-auth table"
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
