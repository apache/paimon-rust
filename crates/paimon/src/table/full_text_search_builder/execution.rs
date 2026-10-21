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

//! Serializable full-text shard work; global filtering and ranking stay in core.

use super::*;
use crate::table::search_task::{execute_tasks, SEARCH_TASK_WINDOW};
use crate::table::{DataSplit, SearchTask, SearchTaskExecutor};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const INDEX: &str = "fulltext-index";
const ROWS: &str = "fulltext-rows";

#[derive(Serialize, Deserialize)]
struct IndexWork {
    entry: IndexManifestEntry,
    query: String,
    local_filter: Option<Vec<u8>>,
}

#[derive(Serialize, Deserialize)]
struct RowsWork {
    query: String,
    field: String,
    limit: usize,
    include_row_ids: Option<Vec<u8>>,
    ranges: Vec<RowRange>,
    splits: Vec<DataSplit>,
    options: HashMap<String, String>,
}

#[derive(Serialize, Deserialize)]
struct Hits {
    row_ids: Vec<u64>,
    scores: Vec<f32>,
}

impl Hits {
    fn from_search(result: SearchResult) -> Self {
        Self {
            row_ids: result.row_ids,
            scores: result.scores,
        }
    }

    fn into_search(self) -> crate::Result<SearchResult> {
        if self.row_ids.len() != self.scores.len() || self.scores.iter().any(|s| !s.is_finite()) {
            return Err(invalid("Full-text task returned invalid row IDs or scores"));
        }
        Ok(SearchResult::new(self.row_ids, self.scores))
    }
}

fn invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

fn encode<T: Serialize>(value: T) -> crate::Result<Value> {
    serde_json::to_value(value)
        .map_err(|error| invalid(format!("Cannot encode full-text task: {error}")))
}

fn decode<T: serde::de::DeserializeOwned>(value: &Value) -> crate::Result<T> {
    serde_json::from_value(value.clone())
        .map_err(|error| invalid(format!("Invalid full-text task payload: {error}")))
}

fn encode_bitmap(ids: &RoaringTreemap) -> crate::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    ids.serialize_into(&mut bytes)
        .map_err(|error| invalid(format!("Cannot encode full-text task row IDs: {error}")))?;
    Ok(bytes)
}

fn decode_bitmap(bytes: Vec<u8>) -> crate::Result<RoaringTreemap> {
    RoaringTreemap::deserialize_from(bytes.as_slice())
        .map_err(|error| invalid(format!("Invalid full-text task row IDs: {error}")))
}

pub(super) async fn search_indexes(
    table: &Table,
    plans: Vec<FullTextIndexSearchPlan>,
    query: &str,
    executor: &dyn SearchTaskExecutor,
) -> crate::Result<Vec<SearchResult>> {
    let mut plans = plans.into_iter();
    let mut results = Vec::new();
    loop {
        // Materialize only the next window's query and bitmap payloads. A large
        // index manifest must not retain a serialized task for every shard.
        let tasks = plans
            .by_ref()
            .take(SEARCH_TASK_WINDOW)
            .map(|plan| {
                SearchTask::new(
                    table,
                    INDEX,
                    encode(IndexWork {
                        entry: plan.entry,
                        query: query.to_string(),
                        local_filter: plan.local_filter.as_ref().map(encode_bitmap).transpose()?,
                    })?,
                )
            })
            .collect::<crate::Result<Vec<_>>>()?;
        if tasks.is_empty() {
            break;
        }
        for value in execute_tasks(executor, tasks).await? {
            results.push(decode::<Hits>(&value)?.into_search()?);
        }
    }
    Ok(results)
}

pub(super) async fn search_raw_rows(
    table: &Table,
    search: &FullTextSearch,
    ranges: &[RowRange],
    options: &HashMap<String, String>,
    partition_filter: Option<&Predicate>,
    executor: &dyn SearchTaskExecutor,
) -> crate::Result<SearchResult> {
    let splits = plan_raw_full_text_splits(table, search, ranges, partition_filter).await?;
    if splits.is_empty() {
        return Ok(SearchResult::empty());
    }
    // A full-text raw fallback is one BM25 corpus, even when the scan has many
    // data splits. Rebuilding one index per split changes relevance scores.
    let task = SearchTask::new(
        table,
        ROWS,
        encode(RowsWork {
            query: search.query_text.clone(),
            field: search.field_name.clone(),
            limit: search.limit,
            include_row_ids: search
                .include_row_ids
                .as_ref()
                .map(encode_bitmap)
                .transpose()?,
            ranges: ranges.to_vec(),
            splits,
            options: options.clone(),
        })?,
    )?;
    let mut values = execute_tasks(executor, vec![task]).await?;
    decode::<Hits>(&values.remove(0))?.into_search()
}

pub(crate) async fn execute_search_task(
    table: &Table,
    kind: &str,
    payload: &Value,
) -> crate::Result<Value> {
    match kind {
        INDEX => {
            let work: IndexWork = decode(payload)?;
            let meta = work
                .entry
                .index_file
                .global_index_meta
                .as_ref()
                .ok_or_else(|| invalid("Full-text index task is missing global index metadata"))?;
            let limit = candidate_limit(meta.row_range_start, meta.row_range_end)?;
            let path = IndexFileLocation::Global {
                table_path: table.location().trim_end_matches('/'),
            }
            .resolve(
                &work.entry.index_file.file_name,
                work.entry.index_file.external_path.as_deref(),
            );
            let input = table.file_io().new_input(&path)?;
            let reader = Box::new(input.reader().await?);
            let result = search_full_text_index(
                reader,
                work.entry.index_file.file_name.clone(),
                work.query,
                limit,
                work.local_filter.map(decode_bitmap).transpose()?,
            )
            .await?;
            encode(Hits::from_search(
                search_result_from_core(result)?.offset(meta.row_range_start),
            ))
        }
        ROWS => {
            let work: RowsWork = decode(payload)?;
            let mut search = FullTextSearch::new(work.query, work.limit, work.field)?;
            search.include_row_ids = work.include_row_ids.map(decode_bitmap).transpose()?;
            let result = read_raw_full_text_splits(
                table,
                &search,
                &work.ranges,
                &work.options,
                &work.splits,
                // Partition pruning already selected these exact source splits.
                None,
            )
            .await?;
            encode(Hits::from_search(result))
        }
        _ => Err(invalid(format!(
            "Unknown full-text search task kind: {kind}"
        ))),
    }
}
