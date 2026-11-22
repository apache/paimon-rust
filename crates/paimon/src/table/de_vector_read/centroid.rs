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

use super::{
    execute_vector_file_work, IndexSearchTiming, RangeReadLimiter, RawVectorMetric,
    VectorIndexWork, VectorMetricCheck, VectorSearchEvaluation,
};
use crate::spec::IndexManifestEntry;
use crate::table::centroid_index::{self, Descriptor, Role, Router};
use crate::vector_search::ScoredRowIds;
use crate::{Error, Result};
use paimon_vindex_core::distance::MetricType;
use std::collections::{BTreeMap, HashMap};

type CentroidFiles<'a> = HashMap<&'a str, Vec<IndexManifestEntry>>;

pub(super) fn plan<'a>(
    entries: &[&'a IndexManifestEntry],
) -> Result<(Vec<&'a IndexManifestEntry>, CentroidFiles<'a>)> {
    let mut plans = entries
        .iter()
        .copied()
        .filter(|entry| !Descriptor::is_wrapped(&entry.index_file))
        .collect::<Vec<_>>();
    let mut files = HashMap::new();
    for group in centroid_index::group(entries.iter().copied())? {
        if group.vector_count != 0 {
            plans.push(group.router);
            files.insert(
                group.router.index_file.file_name.as_str(),
                group.files.into_iter().cloned().collect(),
            );
        }
    }
    Ok((plans, files))
}

struct Route {
    nearest: Vec<usize>,
    visited: usize,
    width: usize,
    limit: usize,
    hits: ScoredRowIds,
}

pub(super) async fn execute_work(
    evaluation: VectorSearchEvaluation<'_>,
    work: VectorIndexWork,
    metric_check: VectorMetricCheck,
    range_read_limiter: RangeReadLimiter,
    batch_parallelism: usize,
) -> Result<(Vec<ScoredRowIds>, IndexSearchTiming)> {
    if work.centroid_files.is_empty() {
        if Descriptor::is_wrapped(&work.entry.index_file) {
            return Err(centroid_index::invalid(
                "Expected a complete centroid generation",
            ));
        }
        return execute_vector_file_work(
            evaluation,
            work,
            metric_check,
            range_read_limiter,
            batch_parallelism,
        )
        .await;
    }

    if work.filter.is_some()
        && (work.top_ks.len() != work.queries.len() || work.top_ks.contains(&0))
    {
        return Err(centroid_index::invalid(
            "Post-filtering requires each original Top-K",
        ));
    }
    let groups = centroid_index::group(&work.centroid_files)?;
    let [group] = groups.as_slice() else {
        return Err(centroid_index::invalid(
            "Expected a single centroid generation",
        ));
    };
    if group.router != &work.entry {
        return Err(centroid_index::invalid(
            "Centroid work does not match its router",
        ));
    }
    let Role::Router(nlist) = group.descriptor.role else {
        unreachable!()
    };
    if nlist == 0 {
        return Ok((
            vec![ScoredRowIds::empty(); work.queries.len()],
            IndexSearchTiming::default(),
        ));
    }
    let configuration = work
        .resolved
        .as_ref()
        .ok_or_else(|| centroid_index::invalid("Missing configuration for centroid search"))?;
    let explicit_width = configuration
        .options
        .get("centroid.nprobe")
        .map(|value| {
            value
                .parse::<i32>()
                .ok()
                .filter(|value| *value > 0)
                .map(|width| (width as usize).min(nlist))
                .ok_or_else(|| Error::ConfigInvalid {
                    message: "'centroid.nprobe' must be a positive integer".into(),
                })
        })
        .transpose()?;
    if work.entry.index_file.index_type == "diskann"
        && configuration.options.contains_key("ivf.nprobe")
    {
        return Err(Error::ConfigInvalid {
            message: "Use 'centroid.nprobe' and 'diskann.l_search' for centroid-sharded DiskANN"
                .into(),
        });
    }
    let router = Router::load(
        evaluation.file_io,
        evaluation.table_path.trim_end_matches('/'),
        &group.router.index_file,
        &group.descriptor,
    )
    .await?;
    metric_check.check(RawVectorMetric::from_vindex(router.metric))?;
    if work
        .queries
        .iter()
        .any(|query| query.vector.len() != router.dimension)
    {
        return Err(centroid_index::invalid(format!(
            "Query vector dimension mismatch: centroid router expects {}",
            router.dimension
        )));
    }

    let mut routes = Vec::with_capacity(work.queries.len());
    for search in &work.queries {
        let matching = search.effective_include_row_ids().map(|ids| ids.len());
        let limit = matching
            .unwrap_or(u64::MAX)
            .min(group.vector_count)
            .min(search.limit as u64) as usize;
        let width = if limit == 0 {
            0
        } else {
            explicit_width.unwrap_or_else(|| {
                centroid_index::automatic_width(
                    nlist,
                    group.vector_count,
                    search.limit,
                    matching,
                    router.metric,
                )
            })
        };
        let nearest = if width == 0 {
            vec![]
        } else if explicit_width.is_none() && router.metric == MetricType::InnerProduct {
            (0..nlist).collect()
        } else {
            router.nearest(&search.vector)?
        };
        routes.push(Route {
            nearest,
            visited: 0,
            width,
            limit,
            hits: ScoredRowIds::empty(),
        });
    }

    let mut timing = IndexSearchTiming::default();
    loop {
        let mut queries_by_centroid = BTreeMap::<_, Vec<_>>::new();
        for (index, route) in routes.iter_mut().enumerate() {
            for &centroid in &route.nearest[route.visited..route.width] {
                if group.shards[&centroid].index_file.row_count > 0 {
                    queries_by_centroid.entry(centroid).or_default().push(index);
                }
            }
            route.visited = route.width;
        }
        for (centroid, indices) in queries_by_centroid {
            let (results, stats) = execute_vector_file_work(
                evaluation,
                VectorIndexWork {
                    entry: group.shards[&centroid].clone(),
                    centroid_files: vec![],
                    queries: indices
                        .iter()
                        .map(|&index| work.queries[index].clone())
                        .collect(),
                    resolved: work.resolved.clone(),
                    filter: work.filter.clone(),
                    top_ks: if work.filter.is_some() {
                        indices.iter().map(|&index| work.top_ks[index]).collect()
                    } else {
                        vec![]
                    },
                },
                metric_check.clone(),
                range_read_limiter.clone(),
                batch_parallelism,
            )
            .await?;
            timing.permit_wait = timing.permit_wait.saturating_add(stats.permit_wait);
            timing.file_reader_open = timing
                .file_reader_open
                .saturating_add(stats.file_reader_open);
            for (index, result) in indices.into_iter().zip(results) {
                let route = &mut routes[index];
                route.hits = route.hits.or(&result).top_k(route.limit);
            }
        }

        let mut expand = false;
        if explicit_width.is_none() {
            for route in &mut routes {
                if route.width > 0 && route.width < nlist && route.hits.row_ids.len() < route.limit
                {
                    route.width = route.width.saturating_mul(2).min(nlist);
                    expand = true;
                }
            }
        }
        if !expand {
            break;
        }
    }
    Ok((routes.into_iter().map(|route| route.hits).collect(), timing))
}
