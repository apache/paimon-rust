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
    execute_vector_file_probes, execute_vector_file_work, matching_row_ids_for_filter, post_filter,
    sorted_row_ids_to_row_ranges, IndexSearchTiming, RangeReadLimiter, RawVectorMetric,
    VectorIndexWork, VectorMetricCheck, VectorSearchEvaluation,
};
use crate::spec::{IndexManifestEntry, VectorIndexOptions};
use crate::table::centroid_bitmap::BitmapPredicate;
use crate::table::centroid_index::{self, Descriptor, Generation, Role, Router};
use crate::table::RowRange;
use crate::vector_search::{ScoredRowIds, VectorSearch};
use crate::vindex::reader::VindexVectorGlobalIndexReader;
use crate::{Error, Result};
use paimon_vindex_core::distance::MetricType;
use paimon_vindex_core::index::VectorIndexMetadata;
use paimon_vindex_core::io::SeekRead;
use roaring::RoaringTreemap;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

type CentroidFiles<'a> = HashMap<&'a str, Generation<'a>>;

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
            files.insert(group.router.index_file.file_name.as_str(), group);
        }
    }
    Ok((plans, files))
}

const ROUTER_LISTS: usize = 64;
const AUTOMATIC_WIDTH: usize = 8;
const JOINT_OUTER_PROBES: usize = 4;
const JOINT_INNER_PROBES: usize = 16;
const SHARD_LISTS: usize = 64;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Probe {
    Standard,
    Joint,
    CheckLayout,
}

#[derive(Clone, Default)]
pub(super) struct ProbeMatches<T> {
    pub(super) standard: T,
    // A legacy shard reports Some only when its native layout supports joint probing.
    pub(super) joint: Option<T>,
}

impl<T> ProbeMatches<T> {
    pub(super) fn standard(standard: T) -> Self {
        Self {
            standard,
            joint: None,
        }
    }
}

pub(super) type NativeMatches = Option<HashMap<u64, f32>>;

pub(super) fn search_file<S: SeekRead + 'static>(
    reader: &mut VindexVectorGlobalIndexReader,
    searches: &[VectorSearch],
    probes: &[Probe],
    source: S,
    validate: impl FnOnce(&VectorIndexMetadata) -> Result<()>,
) -> Result<Vec<ProbeMatches<NativeMatches>>> {
    if probes.is_empty() {
        return reader
            .visit_batch_vector_search_validated(searches, |_| Ok(source), validate)
            .map(|hits| hits.into_iter().map(ProbeMatches::standard).collect());
    }

    debug_assert_eq!(searches.len(), probes.len());
    reader.load_validated(|_| Ok(source), validate)?;
    let metadata = reader.metadata()?;
    let compatible = metadata.nlist == SHARD_LISTS && metadata.metric == MetricType::L2;
    let mut standard_queries = Vec::new();
    let mut standard_indices = Vec::new();
    let mut joint_queries = Vec::new();
    let mut joint_indices = Vec::new();
    for (index, (query, probe)) in searches.iter().zip(probes).enumerate() {
        if *probe != Probe::Joint {
            standard_queries.push(query);
            standard_indices.push(index);
        }
        if *probe == Probe::Joint || (*probe == Probe::CheckLayout && compatible) {
            joint_queries.push(query);
            joint_indices.push(index);
        }
    }

    let mut results = vec![ProbeMatches::default(); searches.len()];
    if !standard_queries.is_empty() {
        let hits = reader.search_loaded_batch(&standard_queries, None)?;
        for (index, hits) in standard_indices.into_iter().zip(hits) {
            results[index].standard = hits;
        }
    }
    if joint_queries.is_empty() {
        return Ok(results);
    }

    // Java retries an incomplete local joint probe at the ordinary width.
    // A legacy probe has already computed those results for the other policy.
    let joint_hits = reader.search_loaded_batch(&joint_queries, Some(JOINT_INNER_PROBES))?;
    let mut retry = Vec::new();
    for (index, hits) in joint_indices.into_iter().zip(joint_hits) {
        if hits.as_ref().map_or(0, HashMap::len) < searches[index].limit {
            if probes[index] == Probe::CheckLayout {
                results[index].joint = Some(results[index].standard.clone());
            } else {
                retry.push(index);
            }
        } else {
            results[index].joint = Some(hits);
        }
    }
    if !retry.is_empty() {
        let queries = retry
            .iter()
            .map(|&index| &searches[index])
            .collect::<Vec<_>>();
        for (index, hits) in retry
            .into_iter()
            .zip(reader.search_loaded_batch(&queries, None)?)
        {
            results[index].joint = Some(hits);
        }
    }
    Ok(results)
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(super) struct BitmapWork {
    pub(super) predicate: BitmapPredicate,
    pub(super) deleted: Vec<RowRange>,
}

struct Route {
    nearest: Vec<usize>,
    visited: usize,
    width: usize,
    limit: usize,
    hits: ScoredRowIds,
    joint: bool,
    legacy_joint_hits: ScoredRowIds,
}

pub(super) async fn execute_work(
    evaluation: VectorSearchEvaluation<'_>,
    mut work: VectorIndexWork,
    metric_check: VectorMetricCheck,
    range_read_limiter: RangeReadLimiter,
    batch_parallelism: usize,
) -> Result<(Vec<ScoredRowIds>, IndexSearchTiming)> {
    let allow_buffered_read =
        work.queries.len() > 1 || work.filter.is_some() || work.bitmap.is_some();
    if work.centroid_files.is_empty() {
        if Descriptor::is_wrapped(&work.entry.index_file) || work.bitmap.is_some() {
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
            allow_buffered_read,
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

    let joint_enabled = explicit_width.is_none()
        && nlist == ROUTER_LISTS
        && router.metric == MetricType::L2
        && work.filter.is_none()
        && work.bitmap.is_none()
        && matches!(
            work.entry.index_file.index_type.as_str(),
            "ivf-flat" | "ivf-pq"
        )
        && [
            "ivf.nprobe",
            "diskann.l_search",
            "ivf.max_initial_filter_expansion_factor",
        ]
        .iter()
        .all(|option| !configuration.options.contains_key(*option));
    let search = RoutedSearch {
        evaluation,
        group,
        router: &router,
        configuration,
        explicit_width,
        metric_check,
        range_read_limiter,
        batch_parallelism,
        allow_buffered_read,
        joint_enabled,
        bitmap: work.bitmap.as_ref(),
    };
    let Some(predicate) = work.filter else {
        let mut timing = IndexSearchTiming::default();
        let results = search.execute_batch(&work.queries, &mut timing).await?;
        return Ok((results, timing));
    };
    let table = evaluation
        .table
        .ok_or_else(|| centroid_index::invalid("Vector post-filter requires table context"))?;
    let factor = post_filter::candidates_factor(&configuration.options)?;
    let meta = group.router.index_file.global_index_meta.as_ref().unwrap();
    let range_size = meta.row_range_end.abs_diff(meta.row_range_start) + 1;
    let targets: Vec<_> = work.queries.iter().map(|query| query.limit).collect();
    for (query, &original_top_k) in work.queries.iter_mut().zip(&work.top_ks) {
        query.limit = post_filter::candidate_limit(original_top_k, factor, query.limit, range_size);
    }
    let max_candidates = work
        .queries
        .iter()
        .map(|query| query.limit)
        .max()
        .unwrap_or(1);
    let batch_size = post_filter::batch_size(max_candidates);
    let mut timing = IndexSearchTiming::default();
    let mut results = Vec::with_capacity(work.queries.len());
    for (queries, targets) in work
        .queries
        .chunks(batch_size)
        .zip(targets.chunks(batch_size))
    {
        let candidates = search.execute_batch(queries, &mut timing).await?;
        let candidate_ids: RoaringTreemap = candidates
            .iter()
            .flat_map(|hits| hits.row_ids.iter().copied())
            .collect();
        let matching = if candidate_ids.is_empty() {
            candidate_ids
        } else {
            let ranges = sorted_row_ids_to_row_ranges(candidate_ids.iter())?;
            matching_row_ids_for_filter(table, &predicate, Some(ranges)).await?
        };
        results.extend(candidates.into_iter().zip(targets).map(|(hits, &target)| {
            let (ids, scores) = hits
                .row_ids
                .into_iter()
                .zip(hits.scores)
                .filter(|(id, _)| matching.contains(*id))
                .take(target)
                .unzip();
            ScoredRowIds::new(ids, scores)
        }));
    }
    Ok((results, timing))
}

struct RoutedSearch<'a> {
    evaluation: VectorSearchEvaluation<'a>,
    group: &'a Generation<'a>,
    router: &'a Router,
    configuration: &'a VectorIndexOptions,
    explicit_width: Option<usize>,
    metric_check: VectorMetricCheck,
    range_read_limiter: RangeReadLimiter,
    batch_parallelism: usize,
    allow_buffered_read: bool,
    joint_enabled: bool,
    bitmap: Option<&'a BitmapWork>,
}

impl RoutedSearch<'_> {
    async fn execute_batch(
        &self,
        queries: &[VectorSearch],
        timing: &mut IndexSearchTiming,
    ) -> Result<Vec<ScoredRowIds>> {
        let group = self.group;
        let router = self.router;
        let nlist = router.nlist();
        let explicit_width = self.explicit_width;
        let mut routes = Vec::with_capacity(queries.len());
        for search in queries {
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
                joint: false,
                legacy_joint_hits: ScoredRowIds::empty(),
            });
        }

        let legacy_probed = if self.joint_enabled {
            self.resolve_joint_routes(queries, &mut routes, timing)
                .await?
        } else {
            BTreeSet::new()
        };

        loop {
            let mut queries_by_centroid = BTreeMap::<_, Vec<_>>::new();
            for (index, route) in routes.iter_mut().enumerate() {
                for &centroid in &route.nearest[route.visited..route.width] {
                    if group.shards[&centroid].index_file.row_count > 0
                        && !(route.visited == 0 && legacy_probed.contains(&centroid))
                    {
                        let probe = if route.joint {
                            Probe::Joint
                        } else {
                            Probe::Standard
                        };
                        queries_by_centroid
                            .entry(centroid)
                            .or_default()
                            .push((index, probe));
                    }
                }
                route.visited = route.width;
            }
            for (centroid, pending) in queries_by_centroid {
                let (indices, probes): (Vec<_>, Vec<_>) = pending.into_iter().unzip();
                let mut shard = self.shard_work(queries, centroid, &indices);
                if let Some(bitmap) = self.bitmap {
                    let table = self.evaluation.table.ok_or_else(|| {
                        centroid_index::invalid("Centroid Bitmap search requires table context")
                    })?;
                    let file = &group
                        .bitmaps
                        .get(&centroid)
                        .ok_or_else(|| centroid_index::invalid("Missing centroid Bitmap file"))?
                        .index_file;
                    let source = file.global_index_meta.as_ref().unwrap();
                    let mut filter = bitmap.predicate.matching(table, file).await?;
                    for range in &bitmap.deleted {
                        if range.from() < source.row_range_start
                            || range.to() > source.row_range_end
                            || range.from() > range.to()
                        {
                            return Err(centroid_index::invalid(
                                "Invalid deletion range for centroid Bitmap task",
                            ));
                        }
                        filter.remove_range(
                            (range.from() - source.row_range_start) as u64
                                ..=(range.to() - source.row_range_start) as u64,
                        );
                    }
                    if filter.is_empty() {
                        continue;
                    }
                    let filter = Arc::new(filter);
                    for query in &mut shard.queries {
                        let eligible = query.effective_include_row_ids().map_or_else(
                            || Arc::clone(&filter),
                            |include| Arc::new(include & filter.as_ref()),
                        );
                        query.set_shared_include_row_ids(eligible);
                    }
                }
                let (results, stats) = execute_vector_file_probes(
                    self.evaluation,
                    shard,
                    self.metric_check.clone(),
                    self.range_read_limiter.clone(),
                    self.batch_parallelism,
                    if probes.contains(&Probe::Joint) {
                        probes
                    } else {
                        vec![]
                    },
                    self.allow_buffered_read,
                )
                .await?;
                add_timing(timing, stats);
                for (index, result) in indices.into_iter().zip(results) {
                    let route = &mut routes[index];
                    let hits = result.joint.unwrap_or(result.standard);
                    route.hits = route.hits.or(&hits).top_k(route.limit);
                }
            }

            let mut expand = false;
            if explicit_width.is_none() {
                for route in &mut routes {
                    if route.width > 0
                        && route.width < nlist
                        && route.hits.row_ids.len() < route.limit
                    {
                        route.width = route.width.saturating_mul(2).min(nlist);
                        route.joint = false;
                        expand = true;
                    }
                }
            }
            if !expand {
                break;
            }
        }
        Ok(routes.into_iter().map(|route| route.hits).collect())
    }

    fn shard_work(
        &self,
        queries: &[VectorSearch],
        id: usize,
        indices: &[usize],
    ) -> VectorIndexWork {
        VectorIndexWork {
            entry: self.group.shards[&id].clone(),
            centroid_files: vec![],
            bitmap: None,
            queries: indices
                .iter()
                .map(|&index| queries[index].clone())
                .collect(),
            resolved: Some(self.configuration.clone()),
            filter: None,
            top_ks: vec![],
        }
    }

    async fn resolve_joint_routes(
        &self,
        queries: &[VectorSearch],
        routes: &mut [Route],
        timing: &mut IndexSearchTiming,
    ) -> Result<BTreeSet<usize>> {
        let group = self.group;
        let mut layouts = HashMap::new();
        let mut legacy = BTreeSet::new();
        for (route, query) in routes.iter_mut().zip(queries) {
            if route.width != AUTOMATIC_WIDTH
                || !(10..=100).contains(&query.limit)
                || query.effective_include_row_ids().is_some()
            {
                continue;
            }
            let mut unknown = Vec::new();
            route.joint = true;
            for &id in &route.nearest[..JOINT_OUTER_PROBES] {
                let file = &group.shards[&id].index_file;
                if file.row_count < (JOINT_INNER_PROBES * query.limit) as i64 {
                    route.joint = false;
                    break;
                }
                let layout = match layouts.entry(id) {
                    std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        let desc = Descriptor::parse(file)?.expect("validated centroid shard");
                        let options = centroid_index::payload_options(&desc)?;
                        let layout = options
                            .get("localNlist")
                            .map(|count| {
                                count
                                    .parse::<usize>()
                                    .map(|n| {
                                        n == SHARD_LISTS
                                            && options
                                                .get("metric")
                                                .is_some_and(|value| value == "l2")
                                    })
                                    .map_err(|_| {
                                        centroid_index::invalid("Invalid centroid shard localNlist")
                                    })
                            })
                            .transpose()?;
                        *entry.insert(layout)
                    }
                };
                match layout {
                    Some(false) => {
                        route.joint = false;
                        break;
                    }
                    None => unknown.push(id),
                    Some(true) => {}
                }
            }
            if route.joint {
                if unknown.is_empty() {
                    route.width = JOINT_OUTER_PROBES;
                } else {
                    legacy.extend(unknown);
                }
            }
        }

        // Evaluate both policies while each legacy shard's native reader is open.
        // Only the winning policy's bounded top-K survives layout resolution.
        for &id in &legacy {
            let (indices, probes): (Vec<_>, Vec<_>) = routes
                .iter()
                .enumerate()
                .filter_map(|(index, route)| {
                    let rank = route.nearest[..route.width]
                        .iter()
                        .position(|&candidate| candidate == id)?;
                    let probe = if route.joint && rank < JOINT_OUTER_PROBES {
                        Probe::CheckLayout
                    } else {
                        Probe::Standard
                    };
                    Some((index, probe))
                })
                .unzip();
            let (hits, stats) = execute_vector_file_probes(
                self.evaluation,
                self.shard_work(queries, id, &indices),
                self.metric_check.clone(),
                self.range_read_limiter.clone(),
                self.batch_parallelism,
                probes.clone(),
                self.allow_buffered_read,
            )
            .await?;
            add_timing(timing, stats);
            for ((index, probe), hits) in indices.into_iter().zip(probes).zip(hits) {
                let route = &mut routes[index];
                route.hits = route.hits.or(&hits.standard).top_k(route.limit);
                if probe == Probe::CheckLayout {
                    if let Some(joint) = hits.joint {
                        route.legacy_joint_hits =
                            route.legacy_joint_hits.or(&joint).top_k(route.limit);
                    } else {
                        route.joint = false;
                    }
                }
            }
        }
        for route in routes {
            if route.joint {
                route.width = JOINT_OUTER_PROBES;
                std::mem::swap(&mut route.hits, &mut route.legacy_joint_hits);
            }
            route.legacy_joint_hits = ScoredRowIds::empty();
        }
        Ok(legacy)
    }
}

fn add_timing(timing: &mut IndexSearchTiming, stats: IndexSearchTiming) {
    timing.permit_wait = timing.permit_wait.saturating_add(stats.permit_wait);
    timing.file_reader_open = timing
        .file_reader_open
        .saturating_add(stats.file_reader_open);
}

#[cfg(test)]
#[path = "centroid_joint_tests.rs"]
mod joint_tests;
