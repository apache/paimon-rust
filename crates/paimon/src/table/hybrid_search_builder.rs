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

//! Hybrid search builder for combining multiple search routes.
//!
//! Reference: `org.apache.paimon.table.source.HybridSearchBuilder`.

use std::collections::HashMap;

use crate::spec::CoreOptions;
use crate::table::{RowRange, Table};
use crate::vector_search::SearchResult;

const RRF_K: f32 = 60.0;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HybridSearchRanker {
    Rrf,
    WeightedScore,
    Mrr,
}

impl HybridSearchRanker {
    pub const RRF: &'static str = "rrf";
    pub const WEIGHTED_SCORE: &'static str = "weighted_score";
    pub const MRR: &'static str = "mrr";

    pub fn parse(ranker: &str) -> crate::Result<Self> {
        match ranker.trim().to_ascii_lowercase().as_str() {
            "" | Self::RRF => Ok(Self::Rrf),
            Self::WEIGHTED_SCORE => Ok(Self::WeightedScore),
            Self::MRR => Ok(Self::Mrr),
            _ => Err(crate::Error::ConfigInvalid {
                message: format!("Unsupported hybrid ranker: {ranker}"),
            }),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rrf => Self::RRF,
            Self::WeightedScore => Self::WEIGHTED_SCORE,
            Self::Mrr => Self::MRR,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HybridSearchRouteKind {
    Vector,
    FullText,
}

#[derive(Clone, Debug)]
pub struct HybridSearchRoute {
    kind: HybridSearchRouteKind,
    field_name: String,
    vector: Option<Vec<f32>>,
    full_text_query: Option<String>,
    limit: usize,
    weight: f32,
    options: HashMap<String, String>,
}

impl HybridSearchRoute {
    pub fn vector(
        field_name: impl Into<String>,
        vector: Vec<f32>,
        limit: usize,
        weight: f32,
        options: HashMap<String, String>,
    ) -> crate::Result<Self> {
        let field_name = field_name.into();
        Self::validate_common(&field_name, limit, weight)?;
        if vector.is_empty() {
            return Err(crate::Error::DataInvalid {
                message: "Search vector cannot be empty".to_string(),
                source: None,
            });
        }
        Ok(Self {
            kind: HybridSearchRouteKind::Vector,
            field_name,
            vector: Some(vector),
            full_text_query: None,
            limit,
            weight,
            options,
        })
    }

    pub fn full_text(
        field_name: impl Into<String>,
        query: impl Into<String>,
        limit: usize,
        weight: f32,
        options: HashMap<String, String>,
    ) -> crate::Result<Self> {
        if !options.is_empty() {
            return Err(crate::Error::ConfigInvalid {
                message: "Full-text hybrid route options are not supported yet".to_string(),
            });
        }

        let field_name = field_name.into();
        let query = query.into();
        Self::validate_common(&field_name, limit, weight)?;
        if query.is_empty() {
            return Err(crate::Error::ConfigInvalid {
                message: "Full-text route query cannot be empty".to_string(),
            });
        }

        Ok(Self {
            kind: HybridSearchRouteKind::FullText,
            field_name,
            vector: None,
            full_text_query: Some(query),
            limit,
            weight,
            options,
        })
    }

    fn validate_common(field_name: &str, limit: usize, weight: f32) -> crate::Result<()> {
        if field_name.is_empty() {
            return Err(crate::Error::DataInvalid {
                message: "Field name cannot be null or empty".to_string(),
                source: None,
            });
        }
        if limit == 0 {
            return Err(crate::Error::ConfigInvalid {
                message: "Limit must be positive".to_string(),
            });
        }
        if !weight.is_finite() || weight <= 0.0 {
            return Err(crate::Error::ConfigInvalid {
                message: format!("Weight must be finite and positive, got: {weight}"),
            });
        }
        Ok(())
    }

    pub fn kind(&self) -> HybridSearchRouteKind {
        self.kind
    }

    pub fn field_name(&self) -> &str {
        &self.field_name
    }

    pub fn vector_value(&self) -> Option<&[f32]> {
        self.vector.as_deref()
    }

    pub fn full_text_query(&self) -> Option<&str> {
        self.full_text_query.as_deref()
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn weight(&self) -> f32 {
        self.weight
    }

    pub fn options(&self) -> &HashMap<String, String> {
        &self.options
    }
}

pub struct HybridSearchBuilder<'a> {
    table: &'a Table,
    routes: Vec<HybridSearchRoute>,
    limit: Option<usize>,
    ranker: HybridSearchRanker,
}

impl<'a> HybridSearchBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            routes: Vec::new(),
            limit: None,
            ranker: HybridSearchRanker::Rrf,
        }
    }

    pub fn add_route(&mut self, route: HybridSearchRoute) -> &mut Self {
        self.routes.push(route);
        self
    }

    pub fn add_vector_route(
        &mut self,
        field_name: &str,
        vector: Vec<f32>,
        limit: usize,
        weight: f32,
        options: HashMap<String, String>,
    ) -> crate::Result<&mut Self> {
        self.routes.push(HybridSearchRoute::vector(
            field_name, vector, limit, weight, options,
        )?);
        Ok(self)
    }

    pub fn add_full_text_route(
        &mut self,
        field_name: &str,
        query: &str,
        limit: usize,
        weight: f32,
        options: HashMap<String, String>,
    ) -> crate::Result<&mut Self> {
        self.routes.push(HybridSearchRoute::full_text(
            field_name, query, limit, weight, options,
        )?);
        Ok(self)
    }

    pub fn with_limit(&mut self, limit: usize) -> &mut Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_ranker(&mut self, ranker: &str) -> crate::Result<&mut Self> {
        self.ranker = HybridSearchRanker::parse(ranker)?;
        Ok(self)
    }

    pub fn with_rrf_ranker(&mut self) -> &mut Self {
        self.ranker = HybridSearchRanker::Rrf;
        self
    }

    pub fn with_weighted_score_ranker(&mut self) -> &mut Self {
        self.ranker = HybridSearchRanker::WeightedScore;
        self
    }

    pub fn with_mrr_ranker(&mut self) -> &mut Self {
        self.ranker = HybridSearchRanker::Mrr;
        self
    }

    pub async fn execute(&self) -> crate::Result<Vec<RowRange>> {
        self.execute_scored().await?.to_row_ranges()
    }

    pub async fn execute_scored(&self) -> crate::Result<SearchResult> {
        CoreOptions::new(self.table.schema().options()).ensure_read_authorized()?;
        let limit = self.limit.ok_or_else(|| crate::Error::ConfigInvalid {
            message: "Limit must be set via with_limit()".to_string(),
        })?;
        if self.routes.is_empty() {
            return Err(crate::Error::ConfigInvalid {
                message: "Routes cannot be empty".to_string(),
            });
        }

        let mut route_results = Vec::with_capacity(self.routes.len());
        for route in &self.routes {
            let result = match route.kind {
                HybridSearchRouteKind::Vector => {
                    let mut builder = self.table.new_vector_search_builder();
                    builder
                        .with_vector_column(&route.field_name)
                        .with_query_vector(route.vector.clone().expect("validated vector route"))
                        .with_limit(route.limit)
                        .with_options(route.options.clone());
                    builder.execute_scored().await?
                }
                HybridSearchRouteKind::FullText => {
                    execute_full_text_route(self.table, route).await?
                }
            };
            if !result.is_empty() {
                route_results.push(WeightedRouteResult {
                    result,
                    weight: route.weight,
                });
            }
        }

        Ok(rank_results(self.ranker, &route_results, limit))
    }
}

#[cfg(feature = "fulltext")]
async fn execute_full_text_route(
    table: &Table,
    route: &HybridSearchRoute,
) -> crate::Result<SearchResult> {
    let mut builder = table.new_full_text_search_builder();
    builder
        .with_text_column(&route.field_name)
        .with_query_text(
            route
                .full_text_query
                .as_deref()
                .expect("validated full-text route"),
        )
        .with_limit(route.limit);
    let result = builder.execute_scored().await?;
    Ok(SearchResult::new(result.row_ids, result.scores))
}

#[cfg(not(feature = "fulltext"))]
async fn execute_full_text_route(
    _table: &Table,
    _route: &HybridSearchRoute,
) -> crate::Result<SearchResult> {
    Err(crate::Error::ConfigInvalid {
        message: "Full-text hybrid routes require the fulltext feature".to_string(),
    })
}

struct WeightedRouteResult {
    result: SearchResult,
    weight: f32,
}

fn rank_results(
    ranker: HybridSearchRanker,
    route_results: &[WeightedRouteResult],
    limit: usize,
) -> SearchResult {
    match ranker {
        HybridSearchRanker::Rrf => rrf(route_results, limit),
        HybridSearchRanker::WeightedScore => weighted_score(route_results, limit),
        HybridSearchRanker::Mrr => mrr(route_results, limit),
    }
}

fn rrf(route_results: &[WeightedRouteResult], limit: usize) -> SearchResult {
    let mut scores = HashMap::new();
    for route_result in route_results {
        for (rank, (row_id, _score)) in ranked_row_ids(&route_result.result).iter().enumerate() {
            let contribution = route_result.weight / (RRF_K + rank as f32 + 1.0);
            add_score(&mut scores, *row_id, contribution);
        }
    }
    top_k(scores, limit)
}

fn mrr(route_results: &[WeightedRouteResult], limit: usize) -> SearchResult {
    let mut scores = HashMap::new();
    for route_result in route_results {
        for (rank, (row_id, _score)) in ranked_row_ids(&route_result.result).iter().enumerate() {
            let contribution = route_result.weight / (rank as f32 + 1.0);
            add_score(&mut scores, *row_id, contribution);
        }
    }
    top_k(scores, limit)
}

fn weighted_score(route_results: &[WeightedRouteResult], limit: usize) -> SearchResult {
    let mut scores = HashMap::new();
    for route_result in route_results {
        let ranked = ranked_row_ids(&route_result.result);
        if ranked.is_empty() {
            continue;
        }

        let (mut min, mut max) = (f32::INFINITY, f32::NEG_INFINITY);
        for (_row_id, score) in &ranked {
            min = min.min(*score);
            max = max.max(*score);
        }
        let range = max - min;

        for (row_id, score) in ranked {
            let normalized = if range > 0.0 {
                (score - min) / range
            } else {
                1.0
            };
            add_score(&mut scores, row_id, route_result.weight * normalized);
        }
    }
    top_k(scores, limit)
}

fn ranked_row_ids(result: &SearchResult) -> Vec<(u64, f32)> {
    let mut best_scores = HashMap::new();
    for (&row_id, &score) in result.row_ids.iter().zip(&result.scores) {
        best_scores
            .entry(row_id)
            .and_modify(|old: &mut f32| {
                if score > *old {
                    *old = score;
                }
            })
            .or_insert(score);
    }

    let mut ranked: Vec<_> = best_scores.into_iter().collect();
    ranked.sort_by(|(left_id, left_score), (right_id, right_score)| {
        right_score
            .partial_cmp(left_score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left_id.cmp(right_id))
    });
    ranked
}

fn add_score(scores: &mut HashMap<u64, f32>, row_id: u64, score: f32) {
    scores
        .entry(row_id)
        .and_modify(|old_score| *old_score += score)
        .or_insert(score);
}

fn top_k(scores: HashMap<u64, f32>, limit: usize) -> SearchResult {
    if scores.is_empty() || limit == 0 {
        return SearchResult::empty();
    }

    let mut entries: Vec<_> = scores.into_iter().collect();
    entries.sort_by(|(left_id, left_score), (right_id, right_score)| {
        right_score
            .partial_cmp(left_score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left_id.cmp(right_id))
    });
    entries.truncate(limit);

    let (row_ids, scores): (Vec<_>, Vec<_>) = entries.into_iter().unzip();
    SearchResult::new(row_ids, scores)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route_result(row_ids: Vec<u64>, scores: Vec<f32>, weight: f32) -> WeightedRouteResult {
        WeightedRouteResult {
            result: SearchResult::new(row_ids, scores),
            weight,
        }
    }

    #[test]
    fn test_rrf_prefers_overlap() {
        let ranked = rank_results(
            HybridSearchRanker::Rrf,
            &[
                route_result(vec![1, 2], vec![0.9, 0.8], 1.0),
                route_result(vec![2, 3], vec![0.95, 0.1], 1.0),
            ],
            1,
        );

        assert_eq!(ranked.row_ids, vec![2]);
    }

    #[test]
    fn test_weighted_score_min_max_normalizes_per_route() {
        let ranked = rank_results(
            HybridSearchRanker::WeightedScore,
            &[
                route_result(vec![1, 2, 3], vec![10.0, 5.0, 0.0], 2.0),
                route_result(vec![1, 2, 3], vec![100.0, 50.0, 0.0], 1.0),
            ],
            3,
        );

        let scores: HashMap<_, _> = ranked.row_ids.into_iter().zip(ranked.scores).collect();
        assert!((scores[&1] - 3.0).abs() < 1e-6);
        assert!((scores[&2] - 1.5).abs() < 1e-6);
        assert!((scores[&3] - 0.0).abs() < 1e-6);
    }

    #[test]
    fn test_mrr_uses_reciprocal_rank_without_constant() {
        let ranked = rank_results(
            HybridSearchRanker::Mrr,
            &[
                route_result(vec![1, 2], vec![0.9, 0.8], 1.0),
                route_result(vec![2, 3], vec![0.95, 0.1], 1.0),
            ],
            2,
        );

        assert_eq!(ranked.row_ids[0], 2);
        assert!(ranked.scores[0] > ranked.scores[1]);
    }
}
