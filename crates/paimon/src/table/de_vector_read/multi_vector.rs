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

use super::{RawScoreTopK, VectorSearch};
use crate::common::number::{accumulate_f64_partials, round_f64_partials};
use crate::spec::ROW_ID_FIELD_NAME;
use crate::vindex::multi_vector::{invalid, validate_queries, MultiVectorArray};
use crate::vindex::pkvector::metric::{
    cosine_norm, cosine_similarity_f64, cosine_similarity_with_norms,
};
use crate::Result;
use arrow_array::{Array, Int64Array, RecordBatch};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Serialize, Deserialize)]
pub(super) enum ListScoring {
    Independent,
    MaxSim,
}

impl ListScoring {
    pub(super) fn result_count(self, query_count: usize) -> usize {
        match self {
            Self::Independent => query_count,
            Self::MaxSim => 1,
        }
    }
}

pub(super) struct ListScorer {
    mode: ListScoring,
    query_norms: Vec<f64>,
}

impl ListScorer {
    pub(super) fn new(mode: ListScoring, searches: &[VectorSearch]) -> Result<Self> {
        let queries = searches
            .iter()
            .map(|search| search.vector.as_slice())
            .collect::<Vec<_>>();
        validate_queries(&queries, None)?;
        Ok(Self {
            mode,
            query_norms: queries.into_iter().map(cosine_norm).collect(),
        })
    }

    pub(super) fn collect(
        &self,
        batch: &RecordBatch,
        searches: &[VectorSearch],
        top_k: &mut [RawScoreTopK],
    ) -> Result<()> {
        let field_name = &searches[0].field_name;
        let dimension = searches[0].vector.len();
        let column = batch
            .column_by_name(field_name)
            .ok_or_else(|| invalid(format!("MultiVector column '{field_name}' is missing")))?;
        let column = MultiVectorArray::new(column.as_ref())?;
        let row_ids = batch
            .column_by_name(ROW_ID_FIELD_NAME)
            .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
            .filter(|ids| ids.null_count() == 0)
            .ok_or_else(|| invalid("MultiVector scoring requires non-null Int64 _ROW_ID"))?;
        let mut selected = Vec::with_capacity(searches.len());
        let mut maxima = vec![f64::NEG_INFINITY; searches.len()];
        let mut partials = Vec::new();
        let aggregate_queries = matches!(self.mode, ListScoring::MaxSim) && searches.len() > 1;
        let shared_filter = searches[0].effective_include_row_ids();
        for (row, &id) in row_ids.values().iter().enumerate() {
            let id = u64::try_from(id).map_err(|_| invalid("Negative MultiVector row ID"))?;
            selected.clear();
            match self.mode {
                ListScoring::MaxSim => {
                    if shared_filter.is_some_and(|ids| !ids.contains(id)) {
                        continue;
                    }
                    selected.extend(0..searches.len());
                }
                ListScoring::Independent => {
                    selected.extend(searches.iter().enumerate().filter_map(|(i, search)| {
                        search
                            .effective_include_row_ids()
                            .is_none_or(|ids| ids.contains(id))
                            .then_some(i)
                    }));
                }
            }
            if selected.is_empty() {
                continue;
            }
            let mut found = false;
            maxima.fill(f64::NEG_INFINITY);
            for embedding in column.row(row, dimension) {
                let embedding = embedding?;
                found = true;
                let vector_norm = cosine_norm(embedding);
                for &i in &selected {
                    let query = &searches[i].vector;
                    let query_norm = self.query_norms[i];
                    let similarity = if aggregate_queries {
                        cosine_similarity_f64(
                            query,
                            embedding,
                            query_norm * vector_norm,
                            &mut partials,
                        )
                    } else {
                        f64::from(cosine_similarity_with_norms(
                            query,
                            query_norm,
                            embedding,
                            vector_norm,
                            &mut partials,
                        ))
                    };
                    maxima[i] = maxima[i].max(similarity);
                }
            }
            if found {
                match self.mode {
                    ListScoring::MaxSim => {
                        let score = if aggregate_queries {
                            accumulate_f64_partials(maxima.iter().copied(), &mut partials);
                            round_f64_partials(&partials)
                        } else {
                            maxima[0]
                        };
                        top_k[0].offer(id, score as f32);
                    }
                    ListScoring::Independent => {
                        for &i in &selected {
                            top_k[i].offer(id, maxima[i] as f32);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
