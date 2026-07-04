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

use std::collections::HashMap;

#[derive(Clone)]
pub struct VectorSearch {
    pub vector: Vec<f32>,
    pub limit: usize,
    pub field_name: String,
    pub include_row_ids: Option<roaring::RoaringTreemap>,
}

impl VectorSearch {
    pub fn new(vector: Vec<f32>, limit: usize, field_name: String) -> crate::Result<Self> {
        if vector.is_empty() {
            return Err(crate::Error::DataInvalid {
                message: "Search vector cannot be empty".to_string(),
                source: None,
            });
        }
        if limit == 0 || limit > i32::MAX as usize {
            return Err(crate::Error::DataInvalid {
                message: format!("Limit must be between 1 and {}, got: {}", i32::MAX, limit),
                source: None,
            });
        }
        if field_name.is_empty() {
            return Err(crate::Error::DataInvalid {
                message: "Field name cannot be null or empty".to_string(),
                source: None,
            });
        }
        Ok(Self {
            vector,
            limit,
            field_name,
            include_row_ids: None,
        })
    }

    pub fn with_include_row_ids(mut self, include_row_ids: roaring::RoaringTreemap) -> Self {
        self.include_row_ids = Some(include_row_ids);
        self
    }
}

impl std::fmt::Display for VectorSearch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "VectorSearch(field_name={}, limit={})",
            self.field_name, self.limit
        )
    }
}

pub struct GlobalIndexIOMeta {
    pub file_path: String,
    pub file_size: u64,
    pub metadata: Vec<u8>,
}

impl GlobalIndexIOMeta {
    pub fn new(file_path: String, file_size: u64, metadata: Vec<u8>) -> Self {
        Self {
            file_path,
            file_size,
            metadata,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SearchResult {
    pub row_ids: Vec<u64>,
    pub scores: Vec<f32>,
}

impl SearchResult {
    pub fn new(row_ids: Vec<u64>, scores: Vec<f32>) -> Self {
        assert_eq!(row_ids.len(), scores.len());
        Self { row_ids, scores }
    }

    pub fn empty() -> Self {
        Self {
            row_ids: Vec::new(),
            scores: Vec::new(),
        }
    }

    pub fn from_scored_map(map: HashMap<u64, f32>) -> Self {
        let mut row_ids = Vec::with_capacity(map.len());
        let mut scores = Vec::with_capacity(map.len());
        for (id, score) in map {
            row_ids.push(id);
            scores.push(score);
        }
        Self { row_ids, scores }
    }

    pub fn len(&self) -> usize {
        self.row_ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.row_ids.is_empty()
    }

    pub fn offset(&self, offset: i64) -> Self {
        if offset == 0 {
            return self.clone();
        }
        let row_ids = self
            .row_ids
            .iter()
            .map(|&id| {
                if offset >= 0 {
                    id.saturating_add(offset as u64)
                } else {
                    id.saturating_sub(offset.unsigned_abs())
                }
            })
            .collect();
        Self {
            row_ids,
            scores: self.scores.clone(),
        }
    }

    pub fn or(&self, other: &SearchResult) -> Self {
        let mut row_ids = self.row_ids.clone();
        let mut scores = self.scores.clone();
        row_ids.extend_from_slice(&other.row_ids);
        scores.extend_from_slice(&other.scores);
        Self { row_ids, scores }
    }

    pub fn top_k(&self, k: usize) -> Self {
        if self.row_ids.len() <= k {
            return self.clone();
        }
        let mut indices: Vec<usize> = (0..self.row_ids.len()).collect();
        indices.sort_by(|&a, &b| {
            self.scores[b]
                .partial_cmp(&self.scores[a])
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        indices.truncate(k);
        let row_ids = indices.iter().map(|&i| self.row_ids[i]).collect();
        let scores = indices.iter().map(|&i| self.scores[i]).collect();
        Self { row_ids, scores }
    }

    pub(crate) fn without_deleted_row_ranges(
        &self,
        deleted_rows: Option<&crate::table::global_index_scanner::RowRangeIndex>,
    ) -> crate::Result<Self> {
        let Some(deleted_rows) = deleted_rows else {
            return Ok(self.clone());
        };

        let mut row_ids = Vec::with_capacity(self.row_ids.len());
        let mut scores = Vec::with_capacity(self.scores.len());
        for (&row_id, &score) in self.row_ids.iter().zip(&self.scores) {
            let row_id_i64 = i64::try_from(row_id).map_err(|_| crate::Error::DataInvalid {
                message: format!(
                    "Vector search row id {row_id} exceeds i64::MAX and cannot be checked against deletion vectors"
                ),
                source: None,
            })?;
            if !deleted_rows.intersects(row_id_i64, row_id_i64) {
                row_ids.push(row_id);
                scores.push(score);
            }
        }
        Ok(Self { row_ids, scores })
    }

    pub fn to_row_ranges(&self) -> crate::Result<Vec<crate::table::RowRange>> {
        if self.row_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut sorted = self
            .row_ids
            .iter()
            .copied()
            .map(|id| {
                i64::try_from(id).map_err(|_| crate::Error::DataInvalid {
                    message: format!(
                        "Vector search row id {id} exceeds i64::MAX and cannot be converted to RowRange"
                    ),
                    source: None,
                })
            })
            .collect::<crate::Result<Vec<_>>>()?;

        sorted.sort_unstable();
        sorted.dedup();
        let mut ranges = Vec::new();
        let mut start = sorted[0];
        let mut end = start;
        for &id in &sorted[1..] {
            if end.checked_add(1) == Some(id) {
                end = id;
            } else {
                ranges.push(crate::table::RowRange::new(start, end));
                start = id;
                end = id;
            }
        }
        ranges.push(crate::table::RowRange::new(start, end));
        Ok(ranges)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_search_clone_preserves_include_row_ids() {
        let mut include_row_ids = roaring::RoaringTreemap::new();
        include_row_ids.insert(1);
        include_row_ids.insert(3);

        let vector_search = VectorSearch::new(vec![1.0, 2.0], 10, "embedding".to_string())
            .unwrap()
            .with_include_row_ids(include_row_ids.clone());

        let cloned = vector_search.clone();
        assert_eq!(cloned.vector, vector_search.vector);
        assert_eq!(cloned.limit, vector_search.limit);
        assert_eq!(cloned.field_name, vector_search.field_name);
        assert_eq!(cloned.include_row_ids.as_ref(), Some(&include_row_ids));
    }

    #[test]
    fn test_search_result_from_scored_map() {
        let mut map = HashMap::new();
        map.insert(1u64, 0.9f32);
        map.insert(2, 0.5);
        let result = SearchResult::from_scored_map(map);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_search_result_top_k() {
        let result = SearchResult::new(vec![1, 2, 3, 4, 5], vec![0.1, 0.9, 0.5, 0.8, 0.3]);
        let top = result.top_k(2);
        assert_eq!(top.len(), 2);
        assert!(top.row_ids.contains(&2));
        assert!(top.row_ids.contains(&4));
    }

    #[test]
    fn test_search_result_filters_deleted_row_ranges() {
        let result = SearchResult::new(vec![1, 2, 3, 4], vec![0.1, 0.9, 0.8, 0.2]);
        let deleted = crate::table::global_index_scanner::RowRangeIndex::create(vec![
            crate::table::RowRange::new(2, 3),
        ]);

        let filtered = result
            .without_deleted_row_ranges(Some(&deleted))
            .unwrap()
            .top_k(10);
        assert_eq!(filtered.row_ids, vec![1, 4]);
        assert_eq!(filtered.scores, vec![0.1, 0.2]);
    }

    #[test]
    fn test_search_result_offset() {
        let result = SearchResult::new(vec![0, 1], vec![0.5, 0.6]);
        let offset = result.offset(100);
        assert_eq!(offset.row_ids, vec![100, 101]);
        assert_eq!(offset.scores, vec![0.5, 0.6]);
    }

    #[test]
    fn test_search_result_or() {
        let a = SearchResult::new(vec![1, 2], vec![0.5, 0.6]);
        let b = SearchResult::new(vec![3], vec![0.7]);
        let merged = a.or(&b);
        assert_eq!(merged.len(), 3);
    }

    #[test]
    fn test_search_result_to_row_ranges() {
        let result = SearchResult::new(vec![5, 1, 2, 3, 10], vec![0.1; 5]);
        let ranges = result.to_row_ranges().unwrap();
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].from(), 1);
        assert_eq!(ranges[0].to(), 3);
        assert_eq!(ranges[1].from(), 5);
        assert_eq!(ranges[1].to(), 5);
        assert_eq!(ranges[2].from(), 10);
        assert_eq!(ranges[2].to(), 10);
    }

    #[test]
    fn test_search_result_to_row_ranges_rejects_i64_overflow() {
        let result = SearchResult::new(vec![i64::MAX as u64 + 1], vec![0.1]);
        let err = result.to_row_ranges().unwrap_err();
        assert!(
            err.to_string().contains("exceeds i64::MAX"),
            "unexpected error: {err}"
        );
    }
}
