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

//! Full-text search types for global index.
//!
//! Reference: [org.apache.paimon.predicate.FullTextSearch](https://github.com/apache/paimon/blob/master/paimon-common/src/main/java/org/apache/paimon/predicate/FullTextSearch.java)

/// Full-text search predicate.
///
/// Reference: `org.apache.paimon.predicate.FullTextSearch`
#[derive(Debug, Clone)]
pub struct FullTextSearch {
    pub query_text: String,
    pub field_name: String,
    pub limit: usize,
}

impl FullTextSearch {
    pub fn new(query_text: String, limit: usize, field_name: String) -> crate::Result<Self> {
        if query_text.is_empty() {
            return Err(crate::Error::ConfigInvalid {
                message: "Query text cannot be empty".to_string(),
            });
        }
        if limit == 0 {
            return Err(crate::Error::ConfigInvalid {
                message: "Limit must be positive".to_string(),
            });
        }
        if field_name.is_empty() {
            return Err(crate::Error::ConfigInvalid {
                message: "Field name cannot be empty".to_string(),
            });
        }
        Ok(Self {
            query_text,
            field_name,
            limit,
        })
    }
}

/// Search result containing parallel arrays of row IDs and scores.
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

    pub fn len(&self) -> usize {
        self.row_ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.row_ids.is_empty()
    }

    /// Apply an offset to all row IDs.
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

    /// Merge two search results.
    pub fn or(&self, other: &SearchResult) -> Self {
        let mut row_ids = self.row_ids.clone();
        let mut scores = self.scores.clone();
        row_ids.extend_from_slice(&other.row_ids);
        scores.extend_from_slice(&other.scores);
        Self { row_ids, scores }
    }

    /// Return top-k results by score (descending).
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

    /// Convert to sorted, merged row ranges.
    pub fn to_row_ranges(&self) -> Vec<crate::table::RowRange> {
        if self.row_ids.is_empty() {
            return Vec::new();
        }
        let mut sorted: Vec<u64> = self.row_ids.clone();
        sorted.sort_unstable();
        sorted.dedup();
        let mut ranges = Vec::new();
        let mut start = sorted[0] as i64;
        let mut end = start;
        for &id in &sorted[1..] {
            let id = id as i64;
            if id == end + 1 {
                end = id;
            } else {
                ranges.push(crate::table::RowRange::new(start, end));
                start = id;
                end = id;
            }
        }
        ranges.push(crate::table::RowRange::new(start, end));
        ranges
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_full_text_search_new() {
        let fts = FullTextSearch::new("hello".into(), 10, "text".into()).unwrap();
        assert_eq!(fts.query_text, "hello");
        assert_eq!(fts.limit, 10);
        assert_eq!(fts.field_name, "text");
    }

    #[test]
    fn test_full_text_search_empty_query() {
        let result = FullTextSearch::new("".into(), 10, "text".into());
        assert!(result.is_err());
    }
}
