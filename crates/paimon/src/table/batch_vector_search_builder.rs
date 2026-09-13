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

//! Configures batch vector queries and dispatches to DE or primary-key readers.

use crate::spec::{CoreOptions, Predicate};
use crate::table::de_vector_scan::PreparedVectorSearchFilter;
use crate::table::vector_read::BatchVectorRead;
use crate::table::vector_scan::{PlanContext, VectorScan};
use crate::table::Table;
use crate::vector_search::SearchResult;
use roaring::RoaringTreemap;
use std::collections::HashMap;
use std::sync::Arc;

pub struct BatchVectorSearchBuilder<'a> {
    table: &'a Table,
    vector_column: Option<String>,
    query_vectors: Option<Vec<Vec<f32>>>,
    limit: Option<usize>,
    options: HashMap<String, String>,
    filter: Option<Predicate>,
    include_row_ids: Option<Arc<RoaringTreemap>>,
    prepared_filter: Option<PreparedVectorSearchFilter>,
}

impl<'a> BatchVectorSearchBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            vector_column: None,
            query_vectors: None,
            limit: None,
            options: HashMap::new(),
            filter: None,
            include_row_ids: None,
            prepared_filter: None,
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

    pub fn with_options(&mut self, options: HashMap<String, String>) -> &mut Self {
        self.options = options;
        self
    }

    /// Attach one scalar predicate shared by every query in the batch and applied
    /// before vector Top-K. See [`crate::table::VectorSearchBuilder::with_filter`] for the
    /// primary-key and data-evolution execution semantics.
    pub fn with_filter(&mut self, filter: Predicate) -> &mut Self {
        self.filter = Some(filter);
        self.include_row_ids = None;
        self.prepared_filter = None;
        self
    }

    /// Attach a prepared scalar pre-filter together with the exact table
    /// snapshot against which its row-ID allow-list was evaluated.
    pub fn with_prepared_filter(
        &mut self,
        prepared_filter: PreparedVectorSearchFilter,
    ) -> &mut Self {
        self.prepared_filter = Some(prepared_filter);
        self.filter = None;
        self.include_row_ids = None;
        self
    }

    /// Attach a caller-managed row-ID allow-list.
    ///
    /// This low-level API does not bind the allow-list to a table snapshot.
    /// Prefer [`Self::with_prepared_filter`] for scalar pre-filters.
    pub fn with_include_row_ids(&mut self, include_row_ids: RoaringTreemap) -> &mut Self {
        self.include_row_ids = Some(Arc::new(include_row_ids));
        self.filter = None;
        self.prepared_filter = None;
        self
    }

    /// Create the same query-independent scan used by a single-vector builder.
    pub fn new_scan(&self) -> crate::Result<VectorScan> {
        let column = self.column()?;
        VectorScan::new(
            self.table,
            column,
            self.filter.as_ref(),
            self.include_row_ids.as_ref(),
            self.prepared_filter.as_ref(),
        )
    }

    /// Create an owned batch reader; result i belongs to input query i.
    pub fn new_read(&self) -> crate::Result<BatchVectorRead> {
        let column = self.column()?;
        PlanContext::new(
            self.table,
            column,
            self.filter.as_ref(),
            self.include_row_ids.as_ref(),
            self.prepared_filter.as_ref(),
        )?;
        let queries = self
            .query_vectors
            .as_deref()
            .filter(|queries| !queries.is_empty())
            .ok_or_else(|| crate::Error::ConfigInvalid {
                message: "Query vectors must be set via with_query_vectors()".to_string(),
            })?;
        let limit = self.limit.ok_or_else(|| crate::Error::ConfigInvalid {
            message: "Limit must be set via with_limit()".to_string(),
        })?;
        let query_refs: Vec<&[f32]> = queries.iter().map(Vec::as_slice).collect();
        BatchVectorRead::new(
            self.table,
            column,
            &query_refs,
            limit,
            &self.options,
            self.filter.as_ref(),
            self.include_row_ids.as_ref(),
            self.prepared_filter.as_ref(),
        )
    }

    /// Search every query against one plan, including empty per-query results.
    pub async fn execute(&self) -> crate::Result<Vec<SearchResult>> {
        let read = self.new_read()?;
        read.read(self.new_scan()?.plan().await?).await
    }

    fn column(&self) -> crate::Result<&str> {
        CoreOptions::new(self.table.schema().options()).ensure_read_authorized()?;
        self.vector_column
            .as_deref()
            .ok_or_else(|| crate::Error::ConfigInvalid {
                message: "Vector column must be set via with_vector_column()".to_string(),
            })
    }
}

#[cfg(test)]
mod tests;
