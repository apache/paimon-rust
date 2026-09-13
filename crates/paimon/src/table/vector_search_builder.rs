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

//! Configures vector queries and dispatches to global-index or primary-key readers.

use crate::spec::{CoreOptions, Predicate};
use crate::table::vector_read::{BatchVectorRead, VectorRead};
use crate::table::vector_scan::VectorScan;
use crate::table::Table;
use crate::vector_search::SearchResult;
use std::collections::HashMap;

pub struct VectorSearchBuilder<'a> {
    table: &'a Table,
    vector_column: Option<String>,
    query_vector: Option<Vec<f32>>,
    limit: Option<usize>,
    options: HashMap<String, String>,
    filter: Option<Predicate>,
}

impl<'a> VectorSearchBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            vector_column: None,
            query_vector: None,
            limit: None,
            options: HashMap::new(),
            filter: None,
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

    pub fn with_options(&mut self, options: HashMap<String, String>) -> &mut Self {
        self.options = options;
        self
    }

    /// Attach a scalar predicate applied before vector Top-K.
    ///
    /// On the primary-key vector path this remains a residual allow-list over
    /// physical positions, mirroring Java `PrimaryKeyVectorRead`. On the
    /// data-evolution/global-index path the predicate is evaluated through a
    /// snapshot-pinned table read (which can use scalar global indexes such as
    /// BTree), producing global row IDs that are localized for each vector-index
    /// shard and passed to the vector backend as an include filter.
    ///
    /// The whole predicate is both pushed into the scan — where it prunes whole
    /// data files by their column stats — and applied per row as a residual over
    /// the surviving files, so results stay exact. Sub-file row-range narrowing is
    /// not performed; a surviving file is re-read in full for the residual.
    pub fn with_filter(&mut self, filter: Predicate) -> &mut Self {
        self.filter = Some(filter);
        self
    }

    /// Create a query-independent scan. Only the vector column must be configured.
    pub fn new_scan(&self) -> crate::Result<VectorScan> {
        CoreOptions::new(self.table.schema().options()).ensure_read_authorized()?;
        let column = self
            .vector_column
            .as_deref()
            .ok_or_else(|| crate::Error::ConfigInvalid {
                message: "Vector column must be set via with_vector_column()".to_string(),
            })?;
        VectorScan::new(self.table, column, self.filter.as_ref(), None, None)
    }

    /// Create an owned reader; query errors are reported before planning.
    pub fn new_read(&self) -> crate::Result<VectorRead> {
        let (column, query, limit) = self.query()?;
        Ok(VectorRead {
            batch: BatchVectorRead::new(
                self.table,
                column,
                &[query],
                limit,
                &self.options,
                self.filter.as_ref(),
                None,
                None,
            )?,
        })
    }

    /// Search locally using the same Scan -> Plan -> Read API exposed to engines.
    /// Use the result's `new_read_builder()` to materialize projected columns.
    pub async fn execute(&self) -> crate::Result<SearchResult> {
        let read = self.new_read()?;
        read.read(self.new_scan()?.plan().await?).await
    }

    fn query(&self) -> crate::Result<(&str, &[f32], usize)> {
        CoreOptions::new(self.table.schema().options()).ensure_read_authorized()?;
        let column = self
            .vector_column
            .as_deref()
            .ok_or_else(|| crate::Error::ConfigInvalid {
                message: "Vector column must be set via with_vector_column()".to_string(),
            })?;
        let vector = self
            .query_vector
            .as_deref()
            .ok_or_else(|| crate::Error::ConfigInvalid {
                message: "Query vector must be set via with_query_vector()".to_string(),
            })?;
        let limit = self.limit.ok_or_else(|| crate::Error::ConfigInvalid {
            message: "Limit must be set via with_limit()".to_string(),
        })?;
        Ok((column, vector, limit))
    }
}

#[cfg(test)]
mod tests;
