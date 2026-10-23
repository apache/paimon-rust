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
use crate::table::vector_scan::{PlanContext, VectorScan};
use crate::table::vector_search_common::{add_filter, validate_partition_filter};
use crate::table::Table;
use crate::vector_search::GlobalIndexResult;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Configures a vector query. Clones share the same live configuration.
///
/// Each scan, reader, or execution captures its settings before performing I/O;
/// later edits affect future requests without changing work already created.
#[derive(Clone)]
pub struct VectorSearchBuilder<'a> {
    table: Cow<'a, Table>,
    config: Arc<RwLock<VectorSearchConfig>>,
}

#[derive(Clone, Default)]
struct VectorSearchConfig {
    vector_column: Option<String>,
    query_vector: Option<Vec<f32>>,
    limit: Option<usize>,
    options: HashMap<String, String>,
    filter: Option<Predicate>,
}

impl<'a> VectorSearchBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table: Cow::Borrowed(table),
            config: Arc::new(RwLock::new(VectorSearchConfig::default())),
        }
    }

    pub(crate) fn from_owned(table: Arc<Table>) -> VectorSearchBuilder<'static> {
        VectorSearchBuilder {
            table: Cow::Owned(table.as_ref().clone()),
            config: Arc::new(RwLock::new(VectorSearchConfig::default())),
        }
    }

    /// Retain this builder's table and shared configuration without a borrow.
    pub fn into_owned(self) -> VectorSearchBuilder<'static> {
        VectorSearchBuilder {
            table: Cow::Owned(self.table.into_owned()),
            config: self.config,
        }
    }

    pub fn with_vector_column(&mut self, name: &str) -> &mut Self {
        self.config.write().unwrap().vector_column = Some(name.to_string());
        self
    }

    pub fn with_query_vector(&mut self, vector: Vec<f32>) -> &mut Self {
        self.config.write().unwrap().query_vector = Some(vector);
        self
    }

    pub fn with_limit(&mut self, limit: usize) -> &mut Self {
        self.config.write().unwrap().limit = Some(limit);
        self
    }

    pub fn with_options(&mut self, options: HashMap<String, String>) -> &mut Self {
        self.config.write().unwrap().options.extend(options);
        self
    }

    /// Attach a scalar predicate applied before vector Top-K.
    ///
    /// On the primary-key vector path this remains a residual allow-list over
    /// physical positions, mirroring Java `PrimaryKeyVectorRead`. On the
    /// data-evolution/global-index path an exact scalar-index lookup supplies
    /// the vector backend's row-ID include filter. Candidate-only answers are
    /// refined against snapshot-pinned data when
    /// `global-index.filter.refine-from-data=true`; otherwise those candidates
    /// are excluded. If no scalar index can evaluate the predicate, indexed
    /// ranges are scored from raw data even in FAST mode. Raw searches always
    /// apply the row predicate before Top-K, matching Java.
    pub fn with_filter(&mut self, filter: Predicate) -> &mut Self {
        add_filter(&mut self.config.write().unwrap().filter, filter);
        self
    }

    /// Add a partition-only predicate expressed against the table schema.
    pub fn with_partition_filter(&mut self, filter: Predicate) -> crate::Result<&mut Self> {
        validate_partition_filter(&self.table, &filter)?;
        Ok(self.with_filter(filter))
    }

    /// Create a query-independent scan. Only the vector column must be configured.
    pub fn new_scan(&self) -> crate::Result<VectorScan> {
        let config = self.config.read().unwrap().clone();
        self.scan_with_config(&config)
    }

    fn scan_with_config(&self, config: &VectorSearchConfig) -> crate::Result<VectorScan> {
        CoreOptions::new(self.table.schema().options()).ensure_read_authorized()?;
        let column =
            config
                .vector_column
                .as_deref()
                .ok_or_else(|| crate::Error::ConfigInvalid {
                    message: "Vector column must be set via with_vector_column()".to_string(),
                })?;
        VectorScan::new(&self.table, column, config.filter.as_ref(), None, None)
            .map(|scan| scan.with_options(&config.options))
    }

    /// Create an owned reader; query errors are reported before planning.
    pub fn new_read(&self) -> crate::Result<VectorRead> {
        let config = self.config.read().unwrap().clone();
        self.read_with_config(&config)
    }

    fn read_with_config(&self, config: &VectorSearchConfig) -> crate::Result<VectorRead> {
        let (column, query, limit) = self.query(config)?;
        let context = PlanContext::new(&self.table, column, config.filter.as_ref(), None, None)?;
        Ok(VectorRead {
            batch: BatchVectorRead::new(
                &self.table,
                column,
                &[query],
                limit,
                &config.options,
                config.filter.as_ref(),
                context,
            )?,
        })
    }

    pub(crate) fn new_scan_and_read(&self) -> crate::Result<(VectorScan, VectorRead)> {
        let config = self.config.read().unwrap().clone();
        let read = self.read_with_config(&config)?;
        Ok((self.scan_with_config(&config)?, read))
    }

    /// Search locally using the same Scan -> Plan -> Read API exposed to engines.
    /// Use the result's `new_read_builder()` to materialize projected columns.
    pub async fn execute(&self) -> crate::Result<GlobalIndexResult> {
        let (scan, read) = self.new_scan_and_read()?;
        read.read(scan.plan().await?).await
    }

    fn query<'c>(
        &self,
        config: &'c VectorSearchConfig,
    ) -> crate::Result<(&'c str, &'c [f32], usize)> {
        CoreOptions::new(self.table.schema().options()).ensure_read_authorized()?;
        let column =
            config
                .vector_column
                .as_deref()
                .ok_or_else(|| crate::Error::ConfigInvalid {
                    message: "Vector column must be set via with_vector_column()".to_string(),
                })?;
        let vector = config
            .query_vector
            .as_deref()
            .ok_or_else(|| crate::Error::ConfigInvalid {
                message: "Query vector must be set via with_query_vector()".to_string(),
            })?;
        let limit = config.limit.ok_or_else(|| crate::Error::ConfigInvalid {
            message: "Limit must be set via with_limit()".to_string(),
        })?;
        Ok((column, vector, limit))
    }
}

#[cfg(test)]
mod tests;
