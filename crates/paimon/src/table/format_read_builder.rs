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

//! Read builder for Java-compatible `type=format-table` metadata.

use super::partition_filter::PartitionFilter;
use super::read_builder::split_scan_predicates;
use super::read_builder::{resolve_projected_fields, validate_projection_possible};
use super::{Table, TableRead, TableScan};
use crate::spec::{DataField, Predicate};
use crate::table::source::RowRange;
use crate::Result;
use std::collections::HashSet;

#[derive(Debug, Clone)]
pub(crate) struct FormatReadBuilder<'a> {
    table: &'a Table,
    read_type: Option<Vec<DataField>>,
    /// Deferred projection column names, resolved lazily at read build time so
    /// projection and case sensitivity are order-independent. Mutually exclusive
    /// with `read_type`.
    projection_names: Option<Vec<String>>,
    partition_filter: Option<PartitionFilter>,
    data_predicates: Vec<Predicate>,
    limit: Option<usize>,
    case_sensitive: bool,
    filter_columns: HashSet<usize>,
}

impl<'a> FormatReadBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            read_type: None,
            projection_names: None,
            partition_filter: None,
            data_predicates: Vec::new(),
            limit: None,
            case_sensitive: true,
            filter_columns: HashSet::new(),
        }
    }

    pub(crate) fn table(&self) -> &'a Table {
        self.table
    }

    pub(crate) fn with_projection(&mut self, columns: &[&str]) -> Result<&mut Self> {
        let projection_names: Vec<String> = columns.iter().map(|c| (*c).to_string()).collect();
        validate_projection_possible(
            self.table.identifier().full_name(),
            self.table.schema().fields(),
            &projection_names,
        )?;
        self.projection_names = Some(projection_names);
        self.read_type = None;
        Ok(self)
    }

    /// Set whether column-name matching is case-sensitive. Defaults to `true`.
    pub(crate) fn with_case_sensitive(&mut self, case_sensitive: bool) -> &mut Self {
        self.case_sensitive = case_sensitive;
        self
    }

    pub(crate) fn with_read_type(&mut self, read_type: Vec<DataField>) -> &mut Self {
        self.read_type = Some(read_type);
        self.projection_names = None;
        self
    }

    pub(crate) fn with_filter(&mut self, filter: Predicate) -> &mut Self {
        // Capture the full predicate's columns before it is split, so masked and
        // out-of-scope partition keys can't prune on their raw value.
        self.filter_columns.clear();
        filter.collect_leaf_field_indices(&mut self.filter_columns);
        let (partition_predicate, data_predicates) = split_scan_predicates(self.table, filter);
        self.partition_filter = partition_predicate.map(|pred| {
            PartitionFilter::from_predicate(pred, &self.table.schema().partition_fields())
        });
        self.data_predicates = data_predicates;
        self
    }

    pub(crate) fn is_exact_filter_pushdown(&self, _filter: &Predicate) -> bool {
        false
    }

    pub(crate) fn with_row_ranges(&mut self, _ranges: Vec<RowRange>) -> &mut Self {
        self
    }

    pub(crate) fn with_limit(&mut self, limit: usize) -> &mut Self {
        self.limit = Some(limit);
        self
    }

    pub(crate) fn new_scan(&self) -> TableScan<'a> {
        TableScan::new(
            self.table,
            self.partition_filter.clone(),
            Vec::new(),
            None,
            self.limit,
            None,
        )
        .with_query_auth_scope(
            self.filter_columns.clone(),
            self.projected_schema_indices(),
            self.projected_system_field_names(),
        )
    }

    /// Table-schema indices of the projected columns (`None` = all).
    /// Projected system fields (`_ROW_ID`, …). `projected_schema_indices` drops
    /// them for lack of an index, but Java's `select` includes them.
    fn projected_system_field_names(&self) -> Vec<String> {
        // Format tables have no row-id extraction (`with_row_ranges` is inert),
        // so the read type is the only source.
        crate::table::query_auth::projected_system_field_names(
            self.resolve_read_type().ok().flatten().as_deref(),
            &std::collections::HashSet::new(),
            false,
        )
    }

    fn projected_schema_indices(&self) -> Option<Vec<usize>> {
        // Resolve names too (see `PaimonReadBuilder::projected_schema_indices`).
        self.resolve_read_type().ok().flatten().map(|fields| {
            fields
                .iter()
                .filter_map(|f| {
                    self.table
                        .schema()
                        .fields()
                        .iter()
                        .position(|s| s.id() == f.id())
                })
                .collect()
        })
    }

    pub(crate) fn new_read(&self) -> Result<TableRead<'a>> {
        // Query-auth is enforced in `TableRead::to_arrow` off the grant stamped
        // on the splits by planning; no gate needed here (see the Paimon
        // `PaimonReadBuilder::new_read`).
        let read_type = match self.resolve_read_type()? {
            None => self.table.schema().fields().to_vec(),
            Some(fields) => fields,
        };
        Ok(TableRead::new_format(
            self.table,
            read_type,
            self.data_predicates.clone(),
            self.limit,
        ))
    }

    /// Resolve the effective read type, deferring projection name resolution to
    /// the case sensitivity effective at build time (order-independent with
    /// `with_case_sensitive`).
    fn resolve_read_type(&self) -> Result<Option<Vec<DataField>>> {
        if let Some(read_type) = &self.read_type {
            return Ok(Some(read_type.clone()));
        }
        if let Some(names) = &self.projection_names {
            return Ok(Some(resolve_projected_fields(
                self.table.identifier().full_name(),
                self.table.schema().fields(),
                names,
                self.case_sensitive,
            )?));
        }
        Ok(None)
    }
}
