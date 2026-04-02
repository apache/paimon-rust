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

//! ReadBuilder and TableRead for table read API.
//!
//! Reference: [Java ReadBuilder.withProjection](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/table/source/ReadBuilder.java)
//! and [TypeUtils.project](https://github.com/apache/paimon/blob/master/paimon-common/src/main/java/org/apache/paimon/utils/TypeUtils.java).

use super::{ArrowRecordBatchStream, Table, TableScan};
use crate::arrow::ArrowReaderBuilder;
use crate::spec::{CoreOptions, DataField, Predicate};
use crate::Result;
use crate::{DataSplit, Error};
use std::collections::{HashMap, HashSet};

/// Builder for table scan and table read (new_scan, new_read).
///
/// Rust keeps a names-based projection API for ergonomics, while aligning the
/// resulting read semantics with Java Paimon's order-preserving projection.
#[derive(Debug, Clone)]
pub struct ReadBuilder<'a> {
    table: &'a Table,
    projected_fields: Option<Vec<String>>,
    filter: Option<Predicate>,
}

impl<'a> ReadBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            projected_fields: None,
            filter: None,
        }
    }

    /// Set column projection by name. Output order follows the caller-specified order.
    /// Unknown or duplicate names cause `new_read()` to fail; an empty list is a valid
    /// zero-column projection.
    pub fn with_projection(&mut self, columns: &[&str]) -> &mut Self {
        self.projected_fields = Some(columns.iter().map(|c| (*c).to_string()).collect());
        self
    }

    /// Set a filter predicate for scan planning.
    ///
    /// The predicate should use table schema field indices (as produced by
    /// [`PredicateBuilder`]).  During [`TableScan::plan`] the filter is
    /// decomposed at AND boundaries: partition-only conjuncts are extracted
    /// and used to prune partitions; all other conjuncts are currently
    /// **ignored** (neither `TableScan` nor `TableRead` applies data-level
    /// predicates yet).
    ///
    /// This means rows returned by `TableRead` may **not** satisfy the full
    /// filter — callers must apply remaining predicates themselves until
    /// data-level pushdown is implemented.
    pub fn with_filter(&mut self, filter: Predicate) -> &mut Self {
        self.filter = Some(filter);
        self
    }

    /// Create a table scan. Call [TableScan::plan] to get splits.
    pub fn new_scan(&self) -> TableScan<'a> {
        TableScan::new(self.table, self.filter.clone())
    }

    /// Create a table read for consuming splits (e.g. from a scan plan).
    pub fn new_read(&self) -> Result<TableRead<'a>> {
        let read_type = match &self.projected_fields {
            None => self.table.schema.fields().to_vec(),
            Some(projected) => self.resolve_projected_fields(projected)?,
        };

        Ok(TableRead::new(self.table, read_type))
    }

    fn resolve_projected_fields(&self, projected_fields: &[String]) -> Result<Vec<DataField>> {
        if projected_fields.is_empty() {
            return Ok(Vec::new());
        }

        let full_name = self.table.identifier().full_name();
        let field_map: HashMap<&str, &DataField> = self
            .table
            .schema
            .fields()
            .iter()
            .map(|field| (field.name(), field))
            .collect();

        let mut seen = HashSet::with_capacity(projected_fields.len());
        let mut resolved = Vec::with_capacity(projected_fields.len());

        for name in projected_fields {
            if !seen.insert(name.as_str()) {
                return Err(Error::ConfigInvalid {
                    message: format!("Duplicate projection column '{name}' for table {full_name}"),
                });
            }

            let field = field_map
                .get(name.as_str())
                .ok_or_else(|| Error::ColumnNotExist {
                    full_name: full_name.clone(),
                    column: name.clone(),
                })?;
            resolved.push((*field).clone());
        }

        Ok(resolved)
    }
}

/// Table read: reads data from splits (e.g. produced by [TableScan::plan]).
///
/// Reference: [pypaimon.read.table_read.TableRead](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/read/table_read.py)
#[derive(Debug, Clone)]
pub struct TableRead<'a> {
    table: &'a Table,
    read_type: Vec<DataField>,
}

impl<'a> TableRead<'a> {
    /// Create a new TableRead with a specific read type (projected fields).
    pub fn new(table: &'a Table, read_type: Vec<DataField>) -> Self {
        Self { table, read_type }
    }

    /// Schema (fields) that this read will produce.
    pub fn read_type(&self) -> &[DataField] {
        &self.read_type
    }

    /// Table for this read.
    pub fn table(&self) -> &Table {
        self.table
    }

    /// Returns an [`ArrowRecordBatchStream`].
    pub fn to_arrow(&self, data_splits: &[DataSplit]) -> crate::Result<ArrowRecordBatchStream> {
        // todo: consider get read batch size from table
        let has_primary_keys = !self.table.schema.primary_keys().is_empty();
        let core_options = CoreOptions::new(self.table.schema.options());
        let deletion_vectors_enabled = core_options.deletion_vectors_enabled();
        let data_evolution = core_options.data_evolution_enabled();

        if has_primary_keys && !deletion_vectors_enabled {
            return Err(Error::Unsupported {
                message: format!(
                    "Reading primary-key tables without deletion vectors is not yet supported. Primary keys: {:?}",
                    self.table.schema.primary_keys()
                ),
            });
        }

        let reader =
            ArrowReaderBuilder::new(self.table.file_io.clone()).build(self.read_type().to_vec());

        if data_evolution {
            reader.read_data_evolution(data_splits, self.table.schema.fields())
        } else {
            reader.read(data_splits)
        }
    }
}
