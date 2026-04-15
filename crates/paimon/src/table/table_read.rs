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

use super::data_evolution_reader::DataEvolutionReader;
use super::data_file_reader::DataFileReader;
use super::kv_file_reader::{KeyValueFileReader, KeyValueReadConfig};
use super::read_builder::split_scan_predicates;
use super::{ArrowRecordBatchStream, Table};
use crate::arrow::filtering::reader_pruning_predicates;
use crate::spec::{CoreOptions, DataField, Predicate};
use crate::DataSplit;

/// Table read: reads data from splits (e.g. produced by [TableScan::plan]).
///
/// Reference: [pypaimon.read.table_read.TableRead](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/read/table_read.py)
#[derive(Debug, Clone)]
pub struct TableRead<'a> {
    table: &'a Table,
    read_type: Vec<DataField>,
    data_predicates: Vec<Predicate>,
}

impl<'a> TableRead<'a> {
    /// Create a new TableRead with a specific read type (projected fields).
    pub fn new(
        table: &'a Table,
        read_type: Vec<DataField>,
        data_predicates: Vec<Predicate>,
    ) -> Self {
        Self {
            table,
            read_type,
            data_predicates,
        }
    }

    /// Schema (fields) that this read will produce.
    pub fn read_type(&self) -> &[DataField] {
        &self.read_type
    }

    /// Data predicates for read-side pruning.
    pub fn data_predicates(&self) -> &[Predicate] {
        &self.data_predicates
    }

    /// Table for this read.
    pub fn table(&self) -> &Table {
        self.table
    }

    /// Set a filter predicate for conservative read-side pruning.
    pub fn with_filter(mut self, filter: Predicate) -> Self {
        let (_, data_predicates) = split_scan_predicates(self.table, filter);
        self.data_predicates = reader_pruning_predicates(data_predicates);
        self
    }

    /// Returns an [`ArrowRecordBatchStream`].
    pub fn to_arrow(&self, data_splits: &[DataSplit]) -> crate::Result<ArrowRecordBatchStream> {
        let has_primary_keys = !self.table.schema.primary_keys().is_empty();
        let core_options = CoreOptions::new(self.table.schema.options());
        let deletion_vectors_enabled = core_options.deletion_vectors_enabled();
        let data_evolution = core_options.data_evolution_enabled();

        // PK table without DV: route by merge engine.
        // Exhaustive match ensures new MergeEngine variants trigger a compile error.
        if has_primary_keys && !deletion_vectors_enabled {
            match core_options.merge_engine()? {
                crate::spec::MergeEngine::Deduplicate => {
                    let reader = KeyValueFileReader::new(
                        self.table.file_io.clone(),
                        KeyValueReadConfig {
                            schema_manager: self.table.schema_manager().clone(),
                            table_schema_id: self.table.schema().id(),
                            table_fields: self.table.schema.fields().to_vec(),
                            read_type: self.read_type().to_vec(),
                            predicates: self.data_predicates.clone(),
                            primary_keys: self.table.schema.primary_keys().to_vec(),
                            sequence_fields: core_options
                                .sequence_fields()
                                .iter()
                                .map(|s| s.to_string())
                                .collect(),
                        },
                    );
                    return reader.read(data_splits);
                }
                crate::spec::MergeEngine::FirstRow => {
                    // Fall through to DataFileReader — scan already skips level-0
                }
            }
        }

        // PK table with DV or append-only table: DataFileReader / DataEvolutionReader
        if data_evolution {
            let reader = DataEvolutionReader::new(
                self.table.file_io.clone(),
                self.table.schema_manager().clone(),
                self.table.schema().id(),
                self.table.schema.fields().to_vec(),
                self.read_type().to_vec(),
            )?;
            reader.read(data_splits)
        } else {
            let reader = DataFileReader::new(
                self.table.file_io.clone(),
                self.table.schema_manager().clone(),
                self.table.schema().id(),
                self.table.schema.fields().to_vec(),
                self.read_type().to_vec(),
                self.data_predicates.clone(),
            );
            reader.read(data_splits)
        }
    }
}
