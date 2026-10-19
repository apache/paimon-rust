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

//! Ranked materialization of a snapshot-scoped global-index result.

use super::{GlobalIndexResult, GlobalIndexSelection};
use crate::spec::CoreOptions;
use crate::table::data_file_reader::DataFileReader;
use crate::table::de_vector_read::{materialize_row_ids, DeVectorRead};
use crate::table::pk_vector_read::materialize_positions;
use crate::table::vector_search_common::resolve_materialize_read_type;
use crate::table::ArrowRecordBatchStream;

/// Reads selected user columns and `__paimon_search_score`, in relevance order.
/// Internal global row IDs and PK positions are not output. Reading a result
/// never reruns search or replans its primary-key source files.
#[derive(Debug, Clone)]
pub struct GlobalIndexResultReadBuilder<'a> {
    result: &'a GlobalIndexResult,
    projection: Option<Vec<String>>,
}

impl<'a> GlobalIndexResultReadBuilder<'a> {
    pub(super) fn new(result: &'a GlobalIndexResult) -> Self {
        Self {
            result,
            projection: None,
        }
    }

    /// Select user columns in this order. Defaults to every user table column.
    pub fn with_projection(&mut self, columns: &[&str]) -> &mut Self {
        self.projection = Some(columns.iter().map(|name| name.to_string()).collect());
        self
    }

    pub async fn read(&self) -> crate::Result<ArrowRecordBatchStream> {
        let context =
            self.result
                .read_context
                .as_ref()
                .ok_or_else(|| crate::Error::Unsupported {
                    message: "Direct result reads require a result returned by a table search"
                        .into(),
                })?;
        let table = &context.table;
        let core_options = CoreOptions::new(table.schema().options());
        core_options.ensure_read_authorized()?;
        match &self.result.selection {
            GlobalIndexSelection::ScoredRowIds(hits) => {
                let read_type = match &context.vector_column {
                    Some(column) => {
                        DeVectorRead::read_type(table, column, self.projection.as_deref())?
                    }
                    None => resolve_materialize_read_type(table, self.projection.as_deref())?,
                };
                materialize_row_ids(table, hits, read_type).await
            }
            GlobalIndexSelection::Splits(_) => {
                let positions =
                    context
                        .positions
                        .as_deref()
                        .ok_or_else(|| crate::Error::Unsupported {
                            message: "Direct primary-key reads require ranked search positions"
                                .into(),
                        })?;
                let splits = self.result.indexed_splits()?;
                let read_type = resolve_materialize_read_type(table, self.projection.as_deref())?;
                let reader = DataFileReader::new(
                    table.file_io().clone(),
                    table.schema_manager().clone(),
                    table.schema().id(),
                    table.schema().fields().to_vec(),
                    table.data_fields_extractor(),
                    read_type,
                    Vec::new(),
                )
                .with_table_options(table.schema().options().clone());
                materialize_positions(positions, &splits, &reader).await
            }
            GlobalIndexSelection::RowRanges(_) => Err(crate::Error::Unsupported {
                message: "Unscored row ranges must be read through a table scan".into(),
            }),
        }
    }
}
