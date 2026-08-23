// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeSet;

use paimon::table::Table;
use serde::{Deserialize, Serialize};

use crate::error::{LookupError, Result};
use crate::key::{supports_global_btree, validate_key_type};
use crate::model::TableRef;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LookupStrategy {
    PrimaryKey,
    GlobalBtree,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueryBudget {
    pub max_batch_keys: usize,
    pub max_planned_files: usize,
    pub max_planned_bytes: u64,
}

impl Default for QueryBudget {
    fn default() -> Self {
        Self {
            max_batch_keys: 200,
            max_planned_files: 16,
            max_planned_bytes: 128 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TableLookupPolicy {
    pub table: TableRef,
    pub key_fields: Vec<String>,
    pub blob_fields: BTreeSet<String>,
    pub strategy: LookupStrategy,
    #[serde(default)]
    pub budget: QueryBudget,
}

impl TableLookupPolicy {
    pub fn validate_definition(&self) -> Result<()> {
        if self.key_fields.is_empty() {
            return Err(LookupError::InvalidPolicy(format!(
                "{} has no key fields",
                self.table.full_name()
            )));
        }
        if self.key_fields.iter().collect::<BTreeSet<_>>().len() != self.key_fields.len() {
            return Err(LookupError::InvalidPolicy(format!(
                "{} has duplicate key fields",
                self.table.full_name()
            )));
        }
        if self.blob_fields.is_empty() {
            return Err(LookupError::InvalidPolicy(format!(
                "{} has no allowed BLOB fields",
                self.table.full_name()
            )));
        }
        if self.budget.max_batch_keys == 0
            || self.budget.max_planned_files == 0
            || self.budget.max_planned_bytes == 0
        {
            return Err(LookupError::InvalidPolicy(format!(
                "{} has a zero query budget",
                self.table.full_name()
            )));
        }
        Ok(())
    }

    pub(crate) fn validate_table(&self, table: &Table) -> Result<()> {
        self.validate_table_for_blob_fields(
            table,
            self.blob_fields.iter().map(String::as_str),
            false,
        )
    }

    pub(crate) fn validate_table_for_request(
        &self,
        table: &Table,
        blob_fields: &[String],
    ) -> Result<()> {
        self.validate_table_for_blob_fields(table, blob_fields.iter().map(String::as_str), true)
    }

    fn validate_table_for_blob_fields<'a>(
        &self,
        table: &Table,
        blob_fields: impl IntoIterator<Item = &'a str>,
        request_context: bool,
    ) -> Result<()> {
        self.validate_definition()?;
        let schema = table.schema();
        for key in &self.key_fields {
            let field = schema
                .fields()
                .iter()
                .find(|field| field.name() == key)
                .ok_or_else(|| {
                    LookupError::InvalidPolicy(format!(
                        "lookup key field '{key}' does not exist in {}",
                        self.table.full_name()
                    ))
                })?;
            validate_key_type(key, field.data_type())?;
            if self.strategy == LookupStrategy::GlobalBtree
                && !supports_global_btree(field.data_type())
            {
                return Err(LookupError::InvalidPolicy(format!(
                    "GLOBAL_BTREE lookup key field '{key}' in {} has a type that cannot be indexed by a sorted global index: {:?}",
                    self.table.full_name(),
                    field.data_type()
                )));
            }
        }
        for blob in blob_fields {
            let Some(field) = schema.fields().iter().find(|field| field.name() == blob) else {
                let message = format!(
                    "BLOB field '{blob}' does not exist in the selected snapshot of {}",
                    self.table.full_name()
                );
                return Err(if request_context {
                    LookupError::InvalidRequest(message)
                } else {
                    LookupError::InvalidPolicy(message)
                });
            };
            if !field.data_type().is_blob_type() {
                let message = format!(
                    "field '{blob}' in the selected snapshot of {} is not a scalar BLOB",
                    self.table.full_name()
                );
                return Err(if request_context {
                    LookupError::InvalidRequest(message)
                } else {
                    LookupError::InvalidPolicy(message)
                });
            }
        }

        match self.strategy {
            LookupStrategy::PrimaryKey => {
                let configured = self.key_fields.iter().collect::<BTreeSet<_>>();
                let primary = schema.primary_keys().iter().collect::<BTreeSet<_>>();
                if primary.is_empty() || configured != primary {
                    return Err(LookupError::InvalidPolicy(format!(
                        "{} PRIMARY_KEY lookup must configure the complete primary key {:?}",
                        self.table.full_name(),
                        schema.primary_keys()
                    )));
                }
            }
            LookupStrategy::GlobalBtree => {
                let options = schema.core_options();
                if !options.data_evolution_enabled()
                    || !options.row_tracking_enabled()
                    || !options.global_index_enabled()
                    || options.deletion_vectors_enabled()
                    || !schema.primary_keys().is_empty()
                {
                    return Err(LookupError::InvalidPolicy(format!(
                        "{} GLOBAL_BTREE lookup requires a row-tracking data-evolution table with global indexes enabled, no primary key, and no deletion vectors",
                        self.table.full_name()
                    )));
                }
            }
        }
        Ok(())
    }
}
