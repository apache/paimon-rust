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

//! Snapshot-bound index tasks, independent of their scheduling framework.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::global_index_build_common::vector::VectorIndexShard;
use super::vindex_index_build_builder::timing::VectorIndexBuildTiming;
use super::vindex_index_build_builder::VindexIndexBuildBuilder;
use super::{CommitMessage, RowRange, Table};
use crate::spec::{Snapshot, TableSchema};
use crate::{Error, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BuildContext {
    location: String,
    identifier: String,
    branch: String,
    schema: TableSchema,
    snapshot: Option<Snapshot>,
    column: String,
    index_type: String,
    options: HashMap<String, String>,
}

/// An immutable global-index build over one captured table snapshot.
/// Contains metadata only: clients, FileIO and runtime handles stay in Table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalIndexBuildPlan {
    context: BuildContext,
    shards: Vec<VectorIndexShard>,
}

impl GlobalIndexBuildPlan {
    pub(crate) fn new(
        table: &Table,
        snapshot: Option<Snapshot>,
        column: String,
        index_type: String,
        options: HashMap<String, String>,
        shards: Vec<VectorIndexShard>,
    ) -> Self {
        Self {
            context: BuildContext {
                location: table.location().to_string(),
                identifier: table.identifier().full_name(),
                branch: table.branch().to_string(),
                schema: table.schema().clone(),
                snapshot,
                column,
                index_type,
                options,
            },
            shards,
        }
    }

    pub fn snapshot_id(&self) -> Option<i64> {
        self.context.snapshot.as_ref().map(Snapshot::id)
    }

    /// Each task carries the same captured schema, options and snapshot.
    pub fn shards(&self) -> Vec<GlobalIndexBuildShard> {
        self.shards
            .iter()
            .cloned()
            .map(|shard| GlobalIndexBuildShard {
                context: self.context.clone(),
                shard,
            })
            .collect()
    }
}

/// A complete physical row range and its files, executable on another worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalIndexBuildShard {
    context: BuildContext,
    shard: VectorIndexShard,
}

impl GlobalIndexBuildShard {
    pub fn snapshot_id(&self) -> i64 {
        self.shard.snapshot_id
    }

    pub fn row_range(&self) -> RowRange {
        RowRange::new(self.shard.row_range_start, self.shard.row_range_end)
    }

    pub(in crate::table) async fn build(
        &self,
        table: &Table,
    ) -> Result<Option<(CommitMessage, Option<VectorIndexBuildTiming>)>> {
        // A serialized task grants no authorization. Check the worker's table
        // before replacing its schema with the captured read view.
        table.schema().core_options().ensure_read_authorized()?;
        table.ensure_not_branch_reference_for_write()?;
        if table.location() != self.context.location
            || table.identifier().full_name() != self.context.identifier
            || table.branch() != self.context.branch
        {
            return Err(Error::DataInvalid {
                message: "Global index build shard belongs to a different table or branch".into(),
                source: None,
            });
        }
        let snapshot = self
            .context
            .snapshot
            .as_ref()
            .ok_or_else(|| Error::DataInvalid {
                message: "Global index build shard requires a captured snapshot".into(),
                source: None,
            })?;
        if snapshot.id() != self.shard.snapshot_id
            || self.shard.row_range_start < 0
            || self.shard.row_range_end < self.shard.row_range_start
        {
            return Err(Error::DataInvalid {
                message: "Invalid global index build shard snapshot or row range".into(),
                source: None,
            });
        }
        let table = table
            .copy_with_resolved_schema(self.context.schema.clone(), &self.context.branch)?
            .copy_with_pinned_snapshot(Some(snapshot));
        let mut builder = VindexIndexBuildBuilder::new(&table, &self.context.index_type);
        builder
            .with_index_column(&self.context.column)
            .with_options(self.context.options.clone());
        builder.build_planned_shard(&self.shard).await
    }
}
