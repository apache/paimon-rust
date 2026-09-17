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

use crate::spec::{CoreOptions, DataField, ManifestEntry};
use crate::table::global_index_build_common::vector::{plan_vector_index_shards, VectorIndexShard};
use crate::table::RowRange;
use crate::Result;

pub(crate) type VindexIndexShard = VectorIndexShard;

#[allow(clippy::too_many_arguments)]
pub(super) fn plan_vindex_shards(
    table_location: &str,
    partition_keys: &[String],
    schema_fields: &[DataField],
    core_options: &CoreOptions,
    snapshot_id: i64,
    entries: Vec<ManifestEntry>,
    rows_per_shard: i64,
    indexed: &[RowRange],
) -> Result<Vec<VindexIndexShard>> {
    plan_vector_index_shards(
        table_location,
        partition_keys,
        schema_fields,
        core_options,
        snapshot_id,
        entries,
        rows_per_shard,
        indexed,
        "vindex",
    )
}
