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

use super::planning::VindexIndexShard;
pub(super) use crate::table::index_build::vector_extraction::*;
use crate::table::{DataSplit, DataSplitBuilder, RowRange};
use crate::Result;
pub(super) fn data_split_for_shard(shard: &VindexIndexShard) -> Result<DataSplit> {
    data_split_for_shard_ranges(
        shard,
        vec![RowRange::new(shard.row_range_start, shard.row_range_end)],
    )
}

pub(super) fn data_split_for_shard_ranges(
    shard: &VindexIndexShard,
    row_ranges: Vec<RowRange>,
) -> Result<DataSplit> {
    // Java builds over stable physical row IDs, including deleted vectors.
    // Do not attach deletion files: live-row filtering belongs to the query
    // snapshot, and filtering here would leave holes in the training input.
    DataSplitBuilder::new()
        .with_snapshot(shard.snapshot_id)
        .with_partition(shard.partition.clone())
        .with_bucket(shard.source_bucket)
        .with_bucket_path(shard.bucket_path.clone())
        .with_total_buckets(shard.total_buckets)
        .with_data_files(shard.files.clone())
        .with_row_ranges(row_ranges)
        .build()
}
