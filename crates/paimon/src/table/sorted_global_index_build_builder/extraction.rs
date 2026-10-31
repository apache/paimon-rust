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

//! Data Evolution split adapter for the shared native index build input.
use super::planning::SortedGlobalIndexShard;
#[cfg(test)]
pub(super) use crate::table::index_build::extraction::*;
use crate::table::stats_filter::group_by_overlapping_row_id;
use crate::table::{DataSplit, DataSplitBuilder, RowRange};
use crate::Result;
pub(super) fn build_read_splits_for_shard(
    shard: &SortedGlobalIndexShard,
) -> Result<Vec<DataSplit>> {
    let shard_range = RowRange::new(shard.row_range_start, shard.row_range_end);
    group_by_overlapping_row_id(shard.files.clone())
        .into_iter()
        .filter_map(|files| {
            let ranges = files
                .iter()
                .filter_map(|file| {
                    file.row_id_range()
                        .and_then(|(start, end)| shard_range.intersect_inclusive(start, end))
                })
                .collect::<Vec<_>>();
            let ranges = crate::table::merge_row_ranges(ranges);
            if ranges.is_empty() {
                return None;
            }
            let raw_convertible = files.len() == 1;
            // Like Java's global-index build splits, deliberately omit deletion
            // files: indexes cover physical row IDs. Readers apply the query
            // snapshot's deletion vectors, including deletes after this build.
            Some(
                DataSplitBuilder::new()
                    .with_snapshot(shard.snapshot_id)
                    .with_partition(shard.partition.clone())
                    .with_bucket(shard.source_bucket)
                    .with_bucket_path(shard.bucket_path.clone())
                    .with_total_buckets(shard.total_buckets)
                    .with_data_files(files)
                    .with_row_ranges(ranges)
                    .with_raw_convertible(raw_convertible)
                    .build(),
            )
        })
        .collect()
}
