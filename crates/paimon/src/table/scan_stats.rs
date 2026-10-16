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

//! Scan planning statistics.

use std::fmt;

/// How much metadata a scan should collect statistics for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanStatsMode {
    /// Record work performed by normal planning, retaining early pruning and LIMIT.
    Lightweight,
    /// Count every partition-selected manifest entry and all candidates before LIMIT.
    Complete,
}

/// Stage counters collected while planning a table scan.
///
/// The counters are intended for explain output and regression tests. They
/// describe pruning at metadata planning time only; reader-side Parquet row
/// group pruning and DataFusion residual filters are outside these statistics.
/// [`super::TableScan::with_stats`] enables collection and stores
/// this type on [`super::Plan::stats`]. Complete collection decodes all
/// entries of partition-selected manifests and counts candidates before LIMIT.
/// Snapshot metadata is available separately from [`super::Plan::snapshot_id`].
#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScanStats {
    /// True when all partition-selected entries were counted before bucket,
    /// sidecar and LIMIT optimizations. Lightweight statistics keep false.
    pub complete: bool,
    pub base_manifest_files: usize,
    pub delta_manifest_files: usize,
    pub manifest_files_before_partition_pruning: usize,
    pub manifest_files_after_partition_pruning: usize,
    pub manifest_files_pruned_by_row_ranges: usize,
    pub manifest_entries_read: usize,
    /// ADD plus DELETE records in manifest metadata, before partition pruning.
    pub entries_potential_total: u64,
    /// Complete-funnel counters, populated only when `complete` is true.
    pub entries_after_partition: usize,
    pub entries_after_bucket: usize,
    /// Selected physical file references for Data Evolution; otherwise entries
    /// surviving schema-aware file statistics before ADD/DELETE netting.
    pub entries_after_stats: usize,
    pub manifest_entries_pruned_by_bucket: usize,
    pub manifest_entries_pruned_by_partition: usize,
    pub manifest_entries_after_entry_pruning: usize,
    pub manifest_entries_pruned_by_level: usize,
    pub manifest_entries_pruned_by_row_ranges: usize,
    pub manifest_entries_pruned_by_data_stats: usize,
    pub manifest_entries_after_manifest_filters: usize,
    pub manifest_entries_after_merge: usize,
    pub manifest_entries_pruned_by_cross_schema_stats: usize,
    pub manifest_entries_after_cross_schema_stats: usize,
    pub data_evolution_groups_before_stats: usize,
    pub data_evolution_groups_pruned_by_stats: usize,
    pub data_evolution_groups_pruned_by_row_ranges: usize,
    pub split_candidates_built: usize,
    pub limit_early_stopped: bool,
    pub splits_before_limit: usize,
    pub splits_after_limit: usize,
    pub final_splits: usize,
    pub final_files: usize,
    /// Sum of known data-file sizes referenced by the final plan.
    pub planned_data_file_bytes: u64,
    pub limit: Option<usize>,
}

impl ScanStats {
    pub(crate) fn new(mode: ScanStatsMode) -> Self {
        Self {
            complete: mode == ScanStatsMode::Complete,
            ..Default::default()
        }
    }

    pub(crate) fn record_manifest_lists(&mut self, base_count: usize, delta_count: usize) {
        self.base_manifest_files = base_count;
        self.delta_manifest_files = delta_count;
        self.manifest_files_before_partition_pruning = base_count + delta_count;
    }

    pub(crate) fn record_final_plan(
        &mut self,
        splits_before_limit: usize,
        splits: usize,
        files: usize,
    ) {
        self.record_final_plan_with_limit(
            splits_before_limit,
            splits_before_limit,
            splits,
            files,
            false,
        );
    }

    pub(crate) fn record_final_plan_with_limit(
        &mut self,
        split_candidates_built: usize,
        splits_before_limit: usize,
        splits: usize,
        files: usize,
        limit_early_stopped: bool,
    ) {
        self.split_candidates_built = split_candidates_built;
        self.limit_early_stopped = limit_early_stopped;
        self.splits_before_limit = splits_before_limit;
        self.splits_after_limit = splits;
        self.final_splits = splits;
        self.final_files = files;
    }
}

impl fmt::Display for ScanStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "manifests={}/{}, manifest_row_range_pruned={}, entries_read={}, bucket_pruned={}, partition_pruned={}, entry_row_range_pruned={}, data_stats_pruned={}, cross_schema_pruned={}, split_candidates_built={}, limit_early_stopped={}, splits_before_limit={}, splits_after_limit={}, files={}, data_file_bytes={}",
            self.manifest_files_after_partition_pruning,
            self.manifest_files_before_partition_pruning,
            self.manifest_files_pruned_by_row_ranges,
            self.manifest_entries_read,
            self.manifest_entries_pruned_by_bucket,
            self.manifest_entries_pruned_by_partition,
            self.manifest_entries_pruned_by_row_ranges,
            self.manifest_entries_pruned_by_data_stats,
            self.manifest_entries_pruned_by_cross_schema_stats,
            self.split_candidates_built,
            self.limit_early_stopped,
            self.splits_before_limit,
            self.splits_after_limit,
            self.final_files,
            self.planned_data_file_bytes
        )
    }
}
