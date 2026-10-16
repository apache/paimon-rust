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

//! Complete metadata-pruning counters for an explicitly requested explain pass.

/// Counts manifest entries, including ADD and DELETE records, at each filter.
/// These are planning counters, not estimates of visible data rows. Manifest
/// partition pruning contributes to `entries_potential_total`; entry counters
/// then follow partition, bucket and file-statistics filtering in that order.
///
/// Unlike the lightweight [`super::ScanTrace`], collecting these counters reads
/// all entries of partition-selected manifests, bypassing early bucket and
/// sidecar filters. Normal planning keeps those optimizations.
#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScanStats {
    pub manifest_files_total: usize,
    pub manifest_files_after_partition: usize,
    pub entries_potential_total: u64,
    pub entries_total: usize,
    pub entries_after_partition: usize,
    pub entries_after_bucket: usize,
    /// Data-evolution scans count the selected physical files after group
    /// statistics and projection, before LIMIT. Other scans count entries
    /// surviving file statistics (including cross-schema statistics).
    pub entries_after_stats: usize,
}
