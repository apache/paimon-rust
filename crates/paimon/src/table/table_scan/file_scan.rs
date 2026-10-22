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

//! File metadata planning before row selection and split packing.

use super::{
    read_all_manifest_entries, retain_primary_key_conjuncts, BucketSelector, CoreOptions,
    ManifestEntry, ManifestListSource, PaimonTableScan, ScanStats, TableScan, TableScanKind,
};
use crate::table::incremental_scan::timestamp_snapshot_range;
use crate::table::{IncrementalScan, IncrementalScanMode};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy)]
pub(super) struct FileScanOptions {
    pub(super) skip_level0: bool,
    pub(super) only_read_real_buckets: bool,
}

/// A selected snapshot's filtered files, before split packing or row selection.
#[derive(Debug, Clone)]
pub struct FileScanPlan {
    files: Vec<ManifestEntry>,
    snapshot_id: Option<i64>,
}

impl FileScanPlan {
    pub fn files(&self) -> &[ManifestEntry] {
        &self.files
    }

    /// None for an empty table or a timestamp window with no selected snapshot.
    pub fn snapshot_id(&self) -> Option<i64> {
        self.snapshot_id
    }
}

impl TableScan<'_> {
    /// Override file-level policies without changing snapshot selection or
    /// whole-file metadata authorization. Unconfigured queries keep their
    /// normal merge-engine and deletion-vector policies.
    pub fn with_file_filters(
        mut self,
        skip_level0: bool,
        only_read_real_buckets: bool,
    ) -> crate::Result<Self> {
        let TableScanKind::Paimon(scan) = &mut self.0 else {
            return Err(crate::Error::Unsupported {
                message: "File filters require a Paimon table".into(),
            });
        };
        scan.file_scan_options = Some(FileScanOptions {
            skip_level0,
            only_read_real_buckets,
        });
        Ok(self)
    }

    /// Plan filtered, merged file entries without applying projections, LIMIT,
    /// row ranges, split distribution, or global-index selections.
    ///
    /// The independent policies match FileStoreScan: skipping level zero never
    /// changes whether pending buckets are visible. Metadata requires an
    /// unrestricted whole-table grant even when the row reader projects fields.
    pub async fn plan_files(
        &self,
        skip_level0: bool,
        only_read_real_buckets: bool,
    ) -> crate::Result<FileScanPlan> {
        let mut requested = self.clone();
        let TableScanKind::Paimon(scan) = &mut requested.0 else {
            return Err(crate::Error::Unsupported {
                message: "Format tables do not have Paimon manifest entries".into(),
            });
        };
        scan.projected_read_field_ids = None;
        scan.row_ranges = None;
        scan.split_selection = None;
        scan.file_scan_options = None;
        scan.limit = None;
        // Authorize before turning on trusted metadata-reader behavior.
        scan.scan_all_files = false;
        let authorized = requested.authorize_all_files().await?;
        let TableScanKind::Paimon(scan) = &authorized.0 else {
            unreachable!("file metadata planning only accepts Paimon tables")
        };
        let plan = scan
            .plan_file_entries(skip_level0, only_read_real_buckets)
            .await?;
        scan.check_planned_files().await?;
        Ok(plan)
    }
}

impl PaimonTableScan<'_> {
    async fn plan_file_entries(
        &self,
        skip_level0: bool,
        only_read_real_buckets: bool,
    ) -> crate::Result<FileScanPlan> {
        let options = self.table.schema().core_options();
        if let Some((start, end)) = options.incremental_timestamp_window()? {
            let Some((start, end)) =
                timestamp_snapshot_range(&self.table.snapshot_manager(), start, end).await?
            else {
                return Ok(FileScanPlan {
                    files: Vec::new(),
                    snapshot_id: None,
                });
            };
            let incremental = IncrementalScan::new(
                &self.table,
                TableScan(TableScanKind::Paimon(self.clone())),
                IncrementalScanMode::Auto,
                start,
                end,
            );
            let (snapshot, lists) = incremental.selected_event_manifest_lists().await?;
            let files = self
                .read_file_entries(
                    ManifestListSource::IncrementalLists(lists),
                    skip_level0,
                    only_read_real_buckets,
                )
                .await?;
            return Ok(FileScanPlan {
                files,
                snapshot_id: Some(snapshot.id()),
            });
        }
        let Some(snapshot) = super::super::time_travel::resolve_snapshot(&self.table).await? else {
            return Ok(FileScanPlan {
                files: Vec::new(),
                snapshot_id: None,
            });
        };
        let files = self
            .read_file_entries(
                ManifestListSource::Snapshot(&snapshot),
                skip_level0,
                only_read_real_buckets,
            )
            .await?;
        Ok(FileScanPlan {
            files,
            snapshot_id: Some(snapshot.id()),
        })
    }

    async fn read_file_entries(
        &self,
        source: ManifestListSource<'_>,
        skip_level0: bool,
        only_read_real_buckets: bool,
    ) -> crate::Result<Vec<ManifestEntry>> {
        let options = CoreOptions::new(self.table.schema().options());
        let primary_key = !self.table.schema().primary_keys().is_empty();
        let data_predicates = if options.data_evolution_enabled() {
            Vec::new()
        } else if primary_key && !skip_level0 {
            retain_primary_key_conjuncts(
                &self.data_predicates,
                self.table.schema().fields(),
                &self.table.schema().trimmed_primary_keys(),
            )
        } else {
            self.data_predicates.clone()
        };
        let key_predicates = self.key_stats_predicates(&data_predicates);
        let bucket_selector = self
            .bucket_predicate
            .as_ref()
            .map(|predicate| BucketSelector::new(&self.table, predicate))
            .transpose()?
            .flatten();
        let entries = read_all_manifest_entries(
            &self.table,
            source,
            skip_level0,
            true,
            primary_key,
            self.partition_filter.as_ref(),
            &self.table.schema().partition_fields(),
            &data_predicates,
            &key_predicates,
            self.table.schema().id(),
            self.table.schema().fields(),
            &self.table.schema().trimmed_primary_keys(),
            bucket_selector.as_ref(),
            None,
            options.manifest_sidecar_enabled(),
            options.scan_manifest_parallelism()?,
            &mut ScanStats::default(),
        )
        .await?;
        let mut files = Vec::with_capacity(entries.len());
        let mut schema_cache = HashMap::new();
        for entry in entries {
            if only_read_real_buckets && entry.bucket() < 0 {
                continue;
            }
            // Older-schema stats need the same field-id evolution as row scans.
            if entry.file().schema_id != self.table.schema().id()
                && !data_predicates.is_empty()
                && !super::data_file_matches_predicates_for_table(
                    &self.table,
                    entry.file(),
                    &data_predicates,
                    &key_predicates,
                    &mut schema_cache,
                )
                .await
            {
                continue;
            }
            files.push(entry);
        }
        Ok(files)
    }
}
