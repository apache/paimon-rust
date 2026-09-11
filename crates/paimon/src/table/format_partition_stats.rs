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

//! Measures the partitions of a Format Table with catalog-managed partitions.

use std::collections::HashMap;

use futures::{StreamExt, TryStreamExt};

use super::format_partition::FormatTablePartitionPaths;
use super::format_table_scan::{list_format_table_data_files, supported_format_table_extension};
use super::Table;
use crate::arrow::format::read_file_row_count;
use crate::io::FileStatus;
use crate::spec::{CoreOptions, Partition, PartitionStatistics};

/// Measures what the partitions of a Format Table currently hold.
///
/// File count, byte size and last file creation time come from a directory listing. The row
/// count needs every file's footer, which no listing opens, so it is asked for rather than
/// assumed. A partition holding nothing measures as an exact zero, with no last file to date.
///
/// It lists through the listing the scan uses, so a measurement counts exactly the files a
/// reader would return and committer staging trees are left out. A listing failure aborts the
/// whole collection: a truncated listing looks exactly like a partition that lost files.
///
/// The result is a whole-partition measurement, so a catalog should replace what it holds with
/// it rather than add it up. It never decides that a partition should exist; it measures the
/// ones it is given.
///
/// Mirrors Java `FormatTablePartitionStatsCollector`.
#[derive(Debug)]
pub struct FormatTablePartitionStatsCollector<'a> {
    table: &'a Table,
    with_record_count: bool,
    parallelism: usize,
}

impl<'a> FormatTablePartitionStatsCollector<'a> {
    /// Measure `table`, reading file footers for row counts only when `with_record_count` is set.
    ///
    /// `parallelism` bounds the storage requests in flight: partition listings and footer reads
    /// share it, so it applies to one large partition as much as to many small ones. A value below
    /// one is read as one.
    pub fn new(table: &'a Table, with_record_count: bool, parallelism: usize) -> Self {
        Self {
            table,
            with_record_count,
            parallelism: parallelism.max(1),
        }
    }

    /// Measure the given complete partition specs. The result is aligned with `partitions` one for
    /// one, so it can be sent to the catalog together with the same specs.
    pub async fn collect(
        &self,
        partitions: &[HashMap<String, String>],
    ) -> crate::Result<Vec<PartitionStatistics>> {
        if partitions.is_empty() {
            return Ok(Vec::new());
        }
        if !self.table.has_catalog_managed_partitions() {
            return Err(crate::Error::Unsupported {
                message: format!(
                    "Format Table {} does not have catalog-managed partitions, so its partitions \
                     cannot be measured",
                    self.table.identifier().full_name()
                ),
            });
        }
        let options = CoreOptions::new(self.table.schema().options());
        let format_extension = supported_format_table_extension(&options.file_format())?;
        let partition_paths = FormatTablePartitionPaths::new(
            self.table.schema().partition_keys().iter().cloned(),
            options.format_table_partition_only_value_in_path(),
        );
        let table_path = options
            .path()
            .unwrap_or_else(|| self.table.location())
            .trim_end_matches('/');
        let directories = partitions
            .iter()
            .map(|spec| {
                partition_paths
                    .relative_path(spec)
                    .map(|relative_path| format!("{table_path}/{relative_path}"))
            })
            .collect::<crate::Result<Vec<_>>>()?;

        let file_io = self.table.file_io();
        // Each future owns what it lists. A stream over borrowed items would leave the future of
        // any SQL statement that measures partitions without a provable `Send`.
        let listings: Vec<Vec<FileStatus>> = futures::stream::iter(directories)
            .map(|directory| async move {
                // Each directory is a complete partition, so no partition level lies below it.
                list_format_table_data_files(file_io, &directory, 0, format_extension).await
            })
            .buffered(self.parallelism)
            .try_collect()
            .await?;

        let record_counts = if self.with_record_count {
            self.count_rows(&options.file_format(), &listings).await
        } else {
            vec![Partition::UNKNOWN; listings.len()]
        };

        Ok(partitions
            .iter()
            .zip(&listings)
            .zip(record_counts)
            .map(|((spec, files), record_count)| statistics(spec, files, record_count))
            .collect())
    }

    /// The rows each listed partition holds. Every file of every partition goes through one
    /// bounded stream, so a partition with many files is counted with all of it.
    async fn count_rows(&self, file_format: &str, listings: &[Vec<FileStatus>]) -> Vec<i64> {
        let file_io = self.table.file_io();
        let files = listings
            .iter()
            .enumerate()
            .flat_map(|(index, files)| {
                files
                    .iter()
                    .map(move |file| (index, file.path.clone(), file.size))
            })
            .collect::<Vec<_>>();
        let counts: Vec<(usize, Option<i64>)> = futures::stream::iter(files)
            .map(|(index, path, size)| async move {
                let count = match read_file_row_count(file_io, file_format, &path, size).await {
                    Ok(count) => count,
                    Err(error) => {
                        log::warn!(
                            "Failed to read the row count of {path} in table {}; the row count \
                             of its partition stays unknown: {error}",
                            self.table.identifier().full_name()
                        );
                        None
                    }
                };
                (index, count)
            })
            .buffered(self.parallelism)
            .collect()
            .await;

        // A partition with no files counted nothing and so holds exactly zero rows. One file
        // whose count is unknown makes the whole partition unknown rather than short: a sum
        // missing a file, reported as exact, is worse than no number at all.
        let mut record_counts = vec![Some(0i64); listings.len()];
        for (index, count) in counts {
            record_counts[index] = match (record_counts[index], count) {
                (Some(total), Some(count)) => total.checked_add(count),
                _ => None,
            };
        }
        record_counts
            .into_iter()
            .map(|count| count.unwrap_or(Partition::UNKNOWN))
            .collect()
    }
}

/// What the listed files of a partition add up to.
fn statistics(
    spec: &HashMap<String, String>,
    files: &[FileStatus],
    record_count: i64,
) -> PartitionStatistics {
    let file_size_in_bytes = files
        .iter()
        .map(|file| i64::try_from(file.size).unwrap_or(i64::MAX))
        .fold(0i64, i64::saturating_add);
    let last_file_creation_time = files
        .iter()
        .filter_map(|file| file.last_modified)
        .map(|modified| modified.timestamp_millis())
        .max()
        .unwrap_or(Partition::UNKNOWN);
    PartitionStatistics {
        spec: spec.clone(),
        record_count,
        file_size_in_bytes,
        file_count: files.len() as i64,
        last_file_creation_time,
        total_buckets: Partition::UNKNOWN_TOTAL_BUCKETS,
    }
}
