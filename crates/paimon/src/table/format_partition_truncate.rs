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

//! Deletes the data files of a Format Table for TRUNCATE TABLE.

use std::collections::HashMap;

use super::format_partition::FormatTablePartitionPaths;
use super::format_table_scan::list_format_table_files;
use super::Table;
use crate::spec::CoreOptions;

/// Empties a Format Table or its partitions, keeping every directory and catalog registration.
/// Mirrors the file side of Java `FormatTableCommit#truncateTable` and `#truncatePartitions`.
#[derive(Debug)]
pub struct FormatTableTruncator<'a> {
    table: &'a Table,
    table_path: String,
    partition_paths: FormatTablePartitionPaths,
    default_partition_name: String,
}

impl<'a> FormatTableTruncator<'a> {
    pub fn new(table: &'a Table) -> Self {
        let options = CoreOptions::new(table.schema().options());
        let table_path = options
            .path()
            .unwrap_or_else(|| table.location())
            .trim_end_matches('/')
            .to_string();
        let partition_paths = FormatTablePartitionPaths::new(
            table.schema().partition_keys().iter().cloned(),
            options.format_table_partition_only_value_in_path(),
        );
        let default_partition_name = options.partition_default_name().to_string();
        Self {
            table,
            table_path,
            partition_paths,
            default_partition_name,
        }
    }

    /// Deletes the data files of an unpartitioned table.
    pub async fn truncate_unpartitioned(&self) -> crate::Result<()> {
        if !self.table.schema().partition_keys().is_empty() {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Format Table {} is partitioned, so it is truncated partition by partition",
                    self.table.identifier().full_name()
                ),
                source: None,
            });
        }
        self.delete_data_files(&self.table_path).await
    }

    /// Deletes the data files of one complete partition in its directory below the table.
    pub async fn truncate_partition(&self, spec: &HashMap<String, String>) -> crate::Result<()> {
        let relative_path = self.partition_paths.relative_path(spec)?;
        self.delete_data_files(&format!("{}/{relative_path}", self.table_path))
            .await
    }

    /// The complete partitions whose directories sit below the table directory, for a table that
    /// does not keep its partitions in a catalog.
    pub async fn discover_partitions(&self) -> crate::Result<Vec<HashMap<String, String>>> {
        self.partition_paths
            .discover(
                self.table.file_io(),
                &self.table_path,
                &self.default_partition_name,
            )
            .await
    }

    /// Every non-hidden file below `directory`, whatever its extension: a Java reader reads those
    /// too. Staging trees such as `_temporary` hold another writer's uncommitted output and stay.
    async fn delete_data_files(&self, directory: &str) -> crate::Result<()> {
        let file_io = self.table.file_io();
        let files = list_format_table_files(file_io, directory, 0, None).await?;
        for file in files.iter().filter(|file| !file.is_dir) {
            file_io.delete_file(&file.path).await?;
        }
        Ok(())
    }
}
