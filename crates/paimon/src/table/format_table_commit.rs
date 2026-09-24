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

//! Publish prepared Format Table files without creating a Paimon snapshot.
//! Java's `FormatTableCommit` treats published data and catalog partition
//! registration as separate side effects, with different rollback rules before
//! and after a partition becomes visible to readers.

use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use super::commit_message::{CommitMessage, FormatFileCommit};
use super::format_partition::FormatTablePartitionPaths;
use super::format_table_scan::list_format_table_files;
use super::Table;
use crate::spec::{CoreOptions, Datum, Partition, PartitionStatistics};
use crate::Result;

pub(crate) struct FormatTableCommit<'a> {
    table: &'a Table,
    table_path: String,
    paths: FormatTablePartitionPaths,
    default_partition_name: String,
    dynamic_partition_overwrite: bool,
}

impl<'a> FormatTableCommit<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        let options = CoreOptions::new(table.schema().options());
        Self {
            table,
            table_path: options
                .path()
                .unwrap_or_else(|| table.location())
                .trim_end_matches('/')
                .to_string(),
            paths: FormatTablePartitionPaths::new(
                table.schema().partition_keys().iter().cloned(),
                options.format_table_partition_only_value_in_path(),
            ),
            default_partition_name: options.partition_default_name().to_string(),
            dynamic_partition_overwrite: table
                .schema()
                .options()
                .get("dynamic-partition-overwrite")
                .is_none_or(|value| value.eq_ignore_ascii_case("true")),
        }
    }

    pub(crate) async fn append(&self, messages: &[CommitMessage]) -> Result<()> {
        self.apply(messages, None).await
    }

    pub(crate) async fn overwrite(
        &self,
        messages: &[CommitMessage],
        static_partition: Option<&HashMap<String, Option<Datum>>>,
    ) -> Result<()> {
        self.apply(messages, Some(static_partition)).await
    }

    pub(crate) async fn abort(&self, messages: &[CommitMessage]) -> Result<()> {
        for message in messages {
            if let Some(file) = &message.format_file {
                self.table.file_io().delete_file(&file.staged_path).await?;
            }
        }
        Ok(())
    }

    /// `None` is append; `Some(None)` is an overwrite without static
    /// partitions; `Some(Some(spec))` selects the leading static prefix.
    async fn apply(
        &self,
        messages: &[CommitMessage],
        overwrite: Option<Option<&HashMap<String, Option<Datum>>>>,
    ) -> Result<()> {
        self.table.ensure_not_branch_reference_for_write()?;
        let files = self.validate_messages(messages).await?;
        let managed = self.table.has_catalog_managed_partitions();
        let requested = files
            .iter()
            .map(|file| file.partition.clone())
            .collect::<Vec<_>>();
        let selected = if let Some(static_partition) = overwrite {
            self.selected_overwrite_partitions(&requested, static_partition)
                .await?
        } else {
            Vec::new()
        };
        let static_prefix = match overwrite {
            Some(Some(spec)) => self.static_prefix(Some(spec))?,
            _ => HashMap::new(),
        };
        if files.iter().any(|file| {
            !static_prefix
                .iter()
                .all(|(key, value)| file.partition.get(key) == Some(value))
        }) {
            return Err(crate::Error::DataInvalid {
                message: "Format Table output is outside the static overwrite partition".into(),
                source: None,
            });
        }

        // Validate registry paths before deleting or publishing anything. A
        // partition with a custom location cannot be written through the table
        // directory; Java rejects it for the same reason.
        if managed {
            let mut touched = requested.clone();
            touched.extend(selected.iter().cloned());
            self.validate_registered_partitions(&touched).await?;
        }

        if overwrite.is_some() {
            for spec in &selected {
                let directory = self.partition_directory(spec)?;
                for status in list_format_table_files(self.table.file_io(), &directory, 0, None)
                    .await?
                    .into_iter()
                    .filter(|status| !status.is_dir)
                {
                    self.table.file_io().delete_file(&status.path).await?;
                }
            }
            if !static_prefix.is_empty() {
                let relative = self.paths.relative_prefix_path(&static_prefix)?;
                self.table
                    .file_io()
                    .mkdirs(&format!("{}/{relative}/", self.table_path))
                    .await?;
            }
        }

        let mut published: Vec<String> = Vec::new();
        for file in &files {
            let result = self.publish(file).await;
            if let Err(error) = result {
                // An overwrite has already removed old files, so replacement
                // files must survive an uncertain partial publish. Append can
                // safely roll back files this attempt uniquely named.
                if overwrite.is_none() {
                    for path in &published {
                        let _ = self.table.file_io().delete_file(path).await;
                    }
                }
                self.discard_staging(&files).await;
                return Err(error);
            }
            published.push(file.target_path.clone());
        }
        self.discard_staging(&files).await;

        if managed {
            let stats = self.partition_statistics(&files, &selected, overwrite.is_some());
            let specs = stats
                .iter()
                .map(|stat| stat.spec.clone())
                .collect::<Vec<_>>();
            if !specs.is_empty() {
                let env = self
                    .table
                    .rest_env()
                    .expect("managed partition REST environment");
                let result = env
                    .api()
                    .create_partitions_with_statistics(
                        env.identifier(),
                        specs,
                        true,
                        Some(stats),
                        overwrite.is_some(),
                    )
                    .await;
                if let Err(error) = result {
                    if overwrite.is_some() {
                        // The old data is already gone. Keep replacements even
                        // when reporting metadata failed, as Java does.
                        return Err(error);
                    }
                    // After files are published a registration error leaves an
                    // uncertain result. A retry must not duplicate rows.
                    for path in published {
                        let _ = self.table.file_io().delete_file(&path).await;
                    }
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    async fn validate_messages(&self, messages: &[CommitMessage]) -> Result<Vec<FormatFileCommit>> {
        let mut files = Vec::with_capacity(messages.len());
        let mut targets = HashSet::new();
        for message in messages {
            let file = message
                .format_file
                .as_ref()
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: "Format Table commit requires staged Format Table file messages"
                        .into(),
                    source: None,
                })?;
            let expected_directory = self.partition_directory(&file.partition)?;
            let parent = file.target_path.rsplit_once('/').map(|(parent, _)| parent);
            if parent != Some(expected_directory.as_str()) {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Format Table target is outside partition: {}",
                        file.target_path
                    ),
                    source: None,
                });
            }
            if !file
                .staged_path
                .starts_with(&format!("{}/_temporary/", self.table_path))
            {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Format Table staging path is outside table: {}",
                        file.staged_path
                    ),
                    source: None,
                });
            }
            if !targets.insert(file.target_path.as_str()) {
                return Err(crate::Error::DataInvalid {
                    message: format!("Duplicate Format Table target: {}", file.target_path),
                    source: None,
                });
            }
            if !self.table.file_io().exists(&file.staged_path).await? {
                return Err(crate::Error::DataInvalid {
                    message: format!("Staged Format Table file is missing: {}", file.staged_path),
                    source: None,
                });
            }
            if self.table.file_io().exists(&file.target_path).await? {
                return Err(crate::Error::DataInvalid {
                    message: format!("Format Table target already exists: {}", file.target_path),
                    source: None,
                });
            }
            files.push(file.clone());
        }
        Ok(files)
    }

    async fn validate_registered_partitions(
        &self,
        specs: &[HashMap<String, String>],
    ) -> Result<()> {
        if specs.is_empty() {
            return Ok(());
        }
        let env = self
            .table
            .rest_env()
            .expect("managed partition REST environment");
        let registered = env
            .api()
            .list_partitions_by_names(env.identifier(), specs.to_vec())
            .await?;
        let requested = specs.iter().map(spec_key).collect::<HashSet<_>>();
        for partition in registered {
            if !requested.contains(&spec_key(&partition.spec)) {
                return Err(crate::Error::DataInvalid {
                    message: "Catalog returned an unrequested Format Table partition".into(),
                    source: None,
                });
            }
            if partition
                .options
                .as_ref()
                .is_some_and(|options| options.contains_key("path"))
            {
                return Err(crate::Error::Unsupported {
                    message:
                        "Writing a Format Table partition with a custom location is not supported"
                            .into(),
                });
            }
        }
        Ok(())
    }

    fn partition_directory(&self, spec: &HashMap<String, String>) -> Result<String> {
        let relative = self.paths.relative_path(spec)?;
        Ok(if relative.is_empty() {
            self.table_path.clone()
        } else {
            format!("{}/{relative}", self.table_path)
        })
    }

    async fn publish(&self, file: &FormatFileCommit) -> Result<()> {
        let directory = self.partition_directory(&file.partition)?;
        self.table
            .file_io()
            .mkdirs(&format!("{directory}/"))
            .await?;
        // Java opens each target with overwrite=false. The UUID-based name and
        // preflight existence check make a collision extremely unlikely; test
        // once more immediately before moving a file into place.
        if self.table.file_io().exists(&file.target_path).await? {
            return Err(crate::Error::DataInvalid {
                message: format!("Format Table target already exists: {}", file.target_path),
                source: None,
            });
        }
        match self
            .table
            .file_io()
            .rename(&file.staged_path, &file.target_path)
            .await
        {
            Ok(()) => Ok(()),
            Err(crate::Error::IoUnexpected { source, .. })
                if source.kind() == opendal::ErrorKind::Unsupported =>
            {
                let result = self
                    .table
                    .file_io()
                    .copy_file_streaming(&file.staged_path, &file.target_path)
                    .await;
                if result.is_err() {
                    let _ = self.table.file_io().delete_file(&file.target_path).await;
                }
                result
            }
            Err(error) => Err(error),
        }
    }

    async fn discard_staging(&self, files: &[FormatFileCommit]) {
        for file in files {
            let _ = self.table.file_io().delete_file(&file.staged_path).await;
        }
    }

    async fn selected_overwrite_partitions(
        &self,
        written: &[HashMap<String, String>],
        static_partition: Option<&HashMap<String, Option<Datum>>>,
    ) -> Result<Vec<HashMap<String, String>>> {
        let keys = self.table.schema().partition_keys();
        if keys.is_empty() {
            if static_partition.is_some_and(|spec| !spec.is_empty()) {
                return Err(crate::Error::DataInvalid {
                    message: "An unpartitioned Format Table cannot have a static partition".into(),
                    source: None,
                });
            }
            return Ok(vec![HashMap::new()]);
        }
        let prefix = self.static_prefix(static_partition)?;
        if prefix.is_empty() && self.dynamic_partition_overwrite {
            return Ok(deduplicate_specs(written));
        }
        let candidates = if self.table.has_catalog_managed_partitions() {
            let env = self
                .table
                .rest_env()
                .expect("managed partition REST environment");
            env.api()
                .list_partitions(env.identifier())
                .await?
                .into_iter()
                .map(|part| part.spec)
                .collect::<Vec<_>>()
        } else {
            self.paths
                .discover(
                    self.table.file_io(),
                    &self.table_path,
                    &self.default_partition_name,
                )
                .await?
        };
        let mut selected = candidates
            .into_iter()
            .filter(|spec| {
                prefix
                    .iter()
                    .all(|(key, value)| spec.get(key) == Some(value))
            })
            .collect::<Vec<_>>();
        selected.extend(
            written
                .iter()
                .filter(|spec| {
                    prefix
                        .iter()
                        .all(|(key, value)| spec.get(key) == Some(value))
                })
                .cloned(),
        );
        if prefix.len() == keys.len() {
            selected.push(prefix);
        }
        Ok(deduplicate_specs(&selected))
    }

    fn static_prefix(
        &self,
        static_partition: Option<&HashMap<String, Option<Datum>>>,
    ) -> Result<HashMap<String, String>> {
        let Some(static_partition) = static_partition else {
            return Ok(HashMap::new());
        };
        let keys = self.table.schema().partition_keys();
        let mut prefix = HashMap::new();
        let mut missing = false;
        for key in keys {
            match static_partition.get(key) {
                Some(_) if missing => {
                    return Err(crate::Error::DataInvalid {
                        message: format!("Static partition '{key}' lacks its leading partition"),
                        source: None,
                    });
                }
                Some(value) => {
                    let field = self
                        .table
                        .schema()
                        .fields()
                        .iter()
                        .find(|field| field.name() == key)
                        .expect("partition field");
                    let text = match value {
                        None => self.default_partition_name.clone(),
                        Some(datum) => super::format_partition::format_partition_value(
                            datum,
                            field.data_type(),
                            &self.default_partition_name,
                            CoreOptions::new(self.table.schema().options())
                                .legacy_partition_name(),
                        )
                        .ok_or_else(|| crate::Error::DataInvalid {
                            message: format!(
                                "Static partition '{key}' has a value incompatible with its data type"
                            ),
                            source: None,
                        })?,
                    };
                    prefix.insert(key.clone(), text);
                }
                None => missing = true,
            }
        }
        if static_partition.keys().any(|key| !keys.contains(key)) {
            return Err(crate::Error::DataInvalid {
                message: "Unknown static Format Table partition column".into(),
                source: None,
            });
        }
        Ok(prefix)
    }

    fn partition_statistics(
        &self,
        files: &[FormatFileCommit],
        selected: &[HashMap<String, String>],
        overwrite: bool,
    ) -> Vec<PartitionStatistics> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let mut by_spec: HashMap<Vec<(String, String)>, PartitionStatistics> = HashMap::new();
        if overwrite {
            for spec in selected {
                let key = spec_key(spec);
                by_spec.insert(
                    key,
                    PartitionStatistics {
                        spec: spec.clone(),
                        record_count: 0,
                        file_size_in_bytes: 0,
                        file_count: 0,
                        last_file_creation_time: now,
                        total_buckets: Partition::UNKNOWN_TOTAL_BUCKETS,
                    },
                );
            }
        }
        for file in files {
            let stat =
                by_spec
                    .entry(spec_key(&file.partition))
                    .or_insert_with(|| PartitionStatistics {
                        spec: file.partition.clone(),
                        record_count: 0,
                        file_size_in_bytes: 0,
                        file_count: 0,
                        last_file_creation_time: now,
                        total_buckets: Partition::UNKNOWN_TOTAL_BUCKETS,
                    });
            stat.record_count += file.record_count;
            stat.file_size_in_bytes += file.file_size;
            stat.file_count += 1;
        }
        by_spec.into_values().collect()
    }
}

fn deduplicate_specs(specs: &[HashMap<String, String>]) -> Vec<HashMap<String, String>> {
    let mut seen = HashSet::new();
    specs
        .iter()
        .filter(|spec| seen.insert(spec_key(spec)))
        .cloned()
        .collect()
}

fn spec_key(spec: &HashMap<String, String>) -> Vec<(String, String)> {
    let mut key = spec
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    key.sort();
    key
}
