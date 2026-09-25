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

//! File writer for a Format Table. Java's `FormatTableWrite` puts partition columns in
//! the directory name and writes only the remaining columns to each data file.
//! Files stay below a hidden staging directory until `FormatTableCommit` publishes them.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema as ArrowSchema};

use super::commit_message::{CommitMessage, FormatFileCommit};
use super::format_partition::FormatTablePartitionPaths;
use super::format_table_scan::supported_format_table_extension;
use super::table_write::take_rows;
use super::Table;
use crate::arrow::build_target_arrow_schema;
use crate::arrow::format::{create_format_writer, with_write_resources, FormatFileWriter};
use crate::resource::ResourceContext;
use crate::spec::{BinaryRow, CoreOptions, DataField};
use crate::Result;

struct OpenFile {
    writer: Box<dyn FormatFileWriter>,
    staged_path: String,
    target_path: String,
    record_count: i64,
}

struct PartitionWriter {
    spec: HashMap<String, String>,
    directory: String,
    open: Option<OpenFile>,
}

/// Mirrors the file side of Java `FormatTableWrite` and
/// `FormatTableRollingFileWriter`. The table schema is the input schema; a
/// file's schema omits partition columns. A failed write owns and deletes its
/// staged files, while prepared files are transferred to the committer.
pub(crate) struct FormatTableWriter {
    table: Table,
    full_schema: Arc<ArrowSchema>,
    file_schema: Arc<ArrowSchema>,
    file_fields: Vec<DataField>,
    partition_indices: Vec<usize>,
    data_indices: Vec<usize>,
    partition_paths: FormatTablePartitionPaths,
    table_path: String,
    data_file_prefix: String,
    extension: String,
    compression: String,
    zstd_level: i32,
    target_file_size: i64,
    target_file_rows: i64,
    resources: Option<ResourceContext>,
    staging_root: String,
    writers: HashMap<Vec<u8>, PartitionWriter>,
    prepared: Vec<CommitMessage>,
    owned_staging: HashSet<String>,
    failed: bool,
}

impl FormatTableWriter {
    pub(crate) fn set_resources(&mut self, resources: ResourceContext) {
        self.resources = Some(resources);
    }

    pub(crate) fn new(table: &Table, resources: Option<ResourceContext>) -> Result<Self> {
        table.ensure_not_branch_reference_for_write()?;
        let schema = table.schema();
        if let Some(field) = schema
            .fields()
            .iter()
            .find(|field| field.default_value().is_some())
        {
            return Err(crate::Error::Unsupported {
                message: format!(
                    "Format Table column default for '{}' is not supported by the Rust writer",
                    field.name()
                ),
            });
        }
        let options = CoreOptions::new(schema.options());
        let format = options.file_format();
        let extension = supported_format_table_extension(&format)?.to_string();
        // Readers cover more external file types than native writers. Reject
        // unsupported formats before staging any files, rather than failing
        // after the first RecordBatch has been routed.
        match format.as_str() {
            "parquet" | "row" => {}
            #[cfg(feature = "vortex")]
            "vortex" => {}
            _ => {
                return Err(crate::Error::Unsupported {
                    message: format!(
                        "Format Table file.format '{format}' can be read but cannot be written by the Rust client"
                    ),
                });
            }
        }
        let full_schema = build_target_arrow_schema(schema.fields())?;
        let partition_indices = schema
            .partition_keys()
            .iter()
            .map(|key| {
                schema
                    .fields()
                    .iter()
                    .position(|field| field.name() == key)
                    .ok_or_else(|| crate::Error::DataInvalid {
                        message: format!("Unknown Format Table partition column '{key}'"),
                        source: None,
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let data_indices = (0..schema.fields().len())
            .filter(|index| !partition_indices.contains(index))
            .collect::<Vec<_>>();
        let file_fields = data_indices
            .iter()
            .map(|&index| schema.fields()[index].clone())
            .collect::<Vec<_>>();
        let file_schema = Arc::new(ArrowSchema::new(
            data_indices
                .iter()
                .map(|&index| Arc::new(Field::clone(full_schema.field(index))))
                .collect::<Vec<_>>(),
        ));
        let target_file_rows = schema
            .options()
            .get("target-file-row-num")
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(i64::MAX);
        if target_file_rows <= 0 {
            return Err(crate::Error::ConfigInvalid {
                message: "target-file-row-num must be positive".into(),
            });
        }
        let compression = format_table_compression(schema.options(), &format);
        let extension = if schema
            .options()
            .get("file.suffix.include.compression")
            .is_some_and(|value| value.eq_ignore_ascii_case("true"))
            && !matches!(compression.as_str(), "" | "none")
        {
            format!(".{compression}{extension}")
        } else {
            extension
        };
        let table_path = options
            .path()
            .unwrap_or_else(|| table.location())
            .trim_end_matches('/')
            .to_string();
        let staging_root = format!("{table_path}/_temporary/{}", uuid::Uuid::new_v4());
        Ok(Self {
            table: table.clone(),
            full_schema,
            file_schema,
            file_fields,
            partition_indices,
            data_indices,
            partition_paths: FormatTablePartitionPaths::new(
                schema.partition_keys().iter().cloned(),
                options.format_table_partition_only_value_in_path(),
            ),
            table_path,
            data_file_prefix: options.data_file_prefix().to_string(),
            extension,
            compression,
            zstd_level: options.file_compression_zstd_level(),
            target_file_size: options.target_file_size(),
            target_file_rows,
            resources,
            staging_root,
            writers: HashMap::new(),
            prepared: Vec::new(),
            owned_staging: HashSet::new(),
            failed: false,
        })
    }

    pub(crate) async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.failed {
            return Err(crate::Error::DataInvalid {
                message: "Format Table writer failed; create a new writer".into(),
                source: None,
            });
        }
        let result = self.write_inner(batch).await;
        if result.is_err() {
            self.failed = true;
            self.close().await;
        }
        result
    }

    async fn write_inner(&mut self, batch: &RecordBatch) -> Result<()> {
        self.check_schema(batch)?;
        if batch.num_rows() == 0 {
            return Ok(());
        }

        // Validate all non-null fields before opening a file. Java validates the
        // input row before default-value replacement and partition extraction.
        for (field_index, field) in self.full_schema.fields().iter().enumerate() {
            if !field.is_nullable() && batch.column(field_index).null_count() > 0 {
                return Err(crate::Error::DataInvalid {
                    message: format!("Cannot write null to non-null column({})", field.name()),
                    source: None,
                });
            }
        }

        let mut groups: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
        let mut specs = HashMap::new();
        let partition_fields = self.table.schema().partition_fields();
        let options = CoreOptions::new(self.table.schema().options());
        let computer = crate::spec::PartitionComputer::new(
            self.table.schema().partition_keys(),
            self.table.schema().fields(),
            options.partition_default_name(),
            options.legacy_partition_name(),
        )?;
        for row_index in 0..batch.num_rows() {
            let partition = BinaryRow::from_arrow(
                batch,
                row_index,
                &self.partition_indices,
                &partition_fields,
            )?;
            let key = partition.to_serialized_bytes();
            groups.entry(key.clone()).or_default().push(row_index);
            if let std::collections::hash_map::Entry::Vacant(entry) = specs.entry(key) {
                entry.insert(
                    computer
                        .generate_part_values(&partition)?
                        .into_iter()
                        .collect(),
                );
            }
        }

        for (key, rows) in groups {
            let input = take_rows(batch, &rows)?;
            let file_batch = self.project_data_columns(&input)?;
            if !self.writers.contains_key(&key) {
                let spec = specs.remove(&key).unwrap();
                let relative = self.partition_paths.relative_path(&spec)?;
                let directory = if relative.is_empty() {
                    self.table_path.clone()
                } else {
                    format!("{}/{relative}", self.table_path)
                };
                self.writers.insert(
                    key.clone(),
                    PartitionWriter {
                        spec,
                        directory,
                        open: None,
                    },
                );
            }
            self.write_partition(&key, &file_batch).await?;
        }
        Ok(())
    }

    fn check_schema(&self, batch: &RecordBatch) -> Result<()> {
        let expected = self.full_schema.fields();
        let actual = batch.schema();
        if actual.fields().len() != expected.len() {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Format Table write expects {} columns, got {}",
                    expected.len(),
                    actual.fields().len()
                ),
                source: None,
            });
        }
        for (index, field) in expected.iter().enumerate() {
            let given = actual.field(index);
            if field.name() != given.name() || field.data_type() != given.data_type() {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Format Table column {} expects {}: {:?}, got {}: {:?}",
                        index,
                        field.name(),
                        field.data_type(),
                        given.name(),
                        given.data_type()
                    ),
                    source: None,
                });
            }
        }
        Ok(())
    }

    fn project_data_columns(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        RecordBatch::try_new(
            self.file_schema.clone(),
            self.data_indices
                .iter()
                .map(|&index| batch.column(index).clone())
                .collect(),
        )
        .map_err(|error| crate::Error::DataInvalid {
            message: format!("Cannot project Format Table data columns: {error}"),
            source: Some(Box::new(error)),
        })
    }

    async fn write_partition(&mut self, key: &[u8], batch: &RecordBatch) -> Result<()> {
        let mut remaining = batch.clone();
        while remaining.num_rows() > 0 {
            if self.writers.get(key).unwrap().open.is_none() {
                self.open_file(key).await?;
            }
            let current_rows = self
                .writers
                .get(key)
                .unwrap()
                .open
                .as_ref()
                .unwrap()
                .record_count;
            let available = (self.target_file_rows - current_rows) as usize;
            let rows = available.min(remaining.num_rows());
            let chunk = remaining.slice(0, rows);
            self.writers
                .get_mut(key)
                .unwrap()
                .open
                .as_mut()
                .unwrap()
                .writer
                .write(&chunk)
                .await?;
            remaining = remaining.slice(rows, remaining.num_rows() - rows);
            let open = self.writers.get_mut(key).unwrap().open.as_mut().unwrap();
            open.record_count += rows as i64;
            let should_roll = open.record_count >= self.target_file_rows
                || open.writer.num_bytes() as i64 >= self.target_file_size;
            if should_roll {
                self.close_file(key).await?;
            }
        }
        Ok(())
    }

    async fn open_file(&mut self, key: &[u8]) -> Result<()> {
        let partition = self.writers.get(key).unwrap();
        let file_name = format!(
            "{}{}{}",
            self.data_file_prefix,
            uuid::Uuid::new_v4(),
            self.extension
        );
        let target_path = format!("{}/{file_name}", partition.directory);
        let staged_path = format!("{}/{}", self.staging_root, file_name);
        self.table
            .file_io()
            .mkdirs(&format!("{}/", self.staging_root))
            .await?;
        self.owned_staging.insert(staged_path.clone());
        let output = self.table.file_io().new_output(&staged_path)?;
        let writer = create_format_writer(
            &output,
            self.file_schema.clone(),
            &self.compression,
            self.zstd_level,
            Some(self.table.file_io().clone()),
            Some(&self.file_fields),
            Some(self.table.schema().options()),
        )
        .await?;
        self.writers.get_mut(key).unwrap().open = Some(OpenFile {
            writer: with_write_resources(writer, self.resources.as_ref()),
            staged_path,
            target_path,
            record_count: 0,
        });
        Ok(())
    }

    async fn close_file(&mut self, key: &[u8]) -> Result<()> {
        let partition = self.writers.get_mut(key).unwrap();
        let Some(file) = partition.open.take() else {
            return Ok(());
        };
        file.writer.close().await?;
        let size = self
            .table
            .file_io()
            .get_status(&file.staged_path)
            .await?
            .size as i64;
        let mut message = CommitMessage::new(key.to_vec(), 0, Vec::new());
        message.format_file = Some(FormatFileCommit {
            staged_path: file.staged_path.clone(),
            target_path: file.target_path,
            partition: partition.spec.clone(),
            record_count: file.record_count,
            file_size: size,
        });
        self.owned_staging.remove(&file.staged_path);
        self.prepared.push(message);
        Ok(())
    }

    pub(crate) async fn prepare_commit(&mut self) -> Result<Vec<CommitMessage>> {
        if self.failed {
            return Err(crate::Error::DataInvalid {
                message: "Format Table writer failed; create a new writer".into(),
                source: None,
            });
        }
        let keys = self.writers.keys().cloned().collect::<Vec<_>>();
        for key in &keys {
            if let Err(error) = self.close_file(key).await {
                self.failed = true;
                self.close().await;
                return Err(error);
            }
        }
        self.writers.clear();
        Ok(std::mem::take(&mut self.prepared))
    }

    pub(crate) async fn close(&mut self) {
        self.writers.clear();
        for path in self.owned_staging.drain() {
            let _ = self.table.file_io().delete_file(&path).await;
        }
        for message in self.prepared.drain(..) {
            if let Some(file) = message.format_file {
                let _ = self.table.file_io().delete_file(&file.staged_path).await;
            }
        }
    }
}

fn format_table_compression(options: &HashMap<String, String>, format: &str) -> String {
    options
        .get("file.compression")
        .or_else(|| options.get("format-table.file.compression"))
        .or_else(|| options.get("compression"))
        .cloned()
        .unwrap_or_else(|| {
            match format {
                "parquet" => "snappy",
                "orc" | "avro" | "mosaic" => "zstd",
                _ => "none",
            }
            .to_string()
        })
}
