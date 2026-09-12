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

use std::collections::{BTreeMap, HashMap};

use arrow_array::RecordBatch;
use bytes::Bytes;

use crate::common::Options;
use crate::file_index::file_index_writer::FileIndexWriter;
use crate::file_index::file_indexer_factory::FileIndexerFactory;
use crate::file_index::serialize_column_indexes;
use crate::spec::{extract_datum_from_arrow, CoreOptions, DataField};
use crate::{Error, Result};

#[derive(Clone)]
struct IndexColumnOptions {
    field: DataField,
    position: usize,
    indexes: BTreeMap<String, Options>,
}

/// Validated top-level column indexes, enabled explicitly by ordinary append writes.
#[derive(Clone)]
pub(super) struct FileIndexOptions {
    columns: Vec<IndexColumnOptions>,
    pub(super) in_manifest_threshold: i64,
}

impl FileIndexOptions {
    pub(super) fn parse(
        options: &HashMap<String, String>,
        fields: &[DataField],
    ) -> Result<Option<Self>> {
        let mut columns: BTreeMap<String, BTreeMap<String, Options>> = BTreeMap::new();
        for (key, value) in options {
            let Some(identifier) = key
                .strip_prefix("file-index.")
                .and_then(|key| key.strip_suffix(".columns"))
            else {
                continue;
            };
            if !FileIndexerFactory::is_supported(identifier) {
                return Err(Error::Unsupported {
                    message: format!("Unsupported file index in {key}: {identifier}"),
                });
            }
            for column in value.split(',').map(str::trim) {
                if column.is_empty() {
                    return Err(Error::ConfigInvalid {
                        message: format!("Empty column in {key}"),
                    });
                }
                columns
                    .entry(column.to_string())
                    .or_default()
                    .entry(identifier.to_string())
                    .or_default();
            }
        }

        for (key, value) in options {
            let Some(suffix) = key.strip_prefix("file-index.") else {
                continue;
            };
            if suffix == "read.enabled"
                || suffix == "in-manifest-threshold"
                || suffix.ends_with(".columns")
            {
                continue;
            }
            let parts = suffix.split_once('.').and_then(|(identifier, rest)| {
                rest.rsplit_once('.')
                    .map(|(column, option)| (identifier, column, option))
            });
            let Some((identifier, column, option)) = parts else {
                return Err(Error::ConfigInvalid {
                    message: format!("Invalid file index option: {key}"),
                });
            };
            let Some(index_options) = columns.get_mut(column).and_then(|c| c.get_mut(identifier))
            else {
                return Err(Error::ConfigInvalid {
                    message: format!(
                        "{key} requires column '{column}' in file-index.{identifier}.columns"
                    ),
                });
            };
            if !matches!(
                (identifier, option),
                ("bitmap", "version" | "index-block-size") | ("bloom-filter", "items" | "fpp")
            ) {
                return Err(Error::ConfigInvalid {
                    message: format!("Unknown file index option: {key}"),
                });
            }
            index_options.set(option, value);
        }

        let in_manifest_threshold = CoreOptions::new(options).file_index_in_manifest_threshold()?;
        let columns = columns
            .into_iter()
            .map(|(name, indexes)| {
                let (position, field) = fields
                    .iter()
                    .enumerate()
                    .find(|(_, field)| field.name() == name)
                    .ok_or_else(|| Error::ConfigInvalid {
                        message: format!(
                            "File index column '{name}' does not exist as a top-level field"
                        ),
                    })?;
                for (identifier, options) in &indexes {
                    FileIndexerFactory::create_writer(
                        identifier,
                        field.data_type().clone(),
                        options,
                    )?;
                }
                Ok(IndexColumnOptions {
                    field: field.clone(),
                    position,
                    indexes,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((!columns.is_empty()).then_some(Self {
            columns,
            in_manifest_threshold,
        }))
    }

    pub(super) fn create_writer(&self) -> Result<DataFileIndexWriter> {
        let columns = self
            .columns
            .iter()
            .map(|column| {
                let writers = column
                    .indexes
                    .iter()
                    .map(|(identifier, options)| {
                        Ok((
                            identifier.clone(),
                            FileIndexerFactory::create_writer(
                                identifier,
                                column.field.data_type().clone(),
                                options,
                            )?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(IndexColumn {
                    field: column.field.clone(),
                    position: column.position,
                    writers,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(DataFileIndexWriter { columns })
    }
}

struct IndexColumn {
    field: DataField,
    position: usize,
    writers: Vec<(String, Box<dyn FileIndexWriter>)>,
}

pub(super) struct DataFileIndexWriter {
    columns: Vec<IndexColumn>,
}

impl DataFileIndexWriter {
    pub(super) fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        for column in &mut self.columns {
            for row in 0..batch.num_rows() {
                let datum = extract_datum_from_arrow(
                    batch,
                    row,
                    column.position,
                    column.field.data_type(),
                )?;
                for (_, writer) in &mut column.writers {
                    writer.write(datum.as_ref())?;
                }
            }
        }
        Ok(())
    }

    pub(super) fn serialize(mut self) -> Result<Bytes> {
        let indexes = self
            .columns
            .iter_mut()
            .map(|column| {
                let indexes = column
                    .writers
                    .iter_mut()
                    .map(|(identifier, writer)| {
                        Ok((
                            identifier.clone(),
                            if writer.empty() {
                                None
                            } else {
                                Some(writer.serialized_bytes()?)
                            },
                        ))
                    })
                    .collect::<Result<HashMap<_, _>>>()?;
                Ok((column.field.name().to_string(), indexes))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        serialize_column_indexes(indexes)
    }
}

#[cfg(test)]
mod tests;
