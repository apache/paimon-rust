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

use super::lookup_file::LookupFileCache;
use crate::io::FileRead;
use crate::resource::ResourceContext;
use crate::spec::{BinaryRowBuilder, DataField, DataFileMeta};
use crate::table::merge_tree_split_generator::KeyComparator;
use crate::table::sort_merge::{LookupProvider, FILE_LEVEL_METADATA_KEY};
use crate::table::{DataSplit, Table};
use crate::{Error, Result};
use arrow_array::RecordBatch;
use arrow_row::{RowConverter, SortField};
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

struct HistoryFile {
    meta: DataFileMeta,
    range_compatible: bool,
    identity: tokio::sync::OnceCell<String>,
    vector: Option<crate::DeletionFile>,
}

struct HistoryLevel {
    files: Vec<HistoryFile>,
    indexed: bool,
}

struct LevelLookup {
    table: Table,
    split: DataSplit,
    levels: BTreeMap<i32, HistoryLevel>,
    cache: Arc<LookupFileCache>,
    keys: Vec<usize>,
    key_fields: Vec<DataField>,
    converter: Mutex<RowConverter>,
    comparator: Option<KeyComparator>,
    resources: Option<ResourceContext>,
    schema_identity: Vec<u8>,
    deletions: Option<LookupDeletions>,
}

pub(super) type LookupDeletions = Arc<Mutex<HashMap<String, roaring::RoaringBitmap>>>;

pub(in crate::table::compact) async fn provider(
    table: &Table,
    split: &DataSplit,
    files: Vec<DataFileMeta>,
    cache: Arc<LookupFileCache>,
    resources: Option<ResourceContext>,
) -> Result<(Arc<dyn LookupProvider>, Option<LookupDeletions>)> {
    let fields = super::read_type(table);
    let keys = table
        .schema()
        .trimmed_primary_keys()
        .iter()
        .map(|name| {
            fields
                .iter()
                .position(|field| field.name() == name)
                .unwrap()
        })
        .collect::<Vec<_>>();
    let key_fields = keys
        .iter()
        .map(|&index| fields[index].clone())
        .collect::<Vec<_>>();
    let schema = crate::arrow::build_read_arrow_schema(&key_fields)?;
    let converter = RowConverter::new(
        schema
            .fields()
            .iter()
            .map(|field| SortField::new(field.data_type().clone()))
            .collect(),
    )
    .map_err(invalid)?;
    let comparator = KeyComparator::from_table_schema(table.schema());
    let dv = table.schema().core_options().deletion_vectors_enabled();
    let history = DataSplit::builder()
        .with_snapshot(split.snapshot_id())
        .with_partition(split.partition().clone())
        .with_bucket(split.bucket())
        .with_total_buckets(split.total_buckets())
        .with_bucket_path(split.bucket_path().to_string())
        .with_data_files(files.clone())
        .build()?;
    let history = if dv {
        super::deletion_vectors::with_deletion_files(
            table,
            &history,
            table
                .travel_snapshot()
                .ok_or_else(|| invalid("Lookup needs a captured snapshot"))?,
        )
        .await?
    } else {
        history
    };
    let mut schemas = BTreeMap::from([(table.schema().id(), true)]);
    let mut levels = BTreeMap::<_, HistoryLevel>::new();
    for (index, meta) in files.into_iter().enumerate() {
        let compatible = if let Some(&compatible) = schemas.get(&meta.schema_id) {
            compatible
        } else {
            let schema = table.schema_manager().schema(meta.schema_id).await?;
            let old_keys = schema.trimmed_primary_keys();
            let compatible = old_keys.len() == key_fields.len()
                && old_keys.iter().zip(&key_fields).all(|(name, current)| {
                    schema
                        .fields()
                        .iter()
                        .find(|field| field.name() == name)
                        .is_some_and(|old| {
                            old.id() == current.id()
                                && old.data_type().equals_ignore_nullable(current.data_type())
                        })
                });
            schemas.insert(meta.schema_id, compatible);
            compatible
        };
        let range_compatible = compatible
            && comparator.as_ref().is_some_and(|compare| {
                compare
                    .compare(&meta.min_key, &meta.max_key)
                    .is_some_and(|order| !order.is_gt())
            });
        levels
            .entry(meta.level)
            .or_insert_with(|| HistoryLevel {
                files: Vec::new(),
                indexed: false,
            })
            .files
            .push(HistoryFile {
                meta,
                range_compatible,
                identity: tokio::sync::OnceCell::new(),
                vector: history
                    .data_deletion_files()
                    .and_then(|files| files.get(index))
                    .cloned()
                    .flatten(),
            });
    }
    for level in levels.values_mut() {
        if level.files.iter().all(|file| file.range_compatible) {
            let compare = comparator.as_ref().unwrap();
            level
                .files
                .sort_by(|a, b| compare.compare(&a.meta.min_key, &b.meta.min_key).unwrap());
            level.indexed = level.files.windows(2).all(|pair| {
                compare.compare(&pair[0].meta.max_key, &pair[1].meta.min_key)
                    == Some(Ordering::Less)
            });
        }
    }
    let options = table.schema().options().iter().collect::<BTreeMap<_, _>>();
    let schema_identity = serde_json::to_vec(&(
        table.schema().fields(),
        table.schema().primary_keys(),
        table.schema().partition_keys(),
        options,
    ))
    .map_err(invalid)?;
    let deletions = dv.then(|| Arc::new(Mutex::new(HashMap::new())));
    Ok((
        Arc::new(LevelLookup {
            table: table.clone(),
            split: split.clone(),
            levels,
            cache,
            keys,
            key_fields,
            converter: Mutex::new(converter),
            comparator,
            resources,
            schema_identity,
            deletions: deletions.clone(),
        }),
        deletions,
    ))
}

#[async_trait::async_trait]
impl LookupProvider for LevelLookup {
    async fn lookup(&self, key: &[u8]) -> Result<Option<RecordBatch>> {
        let binary_key = {
            let converter = self.converter.lock().unwrap();
            let parser = converter.parser();
            let columns = converter
                .convert_rows(std::iter::once(parser.parse(key)))
                .map_err(invalid)?;
            let schema = crate::arrow::build_read_arrow_schema(&self.key_fields)?;
            let batch = RecordBatch::try_new(schema, columns).map_err(invalid)?;
            let mut row = BinaryRowBuilder::new(self.key_fields.len() as i32);
            for (index, field) in self.key_fields.iter().enumerate() {
                if let Some(value) =
                    crate::spec::extract_datum_from_arrow(&batch, 0, index, field.data_type())?
                {
                    row.write_datum(index, &value, field.data_type());
                } else {
                    row.set_null_at(index);
                }
            }
            row.build_serialized()
        };
        let key_comparable = self
            .comparator
            .as_ref()
            .is_some_and(|compare| compare.compare(&binary_key, &binary_key).is_some());
        for level in self.levels.values() {
            let files = if level.indexed && key_comparable {
                let compare = self.comparator.as_ref().unwrap();
                let index = level.files.partition_point(|file| {
                    compare.compare(&file.meta.max_key, &binary_key) == Some(Ordering::Less)
                });
                level
                    .files
                    .get(index)
                    .map(std::slice::from_ref)
                    .unwrap_or_default()
            } else {
                level.files.as_slice()
            };
            let mut found = None;
            for file in files {
                // Old key types may have been promoted. Unverifiable ranges
                // must not prune a possible history match.
                if file.range_compatible {
                    if let Some(compare) = &self.comparator {
                        if let (Some(min), Some(max)) = (
                            compare.compare(&file.meta.min_key, &binary_key),
                            compare.compare(&file.meta.max_key, &binary_key),
                        ) {
                            if min.is_gt() || max.is_lt() {
                                continue;
                            }
                        }
                    }
                }
                let identity = file
                    .identity
                    .get_or_try_init(|| async {
                        let reader = self
                            .table
                            .file_io()
                            .new_input(&self.split.data_file_path(&file.meta))?
                            .reader_with_size(file.meta.file_size as u64)
                            .await?;
                        let namespace = reader
                            .cache_key()
                            .map(str::to_string)
                            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                        let mut physical = file.meta.clone();
                        physical.level = 0;
                        let mut digest = Sha256::new();
                        digest.update(b"paimon-lookup-sst-v1");
                        digest.update((namespace.len() as u64).to_le_bytes());
                        digest.update(namespace.as_bytes());
                        digest.update(serde_json::to_vec(&physical).map_err(invalid)?);
                        digest.update(&self.schema_identity);
                        Ok::<_, Error>(hex::encode(digest.finalize()))
                    })
                    .await?;
                let input = || {
                    let split = DataSplit::builder()
                        .with_snapshot(self.split.snapshot_id())
                        .with_partition(self.split.partition().clone())
                        .with_bucket(self.split.bucket())
                        .with_total_buckets(self.split.total_buckets())
                        .with_bucket_path(self.split.bucket_path().to_string())
                        .with_data_files(vec![file.meta.clone()])
                        .build()?;
                    // Preserve physical ordinals before snapshot DV filtering.
                    super::new_reader(&self.table, self.resources.clone())?
                        .read_physical_file(split, file.meta.clone())
                };
                let row = if self.deletions.is_some() {
                    self.cache
                        .lookup_with_position(identity, key, &self.keys, &input)
                        .await?
                } else {
                    self.cache
                        .lookup(identity, key, &self.keys, &input)
                        .await?
                        .map(|batch| (batch, 0))
                };
                if let Some((batch, position)) = row {
                    if let Some(vector) = &file.vector {
                        if self
                            .cache
                            .is_deleted(
                                self.table.file_io(),
                                vector,
                                position,
                                self.resources.as_ref(),
                            )
                            .await?
                        {
                            continue;
                        }
                    }
                    if found.is_some() {
                        return Err(invalid(
                            "Lookup history has overlapping keys within a positive level",
                        ));
                    }
                    if let Some(deletions) = &self.deletions {
                        let position = u32::try_from(position)
                            .map_err(|_| invalid("PK deletion vector position exceeds u32::MAX"))?;
                        deletions
                            .lock()
                            .unwrap()
                            .entry(file.meta.file_name.clone())
                            .or_default()
                            .insert(position);
                    }
                    let mut metadata = batch.schema().metadata().clone();
                    metadata.insert(FILE_LEVEL_METADATA_KEY.into(), file.meta.level.to_string());
                    found = Some(
                        RecordBatch::try_new(
                            Arc::new(batch.schema().as_ref().clone().with_metadata(metadata)),
                            batch.columns().to_vec(),
                        )
                        .map_err(invalid)?,
                    );
                }
            }
            if found.is_some() {
                return Ok(found);
            }
        }
        Ok(None)
    }
}

fn invalid(message: impl ToString) -> Error {
    Error::DataInvalid {
        message: message.to_string(),
        source: None,
    }
}
