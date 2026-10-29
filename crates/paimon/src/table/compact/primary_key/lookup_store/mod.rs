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

//! Snapshot-bound level lookup with disposable local SST/IPC files.
mod cache;
pub(in crate::table) use cache::LookupCache;
pub use cache::LookupCacheMetrics;

use crate::deletion_vector::{DeletionVector, DeletionVectorFactory};
use crate::io::FileRead;
use crate::resource::ResourceContext;
use crate::spec::{ChangelogProducer, DataFileMeta, MergeEngine};
use crate::table::data_file_reader::DataFileReader;
use crate::table::kv_file_reader::key_value_fields_extractor;
use crate::table::merge_tree_split_generator::KeyComparator;
use crate::table::sort_merge::{LookupProvider, FILE_LEVEL_METADATA_KEY};
use crate::table::{DataSplit, Table};
use crate::{Error, Result};
use arrow_array::RecordBatch;
use arrow_row::{RowConverter, SortField};
use cache::invalid;
pub(super) type LookupDeletions = Arc<Mutex<HashMap<String, roaring::RoaringBitmap>>>;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(in crate::table) struct LookupDeletion {
    pub partition: Vec<u8>,
    pub bucket: i32,
    pub file_name: String,
    #[serde(with = "serde_bytes")]
    pub bitmap: Vec<u8>,
}

pub(in crate::table) fn needed(table: &Table) -> Result<bool> {
    let options = table.schema().core_options();
    Ok(options.deletion_vectors_enabled()
        || options.try_changelog_producer()? == ChangelogProducer::Lookup
        || options.merge_engine()? == MergeEngine::FirstRow)
}
struct HistoryFile {
    meta: DataFileMeta,
    identity: tokio::sync::OnceCell<String>,
    vector: Option<crate::DeletionFile>,
}
struct LevelLookup {
    table: Table,
    split: DataSplit,
    levels: BTreeMap<i32, Vec<HistoryFile>>,
    cache: Arc<LookupCache>,
    keys: Vec<usize>,
    converter: Mutex<RowConverter>,
    key_fields: Vec<crate::spec::DataField>,
    comparator: Option<KeyComparator>,
    resources: Option<ResourceContext>,
    vectors: tokio::sync::Mutex<VectorCache>,
    schema_identity: Vec<u8>,
    deletions: LookupDeletions,
}

struct VectorCache {
    entries: lru::LruCache<
        String,
        (
            Arc<DeletionVector>,
            Option<crate::resource::MemoryReservation>,
            usize,
        ),
    >,
    bytes: usize,
    limit: usize,
}

pub(super) async fn provider(
    table: &Table,
    split: &DataSplit,
    files: &[DataFileMeta],
    cache: Arc<LookupCache>,
    resources: Option<ResourceContext>,
) -> Result<(Arc<dyn LookupProvider>, LookupDeletions)> {
    let fields = super::read_type(table);
    let arrow_schema = crate::arrow::build_read_arrow_schema(&fields)?;
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
        .map(|key| fields[*key].clone())
        .collect::<Vec<_>>();
    let converter = RowConverter::new(
        keys.iter()
            .map(|key| SortField::new(arrow_schema.field(*key).data_type().clone()))
            .collect(),
    )
    .map_err(invalid)?;
    let comparator = KeyComparator::from_table_schema(table.schema());
    let all_split = DataSplit::builder()
        .with_snapshot(split.snapshot_id())
        .with_partition(split.partition().clone())
        .with_bucket(split.bucket())
        .with_total_buckets(split.total_buckets())
        .with_bucket_path(split.bucket_path().into())
        .with_data_files(files.to_vec())
        .build()?;
    let all_split = super::deletion_vectors::with_deletion_files(
        table,
        &all_split,
        table
            .travel_snapshot()
            .ok_or_else(|| invalid("Lookup needs its captured snapshot"))?,
    )
    .await?;
    let properties = table
        .schema()
        .options()
        .iter()
        .filter(|(key, _)| {
            !key.starts_with("scan.") && key.as_str() != "incremental-between-timestamp"
        })
        .collect::<BTreeMap<_, _>>();
    let schema_identity = serde_json::to_vec(&(
        table.schema().id(),
        table.schema().fields(),
        table.schema().primary_keys(),
        table.schema().partition_keys(),
        properties,
    ))
    .map_err(invalid)?;
    let mut levels = BTreeMap::<i32, Vec<HistoryFile>>::new();
    for (index, file) in files.iter().enumerate() {
        levels.entry(file.level).or_default().push(HistoryFile {
            meta: file.clone(),
            identity: tokio::sync::OnceCell::new(),
            vector: all_split
                .data_deletion_files()
                .and_then(|files| files.get(index))
                .cloned()
                .flatten(),
        });
    }
    for files in levels.values_mut() {
        files.sort_by(|a, b| {
            comparator
                .as_ref()
                .and_then(|cmp| cmp.compare(&a.meta.min_key, &b.meta.min_key))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        for pair in files.windows(2) {
            if comparator
                .as_ref()
                .and_then(|cmp| cmp.compare(&pair[0].meta.max_key, &pair[1].meta.min_key))
                != Some(std::cmp::Ordering::Less)
            {
                return Err(invalid(
                    "Lookup level has overlapping or unverifiable key ranges",
                ));
            }
        }
    }
    let deletions = Arc::new(Mutex::new(HashMap::new()));
    let vector_limit = cache.memory_limit() / 4;
    Ok((
        Arc::new(LevelLookup {
            table: table.clone(),
            split: all_split,
            levels,
            cache,
            keys,
            converter: Mutex::new(converter),
            key_fields,
            comparator,
            resources,
            vectors: tokio::sync::Mutex::new(VectorCache {
                entries: lru::LruCache::unbounded(),
                bytes: 0,
                limit: vector_limit,
            }),
            schema_identity,
            deletions: deletions.clone(),
        }),
        deletions,
    ))
}

impl LevelLookup {
    fn reader(&self) -> Result<DataFileReader> {
        let schema = self.table.schema();
        let budget = crate::table::table_read::configured_parquet_read_budget(&self.table)?;
        let budget = match &self.resources {
            Some(resources) => budget.with_resources(resources.clone()),
            None => budget.without_prefetch(),
        };
        Ok(DataFileReader::new(
            self.table.file_io().clone(),
            self.table.schema_manager().clone(),
            schema.id(),
            schema.fields().to_vec(),
            key_value_fields_extractor(schema.fields(), &schema.trimmed_primary_keys()),
            super::read_type(&self.table),
            vec![],
        )
        .with_batch_size(Some(schema.core_options().read_batch_size()?))
        .with_table_options(schema.options().clone())
        .with_parquet_read_budget(Some(Arc::new(budget.without_prefetch()))))
    }
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
            let mut row = crate::spec::BinaryRowBuilder::new(self.key_fields.len() as i32);
            for (index, field) in self.key_fields.iter().enumerate() {
                if let Some(datum) =
                    crate::spec::extract_datum_from_arrow(&batch, 0, index, field.data_type())?
                {
                    row.write_datum(index, &datum, field.data_type());
                } else {
                    row.set_null_at(index);
                }
            }
            row.build_serialized()
        };
        for files in self.levels.values() {
            let index = files.partition_point(|file| {
                self.comparator
                    .as_ref()
                    .and_then(|cmp| cmp.compare(&file.meta.max_key, &binary_key))
                    == Some(std::cmp::Ordering::Less)
            });
            let Some(file) = files.get(index) else {
                continue;
            };
            if self
                .comparator
                .as_ref()
                .and_then(|cmp| cmp.compare(&file.meta.min_key, &binary_key))
                == Some(std::cmp::Ordering::Greater)
            {
                continue;
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
                    digest.update(b"paimon-lookup-v1");
                    digest.update(namespace.as_bytes());
                    digest.update(serde_json::to_vec(&physical).map_err(invalid)?);
                    digest.update(&self.schema_identity);
                    Ok::<_, Error>(hex::encode(digest.finalize()))
                })
                .await?;
            let mut result = None;
            for attempt in 0..2 {
                let cached = self
                    .cache
                    .get(
                        identity,
                        || {
                            self.reader()?.read_single_file_stream(
                                &self.split,
                                file.meta.clone(),
                                None,
                                None,
                                None,
                            )
                        },
                        &self.keys,
                    )
                    .await;
                match cached {
                    Ok(cached) => match cached.lookup(key).await {
                        Ok(row) => {
                            result = row;
                            break;
                        }
                        Err(error @ Error::ResourceExhausted { .. }) => return Err(error),
                        Err(error) => {
                            drop(cached);
                            self.cache.invalidate(identity);
                            if attempt == 1 {
                                return Err(error);
                            }
                        }
                    },
                    Err(error @ Error::ResourceExhausted { .. }) => return Err(error),
                    Err(error) => {
                        self.cache.invalidate(identity);
                        if attempt == 1 {
                            return Err(error);
                        }
                    }
                }
            }
            if let Some((batch, position)) = result {
                if let Some(deletion_file) = &file.vector {
                    let mut vectors = self.vectors.lock().await;
                    if !vectors.entries.contains(&file.meta.file_name) {
                        if usize::try_from(deletion_file.length())
                            .map_or(true, |length| length > vectors.limit)
                        {
                            return Err(Error::ResourceExhausted {
                                message: "Lookup deletion vector exceeds its cache memory budget"
                                    .into(),
                            });
                        }
                        let vector = Arc::new(
                            DeletionVectorFactory::read(self.table.file_io(), deletion_file)
                                .await?,
                        );
                        let size = vector.retained_bytes();
                        if size > vectors.limit {
                            return Err(Error::ResourceExhausted {
                                message: "Lookup deletion vector exceeds its cache memory budget"
                                    .into(),
                            });
                        }
                        while vectors.bytes.saturating_add(size) > vectors.limit {
                            if let Some((_, (_, reservation, bytes))) = vectors.entries.pop_lru() {
                                vectors.bytes = vectors.bytes.saturating_sub(bytes);
                                drop(reservation);
                            } else {
                                break;
                            }
                        }
                        let reservation = self
                            .resources
                            .as_ref()
                            .map(|resources| -> Result<_> {
                                let mut reservation = resources.reservation();
                                reservation.try_grow(size)?;
                                Ok(reservation)
                            })
                            .transpose()?;
                        vectors
                            .entries
                            .put(file.meta.file_name.clone(), (vector, reservation, size));
                        vectors.bytes += size;
                    }
                    if vectors
                        .entries
                        .get(&file.meta.file_name)
                        .unwrap()
                        .0
                        .is_deleted(position)
                    {
                        continue;
                    }
                }
                if self
                    .table
                    .schema()
                    .core_options()
                    .deletion_vectors_enabled()
                {
                    let position = u32::try_from(position)
                        .map_err(|_| invalid("PK deletion vector position exceeds u32::MAX"))?;
                    self.deletions
                        .lock()
                        .unwrap()
                        .entry(file.meta.file_name.clone())
                        .or_default()
                        .insert(position);
                }
                let mut metadata = batch.schema().metadata().clone();
                metadata.insert(FILE_LEVEL_METADATA_KEY.into(), file.meta.level.to_string());
                metadata.insert("paimon.compact.lookup-base".into(), "true".into());
                return RecordBatch::try_new(
                    Arc::new(batch.schema().as_ref().clone().with_metadata(metadata)),
                    batch.columns().to_vec(),
                )
                .map(Some)
                .map_err(invalid);
            }
        }
        Ok(None)
    }
}
