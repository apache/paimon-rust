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
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

struct HistoryFile {
    meta: DataFileMeta,
    identity: tokio::sync::OnceCell<String>,
}

struct LevelLookup {
    table: Table,
    split: DataSplit,
    levels: BTreeMap<i32, Vec<HistoryFile>>,
    cache: Arc<LookupFileCache>,
    keys: Vec<usize>,
    key_fields: Vec<DataField>,
    converter: Mutex<RowConverter>,
    comparator: Option<KeyComparator>,
    resources: Option<ResourceContext>,
    schema_identity: Vec<u8>,
}

pub(in crate::table::compact) fn provider(
    table: &Table,
    split: &DataSplit,
    files: Vec<DataFileMeta>,
    cache: Arc<LookupFileCache>,
    resources: Option<ResourceContext>,
) -> Result<Arc<dyn LookupProvider>> {
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
    let mut levels = BTreeMap::<_, Vec<_>>::new();
    for meta in files {
        levels.entry(meta.level).or_default().push(HistoryFile {
            meta,
            identity: tokio::sync::OnceCell::new(),
        });
    }
    let options = table.schema().options().iter().collect::<BTreeMap<_, _>>();
    let schema_identity = serde_json::to_vec(&(
        table.schema().fields(),
        table.schema().primary_keys(),
        table.schema().partition_keys(),
        options,
    ))
    .map_err(invalid)?;
    Ok(Arc::new(LevelLookup {
        table: table.clone(),
        split: split.clone(),
        levels,
        cache,
        keys,
        key_fields,
        converter: Mutex::new(converter),
        comparator: KeyComparator::from_table_schema(table.schema()),
        resources,
        schema_identity,
    }))
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
        for files in self.levels.values() {
            let mut found = None;
            for file in files {
                // Old key types may have been promoted. Unverifiable ranges
                // must not prune a possible history match.
                if file.meta.schema_id == self.table.schema().id() {
                    if let Some(compare) = &self.comparator {
                        if compare.compare(&file.meta.max_key, &binary_key)
                            == Some(std::cmp::Ordering::Less)
                            || compare.compare(&file.meta.min_key, &binary_key)
                                == Some(std::cmp::Ordering::Greater)
                        {
                            continue;
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
                let row = self
                    .cache
                    .lookup(identity, key, &self.keys, || {
                        let split = DataSplit::builder()
                            .with_snapshot(self.split.snapshot_id())
                            .with_partition(self.split.partition().clone())
                            .with_bucket(self.split.bucket())
                            .with_total_buckets(self.split.total_buckets())
                            .with_bucket_path(self.split.bucket_path().to_string())
                            .with_data_files(vec![file.meta.clone()])
                            .build()?;
                        // Reuse the normal KV decoder for schema evolution, file
                        // formats, and preservation of system sequence/row kind.
                        super::new_reader(&self.table, self.resources.clone())?
                            .read_with_merge_function(&[split], |_, _| {
                                Ok(Box::new(super::CompactDeduplicate))
                            })
                    })
                    .await?;
                if let Some(batch) = row {
                    if found.is_some() {
                        return Err(invalid(
                            "Lookup history has overlapping keys within a positive level",
                        ));
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
