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

//! Type-specific index readers and shard I/O execution.

use super::entry::{
    sorted_entry_meta, GlobalIndexEntry, GlobalIndexEntryMeta, GlobalIndexFileKind,
};
use super::query_plan::{add_file_size, EntryQueryPlan, EntryQueryResult, FallbackScanPlan};
use super::{BoxedCmp, GlobalIndexScanner, INDEX_DIR};
use crate::btree::query::{BetweenInfo, IndexQuery};
use crate::btree::{make_key_comparator, serialize_datum, BTreeIndexMeta, BTreeIndexReader};
use crate::fm_index::FMGlobalIndexReader;
use crate::spec::{DataType, Datum, PredicateOperator};
use crate::table::bitmap_global_index_format::serialize_bitmap_datum;
use crate::table::bitmap_global_index_reader::BitmapGlobalIndexReader;
use crate::{Error, Result};
use roaring::RoaringTreemap;
use std::sync::Arc;

pub(super) enum OpenedGlobalIndexReader {
    BTree(BTreeIndexReader<BoxedCmp>),
    Bitmap(BitmapGlobalIndexReader),
    FM(FMGlobalIndexReader),
}

impl OpenedGlobalIndexReader {
    async fn query(
        &self,
        op: PredicateOperator,
        literals: &[Datum],
        data_type: &DataType,
    ) -> std::io::Result<Option<RoaringTreemap>> {
        match self {
            Self::BTree(reader) => reader.query(op, literals, data_type).await.map(Some),
            Self::Bitmap(reader) => reader.query(op, literals, data_type).await.map(Some),
            Self::FM(reader) => match op {
                PredicateOperator::Contains => {
                    let literal = literals.first().ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "FM contains requires one literal",
                        )
                    })?;
                    reader
                        .contains(&serialize_bitmap_datum(literal, data_type))
                        .await
                }
                PredicateOperator::IsNull => reader.is_null().await.map(Some),
                PredicateOperator::IsNotNull => reader.is_not_null().await.map(Some),
                _ => Ok(None),
            },
        }
    }

    async fn range_query(
        &self,
        from: &[u8],
        to: &[u8],
        data_type: &DataType,
        from_inclusive: bool,
        to_inclusive: bool,
    ) -> std::io::Result<RoaringTreemap> {
        match self {
            Self::BTree(reader) => {
                reader
                    .range_query(from, to, from_inclusive, to_inclusive)
                    .await
            }
            Self::Bitmap(reader) => {
                reader
                    .range_query(from, to, data_type, from_inclusive, to_inclusive)
                    .await
            }
            Self::FM(_) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "FM index does not support ordered range queries",
            )),
        }
    }
}

impl GlobalIndexScanner {
    pub(super) async fn query_entry(
        &self,
        entry: &GlobalIndexEntry,
        data_type: &DataType,
        between: Option<&BetweenInfo<'_>>,
        plan: &EntryQueryPlan,
        effective_predicates: &[(PredicateOperator, &[Datum], &DataType)],
    ) -> Result<EntryQueryResult> {
        let mut reader = if (plan.between_matches && plan.between_evaluated)
            || !plan.matching_predicates.is_empty()
        {
            Some(self.open_reader_for_entry(entry, data_type).await?)
        } else {
            None
        };
        let mut file_result = None;

        if plan.between_matches && plan.between_evaluated {
            let between = between.expect("evaluated between query is present");
            let serialize_key = match entry.index_type {
                GlobalIndexFileKind::BTree => serialize_datum,
                GlobalIndexFileKind::Bitmap | GlobalIndexFileKind::Multivalue => {
                    serialize_bitmap_datum
                }
                GlobalIndexFileKind::FM => unreachable!("FM range query was rejected in planning"),
            };
            let from_key = serialize_key(between.from, between.data_type);
            let to_key = serialize_key(between.to, between.data_type);
            let bitmap = reader
                .as_ref()
                .expect("reader is opened when between matches")
                .range_query(
                    &from_key,
                    &to_key,
                    between.data_type,
                    between.from_inclusive,
                    between.to_inclusive,
                )
                .await
                .map_err(|error| Self::query_error(entry, error))?;
            file_result = Some(bitmap);
        }

        for &idx in &plan.matching_predicates {
            let (op, literals, data_type) = &effective_predicates[idx];
            let Some(bitmap) = reader
                .as_ref()
                .expect("reader is opened when predicates match")
                .query(*op, literals, data_type)
                .await
                .map_err(|error| Self::query_error(entry, error))?
            else {
                return Ok(EntryQueryResult {
                    bitmap: None,
                    declined: true,
                });
            };
            file_result = Some(match file_result {
                None => bitmap,
                Some(mut existing) => {
                    existing &= bitmap;
                    existing
                }
            });
        }

        // Each concurrent task owns its reader. Only return it to the shared
        // cache after all predicates for this shard have completed.
        if let Some(OpenedGlobalIndexReader::BTree(reader)) = reader.take() {
            self.return_reader(entry.file_name.clone(), reader);
        }
        Ok(EntryQueryResult {
            bitmap: file_result,
            declined: false,
        })
    }

    fn query_error(entry: &GlobalIndexEntry, error: std::io::Error) -> Error {
        Error::DataInvalid {
            message: format!(
                "Global index query failed for {} file '{}'",
                entry.index_type.name(),
                entry.file_name
            ),
            source: Some(Box::new(error)),
        }
    }

    /// Get a cached reader or open a new one for the given file.
    async fn get_or_open_reader(
        &self,
        entry: &GlobalIndexEntry,
        meta: &BTreeIndexMeta,
        data_type: &DataType,
    ) -> Result<OpenedGlobalIndexReader> {
        // Try to take from cache
        {
            let mut cache = self.reader_cache.lock().unwrap();
            if let Some(reader) = cache.remove(&entry.file_name) {
                return Ok(OpenedGlobalIndexReader::BTree(reader));
            }
        }

        // Open new reader
        let path = format!("{}/{INDEX_DIR}/{}", self.table_path, entry.file_name);
        let input = self.file_io.new_input(&path)?;
        let file_size = if entry.file_size > 0 {
            entry.file_size as u64
        } else {
            input.metadata().await?.size
        };
        let file_reader = input.reader().await?;

        let cmp = make_key_comparator(data_type);
        BTreeIndexReader::open(Box::new(file_reader), file_size, meta, cmp)
            .await
            .map(OpenedGlobalIndexReader::BTree)
            .map_err(|e| crate::Error::DataInvalid {
                message: format!("Failed to open BTree index file: {}", entry.file_name),
                source: Some(Box::new(e)),
            })
    }

    async fn open_reader_for_entry(
        &self,
        entry: &GlobalIndexEntry,
        data_type: &DataType,
    ) -> Result<OpenedGlobalIndexReader> {
        match entry.index_type {
            GlobalIndexFileKind::BTree => {
                self.get_or_open_reader(entry, sorted_entry_meta(entry), data_type)
                    .await
            }
            GlobalIndexFileKind::Bitmap => self
                .open_bitmap_reader(entry)
                .await
                .map(OpenedGlobalIndexReader::Bitmap)
                .map_err(|e| crate::Error::DataInvalid {
                    message: format!(
                        "Failed to open bitmap global index file: {}",
                        entry.file_name
                    ),
                    source: Some(Box::new(e)),
                }),
            GlobalIndexFileKind::Multivalue => self
                .open_bitmap_reader(entry)
                .await
                .map(OpenedGlobalIndexReader::Bitmap)
                .map_err(|e| crate::Error::DataInvalid {
                    message: format!(
                        "Failed to open multivalue global index file: {}",
                        entry.file_name
                    ),
                    source: Some(Box::new(e)),
                }),
            GlobalIndexFileKind::FM => self
                .open_fm_reader(entry)
                .await
                .map(OpenedGlobalIndexReader::FM)
                .map_err(|e| crate::Error::DataInvalid {
                    message: format!("Failed to open FM global index file: {}", entry.file_name),
                    source: Some(Box::new(e)),
                }),
        }
    }

    async fn open_fm_reader(
        &self,
        entry: &GlobalIndexEntry,
    ) -> std::io::Result<FMGlobalIndexReader> {
        let GlobalIndexEntryMeta::FM {
            bytes: manifest_meta,
            ..
        } = &entry.meta
        else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "FM entry has non-FM manifest metadata",
            ));
        };
        let path = format!("{}/{INDEX_DIR}/{}", self.table_path, entry.file_name);
        let input = self
            .file_io
            .new_input(&path)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        let file_size = if entry.file_size > 0 {
            entry.file_size as u64
        } else {
            input
                .metadata()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?
                .size
        };
        let file_reader = input
            .reader()
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        FMGlobalIndexReader::open_with_context(
            Box::new(file_reader),
            file_size,
            manifest_meta,
            self.fm_read_options,
            Arc::clone(&self.fm_read_context),
            entry.file_name.clone(),
        )
        .await
    }

    async fn open_bitmap_reader(
        &self,
        entry: &GlobalIndexEntry,
    ) -> std::io::Result<BitmapGlobalIndexReader> {
        let path = format!("{}/{INDEX_DIR}/{}", self.table_path, entry.file_name);
        let input = self
            .file_io
            .new_input(&path)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        let file_size = if entry.file_size > 0 {
            entry.file_size as u64
        } else {
            input
                .metadata()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?
                .size
        };
        let file_reader = input
            .reader()
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        BitmapGlobalIndexReader::open(Box::new(file_reader), file_size).await
    }

    pub(super) fn fallback_scan_plan(
        &self,
        entries: &[&GlobalIndexEntry],
        selected: &[bool],
    ) -> FallbackScanPlan {
        let mut plan = FallbackScanPlan::default();
        let mut btree_total = 0i64;
        let mut bitmap_total = 0i64;
        let mut btree_valid = true;
        let mut bitmap_valid = true;

        for (entry, selected) in entries.iter().zip(selected) {
            if !selected {
                continue;
            }
            match entry.index_type {
                GlobalIndexFileKind::BTree => {
                    plan.selected_btree += 1;
                    btree_valid &= add_file_size(&mut btree_total, entry.file_size);
                }
                GlobalIndexFileKind::Bitmap => {
                    plan.selected_bitmap += 1;
                    bitmap_valid &= add_file_size(&mut bitmap_total, entry.file_size);
                }
                GlobalIndexFileKind::Multivalue => {
                    plan.selected_bitmap += 1;
                    bitmap_valid &= add_file_size(&mut bitmap_total, entry.file_size);
                }
                GlobalIndexFileKind::FM => {}
            }
        }

        plan.allow_btree = plan.selected_btree > 0
            && btree_valid
            && self.btree_fallback_scan_max_size > 0
            && btree_total <= self.btree_fallback_scan_max_size;
        plan.allow_bitmap = plan.selected_bitmap > 0
            && bitmap_valid
            && self.bitmap_fallback_scan_max_size > 0
            && bitmap_total <= self.bitmap_fallback_scan_max_size;
        plan
    }

    /// Return a reader to the cache for future reuse.
    fn return_reader(&self, file_name: String, reader: BTreeIndexReader<BoxedCmp>) {
        let mut cache = self.reader_cache.lock().unwrap();
        cache.insert(file_name, reader);
    }
}
