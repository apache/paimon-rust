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

//! Global index scanner: evaluates predicates against queryable global indexes
//! to produce row ID ranges for data evolution tables.
//!
//! Reference: [org.apache.paimon.index.GlobalIndexScanner](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/index/GlobalIndexScanner.java)

mod deletion_vectors;
mod entry;
mod evaluator;
mod predicates;
mod query_plan;
mod reader;
mod row_ranges;

pub(crate) use deletion_vectors::deleted_row_ranges_for_data_evolution_dvs;
use entry::{validate_fm_file_sets, GlobalIndexEntry, GlobalIndexEntryMeta, GlobalIndexFileKind};
use row_ranges::unindexed_ranges_for_indexed_coverage;
pub(crate) use row_ranges::{
    search_limit_with_deleted_rows, unindexed_ranges_for_global_index_entries, RowRangeIndex,
};

use super::global_index_types::{
    normalize_queryable_global_index_type, BITMAP_GLOBAL_INDEX_TYPE, BTREE_GLOBAL_INDEX_TYPE,
    FM_GLOBAL_INDEX_TYPE, MULTIVALUE_GLOBAL_INDEX_TYPE,
};
use crate::btree::{BTreeIndexMeta, BTreeIndexReader};
use crate::fm_index::{manifest_row_range, FMReadContext, FMReadOptions};
use crate::io::FileIO;
use crate::spec::{DataField, FileKind, GlobalIndexSearchMode, IndexManifestEntry, Predicate};
use crate::table::RowRange;
use crate::{Error, Result};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;

#[cfg(test)]
use std::sync::atomic::{AtomicUsize as TestAtomicUsize, Ordering as TestOrdering};

type BoxedCmp = Box<dyn Fn(&[u8], &[u8]) -> Ordering + Send + Sync>;

const DELETION_VECTORS_INDEX_TYPE: &str = "DELETION_VECTORS";
const INDEX_DIR: &str = "index";

#[cfg(test)]
#[derive(Default)]
struct QueryIoProbe {
    active: TestAtomicUsize,
    peak: TestAtomicUsize,
}

#[cfg(test)]
impl QueryIoProbe {
    async fn enter(&self) -> QueryIoProbeGuard<'_> {
        let current = self.active.fetch_add(1, TestOrdering::SeqCst) + 1;
        self.peak.fetch_max(current, TestOrdering::SeqCst);
        let guard = QueryIoProbeGuard { probe: self };
        tokio::task::yield_now().await;
        guard
    }

    fn peak(&self) -> usize {
        self.peak.load(TestOrdering::SeqCst)
    }
}

#[cfg(test)]
struct QueryIoProbeGuard<'a> {
    probe: &'a QueryIoProbe,
}

#[cfg(test)]
impl Drop for QueryIoProbeGuard<'_> {
    fn drop(&mut self) {
        self.probe.active.fetch_sub(1, TestOrdering::SeqCst);
    }
}

/// Evaluates global index predicates and returns matching row ranges.
///
/// The scanner filters index manifest entries for global index files,
/// uses type-specific metadata for file-level pruning, then reads matching
/// BTree, bitmap, multivalue, or FM files to evaluate predicates and collect row IDs.
/// Opened BTreeIndexReaders are cached for reuse across evaluations.
pub(crate) struct GlobalIndexScanner {
    file_io: FileIO,
    table_path: String,
    global_index_thread_num: usize,
    /// Scan-scoped shard I/O budget shared by all indexed fields.
    query_semaphore: Arc<Semaphore>,
    btree_fallback_scan_max_size: i64,
    bitmap_fallback_scan_max_size: i64,
    fm_read_options: FMReadOptions,
    fm_read_context: Arc<FMReadContext>,
    /// Global index entries grouped by field_id.
    entries_by_field: Vec<(i32, Vec<GlobalIndexEntry>)>,
    /// Indexed row-id coverage grouped by field_id.
    #[cfg(test)]
    coverage_by_field: HashMap<i32, Vec<RowRange>>,
    /// Schema fields for field_id lookup.
    schema_fields: Vec<DataField>,
    /// Cache of opened BTree readers, keyed by file name.
    reader_cache: Mutex<HashMap<String, BTreeIndexReader<BoxedCmp>>>,
    #[cfg(test)]
    query_io_probe: Option<Arc<QueryIoProbe>>,
}

impl GlobalIndexScanner {
    /// Create a scanner from index manifest entries.
    /// Returns `Ok(None)` if there are no global index entries.
    #[cfg(test)]
    pub(crate) fn create(
        file_io: &FileIO,
        table_path: &str,
        global_index_thread_num: usize,
        btree_fallback_scan_max_size: i64,
        bitmap_fallback_scan_max_size: i64,
        index_entries: &[IndexManifestEntry],
        schema_fields: &[DataField],
    ) -> Result<Option<Self>> {
        Self::create_with_fm_options(
            file_io,
            table_path,
            global_index_thread_num,
            btree_fallback_scan_max_size,
            bitmap_fallback_scan_max_size,
            index_entries,
            schema_fields,
            FMReadOptions::default(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn create_with_fm_options(
        file_io: &FileIO,
        table_path: &str,
        global_index_thread_num: usize,
        btree_fallback_scan_max_size: i64,
        bitmap_fallback_scan_max_size: i64,
        index_entries: &[IndexManifestEntry],
        schema_fields: &[DataField],
        fm_read_options: FMReadOptions,
    ) -> Result<Option<Self>> {
        if global_index_thread_num == 0 {
            return Err(Error::DataInvalid {
                message: "Global index thread count must be greater than 0".to_string(),
                source: None,
            });
        }
        if global_index_thread_num > Semaphore::MAX_PERMITS {
            return Err(Error::DataInvalid {
                message: format!(
                    "Global index thread count must not exceed {}",
                    Semaphore::MAX_PERMITS
                ),
                source: None,
            });
        }
        let mut entries_by_field: std::collections::HashMap<i32, Vec<GlobalIndexEntry>> =
            std::collections::HashMap::new();
        #[cfg(test)]
        let mut coverage_by_field: HashMap<i32, Vec<RowRange>> = HashMap::new();

        for entry in index_entries {
            if entry.kind != FileKind::Add {
                continue;
            }
            let Some(index_type) =
                normalize_queryable_global_index_type(&entry.index_file.index_type)
            else {
                continue;
            };
            let global_meta =
                entry
                    .index_file
                    .global_index_meta
                    .as_ref()
                    .ok_or_else(|| Error::DataInvalid {
                        message: format!(
                            "Missing global index metadata for queryable index file '{}'",
                            entry.index_file.file_name
                        ),
                        source: None,
                    })?;

            let index_meta = global_meta
                .index_meta
                .as_ref()
                .ok_or_else(|| Error::DataInvalid {
                    message: format!(
                        "Missing queryable global index metadata for file '{}'",
                        entry.index_file.file_name
                    ),
                    source: None,
                })?;
            let kind = match index_type {
                BTREE_GLOBAL_INDEX_TYPE => GlobalIndexFileKind::BTree,
                BITMAP_GLOBAL_INDEX_TYPE => GlobalIndexFileKind::Bitmap,
                MULTIVALUE_GLOBAL_INDEX_TYPE => GlobalIndexFileKind::Multivalue,
                FM_GLOBAL_INDEX_TYPE => GlobalIndexFileKind::FM,
                _ => unreachable!("normalized queryable global index type"),
            };
            let parsed_meta = if kind == GlobalIndexFileKind::FM {
                let (first_row_id, row_count) =
                    manifest_row_range(index_meta).map_err(|error| Error::DataInvalid {
                        message: format!(
                            "Invalid FM global index metadata for file '{}'",
                            entry.index_file.file_name
                        ),
                        source: Some(Box::new(error)),
                    })?;
                let manifest_row_count =
                    i64::try_from(row_count).map_err(|_| Error::DataInvalid {
                        message: format!(
                            "FM global index row count does not fit i64 for file '{}'",
                            entry.index_file.file_name
                        ),
                        source: None,
                    })?;
                if entry.index_file.row_count != manifest_row_count {
                    return Err(Error::DataInvalid {
                        message: format!(
                            "FM global index row count mismatch for file '{}': manifest={}, file={}",
                            entry.index_file.file_name, row_count, entry.index_file.row_count
                        ),
                        source: None,
                    });
                }
                GlobalIndexEntryMeta::FM {
                    bytes: index_meta.clone(),
                    first_row_id,
                    row_count,
                }
            } else {
                GlobalIndexEntryMeta::Sorted(BTreeIndexMeta::deserialize(index_meta).map_err(
                    |error| Error::DataInvalid {
                        message: format!(
                            "Invalid sorted global index metadata for file '{}'",
                            entry.index_file.file_name
                        ),
                        source: Some(Box::new(error)),
                    },
                )?)
            };

            let resolved = GlobalIndexEntry {
                file_name: entry.index_file.file_name.clone(),
                index_type: kind,
                file_size: entry.index_file.file_size,
                row_range_start: global_meta.row_range_start,
                row_range_end: global_meta.row_range_end,
                meta: parsed_meta,
            };

            #[cfg(test)]
            let row_range = RowRange::new(global_meta.row_range_start, global_meta.row_range_end);
            #[cfg(test)]
            coverage_by_field
                .entry(global_meta.index_field_id)
                .or_default()
                .push(row_range.clone());
            #[cfg(test)]
            if let Some(extra_field_ids) = global_meta.extra_field_ids.as_ref() {
                for extra_field_id in extra_field_ids {
                    coverage_by_field
                        .entry(*extra_field_id)
                        .or_default()
                        .push(row_range.clone());
                }
            }

            entries_by_field
                .entry(global_meta.index_field_id)
                .or_default()
                .push(resolved);
        }

        validate_fm_file_sets(&entries_by_field)?;

        if entries_by_field.is_empty() {
            return Ok(None);
        }

        Ok(Some(Self {
            file_io: file_io.clone(),
            table_path: table_path.trim_end_matches('/').to_string(),
            global_index_thread_num,
            query_semaphore: Arc::new(Semaphore::new(global_index_thread_num)),
            btree_fallback_scan_max_size,
            bitmap_fallback_scan_max_size,
            fm_read_options,
            fm_read_context: Arc::new(FMReadContext::new(fm_read_options.cache_size)),
            entries_by_field: entries_by_field.into_iter().collect(),
            #[cfg(test)]
            coverage_by_field,
            schema_fields: schema_fields.to_vec(),
            reader_cache: Mutex::new(HashMap::new()),
            #[cfg(test)]
            query_io_probe: None,
        }))
    }
}

/// Create a GlobalIndexScanner and evaluate predicates, returning row ranges.
/// This is the main entry point for the table scan integration.
///
/// Returns `None` if global index is not available or predicates can't be evaluated.
pub(crate) struct GlobalIndexEvaluation<'a> {
    pub(crate) file_io: &'a FileIO,
    pub(crate) table_path: &'a str,
    pub(crate) index_entries: &'a [IndexManifestEntry],
    pub(crate) predicates: &'a [Predicate],
    pub(crate) schema_fields: &'a [DataField],
    pub(crate) search_mode: GlobalIndexSearchMode,
    pub(crate) global_index_thread_num: usize,
    pub(crate) btree_fallback_scan_max_size: i64,
    pub(crate) bitmap_fallback_scan_max_size: i64,
    pub(crate) fm_read_options: FMReadOptions,
    pub(crate) next_row_id: Option<i64>,
    pub(crate) data_ranges: &'a [RowRange],
}

pub(crate) async fn evaluate_global_index(
    evaluation: GlobalIndexEvaluation<'_>,
) -> Result<Option<Vec<RowRange>>> {
    let scanner = match GlobalIndexScanner::create_with_fm_options(
        evaluation.file_io,
        evaluation.table_path,
        evaluation.global_index_thread_num,
        evaluation.btree_fallback_scan_max_size,
        evaluation.bitmap_fallback_scan_max_size,
        evaluation.index_entries,
        evaluation.schema_fields,
        evaluation.fm_read_options,
    )? {
        Some(s) => s,
        None => return Ok(None),
    };

    let combined = Predicate::and(evaluation.predicates.to_vec());

    let scan_result = match scanner.evaluate(&combined).await? {
        Some(scan_result) => scan_result,
        None => return Ok(None),
    };
    let mut row_ranges = scan_result.row_ranges;
    row_ranges.extend(unindexed_ranges_for_indexed_coverage(
        &scan_result.indexed_coverage,
        evaluation.search_mode,
        evaluation.next_row_id,
        evaluation.data_ranges,
    ));
    Ok(Some(super::merge_row_ranges(row_ranges)))
}

#[cfg(test)]
mod tests;
