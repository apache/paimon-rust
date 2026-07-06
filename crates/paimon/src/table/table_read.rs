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

use super::data_evolution_reader::DataEvolutionReader;
use super::data_file_reader::DataFileReader;
use super::kv_file_reader::{KeyValueFileReader, KeyValueReadConfig};
use super::read_builder::split_scan_predicates;
use super::{ArrowRecordBatchStream, Table};
use crate::spec::{CoreOptions, DataField, MergeEngine, Predicate};
use crate::DataSplit;

/// Table read: reads data from splits (e.g. produced by [TableScan::plan]).
///
/// Reference: [pypaimon.read.table_read.TableRead](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/read/table_read.py)
#[derive(Debug, Clone)]
pub struct TableRead<'a> {
    table: &'a Table,
    read_type: Vec<DataField>,
    data_predicates: Vec<Predicate>,
}

impl<'a> TableRead<'a> {
    /// Create a new TableRead with a specific read type (projected fields).
    pub fn new(
        table: &'a Table,
        read_type: Vec<DataField>,
        data_predicates: Vec<Predicate>,
    ) -> Self {
        Self {
            table,
            read_type,
            data_predicates,
        }
    }

    /// Schema (fields) that this read will produce.
    pub fn read_type(&self) -> &[DataField] {
        &self.read_type
    }

    /// Data predicates for read-side pruning.
    pub fn data_predicates(&self) -> &[Predicate] {
        &self.data_predicates
    }

    /// Table for this read.
    pub fn table(&self) -> &Table {
        self.table
    }

    /// Set a filter predicate. Used conservatively for read-side pruning and
    /// enforced exactly by residual filtering on append, data-evolution, and
    /// primary-key merge read paths (see
    /// [`ReadBuilder::with_filter`](crate::table::ReadBuilder::with_filter)
    /// for per-format exceptions).
    pub fn with_filter(mut self, filter: Predicate) -> Self {
        let (_, data_predicates) = split_scan_predicates(self.table, filter);
        // Keep the FULL data predicate (including `And`/`Or`/`Not`). Native
        // pushdown / stats pruning skip compound nodes they cannot use, and the
        // residual pass applies the full predicate exactly. Pruning here would
        // drop compound predicates before the residual could enforce them.
        self.data_predicates = data_predicates;
        self
    }

    /// Returns an [`ArrowRecordBatchStream`].
    pub fn to_arrow(&self, data_splits: &[DataSplit]) -> crate::Result<ArrowRecordBatchStream> {
        let has_primary_keys = !self.table.schema.primary_keys().is_empty();
        let core_options = CoreOptions::new(self.table.schema.options());
        // Fail closed for a direct `TableRead` (bypassing `ReadBuilder::new_read`).
        core_options.ensure_read_authorized()?;
        let merge_engine = core_options.merge_engine()?;

        // PK table with Deduplicate engine: splits that may hold multiple
        // versions of a key need KeyValueFileReader for sort-merge dedup;
        // splits marked raw convertible by scan planning — and all compacted
        // files of deletion-vector tables, where DVs mask stale versions —
        // use the faster DataFileReader.
        // PartialUpdate / Aggregation always go through KeyValueFileReader so
        // that per-key materialization can run on the read side.
        if has_primary_keys
            && matches!(
                merge_engine,
                MergeEngine::Deduplicate | MergeEngine::PartialUpdate | MergeEngine::Aggregation
            )
        {
            return self.read_pk(data_splits, &core_options);
        }

        if core_options.data_evolution_enabled() {
            self.read_with_evolution(data_splits, &core_options)
        } else {
            self.read_raw(data_splits)
        }
    }

    /// Read PK table. For `Deduplicate`, splits marked raw convertible by scan
    /// planning (mirrors Java `DataSplit#convertToRawFiles`) use the faster
    /// DataFileReader; the rest go through KeyValueFileReader for sort-merge
    /// dedup. Deletion-vector tables are exempt: their stale versions are
    /// masked by DVs, and KeyValueFileReader does not support DVs, so they keep
    /// the plain level-0 dispatch. `PartialUpdate` and `Aggregation` always go
    /// through KeyValueFileReader because their merge semantics require per-key
    /// materialization even for compacted runs.
    fn read_pk(
        &self,
        data_splits: &[DataSplit],
        core_options: &CoreOptions,
    ) -> crate::Result<ArrowRecordBatchStream> {
        if matches!(
            core_options.merge_engine()?,
            MergeEngine::PartialUpdate | MergeEngine::Aggregation
        ) {
            return self.read_kv(data_splits, core_options);
        }

        // Deletion-vector tables read raw by design: stale versions of a key
        // are masked by DVs, not merged, and KeyValueFileReader does not
        // support DVs. Keep the plain level-0 dispatch for them.
        let dv_enabled = core_options.deletion_vectors_enabled();

        let mut kv_splits = Vec::new();
        let mut raw_splits = Vec::new();
        for split in data_splits {
            if pk_split_needs_merge(split, dv_enabled) {
                kv_splits.push(split.clone());
            } else {
                raw_splits.push(split.clone());
            }
        }

        if raw_splits.is_empty() {
            return self.read_kv(&kv_splits, core_options);
        }
        if kv_splits.is_empty() {
            return self.read_raw(&raw_splits);
        }

        let kv_stream = self.read_kv(&kv_splits, core_options)?;
        let raw_stream = self.read_raw(&raw_splits)?;
        Ok(Box::pin(futures::stream::select_all([
            kv_stream, raw_stream,
        ])))
    }

    /// Read splits via KeyValueFileReader (sort-merge dedup).
    fn read_kv(
        &self,
        splits: &[DataSplit],
        core_options: &CoreOptions,
    ) -> crate::Result<ArrowRecordBatchStream> {
        let reader = KeyValueFileReader::new(
            self.table.file_io.clone(),
            KeyValueReadConfig {
                table_name: self.table.identifier().full_name(),
                table_options: self.table.schema().options().clone(),
                schema_manager: self.table.schema_manager().clone(),
                table_schema_id: self.table.schema().id(),
                table_fields: self.table.schema.fields().to_vec(),
                read_type: self.read_type().to_vec(),
                predicates: self.data_predicates.clone(),
                primary_keys: self.table.schema.trimmed_primary_keys(),
                merge_engine: core_options.merge_engine()?,
                sequence_fields: core_options
                    .sequence_fields()
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
            },
        );
        reader.read(splits)
    }

    /// Read with data-evolution support.
    fn read_with_evolution(
        &self,
        data_splits: &[DataSplit],
        core_options: &CoreOptions,
    ) -> crate::Result<ArrowRecordBatchStream> {
        let reader = DataEvolutionReader::new(
            self.table.file_io.clone(),
            self.table.schema_manager().clone(),
            self.table.schema().id(),
            self.table.schema.fields().to_vec(),
            self.read_type().to_vec(),
            self.data_predicates.clone(),
            core_options.blob_as_descriptor(),
            core_options.blob_descriptor_fields(),
        )?;
        reader.read(data_splits)
    }

    /// Read raw data files without dedup or evolution.
    fn read_raw(&self, data_splits: &[DataSplit]) -> crate::Result<ArrowRecordBatchStream> {
        self.new_data_file_reader().read(data_splits)
    }

    fn new_data_file_reader(&self) -> DataFileReader {
        DataFileReader::new(
            self.table.file_io.clone(),
            self.table.schema_manager().clone(),
            self.table.schema().id(),
            self.table.schema.fields().to_vec(),
            self.read_type().to_vec(),
            self.data_predicates.clone(),
        )
    }
}

/// Whether a primary-key split must go through the sort-merge reader.
///
/// Mirrors Java `PrimaryKeyTableRawFileSplitReadProvider#match`: a raw read
/// needs the split marked raw convertible AND a known `delete_row_count` on
/// every file. Legacy files without the stat may hide delete rows — scan
/// planning treats the missing stat as "no deletes" for compatibility, so the
/// read side must fall back to the merge reader, which drops them.
///
/// Deletion-vector tables keep the plain level-0 dispatch: stale versions are
/// masked by DVs and KeyValueFileReader does not support DVs.
fn pk_split_needs_merge(split: &DataSplit, dv_enabled: bool) -> bool {
    if dv_enabled {
        return split.data_files().iter().any(|f| f.level == 0);
    }
    !split.raw_convertible()
        || split
            .data_files()
            .iter()
            .any(|f| f.delete_row_count.is_none())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{BinaryRow, DataFileMeta};
    use crate::table::query_auth_table;
    use crate::table::source::DataSplitBuilder;

    fn file(name: &str, level: i32, delete_row_count: Option<i64>) -> DataFileMeta {
        DataFileMeta {
            file_name: name.to_string(),
            file_size: 128,
            row_count: 10,
            min_key: Vec::new(),
            max_key: Vec::new(),
            key_stats: BinaryTableStats::new(Vec::new(), Vec::new(), Vec::new()),
            value_stats: BinaryTableStats::new(Vec::new(), Vec::new(), Vec::new()),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id: 0,
            level,
            extra_files: Vec::new(),
            creation_time: None,
            delete_row_count,
            embedded_index: None,
            first_row_id: None,
            write_cols: None,
            external_path: None,
            file_source: None,
            value_stats_cols: None,
        }
    }

    fn split(files: Vec<DataFileMeta>, raw_convertible: bool) -> DataSplit {
        DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("file:/tmp/bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(files)
            .with_raw_convertible(raw_convertible)
            .build()
            .unwrap()
    }

    #[test]
    fn test_pk_split_needs_merge_routing() {
        // Raw convertible with known delete counts: raw read.
        let raw = split(vec![file("a", 5, Some(0))], true);
        assert!(!pk_split_needs_merge(&raw, false));

        // Not raw convertible: merge read.
        let merge = split(vec![file("a", 5, Some(0))], false);
        assert!(pk_split_needs_merge(&merge, false));

        // Raw convertible but a legacy file lacks delete_row_count: the file
        // may hide delete rows, so it must go through the merge reader.
        let legacy = split(vec![file("a", 5, None)], true);
        assert!(pk_split_needs_merge(&legacy, false));

        // Deletion-vector tables dispatch on level 0 only.
        let dv_l0 = split(vec![file("a", 0, None)], false);
        assert!(pk_split_needs_merge(&dv_l0, true));
        let dv_compacted = split(vec![file("a", 5, None)], false);
        assert!(!pk_split_needs_merge(&dv_compacted, true));
    }

    #[test]
    fn test_direct_table_read_fails_closed_when_query_auth_enabled() {
        let table = query_auth_table();
        // Bypass `ReadBuilder` by constructing `TableRead` directly; the `to_arrow` guard
        // still fails closed.
        let read = TableRead::new(&table, table.schema.fields().to_vec(), Vec::new());
        assert!(
            matches!(
                read.to_arrow(&[]),
                Err(crate::Error::Unsupported { ref message }) if message.contains("query-auth.enabled")
            ),
            "directly-constructed read of a query-auth.enabled table must fail closed"
        );
    }
}
