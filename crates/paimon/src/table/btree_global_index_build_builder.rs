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

use crate::btree::{make_key_comparator, serialize_datum, BTreeIndexWriter, BlockCompressionType};
use crate::spec::{
    bucket_dir_name, extract_datum_from_arrow, BinaryRow, CoreOptions, DataField, DataFileMeta,
    DataType, FileKind, GlobalIndexMeta, IndexFileMeta, IndexManifest, ROW_ID_FIELD_NAME,
};
use crate::table::source::is_data_evolution_normal_file;
use crate::table::stats_filter::group_by_overlapping_row_id;
use crate::table::{
    CommitMessage, DataSplit, DataSplitBuilder, RowRange, SnapshotManager, Table, TableCommit,
};
use crate::{Error, Result};
use arrow_array::{Array, Int64Array, RecordBatch};
use futures::TryStreamExt;
use std::cmp::Ordering;
use std::collections::HashMap;

const BTREE_INDEX_TYPE: &str = "btree";
const INDEX_DIR: &str = "index";
const BTREE_BLOCK_SIZE: usize = 4 * 1024;

type BTreeKeyRow = (Option<Vec<u8>>, i64);

pub struct BTreeGlobalIndexBuildBuilder<'a> {
    table: &'a Table,
    index_column: Option<String>,
}

impl<'a> BTreeGlobalIndexBuildBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            index_column: None,
        }
    }

    pub fn with_index_column(&mut self, column: &str) -> &mut Self {
        self.index_column = Some(column.to_string());
        self
    }

    pub async fn execute(&self) -> Result<usize> {
        let index_column = self
            .index_column
            .as_deref()
            .ok_or_else(|| Error::DataInvalid {
                message: "BTree global index column is required".to_string(),
                source: None,
            })?;

        let core_options = CoreOptions::new(self.table.schema().options());
        validate_table_options(self.table, &core_options)?;
        let records_per_range = core_options.sorted_index_records_per_range()?;

        let index_field = find_index_field(self.table, index_column)?;
        validate_btree_field(index_field)?;

        let snapshot_manager = SnapshotManager::new(
            self.table.file_io().clone(),
            self.table.location().to_string(),
        );
        let snapshot = snapshot_manager
            .get_latest_snapshot()
            .await?
            .ok_or_else(|| Error::DataInvalid {
                message: "Cannot build BTree global index without a snapshot".to_string(),
                source: None,
            })?;

        let manifest_entries = self
            .table
            .new_read_builder()
            .new_scan()
            .with_scan_all_files()
            .plan_manifest_entries(&snapshot)
            .await?;
        let shards = plan_btree_shards(
            self.table.location(),
            self.table.schema().partition_keys(),
            self.table.schema().fields(),
            &core_options,
            snapshot.id(),
            manifest_entries,
            records_per_range,
        )?;
        if shards.is_empty() {
            return Ok(0);
        }

        validate_existing_index_overlap(
            self.table,
            snapshot.index_manifest(),
            index_field.id(),
            &shards,
        )
        .await?;

        let shard_count = shards.len();
        let mut messages = Vec::with_capacity(shard_count);
        for shard in shards {
            let index_file = self
                .build_index_file(&shard, index_field, index_column)
                .await?;
            let mut message =
                CommitMessage::new(shard.partition_bytes.clone(), shard.source_bucket, vec![]);
            message.new_index_files = vec![index_file];
            messages.push(message);
        }

        TableCommit::new(
            self.table.clone(),
            format!(
                "global-index-{}-create-{}",
                BTREE_INDEX_TYPE,
                uuid::Uuid::new_v4()
            ),
        )
        .commit_if_latest_snapshot(messages, snapshot.id())
        .await?;

        Ok(shard_count)
    }

    async fn build_index_file(
        &self,
        shard: &BTreeGlobalIndexShard,
        index_field: &DataField,
        index_column: &str,
    ) -> Result<IndexFileMeta> {
        let row_count = checked_row_count(shard.row_range_start, shard.row_range_end)?;
        let mut rows = extract_index_rows(self.table, shard, index_column, index_field).await?;
        let cmp = make_key_comparator(index_field.data_type());
        sort_index_rows(&mut rows, &cmp);

        self.table
            .file_io()
            .mkdirs(&format!(
                "{}/{INDEX_DIR}/",
                self.table.location().trim_end_matches('/')
            ))
            .await?;
        let file_name = format!("btree-global-index-{}.index", uuid::Uuid::new_v4());
        let index_path = format!(
            "{}/{INDEX_DIR}/{}",
            self.table.location().trim_end_matches('/'),
            file_name
        );
        let output = self.table.file_io().new_output(&index_path)?;
        let writer = output.writer().await?;
        let mut writer = BTreeIndexWriter::with_comparator(
            writer,
            BTREE_BLOCK_SIZE,
            BlockCompressionType::None,
            cmp,
        );
        for (key, local_row_id) in &rows {
            writer
                .write(key.as_deref(), *local_row_id)
                .await
                .map_err(|e| Error::DataInvalid {
                    message: format!("Failed to write BTree global index file '{file_name}'"),
                    source: Some(Box::new(e)),
                })?;
        }
        let write_result = writer.finish().await.map_err(|e| Error::DataInvalid {
            message: format!("Failed to finish BTree global index file '{file_name}'"),
            source: Some(Box::new(e)),
        })?;

        if write_result.row_count != u64::try_from(row_count).unwrap() {
            return Err(Error::DataInvalid {
                message: format!(
                    "BTree global index expected {} rows, wrote {}",
                    row_count, write_result.row_count
                ),
                source: None,
            });
        }

        let status = self.table.file_io().get_status(&index_path).await?;
        Ok(IndexFileMeta {
            index_type: BTREE_INDEX_TYPE.to_string(),
            file_name,
            file_size: checked_i32(
                status.size,
                "Index file is too large for Rust IndexFileMeta",
            )?,
            row_count,
            deletion_vectors_ranges: None,
            global_index_meta: Some(GlobalIndexMeta {
                row_range_start: shard.row_range_start,
                row_range_end: shard.row_range_end,
                index_field_id: index_field.id(),
                extra_field_ids: None,
                index_meta: Some(write_result.meta.serialize()),
            }),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BTreeGlobalIndexShard {
    pub partition: BinaryRow,
    pub partition_bytes: Vec<u8>,
    pub files: Vec<DataFileMeta>,
    pub row_range_start: i64,
    pub row_range_end: i64,
    snapshot_id: i64,
    source_bucket: i32,
    total_buckets: i32,
    bucket_path: String,
}

fn validate_table_options(table: &Table, core_options: &CoreOptions) -> Result<()> {
    if !core_options.row_tracking_enabled() {
        return Err(Error::DataInvalid {
            message: "BTree global index build requires 'row-tracking.enabled' = 'true'"
                .to_string(),
            source: None,
        });
    }
    if !core_options.data_evolution_enabled() {
        return Err(Error::DataInvalid {
            message: "BTree global index build requires 'data-evolution.enabled' = 'true'"
                .to_string(),
            source: None,
        });
    }
    if !core_options.global_index_enabled() {
        return Err(Error::DataInvalid {
            message: "BTree global index build requires 'global-index.enabled' = 'true'"
                .to_string(),
            source: None,
        });
    }
    if !table.schema().primary_keys().is_empty() {
        return Err(Error::Unsupported {
            message: "BTree global index build does not support primary-key tables".to_string(),
        });
    }
    if core_options.deletion_vectors_enabled() {
        return Err(Error::Unsupported {
            message:
                "BTree global index build does not support tables with deletion-vectors.enabled=true"
                    .to_string(),
        });
    }
    Ok(())
}

fn find_index_field<'a>(table: &'a Table, column: &str) -> Result<&'a DataField> {
    table
        .schema()
        .fields()
        .iter()
        .find(|field| field.name() == column)
        .ok_or_else(|| Error::ColumnNotExist {
            full_name: table.identifier().full_name(),
            column: column.to_string(),
        })
}

fn validate_btree_field(field: &DataField) -> Result<()> {
    if !is_btree_supported_data_type(field.data_type()) {
        return Err(Error::Unsupported {
            message: format!(
                "BTree global index only supports scalar columns, got {:?} for column '{}'",
                field.data_type(),
                field.name()
            ),
        });
    }
    Ok(())
}

fn is_btree_supported_data_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean(_)
            | DataType::TinyInt(_)
            | DataType::SmallInt(_)
            | DataType::Int(_)
            | DataType::BigInt(_)
            | DataType::Decimal(_)
            | DataType::Double(_)
            | DataType::Float(_)
            | DataType::Char(_)
            | DataType::VarChar(_)
            | DataType::Date(_)
            | DataType::LocalZonedTimestamp(_)
            | DataType::Time(_)
            | DataType::Timestamp(_)
    )
}

fn plan_btree_shards(
    table_location: &str,
    partition_keys: &[String],
    schema_fields: &[DataField],
    core_options: &CoreOptions,
    snapshot_id: i64,
    entries: Vec<crate::spec::ManifestEntry>,
    records_per_range: i64,
) -> Result<Vec<BTreeGlobalIndexShard>> {
    if records_per_range <= 0 {
        return Err(Error::DataInvalid {
            message: format!(
                "Option 'sorted-index.records-per-range' must be greater than 0, got: {records_per_range}"
            ),
            source: None,
        });
    }

    let mut by_partition_bucket: HashMap<(Vec<u8>, i32, i32), Vec<DataFileMeta>> = HashMap::new();
    for entry in entries {
        if *entry.kind() != FileKind::Add {
            continue;
        }
        if entry.file().first_row_id.is_none() {
            return Err(Error::DataInvalid {
                message: format!(
                    "Data file '{}' is missing first_row_id; cannot build a complete BTree global index",
                    entry.file().file_name
                ),
                source: None,
            });
        }
        let (partition, bucket, total_buckets, file) = entry.into_parts();
        by_partition_bucket
            .entry((partition, bucket, total_buckets))
            .or_default()
            .push(file);
    }

    let mut result = Vec::new();
    for ((partition_bytes, source_bucket, total_buckets), files) in by_partition_bucket {
        let partition = if partition_keys.is_empty() {
            BinaryRow::new(0)
        } else {
            BinaryRow::from_serialized_bytes(&partition_bytes)?
        };
        let bucket_path = bucket_path(
            table_location,
            partition_keys,
            schema_fields,
            core_options,
            &partition,
            source_bucket,
        )?;
        let normal_groups = group_normal_file_ranges(files)?;
        for group in normal_groups {
            let (coverage_start, coverage_end) = normal_coverage_range(&group.files)?;
            let start_range = coverage_start / records_per_range;
            let end_range = coverage_end / records_per_range;
            for range_id in start_range..=end_range {
                let range_start = range_id * records_per_range;
                let range_end = range_start + records_per_range - 1;
                let row_range_start = coverage_start.max(range_start);
                let row_range_end = coverage_end.min(range_end);
                result.push(BTreeGlobalIndexShard {
                    partition: partition.clone(),
                    partition_bytes: partition_bytes.clone(),
                    files: group.files.clone(),
                    row_range_start,
                    row_range_end,
                    snapshot_id,
                    source_bucket,
                    total_buckets,
                    bucket_path: bucket_path.clone(),
                });
            }
        }
    }
    result.sort_by(|a, b| {
        a.partition
            .to_serialized_bytes()
            .cmp(&b.partition.to_serialized_bytes())
            .then(a.source_bucket.cmp(&b.source_bucket))
            .then(a.row_range_start.cmp(&b.row_range_start))
    });
    Ok(result)
}

#[derive(Debug)]
struct PlannedFileGroup {
    files: Vec<DataFileMeta>,
}

fn group_normal_file_ranges(files: Vec<DataFileMeta>) -> Result<Vec<PlannedFileGroup>> {
    if files.is_empty() {
        return Ok(Vec::new());
    }
    for file in &files {
        file.row_id_range().ok_or_else(|| Error::DataInvalid {
            message: format!(
                "Data file '{}' is missing first_row_id; cannot build a complete BTree global index",
                file.file_name
            ),
            source: None,
        })?;
    }

    let mut normal_ranges = files
        .iter()
        .filter(|file| is_data_evolution_normal_file(file))
        .filter_map(DataFileMeta::row_id_range)
        .collect::<Vec<_>>();
    normal_ranges.sort_by_key(|(start, _)| *start);

    let mut coverage_ranges: Vec<(i64, i64)> = Vec::new();
    for (file_start, file_end) in normal_ranges {
        match coverage_ranges.last_mut() {
            Some((_, end)) if file_start <= *end + 1 => {
                *end = (*end).max(file_end);
            }
            _ => coverage_ranges.push((file_start, file_end)),
        }
    }

    coverage_ranges
        .into_iter()
        .map(|(start, end)| {
            let mut group_files = files
                .iter()
                .filter(|file| {
                    file.row_id_range().is_some_and(|(file_start, file_end)| {
                        ranges_overlap(start, end, file_start, file_end)
                    })
                })
                .cloned()
                .collect::<Vec<_>>();
            group_files.sort_by_key(|file| {
                (
                    file.first_row_id.unwrap_or(i64::MAX),
                    !is_data_evolution_normal_file(file),
                    file.file_name.clone(),
                )
            });
            Ok(PlannedFileGroup { files: group_files })
        })
        .collect()
}

fn normal_coverage_range(files: &[DataFileMeta]) -> Result<(i64, i64)> {
    let mut start = None;
    let mut end = None;
    for file in files
        .iter()
        .filter(|file| is_data_evolution_normal_file(file))
    {
        let (file_start, file_end) = file.row_id_range().ok_or_else(|| Error::DataInvalid {
            message: format!(
                "Data file '{}' is missing first_row_id; cannot build a complete BTree global index",
                file.file_name
            ),
            source: None,
        })?;
        start = Some(start.map_or(file_start, |value: i64| value.min(file_start)));
        end = Some(end.map_or(file_end, |value: i64| value.max(file_end)));
    }
    start.zip(end).ok_or_else(|| Error::DataInvalid {
        message: "BTree global index shard has no normal data files".to_string(),
        source: None,
    })
}

fn bucket_path(
    table_location: &str,
    partition_keys: &[String],
    schema_fields: &[DataField],
    core_options: &CoreOptions,
    partition: &BinaryRow,
    bucket: i32,
) -> Result<String> {
    let base = table_location.trim_end_matches('/');
    if partition_keys.is_empty() {
        return Ok(format!("{base}/{}", bucket_dir_name(bucket)));
    }
    let computer = crate::spec::PartitionComputer::new(
        partition_keys,
        schema_fields,
        core_options.partition_default_name(),
        core_options.legacy_partition_name(),
    )?;
    Ok(format!(
        "{base}/{}{}",
        computer.generate_partition_path(partition)?,
        bucket_dir_name(bucket)
    ))
}

async fn validate_existing_index_overlap(
    table: &Table,
    index_manifest_name: Option<&str>,
    index_field_id: i32,
    shards: &[BTreeGlobalIndexShard],
) -> Result<()> {
    let Some(index_manifest_name) = index_manifest_name else {
        return Ok(());
    };
    let path = format!(
        "{}/manifest/{}",
        table.location().trim_end_matches('/'),
        index_manifest_name
    );
    let entries = IndexManifest::read(table.file_io(), &path).await?;
    for entry in entries {
        if entry.kind != FileKind::Add {
            continue;
        }
        let Some(meta) = entry.index_file.global_index_meta else {
            continue;
        };
        if meta.index_field_id != index_field_id {
            continue;
        }
        if shards.iter().any(|shard| {
            ranges_overlap(
                meta.row_range_start,
                meta.row_range_end,
                shard.row_range_start,
                shard.row_range_end,
            )
        }) {
            return Err(Error::DataInvalid {
                message: format!(
                    "Existing global index file '{}' overlaps requested row range for field {}",
                    entry.index_file.file_name, index_field_id
                ),
                source: None,
            });
        }
    }
    Ok(())
}

async fn extract_index_rows(
    table: &Table,
    shard: &BTreeGlobalIndexShard,
    index_column: &str,
    index_field: &DataField,
) -> Result<Vec<BTreeKeyRow>> {
    let splits = build_read_splits_for_shard(shard)?;

    let mut read_builder = table.new_read_builder();
    read_builder.with_projection(&[index_column, ROW_ID_FIELD_NAME])?;
    let read = read_builder.new_read()?;
    let batches = read.to_arrow(&splits)?.try_collect::<Vec<_>>().await?;
    extract_index_rows_from_batches(
        &batches,
        index_column,
        index_field.data_type(),
        shard.row_range_start,
        i64::from(checked_row_count(
            shard.row_range_start,
            shard.row_range_end,
        )?),
    )
}

fn build_read_splits_for_shard(shard: &BTreeGlobalIndexShard) -> Result<Vec<DataSplit>> {
    let shard_range = RowRange::new(shard.row_range_start, shard.row_range_end);
    group_by_overlapping_row_id(shard.files.clone())
        .into_iter()
        .filter_map(|files| {
            let ranges = files
                .iter()
                .filter_map(|file| {
                    file.row_id_range()
                        .and_then(|(start, end)| shard_range.intersect_inclusive(start, end))
                })
                .collect::<Vec<_>>();
            let ranges = crate::table::merge_row_ranges(ranges);
            if ranges.is_empty() {
                return None;
            }
            let raw_convertible = files.len() == 1;
            Some(
                DataSplitBuilder::new()
                    .with_snapshot(shard.snapshot_id)
                    .with_partition(shard.partition.clone())
                    .with_bucket(shard.source_bucket)
                    .with_bucket_path(shard.bucket_path.clone())
                    .with_total_buckets(shard.total_buckets)
                    .with_data_files(files)
                    .with_row_ranges(ranges)
                    .with_raw_convertible(raw_convertible)
                    .build(),
            )
        })
        .collect()
}

fn extract_index_rows_from_batches(
    batches: &[RecordBatch],
    index_column: &str,
    data_type: &DataType,
    row_range_start: i64,
    expected_row_count: i64,
) -> Result<Vec<BTreeKeyRow>> {
    let row_count = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    let mut rows = Vec::with_capacity(row_count);
    let mut expected_row_id = row_range_start;
    for batch in batches {
        let value_index =
            batch
                .schema()
                .index_of(index_column)
                .map_err(|e| Error::DataInvalid {
                    message: format!("Index column '{index_column}' not found in read batch: {e}"),
                    source: None,
                })?;
        let row_id_index =
            batch
                .schema()
                .index_of(ROW_ID_FIELD_NAME)
                .map_err(|e| Error::DataInvalid {
                    message: format!("_ROW_ID column not found in read batch: {e}"),
                    source: None,
                })?;
        let row_ids = batch
            .column(row_id_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| Error::DataInvalid {
                message: "BTree global index build requires non-null Int64 _ROW_ID".to_string(),
                source: None,
            })?;

        for row in 0..batch.num_rows() {
            if row_ids.is_null(row) {
                return Err(Error::DataInvalid {
                    message: "BTree global index build found null _ROW_ID".to_string(),
                    source: None,
                });
            }
            let row_id = row_ids.value(row);
            if row_id != expected_row_id {
                return Err(Error::DataInvalid {
                    message: format!(
                        "BTree global index build expected _ROW_ID {}, got {}",
                        expected_row_id, row_id
                    ),
                    source: None,
                });
            }
            expected_row_id += 1;

            let key = extract_datum_from_arrow(batch, row, value_index, data_type)?
                .map(|datum| serialize_datum(&datum, data_type));
            rows.push((key, row_id - row_range_start));
        }
    }
    let actual_row_count = expected_row_id - row_range_start;
    if actual_row_count != expected_row_count {
        return Err(Error::DataInvalid {
            message: format!(
                "BTree global index build expected {} rows, got {}",
                expected_row_count, actual_row_count
            ),
            source: None,
        });
    }
    Ok(rows)
}

fn sort_index_rows(rows: &mut [BTreeKeyRow], cmp: &dyn Fn(&[u8], &[u8]) -> Ordering) {
    rows.sort_by(|left, right| match (&left.0, &right.0) {
        (None, None) => left.1.cmp(&right.1),
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(left_key), Some(right_key)) => {
            cmp(left_key, right_key).then_with(|| left.1.cmp(&right.1))
        }
    });
}

fn checked_i32(value: u64, context: &str) -> Result<i32> {
    i32::try_from(value).map_err(|_| Error::DataInvalid {
        message: format!("{context}: {value}"),
        source: None,
    })
}

fn checked_row_count(row_range_start: i64, row_range_end: i64) -> Result<i32> {
    if row_range_end < row_range_start {
        return Err(Error::DataInvalid {
            message: format!(
                "Invalid BTree global index row range [{row_range_start}, {row_range_end}]"
            ),
            source: None,
        });
    }
    i32::try_from(row_range_end - row_range_start + 1).map_err(|_| Error::DataInvalid {
        message: format!(
            "BTree global index row count is too large for Rust IndexFileMeta: [{row_range_start}, {row_range_end}]"
        ),
        source: None,
    })
}

fn ranges_overlap(left_start: i64, left_end: i64, right_start: i64, right_end: i64) -> bool {
    left_start <= right_end && right_start <= left_end
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Identifier;
    use crate::io::FileIOBuilder;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{
        BinaryType, GlobalIndexSearchMode, IntType, ManifestEntry, PredicateBuilder, Schema,
        TableSchema, VarBinaryType, VarCharType,
    };
    use crate::table::global_index_scanner::{evaluate_global_index, GlobalIndexEvaluation};
    use crate::table::{SnapshotManager, TableCommit, TableWrite};
    use arrow_array::{ArrayRef, Int32Array, Int64Array, StringArray};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use chrono::{DateTime, Utc};
    use std::sync::Arc;

    fn data_file(name: &str, first_row_id: Option<i64>, row_count: i64) -> DataFileMeta {
        DataFileMeta {
            file_name: name.to_string(),
            file_size: 128,
            row_count,
            min_key: vec![],
            max_key: vec![],
            key_stats: BinaryTableStats::new(vec![], vec![], vec![]),
            value_stats: BinaryTableStats::new(vec![], vec![], vec![]),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id: 0,
            level: 0,
            extra_files: vec![],
            creation_time: Some(
                "2024-09-06T07:45:55.039+00:00"
                    .parse::<DateTime<Utc>>()
                    .unwrap(),
            ),
            delete_row_count: None,
            embedded_index: None,
            first_row_id,
            write_cols: None,
            external_path: None,
            file_source: None,
            value_stats_cols: None,
        }
    }

    fn partial_file(name: &str, first_row_id: Option<i64>, row_count: i64) -> DataFileMeta {
        let mut file = data_file(name, first_row_id, row_count);
        file.write_cols = Some(vec!["name".to_string()]);
        file
    }

    fn manifest_entry(file: DataFileMeta) -> ManifestEntry {
        manifest_entry_with_bucket(file, 0, 1)
    }

    fn manifest_entry_with_bucket(
        file: DataFileMeta,
        bucket: i32,
        total_buckets: i32,
    ) -> ManifestEntry {
        ManifestEntry::new(FileKind::Add, vec![], bucket, total_buckets, file, 2)
    }

    fn table_options(records_per_range: &str) -> HashMap<String, String> {
        HashMap::from([
            ("row-tracking.enabled".to_string(), "true".to_string()),
            ("data-evolution.enabled".to_string(), "true".to_string()),
            ("global-index.enabled".to_string(), "true".to_string()),
            (
                "sorted-index.records-per-range".to_string(),
                records_per_range.to_string(),
            ),
        ])
    }

    fn test_table(options: HashMap<String, String>) -> Table {
        test_table_with_path("memory:/test_btree_global_index_builder", options)
    }

    fn test_table_with_path(table_path: &str, options: HashMap<String, String>) -> Table {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("name", DataType::VarChar(VarCharType::string_type()))
            .options(options)
            .build()
            .unwrap();
        Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("default", "test_table"),
            table_path.to_string(),
            TableSchema::new(0, &schema),
            None,
        )
    }

    fn plan(
        entries: Vec<ManifestEntry>,
        records_per_range: i64,
    ) -> Result<Vec<BTreeGlobalIndexShard>> {
        let table = test_table(table_options(&records_per_range.to_string()));
        let core = CoreOptions::new(table.schema().options());
        plan_btree_shards(
            table.location(),
            table.schema().partition_keys(),
            table.schema().fields(),
            &core,
            1,
            entries,
            records_per_range,
        )
    }

    #[test]
    fn test_planner_splits_single_file_across_ranges() {
        let shards = plan(vec![manifest_entry(data_file("a", Some(0), 25))], 10).unwrap();

        assert_eq!(
            shards
                .iter()
                .map(|s| (s.row_range_start, s.row_range_end))
                .collect::<Vec<_>>(),
            vec![(0, 9), (10, 19), (20, 24)]
        );
    }

    #[test]
    fn test_planner_merges_contiguous_normal_files() {
        let shards = plan(
            vec![
                manifest_entry(data_file("a", Some(0), 5)),
                manifest_entry(data_file("b", Some(5), 5)),
            ],
            20,
        )
        .unwrap();

        assert_eq!(shards.len(), 1);
        assert_eq!((shards[0].row_range_start, shards[0].row_range_end), (0, 9));
    }

    #[test]
    fn test_planner_splits_row_id_gap_into_separate_shards() {
        let shards = plan(
            vec![
                manifest_entry(data_file("a", Some(0), 5)),
                manifest_entry(data_file("b", Some(10), 5)),
            ],
            20,
        )
        .unwrap();

        assert_eq!(
            shards
                .iter()
                .map(|s| (s.row_range_start, s.row_range_end))
                .collect::<Vec<_>>(),
            vec![(0, 4), (10, 14)]
        );
    }

    #[test]
    fn test_planner_rejects_missing_first_row_id() {
        let err = plan(vec![manifest_entry(data_file("a", None, 5))], 10)
            .expect_err("missing first_row_id should fail");
        assert!(
            matches!(err, Error::DataInvalid { message, .. } if message.contains("missing first_row_id"))
        );
    }

    #[test]
    fn test_planner_keeps_buckets_separate() {
        let shards = plan(
            vec![
                manifest_entry_with_bucket(data_file("a", Some(0), 5), 0, 2),
                manifest_entry_with_bucket(data_file("b", Some(5), 5), 1, 2),
            ],
            20,
        )
        .unwrap();

        assert_eq!(
            shards
                .iter()
                .map(|s| (
                    s.source_bucket,
                    s.total_buckets,
                    s.row_range_start,
                    s.row_range_end
                ))
                .collect::<Vec<_>>(),
            vec![(0, 2, 0, 4), (1, 2, 5, 9)]
        );
    }

    #[test]
    fn test_planner_keeps_partial_file_in_read_group_without_expanding_coverage() {
        let shards = plan(
            vec![
                manifest_entry(data_file("base", Some(0), 5)),
                manifest_entry(partial_file("partial", Some(0), 5)),
            ],
            20,
        )
        .unwrap();

        assert_eq!(shards.len(), 1);
        assert_eq!((shards[0].row_range_start, shards[0].row_range_end), (0, 4));
        assert_eq!(shards[0].files.len(), 2);
    }

    #[test]
    fn test_build_read_splits_groups_only_overlapping_partial_files() {
        let shards = plan(
            vec![
                manifest_entry(data_file("a", Some(0), 5)),
                manifest_entry(data_file("b", Some(5), 5)),
                manifest_entry(partial_file("partial", Some(0), 5)),
            ],
            20,
        )
        .unwrap();
        assert_eq!(shards.len(), 1);

        let splits = build_read_splits_for_shard(&shards[0]).unwrap();

        assert_eq!(splits.len(), 2);
        assert_eq!(
            splits[0]
                .data_files()
                .iter()
                .map(|file| file.file_name.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "partial"]
        );
        assert_eq!(splits[0].row_ranges(), Some(&[RowRange::new(0, 4)][..]));
        assert!(!splits[0].raw_convertible());

        assert_eq!(
            splits[1]
                .data_files()
                .iter()
                .map(|file| file.file_name.as_str())
                .collect::<Vec<_>>(),
            vec!["b"]
        );
        assert_eq!(splits[1].row_ranges(), Some(&[RowRange::new(5, 9)][..]));
        assert!(splits[1].raw_convertible());
    }

    #[test]
    fn test_validate_btree_field_rejects_complex_type() {
        let field = DataField::new(
            0,
            "items".to_string(),
            DataType::Array(crate::spec::ArrayType::new(DataType::Int(IntType::new()))),
        );
        let err = validate_btree_field(&field).expect_err("array should be rejected");
        assert!(matches!(err, Error::Unsupported { message } if message.contains("scalar")));
    }

    #[test]
    fn test_validate_btree_field_rejects_binary_types() {
        for data_type in [
            DataType::Binary(BinaryType::new(4).unwrap()),
            DataType::VarBinary(VarBinaryType::try_new(true, 4).unwrap()),
        ] {
            let field = DataField::new(0, "bytes".to_string(), data_type);
            let err = validate_btree_field(&field).expect_err("binary should be rejected");
            assert!(matches!(err, Error::Unsupported { message } if message.contains("scalar")));
        }
    }

    fn index_batch(values: Vec<Option<i32>>, row_ids: Vec<Option<i64>>) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, true),
            ArrowField::new(ROW_ID_FIELD_NAME, ArrowDataType::Int64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(values)) as ArrayRef,
                Arc::new(Int64Array::from(row_ids)) as ArrayRef,
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_extract_index_rows_serializes_keys_and_local_row_ids() {
        let batch = index_batch(
            vec![Some(10), None, Some(30)],
            vec![Some(5), Some(6), Some(7)],
        );
        let rows =
            extract_index_rows_from_batches(&[batch], "id", &DataType::Int(IntType::new()), 5, 3)
                .unwrap();

        assert_eq!(
            rows,
            vec![
                (Some(10i32.to_le_bytes().to_vec()), 0),
                (None, 1),
                (Some(30i32.to_le_bytes().to_vec()), 2),
            ]
        );
    }

    #[test]
    fn test_extract_index_rows_rejects_row_id_gap() {
        let batch = index_batch(vec![Some(10), Some(30)], vec![Some(5), Some(7)]);
        let err =
            extract_index_rows_from_batches(&[batch], "id", &DataType::Int(IntType::new()), 5, 2)
                .expect_err("row-id gap should fail");

        assert!(
            matches!(err, Error::DataInvalid { message, .. } if message.contains("expected _ROW_ID"))
        );
    }

    #[test]
    fn test_sort_index_rows_orders_nulls_then_keys() {
        let mut rows = vec![
            (Some(3i32.to_le_bytes().to_vec()), 0),
            (None, 1),
            (Some(1i32.to_le_bytes().to_vec()), 2),
            (Some(1i32.to_le_bytes().to_vec()), 3),
        ];
        let cmp = make_key_comparator(&DataType::Int(IntType::new()));

        sort_index_rows(&mut rows, &cmp);

        assert_eq!(
            rows,
            vec![
                (None, 1),
                (Some(1i32.to_le_bytes().to_vec()), 2),
                (Some(1i32.to_le_bytes().to_vec()), 3),
                (Some(3i32.to_le_bytes().to_vec()), 0),
            ]
        );
    }

    #[test]
    fn test_extract_index_rows_accepts_string_column() {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("name", ArrowDataType::Utf8, true),
            ArrowField::new(ROW_ID_FIELD_NAME, ArrowDataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("alice"), None])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(10), Some(11)])) as ArrayRef,
            ],
        )
        .unwrap();

        let rows = extract_index_rows_from_batches(
            &[batch],
            "name",
            &DataType::VarChar(VarCharType::string_type()),
            10,
            2,
        )
        .unwrap();

        assert_eq!(rows, vec![(Some(b"alice".to_vec()), 0), (None, 1)]);
    }

    fn data_batch(ids: Vec<i32>, names: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("name", ArrowDataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(names)) as ArrayRef,
            ],
        )
        .unwrap()
    }

    async fn setup_dirs(table: &Table) {
        table
            .file_io()
            .mkdirs(&format!("{}/snapshot/", table.location()))
            .await
            .unwrap();
        table
            .file_io()
            .mkdirs(&format!("{}/manifest/", table.location()))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_execute_writes_btree_index_manifest_and_file() {
        let table_path = "memory:/test_btree_global_index_builder_e2e";
        let table = test_table_with_path(table_path, table_options("10"));
        setup_dirs(&table).await;

        let mut table_write = TableWrite::new(&table, "test-user".to_string()).unwrap();
        table_write
            .write_arrow_batch(&data_batch(vec![1, 2, 3], vec!["alice", "bob", "alice"]))
            .await
            .unwrap();
        let messages = table_write.prepare_commit().await.unwrap();
        TableCommit::new(table.clone(), "test-user".to_string())
            .commit(messages)
            .await
            .unwrap();

        let shard_count = table
            .new_btree_global_index_build_builder()
            .with_index_column("name")
            .execute()
            .await
            .unwrap();
        assert_eq!(shard_count, 1);

        let snapshot_manager =
            SnapshotManager::new(table.file_io().clone(), table.location().to_string());
        let snapshot = snapshot_manager
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        let index_manifest = snapshot.index_manifest().expect("index manifest");
        let index_entries = IndexManifest::read(
            table.file_io(),
            &format!("{table_path}/manifest/{index_manifest}"),
        )
        .await
        .unwrap();
        assert_eq!(index_entries.len(), 1);

        let index_file = &index_entries[0].index_file;
        assert_eq!(index_file.index_type, BTREE_INDEX_TYPE);
        assert!(index_file.file_name.starts_with("btree-global-index-"));
        assert_eq!(index_file.row_count, 3);
        assert!(index_file.file_size > 0);

        let global_meta = index_file
            .global_index_meta
            .as_ref()
            .expect("global index meta");
        assert_eq!(global_meta.row_range_start, 0);
        assert_eq!(global_meta.row_range_end, 2);
        assert_eq!(global_meta.index_field_id, 1);
        let btree_meta =
            crate::btree::BTreeIndexMeta::deserialize(global_meta.index_meta.as_ref().unwrap())
                .unwrap();
        assert_eq!(btree_meta.first_key, Some(b"alice".to_vec()));
        assert_eq!(btree_meta.last_key, Some(b"bob".to_vec()));
        assert!(!btree_meta.has_nulls);

        let predicate = PredicateBuilder::new(table.schema().fields())
            .equal("name", crate::spec::Datum::String("alice".to_string()))
            .unwrap();
        let row_ranges = evaluate_global_index(GlobalIndexEvaluation {
            file_io: table.file_io(),
            table_path: table.location(),
            index_entries: &index_entries,
            predicates: &[predicate],
            schema_fields: table.schema().fields(),
            search_mode: GlobalIndexSearchMode::Fast,
            next_row_id: snapshot.next_row_id(),
            data_ranges: &[],
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(row_ranges, vec![RowRange::new(0, 0), RowRange::new(2, 2)]);
    }

    #[tokio::test]
    async fn test_execute_rejects_existing_overlap_before_commit() {
        let table_path = "memory:/test_btree_global_index_builder_overlap";
        let table = test_table_with_path(table_path, table_options("10"));
        setup_dirs(&table).await;

        let mut table_write = TableWrite::new(&table, "test-user".to_string()).unwrap();
        table_write
            .write_arrow_batch(&data_batch(vec![1, 2], vec!["alice", "bob"]))
            .await
            .unwrap();
        let messages = table_write.prepare_commit().await.unwrap();
        TableCommit::new(table.clone(), "test-user".to_string())
            .commit(messages)
            .await
            .unwrap();

        table
            .new_btree_global_index_build_builder()
            .with_index_column("name")
            .execute()
            .await
            .unwrap();

        let err = table
            .new_btree_global_index_build_builder()
            .with_index_column("name")
            .execute()
            .await
            .expect_err("second build should overlap existing index");
        assert!(
            matches!(err, Error::DataInvalid { message, .. } if message.contains("overlaps requested row range"))
        );
    }
}
