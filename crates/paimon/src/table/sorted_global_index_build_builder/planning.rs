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

//! Planning of uncovered, row-range-aligned index shards.

use super::validation::ranges_overlap;
use crate::spec::{
    bucket_dir_name, BinaryRow, CoreOptions, DataField, DataFileMeta, FileKind, ManifestEntry,
    PartitionComputer,
};
use crate::table::source::{exclude_row_ranges, is_data_evolution_normal_file};
use crate::table::RowRange;
use crate::{Error, Result};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SortedGlobalIndexShard {
    pub partition: BinaryRow,
    pub partition_bytes: Vec<u8>,
    pub files: Vec<DataFileMeta>,
    pub row_range_start: i64,
    pub row_range_end: i64,
    pub(super) snapshot_id: i64,
    pub(super) source_bucket: i32,
    pub(super) total_buckets: i32,
    pub(super) bucket_path: String,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn plan_sorted_index_shards(
    table_location: &str,
    partition_keys: &[String],
    schema_fields: &[DataField],
    core_options: &CoreOptions,
    snapshot_id: i64,
    entries: Vec<ManifestEntry>,
    records_per_range: i64,
    indexed: &[RowRange],
) -> Result<Vec<SortedGlobalIndexShard>> {
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
                    "Data file '{}' is missing first_row_id; cannot build a complete sorted global index",
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
            let build_segments =
                exclude_row_ranges(&[RowRange::new(coverage_start, coverage_end)], indexed);
            for seg in build_segments {
                let seg_start = seg.from();
                let seg_end = seg.to();
                let start_range = seg_start / records_per_range;
                let end_range = seg_end / records_per_range;
                for range_id in start_range..=end_range {
                    let range_start = range_id * records_per_range;
                    let range_end = range_start + records_per_range - 1;
                    let row_range_start = seg_start.max(range_start);
                    let row_range_end = seg_end.min(range_end);
                    result.push(SortedGlobalIndexShard {
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
                "Data file '{}' is missing first_row_id; cannot build a complete sorted global index",
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
                "Data file '{}' is missing first_row_id; cannot build a complete sorted global index",
                file.file_name
            ),
            source: None,
        })?;
        start = Some(start.map_or(file_start, |value: i64| value.min(file_start)));
        end = Some(end.map_or(file_end, |value: i64| value.max(file_end)));
    }
    start.zip(end).ok_or_else(|| Error::DataInvalid {
        message: "Sorted global index shard has no normal data files".to_string(),
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
    let computer = PartitionComputer::new(
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
