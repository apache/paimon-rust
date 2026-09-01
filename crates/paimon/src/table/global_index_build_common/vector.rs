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

//! Shared shard planning for contiguous vector global indexes.

use crate::spec::{
    bucket_dir_name, BinaryRow, CoreOptions, DataField, DataFileMeta, DataType, FileKind,
    ManifestEntry, PartitionComputer,
};
use crate::table::source::exclude_row_ranges;
use crate::table::{RowRange, Table};
use crate::{Error, Result};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VectorIndexShard {
    pub(crate) partition: BinaryRow,
    pub(crate) partition_bytes: Vec<u8>,
    pub(crate) files: Vec<DataFileMeta>,
    pub(crate) row_range_start: i64,
    pub(crate) row_range_end: i64,
    pub(crate) snapshot_id: i64,
    pub(crate) source_bucket: i32,
    pub(crate) total_buckets: i32,
    pub(crate) bucket_path: String,
}

pub(crate) fn find_index_field<'a>(table: &'a Table, column: &str) -> Result<&'a DataField> {
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

pub(crate) fn validate_vector_field(field: &DataField, index_name: &str) -> Result<()> {
    let is_array_float = matches!(
        field.data_type(),
        DataType::Array(array) if matches!(array.element_type(), DataType::Float(_))
    );
    let is_vector_float = matches!(
        field.data_type(),
        DataType::Vector(vector) if matches!(vector.element_type(), DataType::Float(_))
    );
    if !is_array_float && !is_vector_float {
        return Err(Error::DataInvalid {
            message: format!(
                "{index_name} index requires ARRAY<FLOAT> or VECTOR<FLOAT> column, got {:?} for column '{}'",
                field.data_type(),
                field.name()
            ),
            source: None,
        });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn plan_vector_index_shards(
    table_location: &str,
    partition_keys: &[String],
    schema_fields: &[DataField],
    core_options: &CoreOptions,
    snapshot_id: i64,
    entries: Vec<ManifestEntry>,
    rows_per_shard: i64,
    indexed: &[RowRange],
    index_name: &str,
) -> Result<Vec<VectorIndexShard>> {
    if rows_per_shard <= 0 {
        return Err(Error::DataInvalid {
            message: format!(
                "Option 'global-index.row-count-per-shard' must be greater than 0, got: {rows_per_shard}"
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
            return Err(missing_row_id_error(entry.file(), index_name));
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
        let mut files_by_shard: HashMap<i64, Vec<DataFileMeta>> = HashMap::new();
        for file in files {
            let (file_start, file_end) = file
                .row_id_range()
                .ok_or_else(|| missing_row_id_error(&file, index_name))?;
            let start_shard = file_start / rows_per_shard;
            let end_shard = file_end / rows_per_shard;
            for shard_id in start_shard..=end_shard {
                files_by_shard
                    .entry(shard_id * rows_per_shard)
                    .or_default()
                    .push(file.clone());
            }
        }

        let mut shard_starts = files_by_shard.keys().copied().collect::<Vec<_>>();
        shard_starts.sort_unstable();
        for shard_start in shard_starts {
            let shard_end = shard_start + rows_per_shard - 1;
            let mut shard_files = files_by_shard.remove(&shard_start).unwrap_or_default();
            shard_files.sort_by_key(|file| file.first_row_id);
            for group in group_contiguous_files(shard_files, index_name)? {
                let group_start = group
                    .first()
                    .and_then(|file| file.first_row_id)
                    .expect("planned groups are non-empty and row-id assigned");
                let group_end = group
                    .iter()
                    .map(|file| file.row_id_range().unwrap().1)
                    .max()
                    .unwrap();
                let coverage_start = group_start.max(shard_start);
                let coverage_end = group_end.min(shard_end);
                let build_segments =
                    exclude_row_ranges(&[RowRange::new(coverage_start, coverage_end)], indexed);
                for segment in build_segments {
                    result.push(VectorIndexShard {
                        partition: partition.clone(),
                        partition_bytes: partition_bytes.clone(),
                        files: group.clone(),
                        row_range_start: segment.from(),
                        row_range_end: segment.to(),
                        snapshot_id,
                        source_bucket,
                        total_buckets,
                        bucket_path: bucket_path.clone(),
                    });
                }
            }
        }
    }
    result.sort_by(|left, right| {
        left.partition
            .to_serialized_bytes()
            .cmp(&right.partition.to_serialized_bytes())
            .then(left.source_bucket.cmp(&right.source_bucket))
            .then(left.row_range_start.cmp(&right.row_range_start))
    });
    Ok(result)
}

fn missing_row_id_error(file: &DataFileMeta, index_name: &str) -> Error {
    Error::DataInvalid {
        message: format!(
            "Data file '{}' is missing first_row_id; cannot build a complete {index_name} index",
            file.file_name
        ),
        source: None,
    }
}

fn group_contiguous_files(
    mut files: Vec<DataFileMeta>,
    index_name: &str,
) -> Result<Vec<Vec<DataFileMeta>>> {
    if files.is_empty() {
        return Ok(Vec::new());
    }
    files.sort_by_key(|file| file.first_row_id);
    let mut groups = Vec::new();
    let mut current = Vec::new();
    let mut current_end = None;
    for file in files {
        let (file_start, file_end) = file
            .row_id_range()
            .ok_or_else(|| missing_row_id_error(&file, index_name))?;
        match current_end {
            None => {
                current.push(file);
                current_end = Some(file_end);
            }
            Some(end) if file_start <= end + 1 => {
                current.push(file);
                current_end = Some(end.max(file_end));
            }
            Some(_) => {
                groups.push(std::mem::take(&mut current));
                current.push(file);
                current_end = Some(file_end);
            }
        }
    }
    if !current.is_empty() {
        groups.push(current);
    }
    Ok(groups)
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
