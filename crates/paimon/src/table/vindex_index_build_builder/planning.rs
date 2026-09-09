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

use crate::spec::{CoreOptions, DataField, ManifestEntry};
use crate::table::global_index_build_common::vector::{plan_vector_index_shards, VectorIndexShard};
use crate::table::RowRange;
use crate::{Error, Result};

use super::validation::checked_row_count;

const MAX_IVF_TRAINING_RANGES: usize = 64;

pub(crate) type VindexIndexShard = VectorIndexShard;

#[allow(clippy::too_many_arguments)]
pub(super) fn plan_vindex_shards(
    table_location: &str,
    partition_keys: &[String],
    schema_fields: &[DataField],
    core_options: &CoreOptions,
    snapshot_id: i64,
    entries: Vec<ManifestEntry>,
    rows_per_shard: i64,
    indexed: &[RowRange],
) -> Result<Vec<VindexIndexShard>> {
    plan_vector_index_shards(
        table_location,
        partition_keys,
        schema_fields,
        core_options,
        snapshot_id,
        entries,
        rows_per_shard,
        indexed,
        "vindex",
    )
}

pub(super) fn plan_ivf_training_ranges(
    shard: &VindexIndexShard,
    training_rows: usize,
) -> Result<Vec<RowRange>> {
    let shard_rows = usize::try_from(checked_row_count(
        shard.row_range_start,
        shard.row_range_end,
    )?)
    .map_err(|error| Error::DataInvalid {
        message: "vindex shard row count does not fit usize".to_string(),
        source: Some(Box::new(error)),
    })?;
    if training_rows == 0 || training_rows > shard_rows {
        return Err(Error::DataInvalid {
            message: format!(
                "Invalid IVF training row count: {training_rows}; shard contains {shard_rows} rows"
            ),
            source: None,
        });
    }
    if training_rows == shard_rows {
        return Ok(Vec::new());
    }

    let range_count = training_rows.min(MAX_IVF_TRAINING_RANGES);
    let gap_count = range_count + 1;
    let skipped_rows = shard_rows - training_rows;
    let seed = mix_seed(
        (shard.snapshot_id as u64)
            ^ (shard.row_range_start as u64).rotate_left(21)
            ^ (shard.row_range_end as u64).rotate_left(42),
    );
    let range_extra_offset = seed as usize % range_count;
    let gap_extra_offset = seed.rotate_left(17) as usize % gap_count;
    let mut cursor = shard.row_range_start;
    let mut ranges = Vec::with_capacity(range_count);

    for gap_index in 0..range_count {
        let gap = skipped_rows / gap_count
            + usize::from(
                (gap_index + gap_count - gap_extra_offset) % gap_count < skipped_rows % gap_count,
            );
        cursor = checked_add_offset(cursor, gap, "training gap")?;
        let length = training_rows / range_count
            + usize::from(
                (gap_index + range_count - range_extra_offset) % range_count
                    < training_rows % range_count,
            );
        let end = checked_add_offset(cursor, length - 1, "training range")?;
        ranges.push(RowRange::new(cursor, end));
        cursor = end.checked_add(1).ok_or_else(|| Error::DataInvalid {
            message: "vindex training range end overflows i64".to_string(),
            source: None,
        })?;
    }

    Ok(ranges)
}

fn checked_add_offset(value: i64, offset: usize, name: &str) -> Result<i64> {
    let offset = i64::try_from(offset).map_err(|error| Error::DataInvalid {
        message: format!("vindex {name} offset does not fit i64"),
        source: Some(Box::new(error)),
    })?;
    value.checked_add(offset).ok_or_else(|| Error::DataInvalid {
        message: format!("vindex {name} offset overflows i64"),
        source: None,
    })
}

fn mix_seed(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
