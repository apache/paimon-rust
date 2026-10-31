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

//! Compare desired global row-ID units with their current index payloads.
//! Like Java PrimaryKeyIndexLevels, rebuild changed units and retire their old
//! payloads atomically. Global indexes use row-ID shards rather than PK levels:
//! a physical compaction preserving logical row IDs does not invalidate them.

use super::*;
use crate::table::global_index_build_common::{
    resolve_index_fields, GlobalIndexBuildView, IndexColumns,
};
use crate::table::{merge_row_ranges, RowRange};
use std::collections::BTreeMap;

type UnitKey = (Vec<u8>, i32, i64);
type IndexFileKey = (Vec<u8>, i32, String);

fn index_file_key(entry: &IndexManifestEntry) -> IndexFileKey {
    (
        entry.partition.clone(),
        entry.bucket,
        entry.index_file.file_name.clone(),
    )
}

pub(super) struct MaintenancePlan {
    replaced: HashSet<IndexFileKey>,
    pub deletions: Vec<CommitMessage>,
}

impl MaintenancePlan {
    pub(super) fn build_view(&self, source: &GlobalIndexBuildView) -> GlobalIndexBuildView {
        source.filter_indexes(|entry| !self.replaced.contains(&index_file_key(entry)))
    }
}

fn partition_key(bytes: &[u8]) -> Vec<u8> {
    if super::super::is_empty_partition(bytes) {
        vec![]
    } else {
        bytes.to_vec()
    }
}

pub(super) async fn plan(
    table: &Table,
    definition: &Definition,
    data: &[ManifestEntry],
    indexes: &[IndexManifestEntry],
) -> Result<MaintenancePlan> {
    let mut options = table.schema().options().clone();
    options.extend(definition.options.clone());
    let core = CoreOptions::new(&options);
    let width = if matches!(
        definition.index_type.as_str(),
        "btree" | "bitmap" | "multivalue" | "fm"
    ) {
        core.sorted_index_records_per_range()?
    } else {
        core.global_index_row_count_per_shard()?
    };
    let fields = resolve_index_fields(
        table,
        &IndexColumns::Columns(vec![definition.column.clone()]),
        &definition.index_type,
    )?;
    let field_id = fields[0].id();
    let vector_metric = if crate::vindex::is_vindex_index_type(&definition.index_type) {
        let options = crate::vindex::VindexVectorIndexOptions::new(
            table.schema().options(),
            &definition.options,
            &definition.index_type,
            fields[0],
        )?;
        Some(crate::vindex::pkvector::metric::VectorSearchMetric::parse(
            &options.native_options["metric"],
        )?)
    } else {
        None
    };
    let mut desired: HashMap<UnitKey, Vec<RowRange>> = HashMap::new();
    for entry in data {
        if *entry.kind() != FileKind::Add {
            continue;
        }
        let Some((start, end)) = entry.file().row_id_range() else {
            return Err(crate::Error::DataInvalid {
                message: "Automatic index maintenance requires assigned source row IDs".into(),
                source: None,
            });
        };
        if start < 0 || end < start {
            return Err(crate::Error::DataInvalid {
                message: "Invalid source row-ID range for automatic index maintenance".into(),
                source: None,
            });
        }
        for shard in start / width..=end / width {
            let from = shard.saturating_mul(width);
            let to = from.saturating_add(width - 1);
            desired
                .entry((partition_key(entry.partition()), entry.bucket(), shard))
                .or_default()
                .push(RowRange::new(start.max(from), end.min(to)));
        }
    }
    let desired = desired
        .into_iter()
        .map(|(key, ranges)| (key, merge_row_ranges(ranges)))
        .collect::<HashMap<_, _>>();
    let selected = indexes
        .iter()
        .filter(|entry| {
            entry.kind == FileKind::Add
                && (entry.index_file.index_type == definition.index_type
                    || vector_metric.is_some()
                        && crate::vindex::is_vindex_index_type(&entry.index_file.index_type))
                && entry
                    .index_file
                    .global_index_meta
                    .as_ref()
                    .is_some_and(|meta| {
                        meta.index_field_id == field_id
                            && meta.extra_field_ids.as_ref().is_none_or(Vec::is_empty)
                    })
        })
        .collect::<Vec<_>>();
    let mut current: HashMap<UnitKey, Vec<&IndexManifestEntry>> = HashMap::new();
    let mut replaced = HashSet::new();
    for entry in &selected {
        let meta = entry.index_file.global_index_meta.as_ref().unwrap();
        if let Some(metric) = vector_metric {
            // A vector column owns one algorithm and scoring metric across all shards.
            // Coverage equality cannot make a previous definition compatible.
            let compatible = entry.index_file.index_type == definition.index_type
                && crate::table::vector_search_common::read_index_vector_metric(
                    table.file_io(),
                    table.location(),
                    entry,
                )
                .await?
                .is_some_and(|persisted| {
                    crate::vindex::pkvector::metric::VectorSearchMetric::from_vindex(persisted)
                        == metric
                });
            if !compatible {
                replaced.insert(index_file_key(entry));
                continue;
            }
        }
        if meta.row_range_start < 0 || meta.row_range_end < meta.row_range_start {
            return Err(crate::Error::DataInvalid {
                message: "Invalid global index row-ID range during maintenance".into(),
                source: None,
            });
        }
        let shard = meta.row_range_start / width;
        if shard != meta.row_range_end / width {
            // A shard-size change can invalidate a former unit's boundaries.
            replaced.insert(index_file_key(entry));
        } else {
            current
                .entry((partition_key(&entry.partition), entry.bucket, shard))
                .or_default()
                .push(entry);
        }
    }
    for (key, files) in current {
        let coverage = merge_row_ranges(
            files
                .iter()
                .map(|entry| {
                    let meta = entry.index_file.global_index_meta.as_ref().unwrap();
                    RowRange::new(meta.row_range_start, meta.row_range_end)
                })
                .collect(),
        );
        let wanted = desired.get(&key).map(Vec::as_slice).unwrap_or(&[]);
        if coverage.as_slice() != wanted || files.len() != wanted.len() {
            replaced.extend(files.iter().map(|entry| index_file_key(entry)));
        }
    }
    let mut deletions: BTreeMap<(Vec<u8>, i32), CommitMessage> = BTreeMap::new();
    for entry in selected {
        if replaced.contains(&index_file_key(entry)) {
            deletions
                .entry((entry.partition.clone(), entry.bucket))
                .or_insert_with(|| {
                    CommitMessage::new(entry.partition.clone(), entry.bucket, vec![])
                })
                .deleted_index_files
                .push(entry.index_file.clone());
        }
    }
    Ok(MaintenancePlan {
        replaced,
        deletions: deletions.into_values().collect(),
    })
}
