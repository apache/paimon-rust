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

//! Append missing row-ID coverage as independent index runs. Select adjacent
//! runs with the shared UniversalCompaction policy and rebuild selected ranges
//! from current data, atomically retiring their old payloads.

use super::*;
use crate::table::global_index_build_common::{
    resolve_index_fields, GlobalIndexBuildView, IndexColumns,
};
use crate::table::source::exclude_row_ranges;
use crate::table::universal_compaction::{Run, UniversalCompaction};
use crate::table::{merge_row_ranges, RowRange};
use std::collections::BTreeMap;

#[cfg(test)]
mod tests;

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
    pub needs_build: bool,
    build_ranges: HashMap<(Vec<u8>, i32), Vec<RowRange>>,
    training_groups: Vec<RowRange>,
}

impl MaintenancePlan {
    pub(super) fn can_expand_training_range(&self, range: &RowRange) -> bool {
        self.training_groups
            .iter()
            .any(|group| group.from() <= range.from() && range.to() <= group.to() && group != range)
    }

    pub(super) fn build_view(&self, source: &GlobalIndexBuildView) -> GlobalIndexBuildView {
        let view = if self.replaced.is_empty() {
            source.clone()
        } else {
            source.filter_indexes(|entry| !self.replaced.contains(&index_file_key(entry)))
        };
        view.filter_sources(|entry| {
            let Some((start, end)) = entry.file().row_id_range() else {
                return false;
            };
            let Some(ranges) = self
                .build_ranges
                .get(&(partition_key(entry.partition()), entry.bucket()))
            else {
                return false;
            };
            let first = ranges.partition_point(|range| range.to() < start);
            ranges.get(first).is_some_and(|range| range.from() <= end)
        })
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
    plan_impl(table, definition, data, indexes, false).await
}

/// A native training constraint may require a larger unit than an independent
/// leaf. Retry once with complete pending groups, preserving the declared codec.
pub(super) async fn plan_for_training(
    table: &Table,
    definition: &Definition,
    data: &[ManifestEntry],
    indexes: &[IndexManifestEntry],
) -> Result<MaintenancePlan> {
    plan_impl(table, definition, data, indexes, true).await
}

async fn plan_impl(
    table: &Table,
    definition: &Definition,
    data: &[ManifestEntry],
    indexes: &[IndexManifestEntry],
    force_training_merge: bool,
) -> Result<MaintenancePlan> {
    let mut options = table.schema().options().clone();
    options.extend(definition.options.clone());
    let core = CoreOptions::new(&options);
    let strategy = UniversalCompaction::from_options(&options, "global-index.")?;
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
    for entry in selected.iter().copied() {
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
        let key = (partition_key(&entry.partition), entry.bucket, shard);
        let contained = desired.get(&key).is_some_and(|groups| {
            groups.iter().any(|group| {
                group.from() <= meta.row_range_start && meta.row_range_end <= group.to()
            })
        });
        if shard != meta.row_range_end / width || !contained {
            // Row removal or shard-boundary changes cannot leave stale coverage.
            replaced.insert(index_file_key(entry));
        } else {
            current.entry(key).or_default().push(entry);
        }
    }
    for files in current.values_mut() {
        retire_overlaps(files, &mut replaced);
    }
    let mut build_ranges: HashMap<(Vec<u8>, i32), Vec<RowRange>> = HashMap::new();
    let mut training_groups = Vec::new();
    for ((partition, bucket, shard), groups) in desired {
        let files = current
            .remove(&(partition.clone(), bucket, shard))
            .unwrap_or_default();
        for group in groups {
            let existing = files
                .iter()
                .copied()
                .filter(|entry| {
                    let meta = entry.index_file.global_index_meta.as_ref().unwrap();
                    group.from() <= meta.row_range_start && meta.row_range_end <= group.to()
                })
                .collect::<Vec<_>>();
            let covered =
                merge_row_ranges(existing.iter().map(|entry| index_range(entry)).collect());
            let missing = exclude_row_ranges(std::slice::from_ref(&group), &covered);
            let mut runs = existing
                .iter()
                .map(|&entry| IndexRun {
                    range: index_range(entry),
                    index: Some(entry),
                })
                .chain(
                    missing
                        .iter()
                        .cloned()
                        .map(|range| IndexRun { range, index: None }),
                )
                .collect::<Vec<_>>();
            // Increasing row IDs act as append sequence: newest runs first, as
            // in PK L0 selection. Only adjacent ranges of one group may merge.
            runs.sort_by_key(|run| std::cmp::Reverse(run.range.from()));
            let candidates = runs
                .iter()
                .map(|run| Run {
                    size: run.range.to() as u128 - run.range.from() as u128 + 1,
                    stop_before_large: true,
                })
                .collect::<Vec<_>>();
            let mut pending = missing;
            if let Some(count) = strategy.pick(&candidates, 0).filter(|&count| count >= 2) {
                for run in &runs[..count] {
                    if let Some(entry) = run.index {
                        replaced.insert(index_file_key(entry));
                    }
                    pending.push(run.range.clone());
                }
            }
            let mut pending = merge_row_ranges(pending);
            if pending.is_empty() {
                continue;
            }
            if pending.as_slice() != std::slice::from_ref(&group) && vector_metric.is_some() {
                training_groups.push(group.clone());
                if force_training_merge {
                    replaced.extend(existing.iter().map(|entry| index_file_key(entry)));
                    pending = vec![group];
                }
            }
            build_ranges
                .entry((partition.clone(), bucket))
                .or_default()
                .extend(pending);
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
    let build_ranges = build_ranges
        .into_iter()
        .map(|(key, ranges)| (key, merge_row_ranges(ranges)))
        .collect::<HashMap<_, _>>();
    Ok(MaintenancePlan {
        replaced,
        deletions: deletions.into_values().collect(),
        needs_build: !build_ranges.is_empty(),
        build_ranges,
        training_groups,
    })
}

struct IndexRun<'a> {
    range: RowRange,
    index: Option<&'a IndexManifestEntry>,
}

fn index_range(entry: &IndexManifestEntry) -> RowRange {
    let meta = entry.index_file.global_index_meta.as_ref().unwrap();
    RowRange::new(meta.row_range_start, meta.row_range_end)
}

/// Adjacent fragments are legitimate runs; overlapping coverage must be
/// replaced before incremental builds can safely reuse the remaining files.
fn retire_overlaps(files: &mut Vec<&IndexManifestEntry>, replaced: &mut HashSet<IndexFileKey>) {
    files.sort_by_key(|entry| {
        entry
            .index_file
            .global_index_meta
            .as_ref()
            .unwrap()
            .row_range_start
    });
    let mut first = 0;
    while first < files.len() {
        let mut end = index_range(files[first]).to();
        let mut next = first + 1;
        while next < files.len() && index_range(files[next]).from() <= end {
            end = end.max(index_range(files[next]).to());
            next += 1;
        }
        if next > first + 1 {
            replaced.extend(files[first..next].iter().map(|entry| index_file_key(entry)));
        }
        first = next;
    }
    files.retain(|entry| !replaced.contains(&index_file_key(entry)));
}
