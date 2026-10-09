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

//! Source-backed BTREE/BITMAP planning for batch primary-key reads.
//! Mirrors Java PrimaryKeySortedIndexScan and PkSortedBucketIndexState:
//! immutable compacted files may use physical positions; merge inputs stay whole.

use super::global_index_scanner::{intersect_sorted_ranges, GlobalIndexScanner};
use super::index_file_path::IndexFileLocation;
use super::{merge_row_ranges, DataSplit, RowRange, Table};
use crate::spec::{
    should_read_pk_index_source, CoreOptions, DataField, DataFileMeta, FileKind,
    IndexManifestEntry, Predicate, PrimaryKeyIndexSourceMeta,
};
use crate::{Error, Result};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;

const MAX_POSITION_RANGES: usize = 4096;

type Options = HashMap<String, String>;
type SourceKey = (Vec<u8>, i32, String, i32);
type SourceGroups = HashMap<SourceKey, (usize, RowRange)>;
type EvaluateFuture<'a> = Pin<Box<dyn Future<Output = Result<Option<Vec<RowRange>>>> + Send + 'a>>;

pub(super) struct Definition {
    field_id: i32,
    index_type: &'static str,
    options: Options,
}

pub(super) fn definitions(fields: &[DataField], options: &Options) -> Result<Vec<Definition>> {
    let mut owners = HashMap::new();
    for family in ["btree", "bitmap", "vector", "full-text", "multivalue", "fm"] {
        let key = format!("pk-{family}.index.columns");
        if let Some(columns) = options.get(&key) {
            let mut unique = HashSet::new();
            for column in columns.split(',').map(str::trim) {
                if !unique.insert(column) {
                    return Err(config_invalid(format!(
                        "{key} contains duplicate column '{column}'"
                    )));
                }
                if owners.insert(column, family).is_some() {
                    return Err(config_invalid(format!(
                        "Column '{column}' can own at most one primary-key index"
                    )));
                }
            }
        }
    }
    let mut result = Vec::new();
    for field in fields {
        let Some(&family @ ("btree" | "bitmap")) = owners.get(field.name()) else {
            continue;
        };
        result.push(Definition {
            field_id: field.id(),
            index_type: family,
            options: definition_options(options, field.name(), family)?,
        });
    }
    Ok(result)
}

fn config_invalid(message: String) -> Error {
    Error::ConfigInvalid { message }
}

fn definition_options(options: &Options, column: &str, family: &str) -> Result<Options> {
    let mut resolved = options.clone();
    resolved.remove("sorted-index.records-per-file");
    resolved.remove("sorted-index.records-per-range");
    let option_key = format!("fields.{column}.pk-{family}.index.options");
    let Some(raw) = options
        .get(&option_key)
        .filter(|raw| !raw.trim().is_empty())
    else {
        return Ok(resolved);
    };
    let parsed: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(raw).map_err(|_| {
            config_invalid(format!(
                "{option_key} must be a JSON object of option key-value pairs"
            ))
        })?;
    let prefix = format!("{family}-index.");
    for (key, value) in parsed {
        if key.trim().is_empty() {
            return Err(config_invalid(format!(
                "{option_key} contains an empty option key"
            )));
        }
        let value = match value {
            serde_json::Value::String(value) => value,
            serde_json::Value::Number(_) | serde_json::Value::Bool(_) => value.to_string(),
            _ => {
                return Err(config_invalid(format!(
                    "{option_key} contains an invalid option value for {key}"
                )))
            }
        };
        let qualified = if key.starts_with(&prefix) || key.starts_with("fields.") {
            key
        } else {
            format!("{prefix}{key}")
        };
        if resolved
            .get(&qualified)
            .is_some_and(|previous| previous != &value)
        {
            return Err(config_invalid(format!(
                "{option_key} defines conflicting values for {qualified}"
            )));
        }
        resolved.insert(qualified, value);
    }
    Ok(resolved)
}

struct Group {
    entry: IndexManifestEntry,
    definition: usize,
    row_count: i64,
    scanner: Option<GlobalIndexScanner>,
    opened: bool,
    // Shared across all active source files: each query reads the payload once.
    queries: Vec<(Predicate, Option<Vec<RowRange>>)>,
}

struct Candidate {
    entry: IndexManifestEntry,
    row_count: i64,
    active_sources: Vec<(String, RowRange)>,
}

fn candidate(entry: &IndexManifestEntry, active: &[&DataFileMeta]) -> Option<(i32, Candidate)> {
    let meta = entry.index_file.global_index_meta.as_ref()?;
    if meta
        .extra_field_ids
        .as_ref()
        .is_some_and(|ids| !ids.is_empty())
    {
        return None;
    }
    let source = PrimaryKeyIndexSourceMeta::from_global_index_meta(meta).ok()?;
    let active: HashMap<_, _> = active
        .iter()
        .filter(|file| file.level == source.data_level() && should_read_pk_index_source(file))
        .map(|file| (file.file_name.as_str(), file.row_count))
        .collect();
    let mut previous: Option<&str> = None;
    let mut offset = 0i64;
    let mut active_sources = Vec::new();
    for file in source.source_files() {
        if previous.is_some_and(|name| name >= file.file_name()) {
            return None;
        }
        previous = Some(file.file_name());
        let end = offset.checked_add(file.row_count())?;
        if let Some(&rows) = active.get(file.file_name()) {
            if rows != file.row_count() {
                return None;
            }
            if end > offset {
                active_sources.push((file.file_name().to_string(), RowRange::new(offset, end - 1)));
            }
        }
        offset = end;
    }
    if active_sources.is_empty()
        || offset <= 0
        || meta.row_range_start != 0
        || meta.row_range_end != offset - 1
        || entry.index_file.row_count != offset
    {
        return None;
    }
    Some((
        source.data_level(),
        Candidate {
            entry: entry.clone(),
            row_count: offset,
            active_sources,
        },
    ))
}

fn plan_groups(
    table: &Table,
    splits: &[DataSplit],
    entries: &[IndexManifestEntry],
    definitions: &[Definition],
) -> (Vec<Group>, SourceGroups) {
    let mut buckets: BTreeMap<_, (Vec<&DataFileMeta>, &str)> = BTreeMap::new();
    for split in splits {
        let (files, _) = buckets
            .entry((split.partition().to_serialized_bytes(), split.bucket()))
            .or_insert_with(|| (Vec::new(), split.bucket_path()));
        files.extend(split.data_files());
    }
    let mut entries_by_bucket: HashMap<_, Vec<_>> = HashMap::new();
    for entry in entries.iter().filter(|entry| entry.kind == FileKind::Add) {
        entries_by_bucket
            .entry((entry.partition.as_slice(), entry.bucket))
            .or_default()
            .push(entry);
    }
    let mut groups = Vec::new();
    let mut sources = HashMap::new();
    for ((partition, bucket), (files, bucket_path)) in buckets {
        let location = IndexFileLocation::BucketLocal {
            table_path: table.location().trim_end_matches('/'),
            bucket_path,
            index_file_in_data_file_dir: table
                .schema()
                .core_options()
                .index_file_in_data_file_dir(),
        };
        for (definition_index, definition) in definitions.iter().enumerate() {
            let mut levels: BTreeMap<i32, Vec<Candidate>> = BTreeMap::new();
            for entry in entries_by_bucket
                .get(&(partition.as_slice(), bucket))
                .into_iter()
                .flatten()
            {
                if entry.index_file.index_type != definition.index_type
                    || entry
                        .index_file
                        .global_index_meta
                        .as_ref()
                        .is_none_or(|meta| meta.index_field_id != definition.field_id)
                {
                    continue;
                }
                if let Some((level, candidate)) = candidate(entry, &files) {
                    levels.entry(level).or_default().push(candidate);
                }
            }
            for candidates in levels.into_values() {
                // Java rejects ambiguous payload coverage at the same level.
                if candidates.len() != 1 {
                    continue;
                }
                let Candidate {
                    mut entry,
                    row_count,
                    active_sources,
                } = candidates.into_iter().next().unwrap();
                entry.index_file.external_path = Some(location.resolve(
                    &entry.index_file.file_name,
                    entry.index_file.external_path.as_deref(),
                ));
                let group_id = groups.len();
                for (file, range) in active_sources {
                    sources.insert(
                        (partition.clone(), bucket, file, definition.field_id),
                        (group_id, range),
                    );
                }
                groups.push(Group {
                    entry,
                    definition: definition_index,
                    row_count,
                    scanner: None,
                    opened: false,
                    queries: Vec::new(),
                });
            }
        }
    }
    (groups, sources)
}

impl Group {
    async fn query(
        &mut self,
        table: &Table,
        definition: &Definition,
        predicate: &Predicate,
    ) -> Result<Option<Vec<RowRange>>> {
        if let Some((_, result)) = self.queries.iter().find(|(query, _)| query == predicate) {
            return Ok(result.clone());
        }
        let result = self.evaluate(table, definition, predicate).await;
        let ranges = match result {
            Ok(Some(ranges))
                if ranges
                    .iter()
                    .all(|range| range.from() >= 0 && range.to() < self.row_count) =>
            {
                Some(ranges)
            }
            Ok(_) => None,
            Err(error) => {
                log::warn!(
                    "Ignoring source-backed PK index '{}': {error}",
                    self.entry.index_file.file_name
                );
                None
            }
        };
        self.queries.push((predicate.clone(), ranges.clone()));
        Ok(ranges)
    }

    async fn evaluate(
        &mut self,
        table: &Table,
        definition: &Definition,
        predicate: &Predicate,
    ) -> Result<Option<Vec<RowRange>>> {
        if !self.opened {
            self.opened = true;
            let options = CoreOptions::new(&definition.options);
            self.scanner = GlobalIndexScanner::create_with_fm_options(
                table.file_io(),
                table.location(),
                options.global_index_thread_num()?,
                options.btree_index_fallback_scan_max_size()?,
                options.btree_index_data_block_cache_size()?,
                options.bitmap_index_fallback_scan_max_size()?,
                std::slice::from_ref(&self.entry),
                table.schema().fields(),
                crate::fm_index::FMReadOptions::default(),
            )?;
        }
        match &self.scanner {
            Some(scanner) => scanner.matching_ranges(predicate).await,
            None => Ok(None),
        }
    }
}

struct Evaluation<'a> {
    table: &'a Table,
    definitions: &'a [Definition],
    groups: Vec<Group>,
    sources: SourceGroups,
}

impl Evaluation<'_> {
    fn evaluate<'a>(
        &'a mut self,
        split: &'a DataSplit,
        file: &'a DataFileMeta,
        predicate: &'a Predicate,
    ) -> EvaluateFuture<'a> {
        Box::pin(async move {
            match predicate {
                Predicate::Leaf { column, .. } => {
                    self.evaluate_field(split, file, column, predicate).await
                }
                Predicate::And(_) => {
                    let mut result: Option<Vec<RowRange>> = None;
                    let mut fields: BTreeMap<String, Vec<Predicate>> = BTreeMap::new();
                    let mut other = Vec::new();
                    for child in predicate.clone().split_and() {
                        if let Predicate::Leaf { column, .. } = &child {
                            fields.entry(column.clone()).or_default().push(child);
                        } else {
                            other.push(child);
                        }
                    }
                    // Same-field bounds must reach the scalar scanner together:
                    // it combines BETWEEN and reuses a single opened reader.
                    for (column, predicates) in fields {
                        let combined = Predicate::and(predicates);
                        if let Some(ranges) =
                            self.evaluate_field(split, file, &column, &combined).await?
                        {
                            result = Some(match result {
                                None => ranges,
                                Some(previous) => intersect_sorted_ranges(&previous, &ranges),
                            });
                        }
                    }
                    for child in &other {
                        if let Some(ranges) = self.evaluate(split, file, child).await? {
                            result = Some(match result {
                                None => ranges,
                                Some(previous) => intersect_sorted_ranges(&previous, &ranges),
                            });
                        }
                    }
                    Ok(result)
                }
                Predicate::Or(children) if !children.is_empty() => {
                    let mut result = Vec::new();
                    for child in children {
                        let Some(ranges) = self.evaluate(split, file, child).await? else {
                            return Ok(None);
                        };
                        result.extend(ranges);
                    }
                    Ok(Some(merge_row_ranges(result)))
                }
                _ => Ok(None),
            }
        })
    }
    async fn evaluate_field(
        &mut self,
        split: &DataSplit,
        file: &DataFileMeta,
        column: &str,
        predicate: &Predicate,
    ) -> Result<Option<Vec<RowRange>>> {
        let Some(field) = self
            .table
            .schema()
            .fields()
            .iter()
            .find(|field| field.name() == column)
        else {
            return Ok(None);
        };
        let key = (
            split.partition().to_serialized_bytes(),
            split.bucket(),
            file.file_name.clone(),
            field.id(),
        );
        let Some((group_id, range)) = self.sources.get(&key).cloned() else {
            return Ok(None);
        };
        let group = &mut self.groups[group_id];
        let definition = &self.definitions[group.definition];
        Ok(group
            .query(self.table, definition, predicate)
            .await?
            .map(|ranges| localize(&ranges, range)))
    }
}

fn localize(ranges: &[RowRange], source: RowRange) -> Vec<RowRange> {
    let start = ranges.partition_point(|range| range.to() < source.from());
    ranges[start..]
        .iter()
        .take_while(|range| range.from() <= source.to())
        .filter_map(|range| {
            let from = range.from().max(source.from());
            let to = range.to().min(source.to());
            (from <= to).then(|| RowRange::new(from - source.from(), to - source.from()))
        })
        .collect()
}

pub(super) async fn refine(
    table: &Table,
    splits: Vec<DataSplit>,
    entries: &[IndexManifestEntry],
    definitions: &[Definition],
    predicates: &[Predicate],
) -> Result<Vec<DataSplit>> {
    let (groups, sources) = plan_groups(table, &splits, entries, definitions);
    if groups.is_empty() {
        return Ok(splits);
    }
    let mut evaluation = Evaluation {
        table,
        definitions,
        groups,
        sources,
    };
    let predicate = Predicate::and(predicates.to_vec());
    let mut result = Vec::new();
    for split in splits {
        if !split.raw_convertible() || split.is_streaming() || split.row_ranges().is_some() {
            result.push(split);
            continue;
        }
        for (file_index, file) in split.data_files().iter().enumerate() {
            let ranges = evaluation.evaluate(&split, file, &predicate).await?;
            if ranges.as_ref().is_some_and(Vec::is_empty) {
                continue;
            }
            let mut selected = split.for_pk_index_file(file_index);
            if let Some(ranges) = ranges.filter(|ranges| {
                ranges.len() <= MAX_POSITION_RANGES
                    && ranges.iter().all(|range| {
                        range.from() >= 0
                            && range.to() < file.row_count
                            && range.to() <= i64::from(i32::MAX)
                    })
            }) {
                selected = selected.with_selected_row_ranges(ranges);
            }
            result.push(selected);
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests;
