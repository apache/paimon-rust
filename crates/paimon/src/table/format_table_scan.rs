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

//! Scan implementation for Java-compatible `type=format-table` metadata.

use std::collections::{HashMap, HashSet};

use super::format_partition::{
    format_partition_value, parse_format_partition_value, FormatTablePartitionPaths,
};
use super::{Plan, RESTEnv, ScanTrace, Table};
use crate::api::RestError;
use crate::spec::stats::BinaryTableStats;
use crate::spec::{
    escape_path_name, extract_datum, BinaryRow, BinaryRowBuilder, CoreOptions, DataField,
    DataFileMeta, DataType, Datum, Partition, PartitionComputer, Predicate, PredicateOperator,
    PATH_OPTION,
};
use crate::table::partition_filter::PartitionFilter;
use crate::table::source::{DataSplitBuilder, RowRange};
use futures::{StreamExt, TryStreamExt};

#[derive(Debug, Clone)]
pub(crate) struct FormatTableScan<'a> {
    table: &'a Table,
    partition_filter: Option<PartitionFilter>,
    limit: Option<usize>,
    row_ranges: Option<Vec<RowRange>>,
}

impl<'a> FormatTableScan<'a> {
    pub(crate) fn new(
        table: &'a Table,
        partition_filter: Option<PartitionFilter>,
        limit: Option<usize>,
        row_ranges: Option<Vec<RowRange>>,
    ) -> Self {
        Self {
            table,
            partition_filter,
            limit,
            row_ranges,
        }
    }

    pub(crate) fn with_row_ranges(mut self, ranges: Vec<RowRange>) -> Self {
        self.row_ranges = Some(ranges);
        self
    }

    pub(crate) async fn plan(&self) -> crate::Result<Plan> {
        self.ensure_query_auth_allowed()?;
        self.plan_inner(None).await
    }

    pub(crate) async fn plan_with_trace(&self) -> crate::Result<(Plan, ScanTrace)> {
        self.ensure_query_auth_allowed()?;
        let mut trace = ScanTrace::default();
        let plan = self.plan_inner(Some(&mut trace)).await?;
        trace.planned_data_file_bytes = plan.planned_data_file_bytes();
        Ok((plan, trace))
    }

    fn ensure_query_auth_allowed(&self) -> crate::Result<()> {
        CoreOptions::new(self.table.schema().options()).ensure_read_authorized()
    }

    async fn plan_inner(&self, trace: Option<&mut ScanTrace>) -> crate::Result<Plan> {
        if self.row_ranges.is_some() {
            return Err(crate::Error::Unsupported {
                message: "Row ranges are not supported for format tables".to_string(),
            });
        }
        let core_options = CoreOptions::new(self.table.schema().options());
        let format_extension = supported_format_table_extension(&core_options.file_format())?;
        let schema_id = self.table.schema().id();
        let table_path = core_options
            .path()
            .unwrap_or_else(|| self.table.location())
            .trim_end_matches('/')
            .to_string();

        let partition_fields = self.table.schema().partition_fields();
        let table_depth = path_segments(&table_path).len();
        let scan_roots = self.scan_roots(&core_options, &table_path).await?;
        // A table with many partitions pays one listing per partition, so they run concurrently.
        // `buffered` keeps the roots in order and stops at the first failure.
        let table_path = table_path.as_str();
        let partition_fields = partition_fields.as_slice();
        let listed: Vec<Vec<crate::DataSplit>> = futures::stream::iter(scan_roots)
            .map(|scan_root| async move {
                let root_segments = path_segments(&scan_root.path);
                let partition_levels_below_root = partition_fields
                    .len()
                    .saturating_sub(root_segments.len().saturating_sub(table_depth));
                let statuses = self
                    .list_status_recursive_if_exists(&scan_root.path)
                    .await?;
                let mut splits = Vec::new();
                for status in statuses {
                    if is_hidden_below_partitions(
                        &root_segments,
                        partition_levels_below_root,
                        &status.path,
                    ) {
                        continue;
                    }
                    if let Some(split) = self
                        .status_to_split(
                            status,
                            table_path,
                            format_extension,
                            schema_id,
                            partition_fields,
                            scan_root.partition.clone(),
                        )
                        .await?
                    {
                        splits.push(split);
                    }
                }
                Ok::<_, crate::Error>(splits)
            })
            .buffered(core_options.format_table_scan_list_parallelism())
            .try_collect()
            .await?;
        let mut splits = listed.into_iter().flatten().collect::<Vec<_>>();

        splits.sort_by(|left, right| {
            left.bucket_path().cmp(right.bucket_path()).then_with(|| {
                left.data_files()[0]
                    .file_name
                    .cmp(&right.data_files()[0].file_name)
            })
        });
        splits = self.apply_limit_pushdown(splits);

        if let Some(trace) = trace {
            trace.record_final_plan(splits.len(), splits.len(), splits.len());
        }
        Ok(Plan::new(splits))
    }

    async fn scan_roots(
        &self,
        core_options: &CoreOptions<'_>,
        table_path: &str,
    ) -> crate::Result<Vec<ScanRoot>> {
        let partition_keys = self.table.schema().partition_keys();
        let partition_fields = self.table.schema().partition_fields();
        if partition_keys.is_empty() {
            return Ok(vec![ScanRoot {
                path: table_path.to_string(),
                partition: BinaryRow::new(0),
            }]);
        }
        if let Some(rest_env) = self
            .table
            .rest_env()
            .filter(|_| self.table.has_catalog_managed_partitions())
        {
            return self
                .catalog_managed_scan_roots(rest_env, table_path, partition_keys, &partition_fields)
                .await;
        }

        let Some(PartitionFilter::PartitionSet { partitions, .. }) = &self.partition_filter else {
            if let Some(PartitionFilter::Predicate(predicate)) = &self.partition_filter {
                if let Some(path) = leading_equality_partition_path(
                    table_path,
                    partition_keys,
                    &partition_fields,
                    predicate,
                    core_options.partition_default_name(),
                    core_options.legacy_partition_name(),
                    core_options.format_table_partition_only_value_in_path(),
                ) {
                    return Ok(vec![ScanRoot {
                        path,
                        partition: BinaryRow::new(0),
                    }]);
                }
            }
            return Ok(vec![ScanRoot {
                path: table_path.to_string(),
                partition: BinaryRow::new(0),
            }]);
        };

        let partition_computer = PartitionComputer::new(
            partition_keys,
            self.table.schema().fields(),
            core_options.partition_default_name(),
            core_options.legacy_partition_name(),
        )?;
        let only_value_in_path = core_options.format_table_partition_only_value_in_path();
        let mut roots = Vec::with_capacity(partitions.len());
        for partition in partitions {
            let row = BinaryRow::from_serialized_bytes(partition)?;
            let partition_path = if only_value_in_path {
                partition_path_from_row(
                    &row,
                    &partition_fields,
                    core_options.partition_default_name(),
                    core_options.legacy_partition_name(),
                    true,
                )?
            } else {
                partition_computer.generate_partition_path(&row)?
            };
            roots.push(ScanRoot {
                path: join_path(table_path, &partition_path),
                partition: row,
            });
        }
        roots.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(roots)
    }

    /// One scan root per registered partition this scan reaches, never one discovered from the
    /// directory tree.
    async fn catalog_managed_scan_roots(
        &self,
        rest_env: &RESTEnv,
        table_path: &str,
        partition_keys: &[String],
        partition_fields: &[DataField],
    ) -> crate::Result<Vec<ScanRoot>> {
        let core_options = CoreOptions::new(self.table.schema().options());
        let partition_paths = FormatTablePartitionPaths::new(
            partition_keys.iter().cloned(),
            core_options.format_table_partition_only_value_in_path(),
        );
        let default_partition_name = core_options.partition_default_name();
        // Ask the catalog only for the partitions the filter can reach. Downloading every
        // registration of a table with many partitions is what dominates planning time,
        // and the local match below still decides what is actually scanned.
        let pattern = match &self.partition_filter {
            Some(filter) => {
                let mut leading_values = leading_equality_partition_values(
                    filter,
                    partition_fields,
                    default_partition_name,
                    core_options.legacy_partition_name(),
                )?;
                // A name pattern compares spellings, so it can only stand in for an equality on a
                // column whose values have one. Another engine may register `month=01` or
                // `active=TRUE`, which would fall out of a pattern built from `month = 1` or
                // `active = true` before the typed match below ever saw them.
                let single_spelling = partition_fields
                    .iter()
                    .take_while(|field| {
                        matches!(field.data_type(), DataType::Char(_) | DataType::VarChar(_))
                    })
                    .count();
                leading_values.truncate(single_spelling);
                partition_paths.name_prefix_pattern(&leading_values)
            }
            None => None,
        };
        let filter = match &self.partition_filter {
            Some(PartitionFilter::Predicate(predicate)) => {
                partition_filter_json(predicate).map(|filter| filter.to_string())
            }
            _ => None,
        };
        let partitions = self
            .list_catalog_partitions(rest_env, pattern.as_deref(), filter.as_deref())
            .await?;
        let mut seen_paths = HashSet::with_capacity(partitions.len());
        let mut roots = Vec::with_capacity(partitions.len());
        for partition in partitions {
            let partition_path = partition_paths
                .relative_path(&partition.spec)
                .map_err(|error| self.invalid_catalog_partition_metadata(error))?;
            if !seen_paths.insert(partition_path.clone()) {
                continue;
            }
            let path = join_path(table_path, &partition_path);
            let row = partition_row_from_catalog_spec(
                &partition.spec,
                partition_fields,
                partition_keys,
                default_partition_name,
            )
            .map_err(|error| self.invalid_catalog_partition_metadata(error))?;
            if !self.partition_matches(&row)? {
                continue;
            }
            // The Rust reader cannot resolve a partition's own location yet, and reading the
            // default directory in its place would return whatever happens to be there.
            if partition
                .options
                .as_ref()
                .is_some_and(|options| options.contains_key(PATH_OPTION))
            {
                return Err(crate::Error::Unsupported {
                    message: format!(
                        "Partition {:?} of Format Table {} is registered at a custom location, \
                         which the Rust reader does not support yet",
                        partition.spec,
                        self.table.identifier().full_name()
                    ),
                });
            }
            roots.push(ScanRoot {
                path,
                partition: row,
            });
        }
        roots.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(roots)
    }

    /// The registered partitions the catalog returns for this scan's pushdown hints.
    ///
    /// A catalog that cannot list by filter is still asked by pattern, so the catalog stays the
    /// source of the partition set; the typed match on the result decides what is scanned.
    async fn list_catalog_partitions(
        &self,
        rest_env: &RESTEnv,
        pattern: Option<&str>,
        filter: Option<&str>,
    ) -> crate::Result<Vec<Partition>> {
        let api = rest_env.api();
        let identifier = rest_env.identifier();
        if let Some(filter) = filter {
            match api
                .list_partitions_by_filter(identifier, filter, pattern)
                .await
            {
                Err(crate::Error::RestApi {
                    source: RestError::NotImplemented { .. },
                }) => {}
                result => return result,
            }
        }
        api.list_partitions_by_name_pattern(identifier, pattern)
            .await
    }

    fn invalid_catalog_partition_metadata(&self, source: crate::Error) -> crate::Error {
        crate::Error::DataInvalid {
            message: format!(
                "Catalog returned invalid partition metadata for Format Table {}",
                self.table.identifier().full_name()
            ),
            source: Some(Box::new(source)),
        }
    }

    async fn list_status_recursive_if_exists(
        &self,
        path: &str,
    ) -> crate::Result<Vec<crate::io::FileStatus>> {
        match self.table.file_io().list_status_recursive(path).await {
            Ok(statuses) => Ok(statuses),
            Err(err) => {
                if !self.table.file_io().exists(path).await.unwrap_or(true) {
                    Ok(Vec::new())
                } else {
                    Err(err)
                }
            }
        }
    }

    async fn status_to_split(
        &self,
        status: crate::io::FileStatus,
        table_path: &str,
        format_extension: &str,
        schema_id: i64,
        partition_fields: &[DataField],
        known_partition: BinaryRow,
    ) -> crate::Result<Option<crate::DataSplit>> {
        let Some((parent, file_name)) = split_parent_and_file(&status.path) else {
            return Ok(None);
        };
        let parent = parent.to_string();
        let file_name = file_name.to_string();
        if !is_format_table_data_file_name(&file_name) {
            return Ok(None);
        }
        if !file_name.to_ascii_lowercase().ends_with(format_extension) {
            return Ok(None);
        }
        let status = if status.size == 0 {
            self.table.file_io().get_status(&status.path).await?
        } else {
            status
        };
        let file_size = i64::try_from(status.size).map_err(|_| crate::Error::DataInvalid {
            message: format!(
                "Format table file '{}' is too large to fit in i64 metadata",
                status.path
            ),
            source: None,
        })?;
        let data_file = data_file_meta(file_name, file_size, schema_id);
        let partition = if partition_fields.is_empty() {
            BinaryRow::new(0)
        } else if known_partition.arity() == partition_fields.len() as i32
            && !known_partition.is_empty()
        {
            known_partition
        } else {
            let core_options = CoreOptions::new(self.table.schema().options());
            let Some(partition) = partition_row_from_path(
                table_path,
                &parent,
                partition_fields,
                self.table.schema().partition_keys(),
                core_options.partition_default_name(),
                core_options.format_table_partition_only_value_in_path(),
            )?
            else {
                return Ok(None);
            };
            if !self.partition_matches(&partition)? {
                return Ok(None);
            }
            partition
        };

        Ok(Some(
            DataSplitBuilder::new()
                .with_snapshot(0)
                .with_partition(partition)
                .with_bucket(0)
                .with_bucket_path(parent)
                .with_total_buckets(1)
                .with_data_files(vec![data_file])
                .with_raw_convertible(true)
                .build()?,
        ))
    }

    fn partition_matches(&self, partition: &BinaryRow) -> crate::Result<bool> {
        match &self.partition_filter {
            Some(filter) => filter.matches_entry(&partition.to_serialized_bytes()),
            None => Ok(true),
        }
    }

    /// A limit of zero needs no split at all. A positive one keeps every split: a format
    /// table has no row counts, so the number of files says nothing about the number of
    /// rows, and dropping files would answer the query from a guess.
    ///
    /// Mirrors Java `FormatTableScan.FormatTableScanPlan.splits`.
    pub(crate) fn apply_limit_pushdown(
        &self,
        splits: Vec<crate::DataSplit>,
    ) -> Vec<crate::DataSplit> {
        match self.limit {
            Some(0) => Vec::new(),
            _ => splits,
        }
    }
}

#[derive(Debug, Clone)]
struct ScanRoot {
    path: String,
    partition: BinaryRow,
}

fn is_format_table_data_file_name(file_name: &str) -> bool {
    !file_name.is_empty() && !file_name.starts_with('.') && !file_name.starts_with('_')
}

/// Whether a listed file is, or lies inside, an entry whose name starts with `.` or `_` below
/// the partition directories, such as a committer staging tree (`_temporary`, `__magic_*`)
/// whose files may never be committed.
///
/// Partition directories are exempt: a value-only layout names the null partition
/// `__DEFAULT_PARTITION__`. `partition_levels_below_root` is how many partition levels still
/// lie under the scan root.
fn is_hidden_below_partitions(
    root_segments: &[&str],
    partition_levels_below_root: usize,
    file_path: &str,
) -> bool {
    let segments = path_segments(file_path);
    // A listing may spell the scheme differently from the root (`file:/` against `file:///`),
    // so what lies below the root is found by segments, not by string prefix. A path outside the
    // root is left to the file-name check.
    let Some(below_root) = segments.strip_prefix(root_segments) else {
        return false;
    };
    below_root
        .iter()
        .skip(partition_levels_below_root)
        .any(|segment| segment.starts_with('.') || segment.starts_with('_'))
}

/// The non-empty segments of a path, without its scheme.
fn path_segments(path: &str) -> Vec<&str> {
    let without_scheme = match path.find("://") {
        Some(index) => &path[index + 3..],
        None => path.split_once(':').map_or(path, |(_, rest)| rest),
    };
    without_scheme
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect()
}

fn split_parent_and_file(path: &str) -> Option<(&str, &str)> {
    let trimmed = path.trim_end_matches('/');
    let slash = trimmed.rfind('/')?;
    Some((&trimmed[..slash], &trimmed[slash + 1..]))
}

fn join_path(parent: &str, child: &str) -> String {
    let parent = parent.trim_end_matches('/');
    let child = child.trim_start_matches('/').trim_end_matches('/');
    if child.is_empty() {
        parent.to_string()
    } else {
        format!("{parent}/{child}")
    }
}

fn leading_equality_partition_path(
    table_path: &str,
    partition_keys: &[String],
    partition_fields: &[DataField],
    predicate: &Predicate,
    default_partition_name: &str,
    legacy_partition_name: bool,
    only_value_in_path: bool,
) -> Option<String> {
    let values = leading_equality_values_from_predicate(
        predicate,
        partition_fields,
        default_partition_name,
        legacy_partition_name,
    );
    if values.is_empty() {
        return None;
    }
    let segments = partition_keys
        .iter()
        .zip(&values)
        .map(|(key, value)| {
            if only_value_in_path {
                escape_path_name(value)
            } else {
                format!("{}={}", escape_path_name(key), escape_path_name(value))
            }
        })
        .collect::<Vec<_>>();
    Some(join_path(table_path, &segments.join("/")))
}

/// The part of a partition predicate that can be sent to the catalog as a filter.
///
/// An AND keeps the children that have a wire form, since leaving out a conjunct only widens
/// what the catalog may return; anything else is sent whole or not at all. Nothing is sent for a
/// predicate that selects everything.
///
/// Mirrors Java `FormatTableScan.extractPartitionPredicate`.
fn partition_filter_json(predicate: &Predicate) -> Option<serde_json::Value> {
    match predicate {
        Predicate::AlwaysTrue => None,
        Predicate::And(children) => {
            let mut pushed = children
                .iter()
                .filter(|child| child.to_rest_json().is_some())
                .cloned()
                .collect::<Vec<_>>();
            match pushed.len() {
                0 => None,
                1 => pushed.pop().and_then(|child| child.to_rest_json()),
                _ => Predicate::And(pushed).to_rest_json(),
            }
        }
        other => other.to_rest_json(),
    }
}

/// The leading run of partition values this filter pins to a single value, in
/// partition-key order, formatted the way the catalog and the partition path spell them.
///
/// Only a leading run is useful: a partition path prefix and the partition-name pattern a
/// catalog prunes on can express nothing else. A null value has no such spelling, so the
/// run stops there.
fn leading_equality_partition_values(
    filter: &PartitionFilter,
    partition_fields: &[DataField],
    default_partition_name: &str,
    legacy_partition_name: bool,
) -> crate::Result<Vec<String>> {
    match filter {
        PartitionFilter::Predicate(predicate) => Ok(leading_equality_values_from_predicate(
            predicate,
            partition_fields,
            default_partition_name,
            legacy_partition_name,
        )),
        // An enumerated partition set still pins a prefix whenever its rows agree on one,
        // which is what `dt = 'a' AND hh IN ('10', '11')` collapses to.
        PartitionFilter::PartitionSet { partitions, .. } => {
            let mut common: Option<Vec<String>> = None;
            for serialized in partitions {
                let row = BinaryRow::from_serialized_bytes(serialized)?;
                let mut values = Vec::with_capacity(partition_fields.len());
                for (index, field) in partition_fields.iter().enumerate() {
                    let Some(datum) = extract_datum(&row, index, field.data_type())? else {
                        break;
                    };
                    let Some(value) = format_partition_value(
                        &datum,
                        field.data_type(),
                        default_partition_name,
                        legacy_partition_name,
                    ) else {
                        break;
                    };
                    values.push(value);
                }
                common = Some(match common {
                    None => values,
                    Some(common) => common
                        .into_iter()
                        .zip(values)
                        .take_while(|(left, right)| left == right)
                        .map(|(left, _)| left)
                        .collect(),
                });
                if common.as_ref().is_some_and(Vec::is_empty) {
                    break;
                }
            }
            Ok(common.unwrap_or_default())
        }
    }
}

/// Mirrors Java `FormatTableScan.extractLeadingEqualityPartitionSpecWhenOnlyAnd`.
fn leading_equality_values_from_predicate(
    predicate: &Predicate,
    partition_fields: &[DataField],
    default_partition_name: &str,
    legacy_partition_name: bool,
) -> Vec<String> {
    let mut pinned: Vec<Option<&Datum>> = vec![None; partition_fields.len()];
    let predicates = predicate.clone().split_and();
    for predicate in &predicates {
        let Predicate::Leaf {
            index,
            op: PredicateOperator::Eq,
            literals,
            ..
        } = predicate
        else {
            continue;
        };
        if *index < pinned.len() {
            pinned[*index] = literals.first();
        }
    }

    let mut values = Vec::new();
    for (field, datum) in partition_fields.iter().zip(pinned) {
        let Some(datum) = datum else {
            break;
        };
        let Some(value) = format_partition_value(
            datum,
            field.data_type(),
            default_partition_name,
            legacy_partition_name,
        ) else {
            break;
        };
        values.push(value);
    }
    values
}

fn partition_path_from_row(
    row: &BinaryRow,
    partition_fields: &[DataField],
    default_partition_name: &str,
    legacy_partition_name: bool,
    only_value_in_path: bool,
) -> crate::Result<String> {
    let mut segments = Vec::with_capacity(partition_fields.len());
    for (idx, field) in partition_fields.iter().enumerate() {
        let value = match extract_datum(row, idx, field.data_type())? {
            None => default_partition_name.to_string(),
            Some(datum) => format_partition_value(
                &datum,
                field.data_type(),
                default_partition_name,
                legacy_partition_name,
            )
            .ok_or_else(|| crate::Error::Unsupported {
                message: format!(
                    "Format table partition path generation does not support type '{:?}'",
                    field.data_type()
                ),
            })?,
        };
        if only_value_in_path {
            segments.push(escape_path_name(&value));
        } else {
            segments.push(format!(
                "{}={}",
                escape_path_name(field.name()),
                escape_path_name(&value)
            ));
        }
    }
    Ok(segments.join("/"))
}

fn partition_row_from_path(
    table_path: &str,
    file_parent: &str,
    partition_fields: &[DataField],
    partition_keys: &[String],
    default_partition_name: &str,
    only_value_in_path: bool,
) -> crate::Result<Option<BinaryRow>> {
    let relative = match file_parent
        .trim_end_matches('/')
        .strip_prefix(table_path.trim_end_matches('/'))
    {
        Some(path) => path.trim_start_matches('/'),
        None => return Ok(None),
    };
    if relative.is_empty() {
        return Ok(None);
    }

    let mut values = Vec::with_capacity(partition_keys.len());
    if only_value_in_path {
        for segment in relative
            .split('/')
            .filter(|segment| !segment.is_empty())
            .take(partition_keys.len())
        {
            let Some(value) = unescape_path_name(segment) else {
                return Ok(None);
            };
            values.push(value);
        }
        if values.len() != partition_keys.len() {
            return Ok(None);
        }
    } else {
        for key in partition_keys {
            let Some(value) = relative
                .split('/')
                .find_map(|segment| partition_segment_value(segment, key))
            else {
                return Ok(None);
            };
            values.push(value);
        }
    }

    let mut builder = BinaryRowBuilder::new(partition_fields.len() as i32);
    for (idx, value) in values.iter().enumerate() {
        if value == default_partition_name {
            builder.set_null_at(idx);
            continue;
        }
        let Some(datum) = parse_format_partition_value(value, partition_fields[idx].data_type())
        else {
            return Ok(None);
        };
        builder.write_datum(idx, &datum, partition_fields[idx].data_type());
    }
    Ok(Some(builder.build()))
}

fn partition_row_from_catalog_spec(
    spec: &HashMap<String, String>,
    partition_fields: &[DataField],
    partition_keys: &[String],
    default_partition_name: &str,
) -> crate::Result<BinaryRow> {
    let mut builder = BinaryRowBuilder::new(partition_fields.len() as i32);
    for (index, (key, field)) in partition_keys.iter().zip(partition_fields).enumerate() {
        let value = spec.get(key).ok_or_else(|| crate::Error::DataInvalid {
            message: format!("Catalog partition is missing column '{key}'"),
            source: None,
        })?;
        if value == default_partition_name {
            builder.set_null_at(index);
            continue;
        }
        let datum = parse_format_partition_value(value, field.data_type()).ok_or_else(|| {
            crate::Error::DataInvalid {
                message: format!(
                    "Invalid catalog partition value {value:?} for column '{key}' with type {:?}",
                    field.data_type()
                ),
                source: None,
            }
        })?;
        builder.write_datum(index, &datum, field.data_type());
    }
    Ok(builder.build())
}

fn partition_segment_value(segment: &str, key: &str) -> Option<String> {
    let (segment_key, segment_value) = segment.split_once('=')?;
    if unescape_path_name(segment_key)? == key {
        unescape_path_name(segment_value)
    } else {
        None
    }
}

fn unescape_path_name(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return None;
            }
            let hi = hex_value(bytes[i + 1])?;
            let lo = hex_value(bytes[i + 2])?;
            out.push((hi << 4) | lo);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn supported_format_table_formats() -> Vec<&'static str> {
    vec![
        "parquet",
        "orc",
        "avro",
        "row",
        "mosaic",
        #[cfg(feature = "vortex")]
        "vortex",
    ]
}

fn supported_format_table_extension(format: &str) -> crate::Result<&'static str> {
    match format.to_ascii_lowercase().as_str() {
        "parquet" => Ok(".parquet"),
        "orc" => Ok(".orc"),
        "avro" => Ok(".avro"),
        "row" => Ok(".row"),
        "mosaic" => Ok(".mosaic"),
        #[cfg(feature = "vortex")]
        "vortex" => Ok(".vortex"),
        other => Err(crate::Error::Unsupported {
            message: format!(
                "Format table file.format '{other}' is not supported by the Rust reader yet, \
                 expected one of: {}",
                supported_format_table_formats().join(", ")
            ),
        }),
    }
}

fn data_file_meta(file_name: String, file_size: i64, schema_id: i64) -> DataFileMeta {
    DataFileMeta {
        file_name,
        file_size,
        row_count: DataFileMeta::ROW_COUNT_UNKNOWN,
        min_key: Vec::new(),
        max_key: Vec::new(),
        key_stats: BinaryTableStats::empty(),
        value_stats: BinaryTableStats::empty(),
        min_sequence_number: 0,
        max_sequence_number: 0,
        schema_id,
        level: 0,
        extra_files: Vec::new(),
        creation_time: None,
        delete_row_count: Some(0),
        embedded_index: None,
        file_source: None,
        value_stats_cols: None,
        external_path: None,
        first_row_id: None,
        write_cols: None,
        column_max_sequence_numbers: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Identifier;
    use crate::io::FileIOBuilder;
    use crate::spec::{IntType, Schema, TableSchema, VarCharType};
    use bytes::Bytes;
    use std::collections::HashSet;

    /// A Parquet format table in memory, partitioned by the given string keys.
    fn format_table(location: &str, partition_keys: &[&str], options: &[(&str, &str)]) -> Table {
        let mut builder = Schema::builder();
        for key in partition_keys {
            builder = builder.column(*key, DataType::VarChar(VarCharType::string_type()));
        }
        builder = builder
            .column("id", DataType::Int(IntType::new()))
            .partition_keys(partition_keys.iter().map(|key| key.to_string()))
            .option("type", "format-table")
            .option("file.format", "parquet");
        for (key, value) in options {
            builder = builder.option(*key, *value);
        }
        Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("default", "format_t"),
            location.to_string(),
            TableSchema::new(0, &builder.build().unwrap()),
            None,
        )
    }

    /// Planning only lists files, so their content never has to be valid Parquet.
    async fn write_files(table: &Table, relative_paths: &[&str]) {
        for relative_path in relative_paths {
            let path = format!("{}/{relative_path}", table.location().trim_end_matches('/'));
            table
                .file_io()
                .new_output(&path)
                .unwrap()
                .write(Bytes::from_static(b"planned, never read"))
                .await
                .unwrap();
        }
    }

    /// The planned files, relative to the table directory, in plan order.
    async fn planned_files(table: &Table, filter: Option<PartitionFilter>) -> Vec<String> {
        let table_prefix = format!("{}/", table.location().trim_end_matches('/'));
        let plan = FormatTableScan::new(table, filter, None, None)
            .plan()
            .await
            .unwrap();
        plan.splits()
            .iter()
            .map(|split| {
                let path = format!(
                    "{}/{}",
                    split.bucket_path(),
                    split.data_files()[0].file_name
                );
                path.strip_prefix(&table_prefix)
                    .map_or(path.clone(), str::to_string)
            })
            .collect()
    }

    /// A filter naming exactly the given partitions; `None` is a null partition value.
    fn partition_set(table: &Table, partitions: &[&[Option<&str>]]) -> PartitionFilter {
        let partition_fields = table.schema().partition_fields();
        let partitions = partitions
            .iter()
            .map(|values| {
                let mut builder = BinaryRowBuilder::new(values.len() as i32);
                for (index, value) in values.iter().enumerate() {
                    match value {
                        Some(value) => builder.write_datum(
                            index,
                            &Datum::String(value.to_string()),
                            partition_fields[index].data_type(),
                        ),
                        None => builder.set_null_at(index),
                    }
                }
                builder.build_serialized()
            })
            .collect::<HashSet<_>>();
        PartitionFilter::from_partition_set(partitions, &partition_fields).unwrap()
    }

    #[test]
    fn test_hidden_names_count_only_below_the_partition_levels() {
        let table_root = path_segments("file:///warehouse/db.db/t");
        assert!(!is_hidden_below_partitions(
            &table_root,
            2,
            "file:/warehouse/db.db/t/__DEFAULT_PARTITION__/b/part-0.parquet"
        ));
        assert!(is_hidden_below_partitions(
            &table_root,
            2,
            "file:/warehouse/db.db/t/a/b/_temporary/0/part-0.parquet"
        ));
        assert!(is_hidden_below_partitions(
            &table_root,
            2,
            "file:/warehouse/db.db/t/a/b/.part-0.parquet"
        ));

        // A root one level down, as a leading-equality scan plans it, has one partition level left.
        let leading_root = path_segments("file:///warehouse/db.db/t/dt=a");
        assert!(!is_hidden_below_partitions(
            &leading_root,
            1,
            "file:/warehouse/db.db/t/dt=a/hh=__DEFAULT_PARTITION__/part-0.parquet"
        ));
        assert!(is_hidden_below_partitions(
            &leading_root,
            1,
            "file:/warehouse/db.db/t/dt=a/hh=1/__magic_job/part-0.parquet"
        ));

        let partition_root = path_segments("file:///warehouse/db.db/t/dt=a/hh=1");
        assert!(!is_hidden_below_partitions(
            &partition_root,
            0,
            "file:/warehouse/db.db/t/dt=a/hh=1/part-0.parquet"
        ));
        assert!(!is_hidden_below_partitions(
            &partition_root,
            0,
            "file:/elsewhere/_temporary/part-0.parquet"
        ));
    }

    #[tokio::test]
    async fn test_scan_skips_committer_staging_files_of_an_unpartitioned_table() {
        let table = format_table("memory:/staging_unpartitioned", &[], &[]);
        write_files(
            &table,
            &[
                "part-0.parquet",
                "_temporary/0/_temporary/attempt_0/part-1.parquet",
                "__magic_job_1/tasks/part-2.parquet",
                ".spark-staging-1/part-3.parquet",
            ],
        )
        .await;

        assert_eq!(planned_files(&table, None).await, vec!["part-0.parquet"]);
    }

    #[tokio::test]
    async fn test_scan_skips_committer_staging_files_below_a_partition_directory() {
        let table = format_table("memory:/staging_partitioned", &["dt"], &[]);
        write_files(
            &table,
            &[
                "dt=a/part-0.parquet",
                "dt=a/_temporary/0/_temporary/attempt_0/part-1.parquet",
                "dt=a/__magic_job_1/tasks/part-2.parquet",
                "dt=b/part-3.parquet",
            ],
        )
        .await;

        assert_eq!(
            planned_files(&table, None).await,
            vec!["dt=a/part-0.parquet", "dt=b/part-3.parquet"]
        );
        let only_a = partition_set(&table, &[&[Some("a")]]);
        assert_eq!(
            planned_files(&table, Some(only_a)).await,
            vec!["dt=a/part-0.parquet"]
        );
    }

    #[tokio::test]
    async fn test_scan_still_reads_a_value_only_default_partition_directory() {
        let table = format_table(
            "memory:/staging_value_only",
            &["dt"],
            &[("format-table.partition-path-only-value", "true")],
        );
        write_files(
            &table,
            &[
                "__DEFAULT_PARTITION__/part-0.parquet",
                "__DEFAULT_PARTITION__/_temporary/0/part-1.parquet",
                "b/part-2.parquet",
            ],
        )
        .await;

        assert_eq!(
            planned_files(&table, None).await,
            vec!["__DEFAULT_PARTITION__/part-0.parquet", "b/part-2.parquet"]
        );
        let null_partition = partition_set(&table, &[&[None]]);
        assert_eq!(
            planned_files(&table, Some(null_partition)).await,
            vec!["__DEFAULT_PARTITION__/part-0.parquet"]
        );
    }

    #[tokio::test]
    async fn test_concurrent_listing_keeps_the_plan_order() {
        let partitions = ["a", "b", "c", "d", "e", "f", "g", "h"];
        // Written in reverse, so the plan order owes nothing to the order of the writes.
        let files = partitions
            .iter()
            .rev()
            .flat_map(|dt| {
                [
                    format!("dt={dt}/part-1.parquet"),
                    format!("dt={dt}/part-0.parquet"),
                ]
            })
            .collect::<Vec<_>>();
        let file_names = files.iter().map(String::as_str).collect::<Vec<_>>();
        let expected = partitions
            .iter()
            .flat_map(|dt| {
                [
                    format!("dt={dt}/part-0.parquet"),
                    format!("dt={dt}/part-1.parquet"),
                ]
            })
            .collect::<Vec<_>>();
        let rows = partitions.iter().map(|dt| [Some(*dt)]).collect::<Vec<_>>();
        let every_partition = rows.iter().map(|row| row.as_slice()).collect::<Vec<_>>();

        for parallelism in ["1", "4"] {
            let table = format_table(
                &format!("memory:/listing_order_{parallelism}"),
                &["dt"],
                &[("format-table.scan.list-parallelism", parallelism)],
            );
            write_files(&table, &file_names).await;

            let filter = partition_set(&table, &every_partition);
            assert_eq!(
                planned_files(&table, Some(filter)).await,
                expected,
                "one listing per partition, parallelism {parallelism}"
            );
            assert_eq!(
                planned_files(&table, None).await,
                expected,
                "one listing of the table, parallelism {parallelism}"
            );
        }
    }

    #[test]
    fn test_unsupported_format_lists_the_supported_ones() {
        let error = supported_format_table_extension("csv").unwrap_err();
        let crate::Error::Unsupported { message } = error else {
            panic!("expected Unsupported, got {error:?}");
        };
        assert!(message.contains("'csv'"), "{message}");
        for format in supported_format_table_formats() {
            assert!(message.contains(format), "{format} missing from {message}");
        }
    }

    #[test]
    fn test_supported_formats_are_accepted_case_insensitively() {
        for format in supported_format_table_formats() {
            let expected = format!(".{format}");
            assert_eq!(supported_format_table_extension(format).unwrap(), expected);
            assert_eq!(
                supported_format_table_extension(&format.to_ascii_uppercase()).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn test_partition_filter_json_sends_what_can_only_widen_the_result() {
        use crate::spec::{DateType, PredicateBuilder};

        let fields = vec![
            DataField::new(
                0,
                "dt".to_string(),
                DataType::VarChar(VarCharType::default()),
            ),
            DataField::new(1, "hh".to_string(), DataType::Int(IntType::new())),
            DataField::new(2, "day".to_string(), DataType::Date(DateType::new())),
        ];
        let builder = PredicateBuilder::new(&fields);
        let dt = builder.equal("dt", Datum::String("a".to_string())).unwrap();
        let hh = builder.greater_than("hh", Datum::Int(10)).unwrap();
        let day = builder.equal("day", Datum::Date(20_656)).unwrap();
        let sent = |predicate: &Predicate| {
            partition_filter_json(predicate).map(|json| {
                Predicate::from_rest_json(&json.to_string(), &fields)
                    .unwrap()
                    .to_string()
            })
        };

        // A DATE literal has no wire form, so the AND goes without it.
        assert_eq!(
            sent(&Predicate::and(vec![dt.clone(), hh.clone(), day.clone()])),
            Some(Predicate::and(vec![dt.clone(), hh]).to_string())
        );
        assert_eq!(
            sent(&Predicate::and(vec![dt.clone(), day.clone()])),
            Some(dt.to_string())
        );
        // Leaving out a child of an OR would narrow it, so it is sent whole or not at all.
        assert_eq!(sent(&Predicate::or(vec![dt.clone(), day])), None);
        assert_eq!(sent(&Predicate::Not(Box::new(dt))), None);
        assert_eq!(sent(&Predicate::AlwaysTrue), None);
    }

    #[test]
    fn test_leading_equality_path_escapes_values_as_partition_paths_do() {
        use crate::spec::PredicateBuilder;

        let fields = vec![DataField::new(
            0,
            "dt".to_string(),
            DataType::VarChar(VarCharType::default()),
        )];
        let keys = vec!["dt".to_string()];
        let path = |value: &str, only_value_in_path: bool| {
            let predicate = PredicateBuilder::new(&fields)
                .equal("dt", Datum::String(value.to_string()))
                .unwrap();
            leading_equality_partition_path(
                "memory:/t",
                &keys,
                &fields,
                &predicate,
                "__DEFAULT_PARTITION__",
                false,
                only_value_in_path,
            )
        };

        // Only the characters Java escapes are escaped; a non-ASCII value is written as it is,
        // as the directory a writer creates for it.
        assert_eq!(
            path("2026/07", false).as_deref(),
            Some("memory:/t/dt=2026%2F07")
        );
        assert_eq!(path("中文", false).as_deref(), Some("memory:/t/dt=中文"));
        assert_eq!(path("中文", true).as_deref(), Some("memory:/t/中文"));
    }
}
