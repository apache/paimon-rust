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

//! Predicate matching and assignment orchestration for TableUpdate.

use std::collections::{HashMap, HashSet};

use arrow_array::RecordBatch;
use futures::TryStreamExt;

use super::data_evolution_writer::RowIdFileIndex;
use super::stats_filter::group_by_overlapping_row_id;
use super::{
    CommitMessage, DataEvolutionWriter, DataSplit, DataSplitBuilder, Table, TableUpdateByRowId,
    UpdateAssignment,
};
use crate::spec::{CoreOptions, Predicate};

fn invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

fn validate(
    table: &Table,
    assignments: &[(String, UpdateAssignment)],
    read_columns: &[String],
) -> crate::Result<bool> {
    let options = CoreOptions::new(table.schema().options());
    if !options.data_evolution_enabled() || !options.row_tracking_enabled() {
        return Err(invalid(
            "update_by_predicate requires data-evolution.enabled and row-tracking.enabled",
        ));
    }
    if assignments.is_empty() {
        return Err(invalid("assignments must not be empty"));
    }
    let callable = assignments
        .iter()
        .any(|(_, value)| matches!(value, UpdateAssignment::Function(_)));
    let array = assignments
        .iter()
        .any(|(_, value)| matches!(value, UpdateAssignment::Array(_)));
    if !callable && !read_columns.is_empty() {
        return Err(invalid("read_columns requires a callable assignment"));
    }
    if callable && array {
        return Err(invalid(
            "Callable assignments cannot be combined with Arrow array assignments",
        ));
    }
    if callable && read_columns.is_empty() {
        return Err(invalid("Callable assignments require read_columns"));
    }
    let fields = table.schema().fields();
    for name in read_columns {
        if !fields.iter().any(|field| field.name() == name) {
            return Err(invalid(format!(
                "Read column {name} is not in table schema"
            )));
        }
    }
    let mut names = HashSet::new();
    for (name, _) in assignments {
        if !fields.iter().any(|field| field.name() == name) {
            return Err(invalid(format!("Column {name} is not in table schema")));
        }
        if !names.insert(name) {
            return Err(invalid(format!("Duplicate assignment column {name}")));
        }
        if table.schema().partition_keys().contains(name) {
            return Err(invalid(format!(
                "update_by_predicate does not support updating partition column '{name}'"
            )));
        }
    }
    // Validate writer support before any caller-provided functions can run.
    let _ = DataEvolutionWriter::new(
        table,
        assignments.iter().map(|(name, _)| name.clone()).collect(),
    )?;
    options.ensure_read_authorized()?;
    Ok(array)
}

pub(super) async fn update(
    table: &Table,
    commit_user: &str,
    predicate: Option<Predicate>,
    assignments: Vec<(String, UpdateAssignment)>,
    read_columns: Vec<String>,
) -> crate::Result<Vec<CommitMessage>> {
    let combine_all = validate(table, &assignments, &read_columns)?;
    let Some(snapshot) = super::time_travel::resolve_snapshot(table).await? else {
        return Ok(Vec::new());
    };
    // Updates must include unindexed rows even when normal reads request the
    // partial/detail scalar-index search mode.
    let scan_table = table
        .copy_with_options(HashMap::from([
            ("scalar-index.search-mode".into(), "FULL".into()),
            ("scan.mode".into(), "default".into()),
        ]))
        .copy_with_pinned_snapshot(&snapshot);
    let mut read_builder = scan_table.new_read_builder();
    let mut projection = Vec::new();
    for name in &read_columns {
        if !projection.contains(&name.as_str()) {
            projection.push(name.as_str());
        }
    }
    projection.push("_ROW_ID");
    read_builder.with_projection(&projection)?;
    if let Some(predicate) = predicate {
        read_builder.with_filter(predicate);
    }
    // Rewrites need all existing column versions, including columns outside
    // the callback/predicate projection, to preserve values of unmatched rows.
    let plan = read_builder.new_scan().with_scan_all_files().plan().await?;
    let index = RowIdFileIndex::from_splits(scan_table.clone(), plan.splits())?;
    let groups = ordered_file_groups(plan.splits())?;
    let mut updater = TableUpdateByRowId::with_index(table, commit_user.into(), index)?;
    let reader = read_builder.new_read()?;
    let schema = crate::arrow::build_target_arrow_schema(table.schema().fields())?;
    let columns: Vec<_> = assignments.iter().map(|(name, _)| name.clone()).collect();
    let result = async {
        if combine_all {
            let matched: Vec<RecordBatch> = reader.to_arrow(&groups)?.try_collect().await?;
            let updates =
                super::update_assignment::assigned_batches(&matched, assignments, schema)?;
            if !updates.is_empty() {
                updater.update_columns(updates, columns).await?;
            }
        } else {
            for group in groups {
                let matched: Vec<RecordBatch> = reader.to_arrow(&[group])?.try_collect().await?;
                let updates = super::update_assignment::assigned_batches(
                    &matched,
                    assignments.clone(),
                    schema.clone(),
                )?;
                if !updates.is_empty() {
                    updater.update_columns(updates, columns.clone()).await?;
                }
            }
        }
        Ok(updater.commit_messages().to_vec())
    }
    .await;
    if result.is_err() {
        let _ = updater.abort().await;
    }
    result
}

/// PyPaimon orders logical groups by row ID within each partition/bucket.
/// Ordinary Rust scans can put multi-file groups before singletons; using that
/// order would attach positional array assignments to different target rows.
fn ordered_file_groups(splits: &[DataSplit]) -> crate::Result<Vec<DataSplit>> {
    let mut buckets = indexmap::IndexMap::<_, Vec<DataSplit>>::new();
    for split in splits {
        buckets
            .entry((split.partition().to_serialized_bytes(), split.bucket()))
            .or_default()
            .extend(file_groups(split)?);
    }
    let mut groups = Vec::new();
    for mut bucket in buckets.into_values() {
        bucket.sort_by_key(|split| {
            split
                .data_files()
                .iter()
                .filter_map(|file| file.first_row_id)
                .min()
        });
        groups.extend(bucket);
    }
    Ok(groups)
}

/// Preserve deletion vectors and index-selected row ranges while ensuring a
/// callback sees exactly one logical row-ID file group (base plus deltas).
fn file_groups(split: &DataSplit) -> crate::Result<Vec<DataSplit>> {
    group_by_overlapping_row_id(split.data_files().to_vec())
        .into_iter()
        .map(|files| {
            let mut builder = DataSplitBuilder::new()
                .with_snapshot(split.snapshot_id())
                .with_partition(split.partition().clone())
                .with_bucket(split.bucket())
                .with_bucket_path(split.bucket_path().into())
                .with_total_buckets(split.total_buckets())
                .with_raw_convertible(files.len() == 1);
            if split.data_deletion_files().is_some() {
                builder = builder.with_data_deletion_files(
                    files
                        .iter()
                        .map(|file| split.deletion_file_for_data_file(file).cloned())
                        .collect(),
                );
            }
            if let Some(ranges) = split.row_ranges() {
                builder = builder.with_row_ranges(ranges.to_vec());
            }
            builder.with_data_files(files).build()
        })
        .collect()
}
