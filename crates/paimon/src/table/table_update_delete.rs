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

//! Predicate deletes prepare changes against one captured snapshot.

use std::collections::HashMap;

use futures::TryStreamExt;
use indexmap::IndexMap;

use super::partition_filter::PartitionFilter;
use super::{CommitMessage, DataEvolutionDeleteWriter, Table};
use crate::spec::{field_idx_to_partition_idx, FileKind, IndexManifest, Predicate, Snapshot};

pub(super) async fn delete(
    table: &Table,
    predicate: Option<Predicate>,
) -> crate::Result<Vec<CommitMessage>> {
    super::write_builder::ensure_table_write_allowed(table)?;
    // Partition-only deletes use manifest metadata directly, but still need
    // the same unrestricted whole-table grant as row-level deletes. Authorize
    // before the empty-target and partition-optimization branches.
    let _authorized = table
        .new_read_builder()
        .new_scan()
        .authorize_all_files()
        .await?;
    if let Some(predicate) = &predicate {
        validate_predicate(predicate, &table.schema().fields_with_read_metadata())?;
    }

    let mapping =
        field_idx_to_partition_idx(table.schema().fields(), table.schema().partition_keys());
    let partition_predicate = predicate.as_ref().and_then(|predicate| {
        (!table.schema().partition_keys().is_empty())
            .then(|| predicate.remap_field_index(&mapping))
            .flatten()
    });
    // Data predicates require deletion vectors even when the table is empty or
    // no rows match. Partition-only deletes remove complete file/index metadata.
    if partition_predicate.is_none() {
        let _ = DataEvolutionDeleteWriter::new(table)?;
    }
    let Some(snapshot) = super::time_travel::resolve_snapshot(table).await? else {
        return Ok(Vec::new());
    };
    if let Some(predicate) = partition_predicate {
        return delete_partitions(table, &snapshot, predicate).await;
    }

    // FULL includes unindexed rows in PARTIAL/DETAIL search mode. Both the
    // predicate reader and the DV writer must keep this exact snapshot.
    let scan_table = table
        .copy_with_options(HashMap::from([(
            "scalar-index.search-mode".into(),
            "FULL".into(),
        )]))
        .copy_with_pinned_snapshot(Some(&snapshot));
    let mut builder = scan_table.new_read_builder();
    builder.with_projection(&["_ROW_ID"])?;
    if let Some(predicate) = predicate {
        builder.with_filter(predicate);
    }
    let reader = builder.new_read()?.for_update();
    let plan = builder.new_scan().plan_all_files().await?;
    let mut writer = DataEvolutionDeleteWriter::new(&scan_table)?;
    let mut batches = reader.to_arrow(plan.splits())?;
    while let Some(batch) = batches.try_next().await? {
        writer.add_matched_batch(batch)?;
    }
    writer.prepare_commit().await
}

fn validate_predicate(
    predicate: &Predicate,
    fields: &[crate::spec::DataField],
) -> crate::Result<()> {
    match predicate {
        Predicate::NestedLeaf { .. } => predicate.validate_nested_fields(fields)?,
        Predicate::Leaf {
            column,
            index,
            data_type,
            ..
        } => {
            if !fields
                .get(*index)
                .is_some_and(|field| field.name() == column && field.data_type() == data_type)
            {
                return Err(crate::Error::DataInvalid {
                    message: format!("Delete predicate field '{column}' does not match the table schema at index {index}"),
                    source: None,
                });
            }
        }
        Predicate::And(children) | Predicate::Or(children) => {
            for child in children {
                validate_predicate(child, fields)?;
            }
        }
        Predicate::Not(child) => validate_predicate(child, fields)?,
        Predicate::AlwaysTrue | Predicate::AlwaysFalse => {}
    }
    Ok(())
}

async fn delete_partitions(
    table: &Table,
    snapshot: &Snapshot,
    predicate: Predicate,
) -> crate::Result<Vec<CommitMessage>> {
    let partition_fields = table.schema().partition_fields();
    let filter = PartitionFilter::from_predicate(predicate, &partition_fields);
    let manager = table.snapshot_manager();
    let mut messages = IndexMap::new();
    for entry in
        super::snapshot_manifest::read_snapshot_entries(&manager, snapshot, Some(&filter)).await?
    {
        partition_message(
            &mut messages,
            entry.partition(),
            entry.bucket(),
            snapshot.id(),
        )
        .deleted_files
        .push(entry.file().clone());
    }
    if let Some(index_manifest) = snapshot.index_manifest() {
        for entry in
            IndexManifest::read(table.file_io(), &manager.manifest_path(index_manifest)).await?
        {
            if entry.kind == FileKind::Add && filter.matches_entry(&entry.partition)? {
                partition_message(&mut messages, &entry.partition, entry.bucket, snapshot.id())
                    .deleted_index_files
                    .push(entry.index_file);
            }
        }
    }
    Ok(messages.into_values().collect())
}

fn partition_message<'a>(
    messages: &'a mut IndexMap<(Vec<u8>, i32), CommitMessage>,
    partition: &[u8],
    bucket: i32,
    snapshot_id: i64,
) -> &'a mut CommitMessage {
    messages
        .entry((partition.to_vec(), bucket))
        .or_insert_with(|| {
            let mut message = CommitMessage::new(partition.to_vec(), bucket, Vec::new());
            message.check_from_snapshot = Some(snapshot_id);
            message
        })
}
