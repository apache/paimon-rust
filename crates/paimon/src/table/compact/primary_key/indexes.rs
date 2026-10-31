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

//! Execute source-backed index maintenance for primary-key compaction.

mod writer;

#[cfg(test)]
mod tests;

use super::super::CompactTask;
use crate::resource::ResourceContext;
use crate::spec::{DataFileMeta, FileKind, IndexManifestEntry, ManifestEntry};
use crate::table::pk_index_maintenance::{levels, plan, unsupported_scalar_units};
use crate::table::pk_index_options::definitions;
use crate::table::{CommitMessage, DataSplit, Table, TableCommit};
use crate::Result;

pub(in crate::table::compact) async fn prepare(
    table: &Table,
    task: &CompactTask,
    split: &DataSplit,
    indexes: &[IndexManifestEntry],
    active: &[ManifestEntry],
    files: &[DataFileMeta],
    resources: Option<ResourceContext>,
) -> Result<Vec<IndexManifestEntry>> {
    let defs = definitions(table.schema())?;
    let changes = task
        .files
        .iter()
        .map(|f| {
            ManifestEntry::new(
                FileKind::Delete,
                task.partition.clone(),
                task.bucket,
                task.total_buckets,
                f.clone(),
                2,
            )
        })
        .chain(files.iter().map(|f| {
            ManifestEntry::new(
                FileKind::Add,
                task.partition.clone(),
                task.bucket,
                task.total_buckets,
                f.clone(),
                2,
            )
        }))
        .collect::<Vec<_>>();
    let after = levels(active, &changes, &task.partition, task.bucket);
    let unsupported = unsupported_scalar_units(table, &after, &defs).await;
    let maintenance = plan(
        table,
        indexes,
        &after,
        &defs,
        &unsupported,
        &task.partition,
        task.bucket,
    )?;
    let mut entries = maintenance.deletes;
    for (level, definition) in maintenance.builds {
        match writer::build(table, split, &after[&level], definition, resources.clone()).await {
            Ok(Some(index_file)) => entries.push(IndexManifestEntry {
                kind: FileKind::Add,
                partition: task.partition.clone(),
                bucket: task.bucket,
                index_file,
                version: 1,
            }),
            Ok(None) => {}
            Err(error) => {
                abort(table, &entries).await;
                return Err(error);
            }
        }
    }
    Ok(entries)
}

pub(in crate::table) async fn abort(table: &Table, entries: &[IndexManifestEntry]) {
    let messages = entries
        .iter()
        .filter(|e| e.kind == FileKind::Add)
        .map(|e| {
            let mut message = CommitMessage::new(e.partition.clone(), e.bucket, vec![]);
            message.new_index_files.push(e.index_file.clone());
            message
        })
        .collect::<Vec<_>>();
    let _ = TableCommit::new(table.clone(), "pk-index-cleanup".into())
        .abort(&messages)
        .await;
}
