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

//! Materialize the replacement state before publishing a DV lookup overwrite.
//! All rows in a replacement bucket are available here, before competing
//! appends can introduce L0 files with pending changelogs.

use std::collections::BTreeMap;

use crate::spec::{
    BinaryRow, DataFileMeta, FileKind, IndexManifestEntry, ManifestEntry, EMPTY_SERIALIZED_ROW,
};
use crate::table::index_file_path::committed_index_file_path;
use crate::table::merge_tree_split_generator::{interval_partition, KeyComparator};
use crate::table::pk_index_maintenance::{is_source_index, source, MaintenanceRound};
use crate::table::universal_compaction::{integer_option, UniversalCompaction};
use crate::table::{DataSplit, Table};
use crate::Result;

pub(in crate::table) async fn materialize(
    table: &Table,
    snapshot_id: i64,
    entries: &mut Vec<ManifestEntry>,
    indexes: &mut Vec<IndexManifestEntry>,
    created_files: &mut Vec<String>,
) -> Result<bool> {
    let options = table.schema().core_options();
    super::super::validate_dv_schemas(
        table,
        entries
            .iter()
            .filter(|e| *e.kind() == FileKind::Add)
            .map(ManifestEntry::file),
    )
    .await?;
    let mut buckets = BTreeMap::new();
    let mut replacement = Vec::new();
    for entry in std::mem::take(entries) {
        if *entry.kind() == FileKind::Delete {
            replacement.push(entry);
        } else {
            let (partition, bucket, total, file) = entry.into_parts();
            let partition = if table.schema().partition_keys().is_empty() {
                EMPTY_SERIALIZED_ROW.clone()
            } else {
                partition
            };
            let (expected_total, files) = buckets
                .entry((partition, bucket))
                .or_insert_with(|| (total, Vec::new()));
            if *expected_total != total {
                return Err(super::super::invalid(
                    "Overwrite files in one bucket have inconsistent total bucket counts",
                ));
            }
            files.push(file);
        }
    }

    let strategy = UniversalCompaction::from_options(table.schema().options(), "")?;
    let level = integer_option(
        table.schema().options(),
        "num-levels",
        strategy.trigger.saturating_add(1),
        2,
    )? - 1;
    let comparator = KeyComparator::from_table_schema(table.schema());
    let allow_upgrade = table
        .schema()
        .options()
        .get("overwrite-upgrade")
        .is_none_or(|value| value.eq_ignore_ascii_case("true"))
        && crate::table::pk_index_options::definitions(table.schema())?.is_empty();
    let round = MaintenanceRound::default();
    let mut indexes_changed = false;
    let mut by_bucket = BTreeMap::<_, Vec<_>>::new();
    for index in std::mem::take(indexes) {
        let partition = if table.schema().partition_keys().is_empty() {
            EMPTY_SERIALIZED_ROW.clone()
        } else {
            index.partition.clone()
        };
        by_bucket
            .entry((partition, index.bucket))
            .or_default()
            .push(index);
    }
    for ((partition, bucket), (total, files)) in buckets {
        let add =
            |file| ManifestEntry::new(FileKind::Add, partition.clone(), bucket, total, file, 2);
        if !files.iter().any(|file| file.level == 0) {
            replacement.extend(files.into_iter().map(add));
            continue;
        }
        for file in &files {
            super::super::validate_file(file, true)?;
        }
        let bucket_path = table.data_file_bucket_path(&partition, bucket)?;
        let split = DataSplit::builder()
            .with_snapshot(snapshot_id)
            .with_partition(BinaryRow::from_serialized_bytes(&partition)?)
            .with_bucket(bucket)
            .with_total_buckets(total)
            .with_bucket_path(bucket_path.clone())
            .with_data_files(files)
            .build()?;
        let mut bucket_indexes = by_bucket
            .remove(&(partition.clone(), bucket))
            .unwrap_or_default();
        if allow_upgrade
            && can_upgrade(split.data_files(), comparator.as_ref(), table.schema().id())
            && bucket_indexes
                .iter()
                .all(|e| e.index_file.index_type == "HASH")
        {
            replacement.extend(split.data_files().iter().cloned().map(|mut file| {
                file.level = level;
                add(file)
            }));
            indexes.extend(bucket_indexes);
            continue;
        }
        for index in &mut bucket_indexes {
            if is_source_index(index) {
                source(index)?;
            } else if index.index_file.index_type == "DELETION_VECTORS" {
                index.partition.clone_from(&partition);
            }
        }
        let masked =
            super::deletion_vectors::with_deletion_entries(table, &split, &bucket_indexes)?;
        let (outputs, _) = super::execute(table, level, &masked, None, None, false).await?;
        created_files.extend(outputs.iter().flat_map(|f| f.collect_files(&bucket_path)));
        // New positive-level files and their source-backed indexes become
        // visible together. Hash bucket assignments survive the replacement.
        let prepared =
            super::indexes::prepare(table, &split, &[], &[], &outputs, &[], None, &round).await?;
        let old_len = bucket_indexes.len();
        bucket_indexes
            .retain(|e| !is_source_index(e) && e.index_file.index_type != "DELETION_VECTORS");
        indexes_changed |= bucket_indexes.len() != old_len || !prepared.is_empty();
        indexes.extend(bucket_indexes);
        for entry in prepared {
            created_files.push(committed_index_file_path(
                table.location(),
                &bucket_path,
                options.index_file_in_data_file_dir(),
                &entry.index_file,
            ));
            indexes.push(entry);
        }
        replacement.extend(outputs.into_iter().map(add));
    }
    indexes.extend(by_bucket.into_values().flatten());
    *entries = replacement;
    Ok(indexes_changed)
}

fn can_upgrade(files: &[DataFileMeta], comparator: Option<&KeyComparator>, schema_id: i64) -> bool {
    files.iter().all(|file| {
        file.level == 0 && file.schema_id == schema_id && file.delete_row_count == Some(0)
    }) && (files.len() == 1
        || comparator.is_some_and(|cmp| {
            interval_partition(files.to_vec(), cmp)
                .iter()
                .all(|section| section.len() == 1)
        }))
}
