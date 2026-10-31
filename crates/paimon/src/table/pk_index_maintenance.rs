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

//! Shared source planning and commit validation for primary-key index maintenance.
//!
//! Compaction builds the planned units. Commit validation checks the same source
//! and compatibility rules against captured and current snapshot metadata.

use crate::spec::{
    should_read_pk_index_source, DataFileMeta, FileKind, IndexManifest, IndexManifestEntry,
    ManifestEntry, PrimaryKeyIndexSourceFile, PrimaryKeyIndexSourceMeta, Snapshot,
};
use crate::table::pk_index_options::{definitions, native_options, Definition};
use crate::table::Table;
use crate::{Error, Result};
use std::collections::{BTreeMap, HashSet};

type Unit = (Vec<u8>, i32, i32, i32);
type Levels = BTreeMap<i32, Vec<DataFileMeta>>;

pub(super) fn is_source_index(entry: &IndexManifestEntry) -> bool {
    entry.index_file.global_index_meta.is_some()
}

pub(super) fn source(entry: &IndexManifestEntry) -> Result<PrimaryKeyIndexSourceMeta> {
    let file = &entry.index_file;
    if !matches!(
        file.index_type.as_str(),
        "btree" | "bitmap" | "multivalue" | "fm" | "full-text"
    ) && !crate::vindex::is_vindex_index_type(&file.index_type)
    {
        return Err(invalid("Unsupported primary-key index type"));
    }
    let meta = file
        .global_index_meta
        .as_ref()
        .ok_or_else(|| invalid("Missing PK index metadata"))?;
    if meta
        .extra_field_ids
        .as_ref()
        .is_some_and(|ids| !ids.is_empty())
        || file.deletion_vectors_ranges.is_some()
    {
        return Err(invalid("Invalid primary-key index metadata"));
    }
    PrimaryKeyIndexSourceMeta::from_global_index_meta(meta)
}

fn unit(entry: &IndexManifestEntry) -> Result<Unit> {
    Ok((
        entry.partition.clone(),
        entry.bucket,
        source(entry)?.data_level(),
        entry
            .index_file
            .global_index_meta
            .as_ref()
            .unwrap()
            .index_field_id,
    ))
}

async fn read_indexes(table: &Table, snapshot: &Snapshot) -> Result<Vec<IndexManifestEntry>> {
    match snapshot.index_manifest() {
        Some(name) => {
            IndexManifest::read(
                table.file_io(),
                &table.snapshot_manager().manifest_path(name),
            )
            .await
        }
        None => Ok(vec![]),
    }
}

pub(super) fn levels(
    active: &[ManifestEntry],
    changes: &[ManifestEntry],
    partition: &[u8],
    bucket: i32,
) -> Levels {
    let removed = changes
        .iter()
        .filter(|e| {
            *e.kind() == FileKind::Delete && e.partition() == partition && e.bucket() == bucket
        })
        .map(|e| e.file().file_name.as_str())
        .collect::<HashSet<_>>();
    let mut levels = Levels::new();
    for entry in active
        .iter()
        .filter(|e| !removed.contains(e.file().file_name.as_str()))
        .chain(changes.iter().filter(|e| *e.kind() == FileKind::Add))
        .filter(|e| {
            e.partition() == partition
                && e.bucket() == bucket
                && should_read_pk_index_source(e.file())
        })
    {
        levels
            .entry(entry.file().level)
            .or_default()
            .push(entry.file().clone());
    }
    for files in levels.values_mut() {
        files.sort_by(|a, b| a.file_name.cmp(&b.file_name));
    }
    levels
}

pub(super) fn source_meta(level: i32, files: &[DataFileMeta]) -> Result<PrimaryKeyIndexSourceMeta> {
    PrimaryKeyIndexSourceMeta::new(
        level,
        files
            .iter()
            .map(|f| PrimaryKeyIndexSourceFile::new(f.file_name.clone(), f.row_count))
            .collect::<Result<_>>()?,
    )
}

pub(super) fn row_count(files: &[DataFileMeta]) -> Result<i64> {
    files.iter().try_fold(0i64, |sum, f| {
        sum.checked_add(f.row_count)
            .ok_or_else(|| invalid("PK index row count overflow"))
    })
}

fn compatible(table: &Table, entry: &IndexManifestEntry, definition: &Definition) -> Result<bool> {
    if entry.index_file.index_type != definition.index_type {
        return Ok(false);
    }
    if crate::vindex::is_vindex_index_type(&definition.index_type)
        && entry.index_file.row_count == 0
        && entry.index_file.file_size == 0
    {
        return Ok(true);
    }
    if definition.index_type == "full-text"
        || crate::vindex::is_vindex_index_type(&definition.index_type)
    {
        let expected = native_options(table, definition)?;
        let recorded = entry
            .index_file
            .global_index_meta
            .as_ref()
            .and_then(|m| m.index_meta.as_deref())
            .and_then(|bytes| serde_json::from_slice::<BTreeMap<String, String>>(bytes).ok());
        return Ok(recorded.as_ref() == Some(&expected));
    }
    Ok(true)
}

pub(super) struct Maintenance<'a> {
    pub(super) deletes: Vec<IndexManifestEntry>,
    pub(super) builds: Vec<(i32, &'a Definition)>,
}

pub(super) async fn unsupported_scalar_units(
    table: &Table,
    levels: &Levels,
    defs: &[Definition],
) -> HashSet<(i32, i32)> {
    let mut unsupported = HashSet::new();
    for (level, files) in levels {
        let files = files.iter().collect::<Vec<_>>();
        for definition in defs.iter().filter(|d| {
            matches!(
                d.index_type.as_str(),
                "btree" | "bitmap" | "multivalue" | "fm"
            )
        }) {
            if !crate::table::pk_index_schema::source_types_match(table, &definition.field, &files)
                .await
            {
                unsupported.insert((*level, definition.field.id()));
            }
        }
    }
    unsupported
}

pub(super) fn plan<'a>(
    table: &Table,
    indexes: &[IndexManifestEntry],
    levels: &Levels,
    defs: &'a [Definition],
    unsupported: &HashSet<(i32, i32)>,
    partition: &[u8],
    bucket: i32,
) -> Result<Maintenance<'a>> {
    let mut existing = BTreeMap::<(i32, i32), Vec<&IndexManifestEntry>>::new();
    for entry in indexes
        .iter()
        .filter(|e| e.partition == partition && e.bucket == bucket && is_source_index(e))
    {
        let meta = source(entry)?;
        existing
            .entry((
                meta.data_level(),
                entry
                    .index_file
                    .global_index_meta
                    .as_ref()
                    .unwrap()
                    .index_field_id,
            ))
            .or_default()
            .push(entry);
    }
    let mut deletes = Vec::new();
    let mut builds = Vec::new();
    for ((level, field), entries) in &existing {
        let expected = levels
            .get(level)
            .map(|files| source_meta(*level, files))
            .transpose()?;
        let definition = defs.iter().find(|d| d.field.id() == *field);
        let valid = entries.len() == 1
            && expected.as_ref() == Some(&source(entries[0])?)
            && match definition {
                Some(d) => {
                    !unsupported.contains(&(*level, *field)) && compatible(table, entries[0], d)?
                }
                None => true,
            };
        if !valid {
            for entry in entries {
                let mut deleted = (*entry).clone();
                deleted.kind = FileKind::Delete;
                deletes.push(deleted);
            }
        }
    }
    for (level, files) in levels {
        if row_count(files)? == 0 {
            continue;
        }
        for definition in defs {
            if unsupported.contains(&(*level, definition.field.id())) {
                continue;
            }
            let valid = existing
                .get(&(*level, definition.field.id()))
                .is_some_and(|entries| {
                    entries.len() == 1
                        && !deletes
                            .iter()
                            .any(|d| d.index_file.file_name == entries[0].index_file.file_name)
                });
            if !valid {
                builds.push((*level, definition));
            }
        }
    }
    Ok(Maintenance { deletes, builds })
}

/// Validate prepared outputs and the captured membership of changed PK units.
pub(super) async fn validate(
    table: &Table,
    captured: Option<&Snapshot>,
    current: &[IndexManifestEntry],
    active: &[ManifestEntry],
    changes: &[ManifestEntry],
    prepared: Option<&[IndexManifestEntry]>,
) -> Result<()> {
    if table.schema().primary_keys().is_empty() {
        return Ok(());
    }
    let defs = definitions(table.schema())?;
    let captured = match captured {
        Some(snapshot) => read_indexes(table, snapshot).await?,
        None => return Err(invalid("PK compaction requires its captured snapshot")),
    };
    let buckets = changes
        .iter()
        .map(|e| (e.partition().to_vec(), e.bucket()))
        .collect::<HashSet<_>>();
    if prepared.is_none()
        && (!defs.is_empty()
            || captured
                .iter()
                .any(|e| is_source_index(e) && buckets.contains(&(e.partition.clone(), e.bucket))))
    {
        return Err(invalid(
            "Primary-key compaction results are missing index maintenance; execute the plan again",
        ));
    }
    let prepared = prepared.unwrap_or_default();
    let mut required_deletes = Vec::new();
    let mut required_builds = HashSet::new();
    let mut after_levels = BTreeMap::new();
    for (partition, bucket) in &buckets {
        let after = levels(active, changes, partition, *bucket);
        let unsupported = unsupported_scalar_units(table, &after, &defs).await;
        let maintenance = plan(
            table,
            &captured,
            &after,
            &defs,
            &unsupported,
            partition,
            *bucket,
        )?;
        required_deletes.extend(maintenance.deletes);
        for (level, definition) in maintenance.builds {
            required_builds.insert((partition.clone(), *bucket, level, definition.field.id()));
        }
        after_levels.insert((partition.clone(), *bucket), after);
    }
    let actual_deletes = prepared
        .iter()
        .filter(|e| e.kind == FileKind::Delete)
        .collect::<Vec<_>>();
    if actual_deletes.len() != required_deletes.len()
        || required_deletes
            .iter()
            .any(|e| !actual_deletes.contains(&e))
    {
        return Err(invalid(
            "PK compaction index retirements do not match captured sources",
        ));
    }
    let mut touched = required_deletes
        .iter()
        .map(unit)
        .collect::<Result<HashSet<_>>>()?;
    touched.extend(required_builds.iter().cloned());
    for key in &touched {
        let before = captured
            .iter()
            .filter(|e| is_source_index(e) && unit(e).is_ok_and(|u| &u == key))
            .collect::<Vec<_>>();
        let now = current
            .iter()
            .filter(|e| is_source_index(e) && unit(e).is_ok_and(|u| &u == key))
            .collect::<Vec<_>>();
        if before != now {
            return Err(invalid(
                "PK index changed concurrently; execute compaction again",
            ));
        }
    }
    let mut names = HashSet::new();
    let mut vector_sources = HashSet::new();
    for entry in prepared.iter().filter(|e| e.kind == FileKind::Add) {
        if !names.insert(entry.index_file.file_name.as_str())
            || current
                .iter()
                .any(|old| old.index_file.file_name == entry.index_file.file_name)
        {
            return Err(invalid("PK index output must have a new unique file name"));
        }
        let key = unit(entry)?;
        if !required_builds.remove(&key) {
            return Err(invalid("Unexpected or duplicate PK index output"));
        }
        let definition = defs
            .iter()
            .find(|d| d.field.id() == key.3)
            .ok_or_else(|| invalid("Undeclared PK index output"))?;
        let files = &after_levels[&(key.0.clone(), key.1)][&key.2];
        let meta = entry.index_file.global_index_meta.as_ref().unwrap();
        let count = row_count(files)?;
        let indexed_rows = entry.index_file.row_count;
        let valid_count = if crate::vindex::is_vindex_index_type(&definition.index_type) {
            indexed_rows >= 0
                && indexed_rows <= count
                && ((indexed_rows == 0) == (entry.index_file.file_size == 0))
        } else {
            indexed_rows == count
        };
        let sources = source(entry)?;
        if sources != source_meta(key.2, files)?
            || meta.row_range_start != 0
            || meta.row_range_end != count - 1
            || !valid_count
            || entry.index_file.file_size < 0
            || !compatible(table, entry, definition)?
        {
            return Err(invalid(
                "PK index output does not match complete compacted level",
            ));
        }
        if crate::vindex::is_vindex_index_type(&definition.index_type) {
            vector_sources.extend(sources.source_files().iter().map(|file| {
                (
                    entry.partition.clone(),
                    entry.bucket,
                    file.file_name().into(),
                )
            }));
        }
    }
    if !required_builds.is_empty() {
        return Err(invalid("Missing PK index output"));
    }
    super::deletion_vector_index::validate_source_deletion_growth(
        table,
        &captured,
        current,
        vector_sources,
    )
    .await?;
    Ok(())
}

fn invalid(message: &str) -> Error {
    Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}
