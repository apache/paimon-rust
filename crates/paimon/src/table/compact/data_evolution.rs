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

//! Normal-file compaction, following Java DataEvolutionCompactCoordinator and
//! DataEvolutionNormalCompactTask. Physical slots and logical deletions survive.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use futures::TryStreamExt;
use roaring::RoaringBitmap;

use super::{invalid, BucketFiles, CompactContext, CompactTask};
use crate::resource::ResourceContext;
use crate::spec::{
    bucket_path, BinaryRow, CoreOptions, DataField, DataFileMeta, DataType, FileKind,
    IndexManifest, IndexManifestEntry, ManifestEntry, PartitionComputer, Snapshot,
};
use crate::table::data_file_index_writer::FileIndexOptions;
use crate::table::data_file_writer::DataFileWriter;
use crate::table::index_file_path::committed_index_file_path;
use crate::table::source::{data_evolution_anchor_file, is_data_evolution_normal_file};
use crate::table::stats_filter::group_by_overlapping_row_id;
use crate::table::{DataEvolutionDeleteWriter, DataSplit, Table, TableScan};
use crate::{Error, Result};

pub(super) fn ensure_supported(table: &Table) -> Result<()> {
    let options = table.schema().core_options();
    if table.is_format_table()
        || !options.row_tracking_enabled()
        || !table.schema().primary_keys().is_empty()
        || options.bucket() != -1
        || options.query_auth_enabled()
        || options.rowkind_field().is_some()
        || options.clustering_incremental_enabled()
        || table
            .schema()
            .options()
            .contains_key("record-level.expire-time")
    {
        return Err(Error::Unsupported {
            message: "Data-evolution compaction requires an unaware append table with row tracking, without query authorization, rowkind fields, incremental clustering or record expiration".into(),
        });
    }
    for key in [
        "data-evolution.compaction.rewrite-row-ids",
        "data-evolution.compaction.split-large-files",
    ] {
        if table
            .schema()
            .options()
            .get(key)
            .is_some_and(|v| v.eq_ignore_ascii_case("true"))
        {
            return Err(Error::Unsupported {
                message: format!("Data-evolution compaction does not support {key}=true"),
            });
        }
    }
    Ok(())
}

pub(super) fn validate_file(file: &DataFileMeta) -> Result<()> {
    if !is_data_evolution_normal_file(file)
        || file.file_size < 0
        || file.row_count <= 0
        || file.level != 0
        || file
            .first_row_id
            .is_none_or(|id| id < 0 || id.checked_add(file.row_count).is_none())
        || file.min_sequence_number <= 0
        || file.max_sequence_number < file.min_sequence_number
        || file.delete_row_count.is_some_and(|count| count != 0)
        || file.write_cols.as_ref().is_some_and(|cols| {
            cols.iter()
                .any(|name| crate::spec::is_row_tracking_column(name))
        })
    {
        return Err(invalid("Data-evolution compaction requires normal files with valid row IDs, row counts and sequence bounds"));
    }
    Ok(())
}

/// Check alignment within each overlapping group and continuity between groups.
pub(super) fn row_range(files: &[DataFileMeta]) -> Result<(i64, i64)> {
    let groups = group_by_overlapping_row_id(files.to_vec());
    let mut range: Option<(i64, i64)> = None;
    for group in groups {
        for file in &group {
            validate_file(file)?;
        }
        let current = group[0]
            .row_id_range()
            .ok_or_else(|| invalid("Missing data-evolution row range"))?;
        if group
            .iter()
            .any(|file| file.row_id_range() != Some(current))
        {
            return Err(invalid(
                "Data-evolution normal files must have aligned row-ID ranges",
            ));
        }
        match range {
            Some((first, last)) if last.checked_add(1) == Some(current.0) => {
                range = Some((first, current.1))
            }
            Some(_) => {
                return Err(invalid(
                    "Data-evolution compaction cannot cross a row-ID gap",
                ))
            }
            None => range = Some(current),
        }
    }
    range.ok_or_else(|| invalid("Empty data-evolution compaction range"))
}

fn output_range(files: &[DataFileMeta], deletion_vectors_enabled: bool) -> Result<(i64, i64)> {
    let (first, last) = row_range(files)?;
    // The current Rust DV writer addresses local positions with u32. Merging
    // otherwise deletable groups must not make later DELETEs unrepresentable.
    if deletion_vectors_enabled && last - first > i64::from(u32::MAX) {
        return Err(Error::Unsupported {
            message: "Data-evolution compaction with deletion vectors cannot produce more than 4294967296 physical row slots in one file; 64-bit deletion-vector writing is not supported".into(),
        });
    }
    Ok((first, last))
}

/// Select complete normal-file groups and pack adjacent row-ID ranges.
pub(super) struct DataEvolutionCompactPlanner {
    min_file_num: usize,
    target_file_size: i128,
    open_file_cost: i64,
    deletion_vectors_enabled: bool,
}

impl DataEvolutionCompactPlanner {
    pub(super) fn new(options: &CoreOptions<'_>) -> Result<Self> {
        Ok(Self {
            min_file_num: options.compaction_min_file_num()?,
            target_file_size: i128::from(options.target_file_size()),
            open_file_cost: options.source_split_open_file_cost(),
            deletion_vectors_enabled: options.deletion_vectors_enabled(),
        })
    }

    pub(super) async fn plan_tasks(
        &self,
        table: &Table,
        scan: &TableScan<'_>,
        context: &CompactContext,
    ) -> Result<Vec<CompactTask>> {
        let mut groups = BucketFiles::new();
        // Read captured manifests rather than query providers so shadowed
        // column versions are also included in each normal-file rewrite.
        for entry in scan.plan_manifest_entries(&context.snapshot).await? {
            if !is_data_evolution_normal_file(entry.file()) {
                continue;
            }
            validate_file(entry.file())?;
            groups
                .entry((
                    entry.partition().to_vec(),
                    entry.bucket(),
                    entry.total_buckets(),
                ))
                .or_default()
                .push(entry.file().clone());
        }
        let mut tasks = Vec::new();
        for ((partition, bucket, total_buckets), files) in groups {
            for files in self.plan(files)? {
                validate_storage_layout(table, &files).await?;
                tasks.push(CompactTask {
                    context: context.clone(),
                    task_id: tasks.len(),
                    partition: partition.clone(),
                    bucket,
                    total_buckets,
                    files,
                    output_level: None,
                });
            }
        }
        Ok(tasks)
    }

    /// Java packs whole groups by sum(max(file_size, open_file_cost)). A heavy
    /// group is isolated; adjacent groups fill bins until weight > target.
    fn plan(&self, files: Vec<DataFileMeta>) -> Result<Vec<Vec<DataFileMeta>>> {
        let mut tasks = Vec::new();
        let mut pending = Vec::new();
        let mut weight = 0i128;
        let mut previous_end: Option<i64> = None;
        let drain = |pending: &mut Vec<DataFileMeta>, tasks: &mut Vec<Vec<DataFileMeta>>| {
            let files = std::mem::take(pending);
            if files.len() >= self.min_file_num {
                tasks.push(files);
            }
        };
        for group in group_by_overlapping_row_id(files) {
            let (first, last) = row_range(&group)?;
            let group_weight = group
                .iter()
                .map(|file| i128::from(file.file_size.max(self.open_file_cost)))
                .sum::<i128>();
            if previous_end.is_some_and(|end| end.checked_add(1) != Some(first))
                || group_weight > self.target_file_size
            {
                drain(&mut pending, &mut tasks);
                weight = 0;
            }
            pending.extend(group);
            weight += group_weight;
            if weight > self.target_file_size {
                drain(&mut pending, &mut tasks);
                weight = 0;
            }
            previous_end = Some(last);
        }
        drain(&mut pending, &mut tasks);
        for task in &tasks {
            output_range(task, self.deletion_vectors_enabled)?;
        }
        Ok(tasks)
    }
}

pub(super) fn validate_inputs(
    partition: &[u8],
    bucket: i32,
    files: &[DataFileMeta],
    active: &[ManifestEntry],
) -> Result<()> {
    let (first, last) = row_range(files)?;
    let names = files
        .iter()
        .map(|file| file.file_name.as_str())
        .collect::<HashSet<_>>();
    if active.iter().any(|entry| {
        entry.partition() == partition
            && entry.bucket() == bucket
            && is_data_evolution_normal_file(entry.file())
            && entry
                .file()
                .row_id_range()
                .is_some_and(|(start, end)| start <= last && end >= first)
            && !names.contains(entry.file().file_name.as_str())
    }) {
        return Err(invalid("Data-evolution compaction inputs changed or omit files from the captured row-ID groups"));
    }
    Ok(())
}

pub(in crate::table) fn validate_replacement_inputs(
    delta: &[ManifestEntry],
    active: &[ManifestEntry],
) -> Result<()> {
    // Keep disjoint submitted task ranges separate: committing a subset must
    // not turn its gaps into selected rows or require untouched groups.
    let mut buckets = BTreeMap::<_, Vec<DataFileMeta>>::new();
    for entry in delta
        .iter()
        .filter(|entry| *entry.kind() == FileKind::Delete)
    {
        buckets
            .entry((entry.partition().to_vec(), entry.bucket()))
            .or_default()
            .push(entry.file().clone());
    }
    for ((partition, bucket), files) in buckets {
        for group in group_by_overlapping_row_id(files) {
            validate_inputs(&partition, bucket, &group, active)?;
        }
    }
    Ok(())
}

fn normal_fields(schema: &crate::spec::TableSchema) -> Vec<DataField> {
    let options = schema.core_options();
    let inline = options.blob_inline_fields();
    schema
        .fields()
        .iter()
        .filter(|field| {
            !(field.data_type().is_blob_file_field() && !inline.contains(field.name())
                || options.vector_file_format().is_some()
                    && matches!(field.data_type(), DataType::Vector(_)))
        })
        .cloned()
        .collect()
}

async fn column_sequences(
    table: &Table,
    inputs: &[DataFileMeta],
    fields: &[DataField],
    max_sequence: i64,
) -> Result<Option<Vec<i64>>> {
    let mut sequences = HashMap::<i32, i64>::new();
    for file in inputs {
        let physical = input_fields(table, file).await?;
        for (position, field) in physical.iter().enumerate() {
            let sequence = file
                .column_max_sequence_numbers
                .as_ref()
                .filter(|values| values.len() == physical.len())
                .and_then(|values| values.get(position))
                .copied()
                .filter(|value| *value > 0 && *value <= file.max_sequence_number)
                .unwrap_or(file.max_sequence_number);
            sequences
                .entry(field.id())
                .and_modify(|current| *current = (*current).max(sequence))
                .or_insert(sequence);
        }
    }
    let result = fields
        .iter()
        .map(|field| sequences.get(&field.id()).copied().unwrap_or(max_sequence))
        .collect::<Vec<_>>();
    Ok(result
        .iter()
        .any(|value| *value != max_sequence)
        .then_some(result))
}

async fn input_fields(table: &Table, file: &DataFileMeta) -> Result<Vec<DataField>> {
    let schema = table.schema_manager().schema(file.schema_id).await?;
    match &file.write_cols {
        Some(columns) => {
            crate::table::data_evolution_fields::project_by_paths(schema.fields(), columns)
        }
        None => Ok(schema.data_file_fields(None)),
    }
}

/// A layout option can start writing VECTOR fields to dedicated files while
/// older normal files still contain inline values. Dropping those providers
/// would turn readable values into NULL; migration requires a separate rewrite.
pub(in crate::table) async fn validate_storage_layout(
    table: &Table,
    files: &[DataFileMeta],
) -> Result<()> {
    let normal_ids = normal_fields(table.schema())
        .into_iter()
        .map(|field| field.id())
        .collect::<HashSet<_>>();
    let dedicated = table
        .schema()
        .fields()
        .iter()
        .filter(|field| !normal_ids.contains(&field.id()))
        .map(|field| (field.id(), field.name()))
        .collect::<HashMap<_, _>>();
    if dedicated.is_empty() {
        return Ok(());
    }
    for file in files {
        for field in input_fields(table, file).await? {
            if let Some(name) = dedicated.get(&field.id()) {
                return Err(Error::Unsupported {
                    message: format!("Data-evolution compaction cannot discard historical inline field '{name}' from '{}'; migrate its values to dedicated storage before compacting", file.file_name),
                });
            }
        }
    }
    Ok(())
}

pub(super) async fn execute(
    table: &Table,
    task: &CompactTask,
    split: &DataSplit,
    partition_path: String,
    resources: Option<ResourceContext>,
) -> Result<Vec<DataFileMeta>> {
    validate_storage_layout(table, task.files()).await?;
    let (first, last) = output_range(
        task.files(),
        table.schema().core_options().deletion_vectors_enabled(),
    )?;
    let fields = normal_fields(table.schema());
    let min_sequence = task
        .files
        .iter()
        .map(|file| file.min_sequence_number)
        .min()
        .unwrap();
    let max_sequence = task
        .files
        .iter()
        .map(|file| file.max_sequence_number)
        .max()
        .unwrap();
    let sequences = column_sequences(table, &task.files, &fields, max_sequence).await?;
    let options = table.schema().core_options();
    // Inline BLOB descriptors and views are normal columns. Preserve their
    // references instead of fetching payloads or resolving upstream values.
    let read_table = table.copy_with_options(HashMap::from([
        ("blob-as-descriptor".into(), "true".into()),
        ("blob-view.resolve.enabled".into(), "false".into()),
    ]));
    let mut builder = read_table.new_read_builder();
    builder.with_read_type(fields.clone());
    let mut read = builder.new_read()?.with_ordered(true);
    if let Some(resources) = &resources {
        read = read.with_resources(resources.clone());
    }
    // The executor's split has no deletion files: retain even deleted physical
    // slots. Commit preparation reattaches the latest logical deletions.
    let mut batches = read.to_arrow(std::slice::from_ref(split))?;
    let index = FileIndexOptions::parse(table.schema().options(), &fields)?;
    let mut writer = DataFileWriter::new(
        table.file_io().clone(),
        table.location().to_string(),
        partition_path,
        task.bucket,
        table.schema().id(),
        i64::MAX,
        options.file_compression().to_string(),
        options.file_compression_zstd_level(),
        options.write_buffer_size()? as i64,
        options.file_format(),
        fields.clone(),
        table.schema().options().clone(),
        Some(1),
        Some(first),
        Some(
            fields
                .iter()
                .map(|field| field.name().to_string())
                .collect(),
        ),
    )?
    .with_file_index(index.map(Arc::new))
    .with_resources(resources);
    let written = async {
        let mut rows = 0i64;
        while let Some(batch) = batches.try_next().await? {
            rows += batch.num_rows() as i64;
            writer.write(&batch).await?;
        }
        if rows != last - first + 1 {
            return Err(invalid("Data-evolution compaction input row count changed"));
        }
        let mut files = writer.prepare_commit().await?;
        for file in &mut files {
            file.min_sequence_number = min_sequence;
            file.max_sequence_number = max_sequence;
            file.column_max_sequence_numbers = sequences.clone();
        }
        validate_outputs(task, &files)?;
        Ok(files)
    }
    .await;
    if written.is_err() {
        writer.abort().await;
    }
    written
}

pub(super) fn validate_outputs(task: &CompactTask, files: &[DataFileMeta]) -> Result<()> {
    let expected = output_range(
        &task.files,
        task.context
            .schema
            .core_options()
            .deletion_vectors_enabled(),
    )?;
    let min_sequence = task
        .files
        .iter()
        .map(|file| file.min_sequence_number)
        .min()
        .unwrap();
    let max_sequence = task
        .files
        .iter()
        .map(|file| file.max_sequence_number)
        .max()
        .unwrap();
    if files.len() != 1
        || files[0].row_id_range() != Some(expected)
        || files[0].min_sequence_number != min_sequence
        || files[0].max_sequence_number != max_sequence
        || files[0].write_cols
            != Some(
                normal_fields(&task.context.schema)
                    .iter()
                    .map(|field| field.name().to_string())
                    .collect(),
            )
    {
        return Err(invalid(
            "Data-evolution compaction must preserve its row-ID range, columns and sequence bounds",
        ));
    }
    Ok(())
}

pub(super) fn validate_indexes(entries: &[IndexManifestEntry]) -> Result<()> {
    if entries.iter().any(|entry| {
        entry.index_file.index_type != "DELETION_VECTORS"
            && entry.index_file.global_index_meta.is_none()
    }) {
        return Err(Error::Unsupported {
            message: "Data-evolution compaction supports deletion vectors and global indexes"
                .into(),
        });
    }
    Ok(())
}

/// Rebase DVs on the latest snapshot on every CAS attempt. A concurrent DELETE
/// is carried forward, including deletions in untouched groups of this bucket.
pub(in crate::table) async fn rewrite_deletion_vectors(
    table: &Table,
    snapshot: &Snapshot,
    delta: &[ManifestEntry],
    created_files: &mut Vec<String>,
) -> Result<Option<Vec<IndexManifestEntry>>> {
    let Some(name) = snapshot.index_manifest() else {
        return Ok(None);
    };
    let mut indexes = IndexManifest::read(
        table.file_io(),
        &table.snapshot_manager().manifest_path(name),
    )
    .await?;
    let mut buckets = BTreeMap::<_, (Vec<DataFileMeta>, Vec<DataFileMeta>)>::new();
    for entry in delta {
        let files = buckets
            .entry((entry.partition().to_vec(), entry.bucket()))
            .or_default();
        match entry.kind() {
            FileKind::Delete => files.0.push(entry.file().clone()),
            FileKind::Add => files.1.push(entry.file().clone()),
        }
    }
    let writer = DataEvolutionDeleteWriter::new(table)?;
    let mut changed = false;
    for ((partition, bucket), (before, after)) in buckets {
        let groups = group_by_overlapping_row_id(before);
        let anchors = groups
            .iter()
            .map(|group| Ok(data_evolution_anchor_file(group)?.file_name.clone()))
            .collect::<Result<HashSet<_>>>()?;
        // Like Java AppendDeleteFileMaintainer, replace only index files
        // touched by selected anchors. Untouched files remain independent.
        let (mut bitmaps, removed) = writer
            .read_existing_bucket_deletion_vectors(&partition, bucket, snapshot, Some(&anchors))
            .await?;
        let mut moved = false;
        for group in groups {
            let anchor = data_evolution_anchor_file(&group)?;
            let Some(bitmap) = bitmaps.shift_remove(&anchor.file_name) else {
                continue;
            };
            moved = true;
            let old_first = anchor.first_row_id.unwrap();
            for position in bitmap {
                if i64::from(position) >= anchor.row_count {
                    return Err(invalid(
                        "Deletion vector position is outside its compact input",
                    ));
                }
                let absolute = old_first + i64::from(position);
                let output = after
                    .iter()
                    .find(|file| {
                        file.row_id_range()
                            .is_some_and(|(start, end)| start <= absolute && absolute <= end)
                    })
                    .ok_or_else(|| invalid("Compacted files do not cover a deleted row ID"))?;
                let relative = u32::try_from(absolute - output.first_row_id.unwrap())
                    .map_err(|_| invalid("Compacted deletion vector position exceeds u32::MAX"))?;
                bitmaps
                    .entry(output.file_name.clone())
                    .or_insert_with(RoaringBitmap::new)
                    .insert(relative);
            }
        }
        if !moved {
            continue;
        }
        let file = writer
            .write_deletion_vector_index_file(&partition, bucket, bitmaps)
            .await?;
        let computer = PartitionComputer::new(
            table.schema().partition_keys(),
            table.schema().fields(),
            table.schema().core_options().partition_default_name(),
            table.schema().core_options().legacy_partition_name(),
        )?;
        let bucket_dir = bucket_path(
            &table.data_file_location(),
            Some(&computer),
            &BinaryRow::from_serialized_bytes(&partition)?,
            bucket,
        )?;
        created_files.push(committed_index_file_path(
            table.location(),
            &bucket_dir,
            table.schema().core_options().index_file_in_data_file_dir(),
            &file,
        ));
        indexes.retain(|entry| {
            !removed.iter().any(|old| {
                entry.partition == partition && entry.bucket == bucket && entry.index_file == *old
            })
        });
        indexes.push(IndexManifestEntry {
            version: 1,
            kind: FileKind::Add,
            partition,
            bucket,
            index_file: file,
        });
        changed = true;
    }
    Ok(changed.then_some(indexes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::stats::BinaryTableStats;

    fn file(name: &str, first: i64, rows: i64, bytes: i64) -> DataFileMeta {
        DataFileMeta {
            file_name: name.into(),
            file_size: bytes,
            row_count: rows,
            min_key: vec![],
            max_key: vec![],
            key_stats: BinaryTableStats::empty(),
            value_stats: BinaryTableStats::empty(),
            min_sequence_number: 1,
            max_sequence_number: 1,
            schema_id: 0,
            level: 0,
            extra_files: vec![],
            creation_time: None,
            delete_row_count: Some(0),
            embedded_index: None,
            file_source: Some(0),
            value_stats_cols: None,
            external_path: None,
            first_row_id: Some(first),
            write_cols: None,
            column_max_sequence_numbers: None,
        }
    }

    fn planned(
        files: Vec<DataFileMeta>,
        target: i64,
        open_cost: i64,
        min: usize,
    ) -> Vec<Vec<String>> {
        let options = HashMap::from([
            ("target-file-size".into(), format!("{target}b")),
            (
                "source.split.open-file-cost".into(),
                format!("{open_cost}b"),
            ),
            ("compaction.min.file-num".into(), min.to_string()),
        ]);
        DataEvolutionCompactPlanner::new(&CoreOptions::new(&options))
            .unwrap()
            .plan(files)
            .unwrap()
            .into_iter()
            .map(|group| group.into_iter().map(|file| file.file_name).collect())
            .collect()
    }

    #[test]
    fn java_contiguous_bin_boundary_and_open_cost() {
        let files = vec![
            file("a", 0, 100, 100),
            file("b", 100, 100, 100),
            file("c", 200, 100, 100),
        ];
        assert_eq!(planned(files.clone(), 199, 1, 2), vec![vec!["a", "b"]]);
        assert_eq!(planned(files.clone(), 200, 1, 2), vec![vec!["a", "b", "c"]]);
        assert_eq!(
            planned(files, 199, 200, 2),
            Vec::<Vec<String>>::new(),
            "each heavy singleton is isolated"
        );
    }

    #[test]
    fn java_gap_and_large_file_boundaries() {
        let files = vec![
            file("a", 0, 10, 10),
            file("b", 10, 10, 10),
            file("large", 20, 10, 1000),
            file("c", 100, 10, 10),
            file("d", 110, 10, 10),
        ];
        assert_eq!(
            planned(files, 100, 1, 2),
            vec![vec!["a", "b"], vec!["c", "d"]]
        );
    }

    #[test]
    fn versions_stay_together_even_when_the_group_exceeds_target() {
        let files = vec![
            file("tail", 0, 10, 1),
            file("base", 10, 10, 80),
            file("partial", 10, 10, 80),
            file("last", 20, 10, 1),
        ];
        assert_eq!(planned(files, 100, 1, 2), vec![vec!["base", "partial"]]);
    }

    #[test]
    fn rejects_gaps_misaligned_versions_and_overflow() {
        assert!(row_range(&[file("a", 0, 10, 1), file("b", 20, 10, 1)]).is_err());
        assert!(row_range(&[file("a", 0, 10, 1), file("b", 5, 10, 1)]).is_err());
        assert!(row_range(&[file("a", i64::MAX, 1, 1)]).is_err());
    }

    #[test]
    fn dv_output_range_keeps_every_row_addressable_after_compaction() {
        let mut options = HashMap::from([
            ("data-evolution.enabled".into(), "true".into()),
            ("deletion-vectors.enabled".into(), "true".into()),
            ("compaction.min.file-num".into(), "2".into()),
        ]);
        let max_rows = i64::from(u32::MAX) + 1;
        let too_wide = vec![file("a", 0, max_rows, 1), file("b", max_rows, 1, 1)];
        let planner = DataEvolutionCompactPlanner::new(&CoreOptions::new(&options)).unwrap();
        let error = planner.plan(too_wide.clone()).unwrap_err();
        assert!(matches!(error, Error::Unsupported { .. }));
        assert!(error.to_string().contains("64-bit deletion-vector writing"));
        let at_limit = vec![file("a", 0, max_rows - 1, 1), file("b", max_rows - 1, 1, 1)];
        assert_eq!(planner.plan(at_limit).unwrap().len(), 1);
        options.insert("deletion-vectors.enabled".into(), "false".into());
        assert_eq!(
            DataEvolutionCompactPlanner::new(&CoreOptions::new(&options))
                .unwrap()
                .plan(too_wide)
                .unwrap()
                .len(),
            1
        );
    }
}
