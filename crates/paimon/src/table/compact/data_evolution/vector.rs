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

//! Dedicated Vector compaction following Java DataEvolutionVectorCompactTask.

use std::collections::{BTreeMap, HashMap, HashSet};

use futures::TryStreamExt;

use super::{column_sequences, input_fields, validate_file, validate_storage_layout};
use crate::resource::ResourceContext;
use crate::spec::{
    CoreOptions, DataField, DataFileMeta, DataType, FileKind, ManifestEntry, TableSchema,
};
use crate::table::compact::{invalid, CompactTask};
use crate::table::data_file_writer::DataFileWriter;
use crate::table::source::is_data_evolution_normal_file;
use crate::table::stats_filter::group_by_overlapping_row_id;
use crate::table::{DataSplit, Table};
use crate::{Error, Result};

pub(super) fn is_vector(file: &DataFileMeta) -> bool {
    file.file_name.to_ascii_lowercase().contains(".vector.")
}

pub(super) fn ensure_supported(options: &CoreOptions<'_>) -> Result<()> {
    if options.vector_file_format().is_none() || options.vector_target_file_size() <= 0 {
        return Err(Error::ConfigInvalid {
            message: "Vector compaction requires vector.file.format and a positive vector.target-file-size".into(),
        });
    }
    Ok(())
}

fn merged_range(mut ranges: Vec<(i64, i64)>) -> Option<(i64, i64)> {
    ranges.sort_unstable();
    let (first, mut last) = *ranges.first()?;
    for (start, end) in ranges.into_iter().skip(1) {
        if start > last + 1 {
            return None;
        }
        last = last.max(end);
    }
    Some((first, last))
}

pub(super) fn row_range(files: &[DataFileMeta]) -> Result<(i64, i64)> {
    for file in files {
        validate_file(file)?;
        if !is_vector(file) {
            return Err(invalid("Vector compaction cannot include other file types"));
        }
    }
    merged_range(
        files
            .iter()
            .filter_map(DataFileMeta::row_id_range)
            .collect(),
    )
    .ok_or_else(|| invalid("Vector compaction requires a nonempty contiguous row-ID range"))
}

/// Resolve historical names to stable IDs, ignoring dropped fields. A field
/// absent from every input must stay absent, rather than acquiring NULL values.
async fn covered_fields(table: &Table, files: &[DataFileMeta]) -> Result<Option<Vec<DataField>>> {
    let current = table
        .schema()
        .fields()
        .iter()
        .filter(|field| matches!(field.data_type(), DataType::Vector(_)))
        .map(|field| (field.id(), field))
        .collect::<HashMap<_, _>>();
    let mut ranges = HashMap::<i32, Vec<(i64, i64)>>::new();
    for file in files {
        validate_file(file)?;
        if !is_vector(file) || file.write_cols.as_ref().is_none_or(Vec::is_empty) {
            return Err(invalid("Vector files must declare their write columns"));
        }
        let mut ids = HashSet::new();
        for field in input_fields(table, file).await? {
            if !matches!(field.data_type(), DataType::Vector(_)) || !ids.insert(field.id()) {
                return Err(invalid("Invalid Vector compaction input fields"));
            }
            if let Some(current) = current.get(&field.id()) {
                if field.data_type() != current.data_type() {
                    return Err(Error::Unsupported {
                        message: "Vector compaction cannot migrate field types".into(),
                    });
                }
                ranges
                    .entry(field.id())
                    .or_default()
                    .push(file.row_id_range().unwrap());
            }
        }
    }
    let Some(range) = merged_range(
        files
            .iter()
            .filter_map(DataFileMeta::row_id_range)
            .collect(),
    ) else {
        return Ok(None);
    };
    if ranges.is_empty()
        || ranges
            .values()
            .any(|ranges| merged_range(ranges.clone()) != Some(range))
    {
        return Ok(None);
    }
    Ok(Some(
        table
            .schema()
            .fields()
            .iter()
            .filter(|field| ranges.contains_key(&field.id()))
            .cloned()
            .collect(),
    ))
}

fn current_fields(task: &CompactTask) -> Result<Vec<DataField>> {
    let ids = task
        .vector_field_ids
        .as_ref()
        .ok_or_else(|| invalid("Missing Vector field identities"))?;
    let fields = task
        .context
        .schema
        .fields()
        .iter()
        .filter(|field| {
            ids.contains(&field.id()) && matches!(field.data_type(), DataType::Vector(_))
        })
        .cloned()
        .collect::<Vec<_>>();
    if fields.is_empty() || fields.iter().map(DataField::id).collect::<Vec<_>>() != *ids {
        return Err(invalid(
            "Vector compaction fields must be current Vector fields in table order",
        ));
    }
    Ok(fields)
}

pub(super) fn validate_task(task: &CompactTask) -> Result<()> {
    ensure_supported(&task.context.schema.core_options())?;
    if task.blob_field_id.is_some() || task.files.len() < 2 {
        return Err(invalid(
            "Vector compaction requires at least two dedicated Vector files",
        ));
    }
    current_fields(task)?;
    row_range(&task.files)?;
    Ok(())
}

fn needs_rewrite(files: &[DataFileMeta], target: i64) -> bool {
    let mut ranges = files
        .iter()
        .filter_map(DataFileMeta::row_id_range)
        .collect::<Vec<_>>();
    ranges.sort_unstable();
    if ranges.windows(2).any(|pair| pair[1].0 <= pair[0].1) {
        return true;
    }
    let completed_writer = files.first().and_then(compacted_writer);
    let already_compacted = completed_writer.is_some_and(|writer| {
        files
            .iter()
            .all(|file| compacted_writer(file) == Some(writer))
    });
    !already_compacted && files.iter().filter(|file| file.file_size < target).count() > 1
}

// Rolled files share a writer UUID and differ by counter. Their compressed
// sizes can all fall below the target without another rewrite being useful.
fn compacted_writer(file: &DataFileMeta) -> Option<(&str, &str)> {
    if file.file_source != Some(1) {
        return None;
    }
    let (stem, format) = file.file_name.rsplit_once(".vector.")?;
    let (writer, counter) = stem.rsplit_once('-')?;
    counter.parse::<u64>().ok()?;
    uuid::Uuid::parse_str(writer.get(writer.len().checked_sub(36)?..)?).ok()?;
    Some((writer, format))
}

/// Overlapping versions are indivisible. Groups have increasing, disjoint
/// ranges, so both boundaries of a normal region can be found by binary search.
struct VectorFileGroup {
    first: i64,
    last: i64,
    files: Vec<DataFileMeta>,
    selected: bool,
}

fn group_vector_files(files: Vec<DataFileMeta>) -> Result<Vec<VectorFileGroup>> {
    group_by_overlapping_row_id(files)
        .into_iter()
        .map(|files| {
            let (first, last) = row_range(&files)?;
            Ok(VectorFileGroup {
                first,
                last,
                files,
                selected: false,
            })
        })
        .collect()
}

fn groups_in_region(
    groups: &mut [VectorFileGroup],
    first: i64,
    last: i64,
) -> &mut [VectorFileGroup] {
    let start = groups.partition_point(|group| group.last < first);
    let end = groups.partition_point(|group| group.first <= last);
    let candidates = &mut groups[start..end];
    if candidates.first().is_some_and(|group| group.first < first)
        || candidates.last().is_some_and(|group| group.last > last)
    {
        return &mut [];
    }
    candidates
}

pub(super) async fn plan(
    table: &Table,
    normals: &[DataFileMeta],
    normal_tasks: &[Vec<DataFileMeta>],
    files: Vec<DataFileMeta>,
) -> Result<Vec<(Vec<i32>, Vec<DataFileMeta>)>> {
    if files.is_empty() {
        return Ok(Vec::new());
    }
    let mut groups = group_vector_files(files)?;
    let options = table.schema().core_options();
    let minimum = options.compaction_min_file_num()?.max(2);
    let mut regions = normal_tasks
        .iter()
        .map(|files| super::row_range(files))
        .collect::<Result<Vec<_>>>()?;
    // Try combined normal tasks first; unsafe field coverage can still permit
    // independent tasks inside their original normal groups.
    regions.extend(
        group_by_overlapping_row_id(normals.to_vec())
            .iter()
            .map(|files| super::row_range(files))
            .collect::<Result<Vec<_>>>()?,
    );
    let mut tasks = Vec::new();
    for (start, end) in regions {
        let candidates = groups_in_region(&mut groups, start, end);
        if candidates.iter().any(|group| group.selected)
            || candidates
                .iter()
                .map(|group| group.files.len())
                .sum::<usize>()
                < minimum
        {
            continue;
        }
        let files = candidates
            .iter()
            .flat_map(|group| &group.files)
            .cloned()
            .collect::<Vec<_>>();
        if !needs_rewrite(&files, options.vector_target_file_size()) {
            continue;
        }
        if let Some(fields) = covered_fields(table, &files).await? {
            for group in candidates {
                group.selected = true;
            }
            tasks.push((fields.iter().map(DataField::id).collect(), files));
        }
    }
    Ok(tasks)
}

pub(super) async fn validate_inputs(
    table: &Table,
    partition: &[u8],
    bucket: i32,
    files: &[DataFileMeta],
    active: &[ManifestEntry],
) -> Result<()> {
    let (first, last) = row_range(files)?;
    if covered_fields(table, files).await?.is_none() {
        return Err(invalid(
            "Every Vector compaction field must cover the entire task range",
        ));
    }
    let names = files
        .iter()
        .map(|file| &file.file_name)
        .collect::<HashSet<_>>();
    let mut normals = Vec::new();
    for entry in active.iter().filter(|entry| {
        entry.partition() == partition
            && entry.bucket() == bucket
            && entry
                .file()
                .row_id_range()
                .is_some_and(|(start, end)| start <= last && end >= first)
    }) {
        if is_vector(entry.file()) && !names.contains(&entry.file().file_name) {
            return Err(invalid(
                "Vector compaction inputs changed or omit overlapping field versions",
            ));
        }
        if is_data_evolution_normal_file(entry.file()) {
            normals.push(entry.file().clone());
        }
    }
    validate_storage_layout(table, &normals).await
}

pub(super) async fn execute(
    table: &Table,
    task: &CompactTask,
    split: &DataSplit,
    partition_path: String,
    resources: Option<ResourceContext>,
) -> Result<Vec<DataFileMeta>> {
    let fields = covered_fields(table, &task.files)
        .await?
        .ok_or_else(|| invalid("Every Vector compaction field must cover the entire task range"))?;
    if fields != current_fields(task)? {
        return Err(invalid(
            "Vector task field identities do not match its historical inputs",
        ));
    }
    let (first, last) = row_range(&task.files)?;
    let (min_sequence, max_sequence) = sequence_bounds(&task.files);
    let sequences = column_sequences(table, &task.files, &fields, max_sequence).await?;
    let options = table.schema().core_options();
    let mut batches = crate::table::data_evolution_reader::read_vector_compaction(
        table,
        split,
        fields.clone(),
        first,
        last,
        resources.clone(),
    )
    .await?;
    let mut writer = DataFileWriter::new(
        table.file_io().clone(),
        table.location().into(),
        partition_path,
        task.bucket,
        table.schema().id(),
        options.vector_target_file_size(),
        options.file_compression().into(),
        options.file_compression_zstd_level(),
        options.write_buffer_size()? as i64,
        format!("vector.{}", options.vector_file_format().unwrap()),
        fields.clone(),
        table.schema().options().clone(),
        Some(1),
        Some(first),
        Some(fields.iter().map(|field| field.name().into()).collect()),
    )?
    .with_resources(resources);
    let result = async {
        let mut rows = 0i64;
        while let Some(batch) = batches.try_next().await? {
            rows += batch.num_rows() as i64;
            writer.write(&batch).await?;
        }
        if rows != last - first + 1 {
            return Err(invalid("Vector compaction physical row count changed"));
        }
        let mut files = writer.prepare_commit().await?;
        let mut next = first;
        for file in &mut files {
            file.first_row_id = Some(next);
            next += file.row_count;
            file.min_sequence_number = min_sequence;
            file.max_sequence_number = max_sequence;
            file.column_max_sequence_numbers = sequences.clone();
        }
        if let Err(error) = validate_outputs(task, &files) {
            writer.delete_files(&files).await;
            return Err(error);
        }
        Ok(files)
    }
    .await;
    if result.is_err() {
        writer.abort().await;
    }
    result
}

fn sequence_bounds(files: &[DataFileMeta]) -> (i64, i64) {
    (
        files
            .iter()
            .map(|file| file.min_sequence_number)
            .min()
            .unwrap(),
        files
            .iter()
            .map(|file| file.max_sequence_number)
            .max()
            .unwrap(),
    )
}

fn validate_metadata(
    schema: &TableSchema,
    inputs: &[DataFileMeta],
    fields: &[DataField],
    outputs: &[DataFileMeta],
) -> Result<()> {
    ensure_supported(&schema.core_options())?;
    let (first, last) = row_range(inputs)?;
    let (min, max) = sequence_bounds(inputs);
    let columns = Some(
        fields
            .iter()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>(),
    );
    let suffix = format!(
        ".vector.{}",
        schema.core_options().vector_file_format().unwrap()
    );
    let mut sorted = outputs.iter().collect::<Vec<_>>();
    sorted.sort_by_key(|file| file.first_row_id);
    let mut next = first;
    for file in sorted {
        validate_file(file)?;
        if !file.file_name.ends_with(&suffix)
            || file.first_row_id != Some(next)
            || file.row_count > last - next + 1
            || file.write_cols != columns
            || file.min_sequence_number != min
            || file.max_sequence_number != max
        {
            return Err(invalid("Vector compaction must preserve consecutive row IDs, fields, format and sequence bounds"));
        }
        next += file.row_count;
    }
    if next != last + 1 {
        return Err(invalid(
            "Vector compaction outputs must cover every physical row slot",
        ));
    }
    Ok(())
}

pub(super) fn validate_outputs(task: &CompactTask, files: &[DataFileMeta]) -> Result<()> {
    validate_metadata(
        &task.context.schema,
        &task.files,
        &current_fields(task)?,
        files,
    )
}

/// Recover task boundaries from consecutive outputs with matching column and
/// sequence metadata.
pub(super) async fn validate_replacement_outputs(
    table: &Table,
    delta: &[ManifestEntry],
) -> Result<()> {
    let mut outputs = BTreeMap::<_, Vec<DataFileMeta>>::new();
    let deleted = delta
        .iter()
        .filter(|entry| *entry.kind() == FileKind::Delete && is_vector(entry.file()))
        .collect::<Vec<_>>();
    for entry in delta
        .iter()
        .filter(|entry| *entry.kind() == FileKind::Add && is_vector(entry.file()))
    {
        let file = entry.file();
        outputs
            .entry((
                entry.partition().to_vec(),
                entry.bucket(),
                file.min_sequence_number,
                file.max_sequence_number,
                file.write_cols.clone(),
                file.column_max_sequence_numbers.clone(),
            ))
            .or_default()
            .push(file.clone());
    }
    let mut consumed = HashSet::new();
    for ((partition, bucket, _, _, _, _), mut files) in outputs {
        files.sort_by_key(|file| file.first_row_id);
        let mut groups: Vec<Vec<DataFileMeta>> = Vec::new();
        for file in files {
            if groups
                .last()
                .and_then(|group| group.last())
                .is_none_or(|previous| {
                    previous
                        .first_row_id
                        .and_then(|first| first.checked_add(previous.row_count))
                        != file.first_row_id
                })
            {
                groups.push(Vec::new());
            }
            groups.last_mut().unwrap().push(file);
        }
        for files in groups {
            let (first, last) = row_range(&files)?;
            let entries = deleted
                .iter()
                .filter(|entry| {
                    entry.partition() == partition
                        && entry.bucket() == bucket
                        && entry
                            .file()
                            .row_id_range()
                            .is_some_and(|(start, end)| start <= last && end >= first)
                })
                .collect::<Vec<_>>();
            let inputs = entries
                .iter()
                .map(|entry| entry.file().clone())
                .collect::<Vec<_>>();
            if inputs.len() < 2 {
                return Err(invalid("Vector output must replace at least two inputs"));
            }
            let fields = covered_fields(table, &inputs).await?.ok_or_else(|| {
                invalid("Every Vector compaction field must cover the entire task range")
            })?;
            validate_metadata(table.schema(), &inputs, &fields, &files)?;
            let sequences =
                column_sequences(table, &inputs, &fields, sequence_bounds(&inputs).1).await?;
            if files
                .iter()
                .any(|file| file.column_max_sequence_numbers != sequences)
            {
                return Err(invalid(
                    "Vector replacement changed per-column sequence metadata",
                ));
            }
            for entry in entries {
                if !consumed.insert((
                    entry.partition().to_vec(),
                    entry.bucket(),
                    entry.file().file_name.clone(),
                )) {
                    return Err(invalid("Vector replacement outputs overlap"));
                }
            }
        }
    }
    if consumed.len() != deleted.len() {
        return Err(invalid(
            "Vector compaction must replace every selected field range",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::tests::file;
    use super::*;

    #[test]
    fn indexed_regions_keep_all_overlapping_versions_and_reject_crossing_groups() {
        let mut groups = group_vector_files(vec![
            file("nested.vector.parquet", 4, 2, 10),
            file("right.vector.parquet", 12, 2, 10),
            file("base.vector.parquet", 0, 10, 10),
            file("extension.vector.parquet", 8, 4, 10),
        ])
        .unwrap();
        assert_eq!(groups.len(), 2);
        for (first, last) in [(0, 9), (4, 5), (10, 13), (13, 13), (14, 20)] {
            assert!(groups_in_region(&mut groups, first, last).is_empty());
        }
        let selected = groups_in_region(&mut groups, 0, 11);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].files.len(), 3);
        selected[0].selected = true;
        let combined = groups_in_region(&mut groups, 0, 13);
        assert_eq!(combined.len(), 2);
        assert!(combined[0].selected);
        assert!(!combined[1].selected);
        assert_eq!(groups_in_region(&mut groups, 12, 13)[0].files.len(), 1);
    }

    #[test]
    fn indexed_regions_handle_many_independent_groups() {
        let mut groups = group_vector_files(
            (0..20_000)
                .rev()
                .map(|index| file(&format!("{index}.vector.parquet"), index * 4, 2, 10))
                .collect(),
        )
        .unwrap();
        for index in 0..20_000 {
            let selected = groups_in_region(&mut groups, index * 4, index * 4 + 3);
            assert_eq!(selected.len(), 1);
            assert_eq!(selected[0].first, index * 4);
        }
    }

    #[test]
    fn overlapping_versions_keep_their_union_but_gaps_are_rejected() {
        assert_eq!(
            row_range(&[
                file("base.vector.parquet", 0, 10, 100),
                file("update.vector.parquet", 5, 10, 10)
            ])
            .unwrap(),
            (0, 14)
        );
        assert!(row_range(&[
            file("left.vector.parquet", 0, 10, 10),
            file("right.vector.parquet", 11, 10, 10)
        ])
        .is_err());
        assert!(row_range(&[file("overflow.vector.parquet", i64::MAX, 1, 10)]).is_err());
        assert!(row_range(&[file("normal.parquet", 0, 10, 10)]).is_err());
    }

    #[test]
    fn only_useful_consolidations_rewrite_rolled_outputs() {
        let mut files = vec![
            file("left.vector.parquet", 0, 10, 1024),
            file("right.vector.parquet", 10, 10, 300),
        ];
        assert!(!needs_rewrite(&files, 1024), "only one undersized tail");
        files[0].file_size = 500;
        assert!(needs_rewrite(&files, 1024), "two undersized files");
        files[0].file_size = 2048;
        files[1].first_row_id = Some(5);
        assert!(
            needs_rewrite(&files, 1024),
            "overlapping versions still need a rewrite"
        );

        let mut files = vec![
            file(
                "custom-00000000-0000-0000-0000-000000000000-0.vector.parquet",
                0,
                10,
                500,
            ),
            file(
                "custom-00000000-0000-0000-0000-000000000000-1.vector.parquet",
                10,
                10,
                300,
            ),
        ];
        for file in &mut files {
            file.file_source = Some(1);
        }
        assert!(!needs_rewrite(&files, 1024));
        files[1].first_row_id = Some(5);
        assert!(needs_rewrite(&files, 1024));
        files[1].first_row_id = Some(10);
        files[1].file_source = Some(0);
        assert!(needs_rewrite(&files, 1024));
        files[1].file_source = Some(1);
        files[1].file_name = "custom-00000000-0000-0000-0000-000000000001-0.vector.parquet".into();
        assert!(needs_rewrite(&files, 1024));
    }
}
