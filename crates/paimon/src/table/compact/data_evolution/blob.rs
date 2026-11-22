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

//! Dedicated BLOB compaction following Java DataEvolutionBlobCompactTask.

use std::collections::{BTreeMap, HashSet};

use futures::TryStreamExt;

use super::{input_fields, validate_file, validate_storage_layout};
use crate::resource::ResourceContext;
use crate::spec::{CoreOptions, DataField, DataFileMeta, FileKind, ManifestEntry};
use crate::table::compact::{invalid, CompactTask};
use crate::table::data_evolution_reader::read_blob_compaction;
use crate::table::data_file_writer::DataFileWriter;
use crate::table::dedicated_format_file_writer::is_blob_or_video_file_name;
use crate::table::source::is_data_evolution_normal_file;
use crate::table::stats_filter::group_by_overlapping_row_id;
use crate::table::{DataSplit, Table};
use crate::{Error, Result};

pub(super) fn is_blob(file: &DataFileMeta) -> bool {
    file.file_name.to_ascii_lowercase().ends_with(".blob")
}

pub(super) fn ensure_supported(options: &CoreOptions<'_>) -> Result<()> {
    if options.blob_target_file_size() <= 0 {
        return Err(Error::ConfigInvalid {
            message: "BLOB compaction requires a positive blob.target-file-size".into(),
        });
    }
    if !options.video_frame_fields().is_empty() {
        return Err(Error::Unsupported {
            message: "Data Evolution video-file compaction is not supported".into(),
        });
    }
    Ok(())
}

pub(super) async fn input_field(table: &Table, file: &DataFileMeta) -> Result<DataField> {
    if file.file_name.to_ascii_lowercase().ends_with(".video") {
        return Err(Error::Unsupported {
            message: "Data Evolution video-file compaction is not supported".into(),
        });
    }
    validate_file(file)?;
    if !is_blob(file) || file.write_cols.as_ref().is_none_or(|cols| cols.len() != 1) {
        return Err(invalid(
            "BLOB compaction requires one write column per dedicated file",
        ));
    }
    let mut fields = input_fields(table, file).await?;
    if fields.len() != 1 || !fields[0].data_type().is_blob_file_field() {
        return Err(invalid("Invalid BLOB compaction input field"));
    }
    Ok(fields.remove(0))
}

async fn validate_fields(table: &Table, files: &[DataFileMeta], field: &DataField) -> Result<()> {
    for file in files {
        let physical = input_field(table, file).await?;
        if physical.id() != field.id() {
            return Err(invalid(
                "BLOB compaction field ID does not match its historical inputs",
            ));
        }
        if physical.data_type() != field.data_type() {
            return Err(Error::Unsupported {
                message: "BLOB compaction cannot migrate field types".into(),
            });
        }
    }
    Ok(())
}

fn current_field(task: &CompactTask) -> Result<DataField> {
    let inline = task.context.schema.core_options().blob_inline_fields();
    task.context
        .schema
        .fields()
        .iter()
        .find(|field| {
            Some(field.id()) == task.blob_field_id
                && field.data_type().is_blob_file_field()
                && !inline.contains(field.name())
        })
        .cloned()
        .ok_or_else(|| invalid("BLOB compaction field is not a current dedicated BLOB field"))
}

pub(super) fn validate_task(task: &CompactTask) -> Result<()> {
    ensure_supported(&task.context.schema.core_options())?;
    if task.files.len() < 2 || !task.files.iter().all(is_blob) {
        return Err(invalid(
            "BLOB compaction requires at least two dedicated BLOB files",
        ));
    }
    current_field(task)?;
    row_range(&task.files)?;
    Ok(())
}

/// Unlike normal files, BLOB versions may overlap only part of another file.
pub(super) fn row_range(files: &[DataFileMeta]) -> Result<(i64, i64)> {
    let mut ranges = Vec::with_capacity(files.len());
    for file in files {
        validate_file(file)?;
        if !is_blob(file) {
            return Err(invalid("BLOB compaction cannot include other file types"));
        }
        ranges.push(
            file.row_id_range()
                .ok_or_else(|| invalid("Missing BLOB row-ID range"))?,
        );
    }
    ranges.sort_unstable();
    let (first, mut last) = *ranges
        .first()
        .ok_or_else(|| invalid("Empty BLOB compaction range"))?;
    for &(start, end) in &ranges[1..] {
        if start > last + 1 {
            return Err(invalid("BLOB compaction cannot cross a row-ID gap"));
        }
        last = last.max(end);
    }
    Ok((first, last))
}

pub(super) async fn plan(
    table: &Table,
    normal_files: &[DataFileMeta],
    normal_tasks: &[Vec<DataFileMeta>],
    files: Vec<DataFileMeta>,
) -> Result<Vec<(i32, Vec<DataFileMeta>)>> {
    let mut regions = normal_tasks
        .iter()
        .map(|files| super::row_range(files))
        .collect::<Result<Vec<_>>>()?;
    let selected = normal_tasks
        .iter()
        .flatten()
        .map(|file| &file.file_name)
        .collect::<HashSet<_>>();
    for group in group_by_overlapping_row_id(normal_files.to_vec()) {
        let range = super::row_range(&group)?;
        if !selected.contains(&group[0].file_name) {
            regions.push(range);
        }
    }
    regions.sort_unstable();
    let inline = table.schema().core_options().blob_inline_fields();
    let dedicated_field_ids = table
        .schema()
        .fields()
        .iter()
        .filter(|field| field.data_type().is_blob_file_field() && !inline.contains(field.name()))
        .map(DataField::id)
        .collect::<HashSet<_>>();
    let mut by_field = BTreeMap::<i32, Vec<DataFileMeta>>::new();
    for file in files {
        let field_id = input_field(table, &file).await?.id();
        if dedicated_field_ids.contains(&field_id) {
            by_field.entry(field_id).or_default().push(file);
        }
    }
    let target_file_size = table.schema().core_options().blob_target_file_size();
    let mut tasks = Vec::new();
    for (field_id, files) in by_field {
        let mut by_region = BTreeMap::<usize, Vec<DataFileMeta>>::new();
        // Keep overlapping versions together so assigning the output's maximum
        // sequence cannot change which value wins.
        for group in group_by_overlapping_row_id(files) {
            let (first, last) = row_range(&group)?;
            if let Some(region) = regions
                .partition_point(|&(start, _)| start <= first)
                .checked_sub(1)
            {
                if regions[region].1 >= last {
                    by_region.entry(region).or_default().extend(group);
                }
            }
        }
        for files in by_region.into_values() {
            tasks.extend(
                pack(files, target_file_size)?
                    .into_iter()
                    .map(|files| (field_id, files)),
            );
        }
    }
    Ok(tasks)
}

fn pack(files: Vec<DataFileMeta>, target_file_size: i64) -> Result<Vec<Vec<DataFileMeta>>> {
    let mut tasks = Vec::new();
    let mut pending = Vec::new();
    let mut pending_size = 0i128;
    let mut previous_end: Option<i64> = None;
    let drain = |pending: &mut Vec<DataFileMeta>, tasks: &mut Vec<Vec<DataFileMeta>>| {
        let files = std::mem::take(pending);
        if files.len() >= 2 {
            tasks.push(files);
        }
    };
    for mut group in group_by_overlapping_row_id(files) {
        group.sort_by(|a, b| {
            (a.first_row_id, a.max_sequence_number, &a.file_name).cmp(&(
                b.first_row_id,
                b.max_sequence_number,
                &b.file_name,
            ))
        });
        let range = row_range(&group)?;
        let large = group.len() == 1 && group[0].file_size >= target_file_size;
        if large || previous_end.is_some_and(|end| end + 1 != range.0) {
            drain(&mut pending, &mut tasks);
            pending_size = 0;
        }
        previous_end = Some(range.1);
        if large {
            continue;
        }
        pending_size += group
            .iter()
            .map(|file| i128::from(file.file_size))
            .sum::<i128>();
        pending.extend(group);
        if pending_size >= i128::from(target_file_size) && pending.len() >= 2 {
            drain(&mut pending, &mut tasks);
            pending_size = 0;
        }
    }
    drain(&mut pending, &mut tasks);
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
    let expected_field_id = input_field(table, &files[0]).await?.id();
    for file in &files[1..] {
        if input_field(table, file).await?.id() != expected_field_id {
            return Err(invalid(
                "BLOB compaction inputs must have the same field ID",
            ));
        }
    }
    let names = files
        .iter()
        .map(|file| &file.file_name)
        .collect::<HashSet<_>>();
    let normals = active
        .iter()
        .filter(|entry| {
            entry.partition() == partition
                && entry.bucket() == bucket
                && is_data_evolution_normal_file(entry.file())
                && entry
                    .file()
                    .row_id_range()
                    .is_some_and(|(start, end)| start <= last && end >= first)
        })
        .map(|entry| entry.file().clone())
        .collect::<Vec<_>>();
    validate_storage_layout(table, &normals).await?;
    for entry in active.iter().filter(|entry| {
        entry.partition() == partition
            && entry.bucket() == bucket
            && is_blob_or_video_file_name(&entry.file().file_name)
            && entry
                .file()
                .row_id_range()
                .is_some_and(|(start, end)| start <= last && end >= first)
    }) {
        if !names.contains(&entry.file().file_name)
            && input_field(table, entry.file()).await?.id() == expected_field_id
        {
            return Err(invalid(
                "BLOB compaction inputs changed or omit overlapping field versions",
            ));
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
    let field = current_field(task)?;
    validate_fields(table, &task.files, &field).await?;
    let (first, last) = row_range(&task.files)?;
    let options = table.schema().core_options();
    let mut batches = read_blob_compaction(
        split,
        field.clone(),
        table.file_io().clone(),
        first,
        last,
        options.read_batch_size()?,
    )?;
    let mut writer = DataFileWriter::new(
        table.file_io().clone(),
        table.location().to_string(),
        partition_path,
        task.bucket,
        table.schema().id(),
        i64::MAX,
        "none".into(),
        0,
        options.write_buffer_size()? as i64,
        "blob".into(),
        vec![field.clone()],
        table.schema().options().clone(),
        Some(1),
        Some(first),
        Some(vec![field.name().into()]),
    )?
    .with_resources(resources);
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
    let sequences = column_sequences(&task.files, max_sequence);
    let result = async {
        let mut row_count = 0i64;
        while let Some(batch) = batches.try_next().await? {
            row_count += batch.num_rows() as i64;
            writer.write(&batch).await?;
        }
        if row_count != last - first + 1 {
            return Err(invalid("BLOB compaction row count changed"));
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
    if result.is_err() {
        writer.abort().await;
    }
    result
}

pub(super) fn validate_outputs(task: &CompactTask, files: &[DataFileMeta]) -> Result<()> {
    let field = current_field(task)?;
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
        || !is_blob(&files[0])
        || files[0].row_id_range() != Some(row_range(&task.files)?)
        || files[0].min_sequence_number != min_sequence
        || files[0].max_sequence_number != max_sequence
        || files[0].column_max_sequence_numbers != column_sequences(&task.files, max_sequence)
        || files[0].write_cols != Some(vec![field.name().into()])
    {
        return Err(invalid("BLOB compaction must preserve its row-ID range, field and sequence bounds in one output"));
    }
    Ok(())
}

fn column_sequences(files: &[DataFileMeta], max_sequence: i64) -> Option<Vec<i64>> {
    let sequence = files
        .iter()
        .map(|file| {
            file.column_max_sequence_numbers
                .as_ref()
                .filter(|values| values.len() == 1)
                .map(|values| values[0])
                .filter(|value| *value > 0 && *value <= file.max_sequence_number)
                .unwrap_or(file.max_sequence_number)
        })
        .max()
        .unwrap();
    (sequence != max_sequence).then_some(vec![sequence])
}

/// Re-resolve field identities and types at publication because serialized
/// worker results may mislabel historical inputs.
pub(super) async fn validate_replacement_outputs(
    table: &Table,
    delta: &[ManifestEntry],
) -> Result<()> {
    let mut deleted = BTreeMap::<_, Vec<DataFileMeta>>::new();
    for entry in delta
        .iter()
        .filter(|entry| *entry.kind() == FileKind::Delete && is_blob(entry.file()))
    {
        let field_id = input_field(table, entry.file()).await?.id();
        deleted
            .entry((entry.partition().to_vec(), entry.bucket(), field_id))
            .or_default()
            .push(entry.file().clone());
    }
    let mut consumed = HashSet::new();
    for entry in delta
        .iter()
        .filter(|entry| *entry.kind() == FileKind::Add && is_blob(entry.file()))
    {
        let field = input_field(table, entry.file()).await?;
        let range = row_range(std::slice::from_ref(entry.file()))?;
        let inputs = deleted
            .get(&(entry.partition().to_vec(), entry.bucket(), field.id()))
            .ok_or_else(|| invalid("BLOB output does not replace the same field ID"))?
            .iter()
            .filter(|file| {
                file.row_id_range()
                    .is_some_and(|(start, end)| start <= range.1 && end >= range.0)
            })
            .cloned()
            .collect::<Vec<_>>();
        validate_fields(table, &inputs, &field).await?;
        let min_sequence = inputs.iter().map(|file| file.min_sequence_number).min();
        let max_sequence = inputs.iter().map(|file| file.max_sequence_number).max();
        if inputs.len() < 2
            || row_range(&inputs)? != range
            || Some(entry.file().min_sequence_number) != min_sequence
            || Some(entry.file().max_sequence_number) != max_sequence
            || entry.file().column_max_sequence_numbers
                != column_sequences(&inputs, max_sequence.unwrap())
        {
            return Err(invalid(
                "BLOB replacement changed field ranges or sequence metadata",
            ));
        }
        for input in inputs {
            if !consumed.insert((entry.partition().to_vec(), entry.bucket(), input.file_name)) {
                return Err(invalid("BLOB replacement outputs overlap"));
            }
        }
    }
    if consumed.len() != deleted.values().map(Vec::len).sum::<usize>() {
        return Err(invalid(
            "BLOB compaction must replace every selected field range",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::tests::file;
    use super::*;

    #[test]
    fn java_blob_target_size_boundaries_keep_overlap_groups_intact() {
        let files = [400, 400, 1024, 512, 512, 600, 600, 400, 500]
            .into_iter()
            .enumerate()
            .map(|(i, bytes)| file(&format!("{i}.blob"), i as i64 * 10, 10, bytes))
            .collect();
        let tasks = pack(files, 1024).unwrap();
        assert_eq!(
            tasks
                .iter()
                .map(|files| row_range(files).unwrap())
                .collect::<Vec<_>>(),
            vec![(0, 19), (30, 49), (50, 69), (70, 89)]
        );
        let mut update = file("update.blob", 5, 10, 1);
        update.max_sequence_number = 2;
        let tasks = pack(vec![file("base.blob", 0, 20, 2048), update], 1024).unwrap();
        assert_eq!(
            tasks.len(),
            1,
            "a large baseline and its overlapping delta stay together"
        );
        assert_eq!(tasks[0].len(), 2);
        assert_eq!(row_range(&tasks[0]).unwrap(), (0, 19));
    }

    #[test]
    fn blob_gaps_singletons_and_invalid_ranges() {
        let tasks = pack(
            vec![
                file("a.blob", 0, 2, 1),
                file("b.blob", 2, 2, 1),
                file("c.blob", 5, 2, 1),
                file("d.blob", 7, 2, 1),
            ],
            1024,
        )
        .unwrap();
        assert_eq!(
            tasks
                .iter()
                .map(|files| row_range(files).unwrap())
                .collect::<Vec<_>>(),
            vec![(0, 3), (5, 8)]
        );
        assert!(
            pack(vec![file("a.blob", 0, 2, 1), file("b.blob", 4, 2, 1)], 1024)
                .unwrap()
                .is_empty()
        );
        assert!(pack(
            vec![file("a.blob", 0, 2, 1024), file("b.blob", 2, 2, 1)],
            1024
        )
        .unwrap()
        .is_empty());
        assert!(row_range(&[file("overflow.blob", i64::MAX, 1, 1)]).is_err());
        assert!(row_range(&[file("a.blob", 0, 2, 1), file("b.blob", 3, 2, 1)]).is_err());
    }
}
