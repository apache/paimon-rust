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

//! Snapshot-bound partition rewrites for postpone tables. Pending rows are
//! replayed after real-bucket history, sorted in bounded batches, then merged
//! through the ordinary PK compactor. No staged files enter a manifest.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{BooleanArray, Int64Array, RecordBatch};
use arrow_ord::sort::{lexsort_to_indices, SortColumn};
use futures::{StreamExt, TryStreamExt};

use super::{invalid, primary_key, validate_file, CompactContext, CompactResult, CompactTask};
use crate::resource::ResourceContext;
use crate::spec::{
    bucket_path_under, BinaryRow, DataFileMeta, FileKind, ManifestEntry, PartitionComputer,
    TableSchema, POSTPONE_BUCKET,
};
use crate::table::data_file_path_factory::DataFilePathFactory;
use crate::table::data_file_reader::DataFileReader;
use crate::table::kv_data_file_writer::{KeyValueFileOptions, KeyValueRollingWriter};
use crate::table::sort_merge::{
    BufferedBatch, MergeFunction, MergeResult, MergeRow, SortMergeReaderBuilder,
};
use crate::table::table_read::{configured_mosaic_prefetch, configured_parquet_read_budget};
use crate::table::{BucketRouter, DataSplit, PostponeBucketPlan, Table};
use crate::Result;

const SORT_FAN_IN: usize = 128;

fn layout(entries: &[&ManifestEntry], schema: &TableSchema) -> Result<i32> {
    let mut known = None;
    for entry in entries {
        validate_file(entry.file(), true)?;
        if entry.bucket() == POSTPONE_BUCKET {
            if entry.total_buckets() != POSTPONE_BUCKET || entry.file().level != 0 {
                return Err(invalid("Invalid postpone input layout"));
            }
            continue;
        }
        let count = entry.total_buckets();
        if entry.bucket() < 0
            || count <= entry.bucket()
            || count <= 0
            || known.is_some_and(|previous| previous != count)
        {
            return Err(invalid("Conflicting postpone partition bucket counts"));
        }
        known = Some(count);
    }
    if let Some(count) = known {
        return Ok(count);
    }
    let options = schema.core_options();
    if let Some(count) = options.postpone_default_bucket_num()? {
        return Ok(count);
    }
    let target_rows = options.postpone_target_row_num_per_bucket()?;
    let target = match target_rows {
        Some(rows) => rows,
        None => options.postpone_target_size_per_bucket()?,
    } as u128;
    let size = entries
        .iter()
        .map(|entry| {
            (if target_rows.is_some() {
                entry.file().row_count
            } else {
                entry.file().file_size
            }) as u128
        })
        .sum::<u128>();
    i32::try_from(size.div_ceil(target).max(1))
        .map_err(|_| invalid("Inferred postpone bucket count exceeds i32::MAX"))
}

pub(super) fn plan_tasks(
    entries: &[ManifestEntry],
    context: &CompactContext,
) -> Result<Vec<CompactTask>> {
    let mut partitions = BTreeMap::<_, Vec<_>>::new();
    for entry in entries {
        partitions
            .entry(entry.partition().to_vec())
            .or_default()
            .push(entry);
    }
    let mut tasks = Vec::new();
    for (partition, mut entries) in partitions {
        if !entries.iter().any(|entry| entry.file().level == 0) {
            continue;
        }
        let total_buckets = layout(&entries, &context.schema)?;
        entries.sort_by(|a, b| {
            (a.bucket(), &a.file().file_name).cmp(&(b.bucket(), &b.file().file_name))
        });
        tasks.push(CompactTask {
            context: context.clone(),
            task_id: tasks.len(),
            partition,
            bucket: POSTPONE_BUCKET,
            total_buckets,
            output_level: Some(
                entries
                    .iter()
                    .map(|entry| entry.file().level)
                    .max()
                    .unwrap_or(0)
                    .max(1),
            ),
            files: entries.iter().map(|entry| entry.file().clone()).collect(),
            input_buckets: entries.iter().map(|entry| entry.bucket()).collect(),
        });
    }
    Ok(tasks)
}

pub(super) fn validate_task(task: &CompactTask) -> Result<()> {
    if task.is_postpone() {
        if task.bucket != POSTPONE_BUCKET
            || task.total_buckets <= 0
            || task.input_buckets.len() != task.files.len()
            || task.output_level.is_none()
            || task.input_buckets.iter().any(|&bucket| {
                bucket != POSTPONE_BUCKET && !(0..task.total_buckets).contains(&bucket)
            })
        {
            return Err(invalid("Invalid postpone compaction task"));
        }
    } else if !task.input_buckets.is_empty() {
        return Err(invalid(
            "Only postpone tasks may contain source bucket mappings",
        ));
    }
    Ok(())
}

pub(super) fn validate_inputs(task: &CompactTask, entries: &[ManifestEntry]) -> Result<()> {
    let entries = entries
        .iter()
        .filter(|entry| entry.partition() == task.partition)
        .cloned()
        .collect::<Vec<_>>();
    let expected = plan_tasks(&entries, &task.context)?
        .into_iter()
        .find(|candidate| candidate.partition == task.partition)
        .ok_or_else(|| invalid("No postpone compaction is eligible"))?;
    if expected.files != task.files
        || expected.input_buckets != task.input_buckets
        || expected.total_buckets != task.total_buckets
        || expected.output_level != task.output_level
    {
        return Err(invalid(
            "Postpone compaction must include the complete captured partition",
        ));
    }
    Ok(())
}

pub(super) fn validate_result(result: &CompactResult) -> Result<()> {
    let task = &result.task;
    if task.is_postpone() {
        if result.file_buckets.len() != result.files.len()
            || result.changelog_buckets.len() != result.changelog_files.len()
            || result
                .file_buckets
                .iter()
                .chain(&result.changelog_buckets)
                .any(|bucket| !(0..task.total_buckets).contains(bucket))
        {
            return Err(invalid("Invalid postpone compaction output buckets"));
        }
    } else if !result.file_buckets.is_empty() || !result.changelog_buckets.is_empty() {
        return Err(invalid(
            "Only postpone results may contain output bucket mappings",
        ));
    }
    Ok(())
}

pub(super) fn validate_changes(
    schema: &TableSchema,
    changes: &[ManifestEntry],
    changelog: &[ManifestEntry],
    captured: &[ManifestEntry],
) -> Result<()> {
    let partitions = changes
        .iter()
        .filter(|entry| *entry.kind() == FileKind::Delete)
        .map(|entry| entry.partition())
        .collect::<HashSet<_>>();
    let mut outputs = HashMap::new();
    for partition in partitions {
        let inputs = captured
            .iter()
            .filter(|entry| entry.partition() == partition)
            .collect::<Vec<_>>();
        let count = layout(&inputs, schema)?;
        let level = inputs
            .iter()
            .map(|entry| entry.file().level)
            .max()
            .unwrap_or(0)
            .max(1);
        let removed = changes
            .iter()
            .filter(|entry| *entry.kind() == FileKind::Delete && entry.partition() == partition)
            .collect::<Vec<_>>();
        let input_files = inputs
            .iter()
            .map(|entry| ((entry.bucket(), entry.file().file_name.as_str()), *entry))
            .collect::<HashMap<_, _>>();
        if !inputs.iter().any(|entry| entry.file().level == 0)
            || inputs.len() != removed.len()
            || removed.iter().any(|entry| {
                !input_files
                    .get(&(entry.bucket(), entry.file().file_name.as_str()))
                    .is_some_and(|input| {
                        input.total_buckets() == entry.total_buckets()
                            && input.file() == entry.file()
                    })
            })
        {
            return Err(invalid(
                "Postpone compaction must replace the complete captured partition",
            ));
        }
        outputs.insert(partition, (count, level));
    }
    for entry in changes
        .iter()
        .filter(|entry| *entry.kind() == FileKind::Add)
        .chain(changelog)
    {
        if !outputs
            .get(entry.partition())
            .is_some_and(|&(count, level)| {
                entry.bucket() >= 0
                    && entry.bucket() < count
                    && entry.total_buckets() == count
                    && entry.file().level == level
            })
        {
            return Err(invalid(
                "Postpone compaction output does not match captured layout",
            ));
        }
    }
    Ok(())
}

/// Replaying pending rows allocates sequences after the captured real files.
/// A concurrent real-bucket write could allocate the same sequences.
pub(in crate::table) fn validate_concurrent_changes(
    changes: &[ManifestEntry],
    active: &[ManifestEntry],
) -> Result<()> {
    let partitions = changes
        .iter()
        .map(|entry| entry.partition())
        .collect::<HashSet<_>>();
    let removed = changes
        .iter()
        .filter(|entry| *entry.kind() == FileKind::Delete)
        .map(|entry| {
            (
                entry.partition(),
                entry.bucket(),
                entry.file().file_name.as_str(),
            )
        })
        .collect::<HashSet<_>>();
    if active.iter().any(|entry| {
        entry.bucket() >= 0
            && partitions.contains(entry.partition())
            && !removed.contains(&(
                entry.partition(),
                entry.bucket(),
                entry.file().file_name.as_str(),
            ))
    }) {
        return Err(invalid(
            "Real-bucket data changed after postpone compaction was planned",
        ));
    }
    Ok(())
}

fn split(
    table: &Table,
    task: &CompactTask,
    partition_path: &str,
    bucket: i32,
    files: Vec<DataFileMeta>,
) -> Result<DataSplit> {
    DataSplit::builder()
        .with_snapshot(task.snapshot_id())
        .with_partition(BinaryRow::from_serialized_bytes(&task.partition)?)
        .with_bucket(bucket)
        .with_total_buckets(task.total_buckets)
        .with_bucket_path(bucket_path_under(
            &table.data_file_location(),
            partition_path,
            bucket,
        ))
        .with_data_files(files)
        .build()
}

async fn cleanup(
    table: &Table,
    task: &CompactTask,
    partition_path: &str,
    files: &[(i32, DataFileMeta)],
) {
    for (bucket, file) in files {
        if let Ok(split) = split(table, task, partition_path, *bucket, vec![]) {
            let path = split.data_file_path(file);
            if let Some((parent, _)) = path.rsplit_once('/') {
                for extra in &file.extra_files {
                    let _ = table
                        .file_io()
                        .delete_file(&format!("{parent}/{extra}"))
                        .await;
                }
            }
            let _ = table.file_io().delete_file(&path).await;
        }
    }
}

async fn stage(
    table: &Table,
    partition_path: &str,
    bucket: i32,
    batch: &RecordBatch,
    resources: Option<&ResourceContext>,
) -> Result<Vec<DataFileMeta>> {
    let schema = table.schema();
    let indices = schema
        .trimmed_primary_keys()
        .iter()
        .map(|key| {
            schema
                .fields()
                .iter()
                .position(|field| field.name() == key)
                .unwrap()
        })
        .collect::<Vec<_>>();
    let mut sort_columns = indices
        .iter()
        .map(|&index| SortColumn {
            values: batch.column(index + 2).clone(),
            options: None,
        })
        .collect::<Vec<_>>();
    sort_columns.push(SortColumn {
        values: batch.column(0).clone(),
        options: None,
    });
    let order =
        lexsort_to_indices(&sort_columns, None).map_err(|error| invalid(&error.to_string()))?;
    let batch = arrow_select::take::take_record_batch(batch, &order)
        .map_err(|error| invalid(&error.to_string()))?;
    write_staged(
        table,
        partition_path,
        bucket,
        futures::stream::once(async { Ok(batch) }).boxed(),
        resources,
    )
    .await
}

async fn write_staged(
    table: &Table,
    partition_path: &str,
    bucket: i32,
    mut batches: crate::table::ArrowRecordBatchStream,
    resources: Option<&ResourceContext>,
) -> Result<Vec<DataFileMeta>> {
    let schema = table.schema();
    let options = schema.core_options();
    let indices = schema
        .trimmed_primary_keys()
        .iter()
        .map(|key| {
            schema
                .fields()
                .iter()
                .position(|field| field.name() == key)
                .unwrap()
        })
        .collect::<Vec<_>>();
    let types = indices
        .iter()
        .map(|&i| schema.fields()[i].data_type().clone())
        .collect::<Vec<_>>();
    let paths =
        DataFilePathFactory::new(table.location(), partition_path, bucket, schema.options())?;
    let mut writer = KeyValueRollingWriter::new(
        KeyValueFileOptions {
            file_io: table.file_io(),
            paths: &paths,
            table_options: schema.options(),
            value_fields: schema.fields(),
            primary_key_indices: &indices,
            primary_key_types: &types,
            schema_id: schema.id(),
            file_format: "parquet",
            file_prefix: "postpone-sort-",
            file_compression: "zstd",
            file_compression_zstd_level: options.file_compression_zstd_level(),
            write_buffer_size: options.write_buffer_size()? as i64,
            level: 0,
            file_source: 1,
            is_changelog: false,
            index_options: None,
            managed_blob_enabled: false,
            resources,
        },
        i64::MAX,
        i64::MAX,
    );
    let result = async {
        while let Some(batch) = batches.try_next().await? {
            writer.write(&batch).await?;
        }
        writer.prepare_commit().await
    }
    .await;
    if result.is_err() {
        writer.abort().await;
    }
    result
}

/// Include the unique replay sequence in the sort key so intermediate merges
/// preserve every physical row. Reducing keys here would change stateful engines
/// and lookup before-images before they see the real-bucket history.
struct PreserveRow;

impl MergeFunction for PreserveRow {
    fn merge(
        &self,
        rows: &[MergeRow],
        _batches: &[BufferedBatch],
        _columns: &[usize],
        _schema: &arrow_schema::SchemaRef,
    ) -> Result<MergeResult> {
        if rows.len() != 1 {
            return Err(invalid("Duplicate postpone replay sequence"));
        }
        Ok(MergeResult::SourceRow {
            batch_idx: rows[0].batch_idx,
            row_idx: rows[0].row_idx,
        })
    }
}

async fn merge_staged(
    table: &Table,
    task: &CompactTask,
    partition_path: &str,
    bucket: i32,
    files: &[DataFileMeta],
    resources: Option<ResourceContext>,
) -> Result<Vec<DataFileMeta>> {
    let schema = crate::arrow::build_read_arrow_schema(&primary_key::read_type(table))?;
    let mut keys = table
        .schema()
        .trimmed_primary_keys()
        .iter()
        .map(|key| {
            schema
                .index_of(key)
                .map_err(|error| invalid(&error.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;
    keys.push(0);
    let values = (0..schema.fields().len())
        .filter(|index| !keys.contains(index))
        .collect::<Vec<_>>();
    let projection = keys.iter().chain(&values).copied().collect::<Vec<_>>();
    let output_schema = Arc::new(
        schema
            .project(&projection)
            .map_err(|error| invalid(&error.to_string()))?,
    );
    let reorder = (0..schema.fields().len())
        .map(|index| projection.iter().position(|&value| value == index).unwrap())
        .collect::<Vec<_>>();
    let source = split(table, task, partition_path, bucket, vec![])?;
    let streams = files
        .iter()
        .map(|file| {
            primary_key::new_reader(table, resources.clone())?
                .read_physical_file(source.clone(), file.clone())
        })
        .collect::<Result<Vec<_>>>()?;
    let batches = SortMergeReaderBuilder::new(
        streams,
        schema.clone(),
        keys,
        0,
        1,
        vec![],
        values,
        output_schema,
        Box::new(PreserveRow),
    )
    .with_batch_size(table.schema().core_options().read_batch_size()?)
    .build()?;
    let batches = batches
        .map(move |batch| {
            batch.and_then(|batch| {
                batch
                    .project(&reorder)
                    .map_err(|error| invalid(&error.to_string()))
            })
        })
        .boxed();
    write_staged(table, partition_path, bucket, batches, resources.as_ref()).await
}

pub(super) async fn execute(
    table: &Table,
    task: &CompactTask,
    resources: Option<ResourceContext>,
) -> Result<CompactResult> {
    let schema = table.schema();
    let options = schema.core_options();
    let partition = BinaryRow::from_serialized_bytes(&task.partition)?;
    let deletions = if options.deletion_vectors_enabled() {
        Some(
            primary_key::deletion_vectors::CapturedDeletionVectors::new(
                table,
                &task.context.snapshot,
            )
            .await?,
        )
    } else {
        None
    };
    let partition_path = PartitionComputer::new(
        schema.partition_keys(),
        schema.fields(),
        options.partition_default_name(),
        options.legacy_partition_name(),
    )?
    .generate_partition_path(&partition)?;
    let router = BucketRouter::new(schema)?.with_bucket_plan(PostponeBucketPlan::from_counts(
        HashMap::from([(task.partition.clone(), task.total_buckets)]),
    ))?;
    let mut buckets = BTreeMap::<i32, Vec<DataFileMeta>>::new();
    let mut pending = Vec::new();
    let mut next_sequence = 0i64;
    for (file, &bucket) in task.files.iter().zip(&task.input_buckets) {
        if bucket == POSTPONE_BUCKET {
            let (writer, _) = file
                .file_name
                .rsplit_once("-w-")
                .ok_or_else(|| invalid("Invalid postpone writer file name"))?;
            pending.push((writer, file));
        } else {
            next_sequence = next_sequence.max(
                file.max_sequence_number
                    .checked_add(1)
                    .ok_or_else(|| invalid("Postpone sequence number overflow"))?,
            );
            buckets.entry(bucket).or_default().push(file.clone());
        }
    }
    // Writer-local sequences and file counters break creation-time ties.
    pending.sort_by_key(|(writer, file)| {
        (
            *writer,
            file.creation_time,
            file.min_sequence_number,
            file.file_name
                .rsplit('-')
                .next()
                .and_then(|tail| tail.split('.').next()?.parse::<u64>().ok()),
        )
    });
    let fields = primary_key::read_type(table);
    let budget = configured_parquet_read_budget(table)?;
    let budget = match &resources {
        Some(resources) => budget.with_resources(resources.clone()).without_prefetch(),
        None => budget.without_prefetch(),
    };
    let reader = DataFileReader::new(
        table.file_io().clone(),
        table.schema_manager().clone(),
        schema.id(),
        schema.fields().to_vec(),
        |fields| Ok(primary_key::read_type_fields(fields)),
        fields,
        vec![],
    )
    .with_batch_size(Some(options.read_batch_size()?))
    .with_parquet_read_budget(Some(Arc::new(budget)))
    .with_table_options(schema.options().clone())
    .with_mosaic_prefetch(configured_mosaic_prefetch(table)?);
    let mut staged = Vec::new();
    let mut pending_runs = BTreeMap::<i32, Vec<DataFileMeta>>::new();
    let mut output = Vec::new();
    let mut changelog_output = Vec::new();
    let result = async {
        for (_, file) in pending {
            let source = split(
                table,
                task,
                &partition_path,
                POSTPONE_BUCKET,
                vec![file.clone()],
            )?;
            let deletion = match &deletions {
                Some(deletions) => deletions.read_vector(table, &source, file).await?,
                None => None,
            };
            let mut batches = reader.clone().read(std::slice::from_ref(&source))?;
            // ROW carries no field schema. Older Rust pending writers omitted
            // duplicated key columns, while Java writes the ordinary KV layout.
            // Probe before accepting any rows; strict ROW decoding rejects a
            // layout that leaves bytes behind or crosses a physical row boundary.
            let first = match batches.try_next().await {
                Err(_) if file.file_name.ends_with(".row") => {
                    batches = primary_key::new_reader(table, resources.clone())?
                        .read_physical_file(source, file.clone())?;
                    batches.try_next().await?
                }
                result => result?,
            };
            let mut batches = futures::stream::iter(first.map(Ok)).chain(batches).boxed();
            let mut rows = 0i64;
            while let Some(batch) = batches.try_next().await? {
                let start = rows as u64;
                rows += batch.num_rows() as i64;
                // Decode and count physical rows before filtering. This also
                // covers the Java ROW fallback, whose physical reader deliberately
                // ignores split masks to preserve lookup ordinals.
                let batch = if let Some(deletion) = &deletion {
                    let live = BooleanArray::from_iter(
                        (0..batch.num_rows())
                            .map(|row| Some(!deletion.is_deleted(start + row as u64))),
                    );
                    arrow_select::filter::filter_record_batch(&batch, &live)
                        .map_err(|error| invalid(&error.to_string()))?
                } else {
                    batch
                };
                if batch.num_rows() == 0 {
                    continue;
                }
                let end = next_sequence
                    .checked_add(batch.num_rows() as i64)
                    .ok_or_else(|| invalid("Postpone sequence number overflow"))?;
                let mut columns = batch.columns().to_vec();
                columns[0] = Arc::new(Int64Array::from_iter_values(next_sequence..end));
                next_sequence = end;
                let batch = RecordBatch::try_new(batch.schema(), columns)
                    .map_err(|error| invalid(&error.to_string()))?;
                let logical = batch
                    .project(&(2..batch.num_columns()).collect::<Vec<_>>())
                    .map_err(|error| invalid(&error.to_string()))?;
                let assigned = router.route(&logical)?;
                let mut groups = BTreeMap::<i32, Vec<usize>>::new();
                for row in 0..batch.num_rows() {
                    groups.entry(assigned.value(row)).or_default().push(row);
                }
                for (bucket, rows) in groups {
                    let batch = crate::table::table_write::take_rows(&batch, &rows)?;
                    let files =
                        stage(table, &partition_path, bucket, &batch, resources.as_ref()).await?;
                    staged.extend(files.iter().cloned().map(|file| (bucket, file)));
                    pending_runs.entry(bucket).or_default().extend(files);
                }
            }
            if rows != file.row_count {
                return Err(invalid("Postpone input row count changed"));
            }
        }
        for (bucket, mut files) in pending_runs {
            while files.len() > SORT_FAN_IN {
                let mut merged = Vec::new();
                for group in files.chunks(SORT_FAN_IN) {
                    let next = merge_staged(
                        table,
                        task,
                        &partition_path,
                        bucket,
                        group,
                        resources.clone(),
                    )
                    .await?;
                    staged.extend(next.iter().cloned().map(|file| (bucket, file)));
                    merged.extend(next);
                }
                files = merged;
            }
            buckets.entry(bucket).or_default().extend(files);
        }
        for (bucket, files) in buckets {
            let source = split(table, task, &partition_path, bucket, files.clone())?;
            let source = match &deletions {
                Some(deletions) => deletions.with_deletion_files(table, &source)?,
                None => source,
            };
            let mut bucket_task = task.clone();
            bucket_task.bucket = bucket;
            bucket_task.files = files;
            let (files, changelog) =
                primary_key::execute(table, &bucket_task, &source, resources.clone(), None).await?;
            output.extend(files.into_iter().map(|file| (bucket, file)));
            changelog_output.extend(changelog.into_iter().map(|file| (bucket, file)));
        }
        Ok(())
    }
    .await;
    cleanup(table, task, &partition_path, &staged).await;
    if let Err(error) = result {
        cleanup(table, task, &partition_path, &output).await;
        cleanup(table, task, &partition_path, &changelog_output).await;
        return Err(error);
    }
    Ok(CompactResult {
        task: task.clone(),
        file_buckets: output.iter().map(|(bucket, _)| *bucket).collect(),
        files: output.into_iter().map(|(_, file)| file).collect(),
        changelog_buckets: changelog_output.iter().map(|(bucket, _)| *bucket).collect(),
        changelog_files: changelog_output.into_iter().map(|(_, file)| file).collect(),
        // Every captured history file is rewritten, so there are no retained
        // lookup targets. Commit still needs explicit DV metadata to validate
        // captured masks and atomically retire their index ranges.
        deletion_vector_updates: options.deletion_vectors_enabled().then(Vec::new),
    })
}
