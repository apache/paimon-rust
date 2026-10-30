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

mod api;
pub use api::{
    PostponeCompactBucketResult, PostponeCompactBuilder, PostponeCompactExecutor,
    PostponeCompactInput, PostponeCompactPlan, PostponeCompactPlanner, PostponeCompactResult,
    PostponeCompactTask,
};

use super::{invalid, primary_key, validate_file, CompactContext, CompactRewrite, PreparedCompact};
use crate::resource::{MemoryReservation, ResourceContext};
use crate::spec::{
    bucket_path_under, BinaryRow, ChangelogProducer, DataFileMeta, FileKind, ManifestEntry,
    PartitionComputer, TableSchema, POSTPONE_BUCKET,
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

pub(super) fn has_row_output(schema: &TableSchema) -> Result<bool> {
    let options = schema.core_options();
    Ok(options.file_format().eq_ignore_ascii_case("row")
        || (options.try_changelog_producer()? == ChangelogProducer::Lookup
            && options.changelog_file_format().eq_ignore_ascii_case("row")))
}

/// One shared retained-input budget across all buckets. Decoder batch boundaries
/// must not turn into a remote object for every bucket and batch.
struct PendingSortBuffer {
    buckets: BTreeMap<i32, (Vec<RecordBatch>, usize)>,
    bytes: usize,
    limit: usize,
    reservation: Option<MemoryReservation>,
}

impl PendingSortBuffer {
    fn new(limit: usize, resources: Option<&ResourceContext>) -> Self {
        Self {
            buckets: BTreeMap::new(),
            bytes: 0,
            limit,
            reservation: resources.map(ResourceContext::reservation),
        }
    }

    fn must_flush_before(&self, batch: &RecordBatch) -> bool {
        self.bytes != 0 && self.bytes.saturating_add(batch.get_array_memory_size()) > self.limit
    }

    fn push(&mut self, bucket: i32, batch: &RecordBatch) -> Result<()> {
        let size = batch.get_array_memory_size();
        let total = self
            .bytes
            .checked_add(size)
            .ok_or_else(|| invalid("Postpone sort buffer size overflow"))?;
        if let Some(reservation) = &mut self.reservation {
            reservation.try_grow(size)?;
        }
        let (batches, bytes) = self.buckets.entry(bucket).or_default();
        batches.push(batch.clone());
        *bytes += size;
        self.bytes = total;
        Ok(())
    }

    async fn flush(
        &mut self,
        table: &Table,
        partition_path: &str,
        resources: Option<&ResourceContext>,
        staged: &mut Vec<(i32, DataFileMeta)>,
        runs: &mut BTreeMap<i32, Vec<DataFileMeta>>,
    ) -> Result<()> {
        while let Some((bucket, (mut batches, bytes))) = self.buckets.pop_first() {
            let batch = if batches.len() == 1 {
                batches.pop().unwrap()
            } else {
                arrow_select::concat::concat_batches(&batches[0].schema(), &batches)
                    .map_err(|error| invalid(&error.to_string()))?
            };
            drop(batches);
            self.bytes -= bytes;
            if let Some(reservation) = &mut self.reservation {
                reservation.try_resize(self.bytes)?;
            }
            let files = stage(table, partition_path, bucket, &batch, resources).await?;
            staged.extend(files.iter().cloned().map(|file| (bucket, file)));
            runs.entry(bucket).or_default().extend(files);
        }
        Ok(())
    }
}

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
) -> Result<Vec<PostponeCompactTask>> {
    let mut partitions = BTreeMap::<_, Vec<_>>::new();
    for entry in entries {
        partitions
            .entry(entry.partition().to_vec())
            .or_default()
            .push(entry);
    }
    let mut tasks = Vec::new();
    for (partition, entries) in partitions {
        if !entries.iter().any(|entry| entry.file().level == 0) {
            continue;
        }
        let total_buckets = layout(&entries, &context.schema)?;
        tasks.push(PostponeCompactTask {
            context: context.clone(),
            task_id: tasks.len(),
            partition,
            total_buckets,
            output_level: entries
                .iter()
                .map(|entry| entry.file().level)
                .max()
                .unwrap_or(0)
                .max(1),
            inputs: entries
                .iter()
                .map(|entry| PostponeCompactInput {
                    bucket: entry.bucket(),
                    total_buckets: entry.total_buckets(),
                    file: entry.file().clone(),
                })
                .collect(),
        });
    }
    Ok(tasks)
}

pub(super) fn validate_task(task: &PostponeCompactTask) -> Result<()> {
    if task.total_buckets <= 0
        || task.inputs.is_empty()
        || task.output_level <= 0
        || task.inputs.iter().any(|input| {
            if input.bucket == POSTPONE_BUCKET {
                input.total_buckets != POSTPONE_BUCKET || input.file.level != 0
            } else {
                !(0..task.total_buckets).contains(&input.bucket)
                    || input.total_buckets != task.total_buckets
            }
        })
    {
        return Err(invalid("Invalid postpone compaction task"));
    }
    Ok(())
}

pub(super) fn validate_inputs(task: &PostponeCompactTask, entries: &[ManifestEntry]) -> Result<()> {
    let entries = entries
        .iter()
        .filter(|entry| entry.partition() == task.partition)
        .cloned()
        .collect::<Vec<_>>();
    let expected = plan_tasks(&entries, &task.context)?
        .into_iter()
        .find(|candidate| candidate.partition == task.partition)
        .ok_or_else(|| invalid("No postpone compaction is eligible"))?;
    if expected.inputs != task.inputs
        || expected.total_buckets != task.total_buckets
        || expected.output_level != task.output_level
    {
        return Err(invalid(
            "Postpone compaction must include the complete captured partition",
        ));
    }
    Ok(())
}

fn validate_result(result: &PostponeCompactResult) -> Result<()> {
    let mut buckets = HashSet::new();
    if result.buckets.iter().any(|bucket| {
        !(0..result.task.total_buckets).contains(&bucket.bucket) || !buckets.insert(bucket.bucket)
    }) {
        return Err(invalid("Invalid postpone compaction output buckets"));
    }
    Ok(())
}

pub(in crate::table) fn ensure_supported(table: &Table) -> Result<()> {
    api::ensure_postpone_supported(table)
}

pub(in crate::table) fn prepare_results(
    table: &Table,
    results: &[PostponeCompactResult],
) -> Result<PreparedCompact> {
    ensure_supported(table)?;
    let rewrites = results
        .iter()
        .map(|result| {
            let task = &result.task;
            task.validate_target(table)?;
            validate_result(result)?;
            Ok(CompactRewrite {
                context: &task.context,
                task_id: task.task_id,
                partition: &task.partition,
                inputs: task
                    .inputs
                    .iter()
                    .map(|input| (input.bucket, input.total_buckets, &input.file))
                    .collect(),
                files: result
                    .buckets
                    .iter()
                    .flat_map(|bucket| {
                        bucket
                            .files
                            .iter()
                            .map(move |file| (bucket.bucket, task.total_buckets, file))
                    })
                    .collect(),
                changelog_files: result
                    .buckets
                    .iter()
                    .flat_map(|bucket| {
                        bucket
                            .changelog_files
                            .iter()
                            .map(move |file| (bucket.bucket, task.total_buckets, file))
                    })
                    .collect(),
                output_level: Some(task.output_level),
                data_evolution_task: None,
                deletion_vector_updates: result.deletion_vector_updates.as_ref(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    super::prepare_rewrites(table, &rewrites)
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
    task: &PostponeCompactTask,
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
    task: &PostponeCompactTask,
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
    task: &PostponeCompactTask,
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
    task: &PostponeCompactTask,
    resources: Option<ResourceContext>,
) -> Result<PostponeCompactResult> {
    let schema = table.schema();
    let options = schema.core_options();
    let mut row_schemas = HashMap::new();
    if has_row_output(schema)? && row_thin_mode(table, schema.id(), &mut row_schemas).await? {
        return Err(crate::Error::Unsupported { message: "Postpone compaction cannot write thin-mode ROW files; change the persisted output schema".into() });
    }
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
    for input in &task.inputs {
        let file = &input.file;
        let bucket = input.bucket;
        if bucket == POSTPONE_BUCKET {
            let (writer, _) = file
                .file_name
                .rsplit_once("-w-")
                .ok_or_else(|| invalid("Invalid postpone writer file name"))?;
            pending.push((writer, file));
        } else {
            if file.file_name.ends_with(".row")
                && row_thin_mode(table, file.schema_id, &mut row_schemas).await?
            {
                return Err(crate::Error::Unsupported {
                    message:
                        "Postpone compaction of real-bucket thin-mode ROW files is not supported"
                            .into(),
                });
            }
            next_sequence = next_sequence.max(
                file.max_sequence_number
                    .checked_add(1)
                    .ok_or_else(|| invalid("Postpone sequence number overflow"))?,
            );
            buckets.entry(bucket).or_default().push(file.clone());
        }
    }
    // Java sequences can be UNKNOWN_SEQUENCE in every pending file. Stable
    // creation-time sorting preserves the captured scan order for ties.
    pending.sort_by_key(|(writer, file)| (*writer, file.creation_time));
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
    let mut sort_buffer = PendingSortBuffer::new(options.write_buffer_size()?, resources.as_ref());
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
            // ROW has no embedded schema, and NULLs can make both layouts have
            // the same byte width. Java KV writers always retain one key-stat
            // slot per trimmed key, even when statistics are disabled. Legacy
            // Rust postpone writers have no key-stat slots. Use that metadata
            // contract, plus the writing schema's thin-mode setting, rather than
            // guessing the layout from a successful decode.
            let kv_row =
                if file.file_name.ends_with(".row") && !file.key_stats.null_counts().is_empty() {
                    !row_thin_mode(table, file.schema_id, &mut row_schemas).await?
                } else {
                    false
                };
            let mut batches = if kv_row {
                primary_key::new_reader(table, resources.clone())?
                    .read_physical_file(source, file.clone())?
            } else {
                reader.clone().read(std::slice::from_ref(&source))?
            };
            let mut rows = 0i64;
            while let Some(batch) = batches.try_next().await? {
                let start = rows as u64;
                rows += batch.num_rows() as i64;
                // Decode and count physical rows before filtering. This also
                // covers the Java ROW reader, whose physical reader deliberately
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
                    if sort_buffer.must_flush_before(&batch) {
                        sort_buffer
                            .flush(
                                table,
                                &partition_path,
                                resources.as_ref(),
                                &mut staged,
                                &mut pending_runs,
                            )
                            .await?;
                    }
                    if let Err(error) = sort_buffer.push(bucket, &batch) {
                        if sort_buffer.bytes == 0
                            || !matches!(error, crate::Error::ResourceExhausted { .. })
                        {
                            return Err(error);
                        }
                        sort_buffer
                            .flush(
                                table,
                                &partition_path,
                                resources.as_ref(),
                                &mut staged,
                                &mut pending_runs,
                            )
                            .await?;
                        sort_buffer.push(bucket, &batch)?;
                    }
                    if sort_buffer.bytes >= sort_buffer.limit {
                        sort_buffer
                            .flush(
                                table,
                                &partition_path,
                                resources.as_ref(),
                                &mut staged,
                                &mut pending_runs,
                            )
                            .await?;
                    }
                }
            }
            if rows != file.row_count {
                return Err(invalid("Postpone input row count changed"));
            }
        }
        sort_buffer
            .flush(
                table,
                &partition_path,
                resources.as_ref(),
                &mut staged,
                &mut pending_runs,
            )
            .await?;
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
            let (files, changelog) =
                primary_key::execute(table, &source, task.output_level, resources.clone(), None)
                    .await?;
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
    let mut results = BTreeMap::<i32, PostponeCompactBucketResult>::new();
    for (bucket, file) in output {
        results
            .entry(bucket)
            .or_insert_with(|| PostponeCompactBucketResult {
                bucket,
                files: vec![],
                changelog_files: vec![],
            })
            .files
            .push(file);
    }
    for (bucket, file) in changelog_output {
        results
            .entry(bucket)
            .or_insert_with(|| PostponeCompactBucketResult {
                bucket,
                files: vec![],
                changelog_files: vec![],
            })
            .changelog_files
            .push(file);
    }
    Ok(PostponeCompactResult {
        task: task.clone(),
        buckets: results.into_values().collect(),
        // Every captured history file is rewritten, so there are no retained
        // lookup targets. Commit still needs explicit DV metadata to validate
        // captured masks and atomically retire their index ranges.
        deletion_vector_updates: options.deletion_vectors_enabled().then(Vec::new),
    })
}

async fn row_thin_mode(
    table: &Table,
    schema_id: i64,
    cache: &mut HashMap<i64, bool>,
) -> Result<bool> {
    if let Some(&thin) = cache.get(&schema_id) {
        return Ok(thin);
    }
    let schema = table.schema_manager().schema(schema_id).await?;
    let thin = schema
        .options()
        .get("data-file.thin-mode")
        .is_some_and(|value| value.eq_ignore_ascii_case("true"));
    cache.insert(schema_id, thin);
    Ok(thin)
}
