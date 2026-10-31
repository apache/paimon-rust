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

pub(in crate::table) mod deletion_vectors;
pub(super) mod indexes;
pub(super) mod lookup;
pub(super) mod lookup_file;
pub(super) mod lookup_store;
pub(in crate::table) mod planner;

use std::sync::Arc;

use arrow_array::{BooleanArray, Int8Array};
use arrow_schema::SchemaRef;
use futures::TryStreamExt;

use super::{split_files_by_bucket, validate_file, CompactContext, CompactTask};
use crate::resource::ResourceContext;
use crate::spec::{
    BigIntType, ChangelogProducer, DataField, DataFileMeta, DataType, MergeEngine, RowKind,
    TinyIntType, SEQUENCE_NUMBER_FIELD_ID, SEQUENCE_NUMBER_FIELD_NAME, VALUE_KIND_FIELD_ID,
    VALUE_KIND_FIELD_NAME,
};
use crate::table::data_file_index_writer::FileIndexOptions;
use crate::table::data_file_path_factory::DataFilePathFactory;
use crate::table::kv_data_file_writer::{KeyValueFileOptions, KeyValueRollingWriter};
use crate::table::kv_file_reader::{KeyValueFileReader, KeyValueReadConfig};
use crate::table::sort_merge::{
    compare_sequence_order, BufferedBatch, LookupProvider, MergeFunction, MergeResult, MergeRow,
    CHANGELOG_BATCH_METADATA_KEY,
};
use crate::table::table_read::{
    configured_mosaic_prefetch, configured_parquet_read_budget, MAX_MERGE_INPUT_STREAMS,
};
use crate::table::{DataSplit, Plan, Table};
use crate::Result;

/// Select incremental sorted runs for supported merge modes.
pub(super) struct PrimaryKeyCompactPlanner {
    pub(super) automatic: bool,
}

impl PrimaryKeyCompactPlanner {
    pub(super) fn plan_tasks(
        &self,
        source: &Plan,
        context: &CompactContext,
    ) -> Result<Vec<CompactTask>> {
        let mut tasks = Vec::new();
        let options = context.schema.core_options();
        let force_level_zero = !self.automatic
            || options.compaction_force_up_level0()?
            || options.try_changelog_producer()? == ChangelogProducer::Lookup
            || options.merge_engine()? == MergeEngine::FirstRow
            || options.deletion_vectors_enabled();
        for ((partition, bucket, total_buckets), files) in split_files_by_bucket(source) {
            for file in &files {
                validate_file(file, true)?;
            }
            if planner::enabled(&context.schema)? {
                if let Some((files, output_level)) = planner::select_with_force_level_zero(
                    files,
                    context.schema.options(),
                    force_level_zero,
                )? {
                    tasks.push(CompactTask {
                        context: context.clone(),
                        task_id: tasks.len(),
                        partition,
                        bucket,
                        total_buckets,
                        files,
                        output_level: Some(output_level),
                    });
                }
                continue;
            }
            // Some stateful reducers only support a complete-bucket rewrite.
            // Automatic writes still wait for a normal Java trigger rather
            // than turning every small batch into an explicit full rewrite.
            if self.automatic
                && planner::select_with_force_level_zero(
                    files.clone(),
                    context.schema.options(),
                    force_level_zero,
                )?
                .is_none()
            {
                continue;
            }
            // One positive level is already a non-overlapping sorted run,
            // including when the previous compaction rolled several files.
            let first_level = files.first().map(|file| file.level);
            if files
                .iter()
                .any(|file| file.level == 0 || Some(file.level) != first_level)
            {
                tasks.push(CompactTask {
                    context: context.clone(),
                    task_id: tasks.len(),
                    partition,
                    bucket,
                    total_buckets,
                    files,
                    output_level: None,
                });
            }
        }
        Ok(tasks)
    }
}

/// Keep the same winner as a deduplicate query, including DELETE/UPDATE_BEFORE.
/// A later write may carry a lower user sequence, so compaction must retain
/// a winning retract rather than resurrect an older value.
struct CompactDeduplicate;

impl MergeFunction for CompactDeduplicate {
    fn merge(
        &self,
        rows: &[MergeRow],
        _batches: &[BufferedBatch],
        _columns: &[usize],
        _schema: &SchemaRef,
    ) -> Result<MergeResult> {
        let row = rows
            .iter()
            .max_by(|a, b| compare_sequence_order(a, b))
            .expect("merge called with empty key group");
        RowKind::from_value(row.value_kind)?;
        Ok(MergeResult::SourceRow {
            batch_idx: row.batch_idx,
            row_idx: row.row_idx,
        })
    }
}

/// Ordinary compaction preserves singleton states as Java's reducer wrapper
/// does. Lookup compaction always runs the reducer, including for one retract.
/// Persist synthesized DELETEs when their state can safely be replayed.
struct CompactReducer {
    inner: Box<dyn MergeFunction>,
    reduce_singletons: bool,
    reject_delete_state: bool,
    reject_retract_group: bool,
}

impl MergeFunction for CompactReducer {
    fn merge(
        &self,
        rows: &[MergeRow],
        batches: &[BufferedBatch],
        columns: &[usize],
        schema: &SchemaRef,
    ) -> Result<MergeResult> {
        if !self.reduce_singletons {
            if let [row] = rows {
                RowKind::from_value(row.value_kind)?;
                return Ok(MergeResult::SourceRow {
                    batch_idx: row.batch_idx,
                    row_idx: row.row_idx,
                });
            }
        }
        if rows.len() > 1
            && self.reject_retract_group
            && rows.iter().any(|row| matches!(row.value_kind, 1 | 3))
        {
            // Some aggregators preserve flags or raw DELETE payloads that a
            // single physical INSERT/DELETE cannot encode, even if the final
            // merged row is an INSERT. Leave this group in its source files.
            return Err(crate::Error::Unsupported {
                message: "Compaction cannot preserve aggregate state for this retract group".into(),
            });
        }
        Ok(match self.inner.merge(rows, batches, columns, schema)? {
            MergeResult::MaterializedDeleteRow(_) if self.reject_delete_state && rows.len() > 1 => {
                // A partial-update reducer initializes from the first retract
                // and then retracts its payload. Replaying an accumulated
                // DELETE can cancel its own state. First-value aggregators
                // also carry an initialized flag that DELETE payloads do not
                // serialize. Keep the sources until replay is safe.
                return Err(crate::Error::Unsupported {
                    message:
                        "Compaction cannot preserve DELETE aggregate state for this merge engine"
                            .into(),
                });
            }
            MergeResult::MaterializedDeleteRow(batch) => MergeResult::MaterializedRow(batch),
            result => result,
        })
    }
}

struct CompactFirstRow {
    inner: Box<dyn MergeFunction>,
    output_level: i32,
}

impl MergeFunction for CompactFirstRow {
    fn merge(
        &self,
        rows: &[MergeRow],
        batches: &[BufferedBatch],
        columns: &[usize],
        schema: &SchemaRef,
    ) -> Result<MergeResult> {
        // Validate all selected records before checking retained history, as
        // Java FirstRowMergeFunctionWrapper does when feeding its reducer.
        let result = self.inner.merge(rows, batches, columns, schema)?;
        for row in rows {
            // Only a lookup hit can be above the output level. Its accepted
            // record stays in retained history; selected positive rows must
            // still be rewritten into this compaction's output.
            if matches!(row.value_kind, 0 | 2) && row.level(batches)? > self.output_level {
                return Ok(MergeResult::Omit);
            }
        }
        Ok(result)
    }
}

fn new_merge_function(
    config: &KeyValueReadConfig,
    fields: &[DataField],
    output_level: i32,
) -> Result<Box<dyn MergeFunction>> {
    if config.merge_engine == MergeEngine::Deduplicate {
        return Ok(Box::new(CompactDeduplicate));
    }
    let inner = KeyValueFileReader::new_unwrapped_merge_function(config, fields)?;
    let aggregate_functions = match config.merge_engine {
        MergeEngine::PartialUpdate => crate::spec::PartialUpdateConfig::new(&config.table_options)
            .validated_aggregate_functions(&config.table_fields, &config.primary_keys)?
            .into_values()
            .collect::<Vec<_>>(),
        MergeEngine::Aggregation => {
            let aggregation = crate::spec::AggregationConfig::new(&config.table_options);
            config
                .table_fields
                .iter()
                .filter(|field| {
                    !config
                        .table_primary_keys
                        .iter()
                        .chain(&config.sequence_fields)
                        .any(|name| name == field.name())
                })
                .map(|field| {
                    aggregation
                        .agg_function_for_field(field.name())
                        .or_else(|| aggregation.default_agg_function())
                        .unwrap_or("last_non_null_value")
                        .to_string()
                })
                .collect()
        }
        _ => vec![],
    };
    Ok(if config.merge_engine == MergeEngine::FirstRow {
        // First-row must still reject retracts unless ignore-delete is enabled.
        Box::new(CompactFirstRow {
            inner,
            output_level,
        })
    } else {
        Box::new(CompactReducer {
            inner,
            // LookupMergeFunction always invokes the underlying reducer, even
            // for one retract. The ordinary reducer wrapper keeps singletons.
            reduce_singletons: config
                .table_options
                .get("changelog-producer")
                .is_some_and(|value| value.eq_ignore_ascii_case("lookup")),
            reject_delete_state: config.merge_engine == MergeEngine::PartialUpdate
                && !crate::spec::CoreOptions::new(&config.table_options).ignore_delete()
                && !aggregate_functions.is_empty(),
            reject_retract_group: !(config.merge_engine == MergeEngine::PartialUpdate
                && crate::spec::CoreOptions::new(&config.table_options).ignore_delete())
                && aggregate_functions.iter().any(|function| {
                    matches!(
                        crate::spec::canonical_aggregator_name(function),
                        "first_value"
                            | "first_non_null_value"
                            | "collect"
                            | "nested_update"
                            | "nested_partial_update"
                            | "listagg"
                    )
                }),
        })
    })
}

fn read_type(table: &Table) -> Vec<DataField> {
    let mut fields = vec![
        DataField::new(
            SEQUENCE_NUMBER_FIELD_ID,
            SEQUENCE_NUMBER_FIELD_NAME.into(),
            DataType::BigInt(BigIntType::new()),
        ),
        DataField::new(
            VALUE_KIND_FIELD_ID,
            VALUE_KIND_FIELD_NAME.into(),
            DataType::TinyInt(TinyIntType::new()),
        ),
    ];
    fields.extend_from_slice(table.schema().fields());
    fields
}

fn new_reader(table: &Table, resources: Option<ResourceContext>) -> Result<KeyValueFileReader> {
    new_reader_with_projection(table, resources, read_type(table))
}

fn new_reader_with_projection(
    table: &Table,
    resources: Option<ResourceContext>,
    read_type: Vec<DataField>,
) -> Result<KeyValueFileReader> {
    let schema = table.schema();
    let options = schema.core_options();
    let merge_engine = options.merge_engine()?;
    let budget = configured_parquet_read_budget(table)?;
    let budget = match &resources {
        Some(resources) => Arc::new(budget.with_resources(resources.clone()).without_prefetch()),
        None => Arc::new(budget.without_prefetch()),
    };
    Ok(KeyValueFileReader::new(
        table.file_io().clone(),
        KeyValueReadConfig {
            table_name: table.identifier().full_name(),
            table_options: schema.options().clone(),
            schema_manager: table.schema_manager().clone(),
            table_schema_id: schema.id(),
            table_fields: schema.fields().to_vec(),
            read_type,
            predicates: vec![],
            primary_keys: schema.trimmed_primary_keys(),
            table_primary_keys: schema.primary_keys().to_vec(),
            merge_engine,
            sequence_fields: options
                .sequence_fields()
                .iter()
                .map(|s| s.to_string())
                .collect(),
            read_batch_size: options.read_batch_size()?,
            merge_splits: true,
            max_merge_input_streams: Some(MAX_MERGE_INPUT_STREAMS),
            parquet_read_budget: Some(budget),
            mosaic_prefetch: configured_mosaic_prefetch(table)?,
        },
    ))
}

pub(super) async fn execute(
    table: &Table,
    task: &CompactTask,
    split: &DataSplit,
    resources: Option<ResourceContext>,
    lookup: Option<Arc<dyn LookupProvider>>,
    emit_changelog: bool,
) -> Result<(Vec<DataFileMeta>, Vec<DataFileMeta>)> {
    let schema = table.schema();
    let options = schema.core_options();
    let lookup_changelog = options.try_changelog_producer()? == ChangelogProducer::Lookup;
    let output_level = task.output_level();
    let drop_delete = options.deletion_vectors_enabled() && output_level > 0;
    let reader = new_reader(table, resources.clone())?.with_lookup(lookup);
    let mut batches =
        reader.read_with_merge_function(std::slice::from_ref(split), move |config, fields| {
            if lookup_changelog {
                lookup::new_merge_function(config, fields, output_level)
            } else {
                new_merge_function(config, fields, output_level)
            }
        })?;
    let keys = schema.trimmed_primary_keys();
    let primary_key_indices = keys
        .iter()
        .map(|name| {
            schema
                .fields()
                .iter()
                .position(|field| field.name() == name)
                .unwrap()
        })
        .collect::<Vec<_>>();
    let primary_key_types = primary_key_indices
        .iter()
        .map(|&i| schema.fields()[i].data_type().clone())
        .collect::<Vec<_>>();
    let partition_path = if schema.partition_keys().is_empty() {
        String::new()
    } else {
        crate::spec::PartitionComputer::new(
            schema.partition_keys(),
            schema.fields(),
            options.partition_default_name(),
            options.legacy_partition_name(),
        )?
        .generate_partition_path(split.partition())?
    };
    let index = FileIndexOptions::parse(schema.options(), schema.fields())?;
    let paths = DataFilePathFactory::new(
        table.location(),
        &partition_path,
        task.bucket,
        schema.options(),
    )?;
    let file_format = options.file_format();
    let file_options = KeyValueFileOptions {
        file_io: table.file_io(),
        paths: &paths,
        table_options: schema.options(),
        value_fields: schema.fields(),
        primary_key_indices: &primary_key_indices,
        primary_key_types: &primary_key_types,
        schema_id: schema.id(),
        file_format: &file_format,
        file_prefix: options.data_file_prefix(),
        file_compression: options.file_compression(),
        file_compression_zstd_level: options.file_compression_zstd_level(),
        write_buffer_size: options.write_buffer_size()? as i64,
        level: task.output_level(),
        file_source: 1,
        is_changelog: false,
        index_options: index.as_ref(),
        managed_blob_enabled: false,
        resources: resources.as_ref(),
    };
    let mut row_limit = options.target_file_row_num()?;
    if options.deletion_vectors_enabled() {
        row_limit = row_limit.min(i64::from(u32::MAX) + 1);
    }
    let mut writer =
        KeyValueRollingWriter::new(file_options, options.target_file_size(), row_limit);
    let changelog_format = options.changelog_file_format();
    let mut changelog_writer = KeyValueRollingWriter::new(
        KeyValueFileOptions {
            file_format: &changelog_format,
            file_prefix: options.changelog_file_prefix(),
            file_compression: options.changelog_file_compression(),
            is_changelog: true,
            index_options: None,
            ..file_options
        },
        options.target_file_size(),
        options.target_file_row_num()?,
    );

    let result = async {
        while let Some(batch) = batches.try_next().await? {
            if batch
                .schema()
                .metadata()
                .contains_key(CHANGELOG_BATCH_METADATA_KEY)
            {
                if !emit_changelog {
                    continue;
                }
                let mut metadata = batch.schema().metadata().clone();
                metadata.remove(CHANGELOG_BATCH_METADATA_KEY);
                let batch = arrow_array::RecordBatch::try_new(
                    Arc::new(batch.schema().as_ref().clone().with_metadata(metadata)),
                    batch.columns().to_vec(),
                )
                .map_err(|error| {
                    super::invalid(&format!("Invalid changelog output batch: {error}"))
                })?;
                changelog_writer.write(&batch).await?;
            } else {
                // Generate lookup changelog from the winning KV first. In DV
                // mode the history is physically deleted, so its winning
                // retract need not remain in the new positive-level data.
                let batch = if drop_delete {
                    let kinds = batch
                        .column_by_name(VALUE_KIND_FIELD_NAME)
                        .and_then(|column| column.as_any().downcast_ref::<Int8Array>())
                        .ok_or_else(|| super::invalid("Invalid compaction value-kind column"))?;
                    let live = kinds
                        .iter()
                        .map(|kind| {
                            let kind =
                                kind.ok_or_else(|| super::invalid("Null compaction value kind"))?;
                            Ok(RowKind::from_value(kind)?.is_add())
                        })
                        .collect::<Result<Vec<_>>>()?;
                    arrow_select::filter::filter_record_batch(&batch, &BooleanArray::from(live))
                        .map_err(|error| super::invalid(&error.to_string()))?
                } else {
                    batch
                };
                writer.write(&batch).await?;
            }
        }
        // Keep both prepared file lists owned by their writers until both have
        // closed successfully, so either failure can clean up the whole task.
        writer.finish().await?;
        changelog_writer.finish().await?;
        Ok((
            writer.prepare_commit().await?,
            changelog_writer.prepare_commit().await?,
        ))
    }
    .await;
    if result.is_err() {
        writer.abort().await;
        changelog_writer.abort().await;
    }
    result
}
