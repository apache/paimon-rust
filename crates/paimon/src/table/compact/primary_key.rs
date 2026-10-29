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

use std::sync::Arc;

use arrow_schema::SchemaRef;
use futures::TryStreamExt;

use super::{split_files_by_bucket, validate_file, CompactContext, CompactTask};
use crate::resource::ResourceContext;
use crate::spec::{
    BigIntType, DataField, DataFileMeta, DataType, MergeEngine, RowKind, TinyIntType,
    SEQUENCE_NUMBER_FIELD_ID, SEQUENCE_NUMBER_FIELD_NAME, VALUE_KIND_FIELD_ID,
    VALUE_KIND_FIELD_NAME,
};
use crate::table::data_file_index_writer::FileIndexOptions;
use crate::table::data_file_path_factory::DataFilePathFactory;
use crate::table::kv_data_file_writer::{KeyValueFileOptions, KeyValueRollingWriter};
use crate::table::kv_file_reader::{KeyValueFileReader, KeyValueReadConfig};
use crate::table::sort_merge::{
    compare_sequence_order, BufferedBatch, MergeFunction, MergeResult, MergeRow,
};
use crate::table::table_read::{
    configured_mosaic_prefetch, configured_parquet_read_budget, MAX_MERGE_INPUT_STREAMS,
};
use crate::table::{DataSplit, Plan, Table};
use crate::Result;

/// Select complete captured buckets that need an L0 upgrade or level merge.
pub(super) struct PrimaryKeyCompactPlanner;

impl PrimaryKeyCompactPlanner {
    pub(super) fn plan_tasks(
        &self,
        source: &Plan,
        context: &CompactContext,
    ) -> Result<Vec<CompactTask>> {
        let mut tasks = Vec::new();
        for ((partition, bucket, total_buckets), files) in split_files_by_bucket(source) {
            for file in &files {
                validate_file(file, true)?;
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
                });
            }
        }
        Ok(tasks)
    }
}

/// Keep the same winner as a deduplicate query, including DELETE/UPDATE_BEFORE.
/// A later write may carry a lower user sequence, so even full compaction
/// must retain a winning retract rather than resurrect an older value.
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

/// The query reader omits singleton retracts and materialized DELETE rows.
/// Compaction persists these states so later inserts/retractions merge exactly
/// as they would against the original files. Singletons remain unreduced, as
/// in Java's reducer wrapper; compound records use the existing engine reducer.
struct CompactReducer {
    inner: Box<dyn MergeFunction>,
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
        if let [row] = rows {
            RowKind::from_value(row.value_kind)?;
            return Ok(MergeResult::SourceRow {
                batch_idx: row.batch_idx,
                row_idx: row.row_idx,
            });
        }
        if self.reject_retract_group && rows.iter().any(|row| matches!(row.value_kind, 1 | 3)) {
            // Some aggregators preserve flags or raw DELETE payloads that a
            // single physical INSERT/DELETE cannot encode, even if the final
            // merged row is an INSERT. Leave this group in its source files.
            return Err(crate::Error::Unsupported {
                message: "Compaction cannot preserve aggregate state for this retract group".into(),
            });
        }
        Ok(match self.inner.merge(rows, batches, columns, schema)? {
            MergeResult::MaterializedDeleteRow(_) if self.reject_delete_state => {
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

fn new_merge_function(
    config: &KeyValueReadConfig,
    fields: &[DataField],
) -> Result<Box<dyn MergeFunction>> {
    if config.merge_engine == MergeEngine::Deduplicate {
        return Ok(Box::new(CompactDeduplicate));
    }
    let inner = KeyValueFileReader::new_merge_function(config, fields)?;
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
        inner
    } else {
        Box::new(CompactReducer {
            inner,
            reject_delete_state: config.merge_engine == MergeEngine::PartialUpdate
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

pub(super) async fn execute(
    table: &Table,
    task: &CompactTask,
    split: &DataSplit,
    resources: Option<ResourceContext>,
) -> Result<Vec<DataFileMeta>> {
    let schema = table.schema();
    let options = schema.core_options();
    let merge_engine = options.merge_engine()?;
    let mut read_type = vec![
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
    read_type.extend_from_slice(schema.fields());
    let budget = configured_parquet_read_budget(table)?;
    let budget = match &resources {
        Some(resources) => Arc::new(budget.with_resources(resources.clone()).without_prefetch()),
        None => Arc::new(budget.without_prefetch()),
    };
    let reader = KeyValueFileReader::new(
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
    );
    let mut batches =
        reader.read_with_merge_function(std::slice::from_ref(split), new_merge_function)?;
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
    let mut writer = KeyValueRollingWriter::new(
        KeyValueFileOptions {
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
        },
        options.target_file_size(),
        options.target_file_row_num()?,
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
