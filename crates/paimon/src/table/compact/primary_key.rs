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

use super::CompactTask;
use crate::resource::ResourceContext;
use crate::spec::{
    BigIntType, DataField, DataFileMeta, DataType, MergeEngine, RowKind, TinyIntType,
    SEQUENCE_NUMBER_FIELD_ID, SEQUENCE_NUMBER_FIELD_NAME, VALUE_KIND_FIELD_ID,
    VALUE_KIND_FIELD_NAME,
};
use crate::table::data_file_index_writer::FileIndexOptions;
use crate::table::kv_file_reader::{KeyValueFileReader, KeyValueReadConfig};
use crate::table::kv_file_writer::{KeyValueFileWriter, KeyValueWriteConfig};
use crate::table::sort_merge::{
    compare_sequence_order, BufferedBatch, MergeFunction, MergeResult, MergeRow,
};
use crate::table::table_read::{
    configured_mosaic_prefetch, configured_parquet_read_budget, MAX_MERGE_INPUT_STREAMS,
};
use crate::table::{DataSplit, Table};
use crate::Result;

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
        Ok(match self.inner.merge(rows, batches, columns, schema)? {
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
    Ok(if config.merge_engine == MergeEngine::FirstRow {
        // First-row must still reject retracts unless ignore-delete is enabled.
        inner
    } else {
        Box::new(CompactReducer { inner })
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
        .collect();
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
    let mut writer = KeyValueFileWriter::new(
        table.file_io().clone(),
        KeyValueWriteConfig {
            table_name: table.identifier().full_name(),
            table_options: schema.options().clone(),
            table_location: table.location().to_string(),
            partition_path,
            bucket: task.bucket,
            schema_id: schema.id(),
            file_compression: options.file_compression().to_string(),
            file_compression_zstd_level: options.file_compression_zstd_level(),
            // Bound staging independently of bucket size. Output remains key ordered
            // across flushes, and each key group is emitted only once by the merge.
            write_buffer_size: (options.write_buffer_size()? as i64)
                .min(options.target_file_size()),
            file_format: options.file_format(),
            data_file_prefix: options.data_file_prefix().to_string(),
            input_changelog: false,
            changelog_file_prefix: options.changelog_file_prefix().to_string(),
            changelog_file_compression: options.changelog_file_compression().to_string(),
            changelog_file_format: options.changelog_file_format(),
            primary_keys: schema.primary_keys().to_vec(),
            primary_key_indices,
            primary_key_types,
            value_fields: schema.fields().to_vec(),
            sequence_field_indices: vec![], // Input is already merged; sequence fields are ordinary values.
            merge_engine,
            file_index_options: index.map(Arc::new),
        },
        0,
    )?
    .with_resources(resources);
    let level = task.output_level();
    let result = async {
        while let Some(batch) = batches.try_next().await? {
            writer.write_compact(&batch, level).await?;
        }
        writer.prepare_compact(level).await
    }
    .await;
    if result.is_err() {
        writer.abort().await;
    }
    result
}
