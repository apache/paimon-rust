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

use super::extraction::data_split_for_shard;
use super::pipeline::GranuleBuildOutcome;
use super::planning::VindexIndexShard;
use super::timing::{vector_index_build_timing_enabled, VectorIndexBuildTiming};
use super::validation::checked_row_count;
use super::VindexIndexBuildBuilder;
use crate::spec::{GlobalIndexMeta, IndexFileMeta, ROW_ID_FIELD_NAME};
use crate::table::data_file_reader::DataFileReadTiming;
use crate::table::table_read::configured_parquet_read_budget;
use crate::vindex::{VindexVectorIndexOptions, DISKANN_IDENTIFIER};
use crate::{Error, Result};
use futures::TryStreamExt;
use paimon_vindex_core::index::VectorIndexWriter;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::table::index_build::{
    vector::{serialize_writer, TrainedVectors, VectorSpill},
    IndexFileWriter, IndexPayload,
};
use crate::table::index_file_path::IndexFileLocation;
use crate::table::RowRange;

pub(super) struct BuiltIndexFile {
    pub(super) meta: IndexFileMeta,
    pub(super) timing: Option<VectorIndexBuildTiming>,
}

impl<'a> VindexIndexBuildBuilder<'a> {
    pub(super) async fn build_index_file(
        &self,
        shard: &VindexIndexShard,
        index_column: &str,
        dimension: i32,
        index_field_id: i32,
        options: &VindexVectorIndexOptions,
        index_meta: Vec<u8>,
    ) -> Result<Option<BuiltIndexFile>> {
        let rows = usize::try_from(checked_row_count(
            shard.row_range_start,
            shard.row_range_end,
        )?)
        .map_err(|error| Error::DataInvalid {
            message: "vindex row count does not fit usize".into(),
            source: Some(Box::new(error)),
        })?;
        let use_granule = self.index_type != DISKANN_IDENTIFIER
            && options.granule_build_enabled
            && !options.needs_vector_count()
            // Invalid training options still produce no file for an all-NULL
            // shard, like Java. Full spill determines whether training is needed.
            && options.training_config(rows).is_ok();
        // Auto IVF sizing needs the complete non-null cardinality, like Java.
        // Explicit nlist/expected-count builds retain the granule fast path.
        log::info!(
            "vindex build strategy: index_type={}, strategy={}",
            self.index_type,
            if use_granule { "granule" } else { "full-spill" }
        );
        if use_granule {
            match self
                .build_index_file_granule(
                    shard,
                    index_column,
                    dimension,
                    index_field_id,
                    options,
                    index_meta.clone(),
                )
                .await?
            {
                GranuleBuildOutcome::Built(built) => return Ok(Some(*built)),
                GranuleBuildOutcome::Sparse => {
                    // Only private in-memory/spilled preparation has happened. The
                    // pinned snapshot is reread with sparse row IDs and valid-only training.
                    log::info!("vindex granule source contains NULL vectors; use sparse full-spill preparation");
                }
            }
        }
        self.build_full_spill_index_file(
            shard,
            index_column,
            dimension,
            index_field_id,
            options,
            index_meta,
        )
        .await
    }

    async fn build_full_spill_index_file(
        &self,
        shard: &VindexIndexShard,
        index_column: &str,
        dimension: i32,
        index_field_id: i32,
        options: &VindexVectorIndexOptions,
        index_meta: Vec<u8>,
    ) -> Result<Option<BuiltIndexFile>> {
        let timing_enabled = vector_index_build_timing_enabled();
        let total_start = timing_enabled.then(Instant::now);
        let mut source_batch_wait = Duration::ZERO;
        let mut raw_temp_write = Duration::ZERO;
        let read_timing = timing_enabled.then(|| Arc::new(DataFileReadTiming::default()));
        let parquet_read_budget = if timing_enabled {
            let budget = configured_parquet_read_budget(self.table)?;
            budget.enable_diagnostics();
            Some(budget)
        } else {
            None
        };
        let mut batch_count = 0usize;
        let row_count = checked_row_count(shard.row_range_start, shard.row_range_end)?;
        let row_count_usize = usize::try_from(row_count).map_err(|e| Error::DataInvalid {
            message: format!("Invalid vindex row count: {row_count}"),
            source: Some(Box::new(e)),
        })?;
        let dimension_usize = usize::try_from(dimension).map_err(|e| Error::DataInvalid {
            message: format!("Invalid vindex dimension: {dimension}"),
            source: Some(Box::new(e)),
        })?;
        let mut spill = VectorSpill::new(
            RowRange::new(shard.row_range_start, shard.row_range_end),
            dimension_usize,
        )?;
        let split = data_split_for_shard(shard)?;
        let mut read_builder = self.table.new_read_builder();
        read_builder.with_projection(&[index_column, ROW_ID_FIELD_NAME])?;
        let read = read_builder.new_read()?;
        let read = match read_timing.as_ref() {
            Some(timing) => read.with_data_file_read_timing(Arc::clone(timing)),
            None => read,
        };
        let read = match parquet_read_budget.as_ref() {
            Some(budget) => read.with_parquet_read_budget(Arc::clone(budget)),
            None => read,
        };
        let mut batches = read.to_arrow(&[split])?;
        loop {
            let source_start = timing_enabled.then(Instant::now);
            let batch = batches.try_next().await?;
            if let Some(start) = source_start {
                source_batch_wait = source_batch_wait.saturating_add(start.elapsed());
            }
            let Some(batch) = batch else { break };
            batch_count += 1;
            raw_temp_write = raw_temp_write.saturating_add(
                spill
                    .push_batch(&batch, index_column, timing_enabled)
                    .await?,
            );
        }
        let bytes_written = spill.bytes_written();
        let Some(spill) = spill.finish().await? else {
            return Ok(None);
        };
        let TrainedVectors {
            writer,
            train_finish,
            raw_temp_reread,
            index_add,
            training_vector_count,
            training_rows_retained,
        } = spill.train(options, timing_enabled).await?;

        let serialize_upload_start = timing_enabled.then(Instant::now);
        let meta = self
            .finish_index_file(writer, shard, index_field_id, index_meta, row_count)
            .await?;
        let serialize_upload =
            serialize_upload_start.map_or(Duration::ZERO, |start| start.elapsed());
        let (oss_read, parquet_decode) = read_timing
            .as_ref()
            .map_or((Duration::ZERO, Duration::ZERO), |timing| {
                (timing.file_read(), timing.parquet_decode())
            });
        let (file_schema_open, first_batch_wait, remaining_batch_wait) = read_timing
            .as_ref()
            .map_or((Duration::ZERO, Duration::ZERO, Duration::ZERO), |timing| {
                timing.file_waits()
            });
        let parquet_diagnostics = parquet_read_budget
            .as_ref()
            .map_or_else(Default::default, |budget| budget.diagnostics());
        let timing = total_start.map(|start| VectorIndexBuildTiming {
            total_without_commit: start.elapsed(),
            source_batch_wait,
            oss_read,
            parquet_decode,
            file_schema_open,
            first_batch_wait,
            remaining_batch_wait,
            parquet_row_group_count: parquet_diagnostics.row_group_count,
            parquet_projected_bytes_min: parquet_diagnostics.projected_bytes_min,
            parquet_projected_bytes_max: parquet_diagnostics.projected_bytes_max,
            parquet_projected_bytes_total: parquet_diagnostics.projected_bytes_total,
            parquet_peak_inflight_row_groups: parquet_diagnostics.peak_inflight,
            raw_temp_write,
            granule_spill_write: Duration::ZERO,
            train_finish,
            raw_temp_reread,
            granule_spill_read: Duration::ZERO,
            index_add,
            serialize_upload,
            rows: row_count_usize,
            training_rows_seen: training_vector_count,
            training_rows_retained,
            batch_count,
            raw_temp_bytes: bytes_written,
            granule_spill_bytes: 0,
            index_bytes: meta.file_size as u64,
            data_file_count: shard.files.len(),
            file_name: meta.file_name.clone(),
        });
        Ok(Some(BuiltIndexFile { meta, timing }))
    }

    pub(super) async fn finish_index_file(
        &self,
        writer: VectorIndexWriter,
        shard: &VindexIndexShard,
        index_field_id: i32,
        index_meta: Vec<u8>,
        row_count: i64,
    ) -> Result<IndexFileMeta> {
        let file_name = format!(
            "vector-{}-global-index-{}.index",
            self.index_type,
            uuid::Uuid::new_v4()
        );
        let meta = GlobalIndexMeta {
            row_range_start: shard.row_range_start,
            row_range_end: shard.row_range_end,
            index_field_id,
            extra_field_ids: None,
            source_meta: Some(
                crate::spec::DataEvolutionIndexSourceMeta::new(shard.snapshot_id)?.serialize(),
            ),
            index_meta: None,
        };
        IndexFileWriter::new(
            self.table,
            &self.index_type,
            file_name,
            IndexFileLocation::Global {
                table_path: self.table.location().trim_end_matches('/'),
            },
            "",
            meta,
        )
        .await?
        .write(self.table, |output| async move {
            serialize_writer(writer, output).await?;
            Ok(Some(IndexPayload {
                row_count,
                index_meta,
            }))
        })
        .await
        .map(|file| file.expect("trained vector payload"))
    }
}
