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

use super::centroid_bitmap::{BitmapBuild, BitmapSpill, BitmapSpillReader};
use super::extraction::data_split_for_shard;
use super::validation::checked_training_vector_count;
use super::VindexIndexBuildBuilder;
use crate::spec::{
    DataEvolutionIndexSourceMeta, FileKind, GlobalIndexMeta, IndexFileMeta, IndexManifestEntry,
    ROW_ID_FIELD_NAME,
};
use crate::table::centroid_index::{self, Descriptor, Role, EMPTY_SHARD};
use crate::table::global_index_build_common::{vector::VectorIndexShard, GlobalIndexBuildView};
use crate::table::index_build::{
    vector::{serialize_writer, VectorSpill},
    IndexFileWriter, IndexPayload,
};
use crate::table::index_file_path::{new_global_index_file_name, IndexFileLocation};
use crate::table::partition_filter::PartitionFilter;
use crate::table::{CommitMessage, RowRange, Table};
use crate::vindex::VindexVectorIndexOptions;
use crate::{Error, Result};
use bytes::Bytes;
use futures::TryStreamExt;
use paimon_vindex_core::index::{CentroidRouter, VectorIndexConfig, VectorIndexWriter};
use std::collections::HashMap;

pub(in crate::table) struct CentroidBuildOptions {
    nlist: String,
    max_points_per_centroid: Option<String>,
    pub(super) bitmap: Option<BitmapBuild>,
}

impl CentroidBuildOptions {
    pub(in crate::table) fn resolve(
        table: &HashMap<String, String>,
        defaults: &HashMap<String, String>,
        user: &HashMap<String, String>,
        kind: &str,
        column: &str,
    ) -> Result<Option<Self>> {
        let option = |key: &str| {
            let field_key = format!("fields.{column}.{key}");
            let type_key = format!("{kind}.{key}");
            [(user, Some(key)), (defaults, Some(key)), (table, None)]
                .into_iter()
                .find_map(|(source, bare_key)| {
                    source
                        .get(&field_key)
                        .or_else(|| source.get(&type_key))
                        .or_else(|| bare_key.and_then(|key| source.get(key)))
                })
        };
        match option("sharding").map(|value| value.trim()) {
            None | Some("range") => Ok(None),
            Some("centroid") => {
                let nlist = option("centroid.nlist").map_or("auto", |value| value.trim());
                if nlist != "auto" && nlist.parse::<i32>().ok().is_none_or(|n| n <= 0) {
                    return Err(centroid_index::invalid(
                        "'centroid.nlist' must be a positive integer or 'auto'",
                    ));
                }
                let max_points_per_centroid = option("ivf.train.max-points-per-centroid").cloned();
                Ok(Some(Self {
                    nlist: nlist.into(),
                    max_points_per_centroid,
                    bitmap: None,
                }))
            }
            Some(value) => Err(centroid_index::invalid(format!(
                "Invalid vector sharding mode '{value}', expected 'range' or 'centroid'"
            ))),
        }
    }

    pub(super) fn validate_table(&self, table: &Table) -> Result<()> {
        if !table.schema().core_options().data_evolution_enabled() {
            return Err(centroid_index::invalid(
                "Centroid sharding requires a Data Evolution table",
            ));
        }
        if self.bitmap.is_some()
            && table
                .schema()
                .core_options()
                .global_index_column_update_action()?
                == crate::spec::GlobalIndexColumnUpdateAction::Ignore
        {
            return Err(centroid_index::invalid(
                "Centroid Bitmap fields require THROW_ERROR or DROP_PARTITION_INDEX",
            ));
        }
        Ok(())
    }

    fn router_config(
        &self,
        local: &VindexVectorIndexOptions,
        count: usize,
    ) -> Result<VectorIndexConfig> {
        let native = &local.native_options;
        let mut options = HashMap::from([
            ("index.type".into(), "ivf_flat".into()),
            ("dimension".into(), local.dimension().to_string()),
            ("metric".into(), native["metric"].clone()),
            ("ivf.coarse-assignment".into(), "exact".into()),
            ("nlist".into(), self.nlist.clone()),
            (
                "expected-vector-count".into(),
                native
                    .get("expected-vector-count")
                    .cloned()
                    .unwrap_or_else(|| count.to_string()),
            ),
        ]);
        if let Some(points) = &self.max_points_per_centroid {
            options.insert("ivf.train.max-points-per-centroid".into(), points.clone());
        }
        VectorIndexConfig::from_options(&options).map_err(|error| Error::ConfigInvalid {
            message: format!("Invalid centroid router options: {error}"),
        })
    }
}

impl VindexIndexBuildBuilder<'_> {
    pub(super) fn centroid_replacements(
        &self,
        view: &GlobalIndexBuildView,
        field_id: i32,
    ) -> Result<Vec<IndexManifestEntry>> {
        let partition_filter = self
            .partition_filter
            .as_ref()
            .map(|predicate| {
                let mapping = crate::spec::field_idx_to_partition_idx(
                    self.table.schema().fields(),
                    self.table.schema().partition_keys(),
                );
                let predicate = predicate.remap_field_index(&mapping).ok_or_else(|| {
                    centroid_index::invalid("Cannot map centroid build filter to partition fields")
                })?;
                Ok::<_, Error>(PartitionFilter::from_predicate(
                    predicate,
                    &self.table.schema().partition_fields(),
                ))
            })
            .transpose()?;
        let mut replacements = vec![];
        for entry in view.index_entries() {
            if entry.kind != FileKind::Add
                || entry.index_file.index_type != self.index_type
                || entry
                    .index_file
                    .global_index_meta
                    .as_ref()
                    .is_none_or(|meta| meta.index_field_id != field_id)
            {
                continue;
            }
            if let Some(filter) = &partition_filter {
                if !filter.matches_entry_for_removal(
                    &entry.partition,
                    self.table.schema().partition_keys().len(),
                )? {
                    continue;
                }
            }
            replacements.push(entry.clone());
        }
        Ok(replacements)
    }

    pub(super) async fn build_centroid_index(
        &self,
        shard: &VectorIndexShard,
        column: &str,
        field_id: i32,
        options: &VindexVectorIndexOptions,
        centroid: CentroidBuildOptions,
    ) -> Result<CommitMessage> {
        let mut read_builder = self.table.new_read_builder();
        let mut projection = vec![column, ROW_ID_FIELD_NAME];
        if let Some(bitmap) = &centroid.bitmap {
            projection.extend(bitmap.fields().map(|field| field.name()));
        }
        read_builder.with_projection(&projection)?;
        let read = read_builder.new_read()?;
        let mut batches = read.to_arrow(&[data_split_for_shard(shard)?])?;
        let mut spill = VectorSpill::new(
            RowRange::new(shard.row_range_start, shard.row_range_end),
            options.dimension(),
        )?;
        let mut bitmap_spill = centroid
            .bitmap
            .as_ref()
            .map(|_| BitmapSpill::new())
            .transpose()?;
        while let Some(batch) = batches.try_next().await? {
            spill.push_batch(&batch, column, false).await?;
            if let (Some(bitmap), Some(bitmap_spill)) = (&centroid.bitmap, &mut bitmap_spill) {
                bitmap_spill.record(bitmap, &batch, column).await?;
            }
        }
        let bitmap_spill = match bitmap_spill {
            Some(spill) => Some(spill.finish().await?),
            None => None,
        };

        let mut message = CommitMessage::new(shard.partition_bytes.clone(), 0, vec![]);
        let outcome = self
            .write_centroid_generation(
                shard,
                field_id,
                options,
                centroid,
                spill,
                bitmap_spill,
                &mut message.new_index_files,
            )
            .await;
        if let Err(error) = outcome {
            super::super::global_index_build_common::preparation::abort_private_outputs(
                self.table,
                &[message],
            )
            .await;
            return Err(error);
        }
        Ok(message)
    }

    #[allow(clippy::too_many_arguments)]
    async fn write_centroid_generation(
        &self,
        shard: &VectorIndexShard,
        field_id: i32,
        options: &VindexVectorIndexOptions,
        centroid: CentroidBuildOptions,
        spill: VectorSpill,
        bitmap_spill: Option<BitmapSpillReader>,
        files: &mut Vec<IndexFileMeta>,
    ) -> Result<()> {
        let extra_fields = centroid
            .bitmap
            .as_ref()
            .map(|bitmap| bitmap.fields().map(|field| field.id()).collect::<Vec<_>>())
            .unwrap_or_default();
        let metric = options.native_options["metric"].as_bytes();
        let (nlist, model_file) = if let Some(mut spill) = spill.finish().await? {
            let vector_count = spill.vector_count();
            let range_size = shard.row_range_end - shard.row_range_start + 1;
            let config = centroid.router_config(options, vector_count)?;
            let stores_local_nlist =
                centroid_index::stores_local_nlist(&self.index_type, config.resolved().metric);
            let ratio = options.train_sample_ratio;
            let (router, mut routed) = tokio::task::spawn_blocking(move || {
                let router = CentroidRouter::from_training(spill.train_router(config, ratio)?)
                    .map_err(centroid_index::router_error)?;
                let routed = spill.route(router.nlist(), |vector| {
                    router
                        .assign_centroid(vector)
                        .map_err(centroid_index::router_error)
                })?;
                Ok::<_, Error>((router, routed))
            })
            .await
            .map_err(task_error)??;
            let mut bitmap_rows =
                bitmap_spill.zip(centroid.bitmap.as_ref().map(BitmapBuild::sorters));
            for id in 0..router.nlist() {
                let (next_routed, next_bitmap_rows, rows) =
                    tokio::task::spawn_blocking(move || {
                        let mut rows = routed.partition(id)?;
                        if let (Some(rows), Some((spill, sorters))) =
                            (rows.as_mut(), bitmap_rows.as_mut())
                        {
                            let base = (id as i64).checked_mul(range_size).ok_or_else(|| {
                                centroid_index::invalid("Centroid Bitmap position overflow")
                            })?;
                            spill.add_rows(sorters, rows.local_row_ids()?, base)?;
                        }
                        Ok::<_, Error>((routed, bitmap_rows, rows))
                    })
                    .await
                    .map_err(task_error)??;
                routed = next_routed;
                bitmap_rows = next_bitmap_rows;
                let (count, index, local_nlist) = match rows {
                    Some(rows) => {
                        let count = rows.vector_count();
                        let mut local = options.clone();
                        local
                            .native_options
                            .insert("expected-vector-count".into(), count.to_string());
                        if let Some(nlist) = local.native_options.get_mut("nlist") {
                            if let Ok(requested) = nlist.trim().parse::<usize>() {
                                let samples =
                                    checked_training_vector_count(count, local.train_sample_ratio)?;
                                *nlist = requested.min(samples).to_string();
                            }
                        }
                        let writer = rows.train(&local, false).await?.writer;
                        let local_nlist = if stores_local_nlist {
                            let nlist = match &writer {
                                VectorIndexWriter::IvfFlat(index) => index.nlist,
                                VectorIndexWriter::IvfPq(index) => index.nlist,
                                _ => unreachable!(
                                    "local nlist is stored only for IVF-FLAT and IVF-PQ"
                                ),
                            };
                            Some(
                                i32::try_from(nlist)
                                    .map_err(|_| {
                                        centroid_index::invalid("Centroid shard nlist is too large")
                                    })?
                                    .to_be_bytes(),
                            )
                        } else {
                            None
                        };
                        (count as i64, Some(writer), local_nlist)
                    }
                    None => (0, None, None),
                };
                files.push(
                    self.write_centroid_file(
                        shard,
                        field_id,
                        &extra_fields,
                        Descriptor {
                            role: Role::Shard(id),
                            payload: local_nlist.as_ref().map(|bytes| bytes.as_slice()),
                        },
                        count,
                        index,
                        Bytes::from_static(EMPTY_SHARD),
                    )
                    .await?,
                );
            }
            if let (Some(bitmap), Some((_, sorters))) = (&centroid.bitmap, bitmap_rows) {
                for (field, sorter) in bitmap.fields().zip(sorters) {
                    let sorted = tokio::task::spawn_blocking(move || sorter.finish())
                        .await
                        .map_err(task_error)??;
                    let file = self
                        .centroid_file_writer(
                            shard,
                            field_id,
                            &extra_fields,
                            new_global_index_file_name("bitmap"),
                        )
                        .await?
                        .write(self.table, |output| async move {
                            bitmap
                                .write(output, sorted, vector_count as i64, field.id())
                                .await
                                .map(Some)
                        })
                        .await?
                        .expect("centroid bitmap always writes a payload");
                    files.push(file);
                }
            }
            (router.nlist(), Bytes::from(router.serialize()))
        } else {
            (0, Bytes::new())
        };
        files.push(
            self.write_centroid_file(
                shard,
                field_id,
                &extra_fields,
                Descriptor {
                    role: Role::Router(nlist),
                    payload: Some(metric),
                },
                0,
                None,
                model_file,
            )
            .await?,
        );
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn write_centroid_file(
        &self,
        shard: &VectorIndexShard,
        field_id: i32,
        extra_fields: &[i32],
        descriptor: Descriptor<'_>,
        row_count: i64,
        writer: Option<VectorIndexWriter>,
        bytes: Bytes,
    ) -> Result<IndexFileMeta> {
        let prefix = format!("vector-{}", self.index_type);
        let file_name = if matches!(descriptor.role, Role::Router(_)) {
            format!(
                "{prefix}-global-index-training-model-{}.index",
                uuid::Uuid::new_v4()
            )
        } else {
            new_global_index_file_name(&prefix)
        };
        let index_meta = descriptor.encode()?;
        self.centroid_file_writer(shard, field_id, extra_fields, file_name)
            .await?
            .write(self.table, |output| async move {
                match writer {
                    Some(writer) => serialize_writer(writer, output).await?,
                    None => output.write(bytes).await?,
                }
                Ok(Some(IndexPayload {
                    row_count,
                    index_meta,
                }))
            })
            .await
            .map(|result| result.expect("centroid index always writes a payload"))
    }
    async fn centroid_file_writer(
        &self,
        shard: &VectorIndexShard,
        field_id: i32,
        extra_fields: &[i32],
        file_name: String,
    ) -> Result<IndexFileWriter> {
        let meta = GlobalIndexMeta {
            row_range_start: shard.row_range_start,
            row_range_end: shard.row_range_end,
            index_field_id: field_id,
            extra_field_ids: (!extra_fields.is_empty()).then(|| extra_fields.to_vec()),
            source_meta: Some(
                DataEvolutionIndexSourceMeta::new_with_schema_id(
                    shard.snapshot_id,
                    self.table.schema().id(),
                )?
                .serialize(),
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
        .await
    }
}

fn task_error(error: tokio::task::JoinError) -> Error {
    Error::UnexpectedError {
        message: format!("Centroid index build task failed: {error}"),
        source: Some(Box::new(error)),
    }
}
