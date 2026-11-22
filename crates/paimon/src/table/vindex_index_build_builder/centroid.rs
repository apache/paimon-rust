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
use super::VindexIndexBuildBuilder;
use crate::spec::{
    DataEvolutionIndexSourceMeta, FileKind, GlobalIndexMeta, IndexFileMeta, IndexManifestEntry,
    ROW_ID_FIELD_NAME,
};
use crate::table::centroid_index::{self, Descriptor, Role, Router, EMPTY_SHARD};
use crate::table::global_index_build_common::{vector::VectorIndexShard, GlobalIndexBuildView};
use crate::table::index_build::{
    vector::{serialize_writer, VectorSpill},
    IndexFileWriter, IndexPayload,
};
use crate::table::index_file_path::IndexFileLocation;
use crate::table::partition_filter::PartitionFilter;
use crate::table::{CommitMessage, RowRange, Table};
use crate::vindex::VindexVectorIndexOptions;
use crate::{Error, Result};
use futures::TryStreamExt;
use paimon_vindex_core::index::{VectorIndexConfig, VectorIndexWriter};
use std::collections::HashMap;

pub(in crate::table) struct CentroidBuildOptions {
    nlist: String,
    max_points_per_centroid: Option<String>,
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
            [user, defaults, table].iter().find_map(|source| {
                source
                    .get(&format!("fields.{column}.{key}"))
                    .or_else(|| source.get(&format!("{kind}.{key}")))
                    .or_else(|| source.get(key))
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
                if !filter.matches_entry(&entry.partition)? {
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
        read_builder.with_projection(&[column, ROW_ID_FIELD_NAME])?;
        let read = read_builder.new_read()?;
        let mut batches = read.to_arrow(&[data_split_for_shard(shard)?])?;
        let mut spill = VectorSpill::new(
            RowRange::new(shard.row_range_start, shard.row_range_end),
            options.dimension(),
        )?;
        while let Some(batch) = batches.try_next().await? {
            spill.push_batch(&batch, column, false).await?;
        }

        let model_id = uuid::Uuid::new_v4().to_string();
        let mut message = CommitMessage::new(shard.partition_bytes.clone(), 0, vec![]);
        let outcome = self
            .write_centroid_generation(
                shard,
                field_id,
                options,
                centroid,
                spill,
                &model_id,
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
        model_id: &str,
        files: &mut Vec<IndexFileMeta>,
    ) -> Result<()> {
        let mut local = options.clone();
        local.native_options.remove("expected-vector-count");
        let metric = &options.native_options["metric"];
        let router_meta = json_options(&options.native_options)?;
        let (nlist, model_file) = if let Some(mut spill) = spill.finish().await? {
            let config = centroid.router_config(options, spill.vector_count())?;
            let ratio = options.train_sample_ratio;
            let dimension = options.dimension();
            let (router, mut routed) = tokio::task::spawn_blocking(move || {
                let VectorIndexWriter::IvfFlat(flat) = spill.train_router(config, ratio)? else {
                    unreachable!("router configuration uses IVF-FLAT");
                };
                let router =
                    Router::new(flat.metric, dimension, flat.quantizer_centroids().to_vec())?;
                let routed = spill.route(router.nlist(), |vector| router.assign(vector))?;
                Ok::<_, Error>((router, routed))
            })
            .await
            .map_err(task_error)??;
            for id in 0..router.nlist() {
                let (next_routed, rows) = tokio::task::spawn_blocking(move || {
                    let rows = routed.partition(id)?;
                    Ok::<_, Error>((routed, rows))
                })
                .await
                .map_err(task_error)??;
                routed = next_routed;
                let mut payload = HashMap::new();
                let (count, index) = match rows {
                    Some(rows) => {
                        let count = rows.vector_count() as i64;
                        let writer = rows.train(&local, false).await?.writer;
                        let nlist = match &writer {
                            VectorIndexWriter::IvfFlat(index) => Some(index.nlist),
                            VectorIndexWriter::IvfPq(index) => Some(index.nlist),
                            VectorIndexWriter::IvfSq(index) => Some(index.nlist),
                            VectorIndexWriter::IvfRq(index) => Some(index.nlist),
                            VectorIndexWriter::DiskAnn(_) => None,
                        };
                        payload.insert("metric".into(), metric.clone());
                        if let Some(nlist) = nlist {
                            payload.insert("localNlist".into(), nlist.to_string());
                        }
                        (count, Some(writer))
                    }
                    None => (0, None),
                };
                let metadata = json_options(&payload)?;
                files.push(
                    self.write_centroid_file(
                        shard,
                        field_id,
                        Descriptor {
                            model_id,
                            version: 1,
                            role: Role::Shard(id),
                            payload: Some(&metadata),
                        },
                        count,
                        index,
                        EMPTY_SHARD.to_vec(),
                    )
                    .await?,
                );
            }
            (router.nlist(), router.encode(model_id)?)
        } else {
            (0, Router::encode_empty(model_id)?)
        };
        files.push(
            self.write_centroid_file(
                shard,
                field_id,
                Descriptor {
                    model_id,
                    version: 1,
                    role: Role::Router(nlist),
                    payload: Some(&router_meta),
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
        descriptor: Descriptor<'_>,
        row_count: i64,
        writer: Option<VectorIndexWriter>,
        bytes: Vec<u8>,
    ) -> Result<IndexFileMeta> {
        let file_name = format!(
            "vector-{}-global-index-{}.index",
            self.index_type,
            uuid::Uuid::new_v4()
        );
        let meta = GlobalIndexMeta {
            row_range_start: shard.row_range_start,
            row_range_end: shard.row_range_end,
            index_field_id: field_id,
            extra_field_ids: None,
            source_meta: Some(DataEvolutionIndexSourceMeta::new(shard.snapshot_id)?.serialize()),
            index_meta: None,
        };
        let index_meta = descriptor.encode()?;
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
            match writer {
                Some(writer) => serialize_writer(writer, output).await?,
                None => output.write(bytes.into()).await?,
            }
            Ok(Some(IndexPayload {
                row_count,
                index_meta,
            }))
        })
        .await
        .map(|result| result.expect("centroid index always writes a payload"))
    }
}

fn json_options(options: &HashMap<String, String>) -> Result<Vec<u8>> {
    serde_json::to_vec(options).map_err(|error| centroid_index::invalid(error.to_string()))
}

fn task_error(error: tokio::task::JoinError) -> Error {
    Error::UnexpectedError {
        message: format!("Centroid index build task failed: {error}"),
        source: Some(Box::new(error)),
    }
}
