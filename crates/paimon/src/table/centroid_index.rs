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

//! Manifest and routing model formats shared with Paimon Java.

use super::index_file_path::IndexFileLocation;
use crate::io::{FileIO, FileRead};
use crate::spec::{DataEvolutionIndexSourceMeta, IndexFileMeta, IndexManifestEntry};
use crate::{Error, Result};
use paimon_vindex_core::distance::MetricType;
use paimon_vindex_core::index::CentroidRouter;
use std::collections::BTreeMap;

const META_MAGIC: &[u8; 8] = b"PAIMCENT";
const META_HEADER: usize = 14;
const META_VERSION: u8 = 1;
pub(super) const EMPTY_SHARD: &[u8] = b"PCEMPTY1";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Role {
    Router(usize),
    Shard(usize),
    Bitmap(usize),
}

#[derive(Clone, Copy)]
pub(super) struct Descriptor<'a> {
    pub(super) role: Role,
    pub(super) payload: Option<&'a [u8]>,
}

impl<'a> Descriptor<'a> {
    pub(super) fn is_empty_router(file: &'a IndexFileMeta) -> Result<bool> {
        Ok(Self::parse(file)?.is_some_and(|descriptor| descriptor.role == Role::Router(0)))
    }

    pub(super) fn is_wrapped(file: &IndexFileMeta) -> bool {
        file.global_index_meta
            .as_ref()
            .and_then(|meta| meta.index_meta.as_deref())
            .is_some_and(|bytes| bytes.starts_with(META_MAGIC))
    }

    pub(super) fn has_bitmaps(file: &IndexFileMeta) -> bool {
        Self::is_wrapped(file)
            && file
                .global_index_meta
                .as_ref()
                .and_then(|meta| meta.extra_field_ids.as_ref())
                .is_some_and(|fields| !fields.is_empty())
    }

    pub(super) fn parse(file: &'a IndexFileMeta) -> Result<Option<Self>> {
        let Some(bytes) = file
            .global_index_meta
            .as_ref()
            .and_then(|meta| meta.index_meta.as_deref())
            .filter(|bytes| bytes.starts_with(META_MAGIC))
        else {
            return Ok(None);
        };
        let invalid = || {
            invalid(format!(
                "Invalid centroid descriptor for '{}'",
                file.file_name
            ))
        };
        if bytes.len() < META_HEADER || bytes[9] != META_VERSION {
            return Err(invalid());
        }
        let count = read_u32(bytes, 10, true);
        if count > i32::MAX as u32 {
            return Err(invalid());
        }
        let role = match bytes[8] {
            b'R' => Role::Router(count as usize),
            b'S' => Role::Shard(count as usize),
            b'B' => Role::Bitmap(count as usize),
            _ => return Err(invalid()),
        };
        Ok(Some(Self {
            role,
            payload: (bytes.len() > META_HEADER).then_some(&bytes[META_HEADER..]),
        }))
    }

    pub(super) fn encode(&self) -> Result<Vec<u8>> {
        let (role, count) = match self.role {
            Role::Router(count) => (b'R', count),
            Role::Shard(id) => (b'S', id),
            Role::Bitmap(id) => (b'B', id),
        };
        let count =
            i32::try_from(count).map_err(|_| invalid("Centroid descriptor value is too large"))?;
        let payload = self.payload.unwrap_or_default();
        let mut bytes = Vec::with_capacity(META_HEADER + payload.len());
        bytes.extend_from_slice(META_MAGIC);
        bytes.extend([role, META_VERSION]);
        bytes.extend(count.to_be_bytes());
        bytes.extend_from_slice(payload);
        Ok(bytes)
    }

    pub(super) fn router_metric(&self) -> Result<MetricType> {
        match self.payload {
            Some(b"l2") => Ok(MetricType::L2),
            Some(b"inner_product") => Ok(MetricType::InnerProduct),
            Some(b"cosine") => Ok(MetricType::Cosine),
            _ => Err(invalid("Invalid centroid routing metric")),
        }
    }

    pub(super) fn local_nlist(&self) -> Result<usize> {
        let bytes: [u8; 4] = self
            .payload
            .and_then(|payload| payload.try_into().ok())
            .ok_or_else(|| invalid("Invalid centroid shard local nlist"))?;
        let count = i32::from_be_bytes(bytes);
        if count <= 0 {
            return Err(invalid("Invalid centroid shard local nlist"));
        }
        Ok(count as usize)
    }
}

pub(super) fn stores_local_nlist(index_type: &str, metric: MetricType) -> bool {
    metric == MetricType::L2 && matches!(index_type, "ivf-flat" | "ivf-pq")
}

pub(super) struct Generation<'a> {
    pub(super) router: &'a IndexManifestEntry,
    pub(super) descriptor: Descriptor<'a>,
    pub(super) shards: BTreeMap<usize, &'a IndexManifestEntry>,
    pub(super) local_nlists: BTreeMap<usize, usize>,
    pub(super) bitmaps: BTreeMap<usize, &'a IndexManifestEntry>,
    pub(super) bitmap_schema_id: Option<i64>,
    pub(super) vector_count: u64,
    pub(super) files: Vec<&'a IndexManifestEntry>,
}

pub(super) fn group<'a>(
    entries: impl IntoIterator<Item = &'a IndexManifestEntry>,
) -> Result<Vec<Generation<'a>>> {
    // Group by range and partition as well as index identity. A complete
    // generation can be moved to a new range by a constant row-ID offset.
    let mut grouped = BTreeMap::<_, Vec<_>>::new();
    for entry in entries {
        let file = &entry.index_file;
        let Some(descriptor) = Descriptor::parse(file)? else {
            continue;
        };
        let meta = file.global_index_meta.as_ref().unwrap();
        grouped
            .entry((
                entry.partition.as_slice(),
                entry.bucket,
                file.index_type.as_str(),
                meta.index_field_id,
                meta.row_range_start,
                meta.row_range_end,
            ))
            .or_default()
            .push((entry, descriptor));
    }
    grouped
        .into_values()
        .map(|files| {
            let (router, descriptor) = files
                .iter()
                .find(|(_, desc)| matches!(desc.role, Role::Router(_)))
                .ok_or_else(|| invalid("Centroid generation has no routing model"))?;
            let router = *router;
            let mut shards = BTreeMap::new();
            let mut local_nlists = BTreeMap::new();
            let mut bitmaps = BTreeMap::new();
            let mut vector_count = 0u64;
            let mut router_count = 0;
            let Role::Router(nlist) = descriptor.role else {
                unreachable!()
            };
            let meta = router.index_file.global_index_meta.as_ref().unwrap();
            let range_len =
                meta.row_range_end
                    .checked_sub(meta.row_range_start)
                    .and_then(|n| n.checked_add(1))
                    .filter(|&n| meta.row_range_start >= 0 && n > 0)
                    .ok_or_else(|| invalid("Invalid centroid row range"))? as u64;
            let invalid = || {
                invalid(format!(
                    "Incomplete or conflicting centroid generation for '{}'",
                    router.index_file.file_name
                ))
            };
            let metric = descriptor.router_metric()?;
            let has_local_nlist = stores_local_nlist(&router.index_file.index_type, metric);
            let extra = meta.extra_field_ids.as_deref().unwrap_or_default();
            if extra.contains(&meta.index_field_id)
                || extra
                    .iter()
                    .enumerate()
                    .any(|(i, field)| extra[..i].contains(field))
            {
                return Err(invalid());
            }
            for (entry, desc) in &files {
                let file = &entry.index_file;
                let file_meta = file.global_index_meta.as_ref().unwrap();
                if file.file_size < 0
                    || file.row_count < 0
                    || file_meta.source_meta != meta.source_meta
                    || file_meta.extra_field_ids != meta.extra_field_ids
                {
                    return Err(invalid());
                }
                match desc.role {
                    Role::Router(count) => {
                        router_count += 1;
                        if file.row_count != 0 || (file.file_size == 0) != (count == 0) {
                            return Err(invalid());
                        }
                    }
                    Role::Shard(id) => {
                        if file.file_size == 0 || id >= nlist || shards.insert(id, *entry).is_some()
                        {
                            return Err(invalid());
                        }
                        if has_local_nlist && file.row_count > 0 {
                            local_nlists.insert(id, desc.local_nlist()?);
                        } else if desc.payload.is_some() {
                            return Err(invalid());
                        }
                        vector_count = vector_count
                            .checked_add(file.row_count as u64)
                            .ok_or_else(invalid)?;
                    }
                    Role::Bitmap(id) => {
                        if extra.is_empty()
                            || file.file_size == 0
                            || id >= nlist
                            || bitmaps.insert(id, *entry).is_some()
                        {
                            return Err(invalid());
                        }
                    }
                }
            }
            if router_count != 1
                || shards.len() != nlist
                || vector_count > range_len
                || (nlist != 0 && vector_count == 0)
            {
                return Err(invalid());
            }
            if !extra.is_empty()
                && shards.iter().any(|(id, shard)| {
                    let count = shard.index_file.row_count;
                    bitmaps.get(id).map(|entry| entry.index_file.row_count)
                        != (count > 0).then_some(count)
                })
            {
                return Err(invalid());
            }
            let bitmap_schema_id = if bitmaps.is_empty() {
                None
            } else {
                let source = meta.source_meta.as_deref().ok_or_else(invalid)?;
                let schema_id =
                    DataEvolutionIndexSourceMeta::deserialize(source)?.source_schema_id();
                if schema_id < 0 {
                    return Err(invalid());
                }
                Some(schema_id)
            };
            Ok(Generation {
                router,
                descriptor: *descriptor,
                shards,
                local_nlists,
                bitmaps,
                bitmap_schema_id,
                vector_count,
                files: files.into_iter().map(|(entry, _)| entry).collect(),
            })
        })
        .collect()
}

pub(super) struct Router {
    pub(super) metric: MetricType,
    pub(super) model: CentroidRouter,
}

impl Router {
    pub(super) async fn load(
        file_io: &FileIO,
        table_path: &str,
        file: &IndexFileMeta,
        descriptor: &Descriptor<'_>,
    ) -> Result<Self> {
        let Role::Router(nlist) = descriptor.role else {
            return Err(invalid("Expected a centroid routing model"));
        };
        let path = IndexFileLocation::Global { table_path }
            .resolve(&file.file_name, file.external_path.as_deref());
        let size = usize::try_from(file.file_size).unwrap_or_default();
        if !(CentroidRouter::HEADER_BYTES..=CentroidRouter::MAX_SERIALIZED_BYTES).contains(&size) {
            return Err(invalid("Invalid centroid routing file size"));
        }
        let input = file_io.new_input(&path)?.reader().await?;
        let bytes = input.read(0..size as u64).await?;
        let router = Self::decode(&bytes, nlist)?;
        if router.metric != descriptor.router_metric()? {
            return Err(invalid("Routing metric differs from manifest metadata"));
        }
        Ok(router)
    }

    fn decode(payload: &[u8], nlist: usize) -> Result<Self> {
        let invalid = || invalid("Invalid centroid routing payload");
        let model = CentroidRouter::deserialize(payload).map_err(router_error)?;
        if model.nlist() != nlist {
            return Err(invalid());
        }
        // Vector Index 0.6.0 validates the metric in the header but exposes no getter.
        let metric = MetricType::from_code(read_u32(payload, 16, false)).ok_or_else(invalid)?;
        Ok(Self { metric, model })
    }
}

/// A colocated Bitmap is part of its primary vector index's identity.
pub(super) fn index_identity_extra_fields(file: &IndexFileMeta) -> Option<&[i32]> {
    let meta = file.global_index_meta.as_ref()?;
    if Descriptor::has_bitmaps(file) {
        None
    } else {
        meta.extra_field_ids.as_deref()
    }
}

pub(super) fn automatic_width(
    nlist: usize,
    vector_count: u64,
    top_k: usize,
    matching: Option<u64>,
    metric: MetricType,
) -> usize {
    if metric == MetricType::InnerProduct {
        return nlist;
    }
    let floor = match nlist {
        4 => 3,
        8 => 4,
        _ => 8,
    };
    let per_shard = vector_count.div_ceil(nlist as u64);
    let budget = (top_k as u128 * 4).div_ceil(per_shard as u128);
    let width = budget
        .max(floor)
        .max(nlist.div_ceil(16) as u128)
        .min(nlist as u128);
    match matching {
        Some(0) => 1,
        Some(matching) if matching < vector_count => (width * vector_count as u128)
            .div_ceil(matching as u128)
            .min(nlist as u128) as usize,
        _ => width as usize,
    }
}

fn read_u32(bytes: &[u8], offset: usize, big_endian: bool) -> u32 {
    let bytes = bytes[offset..offset + 4].try_into().unwrap();
    if big_endian {
        u32::from_be_bytes(bytes)
    } else {
        u32::from_le_bytes(bytes)
    }
}

pub(super) fn invalid(message: impl Into<String>) -> Error {
    Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

pub(super) fn router_error(error: std::io::Error) -> Error {
    Error::DataInvalid {
        message: format!("Invalid centroid router: {error}"),
        source: Some(Box::new(error)),
    }
}
