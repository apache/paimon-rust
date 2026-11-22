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
use crate::spec::{IndexFileMeta, IndexManifestEntry};
use crate::{Error, Result};
use paimon_vindex_core::distance::{fvec_l2sqr, fvec_normalize, MetricType};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};

const META_MAGIC: &[u8; 8] = b"PAIMCENT";
const META_HEADER: usize = 22;
const ROUTER_MAGIC: &[u8; 8] = b"PVROUT01";
const ROUTER_HEADER: usize = 20;
const MAX_ENVELOPE: usize = 65536;
const MAX_CENTERS_BYTES: usize = 512 * 1024 * 1024;
pub(super) const EMPTY_SHARD: &[u8] = b"PCEMPTY1";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Role {
    Router(usize),
    Shard(usize),
    Bitmap(usize),
}

pub(super) struct Descriptor<'a> {
    pub(super) model_id: &'a str,
    pub(super) version: u8,
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
        if bytes.len() < META_HEADER {
            return Err(invalid());
        }
        let count = read_u32(bytes, 10, true);
        let id_len = read_u32(bytes, 14, true) as usize;
        let payload_len = read_u32(bytes, 18, true) as i32;
        let version = bytes[8];
        let role = match (version, bytes[9]) {
            (1 | 2, 1) => Role::Router(count as usize),
            (1 | 2, 2) => Role::Shard(count as usize),
            (2, 3) => Role::Bitmap(count as usize),
            _ => return Err(invalid()),
        };
        if count > i32::MAX as u32
            || id_len == 0
            || payload_len < -1
            || Some(bytes.len() - META_HEADER) != id_len.checked_add(payload_len.max(0) as usize)
        {
            return Err(invalid());
        }
        let rest = &bytes[META_HEADER..];
        let model_id = std::str::from_utf8(&rest[..id_len]).map_err(|_| invalid())?;
        Ok(Some(Self {
            model_id,
            version,
            role,
            payload: (payload_len >= 0).then_some(&rest[id_len..]),
        }))
    }

    pub(super) fn encode(&self) -> Result<Vec<u8>> {
        let (role, count) = match self.role {
            Role::Router(count) => (1, count),
            Role::Shard(id) => (2, id),
            Role::Bitmap(id) => (3, id),
        };
        let int =
            |value| i32::try_from(value).map_err(|_| invalid("Centroid descriptor is too large"));
        let mut bytes = META_MAGIC.to_vec();
        bytes.extend([self.version, role]);
        bytes.extend(int(count)?.to_be_bytes());
        bytes.extend(int(self.model_id.len())?.to_be_bytes());
        bytes.extend(
            self.payload
                .map(|bytes| int(bytes.len()))
                .transpose()?
                .unwrap_or(-1)
                .to_be_bytes(),
        );
        bytes.extend(self.model_id.as_bytes());
        bytes.extend(self.payload.unwrap_or_default());
        Ok(bytes)
    }
}

pub(super) struct Generation<'a> {
    pub(super) router: &'a IndexManifestEntry,
    pub(super) descriptor: Descriptor<'a>,
    pub(super) shards: BTreeMap<usize, &'a IndexManifestEntry>,
    pub(super) bitmaps: BTreeMap<usize, &'a IndexManifestEntry>,
    pub(super) bitmap_schema: Option<i64>,
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
            let mut bitmaps = BTreeMap::new();
            let mut bitmap_schema = None;
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
                    "Incomplete or conflicting centroid generation '{}'",
                    descriptor.model_id
                ))
            };
            let extra = meta.extra_field_ids.as_deref().unwrap_or_default();
            if (descriptor.version == 2) == extra.is_empty()
                || extra.contains(&meta.index_field_id)
                || extra
                    .iter()
                    .enumerate()
                    .any(|(i, field)| extra[..i].contains(field))
            {
                return Err(invalid());
            }
            for (entry, desc) in &files {
                if desc.model_id != descriptor.model_id
                    || desc.version != descriptor.version
                    || entry.index_file.file_size <= 0
                    || entry.index_file.row_count < 0
                    || entry
                        .index_file
                        .global_index_meta
                        .as_ref()
                        .unwrap()
                        .source_meta
                        != meta.source_meta
                    || entry
                        .index_file
                        .global_index_meta
                        .as_ref()
                        .unwrap()
                        .extra_field_ids
                        != meta.extra_field_ids
                {
                    return Err(invalid());
                }
                match desc.role {
                    Role::Router(_) => {
                        router_count += 1;
                        if entry.index_file.row_count != 0 {
                            return Err(invalid());
                        }
                    }
                    Role::Shard(id) => {
                        if id >= nlist || shards.insert(id, *entry).is_some() {
                            return Err(invalid());
                        }
                        vector_count = vector_count
                            .checked_add(entry.index_file.row_count as u64)
                            .ok_or_else(invalid)?;
                    }
                    Role::Bitmap(id) => {
                        if id >= nlist || bitmaps.insert(id, *entry).is_some() {
                            return Err(invalid());
                        }
                        let schema_id =
                            super::centroid_bitmap::schema_id(desc.payload.ok_or_else(invalid)?)?;
                        if bitmap_schema
                            .replace(schema_id)
                            .is_some_and(|previous| previous != schema_id)
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
            if descriptor.version == 2
                && shards.iter().any(|(id, shard)| {
                    let count = shard.index_file.row_count;
                    bitmaps.get(id).map(|entry| entry.index_file.row_count)
                        != (count > 0).then_some(count)
                })
            {
                return Err(invalid());
            }
            Ok(Generation {
                router,
                descriptor: Descriptor::parse(&router.index_file)?.unwrap(),
                shards,
                bitmaps,
                bitmap_schema,
                vector_count,
                files: files.into_iter().map(|(entry, _)| entry).collect(),
            })
        })
        .collect()
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Envelope<'a> {
    version: u8,
    shard_mode: &'a str,
    model_id: &'a str,
}

pub(super) struct Router {
    pub(super) metric: MetricType,
    pub(super) dimension: usize,
    centers: Vec<f32>,
}

impl Router {
    pub(super) fn new(metric: MetricType, dimension: usize, centers: Vec<f32>) -> Result<Self> {
        if dimension == 0
            || dimension > i32::MAX as usize
            || !centers.len().is_multiple_of(dimension)
            || centers.is_empty()
            || centers.len() > MAX_CENTERS_BYTES / 4
            || centers.iter().any(|value| !value.is_finite())
        {
            return Err(invalid("Invalid centroid routing centers or dimensions"));
        }
        Ok(Self {
            metric,
            dimension,
            centers,
        })
    }

    pub(super) fn nlist(&self) -> usize {
        self.centers.len() / self.dimension
    }

    pub(super) fn encode(&self, model_id: &str) -> Result<Vec<u8>> {
        let mut bytes = Self::encode_empty(model_id)?;
        bytes.reserve(ROUTER_HEADER + self.centers.len() * 4);
        bytes.extend(ROUTER_MAGIC);
        bytes.extend((self.dimension as u32).to_le_bytes());
        bytes.extend((self.nlist() as u32).to_le_bytes());
        bytes.extend((self.metric as u32).to_le_bytes());
        for center in &self.centers {
            bytes.extend(center.to_le_bytes());
        }
        Ok(bytes)
    }

    pub(super) fn encode_empty(model_id: &str) -> Result<Vec<u8>> {
        let header = serde_json::to_vec(&Envelope {
            version: 1,
            shard_mode: "centroid-based",
            model_id,
        })
        .map_err(|error| invalid(format!("Cannot encode centroid router envelope: {error}")))?;
        let mut bytes = Vec::with_capacity(4 + header.len());
        bytes.extend((header.len() as u32).to_be_bytes());
        bytes.extend(&header);
        Ok(bytes)
    }

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
        if !(4 + ROUTER_HEADER..=4 + MAX_ENVELOPE + ROUTER_HEADER + MAX_CENTERS_BYTES)
            .contains(&size)
        {
            return Err(invalid("Invalid centroid routing file size"));
        }
        let input = file_io.new_input(&path)?.reader().await?;
        let bytes = input.read(0..size as u64).await?;
        Self::decode(&bytes, descriptor.model_id, nlist)
    }

    fn decode(bytes: &[u8], model_id: &str, nlist: usize) -> Result<Self> {
        let invalid = || {
            invalid(format!(
                "Invalid centroid routing payload for model '{model_id}'"
            ))
        };
        if bytes.len() < 4 {
            return Err(invalid());
        }
        let len = read_u32(bytes, 0, true) as usize;
        if len == 0 || len > MAX_ENVELOPE || bytes.len() < 4 + len + ROUTER_HEADER {
            return Err(invalid());
        }
        let header: Envelope<'_> =
            serde_json::from_slice(&bytes[4..4 + len]).map_err(|_| invalid())?;
        let payload = &bytes[4 + len..];
        if header.version != 1
            || header.shard_mode != "centroid-based"
            || header.model_id != model_id
            || !payload.starts_with(ROUTER_MAGIC)
        {
            return Err(invalid());
        }
        let dimension = read_u32(payload, 8, false) as usize;
        if read_u32(payload, 12, false) as usize != nlist
            || dimension.checked_mul(nlist).and_then(|n| n.checked_mul(4))
                != Some(payload.len() - ROUTER_HEADER)
        {
            return Err(invalid());
        }
        let metric = MetricType::from_code(read_u32(payload, 16, false)).ok_or_else(invalid)?;
        let centers = payload[ROUTER_HEADER..]
            .as_chunks::<4>()
            .0
            .iter()
            .copied()
            .map(f32::from_le_bytes)
            .collect();
        Self::new(metric, dimension, centers)
    }

    fn distances<'a>(
        &'a self,
        vector: &'a [f32],
    ) -> Result<impl Iterator<Item = (usize, f32)> + 'a> {
        if vector.len() != self.dimension || vector.iter().any(|value| !value.is_finite()) {
            return Err(invalid(format!(
                "Centroid routing expects {} finite coordinates, got {}",
                self.dimension,
                vector.len()
            )));
        }
        let mut point = Cow::Borrowed(vector);
        if self.metric == MetricType::Cosine {
            fvec_normalize(point.to_mut());
        }
        // Like native training, coarse assignment is Euclidean in the metric's
        // training space, including when the local index ranks by inner product.
        Ok(self
            .centers
            .chunks_exact(self.dimension)
            .enumerate()
            .map(move |(id, center)| (id, fvec_l2sqr(&point, center))))
    }

    pub(super) fn nearest(&self, vector: &[f32]) -> Result<Vec<usize>> {
        let mut distances = self.distances(vector)?.collect::<Vec<_>>();
        if distances.iter().any(|(_, distance)| !distance.is_finite()) {
            return Err(invalid("Non-finite centroid distance"));
        }
        distances.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        Ok(distances.into_iter().map(|(id, _)| id).collect())
    }

    pub(super) fn assign(&self, vector: &[f32]) -> Result<usize> {
        let mut nearest = (0, f32::INFINITY);
        for (id, distance) in self.distances(vector)? {
            if !distance.is_finite() {
                return Err(invalid("Non-finite centroid distance"));
            }
            if distance < nearest.1 {
                nearest = (id, distance);
            }
        }
        Ok(nearest.0)
    }
}

/// A colocated Bitmap is part of its primary vector index's identity.
pub(super) fn index_identity_extra_fields(file: &IndexFileMeta) -> Option<&[i32]> {
    let meta = file.global_index_meta.as_ref()?;
    if Descriptor::is_wrapped(file)
        && meta
            .index_meta
            .as_ref()
            .is_some_and(|bytes| bytes.get(8) == Some(&2))
    {
        None
    } else {
        meta.extra_field_ids.as_deref()
    }
}

pub(super) fn payload_options(descriptor: &Descriptor<'_>) -> Result<HashMap<String, String>> {
    descriptor
        .payload
        .map(|payload| {
            serde_json::from_slice(payload)
                .map_err(|error| invalid(format!("Invalid centroid scoring metadata: {error}")))
        })
        .transpose()
        .map(Option::unwrap_or_default)
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
