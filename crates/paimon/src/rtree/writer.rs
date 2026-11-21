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

use super::format::{checksum, page_bytes, page_count, put_box, Metadata};
use super::{invalid, BoundingBox, RTreeFileMeta, RTreeIndexOptions};
use crate::io::FileWrite;
use bytes::Bytes;
use std::io;

/// Supplies uniquely named output files. Publishing index metadata is the caller's responsibility.
#[async_trait::async_trait]
pub trait RTreeOutput: Send {
    async fn create(&mut self) -> io::Result<(String, Box<dyn FileWrite>)>;
}

#[derive(Clone, Copy)]
struct Entry {
    bounds: BoundingBox,
    id: u64,
    hilbert: u32,
}

/// Buffers a bounded partition of envelopes, never the source WKB.
/// Source row IDs must be unique, non-negative Java longs.
pub struct RTreeIndexWriter<O: RTreeOutput> {
    output: O,
    options: RTreeIndexOptions,
    entries: Vec<Entry>,
    nulls: Vec<u64>,
    non_nulls: Vec<u64>,
    first: u64,
    last: u64,
    files: Vec<RTreeFileMeta>,
    unordered_ids: bool,
    finished: bool,
}

impl<O: RTreeOutput> RTreeIndexWriter<O> {
    pub fn new(output: O, options: RTreeIndexOptions) -> io::Result<Self> {
        options.validate()?;
        Ok(Self {
            output,
            options,
            entries: Vec::new(),
            nulls: Vec::new(),
            non_nulls: Vec::new(),
            first: u64::MAX,
            last: 0,
            files: Vec::new(),
            finished: false,
            unordered_ids: false,
        })
    }

    pub async fn write(&mut self, wkb: Option<&[u8]>, row_id: u64) -> io::Result<()> {
        if self.finished {
            return Err(invalid("R-tree writer is finished"));
        }
        if row_id >= i64::MAX as u64 {
            return Err(invalid("R-tree row ID must be less than i64::MAX"));
        }
        let bounds = wkb.map(BoundingBox::from_wkb).transpose()?;
        if self.nulls.len() + self.non_nulls.len() == self.options.partition_rows {
            self.flush().await?;
        }
        self.unordered_ids |= self.first != u64::MAX && row_id <= self.last;
        self.first = self.first.min(row_id);
        self.last = self.last.max(row_id);
        match bounds {
            None => self.nulls.push(row_id),
            Some(bounds) => {
                self.non_nulls.push(row_id);
                if !bounds.is_empty() {
                    self.entries.push(Entry {
                        bounds,
                        id: row_id,
                        hilbert: 0,
                    });
                }
            }
        }
        Ok(())
    }

    pub async fn finish(&mut self) -> io::Result<Vec<RTreeFileMeta>> {
        if self.finished {
            return Err(invalid("R-tree writer is finished"));
        }
        self.flush().await?;
        self.finished = true;
        Ok(self.files.clone())
    }

    async fn flush(&mut self) -> io::Result<()> {
        if self.nulls.is_empty() && self.non_nulls.is_empty() {
            return Ok(());
        }
        // A partially written file cannot safely be retried through this writer.
        self.finished = true;
        if self.unordered_ids {
            let mut ids: Vec<_> = self.nulls.iter().chain(&self.non_nulls).copied().collect();
            ids.sort_unstable();
            if ids.windows(2).any(|pair| pair[0] == pair[1]) {
                return Err(invalid("Duplicate R-tree source row ID"));
            }
        }
        let bounds = self
            .entries
            .iter()
            .fold(BoundingBox::empty(), |total, entry| {
                total.union(entry.bounds)
            });
        for entry in &mut self.entries {
            let x = normalize(
                entry.bounds.min_x() / 2.0 + entry.bounds.max_x() / 2.0,
                bounds.min_x(),
                bounds.max_x(),
            );
            let y = normalize(
                entry.bounds.min_y() / 2.0 + entry.bounds.max_y() / 2.0,
                bounds.min_y(),
                bounds.max_y(),
            );
            entry.hilbert = hilbert_index(x, y);
        }
        self.entries
            .sort_unstable_by_key(|entry| (entry.hilbert, entry.id));
        let meta = Metadata {
            page_size: self.options.page_size,
            items: self.entries.len(),
            pages: page_count(self.entries.len(), self.options.page_size),
            nulls: self.nulls.len(),
            non_nulls: self.non_nulls.len(),
            first: self.first,
            last: self.last,
            bounds,
        };
        let header = meta.encode();
        let (name, mut output) = self.output.create().await?;
        output
            .write(Bytes::copy_from_slice(&header))
            .await
            .map_err(io::Error::other)?;
        let mut level = std::mem::take(&mut self.entries);
        let mut page_id = 0;
        let mut depth = 0_u32;
        while !level.is_empty() {
            let mut parents = Vec::with_capacity(level.len().div_ceil(meta.page_size));
            for entries in level.chunks(meta.page_size) {
                let mut bytes = vec![0; page_bytes(meta.page_size)];
                bytes[..4].copy_from_slice(&(entries.len() as u32).to_be_bytes());
                bytes[4..8].copy_from_slice(&depth.to_be_bytes());
                let mut page_bounds = BoundingBox::empty();
                for (index, entry) in entries.iter().enumerate() {
                    let at = 8 + index * 40;
                    put_box(&mut bytes[at..at + 32], entry.bounds);
                    bytes[at + 32..at + 40].copy_from_slice(&entry.id.to_be_bytes());
                    page_bounds = page_bounds.union(entry.bounds);
                }
                checksum(&mut bytes);
                output
                    .write(Bytes::from(bytes))
                    .await
                    .map_err(io::Error::other)?;
                parents.push(Entry {
                    bounds: page_bounds,
                    id: page_id,
                    hilbert: 0,
                });
                page_id += 1;
            }
            if parents.len() == 1 {
                break;
            }
            level = parents;
            depth += 1;
        }
        for ids in [&self.nulls, &self.non_nulls] {
            let mut block = Vec::with_capacity(ids.len() * 8 + 4);
            for id in ids {
                block.extend_from_slice(&id.to_be_bytes());
            }
            block.extend_from_slice(&[0; 4]);
            checksum(&mut block);
            output
                .write(Bytes::from(block))
                .await
                .map_err(io::Error::other)?;
        }
        output.close().await.map_err(io::Error::other)?;
        self.files.push(RTreeFileMeta {
            name,
            file_size: meta.file_size(),
            row_count: meta.row_count(),
            header,
        });
        // Recover the leaf capacity for the next partition, without retaining parents.
        self.entries = Vec::with_capacity(self.options.partition_rows.min(meta.items));
        self.nulls.clear();
        self.non_nulls.clear();
        self.first = u64::MAX;
        self.last = 0;
        self.unordered_ids = false;
        self.finished = false;
        Ok(())
    }
}

fn normalize(center: f64, min: f64, max: f64) -> u32 {
    let extent = max / 2.0 - min / 2.0;
    if extent == 0.0 {
        return 0;
    }
    (65535.0 * ((center / 2.0 - min / 2.0) / extent)).clamp(0.0, 65535.0) as u32
}

// Skilling's axes-to-transpose transform, matching HilbertCurve.bits(16).dimensions(2).
pub(crate) fn hilbert_index(x: u32, y: u32) -> u32 {
    let mut axes = [x, y];
    let mut q = 1 << 15;
    while q > 1 {
        let p = q - 1;
        for i in 0..2 {
            if axes[i] & q != 0 {
                axes[0] ^= p;
            } else {
                let t = (axes[0] ^ axes[i]) & p;
                axes[0] ^= t;
                axes[i] ^= t;
            }
        }
        q >>= 1;
    }
    axes[1] ^= axes[0];
    let mut t = 0;
    q = 1 << 15;
    while q > 1 {
        if axes[1] & q != 0 {
            t ^= q - 1;
        }
        q >>= 1;
    }
    axes[0] ^= t;
    axes[1] ^= t;
    let mut result = 0;
    for bit in (0..16).rev() {
        result = (result << 1) | ((axes[0] >> bit) & 1);
        result = (result << 1) | ((axes[1] >> bit) & 1);
    }
    result
}
