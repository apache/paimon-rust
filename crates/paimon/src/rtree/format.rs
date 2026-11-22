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

use super::{invalid, BoundingBox};
use std::io;

pub(crate) const HEADER_SIZE: usize = 80;
pub(crate) const MAX_PARTITION_ROWS: usize = 1_048_576;
const MAGIC: u32 = 0x5052_5431;
const VERSION: u32 = 1;

/// Container metadata and immutable-file identity supplied by the build owner.
#[derive(Clone, Debug)]
pub struct RTreeFileMeta {
    pub name: String,
    pub file_size: u64,
    pub row_count: usize,
    pub header: [u8; HEADER_SIZE],
}

#[derive(Clone, Debug)]
pub(crate) struct Metadata {
    pub page_size: usize,
    pub items: usize,
    pub pages: usize,
    pub nulls: usize,
    pub non_nulls: usize,
    pub first: u64,
    pub last: u64,
    pub bounds: BoundingBox,
}

pub(crate) fn page_bytes(capacity: usize) -> usize {
    12 + 40 * capacity
}

pub(crate) fn page_count(mut items: usize, capacity: usize) -> usize {
    let mut count = 0;
    while items > 0 {
        items = items.div_ceil(capacity);
        count += items;
        if items == 1 {
            break;
        }
    }
    count
}

pub(crate) fn level_starts(mut items: usize, capacity: usize) -> Vec<u32> {
    let mut starts = Vec::new();
    let mut start = 0;
    while items > 0 {
        starts.push(start);
        items = items.div_ceil(capacity);
        start += items as u32;
        if items == 1 {
            break;
        }
    }
    starts
}

pub(crate) fn checksum(bytes: &mut [u8]) {
    let at = bytes.len() - 4;
    let checksum = crc32fast::hash(&bytes[..at]);
    bytes[at..].copy_from_slice(&checksum.to_be_bytes());
}

pub(crate) fn verify(bytes: &[u8]) -> io::Result<()> {
    if bytes.len() < 4
        || crc32fast::hash(&bytes[..bytes.len() - 4])
            != u32::from_be_bytes(
                bytes[bytes.len() - 4..]
                    .try_into()
                    .expect("checked checksum length"),
            )
    {
        return Err(invalid("R-tree checksum mismatch"));
    }
    Ok(())
}

pub(crate) fn put_box(target: &mut [u8], bounds: BoundingBox) {
    for (index, value) in bounds.coordinates.into_iter().enumerate() {
        target[index * 8..index * 8 + 8].copy_from_slice(&value.to_be_bytes());
    }
}

pub(crate) fn get_box(bytes: &[u8]) -> io::Result<BoundingBox> {
    let mut values = [0.0; 4];
    for (index, value) in values.iter_mut().enumerate() {
        *value = f64::from_be_bytes(
            bytes[index * 8..index * 8 + 8]
                .try_into()
                .expect("checked box length"),
        );
    }
    BoundingBox::new(values[0], values[1], values[2], values[3])
}

pub(crate) fn int(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("checked integer length"),
    )
}

pub(crate) fn long(bytes: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("checked long length"),
    )
}

impl Metadata {
    pub fn row_count(&self) -> usize {
        self.nulls + self.non_nulls
    }
    pub fn rows_offset(&self) -> u64 {
        HEADER_SIZE as u64 + (self.pages * page_bytes(self.page_size)) as u64
    }
    pub fn file_size(&self) -> u64 {
        self.rows_offset() + self.row_count() as u64 * 8 + 8
    }

    pub fn encode(&self) -> [u8; HEADER_SIZE] {
        let mut result = [0; HEADER_SIZE];
        for (index, value) in [
            MAGIC,
            VERSION,
            self.page_size as u32,
            self.items as u32,
            self.pages as u32,
            self.nulls as u32,
            self.non_nulls as u32,
        ]
        .into_iter()
        .enumerate()
        {
            result[index * 4..index * 4 + 4].copy_from_slice(&value.to_be_bytes());
        }
        result[28..36].copy_from_slice(&self.first.to_be_bytes());
        result[36..44].copy_from_slice(&self.last.to_be_bytes());
        put_box(&mut result[44..76], self.bounds);
        checksum(&mut result);
        result
    }

    pub fn parse(file: &RTreeFileMeta, total_rows: u64) -> io::Result<Self> {
        let meta = Self::from_header(&file.header, total_rows)?;
        if file.row_count != meta.row_count() || file.file_size != meta.file_size() {
            return Err(invalid("R-tree file size or row count mismatch"));
        }
        Ok(meta)
    }

    pub(crate) fn from_header(bytes: &[u8; HEADER_SIZE], total_rows: u64) -> io::Result<Self> {
        verify(bytes)?;
        let version = int(bytes, 4);
        if int(bytes, 0) != MAGIC || version != VERSION {
            return Err(invalid("Unsupported R-tree file format"));
        }
        let page_size = int(bytes, 8) as usize;
        let items = int(bytes, 12) as usize;
        let pages = int(bytes, 16) as usize;
        let nulls = int(bytes, 20) as usize;
        let non_nulls = int(bytes, 24) as usize;
        let first = long(bytes, 28);
        let last = long(bytes, 36);
        if !(2..=4096).contains(&page_size)
            || nulls > MAX_PARTITION_ROWS
            || non_nulls > MAX_PARTITION_ROWS
            || nulls + non_nulls == 0
            || nulls + non_nulls > MAX_PARTITION_ROWS
            || items > non_nulls
            || first > last
            || (nulls + non_nulls - 1) as u64 > last - first
            || (nulls + non_nulls == 1 && first != last)
            || total_rows > i64::MAX as u64
            || last >= total_rows
            || pages != page_count(items, page_size)
        {
            return Err(invalid("Invalid R-tree header counts or row range"));
        }
        let bounds = if items == 0 {
            let expected = BoundingBox::empty();
            let mut encoded = [0; 32];
            put_box(&mut encoded, expected);
            if bytes[44..76] != encoded {
                return Err(invalid("Invalid empty R-tree bounds"));
            }
            expected
        } else {
            get_box(&bytes[44..76])?
        };
        Ok(Self {
            page_size,
            items,
            pages,
            nulls,
            non_nulls,
            first,
            last,
            bounds,
        })
    }
}
