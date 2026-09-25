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

//! Java Roaring64Bitmap 1.2.1 wire format: ART over high 48 bits followed by
//! serialized 16-bit containers. This is distinct from Rust RoaringTreemap.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BinaryArray};

use super::{unsupported_type_error, FieldAggregator};
use crate::spec::DataType;
use crate::Error;

#[derive(Debug)]
pub(crate) struct Roaring64Agg {
    field_name: String,
    value: Option<Vec<u8>>,
}

impl Roaring64Agg {
    pub(crate) fn new(field_name: &str, data_type: &DataType) -> crate::Result<Self> {
        if !matches!(data_type, DataType::VarBinary(_)) {
            return Err(unsupported_type_error("rbm64", field_name, data_type));
        }
        Ok(Self {
            field_name: field_name.to_string(),
            value: None,
        })
    }
}

impl FieldAggregator for Roaring64Agg {
    fn name(&self) -> &'static str {
        "rbm64"
    }

    fn reset(&mut self) {
        self.value = None;
    }

    fn agg(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        if array.is_null(row_idx) {
            return Ok(());
        }
        let bytes = array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| Error::DataInvalid {
                message: format!("rbm64 column '{}' requires Arrow Binary", self.field_name),
                source: None,
            })?
            .value(row_idx);
        let Some(accumulator) = &self.value else {
            // Java preserves the first non-null byte sequence unchanged.
            self.value = Some(bytes.to_vec());
            return Ok(());
        };
        let mut values = decode(accumulator).map_err(|message| Error::DataInvalid {
            message: format!(
                "Invalid rbm64 accumulator for '{}': {message}",
                self.field_name
            ),
            source: None,
        })?;
        values.extend(decode(bytes).map_err(|message| Error::DataInvalid {
            message: format!("Invalid rbm64 input for '{}': {message}", self.field_name),
            source: None,
        })?);
        self.value = Some(encode(&values)?);
        Ok(())
    }

    fn agg_reversed(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        self.agg(array, row_idx)
    }

    fn result(&self) -> crate::Result<ArrayRef> {
        Ok(Arc::new(BinaryArray::from(vec![self.value.as_deref()])))
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], String> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or("rbm64 offset overflow")?;
        let slice = self
            .bytes
            .get(self.offset..end)
            .ok_or("truncated rbm64 data")?;
        self.offset = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, String> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
}

fn read_art_node(
    reader: &mut Reader<'_>,
    leaves: &mut Vec<([u8; 6], u64)>,
    remaining: &mut u64,
    depth: usize,
) -> Result<(), String> {
    if depth > 6 {
        return Err("rbm64 ART exceeds six key bytes".into());
    }
    let kind = reader.u8()?;
    let child_count = usize::from(reader.u16()?);
    let prefix_len = usize::from(reader.u8()?);
    if prefix_len > 6 {
        return Err("invalid ART prefix length".into());
    }
    reader.take(prefix_len)?;
    match kind {
        0 => {
            reader.take(4)?;
            if child_count > 4 {
                return Err("invalid ART Node4 count".into());
            }
        }
        1 => {
            reader.take(16)?;
            if child_count > 16 {
                return Err("invalid ART Node16 count".into());
            }
        }
        2 => {
            reader.take(256)?;
            if child_count > 48 {
                return Err("invalid ART Node48 count".into());
            }
        }
        3 => {
            reader.take(32)?;
            if child_count > 256 {
                return Err("invalid ART Node256 count".into());
            }
        }
        4 => {
            if *remaining == 0 {
                return Err("ART has more leaves than keySize".into());
            }
            let high: [u8; 6] = reader.take(6)?.try_into().unwrap();
            let index = reader.u64()?;
            leaves.push((high, index));
            *remaining -= 1;
            return Ok(());
        }
        _ => return Err(format!("unknown ART node type {kind}")),
    }
    if child_count == 0 || u64::try_from(child_count).unwrap() > *remaining {
        return Err("invalid ART child count".into());
    }
    for _ in 0..child_count {
        read_art_node(reader, leaves, remaining, depth + 1)?;
    }
    Ok(())
}

fn decode(bytes: &[u8]) -> Result<BTreeSet<u64>, String> {
    let mut reader = Reader { bytes, offset: 0 };
    match reader.u8()? {
        0 if reader.offset == bytes.len() => return Ok(BTreeSet::new()),
        1 => {}
        tag => return Err(format!("invalid rbm64 empty tag {tag}")),
    }
    let key_size = reader.u64()?;
    if key_size == 0 || key_size > bytes.len() as u64 {
        return Err("invalid rbm64 ART keySize".into());
    }
    let mut leaves = Vec::new();
    let mut remaining = key_size;
    read_art_node(&mut reader, &mut leaves, &mut remaining, 0)?;
    if remaining != 0 {
        return Err("ART keySize does not match leaves".into());
    }

    let outer_len = reader.u32()? as usize;
    if outer_len > bytes.len() / 5 {
        return Err("invalid rbm64 container array count".into());
    }
    let mut containers = Vec::with_capacity(outer_len);
    for _ in 0..outer_len {
        if reader.u8()? != 0xfe {
            return Err("unsupported trimmed rbm64 containers".into());
        }
        let inner_len = reader.u32()? as usize;
        if inner_len > bytes.len() {
            return Err("invalid rbm64 container count".into());
        }
        let mut inner = Vec::with_capacity(inner_len);
        for _ in 0..inner_len {
            match reader.u8()? {
                0 => {
                    inner.push(None);
                    continue;
                }
                1 => {}
                tag => return Err(format!("invalid rbm64 container null tag {tag}")),
            }
            let kind = reader.u8()?;
            let cardinality = reader.u32()? as usize;
            if cardinality > 65536 {
                return Err("invalid rbm64 container cardinality".into());
            }
            let mut lows = Vec::with_capacity(cardinality);
            match kind {
                0 => {
                    let runs = usize::from(reader.u16()?);
                    for _ in 0..runs {
                        let start = reader.u16()?;
                        let length = reader.u16()?;
                        let end = u32::from(start) + u32::from(length);
                        if end >= 65536 {
                            return Err("invalid rbm64 run container".into());
                        }
                        lows.extend((u32::from(start)..=end).map(|value| value as u16));
                    }
                }
                1 => {
                    for word_index in 0..1024u32 {
                        let word = reader.u64()?;
                        for bit in 0..64 {
                            if word & (1u64 << bit) != 0 {
                                lows.push((word_index * 64 + bit) as u16);
                            }
                        }
                    }
                }
                2 => {
                    for _ in 0..cardinality {
                        lows.push(reader.u16()?);
                    }
                }
                _ => return Err(format!("unknown rbm64 container type {kind}")),
            }
            if lows.len() != cardinality {
                return Err("rbm64 container cardinality mismatch".into());
            }
            inner.push(Some(lows));
        }
        containers.push(inner);
    }
    let container_size = reader.u64()?;
    reader.u32()?; // last first-level index
    reader.u32()?; // last second-level index
    if reader.offset != bytes.len() || container_size != key_size {
        return Err("rbm64 trailing bytes or container count mismatch".into());
    }
    let mut result = BTreeSet::new();
    for (high, index) in leaves {
        let first = usize::try_from(index >> 32).map_err(|_| "rbm64 index overflow")?;
        let second = usize::try_from(index as u32).map_err(|_| "rbm64 index overflow")?;
        let lows = containers
            .get(first)
            .and_then(|array| array.get(second))
            .and_then(Option::as_ref)
            .ok_or("ART references missing rbm64 container")?;
        let high = high
            .iter()
            .fold(0u64, |value, byte| (value << 8) | u64::from(*byte));
        result.extend(lows.iter().map(|low| (high << 16) | u64::from(*low)));
    }
    Ok(result)
}

fn write_art_node(keys: &[[u8; 6]], depth: usize, base_index: usize, out: &mut Vec<u8>) {
    if keys.len() == 1 {
        out.extend_from_slice(&[4, 0, 0, 0]); // LeafNode, count=0, prefix=0
        out.extend_from_slice(&keys[0]);
        out.extend_from_slice(&(base_index as u64).to_le_bytes());
        return;
    }
    let mut branch = depth;
    while branch < 6 && keys.iter().all(|key| key[branch] == keys[0][branch]) {
        branch += 1;
    }
    debug_assert!(branch < 6);
    let mut groups = Vec::new();
    let mut from = 0;
    while from < keys.len() {
        let mut to = from + 1;
        while to < keys.len() && keys[to][branch] == keys[from][branch] {
            to += 1;
        }
        groups.push((keys[from][branch], from, to));
        from = to;
    }
    out.push(3); // Node256 accepts any fan-out, including small groups.
    out.extend_from_slice(&(groups.len() as u16).to_le_bytes());
    out.push((branch - depth) as u8);
    out.extend_from_slice(&keys[0][depth..branch]);
    let mut mask = [0u64; 4];
    for (byte, _, _) in &groups {
        mask[usize::from(*byte) / 64] |= 1u64 << (*byte % 64);
    }
    for word in mask {
        out.extend_from_slice(&word.to_le_bytes());
    }
    for (_, from, to) in groups {
        write_art_node(&keys[from..to], branch + 1, base_index + from, out);
    }
}

fn encode(values: &BTreeSet<u64>) -> crate::Result<Vec<u8>> {
    if values.is_empty() {
        return Ok(vec![0]);
    }
    let mut grouped: BTreeMap<[u8; 6], Vec<u16>> = BTreeMap::new();
    for &value in values {
        let bytes = value.to_be_bytes();
        grouped
            .entry(bytes[..6].try_into().unwrap())
            .or_default()
            .push(value as u16);
    }
    let keys: Vec<[u8; 6]> = grouped.keys().copied().collect();
    let count = u32::try_from(keys.len()).map_err(|_| Error::DataInvalid {
        message: "rbm64 has too many high-bit containers".into(),
        source: None,
    })?;
    let mut out = Vec::new();
    out.push(1);
    out.extend_from_slice(&u64::from(count).to_le_bytes());
    write_art_node(&keys, 0, 0, &mut out);
    out.extend_from_slice(&1u32.to_le_bytes()); // one first-level container array
    out.push(0xfe); // NOT_TRIMMED_MARK
    out.extend_from_slice(&count.to_le_bytes());
    for lows in grouped.values() {
        out.push(1); // non-null
        out.push(if lows.len() > 4096 { 1 } else { 2 }); // bitmap or array
        out.extend_from_slice(&(lows.len() as u32).to_le_bytes());
        if lows.len() > 4096 {
            let mut words = [0u64; 1024];
            for &low in lows {
                words[usize::from(low) / 64] |= 1u64 << (low % 64);
            }
            for word in words {
                out.extend_from_slice(&word.to_le_bytes());
            }
        } else {
            for low in lows {
                out.extend_from_slice(&low.to_le_bytes());
            }
        }
    }
    out.extend_from_slice(&u64::from(count).to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes()); // firstLevelIdx
    out.extend_from_slice(&(count - 1).to_le_bytes()); // secondLevelIdx
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::VarBinaryType;

    const JAVA_BITMAP: &str = "AQIAAAAAAAAAAAIAAwAAAAAAAQAEAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAQAAAQAAAAAAAAABAAAA/gIAAAABAgEAAAABAAECAQAAAAEAAgAAAAAAAAAAAAAAAQAAAA==";

    #[test]
    fn unions_java_roaring64_bytes_and_roundtrips_java_format() {
        let java = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, JAVA_BITMAP)
            .unwrap();
        let parsed = decode(&java).unwrap();
        assert_eq!(parsed, BTreeSet::from([1, 4294967297]));
        let extra = encode(&BTreeSet::from([3, 4, 1u64 << 40])).unwrap();
        let input = BinaryArray::from(vec![Some(java.as_slice()), Some(extra.as_slice())]);
        let mut agg = Roaring64Agg::new(
            "bitmap",
            &DataType::VarBinary(VarBinaryType::new(64).unwrap()),
        )
        .unwrap();
        agg.agg(&input, 0).unwrap();
        agg.agg(&input, 1).unwrap();
        let result = agg.result().unwrap();
        let result = result.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(
            decode(result.value(0)).unwrap(),
            BTreeSet::from([1, 3, 4, 4294967297, 1u64 << 40])
        );
    }

    #[test]
    fn rejects_truncated_java_data() {
        let java = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, JAVA_BITMAP)
            .unwrap();
        assert!(decode(&java[..java.len() - 1]).is_err());
    }
}
