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

use std::borrow::Cow;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
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
        for (key, container) in decode(bytes).map_err(|message| Error::DataInvalid {
            message: format!("Invalid rbm64 input for '{}': {message}", self.field_name),
            source: None,
        })? {
            match values.entry(key) {
                Entry::Vacant(vacant) => {
                    vacant.insert(container);
                }
                Entry::Occupied(mut occupied) => occupied.get_mut().union(container),
            }
        }
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

type Ranges = BTreeMap<[u8; 6], Container>;

/// One decoded 16-bit container. Dense bitset containers (Java kind 1)
/// stay as their 1024 words so unions are word-wise ORs, like Java's
/// bitmap container; every other shape is a normalized inclusive range
/// list.
#[derive(Clone, Debug)]
#[cfg_attr(test, derive(PartialEq))]
enum Container {
    Ranges(Vec<(u16, u16)>),
    Words(Box<[u64; 1024]>),
}

impl Container {
    fn cardinality(&self) -> usize {
        match self {
            Container::Ranges(ranges) => ranges
                .iter()
                .map(|(start, end)| usize::from(*end) - usize::from(*start) + 1)
                .sum(),
            Container::Words(words) => words.iter().map(|word| word.count_ones() as usize).sum(),
        }
    }

    /// Number of maximal runs of set bits, i.e. `self.ranges().len()`.
    fn run_count(&self) -> usize {
        match self {
            Container::Ranges(ranges) => ranges.len(),
            Container::Words(words) => {
                // A run starts at each set bit whose lower neighbor is
                // clear; bit 0 looks at bit 63 of the previous word.
                let mut carry = 0;
                let mut runs = 0;
                for &word in words.iter() {
                    runs += (word & !((word << 1) | carry)).count_ones() as usize;
                    carry = word >> 63;
                }
                runs
            }
        }
    }

    /// Normalized inclusive ranges, borrowed for range containers.
    fn ranges(&self) -> Cow<'_, [(u16, u16)]> {
        match self {
            Container::Ranges(ranges) => Cow::Borrowed(ranges),
            Container::Words(words) => Cow::Owned(word_ranges(words)),
        }
    }

    /// OR `other` into `self`. A bitset on either side makes the result a
    /// bitset; two range lists stay a normalized range list.
    fn union(&mut self, other: Container) {
        match (&mut *self, other) {
            (Container::Ranges(own), Container::Ranges(input)) => {
                own.extend(input);
                normalize_ranges(own);
            }
            (Container::Words(own), Container::Ranges(input)) => set_ranges(own, &input),
            (Container::Words(own), Container::Words(input)) => {
                for (own, input) in own.iter_mut().zip(input.iter()) {
                    *own |= input;
                }
            }
            (Container::Ranges(own), Container::Words(mut input)) => {
                set_ranges(&mut input, own);
                *self = Container::Words(input);
            }
        }
    }
}

/// Maximal runs of set bits as inclusive ranges, merged across words.
fn word_ranges(words: &[u64; 1024]) -> Vec<(u16, u16)> {
    let mut ranges: Vec<(u16, u16)> = Vec::new();
    for (index, &word) in words.iter().enumerate() {
        let mut remaining = word;
        while remaining != 0 {
            let low = remaining.trailing_zeros();
            let len = (remaining >> low).trailing_ones();
            let start = (index * 64) as u16 + low as u16;
            let end = start + (len - 1) as u16;
            match ranges.last_mut() {
                Some(last) if u32::from(last.1) + 1 == u32::from(start) => last.1 = end,
                _ => ranges.push((start, end)),
            }
            // `len` can be 64, so shift MAX down rather than `1 << len`.
            remaining &= !((u64::MAX >> (64 - len)) << low);
        }
    }
    ranges
}

/// Set the bits of inclusive `ranges` a word at a time.
fn set_ranges(words: &mut [u64; 1024], ranges: &[(u16, u16)]) {
    for &(start, end) in ranges {
        let (first, last) = (usize::from(start / 64), usize::from(end / 64));
        let head = u64::MAX << (start % 64);
        let tail = u64::MAX >> (63 - end % 64);
        if first == last {
            words[first] |= head & tail;
        } else {
            words[first] |= head;
            words[first + 1..last].fill(u64::MAX);
            words[last] |= tail;
        }
    }
}

fn normalize_ranges(ranges: &mut Vec<(u16, u16)>) {
    ranges.sort_unstable();
    let mut write = 0;
    for read in 0..ranges.len() {
        if write > 0 && u32::from(ranges[read].0) <= u32::from(ranges[write - 1].1) + 1 {
            ranges[write - 1].1 = ranges[write - 1].1.max(ranges[read].1);
        } else {
            ranges[write] = ranges[read];
            write += 1;
        }
    }
    ranges.truncate(write);
}

fn decode(bytes: &[u8]) -> Result<Ranges, String> {
    let mut reader = Reader { bytes, offset: 0 };
    match reader.u8()? {
        0 if reader.offset == bytes.len() => return Ok(BTreeMap::new()),
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
            let container = match kind {
                0 => {
                    let runs = usize::from(reader.u16()?);
                    let mut ranges = Vec::with_capacity(runs);
                    for _ in 0..runs {
                        let start = reader.u16()?;
                        let length = reader.u16()?;
                        let end = u32::from(start) + u32::from(length);
                        if end >= 65536 {
                            return Err("invalid rbm64 run container".into());
                        }
                        ranges.push((start, end as u16));
                    }
                    normalize_ranges(&mut ranges);
                    Container::Ranges(ranges)
                }
                // Dense bitsets stay as words: unions become word-wise
                // ORs and the encode path writes the words back without
                // per-bit expansion, like Java's bitmap container.
                1 => {
                    let mut words = Box::new([0u64; 1024]);
                    for word in words.iter_mut() {
                        *word = reader.u64()?;
                    }
                    Container::Words(words)
                }
                2 => {
                    let mut ranges = Vec::with_capacity(cardinality);
                    for _ in 0..cardinality {
                        let low = reader.u16()?;
                        ranges.push((low, low));
                    }
                    normalize_ranges(&mut ranges);
                    Container::Ranges(ranges)
                }
                _ => return Err(format!("unknown rbm64 container type {kind}")),
            };
            if container.cardinality() != cardinality {
                return Err("rbm64 container cardinality mismatch".into());
            }
            inner.push(Some(container));
        }
        containers.push(inner);
    }
    let container_size = reader.u64()?;
    reader.u32()?; // last first-level index
    reader.u32()?; // last second-level index
    if reader.offset != bytes.len() || container_size != key_size {
        return Err("rbm64 trailing bytes or container count mismatch".into());
    }
    let mut result = BTreeMap::new();
    for (high, index) in leaves {
        let first = usize::try_from(index >> 32).map_err(|_| "rbm64 index overflow")?;
        let second = usize::try_from(index as u32).map_err(|_| "rbm64 index overflow")?;
        let lows = containers
            .get(first)
            .and_then(|array| array.get(second))
            .and_then(Option::as_ref)
            .ok_or("ART references missing rbm64 container")?;
        if result.insert(high, lows.clone()).is_some() {
            return Err("duplicate rbm64 ART key".into());
        }
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

fn encode(values: &Ranges) -> crate::Result<Vec<u8>> {
    if values.is_empty() {
        return Ok(vec![0]);
    }
    let keys: Vec<[u8; 6]> = values.keys().copied().collect();
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
    for container in values.values() {
        let cardinality = container.cardinality();
        let array_bytes = cardinality * 2;
        let run_bytes = 2 + container.run_count() * 4;
        let kind = if run_bytes <= array_bytes && run_bytes <= 8192 {
            0
        } else if array_bytes <= 8192 {
            2
        } else {
            1
        };
        out.push(1); // non-null
        out.push(kind);
        out.extend_from_slice(&(cardinality as u32).to_le_bytes());
        match (kind, container) {
            (0, _) => {
                let ranges = container.ranges();
                out.extend_from_slice(&(ranges.len() as u16).to_le_bytes());
                for &(start, end) in ranges.iter() {
                    out.extend_from_slice(&start.to_le_bytes());
                    out.extend_from_slice(&(end - start).to_le_bytes());
                }
            }
            // The dense shape writes its words straight through, without
            // per-bit expansion on either side of the union.
            (1, Container::Words(words)) => {
                for word in words.iter() {
                    out.extend_from_slice(&word.to_le_bytes());
                }
            }
            (1, Container::Ranges(ranges)) => {
                let mut words = [0u64; 1024];
                set_ranges(&mut words, ranges);
                for word in words {
                    out.extend_from_slice(&word.to_le_bytes());
                }
            }
            (_, _) => {
                for &(start, end) in container.ranges().iter() {
                    for low in start..=end {
                        out.extend_from_slice(&low.to_le_bytes());
                    }
                }
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
    use std::collections::BTreeSet;

    const JAVA_BITMAP: &str = "AQIAAAAAAAAAAAIAAwAAAAAAAQAEAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAQAAAQAAAAAAAAABAAAA/gIAAAABAgEAAAABAAECAQAAAAEAAgAAAAAAAAAAAAAAAQAAAA==";

    fn ranges_from_values(values: impl IntoIterator<Item = u64>) -> Ranges {
        let mut result = BTreeMap::<[u8; 6], Vec<(u16, u16)>>::new();
        for value in values {
            let bytes = value.to_be_bytes();
            let low = value as u16;
            result
                .entry(bytes[..6].try_into().unwrap())
                .or_default()
                .push((low, low));
        }
        result
            .into_iter()
            .map(|(key, mut ranges)| {
                normalize_ranges(&mut ranges);
                (key, Container::Ranges(ranges))
            })
            .collect()
    }

    fn values_from_ranges(ranges: &Ranges) -> BTreeSet<u64> {
        ranges
            .iter()
            .flat_map(|(high, container)| {
                let high = high
                    .iter()
                    .fold(0u64, |value, byte| (value << 8) | u64::from(*byte));
                let lows = container.ranges().into_owned();
                let highs = move |low: u16| (high << 16) | u64::from(low);
                lows.into_iter()
                    .flat_map(move |(start, end)| (start..=end).map(highs))
            })
            .collect()
    }

    #[test]
    fn unions_java_roaring64_bytes_and_roundtrips_java_format() {
        let java = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, JAVA_BITMAP)
            .unwrap();
        let parsed = decode(&java).unwrap();
        assert_eq!(values_from_ranges(&parsed), BTreeSet::from([1, 4294967297]));
        let extra = encode(&ranges_from_values([3, 4, 1u64 << 40])).unwrap();
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
            values_from_ranges(&decode(result.value(0)).unwrap()),
            BTreeSet::from([1, 3, 4, 4294967297, 1u64 << 40])
        );
    }

    #[test]
    fn unions_java_dense_run_without_expanding_one_million_values() {
        let dense = include_bytes!("../goldens/roaring64_java_dense.bin");
        assert_eq!(dense.len(), 542);
        let decoded = decode(dense).unwrap();
        assert_eq!(decoded.len(), 16);
        let input = BinaryArray::from(vec![Some(dense.as_slice()), Some(dense.as_slice())]);
        let mut agg = Roaring64Agg::new(
            "bitmap",
            &DataType::VarBinary(VarBinaryType::new(65535).unwrap()),
        )
        .unwrap();
        agg.agg(&input, 0).unwrap();
        agg.agg(&input, 1).unwrap();
        let output = agg.result().unwrap();
        let output = output.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert!(
            output.value(0).len() <= 1024,
            "dense union lost run compression"
        );
        assert_eq!(decode(output.value(0)).unwrap(), decoded);
    }

    #[test]
    fn rejects_truncated_java_data() {
        let java = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, JAVA_BITMAP)
            .unwrap();
        assert!(decode(&java[..java.len() - 1]).is_err());
    }

    const JAVA_BITSET: &[u8] = include_bytes!("../goldens/roaring64_java_bitset.bin");

    fn key(value: u64) -> [u8; 6] {
        value.to_be_bytes()[..6].try_into().unwrap()
    }

    /// Values of `JAVA_BITSET`: two bitset containers and one array container.
    fn java_bitset_values() -> BTreeSet<u64> {
        (0..65536u64)
            .filter(|v| v % 3 == 0)
            .chain((0..65536u64).filter(|v| v % 3 == 1).map(|v| (1 << 40) | v))
            .chain([(1 << 40) | (1 << 16) | 5])
            .collect()
    }

    fn union_rows(rows: &[&[u8]]) -> Vec<u8> {
        let input = BinaryArray::from(rows.iter().map(|row| Some(*row)).collect::<Vec<_>>());
        let mut agg = Roaring64Agg::new(
            "bitmap",
            &DataType::VarBinary(VarBinaryType::new(VarBinaryType::MAX_LENGTH).unwrap()),
        )
        .unwrap();
        for row in 0..rows.len() {
            agg.agg(&input, row).unwrap();
        }
        let output = agg.result().unwrap();
        let output = output.as_any().downcast_ref::<BinaryArray>().unwrap();
        output.value(0).to_vec()
    }

    #[test]
    fn decodes_java_bitset_containers_as_words() {
        let decoded = decode(JAVA_BITSET).unwrap();
        assert!(matches!(decoded[&key(0)], Container::Words(_)));
        assert!(matches!(decoded[&key(1 << 40)], Container::Words(_)));
        assert!(matches!(
            decoded[&key((1 << 40) | (1 << 16))],
            Container::Ranges(_)
        ));
        assert_eq!(values_from_ranges(&decoded), java_bitset_values());
        assert_eq!(decode(&encode(&decoded).unwrap()).unwrap(), decoded);
    }

    /// Bitset with range, range with bitset, bitset with bitset, and a
    /// bitset key that only the input has.
    #[test]
    fn unions_java_bitset_containers_with_ranges_and_bitsets() {
        let sparse_values = [1u64, 2];
        let sparse = encode(&ranges_from_values(sparse_values)).unwrap();
        let rest_values: Vec<u64> = (0..65536u64).filter(|v| v % 3 != 0).collect();
        let rest = encode(&ranges_from_values(rest_values.iter().copied())).unwrap();
        assert!(matches!(
            decode(&sparse).unwrap()[&key(0)],
            Container::Ranges(_)
        ));
        assert!(matches!(
            decode(&rest).unwrap()[&key(0)],
            Container::Words(_)
        ));

        let with_sparse: BTreeSet<u64> = java_bitset_values()
            .into_iter()
            .chain(sparse_values)
            .collect();
        let with_rest: BTreeSet<u64> = java_bitset_values()
            .into_iter()
            .chain(rest_values)
            .collect();
        for (rows, expected) in [
            (vec![JAVA_BITSET, &sparse], &with_sparse),
            (vec![&sparse, JAVA_BITSET], &with_sparse),
            // Key 0 becomes all 65536 values and is written as one run.
            (vec![JAVA_BITSET, &rest], &with_rest),
        ] {
            let output = union_rows(&rows);
            assert_eq!(&values_from_ranges(&decode(&output).unwrap()), expected);
            assert_eq!(
                output,
                encode(&ranges_from_values(expected.iter().copied())).unwrap()
            );
        }
    }

    fn ranges_bit_by_bit(words: &[u64; 1024]) -> Vec<(u16, u16)> {
        let mut ranges: Vec<(u16, u16)> = Vec::new();
        for low in 0..=u16::MAX {
            if (words[usize::from(low / 64)] >> (low % 64)) & 1 == 1 {
                match ranges.last_mut() {
                    Some(last) if last.1 + 1 == low => last.1 = low,
                    _ => ranges.push((low, low)),
                }
            }
        }
        ranges
    }

    #[test]
    fn word_runs_match_a_bit_by_bit_scan() {
        let patterns: [fn(usize) -> u64; 6] = [
            |_| 0xAAAA_AAAA_AAAA_AAAA,
            |_| u64::MAX,
            // 1025 runs: run encoding, while counting per word gives 2048.
            |_| 0xFFFF_0000_0000_FFFF,
            |index| match index {
                0 | 1 => u64::MAX,
                2 => 1,
                _ => 0,
            },
            |index| if index % 2 == 0 { 1 << 63 } else { 1 },
            |index| (index as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15),
        ];
        for pattern in patterns {
            let words = Box::new(std::array::from_fn(pattern));
            let expected = ranges_bit_by_bit(&words);
            let container = Container::Words(words);
            assert_eq!(container.ranges().as_ref(), expected.as_slice());
            assert_eq!(container.run_count(), expected.len());
            let from_words = encode(&BTreeMap::from([(key(0), container)])).unwrap();
            let from_ranges =
                encode(&BTreeMap::from([(key(0), Container::Ranges(expected))])).unwrap();
            assert_eq!(from_words, from_ranges);
        }
    }

    #[test]
    fn set_ranges_matches_a_bit_loop() {
        let ranges = [
            (0, 0),
            (5, 10),
            (60, 70),
            (63, 64),
            (64, 127),
            (200, 1000),
            (65472, 65535),
            (65535, 65535),
            (0, 65535),
        ];
        for range in ranges
            .iter()
            .map(std::slice::from_ref)
            .chain([&ranges[..8]])
        {
            let mut words = [0x5555_5555_5555_5555u64; 1024];
            let mut expected = words;
            set_ranges(&mut words, range);
            for &(start, end) in range {
                for low in start..=end {
                    expected[usize::from(low / 64)] |= 1 << (low % 64);
                }
            }
            assert_eq!(words, expected, "{range:?}");
        }
    }
}
