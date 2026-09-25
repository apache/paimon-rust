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

//! Apache DataSketches HLL union, matching Java `FieldHllSketchAgg`.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BinaryArray};
use datasketches::error::{Error as SketchError, ErrorKind};
use datasketches::hll::{HllSketch, HllType, HllUnion};

use super::{unsupported_type_error, FieldAggregator};
use crate::spec::DataType;
use crate::Error;

#[derive(Debug)]
pub(crate) struct HllSketchAgg {
    field_name: String,
    value: Option<Vec<u8>>,
}

impl HllSketchAgg {
    pub(crate) fn new(field_name: &str, data_type: &DataType) -> crate::Result<Self> {
        if !matches!(data_type, DataType::VarBinary(_)) {
            return Err(unsupported_type_error("hll_sketch", field_name, data_type));
        }
        Ok(Self {
            field_name: field_name.to_string(),
            value: None,
        })
    }
}

fn deserialize_for_union(bytes: &[u8]) -> Result<HllSketch, SketchError> {
    // datasketches-rs 0.2 reads HLL_4 auxiliary coupons as a packed list.
    // Java's updatable form instead stores them in a sparse hash table. It
    // also skips the registers of compact HLL arrays. Normalize both forms
    // before handing them to the Rust reader.
    if bytes.len() >= 40 && bytes[2] == 7 && bytes[7] & 3 == 2 {
        let lg_k = bytes[3];
        if (4..=21).contains(&lg_k) {
            let k = 1usize << lg_k;
            let hll_type = bytes[7] >> 2;
            let register_len = match hll_type {
                0 => k / 2,
                1 => k * 3 / 4,
                2 => k,
                _ => 0,
            };
            let aux_count = u32::from_le_bytes(bytes[36..40].try_into().unwrap()) as usize;
            if register_len > 0 {
                let compact = bytes[5] & 8 != 0;
                let payload_start = 40 + register_len;
                let slots = if !compact && hll_type == 0 && aux_count > 0 {
                    1usize.checked_shl(bytes[4] as u32).ok_or_else(|| {
                        SketchError::new(
                            ErrorKind::InvalidData,
                            "invalid HLL_4 auxiliary table size",
                        )
                    })?
                } else {
                    aux_count
                };
                let payload_end = slots
                    .checked_mul(4)
                    .and_then(|len| payload_start.checked_add(len));
                if payload_end.is_none_or(|end| bytes.len() < end) {
                    return Err(SketchError::new(
                        ErrorKind::InvalidData,
                        "HLL array ends before its registers or auxiliary entries",
                    ));
                }
                if !compact && hll_type == 0 && aux_count > 0 {
                    let mut normalized = bytes[..payload_start].to_vec();
                    let mut found = 0;
                    for coupon in bytes[payload_start..payload_end.unwrap()]
                        .as_chunks::<4>()
                        .0
                    {
                        if *coupon != [0; 4] {
                            normalized.extend_from_slice(coupon);
                            found += 1;
                        }
                    }
                    if found != aux_count {
                        return Err(SketchError::new(
                            ErrorKind::InvalidData,
                            "HLL_4 auxiliary table count does not match coupons",
                        ));
                    }
                    return HllSketch::deserialize(&normalized);
                }
                if !compact {
                    return HllSketch::deserialize(bytes);
                }
                let mut expanded = bytes.to_vec();
                expanded[5] &= !8;
                // The Rust HLL_6 reader requests one extra byte for a safe
                // packed-register window at the end of the array.
                if hll_type == 1 && expanded.len() == payload_start {
                    expanded.push(0);
                }
                return HllSketch::deserialize(&expanded);
            }
        }
    }
    // datasketches-rs 0.2 deserializes a compact LIST into a backing array of
    // exactly coupon_count slots. A subsequent union silently drops every new
    // coupon because List::update sees no empty slot. Expand only that mode to
    // the Java noncompact representation before deserializing; SET already
    // allocates its full backing array.
    if bytes.len() >= 8 && bytes[2] == 7 && bytes[7] & 3 == 0 && bytes[5] & 8 != 0 {
        let lg_arr = bytes[4] as u32;
        let count = bytes[6] as usize;
        if lg_arr == 3 {
            let capacity = 1usize << lg_arr;
            if capacity >= count && bytes.len() >= 8 + 4 * count {
                let mut expanded = bytes[..8 + 4 * count].to_vec();
                expanded[5] &= !8;
                expanded.resize(8 + 4 * capacity, 0);
                return HllSketch::deserialize(&expanded);
            }
        }
    }
    HllSketch::deserialize(bytes)
}

impl FieldAggregator for HllSketchAgg {
    fn name(&self) -> &'static str {
        "hll_sketch"
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
                message: format!(
                    "hll_sketch column '{}' requires Arrow Binary, got {:?}",
                    self.field_name,
                    array.data_type()
                ),
                source: None,
            })?
            .value(row_idx);
        let Some(accumulator) = self.value.as_ref() else {
            self.value = Some(bytes.to_vec());
            return Ok(());
        };
        let left = deserialize_for_union(accumulator).map_err(|e| Error::DataInvalid {
            message: format!(
                "Invalid hll_sketch accumulator for '{}': {e}",
                self.field_name
            ),
            source: Some(Box::new(e)),
        })?;
        let right = deserialize_for_union(bytes).map_err(|e| Error::DataInvalid {
            message: format!("Invalid hll_sketch input for '{}': {e}", self.field_name),
            source: Some(Box::new(e)),
        })?;
        let mut union = HllUnion::new(left.lg_config_k().max(right.lg_config_k()));
        union.update(&right);
        union.update(&left);
        let mut result = union.get_result(HllType::Hll4).serialize();
        // datasketches-rs serializes HLL_4 auxiliary coupons consecutively,
        // but leaves the header marked updatable. Java and our next union
        // would then interpret those coupons as a sparse hash table. This is
        // the compact HLL_4 layout, so mark it as such when emitting it.
        if result.len() >= 40 && result[2] == 7 && result[7] == 2 {
            result[5] |= 8;
        }
        self.value = Some(result);
        Ok(())
    }

    fn agg_reversed(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        self.agg(array, row_idx)
    }

    fn result(&self) -> crate::Result<ArrayRef> {
        Ok(Arc::new(BinaryArray::from(vec![self.value.as_deref()])))
    }
}

const THETA_MAX: u64 = i64::MAX as u64;
const THETA_NOMINAL_ENTRIES: usize = 4096;

#[derive(Debug)]
struct CompactTheta {
    theta: u64,
    entries: BTreeSet<u64>,
    empty: bool,
}

fn invalid_theta(message: impl Into<String>) -> Error {
    Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

fn parse_theta(bytes: &[u8]) -> crate::Result<CompactTheta> {
    if bytes.len() < 8 || bytes[1] != 3 || !matches!(bytes[2], 2 | 3) {
        return Err(invalid_theta("Expected Apache DataSketches Theta v3 bytes"));
    }
    let compact = bytes[2] == 3 && bytes[5] & 0x10 != 0;
    if bytes[2] == 3 && !compact {
        return Err(invalid_theta("Invalid compact Theta flags"));
    }
    let preamble_longs = (bytes[0] & 0x3f) as usize;
    if !(1..=3).contains(&preamble_longs) {
        return Err(invalid_theta("Invalid compact Theta preamble length"));
    }
    let header_len = preamble_longs * 8;
    if bytes.len() < header_len {
        return Err(invalid_theta("Compact Theta bytes end inside the preamble"));
    }
    let empty = bytes[5] & 0x04 != 0;
    if !empty && bytes[6..8] != [0xcc, 0x93] {
        return Err(invalid_theta(
            "Compact Theta seed hash does not match Java default",
        ));
    }
    let count = match preamble_longs {
        1 if empty => 0,
        1 => 1,
        _ => u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize,
    };
    let slots = if compact {
        count
    } else {
        1usize
            .checked_shl(bytes[4] as u32)
            .filter(|slots| *slots <= 1 << 26)
            .ok_or_else(|| invalid_theta("Invalid Theta update hash table size"))?
    };
    let payload_len = slots
        .checked_mul(8)
        .and_then(|n| header_len.checked_add(n))
        .ok_or_else(|| invalid_theta("Theta entry count overflows"))?;
    if bytes.len() < payload_len {
        return Err(invalid_theta("Theta bytes end before retained entries"));
    }
    let theta = if preamble_longs == 3 {
        u64::from_le_bytes(bytes[16..24].try_into().unwrap())
    } else {
        THETA_MAX
    };
    let mut entries = BTreeSet::new();
    for chunk in bytes[header_len..payload_len].as_chunks::<8>().0 {
        let hash = u64::from_le_bytes(*chunk);
        if !compact && hash == 0 {
            continue;
        }
        if hash == 0 || hash >= theta {
            return Err(invalid_theta(
                "Compact Theta retained hash is outside theta",
            ));
        }
        entries.insert(hash);
    }
    if !compact && entries.len() != count {
        return Err(invalid_theta(
            "Theta update retained count differs from hash table",
        ));
    }
    Ok(CompactTheta {
        theta,
        entries,
        empty,
    })
}

fn union_theta(left: &[u8], right: &[u8]) -> crate::Result<Vec<u8>> {
    let mut left = parse_theta(left)?;
    let right = parse_theta(right)?;
    left.theta = left.theta.min(right.theta);
    left.entries.extend(right.entries);
    left.entries.retain(|hash| *hash < left.theta);
    left.empty &= right.empty;

    if left.entries.len() > THETA_NOMINAL_ENTRIES {
        let threshold = *left.entries.iter().nth(THETA_NOMINAL_ENTRIES).unwrap();
        left.theta = left.theta.min(threshold);
        left.entries.retain(|hash| *hash < left.theta);
    }
    let count = u32::try_from(left.entries.len())
        .map_err(|_| invalid_theta("Compact Theta retained entry count exceeds u32"))?;
    if left.empty {
        return Ok(vec![1, 3, 3, 0, 0, 0x1e, 0, 0]);
    }
    let single = left.theta == THETA_MAX && count == 1;
    let preamble_longs = if single {
        1
    } else if left.theta == THETA_MAX {
        2
    } else {
        3
    };
    let mut result = vec![
        preamble_longs,
        3,
        3,
        0,
        0,
        if single { 0x3a } else { 0x1a },
        0xcc,
        0x93,
    ];
    if preamble_longs > 1 {
        result.extend_from_slice(&count.to_le_bytes());
        result.extend_from_slice(&1.0f32.to_le_bytes());
    }
    if preamble_longs == 3 {
        result.extend_from_slice(&left.theta.to_le_bytes());
    }
    for hash in left.entries {
        result.extend_from_slice(&hash.to_le_bytes());
    }
    Ok(result)
}

/// Union of Java compact and update Theta sketches. The field stores the interoperable
/// DataSketches v3 byte representation, not an approximate cardinality.
#[derive(Debug)]
pub(crate) struct ThetaSketchAgg {
    field_name: String,
    value: Option<Vec<u8>>,
}

impl ThetaSketchAgg {
    pub(crate) fn new(field_name: &str, data_type: &DataType) -> crate::Result<Self> {
        if !matches!(data_type, DataType::VarBinary(_)) {
            return Err(unsupported_type_error(
                "theta_sketch",
                field_name,
                data_type,
            ));
        }
        Ok(Self {
            field_name: field_name.to_string(),
            value: None,
        })
    }
}

impl FieldAggregator for ThetaSketchAgg {
    fn name(&self) -> &'static str {
        "theta_sketch"
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
                message: format!(
                    "theta_sketch column '{}' requires Arrow Binary, got {:?}",
                    self.field_name,
                    array.data_type()
                ),
                source: None,
            })?
            .value(row_idx);
        self.value = Some(match self.value.as_ref() {
            None => bytes.to_vec(),
            Some(accumulator) => union_theta(accumulator, bytes)?,
        });
        Ok(())
    }

    fn agg_reversed(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        self.agg(array, row_idx)
    }

    fn result(&self) -> crate::Result<ArrayRef> {
        Ok(Arc::new(BinaryArray::from(vec![self.value.as_deref()])))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::VarBinaryType;

    const JAVA_A: &str = "AgEHDAMIAgAr8vsGhi/5DQ==";
    const JAVA_B: &str = "AgEHDAMIAgCGL/kNdYFmBw==";
    const JAVA_UNION: &str = "AgEHDAMIAwCGL/kNdYFmByvy+wY=";
    const JAVA_THETA_A: &str = "AgMDAAAazJMCAAAAAACAPxX5fcu9hqEFw5f8EoFwnR4=";
    const JAVA_THETA_B: &str = "AgMDAAAazJMCAAAAAACAP8OX/BKBcJ0eukCzwdoGaV0=";
    const JAVA_THETA_UNION: &str = "AgMDAAAazJMDAAAAAACAPxX5fcu9hqEFw5f8EoFwnR66QLPB2gZpXQ==";
    // Java DataSketches 4.2.0 UpdateSketch#toByteArray with nominal entries 16.
    const JAVA_THETA_UPDATE_A: &str = "wwMCBAUAzJMCAAAAAACAP/////////9/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAw5f8EoFwnR4AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFfl9y72GoQUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==";
    const JAVA_HLL_SET_A: &str = "AwEHDAYIAAEeAAAAgbxdBsPdUQTEtZ8Hhi/5Dch6JATL18IEfHS5B87wWx/SFnMHWX/UDTWpMQTbUi0EnuSbGK48iBEiO+sF7y33B8HpFwUr8vsGxhlqBG7FNAZGSrcEsFtGEjSiYQ51gWYHNkcJB7g/+Qe4VqkMe2XmCPwtQgr2cfIG";
    const JAVA_HLL_SET_B: &str = "AwEHDAYIAAEeAAAAAiK0BMPdUQTEtZ8HxhlqBMh6JASNxIkJzvBbH4/DsAbOoO8FNkcJB5QHwgSXu2AaWX/UDRq80AXbUi0Eni1qByI76wWTVDEFqnOHFoRpzAVt5R0HbsU0Bu8t9wee5JsYMiViBTWpMQS2LwkGuD/5B7hWqQzXeDQG";
    const JAVA_HLL_SET_UNION: &str = "AwEHDAYIAAEtAAAAgbxdBgIitATD3VEExLWfB4Yv+Q3GGWoEyHokBLhWqQzL18IEjcSJCc7wWx+Pw7AG/C1CCs6g7wXSFnMHk1QxBZQHwgSeLWoHl7tgGkZKtwRZf9QNGrzQBdtSLQSe5JsY9nHyBq48iBEiO+sFwekXBapzhxaEacwFbeUdB27FNAbvLfcHsFtGEjIlYgU0omEOti8JBjWpMQQ2RwkHuD/5B9d4NAZ7ZeYIfHS5Byvy+wZ1gWYH";

    #[test]
    fn unions_java_updatable_hll4_with_sparse_auxiliary_table() {
        // Java DataSketches 4.2.0, HLL_4 lgK=16, integers 0..200000.
        // Its updatable form has 12 coupons in 256 sparse auxiliary slots.
        let compact = include_bytes!("../goldens/hll_java_compact_aux.bin");
        let updatable = include_bytes!("../goldens/hll_java_updatable_aux.bin");
        for bytes in [compact.as_slice(), updatable.as_slice()] {
            let input = BinaryArray::from(vec![Some(bytes), Some(bytes), Some(bytes)]);
            let mut agg = HllSketchAgg::new(
                "sketch",
                &DataType::VarBinary(VarBinaryType::new(65535).unwrap()),
            )
            .unwrap();
            agg.agg(&input, 0).unwrap();
            agg.agg(&input, 1).unwrap();
            let second = agg.result().unwrap();
            let second = second.as_any().downcast_ref::<BinaryArray>().unwrap();
            assert_eq!(second.value(0)[5] & 8, 8, "HLL_4 output must be compact");
            agg.agg(&input, 2).unwrap();
            let output = agg.result().unwrap();
            let output = output.as_any().downcast_ref::<BinaryArray>().unwrap();
            let estimate = deserialize_for_union(output.value(0)).unwrap().estimate();
            assert!((estimate - 200552.41133627715).abs() < 1e-6, "{estimate}");
        }
    }

    #[test]
    fn unions_java_compact_dense_hll_sketches() {
        // Java DataSketches 4.2.0 HLL_4/6/8: integers 0..10000 and
        // 5000..15000, serialized with toCompactByteArray(). Java Union(12)
        // estimates 15148.816386062443 for each pair.
        let fixtures: &[(&[u8], &[u8])] = &[
            (
                include_bytes!("../goldens/hll_java_dense_a.bin"),
                include_bytes!("../goldens/hll_java_dense_b.bin"),
            ),
            (
                include_bytes!("../goldens/hll_java_dense6_a.bin"),
                include_bytes!("../goldens/hll_java_dense6_b.bin"),
            ),
            (
                include_bytes!("../goldens/hll_java_dense8_a.bin"),
                include_bytes!("../goldens/hll_java_dense8_b.bin"),
            ),
        ];
        for (a, b) in fixtures {
            let input = BinaryArray::from(vec![Some(*a), Some(*b)]);
            let mut agg = HllSketchAgg::new(
                "sketch",
                &DataType::VarBinary(VarBinaryType::new(8192).unwrap()),
            )
            .unwrap();
            agg.agg(&input, 0).unwrap();
            agg.agg(&input, 1).unwrap();
            let result = agg.result().unwrap();
            let result = result.as_any().downcast_ref::<BinaryArray>().unwrap();
            let estimate = deserialize_for_union(result.value(0)).unwrap().estimate();
            assert!((estimate - 15148.816386062443).abs() < 1e-9);
        }
    }

    #[test]
    fn rejects_truncated_compact_dense_hll() {
        let valid = include_bytes!("../goldens/hll_java_dense_a.bin");
        let truncated = &valid[..valid.len() - 1];
        let input = BinaryArray::from(vec![Some(valid.as_slice()), Some(truncated)]);
        let mut agg = HllSketchAgg::new(
            "sketch",
            &DataType::VarBinary(VarBinaryType::new(4096).unwrap()),
        )
        .unwrap();
        agg.agg(&input, 0).unwrap();
        let err = agg.agg(&input, 1).unwrap_err();
        assert!(err.to_string().contains("ends before its registers"));
    }

    #[test]
    fn unions_java_hll_sketches() {
        let decode = |encoded| {
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded).unwrap()
        };
        let a = decode(JAVA_A);
        let b = decode(JAVA_B);
        let input = BinaryArray::from(vec![Some(a.as_slice()), Some(b.as_slice())]);
        let mut agg = HllSketchAgg::new(
            "sketch",
            &DataType::VarBinary(VarBinaryType::new(64).unwrap()),
        )
        .unwrap();
        agg.agg(&input, 0).unwrap();
        agg.agg(&input, 1).unwrap();
        let result = agg.result().unwrap();
        let result = result.as_any().downcast_ref::<BinaryArray>().unwrap();
        let expected = decode(JAVA_UNION);
        let actual = HllSketch::deserialize(result.value(0)).unwrap();
        let java = HllSketch::deserialize(&expected).unwrap();
        assert_eq!(actual.estimate().round(), 3.0);
        assert_eq!(actual.estimate(), java.estimate());
    }

    #[test]
    fn unions_java_set_mode_hll_sketches() {
        let decode = |encoded| {
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded).unwrap()
        };
        let a = decode(JAVA_HLL_SET_A);
        let b = decode(JAVA_HLL_SET_B);
        let input = BinaryArray::from(vec![Some(a.as_slice()), Some(b.as_slice())]);
        let mut agg = HllSketchAgg::new(
            "sketch",
            &DataType::VarBinary(VarBinaryType::new(64).unwrap()),
        )
        .unwrap();
        agg.agg(&input, 0).unwrap();
        agg.agg(&input, 1).unwrap();
        let result = agg.result().unwrap();
        let result = result.as_any().downcast_ref::<BinaryArray>().unwrap();
        let expected = decode(JAVA_HLL_SET_UNION);
        let actual = HllSketch::deserialize(result.value(0)).unwrap();
        let java = HllSketch::deserialize(&expected).unwrap();
        assert_eq!(actual.estimate(), java.estimate());
    }

    #[test]
    fn unions_java_compact_theta_sketches() {
        let decode = |encoded| {
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded).unwrap()
        };
        let a = decode(JAVA_THETA_A);
        let b = decode(JAVA_THETA_B);
        let input = BinaryArray::from(vec![Some(a.as_slice()), Some(b.as_slice())]);
        let mut agg = ThetaSketchAgg::new(
            "sketch",
            &DataType::VarBinary(VarBinaryType::new(64).unwrap()),
        )
        .unwrap();
        agg.agg(&input, 0).unwrap();
        agg.agg(&input, 1).unwrap();
        let result = agg.result().unwrap();
        let result = result.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(result.value(0), decode(JAVA_THETA_UNION));
    }

    #[test]
    fn unions_java_update_theta_sketches() {
        let decode = |encoded| {
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded).unwrap()
        };
        let a = decode(JAVA_THETA_UPDATE_A);
        let b = decode(JAVA_THETA_B);
        let input = BinaryArray::from(vec![Some(a.as_slice()), Some(b.as_slice())]);
        let mut agg = ThetaSketchAgg::new(
            "sketch",
            &DataType::VarBinary(VarBinaryType::new(4096).unwrap()),
        )
        .unwrap();
        agg.agg(&input, 0).unwrap();
        agg.agg(&input, 1).unwrap();
        let result = agg.result().unwrap();
        let result = result.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(
            parse_theta(result.value(0)).unwrap().entries,
            parse_theta(&decode(JAVA_THETA_UNION)).unwrap().entries
        );
    }

    #[test]
    fn compact_theta_rejects_truncated_preamble() {
        let err = parse_theta(&[3, 3, 3, 0, 0, 0x1a, 0xcc, 0x93]).unwrap_err();
        assert!(matches!(err, Error::DataInvalid { message, .. } if message.contains("preamble")));
    }
}
