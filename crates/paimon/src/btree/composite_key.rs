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

//! Java `CompositeKeySerializer` / `RowCompactedSerializer` tuple encoding.

use super::key_serde::{fixed_key_bytes, make_key_comparator, serialize_datum, KeyComparator};
use super::var_len::{encode_var_int, try_decode_var_int_from_slice};
use crate::spec::{DataField, DataType, Datum};
use crate::{Error, Result};
use std::cmp::Ordering;

pub(crate) struct CompositeKeyCodec {
    types: Vec<DataType>,
    comparators: Vec<KeyComparator>,
}

/// Read fields directly from a compacted row slice, without materializing a row
/// or allocating a component list. The cursor is local to each comparison.
pub(crate) struct CompositeKeyCursor<'codec, 'key> {
    codec: &'codec CompositeKeyCodec,
    key: &'key [u8],
    position: usize,
    offset: usize,
}

fn invalid(message: &str) -> Error {
    Error::DataInvalid {
        message: format!("Invalid composite BTree key: {message}"),
        source: None,
    }
}

/// Serialize a tuple in index-column order, including NULL components. A tuple
/// containing only NULL components is still a key, not a scalar NULL posting.
pub fn serialize_composite_key(values: &[Option<Datum>], fields: &[DataField]) -> Result<Vec<u8>> {
    CompositeKeyCodec::new(fields).serialize(values)
}

impl CompositeKeyCodec {
    pub(crate) fn new(fields: &[DataField]) -> Self {
        Self {
            types: fields.iter().map(|f| f.data_type().clone()).collect(),
            comparators: fields
                .iter()
                .map(|f| make_key_comparator(f.data_type()))
                .collect(),
        }
    }

    pub(crate) fn serialize(&self, values: &[Option<Datum>]) -> Result<Vec<u8>> {
        if values.len() != self.types.len() || values.is_empty() {
            return Err(invalid("tuple arity differs from index fields"));
        }
        // One RowKind byte (INSERT = 0), then packed NULL bits.
        let mut bytes = vec![0; 1 + values.len().div_ceil(8)];
        for (i, (value, ty)) in values.iter().zip(&self.types).enumerate() {
            if !supported_type(ty) {
                return Err(invalid("unsupported non-scalar component type"));
            }
            match value {
                None => bytes[1 + i / 8] |= 1 << (i % 8),
                Some(value) => {
                    crate::spec::validate_datum_matches_type(value, ty)?;
                    let component = self.serialize_component(i, value);
                    if variable_width(ty) {
                        let len = i32::try_from(component.len())
                            .map_err(|_| invalid("component too large"))?;
                        encode_var_int(&mut bytes, len).expect("writing to Vec");
                    }
                    bytes.extend(component);
                }
            }
        }
        // Also validate the input types and complete layout before writing it.
        self.validate_key(&bytes)?;
        Ok(bytes)
    }

    pub(crate) fn serialize_component(&self, i: usize, value: &Datum) -> Vec<u8> {
        // Java Float/Double.compare treats every NaN as equal. Bloom hashing
        // therefore uses the canonical positive NaN payload for each component.
        match value {
            Datum::Float(v) if v.is_nan() => 0x7fc00000u32.to_le_bytes().to_vec(),
            Datum::Double(v) if v.is_nan() => 0x7ff8000000000000u64.to_le_bytes().to_vec(),
            _ => serialize_datum(value, &self.types[i]),
        }
    }

    pub(crate) fn cursor<'a>(&self, key: &'a [u8]) -> Result<CompositeKeyCursor<'_, 'a>> {
        let offset = 1 + self.types.len().div_ceil(8);
        if key.len() < offset {
            return Err(invalid("truncated header"));
        }
        Ok(CompositeKeyCursor {
            codec: self,
            key,
            position: 0,
            offset,
        })
    }

    pub(crate) fn validate_key(&self, key: &[u8]) -> Result<()> {
        self.cursor(key)?.finish()
    }

    pub(crate) fn compare_component(
        &self,
        i: usize,
        a: Option<&[u8]>,
        b: Option<&[u8]>,
    ) -> Result<Ordering> {
        match (a, b) {
            (None, None) => Ok(Ordering::Equal),
            (None, Some(_)) => Ok(Ordering::Less),
            (Some(_), None) => Ok(Ordering::Greater),
            (Some(a), Some(b)) => match &self.types[i] {
                DataType::Float(_) => {
                    let a = f32::from_le_bytes(fixed_key_bytes::<4>(a, "FLOAT")?);
                    let b = f32::from_le_bytes(fixed_key_bytes::<4>(b, "FLOAT")?);
                    Ok(
                        (if a.is_nan() { f32::NAN } else { a }).total_cmp(&if b.is_nan() {
                            f32::NAN
                        } else {
                            b
                        }),
                    )
                }
                DataType::Double(_) => {
                    let a = f64::from_le_bytes(fixed_key_bytes::<8>(a, "DOUBLE")?);
                    let b = f64::from_le_bytes(fixed_key_bytes::<8>(b, "DOUBLE")?);
                    Ok(
                        (if a.is_nan() { f64::NAN } else { a }).total_cmp(&if b.is_nan() {
                            f64::NAN
                        } else {
                            b
                        }),
                    )
                }
                _ => (self.comparators[i])(a, b),
            },
        }
    }

    pub(crate) fn compare_keys(&self, a: &[u8], b: &[u8]) -> Result<Ordering> {
        let mut a = self.cursor(a)?;
        let mut b = self.cursor(b)?;
        let mut order = Ordering::Equal;
        for i in 0..self.types.len() {
            order = self.compare_component(i, a.next_component()?, b.next_component()?)?;
            if order != Ordering::Equal {
                break;
            }
        }
        a.finish()?;
        b.finish()?;
        Ok(order)
    }
}

impl<'key> CompositeKeyCursor<'_, 'key> {
    pub(crate) fn next_component(&mut self) -> Result<Option<&'key [u8]>> {
        let i = self.position;
        let ty = self
            .codec
            .types
            .get(i)
            .ok_or_else(|| invalid("read past tuple arity"))?;
        self.position += 1;
        if self.key[1 + i / 8] & (1 << (i % 8)) != 0 {
            return Ok(None);
        }
        let len = if variable_width(ty) {
            let (len, consumed) = try_decode_var_int_from_slice(self.key, self.offset)?;
            self.offset += consumed;
            usize::try_from(len).map_err(|_| invalid("negative component length"))?
        } else {
            match ty {
                DataType::Boolean(_) | DataType::TinyInt(_) => 1,
                DataType::SmallInt(_) => 2,
                DataType::Int(_) | DataType::Date(_) | DataType::Time(_) | DataType::Float(_) => 4,
                DataType::BigInt(_) | DataType::Double(_) | DataType::Decimal(_) => 8,
                DataType::Timestamp(t) if t.precision() > 3 => {
                    timestamp_len(self.key, self.offset)?
                }
                DataType::LocalZonedTimestamp(t) if t.precision() > 3 => {
                    timestamp_len(self.key, self.offset)?
                }
                DataType::Timestamp(_) | DataType::LocalZonedTimestamp(_) => 8,
                _ => return Err(invalid("unsupported non-scalar component type")),
            }
        };
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| invalid("component length overflow"))?;
        let component = self
            .key
            .get(self.offset..end)
            .ok_or_else(|| invalid("truncated component"))?;
        self.codec
            .compare_component(i, Some(component), Some(component))?;
        self.offset = end;
        Ok(Some(component))
    }

    pub(crate) fn finish(mut self) -> Result<()> {
        // Preserve validation of suffix fields even when an earlier field has
        // already decided the order, so incompatible keys still trigger fallback.
        while self.position < self.codec.types.len() {
            self.next_component()?;
        }
        if self.offset != self.key.len() {
            return Err(invalid("trailing bytes or incompatible schema"));
        }
        Ok(())
    }
}

fn supported_type(ty: &DataType) -> bool {
    variable_width(ty)
        || matches!(
            ty,
            DataType::Boolean(_)
                | DataType::TinyInt(_)
                | DataType::SmallInt(_)
                | DataType::Int(_)
                | DataType::BigInt(_)
                | DataType::Float(_)
                | DataType::Double(_)
                | DataType::Date(_)
                | DataType::Time(_)
                | DataType::Decimal(_)
                | DataType::Timestamp(_)
                | DataType::LocalZonedTimestamp(_)
        )
}

fn variable_width(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Char(_) | DataType::VarChar(_) | DataType::Binary(_) | DataType::VarBinary(_)
    ) || matches!(ty, DataType::Decimal(d) if d.precision() > 18)
}

fn timestamp_len(key: &[u8], offset: usize) -> Result<usize> {
    let start = offset
        .checked_add(8)
        .ok_or_else(|| invalid("timestamp overflow"))?;
    let (nanos, consumed) = try_decode_var_int_from_slice(key, start)?;
    if !(0..1_000_000).contains(&nanos) {
        return Err(invalid("invalid timestamp nanoseconds"));
    }
    Ok(8 + consumed)
}
