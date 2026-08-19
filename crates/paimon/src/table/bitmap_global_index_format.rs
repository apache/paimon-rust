// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Shared key and wire-format primitives for Java-compatible bitmap indexes.

use crate::btree::key_serde::KeyComparator;
use crate::btree::{make_key_comparator, serialize_datum};
use crate::spec::{DataType, Datum, PredicateOperator};
use std::cmp::Ordering;

pub(super) const MAGIC: i32 = 0x4247_4958;
pub(super) const VERSION: i32 = 1;
pub(super) const FOOTER_LENGTH: usize = 48;
pub(super) const BLOCK_TRAILER_LENGTH: usize = 5;
const JAVA_CANONICAL_FLOAT_NAN_BITS: u32 = 0x7fc0_0000;
const JAVA_CANONICAL_DOUBLE_NAN_BITS: u64 = 0x7ff8_0000_0000_0000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct BlockInfo {
    pub(super) offset: u64,
    pub(super) length: usize,
}

// Bitmap follows current Java's floating-point key contract. Shared BTree key
// serde intentionally keeps the already-persisted Rust contract.
pub(crate) fn make_bitmap_key_comparator(data_type: &DataType) -> KeyComparator {
    match data_type {
        DataType::Float(_) => Box::new(|left, right| {
            let left = f32::from_le_bytes(left[..4].try_into().unwrap());
            let right = f32::from_le_bytes(right[..4].try_into().unwrap());
            compare_float_like_java(left, right)
        }),
        DataType::Double(_) => Box::new(|left, right| {
            let left = f64::from_le_bytes(left[..8].try_into().unwrap());
            let right = f64::from_le_bytes(right[..8].try_into().unwrap());
            compare_double_like_java(left, right)
        }),
        _ => make_key_comparator(data_type),
    }
}

pub(crate) fn serialize_bitmap_datum(datum: &Datum, data_type: &DataType) -> Vec<u8> {
    match (datum, data_type) {
        (Datum::Float(value), DataType::Float(_)) => {
            let bits = if value.is_nan() {
                JAVA_CANONICAL_FLOAT_NAN_BITS
            } else {
                value.to_bits()
            };
            bits.to_le_bytes().to_vec()
        }
        (Datum::Double(value), DataType::Double(_)) => {
            let bits = if value.is_nan() {
                JAVA_CANONICAL_DOUBLE_NAN_BITS
            } else {
                value.to_bits()
            };
            bits.to_le_bytes().to_vec()
        }
        _ => serialize_datum(datum, data_type),
    }
}

pub(crate) fn is_bitmap_floating_residual_sensitive_op(op: PredicateOperator) -> bool {
    matches!(
        op,
        PredicateOperator::NotEq
            | PredicateOperator::NotIn
            | PredicateOperator::Lt
            | PredicateOperator::LtEq
            | PredicateOperator::Gt
            | PredicateOperator::GtEq
            | PredicateOperator::Between
            | PredicateOperator::NotBetween
    )
}

fn compare_float_like_java(left: f32, right: f32) -> Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => left.total_cmp(&right),
    }
}

fn compare_double_like_java(left: f64, right: f64) -> Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => left.total_cmp(&right),
    }
}

pub(super) fn block_info(offset: i64, length: i32) -> std::io::Result<BlockInfo> {
    let offset = u64::try_from(offset).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Invalid negative bitmap block offset: {offset}"),
        )
    })?;
    let length = usize::try_from(length).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Invalid negative bitmap block length: {length}"),
        )
    })?;
    Ok(BlockInfo { offset, length })
}
