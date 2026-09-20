// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Java-compatible BTree V1 lists and adaptive V2 postings.

use super::var_len::{encode_var_int, encode_var_long};
use roaring::{RoaringBitmap, RoaringTreemap};
use std::io::{self, Cursor, Read};

const SINGLE: u8 = 0;
const DELTA: u8 = 1;
const ROARING: u8 = 2;

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

// Reject overlong/overflowing encodings instead of allowing high bits to wrap.
fn read_non_negative(input: &mut impl Read, bits: u32) -> io::Result<u64> {
    let mut value = 0u64;
    for shift in (0..bits).step_by(7) {
        let mut byte = [0];
        input.read_exact(&mut byte)?;
        let payload = u64::from(byte[0] & 0x7f);
        if payload >= (1u64 << (bits - shift).min(7)) {
            return Err(invalid("BTree posting integer overflow"));
        }
        value |= payload << shift;
        if byte[0] & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(invalid("Malformed BTree posting integer"))
}

pub(super) fn add_to(data: &[u8], version: u32, target: &mut RoaringTreemap) -> io::Result<()> {
    let mut input = Cursor::new(data);
    if version == 1 {
        let count = read_non_negative(&mut input, 31)?;
        for _ in 0..count {
            target.insert(read_non_negative(&mut input, 63)?);
        }
    } else {
        let mut tag = [0];
        input.read_exact(&mut tag)?;
        match tag[0] {
            SINGLE => {
                target.insert(read_non_negative(&mut input, 63)?);
            }
            DELTA => {
                let count = read_non_negative(&mut input, 31)?;
                if count < 2 {
                    return Err(invalid("Invalid BTree delta posting count"));
                }
                let mut row = read_non_negative(&mut input, 63)?;
                target.insert(row);
                for _ in 1..count {
                    let delta = read_non_negative(&mut input, 63)?;
                    if delta == 0 {
                        return Err(invalid("BTree row ID delta must be positive"));
                    }
                    row = row
                        .checked_add(delta)
                        .filter(|id| *id <= i64::MAX as u64)
                        .ok_or_else(|| invalid("BTree delta row ID overflow"))?;
                    target.insert(row);
                }
            }
            ROARING => {
                // The generic treemap decoder overwrites duplicate high keys.
                // Validate the portable outer map so corrupt input cannot lose rows silently.
                let mut count = [0; 8];
                input.read_exact(&mut count)?;
                let count = u64::from_le_bytes(count);
                if count == 0 {
                    return Err(invalid("Empty BTree Roaring posting"));
                }
                let mut bitmaps = Vec::new();
                let mut previous = None;
                for _ in 0..count {
                    let mut high = [0; 4];
                    input.read_exact(&mut high)?;
                    let high = u32::from_le_bytes(high);
                    if high > i32::MAX as u32 || previous.is_some_and(|last| high <= last) {
                        return Err(invalid("Invalid BTree Roaring high keys"));
                    }
                    let bitmap = RoaringBitmap::deserialize_from(&mut input)?;
                    if bitmap.is_empty() {
                        return Err(invalid("Empty BTree Roaring partition"));
                    }
                    bitmaps.push((high, bitmap));
                    previous = Some(high);
                }
                *target |= RoaringTreemap::from_bitmaps(bitmaps);
            }
            _ => return Err(invalid("Unknown BTree posting encoding")),
        }
    }
    if input.position() != data.len() as u64 {
        return Err(invalid("Trailing bytes in BTree posting"));
    }
    Ok(())
}

fn var_len(value: u64) -> usize {
    ((64 - value.leading_zeros()).max(1) as usize).div_ceil(7)
}

fn container_lower_bound(count: u64, runs: u64) -> u64 {
    4 + (2 * count).min(8192).min(2 + 4 * runs)
}

pub(super) fn serialize(rows: &[i64], version: u32) -> io::Result<Vec<u8>> {
    let count = i32::try_from(rows.len()).map_err(|_| invalid("Too many BTree row IDs"))?;
    if rows.is_empty() || rows.iter().any(|id| *id < 0) {
        return Err(invalid("BTree posting requires non-negative row IDs"));
    }
    if version == 1 {
        let mut result = Vec::new();
        encode_var_int(&mut result, count)?;
        for &row in rows {
            encode_var_long(&mut result, row)?;
        }
        return Ok(result);
    }
    if rows.len() == 1 {
        let mut result = vec![SINGLE];
        encode_var_long(&mut result, rows[0])?;
        return Ok(result);
    }

    let mut delta_size = 1 + var_len(rows.len() as u64) + var_len(rows[0] as u64);
    // Tag, high-key count, first high key, and nested bitmap cookie. As in Java,
    // a lower bound avoids building a bitmap when a delta list must be smaller.
    let mut roaring_min = 1 + 8 + 4 + 4;
    let mut count_in_container = 1;
    let mut runs = 1;
    for pair in rows.windows(2) {
        let (previous, row) = (pair[0], pair[1]);
        if row <= previous {
            return Err(invalid("BTree V2 row IDs must be strictly increasing"));
        }
        delta_size += var_len((row - previous) as u64);
        if row >> 16 != previous >> 16 {
            roaring_min += container_lower_bound(count_in_container, runs);
            if row >> 32 != previous >> 32 {
                roaring_min += 8;
            }
            count_in_container = 1;
            runs = 1;
        } else {
            count_in_container += 1;
            if row != previous + 1 {
                runs += 1;
            }
        }
    }
    roaring_min += container_lower_bound(count_in_container, runs);
    if roaring_min < delta_size as u64 {
        let mut bitmap = RoaringTreemap::new();
        let mut start = rows[0] as u64;
        let mut end = start;
        for &row in &rows[1..] {
            let row = row as u64;
            if row != end + 1 {
                bitmap.insert_range(start..=end);
                start = row;
            }
            end = row;
        }
        bitmap.insert_range(start..=end);
        bitmap.optimize();
        if 1 + bitmap.serialized_size() < delta_size {
            let mut result = Vec::with_capacity(1 + bitmap.serialized_size());
            result.push(ROARING);
            bitmap.serialize_into(&mut result)?;
            return Ok(result);
        }
    }
    let mut result = Vec::with_capacity(delta_size);
    result.push(DELTA);
    encode_var_int(&mut result, count)?;
    encode_var_long(&mut result, rows[0])?;
    for pair in rows.windows(2) {
        encode_var_long(&mut result, pair[1] - pair[0])?;
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodings_and_boundaries() {
        for (rows, tag) in [
            (vec![i64::MAX], SINGLE),
            (vec![0, 128, (1i64 << 32) + 5, i64::MAX], DELTA),
            ((60000..140000).collect(), ROARING),
            (((1i64 << 32) - 10..(1i64 << 32) + 10000).collect(), ROARING),
        ] {
            let expected: RoaringTreemap = rows.iter().map(|id| *id as u64).collect();
            for version in [1, 2] {
                let bytes = serialize(&rows, version).unwrap();
                if version == 2 {
                    assert_eq!(bytes[0], tag);
                }
                let mut actual = RoaringTreemap::new();
                add_to(&bytes, version, &mut actual).unwrap();
                assert_eq!(actual, expected);
            }
        }
    }

    #[test]
    fn rejects_corrupt_postings() {
        for bytes in [
            vec![],
            vec![3],
            vec![SINGLE],
            vec![SINGLE, 1, 2],
            vec![DELTA, 0],
            vec![DELTA, 1, 0],
            vec![DELTA, 2, 1, 0],
            vec![DELTA, 3, 0, 1],
            vec![ROARING, 0, 0, 0, 0, 0, 0, 0, 0],
            vec![SINGLE, 255, 255, 255, 255, 255, 255, 255, 255, 255, 1],
        ] {
            assert!(
                add_to(&bytes, 2, &mut RoaringTreemap::new()).is_err(),
                "{bytes:?}"
            );
        }
        let mut overflow = vec![DELTA, 2];
        encode_var_long(&mut overflow, i64::MAX).unwrap();
        overflow.push(1);
        assert!(add_to(&overflow, 2, &mut RoaringTreemap::new()).is_err());
        for rows in [vec![], vec![-1], vec![1, 1], vec![2, 1]] {
            assert!(serialize(&rows, 2).is_err());
        }
    }

    #[test]
    fn rejects_duplicate_unordered_and_unsigned_roaring_partitions() {
        for keys in [[0, 0], [1, 0], [0, 1 << 31]] {
            let mut bytes = vec![ROARING];
            bytes.extend_from_slice(&2u64.to_le_bytes());
            for (high, row) in keys.into_iter().zip([1, 2]) {
                bytes.extend_from_slice(&u32::to_le_bytes(high));
                RoaringBitmap::from_iter([row])
                    .serialize_into(&mut bytes)
                    .unwrap();
            }
            assert!(add_to(&bytes, 2, &mut RoaringTreemap::new()).is_err());
        }
    }
}
