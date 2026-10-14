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

//! Float.parseFloat / Double.parseDouble text, without a new parser dependency.

use num_bigint::BigUint;
use num_traits::ToPrimitive;

pub(crate) fn parse_java_float(text: &str, single: bool) -> crate::Result<f64> {
    let invalid = || crate::Error::DataInvalid {
        message: format!("Invalid Java floating value: '{text}'"),
        source: None,
    };
    let trimmed = text.trim_matches(|c| c <= ' ');
    let unsigned = trimmed.strip_prefix(['-', '+']).unwrap_or(trimmed);
    if unsigned == "NaN" {
        return Ok(f64::NAN);
    }
    if unsigned == "Infinity" {
        return Ok(if trimmed.starts_with('-') {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        });
    }
    let number = trimmed
        .strip_suffix(['f', 'F', 'd', 'D'])
        .unwrap_or(trimmed);
    let unsigned = number.strip_prefix(['-', '+']).unwrap_or(number);
    if let Some(hex) = unsigned
        .strip_prefix("0x")
        .or_else(|| unsigned.strip_prefix("0X"))
    {
        let (mantissa, exponent) = hex.split_once(['p', 'P']).ok_or_else(invalid)?;
        let exponent_digits = exponent.strip_prefix(['-', '+']).unwrap_or(exponent);
        if exponent_digits.is_empty() || !exponent_digits.bytes().all(|b| b.is_ascii_digit()) {
            return Err(invalid());
        }
        let exponent = exponent.parse::<i64>().unwrap_or_else(|_| {
            if exponent.starts_with('-') {
                i64::MIN
            } else {
                i64::MAX
            }
        });
        let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        if whole.is_empty() && fraction.is_empty()
            || !whole
                .bytes()
                .chain(fraction.bytes())
                .all(|b| b.is_ascii_hexdigit())
        {
            return Err(invalid());
        }
        let coefficient = BigUint::parse_bytes(format!("{whole}{fraction}").as_bytes(), 16)
            .ok_or_else(invalid)?;
        let exponent = exponent.saturating_sub((fraction.len() as i64).saturating_mul(4));
        return Ok(hex_float(
            coefficient,
            exponent,
            number.starts_with('-'),
            single,
        ));
    }
    // Rust's parser accepts some non-Java spellings. Validate the Java decimal
    // grammar first, retaining its ASCII-only digits and suffix/trim rules.
    let (mantissa, exponent) = unsigned.split_once(['e', 'E']).unwrap_or((unsigned, ""));
    if unsigned.contains(['e', 'E']) {
        let digits = exponent.strip_prefix(['-', '+']).unwrap_or(exponent);
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return Err(invalid());
        }
    }
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty() && fraction.is_empty()
        || !whole
            .bytes()
            .chain(fraction.bytes())
            .all(|b| b.is_ascii_digit())
    {
        return Err(invalid());
    }
    if single {
        number.parse::<f32>().map(f64::from).map_err(|_| invalid())
    } else {
        number.parse::<f64>().map_err(|_| invalid())
    }
}

fn hex_float(coefficient: BigUint, exponent: i64, negative: bool, single: bool) -> f64 {
    let fraction = if single { 23 } else { 52 };
    let bias = if single { 127 } else { 1023 };
    let minimum = 1 - bias;
    let sign = u64::from(negative) << if single { 31 } else { 63 };
    let build = |bits| {
        if single {
            f64::from(f32::from_bits(bits as u32))
        } else {
            f64::from_bits(bits)
        }
    };
    if coefficient.bits() == 0 {
        return build(sign);
    }
    let leading = (coefficient.bits() as i64 - 1).saturating_add(exponent);
    let infinity = (((bias * 2 + 1) as u64) << fraction) | sign;
    if leading > bias {
        return build(infinity);
    }
    if leading < minimum - fraction - 1 {
        return build(sign);
    }
    let mut unit = leading.max(minimum) - fraction;
    let shift = unit.saturating_sub(exponent);
    let rounded = if shift > 0 {
        let shift = shift as usize;
        let rounded = &coefficient >> shift;
        let remainder = coefficient - (&rounded << shift);
        let half = BigUint::from(1_u8) << (shift - 1);
        let odd = rounded.bit(0);
        rounded + u8::from(remainder > half || remainder == half && odd)
    } else {
        coefficient << (-shift) as usize
    };
    let mut significand = rounded.to_u64().expect("rounded IEEE significand");
    if significand == 1_u64 << (fraction + 1) {
        significand >>= 1;
        unit += 1;
    }
    let normal = unit + fraction;
    if normal > bias {
        build(infinity)
    } else if significand < 1_u64 << fraction {
        build(sign | significand)
    } else {
        build(sign | ((normal + bias) as u64) << fraction | (significand - (1_u64 << fraction)))
    }
}
