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

//! Java numeric text shared by partition paths and value conversions.

use std::cmp::Ordering;

use num_bigint::BigUint;
use num_traits::ToPrimitive;

/// Character.digit for decimal BMP digits (Java scans UTF-16 chars).
pub(crate) fn java_decimal_digit(ch: char) -> Option<u8> {
    const ZEROES: &[u32] = &[
        0x0030, 0x0660, 0x06f0, 0x07c0, 0x0966, 0x09e6, 0x0a66, 0x0ae6, 0x0b66, 0x0be6, 0x0c66,
        0x0ce6, 0x0d66, 0x0de6, 0x0e50, 0x0ed0, 0x0f20, 0x1040, 0x1090, 0x17e0, 0x1810, 0x1946,
        0x19d0, 0x1a80, 0x1a90, 0x1b50, 0x1bb0, 0x1c40, 0x1c50, 0xa620, 0xa8d0, 0xa900, 0xa9d0,
        0xa9f0, 0xaa50, 0xabf0, 0xff10,
    ];

    ZEROES.iter().find_map(|zero| {
        (u32::from(ch) >= *zero && u32::from(ch) < zero + 10).then(|| (u32::from(ch) - zero) as u8)
    })
}

/// BinaryStringUtils integer text: truncate fractions and reject overflow.
pub(crate) fn parse_java_integer(text: &str, min: i64, max: i64) -> crate::Result<i64> {
    let bytes = text.as_bytes();
    if bytes.is_empty() {
        return Err(integer_text_error("Empty integer value"));
    }
    let negative = bytes[0] == b'-';
    let mut index = usize::from(negative || bytes[0] == b'+');
    if index == bytes.len() {
        return Err(integer_text_error(format!("Invalid integer value: {text}")));
    }

    // Accumulate a negative value so the signed minimum remains representable.
    // Like Java, the digits before '.' may be empty; the fractional part does
    // not affect the result but must contain only decimal digits.
    let limit = if negative { min } else { -max };
    let mut value = 0_i64;
    while index < bytes.len() && bytes[index] != b'.' {
        let digit = bytes[index];
        if !digit.is_ascii_digit() {
            return Err(integer_text_error(format!("Invalid integer value: {text}")));
        }
        value = value
            .checked_mul(10)
            .and_then(|value| value.checked_sub(i64::from(digit - b'0')))
            .filter(|value| *value >= limit)
            .ok_or_else(|| integer_text_error(format!("Integer value overflow: {text}")))?;
        index += 1;
    }
    if index < bytes.len() && !bytes[index + 1..].iter().all(u8::is_ascii_digit) {
        return Err(integer_text_error(format!("Invalid integer value: {text}")));
    }
    Ok(if negative { value } else { -value })
}

fn integer_text_error(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

/// Numeric digits selected by Java 8/11/17 Double.toString, for BigDecimal.valueOf.
/// Its integral shortcut retains digits that the JDK 19+ shortest converter
/// discards. The remaining cases generate digits from the IEEE-754 interval;
/// fixed-width and arbitrary-precision arithmetic use different upper endpoints.
/// The result is coefficient/exponent text, rather than a display format.
pub(crate) fn java_double_decimal_text(value: f64) -> String {
    debug_assert!(value.is_finite());
    if value == 0.0 {
        return "0".into();
    }
    let bits = value.to_bits();
    let sign = if bits >> 63 != 0 { "-" } else { "" };
    let mut exponent = ((bits >> 52) & 2047) as i32 - 1023;
    let mut mantissa = bits & ((1_u64 << 52) - 1);
    let significant = if exponent == -1023 {
        let significant = 64 - mantissa.leading_zeros() as i32;
        let normalize = 53 - significant;
        mantissa <<= normalize;
        exponent = -1022 - normalize;
        significant
    } else {
        mantissa |= 1_u64 << 52;
        53
    };
    let trailing = mantissa.trailing_zeros() as i32;
    let width = 53 - trailing;
    let tiny = (width - exponent - 1).max(0);
    if (-21..=62).contains(&exponent) && tiny == 0 {
        let integer = if exponent >= 52 {
            mantissa << (exponent - 52)
        } else {
            mantissa >> (52 - exponent)
        };
        let discarded = if exponent > significant {
            (1_u64 << (exponent - significant - 1)).ilog10()
        } else {
            0
        };
        let factor = 10_u64.pow(discarded);
        return format!("{sign}{}e{discarded}", (integer + factor / 2) / factor);
    }

    // Approximate log10 from a normalized binary significand. A leading zero
    // corrects an overestimate while keeping the original rounding interval.
    let normalized = mantissa as f64 / (1_u64 << 52) as f64;
    // Retain the JVM's truncated log10(2) estimate for arithmetic-path selection.
    #[allow(clippy::approx_constant)]
    let mut decimal_exponent =
        ((normalized - 1.5) * 0.289529654 + 0.176091259 + f64::from(exponent) * 0.301029995663981)
            .floor() as i32;
    let mut interval = DecimalInterval::new(
        mantissa,
        exponent,
        significant,
        width,
        tiny,
        decimal_exponent,
    );
    let mut coefficient = 0_u128;
    let mut count = 0;
    loop {
        let (digit, mut low, mut high, direction) = interval.next_digit(count != 0);
        if digit == 0 && count == 0 && !high {
            decimal_exponent -= 1;
        } else {
            coefficient = coefficient * 10 + u128::from(digit);
            count += 1;
        }
        if count <= 1 && !(-3..8).contains(&decimal_exponent) {
            low = false;
            high = false;
        }
        if low || high {
            if high
                && (!low
                    || direction == Ordering::Greater
                    || direction == Ordering::Equal && !coefficient.is_multiple_of(2))
            {
                coefficient += 1;
            }
            return format!("{sign}{coefficient}e{}", decimal_exponent - count + 1);
        }
    }
}

enum DecimalInterval {
    Small {
        numerator: i128,
        denominator: i128,
        margin: i128,
        word: u32,
    },
    Big {
        numerator: BigUint,
        denominator: BigUint,
        margin: BigUint,
    },
}

impl DecimalInterval {
    fn new(
        mantissa: u64,
        exponent: i32,
        significant: i32,
        width: i32,
        tiny: i32,
        decimal_exponent: i32,
    ) -> Self {
        // Factor the scaled value and half-ULP margin into powers of 2 and 5,
        // removing common binary factors before choosing an arithmetic width.
        let negative_power = (-decimal_exponent).max(0);
        let positive_power = decimal_exponent.max(0);
        let mut numerator_power = negative_power + tiny + exponent - width + 1;
        let mut denominator_power = positive_power + tiny;
        let mut margin_power = negative_power + tiny + exponent - significant;
        let common = numerator_power.min(denominator_power);
        numerator_power -= common;
        denominator_power -= common;
        margin_power -= common;
        if width == 1 {
            margin_power -= 1;
        }
        if margin_power < 0 {
            numerator_power -= margin_power;
            denominator_power -= margin_power;
            margin_power = 0;
        }
        let five = |power: i32| BigUint::from(5_u8).pow(power as u32);
        let numerator = (BigUint::from(mantissa >> (53 - width)) * five(negative_power))
            << numerator_power as usize;
        let denominator = five(positive_power) << denominator_power as usize;
        let margin = five(negative_power) << margin_power as usize;
        let five_bits = |power: i32| {
            if (1..27).contains(&power) {
                five(power).bits() as i32
            } else {
                power * 3
            }
        };
        let bits = (width + numerator_power + five_bits(negative_power))
            .max(denominator_power + 1 + five_bits(positive_power + 1));
        if bits < 64 {
            Self::Small {
                numerator: numerator.to_i128().expect("fixed-width numerator"),
                denominator: denominator.to_i128().expect("fixed-width denominator"),
                margin: margin.to_i128().expect("fixed-width margin"),
                word: if bits < 32 { 32 } else { 64 },
            }
        } else {
            Self::Big {
                numerator,
                denominator,
                margin,
            }
        }
    }

    fn next_digit(&mut self, after_first: bool) -> (u8, bool, bool, Ordering) {
        match self {
            Self::Small {
                numerator,
                denominator,
                margin,
                word,
            } => {
                let wrap = |value: i128| {
                    if *word == 32 {
                        value as i32 as i128
                    } else {
                        value as i64 as i128
                    }
                };
                let digit = (*numerator / *denominator) as u8;
                *numerator = (*numerator % *denominator) * 10;
                *margin = wrap(*margin * 10);
                let limit = *denominator * 10;
                let (low, high) = if after_first && *margin <= 0 {
                    (true, true)
                } else {
                    (*numerator < *margin, wrap(*numerator + *margin) > limit)
                };
                (digit, low, high, wrap(2 * *numerator - limit).cmp(&0))
            }
            Self::Big {
                numerator,
                denominator,
                margin,
            } => {
                let digit = (&*numerator / &*denominator)
                    .to_u8()
                    .expect("decimal digit");
                *numerator %= &*denominator;
                *numerator *= 10_u8;
                *margin *= 10_u8;
                let limit = &*denominator * 10_u8;
                let low = *numerator < *margin;
                let high = &*numerator + &*margin >= limit;
                (digit, low, high, (&*numerator << 1_usize).cmp(&limit))
            }
        }
    }
}

/// Java's shortest-decimal string contract (JDK 19+), including its notation
/// thresholds and closest choice among one- and two-digit decimals. Rust's
/// shortest Display chooses different digits at ties; Ryū uses ties-to-even.
pub(crate) fn format_java_float<T: ryu::Float + std::fmt::LowerExp + Copy + Into<f64>>(
    value: T,
) -> String {
    let number = value.into();
    if number.is_nan() {
        return "NaN".into();
    }
    if number.is_infinite() {
        return if number.is_sign_negative() {
            "-Infinity"
        } else {
            "Infinity"
        }
        .into();
    }
    if number == 0.0 {
        return if number.is_sign_negative() {
            "-0.0"
        } else {
            "0.0"
        }
        .into();
    }

    let mut buffer = ryu::Buffer::new();
    let (mut negative, mut digits, mut exponent) = decimal_parts(buffer.format_finite(value));
    if digits.len() == 1 {
        // Padding the shortest digit with zero would miss the closer two-digit
        // form of small subnormals, e.g. Double.MIN_VALUE -> 4.9E-324.
        (negative, digits, exponent) = decimal_parts(&format!("{value:.1e}"));
    }
    let mut result = if negative {
        "-".to_string()
    } else {
        String::new()
    };
    if (-3..7).contains(&exponent) {
        let point = exponent + 1;
        if point <= 0 {
            result.push_str("0.");
            result.push_str(&"0".repeat((-point) as usize));
            result.push_str(&digits);
        } else if point as usize >= digits.len() {
            result.push_str(&digits);
            result.push_str(&"0".repeat(point as usize - digits.len()));
            result.push_str(".0");
        } else {
            result.push_str(&digits[..point as usize]);
            result.push('.');
            result.push_str(&digits[point as usize..]);
        }
    } else {
        result.push_str(&digits[..1]);
        result.push('.');
        result.push_str(if digits.len() > 1 { &digits[1..] } else { "0" });
        result.push('E');
        result.push_str(&exponent.to_string());
    }
    result
}

fn decimal_parts(text: &str) -> (bool, String, i32) {
    let (mantissa, exponent) = text
        .split_once('e')
        .map_or((text, 0), |(mantissa, exponent)| {
            (mantissa, exponent.parse::<i32>().expect("decimal exponent"))
        });
    let negative = mantissa.starts_with('-');
    let mantissa = mantissa.trim_start_matches('-');
    let point = mantissa.find('.').unwrap_or(mantissa.len()) as i32;
    let digits = mantissa.replace('.', "");
    let first = digits
        .bytes()
        .position(|byte| byte != b'0')
        .expect("nonzero decimal");
    let exponent = exponent + point - first as i32 - 1;
    let mut digits = digits[first..].to_string();
    while digits.len() > 1 && digits.ends_with('0') {
        digits.pop();
    }
    (negative, digits, exponent)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn double_decimal_digits_match_java_8_11_and_17_golden_outputs() {
        // Outputs obtained from the actual JVMs, including integral rounding,
        // subnormal normalization, and both sides of binary exponent boundaries.
        for (bits, expected) in [
            (0xc381_e88f_38c0_c8fd, "-1.61304845894623136E17"),
            (0x44b5_2d02_c7e1_4af6, "9.999999999999999E22"),
            (0x0000_0000_0000_0001, "4.9E-324"),
            (0x0010_0000_0000_0000, "2.2250738585072014E-308"),
            (0x7fef_ffff_ffff_ffff, "1.7976931348623157E308"),
            (0x0000_0000_0000_0002, "1.0E-323"),
            (0x000f_ffff_ffff_ffff, "2.225073858507201E-308"),
            (0x0010_0000_0000_0001, "2.225073858507202E-308"),
            (0x016f_ffff_ffff_ffff, "9.332636185032188E-302"),
            (0x0170_0000_0000_0000, "9.332636185032189E-302"),
            (0x0170_0000_0000_0001, "9.33263618503219E-302"),
            (0x39af_ffff_ffff_ffff, "7.888609052210117E-31"),
            (0x39b0_0000_0000_0000, "7.888609052210118E-31"),
            (0x39b0_0000_0000_0001, "7.88860905221012E-31"),
            (0x3bef_ffff_ffff_ffff, "5.4210108624275216E-20"),
            (0x3bf0_0000_0000_0000, "5.421010862427522E-20"),
            (0x3bf0_0000_0000_0001, "5.421010862427523E-20"),
            (0x3c9f_ffff_ffff_ffff, "1.1102230246251564E-16"),
            (0x3ca0_0000_0000_0000, "1.1102230246251565E-16"),
            (0x3ca0_0000_0000_0001, "1.1102230246251568E-16"),
            (0x3e9f_ffff_ffff_ffff, "4.7683715820312495E-7"),
            (0x3ea0_0000_0000_0000, "4.76837158203125E-7"),
            (0x3ea0_0000_0000_0001, "4.768371582031251E-7"),
            (0x3fbf_ffff_ffff_ffff, "0.12499999999999999"),
            (0x3fc0_0000_0000_0000, "0.125"),
            (0x3fc0_0000_0000_0001, "0.12500000000000003"),
            (0x3fef_ffff_ffff_ffff, "0.9999999999999999"),
            (0x3ff0_0000_0000_0000, "1.0"),
            (0x3ff0_0000_0000_0001, "1.0000000000000002"),
            (0x3fff_ffff_ffff_ffff, "1.9999999999999998"),
            (0x4000_0000_0000_0000, "2.0"),
            (0x4000_0000_0000_0001, "2.0000000000000004"),
            (0x41ef_ffff_ffff_ffff, "4.2949672959999995E9"),
            (0x41f0_0000_0000_0000, "4.294967296E9"),
            (0x41f0_0000_0000_0001, "4.294967296000001E9"),
            (0x432f_ffff_ffff_ffff, "4.5035996273704955E15"),
            (0x4330_0000_0000_0000, "4.503599627370496E15"),
            (0x4330_0000_0000_0001, "4.503599627370497E15"),
            (0x433f_ffff_ffff_ffff, "9.007199254740991E15"),
            (0x4340_0000_0000_0000, "9.007199254740992E15"),
            (0x4340_0000_0000_0001, "9.007199254740994E15"),
            (0x434f_ffff_ffff_ffff, "1.8014398509481982E16"),
            (0x4350_0000_0000_0000, "1.8014398509481984E16"),
        ] {
            let actual = java_double_decimal_text(f64::from_bits(bits));
            assert_eq!(
                decimal_parts(&actual),
                decimal_parts(&expected.replace('E', "e")),
                "{bits:x}"
            );
        }
    }
}
