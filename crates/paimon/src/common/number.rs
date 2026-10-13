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
