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

//! Java decimal conversions for existing Python-supported schema changes.

use std::sync::Arc;

use arrow_array::{
    cast::AsArray,
    types::{Float32Type, Float64Type},
    ArrayRef, Decimal128Array,
};

use crate::common::number::java_double_decimal_text;
use crate::spec::DataType;

/// Match NumericPrimitiveToDecimalCastRule and StringToDecimalCastRule.
/// Java first builds a BigDecimal from decimal text, rounds HALF_UP, then
/// checks precision. Multiplying the binary float by 10^scale loses digits.
pub(super) fn cast_decimal(
    source: &ArrayRef,
    source_type: &DataType,
    target_type: &DataType,
) -> crate::Result<Option<ArrayRef>> {
    let DataType::Decimal(target) = target_type else {
        return Ok(None);
    };
    let precision = target.precision();
    let scale = target.scale();
    let values = match source_type {
        DataType::Float(_) => source
            .as_primitive::<Float32Type>()
            .iter()
            // Java Number.doubleValue expands FLOAT before Double.toString.
            .map(|v| {
                v.map(|v| float_decimal(f64::from(v), precision, scale))
                    .transpose()
            })
            .collect::<crate::Result<Vec<_>>>()?,
        DataType::Double(_) => source
            .as_primitive::<Float64Type>()
            .iter()
            .map(|v| v.map(|v| float_decimal(v, precision, scale)).transpose())
            .collect::<crate::Result<Vec<_>>>()?,
        DataType::Char(_) | DataType::VarChar(_) => source
            .as_string::<i32>()
            .iter()
            .map(|v| v.map(|v| decimal_text(v, precision, scale)).transpose())
            .collect::<crate::Result<Vec<_>>>()?,
        _ => return Ok(None),
    };
    let values = values.into_iter().map(Option::flatten);
    let result = Decimal128Array::from_iter(values)
        .with_precision_and_scale(precision as u8, scale as i8)
        .map_err(|e| invalid(e.to_string()))?;
    Ok(Some(Arc::new(result)))
}

fn float_decimal(value: f64, precision: u32, scale: u32) -> crate::Result<Option<i128>> {
    if !value.is_finite() {
        return Err(invalid("Non-finite decimal schema-evolution value"));
    }
    decimal_text(&java_double_decimal_text(value), precision, scale)
}

/// Parse only the digits needed by the target, so a long mantissa or exponent
/// never requires a large power of ten. Invalid text is an error; a valid
/// number exceeding target precision returns NULL, as Decimal.fromBigDecimal.
fn decimal_text(text: &str, precision: u32, scale: u32) -> crate::Result<Option<i128>> {
    let invalid_text = || invalid(format!("Invalid decimal schema-evolution value: {text}"));
    let (mantissa, exponent) = text
        .split_once(['e', 'E'])
        .map_or(Ok((text, 0)), |(m, e)| {
            e.parse::<i64>().map(|e| (m, e)).map_err(|_| invalid_text())
        })?;
    let negative = mantissa.starts_with('-');
    let mantissa = mantissa.strip_prefix(['-', '+']).unwrap_or(mantissa);
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty() && fraction.is_empty()
        || !whole
            .bytes()
            .chain(fraction.bytes())
            .all(|b| b.is_ascii_digit())
    {
        return Err(invalid_text());
    }
    // BigDecimal checks the adjusted input scale, rather than the exponent
    // alone: 0e2147483648 is valid, but 0e-2147483648 is not.
    let input_scale = (fraction.len() as i64)
        .checked_sub(exponent)
        .and_then(|scale| i32::try_from(scale).ok())
        .ok_or_else(invalid_text)?;
    let digits = format!("{whole}{fraction}");
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return Ok(Some(0));
    }
    let shift = i64::from(scale) - i64::from(input_scale);
    let power = shift.abs();
    // Match setScale's checkScale and BigInteger.TEN.pow supported-range
    // check. Java evaluates this before target precision, except for zero.
    let max_magnitude_length = i64::from(i32::MAX) / 32 + 1;
    if power > i64::from(i32::MAX) || 4 * power / 32 > max_magnitude_length {
        return Err(invalid(
            "Decimal scale conversion exceeds Java's supported range",
        ));
    }
    let kept = digits.len() as i64 + shift.min(0);
    if kept + shift.max(0) > i64::from(precision) {
        return Ok(None);
    }
    let (mut value, round) = if kept > 0 {
        let kept = kept as usize;
        (
            digits[..kept].parse::<i128>().map_err(|_| invalid_text())?,
            digits.as_bytes().get(kept).is_some_and(|b| *b >= b'5'),
        )
    } else {
        (0, kept == 0 && digits.as_bytes()[0] >= b'5')
    };
    if shift > 0 {
        value *= 10_i128.pow(shift as u32);
    }
    value += i128::from(round);
    if value >= 10_i128.pow(precision) {
        return Ok(None);
    }
    Ok(Some(if negative { -value } else { value }))
}

fn invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Decimal128Array, Float32Array, Float64Array, StringArray};

    use crate::arrow::nested_evolution::evolve_column;
    use crate::spec::{DataType, DecimalType, DoubleType, FloatType, VarCharType};

    fn decimals(source: &ArrayRef, from: DataType, precision: u32, scale: u32) -> Decimal128Array {
        evolve_column(
            source,
            &from,
            &DataType::Decimal(DecimalType::new(precision, scale).unwrap()),
        )
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap()
        .clone()
    }

    #[test]
    fn doubles_use_java_decimal_text_before_half_up_rounding() {
        let source: ArrayRef = Arc::new(Float64Array::from(vec![
            None,
            Some(1.005),
            Some(-1.005),
            Some(2.675),
            Some(-2.675),
            Some(0.145),
            Some(999.995),
            Some(1e30),
            None,
            Some(0.0),
        ]));
        let actual = decimals(
            &source.slice(1, 8),
            DataType::Double(DoubleType::new()),
            5,
            2,
        );
        assert_eq!(
            actual,
            Decimal128Array::from(vec![
                Some(101),
                Some(-101),
                Some(268),
                Some(-268),
                Some(15),
                None,
                None,
                None,
            ])
            .with_precision_and_scale(5, 2)
            .unwrap()
        );
    }

    #[test]
    fn decimal_digits_match_java_8_11_and_17_rather_than_modern_shortest_text() {
        let value = f64::from_bits(0xc381_e88f_38c0_c8fd);
        let source: ArrayRef = Arc::new(Float64Array::from(vec![Some(value), Some(-value), None]));
        assert_eq!(
            decimals(&source, DataType::Double(DoubleType::new()), 38, 20),
            Decimal128Array::from(vec![
                Some(-16_130_484_589_462_313_600_000_000_000_000_000_000),
                Some(16_130_484_589_462_313_600_000_000_000_000_000_000),
                None,
            ])
            .with_precision_and_scale(38, 20)
            .unwrap()
        );
        let source: ArrayRef = Arc::new(Float64Array::from(vec![1e23]));
        assert_eq!(
            decimals(&source, DataType::Double(DoubleType::new()), 38, 6),
            Decimal128Array::from(vec![99_999_999_999_999_990_000_000_000_000_i128])
                .with_precision_and_scale(38, 6)
                .unwrap()
        );
    }

    #[test]
    fn float_is_promoted_to_double_before_decimal_text_conversion() {
        let source: ArrayRef =
            Arc::new(Float32Array::from(vec![Some(1.005), Some(1.2345679), None]));
        let actual = decimals(&source, DataType::Float(FloatType::new()), 38, 20);
        assert_eq!(
            actual,
            Decimal128Array::from(vec![
                Some(100_499_999_523_162_840_000),
                Some(123_456_788_063_049_320_000),
                None,
            ])
            .with_precision_and_scale(38, 20)
            .unwrap()
        );
    }

    #[test]
    fn doubles_keep_decimal_significant_digits_at_high_scale() {
        let source: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1.005),
            Some(1.2345678901234567),
            Some(f64::MIN_POSITIVE),
            None,
        ]));
        let actual = decimals(&source, DataType::Double(DoubleType::new()), 38, 20);
        assert_eq!(
            actual,
            Decimal128Array::from(vec![
                Some(100_500_000_000_000_000_000),
                Some(123_456_789_012_345_670_000),
                Some(0),
                None,
            ])
            .with_precision_and_scale(38, 20)
            .unwrap()
        );
    }

    #[test]
    fn decimal_text_rounds_and_nulls_overflow_after_rounding() {
        let source: ArrayRef = Arc::new(StringArray::from(vec![
            "1.005", "-1.005", "+.005", "2.675", "1e-2", "999.995", "1e100",
        ]));
        assert_eq!(
            decimals(&source, DataType::VarChar(VarCharType::string_type()), 5, 2),
            Decimal128Array::from(vec![
                Some(101),
                Some(-101),
                Some(1),
                Some(268),
                Some(1),
                None,
                None
            ])
            .with_precision_and_scale(5, 2)
            .unwrap()
        );
    }

    #[test]
    fn invalid_decimal_text_is_an_error_instead_of_silent_null() {
        for text in [
            "",
            " 1.2 ",
            "1_2",
            "1.2.3",
            "1e",
            "Infinity",
            "0e-2147483648",
            "0.1e-2147483647",
            "1e2147483647",
            "1e-2147483647",
            "1e9999999999999999999",
        ] {
            let source: ArrayRef = Arc::new(StringArray::from(vec![None, Some(text)]));
            assert!(
                evolve_column(
                    &source,
                    &DataType::VarChar(VarCharType::string_type()),
                    &DataType::Decimal(DecimalType::new(5, 2).unwrap())
                )
                .is_err(),
                "{text:?}"
            );
        }
    }

    #[test]
    fn decimal_text_handles_long_mantissas_and_zero_exponents_without_large_powers() {
        let long = format!("1.005{}", "0".repeat(200));
        let tiny = format!("0.{}1", "0".repeat(200));
        let source: ArrayRef = Arc::new(StringArray::from(vec![
            long.as_str(),
            tiny.as_str(),
            "0e2147483648",
            "0.0e2147483648",
            "0e2147483647",
        ]));
        assert_eq!(
            decimals(&source, DataType::VarChar(VarCharType::string_type()), 5, 2),
            Decimal128Array::from(vec![101, 0, 0, 0, 0])
                .with_precision_and_scale(5, 2)
                .unwrap()
        );
    }
}
