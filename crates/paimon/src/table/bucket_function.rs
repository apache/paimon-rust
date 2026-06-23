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

use crate::spec::{
    batch_build_binary_rows, BinaryRow, BucketFunctionType, DataField, DataType, Datum,
};
use arrow_array::RecordBatch;

pub(crate) fn validate_bucket_function(
    bucket_function_type: BucketFunctionType,
    bucket_key_fields: &[DataField],
) -> crate::Result<()> {
    if bucket_function_type == BucketFunctionType::Mod {
        if bucket_key_fields.len() != 1 {
            return Err(crate::Error::ConfigInvalid {
                message: "bucket key must have exactly one field in mod bucket function"
                    .to_string(),
            });
        }
        let data_type = bucket_key_fields[0].data_type();
        if !matches!(data_type, DataType::Int(_) | DataType::BigInt(_)) {
            return Err(crate::Error::ConfigInvalid {
                message: format!(
                    "bucket key type must be INT or BIGINT in mod bucket function, but got {data_type:?}"
                ),
            });
        }
    }
    Ok(())
}

pub(crate) fn batch_bucket_ids(
    batch: &RecordBatch,
    field_indices: &[usize],
    fields: &[DataField],
    bucket_function_type: BucketFunctionType,
    total_buckets: i32,
) -> crate::Result<Vec<i32>> {
    let rows = batch_build_binary_rows(batch, field_indices, fields)?;
    let bucket_key_fields: Vec<DataField> = field_indices
        .iter()
        .map(|&idx| fields[idx].clone())
        .collect();
    rows.iter()
        .map(|row| bucket_for_row(row, &bucket_key_fields, bucket_function_type, total_buckets))
        .collect()
}

pub(crate) fn bucket_for_datums(
    datums: &[(Option<&Datum>, &DataType)],
    bucket_function_type: BucketFunctionType,
    total_buckets: i32,
) -> crate::Result<i32> {
    match bucket_function_type {
        BucketFunctionType::Default => {
            Ok(BinaryRow::compute_bucket_from_datums(datums, total_buckets))
        }
        BucketFunctionType::Mod => mod_bucket_from_datums(datums, total_buckets),
        BucketFunctionType::Hive => hive_bucket_from_datums(datums, total_buckets),
    }
}

fn bucket_for_row(
    row: &BinaryRow,
    bucket_key_fields: &[DataField],
    bucket_function_type: BucketFunctionType,
    total_buckets: i32,
) -> crate::Result<i32> {
    match bucket_function_type {
        BucketFunctionType::Default => Ok(default_bucket(row.hash_code(), total_buckets)),
        BucketFunctionType::Mod => mod_bucket_from_row(row, bucket_key_fields, total_buckets),
        BucketFunctionType::Hive => hive_bucket_from_row(row, bucket_key_fields, total_buckets),
    }
}

fn default_bucket(hash: i32, total_buckets: i32) -> i32 {
    (hash % total_buckets).wrapping_abs()
}

fn floor_mod_i64(value: i64, divisor: i32) -> i32 {
    value.rem_euclid(divisor as i64) as i32
}

fn mod_bucket_from_row(
    row: &BinaryRow,
    bucket_key_fields: &[DataField],
    total_buckets: i32,
) -> crate::Result<i32> {
    validate_bucket_function(BucketFunctionType::Mod, bucket_key_fields)?;
    if row.is_null_at(0) {
        return Ok(0);
    }
    match bucket_key_fields[0].data_type() {
        DataType::Int(_) => Ok(floor_mod_i64(row.get_int(0)? as i64, total_buckets)),
        DataType::BigInt(_) => Ok(floor_mod_i64(row.get_long(0)?, total_buckets)),
        other => Err(crate::Error::Unsupported {
            message: format!("bucket key type must be INT or BIGINT, but got {other:?}"),
        }),
    }
}

fn mod_bucket_from_datums(
    datums: &[(Option<&Datum>, &DataType)],
    total_buckets: i32,
) -> crate::Result<i32> {
    if datums.len() != 1 {
        return Err(crate::Error::ConfigInvalid {
            message: "bucket key must have exactly one field in mod bucket function".to_string(),
        });
    }
    match datums[0] {
        (None, DataType::Int(_) | DataType::BigInt(_)) => Ok(0),
        (Some(Datum::Int(v)), DataType::Int(_)) => Ok(floor_mod_i64(*v as i64, total_buckets)),
        (Some(Datum::Long(v)), DataType::BigInt(_)) => Ok(floor_mod_i64(*v, total_buckets)),
        (_, data_type) => Err(crate::Error::Unsupported {
            message: format!("bucket key type must be INT or BIGINT, but got {data_type:?}"),
        }),
    }
}

fn hive_bucket_from_row(
    row: &BinaryRow,
    bucket_key_fields: &[DataField],
    total_buckets: i32,
) -> crate::Result<i32> {
    let mut hash = 0_i32;
    for (pos, field) in bucket_key_fields.iter().enumerate() {
        let datum = row.get_datum(pos, field.data_type())?;
        hash = hash
            .wrapping_mul(31)
            .wrapping_add(hive_hash_datum(datum.as_ref(), field.data_type())?);
    }
    Ok(positive_mod(hash, total_buckets))
}

fn hive_bucket_from_datums(
    datums: &[(Option<&Datum>, &DataType)],
    total_buckets: i32,
) -> crate::Result<i32> {
    let mut hash = 0_i32;
    for (datum, data_type) in datums {
        hash = hash
            .wrapping_mul(31)
            .wrapping_add(hive_hash_datum(*datum, data_type)?);
    }
    Ok(positive_mod(hash, total_buckets))
}

fn positive_mod(hash: i32, total_buckets: i32) -> i32 {
    ((hash as u32 & 0x7fff_ffff) % total_buckets as u32) as i32
}

fn hive_hash_datum(datum: Option<&Datum>, data_type: &DataType) -> crate::Result<i32> {
    let Some(datum) = datum else {
        return Ok(0);
    };

    match (datum, data_type) {
        (Datum::Bool(v), DataType::Boolean(_)) => Ok(i32::from(*v)),
        (Datum::TinyInt(v), DataType::TinyInt(_)) => Ok(*v as i32),
        (Datum::SmallInt(v), DataType::SmallInt(_)) => Ok(*v as i32),
        (Datum::Int(v), DataType::Int(_)) => Ok(*v),
        (Datum::Long(v), DataType::BigInt(_)) => Ok(java_long_hash(*v)),
        (Datum::Float(v), DataType::Float(_)) => Ok(java_float_bits(*v) as i32),
        (Datum::Double(v), DataType::Double(_)) => Ok(java_long_hash(java_double_bits(*v) as i64)),
        (Datum::String(v), DataType::Char(_) | DataType::VarChar(_)) => {
            Ok(hive_hash_bytes(v.as_bytes()))
        }
        (Datum::Bytes(v), DataType::Binary(_) | DataType::VarBinary(_)) => Ok(hive_hash_bytes(v)),
        (
            Datum::Decimal {
                unscaled, scale, ..
            },
            DataType::Decimal(_),
        ) => {
            let (unscaled, scale) = normalize_decimal(*unscaled, *scale);
            Ok(java_big_decimal_hash(unscaled, scale))
        }
        (Datum::Date(v), DataType::Date(_)) => Ok(*v),
        (Datum::Time(v), DataType::Time(_)) => Ok(*v),
        _ => Err(crate::Error::Unsupported {
            message: format!("Unsupported type as bucket key type {data_type:?}"),
        }),
    }
}

fn hive_hash_bytes(bytes: &[u8]) -> i32 {
    bytes.iter().fold(0_i32, |hash, byte| {
        hash.wrapping_mul(31).wrapping_add(*byte as i8 as i32)
    })
}

fn java_long_hash(value: i64) -> i32 {
    let bits = value as u64;
    (bits ^ (bits >> 32)) as u32 as i32
}

fn java_float_bits(value: f32) -> u32 {
    if value == 0.0 {
        0
    } else if value.is_nan() {
        0x7fc0_0000
    } else {
        value.to_bits()
    }
}

fn java_double_bits(value: f64) -> u64 {
    if value == 0.0 {
        0
    } else if value.is_nan() {
        0x7ff8_0000_0000_0000
    } else {
        value.to_bits()
    }
}

fn normalize_decimal(mut unscaled: i128, mut scale: u32) -> (i128, u32) {
    if unscaled == 0 {
        return (0, 0);
    }
    while scale > 0 && unscaled % 10 == 0 {
        unscaled /= 10;
        scale -= 1;
    }
    (unscaled, scale)
}

fn java_big_decimal_hash(unscaled: i128, scale: u32) -> i32 {
    if let Ok(compact) = i64::try_from(unscaled) {
        if compact != i64::MIN {
            let val = if compact < 0 {
                compact.wrapping_neg() as u64
            } else {
                compact as u64
            };
            let temp = ((val >> 32) as i32)
                .wrapping_mul(31)
                .wrapping_add(val as u32 as i32);
            let signed_temp = if compact < 0 {
                temp.wrapping_neg()
            } else {
                temp
            };
            return signed_temp.wrapping_mul(31).wrapping_add(scale as i32);
        }
    }

    java_big_integer_hash(unscaled)
        .wrapping_mul(31)
        .wrapping_add(scale as i32)
}

fn java_big_integer_hash(value: i128) -> i32 {
    if value == 0 {
        return 0;
    }

    let sign = if value < 0 { -1_i32 } else { 1_i32 };
    let mut magnitude = value.unsigned_abs();
    let mut words = Vec::new();
    while magnitude != 0 {
        words.push((magnitude & 0xffff_ffff) as u32);
        magnitude >>= 32;
    }
    words.reverse();

    let hash = words.into_iter().fold(0_i32, |hash, word| {
        hash.wrapping_mul(31).wrapping_add(word as i32)
    });
    hash.wrapping_mul(sign)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{BigIntType, BooleanType, DecimalType, IntType, VarBinaryType, VarCharType};

    #[test]
    fn mod_bucket_uses_floor_mod_for_int_and_bigint() {
        let int_type = DataType::Int(IntType::new());
        let long_type = DataType::BigInt(BigIntType::new());

        assert_eq!(
            bucket_for_datums(
                &[(Some(&Datum::Int(-3)), &int_type)],
                BucketFunctionType::Mod,
                5,
            )
            .unwrap(),
            2
        );
        assert_eq!(
            bucket_for_datums(
                &[(Some(&Datum::Long(17)), &long_type)],
                BucketFunctionType::Mod,
                5,
            )
            .unwrap(),
            2
        );
    }

    #[test]
    fn hive_bucket_matches_java_reference_case() {
        let bool_type = DataType::Boolean(BooleanType::new());
        let int_type = DataType::Int(IntType::new());
        let string_type = DataType::VarChar(VarCharType::default());
        let bytes_type = DataType::VarBinary(VarBinaryType::default());
        let decimal_type = DataType::Decimal(DecimalType::new(10, 4).unwrap());

        let bucket = bucket_for_datums(
            &[
                (Some(&Datum::Bool(true)), &bool_type),
                (Some(&Datum::Int(7)), &int_type),
                (Some(&Datum::String("hello".into())), &string_type),
                (Some(&Datum::Bytes(vec![1, 2, 3])), &bytes_type),
                (
                    Some(&Datum::Decimal {
                        unscaled: 123400,
                        precision: 10,
                        scale: 4,
                    }),
                    &decimal_type,
                ),
            ],
            BucketFunctionType::Hive,
            8,
        )
        .unwrap();

        let expected_hash = 31_i32
            .wrapping_mul(
                31_i32
                    .wrapping_mul(
                        31_i32
                            .wrapping_mul(31_i32.wrapping_mul(1).wrapping_add(7))
                            .wrapping_add(99_162_322),
                    )
                    .wrapping_add(1_026),
            )
            .wrapping_add(38_256);
        assert_eq!(bucket, positive_mod(expected_hash, 8));
    }

    #[test]
    fn hive_decimal_hash_trims_trailing_zeros_like_big_decimal() {
        assert_eq!(normalize_decimal(123400, 4), (1234, 2));
        assert_eq!(normalize_decimal(0, 8), (0, 0));
    }
}
