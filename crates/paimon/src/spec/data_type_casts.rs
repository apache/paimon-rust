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

//! Utilities for casting [`DataType`], mirroring Java `DataTypeCasts`.
//!
//! Like Java, the rules are defined at type-root granularity: two types with
//! the same root (e.g. two `DECIMAL`s with different precision) are always
//! implicitly castable, and parameters of the target type are not inspected.

use crate::spec::DataType;

/// Returns whether the source type can be cast to the target type, mirroring
/// Java `DataTypeCasts.supportsCast(sourceType, targetType, allowExplicit)`.
///
/// With `allow_explicit` false, only implicit casts are considered: type
/// widening and generalization that never lose information. With
/// `allow_explicit` true, casts corresponding to the SQL `CAST` specification
/// are also allowed (e.g. `BIGINT` to `INT`, or most predefined types to
/// character strings).
pub(crate) fn supports_cast(source: &DataType, target: &DataType, allow_explicit: bool) -> bool {
    // A NOT NULL type cannot store a NULL type, but it might be useful to
    // cast explicitly with knowledge about the data.
    if source.is_nullable() && !target.is_nullable() && !allow_explicit {
        return false;
    }
    // Ignore nullability during compare.
    match (
        source.copy_with_nullable(true),
        target.copy_with_nullable(true),
    ) {
        (Ok(s), Ok(t)) if s == t => return true,
        (Err(_), _) | (_, Err(_)) => return false,
        _ => {}
    }
    if implicit_cast_supported(source, target) {
        return true;
    }
    allow_explicit && explicit_cast_supported(source, target)
}

fn same_root(a: &DataType, b: &DataType) -> bool {
    std::mem::discriminant(a) == std::mem::discriminant(b)
}

/// Java `DataTypeFamily.CHARACTER_STRING`.
fn is_character_string(t: &DataType) -> bool {
    matches!(t, DataType::Char(_) | DataType::VarChar(_))
}

/// Java `DataTypeFamily.BINARY_STRING`.
fn is_binary_string(t: &DataType) -> bool {
    matches!(t, DataType::Binary(_) | DataType::VarBinary(_))
}

/// Java `DataTypeFamily.INTEGER_NUMERIC`.
fn is_integer_numeric(t: &DataType) -> bool {
    matches!(
        t,
        DataType::TinyInt(_) | DataType::SmallInt(_) | DataType::Int(_) | DataType::BigInt(_)
    )
}

/// Java `DataTypeFamily.NUMERIC`.
fn is_numeric(t: &DataType) -> bool {
    is_integer_numeric(t)
        || matches!(
            t,
            DataType::Decimal(_) | DataType::Float(_) | DataType::Double(_)
        )
}

/// Java `DataTypeFamily.TIMESTAMP`.
fn is_timestamp(t: &DataType) -> bool {
    matches!(t, DataType::Timestamp(_) | DataType::LocalZonedTimestamp(_))
}

/// Java `DataTypeFamily.DATETIME`.
fn is_datetime(t: &DataType) -> bool {
    is_timestamp(t) || matches!(t, DataType::Date(_) | DataType::Time(_))
}

/// Mirrors the `implicitCastingRules` table in Java `DataTypeCasts`. Every
/// root is implicitly castable from itself.
fn implicit_cast_supported(source: &DataType, target: &DataType) -> bool {
    if same_root(source, target) {
        return true;
    }
    match target {
        DataType::VarChar(_) => is_character_string(source),
        DataType::VarBinary(_) => is_binary_string(source),
        DataType::Decimal(_) | DataType::Double(_) => is_numeric(source),
        DataType::SmallInt(_) => matches!(source, DataType::TinyInt(_)),
        DataType::Int(_) => matches!(source, DataType::TinyInt(_) | DataType::SmallInt(_)),
        DataType::BigInt(_) => matches!(
            source,
            DataType::TinyInt(_) | DataType::SmallInt(_) | DataType::Int(_)
        ),
        DataType::Float(_) => matches!(
            source,
            DataType::TinyInt(_)
                | DataType::SmallInt(_)
                | DataType::Int(_)
                | DataType::BigInt(_)
                | DataType::Decimal(_)
        ),
        DataType::Date(_) | DataType::Time(_) => matches!(source, DataType::Timestamp(_)),
        DataType::Timestamp(_) => matches!(source, DataType::LocalZonedTimestamp(_)),
        DataType::LocalZonedTimestamp(_) => matches!(source, DataType::Timestamp(_)),
        _ => false,
    }
}

/// Mirrors the `explicitCastingRules` table in Java `DataTypeCasts`.
fn explicit_cast_supported(source: &DataType, target: &DataType) -> bool {
    match target {
        // PREDEFINED and CONSTRUCTED cover every type root.
        DataType::Char(_) | DataType::VarChar(_) => true,
        DataType::Boolean(_) => is_character_string(source) || is_integer_numeric(source),
        DataType::Binary(_) => {
            is_character_string(source) || matches!(source, DataType::VarBinary(_))
        }
        DataType::VarBinary(_) => {
            is_character_string(source) || matches!(source, DataType::Binary(_))
        }
        DataType::Decimal(_) => {
            is_character_string(source)
                || matches!(source, DataType::Boolean(_))
                || is_timestamp(source)
        }
        DataType::TinyInt(_)
        | DataType::SmallInt(_)
        | DataType::Int(_)
        | DataType::BigInt(_)
        | DataType::Float(_)
        | DataType::Double(_) => {
            is_numeric(source)
                || is_character_string(source)
                || matches!(source, DataType::Boolean(_))
                || is_timestamp(source)
        }
        DataType::Date(_) => is_timestamp(source) || is_character_string(source),
        DataType::Time(_) => {
            matches!(source, DataType::Time(_))
                || is_timestamp(source)
                || is_character_string(source)
        }
        DataType::Timestamp(_) | DataType::LocalZonedTimestamp(_) => {
            is_datetime(source) || is_character_string(source) || is_numeric(source)
        }
        DataType::Blob(_)
        | DataType::Array(_)
        | DataType::Map(_)
        | DataType::Multiset(_)
        | DataType::Row(_)
        | DataType::Vector(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{
        ArrayType, BigIntType, BooleanType, CharType, DataType, DateType, DecimalType, DoubleType,
        IntType, TimestampType, VarCharType,
    };

    fn int() -> DataType {
        DataType::Int(IntType::new())
    }

    fn bigint() -> DataType {
        DataType::BigInt(BigIntType::new())
    }

    fn varchar() -> DataType {
        DataType::VarChar(VarCharType::new(10).unwrap())
    }

    fn timestamp() -> DataType {
        DataType::Timestamp(TimestampType::new(3).unwrap())
    }

    #[test]
    fn test_identity_and_same_root() {
        assert!(supports_cast(&int(), &int(), false));
        // Same root with different parameters is implicitly castable.
        assert!(supports_cast(
            &DataType::Decimal(DecimalType::new(10, 2).unwrap()),
            &DataType::Decimal(DecimalType::new(20, 4).unwrap()),
            false
        ));
        assert!(supports_cast(
            &varchar(),
            &DataType::VarChar(VarCharType::new(20).unwrap()),
            false
        ));
    }

    #[test]
    fn test_implicit_widening() {
        assert!(supports_cast(&int(), &bigint(), false));
        assert!(supports_cast(
            &int(),
            &DataType::Double(DoubleType::new()),
            false
        ));
        assert!(supports_cast(
            &int(),
            &DataType::Decimal(DecimalType::new(10, 2).unwrap()),
            false
        ));
        assert!(supports_cast(
            &timestamp(),
            &DataType::Date(DateType::new()),
            false
        ));
        assert!(supports_cast(
            &DataType::Char(CharType::new(5).unwrap()),
            &varchar(),
            false
        ));
    }

    #[test]
    fn test_narrowing_requires_explicit() {
        assert!(!supports_cast(&bigint(), &int(), false));
        assert!(supports_cast(&bigint(), &int(), true));
        assert!(!supports_cast(&varchar(), &int(), false));
        assert!(supports_cast(&varchar(), &int(), true));
        assert!(!supports_cast(&int(), &varchar(), false));
        assert!(supports_cast(&int(), &varchar(), true));
    }

    #[test]
    fn test_nullability_rule() {
        let nullable_int = int();
        let not_null_int = int().copy_with_nullable(false).unwrap();
        // Nullable to NOT NULL is only allowed explicitly.
        assert!(!supports_cast(&nullable_int, &not_null_int, false));
        assert!(supports_cast(&nullable_int, &not_null_int, true));
        assert!(supports_cast(&not_null_int, &nullable_int, false));
    }

    #[test]
    fn test_unsupported_casts() {
        let array = DataType::Array(ArrayType::new(int()));
        assert!(!supports_cast(&int(), &array, true));
        assert!(!supports_cast(&array, &int(), true));
        // Any type can be cast to a character string explicitly, even arrays.
        assert!(supports_cast(&array, &varchar(), true));
        assert!(!supports_cast(&array, &varchar(), false));
        assert!(!supports_cast(
            &timestamp(),
            &DataType::Boolean(BooleanType::new()),
            true
        ));
    }
}
