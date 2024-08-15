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

use crate::error::*;
use crate::spec::DataField;
use bitflags::bitflags;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, FromInto, SerializeDisplay};
use std::fmt::{Debug, Display, Formatter};

bitflags! {
/// An enumeration of Data type families for clustering {@link DataTypeRoot}s into categories.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/types/DataTypeFamily.java>
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct DataTypeFamily: u32 {
        const PREDEFINED = 1 << 0;
        const CONSTRUCTED = 1 << 1;
        const CHARACTER_STRING = 1 << 2;
        const BINARY_STRING = 1 << 3;
        const NUMERIC = 1 << 4;
        const INTEGER_NUMERIC = 1 << 5;
        const EXACT_NUMERIC = 1 << 6;
        const APPROXIMATE_NUMERIC = 1 << 7;
        const DATETIME = 1 << 8;
        const TIME = 1 << 9;
        const TIMESTAMP = 1 << 10;
        const COLLECTION = 1 << 11;
        const EXTENSION = 1 << 12;
    }
}

/// Data type for paimon table.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/types/DataType.java#L45>
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DataType {
    /// Data type of a boolean with a (possibly) three-valued logic of `TRUE`, `FALSE`, `UNKNOWN`.
    Boolean(BooleanType),

    /// Data type of a 1-byte (2^8) signed integer with values from -128 to 127.
    TinyInt(TinyIntType),
    /// Data type of a 2-byte (2^16) signed integer with values from -32,768 to 32,767.
    SmallInt(SmallIntType),
    /// Data type of a 4-byte (2^32) signed integer with values from -2,147,483,648 to 2,147,483,647.
    Int(IntType),
    /// Data type of an 8-byte (2^64) signed integer with values from -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807.
    BigInt(BigIntType),
    /// Data type of a decimal number with fixed precision and scale.
    Decimal(DecimalType),
    /// Data type of an 8-byte double precision floating point number.
    Double(DoubleType),
    /// Data type of a 4-byte single precision floating point number.
    Float(FloatType),

    /// Data type of a fixed-length binary string (=a sequence of bytes).
    Binary(BinaryType),
    /// Data type of a variable-length binary string (=a sequence of bytes).
    VarBinary(VarBinaryType),
    /// Data type of a fixed-length character string.
    Char(CharType),
    /// Data type of a variable-length character string.
    VarChar(VarCharType),

    /// Data type of a date consisting of `year-month-day` with values ranging from `0000-01-01` to `9999-12-31`
    Date(DateType),
    /// Data type of a timestamp WITH LOCAL time zone consisting of `year-month-day hour:minute:second[.fractional] zone`.
    LocalZonedTimestamp(LocalZonedTimestampType),
    /// Data type of a time WITHOUT time zone consisting of `hour:minute:second[.fractional]` with
    /// up to nanosecond precision and values ranging from `00:00:00.000000000` to `23:59:59.999999999`.
    Time(TimeType),
    /// Data type of a timestamp WITHOUT time zone consisting of `year-month-day hour:minute:second[.fractional]` with up to nanosecond precision and values ranging from `0000-01-01 00:00:00.000000000` to `9999-12-31 23:59:59.999999999`.
    Timestamp(TimestampType),

    /// Data type of an array of elements with same subtype.
    Array(ArrayType),
    /// Data type of an associative array that maps keys `NULL` to values (including `NULL`).
    Map(MapType),
    /// Data type of a multiset (=bag). Unlike a set, it allows for multiple instances for each of its
    /// elements with a common subtype.
    Multiset(MultisetType),
    /// Data type of a sequence of fields. A field consists of a field name, field type, and an optional
    /// description.
    Row(RowType),
}

#[allow(dead_code)]
impl DataType {
    fn is_nullable(&self) -> bool {
        match self {
            DataType::Boolean(v) => v.nullable,
            DataType::TinyInt(v) => v.nullable,
            DataType::SmallInt(v) => v.nullable,
            DataType::Int(v) => v.nullable,
            DataType::BigInt(v) => v.nullable,
            DataType::Decimal(v) => v.nullable,
            DataType::Double(v) => v.nullable,
            DataType::Float(v) => v.nullable,
            DataType::Binary(v) => v.nullable,
            DataType::VarBinary(v) => v.nullable,
            DataType::Char(v) => v.nullable,
            DataType::VarChar(v) => v.nullable,
            DataType::Date(v) => v.nullable,
            DataType::LocalZonedTimestamp(v) => v.nullable,
            DataType::Time(v) => v.nullable,
            DataType::Timestamp(v) => v.nullable,
            DataType::Array(v) => v.nullable,
            DataType::Map(v) => v.nullable,
            DataType::Multiset(v) => v.nullable,
            DataType::Row(v) => v.nullable,
        }
    }
}

/// ArrayType for paimon.
///
/// Data type of an array of elements with same subtype.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/types/ArrayType.java>.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ArrayType {
    #[serde(rename = "type")]
    #[serde_as(as = "FromInto<serde_utils::NullableType<serde_utils::ARRAY>>")]
    nullable: bool,
    #[serde(rename = "element")]
    element_type: Box<DataType>,
}

impl ArrayType {
    pub fn new(element_type: DataType) -> Self {
        Self::with_nullable(true, element_type)
    }

    pub fn with_nullable(nullable: bool, element_type: DataType) -> Self {
        Self {
            nullable,
            element_type: Box::new(element_type),
        }
    }

    pub fn family(&self) -> DataTypeFamily {
        DataTypeFamily::CONSTRUCTED | DataTypeFamily::COLLECTION
    }
}

/// BigIntType for paimon.
///
/// Data type of an 8-byte (2^64) signed integer with values from -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/types/BigIntType.java>.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
#[serde(transparent)]
pub struct BigIntType {
    #[serde_as(as = "FromInto<serde_utils::NullableType<serde_utils::BIGINT>>")]
    nullable: bool,
}

impl Default for BigIntType {
    fn default() -> Self {
        Self::new()
    }
}

impl BigIntType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn family(&self) -> DataTypeFamily {
        DataTypeFamily::PREDEFINED
            | DataTypeFamily::NUMERIC
            | DataTypeFamily::INTEGER_NUMERIC
            | DataTypeFamily::EXACT_NUMERIC
    }
}

/// BinaryType for paimon.
///
/// Data type of a fixed-length binary string (=a sequence of bytes).
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/types/BinaryType.java>.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, SerializeDisplay, Hash)]
#[serde(rename_all = "camelCase")]
pub struct BinaryType {
    nullable: bool,
    length: usize,
}

impl Display for BinaryType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BINARY({})", self.length)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

impl Default for BinaryType {
    fn default() -> Self {
        Self::new(Self::DEFAULT_LENGTH).unwrap()
    }
}

impl BinaryType {
    pub const MIN_LENGTH: usize = 1;

    pub const MAX_LENGTH: usize = usize::MAX;

    pub const DEFAULT_LENGTH: usize = 1;

    pub fn new(length: usize) -> Result<Self, Error> {
        Self::with_nullable(true, length)
    }

    pub fn with_nullable(nullable: bool, length: usize) -> Result<Self, Error> {
        if length < Self::MIN_LENGTH {
            return DataTypeInvalidSnafu {
                message: "Binary string length must be at least 1.".to_string(),
            }
            .fail();
        }
        Ok(Self { nullable, length })
    }

    pub fn length(&self) -> usize {
        self.length
    }

    pub fn family(&self) -> DataTypeFamily {
        DataTypeFamily::PREDEFINED | DataTypeFamily::BINARY_STRING
    }
}

/// BooleanType for paimon.
///
/// Data type of a boolean with a (possibly) three-valued logic of `TRUE`, `FALSE`, `UNKNOWN`.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/master/paimon-common/src/release-0.8.2/java/org/apache/paimon/types/BooleanType.java>.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BooleanType {
    #[serde_as(as = "FromInto<serde_utils::NullableType<serde_utils::BOOLEAN>>")]
    nullable: bool,
}

impl Default for BooleanType {
    fn default() -> Self {
        Self::new()
    }
}

impl BooleanType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn family(&self) -> DataTypeFamily {
        DataTypeFamily::PREDEFINED
    }
}

/// CharType for paimon.
///
/// Data type of a fixed-length character string.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/types/CharType.java>.
#[derive(Debug, Clone, PartialEq, Hash, Eq, Deserialize, SerializeDisplay)]
#[serde(rename_all = "camelCase")]
pub struct CharType {
    nullable: bool,
    length: usize,
}

impl Display for CharType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CHAR({})", self.length)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

impl Default for CharType {
    fn default() -> Self {
        Self::new(Self::DEFAULT_LENGTH).unwrap()
    }
}

impl CharType {
    pub const DEFAULT_LENGTH: usize = 1;

    pub const MIN_LENGTH: usize = 1;

    pub const MAX_LENGTH: usize = 255;

    pub fn new(length: usize) -> Result<Self, Error> {
        Self::with_nullable(true, length)
    }

    pub fn with_nullable(nullable: bool, length: usize) -> Result<Self, Error> {
        if !(Self::MIN_LENGTH..=Self::MAX_LENGTH).contains(&length) {
            return DataTypeInvalidSnafu {
                message: "Char string length must be between 1 and 255.".to_string(),
            }
            .fail();
        }
        Ok(CharType { nullable, length })
    }

    pub fn length(&self) -> usize {
        self.length
    }

    pub fn family(&self) -> DataTypeFamily {
        DataTypeFamily::PREDEFINED | DataTypeFamily::CHARACTER_STRING
    }
}

/// DateType for paimon.
///
/// Data type of a date consisting of `year-month-day` with values ranging from `0000-01-01` to `9999-12-31`
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/types/DateType.java>.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Hash, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DateType {
    #[serde_as(as = "FromInto<serde_utils::NullableType<serde_utils::DATE>>")]
    nullable: bool,
}

impl Default for DateType {
    fn default() -> Self {
        Self::new()
    }
}

impl DateType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn family(&self) -> DataTypeFamily {
        DataTypeFamily::PREDEFINED | DataTypeFamily::DATETIME
    }
}

/// DecimalType for paimon.
///
/// Data type of a decimal number with fixed precision and scale.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/types/DecimalType.java>.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct DecimalType {
    nullable: bool,

    precision: u32,
    scale: u32,
}

impl Display for DecimalType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DECIMAL({}, {})", self.precision, self.scale)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

impl Default for DecimalType {
    fn default() -> Self {
        Self::new(Self::DEFAULT_PRECISION, Self::DEFAULT_SCALE).unwrap()
    }
}

impl DecimalType {
    pub const MIN_PRECISION: u32 = 1;

    pub const MAX_PRECISION: u32 = 38;

    pub const DEFAULT_PRECISION: u32 = 10;

    pub const MIN_SCALE: u32 = 0;

    pub const DEFAULT_SCALE: u32 = 0;

    pub fn new(precision: u32, scale: u32) -> Result<Self, Error> {
        Self::with_nullable(true, precision, scale)
    }

    pub fn with_nullable(nullable: bool, precision: u32, scale: u32) -> Result<Self, Error> {
        if !(Self::MIN_PRECISION..=Self::MAX_PRECISION).contains(&precision) {
            return DataTypeInvalidSnafu {
                message: format!(
                    "Decimal precision must be between {} and {} (both inclusive).",
                    Self::MIN_PRECISION,
                    Self::MAX_PRECISION
                ),
            }
            .fail();
        }

        if !(Self::MIN_SCALE..=precision).contains(&scale) {
            return DataTypeInvalidSnafu {
                message: format!(
                    "Decimal scale must be between {} and {} (both inclusive).",
                    Self::MIN_SCALE,
                    precision
                ),
            }
            .fail();
        }

        Ok(DecimalType {
            nullable,
            precision,
            scale,
        })
    }

    pub fn precision(&self) -> u32 {
        self.precision
    }

    pub fn scale(&self) -> u32 {
        self.scale
    }

    pub fn family(&self) -> DataTypeFamily {
        DataTypeFamily::PREDEFINED | DataTypeFamily::NUMERIC | DataTypeFamily::EXACT_NUMERIC
    }
}

/// DoubleType for paimon.
///
/// Data type of an 8-byte double precision floating point number.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/types/DoubleType.java>.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
#[serde(transparent)]
pub struct DoubleType {
    #[serde_as(as = "FromInto<serde_utils::NullableType<serde_utils::DOUBLE>>")]
    nullable: bool,
}

impl Display for DoubleType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DOUBLE")?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

impl Default for DoubleType {
    fn default() -> Self {
        Self::new()
    }
}

impl DoubleType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn family(&self) -> DataTypeFamily {
        DataTypeFamily::PREDEFINED | DataTypeFamily::NUMERIC | DataTypeFamily::APPROXIMATE_NUMERIC
    }
}

/// FloatType for paimon.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/types/FloatType.java>.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
#[serde(transparent)]
pub struct FloatType {
    #[serde_as(as = "FromInto<serde_utils::NullableType<serde_utils::FLOAT>>")]
    nullable: bool,
}

impl Default for FloatType {
    fn default() -> Self {
        Self::new()
    }
}

impl FloatType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn family(&self) -> DataTypeFamily {
        DataTypeFamily::PREDEFINED | DataTypeFamily::NUMERIC | DataTypeFamily::APPROXIMATE_NUMERIC
    }
}

/// IntType for paimon.
///
/// Data type of a 4-byte (2^32) signed integer with values from -2,147,483,648 to 2,147,483,647.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/types/IntType.java>.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IntType {
    #[serde_as(as = "FromInto<serde_utils::NullableType<serde_utils::INT>>")]
    nullable: bool,
}

impl Default for IntType {
    fn default() -> Self {
        Self::new()
    }
}

impl IntType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn family(&self) -> DataTypeFamily {
        DataTypeFamily::PREDEFINED
            | DataTypeFamily::NUMERIC
            | DataTypeFamily::INTEGER_NUMERIC
            | DataTypeFamily::EXACT_NUMERIC
    }
}

/// LocalZonedTimestampType for paimon.
///
/// Data type of a timestamp WITH LOCAL time zone consisting of `year-month-day hour:minute:second[.fractional] zone` with up to nanosecond precision and values ranging from `0000-01-01 00:00:00.000000000 +14:59` to `9999-12-31 23:59:59.999999999 -14:59`. Leap seconds (23:59:60 and 23:59:61) are not supported as the semantics are closer to a point in time than a wall-clock time.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/types/TimestampType.java>.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, SerializeDisplay, Hash)]
pub struct LocalZonedTimestampType {
    nullable: bool,
    precision: u32,
}

impl Display for LocalZonedTimestampType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TIMESTAMP WITH LOCAL TIME ZONE({})", self.precision)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

impl Default for LocalZonedTimestampType {
    fn default() -> Self {
        Self::new(Self::DEFAULT_PRECISION).unwrap()
    }
}

impl LocalZonedTimestampType {
    pub const MIN_PRECISION: u32 = TimestampType::MIN_PRECISION;

    pub const MAX_PRECISION: u32 = TimestampType::MAX_PRECISION;

    pub const DEFAULT_PRECISION: u32 = TimestampType::DEFAULT_PRECISION;

    pub fn new(precision: u32) -> Result<Self, Error> {
        Self::with_nullable(true, precision)
    }

    pub fn with_nullable(nullable: bool, precision: u32) -> Result<Self, Error> {
        if !(Self::MIN_PRECISION..=Self::MAX_PRECISION).contains(&precision) {
            return DataTypeInvalidSnafu {
                message: format!(
                    "LocalZonedTimestamp precision must be between {} and {} (both inclusive).",
                    Self::MIN_PRECISION,
                    Self::MAX_PRECISION
                ),
            }
            .fail();
        }

        Ok(LocalZonedTimestampType {
            nullable,
            precision,
        })
    }

    pub fn precision(&self) -> u32 {
        self.precision
    }

    pub fn family(&self) -> DataTypeFamily {
        DataTypeFamily::PREDEFINED
            | DataTypeFamily::DATETIME
            | DataTypeFamily::TIMESTAMP
            | DataTypeFamily::EXTENSION
    }
}

/// SmallIntType for paimon.
///
/// Data type of a 2-byte (2^16) signed integer with values from -32,768 to 32,767.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/types/SmallIntType.java>.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
#[serde(transparent)]
pub struct SmallIntType {
    #[serde_as(as = "FromInto<serde_utils::NullableType<serde_utils::SMALLINT>>")]
    nullable: bool,
}

impl Default for SmallIntType {
    fn default() -> Self {
        Self::new()
    }
}

impl SmallIntType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn family(&self) -> DataTypeFamily {
        DataTypeFamily::PREDEFINED
            | DataTypeFamily::NUMERIC
            | DataTypeFamily::INTEGER_NUMERIC
            | DataTypeFamily::EXACT_NUMERIC
    }
}

/// TimeType for paimon.
///
/// Data type of a time WITHOUT time zone consisting of `hour:minute:second[.fractional]` with
/// up to nanosecond precision and values ranging from `00:00:00.000000000` to `23:59:59.999999999`.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/types/TimeType.java>.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, SerializeDisplay, Hash)]
pub struct TimeType {
    nullable: bool,
    precision: u32,
}

impl Display for TimeType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TIME({})", self.precision)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

impl Default for TimeType {
    fn default() -> Self {
        Self::new(TimeType::DEFAULT_PRECISION).unwrap()
    }
}

impl TimeType {
    pub const MIN_PRECISION: u32 = 0;

    pub const MAX_PRECISION: u32 = 9;

    pub const DEFAULT_PRECISION: u32 = 0;

    pub fn new(precision: u32) -> Result<Self, Error> {
        Self::with_nullable(true, precision)
    }

    pub fn with_nullable(nullable: bool, precision: u32) -> Result<Self, Error> {
        if !(Self::MIN_PRECISION..=Self::MAX_PRECISION).contains(&precision) {
            return DataTypeInvalidSnafu {
                message: format!(
                    "Time precision must be between {} and {} (both inclusive).",
                    Self::MIN_PRECISION,
                    Self::MAX_PRECISION
                ),
            }
            .fail();
        }

        Ok(TimeType {
            nullable,
            precision,
        })
    }

    pub fn precision(&self) -> u32 {
        self.precision
    }

    pub fn family(&self) -> DataTypeFamily {
        DataTypeFamily::PREDEFINED | DataTypeFamily::DATETIME | DataTypeFamily::TIME
    }
}

/// TimestampType for paimon.
///
/// Data type of a timestamp WITHOUT time zone consisting of `year-month-day hour:minute:second[.fractional]` with up to nanosecond precision and values ranging from `0000-01-01 00:00:00.000000000` to `9999-12-31 23:59:59.999999999`.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/types/TimestampType.java>.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, SerializeDisplay, Hash)]
pub struct TimestampType {
    nullable: bool,
    precision: u32,
}

impl Display for TimestampType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TIMESTAMP({})", self.precision)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

impl Default for TimestampType {
    fn default() -> Self {
        Self::new(Self::DEFAULT_PRECISION).unwrap()
    }
}

impl TimestampType {
    pub const MIN_PRECISION: u32 = 0;

    pub const MAX_PRECISION: u32 = 9;

    pub const DEFAULT_PRECISION: u32 = 6;

    pub fn new(precision: u32) -> Result<Self, Error> {
        Self::with_nullable(true, precision)
    }

    pub fn with_nullable(nullable: bool, precision: u32) -> Result<Self, Error> {
        if !(Self::MIN_PRECISION..=Self::MAX_PRECISION).contains(&precision) {
            return DataTypeInvalidSnafu {
                message: format!(
                    "Timestamp precision must be between {} and {} (both inclusive).",
                    Self::MIN_PRECISION,
                    Self::MAX_PRECISION
                ),
            }
            .fail();
        }

        Ok(TimestampType {
            nullable,
            precision,
        })
    }

    pub fn precision(&self) -> u32 {
        self.precision
    }

    pub fn family(&self) -> DataTypeFamily {
        DataTypeFamily::PREDEFINED | DataTypeFamily::DATETIME | DataTypeFamily::TIMESTAMP
    }
}

/// TinyIntType for paimon.
///
/// Data type of a 1-byte signed integer with values from -128 to 127.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/master/paimon-common/src/release-0.8.2/java/org/apache/paimon/types/TinyIntType.java>.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
#[serde(transparent)]
pub struct TinyIntType {
    #[serde_as(as = "FromInto<serde_utils::NullableType<serde_utils::TINYINT>>")]
    nullable: bool,
}

impl Default for TinyIntType {
    fn default() -> Self {
        Self::new()
    }
}

impl TinyIntType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn family(&self) -> DataTypeFamily {
        DataTypeFamily::PREDEFINED
            | DataTypeFamily::NUMERIC
            | DataTypeFamily::INTEGER_NUMERIC
            | DataTypeFamily::EXACT_NUMERIC
    }
}

/// VarBinaryType for paimon.
///
/// Data type of a variable-length binary string (=a sequence of bytes).
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/types/VarBinaryType.java>.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, SerializeDisplay, Hash)]
pub struct VarBinaryType {
    nullable: bool,
    length: u32,
}

impl Display for VarBinaryType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "VARBINARY({})", self.length)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

impl Default for VarBinaryType {
    fn default() -> Self {
        Self::new(Self::DEFAULT_LENGTH).unwrap()
    }
}

impl VarBinaryType {
    pub const MIN_LENGTH: u32 = 1;

    pub const MAX_LENGTH: u32 = isize::MAX as u32;

    pub const DEFAULT_LENGTH: u32 = 1;

    pub fn new(length: u32) -> Result<Self, Error> {
        Self::try_new(true, length)
    }

    pub fn try_new(nullable: bool, length: u32) -> Result<Self, Error> {
        if length < Self::MIN_LENGTH {
            return DataTypeInvalidSnafu {
                message: "VarBinary string length must be at least 1.".to_string(),
            }
            .fail();
        }

        Ok(VarBinaryType { nullable, length })
    }

    pub fn length(&self) -> u32 {
        self.length
    }

    pub fn family(&self) -> DataTypeFamily {
        DataTypeFamily::PREDEFINED | DataTypeFamily::BINARY_STRING
    }
}

/// VarCharType for paimon.
///
/// Data type of a variable-length character string.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/types/VarCharType.java>.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, SerializeDisplay, Hash)]
pub struct VarCharType {
    nullable: bool,
    length: u32,
}

impl Display for VarCharType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "VARCHAR({})", self.length)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

impl Default for VarCharType {
    fn default() -> Self {
        Self::new(Self::DEFAULT_LENGTH).unwrap()
    }
}

impl VarCharType {
    pub const MIN_LENGTH: u32 = 1;

    pub const MAX_LENGTH: u32 = isize::MAX as u32;

    pub const DEFAULT_LENGTH: u32 = 1;

    pub fn new(length: u32) -> Result<Self, Error> {
        Self::with_nullable(true, length)
    }

    pub fn with_nullable(nullable: bool, length: u32) -> Result<Self, Error> {
        if !(Self::MIN_LENGTH..=Self::MAX_LENGTH).contains(&length) {
            return DataTypeInvalidSnafu {
                message: format!(
                    "VarChar string length must be between {} and {} (both inclusive).",
                    Self::MIN_LENGTH,
                    Self::MAX_LENGTH
                ),
            }
            .fail();
        }

        Ok(VarCharType { nullable, length })
    }

    pub fn length(&self) -> u32 {
        self.length
    }

    pub fn family(&self) -> DataTypeFamily {
        DataTypeFamily::PREDEFINED | DataTypeFamily::CHARACTER_STRING
    }
}

/// MapType for paimon.
///
/// Data type of an associative array that maps keys `NULL` to values (including `NULL`).
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/types/MapType.java>.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct MapType {
    #[serde(rename = "type")]
    #[serde_as(as = "FromInto<serde_utils::NullableType<serde_utils::MAP>>")]
    nullable: bool,
    #[serde(rename = "key")]
    key_type: Box<DataType>,
    #[serde(rename = "value")]
    value_type: Box<DataType>,
}

impl MapType {
    pub fn new(key_type: DataType, value_type: DataType) -> Self {
        Self::with_nullable(true, key_type, value_type)
    }

    pub fn with_nullable(nullable: bool, key_type: DataType, value_type: DataType) -> Self {
        Self {
            nullable,
            key_type: Box::new(key_type),
            value_type: Box::new(value_type),
        }
    }

    pub fn family(&self) -> DataTypeFamily {
        DataTypeFamily::CONSTRUCTED | DataTypeFamily::COLLECTION
    }
}

/// MultisetType for paimon.
///
/// Data type of a multiset (=bag). Unlike a set, it allows for multiple instances for each of its
/// elements with a common subtype.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/types/MultisetType.java>.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct MultisetType {
    #[serde(rename = "type")]
    #[serde_as(as = "FromInto<serde_utils::NullableType<serde_utils::MULTISET>>")]
    nullable: bool,
    #[serde(rename = "element")]
    element_type: Box<DataType>,
}

impl MultisetType {
    pub fn new(element_type: DataType) -> Self {
        Self::with_nullable(true, element_type)
    }

    pub fn with_nullable(nullable: bool, element_type: DataType) -> Self {
        Self {
            nullable,
            element_type: Box::new(element_type),
        }
    }

    pub fn family(&self) -> DataTypeFamily {
        DataTypeFamily::CONSTRUCTED | DataTypeFamily::COLLECTION
    }
}

/// RowType for paimon.
///
/// Data type of a sequence of fields. A field consists of a field name, field type, and an optional
/// description. The most specific type of a row of a table is a row type. In this case, each column
/// of the row corresponds to the field of the row type that has the same ordinal position as the
/// column.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/types/RowType.java>.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct RowType {
    #[serde(rename = "type")]
    #[serde_as(as = "FromInto<serde_utils::NullableType<serde_utils::ROW>>")]
    nullable: bool,
    fields: Vec<DataField>,
}

impl RowType {
    pub const fn new(fields: Vec<DataField>) -> Self {
        Self::with_nullable(true, fields)
    }

    pub const fn with_nullable(nullable: bool, fields: Vec<DataField>) -> Self {
        Self { nullable, fields }
    }

    pub fn family(&self) -> DataTypeFamily {
        DataTypeFamily::CONSTRUCTED
    }
}

mod serde_utils {
    // We use name like `BOOLEAN` by design to avoid conflict.
    #![allow(clippy::upper_case_acronyms)]

    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::marker::PhantomData;

    pub trait DataTypeName {
        const NAME: &'static str;
    }

    pub struct BOOLEAN;
    impl DataTypeName for BOOLEAN {
        const NAME: &'static str = "BOOLEAN";
    }

    pub struct ARRAY;
    impl DataTypeName for ARRAY {
        const NAME: &'static str = "ARRAY";
    }

    pub struct DATE;
    impl DataTypeName for DATE {
        const NAME: &'static str = "DATE";
    }

    pub struct DOUBLE;
    impl DataTypeName for DOUBLE {
        const NAME: &'static str = "DOUBLE";
    }

    pub struct FLOAT;
    impl DataTypeName for FLOAT {
        const NAME: &'static str = "FLOAT";
    }

    pub struct INT;
    impl DataTypeName for INT {
        const NAME: &'static str = "INT";
    }

    pub struct BIGINT;
    impl DataTypeName for BIGINT {
        const NAME: &'static str = "BIGINT";
    }

    pub struct SMALLINT;
    impl DataTypeName for SMALLINT {
        const NAME: &'static str = "SMALLINT";
    }

    pub struct TINYINT;
    impl DataTypeName for TINYINT {
        const NAME: &'static str = "TINYINT";
    }

    pub struct MAP;
    impl DataTypeName for MAP {
        const NAME: &'static str = "MAP";
    }

    pub struct MULTISET;
    impl DataTypeName for MULTISET {
        const NAME: &'static str = "MULTISET";
    }

    pub struct ROW;
    impl DataTypeName for ROW {
        const NAME: &'static str = "ROW";
    }

    pub struct NullableType<T: DataTypeName> {
        nullable: bool,
        value: PhantomData<T>,
    }

    impl<T: DataTypeName> From<bool> for NullableType<T> {
        fn from(value: bool) -> Self {
            Self {
                nullable: value,
                value: PhantomData,
            }
        }
    }
    impl<T: DataTypeName> From<NullableType<T>> for bool {
        fn from(value: NullableType<T>) -> Self {
            value.nullable
        }
    }

    impl<T: DataTypeName> Serialize for NullableType<T> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            if self.nullable {
                serializer.serialize_str(T::NAME)
            } else {
                serializer.serialize_str(&format!("{} NOT NULL", T::NAME))
            }
        }
    }

    /// TODO: we should support more edge cases.
    impl<'de, T: DataTypeName> Deserialize<'de> for NullableType<T> {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            let s = String::deserialize(deserializer)?;

            let (name, nullable) = s.split_once(" ").unwrap_or((s.as_str(), ""));

            if name == T::NAME && nullable.is_empty() {
                Ok(NullableType::from(true))
            } else if name == T::NAME && nullable == "NOT NULL" {
                Ok(NullableType::from(false))
            } else {
                let expect = format!("{} or {} NOT NULL", T::NAME, T::NAME);
                Err(serde::de::Error::invalid_value(
                    serde::de::Unexpected::Str(s.as_str()),
                    &expect.as_str(),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    fn load_fixture(name: &str) -> String {
        let workdir =
            std::env::current_dir().unwrap_or_else(|err| panic!("current_dir must exist: {err}"));
        let path = workdir.join(format!("tests/fixtures/{name}.json"));

        let content = std::fs::read(&path)
            .unwrap_or_else(|err| panic!("fixtures {path:?} load failed: {err}"));
        String::from_utf8(content).expect("fixtures content must be valid utf8")
    }

    fn test_cases() -> Vec<(&'static str, DataType)> {
        vec![
            (
                "array_type",
                DataType::Array(ArrayType {
                    nullable: false,
                    element_type: DataType::Int(IntType::with_nullable(false)).into(),
                }),
            ),
            (
                "array_type_nullable",
                DataType::Array(ArrayType {
                    nullable: true,
                    element_type: DataType::Int(IntType::with_nullable(true)).into(),
                }),
            ),
            (
                "bigint_type",
                DataType::BigInt(BigIntType { nullable: false }),
            ),
            (
                "bigint_type_nullable",
                DataType::BigInt(BigIntType { nullable: true }),
            ),
            // FIXME: binary doesn't implement deserialize.
            // (
            //     "binary_type",
            //     DataType::Binary(BinaryType {
            //         nullable: false,
            //         length: 22,
            //     }),
            // ),
            // (
            //     "binary_type_nullable",
            //     DataType::Binary(BinaryType {
            //         nullable: true,
            //         length: 22,
            //     }),
            // ),
            (
                "boolean_type",
                DataType::Boolean(BooleanType { nullable: false }),
            ),
            (
                "boolean_type_nullable",
                DataType::Boolean(BooleanType { nullable: true }),
            ),
            // FIXME: binary doesn't implement deserialize.
            // (
            //     "char_type",
            //     DataType::Char(CharType {
            //         nullable: false,
            //         length: 33,
            //     }),
            // ),
            // (
            //     "char_type_nullable",
            //     DataType::Char(CharType {
            //         nullable: true,
            //         length: 33,
            //     }),
            // ),
            ("date_type", DataType::Date(DateType { nullable: false })),
            (
                "date_type_nullable",
                DataType::Date(DateType { nullable: true }),
            ),
            // FIXME: DecimalType serialize failed.
            // (
            //     "decimal_type",
            //     DataType::Decimal(DecimalType {
            //         nullable: false,
            //         precision: 10,
            //         scale: 2,
            //     }),
            // ),
            // (
            //     "decimal_type_nullable",
            //     DataType::Decimal(DecimalType {
            //         nullable: true,
            //         precision: 10,
            //         scale: 2,
            //     }),
            // ),
            (
                "double_type",
                DataType::Double(DoubleType { nullable: false }),
            ),
            (
                "double_type_nullable",
                DataType::Double(DoubleType { nullable: true }),
            ),
            ("float_type", DataType::Float(FloatType { nullable: false })),
            (
                "float_type_nullable",
                DataType::Float(FloatType { nullable: true }),
            ),
            ("int_type", DataType::Int(IntType { nullable: false })),
            (
                "int_type_nullable",
                DataType::Int(IntType { nullable: true }),
            ),
            // FIXME: LocalZonedTimestampType serialize failed.
            // (
            //     "local_zoned_timestamp_type",
            //     DataType::LocalZonedTimestamp(LocalZonedTimestampType {
            //         nullable: false,
            //         precision: 3,
            //     }),
            // ),
            // (
            //     "local_zoned_timestamp_type_nullable",
            //     DataType::LocalZonedTimestamp(LocalZonedTimestampType {
            //         nullable: true,
            //         precision: 3,
            //     }),
            // ),
            // FIXME: VarCharType doesn't support deserialize.
            // (
            //     "map_type",
            //     DataType::Map(MapType {
            //         nullable: false,
            //         key_type: DataType::VarChar(VarCharType {
            //             nullable: true,
            //             length: 20,
            //         })
            //         .into(),
            //         value_type: DataType::Int(IntType { nullable: false }).into(),
            //     }),
            // ),
            // (
            //     "map_type_nullable",
            //     DataType::Map(MapType {
            //         nullable: true,
            //         key_type: DataType::VarChar(VarCharType {
            //             nullable: true,
            //             length: 20,
            //         })
            //         .into(),
            //         value_type: DataType::Int(IntType { nullable: true }).into(),
            //     }),
            // ),
            (
                "multiset_type",
                DataType::Multiset(MultisetType {
                    nullable: false,
                    element_type: DataType::Int(IntType { nullable: false }).into(),
                }),
            ),
            (
                "multiset_type_nullable",
                DataType::Multiset(MultisetType {
                    nullable: true,
                    element_type: DataType::Int(IntType { nullable: true }).into(),
                }),
            ),
            // FIXME: VarChar doesn't support deserialize.
            // (
            //     "row_type",
            //     DataType::Row(RowType {
            //         nullable: false,
            //         fields: vec![
            //             DataField::new(0, "a".into(), DataType::Int(IntType { nullable: false })),
            //             DataField::new(
            //                 1,
            //                 "b".into(),
            //                 DataType::VarChar(VarCharType {
            //                     nullable: false,
            //                     length: 20,
            //                 }),
            //             ),
            //         ],
            //     }),
            // ),
            // (
            //     "row_type_nullable",
            //     DataType::Row(RowType {
            //         nullable: true,
            //         fields: vec![
            //             DataField::new(0, "a".into(), DataType::Int(IntType { nullable: true })),
            //             DataField::new(
            //                 1,
            //                 "b".into(),
            //                 DataType::VarChar(VarCharType {
            //                     nullable: true,
            //                     length: 20,
            //                 }),
            //             ),
            //         ],
            //     }),
            // ),
            (
                "smallint_type",
                DataType::SmallInt(SmallIntType { nullable: false }),
            ),
            (
                "smallint_type_nullable",
                DataType::SmallInt(SmallIntType { nullable: true }),
            ),
            // FIXME: time and timestamp doesn't implement deserialize.
            // (
            //     "time_type",
            //     DataType::Time(TimeType {
            //         nullable: false,
            //         precision: 9,
            //     }),
            // ),
            // (
            //     "time_type_nullable",
            //     DataType::Time(TimeType {
            //         nullable: true,
            //         precision: 0,
            //     }),
            // ),
            // (
            //     "timestamp_type",
            //     DataType::Timestamp(TimestampType {
            //         nullable: false,
            //         precision: 6,
            //     }),
            // ),
            // (
            //     "timestamp_type_nullable",
            //     DataType::Timestamp(TimestampType {
            //         nullable: true,
            //         precision: 6,
            //     }),
            // ),
            (
                "tinyint_type",
                DataType::TinyInt(TinyIntType { nullable: false }),
            ),
            (
                "tinyint_type_nullable",
                DataType::TinyInt(TinyIntType { nullable: true }),
            ),
            // FIXME: varbinary & varchar doesn't implement deserialize.
            // (
            //     "varbinary_type",
            //     DataType::VarBinary(VarBinaryType {
            //         nullable: false,
            //         length: 233,
            //     }),
            // ),
            // (
            //     "varbinary_type_nullable",
            //     DataType::VarBinary(VarBinaryType {
            //         nullable: true,
            //         length: 233,
            //     }),
            // ),
            // (
            //     "varchar_type",
            //     DataType::VarChar(VarCharType {
            //         nullable: false,
            //         length: 33,
            //     }),
            // ),
            // (
            //     "varchar_type_nullable",
            //     DataType::VarChar(VarCharType {
            //         nullable: true,
            //         length: 33,
            //     }),
            // ),
        ]
    }

    /// Test data type serialize against with test fixtures.
    ///
    /// The name is the test fixtures file name.
    #[test]
    fn test_data_type_serialize() {
        for (name, input) in test_cases() {
            let actual = serde_json::to_string(&input)
                .unwrap_or_else(|err| panic!("serialize failed for {name}: {err}"));

            assert_eq!(
                actual,
                load_fixture(name),
                "test data type serialize for {name}"
            )
        }
    }

    /// Test data type serialize against with test fixtures.
    ///
    /// The name is the test fixtures file name.
    #[test]
    fn test_data_type_deserialize() {
        for (name, expect) in test_cases() {
            let actual = serde_json::from_str::<DataType>(&load_fixture(name))
                .unwrap_or_else(|err| panic!("deserialize failed for {name}: {err}"));

            assert_eq!(actual, expect, "test data type deserialize for {name}")
        }
    }
}
