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
use serde_with::{serde_as, DisplayFromStr};
use std::fmt::{Debug, Display, Formatter};
use std::str::FromStr;

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
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
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

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Boolean(v) => write!(f, "{v}"),
            DataType::TinyInt(v) => write!(f, "{v}"),
            DataType::SmallInt(v) => write!(f, "{v}"),
            DataType::Int(v) => write!(f, "{v}"),
            DataType::BigInt(v) => write!(f, "{v}"),
            DataType::Decimal(v) => write!(f, "{v}"),
            DataType::Double(v) => write!(f, "{v}"),
            DataType::Float(v) => write!(f, "{v}"),
            DataType::Binary(v) => write!(f, "{v}"),
            DataType::VarBinary(v) => write!(f, "{v}"),
            DataType::Char(v) => write!(f, "{v}"),
            DataType::VarChar(v) => write!(f, "{v}"),
            DataType::Date(v) => write!(f, "{v}"),
            DataType::LocalZonedTimestamp(v) => write!(f, "{v}"),
            DataType::Time(v) => write!(f, "{v}"),
            DataType::Timestamp(v) => write!(f, "{v}"),
            DataType::Array(v) => write!(f, "{v}"),
            DataType::Map(v) => write!(f, "{v}"),
            DataType::Multiset(v) => write!(f, "{v}"),
            DataType::Row(v) => write!(f, "{v}"),
        }
    }
}

impl FromStr for DataType {
    type Err = Error;

    fn from_str(v: &str) -> Result<Self, Self::Err> {
        let v: String = v.to_uppercase();
        match v.as_str() {
            "BOOLEAN" => Ok(DataType::Boolean(BooleanType::new())),
            "TINYINT" => Ok(DataType::TinyInt(TinyIntType::new())),
            "SMALLINT" => Ok(DataType::SmallInt(SmallIntType::new())),
            "INTEGER" => Ok(DataType::Int(IntType::new())),
            "BIGINT" => Ok(DataType::BigInt(BigIntType::new())),
            "DECIMAL" => Ok(DataType::Decimal(DecimalType::new(
                DecimalType::DEFAULT_PRECISION,
                DecimalType::DEFAULT_SCALE,
            )?)),
            "DOUBLE" => Ok(DataType::Double(DoubleType::new())),
            "FLOAT" => Ok(DataType::Float(FloatType::new())),
            "BINARY" => Ok(DataType::Binary(BinaryType::new(
                BinaryType::DEFAULT_LENGTH,
            )?)),
            "VARBINARY" => Ok(DataType::VarBinary(VarBinaryType::new(
                VarBinaryType::DEFAULT_LENGTH,
            )?)),
            "CHAR" => Ok(DataType::Char(CharType::new(CharType::DEFAULT_LENGTH)?)),
            "VARCHAR" => Ok(DataType::VarChar(VarCharType::new(
                VarCharType::DEFAULT_LENGTH,
            )?)),
            "DATE" => Ok(DataType::Date(DateType::new())),
            "TIMESTAMP" => Ok(DataType::Timestamp(TimestampType::new(
                TimestampType::DEFAULT_PRECISION,
            )?)),
            "TIME" => Ok(DataType::Time(TimeType::new(TimeType::DEFAULT_PRECISION)?)),
            "TIMESTAMP WITH LOCAL TIME ZONE" => Ok(DataType::LocalZonedTimestamp(
                LocalZonedTimestampType::new(LocalZonedTimestampType::DEFAULT_PRECISION)?,
            )),
            "ARRAY<INTEGER>" => Ok(DataType::Array(ArrayType::new(DataType::Int(
                IntType::new(),
            )))),
            "ARRAY<BIGINT>" => Ok(DataType::Array(ArrayType::new(DataType::BigInt(
                BigIntType::new(),
            )))),
            "ARRAY<BOOLEAN>" => Ok(DataType::Array(ArrayType::new(DataType::Boolean(
                BooleanType::new(),
            )))),
            "ARRAY<CHAR>" => Ok(DataType::Array(ArrayType::new(DataType::Char(
                CharType::new(CharType::DEFAULT_LENGTH)?,
            )))),
            "ARRAY<VARCHAR>" => Ok(DataType::Array(ArrayType::new(DataType::VarChar(
                VarCharType::new(VarCharType::DEFAULT_LENGTH)?,
            )))),
            "ARRAY<VARBINARY>" => Ok(DataType::Array(ArrayType::new(DataType::VarBinary(
                VarBinaryType::new(VarBinaryType::DEFAULT_LENGTH)?,
            )))),
            "ARRAY<DATE>" => Ok(DataType::Array(ArrayType::new(DataType::Date(
                DateType::new(),
            )))),
            "ARRAY<TIMESTAMP>" => Ok(DataType::Array(ArrayType::new(DataType::Timestamp(
                TimestampType::new(TimestampType::DEFAULT_PRECISION)?,
            )))),
            "ARRAY<TIME>" => Ok(DataType::Array(ArrayType::new(DataType::Time(
                TimeType::new(TimeType::DEFAULT_PRECISION)?,
            )))),
            "ARRAY<DECIMAL>" => Ok(DataType::Array(ArrayType::new(DataType::Decimal(
                DecimalType::new(DecimalType::DEFAULT_PRECISION, DecimalType::DEFAULT_SCALE)?,
            )))),
            "ARRAY<DOUBLE>" => Ok(DataType::Array(ArrayType::new(DataType::Double(
                DoubleType::new(),
            )))),
            "ARRAY<FLOAT>" => Ok(DataType::Array(ArrayType::new(DataType::Float(
                FloatType::new(),
            )))),
            _ => DataTypeInvalidSnafu {
                message: format!("Invalid data type: {}", v),
            }
            .fail(),
        }
    }
}

/// ArrayType for paimon.
///
/// Data type of an array of elements with same subtype.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/types/ArrayType.java>.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
#[serde(rename_all = "camelCase")]
pub struct ArrayType {
    #[serde_as(as = "DisplayFromStr")]
    nullable: bool,
    #[serde(rename = "type")]
    #[serde_as(as = "Box<DisplayFromStr>")]
    element_type: Box<DataType>,
}

impl Display for ArrayType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ARRAY<{}>", self.element_type)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
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
pub struct BigIntType {
    #[serde_as(as = "DisplayFromStr")]
    nullable: bool,
}

impl Display for BigIntType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BIGINT")?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
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
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
#[serde(rename_all = "camelCase")]
pub struct BinaryType {
    #[serde_as(as = "DisplayFromStr")]
    nullable: bool,
    #[serde_as(as = "DisplayFromStr")]
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
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct BooleanType {
    #[serde_as(as = "DisplayFromStr")]
    nullable: bool,
}

impl Display for BooleanType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BOOLEAN")?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
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
#[serde_as]
#[derive(Debug, Clone, PartialEq, Hash, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CharType {
    #[serde_as(as = "DisplayFromStr")]
    nullable: bool,
    #[serde_as(as = "DisplayFromStr")]
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
#[derive(Debug, Clone, PartialEq, Hash, Eq, Deserialize, Serialize)]
pub struct DateType {
    #[serde_as(as = "DisplayFromStr")]
    nullable: bool,
}

impl Display for DateType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DATE")?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
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
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct DecimalType {
    #[serde_as(as = "DisplayFromStr")]
    nullable: bool,
    #[serde_as(as = "DisplayFromStr")]
    precision: u32,
    #[serde_as(as = "DisplayFromStr")]
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
pub struct DoubleType {
    #[serde_as(as = "DisplayFromStr")]
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
pub struct FloatType {
    #[serde_as(as = "DisplayFromStr")]
    nullable: bool,
}

impl Display for FloatType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FLOAT")?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
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
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct IntType {
    #[serde_as(as = "DisplayFromStr")]
    nullable: bool,
}

impl Display for IntType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "INTEGER")?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
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
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct LocalZonedTimestampType {
    #[serde_as(as = "DisplayFromStr")]
    nullable: bool,
    #[serde_as(as = "DisplayFromStr")]
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
pub struct SmallIntType {
    #[serde_as(as = "DisplayFromStr")]
    nullable: bool,
}

impl Display for SmallIntType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SMALLINT")?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
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
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct TimeType {
    #[serde_as(as = "DisplayFromStr")]
    nullable: bool,
    #[serde_as(as = "DisplayFromStr")]
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
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct TimestampType {
    #[serde_as(as = "DisplayFromStr")]
    nullable: bool,
    #[serde_as(as = "DisplayFromStr")]
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
pub struct TinyIntType {
    #[serde_as(as = "DisplayFromStr")]
    nullable: bool,
}

impl Display for TinyIntType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TINYINT")?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
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
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct VarBinaryType {
    #[serde_as(as = "DisplayFromStr")]
    nullable: bool,
    #[serde_as(as = "DisplayFromStr")]
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
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct VarCharType {
    #[serde_as(as = "DisplayFromStr")]
    nullable: bool,
    #[serde_as(as = "DisplayFromStr")]
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
    #[serde_as(as = "DisplayFromStr")]
    nullable: bool,
    #[serde_as(as = "Box<DisplayFromStr>")]
    key_type: Box<DataType>,
    #[serde_as(as = "Box<DisplayFromStr>")]
    value_type: Box<DataType>,
}

impl Display for MapType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MAP<{}, {}>", self.key_type, self.value_type)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
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
    #[serde_as(as = "DisplayFromStr")]
    nullable: bool,
    #[serde_as(as = "Box<DisplayFromStr>")]
    element_type: Box<DataType>,
}

impl Display for MultisetType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MULTISET<{}>", self.element_type)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
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
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
#[serde_as]
pub struct RowType {
    #[serde_as(as = "DisplayFromStr")]
    nullable: bool,
    #[serde_as(as = "Vec<DisplayFromStr>")]
    fields: Vec<DataField>,
}

impl Display for RowType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let fields = self
            .fields
            .iter()
            .map(|field| field.to_string())
            .collect::<Vec<String>>()
            .join(", ");
        write!(f, "ROW<{}>", fields)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
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

#[cfg(test)]
mod tests {

    use serde_json::{json, Value};

    use super::*;

    #[test]
    fn test_data_type_to_string() {
        assert_eq!(
            DataType::Boolean(BooleanType::with_nullable(true)).to_string(),
            "BOOLEAN"
        );
        assert_eq!(
            DataType::Boolean(BooleanType::with_nullable(false)).to_string(),
            "BOOLEAN NOT NULL"
        );
        assert_eq!(
            DataType::TinyInt(TinyIntType::with_nullable(true)).to_string(),
            "TINYINT"
        );
        assert_eq!(
            DataType::TinyInt(TinyIntType::with_nullable(false)).to_string(),
            "TINYINT NOT NULL"
        );
        assert_eq!(
            DataType::SmallInt(SmallIntType::with_nullable(true)).to_string(),
            "SMALLINT"
        );
        assert_eq!(
            DataType::SmallInt(SmallIntType::with_nullable(false)).to_string(),
            "SMALLINT NOT NULL"
        );
        assert_eq!(
            DataType::Int(IntType::with_nullable(true)).to_string(),
            "INTEGER"
        );
        assert_eq!(
            DataType::Int(IntType::with_nullable(false)).to_string(),
            "INTEGER NOT NULL"
        );
        assert_eq!(
            DataType::BigInt(BigIntType::with_nullable(true)).to_string(),
            "BIGINT"
        );
        assert_eq!(
            DataType::BigInt(BigIntType::with_nullable(false)).to_string(),
            "BIGINT NOT NULL"
        );
        assert_eq!(
            DataType::Decimal(DecimalType::with_nullable(true, 10, 2).unwrap()).to_string(),
            "DECIMAL(10, 2)"
        );
        assert_eq!(
            DataType::Decimal(DecimalType::with_nullable(false, 10, 2).unwrap()).to_string(),
            "DECIMAL(10, 2) NOT NULL"
        );
        assert_eq!(
            DataType::Double(DoubleType::with_nullable(true)).to_string(),
            "DOUBLE"
        );
        assert_eq!(
            DataType::Double(DoubleType::with_nullable(false)).to_string(),
            "DOUBLE NOT NULL"
        );
        assert_eq!(
            DataType::Float(FloatType::with_nullable(true)).to_string(),
            "FLOAT"
        );
        assert_eq!(
            DataType::Float(FloatType::with_nullable(false)).to_string(),
            "FLOAT NOT NULL"
        );
        assert_eq!(
            DataType::Binary(BinaryType::with_nullable(true, 10).unwrap()).to_string(),
            "BINARY(10)"
        );
        assert_eq!(
            DataType::Binary(BinaryType::with_nullable(false, 10).unwrap()).to_string(),
            "BINARY(10) NOT NULL"
        );
        assert_eq!(
            DataType::VarBinary(VarBinaryType::try_new(true, 10).unwrap()).to_string(),
            "VARBINARY(10)"
        );
        assert_eq!(
            DataType::VarBinary(VarBinaryType::try_new(false, 10).unwrap()).to_string(),
            "VARBINARY(10) NOT NULL"
        );
        assert_eq!(
            DataType::Char(CharType::with_nullable(true, 10).unwrap()).to_string(),
            "CHAR(10)"
        );
        assert_eq!(
            DataType::Char(CharType::with_nullable(false, 10).unwrap()).to_string(),
            "CHAR(10) NOT NULL"
        );
        assert_eq!(
            DataType::VarChar(VarCharType::with_nullable(true, 10).unwrap()).to_string(),
            "VARCHAR(10)"
        );
        assert_eq!(
            DataType::VarChar(VarCharType::with_nullable(false, 10).unwrap()).to_string(),
            "VARCHAR(10) NOT NULL"
        );
        assert_eq!(
            DataType::Date(DateType::with_nullable(true)).to_string(),
            "DATE"
        );
        assert_eq!(
            DataType::Date(DateType::with_nullable(false)).to_string(),
            "DATE NOT NULL"
        );
        assert_eq!(
            DataType::LocalZonedTimestamp(LocalZonedTimestampType::with_nullable(true, 6).unwrap())
                .to_string(),
            "TIMESTAMP WITH LOCAL TIME ZONE(6)"
        );
        assert_eq!(
            DataType::LocalZonedTimestamp(
                LocalZonedTimestampType::with_nullable(false, 6).unwrap()
            )
            .to_string(),
            "TIMESTAMP WITH LOCAL TIME ZONE(6) NOT NULL"
        );
        assert_eq!(
            DataType::Time(TimeType::with_nullable(true, 6).unwrap()).to_string(),
            "TIME(6)"
        );
        assert_eq!(
            DataType::Time(TimeType::with_nullable(false, 6).unwrap()).to_string(),
            "TIME(6) NOT NULL"
        );
        assert_eq!(
            DataType::Timestamp(TimestampType::with_nullable(false, 6).unwrap()).to_string(),
            "TIMESTAMP(6) NOT NULL"
        );
        assert_eq!(
            DataType::Timestamp(TimestampType::with_nullable(true, 6).unwrap()).to_string(),
            "TIMESTAMP(6)"
        );
        let int_type = DataType::Int(IntType::with_nullable(true));
        let arr_type = DataType::Array(ArrayType::with_nullable(true, int_type.clone()));
        assert_eq!(arr_type.to_string(), "ARRAY<INTEGER>");
        assert_eq!(
            DataType::Array(ArrayType::with_nullable(true, arr_type.clone())).to_string(),
            "ARRAY<ARRAY<INTEGER>>"
        );
        let map_type = DataType::Map(MapType::with_nullable(
            true,
            int_type.clone(),
            arr_type.clone(),
        ));
        assert_eq!(map_type.to_string(), "MAP<INTEGER, ARRAY<INTEGER>>");
        let multiset_type = DataType::Multiset(MultisetType::with_nullable(true, int_type.clone()));
        assert_eq!(multiset_type.to_string(), "MULTISET<INTEGER>");
        let row_type = DataType::Row(RowType::with_nullable(
            true,
            vec![
                DataField::new(1, "a".to_string(), int_type.clone()),
                DataField::new(2, "b".to_string(), arr_type.clone()),
            ],
        ));
        assert_eq!(row_type.to_string(), "ROW<INTEGER, ARRAY<INTEGER>>");
    }

    #[test]
    fn test_datatype_serialization_deserialization() {
        // boolean
        let boolean_type: DataType = DataType::Boolean(BooleanType::with_nullable(true));
        let boolean_serialized: Value = serde_json::to_value(&boolean_type).unwrap();
        assert_eq!(boolean_serialized, json!({"Boolean": {"nullable": "true"}}));

        let boolean_deserialized: DataType = serde_json::from_value(boolean_serialized).unwrap();
        assert_eq!(boolean_deserialized, boolean_type);

        // tinyint
        let tinyint_type: DataType = DataType::TinyInt(TinyIntType::with_nullable(true));
        let tinyint_serialized: Value = serde_json::to_value(&tinyint_type).unwrap();
        assert_eq!(tinyint_serialized, json!({"TinyInt": {"nullable": "true"}}));
        let tinyint_deserialized: DataType = serde_json::from_value(tinyint_serialized).unwrap();
        assert_eq!(tinyint_deserialized, tinyint_type);

        // smallint
        let smallint_type: DataType = DataType::SmallInt(SmallIntType::with_nullable(true));
        let smallint_serialized: Value = serde_json::to_value(&smallint_type).unwrap();
        assert_eq!(
            smallint_serialized,
            json!({"SmallInt": {"nullable": "true"}})
        );
        let smallint_deserialized: DataType = serde_json::from_value(smallint_serialized).unwrap();
        assert_eq!(smallint_deserialized, smallint_type);

        // int
        let int_type: DataType = DataType::Int(IntType::with_nullable(true));
        let int_serialized: Value = serde_json::to_value(&int_type).unwrap();
        assert_eq!(int_serialized, json!({"Int": {"nullable": "true"}}));
        let int_deserialized: DataType = serde_json::from_value(int_serialized).unwrap();
        assert_eq!(int_deserialized, int_type);

        // bigint
        let bigint_type: DataType = DataType::BigInt(BigIntType::with_nullable(true));
        let bigint_serialized: Value = serde_json::to_value(&bigint_type).unwrap();
        assert_eq!(bigint_serialized, json!({"BigInt": {"nullable": "true"}}));
        let bigint_deserialized: DataType = serde_json::from_value(bigint_serialized).unwrap();
        assert_eq!(bigint_deserialized, bigint_type);

        // decimal
        let decimal_type: DataType =
            DataType::Decimal(DecimalType::with_nullable(true, 10, 2).unwrap());
        let decimal_serialized: Value = serde_json::to_value(&decimal_type).unwrap();
        assert_eq!(
            decimal_serialized,
            json!({"Decimal": {"nullable": "true", "precision": "10", "scale": "2"}})
        );
        let decimal_deserialized: DataType = serde_json::from_value(decimal_serialized).unwrap();
        assert_eq!(decimal_deserialized, decimal_type);

        // double
        let double_type: DataType = DataType::Double(DoubleType::with_nullable(true));
        let double_serialized: Value = serde_json::to_value(&double_type).unwrap();
        assert_eq!(double_serialized, json!({"Double": {"nullable": "true"}}));
        let double_deserialized: DataType = serde_json::from_value(double_serialized).unwrap();
        assert_eq!(double_deserialized, double_type);

        // float
        let float_type: DataType = DataType::Float(FloatType::with_nullable(true));
        let float_serialized: Value = serde_json::to_value(&float_type).unwrap();
        assert_eq!(float_serialized, json!({"Float": {"nullable": "true"}}));
        let float_deserialized: DataType = serde_json::from_value(float_serialized).unwrap();
        assert_eq!(float_deserialized, float_type);

        // binary
        let binary_type: DataType = DataType::Binary(BinaryType::with_nullable(true, 10).unwrap());
        let binary_serialized: Value = serde_json::to_value(&binary_type).unwrap();
        assert_eq!(
            binary_serialized,
            json!({"Binary": {"nullable": "true", "length": "10"}})
        );
        let binary_deserialized: DataType = serde_json::from_value(binary_serialized).unwrap();
        assert_eq!(binary_deserialized, binary_type);

        // varbinary
        let varbinary_type: DataType =
            DataType::VarBinary(VarBinaryType::try_new(true, 10).unwrap());
        let varbinary_serialized: Value = serde_json::to_value(&varbinary_type).unwrap();
        assert_eq!(
            varbinary_serialized,
            json!({"VarBinary": {"nullable": "true", "length": "10"}})
        );
        let varbinary_deserialized: DataType =
            serde_json::from_value(varbinary_serialized).unwrap();
        assert_eq!(varbinary_deserialized, varbinary_type);

        // char
        let char_type: DataType = DataType::Char(CharType::with_nullable(true, 10).unwrap());
        let char_serialized: Value = serde_json::to_value(&char_type).unwrap();
        assert_eq!(
            char_serialized,
            json!({"Char": {"nullable": "true", "length": "10"}})
        );
        let char_deserialized: DataType = serde_json::from_value(char_serialized).unwrap();
        assert_eq!(char_deserialized, char_type);

        // varchar
        let varchar_type: DataType =
            DataType::VarChar(VarCharType::with_nullable(true, 10).unwrap());
        let varchar_serialized: Value = serde_json::to_value(&varchar_type).unwrap();
        assert_eq!(
            varchar_serialized,
            json!({"VarChar": {"nullable": "true", "length": "10"}})
        );
        let varchar_deserialized: DataType = serde_json::from_value(varchar_serialized).unwrap();
        assert_eq!(varchar_deserialized, varchar_type);

        // date
        let date_type: DataType = DataType::Date(DateType::with_nullable(true));
        let date_serialized: Value = serde_json::to_value(&date_type).unwrap();
        assert_eq!(date_serialized, json!({"Date": {"nullable": "true"}}));
        let date_deserialized: DataType = serde_json::from_value(date_serialized).unwrap();
        assert_eq!(date_deserialized, date_type);

        // localzonedtimestamp
        let localzonedtimestamp_type: DataType =
            DataType::LocalZonedTimestamp(LocalZonedTimestampType::with_nullable(true, 6).unwrap());
        let localzonedtimestamp_serialized: Value =
            serde_json::to_value(&localzonedtimestamp_type).unwrap();
        assert_eq!(
            localzonedtimestamp_serialized,
            json!({"LocalZonedTimestamp": {"nullable": "true", "precision": "6"}})
        );
        let localzonedtimestamp_deserialized: DataType =
            serde_json::from_value(localzonedtimestamp_serialized).unwrap();
        assert_eq!(localzonedtimestamp_deserialized, localzonedtimestamp_type);

        // time
        let time_type: DataType = DataType::Time(TimeType::with_nullable(true, 6).unwrap());
        let time_serialized: Value = serde_json::to_value(&time_type).unwrap();
        assert_eq!(
            time_serialized,
            json!({"Time": {"nullable": "true", "precision": "6"}})
        );
        let time_deserialized: DataType = serde_json::from_value(time_serialized).unwrap();
        assert_eq!(time_deserialized, time_type);

        // timestamp
        let timestamp_type: DataType =
            DataType::Timestamp(TimestampType::with_nullable(true, 6).unwrap());
        let timestamp_serialized: Value = serde_json::to_value(&timestamp_type).unwrap();
        assert_eq!(
            timestamp_serialized,
            json!({"Timestamp": {"nullable": "true", "precision": "6"}})
        );
        let timestamp_deserialized: DataType =
            serde_json::from_value(timestamp_serialized).unwrap();
        assert_eq!(timestamp_deserialized, timestamp_type);

        // array
        let arr_type: DataType = DataType::Array(ArrayType::with_nullable(
            true,
            DataType::Int(IntType::with_nullable(true)),
        ));
        let arr_serialized: Value = serde_json::to_value(&arr_type).unwrap();
        assert_eq!(
            arr_serialized,
            json!({"Array": {"nullable": "true", "type": "INTEGER"}})
        );

        let arr_deserialized: DataType = serde_json::from_value(arr_serialized).unwrap();
        assert_eq!(arr_deserialized, arr_type);

        // map
        let map_type: DataType = DataType::Map(MapType::with_nullable(
            true,
            int_type.clone(),
            arr_type.clone(),
        ));
        let map_serialized: Value = serde_json::to_value(&map_type).unwrap();
        assert_eq!(
            map_serialized,
            json!({"Map": {"nullable": "true", "key_type": "INTEGER", "value_type": "ARRAY<INTEGER>"}})
        );
        let map_deserialized: DataType = serde_json::from_value(map_serialized).unwrap();
        assert_eq!(map_deserialized, map_type);

        // multiset
        let multiset_type: DataType = DataType::Multiset(MultisetType::with_nullable(
            true,
            DataType::Int(IntType::with_nullable(true)),
        ));
        let multiset_serialized: Value = serde_json::to_value(&multiset_type).unwrap();
        assert_eq!(
            multiset_serialized,
            json!({"Multiset": {"nullable": "true", "element_type": "INTEGER"}})
        );
        let multiset_deserialized: DataType = serde_json::from_value(multiset_serialized).unwrap();
        assert_eq!(multiset_deserialized, multiset_type);

        // row
        let row_type: DataType = DataType::Row(RowType::with_nullable(
            true,
            vec![
                DataField::new(1, "a".to_string(), int_type.clone()),
                DataField::new(2, "b".to_string(), arr_type.clone()),
            ],
        ));
        let row_serialized: Value = serde_json::to_value(&row_type).unwrap();
        assert_eq!(
            row_serialized,
            json!({"Row": {"nullable": true, "fields": [{"description": serde_json::Value::Null,"id": 1, "name": "a", "type": "INTEGER"}, 
            {"description": serde_json::Value::Null,"id": 2, "name": "b", "type": "ARRAY<INTEGER>"}]}})
        );
        let row_deserialized: DataType = serde_json::from_value(row_serialized).unwrap();
        assert_eq!(row_deserialized, row_type);
    }
}
