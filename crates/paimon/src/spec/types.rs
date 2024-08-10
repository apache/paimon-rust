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
use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_with::{DeserializeFromStr, SerializeDisplay};
use std::fmt;
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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ArrayType {
    nullable: bool,
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

impl Serialize for ArrayType {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("ArrayType", 2)?;
        let typ = if self.nullable {
            "ARRAY"
        } else {
            "ARRAY NOT NULL"
        };
        s.serialize_field("type", typ)?;
        s.serialize_field("element", &self.element_type)?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for ArrayType {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "lowercase")]
        enum Field {
            Type,
            Element,
        }

        struct ArrayTypeVisitor;

        impl<'de> Visitor<'de> for ArrayTypeVisitor {
            type Value = ArrayType;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("ArrayType")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ArrayType, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut nullable = None;
                let mut element: Option<DataType> = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Type => {
                            if nullable.is_some() {
                                return Err(serde::de::Error::duplicate_field("type"));
                            }
                            match map.next_value()? {
                                "ARRAY" => nullable = Some(true),
                                "ARRAY NOT NULL" => nullable = Some(false),
                                v => Err(serde::de::Error::invalid_value(
                                    serde::de::Unexpected::Str(v),
                                    &"ARRAY or ARRAY NOT NULL",
                                ))?,
                            }
                        }
                        Field::Element => {
                            if element.is_some() {
                                return Err(serde::de::Error::duplicate_field("element"));
                            }
                            element = Some(map.next_value()?);
                        }
                    }
                }

                Ok(ArrayType {
                    nullable: nullable.ok_or_else(|| serde::de::Error::missing_field("type"))?,
                    element_type: element
                        .ok_or_else(|| serde::de::Error::missing_field("element"))?
                        .into(),
                })
            }
        }

        const FIELDS: &[&str] = &["type", "element"];
        deserializer.deserialize_struct("ArrayType", FIELDS, ArrayTypeVisitor)
    }
}

/// BigIntType for paimon.
///
/// Data type of an 8-byte (2^64) signed integer with values from -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/types/BigIntType.java>.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, SerializeDisplay, Hash)]
pub struct BigIntType {
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
#[derive(Debug, Clone, PartialEq, Eq, DeserializeFromStr, SerializeDisplay, Hash)]
pub struct BooleanType {
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

impl FromStr for BooleanType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "BOOLEAN" => Ok(Self { nullable: true }),
            "BOOLEAN NOT NULL" => Ok(Self { nullable: false }),
            v => Err(Error::DataTypeInvalid {
                message: format!("invalid boolean type: {v}"),
            })?,
        }
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
#[derive(Debug, Clone, PartialEq, Hash, Eq, Deserialize, SerializeDisplay)]
pub struct DateType {
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
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, SerializeDisplay, Hash)]
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
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, SerializeDisplay, Hash)]
pub struct DoubleType {
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
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, SerializeDisplay, Hash)]
pub struct FloatType {
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
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, SerializeDisplay, Hash)]
pub struct IntType {
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
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, SerializeDisplay, Hash)]
pub struct SmallIntType {
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
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, SerializeDisplay, Hash)]
pub struct TinyIntType {
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
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct MapType {
    nullable: bool,
    key_type: Box<DataType>,
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
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct MultisetType {
    nullable: bool,
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
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct RowType {
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

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_data_type_to_string() {
        assert_eq!(BooleanType::with_nullable(true).to_string(), "BOOLEAN");
        assert_eq!(
            BooleanType::with_nullable(false).to_string(),
            "BOOLEAN NOT NULL"
        );
        assert_eq!(TinyIntType::with_nullable(true).to_string(), "TINYINT");
        assert_eq!(
            TinyIntType::with_nullable(false).to_string(),
            "TINYINT NOT NULL"
        );
        assert_eq!(SmallIntType::with_nullable(true).to_string(), "SMALLINT");
        assert_eq!(
            SmallIntType::with_nullable(false).to_string(),
            "SMALLINT NOT NULL"
        );
        assert_eq!(IntType::with_nullable(true).to_string(), "INTEGER");
        assert_eq!(
            IntType::with_nullable(false).to_string(),
            "INTEGER NOT NULL"
        );
        assert_eq!(BigIntType::with_nullable(true).to_string(), "BIGINT");
        assert_eq!(
            BigIntType::with_nullable(false).to_string(),
            "BIGINT NOT NULL"
        );
        assert_eq!(
            DecimalType::with_nullable(true, 10, 2).unwrap().to_string(),
            "DECIMAL(10, 2)"
        );
        assert_eq!(
            DecimalType::with_nullable(false, 10, 2)
                .unwrap()
                .to_string(),
            "DECIMAL(10, 2) NOT NULL"
        );
        assert_eq!(DoubleType::with_nullable(true).to_string(), "DOUBLE");
        assert_eq!(
            DoubleType::with_nullable(false).to_string(),
            "DOUBLE NOT NULL"
        );
        assert_eq!(FloatType::with_nullable(true).to_string(), "FLOAT");
        assert_eq!(
            FloatType::with_nullable(false).to_string(),
            "FLOAT NOT NULL"
        );
        assert_eq!(
            BinaryType::with_nullable(true, 10).unwrap().to_string(),
            "BINARY(10)"
        );
        assert_eq!(
            BinaryType::with_nullable(false, 10).unwrap().to_string(),
            "BINARY(10) NOT NULL"
        );
        assert_eq!(
            VarBinaryType::try_new(true, 10).unwrap().to_string(),
            "VARBINARY(10)"
        );
        assert_eq!(
            VarBinaryType::try_new(false, 10).unwrap().to_string(),
            "VARBINARY(10) NOT NULL"
        );
        assert_eq!(
            CharType::with_nullable(true, 10).unwrap().to_string(),
            "CHAR(10)"
        );
        assert_eq!(
            CharType::with_nullable(false, 10).unwrap().to_string(),
            "CHAR(10) NOT NULL"
        );
        assert_eq!(
            VarCharType::with_nullable(true, 10).unwrap().to_string(),
            "VARCHAR(10)"
        );
        assert_eq!(
            VarCharType::with_nullable(false, 10).unwrap().to_string(),
            "VARCHAR(10) NOT NULL"
        );
        assert_eq!(DateType::with_nullable(true).to_string(), "DATE");
        assert_eq!(DateType::with_nullable(false).to_string(), "DATE NOT NULL");
        assert_eq!(
            LocalZonedTimestampType::with_nullable(true, 6)
                .unwrap()
                .to_string(),
            "TIMESTAMP WITH LOCAL TIME ZONE(6)"
        );
        assert_eq!(
            LocalZonedTimestampType::with_nullable(false, 6)
                .unwrap()
                .to_string(),
            "TIMESTAMP WITH LOCAL TIME ZONE(6) NOT NULL"
        );
        assert_eq!(
            TimeType::with_nullable(true, 6).unwrap().to_string(),
            "TIME(6)"
        );
        assert_eq!(
            TimeType::with_nullable(false, 6).unwrap().to_string(),
            "TIME(6) NOT NULL"
        );
        assert_eq!(
            TimestampType::with_nullable(false, 6).unwrap().to_string(),
            "TIMESTAMP(6) NOT NULL"
        );
        assert_eq!(
            TimestampType::with_nullable(true, 6).unwrap().to_string(),
            "TIMESTAMP(6)"
        );
    }

    /// TODO: replace expect with exist fixture.
    #[test]
    fn test_data_type_serialize() {
        // name, input, expect.
        let cases = vec![
            (
                "boolean",
                DataType::Boolean(BooleanType::with_nullable(true)),
                r#""BOOLEAN""#,
            ),
            (
                "array with boolean",
                DataType::Array(ArrayType {
                    nullable: false,
                    element_type: DataType::Boolean(BooleanType::with_nullable(false)).into(),
                }),
                r#"{"type":"ARRAY NOT NULL","element":"BOOLEAN NOT NULL"}"#,
            ),
        ];

        for (name, input, expect) in cases {
            assert_eq!(
                serde_json::to_string(&input).unwrap(),
                expect,
                "test data type serialize for {name}"
            )
        }
    }

    /// TODO: replace expect with exist fixture.
    #[test]
    fn test_data_type_deserialize() {
        // name, input, expect.
        let cases = vec![
            (
                "boolean",
                r#""BOOLEAN""#,
                DataType::Boolean(BooleanType::with_nullable(true)),
            ),
            (
                "array with boolean",
                r#"{"type":"ARRAY NOT NULL","element":"BOOLEAN NOT NULL"}"#,
                DataType::Array(ArrayType {
                    nullable: false,
                    element_type: DataType::Boolean(BooleanType::with_nullable(false)).into(),
                }),
            ),
        ];

        for (name, input, expect) in cases {
            assert_eq!(
                serde_json::from_str::<DataType>(input).unwrap(),
                expect,
                "test data type deserialize for {name}"
            )
        }
    }
}
