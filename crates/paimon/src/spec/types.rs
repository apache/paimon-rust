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

use crate::error::Error;
use bitflags::bitflags;
use serde::{Deserialize, Serialize};
use std::fmt::{Arguments, Display, Formatter};
use std::str::FromStr;

bitflags! {
/// An enumeration of Data type families for clustering {@link DataTypeRoot}s into categories.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/master/paimon-common/src/main/java/org/apache/paimon/types/DataTypeFamily.java>
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// The root of data type.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/db8bcd7fdd9c2705435d2ab1d2341c52d1f67ee5/paimon-common/src/main/java/org/apache/paimon/types/DataTypeRoot.java#L49>
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub enum DataTypeRoot {
    Char,
    Varchar,
    Boolean,
    Binary,
    Varbinary,
    Decimal,
    Tinyint,
    Smallint,
    Integer,
    Bigint,
    Float,
    Double,
    Date,
    TimeWithoutTimeZone,
    TimestampWithoutTimeZone,
    TimestampWithLocalTimeZone,
    Array,
    Multiset,
    Map,
    Row,
}

impl DataTypeRoot {
    pub fn families(&self) -> DataTypeFamily {
        match self {
            Self::Char => DataTypeFamily::PREDEFINED | DataTypeFamily::CHARACTER_STRING,
            Self::Varchar => DataTypeFamily::PREDEFINED | DataTypeFamily::CHARACTER_STRING,
            Self::Boolean => DataTypeFamily::PREDEFINED,
            Self::Binary => DataTypeFamily::PREDEFINED | DataTypeFamily::BINARY_STRING,
            Self::Varbinary => DataTypeFamily::PREDEFINED | DataTypeFamily::BINARY_STRING,
            Self::Decimal => {
                DataTypeFamily::PREDEFINED | DataTypeFamily::NUMERIC | DataTypeFamily::EXACT_NUMERIC
            }
            Self::Tinyint => {
                DataTypeFamily::PREDEFINED
                    | DataTypeFamily::NUMERIC
                    | DataTypeFamily::INTEGER_NUMERIC
                    | DataTypeFamily::EXACT_NUMERIC
            }
            Self::Smallint => {
                DataTypeFamily::PREDEFINED
                    | DataTypeFamily::NUMERIC
                    | DataTypeFamily::INTEGER_NUMERIC
                    | DataTypeFamily::EXACT_NUMERIC
            }
            Self::Integer => {
                DataTypeFamily::PREDEFINED
                    | DataTypeFamily::NUMERIC
                    | DataTypeFamily::INTEGER_NUMERIC
                    | DataTypeFamily::EXACT_NUMERIC
            }
            Self::Bigint => {
                DataTypeFamily::PREDEFINED
                    | DataTypeFamily::NUMERIC
                    | DataTypeFamily::INTEGER_NUMERIC
                    | DataTypeFamily::EXACT_NUMERIC
            }
            Self::Float => {
                DataTypeFamily::PREDEFINED
                    | DataTypeFamily::NUMERIC
                    | DataTypeFamily::APPROXIMATE_NUMERIC
            }
            Self::Double => {
                DataTypeFamily::PREDEFINED
                    | DataTypeFamily::NUMERIC
                    | DataTypeFamily::APPROXIMATE_NUMERIC
            }
            Self::Date => DataTypeFamily::PREDEFINED | DataTypeFamily::DATETIME,
            Self::TimeWithoutTimeZone => {
                DataTypeFamily::PREDEFINED | DataTypeFamily::DATETIME | DataTypeFamily::TIME
            }
            Self::TimestampWithoutTimeZone => {
                DataTypeFamily::PREDEFINED | DataTypeFamily::DATETIME | DataTypeFamily::TIMESTAMP
            }
            Self::TimestampWithLocalTimeZone => {
                DataTypeFamily::PREDEFINED
                    | DataTypeFamily::DATETIME
                    | DataTypeFamily::TIMESTAMP
                    | DataTypeFamily::EXTENSION
            }
            Self::Array => DataTypeFamily::CONSTRUCTED | DataTypeFamily::COLLECTION,
            Self::Multiset => DataTypeFamily::CONSTRUCTED | DataTypeFamily::COLLECTION,
            Self::Map => DataTypeFamily::CONSTRUCTED | DataTypeFamily::EXTENSION,
            Self::Row => DataTypeFamily::CONSTRUCTED,
        }
    }
}

/// A visitor that can visit different data types.
pub trait DataTypeVisitor<R> {
    fn visit(&mut self, data_type: &DataType) -> R;
}

/// Data type for paimon table.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/db8bcd7fdd9c2705435d2ab1d2341c52d1f67ee5/paimon-common/src/main/java/org/apache/paimon/types/DataType.java#L45>
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct DataType {
    is_nullable: bool,
    type_root: DataTypeRoot,
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if !self.is_nullable() {
            write!(f, "{} NOT NULL", self.as_sql_string())
        } else {
            write!(f, "{}", self.as_sql_string())
        }
    }
}

impl FromStr for DataType {
    type Err = Error;

    fn from_str(_: &str) -> Result<Self, Self::Err> {
        todo!()
    }
}

#[allow(dead_code)]
impl DataType {
    fn new(is_nullable: bool, type_root: DataTypeRoot) -> Self {
        Self {
            is_nullable,
            type_root,
        }
    }

    /// Returns true if the data type is nullable.
    ///
    /// Impl Reference: <https://github.com/apache/paimon/blob/db8bcd7fdd9c2705435d2ab1d2341c52d1f67ee5/paimon-common/src/main/java/org/apache/paimon/types/DataType.java#L59>
    fn is_nullable(&self) -> bool {
        self.is_nullable
    }

    /// Returns the root of the data type.
    ///
    /// Impl Reference: <https://github.com/apache/paimon/blob/db8bcd7fdd9c2705435d2ab1d2341c52d1f67ee5/paimon-common/src/main/java/org/apache/paimon/types/DataType.java#L66>
    fn get_type_root(&self) -> &DataTypeRoot {
        &self.type_root
    }

    /// Returns whether the root of the type equals to the type_root or not.
    ///
    /// Impl Reference: <https://github.com/apache/paimon/blob/db8bcd7fdd9c2705435d2ab1d2341c52d1f67ee5/paimon-common/src/main/java/org/apache/paimon/types/DataType.java#L75>
    fn is(&self, type_root: &DataTypeRoot) -> bool {
        &self.type_root == type_root
    }

    /// Returns whether the family type of the type equals to the family or not.
    ///
    /// Impl Reference: <https://github.com/apache/paimon/blob/db8bcd7fdd9c2705435d2ab1d2341c52d1f67ee5/paimon-common/src/main/java/org/apache/paimon/types/DataType.java#L103>
    fn is_with_family(&self, family: DataTypeFamily) -> bool {
        self.type_root.families().contains(family)
    }

    /// Returns whether the root of the type equals to at least on of the type_roots or not.
    ///
    /// Impl Reference: <https://github.com/apache/paimon/blob/db8bcd7fdd9c2705435d2ab1d2341c52d1f67ee5/paimon-common/src/main/java/org/apache/paimon/types/DataType.java#L84>
    fn is_any_of(&self, type_roots: &[DataTypeRoot]) -> bool {
        type_roots.iter().any(|tr: &DataTypeRoot| self.is(tr))
    }

    /// Returns whether the root of the type is part of at least one family of the families or not.
    /// Impl Reference: <https://github.com/apache/paimon/blob/db8bcd7fdd9c2705435d2ab1d2341c52d1f67ee5/paimon-common/src/main/java/org/apache/paimon/types/DataType.java#L94>
    fn is_any_of_family(&self, families: &[DataTypeFamily]) -> bool {
        families
            .iter()
            .any(|f: &DataTypeFamily| self.is_with_family(f.clone()))
    }

    /// Returns a deep copy of this type with possibly different nullability.
    /// Impl Reference: <https://github.com/apache/paimon/blob/db8bcd7fdd9c2705435d2ab1d2341c52d1f67ee5/paimon-common/src/main/java/org/apache/paimon/types/DataType.java#L113>
    fn copy(&self, is_nullable: bool) -> Self {
        Self {
            is_nullable,
            type_root: self.type_root.clone(),
        }
    }

    /// Returns a deep copy of this type. It requires an implementation of {@link #copy(boolean)}.
    /// Impl Reference: <https://github.com/apache/paimon/blob/db8bcd7fdd9c2705435d2ab1d2341c52d1f67ee5/paimon-common/src/main/java/org/apache/paimon/types/DataType.java#L120>
    fn copy_with_nullable(&self) -> Self {
        self.copy(self.is_nullable)
    }

    /// Compare two data types without nullable.
    /// Impl Reference: <https://github.com/apache/paimon/blob/db8bcd7fdd9c2705435d2ab1d2341c52d1f67ee5/paimon-common/src/main/java/org/apache/paimon/types/DataType.java#L129>
    fn copy_ignore_nullable(&self) -> Self {
        self.copy(false)
    }

    fn serialize_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }

    fn with_nullability(&self, args: Arguments) -> String {
        if !self.is_nullable() {
            format!("{} NOT NULL", args)
        } else {
            format!("{}", args)
        }
    }

    fn accept<T>(&self, visitor: &mut T)
    where
        T: DataTypeVisitor<T>,
    {
        visitor.visit(self);
    }

    fn not_null(&self) -> Self {
        self.copy(false)
    }

    fn nullable(&self) -> Self {
        self.copy(true)
    }

    fn as_sql_string(&self) -> String {
        match self.type_root {
            DataTypeRoot::Char => CharType::default_value().as_sql_string(),
            DataTypeRoot::Varchar => VarCharType::default_value().as_sql_string(),
            DataTypeRoot::Boolean => BooleanType::default_value().as_sql_string(),
            DataTypeRoot::Binary => BinaryType::default_value().as_sql_string(),
            DataTypeRoot::Varbinary => VarBinaryType::default_value().as_sql_string(),
            DataTypeRoot::Decimal => DecimalType::default_value().as_sql_string(),
            DataTypeRoot::Tinyint => TinyIntType::default_value().as_sql_string(),
            DataTypeRoot::Smallint => SmallIntType::default_value().as_sql_string(),
            DataTypeRoot::Integer => IntType::default_value().as_sql_string(),
            DataTypeRoot::Bigint => BigIntType::default_value().as_sql_string(),
            DataTypeRoot::Float => FloatType::default_value().as_sql_string(),
            DataTypeRoot::Double => DoubleType::default_value().as_sql_string(),
            DataTypeRoot::Date => DateType::default_value().as_sql_string(),
            DataTypeRoot::TimeWithoutTimeZone => TimeType::default_value().as_sql_string(),
            DataTypeRoot::TimestampWithoutTimeZone => {
                TimestampType::default_value().as_sql_string()
            }
            DataTypeRoot::TimestampWithLocalTimeZone => {
                LocalZonedTimestampType::default_value().as_sql_string()
            }
            DataTypeRoot::Array => ArrayType::default_value().as_sql_string(),
            DataTypeRoot::Multiset => todo!(),
            DataTypeRoot::Map => todo!(),
            DataTypeRoot::Row => todo!(),
        }
    }
}

/// ArrayType for paimon.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/db8bcd7fdd9c2705435d2ab1d2341c52d1f67ee5/paimon-common/src/main/java/org/apache/paimon/types/ArrayType.java>.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
#[serde(rename_all = "camelCase")]
pub struct ArrayType {
    pub element_type: DataType,
}

impl ArrayType {
    pub fn new(is_nullable: bool) -> Self {
        Self {
            element_type: DataType::new(is_nullable, DataTypeRoot::Array),
        }
    }

    pub fn default_value() -> Self {
        Self::new(true)
    }

    pub fn as_sql_string(&self) -> String {
        self.element_type
            .with_nullability(format_args!("ARRAY<{}>", self.element_type))
    }
}

/// BigIntType for paimon.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/master/paimon-common/src/main/java/org/apache/paimon/types/BigIntType.java>.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct BigIntType {
    pub element_type: DataType,
}

impl BigIntType {
    pub fn new(is_nullable: bool) -> Self {
        Self {
            element_type: DataType::new(is_nullable, DataTypeRoot::Bigint),
        }
    }

    pub fn default_value() -> Self {
        Self::new(true)
    }

    pub fn as_sql_string(&self) -> String {
        self.element_type
            .with_nullability(format_args!("{}", "BIGINT"))
    }
}

/// BinaryType for paimon.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/master/paimon-common/src/main/java/org/apache/paimon/types/BinaryType.java>.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
#[serde(rename_all = "camelCase")]
pub struct BinaryType {
    pub element_type: DataType,
    length: usize,
}

impl BinaryType {
    pub const MIN_LENGTH: usize = 1;

    pub const MAX_LENGTH: usize = isize::MAX as usize;

    pub const DEFAULT_LENGTH: usize = 1;

    pub fn new(is_nullable: bool, length: usize) -> Self {
        Self::new_with_result(is_nullable, length).unwrap()
    }

    pub fn new_with_result(is_nullable: bool, length: usize) -> Result<Self, &'static str> {
        if length < Self::MIN_LENGTH {
            Err("Binary string length must be at least 1.")
        } else {
            Ok(Self {
                element_type: DataType {
                    is_nullable,
                    type_root: DataTypeRoot::Binary,
                },
                length,
            })
        }
    }

    pub fn with_length(length: usize) -> Self {
        Self::new(true, length)
    }

    pub fn default_value() -> Self {
        Self::with_length(Self::DEFAULT_LENGTH)
    }

    pub fn get_length(&self) -> usize {
        self.length
    }

    pub fn as_sql_string(&self) -> String {
        self.element_type
            .with_nullability(format_args!("BINARY({})", self.length))
    }
}

/// BooleanType for paimon.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/master/paimon-common/src/main/java/org/apache/paimon/types/BooleanType.java>.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct BooleanType {
    pub element_type: DataType,
}

impl BooleanType {
    pub fn new(is_nullable: bool) -> Self {
        Self {
            element_type: DataType::new(is_nullable, DataTypeRoot::Boolean),
        }
    }

    pub fn default_value() -> Self {
        Self::new(true)
    }

    pub fn as_sql_string(&self) -> String {
        self.element_type
            .with_nullability(format_args!("{}", "BOOLEAN"))
    }
}

/// CharType for paimon.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/db8bcd7fdd9c2705435d2ab1d2341c52d1f67ee5/paimon-common/src/main/java/org/apache/paimon/types/CharType.java>.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CharType {
    element_type: DataType,
    length: usize,
}

impl CharType {
    pub const DEFAULT_LENGTH: usize = 1;

    pub const MIN_LENGTH: usize = 1;

    pub const MAX_LENGTH: usize = 255;

    pub fn new(is_nullable: bool, length: usize) -> Self {
        Self::new_with_result(is_nullable, length).unwrap()
    }

    pub fn new_with_result(is_nullable: bool, length: usize) -> Result<Self, &'static str> {
        if !(Self::MIN_LENGTH..=Self::MAX_LENGTH).contains(&length) {
            Err("Character string length must be between 1 and 255 (both inclusive).")
        } else {
            Ok(CharType {
                element_type: DataType {
                    is_nullable,
                    type_root: DataTypeRoot::Char,
                },
                length,
            })
        }
    }

    pub fn with_length(length: usize) -> Self {
        Self::new(true, length)
    }

    pub fn default_value() -> Self {
        Self::with_length(Self::DEFAULT_LENGTH)
    }

    pub fn get_length(&self) -> usize {
        self.length
    }

    pub fn as_sql_string(&self) -> String {
        self.element_type
            .with_nullability(format_args!("CHAR({})", self.length))
    }
}

/// DateType for paimon.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/master/paimon-common/src/main/java/org/apache/paimon/types/DateType.java>.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct DateType {
    element_type: DataType,
}

impl DateType {
    pub fn new(is_nullable: bool) -> Self {
        Self {
            element_type: DataType::new(is_nullable, DataTypeRoot::Date),
        }
    }

    pub fn default_value() -> Self {
        Self::new(true)
    }

    pub fn as_sql_string(&self) -> String {
        self.element_type
            .with_nullability(format_args!("{}", "DATE"))
    }
}

/// DecimalType for paimon.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/master/paimon-common/src/main/java/org/apache/paimon/types/DecimalType.java>.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct DecimalType {
    element_type: DataType,
    precision: u32,
    scale: u32,
}

impl DecimalType {
    pub const MIN_PRECISION: u32 = 1;

    pub const MAX_PRECISION: u32 = 38;

    pub const DEFAULT_PRECISION: u32 = 10;

    pub const MIN_SCALE: u32 = 0;

    pub const DEFAULT_SCALE: u32 = 0;

    pub fn new(is_nullable: bool, precision: u32, scale: u32) -> Self {
        Self::new_with_result(is_nullable, precision, scale).unwrap()
    }

    pub fn new_with_result(is_nullable: bool, precision: u32, scale: u32) -> Result<Self, String> {
        if !(Self::MIN_PRECISION..=Self::MAX_PRECISION).contains(&precision) {
            return Err(format!(
                "Decimal precision must be between {} and {} (both inclusive).",
                Self::MIN_PRECISION,
                Self::MAX_PRECISION
            ));
        }

        if !(Self::MIN_SCALE..=precision).contains(&scale) {
            return Err(format!(
                "Decimal scale must be between {} and the precision {} (both inclusive).",
                Self::MIN_SCALE,
                precision
            ));
        }

        Ok(DecimalType {
            element_type: DataType {
                is_nullable,
                type_root: DataTypeRoot::Decimal,
            },
            precision,
            scale,
        })
    }

    pub fn with_precision_and_scale(precision: u32, scale: u32) -> Self {
        Self::new(true, precision, scale)
    }

    pub fn default_value() -> Self {
        Self::with_precision_and_scale(Self::DEFAULT_PRECISION, Self::DEFAULT_SCALE)
    }

    pub fn get_precision(&self) -> u32 {
        self.precision
    }

    pub fn get_scale(&self) -> u32 {
        self.scale
    }

    pub fn as_sql_string(&self) -> String {
        self.element_type
            .with_nullability(format_args!("DECIMAL({}, {})", self.precision, self.scale))
    }
}

/// DoubleType for paimon.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/master/paimon-common/src/main/java/org/apache/paimon/types/DoubleType.java>.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct DoubleType {
    element_type: DataType,
}

impl DoubleType {
    pub fn new(is_nullable: bool) -> Self {
        Self {
            element_type: DataType::new(is_nullable, DataTypeRoot::Double),
        }
    }

    pub fn default_value() -> Self {
        Self::new(true)
    }

    pub fn as_sql_string(&self) -> String {
        self.element_type
            .with_nullability(format_args!("{}", "DOUBLE"))
    }
}

/// FloatType for paimon.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/master/paimon-common/src/main/java/org/apache/paimon/types/FloatType.java>.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct FloatType {
    element_type: DataType,
}

impl FloatType {
    pub fn new(is_nullable: bool) -> Self {
        Self {
            element_type: DataType::new(is_nullable, DataTypeRoot::Float),
        }
    }

    pub fn default_value() -> Self {
        Self::new(true)
    }

    pub fn as_sql_string(&self) -> String {
        self.element_type
            .with_nullability(format_args!("{}", "FLOAT"))
    }
}

/// IntType for paimon.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/master/paimon-common/src/main/java/org/apache/paimon/types/IntType.java>.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct IntType {
    element_type: DataType,
}

impl IntType {
    pub fn new(is_nullable: bool) -> Self {
        Self {
            element_type: DataType::new(is_nullable, DataTypeRoot::Integer),
        }
    }

    pub fn default_value() -> Self {
        Self::new(true)
    }

    pub fn as_sql_string(&self) -> String {
        self.element_type
            .with_nullability(format_args!("{}", "INTEGER"))
    }
}

/// LocalZonedTimestampType for paimon.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/master/paimon-common/src/main/java/org/apache/paimon/types/TimestampType.java>.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct LocalZonedTimestampType {
    element_type: DataType,
    precision: u32,
}

impl LocalZonedTimestampType {
    pub const MIN_PRECISION: u32 = TimestampType::MIN_PRECISION;

    pub const MAX_PRECISION: u32 = TimestampType::MAX_PRECISION;

    pub const DEFAULT_PRECISION: u32 = TimestampType::DEFAULT_PRECISION;

    pub fn new(is_nullable: bool, precision: u32) -> Self {
        LocalZonedTimestampType::new_with_result(is_nullable, precision).unwrap()
    }

    pub fn new_with_result(is_nullable: bool, precision: u32) -> Result<Self, String> {
        if !(Self::MIN_PRECISION..=Self::MAX_PRECISION).contains(&precision) {
            return Err(format!(
                "Timestamp precision must be between {} and {} (both inclusive).",
                Self::MIN_PRECISION,
                Self::MAX_PRECISION
            ));
        }

        Ok(LocalZonedTimestampType {
            element_type: DataType {
                is_nullable,
                type_root: DataTypeRoot::TimestampWithLocalTimeZone,
            },
            precision,
        })
    }

    pub fn with_precision(precision: u32) -> Self {
        Self::new(true, precision)
    }

    pub fn default_value() -> Self {
        Self::with_precision(Self::DEFAULT_PRECISION)
    }

    pub fn get_precision(&self) -> u32 {
        self.precision
    }

    pub fn as_sql_string(&self) -> String {
        self.element_type.with_nullability(format_args!(
            "TIMESTAMP WITH LOCAL TIME ZONE({})",
            self.precision
        ))
    }
}

/// Next TODO: MapType、MultisetType、RowType

/// SmallIntType for paimon.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/master/paimon-common/src/main/java/org/apache/paimon/types/SmallIntType.java>.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct SmallIntType {
    element_type: DataType,
}

impl SmallIntType {
    pub fn new(is_nullable: bool) -> Self {
        Self {
            element_type: DataType::new(is_nullable, DataTypeRoot::Smallint),
        }
    }

    pub fn default_value() -> Self {
        Self::new(true)
    }

    pub fn as_sql_string(&self) -> String {
        self.element_type
            .with_nullability(format_args!("{}", "SMALLINT"))
    }
}

/// TimeType for paimon.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/master/paimon-common/src/main/java/org/apache/paimon/types/TimeType.java>.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct TimeType {
    element_type: DataType,
    precision: u32,
}

impl TimeType {
    pub const MIN_PRECISION: u32 = 0;

    pub const MAX_PRECISION: u32 = 9;

    pub const DEFAULT_PRECISION: u32 = 0;

    pub fn new(is_nullable: bool, precision: u32) -> Self {
        Self::new_with_result(is_nullable, precision).unwrap()
    }

    pub fn new_with_result(is_nullable: bool, precision: u32) -> Result<Self, String> {
        if !(Self::MIN_PRECISION..=Self::MAX_PRECISION).contains(&precision) {
            return Err(format!(
                "Time precision must be between {} and {} (both inclusive).",
                Self::MIN_PRECISION,
                Self::MAX_PRECISION
            ));
        }

        Ok(TimeType {
            element_type: DataType {
                is_nullable,
                type_root: DataTypeRoot::TimeWithoutTimeZone,
            },
            precision,
        })
    }

    pub fn with_precision(precision: u32) -> Self {
        Self::new(true, precision)
    }

    pub fn default_value() -> Self {
        Self::with_precision(TimeType::DEFAULT_PRECISION)
    }

    pub fn get_precision(&self) -> u32 {
        self.precision
    }

    pub fn as_sql_string(&self) -> String {
        self.element_type
            .with_nullability(format_args!("TIME({})", self.precision))
    }
}

/// TimestampType for paimon.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/master/paimon-common/src/main/java/org/apache/paimon/types/TimestampType.java>.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct TimestampType {
    element_type: DataType,
    precision: u32,
}

impl TimestampType {
    pub const MIN_PRECISION: u32 = 0;

    pub const MAX_PRECISION: u32 = 9;

    pub const DEFAULT_PRECISION: u32 = 6;

    pub fn new(is_nullable: bool, precision: u32) -> Self {
        Self::new_with_result(is_nullable, precision).unwrap()
    }

    pub fn new_with_result(is_nullable: bool, precision: u32) -> Result<Self, String> {
        if !(Self::MIN_PRECISION..=Self::MAX_PRECISION).contains(&precision) {
            return Err(format!(
                "Timestamp precision must be between {} and {} (both inclusive).",
                Self::MIN_PRECISION,
                Self::MAX_PRECISION
            ));
        }

        Ok(TimestampType {
            element_type: DataType {
                is_nullable,
                type_root: DataTypeRoot::TimestampWithoutTimeZone,
            },
            precision,
        })
    }

    pub fn with_precision(precision: u32) -> Self {
        Self::new(true, precision)
    }

    pub fn default_value() -> Self {
        Self::with_precision(Self::DEFAULT_PRECISION)
    }

    pub fn get_precision(&self) -> u32 {
        self.precision
    }

    pub fn as_sql_string(&self) -> String {
        self.element_type
            .with_nullability(format_args!("TIMESTAMP({})", self.precision))
    }
}

/// TinyIntType for paimon.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/master/paimon-common/src/main/java/org/apache/paimon/types/TinyIntType.java>.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct TinyIntType {
    element_type: DataType,
}

impl TinyIntType {
    pub fn new(is_nullable: bool) -> Self {
        Self {
            element_type: DataType::new(is_nullable, DataTypeRoot::Tinyint),
        }
    }

    pub fn default_value() -> Self {
        Self::new(true)
    }

    pub fn as_sql_string(&self) -> String {
        self.element_type
            .with_nullability(format_args!("{}", "TINYINT"))
    }
}

/// VarBinaryType for paimon.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/master/paimon-common/src/main/java/org/apache/paimon/types/VarBinaryType.java>.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct VarBinaryType {
    element_type: DataType,
    length: u32,
}

impl VarBinaryType {
    pub const MIN_LENGTH: u32 = 1;

    pub const MAX_LENGTH: u32 = isize::MAX as u32;

    pub const DEFAULT_LENGTH: u32 = 1;

    pub fn new(is_nullable: bool, length: u32) -> Self {
        Self::new_with_result(is_nullable, length).unwrap()
    }

    pub fn new_with_result(is_nullable: bool, length: u32) -> Result<Self, String> {
        if length < Self::MIN_LENGTH {
            return Err("Binary string length must be at least 1.".to_string());
        }

        Ok(VarBinaryType {
            element_type: DataType {
                is_nullable,
                type_root: DataTypeRoot::Varbinary,
            },
            length,
        })
    }

    pub fn with_length(length: u32) -> Self {
        Self::new(true, length)
    }

    pub fn default_value() -> Self {
        Self::with_length(Self::DEFAULT_LENGTH)
    }

    pub fn get_length(&self) -> u32 {
        self.length
    }

    pub fn as_sql_string(&self) -> String {
        self.element_type
            .with_nullability(format_args!("VARBINARY({})", self.length))
    }
}

/// VarCharType for paimon.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/master/paimon-common/src/main/java/org/apache/paimon/types/VarCharType.java>.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct VarCharType {
    element_type: DataType,
    length: u32,
}

impl VarCharType {
    pub const MIN_LENGTH: u32 = 1;

    pub const MAX_LENGTH: u32 = isize::MAX as u32;

    pub const DEFAULT_LENGTH: u32 = 1;

    pub fn new(is_nullable: bool, length: u32) -> Self {
        Self::new_with_result(is_nullable, length).unwrap()
    }

    pub fn new_with_result(is_nullable: bool, length: u32) -> Result<Self, String> {
        if !(Self::MIN_LENGTH..=Self::MAX_LENGTH).contains(&length) {
            return Err(format!(
                "Character string length must be between {} and {} (both inclusive).",
                Self::MIN_LENGTH,
                Self::MAX_LENGTH
            ));
        }

        Ok(VarCharType {
            element_type: DataType {
                is_nullable,
                type_root: DataTypeRoot::Varchar,
            },
            length,
        })
    }

    pub fn with_length(length: u32) -> Self {
        Self::new(true, length)
    }

    pub fn default_value() -> Self {
        Self::with_length(Self::DEFAULT_LENGTH)
    }

    pub fn get_length(&self) -> u32 {
        self.length
    }

    pub fn as_sql_string(&self) -> String {
        self.element_type
            .with_nullability(format_args!("VARCHAR({})", self.length))
    }
}
