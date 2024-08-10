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

use crate::error::DataTypeParsingSnafu;
use crate::spec::types::DataType;
use crate::Error;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashSet, fmt, str::FromStr};

use super::{
    ArrayType, BigIntType, BinaryType, BooleanType, CharType, DataField, DateType, DecimalType,
    DoubleType, FloatType, IntType, LocalZonedTimestampType, MapType, MultisetType, RowType,
    SmallIntType, TimeType, TimestampType, TinyIntType, VarBinaryType, VarCharType,
};

/// --------------------------------------------------------------------------------------------
/// Tokenizer
/// --------------------------------------------------------------------------------------------

const CHAR_BEGIN_SUBTYPE: char = '<';
const CHAR_END_SUBTYPE: char = '>';
const CHAR_BEGIN_PARAMETER: char = '(';
const CHAR_END_PARAMETER: char = ')';
const CHAR_LIST_SEPARATOR: char = ',';
const CHAR_STRING: char = '\'';
const CHAR_IDENTIFIER: char = '`';
const CHAR_DOT: char = '.';

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum Keyword {
    CHAR,
    VARCHAR,
    STRING,
    BOOLEAN,
    BINARY,
    VARBINARY,
    BYTES,
    DECIMAL,
    NUMERIC,
    DEC,
    TINYINT,
    SMALLINT,
    INT,
    INTEGER,
    BIGINT,
    FLOAT,
    DOUBLE,
    PRECISION,
    DATE,
    TIME,
    WITH,
    WITHOUT,
    LOCAL,
    ZONE,
    TIMESTAMP,
    TimestampLtz,
    INTERVAL,
    YEAR,
    MONTH,
    DAY,
    HOUR,
    MINUTE,
    SECOND,
    TO,
    ARRAY,
    MULTISET,
    MAP,
    ROW,
    NULL,
    RAW,
    LEGACY,
    NOT,
}

impl fmt::Display for Keyword {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Keyword::CHAR => write!(f, "CHAR"),
            Keyword::VARCHAR => write!(f, "VARCHAR"),
            Keyword::STRING => write!(f, "STRING"),
            Keyword::BOOLEAN => write!(f, "BOOLEAN"),
            Keyword::BINARY => write!(f, "BINARY"),
            Keyword::VARBINARY => write!(f, "VARBINARY"),
            Keyword::BYTES => write!(f, "BYTES"),
            Keyword::DECIMAL => write!(f, "DECIMAL"),
            Keyword::NUMERIC => write!(f, "NUMERIC"),
            Keyword::DEC => write!(f, "DEC"),
            Keyword::TINYINT => write!(f, "TINYINT"),
            Keyword::SMALLINT => write!(f, "SMALLINT"),
            Keyword::INT => write!(f, "INT"),
            Keyword::INTEGER => write!(f, "INTEGER"),
            Keyword::BIGINT => write!(f, "BIGINT"),
            Keyword::FLOAT => write!(f, "FLOAT"),
            Keyword::DOUBLE => write!(f, "DOUBLE"),
            Keyword::PRECISION => write!(f, "PRECISION"),
            Keyword::DATE => write!(f, "DATE"),
            Keyword::TIME => write!(f, "TIME"),
            Keyword::WITH => write!(f, "WITH"),
            Keyword::WITHOUT => write!(f, "WITHOUT"),
            Keyword::LOCAL => write!(f, "LOCAL"),
            Keyword::ZONE => write!(f, "ZONE"),
            Keyword::TIMESTAMP => write!(f, "TIMESTAMP"),
            Keyword::TimestampLtz => write!(f, "TIMESTAMP_LTZ"),
            Keyword::INTERVAL => write!(f, "INTERVAL"),
            Keyword::YEAR => write!(f, "YEAR"),
            Keyword::MONTH => write!(f, "MONTH"),
            Keyword::DAY => write!(f, "DAY"),
            Keyword::HOUR => write!(f, "HOUR"),
            Keyword::MINUTE => write!(f, "MINUTE"),
            Keyword::SECOND => write!(f, "SECOND"),
            Keyword::TO => write!(f, "TO"),
            Keyword::ARRAY => write!(f, "ARRAY"),
            Keyword::MULTISET => write!(f, "MULTISET"),
            Keyword::MAP => write!(f, "MAP"),
            Keyword::ROW => write!(f, "ROW"),
            Keyword::NULL => write!(f, "NULL"),
            Keyword::RAW => write!(f, "RAW"),
            Keyword::LEGACY => write!(f, "LEGACY"),
            Keyword::NOT => write!(f, "NOT"),
        }
    }
}

impl FromStr for Keyword {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "CHAR" => Ok(Keyword::CHAR),
            "VARCHAR" => Ok(Keyword::VARCHAR),
            "STRING" => Ok(Keyword::STRING),
            "BOOLEAN" => Ok(Keyword::BOOLEAN),
            "BINARY" => Ok(Keyword::BINARY),
            "VARBINARY" => Ok(Keyword::VARBINARY),
            "BYTES" => Ok(Keyword::BYTES),
            "DECIMAL" => Ok(Keyword::DECIMAL),
            "NUMERIC" => Ok(Keyword::NUMERIC),
            "DEC" => Ok(Keyword::DEC),
            "TINYINT" => Ok(Keyword::TINYINT),
            "SMALLINT" => Ok(Keyword::SMALLINT),
            "INT" => Ok(Keyword::INT),
            "INTEGER" => Ok(Keyword::INTEGER),
            "BIGINT" => Ok(Keyword::BIGINT),
            "FLOAT" => Ok(Keyword::FLOAT),
            "DOUBLE" => Ok(Keyword::DOUBLE),
            "PRECISION" => Ok(Keyword::PRECISION),
            "DATE" => Ok(Keyword::DATE),
            "TIME" => Ok(Keyword::TIME),
            "WITH" => Ok(Keyword::WITH),
            "WITHOUT" => Ok(Keyword::WITHOUT),
            "LOCAL" => Ok(Keyword::LOCAL),
            "ZONE" => Ok(Keyword::ZONE),
            "TIMESTAMP" => Ok(Keyword::TIMESTAMP),
            "TIMESTAMP_LTZ" => Ok(Keyword::TimestampLtz),
            "INTERVAL" => Ok(Keyword::INTERVAL),
            "YEAR" => Ok(Keyword::YEAR),
            "MONTH" => Ok(Keyword::MONTH),
            "DAY" => Ok(Keyword::DAY),
            "HOUR" => Ok(Keyword::HOUR),
            "MINUTE" => Ok(Keyword::MINUTE),
            "SECOND" => Ok(Keyword::SECOND),
            "TO" => Ok(Keyword::TO),
            "ARRAY" => Ok(Keyword::ARRAY),
            "MULTISET" => Ok(Keyword::MULTISET),
            "MAP" => Ok(Keyword::MAP),
            "ROW" => Ok(Keyword::ROW),
            "NULL" => Ok(Keyword::NULL),
            "RAW" => Ok(Keyword::RAW),
            "LEGACY" => Ok(Keyword::LEGACY),
            "NOT" => Ok(Keyword::NOT),
            _ => Err(()),
        }
    }
}

impl Keyword {
    pub fn variants() -> HashSet<Keyword> {
        vec![
            Keyword::CHAR,
            Keyword::VARCHAR,
            Keyword::STRING,
            Keyword::BOOLEAN,
            Keyword::BINARY,
            Keyword::VARBINARY,
            Keyword::BYTES,
            Keyword::DECIMAL,
            Keyword::NUMERIC,
            Keyword::DEC,
            Keyword::TINYINT,
            Keyword::SMALLINT,
            Keyword::INT,
            Keyword::INTEGER,
            Keyword::BIGINT,
            Keyword::FLOAT,
            Keyword::DOUBLE,
            Keyword::PRECISION,
            Keyword::DATE,
            Keyword::TIME,
            Keyword::WITH,
            Keyword::WITHOUT,
            Keyword::LOCAL,
            Keyword::ZONE,
            Keyword::TIMESTAMP,
            Keyword::TimestampLtz,
            Keyword::INTERVAL,
            Keyword::YEAR,
            Keyword::MONTH,
            Keyword::DAY,
            Keyword::HOUR,
            Keyword::MINUTE,
            Keyword::SECOND,
            Keyword::TO,
            Keyword::ARRAY,
            Keyword::MULTISET,
            Keyword::MAP,
            Keyword::ROW,
            Keyword::NULL,
            Keyword::RAW,
            Keyword::LEGACY,
            Keyword::NOT,
        ]
        .into_iter()
        .collect()
    }

    pub fn is_keyword(s: &str) -> bool {
        Keyword::variants().iter().any(|v| v.to_string() == s)
    }
}

#[derive(Debug, PartialEq)]
pub enum TokenType {
    /// e.g. "ROW<"
    BeginSubtype,
    /// e.g. "ROW<..>"
    EndSubtype,
    /// e.g. "CHAR("
    BeginParameter,
    /// e.g. "CHAR(...)"
    EndParameter,
    /// e.g. "ROW<INT,"
    ListSeparator,
    /// e.g. "ROW<name INT 'Comment'"
    LiteralString,
    /// CHAR(12
    LiteralInt,
    /// e.g. "CHAR" or "TO"
    Keyword,
    /// e.g. "ROW<name" or "myCatalog.myDatabase"
    Identifier,
    /// e.g. "myCatalog.myDatabase."
    IdentifierSeparator,
}

#[derive(Debug, PartialEq)]
pub struct Token {
    token_type: TokenType,
    cursor_position: isize,
    value: String,
}

impl Token {
    pub fn new(token_type: TokenType, cursor_position: isize, value: String) -> Self {
        Token {
            token_type,
            cursor_position,
            value,
        }
    }
}

pub struct TokenParser {
    input_string: String,
    tokens: Vec<Token>,
    last_valid_token: isize,
    current_token: isize,
}

impl TokenParser {
    pub fn new(input_string: String, tokens: Vec<Token>) -> Self {
        TokenParser {
            input_string,
            tokens,
            last_valid_token: -1,
            current_token: -1,
        }
    }

    pub fn parse_tokens(&mut self) -> Result<DataType, Error> {
        let type_data = self.parse_type_with_nullability();
        if self.has_remaining_tokens() {
            self.next_token();
            self.parsing_error(format!("Unexpected token: {}", self.token().value));
        }
        Ok(type_data)
    }

    pub fn last_cursor(&self) -> usize {
        if self.last_valid_token < 0 {
            0
        } else {
            self.tokens[self.last_valid_token as usize].cursor_position as usize + 1
        }
    }

    pub fn parsing_error(&self, cause: String) -> ! {
        panic!(
            "Could not parse type at position {}: {}\nInput type string: {}",
            self.last_cursor(),
            cause,
            self.input_string
        );
    }

    pub fn has_remaining_tokens(&self) -> bool {
        self.current_token + 1 < self.tokens.len() as isize
    }

    pub fn token(&self) -> &Token {
        &self.tokens[self.current_token as usize]
    }

    pub fn token_as_int(&self) -> isize {
        self.token().value.parse::<isize>().unwrap_or_else(|_| {
            self.parsing_error(String::from("Invalid integer value."));
        })
    }

    pub fn token_as_keyword(&self) -> Keyword {
        if Keyword::is_keyword(self.token().value.as_str()) {
            match self.token().value.as_str() {
                "CHAR" => Keyword::CHAR,
                "VARCHAR" => Keyword::VARCHAR,
                "STRING" => Keyword::STRING,
                "BOOLEAN" => Keyword::BOOLEAN,
                "BINARY" => Keyword::BINARY,
                "VARBINARY" => Keyword::VARBINARY,
                "BYTES" => Keyword::BYTES,
                "DECIMAL" => Keyword::DECIMAL,
                "NUMERIC" => Keyword::NUMERIC,
                "DEC" => Keyword::DEC,
                "TINYINT" => Keyword::TINYINT,
                "SMALLINT" => Keyword::SMALLINT,
                "INT" => Keyword::INT,
                "INTEGER" => Keyword::INTEGER,
                "BIGINT" => Keyword::BIGINT,
                "FLOAT" => Keyword::FLOAT,
                "DOUBLE" => Keyword::DOUBLE,
                "PRECISION" => Keyword::PRECISION,
                "DATE" => Keyword::DATE,
                "TIME" => Keyword::TIME,
                "WITH" => Keyword::WITH,
                "WITHOUT" => Keyword::WITHOUT,
                "LOCAL" => Keyword::LOCAL,
                "ZONE" => Keyword::ZONE,
                "TIMESTAMP" => Keyword::TIMESTAMP,
                "TIMESTAMP_LTZ" => Keyword::TimestampLtz,
                "INTERVAL" => Keyword::INTERVAL,
                "YEAR" => Keyword::YEAR,
                "MONTH" => Keyword::MONTH,
                "DAY" => Keyword::DAY,
                "HOUR" => Keyword::HOUR,
                "MINUTE" => Keyword::MINUTE,
                "SECOND" => Keyword::SECOND,
                "TO" => Keyword::TO,
                "ARRAY" => Keyword::ARRAY,
                "MULTISET" => Keyword::MULTISET,
                "MAP" => Keyword::MAP,
                "ROW" => Keyword::ROW,
                "NULL" => Keyword::NULL,
                "RAW" => Keyword::RAW,
                "LEGACY" => Keyword::LEGACY,
                "NOT" => Keyword::NOT,
                _ => panic!("Unsupported type: {}", self.token().value),
            }
        } else {
            self.parsing_error(format!("Expected keyword, found: {}", self.token().value));
        }
    }

    pub fn next_token(&mut self) {
        self.current_token += 1;
        if self.current_token >= self.tokens.len() as isize {
            self.parsing_error(String::from("Unexpected end."));
        } else {
            self.last_valid_token = self.current_token - 1;
        }
    }

    pub fn next_token_type(&mut self, expected_type: TokenType) {
        self.next_token();
        let token = self.token();
        if token.token_type != expected_type {
            self.parsing_error(format!(
                "<{:?}> expected but was <{:?}>.",
                expected_type, token.token_type
            ));
        }
    }

    pub fn next_token_keyword(&mut self, expected_keyword: Keyword) {
        self.next_token();
        let token = self.token();
        if token.token_type != TokenType::Keyword {
            self.parsing_error(format!(
                "<{:?}> expected but was <{:?}>.",
                expected_keyword, token.token_type
            ));
        }
    }

    pub fn has_next_token_type(&mut self, types: Vec<TokenType>) -> bool {
        if self.current_token + types.len() as isize + 1 > self.tokens.len() as isize {
            return false;
        }
        for (i, type_) in types.iter().enumerate() {
            let look_ahead = &self.tokens[self.current_token as usize + i + 1];
            if look_ahead.token_type != *type_ {
                return false;
            }
        }
        true
    }

    pub fn has_next_token_keyword(&mut self, keywords: Vec<Keyword>) -> bool {
        if self.current_token + keywords.len() as isize + 1 > self.tokens.len() as isize {
            return false;
        }
        for (i, v) in keywords.iter().enumerate() {
            let look_ahead = &self.tokens[self.current_token as usize + i + 1];
            if look_ahead.token_type != TokenType::Keyword
                || *v != Keyword::from_str(look_ahead.value.as_str()).unwrap()
            {
                return false;
            }
        }
        true
    }

    pub fn parse_nullability(&mut self) -> bool {
        if self.has_next_token_keyword(vec![Keyword::NOT, Keyword::NULL]) {
            self.next_token_keyword(Keyword::NOT);
            self.next_token_keyword(Keyword::NULL);
            false
        } else if self.has_next_token_keyword(vec![Keyword::NULL]) {
            self.next_token_keyword(Keyword::NULL);
            true
        } else {
            true
        }
    }

    pub fn parse_type_with_nullability(&mut self) -> DataType {
        let data_type = self
            .parse_type_by_keyword()
            .unwrap()
            .copy(self.parse_nullability());

        if self.has_next_token_keyword(vec![Keyword::ARRAY]) {
            self.next_token_keyword(Keyword::ARRAY);
            return DataType::Array(ArrayType::with_nullable(
                self.parse_nullability(),
                data_type,
            ));
        } else if self.has_next_token_keyword(vec![Keyword::MULTISET]) {
            self.next_token_keyword(Keyword::MULTISET);
            return DataType::Multiset(MultisetType::with_nullable(
                self.parse_nullability(),
                data_type,
            ));
        }

        data_type
    }

    pub fn parse_type_by_keyword(&mut self) -> Result<DataType, Error> {
        self.next_token_type(TokenType::Keyword);
        match self.token_as_keyword() {
            Keyword::CHAR => Ok(self.parse_char_type()),
            Keyword::VARCHAR => Ok(self.parse_var_char_type()),
            Keyword::STRING => Ok(DataType::VarChar(VarCharType::default())),
            Keyword::BOOLEAN => Ok(DataType::Boolean(BooleanType::new())),
            Keyword::BINARY => Ok(self.parse_binary_type()),
            Keyword::VARBINARY => Ok(self.parse_var_binary_type()),
            Keyword::BYTES => Ok(DataType::VarBinary(
                VarBinaryType::new(VarBinaryType::MAX_LENGTH).unwrap(),
            )),
            Keyword::DECIMAL | Keyword::NUMERIC | Keyword::DEC => Ok(self.parse_decimal_type()),
            Keyword::TINYINT => Ok(DataType::TinyInt(TinyIntType::default())),
            Keyword::SMALLINT => Ok(DataType::SmallInt(SmallIntType::default())),
            Keyword::INT | Keyword::INTEGER => Ok(DataType::Int(IntType::default())),
            Keyword::BIGINT => Ok(DataType::BigInt(BigIntType::default())),
            Keyword::FLOAT => Ok(DataType::Float(FloatType::new())),
            Keyword::DOUBLE => Ok(self.parse_double_type()),
            Keyword::DATE => Ok(DataType::Date(DateType::new())),
            Keyword::TIME => Ok(self.parse_time_type()),
            Keyword::TIMESTAMP => Ok(self.parse_timestamp_type()),
            Keyword::TimestampLtz => Ok(self.parse_timestamp_ltz_type()),
            _ => DataTypeParsingSnafu {
                message: format!("Unsupported type: {}", self.token().value),
            }
            .fail(),
        }
    }

    pub fn parse_string_type(&mut self) -> isize {
        if self.has_next_token_type(vec![TokenType::BeginParameter]) {
            self.next_token_type(TokenType::BeginParameter);
            self.next_token_type(TokenType::LiteralInt);
            let length = self.token_as_int();
            self.next_token_type(TokenType::EndParameter);
            length
        } else {
            -1
        }
    }

    pub fn parse_char_type(&mut self) -> DataType {
        let length = self.parse_string_type();
        if length < 0 {
            DataType::Char(CharType::default())
        } else {
            DataType::Char(CharType::new(length as usize).unwrap())
        }
    }

    pub fn parse_var_char_type(&mut self) -> DataType {
        let length = self.parse_string_type();
        if length < 0 {
            DataType::VarChar(VarCharType::default())
        } else {
            DataType::VarChar(VarCharType::new(length as u32).unwrap())
        }
    }

    pub fn parse_binary_type(&mut self) -> DataType {
        let length = self.parse_string_type();
        if length < 0 {
            DataType::Binary(BinaryType::default())
        } else {
            DataType::Binary(BinaryType::new(length as usize).unwrap())
        }
    }

    pub fn parse_var_binary_type(&mut self) -> DataType {
        let length = self.parse_string_type();
        if length < 0 {
            DataType::VarBinary(VarBinaryType::default())
        } else {
            DataType::VarBinary(VarBinaryType::new(length as u32).unwrap())
        }
    }

    pub fn parse_decimal_type(&mut self) -> DataType {
        let mut precision = DecimalType::DEFAULT_PRECISION;
        let mut scale = DecimalType::DEFAULT_SCALE;

        if self.has_next_token_type(vec![TokenType::BeginParameter]) {
            self.next_token_type(TokenType::BeginParameter);
            self.next_token_type(TokenType::LiteralInt);
            precision = self.token_as_int() as u32;

            if self.has_next_token_type(vec![TokenType::ListSeparator]) {
                self.next_token_type(TokenType::ListSeparator);
                self.next_token_type(TokenType::LiteralInt);
                scale = self.token_as_int() as u32;
            }
            self.next_token_type(TokenType::EndParameter);
        }

        DataType::Decimal(DecimalType::new(precision, scale).unwrap())
    }

    pub fn parse_double_type(&mut self) -> DataType {
        if self.has_next_token_keyword(vec![Keyword::PRECISION]) {
            self.next_token_keyword(Keyword::PRECISION);
        }
        DataType::Double(DoubleType::new())
    }

    pub fn parse_time_type(&mut self) -> DataType {
        let precision = self.parse_optional_precision(TimeType::DEFAULT_PRECISION);

        if self.has_next_token_keyword(vec![Keyword::WITHOUT]) {
            self.next_token_keyword(Keyword::WITHOUT);
            self.next_token_keyword(Keyword::TIME);
            self.next_token_keyword(Keyword::ZONE);
        }

        DataType::Time(TimeType::new(precision).unwrap())
    }

    pub fn parse_timestamp_type(&mut self) -> DataType {
        let precision = self.parse_optional_precision(TimestampType::DEFAULT_PRECISION);

        if self.has_next_token_keyword(vec![Keyword::WITHOUT]) {
            self.next_token_keyword(Keyword::WITHOUT);
            self.next_token_keyword(Keyword::TIME);
            self.next_token_keyword(Keyword::ZONE);
        } else if self.has_next_token_keyword(vec![Keyword::WITH]) {
            self.next_token_keyword(Keyword::WITH);
            if self.has_next_token_keyword(vec![Keyword::LOCAL]) {
                self.next_token_keyword(Keyword::LOCAL);
                self.next_token_keyword(Keyword::TIME);
                self.next_token_keyword(Keyword::ZONE);
                return DataType::LocalZonedTimestamp(
                    LocalZonedTimestampType::new(precision).unwrap(),
                );
            }
        }
        DataType::Timestamp(TimestampType::new(precision).unwrap())
    }

    pub fn parse_timestamp_ltz_type(&mut self) -> DataType {
        let precision = self.parse_optional_precision(LocalZonedTimestampType::DEFAULT_PRECISION);
        DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(precision).unwrap())
    }

    pub fn parse_optional_precision(&mut self, default_precision: u32) -> u32 {
        let mut precision = default_precision;
        if self.has_next_token_type(vec![TokenType::BeginParameter]) {
            self.next_token_type(TokenType::BeginParameter);
            self.next_token_type(TokenType::LiteralInt);
            precision = self.token_as_int() as u32;
            self.next_token_type(TokenType::EndParameter);
        }
        precision
    }
}

#[derive(Debug, Default)]
pub struct DataTypeJsonParser {}

impl DataTypeJsonParser {
    pub fn is_delimiter(&mut self, character: char) -> bool {
        character.is_whitespace()
            || character == CHAR_BEGIN_SUBTYPE
            || character == CHAR_END_SUBTYPE
            || character == CHAR_BEGIN_PARAMETER
            || character == CHAR_END_PARAMETER
            || character == CHAR_LIST_SEPARATOR
            || character == CHAR_DOT
    }

    pub fn is_digit(&mut self, c: char) -> bool {
        c.is_ascii_digit()
    }

    pub fn consume_escaped(
        &mut self,
        builder: &mut String,
        chars: &str,
        mut cursor: usize,
        delimiter: char,
    ) -> usize {
        // skip delimiter
        cursor += 1;

        while cursor < chars.len() {
            let cur_char = chars.chars().nth(cursor).unwrap();
            if cur_char == delimiter
                && cursor + 1 < chars.len()
                && chars.chars().nth(cursor + 1).unwrap() == delimiter
            {
                // escaping of the escaping char e.g. "'Hello '' World'"
                cursor += 1;
                builder.push(cur_char);
            } else if cur_char == delimiter {
                break;
            } else {
                builder.push(cur_char);
            }
            cursor += 1;
        }
        cursor
    }

    pub fn consume_int(&mut self, builder: &mut String, chars: &str, mut cursor: usize) -> usize {
        while cursor < chars.len() && self.is_digit(chars.chars().nth(cursor).unwrap()) {
            builder.push(chars.chars().nth(cursor).unwrap());
            cursor += 1;
        }
        cursor - 1
    }

    pub fn consume_identifier(
        &mut self,
        builder: &mut String,
        chars: &str,
        mut cursor: usize,
    ) -> usize {
        while cursor < chars.len() && !self.is_delimiter(chars.chars().nth(cursor).unwrap()) {
            builder.push(chars.chars().nth(cursor).unwrap());
            cursor += 1;
        }
        cursor - 1
    }

    pub fn tokenize(&mut self, chars: &str) -> Vec<Token> {
        let mut tokens = Vec::new();
        let mut builder = String::new();
        let mut cursor = 0;
        while cursor < chars.len() {
            let cur_char = chars.chars().nth(cursor).unwrap();
            match cur_char {
                CHAR_BEGIN_SUBTYPE => {
                    tokens.push(Token::new(
                        TokenType::BeginSubtype,
                        cursor as isize,
                        cur_char.to_string(),
                    ));
                }
                CHAR_END_SUBTYPE => {
                    tokens.push(Token::new(
                        TokenType::EndSubtype,
                        cursor as isize,
                        cur_char.to_string(),
                    ));
                }
                CHAR_BEGIN_PARAMETER => {
                    tokens.push(Token::new(
                        TokenType::BeginParameter,
                        cursor as isize,
                        cur_char.to_string(),
                    ));
                }
                CHAR_END_PARAMETER => {
                    tokens.push(Token::new(
                        TokenType::EndParameter,
                        cursor as isize,
                        cur_char.to_string(),
                    ));
                }
                CHAR_LIST_SEPARATOR => {
                    tokens.push(Token::new(
                        TokenType::ListSeparator,
                        cursor as isize,
                        cur_char.to_string(),
                    ));
                }
                CHAR_DOT => {
                    tokens.push(Token::new(
                        TokenType::IdentifierSeparator,
                        cursor as isize,
                        cur_char.to_string(),
                    ));
                }
                CHAR_STRING => {
                    builder.clear();
                    cursor = self.consume_escaped(&mut builder, chars, cursor, CHAR_STRING);
                    tokens.push(Token::new(
                        TokenType::LiteralString,
                        cursor as isize,
                        builder.clone(),
                    ));
                }
                CHAR_IDENTIFIER => {
                    builder.clear();
                    cursor = self.consume_escaped(&mut builder, chars, cursor, CHAR_IDENTIFIER);
                    tokens.push(Token::new(
                        TokenType::Identifier,
                        cursor as isize,
                        builder.clone(),
                    ));
                }
                _ => {
                    if cur_char.is_whitespace() {
                        cursor += 1;
                        continue;
                    }
                    if self.is_digit(cur_char) {
                        builder.clear();
                        cursor = self.consume_int(&mut builder, chars, cursor);
                        tokens.push(Token::new(
                            TokenType::LiteralInt,
                            cursor as isize,
                            builder.clone(),
                        ));
                        cursor += 1;
                        continue;
                    }
                    builder.clear();
                    cursor = self.consume_identifier(&mut builder, chars, cursor);
                    let token = builder.clone();
                    let normalized_token = token.to_uppercase();
                    if Keyword::is_keyword(&normalized_token) {
                        tokens.push(Token::new(
                            TokenType::Keyword,
                            cursor as isize,
                            normalized_token,
                        ));
                    } else {
                        tokens.push(Token::new(TokenType::Identifier, cursor as isize, token));
                    }
                }
            }
            cursor += 1;
        }
        tokens
    }

    pub fn parse_atomic_type_sql_string(&mut self, string: &str) -> Result<DataType, Error> {
        let tokens = self.tokenize(string);
        let mut converter = TokenParser::new(string.to_string(), tokens);
        converter.parse_tokens()
    }

    pub fn parse_data_field(&mut self, json: Value) -> DataField {
        let id = json["id"].as_i64().unwrap() as i32;
        let name = json["name"].as_str().unwrap().to_string();
        let data_type = self.parse_data_type(json["type"].clone());

        let description = json
            .get("description")
            .map(|description_node| description_node.as_str().unwrap().to_string());

        DataField::new(id, name, data_type.unwrap()).with_description(description)
    }

    pub fn parse_data_type(&mut self, json: Value) -> Result<DataType, Error> {
        if json.is_string() {
            return self.parse_atomic_type_sql_string(json.as_str().unwrap());
        } else if json.is_object() {
            let type_string = json["type"].as_str().unwrap();
            if type_string.starts_with("ARRAY") {
                let element = self.parse_data_type(json["element"].clone()).unwrap();
                return Ok(DataType::Array(ArrayType::with_nullable(
                    !type_string.contains("NOT NULL"),
                    element,
                )));
            } else if type_string.starts_with("MULTISET") {
                let element = self.parse_data_type(json["element"].clone()).unwrap();
                return Ok(DataType::Multiset(MultisetType::with_nullable(
                    !type_string.contains("NOT NULL"),
                    element,
                )));
            } else if type_string.starts_with("MAP") {
                let key = self.parse_data_type(json["key"].clone()).unwrap();
                let value = self.parse_data_type(json["value"].clone()).unwrap();
                return Ok(DataType::Map(MapType::with_nullable(
                    !type_string.contains("NOT NULL"),
                    key,
                    value,
                )));
            } else if type_string.starts_with("ROW") {
                let field_array = json["fields"].clone();
                let mut fields = Vec::new();
                for field in field_array.as_array().unwrap() {
                    fields.push(self.parse_data_field(field.clone()))
                }
                return Ok(DataType::Row(RowType::with_nullable(
                    !type_string.contains("NOT NULL"),
                    fields,
                )));
            } else {
                return DataTypeParsingSnafu {
                    message: format!("Can not parse: {:?}", json),
                }
                .fail();
            }
        } else {
            return DataTypeParsingSnafu {
                message: format!("Can not parse: {:?}", json),
            }
            .fail();
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use crate::spec::{
        ArrayType, BigIntType, BinaryType, BooleanType, CharType, DataField, DataType,
        DataTypeJsonParser, DateType, DecimalType, DoubleType, FloatType, IntType,
        LocalZonedTimestampType, MapType, MultisetType, RowType, SmallIntType, TimeType,
        TimestampType, TinyIntType, VarBinaryType, VarCharType,
    };

    #[derive(Debug)]
    struct TestSpec {
        json_value: Value,
        expected_type: Option<DataType>,
    }

    impl TestSpec {
        fn new(json_value: Value) -> Self {
            Self {
                json_value,
                expected_type: None,
            }
        }

        fn with_string(json_string: String) -> Self {
            Self {
                json_value: json!(json_string),
                expected_type: None,
            }
        }

        fn expect_type(&mut self, expected_type: DataType) -> Self {
            Self {
                json_value: self.json_value.clone(),
                expected_type: Some(expected_type),
            }
        }
    }

    /// DataTypeJsonParser tests
    ///
    /// Impl Reference: https://github.com/apache/paimon/blob/master/paimon-core/src/test/java/org/apache/paimon/schema/DataTypeJsonParserTest.java
    fn test_data() -> Vec<TestSpec> {
        let mut tests = Vec::new();
        tests.push(
            TestSpec::with_string("CHAR".to_string())
                .expect_type(DataType::Char(CharType::default())),
        );
        tests.push(
            TestSpec::with_string("CHAR NOT NULL".to_string())
                .expect_type(DataType::Char(CharType::with_nullable(false, 1).unwrap())),
        );
        tests.push(
            TestSpec::with_string("char not null".to_string())
                .expect_type(DataType::Char(CharType::with_nullable(false, 1).unwrap())),
        );
        tests.push(
            TestSpec::with_string("CHAR NULL".to_string())
                .expect_type(DataType::Char(CharType::default())),
        );
        tests.push(
            TestSpec::with_string("CHAR(33)".to_string())
                .expect_type(DataType::Char(CharType::with_nullable(true, 33).unwrap())),
        );
        tests.push(
            TestSpec::with_string("VARCHAR".to_string())
                .expect_type(DataType::VarChar(VarCharType::default())),
        );
        tests.push(
            TestSpec::with_string("VARCHAR(33)".to_string()).expect_type(DataType::VarChar(
                VarCharType::with_nullable(true, 33).unwrap(),
            )),
        );
        tests.push(
            TestSpec::with_string("STRING".to_string())
                .expect_type(DataType::VarChar(VarCharType::default())),
        );
        tests.push(
            TestSpec::with_string("BOOLEAN".to_string())
                .expect_type(DataType::Boolean(BooleanType::default())),
        );
        tests.push(
            TestSpec::with_string("BINARY".to_string())
                .expect_type(DataType::Binary(BinaryType::default())),
        );
        tests.push(
            TestSpec::with_string("BINARY(33)".to_string()).expect_type(DataType::Binary(
                BinaryType::with_nullable(true, 33).unwrap(),
            )),
        );
        tests.push(
            TestSpec::with_string("VARBINARY".to_string())
                .expect_type(DataType::VarBinary(VarBinaryType::default())),
        );
        tests.push(
            TestSpec::with_string("VARBINARY(33)".to_string()).expect_type(DataType::VarBinary(
                VarBinaryType::with_nullable(true, 33).unwrap(),
            )),
        );
        tests.push(
            TestSpec::with_string("BYTES".to_string()).expect_type(DataType::VarBinary(
                VarBinaryType::with_nullable(true, VarBinaryType::MAX_LENGTH).unwrap(),
            )),
        );
        tests.push(
            TestSpec::with_string("DECIMAL".to_string())
                .expect_type(DataType::Decimal(DecimalType::default())),
        );
        tests.push(
            TestSpec::with_string("DEC".to_string())
                .expect_type(DataType::Decimal(DecimalType::default())),
        );
        tests.push(
            TestSpec::with_string("NUMERIC".to_string())
                .expect_type(DataType::Decimal(DecimalType::default())),
        );
        tests.push(
            TestSpec::with_string("DECIMAL(10)".to_string()).expect_type(DataType::Decimal(
                DecimalType::with_nullable(true, 10, 0).unwrap(),
            )),
        );
        tests.push(
            TestSpec::with_string("DEC(10)".to_string()).expect_type(DataType::Decimal(
                DecimalType::with_nullable(true, 10, 0).unwrap(),
            )),
        );
        tests.push(
            TestSpec::with_string("NUMERIC(10)".to_string()).expect_type(DataType::Decimal(
                DecimalType::with_nullable(true, 10, 0).unwrap(),
            )),
        );
        tests.push(
            TestSpec::with_string("DECIMAL(10, 3)".to_string()).expect_type(DataType::Decimal(
                DecimalType::with_nullable(true, 10, 3).unwrap(),
            )),
        );
        tests.push(
            TestSpec::with_string("DEC(10, 3)".to_string()).expect_type(DataType::Decimal(
                DecimalType::with_nullable(true, 10, 3).unwrap(),
            )),
        );
        tests.push(
            TestSpec::with_string("NUMERIC(10, 3)".to_string()).expect_type(DataType::Decimal(
                DecimalType::with_nullable(true, 10, 3).unwrap(),
            )),
        );
        tests.push(
            TestSpec::with_string("TINYINT".to_string())
                .expect_type(DataType::TinyInt(TinyIntType::default())),
        );
        tests.push(
            TestSpec::with_string("SMALLINT".to_string())
                .expect_type(DataType::SmallInt(SmallIntType::default())),
        );
        tests.push(
            TestSpec::with_string("INTEGER".to_string())
                .expect_type(DataType::Int(IntType::default())),
        );
        tests.push(
            TestSpec::with_string("INT".to_string()).expect_type(DataType::Int(IntType::default())),
        );
        tests.push(
            TestSpec::with_string("BIGINT".to_string())
                .expect_type(DataType::BigInt(BigIntType::default())),
        );
        tests.push(
            TestSpec::with_string("FLOAT".to_string())
                .expect_type(DataType::Float(FloatType::default())),
        );
        tests.push(
            TestSpec::with_string("DOUBLE".to_string())
                .expect_type(DataType::Double(DoubleType::default())),
        );
        tests.push(
            TestSpec::with_string("DOUBLE PRECISION".to_string())
                .expect_type(DataType::Double(DoubleType::default())),
        );
        tests.push(
            TestSpec::with_string("DATE".to_string())
                .expect_type(DataType::Date(DateType::default())),
        );
        tests.push(
            TestSpec::with_string("TIME".to_string())
                .expect_type(DataType::Time(TimeType::default())),
        );
        tests.push(
            TestSpec::with_string("TIME(3)".to_string())
                .expect_type(DataType::Time(TimeType::new(3).unwrap())),
        );
        tests.push(
            TestSpec::with_string("TIME WITHOUT TIME ZONE".to_string())
                .expect_type(DataType::Time(TimeType::default())),
        );
        tests.push(
            TestSpec::with_string("TIME(3) WITHOUT TIME ZONE".to_string())
                .expect_type(DataType::Time(TimeType::new(3).unwrap())),
        );
        tests.push(
            TestSpec::with_string("TIMESTAMP".to_string())
                .expect_type(DataType::Timestamp(TimestampType::default())),
        );
        tests.push(
            TestSpec::with_string("TIMESTAMP(3)".to_string())
                .expect_type(DataType::Timestamp(TimestampType::new(3).unwrap())),
        );
        tests.push(
            TestSpec::with_string("TIMESTAMP WITHOUT TIME ZONE".to_string())
                .expect_type(DataType::Timestamp(TimestampType::default())),
        );
        tests.push(
            TestSpec::with_string("TIMESTAMP(3) WITHOUT TIME ZONE".to_string())
                .expect_type(DataType::Timestamp(TimestampType::new(3).unwrap())),
        );
        tests.push(
            TestSpec::with_string("TIMESTAMP WITH LOCAL TIME ZONE".to_string()).expect_type(
                DataType::LocalZonedTimestamp(LocalZonedTimestampType::default()),
            ),
        );
        tests.push(
            TestSpec::with_string("TIMESTAMP_LTZ".to_string()).expect_type(
                DataType::LocalZonedTimestamp(LocalZonedTimestampType::default()),
            ),
        );
        tests.push(
            TestSpec::with_string("TIMESTAMP(3) WITH LOCAL TIME ZONE".to_string()).expect_type(
                DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(3).unwrap()),
            ),
        );
        tests.push(
            TestSpec::with_string("TIMESTAMP_LTZ(3)".to_string()).expect_type(
                DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(3).unwrap()),
            ),
        );
        tests.push(
            TestSpec::new(json!({"type":"ARRAY","element":"TIMESTAMP(3) WITH LOCAL TIME ZONE"}))
                .expect_type(DataType::Array(ArrayType::with_nullable(
                    true,
                    DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(3).unwrap()),
                ))),
        );
        tests.push(
            TestSpec::new(json!({"type":"ARRAY","element":"INT NOT NULL"})).expect_type(
                DataType::Array(ArrayType::with_nullable(
                    true,
                    DataType::Int(IntType::with_nullable(false)),
                )),
            ),
        );
        tests.push(
            TestSpec::new(json!({"type":"ARRAY","element":"INT"})).expect_type(DataType::Array(
                ArrayType::with_nullable(true, DataType::Int(IntType::default())),
            )),
        );
        tests.push(
            TestSpec::new(json!({"type":"ARRAY NOT NULL","element":"INT"})).expect_type(
                DataType::Array(ArrayType::with_nullable(
                    false,
                    DataType::Int(IntType::default()),
                )),
            ),
        );
        tests.push(
            TestSpec::new(json!({"type":"MULTISET","element":"INT NOT NULL"})).expect_type(
                DataType::Multiset(MultisetType::with_nullable(
                    true,
                    DataType::Int(IntType::with_nullable(false)),
                )),
            ),
        );
        tests.push(
            TestSpec::new(json!({"type":"MULTISET","element":"INT"})).expect_type(
                DataType::Multiset(MultisetType::with_nullable(
                    true,
                    DataType::Int(IntType::default()),
                )),
            ),
        );
        tests.push(
            TestSpec::new(json!({"type":"MULTISET","element":"INT NOT NULL"})).expect_type(
                DataType::Multiset(MultisetType::with_nullable(
                    true,
                    DataType::Int(IntType::with_nullable(false)),
                )),
            ),
        );
        tests.push(
            TestSpec::new(json!({"type":"MULTISET NOT NULL","element":"INT"})).expect_type(
                DataType::Multiset(MultisetType::with_nullable(
                    false,
                    DataType::Int(IntType::default()),
                )),
            ),
        );
        tests.push(
            TestSpec::new(json!({"type":"MAP","key":"BIGINT","value":"BOOLEAN"})).expect_type(
                DataType::Map(MapType::with_nullable(
                    true,
                    DataType::BigInt(BigIntType::default()),
                    DataType::Boolean(BooleanType::default()),
                )),
            ),
        );
        tests.push(TestSpec::new(
            json!({"type":"ROW","fields":[{"id":0,"name":"f0","type":"INT NOT NULL"},{"id":1,"name":"f1","type":"BOOLEAN"}]}),
        ).expect_type(DataType::Row(RowType::with_nullable(
            true,
            vec![
                DataField::new(0, "f0".to_string(), DataType::Int(IntType::with_nullable(false))),
                DataField::new(1, "f1".to_string(), DataType::Boolean(BooleanType::default())),
            ],
        ))));
        tests.push(TestSpec::new(
            json!({"type":"ROW","fields":[{"id":0,"name":"f0","type":"INT NOT NULL"},{"id":1,"name":"f1","type":"BOOLEAN"}]}),
        ).expect_type(DataType::Row(RowType::with_nullable(
            true,
            vec![
                DataField::new(0, "f0".to_string(), DataType::Int(IntType::with_nullable(false))),
                DataField::new(1, "f1".to_string(), DataType::Boolean(BooleanType::default())),
            ],
        ))));
        tests.push(
            TestSpec::new(json!({"type":"ROW","fields":[{"id":0,"name":"f0","type":"INT"}]}))
                .expect_type(DataType::Row(RowType::with_nullable(
                    true,
                    vec![DataField::new(
                        0,
                        "f0".to_string(),
                        DataType::Int(IntType::default()),
                    )],
                ))),
        );
        tests.push(
            TestSpec::new(json!({"type":"ROW","fields":[]}))
                .expect_type(DataType::Row(RowType::with_nullable(true, vec![]))),
        );
        tests.push(TestSpec::new(
            json!({"type":"ROW","fields":[{"id":0,"name":"f0","type":"INT NOT NULL","description":"This is a comment."},{"id":1,"name":"f1","type":"BOOLEAN","description":"This as well."}]}),
        ).expect_type(DataType::Row(RowType::with_nullable(
            true,
            vec![
                DataField::new(0, "f0".to_string(), DataType::Int(IntType::with_nullable(false))).with_description(Some("This is a comment.".to_string())),
                DataField::new(1, "f1".to_string(), DataType::Boolean(BooleanType::default())).with_description(Some("This as well.".to_string())),
            ],
        ))));

        tests
    }

    #[test]
    fn test_data_type_json_parser() {
        let tests = test_data();
        for test in tests {
            assert_eq!(
                DataTypeJsonParser::default()
                    .parse_data_type(test.json_value)
                    .unwrap(),
                test.expected_type.unwrap()
            );
        }
    }
}
