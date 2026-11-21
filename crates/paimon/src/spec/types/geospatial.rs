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

use super::DataTypeFamily;
use crate::{Error, Result};
use regex::Regex;
use serde_with::{DeserializeFromStr, SerializeDisplay};
use std::fmt::{self, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::{Arc, LazyLock};

const DEFAULT_CRS: &str = "OGC:CRS84";

// Java's Character.isWhitespace(char) excludes non-breaking spaces and includes
// the four ASCII information separators. Rust's Unicode whitespace class differs.
const JAVA_WHITESPACE: &str =
    r"\x09-\x0d\x1c-\x20\u1680\u2000-\u2006\u2008-\u200a\u2028\u2029\u205f\u3000";
const CRS_PUNCTUATION: &str = "<>(),.'`";

/// Preserve the original spelling for metadata and compare CRS names ignoring case.
#[derive(Debug, Clone)]
pub(super) struct Crs(Arc<str>);

impl Crs {
    fn new(value: String) -> Result<Self> {
        if value.is_empty() {
            return Err(Error::DataTypeInvalid {
                message: "Empty CRS".into(),
            });
        }
        Ok(Self(value.into()))
    }

    fn folded(&self) -> impl Iterator<Item = char> + '_ {
        // Java compares individual characters using simple upper/lower case mappings.
        self.0.chars().map(|c| {
            let mut upper = c.to_uppercase();
            let first = upper.next().unwrap();
            let simple = if upper.next().is_none() { first } else { c };
            simple.to_lowercase().next().unwrap()
        })
    }
}

impl PartialEq for Crs {
    fn eq(&self, other: &Self) -> bool {
        self.folded().eq(other.folded())
    }
}

impl Eq for Crs {}

impl Hash for Crs {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for c in self.folded() {
            c.hash(state);
        }
        // Terminate this variable-length field before hashing the algorithm.
        u32::MAX.hash(state);
    }
}

impl Display for Crs {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        static NEEDS_QUOTES: LazyLock<Regex> = LazyLock::new(|| {
            // Java checks the first UTF-16 code unit for a decimal digit.
            Regex::new(&format!(
                r"^[\d&&[\x00-\uFFFF]]|[{JAVA_WHITESPACE}{CRS_PUNCTUATION}]"
            ))
            .unwrap()
        });
        if NEEDS_QUOTES.is_match(&self.0) {
            write!(f, "'{}'", self.0.replace('\'', "''"))
        } else {
            f.write_str(&self.0)
        }
    }
}

/// Interpolation of edges on a geography.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum EdgeAlgorithm {
    #[default]
    Spherical,
    Vincenty,
    Thomas,
    Andoyer,
    Karney,
}

impl Display for EdgeAlgorithm {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Spherical => "spherical",
            Self::Vincenty => "vincenty",
            Self::Thomas => "thomas",
            Self::Andoyer => "andoyer",
            Self::Karney => "karney",
        })
    }
}

impl FromStr for EdgeAlgorithm {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "spherical" => Ok(Self::Spherical),
            "vincenty" => Ok(Self::Vincenty),
            "thomas" => Ok(Self::Thomas),
            "andoyer" => Ok(Self::Andoyer),
            "karney" => Ok(Self::Karney),
            _ => Err(Error::DataTypeInvalid {
                message: format!("Invalid edge interpolation algorithm: {s}"),
            }),
        }
    }
}

/// Planar geometry with a coordinate reference system.
#[derive(Debug, Clone, PartialEq, Eq, Hash, SerializeDisplay, DeserializeFromStr)]
pub struct GeometryType {
    pub(super) nullable: bool,
    pub(super) crs: Crs,
}

impl GeometryType {
    pub const DEFAULT_CRS: &'static str = DEFAULT_CRS;

    pub fn new(crs: impl Into<String>) -> Result<Self> {
        Self::with_nullable(true, crs)
    }

    pub fn with_nullable(nullable: bool, crs: impl Into<String>) -> Result<Self> {
        Ok(Self {
            nullable,
            crs: Crs::new(crs.into())?,
        })
    }

    pub fn crs(&self) -> &str {
        &self.crs.0
    }

    pub fn family(&self) -> DataTypeFamily {
        DataTypeFamily::PREDEFINED
    }
}

impl Default for GeometryType {
    fn default() -> Self {
        Self::new(DEFAULT_CRS).unwrap()
    }
}

impl Display for GeometryType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GEOMETRY({}){}",
            self.crs,
            if self.nullable { "" } else { " NOT NULL" }
        )
    }
}

impl FromStr for GeometryType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let (nullable, crs, algorithm) = parse_parameters("GEOMETRY", s)?;
        if algorithm.is_some() {
            return Err(Error::DataTypeInvalid {
                message: format!("Unexpected GEOMETRY algorithm in {s}"),
            });
        }
        Self::with_nullable(nullable, crs)
    }
}

/// Geography with a coordinate reference system and an edge interpolation algorithm.
#[derive(Debug, Clone, PartialEq, Eq, Hash, SerializeDisplay, DeserializeFromStr)]
pub struct GeographyType {
    pub(super) nullable: bool,
    pub(super) crs: Crs,
    pub(super) algorithm: EdgeAlgorithm,
}

impl GeographyType {
    pub const DEFAULT_CRS: &'static str = DEFAULT_CRS;
    pub const DEFAULT_ALGORITHM: EdgeAlgorithm = EdgeAlgorithm::Spherical;

    pub fn new(crs: impl Into<String>, algorithm: EdgeAlgorithm) -> Result<Self> {
        Self::with_nullable(true, crs, algorithm)
    }

    pub fn with_nullable(
        nullable: bool,
        crs: impl Into<String>,
        algorithm: EdgeAlgorithm,
    ) -> Result<Self> {
        Ok(Self {
            nullable,
            crs: Crs::new(crs.into())?,
            algorithm,
        })
    }

    pub fn crs(&self) -> &str {
        &self.crs.0
    }

    pub fn algorithm(&self) -> EdgeAlgorithm {
        self.algorithm
    }

    pub fn family(&self) -> DataTypeFamily {
        DataTypeFamily::PREDEFINED
    }
}

impl Default for GeographyType {
    fn default() -> Self {
        Self::new(DEFAULT_CRS, Self::DEFAULT_ALGORITHM).unwrap()
    }
}

impl Display for GeographyType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GEOGRAPHY({}, {}){}",
            self.crs,
            self.algorithm,
            if self.nullable { "" } else { " NOT NULL" }
        )
    }
}

impl FromStr for GeographyType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let (nullable, crs, algorithm) = parse_parameters("GEOGRAPHY", s)?;
        Self::with_nullable(
            nullable,
            crs,
            algorithm
                .map(|s| s.parse())
                .transpose()?
                .unwrap_or_default(),
        )
    }
}

fn parse_parameters(name: &str, input: &str) -> Result<(bool, String, Option<String>)> {
    // Java accepts identifiers (including backtick-quoted identifiers) and SQL string literals.
    // Splitting on commas would split a quoted CRS definition in the middle.
    static PARAMS: LazyLock<Regex> = LazyLock::new(|| {
        let token = format!(
            r"(?:'(?:[^']|'')*'|`(?:[^`]|``)*`|[^{JAVA_WHITESPACE}{CRS_PUNCTUATION}0-9][^{JAVA_WHITESPACE}{CRS_PUNCTUATION}]*)"
        );
        let space = format!("[{JAVA_WHITESPACE}]");
        Regex::new(&format!(
            r"^{space}*(?:\({space}*({token}){space}*(?:,{space}*({token}){space}*)?\))?(?:{space}+(NOT{space}+NULL))?{space}*\z"
        ))
        .unwrap()
    });
    let invalid = || Error::DataTypeInvalid {
        message: format!("Invalid {name} type: {input}"),
    };
    let suffix = input.strip_prefix(name).ok_or_else(invalid)?;
    let parts = PARAMS.captures(suffix).ok_or_else(invalid)?;
    let unquote = |s: &str| match s.as_bytes()[0] {
        b'\'' => s[1..s.len() - 1].replace("''", "'"),
        b'`' => s[1..s.len() - 1].replace("``", "`"),
        _ => s.to_owned(),
    };
    let crs = parts
        .get(1)
        .map_or_else(|| DEFAULT_CRS.into(), |m| unquote(m.as_str()));
    Ok((
        parts.get(3).is_none(),
        crs,
        parts.get(2).map(|m| unquote(m.as_str())),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{supports_cast, DataType};
    use std::collections::HashSet;

    #[test]
    fn geospatial_java_schema_round_trip() {
        for (input, canonical) in [
            ("GEOMETRY", "GEOMETRY(OGC:CRS84)"),
            (
                "GEOGRAPHY NOT NULL",
                "GEOGRAPHY(OGC:CRS84, spherical) NOT NULL",
            ),
            ("GEOGRAPHY(EPSG:4326)", "GEOGRAPHY(EPSG:4326, spherical)"),
            ("GEOMETRY(local\u{00a0}grid)", "GEOMETRY(local\u{00a0}grid)"),
            (
                "GEOMETRY('local\u{001c}grid')",
                "GEOMETRY('local\u{001c}grid')",
            ),
            ("GEOMETRY(²grid)", "GEOMETRY(²grid)"),
            (
                "GEOMETRY('custom, crs''s (definition)')",
                "GEOMETRY('custom, crs''s (definition)')",
            ),
            (
                "GEOGRAPHY('123', KARNEY) NOT NULL",
                "GEOGRAPHY('123', karney) NOT NULL",
            ),
        ] {
            let ty: DataType = serde_json::from_value(serde_json::json!(input)).unwrap();
            assert_eq!(ty.to_string(), canonical);
            assert_eq!(serde_json::to_value(&ty).unwrap(), canonical);
            assert_eq!(
                serde_json::from_value::<DataType>(serde_json::json!(canonical)).unwrap(),
                ty
            );
        }
        for name in ["SPHERICAL", "Vincenty", "THOMAS", "andoyer", "KARNEY"] {
            assert_eq!(
                name.parse::<EdgeAlgorithm>().unwrap().to_string(),
                name.to_ascii_lowercase()
            );
        }
        for invalid in [
            "GEOMETRY()",
            "GEOMETRY('')",
            "GEOMETRY(123)",
            "GEOMETRY(A, B)",
            "GEOGRAPHY(x,)",
            "GEOGRAPHY(x, rhumb)",
            "GEOGRAPHY('unclosed)",
            "GEOMETRY(x) trailing",
        ] {
            assert!(
                serde_json::from_value::<DataType>(serde_json::json!(invalid)).is_err(),
                "{invalid}"
            );
        }
    }

    #[test]
    fn geospatial_identity_and_casts_include_crs_and_algorithm() {
        let parse = |s: &str| serde_json::from_value::<DataType>(serde_json::json!(s)).unwrap();
        let geo = parse("GEOGRAPHY(EPSG:4326, karney)");
        let same = parse("GEOGRAPHY(epsg:4326, KARNEY)");
        assert!(HashSet::from([geo.clone()]).contains(&same));
        let required = same.copy_with_nullable(false).unwrap();
        assert!(supports_cast(&geo, &required, true));
        assert!(!supports_cast(&geo, &required, false));
        for other in [
            "GEOGRAPHY(EPSG:4326)",
            "GEOGRAPHY(EPSG:3857, karney)",
            "GEOMETRY(EPSG:4326)",
            "BYTES",
            "STRING",
        ] {
            assert!(!supports_cast(&geo, &parse(other), true));
            assert!(!supports_cast(&parse(other), &geo, true));
        }
    }
}
