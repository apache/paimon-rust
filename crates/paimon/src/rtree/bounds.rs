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

use super::invalid;
use std::io;

/// A finite, closed XY bounding box, or the canonical empty envelope.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BoundingBox {
    pub(crate) coordinates: [f64; 4],
}

impl BoundingBox {
    pub fn new(min_x: f64, min_y: f64, max_x: f64, max_y: f64) -> io::Result<Self> {
        if ![min_x, min_y, max_x, max_y].iter().all(|v| v.is_finite())
            || min_x > max_x
            || min_y > max_y
        {
            return Err(invalid("Invalid R-tree bounding box"));
        }
        Ok(Self {
            coordinates: [min_x, min_y, max_x, max_y],
        })
    }

    pub(crate) fn from_literals(values: &[crate::spec::Datum]) -> io::Result<Self> {
        use crate::spec::Datum::Double;
        match values {
            [Double(a), Double(b), Double(c), Double(d)] => Self::new(*a, *b, *c, *d),
            _ => Err(invalid("BBOX_INTERSECTS expects four double coordinates")),
        }
    }

    pub fn empty() -> Self {
        Self {
            coordinates: [
                f64::INFINITY,
                f64::INFINITY,
                f64::NEG_INFINITY,
                f64::NEG_INFINITY,
            ],
        }
    }

    pub fn min_x(&self) -> f64 {
        self.coordinates[0]
    }
    pub fn min_y(&self) -> f64 {
        self.coordinates[1]
    }
    pub fn max_x(&self) -> f64 {
        self.coordinates[2]
    }
    pub fn max_y(&self) -> f64 {
        self.coordinates[3]
    }
    pub fn is_empty(&self) -> bool {
        self.min_x() > self.max_x()
    }

    pub fn intersects(&self, other: &Self) -> bool {
        !self.is_empty()
            && !other.is_empty()
            && self.min_x() <= other.max_x()
            && self.max_x() >= other.min_x()
            && self.min_y() <= other.max_y()
            && self.max_y() >= other.min_y()
    }

    pub(crate) fn union(self, other: Self) -> Self {
        if self.is_empty() {
            return other;
        }
        if other.is_empty() {
            return self;
        }
        Self {
            coordinates: [
                std::cmp::min_by(self.min_x(), other.min_x(), f64::total_cmp),
                std::cmp::min_by(self.min_y(), other.min_y(), f64::total_cmp),
                std::cmp::max_by(self.max_x(), other.max_x(), f64::total_cmp),
                std::cmp::max_by(self.max_y(), other.max_y(), f64::total_cmp),
            ],
        }
    }

    /// Parse an envelope from 2D OGC WKB without constructing geometry objects.
    /// Empty points use two NaNs. EWKB, Z/M, malformed input, non-finite
    /// non-empty coordinates and nesting deeper than 64 are rejected.
    pub fn from_wkb(bytes: &[u8]) -> io::Result<Self> {
        let mut parser = Wkb {
            bytes,
            offset: 0,
            bounds: Self::empty(),
        };
        parser.geometry(0, None)?;
        if parser.offset != bytes.len() {
            return Err(invalid("Trailing bytes in WKB geometry"));
        }
        Ok(parser.bounds)
    }
}

struct Wkb<'a> {
    bytes: &'a [u8],
    offset: usize,
    bounds: BoundingBox,
}

impl Wkb<'_> {
    fn take<const N: usize>(&mut self) -> io::Result<[u8; N]> {
        let end = self
            .offset
            .checked_add(N)
            .ok_or_else(|| invalid("Invalid WKB length"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| invalid("Truncated WKB"))?;
        self.offset = end;
        Ok(value.try_into().expect("fixed-size checked slice"))
    }

    fn integer(&mut self, little: bool) -> io::Result<u32> {
        let value = self.take()?;
        Ok(if little {
            u32::from_le_bytes(value)
        } else {
            u32::from_be_bytes(value)
        })
    }

    fn double(&mut self, little: bool) -> io::Result<f64> {
        let value = self.take()?;
        Ok(if little {
            f64::from_le_bytes(value)
        } else {
            f64::from_be_bytes(value)
        })
    }

    fn count(&mut self, little: bool, minimum_bytes: usize) -> io::Result<usize> {
        let count = self.integer(little)? as usize;
        if count > (self.bytes.len() - self.offset) / minimum_bytes {
            return Err(invalid("Invalid WKB element count"));
        }
        Ok(count)
    }

    fn point(&mut self, little: bool, empty_allowed: bool) -> io::Result<()> {
        let x = self.double(little)?;
        let y = self.double(little)?;
        if empty_allowed && x.is_nan() && y.is_nan() {
            return Ok(());
        }
        if !x.is_finite() || !y.is_finite() {
            return Err(invalid("Non-finite WKB coordinate"));
        }
        self.bounds = self.bounds.union(BoundingBox {
            coordinates: [x, y, x, y],
        });
        Ok(())
    }

    fn points(&mut self, little: bool) -> io::Result<()> {
        let count = self.count(little, 16)?;
        for _ in 0..count {
            self.point(little, false)?;
        }
        Ok(())
    }

    fn geometry(&mut self, depth: usize, expected: Option<u32>) -> io::Result<()> {
        if depth > 64 {
            return Err(invalid("Excessive WKB nesting"));
        }
        let order = self.take::<1>()?[0];
        if order > 1 {
            return Err(invalid("Invalid WKB byte order"));
        }
        let little = order == 1;
        let kind = self.integer(little)?;
        if !(1..=7).contains(&kind) || expected.is_some_and(|value| value != kind) {
            return Err(invalid("Unsupported or invalid WKB geometry type"));
        }
        match kind {
            1 => self.point(little, true)?,
            2 => self.points(little)?,
            3 => {
                let rings = self.count(little, 4)?;
                for _ in 0..rings {
                    self.points(little)?;
                }
            }
            4..=7 => {
                let count = self.count(little, 5)?;
                let child_type = (kind < 7).then_some(kind - 3);
                for _ in 0..count {
                    self.geometry(depth + 1, child_type)?;
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}
