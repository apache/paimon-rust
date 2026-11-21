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

//! Java-compatible packed R-tree for planar GEOMETRY WKB.
//!
//! Table builds use `Table::new_global_index_build_builder` with index type `rtree`.
//! Use `PredicateBuilder::bbox_intersects` for inclusive XY envelope intersection.

mod bounds;
mod format;
mod options;
mod reader;
pub(crate) use format::Metadata;
pub(crate) use options::RTreeReadContext;
mod writer;

pub use bounds::BoundingBox;
pub use format::RTreeFileMeta;
pub use reader::{RTreeFileSource, RTreeIndexReader, RTreePageCache};
pub use writer::{RTreeIndexWriter, RTreeOutput};

use std::io;

/// Build bounds matching the Java implementation.
#[derive(Clone, Debug)]
pub struct RTreeIndexOptions {
    pub page_size: usize,
    pub partition_rows: usize,
}

impl Default for RTreeIndexOptions {
    fn default() -> Self {
        Self {
            page_size: 256,
            partition_rows: 65_536,
        }
    }
}

impl RTreeIndexOptions {
    pub(crate) fn from_options(
        options: &std::collections::HashMap<String, String>,
    ) -> crate::Result<Self> {
        let defaults = Self::default();
        let parse = |key: &str, default| -> crate::Result<usize> {
            options.get(key).map_or(Ok(default), |value| {
                value.parse().map_err(|_| crate::Error::ConfigInvalid {
                    message: format!("Invalid {key}: {value}"),
                })
            })
        };
        let result = Self {
            page_size: parse("rtree-index.page-size", defaults.page_size)?,
            partition_rows: parse("rtree-index.partition-row-count", defaults.partition_rows)?,
        };
        result.validate().map_err(|e| crate::Error::ConfigInvalid {
            message: e.to_string(),
        })?;
        Ok(result)
    }

    fn validate(&self) -> io::Result<()> {
        if !(2..=4096).contains(&self.page_size) || !(1..=1_048_576).contains(&self.partition_rows)
        {
            return Err(invalid(
                "R-tree page size or partition row count is out of range",
            ));
        }
        Ok(())
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests;
