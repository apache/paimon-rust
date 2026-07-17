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

//! Shredding plans that convert between logical table fields and the physical
//! on-file layout, mirroring Java's `org.apache.paimon.data.shredding`
//! (`ShreddingWritePlan` / `ShreddingReadPlan`).
//!
//! Two layouts exist today:
//! - [`variant`]: Variant shredding (schema-driven or inferred).
//! - [`map`]: MAP shared-shredding (PIP-43), Java-metadata compatible.

pub(crate) mod map;
pub(crate) mod variant;

use crate::spec::DataField;
use crate::{Error, Result};
use arrow_array::RecordBatch;
use std::collections::HashMap;

/// Per-field metadata committed into the file footer at close time:
/// top-level field name -> (metadata key -> value).
pub(crate) type FieldMetadata = HashMap<String, HashMap<String, String>>;

/// A physical write plan for one file, mirroring Java's `ShreddingWritePlan`.
///
/// The plan owns all file-local shredding state (e.g. the MAP field dictionary
/// and column allocator), so [`Self::to_physical_batch`] takes `&mut self` and
/// must be called for every written batch in order.
pub(crate) trait ShreddingWritePlan: Send {
    /// Logical (table) fields before shredding.
    #[allow(dead_code)] // Part of the mirrored Java API surface.
    fn logical_fields(&self) -> &[DataField];

    /// Physical fields written to the file.
    fn physical_fields(&self) -> &[DataField];

    /// Convert one logical batch into the physical layout.
    fn to_physical_batch(&mut self, batch: &RecordBatch) -> Result<RecordBatch>;

    /// Per-field metadata to commit into the file footer at close time.
    ///
    /// `compression` is the file compression codec (`none`/`lz4`/`zstd`), used
    /// for the MAP field dictionary. The default returns an empty map, matching
    /// Java's `ShreddingWritePlan.fieldMetadata`.
    fn field_metadata(&self, _compression: Option<&str>) -> Result<FieldMetadata> {
        Ok(HashMap::new())
    }
}

/// A physical read plan for one file, mirroring Java's `ShreddingReadPlan`.
pub(crate) trait ShreddingReadPlan: Send + Sync {
    /// Logical (table) fields after assembly.
    #[allow(dead_code)] // Part of the mirrored Java API surface.
    fn logical_fields(&self) -> &[DataField];

    /// Physical fields decoded from the file.
    #[allow(dead_code)] // Part of the mirrored Java API surface.
    fn physical_fields(&self) -> &[DataField];

    /// Whether the physical layout equals the logical one (no assembly needed).
    #[allow(dead_code)] // Part of the mirrored Java API surface.
    fn is_identity(&self) -> bool {
        self.logical_fields() == self.physical_fields()
    }

    /// Rebuild logical columns from a decoded physical batch.
    fn assemble_batch(&self, batch: &RecordBatch) -> Result<RecordBatch>;
}

pub(crate) fn option_bool(
    options: &HashMap<String, String>,
    keys: &[&str],
    default_value: bool,
) -> Result<bool> {
    let Some(value) = keys.iter().find_map(|key| options.get(*key)) else {
        return Ok(default_value);
    };
    value.parse::<bool>().map_err(|e| Error::DataInvalid {
        message: format!("Invalid boolean option value '{value}'"),
        source: Some(Box::new(e)),
    })
}

pub(crate) fn option_usize(
    options: &HashMap<String, String>,
    key: &str,
    default_value: usize,
) -> Result<usize> {
    let Some(value) = options.get(key) else {
        return Ok(default_value);
    };
    value.parse::<usize>().map_err(|e| Error::DataInvalid {
        message: format!("Invalid integer option {key}={value}"),
        source: Some(Box::new(e)),
    })
}

pub(crate) fn option_f64(
    options: &HashMap<String, String>,
    key: &str,
    default_value: f64,
) -> Result<f64> {
    let Some(value) = options.get(key) else {
        return Ok(default_value);
    };
    value.parse::<f64>().map_err(|e| Error::DataInvalid {
        message: format!("Invalid double option {key}={value}"),
        source: Some(Box::new(e)),
    })
}
