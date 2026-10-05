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
pub(crate) trait ShreddingWritePlan: Send + std::any::Any {
    /// Logical (table) fields before shredding.
    fn logical_fields(&self) -> &[DataField];

    /// Physical fields written to the file.
    fn physical_fields(&self) -> &[DataField];

    /// Convert one logical batch into the physical layout.
    fn to_physical_batch(&mut self, batch: &RecordBatch) -> Result<RecordBatch>;

    /// Widths reported only after a file closes successfully.
    fn file_max_row_widths(&self) -> HashMap<String, usize> {
        HashMap::new()
    }

    /// Per-field metadata to commit into the file footer at close time.
    ///
    /// `compression` is the file compression codec (`none`/`lz4`/`zstd`), used
    /// for the MAP field dictionary. The default returns an empty map, matching
    /// Java's `ShreddingWritePlan.fieldMetadata`.
    fn field_metadata(&self, _compression: Option<&str>) -> Result<FieldMetadata> {
        Ok(HashMap::new())
    }
}

/// Creates file-local plans and owns rolling-writer-scoped shredding state,
/// mirroring Java's `ShreddingWritePlanFactory`.
pub(crate) trait ShreddingWritePlanFactory: Send + Sync {
    fn should_create_write_plan(&self) -> bool;

    /// Some(count) defers physical writer creation until inference has input.
    fn infer_buffer_row_count(&self) -> Option<usize> {
        None
    }

    fn create_write_plan(&self, batches: &[RecordBatch]) -> Result<Box<dyn ShreddingWritePlan>>;

    fn validate_compression(&self, _compression: &str) -> Result<()> {
        Ok(())
    }

    /// Called only after the underlying file closes successfully.
    fn on_file_completed(&self, _plan: &dyn ShreddingWritePlan) -> Result<()> {
        Ok(())
    }

    /// Whether the next plan depends on the previous file's close callback.
    fn needs_completed_file_stats(&self) -> bool {
        false
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
    // Match Java's `OptionsUtils.convertToBoolean`: accept "true"/"false"
    // case-insensitively and reject anything else. Rust's `str::parse::<bool>`
    // only accepts exact lowercase "true"/"false", so a table property written
    // by a Java engine such as `variant.inferShreddingSchema=TRUE` would
    // otherwise fail to parse.
    if value.eq_ignore_ascii_case("true") {
        Ok(true)
    } else if value.eq_ignore_ascii_case("false") {
        Ok(false)
    } else {
        Err(Error::DataInvalid {
            message: format!(
                "Invalid boolean option value '{value}', expected true or false (case insensitive)"
            ),
            source: None,
        })
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_option_bool_accepts_case_insensitive_true_false() {
        for v in ["true", "TRUE", "True", "tRuE"] {
            let opts = HashMap::from([("k".to_string(), v.to_string())]);
            assert!(
                option_bool(&opts, &["k"], false).unwrap(),
                "value {v} should parse as true"
            );
        }
        for v in ["false", "FALSE", "False"] {
            let opts = HashMap::from([("k".to_string(), v.to_string())]);
            assert!(
                !option_bool(&opts, &["k"], true).unwrap(),
                "value {v} should parse as false"
            );
        }
    }

    #[test]
    fn test_option_bool_default_when_absent() {
        let opts: HashMap<String, String> = HashMap::new();
        assert!(option_bool(&opts, &["k"], true).unwrap());
        assert!(!option_bool(&opts, &["k"], false).unwrap());
    }

    #[test]
    fn test_option_bool_rejects_non_boolean() {
        for v in ["yes", "1", "on", ""] {
            let opts = HashMap::from([("k".to_string(), v.to_string())]);
            assert!(
                option_bool(&opts, &["k"], false).is_err(),
                "value {v:?} should be rejected"
            );
        }
    }
}
