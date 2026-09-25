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

//! Portable roaring32 union, matching Java `FieldRoaringBitmap32Agg`.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BinaryArray};
use roaring::RoaringBitmap;

use super::{unsupported_type_error, FieldAggregator};
use crate::spec::DataType;
use crate::Error;

#[derive(Debug)]
pub(crate) struct Roaring32Agg {
    field_name: String,
    value: Option<Vec<u8>>,
}

impl Roaring32Agg {
    pub(crate) fn new(field_name: &str, data_type: &DataType) -> crate::Result<Self> {
        if !matches!(data_type, DataType::VarBinary(_)) {
            return Err(unsupported_type_error("rbm32", field_name, data_type));
        }
        Ok(Self {
            field_name: field_name.to_string(),
            value: None,
        })
    }
}

impl FieldAggregator for Roaring32Agg {
    fn name(&self) -> &'static str {
        "rbm32"
    }

    fn reset(&mut self) {
        self.value = None;
    }

    fn agg(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        if array.is_null(row_idx) {
            return Ok(());
        }
        let bytes = array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| Error::DataInvalid {
                message: format!(
                    "rbm32 column '{}' requires Arrow Binary, got {:?}",
                    self.field_name,
                    array.data_type()
                ),
                source: None,
            })?
            .value(row_idx);
        let Some(accumulator) = self.value.as_ref() else {
            // Java returns the first non-null value unchanged.
            self.value = Some(bytes.to_vec());
            return Ok(());
        };
        let mut current = RoaringBitmap::deserialize_from(accumulator.as_slice()).map_err(|e| {
            Error::DataInvalid {
                message: format!("Invalid rbm32 accumulator for '{}': {e}", self.field_name),
                source: Some(Box::new(e)),
            }
        })?;
        let incoming = RoaringBitmap::deserialize_from(bytes).map_err(|e| Error::DataInvalid {
            message: format!("Invalid rbm32 input for '{}': {e}", self.field_name),
            source: Some(Box::new(e)),
        })?;
        current |= incoming;
        let mut result = Vec::new();
        current
            .serialize_into(&mut result)
            .map_err(|e| Error::UnexpectedError {
                message: format!("Failed to serialize rbm32 for '{}': {e}", self.field_name),
                source: Some(Box::new(e)),
            })?;
        self.value = Some(result);
        Ok(())
    }

    fn agg_reversed(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        self.agg(array, row_idx)
    }

    fn result(&self) -> crate::Result<ArrayRef> {
        Ok(Arc::new(BinaryArray::from(vec![self.value.as_deref()])))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::VarBinaryType;

    // Serialized by Java RoaringBitmap 1.2.1 after runOptimize(), with
    // values {1, 3, 65537}. The input must be readable before any Rust union.
    const JAVA_BITMAP: &str = "OjAAAAIAAAAAAAEAAQAAABgAAAAcAAAAAQADAAEA";

    #[test]
    fn unions_java_roaring32_bytes() {
        let java = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, JAVA_BITMAP)
            .unwrap();
        let mut extra = RoaringBitmap::new();
        extra.insert(3);
        extra.insert(4);
        let mut extra_bytes = Vec::new();
        extra.serialize_into(&mut extra_bytes).unwrap();
        let input = BinaryArray::from(vec![Some(java.as_slice()), Some(extra_bytes.as_slice())]);
        let mut agg = Roaring32Agg::new(
            "bitmap",
            &DataType::VarBinary(VarBinaryType::new(32).unwrap()),
        )
        .unwrap();
        agg.agg(&input, 0).unwrap();
        agg.agg(&input, 1).unwrap();
        let result = agg.result().unwrap();
        let result = result.as_any().downcast_ref::<BinaryArray>().unwrap();
        let bitmap = RoaringBitmap::deserialize_from(result.value(0)).unwrap();
        assert_eq!(bitmap.iter().collect::<Vec<_>>(), vec![1, 3, 4, 65537]);
    }
}
