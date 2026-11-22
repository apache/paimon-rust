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

//! Shared schema, Arrow and cosine semantics for embedding-list columns.

use super::pkvector::metric::cosine_norm;
use crate::spec::DataType;
use crate::{Error, Result};
use arrow_array::{Array, FixedSizeListArray, Float32Array, ListArray};

/// An ARRAY of variable or fixed-width FLOAT embeddings.
pub(crate) fn embedding_type(data_type: &DataType) -> Option<&DataType> {
    let DataType::Array(array) = data_type else {
        return None;
    };
    let inner = array.element_type();
    let is_embedding = match inner {
        DataType::Vector(vector) => matches!(vector.element_type(), DataType::Float(_)),
        DataType::Array(array) => matches!(array.element_type(), DataType::Float(_)),
        _ => false,
    };
    is_embedding.then_some(inner)
}

pub(crate) fn validate_queries(queries: &[&[f32]], width: Option<usize>) -> Result<()> {
    let dimension = queries.first().map_or(0, |query| query.len());
    if dimension == 0
        || width.is_some_and(|width| width != dimension)
        || queries.iter().any(|vector| {
            vector.len() != dimension || vector.iter().any(|coordinate| !coordinate.is_finite())
        })
    {
        return Err(invalid(
            "MultiVector queries require at least one vector with a consistent, positive dimension and finite coordinates",
        ));
    }
    Ok(())
}

pub(crate) fn normalize_into(vector: &[f32], output: &mut Vec<f32>) {
    let norm = cosine_norm(vector);
    let scale = if norm == 0.0 { 1.0 } else { 1.0 / norm };
    output.extend(
        vector
            .iter()
            .map(|value| (f64::from(*value) * scale) as f32),
    );
}

enum Embeddings<'a> {
    Array(&'a ListArray),
    Vector(&'a FixedSizeListArray),
}

pub(crate) struct MultiVectorArray<'a> {
    rows: &'a ListArray,
    embeddings: Embeddings<'a>,
    values: &'a Float32Array,
}

impl<'a> MultiVectorArray<'a> {
    pub(crate) fn new(column: &'a dyn Array) -> Result<Self> {
        let rows = column
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| invalid("MultiVector columns require an Arrow ListArray"))?;
        let inner = rows.values().as_any();
        let (embeddings, coordinates) = if let Some(list) = inner.downcast_ref::<ListArray>() {
            (Embeddings::Array(list), list.values())
        } else if let Some(list) = inner.downcast_ref::<FixedSizeListArray>() {
            (Embeddings::Vector(list), list.values())
        } else {
            return Err(invalid(
                "MultiVector embeddings must be Arrow Lists or FixedSizeLists",
            ));
        };
        let values = coordinates
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("MultiVector coordinates must be Float32"))?;
        Ok(Self {
            rows,
            embeddings,
            values,
        })
    }

    /// Null lists and null embeddings represent no vectors. Only validate
    /// coordinates belonging to this visible row, including on sliced batches.
    pub(crate) fn row(
        &self,
        row: usize,
        dimension: usize,
    ) -> impl Iterator<Item = Result<&'a [f32]>> + '_ {
        let offsets = self.rows.value_offsets();
        let start = offsets[row] as usize;
        let end = if self.rows.is_null(row) {
            start
        } else {
            offsets[row + 1] as usize
        };
        (start..end).filter_map(move |index| {
            let (start, end) = match self.embeddings {
                Embeddings::Array(list) => {
                    if list.is_null(index) {
                        return None;
                    }
                    let offsets = list.value_offsets();
                    (offsets[index] as usize, offsets[index + 1] as usize)
                }
                Embeddings::Vector(list) => {
                    if list.is_null(index) {
                        return None;
                    }
                    let start = list.value_offset(index) as usize;
                    (start, start + list.value_length() as usize)
                }
            };
            if end - start != dimension {
                return Some(Err(invalid(format!(
                    "MultiVector dimension mismatch: expected {dimension}, got {}",
                    end - start
                ))));
            }
            let values = &self.values.values()[start..end];
            if (self.values.null_count() != 0 && (start..end).any(|i| self.values.is_null(i)))
                || values.iter().any(|coordinate| !coordinate.is_finite())
            {
                return Some(Err(invalid(
                    "MultiVector coordinates must be non-null and finite",
                )));
            }
            Some(Ok(values))
        })
    }
}

pub(crate) fn invalid(message: impl Into<String>) -> Error {
    Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}
