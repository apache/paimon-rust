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

//! Arrow assignment semantics shared by native update callers.

use arrow_array::{ArrayRef, RecordBatch, UInt32Array};
use arrow_schema::{DataType, SchemaRef};

use super::update_input::{cast_update_value, CastMode};
use arrow_select::take::take;
use std::sync::Arc;

type AssignmentFunction = dyn Fn(&[RecordBatch]) -> crate::Result<Vec<ArrayRef>> + Send + Sync;
type ScalarFunction = dyn Fn() -> crate::Result<ArrayRef> + Send + Sync;

/// A literal, an Arrow array, or a function evaluated per matched file group.
#[derive(Clone)]
pub enum UpdateAssignment {
    Scalar(ArrayRef),
    Array(Vec<ArrayRef>),
    /// Receives the projected matched rows of one logical file group.
    Function(Arc<AssignmentFunction>),
    /// Convert a foreign-language scalar only when matching rows exist.
    DeferredScalar(Arc<ScalarFunction>),
}

fn invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

struct Values {
    scalar: bool,
    chunks: Vec<ArrayRef>,
    chunk: usize,
    offset: usize,
}

impl Values {
    fn remaining(&self) -> usize {
        if self.scalar {
            usize::MAX
        } else {
            self.chunks[self.chunk].len() - self.offset
        }
    }

    fn take(&mut self, count: usize) -> crate::Result<ArrayRef> {
        if self.scalar {
            return take(
                self.chunks[0].as_ref(),
                &UInt32Array::from(vec![0; count]),
                None,
            )
            .map_err(|error| invalid(error.to_string()));
        }
        let array = self.chunks[self.chunk].slice(self.offset, count);
        self.offset += count;
        if self.offset == self.chunks[self.chunk].len() {
            self.chunk += 1;
            self.offset = 0;
        }
        Ok(array)
    }
}

pub(super) fn assigned_batches(
    matched: &[RecordBatch],
    assignments: Vec<(String, UpdateAssignment)>,
    schema: SchemaRef,
) -> crate::Result<Vec<RecordBatch>> {
    let row_count: usize = matched.iter().map(RecordBatch::num_rows).sum();
    if row_count == 0 {
        return Ok(Vec::new());
    }
    let mut names = std::collections::HashSet::new();
    let mut fields = vec![Arc::new(arrow_schema::Field::new(
        "_ROW_ID",
        DataType::Int64,
        false,
    ))];
    let mut values = Vec::new();
    for (name, assignment) in assignments {
        if name == "_ROW_ID" || !names.insert(name.clone()) {
            return Err(invalid(format!(
                "Invalid or duplicate assignment column {name}"
            )));
        }
        let field = schema
            .field_with_name(&name)
            .map_err(|error| invalid(error.to_string()))?;
        let (scalar, chunks) = match assignment {
            UpdateAssignment::Scalar(value) => (true, vec![value]),
            UpdateAssignment::Array(chunks) => (false, chunks),
            UpdateAssignment::Function(function) => (false, function(matched)?),
            UpdateAssignment::DeferredScalar(function) => (true, vec![function()?]),
        };
        let length: usize = chunks.iter().map(|array| array.len()).sum();
        if length != if scalar { 1 } else { row_count } {
            return Err(invalid(format!(
                "Assignment array length must match matched row count: {length} != {row_count}"
            )));
        }
        let chunks = chunks
            .iter()
            .filter(|chunk| !chunk.is_empty())
            .map(|chunk| cast_update_value(chunk, field.data_type(), CastMode::Assignment))
            .collect::<crate::Result<Vec<_>>>()?;
        values.push(Values {
            scalar,
            chunks,
            chunk: 0,
            offset: 0,
        });
        fields.push(Arc::new(field.clone()));
    }
    if values.is_empty() {
        return Err(invalid("assignments must not be empty"));
    }
    let output_schema = Arc::new(arrow_schema::Schema::new(fields));
    let mut output = Vec::new();
    for batch in matched {
        let row_ids = batch
            .column_by_name("_ROW_ID")
            .ok_or_else(|| invalid("Input data must contain _ROW_ID column"))?;
        let mut offset = 0;
        while offset < batch.num_rows() {
            let count = values
                .iter()
                .map(Values::remaining)
                .fold(batch.num_rows() - offset, usize::min);
            let mut arrays = vec![row_ids.slice(offset, count)];
            for value in &mut values {
                arrays.push(value.take(count)?);
            }
            output.push(
                RecordBatch::try_new(output_schema.clone(), arrays)
                    .map_err(|error| invalid(error.to_string()))?,
            );
            offset += count;
        }
    }
    Ok(output)
}
