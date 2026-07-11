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

//! Parse row kind from a user STRING column (`rowkind.field` table option).
//!
//! Reference: Java `org.apache.paimon.table.sink.RowKindGenerator`.

use arrow_array::{Array, RecordBatch, StringArray};

use crate::spec::{RowKind, TableSchema};

pub struct RowKindGenerator {
    index: usize,
}

impl RowKindGenerator {
    pub fn create(schema: &TableSchema, field_name: &str) -> crate::Result<Self> {
        let index = schema
            .fields()
            .iter()
            .position(|f| f.name() == field_name)
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!(
                    "Can not find rowkind {field_name} in table schema: {:?}",
                    schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
                ),
                source: None,
            })?;
        Ok(Self { index })
    }

    pub fn generate(&self, batch: &RecordBatch, row: usize) -> crate::Result<RowKind> {
        let col = batch.column(self.index);
        if col.is_null(row) {
            return Err(crate::Error::DataInvalid {
                message: "Row kind cannot be null.".to_string(),
                source: None,
            });
        }
        let strings = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            crate::Error::DataInvalid {
                message: "rowkind.field column must be Utf8/String".to_string(),
                source: None,
            }
        })?;
        RowKind::from_short_string(strings.value(row))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};

    use crate::spec::{DataType, IntType, RowKind, Schema, TableSchema, VarCharType};

    use super::RowKindGenerator;

    fn test_schema(op_col: &str) -> TableSchema {
        TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column(op_col, DataType::VarChar(VarCharType::string_type()))
                .primary_key(["id"])
                .option("rowkind.field", op_col)
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn generate_parses_short_string_column() {
        let schema = test_schema("op");
        let gen = RowKindGenerator::create(&schema, "op").unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Arc::new(ArrowField::new("id", ArrowDataType::Int32, false)),
                Arc::new(ArrowField::new("op", ArrowDataType::Utf8, false)),
            ])),
            vec![
                Arc::new(arrow_array::Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["-D"])),
            ],
        )
        .unwrap();
        assert_eq!(gen.generate(&batch, 0).unwrap(), RowKind::Delete);
    }
}
