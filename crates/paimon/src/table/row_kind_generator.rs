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

use arrow_array::{LargeStringArray, RecordBatch, StringArray};

use crate::spec::{RowKind, TableSchema};

pub struct RowKindGenerator {
    field_name: String,
}

impl RowKindGenerator {
    pub fn create(schema: &TableSchema, field_name: &str) -> crate::Result<Self> {
        schema
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
        Ok(Self {
            field_name: field_name.to_owned(),
        })
    }

    pub fn generate_batch(&self, batch: &RecordBatch) -> crate::Result<Vec<RowKind>> {
        // with_write_type may reorder the physical columns. Resolve once for
        // the batch, then parse values without a per-row field-name search.
        let col =
            batch
                .column_by_name(&self.field_name)
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: format!(
                        "Cannot find rowkind field '{}' in write input",
                        self.field_name
                    ),
                    source: None,
                })?;
        let parse = |value: Option<&str>| {
            value
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: "Row kind cannot be null.".into(),
                    source: None,
                })
                .and_then(RowKind::from_short_string)
        };
        if let Some(strings) = col.as_any().downcast_ref::<StringArray>() {
            strings.iter().map(parse).collect()
        } else if let Some(strings) = col.as_any().downcast_ref::<LargeStringArray>() {
            strings.iter().map(parse).collect()
        } else {
            Err(crate::Error::DataInvalid {
                message: "rowkind.field column must be Utf8/String".into(),
                source: None,
            })
        }
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
        assert_eq!(gen.generate_batch(&batch).unwrap(), vec![RowKind::Delete]);
    }
}
