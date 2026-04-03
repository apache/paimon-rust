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

//! Schema evolution utilities for mapping between table schema and data file schema.
//!
//! Reference: [org.apache.paimon.schema.SchemaEvolutionUtil](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/schema/SchemaEvolutionUtil.java)

use crate::spec::DataField;
use std::collections::HashMap;

/// Sentinel value indicating a field does not exist in the data schema.
pub const NULL_FIELD_INDEX: i32 = -1;

/// Create index mapping from table fields to underlying data fields using field IDs.
///
/// For example, the table and data fields are as follows:
/// - table fields: `1->c, 6->b, 3->a`
/// - data fields: `1->a, 3->c`
///
/// We get the index mapping `[0, -1, 1]`, where:
/// - `0` is the index of table field `1->c` in data fields
/// - `-1` means field `6->b` does not exist in data fields
/// - `1` is the index of table field `3->a` in data fields
///
/// Returns `None` if the mapping is identity (no evolution needed).
///
/// Reference: [SchemaEvolutionUtil.createIndexMapping](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/schema/SchemaEvolutionUtil.java)
pub fn create_index_mapping(
    table_fields: &[DataField],
    data_fields: &[DataField],
) -> Option<Vec<i32>> {
    let mut field_id_to_index: HashMap<i32, i32> = HashMap::with_capacity(data_fields.len());
    for (i, field) in data_fields.iter().enumerate() {
        field_id_to_index.insert(field.id(), i as i32);
    }

    let mut index_mapping = Vec::with_capacity(table_fields.len());
    for field in table_fields {
        let data_index = field_id_to_index
            .get(&field.id())
            .copied()
            .unwrap_or(NULL_FIELD_INDEX);
        index_mapping.push(data_index);
    }

    // Check if mapping is identity (no evolution needed).
    let is_identity = index_mapping.len() == data_fields.len()
        && index_mapping
            .iter()
            .enumerate()
            .all(|(i, &idx)| idx == i as i32);

    if is_identity {
        None
    } else {
        Some(index_mapping)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{DataType, IntType, VarCharType};

    fn field(id: i32, name: &str) -> DataField {
        DataField::new(id, name.to_string(), DataType::Int(IntType::new()))
    }

    #[test]
    fn test_identity_mapping() {
        let table_fields = vec![field(0, "a"), field(1, "b"), field(2, "c")];
        let data_fields = vec![field(0, "a"), field(1, "b"), field(2, "c")];
        assert_eq!(create_index_mapping(&table_fields, &data_fields), None);
    }

    #[test]
    fn test_added_column() {
        // Table has 3 fields, data file only has the first 2
        let table_fields = vec![field(0, "a"), field(1, "b"), field(2, "c")];
        let data_fields = vec![field(0, "a"), field(1, "b")];
        assert_eq!(
            create_index_mapping(&table_fields, &data_fields),
            Some(vec![0, 1, -1])
        );
    }

    #[test]
    fn test_reordered_fields() {
        let table_fields = vec![field(1, "c"), field(6, "b"), field(3, "a")];
        let data_fields = vec![field(1, "a"), field(3, "c")];
        assert_eq!(
            create_index_mapping(&table_fields, &data_fields),
            Some(vec![0, -1, 1])
        );
    }

    #[test]
    fn test_renamed_column() {
        // Field ID stays the same even if name changed
        let table_fields = vec![field(0, "id"), field(1, "new_name")];
        let data_fields = vec![field(0, "id"), field(1, "old_name")];
        // Identity mapping since field IDs match positionally
        assert_eq!(create_index_mapping(&table_fields, &data_fields), None);
    }

    #[test]
    fn test_empty_data_fields() {
        let table_fields = vec![field(0, "a"), field(1, "b")];
        let data_fields: Vec<DataField> = vec![];
        assert_eq!(
            create_index_mapping(&table_fields, &data_fields),
            Some(vec![-1, -1])
        );
    }

    #[test]
    fn test_type_promotion_same_mapping() {
        // Type promotion doesn't affect index mapping — only field IDs matter
        let table_fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "name".to_string(),
                DataType::VarChar(VarCharType::string_type()),
            ),
        ];
        let data_fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "name".to_string(),
                DataType::VarChar(VarCharType::string_type()),
            ),
        ];
        assert_eq!(create_index_mapping(&table_fields, &data_fields), None);
    }
}
