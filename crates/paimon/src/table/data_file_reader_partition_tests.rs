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

use super::*;
use crate::spec::{BigIntType, BinaryRowBuilder, IntType};
use arrow_array::Int32Array;

#[test]
fn absent_partition_columns_follow_field_identity_and_reconcile_types() {
    let source = DataField::new(7, "old_pt".into(), DataType::Int(IntType::new()));
    let target = DataField::new(7, "new_pt".into(), DataType::BigInt(BigIntType::new()));
    let mut partition = BinaryRowBuilder::new(1);
    partition.write_int(0, 42);
    let partition = partition.build();
    let column = missing_file_column(
        &target,
        &arrow_schema::DataType::Int64,
        &partition,
        &[source],
        2,
    )
    .unwrap();
    assert_eq!(
        column.as_any().downcast_ref::<Int64Array>().unwrap(),
        &Int64Array::from(vec![42, 42])
    );
    // A newly added field with the old name has a different identity.
    let added = DataField::new(8, "old_pt".into(), DataType::Int(IntType::new()));
    assert_eq!(
        missing_file_column(
            &added,
            &arrow_schema::DataType::Int32,
            &partition,
            &[target],
            2,
        )
        .unwrap()
        .null_count(),
        2
    );
}

#[test]
fn absent_null_partition_columns_remain_null() {
    let field = DataField::new(7, "pt".into(), DataType::Int(IntType::new()));
    let mut partition = BinaryRowBuilder::new(1);
    partition.set_null_at(0);
    assert_eq!(
        missing_file_column(
            &field,
            &arrow_schema::DataType::Int32,
            &partition.build(),
            std::slice::from_ref(&field),
            3,
        )
        .unwrap()
        .null_count(),
        3
    );
}

#[test]
fn present_physical_partition_nulls_are_not_replaced_by_split_values() {
    let field = DataField::new(7, "pt".into(), DataType::Int(IntType::new()));
    let schema = crate::arrow::build_read_arrow_schema(std::slice::from_ref(&field)).unwrap();
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![None]))]).unwrap();
    let mut partition = BinaryRowBuilder::new(1);
    partition.write_int(0, 42);
    let result = project_file_batch(
        &batch,
        &schema,
        std::slice::from_ref(&field),
        std::slice::from_ref(&field),
        None,
        None,
        (&partition.build(), std::slice::from_ref(&field)),
    )
    .unwrap();
    assert_eq!(result.column(0).null_count(), 1);
}
