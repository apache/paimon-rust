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
use paimon::spec::{DecimalType, TimestampType, VarBinaryType};

unsafe fn export(table: *const paimon_table, schema: &mut FFI_ArrowSchema) -> *mut paimon_error {
    paimon_table_export_arrow_schema(table, (schema as *mut FFI_ArrowSchema).cast())
}

unsafe fn invalid(error: *mut paimon_error) {
    assert!(!error.is_null());
    let code = (*error).code;
    paimon_error_free(error);
    assert_eq!(code, PaimonErrorCode::InvalidInput as i32);
}

#[test]
fn exported_table_arrow_schema_preserves_types_and_outlives_table() {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::with_nullable(false)))
        .column("name", DataType::VarChar(VarCharType::string_type()))
        .column(
            "amount",
            DataType::Decimal(DecimalType::new(12, 2).unwrap()),
        )
        .column("time", DataType::Timestamp(TimestampType::new(6).unwrap()))
        .column(
            "bytes",
            DataType::VarBinary(VarBinaryType::new(100).unwrap()),
        )
        .build()
        .unwrap();
    unsafe {
        let table = wrap_table(Table::new(
            memory_file_io(),
            Identifier::new("default", "pinned"),
            "memory:/no-catalog-or-schema-files".into(),
            TableSchema::new(17, &schema),
            None,
        ));
        let mut output = FFI_ArrowSchema::empty();
        assert!(export(table, &mut output).is_null());
        // Reusing live storage is rejected without replacing its contents.
        invalid(export(table, &mut output));
        unwrap_table(table);
        let actual = ArrowSchema::try_from(&output).unwrap();
        let expected = [
            ("id", ArrowDataType::Int32, false),
            ("name", ArrowDataType::Utf8, true),
            ("amount", ArrowDataType::Decimal128(12, 2), true),
            (
                "time",
                ArrowDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                true,
            ),
            ("bytes", ArrowDataType::Binary, true),
        ];
        assert_eq!(actual.fields().len(), expected.len());
        for (id, (field, (name, data_type, nullable))) in
            actual.fields().iter().zip(expected).enumerate()
        {
            assert_eq!(field.name(), name);
            assert_eq!(field.data_type(), &data_type);
            assert_eq!(field.is_nullable(), nullable);
            assert_eq!(
                field.metadata().get("PARQUET:field_id"),
                Some(&id.to_string())
            );
        }
        output.release.unwrap()(&mut output);
        assert!(output.release.is_none());
    }
}

#[test]
fn exported_table_arrow_schema_validates_pointers_and_allows_released_storage() {
    unsafe {
        let table = wrap_table(Table::new(
            memory_file_io(),
            Identifier::new("default", "schema"),
            "memory:/schema".into(),
            simple_table_schema(),
            None,
        ));
        let mut output = FFI_ArrowSchema::empty();
        invalid(export(ptr::null(), &mut output));
        assert!(output.release.is_none());
        invalid(paimon_table_export_arrow_schema(table, ptr::null_mut()));
        invalid(export(
            &paimon_table {
                inner: ptr::null_mut(),
            },
            &mut output,
        ));
        assert!(output.release.is_none());
        for _ in 0..2 {
            assert!(export(table, &mut output).is_null());
            assert_eq!(ArrowSchema::try_from(&output).unwrap().fields().len(), 2);
            output.release.unwrap()(&mut output);
            assert!(output.release.is_none());
        }
        unwrap_table(table);
    }
}
