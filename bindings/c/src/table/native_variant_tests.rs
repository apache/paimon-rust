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
use arrow::buffer::OffsetBuffer;
use arrow_array::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{make_array, Int32Array};
use arrow_schema::Field as ArrowField;
use paimon::spec::{ArrayType, IntType, MapType, RowType, VarBinaryType, VariantType};
use paimon::variant::GenericVariant;
use std::collections::HashMap;

fn variant_type() -> DataType {
    DataType::Variant(VariantType::new())
}

fn input_array(
    values: Vec<Option<&[u8]>>,
    metadata: Vec<Option<&[u8]>>,
    nulls: Option<NullBuffer>,
) -> ArrayRef {
    Arc::new(StructArray::new(
        vec![
            Arc::new(ArrowField::new("value", ArrowDataType::Binary, true)),
            Arc::new(ArrowField::new("metadata", ArrowDataType::Binary, true)),
        ]
        .into(),
        vec![
            Arc::new(BinaryArray::from(values)),
            Arc::new(BinaryArray::from(metadata)),
        ],
        nulls,
    ))
}

fn assert_payload(array: &ArrayRef, row: usize, expected: &GenericVariant) {
    assert_eq!(array.data_type(), &paimon::arrow::variant_arrow_type());
    let input = array.as_any().downcast_ref::<StructArray>().unwrap();
    assert!(input.is_valid(row));
    let value = input
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    let metadata = input
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(value.value(row), expected.value());
    assert_eq!(metadata.value(row), expected.metadata());
}

#[test]
fn native_variant_preserves_payload_and_c_data_roundtrip() {
    let variants: Vec<_> = [
        "null",
        "true",
        "1234567890123456789",
        "1.25",
        r#"{"a":[1,"x",null]}"#,
    ]
    .iter()
    .map(|json| GenericVariant::parse_json(json).unwrap())
    .collect();
    let input = input_array(
        variants.iter().map(|v| Some(v.value())).collect(),
        variants.iter().map(|v| Some(v.metadata())).collect(),
        None,
    );
    let normalized = canonicalize_variants(&input, &variant_type(), "v").unwrap();
    let ffi_array = FFI_ArrowArray::new(&normalized.to_data());
    let ffi_schema = FFI_ArrowSchema::try_from(normalized.data_type()).unwrap();
    // The imported array owns the exported buffers through Arrow's release callback.
    let imported = make_array(unsafe { from_ffi(ffi_array, &ffi_schema) }.unwrap());
    for (row, variant) in variants.iter().enumerate() {
        assert_payload(&imported, row, variant);
    }
    let unchanged = canonicalize_variants(&normalized, &variant_type(), "v").unwrap();
    assert!(Arc::ptr_eq(&normalized, &unchanged));
}

#[test]
fn native_variant_preserves_non_json_scalar_types() {
    let metadata = GenericVariant::parse_json("null")
        .unwrap()
        .metadata()
        .to_vec();
    // Paimon Variant primitive tags occupy the upper six header bits. These
    // values must remain DATE, TIMESTAMP_NTZ and BINARY, not JSON strings.
    let mut date = vec![11 << 2];
    date.extend_from_slice(&20000_i32.to_le_bytes());
    let mut timestamp_ntz = vec![13 << 2];
    timestamp_ntz.extend_from_slice(&123456789_i64.to_le_bytes());
    let mut binary = vec![15 << 2];
    binary.extend_from_slice(&3_u32.to_le_bytes());
    binary.extend_from_slice(&[0, 128, 255]);
    for bytes in [date, timestamp_ntz, binary] {
        let expected = GenericVariant::from_parts(bytes, metadata.clone()).unwrap();
        let input = input_array(
            vec![Some(expected.value())],
            vec![Some(expected.metadata())],
            None,
        );
        let normalized = canonicalize_variants(&input, &variant_type(), "v").unwrap();
        assert_payload(&normalized, 0, &expected);
    }
}

#[test]
fn native_variant_distinguishes_sql_null_from_encoded_null_after_slice() {
    let value = GenericVariant::parse_json("7").unwrap();
    let encoded_null = GenericVariant::parse_json("null").unwrap();
    let input = input_array(
        vec![
            Some(value.value()),
            Some(value.value()),
            Some(&[]),
            None,
            Some(encoded_null.value()),
        ],
        vec![
            Some(value.metadata()),
            Some(value.metadata()),
            Some(&[]),
            None,
            Some(encoded_null.metadata()),
        ],
        Some(NullBuffer::from(vec![true, false, true, true, true])),
    );
    let normalized = canonicalize_variants(&input.slice(1, 4), &variant_type(), "v").unwrap();
    assert_eq!(normalized.null_count(), 3);
    for row in 0..3 {
        assert!(normalized.is_null(row));
    }
    assert_payload(&normalized, 3, &encoded_null);
}

#[test]
fn native_variant_rejects_incomplete_payload() {
    let value = GenericVariant::parse_json("7").unwrap();
    for (v, m) in [
        (None, Some(value.metadata())),
        (Some(value.value()), None),
        (Some(&[][..]), Some(value.metadata())),
    ] {
        let input = input_array(vec![v], vec![m], None);
        let error = canonicalize_variants(&input, &variant_type(), "v").unwrap_err();
        assert!(error.to_string().contains("Incomplete Variant payload"));
    }
    let input: ArrayRef = Arc::new(Int32Array::from(vec![1]));
    assert!(canonicalize_variants(&input, &variant_type(), "v").is_err());
    let malformed = input_array(vec![Some(&[0xff])], vec![Some(value.metadata())], None);
    assert!(canonicalize_variants(&malformed, &variant_type(), "v").is_err());
}

#[test]
fn native_variant_normalizes_array_map_and_row() {
    let value = GenericVariant::parse_json(r#"{"a":1}"#).unwrap();
    let variant = input_array(
        vec![Some(value.value()), None],
        vec![Some(value.metadata()), None],
        None,
    );
    let entries = StructArray::new(
        vec![
            Arc::new(ArrowField::new("key", ArrowDataType::Int32, false)),
            Arc::new(ArrowField::new("value", variant.data_type().clone(), true)),
        ]
        .into(),
        vec![Arc::new(Int32Array::from(vec![1, 2])), variant],
        None,
    );
    let map: ArrayRef = Arc::new(MapArray::new(
        Arc::new(ArrowField::new(
            "entries",
            entries.data_type().clone(),
            false,
        )),
        OffsetBuffer::new(vec![0, 2].into()),
        entries,
        None,
        false,
    ));
    let list: ArrayRef = Arc::new(ListArray::new(
        Arc::new(ArrowField::new("item", map.data_type().clone(), true)),
        OffsetBuffer::new(vec![0, 1].into()),
        map,
        None,
    ));
    let row: ArrayRef = Arc::new(StructArray::new(
        vec![Arc::new(ArrowField::new(
            "items",
            list.data_type().clone(),
            true,
        ))]
        .into(),
        vec![list],
        None,
    ));
    let row_type = DataType::Row(RowType::new(vec![DataField::new(
        1,
        "items".into(),
        DataType::Array(ArrayType::new(DataType::Map(MapType::new(
            DataType::Int(IntType::new()),
            variant_type(),
        )))),
    )]));
    let schema = Arc::new(ArrowSchema::new_with_metadata(
        vec![ArrowField::new("r", row.data_type().clone(), true)],
        HashMap::from([("source".into(), "test".into())]),
    ));
    let batch = RecordBatch::try_new(schema, vec![row]).unwrap();
    let normalized = canonicalize_batch(batch, &[DataField::new(0, "r".into(), row_type)]).unwrap();
    assert_eq!(
        normalized.schema().metadata().get("source").unwrap(),
        "test"
    );
    let row = normalized
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let list = row.column(0).as_any().downcast_ref::<ListArray>().unwrap();
    let map = list.values().as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(list.value_offsets(), &[0, 1]);
    assert_eq!(map.value_offsets(), &[0, 2]);
    assert_payload(map.values(), 0, &value);
    assert!(map.values().is_null(1));
}

#[test]
fn normalize_only_visible_collection_entries() {
    let encoded_null = GenericVariant::parse_json("null").unwrap();
    let input = input_array(
        vec![None, Some(encoded_null.value()), Some(&[0xff])],
        vec![None, Some(encoded_null.metadata()), None],
        None,
    );
    let key = StructArray::new(
        vec![Arc::new(ArrowField::new(
            "v",
            input.data_type().clone(),
            true,
        ))]
        .into(),
        vec![input.clone()],
        None,
    );
    let entries = StructArray::new(
        vec![
            Arc::new(ArrowField::new("key", key.data_type().clone(), false)),
            Arc::new(ArrowField::new("count", ArrowDataType::Int32, false)),
        ]
        .into(),
        vec![Arc::new(key), Arc::new(Int32Array::from(vec![4, 3, 2]))],
        None,
    );
    let offsets = OffsetBuffer::new(vec![0, 1, 2, 3].into());
    let list: ArrayRef = Arc::new(ListArray::new(
        Arc::new(ArrowField::new("item", input.data_type().clone(), true)),
        offsets.clone(),
        input,
        None,
    ));
    let ty = DataType::Array(ArrayType::new(variant_type()));
    assert!(canonicalize_variants(&list, &ty, "a").is_err());
    let selected = canonicalize_variants(&list.slice(1, 1), &ty, "a").unwrap();
    let list = selected.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.value_offsets(), &[0, 1]);
    assert_payload(list.values(), 0, &encoded_null);

    let map: ArrayRef = Arc::new(MapArray::new(
        Arc::new(ArrowField::new(
            "entries",
            entries.data_type().clone(),
            false,
        )),
        offsets,
        entries,
        Some(NullBuffer::from(vec![true, true, false])),
        false,
    ));
    let ty = DataType::Multiset(paimon::spec::MultisetType::new(DataType::Row(
        RowType::new(vec![DataField::new(1, "v".into(), variant_type())]),
    )));
    let selected = canonicalize_variants(&map, &ty, "m").unwrap();
    let map = selected.as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(map.value_offsets(), &[0, 1, 2, 2]);
    let variants = map
        .keys()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .column(0);
    assert!(variants.is_null(0));
    assert_payload(variants, 1, &encoded_null);
    assert_eq!(
        map.values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[4, 3]
    );
}

#[test]
fn native_variant_does_not_reinterpret_binary_row() {
    let input = input_array(vec![Some(&[])], vec![Some(&[])], None);
    let binary = DataType::VarBinary(VarBinaryType::new(100).unwrap());
    let row_type = DataType::Row(RowType::new(vec![
        DataField::new(0, "value".into(), binary.clone()),
        DataField::new(1, "metadata".into(), binary),
    ]));
    let normalized = canonicalize_variants(&input, &row_type, "r").unwrap();
    assert!(Arc::ptr_eq(&input, &normalized));
    assert!(normalized.is_valid(0));
}

#[test]
fn native_variant_supports_empty_batch() {
    let input = input_array(vec![], vec![], None);
    let normalized = canonicalize_variants(&input, &variant_type(), "v").unwrap();
    assert_eq!(normalized.len(), 0);
    assert_eq!(normalized.data_type(), &paimon::arrow::variant_arrow_type());
}
