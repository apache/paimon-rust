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
use arrow_array::types::{Int16Type, Int8Type};
use arrow_array::{
    Array, BinaryArray, BinaryViewArray, DictionaryArray, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, RunArray, StringArray, StringViewArray, UnionArray,
};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{Field, UnionFields};
use futures::TryStreamExt;
use std::sync::Arc;

fn numeric_union(dense: bool) -> ArrayRef {
    let fields = UnionFields::try_new(
        [7, 42, 99],
        [
            Field::new("small", DataType::Int32, true),
            Field::new("large", DataType::Int64, true),
            Field::new("unused", DataType::Utf8, true),
        ],
    )
    .unwrap();
    let (small, large) = if dense {
        (
            vec![Some(0), Some(1), Some(2)],
            vec![Some(i64::MAX), Some(1), None],
        )
    } else {
        (
            vec![Some(0), Some(1), Some(0), Some(2), Some(0)],
            vec![
                Some(i64::MAX),
                Some(i64::MAX),
                Some(1),
                Some(i64::MAX),
                None,
            ],
        )
    };
    Arc::new(
        UnionArray::try_new(
            fields,
            ScalarBuffer::from(vec![42, 7, 42, 7, 42]),
            dense.then(|| ScalarBuffer::from(vec![0, 1, 1, 2, 2])),
            vec![
                Arc::new(Int32Array::from(small)),
                Arc::new(Int64Array::from(large)),
                Arc::new(StringArray::from(vec![
                    "never a numeric key";
                    if dense { 1 } else { 5 }
                ])),
            ],
        )
        .unwrap(),
    )
}

#[test]
fn union_match_keys_use_only_active_values_and_keep_logical_nulls() {
    for dense in [true, false] {
        let input = numeric_union(dense);
        let selected = input.slice(1, 4);
        let actual = normalize_match_key(&selected, &DataType::Int32).unwrap();
        assert_eq!(
            actual.to_data(),
            Int32Array::from(vec![Some(1), Some(1), Some(2), None]).to_data()
        );
        assert!(
            normalize_match_key(&input, &DataType::Int32).is_err(),
            "active overflow must fail"
        );
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![Some(3), None, Some(0), Some(1), Some(2)]),
                selected,
            )
            .unwrap(),
        );
        assert_eq!(
            normalize_match_key(&dictionary, &DataType::Int32)
                .unwrap()
                .to_data(),
            Int32Array::from(vec![None, None, Some(1), Some(1), Some(2)]).to_data()
        );
        let unused: ArrayRef = Arc::new(
            DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![None, None]), input).unwrap(),
        );
        assert_eq!(
            normalize_match_key(&unused, &DataType::Int32)
                .unwrap()
                .null_count(),
            2
        );
    }
}

#[test]
fn dictionary_view_keys_materialize_without_changing_bytes_or_nulls() {
    let strings: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(2), None, Some(0), Some(1)]),
            Arc::new(StringViewArray::from(vec![
                Some("a"),
                Some("longer-than-inline-view"),
                None,
            ])),
        )
        .unwrap(),
    );
    assert_eq!(
        normalize_match_key(&strings, &DataType::Utf8)
            .unwrap()
            .to_data(),
        StringArray::from(vec![None, None, Some("a"), Some("longer-than-inline-view")]).to_data()
    );
    let binary: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(2), None, Some(0), Some(1)]),
            Arc::new(BinaryViewArray::from(vec![
                Some(b"\xff\0".as_slice()),
                Some(b"longer-than-inline-view".as_slice()),
                None,
            ])),
        )
        .unwrap(),
    );
    assert_eq!(
        normalize_match_key(&binary, &DataType::Binary)
            .unwrap()
            .to_data(),
        BinaryArray::from(vec![
            None,
            None,
            Some(b"\xff\0".as_slice()),
            Some(b"longer-than-inline-view".as_slice())
        ])
        .to_data()
    );
}

#[test]
fn dictionary_run_end_keys_decode_slices_and_ignore_unselected_overflow() {
    let values = RunArray::<Int16Type>::try_new(
        &Int16Array::from(vec![2, 4, 5]),
        &Int64Array::from(vec![Some(i64::MAX), Some(1), None]),
    )
    .unwrap();
    let values: ArrayRef = Arc::new(values.slice(2, 3));
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(2), None, Some(0), Some(1)]),
            values,
        )
        .unwrap(),
    );
    assert_eq!(
        normalize_match_key(&dictionary, &DataType::Int32)
            .unwrap()
            .to_data(),
        Int32Array::from(vec![None, None, Some(1), Some(1)]).to_data()
    );
}

#[test]
fn union_key_conversion_never_uses_lossy_assignment_or_text_parsing() {
    let fields = UnionFields::try_new(
        [7, 42],
        [
            Field::new("number", DataType::Float64, true),
            Field::new("text", DataType::Utf8, true),
        ],
    )
    .unwrap();
    let input: ArrayRef = Arc::new(
        UnionArray::try_new(
            fields,
            ScalarBuffer::from(vec![7, 7, 42]),
            Some(ScalarBuffer::from(vec![0, 1, 0])),
            vec![
                Arc::new(Float64Array::from(vec![1.0, 1.5])),
                Arc::new(StringArray::from(vec!["1"])),
            ],
        )
        .unwrap(),
    );
    assert_eq!(
        normalize_match_key(&input.slice(0, 1), &DataType::Int32)
            .unwrap()
            .to_data(),
        Int32Array::from(vec![1]).to_data()
    );
    assert!(normalize_match_key(&input.slice(1, 1), &DataType::Int32).is_err());
    assert!(normalize_match_key(&input.slice(2, 1), &DataType::Int32).is_err());
}

#[test]
fn typed_null_string_keys_match_without_casting_unselected_dictionary_values() {
    let plain: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>; 3]));
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), Some(1), None, Some(1), Some(0)]),
            Arc::new(StringArray::from(vec![Some("not an integer"), None])),
        )
        .unwrap(),
    );
    for input in [plain, dictionary.slice(1, 3)] {
        let normalized = normalize_match_key(&input, &DataType::Int32).unwrap();
        assert_eq!(normalized.data_type(), &DataType::Int32);
        assert_eq!(normalized.len(), 3);
        assert_eq!(normalized.null_count(), 3);
    }
    assert!(normalize_match_key(&dictionary, &DataType::Int32).is_err());
    let non_null: ArrayRef = Arc::new(StringArray::from(vec![None, Some("1")]));
    assert!(normalize_match_key(&non_null, &DataType::Int32).is_err());
}

#[test]
fn typed_null_union_children_match_null_keys_without_casting_their_type() {
    for dense in [true, false] {
        let fields = UnionFields::try_new(
            [7, 42],
            [
                Field::new("number", DataType::Int32, true),
                Field::new("null_text", DataType::Utf8, true),
            ],
        )
        .unwrap();
        let input: ArrayRef = Arc::new(
            UnionArray::try_new(
                fields,
                ScalarBuffer::from(vec![7, 42]),
                dense.then(|| ScalarBuffer::from(vec![0, 0])),
                if dense {
                    vec![
                        Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                        Arc::new(StringArray::from(vec![None::<&str>])),
                    ]
                } else {
                    vec![
                        Arc::new(Int32Array::from(vec![1, 0])) as ArrayRef,
                        Arc::new(StringArray::from(vec![Some("inactive"), None])),
                    ]
                },
            )
            .unwrap(),
        );
        let expected = Int32Array::from(vec![Some(1), None]);
        assert_eq!(
            normalize_match_key(&input, &DataType::Int32)
                .unwrap()
                .to_data(),
            expected.to_data()
        );
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![Some(1), Some(0), None]),
                input,
            )
            .unwrap(),
        );
        assert_eq!(
            normalize_match_key(&dictionary, &DataType::Int32)
                .unwrap()
                .to_data(),
            Int32Array::from(vec![None, Some(1), None]).to_data()
        );
    }
}

async fn seeded_table(keys: ArrayRef) -> crate::table::Table {
    use crate::spec::{
        DataType as PaimonType, IntType, Schema, TableSchema, VarBinaryType, VarCharType,
    };
    let key_type = match keys.data_type() {
        DataType::Int32 => PaimonType::Int(IntType::new()),
        DataType::Utf8 => PaimonType::VarChar(VarCharType::string_type()),
        DataType::Binary => PaimonType::VarBinary(VarBinaryType::default()),
        _ => unreachable!(),
    };
    let schema = Schema::builder()
        .column("key", key_type)
        .column("value", PaimonType::Int(IntType::new()))
        .column("keep", PaimonType::Int(IntType::new()))
        .option("data-evolution.enabled", "true")
        .option("row-tracking.enabled", "true")
        .build()
        .unwrap();
    let table = crate::table::Table::new(
        crate::io::FileIOBuilder::new("memory").build().unwrap(),
        crate::catalog::Identifier::new("db", "encoded"),
        "memory:/encoded-upsert".into(),
        TableSchema::new(0, &schema),
        None,
    );
    let initial = arrow_array::RecordBatch::try_from_iter([
        ("key", keys),
        (
            "value",
            Arc::new(Int32Array::from(vec![10, 11, 20, 30])) as ArrayRef,
        ),
        (
            "keep",
            Arc::new(Int32Array::from(vec![0, 1, 2, 3])) as ArrayRef,
        ),
    ])
    .unwrap();
    let mut writer = table.new_write_builder().new_write().unwrap();
    writer.write_arrow_batch(&initial).await.unwrap();
    crate::table::TableCommit::new(table.clone(), "seed".into())
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();
    writer.close().await;
    table
}

async fn stored_values(table: &crate::table::Table) -> Vec<(i32, i32)> {
    let mut builder = table.new_read_builder();
    builder.with_projection(&["keep", "value"]).unwrap();
    let plan = builder.new_scan().plan().await.unwrap();
    let mut rows = Vec::new();
    let mut stream = builder.new_read().unwrap().to_arrow(plan.splits()).unwrap();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let keep = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        rows.extend((0..batch.num_rows()).map(|row| (keep.value(row), values.value(row))));
    }
    rows.sort_unstable();
    rows
}

#[tokio::test]
async fn encoded_keys_drive_upsert_last_wins_and_existing_row_fanout() {
    let mut cases = Vec::<(ArrayRef, ArrayRef)>::new();
    for dense in [true, false] {
        let union = numeric_union(dense).slice(1, 4);
        let indices = arrow_array::UInt32Array::from(vec![0, 3, 1, 2]);
        let direct = arrow_select::take::take(union.as_ref(), &indices, None).unwrap();
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![Some(0), None, Some(1), Some(2)]),
                union,
            )
            .unwrap(),
        );
        for input in [direct, dictionary] {
            cases.push((
                Arc::new(Int32Array::from(vec![Some(1), Some(1), Some(2), None])),
                input,
            ));
        }
    }
    let strings: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), None, Some(0), Some(1)]),
            Arc::new(StringViewArray::from(vec![
                Some("a"),
                Some("longer-than-inline-view"),
                None,
            ])),
        )
        .unwrap(),
    );
    cases.push((
        Arc::new(StringArray::from(vec![
            Some("a"),
            Some("a"),
            Some("longer-than-inline-view"),
            None,
        ])),
        strings,
    ));
    let binary: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), None, Some(0), Some(1)]),
            Arc::new(BinaryViewArray::from(vec![
                Some(b"\xff\0".as_slice()),
                Some(b"longer-than-inline-view".as_slice()),
            ])),
        )
        .unwrap(),
    );
    cases.push((
        Arc::new(BinaryArray::from(vec![
            Some(b"\xff\0".as_slice()),
            Some(b"\xff\0".as_slice()),
            Some(b"longer-than-inline-view".as_slice()),
            None,
        ])),
        binary,
    ));
    let runs: ArrayRef = Arc::new(
        RunArray::<Int16Type>::try_new(
            &Int16Array::from(vec![2, 3]),
            &Int32Array::from(vec![1, 2]),
        )
        .unwrap(),
    );
    let runs: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), None, Some(1), Some(2)]),
            runs,
        )
        .unwrap(),
    );
    cases.push((
        Arc::new(Int32Array::from(vec![Some(1), Some(1), Some(2), None])),
        runs,
    ));
    for (seed, input) in cases {
        let table = seeded_table(seed).await;
        let source = arrow_array::RecordBatch::try_from_iter([
            ("key", input),
            (
                "value",
                Arc::new(Int32Array::from(vec![90, 91, 92, 93])) as ArrayRef,
            ),
        ])
        .unwrap();
        let mut update = table.new_write_builder().new_update().unwrap();
        update.with_update_type(vec!["value".into()]).unwrap();
        let messages = update
            .upsert_by_arrow_with_key(
                vec![source.slice(0, 2), source.slice(2, 2)],
                vec!["key".into()],
            )
            .await
            .unwrap();
        crate::table::TableCommit::new(table.clone(), "update".into())
            .commit(messages)
            .await
            .unwrap();
        assert_eq!(
            stored_values(&table).await,
            vec![(0, 92), (1, 92), (2, 93), (3, 91)]
        );
    }
}

#[tokio::test]
async fn invalid_encoded_key_fails_before_any_matched_updates_are_staged() {
    let table = seeded_table(Arc::new(Int32Array::from(vec![
        Some(1),
        Some(1),
        Some(2),
        None,
    ])))
    .await;
    let fields = UnionFields::try_new(
        [7, 42],
        [
            Field::new("valid", DataType::Int32, true),
            Field::new("invalid", DataType::Float64, true),
        ],
    )
    .unwrap();
    let keys: ArrayRef = Arc::new(
        UnionArray::try_new(
            fields,
            ScalarBuffer::from(vec![7, 42]),
            Some(ScalarBuffer::from(vec![0, 0])),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Float64Array::from(vec![1.5])),
            ],
        )
        .unwrap(),
    );
    let source = arrow_array::RecordBatch::try_from_iter([
        ("key", keys),
        (
            "value",
            Arc::new(Int32Array::from(vec![90, 91])) as ArrayRef,
        ),
    ])
    .unwrap();
    let before = table
        .file_io()
        .list_status_recursive(table.location())
        .await
        .unwrap()
        .len();
    let mut update = table.new_write_builder().new_update().unwrap();
    update.with_update_type(vec!["value".into()]).unwrap();
    assert!(update
        .upsert_by_arrow_with_key(vec![source], vec!["key".into()])
        .await
        .is_err());
    assert_eq!(
        table
            .file_io()
            .list_status_recursive(table.location())
            .await
            .unwrap()
            .len(),
        before
    );
    assert_eq!(
        stored_values(&table).await,
        vec![(0, 10), (1, 11), (2, 20), (3, 30)]
    );

    // An exact Union key can match existing rows, but an unmatched row still
    // follows ordinary append layout rules. Key normalization must never
    // silently rewrite its payload or stage matched updates before this fails.
    let keys: ArrayRef = Arc::new(
        UnionArray::try_new(
            UnionFields::try_new([7], [Field::new("number", DataType::Int32, true)]).unwrap(),
            ScalarBuffer::from(vec![7, 7]),
            Some(ScalarBuffer::from(vec![0, 1])),
            vec![Arc::new(Int32Array::from(vec![1, 3]))],
        )
        .unwrap(),
    );
    let source = arrow_array::RecordBatch::try_from_iter([
        ("key", keys),
        (
            "value",
            Arc::new(Int32Array::from(vec![90, 91])) as ArrayRef,
        ),
    ])
    .unwrap();
    let error = update
        .upsert_by_arrow_with_key(vec![source], vec!["key".into()])
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("Invalid upsert column 'key'"),
        "{error}"
    );
    assert_eq!(
        table
            .file_io()
            .list_status_recursive(table.location())
            .await
            .unwrap()
            .len(),
        before
    );
    assert_eq!(
        stored_values(&table).await,
        vec![(0, 10), (1, 11), (2, 20), (3, 30)]
    );
}
