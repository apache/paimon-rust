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

//! Persisted user-sequence controls for Java's `UserDefinedSeqComparator`.

#[path = "common/rowkind_helpers.rs"]
mod helpers;

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int32Array, RecordBatch, StringArray, Time32MillisecondArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use futures::StreamExt;
use helpers::{memory_table, persist_table_schema, setup_dirs, write_batch};
use paimon::spec::{
    BooleanType, DataType, DateType, DecimalType, DoubleType, FloatType, IntType,
    LocalZonedTimestampType, Schema, TableSchema, TimeType, TimestampType, VarBinaryType,
    VarCharType,
};
use paimon::table::Table;

fn sequence_batch(rows: &[(Option<&str>, i32)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("seq", ArrowDataType::Utf8, true),
            ArrowField::new("value", ArrowDataType::Int32, true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1; rows.len()])),
            Arc::new(StringArray::from(
                rows.iter().map(|(seq, _)| *seq).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|(_, value)| *value).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

async fn sequence_table(path: &str, engine: &str, descending: bool) -> Table {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("seq", DataType::VarChar(VarCharType::string_type()))
        .column("value", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("merge-engine", engine)
        .option("sequence.field", "seq")
        .option(
            "sequence.field.sort-order",
            if descending {
                "descending"
            } else {
                "ascending"
            },
        )
        .build()
        .unwrap();
    let (file_io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&file_io, path).await;
    persist_table_schema(&file_io, path, table.schema()).await;
    table
}

async fn scan_sequence_value(table: &Table) -> (Option<String>, i32) {
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let mut stream = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap();
    let batch = stream.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    let sequences = batch
        .column_by_name("seq")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let values = batch
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let result = (
        (!sequences.is_null(0)).then(|| sequences.value(0).to_string()),
        values.value(0),
    );
    assert!(stream.next().await.is_none());
    result
}

#[tokio::test]
async fn string_sequence_respects_order_in_flush_and_across_commits() {
    // The one-commit case tests the write buffer; separate commits test the
    // read-side merge of independently persisted files. Java chooses the
    // lexicographically greatest sequence in ascending mode and the least in
    // descending mode, regardless of arrival order.
    for engine in ["deduplicate", "partial-update", "aggregation"] {
        for descending in [false, true] {
            for separate_commits in [false, true] {
                let path =
                    format!("memory:/pk_user_sequence/{engine}_{descending}_{separate_commits}");
                let table = sequence_table(&path, engine, descending).await;
                if separate_commits {
                    write_batch(&table, &sequence_batch(&[(Some("z"), 10)])).await;
                    write_batch(&table, &sequence_batch(&[(Some("a"), 20)])).await;
                } else {
                    write_batch(&table, &sequence_batch(&[(Some("z"), 10), (Some("a"), 20)])).await;
                }
                let expected = if descending {
                    (Some("a".to_string()), 20)
                } else {
                    (Some("z".to_string()), 10)
                };
                assert_eq!(
                    scan_sequence_value(&table).await,
                    expected,
                    "{engine}, descending={descending}, separate_commits={separate_commits}"
                );
            }
        }
    }
}

#[tokio::test]
async fn null_user_sequence_stays_first_in_both_orders() {
    for descending in [false, true] {
        let path = format!("memory:/pk_user_sequence/null_{descending}");
        let table = sequence_table(&path, "deduplicate", descending).await;
        write_batch(&table, &sequence_batch(&[(Some("z"), 10)])).await;
        write_batch(&table, &sequence_batch(&[(None, 20)])).await;
        assert_eq!(
            scan_sequence_value(&table).await,
            (Some("z".into()), 10),
            "descending={descending}"
        );
    }
}

struct TypedSequenceCase {
    name: &'static str,
    field_type: DataType,
    low: ArrayRef,
    high: ArrayRef,
}

fn typed_sequence_cases() -> Vec<TypedSequenceCase> {
    vec![
        TypedSequenceCase {
            name: "boolean",
            field_type: DataType::Boolean(BooleanType::new()),
            low: Arc::new(BooleanArray::from(vec![false])),
            high: Arc::new(BooleanArray::from(vec![true])),
        },
        TypedSequenceCase {
            name: "binary",
            field_type: DataType::VarBinary(VarBinaryType::new(16).unwrap()),
            low: Arc::new(BinaryArray::from_iter_values([b"\x00".as_slice()])),
            high: Arc::new(BinaryArray::from_iter_values([b"\xff".as_slice()])),
        },
        TypedSequenceCase {
            name: "date",
            field_type: DataType::Date(DateType::new()),
            low: Arc::new(Date32Array::from(vec![-1])),
            high: Arc::new(Date32Array::from(vec![1])),
        },
        TypedSequenceCase {
            name: "decimal",
            field_type: DataType::Decimal(DecimalType::new(10, 2).unwrap()),
            low: Arc::new(
                Decimal128Array::from(vec![-123_i128])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
            high: Arc::new(
                Decimal128Array::from(vec![456_i128])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
        },
        TypedSequenceCase {
            name: "float",
            field_type: DataType::Float(FloatType::new()),
            low: Arc::new(Float32Array::from(vec![-0.0])),
            high: Arc::new(Float32Array::from(vec![0.0])),
        },
        TypedSequenceCase {
            name: "double",
            field_type: DataType::Double(DoubleType::new()),
            low: Arc::new(Float64Array::from(vec![-0.0])),
            high: Arc::new(Float64Array::from(vec![0.0])),
        },
        TypedSequenceCase {
            name: "time",
            field_type: DataType::Time(TimeType::new(3).unwrap()),
            low: Arc::new(Time32MillisecondArray::from(vec![0])),
            high: Arc::new(Time32MillisecondArray::from(vec![86_399_999])),
        },
        TypedSequenceCase {
            name: "timestamp",
            field_type: DataType::Timestamp(TimestampType::new(6).unwrap()),
            low: Arc::new(TimestampMicrosecondArray::from(vec![-1])),
            high: Arc::new(TimestampMicrosecondArray::from(vec![1])),
        },
        TypedSequenceCase {
            name: "timestamp_ltz",
            field_type: DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(6).unwrap()),
            low: Arc::new(TimestampMicrosecondArray::from(vec![-1]).with_timezone("UTC")),
            high: Arc::new(TimestampMicrosecondArray::from(vec![1]).with_timezone("UTC")),
        },
    ]
}

fn typed_sequence_batch(sequence: ArrayRef, value: i32) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("seq", sequence.data_type().clone(), true),
            ArrowField::new("value", ArrowDataType::Int32, true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            sequence,
            Arc::new(Int32Array::from(vec![value])),
        ],
    )
    .unwrap()
}

async fn typed_sequence_table(
    path: &str,
    engine: &str,
    sequence_type: DataType,
    descending: bool,
) -> Table {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("seq", sequence_type)
        .column("value", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("merge-engine", engine)
        .option("sequence.field", "seq")
        .option(
            "sequence.field.sort-order",
            if descending {
                "descending"
            } else {
                "ascending"
            },
        )
        .build()
        .unwrap();
    let (file_io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&file_io, path).await;
    persist_table_schema(&file_io, path, table.schema()).await;
    table
}

async fn scan_typed_sequence_value(table: &Table) -> i32 {
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let mut stream = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap();
    let batch = stream.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    let value = batch
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .value(0);
    assert!(stream.next().await.is_none());
    value
}

#[tokio::test]
async fn typed_user_sequences_match_java_in_flush_and_across_files() {
    // These types previously fell through the reader's integer-only sequence
    // extraction and were all treated as equal. Both commit layouts exercise
    // the write-buffer comparator and the cross-file reader comparator.
    for case in typed_sequence_cases() {
        for engine in ["deduplicate", "partial-update", "aggregation"] {
            for descending in [false, true] {
                for separate_commits in [false, true] {
                    let path = format!(
                        "memory:/pk_user_sequence/typed_{}_{}_{}_{}",
                        case.name, engine, descending, separate_commits
                    );
                    let table =
                        typed_sequence_table(&path, engine, case.field_type.clone(), descending)
                            .await;
                    let high = typed_sequence_batch(case.high.clone(), 10);
                    let low = typed_sequence_batch(case.low.clone(), 20);
                    if separate_commits {
                        write_batch(&table, &high).await;
                        write_batch(&table, &low).await;
                    } else {
                        let batch =
                            arrow_select::concat::concat_batches(&high.schema(), &[high, low])
                                .unwrap();
                        write_batch(&table, &batch).await;
                    }
                    assert_eq!(
                        scan_typed_sequence_value(&table).await,
                        if descending { 20 } else { 10 },
                        "type={}, engine={engine}, descending={descending}, separate_commits={separate_commits}",
                        case.name
                    );
                }
            }
        }
    }
}

fn composite_sequence_batch(rows: &[(Option<&str>, Option<i32>, i32)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("seq_text", ArrowDataType::Utf8, true),
            ArrowField::new("seq_number", ArrowDataType::Int32, true),
            ArrowField::new("value", ArrowDataType::Int32, true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1; rows.len()])),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

async fn composite_sequence_table(path: &str, engine: &str, descending: bool) -> Table {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("seq_text", DataType::VarChar(VarCharType::string_type()))
        .column("seq_number", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("merge-engine", engine)
        .option("sequence.field", "seq_text,seq_number")
        .option(
            "sequence.field.sort-order",
            if descending {
                "descending"
            } else {
                "ascending"
            },
        )
        .build()
        .unwrap();
    let (file_io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&file_io, path).await;
    persist_table_schema(&file_io, path, table.schema()).await;
    table
}

#[tokio::test]
async fn composite_user_sequence_uses_lexicographic_order_and_nulls_first() {
    // The second sequence field decides between the two "b" rows. A NULL in
    // either field is smaller than a non-NULL value in both directions.
    let rows = [
        (Some("b"), Some(0), 10),
        (Some("a"), Some(9), 20),
        (Some("b"), Some(1), 30),
        (Some("b"), None, 40),
        (None, Some(100), 50),
    ];
    for engine in ["deduplicate", "partial-update", "aggregation"] {
        for descending in [false, true] {
            for separate_commits in [false, true] {
                let path = format!(
                    "memory:/pk_user_sequence/composite_{engine}_{descending}_{separate_commits}"
                );
                let table = composite_sequence_table(&path, engine, descending).await;
                if separate_commits {
                    for row in rows {
                        write_batch(&table, &composite_sequence_batch(&[row])).await;
                    }
                } else {
                    write_batch(&table, &composite_sequence_batch(&rows)).await;
                }
                assert_eq!(
                    scan_typed_sequence_value(&table).await,
                    if descending { 20 } else { 30 },
                    "engine={engine}, descending={descending}, separate_commits={separate_commits}"
                );
            }
        }
    }
}
