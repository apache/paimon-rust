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
use crate::bucket_router::*;
use arrow_array::{ArrayRef, Int64Array, LargeBinaryArray, RecordBatchOptions};
use paimon::spec::{BigIntType, DecimalType, TimestampType};

fn schema(data_type: DataType, function: &str, partitioned: bool) -> TableSchema {
    let mut builder = Schema::builder()
        .column("pt", DataType::VarChar(VarCharType::string_type()))
        .column("id", data_type)
        .option("bucket", "100")
        .option("bucket-key", "id")
        .option("bucket-function.type", function)
        .option("write-only", "true");
    if partitioned {
        builder = builder.partition_keys(["pt"]);
    }
    TableSchema::new(0, &builder.build().unwrap())
}

fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    RecordBatch::try_from_iter(columns).unwrap()
}

unsafe fn router(schema: &TableSchema) -> *mut paimon_bucket_router {
    let json = CString::new(serde_json::to_string(schema).unwrap()).unwrap();
    let result = paimon_bucket_router_new_from_schema_json(json.as_ptr());
    assert!(result.error.is_null());
    result.router
}

unsafe fn route(
    handle: *const paimon_bucket_router,
    input: RecordBatch,
) -> crate::result::paimon_result_next_batch {
    let (mut array, mut schema) = export_batch_to_ffi(input);
    let output = paimon_bucket_router_route_with_partitions_arrow(
        handle,
        (&mut **array as *mut FFI_ArrowArray).cast(),
        (&mut **schema as *mut FFI_ArrowSchema).cast(),
    );
    assert!(array.is_released());
    assert!(schema.release.is_none());
    output
}

unsafe fn import(output: crate::result::paimon_result_next_batch) -> RecordBatch {
    assert!(output.error.is_null());
    let batch = crate::write::import_record_batch(output.batch.array, output.batch.schema).unwrap();
    paimon_arrow_batch_free(output.batch);
    batch
}

#[test]
fn joint_routing_java_goldens_and_output_lifetime() {
    let cases: Vec<(DataType, ArrayRef, Vec<i32>)> = vec![
        (
            DataType::Int(IntType::new()),
            Arc::new(Int32Array::from(vec![i32::MIN, -3, 0, 5, i32::MAX])),
            vec![19, 15, 99, 96, 21],
        ),
        (
            DataType::VarChar(VarCharType::string_type()),
            Arc::new(StringArray::from(vec![
                None,
                Some(""),
                Some("hello"),
                Some("abcdefghijk"),
                Some("你好"),
            ])),
            vec![44, 19, 46, 23, 58],
        ),
        (
            DataType::Decimal(DecimalType::new(20, 2).unwrap()),
            Arc::new(
                arrow_array::Decimal128Array::from(vec![
                    None,
                    Some(12340),
                    Some(-9999999),
                    Some(0),
                ])
                .with_precision_and_scale(20, 2)
                .unwrap(),
            ),
            vec![60, 60, 44, 85],
        ),
        (
            DataType::Timestamp(TimestampType::new(9).unwrap()),
            Arc::new(arrow_array::TimestampNanosecondArray::from(vec![
                -1_000_001,
                -1,
                0,
                1,
                1_000_001,
                i64::MAX,
            ])),
            vec![38, 85, 54, 32, 55, 39],
        ),
    ];
    // Same Java BinaryRow fixtures as the core bucket-router suite.
    for (data_type, values, expected) in cases {
        unsafe {
            let handle = router(&schema(data_type, "default", false));
            let input = batch(vec![("id", values)]);
            let output = route(handle, input.clone());
            let again = import(route(handle, input));
            paimon_bucket_router_free(handle);
            let output = import(output);
            assert_eq!(&output, &again);
            assert_eq!(
                output
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values(),
                &expected
            );
            let keys = output
                .column(0)
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap();
            assert!(keys.iter().all(|key| key == keys.iter().next().unwrap()));
        }
    }
    for (function, expected) in [
        ("mod", vec![0, 92, 97, 0, 7]),
        ("hive", vec![0, 0, 2, 0, 0]),
    ] {
        unsafe {
            let handle = router(&schema(
                DataType::BigInt(BigIntType::new()),
                function,
                false,
            ));
            let input = batch(vec![(
                "id",
                Arc::new(Int64Array::from(vec![
                    None,
                    Some(i64::MIN),
                    Some(-3),
                    Some(0),
                    Some(i64::MAX),
                ])),
            )]);
            let output = import(route(handle, input));
            // Explicit expectations use 100 buckets. Java MOD uses floorMod(value, count).
            assert_eq!(
                output
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values(),
                &expected
            );
            paimon_bucket_router_free(handle);
        }
    }
}

#[test]
fn joint_routing_matches_writer_partition_and_bucket_metadata() {
    let schema = schema(DataType::Int(IntType::new()), "default", true);
    let input = batch(vec![
        ("id", Arc::new(Int32Array::from(vec![5, 0, -3]))),
        ("pt", Arc::new(StringArray::from(vec!["a", "b", "a"]))),
    ]);
    let result = unsafe {
        let handle = router(&schema);
        let output = import(route(handle, input.clone()));
        let missing = route(handle, input.project(&[0]).unwrap());
        assert!(!missing.error.is_null());
        paimon_error_free(missing.error);
        paimon_bucket_router_free(handle);
        output
    };
    let keys = result
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert_eq!(keys.value(0), keys.value(2));
    assert_ne!(keys.value(0), keys.value(1));
    let buckets = result
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(buckets.values(), &[96, 99, 15]);
    crate::runtime().block_on(async {
        let io = memory_file_io();
        let path = "memory:/joint_routing_writer";
        io.mkdirs(&format!("{path}/schema")).await.unwrap();
        io.mkdirs(&format!("{path}/snapshot")).await.unwrap();
        let table = Table::new(
            io.clone(),
            Identifier::new("db", "route"),
            path.into(),
            schema,
            None,
        );
        let mut writer = table.new_write_builder().new_write().unwrap();
        writer
            .write_arrow_batch(&input.project(&[1, 0]).unwrap())
            .await
            .unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        let mut actual = messages
            .iter()
            .map(|message| {
                assert_eq!(
                    message
                        .new_files
                        .iter()
                        .map(|file| file.row_count)
                        .sum::<i64>(),
                    1
                );
                (message.partition.clone(), message.bucket)
            })
            .collect::<Vec<_>>();
        let mut expected = (0..3)
            .map(|row| (keys.value(row).to_vec(), buckets.value(row)))
            .collect::<Vec<_>>();
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected);
    });
}

#[test]
fn joint_routing_errors_consume_imported_inputs_and_return_empty_output() {
    unsafe {
        let handle = router(&schema(DataType::Int(IntType::new()), "default", false));
        for input in [
            batch(vec![("wrong", Arc::new(Int32Array::from(vec![5])))]),
            batch(vec![("id", Arc::new(StringArray::from(vec!["bad"])))]),
            batch(vec![
                ("id", Arc::new(Int32Array::from(vec![5]))),
                ("id", Arc::new(Int32Array::from(vec![5]))),
            ]),
        ] {
            let output = route(handle, input);
            assert!(!output.error.is_null());
            assert!(output.batch.array.is_null() && output.batch.schema.is_null());
            paimon_error_free(output.error);
        }
        let (mut array, mut schema) =
            export_batch_to_ffi(batch(vec![("id", Arc::new(Int32Array::from(vec![5])))]));
        let output = paimon_bucket_router_route_with_partitions_arrow(
            ptr::null(),
            (&mut **array as *mut FFI_ArrowArray).cast(),
            (&mut **schema as *mut FFI_ArrowSchema).cast(),
        );
        assert!(!array.is_released() && schema.release.is_some());
        paimon_error_free(output.error);
        drop(FFI_ArrowArray::from_raw(
            (&mut **array) as *mut FFI_ArrowArray,
        ));
        drop(FFI_ArrowSchema::from_raw(
            (&mut **schema) as *mut FFI_ArrowSchema,
        ));
        paimon_bucket_router_free(handle);
    }
}

#[test]
fn joint_routing_schema_export_and_zero_column_batches() {
    let definition = TableSchema::new(0, &Schema::builder().option("bucket", "1").build().unwrap());
    unsafe {
        let handle = router(&definition);
        let mut schema = FFI_ArrowSchema::empty();
        assert!(paimon_bucket_router_export_arrow_schema(
            handle,
            (&mut schema as *mut FFI_ArrowSchema).cast()
        )
        .is_null());
        let live = paimon_bucket_router_export_arrow_schema(
            handle,
            (&mut schema as *mut FFI_ArrowSchema).cast(),
        );
        assert!(!live.is_null());
        paimon_error_free(live);
        assert_eq!(ArrowSchema::try_from(&schema).unwrap().fields().len(), 0);
        schema.release.unwrap()(&mut schema);
        for rows in [0, 7] {
            let input = RecordBatch::try_new_with_options(
                Arc::new(ArrowSchema::empty()),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(rows)),
            )
            .unwrap();
            let output = import(route(handle, input));
            assert_eq!(output.num_rows(), rows);
            assert_eq!(
                output
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values(),
                &vec![0; rows]
            );
        }
        paimon_bucket_router_free(handle);
        paimon_bucket_router_free(ptr::null_mut());
    }
}

#[test]
fn joint_routing_rejects_null_keys_and_postpone_and_exports_union() {
    unsafe {
        let definition = schema(
            DataType::Int(IntType::with_nullable(false)),
            "default",
            true,
        );
        let handle = router(&definition);
        let mut exported = FFI_ArrowSchema::empty();
        assert!(paimon_bucket_router_export_arrow_schema(
            handle,
            (&mut exported as *mut FFI_ArrowSchema).cast()
        )
        .is_null());
        let names = ArrowSchema::try_from(&exported)
            .unwrap()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["pt", "id"]);
        let input = batch(vec![
            ("pt", Arc::new(StringArray::from(vec!["a"]))),
            ("id", Arc::new(Int32Array::from(vec![None]))),
        ]);
        let failed = route(handle, input);
        assert!(!failed.error.is_null());
        paimon_error_free(failed.error);
        paimon_bucket_router_free(handle);
        assert_eq!(ArrowSchema::try_from(&exported).unwrap().fields().len(), 2);
        exported.release.unwrap()(&mut exported);
        let definition = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .primary_key(["id"])
                .option("bucket", "-2")
                .build()
                .unwrap(),
        );
        let handle = router(&definition);
        let failed = route(
            handle,
            batch(vec![("id", Arc::new(Int32Array::from(vec![5])))]),
        );
        assert_eq!((*failed.error).code, PaimonErrorCode::Unsupported as i32);
        paimon_error_free(failed.error);
        paimon_bucket_router_free(handle);
    }
}

#[test]
fn joint_routing_import_failure_consumes_structs_and_calls_can_share_router() {
    unsafe {
        let handle = router(&schema(DataType::Int(IntType::new()), "default", false));
        let data = Int32Array::from(vec![5]).to_data();
        let mut array = FFI_ArrowArray::new(&data);
        let mut schema = FFI_ArrowSchema::try_from(data.data_type()).unwrap();
        let failed = paimon_bucket_router_route_with_partitions_arrow(
            handle,
            (&mut array as *mut FFI_ArrowArray).cast(),
            (&mut schema as *mut FFI_ArrowSchema).cast(),
        );
        assert!(array.is_released() && schema.release.is_none());
        assert!(!failed.error.is_null());
        assert!(failed.batch.array.is_null() && failed.batch.schema.is_null());
        paimon_error_free(failed.error);
        let address = handle as usize;
        std::thread::scope(|scope| {
            let jobs = (0..4)
                .map(|_| {
                    scope.spawn(move || {
                        let input = batch(vec![("id", Arc::new(Int32Array::from(vec![5])))]);
                        let output = import(route(address as *const paimon_bucket_router, input));
                        assert_eq!(
                            output
                                .column(1)
                                .as_any()
                                .downcast_ref::<Int32Array>()
                                .unwrap()
                                .value(0),
                            96
                        );
                    })
                })
                .collect::<Vec<_>>();
            for job in jobs {
                job.join().unwrap();
            }
        });
        paimon_bucket_router_free(handle);
    }
}
