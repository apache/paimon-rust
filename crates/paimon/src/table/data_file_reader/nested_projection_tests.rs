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
use crate::arrow::format::{
    create_format_reader_with_budget, create_format_writer, FormatReadFields, MosaicPrefetchOptions,
};
use crate::arrow::{build_read_arrow_schema, build_target_arrow_schema};
use crate::io::FileIOBuilder;
use crate::spec::stats::BinaryTableStats;
use crate::spec::{ArrayType, BinaryRow, DataFileMeta, IntType, MapType, RowType, VarCharType};
use crate::table::source::DataSplitBuilder;
use arrow_array::UInt32Array;
use arrow_select::concat::concat_batches;
use bytes::Bytes;
use futures::TryStreamExt;
use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};

fn f(id: i32, name: &str, typ: DataType) -> DataField {
    DataField::new(id, name.into(), typ)
}
fn int() -> DataType {
    DataType::Int(IntType::new())
}
fn string() -> DataType {
    DataType::VarChar(VarCharType::string_type())
}
fn row(fields: Vec<DataField>) -> DataType {
    DataType::Row(RowType::new(fields))
}
fn array(element: DataType) -> DataType {
    DataType::Array(ArrayType::new(element))
}
fn map(value: DataType) -> DataType {
    DataType::Map(MapType::new(string(), value))
}
fn leaves(base: i32, projected: bool) -> DataType {
    if projected {
        row(vec![
            f(
                base + 2,
                "renamed_tail",
                DataType::BigInt(crate::spec::BigIntType::new()),
            ),
            f(base + 1, "keep", int()),
        ])
    } else {
        row(vec![
            f(base + 1, "keep", int()),
            f(base + 2, "tail", int()),
            f(base + 3, "unused", string()),
        ])
    }
}
fn fields(projected: bool) -> Vec<DataField> {
    vec![
        f(
            10,
            "profile",
            row(vec![f(11, "detail", leaves(11, projected))]),
        ),
        f(20, "items", array(leaves(20, projected))),
        f(30, "groups", array(array(leaves(30, projected)))),
        f(40, "attrs", map(leaves(40, projected))),
        f(
            50,
            "complex",
            row(vec![f(51, "lookup", map(array(leaves(51, projected))))]),
        ),
        f(60, "array_maps", array(map(leaves(60, projected)))),
    ]
}
fn input() -> RecordBatch {
    let leaf = serde_json::json!({"keep": 7, "tail": 9, "unused": "discard".repeat(2048)});
    let rows = [
        serde_json::json!({"profile":{"detail":leaf}, "items":[leaf,null,leaf], "groups":[[leaf,null],[],null], "attrs":{"a":leaf,"b":null}, "complex":{"lookup":{"x":[leaf,null],"y":[]}},"array_maps":[{"k":leaf},null,{}]}),
        serde_json::json!({"profile":null,"items":null,"groups":null,"attrs":null,"complex":null,"array_maps":null}),
        serde_json::json!({"profile":{"detail":null},"items":[],"groups":[],"attrs":{},"complex":{"lookup":{}},"array_maps":[]}),
        serde_json::json!({"profile":{"detail":{"keep":null,"tail":3}},"items":[{"keep":null,"tail":4}],"groups":[[null]],"attrs":{"z":{"keep":null,"tail":5}},"complex":{"lookup":null},"array_maps":[{"z":null}]}),
        serde_json::json!({"profile":{"detail":{"unused":"present"}},"items":[{"unused":"present"}],"groups":[[{"unused":"present"}]],"attrs":{"k":{"unused":"present"}},"complex":{"lookup":{"k":[{"unused":"present"}]}},"array_maps":[{"k":{"unused":"present"}}]}),
    ];
    let json = rows
        .iter()
        .map(|row| row.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    arrow_json::ReaderBuilder::new(build_target_arrow_schema(&fields(false)).unwrap())
        .build(Cursor::new(json))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
}
fn expected(input: &RecordBatch, source: &[DataField], target: &[DataField]) -> RecordBatch {
    let columns = target
        .iter()
        .map(|target| {
            let index = source
                .iter()
                .position(|source| source.id() == target.id())
                .unwrap();
            crate::arrow::nested_evolution::evolve_field(
                input.column(index),
                &source[index],
                target,
            )
            .unwrap()
        })
        .collect();
    RecordBatch::try_new(build_read_arrow_schema(target).unwrap(), columns).unwrap()
}

// Avro's native MAP has unordered entries. Compare mappings recursively while
// retaining ARRAY order, ROW nullness and empty-versus-null containers.
fn json_rows(batch: &RecordBatch) -> serde_json::Value {
    let mut writer = arrow_json::ArrayWriter::new(Vec::new());
    writer.write_batches(&[batch]).unwrap();
    writer.finish().unwrap();
    serde_json::from_slice(&writer.into_inner()).unwrap()
}
fn meta(name: &str, size: u64, rows: usize) -> DataFileMeta {
    DataFileMeta {
        file_name: name.into(),
        file_size: size as i64,
        row_count: rows as i64,
        min_key: vec![],
        max_key: vec![],
        key_stats: BinaryTableStats::empty(),
        value_stats: BinaryTableStats::empty(),
        min_sequence_number: 0,
        max_sequence_number: 0,
        schema_id: 1,
        level: 0,
        extra_files: vec![],
        creation_time: None,
        delete_row_count: None,
        embedded_index: None,
        file_source: None,
        value_stats_cols: None,
        external_path: None,
        first_row_id: None,
        write_cols: None,
        column_max_sequence_numbers: None,
    }
}

#[tokio::test]
async fn nested_projection_roundtrips_parquet_and_fallback_formats() {
    let source = fields(false);
    let input = input();
    let target = fields(true).into_iter().rev().collect::<Vec<_>>();
    for extension in ["parquet", "orc", "avro", "row", "json"]
        .into_iter()
        .chain(cfg!(feature = "vortex").then_some("vortex"))
    {
        let source = if extension == "vortex" {
            source[..3].to_vec()
        } else {
            source.clone()
        };
        let input = if extension == "vortex" {
            input.project(&[0, 1, 2]).unwrap()
        } else {
            input.clone()
        };
        let target = target
            .iter()
            .filter(|field| source.iter().any(|source| source.id() == field.id()))
            .cloned()
            .collect::<Vec<_>>();
        let expected = expected(&input, &source, &target);
        assert_eq!(
            json_rows(&expected)[0]["profile"]["detail"],
            serde_json::json!({"renamed_tail":9,"keep":7})
        );
        let io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = format!("memory:/nested_projection_{extension}");
        let bucket = format!("{table_path}/bucket-0");
        let name = format!("part.{extension}");
        let output = io.new_output(&format!("{bucket}/{name}")).unwrap();
        let size = if extension == "orc" {
            // orc-rust's writer cannot encode nested types. This fixture was
            // written by Apache ORC through PyArrow; tests need no Python.
            let bytes = include_bytes!("../../../testdata/nested_projection/nested.orc");
            output.write(Bytes::from_static(bytes)).await.unwrap();
            bytes.len() as u64
        } else {
            let mut writer = create_format_writer(
                &output,
                input.schema(),
                "none",
                1,
                None,
                Some(&source),
                None,
            )
            .await
            .unwrap();
            writer.write(&input).await.unwrap();
            writer.close().await.unwrap().file_size
        };
        let file = meta(&name, size, input.num_rows());
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket)
            .with_total_buckets(1)
            .with_data_files(vec![file.clone()])
            .build()
            .unwrap();
        let reader = DataFileReader::new(
            io.clone(),
            SchemaManager::new(io, table_path),
            1,
            source.clone(),
            |fields| Ok(fields.to_vec()),
            target.clone(),
            vec![],
        );
        for schema in [None, Some(source.clone())] {
            let batches = reader
                .read_single_file_stream(&split, file.clone(), schema.clone(), None, None)
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap_or_else(|err| panic!("{extension}: {err}"));
            let actual = concat_batches(&expected.schema(), &batches).unwrap();
            assert_eq!(actual.schema(), expected.schema());
            assert_eq!(
                json_rows(&actual),
                json_rows(&expected),
                "{extension}, explicit schema: {}",
                schema.is_some()
            );
            if extension == "json" {
                continue;
            } // line-oriented formats have no positional reads
            let batches = reader
                .read_single_file_stream_local(&split, file.clone(), schema, None, vec![0, 2])
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap_or_else(|err| panic!("{extension}: {err}"));
            let selected =
                arrow_select::take::take_record_batch(&expected, &UInt32Array::from(vec![0, 2]))
                    .unwrap();
            assert_eq!(
                json_rows(&concat_batches(&selected.schema(), &batches).unwrap()),
                json_rows(&selected),
                "local {extension}"
            );
        }
        let missing = row(vec![f(100, "added", int())]);
        let requested = vec![
            f(20, "items", array(missing.clone())),
            f(40, "attrs", map(missing)),
        ]
        .into_iter()
        .filter(|field| source.iter().any(|source| source.id() == field.id()))
        .collect::<Vec<_>>();
        for nested_enabled in [false, true] {
            let reader = reader
                .clone()
                .with_read_type(requested.clone())
                .with_nested_field_enabled(nested_enabled);
            let batches = reader
                .read_single_file_stream(&split, file.clone(), Some(source.clone()), None, None)
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            let mut wanted = self::expected(&input, &source, &requested);
            if !nested_enabled {
                wanted = RecordBatch::try_new(
                    wanted.schema(),
                    wanted
                        .schema()
                        .fields()
                        .iter()
                        .map(|field| {
                            arrow_array::new_null_array(field.data_type(), input.num_rows())
                        })
                        .collect(),
                )
                .unwrap();
            }
            let actual = concat_batches(&wanted.schema(), &batches).unwrap();
            assert_eq!(
                json_rows(&actual),
                json_rows(&wanted),
                "{extension}: all-missing, nested={nested_enabled}"
            );
        }
    }
}

#[derive(Clone)]
struct CountingRead {
    data: Bytes,
    bytes: Arc<AtomicU64>,
}
#[async_trait::async_trait]
impl FileRead for CountingRead {
    async fn read(&self, range: std::ops::Range<u64>) -> crate::Result<Bytes> {
        self.bytes
            .fetch_add(range.end - range.start, Ordering::Relaxed);
        Ok(self.data.slice(range.start as usize..range.end as usize))
    }
}

#[tokio::test]
async fn nested_projection_reduces_parquet_data_io_in_every_container() {
    let source = fields(false);
    let original = input();
    let input = concat_batches(&original.schema(), (0..64).map(|_| &original)).unwrap();
    let io = FileIOBuilder::new("memory").build().unwrap();
    let output = io
        .new_output("memory:/nested_projection_io.parquet")
        .unwrap();
    let options = HashMap::from([("parquet.enable.dictionary".into(), "false".into())]);
    let mut writer = create_format_writer(
        &output,
        input.schema(),
        "none",
        1,
        None,
        Some(&source),
        Some(&options),
    )
    .await
    .unwrap();
    writer.write(&input).await.unwrap();
    let size = writer.close().await.unwrap().file_size;
    let data = io
        .new_input(output.location())
        .unwrap()
        .read()
        .await
        .unwrap();
    for (full, projected) in source.iter().zip(fields(true)) {
        let mut read_bytes = vec![];
        for requested in [full, &projected] {
            let physical =
                read_data_fields(&source, std::slice::from_ref(requested), false).unwrap();
            let configured = create_format_reader_with_budget(
                output.location(),
                false,
                FormatReadFields {
                    data_schema: &source,
                    projected: &physical,
                },
                &HashMap::new(),
                None,
                1,
                MosaicPrefetchOptions::default(),
            )
            .unwrap();
            let count = Arc::new(AtomicU64::new(0));
            let stream = configured
                .reader
                .read_batch_stream(
                    Box::new(CountingRead {
                        data: data.clone(),
                        bytes: count.clone(),
                    }),
                    size,
                    &configured.read_fields,
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();
            count.store(0, Ordering::Relaxed);
            let batches = stream.try_collect::<Vec<_>>().await.unwrap();
            let actual = concat_batches(&batches[0].schema(), &batches).unwrap();
            let wanted = expected(&input, &source, std::slice::from_ref(requested));
            let normalized = expected(
                &actual,
                &configured.read_fields,
                std::slice::from_ref(requested),
            );
            assert_eq!(normalized, wanted, "{}", requested.name());
            read_bytes.push(count.load(Ordering::Relaxed));
        }
        assert!(
            read_bytes[1] * 2 < read_bytes[0],
            "{}: projected {} bytes, full {} bytes",
            full.name(),
            read_bytes[1],
            read_bytes[0]
        );
    }
}

#[test]
fn missing_container_leaves_follow_java_nested_field_mode() {
    let source = row(vec![f(1, "old", int())]);
    let requested = row(vec![f(2, "added", int())]);
    for (source, requested) in [
        (array(source.clone()), array(requested.clone())),
        (map(source.clone()), map(requested.clone())),
        (
            DataType::Map(MapType::new(source.clone(), int())),
            DataType::Map(MapType::new(requested.clone(), int())),
        ),
    ] {
        // FormatReaderMapping returns null for the whole type when a ROW has
        // no surviving children outside nested-field mode. In nested-field
        // mode it keeps a physical anchor, recursively through containers.
        assert_eq!(prune_data_type(&requested, &source, false).unwrap(), None);
        assert_eq!(
            prune_data_type(&requested, &source, true).unwrap(),
            Some(source)
        );
    }
}

#[tokio::test]
async fn shared_map_value_nested_projection_prunes_direct_and_overflow_sources() {
    let source = vec![fields(false)[3].clone()];
    let target = vec![fields(true)[3].clone()];
    let original = input();
    let input = RecordBatch::try_new(
        build_target_arrow_schema(&source).unwrap(),
        vec![original.column(3).clone()],
    )
    .unwrap();
    let io = FileIOBuilder::new("memory").build().unwrap();
    let output = io
        .new_output("memory:/shared_map_value_nested_projection.parquet")
        .unwrap();
    let options = HashMap::from([
        (
            "fields.attrs.map.storage-layout".into(),
            "shared-shredding".into(),
        ),
        (
            "fields.attrs.map.shared-shredding.max-columns".into(),
            "1".into(),
        ),
    ]);
    let mut writer = create_format_writer(
        &output,
        input.schema(),
        "none",
        1,
        None,
        Some(&source),
        Some(&options),
    )
    .await
    .unwrap();
    writer.write(&input).await.unwrap();
    let size = writer.close().await.unwrap().file_size;
    let physical = read_data_fields(&source, &target, false).unwrap();
    let configured = create_format_reader_with_budget(
        output.location(),
        false,
        FormatReadFields {
            data_schema: &source,
            projected: &physical,
        },
        &HashMap::new(),
        None,
        1,
        MosaicPrefetchOptions::default(),
    )
    .unwrap();
    let batches = configured
        .reader
        .read_batch_stream(
            Box::new(
                io.new_input(output.location())
                    .unwrap()
                    .reader()
                    .await
                    .unwrap(),
            ),
            size,
            &configured.read_fields,
            None,
            None,
            None,
        )
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let decoded = concat_batches(&batches[0].schema(), &batches).unwrap();
    assert_eq!(
        expected(&decoded, &physical, &target),
        expected(&input, &source, &target)
    );
    let values = decoded
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::MapArray>()
        .unwrap()
        .values();
    assert_eq!(
        values
            .as_any()
            .downcast_ref::<arrow_array::StructArray>()
            .unwrap()
            .num_columns(),
        2
    );
}

#[tokio::test]
async fn map_row_key_projection_preserves_entries_even_when_keys_become_equal() {
    use arrow_array::{Int32Array, MapArray, StringArray, StructArray};
    use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow_schema::DataType as ArrowType;
    let map_field = |children| {
        f(
            0,
            "m",
            DataType::Map(MapType::new(
                DataType::Row(RowType::with_nullable(false, children)),
                int(),
            )),
        )
    };
    let source = vec![map_field(vec![
        f(1, "keep", int()),
        f(2, "unused", string()),
    ])];
    let target = vec![map_field(vec![f(1, "keep", int())])];
    let schema = build_target_arrow_schema(&source).unwrap();
    let ArrowType::Map(entries_field, _) = schema.field(0).data_type() else {
        unreachable!()
    };
    let ArrowType::Struct(entry_fields) = entries_field.data_type() else {
        unreachable!()
    };
    let ArrowType::Struct(key_fields) = entry_fields[0].data_type() else {
        unreachable!()
    };
    let keys = StructArray::new(
        key_fields.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 1, 2])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
        None,
    );
    let entries = StructArray::new(
        entry_fields.clone(),
        vec![Arc::new(keys), Arc::new(Int32Array::from(vec![10, 20, 30]))],
        None,
    );
    let maps = MapArray::new(
        entries_field.clone(),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 3])),
        entries,
        Some(NullBuffer::from(vec![true, false, true])),
        false,
    );
    let input = RecordBatch::try_new(schema, vec![Arc::new(maps)]).unwrap();
    let io = FileIOBuilder::new("memory").build().unwrap();
    let output = io
        .new_output("memory:/nested_map_row_keys.parquet")
        .unwrap();
    let mut writer = create_format_writer(
        &output,
        input.schema(),
        "none",
        1,
        None,
        Some(&source),
        None,
    )
    .await
    .unwrap();
    writer.write(&input).await.unwrap();
    let size = writer.close().await.unwrap().file_size;
    let physical = read_data_fields(&source, &target, false).unwrap();
    let configured = create_format_reader_with_budget(
        output.location(),
        false,
        FormatReadFields {
            data_schema: &source,
            projected: &physical,
        },
        &HashMap::new(),
        None,
        1,
        MosaicPrefetchOptions::default(),
    )
    .unwrap();
    let batches = configured
        .reader
        .read_batch_stream(
            Box::new(
                io.new_input(output.location())
                    .unwrap()
                    .reader()
                    .await
                    .unwrap(),
            ),
            size,
            &configured.read_fields,
            None,
            None,
            None,
        )
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let decoded = concat_batches(&batches[0].schema(), &batches).unwrap();
    let maps = decoded
        .column(0)
        .as_any()
        .downcast_ref::<MapArray>()
        .unwrap();
    assert_eq!(
        maps.keys()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .num_columns(),
        1
    );
    assert_eq!(
        expected(&decoded, &physical, &target),
        expected(&input, &source, &target)
    );
}

#[tokio::test]
async fn java_list_layouts_decode_the_projected_row_without_shape_changes() {
    use arrow_array::{Array, Int32Array, ListArray, StructArray};
    let target = vec![f(0, "items", array(row(vec![f(2, "wanted", int())])))];
    for (name, bytes, repeated) in [
        (
            "standard",
            include_bytes!("../../../testdata/nested_projection/standard.parquet").as_slice(),
            false,
        ),
        (
            "array",
            include_bytes!("../../../testdata/nested_projection/array.parquet").as_slice(),
            false,
        ),
        (
            "tuple",
            include_bytes!("../../../testdata/nested_projection/tuple.parquet").as_slice(),
            false,
        ),
        (
            "repeated",
            include_bytes!("../../../testdata/nested_projection/repeated.parquet").as_slice(),
            true,
        ),
    ] {
        let configured = create_format_reader_with_budget(
            "data.parquet",
            false,
            FormatReadFields {
                data_schema: &target,
                projected: &target,
            },
            &HashMap::new(),
            None,
            1,
            MosaicPrefetchOptions::default(),
        )
        .unwrap();
        let batches = configured
            .reader
            .read_batch_stream(
                Box::new(CountingRead {
                    data: Bytes::from_static(bytes),
                    bytes: Arc::new(AtomicU64::new(0)),
                }),
                bytes.len() as u64,
                &target,
                None,
                None,
                None,
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let decoded = concat_batches(&batches[0].schema(), &batches).unwrap();
        let list = decoded
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(list.value_offsets(), &[0, 2, 2, 2], "{name}");
        assert!(list.is_valid(1), "{name}: empty list");
        assert_eq!(list.is_null(2), !repeated, "{name}: null list");
        let elements = list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(elements.column_names(), vec!["wanted"], "{name}");
        assert_eq!(
            elements
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[7, 8],
            "{name}"
        );
    }
}

#[tokio::test]
async fn fallback_does_not_resurrect_omitted_children_inside_retained_rows() {
    let nested = |base, projected| {
        let leaf = |old, new| {
            row(vec![if projected {
                f(new, "added", int())
            } else {
                f(old, "old", int())
            }])
        };
        row(vec![
            f(base + 1, "keep", int()),
            f(base + 2, "inner", leaf(base + 3, base + 4)),
            f(base + 5, "items", array(leaf(base + 6, base + 7))),
            f(base + 8, "attrs", map(leaf(base + 9, base + 10))),
        ])
    };
    let schema_fields = |projected| {
        vec![
            f(0, "p", nested(10, projected)),
            f(100, "arr", array(nested(110, projected))),
            f(200, "m", map(nested(210, projected))),
        ]
    };
    let source = schema_fields(false);
    let requested = schema_fields(true);
    let leaf =
        serde_json::json!({"keep":9,"inner":{"old":7},"items":[{"old":7}],"attrs":{"a":{"old":7}}});
    let json = format!(
        "{}\n{{}}",
        serde_json::json!({"p":leaf,"arr":[leaf,null],"m":{"x":leaf}})
    );
    let input = arrow_json::ReaderBuilder::new(build_target_arrow_schema(&source).unwrap())
        .build(Cursor::new(json))
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    for extension in ["parquet", "avro", "row", "json"] {
        let io = FileIOBuilder::new("memory").build().unwrap();
        let table = format!("memory:/deep_missing_{extension}");
        let bucket = format!("{table}/bucket-0");
        let name = format!("part.{extension}");
        let output = io.new_output(&format!("{bucket}/{name}")).unwrap();
        let mut writer = create_format_writer(
            &output,
            input.schema(),
            "none",
            1,
            None,
            Some(&source),
            None,
        )
        .await
        .unwrap();
        writer.write(&input).await.unwrap();
        let file = meta(
            &name,
            writer.close().await.unwrap().file_size,
            input.num_rows(),
        );
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket)
            .with_total_buckets(1)
            .with_data_files(vec![file.clone()])
            .build()
            .unwrap();
        let reader = DataFileReader::new(
            io.clone(),
            SchemaManager::new(io, table),
            1,
            source.clone(),
            |fields| Ok(fields.to_vec()),
            requested.clone(),
            vec![],
        );
        for enabled in [false, true] {
            let batches = reader
                .clone()
                .with_nested_field_enabled(enabled)
                .read_single_file_stream(&split, file.clone(), Some(source.clone()), None, None)
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            let actual =
                concat_batches(&build_read_arrow_schema(&requested).unwrap(), &batches).unwrap();
            let leaf = if enabled {
                serde_json::json!({"keep":9,"inner":{},"items":[{}],"attrs":{"a":{}}})
            } else {
                serde_json::json!({"keep":9})
            };
            let wanted = serde_json::json!([{"p":leaf,"arr":[leaf,null],"m":{"x":leaf}},{}]);
            assert_eq!(json_rows(&actual), wanted, "{extension}, nested={enabled}");
        }
    }
}
