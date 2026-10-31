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
use crate::spec::{
    DataType, Datum, FileKind, IndexManifestEntry, IntType, PredicateBuilder,
    Schema as TableSchema, SchemaChange, TableSchema as ResolvedSchema, VarCharType,
};
use crate::table::global_index_scanner::GlobalIndexScanner;
use arrow_array::{ArrayRef, Int32Array, StringArray};

#[tokio::test]
async fn masked_sources_preserve_dense_ordinals_across_files_and_single_row_batches() {
    use crate::spec::{ArrayType, BinaryRow, FloatType};
    use arrow_array::{
        builder::{Float32Builder, ListBuilder},
        Array, Float32Array, ListArray,
    };
    let schema = TableSchema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "embedding",
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        )
        .primary_key(["id"])
        .option("bucket", "1")
        .option("write-only", "true")
        .option("read.batch-size", "1")
        .build()
        .unwrap();
    let (table, counter) =
        crate::table::maintenance_test_utils::table(ResolvedSchema::new(0, &schema)).await;
    let write = table.new_write_builder();
    let mut files = Vec::new();
    for values in [vec![2, 3, 4], vec![10, 11]] {
        let mut vectors = ListBuilder::new(Float32Builder::new()).with_field(Arc::new(Field::new(
            "element",
            ArrowType::Float32,
            true,
        )));
        for &value in &values {
            vectors.values().append_slice(&[value as f32, 0.]);
            vectors.append(true);
        }
        let batch = RecordBatch::try_from_iter([
            ("id", Arc::new(Int32Array::from(values)) as ArrayRef),
            ("embedding", Arc::new(vectors.finish()) as ArrayRef),
        ])
        .unwrap();
        let mut writer = write.new_write().unwrap();
        writer.write_arrow_batch(&batch).await.unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].new_files.len(), 1);
        files.extend(messages[0].new_files.clone());
        write.new_commit().commit(messages).await.unwrap();
    }
    let path = format!("{}/bucket-0", table.data_file_location());
    let snapshot = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    let split = DataSplit::builder()
        .with_snapshot(snapshot.id())
        .with_partition(BinaryRow::new(0))
        .with_bucket(0)
        .with_total_buckets(1)
        .with_bucket_path(path.clone())
        .with_data_files(files.clone())
        .build()
        .unwrap();
    let field = table.schema().fields()[1].clone();
    for read_size in [1, 8] {
        let table = table.copy_with_options(HashMap::from([(
            "read.batch-size".into(),
            read_size.to_string(),
        )]));
        for (first, second, expected) in [
            (
                vec![],
                vec![],
                vec![Some(2.), Some(3.), Some(4.), Some(10.), Some(11.)],
            ),
            (
                vec![0],
                vec![],
                vec![None, Some(3.), Some(4.), Some(10.), Some(11.)],
            ),
            (
                vec![1],
                vec![1],
                vec![Some(2.), None, Some(4.), Some(10.), None],
            ),
            (vec![0, 1, 2], vec![0, 1], vec![None; 5]),
        ] {
            let masks = [(&files[0], first), (&files[1], second)]
                .into_iter()
                .filter(|(_, positions)| !positions.is_empty())
                .map(|(file, positions)| (file.file_name.clone(), positions.into_iter().collect()))
                .collect::<HashMap<String, RoaringBitmap>>();
            counter.watch(
                files
                    .iter()
                    .map(|file| format!("{path}/{}", file.file_name))
                    .collect(),
            );
            let output = batches(
                table.clone(),
                split.clone(),
                files.clone(),
                field.clone(),
                Arc::new(masks),
                None,
            )
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
            let mut actual = Vec::new();
            for batch in output {
                assert!(
                    batch.num_rows() <= read_size,
                    "NULL gaps must obey the read batch bound"
                );
                let positions = batch
                    .column_by_name(ROW_ID_FIELD_NAME)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let vectors = batch
                    .column_by_name("embedding")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    let value = (!vectors.is_null(row)).then(|| {
                        let vector = vectors.value(row);
                        vector
                            .as_any()
                            .downcast_ref::<Float32Array>()
                            .unwrap()
                            .value(0)
                    });
                    actual.push((positions.value(row), value));
                }
            }
            assert_eq!(
                actual,
                expected
                    .iter()
                    .enumerate()
                    .map(|(row, &value)| (row as i64, value))
                    .collect::<Vec<_>>()
            );
            if expected.iter().all(Option::is_none) {
                assert!(
                    counter.counts().values().all(|&opens| opens == 0),
                    "fully deleted sources need no data I/O"
                );
            }
        }
    }
}

#[tokio::test]
async fn scalar_source_projection_does_not_decode_unrelated_evolved_columns() {
    let schema = TableSchema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .column("text", DataType::VarChar(VarCharType::string_type()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("write-only", "true")
        .option("index-file-in-data-file-dir", "false")
        .build()
        .unwrap();
    let (table, _) =
        crate::table::maintenance_test_utils::table(ResolvedSchema::new(0, &schema)).await;
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
        (
            "value",
            Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef,
        ),
        (
            "text",
            Arc::new(StringArray::from(vec!["not-a-number", "also-not-a-number"])) as ArrayRef,
        ),
    ])
    .unwrap();
    let write = table.new_write_builder();
    let mut writer = write.new_write().unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    write
        .new_commit()
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();

    // Use a real compacted KV source, rather than changing an L0 file's metadata.
    let compact = table.new_compact_builder();
    let plan = compact.new_planner().unwrap().plan().await.unwrap();
    let result = compact
        .new_executor()
        .unwrap()
        .execute(&plan.tasks()[0])
        .await
        .unwrap();
    compact
        .new_commit()
        .unwrap()
        .commit_compact(vec![result])
        .await
        .unwrap();
    let source_plan = table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan()
        .await
        .unwrap();
    assert_eq!(source_plan.splits().len(), 1);
    let split = source_plan.splits()[0].clone();
    let files = split.data_files().to_vec();
    assert_eq!(files.len(), 1);
    assert!(crate::spec::should_read_pk_index_source(&files[0]));

    // This legal explicit type change cannot decode the old text values as INT.
    // Building an index on the unchanged value column must not perform that cast.
    let evolved = table
        .schema()
        .apply_changes(vec![
            SchemaChange::update_column_type("text".into(), DataType::Int(IntType::new())),
            SchemaChange::set_option("pk-btree.index.columns".into(), "value".into()),
        ])
        .unwrap();
    table
        .file_io()
        .new_output(&table.schema_manager().schema_path(evolved.id()))
        .unwrap()
        .write(serde_json::to_vec(&evolved).unwrap().into())
        .await
        .unwrap();
    let current = table.copy_with_resolved_schema(evolved, "main").unwrap();
    crate::table::compact::primary_key::new_reader(&current, None)
        .unwrap()
        .read_physical_file(split.clone(), files[0].clone())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .expect_err("the full-column control must decode the invalid text-to-INT cast");

    let definition = crate::table::pk_index_options::definitions(current.schema())
        .unwrap()
        .remove(0);
    let source_batches = batches(
        current.clone(),
        split.clone(),
        files.clone(),
        definition.field.clone(),
        Arc::default(),
        None,
    )
    .try_collect::<Vec<_>>()
    .await
    .unwrap();
    let mut positions = Vec::new();
    for batch in source_batches {
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec!["value", ROW_ID_FIELD_NAME]
        );
        let values = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let ordinals = batch
            .column_by_name(ROW_ID_FIELD_NAME)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        positions.extend((0..batch.num_rows()).map(|row| (values.value(row), ordinals.value(row))));
    }
    assert_eq!(positions, vec![(10, 0), (20, 1)]);

    let payload = build(&current, &split, &files, &definition, Arc::default(), None)
        .await
        .unwrap()
        .unwrap();
    let entry = IndexManifestEntry {
        kind: FileKind::Add,
        partition: split.partition().to_serialized_bytes(),
        bucket: split.bucket(),
        index_file: payload,
        version: 1,
    };
    let scanner = GlobalIndexScanner::create_with_fm_options(
        current.file_io(),
        current.location(),
        1,
        i64::MAX,
        128 * 1024 * 1024,
        i64::MAX,
        &[entry],
        current.schema().fields(),
        crate::fm_index::FMReadOptions::default(),
    )
    .unwrap()
    .unwrap();
    let predicate = PredicateBuilder::new(current.schema().fields())
        .equal("value", Datum::Int(20))
        .unwrap();
    assert_eq!(
        scanner.matching_ranges(&predicate).await.unwrap(),
        Some(vec![RowRange::new(1, 1)])
    );
}

#[tokio::test]
async fn vector_source_deletions_skip_schema_casts_without_renumbering_live_rows() {
    use crate::spec::{ArrayType, FloatType, PrimaryKeyIndexSourceMeta};
    use crate::vector_search::{GlobalIndexIOMeta, VectorSearch};
    use crate::vindex::reader::VindexVectorGlobalIndexReader;
    use arrow_array::{
        builder::ListBuilder, builder::StringBuilder, Array, Float32Array, ListArray,
    };
    use std::io::Cursor;

    let schema = TableSchema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "embedding",
            DataType::Array(ArrayType::new(
                DataType::VarChar(VarCharType::string_type()),
            )),
        )
        .primary_key(["id"])
        .option("bucket", "1")
        .option("write-only", "true")
        .option("deletion-vectors.enabled", "true")
        .option("deletion-vectors.merge-on-read", "false")
        .option("index-file-in-data-file-dir", "false")
        .build()
        .unwrap();
    let (table, _) =
        crate::table::maintenance_test_utils::table(ResolvedSchema::new(0, &schema)).await;
    let mut vectors = ListBuilder::new(StringBuilder::new()).with_field(Arc::new(
        arrow_schema::Field::new("element", ArrowType::Utf8, true),
    ));
    for vector in [["not-a-float", "0"], ["1", "0"]] {
        vectors.values().append_value(vector[0]);
        vectors.values().append_value(vector[1]);
        vectors.append(true);
    }
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
        ("embedding", Arc::new(vectors.finish()) as ArrayRef),
    ])
    .unwrap();
    let write = table.new_write_builder();
    let mut writer = write.new_write().unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    write
        .new_commit()
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();
    let compact = table.new_compact_builder();
    let plan = compact.new_planner().unwrap().plan().await.unwrap();
    let result = compact
        .new_auto_executor()
        .unwrap()
        .execute(&plan.tasks()[0])
        .await
        .unwrap();
    compact
        .new_commit()
        .unwrap()
        .commit_compact(vec![result])
        .await
        .unwrap();
    let source_plan = table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan()
        .await
        .unwrap();
    assert_eq!(source_plan.splits().len(), 1);
    let split = source_plan.splits()[0].clone();
    let files = split.data_files().to_vec();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].row_count, 2);
    assert!(crate::spec::should_read_pk_index_source(&files[0]));

    let evolved = table
        .schema()
        .apply_changes(vec![
            SchemaChange::update_column_type(
                "embedding".into(),
                DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
            ),
            SchemaChange::set_option("pk-vector.index.columns".into(), "embedding".into()),
            SchemaChange::set_option(
                "fields.embedding.pk-vector.index.type".into(),
                "ivf-flat".into(),
            ),
            SchemaChange::set_option(
                "fields.embedding.pk-vector.distance.metric".into(),
                "l2".into(),
            ),
            SchemaChange::set_option(
                "fields.embedding.pk-vector.index.options".into(),
                r#"{"dimension":2,"nlist":1}"#.into(),
            ),
        ])
        .unwrap();
    table
        .file_io()
        .new_output(&table.schema_manager().schema_path(evolved.id()))
        .unwrap()
        .write(serde_json::to_vec(&evolved).unwrap().into())
        .await
        .unwrap();
    let current = table.copy_with_resolved_schema(evolved, "main").unwrap();
    let definition = crate::table::pk_index_options::definitions(current.schema())
        .unwrap()
        .remove(0);
    batches(
        current.clone(),
        split.clone(),
        files.clone(),
        definition.field.clone(),
        Arc::default(),
        None,
    )
    .try_collect::<Vec<_>>()
    .await
    .expect_err("an unmasked source must reject the invalid STRING-to-FLOAT element");

    // Deletion positions refer to the old file, before decoding/casting values.
    // The surviving vector must retain ordinal 1 even though native count is 1.
    let deletions = Arc::new(HashMap::from([(
        files[0].file_name.clone(),
        RoaringBitmap::from_iter([0]),
    )]));
    let source_batches = batches(
        current.clone(),
        split.clone(),
        files.clone(),
        definition.field.clone(),
        deletions.clone(),
        None,
    )
    .try_collect::<Vec<_>>()
    .await
    .unwrap();
    let mut rows = Vec::new();
    for batch in source_batches {
        let vectors = batch
            .column_by_name("embedding")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let ordinals = batch
            .column_by_name(ROW_ID_FIELD_NAME)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let vector = (!vectors.is_null(row)).then(|| {
                vectors
                    .value(row)
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            });
            rows.push((ordinals.value(row), vector));
        }
    }
    assert_eq!(rows, vec![(0, None), (1, Some(vec![1., 0.]))]);

    let payload = build(&current, &split, &files, &definition, deletions, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(payload.row_count, 1);
    let meta = payload.global_index_meta.as_ref().unwrap();
    assert_eq!((meta.row_range_start, meta.row_range_end), (0, 1));
    let source = PrimaryKeyIndexSourceMeta::from_global_index_meta(meta).unwrap();
    assert_eq!(source.source_files().len(), 1);
    assert_eq!(source.source_files()[0].row_count(), 2);
    let path = format!("{}/index/{}", current.location(), payload.file_name);
    let bytes = current
        .file_io()
        .new_input(&path)
        .unwrap()
        .read()
        .await
        .unwrap()
        .to_vec();
    let io_meta = GlobalIndexIOMeta::new(
        path,
        payload.file_size as u64,
        meta.index_meta.clone().unwrap(),
    );
    let mut reader = VindexVectorGlobalIndexReader::new(io_meta, HashMap::new());
    let query = VectorSearch::new(vec![1., 0.], 1, "embedding".into()).unwrap();
    let hits = reader
        .visit_vector_search(&query, |_| Ok(Cursor::new(bytes)))
        .unwrap()
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert!(hits.contains_key(&1));
}
