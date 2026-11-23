// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use arrow_array::{types::Float32Type, ArrayRef, Int32Array, ListArray, RecordBatch};
use paimon::catalog::{Catalog, FileSystemCatalog, Identifier};
use paimon::spec::{
    ArrayType, BigIntType, DataEvolutionIndexSourceMeta, DataType, Datum, FloatType, IndexFileMeta,
    IndexManifest, IntType, Predicate, PredicateBuilder, Schema, SchemaChange, VarCharType,
};
use paimon::table::{SearchScanSplit, Table};
use paimon::Options;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

#[path = "../testdata/centroid/mod.rs"]
mod java_fixture;

fn catalog(warehouse: &Path) -> FileSystemCatalog {
    let mut options = Options::new();
    options.set("warehouse", warehouse.to_str().unwrap());
    FileSystemCatalog::new(options).unwrap()
}

async fn index_files(table: &Table, column: &str) -> Vec<IndexFileMeta> {
    let field = table
        .schema()
        .fields()
        .iter()
        .find(|f| f.name() == column)
        .unwrap();
    let manager = table.snapshot_manager();
    let snapshot = manager.get_latest_snapshot().await.unwrap().unwrap();
    IndexManifest::read(
        table.file_io(),
        &manager.manifest_path(snapshot.index_manifest().unwrap()),
    )
    .await
    .unwrap()
    .into_iter()
    .filter_map(|entry| {
        let file = entry.index_file;
        (file.global_index_meta.as_ref()?.index_field_id == field.id()).then_some(file)
    })
    .collect()
}

fn descriptor(file: &IndexFileMeta) -> &[u8] {
    let bytes = file
        .global_index_meta
        .as_ref()
        .unwrap()
        .index_meta
        .as_deref()
        .unwrap();
    assert_eq!(&bytes[..8], b"PAIMCENT");
    assert_eq!(bytes[8], 1);
    bytes
}

async fn search(
    table: &Table,
    column: &str,
    filter: Option<Predicate>,
    candidates_factor: usize,
) -> Vec<u64> {
    let mut query = table.new_vector_search_builder();
    query
        .with_vector_column(column)
        .with_query_vector(vec![1.0, 0.0])
        .with_limit(1)
        .with_options(HashMap::from([
            ("centroid.nprobe".into(), "2".into()),
            ("ivf.nprobe".into(), "64".into()),
            (
                "vector-index.post-filter.candidates-factor".into(),
                candidates_factor.to_string(),
            ),
        ]));
    if let Some(filter) = filter {
        query.with_filter(filter);
    }
    let plan = query.new_scan().unwrap().plan().await.unwrap();
    assert!(plan
        .splits()
        .iter()
        .all(|split| !matches!(split, SearchScanSplit::Raw { .. })));
    query
        .new_read()
        .unwrap()
        .read(plan)
        .await
        .unwrap()
        .row_ids()
        .unwrap()
        .row_ids
        .clone()
}

#[tokio::test]
async fn reads_java_centroid_manifest_and_single_bitmap_across_schema_evolution() {
    let fixture = java_fixture::unpack();
    let catalog = catalog(&fixture.path().join("warehouse"));
    let identifier = Identifier::new("test", "centroid_bitmap");
    let table = catalog.get_table(&identifier).await.unwrap();
    let files = index_files(&table, "v").await;
    assert_eq!(files.len(), 5);
    let empty_router = files
        .iter()
        .find(|file| descriptor(file)[9] == b'R' && file.file_size == 0)
        .unwrap();
    assert_eq!(&descriptor(empty_router)[10..], b"\0\0\0\0l2");
    let empty_shard = files
        .iter()
        .find(|file| descriptor(file)[9] == b'S' && file.row_count == 0)
        .unwrap();
    assert_eq!(descriptor(empty_shard).len(), 14);
    assert_eq!(empty_shard.file_size, 8);
    let shard = files
        .iter()
        .find(|file| descriptor(file)[9] == b'S' && file.row_count > 0)
        .unwrap();
    assert_eq!(&descriptor(shard)[14..], &1i32.to_be_bytes());
    let bitmap = files
        .iter()
        .find(|file| descriptor(file)[9] == b'B')
        .unwrap();
    assert_eq!(descriptor(bitmap).len(), 14);
    assert_eq!(&descriptor(bitmap)[10..14], &descriptor(shard)[10..14]);
    let bitmap_meta = bitmap.global_index_meta.as_ref().unwrap();
    assert_eq!(bitmap_meta.extra_field_ids.as_deref(), Some([2].as_slice()));
    let source =
        DataEvolutionIndexSourceMeta::deserialize(bitmap_meta.source_meta.as_deref().unwrap())
            .unwrap();
    assert_eq!(source.source_schema_id(), 1);
    assert_eq!(source.source_schema_id(), table.schema().id());

    // With one candidate, post-filtering the unfiltered hit cannot pass either predicate.
    assert_eq!(search(&table, "v", None, 1).await, [0]);
    let predicates = PredicateBuilder::new(table.schema().fields());
    assert_eq!(
        search(
            &table,
            "v",
            Some(predicates.equal("status", Datum::Int(9)).unwrap()),
            1,
        )
        .await,
        [1]
    );
    assert_eq!(
        search(&table, "v", Some(predicates.is_null("status").unwrap()), 1).await,
        [2]
    );

    catalog
        .alter_table(
            &identifier,
            vec![SchemaChange::update_column_type(
                "status".into(),
                DataType::BigInt(BigIntType::new()),
            )],
            false,
        )
        .await
        .unwrap();
    let evolved = catalog.get_table(&identifier).await.unwrap();
    assert!(evolved.schema().id() > source.source_schema_id());
    table
        .file_io()
        .delete_file(&format!(
            "{}/index/{}",
            table.location().trim_end_matches('/'),
            bitmap.file_name
        ))
        .await
        .unwrap();
    let filter = PredicateBuilder::new(evolved.schema().fields())
        .equal("status", Datum::Long(9))
        .unwrap();
    assert_eq!(search(&evolved, "v", Some(filter), 8).await, [1]);
    assert_eq!(
        evolved
            .new_global_index_drop_builder()
            .with_index_column("v")
            .with_index_type("ivf-flat")
            .execute()
            .await
            .unwrap(),
        5
    );
}

#[tokio::test]
async fn reads_java_centroid_metrics_and_ivf_pq_geometry() {
    let fixture = java_fixture::unpack();
    let table = catalog(&fixture.path().join("warehouse"))
        .get_table(&Identifier::new("test", "centroid_metrics"))
        .await
        .unwrap();
    for (column, metric, metric_code, expected) in [
        ("pq", b"l2".as_slice(), 0u32, 0),
        ("cos", b"cosine".as_slice(), 2, 0),
        ("ip", b"inner_product".as_slice(), 1, 1),
    ] {
        let files = index_files(&table, column).await;
        let router = files
            .iter()
            .find(|file| descriptor(file)[9] == b'R')
            .unwrap();
        assert_eq!(&descriptor(router)[14..], metric);
        let native = table
            .file_io()
            .new_input(&format!(
                "{}/index/{}",
                table.location().trim_end_matches('/'),
                router.file_name
            ))
            .unwrap()
            .read()
            .await
            .unwrap();
        assert_eq!(&native[..8], b"PVROUT01");
        assert_eq!(&native[16..20], &metric_code.to_le_bytes());
        let mut populated = 0;
        for shard in files
            .iter()
            .filter(|file| descriptor(file)[9] == b'S' && file.row_count > 0)
        {
            populated += 1;
            if column == "pq" {
                assert_eq!(&descriptor(shard)[14..], &64i32.to_be_bytes());
            } else {
                assert_eq!(descriptor(shard).len(), 14);
            }
        }
        assert!(populated > 0);
        assert_eq!(search(&table, column, None, 8).await, [expected]);
    }
}

fn vectors(rows: impl IntoIterator<Item = Option<[f32; 2]>>) -> ArrayRef {
    Arc::new(ListArray::from_iter_primitive::<Float32Type, _, _>(
        rows.into_iter()
            .map(|row| row.map(|v| v.into_iter().map(Some))),
    ))
}

async fn append(table: &Table, commit_user: &str, batch: RecordBatch) {
    let builder = table
        .new_write_builder()
        .with_commit_user(commit_user)
        .unwrap();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    builder
        .new_commit()
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();
}

async fn build(
    table: &Table,
    column: &str,
    kind: &str,
    metric: &str,
    nlist: &str,
    extra: &[(&str, &str)],
) {
    let mut options = HashMap::from([
        (format!("{kind}.sharding"), "centroid".into()),
        (format!("{kind}.dimension"), "2".into()),
        (format!("{kind}.metric"), metric.into()),
        (
            format!("{kind}.centroid.nlist"),
            if column == "pq" { "1" } else { "2" }.into(),
        ),
        (format!("{kind}.nlist"), nlist.into()),
        (format!("{kind}.train.sample-ratio"), "1".into()),
    ]);
    options.extend(
        extra
            .iter()
            .map(|(key, value)| ((*key).into(), (*value).into())),
    );
    assert!(
        table
            .new_global_index_build_builder()
            .with_index_column(column)
            .with_index_type(kind)
            .with_options(options)
            .execute()
            .await
            .unwrap()
            > 0
    );
}

#[tokio::test]
#[ignore = "writes the companion Java test warehouse"]
async fn write_rust_centroid_warehouse() {
    let directory = std::env::var("PAIMON_RUST_CENTROID_FIXTURE").unwrap();
    let catalog = catalog(Path::new(&directory));
    catalog
        .create_database("test", true, HashMap::new())
        .await
        .unwrap();
    let options = || {
        Schema::builder()
            .option("bucket", "-1")
            .option("row-tracking.enabled", "true")
            .option("data-evolution.enabled", "true")
            .option("file.format", "parquet")
            .option("write-only", "true")
            .option("vector-index.search-mode", "detail")
            .option("global-index.column-update-action", "THROW_ERROR")
    };
    let integer = || DataType::Int(IntType::new());
    let vector = || DataType::Array(ArrayType::new(DataType::Float(FloatType::new())));

    let bitmap = Identifier::new("test", "centroid_bitmap");
    catalog
        .create_table(
            &bitmap,
            options()
                .column("id", integer())
                .column("v", vector())
                .column("status", integer())
                .column("p", integer())
                .partition_keys(["p"])
                .build()
                .unwrap(),
            false,
        )
        .await
        .unwrap();
    catalog
        .alter_table(
            &bitmap,
            vec![SchemaChange::add_column(
                "note".into(),
                DataType::VarChar(VarCharType::string_type()),
            )],
            false,
        )
        .await
        .unwrap();
    let bitmap = catalog.get_table(&bitmap).await.unwrap();
    append(
        &bitmap,
        "populated",
        RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int32Array::from(vec![0, 1, 2, 3])) as ArrayRef,
            ),
            ("v", vectors([Some([1.0, 0.0]); 4])),
            (
                "status",
                Arc::new(Int32Array::from(vec![Some(0), Some(9), None, Some(0)])) as ArrayRef,
            ),
            ("p", Arc::new(Int32Array::from(vec![0; 4])) as ArrayRef),
            (
                "note",
                Arc::new(arrow_array::StringArray::from(vec![None::<&str>; 4])) as ArrayRef,
            ),
        ])
        .unwrap(),
    )
    .await;
    append(
        &bitmap,
        "all-null",
        RecordBatch::try_from_iter([
            ("id", Arc::new(Int32Array::from(vec![4])) as ArrayRef),
            ("v", vectors([None])),
            ("status", Arc::new(Int32Array::from(vec![9])) as ArrayRef),
            ("p", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
            (
                "note",
                Arc::new(arrow_array::StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
        ])
        .unwrap(),
    )
    .await;
    build(
        &bitmap,
        "v",
        "ivf-flat",
        "l2",
        "1",
        &[("extra_fields", r#"{"status":"bitmap"}"#)],
    )
    .await;

    let metrics = Identifier::new("test", "centroid_metrics");
    catalog
        .create_table(
            &metrics,
            options()
                .column("id", integer())
                .column("pq", vector())
                .column("cos", vector())
                .column("ip", vector())
                .build()
                .unwrap(),
            false,
        )
        .await
        .unwrap();
    let metrics = catalog.get_table(&metrics).await.unwrap();
    let values = vectors((0..512).map(|i| {
        Some(match i {
            0 => [1.0, 0.0],
            1 => [10.0, 0.1],
            _ => [0.0, 100.0 + i as f32],
        })
    }));
    append(
        &metrics,
        "vectors",
        RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int32Array::from_iter_values(0..512)) as ArrayRef,
            ),
            ("pq", Arc::clone(&values)),
            ("cos", Arc::clone(&values)),
            ("ip", values),
        ])
        .unwrap(),
    )
    .await;
    build(
        &metrics,
        "pq",
        "ivf-pq",
        "l2",
        "64",
        &[("ivf-pq.pq.m", "1")],
    )
    .await;
    build(&metrics, "cos", "ivf-flat", "cosine", "1", &[]).await;
    build(&metrics, "ip", "ivf-flat", "inner_product", "1", &[]).await;
}
