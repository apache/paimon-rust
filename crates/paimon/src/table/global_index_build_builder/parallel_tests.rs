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

//! Integration coverage for real generic writers using the shared shard runner.

use super::*;
use crate::catalog::Identifier;
use crate::io::FileIOBuilder;
use crate::spec::{ArrayType, DataType, FloatType, IntType, Schema, TableSchema, VarCharType};
use crate::table::TableWrite;
use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowType, Field, Schema as ArrowSchema};
use std::sync::Arc;

fn table(kind: &str, parallelism: usize, external: bool) -> Table {
    let location = format!("memory:/generic-parallel-{kind}-{}", uuid::Uuid::new_v4());
    let mut options = HashMap::from([
        ("row-tracking.enabled".into(), "true".into()),
        ("data-evolution.enabled".into(), "true".into()),
        ("global-index.enabled".into(), "true".into()),
        ("bucket".into(), "-1".into()),
        ("global-index.row-count-per-shard".into(), "4".into()),
        (
            "global-index.build.parallelism".into(),
            parallelism.to_string(),
        ),
        ("vector-index.search-mode".into(), "fast".into()),
        ("full-text-index.search-mode".into(), "fast".into()),
    ]);
    if external {
        options.insert(
            "global-index.external-path".into(),
            format!("{location}-external"),
        );
    }
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("text", DataType::VarChar(VarCharType::string_type()))
        .column(
            "embedding",
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        )
        .options(options)
        .build()
        .unwrap();
    Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("default", "parallel"),
        location,
        TableSchema::new(0, &schema),
        None,
    )
}

async fn append(table: &Table, start: usize, vectors: Vec<Option<Vec<Option<f32>>>>) {
    let rows = vectors.len();
    let mut values = ListBuilder::new(Float32Builder::new());
    for vector in vectors {
        match vector {
            Some(vector) => {
                for value in vector {
                    values.values().append_option(value);
                }
                values.append(true);
            }
            None => values.append(false),
        }
    }
    let values = values.finish();
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", ArrowType::Int32, false),
        Field::new("text", ArrowType::Utf8, true),
        Field::new("embedding", values.data_type().clone(), true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(
            (start..start + rows).map(|i| i as i32).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            (start..start + rows)
                .map(|i| (i % 2 == 1).then_some("paimon index"))
                .collect::<Vec<_>>(),
        )),
        Arc::new(values),
    ];
    let mut write = TableWrite::new(table, "data".into()).unwrap();
    write
        .write_arrow_batch(&RecordBatch::try_new(schema, columns).unwrap())
        .await
        .unwrap();
    TableCommit::new(table.clone(), "data".into())
        .commit(write.prepare_commit().await.unwrap())
        .await
        .unwrap();
}

fn builder<'a>(table: &'a Table, kind: &str) -> GlobalIndexBuildBuilder<'a> {
    let mut builder = table.new_global_index_build_builder();
    builder
        .with_index_column(if kind == "full-text" {
            "text"
        } else {
            "embedding"
        })
        .with_index_type(kind);
    if kind != "full-text" {
        let mut options = HashMap::from([(format!("{kind}.dimension"), "2".into())]);
        if kind != "diskann" {
            options.insert(format!("{kind}.nlist"), "1".into());
        }
        if kind == "ivf-pq" {
            options.insert("ivf-pq.pq.m".into(), "1".into());
        }
        if kind == "diskann" {
            options.insert("diskann.pq.bits".into(), "4".into());
        }
        builder.with_options(options);
    }
    builder
}

fn path(table: &Table, file: &crate::spec::IndexFileMeta) -> String {
    file.external_path
        .clone()
        .unwrap_or_else(|| format!("{}/index/{}", table.location(), file.file_name))
}

#[tokio::test]
async fn parallel_native_writers_match_serial_shards_and_incremental_coverage() {
    let mut kinds = vec!["ivf-flat", "ivf-pq", "ivf-sq", "ivf-rq", "diskann"];
    if cfg!(feature = "fulltext") {
        kinds.push("full-text");
    }
    for kind in kinds {
        for external in [false, true] {
            let serial = table(kind, 1, external);
            let parallel = table(kind, 3, external);
            // The first vector shard is all NULL; subsequent shards have sparse IDs.
            let rows = (0..20)
                .map(|i| (i >= 4 && i % 2 == 1).then_some(vec![Some(1.), Some(0.)]))
                .collect::<Vec<_>>();
            append(&serial, 0, rows.clone()).await;
            append(&parallel, 0, rows).await;
            let control = builder(&serial, kind).build().await.unwrap();
            let messages = builder(&parallel, kind).build().await.unwrap();
            assert_eq!(messages.len(), control.len(), "{kind}");
            assert_eq!(
                parallel
                    .snapshot_manager()
                    .get_latest_snapshot()
                    .await
                    .unwrap()
                    .unwrap()
                    .id(),
                1
            );
            for (actual, expected) in messages.iter().zip(&control) {
                let file = &actual.new_index_files[0];
                let reference = &expected.new_index_files[0];
                assert_eq!(file.row_count, reference.row_count);
                let mut actual_meta = file.global_index_meta.clone().unwrap();
                let mut expected_meta = reference.global_index_meta.clone().unwrap();
                let actual_options: serde_json::Value =
                    serde_json::from_slice(&actual_meta.index_meta.take().unwrap()).unwrap();
                let expected_options: serde_json::Value =
                    serde_json::from_slice(&expected_meta.index_meta.take().unwrap()).unwrap();
                assert_eq!(actual_meta, expected_meta);
                assert_eq!(actual_options, expected_options);
                let actual = parallel
                    .file_io()
                    .new_input(&path(&parallel, file))
                    .unwrap()
                    .read()
                    .await
                    .unwrap();
                let expected = serial
                    .file_io()
                    .new_input(&path(&serial, reference))
                    .unwrap()
                    .read()
                    .await
                    .unwrap();
                // Full-text archives contain generated segment identities; vector files are deterministic.
                if kind != "full-text" {
                    assert_eq!(actual, expected, "{kind}");
                }
            }
            TableCommit::new(parallel.clone(), "indexes".into())
                .commit(messages)
                .await
                .unwrap();
            assert!(builder(&parallel, kind).build().await.unwrap().is_empty());
            append(
                &parallel,
                20,
                vec![
                    None,
                    Some(vec![Some(0.), Some(1.)]),
                    None,
                    Some(vec![Some(1.), Some(1.)]),
                ],
            )
            .await;
            let new = builder(&parallel, kind).build().await.unwrap();
            assert_eq!(new.len(), 1);
            assert_eq!(
                new[0].new_index_files[0]
                    .global_index_meta
                    .as_ref()
                    .unwrap()
                    .row_range_start,
                20
            );
            TableCommit::new(parallel.clone(), "new-indexes".into())
                .commit(new)
                .await
                .unwrap();
            if kind != "full-text" {
                let result = parallel
                    .new_vector_search_builder()
                    .with_vector_column("embedding")
                    .with_query_vector(vec![1., 1.])
                    .with_limit(64)
                    .execute()
                    .await
                    .unwrap();
                let mut ids = result.row_ids().unwrap().row_ids.clone();
                ids.sort_unstable();
                assert_eq!(ids, vec![5, 7, 9, 11, 13, 15, 17, 19, 21, 23], "{kind}");
            }
            #[cfg(feature = "fulltext")]
            if kind == "full-text" {
                let result = parallel
                    .new_full_text_search_builder()
                    .with_text_column("text")
                    .with_query_text("paimon")
                    .with_limit(64)
                    .execute()
                    .await
                    .unwrap();
                let mut ids = result
                    .into_iter()
                    .map(|range| range.from())
                    .collect::<Vec<_>>();
                ids.sort_unstable();
                assert_eq!(ids, (1..24).step_by(2).collect::<Vec<_>>());
            }
        }
    }
}

#[tokio::test]
async fn failed_parallel_shards_clean_private_indexes_and_retain_handed_off_files() {
    for external in [false, true] {
        let table = table("ivf-flat", 3, external);
        append(&table, 0, vec![Some(vec![Some(1.), Some(0.)]); 12]).await;
        // These files are already owned by a caller and must survive a later failure.
        let handed_off = builder(&table, "ivf-sq").build().await.unwrap();
        append(&table, 12, vec![Some(vec![Some(1.), None]); 4]).await;
        let before = table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap()
            .id();
        let error = builder(&table, "ivf-flat").execute().await.unwrap_err();
        assert!(error.to_string().contains("null vector element"));
        assert_eq!(
            table
                .snapshot_manager()
                .get_latest_snapshot()
                .await
                .unwrap()
                .unwrap()
                .id(),
            before
        );
        for message in &handed_off {
            assert!(table
                .file_io()
                .exists(&path(&table, &message.new_index_files[0]))
                .await
                .unwrap());
        }
        let directory = if external {
            format!("{}-external", table.location())
        } else {
            format!("{}/index", table.location())
        };
        let remaining = table.file_io().list_status(&directory).await.unwrap();
        assert_eq!(
            remaining.len(),
            handed_off.len(),
            "private files from in-flight workers leaked"
        );
    }
}

#[tokio::test]
async fn invalid_parallelism_is_rejected_for_every_generic_family_before_snapshot_reads() {
    let mut kinds = vec!["ivf-flat", "ivf-pq", "ivf-sq", "ivf-rq", "diskann"];
    if cfg!(feature = "fulltext") {
        kinds.push("full-text");
    }
    for kind in kinds {
        let table = table(kind, 1, false);
        for value in ["0", "-1", "invalid"] {
            let mut builder = builder(&table, kind);
            let mut options = builder.options.clone();
            options.insert("global-index.build.parallelism".into(), value.into());
            builder.with_options(options);
            assert!(
                builder
                    .build()
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("parallelism"),
                "{kind}"
            );
        }
    }
}
