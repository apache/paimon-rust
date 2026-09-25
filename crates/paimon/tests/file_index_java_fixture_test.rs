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

//! End-to-end compatibility coverage for FileIndex tables written and committed
//! by Apache Paimon Java. See `testdata/file_index/README.md` for provenance and
//! regeneration instructions.

#![cfg(all(not(target_os = "windows"), feature = "storage-fs"))]

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use opendal::Operator;
use paimon::catalog::Identifier;
use paimon::io::{FileIO, FileIOBuilder, FileIOProvider};
use paimon::spec::{Datum, Predicate, PredicateBuilder};
use paimon::table::{SchemaManager, Table};

const FIXTURE: &str = "testdata/file_index/default.db";

#[derive(Clone, Copy)]
struct FixtureCase {
    table: &'static str,
    embedded: bool,
    range_bitmap: bool,
}

const CASES: &[FixtureCase] = &[
    FixtureCase {
        table: "bitmap_embedded",
        embedded: true,
        range_bitmap: false,
    },
    FixtureCase {
        table: "bitmap_sidecar",
        embedded: false,
        range_bitmap: false,
    },
    FixtureCase {
        table: "bloom_filter_embedded",
        embedded: true,
        range_bitmap: false,
    },
    FixtureCase {
        table: "bloom_filter_sidecar",
        embedded: false,
        range_bitmap: false,
    },
    FixtureCase {
        table: "range_bitmap_embedded",
        embedded: true,
        range_bitmap: true,
    },
    FixtureCase {
        table: "range_bitmap_sidecar",
        embedded: false,
        range_bitmap: true,
    },
];

#[derive(Debug)]
struct FsProbe {
    operator: Operator,
    data_file_opens: AtomicUsize,
}

impl FsProbe {
    fn new() -> Arc<Self> {
        let mut config = opendal_service_fs::FsConfig::default();
        config.root = Some("/".to_string());
        Arc::new(Self {
            operator: Operator::from_config(config).expect("build filesystem operator"),
            data_file_opens: AtomicUsize::new(0),
        })
    }

    fn reset(&self) {
        self.data_file_opens.store(0, Ordering::SeqCst);
    }

    fn data_file_opens(&self) -> usize {
        self.data_file_opens.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl FileIOProvider for FsProbe {
    async fn create(&self, path: &str) -> paimon::Result<(Operator, String)> {
        if path.ends_with(".parquet") {
            self.data_file_opens.fetch_add(1, Ordering::SeqCst);
        }
        let relative = path
            .strip_prefix("file://")
            .expect("fixture paths must be file URLs")
            .trim_start_matches('/')
            .to_string();
        Ok((self.operator.clone(), relative))
    }
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create staged fixture directory");
    for entry in std::fs::read_dir(src).expect("read fixture directory") {
        let entry = entry.expect("read fixture entry");
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if from.is_dir() {
            copy_dir(&from, &to);
        } else {
            std::fs::copy(&from, &to).expect("copy fixture file");
        }
    }
}

async fn open_fixture(case: FixtureCase) -> (tempfile::TempDir, Table, Arc<FsProbe>) {
    let src = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(FIXTURE)
        .join(case.table);
    let temp = tempfile::tempdir().expect("create temp dir");
    let staged = temp.path().join(case.table);
    copy_dir(&src, &staged);

    let probe = FsProbe::new();
    let file_io: FileIO = FileIOBuilder::new("file")
        .with_provider(probe.clone())
        .build()
        .expect("build probed FileIO");
    let location = format!("file://{}", staged.display());
    let schema = SchemaManager::new(file_io.clone(), location.clone())
        .latest()
        .await
        .expect("load fixture schemas")
        .expect("fixture table has no schema");
    let table = Table::new(
        file_io,
        Identifier::new("default", case.table),
        location,
        (*schema).clone(),
        None,
    );
    (temp, table, probe)
}

fn rows(batches: &[RecordBatch]) -> Vec<(Option<i32>, String)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id is int32");
        let payloads = batch
            .column_by_name("payload")
            .expect("payload column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("payload is utf8");
        rows.extend((0..batch.num_rows()).map(|row| {
            let id = (!ids.is_null(row)).then(|| ids.value(row));
            (id, payloads.value(row).to_string())
        }));
    }
    rows.sort_unstable();
    rows
}

async fn query(
    table: &Table,
    index_enabled: bool,
    predicate: Predicate,
) -> Vec<(Option<i32>, String)> {
    let table = table.copy_with_options(HashMap::from([(
        "file-index.read.enabled".to_string(),
        index_enabled.to_string(),
    )]));
    let mut builder = table.new_read_builder();
    builder.with_filter(predicate);
    let plan = builder.new_scan().plan().await.expect("plan fixture query");
    let batches = builder
        .new_read()
        .expect("create fixture reader")
        .to_arrow(plan.splits())
        .expect("create fixture Arrow stream")
        .try_collect::<Vec<_>>()
        .await
        .expect("read fixture rows");
    rows(&batches)
}

#[tokio::test]
async fn reads_java_written_file_indexes_and_prunes_data_files() {
    for case in CASES.iter().copied() {
        let (_temp, table, probe) = open_fixture(case).await;
        let predicates = PredicateBuilder::new(table.schema().fields());
        let id_one = predicates.equal("id", Datum::Int(1)).unwrap();
        let null_id = predicates.is_null("id").unwrap();
        let missing = if case.range_bitmap {
            predicates
                .between("id", Datum::Int(3), Datum::Int(7))
                .unwrap()
        } else {
            predicates.equal("id", Datum::Int(2)).unwrap()
        };
        let residual = Predicate::and(vec![
            id_one.clone(),
            predicates
                .equal("payload", Datum::String("keep".to_string()))
                .unwrap(),
        ]);

        let mut planning_builder = table.new_read_builder();
        planning_builder.with_filter(missing.clone());
        let (plan, trace) = planning_builder
            .new_scan()
            .plan_with_trace()
            .await
            .expect("plan missing-value query");
        assert_eq!(
            trace.manifest_entries_pruned_by_data_stats, 0,
            "{}",
            case.table
        );
        assert_eq!(trace.final_files, 1, "{}", case.table);
        let file = &plan.splits()[0].data_files()[0];
        assert_eq!(file.row_count, 4, "{}", case.table);
        assert_eq!(
            file.embedded_index.is_some(),
            case.embedded,
            "{}",
            case.table
        );
        assert_eq!(
            file.extra_files.iter().any(|name| name.ends_with(".index")),
            !case.embedded,
            "{}",
            case.table
        );

        for enabled in [false, true] {
            assert_eq!(
                query(&table, enabled, id_one.clone()).await,
                vec![(Some(1), "drop".to_string()), (Some(1), "keep".to_string())],
                "{} with FileIndex enabled={enabled}",
                case.table
            );
            assert_eq!(
                query(&table, enabled, null_id.clone()).await,
                vec![(None, "null-id".to_string())],
                "{} with FileIndex enabled={enabled}",
                case.table
            );
            assert_eq!(
                query(&table, enabled, residual.clone()).await,
                vec![(Some(1), "keep".to_string())],
                "{} must retain row-level residual filtering with FileIndex enabled={enabled}",
                case.table
            );
            if case.range_bitmap {
                let range_hit = predicates
                    .between("id", Datum::Int(1), Datum::Int(3))
                    .unwrap();
                assert_eq!(
                    query(&table, enabled, range_hit).await,
                    vec![(Some(1), "drop".to_string()), (Some(1), "keep".to_string())],
                    "{} range query with FileIndex enabled={enabled}",
                    case.table
                );
            }
            assert!(
                query(&table, enabled, missing.clone()).await.is_empty(),
                "{} with FileIndex enabled={enabled}",
                case.table
            );
        }

        probe.reset();
        assert!(query(&table, true, missing.clone()).await.is_empty());
        assert_eq!(
            probe.data_file_opens(),
            0,
            "{} must be skipped by its Java-written FileIndex",
            case.table
        );

        probe.reset();
        assert!(query(&table, false, missing).await.is_empty());
        assert!(
            probe.data_file_opens() > 0,
            "{} must open the data file when FileIndex reads are disabled",
            case.table
        );
    }
}
