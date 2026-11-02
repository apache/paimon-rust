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
use crate::io::{FileIOBuilder, FileIOProvider};
use crate::spec::{
    DataEvolutionIndexSourceMeta, DataType, IntType, PrimaryKeyIndexSourceFile,
    PrimaryKeyIndexSourceMeta, Schema, TableSchema,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Debug)]
struct StatFailure {
    operator: opendal::Operator,
    calls: AtomicUsize,
    fail_stat: bool,
}
#[async_trait::async_trait]
impl FileIOProvider for StatFailure {
    async fn create(&self, path: &str) -> Result<(opendal::Operator, String)> {
        if path.ends_with("/new.index")
            && self.calls.fetch_add(1, Ordering::SeqCst) == 1
            && self.fail_stat
        {
            return Err(Error::UnexpectedError {
                message: "injected index stat failure".into(),
                source: None,
            });
        }
        Ok((
            self.operator.clone(),
            path.strip_prefix("index-test:/").unwrap().into(),
        ))
    }
}

/// Both maintenance paths share output ownership, while supplying their own
/// source frames and placement. Never delete a previous snapshot's payload.
#[tokio::test]
async fn output_ownership_and_source_metadata_are_identical_for_pk_and_de() {
    for bucket_local in [false, true] {
        for failure in ["none", "write", "stat"] {
            let provider = Arc::new(StatFailure {
                operator:
                    opendal::Operator::from_config(opendal::services::MemoryConfig::default())
                        .unwrap(),
                calls: AtomicUsize::new(0),
                fail_stat: failure == "stat",
            });
            let io = FileIOBuilder::new("memory")
                .with_provider(provider)
                .build()
                .unwrap();
            let schema = Schema::builder()
                .column("value", DataType::Int(IntType::new()))
                .option("index-file-in-data-file-dir", "true")
                .build()
                .unwrap();
            let table = Table::new(
                io.clone(),
                crate::catalog::Identifier::new("default", "output"),
                "index-test:/table".into(),
                TableSchema::new(0, &schema),
                None,
            );
            let source = if bucket_local {
                PrimaryKeyIndexSourceMeta::new(
                    1,
                    vec![PrimaryKeyIndexSourceFile::new("data.parquet".into(), 3).unwrap()],
                )
                .unwrap()
                .serialize()
                .unwrap()
            } else {
                DataEvolutionIndexSourceMeta::new(3).unwrap().serialize()
            };
            let start = if bucket_local { 0 } else { 11 };
            let location = if bucket_local {
                IndexFileLocation::BucketLocal {
                    table_path: table.location(),
                    bucket_path: "index-test:/table/custom/p=a/bucket-3",
                    index_file_in_data_file_dir: true,
                }
            } else {
                IndexFileLocation::Global {
                    table_path: table.location(),
                }
            };
            let path = location.resolve("new.index", None);
            let old_path = location.resolve("old.index", None);
            let file = IndexFileWriter::new(
                &table,
                "btree",
                "new.index".into(),
                location,
                "custom/p=a/bucket-3",
                GlobalIndexMeta {
                    row_range_start: start,
                    row_range_end: start + 2,
                    index_field_id: 0,
                    extra_field_ids: None,
                    source_meta: Some(source.clone()),
                    index_meta: None,
                },
            )
            .await
            .unwrap();
            io.new_output(&old_path)
                .unwrap()
                .write(Bytes::from_static(b"old"))
                .await
                .unwrap();
            let outcome = file
                .write(&table, |output| async move {
                    output.write(Bytes::from_static(b"payload")).await?;
                    if failure == "write" {
                        return Err(Error::UnexpectedError {
                            message: "injected index write failure".into(),
                            source: None,
                        });
                    }
                    Ok(Some(IndexPayload {
                        row_count: 3,
                        index_meta: vec![1, 2, 3],
                    }))
                })
                .await;
            assert_eq!(
                io.new_input(&old_path).unwrap().read().await.unwrap(),
                Bytes::from_static(b"old")
            );
            if failure == "none" {
                let file = outcome.unwrap().unwrap();
                assert_eq!(file.file_size, 7);
                assert_eq!(file.row_count, 3);
                let meta = file.global_index_meta.unwrap();
                assert_eq!(meta.source_meta, Some(source));
                assert_eq!(
                    (meta.row_range_start, meta.row_range_end),
                    (start, start + 2)
                );
                assert_eq!(meta.index_meta, Some(vec![1, 2, 3]));
                assert!(io.exists(&path).await.unwrap());
            } else {
                assert!(outcome.unwrap_err().to_string().contains("injected index"));
                assert!(
                    !io.exists(&path).await.unwrap(),
                    "private payload must be removed on {failure} failure"
                );
            }
        }
    }
}
