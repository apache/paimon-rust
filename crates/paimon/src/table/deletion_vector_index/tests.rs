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
use crate::spec::{DataType, IntType, Schema, TableSchema};

#[tokio::test]
async fn unchanged_source_masks_and_new_masks_do_not_open_payloads() {
    for in_data in ["false", "true"] {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .primary_key(["id"])
            .option("bucket", "1")
            .option("deletion-vectors.enabled", "true")
            .option("deletion-vectors.merge-on-read", "false")
            .option("index-file-in-data-file-dir", in_data)
            .build()
            .unwrap();
        let (table, counter) =
            crate::table::maintenance_test_utils::table(TableSchema::new(0, &schema)).await;
        let file = IndexFileMeta {
            index_type: DELETION_VECTORS_INDEX_TYPE.into(),
            file_name: "unopened-dv".into(),
            file_size: 100,
            row_count: 1,
            deletion_vectors_ranges: Some(IndexMap::from([(
                "source.parquet".into(),
                DeletionVectorMeta {
                    offset: 1,
                    length: 20,
                    cardinality: Some(1),
                },
            )])),
            external_path: None,
            global_index_meta: None,
        };
        let partition = BinaryRow::new(0).to_serialized_bytes();
        let path = file_path(&table, &partition, 0, &file).unwrap();
        let entry = IndexManifestEntry {
            kind: FileKind::Add,
            partition,
            bucket: 0,
            index_file: file,
            version: 1,
        };
        // These files deliberately do not exist. Reopening an unchanged mask
        // would fail instead of merely adding an invisible extra I/O.
        let mut unrelated = entry.clone();
        unrelated.bucket = 1;
        unrelated.index_file.file_name = "unrelated-unopened-dv".into();
        let unrelated_path = file_path(
            &table,
            &unrelated.partition,
            unrelated.bucket,
            &unrelated.index_file,
        )
        .unwrap();
        counter.watch(vec![path, unrelated_path]);
        let sources = HashSet::from([(vec![], 0, "source.parquet".into())]);
        validate_source_deletion_growth(
            &table,
            std::slice::from_ref(&entry),
            &[entry.clone(), unrelated],
            sources.clone(),
        )
        .await
        .unwrap();
        validate_source_deletion_growth(&table, &[], &[entry], sources)
            .await
            .unwrap();
        assert!(
            counter.counts().values().all(|reads| *reads == 0),
            "{in_data}: {:?}",
            counter.counts()
        );
    }
}
