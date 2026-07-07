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

use super::global_index_types::{normalize_sorted_global_index_type, BTREE_GLOBAL_INDEX_TYPE};
use crate::spec::{DataField, FileKind, IndexFileMeta, IndexManifest};
use crate::table::{CommitMessage, SnapshotManager, Table, TableCommit};
use crate::{Error, Result};
use std::collections::HashMap;

pub struct BTreeGlobalIndexDropBuilder<'a> {
    table: &'a Table,
    index_column: Option<String>,
    index_type: String,
}

impl<'a> BTreeGlobalIndexDropBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            index_column: None,
            index_type: BTREE_GLOBAL_INDEX_TYPE.to_string(),
        }
    }

    pub fn with_index_column(&mut self, column: &str) -> &mut Self {
        self.index_column = Some(column.to_string());
        self
    }

    pub fn with_index_type(&mut self, index_type: &str) -> &mut Self {
        self.index_type = index_type.to_string();
        self
    }

    pub async fn execute(&self) -> Result<usize> {
        let index_type = normalize_sorted_global_index_type(&self.index_type).ok_or_else(|| {
            Error::Unsupported {
                message: format!(
                    "Sorted global index drop only supports index_type => 'btree' or 'bitmap', got '{}'",
                    self.index_type
                ),
            }
        })?;
        let index_column = self
            .index_column
            .as_deref()
            .ok_or_else(|| Error::DataInvalid {
                message: "Sorted global index column is required".to_string(),
                source: None,
            })?;
        let index_field = find_index_field(self.table, index_column)?;

        let snapshot_manager = SnapshotManager::new(
            self.table.file_io().clone(),
            self.table.location().to_string(),
        );
        let Some(snapshot) = snapshot_manager.get_latest_snapshot().await? else {
            return Ok(0);
        };
        let Some(index_manifest_name) = snapshot.index_manifest() else {
            return Ok(0);
        };

        let index_entries = IndexManifest::read(
            self.table.file_io(),
            &snapshot_manager.manifest_path(index_manifest_name),
        )
        .await?;
        let mut deletions_by_partition_bucket: HashMap<(Vec<u8>, i32), Vec<IndexFileMeta>> =
            HashMap::new();
        let mut dropped = 0;
        for entry in index_entries {
            if entry.kind != FileKind::Add || entry.index_file.index_type != index_type {
                continue;
            }
            let Some(global_meta) = entry.index_file.global_index_meta.as_ref() else {
                continue;
            };
            if global_meta.index_field_id != index_field.id() {
                continue;
            }
            dropped += 1;
            deletions_by_partition_bucket
                .entry((entry.partition, entry.bucket))
                .or_default()
                .push(entry.index_file);
        }
        if dropped == 0 {
            return Ok(0);
        }

        let mut groups = deletions_by_partition_bucket
            .into_iter()
            .collect::<Vec<_>>();
        groups.sort_by(
            |((left_partition, left_bucket), _), ((right_partition, right_bucket), _)| {
                left_partition
                    .cmp(right_partition)
                    .then(left_bucket.cmp(right_bucket))
            },
        );
        let messages = groups
            .into_iter()
            .map(|((partition, bucket), deleted_index_files)| {
                let mut message = CommitMessage::new(partition, bucket, vec![]);
                message.deleted_index_files = deleted_index_files;
                message
            })
            .collect::<Vec<_>>();

        TableCommit::new(
            self.table.clone(),
            format!("global-index-{}-drop-{}", index_type, uuid::Uuid::new_v4()),
        )
        .commit_if_latest_snapshot(messages, snapshot.id())
        .await?;

        Ok(dropped)
    }
}

fn find_index_field<'a>(table: &'a Table, column: &str) -> Result<&'a DataField> {
    table
        .schema()
        .fields()
        .iter()
        .find(|field| field.name() == column)
        .ok_or_else(|| Error::ColumnNotExist {
            full_name: table.identifier().full_name(),
            column: column.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Identifier;
    use crate::io::FileIOBuilder;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{
        BinaryRow, DataFileMeta, DataType, DeletionVectorMeta, GlobalIndexMeta, IndexManifestEntry,
        IntType, Schema, TableSchema, VarCharType,
    };
    use crate::table::TableCommit;
    use chrono::{DateTime, Utc};
    use indexmap::IndexMap;

    fn test_table(table_path: &str) -> Table {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("name", DataType::VarChar(VarCharType::string_type()))
            .build()
            .unwrap();
        Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("default", "test_table"),
            table_path.to_string(),
            TableSchema::new(0, &schema),
            None,
        )
    }

    async fn setup_dirs(table: &Table) {
        table
            .file_io()
            .mkdirs(&format!("{}/snapshot/", table.location()))
            .await
            .unwrap();
        table
            .file_io()
            .mkdirs(&format!("{}/manifest/", table.location()))
            .await
            .unwrap();
    }

    fn global_index_file(
        index_type: &str,
        name: &str,
        index_field_id: i32,
        row_range_start: i64,
        row_range_end: i64,
    ) -> IndexFileMeta {
        IndexFileMeta {
            index_type: index_type.to_string(),
            file_name: name.to_string(),
            file_size: 128,
            row_count: (row_range_end - row_range_start + 1) as i32,
            deletion_vectors_ranges: None,
            global_index_meta: Some(GlobalIndexMeta {
                row_range_start,
                row_range_end,
                index_field_id,
                extra_field_ids: None,
                index_meta: None,
            }),
        }
    }

    fn hash_index_file(name: &str) -> IndexFileMeta {
        IndexFileMeta {
            index_type: "HASH".to_string(),
            file_name: name.to_string(),
            file_size: 64,
            row_count: 1,
            deletion_vectors_ranges: None,
            global_index_meta: None,
        }
    }

    fn deletion_vector_index_file(name: &str) -> IndexFileMeta {
        IndexFileMeta {
            index_type: "DELETION_VECTORS".to_string(),
            file_name: name.to_string(),
            file_size: 64,
            row_count: 1,
            deletion_vectors_ranges: Some(IndexMap::from([(
                "data-0.parquet".to_string(),
                DeletionVectorMeta {
                    offset: 1,
                    length: 8,
                    cardinality: Some(1),
                },
            )])),
            global_index_meta: None,
        }
    }

    fn data_file(name: &str) -> DataFileMeta {
        DataFileMeta {
            file_name: name.to_string(),
            file_size: 128,
            row_count: 1,
            min_key: vec![],
            max_key: vec![],
            key_stats: BinaryTableStats::empty(),
            value_stats: BinaryTableStats::empty(),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id: 0,
            level: 0,
            extra_files: vec![],
            creation_time: Some(
                "2024-09-06T07:45:55.039+00:00"
                    .parse::<DateTime<Utc>>()
                    .unwrap(),
            ),
            delete_row_count: None,
            embedded_index: None,
            first_row_id: None,
            write_cols: None,
            external_path: None,
            file_source: None,
            value_stats_cols: None,
        }
    }

    async fn latest_index_entries(table: &Table) -> Vec<IndexManifestEntry> {
        let snapshot_manager =
            SnapshotManager::new(table.file_io().clone(), table.location().to_string());
        let snapshot = snapshot_manager
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        let Some(index_manifest_name) = snapshot.index_manifest() else {
            return Vec::new();
        };
        IndexManifest::read(
            table.file_io(),
            &snapshot_manager.manifest_path(index_manifest_name),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_drop_btree_global_index_only_removes_target_entries() {
        let table = test_table("memory:/test_drop_btree_global_index");
        setup_dirs(&table).await;

        let mut message = CommitMessage::new(
            BinaryRow::new(0).to_serialized_bytes(),
            0,
            vec![data_file("data-0.parquet")],
        );
        message.new_index_files = vec![
            global_index_file(BTREE_GLOBAL_INDEX_TYPE, "btree-id.index", 0, 0, 9),
            global_index_file(BTREE_GLOBAL_INDEX_TYPE, "btree-name.index", 1, 0, 9),
            global_index_file("full-text", "fulltext-id.index", 0, 100, 109),
            hash_index_file("hash.index"),
            deletion_vector_index_file("dv.index"),
        ];
        TableCommit::new(table.clone(), "test-user".to_string())
            .commit(vec![message])
            .await
            .unwrap();

        let dropped = table
            .new_btree_global_index_drop_builder()
            .with_index_column("id")
            .execute()
            .await
            .unwrap();
        assert_eq!(dropped, 1);

        let mut remaining = latest_index_entries(&table)
            .await
            .into_iter()
            .map(|entry| entry.index_file.file_name)
            .collect::<Vec<_>>();
        remaining.sort();
        assert_eq!(
            remaining,
            vec![
                "btree-name.index".to_string(),
                "dv.index".to_string(),
                "fulltext-id.index".to_string(),
                "hash.index".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn test_drop_btree_global_index_is_idempotent_without_match() {
        let table = test_table("memory:/test_drop_btree_global_index_idempotent");
        setup_dirs(&table).await;

        let mut message = CommitMessage::new(vec![], 0, vec![data_file("data-0.parquet")]);
        message.new_index_files = vec![global_index_file(
            BTREE_GLOBAL_INDEX_TYPE,
            "btree-name.index",
            1,
            0,
            9,
        )];
        TableCommit::new(table.clone(), "test-user".to_string())
            .commit(vec![message])
            .await
            .unwrap();

        let dropped = table
            .new_btree_global_index_drop_builder()
            .with_index_column("id")
            .execute()
            .await
            .unwrap();
        assert_eq!(dropped, 0);

        let remaining = latest_index_entries(&table).await;
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].index_file.file_name, "btree-name.index");
    }
}
