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

use super::global_index_build_common::IndexColumns;
use super::global_index_types::{
    normalize_global_index_type, BTREE_GLOBAL_INDEX_TYPE, SUPPORTED_GLOBAL_INDEX_TYPES_FOR_DROP,
};
use crate::spec::{
    eval_row, extract_datum, field_idx_to_partition_idx, BinaryRow, DataType, Datum, FileKind,
    IndexFileMeta, IndexManifest, Predicate, PredicateOperator,
};
use crate::table::{CommitMessage, Table, TableCommit};
use crate::{Error, Result};
use std::collections::HashMap;

pub struct GlobalIndexDropBuilder<'a> {
    table: &'a Table,
    index_column: Option<IndexColumns>,
    index_type: String,
    partition_filter: Option<Predicate>,
    dry_run: bool,
}

impl<'a> GlobalIndexDropBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            index_column: None,
            index_type: BTREE_GLOBAL_INDEX_TYPE.to_string(),
            partition_filter: None,
            dry_run: false,
        }
    }

    /// Whether this index identity can be selected by this dropper.
    pub fn supports_index_type(index_type: &str) -> bool {
        normalize_global_index_type(index_type.trim()).is_some()
    }

    pub fn with_index_column(&mut self, column: &str) -> &mut Self {
        self.index_column = Some(IndexColumns::Column(column.to_string()));
        self
    }

    /// Drop exactly the definition with these ordered BTree columns.
    pub fn with_index_columns(&mut self, columns: &[&str]) -> &mut Self {
        self.index_column = Some(IndexColumns::Columns(
            columns.iter().map(|column| (*column).to_string()).collect(),
        ));
        self
    }

    pub fn with_index_type(&mut self, index_type: &str) -> &mut Self {
        self.index_type = index_type.to_string();
        self
    }

    /// Restrict the drop using table-field indices. Repeated calls combine filters with AND.
    pub fn with_partition_filter(&mut self, filter: Predicate) -> Result<&mut Self> {
        super::partition_filter::validate_partition_filter(self.table, &filter)?;
        self.partition_filter = Some(match self.partition_filter.take() {
            Some(previous) => Predicate::and(vec![previous, filter]),
            None => filter,
        });
        Ok(self)
    }

    /// Return the matched index file count without committing when enabled.
    pub fn with_dry_run(&mut self, dry_run: bool) -> &mut Self {
        self.dry_run = dry_run;
        self
    }

    /// Prepare metadata deletions without publishing a snapshot or removing files.
    /// The caller decides whether to commit the returned messages.
    pub async fn build(&self) -> Result<Vec<CommitMessage>> {
        Ok(self.prepare().await?.1)
    }

    pub async fn execute(&self) -> Result<usize> {
        let (snapshot_id, messages) = self.prepare().await?;
        let dropped = messages
            .iter()
            .map(|message| message.deleted_index_files.len())
            .sum();
        if self.dry_run || dropped == 0 {
            return Ok(dropped);
        }
        TableCommit::new(
            self.table.clone(),
            format!(
                "global-index-{}-drop-{}",
                self.index_type,
                uuid::Uuid::new_v4()
            ),
        )
        .commit_if_latest_snapshot(messages, snapshot_id.expect("nonempty drop has a snapshot"))
        .await?;
        // A failed commit may have been published. Never remove index files here.
        Ok(dropped)
    }

    async fn prepare(&self) -> Result<(Option<i64>, Vec<CommitMessage>)> {
        // Dropping an index reads the index manifest.
        crate::spec::CoreOptions::new(self.table.schema().options()).ensure_read_authorized()?;

        self.table.ensure_not_branch_reference_for_write()?;

        let index_type = normalize_global_index_type(self.index_type.trim()).ok_or_else(|| {
            Error::Unsupported {
                message: format!(
                    "unsupported global index type '{}'; supported: {}",
                    self.index_type, SUPPORTED_GLOBAL_INDEX_TYPES_FOR_DROP
                ),
            }
        })?;
        let index_column = self
            .index_column
            .as_ref()
            .ok_or_else(|| Error::DataInvalid {
                message: "Global index column is required".to_string(),
                source: None,
            })?;
        let index_fields = super::global_index_build_common::resolve_index_fields(
            self.table,
            index_column,
            index_type,
        )?;
        let extra_field_ids = index_fields
            .iter()
            .skip(1)
            .map(|field| field.id())
            .collect::<Vec<_>>();
        let partition_filter = self
            .partition_filter
            .as_ref()
            .map(|filter| {
                let mapping = field_idx_to_partition_idx(
                    self.table.schema().fields(),
                    self.table.schema().partition_keys(),
                );
                filter
                    .remap_field_index(&mapping)
                    .ok_or_else(|| Error::ConfigInvalid {
                        message: "Partition filter must reference only partition keys".to_string(),
                    })
            })
            .transpose()?;

        // The table's manager resolves a REST-managed table's latest snapshot
        // through the catalog, like reads and commit validation do.
        let snapshot_manager = self.table.snapshot_manager();
        let Some(snapshot) = snapshot_manager.get_latest_snapshot().await? else {
            return Ok((None, vec![]));
        };
        let Some(index_manifest_name) = snapshot.index_manifest() else {
            return Ok((Some(snapshot.id()), vec![]));
        };

        let index_entries = IndexManifest::read(
            self.table.file_io(),
            &snapshot_manager.manifest_path(index_manifest_name),
        )
        .await?;
        let mut deletions_by_partition_bucket: HashMap<(Vec<u8>, i32), Vec<IndexFileMeta>> =
            HashMap::new();
        for entry in index_entries {
            if entry.kind != FileKind::Add {
                continue;
            }
            if normalize_global_index_type(&entry.index_file.index_type) != Some(index_type) {
                continue;
            }
            let Some(global_meta) = entry.index_file.global_index_meta.as_ref() else {
                continue;
            };
            if global_meta.index_field_id != index_fields[0].id()
                || !super::global_index_build_common::same_extra_field_ids(
                    global_meta.extra_field_ids.as_deref(),
                    Some(&extra_field_ids),
                )
            {
                continue;
            }
            if let Some(filter) = &partition_filter {
                // Deletion requires exact matching, not conservative scan pruning.
                let partition = BinaryRow::from_serialized_bytes(&entry.partition)?;
                if partition.arity() as usize != self.table.schema().partition_keys().len() {
                    return Err(Error::DataInvalid {
                        message: "Index partition row arity does not match partition keys"
                            .to_string(),
                        source: None,
                    });
                }
                if !eval_partition_filter(filter, &partition)? {
                    continue;
                }
            }
            deletions_by_partition_bucket
                .entry((entry.partition, entry.bucket))
                .or_default()
                .push(entry.index_file);
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

        Ok((Some(snapshot.id()), messages))
    }
}

/// Preserve Java partition comparisons without changing general row evaluation.
fn eval_partition_filter(predicate: &Predicate, row: &BinaryRow) -> Result<bool> {
    use std::cmp::Ordering::{Equal, Greater, Less};
    use PredicateOperator::*;

    match predicate {
        Predicate::And(children) => {
            for child in children {
                if !eval_partition_filter(child, row)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Predicate::Or(children) => {
            for child in children {
                if eval_partition_filter(child, row)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Predicate::Not(inner) => Ok(!eval_partition_filter(inner, row)?),
        Predicate::Leaf {
            index,
            data_type,
            op,
            literals,
            ..
        } if matches!(data_type, DataType::Float(_) | DataType::Double(_))
            && !matches!(op, IsNull | IsNotNull) =>
        {
            let Some(value) = extract_datum(row, *index, data_type)? else {
                return Ok(false);
            };
            let compare = |literal: &Datum| {
                // Widening FLOAT to DOUBLE is exact, including the sign of zero.
                let (left, right) = match (&value, literal) {
                    (Datum::Float(left), Datum::Float(right)) => {
                        (f64::from(*left), f64::from(*right))
                    }
                    (Datum::Double(left), Datum::Double(right)) => (*left, *right),
                    _ => return None,
                };
                Some(match (left.is_nan(), right.is_nan()) {
                    // Java canonicalizes every NaN before comparing floating-point bits.
                    (true, true) => Equal,
                    (true, false) => Greater,
                    (false, true) => Less,
                    (false, false) => left.total_cmp(&right),
                })
            };
            let first = || literals.first().and_then(compare);
            Ok(match op {
                Eq => first() == Some(Equal),
                NotEq => matches!(first(), Some(Less | Greater)),
                Lt => first() == Some(Less),
                LtEq => matches!(first(), Some(Less | Equal)),
                Gt => first() == Some(Greater),
                GtEq => matches!(first(), Some(Greater | Equal)),
                In => literals
                    .iter()
                    .any(|literal| compare(literal) == Some(Equal)),
                NotIn => !literals
                    .iter()
                    .any(|literal| compare(literal) == Some(Equal)),
                Between | NotBetween => {
                    let between = matches!(first(), Some(Greater | Equal))
                        && matches!(literals.get(1).and_then(compare), Some(Less | Equal));
                    if *op == Between {
                        between
                    } else {
                        !between
                    }
                }
                _ => {
                    return Err(Error::ConfigInvalid {
                        message: format!("Unsupported floating partition operator '{op}'"),
                    })
                }
            })
        }
        _ => eval_row(predicate, row),
    }
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
    use std::collections::BTreeMap;

    #[test]
    fn test_floating_partition_comparisons_follow_java_ordering() {
        use crate::spec::{BinaryRowBuilder, DoubleType, FloatType};
        use PredicateOperator::*;

        for data_type in [
            DataType::Float(FloatType::new()),
            DataType::Double(DoubleType::new()),
        ] {
            let datum = |value| match data_type {
                DataType::Float(_) => Datum::Float(value as f32),
                _ => Datum::Double(value),
            };
            // Java compares all NaN signs and payloads equally, above positive infinity.
            let values = [
                f64::NEG_INFINITY,
                -0.0,
                0.0,
                f64::INFINITY,
                f64::NAN,
                f64::from_bits(0xfff8_1234_5678_0000),
            ];
            let mut rows = values
                .iter()
                .map(|value| {
                    let mut row = BinaryRowBuilder::new(1);
                    row.write_datum(0, &datum(*value), &data_type);
                    row.build()
                })
                .collect::<Vec<_>>();
            let mut null = BinaryRowBuilder::new(1);
            null.set_null_at(0);
            rows.push(null.build());
            let leaf = |op, literals: Vec<f64>| Predicate::Leaf {
                column: "f".into(),
                index: 0,
                data_type: data_type.clone(),
                op,
                literals: literals.into_iter().map(datum).collect(),
            };
            for (op, literals, expected) in [
                (Eq, vec![0.0], vec![2]),
                (Eq, vec![f64::NAN], vec![4, 5]),
                (NotEq, vec![0.0], vec![0, 1, 3, 4, 5]),
                (Lt, vec![0.0], vec![0, 1]),
                (LtEq, vec![0.0], vec![0, 1, 2]),
                (Gt, vec![0.0], vec![3, 4, 5]),
                (GtEq, vec![0.0], vec![2, 3, 4, 5]),
                (In, vec![-0.0, f64::NAN], vec![1, 4, 5]),
                (NotIn, vec![-0.0, f64::NAN], vec![0, 2, 3]),
                (Between, vec![-0.0, 0.0], vec![1, 2]),
                (NotBetween, vec![-0.0, 0.0], vec![0, 3, 4, 5]),
                (IsNull, vec![], vec![6]),
                (IsNotNull, vec![], vec![0, 1, 2, 3, 4, 5]),
            ] {
                let predicate = leaf(op, literals);
                for (i, row) in rows.iter().enumerate() {
                    assert_eq!(
                        eval_partition_filter(&predicate, row).unwrap(),
                        expected.contains(&i),
                        "{data_type:?}: {op}, row {i}"
                    );
                }
            }
            let nested = Predicate::and(vec![
                Predicate::or(vec![leaf(Eq, vec![0.0]), leaf(Eq, vec![f64::NAN])]),
                Predicate::Not(Box::new(leaf(Eq, vec![0.0]))),
            ]);
            for (i, row) in rows.iter().enumerate() {
                assert_eq!(
                    eval_partition_filter(&nested, row).unwrap(),
                    matches!(i, 4 | 5)
                );
            }
        }
    }

    #[tokio::test]
    async fn test_query_auth_table_refuses_index_builds_and_drops() {
        let table = crate::table::query_auth_table();

        let refused = |err: crate::Error, what: &str| {
            assert!(
                matches!(err, crate::Error::Unsupported { ref message }
                    if message.contains("query-auth.enabled")),
                "{what} on a query-auth.enabled table must fail closed, got {err:?}"
            );
        };

        for dry_run in [false, true] {
            refused(
                table
                    .new_global_index_drop_builder()
                    .with_index_type("btree")
                    .with_dry_run(dry_run)
                    .execute()
                    .await
                    .unwrap_err(),
                "dropping a global index",
            );
        }
        refused(
            table
                .new_global_index_build_builder()
                .execute()
                .await
                .unwrap_err(),
            "building a BTree global index",
        );
        refused(
            table
                .new_global_index_build_builder()
                .with_index_type("vector")
                .execute()
                .await
                .unwrap_err(),
            "building a vector index",
        );
        refused(
            table
                .new_global_index_build_builder()
                .with_index_type("diskann")
                .execute()
                .await
                .unwrap_err(),
            "building a diskann index",
        );
    }

    fn test_table(table_path: &str) -> Table {
        let schema = Schema::builder()
            .option("write-only", "true")
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
            row_count: (row_range_end - row_range_start + 1),
            deletion_vectors_ranges: None,
            external_path: None,
            global_index_meta: Some(GlobalIndexMeta {
                row_range_start,
                row_range_end,
                index_field_id,
                extra_field_ids: None,
                source_meta: None,
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
            external_path: None,
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
            external_path: None,
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
            column_max_sequence_numbers: None,
        }
    }

    async fn latest_index_entries(table: &Table) -> Vec<IndexManifestEntry> {
        let snapshot_manager = crate::table::SnapshotManager::new(
            table.file_io().clone(),
            table.location().to_string(),
        );
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
    async fn build_prepares_only_metadata_deletions_until_explicit_commit() {
        for dry_run in [false, true] {
            let table = test_table(&format!("memory:/prepared_index_drop_{dry_run}"));
            setup_dirs(&table).await;
            let mut message = CommitMessage::new(vec![], 0, vec![data_file("data.parquet")]);
            message.new_index_files = vec![
                global_index_file("btree", "id.index", 0, 0, 9),
                global_index_file("bitmap", "id-bitmap.index", 0, 0, 9),
                global_index_file("btree", "name.index", 1, 0, 9),
            ];
            TableCommit::new(table.clone(), "prepare-test".into())
                .commit(vec![message])
                .await
                .unwrap();
            let snapshot_id = table
                .snapshot_manager()
                .get_latest_snapshot_id()
                .await
                .unwrap();
            let before = table_files(&table).await;
            let mut builder = table.new_global_index_drop_builder();
            builder.with_index_column("id").with_dry_run(dry_run);
            let messages = builder.build().await.unwrap();
            assert_eq!(messages.len(), 1);
            assert!(messages[0].new_files.is_empty());
            assert!(messages[0].new_index_files.is_empty());
            assert_eq!(messages[0].deleted_index_files.len(), 1);
            assert_eq!(messages[0].deleted_index_files[0].file_name, "id.index");
            assert_eq!(latest_index_entries(&table).await.len(), 3);
            assert_eq!(table_files(&table).await, before);
            assert_eq!(
                table
                    .snapshot_manager()
                    .get_latest_snapshot_id()
                    .await
                    .unwrap(),
                snapshot_id
            );
            // build() is always preparation, even on a builder configured for
            // execute()'s dry run; only this explicit call publishes deletions.
            TableCommit::new(table.clone(), "explicit-drop".into())
                .commit(messages)
                .await
                .unwrap();
            let mut names = latest_index_entries(&table)
                .await
                .into_iter()
                .map(|entry| entry.index_file.file_name)
                .collect::<Vec<_>>();
            names.sort();
            assert_eq!(names, vec!["id-bitmap.index", "name.index"]);
            assert!(builder.build().await.unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn build_validates_empty_tables_and_authorization_without_publication() {
        let table = test_table("memory:/prepared_index_drop_empty");
        setup_dirs(&table).await;
        assert!(table.new_global_index_drop_builder().build().await.is_err());
        assert!(table
            .new_global_index_drop_builder()
            .with_index_column("id")
            .build()
            .await
            .unwrap()
            .is_empty());
        assert!(crate::table::query_auth_table()
            .new_global_index_drop_builder()
            .with_index_column("id")
            .build()
            .await
            .is_err());
        assert!(GlobalIndexDropBuilder::supports_index_type(" bTrEe "));
        assert!(GlobalIndexDropBuilder::supports_index_type("FULL-TEXT"));
        assert!(!GlobalIndexDropBuilder::supports_index_type("hash"));
    }

    async fn table_files(table: &Table) -> BTreeMap<String, Vec<u8>> {
        let mut files = BTreeMap::new();
        for status in table
            .file_io()
            .list_status_recursive(&format!("{}/", table.location()))
            .await
            .unwrap()
        {
            let content = table
                .file_io()
                .new_input(&status.path)
                .unwrap()
                .read()
                .await
                .unwrap();
            files.insert(status.path, content.to_vec());
        }
        files
    }

    async fn partitioned_index_table(path: &str) -> Table {
        let schema = Schema::builder()
            .option("write-only", "true")
            .column("id", DataType::Int(IntType::new()))
            .column("dt", DataType::VarChar(VarCharType::string_type()))
            .column("name", DataType::VarChar(VarCharType::string_type()))
            .column("hr", DataType::Int(IntType::new()))
            .partition_keys(["hr", "dt"])
            .build()
            .unwrap();
        let table = Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("default", "partitioned_indexes"),
            path.to_string(),
            TableSchema::new(0, &schema),
            None,
        );
        setup_dirs(&table).await;
        let mut messages = Vec::new();
        for (i, (dt, hr)) in [
            (Some("2026-10-10"), 1),
            (Some("2026-10-10"), 2),
            (Some("2026-10-11"), 1),
            (Some("2026-10-11"), 2),
            (None, 1),
        ]
        .into_iter()
        .enumerate()
        {
            let mut partition = crate::spec::BinaryRowBuilder::new(2);
            partition.write_int(0, hr);
            match dt {
                Some(dt) => partition.write_string(1, dt),
                None => partition.set_null_at(1),
            }
            let start = i as i64 * 10;
            let end = start + 9;
            let mut composite =
                global_index_file("btree", &format!("composite-{i}"), 0, start, end);
            composite
                .global_index_meta
                .as_mut()
                .unwrap()
                .extra_field_ids = Some(vec![2]);
            let mut message = CommitMessage::new(
                partition.build_serialized(),
                0,
                vec![data_file(&format!("data-{i}.parquet"))],
            );
            message.new_index_files = vec![
                global_index_file("btree", &format!("id-{i}"), 0, start, end),
                composite,
                global_index_file("btree", &format!("name-{i}"), 2, start, end),
                global_index_file("bitmap", &format!("bitmap-{i}"), 0, start, end),
                hash_index_file(&format!("hash-{i}")),
            ];
            messages.push(message);
        }
        TableCommit::new(table.clone(), "setup".into())
            .commit(messages)
            .await
            .unwrap();
        table
    }

    #[tokio::test]
    async fn test_drop_global_index_partition_filter_preserves_other_entries() {
        let table = partitioned_index_table("memory:/drop_partition_alternatives").await;
        let pb = crate::spec::PredicateBuilder::new(table.schema().fields());
        let filter = Predicate::or(vec![
            pb.partition_predicate(&[
                ("dt", Some(crate::spec::Datum::String("2026-10-10".into()))),
                ("hr", Some(crate::spec::Datum::Int(1))),
            ])
            .unwrap(),
            pb.partition_predicate(&[
                ("dt", Some(crate::spec::Datum::String("2026-10-11".into()))),
                ("hr", Some(crate::spec::Datum::Int(2))),
            ])
            .unwrap(),
        ]);
        for columns in ["id", "id,name"] {
            let before = table_files(&table).await;
            let previous_entries = latest_index_entries(&table).await;
            let snapshot_id = table
                .snapshot_manager()
                .get_latest_snapshot()
                .await
                .unwrap()
                .unwrap()
                .id();
            let mut builder = table.new_global_index_drop_builder();
            builder.with_index_column(columns).with_dry_run(true);
            builder.with_partition_filter(filter.clone()).unwrap();
            assert_eq!(builder.execute().await.unwrap(), 2);
            assert_eq!(table_files(&table).await, before);

            builder.with_dry_run(false);
            assert_eq!(builder.execute().await.unwrap(), 2);
            let prefix = if columns == "id" { "id" } else { "composite" };
            let expected = previous_entries
                .into_iter()
                .filter(|entry| {
                    ![format!("{prefix}-0"), format!("{prefix}-3")]
                        .contains(&entry.index_file.file_name)
                })
                .map(|entry| (entry.index_file.file_name.clone(), entry))
                .collect::<BTreeMap<_, _>>();
            let remaining = latest_index_entries(&table)
                .await
                .into_iter()
                .map(|entry| (entry.index_file.file_name.clone(), entry))
                .collect::<BTreeMap<_, _>>();
            assert_eq!(remaining, expected);
            assert_eq!(
                table
                    .snapshot_manager()
                    .get_latest_snapshot()
                    .await
                    .unwrap()
                    .unwrap()
                    .id(),
                snapshot_id + 1
            );
        }
    }

    #[tokio::test]
    async fn test_drop_global_index_partition_filters_combine_with_and() {
        let table = partitioned_index_table("memory:/drop_partition_and").await;
        let pb = crate::spec::PredicateBuilder::new(table.schema().fields());
        let mut builder = table.new_global_index_drop_builder();
        builder.with_index_column("id").with_dry_run(true);
        builder
            .with_partition_filter(
                pb.equal("dt", crate::spec::Datum::String("2026-10-10".into()))
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(builder.execute().await.unwrap(), 2);
        builder
            .with_partition_filter(pb.equal("hr", crate::spec::Datum::Int(2)).unwrap())
            .unwrap();
        assert_eq!(builder.execute().await.unwrap(), 1);
        builder.with_dry_run(false);
        assert_eq!(builder.execute().await.unwrap(), 1);
        let remaining = latest_index_entries(&table).await;
        assert!(!remaining
            .iter()
            .any(|entry| entry.index_file.file_name == "id-1"));
        assert!(remaining
            .iter()
            .any(|entry| entry.index_file.file_name == "id-0"));
    }

    #[tokio::test]
    async fn test_drop_global_index_partition_no_match_and_null() {
        let table = partitioned_index_table("memory:/drop_partition_no_match").await;
        let pb = crate::spec::PredicateBuilder::new(table.schema().fields());
        let before = table_files(&table).await;
        for dry_run in [true, false] {
            let mut builder = table.new_global_index_drop_builder();
            builder.with_index_column("id").with_dry_run(dry_run);
            builder
                .with_partition_filter(pb.equal("hr", crate::spec::Datum::Int(99)).unwrap())
                .unwrap();
            assert_eq!(builder.execute().await.unwrap(), 0);
            assert_eq!(table_files(&table).await, before);
        }
        let mut builder = table.new_global_index_drop_builder();
        builder.with_index_column("id");
        builder
            .with_partition_filter(pb.is_null("dt").unwrap())
            .unwrap();
        assert_eq!(builder.execute().await.unwrap(), 1);
        assert!(!latest_index_entries(&table)
            .await
            .iter()
            .any(|entry| entry.index_file.file_name == "id-4"));
    }

    #[tokio::test]
    async fn test_drop_global_index_partition_filter_rejects_data_columns() {
        let table = partitioned_index_table("memory:/drop_partition_invalid").await;
        let pb = crate::spec::PredicateBuilder::new(table.schema().fields());
        let before = table_files(&table).await;
        for filter in [
            pb.equal("id", crate::spec::Datum::Int(1)).unwrap(),
            Predicate::or(vec![pb.is_null("dt").unwrap(), pb.is_null("name").unwrap()]),
        ] {
            assert!(table
                .new_global_index_drop_builder()
                .with_partition_filter(filter)
                .is_err());
        }
        assert_eq!(table_files(&table).await, before);
        let unpartitioned = test_table("memory:/drop_unpartitioned_filter");
        assert!(unpartitioned
            .new_global_index_drop_builder()
            .with_partition_filter(Predicate::AlwaysTrue)
            .is_err());
    }

    #[tokio::test]
    async fn test_drop_global_index_partition_filter_rejects_invalid_rows() {
        for partition in [vec![0, 0], BinaryRow::new(0).to_serialized_bytes()] {
            let table = partitioned_index_table("memory:/drop_partition_invalid_row").await;
            let sm = table.snapshot_manager();
            let snapshot = sm.get_latest_snapshot().await.unwrap().unwrap();
            let path = sm.manifest_path(snapshot.index_manifest().unwrap());
            let mut entries = latest_index_entries(&table).await;
            entries
                .iter_mut()
                .find(|entry| entry.index_file.file_name == "id-0")
                .unwrap()
                .partition = partition;
            IndexManifest::write(table.file_io(), &path, &entries)
                .await
                .unwrap();
            let before = table_files(&table).await;
            for dry_run in [true, false] {
                let pb = crate::spec::PredicateBuilder::new(table.schema().fields());
                let mut builder = table.new_global_index_drop_builder();
                builder.with_index_column("id").with_dry_run(dry_run);
                builder
                    .with_partition_filter(pb.is_null("dt").unwrap())
                    .unwrap();
                assert!(builder.execute().await.is_err());
                assert_eq!(table_files(&table).await, before);
            }
        }
    }

    #[tokio::test]
    async fn test_drop_global_index_dry_run_preserves_files() {
        for index_type in [
            "btree",
            "bitmap",
            "multivalue",
            "fm",
            "full-text",
            "ivf-flat",
            "ivf-pq",
            "ivf-sq",
            "ivf-rq",
            "diskann",
        ] {
            let table = test_table(&format!("memory:/dry_run_{index_type}"));
            setup_dirs(&table).await;
            let mut message = CommitMessage::new(vec![], 0, vec![data_file("data.parquet")]);
            message.new_index_files = vec![
                global_index_file(index_type, "id-0.index", 0, 0, 9),
                global_index_file(index_type, "id-1.index", 0, 10, 19),
                global_index_file(index_type, "name.index", 1, 0, 19),
                hash_index_file("hash.index"),
            ];
            TableCommit::new(table.clone(), "test-user".into())
                .commit(vec![message])
                .await
                .unwrap();
            let before = table_files(&table).await;

            let mut builder = table.new_global_index_drop_builder();
            builder
                .with_index_column("id")
                .with_index_type(index_type)
                .with_dry_run(true);
            assert_eq!(builder.execute().await.unwrap(), 2, "{index_type}");
            assert_eq!(table_files(&table).await, before, "{index_type}");
            assert_eq!(latest_index_entries(&table).await.len(), 4);

            builder.with_dry_run(false);
            assert_eq!(builder.execute().await.unwrap(), 2, "{index_type}");
            assert_eq!(latest_index_entries(&table).await.len(), 2);
        }
    }

    #[tokio::test]
    async fn test_drop_global_index_dry_run_matches_full_column_list() {
        let table = test_table("memory:/dry_run_composite");
        setup_dirs(&table).await;
        let mut composite = global_index_file("btree", "id-name.index", 0, 0, 9);
        composite
            .global_index_meta
            .as_mut()
            .unwrap()
            .extra_field_ids = Some(vec![1]);
        let mut reversed = global_index_file("btree", "name-id.index", 1, 0, 9);
        reversed.global_index_meta.as_mut().unwrap().extra_field_ids = Some(vec![0]);
        let mut message = CommitMessage::new(vec![], 0, vec![data_file("data.parquet")]);
        message.new_index_files = vec![
            global_index_file("btree", "id.index", 0, 0, 9),
            composite,
            reversed,
            global_index_file("bitmap", "bitmap-id.index", 0, 0, 9),
        ];
        TableCommit::new(table.clone(), "test-user".into())
            .commit(vec![message])
            .await
            .unwrap();
        let before = table_files(&table).await;

        for columns in ["id", "id,name", "name,id"] {
            assert_eq!(
                table
                    .new_global_index_drop_builder()
                    .with_index_column(columns)
                    .with_dry_run(true)
                    .execute()
                    .await
                    .unwrap(),
                1,
                "{columns}"
            );
        }
        assert_eq!(table_files(&table).await, before);
    }

    #[tokio::test]
    async fn test_drop_global_index_dry_run_without_match() {
        for (case, index_files) in [
            ("no_snapshot", None),
            ("no_index_manifest", Some(vec![])),
            (
                "no_match",
                Some(vec![global_index_file("btree", "name.index", 1, 0, 9)]),
            ),
        ] {
            let table = test_table(&format!("memory:/dry_run_{case}"));
            setup_dirs(&table).await;
            if let Some(index_files) = index_files {
                let mut message = CommitMessage::new(vec![], 0, vec![data_file("data.parquet")]);
                message.new_index_files = index_files;
                TableCommit::new(table.clone(), "test-user".into())
                    .commit(vec![message])
                    .await
                    .unwrap();
            }
            let before = table_files(&table).await;
            assert_eq!(
                table
                    .new_global_index_drop_builder()
                    .with_index_column("id")
                    .with_dry_run(true)
                    .execute()
                    .await
                    .unwrap(),
                0,
                "{case}"
            );
            assert_eq!(table_files(&table).await, before, "{case}");
        }
    }

    #[tokio::test]
    async fn test_drop_global_index_dry_run_validates_columns() {
        let table = test_table("memory:/dry_run_invalid_columns");
        for column in [None, Some(""), Some(" "), Some(", ,"), Some("missing")] {
            let mut builder = table.new_global_index_drop_builder();
            builder.with_dry_run(true);
            if let Some(column) = column {
                builder.with_index_column(column);
            }
            assert!(builder.execute().await.is_err(), "{column:?}");
        }
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
            .new_global_index_drop_builder()
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
    async fn test_drop_full_text_global_index_only_removes_target_entries() {
        let table = test_table("memory:/test_drop_full_text_global_index");
        setup_dirs(&table).await;

        let mut message = CommitMessage::new(
            BinaryRow::new(0).to_serialized_bytes(),
            0,
            vec![data_file("data-0.parquet")],
        );
        message.new_index_files = vec![
            global_index_file("full-text", "full-text-name-0.index", 1, 0, 9),
            global_index_file("full-text", "full-text-name-1.index", 1, 10, 19),
            global_index_file("full-text", "full-text-id.index", 0, 0, 9),
            global_index_file(BTREE_GLOBAL_INDEX_TYPE, "btree-name.index", 1, 0, 9),
        ];
        TableCommit::new(table.clone(), "test-user".to_string())
            .commit(vec![message])
            .await
            .unwrap();

        let dropped = table
            .new_global_index_drop_builder()
            .with_index_column("name")
            .with_index_type("Full-Text")
            .execute()
            .await
            .unwrap();
        assert_eq!(dropped, 2);

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
                "full-text-id.index".to_string(),
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
            .new_global_index_drop_builder()
            .with_index_column("id")
            .execute()
            .await
            .unwrap();
        assert_eq!(dropped, 0);

        let remaining = latest_index_entries(&table).await;
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].index_file.file_name, "btree-name.index");
    }

    #[tokio::test]
    async fn test_drop_vindex_preserves_other_types_case_insensitive() {
        let table = test_table("memory:/test_drop_vindex_identity");
        setup_dirs(&table).await;

        let mut message = CommitMessage::new(
            BinaryRow::new(0).to_serialized_bytes(),
            0,
            vec![data_file("data-0.parquet")],
        );
        message.new_index_files = vec![
            global_index_file("ivf-flat", "ivf-flat-id.index", 0, 0, 9),
            global_index_file("ivf-pq", "ivf-pq-id.index", 0, 0, 9),
            global_index_file("diskann", "diskann-id.index", 0, 0, 9),
            global_index_file(BTREE_GLOBAL_INDEX_TYPE, "btree-id.index", 0, 0, 9),
        ];
        TableCommit::new(table.clone(), "test-user".to_string())
            .commit(vec![message])
            .await
            .unwrap();

        // Uppercase request must canonicalize and drop ONLY ivf-flat.
        let dropped = table
            .new_global_index_drop_builder()
            .with_index_column("id")
            .with_index_type("IVF-FLAT")
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
                "btree-id.index".to_string(),
                "diskann-id.index".to_string(),
                "ivf-pq-id.index".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn test_drop_unsupported_type_errors() {
        let table = test_table("memory:/test_drop_unsupported");
        setup_dirs(&table).await;

        let err = table
            .new_global_index_drop_builder()
            .with_index_column("id")
            .with_index_type("hash")
            .execute()
            .await
            .expect_err("unsupported type must error");
        assert!(matches!(
            err,
            Error::Unsupported { message }
                if message.contains("unsupported global index type") && message.contains("ivf-pq")
        ));
    }
}
