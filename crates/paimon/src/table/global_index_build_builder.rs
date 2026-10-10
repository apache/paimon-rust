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

use std::collections::HashMap;
use std::time::Instant;

#[cfg(feature = "fulltext")]
use super::full_text_index_build_builder::FullTextIndexBuildBuilder;
use super::global_index_build_common::{resolve_index_fields, IndexColumns};
use super::global_index_types::{
    normalize_global_index_type, normalize_queryable_global_index_type, BTREE_GLOBAL_INDEX_TYPE,
    FULL_TEXT_GLOBAL_INDEX_TYPE,
};
use super::lumina_index_build_builder::LuminaIndexBuildBuilder;
use super::sorted_global_index_build_builder::SortedGlobalIndexBuildBuilder;
use super::vindex_index_build_builder::timing::{
    vector_index_build_timing_enabled, VectorIndexBuildTiming,
};
use super::vindex_index_build_builder::VindexIndexBuildBuilder;
use super::{CommitMessage, Table, TableCommit};
use crate::lumina::LUMINA_IDENTIFIER;
use crate::spec::{CoreOptions, Predicate};
use crate::{Error, Result};

/// Build global indexes by type, like Java's `GlobalIndexer.create`.
/// Sorting and native writer selection are internal implementation details.
pub struct GlobalIndexBuildBuilder<'a> {
    table: &'a Table,
    columns: Option<IndexColumns>,
    index_type: String,
    options: HashMap<String, String>,
    partition_filter: Option<Predicate>,
}

struct PreparedBuild {
    snapshot_id: Option<i64>,
    messages: Vec<CommitMessage>,
    vector_timings: Vec<VectorIndexBuildTiming>,
}

impl From<(Option<i64>, Vec<CommitMessage>)> for PreparedBuild {
    fn from((snapshot_id, messages): (Option<i64>, Vec<CommitMessage>)) -> Self {
        Self {
            snapshot_id,
            messages,
            vector_timings: vec![],
        }
    }
}

impl<'a> GlobalIndexBuildBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            columns: None,
            index_type: BTREE_GLOBAL_INDEX_TYPE.into(),
            options: HashMap::new(),
            partition_filter: None,
        }
    }

    /// Whether this build includes a writer for the requested index type.
    pub fn supports_index_type(index_type: &str) -> bool {
        normalize_global_index_type(index_type.trim()).is_some_and(|index_type| {
            index_type != FULL_TEXT_GLOBAL_INDEX_TYPE || cfg!(feature = "fulltext")
        })
    }

    pub fn with_index_column(&mut self, column: &str) -> &mut Self {
        self.columns = Some(IndexColumns::Column(column.into()));
        self
    }

    /// Select columns in physical tuple-key order; only BTree supports multiple columns.
    pub fn with_index_columns(&mut self, columns: &[&str]) -> &mut Self {
        self.columns = Some(IndexColumns::Columns(
            columns.iter().map(|column| (*column).into()).collect(),
        ));
        self
    }

    pub fn with_index_type(&mut self, index_type: &str) -> &mut Self {
        self.index_type = index_type.into();
        self
    }

    /// Overrides table options for this build. Full-text and vindex builds use
    /// `global-index.build.parallelism` to bound concurrent shards (default: 1).
    pub fn with_options(&mut self, options: HashMap<String, String>) -> &mut Self {
        self.options = options;
        self
    }

    /// Restrict the build to partitions. Repeated calls combine filters with AND.
    pub fn with_partition_filter(&mut self, filter: Predicate) -> Result<&mut Self> {
        super::partition_filter::validate_partition_filter(self.table, &filter)?;
        self.partition_filter = Some(match self.partition_filter.take() {
            Some(previous) => Predicate::and(vec![previous, filter]),
            None => filter,
        });
        Ok(self)
    }

    /// Prepare files without publishing a snapshot. The caller owns the returned messages.
    /// No snapshot or no uncovered rows produces an empty list.
    pub async fn build(&self) -> Result<Vec<CommitMessage>> {
        Ok(self.prepare().await?.messages)
    }

    /// Prepare and commit, rejecting changes to the snapshot used for the build.
    pub async fn execute(&self) -> Result<usize> {
        let prepared = self.prepare().await?;
        if prepared.messages.is_empty() {
            return Ok(0);
        }
        let count = prepared
            .messages
            .iter()
            .map(|message| message.new_index_files.len())
            .sum();
        let commit_start = vector_index_build_timing_enabled().then(Instant::now);
        TableCommit::new(
            self.table.clone(),
            format!("global-index-create-{}", uuid::Uuid::new_v4()),
        )
        .commit_if_latest_snapshot(
            prepared.messages,
            prepared
                .snapshot_id
                .expect("nonempty index build has a snapshot"),
        )
        .await?;
        if let Some(start) = commit_start {
            let elapsed = start.elapsed();
            for timing in prepared.vector_timings {
                timing.log(self.index_type.trim(), elapsed);
            }
        }
        // Once submitted, messages may have been committed even if an error is returned.
        // Never abort their files on a failed or uncertain commit.
        Ok(count)
    }

    async fn prepare(&self) -> Result<PreparedBuild> {
        CoreOptions::new(self.table.schema().options()).ensure_read_authorized()?;
        self.table.ensure_not_branch_reference_for_write()?;
        let index_type = normalize_global_index_type(self.index_type.trim()).ok_or_else(|| {
            Error::Unsupported {
                message: format!("Unsupported global index type: '{}'", self.index_type),
            }
        })?;
        let columns = self.columns.as_ref().ok_or_else(|| Error::DataInvalid {
            message: "Global index column is required".into(),
            source: None,
        })?;
        let fields = resolve_index_fields(self.table, columns, index_type)?;
        let columns = fields.iter().map(|field| field.name()).collect::<Vec<_>>();
        if normalize_queryable_global_index_type(index_type).is_some() {
            let mut builder = SortedGlobalIndexBuildBuilder::new(self.table);
            builder
                .with_index_columns(&columns)
                .with_index_type(index_type)
                .with_options(self.options.clone());
            if let Some(filter) = &self.partition_filter {
                builder.with_partition_filter(filter.clone())?;
            }
            return Ok(builder.prepare().await?.into());
        }
        if index_type == FULL_TEXT_GLOBAL_INDEX_TYPE {
            #[cfg(feature = "fulltext")]
            {
                let mut builder = FullTextIndexBuildBuilder::new(self.table);
                builder
                    .with_index_column(columns[0])
                    .with_options(self.options.clone());
                builder.partition_filter = self.partition_filter.clone();
                return Ok(builder.prepare().await?.into());
            }
            #[cfg(not(feature = "fulltext"))]
            return Err(Error::Unsupported {
                message: "Full-text global index build requires the 'fulltext' feature".into(),
            });
        }
        if index_type == LUMINA_IDENTIFIER {
            let mut builder = LuminaIndexBuildBuilder::new(self.table);
            builder
                .with_index_column(columns[0])
                .with_options(self.options.clone());
            builder.partition_filter = self.partition_filter.clone();
            return Ok(builder.prepare().await?.into());
        }
        let mut builder = VindexIndexBuildBuilder::new(self.table, index_type);
        builder
            .with_index_column(columns[0])
            .with_options(self.options.clone());
        builder.partition_filter = self.partition_filter.clone();
        let (snapshot_id, messages, vector_timings) = builder.prepare().await?;
        Ok(PreparedBuild {
            snapshot_id,
            messages,
            vector_timings,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Identifier;
    use crate::io::FileIOBuilder;
    use crate::spec::{ArrayType, DataType, FloatType, IntType, Schema, TableSchema, VarCharType};

    fn empty_table() -> Table {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("text", DataType::VarChar(VarCharType::string_type()))
            .column(
                "vector",
                DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
            )
            .options(HashMap::from([
                ("row-tracking.enabled".into(), "true".into()),
                ("data-evolution.enabled".into(), "true".into()),
                ("global-index.enabled".into(), "true".into()),
            ]))
            .build()
            .unwrap();
        Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("default", "empty"),
            "memory:/empty_global_index".into(),
            TableSchema::new(0, &schema),
            None,
        )
    }

    #[tokio::test]
    async fn all_index_families_share_empty_build_and_execute_contract() {
        let table = empty_table();
        for (kind, column) in [
            ("btree", "id"),
            ("bitmap", "id"),
            ("multivalue", "vector"),
            ("fm", "text"),
            ("full-text", "text"),
            ("lumina", "vector"),
            ("lumina-vector-ann", "vector"),
            ("ivf-flat", "vector"),
            ("ivf-pq", "vector"),
            ("ivf-sq", "vector"),
            ("ivf-rq", "vector"),
            ("diskann", "vector"),
        ] {
            if !GlobalIndexBuildBuilder::supports_index_type(kind) {
                continue;
            }
            let mut options = HashMap::from([(
                format!(
                    "{}.dimension",
                    if kind == "lumina-vector-ann" {
                        "lumina"
                    } else {
                        kind
                    }
                ),
                "2".into(),
            )]);
            if kind == "ivf-pq" {
                options.insert("ivf-pq.pq.m".into(), "1".into());
            }
            options.insert("global-index.row-count-per-shard".into(), "4".into());
            let mut builder = table.new_global_index_build_builder();
            builder
                .with_index_column(column)
                .with_index_type(&format!(" {} ", kind.to_uppercase()))
                .with_options(options);
            assert!(builder.build().await.unwrap().is_empty(), "{kind}");
            assert_eq!(builder.execute().await.unwrap(), 0, "{kind}");
        }
        assert!(table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn column_validation_precedes_any_index_io() {
        let table = empty_table();
        for kind in ["bitmap", "fm", "full-text", "ivf-flat", "lumina"] {
            let mut builder = table.new_global_index_build_builder();
            builder
                .with_index_columns(&["id", "text"])
                .with_index_type(kind);
            assert!(
                matches!(builder.build().await, Err(Error::Unsupported { message }) if message.contains("multiple index columns"))
            );
        }
        for columns in [vec![], vec![""], vec!["id", "id"]] {
            assert!(table
                .new_global_index_build_builder()
                .with_index_columns(&columns)
                .build()
                .await
                .is_err());
        }
        assert!(matches!(
            table
                .new_global_index_build_builder()
                .with_index_column("missing")
                .build()
                .await,
            Err(Error::ColumnNotExist { .. })
        ));
        assert!(matches!(
            table
                .new_global_index_build_builder()
                .with_index_type("unknown")
                .build()
                .await,
            Err(Error::Unsupported { .. })
        ));
        assert!(!GlobalIndexBuildBuilder::supports_index_type("unknown"));
    }

    #[cfg(not(feature = "fulltext"))]
    #[tokio::test]
    async fn full_text_without_feature_is_explicitly_unsupported() {
        assert!(!GlobalIndexBuildBuilder::supports_index_type("full-text"));
        assert!(
            matches!(empty_table().new_global_index_build_builder().with_index_column("text")
            .with_index_type("full-text").build().await, Err(Error::Unsupported { message }) if message.contains("fulltext"))
        );
    }
}

#[cfg(test)]
#[path = "global_index_build_builder/parallel_tests.rs"]
mod parallel_tests;
