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

//! Key-value file reader for primary-key tables using sort-merge with LoserTree.
//!
//! Each data file in a split is read as a separate sorted stream. The streams
//! are merged by primary key using a LoserTree, and rows with the same key are
//! deduplicated by keeping the one with the highest `_SEQUENCE_NUMBER`.
//! Non-primary-key predicate conjuncts are enforced by an exact post-merge
//! residual filter; only primary-key conjuncts are pushed below the merge.
//!
//! Reference: Java Paimon `SortMergeReaderWithMinHeap`.

use super::data_file_reader::DataFileReader;
use super::sort_merge::{
    AggregateMergeFunction, DeduplicateMergeFunction, PartialUpdateMergeFunction,
    SortMergeReaderBuilder,
};
use crate::arrow::build_target_arrow_schema;
use crate::io::FileIO;
use crate::spec::{
    BigIntType, DataField, DataType as PaimonDataType, MergeEngine, Predicate, TinyIntType,
    SEQUENCE_NUMBER_FIELD_ID, SEQUENCE_NUMBER_FIELD_NAME, VALUE_KIND_FIELD_ID,
    VALUE_KIND_FIELD_NAME,
};
use crate::table::schema_manager::SchemaManager;
use crate::table::ArrowRecordBatchStream;
use crate::{DataSplit, Error};
use arrow_array::{RecordBatch, RecordBatchOptions};

use async_stream::try_stream;
use futures::StreamExt;
use std::collections::HashMap;

/// Reads primary-key table data files using sort-merge deduplication.
pub(crate) struct KeyValueFileReader {
    file_io: FileIO,
    config: KeyValueReadConfig,
    /// PK-only conjuncts pushed down to the per-file readers before merge.
    /// Non-PK conjuncts must not run pre-merge (they can change which version
    /// of a key survives); they are enforced by the post-merge residual
    /// filter using the full `config.predicates` instead.
    pushdown_predicates: Vec<Predicate>,
}

/// Configuration for [`KeyValueFileReader`], grouping table schema and
/// key/predicate parameters.
pub(crate) struct KeyValueReadConfig {
    pub table_name: String,
    pub table_options: HashMap<String, String>,
    pub schema_manager: SchemaManager,
    pub table_schema_id: i64,
    pub table_fields: Vec<DataField>,
    pub read_type: Vec<DataField>,
    pub predicates: Vec<Predicate>,
    pub primary_keys: Vec<String>,
    pub merge_engine: MergeEngine,
    pub sequence_fields: Vec<String>,
}

/// Keep only the conjuncts of `predicates` that reference primary-key columns,
/// preserving table-schema field indices. Mixed `AND`s keep their PK children;
/// `OR`/`NOT` require every child to be PK-only (see
/// [`Predicate::project_field_index_inclusive`]).
///
/// Used for pre-merge pushdown in [`KeyValueFileReader`] and for per-file
/// stats pruning of primary-key tables in scan planning: a key's versions all
/// share the key columns, so key conjuncts can never drop one version of a
/// key while keeping another — non-key conjuncts can, which corrupts merge.
pub(super) fn retain_primary_key_conjuncts(
    predicates: &[Predicate],
    table_fields: &[DataField],
    primary_keys: &[String],
) -> Vec<Predicate> {
    let pk_set: std::collections::HashSet<&str> = primary_keys.iter().map(|s| s.as_str()).collect();
    let mapping: Vec<Option<usize>> = table_fields
        .iter()
        .enumerate()
        .map(|(i, f)| {
            if pk_set.contains(f.name()) {
                Some(i)
            } else {
                None
            }
        })
        .collect();
    predicates
        .iter()
        .filter_map(|p| p.project_field_index_inclusive(&mapping))
        .collect()
}

impl KeyValueFileReader {
    pub(crate) fn new(file_io: FileIO, config: KeyValueReadConfig) -> Self {
        let pushdown_predicates = retain_primary_key_conjuncts(
            &config.predicates,
            &config.table_fields,
            &config.primary_keys,
        );
        Self {
            file_io,
            config,
            pushdown_predicates,
        }
    }

    fn new_merge_function(
        merge_engine: MergeEngine,
        table_options: &HashMap<String, String>,
        table_name: &str,
        merge_output_fields: &[DataField],
        primary_keys: &[String],
        sequence_fields: &[String],
    ) -> crate::Result<Box<dyn super::sort_merge::MergeFunction>> {
        match merge_engine {
            MergeEngine::Deduplicate => Ok(Box::new(DeduplicateMergeFunction)),
            MergeEngine::PartialUpdate => Ok(Box::new(PartialUpdateMergeFunction::new(
                table_options,
                table_name,
            )?)),
            MergeEngine::FirstRow => Err(Error::Unsupported {
                message: "KeyValueFileReader does not support merge-engine=first-row; first-row reads should use the non-KV path".to_string(),
            }),
            MergeEngine::Aggregation => Ok(Box::new(AggregateMergeFunction::new(
                table_options,
                table_name,
                merge_output_fields,
                primary_keys,
                sequence_fields,
            )?)),
        }
    }

    pub fn read(self, data_splits: &[DataSplit]) -> crate::Result<ArrowRecordBatchStream> {
        // Build the internal read type for thin-mode files.
        // Physical file schema: [_SEQUENCE_NUMBER, _VALUE_KIND, all_user_cols...]
        // We need: _SEQ + _VK + union(read_type, primary_keys)
        let seq_field = DataField::new(
            SEQUENCE_NUMBER_FIELD_ID,
            SEQUENCE_NUMBER_FIELD_NAME.to_string(),
            PaimonDataType::BigInt(BigIntType::new()),
        );
        let value_kind_field = DataField::new(
            VALUE_KIND_FIELD_ID,
            VALUE_KIND_FIELD_NAME.to_string(),
            PaimonDataType::TinyInt(TinyIntType::new()),
        );

        let key_names: std::collections::HashSet<&str> = self
            .config
            .primary_keys
            .iter()
            .map(|s| s.as_str())
            .collect();

        // Collect key fields from table schema.
        let key_fields: Vec<DataField> = self
            .config
            .primary_keys
            .iter()
            .map(|pk| {
                self.config
                    .table_fields
                    .iter()
                    .find(|f| f.name() == pk)
                    .cloned()
                    .ok_or_else(|| Error::UnexpectedError {
                        message: format!("Primary key column '{pk}' not found in table schema"),
                        source: None,
                    })
            })
            .collect::<crate::Result<Vec<_>>>()?;

        // User columns = read_type fields + any key fields not already in read_type
        //              + any sequence fields not already included.
        let read_type_names: std::collections::HashSet<&str> =
            self.config.read_type.iter().map(|f| f.name()).collect();
        let mut user_fields: Vec<DataField> = self.config.read_type.clone();
        for kf in &key_fields {
            if !read_type_names.contains(kf.name()) {
                user_fields.push(kf.clone());
            }
        }
        // Add sequence fields if not already present.
        for sf_name in &self.config.sequence_fields {
            if user_fields.iter().all(|f| f.name() != sf_name.as_str()) {
                let sf = self
                    .config
                    .table_fields
                    .iter()
                    .find(|f| f.name() == sf_name.as_str())
                    .cloned()
                    .ok_or_else(|| Error::UnexpectedError {
                        message: format!("Sequence field '{sf_name}' not found in table schema"),
                        source: None,
                    })?;
                user_fields.push(sf);
            }
        }

        // Widen with predicate columns not already read so the post-merge
        // residual filter can evaluate every leaf (predicate leaf indices are
        // table-schema positions). Extras ride through the merge as ordinary
        // value columns — partial-update/aggregation apply their configured
        // per-field semantics to them, so the residual sees properly MERGED
        // values — and the read_type reorder below drops them from the output.
        let residual_file_predicates =
            (!self.config.predicates.is_empty()).then(|| crate::arrow::format::FilePredicates {
                predicates: self.config.predicates.clone(),
                file_fields: self.config.table_fields.clone(),
            });
        let user_fields = crate::arrow::residual::widen_scan_fields(
            &user_fields,
            residual_file_predicates.as_ref(),
        );

        // Internal read type: [_SEQ, _VK, user_fields...]
        let mut internal_read_type: Vec<DataField> = Vec::new();
        internal_read_type.push(seq_field);
        internal_read_type.push(value_kind_field);
        internal_read_type.extend(user_fields.clone());

        let internal_schema = build_target_arrow_schema(&internal_read_type)?;

        // Output schema: user's read_type order
        let output_schema = build_target_arrow_schema(&self.config.read_type)?;

        // Indices within internal_schema (offset 2 for _SEQ and _VK).
        let seq_index = 0;
        let value_kind_index = 1;
        let key_indices: Vec<usize> = self
            .config
            .primary_keys
            .iter()
            .map(|pk| {
                user_fields
                    .iter()
                    .position(|f| f.name() == pk)
                    .map(|p| p + 2)
                    .unwrap()
            })
            .collect();
        let value_fields: Vec<DataField> = user_fields
            .iter()
            .filter(|f| !key_names.contains(f.name()))
            .cloned()
            .collect();
        let value_indices: Vec<usize> = user_fields
            .iter()
            .enumerate()
            .filter(|(_, f)| !key_names.contains(f.name()))
            .map(|(i, _)| i + 2)
            .collect();

        // If sequence.field is configured, find each field's index in the internal schema.
        let user_sequence_indices: Vec<usize> = self
            .config
            .sequence_fields
            .iter()
            .filter_map(|sf| {
                user_fields
                    .iter()
                    .position(|f| f.name() == sf.as_str())
                    .map(|p| p + 2)
            })
            .collect();

        // Build the reorder mapping: merge output is [keys..., values...],
        // but user wants them in read_type order.
        let num_keys = key_fields.len();
        let mut reorder_map: Vec<usize> = vec![0; self.config.read_type.len()];
        for (out_idx, field) in self.config.read_type.iter().enumerate() {
            if key_names.contains(field.name()) {
                // Find position in key_fields
                let key_pos = key_fields
                    .iter()
                    .position(|kf| kf.name() == field.name())
                    .unwrap();
                reorder_map[out_idx] = key_pos;
            } else {
                // Find position in value_fields
                let val_pos = value_fields
                    .iter()
                    .position(|vf| vf.name() == field.name())
                    .unwrap();
                reorder_map[out_idx] = num_keys + val_pos;
            }
        }

        let splits: Vec<DataSplit> = data_splits.to_vec();
        let file_io = self.file_io;
        let merge_engine = self.config.merge_engine;
        let schema_manager = self.config.schema_manager;
        let table_schema_id = self.config.table_schema_id;
        let table_fields = self.config.table_fields;
        let table_name = self.config.table_name;
        let table_options = self.config.table_options;
        let pushdown_predicates = self.pushdown_predicates;
        let residual_predicates = self.config.predicates;
        let primary_keys = self.config.primary_keys;
        let sequence_fields = self.config.sequence_fields;

        // Build the merge output schema (keys + values, no system columns).
        let mut merge_output_fields: Vec<DataField> = Vec::new();
        merge_output_fields.extend(key_fields);
        merge_output_fields.extend(value_fields);
        let merge_output_schema = build_target_arrow_schema(&merge_output_fields)?;

        Ok(try_stream! {
            for split in &splits {
                // DV mode should not reach KeyValueFileReader.
                if split
                    .data_deletion_files()
                    .is_some_and(|files| files.iter().any(Option::is_some))
                {
                    Err(Error::Unsupported {
                        message: "KeyValueFileReader does not support deletion vectors".to_string(),
                    })?;
                }

                // Create one stream per data file.
                let mut file_streams: Vec<ArrowRecordBatchStream> = Vec::new();

                for file_meta in split.data_files().to_vec() {
                    let data_fields: Option<Vec<DataField>> = if file_meta.schema_id != table_schema_id {
                        let data_schema = schema_manager.schema(file_meta.schema_id).await?;
                        Some(data_schema.fields().to_vec())
                    } else {
                        None
                    };

                    let reader = DataFileReader::new(
                        file_io.clone(),
                        schema_manager.clone(),
                        table_schema_id,
                        table_fields.clone(),
                        internal_read_type.clone(),
                        pushdown_predicates.clone(),
                    );

                    let stream = reader.read_single_file_stream(
                        split,
                        file_meta,
                        data_fields,
                        None,
                        None,
                    )?;
                    file_streams.push(stream);
                }

                if file_streams.is_empty() {
                    continue;
                }

                // Always go through sort-merge even for a single file: files
                // written before the writer merged key groups at flush may
                // still contain duplicate keys.
                let mut merge_stream = SortMergeReaderBuilder::new(
                    file_streams,
                    internal_schema.clone(),
                    key_indices.clone(),
                    seq_index,
                    value_kind_index,
                    user_sequence_indices.clone(),
                    value_indices.clone(),
                    merge_output_schema.clone(),
                    Self::new_merge_function(
                        merge_engine,
                        &table_options,
                        &table_name,
                        &merge_output_fields,
                        &primary_keys,
                        &sequence_fields,
                    )?,
                )
                .build()?;

                while let Some(batch) = merge_stream.next().await {
                    let batch = batch?;
                    // Post-merge residual: enforce the FULL data predicate on
                    // merged rows. PK conjuncts are also in this set (they were
                    // already pushed down pre-merge); re-evaluating them on
                    // already-matching rows is a no-op and keeps one shared
                    // evaluator instead of deriving a non-PK subset. Runs on
                    // the merge-output batch (keys + values, including widened
                    // predicate columns); the reorder below projects the
                    // output back to read_type.
                    let batch = if residual_predicates.is_empty() {
                        batch
                    } else {
                        match crate::arrow::residual::evaluate_predicates_mask(
                            &batch,
                            &residual_predicates,
                            &table_fields,
                            &merge_output_fields,
                        )? {
                            Some(mask) => {
                                arrow_select::filter::filter_record_batch(&batch, &mask).map_err(
                                    |e| Error::DataInvalid {
                                        message: format!(
                                            "Failed to filter merged batch by predicates: {e}"
                                        ),
                                        source: Some(Box::new(e)),
                                    },
                                )?
                            }
                            None => batch,
                        }
                    };
                    // Reorder columns from [keys..., values...] to read_type order.
                    let columns: Vec<_> = reorder_map
                        .iter()
                        .map(|&src| batch.column(src).clone())
                        .collect();
                    // An explicit row count keeps empty projections working
                    // (e.g. COUNT(*) reads no columns).
                    let options =
                        RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
                    let reordered =
                        RecordBatch::try_new_with_options(output_schema.clone(), columns, &options)
                            .map_err(|e| Error::UnexpectedError {
                                message: format!("Failed to reorder merged RecordBatch: {e}"),
                                source: Some(Box::new(e)),
                            })?;
                    yield reordered;
                }
            }
        }
        .boxed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Identifier;
    use crate::io::FileIOBuilder;
    use crate::spec::{DataType, Datum, IntType, PredicateBuilder, Schema, TableSchema};
    use crate::table::table_commit::TableCommit;
    use crate::table::{Table, TableWrite};
    use arrow_array::{Array, Int32Array};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use std::sync::Arc;

    fn test_file_io() -> FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    fn pk_table(file_io: &FileIO, table_path: &str, options: &[(&str, &str)]) -> Table {
        let mut builder = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .primary_key(["id"])
            .option("bucket", "1");
        for (key, value) in options {
            builder = builder.option(*key, *value);
        }
        Table::new(
            file_io.clone(),
            Identifier::new("default", "kv_residual_t"),
            table_path.to_string(),
            TableSchema::new(0, &builder.build().unwrap()),
            None,
        )
    }

    async fn setup_dirs(file_io: &FileIO, table_path: &str) {
        file_io
            .mkdirs(&format!("{table_path}/snapshot/"))
            .await
            .unwrap();
        file_io
            .mkdirs(&format!("{table_path}/manifest/"))
            .await
            .unwrap();
    }

    fn int_batch(ids: Vec<i32>, values: Vec<Option<i32>>) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("value", ArrowDataType::Int32, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int32Array::from(values)),
            ],
        )
        .unwrap()
    }

    fn evo_batch(ids: Vec<i32>, values: Vec<Option<i32>>, scores: Vec<Option<i32>>) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("value", ArrowDataType::Int32, true),
            ArrowField::new("score", ArrowDataType::Int32, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int32Array::from(values)),
                Arc::new(Int32Array::from(scores)),
            ],
        )
        .unwrap()
    }

    /// User schema for the evolution fixture: `id INT pk, value INT` at
    /// version 0, plus `score INT` (new field id 2) at version 1. Field ids
    /// line up across versions exactly as a real ADD COLUMN produces.
    fn evo_user_schema(with_score: bool) -> Schema {
        let mut builder = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()));
        if with_score {
            builder = builder.column("score", DataType::Int(IntType::new()));
        }
        builder
            .primary_key(["id"])
            .option("bucket", "1")
            .build()
            .unwrap()
    }

    /// Persist a schema version as `{table_path}/schema/schema-{id}` JSON so
    /// `SchemaManager::schema` can resolve old-file schemas at read time. The
    /// write path only stamps `DataFileMeta.schema_id`; schema files are
    /// normally written by the catalog, which these fixtures bypass. Follows
    /// the `write_schema_file` pattern from the table_scan tests.
    async fn write_schema_file(table: &Table, schema: &TableSchema) {
        let path = table.schema_manager().schema_path(schema.id());
        let dir = path.rsplit_once('/').map(|(dir, _)| dir).unwrap();
        table.file_io().mkdirs(dir).await.unwrap();
        let json = serde_json::to_vec(schema).unwrap();
        table
            .file_io()
            .new_output(&path)
            .unwrap()
            .write(bytes::Bytes::from(json))
            .await
            .unwrap();
    }

    async fn write_commit(table: &Table, batch: &RecordBatch) {
        let mut tw = TableWrite::new(table, "test-user".to_string()).unwrap();
        tw.write_arrow_batch(batch).await.unwrap();
        let msgs = tw.prepare_commit().await.unwrap();
        TableCommit::new(table.clone(), "test-user".to_string())
            .commit(msgs)
            .await
            .unwrap();
    }

    async fn read_rows(
        table: &Table,
        projection: Option<&[&str]>,
        filter: Option<Predicate>,
    ) -> Vec<RecordBatch> {
        let mut rb = table.new_read_builder();
        if let Some(cols) = projection {
            rb.with_projection(cols).unwrap();
        }
        if let Some(f) = filter {
            rb.with_filter(f);
        }
        let plan = rb.new_scan().plan().await.unwrap();
        let read = rb.new_read().unwrap();
        futures::TryStreamExt::try_collect(read.to_arrow(plan.splits()).unwrap())
            .await
            .unwrap()
    }

    fn int_column(batches: &[RecordBatch], name: &str) -> Vec<i32> {
        batches
            .iter()
            .flat_map(|b| {
                let idx = b.schema().index_of(name).unwrap();
                let arr = b.column(idx).as_any().downcast_ref::<Int32Array>().unwrap();
                (0..arr.len()).map(|i| arr.value(i)).collect::<Vec<_>>()
            })
            .collect()
    }

    #[test]
    fn retain_primary_key_conjuncts_semantics() {
        let fields = vec![
            DataField::new(0, "id".to_string(), PaimonDataType::Int(IntType::new())),
            DataField::new(1, "value".to_string(), PaimonDataType::Int(IntType::new())),
        ];
        let pks = vec!["id".to_string()];
        let pb = PredicateBuilder::new(&fields);

        // Plain PK leaf: kept. Plain non-PK leaf: dropped.
        let kept =
            retain_primary_key_conjuncts(&[pb.equal("id", Datum::Int(1)).unwrap()], &fields, &pks);
        assert_eq!(kept.len(), 1);
        let dropped = retain_primary_key_conjuncts(
            &[pb.equal("value", Datum::Int(1)).unwrap()],
            &fields,
            &pks,
        );
        assert!(dropped.is_empty());

        // Mixed AND keeps the PK child only.
        let mixed = Predicate::and(vec![
            pb.equal("id", Datum::Int(1)).unwrap(),
            pb.equal("value", Datum::Int(2)).unwrap(),
        ]);
        let kept = retain_primary_key_conjuncts(&[mixed], &fields, &pks);
        assert_eq!(kept.len(), 1);
        assert!(matches!(&kept[0], Predicate::Leaf { index: 0, .. }));

        // OR with a non-PK child: dropped entirely (cannot be tightened).
        let or = Predicate::or(vec![
            pb.equal("id", Datum::Int(1)).unwrap(),
            pb.equal("value", Datum::Int(2)).unwrap(),
        ]);
        assert!(retain_primary_key_conjuncts(&[or], &fields, &pks).is_empty());

        // Constant predicates reference no columns and must survive the PK
        // trim verbatim. The post-merge residual (full predicate set) would
        // still mask every row to false if AlwaysFalse were dropped here, but
        // the scan/pushdown layers would lose their prune-everything fast
        // path (stats_filter treats any AlwaysFalse as prune-all).
        let kept = retain_primary_key_conjuncts(&[Predicate::AlwaysFalse], &fields, &pks);
        assert_eq!(kept.len(), 1);
        assert!(matches!(&kept[0], Predicate::AlwaysFalse));
        let kept = retain_primary_key_conjuncts(&[Predicate::AlwaysTrue], &fields, &pks);
        assert_eq!(kept.len(), 1);
        assert!(matches!(&kept[0], Predicate::AlwaysTrue));
    }

    /// Non-PK equality filter on a dedup PK table read through the sort-merge
    /// path must return only matching rows. Before the post-merge residual,
    /// the non-PK conjunct was silently dropped and all rows came back.
    #[tokio::test]
    async fn kv_read_applies_non_pk_filter_exactly() {
        let file_io = test_file_io();
        let table_path = "memory:/kv_residual_eq";
        setup_dirs(&file_io, table_path).await;
        let table = pk_table(&file_io, table_path, &[]);

        // Overlapping keys across two commits -> split is not raw convertible
        // -> forced through KeyValueFileReader.
        write_commit(
            &table,
            &int_batch(vec![1, 2, 3], vec![Some(10), Some(20), Some(30)]),
        )
        .await;
        write_commit(
            &table,
            &int_batch(vec![1, 2, 3], vec![Some(11), Some(21), Some(31)]),
        )
        .await;

        let fields = table.schema().fields().to_vec();
        let filter = PredicateBuilder::new(&fields)
            .equal("value", Datum::Int(21))
            .unwrap();
        let batches = read_rows(&table, None, Some(filter)).await;

        assert_eq!(int_column(&batches, "id"), vec![2]);
        assert_eq!(int_column(&batches, "value"), vec![21]);
    }

    /// Gap-A: the predicate column is NOT in the projection. The merge read
    /// must widen internally, filter, then project back — output schema must
    /// contain only the projected column.
    #[tokio::test]
    async fn kv_read_filters_on_unprojected_column() {
        let file_io = test_file_io();
        let table_path = "memory:/kv_residual_gap_a";
        setup_dirs(&file_io, table_path).await;
        let table = pk_table(&file_io, table_path, &[]);

        write_commit(
            &table,
            &int_batch(vec![1, 2, 3], vec![Some(10), Some(20), Some(30)]),
        )
        .await;
        write_commit(
            &table,
            &int_batch(vec![1, 2, 3], vec![Some(11), Some(21), Some(31)]),
        )
        .await;

        let fields = table.schema().fields().to_vec();
        let filter = PredicateBuilder::new(&fields)
            .equal("value", Datum::Int(21))
            .unwrap();
        let batches = read_rows(&table, Some(&["id"]), Some(filter)).await;

        assert_eq!(int_column(&batches, "id"), vec![2]);
        for batch in &batches {
            assert_eq!(
                batch.num_columns(),
                1,
                "widened predicate column must not leak into the output"
            );
            assert_eq!(batch.schema().field(0).name(), "id");
        }
    }

    /// Regression: PK-column filters were already exact (pushed down pre-merge
    /// AND now re-checked in the residual). Must stay exact.
    #[tokio::test]
    async fn kv_read_pk_filter_still_exact() {
        let file_io = test_file_io();
        let table_path = "memory:/kv_residual_pk";
        setup_dirs(&file_io, table_path).await;
        let table = pk_table(&file_io, table_path, &[]);

        write_commit(
            &table,
            &int_batch(vec![1, 2, 3], vec![Some(10), Some(20), Some(30)]),
        )
        .await;
        write_commit(
            &table,
            &int_batch(vec![1, 2, 3], vec![Some(11), Some(21), Some(31)]),
        )
        .await;

        let fields = table.schema().fields().to_vec();
        let filter = PredicateBuilder::new(&fields)
            .equal("id", Datum::Int(2))
            .unwrap();
        let batches = read_rows(&table, None, Some(filter)).await;

        assert_eq!(int_column(&batches, "id"), vec![2]);
        assert_eq!(int_column(&batches, "value"), vec![21]);
    }

    /// A filter matching only a superseded version must return nothing: the
    /// newer version wins the merge first, THEN the filter runs. If the full
    /// predicate leaked below the merge, the stale (2, 20) row would survive
    /// its file's scan, win against nothing, and leak into the output.
    #[tokio::test]
    async fn kv_read_filter_on_superseded_value_returns_nothing() {
        let file_io = test_file_io();
        let table_path = "memory:/kv_residual_superseded";
        setup_dirs(&file_io, table_path).await;
        let table = pk_table(&file_io, table_path, &[]);

        write_commit(
            &table,
            &int_batch(vec![1, 2, 3], vec![Some(10), Some(20), Some(30)]),
        )
        .await;
        write_commit(
            &table,
            &int_batch(vec![1, 2, 3], vec![Some(11), Some(21), Some(31)]),
        )
        .await;

        let fields = table.schema().fields().to_vec();
        let filter = PredicateBuilder::new(&fields)
            .equal("value", Datum::Int(20))
            .unwrap();
        let batches = read_rows(&table, None, Some(filter)).await;

        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total, 0,
            "superseded value must not resurrect through the filter"
        );
    }

    /// Compound residual `value > 15 AND value < 25` on merged values.
    #[tokio::test]
    async fn kv_read_applies_compound_range_filter() {
        let file_io = test_file_io();
        let table_path = "memory:/kv_residual_range";
        setup_dirs(&file_io, table_path).await;
        let table = pk_table(&file_io, table_path, &[]);

        write_commit(
            &table,
            &int_batch(vec![1, 2, 3], vec![Some(10), Some(20), Some(30)]),
        )
        .await;
        write_commit(
            &table,
            &int_batch(vec![1, 2, 3], vec![Some(11), Some(21), Some(31)]),
        )
        .await;

        let fields = table.schema().fields().to_vec();
        let pb = PredicateBuilder::new(&fields);
        let filter = Predicate::and(vec![
            pb.greater_than("value", Datum::Int(15)).unwrap(),
            pb.less_than("value", Datum::Int(25)).unwrap(),
        ]);
        let batches = read_rows(&table, None, Some(filter)).await;

        assert_eq!(int_column(&batches, "id"), vec![2]);
        assert_eq!(int_column(&batches, "value"), vec![21]);
    }

    /// COUNT(*)-style read: empty projection + non-PK filter. The residual
    /// runs on the pre-reorder merge batch (which still has columns), and the
    /// zero-column output batch must carry the filtered row count.
    #[tokio::test]
    async fn kv_read_empty_projection_with_filter_keeps_row_count() {
        let file_io = test_file_io();
        let table_path = "memory:/kv_residual_count";
        setup_dirs(&file_io, table_path).await;
        let table = pk_table(&file_io, table_path, &[]);

        write_commit(
            &table,
            &int_batch(vec![1, 2, 3], vec![Some(10), Some(20), Some(30)]),
        )
        .await;
        write_commit(
            &table,
            &int_batch(vec![1, 2, 3], vec![Some(11), Some(21), Some(31)]),
        )
        .await;

        let fields = table.schema().fields().to_vec();
        let filter = PredicateBuilder::new(&fields)
            .greater_than("value", Datum::Int(15))
            .unwrap();
        let batches = read_rows(&table, Some(&[] as &[&str]), Some(filter)).await;

        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2, "only merged rows with value > 15 (21, 31) count");
        for batch in &batches {
            assert_eq!(batch.num_columns(), 0);
        }
    }

    /// String residual op (starts_with) on a value column — exercises the
    /// residual string kernel on the KV path.
    #[tokio::test]
    async fn kv_read_applies_string_starts_with_filter() {
        use crate::spec::VarCharType;
        use arrow_array::StringArray;

        let file_io = test_file_io();
        let table_path = "memory:/kv_residual_string";
        setup_dirs(&file_io, table_path).await;

        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("name", DataType::VarChar(VarCharType::string_type()))
            .primary_key(["id"])
            .option("bucket", "1")
            .build()
            .unwrap();
        let table = Table::new(
            file_io.clone(),
            Identifier::new("default", "kv_residual_string_t"),
            table_path.to_string(),
            TableSchema::new(0, &schema),
            None,
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("name", ArrowDataType::Utf8, true),
        ]));
        let make = |ids: Vec<i32>, names: Vec<&str>| {
            RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(Int32Array::from(ids)),
                    Arc::new(StringArray::from(names)),
                ],
            )
            .unwrap()
        };

        write_commit(
            &table,
            &make(vec![1, 2, 3], vec!["apple", "banana", "apricot"]),
        )
        .await;
        write_commit(&table, &make(vec![2], vec!["avocado"])).await;

        let fields = table.schema().fields().to_vec();
        let filter = PredicateBuilder::new(&fields)
            .starts_with("name", Datum::String("a".to_string()))
            .unwrap();
        let batches = read_rows(&table, None, Some(filter)).await;

        // Merged rows: (1, apple), (2, avocado), (3, apricot) — all start with 'a'.
        // The overwritten (2, banana) must not resurrect; if the filter ran
        // pre-merge it would also be wrong the other way (banana dropped, but
        // then avocado wins anyway — so also assert the merged VALUE).
        let mut ids = int_column(&batches, "id");
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2, 3]);
        let names: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                let idx = b.schema().index_of("name").unwrap();
                let arr = b
                    .column(idx)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                (0..arr.len())
                    .map(|i| arr.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert!(names.contains(&"avocado".to_string()));
        assert!(!names.contains(&"banana".to_string()));
    }

    /// Aggregation (sum): inputs 10 + 20 merge to 30. `value = 30` must match
    /// the merged row (a pre-merge filter would drop both inputs);
    /// `value = 10` must match nothing (a pre-merge filter would keep the
    /// 10-input and leak it).
    #[tokio::test]
    async fn kv_read_aggregation_filters_on_merged_value() {
        let file_io = test_file_io();
        let table_path = "memory:/kv_residual_agg";
        setup_dirs(&file_io, table_path).await;
        let table = pk_table(
            &file_io,
            table_path,
            &[
                ("merge-engine", "aggregation"),
                ("fields.value.aggregate-function", "sum"),
            ],
        );

        write_commit(&table, &int_batch(vec![1], vec![Some(10)])).await;
        write_commit(&table, &int_batch(vec![1], vec![Some(20)])).await;

        let fields = table.schema().fields().to_vec();

        let match_merged = PredicateBuilder::new(&fields)
            .equal("value", Datum::Int(30))
            .unwrap();
        let batches = read_rows(&table, None, Some(match_merged)).await;
        assert_eq!(int_column(&batches, "id"), vec![1]);
        assert_eq!(int_column(&batches, "value"), vec![30]);

        let match_input = PredicateBuilder::new(&fields)
            .equal("value", Datum::Int(10))
            .unwrap();
        let batches = read_rows(&table, None, Some(match_input)).await;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0, "pre-merge input value must not leak through");
    }

    /// Aggregation + Gap-A: the aggregated predicate column is unprojected.
    /// The widened column must be aggregated with its configured function
    /// (sum), not treated as a plain latest-value column.
    #[tokio::test]
    async fn kv_read_aggregation_filters_merged_value_unprojected() {
        let file_io = test_file_io();
        let table_path = "memory:/kv_residual_agg_gap_a";
        setup_dirs(&file_io, table_path).await;
        let table = pk_table(
            &file_io,
            table_path,
            &[
                ("merge-engine", "aggregation"),
                ("fields.value.aggregate-function", "sum"),
            ],
        );

        write_commit(&table, &int_batch(vec![1], vec![Some(10)])).await;
        write_commit(&table, &int_batch(vec![1], vec![Some(20)])).await;

        let fields = table.schema().fields().to_vec();
        let filter = PredicateBuilder::new(&fields)
            .equal("value", Datum::Int(30))
            .unwrap();
        let batches = read_rows(&table, Some(&["id"]), Some(filter)).await;
        assert_eq!(int_column(&batches, "id"), vec![1]);
    }

    /// Partial-update: (1, a=5, b=NULL) then (1, a=NULL, b=7) merge to
    /// (1, 5, 7). A conjunction over both columns only matches the MERGED row
    /// — no single input row satisfies it.
    #[tokio::test]
    async fn kv_read_partial_update_filters_on_merged_row() {
        let file_io = test_file_io();
        let table_path = "memory:/kv_residual_pu";
        setup_dirs(&file_io, table_path).await;

        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("a", DataType::Int(IntType::new()))
            .column("b", DataType::Int(IntType::new()))
            .primary_key(["id"])
            .option("bucket", "1")
            .option("merge-engine", "partial-update")
            .build()
            .unwrap();
        let table = Table::new(
            file_io.clone(),
            Identifier::new("default", "kv_residual_pu_t"),
            table_path.to_string(),
            TableSchema::new(0, &schema),
            None,
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("a", ArrowDataType::Int32, true),
            ArrowField::new("b", ArrowDataType::Int32, true),
        ]));
        let make = |ids: Vec<i32>, a: Vec<Option<i32>>, b: Vec<Option<i32>>| {
            RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(Int32Array::from(ids)),
                    Arc::new(Int32Array::from(a)),
                    Arc::new(Int32Array::from(b)),
                ],
            )
            .unwrap()
        };

        write_commit(&table, &make(vec![1], vec![Some(5)], vec![None])).await;
        write_commit(&table, &make(vec![1], vec![None], vec![Some(7)])).await;

        let fields = table.schema().fields().to_vec();
        let pb = PredicateBuilder::new(&fields);
        let filter = Predicate::and(vec![
            pb.equal("a", Datum::Int(5)).unwrap(),
            pb.equal("b", Datum::Int(7)).unwrap(),
        ]);
        let batches = read_rows(&table, None, Some(filter)).await;

        assert_eq!(int_column(&batches, "id"), vec![1]);
        assert_eq!(int_column(&batches, "a"), vec![5]);
        assert_eq!(int_column(&batches, "b"), vec![7]);
    }

    /// An AlwaysFalse filter on a PK table must return nothing, end to end.
    /// Two layers enforce it: scan-side stats pruning treats AlwaysFalse as
    /// prune-everything (plans no files), and the post-merge residual masks
    /// every row to false. This locks the composed contract regardless of
    /// which layer short-circuits first.
    #[tokio::test]
    async fn kv_read_always_false_filter_returns_nothing() {
        let file_io = test_file_io();
        let table_path = "memory:/kv_residual_always_false";
        setup_dirs(&file_io, table_path).await;
        let table = pk_table(&file_io, table_path, &[]);

        // Overlapping keys across two commits -> split is not raw convertible
        // -> forced through KeyValueFileReader.
        write_commit(
            &table,
            &int_batch(vec![1, 2, 3], vec![Some(10), Some(20), Some(30)]),
        )
        .await;
        write_commit(
            &table,
            &int_batch(vec![1, 2, 3], vec![Some(11), Some(21), Some(31)]),
        )
        .await;

        let batches = read_rows(&table, None, Some(Predicate::AlwaysFalse)).await;

        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0, "AlwaysFalse must return no rows on a PK table");
    }

    /// Schema evolution on the KV residual path: a predicate column that is
    /// MISSING from an old-schema file is null-filled pre-merge by
    /// DataFileReader; the post-merge residual must treat those NULLs as
    /// non-matching (comparison mask NULL -> false), and `is_null` must match
    /// exactly them. Locks the null-fill -> merge -> residual composition; the
    /// shared evaluator's semantics are already locked on the data-evolution
    /// path (`test_evolution_read_null_filled_predicate_column_semantics`).
    ///
    /// Setup: commit 1 goes through a Table at schema 0 (id, value), stamping
    /// schema_id 0 into its files; commit 2 goes through a Table at the same
    /// path at schema 1 (id, value, score — new field id). Both schema JSONs
    /// are persisted so `SchemaManager::schema(0)` resolves at read time. Keys
    /// overlap across commits so the split is not raw convertible and routes
    /// through the KV merge reader.
    #[tokio::test]
    async fn kv_read_schema_evolution_null_filled_predicate_semantics() {
        let file_io = test_file_io();
        let table_path = "memory:/kv_residual_schema_evolution";
        setup_dirs(&file_io, table_path).await;

        let schema0 = TableSchema::new(0, &evo_user_schema(false));
        let schema1 = TableSchema::new(1, &evo_user_schema(true));
        let table_v0 = Table::new(
            file_io.clone(),
            Identifier::new("default", "kv_residual_evo_t"),
            table_path.to_string(),
            schema0.clone(),
            None,
        );
        let table_v1 = Table::new(
            file_io.clone(),
            Identifier::new("default", "kv_residual_evo_t"),
            table_path.to_string(),
            schema1.clone(),
            None,
        );
        write_schema_file(&table_v1, &schema0).await;
        write_schema_file(&table_v1, &schema1).await;

        // Commit 1 at schema 0: files carry schema_id 0. Commit 2 at schema 1
        // overwrites key 3, so file_meta.schema_id != table_schema_id holds for
        // the old files when reading through table_v1, forcing the null-fill
        // remap in read() -> DataFileReader::read_single_file_stream.
        write_commit(
            &table_v0,
            &int_batch(vec![1, 2, 3], vec![Some(10), Some(20), Some(30)]),
        )
        .await;
        write_commit(
            &table_v1,
            &evo_batch(vec![3], vec![Some(31)], vec![Some(300)]),
        )
        .await;

        // Merged rows: (1, 10, NULL), (2, 20, NULL), (3, 31, 300).
        let fields = table_v1.schema().fields().to_vec();
        let pb = PredicateBuilder::new(&fields);

        // Comparison: score = 300 matches only id 3; old rows' null-filled
        // score must collapse to false, not match or error.
        let filter = pb.equal("score", Datum::Int(300)).unwrap();
        let batches = read_rows(&table_v1, None, Some(filter)).await;
        assert_eq!(int_column(&batches, "id"), vec![3]);
        assert_eq!(int_column(&batches, "value"), vec![31]);
        assert_eq!(int_column(&batches, "score"), vec![300]);

        // IS NULL: matches exactly the null-filled old rows (ids 1, 2).
        let filter = pb.is_null("score").unwrap();
        let batches = read_rows(&table_v1, None, Some(filter)).await;
        let mut ids = int_column(&batches, "id");
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2]);

        // Gap-A on an evolution column: score is filtered but not projected.
        // The merge read must widen internally (null-filling score for the old
        // files), filter, then project back to just "id".
        let filter = pb.equal("score", Datum::Int(300)).unwrap();
        let batches = read_rows(&table_v1, Some(&["id"]), Some(filter)).await;
        assert_eq!(int_column(&batches, "id"), vec![3]);
        for batch in &batches {
            assert_eq!(
                batch.num_columns(),
                1,
                "widened evolution column must not leak into the output"
            );
            assert_eq!(batch.schema().field(0).name(), "id");
        }
    }
}
