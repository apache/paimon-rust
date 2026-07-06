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
            rb.with_projection(cols);
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
}
