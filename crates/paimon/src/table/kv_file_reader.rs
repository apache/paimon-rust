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
//! Data files with disjoint key ranges are concatenated into sorted runs. The
//! runs are merged by primary key using a LoserTree, and rows with the same key
//! are deduplicated by keeping the one with the highest `_SEQUENCE_NUMBER`.
//! Non-primary-key predicate conjuncts are enforced by an exact post-merge
//! residual filter; only primary-key conjuncts are pushed below the merge.
//!
//! Reference: Java Paimon `SortMergeReaderWithMinHeap`.

use super::data_file_reader::DataFileReader;
use super::sort_merge::{
    AggregateMergeFunction, DeduplicateMergeFunction, PartialUpdateMergeFunction,
    SortMergeReaderBuilder,
};
use crate::arrow::{build_target_arrow_schema, ParquetReadBudget};
use crate::deletion_vector::DeletionVectorFactory;
use crate::io::FileIO;
use crate::spec::{
    BigIntType, DataField, DataFileMeta, DataType as PaimonDataType, MergeEngine,
    PartialUpdateConfig, Predicate, TinyIntType, SEQUENCE_NUMBER_FIELD_ID,
    SEQUENCE_NUMBER_FIELD_NAME, VALUE_KIND_FIELD_ID, VALUE_KIND_FIELD_NAME,
};
use crate::table::schema_manager::SchemaManager;
use crate::table::ArrowRecordBatchStream;
use crate::{DataSplit, DeletionFile, Error};
use arrow_array::{RecordBatch, RecordBatchOptions};

use async_stream::try_stream;
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;

/// Reads primary-key table data files using sort-merge deduplication.
pub(crate) struct KeyValueFileReader {
    file_io: FileIO,
    config: KeyValueReadConfig,
    /// PK-only conjuncts pushed down to the per-file readers before merge.
    /// Non-PK conjuncts must not run pre-merge (they can change which version
    /// of a key survives); they are enforced by the post-merge residual
    /// filter using the full `config.predicates` instead.
    pushdown_predicates: Vec<Predicate>,
    #[cfg(test)]
    input_batch_sizes: Option<std::sync::Arc<std::sync::Mutex<Vec<usize>>>>,
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
    pub read_batch_size: usize,
    /// Merge files from all supplied splits into one globally key-sorted stream.
    pub merge_splits: bool,
    /// Optional cap on sorted-run inputs merged concurrently by one LoserTree.
    /// This limits merge fan-in, not files: files within a run are opened serially.
    pub max_merge_input_streams: Option<usize>,
    /// Scan-shared Parquet concurrency and projected-byte budget.
    pub parquet_read_budget: Option<Arc<ParquetReadBudget>>,
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

fn widen_partial_update_sequence_group_fields(
    merge_engine: MergeEngine,
    table_options: &HashMap<String, String>,
    table_fields: &[DataField],
    primary_keys: &[String],
    mut user_fields: Vec<DataField>,
) -> crate::Result<Vec<DataField>> {
    if merge_engine != MergeEngine::PartialUpdate {
        return Ok(user_fields);
    }

    let projected_fields = user_fields
        .iter()
        .map(|field| field.name().to_string())
        .collect::<Vec<_>>();
    let required_fields = PartialUpdateConfig::new(table_options).required_sequence_fields(
        table_fields,
        primary_keys,
        &projected_fields,
    )?;
    for field_name in required_fields {
        if user_fields.iter().any(|field| field.name() == field_name) {
            continue;
        }
        let field = table_fields
            .iter()
            .find(|field| field.name() == field_name)
            .cloned()
            .ok_or_else(|| Error::UnexpectedError {
                message: format!(
                    "Partial-update sequence field '{field_name}' not found in table schema"
                ),
                source: None,
            })?;
        user_fields.push(field);
    }
    Ok(user_fields)
}

fn ensure_merge_input_limit(input_stream_count: usize, limit: Option<usize>) -> crate::Result<()> {
    if let Some(limit) = limit {
        if input_stream_count <= limit {
            return Ok(());
        }
        return Err(Error::Unsupported {
            message: format!(
                "KeyValueFileReader refuses to merge {input_stream_count} overlapping sorted-run input streams in one sort-merge group; maximum is {limit}. Compact the table before reading this highly fragmented group"
            ),
        });
    }
    Ok(())
}

struct MergeRun {
    files: Vec<MergeFile>,
}

struct MergeFile {
    split: Arc<DataSplit>,
    file: DataFileMeta,
}

fn plan_merge_groups(
    split_group: &[Arc<DataSplit>],
    comparator: Option<&super::merge_tree_split_generator::KeyComparator>,
    merge_splits: bool,
) -> Vec<Vec<MergeRun>> {
    let Some(comparator) = comparator else {
        let runs = split_group
            .iter()
            .flat_map(|split| {
                let files = split.data_files().to_vec();
                let split = Arc::clone(split);
                files.into_iter().map(move |file| MergeRun {
                    files: vec![MergeFile {
                        split: Arc::clone(&split),
                        file,
                    }],
                })
            })
            .collect::<Vec<_>>();
        return if runs.is_empty() {
            Vec::new()
        } else {
            vec![runs]
        };
    };

    if merge_splits {
        let files = split_group
            .iter()
            .flat_map(|split| {
                let files = split.data_files().to_vec();
                let split = Arc::clone(split);
                files.into_iter().map(move |file| MergeFile {
                    split: Arc::clone(&split),
                    file,
                })
            })
            .collect::<Vec<_>>();
        let runs = super::merge_tree_split_generator::pack_sorted_runs_by(
            files,
            comparator,
            |merge_file| &merge_file.file,
        )
        .into_iter()
        .map(|files| MergeRun { files })
        .collect::<Vec<_>>();
        return if runs.is_empty() {
            Vec::new()
        } else {
            vec![runs]
        };
    }

    let mut groups = Vec::new();
    for split in split_group {
        for section in super::merge_tree_split_generator::interval_partition(
            split.data_files().to_vec(),
            comparator,
        ) {
            let runs = super::merge_tree_split_generator::pack_sorted_runs(section, comparator)
                .into_iter()
                .map(|files| MergeRun {
                    files: files
                        .into_iter()
                        .map(|file| MergeFile {
                            split: Arc::clone(split),
                            file,
                        })
                        .collect(),
                })
                .collect::<Vec<_>>();
            if !runs.is_empty() {
                groups.push(runs);
            }
        }
    }
    groups
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
            #[cfg(test)]
            input_batch_sizes: None,
        }
    }

    #[cfg(test)]
    fn with_input_batch_sizes(
        mut self,
        input_batch_sizes: std::sync::Arc<std::sync::Mutex<Vec<usize>>>,
    ) -> Self {
        self.input_batch_sizes = Some(input_batch_sizes);
        self
    }

    fn new_merge_function(
        merge_engine: MergeEngine,
        table_options: &HashMap<String, String>,
        table_name: &str,
        table_fields: &[DataField],
        merge_output_fields: &[DataField],
        primary_keys: &[String],
        sequence_fields: &[String],
    ) -> crate::Result<Box<dyn super::sort_merge::MergeFunction>> {
        match merge_engine {
            MergeEngine::Deduplicate => Ok(Box::new(DeduplicateMergeFunction)),
            MergeEngine::PartialUpdate => Ok(Box::new(
                PartialUpdateMergeFunction::new_with_schema(
                    table_options,
                    table_name,
                    table_fields,
                    merge_output_fields,
                    primary_keys,
                )?,
            )),
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
        // A projected `_ROW_ID` is synthesized as all-nulls here, so the residual
        // would silently drop every row rather than hit its missing-column guard.
        super::row_id_predicate::reject_row_id_filter(
            &self.config.predicates,
            "primary-key tables",
        )?;
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
        let key_comparator = if key_fields.is_empty() {
            None
        } else {
            Some(super::merge_tree_split_generator::KeyComparator::new(
                key_fields
                    .iter()
                    .map(|field| field.data_type().clone())
                    .collect(),
            ))
        };
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
                row_filter_factory: None,
                file_fields: self.config.table_fields.clone(),
            });
        let user_fields = crate::arrow::residual::widen_scan_fields(
            &user_fields,
            residual_file_predicates.as_ref(),
        );
        let user_fields = widen_partial_update_sequence_group_fields(
            self.config.merge_engine,
            &self.config.table_options,
            &self.config.table_fields,
            &self.config.primary_keys,
            user_fields,
        )?;

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

        let merge_splits = self.config.merge_splits;
        let data_splits = data_splits
            .iter()
            .cloned()
            .map(Arc::new)
            .collect::<Vec<_>>();
        let split_groups: Vec<Vec<Arc<DataSplit>>> = if merge_splits {
            vec![data_splits]
        } else {
            data_splits.into_iter().map(|split| vec![split]).collect()
        };
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
        let read_batch_size = self.config.read_batch_size;
        let max_merge_input_streams = self.config.max_merge_input_streams;
        let parquet_read_budget = self.config.parquet_read_budget;
        #[cfg(test)]
        let input_batch_sizes = self.input_batch_sizes;

        // Build the merge output schema (keys + values, no system columns).
        let mut merge_output_fields: Vec<DataField> = Vec::new();
        merge_output_fields.extend(key_fields);
        merge_output_fields.extend(value_fields);
        let merge_output_schema = build_target_arrow_schema(&merge_output_fields)?;

        Ok(try_stream! {
            for split_group in &split_groups {
                // A deletion-vector merge-on-read split can mix compacted
                // sources carrying DVs with uncompacted level-0 files. Keep
                // only the small per-file metadata here; load each bitmap when
                // its sorted run reaches that physical file.
                let mut deletion_files_by_split =
                    HashMap::<usize, Arc<HashMap<String, DeletionFile>>>::new();
                for split in split_group {
                    let Some(deletion_files) = split.data_deletion_files() else {
                        continue;
                    };
                    let by_name = split
                        .data_files()
                        .iter()
                        .zip(deletion_files.iter())
                        .filter_map(|(data_file, deletion_file)| {
                            deletion_file
                                .as_ref()
                                .map(|file| (data_file.file_name.clone(), file.clone()))
                        })
                        .collect::<HashMap<_, _>>();
                    if !by_name.is_empty() {
                        deletion_files_by_split
                            .insert(Arc::as_ptr(split) as usize, Arc::new(by_name));
                    }
                }
                for merge_group in plan_merge_groups(
                    split_group,
                    key_comparator.as_ref(),
                    merge_splits,
                ) {
                    let input_stream_count = merge_group.len();
                    ensure_merge_input_limit(input_stream_count, max_merge_input_streams)?;
                    // Sort-merge must first obtain one batch from every input
                    // stream. Keep concurrent row-group reads disabled whenever
                    // multiple runs advance in lockstep; one run may still use
                    // the shared budget because its files are opened serially.
                    let group_parquet_read_budget = if input_stream_count == 1 {
                        parquet_read_budget.clone()
                    } else {
                        None
                    };
                    let mut file_streams: Vec<ArrowRecordBatchStream> = Vec::new();

                    for MergeRun { files } in merge_group {
                        let reader = DataFileReader::new(
                            file_io.clone(),
                            schema_manager.clone(),
                            table_schema_id,
                            table_fields.clone(),
                            internal_read_type.clone(),
                            pushdown_predicates.clone(),
                        )
                        .with_batch_size(Some(read_batch_size))
                        .with_parquet_read_budget(group_parquet_read_budget.clone());
                        let run_schema_manager = schema_manager.clone();
                        let run_file_io = file_io.clone();
                        let deletion_files_by_split = deletion_files_by_split.clone();
                        let run_stream: ArrowRecordBatchStream = Box::pin(try_stream! {
                            for MergeFile { split, file: file_meta } in files {
                                let data_fields: Option<Vec<DataField>> =
                                    if file_meta.schema_id != table_schema_id {
                                        let data_schema =
                                            run_schema_manager.schema(file_meta.schema_id).await?;
                                        Some(data_schema.fields().to_vec())
                                } else {
                                    None
                                };
                                let deletion_file = deletion_files_by_split
                                    .get(&(Arc::as_ptr(&split) as usize))
                                    .and_then(|files| files.get(&file_meta.file_name))
                                    .cloned();
                                let deletion_vector = match deletion_file {
                                    Some(file) => Some(Arc::new(
                                        DeletionVectorFactory::read(&run_file_io, &file).await?,
                                    )),
                                    None => None,
                                };
                                let mut file_stream = reader.read_single_file_stream(
                                    split.as_ref(),
                                    file_meta,
                                    data_fields,
                                    deletion_vector,
                                    split.row_ranges().map(|ranges| ranges.to_vec()),
                                )?;
                                while let Some(batch) = file_stream.next().await {
                                    yield batch?;
                                }
                            }
                        });
                        #[cfg(test)]
                        let run_stream = if let Some(batch_sizes) = input_batch_sizes.clone() {
                            run_stream
                                .inspect(move |batch| {
                                    if let Ok(batch) = batch {
                                        batch_sizes.lock().unwrap().push(batch.num_rows());
                                    }
                                })
                                .boxed()
                        } else {
                            run_stream
                        };
                        file_streams.push(run_stream);
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
                            &table_fields,
                            &merge_output_fields,
                            &primary_keys,
                            &sequence_fields,
                        )?,
                    )
                    .build()?;

                    while let Some(batch) = merge_stream.next().await {
                        let batch = batch?;
                        // The post-merge residual enforces the FULL data predicate
                        // on merged rows. PK conjuncts are also in this set (they
                        // were already pushed down pre-merge); re-evaluating them
                        // on already-matching rows is a no-op and keeps one shared
                        // evaluator instead of deriving a non-PK subset. Runs on
                        // the merge-output batch (keys + values, including widened
                        // predicate columns); the reorder below projects the output
                        // back to read_type.
                        let batch = if residual_predicates.is_empty() {
                            batch
                        } else {
                            match crate::arrow::residual::evaluate_predicates_mask(
                                &batch,
                                &residual_predicates,
                                &table_fields,
                                &merge_output_fields,
                            )? {
                                Some(mask) => arrow_select::filter::filter_record_batch(
                                    &batch, &mask,
                                )
                                .map_err(|e| Error::DataInvalid {
                                    message: format!(
                                        "Failed to filter merged batch by predicates: {e}"
                                    ),
                                    source: Some(Box::new(e)),
                                })?,
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
                        let reordered = RecordBatch::try_new_with_options(
                            output_schema.clone(),
                            columns,
                            &options,
                        )
                        .map_err(|e| Error::UnexpectedError {
                            message: format!("Failed to reorder merged RecordBatch: {e}"),
                            source: Some(Box::new(e)),
                        })?;
                        yield reordered;
                    }
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
    use crate::deletion_vector::DeletionVector;
    use crate::io::FileIOBuilder;
    use crate::spec::{
        stats::BinaryTableStats, BinaryRow, DataFileMeta, DataType, Datum, IntType,
        PredicateBuilder, Schema, TableSchema, VarCharType,
    };
    use crate::table::source::{DataSplitBuilder, DeletionFile};
    use crate::table::table_commit::TableCommit;
    use crate::table::{Table, TableWrite};
    use arrow_array::{Array, Int32Array, Int64Array, Int8Array, StringArray};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use bytes::Bytes;
    use futures::TryStreamExt;
    use parquet::arrow::AsyncArrowWriter;
    use parquet::file::metadata::ParquetMetaDataReader;
    use parquet::file::properties::WriterProperties;
    use roaring::RoaringBitmap;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_row_id_filter_on_a_primary_key_table_is_rejected() {
        let file_io = test_file_io();
        let table_path = "memory:/kv_row_id_filter";
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

        let row_id = crate::spec::row_id_leaf(
            crate::spec::PredicateOperator::NotEq,
            vec![Datum::Long(102)],
        );
        let mut read_builder = table.new_read_builder();
        read_builder
            .with_projection(&["id", "value", crate::spec::ROW_ID_FIELD_NAME])
            .unwrap();
        read_builder.with_filter(row_id);
        let plan = read_builder.new_scan().plan().await.unwrap();
        let err = read_builder
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .err()
            .expect("a _ROW_ID filter must be rejected");

        assert!(
            matches!(&err, Error::Unsupported { message } if message.contains("_ROW_ID")),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn test_row_id_conjunct_is_not_treated_as_a_primary_key_conjunct() {
        let fields = vec![
            DataField::new(0, "k".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "v".to_string(), DataType::Int(IntType::new())),
        ];
        let row_id =
            crate::spec::row_id_leaf(crate::spec::PredicateOperator::Eq, vec![Datum::Long(5)]);
        let on_key = PredicateBuilder::new(&fields)
            .equal("k", Datum::Int(1))
            .unwrap();

        assert_eq!(
            retain_primary_key_conjuncts(
                std::slice::from_ref(&row_id),
                &fields,
                &["k".to_string()]
            ),
            Vec::new()
        );
        assert_eq!(
            retain_primary_key_conjuncts(
                &[Predicate::and(vec![row_id, on_key.clone()])],
                &fields,
                &["k".to_string()],
            ),
            vec![on_key]
        );
    }

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

    async fn write_deletion_file(
        file_io: &FileIO,
        table_path: &str,
        deleted_rows: &[u32],
    ) -> DeletionFile {
        let path = format!("{table_path}/index/dv");
        file_io
            .mkdirs(&format!("{table_path}/index/"))
            .await
            .unwrap();
        let bitmap = deleted_rows.iter().copied().collect::<RoaringBitmap>();
        let bytes = DeletionVector::from_bitmap(bitmap)
            .serialize_to_bytes()
            .unwrap();
        let bitmap_length = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
        file_io
            .new_output(&path)
            .unwrap()
            .write(Bytes::from(bytes))
            .await
            .unwrap();
        DeletionFile::new(
            path,
            0,
            i64::from(bitmap_length),
            Some(deleted_rows.len() as i64),
        )
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

    fn dummy_data_file(name: String) -> DataFileMeta {
        DataFileMeta {
            file_name: name,
            file_size: 128,
            row_count: 1,
            min_key: Vec::new(),
            max_key: Vec::new(),
            key_stats: BinaryTableStats::new(Vec::new(), Vec::new(), Vec::new()),
            value_stats: BinaryTableStats::new(Vec::new(), Vec::new(), Vec::new()),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id: 0,
            level: 0,
            extra_files: Vec::new(),
            creation_time: None,
            delete_row_count: Some(0),
            embedded_index: None,
            file_source: None,
            value_stats_cols: None,
            external_path: None,
            first_row_id: None,
            write_cols: None,
        }
    }

    /// Java-compatible DV merge-on-read is a batch visibility override: the
    /// default still hides uncompacted level-0 files, while a dynamic override
    /// includes them and merges overlapping key versions. A tiny split target
    /// makes this also catch planners that incorrectly separate overlapping
    /// files into independent raw splits.
    #[tokio::test]
    async fn dv_merge_on_read_exposes_and_merges_level_zero_files() {
        let file_io = test_file_io();
        let table_path = "memory:/dv_merge_on_read_level_zero";
        setup_dirs(&file_io, table_path).await;
        let table = pk_table(
            &file_io,
            table_path,
            &[
                ("deletion-vectors.enabled", "true"),
                ("source.split.target-size", "1b"),
                ("source.split.open-file-cost", "1b"),
            ],
        );

        write_commit(&table, &int_batch(vec![1, 2], vec![Some(10), Some(20)])).await;
        write_commit(&table, &int_batch(vec![1, 3], vec![Some(11), Some(30)])).await;

        let hidden_plan = table.new_read_builder().new_scan().plan().await.unwrap();
        assert!(
            hidden_plan.splits().is_empty(),
            "DV batch reads must keep hiding level-0 files by default"
        );

        let merge_on_read = table.copy_with_options(HashMap::from([(
            "deletion-vectors.merge-on-read".to_string(),
            "true".to_string(),
        )]));
        let read_builder = merge_on_read.new_read_builder();
        let plan = read_builder.new_scan().plan().await.unwrap();
        assert_eq!(
            plan.splits().len(),
            1,
            "overlapping versions must share one split"
        );
        assert_eq!(plan.splits()[0].data_files().len(), 2);
        assert!(!plan.splits()[0].raw_convertible());

        let batches = read_builder
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(int_column(&batches, "id"), vec![1, 2, 3]);
        assert_eq!(int_column(&batches, "value"), vec![11, 20, 30]);

        let stale_filter = PredicateBuilder::new(merge_on_read.schema().fields())
            .equal("value", Datum::Int(10))
            .unwrap();
        let stale_batches = read_rows(&merge_on_read, None, Some(stale_filter)).await;
        assert_eq!(
            stale_batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            0,
            "a predicate matching only the superseded L0 value must not resurrect it"
        );
    }

    #[tokio::test]
    async fn dynamic_dv_merge_on_read_is_ignored_without_deletion_vectors() {
        let file_io = test_file_io();
        let table_path = "memory:/ignored_dynamic_dv_merge_on_read";
        setup_dirs(&file_io, table_path).await;
        let table = pk_table(&file_io, table_path, &[]);
        write_commit(&table, &int_batch(vec![1, 2], vec![Some(10), Some(20)])).await;
        write_commit(&table, &int_batch(vec![1, 3], vec![Some(11), Some(30)])).await;

        let merge_on_read = table.copy_with_options(HashMap::from([(
            "deletion-vectors.merge-on-read".to_string(),
            "true".to_string(),
        )]));
        let batches = read_rows(&merge_on_read, None, None).await;
        assert_eq!(int_column(&batches, "id"), vec![1, 2, 3]);
        assert_eq!(int_column(&batches, "value"), vec![11, 20, 30]);
    }

    /// A MOR split can contain both uncompacted records and compacted source
    /// files with deletion vectors. DV filtering must happen per physical file
    /// before the surviving records enter the key merge; otherwise a deleted
    /// key with no replacement is resurrected.
    #[tokio::test]
    async fn dv_merge_on_read_applies_deletion_vectors_before_key_merge() {
        let file_io = test_file_io();
        let table_path = "memory:/dv_merge_on_read_with_dv";
        setup_dirs(&file_io, table_path).await;
        let table = pk_table(
            &file_io,
            table_path,
            &[
                ("deletion-vectors.enabled", "true"),
                ("deletion-vectors.merge-on-read", "true"),
            ],
        );

        write_commit(&table, &int_batch(vec![1, 2], vec![Some(10), Some(20)])).await;
        write_commit(&table, &int_batch(vec![1, 3], vec![Some(11), Some(30)])).await;

        let all_files = table
            .new_read_builder()
            .new_scan()
            .with_scan_all_files()
            .plan()
            .await
            .unwrap();
        let mut files = all_files
            .splits()
            .iter()
            .flat_map(|split| split.data_files().iter().cloned())
            .collect::<Vec<_>>();
        files.sort_by_key(|file| file.min_sequence_number);
        assert_eq!(files.len(), 2);
        files[0].level = 1;

        let deletion_file = write_deletion_file(&file_io, table_path, &[1]).await;
        let split = DataSplitBuilder::new()
            .with_snapshot(2)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(format!("{table_path}/bucket-0"))
            .with_total_buckets(1)
            .with_data_files(files)
            .with_data_deletion_files(vec![Some(deletion_file), None])
            .with_raw_convertible(false)
            .build()
            .unwrap();

        let batches = table
            .new_read_builder()
            .new_read()
            .unwrap()
            .to_arrow(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(int_column(&batches, "id"), vec![1, 3]);
        assert_eq!(int_column(&batches, "value"), vec![11, 30]);
    }

    /// Disjoint files in one split are consumed as sequential merge groups. A
    /// DV attached to a late file must not be read before an earlier group can
    /// emit its output.
    #[tokio::test]
    async fn dv_merge_on_read_loads_deletion_vectors_per_file() {
        let file_io = test_file_io();
        let table_path = "memory:/dv_merge_on_read_lazy_dv";
        setup_dirs(&file_io, table_path).await;
        let table = pk_table(
            &file_io,
            table_path,
            &[
                ("deletion-vectors.enabled", "true"),
                ("deletion-vectors.merge-on-read", "true"),
            ],
        );

        let mut files = Vec::new();
        for i in 0..10 {
            files.push(
                write_multi_row_group_kv_file(
                    &file_io,
                    table_path,
                    &format!("part-{i}.parquet"),
                    i * 1_000,
                    i64::from(i),
                    i,
                )
                .await,
            );
        }
        let mut deletion_files = vec![None; files.len()];
        deletion_files[9] = Some(DeletionFile::new(
            format!("{table_path}/index/not-yet-read.dv"),
            0,
            1,
            Some(1),
        ));
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(format!("{table_path}/bucket-0"))
            .with_total_buckets(1)
            .with_data_files(files)
            .with_data_deletion_files(deletion_files)
            .with_raw_convertible(false)
            .build()
            .unwrap();

        let mut stream = table
            .new_read_builder()
            .new_read()
            .unwrap()
            .to_arrow(&[split])
            .unwrap();
        let first = stream
            .next()
            .await
            .expect("the first output batch")
            .expect("a late deletion vector must be loaded lazily");
        assert_eq!(first.num_rows(), 128);

        let err = stream.try_collect::<Vec<_>>().await.unwrap_err();
        assert!(
            err.to_string().contains("not-yet-read.dv"),
            "the late file must still surface its missing DV when reached: {err:?}"
        );
    }

    fn int_key(value: i32) -> Vec<u8> {
        let mut builder = crate::spec::BinaryRowBuilder::new(1);
        builder.write_int(0, value);
        builder.build_serialized()
    }

    async fn write_multi_row_group_kv_file(
        file_io: &FileIO,
        table_path: &str,
        file_name: &str,
        start_id: i32,
        sequence: i64,
        value: i32,
    ) -> DataFileMeta {
        let schema = crate::arrow::build_target_arrow_schema(&[
            DataField::new(
                SEQUENCE_NUMBER_FIELD_ID,
                SEQUENCE_NUMBER_FIELD_NAME.to_string(),
                DataType::BigInt(BigIntType::new()),
            ),
            DataField::new(
                VALUE_KIND_FIELD_ID,
                VALUE_KIND_FIELD_NAME.to_string(),
                DataType::TinyInt(TinyIntType::new()),
            ),
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "value".to_string(), DataType::Int(IntType::new())),
        ])
        .unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_value(sequence, 128)),
                Arc::new(Int8Array::from_value(0, 128)),
                Arc::new(Int32Array::from_iter_values(start_id..start_id + 128)),
                Arc::new(Int32Array::from_value(value, 128)),
            ],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(64))
            .set_dictionary_enabled(false)
            .build();
        let mut bytes = Vec::new();
        {
            let mut writer = AsyncArrowWriter::try_new(&mut bytes, schema, Some(props)).unwrap();
            writer.write(&batch).await.unwrap();
            writer.close().await.unwrap();
        }
        let parquet_bytes = bytes::Bytes::from(bytes);
        let metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&parquet_bytes)
            .unwrap();
        assert_eq!(metadata.num_row_groups(), 2);

        let bucket_path = format!("{table_path}/bucket-0");
        file_io.mkdirs(&format!("{bucket_path}/")).await.unwrap();
        file_io
            .new_output(&format!("{bucket_path}/{file_name}"))
            .unwrap()
            .write(parquet_bytes.clone())
            .await
            .unwrap();

        let mut file = dummy_data_file(file_name.to_string());
        file.file_size = parquet_bytes.len() as i64;
        file.row_count = 128;
        file.min_key = int_key(start_id);
        file.max_key = int_key(start_id + 127);
        file
    }

    fn kv_reader_with_budget(table: &Table, budget: Arc<ParquetReadBudget>) -> KeyValueFileReader {
        let core_options = table.schema().core_options();
        KeyValueFileReader::new(
            table.file_io().clone(),
            KeyValueReadConfig {
                table_name: table.identifier().full_name(),
                table_options: table.schema().options().clone(),
                schema_manager: table.schema_manager().clone(),
                table_schema_id: table.schema().id(),
                table_fields: table.schema().fields().to_vec(),
                read_type: table.schema().fields().to_vec(),
                predicates: Vec::new(),
                primary_keys: table.schema().trimmed_primary_keys(),
                merge_engine: core_options.merge_engine().unwrap(),
                sequence_fields: Vec::new(),
                read_batch_size: core_options.read_batch_size().unwrap(),
                merge_splits: true,
                max_merge_input_streams: None,
                parquet_read_budget: Some(budget),
            },
        )
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

    #[test]
    fn widen_partial_update_projection_with_sequence_fields() {
        let table_fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "version".to_string(), DataType::Int(IntType::new())),
            DataField::new(2, "price".to_string(), DataType::Int(IntType::new())),
        ];
        let options = HashMap::from([
            ("merge-engine".to_string(), "partial-update".to_string()),
            (
                "fields.version.sequence-group".to_string(),
                "price".to_string(),
            ),
        ]);

        let widened = widen_partial_update_sequence_group_fields(
            MergeEngine::PartialUpdate,
            &options,
            &table_fields,
            &["id".to_string()],
            vec![table_fields[2].clone()],
        )
        .unwrap();

        assert_eq!(
            widened.iter().map(DataField::name).collect::<Vec<_>>(),
            vec!["price", "version"]
        );
    }

    #[tokio::test]
    async fn kv_merge_rejects_too_many_sorted_runs_on_read_path() {
        let file_io = test_file_io();
        let table_path = "memory:/kv_merge_fan_in_limit";
        let table = pk_table(&file_io, table_path, &[]);
        let core_options = table.schema().core_options();
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(format!("{table_path}/bucket-0"))
            .with_total_buckets(1)
            .with_data_files(
                (0..257)
                    .map(|i| dummy_data_file(format!("file-{i}.parquet")))
                    .collect(),
            )
            .build()
            .unwrap();
        let reader = KeyValueFileReader::new(
            table.file_io().clone(),
            KeyValueReadConfig {
                table_name: table.identifier().full_name(),
                table_options: table.schema().options().clone(),
                schema_manager: table.schema_manager().clone(),
                table_schema_id: table.schema().id(),
                table_fields: table.schema().fields().to_vec(),
                read_type: table.schema().fields().to_vec(),
                predicates: Vec::new(),
                primary_keys: table.schema().trimmed_primary_keys(),
                merge_engine: core_options.merge_engine().unwrap(),
                sequence_fields: Vec::new(),
                read_batch_size: core_options.read_batch_size().unwrap(),
                merge_splits: true,
                max_merge_input_streams: Some(256),
                parquet_read_budget: None,
            },
        );

        let err = reader
            .read(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Unsupported { message } if message.contains("sorted-run input streams")),
            "KV merge must fail before opening an unbounded number of sorted-run inputs"
        );
    }

    #[tokio::test]
    async fn dv_mor_table_read_bounds_merge_fan_in() {
        let file_io = test_file_io();
        let table_path = "memory:/dv_mor_merge_fan_in_limit";
        let table = pk_table(
            &file_io,
            table_path,
            &[
                ("deletion-vectors.enabled", "true"),
                ("deletion-vectors.merge-on-read", "true"),
            ],
        );
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(format!("{table_path}/bucket-0"))
            .with_total_buckets(1)
            .with_data_files(
                (0..257)
                    .map(|i| dummy_data_file(format!("file-{i}.parquet")))
                    .collect(),
            )
            .build()
            .unwrap();

        let err = table
            .new_read_builder()
            .new_read()
            .unwrap()
            .to_arrow(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Unsupported { message } if message.contains("sorted-run input streams")),
            "DV merge-on-read must reject unbounded production merge fan-in"
        );
    }

    #[test]
    fn sorted_run_planning_limits_each_section_to_overlap_depth() {
        let file = |name: &str, min: i32, max: i32| {
            let mut file = dummy_data_file(name.to_string());
            file.min_key = int_key(min);
            file.max_key = int_key(max);
            file
        };
        let split = Arc::new(
            DataSplitBuilder::new()
                .with_snapshot(1)
                .with_partition(BinaryRow::new(0))
                .with_bucket(0)
                .with_bucket_path("memory:/sorted-run-plan/bucket-0".to_string())
                .with_total_buckets(1)
                .with_data_files(vec![
                    file("a", 1, 10),
                    file("b", 5, 15),
                    file("c", 20, 30),
                    file("d", 25, 35),
                    file("e", 40, 50),
                    file("f", 45, 55),
                ])
                .build()
                .unwrap(),
        );
        let comparator =
            super::super::merge_tree_split_generator::KeyComparator::new(vec![DataType::Int(
                IntType::new(),
            )]);

        let grouped = plan_merge_groups(std::slice::from_ref(&split), Some(&comparator), false);
        assert_eq!(grouped.len(), 3);
        assert!(grouped.iter().all(|section| section.len() == 2));

        let fallback = plan_merge_groups(std::slice::from_ref(&split), None, false);
        assert_eq!(fallback.len(), 1);
        assert_eq!(fallback[0].len(), 6);
    }

    #[test]
    fn sorted_run_planning_merges_disjoint_sections_across_splits() {
        let file = |name: String, key: i32| {
            let mut file = dummy_data_file(name);
            file.min_key = int_key(key);
            file.max_key = int_key(key);
            file
        };
        let split = |path: &str, files| {
            Arc::new(
                DataSplitBuilder::new()
                    .with_snapshot(1)
                    .with_partition(BinaryRow::new(0))
                    .with_bucket(0)
                    .with_bucket_path(path.to_string())
                    .with_total_buckets(1)
                    .with_data_files(files)
                    .build()
                    .unwrap(),
            )
        };
        let first = split(
            "memory:/sorted-run-plan/first",
            (0..129)
                .map(|index| file(format!("first-{index}"), index * 4))
                .collect(),
        );
        let second = split(
            "memory:/sorted-run-plan/second",
            (0..128)
                .map(|index| file(format!("second-{index}"), index * 4 + 2))
                .collect(),
        );
        let comparator =
            super::super::merge_tree_split_generator::KeyComparator::new(vec![DataType::Int(
                IntType::new(),
            )]);

        let grouped = plan_merge_groups(&[first, second], Some(&comparator), true);
        assert_eq!(grouped.len(), 1);
        assert_eq!(grouped[0].len(), 1, "global overlap depth is one");
        ensure_merge_input_limit(grouped[0].len(), Some(256)).unwrap();
        let files = &grouped[0][0].files;
        assert_eq!(files.len(), 257);
        assert_eq!(
            files
                .iter()
                .take(4)
                .map(|file| file.file.file_name.as_str())
                .collect::<Vec<_>>(),
            vec!["first-0", "second-0", "first-1", "second-1"]
        );
        for file in files {
            let expected_path = if file.file.file_name.starts_with("first-") {
                "memory:/sorted-run-plan/first"
            } else {
                "memory:/sorted-run-plan/second"
            };
            assert_eq!(file.split.bucket_path(), expected_path);
        }
    }

    #[tokio::test]
    async fn kv_input_decode_honors_read_batch_size_without_changing_merge_batching() {
        let file_io = test_file_io();
        let table_path = "memory:/kv_read_batch_size";
        setup_dirs(&file_io, table_path).await;
        let table = pk_table(&file_io, table_path, &[("read.batch-size", "2")]);

        write_commit(
            &table,
            &int_batch(
                vec![1, 2, 3, 4, 5],
                vec![Some(10), Some(20), Some(30), Some(40), Some(50)],
            ),
        )
        .await;
        write_commit(
            &table,
            &int_batch(
                vec![1, 2, 3, 4, 5],
                vec![Some(11), Some(21), Some(31), Some(41), Some(51)],
            ),
        )
        .await;

        let read_builder = table.new_read_builder();
        let plan = read_builder.new_scan().plan().await.unwrap();
        let core_options = table.schema().core_options();
        let input_batch_sizes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let reader = KeyValueFileReader::new(
            table.file_io().clone(),
            KeyValueReadConfig {
                table_name: table.identifier().full_name(),
                table_options: table.schema().options().clone(),
                schema_manager: table.schema_manager().clone(),
                table_schema_id: table.schema().id(),
                table_fields: table.schema().fields().to_vec(),
                read_type: table.schema().fields().to_vec(),
                predicates: Vec::new(),
                primary_keys: table.schema().trimmed_primary_keys(),
                merge_engine: core_options.merge_engine().unwrap(),
                sequence_fields: core_options
                    .sequence_fields()
                    .iter()
                    .map(|field| field.to_string())
                    .collect(),
                read_batch_size: core_options.read_batch_size().unwrap(),
                merge_splits: false,
                max_merge_input_streams: None,
                parquet_read_budget: None,
            },
        )
        .with_input_batch_sizes(input_batch_sizes.clone());
        let batches = reader
            .read(plan.splits())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let mut decoded_batch_sizes = input_batch_sizes.lock().unwrap().clone();
        decoded_batch_sizes.sort_unstable();
        assert_eq!(decoded_batch_sizes, vec![1, 1, 2, 2, 2, 2]);
        assert_eq!(batches.len(), 1, "merge output batching stays independent");
        assert_eq!(batches[0].num_rows(), 5);
        assert_eq!(int_column(&batches, "value"), vec![11, 21, 31, 41, 51]);
    }

    #[tokio::test]
    async fn kv_merge_with_multiple_runs_does_not_deadlock() {
        let file_io = test_file_io();
        let table_path = "memory:/kv_shared_parquet_budget";
        setup_dirs(&file_io, table_path).await;
        let table = pk_table(
            &file_io,
            table_path,
            &[
                ("read.batch-size", "1"),
                ("read.parquet.row-group.parallelism", "2"),
            ],
        );
        let first =
            write_multi_row_group_kv_file(&file_io, table_path, "first.parquet", 0, 0, 10).await;
        let second =
            write_multi_row_group_kv_file(&file_io, table_path, "second.parquet", 0, 1, 11).await;
        let split = Arc::new(
            DataSplitBuilder::new()
                .with_snapshot(1)
                .with_partition(BinaryRow::new(0))
                .with_bucket(0)
                .with_bucket_path(format!("{table_path}/bucket-0"))
                .with_total_buckets(1)
                .with_data_files(vec![first, second])
                .build()
                .unwrap(),
        );
        let comparator =
            super::super::merge_tree_split_generator::KeyComparator::new(vec![DataType::Int(
                IntType::new(),
            )]);
        let planned = plan_merge_groups(std::slice::from_ref(&split), Some(&comparator), false);
        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].len(), 2);
        let core_options = table.schema().core_options();
        let reader = KeyValueFileReader::new(
            table.file_io().clone(),
            KeyValueReadConfig {
                table_name: table.identifier().full_name(),
                table_options: table.schema().options().clone(),
                schema_manager: table.schema_manager().clone(),
                table_schema_id: table.schema().id(),
                table_fields: table.schema().fields().to_vec(),
                read_type: table.schema().fields().to_vec(),
                predicates: Vec::new(),
                primary_keys: table.schema().trimmed_primary_keys(),
                merge_engine: core_options.merge_engine().unwrap(),
                sequence_fields: Vec::new(),
                read_batch_size: core_options.read_batch_size().unwrap(),
                merge_splits: false,
                max_merge_input_streams: None,
                parquet_read_budget: Some(Arc::new(ParquetReadBudget::new(2, 256 << 20).unwrap())),
            },
        );
        let batches = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reader
                .read(std::slice::from_ref(split.as_ref()))
                .unwrap()
                .try_collect::<Vec<_>>(),
        )
        .await
        .expect("multiple sorted-run inputs must not deadlock on shared Parquet permits")
        .unwrap();

        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            128
        );
    }

    #[tokio::test]
    async fn single_sorted_run_uses_shared_budget_across_files() {
        let file_io = test_file_io();
        let table_path = "memory:/kv_single_run_shared_budget";
        setup_dirs(&file_io, table_path).await;
        let table = pk_table(
            &file_io,
            table_path,
            &[
                ("read.batch-size", "1"),
                ("read.parquet.row-group.parallelism", "1"),
            ],
        );
        let low =
            write_multi_row_group_kv_file(&file_io, table_path, "low.parquet", 0, 0, 10).await;
        let high =
            write_multi_row_group_kv_file(&file_io, table_path, "high.parquet", 200, 0, 20).await;
        let split = Arc::new(
            DataSplitBuilder::new()
                .with_snapshot(1)
                .with_partition(BinaryRow::new(0))
                .with_bucket(0)
                .with_bucket_path(format!("{table_path}/bucket-0"))
                .with_total_buckets(1)
                .with_data_files(vec![high, low])
                .build()
                .unwrap(),
        );
        let comparator =
            super::super::merge_tree_split_generator::KeyComparator::new(vec![DataType::Int(
                IntType::new(),
            )]);
        let planned = plan_merge_groups(std::slice::from_ref(&split), Some(&comparator), true);
        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].len(), 1);
        assert_eq!(planned[0][0].files.len(), 2);

        let reader = kv_reader_with_budget(
            &table,
            Arc::new(ParquetReadBudget::new(1, 256 << 20).unwrap()),
        );
        let batches = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reader
                .read(std::slice::from_ref(split.as_ref()))
                .unwrap()
                .try_collect::<Vec<_>>(),
        )
        .await
        .expect("one sorted run must reuse a single shared Parquet permit across files")
        .unwrap();

        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            256
        );
    }

    #[tokio::test]
    async fn concurrent_single_runs_share_one_parquet_budget() {
        let file_io = test_file_io();
        let table_path = "memory:/kv_concurrent_single_run_budget";
        setup_dirs(&file_io, table_path).await;
        let table = pk_table(
            &file_io,
            table_path,
            &[
                ("read.batch-size", "1"),
                ("read.parquet.row-group.parallelism", "1"),
            ],
        );
        let first_low =
            write_multi_row_group_kv_file(&file_io, table_path, "first-low.parquet", 0, 0, 10)
                .await;
        let first_high =
            write_multi_row_group_kv_file(&file_io, table_path, "first-high.parquet", 200, 0, 20)
                .await;
        let second_low =
            write_multi_row_group_kv_file(&file_io, table_path, "second-low.parquet", 400, 0, 30)
                .await;
        let second_high =
            write_multi_row_group_kv_file(&file_io, table_path, "second-high.parquet", 600, 0, 40)
                .await;
        let split = |files| {
            DataSplitBuilder::new()
                .with_snapshot(1)
                .with_partition(BinaryRow::new(0))
                .with_bucket(0)
                .with_bucket_path(format!("{table_path}/bucket-0"))
                .with_total_buckets(1)
                .with_data_files(files)
                .build()
                .unwrap()
        };
        let first_split = split(vec![first_high, first_low]);
        let second_split = split(vec![second_high, second_low]);
        let budget = Arc::new(ParquetReadBudget::new(1, 256 << 20).unwrap());
        let first_reader = kv_reader_with_budget(&table, budget.clone());
        let second_reader = kv_reader_with_budget(&table, budget);

        let (first_batches, second_batches) =
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                tokio::try_join!(
                    first_reader
                        .read(&[first_split])
                        .unwrap()
                        .try_collect::<Vec<_>>(),
                    second_reader
                        .read(&[second_split])
                        .unwrap()
                        .try_collect::<Vec<_>>()
                )
            })
            .await
            .expect("concurrent single-run readers must make progress with one shared permit")
            .unwrap();

        assert_eq!(
            first_batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            256
        );
        assert_eq!(
            second_batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            256
        );
    }

    #[tokio::test]
    async fn sorted_run_read_matches_per_file_fan_out() {
        let file_io = test_file_io();
        let table_path = "memory:/kv_sorted_run";
        let table = pk_table(&file_io, table_path, &[]);
        let low =
            write_multi_row_group_kv_file(&file_io, table_path, "low.parquet", 0, 0, 10).await;
        let high =
            write_multi_row_group_kv_file(&file_io, table_path, "high.parquet", 200, 0, 20).await;
        let split = |files| {
            DataSplitBuilder::new()
                .with_snapshot(1)
                .with_partition(BinaryRow::new(0))
                .with_bucket(0)
                .with_bucket_path(format!("{table_path}/bucket-0"))
                .with_total_buckets(1)
                .with_data_files(files)
                .build()
                .unwrap()
        };
        let grouped_split = split(vec![high.clone(), low.clone()]);
        let per_file_splits = vec![split(vec![high]), split(vec![low])];
        let core_options = table.schema().core_options();

        let read = |splits: &[DataSplit], merge_splits| {
            KeyValueFileReader::new(
                table.file_io().clone(),
                KeyValueReadConfig {
                    table_name: table.identifier().full_name(),
                    table_options: table.schema().options().clone(),
                    schema_manager: table.schema_manager().clone(),
                    table_schema_id: table.schema().id(),
                    table_fields: table.schema().fields().to_vec(),
                    read_type: table.schema().fields().to_vec(),
                    predicates: Vec::new(),
                    primary_keys: table.schema().trimmed_primary_keys(),
                    merge_engine: core_options.merge_engine().unwrap(),
                    sequence_fields: Vec::new(),
                    read_batch_size: core_options.read_batch_size().unwrap(),
                    merge_splits,
                    max_merge_input_streams: None,
                    parquet_read_budget: None,
                },
            )
            .read(splits)
            .unwrap()
        };

        let grouped = read(std::slice::from_ref(&grouped_split), false)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let per_file = read(&per_file_splits, true)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let expected = (0..128).chain(200..328).collect::<Vec<_>>();
        assert_eq!(int_column(&grouped, "id"), expected);
        assert_eq!(int_column(&grouped, "id"), int_column(&per_file, "id"));
    }

    #[tokio::test]
    async fn sorted_runs_preserve_global_merge_across_splits() {
        let file_io = test_file_io();
        let table_path = "memory:/kv_sorted_run_merge_splits";
        let table = pk_table(&file_io, table_path, &[]);
        let low =
            write_multi_row_group_kv_file(&file_io, table_path, "low.parquet", 0, 0, 10).await;
        let high =
            write_multi_row_group_kv_file(&file_io, table_path, "high.parquet", 300, 0, 20).await;
        let middle =
            write_multi_row_group_kv_file(&file_io, table_path, "middle.parquet", 100, 1, 30).await;
        let split_with_run = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(format!("{table_path}/bucket-0"))
            .with_total_buckets(1)
            .with_data_files(vec![high, low])
            .build()
            .unwrap();
        let overlapping_split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(format!("{table_path}/bucket-0"))
            .with_total_buckets(1)
            .with_data_files(vec![middle])
            .build()
            .unwrap();
        let core_options = table.schema().core_options();
        let reader = KeyValueFileReader::new(
            table.file_io().clone(),
            KeyValueReadConfig {
                table_name: table.identifier().full_name(),
                table_options: table.schema().options().clone(),
                schema_manager: table.schema_manager().clone(),
                table_schema_id: table.schema().id(),
                table_fields: table.schema().fields().to_vec(),
                read_type: table.schema().fields().to_vec(),
                predicates: Vec::new(),
                primary_keys: table.schema().trimmed_primary_keys(),
                merge_engine: core_options.merge_engine().unwrap(),
                sequence_fields: Vec::new(),
                read_batch_size: core_options.read_batch_size().unwrap(),
                merge_splits: true,
                max_merge_input_streams: Some(256),
                parquet_read_budget: None,
            },
        );
        let batches = reader
            .read(&[split_with_run, overlapping_split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let expected_ids = (0..228).chain(300..428).collect::<Vec<_>>();
        let expected_values = std::iter::repeat_n(10, 100)
            .chain(std::iter::repeat_n(30, 128))
            .chain(std::iter::repeat_n(20, 128))
            .collect::<Vec<_>>();
        assert_eq!(int_column(&batches, "id"), expected_ids);
        assert_eq!(int_column(&batches, "value"), expected_values);
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

    #[tokio::test]
    async fn kv_read_partial_update_sequence_groups_with_projection() {
        let file_io = test_file_io();
        let table_path = "memory:/kv_partial_update_sequence_groups";
        setup_dirs(&file_io, table_path).await;

        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("seq_a", DataType::Int(IntType::new()))
            .column("value_a", DataType::Int(IntType::new()))
            .column("seq_b", DataType::Int(IntType::new()))
            .column("value_b", DataType::Int(IntType::new()))
            .primary_key(["id"])
            .option("bucket", "1")
            .option("merge-engine", "partial-update")
            .build()
            .unwrap();
        let table = Table::new(
            file_io.clone(),
            Identifier::new("default", "kv_partial_update_sequence_groups_t"),
            table_path.to_string(),
            TableSchema::new(0, &schema),
            None,
        );
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("seq_a", ArrowDataType::Int32, true),
            ArrowField::new("value_a", ArrowDataType::Int32, true),
            ArrowField::new("seq_b", ArrowDataType::Int32, true),
            ArrowField::new("value_b", ArrowDataType::Int32, true),
        ]));
        let make = |seq_a, value_a, seq_b, value_b| {
            RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1])),
                    Arc::new(Int32Array::from(vec![seq_a])),
                    Arc::new(Int32Array::from(vec![value_a])),
                    Arc::new(Int32Array::from(vec![seq_b])),
                    Arc::new(Int32Array::from(vec![value_b])),
                ],
            )
            .unwrap()
        };

        write_commit(&table, &make(10, 100, 10, 1000)).await;
        write_commit(&table, &make(9, 200, 11, 2000)).await;

        let sequence_group_schema = table.schema().copy_with_options(HashMap::from([
            (
                "fields.seq_a.sequence-group".to_string(),
                "value_a".to_string(),
            ),
            (
                "fields.seq_b.sequence-group".to_string(),
                "value_b".to_string(),
            ),
        ]));
        let sequence_group_table = Table::new(
            file_io,
            Identifier::new("default", "kv_partial_update_sequence_groups_t"),
            table_path.to_string(),
            sequence_group_schema,
            None,
        );

        let batches = read_rows(
            &sequence_group_table,
            Some(&["id", "value_a", "value_b"]),
            None,
        )
        .await;

        assert_eq!(int_column(&batches, "value_a"), vec![100]);
        assert_eq!(int_column(&batches, "value_b"), vec![2000]);
        for batch in batches {
            assert_eq!(
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect::<Vec<_>>(),
                vec!["id", "value_a", "value_b"]
            );
        }
    }

    #[tokio::test]
    async fn kv_read_partial_update_sequence_group_aggregation_with_projection() {
        let file_io = test_file_io();
        let table_path = "memory:/kv_partial_update_sequence_group_aggregation";
        setup_dirs(&file_io, table_path).await;

        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("version", DataType::Int(IntType::new()))
            .column("value", DataType::VarChar(VarCharType::string_type()))
            .primary_key(["id"])
            .option("bucket", "1")
            .option("merge-engine", "partial-update")
            .build()
            .unwrap();
        let table = Table::new(
            file_io.clone(),
            Identifier::new("default", "kv_partial_update_sequence_group_aggregation_t"),
            table_path.to_string(),
            TableSchema::new(0, &schema),
            None,
        );
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("version", ArrowDataType::Int32, true),
            ArrowField::new("value", ArrowDataType::Utf8, true),
        ]));
        let make = |id, version, value| {
            RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![id])),
                    Arc::new(Int32Array::from(vec![version])),
                    Arc::new(StringArray::from(vec![value])),
                ],
            )
            .unwrap()
        };

        write_commit(&table, &make(1, 10, "b")).await;
        write_commit(&table, &make(1, 9, "a")).await;
        write_commit(&table, &make(1, 11, "c")).await;
        write_commit(&table, &make(2, 5, "x")).await;
        write_commit(&table, &make(2, 6, "y")).await;

        let aggregation_schema = table.schema().copy_with_options(HashMap::from([
            (
                "fields.version.sequence-group".to_string(),
                "value".to_string(),
            ),
            (
                "fields.value.aggregate-function".to_string(),
                "listagg".to_string(),
            ),
        ]));
        let aggregation_table = Table::new(
            file_io,
            Identifier::new("default", "kv_partial_update_sequence_group_aggregation_t"),
            table_path.to_string(),
            aggregation_schema,
            None,
        );

        let batches = read_rows(&aggregation_table, Some(&["id", "value"]), None).await;

        let mut rows = batches
            .iter()
            .flat_map(|batch| {
                let ids = batch
                    .column(batch.schema().index_of("id").unwrap())
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let index = batch.schema().index_of("value").unwrap();
                let array = batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                (0..array.len())
                    .map(|row| (ids.value(row), array.value(row).to_string()))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        rows.sort_by_key(|row| row.0);
        assert_eq!(rows, vec![(1, "a,b,c".to_string()), (2, "x,y".to_string())]);
        for batch in batches {
            assert_eq!(
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect::<Vec<_>>(),
                vec!["id", "value"]
            );
        }
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
