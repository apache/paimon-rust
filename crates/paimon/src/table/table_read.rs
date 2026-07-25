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

use super::data_evolution_reader::DataEvolutionReader;
use super::data_file_reader::DataFileReader;
use super::format_table_read::FormatTableRead;
use super::incremental_scan::{IncrementalPlan, IncrementalScanMode, IncrementalSplit};
use super::kv_file_reader::{KeyValueFileReader, KeyValueReadConfig};
use super::read_builder::split_scan_predicates;
use super::{ArrowRecordBatchStream, Table};
use crate::arrow::build_target_arrow_schema;
use crate::spec::{
    BigIntType, CoreOptions, DataField, DataType, MergeEngine, Predicate, TinyIntType,
    ROW_KIND_FIELD_ID, ROW_KIND_FIELD_NAME, SEQUENCE_NUMBER_FIELD_ID, SEQUENCE_NUMBER_FIELD_NAME,
    VALUE_KIND_FIELD_ID, VALUE_KIND_FIELD_NAME,
};
use crate::DataSplit;
use arrow_array::{
    builder::StringBuilder, Array, ArrayRef, RecordBatch, RecordBatchOptions, StringArray,
    UInt32Array,
};
use arrow_schema::Schema as ArrowSchema;
use arrow_select::concat::concat as arrow_concat;
use arrow_select::take::take;
use futures::{stream, StreamExt};
use std::cmp::Ordering;
use std::sync::Arc;

/// Table read: reads data from splits (e.g. produced by [TableScan::plan]).
///
/// Reference: [pypaimon.read.table_read.TableRead](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/read/table_read.py)
#[derive(Debug, Clone)]
pub struct TableRead<'a>(TableReadKind<'a>);

#[derive(Debug, Clone)]
enum TableReadKind<'a> {
    Paimon(PaimonTableRead<'a>),
    Format(FormatTableRead<'a>),
}

impl<'a> TableRead<'a> {
    /// Create a new TableRead with a specific read type (projected fields).
    pub fn new(
        table: &'a Table,
        read_type: Vec<DataField>,
        data_predicates: Vec<Predicate>,
    ) -> Self {
        if table.is_format_table() {
            Self::new_format(table, read_type, data_predicates, None)
        } else {
            Self(TableReadKind::Paimon(PaimonTableRead::new(
                table,
                read_type,
                data_predicates,
            )))
        }
    }

    pub(crate) fn new_format(
        table: &'a Table,
        read_type: Vec<DataField>,
        data_predicates: Vec<Predicate>,
        limit: Option<usize>,
    ) -> Self {
        Self(TableReadKind::Format(FormatTableRead::new(
            table,
            read_type,
            data_predicates,
            limit,
        )))
    }

    /// Schema (fields) that this read will produce.
    pub fn read_type(&self) -> &[DataField] {
        match &self.0 {
            TableReadKind::Paimon(read) => read.read_type(),
            TableReadKind::Format(read) => read.read_type(),
        }
    }

    /// Data predicates for read-side pruning.
    pub fn data_predicates(&self) -> &[Predicate] {
        match &self.0 {
            TableReadKind::Paimon(read) => read.data_predicates(),
            TableReadKind::Format(read) => read.data_predicates(),
        }
    }

    /// Table for this read.
    pub fn table(&self) -> &Table {
        match &self.0 {
            TableReadKind::Paimon(read) => read.table(),
            TableReadKind::Format(read) => read.table(),
        }
    }

    /// Set a filter predicate.
    pub fn with_filter(self, filter: Predicate) -> Self {
        match self.0 {
            TableReadKind::Paimon(read) => Self(TableReadKind::Paimon(read.with_filter(filter))),
            TableReadKind::Format(read) => Self(TableReadKind::Format(read.with_filter(filter))),
        }
    }

    /// Attach an engine-specific Parquet decoder-filter factory.
    ///
    /// The hook is used only by schema-identical raw reads. Callers must still
    /// enforce the expression after the scan because an individual file may not
    /// be able to build a decoder filter.
    pub fn with_row_filter_factory(self, factory: Arc<dyn crate::arrow::RowFilterFactory>) -> Self {
        match self.0 {
            TableReadKind::Paimon(read) => {
                Self(TableReadKind::Paimon(read.with_row_filter_factory(factory)))
            }
            TableReadKind::Format(read) => {
                Self(TableReadKind::Format(read.with_row_filter_factory(factory)))
            }
        }
    }

    /// Returns an [`ArrowRecordBatchStream`].
    pub fn to_arrow(&self, data_splits: &[DataSplit]) -> crate::Result<ArrowRecordBatchStream> {
        match &self.0 {
            TableReadKind::Paimon(read) => read.to_arrow(data_splits),
            TableReadKind::Format(read) => read.to_arrow(data_splits),
        }
    }

    /// Returns an [`ArrowRecordBatchStream`] for an incremental scan plan.
    ///
    /// Delta/Changelog use [`IncrementalSplit::Data`]. Diff uses
    /// [`IncrementalSplit::DiffPair`] and emits after-image rows only.
    pub fn to_incremental_arrow(
        &self,
        plan: &IncrementalPlan,
    ) -> crate::Result<ArrowRecordBatchStream> {
        self.ensure_query_auth_allowed()?;
        plan.validate()?;
        match &self.0 {
            TableReadKind::Paimon(read) => read.to_incremental_arrow(plan),
            TableReadKind::Format(_) => Err(crate::Error::Unsupported {
                message: "Format tables do not support incremental batch read".to_string(),
            }),
        }
    }

    /// Returns an audit-log [`ArrowRecordBatchStream`] for an incremental plan.
    ///
    /// Output schema is `rowkind` (+ optional `_SEQUENCE_NUMBER`) followed by
    /// the projected user columns. Primary-key Delta and Changelog rows take
    /// kinds from `_VALUE_KIND`; append-only Delta rows are `+I`. Diff emits
    /// `+I`/`-U`/`+U`/`-D` from before/after image comparison.
    pub fn to_audit_log_arrow(
        &self,
        plan: &IncrementalPlan,
    ) -> crate::Result<ArrowRecordBatchStream> {
        self.ensure_query_auth_allowed()?;
        plan.validate()?;
        match &self.0 {
            TableReadKind::Paimon(read) => read.to_audit_log_arrow(plan),
            TableReadKind::Format(_) => Err(crate::Error::Unsupported {
                message: "Format tables do not support audit log batch read".to_string(),
            }),
        }
    }

    fn ensure_query_auth_allowed(&self) -> crate::Result<()> {
        CoreOptions::new(self.table().schema().options()).ensure_read_authorized()
    }
}

#[derive(Debug, Clone)]
struct PaimonTableRead<'a> {
    table: &'a Table,
    read_type: Vec<DataField>,
    data_predicates: Vec<Predicate>,
    row_filter_factory: Option<Arc<dyn crate::arrow::RowFilterFactory>>,
}

impl<'a> PaimonTableRead<'a> {
    /// Create a new TableRead with a specific read type (projected fields).
    pub fn new(
        table: &'a Table,
        read_type: Vec<DataField>,
        data_predicates: Vec<Predicate>,
    ) -> Self {
        Self {
            table,
            read_type,
            data_predicates,
            row_filter_factory: None,
        }
    }

    /// Schema (fields) that this read will produce.
    pub fn read_type(&self) -> &[DataField] {
        &self.read_type
    }

    /// Data predicates for read-side pruning.
    pub fn data_predicates(&self) -> &[Predicate] {
        &self.data_predicates
    }

    /// Table for this read.
    pub fn table(&self) -> &Table {
        self.table
    }

    /// Set a filter predicate. Used conservatively for read-side pruning and
    /// enforced exactly by residual filtering on append, data-evolution, and
    /// primary-key merge read paths (see
    /// [`ReadBuilder::with_filter`](crate::table::ReadBuilder::with_filter)
    /// for per-format exceptions).
    pub fn with_filter(mut self, filter: Predicate) -> Self {
        let (_, data_predicates) = split_scan_predicates(self.table, filter);
        // Keep the FULL data predicate (including `And`/`Or`/`Not`). Native
        // pushdown / stats pruning skip compound nodes they cannot use, and the
        // residual pass applies the full predicate exactly. Pruning here would
        // drop compound predicates before the residual could enforce them.
        self.data_predicates = data_predicates;
        self
    }

    fn with_row_filter_factory(mut self, factory: Arc<dyn crate::arrow::RowFilterFactory>) -> Self {
        self.row_filter_factory = Some(factory);
        self
    }

    /// Returns an [`ArrowRecordBatchStream`] for an incremental scan plan.
    pub fn to_incremental_arrow(
        &self,
        plan: &IncrementalPlan,
    ) -> crate::Result<ArrowRecordBatchStream> {
        if plan.mode() == IncrementalScanMode::Diff {
            return self.to_incremental_diff_arrow(plan);
        }

        let mut data_splits = Vec::new();
        for split in plan.splits() {
            match split {
                IncrementalSplit::Data(data) => data_splits.push(data.clone()),
                IncrementalSplit::DiffPair { .. } => {
                    return Err(crate::Error::UnexpectedError {
                        message: "DiffPair appeared in non-Diff incremental plan".to_string(),
                        source: None,
                    });
                }
            }
        }
        // Delta / Changelog rows are read as-is from planned files (no full-table
        // merge against historical base versions).
        self.new_data_file_reader()?.read(&data_splits)
    }

    fn to_incremental_diff_arrow(
        &self,
        plan: &IncrementalPlan,
    ) -> crate::Result<ArrowRecordBatchStream> {
        let pairs = diff_pairs(plan)?;
        let parallel = CoreOptions::new(self.table.schema().options()).diff_parallelism();
        let table = self.table.clone();
        let read_type = self.read_type.clone();
        let data_predicates = self.data_predicates.clone();

        Ok(Box::pin(async_stream::try_stream! {
            let mut workers = stream::iter(pairs.into_iter().map(|(before, after)| {
                let table = table.clone();
                let read_type = read_type.clone();
                let data_predicates = data_predicates.clone();
                let worker: ArrowRecordBatchStream = Box::pin(async_stream::try_stream! {
                    let pair_read =
                        PaimonTableRead::new(&table, read_type, data_predicates);
                    let mut pair_stream = pair_read.to_diff_after_image_stream(&before, &after)?;
                    while let Some(batch) = pair_stream.next().await {
                        yield batch?;
                    }
                });
                worker
            }))
            .flatten_unordered(parallel);
            while let Some(batch) = workers.next().await {
                yield batch?;
            }
        }))
    }

    /// Returns an audit-log stream for a planned incremental scan.
    pub fn to_audit_log_arrow(
        &self,
        plan: &IncrementalPlan,
    ) -> crate::Result<ArrowRecordBatchStream> {
        match plan.mode() {
            IncrementalScanMode::Diff => self.audit_diff_stream(plan),
            IncrementalScanMode::Delta => {
                self.audit_raw_stream(plan, !self.table.schema().primary_keys().is_empty())
            }
            IncrementalScanMode::Changelog => self.audit_raw_stream(plan, true),
            IncrementalScanMode::Auto => Err(crate::Error::DataInvalid {
                message: "Incremental plan mode Auto must be resolved before consumption"
                    .to_string(),
                source: None,
            }),
        }
    }

    fn audit_raw_stream(
        &self,
        plan: &IncrementalPlan,
        has_value_kind: bool,
    ) -> crate::Result<ArrowRecordBatchStream> {
        plan.validate()?;
        let data_splits = plan.data_splits();
        let user_read_type = self.read_type.clone();
        let include_sequence = audit_sequence_number_enabled(self.table);
        let audit_schema = audit_schema_for_read_type(&user_read_type, include_sequence)?;

        let mut read_type = user_read_type.clone();
        if include_sequence {
            read_type.insert(
                0,
                DataField::new(
                    SEQUENCE_NUMBER_FIELD_ID,
                    SEQUENCE_NUMBER_FIELD_NAME.to_string(),
                    DataType::BigInt(BigIntType::new()),
                ),
            );
        }
        if has_value_kind {
            read_type.push(DataField::new(
                VALUE_KIND_FIELD_ID,
                VALUE_KIND_FIELD_NAME.to_string(),
                DataType::TinyInt(TinyIntType::new()),
            ));
        }

        let reader = DataFileReader::new(
            self.table.file_io.clone(),
            self.table.schema_manager().clone(),
            self.table.schema().id(),
            self.table.schema.fields().to_vec(),
            read_type,
            self.data_predicates.clone(),
        )
        .with_batch_size(Some(self.table.schema().core_options().read_batch_size()?));
        let raw_stream = reader.read(&data_splits)?;

        Ok(Box::pin(async_stream::try_stream! {
            futures::pin_mut!(raw_stream);
            while let Some(batch) = raw_stream.next().await {
                let batch = batch?;
                let rowkind_col: ArrayRef = if has_value_kind {
                    let col = batch
                        .column_by_name(VALUE_KIND_FIELD_NAME)
                        .ok_or_else(|| crate::Error::DataInvalid {
                            message: "Changelog audit read missing _VALUE_KIND column".to_string(),
                            source: None,
                        })?;
                    Arc::new(rowkind_array_from_column(col)?)
                } else {
                    let inserts: Vec<&'static str> = (0..batch.num_rows()).map(|_| "+I").collect();
                    Arc::new(StringArray::from(inserts))
                };

                let mut columns: Vec<ArrayRef> = vec![rowkind_col];
                if include_sequence {
                    let seq_col = batch
                        .column_by_name(SEQUENCE_NUMBER_FIELD_NAME)
                        .ok_or_else(|| crate::Error::DataInvalid {
                            message: "Audit read missing _SEQUENCE_NUMBER column".to_string(),
                            source: None,
                        })?;
                    columns.push(seq_col.clone());
                }
                for field in &user_read_type {
                    let col = batch
                        .column_by_name(field.name())
                        .ok_or_else(|| crate::Error::DataInvalid {
                            message: format!(
                                "Audit read missing column '{}'",
                                field.name()
                            ),
                            source: None,
                        })?;
                    columns.push(col.clone());
                }
                yield RecordBatch::try_new(audit_schema.clone(), columns).map_err(|e| {
                    crate::Error::UnexpectedError {
                        message: format!("Failed to build audit log batch: {e}"),
                        source: Some(Box::new(e)),
                    }
                })?;
            }
        }))
    }

    fn audit_diff_stream(&self, plan: &IncrementalPlan) -> crate::Result<ArrowRecordBatchStream> {
        let pairs = diff_pairs(plan)?;
        let parallel = CoreOptions::new(self.table.schema().options()).diff_parallelism();
        let table = self.table.clone();
        let read_type = self.read_type.clone();
        let data_predicates = self.data_predicates.clone();

        Ok(Box::pin(async_stream::try_stream! {
            let mut workers = stream::iter(pairs.into_iter().map(|(before, after)| {
                let table = table.clone();
                let read_type = read_type.clone();
                let data_predicates = data_predicates.clone();
                let worker: ArrowRecordBatchStream = Box::pin(async_stream::try_stream! {
                    let pair_read = PaimonTableRead::new(&table, read_type, data_predicates);
                    let mut pair_stream =
                        pair_read.to_audit_log_arrow_for_diff(&before, &after)?;
                    while let Some(batch) = pair_stream.next().await {
                        yield batch?;
                    }
                });
                worker
            }))
            .flatten_unordered(parallel);
            while let Some(batch) = workers.next().await {
                yield batch?;
            }
        }))
    }

    fn to_audit_log_arrow_for_diff(
        &self,
        before: &[DataSplit],
        after: &[DataSplit],
    ) -> crate::Result<ArrowRecordBatchStream> {
        let include_sequence = audit_sequence_number_enabled(self.table);
        let audit_schema = audit_schema_for_read_type(&self.read_type, include_sequence)?;

        let mut diff_read_type = self.table.schema().fields().to_vec();
        ensure_diff_supported_read_type(&diff_read_type)?;
        if include_sequence {
            diff_read_type.insert(
                0,
                DataField::new(
                    SEQUENCE_NUMBER_FIELD_ID,
                    SEQUENCE_NUMBER_FIELD_NAME.to_string(),
                    DataType::BigInt(BigIntType::new()),
                ),
            );
        }

        let key_indices = primary_key_indices(self.table, &diff_read_type)?;
        let value_indices = value_indices_for_diff(self.table, &diff_read_type);

        let before = before.to_vec();
        let after = after.to_vec();
        let table = self.table.clone();
        let read_type_for_output = self.read_type.clone();
        let data_predicates = self.data_predicates.clone();

        Ok(Box::pin(async_stream::try_stream! {
            let core_options = CoreOptions::new(table.schema().options());
            let pair_read = PaimonTableRead::new(&table, diff_read_type.clone(), data_predicates);
            let before_stream =
                pair_read.read_pk_sorted_for_diff_with_type(&before, &core_options, &diff_read_type)?;
            let after_stream =
                pair_read.read_pk_sorted_for_diff_with_type(&after, &core_options, &diff_read_type)?;
            let mut bc = ArrowCursor::new(before_stream).await?;
            let mut ac = ArrowCursor::new(after_stream).await?;
            let mut data_col_indices: Option<Vec<usize>> = None;
            let mut builder = AuditBatchBuilder::new(audit_schema.clone());

            while bc.alive() || ac.alive() {
                let indices = data_col_indices.get_or_insert_with(|| {
                    let sample = if bc.alive() {
                        bc.batch()
                    } else {
                        ac.batch()
                    };
                    diff_output_col_indices(sample, &read_type_for_output, include_sequence)
                        .expect("diff output column indices")
                });
                if !builder.has_data_columns() {
                    builder.set_data_col_indices(indices.clone());
                }
                match cursor_cmp(&bc, &ac, &key_indices, &value_indices)? {
                    CursorOrd::BeforeOnly => {
                        builder.push("-D", bc.batch(), bc.row());
                        bc.advance().await?;
                    }
                    CursorOrd::AfterOnly => {
                        builder.push("+I", ac.batch(), ac.row());
                        ac.advance().await?;
                    }
                    CursorOrd::EqualSame => {
                        bc.advance().await?;
                        ac.advance().await?;
                    }
                    CursorOrd::EqualDiff => {
                        builder.push("-U", bc.batch(), bc.row());
                        builder.push("+U", ac.batch(), ac.row());
                        bc.advance().await?;
                        ac.advance().await?;
                    }
                }
                if builder.len() >= DIFF_BATCH_SIZE {
                    yield builder.flush()?;
                }
            }
            if builder.len() > 0 {
                yield builder.flush()?;
            }
        }))
    }

    fn to_diff_after_image_stream(
        &self,
        before: &[DataSplit],
        after: &[DataSplit],
    ) -> crate::Result<ArrowRecordBatchStream> {
        let diff_read_type = self.table.schema().fields().to_vec();
        ensure_diff_supported_read_type(&diff_read_type)?;
        let key_indices = primary_key_indices(self.table, &diff_read_type)?;
        let value_indices = value_indices_for_diff(self.table, &diff_read_type);
        let output_schema = build_target_arrow_schema(&self.read_type)?;
        let output_col_indices = self
            .read_type
            .iter()
            .map(|field| {
                diff_read_type
                    .iter()
                    .position(|candidate| candidate.id() == field.id())
                    .ok_or_else(|| crate::Error::DataInvalid {
                        message: format!("Diff read missing projected column '{}'", field.name()),
                        source: None,
                    })
            })
            .collect::<crate::Result<Vec<_>>>()?;

        let table = self.table.clone();
        let data_predicates = self.data_predicates.clone();
        let before = before.to_vec();
        let after = after.to_vec();

        Ok(Box::pin(async_stream::try_stream! {
            let core_options = CoreOptions::new(table.schema().options());
            let pair_read = PaimonTableRead::new(&table, diff_read_type.clone(), data_predicates);
            let before_stream = pair_read.read_pk_sorted_for_diff_with_type(
                &before,
                &core_options,
                &diff_read_type,
            )?;
            let after_stream = pair_read.read_pk_sorted_for_diff_with_type(
                &after,
                &core_options,
                &diff_read_type,
            )?;
            let mut bc = ArrowCursor::new(before_stream).await?;
            let mut ac = ArrowCursor::new(after_stream).await?;
            let mut builder =
                DiffAfterImageBatchBuilder::new(output_schema.clone(), output_col_indices.clone());

            while bc.alive() || ac.alive() {
                match cursor_cmp(&bc, &ac, &key_indices, &value_indices)? {
                    CursorOrd::BeforeOnly => {
                        bc.advance().await?;
                    }
                    CursorOrd::AfterOnly => {
                        builder.push(ac.batch(), ac.row());
                        ac.advance().await?;
                    }
                    CursorOrd::EqualSame => {
                        bc.advance().await?;
                        ac.advance().await?;
                    }
                    CursorOrd::EqualDiff => {
                        builder.push(ac.batch(), ac.row());
                        bc.advance().await?;
                        ac.advance().await?;
                    }
                }
                if builder.len() >= DIFF_BATCH_SIZE {
                    yield builder.flush()?;
                }
            }
            if builder.len() > 0 {
                yield builder.flush()?;
            }
        }))
    }

    fn read_pk_sorted_for_diff_with_type(
        &self,
        splits: &[DataSplit],
        core_options: &CoreOptions,
        read_type: &[DataField],
    ) -> crate::Result<ArrowRecordBatchStream> {
        if splits.is_empty() {
            return Ok(Box::pin(futures::stream::empty()));
        }
        for split in splits {
            if split
                .data_deletion_files()
                .is_some_and(|files| files.iter().any(|file| file.is_some()))
            {
                return Err(crate::Error::Unsupported {
                    message: "Batch incremental Diff does not support deletion vectors".to_string(),
                });
            }
        }
        let reader = KeyValueFileReader::new(
            self.table.file_io.clone(),
            KeyValueReadConfig {
                table_name: self.table.identifier().full_name(),
                table_options: self.table.schema().options().clone(),
                schema_manager: self.table.schema_manager().clone(),
                table_schema_id: self.table.schema().id(),
                table_fields: self.table.schema.fields().to_vec(),
                read_type: read_type.to_vec(),
                predicates: self.data_predicates.clone(),
                primary_keys: self.table.schema.trimmed_primary_keys(),
                merge_engine: core_options.merge_engine()?,
                sequence_fields: core_options
                    .sequence_fields()
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
                read_batch_size: core_options.read_batch_size()?,
                merge_splits: true,
                max_merge_file_streams: Some(256),
            },
        );
        reader.read(splits)
    }

    /// Returns an [`ArrowRecordBatchStream`].
    pub fn to_arrow(&self, data_splits: &[DataSplit]) -> crate::Result<ArrowRecordBatchStream> {
        let has_primary_keys = !self.table.schema.primary_keys().is_empty();
        let core_options = self.table.schema.core_options();
        // Fail closed for a direct `TableRead` (bypassing `ReadBuilder::new_read`).
        core_options.ensure_read_authorized()?;
        let merge_engine = core_options.merge_engine()?;

        // Route supported PK merge engines through the split-aware reader.
        // Deduplicate may mix raw and KV splits. Partial-update and aggregation
        // use KV reads normally, but fully materialized DV plans can read raw.
        if has_primary_keys
            && matches!(
                merge_engine,
                MergeEngine::Deduplicate | MergeEngine::PartialUpdate | MergeEngine::Aggregation
            )
        {
            return self.read_pk(data_splits, &core_options);
        }

        if core_options.data_evolution_enabled() {
            self.read_with_evolution(data_splits, &core_options)
        } else {
            self.read_raw(data_splits)
        }
    }

    /// Read PK table. For `Deduplicate`, splits marked raw convertible by scan
    /// planning (mirrors Java `DataSplit#convertToRawFiles`) use the faster
    /// DataFileReader; the rest go through KeyValueFileReader for sort-merge
    /// dedup. A fully materialized deletion-vector plan for `PartialUpdate` or
    /// `Aggregation` can also be read raw because DVs already mask stale rows.
    /// Plans that still need any per-key merge fail closed because mixing raw
    /// and merged outputs would produce incorrect results.
    fn read_pk(
        &self,
        data_splits: &[DataSplit],
        core_options: &CoreOptions,
    ) -> crate::Result<ArrowRecordBatchStream> {
        let merge_engine = core_options.merge_engine()?;
        let dv_enabled = core_options.deletion_vectors_enabled();
        if matches!(
            merge_engine,
            MergeEngine::PartialUpdate | MergeEngine::Aggregation
        ) && !dv_enabled
        {
            return self.read_kv(data_splits, core_options);
        }

        if matches!(
            merge_engine,
            MergeEngine::PartialUpdate | MergeEngine::Aggregation
        ) {
            let merge_engine_name = match merge_engine {
                MergeEngine::PartialUpdate => "partial-update",
                MergeEngine::Aggregation => "aggregation",
                _ => unreachable!("guarded by partial-update/aggregation match"),
            };
            if core_options.deletion_vectors_merge_on_read() {
                return Err(crate::Error::Unsupported {
                    message: format!(
                        "merge-engine={merge_engine_name} with deletion-vectors.merge-on-read=true is not supported"
                    ),
                });
            }
            if !data_splits
                .iter()
                .all(DataSplit::is_fully_materialized_pk_dv)
            {
                return Err(crate::Error::Unsupported {
                    message: format!(
                        "merge-engine={merge_engine_name} with deletion vectors can only read fully materialized compacted splits"
                    ),
                });
            }
            return self.read_raw(data_splits);
        }

        // Deletion-vector tables read raw by design: stale versions of a key
        // are masked by DVs, not merged, and KeyValueFileReader does not
        // support DVs. Keep the plain level-0 dispatch for them.
        let mut kv_splits = Vec::new();
        let mut raw_splits = Vec::new();
        for split in data_splits {
            if pk_split_needs_merge(split, dv_enabled) {
                kv_splits.push(split.clone());
            } else {
                raw_splits.push(split.clone());
            }
        }

        if raw_splits.is_empty() {
            return self.read_kv(&kv_splits, core_options);
        }
        if kv_splits.is_empty() {
            return self.read_raw(&raw_splits);
        }

        let kv_stream = self.read_kv(&kv_splits, core_options)?;
        let raw_stream = self.read_raw(&raw_splits)?;
        Ok(Box::pin(futures::stream::select_all([
            kv_stream, raw_stream,
        ])))
    }

    /// Read splits via KeyValueFileReader (sort-merge dedup).
    fn read_kv(
        &self,
        splits: &[DataSplit],
        core_options: &CoreOptions,
    ) -> crate::Result<ArrowRecordBatchStream> {
        let reader = KeyValueFileReader::new(
            self.table.file_io.clone(),
            KeyValueReadConfig {
                table_name: self.table.identifier().full_name(),
                table_options: self.table.schema().options().clone(),
                schema_manager: self.table.schema_manager().clone(),
                table_schema_id: self.table.schema().id(),
                table_fields: self.table.schema.fields().to_vec(),
                read_type: self.read_type().to_vec(),
                predicates: self.data_predicates.clone(),
                primary_keys: self.table.schema.trimmed_primary_keys(),
                merge_engine: core_options.merge_engine()?,
                sequence_fields: core_options
                    .sequence_fields()
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
                read_batch_size: core_options.read_batch_size()?,
                merge_splits: false,
                max_merge_file_streams: None,
            },
        );
        reader.read(splits)
    }

    /// Read with data-evolution support.
    fn read_with_evolution(
        &self,
        data_splits: &[DataSplit],
        core_options: &CoreOptions,
    ) -> crate::Result<ArrowRecordBatchStream> {
        let reader = DataEvolutionReader::new(
            self.table.file_io.clone(),
            self.table.schema_manager().clone(),
            self.table.schema().id(),
            self.table.schema.fields().to_vec(),
            self.read_type().to_vec(),
            self.data_predicates.clone(),
            core_options.blob_as_descriptor(),
            core_options.blob_descriptor_fields(),
            core_options.blob_view_fields(),
            core_options.blob_view_resolve_enabled(),
            self.table.rest_env().cloned(),
        )?
        .with_batch_size(Some(core_options.read_batch_size()?));
        reader.read(data_splits)
    }

    /// Read raw data files without dedup or evolution.
    fn read_raw(&self, data_splits: &[DataSplit]) -> crate::Result<ArrowRecordBatchStream> {
        self.new_data_file_reader()?.read(data_splits)
    }

    fn new_data_file_reader(&self) -> crate::Result<DataFileReader> {
        let mut reader = DataFileReader::new(
            self.table.file_io.clone(),
            self.table.schema_manager().clone(),
            self.table.schema().id(),
            self.table.schema.fields().to_vec(),
            self.read_type().to_vec(),
            self.data_predicates.clone(),
        )
        .with_batch_size(Some(self.table.schema().core_options().read_batch_size()?));
        // The engine decoder filter is safe only on the plain append/raw path.
        // This constructor is also used by raw-convertible primary-key splits,
        // where positional merge semantics must remain untouched.
        if self.table.schema().primary_keys().is_empty() {
            if let Some(factory) = &self.row_filter_factory {
                reader = reader.with_row_filter_factory(Arc::clone(factory));
            }
        }
        Ok(reader)
    }
}

fn audit_schema_for_read_type(
    read_type: &[DataField],
    include_sequence: bool,
) -> crate::Result<Arc<ArrowSchema>> {
    let mut fields = Vec::with_capacity(read_type.len() + 2);
    fields.push(DataField::new(
        ROW_KIND_FIELD_ID,
        ROW_KIND_FIELD_NAME.to_string(),
        DataType::VarChar(crate::spec::VarCharType::string_type()),
    ));
    if include_sequence {
        fields.push(DataField::new(
            SEQUENCE_NUMBER_FIELD_ID,
            SEQUENCE_NUMBER_FIELD_NAME.to_string(),
            DataType::BigInt(BigIntType::new()),
        ));
    }
    fields.extend(read_type.iter().cloned());
    build_target_arrow_schema(&fields)
}

fn audit_sequence_number_enabled(table: &Table) -> bool {
    table
        .schema()
        .options()
        .get("table-read.sequence-number.enabled")
        .is_some_and(|v| v.eq_ignore_ascii_case("true"))
}

fn rowkind_array_from_column(column: &dyn arrow_array::Array) -> crate::Result<StringArray> {
    let values = column
        .as_any()
        .downcast_ref::<arrow_array::Int8Array>()
        .ok_or_else(|| crate::Error::DataInvalid {
            message: "AuditLogTable _VALUE_KIND column must be Int8".to_string(),
            source: None,
        })?;
    let mut strings = Vec::with_capacity(values.len());
    for idx in 0..values.len() {
        if values.is_null(idx) {
            return Err(crate::Error::DataInvalid {
                message: format!("AuditLogTable _VALUE_KIND is null at row {idx}"),
                source: None,
            });
        }
        let rowkind = match values.value(idx) {
            0 => "+I",
            1 => "-U",
            2 => "+U",
            3 => "-D",
            value => {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "AuditLogTable _VALUE_KIND has invalid value {value} at row {idx}"
                    ),
                    source: None,
                });
            }
        };
        strings.push(rowkind);
    }
    Ok(StringArray::from(strings))
}

const DIFF_BATCH_SIZE: usize = 8192;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CursorOrd {
    BeforeOnly,
    AfterOnly,
    EqualSame,
    EqualDiff,
}

struct ArrowCursor {
    stream: ArrowRecordBatchStream,
    batch: Option<RecordBatch>,
    row: usize,
}

impl ArrowCursor {
    async fn new(stream: ArrowRecordBatchStream) -> crate::Result<Self> {
        let mut cursor = Self {
            stream,
            batch: None,
            row: 0,
        };
        cursor.advance().await?;
        Ok(cursor)
    }

    fn alive(&self) -> bool {
        self.batch.is_some()
    }

    fn batch(&self) -> &RecordBatch {
        self.batch.as_ref().expect("cursor must be alive")
    }

    fn row(&self) -> usize {
        self.row
    }

    async fn advance(&mut self) -> crate::Result<()> {
        loop {
            if let Some(ref batch) = self.batch {
                if self.row + 1 < batch.num_rows() {
                    self.row += 1;
                    return Ok(());
                }
            }
            match self.stream.next().await {
                Some(Ok(batch)) if batch.num_rows() > 0 => {
                    self.batch = Some(batch);
                    self.row = 0;
                    return Ok(());
                }
                Some(Ok(_)) => continue,
                Some(Err(e)) => return Err(e),
                None => {
                    self.batch = None;
                    return Ok(());
                }
            }
        }
    }
}

struct AuditBatchBuilder {
    schema: Arc<ArrowSchema>,
    rowkind: StringBuilder,
    row_indices: Vec<(usize, usize)>,
    pinned_batches: Vec<RecordBatch>,
    data_col_indices: Vec<usize>,
    len: usize,
}

impl AuditBatchBuilder {
    fn new(schema: Arc<ArrowSchema>) -> Self {
        Self {
            schema,
            rowkind: StringBuilder::new(),
            row_indices: Vec::new(),
            pinned_batches: Vec::new(),
            data_col_indices: Vec::new(),
            len: 0,
        }
    }

    fn has_data_columns(&self) -> bool {
        !self.data_col_indices.is_empty()
    }

    fn set_data_col_indices(&mut self, indices: Vec<usize>) {
        self.data_col_indices = indices;
    }

    fn len(&self) -> usize {
        self.len
    }

    fn push(&mut self, kind: &str, batch: &RecordBatch, row: usize) {
        self.rowkind.append_value(kind);
        let batch_id = self.pin_batch(batch);
        self.row_indices.push((batch_id, row));
        self.len += 1;
    }

    fn pin_batch(&mut self, batch: &RecordBatch) -> usize {
        if let Some(last) = self.pinned_batches.last() {
            if std::ptr::eq(batch, last) {
                return self.pinned_batches.len() - 1;
            }
        }
        let batch_id = self.pinned_batches.len();
        self.pinned_batches.push(batch.clone());
        batch_id
    }

    fn flush(&mut self) -> crate::Result<RecordBatch> {
        let mut columns: Vec<ArrayRef> = vec![Arc::new(self.rowkind.finish())];
        self.rowkind = StringBuilder::new();
        for &col_idx in &self.data_col_indices {
            let taken: Vec<ArrayRef> = self
                .row_indices
                .iter()
                .map(|(batch_id, row)| {
                    take(
                        self.pinned_batches[*batch_id].column(col_idx).as_ref(),
                        &UInt32Array::from(vec![*row as u32]),
                        None,
                    )
                    .map_err(|e| crate::Error::UnexpectedError {
                        message: format!("Failed to take audit diff column: {e}"),
                        source: Some(Box::new(e)),
                    })
                })
                .collect::<crate::Result<Vec<_>>>()?;
            let refs: Vec<&dyn Array> = taken.iter().map(|array| array.as_ref()).collect();
            columns.push(
                arrow_concat(&refs).map_err(|e| crate::Error::UnexpectedError {
                    message: format!("Failed to concat audit diff column: {e}"),
                    source: Some(Box::new(e)),
                })?,
            );
        }
        self.row_indices.clear();
        self.pinned_batches.clear();
        self.len = 0;
        RecordBatch::try_new(self.schema.clone(), columns).map_err(|e| {
            crate::Error::UnexpectedError {
                message: format!("Failed to build audit diff batch: {e}"),
                source: Some(Box::new(e)),
            }
        })
    }
}

struct DiffAfterImageBatchBuilder {
    schema: Arc<ArrowSchema>,
    row_indices: Vec<(usize, usize)>,
    pinned_batches: Vec<RecordBatch>,
    col_indices: Vec<usize>,
    len: usize,
}

impl DiffAfterImageBatchBuilder {
    fn new(schema: Arc<ArrowSchema>, col_indices: Vec<usize>) -> Self {
        Self {
            schema,
            row_indices: Vec::new(),
            pinned_batches: Vec::new(),
            col_indices,
            len: 0,
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn push(&mut self, batch: &RecordBatch, row: usize) {
        let batch_id = self.pin_batch(batch);
        self.row_indices.push((batch_id, row));
        self.len += 1;
    }

    fn pin_batch(&mut self, batch: &RecordBatch) -> usize {
        if let Some(last) = self.pinned_batches.last() {
            if std::ptr::eq(batch, last) {
                return self.pinned_batches.len() - 1;
            }
        }
        let batch_id = self.pinned_batches.len();
        self.pinned_batches.push(batch.clone());
        batch_id
    }

    fn flush(&mut self) -> crate::Result<RecordBatch> {
        let row_count = self.len;
        let mut columns = Vec::with_capacity(self.col_indices.len());
        for &col_idx in &self.col_indices {
            let taken: Vec<ArrayRef> = self
                .row_indices
                .iter()
                .map(|(batch_id, row)| {
                    take(
                        self.pinned_batches[*batch_id].column(col_idx).as_ref(),
                        &UInt32Array::from(vec![*row as u32]),
                        None,
                    )
                    .map_err(|e| crate::Error::UnexpectedError {
                        message: format!("Failed to take diff after-image column: {e}"),
                        source: Some(Box::new(e)),
                    })
                })
                .collect::<crate::Result<Vec<_>>>()?;
            let refs: Vec<&dyn Array> = taken.iter().map(|array| array.as_ref()).collect();
            columns.push(
                arrow_concat(&refs).map_err(|e| crate::Error::UnexpectedError {
                    message: format!("Failed to concat diff after-image column: {e}"),
                    source: Some(Box::new(e)),
                })?,
            );
        }
        self.row_indices.clear();
        self.pinned_batches.clear();
        self.len = 0;
        let options = RecordBatchOptions::new().with_row_count(Some(row_count));
        RecordBatch::try_new_with_options(self.schema.clone(), columns, &options).map_err(|e| {
            crate::Error::UnexpectedError {
                message: format!("Failed to build diff after-image batch: {e}"),
                source: Some(Box::new(e)),
            }
        })
    }
}

fn diff_pairs(plan: &IncrementalPlan) -> crate::Result<Vec<(Vec<DataSplit>, Vec<DataSplit>)>> {
    plan.validate()?;
    if plan.mode() != IncrementalScanMode::Diff {
        return Err(crate::Error::DataInvalid {
            message: "Diff reader requires a Diff incremental plan".to_string(),
            source: None,
        });
    }
    plan.splits()
        .iter()
        .map(|split| match split {
            IncrementalSplit::DiffPair { before, after } => Ok((before.clone(), after.clone())),
            IncrementalSplit::Data(_) => Err(crate::Error::DataInvalid {
                message: "Diff incremental plan contains a Data split".to_string(),
                source: None,
            }),
        })
        .collect()
}

fn diff_output_col_indices(
    batch: &RecordBatch,
    read_type: &[DataField],
    include_sequence: bool,
) -> crate::Result<Vec<usize>> {
    let mut indices = Vec::with_capacity(read_type.len() + usize::from(include_sequence));
    if include_sequence {
        indices.push(
            batch
                .schema()
                .index_of(SEQUENCE_NUMBER_FIELD_NAME)
                .map_err(|e| crate::Error::DataInvalid {
                    message: format!("Diff read missing _SEQUENCE_NUMBER: {e}"),
                    source: None,
                })?,
        );
    }
    for field in read_type {
        indices.push(batch.schema().index_of(field.name()).map_err(|e| {
            crate::Error::DataInvalid {
                message: format!("Diff read missing column '{}': {e}", field.name()),
                source: None,
            }
        })?);
    }
    Ok(indices)
}

fn value_indices_for_diff(table: &Table, fields: &[DataField]) -> Vec<usize> {
    let primary_key_names = table.schema().trimmed_primary_keys();
    let primary_keys: std::collections::HashSet<&str> =
        primary_key_names.iter().map(|key| key.as_str()).collect();
    fields
        .iter()
        .enumerate()
        .filter(|(_, field)| {
            field.name() != SEQUENCE_NUMBER_FIELD_NAME && !primary_keys.contains(field.name())
        })
        .map(|(index, _)| index)
        .collect()
}

fn primary_key_indices(table: &Table, read_type: &[DataField]) -> crate::Result<Vec<usize>> {
    let mut indices = Vec::new();
    for pk in table.schema().trimmed_primary_keys() {
        let idx = read_type
            .iter()
            .position(|field| field.name() == pk)
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!("Primary key column '{pk}' missing from Diff comparison schema"),
                source: None,
            })?;
        indices.push(idx);
    }
    Ok(indices)
}

fn ensure_diff_supported_read_type(read_type: &[DataField]) -> crate::Result<()> {
    for field in read_type {
        if !is_diff_supported_type(field.data_type()) {
            return Err(crate::Error::Unsupported {
                message: format!(
                    "Batch incremental Diff does not support column '{}' of type {:?}",
                    field.name(),
                    field.data_type()
                ),
            });
        }
    }
    Ok(())
}

fn is_diff_supported_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean(_)
            | DataType::TinyInt(_)
            | DataType::SmallInt(_)
            | DataType::Int(_)
            | DataType::BigInt(_)
            | DataType::Float(_)
            | DataType::Double(_)
            | DataType::Char(_)
            | DataType::VarChar(_)
            | DataType::Date(_)
    )
}

fn cursor_cmp(
    bc: &ArrowCursor,
    ac: &ArrowCursor,
    key_indices: &[usize],
    value_indices: &[usize],
) -> crate::Result<CursorOrd> {
    match (bc.alive(), ac.alive()) {
        (false, false) => unreachable!("cursor_cmp called with both streams exhausted"),
        (false, true) => return Ok(CursorOrd::AfterOnly),
        (true, false) => return Ok(CursorOrd::BeforeOnly),
        (true, true) => {}
    }
    match compare_pk(bc, ac, key_indices)? {
        Ordering::Less => Ok(CursorOrd::BeforeOnly),
        Ordering::Greater => Ok(CursorOrd::AfterOnly),
        Ordering::Equal => {
            if rows_equal_at(bc.batch(), bc.row(), ac.batch(), ac.row(), value_indices)? {
                Ok(CursorOrd::EqualSame)
            } else {
                Ok(CursorOrd::EqualDiff)
            }
        }
    }
}

fn compare_pk(
    bc: &ArrowCursor,
    ac: &ArrowCursor,
    key_indices: &[usize],
) -> crate::Result<Ordering> {
    for &idx in key_indices {
        let ord = scalar_compare(
            bc.batch().column(idx),
            bc.row(),
            ac.batch().column(idx),
            ac.row(),
        )?;
        if ord != Ordering::Equal {
            return Ok(ord);
        }
    }
    Ok(Ordering::Equal)
}

fn rows_equal_at(
    left_batch: &RecordBatch,
    left_row: usize,
    right_batch: &RecordBatch,
    right_row: usize,
    indices: &[usize],
) -> crate::Result<bool> {
    for &idx in indices {
        let ord = scalar_compare(
            left_batch.column(idx),
            left_row,
            right_batch.column(idx),
            right_row,
        )?;
        if ord != Ordering::Equal {
            return Ok(false);
        }
    }
    Ok(true)
}

fn scalar_compare(
    left: &dyn Array,
    left_row: usize,
    right: &dyn Array,
    right_row: usize,
) -> crate::Result<Ordering> {
    use arrow_array::{
        BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    };

    match (left.is_null(left_row), right.is_null(right_row)) {
        (true, true) => return Ok(Ordering::Equal),
        (true, false) => return Ok(Ordering::Less),
        (false, true) => return Ok(Ordering::Greater),
        (false, false) => {}
    }

    macro_rules! compare {
        ($ty:ty, $getter:expr) => {
            if let (Some(a), Some(b)) = (
                left.as_any().downcast_ref::<$ty>(),
                right.as_any().downcast_ref::<$ty>(),
            ) {
                return Ok($getter(a, left_row).cmp(&$getter(b, right_row)));
            }
        };
    }

    compare!(Int8Array, |a: &Int8Array, r| a.value(r));
    compare!(Int16Array, |a: &Int16Array, r| a.value(r));
    compare!(Int32Array, |a: &Int32Array, r| a.value(r));
    compare!(Int64Array, |a: &Int64Array, r| a.value(r));
    compare!(UInt8Array, |a: &UInt8Array, r| a.value(r));
    compare!(UInt16Array, |a: &UInt16Array, r| a.value(r));
    compare!(UInt32Array, |a: &UInt32Array, r| a.value(r));
    compare!(UInt64Array, |a: &UInt64Array, r| a.value(r));
    compare!(BooleanArray, |a: &BooleanArray, r| a.value(r));
    compare!(Date32Array, |a: &Date32Array, r| a.value(r));

    if let (Some(a), Some(b)) = (
        left.as_any().downcast_ref::<StringArray>(),
        right.as_any().downcast_ref::<StringArray>(),
    ) {
        return Ok(a.value(left_row).cmp(b.value(right_row)));
    }

    if let (Some(a), Some(b)) = (
        left.as_any().downcast_ref::<Float32Array>(),
        right.as_any().downcast_ref::<Float32Array>(),
    ) {
        let (left, right) = (a.value(left_row), b.value(right_row));
        return Ok(if left.is_nan() && right.is_nan() {
            Ordering::Equal
        } else {
            left.total_cmp(&right)
        });
    }
    if let (Some(a), Some(b)) = (
        left.as_any().downcast_ref::<Float64Array>(),
        right.as_any().downcast_ref::<Float64Array>(),
    ) {
        let (left, right) = (a.value(left_row), b.value(right_row));
        return Ok(if left.is_nan() && right.is_nan() {
            Ordering::Equal
        } else {
            left.total_cmp(&right)
        });
    }

    Err(crate::Error::Unsupported {
        message: format!(
            "Batch incremental Diff does not support comparing column type {:?}",
            left.data_type()
        ),
    })
}

/// Whether a primary-key split must go through the sort-merge reader.
///
/// Mirrors Java `PrimaryKeyTableRawFileSplitReadProvider#match`: a raw read
/// needs the split marked raw convertible AND a known `delete_row_count` on
/// every file. Legacy files without the stat may hide delete rows — scan
/// planning treats the missing stat as "no deletes" for compatibility, so the
/// read side must fall back to the merge reader, which drops them.
///
/// Deletion-vector tables keep the plain level-0 dispatch: stale versions are
/// masked by DVs and KeyValueFileReader does not support DVs.
fn pk_split_needs_merge(split: &DataSplit, dv_enabled: bool) -> bool {
    if dv_enabled {
        return split.data_files().iter().any(|f| f.level == 0);
    }
    !split.raw_convertible()
        || split
            .data_files()
            .iter()
            .any(|f| f.delete_row_count.is_none())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{BinaryRow, DataFileMeta};
    use crate::table::query_auth_table;
    use crate::table::source::DataSplitBuilder;

    fn file(name: &str, level: i32, delete_row_count: Option<i64>) -> DataFileMeta {
        DataFileMeta {
            file_name: name.to_string(),
            file_size: 128,
            row_count: 10,
            min_key: Vec::new(),
            max_key: Vec::new(),
            key_stats: BinaryTableStats::new(Vec::new(), Vec::new(), Vec::new()),
            value_stats: BinaryTableStats::new(Vec::new(), Vec::new(), Vec::new()),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id: 0,
            level,
            extra_files: Vec::new(),
            creation_time: None,
            delete_row_count,
            embedded_index: None,
            first_row_id: None,
            write_cols: None,
            external_path: None,
            file_source: None,
            value_stats_cols: None,
        }
    }

    fn split(files: Vec<DataFileMeta>, raw_convertible: bool) -> DataSplit {
        DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("file:/tmp/bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(files)
            .with_raw_convertible(raw_convertible)
            .build()
            .unwrap()
    }

    #[test]
    fn test_pk_split_needs_merge_routing() {
        // Raw convertible with known delete counts: raw read.
        let raw = split(vec![file("a", 5, Some(0))], true);
        assert!(!pk_split_needs_merge(&raw, false));

        // Not raw convertible: merge read.
        let merge = split(vec![file("a", 5, Some(0))], false);
        assert!(pk_split_needs_merge(&merge, false));

        // Raw convertible but a legacy file lacks delete_row_count: the file
        // may hide delete rows, so it must go through the merge reader.
        let legacy = split(vec![file("a", 5, None)], true);
        assert!(pk_split_needs_merge(&legacy, false));

        // Deletion-vector tables dispatch on level 0 only.
        let dv_l0 = split(vec![file("a", 0, None)], false);
        assert!(pk_split_needs_merge(&dv_l0, true));
        let dv_compacted = split(vec![file("a", 5, None)], false);
        assert!(!pk_split_needs_merge(&dv_compacted, true));
    }

    #[test]
    fn test_rowkind_rejects_null_value_kind() {
        let values = arrow_array::Int8Array::from(vec![Some(0), None]);
        assert!(matches!(
            rowkind_array_from_column(&values),
            Err(crate::Error::DataInvalid { ref message, .. }) if message.contains("null at row 1")
        ));
    }

    #[test]
    fn test_rowkind_rejects_invalid_value_kind() {
        let values = arrow_array::Int8Array::from(vec![4]);
        assert!(matches!(
            rowkind_array_from_column(&values),
            Err(crate::Error::DataInvalid { ref message, .. })
                if message.contains("invalid value 4 at row 0")
        ));
    }

    #[test]
    fn test_direct_table_read_fails_closed_when_query_auth_enabled() {
        let table = query_auth_table();
        // Bypass `ReadBuilder` by constructing `TableRead` directly; the `to_arrow` guard
        // still fails closed.
        let read = TableRead::new(&table, table.schema.fields().to_vec(), Vec::new());
        assert!(
            matches!(
                read.to_arrow(&[]),
                Err(crate::Error::Unsupported { ref message }) if message.contains("query-auth.enabled")
            ),
            "directly-constructed read of a query-auth.enabled table must fail closed"
        );
    }

    #[test]
    fn test_direct_incremental_read_fails_closed_when_query_auth_enabled() {
        let table = query_auth_table();
        let read = TableRead::new(&table, table.schema.fields().to_vec(), Vec::new());
        let plan = IncrementalPlan::new(IncrementalScanMode::Delta, Vec::new());
        assert!(
            matches!(
                read.to_incremental_arrow(&plan),
                Err(crate::Error::Unsupported { ref message }) if message.contains("query-auth.enabled")
            ),
            "directly-constructed incremental read of a query-auth.enabled table must fail closed"
        );
    }

    #[test]
    fn test_direct_audit_log_read_fails_closed_when_query_auth_enabled() {
        let table = query_auth_table();
        let read = TableRead::new(&table, table.schema.fields().to_vec(), Vec::new());
        let plan = IncrementalPlan::new(IncrementalScanMode::Delta, Vec::new());
        assert!(
            matches!(
                read.to_audit_log_arrow(&plan),
                Err(crate::Error::Unsupported { ref message }) if message.contains("query-auth.enabled")
            ),
            "directly-constructed audit-log read of a query-auth.enabled table must fail closed"
        );
    }

    #[test]
    fn test_diff_rejects_types_without_comparator_support() {
        use crate::spec::{ArrayType, DecimalType, IntType, TimestampType};

        let decimal = DataField::new(
            1,
            "amount".to_string(),
            DataType::Decimal(DecimalType::new(10, 2).unwrap()),
        );
        let nested = DataField::new(
            2,
            "tags".to_string(),
            DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
        );
        let timestamp = DataField::new(
            3,
            "created_at".to_string(),
            DataType::Timestamp(TimestampType::new(6).unwrap()),
        );
        assert!(matches!(
            ensure_diff_supported_read_type(&[decimal]),
            Err(crate::Error::Unsupported { message }) if message.contains("amount")
        ));
        assert!(matches!(
            ensure_diff_supported_read_type(&[nested]),
            Err(crate::Error::Unsupported { message }) if message.contains("tags")
        ));
        assert!(matches!(
            ensure_diff_supported_read_type(&[timestamp]),
            Err(crate::Error::Unsupported { message }) if message.contains("created_at")
        ));
    }

    #[test]
    fn test_diff_scalar_compare_distinguishes_null_and_nan_values() {
        use arrow_array::{Float32Array, Int32Array};

        let null = Int32Array::from(vec![None]);
        let zero = Int32Array::from(vec![Some(0)]);
        assert_eq!(
            scalar_compare(&null, 0, &zero, 0).unwrap(),
            Ordering::Less,
            "NULL -> 0 must be reported as a changed value"
        );

        let nan = Float32Array::from(vec![f32::NAN]);
        let one = Float32Array::from(vec![1.0]);
        assert_ne!(
            scalar_compare(&nan, 0, &one, 0).unwrap(),
            Ordering::Equal,
            "NaN must not hide a change to a finite value"
        );

        let negative_nan = Float32Array::from(vec![f32::from_bits(0xffc0_0001)]);
        assert_eq!(
            scalar_compare(&nan, 0, &negative_nan, 0).unwrap(),
            Ordering::Equal,
            "all NaN representations must compare equal like Java Float.compare"
        );

        let negative_zero = Float32Array::from(vec![-0.0]);
        let positive_zero = Float32Array::from(vec![0.0]);
        assert_ne!(
            scalar_compare(&negative_zero, 0, &positive_zero, 0).unwrap(),
            Ordering::Equal,
            "signed zero must remain distinguishable like Java Float.compare"
        );
    }
}
