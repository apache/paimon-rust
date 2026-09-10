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
use super::data_file_reader::{DataFileReadTiming, DataFileReader};
use super::format_table_read::FormatTableRead;
use super::incremental_scan::{IncrementalPlan, IncrementalScanMode, IncrementalSplit};
use super::kv_file_reader::{KeyValueFileReader, KeyValueReadConfig};
use super::pk_vector_indexed_split_read::{
    prepare_indexed_split, PkVectorIndexedSplit, PkVectorIndexedSplitRead,
};
use super::pk_vector_position_read::{PKEY_VECTOR_POSITION_COLUMN, SEARCH_SCORE_COLUMN};
use super::read_builder::split_scan_predicates;
use super::vector_search_builder::ensure_no_reserved_read_columns;
use super::{ArrowRecordBatchStream, Table};
use crate::arrow::ParquetReadBudget;
use crate::arrow::{build_target_arrow_schema, PARQUET_FIELD_ID_META_KEY};
use crate::spec::{
    BigIntType, CoreOptions, DataField, DataType, FloatType, MergeEngine, Predicate, TinyIntType,
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
use futures::{stream, StreamExt, TryStreamExt};
use std::cmp::Ordering;
use std::sync::Arc;

const MAX_MERGE_INPUT_STREAMS: usize = 256;

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

/// Drop `_PKEY_VECTOR_POSITION` from a materialized batch. The positional read
/// appends it so a caller can map rows back to the positions it asked for; this read
/// emits rows in that same physical order, so nothing downstream needs it.
fn strip_position_column(batch: &RecordBatch) -> crate::Result<RecordBatch> {
    let schema = batch.schema();
    let Some(drop_at) = schema
        .fields()
        .iter()
        .position(|f| f.name() == PKEY_VECTOR_POSITION_COLUMN)
    else {
        // The positional read appends this column unconditionally, so its absence
        // means the producer changed under us. The reordering path this replaced
        // failed loudly here too; passing the batch through would silently hand the
        // caller a schema it did not ask for.
        return Err(crate::Error::DataInvalid {
            message: format!(
                "vector search read expected a {PKEY_VECTOR_POSITION_COLUMN} column to strip"
            ),
            source: None,
        });
    };
    // The positional read appends the score column as a bare Arrow field, while every
    // user column carries its Paimon field id as `PARQUET:field_id` metadata. Re-emit it
    // with that metadata so this route's output schema is exactly
    // `build_target_arrow_schema(indexed_read_type())` -- otherwise the schema a caller
    // is told to expect and the one it receives differ in metadata alone, which is the
    // worst kind of difference to debug. Confined to this route on purpose:
    // `execute_read` shares the producer but not this function, and widening its wire
    // schema is not this change's business.
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != drop_at)
        .map(|(_, f)| {
            if f.name() == SEARCH_SCORE_COLUMN
                && !f.metadata().contains_key(PARQUET_FIELD_ID_META_KEY)
            {
                let mut metadata = f.metadata().clone();
                metadata.insert(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    search_score_field().id().to_string(),
                );
                Arc::new(f.as_ref().clone().with_metadata(metadata))
            } else {
                f.clone()
            }
        })
        .collect();
    let columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != drop_at)
        .map(|(_, c)| Arc::clone(c))
        .collect();
    RecordBatch::try_new_with_options(
        Arc::new(ArrowSchema::new(fields)),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(|e| crate::Error::DataInvalid {
        message: format!("failed to drop the vector-search position column: {e}"),
        source: None,
    })
}

/// The `__paimon_search_score` field an indexed read appends.
///
/// Id and name mirror Java's `VectorSearchProcedure.SEARCH_SCORE_FIELD`
/// (`new DataField(Integer.MAX_VALUE, SEARCH_SCORE, DataTypes.FLOAT())`), so the same
/// column has the same identity on both sides. Non-null, because the column
/// `PkVectorPositionRead` actually appends is non-null -- this describes what is
/// produced, not what Java's nullable default would be.
fn search_score_field() -> DataField {
    DataField::new(
        i32::MAX,
        SEARCH_SCORE_COLUMN.to_string(),
        DataType::Float(FloatType::with_nullable(false)),
    )
}

pub(super) fn configured_parquet_read_budget(
    table: &Table,
) -> crate::Result<Arc<ParquetReadBudget>> {
    let options = table.schema().core_options();
    Ok(Arc::new(ParquetReadBudget::new(
        options.parquet_row_group_parallelism()?,
        options.parquet_row_group_max_inflight_bytes()?,
    )?))
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
    /// Note: on a PRIMARY-KEY table an ordinary [`to_arrow`](Self::to_arrow) silently
    /// ignores the factory -- `new_data_file_reader` attaches one only when the table has
    /// no primary keys. [`to_arrow_indexed`](Self::to_arrow_indexed) rejects it instead of
    /// ignoring it, so the two terminals differ on purpose.
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

    /// Override the Parquet resource budget shared by this read.
    #[doc(hidden)]
    pub fn with_parquet_read_budget(self, budget: Arc<ParquetReadBudget>) -> Self {
        match self.0 {
            TableReadKind::Paimon(read) => {
                Self(TableReadKind::Paimon(read.with_parquet_read_budget(budget)))
            }
            TableReadKind::Format(read) => {
                Self(TableReadKind::Format(read.with_parquet_read_budget(budget)))
            }
        }
    }

    /// Record that the read builder normalized a partition predicate out of the
    /// caller's filter. Only the scan-bypassing vector search read consults it; see
    /// [`Self::to_arrow_indexed`].
    pub(crate) fn with_filter_set(self, was_set: bool) -> Self {
        match self.0 {
            TableReadKind::Paimon(mut read) => {
                read.filter_set = was_set;
                Self(TableReadKind::Paimon(read))
            }
            TableReadKind::Format(read) => Self(TableReadKind::Format(read)),
        }
    }

    /// Record the scan-only restrictions the caller set on the read builder: a limit,
    /// and explicit row ranges. An ordinary read never consults them -- the scan
    /// applies both -- but a read that bypasses the scan must refuse them rather than
    /// return more rows than were asked for. See [`Self::to_arrow_indexed`].
    pub(crate) fn with_scan_only_restrictions(
        self,
        limit_set: bool,
        explicit_row_ranges_set: bool,
    ) -> Self {
        match self.0 {
            TableReadKind::Paimon(mut read) => {
                read.limit_set = limit_set;
                read.explicit_row_ranges_set = explicit_row_ranges_set;
                Self(TableReadKind::Paimon(read))
            }
            TableReadKind::Format(read) => Self(TableReadKind::Format(read)),
        }
    }

    pub(crate) fn with_data_file_read_timing(self, timing: Arc<DataFileReadTiming>) -> Self {
        match self.0 {
            TableReadKind::Paimon(read) => Self(TableReadKind::Paimon(
                read.with_data_file_read_timing(timing),
            )),
            TableReadKind::Format(read) => Self(TableReadKind::Format(read)),
        }
    }

    /// Returns an [`ArrowRecordBatchStream`].
    pub fn to_arrow(&self, data_splits: &[DataSplit]) -> crate::Result<ArrowRecordBatchStream> {
        match &self.0 {
            TableReadKind::Paimon(read) => read.to_arrow(data_splits),
            TableReadKind::Format(read) => read.to_arrow(data_splits),
        }
    }

    /// Read the rows a primary-key vector search selected.
    ///
    /// Step two of the two-step primary-key vector read: the search decided WHICH
    /// rows -- one [`PkVectorIndexedSplit`] per data file, carrying that file's
    /// selected physical positions and their scores -- and this decides WHICH
    /// COLUMNS. Mirrors Java handing a `PrimaryKeyVectorResult`'s `IndexedSplit`s to
    /// an ordinary read.
    ///
    /// Output columns are this read's own read type -- from
    /// [`ReadBuilder::with_projection`](crate::table::ReadBuilder::with_projection)
    /// or `with_read_type` -- plus `__paimon_search_score`, which is exactly what
    /// [`indexed_read_type`](Self::indexed_read_type) reports. The internal
    /// `_PKEY_VECTOR_POSITION` column is stripped before the rows leave; a read type
    /// naming `_ROW_ID` (or either metadata column) is REJECTED, not hidden.
    ///
    /// Rows come back in the order the splits are given, and within a split in
    /// ascending physical position -- not ranked. `VectorRead::read` returns its
    /// splits in ascending `(partition, bucket, file)` order, so its output reads
    /// in that order; a caller passing its own slice gets its own order:
    /// a caller wanting them best-first sorts on the score column. Java behaves the
    /// same way, its procedure sorting after the read. Not reordering here is also
    /// what lets this stream a split at a time instead of collecting every batch.
    ///
    /// A scalar filter, a row filter factory, a `with_limit` and explicit
    /// `with_row_ranges` on this read are all REJECTED. None can be honoured: this
    /// read runs no scan, and the limit and ranges never reach a `TableRead` at all,
    /// so accepting them would return more rows than the caller asked for --
    /// `with_row_ranges(vec![])` even documents "selects no rows". Put the predicate and
    /// the limit on the vector search builder, where they apply before Top-K.
    ///
    /// Java IGNORES a limit here rather than refusing it: `ReadBuilderImpl.newRead`
    /// forwards it, but `PrimaryKeyIndexedSplitRead` does not override `withLimit`
    /// and inherits `SplitRead`'s no-op, so `withLimit(1)` over a split selecting
    /// three rows still returns three. Refusing is a deliberate divergence -- a
    /// silently dropped limit is the failure this read refuses everywhere else.
    ///
    /// PRECONDITION, unchecked: the splits must come from a search over THIS table.
    /// They name data files by path, so reading another table's splits here would open
    /// those files under this table's schema -- which may fail, or may return the other
    /// table's rows. `DataSplit` carries no table identity to check against, the same
    /// as for `to_arrow`.
    pub fn to_arrow_indexed(
        &self,
        splits: &[PkVectorIndexedSplit],
    ) -> crate::Result<ArrowRecordBatchStream> {
        self.ensure_query_auth_allowed()?;
        match &self.0 {
            TableReadKind::Paimon(read) => read.to_arrow_indexed(splits),
            TableReadKind::Format(_) => Err(crate::Error::Unsupported {
                message: "Format tables do not support vector search read".to_string(),
            }),
        }
    }

    /// The schema [`Self::to_arrow_indexed`] produces: this read's read type plus
    /// `__paimon_search_score`.
    ///
    /// [`read_type`](Self::read_type) describes [`to_arrow`](Self::to_arrow) and does
    /// not carry the score column, so it is the wrong answer for an indexed read. A
    /// caller that must know the output schema BEFORE reading has no other way to get
    /// it: a search that matched nothing yields no batch to learn it from.
    ///
    /// Fallible for the same reason `to_arrow_indexed` is -- a read type naming a
    /// reserved column is rejected rather than answered with a schema that carries
    /// that column twice. The score field takes id `i32::MAX` and non-null `FLOAT`,
    /// the identity Java's `VectorSearchProcedure.SEARCH_SCORE_FIELD` uses.
    pub fn indexed_read_type(&self) -> crate::Result<Vec<DataField>> {
        // Refuse on the same tables `to_arrow_indexed` refuses: describing the output of
        // a read that can never run would be a plausible-looking wrong answer.
        if matches!(self.0, TableReadKind::Format(_)) {
            return Err(crate::Error::Unsupported {
                message: "Format tables do not support vector search read".to_string(),
            });
        }
        ensure_no_reserved_read_columns(self.read_type())?;
        let mut fields = self.read_type().to_vec();
        fields.push(search_score_field());
        Ok(fields)
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
    /// Whether the read builder also normalized a PARTITION predicate out of the
    /// caller's filter. An ordinary read never needs it -- partition pruning happens
    /// in the scan -- but a read that bypasses the scan does, or it would silently
    /// ignore a filter the caller set. See
    /// [`TableRead::to_arrow_indexed`](TableRead::to_arrow_indexed).
    filter_set: bool,
    /// Whether the read builder held a `with_limit`, and whether it held EXPLICIT
    /// `with_row_ranges` (ranges derived from a filter are covered by `filter_set`).
    /// Neither reaches an ordinary read -- both feed `TableScan` -- so only a
    /// scan-bypassing read needs them, and only to refuse.
    limit_set: bool,
    explicit_row_ranges_set: bool,
    row_filter_factory: Option<Arc<dyn crate::arrow::RowFilterFactory>>,
    parquet_read_budget: Option<Arc<ParquetReadBudget>>,
    data_file_read_timing: Option<Arc<DataFileReadTiming>>,
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
            filter_set: false,
            limit_set: false,
            explicit_row_ranges_set: false,
            row_filter_factory: None,
            parquet_read_budget: None,
            data_file_read_timing: None,
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
        let (_partition_predicate, data_predicates) = split_scan_predicates(self.table, filter);
        // Keep the FULL data predicate (including `And`/`Or`/`Not`). Native
        // pushdown / stats pruning skip compound nodes they cannot use, and the
        // residual pass applies the full predicate exactly. Pruning here would
        // drop compound predicates before the residual could enforce them.
        self.data_predicates = data_predicates;
        // The partition half is dropped here as it is on the builder -- an ordinary
        // read never applies it, the scan does. Record only that a filter was SET, not
        // which halves survived: normalization drops a partition conjunct and
        // `_ROW_ID` extraction drops an exact row-id one, so a scan-bypassing read
        // that inspected the surviving predicates could see an empty list and
        // silently ignore a filter the caller set.
        self.filter_set = true;
        self
    }

    fn with_row_filter_factory(mut self, factory: Arc<dyn crate::arrow::RowFilterFactory>) -> Self {
        self.row_filter_factory = Some(factory);
        self
    }

    fn with_parquet_read_budget(mut self, budget: Arc<ParquetReadBudget>) -> Self {
        self.parquet_read_budget = Some(budget);
        self
    }

    fn with_data_file_read_timing(mut self, timing: Arc<DataFileReadTiming>) -> Self {
        self.data_file_read_timing = Some(timing);
        self
    }

    fn parquet_read_budget(&self) -> crate::Result<Arc<ParquetReadBudget>> {
        match &self.parquet_read_budget {
            Some(budget) => Ok(Arc::clone(budget)),
            None => configured_parquet_read_budget(self.table),
        }
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
        let parquet_read_budget = self.parquet_read_budget()?;

        Ok(Box::pin(async_stream::try_stream! {
            let mut workers = stream::iter(pairs.into_iter().map(|(before, after)| {
                let table = table.clone();
                let read_type = read_type.clone();
                let data_predicates = data_predicates.clone();
                let parquet_read_budget = Arc::clone(&parquet_read_budget);
                let worker: ArrowRecordBatchStream = Box::pin(async_stream::try_stream! {
                    let pair_read = PaimonTableRead::new(&table, read_type, data_predicates)
                        .with_parquet_read_budget(parquet_read_budget);
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
        let core_options = self.table.schema().core_options();
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
        .with_file_index_read_enabled(core_options.file_index_read_enabled())
        .with_batch_size(Some(core_options.read_batch_size()?))
        .with_parquet_read_budget(Some(self.parquet_read_budget()?));
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
        let parquet_read_budget = self.parquet_read_budget()?;

        Ok(Box::pin(async_stream::try_stream! {
            let mut workers = stream::iter(pairs.into_iter().map(|(before, after)| {
                let table = table.clone();
                let read_type = read_type.clone();
                let data_predicates = data_predicates.clone();
                let parquet_read_budget = Arc::clone(&parquet_read_budget);
                let worker: ArrowRecordBatchStream = Box::pin(async_stream::try_stream! {
                    let pair_read = PaimonTableRead::new(&table, read_type, data_predicates)
                        .with_parquet_read_budget(parquet_read_budget);
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
        let parquet_read_budget = self.parquet_read_budget()?;

        Ok(Box::pin(async_stream::try_stream! {
            let core_options = CoreOptions::new(table.schema().options());
            let pair_read = PaimonTableRead::new(&table, diff_read_type.clone(), data_predicates)
                .with_parquet_read_budget(parquet_read_budget);
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
        let parquet_read_budget = self.parquet_read_budget()?;

        Ok(Box::pin(async_stream::try_stream! {
            let core_options = CoreOptions::new(table.schema().options());
            let pair_read = PaimonTableRead::new(&table, diff_read_type.clone(), data_predicates)
                .with_parquet_read_budget(parquet_read_budget);
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
                max_merge_input_streams: Some(MAX_MERGE_INPUT_STREAMS),
                // Diff primes the before and after streams in sequence. Keeping
                // a row-group permit across yielded batches can otherwise let
                // the first side block the second side indefinitely.
                parquet_read_budget: None,
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

        // Compacted deletion-vector splits read raw: their stale versions are
        // masked directly by DVs. A split containing level-0 data goes through
        // the key merge; KeyValueFileReader applies any attached per-file DVs
        // before merging the uncompacted versions.
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
                max_merge_input_streams: (core_options.deletion_vectors_enabled()
                    && core_options.deletion_vectors_merge_on_read())
                .then_some(MAX_MERGE_INPUT_STREAMS),
                parquet_read_budget: Some(self.parquet_read_budget()?),
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
        .with_batch_size(Some(core_options.read_batch_size()?))
        .with_parquet_read_budget(Some(self.parquet_read_budget()?))
        .with_read_timing(self.data_file_read_timing.clone());
        reader.read(data_splits)
    }

    /// Read raw data files without dedup or evolution.
    fn read_raw(&self, data_splits: &[DataSplit]) -> crate::Result<ArrowRecordBatchStream> {
        self.new_data_file_reader()?.read(data_splits)
    }

    /// See [`TableRead::to_arrow_indexed`].
    ///
    /// Eager errors: a projection naming a reserved column, a read carrying a filter /
    /// limit / row ranges / row filter factory, a bad fan-in, and every split's own
    /// structural validation -- single data file, range bounds and ordering, score
    /// alignment, scores present. Only the work that touches storage stays lazy: schema
    /// resolution, deletion-vector load, opening the files.
    fn to_arrow_indexed(
        &self,
        splits: &[PkVectorIndexedSplit],
    ) -> crate::Result<ArrowRecordBatchStream> {
        // `ReadBuilder::with_projection` permits `_ROW_ID`, but the positional read
        // recovers physical positions through it and rejects it deep inside a lazy
        // stream. Catch it here instead.
        ensure_no_reserved_read_columns(self.read_type())?;
        crate::table::pk_vector_indexed_split_read::validate_fan_in(splits)?;

        // Every input that would make this read return something other than exactly
        // the rows the search selected, because it can honour none of them.
        //
        // `filter_set` covers a filter however little of it survived normalization --
        // the read builder drops a `pt = 'A'` conjunct into a partition predicate and an
        // exact `_ROW_ID` conjunct into row ranges, so inspecting the surviving
        // `data_predicates` alone would let either through unapplied.
        //
        // A limit and explicit row ranges never reach ANY `TableRead`: the read builder
        // keeps both for `TableScan`, and this read runs no scan. Accepting them would
        // return MORE rows than asked for, and `with_row_ranges(vec![])` documents
        // "selects no rows", which returning every hit flatly contradicts. Java IGNORES
        // a limit here (`PrimaryKeyIndexedSplitRead` inherits `SplitRead`'s no-op
        // `withLimit`), so refusing one is a deliberate divergence rather than a
        // shared property.
        //
        // A `row_filter_factory` is the last one, and the only one that could not drop a
        // row even if it were honoured: `new_data_file_reader` attaches a factory only
        // when the table has no primary keys, and `new_vector_search_file_reader` never
        // attaches one. It is refused because it would be IGNORED, not because it is
        // dangerous -- note `to_arrow` on a primary-key table ignores it silently.
        let mut refused: Vec<&str> = Vec::new();
        if !self.data_predicates.is_empty() || self.filter_set {
            refused.push("a filter");
        }
        if self.limit_set {
            refused.push("a limit");
        }
        if self.explicit_row_ranges_set {
            refused.push("row ranges");
        }
        if self.row_filter_factory.is_some() {
            refused.push("a row filter factory");
        }
        if !refused.is_empty() {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "vector search read cannot honour {}: it bypasses scan planning and \
                     returns exactly the rows the search selected. Put the predicate and \
                     the limit on the vector search builder (with_filter / with_limit), \
                     where they apply BEFORE Top-K. Applied here, a filter or a limit \
                     would silently return fewer rows than the search was asked for, a \
                     partition filter would not be applied at all because this read has \
                     no scan, and a row filter factory is never attached by this read",
                    refused.join(", ")
                ),
                source: None,
            });
        }

        // Validate and expand EVERY split before the stream exists. This is pure
        // metadata work, so a malformed split is reported before any row is handed out
        // -- not after the splits before it have already been read.
        let prepared = splits
            .iter()
            .map(prepare_indexed_split)
            .collect::<crate::Result<Vec<_>>>()?;

        // `indexed_read_type` promises the score column, so every split must carry
        // scores: one that does not would emit a narrower schema mid-stream and make
        // that promise false. `VectorRead::read` always attaches them; the only
        // score-less producer is the internal rerank path, which never gets here.
        if let Some(without) = prepared.iter().find(|p| !p.has_scores()) {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "indexed split for {} carries no scores, but a vector search read \
                     emits a {SEARCH_SCORE_COLUMN} column for every split",
                    without.file_name()
                ),
                source: None,
            });
        }

        let split_read = PkVectorIndexedSplitRead::new(self.new_vector_search_file_reader()?);
        let stream = async_stream::try_stream! {
            for one in prepared {
                let mut batches = split_read.read_prepared(one)?;
                while let Some(batch) = batches.try_next().await? {
                    yield strip_position_column(&batch)?;
                }
            }
        };
        Ok(Box::pin(stream))
    }

    /// As [`Self::new_data_file_reader`], but predicate-free and with no engine
    /// row-filter factory, which the positional read requires (see
    /// [`Self::to_arrow_indexed`]).
    ///
    /// The parquet read budget IS newly honoured, which the terminal this replaced was
    /// not: that one built a bare `DataFileReader`, so a vector read ignored the
    /// table's row-group inflight bound. The budget is per-read, so a single-file
    /// positional read cannot starve another.
    ///
    /// Batch size and read timing are left unset: `read_single_file_stream_local`
    /// passes `None` as the format reader's batch size and never consults the timing,
    /// so setting them would be inert.
    fn new_vector_search_file_reader(&self) -> crate::Result<DataFileReader> {
        Ok(DataFileReader::new(
            self.table.file_io.clone(),
            self.table.schema_manager().clone(),
            self.table.schema().id(),
            self.table.schema.fields().to_vec(),
            self.read_type().to_vec(),
            Vec::new(),
        )
        .with_parquet_read_budget(Some(self.parquet_read_budget()?)))
    }

    fn new_data_file_reader(&self) -> crate::Result<DataFileReader> {
        let core_options = self.table.schema().core_options();
        let mut reader = DataFileReader::new(
            self.table.file_io.clone(),
            self.table.schema_manager().clone(),
            self.table.schema().id(),
            self.table.schema.fields().to_vec(),
            self.read_type().to_vec(),
            self.data_predicates.clone(),
        )
        .with_file_index_read_enabled(core_options.file_index_read_enabled())
        .with_batch_size(Some(core_options.read_batch_size()?))
        .with_parquet_read_budget(Some(self.parquet_read_budget()?))
        .with_read_timing(self.data_file_read_timing.clone());
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
/// Deletion-vector tables merge only splits containing level-0 files. Fully
/// compacted splits stay on the raw path, while the merge reader applies any
/// attached DVs before reconciling uncompacted key versions.
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
    use crate::catalog::Identifier;
    use crate::common::Options;
    use crate::file_index::file_indexer_factory::{FileIndexerFactory, BITMAP_INDEX};
    use crate::file_index::write_column_indexes;
    use crate::io::FileIOBuilder;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{
        BinaryRow, DataFileMeta, DataType, Datum, IntType, PredicateBuilder, Schema, TableSchema,
    };
    use crate::table::query_auth_table;
    use crate::table::source::DataSplitBuilder;
    use futures::TryStreamExt;

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
            column_max_sequence_numbers: None,
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

    /// A format table with no other quirks -- the budget helper below deliberately
    /// carries an invalid option, which would fail `new_read` before the read kind
    /// mattered.
    fn plain_format_table() -> Table {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .option("type", "format-table");
        Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("default", "fmt_t"),
            "memory:/fmt_t".to_string(),
            TableSchema::new(0, &schema.build().unwrap()),
            None,
        )
    }

    fn table_with_invalid_parquet_budget(format_table: bool) -> Table {
        let mut schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .option("read.parquet.row-group.parallelism", "0");
        if format_table {
            schema = schema.option("type", "format-table");
        }
        Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("default", "budget_t"),
            "memory:/budget_t".to_string(),
            TableSchema::new(0, &schema.build().unwrap()),
            None,
        )
    }

    async fn embedded_bitmap_index() -> Vec<u8> {
        let mut writer = FileIndexerFactory::create_writer(
            BITMAP_INDEX,
            DataType::Int(IntType::new()),
            &Options::new(),
        )
        .unwrap();
        writer.write(Some(&Datum::Int(1))).unwrap();
        let indexes = std::collections::HashMap::from([(
            "id".to_string(),
            std::collections::HashMap::from([(
                BITMAP_INDEX.to_string(),
                Some(writer.serialized_bytes().unwrap()),
            )]),
        )]);
        write_column_indexes("memory:/table_read_file_index_source", indexes)
            .await
            .unwrap()
            .to_input_file()
            .read()
            .await
            .unwrap()
            .to_vec()
    }

    fn file_index_table(path: &str, enabled: Option<bool>) -> Table {
        let mut builder = Schema::builder().column("id", DataType::Int(IntType::new()));
        if let Some(enabled) = enabled {
            builder = builder.option("file-index.read.enabled", enabled.to_string());
        }
        Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("default", "file_index_t"),
            path.to_string(),
            TableSchema::new(0, &builder.build().unwrap()),
            None,
        )
    }

    #[tokio::test]
    async fn test_raw_table_read_paths_honor_file_index_read_option() {
        let mut indexed_file = file("missing.mosaic", 5, Some(0));
        indexed_file.row_count = 1;
        indexed_file.embedded_index = Some(embedded_bitmap_index().await);
        let split = split(vec![indexed_file], true);
        let table = file_index_table("memory:/table_read_file_index", None);
        let fields = table.schema().fields().to_vec();
        let predicate = PredicateBuilder::new(&fields)
            .equal("id", Datum::Int(99))
            .unwrap();
        let read = TableRead::new(&table, fields, vec![predicate]);

        let normal = read
            .to_arrow(std::slice::from_ref(&split))
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(normal.is_empty());

        let plan = IncrementalPlan::new(
            IncrementalScanMode::Delta,
            vec![IncrementalSplit::Data(split.clone())],
        );
        let incremental = read
            .to_incremental_arrow(&plan)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(incremental.is_empty());
        let audit = read
            .to_audit_log_arrow(&plan)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(audit.is_empty());

        let disabled_table =
            file_index_table("memory:/table_read_file_index_disabled", Some(false));
        let disabled_fields = disabled_table.schema().fields().to_vec();
        let disabled_predicate = PredicateBuilder::new(&disabled_fields)
            .equal("id", Datum::Int(99))
            .unwrap();
        let disabled_read =
            TableRead::new(&disabled_table, disabled_fields, vec![disabled_predicate]);
        let disabled = disabled_read
            .to_arrow(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await;
        assert!(disabled.is_err());
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
    fn test_direct_table_read_validates_and_can_override_parquet_budget() {
        for format_table in [false, true] {
            let table = table_with_invalid_parquet_budget(format_table);
            let read = TableRead::new(&table, table.schema.fields().to_vec(), Vec::new());
            assert!(matches!(
                read.to_arrow(&[]),
                Err(crate::Error::DataInvalid { ref message, .. })
                    if message.contains("row-group.parallelism")
            ));

            let read = TableRead::new(&table, table.schema.fields().to_vec(), Vec::new())
                .with_parquet_read_budget(Arc::new(ParquetReadBudget::default()));
            assert!(read.to_arrow(&[]).is_ok());
        }
    }

    /// A row filter factory cannot be honoured here at all: this read never attaches
    /// one, and on a primary-key table `to_arrow` does not either. It is refused because
    /// it would otherwise be silently IGNORED -- not because it could drop rows.
    /// DataFusion attaches one whenever it has runtime filters, so this is a live path.
    #[test]
    fn indexed_read_rejects_a_row_filter_factory() {
        use crate::table::pk_vector_indexed_split_read::PkVectorIndexedSplit;
        use crate::table::RowRange;

        #[derive(Debug)]
        struct NoopFactory;
        impl crate::arrow::RowFilterFactory for NoopFactory {
            fn create(
                &self,
                _context: crate::arrow::RowFilterContext<'_>,
            ) -> crate::Result<Vec<Box<dyn crate::arrow::RowFilter>>> {
                Ok(Vec::new())
            }
        }

        let table = table_with_invalid_parquet_budget(false);
        let split = PkVectorIndexedSplit {
            split: split(vec![file("a", 5, Some(0))], false),
            row_ranges: vec![RowRange::new(0, 0)],
            scores: Some(vec![1.0]),
        };
        let base = || {
            TableRead::new(&table, table.schema.fields().to_vec(), Vec::new())
                .with_parquet_read_budget(Arc::new(ParquetReadBudget::default()))
        };

        assert!(base()
            .to_arrow_indexed(std::slice::from_ref(&split))
            .is_ok());
        assert!(matches!(
            base()
                .with_row_filter_factory(Arc::new(NoopFactory))
                .to_arrow_indexed(&[split]),
            Err(crate::Error::DataInvalid { ref message, .. })
                if message.contains("row filter factory")
        ));
    }

    /// The fan-in guard is wired into `to_arrow_indexed`, not merely defined: it
    /// rejects before the read touches storage, so a memory table with no files on disk
    /// is enough to reach it.
    #[test]
    fn indexed_read_rejects_bad_fan_in() {
        use crate::table::pk_vector_indexed_split_read::PkVectorIndexedSplit;
        use crate::table::RowRange;

        let table = table_with_invalid_parquet_budget(false);
        let read = TableRead::new(&table, table.schema.fields().to_vec(), Vec::new())
            .with_parquet_read_budget(Arc::new(ParquetReadBudget::default()));
        let at = |snapshot: i64, name: &str| PkVectorIndexedSplit {
            split: DataSplitBuilder::new()
                .with_snapshot(snapshot)
                .with_partition(BinaryRow::new(0))
                .with_bucket(0)
                .with_bucket_path("memory:/budget_t/bucket-0".to_string())
                .with_total_buckets(1)
                .with_data_files(vec![file(name, 5, Some(0))])
                .build()
                .unwrap(),
            row_ranges: vec![RowRange::new(0, 0)],
            scores: Some(vec![1.0]),
        };

        assert!(read.to_arrow_indexed(&[at(1, "a"), at(1, "b")]).is_ok());
        assert!(matches!(
            read.to_arrow_indexed(&[at(1, "a"), at(2, "b")]),
            Err(crate::Error::DataInvalid { ref message, .. })
                if message.contains("one snapshot")
        ));
        assert!(matches!(
            read.to_arrow_indexed(&[at(1, "a"), at(1, "a")]),
            Err(crate::Error::DataInvalid { ref message, .. })
                if message.contains("twice")
        ));
    }

    /// A malformed split must be reported BEFORE any row is emitted, not after the
    /// splits ahead of it have already been handed to the caller. The second split's
    /// range is past its file, which only `validate_and_expand` catches -- so this
    /// fails only if that check runs eagerly for every split.
    #[test]
    fn indexed_read_validates_every_split_before_streaming() {
        use crate::table::pk_vector_indexed_split_read::PkVectorIndexedSplit;
        use crate::table::RowRange;

        let table = table_with_invalid_parquet_budget(false);
        let read = TableRead::new(&table, table.schema.fields().to_vec(), Vec::new())
            .with_parquet_read_budget(Arc::new(ParquetReadBudget::default()));
        let one = |name: &str, range: RowRange| PkVectorIndexedSplit {
            split: DataSplitBuilder::new()
                .with_snapshot(1)
                .with_partition(BinaryRow::new(0))
                .with_bucket(0)
                .with_bucket_path("memory:/budget_t/bucket-0".to_string())
                .with_total_buckets(1)
                .with_data_files(vec![file(name, 5, Some(0))])
                .build()
                .unwrap(),
            row_ranges: vec![range],
            scores: Some(vec![1.0]),
        };

        // `file(..)` builds a 10-row file, so [50, 50] is outside it.
        let error = read
            .to_arrow_indexed(&[
                one("a", RowRange::new(0, 0)),
                one("b", RowRange::new(50, 50)),
            ])
            .map(|_| ())
            .expect_err("a range past the file must be rejected");
        assert!(
            format!("{error:?}").contains("outside [0, 10)"),
            "got: {error:?}"
        );
    }

    /// `indexed_read_type` promises a score column for the whole stream, so a split
    /// without scores -- which would emit a narrower schema partway through -- has to be
    /// refused rather than silently change the output shape.
    #[test]
    fn indexed_read_requires_every_split_to_carry_scores() {
        use crate::table::pk_vector_indexed_split_read::PkVectorIndexedSplit;
        use crate::table::RowRange;

        let table = table_with_invalid_parquet_budget(false);
        let read = TableRead::new(&table, table.schema.fields().to_vec(), Vec::new())
            .with_parquet_read_budget(Arc::new(ParquetReadBudget::default()));
        let split = PkVectorIndexedSplit {
            split: split(vec![file("a", 5, Some(0))], false),
            row_ranges: vec![RowRange::new(0, 0)],
            scores: None,
        };
        assert!(matches!(
            read.to_arrow_indexed(&[split]),
            Err(crate::Error::DataInvalid { ref message, .. })
                if message.contains("carries no scores")
        ));
    }

    /// The indexed read appends a column `read_type()` does not mention, and a search
    /// that matched nothing yields no batch to learn the schema from -- so the schema
    /// has to be askable up front.
    #[test]
    fn indexed_read_type_is_the_read_type_plus_the_score_column() {
        let table = table_with_invalid_parquet_budget(false);
        let read = TableRead::new(&table, table.schema.fields().to_vec(), Vec::new());

        let indexed = read.indexed_read_type().unwrap();
        let (score, user) = indexed.split_last().unwrap();
        assert_eq!(
            user,
            read.read_type(),
            "the user columns come first, unchanged"
        );
        assert_eq!(score.name(), SEARCH_SCORE_COLUMN);
        assert_eq!(
            score.id(),
            i32::MAX,
            "same field id as Java's VectorSearchProcedure.SEARCH_SCORE_FIELD"
        );
        assert!(
            !score.data_type().is_nullable(),
            "the column the positional read appends is non-null"
        );

        // A read type that already names the score column would otherwise be answered
        // with a schema carrying it twice, which is why this is fallible.
        let colliding = vec![DataField::new(
            0,
            SEARCH_SCORE_COLUMN.to_string(),
            DataType::Int(crate::spec::IntType::new()),
        )];
        assert!(TableRead::new(&table, colliding, Vec::new())
            .indexed_read_type()
            .is_err());
    }

    /// A format table can never run an indexed read, so it must not be handed a schema
    /// describing one -- `to_arrow_indexed` refuses it, and these two have to agree.
    #[test]
    fn indexed_read_type_refuses_a_format_table() {
        let table = plain_format_table();
        let read = table.new_read_builder().new_read().unwrap();
        assert!(matches!(
            read.to_arrow_indexed(&[]),
            Err(crate::Error::Unsupported { .. })
        ));
        assert!(matches!(
            read.indexed_read_type(),
            Err(crate::Error::Unsupported { .. })
        ));
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
