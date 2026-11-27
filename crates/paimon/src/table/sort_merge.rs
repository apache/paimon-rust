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

//! Sort-merge reader with LoserTree for primary-key table reads.
//!
//! Merges multiple sorted `ArrowRecordBatchStream`s by primary key using a
//! tournament tree (LoserTree), applying a [`MergeFunction`] to deduplicate
//! rows sharing the same key.
//!
//! Reference:
//! - Java Paimon: `SortMergeReaderWithMinHeap`
//! - DataFusion: `SortPreservingMergeStream` (LoserTree layout)
//! - Arrow-row: `RowConverter` for efficient key comparison

use crate::spec::{AggregationConfig, CoreOptions, DataField, PartialUpdateConfig, RowKind};
use crate::table::aggregator::{new_aggregator, FieldAggregator};
use crate::table::ArrowRecordBatchStream;
use crate::Error;
use arrow_array::{
    make_array, new_null_array, Array, ArrayRef, Int32Array, Int64Array, Int8Array, RecordBatch,
    RecordBatchOptions,
};
use arrow_data::transform::MutableArrayData;
use arrow_ord::ord::make_comparator;
use arrow_row::{OwnedRow, RowConverter, Rows, SortField};
use arrow_schema::{ArrowError, DataType, Schema, SchemaRef, SortOptions};
use arrow_select::interleave::interleave;
use async_stream::try_stream;
use futures::StreamExt;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------
// MergeFunction
// ---------------------------------------------------------------------------

/// Buffered batches used by the merge reader.
///
/// Source batches keep the internal read schema, while materialized batches
/// already match the merge output schema.
#[derive(Clone)]
pub(crate) enum BufferedBatch {
    Source(RecordBatch),
    Materialized(RecordBatch),
}

impl BufferedBatch {
    pub(super) fn column_for_output<'a>(
        &'a self,
        output_col_idx: usize,
        source_output_col_indices: &[usize],
    ) -> &'a dyn arrow_array::Array {
        match self {
            Self::Source(batch) => batch
                .column(source_output_col_indices[output_col_idx])
                .as_ref(),
            Self::Materialized(batch) => batch.column(output_col_idx).as_ref(),
        }
    }

    /// [`Self::column_for_output`] as the `ArrayRef` the batch holds.
    pub(super) fn column_ref_for_output(
        &self,
        output_col_idx: usize,
        source_output_col_indices: &[usize],
    ) -> &arrow_array::ArrayRef {
        match self {
            Self::Source(batch) => batch.column(source_output_col_indices[output_col_idx]),
            Self::Materialized(batch) => batch.column(output_col_idx),
        }
    }
}

/// A row reference as an index into the batch buffer.
#[derive(Clone)]
pub(crate) struct MergeRow {
    /// Index into the shared batch buffer.
    pub batch_idx: usize,
    pub row_idx: usize,
    pub sequence_number: i64,
    pub value_kind: i8,
    /// Row-encoded user-defined sequence fields, with Java's sort direction
    /// and nulls-first ordering. `None` when `sequence.field` is not set.
    pub user_sequence: Option<OwnedRow>,
}

impl MergeRow {
    fn source_batch<'a>(
        &self,
        batch_buffer: &'a [BufferedBatch],
    ) -> crate::Result<&'a RecordBatch> {
        match batch_buffer.get(self.batch_idx) {
            Some(BufferedBatch::Source(batch)) => Ok(batch),
            Some(BufferedBatch::Materialized(_)) => Err(Error::UnexpectedError {
                message: format!(
                    "Merge row unexpectedly referenced a materialized batch at index {}",
                    self.batch_idx
                ),
                source: None,
            }),
            None => Err(Error::UnexpectedError {
                message: format!(
                    "Merge row referenced batch index {} outside the current buffer",
                    self.batch_idx
                ),
                source: None,
            }),
        }
    }

    pub(super) fn level(&self, batches: &[BufferedBatch]) -> crate::Result<i32> {
        let batch = self.source_batch(batches)?;
        let levels = batch
            .column_by_name(LEVEL_FIELD_NAME)
            .and_then(|column| column.as_any().downcast_ref::<Int32Array>())
            .filter(|levels| self.row_idx < levels.len() && levels.is_valid(self.row_idx))
            .ok_or_else(|| Error::DataInvalid {
                message: "Merge input must contain a non-null INT _LEVEL for each row".into(),
                source: None,
            })?;
        Ok(levels.value(self.row_idx))
    }
}

/// Merge result for rows sharing the same primary key.
pub(crate) enum MergeResult {
    /// Reuse an existing source row from the batch buffer.
    SourceRow { batch_idx: usize, row_idx: usize },
    /// Emit a synthesized one-row batch matching the merge output schema.
    MaterializedRow(RecordBatch),
    /// Persist a synthesized DELETE or UPDATE_BEFORE during a write; scans
    /// omit it.
    MaterializedRetractRow(RecordBatch, RowKind),
    /// Omit this key from the output.
    Omit,
}

/// A live merged row whose user columns are copied from the input rows, so the
/// reader can gather it with the rest of its output instead of building a one-row
/// batch per key.
pub(crate) struct PickedRow {
    /// Per output column, the input row to copy, or `None` for NULL and for the
    /// `_SEQUENCE_NUMBER` / `_VALUE_KIND` columns.
    columns: Vec<Option<(usize, usize)>>,
    sequence_number: i64,
    /// One-row results of field aggregators, indexed by output column. Empty when
    /// no column is aggregated.
    values: Vec<Option<ArrayRef>>,
}

/// What [`MergeFunction::merge_picked`] returns.
pub(crate) enum PickedMerge {
    Picked(PickedRow),
    /// Any other result, as [`MergeFunction::merge`] returns it.
    Result(MergeResult),
}

/// Java's reducer wrapper returns its first KeyValue unchanged when no
/// second version arrives. The user-row reader drops a lone retract.
fn singleton_reducer_result(rows: &[MergeRow]) -> crate::Result<Option<MergeResult>> {
    let [row] = rows else {
        return Ok(None);
    };
    Ok(Some(if RowKind::from_value(row.value_kind)?.is_add() {
        MergeResult::SourceRow {
            batch_idx: row.batch_idx,
            row_idx: row.row_idx,
        }
    } else {
        MergeResult::Omit
    }))
}

/// Whether a merge function also reduces a key with a single version. Only the
/// lookup changelog producer needs that; otherwise the lone version is returned
/// as is (see [`singleton_reducer_result`]).
pub(crate) fn reduces_singletons(table_options: &HashMap<String, String>) -> bool {
    table_options
        .get("changelog-producer")
        .is_some_and(|value| value.eq_ignore_ascii_case("lookup"))
}

/// Merge function applied to rows sharing the same primary key.
///
/// Deduplicate-style engines can keep returning a source row. Future
/// field-wise engines may instead materialize a new output row.
pub(crate) trait MergeFunction: Send + Sync {
    /// Merge all rows sharing the same key into a final output result.
    fn merge(
        &self,
        rows: &[MergeRow],
        batch_buffer: &[BufferedBatch],
        source_output_col_indices: &[usize],
        output_schema: &SchemaRef,
    ) -> crate::Result<MergeResult>;

    /// Emit at most one key's changelog alongside its merged state. The reader
    /// hands changelog on in batches of up to its batch size, keeping memory
    /// bounded even when all data rows are deleted.
    fn merge_with_changelog(
        &self,
        rows: &[MergeRow],
        batches: &[BufferedBatch],
        columns: &[usize],
        schema: &SchemaRef,
    ) -> crate::Result<(MergeResult, Option<RecordBatch>)> {
        Ok((self.merge(rows, batches, columns, schema)?, None))
    }

    /// Merge without materializing a live row, for merge functions that only copy
    /// input values and emit no changelog. `None` means the caller should use
    /// [`Self::merge_with_changelog`].
    fn merge_picked(
        &self,
        _rows: &[MergeRow],
        _batches: &[BufferedBatch],
        _columns: &[usize],
        _schema: &SchemaRef,
    ) -> crate::Result<Option<PickedMerge>> {
        Ok(None)
    }
}

/// Point lookup into retained sorted runs during incremental compaction.
#[async_trait::async_trait]
pub(crate) trait LookupProvider: Send + Sync {
    async fn lookup(&self, key: &[u8]) -> crate::Result<Option<RecordBatch>>;
}

const LEVEL_FIELD_NAME: &str = "_LEVEL";

/// Java KeyValue.schemaWithLevel appends the non-null INT SpecialFields.LEVEL.
/// This column carries each input row's file level through merging; it is not
/// part of the data-file schema or the merge output.
pub(super) fn schema_with_level(schema: &Schema) -> crate::Result<SchemaRef> {
    let level = crate::arrow::build_target_arrow_schema(&[DataField::new(
        i32::MAX - 3,
        LEVEL_FIELD_NAME.to_string(),
        crate::spec::DataType::Int(crate::spec::IntType::with_nullable(false)),
    )])?;
    let mut fields = schema.fields().to_vec();
    fields.push(level.field(0).clone().into());
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

pub(super) fn with_level(batch: RecordBatch, level: i32) -> crate::Result<RecordBatch> {
    let schema = schema_with_level(batch.schema().as_ref())?;
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(Int32Array::from(vec![level; batch.num_rows()])));
    RecordBatch::try_new(schema, columns).map_err(|error| Error::DataInvalid {
        message: format!("Invalid merge input with level: {error}"),
        source: Some(Box::new(error)),
    })
}

/// Route changelog batches to a separate writer.
pub(super) const CHANGELOG_BATCH_METADATA_KEY: &str = "paimon.compact.changelog";

/// Lookup states at positive levels are already materialized. The lowest
/// positive level is the latest state; older levels must not be reduced again.
/// Mirrors Java LookupMergeFunction#pickHighLevel/getResult.
pub(super) fn lookup_rows(
    rows: &[MergeRow],
    batches: &[BufferedBatch],
) -> crate::Result<(Option<MergeRow>, bool, Vec<MergeRow>)> {
    let mut base = None;
    let mut base_level = i32::MAX;
    let mut levels = Vec::with_capacity(rows.len());
    for (index, row) in rows.iter().enumerate() {
        let level = row.level(batches)?;
        if level > 0 && (base.is_none() || level < base_level) {
            base = Some(index);
            base_level = level;
        }
        levels.push(level);
    }
    let has_l0 = levels.contains(&0);
    let selected = rows
        .iter()
        .enumerate()
        .filter(|(index, _)| levels[*index] <= 0 || base == Some(*index))
        .map(|(_, row)| row.clone())
        .collect();
    Ok((base.map(|index| rows[index].clone()), has_l0, selected))
}

pub(super) struct LookupMergeFunction(pub Box<dyn MergeFunction>);

impl MergeFunction for LookupMergeFunction {
    fn merge(
        &self,
        rows: &[MergeRow],
        batches: &[BufferedBatch],
        columns: &[usize],
        schema: &SchemaRef,
    ) -> crate::Result<MergeResult> {
        let (_, _, selected) = lookup_rows(rows, batches)?;
        self.0.merge(&selected, batches, columns, schema)
    }
}

/// Java queries wrap LookupMergeFunction in ReducerMergeFunctionWrapper, then
/// drop retract results. The shortcut counts the original inputs, before
/// lookup discards superseded high-level states. Compaction has no such wrapper.
pub(super) struct QueryReducerMergeFunction(pub Box<dyn MergeFunction>);

impl MergeFunction for QueryReducerMergeFunction {
    fn merge(
        &self,
        rows: &[MergeRow],
        batches: &[BufferedBatch],
        columns: &[usize],
        schema: &SchemaRef,
    ) -> crate::Result<MergeResult> {
        if let Some(result) = singleton_reducer_result(rows)? {
            return Ok(result);
        }
        self.0.merge(rows, batches, columns, schema)
    }
}

/// Deduplicate merge: keeps the row with the highest sequence.
/// When `sequence.field` is configured, compares its typed Arrow row encoding
/// first, then falls back to system `_SEQUENCE_NUMBER` as tie-breaker.
/// When sequence numbers are equal, keeps the last-added row (last-writer-wins).
/// Filters out DELETE and UPDATE_BEFORE rows.
pub(crate) struct DeduplicateMergeFunction;

pub(super) fn compare_sequence_order(lhs: &MergeRow, rhs: &MergeRow) -> Ordering {
    lhs.user_sequence
        .cmp(&rhs.user_sequence)
        .then_with(|| lhs.sequence_number.cmp(&rhs.sequence_number))
        // Java SortMergeReader compares isAdd() after equal sequence numbers:
        // retracts come first, adds last.
        .then_with(|| matches!(lhs.value_kind, 0 | 2).cmp(&matches!(rhs.value_kind, 0 | 2)))
}

impl MergeFunction for DeduplicateMergeFunction {
    fn merge(
        &self,
        rows: &[MergeRow],
        _batch_buffer: &[BufferedBatch],
        _source_output_col_indices: &[usize],
        _output_schema: &SchemaRef,
    ) -> crate::Result<MergeResult> {
        let winner = rows
            .iter()
            .reduce(|best, r| {
                let ord = compare_sequence_order(r, best);
                // >= semantics: last-writer-wins for equal values.
                if ord.is_ge() {
                    r
                } else {
                    best
                }
            })
            .expect("merge called with empty rows");
        if RowKind::from_value(winner.value_kind)?.is_add() {
            Ok(MergeResult::SourceRow {
                batch_idx: winner.batch_idx,
                row_idx: winner.row_idx,
            })
        } else {
            Ok(MergeResult::Omit)
        }
    }
}

/// First-row merge: keep the earliest sequence, retaining the first input on ties.
/// Java rejects retracts even when an earlier add has already been selected.
pub(crate) struct FirstRowMergeFunction {
    pub ignore_delete: bool,
}

impl MergeFunction for FirstRowMergeFunction {
    fn merge(
        &self,
        rows: &[MergeRow],
        _batch_buffer: &[BufferedBatch],
        _source_output_col_indices: &[usize],
        _output_schema: &SchemaRef,
    ) -> crate::Result<MergeResult> {
        let mut first: Option<&MergeRow> = None;
        for row in rows {
            if !RowKind::from_value(row.value_kind)?.is_add() {
                if self.ignore_delete {
                    continue;
                }
                return Err(Error::Unsupported {
                    message: "merge-engine=first-row does not support DELETE or UPDATE_BEFORE rows; set ignore-delete=true to ignore them".to_string(),
                });
            }
            if first.is_none_or(|best| row.sequence_number < best.sequence_number) {
                first = Some(row);
            }
        }
        Ok(match first {
            Some(row) => MergeResult::SourceRow {
                batch_idx: row.batch_idx,
                row_idx: row.row_idx,
            },
            None => MergeResult::Omit,
        })
    }
}

/// Partial-update merge: for each non-key column, keep the latest non-null
/// value or apply its configured field aggregator.
///
/// Sequence-group aggregators use forward accumulation when the incoming
/// group sequence is newer or equal and reversed accumulation when it is
/// older, matching Java `PartialUpdateMergeFunction`.
///
/// DELETE / UPDATE_BEFORE rows are ignored when `ignore-delete=true` and
/// treated as unsupported otherwise.
#[derive(Debug)]
pub(crate) struct PartialUpdateMergeFunction {
    ignore_delete: bool,
    reduce_singletons: bool,
    remove_record_on_delete: bool,
    sequence_group_partial_delete: HashSet<usize>,
    sequence_group_enabled: bool,
    sequence_groups: Vec<RuntimeSequenceGroup>,
    grouped_fields: HashSet<usize>,
    aggregators: Option<Mutex<FieldAggregatorSlots>>,
}

type FieldAggregatorSlots = Vec<Option<Box<dyn FieldAggregator>>>;

fn is_merged_metadata_field(field: &arrow_schema::Field) -> bool {
    matches!(
        field.name().as_str(),
        crate::spec::SEQUENCE_NUMBER_FIELD_NAME | crate::spec::VALUE_KIND_FIELD_NAME
    )
}

/// KV metadata belongs to the merged record, outside user-field aggregation.
/// Java merge functions carry the latest effective sequence separately and
/// normalize surviving records to INSERT, even when the last input retracts.
fn merged_metadata_column(
    field: &arrow_schema::Field,
    row: &MergeRow,
    kind: RowKind,
) -> Option<ArrayRef> {
    match field.name().as_str() {
        crate::spec::SEQUENCE_NUMBER_FIELD_NAME => {
            Some(std::sync::Arc::new(Int64Array::from(vec![
                row.sequence_number,
            ])))
        }
        crate::spec::VALUE_KIND_FIELD_NAME => {
            Some(std::sync::Arc::new(Int8Array::from(vec![kind as i8])))
        }
        _ => None,
    }
}

#[derive(Debug, Clone)]
struct RuntimeSequenceGroup {
    sequence_indices: Vec<usize>,
    protected_indices: Vec<usize>,
    ascending: bool,
}

impl PartialUpdateMergeFunction {
    /// Java replaces the current row with the DELETE value. Later inserts may
    /// leave some columns null, so their merge must start from that value.
    fn replace_with_source(
        selected_by_col: &mut [Option<(usize, usize)>],
        aggregators: Option<&mut [Option<Box<dyn FieldAggregator>>]>,
        batch_idx: usize,
        row_idx: usize,
        batch_buffer: &[BufferedBatch],
        source_output_col_indices: &[usize],
    ) -> crate::Result<()> {
        selected_by_col.fill(Some((batch_idx, row_idx)));
        if let Some(aggregators) = aggregators {
            for (index, aggregator) in aggregators.iter_mut().enumerate() {
                if let Some(aggregator) = aggregator {
                    aggregator.replace_with_delete(
                        batch_buffer[batch_idx].column_for_output(index, source_output_col_indices),
                        row_idx,
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Java `updateNonNullFields`: ungrouped fields take the row's non-null
    /// values, or feed their aggregator.
    fn update_non_null_fields(
        &self,
        selected_by_col: &mut [Option<(usize, usize)>],
        mut aggregators: Option<&mut [Option<Box<dyn FieldAggregator>>]>,
        row: &MergeRow,
        batch_buffer: &[BufferedBatch],
        source_output_col_indices: &[usize],
    ) -> crate::Result<()> {
        for (output_col_idx, selected) in selected_by_col.iter_mut().enumerate() {
            if self.grouped_fields.contains(&output_col_idx) {
                continue;
            }
            let source_array = batch_buffer[row.batch_idx]
                .column_for_output(output_col_idx, source_output_col_indices);
            if let Some(aggregator) = aggregators
                .as_mut()
                .and_then(|aggregators| aggregators.get_mut(output_col_idx))
                .and_then(Option::as_mut)
            {
                aggregator.agg_array(
                    batch_buffer[row.batch_idx]
                        .column_ref_for_output(output_col_idx, source_output_col_indices),
                    row.row_idx,
                )?;
            } else if !source_array.is_null(row.row_idx) {
                *selected = Some((row.batch_idx, row.row_idx));
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn new(
        table_options: &HashMap<String, String>,
        table_name: &str,
    ) -> crate::Result<Self> {
        PartialUpdateConfig::new(table_options).validate_write_mode(true, table_name)?;
        Ok(Self {
            ignore_delete: CoreOptions::new(table_options).ignore_delete(),
            reduce_singletons: reduces_singletons(table_options),
            remove_record_on_delete: PartialUpdateConfig::new(table_options)
                .remove_record_on_delete(),
            sequence_group_partial_delete: HashSet::new(),
            sequence_group_enabled: false,
            sequence_groups: Vec::new(),
            grouped_fields: HashSet::new(),
            aggregators: None,
        })
    }

    pub(crate) fn new_with_schema(
        table_options: &HashMap<String, String>,
        table_name: &str,
        table_fields: &[DataField],
        output_fields: &[DataField],
        primary_keys: &[String],
    ) -> crate::Result<Self> {
        let config = PartialUpdateConfig::new(table_options);
        config.validate_read_mode(true, table_name)?;
        let groups = config.validated_sequence_groups(table_fields, primary_keys)?;
        let sequence_group_enabled = !groups.is_empty();
        let declared_group_sequences: HashSet<&str> = groups
            .iter()
            .flat_map(|group| group.sequence_fields.iter().map(String::as_str))
            .collect();
        let sequence_group_partial_delete = config
            .remove_record_on_sequence_group()
            .map(|fields| {
                fields.split(',').map(str::trim).map(|field| {
                    if !declared_group_sequences.contains(field) {
                        return Err(Error::ConfigInvalid {
                            message: format!("Field '{field}' in partial-update.remove-record-on-sequence-group must belong to a sequence group"),
                        });
                    }
                    output_fields.iter().position(|candidate| candidate.name() == field)
                        .ok_or_else(|| Error::ConfigInvalid {
                            message: format!("Projected sequence group is missing field '{field}' required for partial delete"),
                        })
                }).collect::<crate::Result<HashSet<_>>>()
            })
            .transpose()?.unwrap_or_default();
        let aggregate_functions =
            config.validated_aggregate_functions(table_fields, primary_keys)?;
        let field_indices: HashMap<&str, usize> = output_fields
            .iter()
            .enumerate()
            .map(|(index, field)| (field.name(), index))
            .collect();
        let sequence_groups = groups
            .into_iter()
            .filter(|group| {
                group
                    .sequence_fields
                    .iter()
                    .chain(group.protected_fields.iter())
                    .any(|field| field_indices.contains_key(field.as_str()))
            })
            .map(|group| {
                let sequence_indices = group
                    .sequence_fields
                    .iter()
                    .map(|field| {
                        field_indices.get(field.as_str()).copied().ok_or_else(|| {
                            Error::UnexpectedError {
                                message: format!(
                                    "Projected partial-update sequence group is missing \
                                     sequence field '{field}'"
                                ),
                                source: None,
                            }
                        })
                    })
                    .collect::<crate::Result<Vec<_>>>()?;
                let protected_indices = group
                    .protected_fields
                    .iter()
                    .filter_map(|field| field_indices.get(field.as_str()).copied())
                    .collect();
                Ok(RuntimeSequenceGroup {
                    sequence_indices,
                    protected_indices,
                    ascending: group.ascending,
                })
            })
            .collect::<crate::Result<Vec<_>>>()?;
        let grouped_fields = sequence_groups
            .iter()
            .flat_map(|group| {
                group
                    .sequence_indices
                    .iter()
                    .chain(group.protected_indices.iter())
            })
            .copied()
            .collect();
        let aggregators = output_fields
            .iter()
            .map(|field| -> crate::Result<Option<Box<dyn FieldAggregator>>> {
                let Some(function) = aggregate_functions.get(field.name()) else {
                    return Ok(None);
                };
                Ok(Some(new_aggregator(
                    function,
                    field.name(),
                    field.data_type(),
                    table_options,
                )?))
            })
            .collect::<crate::Result<Vec<_>>>()?;

        Ok(Self {
            ignore_delete: CoreOptions::new(table_options).ignore_delete(),
            reduce_singletons: reduces_singletons(table_options),
            remove_record_on_delete: config.remove_record_on_delete(),
            sequence_group_partial_delete,
            sequence_group_enabled,
            sequence_groups,
            grouped_fields,
            aggregators: aggregators
                .iter()
                .any(Option::is_some)
                .then(|| Mutex::new(aggregators)),
        })
    }
}

impl MergeFunction for PartialUpdateMergeFunction {
    fn merge(
        &self,
        rows: &[MergeRow],
        batch_buffer: &[BufferedBatch],
        source_output_col_indices: &[usize],
        output_schema: &SchemaRef,
    ) -> crate::Result<MergeResult> {
        if !self.reduce_singletons {
            if let Some(result) = singleton_reducer_result(rows)? {
                return Ok(result);
            }
        }
        self.merge_unreduced(rows, batch_buffer, source_output_col_indices, output_schema)
    }

    fn merge_picked(
        &self,
        rows: &[MergeRow],
        batch_buffer: &[BufferedBatch],
        source_output_col_indices: &[usize],
        output_schema: &SchemaRef,
    ) -> crate::Result<Option<PickedMerge>> {
        // Aggregated fields have no input row to copy, and a lone version is
        // already returned as a source row by `merge`.
        if self.aggregators.is_some() || (!self.reduce_singletons && rows.len() == 1) {
            return Ok(None);
        }
        self.merge_rows(
            rows,
            batch_buffer,
            source_output_col_indices,
            output_schema,
            true,
        )
        .map(Some)
    }
}

impl PartialUpdateMergeFunction {
    pub(super) fn merge_unreduced(
        &self,
        rows: &[MergeRow],
        batch_buffer: &[BufferedBatch],
        source_output_col_indices: &[usize],
        output_schema: &SchemaRef,
    ) -> crate::Result<MergeResult> {
        match self.merge_rows(
            rows,
            batch_buffer,
            source_output_col_indices,
            output_schema,
            false,
        )? {
            PickedMerge::Result(result) => Ok(result),
            PickedMerge::Picked(_) => unreachable!("picked rows are only returned when requested"),
        }
    }

    /// With `pick`, a live row is returned as the input rows its columns come from.
    fn merge_rows(
        &self,
        rows: &[MergeRow],
        batch_buffer: &[BufferedBatch],
        source_output_col_indices: &[usize],
        output_schema: &SchemaRef,
        pick: bool,
    ) -> crate::Result<PickedMerge> {
        if rows.is_empty() {
            return Err(Error::UnexpectedError {
                message: "merge called with empty rows".to_string(),
                source: None,
            });
        }

        let mut ordered_row_indices: Vec<usize> = (0..rows.len()).collect();
        ordered_row_indices.sort_by(|&lhs_idx, &rhs_idx| {
            compare_sequence_order(&rows[lhs_idx], &rows[rhs_idx])
                .then_with(|| lhs_idx.cmp(&rhs_idx))
        });

        let mut selected_by_col: Vec<Option<(usize, usize)>> =
            vec![None; output_schema.fields().len()];
        let mut group_sequence_rows: Vec<Option<(usize, usize)>> =
            vec![None; self.sequence_groups.len()];
        let mut aggregators = match &self.aggregators {
            Some(aggregators) => Some(aggregators.lock().map_err(|e| Error::UnexpectedError {
                message: format!("PartialUpdateMergeFunction aggregator mutex poisoned: {e}"),
                source: None,
            })?),
            None => None,
        };
        if let Some(aggregators) = aggregators.as_mut() {
            for aggregator in aggregators.iter_mut().flatten() {
                aggregator.reset();
            }
        }
        let mut saw_add = false;
        let mut current_delete_row = false;
        // Whether a DELETE re-initialized the row and no later add cleared it,
        // see the result kind below.
        let mut meet_delete = false;
        let mut initialized = false;
        let mut latest_row = None;

        for row_idx in ordered_row_indices {
            let row = &rows[row_idx];
            let kind = RowKind::from_value(row.value_kind)?;
            if !kind.is_add() {
                if self.ignore_delete && !self.reduce_singletons {
                    continue;
                }
                if !initialized {
                    // Java initializes the row from the first retract before
                    // checking ignore-delete or applying sequence-group
                    // retractions. Keep that payload for later sparse updates.
                    Self::replace_with_source(
                        &mut selected_by_col,
                        aggregators.as_mut().map(|guard| guard.as_mut_slice()),
                        row.batch_idx,
                        row.row_idx,
                        batch_buffer,
                        source_output_col_indices,
                    )?;
                    group_sequence_rows.fill(Some((row.batch_idx, row.row_idx)));
                    initialized = true;
                }
                if self.ignore_delete {
                    continue;
                }
                latest_row = Some(row);
                if self.sequence_group_enabled {
                    let mut full_delete = false;
                    for (group_idx, group) in self.sequence_groups.iter().enumerate() {
                        let sequence_is_empty = group.sequence_indices.iter().all(|&index| {
                            batch_buffer[row.batch_idx]
                                .column_for_output(index, source_output_col_indices)
                                .is_null(row.row_idx)
                        });
                        if sequence_is_empty {
                            continue;
                        }
                        let ordering = match group_sequence_rows[group_idx] {
                            None => Ordering::Greater,
                            Some((current_batch, current_row)) => compare_sequence_group_rows(
                                row.batch_idx,
                                row.row_idx,
                                current_batch,
                                current_row,
                                group,
                                batch_buffer,
                                source_output_col_indices,
                            )?,
                        };
                        let advance = ordering.is_ge();
                        if advance
                            && kind == RowKind::Delete
                            && group
                                .sequence_indices
                                .iter()
                                .any(|index| self.sequence_group_partial_delete.contains(index))
                        {
                            full_delete = true;
                            break;
                        }
                        if advance {
                            group_sequence_rows[group_idx] = Some((row.batch_idx, row.row_idx));
                            for &index in &group.sequence_indices {
                                selected_by_col[index] = Some((row.batch_idx, row.row_idx));
                            }
                        }
                        for &index in &group.protected_indices {
                            if let Some(aggregator) = aggregators
                                .as_mut()
                                .and_then(|slots| slots.get_mut(index))
                                .and_then(Option::as_mut)
                            {
                                let source = batch_buffer[row.batch_idx]
                                    .column_for_output(index, source_output_col_indices);
                                aggregator.retract(source, row.row_idx)?;
                            } else if advance {
                                selected_by_col[index] = None;
                            }
                        }
                    }
                    if full_delete {
                        current_delete_row = true;
                        meet_delete = true;
                        Self::replace_with_source(
                            &mut selected_by_col,
                            aggregators.as_mut().map(|guard| guard.as_mut_slice()),
                            row.batch_idx,
                            row.row_idx,
                            batch_buffer,
                            source_output_col_indices,
                        )?;
                        group_sequence_rows.fill(Some((row.batch_idx, row.row_idx)));
                    }
                    continue;
                }
                if self.remove_record_on_delete {
                    current_delete_row = true;
                    if kind == RowKind::Delete {
                        meet_delete = true;
                        Self::replace_with_source(
                            &mut selected_by_col,
                            aggregators.as_mut().map(|guard| guard.as_mut_slice()),
                            row.batch_idx,
                            row.row_idx,
                            batch_buffer,
                            source_output_col_indices,
                        )?;
                    } else {
                        // Java PR 11178: an UPDATE_BEFORE is a retraction. It
                        // marks the row removed without re-initializing it, so
                        // a later add keeps the fields of other writers, and
                        // it applies its own non-null fields like an add.
                        self.update_non_null_fields(
                            &mut selected_by_col,
                            aggregators.as_mut().map(|guard| guard.as_mut_slice()),
                            row,
                            batch_buffer,
                            source_output_col_indices,
                        )?;
                    }
                    continue;
                }
                return Err(crate::Error::Unsupported {
                    message: "merge-engine=partial-update does not support DELETE or UPDATE_BEFORE without ignore-delete, sequence-group, or remove-record-on-delete".into(),
                });
            }
            saw_add = true;
            initialized = true;
            if !self.sequence_group_enabled {
                current_delete_row = false;
                meet_delete = false;
            }
            latest_row = Some(row);

            self.update_non_null_fields(
                &mut selected_by_col,
                aggregators.as_mut().map(|guard| guard.as_mut_slice()),
                row,
                batch_buffer,
                source_output_col_indices,
            )?;

            for (group_idx, group) in self.sequence_groups.iter().enumerate() {
                let sequence_is_empty = group.sequence_indices.iter().all(|&output_col_idx| {
                    batch_buffer[row.batch_idx]
                        .column_for_output(output_col_idx, source_output_col_indices)
                        .is_null(row.row_idx)
                });
                if sequence_is_empty {
                    continue;
                }

                let sequence_ordering = match group_sequence_rows[group_idx] {
                    None => Ordering::Greater,
                    Some((current_batch_idx, current_row_idx)) => compare_sequence_group_rows(
                        row.batch_idx,
                        row.row_idx,
                        current_batch_idx,
                        current_row_idx,
                        group,
                        batch_buffer,
                        source_output_col_indices,
                    )?,
                };
                let should_advance = sequence_ordering.is_ge();

                for &output_col_idx in &group.protected_indices {
                    if let Some(aggregator) = aggregators
                        .as_mut()
                        .and_then(|aggregators| aggregators.get_mut(output_col_idx))
                        .and_then(Option::as_mut)
                    {
                        let source_array = batch_buffer[row.batch_idx]
                            .column_for_output(output_col_idx, source_output_col_indices);
                        if should_advance {
                            aggregator.agg(source_array, row.row_idx)?;
                        } else {
                            aggregator.agg_reversed(source_array, row.row_idx)?;
                        }
                    } else if should_advance {
                        selected_by_col[output_col_idx] = Some((row.batch_idx, row.row_idx));
                    }
                }

                if !should_advance {
                    continue;
                }

                if group
                    .sequence_indices
                    .iter()
                    .any(|index| self.sequence_group_partial_delete.contains(index))
                {
                    current_delete_row = false;
                }
                group_sequence_rows[group_idx] = Some((row.batch_idx, row.row_idx));
                for &output_col_idx in &group.sequence_indices {
                    selected_by_col[output_col_idx] = Some((row.batch_idx, row.row_idx));
                }
            }
        }

        if !initialized {
            return Ok(PickedMerge::Result(MergeResult::Omit));
        }
        // A row removed only by UPDATE_BEFORE since the last add stays
        // UPDATE_BEFORE, so merging a pre-merged result again does not
        // re-initialize the row like a DELETE would. This also covers keys
        // with only UPDATE_BEFORE rows.
        let result_kind = if current_delete_row {
            if meet_delete {
                RowKind::Delete
            } else {
                RowKind::UpdateBefore
            }
        } else if saw_add {
            RowKind::Insert
        } else {
            RowKind::Delete
        };
        let deleted = result_kind != RowKind::Insert;
        // Ignored legacy retracts seed the value but do not advance Java's
        // latestSequenceNumber, whose reset value is zero.
        let ignored_retract_metadata = latest_row.is_none().then(|| MergeRow {
            sequence_number: 0,
            ..rows[0].clone()
        });
        let latest_row = latest_row.or(ignored_retract_metadata.as_ref()).unwrap();

        if pick && !deleted {
            let mut columns = selected_by_col;
            for (output_col_idx, field) in output_schema.fields().iter().enumerate() {
                if is_merged_metadata_field(field) {
                    columns[output_col_idx] = None;
                    continue;
                }
                let is_null = columns[output_col_idx].is_none_or(|(batch_idx, row_idx)| {
                    batch_buffer[batch_idx]
                        .column_for_output(output_col_idx, source_output_col_indices)
                        .is_null(row_idx)
                });
                if is_null && !field.is_nullable() {
                    return Err(Error::DataInvalid {
                        message: format!(
                            "merge-engine=partial-update produced NULL for non-nullable field '{}'",
                            field.name()
                        ),
                        source: None,
                    });
                }
            }
            return Ok(PickedMerge::Picked(PickedRow {
                columns,
                sequence_number: latest_row.sequence_number,
                values: Vec::new(),
            }));
        }

        let output_columns: Vec<ArrayRef> = output_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(output_col_idx, field)| {
                if let Some(column) = merged_metadata_column(field, latest_row, result_kind) {
                    return Ok(column);
                }
                let column = match aggregators
                    .as_ref()
                    .and_then(|aggregators| aggregators.get(output_col_idx))
                    .and_then(Option::as_ref)
                {
                    Some(aggregator) => aggregator.result()?,
                    None => match selected_by_col[output_col_idx] {
                        Some((batch_idx, row_idx)) => batch_buffer[batch_idx]
                            .column_for_output(output_col_idx, source_output_col_indices)
                            .slice(row_idx, 1),
                        None => new_null_array(field.data_type(), 1),
                    },
                };
                if (saw_add || current_delete_row) && !field.is_nullable() && column.is_null(0) {
                    return Err(Error::DataInvalid {
                        message: format!(
                            "merge-engine=partial-update produced NULL for non-nullable field '{}'",
                            field.name()
                        ),
                        source: None,
                    });
                }
                Ok(column)
            })
            .collect::<crate::Result<Vec<_>>>()?;

        let batch = RecordBatch::try_new(output_schema.clone(), output_columns).map_err(|e| {
            Error::UnexpectedError {
                message: format!("Failed to build partial-update materialized row: {e}"),
                source: Some(Box::new(e)),
            }
        })?;

        if deleted {
            debug_assert!(result_kind.is_retract());
            Ok(PickedMerge::Result(MergeResult::MaterializedRetractRow(
                batch,
                result_kind,
            )))
        } else {
            Ok(PickedMerge::Result(MergeResult::MaterializedRow(batch)))
        }
    }
}

fn compare_sequence_group_rows(
    incoming_batch_idx: usize,
    incoming_row_idx: usize,
    current_batch_idx: usize,
    current_row_idx: usize,
    group: &RuntimeSequenceGroup,
    batch_buffer: &[BufferedBatch],
    source_output_col_indices: &[usize],
) -> crate::Result<Ordering> {
    for &output_col_idx in &group.sequence_indices {
        let incoming = batch_buffer[incoming_batch_idx]
            .column_for_output(output_col_idx, source_output_col_indices);
        let current = batch_buffer[current_batch_idx]
            .column_for_output(output_col_idx, source_output_col_indices);
        let comparator = make_comparator(
            incoming,
            current,
            SortOptions {
                descending: false,
                nulls_first: true,
            },
        )
        .map_err(|e| Error::DataInvalid {
            message: format!("Failed to compare partial-update sequence-group fields: {e}"),
            source: Some(Box::new(e)),
        })?;
        let mut ordering = comparator(incoming_row_idx, current_row_idx);
        // Top-level nulls have lower precedence in either direction. Descending
        // order reverses non-null value comparisons, including nulls inside ROWs.
        if !group.ascending
            && !incoming.is_null(incoming_row_idx)
            && !current.is_null(current_row_idx)
        {
            ordering = ordering.reverse();
        }
        if !ordering.is_eq() {
            return Ok(ordering);
        }
    }
    Ok(Ordering::Equal)
}

// ---------------------------------------------------------------------------
// AggregateMergeFunction
// ---------------------------------------------------------------------------

/// Aggregation merge: apply a per-field aggregator across all rows
/// sharing the same primary key.
///
/// For each output column, the aggregator is selected by the following
/// priority (matching Java `AggregateMergeFunction#getAggFuncName`):
///
/// 1. Columns listed in the `sequence_fields` constructor argument (which
///    the reader populates from the `sequence.field` table option) are
///    forced to `last_value`, regardless of any per-field configuration.
/// 2. Primary-key columns get no aggregator; their value is copied through
///    from a representative row (Paimon guarantees same-PK rows share the
///    same key).
/// 3. `fields.<col>.aggregate-function`, if set.
/// 4. `fields.default-aggregate-function`, if set.
/// 5. Fall back to `last_non_null_value`.
///
/// Sequence is checked before primary key so a column that is both a PK
/// and a sequence field still gets `last_value`.
///
/// DELETE / UPDATE_BEFORE rows call each field aggregator's retract method.
/// `aggregation.remove-record-on-delete` turns DELETE into a full-row tombstone.
///
/// `aggregators` is held behind a `Mutex` so the implementation can mutate
/// per-key accumulators inside `MergeFunction::merge(&self, ...)` without
/// changing the trait signature shared with the other merge functions.  The
/// merge function is invoked sequentially by `sort_merge_stream`, so the
/// lock is effectively uncontended.
///
/// Reference: Java `org.apache.paimon.mergetree.compact.aggregate.AggregateMergeFunction`.
#[derive(Debug)]
pub(crate) struct AggregateMergeFunction {
    /// One slot per output column. `None` marks keys or KV metadata;
    /// `Some` holds the aggregator that owns a user value column.
    aggregators: Mutex<Vec<Option<Box<dyn FieldAggregator>>>>,
    remove_record_on_delete: bool,
    reduce_singletons: bool,
}

impl AggregateMergeFunction {
    pub(crate) fn new(
        table_options: &HashMap<String, String>,
        table_name: &str,
        output_fields: &[DataField],
        primary_keys: &[String],
        sequence_fields: &[String],
    ) -> crate::Result<Self> {
        let config = AggregationConfig::new(table_options);
        config.validate_runtime_mode(true, table_name)?;

        let pk_set: HashSet<&str> = primary_keys.iter().map(String::as_str).collect();
        let seq_set: HashSet<&str> = sequence_fields.iter().map(String::as_str).collect();

        // Per Java AggregateMergeFunction#createFieldAggregators, the priority
        // order is:
        //   1. sequence fields  → `last_value`
        //   2. primary keys     → no aggregator (PK columns are copied through)
        //   3. per-field `fields.<col>.aggregate-function`
        //   4. table-level `fields.default-aggregate-function`
        //   5. fall back to `last_non_null_value`
        // Sequence is checked before PK so a column that is both PK and sequence
        // field still gets `last_value` (matching Java).
        let aggregators: Vec<Option<Box<dyn FieldAggregator>>> = output_fields
            .iter()
            .map(|field| -> crate::Result<Option<Box<dyn FieldAggregator>>> {
                let name = field.name();
                if matches!(
                    field.id(),
                    crate::spec::SEQUENCE_NUMBER_FIELD_ID | crate::spec::VALUE_KIND_FIELD_ID
                ) {
                    return Ok(None);
                }
                let agg_name: &str = if seq_set.contains(name) {
                    "last_value"
                } else if pk_set.contains(name) {
                    return Ok(None);
                } else if let Some(per_field) = config.agg_function_for_field(name) {
                    per_field
                } else if let Some(default) = config.default_agg_function() {
                    default
                } else {
                    "last_non_null_value"
                };
                Ok(Some(new_aggregator(
                    agg_name,
                    name,
                    field.data_type(),
                    table_options,
                )?))
            })
            .collect::<crate::Result<Vec<_>>>()?;

        Ok(Self {
            aggregators: Mutex::new(aggregators),
            remove_record_on_delete: config.remove_record_on_delete(),
            reduce_singletons: reduces_singletons(table_options),
        })
    }

    /// Aggregate a key group already ordered by user sequence and auto sequence.
    /// The write buffer uses its sorted order directly; the merge reader sorts
    /// references from its input streams before sharing the same implementation.
    pub(super) fn merge_ordered(
        &self,
        rows: &[&MergeRow],
        batch_buffer: &[BufferedBatch],
        source_output_col_indices: &[usize],
        output_schema: &SchemaRef,
    ) -> crate::Result<(RecordBatch, bool)> {
        match self.merge_ordered_rows(
            rows,
            batch_buffer,
            source_output_col_indices,
            output_schema,
            false,
        )? {
            PickedMerge::Result(MergeResult::MaterializedRow(batch)) => Ok((batch, false)),
            PickedMerge::Result(MergeResult::MaterializedRetractRow(batch, _)) => Ok((batch, true)),
            _ => unreachable!("aggregation materializes every row unless picks are requested"),
        }
    }

    /// With `pick`, a live row is returned as its key row plus the aggregator results.
    fn merge_ordered_rows(
        &self,
        rows: &[&MergeRow],
        batch_buffer: &[BufferedBatch],
        source_output_col_indices: &[usize],
        output_schema: &SchemaRef,
        pick: bool,
    ) -> crate::Result<PickedMerge> {
        if rows.is_empty() {
            return Err(Error::UnexpectedError {
                message: "merge called with empty rows".to_string(),
                source: None,
            });
        }

        let mut aggregators = self
            .aggregators
            .lock()
            .map_err(|e| Error::UnexpectedError {
                message: format!("AggregateMergeFunction aggregator mutex poisoned: {e}"),
                source: None,
            })?;
        for slot in aggregators.iter_mut() {
            if let Some(agg) = slot.as_mut() {
                agg.reset();
            }
        }

        let mut current_delete_row = false;
        for row in rows {
            let kind = RowKind::from_value(row.value_kind)?;
            if self.remove_record_on_delete && kind == RowKind::Delete {
                for (col_idx, aggregator) in aggregators.iter_mut().enumerate() {
                    if let Some(aggregator) = aggregator.as_mut() {
                        let source_array = batch_buffer[row.batch_idx]
                            .column_for_output(col_idx, source_output_col_indices);
                        aggregator.replace_with_delete(source_array, row.row_idx)?;
                    }
                }
                current_delete_row = true;
                continue;
            }
            current_delete_row = false;
            for (col_idx, slot) in aggregators.iter_mut().enumerate() {
                if let Some(agg) = slot.as_mut() {
                    let source_array = batch_buffer[row.batch_idx]
                        .column_ref_for_output(col_idx, source_output_col_indices);
                    if kind.is_add() {
                        agg.agg_array(source_array, row.row_idx)?;
                    } else {
                        agg.retract(source_array.as_ref(), row.row_idx)?;
                    }
                }
            }
        }

        // Java getResult uses the last KV for keys and record metadata.
        let pk_source = rows.last().unwrap();
        let null_error = |field: &arrow_schema::Field, aggregator: Option<&dyn FieldAggregator>| {
            let message = match aggregator {
                Some(agg) => format!(
                    "merge-engine=aggregation: aggregator '{}' produced NULL for \
                     non-nullable field '{}'",
                    agg.name(),
                    field.name()
                ),
                None => format!(
                    "merge-engine=aggregation: primary-key column '{}' contains NULL on a \
                     source row; declare the column nullable or fix the upstream data",
                    field.name()
                ),
            };
            Error::DataInvalid {
                message,
                source: None,
            }
        };

        if pick && !current_delete_row {
            let mut columns = vec![None; output_schema.fields().len()];
            let mut values = vec![None; output_schema.fields().len()];
            for (col_idx, field) in output_schema.fields().iter().enumerate() {
                if is_merged_metadata_field(field) {
                    continue;
                }
                let is_null = match aggregators[col_idx].as_ref() {
                    Some(agg) => {
                        let value = agg.result()?;
                        let is_null = value.is_null(0);
                        values[col_idx] = Some(value);
                        is_null
                    }
                    None => {
                        columns[col_idx] = Some((pk_source.batch_idx, pk_source.row_idx));
                        batch_buffer[pk_source.batch_idx]
                            .column_for_output(col_idx, source_output_col_indices)
                            .is_null(pk_source.row_idx)
                    }
                };
                if is_null && !field.is_nullable() {
                    return Err(null_error(field, aggregators[col_idx].as_deref()));
                }
            }
            return Ok(PickedMerge::Picked(PickedRow {
                columns,
                sequence_number: pk_source.sequence_number,
                values,
            }));
        }

        let output_columns: Vec<ArrayRef> = aggregators
            .iter()
            .enumerate()
            .map(|(col_idx, slot)| -> crate::Result<ArrayRef> {
                if let Some(column) = merged_metadata_column(
                    output_schema.field(col_idx),
                    pk_source,
                    if current_delete_row {
                        RowKind::Delete
                    } else {
                        RowKind::Insert
                    },
                ) {
                    return Ok(column);
                }
                if current_delete_row {
                    return Ok(batch_buffer[pk_source.batch_idx]
                        .column_for_output(col_idx, source_output_col_indices)
                        .slice(pk_source.row_idx, 1));
                }
                match slot {
                    Some(agg) => agg.result(),
                    None => Ok(batch_buffer[pk_source.batch_idx]
                        .column_for_output(col_idx, source_output_col_indices)
                        .slice(pk_source.row_idx, 1)),
                }
            })
            .collect::<crate::Result<Vec<_>>>()?;

        // Defensive check: non-nullable fields must not contain NULL on the
        // merged output (e.g. `min` on an all-NULL value group, or a NULL
        // primary-key cell on the source row).  Split the message so the
        // operator knows whether to look at the aggregator config or at the
        // upstream data.
        for (col_idx, field) in output_schema.fields().iter().enumerate() {
            if !field.is_nullable() && output_columns[col_idx].is_null(0) {
                return Err(null_error(field, aggregators[col_idx].as_deref()));
            }
        }

        let batch = RecordBatch::try_new(output_schema.clone(), output_columns).map_err(|e| {
            Error::UnexpectedError {
                message: format!("Failed to build aggregation materialized row: {e}"),
                source: Some(Box::new(e)),
            }
        })?;

        Ok(PickedMerge::Result(if current_delete_row {
            MergeResult::MaterializedRetractRow(batch, RowKind::Delete)
        } else {
            MergeResult::MaterializedRow(batch)
        }))
    }
}

impl MergeFunction for AggregateMergeFunction {
    fn merge(
        &self,
        rows: &[MergeRow],
        batch_buffer: &[BufferedBatch],
        source_output_col_indices: &[usize],
        output_schema: &SchemaRef,
    ) -> crate::Result<MergeResult> {
        if !self.reduce_singletons {
            if let Some(result) = singleton_reducer_result(rows)? {
                return Ok(result);
            }
        }
        let mut ordered: Vec<_> = rows.iter().collect();
        // Stable sorting keeps input order for equal sequences.
        ordered.sort_by(|lhs, rhs| compare_sequence_order(lhs, rhs));
        let (batch, delete) = self.merge_ordered(
            &ordered,
            batch_buffer,
            source_output_col_indices,
            output_schema,
        )?;
        Ok(if delete {
            MergeResult::MaterializedRetractRow(batch, RowKind::Delete)
        } else {
            MergeResult::MaterializedRow(batch)
        })
    }

    fn merge_picked(
        &self,
        rows: &[MergeRow],
        batch_buffer: &[BufferedBatch],
        source_output_col_indices: &[usize],
        output_schema: &SchemaRef,
    ) -> crate::Result<Option<PickedMerge>> {
        // A lone version is already returned as a source row by `merge`.
        if !self.reduce_singletons && rows.len() == 1 {
            return Ok(None);
        }
        let mut ordered: Vec<_> = rows.iter().collect();
        ordered.sort_by(|lhs, rhs| compare_sequence_order(lhs, rhs));
        self.merge_ordered_rows(
            &ordered,
            batch_buffer,
            source_output_col_indices,
            output_schema,
            true,
        )
        .map(Some)
    }
}

// ---------------------------------------------------------------------------
// SortMergeCursor
// ---------------------------------------------------------------------------

/// Cursor tracking position within a single stream's current RecordBatch.
struct SortMergeCursor {
    batch: RecordBatch,
    /// Row-encoded keys for the current batch (via arrow-row).
    rows: Rows,
    /// Typed user-defined sequence fields for the current batch.
    user_rows: Option<Rows>,
    /// `_SEQUENCE_NUMBER` and `_VALUE_KIND`, downcast once per batch.
    sequence_numbers: Int64Array,
    value_kinds: Option<Int8Array>,
    offset: usize,
}

impl SortMergeCursor {
    fn new(
        batch: RecordBatch,
        rows: Rows,
        user_rows: Option<Rows>,
        seq_index: usize,
        value_kind_index: usize,
    ) -> Self {
        let sequence_numbers = batch
            .column(seq_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("_SEQUENCE_NUMBER column must be Int64")
            .clone();
        let value_kinds = batch
            .column(value_kind_index)
            .as_any()
            .downcast_ref::<Int8Array>()
            .cloned();
        Self {
            batch,
            rows,
            user_rows,
            sequence_numbers,
            value_kinds,
            offset: 0,
        }
    }

    fn is_finished(&self) -> bool {
        self.offset >= self.rows.num_rows()
    }

    fn current_row(&self) -> arrow_row::Row<'_> {
        self.rows.row(self.offset)
    }

    fn advance(&mut self) {
        self.offset += 1;
    }

    fn sequence_number(&self) -> i64 {
        self.sequence_numbers.value(self.offset)
    }

    fn value_kind(&self) -> i8 {
        match &self.value_kinds {
            Some(kinds) if kinds.is_valid(self.offset) => kinds.value(self.offset),
            _ => 0, // default to INSERT for NULL or missing _VALUE_KIND
        }
    }

    fn user_sequence(&self) -> Option<OwnedRow> {
        self.user_rows
            .as_ref()
            .map(|rows| rows.row(self.offset).owned())
    }
}

// ---------------------------------------------------------------------------
// LoserTree
// ---------------------------------------------------------------------------

/// A LoserTree (tournament tree) for k-way merge.
///
/// Layout follows DataFusion's `SortPreservingMergeStream`:
/// - `nodes[0]` = overall winner index
/// - `nodes[1..k]` = loser at each internal node
///
/// Reference: <https://en.wikipedia.org/wiki/K-way_merge_algorithm#Tournament_Tree>
struct LoserTree {
    /// nodes[0] = winner, nodes[1..] = losers
    nodes: Vec<usize>,
    num_streams: usize,
}

impl LoserTree {
    fn new(num_streams: usize) -> Self {
        Self {
            nodes: vec![usize::MAX; num_streams],
            num_streams,
        }
    }

    fn winner(&self) -> usize {
        self.nodes[0]
    }

    /// Leaf node index for a given stream index.
    fn leaf_index(&self, stream_idx: usize) -> usize {
        (self.num_streams + stream_idx) / 2
    }

    fn parent_index(node_idx: usize) -> usize {
        node_idx / 2
    }

    /// Build the tree from scratch given a comparison function.
    /// `is_gt(a, b)` returns true if stream `a` > stream `b`.
    fn init(&mut self, is_gt: impl Fn(usize, usize) -> bool) {
        self.nodes.fill(usize::MAX);
        for i in 0..self.num_streams {
            let mut winner = i;
            let mut cmp_node = self.leaf_index(i);
            while cmp_node != 0 && self.nodes[cmp_node] != usize::MAX {
                let challenger = self.nodes[cmp_node];
                if is_gt(winner, challenger) {
                    self.nodes[cmp_node] = winner;
                    winner = challenger;
                }
                cmp_node = Self::parent_index(cmp_node);
            }
            self.nodes[cmp_node] = winner;
        }
    }

    /// Update the tree after the winner has been consumed/advanced.
    fn update(&mut self, is_gt: impl Fn(usize, usize) -> bool) {
        let mut winner = self.nodes[0];
        let mut cmp_node = self.leaf_index(winner);
        while cmp_node != 0 {
            let challenger = self.nodes[cmp_node];
            if is_gt(winner, challenger) {
                self.nodes[cmp_node] = winner;
                winner = challenger;
            }
            cmp_node = Self::parent_index(cmp_node);
        }
        self.nodes[0] = winner;
    }
}

// ---------------------------------------------------------------------------
// SortMergeReader
// ---------------------------------------------------------------------------

/// Configuration for building a [`SortMergeReader`].
pub(crate) struct SortMergeReaderBuilder {
    /// Inputs must each be monotonic by primary key. Concatenated files are valid
    /// only when their key ranges are strictly disjoint and ordered.
    streams: Vec<ArrowRecordBatchStream>,
    /// Full schema of the input streams (key + seq + value_kind + value columns).
    input_schema: SchemaRef,
    /// Indices of primary key columns in input_schema.
    key_indices: Vec<usize>,
    /// Index of _SEQUENCE_NUMBER column in input_schema.
    seq_index: usize,
    /// Index of _VALUE_KIND column in input_schema.
    value_kind_index: usize,
    /// Indices of user-defined sequence field columns in input_schema (if configured).
    user_sequence_indices: Vec<usize>,
    user_sequence_descending: bool,
    /// Indices of user value columns in input_schema (output columns).
    value_indices: Vec<usize>,
    /// Output schema (key + value columns, no system columns).
    output_schema: SchemaRef,
    merge_function: Box<dyn MergeFunction>,
    batch_size: usize,
    lookup: Option<Arc<dyn LookupProvider>>,
}

impl SortMergeReaderBuilder {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        streams: Vec<ArrowRecordBatchStream>,
        input_schema: SchemaRef,
        key_indices: Vec<usize>,
        seq_index: usize,
        value_kind_index: usize,
        user_sequence_indices: Vec<usize>,
        value_indices: Vec<usize>,
        output_schema: SchemaRef,
        merge_function: Box<dyn MergeFunction>,
    ) -> Self {
        Self {
            streams,
            input_schema,
            key_indices,
            seq_index,
            value_kind_index,
            user_sequence_indices,
            user_sequence_descending: false,
            value_indices,
            output_schema,
            merge_function,
            batch_size: 1024,
            lookup: None,
        }
    }

    pub(crate) fn with_lookup(mut self, lookup: Option<Arc<dyn LookupProvider>>) -> Self {
        self.lookup = lookup;
        self
    }

    pub(crate) fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Replace the input streams, for a builder prepared before they exist.
    pub(crate) fn with_streams(mut self, streams: Vec<ArrowRecordBatchStream>) -> Self {
        self.streams = streams;
        self
    }

    pub(crate) fn with_user_sequence_descending(mut self, descending: bool) -> Self {
        self.user_sequence_descending = descending;
        self
    }

    /// Build the sort-merge stream.
    pub(crate) fn build(self) -> crate::Result<ArrowRecordBatchStream> {
        let sort_fields: Vec<SortField> = self
            .key_indices
            .iter()
            .map(|&idx| SortField::new(self.input_schema.field(idx).data_type().clone()))
            .collect();

        let row_converter = RowConverter::new(sort_fields).map_err(|e| Error::UnexpectedError {
            message: format!("Failed to create RowConverter: {e}"),
            source: Some(Box::new(e)),
        })?;

        let user_sequence_converter = if self.user_sequence_indices.is_empty() {
            None
        } else {
            let fields = self
                .user_sequence_indices
                .iter()
                .map(|&idx| {
                    SortField::new_with_options(
                        self.input_schema.field(idx).data_type().clone(),
                        SortOptions {
                            descending: self.user_sequence_descending,
                            nulls_first: true,
                        },
                    )
                })
                .collect();
            Some(RowConverter::new(fields).map_err(|e| Error::DataInvalid {
                message: format!("Unsupported user sequence field type: {e}"),
                source: Some(Box::new(e)),
            })?)
        };

        sort_merge_stream(
            self.streams,
            row_converter,
            user_sequence_converter,
            self.key_indices,
            self.seq_index,
            self.value_kind_index,
            self.user_sequence_indices,
            self.value_indices,
            self.output_schema,
            self.merge_function,
            self.batch_size,
            self.lookup.map(|lookup| (lookup, self.input_schema)),
        )
    }
}

/// Convert a RecordBatch's key columns into arrow-row `Rows`.
fn convert_batch_keys(
    batch: &RecordBatch,
    key_indices: &[usize],
    converter: &mut RowConverter,
) -> crate::Result<Rows> {
    let key_columns: Vec<ArrayRef> = key_indices
        .iter()
        .map(|&idx| batch.column(idx).clone())
        .collect();
    converter
        .convert_columns(&key_columns)
        .map_err(|e| Error::UnexpectedError {
            message: format!("Failed to convert key columns to Rows: {e}"),
            source: Some(Box::new(e)),
        })
}

fn convert_batch_user_sequences(
    batch: &RecordBatch,
    indices: &[usize],
    converter: Option<&mut RowConverter>,
) -> crate::Result<Option<Rows>> {
    let Some(converter) = converter else {
        return Ok(None);
    };
    let columns = indices
        .iter()
        .map(|&idx| batch.column(idx).clone())
        .collect::<Vec<_>>();
    converter
        .convert_columns(&columns)
        .map(Some)
        .map_err(|e| Error::DataInvalid {
            message: format!("Failed to encode user sequence fields: {e}"),
            source: Some(Box::new(e)),
        })
}

/// Compare two cursors by their current key. `None` cursors are treated as
/// greater than any value (exhausted streams sink to the bottom).
fn compare_cursors(cursors: &[Option<SortMergeCursor>], a: usize, b: usize) -> Ordering {
    match (&cursors[a], &cursors[b]) {
        (None, None) => Ordering::Equal,
        (None, _) => Ordering::Greater,
        (_, None) => Ordering::Less,
        (Some(ca), Some(cb)) => ca.current_row().cmp(&cb.current_row()),
    }
}

/// The main sort-merge stream implementation.
///
/// Uses an interleave-based output strategy (like DataFusion's BatchBuilder):
/// instead of slicing individual rows and concatenating, we record
/// `(batch_idx, row_idx)` indices and use `arrow_select::interleave` to
/// gather all output rows in one pass per column.
#[allow(clippy::too_many_arguments)]
fn sort_merge_stream(
    mut streams: Vec<ArrowRecordBatchStream>,
    mut row_converter: RowConverter,
    mut user_sequence_converter: Option<RowConverter>,
    key_indices: Vec<usize>,
    seq_index: usize,
    value_kind_index: usize,
    user_sequence_indices: Vec<usize>,
    value_indices: Vec<usize>,
    output_schema: SchemaRef,
    merge_function: Box<dyn MergeFunction>,
    batch_size: usize,
    lookup: Option<(Arc<dyn LookupProvider>, SchemaRef)>,
) -> crate::Result<ArrowRecordBatchStream> {
    let num_streams = streams.len();
    if num_streams == 0 {
        return Ok(futures::stream::empty().boxed());
    }

    // Output column indices for source batches: key columns + value columns
    // (skip system columns like _SEQUENCE_NUMBER).
    let source_output_col_indices: Vec<usize> = key_indices
        .iter()
        .chain(value_indices.iter())
        .copied()
        .collect();

    Ok(try_stream! {
        // Initialize cursors: read first non-empty batch from each stream.
        // Loop to skip empty batches (e.g. from predicate filtering).
        let mut cursors: Vec<Option<SortMergeCursor>> = Vec::with_capacity(num_streams);
        for stream in &mut streams {
            let mut found = false;
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result?;
                if batch.num_rows() > 0 {
                    let rows = convert_batch_keys(&batch, &key_indices, &mut row_converter)?;
                    let user_rows = convert_batch_user_sequences(
                        &batch,
                        &user_sequence_indices,
                        user_sequence_converter.as_mut(),
                    )?;
                    cursors.push(Some(SortMergeCursor::new(batch, rows, user_rows, seq_index, value_kind_index)));
                    found = true;
                    break;
                }
            }
            if !found {
                cursors.push(None);
            }
        }

        // Build loser tree.
        let mut tree = LoserTree::new(num_streams);
        tree.init(|a, b| compare_cursors(&cursors, a, b).then_with(|| a.cmp(&b)).is_gt());

        // Batch buffer: stores RecordBatches referenced by output indices.
        // Each cursor's current batch gets an entry; when a cursor advances
        // to a new batch, the old one stays in the buffer until the output
        // batch is flushed.
        let mut batch_buffer: Vec<BufferedBatch> = Vec::new();
        // Map from stream_idx -> current batch_buffer index.
        let mut stream_batch_idx: Vec<Option<usize>> = vec![None; num_streams];

        // Register initial batches.
        for (i, cursor) in cursors.iter().enumerate() {
            if let Some(c) = cursor {
                let idx = batch_buffer.len();
                batch_buffer.push(BufferedBatch::Source(c.batch.clone()));
                stream_batch_idx[i] = Some(idx);
            }
        }

        // Output indices: (batch_buffer_idx, row_idx) for interleave.
        let mut output_indices: Vec<(usize, usize)> = Vec::with_capacity(batch_size);
        // Output rows at `(PICKED_ROW, i)` are `picked_rows[i]`.
        let mut picked_rows: Vec<PickedRow> = Vec::new();
        // Changelog rows not yet handed on, and the schema that marks them. A
        // row taken from a source batch is a zero-copy slice that keeps the
        // whole batch alive, so `pending_changelog` is copied into
        // `changelog_chunks` whenever source batches are released.
        let mut pending_changelog: Vec<RecordBatch> = Vec::new();
        let mut changelog_chunks: Vec<RecordBatch> = Vec::new();
        let mut changelog_rows = 0;
        let changelog_schema = {
            let mut metadata = output_schema.metadata().clone();
            metadata.insert(CHANGELOG_BATCH_METADATA_KEY.into(), "true".into());
            Arc::new(output_schema.as_ref().clone().with_metadata(metadata))
        };
        // Reused for every key, so grouping allocates nothing per key.
        let mut winner_key: Vec<u8> = Vec::new();
        let mut same_key_rows: Vec<MergeRow> = Vec::new();

        loop {
            let winner_idx = tree.winner();
            // Check if all streams are exhausted.
            if cursors[winner_idx].is_none() {
                break;
            }

            // Capture the winner's key for grouping same-key rows. Encoded
            // rows compare by their bytes, so byte equality is key equality.
            let winner = cursors[winner_idx].as_ref().unwrap();
            winner_key.clear();
            winner_key.extend_from_slice(winner.current_row().as_ref());

            // Collect all rows with the same key across all streams.
            same_key_rows.clear();

            loop {
                let current_winner = tree.winner();
                let matches = match &cursors[current_winner] {
                    None => false,
                    Some(c) => c.current_row().as_ref() == winner_key.as_slice(),
                };
                if !matches {
                    break;
                }

                // Record this row.
                {
                    let cursor = cursors[current_winner].as_ref().unwrap();
                    let buf_idx = stream_batch_idx[current_winner].unwrap();
                    same_key_rows.push(MergeRow {
                        batch_idx: buf_idx,
                        row_idx: cursor.offset,
                        sequence_number: cursor.sequence_number(),
                        value_kind: cursor.value_kind(),
                        user_sequence: cursor.user_sequence(),
                    });
                }

                // Advance the cursor.
                {
                    let cursor = cursors[current_winner].as_mut().unwrap();
                    cursor.advance();
                    if cursor.is_finished() {
                        // Try to get next non-empty batch from this stream.
                        // Loop to skip empty batches.
                        cursors[current_winner] = None;
                        while let Some(batch_result) = streams[current_winner].next().await {
                            let batch = batch_result?;
                            if batch.num_rows() > 0 {
                                let rows = convert_batch_keys(&batch, &key_indices, &mut row_converter)?;
                                let user_rows = convert_batch_user_sequences(
                                    &batch,
                                    &user_sequence_indices,
                                    user_sequence_converter.as_mut(),
                                )?;
                                let buf_idx = batch_buffer.len();
                                batch_buffer.push(BufferedBatch::Source(batch.clone()));
                                stream_batch_idx[current_winner] = Some(buf_idx);
                                cursors[current_winner] = Some(SortMergeCursor::new(batch, rows, user_rows, seq_index, value_kind_index));
                                break;
                            }
                        }
                    }
                }

                // Update loser tree after advancing.
                tree.update(|a, b| compare_cursors(&cursors, a, b).then_with(|| a.cmp(&b)).is_gt());
            }

            if let Some((lookup, schema)) = &lookup {
                let mut has_base = false;
                for row in &same_key_rows {
                    if row.level(&batch_buffer)? > 0 {
                        has_base = true;
                        break;
                    }
                }
                if !has_base {
                    if let Some(batch) = lookup.lookup(&winner_key).await? {
                        if batch.num_rows() != 1 || batch.schema().fields() != schema.fields() {
                            Err(Error::DataInvalid { message: "Lookup must return one KV row matching the merge schema".into(), source: None })?;
                        }
                        let keys = convert_batch_keys(&batch, &key_indices, &mut row_converter)?;
                        if keys.row(0).as_ref() != winner_key.as_slice() {
                            Err(Error::DataInvalid { message: "Lookup returned a different primary key".into(), source: None })?;
                        }
                        let user = convert_batch_user_sequences(&batch, &user_sequence_indices, user_sequence_converter.as_mut())?;
                        let sequence_number = batch.column(seq_index).as_any().downcast_ref::<Int64Array>()
                            .ok_or_else(|| Error::DataInvalid { message: "Invalid lookup sequence".into(), source: None })?.value(0);
                        let value_kind = batch.column(value_kind_index).as_any().downcast_ref::<Int8Array>()
                            .ok_or_else(|| Error::DataInvalid { message: "Invalid lookup row kind".into(), source: None })?.value(0);
                        let batch_idx = batch_buffer.len();
                        batch_buffer.push(BufferedBatch::Source(batch));
                        same_key_rows.insert(0, MergeRow { batch_idx, row_idx: 0, sequence_number, value_kind,
                            user_sequence: user.map(|rows| rows.row(0).owned()) });
                    }
                }
            }
            let (merged, changelog) = match merge_function.merge_picked(
                &same_key_rows, &batch_buffer, &source_output_col_indices, &output_schema,
            )? {
                Some(PickedMerge::Picked(row)) => {
                    output_indices.push((PICKED_ROW, picked_rows.len()));
                    picked_rows.push(row);
                    // Already queued above.
                    (MergeResult::Omit, None)
                }
                Some(PickedMerge::Result(result)) => (result, None),
                None => merge_function.merge_with_changelog(
                    &same_key_rows, &batch_buffer, &source_output_col_indices, &output_schema,
                )?,
            };
            match merged {
                MergeResult::SourceRow { batch_idx, row_idx } => {
                    output_indices.push((batch_idx, row_idx));
                }
                MergeResult::MaterializedRow(batch) => {
                    if batch.num_rows() != 1 {
                        Err(Error::UnexpectedError {
                            message: format!(
                                "Materialized merge result must contain exactly one row, got {}",
                                batch.num_rows()
                            ),
                            source: None,
                        })?;
                    }
                    if batch.schema().as_ref() != output_schema.as_ref() {
                        Err(Error::UnexpectedError {
                            message: "Materialized merge result schema does not match merge output schema".to_string(),
                            source: None,
                        })?;
                    }
                    let batch_idx = batch_buffer.len();
                    batch_buffer.push(BufferedBatch::Materialized(batch));
                    output_indices.push((batch_idx, 0));
                }
                MergeResult::Omit | MergeResult::MaterializedRetractRow(..) => {}
            }

            if let Some(changelog) = changelog {
                if !Arc::ptr_eq(&changelog.schema(), &output_schema)
                    && changelog.schema().as_ref() != output_schema.as_ref()
                {
                    Err(Error::UnexpectedError { message: "Changelog merge result schema does not match merge output schema".into(), source: None })?;
                }
                changelog_rows += changelog.num_rows();
                pending_changelog.push(changelog);
                // Changelog comes one key at a time; hand it on in batches so the
                // writer does not handle a one-row batch per key. The buffer stays
                // bounded by the batch size even when every data row is deleted.
                if changelog_rows >= batch_size {
                    yield changelog_batch(
                        &changelog_schema,
                        &mut changelog_chunks,
                        &mut pending_changelog,
                    )?;
                    changelog_rows = 0;
                }
            }
            if output_indices.is_empty()
                && compact_batch_buffer(&mut batch_buffer, &mut stream_batch_idx, &cursors)
            {
                detach_changelog(&mut changelog_chunks, &mut pending_changelog)?;
            }

            // Yield a batch when we've accumulated enough rows.
            if output_indices.len() >= batch_size {
                let batch = build_output(
                    &output_schema,
                    &batch_buffer,
                    &source_output_col_indices,
                    &output_indices,
                    &picked_rows,
                )?;
                output_indices.clear();
                picked_rows.clear();
                // Compact batch buffer after the pending output rows have been
                // materialized. Source batches still referenced by cursors stay
                // alive; materialized batches can be dropped here because they
                // are referenced only by the flushed output_indices above.
                if compact_batch_buffer(&mut batch_buffer, &mut stream_batch_idx, &cursors) {
                    detach_changelog(&mut changelog_chunks, &mut pending_changelog)?;
                }
                yield batch;
            }
        }

        if !pending_changelog.is_empty() || !changelog_chunks.is_empty() {
            yield changelog_batch(&changelog_schema, &mut changelog_chunks, &mut pending_changelog)?;
        }

        // Yield remaining rows.
        if !output_indices.is_empty() {
            let batch = build_output(
                &output_schema,
                &batch_buffer,
                &source_output_col_indices,
                &output_indices,
                &picked_rows,
            )?;
            yield batch;
        }
    }
    .boxed())
}

/// Copies the pending per-key changelog rows into one chunk that owns its
/// buffers. A row sliced from a source batch keeps that whole batch alive and
/// is charged in full by the format writer.
fn detach_changelog(
    chunks: &mut Vec<RecordBatch>,
    pending: &mut Vec<RecordBatch>,
) -> crate::Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    let schema = pending[0].schema();
    let indices = pending
        .iter()
        .enumerate()
        .flat_map(|(batch, rows)| (0..rows.num_rows()).map(move |row| (batch, row)))
        .collect::<Vec<_>>();
    let chunk = (0..schema.fields().len())
        .map(|column| {
            let arrays = pending
                .iter()
                .map(|batch| batch.column(column).as_ref())
                .collect::<Vec<_>>();
            if indexes_each_element(schema.field(column).data_type()) {
                copy_ranges(&arrays, indices.len())
            } else {
                interleave(&arrays, &indices)
            }
        })
        .collect::<Result<Vec<_>, _>>()
        .and_then(|columns| {
            RecordBatch::try_new_with_options(
                schema,
                columns,
                &RecordBatchOptions::new().with_row_count(Some(indices.len())),
            )
        })
        .map_err(|error| Error::DataInvalid {
            message: format!("Invalid changelog batch: {error}"),
            source: Some(Box::new(error)),
        })?;
    chunks.push(chunk);
    pending.clear();
    Ok(())
}

/// Whether `interleave` builds an index for every element of a selected row:
/// it does for MAP entries and for list items that are not primitive.
fn indexes_each_element(data_type: &DataType) -> bool {
    match data_type {
        DataType::Map(..) => true,
        DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _) => {
            !item.data_type().is_primitive()
        }
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| indexes_each_element(field.data_type())),
        _ => false,
    }
}

/// Copies whole arrays one after another, by value range.
fn copy_ranges(arrays: &[&dyn Array], rows: usize) -> Result<ArrayRef, ArrowError> {
    let data = arrays
        .iter()
        .map(|array| array.to_data())
        .collect::<Vec<_>>();
    let mut copy = MutableArrayData::new(data.iter().collect(), false, rows);
    for (index, array) in data.iter().enumerate() {
        copy.try_extend(index, 0, array.len())?;
    }
    Ok(make_array(copy.freeze()))
}

/// Hands on the changelog gathered so far as one batch that owns its rows.
fn changelog_batch(
    schema: &SchemaRef,
    chunks: &mut Vec<RecordBatch>,
    pending: &mut Vec<RecordBatch>,
) -> crate::Result<RecordBatch> {
    detach_changelog(chunks, pending)?;
    let batch = if chunks.len() == 1 {
        chunks.pop().unwrap().with_schema(schema.clone())
    } else {
        let batch = arrow_select::concat::concat_batches(schema, chunks.iter());
        chunks.clear();
        batch
    };
    batch.map_err(|error| Error::DataInvalid {
        message: format!("Invalid changelog batch: {error}"),
        source: Some(Box::new(error)),
    })
}

/// Batch index marking an output row as a [`PickedRow`].
const PICKED_ROW: usize = usize::MAX;

fn build_output(
    schema: &SchemaRef,
    batch_buffer: &[BufferedBatch],
    source_output_col_indices: &[usize],
    indices: &[(usize, usize)],
    picked_rows: &[PickedRow],
) -> crate::Result<RecordBatch> {
    if picked_rows.is_empty() {
        return build_output_interleave(schema, batch_buffer, source_output_col_indices, indices);
    }
    // Picked rows take each column from its own input row. Their NULLs and KV
    // metadata come from one extra array per column, at the picked row's index.
    let extra_idx = batch_buffer.len();
    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(output_col_idx, field)| {
            let extra: ArrayRef = match field.name().as_str() {
                crate::spec::SEQUENCE_NUMBER_FIELD_NAME => Arc::new(Int64Array::from_iter_values(
                    picked_rows.iter().map(|row| row.sequence_number),
                )),
                crate::spec::VALUE_KIND_FIELD_NAME => {
                    Arc::new(Int8Array::from(vec![
                        RowKind::Insert as i8;
                        picked_rows.len()
                    ]))
                }
                _ if picked_rows
                    .iter()
                    .any(|row| matches!(row.values.get(output_col_idx), Some(Some(_)))) =>
                {
                    let nulls = new_null_array(field.data_type(), 1);
                    let parts: Vec<&dyn arrow_array::Array> = picked_rows
                        .iter()
                        .map(|row| match row.values.get(output_col_idx) {
                            Some(Some(value)) => value.as_ref(),
                            _ => nulls.as_ref(),
                        })
                        .collect();
                    arrow_select::concat::concat(&parts).map_err(|e| Error::UnexpectedError {
                        message: format!("Failed to concat merged column {output_col_idx}: {e}"),
                        source: Some(Box::new(e)),
                    })?
                }
                _ => new_null_array(field.data_type(), picked_rows.len()),
            };
            let mut arrays: Vec<&dyn arrow_array::Array> = batch_buffer
                .iter()
                .map(|batch| batch.column_for_output(output_col_idx, source_output_col_indices))
                .collect();
            arrays.push(extra.as_ref());
            let column_indices: Vec<(usize, usize)> = indices
                .iter()
                .map(|&(batch_idx, row_idx)| {
                    if batch_idx == PICKED_ROW {
                        picked_rows[row_idx].columns[output_col_idx].unwrap_or((extra_idx, row_idx))
                    } else {
                        (batch_idx, row_idx)
                    }
                })
                .collect();
            interleave(&arrays, &column_indices).map_err(|e| Error::UnexpectedError {
                message: format!("Failed to interleave output column {output_col_idx}: {e}"),
                source: Some(Box::new(e)),
            })
        })
        .collect::<crate::Result<Vec<_>>>()?;

    RecordBatch::try_new(schema.clone(), columns).map_err(|e| Error::UnexpectedError {
        message: format!("Failed to build interleaved RecordBatch: {e}"),
        source: Some(Box::new(e)),
    })
}

/// Build an output RecordBatch using `interleave` to gather rows from the
/// batch buffer in one pass per column.
fn build_output_interleave(
    schema: &SchemaRef,
    batch_buffer: &[BufferedBatch],
    source_output_col_indices: &[usize],
    indices: &[(usize, usize)],
) -> crate::Result<RecordBatch> {
    let columns: Vec<ArrayRef> = (0..schema.fields().len())
        .map(|output_col_idx| {
            let arrays: Vec<&dyn arrow_array::Array> = batch_buffer
                .iter()
                .map(|batch| batch.column_for_output(output_col_idx, source_output_col_indices))
                .collect();
            interleave(&arrays, indices).map_err(|e| Error::UnexpectedError {
                message: format!("Failed to interleave output column {output_col_idx}: {e}"),
                source: Some(Box::new(e)),
            })
        })
        .collect::<crate::Result<Vec<_>>>()?;

    RecordBatch::try_new(schema.clone(), columns).map_err(|e| Error::UnexpectedError {
        message: format!("Failed to build interleaved RecordBatch: {e}"),
        source: Some(Box::new(e)),
    })
}

/// Compact the batch buffer by removing batches no longer referenced by any
/// cursor, and updating indices accordingly.
/// Returns whether any batch was released.
fn compact_batch_buffer(
    batch_buffer: &mut Vec<BufferedBatch>,
    stream_batch_idx: &mut [Option<usize>],
    cursors: &[Option<SortMergeCursor>],
) -> bool {
    // Collect which buffer indices are still alive (referenced by a cursor).
    let mut alive: Vec<bool> = vec![false; batch_buffer.len()];
    for (i, cursor) in cursors.iter().enumerate() {
        if cursor.is_some() {
            if let Some(idx) = stream_batch_idx[i] {
                alive[idx] = true;
            }
        }
    }

    // Build old->new index mapping.
    let mut new_indices: Vec<Option<usize>> = vec![None; batch_buffer.len()];
    let mut new_buffer: Vec<BufferedBatch> = Vec::new();
    for (old_idx, is_alive) in alive.iter().enumerate() {
        if *is_alive {
            new_indices[old_idx] = Some(new_buffer.len());
            new_buffer.push(batch_buffer[old_idx].clone());
        }
    }

    let released = new_buffer.len() < batch_buffer.len();
    *batch_buffer = new_buffer;

    // Remap stream_batch_idx.
    for (i, cursor) in cursors.iter().enumerate() {
        if cursor.is_some() {
            if let Some(old_idx) = stream_batch_idx[i] {
                stream_batch_idx[i] = new_indices[old_idx];
            }
        } else {
            stream_batch_idx[i] = None;
        }
    }
    released
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_array::{Array, BinaryArray, Int32Array, Int64Array, Int8Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use futures::TryStreamExt;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn level_column_survives_concatenation_and_serialization() {
        let input = RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
        )])
        .unwrap();
        let inputs = [
            with_level(input.clone(), 0).unwrap(),
            with_level(input, 3).unwrap(),
        ];
        let schema = inputs[0].schema();
        let field = schema.fields().last().unwrap();
        assert_eq!(field.name(), "_LEVEL");
        assert_eq!(field.data_type(), &DataType::Int32);
        assert!(!field.is_nullable());
        assert_eq!(
            field.metadata()["PARQUET:field_id"],
            (i32::MAX - 3).to_string()
        );
        let batch = arrow_select::concat::concat_batches(&schema, &inputs).unwrap();
        let mut bytes = Vec::new();
        {
            let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut bytes, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        let restored = arrow_ipc::reader::StreamReader::try_new(bytes.as_slice(), None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(restored.schema(), schema);
        let batches = vec![BufferedBatch::Source(restored)];
        let levels = (0..4)
            .map(|row_idx| {
                MergeRow {
                    batch_idx: 0,
                    row_idx,
                    sequence_number: row_idx as i64,
                    value_kind: 0,
                    user_sequence: None,
                }
                .level(&batches)
                .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(levels, vec![0, 0, 3, 3]);
    }

    #[test]
    fn lookup_uses_each_rows_level_in_a_mixed_batch() {
        let batch = RecordBatch::try_from_iter([(
            "_LEVEL",
            Arc::new(Int32Array::from(vec![-1, 0, 5, 2])) as ArrayRef,
        )])
        .unwrap();
        let batches = vec![BufferedBatch::Source(batch)];
        let rows = (0..4)
            .map(|row_idx| MergeRow {
                batch_idx: 0,
                row_idx,
                sequence_number: row_idx as i64,
                value_kind: 0,
                user_sequence: None,
            })
            .collect::<Vec<_>>();
        let (base, has_l0, selected) = lookup_rows(&rows, &batches).unwrap();
        assert_eq!(base.unwrap().row_idx, 3);
        assert!(has_l0);
        assert_eq!(
            selected.iter().map(|row| row.row_idx).collect::<Vec<_>>(),
            vec![0, 1, 3]
        );
    }

    #[test]
    fn first_row_merge_keeps_earliest_sequence_and_first_tie() {
        let rows: Vec<_> = [30, 10, 10, 20]
            .into_iter()
            .enumerate()
            .map(|(row_idx, sequence_number)| MergeRow {
                batch_idx: 0,
                row_idx,
                sequence_number,
                value_kind: 0,
                user_sequence: None,
            })
            .collect();
        let result = FirstRowMergeFunction {
            ignore_delete: false,
        }
        .merge(&rows, &[], &[], &make_output_schema())
        .unwrap();
        assert!(matches!(
            result,
            MergeResult::SourceRow {
                batch_idx: 0,
                row_idx: 1
            }
        ));
    }

    #[test]
    fn first_row_merge_validates_every_retract_and_honors_ignore_delete() {
        for kind in [1, 3] {
            let mut rows = vec![
                MergeRow {
                    batch_idx: 0,
                    row_idx: 0,
                    sequence_number: 1,
                    value_kind: 0,
                    user_sequence: None,
                },
                MergeRow {
                    batch_idx: 0,
                    row_idx: 1,
                    sequence_number: 2,
                    value_kind: kind,
                    user_sequence: None,
                },
            ];
            let merge = FirstRowMergeFunction {
                ignore_delete: false,
            };
            assert!(matches!(
                merge.merge(&rows, &[], &[], &make_output_schema()),
                Err(Error::Unsupported { .. })
            ));
            let merge = FirstRowMergeFunction {
                ignore_delete: true,
            };
            assert!(matches!(
                merge.merge(&rows, &[], &[], &make_output_schema()).unwrap(),
                MergeResult::SourceRow { row_idx: 0, .. }
            ));
            rows.remove(0);
            assert!(matches!(
                merge.merge(&rows, &[], &[], &make_output_schema()).unwrap(),
                MergeResult::Omit
            ));
        }
    }

    fn make_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("_SEQUENCE_NUMBER", DataType::Int64, false),
            Field::new("_VALUE_KIND", DataType::Int8, false),
            Field::new("value", DataType::Utf8, true),
        ]))
    }

    fn make_output_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("value", DataType::Utf8, true),
        ]))
    }

    fn make_batch(
        schema: &SchemaRef,
        pks: Vec<i32>,
        seqs: Vec<i64>,
        values: Vec<Option<&str>>,
    ) -> RecordBatch {
        let len = pks.len();
        make_batch_with_kind(schema, pks, seqs, vec![0i8; len], values)
    }

    fn make_batch_with_kind(
        schema: &SchemaRef,
        pks: Vec<i32>,
        seqs: Vec<i64>,
        kinds: Vec<i8>,
        values: Vec<Option<&str>>,
    ) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(pks)),
                Arc::new(Int64Array::from(seqs)),
                Arc::new(Int8Array::from(kinds)),
                Arc::new(StringArray::from(values)),
            ],
        )
        .unwrap()
    }

    fn stream_from_batches(batches: Vec<RecordBatch>) -> ArrowRecordBatchStream {
        futures::stream::iter(batches.into_iter().map(Ok)).boxed()
    }

    struct MaterializingMergeFunction;

    impl MergeFunction for MaterializingMergeFunction {
        fn merge(
            &self,
            rows: &[MergeRow],
            batch_buffer: &[BufferedBatch],
            source_output_col_indices: &[usize],
            output_schema: &SchemaRef,
        ) -> crate::Result<MergeResult> {
            let first = rows.first().expect("merge called with empty rows");
            let source_batch = first.source_batch(batch_buffer)?;
            let pk = source_batch
                .column(source_output_col_indices[0])
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("pk column must be Int32")
                .value(first.row_idx);

            let batch = RecordBatch::try_new(
                output_schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![pk])) as ArrayRef,
                    Arc::new(StringArray::from(vec![Some("merged")])) as ArrayRef,
                ],
            )
            .map_err(|e| Error::UnexpectedError {
                message: format!("Failed to build materialized merge batch: {e}"),
                source: Some(Box::new(e)),
            })?;

            Ok(MergeResult::MaterializedRow(batch))
        }
    }

    #[tokio::test]
    async fn test_loser_tree_basic() {
        // 3 streams, verify init produces correct winner
        let schema = make_schema();
        let s0 = stream_from_batches(vec![make_batch(
            &schema,
            vec![1, 3],
            vec![1, 1],
            vec![Some("a"), Some("c")],
        )]);
        let s1 = stream_from_batches(vec![make_batch(
            &schema,
            vec![2, 4],
            vec![1, 1],
            vec![Some("b"), Some("d")],
        )]);
        let s2 = stream_from_batches(vec![make_batch(&schema, vec![5], vec![1], vec![Some("e")])]);

        let output_schema = make_output_schema();
        let result = SortMergeReaderBuilder::new(
            vec![s0, s1, s2],
            schema,
            vec![0], // key: pk
            1,       // seq index
            2,       // value_kind index
            vec![],  // no user sequence fields
            vec![3], // value index
            output_schema,
            Box::new(DeduplicateMergeFunction),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let pks: Vec<i32> = result
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(pks, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn test_deduplicate_merge() {
        // Two streams with overlapping keys, different sequence numbers
        let schema = make_schema();
        let s0 = stream_from_batches(vec![make_batch(
            &schema,
            vec![1, 2, 3],
            vec![1, 1, 1],
            vec![Some("old_a"), Some("old_b"), Some("old_c")],
        )]);
        let s1 = stream_from_batches(vec![make_batch(
            &schema,
            vec![1, 2, 4],
            vec![2, 2, 2],
            vec![Some("new_a"), Some("new_b"), Some("new_d")],
        )]);

        let output_schema = make_output_schema();
        let result = SortMergeReaderBuilder::new(
            vec![s0, s1],
            schema,
            vec![0],
            1,
            2,       // value_kind index
            vec![],  // no user sequence fields
            vec![3], // value index
            output_schema,
            Box::new(DeduplicateMergeFunction),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let pks: Vec<i32> = result
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        let values: Vec<String> = result
            .iter()
            .flat_map(|b| {
                let arr = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
                (0..arr.len())
                    .map(|i| arr.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();

        assert_eq!(pks, vec![1, 2, 3, 4]);
        // key 1,2: newer seq wins; key 3: only in s0; key 4: only in s1
        assert_eq!(values, vec!["new_a", "new_b", "old_c", "new_d"]);
    }

    #[tokio::test]
    async fn test_empty_streams() {
        let schema = make_schema();
        let output_schema = make_output_schema();
        let result = SortMergeReaderBuilder::new(
            vec![],
            schema,
            vec![0],
            1,
            2,       // value_kind index
            vec![],  // no user sequence fields
            vec![3], // value index
            output_schema,
            Box::new(DeduplicateMergeFunction),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_single_stream_no_duplicates() {
        let schema = make_schema();
        let s0 = stream_from_batches(vec![make_batch(
            &schema,
            vec![1, 2, 3],
            vec![1, 1, 1],
            vec![Some("a"), Some("b"), Some("c")],
        )]);

        let output_schema = make_output_schema();
        let result = SortMergeReaderBuilder::new(
            vec![s0],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3],
            output_schema,
            Box::new(DeduplicateMergeFunction),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let pks: Vec<i32> = result
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(pks, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_multi_batch_per_stream() {
        let schema = make_schema();
        // Stream 0: two batches
        let s0 = stream_from_batches(vec![
            make_batch(&schema, vec![1, 3], vec![1, 1], vec![Some("a"), Some("c")]),
            make_batch(&schema, vec![5, 7], vec![1, 1], vec![Some("e"), Some("g")]),
        ]);
        // Stream 1: two batches
        let s1 = stream_from_batches(vec![
            make_batch(&schema, vec![2, 4], vec![1, 1], vec![Some("b"), Some("d")]),
            make_batch(&schema, vec![6], vec![1], vec![Some("f")]),
        ]);

        let output_schema = make_output_schema();
        let result = SortMergeReaderBuilder::new(
            vec![s0, s1],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3],
            output_schema,
            Box::new(DeduplicateMergeFunction),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let pks: Vec<i32> = result
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(pks, vec![1, 2, 3, 4, 5, 6, 7]);
    }

    #[tokio::test]
    async fn test_batch_size_boundary() {
        let schema = make_schema();
        let s0 = stream_from_batches(vec![make_batch(
            &schema,
            vec![1, 2, 3, 4, 5],
            vec![1, 1, 1, 1, 1],
            vec![Some("a"), Some("b"), Some("c"), Some("d"), Some("e")],
        )]);

        let output_schema = make_output_schema();
        let result = SortMergeReaderBuilder::new(
            vec![s0],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3],
            output_schema,
            Box::new(DeduplicateMergeFunction),
        )
        .with_batch_size(2)
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        // Should produce 3 batches: [2, 2, 1] rows
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].num_rows(), 2);
        assert_eq!(result[1].num_rows(), 2);
        assert_eq!(result[2].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_multi_sequence_fields() {
        // Schema: pk, _SEQUENCE_NUMBER, _VALUE_KIND, seq1, seq2, value
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("_SEQUENCE_NUMBER", DataType::Int64, false),
            Field::new("_VALUE_KIND", DataType::Int8, false),
            Field::new("seq1", DataType::Int64, false),
            Field::new("seq2", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
        ]));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("value", DataType::Utf8, true),
        ]));

        // pk=1: s0 has (seq1=10, seq2=1), s1 has (seq1=10, seq2=2) → s1 wins (second field higher)
        // pk=2: s0 has (seq1=20, seq2=1), s1 has (seq1=10, seq2=99) → s0 wins (first field higher)
        let s0 = stream_from_batches(vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![1, 1])),
                Arc::new(Int8Array::from(vec![0, 0])),
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(Int64Array::from(vec![1, 1])),
                Arc::new(StringArray::from(vec!["old_a", "winner_b"])),
            ],
        )
        .unwrap()]);
        let s1 = stream_from_batches(vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![2, 2])),
                Arc::new(Int8Array::from(vec![0, 0])),
                Arc::new(Int64Array::from(vec![10, 10])),
                Arc::new(Int64Array::from(vec![2, 99])),
                Arc::new(StringArray::from(vec!["winner_a", "loser_b"])),
            ],
        )
        .unwrap()]);

        let result = SortMergeReaderBuilder::new(
            vec![s0, s1],
            schema,
            vec![0],    // key: pk
            1,          // seq index
            2,          // value_kind index
            vec![3, 4], // user sequence fields: seq1, seq2
            vec![5],    // value index
            output_schema,
            Box::new(DeduplicateMergeFunction),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let values: Vec<String> = result
            .iter()
            .flat_map(|b| {
                let arr = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
                (0..arr.len())
                    .map(|i| arr.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(values, vec!["winner_a", "winner_b"]);
    }

    #[tokio::test]
    async fn test_descending_user_sequence_keeps_nulls_first() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("_SEQUENCE_NUMBER", DataType::Int64, false),
            Field::new("_VALUE_KIND", DataType::Int8, false),
            Field::new("ts", DataType::Int64, true),
            Field::new("value", DataType::Utf8, false),
        ]));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 1])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int8Array::from(vec![0, 0, 0])),
                Arc::new(Int64Array::from(vec![Some(100), Some(50), None])),
                Arc::new(StringArray::from(vec!["high", "low", "null"])),
            ],
        )
        .unwrap();

        let result = SortMergeReaderBuilder::new(
            vec![stream_from_batches(vec![batch])],
            schema,
            vec![0],
            1,
            2,
            vec![3],
            vec![4],
            output_schema,
            Box::new(DeduplicateMergeFunction),
        )
        .with_user_sequence_descending(true)
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        assert_eq!(result.len(), 1);
        let values = result[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "low");
    }

    #[tokio::test]
    async fn test_string_user_sequence_across_streams_in_both_orders() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("_SEQUENCE_NUMBER", DataType::Int64, false),
            Field::new("_VALUE_KIND", DataType::Int8, false),
            Field::new("seq", DataType::Utf8, true),
            Field::new("value", DataType::Utf8, false),
        ]));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("seq", DataType::Utf8, true),
            Field::new("value", DataType::Utf8, false),
        ]));
        for (descending, expected) in [(false, "high"), (true, "low")] {
            let streams = [
                (Some("z"), 1, "high"),
                (Some("a"), 2, "low"),
                (None, 3, "null"),
            ]
            .into_iter()
            .map(|(seq, number, value)| {
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int32Array::from(vec![1])),
                        Arc::new(Int64Array::from(vec![number])),
                        Arc::new(Int8Array::from(vec![0])),
                        Arc::new(StringArray::from(vec![seq])),
                        Arc::new(StringArray::from(vec![value])),
                    ],
                )
                .unwrap();
                stream_from_batches(vec![batch])
            })
            .collect();
            let batches = SortMergeReaderBuilder::new(
                streams,
                schema.clone(),
                vec![0],
                1,
                2,
                vec![3],
                vec![3, 4],
                output_schema.clone(),
                Box::new(DeduplicateMergeFunction),
            )
            .with_user_sequence_descending(descending)
            .build()
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
            assert_eq!(batches.len(), 1);
            let values = batches[0]
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(values.value(0), expected, "descending={descending}");
        }
    }

    #[tokio::test]
    async fn test_equal_sequence_places_retract_before_add() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("_SEQUENCE_NUMBER", DataType::Int64, false),
            Field::new("_VALUE_KIND", DataType::Int8, false),
            Field::new("seq", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("seq", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        for kinds in [[3, 0], [0, 3]] {
            let streams = kinds
                .into_iter()
                .map(|kind| {
                    let batch = RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            Arc::new(Int32Array::from(vec![1])),
                            Arc::new(Int64Array::from(vec![5])),
                            Arc::new(Int8Array::from(vec![kind])),
                            Arc::new(StringArray::from(vec!["same"])),
                            Arc::new(StringArray::from(vec![if kind == 0 {
                                "add"
                            } else {
                                "delete"
                            }])),
                        ],
                    )
                    .unwrap();
                    stream_from_batches(vec![batch])
                })
                .collect();
            let batches = SortMergeReaderBuilder::new(
                streams,
                schema.clone(),
                vec![0],
                1,
                2,
                vec![3],
                vec![3, 4],
                output_schema.clone(),
                Box::new(DeduplicateMergeFunction),
            )
            .build()
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
            assert_eq!(batches.len(), 1);
            let values = batches[0]
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(values.value(0), "add");
        }
    }

    #[tokio::test]
    async fn test_delete_row_filtered() {
        let schema = make_schema();
        // Stream 0: pk=1 INSERT (seq=1), pk=2 INSERT (seq=1)
        let s0 = stream_from_batches(vec![make_batch_with_kind(
            &schema,
            vec![1, 2],
            vec![1, 1],
            vec![0, 0],
            vec![Some("a"), Some("b")],
        )]);
        // Stream 1: pk=1 DELETE (seq=2) — should win and be filtered out
        let s1 = stream_from_batches(vec![make_batch_with_kind(
            &schema,
            vec![1],
            vec![2],
            vec![3], // DELETE
            vec![Some("a")],
        )]);

        let output_schema = make_output_schema();
        let result = SortMergeReaderBuilder::new(
            vec![s0, s1],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3],
            output_schema,
            Box::new(DeduplicateMergeFunction),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let pks: Vec<i32> = result
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        // pk=1 deleted, only pk=2 remains
        assert_eq!(pks, vec![2]);
    }

    #[tokio::test]
    async fn test_single_stream_duplicate_keys() {
        let schema = make_schema();
        // Single stream with duplicate pk=1 (seq 1 and 2), unique pk=2
        let s0 = stream_from_batches(vec![make_batch(
            &schema,
            vec![1, 1, 2],
            vec![1, 2, 1],
            vec![Some("old"), Some("new"), Some("only")],
        )]);

        let output_schema = make_output_schema();
        let result = SortMergeReaderBuilder::new(
            vec![s0],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3],
            output_schema,
            Box::new(DeduplicateMergeFunction),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let pks: Vec<i32> = result
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        let values: Vec<String> = result
            .iter()
            .flat_map(|b| {
                let arr = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
                (0..arr.len())
                    .map(|i| arr.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();

        assert_eq!(pks, vec![1, 2]);
        assert_eq!(values, vec!["new", "only"]);
    }

    #[tokio::test]
    async fn test_single_row_per_stream() {
        let schema = make_schema();
        let s0 = stream_from_batches(vec![make_batch(&schema, vec![3], vec![1], vec![Some("c")])]);
        let s1 = stream_from_batches(vec![make_batch(&schema, vec![1], vec![1], vec![Some("a")])]);
        let s2 = stream_from_batches(vec![make_batch(&schema, vec![2], vec![1], vec![Some("b")])]);

        let output_schema = make_output_schema();
        let result = SortMergeReaderBuilder::new(
            vec![s0, s1, s2],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3],
            output_schema,
            Box::new(DeduplicateMergeFunction),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let pks: Vec<i32> = result
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        let values: Vec<String> = result
            .iter()
            .flat_map(|b| {
                let arr = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
                (0..arr.len())
                    .map(|i| arr.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();

        assert_eq!(pks, vec![1, 2, 3]);
        assert_eq!(values, vec!["a", "b", "c"]);
    }

    /// Helper to create an empty batch with the test schema.
    fn make_empty_batch(schema: &SchemaRef) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(Vec::<i32>::new())),
                Arc::new(Int64Array::from(Vec::<i64>::new())),
                Arc::new(Int8Array::from(Vec::<i8>::new())),
                Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_empty_batches_skipped() {
        // Regression: empty batches (e.g. from predicate filtering) must be
        // skipped, not treated as stream exhaustion.
        let schema = make_schema();

        // Stream 0: empty batch at start, then real data
        let s0 = stream_from_batches(vec![
            make_empty_batch(&schema),
            make_batch(&schema, vec![1, 3], vec![1, 1], vec![Some("a"), Some("c")]),
        ]);
        // Stream 1: data, empty batch in the middle, then more data
        let s1 = stream_from_batches(vec![
            make_batch(&schema, vec![2], vec![1], vec![Some("b")]),
            make_empty_batch(&schema),
            make_empty_batch(&schema),
            make_batch(&schema, vec![4], vec![1], vec![Some("d")]),
        ]);

        let output_schema = make_output_schema();
        let result = SortMergeReaderBuilder::new(
            vec![s0, s1],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3],
            output_schema,
            Box::new(DeduplicateMergeFunction),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let pks: Vec<i32> = result
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        let values: Vec<String> = result
            .iter()
            .flat_map(|b| {
                let arr = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
                (0..arr.len())
                    .map(|i| arr.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();

        assert_eq!(pks, vec![1, 2, 3, 4]);
        assert_eq!(values, vec!["a", "b", "c", "d"]);
    }

    #[tokio::test]
    async fn test_materialized_merge_result_path() {
        let schema = make_schema();
        let s0 = stream_from_batches(vec![make_batch(
            &schema,
            vec![1, 2],
            vec![1, 1],
            vec![Some("old_a"), Some("old_b")],
        )]);
        let s1 = stream_from_batches(vec![make_batch(
            &schema,
            vec![1, 3],
            vec![2, 1],
            vec![Some("new_a"), Some("c")],
        )]);

        let result = SortMergeReaderBuilder::new(
            vec![s0, s1],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3],
            make_output_schema(),
            Box::new(MaterializingMergeFunction),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let pks: Vec<i32> = result
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        let values: Vec<String> = result
            .iter()
            .flat_map(|b| {
                let arr = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
                (0..arr.len())
                    .map(|i| arr.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();

        assert_eq!(pks, vec![1, 2, 3]);
        assert_eq!(values, vec!["merged", "merged", "merged"]);
    }

    /// Picked partial-update rows read the same as materialized ones, across output
    /// batches and next to lone versions, including NULL columns and KV metadata.
    #[tokio::test]
    async fn test_partial_update_picked_rows_match_materialized_rows() {
        struct Materializing(PartialUpdateMergeFunction);
        impl MergeFunction for Materializing {
            fn merge(
                &self,
                rows: &[MergeRow],
                batch_buffer: &[BufferedBatch],
                source_output_col_indices: &[usize],
                output_schema: &SchemaRef,
            ) -> crate::Result<MergeResult> {
                self.0
                    .merge(rows, batch_buffer, source_output_col_indices, output_schema)
            }
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("_SEQUENCE_NUMBER", DataType::Int64, false),
            Field::new("_VALUE_KIND", DataType::Int8, false),
            Field::new("v_int", DataType::Int32, true),
            Field::new("v_str", DataType::Utf8, true),
        ]));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("_SEQUENCE_NUMBER", DataType::Int64, false),
            Field::new("_VALUE_KIND", DataType::Int8, false),
            Field::new("v_int", DataType::Int32, true),
            Field::new("v_str", DataType::Utf8, true),
        ]));
        let streams = || {
            let batch =
                |pk: Vec<i32>, seq: Vec<i64>, v_int: Vec<Option<i32>>, v_str: Vec<Option<&str>>| {
                    let kinds = vec![0i8; pk.len()];
                    stream_from_batches(vec![RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            Arc::new(Int32Array::from(pk)),
                            Arc::new(Int64Array::from(seq)),
                            Arc::new(Int8Array::from(kinds)),
                            Arc::new(Int32Array::from(v_int)),
                            Arc::new(StringArray::from(v_str)),
                        ],
                    )
                    .unwrap()])
                };
            vec![
                batch(
                    vec![1, 2, 3, 4],
                    vec![1, 1, 1, 1],
                    vec![Some(10), Some(20), Some(30), None],
                    vec![Some("a1"), Some("a2"), None, Some("a4")],
                ),
                batch(
                    vec![1, 2, 4, 5],
                    vec![2, 2, 2, 1],
                    vec![None, Some(21), None, Some(50)],
                    vec![Some("b1"), None, None, None],
                ),
                batch(
                    vec![1, 3, 5, 6],
                    vec![3, 3, 2, 1],
                    vec![Some(12), None, None, Some(60)],
                    vec![None, Some("c3"), None, Some("c6")],
                ),
            ]
        };
        let read = |merge: Box<dyn MergeFunction>| {
            SortMergeReaderBuilder::new(
                streams(),
                schema.clone(),
                vec![0],
                1,
                2,
                vec![],
                vec![1, 2, 3, 4],
                output_schema.clone(),
                merge,
            )
            .with_batch_size(2)
            .build()
            .unwrap()
            .try_collect::<Vec<_>>()
        };
        let new_merge = || PartialUpdateMergeFunction::new(&HashMap::new(), "test_table").unwrap();
        let picked = read(Box::new(new_merge())).await.unwrap();
        let materialized = read(Box::new(Materializing(new_merge()))).await.unwrap();
        assert_eq!(picked, materialized);

        let picked = arrow_select::concat::concat_batches(&output_schema, &picked).unwrap();
        assert_eq!(
            picked.column(1).as_ref(),
            &Int64Array::from(vec![3, 2, 3, 2, 2, 1]) as &dyn arrow_array::Array
        );
        assert_eq!(
            picked.column(3).as_ref(),
            &Int32Array::from(vec![Some(12), Some(21), Some(30), None, Some(50), Some(60)])
                as &dyn arrow_array::Array
        );
        assert_eq!(
            picked.column(4).as_ref(),
            &StringArray::from(vec![
                Some("b1"),
                Some("a2"),
                Some("c3"),
                Some("a4"),
                None,
                Some("c6")
            ]) as &dyn arrow_array::Array
        );
    }

    /// Changelog from consecutive keys is handed on in key order, in batches of up
    /// to the batch size rather than one batch per key.
    #[tokio::test]
    async fn test_changelog_is_emitted_in_batches() {
        struct EveryKeyChangelog;
        impl MergeFunction for EveryKeyChangelog {
            fn merge(
                &self,
                rows: &[MergeRow],
                batch_buffer: &[BufferedBatch],
                source_output_col_indices: &[usize],
                output_schema: &SchemaRef,
            ) -> crate::Result<MergeResult> {
                DeduplicateMergeFunction.merge(
                    rows,
                    batch_buffer,
                    source_output_col_indices,
                    output_schema,
                )
            }

            fn merge_with_changelog(
                &self,
                rows: &[MergeRow],
                batches: &[BufferedBatch],
                columns: &[usize],
                schema: &SchemaRef,
            ) -> crate::Result<(MergeResult, Option<RecordBatch>)> {
                let last = rows.last().unwrap();
                let changelog = build_output_interleave(
                    schema,
                    batches,
                    columns,
                    &[(last.batch_idx, last.row_idx)],
                )?;
                Ok((self.merge(rows, batches, columns, schema)?, Some(changelog)))
            }
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("_SEQUENCE_NUMBER", DataType::Int64, false),
            Field::new("_VALUE_KIND", DataType::Int8, false),
            Field::new("value", DataType::Int32, true),
        ]));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("value", DataType::Int32, true),
        ]));
        let stream = |pk: Vec<i32>, seq: i64| {
            let n = pk.len();
            stream_from_batches(vec![RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(pk.clone())),
                    Arc::new(Int64Array::from(vec![seq; n])),
                    Arc::new(Int8Array::from(vec![0; n])),
                    Arc::new(Int32Array::from_iter_values(pk.iter().map(|k| k * 10))),
                ],
            )
            .unwrap()])
        };
        let batches = SortMergeReaderBuilder::new(
            vec![stream(vec![1, 2, 4], 1), stream(vec![2, 3, 5], 2)],
            schema.clone(),
            vec![0],
            1,
            2,
            vec![],
            vec![3],
            output_schema.clone(),
            Box::new(EveryKeyChangelog),
        )
        .with_batch_size(2)
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let pks = |batch: &RecordBatch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        };
        let (changelog, data): (Vec<_>, Vec<_>) = batches.iter().partition(|batch| {
            batch
                .schema()
                .metadata()
                .contains_key(CHANGELOG_BATCH_METADATA_KEY)
        });
        assert_eq!(
            changelog.iter().map(|batch| pks(batch)).collect::<Vec<_>>(),
            vec![vec![1, 2], vec![3, 4], vec![5]]
        );
        assert_eq!(
            data.iter().flat_map(|batch| pks(batch)).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
    }

    #[test]
    fn test_detached_nested_changelog_rows_own_their_entries() {
        use arrow_array::builder::{Int32Builder, MapBuilder};
        let mut maps = MapBuilder::new(None, Int32Builder::new(), Int32Builder::new());
        for (row, len) in [3, 100_000, 2].into_iter().enumerate() {
            for entry in 0..len {
                maps.keys().append_value(entry);
                maps.values().append_value(row as i32);
            }
            maps.append(true).unwrap();
        }
        let maps = Arc::new(maps.finish()) as ArrayRef;
        let batch = RecordBatch::try_from_iter([
            ("pk", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
            ("value", maps.clone()),
        ])
        .unwrap();
        let source = maps.as_map().entries().column(0).to_data().buffers()[0].clone();

        let mut chunks = Vec::new();
        let mut pending = vec![batch.slice(1, 1), batch.slice(2, 1)];
        detach_changelog(&mut chunks, &mut pending).unwrap();
        drop(batch);

        assert!(pending.is_empty());
        let chunk = &chunks[0];
        assert_eq!(chunk.num_rows(), 2);
        let copied = chunk.column(1).as_map();
        assert_eq!(copied.value_length(0), 100_000);
        assert_eq!(copied.value_length(1), 2);
        assert_eq!(
            copied
                .value(1)
                .column(0)
                .as_primitive::<arrow_array::types::Int32Type>()
                .values(),
            &[0, 1]
        );
        // Only `maps` and `source` hold the keys, not the copied chunk.
        assert_eq!(source.strong_count(), 2);
    }

    #[test]
    fn test_only_element_indexed_columns_are_copied_by_range() {
        let item = |data_type| Arc::new(Field::new("item", data_type, true));
        let map = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Int32, false),
                        Field::new("value", DataType::Int32, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        assert!(indexes_each_element(&map));
        assert!(indexes_each_element(&DataType::List(item(DataType::Utf8))));
        assert!(indexes_each_element(&DataType::Struct(
            vec![Field::new("m", map, true)].into()
        )));
        assert!(!indexes_each_element(&DataType::List(item(
            DataType::Int64
        ))));
        assert!(!indexes_each_element(&DataType::Struct(
            vec![Field::new("a", DataType::Int32, true)].into()
        )));
        assert!(!indexes_each_element(&DataType::Utf8));
    }

    /// A changelog row sliced from a source batch must not keep that batch
    /// alive after the reader releases it, nor be handed on still sharing it.
    #[tokio::test]
    async fn test_changelog_rows_do_not_keep_released_source_batches() {
        // Emits the first row of every input batch as a zero-copy slice.
        struct SlicedChangelog;
        impl MergeFunction for SlicedChangelog {
            fn merge(
                &self,
                rows: &[MergeRow],
                batch_buffer: &[BufferedBatch],
                source_output_col_indices: &[usize],
                output_schema: &SchemaRef,
            ) -> crate::Result<MergeResult> {
                DeduplicateMergeFunction.merge(
                    rows,
                    batch_buffer,
                    source_output_col_indices,
                    output_schema,
                )
            }

            fn merge_with_changelog(
                &self,
                rows: &[MergeRow],
                batches: &[BufferedBatch],
                columns: &[usize],
                schema: &SchemaRef,
            ) -> crate::Result<(MergeResult, Option<RecordBatch>)> {
                let last = rows.last().unwrap();
                let changelog = (last.row_idx == 0).then(|| {
                    let values = (0..schema.fields().len())
                        .map(|index| {
                            batches[last.batch_idx]
                                .column_for_output(index, columns)
                                .slice(last.row_idx, 1)
                        })
                        .collect();
                    RecordBatch::try_new(schema.clone(), values).unwrap()
                });
                Ok((self.merge(rows, batches, columns, schema)?, changelog))
            }
        }

        const ROWS: usize = 8;
        const VALUE: usize = 4096;
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("_SEQUENCE_NUMBER", DataType::Int64, false),
            Field::new("_VALUE_KIND", DataType::Int8, false),
            Field::new("value", DataType::Binary, false),
        ]));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("value", DataType::Binary, false),
        ]));
        let batch = |pks: Vec<i32>| {
            let n = pks.len();
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(pks)),
                    Arc::new(Int64Array::from(vec![1; n])),
                    Arc::new(Int8Array::from(vec![0; n])),
                    Arc::new(BinaryArray::from_iter_values(std::iter::repeat_n(
                        [7u8; VALUE],
                        n,
                    ))),
                ],
            )
            .unwrap()
        };
        // 17 input batches: the last changelog batch holds a lone row.
        let inputs = (0..17)
            .map(|b| batch((b * ROWS as i32..(b + 1) * ROWS as i32).collect()))
            .collect::<Vec<_>>();
        let payloads = inputs
            .iter()
            .map(|batch| batch.column(3).as_binary::<i32>().values().clone())
            .collect::<Vec<_>>();
        let mut stream = SortMergeReaderBuilder::new(
            vec![
                stream_from_batches(inputs),
                stream_from_batches(vec![batch(vec![1000])]),
            ],
            schema.clone(),
            vec![0],
            1,
            2,
            vec![],
            vec![3],
            output_schema.clone(),
            Box::new(SlicedChangelog),
        )
        .with_batch_size(ROWS)
        .build()
        .unwrap();

        let mut data_batches = 0usize;
        let mut stale = 0;
        let mut changelog_pks = Vec::new();
        while let Some(batch) = stream.try_next().await.unwrap() {
            if batch
                .schema()
                .metadata()
                .contains_key(CHANGELOG_BATCH_METADATA_KEY)
            {
                assert!(
                    batch.get_array_memory_size() < (batch.num_rows() + 1) * VALUE,
                    "a {}-row changelog batch holds {} bytes",
                    batch.num_rows(),
                    batch.get_array_memory_size()
                );
                let pks = batch
                    .column(0)
                    .as_primitive::<arrow_array::types::Int32Type>();
                changelog_pks.extend(pks.values().iter().copied());
            } else {
                // The input batches before the previous one are fully merged.
                data_batches += 1;
                let merged = &payloads[..data_batches.saturating_sub(1).min(payloads.len())];
                stale = stale.max(merged.iter().filter(|p| p.strong_count() > 1).count());
            }
        }
        assert_eq!(
            changelog_pks,
            (0..17)
                .map(|b| b * ROWS as i32)
                .chain([1000])
                .collect::<Vec<_>>()
        );
        assert_eq!(stale, 0, "merged source batches were kept alive");
    }

    #[tokio::test]
    async fn test_partial_update_merge_keeps_latest_non_null_values() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("_SEQUENCE_NUMBER", DataType::Int64, false),
            Field::new("_VALUE_KIND", DataType::Int8, false),
            Field::new("v_int", DataType::Int32, true),
            Field::new("v_str", DataType::Utf8, true),
        ]));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("v_int", DataType::Int32, true),
            Field::new("v_str", DataType::Utf8, true),
        ]));

        let s0 = stream_from_batches(vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![1, 1])),
                Arc::new(Int8Array::from(vec![0, 0])),
                Arc::new(Int32Array::from(vec![10, 20])),
                Arc::new(StringArray::from(vec![Some("old-1"), Some("old-2")])),
            ],
        )
        .unwrap()]);
        let s1 = stream_from_batches(vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![2, 2, 1])),
                Arc::new(Int8Array::from(vec![0, 0, 0])),
                Arc::new(Int32Array::from(vec![None, Some(200), Some(30)])),
                Arc::new(StringArray::from(vec![Some("new-1"), None, None])),
            ],
        )
        .unwrap()]);

        let result = SortMergeReaderBuilder::new(
            vec![s0, s1],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3, 4],
            output_schema,
            Box::new(PartialUpdateMergeFunction::new(&HashMap::new(), "test_table").unwrap()),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let mut rows: Vec<(i32, Option<i32>, Option<String>)> = Vec::new();
        for batch in &result {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let ints = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let strs = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                rows.push((
                    ids.value(i),
                    if ints.is_null(i) {
                        None
                    } else {
                        Some(ints.value(i))
                    },
                    if strs.is_null(i) {
                        None
                    } else {
                        Some(strs.value(i).to_string())
                    },
                ));
            }
        }
        rows.sort_by_key(|row| row.0);

        assert_eq!(
            rows,
            vec![
                (1, Some(10), Some("new-1".to_string())),
                (2, Some(200), Some("old-2".to_string())),
                (3, Some(30), None),
            ]
        );
    }

    #[tokio::test]
    async fn test_partial_update_sequence_groups_advance_independently() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("_SEQUENCE_NUMBER", DataType::Int64, false),
            Field::new("_VALUE_KIND", DataType::Int8, false),
            Field::new("seq_a", DataType::Int32, true),
            Field::new("value_a", DataType::Int32, true),
            Field::new("seq_b", DataType::Int32, true),
            Field::new("value_b", DataType::Int32, true),
        ]));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("seq_a", DataType::Int32, true),
            Field::new("value_a", DataType::Int32, true),
            Field::new("seq_b", DataType::Int32, true),
            Field::new("value_b", DataType::Int32, true),
        ]));
        let output_fields = vec![
            DataField::new(0, "pk".into(), crate::spec::DataType::Int(IntType::new())),
            DataField::new(
                1,
                "seq_a".into(),
                crate::spec::DataType::Int(IntType::new()),
            ),
            DataField::new(
                2,
                "value_a".into(),
                crate::spec::DataType::Int(IntType::new()),
            ),
            DataField::new(
                3,
                "seq_b".into(),
                crate::spec::DataType::Int(IntType::new()),
            ),
            DataField::new(
                4,
                "value_b".into(),
                crate::spec::DataType::Int(IntType::new()),
            ),
        ];
        let options = HashMap::from([
            ("merge-engine".to_string(), "partial-update".to_string()),
            (
                "fields.seq_a.sequence-group".to_string(),
                "value_a".to_string(),
            ),
            (
                "fields.seq_b.sequence-group".to_string(),
                "value_b".to_string(),
            ),
        ]);
        let old = stream_from_batches(vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int8Array::from(vec![0])),
                Arc::new(Int32Array::from(vec![10])),
                Arc::new(Int32Array::from(vec![100])),
                Arc::new(Int32Array::from(vec![10])),
                Arc::new(Int32Array::from(vec![1000])),
            ],
        )
        .unwrap()]);
        let mixed = stream_from_batches(vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(Int8Array::from(vec![0])),
                Arc::new(Int32Array::from(vec![9])),
                Arc::new(Int32Array::from(vec![200])),
                Arc::new(Int32Array::from(vec![11])),
                Arc::new(Int32Array::from(vec![2000])),
            ],
        )
        .unwrap()]);

        let result = SortMergeReaderBuilder::new(
            vec![old, mixed],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3, 4, 5, 6],
            output_schema,
            Box::new(
                PartialUpdateMergeFunction::new_with_schema(
                    &options,
                    "test_table",
                    &output_fields,
                    &output_fields,
                    &["pk".to_string()],
                )
                .unwrap(),
            ),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            10
        );
        assert_eq!(
            batch
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            100
        );
        assert_eq!(
            batch
                .column(3)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            11
        );
        assert_eq!(
            batch
                .column(4)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            2000
        );
    }

    #[tokio::test]
    async fn test_partial_update_descending_composite_sequence_group() {
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("major", DataType::Int32, true),
            Field::new("minor", DataType::Int32, true),
            Field::new("value", DataType::Int32, true),
        ]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("_SEQUENCE_NUMBER", DataType::Int64, false),
            Field::new("_VALUE_KIND", DataType::Int8, false),
            Field::new("major", DataType::Int32, true),
            Field::new("minor", DataType::Int32, true),
            Field::new("value", DataType::Int32, true),
        ]));
        let fields = ["pk", "major", "minor", "value"]
            .iter()
            .enumerate()
            .map(|(id, name)| {
                DataField::new(
                    id as i32,
                    (*name).into(),
                    PaimonDataType::Int(IntType::new()),
                )
            })
            .collect::<Vec<_>>();
        let options = HashMap::from([
            ("merge-engine".into(), "partial-update".into()),
            ("fields.major,minor.sequence-group".into(), "value".into()),
            (
                "fields.major,minor.sequence-group.sort-order".into(),
                "descending".into(),
            ),
        ]);
        // Each case contains two arrival-ordered rows and the winning tuple/value.
        let cases = [
            // Lexicographic order: each non-null component sorts descending.
            (
                [(Some(2), Some(0), Some(10)), (Some(1), Some(9), Some(20))],
                (Some(1), Some(9), Some(20)),
            ),
            (
                [(Some(1), Some(9), Some(10)), (Some(1), Some(2), Some(20))],
                (Some(1), Some(2), Some(20)),
            ),
            (
                [(Some(1), Some(2), Some(10)), (Some(1), Some(9), Some(20))],
                (Some(1), Some(2), Some(10)),
            ),
            // Non-null wins over null in either position, regardless of direction.
            (
                [(None, Some(0), Some(10)), (Some(1), None, Some(20))],
                (Some(1), None, Some(20)),
            ),
            (
                [(Some(1), None, Some(10)), (None, Some(0), Some(20))],
                (Some(1), None, Some(10)),
            ),
            (
                [(Some(1), None, Some(10)), (Some(1), Some(9), Some(20))],
                (Some(1), Some(9), Some(20)),
            ),
            (
                [(Some(1), Some(9), Some(10)), (Some(1), None, Some(20))],
                (Some(1), Some(9), Some(10)),
            ),
            // All-null tuples skip updates; ties can clear protected fields.
            (
                [(Some(1), Some(2), Some(10)), (None, None, Some(20))],
                (Some(1), Some(2), Some(10)),
            ),
            (
                [(Some(1), Some(2), Some(10)), (Some(1), Some(2), None)],
                (Some(1), Some(2), None),
            ),
        ];
        for (rows, expected) in cases {
            // Separate streams exercise read-time merging of separate files.
            let streams = rows
                .iter()
                .enumerate()
                .map(|(index, &(major, minor, value))| {
                    stream_from_batches(vec![RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            Arc::new(Int32Array::from(vec![1])),
                            Arc::new(Int64Array::from(vec![index as i64])),
                            Arc::new(Int8Array::from(vec![0])),
                            Arc::new(Int32Array::from(vec![major])),
                            Arc::new(Int32Array::from(vec![minor])),
                            Arc::new(Int32Array::from(vec![value])),
                        ],
                    )
                    .unwrap()])
                })
                .collect();
            let result = SortMergeReaderBuilder::new(
                streams,
                schema.clone(),
                vec![0],
                1,
                2,
                vec![],
                vec![3, 4, 5],
                output_schema.clone(),
                Box::new(
                    PartialUpdateMergeFunction::new_with_schema(
                        &options,
                        "t",
                        &fields,
                        &fields,
                        &["pk".into()],
                    )
                    .unwrap(),
                ),
            )
            .build()
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].num_rows(), 1);
            for (index, value) in [expected.0, expected.1, expected.2].into_iter().enumerate() {
                let actual = result[0]
                    .column(index + 1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                assert_eq!(
                    actual.iter().collect::<Vec<_>>(),
                    vec![value],
                    "rows: {rows:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_partial_update_composite_sequence_group_skips_empty_sequence() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("_SEQUENCE_NUMBER", DataType::Int64, false),
            Field::new("_VALUE_KIND", DataType::Int8, false),
            Field::new("seq_major", DataType::Int32, true),
            Field::new("seq_minor", DataType::Int32, true),
            Field::new("value", DataType::Int32, true),
        ]));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("seq_major", DataType::Int32, true),
            Field::new("seq_minor", DataType::Int32, true),
            Field::new("value", DataType::Int32, true),
        ]));
        let output_fields = vec![
            DataField::new(0, "pk".into(), crate::spec::DataType::Int(IntType::new())),
            DataField::new(
                1,
                "seq_major".into(),
                crate::spec::DataType::Int(IntType::new()),
            ),
            DataField::new(
                2,
                "seq_minor".into(),
                crate::spec::DataType::Int(IntType::new()),
            ),
            DataField::new(
                3,
                "value".into(),
                crate::spec::DataType::Int(IntType::new()),
            ),
        ];
        let options = HashMap::from([
            ("merge-engine".to_string(), "partial-update".to_string()),
            (
                "fields.seq_major,seq_minor.sequence-group".to_string(),
                "value".to_string(),
            ),
        ]);
        let stream = stream_from_batches(vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 1, 1, 1])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(Int8Array::from(vec![0, 0, 0, 0, 0])),
                Arc::new(Int32Array::from(vec![
                    Some(1),
                    Some(2),
                    None,
                    Some(2),
                    Some(3),
                ])),
                Arc::new(Int32Array::from(vec![
                    Some(2),
                    Some(1),
                    None,
                    None,
                    Some(0),
                ])),
                Arc::new(Int32Array::from(vec![
                    Some(100),
                    Some(200),
                    Some(300),
                    Some(400),
                    None,
                ])),
            ],
        )
        .unwrap()]);

        let result = SortMergeReaderBuilder::new(
            vec![stream],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3, 4, 5],
            output_schema,
            Box::new(
                PartialUpdateMergeFunction::new_with_schema(
                    &options,
                    "test_table",
                    &output_fields,
                    &output_fields,
                    &["pk".to_string()],
                )
                .unwrap(),
            ),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let batch = &result[0];
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            3
        );
        assert_eq!(
            batch
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            0
        );
        assert!(batch.column(3).is_null(0));
    }

    #[tokio::test]
    async fn test_partial_update_aggregation_composite_sequence_accepts_partial_null_tuple() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("_SEQUENCE_NUMBER", DataType::Int64, false),
            Field::new("_VALUE_KIND", DataType::Int8, false),
            Field::new("seq_major", DataType::Int32, true),
            Field::new("seq_minor", DataType::Int32, true),
            Field::new("value", DataType::Utf8, true),
        ]));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("seq_major", DataType::Int32, true),
            Field::new("seq_minor", DataType::Int32, true),
            Field::new("value", DataType::Utf8, true),
        ]));
        let output_fields = vec![
            DataField::new(0, "pk".into(), crate::spec::DataType::Int(IntType::new())),
            DataField::new(
                1,
                "seq_major".into(),
                crate::spec::DataType::Int(IntType::new()),
            ),
            DataField::new(
                2,
                "seq_minor".into(),
                crate::spec::DataType::Int(IntType::new()),
            ),
            DataField::new(
                3,
                "value".into(),
                crate::spec::DataType::VarChar(VarCharType::string_type()),
            ),
        ];
        let options = HashMap::from([
            ("merge-engine".to_string(), "partial-update".to_string()),
            (
                "fields.seq_major,seq_minor.sequence-group".to_string(),
                "value".to_string(),
            ),
            (
                "fields.value.aggregate-function".to_string(),
                "listagg".to_string(),
            ),
        ]);
        let stream = stream_from_batches(vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 1])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int8Array::from(vec![0, 0, 0])),
                Arc::new(Int32Array::from(vec![Some(10), Some(11), None])),
                Arc::new(Int32Array::from(vec![Some(1), None, None])),
                Arc::new(StringArray::from(vec![
                    Some("base"),
                    Some("partial"),
                    Some("ignored"),
                ])),
            ],
        )
        .unwrap()]);

        let result = SortMergeReaderBuilder::new(
            vec![stream],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3, 4, 5],
            output_schema,
            Box::new(
                PartialUpdateMergeFunction::new_with_schema(
                    &options,
                    "test_table",
                    &output_fields,
                    &output_fields,
                    &["pk".to_string()],
                )
                .unwrap(),
            ),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let batch = &result[0];
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            11
        );
        assert!(batch.column(2).is_null(0));
        assert_eq!(
            batch
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "base,partial"
        );
    }

    #[tokio::test]
    async fn test_partial_update_last_non_null_aggregation_without_sequence_group() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("_SEQUENCE_NUMBER", DataType::Int64, false),
            Field::new("_VALUE_KIND", DataType::Int8, false),
            Field::new("value", DataType::Int32, true),
        ]));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("value", DataType::Int32, true),
        ]));
        let output_fields = vec![
            DataField::new(0, "pk".into(), crate::spec::DataType::Int(IntType::new())),
            DataField::new(
                1,
                "value".into(),
                crate::spec::DataType::Int(IntType::new()),
            ),
        ];
        let options = HashMap::from([
            ("merge-engine".to_string(), "partial-update".to_string()),
            (
                "fields.value.aggregate-function".to_string(),
                "last_non_null_value".to_string(),
            ),
        ]);
        let stream = stream_from_batches(vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 1])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int8Array::from(vec![0, 0, 0])),
                Arc::new(Int32Array::from(vec![Some(10), Some(30), None])),
            ],
        )
        .unwrap()]);

        let result = SortMergeReaderBuilder::new(
            vec![stream],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3],
            output_schema,
            Box::new(
                PartialUpdateMergeFunction::new_with_schema(
                    &options,
                    "test_table",
                    &output_fields,
                    &output_fields,
                    &["pk".to_string()],
                )
                .unwrap(),
            ),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        assert_eq!(
            result[0]
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            30
        );
    }

    #[tokio::test]
    async fn test_partial_update_sequence_group_listagg_preserves_group_sequence_order() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("_SEQUENCE_NUMBER", DataType::Int64, false),
            Field::new("_VALUE_KIND", DataType::Int8, false),
            Field::new("version", DataType::Int32, true),
            Field::new("value", DataType::Utf8, true),
        ]));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("version", DataType::Int32, true),
            Field::new("value", DataType::Utf8, true),
        ]));
        let output_fields = vec![
            DataField::new(0, "pk".into(), crate::spec::DataType::Int(IntType::new())),
            DataField::new(
                1,
                "version".into(),
                crate::spec::DataType::Int(IntType::new()),
            ),
            DataField::new(
                2,
                "value".into(),
                crate::spec::DataType::VarChar(VarCharType::string_type()),
            ),
        ];
        let options = HashMap::from([
            ("merge-engine".to_string(), "partial-update".to_string()),
            (
                "fields.version.sequence-group".to_string(),
                "value".to_string(),
            ),
            (
                "fields.value.aggregate-function".to_string(),
                "listagg".to_string(),
            ),
        ]);
        let stream = stream_from_batches(vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 1, 1, 1])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(Int8Array::from(vec![0, 0, 0, 0, 0])),
                Arc::new(Int32Array::from(vec![
                    Some(10),
                    Some(12),
                    Some(11),
                    Some(12),
                    None,
                ])),
                Arc::new(StringArray::from(vec![
                    Some("b"),
                    Some("d"),
                    Some("c"),
                    Some("e"),
                    Some("ignored"),
                ])),
            ],
        )
        .unwrap()]);

        let result = SortMergeReaderBuilder::new(
            vec![stream],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3, 4],
            output_schema,
            Box::new(
                PartialUpdateMergeFunction::new_with_schema(
                    &options,
                    "test_table",
                    &output_fields,
                    &output_fields,
                    &["pk".to_string()],
                )
                .unwrap(),
            ),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let batch = &result[0];
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            12
        );
        assert_eq!(
            batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "c,b,d,e"
        );
    }

    #[tokio::test]
    async fn test_partial_update_merge_rejects_delete_like_rows() {
        let schema = make_schema();
        let output_schema = make_output_schema();
        let s0 = stream_from_batches(vec![make_batch_with_kind(
            &schema,
            vec![1],
            vec![1],
            vec![0],
            vec![Some("old")],
        )]);
        let s1 = stream_from_batches(vec![make_batch_with_kind(
            &schema,
            vec![1],
            vec![2],
            vec![3],
            vec![Some("delete")],
        )]);

        let err = SortMergeReaderBuilder::new(
            vec![s0, s1],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3],
            output_schema,
            Box::new(PartialUpdateMergeFunction::new(&HashMap::new(), "test_table").unwrap()),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            Error::Unsupported { message }
            if message.contains("partial-update does not support DELETE or UPDATE_BEFORE")
        ));
    }

    #[tokio::test]
    async fn test_partial_update_merge_ignores_delete_when_configured() {
        let schema = make_schema();
        let output_schema = make_output_schema();
        let s0 = stream_from_batches(vec![make_batch_with_kind(
            &schema,
            vec![1],
            vec![1],
            vec![0],
            vec![Some("old")],
        )]);
        let s1 = stream_from_batches(vec![make_batch_with_kind(
            &schema,
            vec![1],
            vec![2],
            vec![3],
            vec![Some("delete")],
        )]);
        let options = HashMap::from([
            ("merge-engine".to_string(), "partial-update".to_string()),
            ("ignore-delete".to_string(), "true".to_string()),
        ]);

        let result = SortMergeReaderBuilder::new(
            vec![s0, s1],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3],
            output_schema,
            Box::new(PartialUpdateMergeFunction::new(&options, "test_table").unwrap()),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 1);
        assert_eq!(
            result[0]
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "old"
        );
    }

    #[tokio::test]
    async fn test_partial_update_merge_alias_ignores_update_before() {
        let schema = make_schema();
        let output_schema = make_output_schema();
        let s0 = stream_from_batches(vec![make_batch_with_kind(
            &schema,
            vec![1],
            vec![1],
            vec![0],
            vec![Some("old")],
        )]);
        let s1 = stream_from_batches(vec![make_batch_with_kind(
            &schema,
            vec![1],
            vec![2],
            vec![1],
            vec![Some("before")],
        )]);
        let s2 = stream_from_batches(vec![make_batch_with_kind(
            &schema,
            vec![1],
            vec![3],
            vec![2],
            vec![Some("new")],
        )]);
        let options = HashMap::from([
            ("merge-engine".to_string(), "partial-update".to_string()),
            (
                "partial-update.ignore-delete".to_string(),
                "true".to_string(),
            ),
        ]);

        let result = SortMergeReaderBuilder::new(
            vec![s0, s1, s2],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3],
            output_schema,
            Box::new(PartialUpdateMergeFunction::new(&options, "test_table").unwrap()),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 1);
        assert_eq!(
            result[0]
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "new"
        );
    }

    #[test]
    fn test_partial_update_delete_preserves_payload_and_latest_metadata() {
        let schema = make_schema();
        let batch = make_batch_with_kind(
            &schema,
            vec![1, 1, 1],
            vec![10, 11, 12],
            vec![0, 3, 1],
            vec![Some("initial"), Some("deleted"), Some("before")],
        );
        let rows = [0, 3, 1]
            .into_iter()
            .enumerate()
            .map(|(row_idx, value_kind)| MergeRow {
                batch_idx: 0,
                row_idx,
                sequence_number: 10 + row_idx as i64,
                value_kind,
                user_sequence: None,
            })
            .collect::<Vec<_>>();
        let options = HashMap::from([
            ("merge-engine".into(), "partial-update".into()),
            (
                "partial-update.remove-record-on-delete".into(),
                "true".into(),
            ),
        ]);
        let merge = PartialUpdateMergeFunction::new(&options, "test_table").unwrap();
        let result = merge
            .merge(
                &rows,
                &[BufferedBatch::Source(batch)],
                &[0, 1, 2, 3],
                &schema,
            )
            .unwrap();
        let MergeResult::MaterializedRetractRow(deleted, RowKind::Delete) = result else {
            panic!("expected a materialized delete row");
        };
        assert_eq!(
            deleted
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            12
        );
        assert_eq!(
            deleted
                .column(2)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(0),
            RowKind::Delete.to_value()
        );
        assert_eq!(
            deleted
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            // Java PR 11178: the UPDATE_BEFORE after the DELETE applies its
            // non-null fields and keeps the DELETE result kind.
            "before"
        );
    }

    #[test]
    fn test_partial_update_add_clears_delete_state_before_update_before() {
        let schema = make_schema();
        let options = HashMap::from([
            ("merge-engine".into(), "partial-update".into()),
            (
                "partial-update.remove-record-on-delete".into(),
                "true".into(),
            ),
        ]);
        let merge = PartialUpdateMergeFunction::new(&options, "test_table").unwrap();

        for add_kind in [RowKind::Insert, RowKind::UpdateAfter] {
            let kinds = [
                RowKind::Delete.to_value(),
                add_kind.to_value(),
                RowKind::UpdateBefore.to_value(),
            ];
            let batch = make_batch_with_kind(
                &schema,
                vec![1, 1, 1],
                vec![10, 11, 12],
                kinds.to_vec(),
                vec![Some("deleted"), Some("added"), Some("before")],
            );
            let rows = kinds
                .into_iter()
                .enumerate()
                .map(|(row_idx, value_kind)| MergeRow {
                    batch_idx: 0,
                    row_idx,
                    sequence_number: 10 + row_idx as i64,
                    value_kind,
                    user_sequence: None,
                })
                .collect::<Vec<_>>();
            let result = merge
                .merge(
                    &rows,
                    &[BufferedBatch::Source(batch)],
                    &[0, 1, 2, 3],
                    &schema,
                )
                .unwrap();
            // Java PR 11178: without sequence groups, an add after a DELETE
            // clears the DELETE state, so a trailing UPDATE_BEFORE stays
            // UPDATE_BEFORE instead of emitting DELETE and re-initializing
            // older files on read.
            let MergeResult::MaterializedRetractRow(retracted, RowKind::UpdateBefore) = result
            else {
                panic!("expected a materialized update-before row for add_kind={add_kind:?}");
            };
            assert_eq!(
                retracted
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                12
            );
            assert_eq!(
                retracted
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .unwrap()
                    .value(0),
                RowKind::UpdateBefore.to_value()
            );
            assert_eq!(
                retracted
                    .column(3)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                "before"
            );
        }
    }

    #[tokio::test]
    async fn test_partial_update_delete_value_is_baseline_for_later_sparse_insert() {
        let schema = make_schema();
        let stream = stream_from_batches(vec![make_batch_with_kind(
            &schema,
            vec![1, 1],
            vec![1, 2],
            vec![3, 0],
            vec![Some("from-delete"), None],
        )]);
        let options = HashMap::from([
            ("merge-engine".into(), "partial-update".into()),
            (
                "partial-update.remove-record-on-delete".into(),
                "true".into(),
            ),
        ]);
        let output = SortMergeReaderBuilder::new(
            vec![stream],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3],
            make_output_schema(),
            Box::new(PartialUpdateMergeFunction::new(&options, "test_table").unwrap()),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].num_rows(), 1);
        assert_eq!(
            output[0]
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "from-delete"
        );
    }

    #[tokio::test]
    async fn test_partial_update_merge_omits_retract_only_key_when_configured() {
        let schema = make_schema();
        let output_schema = make_output_schema();
        let stream = stream_from_batches(vec![make_batch_with_kind(
            &schema,
            vec![1],
            vec![1],
            vec![3],
            vec![Some("delete")],
        )]);
        let options = HashMap::from([
            ("merge-engine".to_string(), "partial-update".to_string()),
            ("ignore-delete".to_string(), "true".to_string()),
        ]);

        let result = SortMergeReaderBuilder::new(
            vec![stream],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3],
            output_schema,
            Box::new(PartialUpdateMergeFunction::new(&options, "test_table").unwrap()),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    }

    #[test]
    fn test_partial_update_merge_function_new_rejects_unsupported_options() {
        let options = HashMap::from([
            ("merge-engine".to_string(), "partial-update".to_string()),
            ("fields.price.ignore-delete".to_string(), "true".to_string()),
        ]);

        let err = PartialUpdateMergeFunction::new(&options, "default.t").unwrap_err();

        assert!(matches!(
            err,
            Error::Unsupported { message }
            if message.contains("fields.price.ignore-delete")
        ));
    }

    #[test]
    fn test_partial_update_new_with_schema_validates_aggregate_functions() {
        let fields = vec![
            DataField::new(0, "pk".into(), crate::spec::DataType::Int(IntType::new())),
            DataField::new(
                1,
                "version".into(),
                crate::spec::DataType::Int(IntType::new()),
            ),
            DataField::new(
                2,
                "value".into(),
                crate::spec::DataType::Int(IntType::new()),
            ),
        ];
        let options = HashMap::from([
            ("merge-engine".to_string(), "partial-update".to_string()),
            (
                "fields.version.sequence-group".to_string(),
                "value".to_string(),
            ),
            (
                "fields.value.aggregate-function".to_string(),
                "sume".to_string(),
            ),
        ]);

        let err = PartialUpdateMergeFunction::new_with_schema(
            &options,
            "default.t",
            &fields,
            &fields,
            &["pk".to_string()],
        )
        .unwrap_err();

        assert!(
            matches!(err, Error::ConfigInvalid { ref message }
                if message.contains("sume") && message.contains("value")),
            "expected schema-aware aggregate validation, got {err:?}"
        );
    }

    // ---------- AggregateMergeFunction ----------

    use crate::spec::{DataType as PaimonDataType, IntType, VarCharType};

    fn aggregation_output_fields() -> Vec<DataField> {
        vec![
            DataField::new(0, "pk".into(), PaimonDataType::Int(IntType::new())),
            DataField::new(1, "amount".into(), PaimonDataType::Int(IntType::new())),
            DataField::new(
                2,
                "tag".into(),
                // listagg requires unbounded VARCHAR (STRING), matching Java.
                PaimonDataType::VarChar(VarCharType::string_type()),
            ),
        ]
    }

    fn aggregation_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("_SEQUENCE_NUMBER", DataType::Int64, false),
            Field::new("_VALUE_KIND", DataType::Int8, false),
            Field::new("amount", DataType::Int32, true),
            Field::new("tag", DataType::Utf8, true),
        ]))
    }

    fn aggregation_output_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("amount", DataType::Int32, true),
            Field::new("tag", DataType::Utf8, true),
        ]))
    }

    fn agg_options(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        let mut opts = HashMap::from([("merge-engine".to_string(), "aggregation".to_string())]);
        for (k, v) in pairs {
            opts.insert((*k).to_string(), (*v).to_string());
        }
        opts
    }

    fn build_agg_function(options: HashMap<String, String>) -> Box<dyn MergeFunction> {
        Box::new(
            AggregateMergeFunction::new(
                &options,
                "test_table",
                &aggregation_output_fields(),
                &["pk".to_string()],
                &[],
            )
            .unwrap(),
        )
    }

    /// Picked aggregation rows read the same as materialized ones, across output
    /// batches and next to lone versions, deleted keys and all-NULL aggregates.
    #[tokio::test]
    async fn test_aggregate_picked_rows_match_materialized_rows() {
        struct Materializing(AggregateMergeFunction);
        impl MergeFunction for Materializing {
            fn merge(
                &self,
                rows: &[MergeRow],
                batch_buffer: &[BufferedBatch],
                source_output_col_indices: &[usize],
                output_schema: &SchemaRef,
            ) -> crate::Result<MergeResult> {
                self.0
                    .merge(rows, batch_buffer, source_output_col_indices, output_schema)
            }
        }

        let schema = aggregation_schema();
        let output_schema = aggregation_output_schema();
        let streams = || {
            let batch = |pk: Vec<i32>,
                         seq: i64,
                         kinds: Vec<i8>,
                         amount: Vec<Option<i32>>,
                         tag: Vec<Option<&str>>| {
                let seqs = vec![seq; pk.len()];
                stream_from_batches(vec![RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int32Array::from(pk)),
                        Arc::new(Int64Array::from(seqs)),
                        Arc::new(Int8Array::from(kinds)),
                        Arc::new(Int32Array::from(amount)),
                        Arc::new(StringArray::from(tag)),
                    ],
                )
                .unwrap()])
            };
            vec![
                batch(
                    vec![1, 2, 3, 4],
                    1,
                    vec![0, 0, 0, 0],
                    vec![Some(10), Some(20), None, Some(40)],
                    vec![Some("a"), Some("x"), None, Some("d")],
                ),
                // Key 4 ends with a DELETE, which removes it.
                batch(
                    vec![1, 2, 4, 5],
                    2,
                    vec![0, 0, RowKind::Delete as i8, 0],
                    vec![Some(5), None, None, Some(50)],
                    vec![None, Some("y"), None, Some("e")],
                ),
                batch(
                    vec![1, 3, 6],
                    3,
                    vec![0, 0, 0],
                    vec![Some(1), None, Some(60)],
                    vec![Some("c"), None, Some("f")],
                ),
            ]
        };
        let options = agg_options(&[
            ("fields.amount.aggregate-function", "sum"),
            ("fields.tag.aggregate-function", "last_non_null_value"),
            ("aggregation.remove-record-on-delete", "true"),
        ]);
        let new_merge = || {
            AggregateMergeFunction::new(
                &options,
                "test_table",
                &aggregation_output_fields(),
                &["pk".to_string()],
                &[],
            )
            .unwrap()
        };
        let read = |merge: Box<dyn MergeFunction>| {
            SortMergeReaderBuilder::new(
                streams(),
                schema.clone(),
                vec![0],
                1,
                2,
                vec![],
                vec![3, 4],
                output_schema.clone(),
                merge,
            )
            .with_batch_size(2)
            .build()
            .unwrap()
            .try_collect::<Vec<_>>()
        };
        let picked = read(Box::new(new_merge())).await.unwrap();
        let materialized = read(Box::new(Materializing(new_merge()))).await.unwrap();
        assert_eq!(picked, materialized);

        let picked = arrow_select::concat::concat_batches(&output_schema, &picked).unwrap();
        assert_eq!(
            picked.column(0).as_ref(),
            &Int32Array::from(vec![1, 2, 3, 5, 6]) as &dyn arrow_array::Array
        );
        assert_eq!(
            picked.column(1).as_ref(),
            &Int32Array::from(vec![Some(16), Some(20), None, Some(50), Some(60)])
                as &dyn arrow_array::Array
        );
        assert_eq!(
            picked.column(2).as_ref(),
            &StringArray::from(vec![Some("c"), Some("y"), None, Some("e"), Some("f")])
                as &dyn arrow_array::Array
        );
    }

    #[tokio::test]
    async fn test_aggregate_merge_sum_and_listagg() {
        let schema = aggregation_schema();
        let output_schema = aggregation_output_schema();
        let s0 = stream_from_batches(vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![1, 1])),
                Arc::new(Int8Array::from(vec![0, 0])),
                Arc::new(Int32Array::from(vec![Some(10), Some(20)])),
                Arc::new(StringArray::from(vec![Some("a"), Some("x")])),
            ],
        )
        .unwrap()]);
        let s1 = stream_from_batches(vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![2, 2, 1])),
                Arc::new(Int8Array::from(vec![0, 0, 0])),
                Arc::new(Int32Array::from(vec![Some(5), Some(7), Some(99)])),
                Arc::new(StringArray::from(vec![Some("b"), None, Some("solo")])),
            ],
        )
        .unwrap()]);

        let options = agg_options(&[
            ("fields.amount.aggregate-function", "sum"),
            ("fields.tag.aggregate-function", "listagg"),
            ("fields.tag.list-agg-delimiter", "|"),
        ]);

        let batches = SortMergeReaderBuilder::new(
            vec![s0, s1],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3, 4],
            output_schema,
            build_agg_function(options),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let mut rows: Vec<(i32, Option<i32>, Option<String>)> = Vec::new();
        for batch in &batches {
            let pks = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let amounts = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let tags = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                rows.push((
                    pks.value(i),
                    if amounts.is_null(i) {
                        None
                    } else {
                        Some(amounts.value(i))
                    },
                    if tags.is_null(i) {
                        None
                    } else {
                        Some(tags.value(i).to_string())
                    },
                ));
            }
        }
        rows.sort_by_key(|row| row.0);

        assert_eq!(
            rows,
            vec![
                (1, Some(15), Some("a|b".to_string())),
                (2, Some(27), Some("x".to_string())),
                (3, Some(99), Some("solo".to_string())),
            ]
        );
    }

    #[tokio::test]
    async fn test_aggregate_merge_retracts_delete_rows() {
        let schema = aggregation_schema();
        let output_schema = aggregation_output_schema();
        let s0 = stream_from_batches(vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int8Array::from(vec![0])),
                Arc::new(Int32Array::from(vec![Some(10)])),
                Arc::new(StringArray::from(vec![Some("a")])),
            ],
        )
        .unwrap()]);
        // Row kind 3 = DELETE
        let s1 = stream_from_batches(vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(Int8Array::from(vec![3])),
                Arc::new(Int32Array::from(vec![Some(99)])),
                Arc::new(StringArray::from(vec![Some("b")])),
            ],
        )
        .unwrap()]);

        let options = agg_options(&[
            ("fields.amount.aggregate-function", "sum"),
            ("fields.tag.aggregate-function", "last_value"),
        ]);

        let result = SortMergeReaderBuilder::new(
            vec![s0, s1],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3, 4],
            output_schema,
            build_agg_function(options),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
        assert_eq!(result.len(), 1);
        let amount = result[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let tag = result[0]
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(amount.value(0), -89);
        assert!(tag.is_null(0));
    }

    #[tokio::test]
    async fn test_aggregate_remove_record_on_delete_keeps_delete_payload() {
        let schema = aggregation_schema();
        let input = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 1])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int8Array::from(vec![0, 3, 0])),
                Arc::new(Int32Array::from(vec![Some(100), Some(10), Some(5)])),
                Arc::new(StringArray::from(vec![Some("old"), Some("deleted"), None])),
            ],
        )
        .unwrap();
        let options = agg_options(&[
            ("aggregation.remove-record-on-delete", "true"),
            ("fields.amount.aggregate-function", "sum"),
        ]);
        let result = SortMergeReaderBuilder::new(
            vec![stream_from_batches(vec![input])],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3, 4],
            aggregation_output_schema(),
            build_agg_function(options),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
        assert_eq!(result.len(), 1);
        let amount = result[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let tag = result[0]
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(amount.value(0), 15);
        assert_eq!(tag.value(0), "deleted");
    }

    #[test]
    fn test_aggregate_merge_function_falls_back_to_last_non_null_value() {
        // With no per-field nor default aggregate-function configured, each
        // value column should fall back to last_non_null_value (matching Java
        // AggregateMergeFunction#getAggFuncName).
        let options = HashMap::from([("merge-engine".to_string(), "aggregation".to_string())]);
        let mf = AggregateMergeFunction::new(
            &options,
            "test_table",
            &aggregation_output_fields(),
            &["pk".to_string()],
            &[],
        )
        .expect("aggregation engine must accept tables without explicit per-field config");
        // Smoke check: construction succeeds.
        let _ = mf;
    }

    #[test]
    fn test_aggregate_merge_function_sequence_field_forced_last_value() {
        // 'tag' is the sequence field; even though the table-level default is
        // listagg, it should be forced to last_value (so the latest tag survives).
        let schema = aggregation_schema();
        let output_schema = aggregation_output_schema();
        let options = agg_options(&[
            ("fields.amount.aggregate-function", "sum"),
            ("fields.default-aggregate-function", "listagg"),
            ("sequence.field", "tag"),
        ]);
        let mf = AggregateMergeFunction::new(
            &options,
            "test_table",
            &aggregation_output_fields(),
            &["pk".to_string()],
            &["tag".to_string()],
        )
        .unwrap();

        let s0 = stream_from_batches(vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 1])),
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int8Array::from(vec![0, 0])),
                Arc::new(Int32Array::from(vec![Some(3), Some(4)])),
                Arc::new(StringArray::from(vec![Some("first"), Some("second")])),
            ],
        )
        .unwrap()]);

        // Use the merge function via builder.
        let batches = futures::executor::block_on(async {
            SortMergeReaderBuilder::new(
                vec![s0],
                schema,
                vec![0],
                1,
                2,
                vec![],
                vec![3, 4],
                output_schema,
                Box::new(mf),
            )
            .build()
            .unwrap()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap()
        });

        let batch = &batches[0];
        let tags = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let amounts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(tags.value(0), "second"); // last_value, not listagg.
        assert_eq!(amounts.value(0), 7); // sum of 3 + 4.
    }

    #[test]
    fn test_aggregate_merge_function_default_function_applies_when_per_field_absent() {
        let options = agg_options(&[("fields.default-aggregate-function", "last_non_null_value")]);
        let mf = AggregateMergeFunction::new(
            &options,
            "test_table",
            &aggregation_output_fields(),
            &["pk".to_string()],
            &[],
        )
        .unwrap();
        // Construction must succeed: amount and tag fall back to the default.
        // Tag is VarChar — last_non_null_value supports any type.
        let _ = mf;
    }

    #[test]
    fn test_aggregate_merge_function_rejects_unsupported_options() {
        let options = agg_options(&[("fields.amount.sequence-group", "true")]);
        let err = AggregateMergeFunction::new(
            &options,
            "test_table",
            &aggregation_output_fields(),
            &["pk".to_string()],
            &[],
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::Unsupported { message } if message.contains("sequence-group"))
        );
    }
}
