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

use super::shredding::PhysicalFormatWriterFactory;
use super::{FilePredicates, FormatFileReader, FormatFileWriter};
use crate::arrow::filtering::{predicates_may_match_with_schema, StatsAccessor};
use crate::io::{FileRead, OutputFile};
use crate::spec::{DataField, DataType, Datum, Predicate, PredicateOperator};
use crate::table::{ArrowRecordBatchStream, RowRange};
use crate::Error;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Datum as ArrowDatum, Decimal128Array,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, Scalar,
    StringArray,
};
use arrow_ord::cmp::{
    eq as arrow_eq, gt as arrow_gt, gt_eq as arrow_gt_eq, lt as arrow_lt, lt_eq as arrow_lt_eq,
    neq as arrow_neq,
};
use arrow_schema::ArrowError;
use arrow_string::like::{
    contains as arrow_contains, ends_with as arrow_ends_with, like as arrow_like,
    starts_with as arrow_starts_with,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use parquet::arrow::arrow_reader::{
    ArrowPredicate, ArrowPredicateFn, ArrowReaderOptions, RowFilter, RowSelection, RowSelector,
};
use parquet::arrow::async_reader::{AsyncFileReader, MetadataFetch};
use parquet::arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::{
    PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader, RowGroupMetaData,
};
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::file::properties::WriterProperties;
use parquet::file::statistics::Statistics as ParquetStatistics;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

pub(crate) struct ParquetFormatReader;

/// Parquet implementation of [`FormatFileWriter`].
/// Streams data directly to storage via `AsyncArrowWriter` + opendal.
pub(crate) struct ParquetFormatWriter {
    inner: AsyncArrowWriter<Box<dyn crate::io::AsyncFileWrite>>,
}

pub(crate) struct ParquetPhysicalWriterFactory {
    output: OutputFile,
    compression: String,
    zstd_level: i32,
}

impl ParquetPhysicalWriterFactory {
    pub(crate) fn new(output: &OutputFile, compression: &str, zstd_level: i32) -> Self {
        Self {
            output: output.clone(),
            compression: compression.to_string(),
            zstd_level,
        }
    }
}

#[async_trait]
impl PhysicalFormatWriterFactory for ParquetPhysicalWriterFactory {
    async fn create_writer(
        &mut self,
        schema: arrow_schema::SchemaRef,
        _write_fields: Option<&[DataField]>,
    ) -> crate::Result<Box<dyn FormatFileWriter>> {
        Ok(Box::new(
            ParquetFormatWriter::new(&self.output, schema, &self.compression, self.zstd_level)
                .await?,
        ))
    }
}

impl ParquetFormatWriter {
    pub(crate) async fn new(
        output: &OutputFile,
        schema: arrow_schema::SchemaRef,
        compression: &str,
        zstd_level: i32,
    ) -> crate::Result<Self> {
        let async_write = output.async_writer().await?;
        let codec = parse_compression(compression, zstd_level);
        let inner = create_parquet_arrow_writer(async_write, schema, codec)?;
        Ok(Self { inner })
    }
}

fn create_parquet_arrow_writer(
    async_write: Box<dyn crate::io::AsyncFileWrite>,
    schema: arrow_schema::SchemaRef,
    codec: Compression,
) -> crate::Result<AsyncArrowWriter<Box<dyn crate::io::AsyncFileWrite>>> {
    let props = WriterProperties::builder().set_compression(codec).build();
    AsyncArrowWriter::try_new(async_write, schema, Some(props)).map_err(|e| {
        crate::Error::DataInvalid {
            message: format!("Failed to create parquet writer: {e}"),
            source: None,
        }
    })
}

/// Map Paimon `file.compression` value to parquet [`Compression`].
fn parse_compression(codec: &str, zstd_level: i32) -> Compression {
    match codec.to_ascii_lowercase().as_str() {
        "zstd" => {
            let level = ZstdLevel::try_new(zstd_level).unwrap_or_default();
            Compression::ZSTD(level)
        }
        "lz4" => Compression::LZ4_RAW,
        "snappy" => Compression::SNAPPY,
        "gzip" | "gz" => Compression::GZIP(Default::default()),
        "none" | "uncompressed" => Compression::UNCOMPRESSED,
        _ => Compression::UNCOMPRESSED,
    }
}

#[async_trait]
impl FormatFileWriter for ParquetFormatWriter {
    async fn write(&mut self, batch: &RecordBatch) -> crate::Result<()> {
        self.inner
            .write(batch)
            .await
            .map_err(|e| crate::Error::DataInvalid {
                message: format!("Failed to write parquet batch: {e}"),
                source: None,
            })
    }

    fn num_bytes(&self) -> usize {
        self.inner.bytes_written() + self.inner.in_progress_size()
    }

    fn in_progress_size(&self) -> usize {
        self.inner.in_progress_size()
    }

    async fn flush(&mut self) -> crate::Result<()> {
        self.inner
            .flush()
            .await
            .map_err(|e| crate::Error::DataInvalid {
                message: format!("Failed to flush parquet writer: {e}"),
                source: None,
            })
    }

    async fn close(mut self: Box<Self>) -> crate::Result<u64> {
        self.inner
            .finish()
            .await
            .map_err(|e| crate::Error::DataInvalid {
                message: format!("Failed to close parquet writer: {e}"),
                source: None,
            })?;
        Ok(self.inner.bytes_written() as u64)
    }
}

#[async_trait]
impl FormatFileReader for ParquetFormatReader {
    async fn read_batch_stream(
        &self,
        reader: Box<dyn FileRead>,
        file_size: u64,
        read_fields: &[DataField],
        predicates: Option<&FilePredicates>,
        batch_size: Option<usize>,
        row_selection: Option<Vec<RowRange>>,
    ) -> crate::Result<ArrowRecordBatchStream> {
        let arrow_file_reader = ArrowFileReader::new(file_size, reader);

        let empty_predicates = Vec::new();
        let (preds, file_fields): (&[Predicate], &[DataField]) = match predicates {
            Some(fp) => (&fp.predicates, &fp.file_fields),
            None => (&empty_predicates, &[]),
        };

        // Only load the Parquet page index (ColumnIndex + OffsetIndex) when a
        // predicate can use it for page-level pruning — matching Java Paimon,
        // which gets page-level skipping for free via parquet-mr's
        // `readNextFilteredRowGroup`. arrow-rs does not do this automatically, so
        // we build the RowSelection ourselves below. Without a predicate the
        // index is pure overhead (an extra metadata read with no benefit), so we
        // skip it. `Optional` lets files without a page index fall through to
        // row-group-level pruning instead of erroring.
        let mut arrow_options = ArrowReaderOptions::new();
        if !preds.is_empty() {
            arrow_options = arrow_options.with_page_index_policy(PageIndexPolicy::Optional);
        }
        let mut batch_stream_builder =
            ParquetRecordBatchStreamBuilder::new_with_options(arrow_file_reader, arrow_options)
                .await?;

        let parquet_schema = batch_stream_builder.parquet_schema().clone();
        let root_schema = parquet_schema.root_schema();
        let root_indices: Vec<usize> = read_fields
            .iter()
            .filter_map(|f| {
                root_schema
                    .get_fields()
                    .iter()
                    .position(|pf| pf.name() == f.name())
            })
            .collect();

        let mask = ProjectionMask::roots(&parquet_schema, root_indices);
        batch_stream_builder = batch_stream_builder.with_projection(mask);

        let parquet_row_filter = build_parquet_row_filter(&parquet_schema, preds, file_fields)?;
        if let Some(f) = parquet_row_filter {
            batch_stream_builder = batch_stream_builder.with_row_filter(f);
        }

        let predicate_row_selection = build_predicate_row_selection(
            batch_stream_builder.metadata().row_groups(),
            preds,
            file_fields,
        )?;
        let mut combined_selection = predicate_row_selection;

        // Page-level selection. Returns `None` when ColumnIndex / OffsetIndex are
        // absent (page index not loaded, older files, writer without page index)
        // or when no page could be skipped, so intersecting is a no-op then.
        let page_selection =
            build_predicate_page_selection(batch_stream_builder.metadata(), preds, file_fields)?;
        combined_selection = intersect_optional_row_selections(combined_selection, page_selection);

        if let Some(ref ranges) = row_selection {
            let range_selection =
                build_row_ranges_selection(batch_stream_builder.metadata().row_groups(), ranges);
            combined_selection =
                intersect_optional_row_selections(combined_selection, Some(range_selection));
        }
        if let Some(sel) = combined_selection {
            batch_stream_builder = batch_stream_builder.with_row_selection(sel);
        }
        if let Some(size) = batch_size {
            batch_stream_builder = batch_stream_builder.with_batch_size(size);
        }

        Ok(batch_stream_builder.build()?.map_err(Error::from).boxed())
    }
}

// ---------------------------------------------------------------------------
// Parquet row-filter helpers
// ---------------------------------------------------------------------------

fn build_parquet_row_filter(
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
    predicates: &[Predicate],
    file_fields: &[DataField],
) -> crate::Result<Option<RowFilter>> {
    if predicates.is_empty() {
        return Ok(None);
    }

    let mut filters: Vec<Box<dyn ArrowPredicate>> = Vec::new();

    for predicate in predicates {
        if let Some(filter) = build_parquet_arrow_predicate(parquet_schema, predicate, file_fields)?
        {
            filters.push(filter);
        }
    }

    if filters.is_empty() {
        Ok(None)
    } else {
        Ok(Some(RowFilter::new(filters)))
    }
}

fn build_parquet_arrow_predicate(
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
    predicate: &Predicate,
    file_fields: &[DataField],
) -> crate::Result<Option<Box<dyn ArrowPredicate>>> {
    let Predicate::Leaf {
        index,
        data_type: _,
        op,
        literals,
        ..
    } = predicate
    else {
        return Ok(None);
    };
    if !predicate_supported_for_parquet_row_filter(*op) {
        return Ok(None);
    }

    let Some(file_field) = file_fields.get(*index) else {
        return Ok(None);
    };
    let Some(root_index) = parquet_root_index(parquet_schema, file_field.name()) else {
        return Ok(None);
    };
    if !parquet_row_filter_literals_supported(*op, literals, file_field.data_type())? {
        return Ok(None);
    }

    let projection = ProjectionMask::roots(parquet_schema, [root_index]);
    let op = *op;
    let data_type = file_field.data_type().clone();
    let literals = literals.to_vec();
    Ok(Some(Box::new(ArrowPredicateFn::new(
        projection,
        move |batch: RecordBatch| {
            let Some(column) = batch.columns().first() else {
                return Ok(BooleanArray::new_null(batch.num_rows()));
            };
            evaluate_exact_leaf_predicate(column, &data_type, op, &literals)
        },
    ))))
}

fn predicate_supported_for_parquet_row_filter(op: PredicateOperator) -> bool {
    matches!(
        op,
        PredicateOperator::IsNull
            | PredicateOperator::IsNotNull
            | PredicateOperator::Eq
            | PredicateOperator::NotEq
            | PredicateOperator::Lt
            | PredicateOperator::LtEq
            | PredicateOperator::Gt
            | PredicateOperator::GtEq
            | PredicateOperator::In
            | PredicateOperator::NotIn
            | PredicateOperator::StartsWith
            | PredicateOperator::EndsWith
            | PredicateOperator::Contains
            | PredicateOperator::Like
            | PredicateOperator::Between
            | PredicateOperator::NotBetween
    )
}

fn parquet_row_filter_literals_supported(
    op: PredicateOperator,
    literals: &[Datum],
    file_data_type: &DataType,
) -> crate::Result<bool> {
    match op {
        PredicateOperator::IsNull | PredicateOperator::IsNotNull => Ok(true),
        PredicateOperator::Eq
        | PredicateOperator::NotEq
        | PredicateOperator::Lt
        | PredicateOperator::LtEq
        | PredicateOperator::Gt
        | PredicateOperator::GtEq => {
            let Some(literal) = literals.first() else {
                return Ok(false);
            };
            Ok(literal_scalar_for_parquet_filter(literal, file_data_type)?.is_some())
        }
        PredicateOperator::In | PredicateOperator::NotIn => {
            for literal in literals {
                if literal_scalar_for_parquet_filter(literal, file_data_type)?.is_none() {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        PredicateOperator::StartsWith
        | PredicateOperator::EndsWith
        | PredicateOperator::Contains
        | PredicateOperator::Like => {
            // Substring kernels only run against string-typed columns; reject
            // non-string file types early so the filter falls back to stats
            // pruning + residual evaluation.
            if !matches!(file_data_type, DataType::Char(_) | DataType::VarChar(_)) {
                return Ok(false);
            }
            let Some(literal) = literals.first() else {
                return Ok(false);
            };
            Ok(literal_scalar_for_parquet_filter(literal, file_data_type)?.is_some())
        }
        PredicateOperator::Between | PredicateOperator::NotBetween => {
            if literals.len() != 2 {
                return Ok(false);
            }
            for literal in literals {
                if literal_scalar_for_parquet_filter(literal, file_data_type)?.is_none() {
                    return Ok(false);
                }
            }
            Ok(true)
        }
    }
}

fn parquet_root_index(
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
    root_name: &str,
) -> Option<usize> {
    parquet_schema
        .root_schema()
        .get_fields()
        .iter()
        .position(|field| field.name() == root_name)
}

// ---------------------------------------------------------------------------
// Predicate evaluation helpers
// ---------------------------------------------------------------------------

fn evaluate_exact_leaf_predicate(
    array: &ArrayRef,
    data_type: &DataType,
    op: PredicateOperator,
    literals: &[Datum],
) -> Result<BooleanArray, ArrowError> {
    match op {
        PredicateOperator::IsNull => Ok(boolean_mask_from_predicate(array.len(), |row_index| {
            array.is_null(row_index)
        })),
        PredicateOperator::IsNotNull => Ok(boolean_mask_from_predicate(array.len(), |row_index| {
            array.is_valid(row_index)
        })),
        PredicateOperator::In | PredicateOperator::NotIn => {
            evaluate_set_membership_predicate(array, data_type, op, literals)
        }
        PredicateOperator::Eq
        | PredicateOperator::NotEq
        | PredicateOperator::Lt
        | PredicateOperator::LtEq
        | PredicateOperator::Gt
        | PredicateOperator::GtEq
        | PredicateOperator::StartsWith
        | PredicateOperator::EndsWith
        | PredicateOperator::Contains
        | PredicateOperator::Like => {
            let Some(literal) = literals.first() else {
                return Ok(BooleanArray::from(vec![true; array.len()]));
            };
            let Some(scalar) = literal_scalar_for_parquet_filter(literal, data_type)
                .map_err(|e| ArrowError::ComputeError(e.to_string()))?
            else {
                return Ok(BooleanArray::from(vec![true; array.len()]));
            };
            let result = evaluate_column_predicate(array, &scalar, op)?;
            Ok(sanitize_filter_mask(result))
        }
        PredicateOperator::Between | PredicateOperator::NotBetween => {
            evaluate_between_predicate(array, data_type, op, literals)
        }
    }
}

/// `Between` / `NotBetween` translate to `gt_eq(col, low) & lt_eq(col, high)`
/// (and its negation). `arrow_ord::cmp` produces nullable masks: any null
/// row makes the comparison null, so a fully-built `Between` mask preserves
/// nulls. `NotBetween` then negates valid rows and leaves nulls null —
/// matching SQL three-valued logic; `sanitize_filter_mask` collapses nulls
/// into `false` to match the predicate evaluator's "NULL → false" rule.
fn evaluate_between_predicate(
    array: &ArrayRef,
    data_type: &DataType,
    op: PredicateOperator,
    literals: &[Datum],
) -> Result<BooleanArray, ArrowError> {
    let (Some(low), Some(high)) = (literals.first(), literals.get(1)) else {
        return Ok(BooleanArray::from(vec![true; array.len()]));
    };
    let Some(low_scalar) = literal_scalar_for_parquet_filter(low, data_type)
        .map_err(|e| ArrowError::ComputeError(e.to_string()))?
    else {
        return Ok(BooleanArray::from(vec![true; array.len()]));
    };
    let Some(high_scalar) = literal_scalar_for_parquet_filter(high, data_type)
        .map_err(|e| ArrowError::ComputeError(e.to_string()))?
    else {
        return Ok(BooleanArray::from(vec![true; array.len()]));
    };
    let lo_mask = arrow_gt_eq(array, &low_scalar)?;
    let hi_mask = arrow_lt_eq(array, &high_scalar)?;
    let between = arrow_arith::boolean::and_kleene(&lo_mask, &hi_mask)?;
    let result = match op {
        PredicateOperator::Between => between,
        PredicateOperator::NotBetween => arrow_arith::boolean::not(&between)?,
        _ => unreachable!(),
    };
    Ok(sanitize_filter_mask(result))
}

fn evaluate_set_membership_predicate(
    array: &ArrayRef,
    data_type: &DataType,
    op: PredicateOperator,
    literals: &[Datum],
) -> Result<BooleanArray, ArrowError> {
    if literals.is_empty() {
        return Ok(match op {
            PredicateOperator::In => BooleanArray::from(vec![false; array.len()]),
            PredicateOperator::NotIn => {
                boolean_mask_from_predicate(array.len(), |row_index| array.is_valid(row_index))
            }
            _ => unreachable!(),
        });
    }

    let mut combined = match op {
        PredicateOperator::In => BooleanArray::from(vec![false; array.len()]),
        PredicateOperator::NotIn => {
            boolean_mask_from_predicate(array.len(), |row_index| array.is_valid(row_index))
        }
        _ => unreachable!(),
    };

    for literal in literals {
        let Some(scalar) = literal_scalar_for_parquet_filter(literal, data_type)
            .map_err(|e| ArrowError::ComputeError(e.to_string()))?
        else {
            return Ok(BooleanArray::from(vec![true; array.len()]));
        };
        let comparison_op = match op {
            PredicateOperator::In => PredicateOperator::Eq,
            PredicateOperator::NotIn => PredicateOperator::NotEq,
            _ => unreachable!(),
        };
        let mask = sanitize_filter_mask(evaluate_column_predicate(array, &scalar, comparison_op)?);
        combined = combine_filter_masks(&combined, &mask, matches!(op, PredicateOperator::In));
    }

    Ok(combined)
}

fn evaluate_column_predicate(
    column: &ArrayRef,
    scalar: &Scalar<ArrayRef>,
    op: PredicateOperator,
) -> Result<BooleanArray, ArrowError> {
    match op {
        PredicateOperator::Eq => arrow_eq(column, scalar),
        PredicateOperator::NotEq => arrow_neq(column, scalar),
        PredicateOperator::Lt => arrow_lt(column, scalar),
        PredicateOperator::LtEq => arrow_lt_eq(column, scalar),
        PredicateOperator::Gt => arrow_gt(column, scalar),
        PredicateOperator::GtEq => arrow_gt_eq(column, scalar),
        PredicateOperator::StartsWith
        | PredicateOperator::EndsWith
        | PredicateOperator::Contains
        | PredicateOperator::Like => {
            let pattern = pattern_scalar_for_string_kernel(scalar, column.data_type())?;
            match op {
                PredicateOperator::StartsWith => arrow_starts_with(column, &pattern),
                PredicateOperator::EndsWith => arrow_ends_with(column, &pattern),
                PredicateOperator::Contains => arrow_contains(column, &pattern),
                PredicateOperator::Like => arrow_like(column, &pattern),
                _ => unreachable!(),
            }
        }
        PredicateOperator::IsNull
        | PredicateOperator::IsNotNull
        | PredicateOperator::In
        | PredicateOperator::NotIn
        | PredicateOperator::Between
        | PredicateOperator::NotBetween => Ok(BooleanArray::new_null(column.len())),
    }
}

/// `arrow_string::like::*` kernels reject mismatched string types — Utf8 column
/// against Utf8 pattern is fine, but a LargeUtf8 / Utf8View column needs a
/// pattern of the same flavour. The shared scalar built upstream is always
/// `StringArray` (Utf8); promote it to match the column when needed.
fn pattern_scalar_for_string_kernel(
    scalar: &Scalar<ArrayRef>,
    column_type: &arrow_schema::DataType,
) -> Result<Scalar<ArrayRef>, ArrowError> {
    use arrow_array::{LargeStringArray, StringArray, StringViewArray};
    use arrow_schema::DataType as ArrowDataType;

    let arr = scalar.get().0;
    let value = arr
        .as_any()
        .downcast_ref::<StringArray>()
        .and_then(|s| (s.len() == 1 && s.is_valid(0)).then(|| s.value(0).to_string()));
    let Some(value) = value else {
        return Ok(scalar.clone());
    };
    Ok(match column_type {
        ArrowDataType::Utf8 => Scalar::new(Arc::new(StringArray::from(vec![value])) as ArrayRef),
        ArrowDataType::LargeUtf8 => {
            Scalar::new(Arc::new(LargeStringArray::from(vec![value])) as ArrayRef)
        }
        ArrowDataType::Utf8View => {
            Scalar::new(Arc::new(StringViewArray::from(vec![value])) as ArrayRef)
        }
        ArrowDataType::Dictionary(_, value_type) if value_type.as_ref() == &ArrowDataType::Utf8 => {
            Scalar::new(Arc::new(StringArray::from(vec![value])) as ArrayRef)
        }
        other => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "string predicate against non-string column type {other:?}"
            )))
        }
    })
}

fn sanitize_filter_mask(mask: BooleanArray) -> BooleanArray {
    if mask.null_count() == 0 {
        return mask;
    }

    boolean_mask_from_predicate(mask.len(), |row_index| {
        mask.is_valid(row_index) && mask.value(row_index)
    })
}

fn combine_filter_masks(left: &BooleanArray, right: &BooleanArray, use_or: bool) -> BooleanArray {
    debug_assert_eq!(left.len(), right.len());
    boolean_mask_from_predicate(left.len(), |row_index| {
        if use_or {
            left.value(row_index) || right.value(row_index)
        } else {
            left.value(row_index) && right.value(row_index)
        }
    })
}

fn boolean_mask_from_predicate(
    len: usize,
    mut predicate: impl FnMut(usize) -> bool,
) -> BooleanArray {
    BooleanArray::from((0..len).map(&mut predicate).collect::<Vec<_>>())
}

// ---------------------------------------------------------------------------
// Row-group statistics pruning
// ---------------------------------------------------------------------------

struct ParquetRowGroupStats<'a> {
    row_group: &'a RowGroupMetaData,
    column_indices: &'a [Option<usize>],
}

impl StatsAccessor for ParquetRowGroupStats<'_> {
    fn row_count(&self) -> i64 {
        self.row_group.num_rows()
    }

    fn null_count(&self, index: usize) -> Option<i64> {
        let _ = index;
        None
    }

    fn min_value(&self, index: usize, data_type: &DataType) -> Option<Datum> {
        let column_index = self.column_indices.get(index).copied().flatten()?;
        parquet_stats_to_datum(
            self.row_group.column(column_index).statistics()?,
            data_type,
            true,
        )
    }

    fn max_value(&self, index: usize, data_type: &DataType) -> Option<Datum> {
        let column_index = self.column_indices.get(index).copied().flatten()?;
        parquet_stats_to_datum(
            self.row_group.column(column_index).statistics()?,
            data_type,
            false,
        )
    }
}

fn build_predicate_row_selection(
    row_groups: &[RowGroupMetaData],
    predicates: &[Predicate],
    file_fields: &[DataField],
) -> crate::Result<Option<RowSelection>> {
    if predicates.is_empty() || row_groups.is_empty() {
        return Ok(None);
    }

    // Predicates have already been remapped to file-level indices by the caller
    // (remap_predicates_to_file in reader.rs), so we use an identity mapping here.
    let identity_mapping: Vec<Option<usize>> = (0..file_fields.len()).map(Some).collect();
    let column_indices = build_row_group_column_indices(row_groups[0].columns(), file_fields);
    let mut selectors = Vec::with_capacity(row_groups.len());
    let mut all_selected = true;

    for row_group in row_groups {
        let stats = ParquetRowGroupStats {
            row_group,
            column_indices: &column_indices,
        };
        let may_match =
            predicates_may_match_with_schema(predicates, &stats, &identity_mapping, file_fields);
        if !may_match {
            all_selected = false;
        }
        selectors.push(if may_match {
            RowSelector::select(row_group.num_rows() as usize)
        } else {
            RowSelector::skip(row_group.num_rows() as usize)
        });
    }

    if all_selected {
        Ok(None)
    } else {
        Ok(Some(selectors.into()))
    }
}

fn build_row_group_column_indices(
    columns: &[parquet::file::metadata::ColumnChunkMetaData],
    file_fields: &[DataField],
) -> Vec<Option<usize>> {
    let mut by_root_name: HashMap<&str, Option<usize>> = HashMap::new();
    for (column_index, column) in columns.iter().enumerate() {
        let Some(root_name) = column.column_path().parts().first() else {
            continue;
        };
        let entry = by_root_name
            .entry(root_name.as_str())
            .or_insert(Some(column_index));
        if entry.is_some() && *entry != Some(column_index) {
            *entry = None;
        }
    }

    file_fields
        .iter()
        .map(|field| by_root_name.get(field.name()).copied().flatten())
        .collect()
}

// ---------------------------------------------------------------------------
// Page-index (ColumnIndex / OffsetIndex) pruning
// ---------------------------------------------------------------------------

/// Stats view over one data page of a **single** column, backed by the Parquet
/// ColumnIndex. Plugs into the same [`StatsAccessor`] evaluator as row-group
/// pruning so both layers share identical fail-open semantics.
///
/// Only `target_index` is exposed; every other column reports no stats so its
/// leaves fail open. Parquet pages are laid out per column chunk (different
/// columns may have different page counts and boundaries), so each column must
/// be pruned against its own page layout — see [`build_predicate_page_selection`].
struct ParquetPageStats<'a> {
    /// File-field index this page belongs to.
    target_index: usize,
    /// ColumnIndex of the target column for the current row group.
    column_index: &'a ColumnIndexMetaData,
    page_idx: usize,
    page_row_count: i64,
}

impl StatsAccessor for ParquetPageStats<'_> {
    fn row_count(&self) -> i64 {
        self.page_row_count
    }

    fn null_count(&self, index: usize) -> Option<i64> {
        if index != self.target_index {
            return None;
        }
        self.column_index.null_count(self.page_idx)
    }

    fn min_value(&self, index: usize, data_type: &DataType) -> Option<Datum> {
        if index != self.target_index {
            return None;
        }
        page_index_value_to_datum(self.column_index, self.page_idx, data_type, true)
    }

    fn max_value(&self, index: usize, data_type: &DataType) -> Option<Datum> {
        if index != self.target_index {
            return None;
        }
        page_index_value_to_datum(self.column_index, self.page_idx, data_type, false)
    }
}

/// Decode a per-page min/max from a [`ColumnIndexMetaData`] into a [`Datum`].
///
/// Returns `None` (fail-open: keep the page) for null pages, missing values, or
/// any type that the footer-side path also excludes (decimals, sub-millisecond
/// timestamps).
fn page_index_value_to_datum(
    column_index: &ColumnIndexMetaData,
    page_idx: usize,
    data_type: &DataType,
    is_min: bool,
) -> Option<Datum> {
    if column_index.is_null_page(page_idx) {
        return None;
    }
    macro_rules! primitive {
        ($idx:expr) => {
            if is_min {
                $idx.min_values().get(page_idx)
            } else {
                $idx.max_values().get(page_idx)
            }
        };
    }
    macro_rules! bytes {
        ($idx:expr) => {
            if is_min {
                $idx.min_value(page_idx)
            } else {
                $idx.max_value(page_idx)
            }
        };
    }
    match (column_index, data_type) {
        (ColumnIndexMetaData::BOOLEAN(idx), DataType::Boolean(_)) => {
            primitive!(idx).copied().map(Datum::Bool)
        }
        (ColumnIndexMetaData::INT32(idx), DataType::TinyInt(_)) => primitive!(idx)
            .and_then(|v| i8::try_from(*v).ok())
            .map(Datum::TinyInt),
        (ColumnIndexMetaData::INT32(idx), DataType::SmallInt(_)) => primitive!(idx)
            .and_then(|v| i16::try_from(*v).ok())
            .map(Datum::SmallInt),
        (ColumnIndexMetaData::INT32(idx), DataType::Int(_)) => {
            primitive!(idx).copied().map(Datum::Int)
        }
        (ColumnIndexMetaData::INT32(idx), DataType::Date(_)) => {
            primitive!(idx).copied().map(Datum::Date)
        }
        (ColumnIndexMetaData::INT32(idx), DataType::Time(_)) => {
            primitive!(idx).copied().map(Datum::Time)
        }
        (ColumnIndexMetaData::INT64(idx), DataType::BigInt(_)) => {
            primitive!(idx).copied().map(Datum::Long)
        }
        (ColumnIndexMetaData::INT64(idx), DataType::Timestamp(ts)) if ts.precision() <= 3 => {
            primitive!(idx)
                .copied()
                .map(|millis| Datum::Timestamp { millis, nanos: 0 })
        }
        (ColumnIndexMetaData::INT64(idx), DataType::LocalZonedTimestamp(ts))
            if ts.precision() <= 3 =>
        {
            primitive!(idx)
                .copied()
                .map(|millis| Datum::LocalZonedTimestamp { millis, nanos: 0 })
        }
        (ColumnIndexMetaData::FLOAT(idx), DataType::Float(_)) => {
            primitive!(idx).copied().map(Datum::Float)
        }
        (ColumnIndexMetaData::DOUBLE(idx), DataType::Double(_)) => {
            primitive!(idx).copied().map(Datum::Double)
        }
        (ColumnIndexMetaData::BYTE_ARRAY(idx), DataType::Char(_))
        | (ColumnIndexMetaData::BYTE_ARRAY(idx), DataType::VarChar(_)) => bytes!(idx)
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .map(|s| Datum::String(s.to_string())),
        (ColumnIndexMetaData::BYTE_ARRAY(idx), DataType::Binary(_))
        | (ColumnIndexMetaData::BYTE_ARRAY(idx), DataType::VarBinary(_)) => {
            bytes!(idx).map(|bytes| Datum::Bytes(bytes.to_vec()))
        }
        (ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(idx), DataType::Binary(_))
        | (ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(idx), DataType::VarBinary(_)) => {
            bytes!(idx).map(|bytes| Datum::Bytes(bytes.to_vec()))
        }
        _ => None,
    }
}

/// Build a page-granular [`RowSelection`] from the Parquet ColumnIndex /
/// OffsetIndex by pruning each predicate column against **its own** page layout,
/// then intersecting the per-column selections.
///
/// Parquet splits pages per column chunk, so different columns in a row group
/// may have different page counts and boundaries. Each column is therefore
/// evaluated over its own `OffsetIndex` pages, exposing only that column to the
/// stats evaluator (every other column fails open). Because the top-level
/// predicate list is a conjunction, intersecting the per-column selections is
/// sound.
///
/// Returns `None` when the page index is absent or when no page could be skipped
/// (so the caller leaves the coarser row-group selection untouched). A page is
/// kept whenever its stats are unavailable or the predicate cannot be decided
/// from them — pruning never drops a page it is unsure about.
fn build_predicate_page_selection(
    metadata: &ParquetMetaData,
    predicates: &[Predicate],
    file_fields: &[DataField],
) -> crate::Result<Option<RowSelection>> {
    if predicates.is_empty() {
        return Ok(None);
    }
    let (Some(column_index), Some(offset_index)) =
        (metadata.column_index(), metadata.offset_index())
    else {
        return Ok(None);
    };
    let row_groups = metadata.row_groups();
    if row_groups.is_empty() {
        return Ok(None);
    }

    // Predicates are already remapped to file-level indices by the caller, so an
    // identity mapping suffices (same convention as row-group pruning).
    let identity_mapping: Vec<Option<usize>> = (0..file_fields.len()).map(Some).collect();
    let column_lookup = build_row_group_column_indices(row_groups[0].columns(), file_fields);
    let total_rows: usize = row_groups.iter().map(|rg| rg.num_rows() as usize).sum();

    let mut referenced_fields = Vec::new();
    for predicate in predicates {
        collect_leaf_field_indices(predicate, &mut referenced_fields);
    }
    referenced_fields.sort_unstable();
    referenced_fields.dedup();

    let mut combined: Option<RowSelection> = None;
    for field_index in referenced_fields {
        // The column must resolve to a single parquet column present in the file.
        let Some(parquet_col) = column_lookup.get(field_index).copied().flatten() else {
            continue;
        };

        let mut ranges: Vec<Range<usize>> = Vec::new();
        let mut rg_base = 0usize;
        let mut any_skipped = false;

        for (rg_idx, row_group) in row_groups.iter().enumerate() {
            let rg_rows = row_group.num_rows() as usize;
            let base = rg_base;
            rg_base += rg_rows;

            // Missing page index for this column/row group: keep the whole group
            // (row-group pruning still applied).
            let (Some(col_index), Some(col_offset)) = (
                column_index.get(rg_idx).and_then(|rg| rg.get(parquet_col)),
                offset_index.get(rg_idx).and_then(|rg| rg.get(parquet_col)),
            ) else {
                ranges.push(base..base + rg_rows);
                continue;
            };
            let pages = col_offset.page_locations();
            // Fail open when the column index is absent for this chunk
            // (`ColumnIndexMetaData::NONE` — its accessors panic rather than
            // return `None`), when the two indexes disagree on the page count,
            // or when the page row boundaries are malformed. Either way we
            // cannot safely map page stats to row ranges.
            if pages.is_empty()
                || matches!(col_index, ColumnIndexMetaData::NONE)
                || col_index.num_pages() as usize != pages.len()
                || !page_boundaries_valid(pages, rg_rows)
            {
                ranges.push(base..base + rg_rows);
                continue;
            }

            for (page_idx, page) in pages.iter().enumerate() {
                let page_start = page.first_row_index as usize;
                let page_end = pages
                    .get(page_idx + 1)
                    .map_or(rg_rows, |next| next.first_row_index as usize);
                if page_end <= page_start {
                    continue;
                }
                let stats = ParquetPageStats {
                    target_index: field_index,
                    column_index: col_index,
                    page_idx,
                    page_row_count: (page_end - page_start) as i64,
                };
                if predicates_may_match_with_schema(
                    predicates,
                    &stats,
                    &identity_mapping,
                    file_fields,
                ) {
                    ranges.push(base + page_start..base + page_end);
                } else {
                    any_skipped = true;
                }
            }
        }

        if !any_skipped {
            continue;
        }
        let selection = RowSelection::from_consecutive_ranges(ranges.into_iter(), total_rows);
        combined = Some(match combined {
            Some(prev) => prev.intersection(&selection),
            None => selection,
        });
    }

    Ok(combined)
}

/// Collect the file-field indices referenced by leaf predicates (recursing
/// through compound nodes). Indices are already file-level (see caller).
fn collect_leaf_field_indices(predicate: &Predicate, out: &mut Vec<usize>) {
    match predicate {
        Predicate::Leaf { index, .. } => out.push(*index),
        Predicate::And(children) | Predicate::Or(children) => {
            for child in children {
                collect_leaf_field_indices(child, out);
            }
        }
        Predicate::Not(child) => collect_leaf_field_indices(child, out),
        Predicate::AlwaysTrue | Predicate::AlwaysFalse => {}
    }
}

/// Validate that an OffsetIndex's page row boundaries are well-formed for a row
/// group of `rg_rows` rows: the first page starts at row 0, `first_row_index`
/// is strictly increasing, and every value stays within `[0, rg_rows]`.
///
/// Malformed metadata (negative, non-monotonic, or out-of-range boundaries)
/// would otherwise produce invalid row ranges — huge values from `i64 as usize`
/// underflow, or ranges past the row group — that panic
/// `RowSelection::from_consecutive_ranges`. Callers fail open when this returns
/// `false`.
fn page_boundaries_valid(
    pages: &[parquet::file::page_index::offset_index::PageLocation],
    rg_rows: usize,
) -> bool {
    let rg_rows = rg_rows as i64;
    let mut prev: i64 = -1;
    for (page_idx, page) in pages.iter().enumerate() {
        let first_row = page.first_row_index;
        if page_idx == 0 && first_row != 0 {
            return false;
        }
        if first_row <= prev || first_row > rg_rows {
            return false;
        }
        prev = first_row;
    }
    true
}

// ---------------------------------------------------------------------------
// Parquet statistics → Datum conversion
// ---------------------------------------------------------------------------

fn parquet_stats_to_datum(
    stats: &ParquetStatistics,
    data_type: &DataType,
    is_min: bool,
) -> Option<Datum> {
    let exact = if is_min {
        stats.min_is_exact()
    } else {
        stats.max_is_exact()
    };
    if !exact {
        return None;
    }

    match (stats, data_type) {
        (ParquetStatistics::Boolean(stats), DataType::Boolean(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(Datum::Bool)
        }
        (ParquetStatistics::Int32(stats), DataType::TinyInt(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .and_then(|value| i8::try_from(*value).ok())
                .map(Datum::TinyInt)
        }
        (ParquetStatistics::Int32(stats), DataType::SmallInt(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .and_then(|value| i16::try_from(*value).ok())
                .map(Datum::SmallInt)
        }
        (ParquetStatistics::Int32(stats), DataType::Int(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(Datum::Int)
        }
        (ParquetStatistics::Int32(stats), DataType::Date(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(Datum::Date)
        }
        (ParquetStatistics::Int32(stats), DataType::Time(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(Datum::Time)
        }
        (ParquetStatistics::Int64(stats), DataType::BigInt(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(Datum::Long)
        }
        (ParquetStatistics::Int64(stats), DataType::Timestamp(ts)) if ts.precision() <= 3 => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(|millis| Datum::Timestamp { millis, nanos: 0 })
        }
        (ParquetStatistics::Int64(stats), DataType::LocalZonedTimestamp(ts))
            if ts.precision() <= 3 =>
        {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(|millis| Datum::LocalZonedTimestamp { millis, nanos: 0 })
        }
        (ParquetStatistics::Float(stats), DataType::Float(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(Datum::Float)
        }
        (ParquetStatistics::Double(stats), DataType::Double(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(Datum::Double)
        }
        (ParquetStatistics::ByteArray(stats), DataType::Char(_))
        | (ParquetStatistics::ByteArray(stats), DataType::VarChar(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .and_then(|value| std::str::from_utf8(value.data()).ok())
                .map(|value| Datum::String(value.to_string()))
        }
        (ParquetStatistics::ByteArray(stats), DataType::Binary(_))
        | (ParquetStatistics::ByteArray(stats), DataType::VarBinary(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .map(|value| Datum::Bytes(value.data().to_vec()))
        }
        (ParquetStatistics::FixedLenByteArray(stats), DataType::Binary(_))
        | (ParquetStatistics::FixedLenByteArray(stats), DataType::VarBinary(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .map(|value| Datum::Bytes(value.data().to_vec()))
        }
        _ => None,
    }
}

fn exact_parquet_value<'a, T>(
    is_min: bool,
    min: Option<&'a T>,
    max: Option<&'a T>,
) -> Option<&'a T> {
    if is_min {
        min
    } else {
        max
    }
}

// ---------------------------------------------------------------------------
// Literal → Arrow scalar conversion
// ---------------------------------------------------------------------------

fn literal_scalar_for_parquet_filter(
    literal: &Datum,
    file_data_type: &DataType,
) -> crate::Result<Option<Scalar<ArrayRef>>> {
    let array: ArrayRef = match file_data_type {
        DataType::Boolean(_) => match literal {
            Datum::Bool(value) => Arc::new(BooleanArray::new_scalar(*value).into_inner()),
            _ => return Ok(None),
        },
        DataType::TinyInt(_) => {
            match integer_literal(literal).and_then(|value| i8::try_from(value).ok()) {
                Some(value) => Arc::new(Int8Array::new_scalar(value).into_inner()),
                None => return Ok(None),
            }
        }
        DataType::SmallInt(_) => {
            match integer_literal(literal).and_then(|value| i16::try_from(value).ok()) {
                Some(value) => Arc::new(Int16Array::new_scalar(value).into_inner()),
                None => return Ok(None),
            }
        }
        DataType::Int(_) => {
            match integer_literal(literal).and_then(|value| i32::try_from(value).ok()) {
                Some(value) => Arc::new(Int32Array::new_scalar(value).into_inner()),
                None => return Ok(None),
            }
        }
        DataType::BigInt(_) => {
            match integer_literal(literal).and_then(|value| i64::try_from(value).ok()) {
                Some(value) => Arc::new(Int64Array::new_scalar(value).into_inner()),
                None => return Ok(None),
            }
        }
        DataType::Float(_) => match float32_literal(literal) {
            Some(value) => Arc::new(Float32Array::new_scalar(value).into_inner()),
            None => return Ok(None),
        },
        DataType::Double(_) => match float64_literal(literal) {
            Some(value) => Arc::new(Float64Array::new_scalar(value).into_inner()),
            None => return Ok(None),
        },
        DataType::Char(_) | DataType::VarChar(_) => match literal {
            Datum::String(value) => Arc::new(StringArray::new_scalar(value.as_str()).into_inner()),
            _ => return Ok(None),
        },
        DataType::Binary(_) | DataType::VarBinary(_) => match literal {
            Datum::Bytes(value) => Arc::new(BinaryArray::new_scalar(value.as_slice()).into_inner()),
            _ => return Ok(None),
        },
        DataType::Date(_) => match literal {
            Datum::Date(value) => Arc::new(Date32Array::new_scalar(*value).into_inner()),
            _ => return Ok(None),
        },
        DataType::Decimal(decimal) => match literal {
            Datum::Decimal {
                unscaled,
                precision,
                scale,
            } if *precision <= decimal.precision() && *scale == decimal.scale() => {
                let precision =
                    u8::try_from(decimal.precision()).map_err(|_| Error::Unsupported {
                        message: "Decimal precision exceeds Arrow decimal128 range".to_string(),
                    })?;
                let scale =
                    i8::try_from(decimal.scale() as i32).map_err(|_| Error::Unsupported {
                        message: "Decimal scale exceeds Arrow decimal128 range".to_string(),
                    })?;
                Arc::new(
                    Decimal128Array::new_scalar(*unscaled)
                        .into_inner()
                        .with_precision_and_scale(precision, scale)
                        .map_err(|e| Error::UnexpectedError {
                            message: format!(
                                "Failed to build decimal scalar for parquet row filter: {e}"
                            ),
                            source: Some(Box::new(e)),
                        })?,
                )
            }
            _ => return Ok(None),
        },
        DataType::Time(_)
        | DataType::Timestamp(_)
        | DataType::LocalZonedTimestamp(_)
        | DataType::Variant(_)
        | DataType::Blob(_)
        | DataType::Array(_)
        | DataType::Map(_)
        | DataType::Multiset(_)
        | DataType::Row(_)
        | DataType::Vector(_) => return Ok(None),
    };

    Ok(Some(Scalar::new(array)))
}

fn integer_literal(literal: &Datum) -> Option<i128> {
    match literal {
        Datum::TinyInt(value) => Some(i128::from(*value)),
        Datum::SmallInt(value) => Some(i128::from(*value)),
        Datum::Int(value) => Some(i128::from(*value)),
        Datum::Long(value) => Some(i128::from(*value)),
        _ => None,
    }
}

fn float32_literal(literal: &Datum) -> Option<f32> {
    match literal {
        Datum::Float(value) => Some(*value),
        Datum::Double(value) => {
            let casted = *value as f32;
            ((casted as f64) == *value).then_some(casted)
        }
        _ => None,
    }
}

fn float64_literal(literal: &Datum) -> Option<f64> {
    match literal {
        Datum::Float(value) => Some(f64::from(*value)),
        Datum::Double(value) => Some(*value),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Row selection helpers (DV, row ranges)
// ---------------------------------------------------------------------------

fn intersect_optional_row_selections(
    left: Option<RowSelection>,
    right: Option<RowSelection>,
) -> Option<RowSelection> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.intersection(&right)),
        (Some(selection), None) | (None, Some(selection)) => Some(selection),
        (None, None) => None,
    }
}

/// Build a Parquet [RowSelection] from inclusive `[from, to]` file-local row ranges (0-based).
fn build_row_ranges_selection(
    row_group_metadata_list: &[RowGroupMetaData],
    row_ranges: &[RowRange],
) -> RowSelection {
    let total_rows: i64 = row_group_metadata_list.iter().map(|rg| rg.num_rows()).sum();
    if total_rows == 0 {
        return vec![].into();
    }

    let file_end = total_rows - 1;
    let mut local_ranges: Vec<(usize, usize)> = row_ranges
        .iter()
        .filter_map(|r| {
            if r.to() < 0 || r.from() > file_end {
                return None;
            }
            let local_start = r.from().max(0) as usize;
            let local_end = (r.to().min(file_end) + 1) as usize;
            Some((local_start, local_end))
        })
        .collect();
    local_ranges.sort_by_key(|&(s, _)| s);

    let mut selectors: Vec<RowSelector> = Vec::new();
    let mut cursor: usize = 0;
    for (start, end) in &local_ranges {
        if *start > cursor {
            selectors.push(RowSelector::skip(*start - cursor));
        }
        let select_start = (*start).max(cursor);
        if *end > select_start {
            selectors.push(RowSelector::select(*end - select_start));
        }
        cursor = cursor.max(*end);
    }
    let total = total_rows as usize;
    if cursor < total {
        selectors.push(RowSelector::skip(total - cursor));
    }
    selectors.into()
}

// ---------------------------------------------------------------------------
// ArrowFileReader — async Parquet IO adapter
// ---------------------------------------------------------------------------

/// ArrowFileReader is a wrapper around a FileRead that impls parquets AsyncFileReader.
///
/// # TODO
///
/// [ParquetObjectReader](https://docs.rs/parquet/latest/src/parquet/arrow/async_reader/store.rs.html#64)
/// contains the following hints to speed up metadata loading, similar to iceberg, we can consider adding them to this struct:
///
/// - `metadata_size_hint`: Provide a hint as to the size of the parquet file's footer.
/// - `preload_column_index`: Load the Column Index  as part of [`Self::get_metadata`].
/// - `preload_offset_index`: Load the Offset Index as part of [`Self::get_metadata`].
struct ArrowFileReader {
    file_size: u64,
    r: Box<dyn FileRead>,
}

/// coalesce threshold: 1 MiB.
const RANGE_COALESCE_BYTES: u64 = 1024 * 1024;
/// concurrent range fetches.
const RANGE_FETCH_CONCURRENCY: usize = 10;
/// metadata prefetch hint: 512 KiB.
const METADATA_SIZE_HINT: usize = 512 * 1024;
/// Minimum range size for splitting: 4 MiB.
/// The block size used for split alignment and as the minimum split
/// granularity.  Ranges smaller than this will not be split further to
/// avoid excessive small IO requests whose per-request overhead dominates.
const IO_BLOCK_SIZE: u64 = 4 * 1024 * 1024;

impl ArrowFileReader {
    fn new(file_size: u64, r: Box<dyn FileRead>) -> Self {
        Self { file_size, r }
    }

    fn read_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        Box::pin(self.r.read(range.start..range.end).map_err(|err| {
            let err_msg = format!("{err}");
            parquet::errors::ParquetError::External(err_msg.into())
        }))
    }
}

impl MetadataFetch for ArrowFileReader {
    fn fetch(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.read_bytes(range)
    }
}

impl AsyncFileReader for ArrowFileReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.read_bytes(range)
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        let coalesce_bytes = RANGE_COALESCE_BYTES;
        let concurrency = RANGE_FETCH_CONCURRENCY;

        async move {
            if ranges.is_empty() {
                return Ok(vec![]);
            }

            // Two-phase range optimization:
            // Phase 1: Merge nearby ranges based on coalesce threshold.
            let coalesced = merge_byte_ranges(&ranges, coalesce_bytes);
            // Phase 2: Split large merged ranges to utilize concurrency,
            // but only at original range boundaries.
            let fetch_ranges = split_ranges_for_concurrency(coalesced, concurrency);

            // Fetch merged ranges concurrently.
            let r = &self.r;
            let fetched: Vec<Bytes> = if fetch_ranges.len() <= concurrency {
                // All ranges fit within the concurrency limit — fire them all at once.
                futures::future::try_join_all(fetch_ranges.iter().map(|range| {
                    r.read(range.clone())
                        .map_err(|e| parquet::errors::ParquetError::External(format!("{e}").into()))
                }))
                .await?
            } else {
                // More ranges than concurrency slots — use buffered stream.
                futures::stream::iter(fetch_ranges.iter().cloned())
                    .map(|range| async move {
                        r.read(range).await.map_err(|e| {
                            parquet::errors::ParquetError::External(format!("{e}").into())
                        })
                    })
                    .buffered(concurrency)
                    .try_collect()
                    .await?
            };

            // Slice the fetched data back into the originally requested
            // ranges.  A single original range may span multiple fetch
            // chunks, so we copy from as many chunks as needed.
            let result: parquet::errors::Result<Vec<Bytes>> = ranges
                .iter()
                .map(|range| {
                    // Find the first fetch chunk whose end is past range.start.
                    let first = fetch_ranges.partition_point(|v| v.end <= range.start);
                    if first >= fetch_ranges.len() {
                        return Err(parquet::errors::ParquetError::General(format!(
                            "No fetch range covers requested range {}..{}",
                            range.start, range.end
                        )));
                    }

                    let need = (range.end - range.start) as usize;

                    // Fast path: the original range fits entirely within one
                    // fetch chunk — zero-copy slice.
                    let fr = &fetch_ranges[first];
                    if range.end <= fr.end {
                        let start = (range.start - fr.start) as usize;
                        let end = (range.end - fr.start) as usize;
                        return Ok(fetched[first].slice(start..end));
                    }

                    // Slow path: the original range spans multiple fetch
                    // chunks — copy pieces into a new buffer (mirrors Java's
                    // copyMultiBytesToBytes).
                    let mut buf = Vec::with_capacity(need);
                    let mut pos = range.start;
                    for i in first..fetch_ranges.len() {
                        if pos >= range.end {
                            break;
                        }
                        let fr = &fetch_ranges[i];
                        let chunk = &fetched[i];
                        let src_start = (pos - fr.start) as usize;
                        let src_end = ((range.end.min(fr.end)) - fr.start) as usize;
                        if src_end > chunk.len() {
                            return Err(parquet::errors::ParquetError::General(format!(
                                "Fetched data too short for range {}..{}: \
                                 chunk {}..{} has {} bytes, need up to offset {}",
                                range.start,
                                range.end,
                                fr.start,
                                fr.end,
                                chunk.len(),
                                src_end,
                            )));
                        }
                        buf.extend_from_slice(&chunk[src_start..src_end]);
                        pos = fr.end;
                    }
                    if buf.len() != need {
                        return Err(parquet::errors::ParquetError::General(format!(
                            "Assembled {} bytes for range {}..{}, expected {}",
                            buf.len(),
                            range.start,
                            range.end,
                            need,
                        )));
                    }
                    Ok(Bytes::from(buf))
                })
                .collect();
            result
        }
        .boxed()
    }

    fn get_metadata(
        &mut self,
        options: Option<&ArrowReaderOptions>,
    ) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        let metadata_opts = options.map(|o| o.metadata_options().clone());
        // The page-index policies live on `ArrowReaderOptions` directly, not
        // inside `metadata_options`, so they must be forwarded explicitly (the
        // upstream default `AsyncFileReader::get_metadata` does the same).
        // Without this, `with_page_index_policy` would silently no-op here and
        // no page index would ever be loaded.
        let column_index_policy = options.map(|o| o.column_index_policy());
        let offset_index_policy = options.map(|o| o.offset_index_policy());
        let prefetch_hint = Some(METADATA_SIZE_HINT);
        Box::pin(async move {
            let file_size = self.file_size;
            let mut reader = ParquetMetaDataReader::new()
                .with_prefetch_hint(prefetch_hint)
                .with_metadata_options(metadata_opts);
            if let Some(policy) = column_index_policy {
                reader = reader.with_column_index_policy(policy);
            }
            if let Some(policy) = offset_index_policy {
                reader = reader.with_offset_index_policy(policy);
            }
            let metadata = reader.load_and_finish(self, file_size).await?;
            Ok(Arc::new(metadata))
        })
    }
}

// ---------------------------------------------------------------------------
// Range coalescing
// ---------------------------------------------------------------------------

/// Merge nearby byte ranges to reduce the number of requests.
///
/// Ranges whose gap is ≤ `coalesce` bytes are merged into a single range.
/// The input does not need to be sorted.
fn merge_byte_ranges(ranges: &[Range<u64>], coalesce: u64) -> Vec<Range<u64>> {
    if ranges.is_empty() {
        return vec![];
    }

    let mut sorted = ranges.to_vec();
    sorted.sort_unstable_by_key(|r| r.start);

    let mut merged = Vec::with_capacity(sorted.len());
    let mut start_idx = 0;
    let mut end_idx = 1;

    while start_idx != sorted.len() {
        let mut range_end = sorted[start_idx].end;

        while end_idx != sorted.len()
            && sorted[end_idx]
                .start
                .checked_sub(range_end)
                .map(|delta| delta <= coalesce)
                .unwrap_or(true)
        {
            range_end = range_end.max(sorted[end_idx].end);
            end_idx += 1;
        }

        merged.push(sorted[start_idx].start..range_end);
        start_idx = end_idx;
        end_idx += 1;
    }

    merged
}

/// Split merged ranges into fixed-size batches to utilize concurrency,
/// Each merged range is divided into chunks of `expected_size`,
/// with the last chunk taking whatever remains.
/// Ranges smaller than `2 * IO_BLOCK_SIZE` are kept as-is to
/// avoid excessive small IO requests.
fn split_ranges_for_concurrency(merged: Vec<Range<u64>>, concurrency: usize) -> Vec<Range<u64>> {
    if merged.is_empty() || concurrency <= 1 {
        return merged;
    }

    let mut result = Vec::with_capacity(merged.len());

    for range in &merged {
        let length = range.end - range.start;
        let raw_size = IO_BLOCK_SIZE.max(length.div_ceil(concurrency as u64));
        // Round up to the nearest multiple of IO_BLOCK_SIZE (4 MB) so that
        // every split boundary is 4 MB-aligned relative to the range start.
        let expected_size = raw_size.div_ceil(IO_BLOCK_SIZE) * IO_BLOCK_SIZE;
        let min_tail_size = expected_size.max(IO_BLOCK_SIZE * 2);

        let mut offset = range.start;
        let end = range.end;

        // Align the first split boundary: if `offset` is not 4 MB-aligned,
        // emit a short head chunk so that all subsequent chunks start on a
        // 4 MB boundary.
        let misalign = offset % IO_BLOCK_SIZE;
        if misalign != 0 {
            let first_end = (offset - misalign + IO_BLOCK_SIZE).min(end);
            result.push(offset..first_end);
            offset = first_end;
        }

        loop {
            if offset >= end {
                break;
            }
            if end - offset < min_tail_size {
                result.push(offset..end);
                break;
            } else {
                result.push(offset..offset + expected_size);
                offset += expected_size;
            }
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::build_parquet_row_filter;
    use super::ParquetFormatWriter;
    use super::{
        AsyncArrowWriter, PageIndexPolicy, ParquetMetaDataReader, Predicate, PredicateOperator,
        RowSelection,
    };
    use crate::arrow::format::{create_format_reader, create_format_writer, FormatFileWriter};
    use crate::arrow::{build_target_arrow_schema, variant_arrow_type};
    use crate::io::FileIOBuilder;
    use crate::spec::{DataField, DataType, Datum, IntType, PredicateBuilder, VariantType};
    use crate::variant::GenericVariant;
    use arrow_array::{BinaryArray, Int32Array, RecordBatch, StructArray};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use futures::TryStreamExt;
    use parquet::schema::{parser::parse_message_type, types::SchemaDescriptor};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn test_fields() -> Vec<DataField> {
        vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "score".to_string(), DataType::Int(IntType::new())),
        ]
    }

    fn test_parquet_schema() -> SchemaDescriptor {
        SchemaDescriptor::new(Arc::new(
            parse_message_type(
                "
                message test_schema {
                  OPTIONAL INT32 id;
                  OPTIONAL INT32 score;
                }
                ",
            )
            .expect("test schema should parse"),
        ))
    }

    #[test]
    fn test_build_parquet_row_filter_supports_null_and_membership_predicates() {
        let fields = test_fields();
        let builder = PredicateBuilder::new(&fields);
        let predicates = vec![
            builder
                .is_null("id")
                .expect("is null predicate should build"),
            builder
                .is_in("score", vec![Datum::Int(7)])
                .expect("in predicate should build"),
            builder
                .is_not_in("score", vec![Datum::Int(9)])
                .expect("not in predicate should build"),
        ];

        let row_filter = build_parquet_row_filter(&test_parquet_schema(), &predicates, &fields)
            .expect("parquet row filter should build");

        assert!(row_filter.is_some());
    }

    // -----------------------------------------------------------------------
    // String predicate tests (StartsWith / EndsWith / Contains)
    // -----------------------------------------------------------------------

    fn run_string_op(
        op: super::PredicateOperator,
        column: arrow_array::ArrayRef,
        pattern: &str,
    ) -> arrow_array::BooleanArray {
        use crate::spec::VarCharType;
        let dt = DataType::VarChar(VarCharType::default());
        super::evaluate_exact_leaf_predicate(
            &column,
            &dt,
            op,
            &[Datum::String(pattern.to_string())],
        )
        .expect("string op should evaluate")
    }

    #[test]
    fn test_evaluate_starts_with_string_array() {
        use arrow_array::StringArray;
        let arr: arrow_array::ArrayRef = Arc::new(StringArray::from(vec![
            Some("foo"),
            Some("foobar"),
            Some("baz"),
            None,
        ]));
        let mask = run_string_op(super::PredicateOperator::StartsWith, arr, "foo");
        let expected = arrow_array::BooleanArray::from(vec![true, true, false, false]);
        assert_eq!(mask, expected);
    }

    #[test]
    fn test_evaluate_ends_with_large_string_array() {
        use arrow_array::LargeStringArray;
        let arr: arrow_array::ArrayRef = Arc::new(LargeStringArray::from(vec![
            Some("hello"),
            Some("world"),
            Some("ello"),
            None,
        ]));
        let mask = run_string_op(super::PredicateOperator::EndsWith, arr, "ello");
        let expected = arrow_array::BooleanArray::from(vec![true, false, true, false]);
        assert_eq!(mask, expected);
    }

    #[test]
    fn test_evaluate_contains_string_view_array() {
        use arrow_array::StringViewArray;
        let arr: arrow_array::ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("apple pie"),
            Some("banana"),
            Some("crab apple"),
            None,
        ]));
        let mask = run_string_op(super::PredicateOperator::Contains, arr, "apple");
        let expected = arrow_array::BooleanArray::from(vec![true, false, true, false]);
        assert_eq!(mask, expected);
    }

    #[test]
    fn test_evaluate_like_pattern_with_underscore_and_percent() {
        use arrow_array::StringArray;
        let arr: arrow_array::ArrayRef = Arc::new(StringArray::from(vec![
            Some("foobar"),
            Some("foox"),
            Some("zoobar"),
            None,
        ]));
        // f_o% matches "foobar" (f-o-o then anything) and "foox" (f-o-o then x)
        // but not "zoobar".
        let mask = run_string_op(super::PredicateOperator::Like, arr, "f_o%");
        let expected = arrow_array::BooleanArray::from(vec![true, true, false, false]);
        assert_eq!(mask, expected);
    }

    #[test]
    fn test_evaluate_like_escaped_percent_treated_literally() {
        use arrow_array::StringArray;
        let arr: arrow_array::ArrayRef =
            Arc::new(StringArray::from(vec![Some("100%"), Some("1000"), None]));
        let mask = run_string_op(super::PredicateOperator::Like, arr, r"100\%");
        let expected = arrow_array::BooleanArray::from(vec![true, false, false]);
        assert_eq!(mask, expected);
    }

    // -----------------------------------------------------------------------
    // BETWEEN / NOT BETWEEN row-filter tests
    // -----------------------------------------------------------------------

    fn run_between(
        op: super::PredicateOperator,
        column: arrow_array::ArrayRef,
        low: i32,
        high: i32,
    ) -> arrow_array::BooleanArray {
        let dt = DataType::Int(IntType::new());
        super::evaluate_exact_leaf_predicate(&column, &dt, op, &[Datum::Int(low), Datum::Int(high)])
            .expect("BETWEEN should evaluate")
    }

    #[test]
    fn test_evaluate_between_int_array() {
        let arr: arrow_array::ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(5),
            Some(10),
            Some(11),
            None,
        ]));
        let mask = run_between(super::PredicateOperator::Between, arr, 5, 10);
        let expected = arrow_array::BooleanArray::from(vec![false, true, true, false, false]);
        assert_eq!(mask, expected);
    }

    #[test]
    fn test_evaluate_not_between_treats_null_as_false() {
        let arr: arrow_array::ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(5),
            Some(10),
            Some(11),
            None,
        ]));
        let mask = run_between(super::PredicateOperator::NotBetween, arr, 5, 10);
        // NULL → false (residual filter convention; matches sanitize_filter_mask).
        let expected = arrow_array::BooleanArray::from(vec![true, false, false, true, false]);
        assert_eq!(mask, expected);
    }

    // -----------------------------------------------------------------------
    // merge_byte_ranges tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_merge_byte_ranges_empty() {
        assert_eq!(
            super::merge_byte_ranges(&[], 1024),
            Vec::<std::ops::Range<u64>>::new()
        );
    }

    #[test]
    fn test_merge_byte_ranges_no_coalesce() {
        // Ranges far apart should not be merged
        let ranges = vec![0..100, 1_000_000..1_000_100];
        let merged = super::merge_byte_ranges(&ranges, 1024);
        assert_eq!(merged, vec![0..100, 1_000_000..1_000_100]);
    }

    #[test]
    fn test_merge_byte_ranges_coalesce() {
        // Ranges within the gap threshold should be merged
        let ranges = vec![0..100, 200..300, 500..600];
        let merged = super::merge_byte_ranges(&ranges, 1024);
        assert_eq!(merged, vec![0..600]);
    }

    #[test]
    fn test_merge_byte_ranges_zero_coalesce_gap() {
        // With coalesce=0, ranges with a 1-byte gap should NOT merge
        let ranges = vec![0..100, 101..200];
        let merged = super::merge_byte_ranges(&ranges, 0);
        assert_eq!(merged, vec![0..100, 101..200]);
    }

    // -----------------------------------------------------------------------
    // split_ranges_for_concurrency tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_split_aligned_range_0_to_20mb() {
        // 0..20MB, concurrency=4:
        //   raw_size = max(4MB, 5MB+1) = 5MB+1
        //   expected_size = ceil((5MB+1)/4MB)*4MB = 8MB
        //   min_tail_size = max(8MB, 8MB) = 8MB
        //   No misalign. Chunks: [0..8, 8..16, 16..20]
        let mb = 1024 * 1024u64;
        #[allow(clippy::single_range_in_vec_init)]
        let merged = vec![0..20 * mb];
        let result = super::split_ranges_for_concurrency(merged, 4);
        assert_eq!(result, vec![0..8 * mb, 8 * mb..16 * mb, 16 * mb..20 * mb]);
    }

    #[test]
    fn test_split_unaligned_start_6_to_14mb() {
        // 6MB..14MB, concurrency=4:
        //   raw_size = max(4MB, 2MB+1) = 4MB
        //   expected_size = 4MB, min_tail_size = 8MB
        //   Head: 6..8MB. Loop: 8+8=16 > 14 → tail 8..14.
        //   Result: [6..8, 8..14]
        let mb = 1024 * 1024u64;
        #[allow(clippy::single_range_in_vec_init)]
        let merged = vec![6 * mb..14 * mb];
        let result = super::split_ranges_for_concurrency(merged, 4);
        assert_eq!(result, vec![6 * mb..8 * mb, 8 * mb..14 * mb]);
    }

    #[test]
    fn test_split_unaligned_start_6_to_22mb() {
        // 6MB..22MB, concurrency=4:
        //   raw_size = max(4MB, ceil(16MB/4)) = 4MB
        //   expected_size = ceil(4MB/4MB)*4MB = 4MB
        //   min_tail_size = max(4MB, 8MB) = 8MB
        //   Head: 6..8MB (misalign=2MB).
        //   Loop: 22-8=14≥8 → 8..12; 22-12=10≥8 → 12..16; 22-16=6<8 → tail 16..22.
        //   Result: [6..8, 8..12, 12..16, 16..22]
        let mb = 1024 * 1024u64;
        #[allow(clippy::single_range_in_vec_init)]
        let merged = vec![6 * mb..22 * mb];
        let result = super::split_ranges_for_concurrency(merged, 4);
        assert_eq!(
            result,
            vec![
                6 * mb..8 * mb,
                8 * mb..12 * mb,
                12 * mb..16 * mb,
                16 * mb..22 * mb,
            ]
        );
    }

    #[test]
    fn test_split_already_aligned_8_to_24mb() {
        // 8MB..24MB, concurrency=4:
        //   raw_size = max(4MB, ceil(16MB/4)) = 4MB
        //   expected_size = 4MB, min_tail_size = 8MB
        //   No misalign.
        //   Loop: 24-8=16≥8 → 8..12; 24-12=12≥8 → 12..16; 24-16=8≥8 → 16..20; 24-20=4<8 → tail 20..24.
        //   Result: [8..12, 12..16, 16..20, 20..24]
        let mb = 1024 * 1024u64;
        #[allow(clippy::single_range_in_vec_init)]
        let merged = vec![8 * mb..24 * mb];
        let result = super::split_ranges_for_concurrency(merged, 4);
        assert_eq!(
            result,
            vec![
                8 * mb..12 * mb,
                12 * mb..16 * mb,
                16 * mb..20 * mb,
                20 * mb..24 * mb,
            ]
        );
    }

    #[test]
    fn test_split_multiple_ranges() {
        // [0..20MB, 24..44MB], concurrency=4:
        //   Range 0..20MB → [0..8, 8..16, 16..20] (same as test above)
        //   Range 24..44MB (20MB): expected_size=8MB, min_tail_size=8MB, no misalign.
        //     24+8=32 ≤ 44 → 24..32; 32+8=40 ≤ 44 → 32..40; 40+8=48 > 44 → tail 40..44.
        //   Result: [0..8, 8..16, 16..20, 24..32, 32..40, 40..44]
        let mb = 1024 * 1024u64;
        let merged = vec![0..20 * mb, 24 * mb..44 * mb];
        let result = super::split_ranges_for_concurrency(merged, 4);
        assert_eq!(
            result,
            vec![
                0..8 * mb,
                8 * mb..16 * mb,
                16 * mb..20 * mb,
                24 * mb..32 * mb,
                32 * mb..40 * mb,
                40 * mb..44 * mb,
            ]
        );
    }

    #[test]
    fn test_split_empty() {
        let merged: Vec<std::ops::Range<u64>> = vec![];
        let result = super::split_ranges_for_concurrency(merged, 4);
        assert!(result.is_empty());
    }

    fn writer_arrow_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("value", ArrowDataType::Int32, false),
        ]))
    }

    fn writer_test_batch(
        schema: &Arc<ArrowSchema>,
        ids: Vec<i32>,
        values: Vec<i32>,
    ) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int32Array::from(values)),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_parquet_writer_write_and_close() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/test_parquet_writer_write_close.parquet";
        let output = file_io.new_output(path).unwrap();
        let schema = writer_arrow_schema();

        let mut writer: Box<dyn FormatFileWriter> = Box::new(
            ParquetFormatWriter::new(&output, schema.clone(), "zstd", 1)
                .await
                .unwrap(),
        );

        let batch = writer_test_batch(&schema, vec![1, 2, 3], vec![10, 20, 30]);
        writer.write(&batch).await.unwrap();
        writer.close().await.unwrap();

        // Verify valid parquet by reading back
        let bytes = file_io.new_input(path).unwrap().read().await.unwrap();
        let reader =
            parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(bytes, 1024).unwrap();
        let total_rows: usize = reader.into_iter().map(|r| r.unwrap().num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_parquet_writer_multiple_batches() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/test_parquet_writer_multi.parquet";
        let output = file_io.new_output(path).unwrap();
        let schema = writer_arrow_schema();

        let mut writer: Box<dyn FormatFileWriter> = Box::new(
            ParquetFormatWriter::new(&output, schema.clone(), "zstd", 1)
                .await
                .unwrap(),
        );

        writer
            .write(&writer_test_batch(&schema, vec![1, 2], vec![10, 20]))
            .await
            .unwrap();
        writer
            .write(&writer_test_batch(&schema, vec![3, 4, 5], vec![30, 40, 50]))
            .await
            .unwrap();
        writer.close().await.unwrap();

        let bytes = file_io.new_input(path).unwrap().read().await.unwrap();
        let reader =
            parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(bytes, 1024).unwrap();
        let total_rows: usize = reader.into_iter().map(|r| r.unwrap().num_rows()).sum();
        assert_eq!(total_rows, 5);
    }

    #[tokio::test]
    async fn test_parquet_inline_fixed_size_list_roundtrip() {
        use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
        use arrow_array::{Array, FixedSizeListArray, Float32Array};

        // Build a FixedSizeList<Float32, 2> column: row 0 = [1.0, 2.0], row 1 = null.
        let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), 2).with_field(Arc::new(
            ArrowField::new("element", ArrowDataType::Float32, true),
        ));
        builder.values().append_value(1.0);
        builder.values().append_value(2.0);
        builder.append(true);
        builder.values().append_value(0.0);
        builder.values().append_value(0.0);
        builder.append(false); // null vector row
        let vec_array = builder.finish();

        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "embedding",
            ArrowDataType::FixedSizeList(
                Arc::new(ArrowField::new("element", ArrowDataType::Float32, true)),
                2,
            ),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(vec_array)]).unwrap();

        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/test_parquet_inline_vector.parquet";
        let output = file_io.new_output(path).unwrap();
        let mut writer: Box<dyn FormatFileWriter> = Box::new(
            ParquetFormatWriter::new(&output, schema.clone(), "zstd", 1)
                .await
                .unwrap(),
        );
        writer.write(&batch).await.unwrap();
        writer.close().await.unwrap();

        let bytes = file_io.new_input(path).unwrap().read().await.unwrap();
        let reader =
            parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(bytes, 1024).unwrap();
        let batches: Vec<RecordBatch> = reader.into_iter().map(|r| r.unwrap()).collect();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);

        let col = batches[0].column(0);
        let fsl = col
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("column should be FixedSizeListArray");
        assert_eq!(fsl.value_length(), 2);
        assert!(fsl.is_valid(0));
        assert!(fsl.is_null(1)); // null vector row preserved

        let row0 = fsl.value(0);
        let floats = row0
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("child should be Float32Array");
        assert_eq!(floats.values(), &[1.0, 2.0]);
    }

    #[tokio::test]
    async fn test_parquet_variant_shredding_write_read_roundtrip() {
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "v".to_string(), DataType::Variant(VariantType::new())),
        ];
        let options = HashMap::from([(
            "parquet.variant.shreddingSchema".to_string(),
            r#"{"type":"ROW","fields":[{"name":"v","type":{"type":"ROW","fields":[{"name":"age","type":"BIGINT"},{"name":"city","type":"STRING"}]}}]}"#.to_string(),
        )]);
        let variants = [
            GenericVariant::parse_json(r#"{"age":27,"city":"Beijing"}"#).unwrap(),
            GenericVariant::parse_json(r#"{"age":27}"#).unwrap(),
            GenericVariant::parse_json(r#"{"city":"Beijing","other":"xxx"}"#).unwrap(),
            GenericVariant::parse_json(r#"{"age":"27"}"#).unwrap(),
        ];
        let value_items = variants
            .iter()
            .map(|variant| Some(variant.value()))
            .collect::<Vec<_>>();
        let metadata_items = variants
            .iter()
            .map(|variant| Some(variant.metadata()))
            .collect::<Vec<_>>();
        let variant_fields = match variant_arrow_type() {
            arrow_schema::DataType::Struct(fields) => fields,
            _ => unreachable!("variant_arrow_type is a struct"),
        };
        let variant_column = StructArray::try_new(
            variant_fields,
            vec![
                Arc::new(BinaryArray::from(value_items)),
                Arc::new(BinaryArray::from(metadata_items)),
            ],
            None,
        )
        .unwrap();
        let logical_schema = build_target_arrow_schema(&fields).unwrap();
        let batch = RecordBatch::try_new(
            logical_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(variant_column),
            ],
        )
        .unwrap();

        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = format!("memory:/variant_shredding_{}.parquet", uuid::Uuid::new_v4());
        let output = file_io.new_output(&path).unwrap();
        let mut writer = create_format_writer(
            &output,
            logical_schema,
            "zstd",
            1,
            None,
            Some(&fields),
            Some(&options),
        )
        .await
        .unwrap();
        writer.write(&batch).await.unwrap();
        let file_size = writer.close().await.unwrap();

        let raw_bytes = file_io.new_input(&path).unwrap().read().await.unwrap();
        let raw_batches =
            parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(raw_bytes, 1024)
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap();
        let raw_variant_column = raw_batches[0].column_by_name("v").unwrap();
        let arrow_schema::DataType::Struct(raw_variant_fields) = raw_variant_column.data_type()
        else {
            panic!("expected physical variant column to be a struct");
        };
        assert!(raw_variant_fields
            .iter()
            .any(|field| field.name() == "typed_value"));

        let input = file_io.new_input(&path).unwrap();
        let file_reader = input.reader().await.unwrap();
        let reader = create_format_reader(&path, false, &fields).unwrap();
        let batches = reader
            .read_batch_stream(Box::new(file_reader), file_size, &fields, None, None, None)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);
        let payload = batches[0]
            .column_by_name("v")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let values = payload
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let metadata = payload
            .column_by_name("metadata")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        for (idx, expected) in variants.iter().enumerate() {
            let actual = GenericVariant::from_parts(
                values.value(idx).to_vec(),
                metadata.value(idx).to_vec(),
            )
            .unwrap();
            assert_eq!(actual.to_json().unwrap(), expected.to_json().unwrap());
        }
    }

    #[tokio::test]
    async fn test_parquet_variant_infer_shredding_write_read_roundtrip() {
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "v".to_string(), DataType::Variant(VariantType::new())),
        ];
        let options = HashMap::from([
            (
                "variant.inferShreddingSchema".to_string(),
                "true".to_string(),
            ),
            (
                "variant.shredding.maxInferBufferRow".to_string(),
                "2".to_string(),
            ),
        ]);
        let first_variants = vec![
            GenericVariant::parse_json(r#"{"age":27,"city":"Beijing"}"#).unwrap(),
            GenericVariant::parse_json(r#"{"age":28,"city":"Hangzhou"}"#).unwrap(),
        ];
        let late_variants =
            vec![GenericVariant::parse_json(r#"{"age":"old","city":"Shanghai"}"#).unwrap()];
        let logical_schema = build_target_arrow_schema(&fields).unwrap();
        let make_batch = |ids: Vec<i32>, variants: &[GenericVariant]| {
            let value_items = variants
                .iter()
                .map(|variant| Some(variant.value()))
                .collect::<Vec<_>>();
            let metadata_items = variants
                .iter()
                .map(|variant| Some(variant.metadata()))
                .collect::<Vec<_>>();
            let variant_fields = match variant_arrow_type() {
                arrow_schema::DataType::Struct(fields) => fields,
                _ => unreachable!("variant_arrow_type is a struct"),
            };
            let variant_column = StructArray::try_new(
                variant_fields,
                vec![
                    Arc::new(BinaryArray::from(value_items)),
                    Arc::new(BinaryArray::from(metadata_items)),
                ],
                None,
            )
            .unwrap();
            RecordBatch::try_new(
                logical_schema.clone(),
                vec![Arc::new(Int32Array::from(ids)), Arc::new(variant_column)],
            )
            .unwrap()
        };

        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = format!(
            "memory:/variant_infer_shredding_{}.parquet",
            uuid::Uuid::new_v4()
        );
        let output = file_io.new_output(&path).unwrap();
        let mut writer = create_format_writer(
            &output,
            logical_schema.clone(),
            "zstd",
            1,
            None,
            Some(&fields),
            Some(&options),
        )
        .await
        .unwrap();
        writer
            .write(&make_batch(vec![1, 2], &first_variants))
            .await
            .unwrap();
        writer
            .write(&make_batch(vec![3], &late_variants))
            .await
            .unwrap();
        let file_size = writer.close().await.unwrap();

        let raw_bytes = file_io.new_input(&path).unwrap().read().await.unwrap();
        let raw_batches =
            parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(raw_bytes, 1024)
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap();
        let raw_variant_column = raw_batches[0].column_by_name("v").unwrap();
        let arrow_schema::DataType::Struct(raw_variant_fields) = raw_variant_column.data_type()
        else {
            panic!("expected physical variant column to be a struct");
        };
        assert!(raw_variant_fields
            .iter()
            .any(|field| field.name() == "typed_value"));

        let input = file_io.new_input(&path).unwrap();
        let file_reader = input.reader().await.unwrap();
        let reader = create_format_reader(&path, false, &fields).unwrap();
        let batches = reader
            .read_batch_stream(Box::new(file_reader), file_size, &fields, None, None, None)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);
        let payload = batches[0]
            .column_by_name("v")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let values = payload
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let metadata = payload
            .column_by_name("metadata")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let expected = first_variants
            .iter()
            .chain(late_variants.iter())
            .collect::<Vec<_>>();
        for (idx, expected) in expected.iter().enumerate() {
            let actual = GenericVariant::from_parts(
                values.value(idx).to_vec(),
                metadata.value(idx).to_vec(),
            )
            .unwrap();
            assert_eq!(actual.to_json().unwrap(), expected.to_json().unwrap());
        }
    }

    // -----------------------------------------------------------------------
    // Page-index (ColumnIndex / OffsetIndex) pruning
    // -----------------------------------------------------------------------

    /// Write a single row group split into `total_rows / page_row_limit` data
    /// pages. `id` runs 0..total_rows so page `p` covers ids
    /// `[p*page_row_limit, (p+1)*page_row_limit)`.
    async fn write_multi_page_parquet(page_row_limit: usize, total_rows: i32) -> Vec<u8> {
        let schema = writer_arrow_schema();
        let props = parquet::file::properties::WriterProperties::builder()
            .set_data_page_row_count_limit(page_row_limit)
            .set_write_batch_size(page_row_limit)
            .set_max_row_group_row_count(Some(total_rows as usize))
            .build();
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer =
                AsyncArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).unwrap();
            let ids: Vec<i32> = (0..total_rows).collect();
            let values: Vec<i32> = ids.iter().map(|v| v * 10).collect();
            writer
                .write(&writer_test_batch(&schema, ids, values))
                .await
                .unwrap();
            writer.close().await.unwrap();
        }
        buf
    }

    /// Parse metadata from in-memory parquet bytes, optionally loading the page
    /// index — mirrors what the reader does via `with_page_index_policy`.
    fn load_metadata_with_page_index(
        bytes: &[u8],
        page_index: bool,
    ) -> Arc<parquet::file::metadata::ParquetMetaData> {
        let mut reader = ParquetMetaDataReader::new();
        if page_index {
            reader = reader
                .with_column_index_policy(PageIndexPolicy::Optional)
                .with_offset_index_policy(PageIndexPolicy::Optional);
        }
        let owned: bytes::Bytes = bytes.to_vec().into();
        Arc::new(reader.parse_and_finish(&owned).unwrap())
    }

    fn int_field(name: &str) -> DataField {
        DataField::new(0, name.to_string(), DataType::Int(IntType::new()))
    }

    fn id_leaf(op: PredicateOperator, literals: Vec<Datum>) -> Predicate {
        Predicate::Leaf {
            column: "id".to_string(),
            index: 0,
            data_type: DataType::Int(IntType::new()),
            op,
            literals,
        }
    }

    #[tokio::test]
    async fn test_page_selection_eq_keeps_only_matching_page() {
        // 80 rows / 10 per page = 8 pages; page p covers ids [p*10, p*10+10).
        let bytes = write_multi_page_parquet(10, 80).await;
        let metadata = load_metadata_with_page_index(&bytes, true);
        let fields = vec![int_field("id"), int_field("value")];

        // Eq(35) falls only in page 3 ([30, 40)).
        let predicates = vec![id_leaf(PredicateOperator::Eq, vec![Datum::Int(35)])];
        let sel = super::build_predicate_page_selection(&metadata, &predicates, &fields)
            .unwrap()
            .expect("a page should be skipped");
        assert_eq!(sel.row_count(), 10);
    }

    #[tokio::test]
    async fn test_page_selection_eq_outside_all_pages_skips_everything() {
        let bytes = write_multi_page_parquet(10, 80).await;
        let metadata = load_metadata_with_page_index(&bytes, true);
        let fields = vec![int_field("id"), int_field("value")];

        // 1000 is past every page's max (79).
        let predicates = vec![id_leaf(PredicateOperator::Eq, vec![Datum::Int(1000)])];
        let sel = super::build_predicate_page_selection(&metadata, &predicates, &fields)
            .unwrap()
            .expect("all pages should be skipped");
        assert_eq!(sel.row_count(), 0);
    }

    #[tokio::test]
    async fn test_page_selection_range_keeps_overlapping_pages() {
        let bytes = write_multi_page_parquet(10, 80).await;
        let metadata = load_metadata_with_page_index(&bytes, true);
        let fields = vec![int_field("id"), int_field("value")];

        // Lt(25) overlaps pages 0 ([0,10)), 1 ([10,20)), 2 ([20,30)) — 30 rows.
        let predicates = vec![id_leaf(PredicateOperator::Lt, vec![Datum::Int(25)])];
        let sel = super::build_predicate_page_selection(&metadata, &predicates, &fields)
            .unwrap()
            .expect("some pages should be skipped");
        assert_eq!(sel.row_count(), 30);
    }

    #[tokio::test]
    async fn test_page_selection_neq_falls_open() {
        let bytes = write_multi_page_parquet(10, 80).await;
        let metadata = load_metadata_with_page_index(&bytes, true);
        let fields = vec![int_field("id"), int_field("value")];

        // NotEq is conservative under stats: every page holds other values, so
        // no page can be excluded → helper returns None (selection unchanged).
        let predicates = vec![id_leaf(PredicateOperator::NotEq, vec![Datum::Int(35)])];
        let sel = super::build_predicate_page_selection(&metadata, &predicates, &fields).unwrap();
        assert!(sel.is_none(), "NotEq must not skip any page (got {sel:?})");
    }

    #[tokio::test]
    async fn test_page_selection_returns_none_without_page_index() {
        let bytes = write_multi_page_parquet(10, 80).await;
        // Metadata parsed without the page index → helper must fall open.
        let metadata = load_metadata_with_page_index(&bytes, false);
        let fields = vec![int_field("id"), int_field("value")];

        let predicates = vec![id_leaf(PredicateOperator::Eq, vec![Datum::Int(35)])];
        let sel = super::build_predicate_page_selection(&metadata, &predicates, &fields).unwrap();
        assert!(
            sel.is_none(),
            "missing page index must fall open (got {sel:?})"
        );
    }

    /// Expand a [`RowSelection`] into the set of selected 0-based row indices.
    fn selected_rows(sel: &RowSelection) -> Vec<usize> {
        let mut rows = Vec::new();
        let mut pos = 0usize;
        for selector in sel.iter() {
            if !selector.skip {
                rows.extend(pos..pos + selector.row_count);
            }
            pos += selector.row_count;
        }
        rows
    }

    /// Write two columns whose page layouts differ: `id` is a single page while
    /// `value` is forced into ~10-row pages via a per-column byte-size limit.
    /// This is the layout that exposes the "borrow another column's page rows"
    /// bug — the driver column's pages must not be reused for `value`'s stats.
    async fn write_divergent_page_layout_parquet(total_rows: i32) -> Vec<u8> {
        use parquet::schema::types::ColumnPath;
        let schema = writer_arrow_schema();
        let props = parquet::file::properties::WriterProperties::builder()
            // `value` (4 bytes/row): ~40-byte pages ≈ 10 rows per page.
            .set_column_data_page_size_limit(ColumnPath::from("value"), 40)
            .set_write_batch_size(10)
            // `id` keeps the default large page limit → a single page.
            .set_max_row_group_row_count(Some(total_rows as usize))
            .build();
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer =
                AsyncArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).unwrap();
            let ids: Vec<i32> = (0..total_rows).collect();
            let values: Vec<i32> = ids.iter().map(|v| v * 10).collect();
            writer
                .write(&writer_test_batch(&schema, ids, values))
                .await
                .unwrap();
            writer.close().await.unwrap();
        }
        buf
    }

    #[tokio::test]
    async fn test_page_selection_predicate_on_non_driver_column() {
        // Regression: pruning must use each column's own page boundaries. Here
        // `value` has many pages while `id` has one; a predicate on `value`
        // must never drop rows that actually match.
        let bytes = write_divergent_page_layout_parquet(80).await;
        let metadata = load_metadata_with_page_index(&bytes, true);
        let fields = vec![int_field("id"), int_field("value")];

        // value == 350 (i.e. id == 35). Predicate is on the second field.
        let predicates = vec![Predicate::Leaf {
            column: "value".to_string(),
            index: 1,
            data_type: DataType::Int(IntType::new()),
            op: PredicateOperator::Eq,
            literals: vec![Datum::Int(350)],
        }];
        let sel = super::build_predicate_page_selection(&metadata, &predicates, &fields)
            .unwrap()
            .expect("value pages should be prunable");

        // The matching row (index 35) must survive; pruning is still effective
        // (not every row is kept).
        let rows = selected_rows(&sel);
        assert!(rows.contains(&35), "matching row 35 must be kept: {rows:?}");
        assert!(
            sel.row_count() < 80,
            "some non-matching pages should be skipped (kept {} rows)",
            sel.row_count()
        );
    }

    #[tokio::test]
    async fn test_page_selection_fails_open_on_missing_column_index() {
        // A column with only chunk-level statistics has no ColumnIndex
        // (`ColumnIndexMetaData::NONE`) but still gets an OffsetIndex. Its
        // accessors panic rather than return None, so pruning must fail open
        // for that column instead of touching the index.
        use parquet::file::properties::EnabledStatistics;
        use parquet::schema::types::ColumnPath;

        let schema = writer_arrow_schema();
        let props = parquet::file::properties::WriterProperties::builder()
            .set_data_page_row_count_limit(10)
            .set_write_batch_size(10)
            .set_max_row_group_row_count(Some(80))
            // `value` keeps chunk stats only → no column index, but an offset
            // index is still written.
            .set_column_statistics_enabled(ColumnPath::from("value"), EnabledStatistics::Chunk)
            .build();
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer =
                AsyncArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).unwrap();
            let ids: Vec<i32> = (0..80).collect();
            let values: Vec<i32> = ids.iter().map(|v| v * 10).collect();
            writer
                .write(&writer_test_batch(&schema, ids, values))
                .await
                .unwrap();
            writer.close().await.unwrap();
        }
        let metadata = load_metadata_with_page_index(&buf, true);
        let fields = vec![int_field("id"), int_field("value")];

        // Predicate on the column without a column index must not panic; it
        // falls open (kept whole), so no page is skipped for it.
        let predicates = vec![Predicate::Leaf {
            column: "value".to_string(),
            index: 1,
            data_type: DataType::Int(IntType::new()),
            op: PredicateOperator::Eq,
            literals: vec![Datum::Int(350)],
        }];
        let sel = super::build_predicate_page_selection(&metadata, &predicates, &fields).unwrap();
        assert!(
            sel.is_none(),
            "column without a ColumnIndex must fall open (got {sel:?})"
        );
    }

    #[test]
    fn test_page_boundaries_valid() {
        use parquet::file::page_index::offset_index::PageLocation;
        let page = |first_row_index: i64| PageLocation {
            offset: 0,
            compressed_page_size: 0,
            first_row_index,
        };

        // Well-formed: starts at 0, strictly increasing, within [0, rg_rows].
        assert!(super::page_boundaries_valid(
            &[page(0), page(10), page(20)],
            30
        ));
        // Single page starting at 0.
        assert!(super::page_boundaries_valid(&[page(0)], 10));

        // First page not at row 0.
        assert!(!super::page_boundaries_valid(&[page(5), page(10)], 30));
        // Negative first_row_index (would cast to a huge usize).
        assert!(!super::page_boundaries_valid(&[page(0), page(-1)], 30));
        // Non-monotonic.
        assert!(!super::page_boundaries_valid(
            &[page(0), page(20), page(10)],
            30
        ));
        // Duplicate (not strictly increasing).
        assert!(!super::page_boundaries_valid(
            &[page(0), page(10), page(10)],
            30
        ));
        // Beyond the row group.
        assert!(!super::page_boundaries_valid(&[page(0), page(40)], 30));
    }
}
