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

use super::{FilePredicates, FormatFileReader, FormatFileWriter};
use crate::io::{FileRead, OutputFile};
use crate::spec::{DataField, Datum, Predicate, PredicateOperator};
use crate::table::{ArrowRecordBatchStream, RowRange};
use crate::Error;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::StreamExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use vortex::array::arrow::{FromArrowArray, IntoArrowArray};
use vortex::array::dtype::arrow::FromArrowType;
use vortex::array::dtype::DType;
use vortex::array::expr::{
    and_collect, col, eq, gt, gt_eq, is_null, lit, lt, lt_eq, not, not_eq, or_collect, Expression,
};
use vortex::array::stream::{ArrayStreamAdapter, ArrayStreamExt};
use vortex::array::ArrayRef;
use vortex::buffer::{Alignment, ByteBuffer};
use vortex::error::VortexResult;
use vortex::file::{OpenOptionsSessionExt, WriteOptionsSessionExt};
use vortex::io::{IoBuf, VortexReadAt, VortexWrite};
use vortex::scan::selection::Selection;
use vortex::session::VortexSession;
use vortex::VortexSessionDefault;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum number of concurrent read requests for Vortex file IO.
const DEFAULT_READ_CONCURRENCY: usize = 10;

// ---------------------------------------------------------------------------
// IO Adapters
// ---------------------------------------------------------------------------

/// Adapts paimon's `FileRead` to Vortex's `VortexReadAt`.
struct PaimonVortexReadAt {
    file_size: u64,
    reader: Arc<dyn FileRead>,
}

impl VortexReadAt for PaimonVortexReadAt {
    fn uri(&self) -> Option<&Arc<str>> {
        None
    }

    fn concurrency(&self) -> usize {
        DEFAULT_READ_CONCURRENCY
    }

    fn size(&self) -> futures::future::BoxFuture<'static, VortexResult<u64>> {
        let size = self.file_size;
        Box::pin(async move { Ok(size) })
    }

    fn read_at(
        &self,
        offset: u64,
        length: usize,
        alignment: Alignment,
    ) -> BoxFuture<'static, VortexResult<vortex::array::buffer::BufferHandle>> {
        let reader = Arc::clone(&self.reader);
        Box::pin(async move {
            let bytes = reader
                .read(offset..offset + length as u64)
                .await
                .map_err(|e| vortex::error::vortex_err!("paimon read error: {e}"))?;
            // Zero-copy when the Bytes pointer is already aligned; falls back to copy otherwise.
            let buffer = ByteBuffer::from(bytes).aligned(alignment);
            Ok(vortex::array::buffer::BufferHandle::new_host(buffer))
        })
    }
}

// ---------------------------------------------------------------------------
// VortexFormatReader
// ---------------------------------------------------------------------------

pub(crate) struct VortexFormatReader;

#[async_trait]
impl FormatFileReader for VortexFormatReader {
    async fn read_batch_stream(
        &self,
        reader: Box<dyn FileRead>,
        file_size: u64,
        read_fields: &[DataField],
        predicates: Option<&FilePredicates>,
        _batch_size: Option<usize>,
        row_selection: Option<Vec<RowRange>>,
    ) -> crate::Result<ArrowRecordBatchStream> {
        let session = VortexSession::default();

        let source = Arc::new(PaimonVortexReadAt {
            file_size,
            reader: Arc::from(reader),
        });

        let vortex_file = session
            .open_options()
            .with_file_size(file_size)
            .open(source)
            .await
            .map_err(|e| Error::DataInvalid {
                message: format!("Failed to open Vortex file: {e}"),
                source: None,
            })?;

        // Build the target Arrow schema for the projected fields.
        let target_schema = crate::arrow::build_target_arrow_schema(read_fields)?;

        if read_fields.is_empty() {
            let row_count = match &row_selection {
                Some(ranges) => ranges.iter().map(|r| r.count() as usize).sum(),
                None => vortex_file.row_count() as usize,
            };
            let batch = RecordBatch::try_new_with_options(
                target_schema,
                vec![],
                &arrow_array::RecordBatchOptions::new().with_row_count(Some(row_count)),
            )
            .map_err(|e| Error::DataInvalid {
                message: format!("Failed to build empty RecordBatch: {e}"),
                source: None,
            })?;
            return Ok(Box::pin(futures::stream::once(async { Ok(batch) })));
        }

        // Build projection expression for requested fields.
        let projected_names: Vec<&str> = read_fields.iter().map(|f| f.name()).collect();

        let mut scan_builder = vortex_file.scan().map_err(|e| Error::DataInvalid {
            message: format!("Failed to create Vortex scan: {e}"),
            source: None,
        })?;

        // Apply column projection.
        {
            use vortex::array::expr::{root, select};
            scan_builder = scan_builder.with_projection(select(projected_names, root()));
        }

        // Push predicate filter down to Vortex.
        if let Some(fp) = predicates {
            if let Some(filter_expr) = predicates_to_vortex_expr(&fp.predicates, &fp.file_fields) {
                scan_builder = scan_builder.with_filter(filter_expr);
            }
        }

        // Push row selection down to Vortex.
        // For a single contiguous range, use with_row_range directly (avoids roaring bitmap overhead).
        // For multiple ranges, build a Selection::IncludeRoaring bitmap.
        if let Some(ref ranges) = row_selection {
            let total_rows = vortex_file.row_count();
            if let Some(range) = as_single_row_range(ranges, total_rows) {
                scan_builder = scan_builder.with_row_range(range);
            } else {
                let selection = row_ranges_to_selection(ranges, total_rows);
                scan_builder = scan_builder.with_selection(selection);
            }
        }

        let vortex_stream = scan_builder
            .into_array_stream()
            .map_err(|e| Error::DataInvalid {
                message: format!("Failed to build Vortex array stream: {e}"),
                source: None,
            })?;

        // Convert Vortex stream to Arrow RecordBatch stream.
        let stream = vortex_stream
            .map(move |result| {
                result.map_err(|e| Error::DataInvalid {
                    message: format!("Vortex read error: {e}"),
                    source: None,
                })
            })
            .map(move |result| {
                let schema = target_schema.clone();
                result.and_then(|vortex_array| vortex_array_to_record_batch(vortex_array, &schema))
            });

        Ok(Box::pin(stream))
    }
}

/// If the ranges represent a single contiguous range, return it as a `Range<u64>`
/// for use with `ScanBuilder::with_row_range` (more efficient than a roaring bitmap).
fn as_single_row_range(ranges: &[RowRange], total_rows: u64) -> Option<std::ops::Range<u64>> {
    if ranges.is_empty() || total_rows == 0 {
        return None;
    }
    let file_end = total_rows as i64 - 1;
    if ranges.len() == 1 {
        let r = &ranges[0];
        if r.to() < 0 || r.from() > file_end {
            return None;
        }
        let from = r.from().max(0) as u64;
        let to = (r.to().min(file_end) as u64) + 1;
        return Some(from..to);
    }
    None
}

// ---------------------------------------------------------------------------
// Predicate → Vortex Expression conversion
// ---------------------------------------------------------------------------

/// Convert a list of Paimon predicates (ANDed together) into a single Vortex filter expression.
fn predicates_to_vortex_expr(
    predicates: &[Predicate],
    file_fields: &[DataField],
) -> Option<Expression> {
    let exprs: Vec<Expression> = predicates
        .iter()
        .filter_map(|p| predicate_to_vortex_expr(p, file_fields))
        .collect();
    and_collect(exprs)
}

/// Convert a single Paimon `Predicate` tree node into a Vortex `Expression`.
fn predicate_to_vortex_expr(
    predicate: &Predicate,
    file_fields: &[DataField],
) -> Option<Expression> {
    match predicate {
        Predicate::AlwaysTrue => Some(lit(true)),
        Predicate::AlwaysFalse => Some(lit(false)),
        Predicate::And(children) => {
            // Dropping unconvertible children is safe for AND: it makes the filter
            // less restrictive, so no matching rows are incorrectly excluded.
            let exprs: Vec<Expression> = children
                .iter()
                .filter_map(|c| predicate_to_vortex_expr(c, file_fields))
                .collect();
            and_collect(exprs)
        }
        Predicate::Or(children) => {
            // All children must be convertible; otherwise skip the entire OR
            // to avoid incorrectly filtering out rows that match unconverted branches.
            let exprs: Vec<Expression> = children
                .iter()
                .map(|c| predicate_to_vortex_expr(c, file_fields))
                .collect::<Option<Vec<_>>>()?;
            or_collect(exprs)
        }
        Predicate::Not(inner) => predicate_to_vortex_expr(inner, file_fields).map(not),
        Predicate::Leaf {
            column,
            index,
            op,
            literals,
            ..
        } => leaf_to_vortex_expr(column, *index, *op, literals, file_fields),
    }
}

/// Convert a leaf predicate to a Vortex expression.
fn leaf_to_vortex_expr(
    _column: &str,
    index: usize,
    op: PredicateOperator,
    literals: &[Datum],
    file_fields: &[DataField],
) -> Option<Expression> {
    let file_field = file_fields.get(index)?;
    // Use the file-level column name for the Vortex expression.
    let column_expr = col(file_field.name());

    match op {
        PredicateOperator::IsNull => Some(is_null(column_expr)),
        PredicateOperator::IsNotNull => Some(not(is_null(column_expr))),
        PredicateOperator::Eq => {
            let v = datum_to_vortex_lit(literals.first()?, file_field)?;
            Some(eq(column_expr, v))
        }
        PredicateOperator::NotEq => {
            let v = datum_to_vortex_lit(literals.first()?, file_field)?;
            Some(not_eq(column_expr, v))
        }
        PredicateOperator::Lt => {
            let v = datum_to_vortex_lit(literals.first()?, file_field)?;
            Some(lt(column_expr, v))
        }
        PredicateOperator::LtEq => {
            let v = datum_to_vortex_lit(literals.first()?, file_field)?;
            Some(lt_eq(column_expr, v))
        }
        PredicateOperator::Gt => {
            let v = datum_to_vortex_lit(literals.first()?, file_field)?;
            Some(gt(column_expr, v))
        }
        PredicateOperator::GtEq => {
            let v = datum_to_vortex_lit(literals.first()?, file_field)?;
            Some(gt_eq(column_expr, v))
        }
        PredicateOperator::In => {
            // OR of eq for each literal value.
            // All literals must be convertible; otherwise skip the entire predicate
            // to avoid incorrectly filtering out rows that match unconverted literals.
            let exprs: Vec<Expression> = literals
                .iter()
                .map(|d| datum_to_vortex_lit(d, file_field).map(|v| eq(col(file_field.name()), v)))
                .collect::<Option<Vec<_>>>()?;
            or_collect(exprs)
        }
        PredicateOperator::NotIn => {
            // AND of not_eq for each literal value.
            // All literals must be convertible; otherwise skip the entire predicate
            // to avoid incorrectly keeping rows that match unconverted literals.
            let exprs: Vec<Expression> = literals
                .iter()
                .map(|d| {
                    datum_to_vortex_lit(d, file_field).map(|v| not_eq(col(file_field.name()), v))
                })
                .collect::<Option<Vec<_>>>()?;
            and_collect(exprs)
        }
    }
}

/// Convert a Paimon `Datum` to a Vortex literal `Expression`.
/// Returns `None` for types not yet supported by this conversion.
fn datum_to_vortex_lit(datum: &Datum, file_field: &DataField) -> Option<Expression> {
    use crate::spec::DataType as PaimonDataType;
    use vortex::array::dtype::Nullability;
    use vortex::array::scalar::{PValue, Scalar, ScalarValue};
    match datum {
        Datum::Bool(v) => Some(lit(*v)),
        Datum::TinyInt(v) => Some(lit(*v)),
        Datum::SmallInt(v) => Some(lit(*v)),
        Datum::Int(v) => Some(lit(*v)),
        Datum::Long(v) => Some(lit(*v)),
        Datum::Float(v) => Some(lit(*v)),
        Datum::Double(v) => Some(lit(*v)),
        Datum::String(v) => Some(lit(v.as_str())),
        Datum::Bytes(v) => Some(lit(v.as_slice())),
        // Date: stored as days since epoch (i32) in both Paimon and Vortex.
        Datum::Date(v) => {
            use vortex::extension::datetime::{Date, TimeUnit};
            let dtype =
                DType::Extension(Date::new(TimeUnit::Days, Nullability::NonNullable).erased());
            let scalar =
                Scalar::try_new(dtype, Some(ScalarValue::Primitive(PValue::I32(*v)))).ok()?;
            Some(lit(scalar))
        }
        // Time: stored as milliseconds since midnight (i32) in Paimon.
        Datum::Time(v) => {
            use vortex::extension::datetime::{Time, TimeUnit};
            let dtype = DType::Extension(
                Time::new(TimeUnit::Milliseconds, Nullability::NonNullable).erased(),
            );
            let scalar =
                Scalar::try_new(dtype, Some(ScalarValue::Primitive(PValue::I32(*v)))).ok()?;
            Some(lit(scalar))
        }
        // Timestamp: convert (millis, nanos) to the unit matching the field precision.
        //   precision 0-3 → milliseconds, 4-6 → microseconds, 7-9 → nanoseconds.
        Datum::Timestamp { millis, nanos } => {
            use vortex::extension::datetime::Timestamp;
            let precision = match file_field.data_type() {
                PaimonDataType::Timestamp(ts) => ts.precision(),
                _ => return None,
            };
            let (time_unit, value) = precision_to_time_unit_and_value(*millis, *nanos, precision);
            let dtype =
                DType::Extension(Timestamp::new(time_unit, Nullability::NonNullable).erased());
            let scalar =
                Scalar::try_new(dtype, Some(ScalarValue::Primitive(PValue::I64(value)))).ok()?;
            Some(lit(scalar))
        }
        Datum::LocalZonedTimestamp { millis, nanos } => {
            use vortex::extension::datetime::Timestamp;
            let precision = match file_field.data_type() {
                PaimonDataType::LocalZonedTimestamp(ts) => ts.precision(),
                _ => return None,
            };
            let (time_unit, value) = precision_to_time_unit_and_value(*millis, *nanos, precision);
            let dtype = DType::Extension(
                Timestamp::new_with_tz(time_unit, Some(Arc::from("UTC")), Nullability::NonNullable)
                    .erased(),
            );
            let scalar =
                Scalar::try_new(dtype, Some(ScalarValue::Primitive(PValue::I64(value)))).ok()?;
            Some(lit(scalar))
        }
        // Decimal: construct a Vortex Scalar with the correct precision and scale.
        Datum::Decimal {
            unscaled,
            precision,
            scale,
        } => {
            use vortex::array::dtype::DecimalDType;
            use vortex::array::scalar::{DecimalValue, ScalarValue as SV};
            let precision = u8::try_from(*precision).ok()?;
            let scale = i8::try_from(*scale).ok()?;
            let dtype = DType::Decimal(
                DecimalDType::new(precision, scale),
                Nullability::NonNullable,
            );
            let scalar =
                Scalar::try_new(dtype, Some(SV::Decimal(DecimalValue::I128(*unscaled)))).ok()?;
            Some(lit(scalar))
        }
    }
}

/// Convert Paimon's (millis, sub-millis nanos) pair to the Vortex TimeUnit and i64 storage value
/// for the given timestamp precision.
fn precision_to_time_unit_and_value(
    millis: i64,
    nanos: i32,
    precision: u32,
) -> (vortex::extension::datetime::TimeUnit, i64) {
    use vortex::extension::datetime::TimeUnit;
    match precision {
        0..=3 => (TimeUnit::Milliseconds, millis),
        4..=6 => (
            TimeUnit::Microseconds,
            millis * 1_000 + (nanos as i64) / 1_000,
        ),
        _ => (TimeUnit::Nanoseconds, millis * 1_000_000 + (nanos as i64)),
    }
}

/// Convert paimon `RowRange`s (inclusive [from, to]) to a Vortex `Selection`.
fn row_ranges_to_selection(ranges: &[RowRange], total_rows: u64) -> Selection {
    let mut bitmap = roaring::RoaringTreemap::new();
    let file_end = if total_rows == 0 {
        return Selection::IncludeRoaring(bitmap);
    } else {
        total_rows as i64 - 1
    };

    for range in ranges {
        if range.to() < 0 || range.from() > file_end {
            continue;
        }
        let from = range.from().max(0) as u64;
        let to = (range.to().min(file_end) as u64) + 1; // exclusive end
        bitmap.insert_range(from..to);
    }

    Selection::IncludeRoaring(bitmap)
}

/// Convert a Vortex ArrayRef to an Arrow RecordBatch.
fn vortex_array_to_record_batch(
    vortex_array: ArrayRef,
    schema: &SchemaRef,
) -> crate::Result<RecordBatch> {
    let arrow_array = vortex_array
        .into_arrow_preferred()
        .map_err(|e| Error::DataInvalid {
            message: format!("Failed to convert Vortex array to Arrow: {e}"),
            source: None,
        })?;

    let struct_array = arrow_array
        .as_any()
        .downcast_ref::<arrow_array::StructArray>()
        .ok_or_else(|| Error::DataInvalid {
            message: "Vortex array did not convert to Arrow StructArray".to_string(),
            source: None,
        })?;

    RecordBatch::try_new(schema.clone(), struct_array.columns().to_vec()).map_err(|e| {
        Error::DataInvalid {
            message: format!("Failed to build RecordBatch from Vortex data: {e}"),
            source: None,
        }
    })
}

// ---------------------------------------------------------------------------
// VortexWrite adapter
// ---------------------------------------------------------------------------

/// Adapts paimon's `AsyncFileWrite` (tokio AsyncWrite) to Vortex's `VortexWrite`,
/// with an `AtomicU64` counter tracking bytes flushed to storage.
struct CountingPaimonWrite {
    inner: Box<dyn crate::io::AsyncFileWrite>,
    bytes_written: Arc<AtomicU64>,
}

impl VortexWrite for CountingPaimonWrite {
    async fn write_all<B: IoBuf>(&mut self, buffer: B) -> std::io::Result<B> {
        let len = buffer.as_slice().len() as u64;
        tokio::io::AsyncWriteExt::write_all(&mut self.inner, buffer.as_slice()).await?;
        self.bytes_written.fetch_add(len, Ordering::Relaxed);
        Ok(buffer)
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        tokio::io::AsyncWriteExt::flush(&mut self.inner).await
    }

    async fn shutdown(&mut self) -> std::io::Result<()> {
        tokio::io::AsyncWriteExt::shutdown(&mut self.inner).await
    }
}

// ---------------------------------------------------------------------------
// VortexFormatWriter
// ---------------------------------------------------------------------------

/// Vortex implementation of [`FormatFileWriter`].
///
/// Uses a background task with a channel for streaming writes:
/// - `write()` converts each RecordBatch to a Vortex ArrayRef and sends it through a channel
/// - A background `tokio::spawn` task runs `VortexWriteOptions::write()` consuming the channel
/// - `close()` drops the sender (signaling EOF) and awaits the background task
///
/// This avoids buffering all data in memory and provides accurate `num_bytes()`.
pub(crate) struct VortexFormatWriter {
    /// Channel sender for pushing arrays to the background write task.
    sender: Option<kanal::AsyncSender<VortexResult<ArrayRef>>>,
    /// Background write task handle.
    write_task: Option<tokio::task::JoinHandle<VortexResult<vortex::file::WriteSummary>>>,
    /// Bytes already flushed to storage (updated by the background task).
    bytes_written: Arc<AtomicU64>,
}

impl VortexFormatWriter {
    pub(crate) async fn new(output: &OutputFile, schema: SchemaRef) -> crate::Result<Self> {
        let dtype = DType::from_arrow(schema);

        // Create channel for streaming arrays to the background writer.
        let (sender, receiver) = kanal::bounded_async::<VortexResult<ArrayRef>>(1);

        // Wrap receiver as an ArrayStream.
        use vortex::io::kanal_ext::KanalExt;
        let array_stream = ArrayStreamAdapter::new(dtype, receiver.into_stream());
        let sendable_stream = ArrayStreamExt::boxed(array_stream);

        // Create the counting VortexWrite sink.
        let async_writer = output.async_writer().await?;
        let bytes_written = Arc::new(AtomicU64::new(0));
        let sink = CountingPaimonWrite {
            inner: async_writer,
            bytes_written: Arc::clone(&bytes_written),
        };

        // Spawn the background write task.
        let session = VortexSession::default();
        let write_task = tokio::spawn(async move {
            let mut sink = sink;
            let result = session
                .write_options()
                .write(&mut sink, sendable_stream)
                .await;
            // Vortex only calls flush(), but opendal needs shutdown() to finalize the file.
            sink.shutdown()
                .await
                .map_err(|e| vortex::error::vortex_err!("shutdown error: {e}"))?;
            result
        });

        Ok(Self {
            sender: Some(sender),
            write_task: Some(write_task),
            bytes_written,
        })
    }
}

#[async_trait]
impl FormatFileWriter for VortexFormatWriter {
    async fn write(&mut self, batch: &RecordBatch) -> crate::Result<()> {
        let vortex_arr =
            ArrayRef::from_arrow(batch.clone(), false).map_err(|e| Error::DataInvalid {
                message: format!("Failed to convert RecordBatch to Vortex: {e}"),
                source: None,
            })?;

        let sender = self.sender.as_ref().ok_or_else(|| Error::DataInvalid {
            message: "VortexFormatWriter already closed".to_string(),
            source: None,
        })?;

        if sender.send(Ok(vortex_arr)).await.is_err() {
            // Channel closed — the background task has exited. Try to retrieve the real error.
            if let Some(task) = self.write_task.take() {
                match task.await {
                    Ok(Err(e)) => {
                        return Err(Error::DataInvalid {
                            message: format!("Vortex background write task failed: {e}"),
                            source: None,
                        });
                    }
                    Err(e) => {
                        return Err(Error::DataInvalid {
                            message: format!("Vortex background write task panicked: {e}"),
                            source: None,
                        });
                    }
                    Ok(Ok(_)) => {}
                }
            }
            return Err(Error::DataInvalid {
                message: "Vortex background write task exited unexpectedly".to_string(),
                source: None,
            });
        }
        Ok(())
    }

    fn num_bytes(&self) -> usize {
        self.bytes_written.load(Ordering::Relaxed) as usize
    }

    fn in_progress_size(&self) -> usize {
        // Vortex manages its own internal buffering in the background task;
        // we have no visibility into it, so report 0.
        0
    }

    async fn flush(&mut self) -> crate::Result<()> {
        // Vortex handles flushing internally in the background task.
        Ok(())
    }

    async fn close(mut self: Box<Self>) -> crate::Result<u64> {
        // Drop the sender to signal EOF to the background stream.
        drop(self.sender.take());

        // Await the background write task.
        let task = self.write_task.take().ok_or_else(|| Error::DataInvalid {
            message: "VortexFormatWriter already closed".to_string(),
            source: None,
        })?;

        let summary = task
            .await
            .map_err(|e| Error::DataInvalid {
                message: format!("Vortex write task panicked: {e}"),
                source: None,
            })?
            .map_err(|e| Error::DataInvalid {
                message: format!("Failed to write Vortex file: {e}"),
                source: None,
            })?;

        Ok(summary.size())
    }
}

impl Drop for VortexFormatWriter {
    fn drop(&mut self) {
        if let Some(task) = self.write_task.take() {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::format::FormatFileWriter;
    use crate::io::FileIOBuilder;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};

    fn test_arrow_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("value", ArrowDataType::Int32, false),
        ]))
    }

    fn test_batch(schema: &Arc<ArrowSchema>, ids: Vec<i32>, values: Vec<i32>) -> RecordBatch {
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
    async fn test_vortex_writer_write_and_read() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/test_vortex_write_read.vortex";
        let output = file_io.new_output(path).unwrap();
        let schema = test_arrow_schema();

        let mut writer: Box<dyn FormatFileWriter> = Box::new(
            VortexFormatWriter::new(&output, schema.clone())
                .await
                .unwrap(),
        );

        let batch = test_batch(&schema, vec![1, 2, 3], vec![10, 20, 30]);
        writer.write(&batch).await.unwrap();
        let bytes = writer.close().await.unwrap();
        assert!(bytes > 0);

        // Read back using VortexFormatReader.
        let input = file_io.new_input(path).unwrap();
        let file_reader = input.reader().await.unwrap();
        let metadata = input.metadata().await.unwrap();

        let read_fields = vec![
            crate::spec::DataField::new(
                0,
                "id".to_string(),
                crate::spec::DataType::Int(crate::spec::IntType::new()),
            ),
            crate::spec::DataField::new(
                1,
                "value".to_string(),
                crate::spec::DataType::Int(crate::spec::IntType::new()),
            ),
        ];

        let reader = VortexFormatReader;
        let mut stream = reader
            .read_batch_stream(
                Box::new(file_reader),
                metadata.size,
                &read_fields,
                None,
                None,
                None,
            )
            .await
            .unwrap();

        let mut total_rows = 0;
        while let Some(result) = stream.next().await {
            let batch = result.unwrap();
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_vortex_writer_multiple_batches() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/test_vortex_multi.vortex";
        let output = file_io.new_output(path).unwrap();
        let schema = test_arrow_schema();

        let mut writer: Box<dyn FormatFileWriter> = Box::new(
            VortexFormatWriter::new(&output, schema.clone())
                .await
                .unwrap(),
        );

        writer
            .write(&test_batch(&schema, vec![1, 2], vec![10, 20]))
            .await
            .unwrap();
        writer
            .write(&test_batch(&schema, vec![3, 4, 5], vec![30, 40, 50]))
            .await
            .unwrap();
        writer.close().await.unwrap();

        let input = file_io.new_input(path).unwrap();
        let file_reader = input.reader().await.unwrap();
        let metadata = input.metadata().await.unwrap();

        let read_fields = vec![
            crate::spec::DataField::new(
                0,
                "id".to_string(),
                crate::spec::DataType::Int(crate::spec::IntType::new()),
            ),
            crate::spec::DataField::new(
                1,
                "value".to_string(),
                crate::spec::DataType::Int(crate::spec::IntType::new()),
            ),
        ];

        let reader = VortexFormatReader;
        let mut stream = reader
            .read_batch_stream(
                Box::new(file_reader),
                metadata.size,
                &read_fields,
                None,
                None,
                None,
            )
            .await
            .unwrap();

        let mut total_rows = 0;
        while let Some(result) = stream.next().await {
            let batch = result.unwrap();
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 5);
    }

    #[tokio::test]
    async fn test_vortex_read_with_row_selection() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/test_vortex_row_sel.vortex";
        let output = file_io.new_output(path).unwrap();
        let schema = test_arrow_schema();

        // Write 5 rows: id=[1,2,3,4,5], value=[10,20,30,40,50]
        let mut writer: Box<dyn FormatFileWriter> = Box::new(
            VortexFormatWriter::new(&output, schema.clone())
                .await
                .unwrap(),
        );
        writer
            .write(&test_batch(
                &schema,
                vec![1, 2, 3, 4, 5],
                vec![10, 20, 30, 40, 50],
            ))
            .await
            .unwrap();
        writer.close().await.unwrap();

        let input = file_io.new_input(path).unwrap();
        let file_reader = input.reader().await.unwrap();
        let metadata = input.metadata().await.unwrap();

        let read_fields = vec![
            crate::spec::DataField::new(
                0,
                "id".to_string(),
                crate::spec::DataType::Int(crate::spec::IntType::new()),
            ),
            crate::spec::DataField::new(
                1,
                "value".to_string(),
                crate::spec::DataType::Int(crate::spec::IntType::new()),
            ),
        ];

        // Select rows [1,3] (inclusive), i.e. rows at index 1,2,3 → id=[2,3,4]
        let row_selection = vec![RowRange::new(1, 3)];

        let reader = VortexFormatReader;
        let mut stream = reader
            .read_batch_stream(
                Box::new(file_reader),
                metadata.size,
                &read_fields,
                None,
                None,
                Some(row_selection),
            )
            .await
            .unwrap();

        let mut all_ids = Vec::new();
        while let Some(result) = stream.next().await {
            let batch = result.unwrap();
            let id_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            all_ids.extend(id_col.values().iter().copied());
        }
        assert_eq!(all_ids, vec![2, 3, 4]);
    }

    #[tokio::test]
    async fn test_vortex_read_with_empty_projection() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/test_vortex_empty_proj.vortex";
        let output = file_io.new_output(path).unwrap();
        let schema = test_arrow_schema();

        let mut writer: Box<dyn FormatFileWriter> = Box::new(
            VortexFormatWriter::new(&output, schema.clone())
                .await
                .unwrap(),
        );
        writer
            .write(&test_batch(
                &schema,
                vec![1, 2, 3, 4, 5],
                vec![10, 20, 30, 40, 50],
            ))
            .await
            .unwrap();
        writer.close().await.unwrap();

        let input = file_io.new_input(path).unwrap();
        let file_reader = input.reader().await.unwrap();
        let metadata = input.metadata().await.unwrap();

        let reader = VortexFormatReader;
        let mut stream = reader
            .read_batch_stream(
                Box::new(file_reader),
                metadata.size,
                &[], // empty projection
                None,
                None,
                None,
            )
            .await
            .unwrap();

        let mut total_rows = 0;
        while let Some(result) = stream.next().await {
            let batch = result.unwrap();
            assert_eq!(batch.num_columns(), 0);
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 5);
    }

    // -----------------------------------------------------------------------
    // Predicate conversion unit tests
    // -----------------------------------------------------------------------

    use crate::spec::{DataType as PaimonDataType, IntType, PredicateBuilder};

    fn test_file_fields() -> Vec<DataField> {
        vec![
            DataField::new(0, "id".to_string(), PaimonDataType::Int(IntType::new())),
            DataField::new(1, "value".to_string(), PaimonDataType::Int(IntType::new())),
        ]
    }

    #[test]
    fn test_predicate_eq_converts() {
        let fields = test_file_fields();
        let builder = PredicateBuilder::new(&fields);
        let pred = builder.equal("id", Datum::Int(3)).unwrap();
        let expr = predicates_to_vortex_expr(&[pred], &fields);
        assert!(expr.is_some());
    }

    #[test]
    fn test_predicate_not_eq_converts() {
        let fields = test_file_fields();
        let builder = PredicateBuilder::new(&fields);
        let pred = builder.not_equal("value", Datum::Int(10)).unwrap();
        let expr = predicates_to_vortex_expr(&[pred], &fields);
        assert!(expr.is_some());
    }

    #[test]
    fn test_predicate_lt_gt_converts() {
        let fields = test_file_fields();
        let builder = PredicateBuilder::new(&fields);
        let lt_pred = builder.less_than("id", Datum::Int(5)).unwrap();
        let gt_pred = builder.greater_than("value", Datum::Int(20)).unwrap();
        let expr = predicates_to_vortex_expr(&[lt_pred, gt_pred], &fields);
        assert!(expr.is_some(), "AND of Lt and Gt should convert");
    }

    #[test]
    fn test_predicate_is_null_converts() {
        let fields = test_file_fields();
        let builder = PredicateBuilder::new(&fields);
        let pred = builder.is_null("id").unwrap();
        let expr = predicates_to_vortex_expr(&[pred], &fields);
        assert!(expr.is_some());
    }

    #[test]
    fn test_predicate_is_not_null_converts() {
        let fields = test_file_fields();
        let builder = PredicateBuilder::new(&fields);
        let pred = builder.is_not_null("value").unwrap();
        let expr = predicates_to_vortex_expr(&[pred], &fields);
        assert!(expr.is_some());
    }

    #[test]
    fn test_predicate_in_converts() {
        let fields = test_file_fields();
        let builder = PredicateBuilder::new(&fields);
        let pred = builder
            .is_in("id", vec![Datum::Int(1), Datum::Int(3)])
            .unwrap();
        let expr = predicates_to_vortex_expr(&[pred], &fields);
        assert!(expr.is_some());
    }

    #[test]
    fn test_predicate_not_in_converts() {
        let fields = test_file_fields();
        let builder = PredicateBuilder::new(&fields);
        let pred = builder
            .is_not_in("id", vec![Datum::Int(2), Datum::Int(4)])
            .unwrap();
        let expr = predicates_to_vortex_expr(&[pred], &fields);
        assert!(expr.is_some());
    }

    #[test]
    fn test_predicate_in_with_unsupported_literal_skips_entirely() {
        let fields = test_file_fields();
        // Manually build an In predicate with a Decimal literal whose precision
        // exceeds u8 range, making it unconvertible to Vortex.
        let pred = Predicate::Leaf {
            column: "id".to_string(),
            index: 0,
            data_type: PaimonDataType::Int(IntType::new()),
            op: PredicateOperator::In,
            literals: vec![
                Datum::Int(1),
                Datum::Decimal {
                    unscaled: 100,
                    precision: 256,
                    scale: 2,
                },
            ],
        };
        // The entire In should be skipped (None) because one literal can't convert.
        let expr = predicate_to_vortex_expr(&pred, &fields);
        assert!(expr.is_none());
    }

    #[test]
    fn test_predicate_or_with_unsupported_branch_skips_entirely() {
        let fields = test_file_fields();
        let builder = PredicateBuilder::new(&fields);
        let convertible = builder.equal("id", Datum::Int(1)).unwrap();
        // Build an unconvertible leaf (Decimal with precision > u8::MAX).
        let unconvertible = Predicate::Leaf {
            column: "id".to_string(),
            index: 0,
            data_type: PaimonDataType::Int(IntType::new()),
            op: PredicateOperator::Eq,
            literals: vec![Datum::Decimal {
                unscaled: 100,
                precision: 256,
                scale: 2,
            }],
        };
        let or_pred = Predicate::Or(vec![convertible, unconvertible]);
        // The entire OR should be skipped because one branch can't convert.
        let expr = predicate_to_vortex_expr(&or_pred, &fields);
        assert!(expr.is_none());
    }

    #[test]
    fn test_predicate_and_with_unsupported_branch_keeps_convertible() {
        let fields = test_file_fields();
        let builder = PredicateBuilder::new(&fields);
        let convertible = builder.equal("id", Datum::Int(1)).unwrap();
        let unconvertible = Predicate::Leaf {
            column: "id".to_string(),
            index: 0,
            data_type: PaimonDataType::Int(IntType::new()),
            op: PredicateOperator::Eq,
            literals: vec![Datum::Decimal {
                unscaled: 100,
                precision: 256,
                scale: 2,
            }],
        };
        let and_pred = Predicate::And(vec![convertible, unconvertible]);
        // AND should still produce an expression from the convertible branch.
        let expr = predicate_to_vortex_expr(&and_pred, &fields);
        assert!(expr.is_some());
    }

    #[test]
    fn test_predicate_always_true_false() {
        let fields = test_file_fields();
        assert!(predicate_to_vortex_expr(&Predicate::AlwaysTrue, &fields).is_some());
        assert!(predicate_to_vortex_expr(&Predicate::AlwaysFalse, &fields).is_some());
    }

    #[test]
    fn test_predicate_not_converts() {
        let fields = test_file_fields();
        let builder = PredicateBuilder::new(&fields);
        let inner = builder.equal("id", Datum::Int(3)).unwrap();
        let pred = Predicate::Not(Box::new(inner));
        let expr = predicate_to_vortex_expr(&pred, &fields);
        assert!(expr.is_some());
    }

    #[test]
    fn test_empty_predicates_returns_none() {
        let fields = test_file_fields();
        let expr = predicates_to_vortex_expr(&[], &fields);
        assert!(expr.is_none());
    }

    // -----------------------------------------------------------------------
    // Integration tests: predicate pushdown through VortexFormatReader
    // -----------------------------------------------------------------------

    /// Helper: write test data and read back with given predicates, return collected id values.
    async fn write_and_read_with_predicates(
        path: &str,
        predicates: Option<FilePredicates>,
    ) -> Vec<i32> {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let output = file_io.new_output(path).unwrap();
        let schema = test_arrow_schema();

        // Write 5 rows: id=[1,2,3,4,5], value=[10,20,30,40,50]
        let mut writer: Box<dyn FormatFileWriter> = Box::new(
            VortexFormatWriter::new(&output, schema.clone())
                .await
                .unwrap(),
        );
        writer
            .write(&test_batch(
                &schema,
                vec![1, 2, 3, 4, 5],
                vec![10, 20, 30, 40, 50],
            ))
            .await
            .unwrap();
        writer.close().await.unwrap();

        let input = file_io.new_input(path).unwrap();
        let file_reader = input.reader().await.unwrap();
        let metadata = input.metadata().await.unwrap();

        let read_fields = test_file_fields();

        let reader = VortexFormatReader;
        let mut stream = reader
            .read_batch_stream(
                Box::new(file_reader),
                metadata.size,
                &read_fields,
                predicates.as_ref(),
                None,
                None,
            )
            .await
            .unwrap();

        let mut all_ids = Vec::new();
        while let Some(result) = stream.next().await {
            let batch = result.unwrap();
            let id_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            all_ids.extend(id_col.values().iter().copied());
        }
        all_ids
    }

    #[tokio::test]
    async fn test_vortex_read_with_eq_predicate() {
        let fields = test_file_fields();
        let builder = PredicateBuilder::new(&fields);
        let pred = builder.equal("id", Datum::Int(3)).unwrap();
        let fp = FilePredicates {
            predicates: vec![pred],
            file_fields: fields,
        };
        let ids =
            write_and_read_with_predicates("memory:/test_vortex_pred_eq.vortex", Some(fp)).await;
        assert_eq!(ids, vec![3]);
    }

    #[tokio::test]
    async fn test_vortex_read_with_gt_predicate() {
        let fields = test_file_fields();
        let builder = PredicateBuilder::new(&fields);
        let pred = builder.greater_than("id", Datum::Int(3)).unwrap();
        let fp = FilePredicates {
            predicates: vec![pred],
            file_fields: fields,
        };
        let ids =
            write_and_read_with_predicates("memory:/test_vortex_pred_gt.vortex", Some(fp)).await;
        assert_eq!(ids, vec![4, 5]);
    }

    #[tokio::test]
    async fn test_vortex_read_with_in_predicate() {
        let fields = test_file_fields();
        let builder = PredicateBuilder::new(&fields);
        let pred = builder
            .is_in("id", vec![Datum::Int(1), Datum::Int(4)])
            .unwrap();
        let fp = FilePredicates {
            predicates: vec![pred],
            file_fields: fields,
        };
        let ids =
            write_and_read_with_predicates("memory:/test_vortex_pred_in.vortex", Some(fp)).await;
        assert_eq!(ids, vec![1, 4]);
    }

    #[tokio::test]
    async fn test_vortex_read_with_combined_predicates() {
        let fields = test_file_fields();
        let builder = PredicateBuilder::new(&fields);
        // id >= 2 AND value < 50
        let pred1 = builder.greater_or_equal("id", Datum::Int(2)).unwrap();
        let pred2 = builder.less_than("value", Datum::Int(50)).unwrap();
        let fp = FilePredicates {
            predicates: vec![pred1, pred2],
            file_fields: fields,
        };
        let ids =
            write_and_read_with_predicates("memory:/test_vortex_pred_combined.vortex", Some(fp))
                .await;
        // id=[2,3,4] (id>=2 and value<50, excludes id=5/value=50)
        assert_eq!(ids, vec![2, 3, 4]);
    }

    #[tokio::test]
    async fn test_vortex_read_with_no_match_predicate() {
        let fields = test_file_fields();
        let builder = PredicateBuilder::new(&fields);
        let pred = builder.equal("id", Datum::Int(99)).unwrap();
        let fp = FilePredicates {
            predicates: vec![pred],
            file_fields: fields,
        };
        let ids =
            write_and_read_with_predicates("memory:/test_vortex_pred_nomatch.vortex", Some(fp))
                .await;
        assert!(ids.is_empty());
    }

    // -----------------------------------------------------------------------
    // Timestamp precision tests
    // -----------------------------------------------------------------------

    use crate::spec::{LocalZonedTimestampType, TimestampType};

    #[test]
    fn test_precision_to_time_unit_and_value_millis() {
        use vortex::extension::datetime::TimeUnit;
        // precision 0-3 → millis, nanos ignored
        assert_eq!(
            precision_to_time_unit_and_value(1000, 500_000, 0),
            (TimeUnit::Milliseconds, 1000)
        );
        assert_eq!(
            precision_to_time_unit_and_value(1000, 500_000, 3),
            (TimeUnit::Milliseconds, 1000)
        );
    }

    #[test]
    fn test_precision_to_time_unit_and_value_micros() {
        use vortex::extension::datetime::TimeUnit;
        // precision 4-6 → micros = millis * 1000 + nanos / 1000
        // 1000ms, 500_000ns (= 500µs) → 1_000_500µs
        assert_eq!(
            precision_to_time_unit_and_value(1000, 500_000, 6),
            (TimeUnit::Microseconds, 1_000_500)
        );
        assert_eq!(
            precision_to_time_unit_and_value(1000, 0, 4),
            (TimeUnit::Microseconds, 1_000_000)
        );
    }

    #[test]
    fn test_precision_to_time_unit_and_value_nanos() {
        use vortex::extension::datetime::TimeUnit;
        // precision 7-9 → nanos = millis * 1_000_000 + nanos
        // 1000ms, 500_000ns → 1_000_500_000ns
        assert_eq!(
            precision_to_time_unit_and_value(1000, 500_000, 9),
            (TimeUnit::Nanoseconds, 1_000_500_000)
        );
        assert_eq!(
            precision_to_time_unit_and_value(1000, 0, 7),
            (TimeUnit::Nanoseconds, 1_000_000_000)
        );
    }

    #[test]
    fn test_datum_to_vortex_lit_timestamp_precision() {
        let ts_field_millis = DataField::new(
            0,
            "ts".to_string(),
            PaimonDataType::Timestamp(TimestampType::new(3).unwrap()),
        );
        let ts_field_micros = DataField::new(
            0,
            "ts".to_string(),
            PaimonDataType::Timestamp(TimestampType::new(6).unwrap()),
        );
        let ts_field_nanos = DataField::new(
            0,
            "ts".to_string(),
            PaimonDataType::Timestamp(TimestampType::new(9).unwrap()),
        );

        let datum = Datum::Timestamp {
            millis: 1000,
            nanos: 500_000,
        };

        // All should produce Some
        assert!(datum_to_vortex_lit(&datum, &ts_field_millis).is_some());
        assert!(datum_to_vortex_lit(&datum, &ts_field_micros).is_some());
        assert!(datum_to_vortex_lit(&datum, &ts_field_nanos).is_some());
    }

    #[test]
    fn test_datum_to_vortex_lit_local_zoned_timestamp_precision() {
        let field_millis = DataField::new(
            0,
            "ts".to_string(),
            PaimonDataType::LocalZonedTimestamp(LocalZonedTimestampType::new(3).unwrap()),
        );
        let field_micros = DataField::new(
            0,
            "ts".to_string(),
            PaimonDataType::LocalZonedTimestamp(LocalZonedTimestampType::new(6).unwrap()),
        );

        let datum = Datum::LocalZonedTimestamp {
            millis: 2000,
            nanos: 123_456,
        };

        assert!(datum_to_vortex_lit(&datum, &field_millis).is_some());
        assert!(datum_to_vortex_lit(&datum, &field_micros).is_some());
    }

    #[test]
    fn test_datum_to_vortex_lit_timestamp_wrong_field_type_returns_none() {
        // Timestamp datum with an Int field should return None
        let int_field = DataField::new(0, "id".to_string(), PaimonDataType::Int(IntType::new()));
        let datum = Datum::Timestamp {
            millis: 1000,
            nanos: 0,
        };
        assert!(datum_to_vortex_lit(&datum, &int_field).is_none());

        let datum_lz = Datum::LocalZonedTimestamp {
            millis: 1000,
            nanos: 0,
        };
        assert!(datum_to_vortex_lit(&datum_lz, &int_field).is_none());
    }
}
