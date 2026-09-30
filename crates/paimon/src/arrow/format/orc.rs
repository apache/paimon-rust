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

use super::{FilePredicates, FormatFileReader, FormatFileWriter, FormatWriteResult};
use crate::io::{FileRead, FileWrite, OutputFile};
use crate::spec::{is_row_id_column, DataField, DataType, Datum, Predicate, PredicateOperator};
use crate::table::{ArrowRecordBatchStream, RowRange};
use crate::Error;
use arrow_array::RecordBatch;
use arrow_schema::{DataType as ArrowDataType, SchemaRef};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{future::BoxFuture, StreamExt};
use orc_rust::predicate::PredicateValue;
use orc_rust::projection::ProjectionMask;
use orc_rust::reader::AsyncChunkReader;
use orc_rust::ArrowReaderBuilder;
use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};

const ORC_IN_PREDICATE_MAX_LITERALS: usize = 20;

pub(crate) struct OrcFormatReader;

const ORC_STRIPE_SIZE: usize = 64 * 1024 * 1024;
const ORC_WORKER_COUNT: usize = 4;
static NEXT_ORC_WRITER_ID: AtomicU64 = AtomicU64::new(1);
static ORC_WORKER_POOL: Mutex<Option<Vec<mpsc::UnboundedSender<WriterCommand>>>> = Mutex::new(None);

/// `orc-rust` exposes a synchronous writer. Drain completed stripes into
/// Paimon's async FileWrite so a file does not retain every input batch.
pub(crate) struct OrcFormatWriter {
    schema: SchemaRef,
    worker: mpsc::UnboundedSender<WriterCommand>,
    writer_id: u64,
    output: Box<dyn FileWrite>,
    pending_bytes: usize,
    pending_rows: usize,
    bytes_written: usize,
}

enum WriterCommand {
    Create(u64, SchemaRef, oneshot::Sender<crate::Result<()>>),
    Write(u64, RecordBatch, oneshot::Sender<crate::Result<()>>),
    Flush(u64, oneshot::Sender<crate::Result<Vec<u8>>>),
    Close(u64, oneshot::Sender<crate::Result<Vec<u8>>>),
    Drop(u64),
}

struct WorkerWriter {
    writer: orc_rust::ArrowWriter<SharedBuffer>,
    encoded: Arc<Mutex<Vec<u8>>>,
}

#[derive(Clone)]
struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl OrcFormatWriter {
    pub(crate) async fn new(output: &OutputFile, schema: SchemaRef) -> crate::Result<Self> {
        // orc-rust 0.8 uses `unimplemented!` for other Arrow types. Report a
        // normal error before it can panic or leave a partial staged file.
        for field in schema.fields() {
            if !matches!(
                field.data_type(),
                ArrowDataType::Float32
                    | ArrowDataType::Float64
                    | ArrowDataType::Int8
                    | ArrowDataType::Int16
                    | ArrowDataType::Int32
                    | ArrowDataType::Int64
                    | ArrowDataType::Utf8
                    | ArrowDataType::LargeUtf8
                    | ArrowDataType::Binary
                    | ArrowDataType::LargeBinary
                    | ArrowDataType::Boolean
            ) {
                return Err(Error::Unsupported {
                    message: format!(
                        "ORC writer does not support column '{}' ({:?})",
                        field.name(),
                        field.data_type()
                    ),
                });
            }
        }
        let output = output.writer().await?;
        let writer_id = NEXT_ORC_WRITER_ID.fetch_add(1, Ordering::Relaxed);
        let worker = orc_worker(writer_id)?;
        let (ready_tx, ready_rx) = oneshot::channel();
        worker
            .send(WriterCommand::Create(writer_id, schema.clone(), ready_tx))
            .map_err(|_| worker_stopped())?;
        ready_rx.await.map_err(|_| worker_stopped())??;
        Ok(Self {
            schema,
            worker,
            writer_id,
            output,
            pending_bytes: 0,
            pending_rows: 0,
            bytes_written: 0,
        })
    }

    async fn publish_encoded(&mut self, encoded: Vec<u8>) -> crate::Result<()> {
        if !encoded.is_empty() {
            self.bytes_written += encoded.len();
            self.output.write(Bytes::from(encoded)).await?;
        }
        Ok(())
    }
}

impl Drop for OrcFormatWriter {
    fn drop(&mut self) {
        let _ = self.worker.send(WriterCommand::Drop(self.writer_id));
    }
}

fn orc_worker(writer_id: u64) -> crate::Result<mpsc::UnboundedSender<WriterCommand>> {
    let mut pool = ORC_WORKER_POOL.lock().map_err(|_| worker_stopped())?;
    if pool.is_none() {
        let count = std::thread::available_parallelism()
            .map(|count| count.get().min(ORC_WORKER_COUNT))
            .unwrap_or(1);
        let mut workers = Vec::with_capacity(count);
        for index in 0..count {
            let (sender, receiver) = mpsc::unbounded_channel();
            std::thread::Builder::new()
                .name(format!("paimon-orc-writer-{index}"))
                .spawn(move || run_orc_worker(receiver))
                .map_err(|error| Error::UnexpectedError {
                    message: format!("Failed to start ORC writer worker: {error}"),
                    source: Some(Box::new(error)),
                })?;
            workers.push(sender);
        }
        *pool = Some(workers);
    }
    let workers = pool.as_mut().unwrap();
    let index = (writer_id as usize) % workers.len();
    if workers[index].is_closed() {
        let (sender, receiver) = mpsc::unbounded_channel();
        std::thread::Builder::new()
            .name(format!("paimon-orc-writer-{index}"))
            .spawn(move || run_orc_worker(receiver))
            .map_err(|error| Error::UnexpectedError {
                message: format!("Failed to restart ORC writer worker: {error}"),
                source: Some(Box::new(error)),
            })?;
        workers[index] = sender;
    }
    Ok(workers[index].clone())
}

fn run_orc_worker(mut commands: mpsc::UnboundedReceiver<WriterCommand>) {
    let mut writers: HashMap<u64, WorkerWriter> = HashMap::new();
    while let Some(command) = commands.blocking_recv() {
        match command {
            WriterCommand::Create(id, schema, done) => {
                let encoded = Arc::new(Mutex::new(Vec::new()));
                let result =
                    orc_rust::ArrowWriterBuilder::new(SharedBuffer(encoded.clone()), schema)
                        .with_stripe_byte_size(usize::MAX)
                        .try_build()
                        .map_err(|error| Error::DataInvalid {
                            message: format!("Failed to create ORC writer: {error}"),
                            source: Some(Box::new(error)),
                        });
                if done
                    .send(result.map(|writer| {
                        writers.insert(id, WorkerWriter { writer, encoded });
                    }))
                    .is_err()
                {
                    writers.remove(&id);
                }
            }
            WriterCommand::Write(id, batch, done) => {
                let result = writers
                    .get_mut(&id)
                    .ok_or_else(worker_stopped)
                    .and_then(|state| {
                        state
                            .writer
                            .write(&batch)
                            .map_err(|error| Error::DataInvalid {
                                message: format!("Failed to write ORC batch: {error}"),
                                source: Some(Box::new(error)),
                            })
                    });
                let _ = done.send(result);
            }
            WriterCommand::Flush(id, done) => {
                let result = writers
                    .get_mut(&id)
                    .ok_or_else(worker_stopped)
                    .and_then(|state| {
                        state
                            .writer
                            .flush_stripe()
                            .map_err(|error| Error::DataInvalid {
                                message: format!("Failed to flush ORC stripe: {error}"),
                                source: Some(Box::new(error)),
                            })?;
                        Ok(std::mem::take(&mut *state.encoded.lock().unwrap()))
                    });
                let _ = done.send(result);
            }
            WriterCommand::Close(id, done) => {
                let result = writers
                    .remove(&id)
                    .ok_or_else(worker_stopped)
                    .and_then(|state| {
                        state.writer.close().map_err(|error| Error::DataInvalid {
                            message: format!("Failed to close ORC writer: {error}"),
                            source: Some(Box::new(error)),
                        })?;
                        let encoded = std::mem::take(&mut *state.encoded.lock().unwrap());
                        Ok(encoded)
                    });
                let _ = done.send(result);
            }
            WriterCommand::Drop(id) => {
                writers.remove(&id);
            }
        }
    }
}

fn worker_stopped() -> Error {
    Error::UnexpectedError {
        message: "ORC writer worker stopped unexpectedly".into(),
        source: None,
    }
}

#[async_trait]
impl FormatFileWriter for OrcFormatWriter {
    async fn write(&mut self, batch: &RecordBatch) -> crate::Result<()> {
        if batch.schema() != self.schema {
            return Err(Error::DataInvalid {
                message: "ORC batch schema differs from file schema".into(),
                source: None,
            });
        }
        let (done, result) = oneshot::channel();
        self.worker
            .send(WriterCommand::Write(self.writer_id, batch.clone(), done))
            .map_err(|_| worker_stopped())?;
        result.await.map_err(|_| worker_stopped())??;
        self.pending_bytes = self
            .pending_bytes
            .saturating_add(batch.get_array_memory_size());
        self.pending_rows = self.pending_rows.saturating_add(batch.num_rows());
        if self.pending_bytes >= ORC_STRIPE_SIZE {
            self.flush().await?;
        }
        Ok(())
    }

    fn num_bytes(&self) -> usize {
        self.bytes_written.saturating_add(self.pending_bytes)
    }
    fn in_progress_size(&self) -> usize {
        self.pending_bytes
    }
    fn pending_rows(&self) -> Option<usize> {
        Some(self.pending_rows)
    }
    async fn flush(&mut self) -> crate::Result<()> {
        if self.pending_rows > 0 {
            let (done, result) = oneshot::channel();
            self.worker
                .send(WriterCommand::Flush(self.writer_id, done))
                .map_err(|_| worker_stopped())?;
            let encoded = result.await.map_err(|_| worker_stopped())??;
            self.pending_bytes = 0;
            self.pending_rows = 0;
            self.publish_encoded(encoded).await?;
        }
        Ok(())
    }
    async fn close(mut self: Box<Self>) -> crate::Result<FormatWriteResult> {
        let (done, result) = oneshot::channel();
        self.worker
            .send(WriterCommand::Close(self.writer_id, done))
            .map_err(|_| worker_stopped())?;
        let encoded = result.await.map_err(|_| worker_stopped())??;
        self.publish_encoded(encoded).await?;
        self.output.close().await?;
        Ok(FormatWriteResult::new(self.bytes_written as u64))
    }
}

#[async_trait]
impl FormatFileReader for OrcFormatReader {
    async fn read_batch_stream(
        &self,
        reader: Box<dyn FileRead>,
        file_size: u64,
        read_fields: &[DataField],
        predicates: Option<&FilePredicates>,
        batch_size: Option<usize>,
        row_selection: Option<Vec<RowRange>>,
    ) -> crate::Result<ArrowRecordBatchStream> {
        let orc_reader = OrcFileReader::new(file_size, reader);

        let builder = ArrowReaderBuilder::try_new_async(orc_reader)
            .await
            .map_err(|e| Error::UnexpectedError {
                message: format!("Failed to open ORC file: {e}"),
                source: Some(Box::new(e)),
            })?;

        // Widen the scan to include predicate columns so the residual filter can
        // see every column it references, even when a predicate column is not part
        // of the requested projection. DataFileReader projects the returned batch
        // to the requested output by name afterwards, so the extra columns are
        // harmless.
        let scan_fields = crate::arrow::residual::widen_scan_fields(read_fields, predicates);
        let projected_names: Vec<String> =
            scan_fields.iter().map(|f| f.name().to_string()).collect();
        let orc_predicate = build_orc_predicate(predicates);
        let projection =
            ProjectionMask::named_roots(builder.file_metadata().root_data_type(), &projected_names);

        let mut builder = builder.with_projection(projection);

        if let Some(predicate) = orc_predicate {
            builder = builder.with_predicate(predicate);
        }

        if let Some(size) = batch_size {
            builder = builder.with_batch_size(size);
        }

        if let Some(ref ranges) = row_selection {
            let total_rows: usize = builder
                .file_metadata()
                .stripe_metadatas()
                .iter()
                .map(|s| s.number_of_rows() as usize)
                .sum();
            let selection = build_range_row_selection(total_rows, ranges);
            builder = builder.with_row_selection(selection);
        }

        let stream = builder.build_async();
        // Stripe/row-group pruning above only skips whole stripes whose stats cannot
        // match; non-matching rows inside a selected stripe survive. Apply the exact
        // residual filter on each emitted batch so the reader returns exactly the rows
        // matching the pushed-down predicate. The batch is widened with predicate
        // columns; DataFileReader projects to the requested output by name afterwards.
        // Own the predicate context (scan_fields + cloned FilePredicates) for the
        // 'static stream.
        let residual: Option<(FilePredicates, Vec<DataField>)> = predicates.map(|fp| {
            (
                FilePredicates {
                    predicates: fp.predicates.clone(),
                    row_filter_factory: None,
                    file_fields: fp.file_fields.clone(),
                },
                scan_fields,
            )
        });
        Ok(stream
            .map(move |r| {
                let batch = r.map_err(|e| Error::UnexpectedError {
                    message: format!("ORC read error: {e}"),
                    source: Some(Box::new(e)),
                })?;
                match &residual {
                    Some((fp, scan_fields)) => {
                        crate::arrow::residual::filter_record_batch_by_predicates(
                            batch,
                            fp,
                            scan_fields,
                        )
                    }
                    None => Ok(batch),
                }
            })
            .boxed())
    }
}

// ---------------------------------------------------------------------------
// Paimon predicates → orc-rust conservative row-group predicates.
//
// orc-rust evaluates these predicates against row-group statistics and may keep
// non-matching rows from a selected row group. Exact residual filtering remains
// the caller's responsibility.
// ---------------------------------------------------------------------------

fn build_orc_predicate(
    predicates: Option<&FilePredicates>,
) -> Option<orc_rust::predicate::Predicate> {
    let predicates = predicates?;
    let mut orc_predicates = Vec::new();
    for predicate in &predicates.predicates {
        if let Some(predicate) = build_orc_predicate_inner(
            predicate,
            &predicates.file_fields,
            CompoundPredicateMode::RootAnd,
        ) {
            orc_predicates.push(predicate);
        }
    }

    match orc_predicates.len() {
        0 => None,
        1 => orc_predicates.pop(),
        _ => Some(orc_rust::predicate::Predicate::and(orc_predicates)),
    }
}

fn build_orc_predicate_inner(
    predicate: &Predicate,
    file_fields: &[DataField],
    mode: CompoundPredicateMode,
) -> Option<orc_rust::predicate::Predicate> {
    match predicate {
        Predicate::Leaf { .. } => build_orc_leaf_predicate(predicate, file_fields),
        Predicate::And(children) => build_orc_and_predicate(children, file_fields, mode),
        Predicate::Or(children) => build_orc_or_predicate(children, file_fields),
        Predicate::AlwaysTrue => None,
        Predicate::AlwaysFalse | Predicate::Not(_) => None,
    }
}

#[derive(Clone, Copy)]
enum CompoundPredicateMode {
    RootAnd,
    RequireExact,
}

fn build_orc_and_predicate(
    children: &[Predicate],
    file_fields: &[DataField],
    mode: CompoundPredicateMode,
) -> Option<orc_rust::predicate::Predicate> {
    let require_exact = matches!(mode, CompoundPredicateMode::RequireExact);

    let mut converted = Vec::with_capacity(children.len());
    for child in children {
        match build_orc_predicate_inner(child, file_fields, CompoundPredicateMode::RootAnd) {
            Some(predicate) => converted.push(predicate),
            None if require_exact => return None,
            None => {}
        }
    }

    match converted.len() {
        0 => None,
        1 => converted.pop(),
        _ => Some(orc_rust::predicate::Predicate::and(converted)),
    }
}

fn build_orc_or_predicate(
    children: &[Predicate],
    file_fields: &[DataField],
) -> Option<orc_rust::predicate::Predicate> {
    let mut converted = Vec::with_capacity(children.len());
    for child in children {
        converted.push(build_orc_predicate_inner(
            child,
            file_fields,
            CompoundPredicateMode::RequireExact,
        )?);
    }

    match converted.len() {
        0 => None,
        1 => converted.pop(),
        _ => Some(orc_rust::predicate::Predicate::or(converted)),
    }
}

fn build_orc_leaf_predicate(
    predicate: &Predicate,
    file_fields: &[DataField],
) -> Option<orc_rust::predicate::Predicate> {
    let Predicate::Leaf {
        column,
        index,
        op,
        literals,
        ..
    } = predicate
    else {
        return None;
    };
    // Not in the file, and its index would push the wrong column down.
    if is_row_id_column(column) {
        return None;
    }
    let file_field = file_fields.get(*index)?;
    let column = file_field.name();

    match op {
        PredicateOperator::IsNull | PredicateOperator::IsNotNull
            if data_type_supported_for_orc_predicate(file_field.data_type()) =>
        {
            Some(match op {
                PredicateOperator::IsNull => orc_rust::predicate::Predicate::is_null(column),
                PredicateOperator::IsNotNull => orc_rust::predicate::Predicate::is_not_null(column),
                _ => unreachable!(),
            })
        }
        PredicateOperator::Eq
        | PredicateOperator::Lt
        | PredicateOperator::LtEq
        | PredicateOperator::Gt
        | PredicateOperator::GtEq => {
            if *op == PredicateOperator::Eq
                && matches!(
                    file_field.data_type(),
                    DataType::Float(_) | DataType::Double(_)
                )
            {
                return None;
            }
            let literal = literals.first()?;
            let value = datum_to_orc_value(literal, file_field.data_type())?;
            Some(match op {
                PredicateOperator::Eq => orc_rust::predicate::Predicate::eq(column, value),
                PredicateOperator::Lt => orc_rust::predicate::Predicate::lt(column, value),
                PredicateOperator::LtEq => orc_rust::predicate::Predicate::lte(column, value),
                PredicateOperator::Gt => orc_rust::predicate::Predicate::gt(column, value),
                PredicateOperator::GtEq => orc_rust::predicate::Predicate::gte(column, value),
                _ => unreachable!(),
            })
        }
        PredicateOperator::In => {
            if literals.is_empty() || literals.len() > ORC_IN_PREDICATE_MAX_LITERALS {
                return None;
            }
            let mut values = Vec::with_capacity(literals.len());
            for literal in literals {
                values.push(orc_rust::predicate::Predicate::eq(
                    column,
                    datum_to_orc_value(literal, file_field.data_type())?,
                ));
            }
            Some(orc_rust::predicate::Predicate::or(values))
        }
        PredicateOperator::IsNull
        | PredicateOperator::IsNotNull
        | PredicateOperator::NotEq
        | PredicateOperator::NotIn => None,
        // String/range ops are not pushed into ORC; returning None falls open to
        // the outer stats-prune + arrow row-filter path.
        PredicateOperator::StartsWith
        | PredicateOperator::EndsWith
        | PredicateOperator::Contains
        | PredicateOperator::Like
        | PredicateOperator::Between
        | PredicateOperator::NotBetween
        | PredicateOperator::ArrayContains
        | PredicateOperator::ArraysOverlap
        | PredicateOperator::ArrayContainsAll => None,
    }
}

fn data_type_supported_for_orc_predicate(data_type: &DataType) -> bool {
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
    )
}

fn datum_to_orc_value(datum: &Datum, data_type: &DataType) -> Option<PredicateValue> {
    match (datum, data_type) {
        (Datum::Bool(value), DataType::Boolean(_)) => Some(PredicateValue::Boolean(Some(*value))),
        (Datum::TinyInt(value), DataType::TinyInt(_)) => Some(PredicateValue::Int8(Some(*value))),
        (Datum::SmallInt(value), DataType::SmallInt(_)) => {
            Some(PredicateValue::Int16(Some(*value)))
        }
        (Datum::Int(value), DataType::Int(_)) => Some(PredicateValue::Int32(Some(*value))),
        (Datum::Long(value), DataType::BigInt(_)) => Some(PredicateValue::Int64(Some(*value))),
        (Datum::Float(value), DataType::Float(_)) => Some(PredicateValue::Float32(Some(*value))),
        (Datum::Double(value), DataType::Double(_)) => Some(PredicateValue::Float64(Some(*value))),
        (Datum::String(value), DataType::Char(_) | DataType::VarChar(_)) => {
            Some(PredicateValue::Utf8(Some(value.clone())))
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Row ranges → orc_rust::RowSelection
// ---------------------------------------------------------------------------

fn build_range_row_selection(
    total_rows: usize,
    row_ranges: &[RowRange],
) -> orc_rust::row_selection::RowSelection {
    if total_rows == 0 {
        return orc_rust::row_selection::RowSelection::default();
    }

    let file_end = total_rows as i64 - 1;
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

    orc_rust::row_selection::RowSelection::from_consecutive_ranges(
        local_ranges.into_iter().map(|(s, e)| s..e),
        total_rows,
    )
}

/// Rows in an ORC file, read from its footer alone.
pub(crate) async fn read_row_count(
    reader: Box<dyn FileRead>,
    file_size: u64,
) -> crate::Result<i64> {
    let builder = ArrowReaderBuilder::try_new_async(OrcFileReader::new(file_size, reader))
        .await
        .map_err(|error| Error::UnexpectedError {
            message: format!("Failed to open ORC file: {error}"),
            source: Some(Box::new(error)),
        })?;
    let rows = builder
        .file_metadata()
        .stripe_metadatas()
        .iter()
        .map(|stripe| stripe.number_of_rows())
        .sum::<u64>();
    i64::try_from(rows).map_err(|_| Error::DataInvalid {
        message: format!("ORC file holds {rows} rows, more than a row count can carry"),
        source: None,
    })
}

// ---------------------------------------------------------------------------
// OrcFileReader — adapts paimon FileRead to orc-rust AsyncChunkReader
// ---------------------------------------------------------------------------

struct OrcFileReader {
    file_size: u64,
    r: Box<dyn FileRead>,
}

impl OrcFileReader {
    fn new(file_size: u64, r: Box<dyn FileRead>) -> Self {
        Self { file_size, r }
    }
}

impl AsyncChunkReader for OrcFileReader {
    fn len(&mut self) -> BoxFuture<'_, std::io::Result<u64>> {
        Box::pin(std::future::ready(Ok(self.file_size)))
    }

    fn get_bytes(
        &mut self,
        offset_from_start: u64,
        length: u64,
    ) -> BoxFuture<'_, std::io::Result<Bytes>> {
        Box::pin(async move {
            self.r
                .read(offset_from_start..offset_from_start + length)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType as ArrowDataType, Field, Schema};
    use orc_rust::row_selection::RowSelector;
    use std::sync::Arc;

    use crate::spec::{DateType, DecimalType, DoubleType, FloatType, IntType};

    fn field(index: i32, name: &str, data_type: DataType) -> DataField {
        DataField::new(index, name.to_string(), data_type)
    }

    fn leaf(index: usize, op: PredicateOperator, literals: Vec<Datum>) -> Predicate {
        Predicate::Leaf {
            column: format!("c{index}"),
            index,
            data_type: DataType::Int(IntType::new()),
            op,
            literals,
        }
    }

    fn file_predicates(predicates: Vec<Predicate>, file_fields: Vec<DataField>) -> FilePredicates {
        FilePredicates {
            predicates,
            row_filter_factory: None,
            file_fields,
        }
    }

    #[test]
    fn test_build_range_row_selection_single_range() {
        let ranges = vec![RowRange::new(2, 4)];
        let sel = build_range_row_selection(6, &ranges);
        // rows 0,1 skip; 2,3,4 select; 5 skip
        let expected: orc_rust::row_selection::RowSelection = vec![
            RowSelector::skip(2),
            RowSelector::select(3),
            RowSelector::skip(1),
        ]
        .into();
        assert_eq!(sel, expected);
    }

    #[test]
    fn test_build_range_row_selection_with_offset() {
        let ranges = vec![RowRange::new(1, 3)];
        let sel = build_range_row_selection(5, &ranges);
        let expected: orc_rust::row_selection::RowSelection = vec![
            RowSelector::skip(1),
            RowSelector::select(3),
            RowSelector::skip(1),
        ]
        .into();
        assert_eq!(sel, expected);
    }

    #[test]
    fn test_build_range_row_selection_out_of_file() {
        let ranges = vec![RowRange::new(10, 20)];
        let sel = build_range_row_selection(5, &ranges);
        let expected: orc_rust::row_selection::RowSelection = vec![RowSelector::skip(5)].into();
        assert_eq!(sel, expected);
    }

    #[test]
    fn test_build_orc_predicate_supported_leaf() {
        let predicates = file_predicates(
            vec![leaf(0, PredicateOperator::GtEq, vec![Datum::Int(7)])],
            vec![field(0, "id", DataType::Int(IntType::new()))],
        );

        let predicate = build_orc_predicate(Some(&predicates)).unwrap();
        assert_eq!(
            predicate,
            orc_rust::predicate::Predicate::gte("id", PredicateValue::Int32(Some(7)))
        );
    }

    #[test]
    fn test_build_orc_predicate_type_mismatch_fails_open() {
        let predicates = file_predicates(
            vec![leaf(0, PredicateOperator::Eq, vec![Datum::Long(7)])],
            vec![field(0, "id", DataType::Int(IntType::new()))],
        );

        assert!(build_orc_predicate(Some(&predicates)).is_none());
    }

    #[test]
    fn test_build_orc_predicate_unsupported_type_fails_open() {
        let predicates = file_predicates(
            vec![Predicate::Leaf {
                column: "dt".to_string(),
                index: 0,
                data_type: DataType::Date(DateType::new()),
                op: PredicateOperator::Eq,
                literals: vec![Datum::Date(1)],
            }],
            vec![field(0, "dt", DataType::Date(DateType::new()))],
        );

        assert!(build_orc_predicate(Some(&predicates)).is_none());
    }

    #[test]
    fn test_build_orc_predicate_unsupported_operator_fails_open() {
        let predicates = file_predicates(
            vec![leaf(0, PredicateOperator::NotEq, vec![Datum::Int(7)])],
            vec![field(0, "id", DataType::Int(IntType::new()))],
        );

        assert!(build_orc_predicate(Some(&predicates)).is_none());
    }

    #[test]
    fn test_build_orc_predicate_float_eq_fails_open() {
        let float_predicates = file_predicates(
            vec![Predicate::Leaf {
                column: "f".to_string(),
                index: 0,
                data_type: DataType::Float(FloatType::new()),
                op: PredicateOperator::Eq,
                literals: vec![Datum::Float(1.5)],
            }],
            vec![field(0, "f", DataType::Float(FloatType::new()))],
        );
        let double_predicates = file_predicates(
            vec![Predicate::Leaf {
                column: "d".to_string(),
                index: 0,
                data_type: DataType::Double(DoubleType::new()),
                op: PredicateOperator::Eq,
                literals: vec![Datum::Double(2.5)],
            }],
            vec![field(0, "d", DataType::Double(DoubleType::new()))],
        );

        assert!(build_orc_predicate(Some(&float_predicates)).is_none());
        assert!(build_orc_predicate(Some(&double_predicates)).is_none());
    }

    #[test]
    fn test_build_orc_predicate_is_null_supported_leaf() {
        let predicates = file_predicates(
            vec![leaf(0, PredicateOperator::IsNull, vec![])],
            vec![field(0, "id", DataType::Int(IntType::new()))],
        );

        let predicate = build_orc_predicate(Some(&predicates)).unwrap();
        assert_eq!(predicate, orc_rust::predicate::Predicate::is_null("id"));
    }

    #[test]
    fn test_build_orc_predicate_is_null_requires_supported_type() {
        let decimal_type = DataType::Decimal(DecimalType::new(10, 2).unwrap());
        let predicates = file_predicates(
            vec![Predicate::Leaf {
                column: "amount".to_string(),
                index: 0,
                data_type: decimal_type.clone(),
                op: PredicateOperator::IsNull,
                literals: vec![],
            }],
            vec![field(0, "amount", decimal_type)],
        );

        assert!(build_orc_predicate(Some(&predicates)).is_none());
    }

    #[test]
    fn test_build_orc_predicate_is_not_null_requires_supported_type() {
        let decimal_type = DataType::Decimal(DecimalType::new(10, 2).unwrap());
        let predicates = file_predicates(
            vec![Predicate::Leaf {
                column: "amount".to_string(),
                index: 0,
                data_type: decimal_type.clone(),
                op: PredicateOperator::IsNotNull,
                literals: vec![],
            }],
            vec![field(0, "amount", decimal_type)],
        );

        assert!(build_orc_predicate(Some(&predicates)).is_none());
    }

    #[test]
    fn test_build_orc_predicate_index_out_of_bounds_fails_open() {
        let predicates = file_predicates(
            vec![leaf(1, PredicateOperator::Eq, vec![Datum::Int(7)])],
            vec![field(0, "id", DataType::Int(IntType::new()))],
        );

        assert!(build_orc_predicate(Some(&predicates)).is_none());
    }

    #[test]
    fn test_build_orc_predicate_and_pushes_supported_children() {
        let predicates = file_predicates(
            vec![Predicate::and(vec![
                leaf(0, PredicateOperator::Gt, vec![Datum::Int(1)]),
                leaf(0, PredicateOperator::NotEq, vec![Datum::Int(7)]),
            ])],
            vec![field(0, "id", DataType::Int(IntType::new()))],
        );

        let predicate = build_orc_predicate(Some(&predicates)).unwrap();
        assert_eq!(
            predicate,
            orc_rust::predicate::Predicate::gt("id", PredicateValue::Int32(Some(1)))
        );
    }

    #[test]
    fn test_build_orc_predicate_top_level_and_pushes_supported_predicates() {
        let predicates = file_predicates(
            vec![
                leaf(0, PredicateOperator::Gt, vec![Datum::Int(1)]),
                leaf(0, PredicateOperator::LtEq, vec![Datum::Int(9)]),
                leaf(0, PredicateOperator::NotEq, vec![Datum::Int(7)]),
            ],
            vec![field(0, "id", DataType::Int(IntType::new()))],
        );

        let predicate = build_orc_predicate(Some(&predicates)).unwrap();
        assert_eq!(
            predicate,
            orc_rust::predicate::Predicate::and(vec![
                orc_rust::predicate::Predicate::gt("id", PredicateValue::Int32(Some(1))),
                orc_rust::predicate::Predicate::lte("id", PredicateValue::Int32(Some(9))),
            ])
        );
    }

    #[test]
    fn test_build_orc_predicate_or_requires_all_children_supported() {
        let predicates = file_predicates(
            vec![Predicate::or(vec![
                leaf(0, PredicateOperator::Lt, vec![Datum::Int(1)]),
                Predicate::Not(Box::new(leaf(
                    0,
                    PredicateOperator::Eq,
                    vec![Datum::Int(7)],
                ))),
            ])],
            vec![field(0, "id", DataType::Int(IntType::new()))],
        );

        assert!(build_orc_predicate(Some(&predicates)).is_none());
    }

    #[test]
    fn test_build_orc_predicate_or_with_nested_and_requires_exact_children() {
        let predicates = file_predicates(
            vec![Predicate::or(vec![
                Predicate::and(vec![
                    leaf(0, PredicateOperator::Gt, vec![Datum::Int(1)]),
                    leaf(0, PredicateOperator::NotEq, vec![Datum::Int(7)]),
                ]),
                leaf(0, PredicateOperator::Lt, vec![Datum::Int(0)]),
            ])],
            vec![field(0, "id", DataType::Int(IntType::new()))],
        );

        assert!(build_orc_predicate(Some(&predicates)).is_none());
    }

    #[test]
    fn test_build_orc_predicate_in_limit() {
        let predicates = file_predicates(
            vec![leaf(
                0,
                PredicateOperator::In,
                (0..=ORC_IN_PREDICATE_MAX_LITERALS)
                    .map(|value| Datum::Int(value as i32))
                    .collect(),
            )],
            vec![field(0, "id", DataType::Int(IntType::new()))],
        );

        assert!(build_orc_predicate(Some(&predicates)).is_none());
    }

    #[test]
    fn test_build_orc_predicate_in_supported_literals() {
        let predicates = file_predicates(
            vec![leaf(
                0,
                PredicateOperator::In,
                vec![Datum::Int(1), Datum::Int(3)],
            )],
            vec![field(0, "id", DataType::Int(IntType::new()))],
        );

        let predicate = build_orc_predicate(Some(&predicates)).unwrap();
        assert_eq!(
            predicate,
            orc_rust::predicate::Predicate::or(vec![
                orc_rust::predicate::Predicate::eq("id", PredicateValue::Int32(Some(1))),
                orc_rust::predicate::Predicate::eq("id", PredicateValue::Int32(Some(3))),
            ])
        );
    }

    /// Encode a single-stripe ORC file (all rows in one stripe) into memory bytes.
    fn write_single_stripe_orc(schema: Arc<Schema>, batch: &RecordBatch) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        let mut writer = orc_rust::ArrowWriterBuilder::new(&mut buf, schema)
            // Large stripe byte size keeps all rows in a single stripe so stripe
            // stats cannot exclude the non-matching rows.
            .with_stripe_byte_size(64 * 1024 * 1024)
            .try_build()
            .unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
        buf
    }

    #[tokio::test]
    async fn orc_writer_flushes_stripes_and_accepts_more_rows() {
        use crate::io::FileIOBuilder;
        use crate::spec::IntType;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            true,
        )]));
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/test_orc_writer_flush.orc";
        let mut writer = OrcFormatWriter::new(&file_io.new_output(path).unwrap(), schema.clone())
            .await
            .unwrap();
        for ids in [vec![1, 2], vec![3, 4]] {
            let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(ids))])
                .unwrap();
            writer.write(&batch).await.unwrap();
            assert_eq!(writer.pending_rows(), Some(2));
            writer.flush().await.unwrap();
            assert_eq!(writer.pending_rows(), Some(0));
            assert!(writer.num_bytes() > 0);
        }
        Box::new(writer).close().await.unwrap();

        let input = file_io.new_input(path).unwrap();
        let size = input.metadata().await.unwrap().size;
        let read_fields = [field(0, "id", DataType::Int(IntType::new()))];
        let mut stream = OrcFormatReader
            .read_batch_stream(
                Box::new(input.reader().await.unwrap()),
                size,
                &read_fields,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        let mut actual = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            actual.extend(values.values().iter().copied());
        }
        assert_eq!(actual, [1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn orc_workers_keep_multiple_open_files_independent() {
        use crate::io::FileIOBuilder;
        use crate::spec::IntType;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            true,
        )]));
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let mut open = Vec::new();
        for id in 0..(ORC_WORKER_COUNT * 2) {
            let path = format!("memory:/test_orc_worker_{id}.orc");
            let mut writer =
                OrcFormatWriter::new(&file_io.new_output(&path).unwrap(), schema.clone())
                    .await
                    .unwrap();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(vec![id as i32]))],
            )
            .unwrap();
            writer.write(&batch).await.unwrap();
            open.push((id, path, writer));
        }
        for (id, path, writer) in open {
            Box::new(writer).close().await.unwrap();
            let input = file_io.new_input(&path).unwrap();
            let size = input.metadata().await.unwrap().size;
            let fields = [field(0, "id", DataType::Int(IntType::new()))];
            let mut stream = OrcFormatReader
                .read_batch_stream(
                    Box::new(input.reader().await.unwrap()),
                    size,
                    &fields,
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();
            let batch = stream.next().await.unwrap().unwrap();
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(values.value(0), id as i32);
            assert!(stream.next().await.is_none());
        }
    }

    #[tokio::test]
    async fn test_orc_read_applies_exact_residual_filter() {
        use crate::io::FileIOBuilder;
        use crate::spec::{IntType, VarCharType};

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("age", ArrowDataType::Int32, true),
            Field::new("name", ArrowDataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])),
                Arc::new(StringArray::from(vec![
                    "apple", "banana", "apricot", "cherry", "avocado",
                ])),
            ],
        )
        .unwrap();

        let orc_bytes = write_single_stripe_orc(arrow_schema, &batch);

        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/test_orc_residual_filter.orc";
        let output = file_io.new_output(path).unwrap();
        output.write(Bytes::from(orc_bytes)).await.unwrap();

        let input = file_io.new_input(path).unwrap();
        let file_size = input.metadata().await.unwrap().size;

        let read_fields = vec![
            field(0, "age", DataType::Int(IntType::new())),
            field(1, "name", DataType::VarChar(VarCharType::string_type())),
        ];

        // age > 25 -> [30, 40, 50]; single stripe means stripe pruning cannot drop
        // the non-matching rows, so the exact residual filter must do it.
        let predicates = file_predicates(
            vec![leaf(0, PredicateOperator::Gt, vec![Datum::Int(25)])],
            read_fields.clone(),
        );

        let reader_input = input.reader().await.unwrap();
        let reader = OrcFormatReader;
        let mut stream = reader
            .read_batch_stream(
                Box::new(reader_input),
                file_size,
                &read_fields,
                Some(&predicates),
                None,
                None,
            )
            .await
            .unwrap();

        let mut ages: Vec<i32> = Vec::new();
        while let Some(result) = stream.next().await {
            let batch = result.unwrap();
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            ages.extend(col.values().iter().copied());
        }

        assert_eq!(ages, vec![30, 40, 50]);
    }

    /// Encode a single-stripe ORC file with three columns (id, name, age).
    fn write_single_stripe_orc_three_cols(schema: Arc<Schema>, batch: &RecordBatch) -> Vec<u8> {
        write_single_stripe_orc(schema, batch)
    }

    #[tokio::test]
    async fn test_orc_read_filters_on_non_projected_predicate_column() {
        // Gap A: read only [name] but filter on the non-projected [age] column.
        // The reader must scan the predicate column, filter exactly, and still
        // return the matching rows. (DataFileReader later projects to [name] by
        // name; the reader-level batch may keep the extra `age` column.)
        use crate::io::FileIOBuilder;
        use crate::spec::{IntType, VarCharType};

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, true),
            Field::new("name", ArrowDataType::Utf8, true),
            Field::new("age", ArrowDataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )
        .unwrap();

        let orc_bytes = write_single_stripe_orc_three_cols(arrow_schema, &batch);

        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/test_orc_non_projected_predicate.orc";
        let output = file_io.new_output(path).unwrap();
        output.write(Bytes::from(orc_bytes)).await.unwrap();

        let input = file_io.new_input(path).unwrap();
        let file_size = input.metadata().await.unwrap().size;

        // Read only [name] (NOT age).
        let read_fields = vec![field(
            1,
            "name",
            DataType::VarChar(VarCharType::string_type()),
        )];

        // File-level schema for predicate resolution: (id, name, age).
        let file_fields = vec![
            field(0, "id", DataType::Int(IntType::new())),
            field(1, "name", DataType::VarChar(VarCharType::string_type())),
            field(2, "age", DataType::Int(IntType::new())),
        ];

        // age > 25 -> rows c, d, e. `age` is not in read_fields, so before the fix
        // the residual evaluator can't see it and silently returns all 5 names.
        let predicates = file_predicates(
            vec![leaf(2, PredicateOperator::Gt, vec![Datum::Int(25)])],
            file_fields,
        );

        let reader_input = input.reader().await.unwrap();
        let reader = OrcFormatReader;
        let mut stream = reader
            .read_batch_stream(
                Box::new(reader_input),
                file_size,
                &read_fields,
                Some(&predicates),
                None,
                None,
            )
            .await
            .unwrap();

        let mut names: Vec<String> = Vec::new();
        while let Some(result) = stream.next().await {
            let batch = result.unwrap();
            let name_idx = batch.schema().index_of("name").unwrap();
            let col = batch
                .column(name_idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            names.extend((0..col.len()).map(|i| col.value(i).to_string()));
        }

        // Assert on FILTERED ROWS/values, not on an exact column set.
        assert_eq!(names, vec!["c", "d", "e"]);
    }

    #[tokio::test]
    async fn test_orc_read_applies_exact_residual_filter_like() {
        use crate::io::FileIOBuilder;
        use crate::spec::{IntType, VarCharType};

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("age", ArrowDataType::Int32, true),
            Field::new("name", ArrowDataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])),
                Arc::new(StringArray::from(vec![
                    "apple", "banana", "apricot", "cherry", "avocado",
                ])),
            ],
        )
        .unwrap();

        let orc_bytes = write_single_stripe_orc(arrow_schema, &batch);

        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/test_orc_residual_filter_like.orc";
        let output = file_io.new_output(path).unwrap();
        output.write(Bytes::from(orc_bytes)).await.unwrap();

        let input = file_io.new_input(path).unwrap();
        let file_size = input.metadata().await.unwrap().size;

        let read_fields = vec![
            field(0, "age", DataType::Int(IntType::new())),
            field(1, "name", DataType::VarChar(VarCharType::string_type())),
        ];

        // name like 'a%' -> apple, apricot, avocado (3 rows). `Like` is not pushed
        // into ORC stripe pruning, so this exercises the residual filter directly.
        let predicates = file_predicates(
            vec![Predicate::Leaf {
                column: "name".to_string(),
                index: 1,
                data_type: DataType::VarChar(VarCharType::string_type()),
                op: PredicateOperator::Like,
                literals: vec![Datum::String("a%".to_string())],
            }],
            read_fields.clone(),
        );

        let reader_input = input.reader().await.unwrap();
        let reader = OrcFormatReader;
        let mut stream = reader
            .read_batch_stream(
                Box::new(reader_input),
                file_size,
                &read_fields,
                Some(&predicates),
                None,
                None,
            )
            .await
            .unwrap();

        let mut names: Vec<String> = Vec::new();
        while let Some(result) = stream.next().await {
            let batch = result.unwrap();
            let col = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            names.extend((0..col.len()).map(|i| col.value(i).to_string()));
        }

        assert_eq!(names, vec!["apple", "apricot", "avocado"]);
    }
}
