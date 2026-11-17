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

use std::collections::BTreeMap;
use std::fmt;
use std::io::Cursor;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BinaryArray, Int64Array, RecordBatch, RecordBatchOptions, StringArray,
    TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::json::{reader::infer_json_schema_from_iterator, ReaderBuilder};
use datafusion::catalog::Session;
use datafusion::common::project_schema;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{lit, Expr};
use datafusion::object_store::{ObjectStore, ObjectStoreExt};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::prelude::SessionContext;
use futures::{stream, StreamExt, TryStreamExt};
use serde_json::Value;

use super::{error, metadata, File, Format, Options};

fn json_records(bytes: &[u8]) -> Result<Vec<Value>> {
    match serde_json::from_slice::<Value>(bytes)
        .map_err(|e| error(format!("invalid multiLine JSON: {e}")))?
    {
        value @ Value::Object(_) => Ok(vec![value]),
        Value::Array(values) if values.iter().all(Value::is_object) => Ok(values),
        _ => Err(error(
            "multiLine JSON must be an object or an array of objects",
        )),
    }
}

/// Normalize Spark's quote-only escape semantics before Arrow inference, and
/// keep quoted records intact across object-store chunks.
pub(super) async fn infer_csv_schema(
    state: &dyn Session,
    store: &Arc<dyn ObjectStore>,
    files: &[File],
    options: &Options,
) -> Result<SchemaRef> {
    let format = CsvFormat::default()
        .with_has_header(options.header.unwrap_or(true))
        .with_delimiter(options.delimiter.unwrap_or(b','))
        .with_quote(options.quote.unwrap_or(b'"'))
        .with_escape(options.escape)
        .with_null_regex(
            options
                .null_value
                .as_ref()
                .map(|value| format!("^{}$", regex::escape(value))),
        );
    let mut records_to_read = 1_000;
    let mut fields = Fields::empty();
    for file in files {
        let source = store.get(&file.meta.location).await?.into_stream();
        let stream = stream::try_unfold(
            (
                source,
                CsvInput::new(options),
                Some(CsvLines::new(options)),
                Vec::new(),
                false,
            ),
            |(mut source, mut input, mut validator, mut pending, mut eof)| async move {
                while !eof {
                    match source.next().await {
                        Some(chunk) => {
                            let chunk = chunk?;
                            if let Some(validator) = &mut validator {
                                validator.validate(&chunk)?;
                            }
                            let (normalized, end) = input.normalize(&chunk);
                            let start = pending.len();
                            pending.extend_from_slice(&normalized);
                            if let Some(end) = end {
                                let tail = pending.split_off(start + end);
                                if pending.iter().all(|byte| *byte == b'\n') {
                                    pending = tail;
                                    continue;
                                }
                                return Ok(Some((
                                    pending.into(),
                                    (source, input, validator, tail, eof),
                                )));
                            }
                        }
                        None => {
                            if let Some(validator) = &validator {
                                validator.finish()?;
                            }
                            eof = true;
                        }
                    }
                }
                if pending.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some((
                        std::mem::take(&mut pending).into(),
                        (source, input, validator, pending, eof),
                    )))
                }
            },
        );
        let (schema, records_read) = format
            .infer_schema_from_stream(state, records_to_read, stream)
            .await?;
        fields = merge_csv_fields(&fields, schema.fields())?;
        records_to_read -= records_read;
        if records_to_read == 0 {
            break;
        }
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn merge_csv_fields(left: &Fields, right: &Fields) -> Result<Fields> {
    if left.is_empty() {
        return Ok(right.clone());
    }
    if right.is_empty() {
        return Ok(left.clone());
    }
    if left.len() != right.len() {
        return Err(error("CSV files have different numbers of columns"));
    }
    // Spark applies the inferred CSV schema by position across files. Keep the
    // first header's names and order even if subsequent files rename columns.
    left.iter()
        .zip(right)
        .map(|(left, right)| {
            let data_type = match (left.data_type(), right.data_type()) {
                (a, b) if a == b => a.clone(),
                (DataType::Null, other) | (other, DataType::Null) => other.clone(),
                (DataType::Int64, DataType::Float64) | (DataType::Float64, DataType::Int64) => {
                    DataType::Float64
                }
                // CSV fields are scalar strings in the source. Conflicting
                // scalar inferences, including dates/timestamps, remain strings.
                _ => DataType::Utf8,
            };
            Ok(Arc::new(Field::new(left.name(), data_type, true)))
        })
        .collect()
}

async fn infer_ndjson_schema(store: &Arc<dyn ObjectStore>, file: &File) -> Result<Schema> {
    let mut source = store.get(&file.meta.location).await?.into_stream();
    let mut pending = Vec::<u8>::new();
    let mut offset = 0;
    let mut eof = false;
    let mut values = Vec::new();
    while values.len() < 1_000 {
        let unread = &pending[offset..];
        let line_end = unread.iter().position(|byte| matches!(byte, b'\r' | b'\n'));
        if let Some(end) = line_end.or_else(|| eof.then_some(unread.len())) {
            let line = &unread[..end];
            if line.iter().any(|byte| !byte.is_ascii_whitespace()) {
                values.push(
                    serde_json::from_slice::<Value>(line)
                        .map_err(|cause| error(format!("invalid JSON: {cause}")))?,
                );
            }
            offset += end + usize::from(line_end.is_some());
            if eof && offset == pending.len() {
                break;
            }
        } else {
            pending.drain(..offset);
            offset = 0;
            match source.next().await {
                Some(chunk) => pending.extend_from_slice(&chunk?),
                None => eof = true,
            }
        }
    }
    Ok(infer_json_schema_from_iterator(values.into_iter().map(Ok))?)
}

/// Infer each file separately to bound retained JSON values, then combine field
/// types using the same numeric widening as inference within a single file.
pub(super) async fn infer_json_schema(
    store: &Arc<dyn ObjectStore>,
    files: &[File],
    multi_line: bool,
) -> Result<SchemaRef> {
    let mut fields = Fields::empty();
    for file in files {
        let schema = if multi_line {
            let bytes = store.get(&file.meta.location).await?.bytes().await?;
            infer_json_schema_from_iterator(json_records(&bytes)?.into_iter().map(Ok))?
        } else {
            infer_ndjson_schema(store, file).await?
        };
        fields = merge_json_fields(&fields, schema.fields())?;
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn merge_json_fields(left: &Fields, right: &Fields) -> Result<Fields> {
    let mut fields = left
        .iter()
        .map(|field| (field.name().clone(), field.clone()))
        .collect::<BTreeMap<_, _>>();
    for field in right {
        let merged = match fields.get(field.name()) {
            Some(previous) => Arc::new(Field::new(
                field.name(),
                merge_json_type(previous.data_type(), field.data_type())?,
                true,
            )),
            None => field.clone(),
        };
        fields.insert(field.name().clone(), merged);
    }
    Ok(fields.into_values().collect())
}

fn merge_json_type(left: &DataType, right: &DataType) -> Result<DataType> {
    use DataType::*;
    Ok(match (left, right) {
        (a, b) if a == b => a.clone(),
        (Null, other) | (other, Null) => other.clone(),
        (Int64, Float64) | (Float64, Int64) => Float64,
        (Struct(a), Struct(b)) => Struct(merge_json_fields(a, b)?),
        (List(a), List(b)) => List(Arc::new(Field::new(
            a.name(),
            merge_json_type(a.data_type(), b.data_type())?,
            true,
        ))),
        (Boolean | Int64 | Float64 | Utf8, Boolean | Int64 | Float64 | Utf8) => Utf8,
        _ => return Err(error(format!("incompatible JSON types {left} and {right}"))),
    })
}

pub(super) async fn provider(
    ctx: &SessionContext,
    store: Arc<dyn ObjectStore>,
    files: Vec<File>,
    options: Options,
    format: Format,
    schema: Option<SchemaRef>,
) -> Result<Arc<dyn TableProvider>> {
    let schema = match format {
        Format::Text => Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )])),
        Format::Binary => Arc::new(Schema::new(vec![
            Field::new("path", DataType::Utf8, false),
            Field::new(
                "modificationTime",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("length", DataType::Int64, false),
            Field::new("content", DataType::Binary, false),
        ])),
        Format::Csv => schema.ok_or_else(|| error("CSV schema is missing"))?,
        Format::Json => match schema {
            Some(schema) => schema,
            None => infer_json_schema(&store, &files, options.multi_line).await?,
        },
        _ => unreachable!(),
    };
    let source_metadata = schema
        .fields()
        .iter()
        .any(|field| field.name() == "_metadata");
    let mut plans = Vec::with_capacity(files.len());
    for file in files {
        let table = RawTable {
            store: store.clone(),
            file: file.clone(),
            options: options.clone(),
            format,
            schema: schema.clone(),
        };
        let frame = ctx.read_table(Arc::new(table))?;
        let frame = if source_metadata {
            frame
        } else {
            frame.with_column("_metadata", lit(metadata(&file)?))?
        };
        plans.push(frame.into_unoptimized_plan());
    }
    super::combined_provider(plans)
}

#[derive(Clone, Debug)]
struct RawTable {
    store: Arc<dyn ObjectStore>,
    file: File,
    options: Options,
    format: Format,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for RawTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.fields().len()).collect());
        let schema = project_schema(&self.schema, Some(&projection))?;
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Arc::new(RawExec {
            table: self.clone(),
            projection,
            schema,
            limit,
            batch_size: state.config_options().execution.batch_size.into(),
            properties,
        }))
    }
}

#[derive(Clone, Debug)]
struct RawExec {
    table: RawTable,
    projection: Vec<usize>,
    schema: SchemaRef,
    limit: Option<usize>,
    batch_size: usize,
    properties: Arc<PlanProperties>,
}

impl DisplayAs for RawExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ReadFilesExec: format={:?}, projection={:?}",
            self.table.format, self.projection
        )
    }
}

impl ExecutionPlan for RawExec {
    fn name(&self) -> &str {
        "ReadFilesExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(error("ReadFilesExec is a leaf"));
        }
        Ok(self)
    }
    fn execute(&self, partition: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(error("ReadFilesExec has one partition"));
        }
        let exec = self.clone();
        if self.table.format == Format::Csv
            || (self.table.format == Format::Json && !self.table.options.multi_line)
        {
            return exec.row_stream();
        }
        if self.table.format == Format::Text && !self.table.options.whole_text {
            return exec.text_stream();
        }
        let stream = stream::once(async move { exec.batches().await })
            .map_ok(|batches| stream::iter(batches.into_iter().map(Ok)))
            .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

impl RawExec {
    fn batch(&self, columns: Vec<ArrayRef>, count: usize) -> Result<RecordBatch> {
        let options = RecordBatchOptions::new().with_row_count(Some(count));
        let columns = self
            .projection
            .iter()
            .map(|i| columns[*i].clone())
            .collect();
        Ok(RecordBatch::try_new_with_options(
            self.schema.clone(),
            columns,
            &options,
        )?)
    }

    async fn batches(&self) -> Result<Vec<RecordBatch>> {
        if self.limit == Some(0) {
            return Ok(vec![]);
        }
        let file = &self.table.file;
        let needs_content = self.table.format != Format::Binary || self.projection.contains(&3);
        let bytes = if needs_content {
            Some(
                self.table
                    .store
                    .get(&file.meta.location)
                    .await?
                    .bytes()
                    .await?,
            )
        } else {
            None
        };
        match self.table.format {
            Format::Binary => {
                let content: ArrayRef =
                    Arc::new(BinaryArray::from(vec![bytes.as_ref().map(|b| b.as_ref())]));
                Ok(vec![self.batch(
                    vec![
                        Arc::new(StringArray::from(vec![file.url.to_string()])),
                        Arc::new(TimestampMicrosecondArray::from(vec![file
                            .meta
                            .last_modified
                            .timestamp_micros()])),
                        Arc::new(Int64Array::from(vec![
                            i64::try_from(file.meta.size).map_err(error)?
                        ])),
                        content,
                    ],
                    1,
                )?])
            }
            Format::Text => {
                let text = std::str::from_utf8(bytes.as_ref().unwrap())
                    .map_err(|e| error(format!("invalid UTF-8 text: {e}")))?;
                Ok(vec![
                    self.batch(vec![Arc::new(StringArray::from(vec![text]))], 1)?
                ])
            }
            Format::Json => {
                let values = json_records(bytes.as_ref().unwrap())?;
                let mut data = Vec::new();
                for value in values.into_iter().take(self.limit.unwrap_or(usize::MAX)) {
                    serde_json::to_writer(&mut data, &value).map_err(error)?;
                    data.push(b'\n');
                }
                let reader = ReaderBuilder::new(self.table.schema.clone())
                    .with_batch_size(self.batch_size)
                    .with_coerce_primitive(true)
                    .build(Cursor::new(data))?;
                reader
                    .map(|batch| Ok(batch?.project(&self.projection)?))
                    .collect()
            }
            _ => unreachable!(),
        }
    }

    /// Decode CSV and NDJSON incrementally in projected Arrow-sized batches.
    fn row_stream(self) -> Result<SendableRecordBatchStream> {
        let options = &self.table.options;
        let batch_size = self.batch_size.min(self.limit.unwrap_or(usize::MAX)).max(1);
        let csv = self.table.format == Format::Csv;
        let decoder = if csv {
            let mut reader = datafusion::arrow::csv::ReaderBuilder::new(self.table.schema.clone())
                .with_header(options.header.unwrap_or(true))
                .with_delimiter(options.delimiter.unwrap_or(b','))
                .with_quote(options.quote.unwrap_or(b'"'))
                .with_batch_size(batch_size)
                .with_projection(self.projection.clone());
            if let Some(escape) = options.escape {
                reader = reader.with_escape(escape);
            }
            if let Some(null) = &options.null_value {
                reader = reader.with_null_regex(
                    regex::Regex::new(&format!("^{}$", regex::escape(null))).map_err(error)?,
                );
            }
            if let Some(limit) = self.limit {
                reader = reader.with_bounds(0, limit);
            }
            RowDecoder::Csv(Box::new(reader.build_decoder()))
        } else {
            RowDecoder::Json(
                ReaderBuilder::new(self.schema.clone())
                    .with_batch_size(batch_size)
                    .with_coerce_primitive(true)
                    .build_decoder()?,
            )
        };
        let validator = csv.then(|| CsvLines::new(options));
        let json_validator = (!csv).then(JsonLines::default);
        let input = csv.then(|| CsvInput::new(options));
        let schema = self.schema.clone();
        let open = stream::once(async move {
            let source = self
                .table
                .store
                .get(&self.table.file.meta.location)
                .await?
                .into_stream();
            let stream = stream::try_unfold(
                (
                    source,
                    decoder,
                    validator,
                    json_validator,
                    input,
                    Vec::<u8>::new(),
                    0,
                    false,
                    self.limit.unwrap_or(usize::MAX),
                ),
                |(
                    mut source,
                    mut decoder,
                    mut validator,
                    mut json_validator,
                    mut input,
                    mut pending,
                    mut offset,
                    mut eof,
                    mut remaining,
                )| async move {
                    if remaining == 0 {
                        return Ok(None);
                    }
                    loop {
                        if offset == pending.len() && !eof {
                            match source.next().await {
                                Some(chunk) => {
                                    let chunk = chunk?;
                                    pending = match &mut input {
                                        Some(input) => input.normalize(&chunk).0,
                                        None => chunk.to_vec(),
                                    };
                                    offset = 0;
                                    if pending.is_empty() {
                                        continue;
                                    }
                                }
                                None => {
                                    if let Some(validator) = &validator {
                                        validator.finish()?;
                                    }
                                    eof = true;
                                }
                            }
                        }
                        let consumed = decoder.decode(&pending[offset..])?;
                        if let Some(validator) = &mut validator {
                            validator.validate(&pending[offset..offset + consumed])?;
                        }
                        if let Some(validator) = &mut json_validator {
                            validator.validate(&pending[offset..offset + consumed])?;
                        }
                        offset += consumed;
                        if consumed == 0 {
                            if let Some(batch) = decoder.flush()? {
                                let count = remaining.min(batch.num_rows());
                                let batch = batch.slice(0, count);
                                remaining -= count;
                                return Ok(Some((
                                    batch,
                                    (
                                        source,
                                        decoder,
                                        validator,
                                        json_validator,
                                        input,
                                        pending,
                                        offset,
                                        eof,
                                        remaining,
                                    ),
                                )));
                            }
                            if eof {
                                return Ok(None);
                            }
                        }
                    }
                },
            );
            Ok::<_, datafusion::error::DataFusionError>(stream)
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, open)))
    }

    /// Stream text in Arrow-sized batches; a LIMIT can stop the source early.
    fn text_stream(self) -> Result<SendableRecordBatchStream> {
        let schema = self.schema.clone();
        let open = stream::once(async move {
            let source = self
                .table
                .store
                .get(&self.table.file.meta.location)
                .await?
                .into_stream();
            let remaining = self.limit.unwrap_or(usize::MAX);
            let sep = self.table.options.line_sep.clone();
            let stream = stream::try_unfold(
                (self, source, Vec::<u8>::new(), 0, false, remaining),
                move |(exec, mut source, mut pending, mut offset, mut eof, mut remaining)| {
                    let sep = sep.clone();
                    async move {
                        if remaining == 0 {
                            return Ok(None);
                        }
                        let mut rows = Vec::new();
                        while rows.len() < exec.batch_size && remaining > 0 {
                            let unread = &pending[offset..];
                            let boundary = if let Some(sep) = &sep {
                                unread
                                    .windows(sep.len())
                                    .position(|w| w == sep.as_bytes())
                                    .map(|i| (i, sep.len()))
                            } else {
                                unread
                                    .iter()
                                    .position(|b| matches!(b, b'\r' | b'\n'))
                                    .and_then(|i| {
                                        if unread[i] == b'\r' && i + 1 == unread.len() && !eof {
                                            None
                                        } else {
                                            Some((
                                                i,
                                                if unread[i..].starts_with(b"\r\n") {
                                                    2
                                                } else {
                                                    1
                                                },
                                            ))
                                        }
                                    })
                            };
                            if let Some((end, width)) = boundary {
                                rows.push(
                                    std::str::from_utf8(&unread[..end])
                                        .map_err(error)?
                                        .to_string(),
                                );
                                offset += end + width;
                                remaining -= 1;
                            } else if eof {
                                if !unread.is_empty() {
                                    rows.push(
                                        std::str::from_utf8(unread).map_err(error)?.to_string(),
                                    );
                                    offset = pending.len();
                                    remaining -= 1;
                                }
                                break;
                            } else {
                                // Compact once per source chunk, not once per line.
                                pending.drain(..offset);
                                offset = 0;
                                match source.next().await {
                                    Some(chunk) => pending.extend_from_slice(&chunk?),
                                    None => eof = true,
                                }
                            }
                        }
                        if rows.is_empty() {
                            return Ok(None);
                        }
                        let count = rows.len();
                        let batch = exec.batch(vec![Arc::new(StringArray::from(rows))], count)?;
                        Ok(Some((
                            batch,
                            (exec, source, pending, offset, eof, remaining),
                        )))
                    }
                },
            );
            Ok::<_, datafusion::error::DataFusionError>(stream)
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, open)))
    }
}

enum RowDecoder {
    Csv(Box<datafusion::arrow::csv::reader::Decoder>),
    Json(datafusion::arrow::json::reader::Decoder),
}

/// Arrow's JSON decoder accepts arbitrary whitespace between objects. Enforce
/// the NDJSON physical-line contract for explicit schemas and sampled tails.
#[derive(Default)]
struct JsonLines {
    depth: usize,
    string: bool,
    escaped: bool,
    started: bool,
    complete: bool,
}

impl JsonLines {
    fn validate(&mut self, bytes: &[u8]) -> Result<()> {
        for &byte in bytes {
            if matches!(byte, b'\r' | b'\n') {
                if self.depth != 0 || self.string {
                    return Err(error("JSON object spans lines; set multiLine => true"));
                }
                self.started = false;
                self.complete = false;
                continue;
            }
            if self.string {
                if self.escaped {
                    self.escaped = false;
                } else if byte == b'\\' {
                    self.escaped = true;
                } else if byte == b'"' {
                    self.string = false;
                }
                continue;
            }
            if byte.is_ascii_whitespace() {
                continue;
            }
            if self.complete {
                return Err(error("JSON requires one object per line"));
            }
            if !self.started {
                if byte != b'{' {
                    return Err(error("JSON requires one object per line"));
                }
                self.started = true;
                self.depth = 1;
                continue;
            }
            match byte {
                b'"' => self.string = true,
                b'{' | b'[' => self.depth += 1,
                b'}' | b']' => {
                    self.depth = self.depth.saturating_sub(1);
                    self.complete = self.depth == 0;
                }
                _ => {}
            }
        }
        Ok(())
    }
}

impl RowDecoder {
    fn decode(&mut self, bytes: &[u8]) -> Result<usize> {
        Ok(match self {
            Self::Csv(decoder) => decoder.decode(bytes)?,
            Self::Json(decoder) => decoder.decode(bytes)?,
        })
    }

    fn flush(&mut self) -> Result<Option<RecordBatch>> {
        Ok(match self {
            Self::Csv(decoder) => decoder.flush()?,
            Self::Json(decoder) => decoder.flush()?,
        })
    }
}

#[derive(Clone, Copy)]
enum CsvPosition {
    FieldStart,
    Unquoted,
    Quoted,
    AfterQuote,
    Escaped,
}

/// Track quoting across source chunks, including escaped and doubled quotes.
struct CsvLines {
    position: CsvPosition,
    delimiter: u8,
    quote: u8,
    escape: Option<u8>,
    multi_line: bool,
}

impl CsvLines {
    fn new(options: &Options) -> Self {
        Self {
            position: CsvPosition::FieldStart,
            delimiter: options.delimiter.unwrap_or(b','),
            quote: options.quote.unwrap_or(b'"'),
            escape: options.escape,
            multi_line: options.multi_line,
        }
    }

    fn finish(&self) -> Result<()> {
        if matches!(self.position, CsvPosition::Quoted | CsvPosition::Escaped) {
            Err(error("CSV quoted field is unterminated at end of input"))
        } else {
            Ok(())
        }
    }

    fn validate(&mut self, bytes: &[u8]) -> Result<()> {
        use CsvPosition::*;
        for &byte in bytes {
            if !self.multi_line
                && matches!(self.position, Quoted | Escaped)
                && matches!(byte, b'\r' | b'\n')
            {
                return Err(error("CSV record spans lines; set multiLine => true"));
            }
            self.advance(byte);
        }
        Ok(())
    }

    fn advance(&mut self, byte: u8) {
        use CsvPosition::*;
        self.position = match self.position {
            Escaped => Quoted,
            Quoted if byte == self.quote => AfterQuote,
            Quoted if Some(byte) == self.escape => Escaped,
            Quoted => Quoted,
            AfterQuote if byte == self.quote => Quoted,
            FieldStart if byte == self.quote => Quoted,
            _ if byte == self.delimiter || matches!(byte, b'\r' | b'\n') => FieldStart,
            _ => Unquoted,
        };
    }
}

struct CsvInput {
    lines: CsvLines,
    after_cr: bool,
}

impl CsvInput {
    fn new(options: &Options) -> Self {
        Self {
            lines: CsvLines::new(options),
            after_cr: false,
        }
    }

    /// Arrow escapes every character after the escape byte. Spark only
    /// unescapes quotes and the escape byte, preserving e.g. quoted paths.
    /// Double the escape before other characters to preserve them in Arrow.
    fn normalize(&mut self, bytes: &[u8]) -> (Vec<u8>, Option<usize>) {
        use CsvPosition::*;
        let mut output = Vec::with_capacity(bytes.len());
        let mut record_end = None;
        for &byte in bytes {
            if self.after_cr && byte == b'\n' {
                self.after_cr = false;
                continue;
            }
            self.after_cr = false;
            if matches!(self.lines.position, Escaped)
                && byte != self.lines.quote
                && Some(byte) != self.lines.escape
            {
                output.push(self.lines.escape.unwrap());
            }
            self.lines.advance(byte);
            let boundary =
                matches!(self.lines.position, FieldStart) && matches!(byte, b'\r' | b'\n');
            if boundary {
                // Canonicalize record delimiters so a CRLF split across chunks
                // never becomes an isolated empty inference chunk.
                self.after_cr = byte == b'\r';
                output.push(b'\n');
                record_end = Some(output.len());
            } else {
                output.push(byte);
            }
        }
        (output, record_end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_eof_checks_quote_and_escape_state_across_chunks() {
        let options =
            Options::parse(&[lit("memory://files/data.csv"), lit("multiLine"), lit(true)]).unwrap();
        for (csv, valid) in [
            (b"1,\"closed\"".as_slice(), true),
            (b"1,\"broken".as_slice(), false),
            (b"1,\"broken\\".as_slice(), false),
            (b"1,\"multi\nline\"".as_slice(), true),
        ] {
            for split in 0..=csv.len() {
                let mut validator = CsvLines::new(&options);
                validator.validate(&csv[..split]).unwrap();
                validator.validate(&csv[split..]).unwrap();
                assert_eq!(validator.finish().is_ok(), valid, "split at {split}");
            }
        }
    }

    #[test]
    fn csv_line_validation_preserves_quote_state_at_chunk_boundaries() {
        let options = Options::parse(&[lit("memory://files/data.csv")]).unwrap();
        let valid = b"id,name\r\n1,\"a\\\"b\"\r\n2,\"a\"\"b\"\r\n3,a\"b\r\n";
        for split in 0..=valid.len() {
            let mut validator = CsvLines::new(&options);
            validator.validate(&valid[..split]).unwrap();
            validator.validate(&valid[split..]).unwrap();
        }
        for invalid in [
            b"id,name\n1,\"hello\nworld\"\n".as_slice(),
            b"id,name\n1,\"a\\\"b\rworld\"\n".as_slice(),
        ] {
            for split in 0..=invalid.len() {
                let mut validator = CsvLines::new(&options);
                assert!(validator
                    .validate(&invalid[..split])
                    .and_then(|_| validator.validate(&invalid[split..]))
                    .is_err());
            }
        }
    }

    #[test]
    fn csv_escape_normalization_preserves_paths_across_chunks() {
        let options = Options::parse(&[lit("memory://files/data.csv")]).unwrap();
        let csv = b"id,\"na\\\"me\",path\r\n1,\"a\\\"b\",\"C:\\temp\\file\"\r\n";
        let expected = b"id,\"na\\\"me\",path\n1,\"a\\\"b\",\"C:\\\\temp\\\\file\"\n";
        for split in 0..=csv.len() {
            let mut input = CsvInput::new(&options);
            let mut normalized = input.normalize(&csv[..split]).0;
            normalized.extend_from_slice(&input.normalize(&csv[split..]).0);
            assert_eq!(normalized, expected, "split at {split}");
        }
    }

    #[test]
    fn ndjson_validation_preserves_strings_and_object_state_across_chunks() {
        let valid = b"{\"v\":\"quote\\\"} bracket] and newline\\n\",\"a\":[{}]}\r\n{\"v\":1}\n";
        for split in 0..=valid.len() {
            let mut validator = JsonLines::default();
            validator.validate(&valid[..split]).unwrap();
            validator.validate(&valid[split..]).unwrap();
        }
        for invalid in [b"{\n\"v\":1\n}".as_slice(), b"{} {}\n".as_slice()] {
            for split in 0..=invalid.len() {
                let mut validator = JsonLines::default();
                assert!(validator
                    .validate(&invalid[..split])
                    .and_then(|_| validator.validate(&invalid[split..]))
                    .is_err());
            }
        }
    }
}
