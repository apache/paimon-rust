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

use super::{FilePredicates, FormatFileReader};
use crate::arrow::build_target_arrow_schema;
use crate::arrow::filtering::{predicates_may_match_with_schema, StatsAccessor};
use crate::io::FileRead;
use crate::spec::{DataField, DataType as PaimonDataType, Datum, Predicate};
use crate::table::{ArrowRecordBatchStream, RowRange};
use crate::Error;
use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions, UInt64Array};
use arrow_schema::{DataType as ArrowDataType, SchemaRef, TimeUnit};
use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use paimon_mosaic_core::reader::{InputFile, MosaicReader, ReaderAccess};
use paimon_mosaic_core::schema::MosaicSchema;
use paimon_mosaic_core::stats::ColumnStats;
use paimon_mosaic_core::values::Value as MosaicValue;
use std::collections::{HashMap, HashSet};
use std::io;

pub(crate) struct MosaicFormatReader;

const DEFAULT_BATCH_SIZE: usize = 8192;

#[async_trait]
impl FormatFileReader for MosaicFormatReader {
    async fn read_batch_stream(
        &self,
        reader: Box<dyn FileRead>,
        file_size: u64,
        read_fields: &[DataField],
        predicates: Option<&FilePredicates>,
        batch_size: Option<usize>,
        row_selection: Option<Vec<RowRange>>,
    ) -> crate::Result<ArrowRecordBatchStream> {
        let file_bytes = reader.read(0..file_size).await?;
        let mosaic_reader = MosaicReader::new(MemoryInputFile::new(file_bytes), file_size)
            .map_err(mosaic_read_error)?;

        let file_column_names = mosaic_reader
            .schema()
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<HashSet<_>>();
        let existing_read_fields = read_fields
            .iter()
            .filter(|field| file_column_names.contains(field.name()))
            .cloned()
            .collect::<Vec<_>>();
        let read_schema = build_target_arrow_schema(&existing_read_fields)?;
        validate_mosaic_schema(&read_schema)?;
        let projected_names = existing_read_fields
            .iter()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>();
        let all_projected_columns_missing = !read_fields.is_empty() && projected_names.is_empty();
        let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
        let predicate_state = predicates.map(|predicates| {
            let file_fields = predicates.file_fields.clone();
            let file_column_indices =
                build_file_column_indices(mosaic_reader.schema(), &file_fields);
            (
                predicates.predicates.clone(),
                file_fields,
                file_column_indices,
            )
        });

        Ok(try_stream! {
            let mut row_group_start = 0usize;
            for row_group_index in 0..mosaic_reader.num_row_groups() {
                let row_group_rows = mosaic_reader
                    .row_group_num_rows(row_group_index)
                    .map_err(mosaic_read_error)?;
                let selected_indices = selected_indices_for_row_group(
                    row_group_rows,
                    row_group_start,
                    row_selection.as_deref(),
                )?;
                row_group_start = row_group_start
                    .checked_add(row_group_rows)
                    .ok_or_else(|| Error::DataInvalid {
                        message: "Mosaic row group row count overflow".to_string(),
                        source: None,
                    })?;

                if let Some(indices) = selected_indices.as_ref() {
                    if indices.is_empty() {
                        continue;
                    }
                }

                if let Some((predicates, file_fields, file_column_indices)) = &predicate_state {
                    let row_group_stats = mosaic_reader
                        .row_group_stats(row_group_index)
                        .map_err(mosaic_read_error)?;
                    if !row_group_may_match(
                        row_group_rows,
                        row_group_stats,
                        file_column_indices,
                        predicates,
                        file_fields,
                    )? {
                        continue;
                    }
                }

                let batch = if all_projected_columns_missing {
                    let row_count = selected_indices
                        .as_ref()
                        .map_or(row_group_rows, UInt64Array::len);
                    empty_batch(read_schema.clone(), row_count)?
                } else {
                    let names = projected_names
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<_>>();
                    let mut row_group_reader = mosaic_reader
                        .row_group_reader_by_names(row_group_index, &names)
                        .map_err(mosaic_read_error)?;

                    let batch = row_group_reader
                        .read_columns()
                        .map_err(mosaic_read_error)?;
                    take_rows(batch, selected_indices.as_ref(), &read_schema)?
                };
                for chunk in split_batch(batch, batch_size) {
                    yield chunk;
                }
            }
        }
        .boxed())
    }
}

struct MosaicRowGroupStats<'a> {
    row_count: i64,
    row_group_stats: &'a [ColumnStats],
    file_column_indices: &'a [Option<usize>],
}

impl StatsAccessor for MosaicRowGroupStats<'_> {
    fn row_count(&self) -> i64 {
        self.row_count
    }

    fn null_count(&self, index: usize) -> Option<i64> {
        i64::try_from(self.column_stats(index)?.null_count).ok()
    }

    fn min_value(&self, index: usize, data_type: &PaimonDataType) -> Option<Datum> {
        mosaic_value_to_datum(self.column_stats(index)?.min.as_ref()?, data_type)
    }

    fn max_value(&self, index: usize, data_type: &PaimonDataType) -> Option<Datum> {
        mosaic_value_to_datum(self.column_stats(index)?.max.as_ref()?, data_type)
    }
}

impl MosaicRowGroupStats<'_> {
    fn column_stats(&self, index: usize) -> Option<&ColumnStats> {
        let column_index = self.file_column_indices.get(index).copied().flatten()?;
        self.row_group_stats
            .iter()
            .find(|stats| stats.column_index == column_index)
    }
}

fn row_group_may_match(
    row_group_rows: usize,
    row_group_stats: &[ColumnStats],
    file_column_indices: &[Option<usize>],
    predicates: &[Predicate],
    file_fields: &[DataField],
) -> crate::Result<bool> {
    let row_count = i64::try_from(row_group_rows).map_err(|e| Error::DataInvalid {
        message: "Mosaic row group row count exceeds i64".to_string(),
        source: Some(Box::new(e)),
    })?;
    let identity_mapping = (0..file_fields.len()).map(Some).collect::<Vec<_>>();
    let stats = MosaicRowGroupStats {
        row_count,
        row_group_stats,
        file_column_indices,
    };
    Ok(predicates_may_match_with_schema(
        predicates,
        &stats,
        &identity_mapping,
        file_fields,
    ))
}

fn build_file_column_indices(
    mosaic_schema: &MosaicSchema,
    file_fields: &[DataField],
) -> Vec<Option<usize>> {
    let by_name = mosaic_schema
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| (column.name.as_str(), index))
        .collect::<HashMap<_, _>>();
    file_fields
        .iter()
        .map(|field| by_name.get(field.name()).copied())
        .collect()
}

fn mosaic_value_to_datum(value: &MosaicValue, data_type: &PaimonDataType) -> Option<Datum> {
    match (value, data_type) {
        (MosaicValue::Boolean(value), PaimonDataType::Boolean(_)) => Some(Datum::Bool(*value)),
        (MosaicValue::TinyInt(value), PaimonDataType::TinyInt(_)) => Some(Datum::TinyInt(*value)),
        (MosaicValue::SmallInt(value), PaimonDataType::SmallInt(_)) => {
            Some(Datum::SmallInt(*value))
        }
        (MosaicValue::Integer(value), PaimonDataType::Int(_)) => Some(Datum::Int(*value)),
        (MosaicValue::BigInt(value), PaimonDataType::BigInt(_)) => Some(Datum::Long(*value)),
        (MosaicValue::Float(value), PaimonDataType::Float(_)) => Some(Datum::Float(*value)),
        (MosaicValue::Double(value), PaimonDataType::Double(_)) => Some(Datum::Double(*value)),
        (MosaicValue::Date(value), PaimonDataType::Date(_)) => Some(Datum::Date(*value)),
        (MosaicValue::Time(value), PaimonDataType::Time(_)) => Some(Datum::Time(*value)),
        (MosaicValue::String(value), PaimonDataType::Char(_))
        | (MosaicValue::String(value), PaimonDataType::VarChar(_)) => {
            String::from_utf8(value.clone()).ok().map(Datum::String)
        }
        (MosaicValue::Bytes(value), PaimonDataType::Binary(_))
        | (MosaicValue::Bytes(value), PaimonDataType::VarBinary(_)) => {
            Some(Datum::Bytes(value.clone()))
        }
        (MosaicValue::DecimalCompact(value), PaimonDataType::Decimal(decimal)) => {
            Some(Datum::Decimal {
                unscaled: i128::from(*value),
                precision: decimal.precision(),
                scale: decimal.scale(),
            })
        }
        (MosaicValue::TimestampMillis(value), PaimonDataType::Timestamp(timestamp))
            if timestamp.precision() <= 3 =>
        {
            Some(Datum::Timestamp {
                millis: *value,
                nanos: 0,
            })
        }
        (MosaicValue::TimestampMillis(value), PaimonDataType::LocalZonedTimestamp(timestamp))
            if timestamp.precision() <= 3 =>
        {
            Some(Datum::LocalZonedTimestamp {
                millis: *value,
                nanos: 0,
            })
        }
        (MosaicValue::TimestampMicros(value), PaimonDataType::Timestamp(timestamp))
            if (4..=6).contains(&timestamp.precision()) =>
        {
            let (millis, nanos) = micros_to_millis_nanos(*value);
            Some(Datum::Timestamp { millis, nanos })
        }
        (MosaicValue::TimestampMicros(value), PaimonDataType::LocalZonedTimestamp(timestamp))
            if (4..=6).contains(&timestamp.precision()) =>
        {
            let (millis, nanos) = micros_to_millis_nanos(*value);
            Some(Datum::LocalZonedTimestamp { millis, nanos })
        }
        (
            MosaicValue::TimestampNanos {
                millis,
                nanos_of_milli,
            },
            PaimonDataType::Timestamp(timestamp),
        ) if (7..=9).contains(&timestamp.precision()) => Some(Datum::Timestamp {
            millis: *millis,
            nanos: *nanos_of_milli,
        }),
        (
            MosaicValue::TimestampNanos {
                millis,
                nanos_of_milli,
            },
            PaimonDataType::LocalZonedTimestamp(timestamp),
        ) if (7..=9).contains(&timestamp.precision()) => Some(Datum::LocalZonedTimestamp {
            millis: *millis,
            nanos: *nanos_of_milli,
        }),
        _ => None,
    }
}

fn micros_to_millis_nanos(micros: i64) -> (i64, i32) {
    (
        micros.div_euclid(1000),
        (micros.rem_euclid(1000) * 1000) as i32,
    )
}

#[derive(Clone)]
struct MemoryInputFile {
    data: Bytes,
}

impl MemoryInputFile {
    fn new(data: Bytes) -> Self {
        Self { data }
    }
}

impl InputFile for MemoryInputFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        let offset = usize::try_from(offset).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "mosaic read offset exceeds usize",
            )
        })?;
        let end = offset.checked_add(buf.len()).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "mosaic read range overflows")
        })?;
        let src = self.data.get(offset..end).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "mosaic read range exceeds file size",
            )
        })?;
        buf.copy_from_slice(src);
        Ok(())
    }
}

fn validate_mosaic_schema(schema: &SchemaRef) -> crate::Result<()> {
    for field in schema.fields() {
        validate_mosaic_arrow_type(field.data_type()).map_err(|message| Error::Unsupported {
            message: format!(
                "Mosaic format does not support column '{}' with type {:?}: {message}",
                field.name(),
                field.data_type()
            ),
        })?;
    }
    Ok(())
}

fn validate_mosaic_arrow_type(data_type: &ArrowDataType) -> Result<(), String> {
    match data_type {
        ArrowDataType::Boolean
        | ArrowDataType::Int8
        | ArrowDataType::Int16
        | ArrowDataType::Int32
        | ArrowDataType::Int64
        | ArrowDataType::Float32
        | ArrowDataType::Float64
        | ArrowDataType::Date32
        | ArrowDataType::Utf8
        | ArrowDataType::Binary => Ok(()),
        ArrowDataType::Time32(TimeUnit::Millisecond) => Ok(()),
        ArrowDataType::Decimal128(precision, _) => {
            if *precision == 0 || *precision > 38 {
                Err(format!(
                    "Decimal precision must be in 1..=38, got {precision}"
                ))
            } else {
                Ok(())
            }
        }
        ArrowDataType::Timestamp(
            TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond,
            _,
        ) => Ok(()),
        ArrowDataType::Struct(fields) if is_timestamp_nanos_struct(fields) => Ok(()),
        other => Err(format!("unsupported Arrow type {other:?}")),
    }
}

fn is_timestamp_nanos_struct(fields: &arrow_schema::Fields) -> bool {
    fields.len() == 2
        && fields[0].name() == "millis"
        && *fields[0].data_type() == ArrowDataType::Int64
        && fields[1].name() == "nanos_of_milli"
        && *fields[1].data_type() == ArrowDataType::Int32
}

fn selected_indices_for_row_group(
    row_group_rows: usize,
    row_group_start: usize,
    row_selection: Option<&[RowRange]>,
) -> crate::Result<Option<UInt64Array>> {
    let Some(row_selection) = row_selection else {
        return Ok(None);
    };

    let row_group_end =
        row_group_start
            .checked_add(row_group_rows)
            .ok_or_else(|| Error::DataInvalid {
                message: "Mosaic row group row range overflow".to_string(),
                source: None,
            })?;

    let mut indices = Vec::new();
    for range in row_selection {
        let from = usize::try_from(range.from()).map_err(|e| Error::DataInvalid {
            message: format!(
                "Invalid negative Mosaic row selection start: {}",
                range.from()
            ),
            source: Some(Box::new(e)),
        })?;
        let to_inclusive = usize::try_from(range.to()).map_err(|e| Error::DataInvalid {
            message: format!("Invalid negative Mosaic row selection end: {}", range.to()),
            source: Some(Box::new(e)),
        })?;
        let to = to_inclusive
            .checked_add(1)
            .ok_or_else(|| Error::DataInvalid {
                message: "Mosaic row selection end overflows".to_string(),
                source: None,
            })?;
        let start = from.max(row_group_start);
        let end = to.min(row_group_end);
        if start >= end {
            continue;
        }
        indices.extend((start - row_group_start..end - row_group_start).map(|idx| idx as u64));
    }

    Ok(Some(UInt64Array::from(indices)))
}

fn take_rows(
    batch: RecordBatch,
    indices: Option<&UInt64Array>,
    target_schema: &SchemaRef,
) -> crate::Result<RecordBatch> {
    let Some(indices) = indices else {
        return ensure_schema(batch, target_schema);
    };

    if batch.num_columns() == 0 {
        return empty_batch(target_schema.clone(), indices.len());
    }

    let columns = batch
        .columns()
        .iter()
        .map(|column| {
            arrow_select::take::take(column.as_ref(), indices, None).map_err(|e| {
                Error::UnexpectedError {
                    message: format!("Failed to apply Mosaic row selection: {e}"),
                    source: Some(Box::new(e)),
                }
            })
        })
        .collect::<crate::Result<Vec<ArrayRef>>>()?;

    RecordBatch::try_new(target_schema.clone(), columns).map_err(|e| Error::UnexpectedError {
        message: format!("Failed to build Mosaic RecordBatch: {e}"),
        source: Some(Box::new(e)),
    })
}

fn ensure_schema(batch: RecordBatch, target_schema: &SchemaRef) -> crate::Result<RecordBatch> {
    if batch.schema().as_ref() == target_schema.as_ref() {
        return Ok(batch);
    }

    if batch.num_columns() == 0 {
        return empty_batch(target_schema.clone(), batch.num_rows());
    }

    RecordBatch::try_new(target_schema.clone(), batch.columns().to_vec()).map_err(|e| {
        Error::UnexpectedError {
            message: format!("Failed to align Mosaic RecordBatch schema: {e}"),
            source: Some(Box::new(e)),
        }
    })
}

fn empty_batch(schema: SchemaRef, row_count: usize) -> crate::Result<RecordBatch> {
    RecordBatch::try_new_with_options(
        schema,
        Vec::new(),
        &RecordBatchOptions::new().with_row_count(Some(row_count)),
    )
    .map_err(|e| Error::UnexpectedError {
        message: format!("Failed to build empty Mosaic RecordBatch: {e}"),
        source: Some(Box::new(e)),
    })
}

fn split_batch(batch: RecordBatch, batch_size: usize) -> Vec<RecordBatch> {
    if batch_size == 0 || batch.num_rows() <= batch_size {
        return vec![batch];
    }

    let mut batches = Vec::new();
    let mut offset = 0;
    while offset < batch.num_rows() {
        let len = batch_size.min(batch.num_rows() - offset);
        batches.push(batch.slice(offset, len));
        offset += len;
    }
    batches
}

fn mosaic_read_error(error: io::Error) -> Error {
    Error::DataInvalid {
        message: format!("Failed to read Mosaic file: {error}"),
        source: Some(Box::new(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::format::{FilePredicates, FormatFileReader};
    use crate::spec::{
        ArrayType, BigIntType, BooleanType, DataType, DateType, Datum, DecimalType, DoubleType,
        FloatType, IntType, LocalZonedTimestampType, Predicate, PredicateBuilder, RowType,
        SmallIntType, TimeType, TimestampType, TinyIntType, VarBinaryType, VarCharType,
    };
    use arrow_array::{
        Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
        Int16Array, Int32Array, Int64Array, Int8Array, StringArray, Time32MillisecondArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    };
    use arrow_schema::{DataType as ArrowDataType, Field, Schema};
    use bytes::Bytes;
    use futures::TryStreamExt;
    use paimon_mosaic_core::spec::COMPRESSION_NONE;
    use paimon_mosaic_core::writer::{MosaicWriter, OutputFile, WriterOptions};
    use std::ops::Range;
    use std::sync::Arc;

    struct TestFileRead {
        data: Bytes,
    }

    #[async_trait]
    impl FileRead for TestFileRead {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            let start = usize::try_from(range.start).unwrap();
            let end = usize::try_from(range.end).unwrap();
            Ok(self.data.slice(start..end))
        }
    }

    struct MemOutputFile {
        data: Vec<u8>,
    }

    impl MemOutputFile {
        fn new() -> Self {
            Self { data: Vec::new() }
        }
    }

    impl OutputFile for MemOutputFile {
        fn write(&mut self, data: &[u8]) -> io::Result<()> {
            self.data.extend_from_slice(data);
            Ok(())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn pos(&self) -> u64 {
            self.data.len() as u64
        }
    }

    fn data_fields() -> Vec<DataField> {
        vec![
            DataField::new(
                0,
                "id".to_string(),
                DataType::Int(IntType::with_nullable(false)),
            ),
            DataField::new(
                1,
                "name".to_string(),
                DataType::VarChar(VarCharType::with_nullable(true, 20).unwrap()),
            ),
            DataField::new(
                2,
                "score".to_string(),
                DataType::Int(IntType::with_nullable(true)),
            ),
        ]
    }

    fn field(id: i32, name: &str, data_type: DataType) -> DataField {
        DataField::new(id, name.to_string(), data_type)
    }

    fn arrow_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("name", ArrowDataType::Utf8, true),
            Field::new("score", ArrowDataType::Int32, true),
        ]))
    }

    fn sample_batch() -> RecordBatch {
        RecordBatch::try_new(
            arrow_schema(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )
        .unwrap()
    }

    fn batch(ids: Vec<i32>, names: Vec<&str>, scores: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(
            arrow_schema(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Int32Array::from(scores)),
            ],
        )
        .unwrap()
    }

    fn write_mosaic(batch: &RecordBatch) -> Bytes {
        write_mosaic_batches(std::slice::from_ref(batch), Vec::new())
    }

    fn write_mosaic_batches(batches: &[RecordBatch], stats_columns: Vec<String>) -> Bytes {
        let out = MemOutputFile::new();
        let mut writer = MosaicWriter::new(
            out,
            batches[0].schema().as_ref(),
            WriterOptions {
                compression: COMPRESSION_NONE,
                num_buckets: 2,
                row_group_max_size: 1,
                stats_columns,
                ..Default::default()
            },
        )
        .unwrap();
        for batch in batches {
            writer.write_batch(batch).unwrap();
        }
        writer.close().unwrap();
        Bytes::from(writer.output().data.to_vec())
    }

    async fn read_batches(
        data: Bytes,
        read_fields: &[DataField],
        row_selection: Option<Vec<RowRange>>,
    ) -> crate::Result<Vec<RecordBatch>> {
        read_batches_with_predicates(data, read_fields, None, row_selection).await
    }

    async fn read_batches_with_predicates(
        data: Bytes,
        read_fields: &[DataField],
        predicates: Option<&FilePredicates>,
        row_selection: Option<Vec<RowRange>>,
    ) -> crate::Result<Vec<RecordBatch>> {
        let file_size = data.len() as u64;
        MosaicFormatReader
            .read_batch_stream(
                Box::new(TestFileRead { data }),
                file_size,
                read_fields,
                predicates,
                None,
                row_selection,
            )
            .await?
            .try_collect()
            .await
    }

    fn collect_i32_column(batches: &[RecordBatch], column_index: usize) -> Vec<i32> {
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(column_index)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    fn predicate_file_predicates(
        fields: Vec<DataField>,
        predicates: Vec<Predicate>,
    ) -> FilePredicates {
        FilePredicates {
            predicates,
            file_fields: fields,
        }
    }

    fn multi_row_group_mosaic(stats_columns: Vec<String>) -> Bytes {
        write_mosaic_batches(
            &[
                batch(vec![1, 2], vec!["a", "b"], vec![10, 20]),
                batch(vec![10, 11], vec!["c", "d"], vec![30, 40]),
                batch(vec![20, 21], vec!["e", "f"], vec![50, 60]),
            ],
            stats_columns,
        )
    }

    fn timestamp_fields() -> Vec<DataField> {
        vec![
            DataField::new(
                0,
                "ts".to_string(),
                DataType::Timestamp(TimestampType::new(6).unwrap()),
            ),
            DataField::new(1, "id".to_string(), DataType::Int(IntType::new())),
        ]
    }

    fn timestamp_arrow_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("id", ArrowDataType::Int32, true),
        ]))
    }

    fn timestamp_batch(micros: Vec<i64>, ids: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(
            timestamp_arrow_schema(),
            vec![
                Arc::new(TimestampMicrosecondArray::from(micros)),
                Arc::new(Int32Array::from(ids)),
            ],
        )
        .unwrap()
    }

    fn write_timestamp_mosaic_batches(batches: &[RecordBatch]) -> Bytes {
        let out = MemOutputFile::new();
        let mut writer = MosaicWriter::new(
            out,
            batches[0].schema().as_ref(),
            WriterOptions {
                compression: COMPRESSION_NONE,
                num_buckets: 1,
                row_group_max_size: 1,
                stats_columns: vec!["ts".to_string()],
                ..Default::default()
            },
        )
        .unwrap();
        for batch in batches {
            writer.write_batch(batch).unwrap();
        }
        writer.close().unwrap();
        Bytes::from(writer.output().data.to_vec())
    }

    #[tokio::test]
    async fn test_read_basic_mosaic_file() {
        let data = write_mosaic(&sample_batch());
        let batches = read_batches(data, &data_fields(), None).await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 5);
        assert_eq!(batches[0].schema().fields().len(), 3);
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(4), 5);
    }

    #[tokio::test]
    async fn test_read_projection_order() {
        let fields = data_fields();
        let projected = vec![fields[2].clone(), fields[0].clone()];
        let data = write_mosaic(&sample_batch());
        let batches = read_batches(data, &projected, None).await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "score");
        assert_eq!(batches[0].schema().field(1).name(), "id");
        let scores = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(scores.value(2), 30);
    }

    #[tokio::test]
    async fn test_read_empty_projection() {
        let data = write_mosaic(&sample_batch());
        let batches = read_batches(data, &[], None).await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_columns(), 0);
        assert_eq!(batches[0].num_rows(), 5);
    }

    #[tokio::test]
    async fn test_read_row_selection() {
        let fields = data_fields();
        let data = write_mosaic(&sample_batch());
        let batches = read_batches(
            data,
            &fields,
            Some(vec![RowRange::new(1, 2), RowRange::new(4, 4)]),
        )
        .await
        .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.values(), &[2, 3, 5]);
    }

    #[tokio::test]
    async fn test_read_predicate_prunes_non_matching_row_groups() {
        let fields = data_fields();
        let builder = PredicateBuilder::new(&fields);
        let predicates = predicate_file_predicates(
            fields.clone(),
            vec![builder.equal("id", Datum::Int(10)).unwrap()],
        );
        let data = multi_row_group_mosaic(vec!["id".to_string()]);
        let batches = read_batches_with_predicates(data, &fields, Some(&predicates), None)
            .await
            .unwrap();

        assert_eq!(collect_i32_column(&batches, 0), vec![10, 11]);
    }

    #[tokio::test]
    async fn test_read_predicate_prunes_all_row_groups() {
        let fields = data_fields();
        let builder = PredicateBuilder::new(&fields);
        let predicates = predicate_file_predicates(
            fields.clone(),
            vec![builder.equal("id", Datum::Int(99)).unwrap()],
        );
        let data = multi_row_group_mosaic(vec!["id".to_string()]);
        let batches = read_batches_with_predicates(data, &fields, Some(&predicates), None)
            .await
            .unwrap();

        assert!(batches.is_empty());
    }

    #[tokio::test]
    async fn test_read_predicate_missing_stats_fails_open() {
        let fields = data_fields();
        let builder = PredicateBuilder::new(&fields);
        let predicates = predicate_file_predicates(
            fields.clone(),
            vec![builder.equal("id", Datum::Int(99)).unwrap()],
        );
        let data = multi_row_group_mosaic(vec!["score".to_string()]);
        let batches = read_batches_with_predicates(data, &fields, Some(&predicates), None)
            .await
            .unwrap();

        assert_eq!(collect_i32_column(&batches, 0), vec![1, 2, 10, 11, 20, 21]);
    }

    #[tokio::test]
    async fn test_read_predicate_combines_with_row_selection() {
        let fields = data_fields();
        let builder = PredicateBuilder::new(&fields);
        let predicates = predicate_file_predicates(
            fields.clone(),
            vec![builder.equal("id", Datum::Int(20)).unwrap()],
        );
        let data = multi_row_group_mosaic(vec!["id".to_string()]);
        let batches = read_batches_with_predicates(
            data,
            &fields,
            Some(&predicates),
            Some(vec![RowRange::new(0, 4)]),
        )
        .await
        .unwrap();

        assert_eq!(collect_i32_column(&batches, 0), vec![20]);
    }

    #[tokio::test]
    async fn test_read_predicate_filter_column_not_projected() {
        let fields = data_fields();
        let builder = PredicateBuilder::new(&fields);
        let predicates = predicate_file_predicates(
            fields.clone(),
            vec![builder.equal("id", Datum::Int(10)).unwrap()],
        );
        let projected = vec![fields[2].clone()];
        let data = multi_row_group_mosaic(vec!["id".to_string()]);
        let batches = read_batches_with_predicates(data, &projected, Some(&predicates), None)
            .await
            .unwrap();

        assert_eq!(collect_i32_column(&batches, 0), vec![30, 40]);
    }

    #[tokio::test]
    async fn test_read_predicate_negative_timestamp_microseconds() {
        let fields = timestamp_fields();
        let builder = PredicateBuilder::new(&fields);
        let predicates = predicate_file_predicates(
            fields.clone(),
            vec![builder
                .equal(
                    "ts",
                    Datum::Timestamp {
                        millis: -1,
                        nanos: 999_000,
                    },
                )
                .unwrap()],
        );
        let data = write_timestamp_mosaic_batches(&[
            timestamp_batch(vec![-1, -1], vec![1, 2]),
            timestamp_batch(vec![1_000], vec![3]),
        ]);
        let read_fields = vec![fields[1].clone()];
        let batches = read_batches_with_predicates(data, &read_fields, Some(&predicates), None)
            .await
            .unwrap();

        assert_eq!(collect_i32_column(&batches, 0), vec![1, 2]);
    }

    #[tokio::test]
    async fn test_read_predicate_all_missing_projection_keeps_row_count() {
        let fields = data_fields();
        let predicates = predicate_file_predicates(fields, vec![Predicate::AlwaysTrue]);
        let projected = vec![field(
            3,
            "new_score",
            DataType::Int(IntType::with_nullable(true)),
        )];
        let data = multi_row_group_mosaic(vec!["id".to_string()]);
        let batches = read_batches_with_predicates(data, &projected, Some(&predicates), None)
            .await
            .unwrap();

        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 6);
        assert!(batches.iter().all(|batch| batch.num_columns() == 0));
    }

    #[tokio::test]
    async fn test_read_predicate_missing_column_false_prunes_all_row_groups() {
        let fields = data_fields();
        let predicates = predicate_file_predicates(fields.clone(), vec![Predicate::AlwaysFalse]);
        let projected = vec![
            fields[0].clone(),
            field(3, "new_score", DataType::Int(IntType::with_nullable(true))),
        ];
        let data = multi_row_group_mosaic(vec!["id".to_string()]);
        let batches = read_batches_with_predicates(data, &projected, Some(&predicates), None)
            .await
            .unwrap();

        assert!(batches.is_empty());
    }

    #[tokio::test]
    async fn test_read_predicate_missing_column_is_null_keeps_row_groups() {
        let fields = data_fields();
        let predicates = predicate_file_predicates(fields.clone(), vec![Predicate::AlwaysTrue]);
        let projected = vec![
            fields[0].clone(),
            field(3, "new_score", DataType::Int(IntType::with_nullable(true))),
        ];
        let data = multi_row_group_mosaic(vec!["id".to_string()]);
        let batches = read_batches_with_predicates(data, &projected, Some(&predicates), None)
            .await
            .unwrap();

        assert_eq!(collect_i32_column(&batches, 0), vec![1, 2, 10, 11, 20, 21]);
    }

    #[tokio::test]
    async fn test_read_projection_with_missing_column() {
        let fields = data_fields();
        let projected = vec![
            fields[0].clone(),
            field(3, "new_score", DataType::Int(IntType::with_nullable(true))),
            fields[1].clone(),
        ];
        let data = write_mosaic(&sample_batch());
        let batches = read_batches(data, &projected, None).await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 5);
        assert_eq!(batches[0].num_columns(), 2);
        assert_eq!(batches[0].schema().field(0).name(), "id");
        assert_eq!(batches[0].schema().field(1).name(), "name");
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.values(), &[1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn test_read_projection_with_missing_unsupported_column() {
        let fields = data_fields();
        let projected = vec![
            fields[0].clone(),
            field(
                3,
                "new_items",
                DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
            ),
        ];
        let data = write_mosaic(&sample_batch());
        let batches = read_batches(data, &projected, None).await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 5);
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "id");
    }

    #[tokio::test]
    async fn test_read_projection_with_existing_unsupported_column_returns_error() {
        let projected = vec![field(
            0,
            "id",
            DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
        )];
        let data = write_mosaic(&sample_batch());
        let err = read_batches(data, &projected, None).await.unwrap_err();

        assert!(
            matches!(err, Error::Unsupported { message } if message.contains("Mosaic format does not support column 'id'"))
        );
    }

    #[tokio::test]
    async fn test_read_projection_all_columns_missing() {
        let projected = vec![
            field(3, "new_score", DataType::Int(IntType::with_nullable(true))),
            field(
                4,
                "new_name",
                DataType::VarChar(VarCharType::with_nullable(true, 20).unwrap()),
            ),
        ];
        let data = write_mosaic(&sample_batch());
        let batches = read_batches(data, &projected, None).await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 5);
        assert_eq!(batches[0].num_columns(), 0);
        assert!(batches[0].schema().fields().is_empty());
    }

    #[tokio::test]
    async fn test_read_projection_all_columns_missing_with_row_selection() {
        let projected = vec![field(
            3,
            "new_score",
            DataType::Int(IntType::with_nullable(true)),
        )];
        let data = write_mosaic(&sample_batch());
        let batches = read_batches(data, &projected, Some(vec![RowRange::new(1, 3)]))
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(batches[0].num_columns(), 0);
    }

    #[tokio::test]
    async fn test_read_projection_with_missing_column_and_row_selection() {
        let fields = data_fields();
        let projected = vec![
            fields[2].clone(),
            field(3, "new_id", DataType::Int(IntType::with_nullable(true))),
        ];
        let data = write_mosaic(&sample_batch());
        let batches = read_batches(
            data,
            &projected,
            Some(vec![RowRange::new(0, 1), RowRange::new(4, 4)]),
        )
        .await
        .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "score");
        let scores = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(scores.values(), &[10, 20, 50]);
    }

    #[test]
    fn test_validate_row_type_as_unsupported() {
        let unsupported = vec![DataField::new(
            0,
            "nested".to_string(),
            DataType::Row(RowType::new(vec![DataField::new(
                1,
                "v".to_string(),
                DataType::Int(IntType::new()),
            )])),
        )];
        let schema = build_target_arrow_schema(&unsupported).unwrap();
        let err = validate_mosaic_schema(&schema).unwrap_err();

        assert!(
            matches!(err, Error::Unsupported { message } if message.contains("Mosaic format does not support column 'nested'"))
        );
    }

    fn full_type_fields() -> Vec<DataField> {
        vec![
            field(
                0,
                "f_bool",
                DataType::Boolean(BooleanType::with_nullable(true)),
            ),
            field(
                1,
                "f_tinyint",
                DataType::TinyInt(TinyIntType::with_nullable(true)),
            ),
            field(
                2,
                "f_smallint",
                DataType::SmallInt(SmallIntType::with_nullable(true)),
            ),
            field(3, "f_int", DataType::Int(IntType::with_nullable(false))),
            field(
                4,
                "f_bigint",
                DataType::BigInt(BigIntType::with_nullable(true)),
            ),
            field(
                5,
                "f_float",
                DataType::Float(FloatType::with_nullable(true)),
            ),
            field(
                6,
                "f_double",
                DataType::Double(DoubleType::with_nullable(true)),
            ),
            field(7, "f_date", DataType::Date(DateType::with_nullable(true))),
            field(
                8,
                "f_time",
                DataType::Time(TimeType::with_nullable(true, 3).unwrap()),
            ),
            field(
                9,
                "f_string",
                DataType::VarChar(VarCharType::with_nullable(true, 20).unwrap()),
            ),
            field(
                10,
                "f_binary",
                DataType::VarBinary(VarBinaryType::try_new(true, 20).unwrap()),
            ),
            field(
                11,
                "f_decimal_compact",
                DataType::Decimal(DecimalType::with_nullable(true, 5, 2).unwrap()),
            ),
            field(
                12,
                "f_decimal_large",
                DataType::Decimal(DecimalType::with_nullable(true, 20, 0).unwrap()),
            ),
            field(
                13,
                "f_ts3",
                DataType::Timestamp(TimestampType::with_nullable(true, 3).unwrap()),
            ),
            field(
                14,
                "f_ts6",
                DataType::Timestamp(TimestampType::with_nullable(true, 6).unwrap()),
            ),
            field(
                15,
                "f_ts9",
                DataType::Timestamp(TimestampType::with_nullable(true, 9).unwrap()),
            ),
            field(
                16,
                "f_ltz",
                DataType::LocalZonedTimestamp(
                    LocalZonedTimestampType::with_nullable(true, 6).unwrap(),
                ),
            ),
        ]
    }

    /// Round-trips every scalar/temporal type Mosaic supports through write + read,
    /// asserting values survive the format. ARRAY/MAP are intentionally excluded:
    /// `paimon-mosaic-core` 0.1.0 does not support them and the reader rejects them.
    #[tokio::test]
    async fn test_read_full_types() {
        let fields = full_type_fields();
        let schema = build_target_arrow_schema(&fields).unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(BooleanArray::from(vec![Some(true), Some(false)])),
                Arc::new(Int8Array::from(vec![Some(1i8), Some(-2)])),
                Arc::new(Int16Array::from(vec![Some(100i16), Some(-200)])),
                Arc::new(Int32Array::from(vec![10, 20])),
                Arc::new(Int64Array::from(vec![Some(1_000i64), Some(-2_000)])),
                Arc::new(Float32Array::from(vec![Some(1.5f32), Some(-2.5)])),
                Arc::new(Float64Array::from(vec![Some(3.25f64), Some(-4.75)])),
                Arc::new(Date32Array::from(vec![Some(18_000), Some(19_000)])),
                Arc::new(Time32MillisecondArray::from(vec![
                    Some(3_600_000),
                    Some(7_200_000),
                ])),
                Arc::new(StringArray::from(vec![Some("hello"), Some("mosaic")])),
                Arc::new(BinaryArray::from_opt_vec(vec![
                    Some(b"ab".as_ref()),
                    Some(b"cd".as_ref()),
                ])),
                Arc::new(
                    Decimal128Array::from(vec![Some(12_345i128), Some(-678)])
                        .with_precision_and_scale(5, 2)
                        .unwrap(),
                ),
                Arc::new(
                    Decimal128Array::from(vec![Some(12_345_678_901_234_567_890i128), Some(-1)])
                        .with_precision_and_scale(20, 0)
                        .unwrap(),
                ),
                Arc::new(TimestampMillisecondArray::from(vec![
                    Some(1_700_000_000_000i64),
                    Some(-1),
                ])),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(1_700_000_000_000_000i64),
                    Some(-1),
                ])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    Some(1_700_000_000_123_456_789i64),
                    Some(-1),
                ])),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![Some(1_700_000_000_000_000i64), Some(-1)])
                        .with_timezone("UTC"),
                ),
            ],
        )
        .unwrap();

        let data = write_mosaic(&batch);
        let batches = read_batches(data, &fields, None).await.unwrap();

        assert_eq!(batches.len(), 1);
        let result = &batches[0];
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.num_columns(), fields.len());

        let col = |i: usize| result.column(i);
        let downcast = |i: usize| col(i).as_any();

        assert!(downcast(0).downcast_ref::<BooleanArray>().unwrap().value(0));
        assert!(!downcast(0).downcast_ref::<BooleanArray>().unwrap().value(1));
        assert_eq!(
            downcast(1).downcast_ref::<Int8Array>().unwrap().value(1),
            -2
        );
        assert_eq!(
            downcast(2).downcast_ref::<Int16Array>().unwrap().value(0),
            100
        );
        assert_eq!(
            downcast(3).downcast_ref::<Int32Array>().unwrap().values(),
            &[10, 20]
        );
        assert_eq!(
            downcast(4).downcast_ref::<Int64Array>().unwrap().value(1),
            -2_000
        );
        assert_eq!(
            downcast(5).downcast_ref::<Float32Array>().unwrap().value(0),
            1.5
        );
        assert_eq!(
            downcast(6).downcast_ref::<Float64Array>().unwrap().value(1),
            -4.75
        );
        assert_eq!(
            downcast(7).downcast_ref::<Date32Array>().unwrap().value(0),
            18_000
        );
        assert_eq!(
            downcast(8)
                .downcast_ref::<Time32MillisecondArray>()
                .unwrap()
                .value(0),
            3_600_000
        );
        assert_eq!(
            downcast(9).downcast_ref::<StringArray>().unwrap().value(1),
            "mosaic"
        );
        assert_eq!(
            downcast(10).downcast_ref::<BinaryArray>().unwrap().value(0),
            b"ab"
        );
        assert_eq!(
            downcast(11)
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            12_345
        );
        assert_eq!(
            downcast(12)
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            12_345_678_901_234_567_890
        );
        assert_eq!(
            downcast(13)
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(0),
            1_700_000_000_000
        );
        assert_eq!(
            downcast(14)
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .value(0),
            1_700_000_000_000_000
        );
        assert_eq!(
            downcast(15)
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .value(0),
            1_700_000_000_123_456_789
        );
        assert_eq!(
            downcast(16)
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .value(0),
            1_700_000_000_000_000
        );
        assert_eq!(
            result.schema().field(16).data_type(),
            &ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    /// Null values in nullable columns must round-trip as nulls.
    #[tokio::test]
    async fn test_read_null_values() {
        let fields = data_fields();
        let batch = RecordBatch::try_new(
            arrow_schema(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
                Arc::new(Int32Array::from(vec![Some(10), None, None])),
            ],
        )
        .unwrap();
        let data = write_mosaic(&batch);
        let batches = read_batches(data, &fields, None).await.unwrap();

        assert_eq!(batches.len(), 1);
        let result = &batches[0];
        assert_eq!(result.num_rows(), 3);

        let names = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "a");
        assert!(names.is_null(1));
        assert_eq!(names.value(2), "c");

        let scores = result
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(scores.value(0), 10);
        assert_eq!(scores.null_count(), 2);
    }

    #[test]
    fn test_mosaic_value_to_datum_conversions() {
        let ts = |p| DataType::Timestamp(TimestampType::new(p).unwrap());
        let ltz = |p| DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(p).unwrap());

        // Each supported variant maps to the matching Datum.
        assert_eq!(
            mosaic_value_to_datum(
                &MosaicValue::Boolean(true),
                &DataType::Boolean(BooleanType::new())
            ),
            Some(Datum::Bool(true))
        );
        assert_eq!(
            mosaic_value_to_datum(
                &MosaicValue::TinyInt(7),
                &DataType::TinyInt(TinyIntType::new())
            ),
            Some(Datum::TinyInt(7))
        );
        assert_eq!(
            mosaic_value_to_datum(
                &MosaicValue::SmallInt(-9),
                &DataType::SmallInt(SmallIntType::new())
            ),
            Some(Datum::SmallInt(-9))
        );
        assert_eq!(
            mosaic_value_to_datum(&MosaicValue::Integer(42), &DataType::Int(IntType::new())),
            Some(Datum::Int(42))
        );
        assert_eq!(
            mosaic_value_to_datum(
                &MosaicValue::BigInt(1_000),
                &DataType::BigInt(BigIntType::new())
            ),
            Some(Datum::Long(1_000))
        );
        assert_eq!(
            mosaic_value_to_datum(&MosaicValue::Float(1.5), &DataType::Float(FloatType::new())),
            Some(Datum::Float(1.5))
        );
        assert_eq!(
            mosaic_value_to_datum(
                &MosaicValue::Double(2.5),
                &DataType::Double(DoubleType::new())
            ),
            Some(Datum::Double(2.5))
        );
        assert_eq!(
            mosaic_value_to_datum(&MosaicValue::Date(100), &DataType::Date(DateType::new())),
            Some(Datum::Date(100))
        );
        assert_eq!(
            mosaic_value_to_datum(
                &MosaicValue::Time(200),
                &DataType::Time(TimeType::new(3).unwrap())
            ),
            Some(Datum::Time(200))
        );
        assert_eq!(
            mosaic_value_to_datum(
                &MosaicValue::String(b"hi".to_vec()),
                &DataType::VarChar(VarCharType::new(20).unwrap())
            ),
            Some(Datum::String("hi".to_string()))
        );
        assert_eq!(
            mosaic_value_to_datum(
                &MosaicValue::Bytes(vec![1, 2]),
                &DataType::VarBinary(VarBinaryType::try_new(true, 20).unwrap())
            ),
            Some(Datum::Bytes(vec![1, 2]))
        );
        assert_eq!(
            mosaic_value_to_datum(
                &MosaicValue::DecimalCompact(1_000),
                &DataType::Decimal(DecimalType::new(5, 2).unwrap())
            ),
            Some(Datum::Decimal {
                unscaled: 1_000,
                precision: 5,
                scale: 2,
            })
        );

        // Timestamp precision boundaries select the matching Mosaic encoding.
        assert_eq!(
            mosaic_value_to_datum(&MosaicValue::TimestampMillis(5), &ts(3)),
            Some(Datum::Timestamp {
                millis: 5,
                nanos: 0
            })
        );
        assert_eq!(
            mosaic_value_to_datum(&MosaicValue::TimestampMillis(5), &ltz(3)),
            Some(Datum::LocalZonedTimestamp {
                millis: 5,
                nanos: 0
            })
        );
        assert_eq!(
            mosaic_value_to_datum(&MosaicValue::TimestampMicros(1_500), &ts(6)),
            Some(Datum::Timestamp {
                millis: 1,
                nanos: 500_000,
            })
        );
        assert_eq!(
            mosaic_value_to_datum(
                &MosaicValue::TimestampNanos {
                    millis: 1,
                    nanos_of_milli: 2,
                },
                &ts(9)
            ),
            Some(Datum::Timestamp {
                millis: 1,
                nanos: 2
            })
        );

        // Ambiguous or unsupported inputs must fail open (None).
        assert_eq!(
            mosaic_value_to_datum(&MosaicValue::Null, &DataType::Int(IntType::new())),
            None
        );
        assert_eq!(
            mosaic_value_to_datum(
                &MosaicValue::Integer(1),
                &DataType::BigInt(BigIntType::new())
            ),
            None,
            "type mismatch must not convert"
        );
        assert_eq!(
            mosaic_value_to_datum(
                &MosaicValue::DecimalLarge(vec![0, 0]),
                &DataType::Decimal(DecimalType::new(20, 0).unwrap())
            ),
            None,
            "large decimal stats are not converted"
        );
        assert_eq!(
            mosaic_value_to_datum(&MosaicValue::TimestampMillis(5), &ts(6)),
            None,
            "millis encoding must not satisfy a micros-precision type"
        );
    }

    #[test]
    fn test_split_batch() {
        let chunks = split_batch(sample_batch(), 2);
        assert_eq!(chunks.len(), 3);
        assert_eq!(
            chunks.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
            vec![2, 2, 1]
        );

        let unsplit = split_batch(sample_batch(), 0);
        assert_eq!(unsplit.len(), 1);
        assert_eq!(unsplit[0].num_rows(), 5);

        let whole = split_batch(sample_batch(), 10);
        assert_eq!(whole.len(), 1);
    }
}
