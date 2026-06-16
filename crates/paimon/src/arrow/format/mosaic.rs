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
use crate::io::FileRead;
use crate::spec::DataField;
use crate::table::{ArrowRecordBatchStream, RowRange};
use crate::Error;
use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions, UInt64Array};
use arrow_schema::{DataType as ArrowDataType, SchemaRef, TimeUnit};
use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use paimon_mosaic_core::reader::{InputFile, MosaicReader, ReaderAccess};
use std::collections::HashSet;
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
        _predicates: Option<&FilePredicates>,
        batch_size: Option<usize>,
        row_selection: Option<Vec<RowRange>>,
    ) -> crate::Result<ArrowRecordBatchStream> {
        // Mosaic predicates are currently residual; callers must re-check them for exact filtering.
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
    use crate::arrow::format::FormatFileReader;
    use crate::spec::{ArrayType, DataType, IntType, RowType, VarCharType};
    use arrow_array::{Array, Int32Array, StringArray};
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

    fn write_mosaic(batch: &RecordBatch) -> Bytes {
        let out = MemOutputFile::new();
        let mut writer = MosaicWriter::new(
            out,
            batch.schema().as_ref(),
            WriterOptions {
                compression: COMPRESSION_NONE,
                num_buckets: 2,
                row_group_max_size: u64::MAX,
                ..Default::default()
            },
        )
        .unwrap();
        writer.write_batch(batch).unwrap();
        writer.close().unwrap();
        Bytes::from(writer.output().data.to_vec())
    }

    async fn read_batches(
        data: Bytes,
        read_fields: &[DataField],
        row_selection: Option<Vec<RowRange>>,
    ) -> crate::Result<Vec<RecordBatch>> {
        let file_size = data.len() as u64;
        MosaicFormatReader
            .read_batch_stream(
                Box::new(TestFileRead { data }),
                file_size,
                read_fields,
                None,
                None,
                row_selection,
            )
            .await?
            .try_collect()
            .await
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
}
