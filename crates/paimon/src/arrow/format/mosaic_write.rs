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

//! Async table-writer adapter for the synchronous Mosaic encoder. The encoder
//! buffers one row group; completed blocks are drained after each input batch.

use std::collections::{HashMap, HashSet};
use std::io;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use bytes::Bytes;
use paimon_mosaic_core::writer::{MosaicWriter, OutputFile, WriterOptions};

use super::{FormatFileWriter, FormatWriteResult};
use crate::io::{FileWrite, OutputFile as PaimonOutputFile};
use crate::spec::stats::BinaryTableStats;
use crate::spec::{BinaryRowBuilder, CoreOptions, DataField, DataType, Datum};
use crate::{Error, Result};

/// A synchronous Mosaic output that only owns bytes the asynchronous adapter
/// has not yet sent to storage. `pos` includes drained bytes: Mosaic footer
/// offsets are absolute, not offsets within the pending buffer.
#[derive(Default)]
struct PendingOutput {
    pending: Vec<u8>,
    position: u64,
}

impl PendingOutput {
    fn take(&mut self) -> Bytes {
        Bytes::from(std::mem::take(&mut self.pending))
    }
}

impl OutputFile for PendingOutput {
    fn write(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.position = self
            .position
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "Mosaic file size overflow")
            })?;
        self.pending.extend_from_slice(bytes);
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn pos(&self) -> u64 {
        self.position
    }
}

pub(crate) struct MosaicFormatWriter {
    output: Box<dyn FileWrite>,
    encoder: MosaicWriter<PendingOutput>,
    schema: SchemaRef,
    physical_schema: SchemaRef,
    stats_fields: Vec<DataField>,
    pending_rows: usize,
}

impl MosaicFormatWriter {
    pub(crate) async fn new(
        output: &PaimonOutputFile,
        schema: SchemaRef,
        compression: &str,
        zstd_level: i32,
        write_fields: Option<&[DataField]>,
        format_options: Option<&HashMap<String, String>>,
    ) -> Result<Self> {
        // Java MosaicWriterFactory accepts only zstd, even though the low-level
        // Mosaic encoder also knows an uncompressed wire format.
        if !compression.eq_ignore_ascii_case("zstd") {
            return Err(Error::Unsupported {
                message: format!(
                    "Mosaic format only supports zstd compression, but got: {compression}"
                ),
            });
        }

        let physical_schema = super::timestamp_millis_schema(&schema);
        super::mosaic::validate_mosaic_schema(&physical_schema)?;
        let fields = match write_fields {
            Some(fields) => fields.to_vec(),
            None => super::row::row_type_from_arrow_schema(&schema)?,
        };
        for field in &fields {
            validate_paimon_type(field.data_type())?;
        }
        let mut options = WriterOptions {
            zstd_level,
            ..WriterOptions::default()
        };
        if let Some(format_options) = format_options {
            let core_options = CoreOptions::new(format_options);
            if let Some(block_size) = core_options.file_block_size()? {
                if block_size <= 0 {
                    return Err(Error::ConfigInvalid {
                        message: "file.block-size for Mosaic must be positive".into(),
                    });
                }
                options.row_group_max_size = block_size as u64;
            }
            if let Some(raw) = format_options.get("mosaic.num-buckets") {
                // Java's option has an int type, so values above i32::MAX must
                // fail here too instead of silently using a wider Rust usize.
                let buckets = raw.parse::<i32>().map_err(|source| Error::ConfigInvalid {
                    message: format!("Invalid mosaic.num-buckets '{raw}': {source}"),
                })?;
                if buckets <= 0 {
                    return Err(Error::ConfigInvalid {
                        message: "mosaic.num-buckets must be positive".into(),
                    });
                }
                options.num_buckets = buckets as usize;
            }
            if let Some(raw) = format_options.get("mosaic.stats-columns") {
                let mut seen = HashSet::new();
                options.stats_columns = raw
                    .split(',')
                    .map(str::trim)
                    .filter(|name| !name.is_empty())
                    // Java's statistics extractor resolves selected fields as
                    // a set, even if the option lists a name more than once.
                    .filter(|name| seen.insert(*name))
                    .map(str::to_owned)
                    .collect();
            }
        }
        if options.row_group_max_size == 0 {
            return Err(Error::ConfigInvalid {
                message: "file.block-size for Mosaic must be positive".into(),
            });
        }
        let stats_fields = options
            .stats_columns
            .iter()
            .map(|name| {
                fields
                    .iter()
                    .find(|field| field.name() == name)
                    .cloned()
                    .ok_or_else(|| Error::ConfigInvalid {
                        message: format!(
                            "Mosaic statistics column '{name}' is not in the file schema"
                        ),
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let encoder = MosaicWriter::new(PendingOutput::default(), &physical_schema, options)
            .map_err(mosaic_write_error)?;
        Ok(Self {
            output: output.writer().await?,
            encoder,
            schema,
            physical_schema,
            stats_fields,
            pending_rows: 0,
        })
    }

    async fn drain(&mut self) -> Result<()> {
        let bytes = self.encoder.output_mut().take();
        if !bytes.is_empty() {
            self.output.write(bytes).await?;
        }
        Ok(())
    }

    fn collect_stats(&self) -> Result<Option<BinaryTableStats>> {
        if self.stats_fields.is_empty() || self.encoder.num_row_groups() == 0 {
            return Ok(None);
        }
        let positions = self
            .stats_fields
            .iter()
            .map(|field| {
                self.encoder
                    .schema()
                    .columns
                    .iter()
                    .position(|column| column.name == field.name())
                    .expect("statistics field was checked against Mosaic schema")
            })
            .collect::<Vec<_>>();
        let mut minima: Vec<Option<Datum>> = vec![None; positions.len()];
        let mut maxima: Vec<Option<Datum>> = vec![None; positions.len()];
        let mut null_counts = vec![Some(0_i64); positions.len()];
        for group in 0..self.encoder.num_row_groups() {
            for stat in self.encoder.row_group_stats(group) {
                let Some(output_index) = positions
                    .iter()
                    .position(|position| *position == stat.column_index)
                else {
                    continue;
                };
                let field = &self.stats_fields[output_index];
                let count =
                    i64::try_from(stat.null_count).map_err(|source| Error::DataInvalid {
                        message: format!("Mosaic null count for '{}' exceeds i64", field.name()),
                        source: Some(Box::new(source)),
                    })?;
                let total = null_counts[output_index].as_mut().unwrap();
                *total = total.checked_add(count).ok_or_else(|| Error::DataInvalid {
                    message: format!("Mosaic null count for '{}' exceeds i64", field.name()),
                    source: None,
                })?;
                if let Some(min) = stat.min.as_ref().and_then(|value| {
                    super::mosaic::mosaic_value_to_datum(value, field.data_type())
                }) {
                    if minima[output_index]
                        .as_ref()
                        .is_none_or(|current| min < *current)
                    {
                        minima[output_index] = Some(min);
                    }
                }
                if let Some(max) = stat.max.as_ref().and_then(|value| {
                    super::mosaic::mosaic_value_to_datum(value, field.data_type())
                }) {
                    if maxima[output_index]
                        .as_ref()
                        .is_none_or(|current| max > *current)
                    {
                        maxima[output_index] = Some(max);
                    }
                }
            }
        }
        let mut min_row = BinaryRowBuilder::new(positions.len() as i32);
        let mut max_row = BinaryRowBuilder::new(positions.len() as i32);
        for (index, field) in self.stats_fields.iter().enumerate() {
            match &minima[index] {
                Some(value) => min_row.write_datum(index, value, field.data_type()),
                None => min_row.set_null_at(index),
            }
            match &maxima[index] {
                Some(value) => max_row.write_datum(index, value, field.data_type()),
                None => max_row.set_null_at(index),
            }
        }
        Ok(Some(BinaryTableStats::new(
            min_row.build_serialized(),
            max_row.build_serialized(),
            null_counts,
        )))
    }
}

/// Match MosaicFileFormat.MosaicRowTypeVisitor before the Arrow conversion
/// erases the distinction between a Paimon MAP and MULTISET.
fn validate_paimon_type(data_type: &DataType) -> Result<()> {
    match data_type {
        DataType::Array(array) => validate_paimon_type(array.element_type()),
        DataType::Map(map) => {
            validate_paimon_type(map.key_type())?;
            validate_paimon_type(map.value_type())
        }
        DataType::Variant(_)
        | DataType::Blob(_)
        | DataType::Vector(_)
        | DataType::Multiset(_)
        | DataType::Row(_) => Err(Error::Unsupported {
            message: format!("Mosaic file format does not support type {data_type}"),
        }),
        _ => Ok(()),
    }
}

#[async_trait]
impl FormatFileWriter for MosaicFormatWriter {
    async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.schema() != self.schema {
            return Err(Error::DataInvalid {
                message: "Mosaic writer input schema differs from its file schema".into(),
                source: None,
            });
        }
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let batch = if self.schema == self.physical_schema {
            batch.clone()
        } else {
            let columns = batch
                .columns()
                .iter()
                .zip(self.physical_schema.fields())
                .map(|(array, field)| {
                    if array.data_type() == field.data_type() {
                        Ok(array.clone())
                    } else {
                        arrow_cast::cast(array, field.data_type()).map_err(|source| {
                            Error::DataInvalid {
                                message: format!(
                                    "Cannot convert Mosaic column '{}' to its storage type",
                                    field.name()
                                ),
                                source: Some(Box::new(source)),
                            }
                        })
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            RecordBatch::try_new(self.physical_schema.clone(), columns).map_err(|source| {
                Error::DataInvalid {
                    message: "Cannot build Mosaic storage batch".into(),
                    source: Some(Box::new(source)),
                }
            })?
        };
        let groups_before = self.encoder.num_row_groups();
        self.encoder
            .write_batch(&batch)
            .map_err(mosaic_write_error)?;
        self.pending_rows = if self.encoder.num_row_groups() > groups_before {
            0
        } else {
            self.pending_rows.saturating_add(batch.num_rows())
        };
        self.drain().await
    }

    fn num_bytes(&self) -> usize {
        self.encoder.estimated_file_size() as usize
    }

    fn in_progress_size(&self) -> usize {
        if self.pending_rows == 0 {
            0
        } else {
            self.encoder
                .estimated_file_size()
                .saturating_sub(self.encoder.output().pos()) as usize
        }
    }

    fn pending_rows(&self) -> Option<usize> {
        Some(self.pending_rows)
    }

    async fn flush(&mut self) -> Result<()> {
        // The Mosaic encoder chooses a row-group boundary by file.block-size.
        // It exposes finalization at close, so there is no intermediate forced
        // row-group flush. Drain every completed block before returning.
        self.drain().await
    }

    async fn close(mut self: Box<Self>) -> Result<FormatWriteResult> {
        self.encoder.close().map_err(mosaic_write_error)?;
        let stats = self.collect_stats()?;
        self.drain().await?;
        let file_size = self.encoder.output().pos();
        self.output.close().await?;
        Ok(match stats {
            Some(stats) => FormatWriteResult::with_value_stats(
                file_size,
                stats,
                Some(
                    self.stats_fields
                        .iter()
                        .map(|field| field.name().to_owned())
                        .collect(),
                ),
            ),
            None => FormatWriteResult::new(file_size),
        })
    }
}

fn mosaic_write_error(source: io::Error) -> Error {
    Error::DataInvalid {
        message: format!("Failed to write Mosaic file: {source}"),
        source: Some(Box::new(source)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::build_target_arrow_schema;
    use crate::arrow::format::FormatFileReader;
    use crate::io::FileIOBuilder;
    use crate::spec::{BinaryRow, DataType, IntType, TimestampType, VarCharType};
    use arrow_array::{
        Array, Int32Array, StringArray, TimestampMillisecondArray, TimestampSecondArray,
    };
    use futures::TryStreamExt;
    use std::sync::Arc;

    fn fields() -> Vec<DataField> {
        vec![
            DataField::new(0, "id".into(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "name".into(),
                DataType::VarChar(VarCharType::string_type()),
            ),
        ]
    }

    #[tokio::test]
    async fn writes_multiple_batches_and_collects_java_style_stats() {
        let io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/mosaic-writer/values.mosaic";
        let output = io.new_output(path).unwrap();
        let fields = fields();
        let schema = build_target_arrow_schema(&fields).unwrap();
        let options = HashMap::from([
            ("mosaic.stats-columns".into(), "id, name, id".into()),
            ("mosaic.num-buckets".into(), "2".into()),
            ("file.block-size".into(), "32".into()),
        ]);
        let mut writer = MosaicFormatWriter::new(
            &output,
            schema.clone(),
            "zstd",
            1,
            Some(&fields),
            Some(&options),
        )
        .await
        .unwrap();
        let first = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![Some(3), None])),
                Arc::new(StringArray::from(vec![Some("z"), Some("b")])),
            ],
        )
        .unwrap();
        let second = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(2)])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
            ],
        )
        .unwrap();
        writer.write(&first).await.unwrap();
        writer.write(&second).await.unwrap();
        let result = Box::new(writer).close().await.unwrap();
        let value_stats = result.value_stats.unwrap();
        assert_eq!(value_stats.columns, Some(vec!["id".into(), "name".into()]));
        assert_eq!(value_stats.stats.null_counts(), &vec![Some(1), Some(1)]);
        let min = BinaryRow::from_serialized_bytes(value_stats.stats.min_values()).unwrap();
        let max = BinaryRow::from_serialized_bytes(value_stats.stats.max_values()).unwrap();
        assert_eq!(min.get_int(0).unwrap(), 1);
        assert_eq!(max.get_int(0).unwrap(), 3);
        assert_eq!(min.get_string(1).unwrap(), "a");
        assert_eq!(max.get_string(1).unwrap(), "z");

        let input = io.new_input(path).unwrap().reader().await.unwrap();
        let reader = super::super::mosaic::MosaicFormatReader::default();
        let batches: Vec<_> = reader
            .read_batch_stream(
                Box::new(input),
                result.file_size,
                &fields,
                None,
                Some(2),
                None,
            )
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let ids = batches
            .iter()
            .flat_map(|batch| {
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                (0..batch.num_rows())
                    .map(|row| (!values.is_null(row)).then(|| values.value(row)))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![Some(3), None, Some(1), Some(2)]);
    }

    #[tokio::test]
    async fn stats_preserve_option_order_across_row_groups_and_temporal_types() {
        use crate::spec::{DateType, DecimalType};
        use arrow_array::{Date32Array, Decimal128Array, TimestampMicrosecondArray};

        let fields = vec![
            DataField::new(
                0,
                "ts".into(),
                DataType::Timestamp(TimestampType::new(6).unwrap()),
            ),
            DataField::new(
                1,
                "amount".into(),
                DataType::Decimal(DecimalType::new(10, 2).unwrap()),
            ),
            DataField::new(2, "day".into(), DataType::Date(DateType::new())),
        ];
        let schema = build_target_arrow_schema(&fields).unwrap();
        let options = HashMap::from([
            ("mosaic.stats-columns".into(), "day, amount, ts".into()),
            ("file.block-size".into(), "1".into()),
        ]);
        let io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/mosaic-writer/stats-temporal.mosaic";
        let mut writer = MosaicFormatWriter::new(
            &io.new_output(path).unwrap(),
            schema.clone(),
            "zstd",
            1,
            Some(&fields),
            Some(&options),
        )
        .await
        .unwrap();
        for (timestamps, amounts, days) in [
            (
                vec![Some(3_000_001), None],
                vec![Some(1234), None],
                vec![Some(10), None],
            ),
            (
                vec![Some(1_001_234), Some(2_000_000)],
                vec![Some(-234), Some(500)],
                vec![Some(0), Some(5)],
            ),
        ] {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampMicrosecondArray::from(timestamps)),
                    Arc::new(
                        Decimal128Array::from(amounts)
                            .with_precision_and_scale(10, 2)
                            .unwrap(),
                    ),
                    Arc::new(Date32Array::from(days)),
                ],
            )
            .unwrap();
            writer.write(&batch).await.unwrap();
        }
        let result = Box::new(writer).close().await.unwrap();
        let stats = result.value_stats.unwrap();
        assert_eq!(
            stats.columns,
            Some(vec!["day".into(), "amount".into(), "ts".into()])
        );
        assert_eq!(stats.stats.null_counts(), &[Some(1), Some(1), Some(1)]);
        let min = BinaryRow::from_serialized_bytes(stats.stats.min_values()).unwrap();
        let max = BinaryRow::from_serialized_bytes(stats.stats.max_values()).unwrap();
        assert_eq!(min.get_int(0).unwrap(), 0);
        assert_eq!(max.get_int(0).unwrap(), 10);
        assert_eq!(min.get_decimal_unscaled(1, 10).unwrap(), -234);
        assert_eq!(max.get_decimal_unscaled(1, 10).unwrap(), 1234);
        assert_eq!(min.get_timestamp_raw(2, 6).unwrap(), (1001, 234_000));
        assert_eq!(max.get_timestamp_raw(2, 6).unwrap(), (3000, 1000));
    }

    #[tokio::test]
    async fn timestamp_zero_uses_mosaic_millisecond_storage() {
        let io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/mosaic-writer/timestamp.mosaic";
        let field = DataField::new(
            0,
            "ts".into(),
            DataType::Timestamp(TimestampType::new(0).unwrap()),
        );
        let fields = vec![field];
        let schema = build_target_arrow_schema(&fields).unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(TimestampSecondArray::from(vec![
                Some(-1),
                Some(1_700_000_001),
            ]))],
        )
        .unwrap();
        let mut writer = MosaicFormatWriter::new(
            &io.new_output(path).unwrap(),
            schema,
            "zstd",
            1,
            Some(&fields),
            None,
        )
        .await
        .unwrap();
        writer.write(&batch).await.unwrap();
        let result = Box::new(writer).close().await.unwrap();
        let input = io.new_input(path).unwrap().reader().await.unwrap();
        let reader = super::super::mosaic::MosaicFormatReader::default();
        let batches: Vec<_> = reader
            .read_batch_stream(Box::new(input), result.file_size, &fields, None, None, None)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(values.values(), &[-1_000, 1_700_000_001_000]);
    }

    #[tokio::test]
    async fn nested_timestamp_zero_uses_millisecond_storage() {
        use crate::spec::ArrayType;
        use arrow_array::ListArray;
        use arrow_buffer::{OffsetBuffer, ScalarBuffer};
        use arrow_schema::DataType as ArrowType;

        let fields = vec![DataField::new(
            0,
            "times".into(),
            DataType::Array(ArrayType::new(DataType::Timestamp(
                TimestampType::new(0).unwrap(),
            ))),
        )];
        let schema = build_target_arrow_schema(&fields).unwrap();
        let item = match schema.field(0).data_type() {
            ArrowType::List(item) => item.clone(),
            other => panic!("expected list, got {other:?}"),
        };
        let array = ListArray::try_new(
            item,
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 3])),
            Arc::new(TimestampSecondArray::from(vec![Some(-1), None, Some(42)])),
            None,
        )
        .unwrap();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
        let io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/mosaic-writer/nested-timestamp.mosaic";
        let mut writer = MosaicFormatWriter::new(
            &io.new_output(path).unwrap(),
            schema,
            "zstd",
            1,
            Some(&fields),
            None,
        )
        .await
        .unwrap();
        writer.write(&batch).await.unwrap();
        let result = Box::new(writer).close().await.unwrap();
        let input = io.new_input(path).unwrap().reader().await.unwrap();
        let decoded: Vec<RecordBatch> = super::super::mosaic::MosaicFormatReader::default()
            .read_batch_stream(Box::new(input), result.file_size, &fields, None, None, None)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(decoded.len(), 1);
        let array = decoded[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let times = array.values();
        let times = times
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(times.values(), &[-1_000, 0, 42_000]);
        assert!(times.is_null(1));
        assert!(array.is_valid(0));
        assert!(array.is_valid(1));
    }

    #[tokio::test]
    async fn rejects_unsupported_compression_and_invalid_configuration() {
        let io = FileIOBuilder::new("memory").build().unwrap();
        let fields = fields();
        let schema = build_target_arrow_schema(&fields).unwrap();
        let output = io
            .new_output("memory:/mosaic-writer/invalid.mosaic")
            .unwrap();
        let error = match MosaicFormatWriter::new(
            &output,
            schema.clone(),
            "snappy",
            1,
            Some(&fields),
            None,
        )
        .await
        {
            Ok(_) => panic!("snappy must be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("only supports zstd"));

        for (option, value) in [
            ("mosaic.num-buckets", "0"),
            ("mosaic.num-buckets", "-1"),
            ("mosaic.num-buckets", "2147483648"),
            ("mosaic.num-buckets", "bad"),
            ("file.block-size", "0"),
            ("file.block-size", "-1"),
            ("mosaic.stats-columns", "missing"),
        ] {
            let options = HashMap::from([(option.to_string(), value.to_string())]);
            let result = MosaicFormatWriter::new(
                &output,
                schema.clone(),
                "zstd",
                1,
                Some(&fields),
                Some(&options),
            )
            .await;
            assert!(result.is_err(), "{option}={value} should fail");
        }
    }

    #[tokio::test]
    async fn empty_and_multi_group_files_remain_readable() {
        let io = FileIOBuilder::new("memory").build().unwrap();
        let fields = fields();
        let schema = build_target_arrow_schema(&fields).unwrap();
        let options = HashMap::from([("file.block-size".into(), "1".into())]);
        let empty_path = "memory:/mosaic-writer/empty.mosaic";
        let empty_writer = MosaicFormatWriter::new(
            &io.new_output(empty_path).unwrap(),
            schema.clone(),
            "zstd",
            1,
            Some(&fields),
            Some(&options),
        )
        .await
        .unwrap();
        let empty = Box::new(empty_writer).close().await.unwrap();
        let input = io.new_input(empty_path).unwrap().reader().await.unwrap();
        let empty_batches: Vec<RecordBatch> = super::super::mosaic::MosaicFormatReader::default()
            .read_batch_stream(Box::new(input), empty.file_size, &fields, None, None, None)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert!(empty_batches.is_empty());

        let path = "memory:/mosaic-writer/many-groups.mosaic";
        let mut writer = MosaicFormatWriter::new(
            &io.new_output(path).unwrap(),
            schema.clone(),
            "zstd",
            1,
            Some(&fields),
            Some(&options),
        )
        .await
        .unwrap();
        for start in [0, 10, 20] {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from((start..start + 10).collect::<Vec<_>>())),
                    Arc::new(StringArray::from(
                        (0..10).map(|_| Some("payload")).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            writer.write(&batch).await.unwrap();
            assert_eq!(writer.pending_rows(), Some(0));
        }
        let result = Box::new(writer).close().await.unwrap();
        let input = io.new_input(path).unwrap().reader().await.unwrap();
        let batches: Vec<RecordBatch> = super::super::mosaic::MosaicFormatReader::default()
            .read_batch_stream(Box::new(input), result.file_size, &fields, None, None, None)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 30);
    }

    #[tokio::test]
    async fn writer_reports_buffered_rows_until_a_row_group_is_emitted() {
        let io = FileIOBuilder::new("memory").build().unwrap();
        let fields = fields();
        let schema = build_target_arrow_schema(&fields).unwrap();
        let path = "memory:/mosaic-writer/buffer-lifetime.mosaic";
        let options = HashMap::from([("file.block-size".into(), "1gb".into())]);
        let mut writer = MosaicFormatWriter::new(
            &io.new_output(path).unwrap(),
            schema.clone(),
            "zstd",
            1,
            Some(&fields),
            Some(&options),
        )
        .await
        .unwrap();
        assert_eq!(writer.pending_rows(), Some(0));
        assert_eq!(writer.in_progress_size(), 0);
        for value in [1, 2, 3] {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![value])),
                    Arc::new(StringArray::from(vec!["payload"])),
                ],
            )
            .unwrap();
            writer.write(&batch).await.unwrap();
            assert_eq!(writer.pending_rows(), Some(value as usize));
            assert!(writer.in_progress_size() > 0);
            writer.flush().await.unwrap();
            assert_eq!(writer.pending_rows(), Some(value as usize));
        }
        let result = Box::new(writer).close().await.unwrap();
        let input = io.new_input(path).unwrap().reader().await.unwrap();
        let batches: Vec<RecordBatch> = super::super::mosaic::MosaicFormatReader::default()
            .read_batch_stream(Box::new(input), result.file_size, &fields, None, None, None)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    }

    #[tokio::test]
    async fn written_row_groups_support_projection_and_row_selection() {
        use crate::table::RowRange;

        let io = FileIOBuilder::new("memory").build().unwrap();
        let fields = fields();
        let schema = build_target_arrow_schema(&fields).unwrap();
        let options = HashMap::from([("file.block-size".into(), "1".into())]);
        let path = "memory:/mosaic-writer/selected-groups.mosaic";
        let mut writer = MosaicFormatWriter::new(
            &io.new_output(path).unwrap(),
            schema.clone(),
            "zstd",
            1,
            Some(&fields),
            Some(&options),
        )
        .await
        .unwrap();
        for start in [0, 5, 10] {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from((start..start + 5).collect::<Vec<_>>())),
                    Arc::new(StringArray::from(
                        (start..start + 5)
                            .map(|value| format!("name-{value}"))
                            .collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            writer.write(&batch).await.unwrap();
        }
        let result = Box::new(writer).close().await.unwrap();
        let input = io.new_input(path).unwrap().reader().await.unwrap();
        let selected: Vec<RecordBatch> = super::super::mosaic::MosaicFormatReader::default()
            .read_batch_stream(
                Box::new(input),
                result.file_size,
                &fields[1..],
                None,
                Some(2),
                Some(vec![RowRange::new(3, 6), RowRange::new(12, 13)]),
            )
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let names = selected
            .iter()
            .flat_map(|batch| {
                assert_eq!(batch.num_columns(), 1);
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                values
                    .iter()
                    .map(|value| value.unwrap().to_owned())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec!["name-3", "name-4", "name-5", "name-6", "name-12", "name-13"]
        );
    }

    #[tokio::test]
    async fn rejects_different_input_schema_before_emitting_rows() {
        let io = FileIOBuilder::new("memory").build().unwrap();
        let fields = fields();
        let schema = build_target_arrow_schema(&fields).unwrap();
        let path = "memory:/mosaic-writer/mismatch.mosaic";
        let mut writer = MosaicFormatWriter::new(
            &io.new_output(path).unwrap(),
            schema,
            "zstd",
            1,
            Some(&fields),
            None,
        )
        .await
        .unwrap();
        let wrong_batch = RecordBatch::try_new(
            Arc::new(arrow_schema::Schema::new(vec![
                arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, true),
                arrow_schema::Field::new("id", arrow_schema::DataType::Int32, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["wrong"])),
                Arc::new(Int32Array::from(vec![1])),
            ],
        )
        .unwrap();
        let error = writer.write(&wrong_batch).await.unwrap_err();
        assert!(error.to_string().contains("schema differs"));
        let result = Box::new(writer).close().await.unwrap();
        let input = io.new_input(path).unwrap().reader().await.unwrap();
        let batches: Vec<RecordBatch> = super::super::mosaic::MosaicFormatReader::default()
            .read_batch_stream(Box::new(input), result.file_size, &fields, None, None, None)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert!(batches.is_empty());
    }

    #[tokio::test]
    async fn round_trips_java_supported_scalars_and_collection_types() {
        use crate::spec::{
            ArrayType, BigIntType, BooleanType, DateType, DecimalType, DoubleType, FloatType,
            LocalZonedTimestampType, MapType, SmallIntType, TimeType, TinyIntType,
        };
        use arrow_array::{
            BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int16Array,
            Int64Array, Int8Array, ListArray, MapArray, StructArray, Time32MillisecondArray,
            TimestampMicrosecondArray, TimestampMillisecondArray,
        };
        use arrow_buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
        use arrow_schema::{DataType as ArrowType, Field};

        let fields = vec![
            DataField::new(0, "flag".into(), DataType::Boolean(BooleanType::new())),
            DataField::new(1, "tiny".into(), DataType::TinyInt(TinyIntType::new())),
            DataField::new(2, "small".into(), DataType::SmallInt(SmallIntType::new())),
            DataField::new(3, "big".into(), DataType::BigInt(BigIntType::new())),
            DataField::new(4, "float".into(), DataType::Float(FloatType::new())),
            DataField::new(5, "double".into(), DataType::Double(DoubleType::new())),
            DataField::new(6, "date".into(), DataType::Date(DateType::new())),
            DataField::new(7, "time".into(), DataType::Time(TimeType::new(3).unwrap())),
            DataField::new(
                8,
                "decimal5".into(),
                DataType::Decimal(DecimalType::new(5, 2).unwrap()),
            ),
            DataField::new(
                9,
                "decimal20".into(),
                DataType::Decimal(DecimalType::new(20, 0).unwrap()),
            ),
            DataField::new(
                10,
                "ts3".into(),
                DataType::Timestamp(TimestampType::new(3).unwrap()),
            ),
            DataField::new(
                11,
                "ts6".into(),
                DataType::Timestamp(TimestampType::new(6).unwrap()),
            ),
            DataField::new(
                12,
                "ltz6".into(),
                DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(6).unwrap()),
            ),
            DataField::new(
                13,
                "array".into(),
                DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
            ),
            DataField::new(
                14,
                "map".into(),
                DataType::Map(MapType::new(
                    DataType::VarChar(VarCharType::string_type()),
                    DataType::Int(IntType::new()),
                )),
            ),
        ];
        let schema = build_target_arrow_schema(&fields).unwrap();
        let list_field = match schema.field(13).data_type() {
            ArrowType::List(field) => field.clone(),
            other => panic!("expected list, got {other:?}"),
        };
        let list = ListArray::try_new(
            list_field,
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 3])),
            Arc::new(Int32Array::from(vec![Some(10), None, Some(-1)])),
            Some(NullBuffer::new(BooleanBuffer::from(vec![
                true, false, true,
            ]))),
        )
        .unwrap();
        let map_field = match schema.field(14).data_type() {
            ArrowType::Map(field, _) => field.clone(),
            other => panic!("expected map, got {other:?}"),
        };
        let entry_fields = match map_field.data_type() {
            ArrowType::Struct(fields) => fields.clone(),
            other => panic!("expected map entries struct, got {other:?}"),
        };
        let entries = StructArray::try_new(
            entry_fields,
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int32Array::from(vec![Some(1), None])),
            ],
            None,
        )
        .unwrap();
        let map = MapArray::try_new(
            Arc::new(Field::clone(&map_field)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 2])),
            entries,
            Some(NullBuffer::new(BooleanBuffer::from(vec![
                true, false, true,
            ]))),
            false,
        )
        .unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
                Arc::new(Int8Array::from(vec![Some(7), None, Some(-8)])),
                Arc::new(Int16Array::from(vec![Some(256), None, Some(-512)])),
                Arc::new(Int64Array::from(vec![Some(9_876_543_210), None, Some(-1)])),
                Arc::new(Float32Array::from(vec![Some(1.5), None, Some(-2.5)])),
                Arc::new(Float64Array::from(vec![Some(3.25), None, Some(-4.75)])),
                Arc::new(Date32Array::from(vec![Some(18_000), None, Some(-1)])),
                Arc::new(Time32MillisecondArray::from(vec![
                    Some(3_600_000),
                    None,
                    Some(86_399_999),
                ])),
                Arc::new(
                    Decimal128Array::from(vec![Some(12_345), None, Some(-678)])
                        .with_precision_and_scale(5, 2)
                        .unwrap(),
                ),
                Arc::new(
                    Decimal128Array::from(vec![Some(12_345_678_901_234_567_890), None, Some(-1)])
                        .with_precision_and_scale(20, 0)
                        .unwrap(),
                ),
                Arc::new(TimestampMillisecondArray::from(vec![
                    Some(1_700_000_000_000),
                    None,
                    Some(-1),
                ])),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(1_700_000_000_000_001),
                    None,
                    Some(-1),
                ])),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![
                        Some(1_700_000_000_000_001),
                        None,
                        Some(-1),
                    ])
                    .with_timezone("UTC"),
                ),
                Arc::new(list),
                Arc::new(map),
            ],
        )
        .unwrap();
        let io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/mosaic-writer/all-types.mosaic";
        let mut writer = MosaicFormatWriter::new(
            &io.new_output(path).unwrap(),
            schema,
            "zstd",
            1,
            Some(&fields),
            None,
        )
        .await
        .unwrap();
        writer.write(&batch).await.unwrap();
        let result = Box::new(writer).close().await.unwrap();
        let input = io.new_input(path).unwrap().reader().await.unwrap();
        let decoded: Vec<RecordBatch> = super::super::mosaic::MosaicFormatReader::default()
            .read_batch_stream(Box::new(input), result.file_size, &fields, None, None, None)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].num_rows(), 3);
        assert_eq!(decoded[0].num_columns(), fields.len());
        for (index, field) in fields.iter().enumerate() {
            assert_eq!(
                decoded[0].column(index).to_data(),
                batch.column(index).to_data(),
                "round-trip differs for {}",
                field.name()
            );
        }
    }

    #[test]
    fn rejects_java_unsupported_paimon_types_even_when_arrow_shape_is_supported() {
        use crate::spec::{ArrayType, BlobType, MapType, MultisetType, VariantType, VectorType};

        let int = DataType::Int(IntType::new());
        let unsupported = [
            DataType::Blob(BlobType::new()),
            DataType::Variant(VariantType::new()),
            DataType::Vector(VectorType::new(3, int.clone()).unwrap()),
            DataType::Multiset(MultisetType::new(int.clone())),
        ];
        for data_type in unsupported {
            let error = validate_paimon_type(&data_type).unwrap_err();
            assert!(
                error.to_string().contains("does not support type"),
                "{error}"
            );
        }

        let nested = DataType::Map(MapType::new(
            DataType::VarChar(VarCharType::string_type()),
            DataType::Array(ArrayType::new(DataType::Multiset(MultisetType::new(
                int.clone(),
            )))),
        ));
        let error = validate_paimon_type(&nested).unwrap_err();
        assert!(error.to_string().contains("MULTISET"));
        let supported = DataType::Array(ArrayType::new(DataType::Map(MapType::new(
            DataType::VarChar(VarCharType::string_type()),
            int,
        ))));
        validate_paimon_type(&supported).unwrap();
    }

    #[tokio::test]
    async fn multiset_writer_is_rejected_before_file_creation() {
        use crate::spec::MultisetType;

        let io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/mosaic-writer/multiset.mosaic";
        let fields = vec![DataField::new(
            0,
            "bag".into(),
            DataType::Multiset(MultisetType::new(DataType::Int(IntType::new()))),
        )];
        let schema = build_target_arrow_schema(&fields).unwrap();
        let error = match MosaicFormatWriter::new(
            &io.new_output(path).unwrap(),
            schema,
            "zstd",
            1,
            Some(&fields),
            None,
        )
        .await
        {
            Ok(_) => panic!("Mosaic must reject Paimon MULTISET"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("MULTISET"));
        assert!(!io.new_input(path).unwrap().exists().await.unwrap());
    }
}
