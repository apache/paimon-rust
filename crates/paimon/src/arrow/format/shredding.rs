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
use crate::arrow::build_target_arrow_schema;
use crate::arrow::shredding::{
    assemble_shredded_variant_batch, batch_to_shredded_physical,
    configured_variant_shredding_fields, contains_variant_fields, infer_variant_shredding_fields,
    should_infer_variant_shredding_fields, variant_shredding_infer_buffer_row_count,
};
use crate::io::FileRead;
use crate::spec::DataField;
use crate::table::{ArrowRecordBatchStream, RowRange};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::StreamExt;
use std::collections::HashMap;

#[async_trait]
pub(crate) trait PhysicalFormatWriterFactory: Send {
    async fn create_writer(
        &mut self,
        schema: SchemaRef,
        write_fields: Option<&[DataField]>,
    ) -> crate::Result<Box<dyn FormatFileWriter>>;
}

pub(crate) struct ShreddingFormatReader {
    inner: Box<dyn FormatFileReader>,
}

pub(crate) fn maybe_wrap_reader(
    reader: Box<dyn FormatFileReader>,
    read_fields: &[DataField],
) -> Box<dyn FormatFileReader> {
    if contains_variant_fields(read_fields) {
        Box::new(ShreddingFormatReader::new(reader))
    } else {
        reader
    }
}

impl ShreddingFormatReader {
    pub(crate) fn new(inner: Box<dyn FormatFileReader>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl FormatFileReader for ShreddingFormatReader {
    async fn read_batch_stream(
        &self,
        reader: Box<dyn FileRead>,
        file_size: u64,
        read_fields: &[DataField],
        predicates: Option<&FilePredicates>,
        batch_size: Option<usize>,
        row_selection: Option<Vec<RowRange>>,
    ) -> crate::Result<ArrowRecordBatchStream> {
        let stream = self
            .inner
            .read_batch_stream(
                reader,
                file_size,
                read_fields,
                predicates,
                batch_size,
                row_selection,
            )
            .await?;
        if !contains_variant_fields(read_fields) {
            return Ok(stream);
        }
        let read_fields = read_fields.to_vec();
        Ok(stream
            .map(move |batch| match batch {
                Ok(batch) => assemble_shredded_variant_batch(batch, &read_fields),
                Err(e) => Err(e),
            })
            .boxed())
    }
}

pub(crate) struct ShreddingFormatWriter {
    state: ShreddingWriterState,
}

enum ShreddingWriterState {
    Ready {
        inner: Box<dyn FormatFileWriter>,
        logical_write_fields: Option<Vec<DataField>>,
        physical_write_fields: Option<Vec<DataField>>,
    },
    Infer {
        writer_factory: Option<Box<dyn PhysicalFormatWriterFactory>>,
        schema: SchemaRef,
        logical_write_fields: Vec<DataField>,
        format_options: HashMap<String, String>,
        buffered_batches: Vec<RecordBatch>,
        buffered_row_count: usize,
        infer_buffer_row_count: usize,
    },
    Closed,
}

impl ShreddingFormatWriter {
    pub(crate) async fn create(
        mut writer_factory: Box<dyn PhysicalFormatWriterFactory>,
        schema: SchemaRef,
        write_fields: Option<&[DataField]>,
        format_options: Option<&HashMap<String, String>>,
    ) -> crate::Result<Box<dyn FormatFileWriter>> {
        let Some(fields) = write_fields else {
            return writer_factory.create_writer(schema, write_fields).await;
        };
        if !contains_variant_fields(fields) {
            return writer_factory.create_writer(schema, write_fields).await;
        }

        let Some(options) = format_options else {
            return writer_factory.create_writer(schema, write_fields).await;
        };

        if let Some(physical_fields) = configured_variant_shredding_fields(fields, options)? {
            let writer_schema = build_target_arrow_schema(&physical_fields)?;
            let inner = writer_factory
                .create_writer(writer_schema, Some(&physical_fields))
                .await?;
            return Ok(Box::new(Self {
                state: ShreddingWriterState::Ready {
                    inner,
                    logical_write_fields: Some(fields.to_vec()),
                    physical_write_fields: Some(physical_fields),
                },
            }));
        }

        if should_infer_variant_shredding_fields(fields, options)? {
            return Ok(Box::new(Self {
                state: ShreddingWriterState::Infer {
                    writer_factory: Some(writer_factory),
                    schema,
                    logical_write_fields: fields.to_vec(),
                    format_options: options.clone(),
                    buffered_batches: Vec::new(),
                    buffered_row_count: 0,
                    infer_buffer_row_count: variant_shredding_infer_buffer_row_count(options)?,
                },
            }));
        }

        writer_factory.create_writer(schema, write_fields).await
    }

    async fn finalize_inferred_writer(&mut self) -> crate::Result<()> {
        let (mut writer_factory, schema, logical_write_fields, format_options, buffered_batches) =
            match &mut self.state {
                ShreddingWriterState::Ready { .. } => return Ok(()),
                ShreddingWriterState::Closed => return Ok(()),
                ShreddingWriterState::Infer {
                    writer_factory,
                    schema,
                    logical_write_fields,
                    format_options,
                    buffered_batches,
                    ..
                } => (
                    writer_factory
                        .take()
                        .ok_or_else(|| crate::Error::DataInvalid {
                            message: "Variant shredding writer already finalized".to_string(),
                            source: None,
                        })?,
                    schema.clone(),
                    logical_write_fields.clone(),
                    format_options.clone(),
                    std::mem::take(buffered_batches),
                ),
            };

        let physical_write_fields = infer_variant_shredding_fields(
            &logical_write_fields,
            &buffered_batches,
            &format_options,
        )?;
        let writer_schema = if let Some(fields) = &physical_write_fields {
            build_target_arrow_schema(fields)?
        } else {
            schema
        };
        let inner = writer_factory
            .create_writer(writer_schema, physical_write_fields.as_deref())
            .await?;
        self.state = ShreddingWriterState::Ready {
            inner,
            logical_write_fields: physical_write_fields
                .as_ref()
                .map(|_| logical_write_fields.clone()),
            physical_write_fields,
        };

        for batch in buffered_batches {
            self.write(&batch).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl FormatFileWriter for ShreddingFormatWriter {
    async fn write(&mut self, batch: &RecordBatch) -> crate::Result<()> {
        match &mut self.state {
            ShreddingWriterState::Ready {
                inner,
                logical_write_fields,
                physical_write_fields,
            } => {
                let physical_batch;
                let batch_to_write = if let (Some(logical_fields), Some(physical_fields)) =
                    (logical_write_fields, physical_write_fields)
                {
                    physical_batch =
                        batch_to_shredded_physical(batch, logical_fields, physical_fields)?;
                    &physical_batch
                } else {
                    batch
                };
                inner.write(batch_to_write).await
            }
            ShreddingWriterState::Infer {
                buffered_batches,
                buffered_row_count,
                infer_buffer_row_count,
                ..
            } => {
                let should_finalize = {
                    buffered_batches.push(batch.clone());
                    *buffered_row_count += batch.num_rows();
                    *buffered_row_count >= *infer_buffer_row_count
                };
                if should_finalize {
                    self.finalize_inferred_writer().await?;
                }
                Ok(())
            }
            ShreddingWriterState::Closed => Err(crate::Error::DataInvalid {
                message: "Cannot write to closed shredding writer".to_string(),
                source: None,
            }),
        }
    }

    fn num_bytes(&self) -> usize {
        match &self.state {
            ShreddingWriterState::Ready { inner, .. } => inner.num_bytes(),
            ShreddingWriterState::Infer { .. } | ShreddingWriterState::Closed => 0,
        }
    }

    fn in_progress_size(&self) -> usize {
        match &self.state {
            ShreddingWriterState::Ready { inner, .. } => inner.in_progress_size(),
            ShreddingWriterState::Infer { .. } | ShreddingWriterState::Closed => 0,
        }
    }

    async fn flush(&mut self) -> crate::Result<()> {
        self.finalize_inferred_writer().await?;
        match &mut self.state {
            ShreddingWriterState::Ready { inner, .. } => inner.flush().await,
            ShreddingWriterState::Infer { .. } => unreachable!("infer writer finalized above"),
            ShreddingWriterState::Closed => Ok(()),
        }
    }

    async fn close(mut self: Box<Self>) -> crate::Result<u64> {
        self.finalize_inferred_writer().await?;
        match std::mem::replace(&mut self.state, ShreddingWriterState::Closed) {
            ShreddingWriterState::Ready { inner, .. } => inner.close().await,
            ShreddingWriterState::Infer { .. } => unreachable!("infer writer finalized above"),
            ShreddingWriterState::Closed => Ok(0),
        }
    }
}
