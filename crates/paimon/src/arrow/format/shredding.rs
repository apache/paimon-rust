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
use crate::arrow::build_target_arrow_schema;
use crate::arrow::shredding::map::{
    detect_map_shredding_fields, normalize_field_dict_compression, MapShreddingFieldConfig,
    MapShreddingWritePlan, MAP_SHREDDING_INFER_BUFFER_ROW_COUNT,
};
use crate::arrow::shredding::variant::{
    assemble_shredded_variant_batch, configured_variant_shredding_fields,
    contains_variant_read_fields, infer_variant_shredding_fields,
    should_infer_variant_shredding_fields, variant_shredding_infer_buffer_row_count,
    VariantWritePlan,
};
use crate::arrow::shredding::ShreddingWritePlan;
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
    if contains_variant_read_fields(read_fields) {
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
        if !contains_variant_read_fields(read_fields) {
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
    /// File compression codec, used for the MAP field-dict metadata at close.
    compression: String,
}

enum ShreddingWriterState {
    Ready {
        inner: Box<dyn FormatFileWriter>,
        /// `None` when inference decided no shredding is needed (passthrough).
        plan: Option<Box<dyn ShreddingWritePlan>>,
    },
    Infer {
        writer_factory: Option<Box<dyn PhysicalFormatWriterFactory>>,
        schema: SchemaRef,
        logical_write_fields: Vec<DataField>,
        format_options: HashMap<String, String>,
        buffered_batches: Vec<RecordBatch>,
        buffered_row_count: usize,
        infer_buffer_row_count: usize,
        plan_builder: InferPlanBuilder,
    },
    Closed,
}

/// How to build the [`ShreddingWritePlan`] once enough rows are buffered,
/// mirroring Java's `ShreddingWritePlanFactory`.
enum InferPlanBuilder {
    Variant,
    Map {
        configs: Vec<MapShreddingFieldConfig>,
    },
}

impl ShreddingFormatWriter {
    pub(crate) async fn create(
        mut writer_factory: Box<dyn PhysicalFormatWriterFactory>,
        schema: SchemaRef,
        write_fields: Option<&[DataField]>,
        format_options: Option<&HashMap<String, String>>,
        compression: &str,
    ) -> crate::Result<Box<dyn FormatFileWriter>> {
        let Some(fields) = write_fields else {
            return writer_factory.create_writer(schema, write_fields).await;
        };
        let Some(options) = format_options else {
            return writer_factory.create_writer(schema, write_fields).await;
        };

        let variant_configured = configured_variant_shredding_fields(fields, options)?;
        let variant_infer =
            variant_configured.is_none() && should_infer_variant_shredding_fields(fields, options)?;
        let map_configs = detect_map_shredding_fields(fields, options)?;

        // Mirror Java's ShreddingWritePlanWriterFactories: at most one
        // shredding plan may be active for a file.
        if (variant_configured.is_some() || variant_infer) && !map_configs.is_empty() {
            return Err(crate::Error::Unsupported {
                message: "Variant shredding and MAP shared-shredding cannot be active \
                          for the same file"
                    .to_string(),
            });
        }

        if let Some(physical_fields) = variant_configured {
            let plan = VariantWritePlan::new(fields.to_vec(), physical_fields);
            return Self::create_ready(writer_factory, Box::new(plan), compression).await;
        }

        if variant_infer {
            return Ok(Box::new(Self {
                state: ShreddingWriterState::Infer {
                    writer_factory: Some(writer_factory),
                    schema,
                    logical_write_fields: fields.to_vec(),
                    format_options: options.clone(),
                    buffered_batches: Vec::new(),
                    buffered_row_count: 0,
                    infer_buffer_row_count: variant_shredding_infer_buffer_row_count(options)?,
                    plan_builder: InferPlanBuilder::Variant,
                },
                compression: compression.to_string(),
            }));
        }

        if !map_configs.is_empty() {
            // Validate the field-dict compression eagerly, mirroring Java's
            // SchemaValidation (only none/lz4/zstd are supported).
            normalize_field_dict_compression(Some(compression))?;
            return Ok(Box::new(Self {
                state: ShreddingWriterState::Infer {
                    writer_factory: Some(writer_factory),
                    schema,
                    logical_write_fields: fields.to_vec(),
                    format_options: options.clone(),
                    buffered_batches: Vec::new(),
                    buffered_row_count: 0,
                    infer_buffer_row_count: MAP_SHREDDING_INFER_BUFFER_ROW_COUNT,
                    plan_builder: InferPlanBuilder::Map {
                        configs: map_configs,
                    },
                },
                compression: compression.to_string(),
            }));
        }

        writer_factory.create_writer(schema, write_fields).await
    }

    async fn create_ready(
        mut writer_factory: Box<dyn PhysicalFormatWriterFactory>,
        plan: Box<dyn ShreddingWritePlan>,
        compression: &str,
    ) -> crate::Result<Box<dyn FormatFileWriter>> {
        let writer_schema = build_target_arrow_schema(plan.physical_fields())?;
        let inner = writer_factory
            .create_writer(writer_schema, Some(plan.physical_fields()))
            .await?;
        Ok(Box::new(Self {
            state: ShreddingWriterState::Ready {
                inner,
                plan: Some(plan),
            },
            compression: compression.to_string(),
        }))
    }

    async fn finalize_inferred_writer(&mut self) -> crate::Result<()> {
        let (
            mut writer_factory,
            schema,
            logical_write_fields,
            format_options,
            buffered_batches,
            plan_builder,
        ) = match &mut self.state {
            ShreddingWriterState::Ready { .. } => return Ok(()),
            ShreddingWriterState::Closed => return Ok(()),
            ShreddingWriterState::Infer {
                writer_factory,
                schema,
                logical_write_fields,
                format_options,
                buffered_batches,
                plan_builder,
                ..
            } => (
                writer_factory
                    .take()
                    .ok_or_else(|| crate::Error::DataInvalid {
                        message: "Shredding writer already finalized".to_string(),
                        source: None,
                    })?,
                schema.clone(),
                logical_write_fields.clone(),
                format_options.clone(),
                std::mem::take(buffered_batches),
                std::mem::replace(plan_builder, InferPlanBuilder::Variant),
            ),
        };

        let plan: Option<Box<dyn ShreddingWritePlan>> = match &plan_builder {
            InferPlanBuilder::Variant => infer_variant_shredding_fields(
                &logical_write_fields,
                &buffered_batches,
                &format_options,
            )?
            .map(|physical_fields| {
                Box::new(VariantWritePlan::new(
                    logical_write_fields.clone(),
                    physical_fields,
                )) as Box<dyn ShreddingWritePlan>
            }),
            InferPlanBuilder::Map { configs } => Some(Box::new(MapShreddingWritePlan::infer(
                &logical_write_fields,
                configs,
                &buffered_batches,
            )?)
                as Box<dyn ShreddingWritePlan>),
        };

        let writer_schema = match &plan {
            Some(plan) => build_target_arrow_schema(plan.physical_fields())?,
            None => schema,
        };
        let inner = writer_factory
            .create_writer(
                writer_schema,
                plan.as_ref().map(|plan| plan.physical_fields()),
            )
            .await?;
        self.state = ShreddingWriterState::Ready { inner, plan };

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
            ShreddingWriterState::Ready { inner, plan } => match plan {
                Some(plan) => {
                    let physical_batch = plan.to_physical_batch(batch)?;
                    inner.write(&physical_batch).await
                }
                None => inner.write(batch).await,
            },
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

    async fn close(mut self: Box<Self>) -> crate::Result<FormatWriteResult> {
        self.finalize_inferred_writer().await?;
        let compression = self.compression.clone();
        match std::mem::replace(&mut self.state, ShreddingWriterState::Closed) {
            ShreddingWriterState::Ready { mut inner, plan } => {
                if let Some(plan) = plan {
                    // Commit the shredding metadata into the file footer before
                    // closing, mirroring Java's ShreddingFormatWriter.close.
                    let field_metadata = plan.field_metadata(Some(&compression))?;
                    if !field_metadata.is_empty() {
                        inner.commit_field_metadata(&field_metadata)?;
                    }
                }
                inner.close().await
            }
            ShreddingWriterState::Infer { .. } => unreachable!("infer writer finalized above"),
            ShreddingWriterState::Closed => Ok(FormatWriteResult::new(0)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::build_target_arrow_schema;
    use crate::spec::{DataType, IntType, MapType, VarCharType, VariantType};

    /// Factory that must never be reached: these tests only exercise plan
    /// detection, which fails before any writer is created.
    struct NoopWriterFactory;

    #[async_trait]
    impl PhysicalFormatWriterFactory for NoopWriterFactory {
        async fn create_writer(
            &mut self,
            _schema: SchemaRef,
            _write_fields: Option<&[DataField]>,
        ) -> crate::Result<Box<dyn FormatFileWriter>> {
            unreachable!("no writer should be created when plan detection fails")
        }
    }

    fn string_map_field(id: i32, name: &str) -> DataField {
        DataField::new(
            id,
            name.to_string(),
            DataType::Map(MapType::new(
                DataType::VarChar(VarCharType::new(VarCharType::MAX_LENGTH).unwrap()),
                DataType::Int(IntType::new()),
            )),
        )
    }

    /// Mirroring Java's `ShreddingWritePlanWriterFactories`: at most one
    /// shredding plan may be active for a file.
    #[tokio::test]
    async fn test_variant_and_map_shredding_conflict() {
        let fields = vec![
            DataField::new(0, "v".to_string(), DataType::Variant(VariantType::new())),
            string_map_field(1, "tags"),
        ];
        let schema = build_target_arrow_schema(&fields).unwrap();
        let options = HashMap::from([
            (
                "variant.inferShreddingSchema".to_string(),
                "true".to_string(),
            ),
            (
                "fields.tags.map.storage-layout".to_string(),
                "shared-shredding".to_string(),
            ),
        ]);
        let err = ShreddingFormatWriter::create(
            Box::new(NoopWriterFactory),
            schema,
            Some(&fields),
            Some(&options),
            "zstd",
        )
        .await
        .err()
        .expect("conflicting shredding plans must be rejected");
        assert!(
            matches!(err, crate::Error::Unsupported { .. }),
            "unexpected error: {err}"
        );
    }

    /// Mirroring Java's `SchemaValidation`: MAP shared-shredding only
    /// supports none/lz4/zstd file compression.
    #[tokio::test]
    async fn test_map_shredding_rejects_unsupported_compression() {
        let fields = vec![string_map_field(0, "tags")];
        let schema = build_target_arrow_schema(&fields).unwrap();
        let options = HashMap::from([(
            "fields.tags.map.storage-layout".to_string(),
            "shared-shredding".to_string(),
        )]);
        let err = ShreddingFormatWriter::create(
            Box::new(NoopWriterFactory),
            schema,
            Some(&fields),
            Some(&options),
            "snappy",
        )
        .await
        .err()
        .expect("unsupported compression must be rejected");
        assert!(
            err.to_string()
                .contains("MAP shared-shredding only supports none/lz4/zstd compression"),
            "unexpected error: {err}"
        );
    }
}
