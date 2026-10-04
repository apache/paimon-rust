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

use super::{
    FilePredicates, FormatFileReader, FormatFileWriter, FormatWriteResult, FormatWriterFactory,
};
use crate::arrow::build_target_arrow_schema;
use crate::arrow::shredding::map::{detect_map_shredding_fields, MapShreddingWritePlanFactory};
use crate::arrow::shredding::variant::{
    assemble_shredded_variant_batch, contains_variant_read_fields, VariantShreddingWritePlanFactory,
};
use crate::arrow::shredding::{ShreddingWritePlan, ShreddingWritePlanFactory};
use crate::io::{FileRead, OutputFile};
use crate::spec::DataField;
use crate::table::{ArrowRecordBatchStream, RowRange};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;

/// Raw factories capable of accepting a shredding plan's physical schema,
/// mirroring Java SupportsShreddingWritePlan.
#[async_trait]
pub(crate) trait PhysicalFormatWriterFactory: FormatWriterFactory {
    async fn create_physical_writer(
        &self,
        output: &OutputFile,
        compression: &str,
        schema: SchemaRef,
        write_fields: Option<&[DataField]>,
    ) -> crate::Result<Box<dyn FormatFileWriter>>;
}

/// Detect a plan once when creating a rolling writer's factory.
pub(crate) fn wrap_writer_factory(
    delegate: Arc<dyn PhysicalFormatWriterFactory>,
    fields: Option<&[DataField]>,
    options: Option<&HashMap<String, String>>,
) -> crate::Result<Arc<dyn FormatWriterFactory>> {
    let (Some(fields), Some(options)) = (fields, options) else {
        return Ok(delegate);
    };
    let variant_factory = VariantShreddingWritePlanFactory::new(fields.to_vec(), options.clone())?;
    let map_configs = detect_map_shredding_fields(fields, options)?;
    if variant_factory.should_create_write_plan() && !map_configs.is_empty() {
        return Err(crate::Error::Unsupported {
            message:
                "Variant shredding and MAP shared-shredding cannot be active for the same file"
                    .to_string(),
        });
    }
    let plan_factory: Arc<dyn ShreddingWritePlanFactory> =
        if variant_factory.should_create_write_plan() {
            Arc::new(variant_factory)
        } else if !map_configs.is_empty() {
            Arc::new(MapShreddingWritePlanFactory::new(
                fields.to_vec(),
                map_configs,
            ))
        } else {
            return Ok(delegate);
        };
    Ok(Arc::new(ShreddingWritePlanWriterFactory {
        delegate,
        plan_factory,
    }))
}

struct ShreddingWritePlanWriterFactory {
    delegate: Arc<dyn PhysicalFormatWriterFactory>,
    plan_factory: Arc<dyn ShreddingWritePlanFactory>,
}

#[async_trait]
impl FormatWriterFactory for ShreddingWritePlanWriterFactory {
    async fn create_writer(
        &self,
        output: &OutputFile,
        compression: &str,
    ) -> crate::Result<Box<dyn FormatFileWriter>> {
        self.plan_factory.validate_compression(compression)?;
        let state = if let Some(infer_buffer_row_count) = self.plan_factory.infer_buffer_row_count()
        {
            ShreddingWriterState::Infer {
                writer_factory: self.delegate.clone(),
                output: Box::new(output.clone()),
                buffered_batches: Vec::new(),
                buffered_row_count: 0,
                infer_buffer_row_count,
            }
        } else {
            ShreddingFormatWriter::create_ready_state(
                self.delegate.as_ref(),
                output,
                compression,
                self.plan_factory.create_write_plan(&[])?,
            )
            .await?
        };
        Ok(Box::new(ShreddingFormatWriter {
            state,
            compression: compression.to_string(),
            plan_factory: self.plan_factory.clone(),
        }))
    }

    fn needs_completed_file_stats(&self) -> bool {
        self.plan_factory.needs_completed_file_stats()
    }
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
    compression: String,
    plan_factory: Arc<dyn ShreddingWritePlanFactory>,
}

enum ShreddingWriterState {
    Ready {
        inner: Box<dyn FormatFileWriter>,
        plan: Box<dyn ShreddingWritePlan>,
    },
    Infer {
        writer_factory: Arc<dyn PhysicalFormatWriterFactory>,
        output: Box<OutputFile>,
        buffered_batches: Vec<RecordBatch>,
        buffered_row_count: usize,
        infer_buffer_row_count: usize,
    },
    Closed,
}

impl ShreddingFormatWriter {
    async fn create_ready_state(
        writer_factory: &dyn PhysicalFormatWriterFactory,
        output: &OutputFile,
        compression: &str,
        plan: Box<dyn ShreddingWritePlan>,
    ) -> crate::Result<ShreddingWriterState> {
        // A no-op plan still receives its completion callback, but must
        // preserve the delegate's Arrow schema (notably PK nullability).
        let physical = if plan.physical_fields() == plan.logical_fields() {
            None
        } else {
            Some((
                build_target_arrow_schema(plan.physical_fields())?,
                plan.physical_fields().to_vec(),
            ))
        };
        let inner = match physical {
            Some((schema, fields)) => {
                writer_factory
                    .create_physical_writer(output, compression, schema, Some(&fields))
                    .await?
            }
            None => writer_factory.create_writer(output, compression).await?,
        };
        Ok(ShreddingWriterState::Ready { inner, plan })
    }

    async fn finalize_inferred_writer(&mut self) -> crate::Result<()> {
        if !matches!(&self.state, ShreddingWriterState::Infer { .. }) {
            return Ok(());
        }
        let ShreddingWriterState::Infer {
            writer_factory,
            output,
            buffered_batches,
            ..
        } = std::mem::replace(&mut self.state, ShreddingWriterState::Closed)
        else {
            unreachable!("inference checked above")
        };
        let plan = self.plan_factory.create_write_plan(&buffered_batches)?;
        self.state =
            Self::create_ready_state(writer_factory.as_ref(), &output, &self.compression, plan)
                .await?;
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
            ShreddingWriterState::Ready { inner, plan } => {
                let physical_batch = plan.to_physical_batch(batch)?;
                inner.write(&physical_batch).await
            }
            ShreddingWriterState::Infer {
                buffered_batches,
                buffered_row_count,
                infer_buffer_row_count,
                ..
            } => {
                // Sample the same prefix as Java, even when one Arrow batch
                // crosses the inference threshold.
                let sample_count = batch.num_rows().min(
                    infer_buffer_row_count
                        .saturating_sub(*buffered_row_count)
                        .max(1),
                );
                buffered_batches.push(batch.slice(0, sample_count));
                *buffered_row_count += sample_count;
                if *buffered_row_count >= *infer_buffer_row_count {
                    self.finalize_inferred_writer().await?;
                }
                if sample_count < batch.num_rows() {
                    self.write(&batch.slice(sample_count, batch.num_rows() - sample_count))
                        .await?;
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

    fn retains_batch_data(&self) -> bool {
        match &self.state {
            ShreddingWriterState::Ready { inner, .. } => inner.retains_batch_data(),
            ShreddingWriterState::Infer {
                buffered_batches, ..
            } => !buffered_batches.is_empty(),
            ShreddingWriterState::Closed => false,
        }
    }

    fn pending_rows(&self) -> Option<usize> {
        match &self.state {
            ShreddingWriterState::Ready { inner, .. } => inner.pending_rows(),
            ShreddingWriterState::Infer {
                buffered_row_count, ..
            } => Some(*buffered_row_count),
            ShreddingWriterState::Closed => Some(0),
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
                // Commit the shredding metadata into the file footer before
                // closing, mirroring Java's ShreddingFormatWriter.close.
                let field_metadata = plan.field_metadata(Some(&compression))?;
                if !field_metadata.is_empty() {
                    inner.commit_field_metadata(&field_metadata)?;
                }
                let result = inner.close().await?;
                self.plan_factory.on_file_completed(plan.as_ref())?;
                Ok(result)
            }
            ShreddingWriterState::Infer { .. } => unreachable!("infer writer finalized above"),
            ShreddingWriterState::Closed => Ok(FormatWriteResult::new(0)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::format::with_write_resources;
    use crate::resource::ResourceContext;
    use crate::spec::{DataType, IntType, MapType, VarCharType, VariantType};
    use arrow_array::Int32Array;
    use arrow_schema::{DataType as ArrowDataType, Field, Schema};
    use std::sync::Arc;

    /// Factory that must never be reached: these tests only exercise plan
    /// detection, which fails before any writer is created.
    struct NoopWriterFactory;

    #[async_trait]
    impl FormatWriterFactory for NoopWriterFactory {
        async fn create_writer(
            &self,
            _output: &OutputFile,
            _compression: &str,
        ) -> crate::Result<Box<dyn FormatFileWriter>> {
            unreachable!("no writer should be created")
        }
    }

    #[async_trait]
    impl PhysicalFormatWriterFactory for NoopWriterFactory {
        async fn create_physical_writer(
            &self,
            _output: &OutputFile,
            _compression: &str,
            _schema: SchemaRef,
            _write_fields: Option<&[DataField]>,
        ) -> crate::Result<Box<dyn FormatFileWriter>> {
            unreachable!("no writer should be created when plan detection fails")
        }
    }

    fn test_output() -> OutputFile {
        crate::io::FileIOBuilder::new("memory")
            .build()
            .unwrap()
            .new_output("memory:/shredding.parquet")
            .unwrap()
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

    #[tokio::test]
    async fn inference_buffer_is_charged_without_triggering_row_group_flush() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            ArrowDataType::Int32,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))])
                .unwrap();
        let writer = ShreddingFormatWriter {
            state: ShreddingWriterState::Infer {
                writer_factory: Arc::new(NoopWriterFactory),
                output: Box::new(test_output()),
                buffered_batches: vec![],
                buffered_row_count: 0,
                infer_buffer_row_count: 10,
            },
            compression: "zstd".to_string(),
            plan_factory: Arc::new(
                VariantShreddingWritePlanFactory::new(vec![], HashMap::new()).unwrap(),
            ),
        };
        let resources = ResourceContext::builder().build().unwrap();
        let mut writer = with_write_resources(Box::new(writer), Some(&resources));
        writer.write(&batch).await.unwrap();
        assert_eq!(writer.in_progress_size(), 0);
        assert!(resources.metrics().reserved_memory_bytes > 0);
        drop(writer);
        assert_eq!(resources.metrics().reserved_memory_bytes, 0);
    }

    /// Mirroring Java's `ShreddingWritePlanWriterFactories`: at most one
    /// shredding plan may be active for a file.
    #[tokio::test]
    async fn test_variant_and_map_shredding_conflict() {
        let fields = vec![
            DataField::new(0, "v".to_string(), DataType::Variant(VariantType::new())),
            string_map_field(1, "tags"),
        ];
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
        let err = wrap_writer_factory(Arc::new(NoopWriterFactory), Some(&fields), Some(&options))
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
        let options = HashMap::from([(
            "fields.tags.map.storage-layout".to_string(),
            "shared-shredding".to_string(),
        )]);
        let factory =
            wrap_writer_factory(Arc::new(NoopWriterFactory), Some(&fields), Some(&options))
                .unwrap();
        let err = factory
            .create_writer(&test_output(), "snappy")
            .await
            .err()
            .expect("unsupported compression must be rejected");
        assert!(
            err.to_string()
                .contains("MAP shared-shredding only supports none/lz4/zstd compression"),
            "unexpected error: {err}"
        );
    }
    struct CloseOnlyWriter {
        fail: bool,
    }

    #[async_trait]
    impl FormatFileWriter for CloseOnlyWriter {
        async fn write(&mut self, _batch: &RecordBatch) -> crate::Result<()> {
            Ok(())
        }
        fn num_bytes(&self) -> usize {
            0
        }
        fn in_progress_size(&self) -> usize {
            0
        }
        async fn flush(&mut self) -> crate::Result<()> {
            Ok(())
        }
        fn commit_field_metadata(
            &mut self,
            _metadata: &crate::arrow::shredding::FieldMetadata,
        ) -> crate::Result<()> {
            Ok(())
        }
        async fn close(self: Box<Self>) -> crate::Result<FormatWriteResult> {
            if self.fail {
                Err(crate::Error::DataInvalid {
                    message: "injected close failure".into(),
                    source: None,
                })
            } else {
                Ok(FormatWriteResult::new(0))
            }
        }
    }

    #[tokio::test]
    async fn adaptive_variant_commits_only_after_successful_file_close() {
        let fields = vec![DataField::new(
            0,
            "v".into(),
            DataType::Variant(VariantType::new()),
        )];
        let factory = Arc::new(
            VariantShreddingWritePlanFactory::new(
                fields,
                HashMap::from([
                    ("variant.inferShreddingSchema".into(), "true".into()),
                    ("variant.shredding.inferenceMode".into(), "adaptive".into()),
                    ("variant.shredding.maxInferBufferRow".into(), "4".into()),
                    (
                        "variant.shredding.adaptive.maxInferBufferRow".into(),
                        "1".into(),
                    ),
                ]),
            )
            .unwrap(),
        );
        for fail in [true, false] {
            let writer = Box::new(ShreddingFormatWriter {
                state: ShreddingWriterState::Ready {
                    inner: Box::new(CloseOnlyWriter { fail }),
                    plan: factory.create_write_plan(&[]).unwrap(),
                },
                compression: "zstd".into(),
                plan_factory: factory.clone(),
            });
            assert_eq!(writer.close().await.is_err(), fail);
            assert_eq!(
                factory.infer_buffer_row_count(),
                Some(if fail { 4 } else { 1 })
            );
        }
    }

    #[tokio::test]
    async fn map_factory_advances_only_after_successful_file_close() {
        use arrow_array::builder::{Int32Builder, MapBuilder, StringBuilder};
        let fields = vec![string_map_field(0, "tags")];
        let options = HashMap::from([
            (
                "fields.tags.map.storage-layout".into(),
                "shared-shredding".into(),
            ),
            (
                "fields.tags.map.shared-shredding.max-columns".into(),
                "4".into(),
            ),
        ]);
        let factory = Arc::new(MapShreddingWritePlanFactory::new(
            fields.clone(),
            detect_map_shredding_fields(&fields, &options).unwrap(),
        ));
        let mut map = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        map.keys().append_value("a");
        map.values().append_value(1);
        map.append(true).unwrap();
        let batch =
            RecordBatch::try_from_iter([("tags", Arc::new(map.finish()) as arrow_array::ArrayRef)])
                .unwrap();
        for fail in [true, false] {
            let plan = factory.create_write_plan(&[]).unwrap();
            let mut writer = Box::new(ShreddingFormatWriter {
                state: ShreddingWriterState::Ready {
                    inner: Box::new(CloseOnlyWriter { fail }),
                    plan,
                },
                compression: "zstd".into(),
                plan_factory: factory.clone(),
            });
            writer.write(&batch).await.unwrap();
            assert_eq!(writer.close().await.is_err(), fail);
            let next = factory.create_write_plan(&[]).unwrap();
            let DataType::Row(physical) = next.physical_fields()[0].data_type() else {
                panic!("expected physical MAP struct")
            };
            // Failed files must not affect the next physical schema.
            assert_eq!(physical.fields().len() - 2, if fail { 4 } else { 1 });
        }
    }
}
