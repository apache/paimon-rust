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

//! Resolve BLOB descriptors after primary-key merge and row selection.

use super::blob_resolver::{resolve_blob_column, BlobReadLimiter};
use super::managed_blob_writer::{managed_blob_kind, ManagedBlobKind};
use super::{ArrowRecordBatchStream, Table};
use crate::arrow::format::FilePredicates;
use crate::io::FileIO;
use crate::spec::{CoreOptions, DataField, Predicate};
use crate::Result;
use arrow_array::builder::LargeBinaryBuilder;
use arrow_array::{
    Array, ArrayRef, LargeBinaryArray, ListArray, MapArray, RecordBatch, StructArray,
};
use arrow_schema::DataType as ArrowDataType;
use futures::StreamExt;
use std::collections::HashSet;
use std::sync::Arc;

pub(crate) fn resolve_primary_key_blob_stream(
    stream: ArrowRecordBatchStream,
    fields: &[DataField],
    options: &CoreOptions<'_>,
    file_io: FileIO,
    parallelism: usize,
) -> ArrowRecordBatchStream {
    let selected = resolved_primary_key_blob_fields(fields, options);
    if selected.is_empty() {
        return stream;
    }
    let limiter = BlobReadLimiter::with_parallelism(parallelism);
    Box::pin(async_stream::try_stream! {
        let mut stream = stream;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            yield resolve_batch(batch, &selected, &file_io, &limiter).await?;
        }
    })
}

pub(crate) fn resolved_primary_key_blob_fields(
    fields: &[DataField],
    options: &CoreOptions<'_>,
) -> Vec<(usize, ManagedBlobKind)> {
    if options.blob_as_descriptor() {
        return Vec::new();
    }
    let descriptor_fields = options.blob_descriptor_fields();
    let inline_fields = options.blob_inline_fields();
    fields
        .iter()
        .enumerate()
        .filter_map(|(index, field)| {
            let kind = managed_blob_kind(field.data_type())?;
            (!inline_fields.contains(field.name()) || descriptor_fields.contains(field.name()))
                .then_some((index, kind))
        })
        .collect()
}

fn resolved_blob_indices(fields: &[DataField], options: &CoreOptions<'_>) -> HashSet<usize> {
    resolved_primary_key_blob_fields(fields, options)
        .into_iter()
        .map(|(index, _)| index)
        .collect()
}

fn predicate_uses_resolved_blob(predicate: &Predicate, resolved: &HashSet<usize>) -> bool {
    let mut referenced = HashSet::new();
    predicate.collect_leaf_field_indices(&mut referenced);
    referenced.iter().any(|index| resolved.contains(index))
}

/// Drop payload predicates from file and stats pruning while retaining safe
/// predicates on ordinary columns. The full filter remains on TableRead.
pub(crate) fn scan_predicates(table: &Table, predicates: &[Predicate]) -> Vec<Predicate> {
    if table.schema().primary_keys().is_empty() {
        return predicates.to_vec();
    }
    let options = table.schema().core_options();
    let resolved = resolved_blob_indices(table.schema().fields(), &options);
    if resolved.is_empty() {
        return predicates.to_vec();
    }
    predicates
        .iter()
        .filter(|predicate| !predicate_uses_resolved_blob(predicate, &resolved))
        .cloned()
        .collect()
}

/// Holds the extra columns and exact residual filter needed when a primary-key
/// predicate compares BLOB payloads rather than their Parquet descriptors.
pub(crate) struct ManagedBlobReadPlan {
    scan_fields: Vec<DataField>,
    output_fields: Vec<DataField>,
    predicates: FilePredicates,
}

impl ManagedBlobReadPlan {
    pub(crate) fn new(
        read_type: &[DataField],
        predicates: &[Predicate],
        table_fields: &[DataField],
        options: &CoreOptions<'_>,
    ) -> Option<Self> {
        let resolved = resolved_blob_indices(table_fields, options);
        if resolved.is_empty() {
            return None;
        }
        predicates
            .iter()
            .any(|predicate| predicate_uses_resolved_blob(predicate, &resolved))
            .then(|| {
                let predicates = FilePredicates {
                    predicates: predicates.to_vec(),
                    row_filter_factory: None,
                    file_fields: table_fields.to_vec(),
                };
                let scan_fields =
                    crate::arrow::residual::widen_scan_fields(read_type, Some(&predicates));
                Self {
                    scan_fields,
                    output_fields: read_type.to_vec(),
                    predicates,
                }
            })
    }

    pub(crate) fn scan_fields(&self) -> &[DataField] {
        &self.scan_fields
    }

    pub(crate) fn finish(
        self,
        stream: ArrowRecordBatchStream,
        options: &CoreOptions<'_>,
        file_io: FileIO,
        parallelism: usize,
    ) -> ArrowRecordBatchStream {
        let stream = resolve_primary_key_blob_stream(
            stream,
            &self.scan_fields,
            options,
            file_io,
            parallelism,
        );
        Box::pin(async_stream::try_stream! {
            let mut stream = stream;
            while let Some(batch) = stream.next().await {
                let batch = crate::arrow::residual::filter_record_batch_by_predicates(
                    batch?, &self.predicates, &self.scan_fields,
                )?;
                let indices = self.output_fields
                    .iter()
                    .map(|field| batch.schema().index_of(field.name()))
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|error| crate::Error::DataInvalid {
                        message: format!("Managed BLOB output projection is missing a column: {error}"),
                        source: Some(Box::new(error)),
                    })?;
                yield batch.project(&indices).map_err(|error| crate::Error::DataInvalid {
                    message: format!("Failed to project managed BLOB read output: {error}"),
                    source: Some(Box::new(error)),
                })?;
            }
        })
    }
}

async fn resolve_batch(
    batch: RecordBatch,
    fields: &[(usize, ManagedBlobKind)],
    file_io: &FileIO,
    limiter: &BlobReadLimiter,
) -> Result<RecordBatch> {
    let mut columns = batch.columns().to_vec();
    for &(index, kind) in fields {
        let column = match kind {
            ManagedBlobKind::Scalar => {
                let values = blob_values(columns[index].as_ref())?;
                Arc::new(resolve_blob_column(values, file_io, limiter.clone()).await?) as ArrayRef
            }
            ManagedBlobKind::Array => {
                let array = columns[index]
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| invalid("ARRAY<BLOB> requires ListArray"))?;
                let values = blob_values(array.values().as_ref())?;
                let visible = visible_child_values(values, array.value_offsets(), array)?;
                let resolved =
                    Arc::new(resolve_blob_column(&visible, file_io, limiter.clone()).await?);
                let ArrowDataType::List(element) = array.data_type() else {
                    unreachable!()
                };
                Arc::new(
                    ListArray::try_new(
                        element.clone(),
                        array.offsets().clone(),
                        resolved,
                        array.nulls().cloned(),
                    )
                    .map_err(|error| invalid(&error.to_string()))?,
                )
            }
            ManagedBlobKind::Map => {
                let map = columns[index]
                    .as_any()
                    .downcast_ref::<MapArray>()
                    .ok_or_else(|| invalid("MAP<X, BLOB> requires MapArray"))?;
                let values = blob_values(map.entries().column(1).as_ref())?;
                let visible = visible_child_values(values, map.value_offsets(), map)?;
                let resolved =
                    Arc::new(resolve_blob_column(&visible, file_io, limiter.clone()).await?);
                let ArrowDataType::Map(entries_field, ordered) = map.data_type() else {
                    unreachable!()
                };
                let ArrowDataType::Struct(entry_fields) = entries_field.data_type() else {
                    unreachable!()
                };
                let entries = StructArray::try_new(
                    entry_fields.clone(),
                    vec![map.entries().column(0).clone(), resolved],
                    None,
                )
                .map_err(|error| invalid(&error.to_string()))?;
                Arc::new(
                    MapArray::try_new(
                        entries_field.clone(),
                        map.offsets().clone(),
                        entries,
                        map.nulls().cloned(),
                        *ordered,
                    )
                    .map_err(|error| invalid(&error.to_string()))?,
                )
            }
        };
        columns[index] = column;
    }
    RecordBatch::try_new(batch.schema(), columns).map_err(|error| invalid(&error.to_string()))
}

/// Arrow may retain child values beneath a null collection parent. They are
/// invisible to the row and must not trigger descriptor I/O or fail the read
/// when a stale URI happens to remain in an unused child slot.
fn visible_child_values(
    values: &LargeBinaryArray,
    offsets: &[i32],
    parents: &dyn Array,
) -> Result<LargeBinaryArray> {
    if offsets.len() != parents.len() + 1 {
        return Err(invalid("BLOB collection offsets do not match parent rows"));
    }
    let mut visible = vec![false; values.len()];
    for row in 0..parents.len() {
        if !parents.is_valid(row) {
            continue;
        }
        let start = usize::try_from(offsets[row])
            .map_err(|_| invalid("Negative BLOB collection offset"))?;
        let end = usize::try_from(offsets[row + 1])
            .map_err(|_| invalid("Negative BLOB collection offset"))?;
        let range = visible
            .get_mut(start..end)
            .ok_or_else(|| invalid("BLOB collection offset exceeds child values"))?;
        range.fill(true);
    }
    let mut builder = LargeBinaryBuilder::new();
    for (index, visible) in visible.into_iter().enumerate() {
        if visible && values.is_valid(index) {
            builder.append_value(values.value(index));
        } else {
            builder.append_null();
        }
    }
    Ok(builder.finish())
}

fn blob_values(array: &dyn Array) -> Result<&LargeBinaryArray> {
    array
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .ok_or_else(|| invalid("BLOB values require LargeBinaryArray"))
}

fn invalid(message: &str) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.to_string(),
        source: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::BlobDescriptor;
    use arrow_array::StringArray;
    use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow_schema::{Field, Schema};

    #[tokio::test]
    async fn null_array_parent_does_not_fetch_hidden_descriptor() {
        let missing =
            BlobDescriptor::new("memory:/missing-managed.blob".to_string(), 0, 4).serialize();
        let values = Arc::new(LargeBinaryArray::from(vec![Some(missing.as_slice())]));
        let element = Arc::new(Field::new("element", ArrowDataType::LargeBinary, true));
        let array = Arc::new(
            ListArray::try_new(
                element.clone(),
                OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
                values,
                Some(NullBuffer::from(vec![false])),
            )
            .unwrap(),
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "items",
            ArrowDataType::List(element),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        let io = FileIOBuilder::new("memory").build().unwrap();
        let resolved = resolve_batch(
            batch,
            &[(0, ManagedBlobKind::Array)],
            &io,
            &BlobReadLimiter::new(),
        )
        .await
        .unwrap();
        let array = resolved
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(array.is_null(0));
        assert!(array.values().is_null(0));
    }

    #[tokio::test]
    async fn null_map_parent_does_not_fetch_hidden_descriptor() {
        let missing =
            BlobDescriptor::new("memory:/missing-managed.blob".to_string(), 0, 4).serialize();
        let entry_fields = vec![
            Arc::new(Field::new("key", ArrowDataType::Utf8, false)),
            Arc::new(Field::new("value", ArrowDataType::LargeBinary, true)),
        ];
        let entries = StructArray::try_new(
            entry_fields.clone().into(),
            vec![
                Arc::new(StringArray::from(vec!["hidden"])),
                Arc::new(LargeBinaryArray::from(vec![Some(missing.as_slice())])),
            ],
            None,
        )
        .unwrap();
        let entry_field = Arc::new(Field::new(
            "entries",
            ArrowDataType::Struct(entry_fields.into()),
            false,
        ));
        let map = Arc::new(
            MapArray::try_new(
                entry_field.clone(),
                OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
                entries,
                Some(NullBuffer::from(vec![false])),
                false,
            )
            .unwrap(),
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "named",
            ArrowDataType::Map(entry_field, false),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![map]).unwrap();
        let io = FileIOBuilder::new("memory").build().unwrap();
        let resolved = resolve_batch(
            batch,
            &[(0, ManagedBlobKind::Map)],
            &io,
            &BlobReadLimiter::new(),
        )
        .await
        .unwrap();
        let map = resolved
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap();
        assert!(map.is_null(0));
        assert!(map.entries().column(1).is_null(0));
    }
}
