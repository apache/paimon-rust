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

use super::blob_resolver::{resolve_descriptor_column, BlobReadLimiter};
use super::managed_blob_writer::{managed_blob_kind, ManagedBlobKind};
use super::read_limit::take_limited_batch;
use super::{ArrowRecordBatchStream, Table};
use crate::arrow::format::FilePredicates;
use crate::io::FileIO;
use crate::spec::{CoreOptions, DataField, Predicate, PredicateOperator};
use crate::Result;
use arrow_array::{
    Array, ArrayRef, BooleanArray, LargeBinaryArray, ListArray, MapArray, RecordBatch, StructArray,
    UInt32Array, UInt64Array,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::DataType as ArrowDataType;
use futures::StreamExt;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub(crate) fn resolve_primary_key_blob_stream(
    stream: ArrowRecordBatchStream,
    fields: &[DataField],
    options: &CoreOptions<'_>,
    file_io: FileIO,
    parallelism: usize,
    limit: Option<usize>,
) -> ArrowRecordBatchStream {
    let selected = resolved_primary_key_blob_fields(fields, options);
    if selected.is_empty() && limit.is_none() {
        return stream;
    }
    let limiter = BlobReadLimiter::with_parallelism(parallelism);
    Box::pin(async_stream::try_stream! {
        let mut stream = stream;
        let mut remaining = limit;
        while remaining != Some(0) {
            let Some(batch) = stream.next().await else { break };
            let batch = take_limited_batch(batch?, &mut remaining);
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
    let video_fields = options.video_frame_fields();
    fields
        .iter()
        .enumerate()
        .filter_map(|(index, field)| {
            // Video frames remain lazy VideoFrameDescriptors, a distinct format.
            if video_fields.contains(field.name()) {
                return None;
            }
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

fn predicate_uses_resolved_blob(
    predicate: &Predicate,
    resolved: &HashSet<usize>,
    fields: &[DataField],
) -> bool {
    let mut referenced = Vec::new();
    crate::arrow::residual::collect_predicate_leaf_refs(predicate, &mut referenced);
    referenced.iter().any(|(name, index)| {
        !crate::spec::is_row_tracking_column(name)
            && fields
                .iter()
                .position(|field| field.name() == *name)
                .or_else(|| fields.get(*index).map(|_| *index))
                .is_some_and(|index| resolved.contains(&index))
    })
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
        .filter(|predicate| {
            !predicate_uses_resolved_blob(predicate, &resolved, table.schema().fields())
        })
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
            .any(|predicate| predicate_uses_resolved_blob(predicate, &resolved, table_fields))
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
        limit: Option<usize>,
    ) -> ArrowRecordBatchStream {
        let resolved = resolved_primary_key_blob_fields(&self.scan_fields, options);
        let output_fields = resolved
            .iter()
            .copied()
            .filter(|(index, _)| {
                self.output_fields
                    .iter()
                    .any(|field| field.name() == self.scan_fields[*index].name())
            })
            .collect::<Vec<_>>();
        let all_resolved = resolved_blob_indices(&self.predicates.file_fields, options);
        let ordinary_predicates = FilePredicates {
            predicates: self
                .predicates
                .predicates
                .iter()
                .filter(|predicate| {
                    !predicate_uses_resolved_blob(
                        predicate,
                        &all_resolved,
                        &self.predicates.file_fields,
                    )
                })
                .cloned()
                .collect(),
            row_filter_factory: None,
            file_fields: self.predicates.file_fields.clone(),
        };
        let limiter = BlobReadLimiter::with_parallelism(parallelism);
        Box::pin(async_stream::try_stream! {
            let mut stream = stream;
            let mut remaining = limit;
            'batches: while remaining != Some(0) {
                let Some(batch) = stream.next().await else { break };
                let batch = crate::arrow::residual::filter_record_batch_by_predicates(
                    batch?, &ordinary_predicates, &self.scan_fields,
                )?;
                // Inspect at most the remaining quota of candidate rows. Even
                // if every candidate matches, none is beyond LIMIT. Rejected
                // rows consume no quota; shrink the next batch accordingly.
                let mut offset = 0;
                while offset < batch.num_rows() {
                    if remaining == Some(0) { break 'batches; }
                    let width = remaining.unwrap_or(batch.num_rows()).min(batch.num_rows() - offset);
                    let candidate = batch.slice(offset, width);
                    offset += width;
                    let mut filter = BlobPredicateBatch::new(
                        candidate, &self.predicates, &self.scan_fields,
                        &resolved, &file_io, &limiter,
                    );
                    let mut selected = vec![true; filter.batch.num_rows()];
                    for predicate in &self.predicates.predicates {
                        selected = filter.evaluate(predicate, selected).await?;
                    }
                    // Cache predicate payloads, and fetch output-only payloads
                    // solely for rows that survived Boolean short-circuiting.
                    filter.resolve_fields(&output_fields, &selected).await?;
                    let candidate = arrow_select::filter::filter_record_batch(
                        &filter.batch, &BooleanArray::from(selected),
                    ).map_err(|error| invalid(&error.to_string()))?;
                    let candidate = take_limited_batch(candidate, &mut remaining);
                    if candidate.num_rows() == 0 { continue; }
                    let batch = candidate;
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
            }
        })
    }
}

/// Java's descriptor rows resolve a payload only when a predicate accesses it.
/// Apply the same AND/OR short-circuiting to batches, caching resolved rows so
/// repeated leaves and output projection do not fetch a payload twice.
struct BlobPredicateBatch<'a> {
    batch: RecordBatch,
    predicates: &'a FilePredicates,
    scan_fields: &'a [DataField],
    blob_fields: &'a [(usize, ManagedBlobKind)],
    resolved: HashMap<usize, Vec<bool>>,
    file_io: &'a FileIO,
    limiter: &'a BlobReadLimiter,
}

impl<'a> BlobPredicateBatch<'a> {
    fn new(
        batch: RecordBatch,
        predicates: &'a FilePredicates,
        scan_fields: &'a [DataField],
        blob_fields: &'a [(usize, ManagedBlobKind)],
        file_io: &'a FileIO,
        limiter: &'a BlobReadLimiter,
    ) -> Self {
        Self {
            batch,
            predicates,
            scan_fields,
            blob_fields,
            resolved: HashMap::new(),
            file_io,
            limiter,
        }
    }

    fn evaluate<'b>(
        &'b mut self,
        predicate: &'b Predicate,
        active: Vec<bool>,
    ) -> futures::future::BoxFuture<'b, Result<Vec<bool>>> {
        Box::pin(async move {
            if !active.iter().any(|selected| *selected) {
                return Ok(active);
            }
            match predicate {
                Predicate::AlwaysTrue => Ok(active),
                Predicate::AlwaysFalse => Ok(vec![false; active.len()]),
                Predicate::And(children) => {
                    let mut selected = active;
                    for child in children {
                        selected = self.evaluate(child, selected).await?;
                    }
                    Ok(selected)
                }
                Predicate::Or(children) => {
                    let mut pending = active;
                    let mut selected = vec![false; pending.len()];
                    for child in children {
                        let matched = self.evaluate(child, pending.clone()).await?;
                        for row in 0..pending.len() {
                            selected[row] |= matched[row];
                            pending[row] &= !matched[row];
                        }
                    }
                    Ok(selected)
                }
                Predicate::Not(child) => {
                    let matched = self.evaluate(child, active.clone()).await?;
                    Ok(active
                        .iter()
                        .zip(matched)
                        .map(|(active, matched)| *active && !matched)
                        .collect())
                }
                Predicate::Leaf { op, .. } => {
                    let fields = self
                        .blob_fields
                        .iter()
                        .copied()
                        .filter(|(index, _)| {
                            predicate_uses_resolved_blob(
                                predicate,
                                &HashSet::from([*index]),
                                self.scan_fields,
                            )
                        })
                        .collect::<Vec<_>>();
                    // Java null checks inspect the BLOB reference's validity,
                    // without opening its payload. Resolution preserves nulls.
                    if !matches!(op, PredicateOperator::IsNull | PredicateOperator::IsNotNull) {
                        self.resolve_fields(&fields, &active).await?;
                    }
                    let mask = crate::arrow::residual::evaluate_predicates_mask(
                        &self.batch,
                        std::slice::from_ref(predicate),
                        &self.predicates.file_fields,
                        self.scan_fields,
                    )?;
                    Ok(active
                        .iter()
                        .enumerate()
                        .map(|(row, active)| {
                            *active && mask.as_ref().is_none_or(|mask| mask.value(row))
                        })
                        .collect())
                }
            }
        })
    }

    async fn resolve_fields(
        &mut self,
        fields: &[(usize, ManagedBlobKind)],
        active: &[bool],
    ) -> Result<()> {
        for &(index, kind) in fields {
            let resolved = self
                .resolved
                .entry(index)
                .or_insert_with(|| vec![false; active.len()]);
            let pending = active
                .iter()
                .zip(resolved.iter())
                .map(|(active, resolved)| *active && !*resolved)
                .collect::<Vec<_>>();
            let count = pending.iter().filter(|pending| **pending).count();
            if count == 0 {
                continue;
            }
            if count == self.batch.num_rows() {
                self.batch = resolve_batch(
                    self.batch.clone(),
                    &[(index, kind)],
                    self.file_io,
                    self.limiter,
                )
                .await?;
            } else {
                let selected = arrow_select::filter::filter_record_batch(
                    &self.batch,
                    &BooleanArray::from(pending.clone()),
                )
                .map_err(|error| invalid(&error.to_string()))?;
                let selected =
                    resolve_batch(selected, &[(index, kind)], self.file_io, self.limiter).await?;
                let values = arrow_select::concat::concat(&[
                    self.batch.column(index).as_ref(),
                    selected.column(index).as_ref(),
                ])
                .map_err(|error| invalid(&error.to_string()))?;
                let mut next = self.batch.num_rows() as u64;
                let indices = UInt64Array::from(
                    pending
                        .iter()
                        .enumerate()
                        .map(|(row, selected)| {
                            if *selected {
                                let index = next;
                                next += 1;
                                index
                            } else {
                                row as u64
                            }
                        })
                        .collect::<Vec<_>>(),
                );
                let mut columns = self.batch.columns().to_vec();
                columns[index] = arrow_select::take::take(values.as_ref(), &indices, None)
                    .map_err(|error| invalid(&error.to_string()))?;
                self.batch = RecordBatch::try_new(self.batch.schema(), columns)
                    .map_err(|error| invalid(&error.to_string()))?;
            }
            for (resolved, pending) in resolved.iter_mut().zip(pending) {
                *resolved |= pending;
            }
        }
        Ok(())
    }
}

async fn resolve_batch(
    batch: RecordBatch,
    fields: &[(usize, ManagedBlobKind)],
    file_io: &FileIO,
    limiter: &BlobReadLimiter,
) -> Result<RecordBatch> {
    if fields.is_empty() || batch.num_rows() == 0 {
        return Ok(batch);
    }
    let mut columns = batch.columns().to_vec();
    for &(index, kind) in fields {
        let column = match kind {
            ManagedBlobKind::Scalar => {
                let values = blob_values(columns[index].as_ref())?;
                Arc::new(resolve_descriptor_column(values, file_io, limiter.clone()).await?)
                    as ArrayRef
            }
            ManagedBlobKind::Array => {
                let array = columns[index]
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| invalid("ARRAY<BLOB> requires ListArray"))?;
                let values = blob_values(array.values().as_ref())?;
                let visible = visible_child_values(values, array.offsets(), array)?;
                let resolved = Arc::new(
                    resolve_descriptor_column(&visible.values, file_io, limiter.clone()).await?,
                );
                let ArrowDataType::List(element) = array.data_type() else {
                    unreachable!()
                };
                Arc::new(
                    ListArray::try_new(
                        element.clone(),
                        visible.offsets,
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
                let visible = visible_child_values(values, map.offsets(), map)?;
                let resolved = Arc::new(
                    resolve_descriptor_column(&visible.values, file_io, limiter.clone()).await?,
                );
                let ArrowDataType::Map(entries_field, ordered) = map.data_type() else {
                    unreachable!()
                };
                let ArrowDataType::Struct(entry_fields) = entries_field.data_type() else {
                    unreachable!()
                };
                let keys = match visible.indices {
                    Some(indices) => {
                        arrow_select::take::take(map.entries().column(0).as_ref(), &indices, None)
                            .map_err(|error| invalid(&error.to_string()))?
                    }
                    None => map.entries().column(0).clone(),
                };
                let entries =
                    StructArray::try_new(entry_fields.clone(), vec![keys, resolved], None)
                        .map_err(|error| invalid(&error.to_string()))?;
                Arc::new(
                    MapArray::try_new(
                        entries_field.clone(),
                        visible.offsets,
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

struct VisibleBlobChildren {
    values: LargeBinaryArray,
    offsets: OffsetBuffer<i32>,
    indices: Option<UInt32Array>,
}

/// Drop children outside sliced parents or beneath NULL parents. Rebase the
/// offsets and select the same map keys. Filling hidden values with NULL would
/// violate a non-nullable element/value type, despite those slots being unused.
fn visible_child_values(
    values: &LargeBinaryArray,
    offsets: &OffsetBuffer<i32>,
    parents: &dyn Array,
) -> Result<VisibleBlobChildren> {
    if offsets.len() != parents.len() + 1 {
        return Err(invalid("BLOB collection offsets do not match parent rows"));
    }
    if parents.null_count() == 0
        && offsets[0] == 0
        && offsets[parents.len()] as usize == values.len()
    {
        return Ok(VisibleBlobChildren {
            values: values.clone(),
            offsets: offsets.clone(),
            indices: None,
        });
    }
    let mut indices = Vec::new();
    let mut output_offsets = vec![0_i32];
    for row in 0..parents.len() {
        if parents.is_valid(row) {
            let start = usize::try_from(offsets[row])
                .map_err(|_| invalid("Negative BLOB collection offset"))?;
            let end = usize::try_from(offsets[row + 1])
                .map_err(|_| invalid("Negative BLOB collection offset"))?;
            if start > end || end > values.len() {
                return Err(invalid("BLOB collection offset exceeds child values"));
            }
            indices.extend((start..end).map(|index| index as u32));
        }
        output_offsets.push(
            i32::try_from(indices.len())
                .map_err(|_| invalid("BLOB collection exceeds i32 offsets"))?,
        );
    }
    let indices = UInt32Array::from(indices);
    let selected = arrow_select::take::take(values, &indices, None)
        .map_err(|error| invalid(&error.to_string()))?;
    Ok(VisibleBlobChildren {
        values: blob_values(selected.as_ref())?.clone(),
        offsets: OffsetBuffer::new(ScalarBuffer::from(output_offsets)),
        indices: Some(indices),
    })
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
    async fn video_frames_remain_lazy_descriptors() {
        use crate::spec::{BlobType, DataType, VideoFrameDescriptor};
        use futures::TryStreamExt;

        let frame = VideoFrameDescriptor::new("memory:/must-not-open-video".into(), 0, 8, 2, -1, 0)
            .unwrap()
            .serialize();
        let fields = [DataField::new(
            0,
            "frame".into(),
            DataType::Blob(BlobType::new()),
        )];
        let batch = RecordBatch::try_from_iter([(
            "frame",
            Arc::new(LargeBinaryArray::from(vec![frame.as_slice()])) as ArrayRef,
        )])
        .unwrap();
        let options = HashMap::from([("video-frame-field".into(), "frame".into())]);
        let stream = Box::pin(futures::stream::iter([Ok(batch)]));
        let batches: Vec<_> = resolve_primary_key_blob_stream(
            stream,
            &fields,
            &CoreOptions::new(&options),
            FileIOBuilder::new("memory").build().unwrap(),
            2,
            None,
        )
        .try_collect()
        .await
        .unwrap();
        assert_eq!(batches.len(), 1);
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert_eq!(values.value(0), frame);
    }

    #[tokio::test]
    async fn payload_filter_short_circuits_boolean_branches_and_null_checks() {
        use crate::spec::{BlobType, DataType, Datum, IntType, PredicateBuilder};
        use futures::TryStreamExt;

        let io = FileIOBuilder::new("memory").build().unwrap();
        io.new_output("memory:/predicate-selected")
            .unwrap()
            .write(bytes::Bytes::from_static(b"yes"))
            .await
            .unwrap();
        let good = BlobDescriptor::new("memory:/predicate-selected".into(), 0, 3).serialize();
        let missing = BlobDescriptor::new("memory:/must-not-open".into(), 0, 3).serialize();
        let fields = vec![
            DataField::new(0, "id".into(), DataType::Int(IntType::new())),
            DataField::new(1, "payload".into(), DataType::Blob(BlobType::new())),
        ];
        let batch = RecordBatch::try_new(
            crate::arrow::build_target_arrow_schema(&fields).unwrap(),
            vec![
                Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3])),
                Arc::new(LargeBinaryArray::from(vec![
                    Some(missing.as_slice()),
                    Some(good.as_slice()),
                    None,
                ])),
            ],
        )
        .unwrap();
        let builder = PredicateBuilder::new(&fields);
        let equal_payload = builder
            .equal("payload", Datum::Bytes(b"yes".to_vec()))
            .unwrap();
        let id_one = builder.equal("id", Datum::Int(1)).unwrap();
        let cases = [
            (
                Predicate::Or(vec![id_one.clone(), equal_payload.clone()]),
                vec![1, 2],
            ),
            (
                Predicate::Or(vec![
                    Predicate::And(vec![
                        id_one.clone(),
                        builder.is_not_null("payload").unwrap(),
                    ]),
                    Predicate::And(vec![equal_payload.clone(), equal_payload.clone()]),
                ]),
                vec![1, 2],
            ),
            (
                Predicate::And(vec![
                    builder.is_not_null("payload").unwrap(),
                    Predicate::Not(Box::new(Predicate::Or(vec![
                        id_one,
                        builder
                            .not_equal("payload", Datum::Bytes(b"yes".to_vec()))
                            .unwrap(),
                    ]))),
                ]),
                vec![2],
            ),
            (builder.is_null("payload").unwrap(), vec![3]),
            (builder.is_not_null("payload").unwrap(), vec![1, 2]),
        ];
        let options = std::collections::HashMap::new();
        let options = CoreOptions::new(&options);
        for (predicate, expected) in cases {
            for limit in [None, Some(1), Some(2)] {
                let plan = ManagedBlobReadPlan::new(
                    &fields[..1],
                    std::slice::from_ref(&predicate),
                    &fields,
                    &options,
                )
                .unwrap();
                let stream = Box::pin(futures::stream::iter(vec![Ok(batch.clone())]));
                let batches: Vec<RecordBatch> = plan
                    .finish(stream, &options, io.clone(), 2, limit)
                    .try_collect()
                    .await
                    .unwrap();
                let actual: Vec<i32> = batches
                    .iter()
                    .flat_map(|batch| {
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<arrow_array::Int32Array>()
                            .unwrap()
                            .values()
                            .to_vec()
                    })
                    .collect();
                assert_eq!(
                    actual,
                    expected[..limit.unwrap_or(expected.len()).min(expected.len())]
                );
            }
        }
    }

    #[tokio::test]
    async fn predicate_and_output_share_payload_bytes_resembling_a_descriptor() {
        use crate::spec::{BlobType, DataType, Datum, PredicateBuilder};
        use futures::TryStreamExt;

        let io = FileIOBuilder::new("memory").build().unwrap();
        let payload =
            BlobDescriptor::new("memory:/returned-bytes-must-not-open".into(), 0, 3).serialize();
        io.new_output("memory:/predicate-payload")
            .unwrap()
            .write(bytes::Bytes::from(payload.clone()))
            .await
            .unwrap();
        let descriptor =
            BlobDescriptor::new("memory:/predicate-payload".into(), 0, payload.len() as i64)
                .serialize();
        let fields = vec![DataField::new(
            0,
            "payload".into(),
            DataType::Blob(BlobType::new()),
        )];
        let batch = RecordBatch::try_new(
            crate::arrow::build_target_arrow_schema(&fields).unwrap(),
            vec![Arc::new(LargeBinaryArray::from(vec![Some(
                descriptor.as_slice(),
            )]))],
        )
        .unwrap();
        let predicate = PredicateBuilder::new(&fields)
            .equal("payload", Datum::Bytes(payload.clone()))
            .unwrap();
        let options = std::collections::HashMap::new();
        let options = CoreOptions::new(&options);
        let plan = ManagedBlobReadPlan::new(
            &fields,
            &[Predicate::And(vec![predicate.clone(), predicate])],
            &fields,
            &options,
        )
        .unwrap();
        let stream = Box::pin(futures::stream::iter(vec![Ok(batch)]));
        let batches: Vec<RecordBatch> = plan
            .finish(stream, &options, io, 2, None)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .value(0),
            payload
        );
    }

    #[tokio::test]
    async fn collection_limit_skips_unselected_children_with_both_nullabilities() {
        use crate::spec::{ArrayType, BlobType, DataType, MapType, VarCharType};
        use futures::TryStreamExt;
        for nullable in [true, false] {
            let io = FileIOBuilder::new("memory").build().unwrap();
            io.new_output("memory:/selected")
                .unwrap()
                .write(bytes::Bytes::from_static(b"selected"))
                .await
                .unwrap();
            let selected = BlobDescriptor::new("memory:/selected".into(), 0, 8).serialize();
            let missing = BlobDescriptor::new("memory:/missing".into(), 0, 8).serialize();
            let fields = vec![
                DataField::new(
                    0,
                    "items".into(),
                    DataType::Array(ArrayType::new(DataType::Blob(BlobType::with_nullable(
                        nullable,
                    )))),
                ),
                DataField::new(
                    1,
                    "named".into(),
                    DataType::Map(MapType::new(
                        DataType::VarChar(VarCharType::string_type()),
                        DataType::Blob(BlobType::with_nullable(nullable)),
                    )),
                ),
            ];
            let schema = crate::arrow::build_target_arrow_schema(&fields).unwrap();
            let values: ArrayRef = Arc::new(LargeBinaryArray::from(vec![
                Some(selected.as_slice()),
                Some(missing.as_slice()),
            ]));
            let ArrowDataType::List(element) = schema.field(0).data_type() else {
                panic!("expected list")
            };
            let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 2]));
            let list: ArrayRef = Arc::new(
                ListArray::try_new(element.clone(), offsets.clone(), values.clone(), None).unwrap(),
            );
            let ArrowDataType::Map(entries_field, ordered) = schema.field(1).data_type() else {
                panic!("expected map")
            };
            let ArrowDataType::Struct(entry_fields) = entries_field.data_type() else {
                panic!("expected entries")
            };
            let entries = StructArray::try_new(
                entry_fields.clone(),
                vec![Arc::new(StringArray::from(vec!["keep", "skip"])), values],
                None,
            )
            .unwrap();
            let map: ArrayRef = Arc::new(
                MapArray::try_new(entries_field.clone(), offsets, entries, None, *ordered).unwrap(),
            );
            let batch = RecordBatch::try_new(schema, vec![list, map]).unwrap();
            let stream = Box::pin(futures::stream::iter(vec![Ok(batch)]));
            let batches: Vec<_> = resolve_primary_key_blob_stream(
                stream,
                &fields,
                &CoreOptions::new(&std::collections::HashMap::new()),
                io,
                2,
                Some(1),
            )
            .try_collect()
            .await
            .unwrap();
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].num_rows(), 1);
            let list = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            let items = list.value(0);
            assert_eq!(
                items
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .unwrap()
                    .value(0),
                b"selected"
            );
            let map = batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<MapArray>()
                .unwrap();
            let entries = map.value(0);
            assert_eq!(
                entries
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                "keep"
            );
            assert_eq!(
                entries
                    .column(1)
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .unwrap()
                    .value(0),
                b"selected"
            );
        }
    }

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
        assert_eq!(array.values().len(), 0);
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
        assert_eq!(map.entries().len(), 0);
    }
}
