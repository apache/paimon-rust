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

use super::{PaimonTableRead, TableRead, TableReadKind};
use crate::spec::{row_id_data_field, ROW_ID_FIELD_ID};
use crate::table::pk_vector_indexed_split_read::{PkVectorIndexedSplit, PkVectorIndexedSplitRead};
use crate::table::{ArrowRecordBatchStream, DataSplit, RowRange};
use arrow_array::{ArrayRef, Float32Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use futures::{stream, StreamExt};
use std::sync::Arc;

fn invalid(message: &str) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

fn scored_batch(
    batch: RecordBatch,
    count: usize,
    name: &str,
    scores: ArrayRef,
) -> crate::Result<RecordBatch> {
    let schema = batch.schema();
    let mut fields = schema.fields()[..count].to_vec();
    let mut columns = batch.columns()[..count].to_vec();
    fields.push(Arc::new(Field::new(name, DataType::Float32, true)));
    columns.push(scores);
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
        columns,
    )
    .map_err(|error| invalid(&format!("Invalid scored batch: {error}")))
}

impl TableRead<'_> {
    /// Read one Java IndexedSplit, appending its nullable Float32 score under
    /// `name`. A split without scores produces nulls. Tracked rows use global
    /// row IDs; file-position selections use the same DV-aware reader as PK
    /// vector search. Physical position reads require a predicate-free reader.
    pub fn to_arrow_with_index_score(
        &self,
        split: &DataSplit,
        name: &str,
    ) -> crate::Result<ArrowRecordBatchStream> {
        let ordered = split.ordered_for_score_projection()?;
        let split = ordered.as_ref();
        let selected = std::slice::from_ref(split);
        let ranges = split
            .row_ranges()
            .ok_or_else(|| invalid("Score projection requires an IndexedSplit"))?;
        if name.is_empty() || self.read_type().iter().any(|f| f.name() == name) {
            return Err(invalid(
                "Index score field name must be nonempty and distinct from the projected fields",
            ));
        }
        let name = name.to_owned();
        let count = self.read_type().len();
        let Some(scores) = split.scores() else {
            return Ok(self
                .to_arrow(selected)?
                .map(move |batch| {
                    let batch = batch?;
                    let scores = Arc::new(Float32Array::new_null(batch.num_rows()));
                    scored_batch(batch, count, &name, scores)
                })
                .boxed());
        };
        let TableReadKind::Paimon(read) = &self.kind else {
            return Err(invalid("Scored data splits require a Paimon table"));
        };
        let options = read.table.schema().core_options();
        if read.table.schema().primary_keys().is_empty()
            && (options.row_tracking_enabled() || options.data_evolution_enabled())
        {
            return read_tracked(read.clone(), split, ranges, name);
        }

        // A physical-position reader cannot apply an additional filter before
        // recovering positions. Restricted grants must use the ordinary reader.
        let grant = read.ensure_authorized_by_splits(&options, selected)?;
        if grant.is_some()
            || !read.data_predicates.is_empty()
            || read.row_filter_factory.is_some()
            || read.update_read
            || read.limit.is_some()
        {
            return Err(crate::Error::Unsupported {message: "File-position score projection requires an unrestricted, predicate-free read without a row limit".into()});
        }
        if read.matching_splits(selected)?.is_empty() || ranges.is_empty() {
            return Ok(stream::empty().boxed());
        }
        if !read.table.schema().primary_keys().is_empty() && split.data_files().len() != 1 {
            return Err(invalid(
                "A scored primary-key split must select one physical file",
            ));
        }
        if read.table.schema().primary_keys().is_empty() {
            return read_positions(read, split, ranges, scores, name);
        }
        read.resolving_primary_key_blobs(&options, |inner| {
            read_positions(inner, split, ranges, scores, name)
        })
    }
}

fn read_tracked(
    mut read: PaimonTableRead<'_>,
    split: &DataSplit,
    ranges: &[RowRange],
    name: String,
) -> crate::Result<ArrowRecordBatchStream> {
    let count = read.read_type.len();
    let index = read
        .read_type
        .iter()
        .position(|f| f.id() == ROW_ID_FIELD_ID)
        .unwrap_or_else(|| {
            read.read_type.push(row_id_data_field());
            count
        });
    // Each range indexes a contiguous slice of the split's score array.
    let mut offset = 0;
    let offsets: Vec<usize> = ranges
        .iter()
        .map(|r| {
            let start = offset;
            offset += (r.to() - r.from() + 1) as usize;
            start
        })
        .collect();
    let split = split.clone();
    Ok(read
        .execute_splits(std::slice::from_ref(&split), 1, true, false)?
        .map(move |batch| {
            let batch = batch?;
            let ids = batch
                .column(index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| invalid("Expected Int64 row IDs"))?;
            let ranges = split.row_ranges().unwrap();
            let scores = split.scores().unwrap();
            let values = ids
                .iter()
                .map(|id| {
                    let id = id.ok_or_else(|| invalid("Missing row ID for scored read"))?;
                    let i = ranges.partition_point(|r| r.to() < id);
                    let r = ranges
                        .get(i)
                        .filter(|r| r.from() <= id)
                        .ok_or_else(|| invalid("Row is outside scored selection"))?;
                    Ok(scores[offsets[i] + (id - r.from()) as usize])
                })
                .collect::<crate::Result<Vec<_>>>()?;
            scored_batch(batch, count, &name, Arc::new(Float32Array::from(values)))
        })
        .boxed())
}

fn read_positions(
    read: &PaimonTableRead<'_>,
    split: &DataSplit,
    ranges: &[RowRange],
    scores: &[f32],
    name: String,
) -> crate::Result<ArrowRecordBatchStream> {
    let reader = PkVectorIndexedSplitRead::new(read.new_data_file_reader()?);
    let mut streams = Vec::new();
    let mut base = 0_i64;
    let mut consumed = 0;
    let mut next_range = 0;
    for (file_index, file) in split.data_files().iter().enumerate() {
        if file.row_count < 0 {
            return Err(invalid("Negative file row count"));
        }
        if file.row_count == 0 {
            continue;
        }
        let end = base
            .checked_add(file.row_count)
            .ok_or_else(|| invalid("Split row count overflows"))?;
        let mut local = Vec::new();
        let start = consumed;
        while let Some(r) = ranges.get(next_range).filter(|r| r.from() < end) {
            let low = r.from().max(base);
            let high = r.to().min(end - 1);
            local.push(RowRange::new(low - base, high - base));
            consumed += (high - low + 1) as usize;
            if r.to() >= end {
                break;
            }
            next_range += 1;
        }
        if !local.is_empty() {
            streams.push(reader.read_projected(
                &PkVectorIndexedSplit {
                    split: split.for_pk_index_file(file_index),
                    row_ranges: local,
                    scores: Some(scores[start..consumed].to_vec()),
                },
                Some(name.clone()),
            )?);
        }
        base = end;
    }
    if consumed != scores.len() {
        return Err(invalid("Scored positions exceed the split's data files"));
    }
    Ok(stream::iter(streams).flatten().boxed())
}
