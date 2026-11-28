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

mod audit_log;
mod range_partitioning;
pub(crate) mod scan;
mod search_score;
pub(crate) mod sink;

use std::sync::Arc;

use datafusion::arrow::array::{make_array, ArrayData, ArrayRef};
use datafusion::arrow::compute::{can_cast_types, cast};
use datafusion::arrow::datatypes::{DataType, FieldRef, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::error::{DataFusionError, Result as DFResult};

pub(crate) use audit_log::PaimonAuditLogScan;
pub use scan::PaimonTableScan;
pub(crate) use search_score::{SearchScoreExec, SearchScoreOutputColumn};
pub use sink::PaimonDataSink;

// Arrow casts maps only when their ordering flags agree. Removing the sorted
// guarantee preserves the buffers, including offsets and nulls in nested maps.
fn normalize_map_ordering(data: ArrayData) -> Result<ArrayData, ArrowError> {
    use DataType::*;
    if data.child_data().is_empty() {
        return Ok(data);
    }
    let children = data
        .child_data()
        .iter()
        .cloned()
        .map(normalize_map_ordering)
        .collect::<Result<Vec<_>, _>>()?;
    let field = |original: &FieldRef, index: usize| {
        Arc::new(
            original
                .as_ref()
                .clone()
                .with_data_type(children[index].data_type().clone()),
        )
    };
    let data_type = match data.data_type() {
        Map(entries, _) => Map(field(entries, 0), false),
        Struct(fields) => Struct(
            fields
                .iter()
                .enumerate()
                .map(|(i, f)| field(f, i))
                .collect(),
        ),
        List(f) => List(field(f, 0)),
        LargeList(f) => LargeList(field(f, 0)),
        FixedSizeList(f, size) => FixedSizeList(field(f, 0), *size),
        Dictionary(key, _) => Dictionary(key.clone(), Box::new(children[0].data_type().clone())),
        _ => return Ok(data),
    };
    if &data_type == data.data_type() {
        return Ok(data);
    }
    data.into_builder()
        .data_type(data_type)
        .child_data(children)
        .build()
}

fn cast_column(column: &ArrayRef, target: &DataType) -> Result<ArrayRef, ArrowError> {
    if column.data_type() == target {
        return Ok(Arc::clone(column));
    }
    if !can_cast_types(column.data_type(), target) {
        let data = normalize_map_ordering(column.to_data())?;
        if data.data_type() != column.data_type() {
            return cast(&make_array(data), target);
        }
    }
    cast(column, target)
}

pub(crate) fn cast_batch_to_schema(
    batch: RecordBatch,
    schema: &SchemaRef,
) -> DFResult<RecordBatch> {
    if batch.schema_ref() == schema {
        return Ok(batch);
    }
    if batch.num_columns() != schema.fields().len() {
        return Err(DataFusionError::Execution(format!(
            "Cannot cast a batch with {} columns to a schema with {} fields",
            batch.num_columns(),
            schema.fields().len()
        )));
    }
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| cast_column(column, field.data_type()))
        .collect::<Result<Vec<_>, _>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(Arc::clone(schema), columns, &options).map_err(Into::into)
}
