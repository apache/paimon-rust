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

//! Exact row-ID filtering shared by data-evolution index searches.

use crate::spec::{Predicate, ROW_ID_FIELD_NAME};
use crate::table::{RowRange, Table};
use arrow_array::{Array, Int64Array};
use futures::TryStreamExt;
use roaring::RoaringTreemap;

pub(super) async fn matching_row_ids_for_filter(
    table: &Table,
    filter: &Predicate,
    ranges: Option<Vec<RowRange>>,
) -> crate::Result<RoaringTreemap> {
    let mut read_builder = table.new_read_builder();
    read_builder
        .with_projection(&[ROW_ID_FIELD_NAME])?
        .with_filter(filter.clone());
    if let Some(ranges) = ranges {
        read_builder.with_row_ranges(ranges);
    }
    let plan = read_builder.new_scan().plan().await?;
    let read = read_builder.new_read()?;
    let mut stream = read.to_arrow(plan.splits())?;
    let mut row_ids = RoaringTreemap::new();
    while let Some(batch) = stream.try_next().await? {
        let index =
            batch
                .schema()
                .index_of(ROW_ID_FIELD_NAME)
                .map_err(|_| crate::Error::DataInvalid {
                    message: format!("global index row filter read is missing {ROW_ID_FIELD_NAME}"),
                    source: None,
                })?;
        let values = batch
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!("global index row filter {ROW_ID_FIELD_NAME} column is not Int64"),
                source: None,
            })?;
        for row in 0..values.len() {
            if values.is_null(row) {
                return Err(crate::Error::DataInvalid {
                    message: format!("global index row filter produced a null {ROW_ID_FIELD_NAME}"),
                    source: None,
                });
            }
            let row_id = values.value(row);
            let row_id = u64::try_from(row_id).map_err(|_| crate::Error::DataInvalid {
                message: format!(
                    "global index row filter produced a negative {ROW_ID_FIELD_NAME}: {row_id}"
                ),
                source: None,
            })?;
            row_ids.insert(row_id);
        }
    }
    Ok(row_ids)
}
