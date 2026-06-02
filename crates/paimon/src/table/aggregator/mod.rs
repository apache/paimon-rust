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

//! Per-field aggregators used by `merge-engine=aggregation`.
//!
//! Each [`FieldAggregator`] accumulates values for one output column across
//! the rows that share a primary key and produces a single-row [`ArrayRef`].
//! The accumulator is reused across PK groups by calling [`reset`] between
//! groups.
//!
//! Reference: Java `org.apache.paimon.mergetree.compact.aggregate.FieldAggregator`
//! and the per-function factories under
//! `org.apache.paimon.mergetree.compact.aggregate.factory`.
//!
//! [`reset`]: FieldAggregator::reset

use std::collections::HashMap;

use arrow_array::{Array, ArrayRef};

use crate::spec::DataType;

mod bool_agg;
mod listagg;
mod numeric;
mod value;

pub(crate) use bool_agg::{BoolAndAgg, BoolOrAgg};
pub(crate) use listagg::ListaggAgg;
pub(crate) use numeric::{MaxAgg, MinAgg, ProductAgg, SumAgg};
pub(crate) use value::{FirstNonNullValueAgg, FirstValueAgg, LastNonNullValueAgg, LastValueAgg};

/// Per-field aggregator.
///
/// The merge function calls [`reset`] once at the start of each primary-key
/// group, then [`agg`] once per row in the group (in user-sequence order),
/// and finally [`result`] to materialize the single-row output column.
///
/// `agg` receives the source Arrow array plus the row index to read; the
/// implementation is expected to downcast to the appropriate typed array.
///
/// [`reset`]: FieldAggregator::reset
/// [`agg`]: FieldAggregator::agg
/// [`result`]: FieldAggregator::result
pub(crate) trait FieldAggregator: Send + Sync + std::fmt::Debug {
    /// Aggregator identifier, e.g. `"sum"`. Matches the
    /// `fields.<col>.aggregate-function` option value.
    fn name(&self) -> &'static str;

    /// Reset internal state at the start of a new primary-key group.
    fn reset(&mut self);

    /// Accumulate one input cell.
    fn agg(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()>;

    /// Materialize the current accumulator as a 1-row Arrow array.
    fn result(&self) -> crate::Result<ArrayRef>;
}

/// Construct an aggregator by `name` for a column of type `data_type`.
///
/// `field_name` and `table_options` are forwarded for per-field configuration
/// (e.g. `fields.<col>.list-agg-delimiter` for `listagg`).
///
/// Returns [`Error::ConfigInvalid`] when the name is unknown or the column
/// type is incompatible with the requested aggregator — both indicate a user
/// configuration error and should fail at table creation rather than at read
/// time.
///
/// [`Error::ConfigInvalid`]: crate::Error::ConfigInvalid
pub(crate) fn new_aggregator(
    name: &str,
    field_name: &str,
    data_type: &DataType,
    table_options: &HashMap<String, String>,
) -> crate::Result<Box<dyn FieldAggregator>> {
    match name {
        "sum" => Ok(Box::new(SumAgg::new(field_name, data_type)?)),
        "product" => Ok(Box::new(ProductAgg::new(field_name, data_type)?)),
        "min" => Ok(Box::new(MinAgg::new(field_name, data_type)?)),
        "max" => Ok(Box::new(MaxAgg::new(field_name, data_type)?)),
        "last_value" => Ok(Box::new(LastValueAgg::new(field_name, data_type)?)),
        "first_value" => Ok(Box::new(FirstValueAgg::new(field_name, data_type)?)),
        "last_non_null_value" => Ok(Box::new(LastNonNullValueAgg::new(field_name, data_type)?)),
        "first_non_null_value" => Ok(Box::new(FirstNonNullValueAgg::new(field_name, data_type)?)),
        "bool_and" => Ok(Box::new(BoolAndAgg::new(field_name, data_type)?)),
        "bool_or" => Ok(Box::new(BoolOrAgg::new(field_name, data_type)?)),
        "listagg" => Ok(Box::new(ListaggAgg::new(
            field_name,
            data_type,
            table_options,
        )?)),
        other => Err(crate::Error::ConfigInvalid {
            message: format!(
                "Unknown aggregate function '{other}' for field '{field_name}'; \
                 supported: sum, product, min, max, last_value, first_value, \
                 last_non_null_value, first_non_null_value, bool_and, bool_or, listagg"
            ),
        }),
    }
}

/// Helper: build a `ConfigInvalid` error for an unsupported (aggregator, type)
/// pair so every concrete aggregator emits the same phrasing.
pub(crate) fn unsupported_type_error(
    agg_name: &str,
    field_name: &str,
    data_type: &DataType,
) -> crate::Error {
    crate::Error::ConfigInvalid {
        message: format!(
            "Aggregate function '{agg_name}' does not support data type {data_type:?} \
             for field '{field_name}'"
        ),
    }
}
