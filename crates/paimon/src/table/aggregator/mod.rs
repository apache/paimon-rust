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

    /// Accumulate an input that sorts before the current accumulator.
    ///
    /// This mirrors Java `FieldAggregator#aggReversed(accumulator, input)`,
    /// whose default semantics are `agg(input, accumulator)`. Implementations
    /// must define this explicitly so order-sensitive aggregators cannot
    /// silently fall back to forward accumulation.
    fn agg_reversed(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()>;

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
    // `canonical_aggregator_name` folds Java's undocumented SPI alias
    // `first_not_null_value` onto `first_non_null_value`; the error arm still
    // echoes the caller's spelling. The aggregator itself reports the canonical
    // name from `FieldAggregator::name`, which costs no parity: the one Java
    // message that repeats the configured identifier is the retract rejection in
    // `FieldAggregator`, and retract is rejected here before an aggregator is
    // ever built. Java's non-nullable diagnostic
    // (`AggregateMergeFunction`: "Field <i> can not be null") names no function
    // at all, so the Rust equivalent in `sort_merge` is strictly more specific.
    match crate::spec::canonical_aggregator_name(name) {
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
        _ => Err(crate::Error::ConfigInvalid {
            message: format!(
                "Unknown aggregate function '{name}' for field '{field_name}'; \
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

#[cfg(test)]
mod tests {
    use arrow_array::Int32Array;

    use super::*;
    use crate::spec::IntType;

    /// Java's legacy alias must build the very same aggregator, not merely pass
    /// validation: `FieldFirstNonNullValueAggLegacyFactory` returns a
    /// `FieldFirstNonNullValueAgg`, so both names have to lock the first
    /// non-null value.
    #[test]
    fn test_legacy_first_not_null_value_aggregates_like_the_canonical_name() {
        let data_type = DataType::Int(IntType::new());
        let options = HashMap::new();
        let input = Int32Array::from(vec![None, Some(5), Some(7)]);

        let mut results = Vec::new();
        for name in ["first_non_null_value", "first_not_null_value"] {
            let mut agg = new_aggregator(name, "v", &data_type, &options)
                .unwrap_or_else(|err| panic!("'{name}' should construct: {err:?}"));
            for row in 0..input.len() {
                agg.agg(&input, row).unwrap();
            }
            let out = agg.result().unwrap();
            let out = out
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32 result");
            results.push(out.is_valid(0).then(|| out.value(0)));
        }
        assert_eq!(results[0], Some(5));
        assert_eq!(results[0], results[1], "alias diverged from canonical name");
    }

    /// An unknown name is echoed back exactly as written, so resolving the alias
    /// never renames what the user configured.
    #[test]
    fn test_unknown_aggregate_function_is_echoed_verbatim() {
        let err = new_aggregator(
            "first_not_null_valu",
            "v",
            &DataType::Int(IntType::new()),
            &HashMap::new(),
        )
        .unwrap_err();
        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("'first_not_null_valu'")),
            "expected the caller's spelling, got {err:?}"
        );
    }
}
