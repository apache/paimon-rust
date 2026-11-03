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

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray};
use datafusion::arrow::compute::{and, filter, is_not_null, SortOptions};
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{Result, ScalarValue};
use datafusion::functions_aggregate::first_last::first_value_udaf;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::AggregateOrderSensitivity;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature, Volatility,
};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

// Adapt native first_value's ordering and state without evaluating either input twice.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Selection {
    name: &'static str,
    descending: bool,
    ignore_nulls: bool,
    signature: Signature,
}
pub(crate) fn register(ctx: &SessionContext) {
    for (name, descending, ignore_nulls) in [
        ("__paimon_max_by", true, false),
        ("__paimon_min_by", false, false),
        ("__paimon_arg_max", true, true),
    ] {
        ctx.register_udaf(AggregateUDF::from(Selection {
            name,
            descending,
            ignore_nulls,
            signature: Signature::any(2, Volatility::Immutable),
        }));
    }
}
impl Selection {
    fn ordering(&self, args: &AccumulatorArgs) -> [PhysicalSortExpr; 1] {
        [PhysicalSortExpr {
            expr: Arc::clone(&args.exprs[1]),
            options: SortOptions {
                descending: self.descending,
                nulls_first: false,
            },
        }]
    }
}
impl AggregateUDFImpl for Selection {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }
    fn return_field(&self, fields: &[FieldRef]) -> Result<FieldRef> {
        first_value_udaf().return_field(&fields[..1])
    }
    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let ordering = self.ordering(&args);
        let native = first_value_udaf().accumulator(AccumulatorArgs {
            order_bys: &ordering,
            ignore_nulls: self.ignore_nulls,
            exprs: &args.exprs[..1],
            expr_fields: &args.expr_fields[..1],
            ..args
        })?;
        Ok(Box::new(SelectionAccumulator {
            native,
            ignore_null_values: self.ignore_nulls,
        }))
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        // An empty partial group has no selected key, even if input keys are NOT NULL.
        let key = Arc::new(
            args.input_fields[1]
                .as_ref()
                .clone()
                .with_name(format!("{}[key]", args.name))
                .with_nullable(true),
        );
        first_value_udaf().state_fields(StateFieldsArgs {
            ordering_fields: &[key],
            input_fields: &args.input_fields[..1],
            ..args
        })
    }
    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        let ordering = self.ordering(&args);
        first_value_udaf().groups_accumulator_supported(AccumulatorArgs {
            order_bys: &ordering,
            ..args
        })
    }
    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let ordering = self.ordering(&args);
        let native = first_value_udaf().create_groups_accumulator(AccumulatorArgs {
            order_bys: &ordering,
            ignore_nulls: self.ignore_nulls,
            exprs: &args.exprs[..1],
            expr_fields: &args.expr_fields[..1],
            ..args
        })?;
        Ok(Box::new(SelectionGroupsAccumulator {
            native,
            ignore_null_values: self.ignore_nulls,
        }))
    }
    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        // The key is a normal argument. Native state compares it internally.
        AggregateOrderSensitivity::Insensitive
    }
}
fn eligibility(
    values: &[ArrayRef],
    ignore_null_values: bool,
    opt_filter: Option<&BooleanArray>,
) -> Result<Option<BooleanArray>> {
    let mut mask = None;
    for value in std::iter::once(&values[1]).chain(ignore_null_values.then_some(&values[0])) {
        if value.logical_null_count() > 0 {
            let valid = is_not_null(value.as_ref())?;
            mask = Some(match mask {
                Some(original) => and(&original, &valid)?,
                None => valid,
            });
        }
    }
    if let Some(original) = opt_filter {
        // Native grouped first_value reads filter value bits without its validity bitmap.
        let valid = if original.null_count() == 0 {
            original.clone()
        } else {
            BooleanArray::from(
                original
                    .iter()
                    .map(|value| value.unwrap_or(false))
                    .collect::<Vec<_>>(),
            )
        };
        mask = Some(match mask {
            Some(mask) => and(&mask, &valid)?,
            None => valid,
        });
    }
    Ok(mask)
}
#[derive(Debug)]
struct SelectionAccumulator {
    native: Box<dyn Accumulator>,
    ignore_null_values: bool,
}
impl Accumulator for SelectionAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(mask) = eligibility(values, self.ignore_null_values, None)? else {
            return self.native.update_batch(values);
        };
        let values = values
            .iter()
            .map(|value| filter(value.as_ref(), &mask).map_err(Into::into))
            .collect::<Result<Vec<_>>>()?;
        self.native.update_batch(&values)
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.native.evaluate()
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.native.state()
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.native.merge_batch(states)
    }
    fn size(&self) -> usize {
        self.native.size() + std::mem::size_of::<Self>()
    }
}
struct SelectionGroupsAccumulator {
    native: Box<dyn GroupsAccumulator>,
    ignore_null_values: bool,
}
impl GroupsAccumulator for SelectionGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let mask = eligibility(values, self.ignore_null_values, opt_filter)?;
        self.native
            .update_batch(values, group_indices, mask.as_ref(), total_num_groups)
    }
    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        self.native.evaluate(emit_to)
    }
    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.native.state(emit_to)
    }
    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.native
            .merge_batch(values, group_indices, total_num_groups)
    }
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let mask = eligibility(values, self.ignore_null_values, opt_filter)?;
        self.native.convert_to_state(values, mask.as_ref())
    }
    fn size(&self) -> usize {
        self.native.size() + std::mem::size_of::<Self>()
    }
}
