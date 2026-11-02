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

use datafusion::arrow::array::{new_empty_array, ArrayRef, BooleanArray};
use datafusion::arrow::compute::filter;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::common::config::ConfigOptions;
use datafusion::common::utils::SingleRowListArrayBuilder;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult};
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::type_coercion::functions::fields_with_udf;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, EmitTo, Expr, GroupsAccumulator,
    ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
};
use datafusion::prelude::SessionContext;
use std::sync::Arc;

// Adapt input and return types while retaining upstream evaluation and simplification.
pub(crate) fn apply_compatibility_fixes(ctx: &SessionContext) {
    ctx.register_udf(date_part().as_ref().clone());
    for name in [
        "base64",
        "sha2",
        "date_add",
        "date_sub",
        "factorial",
        "mod",
        "pmod",
        "hex",
        "unhex",
        "hour",
        "size",
    ] {
        let inner = Arc::clone(&ctx.state().scalar_functions()[name]);
        ctx.register_udf(ScalarUDF::from(Inputs::new(inner)));
    }
    for name in ["collect_list", "collect_set", "try_sum", "avg"] {
        let inner = Arc::clone(&ctx.state().aggregate_functions()[name]);
        ctx.register_udaf(AggregateUDF::from(AggregateInputs { inner }));
    }
}

fn date_part() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(DatePart {
        inner: datafusion_spark::function::datetime::date_part(),
    }))
}

#[derive(Debug)]
pub(crate) struct DatePartPlanner;

impl ExprPlanner for DatePartPlanner {
    fn plan_extract(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Planned(Expr::ScalarFunction(
            ScalarFunction::new_udf(date_part(), args),
        )))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DatePart {
    inner: Arc<ScalarUDF>,
}

impl ScalarUDFImpl for DatePart {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }
    fn signature(&self) -> &Signature {
        self.inner.signature()
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        self.inner.inner().return_type(types)
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        // Spark 54 declares every part as Int32 before delegating to this native UDF.
        // Use its actual field so coercion preserves epoch fractions and nanoseconds.
        datafusion::functions::datetime::date_part()
            .inner()
            .return_field_from_args(args)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.inner.invoke_with_args(args)
    }
    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        self.inner.inner().simplify(args, info)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AggregateInputs {
    inner: Arc<AggregateUDF>,
}
impl AggregateUDFImpl for AggregateInputs {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn signature(&self) -> &Signature {
        self.inner.signature()
    }
    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.inner().coerce_types(types)
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        self.inner.inner().return_type(types)
    }
    fn return_field(&self, fields: &[FieldRef]) -> Result<FieldRef> {
        self.inner.return_field(fields)
    }
    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        match self.name() {
            "collect_list" if !args.order_bys.is_empty() => Err(DataFusionError::NotImplemented(
                "collect_list ORDER BY is not supported by DataFusion Spark 54".into(),
            )),
            "collect_list" if args.is_distinct => {
                datafusion_spark::function::aggregate::collect_set().accumulator(args)
            }
            "try_sum" if args.is_distinct => Err(DataFusionError::NotImplemented(
                "try_sum DISTINCT is not supported by DataFusion Spark 54".into(),
            )),
            _ => self.inner.accumulator(args),
        }
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        if self.name() == "collect_list" && args.is_distinct {
            datafusion_spark::function::aggregate::collect_set().state_fields(args)
        } else {
            self.inner.state_fields(args)
        }
    }
    fn default_value(&self, ty: &DataType) -> Result<ScalarValue> {
        if matches!(self.name(), "collect_list" | "collect_set") {
            let DataType::List(field) = ty else {
                return Err(DataFusionError::Internal(
                    "Spark collection return type must be a list".into(),
                ));
            };
            Ok(
                SingleRowListArrayBuilder::new(new_empty_array(field.data_type()))
                    .build_list_scalar(),
            )
        } else {
            let value = self.inner.inner().default_value(ty)?;
            if value == ScalarValue::Null {
                ScalarValue::try_new_null(ty)
            } else {
                Ok(value)
            }
        }
    }
    fn reverse_expr(&self) -> datafusion::logical_expr::ReversedUDAF {
        self.inner.inner().reverse_expr()
    }
    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        self.inner.groups_accumulator_supported(args)
    }
    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(FilteredGroups(
            self.inner.create_groups_accumulator(args)?,
        )))
    }
}

// Spark 54 AVG ignores its grouped FILTER argument. Pre-filter its already-evaluated
// arrays and group indices, keeping the upstream vectorized accumulator and state.
struct FilteredGroups(Box<dyn GroupsAccumulator>);
impl FilteredGroups {
    fn filter(
        values: &[ArrayRef],
        groups: &[usize],
        predicate: &BooleanArray,
    ) -> Result<(Vec<ArrayRef>, Vec<usize>)> {
        let arrays = values
            .iter()
            .map(|value| filter(value.as_ref(), predicate).map_err(Into::into))
            .collect::<Result<Vec<_>>>()?;
        let groups = groups
            .iter()
            .zip(predicate.iter())
            .filter_map(|(group, pass)| (pass == Some(true)).then_some(*group))
            .collect();
        Ok((arrays, groups))
    }
}
impl GroupsAccumulator for FilteredGroups {
    fn supports_convert_to_state(&self) -> bool {
        self.0.supports_convert_to_state()
    }
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        predicate: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        self.0.convert_to_state(values, predicate)
    }
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        groups: &[usize],
        predicate: Option<&BooleanArray>,
        total: usize,
    ) -> Result<()> {
        if let Some(predicate) = predicate {
            let (values, groups) = Self::filter(values, groups, predicate)?;
            self.0.update_batch(&values, &groups, None, total)
        } else {
            self.0.update_batch(values, groups, None, total)
        }
    }
    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        groups: &[usize],
        predicate: Option<&BooleanArray>,
        total: usize,
    ) -> Result<()> {
        if let Some(predicate) = predicate {
            let (values, groups) = Self::filter(values, groups, predicate)?;
            self.0.merge_batch(&values, &groups, None, total)
        } else {
            self.0.merge_batch(values, groups, None, total)
        }
    }
    fn evaluate(&mut self, emit: EmitTo) -> Result<ArrayRef> {
        self.0.evaluate(emit)
    }
    fn state(&mut self, emit: EmitTo) -> Result<Vec<ArrayRef>> {
        self.0.state(emit)
    }
    fn size(&self) -> usize {
        self.0.size() + std::mem::size_of::<Self>()
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Inputs {
    inner: Arc<ScalarUDF>,
    signature: Signature,
}
impl Inputs {
    fn new(inner: Arc<ScalarUDF>) -> Self {
        Self {
            signature: Signature::user_defined(inner.signature().volatility),
            inner,
        }
    }
}
fn decoded(mut ty: &DataType) -> &DataType {
    while let DataType::Dictionary(_, value) = ty {
        ty = value;
    }
    ty
}
impl ScalarUDFImpl for Inputs {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        let mut types = types
            .iter()
            .map(|ty| decoded(ty).clone())
            .collect::<Vec<_>>();
        match (self.name(), types.as_mut_slice()) {
            ("hex", [value]) if *value == DataType::BinaryView => *value = DataType::Binary,
            ("sha2", [value, _]) if matches!(value, DataType::FixedSizeBinary(_)) => {
                *value = DataType::Binary;
            }
            ("hex" | "unhex", [value]) if *value == DataType::Null => *value = DataType::Utf8,
            ("hour", [value]) if *value == DataType::Null => {
                *value = DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            ("size", [value]) if *value == DataType::Null => {
                *value = DataType::List(Arc::new(Field::new("item", DataType::Null, true)))
            }
            ("date_add" | "date_sub", [date, days]) => {
                if matches!(
                    date,
                    DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 | DataType::Null
                ) {
                    *date = DataType::Date32;
                }
                if matches!(days, DataType::Int64 | DataType::Null) {
                    *days = DataType::Int32;
                }
            }
            ("factorial", [value]) if *value == DataType::Int64 => *value = DataType::Int32,
            ("mod" | "pmod", [left, right]) => {
                if *left == DataType::Null {
                    *left = if *right == DataType::Null {
                        DataType::Int64
                    } else {
                        right.clone()
                    };
                }
                if *right == DataType::Null {
                    *right = left.clone();
                }
            }
            _ => {}
        }
        let fields = types
            .iter()
            .map(|ty| Arc::new(Field::new("arg", ty.clone(), true)))
            .collect::<Vec<_>>();
        Ok(fields_with_udf(&fields, self.inner.as_ref())?
            .iter()
            .map(|field| field.data_type().clone())
            .collect())
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        self.inner.inner().return_type(types)
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.inner.inner().return_field_from_args(args)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.inner.invoke_with_args(args)
    }
    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        self.inner.inner().simplify(args, info)
    }
    fn with_updated_config(&self, config: &ConfigOptions) -> Option<ScalarUDF> {
        self.inner
            .inner()
            .with_updated_config(config)
            .map(|inner| ScalarUDF::from(Self::new(Arc::new(inner))))
    }
}
