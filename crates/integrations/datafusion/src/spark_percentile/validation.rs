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

use super::*;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::DFSchema;
use datafusion::logical_expr::expr::NullTreatment;
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::{Expr, LogicalPlan, WindowFunctionDefinition};
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::optimizer::simplify_expressions::ExprSimplifier;

#[derive(Debug)]
pub(crate) struct ValidatePercentiles;
impl AnalyzerRule for ValidatePercentiles {
    fn name(&self) -> &str {
        "validate_spark_percentiles"
    }
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        validate_plan(&plan, config)?;
        Ok(plan)
    }
}

// Run with the current session options, before empty-relation optimizations, and
// also before writing persistent view metadata (which only needs a logical plan).
pub(crate) fn validate_plan(plan: &LogicalPlan, config: &ConfigOptions) -> Result<()> {
    let info = SimplifyContext::builder()
        .with_config_options(Arc::new(config.clone()))
        .with_current_time()
        .build();
    let simplifier = ExprSimplifier::new(info);
    plan.apply_with_subqueries(|plan| {
        for expr in plan.expressions() {
            expr.apply(|expr| {
                let (function, args, null_treatment) = match expr {
                    Expr::AggregateFunction(aggregate) => (
                        Some(&aggregate.func),
                        &aggregate.params.args,
                        aggregate.params.null_treatment,
                    ),
                    Expr::WindowFunction(window) => {
                        let function = match &window.fun {
                            WindowFunctionDefinition::AggregateUDF(function) => Some(function),
                            WindowFunctionDefinition::WindowUDF(function) => function
                                .inner()
                                .downcast_ref::<super::window::PercentileWindow>()
                                .map(|w| &w.function),
                        };
                        (function, &window.params.args, window.params.null_treatment)
                    }
                    _ => return Ok(TreeNodeRecursion::Continue),
                };
                if let Some(function) = function {
                    if let Some(percentile) = function.inner().downcast_ref::<Percentile>() {
                        if null_treatment == Some(NullTreatment::IgnoreNulls) {
                            return Err(invalid("does not accept IGNORE NULLS"));
                        }
                        validate_arguments(percentile, args, &simplifier)?;
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

fn validate_arguments(
    function: &Percentile,
    args: &[Expr],
    simplifier: &ExprSimplifier,
) -> Result<()> {
    if !(2..=3).contains(&args.len()) {
        return Err(invalid("expects value, percentage [, accuracy]"));
    }
    let mut values = Vec::new();
    for expr in &args[1..] {
        expr.apply(|expr| {
            let immutable = match expr {
                Expr::Column(_)
                | Expr::ScalarVariable(..)
                | Expr::AggregateFunction(_)
                | Expr::WindowFunction(_)
                | Expr::Exists(_)
                | Expr::InSubquery(_)
                | Expr::ScalarSubquery(_)
                | Expr::Placeholder(_)
                | Expr::OuterReferenceColumn(..)
                | Expr::HigherOrderFunction(_) => false,
                Expr::ScalarFunction(function) => {
                    function.func.signature().volatility == Volatility::Immutable
                        || (function.args.is_empty()
                            && crate::compat_functions::is_native_context_constant(&function.func))
                }
                _ => true,
            };
            if !immutable {
                return Err(invalid(
                    "percentage and accuracy must be foldable immutable expressions",
                ));
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        let expr = simplifier.coerce(expr.clone(), &DFSchema::empty())?;
        let expr = simplifier.simplify(expr)?;
        values.push(
            expr.as_literal()
                .cloned()
                .ok_or_else(|| invalid("percentage and accuracy must be foldable"))?,
        );
    }
    let mut types = vec![DataType::Float64];
    types.extend(values.iter().map(ScalarValue::data_type));
    let types = function.coerce_types(&types)?;
    let probability = values[0].cast_to(&types[1])?;
    match probability {
        ScalarValue::Float64(Some(value)) => validate_probabilities(&[value])?,
        ScalarValue::List(values) if !values.is_null(0) => {
            let values = values.value(0);
            let values = values
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| invalid("percentage array must contain numbers"))?;
            if values.null_count() != 0 {
                return Err(invalid("percentage array cannot contain NULL"));
            }
            validate_probabilities(values.values())?;
        }
        _ => return Err(invalid("percentage cannot be NULL")),
    }
    if let Some(accuracy) = values.get(1) {
        match accuracy.cast_to(&types[2])? {
            ScalarValue::Int64(Some(value)) if (1..=i64::from(i32::MAX)).contains(&value) => {}
            _ => return Err(invalid("accuracy must be between 1 and 2147483647")),
        }
    }
    Ok(())
}
