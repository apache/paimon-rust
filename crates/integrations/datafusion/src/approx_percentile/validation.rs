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

use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::{Expr, LogicalPlan, WindowFunctionDefinition};
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::optimizer::simplify_expressions::ExprSimplifier;

use super::{error, read_accuracy, read_percentages, ApproxPercentile};

/// Validate before optimizers can eliminate an aggregate or window with LIMIT 0.
/// UDAF simplification alone cannot cover aggregate window expressions in DataFusion.
#[derive(Debug)]
pub(crate) struct ValidateParameters;

impl AnalyzerRule for ValidateParameters {
    fn name(&self) -> &str {
        "validate_spark_approx_percentile_parameters"
    }

    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let simplifier = ExprSimplifier::new(
            SimplifyContext::builder()
                .with_config_options(Arc::new(config.clone()))
                .with_current_time()
                .build(),
        );
        plan.apply_with_subqueries(|node| {
            for expr in node.expressions() {
                expr.apply(|expr| {
                    let args = match expr {
                        Expr::AggregateFunction(call)
                            if call.func.inner().is::<ApproxPercentile>() =>
                        {
                            Some(&call.params.args)
                        }
                        Expr::WindowFunction(call) => match &call.fun {
                            WindowFunctionDefinition::AggregateUDF(func)
                                if func.inner().is::<ApproxPercentile>() =>
                            {
                                Some(&call.params.args)
                            }
                            _ => None,
                        },
                        _ => None,
                    };
                    if let Some(args) = args {
                        let percentage = args
                            .get(1)
                            .ok_or_else(|| error("approx_percentile requires a percentage"))?;
                        read_percentages(literal(percentage, "percentage", &simplifier)?)?;
                        if let Some(accuracy) = args.get(2) {
                            read_accuracy(literal(accuracy, "accuracy", &simplifier)?)?;
                        }
                    }
                    Ok(TreeNodeRecursion::Continue)
                })?;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(plan)
    }
}

fn literal(expr: &Expr, name: &str, simplifier: &ExprSimplifier) -> Result<ScalarValue> {
    if expr.column_refs().is_empty() {
        if let Expr::Literal(value, _) = simplifier.simplify(expr.clone())? {
            return Ok(value);
        }
    }
    Err(error(format!(
        "approx_percentile {name} must be a constant expression"
    )))
}
