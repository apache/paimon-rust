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
use datafusion::arrow::datatypes::Schema;
use datafusion::logical_expr::expr::NullTreatment;
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::planner::{PlannerResult, RawWindowExpr};
use datafusion::logical_expr::{
    PartitionEvaluator, WindowFunctionDefinition, WindowUDF, WindowUDFImpl,
};
use std::ops::Range;

pub(crate) fn plan_window(mut expr: RawWindowExpr) -> Result<PlannerResult<RawWindowExpr>> {
    if let WindowFunctionDefinition::AggregateUDF(function) = &expr.func_def {
        if function.inner().downcast_ref::<Percentile>().is_some() {
            if expr.distinct
                || expr.filter.is_some()
                || expr.null_treatment == Some(NullTreatment::IgnoreNulls)
            {
                return Err(invalid(
                    "Spark percentile windows do not accept DISTINCT, FILTER or IGNORE NULLS",
                ));
            }
            // Growing frames keep DataFusion's online aggregate path. Native sliding
            // aggregates lose frame transitions at empty RANGE frames in DF 54.
            if !expr.window_frame.is_ever_expanding() {
                expr.func_def = WindowFunctionDefinition::WindowUDF(Arc::new(WindowUDF::from(
                    PercentileWindow {
                        function: Arc::clone(function),
                    },
                )));
            }
        }
    }
    Ok(PlannerResult::Original(expr))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct PercentileWindow {
    pub function: Arc<AggregateUDF>,
}
impl WindowUDFImpl for PercentileWindow {
    fn name(&self) -> &str {
        self.function.name()
    }
    fn signature(&self) -> &Signature {
        self.function.signature()
    }
    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        self.function.coerce_types(types)
    }
    fn field(&self, args: WindowUDFFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(
            self.function
                .return_field(args.input_fields())?
                .as_ref()
                .clone()
                .with_name(args.name()),
        ))
    }
    fn partition_evaluator(
        &self,
        args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        let schema = Schema::empty();
        let field = self.function.return_field(args.input_fields())?;
        let inner = make_accumulator(AccumulatorArgs {
            return_field: field,
            schema: &schema,
            ignore_nulls: args.ignore_nulls(),
            order_bys: &[],
            is_reversed: args.is_reversed(),
            name: self.name(),
            is_distinct: false,
            exprs: args.input_exprs(),
            expr_fields: args.input_fields(),
        })?;
        Ok(Box::new(PercentileEvaluator {
            template: inner,
            last: None,
            result: None,
        }))
    }
}
#[derive(Debug)]
struct PercentileEvaluator {
    template: PercentileAccumulator,
    last: Option<Range<usize>>,
    result: Option<ScalarValue>,
}
impl PartitionEvaluator for PercentileEvaluator {
    fn uses_window_frame(&self) -> bool {
        true
    }
    fn evaluate(&mut self, values: &[ArrayRef], range: &Range<usize>) -> Result<ScalarValue> {
        if self.last.as_ref() == Some(range) {
            return Ok(self.result.as_ref().unwrap().clone());
        }
        let mut accumulator = self.template.empty();
        accumulator.update_batch(&[values[0].slice(range.start, range.len())])?;
        let result = accumulator.evaluate()?;
        self.last = Some(range.clone());
        self.result = Some(result.clone());
        Ok(result)
    }
}
