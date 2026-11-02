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

use datafusion::arrow::array::{Array, BinaryArray, BinaryBuilder, BooleanArray, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::planner::TypePlanner;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    visit_expressions_mut, BinaryLength, DataType as SqlType, Expr, FunctionArg, FunctionArgExpr,
    FunctionArguments, Statement,
};
use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
use std::ops::ControlFlow;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct BinaryTypePlanner;
impl TypePlanner for BinaryTypePlanner {
    fn plan_type_field(&self, sql_type: &SqlType) -> Result<Option<FieldRef>> {
        if matches!(sql_type, SqlType::Binary(_) | SqlType::Varbinary(_)) {
            width(sql_type)?;
            Ok(Some(Arc::new(Field::new("", DataType::Binary, true))))
        } else {
            Ok(None)
        }
    }
}
fn width(sql_type: &SqlType) -> Result<Option<(u64, bool)>> {
    let value = match sql_type {
        SqlType::Binary(Some(n)) => Some((*n, true)),
        SqlType::Varbinary(Some(BinaryLength::IntegerLength { length })) => Some((*length, false)),
        _ => None,
    };
    if value.is_some_and(|(n, _)| n == 0 || n > i32::MAX as u64) {
        return Err(DataFusionError::Plan(
            "Binary length must be between 1 and 2147483647".into(),
        ));
    }
    Ok(value)
}
pub(crate) fn rewrite(statement: &mut Statement) -> Result<bool> {
    let mut changed = false;
    let flow = visit_expressions_mut(statement, |node| {
        let Expr::Cast { data_type, .. } = node else {
            return ControlFlow::Continue(());
        };
        let (n, pad) = match width(data_type) {
            Ok(Some(pair)) => pair,
            Ok(None) => return ControlFlow::Continue(()),
            Err(e) => return ControlFlow::Break(e),
        };
        let mut cast = node.clone();
        if let Expr::Cast { data_type, .. } = &mut cast {
            *data_type = SqlType::Bytea;
        }
        let template = format!("__paimon_binary_width(NULL, {n}, {pad})");
        let call = Parser::new(&GenericDialect {})
            .try_with_sql(&template)
            .and_then(|mut p| p.parse_expr());
        let mut call = match call {
            Ok(expr) => expr,
            Err(e) => return ControlFlow::Break(DataFusionError::Plan(e.to_string())),
        };
        if let Expr::Function(function) = &mut call {
            if let FunctionArguments::List(args) = &mut function.args {
                args.args[0] = FunctionArg::Unnamed(FunctionArgExpr::Expr(cast));
            }
        }
        *node = call;
        changed = true;
        ControlFlow::Continue(())
    });
    match flow {
        ControlFlow::Break(e) => Err(e),
        _ => Ok(changed),
    }
}
pub(crate) fn register(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(BinaryWidth {
        signature: Signature::exact(
            vec![DataType::Binary, DataType::Int64, DataType::Boolean],
            Volatility::Immutable,
        ),
    }));
}
#[derive(Debug, PartialEq, Eq, Hash)]
struct BinaryWidth {
    signature: Signature,
}
impl ScalarUDFImpl for BinaryWidth {
    fn name(&self) -> &str {
        "__paimon_binary_width"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let input = arrays[0].as_any().downcast_ref::<BinaryArray>().unwrap();
        let lengths = arrays[1].as_any().downcast_ref::<Int64Array>().unwrap();
        let pads = arrays[2].as_any().downcast_ref::<BooleanArray>().unwrap();
        let mut output = BinaryBuilder::new();
        for i in 0..input.len() {
            if input.is_null(i) || lengths.is_null(i) || pads.is_null(i) {
                output.append_null();
                continue;
            }
            let n = lengths.value(i);
            if n <= 0 || n > i32::MAX as i64 {
                return Err(DataFusionError::Execution("Invalid binary length".into()));
            }
            let input = input.value(i);
            let mut value = input[..input.len().min(n as usize)].to_vec();
            if pads.value(i) {
                value.resize(n as usize, 0);
            }
            output.append_value(value);
        }
        let result = Arc::new(output.finish());
        if args
            .args
            .iter()
            .all(|v| matches!(v, ColumnarValue::Scalar(_)))
        {
            Ok(ColumnarValue::Scalar(
                datafusion::common::ScalarValue::try_from_array(result.as_ref(), 0)?,
            ))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }
}
