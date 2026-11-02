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

use datafusion::arrow::array::{
    Array, ArrayRef, Int64Array, ListBuilder, StringArray, StringBuilder,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion::prelude::SessionContext;
use regex::Regex;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) fn register_compat_functions(ctx: &SessionContext) {
    for (name, alias) in [
        ("current_date", "curdate"),
        ("to_date", "date"),
        ("to_timestamp_millis", "timestamp_millis"),
        ("to_timestamp", "to_timestamp_ltz"),
        ("base64", "to_base64"),
    ] {
        ctx.register_udf(ScalarUDF::from(ConfiguredAlias {
            name: alias,
            inner: Arc::clone(&ctx.state().scalar_functions()[name]),
        }));
    }
    for kind in [Kind::RegexpExtract, Kind::RegexpExtractAll] {
        ctx.register_udf(ScalarUDF::from(CompatFunction {
            kind,
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Int64]),
                ],
                Volatility::Immutable,
            ),
        }));
    }
}

// DataFusion 54's with_aliases wrapper does not forward configuration updates.
// Keep the native function registered separately and forward timezone changes
// and simplification to the alias's own native implementation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ConfiguredAlias {
    name: &'static str,
    inner: Arc<ScalarUDF>,
}

impl ScalarUDFImpl for ConfiguredAlias {
    fn name(&self) -> &str {
        self.name
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
        self.inner.inner().with_updated_config(config).map(|inner| {
            ScalarUDF::from(Self {
                name: self.name,
                inner: Arc::new(inner),
            })
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Kind {
    RegexpExtract,
    RegexpExtractAll,
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CompatFunction {
    kind: Kind,
    signature: Signature,
}
impl ScalarUDFImpl for CompatFunction {
    fn name(&self) -> &str {
        match self.kind {
            Kind::RegexpExtract => "regexp_extract",
            Kind::RegexpExtractAll => "regexp_extract_all",
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(if self.kind == Kind::RegexpExtractAll {
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
        } else {
            DataType::Utf8
        })
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let output = extract_regex(&arrays, self.kind == Kind::RegexpExtractAll)?;
        if args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)))
        {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &output, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(output))
        }
    }
}

fn extract_regex(arrays: &[ArrayRef], all: bool) -> Result<ArrayRef> {
    let text = cast(&arrays[0], &DataType::Utf8)?;
    let pattern = cast(&arrays[1], &DataType::Utf8)?;
    let text = text.as_any().downcast_ref::<StringArray>().unwrap();
    let pattern = pattern.as_any().downcast_ref::<StringArray>().unwrap();
    let index = arrays
        .get(2)
        .map(|array| array.as_any().downcast_ref::<Int64Array>().unwrap());
    let mut cache = HashMap::<&str, Regex>::new();
    let mut strings = StringBuilder::new();
    let mut lists = ListBuilder::new(StringBuilder::new());
    for row in 0..text.len() {
        if arrays.iter().any(|array| array.is_null(row)) {
            strings.append_null();
            lists.append(false);
            continue;
        }
        let group = usize::try_from(index.map(|index| index.value(row)).unwrap_or(1))
            .map_err(|_| error("regexp_extract group index must be non-negative"))?;
        let regex = match cache.entry(pattern.value(row)) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let regex = Regex::new(entry.key())
                    .map_err(|e| error(format!("Invalid regular expression: {e}")))?;
                entry.insert(regex)
            }
        };
        if group >= regex.captures_len() {
            return Err(error(format!(
                "regexp_extract group index {group} exceeds {}",
                regex.captures_len() - 1
            )));
        }
        if all {
            for capture in regex.captures_iter(text.value(row)) {
                lists
                    .values()
                    .append_value(capture.get(group).map(|m| m.as_str()).unwrap_or(""));
            }
            lists.append(true);
        } else {
            let capture = regex.captures(text.value(row));
            strings.append_value(
                capture
                    .as_ref()
                    .and_then(|capture| capture.get(group))
                    .map(|m| m.as_str())
                    .unwrap_or(""),
            );
        }
    }
    Ok(if all {
        Arc::new(lists.finish())
    } else {
        Arc::new(strings.finish())
    })
}

fn error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}
