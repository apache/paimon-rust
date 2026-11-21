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

//! Exact, inclusive XY envelope intersection for a planar GeoArrow WKB field.
use datafusion::arrow::array::{Array, ArrayRef, BinaryArray, BooleanBuilder, Float64Array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use paimon::rtree::BoundingBox;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct BBoxIntersects(Signature);

impl Default for BBoxIntersects {
    fn default() -> Self {
        Self(Signature::exact(
            vec![
                DataType::Binary,
                DataType::Float64,
                DataType::Float64,
                DataType::Float64,
                DataType::Float64,
            ],
            Volatility::Immutable,
        ))
    }
}

impl ScalarUDFImpl for BBoxIntersects {
    fn name(&self) -> &str {
        "bbox_intersects"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if !matches!(
            paimon::arrow::arrow_field_to_paimon_type(&args.arg_fields[0]),
            Ok(paimon::spec::DataType::Geometry(_))
        ) {
            return Err(DataFusionError::Plan(
                "bbox_intersects requires a GEOMETRY column".into(),
            ));
        }
        Ok(Arc::new(Field::new(self.name(), DataType::Boolean, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let values = ColumnarValue::values_to_arrays(&args.args)?;
        let geom = values[0].as_any().downcast_ref::<BinaryArray>().unwrap();
        let coords: Vec<_> = values[1..]
            .iter()
            .map(|a| a.as_any().downcast_ref::<Float64Array>().unwrap())
            .collect();
        let mut out = BooleanBuilder::with_capacity(geom.len());
        for (i, value) in geom.iter().enumerate() {
            let Some(wkb) = value.filter(|_| coords.iter().all(|c| !c.is_null(i))) else {
                out.append_null();
                continue;
            };
            let result = BoundingBox::new(
                coords[0].value(i),
                coords[1].value(i),
                coords[2].value(i),
                coords[3].value(i),
            )
            .and_then(|query| BoundingBox::from_wkb(wkb).map(|b| b.intersects(&query)))
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
            out.append_value(result);
        }
        let array: ArrayRef = Arc::new(out.finish());
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &array, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(array))
        }
    }
}
