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

//! Preserve GeoArrow field metadata at DataFusion write boundaries.

use crate::error::to_datafusion_error;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::config::ConfigOptions;
use datafusion::common::{DFSchema, Result};
use datafusion::logical_expr::expr_rewriter::coerce_plan_expr_for_schema;
use datafusion::logical_expr::{ExprSchemable, LogicalPlan, WriteOp};
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::physical_expr::{expressions::CastExpr, PhysicalExpr};
use std::sync::Arc;

/// An implicit cast can change WKB offset widths or nested element fields.
/// Check the input before that conversion strips its CRS.
pub(crate) fn check_wkb_assignment(
    expr: &Arc<dyn PhysicalExpr>,
    schema: &Schema,
    target: &Field,
) -> Result<()> {
    if !paimon::arrow::contains_geoarrow_wkb(target) {
        return Ok(());
    }
    let field = match expr.downcast_ref::<CastExpr>() {
        Some(cast)
            if !cast.has_explicit_metadata()
                && cast.expr().data_type(schema)? != *cast.cast_type() =>
        {
            cast.expr().return_field(schema)?
        }
        _ => expr.return_field(schema)?,
    };
    paimon::arrow::validate_geoarrow_write(&field, target).map_err(to_datafusion_error)
}

#[derive(Debug)]
pub(crate) struct GeoListValues;

// DataFusion 55.2 drops list-element metadata when it materializes VALUES
// cells as scalars. Build those WKB lists as binary, then restore their type
// once for the column. Other INSERT inputs retain their normal plan.
impl AnalyzerRule for GeoListValues {
    fn name(&self) -> &str {
        "geoarrow_list_values"
    }

    fn analyze(&self, mut plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        let LogicalPlan::Dml(dml) = &mut plan else {
            return Ok(plan);
        };
        if !matches!(dml.op, WriteOp::Insert(_)) {
            return Ok(plan);
        }
        let LogicalPlan::Projection(projection) = dml.input.as_ref() else {
            return Ok(plan);
        };
        let LogicalPlan::Values(values) = projection.input.as_ref() else {
            return Ok(plan);
        };
        let declared = &values.schema;
        let wkb_lists = declared
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(i, field)| {
                let DataType::List(child) = field.data_type() else {
                    return None;
                };
                (child.extension_type_name() == Some("geoarrow.wkb")).then_some((i, child))
            });
        let mut lists = wkb_lists.peekable();
        if lists.peek().is_none() {
            return Ok(plan);
        }
        let mut values = values.clone();
        let mut fields = declared.fields().to_vec();
        for (i, child) in lists {
            let storage = DataType::List(Arc::new(
                child.as_ref().clone().with_metadata(Default::default()),
            ));
            for row in &mut values.values {
                row[i] = std::mem::take(&mut row[i]).cast_to(&storage, &DFSchema::empty())?;
            }
            fields[i] = Arc::new(fields[i].as_ref().clone().with_data_type(storage));
        }
        values.schema = Arc::new(DFSchema::from_unqualified_fields(
            fields.into(),
            declared.metadata().clone(),
        )?);
        let restored = coerce_plan_expr_for_schema(LogicalPlan::Values(values), declared)?;
        let mut projection = projection.clone();
        projection.input = Arc::new(restored);
        dml.input = Arc::new(LogicalPlan::Projection(projection));
        Ok(plan)
    }
}
