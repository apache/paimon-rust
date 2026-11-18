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

//! Validate WKB assignments while the query still carries the input CRS.

use crate::error::to_datafusion_error;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchema, Result};
use datafusion::logical_expr::expr_rewriter::FunctionRewrite;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, WriteOp};
use datafusion::optimizer::analyzer::AnalyzerRule;

#[derive(Debug)]
pub(crate) struct GeoAssignments;

impl FunctionRewrite for GeoAssignments {
    fn name(&self) -> &str {
        "validate_geoarrow_casts"
    }

    fn rewrite(
        &self,
        expr: Expr,
        schema: &DFSchema,
        _: &ConfigOptions,
    ) -> Result<Transformed<Expr>> {
        let (source, target) = match &expr {
            Expr::Cast(cast) => (&cast.expr, &cast.field),
            Expr::TryCast(cast) => (&cast.expr, &cast.field),
            _ => return Ok(Transformed::no(expr)),
        };
        if paimon::arrow::contains_geoarrow_wkb(target) {
            let (_, field) = source.to_field(schema)?;
            paimon::arrow::validate_geoarrow_write(&field, target).map_err(to_datafusion_error)?;
        }
        Ok(Transformed::no(expr))
    }
}

// SQL INSERT coerces to the destination's physical DataType, which is plain
// Binary for a scalar geo column. Check the corresponding source expression
// before that generated coercion can turn text into invalid WKB.
impl AnalyzerRule for GeoAssignments {
    fn name(&self) -> &str {
        "validate_geoarrow_insert"
    }

    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        let LogicalPlan::Dml(dml) = &plan else {
            return Ok(plan);
        };
        if !matches!(dml.op, WriteOp::Insert(_)) {
            return Ok(plan);
        }
        let LogicalPlan::Projection(projection) = dml.input.as_ref() else {
            return Ok(plan);
        };
        for (expr, target) in projection.expr.iter().zip(dml.target.schema().fields()) {
            if target.extension_type_name() != Some("geoarrow.wkb") {
                continue;
            }
            let expr = match expr {
                Expr::Alias(a) => a.expr.as_ref(),
                expr => expr,
            };
            let expr = before_coercion(expr);
            let (_, actual) = expr.to_field(projection.input.schema())?;
            paimon::arrow::validate_geoarrow_write(&actual, target).map_err(to_datafusion_error)?;
            // For directly inserted VALUES, DataFusion puts the coercion into
            // each row expression instead of the outer projection.
            if let (LogicalPlan::Values(values), Expr::Column(column)) =
                (projection.input.as_ref(), expr)
            {
                let index = values.schema.index_of_column(column)?;
                for row in &values.values {
                    let (_, actual) = before_coercion(&row[index]).to_field(&DFSchema::empty())?;
                    paimon::arrow::validate_geoarrow_write(&actual, target)
                        .map_err(to_datafusion_error)?;
                }
            }
        }
        let LogicalPlan::Values(values) = projection.input.as_ref() else {
            return Ok(plan);
        };
        let Some(input) = project_wkb_list_values(values)? else {
            return Ok(plan);
        };
        let mut projection = projection.clone();
        projection.input = std::sync::Arc::new(input);
        let mut dml = dml.clone();
        dml.input = std::sync::Arc::new(LogicalPlan::Projection(projection));
        Ok(LogicalPlan::Dml(dml))
    }
}

pub(crate) fn before_coercion(expr: &Expr) -> &Expr {
    match expr {
        Expr::Cast(c) if c.field.extension_type_name().is_none() => &c.expr,
        expr => expr,
    }
}

/// DataFusion 55.2 scalarizes each VALUES cell and drops metadata on list
/// elements. Materialize binary lists first, then apply the GeoArrow field
/// once to the whole column without converting the WKB values.
fn project_wkb_list_values(
    values: &datafusion::logical_expr::logical_plan::Values,
) -> Result<Option<LogicalPlan>> {
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::logical_expr::expr::Cast;
    use datafusion::logical_expr::logical_plan::Projection;
    use std::sync::Arc;

    let is_wkb_list = |f: &Field| matches!(f.data_type(), DataType::List(child) if child.extension_type_name() == Some("geoarrow.wkb"));
    if !values
        .schema
        .fields()
        .iter()
        .any(|field| is_wkb_list(field))
    {
        return Ok(None);
    }
    let mut values = values.clone();
    let declared = values.schema.clone();
    let mut fields = declared.fields().to_vec();
    let mut projection = Vec::with_capacity(fields.len());
    for (index, field) in fields.iter_mut().enumerate() {
        let column = Expr::Column(declared.qualified_field(index).into());
        if is_wkb_list(field) {
            let DataType::List(child) = field.data_type() else {
                unreachable!()
            };
            let storage = DataType::List(Arc::new(Field::new(
                child.name(),
                child.data_type().clone(),
                child.is_nullable(),
            )));
            for row in &mut values.values {
                row[index] =
                    std::mem::take(&mut row[index]).cast_to(&storage, &DFSchema::empty())?;
            }
            projection.push(
                Expr::Cast(Cast::new_from_field(Box::new(column), field.clone()))
                    .alias(field.name()),
            );
            *field = Arc::new(field.as_ref().clone().with_data_type(storage));
        } else {
            projection.push(column);
        }
    }
    values.schema = Arc::new(DFSchema::from_unqualified_fields(
        fields.into(),
        declared.metadata().clone(),
    )?);
    Ok(Some(LogicalPlan::Projection(
        Projection::try_new_with_schema(
            projection,
            Arc::new(LogicalPlan::Values(values)),
            declared,
        )?,
    )))
}
