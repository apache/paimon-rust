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

//! SQL type planning and GeoArrow metadata for Paimon geospatial fields.

mod bbox_intersects;
pub(crate) use bbox_intersects::BBoxIntersects;

use crate::error::to_datafusion_error;
use datafusion::arrow::datatypes::{DataType as ArrowType, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::{DFSchema, Result};
use datafusion::logical_expr::expr_rewriter::coerce_plan_expr_for_schema;
use datafusion::logical_expr::planner::TypePlanner;
use datafusion::logical_expr::{ExprSchemable, LogicalPlan, WriteOp};
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::sql::sqlparser::ast::{
    visit_expressions_mut, ArrayElemTypeDef, DataType, Expr, VisitMut,
};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct GeoTypePlanner;

impl TypePlanner for GeoTypePlanner {
    fn plan_type_field(&self, sql_type: &DataType) -> Result<Option<FieldRef>> {
        if !contains_sql_type(sql_type, is_geo_type) {
            return crate::vector_type::PaimonTypePlanner.plan_type_field(sql_type);
        }
        if contains_sql_type(sql_type, |ty| {
            matches!(
                ty,
                DataType::Array(ArrayElemTypeDef::SquareBracket(_, Some(_)))
            )
        }) {
            return Err(datafusion::error::DataFusionError::NotImplemented(
                "Fixed-size SQL arrays in geospatial types are not supported; use ARRAY<T>".into(),
            ));
        }
        let ty = crate::sql_context::sql_data_type_to_paimon_type(sql_type, true)?;
        let field =
            paimon::arrow::paimon_type_to_arrow_field("", &ty).map_err(to_datafusion_error)?;
        Ok(Some(Arc::new(field)))
    }
}

fn contains_sql_type(sql_type: &DataType, predicate: fn(&DataType) -> bool) -> bool {
    if predicate(sql_type) {
        return true;
    }
    match sql_type {
        DataType::Array(
            ArrayElemTypeDef::AngleBracket(element)
            | ArrayElemTypeDef::SquareBracket(element, _)
            | ArrayElemTypeDef::Parenthesis(element),
        ) => contains_sql_type(element, predicate),
        DataType::Map(key, value) => {
            contains_sql_type(key, predicate) || contains_sql_type(value, predicate)
        }
        DataType::Struct(fields, _) => fields
            .iter()
            .any(|field| contains_sql_type(&field.field_type, predicate)),
        _ => false,
    }
}

fn is_geo_type(ty: &DataType) -> bool {
    let DataType::Custom(name, _) = ty else {
        return false;
    };
    let [part] = name.0.as_slice() else {
        return false;
    };
    part.as_ident().is_some_and(|ident| {
        ident.quote_style.is_none()
            && (ident.value.eq_ignore_ascii_case("GEOMETRY")
                || ident.value.eq_ignore_ascii_case("GEOGRAPHY"))
    })
}

// sqlparser represents custom type modifiers as bare strings and formats them
// verbatim. Restore quoting in CASTs before storing or re-planning their SQL.
fn quote_geo_cast_modifiers(ty: &mut DataType) {
    if is_geo_type(ty) {
        if let DataType::Custom(_, args) = ty {
            for arg in args {
                *arg = format!("'{}'", arg.replace('\'', "''"));
            }
        }
        return;
    }
    match ty {
        DataType::Array(
            ArrayElemTypeDef::AngleBracket(t)
            | ArrayElemTypeDef::Parenthesis(t)
            | ArrayElemTypeDef::SquareBracket(t, _),
        ) => quote_geo_cast_modifiers(t),
        DataType::Map(k, v) => {
            quote_geo_cast_modifiers(k);
            quote_geo_cast_modifiers(v);
        }
        DataType::Struct(fields, _) => {
            for f in fields {
                quote_geo_cast_modifiers(&mut f.field_type);
            }
        }
        _ => {}
    }
}

/// Preserve CRS quoting when SQL ASTs are formatted for storage or reparsing.
pub(crate) fn quote_cast_types(ast: &mut impl VisitMut) {
    let _ = visit_expressions_mut(ast, |expr| {
        if let Expr::Cast { data_type, .. } = expr {
            quote_geo_cast_modifiers(data_type);
        }
        std::ops::ControlFlow::<()>::Continue(())
    });
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
                let ArrowType::List(child) = field.data_type() else {
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
            let storage = ArrowType::List(Arc::new(
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
