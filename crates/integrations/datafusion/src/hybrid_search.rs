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

//! `hybrid_search` table-valued function for DataFusion.
//!
//! Spark-compatible shape:
//! ```sql
//! SELECT * FROM hybrid_search(
//!   'table_name',
//!   array(named_struct('field', 'embedding', 'query_vector', array(1.0, 0.0))),
//!   array(named_struct('column', 'content', 'query', 'paimon')),
//!   10,
//!   'rrf')
//! ```

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field, Schema, SchemaRef as ArrowSchemaRef,
};
use datafusion::catalog::{Session, TableFunctionImpl};
use datafusion::common::{project_schema, ScalarValue};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use paimon::catalog::Catalog;
use paimon::spec::{
    BigIntType, CoreOptions, DataField, DataType, ROW_ID_FIELD_ID, ROW_ID_FIELD_NAME,
};
use paimon::table::{HybridSearchRanker, HybridSearchRoute, Table};

use crate::error::to_datafusion_error;
use crate::physical_plan::{SearchScoreExec, SearchScoreOutputColumn};
use crate::runtime::{await_with_runtime, block_on_with_runtime};
use crate::table::{datafusion_read_fields, PaimonScanBuilder, PaimonTableProvider};
use crate::table_function_args::{
    extract_int_literal, extract_string_literal, parse_table_identifier,
};
use crate::table_loader::load_data_table_for_read;

const FUNCTION_NAME: &str = "hybrid_search";
const SEARCH_SCORE_COLUMN: &str = "__paimon_search_score";

pub fn register_hybrid_search(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    default_database: &str,
) {
    ctx.register_udf(
        datafusion::functions_nested::make_array::make_array_udf()
            .as_ref()
            .clone()
            .with_aliases(["array"]),
    );
    ctx.register_udtf(
        FUNCTION_NAME,
        Arc::new(HybridSearchFunction::new(catalog, default_database)),
    );
}

pub struct HybridSearchFunction {
    catalog: Arc<dyn Catalog>,
    default_database: String,
}

impl Debug for HybridSearchFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HybridSearchFunction")
            .field("default_database", &self.default_database)
            .finish()
    }
}

impl HybridSearchFunction {
    pub fn new(catalog: Arc<dyn Catalog>, default_database: &str) -> Self {
        Self {
            catalog,
            default_database: default_database.to_string(),
        }
    }
}

impl TableFunctionImpl for HybridSearchFunction {
    fn call(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if args.len() != 4 && args.len() != 5 {
            return Err(DataFusionError::Plan(
                "hybrid_search requires 4 or 5 arguments: (table_name, vector_routes, full_text_routes, limit[, ranker])".to_string(),
            ));
        }

        let table_name = extract_string_literal(FUNCTION_NAME, &args[0], "table_name")?;
        let limit = extract_int_literal(FUNCTION_NAME, &args[3], "limit")?;
        if limit <= 0 {
            return Err(DataFusionError::Plan(
                "hybrid_search: limit must be positive".to_string(),
            ));
        }

        let ranker = if args.len() == 5 {
            extract_string_literal(FUNCTION_NAME, &args[4], "ranker")?
        } else {
            HybridSearchRanker::RRF.to_string()
        };
        HybridSearchRanker::parse(&ranker).map_err(to_datafusion_error)?;

        let mut routes = parse_vector_routes(&args[1], limit as usize)?;
        routes.extend(parse_full_text_routes(&args[2], limit as usize)?);

        let identifier =
            parse_table_identifier(FUNCTION_NAME, &table_name, &self.default_database)?;
        let catalog = Arc::clone(&self.catalog);
        let table = block_on_with_runtime(
            async move { load_data_table_for_read(&catalog, &identifier, FUNCTION_NAME).await },
            "hybrid_search: catalog access thread panicked",
        )?;

        Ok(Arc::new(HybridSearchTableProvider::try_new(
            PaimonTableProvider::try_new(table)?,
            routes,
            limit as usize,
            ranker,
        )?))
    }
}

#[derive(Debug)]
struct HybridSearchTableProvider {
    inner: PaimonTableProvider,
    schema: ArrowSchemaRef,
    routes: Vec<HybridSearchRoute>,
    limit: usize,
    ranker: String,
}

impl HybridSearchTableProvider {
    fn try_new(
        inner: PaimonTableProvider,
        routes: Vec<HybridSearchRoute>,
        limit: usize,
        ranker: String,
    ) -> DFResult<Self> {
        let inner_schema = inner.schema();
        if inner_schema
            .fields()
            .iter()
            .any(|field| field.name() == SEARCH_SCORE_COLUMN)
        {
            return Err(DataFusionError::Plan(format!(
                "hybrid_search: table already contains reserved column {SEARCH_SCORE_COLUMN}"
            )));
        }

        let mut fields = inner_schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        fields.push(Field::new(
            SEARCH_SCORE_COLUMN,
            ArrowDataType::Float32,
            true,
        ));
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            inner_schema.metadata().clone(),
        ));

        Ok(Self {
            inner,
            schema,
            routes,
            limit,
            ranker,
        })
    }
}

#[async_trait]
impl TableProvider for HybridSearchTableProvider {
    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let table = self.inner.table();

        let search_result = await_with_runtime(async {
            let mut builder = table.new_hybrid_search_builder();
            for route in self.routes.clone() {
                builder.add_route(route);
            }
            builder
                .with_limit(self.limit)
                .with_ranker(&self.ranker)
                .map_err(to_datafusion_error)?;
            builder.execute_scored().await.map_err(to_datafusion_error)
        })
        .await?;

        let row_ranges = search_result.to_row_ranges().map_err(to_datafusion_error)?;

        if row_ranges.is_empty() {
            let schema = project_schema(&self.schema, projection)?;
            return Ok(Arc::new(EmptyExec::new(schema)));
        }

        // Raw row-range scans can include non-matching rows from selected files,
        // so the outer limit must remain above the row-ID filter.
        let scan = table
            .new_read_builder()
            .new_scan()
            .with_row_ranges(row_ranges);
        let plan = await_with_runtime(scan.plan())
            .await
            .map_err(to_datafusion_error)?;

        let inner_schema = self.inner.schema();
        let score_index = inner_schema.fields().len();
        let input_read_fields = search_read_fields(table)?;
        let input_schema = paimon::arrow::build_target_arrow_schema(&input_read_fields)
            .map_err(to_datafusion_error)?;
        let row_id_table_index = input_read_fields
            .iter()
            .position(|field| field.name() == ROW_ID_FIELD_NAME)
            .expect("search read fields contain _ROW_ID");
        let projected_indices = projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.fields().len()).collect());
        let mut input_projection = projected_indices
            .iter()
            .copied()
            .filter(|index| *index != score_index)
            .collect::<Vec<_>>();
        if !input_projection.contains(&row_id_table_index) {
            input_projection.push(row_id_table_index);
        }
        let row_id_input_index = input_projection
            .iter()
            .position(|index| *index == row_id_table_index)
            .expect("row ID was added to the input projection");
        let output_columns = projected_indices
            .iter()
            .map(|index| {
                if *index == score_index {
                    SearchScoreOutputColumn::Score
                } else {
                    let input_index = input_projection
                        .iter()
                        .position(|input_index| input_index == index)
                        .expect("projected table column exists in the input projection");
                    SearchScoreOutputColumn::Input(input_index)
                }
            })
            .collect();
        let output_schema = project_schema(&self.schema, projection)?;
        let input = PaimonScanBuilder {
            table,
            schema: &input_schema,
            plan: &plan,
            scan_trace: None,
            projection: Some(&input_projection),
            pushed_predicate: None,
            limit: None,
            target_partitions: state.config_options().execution.target_partitions,
            filter_exact: false,
            case_sensitive: true,
        }
        .build_with_read_fields(input_read_fields)?;
        let scores = search_result
            .row_ids
            .into_iter()
            .zip(search_result.scores)
            .collect();
        Ok(Arc::new(SearchScoreExec::new(
            input,
            output_schema,
            row_id_input_index,
            output_columns,
            Arc::new(scores),
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }
}

fn search_read_fields(table: &Table) -> DFResult<Vec<DataField>> {
    let mut fields = datafusion_read_fields(table);
    if fields.iter().any(|field| field.name() == ROW_ID_FIELD_NAME) {
        return Ok(fields);
    }
    if !CoreOptions::new(table.schema().options()).row_tracking_enabled() {
        return Err(DataFusionError::Plan(
            "hybrid_search: cannot materialize search results because _ROW_ID is not available"
                .to_string(),
        ));
    }
    fields.push(DataField::new(
        ROW_ID_FIELD_ID,
        ROW_ID_FIELD_NAME.to_string(),
        DataType::BigInt(BigIntType::with_nullable(true)),
    ));
    Ok(fields)
}

fn parse_vector_routes(expr: &Expr, default_limit: usize) -> DFResult<Vec<HybridSearchRoute>> {
    if let Some(routes) = extract_literal_array_values(expr, "vector_routes")? {
        return routes
            .iter()
            .map(|route| parse_vector_route_scalar(route, default_limit))
            .collect();
    }

    extract_array_elements(expr, "vector_routes")?
        .into_iter()
        .map(|route| parse_vector_route(route, default_limit))
        .collect()
}

fn parse_full_text_routes(expr: &Expr, default_limit: usize) -> DFResult<Vec<HybridSearchRoute>> {
    if let Some(routes) = extract_literal_array_values(expr, "full_text_routes")? {
        return routes
            .iter()
            .map(|route| parse_full_text_route_scalar(route, default_limit))
            .collect();
    }

    extract_array_elements(expr, "full_text_routes")?
        .into_iter()
        .map(|route| parse_full_text_route(route, default_limit))
        .collect()
}

fn parse_vector_route(expr: &Expr, default_limit: usize) -> DFResult<HybridSearchRoute> {
    let fields = extract_named_struct_fields(expr, "vector route")?;
    let field_name = optional_field(&fields, &["field", "vector_column"])
        .ok_or_else(|| {
            DataFusionError::Plan(
                "hybrid_search: vector route must define field or vector_column".to_string(),
            )
        })
        .and_then(|expr| extract_string_literal(FUNCTION_NAME, expr, "vector route field"))?;
    let vector = required_field(&fields, "query_vector")
        .and_then(|expr| extract_float_array(expr, "query_vector"))?;
    let limit = optional_field(&fields, &["limit"])
        .map(|expr| extract_positive_usize(expr, "vector route limit"))
        .transpose()?
        .unwrap_or(default_limit);
    let weight = optional_field(&fields, &["weight"])
        .map(|expr| extract_positive_f32(expr, "weight"))
        .transpose()?
        .unwrap_or(1.0);
    let options = optional_field(&fields, &["options"])
        .map(extract_options)
        .transpose()?
        .unwrap_or_default();

    HybridSearchRoute::vector(field_name, vector, limit, weight, options)
        .map_err(to_datafusion_error)
}

fn parse_vector_route_scalar(
    scalar: &ScalarValue,
    default_limit: usize,
) -> DFResult<HybridSearchRoute> {
    let fields = extract_struct_scalar_fields(scalar, "vector route")?;
    let field_name = optional_scalar_field(&fields, &["field", "vector_column"])
        .ok_or_else(|| {
            DataFusionError::Plan(
                "hybrid_search: vector route must define field or vector_column".to_string(),
            )
        })
        .and_then(|scalar| scalar_to_string(scalar, "vector route field"))?;
    let vector = required_scalar_field(&fields, "query_vector")
        .and_then(|scalar| scalar_to_float_array(scalar, "query_vector"))?;
    let limit = optional_scalar_field(&fields, &["limit"])
        .map(|scalar| scalar_to_positive_usize(scalar, "vector route limit"))
        .transpose()?
        .unwrap_or(default_limit);
    let weight = optional_scalar_field(&fields, &["weight"])
        .map(|scalar| scalar_to_positive_f32(scalar, "weight"))
        .transpose()?
        .unwrap_or(1.0);
    let options = optional_scalar_field(&fields, &["options"])
        .map(scalar_to_options)
        .transpose()?
        .unwrap_or_default();

    HybridSearchRoute::vector(field_name, vector, limit, weight, options)
        .map_err(to_datafusion_error)
}

fn parse_full_text_route(expr: &Expr, default_limit: usize) -> DFResult<HybridSearchRoute> {
    let fields = extract_named_struct_fields(expr, "full-text route")?;
    let column = required_field(&fields, "column")
        .and_then(|expr| extract_string_literal(FUNCTION_NAME, expr, "full-text route column"))?;
    let query = required_field(&fields, "query")
        .and_then(|expr| extract_string_literal(FUNCTION_NAME, expr, "full-text route query"))?;
    let limit = optional_field(&fields, &["limit"])
        .map(|expr| extract_positive_usize(expr, "full-text route limit"))
        .transpose()?
        .unwrap_or(default_limit);
    let weight = optional_field(&fields, &["weight"])
        .map(|expr| extract_positive_f32(expr, "weight"))
        .transpose()?
        .unwrap_or(1.0);
    let options = optional_field(&fields, &["options"])
        .map(extract_options)
        .transpose()?
        .unwrap_or_default();

    HybridSearchRoute::full_text(column, query, limit, weight, options).map_err(to_datafusion_error)
}

fn parse_full_text_route_scalar(
    scalar: &ScalarValue,
    default_limit: usize,
) -> DFResult<HybridSearchRoute> {
    let fields = extract_struct_scalar_fields(scalar, "full-text route")?;
    let column = required_scalar_field(&fields, "column")
        .and_then(|scalar| scalar_to_string(scalar, "full-text route column"))?;
    let query = required_scalar_field(&fields, "query")
        .and_then(|scalar| scalar_to_string(scalar, "full-text route query"))?;
    let limit = optional_scalar_field(&fields, &["limit"])
        .map(|scalar| scalar_to_positive_usize(scalar, "full-text route limit"))
        .transpose()?
        .unwrap_or(default_limit);
    let weight = optional_scalar_field(&fields, &["weight"])
        .map(|scalar| scalar_to_positive_f32(scalar, "weight"))
        .transpose()?
        .unwrap_or(1.0);
    let options = optional_scalar_field(&fields, &["options"])
        .map(scalar_to_options)
        .transpose()?
        .unwrap_or_default();

    HybridSearchRoute::full_text(column, query, limit, weight, options).map_err(to_datafusion_error)
}

fn extract_array_elements<'a>(expr: &'a Expr, name: &str) -> DFResult<Vec<&'a Expr>> {
    match expr {
        Expr::ScalarFunction(function)
            if is_function(function.name(), &["make_array", "array"]) =>
        {
            Ok(function.args.iter().collect())
        }
        _ => Err(DataFusionError::Plan(format!(
            "hybrid_search: {name} must be array(...), got: {expr}"
        ))),
    }
}

fn extract_literal_array_values(expr: &Expr, name: &str) -> DFResult<Option<Vec<ScalarValue>>> {
    let Expr::Literal(scalar, _) = expr else {
        return Ok(None);
    };
    scalar_array_values(scalar, name).map(Some)
}

fn scalar_array_values(scalar: &ScalarValue, name: &str) -> DFResult<Vec<ScalarValue>> {
    let values = match scalar {
        ScalarValue::List(array) => array.value(0),
        ScalarValue::LargeList(array) => array.value(0),
        ScalarValue::ListView(array) => array.value(0),
        ScalarValue::LargeListView(array) => array.value(0),
        ScalarValue::FixedSizeList(array) => array.value(0),
        _ => {
            return Err(DataFusionError::Plan(format!(
                "hybrid_search: {name} must be an array, got: {scalar}"
            )));
        }
    };

    (0..values.len())
        .map(|index| ScalarValue::try_from_array(values.as_ref(), index))
        .collect()
}

fn extract_named_struct_fields<'a>(
    expr: &'a Expr,
    name: &str,
) -> DFResult<Vec<(String, &'a Expr)>> {
    let Expr::ScalarFunction(function) = expr else {
        return Err(DataFusionError::Plan(format!(
            "hybrid_search: {name} must be named_struct(...), got: {expr}"
        )));
    };
    if !is_function(function.name(), &["named_struct"]) {
        return Err(DataFusionError::Plan(format!(
            "hybrid_search: {name} must be named_struct(...), got: {expr}"
        )));
    }
    if function.args.len() % 2 != 0 {
        return Err(DataFusionError::Plan(format!(
            "hybrid_search: {name} must contain key/value pairs"
        )));
    }

    let mut fields = Vec::with_capacity(function.args.len() / 2);
    for pair in function.args.chunks_exact(2) {
        let key = extract_string_literal(FUNCTION_NAME, &pair[0], "route field name")?;
        fields.push((key, &pair[1]));
    }
    Ok(fields)
}

fn extract_struct_scalar_fields(
    scalar: &ScalarValue,
    name: &str,
) -> DFResult<Vec<(String, ScalarValue)>> {
    let ScalarValue::Struct(array) = scalar else {
        return Err(DataFusionError::Plan(format!(
            "hybrid_search: {name} must be named_struct(...), got: {scalar}"
        )));
    };
    if array.is_null(0) {
        return Err(DataFusionError::Plan(format!(
            "hybrid_search: {name} cannot be null"
        )));
    }

    array
        .fields()
        .iter()
        .zip(array.columns())
        .map(|(field, column)| {
            Ok((
                field.name().clone(),
                ScalarValue::try_from_array(column.as_ref(), 0)?,
            ))
        })
        .collect()
}

fn required_field<'a>(fields: &'a [(String, &'a Expr)], name: &str) -> DFResult<&'a Expr> {
    optional_field(fields, &[name])
        .ok_or_else(|| DataFusionError::Plan(format!("hybrid_search: route must define {name}")))
}

fn optional_field<'a>(fields: &'a [(String, &'a Expr)], names: &[&str]) -> Option<&'a Expr> {
    fields
        .iter()
        .find(|(field_name, _)| names.iter().any(|name| field_name == name))
        .map(|(_, expr)| *expr)
}

fn required_scalar_field<'a>(
    fields: &'a [(String, ScalarValue)],
    name: &str,
) -> DFResult<&'a ScalarValue> {
    optional_scalar_field(fields, &[name])
        .ok_or_else(|| DataFusionError::Plan(format!("hybrid_search: route must define {name}")))
}

fn optional_scalar_field<'a>(
    fields: &'a [(String, ScalarValue)],
    names: &[&str],
) -> Option<&'a ScalarValue> {
    fields
        .iter()
        .find(|(field_name, _)| names.iter().any(|name| field_name == name))
        .map(|(_, scalar)| scalar)
}

fn extract_positive_usize(expr: &Expr, name: &str) -> DFResult<usize> {
    let value = extract_int_literal(FUNCTION_NAME, expr, name)?;
    if value <= 0 {
        return Err(DataFusionError::Plan(format!(
            "hybrid_search: {name} must be positive"
        )));
    }
    Ok(value as usize)
}

fn extract_float_array(expr: &Expr, name: &str) -> DFResult<Vec<f32>> {
    if let Ok(json) = extract_string_literal(FUNCTION_NAME, expr, name) {
        let vector: Vec<f32> = serde_json::from_str(&json).map_err(|e| {
            DataFusionError::Plan(format!(
                "hybrid_search: {name} string must be a JSON array of floats: {e}"
            ))
        })?;
        if vector.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "hybrid_search: {name} cannot be empty"
            )));
        }
        return Ok(vector);
    }

    let elements = extract_array_elements(expr, name)?;
    if elements.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "hybrid_search: {name} cannot be empty"
        )));
    }
    elements
        .into_iter()
        .map(|expr| scalar_to_f32(expr, name))
        .collect()
}

fn extract_positive_f32(expr: &Expr, name: &str) -> DFResult<f32> {
    let value = scalar_to_f32(expr, name)?;
    if !value.is_finite() || value <= 0.0 {
        return Err(DataFusionError::Plan(format!(
            "hybrid_search: {name} must be finite and positive, got: {value}"
        )));
    }
    Ok(value)
}

fn scalar_to_f32(expr: &Expr, name: &str) -> DFResult<f32> {
    let Expr::Literal(scalar, _) = expr else {
        return Err(DataFusionError::Plan(format!(
            "hybrid_search: {name} must be a numeric literal, got: {expr}"
        )));
    };
    match scalar {
        ScalarValue::Float32(Some(value)) => Ok(*value),
        ScalarValue::Float64(Some(value)) => Ok(*value as f32),
        ScalarValue::Int8(Some(value)) => Ok(*value as f32),
        ScalarValue::Int16(Some(value)) => Ok(*value as f32),
        ScalarValue::Int32(Some(value)) => Ok(*value as f32),
        ScalarValue::Int64(Some(value)) => Ok(*value as f32),
        ScalarValue::UInt8(Some(value)) => Ok(*value as f32),
        ScalarValue::UInt16(Some(value)) => Ok(*value as f32),
        ScalarValue::UInt32(Some(value)) => Ok(*value as f32),
        ScalarValue::UInt64(Some(value)) => Ok(*value as f32),
        ScalarValue::Utf8(Some(value)) => value.parse::<f32>().map_err(|e| {
            DataFusionError::Plan(format!(
                "hybrid_search: {name} string must be a float, got '{value}': {e}"
            ))
        }),
        _ => Err(DataFusionError::Plan(format!(
            "hybrid_search: {name} must be a numeric literal, got: {expr}"
        ))),
    }
}

fn scalar_to_string(scalar: &ScalarValue, name: &str) -> DFResult<String> {
    match scalar {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Ok(value.clone()),
        _ => Err(DataFusionError::Plan(format!(
            "hybrid_search: {name} must be a string literal, got: {scalar}"
        ))),
    }
}

fn scalar_to_positive_usize(scalar: &ScalarValue, name: &str) -> DFResult<usize> {
    let value = match scalar {
        ScalarValue::Int8(Some(value)) => *value as i64,
        ScalarValue::Int16(Some(value)) => *value as i64,
        ScalarValue::Int32(Some(value)) => *value as i64,
        ScalarValue::Int64(Some(value)) => *value,
        ScalarValue::UInt8(Some(value)) => *value as i64,
        ScalarValue::UInt16(Some(value)) => *value as i64,
        ScalarValue::UInt32(Some(value)) => *value as i64,
        ScalarValue::UInt64(Some(value)) => i64::try_from(*value).map_err(|_| {
            DataFusionError::Plan(format!("hybrid_search: {name} value exceeds i64 range"))
        })?,
        _ => {
            return Err(DataFusionError::Plan(format!(
                "hybrid_search: {name} must be an integer literal, got: {scalar}"
            )));
        }
    };
    if value <= 0 {
        return Err(DataFusionError::Plan(format!(
            "hybrid_search: {name} must be positive"
        )));
    }
    Ok(value as usize)
}

fn scalar_value_to_f32(scalar: &ScalarValue, name: &str) -> DFResult<f32> {
    match scalar {
        ScalarValue::Float16(Some(value)) => Ok(value.to_f32()),
        ScalarValue::Float32(Some(value)) => Ok(*value),
        ScalarValue::Float64(Some(value)) => Ok(*value as f32),
        ScalarValue::Int8(Some(value)) => Ok(*value as f32),
        ScalarValue::Int16(Some(value)) => Ok(*value as f32),
        ScalarValue::Int32(Some(value)) => Ok(*value as f32),
        ScalarValue::Int64(Some(value)) => Ok(*value as f32),
        ScalarValue::UInt8(Some(value)) => Ok(*value as f32),
        ScalarValue::UInt16(Some(value)) => Ok(*value as f32),
        ScalarValue::UInt32(Some(value)) => Ok(*value as f32),
        ScalarValue::UInt64(Some(value)) => Ok(*value as f32),
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => value.parse::<f32>().map_err(|e| {
            DataFusionError::Plan(format!(
                "hybrid_search: {name} string must be a float, got '{value}': {e}"
            ))
        }),
        _ => Err(DataFusionError::Plan(format!(
            "hybrid_search: {name} must be a numeric literal, got: {scalar}"
        ))),
    }
}

fn scalar_to_positive_f32(scalar: &ScalarValue, name: &str) -> DFResult<f32> {
    let value = scalar_value_to_f32(scalar, name)?;
    if !value.is_finite() || value <= 0.0 {
        return Err(DataFusionError::Plan(format!(
            "hybrid_search: {name} must be finite and positive, got: {value}"
        )));
    }
    Ok(value)
}

fn scalar_to_float_array(scalar: &ScalarValue, name: &str) -> DFResult<Vec<f32>> {
    let values = scalar_array_values(scalar, name)?;
    if values.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "hybrid_search: {name} cannot be empty"
        )));
    }
    values
        .iter()
        .map(|value| scalar_value_to_f32(value, name))
        .collect()
}

fn scalar_to_options(scalar: &ScalarValue) -> DFResult<HashMap<String, String>> {
    if matches!(scalar, ScalarValue::Null) {
        return Ok(HashMap::new());
    }

    if let Ok(json) = scalar_to_string(scalar, "options") {
        if json.trim().is_empty() {
            return Ok(HashMap::new());
        }
        return serde_json::from_str(&json).map_err(|e| {
            DataFusionError::Plan(format!(
                "hybrid_search: options string must be a JSON object: {e}"
            ))
        });
    }

    let ScalarValue::Map(array) = scalar else {
        return Err(DataFusionError::Plan(format!(
            "hybrid_search: options must be map(...), got: {scalar}"
        )));
    };
    if array.is_null(0) {
        return Ok(HashMap::new());
    }

    let entries = array.value(0);
    let keys = entries.column(0);
    let values = entries.column(1);
    (0..entries.len())
        .map(|index| {
            Ok((
                scalar_to_string(
                    &ScalarValue::try_from_array(keys.as_ref(), index)?,
                    "options key",
                )?,
                scalar_to_string(
                    &ScalarValue::try_from_array(values.as_ref(), index)?,
                    "options value",
                )?,
            ))
        })
        .collect()
}

fn extract_options(expr: &Expr) -> DFResult<HashMap<String, String>> {
    if let Ok(json) = extract_string_literal(FUNCTION_NAME, expr, "options") {
        if json.trim().is_empty() {
            return Ok(HashMap::new());
        }
        return serde_json::from_str(&json).map_err(|e| {
            DataFusionError::Plan(format!(
                "hybrid_search: options string must be a JSON object: {e}"
            ))
        });
    }

    let Expr::ScalarFunction(function) = expr else {
        return Err(DataFusionError::Plan(format!(
            "hybrid_search: options must be map(...), got: {expr}"
        )));
    };
    if !is_function(function.name(), &["map", "make_map"]) {
        return Err(DataFusionError::Plan(format!(
            "hybrid_search: options must be map(...), got: {expr}"
        )));
    }
    if function.args.is_empty() {
        return Ok(HashMap::new());
    }

    if function.args.len() == 2
        && is_array_expr(&function.args[0])
        && is_array_expr(&function.args[1])
    {
        let keys = extract_array_elements(&function.args[0], "options keys")?;
        let values = extract_array_elements(&function.args[1], "options values")?;
        if keys.len() != values.len() {
            return Err(DataFusionError::Plan(
                "hybrid_search: options keys and values must have the same length".to_string(),
            ));
        }
        return keys
            .into_iter()
            .zip(values)
            .map(|(key, value)| {
                Ok((
                    extract_string_literal(FUNCTION_NAME, key, "options key")?,
                    extract_string_literal(FUNCTION_NAME, value, "options value")?,
                ))
            })
            .collect();
    }

    if function.args.len() % 2 != 0 {
        return Err(DataFusionError::Plan(
            "hybrid_search: options map must contain key/value pairs".to_string(),
        ));
    }

    function
        .args
        .chunks_exact(2)
        .map(|pair| {
            Ok((
                extract_string_literal(FUNCTION_NAME, &pair[0], "options key")?,
                extract_string_literal(FUNCTION_NAME, &pair[1], "options value")?,
            ))
        })
        .collect()
}

fn is_array_expr(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::ScalarFunction(function) if is_function(function.name(), &["make_array", "array"])
    )
}

fn is_function(actual: &str, expected: &[&str]) -> bool {
    expected
        .iter()
        .any(|expected| actual.eq_ignore_ascii_case(expected))
}
