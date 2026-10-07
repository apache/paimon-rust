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

//! DataFusion conditions for the engine-independent core MERGE operation.

use datafusion::arrow::array::{BooleanArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::prelude::SessionContext;
use paimon::table::MergeCondition;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

fn invalid(error: impl std::fmt::Display) -> paimon::Error {
    paimon::Error::DataInvalid {
        message: format!("Invalid MERGE condition: {error}"),
        source: None,
    }
}

const ROW_INDEX: &str = "__merge_condition_row";

// Full SQL planning handles scalar/IN subqueries. Preserve row identity because
// their join rewrites may reorder the input. Core consumes a mask in input order.
async fn evaluate_query(sql: &str, batch: RecordBatch) -> paimon::Result<BooleanArray> {
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Arc::new(Field::new(ROW_INDEX, DataType::UInt64, false)));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(UInt64Array::from_iter_values(
        0..batch.num_rows() as u64,
    )));
    let input = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(invalid)?;
    let context = SessionContext::new();
    context
        .register_batch("_merge_batch", input)
        .map_err(invalid)?;
    let output = context
        .sql(&format!("SELECT {ROW_INDEX} FROM _merge_batch WHERE {sql}"))
        .await
        .map_err(invalid)?
        .collect()
        .await
        .map_err(invalid)?;
    let mut mask = vec![false; batch.num_rows()];
    for output in output {
        let indices = output
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("Missing condition row index"))?;
        for row in 0..output.num_rows() {
            let index = indices.value(row) as usize;
            if index >= mask.len() || mask[index] {
                return Err(invalid("Condition changed input row cardinality"));
            }
            mask[index] = true;
        }
    }
    Ok(BooleanArray::from(mask))
}

/// Compile a SQL condition over `t.<column>` and `s.<column>` Arrow fields.
/// Target and source dependencies include correlated subquery references.
/// Core controls matching and action order; this adapter only evaluates SQL.
pub async fn compile_merge_condition(
    sql: String,
    schema: SchemaRef,
) -> paimon::Result<MergeCondition> {
    let context = SessionContext::new();
    context
        .register_batch("_merge_batch", RecordBatch::new_empty(schema))
        .map_err(invalid)?;
    let frame = context
        .sql(&format!("SELECT * FROM _merge_batch WHERE {sql}"))
        .await
        .map_err(invalid)?;
    let plan = frame.into_unoptimized_plan();
    let LogicalPlan::Projection(projection) = plan else {
        return Err(invalid("Expected a condition expression"));
    };
    let LogicalPlan::Filter(filter) = projection.input.as_ref() else {
        return Err(invalid("Expected a condition expression"));
    };
    let mut target_columns = BTreeSet::new();
    let mut source_columns = BTreeSet::new();
    // Start at the filter so SELECT * does not turn every field into a
    // dependency. Subquery outer references are not in Expr::column_refs().
    LogicalPlan::Filter(filter.clone())
        .apply_with_subqueries(|plan| {
            for expression in plan.expressions() {
                expression.apply(|expression| {
                    if let Expr::Column(column) | Expr::OuterReferenceColumn(_, column) = expression
                    {
                        if let Some(name) = column.name.strip_prefix("t.") {
                            target_columns.insert(name.to_string());
                        }
                        if let Some(name) = column.name.strip_prefix("s.") {
                            source_columns.insert(name.to_string());
                        }
                    }
                    Ok(TreeNodeRecursion::Continue)
                })?;
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .map_err(invalid)?;
    // Ordinary expressions compile once. Subqueries need the complete planner;
    // validate that plan before matching, then execute against each action batch.
    let compiled = match context
        .state()
        .create_physical_expr(filter.predicate.clone(), filter.input.schema())
    {
        Ok(expression) => Some(expression),
        Err(_) => {
            let plan = context
                .state()
                .optimize(&LogicalPlan::Projection(projection))
                .map_err(invalid)?;
            context
                .state()
                .create_physical_plan(&plan)
                .await
                .map_err(invalid)?;
            None
        }
    };
    let cached = Arc::new(Mutex::new(None::<(SchemaRef, Arc<dyn PhysicalExpr>)>));
    Ok(MergeCondition {
        target_columns: target_columns.into_iter().collect(),
        source_columns: source_columns.into_iter().collect(),
        evaluate: Arc::new(move |batch| {
            let compiled = compiled.clone();
            let cached = cached.clone();
            let sql = sql.clone();
            Box::pin(async move {
                let Some(compiled) = compiled else {
                    return evaluate_query(&sql, batch).await;
                };
                let mut cached = cached.lock().unwrap();
                if cached
                    .as_ref()
                    .is_none_or(|(schema, _)| schema != &batch.schema())
                {
                    let expression = datafusion::physical_expr::utils::reassign_expr_columns(
                        compiled,
                        batch.schema().as_ref(),
                    )
                    .map_err(invalid)?;
                    *cached = Some((batch.schema(), expression));
                }
                let value = cached
                    .as_ref()
                    .unwrap()
                    .1
                    .evaluate(&batch)
                    .map_err(invalid)?
                    .into_array(batch.num_rows())
                    .map_err(invalid)?;
                value
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .cloned()
                    .ok_or_else(|| invalid("Condition must be BOOLEAN"))
            })
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, Int32Array};

    fn batch(names: &[&str], values: &[Vec<Option<i32>>]) -> RecordBatch {
        RecordBatch::try_from_iter(names.iter().zip(values).map(|(name, values)| {
            (
                *name,
                Arc::new(Int32Array::from(values.clone())) as ArrayRef,
            )
        }))
        .unwrap()
    }

    // Runs on a current-thread runtime: condition evaluation must await its
    // plan rather than synchronously blocking on another Tokio runtime.
    #[tokio::test]
    async fn projected_and_reordered_conditions_preserve_nulls() {
        let input = batch(
            &["t.id", "t.value", "s.value"],
            &[
                vec![Some(1), Some(2)],
                vec![Some(10), Some(20)],
                vec![Some(11), None],
            ],
        );
        let condition = compile_merge_condition("\"s.value\" > \"t.value\"".into(), input.schema())
            .await
            .unwrap();
        let projected = input.project(&[2, 1]).unwrap();
        let result = (condition.evaluate)(projected).await.unwrap();
        assert_eq!(result.iter().collect::<Vec<_>>(), vec![Some(true), None]);
        assert_eq!(condition.source_columns, vec!["value"]);
    }

    #[tokio::test]
    async fn scalar_and_in_subqueries_preserve_input_order() {
        let input = batch(&["s.value"], &[vec![Some(3), None, Some(1), Some(2)]]);
        for sql in [
            "\"s.value\" > (SELECT 1)",
            "\"s.value\" IN (SELECT v FROM (VALUES (2), (3)) AS m(v))",
            "\"s.value\" > (SELECT MAX(v) FROM (VALUES (0), (1)) AS m(v))",
        ] {
            let condition = compile_merge_condition(sql.into(), input.schema())
                .await
                .unwrap();
            let result = (condition.evaluate)(input.clone()).await.unwrap();
            assert_eq!(
                result.iter().collect::<Vec<_>>(),
                vec![Some(true), Some(false), Some(false), Some(true)],
                "{sql}"
            );
        }
    }
    #[tokio::test]
    async fn self_conditions_collect_correlated_subquery_dependencies() {
        let input = batch(
            &["s.id", "t.value", "s.unused"],
            &[
                vec![Some(1), Some(2)],
                vec![Some(10), Some(20)],
                vec![Some(3), Some(4)],
            ],
        );
        let condition = compile_merge_condition(
            "EXISTS (SELECT 1 WHERE \"s.id\" = 1 AND \"t.value\" > 5)".into(),
            input.schema(),
        )
        .await
        .unwrap();
        assert_eq!(condition.source_columns, vec!["id"]);
        assert_eq!(condition.target_columns, vec!["value"]);
        let result = (condition.evaluate)(input.project(&[1, 0]).unwrap())
            .await
            .unwrap();
        assert_eq!(
            result.iter().collect::<Vec<_>>(),
            vec![Some(true), Some(false)]
        );
    }
}
