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

use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::context::SQLOptions;
use datafusion::execution::session_state::SessionState;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use datafusion::prelude::DataFrame;
use datafusion::sql::planner::object_name_to_table_reference;
use datafusion::sql::sqlparser::ast::helpers::stmt_create_table::CreateTableBuilder;
use datafusion::sql::sqlparser::ast::{
    CreateTable, CreateTableOptions, Query, Select, SqlOption, Statement, Visit, Visitor,
};
use futures::StreamExt;
use paimon::spec::{CoreOptions, DataType as PaimonDataType, TableType};
use paimon::table::Table;

use super::{
    create_table_schema, extract_table_properties, ok_result, SQLContext, TimeTravelSource,
};
use crate::error::to_datafusion_error;
use crate::physical_plan::cast_batch_to_schema;

impl SQLContext {
    pub(super) async fn handle_create_table_as(
        &self,
        ct: &CreateTable,
        query: Box<Query>,
        partition_keys: Vec<String>,
        state: SessionState,
        options: SQLOptions,
        time_travel_sources: &[TimeTravelSource],
    ) -> DFResult<DataFrame> {
        validate_create_table_as(ct, &query)?;
        Self::ensure_main_branch_base_table(&ct.name, "CREATE TABLE AS SELECT")?;
        let enable_ident_normalization =
            state.config_options().sql_parser.enable_ident_normalization;
        let target = object_name_to_table_reference(ct.name.clone(), enable_ident_normalization)?;
        let (catalog, catalog_name, identifier) =
            self.resolve_table_reference(&target, &state.config_options().catalog)?;

        match catalog.check_table_exists(&identifier).await {
            Ok(()) if ct.if_not_exists => return ok_result(&self.ctx),
            Ok(()) => {
                return Err(to_datafusion_error(paimon::Error::TableAlreadyExist {
                    full_name: identifier.full_name(),
                }));
            }
            Err(paimon::Error::TableNotExist { .. }) => {}
            Err(error) => return Err(to_datafusion_error(error)),
        }

        let table_options = extract_table_properties(&ct.table_options)?;
        let table_type = CoreOptions::new(&table_options)
            .table_type()
            .map_err(to_datafusion_error)?;
        if table_type != TableType::Table {
            return Err(DataFusionError::NotImplemented(format!(
                "CREATE TABLE AS SELECT only supports native Paimon tables, got '{table_type}'"
            )));
        }

        let df =
            Box::pin(self.plan_query(state, Statement::Query(query), options, time_travel_sources))
                .await?;
        let (state, source) = df.into_parts();
        let task_ctx = state.task_ctx();
        // Optimization can narrow nullability beyond the query's declared schema.
        let analyzed =
            state
                .analyzer()
                .execute_and_check(source, state.config_options(), |_, _| {})?;
        validate_lateral_correlations(&analyzed)?;
        let schema = create_table_schema(
            ct,
            partition_keys,
            enable_ident_normalization,
            Some(analyzed.schema().as_arrow()),
            table_options,
        )?;
        if schema.fields().is_empty() {
            return Err(DataFusionError::Plan(
                "CREATE TABLE AS SELECT requires at least one output column".into(),
            ));
        }
        for field in schema.fields() {
            validate_time_precision(field.data_type())?;
        }
        let write_schema = paimon::arrow::build_target_arrow_schema(schema.fields())
            .map_err(to_datafusion_error)?;
        let optimized = state.optimizer().optimize(analyzed, &state, |_, _| {})?;
        let plan = state
            .query_planner()
            .create_physical_plan(&optimized, &state)
            .await?;
        if !has_bounded_output(plan.as_ref()) {
            return Err(DataFusionError::NotImplemented(
                "CREATE TABLE AS SELECT requires a bounded query".into(),
            ));
        }

        match catalog.create_table_exclusive(&identifier, schema).await {
            Ok(()) => {}
            Err(paimon::Error::TableAlreadyExist { .. }) if ct.if_not_exists => {
                return ok_result(&self.ctx);
            }
            Err(error) => return Err(to_datafusion_error(error)),
        }
        let name = format!("{catalog_name}.{}", identifier.full_name());
        let table = catalog.get_table(&identifier).await.map_err(|error| {
            DataFusionError::Context(
                format!("Failed to load newly created table '{name}' for CREATE TABLE AS SELECT"),
                Box::new(to_datafusion_error(error)),
            )
        })?;
        let (mut response, outcome) = tokio::sync::oneshot::channel();
        // Keep the writer alive to clean up or finish an in-flight commit after cancellation.
        tokio::spawn(async move {
            let result = populate_table(table, plan, task_ctx, write_schema, &mut response)
                .await
                .map_err(|error| {
                    DataFusionError::Context(
                        format!(
                            "CREATE TABLE AS SELECT failed for '{name}'. \
                             Inspect the target table before retrying."
                        ),
                        Box::new(error),
                    )
                });
            if let Err(Err(error)) = response.send(result) {
                log::warn!("Cancelled CREATE TABLE AS SELECT for '{name}': {error}");
            }
        });
        outcome.await.map_err(|error| {
            DataFusionError::Execution(format!("CREATE TABLE AS SELECT worker stopped: {error}"))
        })??;
        ok_result(&self.ctx)
    }
}

fn validate_lateral_correlations(plan: &LogicalPlan) -> DFResult<()> {
    // DataFusion can turn a reference to an enclosing query into a local
    // column when it decorrelates an inner LATERAL join.
    plan.apply_with_subqueries(|outer| {
        outer.apply_subqueries(|query| {
            let enclosing_inputs = outer.inputs();
            query.apply_with_subqueries(|node| {
                let LogicalPlan::Join(join) = node else {
                    return Ok(TreeNodeRecursion::Continue);
                };
                let right = match join.right.as_ref() {
                    LogicalPlan::SubqueryAlias(alias) => alias.input.as_ref(),
                    right => right,
                };
                let LogicalPlan::Subquery(lateral) = right else {
                    return Ok(TreeNodeRecursion::Continue);
                };
                let columns = lateral.outer_ref_columns.iter().filter_map(|expr| match expr {
                    Expr::OuterReferenceColumn(_, column) => Some(column),
                    _ => None,
                });
                for column in columns {
                    if !join.left.schema().has_column(column)
                        && enclosing_inputs
                            .iter()
                            .any(|input| input.schema().has_column(column))
                    {
                        return Err(DataFusionError::NotImplemented(format!(
                            "CREATE TABLE AS SELECT cannot materialize a LATERAL source referencing enclosing query column '{column}'"
                        )));
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })
    })?;
    Ok(())
}

fn has_bounded_output(plan: &dyn ExecutionPlan) -> bool {
    if !plan.properties().boundedness.is_unbounded() || plan.fetch().is_some() {
        return true;
    }

    // A pushed-down LIMIT can leave the scan and its ancestors marked unbounded.
    let inputs = plan.children();
    inputs
        .iter()
        .any(|input| input.properties().boundedness.is_unbounded())
        && inputs
            .into_iter()
            .all(|input| has_bounded_output(input.as_ref()))
}

async fn populate_table(
    table: Table,
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
    write_schema: SchemaRef,
    response: &mut tokio::sync::oneshot::Sender<DFResult<()>>,
) -> DFResult<()> {
    let resources = crate::memory::writer_resources(&task_ctx).map_err(to_datafusion_error)?;
    let builder = table.new_write_builder().with_resources(resources);
    let commit = builder.try_new_commit().map_err(to_datafusion_error)?;
    let mut writer = builder.new_write().map_err(to_datafusion_error)?;
    let prepared = async {
        let mut input = execute_stream(plan, task_ctx)?;
        loop {
            let next = tokio::select! {
                biased;
                _ = response.closed() => return Ok(None),
                batch = input.next() => batch,
            };
            let Some(batch) = next else {
                break;
            };
            let batch = cast_batch_to_schema(batch?, &write_schema)?;
            writer
                .write_arrow_batch(&batch)
                .await
                .map_err(to_datafusion_error)?;
        }
        writer
            .prepare_commit()
            .await
            .map(Some)
            .map_err(to_datafusion_error)
    }
    .await;
    writer.close().await;
    let messages = prepared.map_err(|error| {
        DataFusionError::Context("no commit was attempted".into(), Box::new(error))
    })?;
    let Some(messages) = messages else {
        return Ok(());
    };
    if response.is_closed() {
        commit.abort(&messages).await.map_err(to_datafusion_error)?;
        return Ok(());
    }
    // A lost response can follow successful publication, so keep the files on commit errors.
    Box::pin(commit.commit(messages)).await.map_err(|error| {
        DataFusionError::Context(
            "publication may have succeeded".into(),
            Box::new(to_datafusion_error(error)),
        )
    })
}

fn validate_time_precision(data_type: &PaimonDataType) -> DFResult<()> {
    match data_type {
        PaimonDataType::Time(time) if time.precision() > 3 => {
            Err(DataFusionError::NotImplemented(format!(
                "CREATE TABLE AS SELECT cannot preserve TIME({}); Paimon stores TIME with millisecond precision",
                time.precision()
            )))
        }
        PaimonDataType::Array(array) => validate_time_precision(array.element_type()),
        PaimonDataType::Vector(vector) => validate_time_precision(vector.element_type()),
        PaimonDataType::Multiset(multiset) => validate_time_precision(multiset.element_type()),
        PaimonDataType::Map(map) => {
            validate_time_precision(map.key_type())?;
            validate_time_precision(map.value_type())
        }
        PaimonDataType::Row(row) => row
            .fields()
            .iter()
            .try_for_each(|field| validate_time_precision(field.data_type())),
        _ => Ok(()),
    }
}

fn validate_create_table_as(ct: &CreateTable, query: &Query) -> DFResult<()> {
    struct ReadOnlyQuery;
    impl Visitor for ReadOnlyQuery {
        type Break = ();

        fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<()> {
            if matches!(statement, Statement::Query(_)) {
                ControlFlow::Continue(())
            } else {
                ControlFlow::Break(())
            }
        }

        fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<()> {
            if select.into.is_some() {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
    }
    if let ControlFlow::Break(()) = query.visit(&mut ReadOnlyQuery) {
        return Err(DataFusionError::NotImplemented(
            "CREATE TABLE AS SELECT requires a read-only query".into(),
        ));
    }
    if !ct.columns.is_empty() {
        return Err(DataFusionError::NotImplemented(
            "CREATE TABLE AS SELECT with explicit column declarations is not supported".into(),
        ));
    }
    if let CreateTableOptions::Plain(options) = &ct.table_options {
        if let Some(option) = options
            .iter()
            .find(|option| !matches!(option, SqlOption::Comment(_)))
        {
            return Err(DataFusionError::NotImplemented(format!(
                "Unsupported CREATE TABLE AS SELECT option: {option}"
            )));
        }
    }
    // Reject unhandled AST fields, including modifiers added by parser upgrades.
    let mut supported = CreateTableBuilder::new(ct.name.clone())
        .if_not_exists(ct.if_not_exists)
        .table_options(ct.table_options.clone())
        .build();
    supported.comment = ct.comment.clone();
    if ct != &supported {
        return Err(DataFusionError::NotImplemented(
            "Unsupported CREATE TABLE AS SELECT modifiers; only IF NOT EXISTS, PARTITIONED BY, table options and COMMENT are supported".into(),
        ));
    }
    Ok(())
}
