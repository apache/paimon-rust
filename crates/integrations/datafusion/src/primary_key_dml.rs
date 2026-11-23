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

//! Shared snapshot planning and writes for deduplicate primary-key mutations.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use datafusion::arrow::array::{Int8Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::tree_node::Transformed;
use datafusion::datasource::{provider_as_source, source_as_provider};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::DataFrame;
use futures::StreamExt;

use paimon::spec::{MergeEngine, RowKind, VALUE_KIND_FIELD_NAME};
use paimon::table::Table;

use crate::error::to_datafusion_error;
use crate::merge_into::ok_result;
use crate::physical_plan::sink::to_paimon_batch;
use crate::sql_context::SQLContext;
use crate::table::PaimonTableProvider;

pub(crate) fn validate_table(table: &Table, operation: &str) -> DFResult<()> {
    let options = table.schema().core_options();
    if options.merge_engine().map_err(to_datafusion_error)? != MergeEngine::Deduplicate {
        return Err(DataFusionError::Plan(format!(
            "{operation} on primary-key tables requires merge-engine=deduplicate"
        )));
    }
    // Masked values cannot safely identify the keys or preserve unassigned fields.
    if options.query_auth_enabled() {
        return Err(DataFusionError::NotImplemented(format!(
            "{operation} on query-authorized primary-key tables is not supported"
        )));
    }
    if options.rowkind_field().is_some() {
        return Err(DataFusionError::NotImplemented(format!(
            "{operation} on primary-key tables with rowkind.field is not supported"
        )));
    }
    Ok(())
}

pub(crate) async fn write_rows(
    ctx: &SQLContext,
    table: &Table,
    input: DataFrame,
    kind: RowKind,
    operation: &str,
) -> DFResult<DataFrame> {
    let resources =
        crate::memory::writer_resources(&ctx.ctx().task_ctx()).map_err(to_datafusion_error)?;
    let write_builder = table.new_write_builder().with_resources(resources);
    let commit = write_builder
        .try_new_commit()
        .map_err(to_datafusion_error)?;
    let mut writer = write_builder
        .new_write_for_row_mutation()
        .map_err(to_datafusion_error)?;

    let staged = async {
        let mut stream = input.execute_stream().await?;
        let mut matched_rows = 0;
        while let Some(batch) = stream.next().await {
            let batch = to_paimon_batch(batch?, false)?;
            matched_rows += batch.num_rows() as u64;
            let schema = batch.schema();
            let mut fields = schema.fields().to_vec();
            let mut columns = batch.columns().to_vec();
            fields.push(Arc::new(Field::new(
                VALUE_KIND_FIELD_NAME,
                DataType::Int8,
                false,
            )));
            columns.push(Arc::new(Int8Array::from(vec![
                kind.to_value();
                batch.num_rows()
            ])));
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
                columns,
            )?;
            writer
                .write_arrow_batch(&batch)
                .await
                .map_err(to_datafusion_error)?;
        }
        let messages = writer.prepare_commit().await.map_err(to_datafusion_error)?;
        Ok::<_, DataFusionError>((matched_rows, messages))
    }
    .await;
    // Close unprepared output even on scan failure; prepared files are caller-owned.
    writer.close().await;
    let (matched_rows, messages) = staged?;
    if !messages.is_empty() {
        // Publication may have succeeded even on error. Keep prepared files for recovery.
        commit.commit(messages).await.map_err(|error| {
            let context = format!(
                "Primary-key {operation} commit failed for table '{}' (commit_user={}, matched_rows={matched_rows}); prepared files retained",
                table.identifier(),
                write_builder.commit_user(),
            );
            log::warn!("{context}: {error}");
            to_datafusion_error(error).context(context)
        })?;
    }
    ok_result(ctx.ctx(), matched_rows)
}

pub(crate) async fn plan_input(
    ctx: &SQLContext,
    table: &Table,
    table_ref: &str,
    projection: &str,
    selection: Option<&datafusion::sql::sqlparser::ast::Expr>,
    operation: &str,
) -> DFResult<DataFrame> {
    let snapshot = table
        .resolve_read_snapshot()
        .await
        .map_err(to_datafusion_error)?;
    let where_clause = selection
        .map(|predicate| format!(" WHERE {predicate}"))
        .unwrap_or_default();
    let matched = ctx
        .ctx()
        .sql(&format!(
            "SELECT {projection} FROM {table_ref}{where_clause}"
        ))
        .await?;
    let is_live_target = |provider: &PaimonTableProvider| {
        let source = provider.table();
        source.identifier() == table.identifier()
            && source.location() == table.location()
            && source.branch() == table.branch()
            && !source.is_time_traveled()
            && !source.schema().core_options().has_time_travel_selector()
            && !source
                .schema()
                .options()
                .contains_key("incremental-between-timestamp")
    };
    // Validate the direct FROM source: a view can synthesize keys or hide scans.
    let source = match matched.logical_plan() {
        LogicalPlan::Projection(projection) => projection.input.as_ref(),
        plan => plan,
    };
    let source = match source {
        LogicalPlan::Filter(filter) => filter.input.as_ref(),
        plan => plan,
    };
    let target_changed = || {
        DataFusionError::Plan(format!(
            "{operation} target changed during planning or uses historical/incremental read options"
        ))
    };
    let LogicalPlan::TableScan(scan) = source else {
        return Err(target_changed());
    };
    let provider = source_as_provider(&scan.source)?;
    let Some(provider) = provider.downcast_ref::<PaimonTableProvider>() else {
        return Err(target_changed());
    };
    if !is_live_target(provider) {
        return Err(target_changed());
    }
    validate_read_options(provider.table(), table, operation)?;
    // Pin live self-references; preserve explicit historical and incremental reads.
    let plan = matched
        .into_unoptimized_plan()
        .transform_up_with_subqueries(|plan| {
            let LogicalPlan::TableScan(mut scan) = plan else {
                return Ok(Transformed::no(plan));
            };
            let Ok(provider) = source_as_provider(&scan.source) else {
                return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
            };
            if let Some(provider) = provider.downcast_ref::<PaimonTableProvider>() {
                if is_live_target(provider) {
                    validate_read_options(provider.table(), table, operation)?;
                    scan.source = provider_as_source(Arc::new(
                        provider
                            .clone()
                            .with_primary_key_mutation_snapshot(snapshot.as_ref()),
                    ));
                    return Ok(Transformed::yes(LogicalPlan::TableScan(scan)));
                }
            }
            Ok(Transformed::no(LogicalPlan::TableScan(scan)))
        })?
        .data;
    Ok(DataFrame::new(ctx.ctx().state(), plan))
}

fn validate_read_options(source: &Table, target: &Table, operation: &str) -> DFResult<()> {
    let source_schema = source.schema();
    let target_schema = target.schema();
    let source = source_schema.core_options();
    let target = target_schema.core_options();
    if source.merge_engine().map_err(to_datafusion_error)?
        != target.merge_engine().map_err(to_datafusion_error)?
        || source.sequence_fields() != target.sequence_fields()
        || source.sequence_field_sort_order_is_ascending()
            != target.sequence_field_sort_order_is_ascending()
        || source.bucket() != target.bucket()
        || source.bucket_function_type().map_err(to_datafusion_error)?
            != target.bucket_function_type().map_err(to_datafusion_error)?
        || source
            .bucket_key()
            .unwrap_or_else(|| source_schema.trimmed_primary_keys())
            != target
                .bucket_key()
                .unwrap_or_else(|| target_schema.trimmed_primary_keys())
        || source.deletion_vectors_enabled() != target.deletion_vectors_enabled()
        || source.data_evolution_enabled() != target.data_evolution_enabled()
        || source.ignore_delete() != target.ignore_delete()
        || source.rowkind_field() != target.rowkind_field()
        || source.query_auth_enabled() != target.query_auth_enabled()
    {
        return Err(DataFusionError::Plan(format!(
            "Primary-key {operation} requires read options to match the target's merge engine, \
             sequence ordering, buckets, deletion vectors, data evolution, row kinds and query authorization"
        )));
    }
    Ok(())
}
