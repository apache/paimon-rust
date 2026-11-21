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

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::Statistics;
use datafusion::datasource::listing::ListingTable;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::object_store::ObjectStoreRegistry;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::session_state::SessionState;
use datafusion::execution::{SendableRecordBatchStream, SessionStateBuilder, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::object_store::ObjectStore;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use std::sync::Arc;
use url::Url;

#[derive(Debug)]
struct Registry {
    root: Url,
    store: Arc<dyn ObjectStore>,
}

impl ObjectStoreRegistry for Registry {
    fn register_store(&self, _: &Url, _: Arc<dyn ObjectStore>) -> Option<Arc<dyn ObjectStore>> {
        None
    }
    fn get_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        if super::same_authority(url, &self.root) {
            Ok(self.store.clone())
        } else {
            Err(super::error("object store is outside this file scan"))
        }
    }
}

pub(super) fn runtime(
    parent: &RuntimeEnv,
    root: Url,
    store: Arc<dyn ObjectStore>,
) -> Result<Arc<RuntimeEnv>> {
    let cache = &parent.cache_manager;
    // A URL can refer to different objects under different credentials. Share
    // execution budgets, but do not share file metadata across source bindings.
    let caches = CacheManagerConfig {
        file_statistics_cache: None,
        file_statistics_cache_limit: cache.get_file_statistic_cache_limit(),
        list_files_cache: None,
        list_files_cache_limit: cache.get_list_files_cache_limit(),
        list_files_cache_ttl: cache.get_list_files_cache_ttl(),
        file_metadata_cache: None,
        metadata_cache_limit: cache.get_metadata_cache_limit(),
    };
    RuntimeEnvBuilder::from_runtime_env(parent)
        .with_object_store_registry(Arc::new(Registry { root, store }))
        .with_cache_manager(caches)
        .build_arc()
}

#[derive(Debug)]
pub(super) struct ScopedTable {
    inner: ListingTable,
    root: Url,
    store: Arc<dyn ObjectStore>,
}

impl ScopedTable {
    pub(super) fn new(inner: ListingTable, root: Url, store: Arc<dyn ObjectStore>) -> Self {
        Self { inner, root, store }
    }
}

#[async_trait]
impl TableProvider for ScopedTable {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let state = state
            .as_any()
            .downcast_ref::<SessionState>()
            .ok_or_else(|| super::error("requires a DataFusion SessionState"))?;
        let runtime = runtime(state.runtime_env(), self.root.clone(), self.store.clone())?;
        let state = SessionStateBuilder::new_from_existing(state.clone())
            .with_runtime_env(runtime)
            .build();
        let inner = self.inner.scan(&state, projection, filters, limit).await?;
        Ok(Arc::new(ScopedExec {
            inner,
            root: self.root.clone(),
            store: self.store.clone(),
        }))
    }
}

#[derive(Debug)]
struct ScopedExec {
    inner: Arc<dyn ExecutionPlan>,
    root: Url,
    store: Arc<dyn ObjectStore>,
}
impl DisplayAs for ScopedExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ReadFilesSource")
    }
}
impl ExecutionPlan for ScopedExec {
    fn name(&self) -> &str {
        "ReadFilesSource"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }
    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        Ok(input_stats[0].clone())
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [inner] = children.as_slice() else {
            return Err(super::error("file source requires one child"));
        };
        Ok(Arc::new(Self {
            inner: inner.clone(),
            root: self.root.clone(),
            store: self.store.clone(),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let runtime = runtime(
            &context.runtime_env(),
            self.root.clone(),
            self.store.clone(),
        )?;
        let context = TaskContext::new(
            context.task_id(),
            context.session_id(),
            context.session_config().clone(),
            context.scalar_functions().clone(),
            context.higher_order_functions().clone(),
            context.aggregate_functions().clone(),
            context.window_functions().clone(),
            runtime,
        );
        self.inner.execute(partition, Arc::new(context))
    }
}
