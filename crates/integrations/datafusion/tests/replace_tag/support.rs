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

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use opendal::raw::*;
use opendal::{Capability, Operator};
use paimon::catalog::{Database, Identifier};
use paimon::spec::{Schema, SchemaChange};
use paimon::table::Table;
use paimon::{Catalog, CatalogOptions, FileSystemCatalog, Options};
use paimon_datafusion::SQLContext;

pub async fn context_with_table(warehouse: &Path, table: Table) -> SQLContext {
    let mut options = Options::new();
    options.set(
        CatalogOptions::WAREHOUSE,
        format!("file://{}", warehouse.display()),
    );
    let catalog = ResolvedTableCatalog {
        inner: FileSystemCatalog::new(options).unwrap(),
        table,
    };
    let mut context = SQLContext::new();
    context
        .register_catalog_with_default_db("paimon", Arc::new(catalog), None)
        .await
        .unwrap();
    context
}

struct ResolvedTableCatalog {
    inner: FileSystemCatalog,
    table: Table,
}

#[async_trait]
impl Catalog for ResolvedTableCatalog {
    async fn list_databases(&self) -> paimon::Result<Vec<String>> {
        self.inner.list_databases().await
    }

    async fn create_database(
        &self,
        name: &str,
        ignore_if_exists: bool,
        properties: HashMap<String, String>,
    ) -> paimon::Result<()> {
        self.inner
            .create_database(name, ignore_if_exists, properties)
            .await
    }

    async fn get_database(&self, name: &str) -> paimon::Result<Database> {
        self.inner.get_database(name).await
    }

    async fn drop_database(
        &self,
        name: &str,
        ignore_if_not_exists: bool,
        cascade: bool,
    ) -> paimon::Result<()> {
        self.inner
            .drop_database(name, ignore_if_not_exists, cascade)
            .await
    }

    async fn get_table(&self, identifier: &Identifier) -> paimon::Result<Table> {
        assert_eq!(identifier, self.table.identifier());
        Ok(self.table.clone())
    }

    async fn list_tables(&self, database_name: &str) -> paimon::Result<Vec<String>> {
        self.inner.list_tables(database_name).await
    }

    async fn create_table(
        &self,
        identifier: &Identifier,
        creation: Schema,
        ignore_if_exists: bool,
    ) -> paimon::Result<()> {
        self.inner
            .create_table(identifier, creation, ignore_if_exists)
            .await
    }

    async fn drop_table(
        &self,
        identifier: &Identifier,
        ignore_if_not_exists: bool,
    ) -> paimon::Result<()> {
        self.inner
            .drop_table(identifier, ignore_if_not_exists)
            .await
    }

    async fn rename_table(
        &self,
        from: &Identifier,
        to: &Identifier,
        ignore_if_not_exists: bool,
    ) -> paimon::Result<()> {
        self.inner
            .rename_table(from, to, ignore_if_not_exists)
            .await
    }

    async fn alter_table(
        &self,
        identifier: &Identifier,
        changes: Vec<SchemaChange>,
        ignore_if_not_exists: bool,
    ) -> paimon::Result<()> {
        self.inner
            .alter_table(identifier, changes, ignore_if_not_exists)
            .await
    }
}

pub const WRITE_FAILURE: &str = "injected tag writer creation failure";

pub fn fail_tag_writes(operator: Operator) -> (Operator, Arc<AtomicUsize>) {
    let attempts = Arc::new(AtomicUsize::new(0));
    let (context, inner) = operator.into_parts();
    let service = FailTagWrites {
        inner,
        attempts: attempts.clone(),
    };
    (Operator::from_parts(context, Arc::new(service)), attempts)
}

#[derive(Debug)]
struct FailTagWrites {
    inner: Servicer,
    attempts: Arc<AtomicUsize>,
}

impl Service for FailTagWrites {
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        self.inner.capability()
    }

    async fn create_dir(
        &self,
        ctx: &opendal::OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> opendal::Result<RpCreateDir> {
        self.inner.create_dir(ctx, path, args).await
    }

    async fn stat(
        &self,
        ctx: &opendal::OperationContext,
        path: &str,
        args: OpStat,
    ) -> opendal::Result<RpStat> {
        self.inner.stat(ctx, path, args).await
    }

    fn read(
        &self,
        ctx: &opendal::OperationContext,
        path: &str,
        args: OpRead,
    ) -> opendal::Result<Self::Reader> {
        self.inner.read(ctx, path, args)
    }

    fn write(
        &self,
        ctx: &opendal::OperationContext,
        path: &str,
        args: OpWrite,
    ) -> opendal::Result<Self::Writer> {
        if path.ends_with("/tag/tag-release") {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            return Err(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                WRITE_FAILURE,
            ));
        }
        self.inner.write(ctx, path, args)
    }

    fn delete(&self, ctx: &opendal::OperationContext) -> opendal::Result<Self::Deleter> {
        self.inner.delete(ctx)
    }

    fn list(
        &self,
        ctx: &opendal::OperationContext,
        path: &str,
        args: OpList,
    ) -> opendal::Result<Self::Lister> {
        self.inner.list(ctx, path, args)
    }

    fn copy(
        &self,
        ctx: &opendal::OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
        opts: OpCopier,
    ) -> opendal::Result<Self::Copier> {
        self.inner.copy(ctx, from, to, args, opts)
    }

    async fn rename(
        &self,
        ctx: &opendal::OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> opendal::Result<RpRename> {
        self.inner.rename(ctx, from, to, args).await
    }

    async fn presign(
        &self,
        ctx: &opendal::OperationContext,
        path: &str,
        args: OpPresign,
    ) -> opendal::Result<RpPresign> {
        self.inner.presign(ctx, path, args).await
    }
}
