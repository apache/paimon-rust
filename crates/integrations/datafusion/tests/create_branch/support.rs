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
use std::sync::Arc;

use async_trait::async_trait;
use paimon::catalog::{Database, Identifier};
use paimon::spec::{Schema, SchemaChange};
use paimon::table::Table;
use paimon::{Catalog, FileSystemCatalog};
use paimon_datafusion::SQLContext;

pub async fn context_with_table(inner: Arc<FileSystemCatalog>, table: Table) -> SQLContext {
    let mut context = SQLContext::new();
    context
        .register_catalog_with_default_db(
            "paimon",
            Arc::new(ResolvedTableCatalog { inner, table }),
            None,
        )
        .await
        .unwrap();
    context
}

struct ResolvedTableCatalog {
    inner: Arc<FileSystemCatalog>,
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
