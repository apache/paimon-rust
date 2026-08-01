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

use std::error::Error;
use std::sync::Arc;

use datafusion::prelude::{col, lit, SessionContext};
use paimon::catalog::Identifier;
use paimon::{Catalog, CatalogFactory, CatalogOptions, Options};
use paimon_datafusion::PaimonTableProvider;

// This example demonstrates how to query a Paimon table
// using the DataFusion DataFrame API.
//
// Before running this example, create the sample table at
// examples/create_table
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Open the local Paimon catalog
    let catalog = create_catelog().await?;

    // Load the users table
    let identifier = Identifier::new("my_db", "users");
    let table = catalog.get_table(&identifier).await?;

    // DataFusion TableProvider for the Paimon table
    let provider = PaimonTableProvider::try_new(table)?;

    let ctx = SessionContext::new();

    // Register table
    ctx.register_table("user_table", Arc::new(provider))?;

    let df = ctx.table("user_table").await?;

    // Filter users with score >= 90 and select a subset of columns
    let df = df.filter(col("score").gt_eq(lit(90)))?.select(vec![
        col("name"),
        col("city"),
        col("score"),
    ])?;

    // Expected output:
    //
    // +-------+-----------+-------+
    // | name  | city      | score |
    // +-------+-----------+-------+
    // | Alice | New York  | 95    |
    // | Paul  | Bengaluru | 91    |
    // +-------+-----------+-------+

    // Display the results
    df.show().await?;

    Ok(())
}

pub async fn create_catelog() -> Result<Arc<dyn Catalog>, Box<dyn Error>> {
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, "/path-to/testdata");
    let catalog = CatalogFactory::create(options).await?;
    Ok(catalog)
}
