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
