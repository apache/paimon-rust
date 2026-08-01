use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use paimon::catalog::Identifier;
use paimon::spec::{DataType, IntType, Schema, VarCharType};
use paimon::{Catalog, CatalogFactory, CatalogOptions, Options};

// This example creates a paimon table and inserts test data
// set the catalog path and run example using:
// cargo run --package paimon --example create_table
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Open local catalog
    let catalog = create_catelog().await?;

    // Create new database
    catalog
        .create_database("my_db", false, HashMap::new())
        .await?;

    // Define table schema and its data types
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("name", DataType::VarChar(VarCharType::string_type()))
        .column("city", DataType::VarChar(VarCharType::string_type()))
        .column("age", DataType::Int(IntType::new()))
        .column("score", DataType::Int(IntType::new()))
        .build()?;

    let identifier = Identifier::new("my_db", "users");

    // create table
    catalog.create_table(&identifier, schema, false).await?;

    let table = catalog.get_table(&identifier).await?;

    let builder = table.new_write_builder();
    let txn = builder.new_commit();

    let mut writer = builder.new_write()?;

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", ArrowDataType::Int32, false),
        Field::new("name", ArrowDataType::Utf8, false),
        Field::new("city", ArrowDataType::Utf8, false),
        Field::new("age", ArrowDataType::Int32, false),
        Field::new("score", ArrowDataType::Int32, false),
    ]));

    // sample data
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Alice", "Bob", "Paul", "Diana", "Ethan",
            ])),
            Arc::new(StringArray::from(vec![
                "New York",
                "San Francisco",
                "Bengaluru",
                "Amsterdam",
                "Berlin",
            ])),
            Arc::new(Int32Array::from(vec![28, 34, 22, 31, 27])),
            Arc::new(Int32Array::from(vec![95, 82, 91, 88, 76])),
        ],
    )?;

    writer.write_arrow_batch(&batch).await?;

    let msg = writer.prepare_commit().await?;

    txn.commit(msg).await?;

    Ok(())
}

pub async fn create_catelog() -> Result<Arc<dyn Catalog>, Box<dyn Error>> {
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, "/path-to/testdata");
    let catalog = CatalogFactory::create(options).await?;
    Ok(catalog)
}
