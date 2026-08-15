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
use std::error::Error;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use paimon::catalog::Identifier;
use paimon::spec::{DataType, IntType, Schema, VarCharType};
use paimon::{Catalog, CatalogFactory, CatalogOptions, Options};

// This example creates a paimon table and inserts test data
// Run the example by passing the catalog warehouse path first after `--`:
// Eg: cargo run --package paimon --example create_table -- /path/to/warehouse --overwrite
// Use optional --overwrite flag after the warehouse path to automatically drop and re-create
// the table if it already exists.
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);

    let warehouse = args.next().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "usage: cargo run --package paimon --example create_table -- <warehouse-path> --overwrite",
        )
    })?;

    let overwrite = args.any(|arg| arg == "--overwrite");

    // Open local catalog
    let catalog = create_catalog(warehouse).await?;

    // Create new database
    catalog
        .create_database("my_db", true, HashMap::new())
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

    // Check if table exists in catalog
    let table_exists = match catalog.get_table(&identifier).await {
        Ok(_) => true,
        Err(paimon::Error::TableNotExist { .. }) => false,
        Err(error) => return Err(error.into()),
    };

    if table_exists {
        if !overwrite {
            return Err(format!(
                "table {} already exists, pass --overwrite to automatically drop and re-create it",
                identifier
            )
            .into());
        }

        catalog.drop_table(&identifier, false).await?;
    }
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

pub async fn create_catalog(warehouse: String) -> Result<Arc<dyn Catalog>, Box<dyn Error>> {
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    let catalog = CatalogFactory::create(options).await?;
    Ok(catalog)
}
