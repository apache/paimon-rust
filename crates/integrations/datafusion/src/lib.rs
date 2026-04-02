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

//! Apache Paimon DataFusion Integration (read-only).
//!
//! Register a Paimon table as a DataFusion table provider to query it with SQL or DataFrame API.
//!
//! # Example
//!
//! ```ignore
//! use std::sync::Arc;
//! use datafusion::prelude::SessionContext;
//! use paimon_datafusion::PaimonTableProvider;
//!
//! // Obtain a Paimon Table (e.g. from your catalog), then:
//! let provider = PaimonTableProvider::try_new(table)?;
//! let ctx = SessionContext::new();
//! ctx.register_table("my_table", Arc::new(provider))?;
//! let df = ctx.sql("SELECT * FROM my_table").await?;
//! ```
//!
//! This version supports partition predicate pushdown by extracting
//! translatable partition-only conjuncts from DataFusion filters.

mod catalog;
mod error;
mod filter_pushdown;
mod physical_plan;
mod relation_planner;
mod schema;
mod table;

pub use catalog::{PaimonCatalogProvider, PaimonSchemaProvider};
pub use error::to_datafusion_error;
pub use physical_plan::PaimonTableScan;
pub use relation_planner::PaimonRelationPlanner;
pub use schema::paimon_schema_to_arrow;
pub use table::PaimonTableProvider;
